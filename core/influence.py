"""群史记 influence —— 多源影响力排行。

融合三个来源计算群成员影响力, 返回按 score 降序的成员列表:

- LM: LivingMemory 留存消息数 + 活跃天数 (本插件 evidence, 始终尝试)
- SL: self_learning 社交图 PageRank (软来源)
- RA: group_relation_analyzer 关系中心度 (软来源)

软来源 = 插件缺失 / 调用异常 / 数据为空 时视为「来源缺失」, 剩余来源的权重
按比例重归一化 (如仅剩 LM → lm=1.0), 绝不因单个来源故障影响整体排行。

设计约束:
- min-max 归一化时 max==min → 全员 1.0, 绝不除零 (单人群/全员等值均安全)。
- 不直接 import networkx 或任何第三方插件模块; 只通过 context 反射定位
  已注册插件实例, 所有软来源访问均 try/except 包裹。
- 模块顶层不 import astrbot, 保证单元测试无需安装 astrbot 即可运行。
"""

import inspect
import asyncio
import logging
import sqlite3
import time
from collections import Counter, deque
from datetime import date, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {"lm": 0.5, "sl": 0.3, "ra": 0.2}

#: 星实例/分析器解析结果缓存 (P3 性能: 避免每次排行都做全量星扫描 + 受限 BFS)。
#: key -> (time.time(), result) ; result 可以是分析器实例或 None (未找到)。
_PROBE_CACHE: dict[tuple, tuple[float, Any]] = {}
#: 探测缓存 TTL (秒)
_PROBE_TTL = 300.0


def _clear_probe_cache() -> None:
    """清空探测缓存 (测试场景与插件热重载)。"""
    _PROBE_CACHE.clear()


def _cached_probe(key: tuple, producer):
    """带 TTL 的解析结果缓存: 命中直接返回; 生产异常时失效并重抛。"""
    now = time.time()
    entry = _PROBE_CACHE.get(key)
    if entry is not None and now - entry[0] < _PROBE_TTL:
        return entry[1]
    try:
        result = producer()
    except Exception:
        _PROBE_CACHE.pop(key, None)
        raise
    _PROBE_CACHE[key] = (now, result)
    return result


# 软来源插件别名 (小写匹配)
_SL_ALIASES = ("astrbot_plugin_self_learning", "selflearning")
_RA_ALIASES = ("astrbot_plugin_group_relation_analyzer", "grouprelationanalyzer")
_RA_PLUGIN_NAME = "astrbot_plugin_group_relation_analyzer"

# 防御性排除的机器人/占位 sender_id
_BOT_IDS = {"bot", "none", "assistant", "null", "system", "unknown"}


def extract_group_id(umo: str) -> str:
    """从 unified_msg_origin 提取纯群号。

    格式如 "aiocqhttp:GroupMessage:123456789" / "qq:123:group:456"。
    取最后一个 ":" 段; 若末段形如 "<botid>_<groupid>" 则取 "_" 后部分
    (参考真寻日报 _extract_group_id 的兼容处理)。
    """
    s = str(umo or "").strip()
    if not s:
        return ""
    last = s.split(":")[-1].strip()
    if "_" in last:
        last = last.split("_")[-1]
    return last


def _is_bot_id(sender_id: Any) -> bool:
    """防御性机器人 id 判定: None / 空串 / "bot" 等占位 id。"""
    if sender_id is None:
        return True
    s = str(sender_id).strip().lower()
    return not s or s in _BOT_IDS


def _minmax(values: dict[str, float]) -> dict[str, float] | None:
    """min-max 归一化。

    空输入 → None (调用方视为来源缺失);
    max == min → 全员 1.0 (单人群 / 全员等值, 绝不除零)。
    """
    if not values:
        return None
    vals = list(values.values())
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return {k: 1.0 for k in values}
    span = hi - lo
    return {k: (float(v) - lo) / span for k, v in values.items()}


# ---------- LM 来源 ----------


def _lm_source(evidence: Any) -> dict[str, float] | None:
    """LM 来源: lm_raw = 0.7 * 留存消息数 + 0.3 * 活跃天数, 再 min-max。

    - counts: evidence.stats (全部历史, LivingMemory 已按 role='user' 过滤)。
    - active_days: evidence.chronicle_msgs 中每个 sender 的唯一自然日数
      (m.timestamp → datetime.date)。
    - senders 取 counts ∪ chronicle senders。
    空 → None (来源缺失)。
    """
    msgs = getattr(evidence, "chronicle_msgs", None) or []
    counts = Counter(str(getattr(m, "sender_id", "") or "") for m in msgs)

    active_days: dict[str, set] = {}
    for m in msgs:
        sid = str(getattr(m, "sender_id", "") or "")
        if not sid or _is_bot_id(sid):
            continue
        try:
            ts = float(getattr(m, "timestamp", 0) or 0)
            day = date.fromtimestamp(ts)
        except Exception:
            continue
        active_days.setdefault(sid, set()).add(day)

    senders = set(counts) | set(active_days)
    if not senders:
        return None

    raw: dict[str, float] = {}
    for sid in senders:
        if _is_bot_id(sid):
            continue
        key = str(sid)
        try:
            count = float(counts.get(sid, 0) or 0)
        except Exception:
            count = 0.0
        days = float(len(active_days.get(key, ())))
        raw[key] = 0.7 * count + 0.3 * days
    if not raw:
        return None
    return _minmax(raw)


# ---------- 插件实例解析 (反射, 全程 guarded) ----------


def _resolve_star_instance(context: Any, aliases: tuple[str, ...]) -> Any | None:
    """按别名定位已注册插件实例 (返回 star.star_cls 实例)。

    依次尝试 context.get_registered_star(alias) 与 get_all_stars() 全量扫描
    (参考 self_learning feature_delegation._find_active_star 的模式)。
    """
    if context is None:
        return None
    wanted = {str(a).strip().lower() for a in aliases if str(a or "").strip()}

    getter = getattr(context, "get_registered_star", None)
    if callable(getter):
        for alias in aliases:
            try:
                star = getter(alias)
            except Exception:
                star = None
            if star is not None and getattr(star, "activated", True) is not False:
                instance = getattr(star, "star_cls", None)
                if instance is not None:
                    return instance

    all_getter = getattr(context, "get_all_stars", None)
    if not callable(all_getter):
        return None
    try:
        stars = all_getter() or []
    except Exception:
        return None

    for star in stars:
        if star is None or getattr(star, "activated", True) is False:
            continue
        instance = getattr(star, "star_cls", None)
        if instance is None:
            continue
        candidates = {
            getattr(star, "name", None),
            getattr(star, "display_name", None),
            getattr(star, "root_dir_name", None),
            getattr(star, "module_path", None),
        }
        module_path = getattr(star, "module_path", None)
        if isinstance(module_path, str):
            candidates.update(p for p in module_path.split(".") if p)
        candidates.add(type(instance).__name__)
        normalized = {
            str(c).strip().lower() for c in candidates if str(c or "").strip()
        }
        if normalized & wanted:
            return instance
    return None


def _iter_children(obj: Any):
    """产出容器对象的所有子对象 (dict 值 / 列表元素 / 非私有属性值)。"""
    if obj is None:
        return
    if isinstance(obj, dict):
        for v in obj.values():
            if v is not None:
                yield v
        return
    if isinstance(obj, (list, tuple, set)):
        for v in obj:
            if v is not None:
                yield v
        return
    if inspect.ismodule(obj) or inspect.isclass(obj):
        return
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            child = getattr(obj, name)
        except Exception:
            continue
        if child is not None:
            yield child


def _iter_attr_names(obj: Any):
    """对象可探查的属性名 (含 dict 键)。"""
    names: list[str] = []
    try:
        names.extend(n for n in dir(obj) if not n.startswith("_"))
    except Exception:
        pass
    if isinstance(obj, dict):
        names.extend(str(k) for k in obj.keys())
    return names


def _descendable(child: Any) -> bool:
    """是否值得继续下钻 (排除模块/类/函数/原始类型)。"""
    if inspect.ismodule(child) or inspect.isclass(child) or inspect.isroutine(child):
        return False
    if isinstance(child, (str, bytes, int, float, bool, type(None))):
        return False
    if isinstance(child, (dict, list, tuple, set)):
        return True
    return hasattr(child, "__dict__")


def _bfs_influence_analyzer(plugin: Any) -> Any | None:
    """受限 BFS: 在插件实例属性图中定位影响力分析器。

    - 优先: 属性名含 "social_graph" / "influence" 且暴露 get_influence_ranking。
    - 兜底: 任意访问到的、暴露 get_influence_ranking 的对象 (最近优先)。
    - 深度 ≤ 3 且访问节点 ≤ 400, 防止遍历到整个 astrbot 对象图。
    """
    queue = deque([(plugin, 0)])
    seen: set[int] = set()
    visited = 0
    fallback: Any | None = None
    while queue and visited < 400:
        obj, depth = queue.popleft()
        oid = id(obj)
        if oid in seen:
            continue
        seen.add(oid)
        visited += 1

        if callable(getattr(obj, "get_influence_ranking", None)):
            fallback = fallback or obj

        if depth >= 3:
            continue
        for name in _iter_attr_names(obj):
            lower = name.lower()
            try:
                child = getattr(obj, name)
            except Exception:
                continue
            if child is None:
                continue
            if ("social_graph" in lower or "influence" in lower) and callable(
                getattr(child, "get_influence_ranking", None)
            ):
                return child
            if _descendable(child):
                queue.append((child, depth + 1))
    return fallback


def _find_influence_analyzer(plugin: Any) -> Any | None:
    """在插件实例上定位暴露 get_influence_ranking 的分析器 (带 TTL 缓存)。

    首次调用完整解析 (已知属性名 → services 容器 → 受限 BFS); 结果按
    (context 实例) 维度缓存 _PROBE_TTL 秒, 避免每次排行都重复 BFS。
    解析过程中的异常会使缓存失效并向上传播 (调用方已守卫)。
    """
    if plugin is None:
        return None
    key = ("influence_analyzer", id(plugin))
    return _cached_probe(key, lambda: _find_influence_analyzer_uncached(plugin))


def _find_influence_analyzer_uncached(plugin: Any) -> Any | None:
    """(无缓存) 在插件实例上定位暴露 get_influence_ranking 的分析器。

    依次尝试: 已知属性名 → services 容器 → 受限 BFS。失败返回 None。
    """
    if plugin is None:
        return None

    for name in ("social_graph_analyzer", "influence_analyzer", "social_analyzer"):
        try:
            obj = getattr(plugin, name, None)
        except Exception:
            obj = None
        if obj is not None and callable(getattr(obj, "get_influence_ranking", None)):
            return obj

    try:
        services = getattr(plugin, "services", None)
    except Exception:
        services = None
    for probe in _iter_children(services):
        if callable(getattr(probe, "get_influence_ranking", None)):
            return probe
        for name in (
            "social_graph_analyzer",
            "social_graph",
            "social_analyzer",
            "social",
        ):
            try:
                sub = getattr(probe, name, None)
            except Exception:
                sub = None
            if sub is not None and callable(
                getattr(sub, "get_influence_ranking", None)
            ):
                return sub

    return _bfs_influence_analyzer(plugin)


# ---------- SL 来源 (self_learning PageRank) ----------


async def _sl_source(context: Any, group_id: str) -> dict[str, float] | None:
    """SL 来源: 调 self_learning 的 get_influence_ranking, PageRank 值 min-max。

    插件缺失 / 分析器缺失 / 调用异常 / 结果为空 → None (来源缺失)。
    """
    plugin = _resolve_star_instance(context, _SL_ALIASES)
    analyzer = _find_influence_analyzer(plugin)
    if analyzer is None:
        logger.info("SL 来源缺失: 未定位到 self_learning 影响力分析器")
        return None
    method = getattr(analyzer, "get_influence_ranking", None)
    if not callable(method):
        logger.info("SL 来源缺失: 分析器未暴露 get_influence_ranking")
        return None

    result = None
    for attempt in (lambda: method(group_id, top_k=100), lambda: method(group_id)):
        try:
            result = attempt()
            if inspect.isawaitable(result):
                result = await result
            break
        except TypeError:
            result = None
            continue
        except Exception as exc:
            logger.warning(f"SL 来源缺失: get_influence_ranking 调用失败: {exc}")
            return None
    if not result:
        logger.info("SL 来源缺失: PageRank 结果为空")
        return None

    values: dict[str, float] = {}
    for row in result:
        if not isinstance(row, dict):
            continue
        uid = str(row.get("user_id", "") or "")
        if not uid or _is_bot_id(uid):
            continue
        try:
            score = float(row.get("pagerank", row.get("score", 0)) or 0)
        except Exception:
            score = 0.0
        values[uid] = score
    if not values:
        return None
    return _minmax(values)


# ---------- RA 来源 (group_relation_analyzer 中心度) ----------


async def _ra_source(context: Any, group_id: str) -> dict[str, float] | None:
    """RA 来源: 优先进程内查询 API, 回退只读 SQLite。任一失败 → None。"""
    plugin = _resolve_star_instance(context, _RA_ALIASES)
    if plugin is not None:
        values = await _ra_in_process(plugin, group_id)
        if values:
            return _minmax(values)
        logger.info("RA 来源: 进程内查询无数据, 回退只读 SQLite")
    # 与 relation_pairs() 保持一致: 只读 SQLite 一律丢到线程池, 不要在事件循环上做磁盘 I/O
    # (timeout=1 意味着最坏情况会阻塞事件循环 1 秒)。_ra_sqlite 是纯同步函数、
    # 无共享状态、内部已捕获所有异常并返回 None, 因此线程化后返回值与异常语义不变。
    values = await asyncio.to_thread(_ra_sqlite, group_id)
    if not values:
        return None
    return _minmax(values)


async def _ra_in_process(plugin: Any, group_id: str) -> dict[str, float] | None:
    """进程内调用关系分析插件的查询 API (store.overview / export_group)。

    返回 {user_id: 原始分数} 或 None (无 store / 调用失败 / 无数据)。
    """
    store = getattr(plugin, "store", None)
    if store is None:
        return None
    method = getattr(store, "overview", None)
    if not callable(method):
        method = getattr(store, "export_group", None)
    if not callable(method):
        return None

    result = None
    for attempt in (
        lambda: method(group_id, top_limit=200, daily_decay=1.0),
        lambda: method(group_id),
    ):
        try:
            result = attempt()
            if inspect.isawaitable(result):
                result = await result
            break
        except TypeError:
            result = None
            continue
        except Exception as exc:
            logger.warning(f"RA 来源缺失: 进程内查询失败: {exc}")
            return None
    if not isinstance(result, dict):
        return None
    return _parse_ra_result(result)


def _parse_ra_result(result: dict) -> dict[str, float] | None:
    """从 overview/export_group 结果提取每用户分数。

    主取 centers / social_centers (中心度), 缺省回退 active_users / users
    的发言计数。
    """
    values: dict[str, float] = {}

    def _add(uid: Any, score: Any) -> None:
        if uid is None or _is_bot_id(uid):
            return
        try:
            values[str(uid)] = float(score or 0)
        except Exception:
            pass

    for key in ("centers", "social_centers"):
        for item in result.get(key) or []:
            if isinstance(item, dict):
                _add(item.get("user_id", ""), item.get("score", 0))
            elif isinstance(item, (tuple, list)) and len(item) >= 2:
                _add(item[0], item[1])
    if not values:
        for item in result.get("active_users") or []:
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                info = item[1]
                count = info.get("count", 0) if isinstance(info, dict) else info
                _add(item[0], count)
            elif isinstance(item, dict):
                _add(item.get("user_id", ""), item.get("message_count", 0))
        for uid, info in (result.get("users") or {}).items():
            if isinstance(info, dict):
                _add(uid, info.get("count", 0))
    return values or None


def _resolve_ra_db_path() -> Path | None:
    """解析关系分析插件 SQLite 路径 (只读, **零副作用**)。

    这里**不能**用 ``StarTools.get_data_dir("<插件名>")``: 该 API 内部会调用
    ``ensure_dir()`` (astrbot/core/star/star_tools.py:285-290), 也就是在对方插件的
    数据目录里凭空建出文件夹 —— 一个「只读探测」不应该有文件系统副作用。
    改用不建目录的等价算法: astrbot 数据根 / plugin_data / <插件名>, 与
    StarTools.get_data_dir 的返回值完全相同, 只是不再 mkdir。

    回退/可用性语义保持不变: 候选路径都不存在时返回第一个候选, 由调用方打印
    「数据库不存在」; 连数据根都解析不出来时返回 None。
    """
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path  # type: ignore

        root = Path(str(get_astrbot_data_path())) / "plugin_data" / _RA_PLUGIN_NAME
    except Exception:
        return None
    candidates = [
        root / "relations.sqlite3",
        root / _RA_PLUGIN_NAME / "relations.sqlite3",
    ]
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    return candidates[0]


#: RA 各查询所需的最小列集合 (用于 schema 存在性校验)
_RA_DEGREE_COLUMNS = ("group_id", "a", "b", "score")
_RA_DAILY_COLUMNS = ("group_id", "day", "a", "b", "evidence")
_RA_PAIRS_RELATION_COLUMNS = ("group_id", "a", "b", "score", "last_ts")

#: schema 不兼容告警去重: (库路径, 原因) -> 已告警。
#: 一次性告警, 避免每次排行/每条命令都刷同一条 WARNING; 进程内有效。
_RA_SCHEMA_WARNED: set[tuple[str, str]] = set()


def _warn_ra_once(db_path: Any, reason: str, message: str) -> None:
    """同一 (库, 原因) 只告警一次。级别为 WARNING, 保证在默认日志级别可见。"""
    key = (str(db_path), reason)
    if key in _RA_SCHEMA_WARNED:
        return
    _RA_SCHEMA_WARNED.add(key)
    logger.warning(message)


def _ra_read_schema(conn: Any) -> dict[str, set[str]]:
    """读出库里每张表的列名: {表名: {列名}}。

    任何一步失败都退化为「空集合/空字典」而不是抛异常 —— 调用方据此判定
    「缺表 / 缺列」, 从而把 schema 不兼容与「对方就是没数据」区分开。
    """
    schema: dict[str, set[str]] = {}
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    except Exception:  # noqa: BLE001 - 读不到 schema 视作完全不可用
        return schema
    for row in tables:
        try:
            name = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        except Exception:  # noqa: BLE001
            continue
        if not name:
            continue
        try:
            # PRAGMA 不支持参数占位符, 表名只能内插; 这里只内插从 sqlite_master
            # 读出的表名并转义双引号, 不接受任何外部输入。
            info = conn.execute(
                'PRAGMA table_info("%s")' % name.replace('"', '""')
            ).fetchall()
        except Exception:  # noqa: BLE001
            schema[name] = set()
            continue
        cols: set[str] = set()
        for col in info:
            try:
                cname = col["name"] if isinstance(col, sqlite3.Row) else col[1]
            except Exception:  # noqa: BLE001
                continue
            if cname:
                cols.add(str(cname))
        schema[name] = cols
    return schema


def _ra_missing_columns(
    schema: dict[str, set[str]], table: str, required: tuple[str, ...]
) -> list[str] | None:
    """表缺失返回 None; 表存在但缺列返回缺列列表 (空列表 = 完全兼容)。"""
    if table not in schema:
        return None
    return [c for c in required if c not in schema[table]]


def _ra_warn_schema(
    db_path: Any,
    table: str,
    required: tuple[str, ...],
    schema: dict[str, set[str]],
    *,
    consequence: str,
) -> None:
    """对「缺表 / 缺列」给出一次性、可操作的 WARNING。"""
    if table not in schema:
        # 缺表是同一个根因, 两个消费方(影响力榜 / 关系弧)共用这条告警, 所以后果
        # 写全 —— 否则先调用的那一方会把另一方的后果盖掉。
        _warn_ra_once(
            db_path,
            f"missing_table:{table}",
            f"RA 来源 schema 不兼容: 数据库缺少表 {table} ({db_path}); "
            f"该库可能不属于 group_relation_analyzer, 或对方版本过旧。"
            f"本次跳过 RA 来源(影响力榜按其余来源继续, 关系弧分节不展示)。",
        )
        return
    missing = _ra_missing_columns(schema, table, required)
    if not missing:
        return
    _warn_ra_once(
        db_path,
        f"missing_column:{table}:{','.join(required)}:{','.join(missing)}",
        f"RA 来源 schema 不兼容: 表 {table} 缺少列 {missing} ({db_path}); "
        f"实际列为 {sorted(schema[table])}。"
        f"多半是 group_relation_analyzer 表结构变更/改名所致, 建议核对该插件版本"
        f"或用 /关系图 /分析图 确认它自身读写正常。{consequence}",
    )


def _ra_sqlite(group_id: str) -> dict[str, float] | None:
    """只读 relation_analyzer 的 SQLite: relations 表按群统计每用户度数。

    读之前先校验表/列是否存在:
    - 缺表 / 缺列 -> 一次性 WARNING + 返回 None (schema 不兼容, 与「没数据」区分);
    - 表列齐全但本群无记录 -> INFO + 返回 None (对方就是没数据)。

    缺库 / 损坏 / 无表 / 0 行 → None (来源缺失), 绝不抛异常。
    """
    db_path = _resolve_ra_db_path()
    if db_path is None:
        logger.info("RA 来源缺失: 无法解析插件数据目录")
        return None
    try:
        exists = db_path.exists()
    except Exception:
        return None
    if not exists:
        logger.info(f"RA 来源缺失: 数据库不存在 ({db_path})")
        return None

    degree: dict[str, float] = {}
    try:
        conn = sqlite3.connect(
            db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1
        )
        try:
            conn.row_factory = sqlite3.Row
            schema = _ra_read_schema(conn)
            if _ra_missing_columns(schema, "relations", _RA_DEGREE_COLUMNS) != []:
                # 缺表 -> None, 缺列 -> 非空列表; 两者都说明 schema 不兼容
                _ra_warn_schema(
                    db_path,
                    "relations",
                    _RA_DEGREE_COLUMNS,
                    schema,
                    consequence="本次跳过 RA 来源(影响力榜按其余来源继续)。",
                )
                return None
            rows = conn.execute(
                "SELECT a, b, score FROM relations WHERE group_id=?",
                (group_id,),
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(f"RA 来源缺失: SQLite 读取失败: {exc}")
        return None

    for row in rows:
        try:
            score = float(row["score"] or 0)
        except Exception:
            score = 0.0
        a = str(row["a"] or "")
        b = str(row["b"] or "")
        if a and not _is_bot_id(a):
            degree[a] = degree.get(a, 0.0) + score
        if b and not _is_bot_id(b):
            degree[b] = degree.get(b, 0.0) + score
    if not degree:
        # 表与列都齐全却没拿到人 -> 「对方就是没有数据」 (INFO),
        # 与上面的 schema 不兼容 WARNING 在日志上可区分。
        logger.info(
            f"RA 来源: relations 表结构兼容, 但本群无可用记录 (group_id={group_id})"
        )
        return None
    return degree


# ---------- 融合 ----------


def _sender_names(evidence: Any) -> dict[str, str]:
    """从 evidence 消息中解析 sender_id → sender_name (首个命中)。"""
    names: dict[str, str] = {}
    for msgs in (
        getattr(evidence, "chronicle_msgs", None) or [],
        getattr(evidence, "biography_msgs", None) or [],
    ):
        for m in msgs:
            sid = str(getattr(m, "sender_id", "") or "")
            if not sid or sid in names or _is_bot_id(sid):
                continue
            name = getattr(m, "sender_name", None)
            if name:
                names[sid] = str(name)
    return names


def _fuse(
    evidence: Any,
    sources: dict[str, dict[str, float] | None],
    weights: dict | None,
    top_k: int,
) -> list[dict]:
    """融合存活来源: 权重按比例重归一化, 逐成员加权求和, 降序取 top_k。

    - 来源缺失/为空 → 不参与, 剩余来源权重按比例放大 (仅 LM → lm=1.0)。
    - 某发送者在一个来源无值 → 该来源计 0。
    - 自定义权重未覆盖任何存活来源 → 存活来源等权。
    """
    alive = {k: v for k, v in sources.items() if v}
    if not alive or not top_k or top_k <= 0:
        return []

    w = dict(weights or DEFAULT_WEIGHTS)
    alive_names = list(alive)
    picked: dict[str, float] = {}
    for k in alive_names:
        try:
            picked[k] = float(w.get(k, 0.0) or 0.0)
        except Exception:
            picked[k] = 0.0
    total = sum(picked.values())
    if total <= 0:
        share = 1.0 / len(alive_names)
        final_w = {k: share for k in alive_names}
    else:
        final_w = {k: v / total for k, v in picked.items()}

    senders: set[str] = set()
    for v in alive.values():
        senders.update(k for k in v if not _is_bot_id(k))
    active = {
        str(getattr(m, "sender_id", "") or "")
        for m in (getattr(evidence, "chronicle_msgs", None) or [])
        if not _is_bot_id(getattr(m, "sender_id", None))
    }
    senders.intersection_update(active)
    if not senders:
        return []

    names = _sender_names(evidence)
    results = []
    for sid in senders:
        breakdown: dict[str, float] = {}
        score = 0.0
        for src in alive_names:
            value = alive[src].get(sid, 0.0)
            breakdown[src] = round(value, 4)
            score += final_w[src] * value
        results.append(
            {
                "sender_id": sid,
                "sender_name": names.get(sid, sid),
                "score": round(score, 4),
                "breakdown": breakdown,
            }
        )
    results.sort(key=lambda x: (-x["score"], x["sender_id"]))
    return results[:top_k]


# ---------- 主入口 ----------


async def rank_influential(
    evidence: Any,
    context: Any,
    umo: str,
    top_k: int = 3,
    weights: dict | None = None,
) -> list[dict]:
    """计算群成员影响力排行 (多源融合)。

    Args:
        evidence: EvidenceBundle (stats / chronicle_msgs / biography_msgs)。
        context: AstrBot Context (用于反射定位软来源插件)。
        umo: unified_msg_origin 会话标识。
        top_k: 返回前 N 名。
        weights: {lm, sl, ra} 权重, 缺省 DEFAULT_WEIGHTS;
            存活来源按比例重归一化 (来源缺失时)。

    Returns:
        按 score 降序的 [{sender_id, sender_name, score, breakdown}] 列表;
        全部来源缺失时返回 []。
    """
    group_id = extract_group_id(umo)
    lm = _lm_source(evidence)
    sl = await _sl_source(context, group_id)
    ra = await _ra_source(context, group_id)
    if lm is None and sl is None and ra is None:
        logger.info("影响力排行: 所有来源均缺失, 返回空列表")
        return []
    return _fuse(evidence, {"lm": lm, "sl": sl, "ra": ra}, weights, top_k)


async def relation_pairs(
    context: Any, umo: str, top_k: int = 5, window_days: int = 7
) -> list[tuple[str, str, float]] | None:
    """RA 软来源关系对: (用户A, 用户B, 关系强度) 按强度降序。

    只读关系分析插件的 relations.sqlite (与 _ra_sqlite 同一数据源),
    任一环节失败 (插件缺失 / 缺库 / 读取异常 / 无数据) → None,
    绝不抛异常, 由调用方决定是否展示「关系弧」分节。

    Args:
        context: AstrBot Context (与 _ra_source 语义一致, 当前只用于占位)。
        umo: unified_msg_origin 会话标识。
        top_k: 返回前 N 对。

    Returns:
        [(a, b, score)] 列表, 或 None (RA 不可用 / 无数据)。
    """
    group_id = extract_group_id(umo)
    return await asyncio.to_thread(_relation_pairs_sqlite, group_id, top_k, window_days)


def _relation_pairs_sqlite(group_id: str, top_k: int, window_days: int):
    """Read period-specific interaction evidence without blocking the event loop.

    Args:
        group_id: Relation analyzer group identifier.
        top_k: Maximum number of returned pairs.
        window_days: Number of calendar days to include.

    Returns:
        Recent relationship pairs, or None when the source is unavailable.
    """
    db_path = _resolve_ra_db_path()
    if db_path is None:
        logger.info("RA 关系对缺失: 无法解析插件数据目录")
        return None
    try:
        exists = db_path.exists()
    except Exception:  # noqa: BLE001
        return None
    if not exists:
        logger.info(f"RA 关系对缺失: 数据库不存在 ({db_path})")
        return None

    try:
        conn = sqlite3.connect(
            db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1
        )
        try:
            conn.row_factory = sqlite3.Row
            # 先按实际存在的表/列选择查询形态; schema 漂移时明确降级并一次性告警,
            # 而不是等 SELECT 抛异常后只留一条泛泛的「读取失败」。
            schema = _ra_read_schema(conn)
            daily_missing = _ra_missing_columns(
                schema, "relation_daily", _RA_DAILY_COLUMNS
            )
            relations_missing = _ra_missing_columns(
                schema, "relations", _RA_PAIRS_RELATION_COLUMNS
            )
            if daily_missing == []:
                since = (
                    date.today() - timedelta(days=max(1, int(window_days)) - 1)
                ).isoformat()
                rows = conn.execute(
                    "SELECT a,b,SUM(evidence) AS score FROM relation_daily "
                    "WHERE group_id=? AND day>=? AND day<=? GROUP BY a,b HAVING SUM(evidence)>0 "
                    "ORDER BY score DESC,a,b LIMIT ?",
                    (group_id, since, date.today().isoformat(), max(1, int(top_k))),
                ).fetchall()
            elif relations_missing == []:
                if daily_missing:
                    # relation_daily 在但缺列 -> 明确降级到 relations (按 last_ts)
                    _ra_warn_schema(
                        db_path,
                        "relation_daily",
                        _RA_DAILY_COLUMNS,
                        schema,
                        consequence="关系弧本次改用 relations 表(按 last_ts 统计)。",
                    )
                rows = conn.execute(
                    "SELECT a,b,score FROM relations WHERE group_id=? AND last_ts>=? AND last_ts<=? "
                    "AND score>0 ORDER BY score DESC,a,b LIMIT ?",
                    (
                        group_id,
                        time.time() - max(1, int(window_days)) * 86400,
                        time.time(),
                        max(1, int(top_k)),
                    ),
                ).fetchall()
            else:
                # 两种形态都不可用。只对「存在但缺列」的表告警 —— relation_daily
                # 缺失属正常(老版本 RA 本就没有它), 而 relations 缺失说明连兜底形态都没有。
                if "relation_daily" in schema:
                    _ra_warn_schema(
                        db_path,
                        "relation_daily",
                        _RA_DAILY_COLUMNS,
                        schema,
                        consequence="relations 兜底形态也不可用, 本次不展示关系弧分节。",
                    )
                if "relations" in schema:
                    _ra_warn_schema(
                        db_path,
                        "relations",
                        _RA_PAIRS_RELATION_COLUMNS,
                        schema,
                        consequence="本次不展示关系弧分节。",
                    )
                else:
                    _warn_ra_once(
                        db_path,
                        "missing_table:relations",
                        f"RA 来源 schema 不兼容: 数据库缺少 relations 表 ({db_path}), "
                        f"且 relation_daily 也不满足查询要求; 该库可能不属于 "
                        f"group_relation_analyzer。本次不展示关系弧分节。",
                    )
                return None
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"RA 关系对读取失败: {exc}")
        return None

    pairs: list[tuple[str, str, float]] = []
    for row in rows:
        a = str(row["a"] or "")
        b = str(row["b"] or "")
        if not a or not b or _is_bot_id(a) or _is_bot_id(b):
            continue
        try:
            score = float(row["score"] or 0)
        except Exception:  # noqa: BLE001
            score = 0.0
        pairs.append((a, b, score))
    if not pairs:
        # 表与列都齐全却没拿到关系对 -> 「对方就是没有数据」, 与 schema 不兼容 WARNING 区分
        logger.info(
            f"RA 关系对: 表结构兼容, 但本群近 {max(1, int(window_days))} 天无可展示关系对 "
            f"(group_id={group_id})"
        )
        return None
    return pairs
