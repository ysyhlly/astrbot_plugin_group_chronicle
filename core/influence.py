"""群史记 influence —— 多源影响力排行。

融合三个来源计算群成员影响力, 返回按 score 降序的成员列表:

- LM: LivingMemory 留存消息数 + 活跃天数 (本插件 evidence, 始终尝试)
- SL: self_learning / 公开能力插件的社交图排行 (软来源)
- RA: group_relation_analyzer / 公开能力插件的关系中心度 (软来源)

软来源 = 插件缺失 / 未启用 / 调用异常 / 数据为空 时视为「来源缺失」, 剩余来源的权重
按比例重归一化 (如仅剩 LM → lm=1.0), 绝不因单个来源故障影响整体排行。

设计约束:
- min-max 归一化时 max==min → 全员 1.0, 绝不除零 (单人群/全员等值均安全)。
- 不直接 import 第三方插件模块，也不直读第三方插件 SQLite 文件或做深层反射 BFS;
  仅通过 context 公开能力声明 (`get_dynamics_capabilities`) 与已声明公开方法探测,
  所有软来源访问均 try/except 包裹并优雅降级。
- 模块顶层不 import astrbot, 保证单元测试无需安装 astrbot 即可运行。
"""

import inspect
import logging
import time
from collections import Counter
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_WEIGHTS = {"lm": 0.5, "sl": 0.3, "ra": 0.2}


def _clear_probe_cache() -> None:
    """保留兼容入口（无跨重载持久实例缓存）。"""
    return None


# 软来源插件别名 (小写匹配)
_SL_ALIASES = ("astrbot_plugin_self_learning", "selflearning")
_RA_ALIASES = ("astrbot_plugin_group_relation_analyzer", "grouprelationanalyzer")

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


# ---------- 插件实例解析 (公开能力探测, 全程 guarded) ----------


def _plugin_capability_status(
    instance: Any, capability_names: tuple[str, ...]
) -> tuple[bool, bool]:
    """Return (has_manifest, matches_ready_capability) for ``instance``."""
    caps_fn = getattr(instance, "get_dynamics_capabilities", None)
    if not callable(caps_fn):
        return False, False
    try:
        manifest = caps_fn()
    except Exception:
        return True, False
    if not isinstance(manifest, dict):
        return True, False
    if manifest.get("enabled") is False:
        return True, False
    if "ready" in manifest and manifest.get("ready") is not True:
        return True, False
    caps = manifest.get("capabilities")
    if not isinstance(caps, dict):
        return True, False
    matched = any(bool(caps.get(name)) for name in capability_names)
    return True, matched


def _is_usable_instance(instance: Any, capability_names: tuple[str, ...]) -> bool:
    if instance is None:
        return False
    if (
        getattr(instance, "_terminating", False) is True
        or getattr(instance, "closed", False) is True
    ):
        return False
    has_manifest, matched = _plugin_capability_status(instance, capability_names)
    if has_manifest:
        return matched
    return True


def _resolve_star_instance(
    context: Any,
    aliases: tuple[str, ...],
    *,
    capability_names: tuple[str, ...] = (),
) -> Any | None:
    """定位已注册且启用的公开能力插件实例 (返回 star.star_cls 实例)。"""
    if context is None:
        return None
    wanted = {str(a).strip().lower() for a in aliases if str(a or "").strip()}

    all_getter = getattr(context, "get_all_stars", None)
    stars = None
    if callable(all_getter):
        try:
            stars = all_getter() or []
            if isinstance(stars, dict):
                stars = list(stars.values())
        except Exception:
            stars = None

    if capability_names and stars:
        for star in stars:
            if star is None or getattr(star, "activated", True) is False:
                continue
            instance = getattr(star, "star_cls", None)
            if instance is None:
                continue
            if (
                getattr(instance, "_terminating", False) is True
                or getattr(instance, "closed", False) is True
            ):
                continue
            has_manifest, matched = _plugin_capability_status(
                instance, capability_names
            )
            if has_manifest and matched:
                return instance

    getter = getattr(context, "get_registered_star", None)
    if callable(getter):
        for alias in aliases:
            try:
                star = getter(alias)
            except Exception:
                star = None
            if star is not None and getattr(star, "activated", True) is not False:
                instance = getattr(star, "star_cls", None)
                if _is_usable_instance(instance, capability_names):
                    return instance

    if not stars:
        return None

    for star in stars:
        if star is None or getattr(star, "activated", True) is False:
            continue
        instance = getattr(star, "star_cls", None)
        if not _is_usable_instance(instance, capability_names):
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


def _find_influence_analyzer(plugin: Any) -> Any | None:
    """在插件实例的公开接口上定位暴露 get_influence_ranking 的对象 (无深层 BFS)。"""
    if plugin is None:
        return None
    if callable(getattr(plugin, "get_influence_ranking", None)):
        return plugin

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
    if isinstance(services, dict):
        probes = [v for v in services.values() if v is not None]
    elif isinstance(services, (list, tuple, set)):
        probes = [v for v in services if v is not None]
    elif services is not None:
        probes = [
            getattr(services, name, None)
            for name in (
                "social",
                "influence",
                "social_graph",
                "social_graph_analyzer",
                "influence_analyzer",
                "social_analyzer",
            )
        ]
    else:
        probes = []

    for probe in probes:
        if probe is None:
            continue
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

    return None


# ---------- SL 来源 (公开影响力接口 / self_learning) ----------


async def _sl_source(context: Any, group_id: str) -> dict[str, float] | None:
    """SL 来源: 调公开影响力接口 get_influence_ranking, 分数 min-max。

    插件缺失 / 未就绪 / 调用异常 / 结果为空 → None (来源缺失)。
    """
    plugin = _resolve_star_instance(
        context, _SL_ALIASES, capability_names=("influence", "social")
    )
    analyzer = _find_influence_analyzer(plugin)
    if analyzer is None:
        logger.info("SL 来源缺失: 未定位到可用的社交影响力公开接口")
        return None
    method = getattr(analyzer, "get_influence_ranking", None)
    if not callable(method):
        logger.info("SL 来源缺失: 接口未暴露 get_influence_ranking")
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


# ---------- RA 来源 (关系分析公开能力接口) ----------


async def _ra_source(context: Any, group_id: str) -> dict[str, float] | None:
    """RA 来源: 仅通过公开能力探测与进程内接口查询; 不可用时优雅降级返回 None。"""
    plugin = _resolve_star_instance(
        context, _RA_ALIASES, capability_names=("relations", "social")
    )
    if plugin is None:
        return None
    values = await _ra_in_process(plugin, group_id)
    if not values:
        return None
    return _minmax(values)


async def _ra_in_process(plugin: Any, group_id: str) -> dict[str, float] | None:
    """调用关系/社交插件的公开查询接口 (不触碰磁盘 SQLite 文件)。"""
    methods = []
    for attr in ("get_relations_overview", "get_influence_ranking"):
        fn = getattr(plugin, attr, None)
        if callable(fn):
            methods.append(fn)
    store = getattr(plugin, "store", None)
    if store is not None:
        for attr in ("overview", "export_group"):
            fn = getattr(store, attr, None)
            if callable(fn):
                methods.append(fn)
    if not methods:
        return None

    result = None
    for method in methods:
        for attempt in (
            lambda m=method: m(group_id, top_limit=200, daily_decay=1.0),
            lambda m=method: m(group_id),
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
        if result is not None:
            break

    if isinstance(result, list):
        values: dict[str, float] = {}
        for item in result:
            if not isinstance(item, dict):
                continue
            uid = str(item.get("user_id", "") or "")
            if not uid or _is_bot_id(uid):
                continue
            try:
                values[uid] = float(item.get("score", item.get("pagerank", 0)) or 0)
            except Exception:
                continue
        return values or None
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


def _extract_relation_pairs(
    raw: Any, group_id: str, umo: str, top_k: int, window_days: int
) -> list[tuple[str, str, float]] | None:
    """Normalize relation pair structures returned by a public capability method."""
    if isinstance(raw, dict):
        items = None
        for key in ("pairs", "relations", "all_relations", "edges"):
            if key in raw and isinstance(raw[key], list):
                items = raw[key]
                break
        if items is None:
            return None
    elif isinstance(raw, list):
        items = raw
    else:
        return None

    cutoff = time.time() - max(1, int(window_days)) * 86400
    pairs: list[tuple[str, str, float]] = []
    for item in items:
        if isinstance(item, dict):
            row_gid = item.get("group_id")
            if row_gid is not None and str(row_gid) not in (group_id, umo):
                continue
            last_ts = item.get("last_ts")
            if last_ts is not None:
                try:
                    if float(last_ts) < cutoff:
                        continue
                except (TypeError, ValueError):
                    pass
            a = str(item.get("a") or item.get("src") or item.get("user_a") or "")
            b = str(item.get("b") or item.get("dst") or item.get("user_b") or "")
            raw_score = item.get("score", item.get("evidence", 0))
        elif isinstance(item, (tuple, list)) and len(item) >= 3:
            a = str(item[0] or "")
            b = str(item[1] or "")
            raw_score = item[2]
        else:
            continue
        if not a or not b or _is_bot_id(a) or _is_bot_id(b):
            continue
        try:
            score = float(raw_score or 0)
        except Exception:  # noqa: BLE001
            score = 0.0
        if score <= 0:
            continue
        pairs.append((a, b, score))

    if not pairs:
        return None
    pairs.sort(key=lambda row: (-row[2], row[0], row[1]))
    return pairs[: max(1, int(top_k))]


async def relation_pairs(
    context: Any, umo: str, top_k: int = 5, window_days: int = 7
) -> list[tuple[str, str, float]] | None:
    """RA 软来源关系对: (用户A, 用户B, 关系强度) 按强度降序。

    仅通过 context 公开能力与进程内查询接口读取, 不直读他插件 SQLite 文件;
    任一环节不可用 (插件缺失 / 未启用 / 调用异常 / 无数据) → None, 优雅降级。
    """
    group_id = extract_group_id(umo)
    plugin = _resolve_star_instance(
        context, _RA_ALIASES, capability_names=("relations", "social")
    )
    if plugin is None:
        return None

    methods = []
    for attr in ("get_relation_pairs", "get_relations_overview"):
        fn = getattr(plugin, attr, None)
        if callable(fn):
            methods.append((attr, fn))
    store = getattr(plugin, "store", None)
    if store is not None:
        for attr in ("overview", "export_group"):
            fn = getattr(store, attr, None)
            if callable(fn):
                methods.append((attr, fn))
    if not methods:
        return None

    limit = max(1, int(top_k))
    days = max(1, int(window_days))
    for name, method in methods:
        attempts = (
            (
                lambda m=method: m(umo=umo, top_k=limit, window_days=days),
                lambda m=method: m(group_id, top_k=limit, window_days=days),
                lambda m=method: m(group_id, limit, days),
                lambda m=method: m(group_id),
            )
            if name == "get_relation_pairs"
            else (
                lambda m=method: m(
                    group_id, top_limit=max(200, limit), daily_decay=1.0
                ),
                lambda m=method: m(umo, top_limit=max(200, limit)),
                lambda m=method: m(group_id),
            )
        )
        for attempt in attempts:
            try:
                raw = attempt()
                if inspect.isawaitable(raw):
                    raw = await raw
            except TypeError:
                continue
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"RA 关系对读取失败: {exc}")
                return None
            parsed = _extract_relation_pairs(raw, group_id, umo, limit, days)
            if parsed:
                return parsed
            break
    return None
