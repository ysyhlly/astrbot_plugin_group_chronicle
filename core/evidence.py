"""群史记 evidence —— 历史 + 记忆证据收集。

从 LivingMemory 提取群聊历史消息、长期记忆、图谱快照与成员消息统计,
供 chronicle(群周史记) 与 biography(成员传记) 生成使用。

设计约束:
- 所有 LivingMemory 调用均被 try/except 包裹; 失败时降级为空值或抛
  NoActivityError, 绝不向上泄漏 LivingMemory 原始异常。
- 时间过滤基于 Message.timestamp (epoch float)。
- 仅使用 conv_manager.get_messages (取最近 limit 条、时间升序);
  不使用基于索引的 get_messages_range。
"""

import inspect
import math
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from astrbot.api import logger

#: 记忆搜索查询词 (enable_memory_search 的搜索主题, 守卫调用)
_MEMORY_SEARCH_QUERIES = ("大事", "冲突", "梗", "约定")
#: 记忆搜索单查询返回条数
_MEMORY_SEARCH_K = 10
#: 搜索合并后 memories 总条数上限 (基础 + 搜索) 的倍数
_MEMORY_SEARCH_CAP_FACTOR = 2


class NoActivityError(Exception):
    """群内在时间窗内无足够活动(或数据不足), 无法生成史记。"""


@dataclass
class EvidenceBundle:
    """gather_evidence 的收集结果, 供 render/chronicle 生成使用。"""

    chronicle_msgs: list = field(default_factory=list)  # chronicle 时间窗内的用户消息
    biography_msgs: list = field(default_factory=list)  # biography 时间窗内的用户消息
    memories: list = field(default_factory=list)  # 长期记忆 (原始 dict 列表)
    graph_snapshot: dict | None = None  # 图谱快照 (可能为 None)
    stats: dict = field(default_factory=dict)  # sender_id -> 消息数
    newest_ts: float = 0.0  # 拉取到的全部用户消息中最新时间戳
    coverage_start_ts: float = 0.0  # chronicle 窗口最早消息时间戳
    coverage_end_ts: float = 0.0  # chronicle 窗口最晚消息时间戳
    msg_count: int = 0  # len(chronicle_msgs)


def _is_user_msg(m: Any) -> bool:
    """用户消息判定: role 非 assistant 且未标记为机器人消息。"""
    if getattr(m, "role", "user") == "assistant":
        return False
    metadata = getattr(m, "metadata", None) or {}
    if not isinstance(metadata, dict):
        metadata = {}
    if metadata.get("is_bot_message", False):
        return False
    return True


def _result_text(result: Any) -> str:
    """从搜索结果的单个条目提取文本 (HybridResult.content / dict.text / dict.content)。"""
    if isinstance(result, dict):
        raw = result.get("text") or result.get("content") or ""
    else:
        raw = getattr(result, "content", None) or ""
    return str(raw).strip()


async def _merge_search_memories(
    engine: Any, umo: str, memories: list, max_memories: int
) -> list:
    """用 memory_engine.search_memories 补充记忆 (守卫, 失败静默降级)。

    - 仅当 engine 暴露 search_memories 时尝试;
    - 逐查询守卫, 单查询失败不影响其他查询;
    - 与现有记忆按文本去重; 总量上限 max_memories * _MEMORY_SEARCH_CAP_FACTOR。
    """
    search = getattr(engine, "search_memories", None)
    if not callable(search):
        return memories

    seen = {str(m.get("text", "")).strip() for m in memories if isinstance(m, dict)}
    cap = max_memories * _MEMORY_SEARCH_CAP_FACTOR
    try:
        for query in _MEMORY_SEARCH_QUERIES:
            try:
                results = await search(query=query, k=_MEMORY_SEARCH_K, session_id=umo)
            except Exception as e:  # noqa: BLE001 - 单查询失败不影响整体
                logger.warning(f"记忆搜索失败 (query={query!r}): {e}")
                continue
            for result in results or []:
                text = _result_text(result)
                if not text or text in seen:
                    continue
                memories.append({"text": text, "source": "search"})
                seen.add(text)
                if len(memories) >= cap:
                    return memories
    except Exception as e:  # noqa: BLE001 - 整体失败静默降级
        logger.warning(f"记忆搜索整体失败 (umo={umo}): {e}")
    return memories


async def gather_evidence(
    engine: Any,
    conv_manager: Any,
    umo: str,
    chronicle_days: int,
    biography_days: int,
    max_msgs: int,
    max_memories: int,
    min_activity_messages: int = 1,
) -> EvidenceBundle:
    """收集群聊历史与记忆证据。

    Args:
        engine: LivingMemory MemoryEngine 实例。
        conv_manager: LivingMemory ConversationManager 实例。
        umo: unified_msg_origin 会话标识。
        chronicle_days: 周史记时间窗(天)。
        biography_days: 成员传记时间窗(天)。
        max_msgs: 拉取的最大消息条数。
        max_memories: 拉取的最大记忆条数。
        min_activity_messages: chronicle 窗口内最少用户消息数,
            不足则抛 NoActivityError。

    Returns:
        EvidenceBundle。

    Raises:
        NoActivityError: 无法获取历史, 或时间窗内活动不足。
    """
    now = time.time()

    # 1. 获取最近 max_msgs 条消息 (LivingMemory 返回时间升序、最近 limit 条)
    try:
        get_messages = conv_manager.get_messages
        parameters = inspect.signature(get_messages).parameters
        kwargs = {"limit": max(1, int(max_msgs))}
        if "use_cache" in parameters or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()
        ):
            kwargs["use_cache"] = False
        msgs = await get_messages(umo, **kwargs)
    except Exception as e:
        logger.warning(f"获取群聊历史失败 (umo={umo}): {e}")
        raise NoActivityError("无法获取群聊历史(LivingMemory 会话数据异常)") from e
    if msgs is None:
        raise NoActivityError("无法获取群聊历史(LivingMemory 会话数据异常)")

    # 2. 过滤出用户消息 (排除 assistant 与机器人消息)
    user_msgs = []
    for message in msgs:
        if not _is_user_msg(message):
            continue
        try:
            timestamp = float(message.timestamp)
        except (AttributeError, TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(timestamp) and 0 < timestamp <= now:
            user_msgs.append(message)

    # 3-4. 按时间窗过滤
    chronicle_msgs = [
        m
        for m in user_msgs
        if 0 <= (now - float(m.timestamp)) <= chronicle_days * 86400
    ]
    biography_msgs = [
        m
        for m in user_msgs
        if 0 <= (now - float(m.timestamp)) <= biography_days * 86400
    ]

    # 5. 全部用户消息中最新时间戳
    newest_ts = max((float(m.timestamp) for m in user_msgs), default=0.0)

    # 6. 时间窗内活动不足 → 抛 NoActivityError (只抛, 不标记)
    if len(chronicle_msgs) < min_activity_messages:
        raise NoActivityError(
            f"该群在时间窗内无足够活动/数据不足(仅 {len(chronicle_msgs)} 条)"
        )

    # 7. 长期记忆 (失败降级为空列表)
    try:
        memories = await engine.get_session_memories(umo, limit=max_memories)
    except Exception as e:
        logger.warning(f"获取会话记忆失败 (umo={umo}): {e}")
        memories = []
    if memories is None:
        memories = []

    # 7b. 记忆搜索补充 (engine.search_memories 存在时, 全程守卫; 失败静默降级)
    memories = await _merge_search_memories(engine, umo, memories, max_memories)

    # 8. 图谱快照 (无 graph_store 或失败时降级为 None)
    graph_snapshot = None
    gs = getattr(engine, "graph_store", None)
    if gs is not None:
        try:
            graph_snapshot = await gs.get_graph_snapshot(
                session_id=umo, limit_memories=12
            )
        except Exception as e:
            logger.warning(f"获取图谱快照失败 (umo={umo}): {e}")
            graph_snapshot = None

    # Count only messages belonging to the requested period.
    stats = dict(
        Counter(
            str(m.sender_id) for m in chronicle_msgs if getattr(m, "sender_id", None)
        )
    )

    # 10. coverage 字段 (chronicle 窗口的最小/最大时间戳)
    if chronicle_msgs:
        coverage_start_ts = min(float(m.timestamp) for m in chronicle_msgs)
        coverage_end_ts = max(float(m.timestamp) for m in chronicle_msgs)
    else:
        coverage_start_ts = 0.0
        coverage_end_ts = 0.0

    return EvidenceBundle(
        chronicle_msgs=chronicle_msgs,
        biography_msgs=biography_msgs,
        memories=memories,
        graph_snapshot=graph_snapshot,
        stats=stats,
        newest_ts=newest_ts,
        coverage_start_ts=coverage_start_ts,
        coverage_end_ts=coverage_end_ts,
        msg_count=len(chronicle_msgs),
    )
