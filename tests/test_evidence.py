"""EvidenceBundle / gather_evidence 单元测试 —— 仅依赖标准库, 无需 astrbot。

使用假 engine / conv_manager 对象, 验证时间窗过滤、用户消息排除、
异常降级与 NoActivityError 抛出。

运行: python -m pytest tests/test_evidence.py -v
"""

import asyncio
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.evidence import NoActivityError, gather_evidence


# ---------- 假对象 ----------


@dataclass
class FakeMessage:
    """仿照 LivingMemory Message 的最小消息对象。"""

    id: int
    session_id: str
    role: str
    content: str
    sender_id: str
    sender_name: str | None = None
    group_id: str | None = None
    platform: str | None = None
    timestamp: float = 0.0
    metadata: dict = field(default_factory=dict)


def msg(ts: float, role="user", sender_id="u1", is_bot=False, content="hi"):
    return FakeMessage(
        id=int(ts),
        session_id="umotest",
        role=role,
        content=content,
        sender_id=sender_id,
        timestamp=ts,
        metadata={"is_bot_message": True} if is_bot else {},
    )


class FakeGraphStore:
    def __init__(self, snapshot=None, raise_on_call=False):
        self._snapshot = snapshot
        self.raise_on_call = raise_on_call
        self.calls = []

    async def get_graph_snapshot(
        self, session_id=None, persona_id=None, limit_memories=12, **kwargs
    ):
        self.calls.append((session_id, limit_memories))
        if self.raise_on_call:
            raise RuntimeError("graph boom")
        return self._snapshot


class FakeEngine:
    def __init__(self, memories=None, graph_store=None, raise_memories=False):
        self._memories = memories if memories is not None else []
        self.graph_store = graph_store
        self.raise_memories = raise_memories
        self.memory_calls = []

    async def get_session_memories(self, session_id, limit=50):
        self.memory_calls.append((session_id, limit))
        if self.raise_memories:
            raise RuntimeError("memory boom")
        return self._memories


class FakeStore:
    def __init__(self, stats=None, raise_stats=False):
        self._stats = stats if stats is not None else {}
        self.raise_stats = raise_stats

    async def get_user_message_stats(self, session_id):
        if self.raise_stats:
            raise RuntimeError("stats boom")
        return self._stats


class FakeConvManager:
    def __init__(self, msgs=None, store=None, raise_get=False, returns_none=False):
        self._msgs = msgs if msgs is not None else []
        self.store = store
        self.raise_get = raise_get
        self.returns_none = returns_none
        self.get_calls = []

    async def get_messages(self, session_id, limit=50, sender_id=None, use_cache=True):
        self.get_calls.append((session_id, limit))
        if self.raise_get:
            raise RuntimeError("get boom")
        if self.returns_none:
            return None
        return self._msgs


async def _gather(engine, conv, **kw):
    defaults = dict(
        umo="umotest",
        chronicle_days=7,
        biography_days=30,
        max_msgs=50,
        max_memories=20,
        min_activity_messages=1,
    )
    defaults.update(kw)
    return await gather_evidence(engine, conv, **defaults)


# ---------- 测试 ----------


def test_window_filter_and_user_exclusion():
    """1d/10d/40d 用户消息 + assistant + 机器人消息 → 窗口过滤与排除正确。"""
    now = time.time()
    day = 86400
    t_1d, t_10d, t_40d = now - 1 * day, now - 10 * day, now - 40 * day
    msgs = [
        msg(t_1d, sender_id="u1"),
        msg(t_10d, sender_id="u2"),
        msg(t_40d, sender_id="u3"),
        msg(t_1d + 1, role="assistant"),
        msg(t_1d + 2, sender_id="bot", is_bot=True),
    ]
    engine, conv = make_env(msgs)
    bundle = asyncio.run(_gather(engine, conv))

    # chronicle_msgs 仅包含 ≤7d 的用户消息
    assert [m.sender_id for m in bundle.chronicle_msgs] == ["u1"]
    # biography_msgs 包含 ≤30d 的用户消息
    assert [m.sender_id for m in bundle.biography_msgs] == ["u1", "u2"]
    assert bundle.msg_count == 1
    # newest_ts = 最新用户消息时间戳
    assert bundle.newest_ts == t_1d
    # assistant 与 is_bot_message 消息全部被排除
    assert all(getattr(m, "role", "user") != "assistant" for m in bundle.biography_msgs)
    assert not any(
        (m.metadata or {}).get("is_bot_message") for m in bundle.biography_msgs
    )
    # 正确透传 umo 与 limit
    assert conv.get_calls == [("umotest", 50)]


def test_graph_store_none():
    """engine.graph_store = None → graph_snapshot None, 不抛异常。"""
    now = time.time()
    engine, conv = make_env([msg(now - 3600)], engine_kw={"graph_store": None})
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.graph_snapshot is None
    assert bundle.msg_count == 1


def test_graph_raises_degrades():
    """graph_store 调用抛异常 → graph_snapshot None, 不泄漏异常。"""
    now = time.time()
    engine, conv = make_env(
        [msg(now - 3600)],
        engine_kw={"graph_store": FakeGraphStore(raise_on_call=True)},
    )
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.graph_snapshot is None


def test_no_store_attr():
    """conv_manager 无 store 属性 → stats {}, 不抛异常。"""
    now = time.time()
    engine, conv = make_env([msg(now - 3600)])
    conv.store = None  # 移除 store 属性
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.stats == {"u1": 1}


def test_all_old_raises_no_activity():
    """全部消息早于 30d → NoActivityError, 消息含 数据不足/无活动。"""
    now = time.time()
    msgs = [msg(now - 35 * 86400), msg(now - 40 * 86400)]
    engine, conv = make_env(msgs)
    with pytest.raises(NoActivityError) as exc:
        asyncio.run(_gather(engine, conv))
    text = str(exc.value)
    assert "数据不足" in text or "无活动" in text


def test_get_messages_raises():
    """conv_manager.get_messages 抛异常 → NoActivityError。"""
    engine, conv = make_env([], conv_kw={"raise_get": True})
    with pytest.raises(NoActivityError):
        asyncio.run(_gather(engine, conv))


def test_get_messages_none():
    """conv_manager.get_messages 返回 None → NoActivityError。"""
    engine, conv = make_env([], conv_kw={"returns_none": True})
    with pytest.raises(NoActivityError):
        asyncio.run(_gather(engine, conv))


def test_coverage_fields():
    """coverage_start/end_ts 取 chronicle 窗口的最早/最晚时间戳。"""
    now = time.time()
    day = 86400
    t1, t2 = now - 1 * day, now - 2 * day
    engine, conv = make_env([msg(t2, sender_id="a"), msg(t1, sender_id="b")])
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.coverage_start_ts == t2  # 最早
    assert bundle.coverage_end_ts == t1  # 最晚
    assert bundle.msg_count == 2
    assert bundle.newest_ts == t1


def test_min_activity_messages():
    """min_activity_messages=3 且窗口内仅 2 条 → NoActivityError。"""
    now = time.time()
    msgs = [msg(now - 3600), msg(now - 7200)]
    engine, conv = make_env(msgs)
    with pytest.raises(NoActivityError):
        asyncio.run(_gather(engine, conv, min_activity_messages=3))


def test_memories_raise_degrades():
    """get_session_memories 抛异常 → memories [], 不泄漏异常。"""
    now = time.time()
    engine, conv = make_env([msg(now - 3600)], engine_kw={"raise_memories": True})
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.memories == []


def test_graph_memories_stats_success():
    """graph/memories/stats 正常返回 → 原样进入 bundle, 参数透传正确。"""
    now = time.time()
    snapshot = {"nodes": [], "edges": []}
    stats = {"u1": 42, "u2": 7}
    engine, conv = make_env(
        [msg(now - 3600, sender_id="u1")],
        engine_kw={
            "graph_store": FakeGraphStore(snapshot=snapshot),
            "memories": [{"id": 1, "text": "记忆"}],
        },
        conv_kw={"store": FakeStore(stats=stats)},
    )
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.graph_snapshot == snapshot
    assert bundle.memories == [{"id": 1, "text": "记忆"}]
    assert bundle.stats == {"u1": 1}
    # 参数透传: 会话 id 与 limit
    assert engine.graph_store.calls == [("umotest", 12)]
    assert engine.memory_calls == [("umotest", 20)]


def test_stats_raises_degrades():
    """get_user_message_stats 抛异常 → stats {}, 不泄漏异常。"""
    now = time.time()
    engine, conv = make_env(
        [msg(now - 3600)],
        conv_kw={"store": FakeStore(stats={"u1": 5}, raise_stats=True)},
    )
    bundle = asyncio.run(_gather(engine, conv))
    assert bundle.stats == {"u1": 1}


# ---------- 记忆搜索补充 (ABC 批次) ----------


class SearchableEngine(FakeEngine):
    """带 search_memories 的引擎替身 (返回 HybridResult 风格对象)。"""

    def __init__(self, memories=None, results=None, raise_search=False):
        super().__init__(memories=memories)
        self._results = results or []
        self.raise_search = raise_search
        self.search_calls = []

    async def search_memories(self, query, k=5, session_id=None, persona_id=None):
        self.search_calls.append((query, k, session_id))
        if self.raise_search:
            raise RuntimeError("search boom")
        return self._results


class HybridResult:
    """LivingMemory HybridResult 替身 (含 content 字段)。"""

    def __init__(self, content):
        self.content = content


def test_search_memories_merged():
    """engine 有 search_memories → 按查询词补充记忆并去重。"""
    now = time.time()
    engine = SearchableEngine(
        memories=[{"id": 1, "text": "已有记忆"}],
        results=[
            HybridResult("搜索到的金句记忆"),
            {"text": "已有记忆"},  # 与基础记忆重复 → 去重
            HybridResult("另一条搜索记忆"),
        ],
    )
    conv = FakeConvManager(msgs=[msg(now - 3600)])
    bundle = asyncio.run(_gather(engine, conv, max_memories=20))
    texts = [m.get("text") for m in bundle.memories]
    assert "搜索到的金句记忆" in texts
    assert "另一条搜索记忆" in texts
    assert texts.count("已有记忆") == 1  # 去重
    # 每次查询带 session_id=umo
    assert all(session_id == "umotest" for _, _, session_id in engine.search_calls)
    assert len(engine.search_calls) == 4  # 大事/冲突/梗/约定


def test_search_memories_cap():
    """搜索补充总量上限 = max_memories * 2。"""
    now = time.time()
    engine = SearchableEngine(
        memories=[],
        results=[HybridResult(f"记忆内容{i:02d}长度足够") for i in range(50)],
    )
    conv = FakeConvManager(msgs=[msg(now - 3600)])
    bundle = asyncio.run(_gather(engine, conv, max_memories=3))
    assert len(bundle.memories) <= 6


def test_search_memories_raises_degrades():
    """search_memories 抛异常 → 降级为普通记忆, 不泄漏异常。"""
    now = time.time()
    engine = SearchableEngine(
        memories=[{"id": 1, "text": "已有记忆"}], raise_search=True
    )
    conv = FakeConvManager(msgs=[msg(now - 3600)])
    bundle = asyncio.run(_gather(engine, conv))
    assert [m.get("text") for m in bundle.memories] == ["已有记忆"]


def test_search_memories_absent_skipped():
    """engine 无 search_memories → 原样返回基础记忆。"""
    now = time.time()
    engine = FakeEngine(memories=[{"id": 1, "text": "已有记忆"}])
    conv = FakeConvManager(msgs=[msg(now - 3600)])
    bundle = asyncio.run(_gather(engine, conv))
    assert [m.get("text") for m in bundle.memories] == ["已有记忆"]


# ---------- 工具 ----------


def make_env(msgs, engine_kw=None, conv_kw=None):
    engine = FakeEngine(**({} if engine_kw is None else engine_kw))
    conv = FakeConvManager(msgs=msgs, **({} if conv_kw is None else conv_kw))
    return engine, conv
