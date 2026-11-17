"""Tests for core/generation.py.

No AstrBot required: compress_evidence only touches EvidenceBundle-style
attributes, and generate_chronicle's LLM call is monkeypatched.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import generation
from core.generation import (
    NEUTRAL_HISTORIAN_PROMPT,
    _day_bucket_text,
    compress_evidence,
    generate_chronicle,
    generate_continuation,
)


class FakeMsg:
    def __init__(self, sender_name, timestamp, content, sender_id=None):
        self.sender_name = sender_name
        self.sender_id = sender_id if sender_id is not None else sender_name
        self.timestamp = timestamp
        self.content = content


class FakeEvidence:
    def __init__(self, msgs, memories=None, graph_snapshot=None):
        self.chronicle_msgs = msgs
        self.memories = memories or []
        self.graph_snapshot = graph_snapshot


def test_budget_and_per_sender_cap(monkeypatch):
    """(6) 500 条大消息 -> token 估算 <= 6000+margin; 每发送者最多 8 条。"""
    # 预算约束: 500 条 300 字符消息, 默认 6000 token 预算
    msgs = [
        FakeMsg(f"member_{i % 100}", 1700000000.0 + i, "汉" * 300) for i in range(500)
    ]
    compressed = compress_evidence(FakeEvidence(msgs))
    assert len(compressed) // 2 <= 6000 + 300

    # 每发送者上限: 给足预算, 5 个发送者 x 100 条 -> 每条 8 条封顶
    msgs2 = [
        FakeMsg(f"user_{i % 5}", 1700000000.0 + i, "内容" * 50) for i in range(500)
    ]
    compressed2 = compress_evidence(FakeEvidence(msgs2), max_tokens=1_000_000)
    lines = [ln for ln in compressed2.split("\n") if ln.startswith("[")]
    assert len(lines) == 5 * 8
    for sender in range(5):
        n = sum(1 for ln in lines if ln.startswith(f"[user_{sender}|"))
        assert n <= 8


def test_memories_section_presence(monkeypatch):
    """(7) 有记忆 -> 含「## 长期记忆片段」; 无记忆 -> 不含。"""
    msgs = [FakeMsg("a", 1700000000.0, "hello")]
    with_mem = FakeEvidence(msgs, memories=[{"text": "小明喜欢喝奶茶"}, {"text": "  "}])
    out = compress_evidence(FakeEvidence(msgs, memories=[{"text": "小明喜欢喝奶茶"}]))
    assert "## 长期记忆片段" in out
    assert "- 小明喜欢喝奶茶" in out

    without = compress_evidence(FakeEvidence(msgs, memories=[]))
    assert "## 长期记忆片段" not in without

    # 空文本记忆被跳过, 但其他记忆仍保留
    mixed = compress_evidence(with_mem)
    assert "## 长期记忆片段" in mixed
    assert "- 小明喜欢喝奶茶" in mixed


def test_graph_note_appended(monkeypatch):
    """(额外) graph_snapshot 存在 -> 含「## 图谱线索」; 缺失 -> 不含。"""
    msgs = [FakeMsg("a", 1700000000.0, "hello")]
    with_graph = FakeEvidence(
        msgs, graph_snapshot={"nodes": [1, 2, 3], "edges": {"a": "b"}}
    )
    out = compress_evidence(with_graph)
    assert "## 图谱线索" in out
    assert "- nodes: 3 / edges: 1" in out

    without = compress_evidence(FakeEvidence(msgs, graph_snapshot=None))
    assert "## 图谱线索" not in without

    # 非 dict 快照被守卫
    guarded = compress_evidence(FakeEvidence(msgs, graph_snapshot="oops"))
    assert "## 图谱线索" not in guarded


def test_generate_chronicle_builds_prompt_and_calls_llm(monkeypatch):
    """(8) generate_chronicle 组装含时间窗的 prompt, 以 person 的 system_prompt 调 call_llm。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["context"] = context
        captured["umo"] = umo
        captured["prompt"] = prompt
        captured["system_prompt"] = system_prompt
        return "本周群史记正文"

    monkeypatch.setattr(generation, "call_llm", fake_call_llm)

    msgs = [FakeMsg("小明", 1700000000.0, "今天真热闹")]
    evidence = FakeEvidence(msgs, memories=[{"text": "群友爱吃火锅"}])
    text = asyncio.run(
        generate_chronicle(
            "ctx", "umo:GroupMessage:123", evidence, "persona 人设提示词", 7
        )
    )

    assert text == "本周群史记正文"
    assert captured["umo"] == "umo:GroupMessage:123"
    assert "最近 7 天" in captured["prompt"]
    assert "今天真热闹" in captured["prompt"]
    assert "## 长期记忆片段" in captured["prompt"]
    assert captured["system_prompt"] == "persona 人设提示词"


def test_system_prompt_none_uses_neutral(monkeypatch):
    """(9) system_prompt=None -> 使用 NEUTRAL_HISTORIAN_PROMPT。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["system_prompt"] = system_prompt
        return "ok"

    monkeypatch.setattr(generation, "call_llm", fake_call_llm)
    msgs = [FakeMsg("小明", 1700000000.0, "hi")]
    asyncio.run(
        generate_chronicle("ctx", "umo:GroupMessage:123", FakeEvidence(msgs), None, 7)
    )
    assert captured["system_prompt"] == NEUTRAL_HISTORIAN_PROMPT


def test_long_message_truncated_to_200_chars(monkeypatch):
    """(10) 超长消息内容截断到 200 字符。"""
    long_content = "长" * 500
    msgs = [FakeMsg("小明", 1700000000.0, long_content)]
    out = compress_evidence(FakeEvidence(msgs), max_tokens=1_000_000)

    assert long_content[:200] in out
    assert long_content[200:] not in out
    # 格式: [昵称|时间] 内容
    assert out.startswith("[小明|")


def test_timestamp_format_and_order(monkeypatch):
    """(额外) 时间格式 %m-%d %H:%M 且消息按时间升序输出。"""
    from datetime import datetime

    base = 1700000000.0  # 2023-11-14
    # evidence 契约: chronicle_msgs 时间升序 (见 core/evidence.py)
    msgs = [
        FakeMsg("乙", base, "earlier"),
        FakeMsg("甲", base + 100, "later"),
    ]
    out = compress_evidence(FakeEvidence(msgs), max_tokens=1_000_000)
    expected_earlier = datetime.fromtimestamp(base).strftime("%m-%d %H:%M")
    expected_later = datetime.fromtimestamp(base + 100).strftime("%m-%d %H:%M")
    lines = [ln for ln in out.split("\n") if ln.startswith("[")]
    assert lines[0] == f"[乙|{expected_earlier}] earlier"
    assert lines[1] == f"[甲|{expected_later}] later"


# ---------------------------------------------------------------------------
# ABC 批次: 文风 / 自定义提示词 / 两阶段 / 候选金句 / 续写
# ---------------------------------------------------------------------------


def _capture_llm(monkeypatch):
    captured = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured.append({"prompt": prompt, "system_prompt": system_prompt})
        return "ok"

    monkeypatch.setattr(generation, "call_llm", fake_call_llm)
    return captured


def test_style_appended_to_system_prompt(monkeypatch):
    """style=gossip → 系统提示词追加八卦文风指令。"""
    captured = _capture_llm(monkeypatch)
    msgs = [FakeMsg("小明", 1700000000.0, "hi")]
    asyncio.run(
        generate_chronicle("ctx", "umo:1", FakeEvidence(msgs), None, 7, style="gossip")
    )
    sp = captured[0]["system_prompt"]
    assert sp.startswith(NEUTRAL_HISTORIAN_PROMPT)
    assert "八卦" in sp


def test_style_unknown_ignored(monkeypatch):
    """未知文风 → 系统提示词保持原样。"""
    captured = _capture_llm(monkeypatch)
    msgs = [FakeMsg("小明", 1700000000.0, "hi")]
    asyncio.run(
        generate_chronicle(
            "ctx", "umo:1", FakeEvidence(msgs), "自定义", 7, style="weird"
        )
    )
    assert captured[0]["system_prompt"] == "自定义"


def test_prompt_override_wins_over_persona(monkeypatch):
    """prompt_override 非空 → 优先作为系统提示词。"""
    captured = _capture_llm(monkeypatch)
    msgs = [FakeMsg("小明", 1700000000.0, "hi")]
    asyncio.run(
        generate_chronicle(
            "ctx",
            "umo:1",
            FakeEvidence(msgs),
            "persona提示词",
            7,
            prompt_override="我的自定义史官提示词",
            style="classical",
        )
    )
    sp = captured[0]["system_prompt"]
    assert sp.startswith("我的自定义史官提示词")
    assert "文言" in sp


def test_two_phase_calls_llm_twice(monkeypatch):
    """enable_two_phase=true → 先按天压缩, 再据要点成文 (两次 LLM 调用)。"""
    captured = _capture_llm(monkeypatch)
    msgs = [
        FakeMsg("小明", 1700000000.0, "今天真热闹"),
        FakeMsg("小红", 1700000100.0, "明天聚餐"),
    ]
    asyncio.run(
        generate_chronicle(
            "ctx", "umo:1", FakeEvidence(msgs), None, 7, enable_two_phase=True
        )
    )
    assert len(captured) == 2
    assert "按天压缩" in captured[0]["prompt"]
    assert "请基于要点生成群史记" in captured[1]["prompt"]


def test_two_phase_disabled_single_call(monkeypatch):
    captured = _capture_llm(monkeypatch)
    msgs = [FakeMsg("小明", 1700000000.0, "hi")]
    asyncio.run(
        generate_chronicle(
            "ctx", "umo:1", FakeEvidence(msgs), None, 7, enable_two_phase=False
        )
    )
    assert len(captured) == 1


def test_golden_quotes_injected(monkeypatch):
    """候选金句 (长度 8~80) → prompt 含「候选金句」分节。"""
    captured = _capture_llm(monkeypatch)
    msgs = [
        FakeMsg("小明", 1700000000.0, "今天这个梗真的太好笑了"),
        FakeMsg("小红", 1700000100.0, "短"),
    ]
    asyncio.run(generate_chronicle("ctx", "umo:1", FakeEvidence(msgs), None, 7))
    prompt = captured[0]["prompt"]
    assert "## 候选金句" in prompt
    # 金句分节内只有合格候选, 不含被过滤的短句
    quotes_section = prompt.split("## 候选金句", 1)[1]
    assert "今天这个梗真的太好笑了" in quotes_section
    assert "短" not in quotes_section


def test_golden_quotes_disabled(monkeypatch):
    captured = _capture_llm(monkeypatch)
    msgs = [FakeMsg("小明", 1700000000.0, "这是一条足够长的精彩言论内容")]
    asyncio.run(
        generate_chronicle(
            "ctx", "umo:1", FakeEvidence(msgs), None, 7, enable_golden_quotes=False
        )
    )
    assert "候选金句" not in captured[0]["prompt"]


def test_generate_continuation_uses_previous(monkeypatch):
    """续写: prompt 含上一期史记背景 + 新证据。"""
    captured = _capture_llm(monkeypatch)
    msgs = [FakeMsg("小明", 1700000000.0, "新消息内容")]
    asyncio.run(
        generate_continuation(
            "ctx",
            "umo:1",
            "上一期史记正文",
            FakeEvidence(msgs),
            None,
            7,
            style="official",
        )
    )
    prompt = captured[0]["prompt"]
    assert "上一期史记正文" in prompt
    assert "新消息内容" in prompt
    assert "续篇" in prompt


def test_day_bucket_groups_by_day():
    base = 1700000000.0
    msgs = [
        FakeMsg("甲", base, "第一天消息"),
        FakeMsg("乙", base + 100, "第一天另一条"),
        FakeMsg("甲", base + 86400, "第二天消息"),
    ]
    buckets = _day_bucket_text(FakeEvidence(msgs))
    lines = buckets.split("\n")
    assert len(lines) == 2
    assert "第一天消息" in lines[0] and "第一天另一条" in lines[0]
    assert "第二天消息" in lines[1]
