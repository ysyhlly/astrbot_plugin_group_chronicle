"""biography 模块单元测试 —— 仅依赖标准库 + pytest, 无需安装 astrbot。

覆盖:
1. 窗口过滤 + 时间戳格式化的发言行
2. 长期记忆按 sender_name / sender_id 过滤
3. 无发言记录: build 返回 None, generate 返回友好提示且不调用 LLM
4. generate 调用 call_llm(monkeypatched) 并返回其文本
5. system_prompt 为 None 时使用中立传记作者提示词
6. 每成员发言上限 8 条 + 内容截断 200 字
7. LLMUnavailableError 从 call_llm 正常向上传播(不吞掉)

运行: python -m pytest tests/test_biography.py -v
"""

import asyncio
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core.biography as bio_module
from core.biography import (
    BIO_PROMPT_TEMPLATE,
    NEUTRAL_BIOGRAPHER_PROMPT,
    build_biography_prompt,
    generate_biography,
)
from core.evidence import EvidenceBundle
from core.llm_call import LLMUnavailableError


# ---------- 假对象 ----------


@dataclass
class FakeMessage:
    """仿照 LivingMemory Message 的最小消息对象 (biography 需要的最小字段)。"""

    sender_id: str
    sender_name: str | None = None
    timestamp: float = 0.0
    content: str = ""


def make_bundle(msgs, memories=None):
    return EvidenceBundle(biography_msgs=msgs, memories=memories or [])


def run(coro):
    return asyncio.run(coro)


MEMBER_XIAOMING = {"sender_id": "u1", "sender_name": "小明"}


# ---------- 1. 窗口过滤 + 格式化 ----------


def test_prompt_contains_window_filtered_formatted_msgs():
    now = time.time()
    in_win = FakeMessage(
        sender_id="u1", sender_name="小明", timestamp=now - 60, content="今天好开心"
    )
    out_win = FakeMessage(
        sender_id="u1",
        sender_name="小明",
        timestamp=now - 30 * 86400,
        content="一个月前的发言",
    )
    prompt = run(
        build_biography_prompt(MEMBER_XIAOMING, make_bundle([in_win, out_win]), 7)
    )

    assert prompt is not None
    # 窗口内消息包含, 窗口外消息排除
    assert "小明" in prompt
    assert "今天好开心" in prompt
    assert "一个月前的发言" not in prompt
    # 格式: [{sender_name}|{MM-DD HH:MM}] {content}
    expected_time = datetime.fromtimestamp(in_win.timestamp).strftime("%m-%d %H:%M")
    assert f"[小明|{expected_time}] 今天好开心" in prompt
    # 以模板 + 成员名开头
    assert prompt.startswith(BIO_PROMPT_TEMPLATE.format(name="小明"))


# ---------- 2. 记忆过滤 ----------


def test_memories_filtered_by_sender_name_or_id():
    now = time.time()
    msgs = [
        FakeMessage(
            sender_id="u1", sender_name="小明", timestamp=now - 10, content="hi"
        )
    ]
    memories = [
        {"text": "小明喜欢喝奶茶"},
        {"text": "小红喜欢爬山"},  # 不含小明 / u1 → 排除
        {"text": "u1 经常深夜上线"},
    ]
    prompt = run(
        build_biography_prompt(MEMBER_XIAOMING, make_bundle(msgs, memories), 7)
    )

    assert prompt is not None
    assert "小明喜欢喝奶茶" in prompt
    assert "u1 经常深夜上线" in prompt
    assert "小红喜欢爬山" not in prompt


def test_matches_sender_name_when_id_mismatch():
    now = time.time()
    msgs = [
        FakeMessage(
            sender_id="other_id",
            sender_name="小明",
            timestamp=now - 5,
            content="名字匹配也行",
        )
    ]
    prompt = run(build_biography_prompt(MEMBER_XIAOMING, make_bundle(msgs), 7))
    assert prompt is None


# ---------- 3. 无发言记录: 不调用 LLM ----------


def test_no_msgs_returns_none_and_no_llm_call(monkeypatch):
    bundle = make_bundle([])
    member = {"sender_id": "u9", "sender_name": "路人"}
    assert run(build_biography_prompt(member, bundle, 7)) is None

    called = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        called.append((context, umo, prompt, system_prompt))
        return "不应被调用"

    monkeypatch.setattr(bio_module, "call_llm", fake_call_llm)
    result = run(generate_biography(SimpleNamespace(), "umo", bundle, member, None, 7))
    assert result == "该成员在窗口内无发言记录，暂无法生成传记。"
    assert called == []  # LLM 未被调用


# ---------- 4. 正常生成: 调用 call_llm 并返回其文本 ----------


def test_generate_calls_call_llm_and_returns_text(monkeypatch):
    now = time.time()
    msgs = [
        FakeMessage(
            sender_id="u1", sender_name="小明", timestamp=now - 5, content="你好"
        )
    ]
    bundle = make_bundle(msgs)
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        captured["sp"] = system_prompt
        return "这是传记正文"

    monkeypatch.setattr(bio_module, "call_llm", fake_call_llm)
    result = run(
        generate_biography(
            SimpleNamespace(), "g:1", bundle, MEMBER_XIAOMING, "自定义史官", 7
        )
    )

    assert result == "这是传记正文"
    assert captured["prompt"].startswith(BIO_PROMPT_TEMPLATE.format(name="小明"))
    assert "你好" in captured["prompt"]


# ---------- 5. system_prompt None -> 中立传记作者 ----------


def test_default_neutral_biographer_prompt(monkeypatch):
    now = time.time()
    msgs = [
        FakeMessage(sender_id="u1", sender_name="小明", timestamp=now - 5, content="hi")
    ]
    bundle = make_bundle(msgs)
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["sp"] = system_prompt
        return "ok"

    monkeypatch.setattr(bio_module, "call_llm", fake_call_llm)
    run(generate_biography(SimpleNamespace(), "umo", bundle, MEMBER_XIAOMING, None, 7))
    assert captured["sp"] == NEUTRAL_BIOGRAPHER_PROMPT


# ---------- 6. 每成员上限 8 条 + 截断 200 字 ----------


def test_cap_8_and_truncation_200():
    now = time.time()
    msgs = [
        FakeMessage(
            sender_id="u1",
            sender_name="小明",
            timestamp=now - 1000 + i,
            content=f"消息{i:02d}",
        )
        for i in range(12)
    ]
    # 长消息插在中间(now-995), 属于"最近 8 条"之列
    long_msg = FakeMessage(
        sender_id="u1", sender_name="小明", timestamp=now - 995, content="长" * 300
    )
    prompt = run(
        build_biography_prompt(MEMBER_XIAOMING, make_bundle(msgs + [long_msg]), 7)
    )

    assert prompt is not None
    # 13 条按时间升序后取最近 8 条: 消息05~消息11 + 长消息 → 消息04 及更早被排除
    assert "消息04" not in prompt
    assert "消息05" in prompt
    assert "消息11" in prompt
    # 截断: 只保留前 200 字符
    assert "长" * 200 in prompt
    assert ("长" * 201) not in prompt
    assert ("长" * 300) not in prompt


# ---------- 7. LLMUnavailableError 正常传播 ----------


def test_llm_unavailable_propagates(monkeypatch):
    now = time.time()
    msgs = [
        FakeMessage(sender_id="u1", sender_name="小明", timestamp=now - 5, content="hi")
    ]
    bundle = make_bundle(msgs)

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        raise LLMUnavailableError("未配置可用的 LLM")

    monkeypatch.setattr(bio_module, "call_llm", fake_call_llm)
    with pytest.raises(LLMUnavailableError):
        run(
            generate_biography(
                SimpleNamespace(), "umo", bundle, MEMBER_XIAOMING, None, 7
            )
        )
