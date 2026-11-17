"""Tests for core/llm_call.py.

No AstrBot installation required: call_llm uses only attribute guards,
so a fake context drives every path. asyncio.sleep is monkeypatched in
retry tests to keep them fast.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.llm_call as llm_call
from core.llm_call import LLMUnavailableError, call_llm


class FakeResp:
    def __init__(self, text):
        self.completion_text = text


class FakeProvider:
    def __init__(self, result=None, raise_on_chat=False):
        self._result = result
        self._raise_on_chat = raise_on_chat
        self.chat_calls = 0

    async def text_chat(self, prompt, system_prompt=None):
        self.chat_calls += 1
        if self._raise_on_chat:
            raise RuntimeError("text_chat boom")
        return self._result


class FakeContext:
    def __init__(self):
        self.generate_calls = []
        self.pid = "provider-1"
        self.gen_result = None
        self.gen_raise = False
        self.fallback_provider = None
        self.has_new_api = True
        self.using_provider_raise = False

    async def get_current_chat_provider_id(self, umo=None):
        if not self.has_new_api:
            raise AttributeError("get_current_chat_provider_id 不存在")
        if self.pid == "raise":
            raise RuntimeError("pid boom")
        return self.pid

    async def llm_generate(
        self, chat_provider_id=None, prompt=None, system_prompt=None
    ):
        self.generate_calls.append(
            {
                "chat_provider_id": chat_provider_id,
                "prompt": prompt,
                "system_prompt": system_prompt,
            }
        )
        if self.gen_raise:
            raise RuntimeError("llm_generate boom")
        return self.gen_result

    def get_using_provider(self, umo=None):
        if self.using_provider_raise:
            raise RuntimeError("get_using_provider boom")
        return self.fallback_provider


class NoNewApiContext:
    """缺少 get_current_chat_provider_id / llm_generate 的旧版上下文。"""

    def __init__(self, provider):
        self._provider = provider

    def get_using_provider(self, umo=None):
        return self._provider


async def _no_sleep(delay):
    return None


def test_new_api_returns_completion_text(monkeypatch):
    """(1) get_current_chat_provider_id + llm_generate -> completion_text="X" -> "X"."""
    ctx = FakeContext()
    ctx.gen_result = FakeResp("X")
    ctx.fallback_provider = FakeProvider(result=FakeResp("should-not-be-used"))

    text = asyncio.run(call_llm(ctx, "umo:GroupMessage:1", "hello", "sys"))

    assert text == "X"
    assert len(ctx.generate_calls) == 1
    assert ctx.generate_calls[0]["chat_provider_id"] == "provider-1"
    assert ctx.generate_calls[0]["prompt"] == "hello"
    assert ctx.generate_calls[0]["system_prompt"] == "sys"
    assert ctx.fallback_provider.chat_calls == 0


def test_none_response_falls_back_to_text_chat(monkeypatch):
    """(2) llm_generate 返回 None -> 回退 get_using_provider.text_chat -> "Y"."""
    ctx = FakeContext()
    ctx.gen_result = None
    ctx.fallback_provider = FakeProvider(result=FakeResp("Y"))

    text = asyncio.run(call_llm(ctx, "umo:GroupMessage:1", "hello"))

    assert text == "Y"
    assert ctx.fallback_provider.chat_calls == 1


def test_both_paths_fail_raises_unavailable(monkeypatch):
    """(3) 全部路径失败 -> LLMUnavailableError, 消息含「未配置可用的 LLM」且不泄漏内部异常。"""
    monkeypatch.setattr(llm_call.asyncio, "sleep", _no_sleep)
    ctx = FakeContext()
    ctx.gen_raise = True
    ctx.fallback_provider = FakeProvider(raise_on_chat=True)

    try:
        asyncio.run(call_llm(ctx, "umo:GroupMessage:1", "hello"))
        assert False, "应当抛出 LLMUnavailableError"
    except LLMUnavailableError as e:
        assert "未配置可用的 LLM" in str(e)
        # 内部异常不得泄漏到对外错误消息
        assert "boom" not in str(e)
        assert "llm_generate" not in str(e)
        assert "text_chat" not in str(e)
        # 重试确实发生了: 每个路径各调用 2 次
        assert len(ctx.generate_calls) == 2
        assert ctx.fallback_provider.chat_calls == 2


def test_missing_new_api_falls_to_fallback(monkeypatch):
    """(4) 上下文无 get_current_chat_provider_id -> 直接走回退路径。"""
    provider = FakeProvider(result=FakeResp("Y"))
    ctx = NoNewApiContext(provider)

    text = asyncio.run(call_llm(ctx, "umo:GroupMessage:1", "hello"))

    assert text == "Y"
    assert provider.chat_calls == 1


def test_retry_after_transient_failure(monkeypatch):
    """(5) 首次调用抛异常, 重试后成功 -> 返回文本 (asyncio.sleep(1) 被替换)。"""
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(llm_call.asyncio, "sleep", fake_sleep)
    ctx = FakeContext()
    ctx.fallback_provider = FakeProvider(raise_on_chat=True)

    async def flaky_generate(**kwargs):
        ctx.generate_calls.append(kwargs)
        if len(ctx.generate_calls) == 1:
            raise RuntimeError("transient boom")
        return FakeResp("retried-ok")

    ctx.llm_generate = flaky_generate

    text = asyncio.run(call_llm(ctx, "umo:GroupMessage:1", "hello"))

    assert text == "retried-ok"
    assert len(ctx.generate_calls) == 2
    assert sleeps == [1.0]


def test_no_llm_at_all_raises_unavailable(monkeypatch):
    """(额外) 上下文什么都没有 -> 重试后 LLMUnavailableError。"""
    monkeypatch.setattr(llm_call.asyncio, "sleep", _no_sleep)

    class EmptyContext:
        pass

    try:
        asyncio.run(call_llm(EmptyContext(), "umo:GroupMessage:1", "hello"))
        assert False, "应当抛出 LLMUnavailableError"
    except LLMUnavailableError as e:
        assert "未配置可用的 LLM" in str(e)


def test_configured_provider_id_used_first(monkeypatch):
    """配置了 provider_id 时优先用该 ID 调 llm_generate。"""
    ctx = FakeContext()
    ctx.pid = "session-default"  # 会话默认, 不应被用到
    ctx.gen_result = FakeResp("from-configured")

    text = asyncio.run(
        call_llm(
            ctx,
            "umo:GroupMessage:1",
            "hello",
            provider_id="my-special-provider",
        )
    )

    assert text == "from-configured"
    assert len(ctx.generate_calls) == 1
    assert ctx.generate_calls[0]["chat_provider_id"] == "my-special-provider"


def test_configured_provider_id_falls_back_to_get_provider_by_id(monkeypatch):
    """配置的 provider 走 llm_generate 失败时, 用 get_provider_by_id + text_chat。"""
    provider = FakeProvider(result=FakeResp("by-id-ok"))

    class Ctx(FakeContext):
        def get_provider_by_id(self, pid):
            assert pid == "cfg-provider"
            return provider

        async def llm_generate(self, **kwargs):
            self.generate_calls.append(kwargs)
            return None  # 新接口无结果

    ctx = Ctx()
    text = asyncio.run(call_llm(ctx, "umo:1", "p", provider_id="cfg-provider"))
    assert text == "by-id-ok"
    assert provider.chat_calls == 1
