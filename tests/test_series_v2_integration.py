"""Focused tests for Group Chronicle shared ProviderBudget, timezone, provider fallback, exclude_groups & quiet_hours."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.llm_call import LLMUnavailableError, call_llm
from main import GroupChroniclePlugin


class FakeSharedBudget:
    supports_series_telemetry = True

    def __init__(self, *, busy: bool = False):
        self.busy = busy
        self.calls: list[dict] = []

    async def run(
        self,
        provider: str,
        purpose: str,
        invoke,
        *,
        plugin_id: str = "",
        acquire_timeout: float | None = None,
    ):
        self.calls.append(
            {
                "provider": provider,
                "purpose": purpose,
                "plugin_id": plugin_id,
                "acquire_timeout": acquire_timeout,
            }
        )
        if self.busy:
            err = RuntimeError("busy")
            err.is_provider_budget_busy = True
            err.provider = provider
            err.purpose = purpose
            err.reason = "acquire_timeout"
            raise err
        val = invoke()
        return await val if asyncio.iscoroutine(val) else val


def _chat_context(
    *,
    reply_provider: str = "series-reply",
    rhythm_timezone: str = "America/New_York",
    exclude_groups: list[str] | None = None,
    in_quiet_hours: bool = False,
    budget=None,
    llm_text: str = "本周群史记正文",
):
    class ChatOwner:
        def get_series_shared_config(self):
            return {
                "reply_provider": reply_provider,
                "rhythm_timezone": rhythm_timezone,
                "exclude_groups": exclude_groups or [],
                "quiet_hours": {
                    "daily_rhythm_enabled": True,
                    "in_quiet_hours": in_quiet_hours,
                },
            }

    prov = SimpleNamespace(
        meta=SimpleNamespace(id=reply_provider or "host-default"),
        text_chat=AsyncMock(return_value=SimpleNamespace(completion_text=llm_text)),
    )
    ctx = SimpleNamespace(
        get_all_stars=lambda: {
            "chat": SimpleNamespace(
                name="astrbot_plugin_chat_dynamics",
                activated=True,
                star_cls=ChatOwner(),
            )
        },
        get_provider_by_id=lambda pid: prov if pid else None,
        get_current_chat_provider_id=AsyncMock(return_value="session-default"),
        get_using_provider=lambda *a, **kw: prov,
        llm_generate=AsyncMock(return_value=SimpleNamespace(completion_text=llm_text)),
        send_message=AsyncMock(),
    )
    if budget is not None:
        ctx._chat_dynamics_provider_budget = budget
    return ctx


@pytest.mark.asyncio
async def test_chronicle_llm_budget_delegation_and_busy_degradation():
    budget = FakeSharedBudget(busy=False)
    ctx = _chat_context(budget=budget)
    text = await call_llm(ctx, "aiocqhttp:GroupMessage:1001", "生成史记", "你是史官")
    assert text == "本周群史记正文"
    assert len(budget.calls) == 1
    assert budget.calls[0]["plugin_id"] == "astrbot_plugin_group_chronicle"
    assert budget.calls[0]["provider"] == "series-reply"

    # Busy budget degrades immediately without retrying twice
    busy = FakeSharedBudget(busy=True)
    ctx._chat_dynamics_provider_budget = busy
    with pytest.raises(LLMUnavailableError):
        await call_llm(ctx, "aiocqhttp:GroupMessage:1001", "生成史记", "你是史官")
    assert len(busy.calls) == 1


@pytest.mark.asyncio
async def test_chronicle_standalone_and_provider_priority():
    ctx_standalone = SimpleNamespace(
        get_all_stars=lambda: {},
        get_provider_by_id=lambda pid: SimpleNamespace(is_systemone_provider=False),
        get_current_chat_provider_id=AsyncMock(return_value="session-p"),
        llm_generate=AsyncMock(return_value=SimpleNamespace(completion_text="独立运行成功")),
    )
    text = await call_llm(ctx_standalone, "aiocqhttp:GroupMessage:1001", "prompt")
    assert text == "独立运行成功"
    assert ctx_standalone.llm_generate.await_args.kwargs["chat_provider_id"] == "session-p"

    # Explicit override wins over shared reply_provider
    ctx_shared = _chat_context(reply_provider="series-reply")
    plugin = GroupChroniclePlugin(ctx_shared, {"llm_provider_id": "explicit-chronicle"})
    assert plugin._llm_provider_id() == "explicit-chronicle"
    plugin_inherited = GroupChroniclePlugin(ctx_shared, {"llm_provider_id": ""})
    assert plugin_inherited._llm_provider_id() == "series-reply"


@pytest.mark.asyncio
async def test_chronicle_timezone_exclude_groups_and_quiet_hours():
    excluded_umo = "aiocqhttp:GroupMessage:7777"
    allowed_umo = "aiocqhttp:GroupMessage:8888"
    ctx = _chat_context(
        rhythm_timezone="Asia/Tokyo",
        exclude_groups=[excluded_umo],
        in_quiet_hours=True,
    )
    plugin = GroupChroniclePlugin(
        ctx,
        {
            "history_enabled": True,
            "culture_enabled": True,
            "enable_weekly_push": True,
            "push_groups": [allowed_umo, excluded_umo],
        },
    )

    # Timezone inheritance
    assert str(plugin._series_tz()) == "Asia/Tokyo"
    assert str(plugin.history._series_tz()) == "Asia/Tokyo"

    # Exclude groups guard in history.enabled_for & get_series_scope_status
    assert plugin.history.enabled_for(excluded_umo) is False
    assert plugin.history.enabled_for(allowed_umo) is True

    st_ex = await plugin.get_series_scope_status(excluded_umo)
    assert st_ex["enabled"] is False
    assert st_ex["excluded_by_core"] is True
    assert "主控排除" in st_ex["summary"]

    toggle_res = await plugin.set_series_scope_enabled(excluded_umo, True)
    assert toggle_res["ok"] is False
    assert "主控排除" in toggle_res["summary"]

    # Quiet hours suppresses scheduled weekly push completely
    push_one = AsyncMock()
    plugin._weekly_push_one = push_one
    await plugin._weekly_push_run()
    push_one.assert_not_awaited()

    # Outside quiet hours: allowed group is pushed, excluded group is skipped
    ctx_awake = _chat_context(
        rhythm_timezone="Asia/Tokyo",
        exclude_groups=[excluded_umo],
        in_quiet_hours=False,
    )
    plugin.context = ctx_awake
    await plugin._weekly_push_run()
    push_one.assert_awaited_once_with(allowed_umo)
