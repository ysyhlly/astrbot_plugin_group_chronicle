"""Tests for read-only series shared defaults inheritance in Group Chronicle."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.persona import resolve_system_prompt
from main import GroupChroniclePlugin


def _chat_context(*, activated: bool = True):
    class ChatOwner:
        def get_series_shared_config(self):
            return {
                "reply_provider": "series-reply",
                "persona_id": "series-persona",
            }

    persona_manager = SimpleNamespace(
        get_persona=AsyncMock(
            side_effect=lambda pid: SimpleNamespace(system_prompt=f"prompt:{pid}")
        ),
        get_default_persona_v3=AsyncMock(return_value={"prompt": "prompt:default"}),
    )
    return SimpleNamespace(
        get_all_stars=lambda: {
            "chat": SimpleNamespace(
                name="astrbot_plugin_chat_dynamics",
                activated=activated,
                star_cls=ChatOwner(),
            )
        },
        persona_manager=persona_manager,
    )


def _stub_plugin(config: dict, context):
    plugin = GroupChroniclePlugin.__new__(GroupChroniclePlugin)
    plugin.config = config
    plugin.context = context
    return plugin


@pytest.mark.asyncio
async def test_chronicle_inherits_series_provider_and_persona():
    ctx = _chat_context()
    plugin = _stub_plugin({"llm_provider_id": ""}, ctx)
    assert plugin._llm_provider_id() == "series-reply"
    prompt = await resolve_system_prompt(
        ctx, {"persona_mode": "core_persona", "persona_id": ""}
    )
    assert prompt == "prompt:series-persona"


@pytest.mark.asyncio
async def test_chronicle_explicit_config_takes_priority():
    ctx = _chat_context()
    plugin = _stub_plugin({"llm_provider_id": "own-provider"}, ctx)
    assert plugin._llm_provider_id() == "own-provider"
    prompt = await resolve_system_prompt(
        ctx, {"persona_mode": "core_persona", "persona_id": "own-persona"}
    )
    assert prompt == "prompt:own-persona"


@pytest.mark.asyncio
async def test_chronicle_safe_fallback_when_chat_dynamics_missing_or_disabled():
    for ctx in (
        SimpleNamespace(
            get_all_stars=lambda: {},
            persona_manager=SimpleNamespace(get_persona=AsyncMock()),
        ),
        _chat_context(activated=False),
    ):
        plugin = _stub_plugin({"llm_provider_id": ""}, ctx)
        assert plugin._llm_provider_id() is None
        prompt = await resolve_system_prompt(
            ctx, {"persona_mode": "core_persona", "persona_id": ""}
        )
        assert prompt is None
