"""Batch 1 security & privacy regressions for Group Chronicle.

Covers:
- P0-1: Group scope (GROUP_PATTERN), core exclude_groups, allow/block lists, and history.allowed
  enforcement across _lm_and_gather, 史记选题, _universe_analyze/宇宙史记, and 影响力榜.
- P0-2: Scoped self_learning jargon reads (never unscoped / never cross-group).
- P0-3: Leading '/' command acceptance (/史记隐藏, /群大事, /人物等级) and revision guard.
- CultureWeb unified require_series_admin + CSRF enforcement, hidden-entry query/status
  side-channel closure, and note: null returning HTTP 400 (not 503).
"""

from __future__ import annotations

import json
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import main as main_module
from core import gallery, history_commands
from core.llm_call import LLMUnavailableError
from tests.test_commands import (
    FakeEvent,
    all_sent_texts,
    amock,
    collect,
    make_evidence,
    make_plugin,
    patch_lm,
    run,
)
from tests.test_culture import culture, ingest, plugin, proposal, rows  # noqa: F401
from tests.test_history import Event, U, V, message, source  # noqa: F401


# ---------------------------------------------------------------------------
# P0-1: Group scope, core exclude_groups, and history.allowed enforcement
# ---------------------------------------------------------------------------


def test_p0_1_lm_and_gather_enforces_group_pattern_exclude_and_allowed(monkeypatch):
    patch_lm(monkeypatch)
    monkeypatch.setattr(main_module, "gather_evidence", amock(make_evidence()))

    # 1) Non-group / private UMO blocked
    p = make_plugin()
    with pytest.raises(LLMUnavailableError, match="只能在群内使用"):
        run(p._lm_and_gather("aiocqhttp:FriendMessage:12345", None))
    with pytest.raises(LLMUnavailableError, match="只能在群内使用"):
        run(p._lm_and_gather("bare_group_id", None))

    # 2) Core exclude_groups blocked
    p_excl = make_plugin()
    monkeypatch.setattr(p_excl, "_is_excluded_by_core", lambda u: u == U)
    with pytest.raises(LLMUnavailableError, match="主控排除列表"):
        run(p_excl._lm_and_gather(U, None))

    # 3) history_block_groups / history_allow_groups blocked
    p_block = make_plugin({"history_block_groups": [U]})
    with pytest.raises(LLMUnavailableError, match="不在允许范围"):
        run(p_block._lm_and_gather(U, None))

    p_allow = make_plugin({"history_allow_groups": [V]})
    with pytest.raises(LLMUnavailableError, match="不在允许范围"):
        run(p_allow._lm_and_gather(U, None))

    # 4) history.allowed returning False blocked
    p_denied = make_plugin({"history_enabled": True})
    monkeypatch.setattr(p_denied.history, "allowed", AsyncMock(return_value=False))
    with pytest.raises(LLMUnavailableError, match="停用或不在允许范围"):
        run(p_denied._lm_and_gather(U, None))


def test_p0_1_vote_topic_and_influence_enforce_scope(monkeypatch):
    patch_lm(monkeypatch)
    monkeypatch.setattr(main_module, "gather_evidence", amock(make_evidence()))

    # Private UMO rejected by 史记选题 without spawning background job
    p = make_plugin({"enable_vote": True})
    spawned = []
    monkeypatch.setattr(p, "_spawn_bg", lambda coro, *, label: (spawned.append(label), coro.close()))
    ev_priv = FakeEvent(umo="aiocqhttp:FriendMessage:777")
    res_vote = run(collect(p.vote_topic_command(ev_priv)))
    assert res_vote == ["此命令只能在群内使用"]
    assert spawned == []

    # Excluded group rejected by 史记选题 and 影响力榜
    monkeypatch.setattr(p, "_is_excluded_by_core", lambda u: u == U)
    ev_excl = FakeEvent(umo=U)
    assert run(collect(p.vote_topic_command(ev_excl))) == ["本群已在主控排除列表中，无法执行此操作"]
    assert run(collect(p.influence_command(ev_excl))) == ["本群已在主控排除列表中，无法执行此操作"]

    # history.allowed == False rejected by 影响力榜
    p2 = make_plugin({"history_enabled": True})
    monkeypatch.setattr(p2.history, "allowed", AsyncMock(return_value=False))
    assert run(collect(p2.influence_command(FakeEvent(umo=U)))) == ["本群长期记录已停用或不在允许范围"]


def test_p0_1_universe_analyze_skips_excluded_private_and_disallowed_groups(monkeypatch):
    p = make_plugin(
        {
            "universe_groups": [
                U,
                V,
                "aiocqhttp:FriendMessage:999",
                "aiocqhttp:GroupMessage:30003",
            ],
            "history_block_groups": [V],
        }
    )
    monkeypatch.setattr(
        p,
        "_is_excluded_by_core",
        lambda u: u == "aiocqhttp:GroupMessage:30003",
    )
    gathered = []

    async def fake_gather(umo, days=None, *, progress=None):
        gathered.append(umo)
        return make_evidence(msg_count=3), 7

    monkeypatch.setattr(p, "_lm_and_gather", fake_gather)
    monkeypatch.setattr(main_module, "generate_universe_chronicle", amock("综合宇宙史记"))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))

    # Invoking from valid allowed group U only gathers U (skips blocked V, private, and excluded)
    ev = FakeEvent(umo=U, is_admin=True)
    run(
        p._universe_run(
            ev,
            U,
            [U, V, "aiocqhttp:FriendMessage:999", "aiocqhttp:GroupMessage:30003"],
        )
    )
    assert gathered == [U]
    assert any("综合宇宙史记" in t for t in all_sent_texts(p, ev))

    # Invoking universe_command from private chat rejected immediately
    ev_priv = FakeEvent(umo="aiocqhttp:FriendMessage:1", is_admin=True)
    assert run(collect(p.universe_command(ev_priv))) == ["此命令只能在群内使用"]


# ---------------------------------------------------------------------------
# P0-2: Scoped self_learning jargon reads (zero unscoped / cross-group leaks)
# ---------------------------------------------------------------------------


def test_p0_2_soft_read_jargon_skips_unscoped_methods_and_passes_scope():
    called = []

    class FakeLearningPlugin:
        def get_jargon(self):
            called.append("unscoped_called")
            return {"全群泄露词": "绝不能被调用"}

        def query_jargon(self, umo: str):
            called.append(("query_jargon", umo))
            return [{"term": "本群梗", "meaning": "只在当前群"}]

        def get_slang_terms(self, group_id: str):
            called.append(("get_slang_terms", group_id))
            return {"数字群梗": f"来自{group_id}"}

        def get_culture_jargon_v1(self, scope: str):
            called.append(("get_culture_jargon_v1", scope))
            # Return wrong scope to verify cross-group defense discards it
            return {
                "schema_version": 1,
                "scope": "aiocqhttp:GroupMessage:OTHER",
                "terms": [{"name": "外群黑话", "meaning": "不能泄露"}],
            }

    star = SimpleNamespace(activated=True, star_cls=FakeLearningPlugin())
    ctx = SimpleNamespace(get_registered_star=lambda alias: star if alias == "astrbot_plugin_dynamics_learning" else None)

    # Missing umo -> safe empty list, zero calls
    assert run(gallery._soft_read_jargon(ctx, umo=None)) == []
    assert run(gallery._soft_read_jargon(ctx, umo="")) == []
    assert called == []

    # With explicit umo -> unscoped get_jargon is SKIPPED, scoped methods receive umo/group_id,
    # and mismatched scope from get_culture_jargon_v1 is discarded.
    lines = run(gallery._soft_read_jargon(ctx, umo="aiocqhttp:GroupMessage:10086"))
    assert "unscoped_called" not in called
    assert ("query_jargon", "aiocqhttp:GroupMessage:10086") in called
    assert ("get_slang_terms", "10086") in called
    assert any("本群梗：只在当前群" in ln for ln in lines)
    assert any("数字群梗：来自10086" in ln for ln in lines)
    assert not any("全群泄露词" in ln for ln in lines)
    assert not any("外群黑话" in ln for ln in lines)


# ---------------------------------------------------------------------------
# P0-3: Leading '/' command handling & revision guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p0_3_leading_slash_hide_and_revision_guard(plugin):  # noqa: F811
    plugin.history.store.observe(U, "1", "甲", 1700000000.0)
    preview = await history_commands.handle_command(
        plugin.history, Event("/史记隐藏", uid="1")
    )
    assert "作用域预览" in preview
    reply = await history_commands.handle_command(
        plugin.history, Event("/史记隐藏 确认", uid="1")
    )
    assert "已隐藏" in reply
    assert plugin.history.store.hidden(U)

    # Leading '/' with whitespace also accepted
    reply2 = await history_commands.handle_command(
        plugin.history, Event("  /群大事", uid="1")
    )
    assert "暂无已入史的大事" in reply2

    # Revision guard triggers for /群大事 when privacy/revision changes mid-yield
    from tests.test_history import fact

    plugin.config["max_reply_chars"] = 30
    plugin.history.store.add_event(U, fact("2"), status="approved")
    ev = Event("/群大事", uid="2")
    parts = []
    async for part in history_commands.command_reply(plugin, ev):
        parts.append(part)
        if len(parts) == 1:
            plugin.history.store.hide(U, "2")
    assert any("事实或隐私设置已更新" in p for p in parts)


# ---------------------------------------------------------------------------
# CultureWeb: unified require_series_admin + CSRF, side-channel, note: null
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_culture_web_side_channel_and_null_note_and_csrf(culture, monkeypatch):  # noqa: F811
    db, item = await ingest(culture, monkeypatch)
    web = culture.plugin.culture_web
    try:
        # 1) note: null -> 400 (NOT 503!) via CultureWeb.respond
        web_module = types.ModuleType("astrbot.api.web")
        auth_module = types.ModuleType("astrbot.dashboard.api.auth")
        state = {
            "user": "admin",
            "method": "POST",
            "headers": {
                "Host": "dashboard.local",
                "Origin": "https://dashboard.local",
                "Sec-Fetch-Site": "same-origin",
            },
            "body": b"{}",
        }

        class FakeRawRequest:
            @property
            def method(self):
                return state["method"]

            @property
            def headers(self):
                return state["headers"]

            async def body(self):
                return state["body"]

        web_module.request = SimpleNamespace(_request=FakeRawRequest(), username=None)
        web_module.json_response = lambda data, status_code=200: (status_code, data)

        async def fake_require_user(_raw):
            if not state["user"]:
                raise PermissionError("需要 Dashboard 管理员登录")
            return state["user"]

        auth_module.require_dashboard_user = fake_require_user
        monkeypatch.setitem(sys.modules, "astrbot.api.web", web_module)
        monkeypatch.setitem(sys.modules, "astrbot.dashboard.api.auth", auth_module)
        culture.plugin.context.get_config = lambda umo=None: {
            "dashboard": {"username": "admin"}
        }

        # note: null returns 400
        state["body"] = json.dumps(
            {
                "op": "action",
                "scope": U,
                "action": "approve",
                "id": item["id"],
                "revision": item["revision"],
                "note": None,
            }
        ).encode("utf-8")
        status, payload = await web.handle()
        assert status == 400
        assert payload["status"] == "error"

        # CSRF: POST missing Origin/CSRF headers -> 403
        state["headers"] = {"Host": "dashboard.local"}
        status_no_csrf, _ = await web.handle()
        assert status_no_csrf == 403

        # CSRF: POST with Sec-Fetch-Site: cross-site -> 403
        state["headers"] = {
            "Host": "dashboard.local",
            "Origin": "https://dashboard.local",
            "Sec-Fetch-Site": "cross-site",
        }
        status_xsite, _ = await web.handle()
        assert status_xsite == 403

        # api_key: identity rejected on GET and POST -> 403
        state["headers"] = {
            "Host": "dashboard.local",
            "Origin": "https://dashboard.local",
            "Sec-Fetch-Site": "same-origin",
        }
        state["user"] = "api_key:secret"
        state["method"] = "GET"
        status_apikey, _ = await web.overview()
        assert status_apikey == 403

        # Empty dashboard.username fails closed -> 403
        state["user"] = "admin"
        culture.plugin.context.get_config = lambda umo=None: {"dashboard": {"username": ""}}
        status_empty_cfg, _ = await web.overview()
        assert status_empty_cfg == 403
        culture.plugin.context.get_config = lambda umo=None: {"dashboard": {"username": "admin"}}

        # 2) Hide entry and verify list query side-channel is closed
        await culture.action(
            U,
            "hide",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="本人请求隐藏",
        )
        by_keyword = await web.dispatch(
            {"op": "list", "scope": U, "query": "蓝蘑菇"}, "human:admin"
        )
        assert by_keyword["entries"] == []
        assert by_keyword["total"] == 0

        by_id = await web.dispatch(
            {"op": "list", "scope": U, "query": str(item["id"])}, "human:admin"
        )
        assert by_id["entries"] == []
        assert by_id["total"] == 0

        # Non-hidden status filter does not leak hidden items
        by_approved = await web.dispatch(
            {"op": "list", "scope": U, "status": "approved"}, "human:admin"
        )
        assert by_approved["entries"] == []
        assert by_approved["total"] == 0
    finally:
        await db.close()
