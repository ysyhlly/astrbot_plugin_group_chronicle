"""Regression tests for llm_call isolation, revision split, bio concurrency, prose resume, lm_bridge, and health."""

from __future__ import annotations

import asyncio
import sys
import time
import types
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

import core.lifetime as lifetime_mod
import core.llm_call as llm_call_mod
import core.lm_bridge as lm_bridge_mod
import series_panels
from core.archive_store import ArchiveStore
from core.llm_call import LLMUnavailableError, call_llm
from main import GroupChroniclePlugin

U = "aiocqhttp:GroupMessage:99001"


def _fact(key: str, *, uid: str = "1", name: str = "甲", ts: float | None = None, importance: int = 2, role: int = 2):
    when = float(ts if ts is not None else time.time() - 3600)
    return {
        "kind": "milestone",
        "title": f"大事-{key}",
        "detail": f"群里共同完成了重要事件 {key}。",
        "importance": importance,
        "positive": True,
        "risk": False,
        "participants": [{"uid": uid, "name": name, "role": role}],
        "unknown_participants": [],
        "sources": [{"id": abs(hash(key)) % 100000 + 1, "uid": uid, "hash": key, "fragment": 0, "excerpt": key}],
        "event_at": when,
        "date_basis": "recorded",
        "ongoing": False,
        "followups": [],
        "admission": "approved",
        "fingerprint": f"fp_{key}",
    }


async def _no_sleep(_delay: float) -> None:
    return None


# ---------------------------------------------------------------------------
# 1. llm_call: explicit provider execution failures NEVER fall back to flagship
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_explicit_provider_execution_errors_never_fallback_to_session_default(monkeypatch):
    monkeypatch.setattr(llm_call_mod.asyncio, "sleep", _no_sleep)

    for case, exc_or_resp, expected_calls in (
        ("timeout", asyncio.TimeoutError("request timed out"), 1),
        ("rate_limit_429", RuntimeError("HTTP 429 Too Many Requests"), 2),
        ("server_503", RuntimeError("503 Service Unavailable"), 2),
        ("policy_refused", RuntimeError("content_filter safety policy refused"), 1),
        ("empty_resp", NS(completion_text="   "), 2),
    ):
        cheap_prov = NS(
            is_systemone_provider=False,
            text_chat=AsyncMock(return_value=NS(completion_text="should-not-hit-text-chat")),
        )
        flagship_prov = NS(
            is_systemone_provider=False,
            text_chat=AsyncMock(return_value=NS(completion_text="EXPENSIVE-FLAGSHIP")),
        )
        gen_calls: list[str] = []

        async def _llm_generate(chat_provider_id=None, **_kw):
            gen_calls.append(str(chat_provider_id))
            if isinstance(exc_or_resp, BaseException):
                raise exc_or_resp
            return exc_or_resp

        get_session_pid = AsyncMock(return_value="expensive-flagship")
        ctx = NS(
            get_all_stars=lambda: {},
            get_provider_by_id=lambda pid: cheap_prov if pid == "cheap-chronicle" else flagship_prov,
            get_current_chat_provider_id=get_session_pid,
            get_using_provider=lambda **_kw: flagship_prov,
            llm_generate=_llm_generate,
        )

        with pytest.raises(LLMUnavailableError):
            await call_llm(ctx, U, "生成群史记", provider_id="cheap-chronicle")

        assert gen_calls == ["cheap-chronicle"] * expected_calls, f"case={case}"
        assert cheap_prov.text_chat.await_count == 0, f"case={case}"
        assert get_session_pid.await_count == 0, f"case={case}"
        assert flagship_prov.text_chat.await_count == 0, f"case={case}"


@pytest.mark.asyncio
async def test_explicit_provider_missing_or_interface_error_allows_clean_fallback(monkeypatch):
    monkeypatch.setattr(llm_call_mod.asyncio, "sleep", _no_sleep)

    # A. Provider genuinely not found (get_provider_by_id -> None) falls back to session default
    session_calls: list[str] = []

    async def _llm_gen(chat_provider_id=None, **_kw):
        session_calls.append(str(chat_provider_id))
        return NS(completion_text="session-ok")

    ctx_missing = NS(
        get_all_stars=lambda: {},
        get_provider_by_id=lambda _pid: None,
        get_current_chat_provider_id=AsyncMock(return_value="session-default"),
        llm_generate=_llm_gen,
    )
    out = await call_llm(ctx_missing, U, "hello", provider_id="deleted-provider")
    assert out == "session-ok"
    assert session_calls == ["session-default"]

    # B. Old API TypeError on llm_generate falls back to SAME provider's text_chat (not flagship!)
    cheap_prov = NS(
        is_systemone_provider=False,
        text_chat=AsyncMock(return_value=NS(completion_text="same-provider-text-chat")),
    )
    flagship = NS(text_chat=AsyncMock(return_value=NS(completion_text="FLAGSHIP")))

    async def _bad_sig(**_kw):
        raise TypeError("unexpected keyword argument")

    ctx_old_api = NS(
        get_all_stars=lambda: {},
        get_provider_by_id=lambda pid: cheap_prov if pid == "cheap-p" else None,
        get_current_chat_provider_id=AsyncMock(return_value="flagship-p"),
        get_using_provider=lambda **_kw: flagship,
        llm_generate=_bad_sig,
    )
    out2 = await call_llm(ctx_old_api, U, "hello", provider_id="cheap-p")
    assert out2 == "same-provider-text-chat"
    assert cheap_prov.text_chat.await_count == 1
    assert flagship.text_chat.await_count == 0


# ---------------------------------------------------------------------------
# 2. privacy_revision vs content_revision granularity
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_content_revision_does_not_abort_inflight_report_or_bio_but_privacy_does(
    tmp_path, monkeypatch
):
    store = ArchiveStore(tmp_path / "rev.sqlite3")
    now = time.time()
    store.roster(U, [("1", "甲"), ("2", "乙")], now=now - 7200)
    store.add_event(U, _fact("e1", uid="1", name="甲", ts=now - 3600), status="approved")

    priv0 = store.privacy_revision(U)
    cont0 = store.content_revision(U)
    rev0 = store.revision(U)

    # Ordinary speech / nickname update / new event bump content_revision only
    store.observe(U, "3", "新成员丙", now - 100)
    store.observe(U, "1", "甲改名片", now - 50)
    assert store.privacy_revision(U) == priv0
    assert store.content_revision(U) > cont0
    assert store.revision(U) > rev0

    ctx = NS(get_all_stars=lambda: {}, get_config=lambda: {})
    plugin = GroupChroniclePlugin(
        ctx,
        {
            "history_enabled": True,
            "render_mode": "text",
            "top_n_bios": 1,
            "chronicle_days": 7,
        },
    )
    plugin.data_dir = str(tmp_path)
    plugin.history.store = store
    monkeypatch.setattr(plugin.history, "allowed", AsyncMock(return_value=True))

    # A. Mid-generation ordinary content changes do NOT abort build_report or biography
    async def _llm_with_content_activity(_ctx, _umo, prompt, _sp, **_kw):
        store.observe(U, "4", "路人丁", time.time())
        store.add_event(U, _fact("e2", uid="2", name="乙", ts=now - 1800), status="approved")
        return "【群史正文】平稳完成" if "window_days" in prompt else "【人物传】平稳完成"

    monkeypatch.setattr(lifetime_mod, "call_llm", _llm_with_content_activity)
    text, _img, _skipped = await plugin.history.build_report(U, 7, True, "测试群")
    assert "【群史正文】平稳完成" in text
    assert "【人物传】平稳完成" in text

    # B. Mid-generation privacy change (hide member) MUST abort build_report immediately
    async def _llm_with_privacy_hide(_ctx, _umo, _prompt, _sp, **_kw):
        store.hide(U, "2")
        return "不应被发布的正文"

    monkeypatch.setattr(lifetime_mod, "call_llm", _llm_with_privacy_hide)
    with pytest.raises(ValueError):
        await plugin.history.build_report(U, 7, False, "测试群", force=True)
    assert store.privacy_revision(U) > priv0


# ---------------------------------------------------------------------------
# 3. Multi-person biography Semaphore(2) bounded concurrency + prose checkpoint/resume
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_build_report_bio_semaphore_and_prose_checkpoint_resume(
    tmp_path, monkeypatch
):
    store = ArchiveStore(tmp_path / "resume.sqlite3")
    now = time.time()
    members = [("1", "甲"), ("2", "乙"), ("3", "丙"), ("4", "丁")]
    store.roster(U, members, now=now - 7200)
    for idx, (uid, name) in enumerate(members):
        store.add_event(
            U,
            _fact(f"ev_{uid}", uid=uid, name=name, ts=now - 3600 + idx * 60, importance=3, role=3),
            status="approved",
        )

    ctx = NS(get_all_stars=lambda: {}, get_config=lambda: {})
    plugin = GroupChroniclePlugin(
        ctx,
        {
            "history_enabled": True,
            "render_mode": "text",
            "top_n_bios": 4,
            "chronicle_days": 7,
        },
    )
    plugin.data_dir = str(tmp_path)
    plugin.history.store = store
    monkeypatch.setattr(plugin.history, "allowed", AsyncMock(return_value=True))

    prose_calls = 0
    bio_calls: dict[str, int] = {}
    in_flight = 0
    max_in_flight = 0
    fail_uid_once = {"2": True}

    async def _controlled_llm(_ctx, _umo, prompt, _sp, **_kw):
        nonlocal prose_calls, in_flight, max_in_flight
        if "window_days" in prompt:
            prose_calls += 1
            return "【长周期史记正文】第一阶段已持久化"
        # Identify member by name in compact JSON prompt
        uid = next(u for u, nm in members if f'"{nm}"' in prompt)
        bio_calls[uid] = bio_calls.get(uid, 0) + 1
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            await asyncio.sleep(0.03)
            if fail_uid_once.get(uid):
                fail_uid_once[uid] = False
                raise RuntimeError("transient bio failure for 乙")
            return f"传记内容-{uid}"
        finally:
            in_flight -= 1

    monkeypatch.setattr(lifetime_mod, "call_llm", _controlled_llm)

    # First run: prose succeeds & checkpoints, uid "2" fails mid-way
    with pytest.raises(RuntimeError, match="transient bio failure"):
        await plugin.history.build_report(U, 7, True, "并发测试群")

    assert prose_calls == 1
    assert max_in_flight == 2  # Semaphore(2): concurrent (>1) and bounded (<=2)

    # Draft must be persisted at stage=prose_ready
    with store.connection() as db:
        draft_rows = db.execute("SELECT stage FROM report_drafts WHERE umo=?", (U,)).fetchall()
    assert len(draft_rows) == 1
    assert draft_rows[0]["stage"] == "prose_ready"

    # Second run (retry): reuses persisted prose (prose_calls stays 1!) and already-completed bios!
    text, _img, _skipped = await plugin.history.build_report(U, 7, True, "并发测试群")
    assert prose_calls == 1  # Prose was NOT regenerated!
    assert bio_calls["1"] == 1  # Already completed bio was reused!
    assert bio_calls["2"] == 2  # Failed bio retried once and succeeded!
    assert "【长周期史记正文】第一阶段已持久化" in text
    # Deterministic member order preserved in output
    pos = [text.index(f"传记内容-{u}") for u in ("1", "2", "3", "4")]
    assert pos == sorted(pos)

    # Draft is cleaned up after final report commit
    with store.connection() as db:
        assert db.execute("SELECT COUNT(*) FROM report_drafts WHERE umo=?", (U,)).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_main_bio_analyze_uses_bounded_concurrency(tmp_path, monkeypatch):
    store = ArchiveStore(tmp_path / "bio_main.sqlite3")
    now = time.time()
    members = [("1", "甲"), ("2", "乙"), ("3", "丙"), ("4", "丁")]
    store.roster(U, members, now=now - 7200)
    for uid, name in members:
        store.add_event(
            U, _fact(f"b_{uid}", uid=uid, name=name, ts=now - 3600), status="approved"
        )

    ctx = NS(get_all_stars=lambda: {}, get_config=lambda: {})
    plugin = GroupChroniclePlugin(
        ctx, {"history_enabled": True, "top_n_bios": 4, "chronicle_days": 7}
    )
    plugin.history.store = store
    monkeypatch.setattr(plugin.history, "allowed", AsyncMock(return_value=True))

    in_flight = 0
    max_in_flight = 0

    async def _fake_bio(_umo, uid, **_kw):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            await asyncio.sleep(0.03)
            return f"BIO-{uid}"
        finally:
            in_flight -= 1

    delivered: list[str] = []

    async def _fake_deliver(_ctx, _ev, _umo, text, *_a, **_kw):
        delivered.append(text)

    monkeypatch.setattr(plugin.history, "biography", _fake_bio)
    monkeypatch.setattr("main.deliver_text", _fake_deliver)

    await plugin._bio_analyze(NS(), U, None)
    assert max_in_flight == 2
    assert delivered == ["BIO-1\n\nBIO-2\n\nBIO-3\n\nBIO-4"]


# ---------------------------------------------------------------------------
# 4. lm_bridge public facade priority & main.get_series_health()
# ---------------------------------------------------------------------------
def test_lm_bridge_prefers_dynamics_memory_public_facade_and_series_health(monkeypatch):
    lm_bridge_mod._clear_facade_cache()
    monkeypatch.setattr(lm_bridge_mod, "_lm_get_active", None)

    dm_owner = NS(
        get_dynamics_capabilities=lambda: {
            "schema_version": 1,
            "provider_id": "astrbot_plugin_dynamics_memory",
            "enabled": True,
            "ready": True,
            "capabilities": {"memory": 1},
        },
        is_capturing_group=lambda ev: getattr(ev, "unified_msg_origin", "") == U,
    )
    dm_facade = types.ModuleType("astrbot_plugin_dynamics_memory.api.public")
    dm_facade.find_plugin = lambda _ctx: dm_owner
    monkeypatch.setitem(sys.modules, "astrbot_plugin_dynamics_memory.api.public", dm_facade)

    get_by_id_calls: list[str] = []

    def _get_provider_by_id(pid: str):
        get_by_id_calls.append(pid)
        return None

    ctx = NS(
        get_all_stars=lambda: [],
        get_registered_star=lambda _a: None,
        get_config=lambda: {},
        provider_manager=NS(inst_map={"ok-prov": NS(id="ok-prov")}, provider_insts=[]),
        get_provider_by_id=_get_provider_by_id,
    )
    assert lm_bridge_mod.detect_lm(ctx) is dm_owner
    assert lm_bridge_mod.is_dm_capturing_group(ctx, NS(unified_msg_origin=U)) is True
    assert lm_bridge_mod.is_dm_capturing_group(ctx, NS(unified_msg_origin="other")) is False

    # get_series_health(): synchronous, read-only, uses find_provider_silently
    plugin_ok = GroupChroniclePlugin(ctx, {"llm_provider_id": "ok-prov"})
    health_ok = series_panels.read_plugin_health("astrbot_plugin_group_chronicle", plugin_ok)
    assert health_ok is not None
    assert health_ok["ready"] is True
    assert health_ok["degraded"] is False
    assert health_ok["providers"][0]["status"] == "ok"
    assert health_ok["metrics"]["memory_detected"] is True
    assert health_ok["metrics"]["memory_ready"] is True
    assert get_by_id_calls == []

    plugin_missing = GroupChroniclePlugin(ctx, {"llm_provider_id": "gone-prov"})
    health_bad = series_panels.read_plugin_health("astrbot_plugin_group_chronicle", plugin_missing)
    assert health_bad is not None
    assert health_bad["degraded"] is True
    assert health_bad["providers"][0]["status"] == "missing"
    assert get_by_id_calls == []  # Cold-start safe: never triggered host WARN!
