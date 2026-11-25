"""Batch 3 runtime, hot-path offload, timeout & stability regressions for Group Chronicle.

Covers:
- P1-6: get_series_summary() revision-stamped cache & SQL fast-path + async thread-offload helpers.
- P1-7: <think>...</think> stripping in validate_extraction & culture _candidate_list;
  poison-pill skip counter preventing infinite cursor deadlock; watch_scope exception resilience;
  fair history/culture interleaving preventing culture starvation; history_daily_limit=0 allowed.
- P1-8: record_valuable_outcome observes participants without clobbering roster_at or marking
  others departed; call_llm single-attempt timeout, no 4x retry on non-retryable fallback errors,
  middle-truncation preserving trailing prompt instructions; render_chronicle_image Playwright
  concurrency Semaphore bounding concurrent Chromium launches.
"""

from __future__ import annotations

import asyncio
import json
import sys
import threading
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core import culture_contract, lifetime as lifetime_module, llm_call, lm_bridge, render
from core.lifetime import HistoryService, validate_extraction
from core.llm_call import LLMUnavailableError, call_llm
from tests.test_commands import make_plugin, run
from tests.test_history import U, message


# ---------------------------------------------------------------------------
# P1-6: get_series_summary() revision cache, fast-path & async offload
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p1_6_series_summary_caches_by_revision_and_async_offloads(tmp_path: Path):
    plugin = make_plugin({"history_enabled": True, "culture_enabled": True})
    plugin.data_dir = tmp_path
    plugin.history = HistoryService(plugin, plugin.store)
    from core.culture_service import CultureService

    plugin.culture = CultureService(plugin)
    try:
        plugin.history.store.group(U, "缓存群")
        s1 = plugin.get_series_summary()
        assert s1["badge"]["count"] == 0

        # Second call with unchanged revisions hits cache (exact same object / zero entry unpack)
        s2 = plugin.get_series_summary()
        assert s2 is s1

        # Async helper runs on a worker thread off the event loop
        loop_tid = threading.get_ident()
        call_threads: list[int] = []
        orig_compute = plugin._compute_series_summary

        def spy_compute():
            call_threads.append(threading.get_ident())
            return orig_compute()

        plugin._compute_series_summary = spy_compute
        s3 = await plugin.get_series_summary_async()
        assert s3["badge"]["count"] == 0
        assert len(call_threads) == 1 and call_threads[0] != loop_tid
    finally:
        await plugin.history.terminate()


# ---------------------------------------------------------------------------
# P1-7: <think> strip, poison-pill skip, watch_scope resilience, fairness, limit=0
# ---------------------------------------------------------------------------


def test_p1_7_think_tags_stripped_in_history_and_culture_json():
    mat = [
        {"id": 1, "uid": "10", "name": "甲", "ts": 1700000100, "fragment": 0, "content": "一起打通关了副本"},
    ]
    raw_hist = (
        "<think>先分析一下这段群聊是不是重要事件。\n觉得算milestone。</think>\n"
        "```json\n"
        + json.dumps(
            {
                "events": [
                    {
                        "kind": "achievement",
                        "title": "全员通关副本",
                        "detail": "群友协作通关高难副本",
                        "importance": 2,
                        "positive": True,
                        "source_ids": [1],
                        "participants": [{"uid": "10", "role": 3}],
                    }
                ]
            },
            ensure_ascii=False,
        )
        + "\n```"
    )
    facts = validate_extraction(raw_hist, mat)
    assert len(facts) == 1
    assert facts[0]["title"] == "全员通关副本"

    raw_culture = "<think>思考黑话</think>\n```json\n{\"entries\": [{\"name\": \"蓝蘑菇\"}]}\n```"
    entries = culture_contract._candidate_list(raw_culture)
    assert entries == [{"name": "蓝蘑菇"}]


def test_p1_7_poison_pill_skips_after_threshold_and_advances_cursor(tmp_path: Path, monkeypatch):
    plugin = make_plugin({"history_enabled": True, "history_daily_limit": 20})
    plugin.data_dir = tmp_path
    mgr = HistoryService(plugin, plugin.store)
    monkeypatch.setattr(mgr, "allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(lm_bridge, "history_bounds", AsyncMock(return_value={"low": 1, "high": 5}))
    rows = [message(1, "这条消息总会导致模型输出非法 JSON 内容，包含足够的字数触发抽取。", uid="10")]
    monkeypatch.setattr(lm_bridge, "history_page", AsyncMock(return_value=rows))
    monkeypatch.setattr(lifetime_module, "call_llm", AsyncMock(return_value="NOT_VALID_JSON"))

    try:
        # First 2 attempts raise ValueError/JSONDecodeError (retryable)
        for _ in range(2):
            with pytest.raises(ValueError):
                run(mgr.process_one(U, "back"))
        assert mgr.store.job(U)["back_cursor"] == 0

        # 3rd consecutive failure triggers poison-pill skip and advances cursor!
        ok = run(mgr.process_one(U, "back"))
        assert ok is True
        job = mgr.store.job(U)
        assert job["back_cursor"] == 1
        assert "跳过" in job["gaps"]
    finally:
        mgr.store.close()


@pytest.mark.asyncio
async def test_p1_7_watch_scope_survives_transient_exception(tmp_path: Path, monkeypatch):
    plugin = make_plugin({"history_enabled": True})
    plugin.data_dir = tmp_path
    mgr = HistoryService(plugin, plugin.store)
    ticks = {"n": 0}

    async def flaky_refresh():
        ticks["n"] += 1
        if ticks["n"] == 1:
            raise RuntimeError("transient scope glitch")
        plugin._terminating = True

    monkeypatch.setattr(mgr, "refresh_scope", flaky_refresh)
    monkeypatch.setattr(lifetime_module, "_WATCH_SCOPE_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(plugin, "_spawn_bg", lambda coro, *, label: coro.close())

    try:
        await mgr.watch_scope()
        assert ticks["n"] == 2
    finally:
        mgr.store.close()


@pytest.mark.asyncio
async def test_p1_7_culture_not_starved_by_history_and_daily_limit_zero_pauses(
    tmp_path: Path, monkeypatch
):
    # 1) history_daily_limit=0 is NOT clamped to 1
    p_zero = make_plugin({"history_enabled": True, "history_daily_limit": 0})
    assert p_zero._cfg("history_daily_limit", 20) == 0

    # 2) Fair interleaving: even with heavy history backlog, culture gets turns in the same cycle!
    plugin = make_plugin({"history_enabled": True, "culture_enabled": True, "history_daily_limit": 24})
    plugin.data_dir = tmp_path
    mgr = HistoryService(plugin, plugin.store)
    day = "2026-11-25"
    quota_used = {"n": 0}
    order: list[str] = []

    async def fake_process_one(umo, mode):
        order.append("history")
        quota_used["n"] += 1
        return True

    async def fake_culture_step(umo, mode, raise_on_budget_busy=False):
        order.append("culture")
        quota_used["n"] += 1
        return True

    plugin.culture = SimpleNamespace(
        store=SimpleNamespace(job=lambda u: {"updated": 0}),
        step=fake_culture_step,
    )
    monkeypatch.setattr(lm_bridge, "history_connection", lambda ctx: object())
    monkeypatch.setattr(plugin, "_llm_provider_id", lambda: "")
    monkeypatch.setattr(mgr, "refresh_scope", AsyncMock(return_value=None))
    monkeypatch.setattr(mgr, "discover", AsyncMock(return_value=[U]))
    monkeypatch.setattr(mgr, "allowed", AsyncMock(return_value=True))
    monkeypatch.setattr(mgr, "process_one", fake_process_one)
    monkeypatch.setattr(mgr.store, "quota", lambda d: quota_used["n"])

    try:
        await mgr.run_cycle()
        assert "history" in order
        assert "culture" in order
    finally:
        mgr.store.close()


# ---------------------------------------------------------------------------
# P1-8: record_valuable_outcome roster_at, call_llm timeout/retry/truncation, render Semaphore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_p1_8_record_valuable_outcome_does_not_overwrite_roster_at_or_depart_members(
    tmp_path: Path, monkeypatch
):
    plugin = make_plugin({"history_enabled": True})
    plugin.data_dir = tmp_path
    mgr = HistoryService(plugin, plugin.store)
    monkeypatch.setattr(mgr, "allowed", AsyncMock(return_value=True))

    try:
        # Full group roster established with 3 members
        mgr.store.roster(U, [("u1", "甲"), ("u2", "乙"), ("u3", "丙")], now=1700000100.0)
        roster_before = mgr.store.group(U)["roster_at"]

        res = await mgr.record_valuable_outcome(
            U,
            {
                "schema_version": 1,
                "title": "两人小游戏通关",
                "detail": "甲乙完成一局双人游戏",
                "kind": "activity",
                "participants": [{"uid": "u1", "name": "甲"}, {"uid": "u2", "name": "乙"}],
                "event_at": 1700000500.0,
            },
        )
        assert res["admitted"] is True
        # roster_at MUST remain untouched and u3 MUST NOT be marked departed!
        assert mgr.store.group(U)["roster_at"] == roster_before
        with mgr.store.connection() as db:
            u3_period = db.execute(
                "SELECT closed, departed FROM periods WHERE umo=? AND uid='u3'", (U,)
            ).fetchone()
            assert u3_period is not None
            assert u3_period["closed"] == 0
            assert u3_period["departed"] is None
    finally:
        mgr.store.close()


@pytest.mark.asyncio
async def test_p1_8_llm_call_timeout_non_retryable_fast_fail_and_tail_preserving_truncation(
    monkeypatch,
):
    # 1) Prompt truncation preserves BOTH head and trailing output instructions
    big_prompt = "HEADER_START\n" + ("M" * 30000) + "\nTAIL_OUTPUT_JSON_INSTRUCTION"
    truncated = llm_call._truncate_prompt_preserving_tail(big_prompt, 4000)
    assert len(truncated) <= 4000
    assert truncated.startswith("HEADER_START")
    assert truncated.endswith("TAIL_OUTPUT_JSON_INSTRUCTION")

    # 2) Non-retryable 401/Timeout in session/default path must fail fast (NOT 4x retry!)
    monkeypatch.setattr(llm_call.asyncio, "sleep", AsyncMock(return_value=None))
    calls = {"gen": 0, "chat": 0}

    class NonRetryCtx:
        async def get_current_chat_provider_id(self, umo=None):
            return "sess-prov"

        async def llm_generate(self, **kwargs):
            calls["gen"] += 1
            raise RuntimeError("HTTP 401 unauthorized invalid api key")

        def get_using_provider(self, umo=None):
            return SimpleNamespace(text_chat=self._chat)

        async def _chat(self, **kwargs):
            calls["chat"] += 1
            return SimpleNamespace(completion_text="fallback")

    with pytest.raises(LLMUnavailableError):
        await call_llm(NonRetryCtx(), U, "hi", "sys")
    assert calls["gen"] == 1
    assert calls["chat"] == 0

    # 3) Per-attempt timeout bounds hung provider calls and fails fast without 4x retry
    hang_calls = {"gen": 0}

    class HangCtx:
        async def get_current_chat_provider_id(self, umo=None):
            return "hang-prov"

        async def llm_generate(self, **kwargs):
            hang_calls["gen"] += 1
            await asyncio.Event().wait()
            return SimpleNamespace(completion_text="late")

    with pytest.raises(LLMUnavailableError):
        await call_llm(HangCtx(), U, "hi", "sys", call_timeout=0.08)
    assert hang_calls["gen"] == 1


@pytest.mark.asyncio
async def test_p1_8_playwright_render_enforces_concurrency_semaphore(tmp_path: Path, monkeypatch):
    active = {"now": 0, "peak": 0}

    class FakeElement:
        async def screenshot(self, path):
            Path(path).write_bytes(b"PNG")

    class FakePage:
        async def set_content(self, html_content):
            pass

        async def wait_for_timeout(self, ms):
            await asyncio.sleep(0.05)

        async def query_selector(self, sel):
            return FakeElement()

    class FakeBrowser:
        async def new_page(self, viewport=None):
            return FakePage()

        async def close(self):
            pass

    class FakePW:
        async def __aenter__(self):
            active["now"] += 1
            active["peak"] = max(active["peak"], active["now"])
            return SimpleNamespace(
                chromium=SimpleNamespace(launch=AsyncMock(return_value=FakeBrowser()))
            )

        async def __aexit__(self, *args):
            active["now"] -= 1

    fake_mod = types.ModuleType("playwright.async_api")
    fake_mod.async_playwright = lambda: FakePW()
    monkeypatch.setitem(sys.modules, "playwright", types.ModuleType("playwright"))
    monkeypatch.setitem(sys.modules, "playwright.async_api", fake_mod)
    monkeypatch.setattr(render, "render_available", lambda: True)
    monkeypatch.setattr(render, "_PLAYWRIGHT_MAX_CONCURRENCY", 2)
    render._PLAYWRIGHT_SEMAPHORES.clear()

    paths = await asyncio.gather(
        *[
            render.render_chronicle_image(f"# 测试 {i}", "测试群", str(tmp_path))
            for i in range(6)
        ]
    )
    assert all(p is not None and Path(p).exists() for p in paths)
    assert active["peak"] <= 2
