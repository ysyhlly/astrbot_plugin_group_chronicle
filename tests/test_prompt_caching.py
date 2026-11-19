"""Unit tests for Phase 2 prompt prefix-caching stability in group_chronicle."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from core import culture_service, generation, lifetime, llm_call
from core.culture_contract import PROMPT as CULTURE_PROMPT, build_culture_system_prompt
from core.lifetime import (
    EXTRACT_PROMPT,
    SHARED_ARCHIVE_EXTRACTION_PREAMBLE,
    build_extract_system_prompt,
)
from tests.test_culture import culture, proposal  # noqa: F401
from tests.test_generation import FakeEvidence, FakeMsg
from tests.test_history import U, extracted, fact, message, plugin, source  # noqa: F401


@pytest.mark.asyncio
async def test_history_extract_system_prompt_byte_identical_and_compact_ordered_payload(
    plugin, monkeypatch  # noqa: F811
):
    plugin.config["history_batch_size"] = 1
    plugin.history.store.group(U, "技术交流群")
    # Seed two existing approved events with different timestamps to verify ascending ID order
    e1_id = plugin.history.store.add_event(
        U, fact("older", ts=1_700_000_000.0), status="approved"
    )
    e2_id = plugin.history.store.add_event(
        U, fact("newer", ts=1_700_100_000.0), status="approved"
    )
    assert e1_id < e2_id

    rows = [
        message(1, "第一批讨论项目方案", uid="1", ts=1_700_200_000.0),
        message(2, "第二批确认完成交付", uid="2", ts=1_700_200_100.0),
    ]
    db = await source(plugin, rows)
    captured: list[tuple[str, str]] = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured.append((prompt, system_prompt))
        msg_id = json.loads(prompt)["messages"][0]["id"]
        if msg_id == 2:
            return extracted(ids=(2,), uid="2", corroborated=False)
        return '{"events":[]}'

    monkeypatch.setattr(lifetime, "call_llm", fake_call_llm)
    try:
        assert await plugin.history.process_one(U, "back")
        assert await plugin.history.process_one(U, "back")

        assert len(captured) == 2
        prompt_1, sys_1 = captured[0]
        prompt_2, sys_2 = captured[1]

        # 1. system_prompt is strictly byte-identical across consecutive batches for the same group
        assert sys_1 == sys_2
        assert sys_1 == build_extract_system_prompt(U, "技术交流群")
        assert sys_1.startswith(SHARED_ARCHIVE_EXTRACTION_PREAMBLE)
        assert EXTRACT_PROMPT in sys_1
        assert "技术交流群" in sys_1
        assert len(sys_1.encode("utf-8")) >= 2500

        # 2. Compact JSON user prompt with explicit slow-to-fast key order
        for raw_prompt in (prompt_1, prompt_2):
            assert ": " not in raw_prompt and ", " not in raw_prompt
            parsed = json.loads(raw_prompt)
            assert list(parsed.keys()) == [
                "existing_events",
                "known_members",
                "messages",
            ]
            assert [ev["id"] for ev in parsed["existing_events"]] == [e1_id, e2_id]

        # 3. Output parsing and quota accounting stay intact
        assert plugin.history.store.job(U)["back_cursor"] == 2
        events_all = plugin.history.store.events(U, status=None, public=True)
        assert any(ev["title"] == "项目完成" for ev in events_all)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_culture_extract_system_prompt_byte_identical_and_shares_preamble_with_history(
    culture, monkeypatch  # noqa: F811
):
    culture.plugin.config["history_batch_size"] = 1
    culture.history.store.group(U, "技术交流群")

    rows = [
        message(1, "群里说蓝蘑菇就是项目延期", uid="1", ts=1_700_200_000.0),
        message(2, "蓝蘑菇确实指项目延期", uid="2", ts=1_700_200_060.0),
    ]
    db = await source(culture.plugin, rows)
    captured: list[tuple[str, str]] = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured.append((prompt, system_prompt))
        return '{"entries":[]}'

    monkeypatch.setattr(culture_service, "call_llm", fake_call_llm)
    monkeypatch.setattr(
        culture,
        "learning_clues",
        AsyncMock(
            return_value=[
                {"name": "beta梗", "meaning": "含义二"},
                {"name": "alpha梗", "meaning": "含义一"},
            ]
        ),
    )
    try:
        assert await culture.process_one(U, "back")
        assert await culture.process_one(U, "back")

        assert len(captured) == 2
        prompt_1, sys_1 = captured[0]
        prompt_2, sys_2 = captured[1]

        # Byte-identical system_prompt across consecutive culture batches
        assert sys_1 == sys_2
        assert sys_1 == build_culture_system_prompt(U, "技术交流群")
        assert CULTURE_PROMPT in sys_1
        assert "技术交流群" in sys_1
        assert len(sys_1.encode("utf-8")) >= 2500

        # Shares leading static preamble prefix with HistoryService extraction
        hist_sys = build_extract_system_prompt(U, "技术交流群")
        assert sys_1.startswith(SHARED_ARCHIVE_EXTRACTION_PREAMBLE)
        assert hist_sys.startswith(SHARED_ARCHIVE_EXTRACTION_PREAMBLE)

        # Compact JSON with explicit slow-to-fast key order & sorted clues
        for raw_prompt in (prompt_1, prompt_2):
            parsed = json.loads(raw_prompt)
            assert list(parsed.keys()) == [
                "existing",
                "learning_clues",
                "messages",
            ]
            assert [c["name"] for c in parsed["learning_clues"]] == ["alpha梗", "beta梗"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_culture_retry_keeps_system_prompt_identical_and_review_candidate_at_tail(
    culture, monkeypatch  # noqa: F811
):
    culture.history.store.group(U, "梗文化群")
    rows = [
        message(1, "绿雨伞是加班代号", uid="1", ts=1_700_200_000.0),
        message(2, "我也在用绿雨伞表示加班", uid="2", ts=1_700_200_060.0),
    ]
    db = await source(culture.plugin, rows)
    captured: list[tuple[str, str]] = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured.append((prompt, system_prompt))
        if len(captured) == 1:
            # Return one malformed candidate to queue a single-candidate retry
            bad = proposal(rows, name="绿雨伞")
            bad["entries"][0]["sources"][0]["id"] = 9999
            return json.dumps(bad, ensure_ascii=False)
        return json.dumps(proposal(rows, name="绿雨伞"), ensure_ascii=False)

    monkeypatch.setattr(culture_service, "call_llm", fake_call_llm)
    try:
        assert not await culture.process_one(U, "back")
        assert await culture.process_one(U, "back")

        assert len(captured) == 2
        assert captured[0][1] == captured[1][1]
        retry_payload = json.loads(captured[1][0])
        assert list(retry_payload.keys()) == [
            "existing",
            "learning_clues",
            "messages",
            "review_candidate",
        ]
        assert retry_payload["review_candidate"] == "绿雨伞"
        assert culture.store.entries(U)[1] == 1
    finally:
        await db.close()


def test_build_report_and_biography_keep_system_prompt_static_across_windows_and_members(
    plugin, monkeypatch  # noqa: F811
):
    plugin.history.store.group(U, "史记测试群")
    plugin.history.store.observe(U, "1", "甲")
    plugin.history.store.observe(U, "2", "乙")
    plugin.history.store.add_event(U, fact("ev1", uid="1", role=3, importance=3), status="approved")
    plugin.history.store.add_event(U, fact("ev2", uid="2", role=3, importance=3), status="approved")

    report_calls: list[tuple[str, str]] = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        report_calls.append((prompt, system_prompt))
        return "群史正文"

    monkeypatch.setattr(lifetime, "call_llm", fake_call_llm)

    asyncio.run(plugin.history.build_report(U, 7, False, "史记测试群", force=True))
    asyncio.run(
        plugin.history.build_report(
            U, 30, False, "史记测试群", force=True, topic="年度回顾"
        )
    )
    assert len(report_calls) == 2
    assert report_calls[0][1] == report_calls[1][1]
    p1 = json.loads(report_calls[0][0])
    p2 = json.loads(report_calls[1][0])
    assert list(p1.keys()) == ["facts", "window_days"]
    assert list(p2.keys()) == ["facts", "window_days", "topic"]
    assert p1["window_days"] == 7 and p2["window_days"] == 30

    bio_calls: list[tuple[str, str]] = []

    async def fake_bio_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        bio_calls.append((prompt, system_prompt))
        return "成员传记正文"

    monkeypatch.setattr(lifetime, "call_llm", fake_bio_llm)
    asyncio.run(plugin.history.biography(U, "1"))
    asyncio.run(plugin.history.biography(U, "2"))
    assert len(bio_calls) == 2
    assert bio_calls[0][1] == bio_calls[1][1]
    assert "史记测试群" in bio_calls[0][1]


def test_two_phase_chronicle_keeps_identical_system_prompt_and_volatile_tail(monkeypatch):
    calls: list[tuple[str, str]] = []

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        calls.append((prompt, system_prompt))
        return "2026-11-19 | 群内完成讨论" if len(calls) == 1 else "最终群史记"

    monkeypatch.setattr(generation, "call_llm", fake_call_llm)
    msgs = [
        FakeMsg("小明", 1700000000.0, "今天上线新版本"),
        FakeMsg("小红", 1700000200.0, "测试全部通过"),
    ]
    out = asyncio.run(
        generation.generate_chronicle(
            "ctx",
            U,
            FakeEvidence(msgs),
            "固定史官人设",
            14,
            style="official",
            enable_two_phase=True,
            topic="版本发布",
        )
    )
    assert out == "最终群史记"
    assert len(calls) == 2
    # Both phases share the exact same byte-identical system_prompt
    assert calls[0][1] == calls[1][1]
    # Phase 1 puts static task instruction ahead of volatile daily buckets
    assert calls[0][0].index("按天压缩") < calls[0][0].index("今天上线新版本")
    # Phase 2 puts volatile window_days and topic at the tail after summary content
    assert calls[1][0].index("群内完成讨论") < calls[1][0].index("最近 14 天")
    assert calls[1][0].index("最近 14 天") < calls[1][0].index("本期主题：版本发布")


def test_call_llm_normalizes_system_prompt_crlf_for_prefix_stability():
    seen: list[str] = []

    class Ctx:
        async def get_current_chat_provider_id(self, umo=None):
            return "p1"

        async def llm_generate(self, chat_provider_id=None, prompt=None, system_prompt=None):
            seen.append(system_prompt)
            return type("Resp", (), {"completion_text": "ok"})()

    ctx = Ctx()
    asyncio.run(llm_call.call_llm(ctx, U, "batch1", "line1\r\nline2"))
    asyncio.run(llm_call.call_llm(ctx, U, "batch2", "line1\nline2"))
    assert seen[0] == seen[1] == "line1\nline2"
