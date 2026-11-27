"""Wave 3 feature tests for astrbot_plugin_group_chronicle.

Covers:
1. Meme Evolution & Remix:
   - Upgraded /梗溯源 <词> calling visual_media.get_group_top_memes, building
     evolution card (origin dialogue + timeline + top memes + hot-term prompt),
     and graceful degradation when visual_media is absent.
   - /梗图演绎 <词> calling visual_media.generate_series_artwork(style="meme_remix",
     source_plugin="group_chronicle") and graceful degradation when unavailable.
2. /群史盲盒 & /那年今日:
   - /群史盲盒 draws masked public event or slang quiz, reveals on 揭晓,
     enforces hidden-member privacy & aborts reveal if privacy_revision changed.
   - /那年今日 finds prior-year calendar matches (events + slang), renders card,
     and supports low-frequency push_mode=True exact-match appendix.
3. Dynamics Event Bus + 3D World Keepsake Sync:
   - initialize() subscribes to chat_dynamics topic_archived; terminate() unsubscribes.
   - topic_archived ingestion enforces privacy filtering, deduplicates via fingerprint,
     records ring buffer cues & inserts pending candidate events into ArchiveStore.
   - publish_chronicle_milestone generates polaroid artwork via visual_media, persists
     artwork_path into latest chronicle edition, registers external keepsake in
     dynamics_world, and emits milestone_achieved on chat_dynamics Event Bus.
   - Approving high-importance event (importance >= 3) via 史记确认 or WebUI triggers
     milestone keepsake pipeline.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from core import blindbox, history_commands, meme_evolution, series_bridge
from core.blindbox import build_on_this_day, run_blindbox
from core.meme_evolution import build_meme_evolution_card, generate_meme_remix
from core.series_bridge import (
    ingest_archived_topic_event,
    publish_chronicle_milestone,
)

from .test_culture import culture, ingest, proposal, rows  # noqa: F401
from .test_history import Event, U, fact, plugin  # noqa: F401


def _attach_stars(plugin_inst, stars: dict[str, object]):
    wrapped = {
        name: NS(name=name, activated=True, star_cls=obj)
        for name, obj in stars.items()
    }
    plugin_inst.context.get_registered_star = lambda ident: wrapped.get(ident)
    plugin_inst.context.get_all_stars = lambda: wrapped


# ---------------------------------------------------------------------------
# 1. Meme Evolution (/梗溯源) & Meme Remix (/梗图演绎)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meme_evolution_tracing_with_visual_media_and_degradation(
    culture, monkeypatch, tmp_path  # noqa: F811
):
    now = time.time() - 3600
    material = [
        {"id": 1, "role": "user", "content": "群里说蓝蘑菇就是项目延期。", "uid": "1", "name": "甲", "ts": now, "metadata": {}},
        {"id": 2, "role": "user", "content": "蓝蘑菇指延期，这周又蓝蘑菇了。", "uid": "2", "name": "乙", "ts": now + 60, "metadata": {}},
        {"id": 3, "role": "user", "content": "蓝蘑菇已经成为本群固定用语了。", "uid": "3", "name": "丙", "ts": now + 120, "metadata": {}},
    ]
    raw = proposal(material, name="蓝蘑菇", meaning="指项目延期")
    raw["entries"][0]["source_ids"] = [1, 2, 3]
    raw["entries"][0]["timeline"] = [
        {"text": "首次用于调侃项目延期", "source_ids": [1], "event_at": None},
        {"text": "成为群内高频通用梗", "source_ids": [3], "event_at": None},
    ]
    db, _ = await ingest(culture, monkeypatch, material, raw)
    try:
        meme_img = tmp_path / "blue_mushroom.png"
        meme_img.write_bytes(b"png")
        card_img = tmp_path / "evolution_card.png"
        card_img.write_bytes(b"png")

        queries: list[dict] = []

        class FakeVisualMedia:
            async def get_group_top_memes(self, umo, *, keyword="", limit=3):
                queries.append({"umo": umo, "keyword": keyword, "limit": limit})
                return [
                    {
                        "id": "m1",
                        "label": "蓝蘑菇暴走表情",
                        "thumbnail_path": str(meme_img),
                        "usage_count": 9,
                        "tags": ["延期", "蓝蘑菇"],
                    }
                ]

        _attach_stars(culture.plugin, {"astrbot_plugin_visual_media": FakeVisualMedia()})
        culture.plugin.config["render_mode"] = "t2i"
        monkeypatch.setattr(
            meme_evolution,
            "render_text_or_image",
            AsyncMock(return_value=("md", str(card_img))),
        )

        res = await build_meme_evolution_card(culture.plugin, Event(uid="1"), "蓝蘑菇")
        assert queries and queries[0]["keyword"] == "蓝蘑菇"
        assert res["hot"] is True
        assert res["card_image_path"] == str(card_img)
        assert res["meme_images"] == [str(meme_img)]
        assert "蓝蘑菇暴走表情" in res["text"]
        assert "/梗图演绎 蓝蘑菇" in res["text"]

        # Graceful degradation when visual_media is absent and render_mode is off
        _attach_stars(culture.plugin, {})
        culture.plugin.config["render_mode"] = "off"
        degraded = await build_meme_evolution_card(culture.plugin, Event(uid="1"), "蓝蘑菇")
        assert degraded["card_image_path"] is None
        assert degraded["meme_images"] == []
        assert "蓝蘑菇" in degraded["text"]
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_meme_remix_calls_visual_media_and_degrades_gracefully(
    culture, monkeypatch, tmp_path  # noqa: F811
):
    material = rows()
    raw = proposal(material, name="蓝蘑菇", meaning="项目延期")
    db, _ = await ingest(culture, monkeypatch, material, raw)
    try:
        remix_img = tmp_path / "remix.png"
        remix_img.write_bytes(b"png")
        artwork_calls: list[dict] = []

        class FakeVisualMedia:
            async def generate_series_artwork(self, **kwargs):
                artwork_calls.append(kwargs)
                return {"ok": True, "image_path": str(remix_img), "caption": "角色演绎蓝蘑菇"}

        _attach_stars(culture.plugin, {"astrbot_plugin_visual_media": FakeVisualMedia()})
        res = await generate_meme_remix(culture.plugin, Event(uid="1"), "蓝蘑菇")
        assert res["ok"] is True
        assert res["degraded"] is False
        assert res["image_path"] == str(remix_img)
        assert res["from_culture"] is True
        assert artwork_calls[0]["style"] == "meme_remix"
        assert artwork_calls[0]["source_plugin"] == "group_chronicle"
        assert "项目延期" in artwork_calls[0]["prompt"]

        # Graceful degradation when visual_media unavailable
        _attach_stars(culture.plugin, {})
        culture.plugin.config["render_mode"] = "off"
        fallback = await generate_meme_remix(culture.plugin, Event(uid="1"), "蓝蘑菇")
        assert fallback["ok"] is True
        assert fallback["degraded"] is True
        assert fallback["image_path"] is None
        assert "蓝蘑菇" in fallback["text"]
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# 2. /群史盲盒 & /那年今日
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_blindbox_draw_reveal_and_privacy_guard(plugin):  # noqa: F811
    store = plugin.history.store
    now = time.time()
    store.observe(U, "1", "张三", now - 86400 * 10)
    store.observe(U, "2", "隐藏用户", now - 86400 * 10)
    store.add_event(
        U,
        {
            **fact("bb-public", uid="1"),
            "title": "通宵通关大冒险",
            "detail": "张三带队在群里直播通宵打通隐藏关卡",
            "participants": [{"uid": "1", "name": "张三", "role": 3}],
        },
        status="approved",
    )
    store.add_event(
        U,
        {
            **fact("bb-hidden", uid="2"),
            "title": "私密事件不可泄露",
            "detail": "隐藏用户的私密记录",
            "participants": [{"uid": "2", "name": "隐藏用户", "role": 3}],
        },
        status="approved",
    )
    store.hide(U, "2")

    # Draw event blindbox -> must never pick hidden member's event, must mask participant name
    q = await run_blindbox(plugin, Event("群史盲盒 大事", uid="1"), "大事", rng=random.Random(1))
    assert q["ok"] is True
    assert q["kind"] == "question"
    assert "【神秘群友】" in q["text"]
    assert "张三" not in q["text"]
    assert "私密事件不可泄露" not in q["text"]

    # Reveal returns full answer
    ans = await run_blindbox(plugin, Event("群史盲盒 揭晓", uid="1"), "揭晓")
    assert ans["ok"] is True
    assert ans["kind"] == "reveal"
    assert "通宵通关大冒险" in ans["text"]
    assert "张三" in ans["text"]

    # If privacy changes before reveal, reveal is refused safely
    await run_blindbox(plugin, Event("群史盲盒 大事", uid="1"), "大事", rng=random.Random(2))
    store.hide(U, "1")
    blocked = await run_blindbox(plugin, Event("群史盲盒 揭晓", uid="3"), "揭晓")
    assert blocked["ok"] is False
    assert blocked["kind"] == "privacy_changed"
    assert "隐私状态已变化" in blocked["text"]


@pytest.mark.asyncio
async def test_on_this_day_matches_prior_year_events_and_push_mode(
    plugin, monkeypatch, tmp_path  # noqa: F811
):
    tz = plugin._series_tz()
    now_dt = datetime(2026, 11, 27, 12, 0, tzinfo=tz)
    past_dt = datetime(2025, 11, 27, 20, 0, tzinfo=tz)
    other_dt = datetime(2025, 5, 1, 10, 0, tzinfo=tz)

    store = plugin.history.store
    store.observe(U, "1", "张三", past_dt.timestamp())
    store.add_event(
        U,
        {
            **fact("otd-1", uid="1"),
            "title": "群聊一周年纪念夜",
            "detail": "大家一起举办了首届群史大赏",
            "event_at": past_dt.timestamp(),
            "participants": [{"uid": "1", "name": "张三", "role": 3}],
        },
        status="approved",
    )
    store.add_event(
        U,
        {
            **fact("otd-other", uid="1"),
            "title": "五一踏青",
            "detail": "春季活动",
            "event_at": other_dt.timestamp(),
            "participants": [{"uid": "1", "name": "张三", "role": 1}],
        },
        status="approved",
    )

    card_path = tmp_path / "otd_card.png"
    card_path.write_bytes(b"png")
    plugin.config["render_mode"] = "t2i"
    monkeypatch.setattr(
        blindbox,
        "render_text_or_image",
        AsyncMock(return_value=("md", str(card_path))),
    )

    res = await build_on_this_day(
        plugin, U, group_name="测试群", render_card=True, push_mode=False, now_ts=now_dt.timestamp()
    )
    assert res["has_items"] is True
    assert res["exact"] is True
    assert res["image_path"] == str(card_path)
    assert "群聊一周年纪念夜" in res["text"]
    assert "1 年前" in res["text"]
    assert "五一踏青" not in res["text"]

    # Low-frequency push_mode=True returns has_items=False on non-matching date
    non_match = datetime(2026, 11, 28, 12, 0, tzinfo=tz)
    push_miss = await build_on_this_day(
        plugin, U, group_name="测试群", render_card=False, push_mode=True, now_ts=non_match.timestamp()
    )
    assert push_miss["has_items"] is False


# ---------------------------------------------------------------------------
# 3. Dynamics Event Bus + 3D World Keepsake Sync
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_event_bus_subscribe_ingest_topic_archived_and_unsubscribe(plugin):  # noqa: F811
    subscribed: list[tuple] = []
    unsubscribed: list[str] = []

    class FakeChatDynamics:
        def subscribe_dynamics_event(self, event_type, callback, *, owner=""):
            subscribed.append((event_type, callback, owner))
            return "tok-topic-1"

        def unsubscribe_dynamics_event(self, token):
            unsubscribed.append(token)
            return True

    _attach_stars(plugin, {"astrbot_plugin_chat_dynamics": FakeChatDynamics()})

    await plugin.initialize()
    assert len(subscribed) == 1
    assert subscribed[0][0] == "topic_archived"
    assert subscribed[0][2] == "group_chronicle"

    # Hide member 99 to verify privacy filtering
    plugin.history.store.observe(U, "99", "私密人")
    plugin.history.store.hide(U, "99")
    plugin.history.store.observe(U, "1", "张三")

    # 1. Valid archived topic creates deduplicated pending event & ring cue
    payload = {
        "umo": U,
        "topic_id": "t-2026-01",
        "title": "全员联调通宵夜",
        "summary": "群友们合力定位并修复了全部跨插件同步问题，现场欢呼声不断。",
        "message_count": 18,
        "participants": [{"uid": "1", "name": "张三"}, {"uid": "99", "name": "私密人"}],
    }
    r1 = await ingest_archived_topic_event(plugin, payload)
    assert r1["accepted"] is True
    assert r1["event_id"] is not None
    assert len(plugin.get_archived_topic_cues(U)) == 1
    # Duplicate delivery returns same deduplicated event_id
    r2 = await ingest_archived_topic_event(plugin, payload)
    assert r2["accepted"] is True
    assert r2["event_id"] == r1["event_id"]
    pending = plugin.history.store.events(U, status="pending", public=False)
    assert len(pending) == 1
    assert [p["uid"] for p in pending[0]["participants"]] == ["1"]

    # 2. Topic mentioning hidden member's name is privacy-filtered
    blocked = await ingest_archived_topic_event(
        plugin,
        {
            "umo": U,
            "topic_id": "t-2026-02",
            "title": "私密人的秘密讨论",
            "summary": "讨论了私密人的安排",
            "message_count": 10,
        },
    )
    assert blocked["accepted"] is False
    assert blocked["reason"] == "privacy_filtered"

    await plugin.terminate()
    assert unsubscribed == ["tok-topic-1"]


@pytest.mark.asyncio
async def test_milestone_keepsake_pipeline_and_event_approve_trigger(
    plugin, tmp_path  # noqa: F811
):
    polaroid_img = tmp_path / "polaroid.png"
    polaroid_img.write_bytes(b"png")
    artwork_calls: list[dict] = []
    keepsake_calls: list[dict] = []
    emitted_events: list[tuple[str, dict]] = []

    class FakeVisualMedia:
        async def generate_series_artwork(self, **kwargs):
            artwork_calls.append(kwargs)
            return {"ok": True, "image_path": str(polaroid_img)}

    class FakeWorld:
        async def register_external_keepsake(self, **kwargs):
            keepsake_calls.append(kwargs)
            return {"ok": True, "keepsake_id": "ks-1"}

    class FakeChatDynamics:
        async def emit_dynamics_event(self, event_type, payload):
            emitted_events.append((event_type, payload))
            return 2

    _attach_stars(
        plugin,
        {
            "astrbot_plugin_visual_media": FakeVisualMedia(),
            "astrbot_plugin_dynamics_world": FakeWorld(),
            "astrbot_plugin_chat_dynamics": FakeChatDynamics(),
        },
    )

    # Save a chronicle edition so attach_report_artwork can persist artwork_path into it
    plugin.history.store.save_report(
        U,
        "2026-W48:days=7",
        {
            "iso_year": 2026,
            "iso_week": 48,
            "group_name": "测试群",
            "chronicle_text": "# 第48周群史\n本周大事记。",
            "report_kind": "chronicle",
            "created_at": time.time(),
        },
    )

    res = await publish_chronicle_milestone(
        plugin,
        U,
        title="测试群 · 群史记（周卷）",
        summary="本周完成了三波次全系列联动升级。",
        kind="chronicle_edition",
        tab="editions",
        importance=3,
    )
    assert res["ok"] is True
    assert res["image_path"] == str(polaroid_img)
    assert artwork_calls[0]["style"] == "polaroid"
    assert artwork_calls[0]["source_plugin"] == "group_chronicle"
    assert len(keepsake_calls) == 1
    assert keepsake_calls[0]["source_plugin"] == "group_chronicle"
    assert keepsake_calls[0]["image_path"] == str(polaroid_img)
    assert "tab=editions" in keepsake_calls[0]["deep_link"]
    assert emitted_events[0][0] == "milestone_achieved"
    assert emitted_events[0][1]["image_path"] == str(polaroid_img)

    # Verify artwork_path was persisted onto the chronicle edition itself
    reports = plugin.history.store.reports(U)
    assert reports[0]["artwork_path"] == str(polaroid_img)
    assert "tab=editions" in reports[0]["keepsake_deep_link"]

    # Approving a high-importance pending event via 史记确认 triggers milestone keepsake sync
    plugin.history.store.observe(U, "1", "张三")
    eid = plugin.history.store.add_event(
        U,
        {
            **fact("major-ev", uid="1"),
            "title": "群内年度里程碑",
            "detail": "全体群友达成年度目标",
            "importance": 3,
        },
        status="pending",
    )
    reply = await history_commands.handle_command(
        plugin.history, Event(f"史记确认 {eid}", uid="1", admin=True)
    )
    assert "确认入史" in reply
    # Drain background task scheduled by 史记确认
    if plugin._bg_tasks:
        await asyncio.gather(*list(plugin._bg_tasks), return_exceptions=True)
    assert any(k["kind"] == "major_event" and "年度里程碑" in k["title"] for k in keepsake_calls)
    assert any(ev[0] == "milestone_achieved" and ev[1].get("event_id") == eid for ev in emitted_events)
