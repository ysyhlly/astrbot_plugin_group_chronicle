"""Unit tests for v2.0 cross-plugin backend hooks and series_panels presence aggregation."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS

import pytest

import series_panels
from core import lm_bridge

from .test_culture import culture, ingest, proposal, rows  # noqa: F401
from .test_history import U, V, message, plugin  # noqa: F401


@pytest.mark.asyncio
async def test_get_series_summary_counts_pending_disputed_and_degrades(
    culture, monkeypatch  # noqa: F811
):
    plugin_inst = culture.plugin
    # Empty state: 0 pending -> accent tone
    empty_summary = plugin_inst.get_series_summary()
    assert empty_summary["badge"]["count"] == 0
    assert empty_summary["badge"]["tone"] == "accent"
    assert isinstance(empty_summary["pulse"]["headline"], str)

    # Ingest one approved entry and one pending nickname entry
    material = rows()
    raw = proposal(material)
    pending_entry = proposal(material, kind="nickname", subjects=["1"])["entries"][0]
    pending_entry["name"] = "测试外号"
    pending_entry["meaning"] = "另一个群内称呼"
    raw["entries"].append(pending_entry)

    # Include "测试外号" in material so validation accepts it
    material2 = [
        message(1, "群里说蓝蘑菇，就是项目延期的意思。顺便提一下测试外号。", uid="1"),
        message(2, "蓝蘑菇指延期，我也一直这样用。测试外号也是这么叫的。", uid="2"),
    ]
    raw = proposal(material2)
    p2 = proposal(
        material2,
        name="测试外号",
        meaning="另一个群内称呼",
        kind="nickname",
        subjects=["1"],
    )["entries"][0]
    raw["entries"].append(p2)

    db, _ = await ingest(culture, monkeypatch, material2, raw)
    try:
        summary = plugin_inst.get_series_summary()
        assert summary["badge"]["count"] == 1
        assert summary["badge"]["tone"] == "warm"
        assert "1 条待核" in summary["badge"]["label"]
        assert summary["pulse"]["updated_at"]

        # Mark the approved entry as disputed -> 2 pending/disputed
        entries, _ = culture.store.entries(U)
        approved = next(e for e in entries if e["status"] == "approved")
        await culture.action(
            U,
            "correct",
            "2",
            entry_id=approved["id"],
            revision=approved["revision"],
            note="需要核对",
        )
        summary2 = plugin_inst.get_series_summary()
        assert summary2["badge"]["count"] == 2
        assert summary2["badge"]["tone"] == "warm"
        assert "2 条待核" in summary2["badge"]["label"]

        # Privacy hide member 1 -> both entries involve uid 1, so blocked from counts
        culture.history.store.observe(U, "1", "甲", material2[0]["ts"])
        culture.history.store.hide(U, "1")
        summary3 = plugin_inst.get_series_summary()
        assert summary3["badge"]["count"] == 0
        assert summary3["badge"]["tone"] == "accent"

        # Terminating plugin degrades safely without raising
        plugin_inst._terminating = True
        degraded = plugin_inst.get_series_summary()
        assert degraded["badge"]["count"] == 0
        assert degraded["badge"]["tone"] == "muted"
    finally:
        plugin_inst._terminating = False
        await db.close()


@pytest.mark.asyncio
async def test_scope_status_and_optimistic_toggle_respect_allowlist(
    culture, monkeypatch  # noqa: F811
):
    plugin_inst = culture.plugin
    db, _ = await ingest(culture, monkeypatch)
    try:
        status = await plugin_inst.get_series_scope_status(U)
        assert status["enabled"] is True
        assert status["supported"] is True
        assert status["activity_today"] >= 1
        assert "运行中" in status["summary"]
        ver0 = status["version"]

        # Pause with matching version
        res_pause = await plugin_inst.set_series_scope_enabled(
            U, False, expected_version=ver0
        )
        assert res_pause["ok"] is True
        assert res_pause["enabled"] is False
        assert res_pause["version"] != ver0

        status_paused = await plugin_inst.get_series_scope_status(U)
        assert status_paused["enabled"] is False
        assert status_paused["supported"] is True
        assert "已暂停" in status_paused["summary"]

        # Stale expected_version triggers optimistic lock conflict
        conflict = await plugin_inst.set_series_scope_enabled(
            U, True, expected_version=ver0
        )
        assert conflict["ok"] is False
        assert conflict.get("conflict") is True

        # Resume with current version succeeds
        res_resume = await plugin_inst.set_series_scope_enabled(
            U, True, expected_version=res_pause["version"]
        )
        assert res_resume["ok"] is True
        assert res_resume["enabled"] is True

        # Disallowed group cannot be enabled via toggle
        plugin_inst.config["history_block_groups"] = [V]
        blocked_status = await plugin_inst.get_series_scope_status(V)
        assert blocked_status["enabled"] is False
        assert blocked_status["supported"] is False
        blocked_toggle = await plugin_inst.set_series_scope_enabled(V, True)
        assert blocked_toggle["ok"] is False

        # Global culture_enabled=False prevents toggle
        plugin_inst.config["culture_enabled"] = False
        disabled_status = await plugin_inst.get_series_scope_status(U)
        assert disabled_status["enabled"] is False
        assert disabled_status["supported"] is False
        disabled_toggle = await plugin_inst.set_series_scope_enabled(U, True)
        assert disabled_toggle["ok"] is False
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_series_search_items_filters_privacy_and_scope(
    culture, monkeypatch  # noqa: F811
):
    plugin_inst = culture.plugin
    db, item = await ingest(culture, monkeypatch)
    try:
        # Empty query returns []
        assert await plugin_inst.series_search_items("") == []

        # Match by name & meaning
        hits = await plugin_inst.series_search_items("蓝蘑菇", scope=U, limit=5)
        assert len(hits) == 1
        hit = hits[0]
        assert hit["plugin_id"] == "astrbot_plugin_group_chronicle"
        assert hit["page"] == "culture"
        assert hit["kind"] == "culture_entry"
        assert "蓝蘑菇" in hit["title"]
        assert hit["deep_link"]["scope"] == U
        assert hit["deep_link"]["entry_id"] == item["id"]

        # Meaning search works across all allowed scopes when scope=""
        hits_meaning = await plugin_inst.series_search_items("项目延期")
        assert len(hits_meaning) == 1

        # Disallowed scope returns []
        plugin_inst.config["history_block_groups"] = [V]
        assert await plugin_inst.series_search_items("蓝蘑菇", scope=V) == []

        # Hidden entry is never returned
        await culture.action(
            U,
            "hide",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="隐私隐藏",
        )
        assert await plugin_inst.series_search_items("蓝蘑菇", scope=U) == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_get_journal_events_returns_today_culture_and_editions(
    culture, monkeypatch  # noqa: F811
):
    plugin_inst = culture.plugin
    db, item = await ingest(culture, monkeypatch)
    try:
        # Save a chronicle edition recorded today
        now = time.time()
        plugin_inst.history.store.save_report(
            U,
            "test-edition-today",
            {
                "umo": U,
                "title": "本周群史记",
                "summary": "记录了蓝蘑菇等群内热点。",
                "created_at": now,
                "archive": True,
                "cache_key": "test-edition-today",
            },
        )
        events = await plugin_inst.get_journal_events(scope=U, date="today", limit=10)
        assert len(events) == 2
        categories = {ev["category"] for ev in events}
        assert categories == {"group_chronicle"}
        assert all(ev["source_label"] == "群文化沿革" for ev in events)
        assert all(
            ev["target_plugin"] == "astrbot_plugin_group_chronicle"
            and ev["target_route"] == "culture"
            for ev in events
        )
        tones = {ev["tone"] for ev in events}
        assert "accent" in tones and "info" in tones

        # Different date yields empty list
        old_events = await plugin_inst.get_journal_events(
            scope=U, date="2020-01-01", limit=10
        )
        assert old_events == []

        # Hide culture entry -> only edition remains
        await culture.action(
            U,
            "hide",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="隐藏",
        )
        after_hide = await plugin_inst.get_journal_events(
            scope=U, date="today", limit=10
        )
        assert len(after_hide) == 1
        assert after_hide[0]["tone"] == "info"
    finally:
        await db.close()


def test_series_panels_presence_badge_pulse_ttl_and_standalone(monkeypatch, tmp_path):
    series_panels.invalidate_summary_cache()

    labels = {row[0]: row[1] for row in series_panels.CATALOG}
    assert labels["astrbot_plugin_chat_dynamics"] == "对话主控"
    assert labels["astrbot_plugin_group_chronicle"] == "群文化沿革"

    calls = {"chronicle": 0}

    class ChronicleOwner:
        def get_dynamics_capabilities(self):
            return {"schema_version": 1, "ready": True, "enabled": True}

        def get_series_summary(self):
            calls["chronicle"] += 1
            return {
                "badge": {"count": 3, "tone": "warm", "label": "3 条待核"},
                "pulse": {
                    "headline": "3 条待审核/纠正",
                    "updated_at": datetime.now(timezone(timedelta(hours=8))).isoformat(
                        timespec="seconds"
                    ),
                },
            }

    class PlainOwner:
        def get_dynamics_capabilities(self):
            return {"schema_version": 1, "ready": True, "enabled": True}

    class BrokenOwner:
        def get_dynamics_capabilities(self):
            return {"schema_version": 1, "ready": True, "enabled": True}

        def get_series_summary(self):
            raise RuntimeError("boom")

    class AsyncOwner:
        def get_dynamics_capabilities(self):
            return {"schema_version": 1, "ready": True, "enabled": True}

        async def get_series_summary(self):
            return {"badge": {"count": 9, "tone": "warm", "label": "async"}}

    chronicle_owner = ChronicleOwner()
    plain_owner = PlainOwner()
    broken_owner = BrokenOwner()
    async_owner = AsyncOwner()

    ctx = NS(
        get_all_stars=lambda: {
            "gc": NS(
                name="astrbot_plugin_group_chronicle",
                activated=True,
                star_cls=chronicle_owner,
            ),
            "cd": NS(
                name="astrbot_plugin_chat_dynamics",
                activated=True,
                star_cls=plain_owner,
            ),
            "dl": NS(
                name="astrbot_plugin_dynamics_learning",
                activated=True,
                star_cls=broken_owner,
            ),
            "dw": NS(
                name="astrbot_plugin_dynamics_world",
                activated=True,
                star_cls=async_owner,
            ),
        }
    )

    first = series_panels.presence(ctx, "astrbot_plugin_group_chronicle", root=tmp_path)
    by_id = {p["id"]: p for p in first["plugins"]}
    assert by_id["astrbot_plugin_group_chronicle"]["badge"] == {
        "count": 3,
        "tone": "warm",
        "label": "3 条待核",
    }
    assert by_id["astrbot_plugin_group_chronicle"]["pulse"]["headline"] == "3 条待审核/纠正"
    assert by_id["astrbot_plugin_chat_dynamics"]["badge"] is None
    assert by_id["astrbot_plugin_chat_dynamics"]["pulse"] is None
    assert by_id["astrbot_plugin_dynamics_learning"]["badge"] is None
    assert by_id["astrbot_plugin_dynamics_world"]["badge"] is None
    # Missing sibling degrades gracefully
    assert by_id["astrbot_plugin_dynamics_games"]["state"] == "missing"
    assert by_id["astrbot_plugin_dynamics_games"]["badge"] is None
    assert calls["chronicle"] == 1

    # Second call within 10s TTL hits cache
    series_panels.presence(ctx, "astrbot_plugin_group_chronicle", root=tmp_path)
    assert calls["chronicle"] == 1

    # Invalidating cache triggers fresh read
    series_panels.invalidate_summary_cache("astrbot_plugin_group_chronicle")
    series_panels.presence(ctx, "astrbot_plugin_group_chronicle", root=tmp_path)
    assert calls["chronicle"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "deleted",
        "edited",
        "recalled",
        "withdrawn",
        "author",
        "source_switched",
    ],
)
async def test_series_search_and_journal_reverify_sources_on_invalidation(
    culture, monkeypatch, change  # noqa: F811
):
    """Federated search and journal must exclude entries whose sources are deleted/edited/recalled/switched."""
    plugin_inst = culture.plugin
    # Seed two independent entries backed by separate source messages so we can verify
    # only the invalidated entry is filtered out (except when active memory source switches).
    material = [
        message(1, "群里说蓝蘑菇，就是项目延期的意思。", uid="1"),
        message(2, "蓝蘑菇指延期，我也一直这样用。", uid="2"),
        message(3, "大家管加班夜宵叫红苹果，已经成为固定梗。", uid="1"),
        message(4, "红苹果确实指加班夜宵，每周五都会提。", uid="2"),
    ]
    raw = proposal(material[:2], name="蓝蘑菇", meaning="项目延期")
    second = proposal(
        material[2:],
        name="红苹果",
        meaning="加班夜宵",
        kind="nickname",
        subjects=["1"],
    )["entries"][0]
    raw["entries"].append(second)

    db, _ = await ingest(culture, monkeypatch, material, raw)
    try:
        # Also save a chronicle edition recorded today to ensure editions stay visible
        now = time.time()
        plugin_inst.history.store.save_report(
            U,
            "edition-source-check",
            {
                "umo": U,
                "title": "本周群史记",
                "summary": "记录了本周群聊大事。",
                "created_at": now,
                "archive": True,
                "cache_key": "edition-source-check",
            },
        )

        # Baseline: both entries appear in federated search and today's journal
        assert len(await plugin_inst.series_search_items("蓝蘑菇", scope=U)) == 1
        assert len(await plugin_inst.series_search_items("红苹果", scope=U)) == 1
        baseline_events = await plugin_inst.get_journal_events(
            scope=U, date="today", limit=20
        )
        assert any("蓝蘑菇" in ev["title"] for ev in baseline_events)
        assert any("红苹果" in ev["title"] for ev in baseline_events)
        assert any(ev["tone"] == "info" for ev in baseline_events)

        # Invalidate message #1 (cited by "蓝蘑菇") or switch active memory source
        if change == "deleted":
            await db.execute("DELETE FROM messages WHERE id=1")
            await db.commit()
        elif change == "edited":
            await db.execute("UPDATE messages SET content='修改后的无关文本' WHERE id=1")
            await db.commit()
        elif change == "recalled":
            await db.execute(
                "UPDATE messages SET metadata=? WHERE id=1", ('{"recalled":true}',)
            )
            await db.commit()
        elif change == "withdrawn":
            await db.execute(
                "UPDATE messages SET metadata=? WHERE id=1", ('{"withdrawn":true}',)
            )
            await db.commit()
        elif change == "author":
            await db.execute("UPDATE messages SET sender_id='99' WHERE id=1")
            await db.commit()
        elif change == "source_switched":
            monkeypatch.setattr(lm_bridge, "source_identity", lambda _: "other-source")

        # 1. Federated search (both scoped and global) must NOT return "蓝蘑菇"
        assert await plugin_inst.series_search_items("蓝蘑菇", scope=U) == []
        assert await plugin_inst.series_search_items("项目延期", scope="") == []

        # 2. World journal (both scoped and global) must NOT return "蓝蘑菇"
        scoped_journal = await plugin_inst.get_journal_events(
            scope=U, date="today", limit=20
        )
        global_journal = await plugin_inst.get_journal_events(
            scope="", date="today", limit=20
        )
        assert all("蓝蘑菇" not in ev["title"] for ev in scoped_journal)
        assert all("项目延期" not in ev["subtitle"] for ev in scoped_journal)
        assert all("蓝蘑菇" not in ev["title"] for ev in global_journal)
        # Chronicle edition remains intact
        assert any(ev["tone"] == "info" for ev in scoped_journal)

        # 3. Check the unaffected pending entry "红苹果"
        if change == "source_switched":
            # Active memory source switch invalidates all entries from the previous source
            assert await plugin_inst.series_search_items("红苹果", scope=U) == []
            assert all("红苹果" not in ev["title"] for ev in scoped_journal)
        else:
            # Message #1 invalidation must not break unaffected entry "红苹果"
            assert len(await plugin_inst.series_search_items("红苹果", scope=U)) == 1
            assert any("红苹果" in ev["title"] for ev in scoped_journal)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_series_search_and_journal_reverify_pending_entry_sources(
    culture, monkeypatch  # noqa: F811
):
    """Pending/disputed entries also undergo source verification in search and journal."""
    plugin_inst = culture.plugin
    material = [
        message(1, "群里管老王叫茶博士，他泡茶很讲究。", uid="1"),
        message(2, "确实大家都叫他茶博士。", uid="2"),
    ]
    raw = proposal(
        material,
        name="茶博士",
        meaning="擅长泡茶的群友外号",
        kind="nickname",
        subjects=["1"],
    )
    db, item = await ingest(culture, monkeypatch, material, raw)
    try:
        assert item["status"] == "pending"
        assert len(await plugin_inst.series_search_items("茶博士", scope=U)) == 1
        assert any(
            "茶博士" in ev["title"]
            for ev in await plugin_inst.get_journal_events(scope=U, date="today")
        )

        # Recall message #2 -> pending entry disappears from both search and journal
        await db.execute(
            "UPDATE messages SET metadata=? WHERE id=2", ('{"recalled":true}',)
        )
        await db.commit()
        assert await plugin_inst.series_search_items("茶博士", scope=U) == []
        assert (
            await plugin_inst.get_journal_events(scope=U, date="today") == []
        )
    finally:
        await db.close()
