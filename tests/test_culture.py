"""Culture behavior tests against real SQLite source/archival stores."""

import copy
import json
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from core import culture_service, lm_bridge
from core.culture_contract import validate
from core.culture_store import CultureStore
from .test_history import Event, U, V, message, plugin, source  # noqa: F401


def rows():
    return [
        message(1, "群里说蓝蘑菇，就是项目延期的意思。", uid="1"),
        message(2, "蓝蘑菇指延期，我也一直这样用。", uid="2"),
    ]


def proposal(material=None, **overrides):
    material = material or rows()
    entry = dict(
        name="蓝蘑菇",
        meaning="项目延期",
        usage="讨论延期时",
        kind="slang",
        target_id=None,
        subjects=[],
        risk=False,
        conflict=False,
        aliases=[],
        origin=None,
        timeline=[],
        sources=[
            dict(id=m["id"], quote=m["content"], support="independent")
            for m in material
        ],
    )
    entry.update(overrides)
    return {"entries": [entry]}


@pytest.fixture
def culture(plugin, monkeypatch):  # noqa: F811 - pytest fixture injection
    plugin.config["culture_enabled"] = True
    plugin.history.store.group(U)
    monkeypatch.setattr(lm_bridge, "source_identity", lambda ctx: "synthetic-source")
    return plugin.culture


async def ingest(culture, monkeypatch, material=None, raw=None):
    material = material or rows()
    db = await source(culture.plugin, material)
    monkeypatch.setattr(
        culture_service,
        "call_llm",
        AsyncMock(return_value=json.dumps(raw or proposal(material))),
    )
    assert await culture.process_one(U)
    return db, culture.store.entries(U)[0][0]


def test_two_independent_authors_admit_meaning_not_origin():
    material = rows()
    e = validate(proposal(material), material)[0]
    assert e["admission"] == "approved"
    assert e["origin"] is None
    assert e["first_seen"] == min(m["ts"] for m in material)


@pytest.mark.parametrize(
    "variation",
    [
        "same_author",
        "repeat",
        "quoted",
        "reported",
        "nickname",
        "conflict",
        "risk",
        "origin",
    ],
)
def test_weak_or_sensitive_evidence_requires_review(variation):
    material = rows()
    raw = proposal(material)
    e = raw["entries"][0]
    if variation == "same_author":
        material[1]["uid"] = "1"
    elif variation == "repeat":
        material[1]["content"] = material[0]["content"]
        e["sources"][1]["quote"] = material[1]["content"]
    elif variation == "quoted":
        material[1]["_quoted"] = True
    elif variation == "reported":
        e["sources"][1]["support"] = "reported"
    elif variation == "nickname":
        e["kind"] = "nickname"
        e["subjects"] = ["1"]
    elif variation == "origin":
        e["origin"] = dict(text="最早由甲创造", source_ids=[1, 2], event_at=None)
    else:
        e[variation] = True
    result = validate(raw, material)[0]
    assert result["admission"] == "pending"
    if result["origin"]:
        assert not result["origin"]["confirmed"]


@pytest.mark.parametrize(
    "case",
    [
        "unknown_source",
        "fabricated_quote",
        "bot",
        "alias",
        "timeline",
        "cross_target",
        "name",
    ],
)
def test_fabricated_or_cross_scope_references_fail_whole_batch(case):
    material = rows()
    raw = proposal(material)
    e = raw["entries"][0]
    if case == "unknown_source":
        e["sources"][0]["id"] = 999
    elif case == "fabricated_quote":
        e["sources"][0]["quote"] = "凭空编造"
    elif case == "bot":
        material[0]["role"] = "assistant"
    elif case == "alias":
        e["aliases"] = [dict(name="未出现的别称", source_ids=[1])]
    elif case == "timeline":
        e["timeline"] = [dict(text="变化", source_ids=[99])]
    elif case == "name":
        e["name"] = "虚构词"
    else:
        e["target_id"] = 20
    with pytest.raises(ValueError):
        validate(raw, material)


@pytest.mark.asyncio
async def test_full_backfill_independent_of_history_cursor_and_restart(
    culture, monkeypatch
):
    culture.history.store.init_job(U, 2)
    culture.history.store.commit_batch(U, "back", (0, 0), 2, 0, [], [])
    db, item = await ingest(culture, monkeypatch)
    try:
        assert culture.store.job(U)["back_cursor"] == 2
        assert not await culture.process_one(U)
        reopened = CultureStore(culture.history.store)
        assert reopened.job(U)["back_cursor"] == 2
        assert reopened.entries(U)[1] == 1
        result = await culture.query(U, "1", "蓝蘑菇", "origin")
        assert result["entries"][0]["origin"] is None
        assert result["entries"][0]["unknowns"]
        assert culture.history.store.events(U) == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_failed_extraction_and_quota_never_advance(culture, monkeypatch):
    db = await source(culture.plugin, rows())
    try:
        model = AsyncMock(side_effect=ValueError("模型结果无效"))
        monkeypatch.setattr(culture_service, "call_llm", model)
        assert not await culture.step(U, "back")
        assert culture.store.job(U)["back_cursor"] == 0
        assert culture.store.job(U)["error"]
        # Failed call_llm refunds daily quota so transient errors do not drain quota.
        day_str = __import__("datetime").datetime.now(culture.history._series_tz()).date().isoformat()
        assert culture.history.store.quota(day_str) == 0
        # When daily quota is exhausted, process_one does not invoke LLM or advance.
        culture.plugin.config["history_daily_limit"] = 1
        assert culture.history.store.consume_quota(day_str, 1, kind="history")
        assert not await culture.process_one(U)
        assert model.await_count == 1
        assert culture.store.entries(U)[1] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_live_messages_before_backfill_and_multi_page(culture, monkeypatch):
    material = rows()
    db = await source(culture.plugin, material)
    culture.plugin.config["history_batch_size"] = 1
    culture.store.init_job(U, "synthetic-source", 2)
    await db.execute(
        "INSERT INTO messages VALUES(3,?,'user','今天来了新梗','3','丙',?, '{}')",
        (U, material[0]["ts"]),
    )
    await db.commit()
    model = AsyncMock(return_value='{"entries":[]}')
    monkeypatch.setattr(culture_service, "call_llm", model)
    try:
        assert await culture.process_one(U, "live")
        assert culture.store.job(U)["live_cursor"] == 3
        assert culture.store.job(U)["back_cursor"] == 0
        assert await culture.process_one(U, "back")
        assert culture.store.job(U)["back_cursor"] == 1
        assert await culture.process_one(U, "back")
        assert culture.store.job(U)["back_cursor"] == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_long_message_fragment_checkpoint(culture, monkeypatch):
    db = await source(culture.plugin, [message(1, "长消息" * 6000)])
    monkeypatch.setattr(
        culture_service, "call_llm", AsyncMock(return_value='{"entries":[]}')
    )
    try:
        assert await culture.process_one(U)
        job = culture.store.job(U)
        assert job["back_cursor"] == 0 and job["back_offset"] > 0
        for _ in range(8):
            await culture.process_one(U)
        assert culture.store.job(U)["back_cursor"] == 1
        assert culture.store.job(U)["back_offset"] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_source_switch_does_not_reuse_checkpoint(culture, monkeypatch):
    db, _ = await ingest(culture, monkeypatch)
    monkeypatch.setattr(lm_bridge, "source_identity", lambda _: "other-source")
    try:
        assert not await culture.step(U, "back")
        assert "来源已切换" in culture.store.job(U)["error"]
        result = await culture.query(U, "1", "蓝蘑菇")
        assert result["entries"] == [] and result["evidence_missing"]
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "deleted",
        "edited",
        "withdrawn",
        "author",
        "history_hidden",
        "culture_hidden",
        "corrected",
    ],
)
async def test_current_source_and_privacy_validation(culture, monkeypatch, change):
    db, item = await ingest(culture, monkeypatch)
    try:
        if change == "deleted":
            await db.execute("DELETE FROM messages WHERE id=1")
        elif change == "edited":
            await db.execute("UPDATE messages SET content='修改后的文本' WHERE id=1")
        elif change == "withdrawn":
            await db.execute(
                "UPDATE messages SET metadata=? WHERE id=1", ('{"recalled":true}',)
            )
        elif change == "author":
            await db.execute("UPDATE messages SET sender_id='9' WHERE id=1")
        elif change == "history_hidden":
            culture.history.store.observe(U, "1", "甲", rows()[0]["ts"])
            culture.history.store.hide(U, "1")
        elif change == "culture_hidden":
            await culture.action(
                U,
                "hide",
                "1",
                entry_id=item["id"],
                revision=item["revision"],
                note="本人隐藏",
            )
        else:
            await culture.action(
                U,
                "correct",
                "2",
                entry_id=item["id"],
                revision=item["revision"],
                note="解释需要纠正",
            )
        await db.commit()
        assert (await culture.query(U, "1", "蓝蘑菇"))["entries"] == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_cross_group_id_and_member_hiding_permissions(culture, monkeypatch):
    db, item = await ingest(culture, monkeypatch)
    try:
        assert (await culture.query(V, "1", str(item["id"])))["entries"] == []
        with pytest.raises(PermissionError):
            await culture.action(
                U,
                "hide",
                "9",
                entry_id=item["id"],
                revision=item["revision"],
                note="无关人请求",
            )
        with pytest.raises(ValueError):
            await culture.action(
                V,
                "hide",
                "1",
                entry_id=item["id"],
                revision=item["revision"],
                note="跨群",
            )
        culture.plugin.config["culture_enabled"] = False
        await culture.action(
            U,
            "hide",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="关闭后仍可隐藏",
        )
        assert culture.store.entry(U, item["id"])["status"] == "hidden"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_late_model_result_does_not_override_moderation(culture, monkeypatch):
    db = await source(culture.plugin, rows())

    async def model(*args, **kwargs):
        culture.store.set_paused(U, True)
        return json.dumps(proposal())

    monkeypatch.setattr(culture_service, "call_llm", model)
    try:
        assert not await culture.process_one(U)
        assert culture.store.job(U)["back_cursor"] == 0
        assert culture.store.entries(U)[1] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_receipt_guards_deferred_and_streamed_delivery(culture, monkeypatch):
    db, item = await ingest(culture, monkeypatch)
    try:
        event = Event()
        result = json.loads(await culture.tool(event, "蓝蘑菇", "origin"))
        assert result["entries"]
        original = (
            event.send.__closure__
        )  # install succeeded without replacing the evidence owner
        assert original
        await culture.action(
            U,
            "correct",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="需要修订",
        )
        with pytest.raises(RuntimeError, match="失效"):
            await event.send("尚未发送的旧解释")
        event2 = Event()
        event2.send_streaming = AsyncMock()
        updated = culture.store.entry(U, item["id"])
        await culture.action(
            U,
            "approve",
            "admin",
            entry_id=item["id"],
            revision=updated["revision"],
            note="核对后保留",
            admin=True,
        )
        await culture.tool(event2, "蓝蘑菇")

        async def stream():
            yield "开头"
            await db.execute("DELETE FROM messages WHERE id=1")
            await db.commit()
            yield "结尾"

        with pytest.raises(RuntimeError):
            await event2.send_streaming(stream())
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ambiguous_senses_and_merge_preserves_history(culture, monkeypatch):
    material = rows()
    raw = proposal(material)
    second = copy.deepcopy(raw["entries"][0])
    second["meaning"] = "另一个需要澄清的含义"
    raw["entries"].append(second)
    db, _ = await ingest(culture, monkeypatch, material, raw)
    try:
        result = await culture.query(U, "1", "蓝蘑菇")
        assert result["ambiguous"] and len(result["entries"]) == 2
        a, b = culture.store.entries(U)[0]
        await culture.action(
            U,
            "merge",
            "admin",
            entry_id=a["id"],
            revision=a["revision"],
            target_id=b["id"],
            target_revision=b["revision"],
            note="人工核对为同义",
            admin=True,
        )
        assert culture.store.entry(U, b["id"])["status"] == "pending"
        assert culture.store.revisions(U, b["id"])
        assert culture.store.entry(U, a["id"])["status"] == "merged"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ui_detail_edit_review_conflict_pause_and_resume(culture, monkeypatch):
    material = rows()
    raw = proposal(material, kind="nickname", subjects=["1"])
    db, item = await ingest(culture, monkeypatch, material, raw)
    web = culture.plugin.culture_web

    async def action(**kw):
        return await web.dispatch(dict(op="action", scope=U, **kw), "admin")

    try:
        listed = await web.dispatch(dict(op="list", scope=U, status="pending"), "admin")
        assert listed["total"] == 1
        await action(
            action="edit",
            id=item["id"],
            revision=item["revision"],
            patch=dict(meaning="修订后的解释"),
            note="核对原文",
        )
        with pytest.raises(ValueError, match="已更新"):
            await action(
                action="approve",
                id=item["id"],
                revision=item["revision"],
                note="过期审核",
            )
        current = culture.store.entry(U, item["id"])
        await action(
            action="approve", id=item["id"], revision=current["revision"], note="确认"
        )
        detail = await web.dispatch(dict(op="detail", scope=U, id=item["id"]), "admin")
        assert (
            detail["entry"]["meaning"] == "修订后的解释"
            and len(detail["revisions"]) == 2
        )
        await action(action="pause", note="暂停")
        assert not await culture.process_one(U)
        await action(action="resume", note="恢复")
        assert not culture.store.state(U)["paused"]
        with pytest.raises(PermissionError):
            culture.plugin.config["history_block_groups"] = [V]
            await web.dispatch(dict(op="list", scope=V), "admin")
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_hidden_revision_and_sources_not_exposed_by_panel(culture, monkeypatch):
    db, item = await ingest(culture, monkeypatch)
    try:
        await culture.action(
            U,
            "hide",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="本人请求",
        )
        detail = await culture.plugin.culture_web.dispatch(
            dict(op="detail", scope=U, id=item["id"]), "admin"
        )
        assert detail["entry"]["unavailable"]
        assert detail["revisions"] == []
        assert "蓝蘑菇" not in json.dumps(detail, ensure_ascii=False)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_learning_clues_are_optional_and_never_original_evidence(
    culture, monkeypatch
):
    def export(**kwargs):
        return dict(
            schema_version=1,
            scope=kwargs["scope"],
            terms=[dict(name="蓝蘑菇", meaning="线索")],
        )

    culture.plugin.context.get_registered_star = lambda _: NS(
        activated=True, star_cls=NS(get_culture_jargon_v1=export)
    )
    clues = await culture.learning_clues(U, "蓝蘑菇")
    assert clues == [dict(name="蓝蘑菇", meaning="线索")]
    culture.plugin.context.get_registered_star = lambda _: NS(
        activated=False, star_cls=NS(get_culture_jargon_v1=export)
    )
    assert await culture.learning_clues(U, "蓝蘑菇") == []


def test_additive_migration_backs_up_and_preserves_archive(culture):
    assert culture.history.store.path.with_name(
        "history.sqlite3.pre-culture-v1.bak"
    ).exists()
    with culture.history.store.connection() as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("SELECT version FROM culture_meta").fetchone()[0] == 1
    CultureStore(culture.history.store)
    assert culture.history.store.group(U)["umo"] == U


def test_memory_provider_conflict_and_disabled_registry(monkeypatch):
    from .test_lm_bridge import DummyContext, FakeStar, FakePlugin, FakeInitializer

    old = FakePlugin(FakeInitializer())
    new = FakePlugin(FakeInitializer())
    monkeypatch.setattr(lm_bridge, "_lm_get_active", lambda: None)
    context = DummyContext(
        star_by_alias={
            "astrbot_plugin_dynamics_memory": FakeStar(new),
            "LivingMemory": FakeStar(old),
        }
    )
    with pytest.raises(RuntimeError, match="冲突"):
        lm_bridge.detect_lm(context)
    context._star_by_alias["LivingMemory"].activated = False
    assert lm_bridge.detect_lm(context) is new


@pytest.mark.asyncio
async def test_public_receipt_binding_and_revision_validation(culture, monkeypatch):
    db, item = await ingest(culture, monkeypatch)
    try:
        result = await culture.plugin.get_culture_context(
            scope=U, member="1", query="蓝蘑菇"
        )
        receipt = result["receipt"]
        assert await culture.plugin.validate_culture_context(
            scope=U, member="1", receipt=receipt
        )
        assert not await culture.plugin.validate_culture_context(
            scope=V, member="1", receipt=receipt
        )
        assert not await culture.plugin.validate_culture_context(
            scope=U, member="2", receipt=receipt
        )
        await culture.action(
            U,
            "correct",
            "1",
            entry_id=item["id"],
            revision=item["revision"],
            note="纠正解释",
        )
        assert not await culture.plugin.validate_culture_context(
            scope=U, member="1", receipt=receipt
        )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_origin_review_and_distinct_event_record_times(culture, monkeypatch):
    material = rows()
    claimed_time = material[0]["ts"] - 86400
    raw = proposal(
        material,
        origin=dict(
            text="成员追述此前的项目延期事件", source_ids=[1, 2], event_at=claimed_time
        ),
        timeline=[dict(text="后来用于一般延期", source_ids=[2], event_at=None)],
    )
    db, item = await ingest(culture, monkeypatch, material, raw)
    try:
        assert item["status"] == "pending"
        assert item["origin"]["recorded_at"] != item["origin"]["event_at"]
        assert (await culture.query(U, "1", "蓝蘑菇"))["entries"] == []
        await culture.action(
            U,
            "approve",
            "admin",
            entry_id=item["id"],
            revision=item["revision"],
            note="已人工核对追述及发生时间",
            admin=True,
        )
        result = (await culture.query(U, "1", "蓝蘑菇", "origin"))["entries"][0]
        assert result["origin"]["event_at"] == claimed_time
        assert result["first_seen"] == min(m["ts"] for m in material)
        assert result["timeline"][0]["event_at"] is None
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_withdrawn_source_during_model_keeps_cursor(culture, monkeypatch):
    material = rows()
    db = await source(culture.plugin, material)

    async def model(*args, **kwargs):
        await db.execute("DELETE FROM messages WHERE id=1")
        await db.commit()
        return json.dumps(proposal(material))

    monkeypatch.setattr(culture_service, "call_llm", model)
    try:
        assert not await culture.step(U, "back")
        assert culture.store.job(U)["back_cursor"] == 0
        assert culture.store.entries(U)[1] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_already_hidden_history_sources_do_not_enter_model(culture, monkeypatch):
    from .test_history import fact

    material = rows()
    for m in material:
        culture.history.store.observe(U, m["uid"], m["name"], m["ts"])
    event = fact("1")
    event["sources"].append(dict(id="2", uid="2", hash="synthetic", excerpt="原始依据"))
    event_id = culture.history.store.add_event(U, event, status="approved")
    culture.history.store.hide(U, "1", event_id)
    db = await source(culture.plugin, material)
    model = AsyncMock(return_value='{"entries":[]}')
    monkeypatch.setattr(culture_service, "call_llm", model)
    try:
        assert await culture.process_one(U)
        model.assert_not_called()
        assert culture.store.entries(U)[1] == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_no_made_up_origin_from_manual_edit(culture, monkeypatch):
    db, item = await ingest(culture, monkeypatch)
    try:
        with pytest.raises(ValueError, match="无起源证据"):
            await culture.action(
                U,
                "edit",
                "admin",
                entry_id=item["id"],
                revision=item["revision"],
                note="尝试补写",
                patch=dict(origin_text="是甲发明的"),
                admin=True,
            )
        assert culture.store.entry(U, item["id"])["revision"] == item["revision"]
        assert not culture.store.revisions(U, item["id"])
    finally:
        await db.close()


def test_migration_backup_contains_preexisting_history(tmp_path):
    import sqlite3
    from core.archive_store import ArchiveStore
    from .test_history import fact

    archive = ArchiveStore(tmp_path / "history.sqlite3")
    original_id = archive.add_event(U, fact("retained"), status="approved")
    CultureStore(archive)
    assert archive.events(U)[0]["id"] == original_id
    with sqlite3.connect(str(archive.path) + ".pre-culture-v1.bak") as backup:
        assert backup.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert (
            backup.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='culture_entries'"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.asyncio
async def test_tool_guard_failure_never_releases_evidence(culture, monkeypatch):
    db, _ = await ingest(culture, monkeypatch)
    try:
        event = Event()
        event.send = None
        result = json.loads(await culture.tool(event, "蓝蘑菇"))
        assert not result["available"] and result["entries"] == []
        assert not getattr(event, "_culture_guard_installed_v1", False)
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manual", [False, True], ids=["auto-accumulate", "preserve-human-review"]
)
async def test_independent_support_across_pages(culture, monkeypatch, manual):
    material = rows()
    db = await source(culture.plugin, material)
    culture.plugin.config["history_batch_size"] = 1
    model = AsyncMock(side_effect=[json.dumps(proposal([m])) for m in material])
    monkeypatch.setattr(culture_service, "call_llm", model)
    try:
        assert await culture.process_one(U)
        item = culture.store.entries(U)[0][0]
        assert item["status"] == "pending"
        if manual:
            await culture.action(
                U,
                "edit",
                "admin",
                entry_id=item["id"],
                revision=item["revision"],
                note="需要人工复核",
                patch=dict(meaning=item["meaning"]),
                admin=True,
            )
        assert await culture.process_one(U)
        updated = culture.store.entry(U, item["id"])
        assert updated["status"] == ("pending" if manual else "approved")
        assert len(updated["sources"]) == (1 if manual else 2)
        assert len((await culture.query(U, "1", "蓝蘑菇"))["entries"]) == (
            0 if manual else 1
        )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_command_correct_and_no_entry_unknown(culture, monkeypatch):
    db, item = await ingest(culture, monkeypatch)
    try:
        result = await culture.command(Event(), "梗溯源", "不存在的梗")
        assert "不知道" in result
        answer = await culture.command(Event(), "梗溯源", str(item["id"]))
        assert "起源：未知" in answer and "来源 #1" in answer
        text = await culture.command(
            Event(), "文化纠正", f"{item['id']} 这个含义过时了"
        )
        assert "等待审核" in text
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_shared_quota_reserves_history_and_precheck_saves_quota(
    culture, monkeypatch
):
    """Culture caps at daily_limit - reserved_history (10 of 20), and precheck abort saves quota."""
    from datetime import datetime, timedelta, timezone

    day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
    store = culture.history.store

    # Test precheck abort inside process_one while quota is available: token change during learning_clues saves quota.
    material = rows()
    db = await source(culture.plugin, material)
    before_q = store.quota(day)

    async def flip_privacy_in_clues(umo, query):
        culture.history.store.observe(U, "99", "匿名者")
        culture.history.store.hide(U, "99", None)
        return []

    llm_mock = AsyncMock(
        side_effect=AssertionError("LLM must not run on changed token")
    )
    monkeypatch.setattr(culture, "learning_clues", flip_privacy_in_clues)
    monkeypatch.setattr(culture_service, "call_llm", llm_mock)
    try:
        assert await culture.process_one(U) is False
        assert store.quota(day) == before_q
        llm_mock.assert_not_called()
    finally:
        await db.close()

    # Simulate 10 culture calls consuming quota; the 11th must leave 10 slots reserved for history.
    for _ in range(10):
        assert store.consume_quota(day, 20, kind="culture", reserved_history=10)
    assert not store.consume_quota(day, 20, kind="culture", reserved_history=10)
    assert store.consume_quota(day, 20, kind="history")
    assert store.quota_breakdown(day) == {"total": 11, "history": 1, "culture": 10}


@pytest.mark.asyncio
async def test_batch_valid_sources_reduces_queries_and_matches_web_detail(
    culture, monkeypatch
):
    """Multi-entry and multi-revision web inspection batches history_sources lookups."""
    material = [
        message(1, "蓝蘑菇指项目延期，团队都在用。", uid="1"),
        message(2, "确实，蓝蘑菇就是延期的黑话。", uid="2"),
        message(3, "红蘑菇表示提前上线。", uid="1"),
        message(4, "红蘑菇拿来指提前完成。", uid="2"),
    ]
    db = await source(culture.plugin, material)
    culture.plugin.config["history_batch_size"] = 2
    p1 = proposal(material[:2])
    p2 = proposal(material[2:], name="红蘑菇", meaning="提前完成")
    monkeypatch.setattr(
        culture_service,
        "call_llm",
        AsyncMock(side_effect=[json.dumps(p1), json.dumps(p2)]),
    )
    orig_sources = lm_bridge.history_sources
    calls = []

    async def spy_history_sources(ctx, umo, ids):
        calls.append(list(ids))
        return await orig_sources(ctx, umo, ids)

    monkeypatch.setattr(lm_bridge, "history_sources", spy_history_sources)
    try:
        assert await culture.process_one(U)
        assert await culture.process_one(U)
        entries, _ = culture.store.entries(U)
        assert len(entries) == 2

        calls.clear()
        res = await culture.plugin.culture_web.dispatch(
            {"op": "list", "scope": U}, "human:admin"
        )
        assert len(res["entries"]) == 2
        # Single batched history_sources lookup for both entries.
        assert len(calls) == 1
        assert set(calls[0]) == {"1", "2", "3", "4"}

        # Edit entry twice to create multiple revisions and verify detail batches revision checks.
        e0 = entries[0]
        await culture.action(
            U,
            "edit",
            "admin",
            entry_id=e0["id"],
            revision=e0["revision"],
            note="rev1",
            patch={"meaning": "含义一"},
            admin=True,
        )
        e0_v2 = culture.store.entry(U, e0["id"])
        await culture.action(
            U,
            "edit",
            "admin",
            entry_id=e0_v2["id"],
            revision=e0_v2["revision"],
            note="rev2",
            patch={"meaning": "含义二"},
            admin=True,
        )
        calls.clear()
        detail_res = await culture.plugin.culture_web.dispatch(
            {"op": "detail", "scope": U, "id": e0["id"]}, "human:admin"
        )
        assert len(detail_res["revisions"]) >= 2
        assert len(calls) == 2
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_mixed_page_commits_valid_candidate_and_retries_only_rejected_one(culture, monkeypatch):
    material = rows() + [message(3, "绿雨伞表示临时改变计划。", uid="3")]
    db = await source(culture.plugin, material)
    good = proposal(material[:2])["entries"][0]
    corrected = proposal(material[2:], name="绿雨伞", meaning="改变计划")["entries"][0]
    bad = copy.deepcopy(corrected)
    bad["sources"][0]["quote"] = "一段不存在的原话"
    model = AsyncMock(side_effect=[json.dumps({"entries": [good, bad]}),
                                   json.dumps({"entries": [corrected]})])
    monkeypatch.setattr(culture_service, "call_llm", model)
    try:
        assert await culture.process_one(U)
        assert culture.store.entries(U)[1] == 1
        assert culture.store.job(U)["back_cursor"] == 0
        assert culture.store.job(U)["back_pending_review"] == 1
        culture.store = CultureStore(culture.history.store)
        assert await culture.process_one(U)
        retry_input = json.loads(model.await_args.args[2])
        assert retry_input["review_candidate"] == "绿雨伞"
        assert [str(m["id"]) for m in retry_input["messages"]] == ["3"]
        assert culture.store.entries(U)[1] == 2
        assert culture.store.job(U)["back_cursor"] == 3
        assert culture.store.job(U)["processed"] == 3
        assert culture.store.job(U)["back_pending_review"] == 0
    finally:
        await db.close()
