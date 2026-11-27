"""v0.2.0 behavioral acceptance with synthetic identities and source databases."""

import asyncio
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import aiosqlite
import pytest
import main
from astrbot.api import sp
from astrbot.api.message_components import At
from core import lm_bridge, lifetime
from core.archive_store import ArchiveStore, EditionStore
from core.deliver import deliver_text
from core.history_capture import HistoryCaptureFilter, set_active
from core.history_commands import administrator, handle_command, revise_payload
from core.lifetime import bounded_facts, pack_messages, scope_lists, validate_extraction
from core.store import ChronicleStore

U = "synthetic:GroupMessage:100"
V = "synthetic:GroupMessage:200"


def run(coro):
    return asyncio.run(coro)


def fact(key, *, uid="1", role=1, importance=1, ts=None, positive=True, kind="project"):
    return {
        "fingerprint": str(key),
        "title": "项目" + str(key),
        "detail": "完成有依据的群项目",
        "kind": kind,
        "importance": importance,
        "positive": positive,
        "risk": False,
        "event_at": ts if ts is not None else time.time() - 100,
        "ongoing": False,
        "participants": [{"uid": uid, "name": "甲", "role": role}],
        "unknown_participants": [],
        "followups": [],
        "admission": "approved",
        "sources": [
            {"id": str(key), "uid": uid, "hash": "synthetic", "excerpt": "实际证据"}
        ],
    }


def message(key, content="完成项目", *, uid="1", role="user", ts=None):
    return {
        "id": key,
        "content": content,
        "uid": uid,
        "name": "甲" if uid == "1" else "乙",
        "role": role,
        "ts": ts if ts is not None else time.time() - 100,
        "metadata": {},
        "fragment": 0,
    }


def extracted(
    ids=(1, 2),
    *,
    uid="1",
    role=1,
    risk=False,
    corroborated=True,
    kind="project",
    merge_into=None,
):
    return json.dumps(
        {
            "events": [
                {
                    "title": "项目完成",
                    "detail": "群内协作项目已完成",
                    "kind": kind,
                    "importance": 1,
                    "positive": True,
                    "risk": risk,
                    "corroborated": corroborated,
                    "participants": [{"uid": uid, "role": role}],
                    "source_ids": list(ids),
                    "merge_into": merge_into,
                }
            ]
        },
        ensure_ascii=False,
    )


class Event:
    def __init__(self, text="", uid="1", admin=False, umo=U, mentions=(), raw=None):
        self.message_str, self.unified_msg_origin = text, umo
        self.uid, self.admin, self.mentions = uid, admin, list(mentions)
        self.message_obj = NS(
            raw_message=raw or {},
            timestamp=time.time(),
            message_id=hashlib.sha256(text.encode()).hexdigest(),
        )
        self.send = AsyncMock()

    def get_sender_id(self):
        return self.uid

    def get_sender_name(self):
        return "甲" if self.uid == "1" else "乙"

    def get_self_id(self):
        return "0"

    def get_group_id(self):
        return self.unified_msg_origin.rsplit(":", 1)[-1]

    def get_group_name(self):
        return "合成测试群"

    def get_messages(self):
        return [At(qq=i) for i in self.mentions]

    def is_admin(self):
        return self.admin

    def plain_result(self, text):
        return text


@pytest.fixture
def store(tmp_path):
    return ArchiveStore(tmp_path / "history.sqlite3")


@pytest.fixture
def plugin(tmp_path, monkeypatch):
    monkeypatch.setattr(
        main.StarTools, "get_data_dir", lambda _: str(tmp_path / "data")
    )
    context = NS(
        send_message=AsyncMock(return_value=True),
        cron_manager=None,
        get_config=lambda _: {},
        platform_manager=NS(platform_insts=[]),
    )
    plugin = main.GroupChroniclePlugin(
        context, {"history_enabled": True, "render_mode": "off"}
    )
    monkeypatch.setattr(
        lm_bridge, "history_connection", lambda ctx: getattr(ctx, "source", None)
    )
    return plugin


async def source(plugin, rows):
    db = await aiosqlite.connect(":memory:")
    await db.execute(
        "CREATE TABLE messages(id INTEGER PRIMARY KEY,session_id TEXT,role TEXT,content TEXT,sender_id TEXT,sender_name TEXT,timestamp REAL,metadata TEXT)"
    )
    for row in rows:
        await db.execute(
            "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)",
            (
                row["id"],
                row.get("umo", U),
                row["role"],
                row["content"],
                row["uid"],
                row["name"],
                row["ts"],
                json.dumps(row["metadata"]),
            ),
        )
    await db.commit()
    plugin.context.source = db
    return db


def test_first_join_has_one_exact_period(store):
    store.membership(U, "1", "join", 100, "join1", name="甲", self_id="0")
    member = store.member(U, "1")
    assert len(member["periods"]) == 1
    assert member["periods"][0]["joined"] == 100


def test_leave_and_rejoin_preserve_two_periods_and_aliases(store):
    store.membership(U, "1", "join", 100, "join1", name="旧名")
    store.membership(U, "1", "leave", 200, "leave1", reason="leave")
    store.membership(U, "1", "join", 300, "join2", name="新名")
    member = store.member(U, "1")
    assert member["aliases"] == ["旧名", "新名"]
    assert [(p["joined"], p["departed"]) for p in member["periods"]] == [
        (100, 200),
        (300, None),
    ]


def test_duplicate_notices_are_idempotent(store):
    assert store.membership(U, "1", "join", 100, "notice")
    assert not store.membership(U, "1", "join", 100, "notice")
    assert len(store.member(U, "1")["periods"]) == 1


def test_silent_roster_member_gets_unknown_join_and_no_inferred_leave(store):
    store.roster(U, [("1", "甲"), ("2", "乙")], now=100)
    store.roster(U, [("1", "甲")], now=200)
    member = store.member(U, "2")
    assert member["periods"][0]["joined"] is None
    assert member["periods"][0]["departed"] is None
    assert store.grade(U, "2")["grade"] == "小记"


def test_old_observation_does_not_reopen_departed_period(store):
    store.membership(U, "1", "join", 100, "j")
    store.membership(U, "1", "leave", 200, "l")
    store.observe(U, "1", "旧名", 150)
    assert len(store.member(U, "1")["periods"]) == 1
    assert store.member(U, "1")["periods"][0]["closed"] == 1


def test_backfill_moves_unknown_start_without_inventing_join(store):
    store.roster(U, [("1", "甲")], now=500)
    store.observe(U, "1", "旧名", 100)
    member = store.member(U, "1")
    assert member["first_seen"] == 100
    assert member["name"] == "甲"
    assert member["periods"][0]["joined"] is None
    assert member["periods"][0]["observed"] == 100


def test_bot_leave_never_closes_human_periods(store):
    store.roster(U, [("1", "甲")], now=100)
    store.membership(U, "0", "leave", 200, "bot-leave", self_id="0")
    assert store.group(U)["unavailable"] == 1
    assert store.member(U, "1")["periods"][0]["departed"] is None
    assert store.member(U, "0") is None


def test_same_name_and_cross_group_do_not_merge(store):
    for umo, uid in [(U, "1"), (U, "2"), (V, "1")]:
        store.observe(umo, uid, "同名", 100)
    store.add_event(U, fact("first", importance=3, role=3), status="approved")
    assert store.grade(U, "1")["grade"] == "小传"
    assert store.grade(U, "2")["points"] == 0
    assert store.grade(V, "1")["points"] == 0


def test_pending_and_rejected_facts_do_not_advance_grade(store):
    key = store.add_event(U, fact("pending", importance=3, role=3))
    assert store.grade(U, "1")["points"] == 0
    store.review(U, key, "approve", "admin")
    assert store.grade(U, "1")["grade"] == "小传"
    store.review(U, key, "reject", "admin")
    assert store.grade(U, "1")["points"] == 0


@pytest.mark.parametrize(
    "kind,positive", [("dispute", True), ("discipline", True), ("help", False)]
)
def test_negative_events_never_award_contribution(store, kind, positive):
    store.add_event(
        U,
        fact(kind, importance=3, role=3, kind=kind, positive=positive),
        status="approved",
    )
    assert store.grade(U, "1")["points"] == 0


def test_grade_thresholds_and_silence(store):
    base = time.time() - 200 * 86400
    for i in range(8):
        store.add_event(
            U,
            fact(i, role=3, importance=3 if i < 2 else 2, ts=base + i * 20 * 86400),
            status="approved",
        )
    grade = store.grade(U, "1")
    assert grade["grade"] == "列传"
    assert grade["events"] == 8 and grade["points"] == 54 and grade["span_days"] == 140
    assert store.grade(U, "1", {"full_span_days": 500})["grade"] == "小传"


def test_three_ordinary_contributions_can_make_short_biography(store):
    for i in range(3):
        store.add_event(U, fact(i, role=2, importance=2), status="approved")
    assert store.grade(U, "1")["grade"] == "小传"


def test_duplicate_evidence_and_followups_count_one_event(store):
    a = store.add_event(U, fact("a", role=1), status="approved")
    assert store.add_event(U, fact("a", role=1), status="approved") == a
    b = store.add_event(U, fact("b", role=3, importance=3), status="approved")
    store.review(U, b, "merge", "admin", target=a)
    grade = store.grade(U, "1")
    assert grade["events"] == 1 and grade["points"] == 9
    assert len(store.event(U, a)["followups"]) == 1


def test_cross_group_review_and_merge_are_rejected(store):
    a = store.add_event(U, fact("a"), status="approved")
    b = store.add_event(V, fact("b"), status="approved")
    with pytest.raises(ValueError):
        store.review(V, a, "approve", "admin")
    with pytest.raises(ValueError):
        store.review(U, b, "merge", "admin", target=a)
    assert len(store.events(U)) == len(store.events(V)) == 1


def test_unknown_identity_must_be_fixed_before_approval(store):
    candidate = fact("unknown")
    candidate["unknown_participants"] = ["unproven"]
    key = store.add_event(U, candidate)
    with pytest.raises(ValueError):
        store.review(U, key, "approve", "admin")
    assert store.event(U, key, public=False)["status"] == "pending"


def test_correction_recomputes_grade_and_invalidates_edition(store):
    key = store.add_event(U, fact("great", role=3, importance=3), status="approved")
    store.save_report(U, "edition", {"chronicle_text": "旧评价", "created_at": 1})
    replacement = fact("great", role=1, importance=1)
    store.review(U, key, "revise", "admin", payload=replacement, note="证据表明仅参与")
    assert store.grade(U, "1")["grade"] == "小记"
    assert store.cache(U, "edition") is None
    with store.connection() as db:
        assert db.execute("SELECT COUNT(*) FROM revisions").fetchone()[0] == 1


def test_profile_and_event_hiding_block_public_output_and_reimport(store):
    store.observe(U, "1", "甲", 100)
    key = store.add_event(U, fact("secret"), status="approved")
    store.save_report(U, "article", {"chronicle_text": "旧正文", "created_at": 1})
    store.hide(U, "1")
    store.observe(U, "1", "甲改名", 200)
    assert store.member(U, "1")["hidden"] == 1
    assert store.events(U) == []
    assert store.event(U, key, public=False)
    assert store.cache(U, "article") is None


def test_can_only_hide_involved_events(store):
    key = store.add_event(U, fact("own"), status="approved")
    with pytest.raises(ValueError):
        store.hide(U, "2", key)
    store.hide(U, "1", key)
    assert store.privacy_active(U) and store.events(U) == []


def test_legacy_import_is_idempotent_and_preserves_source_bytes(tmp_path, store):
    legacy = ChronicleStore(str(tmp_path / "legacy"))
    legacy.save(
        {
            "umo": U,
            "iso_year": 2026,
            "iso_week": 40,
            "chronicle_text": "旧版文学记录",
            "created_at": 100,
        }
    )
    before = open(legacy._file, "rb").read()
    entries = legacy.list(U)
    assert store.import_legacy(entries) == 1
    assert store.import_legacy(entries) == 0
    assert open(legacy._file, "rb").read() == before
    facade = EditionStore(store, legacy)
    assert len(facade.list(U)) == 1
    assert store.grade(U, "1")["points"] == 0
    store.observe(U, "1", "甲", 100)
    store.hide(U, "1")
    assert facade.list(U) == []


def test_event_hide_cannot_resurface_legacy_prose(tmp_path, store):
    legacy = ChronicleStore(str(tmp_path / "legacy"))
    legacy.save(
        {"umo": U, "iso_year": 2026, "iso_week": 40, "chronicle_text": "涉及个人的旧卷"}
    )
    key = store.add_event(U, fact("a"), status="approved")
    store.hide(U, "1", key)
    store.import_legacy(legacy.list(U))
    assert EditionStore(store, legacy).list(U) == []


def test_new_editions_keep_revisions_and_latest_forced_cache(store):
    store.save_report(U, "base", {"chronicle_text": "第一版", "created_at": 100})
    store.save_report(
        U, "base:forced:2", {"chronicle_text": "第二版", "created_at": 200}
    )
    assert store.cache(U, "base")["chronicle_text"] == "第二版"
    assert len(store.reports(U)) == 2
    assert store.mark_delivered(U, "第二版")
    assert store.cache(U, "base")["delivery_complete"] is True


def test_atomic_checkpoint_conflict_and_failure_leave_no_partial_facts(store):
    store.init_job(U, 10)
    assert not store.commit_batch(U, "back", (5, 0), 10, 0, [], [fact("wrong")])
    broken = fact("bad")
    broken["merge_into"] = 999
    with pytest.raises(ValueError):
        store.commit_batch(U, "back", (0, 0), 10, 0, [], [broken])
    assert store.events(U, status=None) == []
    assert store.job(U)["back_cursor"] == 0


def test_global_quota_is_atomic_across_threads_and_resets_by_day(store):
    with ThreadPoolExecutor(max_workers=8) as pool:
        accepted = list(
            pool.map(lambda _: store.consume_quota("2026-10-05", 20), range(60))
        )
    assert sum(accepted) == 20
    assert store.consume_quota("2026-10-06", 20)


def test_parallel_event_writers_preserve_all_facts(store):
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(
            pool.map(
                lambda key: store.add_event(U, fact(key), status="approved"), range(30)
            )
        )
    assert len(store.events(U)) == 30


@pytest.mark.parametrize(
    "config,allowed",
    [
        ({}, True),
        ({"history_allow_groups": [U]}, True),
        ({"history_allow_groups": [V]}, False),
        ({"history_allow_groups": [U], "history_block_groups": [U]}, False),
        ({"history_block_groups": [V]}, True),
    ],
)
def test_scope_precedence(plugin, config, allowed):
    plugin.config.update(config)
    assert plugin.history.enabled_for(U) is allowed


@pytest.mark.parametrize(
    "bad",
    [
        ["100"],
        "all",
        [None],
        ["x:FriendMessage:1"],
        ["synthetic:GroupMessage:100 ", "bad"],
    ],
)
def test_bad_lists_suspend_every_group(plugin, bad):
    plugin.config["history_allow_groups"] = bad
    assert not plugin.history.enabled_for(U)
    with pytest.raises(ValueError):
        scope_lists(plugin.config)


@pytest.mark.parametrize(
    "state", [{"session_enabled": False}, {U: {"disabled_plugins": ["GroupChronicle"]}}]
)
def test_astrbot_session_scopes_are_respected(plugin, monkeypatch, state):
    async def scoped(**kwargs):
        return state

    monkeypatch.setattr(sp, "get_async", scoped)
    assert not run(plugin.history.allowed(U))


def test_astrbot_global_group_whitelist_is_respected(plugin):
    plugin.context.get_config = lambda _: {
        "platform_settings": {"enable_id_white_list": True, "id_whitelist": ["200"]}
    }
    assert not run(plugin.history.allowed(U))


def test_permission_check_failure_is_closed(plugin, monkeypatch):
    monkeypatch.setattr(
        sp, "get_async", AsyncMock(side_effect=RuntimeError("storage down"))
    )
    assert not run(plugin.history.allowed(U))


def test_large_and_escaped_message_is_resumable_without_dropped_tail():
    content = '长消息\\"\n' * 10000
    row = message(1, content)
    cursor, offset, pieces = 0, 0, []
    while cursor < 1:
        material, cursor, offset, observed = pack_messages([row], cursor, offset)
        assert (
            len(json.dumps(material, ensure_ascii=False, separators=(",", ":")))
            <= 12000
        )
        assert observed
        pieces.extend(m["content"] for m in material)
    assert "".join(pieces) == content


def test_budget_boundary_preserves_next_message_cursor():
    rows = [message(1, "A" * 11000), message(2, "B" * 2000)]
    material, cursor, offset, _ = pack_messages(rows, 0)
    assert cursor == 1 and offset > 0
    remainder, cursor, offset, _ = pack_messages(rows[1:], cursor, offset)
    assert cursor == 2 and offset == 0
    assert (
        "".join(m["content"] for m in material + remainder) == "A" * 11000 + "B" * 2000
    )


def test_hidden_and_bot_messages_do_not_enter_model_material():
    rows = [
        message(1, "hidden text"),
        message(2, "回复甲旧名", uid="2"),
        message(3, "bot", role="assistant"),
        message(4, "普通话", uid="2"),
    ]
    material, cursor, offset, _ = pack_messages(
        rows, 0, hidden=[{"uid": "1", "aliases": ["甲旧名"]}]
    )
    assert [m["id"] for m in material] == [4]
    assert cursor == 4 and offset == 0


@pytest.mark.parametrize(
    "risk,corroborated,uid,kind,status",
    [
        (False, True, "1", "project", "approved"),
        (True, True, "1", "project", "pending"),
        (False, False, "1", "project", "pending"),
        (False, True, "unknown", "project", "pending"),
        (False, True, "1", "dispute", "pending"),
    ],
)
def test_admission_requires_identity_and_independent_confirmation(
    risk, corroborated, uid, kind, status
):
    material = [message(1), message(2, uid="2")]
    result = validate_extraction(
        extracted(risk=risk, corroborated=corroborated, uid=uid, kind=kind), material
    )
    assert result[0]["admission"] == status


def test_two_messages_from_same_person_are_not_independent_confirmation():
    result = validate_extraction(extracted(), [message(1), message(2)])
    assert result[0]["admission"] == "pending"


@pytest.mark.parametrize(
    "raw",
    ['{"events":42}', "[]", "not json", extracted(ids=(999,)), extracted(role=99)],
)
def test_malformed_extraction_does_not_silently_consume_source(raw):
    with pytest.raises((ValueError, json.JSONDecodeError)):
        validate_extraction(raw, [message(1), message(2, uid="2")])


def test_no_event_output_is_valid_and_does_not_force_filler():
    assert validate_extraction('{"events":[]}', [message(1)]) == []


def test_fact_writing_budget_keeps_whole_json_records():
    events = [{**fact(i), "id": i, "detail": "x" * 3000} for i in range(100)]
    selected = bounded_facts(events, 1000)
    assert (
        selected
        and len(json.dumps(selected, ensure_ascii=False, separators=(",", ":"))) <= 1000
    )
    assert json.loads(json.dumps(selected)) == selected


def test_lm_keyset_is_select_only_handles_equal_timestamps_and_group_scope(plugin):
    async def case():
        rows = [message(1), message(3), {**message(2), "umo": V}]
        rows[0]["ts"] = rows[1]["ts"] = 100
        db = await source(plugin, rows)
        before = db.total_changes
        assert await lm_bridge.history_sessions(plugin.context) == [U, V]
        assert (await lm_bridge.history_bounds(plugin.context, U))["high"] == 3
        assert [
            m["id"] for m in await lm_bridge.history_page(plugin.context, U, 1, 3, 20)
        ] == [3]
        assert db.total_changes == before
        await db.close()

    run(case())


def test_successful_batch_freezes_snapshot_and_commits_idempotently(
    plugin, monkeypatch
):
    monkeypatch.setattr(lifetime, "call_llm", AsyncMock(return_value=extracted()))

    async def case():
        db = await source(plugin, [message(1), message(2, uid="2")])
        assert await plugin.history.process_one(U)
        job = plugin.history.store.job(U)
        assert job["frozen_high"] == job["back_cursor"] == 2
        assert len(plugin.history.store.events(U)) == 1
        assert not await plugin.history.process_one(U)
        assert lifetime.call_llm.await_count == 1
        await db.close()

    run(case())


@pytest.mark.parametrize(
    "error,expected_quota",
    [(RuntimeError("model down"), 0), ('{"events":42}', 1)],
)
def test_failed_model_or_parse_retains_cursor_and_charges_quota(
    plugin, monkeypatch, error, expected_quota
):
    mock = (
        AsyncMock(side_effect=error)
        if isinstance(error, Exception)
        else AsyncMock(return_value=error)
    )
    monkeypatch.setattr(lifetime, "call_llm", mock)

    async def case():
        db = await source(plugin, [message(1)])
        with pytest.raises((RuntimeError, ValueError)):
            await plugin.history.process_one(U)
        assert plugin.history.store.job(U)["back_cursor"] == 0
        day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
        assert plugin.history.store.quota(day) == expected_quota
        await db.close()

    run(case())


def test_long_message_resume_survives_service_recreation(plugin, monkeypatch):
    mock = AsyncMock(return_value='{"events":[]}')
    monkeypatch.setattr(lifetime, "call_llm", mock)

    async def case():
        db = await source(plugin, [message(1, "长" * 30000)])
        assert await plugin.history.process_one(U)
        assert plugin.history.store.job(U)["back_offset"] > 0
        plugin.history = lifetime.HistoryService(plugin, plugin.history.legacy)
        while plugin.history.store.job(U)["back_cursor"] < 1:
            assert await plugin.history.process_one(U)
        assert mock.await_count == 3
        await db.close()

    run(case())


def test_new_material_precedes_backfill_under_shared_daily_limit(plugin, monkeypatch):
    plugin.config["history_daily_limit"] = 1
    mock = AsyncMock(return_value='{"events":[]}')
    monkeypatch.setattr(lifetime, "call_llm", mock)

    async def case():
        db = await source(plugin, [message(1, "old"), message(2, "new", uid="2")])
        plugin.history.store.init_job(U, 1)
        await plugin.history.run_cycle()
        job = plugin.history.store.job(U)
        assert job["live_cursor"] == 2 and job["back_cursor"] == 0
        assert (
            "new" in mock.call_args.args[2]
            and '"content":"old"' not in mock.call_args.args[2]
        )
        await db.close()

    run(case())


def test_scope_change_cancels_model_and_keeps_checkpoint(plugin, monkeypatch):
    started = asyncio.Event()

    async def blocked(*args, **kwargs):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(lifetime, "call_llm", blocked)

    async def case():
        db = await source(plugin, [message(1)])
        task = asyncio.create_task(plugin.history.process_one(U))
        await asyncio.wait_for(started.wait(), 1)
        plugin.config["history_block_groups"] = [U]
        await plugin.history.refresh_scope()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert plugin.history.store.job(U)["back_cursor"] == 0
        await db.close()

    run(case())


def test_privacy_change_during_extraction_discards_the_candidate(plugin, monkeypatch):
    async def hidden(*args, **kwargs):
        plugin.history.store.hide(U, "1")
        return extracted()

    monkeypatch.setattr(lifetime, "call_llm", hidden)

    async def case():
        db = await source(plugin, [message(1), message(2, uid="2")])
        assert not await plugin.history.process_one(U)
        assert plugin.history.store.job(U)["back_cursor"] == 0
        assert plugin.history.store.events(U) == []
        await db.close()

    run(case())


def test_source_deletion_is_reported_as_a_gap(plugin):
    async def case():
        db = await source(plugin, [])
        plugin.history.store.init_job(U, 10)
        assert await plugin.history.process_one(U)
        assert json.loads(plugin.history.store.job(U)["gaps"])
        await db.close()

    run(case())


def test_passive_filter_never_wakes_and_deduplicates_notice(plugin):
    async def case():
        set_active(plugin)
        event = Event(
            "",
            uid="2",
            raw={
                "post_type": "notice",
                "notice_type": "group_increase",
                "user_id": 2,
                "group_id": 100,
                "time": int(time.time()),
                "operator_id": 3,
                "sub_type": "approve",
            },
        )
        capture = HistoryCaptureFilter()
        assert capture.filter(event, {}) is False
        assert capture.filter(event, {}) is False
        await asyncio.gather(*list(plugin._bg_tasks))
        assert len(plugin.history.store.member(U, "2")["periods"]) == 1
        assert plugin.context.send_message.await_count == 0

    run(case())


def test_hourly_job_registered_without_weekly_push_and_unloaded(plugin, monkeypatch):
    added, removed = [], []
    scheduler = NS(
        add_job=lambda fn, **kwargs: added.append(kwargs), remove_job=removed.append
    )
    plugin.context.cron_manager = NS(scheduler=scheduler)
    monkeypatch.setattr(plugin.history, "discover", AsyncMock(return_value=[]))

    async def case():
        await plugin.initialize()
        assert any(
            j["id"] == "group_chronicle_history_hourly" and j["max_instances"] == 1
            for j in added
        )
        assert not any(j["id"] == "group_chronicle_weekly" for j in added)
        await plugin.terminate()
        assert "group_chronicle_history_hourly" in removed
        assert not plugin._bg_tasks

    run(case())


def test_silent_small_note_requires_no_model_and_is_cached(plugin, monkeypatch):
    mock = AsyncMock(side_effect=AssertionError("should not call model"))
    monkeypatch.setattr(lifetime, "call_llm", mock)
    plugin.history.store.roster(U, [("1", "甲")])
    first = run(plugin.history.biography(U, "1"))
    second = run(plugin.history.biography(U, "1"))
    assert first == second and len(first) <= 300 and "入群日期未知" in first
    assert mock.await_count == 0


def test_biography_full_lifetime_selection_and_length_limit(plugin, monkeypatch):
    plugin.history.store.observe(U, "1", "甲", time.time() - 500 * 86400)
    plugin.history.store.add_event(
        U,
        fact("ancient", role=3, importance=3, ts=time.time() - 400 * 86400),
        status="approved",
    )
    mock = AsyncMock(return_value="有依据的生涯" * 1000)
    monkeypatch.setattr(lifetime, "call_llm", mock)
    text = run(plugin.history.biography(U, "1"))
    assert "小传" in text and len(text) <= 1000
    assert "ancient" in mock.call_args.args[2]
    assert run(plugin.history.biography(U, "1")) == text
    assert mock.await_count == 1


def test_report_uses_only_approved_facts_and_persists_month_year(plugin, monkeypatch):
    plugin.history.store.observe(U, "1", "甲")
    plugin.history.store.add_event(U, fact("admitted"), status="approved")
    plugin.history.store.add_event(U, fact("pending"))
    mock = AsyncMock(return_value="正式群史正文")
    monkeypatch.setattr(lifetime, "call_llm", mock)
    text, image = run(plugin._build_entry(U, 30, False, "测试", save=False))
    assert image is None and "正式群史正文" in text
    assert (
        "admitted" in mock.call_args.args[2] and "pending" not in mock.call_args.args[2]
    )
    assert plugin.store.list(U)[0]["period_days"] == 30
    plugin.store.mark_delivered(U, text)
    assert (
        run(plugin._build_entry(U, 30, False, "测试", skip_check=False, save=False))[0]
        == text
    )
    assert mock.await_count == 1
    run(plugin._build_entry(U, 365, False, "测试", save=False))
    assert len(plugin.store.list(U)) == 2


def test_no_events_never_calls_model_or_creates_filler(plugin, monkeypatch):
    mock = AsyncMock(side_effect=AssertionError("no filler"))
    monkeypatch.setattr(lifetime, "call_llm", mock)
    text, _, _ = run(plugin.history.build_report(U, None, True, "测试"))
    assert "尚无" in text and not plugin.store.list(U)
    assert mock.await_count == 0


def test_correction_during_composition_never_publishes_old_facts(plugin, monkeypatch):
    key = plugin.history.store.add_event(U, fact("changed"), status="approved")

    async def changing(*args, **kwargs):
        plugin.history.store.review(U, key, "reject", "admin")
        return "旧正文"

    monkeypatch.setattr(lifetime, "call_llm", changing)
    with pytest.raises(ValueError):
        run(plugin.history.build_report(U, 7, False, "测试"))
    assert plugin.store.list(U) == []


def test_user_clue_pending_admin_confirmation_and_grade_revision(plugin):
    reply = run(handle_command(plugin.history, Event("记一笔 活动｜大家举办了活动")))
    assert "等待" in reply
    key = plugin.history.store.events(U, status="pending")[0]["id"]
    assert "仅本群管理员" in run(
        handle_command(plugin.history, Event(f"史记确认 {key}"))
    )
    assert "确认入史" in run(
        handle_command(plugin.history, Event(f"史记确认 {key}", admin=True))
    )
    revised = run(
        handle_command(
            plugin.history,
            Event(
                f"史记修订 {key} 组织活动取得成果｜重要度=3｜核心=1｜说明=核实了组织贡献",
                admin=True,
            ),
        )
    )
    assert "已修订" in revised
    assert plugin.history.store.grade(U, "1")["grade"] == "小传"


def test_manual_unknown_mention_requires_identity_review(plugin):
    reply = run(
        handle_command(
            plugin.history,
            Event("记一笔 线索｜有成员作出了贡献", admin=True, mentions=("999",)),
        )
    )
    assert "等待" in reply
    assert plugin.history.store.member(U, "999") is None
    assert plugin.history.store.events(U, status="pending")[0][
        "unknown_participants"
    ] == ["999"]


def test_manual_admin_clue_is_admitted_without_model(plugin):
    reply = run(
        handle_command(
            plugin.history, Event("记一笔 共同成果｜已完成共同成果", admin=True)
        )
    )
    assert "已确认" in reply and len(plugin.history.store.events(U)) == 1


def test_review_details_private_only_and_no_public_fallback(plugin):
    plugin.history.store.add_event(U, {**fact("private"), "detail": "敏感审核细节"})
    reply = run(handle_command(plugin.history, Event("史记待审", admin=True)))
    assert "敏感审核细节" not in reply
    assert plugin.context.send_message.call_args.args[0] == "synthetic:FriendMessage:1"
    plugin.context.send_message.return_value = False
    reply = run(handle_command(plugin.history, Event("史记待审", admin=True)))
    assert "私发失败" in reply and "敏感审核细节" not in reply


def test_other_group_event_cannot_be_approved_or_corrected(plugin):
    key = plugin.history.store.add_event(V, fact("other"))
    assert "本群没有" in run(
        handle_command(plugin.history, Event(f"史记确认 {key}", admin=True))
    )
    assert "本群没有" in run(
        handle_command(plugin.history, Event(f"史记更正 {key} 建议"))
    )


def test_personal_hide_invalidates_all_public_editions_and_cache(plugin, monkeypatch):
    plugin.history.store.observe(U, "1", "甲")
    plugin.history.store.add_event(U, fact("own"), status="approved")
    monkeypatch.setattr(lifetime, "call_llm", AsyncMock(return_value="可公开的旧正文"))
    text, _, _ = run(plugin.history.build_report(U, 7, False, "测试"))
    assert run(plugin.history.publishable(U, text))
    preview = run(handle_command(plugin.history, Event("史记隐藏")))
    assert "作用域预览" in preview
    assert "已隐藏" in run(handle_command(plugin.history, Event("史记隐藏 确认")))
    assert not run(plugin.history.publishable(U, text))
    assert plugin.store.list(U) == []
    assert "已隐藏" in run(plugin.history.biography(U, "1"))


def test_user_correction_is_pending_until_admin_revision(plugin):
    key = plugin.history.store.add_event(U, fact("correct"), status="approved")
    reply = run(handle_command(plugin.history, Event(f"史记更正 {key} 实际是协助角色")))
    assert "正式事实尚未修改" in reply
    assert plugin.history.store.corrections(U)[0]["note"] == "实际是协助角色"


def test_revisions_require_reason_and_known_member_ids(plugin):
    original = fact("old")
    with pytest.raises(ValueError):
        revise_payload(plugin.history.store, U, original, "新的经过")
    with pytest.raises(ValueError):
        revise_payload(plugin.history.store, U, original, "经过｜核心=999｜说明=核实")


def test_group_owner_role_is_scoped_and_cannot_be_spoofed_by_other_group(plugin):
    raw = {"group_id": 100, "sender": {"user_id": 1, "role": "owner"}}
    assert administrator(plugin, Event(raw=raw))
    assert not administrator(plugin, Event(umo=V, raw=raw))
    assert not administrator(plugin, Event(uid="2", raw=raw))


def test_private_event_cannot_run_group_history_commands(plugin):
    reply = run(
        handle_command(
            plugin.history,
            Event("记一笔 私聊｜不能跨群写入", umo="synthetic:FriendMessage:1"),
        )
    )
    assert "只能在群内" in reply


def test_segment_delivery_stops_exposing_content_after_guard_changes(
    plugin, monkeypatch
):
    allowed = True

    async def send(*args, **kwargs):
        nonlocal allowed
        allowed = False
        return True

    monkeypatch.setattr(plugin.context, "send_message", AsyncMock(side_effect=send))
    monkeypatch.setattr("core.deliver._SEGMENT_SLEEP_SECONDS", 0)
    assert not run(
        deliver_text(
            plugin.context,
            None,
            U,
            "0123456789",
            prefer_context=True,
            use_forward_nodes=False,
            max_chars=3,
            guard=lambda: allowed,
        )
    )
    assert plugin.context.send_message.await_count == 1


def test_recent_followup_of_old_project_is_in_current_period(store):
    a = store.add_event(U, fact("old", ts=time.time() - 200 * 86400), status="approved")
    b = store.add_event(U, fact("finished"), status="approved")
    store.review(U, b, "merge", "admin", target=a)
    assert [e["id"] for e in store.events(U, since=time.time() - 7 * 86400)] == [a]


def test_negative_followup_retains_prior_credit_without_adding_negative_roles(store):
    a = store.add_event(U, fact("positive", role=1, importance=1), status="approved")
    b = store.add_event(
        U, fact("negative", role=3, importance=3, positive=False), status="approved"
    )
    store.review(U, b, "merge", "admin", target=a)
    assert store.grade(U, "1")["points"] == 1
    assert store.grade(U, "1")["major_core"] == 0
    payload, note = revise_payload(
        store, U, store.event(U, a), "标题=修正后的标题｜说明=仅修正标题"
    )
    store.review(U, a, "revise", "admin", payload=payload, note=note)
    assert store.grade(U, "1")["points"] == 1


def test_new_followup_participant_does_not_inherit_ancient_participation_date(store):
    now = time.time()
    a = store.add_event(
        U, fact("old", uid="1", ts=now - 200 * 86400), status="approved"
    )
    b = store.add_event(U, fact("joined", uid="2", ts=now), status="approved")
    store.review(U, b, "merge", "admin", target=a)
    store.add_event(U, fact("other", uid="2", ts=now), status="approved")
    assert store.grade(U, "2")["span_days"] == 0


def test_atomic_edition_commit_rejects_a_stale_fact_revision(store):
    revision = store.revision(U)
    store.add_event(U, fact("new"), status="approved")
    with pytest.raises(ValueError):
        store.save_report(
            U, "stale", {"chronicle_text": "过时正文"}, expected_revision=revision
        )
    assert store.reports(U) == []


def test_missing_notice_name_does_not_replace_a_known_nickname(store):
    store.membership(U, "1", "join", 100, "j1", name="甲")
    store.membership(U, "1", "leave", 200, "l1")
    store.membership(U, "1", "join", 300, "j2")
    assert store.member(U, "1")["name"] == "甲"
    assert "1" not in store.member(U, "1")["aliases"]


def test_bot_outage_gap_survives_rejoin_and_later_job_creation(store):
    store.membership(U, "0", "leave", 100, "out", self_id="0")
    store.membership(U, "0", "join", 200, "back", self_id="0")
    assert store.group(U)["unavailable"] == 0
    assert json.loads(store.init_job(U, 0)["gaps"])


def test_individual_event_hide_blocks_rephrased_reimports_and_scores(store):
    original = fact("first")
    original["title"] = "共同项目完成"
    event_id = store.add_event(U, original, status="approved")
    store.hide(U, "1", event_id)
    repeated = fact("another", role=3, importance=3)
    repeated["title"] = "共同项目已完成"
    store.add_event(U, repeated, status="approved")
    assert store.events(U) == []
    assert store.grade(U, "1")["events"] == 1
    assert store.excluded_sources(U) == {"first"}


def test_hidden_event_source_never_reenters_the_extraction_material():
    material, cursor, _, _ = pack_messages(
        [message(1), message(2, uid="2")], 0, excluded_ids={"1"}
    )
    assert [m["id"] for m in material] == [2] and cursor == 2


def test_quiet_verified_roster_member_can_receive_well_supported_credit():
    result = validate_extraction(
        extracted(uid="3", role=3),
        [message(1), message(2, uid="2")],
        known_members={"3": "沉默贡献者"},
    )
    assert result[0]["admission"] == "approved"
    assert result[0]["participants"][0]["uid"] == "3"


def test_roster_discovery_includes_silent_members_and_excludes_bot(plugin):
    async def action(name, **kwargs):
        return {
            "get_login_info": {"user_id": 0},
            "get_group_list": [{"group_id": 100, "group_name": "合成群"}],
            "get_group_member_list": [
                {"user_id": 0, "nickname": "机器人"},
                {"user_id": 1, "nickname": "甲"},
                {"user_id": 2, "nickname": "乙"},
            ],
        }[name]

    platform = NS(
        metadata=NS(id="synthetic", name="aiocqhttp"), bot=NS(call_action=action)
    )
    plugin.context.platform_manager.platform_insts = [platform]
    assert run(plugin.history.discover()) == [U]
    assert plugin.history.store.member(U, "2") is not None
    assert plugin.history.store.member(U, "0") is None
    material, _, _, _ = pack_messages([message(1, uid="0")], 0, bot_ids={"0"})
    assert material == []


def test_privacy_request_is_immediate_during_model_work(plugin, monkeypatch):
    started = asyncio.Event()

    async def waiting(*args, **kwargs):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(lifetime, "call_llm", waiting)

    async def case():
        db = await source(plugin, [message(1)])
        task = asyncio.create_task(plugin.history.process_one(U))
        await asyncio.wait_for(started.wait(), 1)
        preview = await asyncio.wait_for(
            handle_command(plugin.history, Event("史记隐藏")), 1
        )
        assert "作用域预览" in preview
        reply = await asyncio.wait_for(
            handle_command(plugin.history, Event("史记隐藏 确认")), 1
        )
        assert "已隐藏" in reply
        with pytest.raises(asyncio.CancelledError):
            await task
        assert plugin.history.store.job(U)["back_cursor"] == 0
        await db.close()

    run(case())


@pytest.mark.parametrize(
    "off",
    [
        {"history_enabled": False},
        {"history_block_groups": [U]},
        {"history_allow_groups": ["bad"]},
    ],
)
def test_personal_privacy_requests_work_when_recording_is_disabled(plugin, off):
    plugin.config.update(off)
    preview = run(handle_command(plugin.history, Event("史记隐藏")))
    assert "作用域预览" in preview
    reply = run(handle_command(plugin.history, Event("史记隐藏 确认")))
    assert "已隐藏" in reply
    assert plugin.history.store.member(U, "1")["hidden"] == 1


def test_manual_history_date_and_unknown_date_are_distinguished(plugin):
    run(
        handle_command(
            plugin.history,
            Event("记一笔 旧日成果｜共同完成成果｜日期=2020-01-02", admin=True),
        )
    )
    event = plugin.history.store.events(U)[0]
    assert event["date_basis"] == "declared"
    declared = datetime(2020, 1, 2, tzinfo=timezone(timedelta(hours=8)))
    assert event["event_at"] == declared.timestamp()
    timeline = run(handle_command(plugin.history, Event("群大事 全部")))
    assert "发生于（人工记载）2020-01-02" in timeline
    run(
        handle_command(plugin.history, Event("记一笔 新线索｜曾有重要经历", admin=True))
    )
    assert plugin.history.store.events(U)[-1]["date_basis"] == "recorded"


def test_invalid_source_timestamp_is_skipped_with_a_visible_gap(plugin, monkeypatch):
    mock = AsyncMock(return_value='{"events":[]}')
    monkeypatch.setattr(lifetime, "call_llm", mock)

    async def case():
        db = await source(plugin, [message(1, ts="bad-time"), message(2, uid="2")])
        assert await plugin.history.process_one(U)
        assert plugin.history.store.job(U)["back_cursor"] == 2
        assert json.loads(plugin.history.store.job(U)["gaps"])
        await db.close()

    run(case())


def test_no_new_fact_continuation_never_regenerates_or_adds_biographies(
    plugin, monkeypatch
):
    plugin.history.store.add_event(U, fact("old"), status="approved")
    mock = AsyncMock(return_value="已有正文")
    monkeypatch.setattr(lifetime, "call_llm", mock)
    text, _ = run(plugin._build_entry(U, 30, False, "测试"))
    plugin.store.mark_delivered(U, text)
    run(plugin._continue_analyze(Event(), U))
    assert mock.await_count == 1
    assert "暂无新大事" in str(plugin.context.send_message.call_args)


def test_prefetched_old_export_is_blocked_after_hide(plugin, monkeypatch):
    plugin.history.store.add_event(U, fact("old"), status="approved")
    monkeypatch.setattr(lifetime, "call_llm", AsyncMock(return_value="旧正文"))
    run(plugin._build_entry(U, 7, False, "测试"))
    old = plugin.store.get_by_id(U, 1)
    plugin.history.store.hide(U, "1")
    monkeypatch.setattr(plugin.store, "get_by_id", lambda *args: old)
    event = Event()

    async def case():
        reply = [r async for r in plugin.chronicle_export_command(event)]
        assert "隐藏或修订" in "".join(reply)
        event.send.assert_not_called()

    run(case())


def test_oversized_persona_preserves_style_and_fact_constraints(plugin, monkeypatch):
    plugin.history.store.add_event(U, fact("style"), status="approved")
    plugin.config.update(prompt_chronicle="人设" * 10000, chronicle_style="classical")
    mock = AsyncMock(return_value="正文")
    monkeypatch.setattr(lifetime, "call_llm", mock)
    run(plugin._build_entry(U, 7, False, "测试"))
    system = mock.call_args.args[3]
    assert len(system) < 8000 and "文言" in system and "不能写成确切发生日期" in system


def test_background_generation_failure_is_reported_to_the_caller(plugin, monkeypatch):
    plugin.history.store.observe(U, "1", "甲")
    plugin.history.store.add_event(U, fact("bio"), status="approved")
    monkeypatch.setattr(
        lifetime, "call_llm", AsyncMock(side_effect=RuntimeError("failed"))
    )
    run(plugin._bio_only(Event(), U, "1"))
    assert "史记处理失败" in str(plugin.context.send_message.call_args)


def test_scope_change_cancels_article_model_work_too(plugin, monkeypatch):
    plugin.history.store.add_event(U, fact("article"), status="approved")
    started = asyncio.Event()

    async def waiting(*args, **kwargs):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(lifetime, "call_llm", waiting)

    async def case():
        task = asyncio.create_task(plugin.history.build_report(U, 7, False, "测试"))
        await asyncio.wait_for(started.wait(), 1)
        plugin.config["history_block_groups"] = [U]
        await plugin.history.refresh_scope()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not plugin.history.operations
        assert plugin.history.store.reports(U) == []

    run(case())


def test_archive_store_wal_fallback_privacy_and_report_retention(tmp_path, monkeypatch):
    """ArchiveStore falls back to DELETE cleanly, matches event() vs events() privacy, and prunes forced reports."""
    import core.archive_store as archive_store_mod

    archive_store_mod._JOURNAL_MODES.clear()
    monkeypatch.setattr(archive_store_mod, "_probe_wal_support", lambda _d: False)
    db_path = tmp_path / "wal_fallback.db"
    store = ArchiveStore(db_path)
    try:
        with store.connection() as db:
            mode = db.execute("PRAGMA journal_mode").fetchone()[0].lower()
        assert mode == "delete"

        store.observe(U, "1", "甲")
        store.observe(U, "2", "乙")
        id1 = store.add_event(U, fact("vis1", uid="1"), status="approved")
        id2 = store.add_event(U, fact("vis2", uid="2"), status="approved")
        store.hide(U, "2", None)

        events = store.events(U)
        assert [e["id"] for e in events] == [id1]
        assert store.event(U, id1)["id"] == id1
        assert store.event(U, id2) is None

        # Forced report retention keeps bounded latest reports while bumping version monotonically.
        last_ver = 0
        for i in range(15):
            rid = store.save_report(
                U,
                f"chronicle:base:forced:{i:04d}",
                {
                    "chronicle_text": f"text-{i}",
                    "created_at": 1000.0 + i,
                    "report_kind": "chronicle",
                    "delivery_complete": False,
                },
            )
            saved = next(r for r in store.reports(U) if r["report_id"] == rid)
            assert saved["version"] > last_ver
            last_ver = saved["version"]
        with store.connection() as db:
            cnt = db.execute(
                "SELECT COUNT(*) FROM reports WHERE umo=? AND cache_key LIKE '%:forced:%'",
                (U,),
            ).fetchone()[0]
        assert cnt == archive_store_mod._MAX_FORCED_REVISION_PER_BASE
    finally:
        store.close()
        archive_store_mod._JOURNAL_MODES.clear()


def test_run_cycle_hourly_budget_caps_per_cycle(plugin, monkeypatch):
    """run_cycle caps work per cycle by ceil(daily_limit / 6) instead of draining daily quota."""
    plugin.config["history_enabled"] = True
    plugin.config["history_daily_limit"] = 20
    plugin.config["history_batch_size"] = 1

    calls = 0

    async def fake_process_one(umo, mode="back"):
        nonlocal calls
        calls += 1
        day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
        return plugin.history.store.consume_quota(day, 20, kind="history")

    plugin.history.store.group(U)
    monkeypatch.setattr(lm_bridge, "history_connection", lambda ctx: object())
    monkeypatch.setattr(plugin.history, "process_one", fake_process_one)
    run(plugin.history.run_cycle())
    # Hourly budget = ceil(20 / 6) = 4, rather than draining all 20 in one cycle.
    assert calls == 4


def test_process_one_precheck_saves_quota(plugin, monkeypatch):
    """Pre-check failure before call_llm in process_one must NOT charge quota."""
    plugin.config["history_enabled"] = True
    plugin.config["history_daily_limit"] = 20
    plugin.config["history_batch_size"] = 3

    plugin.history.store.group(V)
    rows_v = [
        {**message(i, f"群项目第{i}次推进内容比较详细", uid="1"), "umo": V}
        for i in range(1, 4)
    ]
    db_v = run(source(plugin, rows_v))
    llm_mock = AsyncMock(
        side_effect=AssertionError("LLM should not run after privacy/scope change")
    )
    monkeypatch.setattr(lifetime, "call_llm", llm_mock)

    orig_allowed = plugin.history.allowed
    allowed_calls = 0

    async def allowed_flips(umo, recording=True):
        nonlocal allowed_calls
        allowed_calls += 1
        if umo == V and allowed_calls >= 2:
            return False
        return await orig_allowed(umo, recording=recording)

    monkeypatch.setattr(plugin.history, "allowed", allowed_flips)
    day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
    before_q = plugin.history.store.quota(day)
    try:
        assert run(plugin.history.process_one(V, "live")) is False
        assert plugin.history.store.quota(day) == before_q
        llm_mock.assert_not_called()
    finally:
        run(db_v.close())


def test_history_capture_skips_chat_when_dm_capturing_keeps_notice(plugin, monkeypatch):
    """Regular group chat skips duplicate capture when DM is active, while OneBot notices still capture."""
    plugin.config["history_enabled"] = True
    set_active(plugin)
    filt = HistoryCaptureFilter()
    captured = []
    spawned = []

    def fake_capture(obs, cfg=None):
        captured.append(obs)
        return obs

    def fake_spawn(coro, label=None):
        spawned.append(coro)
        return None

    monkeypatch.setattr(plugin.history, "capture", fake_capture)
    monkeypatch.setattr(plugin, "_spawn_bg", fake_spawn)
    monkeypatch.setattr(lm_bridge, "is_dm_capturing_group", lambda ctx, ev: True)

    chat_ev = Event("normal group chat")
    notice_ev = Event(
        "",
        raw={
            "post_type": "notice",
            "notice_type": "group_increase",
            "user_id": 777,
            "self_id": 999,
            "time": time.time(),
        },
    )

    filt.filter(chat_ev, {})
    assert "0" in plugin.history.bot_ids.get("synthetic", set())
    filt.filter(notice_ev, {})
    assert len(captured) == 1
    assert captured[0]["kind"] == "join"
    assert captured[0]["uid"] == "777"

    # When DM is not capturing that group, normal chat capture still triggers.
    monkeypatch.setattr(lm_bridge, "is_dm_capturing_group", lambda ctx, ev: False)
    filt.filter(chat_ev, {})
    assert len(captured) == 2
    assert captured[1]["uid"] == "1"
