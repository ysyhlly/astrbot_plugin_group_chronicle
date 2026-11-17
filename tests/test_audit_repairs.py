"""Behavioral regressions for the 2026-10-05 audit findings."""

import asyncio
from datetime import date, datetime, timedelta
import json
import sqlite3
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

import main
from core import biography, deliver, generation, influence, llm_call, structured
from core.evidence import EvidenceBundle, gather_evidence
from core.store import ChronicleStore


def message(sid="u1", name="member", text="synthetic message", timestamp=None):
    return NS(
        sender_id=sid,
        sender_name=name,
        content=text,
        role="user",
        metadata={},
        timestamp=time.time() - 1 if timestamp is None else timestamp,
    )


@pytest.fixture
def plugin(tmp_path):
    context = NS(send_message=AsyncMock(return_value=True), cron_manager=None)
    obj = main.GroupChroniclePlugin(
        context,
        {
            "enable_progress": False,
            "enable_relation_arc": False,
            "enable_gallery": False,
            "render_mode": "off",
        },
    )
    obj.data_dir = str(tmp_path)
    obj.store = ChronicleStore(str(tmp_path / "chronicles"))
    return obj


@pytest.mark.asyncio
async def test_real_contract_message_constructor(plugin):
    assert not hasattr(main.MessageChain(), "plain")
    await plugin._reply(
        None, "synthetic", umo="fake:GroupMessage:g1", prefer_context=True
    )
    assert plugin.context.send_message.call_args.args[1]._chain == [
        ("plain", "synthetic")
    ]


@pytest.mark.asyncio
async def test_failed_delivery_reuses_saved_result(plugin, monkeypatch):
    umo = "fake:GroupMessage:g1"
    evidence = EvidenceBundle(
        chronicle_msgs=[message()], newest_ts=time.time() - 1, msg_count=1
    )
    gather = AsyncMock(return_value=(evidence, 7))
    generate = AsyncMock(return_value="synthetic result")
    monkeypatch.setattr(plugin, "_lm_and_gather", gather)
    monkeypatch.setattr(main, "generate_chronicle", generate)
    monkeypatch.setattr(main, "rank_influential", AsyncMock(return_value=[]))
    plugin.context.send_message.return_value = False
    await plugin._weekly_push_one(umo)
    pending = plugin.store.get_by_id(umo, 1)
    assert pending["delivery_complete"] is False
    assert not plugin.store.should_skip(
        umo, pending["iso_year"], pending["iso_week"], evidence.newest_ts
    )
    plugin.context.send_message.return_value = True
    await plugin._weekly_push_one(umo)
    assert generate.await_count == 1 and gather.await_count == 1
    assert plugin.store.get_by_id(umo, 1)["delivery_complete"] is True
    await plugin._weekly_push_one(umo)
    assert plugin.context.send_message.await_count == 2


@pytest.mark.asyncio
async def test_partial_delivery_is_not_complete(plugin, monkeypatch):
    plugin.context.send_message.side_effect = [True, False]
    monkeypatch.setattr(deliver, "_SEGMENT_SLEEP_SECONDS", 0)
    assert not await deliver.deliver_text(
        plugin.context,
        None,
        "g1",
        "abcdef",
        max_chars=3,
        use_forward_nodes=False,
        prefer_context=True,
    )


@pytest.mark.asyncio
async def test_history_bypasses_short_cache():
    rows = [message() for _ in range(300)]

    async def get_messages(umo, limit, use_cache=True):
        return rows[-50:] if use_cache else rows[-limit:]

    engine = NS(get_session_memories=AsyncMock(return_value=[]))
    bundle = await gather_evidence(
        engine, NS(get_messages=get_messages), "g1", 7, 30, 20000, 800, 101
    )
    assert bundle.msg_count == 300 and bundle.stats == {"u1": 300}


@pytest.mark.asyncio
async def test_old_manager_without_cache_argument_is_supported():
    async def get_messages(umo, limit):
        return [message()]

    bundle = await gather_evidence(
        NS(get_session_memories=AsyncMock(return_value=[])),
        NS(get_messages=get_messages),
        "g1",
        7,
        30,
        800,
        80,
    )
    assert bundle.msg_count == 1


@pytest.mark.asyncio
async def test_same_name_does_not_mix_ids_or_quotas():
    rows = [message("u1", "same", "OWNER") for _ in range(8)] + [
        message("u2", "same", "OTHER") for _ in range(8)
    ]
    evidence = EvidenceBundle(chronicle_msgs=rows, biography_msgs=rows)
    prompt = await biography.build_biography_prompt(
        {"sender_id": "u1", "sender_name": "same"}, evidence, 30
    )
    assert "OWNER" in prompt and "OTHER" not in prompt
    assert generation.compress_evidence(evidence).count("[same|") == 16


def test_memories_and_day_buckets_obey_total_budget():
    evidence = EvidenceBundle(
        chronicle_msgs=[message()], memories=[{"text": "x" * 20000}]
    )
    assert (
        generation._estimate_tokens(
            generation.compress_evidence(evidence, max_tokens=100)
        )
        <= 100
    )
    evidence.chronicle_msgs = [
        message(text="x" * 200, timestamp=time.time() - i * 86400) for i in range(365)
    ]
    assert len(generation._day_bucket_text(evidence)) <= 12000


@pytest.mark.asyncio
async def test_final_llm_budget_includes_system_prompt():
    context = NS(
        get_current_chat_provider_id=AsyncMock(return_value="normal"),
        llm_generate=AsyncMock(return_value=NS(completion_text="ok")),
    )
    await llm_call.call_llm(context, "g1", "x" * 50000, "y" * 50000)
    args = context.llm_generate.call_args.kwargs
    assert len(args["prompt"]) + len(args["system_prompt"]) <= 24000


@pytest.mark.parametrize("field", ["events", "quotes", "new_members", "slang"])
def test_structured_wrong_types_do_not_crash(field):
    parsed = structured.parse_structured_json(json.dumps({field: 42, "overall": "ok"}))
    assert structured.render_structured_markdown(parsed, 7) == "ok"


@pytest.mark.asyncio
async def test_structured_respects_style_persona_and_topic(monkeypatch):
    extract = AsyncMock(return_value={"overall": "synthetic facts"})
    write = AsyncMock(return_value="final styled result")
    monkeypatch.setattr(generation, "extract_structured", extract)
    monkeypatch.setattr(generation, "call_llm", write)
    result = await generation.generate_chronicle(
        NS(),
        "g1",
        EvidenceBundle(),
        "PERSONA",
        7,
        enable_structured=True,
        enable_two_phase=True,
        enable_slang=False,
        style="classical",
        topic="TOPIC",
    )
    assert result == "final styled result"
    assert "TOPIC" in extract.call_args.args[2]
    assert "TOPIC" in write.call_args.args[2]
    assert "PERSONA" in write.call_args.args[3] and "文言" in write.call_args.args[3]
    assert "请围绕其展开" not in result


@pytest.mark.asyncio
@pytest.mark.parametrize("modern", [True, False])
async def test_systemone_cannot_generate(modern, monkeypatch):
    monkeypatch.setattr(llm_call, "_RETRY_ATTEMPTS", 1)
    decision = NS(
        is_systemone_provider=True,
        text_chat=AsyncMock(return_value=NS(completion_text="choice=1")),
    )
    context = NS(get_using_provider=lambda **kwargs: decision)
    if modern:
        context.get_provider_by_id = lambda key: decision
        context.get_current_chat_provider_id = AsyncMock(return_value="decision")
        context.llm_generate = AsyncMock(return_value=NS(completion_text="choice=1"))
    with pytest.raises(llm_call.LLMUnavailableError):
        await llm_call.call_llm(context, "g1", "synthetic")
    assert decision.text_chat.await_count == 0
    if modern:
        assert context.llm_generate.await_count == 0


@pytest.mark.asyncio
async def test_empty_llm_completion_is_not_a_success(monkeypatch):
    monkeypatch.setattr(llm_call, "_RETRY_ATTEMPTS", 1)
    provider = NS(text_chat=AsyncMock(return_value=NS(completion_text="")))
    context = NS(get_using_provider=lambda **kwargs: provider)
    with pytest.raises(llm_call.LLMUnavailableError):
        await llm_call.call_llm(context, "g1", "synthetic")


@pytest.mark.asyncio
async def test_same_name_memories_without_identity_are_ignored():
    evidence = EvidenceBundle(
        biography_msgs=[message("u1", "same", "OWNER"), message("u2", "same", "OTHER")],
        memories=[
            {"text": "same has an ambiguous memory"},
            {"text": "u1 verified memory"},
        ],
    )
    prompt = await biography.build_biography_prompt(
        {"sender_id": "u1", "sender_name": "same"}, evidence, 30
    )
    assert "ambiguous memory" not in prompt and "verified memory" in prompt


@pytest.mark.asyncio
async def test_unload_cancels_running_scheduler_callback(plugin, monkeypatch):
    plugin.config.update(
        enable_weekly_push=True, push_groups=["g1"], enable_cron_reload=False
    )
    entered = asyncio.Event()

    async def hang(umo):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(plugin, "_weekly_push_one", hang)
    task = asyncio.create_task(plugin._weekly_push())
    await entered.wait()
    await plugin.terminate()
    assert task.cancelled() and not plugin._bg_tasks


@pytest.mark.asyncio
async def test_unload_cancels_background_and_new_work(plugin):
    gate = asyncio.Event()
    effects = []

    async def job():
        await gate.wait()
        effects.append(1)

    plugin._spawn_bg(job(), label="synthetic")
    tasks = list(plugin._bg_tasks)
    await asyncio.sleep(0)
    await plugin.terminate()
    assert all(t.cancelled() for t in tasks) and not plugin._bg_tasks
    plugin._spawn_bg(job(), label="after-unload")
    gate.set()
    await asyncio.sleep(0)
    assert not effects and not plugin._bg_tasks


@pytest.mark.asyncio
async def test_vote_timeout_releases_session_lock(plugin, monkeypatch):
    plugin.config["generation_timeout_sec"] = 0.01

    async def hang(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(plugin, "_lm_and_gather", hang)
    await asyncio.wait_for(plugin._vote_propose_run(None, "g1"), 0.5)
    assert not plugin._gen_locks["g1"].locked()
    assert "生成超时" in plugin.context.send_message.call_args.args[1]._chain[0][1]


@pytest.mark.asyncio
async def test_biography_cannot_enter_busy_session(plugin, monkeypatch):
    lock = asyncio.Lock()
    await lock.acquire()
    plugin._gen_locks["g1"] = lock
    gather = AsyncMock()
    monkeypatch.setattr(plugin, "_lm_and_gather", gather)
    await plugin._bio_only(None, "g1", None)
    assert gather.await_count == 0
    lock.release()


@pytest.mark.asyncio
async def test_biography_gather_has_total_timeout(plugin, monkeypatch):
    plugin.config["generation_timeout_sec"] = 0.01

    async def hang(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(plugin, "_lm_and_gather", hang)
    await asyncio.wait_for(plugin._bio_only(None, "g1", None), 0.5)
    assert not plugin._gen_locks["g1"].locked()


@pytest.mark.asyncio
async def test_continuation_filters_and_advances_watermark(plugin, monkeypatch):
    now = time.time()
    year, week = datetime.now().isocalendar()[:2]
    plugin.store.save(
        {
            "umo": "g1",
            "iso_year": year,
            "iso_week": week,
            "newest_msg_ts": now - 50,
            "chronicle_text": "previous",
            "msg_count": 1,
        }
    )
    evidence = EvidenceBundle(
        chronicle_msgs=[
            message(text="OLD", timestamp=now - 100),
            message(text="NEW", timestamp=now - 10),
        ]
    )
    monkeypatch.setattr(plugin, "_lm_and_gather", AsyncMock(return_value=(evidence, 7)))
    generate = AsyncMock(return_value="continuation")
    monkeypatch.setattr(main, "generate_continuation", generate)
    await plugin._continue_run(None, "g1")
    actual = generate.call_args.args[3]
    assert [m.content for m in actual.chronicle_msgs] == ["NEW"]
    assert plugin.store.get_by_id("g1", 1)["newest_msg_ts"] == now - 10
    await plugin._continue_run(None, "g1")
    assert generate.await_count == 1


@pytest.mark.asyncio
async def test_vote_range_matches_actual_candidates(plugin):
    plugin.config["enable_vote"] = True
    plugin._vote_state["g1"] = {
        "topics": ["one"],
        "expires_at": time.time() + 600,
        "voters": set(),
    }
    event = NS(
        unified_msg_origin="g1", get_sender_id=lambda: "v1", plain_result=lambda x: x
    )
    results = [r async for r in plugin.vote_cast_command(event, "3")]
    assert results == ["N 必须为 1-1"] and plugin._vote_state["g1"]["voters"] == set()


@pytest.mark.asyncio
async def test_disabled_weekly_callback_cannot_push(plugin, monkeypatch):
    plugin.config.update(enable_weekly_push=False, push_groups=["g1"])
    push = AsyncMock()
    monkeypatch.setattr(plugin, "_weekly_push_one", push)
    await plugin._weekly_push()
    assert push.await_count == 0


def test_weekly_rank_excludes_inactive_historical_members():
    evidence = EvidenceBundle(
        chronicle_msgs=[message("recent")], stats={"old": 10000, "recent": 1}
    )
    ranked = influence._fuse(
        evidence, {"lm": influence._lm_source(evidence), "sl": {"old": 1}}, None, 3
    )
    assert [r["sender_id"] for r in ranked] == ["recent"]


@pytest.mark.asyncio
async def test_relation_arc_uses_daily_window(tmp_path, monkeypatch):
    db = tmp_path / "relations.sqlite3"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE relation_daily(group_id TEXT,day TEXT,a TEXT,b TEXT,evidence REAL)"
        )
        conn.executemany(
            "INSERT INTO relation_daily VALUES(?,?,?,?,?)",
            [
                ("g1", str(date.today() - timedelta(days=365)), "old_a", "old_b", 100),
                ("g1", str(date.today()), "new_a", "new_b", 3),
                ("g2", str(date.today()), "other_a", "other_b", 999),
            ],
        )
    monkeypatch.setattr(influence, "_resolve_ra_db_path", lambda: db)
    assert await influence.relation_pairs(None, "fake:GroupMessage:g1", top_k=1) == [
        ("new_a", "new_b", 3)
    ]


@pytest.mark.asyncio
async def test_export_uses_file_component_with_unique_names(plugin):
    from astrbot.api.message_components import File

    plugin.store.save(
        {"umo": "g1", "iso_year": 2026, "iso_week": 40, "chronicle_text": "body"}
    )
    event = NS(unified_msg_origin="g1", send=AsyncMock(), plain_result=lambda x: x)
    for _ in range(2):
        assert [r async for r in plugin.chronicle_export_command(event)] == [
            "已导出史记文件"
        ]
    components = [call.args[0]._chain[0] for call in event.send.call_args_list]
    assert all(isinstance(c, File) for c in components)
    assert components[0].file != components[1].file


@pytest.mark.asyncio
async def test_view_and_biography_split_long_results(plugin, monkeypatch):
    plugin.config.update(max_reply_chars=100, use_forward_nodes=False)
    monkeypatch.setattr(deliver, "_SEGMENT_SLEEP_SECONDS", 0)
    plugin.store.save(
        {"umo": "g1", "iso_year": 2026, "iso_week": 40, "chronicle_text": "x" * 3000}
    )
    event = NS(unified_msg_origin="g1", plain_result=lambda x: x)
    results = [r async for r in plugin.chronicle_view_command(event)]
    assert "".join(results) == "x" * 3000 and max(map(len, results)) <= 100
    monkeypatch.setattr(
        plugin, "_lm_and_gather", AsyncMock(return_value=(EvidenceBundle(), 7))
    )
    monkeypatch.setattr(
        main,
        "rank_influential",
        AsyncMock(return_value=[{"sender_id": "u1", "sender_name": "member"}]),
    )
    monkeypatch.setattr(main, "generate_biography", AsyncMock(return_value="y" * 3000))
    await plugin._bio_only(None, "g1", None)
    assert (
        max(
            len(c.args[1]._chain[0][1])
            for c in plugin.context.send_message.call_args_list
        )
        <= 100
    )


def test_zero_limit_is_bounded(plugin):
    assert "".join(deliver.split_text("synthetic", 0)) == "synthetic"
    plugin.config.update(
        max_reply_chars=0, max_msgs=-1, generation_timeout_sec=float("nan")
    )
    assert (
        plugin._cfg("max_reply_chars", 1500) == 1 and plugin._cfg("max_msgs", 800) == 1
    )
    assert plugin._cfg("generation_timeout_sec", 180) == 180


def test_corrupt_store_remains_recoverable(tmp_path):
    path = tmp_path / "chronicles.json"
    original = '{"valuable":"synthetic"'
    path.write_text(original)
    store = ChronicleStore(str(tmp_path))
    with pytest.raises(RuntimeError):
        store.save({"umo": "g1", "iso_year": 2026, "iso_week": 40})
    assert path.read_text() == original


def test_bot_wakeup_mention_is_ignored():
    event = NS(
        get_messages=lambda: [main.At(qq="bot"), main.At(qq="target")],
        get_self_id=lambda: "bot",
    )
    assert main.GroupChroniclePlugin._extract_at_sender(event) == "target"


def test_export_does_not_repeat_legacy_embedded_biography():
    entry = {
        "iso_year": 2026,
        "iso_week": 40,
        "chronicle_text": "body\n## 人物传记：member\nBIO_SENTINEL",
        "biographies": [{"sender_name": "member", "text": "BIO_SENTINEL"}],
    }
    assert deliver.build_export_markdown(entry).count("BIO_SENTINEL") == 1
