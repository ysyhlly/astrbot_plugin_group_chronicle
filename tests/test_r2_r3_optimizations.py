"""Focused regression tests for R2/R3 optimizations in astrbot_plugin_group_chronicle."""

from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timedelta
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

import core.lifetime as lifetime_mod
import core.llm_call as llm_call_mod
import core.lm_bridge as lm_bridge_mod
from core.archive_store import ArchiveStore
from core.llm_call import LLMUnavailableError, call_llm, is_budget_busy_error
from main import GroupChroniclePlugin

U1 = "aiocqhttp:GroupMessage:88001"
U2 = "aiocqhttp:GroupMessage:88002"


async def _no_sleep(_delay: float) -> None:
    return None


class _BusyError(RuntimeError):
    is_provider_budget_busy = True

    def __init__(self, msg: str = "budget_circuit_open", *, retry_after: float = 42.0):
        super().__init__(msg)
        self.retry_after = retry_after
        self.reason = "circuit_open"


# ---------------------------------------------------------------------------
# 1. Series-inherited provider fails fast on non-retryable execution errors
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        asyncio.TimeoutError("upstream timed out"),
        RuntimeError("HTTP 401 Unauthorized"),
        RuntimeError("HTTP 403 Forbidden"),
        RuntimeError("content_filter safety policy refused"),
    ],
)
async def test_series_inherited_provider_fails_fast_on_non_retryable_errors(
    monkeypatch, exc
):
    monkeypatch.setattr(llm_call_mod.asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(
        llm_call_mod,
        "resolve_series_provider",
        lambda _ctx, explicit="", **_kw: explicit or "series-inherited-prov",
    )

    series_prov = NS(
        is_systemone_provider=False,
        text_chat=AsyncMock(return_value=NS(completion_text="should-not-hit")),
    )
    flagship_prov = NS(
        is_systemone_provider=False,
        text_chat=AsyncMock(return_value=NS(completion_text="EXPENSIVE")),
    )
    gen_calls: list[str] = []

    async def _llm_generate(chat_provider_id=None, **_kw):
        gen_calls.append(str(chat_provider_id))
        raise exc

    get_session_pid = AsyncMock(return_value="session-flagship")
    ctx = NS(
        get_all_stars=lambda: {},
        get_provider_by_id=lambda pid: (
            series_prov if pid == "series-inherited-prov" else flagship_prov
        ),
        get_current_chat_provider_id=get_session_pid,
        get_using_provider=lambda **_kw: flagship_prov,
        llm_generate=_llm_generate,
    )

    with pytest.raises(LLMUnavailableError):
        await call_llm(ctx, U1, "测试提示", provider_id=None)

    assert gen_calls == ["series-inherited-prov"]
    assert series_prov.text_chat.await_count == 0
    assert get_session_pid.await_count == 0
    assert flagship_prov.text_chat.await_count == 0


# ---------------------------------------------------------------------------
# 2. Quota refund on failure + run_cycle breaks immediately on busy / circuit
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_run_cycle_breaks_immediately_on_provider_budget_busy(
    tmp_path, monkeypatch
):
    store = ArchiveStore(tmp_path / "busy.sqlite3")
    store.group(U1, "群一")
    store.group(U2, "群二")
    ctx = NS(get_all_stars=lambda: {}, get_config=lambda: {})
    plugin = GroupChroniclePlugin(
        ctx,
        {"history_enabled": True, "culture_enabled": True, "history_daily_limit": 20},
    )
    plugin.history.store = store
    monkeypatch.setattr(lm_bridge_mod, "history_connection", lambda _ctx: object())
    monkeypatch.setattr(plugin.history, "refresh_scope", AsyncMock())
    monkeypatch.setattr(plugin.history, "discover", AsyncMock(return_value=[U1, U2]))
    monkeypatch.setattr(plugin.history, "allowed", AsyncMock(return_value=True))

    called_groups: list[str] = []

    async def _busy_process_one(umo, mode="back"):
        called_groups.append(f"{mode}:{umo}")
        err = LLMUnavailableError("共享模型队列繁忙")
        err.is_provider_budget_busy = True
        err.retry_after = 35.0
        raise err

    culture_step = AsyncMock(return_value=False)
    monkeypatch.setattr(plugin.history, "process_one", _busy_process_one)
    monkeypatch.setattr(plugin.culture, "step", culture_step)

    await plugin.history.run_cycle()

    # Stopped on the very first group without touching U2 or culture!
    assert called_groups == [f"live:{U1}"]
    assert culture_step.await_count == 0
    assert is_budget_busy_error(_BusyError()) is True
    store.close()


@pytest.mark.asyncio
async def test_run_cycle_preflight_skips_when_provider_circuit_open(
    tmp_path, monkeypatch
):
    store = ArchiveStore(tmp_path / "circuit.sqlite3")
    store.group(U1, "群一")
    ctx = NS(get_all_stars=lambda: {}, get_config=lambda: {})
    plugin = GroupChroniclePlugin(
        ctx, {"history_enabled": True, "llm_provider_id": "prov-open"}
    )
    plugin.history.store = store
    monkeypatch.setattr(lm_bridge_mod, "history_connection", lambda _ctx: object())
    monkeypatch.setattr(
        lifetime_mod, "_provider_circuit_remaining", lambda _c, _p: 90.0
    )
    discover = AsyncMock(return_value=[U1])
    monkeypatch.setattr(plugin.history, "discover", discover)

    await plugin.history.run_cycle()
    assert discover.await_count == 0
    store.close()


@pytest.mark.asyncio
async def test_run_cycle_batches_consecutive_pages_per_group(tmp_path, monkeypatch):
    store = ArchiveStore(tmp_path / "batch.sqlite3")
    store.group(U1, "群一")
    store.group(U2, "群二")
    ctx = NS(get_all_stars=lambda: {}, get_config=lambda: {})
    plugin = GroupChroniclePlugin(
        ctx,
        {"history_enabled": True, "culture_enabled": False, "history_daily_limit": 24},
    )
    plugin.history.store = store
    plugin.culture = None
    monkeypatch.setattr(lm_bridge_mod, "history_connection", lambda _ctx: object())
    monkeypatch.setattr(plugin.history, "refresh_scope", AsyncMock())
    monkeypatch.setattr(plugin.history, "discover", AsyncMock(return_value=[U1, U2]))
    monkeypatch.setattr(plugin.history, "allowed", AsyncMock(return_value=True))

    order: list[str] = []
    day = datetime.now(plugin.history._series_tz()).date().isoformat()

    async def _fake_process_one(umo, mode="back"):
        if mode != "live":
            return False
        order.append(umo)
        return store.consume_quota(day, 24, kind="history")

    monkeypatch.setattr(plugin.history, "process_one", _fake_process_one)
    await plugin.history.run_cycle()

    # cycle_budget = ceil(24/6) = 4; consecutive per_group_batch = 2 -> [U1, U1, U2, U2]
    assert order == [U1, U1, U2, U2]
    store.close()


def test_refund_quota_restores_total_and_bucket(tmp_path):
    store = ArchiveStore(tmp_path / "quota.sqlite3")
    day = "2026-11-24"
    assert store.consume_quota(day, 10, kind="history") is True
    assert store.consume_quota(day, 10, kind="culture", reserved_history=5) is True
    assert store.quota_breakdown(day) == {"total": 2, "history": 1, "culture": 1}

    store.refund_quota(day, kind="history")
    assert store.quota_breakdown(day) == {"total": 1, "history": 0, "culture": 1}

    store.refund_quota(day, kind="culture")
    assert store.quota_breakdown(day) == {"total": 0, "history": 0, "culture": 0}
    # Never goes negative
    store.refund_quota(day, kind="culture")
    assert store.quota_breakdown(day) == {"total": 0, "history": 0, "culture": 0}
    store.close()


# ---------------------------------------------------------------------------
# 3. detect_lm short TTL cache + invalidation
# ---------------------------------------------------------------------------
def test_detect_lm_ttl_cache_avoids_repeated_star_scans(monkeypatch):
    lm_bridge_mod.invalidate_lm_cache()
    monkeypatch.setattr(lm_bridge_mod, "_lm_get_active", None)
    scan_count = 0

    owner = NS(
        get_dynamics_capabilities=lambda: {
            "schema_version": 1,
            "provider_id": "astrbot_plugin_dynamics_memory",
            "enabled": True,
            "ready": True,
            "capabilities": {"memory": 1},
        }
    )
    star = NS(
        activated=True,
        star_cls=owner,
        name="astrbot_plugin_dynamics_memory",
        display_name="DynamicsMemory",
        root_dir_name="astrbot_plugin_dynamics_memory",
        module_path="astrbot_plugin_dynamics_memory.main",
    )

    def _get_all_stars():
        nonlocal scan_count
        scan_count += 1
        return [star]

    ctx = NS(get_registered_star=lambda _a: None, get_all_stars=_get_all_stars)

    assert lm_bridge_mod.detect_lm(ctx) is owner
    assert lm_bridge_mod.detect_lm(ctx) is owner
    assert lm_bridge_mod.detect_lm(ctx) is owner
    assert scan_count == 1

    # Explicit invalidation triggers a fresh scan
    lm_bridge_mod.invalidate_lm_cache()
    assert lm_bridge_mod.detect_lm(ctx) is owner
    assert scan_count == 2
    lm_bridge_mod.invalidate_lm_cache()


# ---------------------------------------------------------------------------
# 4. ArchiveStore: auto_vacuum=INCREMENTAL, WAL concurrent reads, retention prune
# ---------------------------------------------------------------------------
def test_archive_store_auto_vacuum_concurrent_reads_and_retention(tmp_path):
    db_path = tmp_path / "archive_opt.sqlite3"
    store = ArchiveStore(db_path)
    store.group(U1, "测试群")

    # 1. PRAGMA auto_vacuum == 2 (INCREMENTAL)
    with store.connection() as db:
        av = db.execute("PRAGMA auto_vacuum").fetchone()[0]
    assert int(av) == 2

    # 2. WAL concurrent read does NOT block on write lock
    if store._journal_mode == "WAL":
        writer_ready = threading.Event()
        release_writer = threading.Event()
        read_result: list[list[str]] = []

        def _writer():
            with store.connection(write=True) as db:
                db.execute("INSERT OR IGNORE INTO groups(umo,name) VALUES(?,?)", (U2, "群二"))
                writer_ready.set()
                release_writer.wait(timeout=3.0)

        t = threading.Thread(target=_writer)
        t.start()
        assert writer_ready.wait(timeout=2.0)
        try:
            # Must complete while writer still holds write lock!
            read_result.append(store.groups())
        finally:
            release_writer.set()
            t.join(timeout=3.0)

        assert U1 in read_result[0]

    # 3. Retention pruning removes old revisions/notices/quota while keeping coverage & recent
    now = time.time()
    old_ts = now - 120 * 86400
    old_day = (datetime.now() - timedelta(days=45)).date().isoformat()
    today = datetime.now().date().isoformat()

    with store.connection(write=True) as db:
        db.execute(
            "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
            (U1, "event", "1", "{}", "admin", "old edit", old_ts),
        )
        db.execute(
            "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
            (U1, "coverage", U1, "{}", "bot", "bot left", old_ts),
        )
        db.execute(
            "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
            (U1, "event", "2", "{}", "admin", "recent edit", now),
        )
        db.execute("INSERT INTO notices(key, created) VALUES(?, ?)", ("old_notice", old_ts))
        db.execute("INSERT INTO notices(key, created) VALUES(?, ?)", ("new_notice", now))
        db.execute("INSERT INTO quota(day, used) VALUES(?, ?)", (old_day, 5))
        db.execute("INSERT INTO quota(day, used) VALUES(?, ?)", (today, 2))
        db.execute(
            "INSERT INTO quota_buckets(day, kind, used) VALUES(?, 'history', 5)",
            (old_day,),
        )
        db.execute(
            "INSERT INTO quota_buckets(day, kind, used) VALUES(?, 'history', 2)",
            (today,),
        )

    pruned = store.prune_retention(
        revisions_days=90, notices_days=90, quota_days=30, now=now
    )
    assert pruned["revisions"] == 1
    assert pruned["notices"] == 1
    assert pruned["quota"] == 1
    assert pruned["quota_buckets"] == 1

    with store.connection() as db:
        revs = [
            (r["entity"], r["note"])
            for r in db.execute("SELECT entity, note FROM revisions ORDER BY id")
        ]
        notices = [r[0] for r in db.execute("SELECT key FROM notices")]
        days = [r[0] for r in db.execute("SELECT day FROM quota")]

    assert revs == [("coverage", "bot left"), ("event", "recent edit")]
    assert notices == ["new_notice"]
    assert days == [today]

    # 4. close() checkpoints WAL cleanly
    store.close()
    wal = db_path.parent / f"{db_path.name}-wal"
    if wal.exists():
        assert wal.stat().st_size == 0
