"""Batch 2 storage, FD lifecycle & retention governance regressions for Group Chronicle.

Covers:
- P1-5: Managed SQLite connection pool replaces threading.local() FD leak; bounded open
  handles across 20+ worker threads, idle handle reclamation, and close() releasing all
  handles and truncating WAL.
- P1-4: run_cycle() executes prune_retention() even on early returns (LM not ready,
  circuit open, quota exhausted, budget busy); prune_retention() cleans report_drafts,
  invalid reports, resolved corrections, culture_revisions, and disk images/exports while
  preserving recent files and coverage records.
- P1-6: Single-transaction capture() (no per-message double-write), auto_vacuum=INCREMENTAL
  activation on legacy auto_vacuum=NONE DBs, and new SQLite indexes verified via
  EXPLAIN QUERY PLAN.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core import lifetime as lifetime_module
from core import lm_bridge
from core.archive_store import ArchiveStore
from core.culture_store import CultureStore
from core.lifetime import HistoryService
from tests.test_commands import make_plugin, run
from tests.test_history import U


# ---------------------------------------------------------------------------
# P1-5: Connection pool bounded handles, idle reclaim, close() + WAL truncate
# ---------------------------------------------------------------------------


def test_p1_5_multithreaded_connection_pool_bounded_and_close_releases_all(tmp_path: Path):
    db_path = tmp_path / "history.sqlite3"
    store = ArchiveStore(db_path)
    try:
        def worker(i: int):
            store.observe(U, f"u{i}", f"User {i}", 1700000000 + i, group_name="测试群")
            return store.groups()

        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
            results = list(pool.map(worker, range(60)))

        assert all(U in r for r in results)
        # Bounded pool instead of 24+ leaked threading.local() connections
        assert store.open_connection_count() <= store._max_pool_size

        # Idle reclamation closes idle handles cleanly
        reclaimed = store.reclaim_idle(max_idle_sec=0.0)
        assert reclaimed >= 1
        assert store.open_connection_count() == 0

        # Pool continues working transparently after reclaim
        assert store.group(U)["name"] == "测试群"
        assert store.open_connection_count() >= 1
    finally:
        store.close()

    # After close(), every connection across all worker threads is closed
    assert store.open_connection_count() == 0
    wal_path = Path(f"{db_path}-wal")
    if wal_path.exists():
        assert wal_path.stat().st_size == 0


# ---------------------------------------------------------------------------
# P1-4: Retention pruning survives early returns + full DB & disk cleanup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("early_case", ["lm_not_ready", "circuit_open", "quota_exhausted", "budget_busy"])
def test_p1_4_run_cycle_always_prunes_retention_despite_early_returns(
    tmp_path: Path, monkeypatch, early_case: str
):
    plugin = make_plugin({"history_enabled": True, "history_daily_limit": 10})
    plugin.data_dir = tmp_path
    mgr = HistoryService(plugin, plugin.store)
    store = mgr.store

    prune_calls: list[dict] = []

    def fake_prune(*, data_dir=None, **kwargs):
        prune_calls.append({"data_dir": data_dir})
        store._last_prune_ts = time.monotonic()
        return {}

    monkeypatch.setattr(store, "prune_retention", fake_prune)
    monkeypatch.setattr(mgr, "refresh_scope", AsyncMock(return_value=None))
    monkeypatch.setattr(mgr, "discover", AsyncMock(return_value=[U]))
    monkeypatch.setattr(mgr, "allowed", AsyncMock(return_value=True))

    if early_case == "lm_not_ready":
        monkeypatch.setattr(lm_bridge, "history_connection", lambda ctx: None)
    elif early_case == "circuit_open":
        monkeypatch.setattr(lm_bridge, "history_connection", lambda ctx: object())
        monkeypatch.setattr(plugin, "_llm_provider_id", lambda: "p1")
        monkeypatch.setattr(lifetime_module, "_provider_circuit_remaining", lambda ctx, pid: 120.0)
    elif early_case == "quota_exhausted":
        monkeypatch.setattr(lm_bridge, "history_connection", lambda ctx: object())
        monkeypatch.setattr(plugin, "_llm_provider_id", lambda: "")
        monkeypatch.setattr(store, "quota", lambda day: 999)
    elif early_case == "budget_busy":
        monkeypatch.setattr(lm_bridge, "history_connection", lambda ctx: object())
        monkeypatch.setattr(plugin, "_llm_provider_id", lambda: "")
        monkeypatch.setattr(store, "quota", lambda day: 0)

        async def raise_busy(umo, mode):
            raise RuntimeError("provider_budget_busy")

        monkeypatch.setattr(mgr, "process_one", raise_busy)
        monkeypatch.setattr(lifetime_module, "is_budget_busy_error", lambda exc: True)
        monkeypatch.setattr(lifetime_module, "budget_busy_retry_after", lambda exc: 30.0)

    try:
        run(mgr.run_cycle())
        assert len(prune_calls) == 1
        assert prune_calls[0]["data_dir"] == tmp_path
        # Second call within 24h respects cooldown
        run(mgr.run_cycle())
        assert len(prune_calls) == 1
    finally:
        store.close()


def test_p1_4_prune_retention_cleans_db_tables_and_disk_dirs(tmp_path: Path):
    db_path = tmp_path / "history.sqlite3"
    store = ArchiveStore(db_path)
    CultureStore(store)  # ensure culture_revisions table exists

    now = time.time()
    old_ts = now - 100 * 86400
    recent_ts = now - 60

    try:
        with store.connection(write=True) as db:
            # revisions: old normal revision deleted, old coverage preserved
            db.execute(
                "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
                (U, "event", "e1", "{}", "u1", "edit", old_ts),
            )
            db.execute(
                "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
                (U, "coverage", "c1", "{}", "sys", "mark", old_ts),
            )
            # notices: old deleted, recent kept
            db.execute(
                "INSERT INTO notices(key,created) VALUES(?,?)",
                ("old_k", old_ts),
            )
            db.execute(
                "INSERT INTO notices(key,created) VALUES(?,?)",
                ("new_k", recent_ts),
            )
            # report_drafts: old deleted, recent kept
            db.execute(
                "INSERT INTO report_drafts(umo,facts_key,stage,privacy_revision,payload,updated) "
                "VALUES(?,'d_old','draft',0,'{}',?)",
                (U, old_ts),
            )
            db.execute(
                "INSERT INTO report_drafts(umo,facts_key,stage,privacy_revision,payload,updated) "
                "VALUES(?,'d_new','draft',0,'{}',?)",
                (U, recent_ts),
            )
            # reports: old invalid deleted, old valid kept, recent invalid kept
            db.execute(
                "INSERT INTO reports(umo,cache_key,entry,created,invalid) "
                "VALUES(?,'r_old_inv','{}',?,1)",
                (U, old_ts),
            )
            db.execute(
                "INSERT INTO reports(umo,cache_key,entry,created,invalid) "
                "VALUES(?,'r_old_ok','{}',?,0)",
                (U, old_ts),
            )
            db.execute(
                "INSERT INTO reports(umo,cache_key,entry,created,invalid) "
                "VALUES(?,'r_new_inv','{}',?,1)",
                (U, recent_ts),
            )
            # corrections: old resolved deleted, old unresolved kept
            db.execute(
                "INSERT INTO corrections(umo,event_id,uid,note,created,resolved) "
                "VALUES(?,1,'u1','fixed',?,1)",
                (U, old_ts),
            )
            db.execute(
                "INSERT INTO corrections(umo,event_id,uid,note,created,resolved) "
                "VALUES(?,1,'u1','open',?,0)",
                (U, old_ts),
            )
            # culture_revisions: old deleted, recent kept
            db.execute(
                "INSERT INTO culture_revisions(umo,entry_id,before_data,actor,action,note,created) "
                "VALUES(?,1,'{}','sys','create','n',?)",
                (U, old_ts),
            )
            db.execute(
                "INSERT INTO culture_revisions(umo,entry_id,before_data,actor,action,note,created) "
                "VALUES(?,1,'{}','sys','edit','n',?)",
                (U, recent_ts),
            )

        # Disk files under images/ and exports/
        images_dir = tmp_path / "images"
        exports_dir = tmp_path / "exports"
        images_dir.mkdir()
        exports_dir.mkdir()
        old_img = images_dir / "old.png"
        new_img = images_dir / "new.png"
        old_exp = exports_dir / "old.md"
        new_exp = exports_dir / "new.md"
        for f in (old_img, new_img, old_exp, new_exp):
            f.write_bytes(b"data")
        os.utime(old_img, (old_ts, old_ts))
        os.utime(old_exp, (old_ts, old_ts))

        stats = store.prune_retention(data_dir=tmp_path)
        assert stats["revisions"] == 1
        assert stats["notices"] == 1
        assert stats["report_drafts"] == 1
        assert stats["invalid_reports"] == 1
        assert stats["corrections"] == 1
        assert stats["culture_revisions"] == 1
        assert stats["media_files"] == 1
        assert stats["export_files"] == 1

        # Verify preserved rows & files
        with store.connection() as db:
            assert db.execute("SELECT COUNT(*) FROM revisions WHERE entity='coverage'").fetchone()[0] == 1
            assert db.execute("SELECT COUNT(*) FROM notices").fetchone()[0] == 1
            assert db.execute("SELECT COUNT(*) FROM report_drafts").fetchone()[0] == 1
            assert db.execute("SELECT COUNT(*) FROM reports WHERE cache_key='r_old_ok'").fetchone()[0] == 1
            assert db.execute("SELECT COUNT(*) FROM reports WHERE cache_key='r_new_inv'").fetchone()[0] == 1
            assert db.execute("SELECT COUNT(*) FROM corrections WHERE resolved=0").fetchone()[0] == 1
            assert db.execute("SELECT COUNT(*) FROM culture_revisions").fetchone()[0] == 1
        assert not old_img.exists() and new_img.exists()
        assert not old_exp.exists() and new_exp.exists()
    finally:
        store.close()


# ---------------------------------------------------------------------------
# P1-6: Single-txn capture, legacy auto_vacuum=INCREMENTAL activation, indexes
# ---------------------------------------------------------------------------


def test_p1_6_capture_single_write_transaction_per_message(tmp_path: Path, monkeypatch):
    plugin = make_plugin({"history_enabled": True})
    plugin.data_dir = tmp_path
    mgr = HistoryService(plugin, plugin.store)
    store = mgr.store
    monkeypatch.setattr(mgr, "allowed", AsyncMock(return_value=True))

    write_txn_count = 0
    orig_conn = store.connection

    def counting_conn(*, write=False):
        nonlocal write_txn_count
        if write:
            write_txn_count += 1
        return orig_conn(write=write)

    monkeypatch.setattr(store, "connection", counting_conn)
    observation = {
        "umo": U,
        "uid": "1001",
        "name": "Alice",
        "ts": 1700000100,
        "self_id": "9999",
        "group_name": "单事务测试群",
    }

    try:
        run(mgr.capture(observation))
        assert write_txn_count == 1
        assert store.group(U)["name"] == "单事务测试群"
        m = store.member(U, "1001")
        assert m is not None and m["name"] == "Alice"
    finally:
        store.close()


def test_p1_6_legacy_db_activates_incremental_auto_vacuum_and_indexes_used(tmp_path: Path):
    db_path = tmp_path / "legacy.sqlite3"
    # Pre-create a legacy SQLite file with auto_vacuum = NONE (0) and real pages
    raw = sqlite3.connect(str(db_path))
    raw.execute("PRAGMA auto_vacuum=NONE")
    raw.execute("CREATE TABLE legacy_seed(id INTEGER PRIMARY KEY, payload TEXT)")
    raw.execute("INSERT INTO legacy_seed(payload) VALUES ('hello')")
    raw.commit()
    assert raw.execute("PRAGMA auto_vacuum").fetchone()[0] == 0
    raw.close()

    store = ArchiveStore(db_path)
    CultureStore(store)
    try:
        assert store._auto_vacuum_enabled is True
        with store.connection() as db:
            # 2 == INCREMENTAL
            assert db.execute("PRAGMA auto_vacuum").fetchone()[0] == 2

            queries_and_indexes = [
                (
                    "EXPLAIN QUERY PLAN SELECT key FROM notices WHERE created < 100",
                    (),
                    "notices_created",
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT id FROM corrections WHERE umo=? AND resolved=1 AND created < 100",
                    (U,),
                    "corrections_group",
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT 1 FROM hides WHERE umo=? AND event_id=?",
                    (U, 1),
                    "hides_event",
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT facts_key FROM report_drafts WHERE updated < 100",
                    (),
                    "report_drafts_updated",
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT id FROM revisions WHERE created < 100 AND entity != 'coverage'",
                    (),
                    "revisions_cleanup",
                ),
                (
                    "EXPLAIN QUERY PLAN SELECT id FROM culture_revisions WHERE umo=? AND entry_id=? ORDER BY id DESC",
                    (U, 1),
                    "culture_revisions_entry",
                ),
            ]
            for sql, params, expected_idx in queries_and_indexes:
                plan_text = " ".join(str(r[-1]) for r in db.execute(sql, params).fetchall())
                assert expected_idx in plan_text, f"{expected_idx} not used in plan: {plan_text}"
    finally:
        store.close()
