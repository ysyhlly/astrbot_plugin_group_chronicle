"""Focused unit tests for 2GB RAM optimizations in astrbot_plugin_group_chronicle."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

_PLUGIN_ROOT = str(Path(__file__).resolve().parents[1])
if _PLUGIN_ROOT not in sys.path:
    sys.path.insert(0, _PLUGIN_ROOT)

import series_panels  # noqa: E402
from core import render as render_mod  # noqa: E402
from core.archive_store import ArchiveStore  # noqa: E402
from core.culture_store import CultureStore  # noqa: E402
from core.lifetime import HistoryService  # noqa: E402
from core.store import ChronicleStore  # noqa: E402


def _set_profile(monkeypatch: pytest.MonkeyPatch, profile: str) -> None:
    monkeypatch.setenv("ASTRBOT_RESOURCE_PROFILE", profile)


def test_render_concurrency_low_ram_args_heavy_slot_and_watermark(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    assert render_mod._PLAYWRIGHT_MAX_CONCURRENCY == 1

    launch_args_seen: list[list[str]] = []
    slot_calls: list[tuple[str, str]] = []

    class _FakePage:
        async def set_content(self, _html: str) -> None:
            pass

        async def wait_for_timeout(self, _ms: int) -> None:
            pass

        async def query_selector(self, _sel: str):
            return self

        async def screenshot(self, path=None) -> None:
            Path(path).write_bytes(b"png")

    class _FakeBrowser:
        async def new_page(self, **kwargs):
            return _FakePage()

        async def close(self) -> None:
            pass

    class _FakeChromium:
        async def launch(self, **kwargs):
            launch_args_seen.append(list(kwargs.get("args") or []))
            return _FakeBrowser()

    class _FakePW:
        def __init__(self) -> None:
            self.chromium = _FakeChromium()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    @asynccontextmanager
    async def fake_slot(context, purpose, *, plugin_id="", timeout=None):
        slot_calls.append((purpose, plugin_id))
        yield {}

    pw = types.ModuleType("playwright")
    pw.__path__ = []
    api = types.ModuleType("playwright.async_api")
    api.async_playwright = _FakePW
    monkeypatch.setitem(sys.modules, "playwright", pw)
    monkeypatch.setitem(sys.modules, "playwright.async_api", api)
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(series_panels, "heavy_subprocess_slot", fake_slot)
    monkeypatch.setattr(series_panels, "_WATERMARK_CACHE", {})
    monkeypatch.setenv("ASTRBOT_TEST_RSS_MB", "100")

    out = asyncio.run(render_mod.render_chronicle_image("# Title", "Group", str(tmp_path)))
    assert out is not None and Path(out).exists()
    assert slot_calls == [("chronicle_render", "astrbot_plugin_group_chronicle")]
    assert launch_args_seen
    for flag in (
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-extensions",
        "--disable-background-networking",
        "--no-zygote",
    ):
        assert flag in launch_args_seen[0]

    # Hard memory watermark gracefully skips Playwright and falls back to None
    _set_profile(monkeypatch, "low_memory")
    monkeypatch.setenv("ASTRBOT_TEST_RSS_MB", "950")
    out_hard = asyncio.run(
        render_mod.render_chronicle_image("# Title", "Group", str(tmp_path))
    )
    assert out_hard is None


def test_render_cache_bounded_and_jinja_env_cached_and_aggressive_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    render_mod.clear_render_cache()
    for i in range(render_mod._RENDER_CACHE_MAX_SIZE + 15):
        p = tmp_path / f"img_{i}.png"
        p.write_bytes(b"x")
        render_mod._render_cache_set(f"k{i}", str(p))
    assert len(render_mod._RENDER_CACHE) == render_mod._RENDER_CACHE_MAX_SIZE
    assert "k0" not in render_mod._RENDER_CACHE

    env1 = render_mod._get_jinja_env()
    env2 = render_mod._get_jinja_env()
    assert env1 is env2

    # In aggressive mode, default render_fallback_playwright=None disables playwright fallback
    _set_profile(monkeypatch, "low_memory_aggressive")
    pw_called = []

    async def fake_pw_render(*args, **kwargs):
        pw_called.append(True)
        return str(tmp_path / "x.png")

    monkeypatch.setattr(render_mod, "render_chronicle_image", fake_pw_render)
    text, img = asyncio.run(
        render_mod.render_text_or_image(
            "hello", "grp", str(tmp_path), enable_cache=False, render_mode="t2i"
        )
    )
    assert text == "hello"
    assert img is None
    assert pw_called == []

    # Explicit override still enables fallback
    text2, img2 = asyncio.run(
        render_mod.render_text_or_image(
            "hello",
            "grp",
            str(tmp_path),
            enable_cache=False,
            render_mode="t2i",
            render_fallback_playwright=True,
        )
    )
    assert text2 == "hello"
    assert img2 is not None
    assert pw_called == [True]


def test_archive_store_pragmas_pool_size_and_events_pushdown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    _set_profile(monkeypatch, "low_memory_aggressive")
    store = ArchiveStore(tmp_path / "hist.sqlite3")
    try:
        assert store._max_pool_size == 1
        assert store._idle_timeout_sec == 30.0
        with store.connection() as db:
            assert db.execute("PRAGMA temp_store").fetchone()[0] == 1  # FILE
            assert db.execute("PRAGMA mmap_size").fetchone()[0] == 0
            assert db.execute("PRAGMA cache_size").fetchone()[0] < 0

        umo = "aiocqhttp:GroupMessage:1001"
        with store.connection(write=True) as db:
            db.execute("INSERT OR IGNORE INTO groups(umo, name) VALUES(?, ?)", (umo, "G"))
            for idx in range(1, 16):
                status = (
                    "rejected"
                    if idx % 3 == 0
                    else ("pending" if idx % 2 == 0 else "approved")
                )
                ongoing = idx == 2  # older event but ongoing=True
                payload = (
                    f'{{"kind":"project","title":"E{idx}","detail":"d{idx}",'
                    f'"participants":[{{"uid":"u1","name":"A","role":1}}],'
                    f'"ongoing":{"true" if ongoing else "false"}}}'
                )
                db.execute(
                    "INSERT INTO events(umo,fingerprint,status,payload,event_at,last_event_at,updated) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (umo, f"fp-{idx}", status, payload, float(idx * 100), float(idx * 100), float(idx * 100)),
                )

        rows = store.events(
            umo, statuses=("approved", "pending"), public=True, limit=5, ongoing_first=True
        )
        assert len(rows) == 5
        assert all(r["status"] in ("approved", "pending") for r in rows)
        assert rows[0]["title"] == "E2"
        assert rows[0]["ongoing"] is True
    finally:
        store.close()


def test_culture_store_entries_sql_pushdown_and_query_pagination(tmp_path: Path):
    archive = ArchiveStore(tmp_path / "culture.sqlite3")
    culture = CultureStore(archive)
    umo = "aiocqhttp:GroupMessage:2002"
    try:
        with archive.connection(write=True) as db:
            for idx in range(1, 11):
                st = "approved" if idx <= 6 else "pending"
                payload = (
                    f'{{"name":"梗{idx}","meaning":"说明{idx}","aliases":[],'
                    f'"sources":[],"timeline":[],"subjects":[],"first_seen":{idx}}}'
                )
                db.execute(
                    "INSERT INTO culture_entries(umo,source,fingerprint,name,status,payload,created,updated) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (umo, "lm", f"fp{idx}", f"梗{idx}", st, payload, float(idx), float(idx)),
                )

        page, total = culture.entries(umo, status="approved", limit=2, offset=1)
        assert total == 6
        assert [item["name"] for item in page] == ["梗5", "梗4"]

        q_page, q_total = culture.entries(umo, status="approved", query="梗", limit=2, offset=2)
        assert q_total == 6
        assert [item["name"] for item in q_page] == ["梗4", "梗3"]
    finally:
        archive.close()


def test_history_capture_coalesces_observe_and_clears_legacy_after_migration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    legacy = ChronicleStore(str(tmp_path / "chronicles"))
    legacy.save(
        {
            "umo": "aiocqhttp:GroupMessage:3003",
            "group_name": "测试群",
            "iso_year": 2026,
            "iso_week": 10,
            "created_at": 1700000000.0,
            "newest_msg_ts": 1700000000.0,
            "chronicle_text": "legacy edition",
            "biographies": [],
            "image_path": None,
        }
    )
    assert len(legacy._data) == 1

    plugin = SimpleNamespace(
        data_dir=str(tmp_path),
        context=SimpleNamespace(cron_manager=None),
        config={"history_enabled": True},
        _terminating=False,
        _cfg=lambda key, default=None: True if key == "history_enabled" else default,
        _spawn_bg=lambda coro, label="": coro.close(),
    )
    service = HistoryService(plugin, legacy)

    async def always_allowed(*args, **kwargs):
        return True

    monkeypatch.setattr(service, "allowed", always_allowed)

    observe_calls: list[tuple] = []
    orig_observe = service.store.observe

    def spy_observe(umo, uid, name, ts=None, *, group_name=None):
        observe_calls.append((umo, uid, name, ts, group_name))
        return orig_observe(umo, uid, name, ts, group_name=group_name)

    monkeypatch.setattr(service.store, "observe", spy_observe)

    async def _run():
        await service.initialize()
        # Legacy dict cleared after SQLite import, while EditionStore still serves the migrated edition
        assert legacy._data == {}
        assert len(service.editions.list("aiocqhttp:GroupMessage:3003")) == 1

        umo = "aiocqhttp:GroupMessage:3003"
        # First observe writes
        await service.capture({"umo": umo, "uid": "u1", "name": "Alice", "ts": 1000.0, "group_name": "G"})
        # Repeated identical observe within 300s is coalesced
        await service.capture({"umo": umo, "uid": "u1", "name": "Alice", "ts": 1100.0, "group_name": "G"})
        assert len(observe_calls) == 1
        # Rename writes immediately
        await service.capture({"umo": umo, "uid": "u1", "name": "Alice2", "ts": 1120.0, "group_name": "G"})
        assert len(observe_calls) == 2
        # Rename back to Alice writes immediately
        await service.capture({"umo": umo, "uid": "u1", "name": "Alice", "ts": 1140.0, "group_name": "G"})
        assert len(observe_calls) == 3
        # Gap >= 300s writes immediately
        await service.capture({"umo": umo, "uid": "u1", "name": "Alice", "ts": 1500.0, "group_name": "G"})
        assert len(observe_calls) == 4
        # Membership event invalidates coalescing cache
        await service.capture(
            {"umo": umo, "uid": "u1", "kind": "join", "ts": 1510.0, "key": "k1", "name": "Alice"}
        )
        await service.capture({"umo": umo, "uid": "u1", "name": "Alice", "ts": 1520.0, "group_name": "G"})
        assert len(observe_calls) == 5

    try:
        asyncio.run(_run())
    finally:
        service.store.close()
