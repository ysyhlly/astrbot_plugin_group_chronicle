"""Tests for core/lm_bridge.py.

No AstrBot / LivingMemory installation required: the only LivingMemory import
inside lm_bridge is guarded, and all plugin fakes live here.
"""

import asyncio
import importlib
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.lm_bridge as lm_bridge


class FakeInitializer:
    """Minimal stand-in for LivingMemory's PluginInitializer."""

    def __init__(self, initialized=True, ready=True, has_engine=True, has_manager=True):
        self.is_initialized = initialized
        self._ready = ready
        self.memory_engine = object() if has_engine else None
        self.conversation_manager = object() if has_manager else None

    async def ensure_initialized(self, timeout=30.0):
        return self._ready


class FakePlugin:
    def __init__(self, initializer=None):
        self.initializer = initializer


class FakeStar:
    def __init__(self, star_cls=None, activated=True):
        self.star_cls = star_cls
        self.activated = activated
        self.name = "LivingMemory"
        self.display_name = "LivingMemory"
        self.root_dir_name = "astrbot_plugin_livingmemory"
        self.module_path = "astrbot_plugin_livingmemory.main"


class DummyContext:
    def __init__(self, star_by_alias=None, stars=None):
        self._star_by_alias = star_by_alias or {}
        self._stars = stars or []

    def get_registered_star(self, alias):
        return self._star_by_alias.get(alias)

    def get_all_stars(self):
        return self._stars


def test_primary_path_returns_active_plugin(monkeypatch):
    """(1) PRIMARY: _lm_get_active() returns the plugin instance directly."""
    plugin = FakePlugin(FakeInitializer())
    monkeypatch.setattr(lm_bridge, "_lm_get_active", lambda: plugin)
    assert lm_bridge.detect_lm(DummyContext()) is plugin


def test_inactive_star_via_get_registered_star_returns_none(monkeypatch):
    """(2) PRIMARY None + get_registered_star returns an inactive star -> None."""
    monkeypatch.setattr(lm_bridge, "_lm_get_active", lambda: None)
    ctx = DummyContext(
        star_by_alias={"LivingMemory": FakeStar(star_cls=FakePlugin(), activated=False)}
    )
    assert lm_bridge.detect_lm(ctx) is None


def test_active_star_fallback_and_ensure_ready(monkeypatch):
    """(3) PRIMARY None + active star (star_cls=fake plugin) -> detect + ensure_ready True."""
    monkeypatch.setattr(lm_bridge, "_lm_get_active", lambda: None)
    plugin = FakePlugin(
        FakeInitializer(initialized=False, has_engine=True, has_manager=True)
    )
    ctx = DummyContext(
        star_by_alias={"LivingMemory": FakeStar(star_cls=plugin, activated=True)}
    )
    found = lm_bridge.detect_lm(ctx)
    assert found is plugin  # star.star_cls, not the star meta
    assert asyncio.run(lm_bridge.ensure_ready(found)) is True


def test_ensure_initialized_failure(monkeypatch):
    """(4) ensure_initialized() False -> ensure_ready False; reason contains 未初始化."""
    plugin = FakePlugin(
        FakeInitializer(
            initialized=False, ready=False, has_engine=True, has_manager=True
        )
    )
    monkeypatch.setattr(lm_bridge, "_lm_get_active", lambda: plugin)
    assert asyncio.run(lm_bridge.ensure_ready(plugin)) is False
    assert "未初始化" in lm_bridge.lm_unavailable_reason(stage="not_ready")


def test_ensure_ready_missing_initializer():
    """ensure_ready with no initializer -> False (getattr guard)."""
    assert asyncio.run(lm_bridge.ensure_ready(FakePlugin(initializer=None))) is False


def test_import_survives_without_livingmemory():
    """(5) LM absent -> module imports, _lm_get_active None, reason says 未加载."""
    # Direct simulation of LM absence
    lm_bridge._lm_get_active = None
    assert lm_bridge.detect_lm(DummyContext()) is None
    assert lm_bridge.lm_unavailable_reason() == "LivingMemory 插件未加载"

    # True import-path simulation: poison sys.modules so the guarded import
    # fails, then re-import the module.
    blocker = types.ModuleType("astrbot_plugin_livingmemory")
    blocker.__path__ = []  # submodule import -> ModuleNotFoundError
    saved = sys.modules.get("astrbot_plugin_livingmemory")
    sys.modules["astrbot_plugin_livingmemory"] = blocker
    try:
        reloaded = importlib.reload(lm_bridge)
    finally:
        if saved is not None:
            sys.modules["astrbot_plugin_livingmemory"] = saved
        else:
            del sys.modules["astrbot_plugin_livingmemory"]
    assert reloaded._lm_get_active is None


def test_all_stars_fallback_matches_by_metadata():
    """get_registered_star misses, get_all_stars scan finds the active star."""
    lm_bridge._lm_get_active = None
    plugin = FakePlugin(FakeInitializer())
    star = FakeStar(star_cls=plugin, activated=True)
    star.name = "astrbot_plugin_livingmemory"
    ctx = DummyContext(star_by_alias={}, stars=[star])
    assert lm_bridge.detect_lm(ctx) is plugin
