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


def test_detect_lm_skips_disabled_dm_and_avoids_false_conflict(monkeypatch):
    """Disabled DM paired with active legacy LM returns legacy without conflict."""
    import pytest

    monkeypatch.setattr(lm_bridge, "_lm_get_active", None)
    dm_disabled = FakePlugin(FakeInitializer())
    dm_disabled.get_dynamics_capabilities = lambda: {
        "schema_version": 1,
        "provider_id": "astrbot_plugin_dynamics_memory",
        "enabled": False,
        "ready": False,
        "capabilities": {"memory": 1},
    }
    legacy = FakePlugin(FakeInitializer())

    dm_star = FakeStar(star_cls=dm_disabled, activated=True)
    dm_star.name = "astrbot_plugin_dynamics_memory"
    legacy_star = FakeStar(star_cls=legacy, activated=True)
    legacy_star.name = "astrbot_plugin_livingmemory"

    ctx = DummyContext(
        star_by_alias={
            "astrbot_plugin_dynamics_memory": dm_star,
            "astrbot_plugin_livingmemory": legacy_star,
        },
        stars=[dm_star, legacy_star],
    )
    assert lm_bridge.detect_lm(ctx) is legacy

    # Genuine dual-active conflict must still raise.
    dm_disabled.get_dynamics_capabilities = lambda: {
        "schema_version": 1,
        "provider_id": "astrbot_plugin_dynamics_memory",
        "enabled": True,
        "ready": True,
        "capabilities": {"memory": 1},
    }
    with pytest.raises(RuntimeError, match="冲突"):
        lm_bridge.detect_lm(ctx)


def test_facade_cache_refreshes_on_module_reload(monkeypatch):
    """Facade cache hits per module identity and refreshes when sys.modules changes."""
    lm_bridge._clear_facade_cache()
    calls = []
    mod1 = types.ModuleType("astrbot_plugin_dynamics_memory.api.public")
    mod1.ping = lambda: calls.append("m1") or "v1"
    monkeypatch.setitem(sys.modules, "astrbot_plugin_dynamics_memory.api.public", mod1)
    monkeypatch.delitem(
        sys.modules, "astrbot_plugin_livingmemory.api.public", raising=False
    )
    monkeypatch.setattr(
        lm_bridge, "_LM_FACADE_MODULES", ("astrbot_plugin_dynamics_memory.api.public",)
    )

    fn1 = lm_bridge._facade_fn("ping")
    assert fn1() == "v1"
    # Second resolution hits cache (same function object returned).
    assert lm_bridge._facade_fn("ping") is fn1

    # Replacing module in sys.modules invalidates cached entry automatically.
    mod2 = types.ModuleType("astrbot_plugin_dynamics_memory.api.public")
    mod2.ping = lambda: calls.append("m2") or "v2"
    monkeypatch.setitem(sys.modules, "astrbot_plugin_dynamics_memory.api.public", mod2)
    fn2 = lm_bridge._facade_fn("ping")
    assert fn2 is not fn1
    assert fn2() == "v2"
    assert calls == ["m1", "m2"]


def test_is_dm_capturing_group_respects_whitelist_and_readiness(monkeypatch):
    monkeypatch.setattr(lm_bridge, "_lm_get_active", None)
    cfg = {
        "session_manager.enable_full_group_capture": True,
        "access_control.whitelist_enabled": True,
        "access_control.allowed_ids": "1001, other:GroupMessage:999",
    }
    dm = FakePlugin(FakeInitializer())
    dm.get_dynamics_capabilities = lambda: {
        "schema_version": 1,
        "provider_id": "astrbot_plugin_dynamics_memory",
        "enabled": True,
        "ready": True,
        "capabilities": {"memory": 1},
    }
    dm.config_manager = types.SimpleNamespace(get=lambda k, d=None: cfg.get(k, d))
    dm_star = FakeStar(star_cls=dm, activated=True)
    dm_star.name = "astrbot_plugin_dynamics_memory"
    ctx = DummyContext(
        star_by_alias={"astrbot_plugin_dynamics_memory": dm_star}, stars=[dm_star]
    )

    allowed_ev = types.SimpleNamespace(
        unified_msg_origin="qq:GroupMessage:1001",
        get_group_id=lambda: "1001",
        get_sender_id=lambda: "42",
    )
    blocked_ev = types.SimpleNamespace(
        unified_msg_origin="qq:GroupMessage:2002",
        get_group_id=lambda: "2002",
        get_sender_id=lambda: "42",
    )
    assert lm_bridge.is_dm_capturing_group(ctx, allowed_ev) is True
    assert lm_bridge.is_dm_capturing_group(ctx, blocked_ev) is False

    cfg["session_manager.enable_full_group_capture"] = False
    assert lm_bridge.is_dm_capturing_group(ctx, allowed_ev) is False
