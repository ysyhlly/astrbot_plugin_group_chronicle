"""LivingMemory bridge — the ONLY module in this plugin that touches
``astrbot_plugin_livingmemory``.

Every other module must go through :func:`detect_lm` / :func:`ensure_ready` /
:func:`get_engine` / :func:`get_conversation_manager` /
:func:`lm_unavailable_reason`.

Hard constraints:

- Importable WITHOUT AstrBot installed: the only LivingMemory imports live in
  the guarded ``try/except`` blocks below; everything else uses
  ``getattr(..., None)`` so older LivingMemory versions degrade gracefully.
- Never reads LivingMemory DB files directly.

Note (B2): primary dependency is now LivingMemory's **public facade**
``astrbot_plugin_livingmemory.api.public``. The older private import of
``core.passive_group_capture.get_active_plugin`` and all local star-scanning
fallbacks are kept verbatim for LivingMemory versions that predate the facade.

Note (B6): facade **functions are resolved lazily at every call** (see
:func:`_facade_fn`) instead of being bound at import time, because
LivingMemory's ``reload_bootstrap.ensure_current`` removes
``astrbot_plugin_livingmemory.*`` submodules from ``sys.modules`` when its source
fingerprint changes. The ``messages`` SQL now lives in the facade
(``query_history_*``) and the local copies below are frozen legacy fallbacks.
"""

from __future__ import annotations

import importlib
import sys
import threading
import time
import weakref
from typing import Any

#: 公开门面模块列表（优先群间记忆 Dynamics Memory 公开门面，兼容旧版 LivingMemory 门面）。
_PRIMARY_FACADE_MODULE = "astrbot_plugin_dynamics_memory.api.public"
_LEGACY_FACADE_MODULE = "astrbot_plugin_livingmemory.api.public"
_LM_FACADE_MODULES: tuple[str, ...] = (
    _PRIMARY_FACADE_MODULE,
    _LEGACY_FACADE_MODULE,
)
_LM_FACADE_MODULE = _PRIMARY_FACADE_MODULE


def _load_public_facade_contract(mod_name: str):
    """Safely load (ALIASES, get_active_plugin) from a public facade module."""
    try:
        mod = importlib.import_module(mod_name)
    except Exception:
        return None, None
    aliases = getattr(mod, "ALIASES", None)
    getter = getattr(mod, "get_active_plugin", None)
    return aliases, (getter if callable(getter) else None)


def _load_legacy_lm_active_getter():
    """Isolated fallback for pre-facade LivingMemory private module; degrades silently."""
    try:
        mod = importlib.import_module(
            "astrbot_plugin_livingmemory.core.passive_group_capture"
        )
        getter = getattr(mod, "get_active_plugin", None)
        return getter if callable(getter) else None
    except Exception:
        return None


_DM_PUBLIC_ALIASES, _dm_public_get_active_plugin = _load_public_facade_contract(
    _PRIMARY_FACADE_MODULE
)
_LM_PUBLIC_ALIASES, _lm_public_get_active_plugin = _load_public_facade_contract(
    _LEGACY_FACADE_MODULE
)
_lm_legacy_get_active = _load_legacy_lm_active_getter()


def _combined_get_active():
    for getter in (
        _dm_public_get_active_plugin,
        _lm_public_get_active_plugin,
        _lm_legacy_get_active,
    ):
        if getter is not None:
            try:
                inst = getter()
                if inst is not None:
                    return inst
            except Exception:
                continue
    return None


#: 保留 _lm_get_active is None == "记忆包均不可导入" 的既有测试语义，且支持 monkeypatch。
_lm_get_active = (
    _combined_get_active
    if any(
        (
            _dm_public_get_active_plugin,
            _lm_public_get_active_plugin,
            _lm_legacy_get_active,
        )
    )
    else None
)

_merged_aliases: list[str] = []
for _src in (
    _DM_PUBLIC_ALIASES
    or ("astrbot_plugin_dynamics_memory", "DynamicsMemory", "群间记忆"),
    _LM_PUBLIC_ALIASES or ("LivingMemory", "astrbot_plugin_livingmemory"),
):
    for _a in _src:
        if _a not in _merged_aliases:
            _merged_aliases.append(_a)
_LM_ALIASES: tuple[str, ...] = tuple(_merged_aliases)


_FACADE_CACHE: dict[tuple[str, str], tuple[Any, Any]] = {}
_FACADE_MISSING: set[str] = set()
_DETECT_LM_TTL_SECONDS = 8.0
_DETECT_LM_CACHE_LOCK = threading.Lock()
# context_id -> (expires_at, ctx_ref, primary_mod, legacy_mod, get_active_fn, owner_or_none, disabled_rivals)
_DETECT_LM_CACHE: dict[int, tuple[float, Any, Any, Any, Any, Any, tuple[Any, ...]]] = {}
_REQUIRED_HISTORY_COLUMNS = frozenset(
    {"id", "role", "content", "sender_id", "sender_name", "timestamp", "metadata"}
)


def invalidate_lm_cache() -> None:
    """Clear the short-lived detect_lm() TTL cache (e.g. on plugin reload/terminate)."""
    with _DETECT_LM_CACHE_LOCK:
        _DETECT_LM_CACHE.clear()


def _clear_facade_cache() -> None:
    """Clear cached facade module/function references and detect_lm TTL cache."""
    _FACADE_CACHE.clear()
    _FACADE_MISSING.clear()
    invalidate_lm_cache()


def _import_facade_module(mod_name: str) -> Any | None:
    current = sys.modules.get(mod_name)
    if current is not None:
        _FACADE_MISSING.discard(mod_name)
        return current
    if mod_name in _FACADE_MISSING:
        return None
    try:
        module = importlib.import_module(mod_name)
    except Exception:
        _FACADE_MISSING.add(mod_name)
        return None
    return module


def _facade_fn(name: str):
    """按优先级惰性解析 Dynamics Memory / LivingMemory 门面函数并按模块对象版本缓存。"""
    fns = []
    for mod_name in _LM_FACADE_MODULES:
        module = _import_facade_module(mod_name)
        if module is None:
            continue
        key = (mod_name, name)
        cached = _FACADE_CACHE.get(key)
        if cached is not None and cached[0] is module:
            fn = cached[1]
        else:
            raw_fn = getattr(module, name, None)
            fn = raw_fn if callable(raw_fn) else None
            _FACADE_CACHE[key] = (module, fn)
        if fn is not None:
            fns.append(fn)
    if not fns:
        return None
    if len(fns) == 1:
        return fns[0]

    def _dispatch(*args, **kwargs):
        last = None
        for fn in fns:
            try:
                res = fn(*args, **kwargs)
                if res:
                    return res
                last = res
            except Exception:
                continue
        return last

    return _dispatch


def _facade_find_plugin(context: Any) -> Any | None:
    """经门面解析插件实例；门面不可用/调用失败时 None（由本地回退接手）。"""
    fn = _facade_fn("find_plugin")
    if fn is None:
        return None
    try:
        return fn(context)
    except Exception:
        return None


def _is_active_star(star: Any) -> bool:
    """A registered star counts as active when not explicitly deactivated and
    it exposes a plugin class instance."""
    if not star:
        return False
    if getattr(star, "activated", True) is False:
        return False
    return getattr(star, "star_cls", None) is not None


def _is_enabled_owner(owner: Any, *, strict_caps: bool = False) -> bool:
    """Skip terminating, series-disabled, or capability-disabled memory instances."""
    if owner is None:
        return False
    if (
        getattr(owner, "_terminating", False) is True
        or getattr(owner, "closed", False) is True
    ):
        return False
    if getattr(owner, "series_runtime_enabled", None) is False:
        return False
    if getattr(owner, "_series_enabled", None) is False:
        return False
    cfg = getattr(owner, "config", None)
    if isinstance(cfg, dict) and cfg.get("series_runtime_enabled") is False:
        return False
    caps = getattr(owner, "get_dynamics_capabilities", None)
    if callable(caps):
        try:
            result = caps()
        except Exception:
            if strict_caps:
                raise
            return False
        if isinstance(result, dict):
            if result.get("enabled") is False:
                return False
            inner = result.get("capabilities")
            if isinstance(inner, dict) and inner.get("enabled") is False:
                return False
    return True


def _detect_legacy_lm(context: Any, *, prefetched_stars: Any = None) -> Any | None:
    """Return the LivingMemory plugin instance (``star.star_cls``), or None.

    FIXED resolution order:

    1. PRIMARY — ``_lm_get_active()`` weakref published by LivingMemory
       itself; returns the LivingMemoryPlugin instance directly. (Tests drive
       this path by monkeypatching ``_lm_get_active``, so it stays first.)
    2. FACADE — LivingMemory's ``api.public.find_plugin(context)``, resolved
       lazily at call time (B6); it re-implements steps 1/3/4 inside the plugin
       that owns them.
    3. FALLBACK — ``context.get_registered_star`` under the aliases
       ``LivingMemory`` / ``astrbot_plugin_livingmemory``; then a scan of
       ``context.get_all_stars()`` matching star metadata against the same
       aliases. The plugin instance (``star.star_cls``) is returned, NOT the
       star meta object.

    Steps 3–4 are kept verbatim as a compatibility fallback and still run when
    the facade is unavailable or returns nothing.
    """
    if _lm_get_active is not None:
        try:
            plugin = _lm_get_active()
        except Exception:
            plugin = None
        if plugin is not None and _is_enabled_owner(plugin):
            return plugin

    plugin = _facade_find_plugin(context)
    if plugin is not None and _is_enabled_owner(plugin):
        return plugin

    getter = getattr(context, "get_registered_star", None)
    if callable(getter):
        for alias in _LM_ALIASES:
            try:
                star = getter(alias)
            except Exception:
                star = None
            if _is_active_star(star):
                owner = getattr(star, "star_cls", None)
                if _is_enabled_owner(owner):
                    return owner

    if prefetched_stars is not None:
        stars = prefetched_stars
    else:
        all_stars_getter = getattr(context, "get_all_stars", None)
        if not callable(all_stars_getter):
            return None
        try:
            stars = all_stars_getter() or []
        except Exception:
            return None

    wanted = {alias.lower() for alias in _LM_ALIASES}
    for star in stars:
        if not _is_active_star(star):
            continue
        candidates = {
            getattr(star, "name", None),
            getattr(star, "display_name", None),
            getattr(star, "root_dir_name", None),
            getattr(star, "module_path", None),
        }
        normalized = {
            str(candidate).strip().lower()
            for candidate in candidates
            if str(candidate or "").strip()
        }
        if normalized & wanted:
            owner = getattr(star, "star_cls", None)
            if _is_enabled_owner(owner):
                return owner
    return None


def _ctx_matches_ref(ctx_ref: Any, context: Any) -> bool:
    if isinstance(ctx_ref, weakref.ReferenceType):
        return ctx_ref() is context
    return ctx_ref is context


def detect_lm(context: Any) -> Any | None:
    """Resolve one active, enabled memory owner; prefer Dynamics Memory public facade.

    Uses a short TTL cache (invalidated on reload, owner state change, or rival
    activation) so per-message filters like ``HistoryCaptureFilter`` avoid scanning
    ``get_all_stars()`` on every group message.

    Never silently combines two active memory stores (raises RuntimeError on conflict).
    """
    ctx_id = id(context)
    now = time.monotonic()
    primary_mod_now = sys.modules.get(_PRIMARY_FACADE_MODULE)
    legacy_mod_now = sys.modules.get(_LEGACY_FACADE_MODULE)
    active_fn_now = _lm_get_active
    with _DETECT_LM_CACHE_LOCK:
        cached = _DETECT_LM_CACHE.get(ctx_id)
    if cached is not None:
        exp, ctx_ref, p_mod, l_mod, a_fn, cached_owner, disabled_rivals = cached
        if (
            now < exp
            and _ctx_matches_ref(ctx_ref, context)
            and p_mod is primary_mod_now
            and l_mod is legacy_mod_now
            and a_fn is active_fn_now
        ):
            owner_ok = (
                cached_owner is None or _is_enabled_owner(cached_owner)
            )
            rivals_ok = not any(
                _is_enabled_owner(rival) for rival in disabled_rivals
            )
            if owner_ok and rivals_ok:
                return cached_owner
        with _DETECT_LM_CACHE_LOCK:
            _DETECT_LM_CACHE.pop(ctx_id, None)

    owners: list[Any] = []
    disabled_rivals_list: list[Any] = []

    def _record_disabled(candidate: Any) -> None:
        if candidate is not None and all(candidate is not r for r in disabled_rivals_list):
            disabled_rivals_list.append(candidate)

    # 1. Prefer Dynamics Memory public facade directly when available
    primary_mod = _import_facade_module(_PRIMARY_FACADE_MODULE)
    primary_find = getattr(primary_mod, "find_plugin", None) if primary_mod is not None else None
    if callable(primary_find):
        try:
            dm_owner = primary_find(context)
        except Exception:
            dm_owner = None
        if dm_owner is not None:
            if _is_enabled_owner(dm_owner):
                owners.append(dm_owner)
            else:
                _record_disabled(dm_owner)

    getter = getattr(context, "get_registered_star", None)
    if callable(getter):
        for alias in (
            "astrbot_plugin_dynamics_memory",
            "astrbot_plugin_livingmemory",
            "LivingMemory",
        ):
            try:
                star = getter(alias)
            except Exception:
                continue
            if _is_active_star(star):
                owner = star.star_cls
                if _is_enabled_owner(owner):
                    if all(owner is not p for p in owners):
                        owners.append(owner)
                else:
                    _record_disabled(owner)
    scanned_stars: Any = None
    scanner = getattr(context, "get_all_stars", None)
    if callable(scanner):
        try:
            stars = scanner() or []
            stars = list(stars.values()) if isinstance(stars, dict) else list(stars)
            scanned_stars = stars
            for star in stars:
                if not _is_active_star(star):
                    continue
                owner = star.star_cls
                caps = getattr(owner, "get_dynamics_capabilities", None)
                if callable(caps):
                    result = caps()
                    capabilities = (
                        result.get("capabilities") if isinstance(result, dict) else None
                    )
                    if (
                        isinstance(capabilities, dict)
                        and capabilities.get("memory") == 1
                    ):
                        if _is_enabled_owner(owner, strict_caps=True):
                            if all(owner is not p for p in owners):
                                owners.append(owner)
                        else:
                            _record_disabled(owner)
        except Exception:
            # A broken registry cannot safely establish unique ownership.
            raise RuntimeError("记忆能力发现失败，已暂停读取") from None
    legacy = _detect_legacy_lm(context, prefetched_stars=scanned_stars)
    if (
        legacy is not None
        and _is_enabled_owner(legacy)
        and all(legacy is not p for p in owners)
    ):
        owners.append(legacy)
    if len(owners) > 1:
        raise RuntimeError("新旧记忆来源冲突，请仅启用一个记忆提供者")
    resolved = owners[0] if owners else None
    try:
        ctx_ref: Any = weakref.ref(context)
    except TypeError:
        ctx_ref = context
    with _DETECT_LM_CACHE_LOCK:
        if len(_DETECT_LM_CACHE) >= 16:
            _DETECT_LM_CACHE.clear()
        _DETECT_LM_CACHE[ctx_id] = (
            now + _DETECT_LM_TTL_SECONDS,
            ctx_ref,
            sys.modules.get(_PRIMARY_FACADE_MODULE),
            sys.modules.get(_LEGACY_FACADE_MODULE),
            _lm_get_active,
            resolved,
            tuple(disabled_rivals_list),
        )
    return resolved


def source_identity(context):
    """Stable provider/store identity; a provider change must not reuse cursors."""
    import hashlib

    plugin = detect_lm(context)
    if plugin is None:
        raise RuntimeError("记忆来源不可用")
    public_id = getattr(plugin, "get_history_source_identity", None)
    if callable(public_id):
        try:
            ident = public_id()
            if isinstance(ident, str) and ident:
                return ident
        except Exception:
            pass
    manager = get_conversation_manager(plugin)
    store = getattr(manager, "store", None)
    path = str(getattr(store, "db_path", "") or getattr(store, "path", ""))
    return hashlib.sha256(
        (
            type(plugin).__module__ + ":" + type(plugin).__qualname__ + ":" + path
        ).encode()
    ).hexdigest()


def _check_history_schema() -> None:
    """Verify facade-advertised history schema/columns when a facade module is loaded."""
    for mod_name in _LM_FACADE_MODULES:
        module = sys.modules.get(mod_name)
        if module is None:
            continue
        cols = getattr(module, "HISTORY_COLUMNS", None)
        if cols is not None and not _REQUIRED_HISTORY_COLUMNS.issubset(set(cols)):
            raise RuntimeError("记忆来源表结构不兼容，已暂停读取")


async def history_sources(context, umo, ids):
    """Re-read exact source IDs from the active connection, scoped to one group."""
    ids = sorted({int(i) for i in ids})
    if not ids:
        return {}
    if len(ids) > 500:
        raise ValueError("来源数量超过单次校验上限")
    connection = history_connection(context)
    if connection is None:
        raise RuntimeError("记忆来源不可用")
    facade_sources = _facade_fn("query_history_sources")
    if callable(facade_sources):
        try:
            data = await facade_sources(connection, umo, ids)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    _check_history_schema()
    placeholders = ",".join("?" for _ in ids)
    import json

    async with connection.execute(
        f"SELECT id,role,content,sender_id,sender_name,timestamp,metadata FROM messages WHERE session_id=? AND id IN ({placeholders})",
        (umo, *ids),
    ) as cursor:
        rows = await cursor.fetchall()
    result = {}
    for r in rows:
        try:
            meta = json.loads(r[6]) if isinstance(r[6], str) else r[6]
        except (ValueError, TypeError):
            meta = {}
        result[str(r[0])] = {
            "id": r[0],
            "role": r[1],
            "content": r[2] or "",
            "uid": str(r[3] or ""),
            "name": r[4] or "",
            "ts": r[5],
            "metadata": meta if isinstance(meta, dict) else {},
        }
    return result


# ------------------------------------------------------------------
# Isolated legacy LivingMemory private-attribute fallbacks (silent degradation)
# ------------------------------------------------------------------


async def _legacy_ensure_ready(plugin: Any) -> bool:
    """Private ``initializer`` fallback for pre-facade LivingMemory; never raises."""
    try:
        initializer = getattr(plugin, "initializer", None)
        if initializer is None:
            return False
        if not getattr(initializer, "is_initialized", False):
            ensure = getattr(initializer, "ensure_initialized", None)
            if not callable(ensure):
                return False
            if not await ensure(timeout=30.0):
                return False
        return (
            getattr(initializer, "memory_engine", None) is not None
            and getattr(initializer, "conversation_manager", None) is not None
        )
    except Exception:
        return False


def _legacy_get_engine(plugin: Any) -> Any | None:
    try:
        initializer = getattr(plugin, "initializer", None)
        return getattr(initializer, "memory_engine", None) if initializer is not None else None
    except Exception:
        return None


def _legacy_get_conversation_manager(plugin: Any) -> Any | None:
    try:
        initializer = getattr(plugin, "initializer", None)
        return (
            getattr(initializer, "conversation_manager", None)
            if initializer is not None
            else None
        )
    except Exception:
        return None


def _legacy_history_connection(plugin: Any) -> Any | None:
    """Private ``conversation_manager.store.connection`` fallback for older LivingMemory."""
    try:
        manager = get_conversation_manager(plugin)
        store = getattr(manager, "store", None)
        connection = getattr(store, "connection", None)
        return connection if callable(getattr(connection, "execute", None)) else None
    except Exception:
        return None


def _legacy_check_dm_capture_config(plugin: Any, event: Any) -> bool:
    """Private ``config_manager`` fallback when Dynamics Memory lacks a public capture probe."""
    cfg = getattr(plugin, "config_manager", None)
    getter = getattr(cfg, "get", None)
    if not callable(getter):
        return True
    try:
        if getter("session_manager.enable_full_group_capture", True) is False:
            return False
        if getter("access_control.whitelist_enabled", False):
            raw_allowed = getter("access_control.allowed_ids", "")
            if isinstance(raw_allowed, str):
                items = [
                    p.strip().casefold()
                    for part in raw_allowed.replace("\n", ",").split(",")
                    if (p := part.strip())
                ]
            elif isinstance(raw_allowed, (list, tuple, set)):
                items = [
                    str(v).strip().casefold() for v in raw_allowed if str(v).strip()
                ]
            else:
                items = []
            allowed = set(items)
            if not allowed:
                return False
            umo = str(getattr(event, "unified_msg_origin", "") or "").casefold()
            gid = ""
            group_getter = getattr(event, "get_group_id", None)
            if callable(group_getter):
                try:
                    gid = str(group_getter() or "").casefold()
                except Exception:
                    gid = ""
            sid = ""
            sender_getter = getattr(event, "get_sender_id", None)
            if callable(sender_getter):
                try:
                    sid = str(sender_getter() or "").casefold()
                except Exception:
                    sid = ""
            if not ({umo, gid, sid} & allowed):
                return False
    except Exception:
        return False
    return True


async def ensure_ready(plugin: Any) -> bool:
    """Ensure the Dynamics Memory / LivingMemory plugin instance is initialized and usable.

    Prefers the public facade ``api.public.ensure_ready(plugin, timeout=30.0,
    require=("memory_engine", "conversation_manager"))``; falls back to isolated
    legacy probing with silent degradation.
    """
    facade_ensure_ready = _facade_fn("ensure_ready")
    if facade_ensure_ready is not None:
        try:
            return bool(
                await facade_ensure_ready(
                    plugin,
                    timeout=30.0,
                    require=("memory_engine", "conversation_manager"),
                )
            )
        except Exception:
            return False
    return await _legacy_ensure_ready(plugin)


def get_engine(plugin: Any) -> Any | None:
    """Return the memory engine via public facade, or isolated legacy fallback."""
    facade_get_memory_engine = _facade_fn("get_memory_engine")
    if facade_get_memory_engine is not None:
        try:
            return facade_get_memory_engine(plugin)
        except Exception:
            return None
    return _legacy_get_engine(plugin)


def get_conversation_manager(plugin: Any) -> Any | None:
    """Return the conversation manager via public facade, or isolated legacy fallback."""
    facade_get_conversation_manager = _facade_fn("get_conversation_manager")
    if facade_get_conversation_manager is not None:
        try:
            return facade_get_conversation_manager(plugin)
        except Exception:
            return None
    return _legacy_get_conversation_manager(plugin)


def lm_unavailable_reason(context: Any = None, stage: str | None = None) -> str:
    """Most specific reason LivingMemory is unavailable.

    ``stage`` may be one of ``"not_loaded"`` / ``"not_detected"`` /
    ``"not_ready"`` to report a known failure stage. When omitted, the reason
    is derived from the current state:

    - module-level import failed        -> "LivingMemory 插件未加载"
    - no plugin instance found          -> "未检测到 LivingMemory"
    - plugin found but not initialized  -> "LivingMemory 尚未初始化"

    Priority: 插件未加载 > 尚未初始化 > 未检测到 (module-level absence wins).
    """
    if _lm_get_active is None or stage == "not_loaded":
        return "LivingMemory 插件未加载"
    if stage == "not_ready":
        return "LivingMemory 尚未初始化"
    if stage == "not_detected":
        return "未检测到 LivingMemory"
    plugin = None
    try:
        plugin = _lm_get_active()
    except Exception:
        plugin = None
    if plugin is None and context is not None:
        try:
            plugin = detect_lm(context)
        except Exception:
            plugin = None
    if plugin is None:
        return "未检测到 LivingMemory"
    return "LivingMemory 尚未初始化"


def history_connection(context):
    """Resolve the active read-only history query surface.

    Prefers public ``plugin.get_history_reader()`` and facade
    ``api.public.get_history_reader(context)`` when the facade resolves the same
    owner as :func:`detect_lm`. Falls back to the isolated legacy store helper
    only when no public reader matches.
    """
    plugin = detect_lm(context)
    if plugin is None:
        return None
    capability = getattr(plugin, "get_dynamics_capabilities", None)
    if callable(capability):
        manifest = capability()
        if (
            not isinstance(manifest, dict)
            or manifest.get("ready") is not True
            or manifest.get("enabled") is False
        ):
            return None

    direct_reader = getattr(plugin, "get_history_reader", None)
    if callable(direct_reader):
        try:
            reader = direct_reader()
            if callable(getattr(reader, "execute", None)):
                return reader
        except Exception:
            pass

    facade_get_history_reader = _facade_fn("get_history_reader")
    if callable(facade_get_history_reader) and _facade_find_plugin(context) is plugin:
        try:
            reader = facade_get_history_reader(context)
            if callable(getattr(reader, "execute", None)):
                return reader
        except Exception:
            pass

    return _legacy_history_connection(plugin)


def is_dm_capturing_group(context: Any, event: Any) -> bool:
    """Return True when active, ready Dynamics Memory already captures this group message."""
    try:
        plugin = detect_lm(context)
    except Exception:
        return False
    if plugin is None:
        return False
    capability = getattr(plugin, "get_dynamics_capabilities", None)
    if not callable(capability):
        return False
    try:
        manifest = capability()
    except Exception:
        return False
    if (
        not isinstance(manifest, dict)
        or manifest.get("provider_id") != "astrbot_plugin_dynamics_memory"
        or manifest.get("enabled") is not True
        or manifest.get("ready") is not True
    ):
        return False

    # Prefer public capture probe on plugin or Dynamics Memory public facade
    public_probe = getattr(plugin, "is_capturing_group", None)
    if callable(public_probe):
        try:
            return bool(public_probe(event))
        except Exception:
            return False
    facade_probe = _facade_fn("is_capturing_group")
    if callable(facade_probe):
        try:
            return bool(facade_probe(plugin, event))
        except Exception:
            pass

    return _legacy_check_dm_capture_config(plugin, event)


async def history_sessions(context):
    """Discover group history without modifying LivingMemory counters.

    B6: SQL 由门面 ``query_history_sessions`` 提供（``messages`` 表名/列名归
    LivingMemory）；门面不可用时退回下面的 legacy 副本（旧版本 LM）。

    Args:
        context: AstrBot context.

    Returns:
        Existing group session identifiers.
    """
    connection = history_connection(context)
    if connection is None:
        return []
    facade_query = _facade_fn("query_history_sessions")
    if facade_query is not None:
        try:
            return await facade_query(connection)
        except Exception:
            pass  # 退回 legacy 副本；若 schema 真的坏了，下面会原样抛错
    _check_history_schema()
    async with connection.execute(
        "SELECT DISTINCT session_id FROM messages WHERE session_id LIKE ?",
        ("%:GroupMessage:%",),
    ) as cursor:
        return [str(row[0]) for row in await cursor.fetchall()]


async def history_bounds(context, umo):
    """Freeze stable message-ID bounds through SELECT-only queries.

    B6: 优先走门面 ``query_history_bounds``；不可用时退回 legacy 副本。

    Args:
        context: AstrBot context.
        umo: Group session identifier.

    Returns:
        Count, highest ID, and earliest/latest timestamps.

    Raises:
        RuntimeError: The active source is unavailable.
    """
    connection = history_connection(context)
    if connection is None:
        raise RuntimeError("LivingMemory 历史连接尚未就绪")
    facade_query = _facade_fn("query_history_bounds")
    if facade_query is not None:
        try:
            return await facade_query(connection, umo)
        except Exception:
            pass  # 退回 legacy 副本
    _check_history_schema()
    async with connection.execute(
        "SELECT COUNT(*),COALESCE(MAX(id),0),MIN(timestamp),MAX(timestamp) FROM messages WHERE session_id=?",
        (umo,),
    ) as cursor:
        row = await cursor.fetchone()
    return {"count": int(row[0]), "high": int(row[1]), "first": row[2], "last": row[3]}


async def history_page(context, umo, after, high, limit):
    """Read an ID-keyset page from the active store, never opening its DB file.

    Args:
        context: AstrBot context.
        umo: Group session identifier.
        after: Exclusive processed ID.
        high: Inclusive snapshot watermark.
        limit: Maximum rows.

    Returns:
        Source messages with stable IDs and decoded metadata.

    Raises:
        RuntimeError: The source connection is unavailable.

    B6: 表名、列名、metadata 解码与时间戳强制转换都由门面
    ``query_history_page`` 负责；不可用时退回下面的 legacy 副本（旧版本 LM），
    返回值结构保持不变。
    """
    import json

    connection = history_connection(context)
    if connection is None:
        raise RuntimeError("LivingMemory 历史连接尚未就绪")
    facade_query = _facade_fn("query_history_page")
    if facade_query is not None:
        try:
            return await facade_query(connection, umo, after, high, limit)
        except Exception:
            pass  # 退回 legacy 副本
    _check_history_schema()
    async with connection.execute(
        "SELECT id,role,content,sender_id,sender_name,timestamp,metadata FROM messages "
        "WHERE session_id=? AND id>? AND id<=? ORDER BY id ASC LIMIT ?",
        (umo, int(after), int(high), max(1, int(limit))),
    ) as cursor:
        rows = await cursor.fetchall()
    result = []
    for row in rows:
        try:
            metadata = json.loads(row[6]) if isinstance(row[6], str) else row[6]
        except (ValueError, TypeError):
            metadata = {}
        try:
            timestamp = float(row[5] or 0)
        except (ValueError, TypeError, OverflowError):
            timestamp = 0
        result.append(
            {
                "id": int(row[0]),
                "role": str(row[1]),
                "content": str(row[2] or ""),
                "uid": str(row[3] or ""),
                "name": str(row[4] or row[3] or ""),
                "ts": timestamp,
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )
    return result


async def query_memory_highlights(
    context: Any,
    umo: str,
    member_id: str = "",
    *,
    since_ts: float = 0.0,
    limit: int = 8,
    max_msgs: int = 600,
) -> dict[str, Any]:
    """Read-only, gracefully-degrading memory & co-occurrence summary for annual reports.

    Uses only public LM/Dynamics Memory hooks via detect_lm/ensure_ready/get_engine/
    get_conversation_manager; never touches disk DB files directly and never raises.
    """
    empty: dict[str, Any] = {
        "available": False,
        "topics": [],
        "co_members": [],
        "message_count": 0,
        "active_days": 0,
        "sample_quotes": [],
    }
    try:
        plugin = detect_lm(context)
        if plugin is None or not await ensure_ready(plugin):
            return empty
        engine = get_engine(plugin)
        conv_manager = get_conversation_manager(plugin)
        topics: list[str] = []
        seen_topics: set[str] = set()

        # 1. Read memory summaries / episodic topics safely
        if engine is not None:
            raw_mems: Any = None
            for method_name, kwargs_list in (
                (
                    "get_recent_memories",
                    [
                        {"session_id": umo, "limit": max(limit * 2, 16)},
                        {"umo": umo, "limit": max(limit * 2, 16)},
                        {"limit": max(limit * 2, 16)},
                    ],
                ),
                (
                    "retrieve",
                    [
                        {"query": str(member_id or "群聊回顾"), "session_id": umo, "top_k": limit},
                        {"query": str(member_id or "群聊回顾"), "umo": umo, "limit": limit},
                    ],
                ),
            ):
                fn = getattr(engine, method_name, None)
                if not callable(fn):
                    continue
                for kw in kwargs_list:
                    try:
                        res = fn(**kw)
                        if hasattr(res, "__await__"):
                            res = await res
                        if res:
                            raw_mems = res
                            break
                    except TypeError:
                        continue
                    except Exception:
                        break
                if raw_mems:
                    break

            mem_list = []
            if isinstance(raw_mems, dict):
                for k in ("memories", "items", "results", "episodes"):
                    if isinstance(raw_mems.get(k), list):
                        mem_list = raw_mems[k]
                        break
            elif isinstance(raw_mems, (list, tuple)):
                mem_list = list(raw_mems)

            for m in mem_list:
                if isinstance(m, dict):
                    txt = str(
                        m.get("summary")
                        or m.get("text")
                        or m.get("content")
                        or m.get("topic")
                        or ""
                    ).strip()
                    ts = float(m.get("timestamp") or m.get("created_at") or 0.0)
                else:
                    txt = str(
                        getattr(m, "summary", None)
                        or getattr(m, "text", None)
                        or getattr(m, "content", None)
                        or ""
                    ).strip()
                    ts = float(
                        getattr(m, "timestamp", 0.0)
                        or getattr(m, "created_at", 0.0)
                        or 0.0
                    )
                if not txt or (since_ts > 0 and ts > 0 and ts < since_ts):
                    continue
                clean_txt = " ".join(txt.split())[:120]
                if clean_txt and clean_txt not in seen_topics:
                    seen_topics.add(clean_txt)
                    topics.append(clean_txt)
                if len(topics) >= limit:
                    break

        # 2. Read recent messages for activity stats, co-occurring peers, and quote samples
        msgs: list[Any] = []
        if conv_manager is not None:
            for method_name in ("get_recent_messages", "get_messages", "fetch_messages"):
                fn = getattr(conv_manager, method_name, None)
                if not callable(fn):
                    continue
                for args, kw in (
                    ((umo,), {"limit": max_msgs}),
                    ((), {"session_id": umo, "limit": max_msgs}),
                    ((umo, max_msgs), {}),
                ):
                    try:
                        res = fn(*args, **kw)
                        if hasattr(res, "__await__"):
                            res = await res
                        if isinstance(res, list):
                            msgs = res
                            break
                    except TypeError:
                        continue
                    except Exception:
                        break
                if msgs:
                    break

        from collections import Counter
        from datetime import date

        target_uid = str(member_id or "").strip()
        msg_count = 0
        days_set: set[Any] = set()
        co_counts: Counter[str] = Counter()
        names_map: dict[str, str] = {}
        quote_samples: list[dict[str, str]] = []
        window_rows: list[tuple[str, str, float, str]] = []

        for m in msgs:
            if isinstance(m, dict):
                role = str(m.get("role") or "user")
                uid = str(m.get("sender_id") or m.get("uid") or "").strip()
                name = str(m.get("sender_name") or m.get("name") or uid).strip()
                ts = float(m.get("timestamp") or m.get("ts") or 0.0)
                content = str(m.get("content") or "").strip()
            else:
                role = str(getattr(m, "role", "user") or "user")
                uid = str(getattr(m, "sender_id", "") or "").strip()
                name = str(getattr(m, "sender_name", "") or uid).strip()
                ts = float(getattr(m, "timestamp", 0.0) or 0.0)
                content = str(getattr(m, "content", "") or "").strip()
            if role != "user" or not uid:
                continue
            if since_ts > 0 and ts > 0 and ts < since_ts:
                continue
            if name:
                names_map[uid] = name
            window_rows.append((uid, name, ts, content))

        for idx, (uid, _name, ts, content) in enumerate(window_rows):
            if target_uid and uid != target_uid:
                continue
            msg_count += 1
            if ts > 0:
                try:
                    days_set.add(date.fromtimestamp(ts))
                except Exception:
                    pass
            if content:
                quote_samples.append({"content": content})
            if target_uid:
                # Neighboring messages within 5 minutes indicate direct conversational co-occurrence
                for j in range(max(0, idx - 3), min(len(window_rows), idx + 4)):
                    other_uid, _other_name, other_ts, _ = window_rows[j]
                    if other_uid and other_uid != target_uid:
                        if ts <= 0 or other_ts <= 0 or abs(other_ts - ts) <= 300:
                            co_counts[other_uid] += 1
            else:
                co_counts[uid] += 1

        co_members = [
            {"uid": uid, "name": names_map.get(uid, uid), "interactions": int(cnt)}
            for uid, cnt in co_counts.most_common(10)
        ]
        return {
            "available": True,
            "topics": topics[:limit],
            "co_members": co_members,
            "message_count": msg_count,
            "active_days": len(days_set),
            "sample_quotes": quote_samples[:200],
        }
    except Exception:
        return empty
