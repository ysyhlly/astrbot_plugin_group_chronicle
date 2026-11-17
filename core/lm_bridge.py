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
from typing import Any

#: 门面模块列表（优先群间记忆 Dynamics Memory，兼容旧版 LivingMemory）。
_LM_FACADE_MODULES: tuple[str, ...] = (
    "astrbot_plugin_dynamics_memory.api.public",
    "astrbot_plugin_livingmemory.api.public",
)
_LM_FACADE_MODULE = _LM_FACADE_MODULES[0]

try:
    from astrbot_plugin_dynamics_memory.api.public import (
        ALIASES as _DM_PUBLIC_ALIASES,
        get_active_plugin as _dm_public_get_active_plugin,
    )
except Exception:
    _DM_PUBLIC_ALIASES = None
    _dm_public_get_active_plugin = None

try:
    from astrbot_plugin_livingmemory.api.public import (
        ALIASES as _LM_PUBLIC_ALIASES,
        get_active_plugin as _lm_public_get_active_plugin,
    )
except Exception:
    _LM_PUBLIC_ALIASES = None
    _lm_public_get_active_plugin = None

try:
    from astrbot_plugin_livingmemory.core.passive_group_capture import (
        get_active_plugin as _lm_legacy_get_active,
    )
except Exception:
    _lm_legacy_get_active = None


def _combined_get_active():
    for getter in (_dm_public_get_active_plugin, _lm_public_get_active_plugin, _lm_legacy_get_active):
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
    if any((_dm_public_get_active_plugin, _lm_public_get_active_plugin, _lm_legacy_get_active))
    else None
)

_merged_aliases: list[str] = []
for _src in (
    _DM_PUBLIC_ALIASES or ("astrbot_plugin_dynamics_memory", "DynamicsMemory", "群间记忆"),
    _LM_PUBLIC_ALIASES or ("LivingMemory", "astrbot_plugin_livingmemory"),
):
    for _a in _src:
        if _a not in _merged_aliases:
            _merged_aliases.append(_a)
_LM_ALIASES: tuple[str, ...] = tuple(_merged_aliases)


def _facade_fn(name: str):
    """按优先级惰性解析 Dynamics Memory / LivingMemory 门面函数。"""
    fns = []
    for mod_name in _LM_FACADE_MODULES:
        try:
            module = importlib.import_module(mod_name)
            fn = getattr(module, name, None)
            if callable(fn):
                fns.append(fn)
        except Exception:
            continue
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


def _detect_legacy_lm(context: Any) -> Any | None:
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
        if plugin is not None:
            return plugin

    plugin = _facade_find_plugin(context)
    if plugin is not None:
        return plugin

    getter = getattr(context, "get_registered_star", None)
    if callable(getter):
        for alias in _LM_ALIASES:
            try:
                star = getter(alias)
            except Exception:
                star = None
            if _is_active_star(star):
                return getattr(star, "star_cls", None)

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
            return getattr(star, "star_cls", None)
    return None


def detect_lm(context: Any) -> Any | None:
    """Resolve one active memory owner; never silently combine two stores."""
    owners = []
    getter = getattr(context, "get_registered_star", None)
    if callable(getter):
        for alias in ("astrbot_plugin_dynamics_memory", "astrbot_plugin_livingmemory", "LivingMemory"):
            try:
                star = getter(alias)
            except Exception:
                continue
            if _is_active_star(star):
                owner = star.star_cls
                if all(owner is not p for p in owners):
                    owners.append(owner)
    scanner = getattr(context, "get_all_stars", None)
    if callable(scanner):
        try:
            stars = scanner() or []
            stars = stars.values() if isinstance(stars, dict) else stars
            for star in stars:
                if not _is_active_star(star):
                    continue
                owner = star.star_cls
                caps = getattr(owner, "get_dynamics_capabilities", None)
                if callable(caps):
                    result = caps()
                    capabilities = result.get("capabilities") if isinstance(result, dict) else None
                    if isinstance(capabilities, dict) and capabilities.get("memory") == 1:
                        if all(owner is not p for p in owners):
                            owners.append(owner)
        except Exception:
            # A broken registry cannot safely establish unique ownership.
            raise RuntimeError("记忆能力发现失败，已暂停读取") from None
    legacy = _detect_legacy_lm(context)
    if legacy is not None and all(legacy is not p for p in owners):
        owners.append(legacy)
    if len(owners) > 1:
        raise RuntimeError("新旧记忆来源冲突，请仅启用一个记忆提供者")
    return owners[0] if owners else None


def source_identity(context):
    """Stable provider/store identity; a provider change must not reuse cursors."""
    import hashlib
    plugin = detect_lm(context)
    if plugin is None:
        raise RuntimeError("记忆来源不可用")
    manager = get_conversation_manager(plugin)
    store = getattr(manager, "store", None)
    path = str(getattr(store, "db_path", "") or getattr(store, "path", ""))
    return hashlib.sha256((type(plugin).__module__ + ":" + type(plugin).__qualname__ + ":" + path).encode()).hexdigest()


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
        result[str(r[0])] = dict(id=r[0], role=r[1], content=r[2] or "", uid=str(r[3] or ""),
                                  name=r[4] or "", ts=r[5], metadata=meta if isinstance(meta, dict) else {})
    return result


async def ensure_ready(plugin: Any) -> bool:
    """Ensure the LivingMemory plugin instance is initialized and usable.

    Prefers the facade ``api.public.ensure_ready(plugin, timeout=30.0,
    require=("memory_engine", "conversation_manager"))``, which implements the
    same policy inside LivingMemory; falls back to the local implementation for
    versions without the facade. Both paths pull ``initializer`` off the plugin
    instance, wait for ``ensure_initialized(timeout=30.0)`` when not yet
    initialized, then check that both ``memory_engine`` and
    ``conversation_manager`` are present. Every attribute access is guarded.

    Behaviour is unchanged: chronicle needs the conversation manager, so the
    ``require`` set stays explicit.
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

    initializer = getattr(plugin, "initializer", None)
    if initializer is None:
        return False
    if not getattr(initializer, "is_initialized", False):
        ensure = getattr(initializer, "ensure_initialized", None)
        if not callable(ensure):
            return False
        try:
            ok = await ensure(timeout=30.0)
        except Exception:
            ok = False
        if not ok:
            return False
    return (
        getattr(initializer, "memory_engine", None) is not None
        and getattr(initializer, "conversation_manager", None) is not None
    )


def get_engine(plugin: Any) -> Any | None:
    """Return the LivingMemory memory engine, or None if unavailable.

    Prefers the facade accessor ``api.public.get_memory_engine(plugin)`` so the
    private ``initializer.memory_engine`` chain is no longer duplicated here.
    """
    facade_get_memory_engine = _facade_fn("get_memory_engine")
    if facade_get_memory_engine is not None:
        try:
            return facade_get_memory_engine(plugin)
        except Exception:
            return None

    initializer = getattr(plugin, "initializer", None)
    if initializer is None:
        return None
    return getattr(initializer, "memory_engine", None)


def get_conversation_manager(plugin: Any) -> Any | None:
    """Return the LivingMemory conversation manager, or None.

    Prefers the facade accessor ``api.public.get_conversation_manager(plugin)``.
    """
    facade_get_conversation_manager = _facade_fn("get_conversation_manager")
    if facade_get_conversation_manager is not None:
        try:
            return facade_get_conversation_manager(plugin)
        except Exception:
            return None

    initializer = getattr(plugin, "initializer", None)
    if initializer is None:
        return None
    return getattr(initializer, "conversation_manager", None)


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
        plugin = detect_lm(context)
    if plugin is None:
        return "未检测到 LivingMemory"
    return "LivingMemory 尚未初始化"


def history_connection(context):
    """Resolve the active read-only history query surface.

    Prefers the facade ``api.public.get_history_reader(context)`` so the private
    attribute chain (``initializer.conversation_manager.store.connection``) is
    owned by LivingMemory instead of being duplicated here. The local chain
    below is kept verbatim as a fallback for LivingMemory versions without the
    facade.

    Note (B6):
        这是本模块唯一的读取器"接缝"：``history_sessions`` / ``history_bounds`` /
        ``history_page`` 都经由它取连接（测试也在此处注入）。SQL 与表/列名已交给
        门面的 ``query_history_*``；本模块只保留一份冻结的 legacy 副本，供没有
        ``api`` 包的旧版本 LivingMemory 使用。

    Args:
        context: AstrBot context.

    Returns:
        The active SQLite connection, or None when unavailable.
    """
    plugin = detect_lm(context)
    capability = getattr(plugin, "get_dynamics_capabilities", None)
    if callable(capability):
        manifest = capability()
        if not isinstance(manifest, dict) or manifest.get("ready") is not True or manifest.get("enabled") is False:
            return None
    manager = get_conversation_manager(plugin) if plugin else None
    store = getattr(manager, "store", None)
    connection = getattr(store, "connection", None)
    # Resolve ownership before invoking a facade; never let a stale facade select
    # another active provider's store and bypass the conflict check above.
    facade_get_history_reader = _facade_fn("get_history_reader")
    if callable(facade_get_history_reader):
        try:
            reader = facade_get_history_reader(context)
            if reader is connection and callable(getattr(reader, "execute", None)):
                return reader
        except Exception:
            pass
    return connection if callable(getattr(connection, "execute", None)) else None


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
