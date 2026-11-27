"""Zero-coupling Wave 3 series bridges for Group Chronicle.

Connects (strictly dynamically, with silent graceful degradation when absent):
- ``astrbot_plugin_chat_dynamics``: Dynamics Event Bus (subscribe ``topic_archived``,
  unsubscribe on terminate, emit ``milestone_achieved``).
- ``astrbot_plugin_visual_media``: ``get_group_top_memes`` & ``generate_series_artwork``
  (``polaroid`` for chronicle volumes / major events, ``meme_remix`` for slang remix).
- ``astrbot_plugin_dynamics_world``: ``register_external_keepsake`` to mount chronicle
  editions and milestone polaroids on the 3D cabin photo wall / bookshelf.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import os
import re
import time
from typing import Any
from urllib.parse import urlencode

from astrbot.api import logger

from .archive_store import encode, private_text

GROUP_PATTERN = re.compile(r"^[^:]+:GroupMessage:.+$")

_TOPIC_RING_CAP = 30
_KEEPSAKE_DEDUP_CAP = 200


def find_star(context: Any, plugin_name: str) -> Any | None:
    """Dynamically locate an activated peer plugin without static imports."""
    if context is None or not plugin_name:
        return None
    try:
        getter = getattr(context, "get_registered_star", None)
        if callable(getter):
            star = getter(plugin_name)
            if star is not None and getattr(star, "activated", True) is not False:
                return getattr(star, "star_cls", None) or star
    except Exception:
        pass
    try:
        get_all = getattr(context, "get_all_stars", None)
        if callable(get_all):
            raw = get_all()
            items = raw.values() if isinstance(raw, dict) else (raw or [])
            for star in items:
                if getattr(star, "activated", True) is False:
                    continue
                name = str(getattr(star, "name", "") or "")
                if name == plugin_name:
                    return getattr(star, "star_cls", None) or star
    except Exception:
        pass
    return None


async def _maybe_await(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


# ---------------------------------------------------------------------------
# Visual Media Bridge
# ---------------------------------------------------------------------------


async def fetch_group_top_memes(
    context: Any, umo: str, *, keyword: str = "", limit: int = 3
) -> list[dict[str, Any]]:
    """Query top group memes from visual_media with graceful degradation."""
    visual = find_star(context, "astrbot_plugin_visual_media")
    if visual is None:
        return []
    fn = getattr(visual, "get_group_top_memes", None)
    if not callable(fn):
        return []
    bound = max(1, min(int(limit or 3), 10))
    raw: Any = None
    for args, kwargs in (
        ((umo,), {"keyword": str(keyword or "").strip(), "limit": bound}),
        ((umo, str(keyword or "").strip(), bound), {}),
        ((umo,), {"limit": bound}),
    ):
        try:
            raw = await asyncio.wait_for(_maybe_await(fn(*args, **kwargs)), timeout=15.0)
            break
        except TypeError:
            continue
        except Exception as exc:
            logger.debug(f"visual_media.get_group_top_memes 不可用或超时: {exc}")
            return []

    if isinstance(raw, dict):
        items = raw.get("memes") or raw.get("items") or raw.get("results") or []
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        items = []

    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        path = str(
            item.get("thumbnail_path")
            or item.get("image_path")
            or item.get("file_path")
            or item.get("path")
            or ""
        ).strip()
        label = str(
            item.get("label")
            or item.get("caption")
            or item.get("description")
            or item.get("title")
            or item.get("keyword")
            or item.get("name")
            or "群聊梗图"
        ).strip()
        tags = item.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]
        normalized.append(
            {
                "id": str(item.get("id") or item.get("meme_id") or ""),
                "label": label[:80],
                "path": path,
                "exists": bool(path and os.path.isfile(path)),
                "usage_count": int(
                    item.get("usage_count")
                    or item.get("use_count")
                    or item.get("count")
                    or 0
                ),
                "tags": [str(t) for t in tags[:6]],
            }
        )
        if len(normalized) >= bound:
            break
    return normalized


async def generate_artwork(
    context: Any,
    *,
    umo: str,
    prompt: str,
    style: str = "polaroid",
    title: str = "",
    source_plugin: str = "group_chronicle",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Invoke visual_media.generate_series_artwork safely with graceful degradation."""
    visual = find_star(context, "astrbot_plugin_visual_media")
    if visual is None:
        return {
            "ok": False,
            "image_path": "",
            "caption": "",
            "degraded": True,
            "reason": "未安装或未启用 visual_media 插件",
        }
    fn = getattr(visual, "generate_series_artwork", None)
    if not callable(fn):
        return {
            "ok": False,
            "image_path": "",
            "caption": "",
            "degraded": True,
            "reason": "visual_media 未暴露 generate_series_artwork 接口",
        }
    full_kwargs = {
        "umo": umo,
        "prompt": str(prompt or "").strip()[:1200],
        "style": str(style or "polaroid"),
        "title": str(title or "")[:120],
        "source_plugin": source_plugin,
        **(extra or {}),
    }
    minimal_kwargs = {
        "umo": umo,
        "prompt": full_kwargs["prompt"],
        "style": full_kwargs["style"],
        "source_plugin": source_plugin,
    }
    raw: Any = None
    for kw in (full_kwargs, minimal_kwargs):
        try:
            raw = await asyncio.wait_for(_maybe_await(fn(**kw)), timeout=60.0)
            break
        except TypeError:
            continue
        except Exception as exc:
            logger.warning(f"visual_media.generate_series_artwork 调用失败: {exc}")
            return {
                "ok": False,
                "image_path": "",
                "caption": "",
                "degraded": True,
                "reason": str(exc),
            }
    if isinstance(raw, str):
        path = raw.strip()
        return {
            "ok": bool(path and os.path.isfile(path)),
            "image_path": path,
            "caption": "",
            "degraded": not bool(path),
            "reason": "" if path else "empty_result",
        }
    if isinstance(raw, dict):
        path = str(
            raw.get("image_path")
            or raw.get("path")
            or raw.get("file_path")
            or ""
        ).strip()
        ok = bool(raw.get("ok", bool(path))) and bool(path)
        return {
            "ok": ok,
            "image_path": path,
            "caption": str(raw.get("caption") or raw.get("text") or "").strip(),
            "degraded": bool(raw.get("degraded", not ok)),
            "reason": str(raw.get("reason") or raw.get("error") or ""),
        }
    return {
        "ok": False,
        "image_path": "",
        "caption": "",
        "degraded": True,
        "reason": "invalid_response",
    }


# ---------------------------------------------------------------------------
# 3D World Keepsake & Event Bus Bridges
# ---------------------------------------------------------------------------


def build_chronicle_deep_link(
    umo: str, *, tab: str = "editions", entry_id: int | None = None
) -> str:
    """Build a canonical series panel deep-link URL query string for Chronicle Workbench."""
    params: dict[str, Any] = {
        "plugin": "astrbot_plugin_group_chronicle",
        "tab": tab,
        "umo": umo,
    }
    if entry_id is not None and int(entry_id) > 0:
        params["entry_id"] = int(entry_id)
    return "/pages/culture/?" + urlencode(params)


async def register_world_keepsake(
    context: Any,
    *,
    umo: str,
    title: str,
    summary: str,
    sender_name: str = "群史馆",
    deep_link: str = "",
    image_path: str = "",
    kind: str = "chronicle_edition",
    persona_id: str = "",
) -> dict[str, Any]:
    """Mount a chronicle keepsake into dynamics_world 3D cabin if available."""
    world = find_star(context, "astrbot_plugin_dynamics_world")
    if world is None:
        return {"ok": False, "reason": "dynamics_world_unavailable"}
    fn = getattr(world, "register_external_keepsake", None)
    if not callable(fn):
        return {"ok": False, "reason": "register_external_keepsake_unavailable"}
    try:
        res = await asyncio.wait_for(
            _maybe_await(
                fn(
                    persona_id=str(persona_id or ""),
                    umo=str(umo or ""),
                    title=str(title or "群史纪念")[:120],
                    summary=str(summary or "")[:400],
                    sender_name=str(sender_name or "群史馆")[:60],
                    source_plugin="group_chronicle",
                    deep_link=str(deep_link or ""),
                    image_path=str(image_path or ""),
                    kind=str(kind or "chronicle_edition"),
                )
            ),
            timeout=15.0,
        )
        if isinstance(res, dict):
            return {"ok": bool(res.get("ok", True)), **res}
        return {"ok": bool(res)}
    except Exception as exc:
        logger.debug(f"dynamics_world.register_external_keepsake 失败（忽略）: {exc}")
        return {"ok": False, "reason": str(exc)}


def subscribe_topic_archived(
    context: Any, callback: Any, *, owner: str = "group_chronicle"
) -> str | None:
    """Subscribe to chat_dynamics Dynamics Event Bus ``topic_archived`` events."""
    core = find_star(context, "astrbot_plugin_chat_dynamics")
    if core is None:
        return None
    fn = getattr(core, "subscribe_dynamics_event", None)
    if not callable(fn):
        return None
    try:
        token = fn("topic_archived", callback, owner=owner)
        return str(token) if token else None
    except TypeError:
        try:
            token = fn("topic_archived", callback)
            return str(token) if token else None
        except Exception as exc:
            logger.debug(f"订阅 topic_archived 失败: {exc}")
            return None
    except Exception as exc:
        logger.debug(f"订阅 topic_archived 失败: {exc}")
        return None


def unsubscribe_event(context: Any, token: str | None) -> bool:
    """Safely unsubscribe from chat_dynamics Event Bus on plugin terminate."""
    if not token:
        return False
    core = find_star(context, "astrbot_plugin_chat_dynamics")
    if core is None:
        return False
    fn = getattr(core, "unsubscribe_dynamics_event", None)
    if not callable(fn):
        return False
    try:
        return bool(fn(token))
    except Exception as exc:
        logger.debug(f"退订事件总线失败（忽略）: {exc}")
        return False


async def emit_milestone_achieved(
    context: Any,
    *,
    umo: str,
    title: str,
    summary: str,
    kind: str = "chronicle_edition",
    image_path: str = "",
    deep_link: str = "",
    event_id: int | None = None,
    report_id: int | None = None,
    importance: int = 3,
) -> int:
    """Emit ``milestone_achieved`` onto chat_dynamics Event Bus."""
    core = find_star(context, "astrbot_plugin_chat_dynamics")
    if core is None:
        return 0
    fn = getattr(core, "emit_dynamics_event", None)
    if not callable(fn):
        return 0
    payload: dict[str, Any] = {
        "umo": str(umo or ""),
        "source_plugin": "group_chronicle",
        "kind": str(kind or "chronicle_edition"),
        "title": str(title or "")[:120],
        "summary": str(summary or "")[:400],
        "image_path": str(image_path or ""),
        "deep_link": str(deep_link or ""),
        "importance": int(importance or 3),
        "timestamp": time.time(),
    }
    if event_id is not None:
        payload["event_id"] = int(event_id)
    if report_id is not None:
        payload["report_id"] = int(report_id)
    try:
        res = await asyncio.wait_for(
            _maybe_await(fn("milestone_achieved", payload)), timeout=10.0
        )
        return int(res or 0)
    except Exception as exc:
        logger.debug(f"派发 milestone_achieved 失败（忽略）: {exc}")
        return 0


async def publish_chronicle_milestone(
    plugin: Any,
    umo: str,
    *,
    title: str,
    summary: str,
    kind: str = "chronicle_edition",
    tab: str = "editions",
    event_id: int | None = None,
    report_id: int | None = None,
    existing_image_path: str = "",
    importance: int = 3,
    sender_name: str = "群史馆",
) -> dict[str, Any]:
    """Generate a polaroid artwork, register a 3D world keepsake, and emit milestone_achieved.

    Completely error-isolated and deduplicated per ``(umo, kind, report_id/event_id/title)``
    so repeated triggers never spam the 3D cabin or artwork quota.
    """
    if getattr(plugin, "_terminating", False) or not umo:
        return {"ok": False, "skipped": True, "reason": "terminating_or_empty_umo"}

    dedup_set: set[str] = getattr(plugin, "_wave3_keepsake_dedup", None)
    if dedup_set is None:
        dedup_set = set()
        plugin._wave3_keepsake_dedup = dedup_set
    dedup_key = f"{umo}:{kind}:{report_id or ''}:{event_id or ''}:{title[:40]}"
    if dedup_key in dedup_set:
        return {"ok": True, "skipped": True, "reason": "already_published"}
    if len(dedup_set) >= _KEEPSAKE_DEDUP_CAP:
        dedup_set.clear()
    dedup_set.add(dedup_key)

    clean_title = str(title or "群史纪念")[:100]
    clean_summary = " ".join(str(summary or "").split())[:300]
    deep_link = build_chronicle_deep_link(
        umo, tab=tab, entry_id=event_id if tab == "events" else None
    )

    # 1. Generate polaroid illustration via visual_media (degrades cleanly to existing_image_path)
    prompt = (
        f"温暖怀旧拍立得纪念插画，记录群聊重要时刻：{clean_title}。"
        f"画面氛围与场景摘要：{clean_summary[:180]}。"
        "画面细腻柔和、无杂乱水印文字，富有群友共同回忆感。"
    )
    artwork = await generate_artwork(
        plugin.context,
        umo=umo,
        prompt=prompt,
        style="polaroid",
        title=clean_title,
        source_plugin="group_chronicle",
    )
    chosen_image = (
        artwork["image_path"]
        if artwork.get("ok") and artwork.get("image_path")
        else str(existing_image_path or "")
    )

    # 1b. Persist generated artwork & keepsake link onto latest chronicle edition
    history = getattr(plugin, "history", None)
    if (
        kind == "chronicle_edition"
        and chosen_image
        and history is not None
        and hasattr(history.store, "attach_report_artwork")
    ):
        try:
            await asyncio.to_thread(
                history.store.attach_report_artwork, umo, chosen_image, deep_link
            )
        except Exception:
            logger.debug("写入卷册插画路径失败（忽略）", exc_info=True)

    # 2. Register external keepsake on 3D World cabin
    keepsake = await register_world_keepsake(
        plugin.context,
        umo=umo,
        title=clean_title,
        summary=clean_summary,
        sender_name=sender_name,
        deep_link=deep_link,
        image_path=chosen_image,
        kind=kind,
    )

    # 3. Emit milestone_achieved on Dynamics Event Bus
    emitted = await emit_milestone_achieved(
        plugin.context,
        umo=umo,
        title=clean_title,
        summary=clean_summary,
        kind=kind,
        image_path=chosen_image,
        deep_link=deep_link,
        event_id=event_id,
        report_id=report_id,
        importance=importance,
    )
    return {
        "ok": True,
        "artwork": artwork,
        "image_path": chosen_image,
        "keepsake": keepsake,
        "emitted": emitted,
        "deep_link": deep_link,
    }


# ---------------------------------------------------------------------------
# Event Bus Consumer: topic_archived -> High-signal Chronicle Candidate
# ---------------------------------------------------------------------------


async def ingest_archived_topic_event(
    plugin: Any, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """Consume a ``topic_archived`` event from chat_dynamics as a chronicle candidate cue.

    - Strictly respects ``history.allowed(umo)`` and hidden-member privacy boundaries.
    - Deduplicates via deterministic ``fingerprint`` in ``ArchiveStore.add_event`` (status=pending).
    - Also maintains a bounded per-group ring buffer ``plugin._archived_topic_cues`` for inspection.
    """
    if getattr(plugin, "_terminating", False) or not isinstance(payload, dict):
        return {"accepted": False, "reason": "invalid_or_terminating"}
    umo = str(payload.get("umo") or payload.get("session_id") or "").strip()
    if not umo or not GROUP_PATTERN.fullmatch(umo):
        return {"accepted": False, "reason": "non_group_scope"}

    history = getattr(plugin, "history", None)
    if history is None or not plugin._cfg("history_enabled", True):
        return {"accepted": False, "reason": "history_disabled"}
    try:
        if not await history.allowed(umo):
            return {"accepted": False, "reason": "group_not_allowed"}
    except Exception:
        return {"accepted": False, "reason": "scope_check_failed"}

    title = str(
        payload.get("title")
        or payload.get("topic")
        or payload.get("subject")
        or ""
    ).strip()[:160]
    summary = str(
        payload.get("summary")
        or payload.get("digest")
        or payload.get("content")
        or ""
    ).strip()[:1200]
    if not title and not summary:
        return {"accepted": False, "reason": "empty_topic"}
    if not title:
        title = summary[:40]
    if not summary:
        summary = title

    msg_count = int(payload.get("message_count") or payload.get("msg_count") or 0)
    topic_id = str(payload.get("topic_id") or payload.get("id") or "").strip()
    event_ts = float(
        payload.get("archived_at")
        or payload.get("ended_at")
        or payload.get("timestamp")
        or time.time()
    )
    raw_participants = payload.get("participants") or []

    def _store_candidate() -> dict[str, Any]:
        store = history.store
        hidden_members = store.hidden(umo)
        hidden_uids = {str(m["uid"]) for m in hidden_members}
        # Privacy guard: never leak hidden member names/UIDs via archived topic summaries
        if any(
            private_text(title, m) or private_text(summary, m) for m in hidden_members
        ):
            return {"accepted": False, "reason": "privacy_filtered"}

        participants: list[dict[str, Any]] = []
        for p in raw_participants if isinstance(raw_participants, list) else []:
            if isinstance(p, dict):
                uid = str(p.get("uid") or p.get("sender_id") or p.get("id") or "").strip()
                name = str(p.get("name") or p.get("sender_name") or uid).strip()
            else:
                uid = str(p or "").strip()
                name = uid
            if not uid or uid in hidden_uids:
                continue
            member_row = store.member(umo, uid)
            if member_row and member_row.get("hidden"):
                continue
            resolved_name = (member_row or {}).get("name") or name or uid
            participants.append({"uid": uid, "name": resolved_name, "role": 1})

        # Keep cue in ring buffer
        ring_map: dict[str, list[dict[str, Any]]] = getattr(
            plugin, "_archived_topic_cues", None
        )
        if ring_map is None:
            ring_map = {}
            plugin._archived_topic_cues = ring_map
        cue = {
            "topic_id": topic_id,
            "title": title,
            "summary": summary,
            "message_count": msg_count,
            "event_at": event_ts,
            "participants": participants,
        }
        ring = ring_map.setdefault(umo, [])
        ring.append(cue)
        if len(ring) > _TOPIC_RING_CAP:
            del ring[:-_TOPIC_RING_CAP]

        # Insert as pending candidate event when substantial enough (>=3 msgs or detailed summary)
        event_id = None
        if msg_count >= 3 or len(summary) >= 24:
            fp_seed = topic_id or f"{title}:{int(event_ts // 3600)}"
            fingerprint = hashlib.sha256(
                encode(["topic_archived", umo, fp_seed]).encode()
            ).hexdigest()
            fact = {
                "title": title,
                "detail": summary,
                "kind": "milestone" if msg_count >= 15 else "activity",
                "importance": 2 if msg_count >= 10 else 1,
                "positive": True,
                "risk": False,
                "ongoing": False,
                "participants": participants,
                "unknown_participants": [],
                "event_at": event_ts,
                "date_basis": "recorded",
                "followups": [],
                "review_note": f"来自对话主控话题归档候选（共 {msg_count} 条消息），待管理员核实入史",
                "sources": [
                    {
                        "id": f"topic_archived:{topic_id or fingerprint[:12]}",
                        "uid": participants[0]["uid"] if participants else "system",
                        "hash": hashlib.sha256(summary.encode()).hexdigest(),
                        "excerpt": summary[:400],
                    }
                ],
                "fingerprint": fingerprint,
            }
            event_id = store.add_event(
                umo, fact, status="pending", actor="chat_dynamics:topic_archived"
            )
        return {"accepted": True, "event_id": event_id, "cue": cue}

    try:
        return await asyncio.to_thread(_store_candidate)
    except Exception as exc:
        logger.debug(f"处理 topic_archived 事件失败（忽略）: {exc}")
        return {"accepted": False, "reason": str(exc)}
