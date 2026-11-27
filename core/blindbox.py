"""Wave 3 Light-Gamification Retrospective: ``/群史盲盒`` & ``/那年今日``.

- ``/群史盲盒 [揭晓|答案|大事|词条]``:
  Randomly draws a public historical event or approved group slang entry, masking
  the participant/answer into a fun group quiz with a 5-minute TTL reveal mechanism
  that re-checks privacy revision before disclosing any answer.
- ``/那年今日 [MMDD]``:
  Finds historical events & earliest retained slang from matching calendar month-day
  in prior years (or ±3 day historical window fallback), renders a themed card image,
  and also supplies a low-frequency optional footer for weekly scheduled pushes.
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import time
from datetime import datetime
from typing import Any

from astrbot.api import logger

from .render import normalize_render_theme, render_text_or_image

BLINDBOX_TTL_SEC = 300.0


def _mask_text_names(text: str, names: list[str]) -> str:
    masked = str(text or "")
    for name in sorted({n for n in names if n and len(n) >= 1}, key=len, reverse=True):
        masked = masked.replace(name, "【神秘群友】")
    return masked


async def run_blindbox(
    plugin: Any,
    event: Any,
    arg: str = "",
    *,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    """Handle ``/群史盲盒 [揭晓|答案|大事|词条]`` with strict privacy enforcement."""
    umo = str(getattr(event, "unified_msg_origin", "") or "")
    member = str(getattr(event, "get_sender_id", lambda: "")() or "")
    clean = str(arg or "").strip().lower()
    history = getattr(plugin, "history", None)
    if history is None:
        return {"ok": False, "kind": "unavailable", "text": "长期群史服务未就绪。"}

    if not await history.allowed(umo):
        return {
            "ok": False,
            "kind": "disallowed",
            "text": "本群长期记录已停用或不在允许范围。",
        }

    box_map: dict[str, dict[str, Any]] = getattr(plugin, "_active_blindboxes", None)
    if box_map is None:
        box_map = {}
        plugin._active_blindboxes = box_map

    now_mono = time.monotonic()
    # Prune expired entries
    for k in [k for k, v in box_map.items() if now_mono > float(v.get("expires_at", 0))]:
        box_map.pop(k, None)

    # Reveal mode
    if clean in ("揭晓", "答案", "reveal", "answer", "开奖"):
        active = box_map.pop(umo, None)
        if not active:
            return {
                "ok": False,
                "kind": "no_active",
                "text": "当前没有进行中的群史盲盒，发送「/群史盲盒」开启一个新盲盒吧！",
            }
        # Privacy re-validation before revealing answer
        def _check_privacy():
            return (
                history.privacy_key(umo) == active["privacy_key"]
                and history.store.privacy_revision(umo) == active["privacy_revision"]
            )

        if not await asyncio.to_thread(_check_privacy):
            return {
                "ok": False,
                "kind": "privacy_changed",
                "text": "该盲盒涉及的档案隐私状态已变化，为保护成员隐私已取消揭晓，请重新开启新盲盒。",
            }
        return {
            "ok": True,
            "kind": "reveal",
            "box_type": active["box_type"],
            "text": active["answer_text"],
        }

    prefer = "any"
    if any(k in clean for k in ("词条", "梗", "文化", "slang", "culture")):
        prefer = "culture"
    elif any(k in clean for k in ("大事", "事件", "历史", "event")):
        prefer = "event"

    tz = plugin._series_tz() if hasattr(plugin, "_series_tz") else None

    def _load_candidates():
        events = history.store.events(umo, public=True)
        privacy_key = history.privacy_key(umo)
        privacy_rev = history.store.privacy_revision(umo)
        return events, privacy_key, privacy_rev

    events, privacy_key, privacy_rev = await asyncio.to_thread(_load_candidates)

    culture_entries: list[dict[str, Any]] = []
    culture = getattr(plugin, "culture", None)
    if culture is not None and prefer in ("any", "culture"):
        try:
            qres = await culture.query(umo, member or "system", "", "origin", limit=20)
            culture_entries = list(qres.get("entries") or [])
        except Exception:
            culture_entries = []

    pool: list[tuple[str, dict[str, Any]]] = []
    if prefer in ("any", "event"):
        pool.extend(("event", ev) for ev in events)
    if prefer in ("any", "culture"):
        pool.extend(("culture", entry) for entry in culture_entries)

    if not pool:
        return {
            "ok": False,
            "kind": "empty",
            "text": "本群暂未积累足够的公开历史大事或群梗词条，等记录多一点再来抽盲盒吧！",
        }

    chooser = rng or random.Random()
    box_type, chosen = chooser.choice(pool)

    if box_type == "event":
        ev_date = datetime.fromtimestamp(float(chosen["event_at"]), tz).strftime(
            "%Y-%m-%d"
        )
        participants = [p["name"] for p in chosen.get("participants", []) if p.get("name")]
        clue_detail = _mask_text_names(
            str(chosen.get("detail") or chosen.get("title") or "")[:140], participants
        )
        question_text = (
            "🎁 群史盲盒 · 历史名场面考据\n"
            f"📅 时间线索：{ev_date}\n"
            f"🔍 事件片段：{clue_detail}\n"
            "❓ 猜猜这件事的标题和关键当事人是谁？\n"
            "💡 5 分钟内发送「/群史盲盒 揭晓」查看完整档案与答案！"
        )
        names_str = "、".join(participants) if participants else "本群群友"
        answer_text = (
            "🎉 群史盲盒揭晓 · 历史名场面\n"
            f"📌 事件 #{chosen['id']}：{chosen['title']}（{ev_date}）\n"
            f"👥 当事人：{names_str}\n"
            f"📖 完整经过：{chosen.get('detail') or '（无更多详情）'}"
        )
    else:
        term_name = str(chosen.get("name") or "")
        first_date = (
            datetime.fromtimestamp(float(chosen["first_seen"]), tz).strftime("%Y-%m-%d")
            if chosen.get("first_seen")
            else "未知"
        )
        sources = list(chosen.get("sources") or [])
        clue_quote = ""
        source_names: list[str] = []
        if sources:
            clue_quote = str(sources[0].get("quote") or "")[:100]
            source_names = [str(s.get("name") or "") for s in sources if s.get("name")]
        masked_quote = _mask_text_names(clue_quote, source_names)
        question_text = (
            "🎁 群史盲盒 · 老梗出处考据\n"
            f"🏷️ 抽中群梗：「{term_name}」（最早留存：{first_date}）\n"
            + (f"💬 原话线索：「{masked_quote}」\n" if masked_quote else "")
            + "❓ 还记得这个梗是什么意思、最早是怎么来的吗？\n"
            "💡 5 分钟内发送「/群史盲盒 揭晓」查看释义与出处！"
        )
        origin_desc = (
            chosen["origin"]["text"]
            if chosen.get("origin")
            else "起源未知（最早留存记录不等于真正起源）"
        )
        answer_text = (
            "🎉 群史盲盒揭晓 · 老梗档案\n"
            f"🏷️ #{chosen['id']} 「{term_name}」\n"
            f"📖 释义：{chosen.get('meaning') or '未记录'}\n"
            f"📜 起源：{origin_desc}\n"
            f"📅 最早留存：{first_date}"
            + (
                f"\n💬 原话记录（{sources[0]['name']}）：「{sources[0]['quote']}」"
                if sources
                else ""
            )
        )

    box_map[umo] = {
        "box_type": box_type,
        "item_id": chosen.get("id"),
        "answer_text": answer_text,
        "privacy_key": privacy_key,
        "privacy_revision": privacy_rev,
        "expires_at": now_mono + BLINDBOX_TTL_SEC,
    }
    return {
        "ok": True,
        "kind": "question",
        "box_type": box_type,
        "text": question_text,
    }


def _parse_month_day(date_arg: str, now_dt: datetime) -> tuple[int, int]:
    clean = str(date_arg or "").strip()
    if not clean:
        return now_dt.month, now_dt.day
    m = re.match(r"^(\d{1,2})\s*[-/月.]\s*(\d{1,2})日?$", clean)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return month, day
    m2 = re.match(r"^(\d{2})(\d{2})$", clean)
    if m2:
        month, day = int(m2.group(1)), int(m2.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return month, day
    return now_dt.month, now_dt.day


async def build_on_this_day(
    plugin: Any,
    umo: str,
    *,
    date_arg: str = "",
    group_name: str = "群聊",
    render_card: bool = True,
    push_mode: bool = False,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Find 'On This Day' historical events & slang from prior years and render a card.

    In ``push_mode=True`` (used for low-frequency weekly push appendix), returns
    ``has_items=False`` unless there is an exact prior-year calendar month-day match.
    """
    history = getattr(plugin, "history", None)
    if history is None or not await history.allowed(umo):
        return {
            "ok": False,
            "has_items": False,
            "text": "本群长期记录已停用或不在允许范围。",
            "image_path": None,
            "events": [],
            "terms": [],
        }

    tz = plugin._series_tz() if hasattr(plugin, "_series_tz") else None
    now_dt = datetime.fromtimestamp(float(now_ts or time.time()), tz)
    target_month, target_day = _parse_month_day(date_arg, now_dt)
    current_year = now_dt.year
    date_label = f"{target_month:02d}月{target_day:02d}日"

    def _scan_events():
        all_public = history.store.events(umo, public=True)
        exact_past: list[tuple[int, dict[str, Any]]] = []
        near_past: list[tuple[int, dict[str, Any]]] = []
        for ev in all_public:
            ev_ts = float(ev.get("event_at") or 0)
            if ev_ts <= 0:
                continue
            dt = datetime.fromtimestamp(ev_ts, tz)
            if dt.year >= current_year and not date_arg:
                continue
            if dt.month == target_month and dt.day == target_day:
                exact_past.append((dt.year, ev))
            elif not push_mode:
                # ±3 days window in historical years
                try:
                    ref = dt.replace(year=current_year)
                    target = now_dt.replace(month=target_month, day=target_day)
                    if abs((ref.date() - target.date()).days) <= 3:
                        near_past.append((dt.year, ev))
                except ValueError:
                    pass
        exact_past.sort(key=lambda x: (-x[0], -int(x[1].get("importance", 1))))
        near_past.sort(key=lambda x: (-x[0], -int(x[1].get("importance", 1))))
        return exact_past, near_past

    exact_events, near_events = await asyncio.to_thread(_scan_events)

    # Also match approved culture terms first retained on this month-day in prior years
    matched_terms: list[tuple[int, dict[str, Any]]] = []
    culture = getattr(plugin, "culture", None)
    if culture is not None:
        try:
            qres = await culture.query(umo, "system", "", "search", limit=30)
            for entry in qres.get("entries") or []:
                fs = float(entry.get("first_seen") or 0)
                if fs <= 0:
                    continue
                dt = datetime.fromtimestamp(fs, tz)
                if dt.year >= current_year and not date_arg:
                    continue
                if dt.month == target_month and dt.day == target_day:
                    matched_terms.append((dt.year, entry))
        except Exception:
            matched_terms = []

    is_exact = bool(exact_events or matched_terms)
    chosen_events = exact_events if exact_events else ([] if push_mode else near_events)
    if not chosen_events and not matched_terms:
        return {
            "ok": True,
            "has_items": False,
            "text": f"📜 那年今日（{date_label}）：本群暂无往年同期的公开史记记录，继续一起创造新回忆吧！",
            "image_path": None,
            "events": [],
            "terms": [],
        }

    section_heading = (
        f"# 那年今日 · {date_label}"
        if is_exact
        else f"# 近期历史同期 · {date_label}前后"
    )
    lines = [section_heading, ""]
    if chosen_events:
        lines.append("## 往年群大事回响")
        for year, ev in chosen_events[:5]:
            years_ago = max(0, current_year - year)
            ago_tag = f"（{years_ago} 年前）" if years_ago > 0 else ""
            names = "、".join(p["name"] for p in ev.get("participants", [])[:4])
            ev_dt = datetime.fromtimestamp(float(ev["event_at"]), tz).strftime("%Y-%m-%d")
            lines.append(
                f"- **{ev_dt}{ago_tag} · {ev['title']}**"
                + (f"（{names}）" if names else "")
                + f"：{str(ev.get('detail') or '')[:140]}"
            )
        lines.append("")
    if matched_terms:
        lines.append("## 往年今日诞生的群梗")
        for year, term in matched_terms[:4]:
            years_ago = max(0, current_year - year)
            ago_tag = f"（{years_ago} 年前）" if years_ago > 0 else ""
            lines.append(
                f"- **{year}年{ago_tag} · 「{term['name']}」**：{term.get('meaning') or ''}"
            )
        lines.append("")

    markdown_text = "\n".join(lines).strip()
    image_path: str | None = None
    render_mode = str(plugin._cfg("render_mode", "t2i")).strip() or "t2i"
    if render_card and not push_mode and render_mode != "off":
        try:
            theme = normalize_render_theme(
                plugin._cfg("render_theme", "classic_scroll")
            )
            _t, image_path = await render_text_or_image(
                markdown_text,
                group_name,
                os.path.join(plugin.data_dir, "images"),
                render_mode=render_mode,
                html_render_func=getattr(plugin, "html_render", None),
                enable_cache=bool(plugin._cfg("enable_render_cache", True)),
                render_fallback_playwright=bool(
                    plugin._cfg("render_fallback_playwright", True)
                ),
                options={"scale": str(plugin._cfg("t2i_scale", "device")).strip() or "device"},
                theme=theme,
                title_suffix="那年今日",
                epigraph="翻开往年今日的群聊旧页",
                seal_char="昔",
            )
        except Exception as exc:
            logger.debug(f"那年今日图文卡渲染失败，降级为纯文本: {exc}")
            image_path = None

    return {
        "ok": True,
        "has_items": True,
        "exact": is_exact,
        "text": markdown_text,
        "image_path": image_path,
        "events": [ev for _, ev in chosen_events[:5]],
        "terms": [t for _, t in matched_terms[:4]],
    }
