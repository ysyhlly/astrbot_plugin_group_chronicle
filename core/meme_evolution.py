"""Wave 3 Group Meme Evolution & Remix features.

1. Upgrades ``/梗溯源 <词>`` with ``visual_media.get_group_top_memes`` into an
   illustrated evolution card (origin dialogue + timeline + top group memes +
   hot-term prompt to run ``/梗图演绎``), with clean graceful degradation when
   ``visual_media`` or image rendering is unavailable.
2. Implements ``/梗图演绎 <词>`` calling ``visual_media.generate_series_artwork``
   with ``style="meme_remix", source_plugin="group_chronicle"``.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any

from astrbot.api import logger

from .render import normalize_render_theme, render_text_or_image
from .series_bridge import fetch_group_top_memes, generate_artwork

HOT_MEME_SOURCE_THRESHOLD = 3
HOT_MEME_TIMELINE_THRESHOLD = 2
HOT_MEME_USAGE_THRESHOLD = 5


def _is_hot_term(entry: dict[str, Any], memes: list[dict[str, Any]]) -> bool:
    src_cnt = len(entry.get("sources") or [])
    tl_cnt = len(entry.get("timeline") or [])
    top_usage = max((int(m.get("usage_count") or 0) for m in memes), default=0)
    return (
        src_cnt >= HOT_MEME_SOURCE_THRESHOLD
        or tl_cnt >= HOT_MEME_TIMELINE_THRESHOLD
        or top_usage >= HOT_MEME_USAGE_THRESHOLD
    )


async def build_meme_evolution_card(
    plugin: Any,
    event: Any,
    query: str,
) -> dict[str, Any]:
    """Build upgraded ``/梗溯源 <词>`` response with top group memes & evolution card.

    Returns a dict:
        ``{"text": str, "card_image_path": str | None, "meme_images": list[str],
           "memes": list[dict], "hot": bool, "entry": dict | None}``
    """
    umo = str(getattr(event, "unified_msg_origin", "") or "")
    member = str(getattr(event, "get_sender_id", lambda: "")() or "")
    base_text = await plugin.culture.command(event, "梗溯源", query)

    # Query structured entry (respects identical privacy & verification rules)
    res = await plugin.culture.query(umo, member, query, "origin", limit=3)
    entries = list(res.get("entries") or [])
    if not entries:
        return {
            "text": base_text,
            "card_image_path": None,
            "meme_images": [],
            "memes": [],
            "hot": False,
            "entry": None,
        }

    primary = entries[0]
    term_name = str(primary.get("name") or query or "").strip()
    memes = await fetch_group_top_memes(
        plugin.context, umo, keyword=term_name, limit=3
    )
    hot = _is_hot_term(primary, memes)

    tz = plugin._series_tz() if hasattr(plugin, "_series_tz") else None

    def _fmt_ts(ts_val: Any) -> str:
        return (
            datetime.fromtimestamp(float(ts_val), tz).strftime("%Y-%m-%d %H:%M")
            if ts_val
            else "未知"
        )

    # Append meme evolution & hot-remix hint to plain text
    extra_lines: list[str] = []
    if memes:
        extra_lines.append("")
        extra_lines.append("🖼️ 本群关联代表梗图：")
        for idx, m in enumerate(memes, 1):
            usage = f"（使用 {m['usage_count']} 次）" if m["usage_count"] else ""
            tags = f" [{' / '.join(m['tags'][:3])}]" if m["tags"] else ""
            extra_lines.append(f"{idx}. {m['label']}{usage}{tags}")
    if hot and term_name:
        extra_lines.append(
            f"🔥 「{term_name}」热度颇高！发送「/梗图演绎 {term_name}」可生成角色演绎的专属表情图。"
        )
    full_text = base_text + ("\n" + "\n".join(extra_lines) if extra_lines else "")

    # Assemble structured markdown for evolution card image
    md_lines = [
        f"# 梗图进化论 · {term_name}",
        "",
        f"> **含义**：{primary.get('meaning') or '未记录'}",
        f"> **用法**：{primary.get('usage') or '未记录'} · **最早留存**：{_fmt_ts(primary.get('first_seen'))}",
        "",
        "## 起源与出处",
        (
            primary["origin"]["text"]
            if primary.get("origin")
            else "起源未知；最早留存记录不等于真正起源。"
        ),
        "",
    ]
    sources = list(primary.get("sources") or [])[:3]
    if sources:
        md_lines.append("## 起源对话留存")
        for s in sources:
            md_lines.append(f"- **{s['name']}**（{_fmt_ts(s.get('ts'))}）：「{s['quote']}」")
        md_lines.append("")
    timeline = list(primary.get("timeline") or [])
    if timeline:
        md_lines.append("## 沿革时间线")
        for node in timeline[:6]:
            md_lines.append(f"- **{_fmt_ts(node.get('recorded_at'))}**：{node['text']}")
        md_lines.append("")
    if memes:
        md_lines.append("## 本群关联代表梗图")
        for m in memes:
            usage = f" · 出现 {m['usage_count']} 次" if m["usage_count"] else ""
            md_lines.append(f"- **{m['label']}**{usage}")
        md_lines.append("")
    if hot:
        md_lines.append(f"> 🔥 热门词条推荐：发送 `/梗图演绎 {term_name}` 生成角色专属演绎表情图。")

    card_image_path: str | None = None
    render_mode = str(plugin._cfg("render_mode", "t2i")).strip() or "t2i"
    if render_mode != "off":
        try:
            group_name = await asyncio.to_thread(plugin._group_name_readonly, umo)
            theme = normalize_render_theme(
                plugin._cfg("render_theme", "classic_scroll")
            )
            _md, card_image_path = await render_text_or_image(
                "\n".join(md_lines),
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
                title_suffix="梗图进化论",
                epigraph="每一枚群梗的来处与演变",
                seal_char="梗",
            )
        except Exception as exc:
            logger.debug(f"梗溯源卡片渲染失败，降级为纯文本: {exc}")
            card_image_path = None

    meme_images = [m["path"] for m in memes if m.get("exists") and m.get("path")]
    return {
        "text": full_text,
        "card_image_path": card_image_path,
        "meme_images": meme_images,
        "memes": memes,
        "hot": hot,
        "entry": primary,
    }


async def generate_meme_remix(
    plugin: Any,
    event: Any,
    term: str,
) -> dict[str, Any]:
    """Generate a character meme-remix sticker for a group slang term via visual_media."""
    umo = str(getattr(event, "unified_msg_origin", "") or "")
    member = str(getattr(event, "get_sender_id", lambda: "")() or "")
    clean_term = str(term or "").strip()[:80]
    if not clean_term:
        return {
            "ok": False,
            "text": "用法：/梗图演绎 <群梗词或ID>（将结合本群文化词库生成角色演绎表情图）。",
            "image_path": None,
        }

    # Try fetching verified culture context first (strictly privacy-safe)
    meaning = ""
    usage = ""
    origin_text = ""
    term_name = clean_term
    from_culture = False
    try:
        res = await plugin.culture.query(umo, member, clean_term, "origin", limit=1)
        entries = list(res.get("entries") or [])
        if entries:
            entry = entries[0]
            term_name = str(entry.get("name") or clean_term)
            meaning = str(entry.get("meaning") or "")
            usage = str(entry.get("usage") or "")
            origin_text = (
                str(entry["origin"]["text"]) if entry.get("origin") else ""
            )
            from_culture = True
    except Exception:
        logger.debug("梗图演绎查询文化词库失败，使用原始词条继续", exc_info=True)

    prompt_parts = [
        f"创作一张活泼有趣的群聊表情包（meme_remix 风格），角色演绎本群热梗「{term_name}」。",
    ]
    if meaning:
        prompt_parts.append(f"热梗含义：{meaning}。")
    if usage:
        prompt_parts.append(f"常见使用场景：{usage}。")
    if origin_text:
        prompt_parts.append(f"背景梗概：{origin_text}。")
    prompt_parts.append("构图简洁、表情生动传神、适合群聊斗图使用。")
    prompt = " ".join(prompt_parts)

    artwork = await generate_artwork(
        plugin.context,
        umo=umo,
        prompt=prompt,
        style="meme_remix",
        title=f"梗图演绎：{term_name}",
        source_plugin="group_chronicle",
        extra={"keyword": term_name, "meaning": meaning},
    )
    if artwork.get("ok") and artwork.get("image_path"):
        caption_lines = [f"🎨 群梗演绎完成：「{term_name}」"]
        if meaning:
            caption_lines.append(f"释义：{meaning}")
        if artwork.get("caption"):
            caption_lines.append(artwork["caption"])
        return {
            "ok": True,
            "text": "\n".join(caption_lines),
            "image_path": artwork["image_path"],
            "from_culture": from_culture,
            "degraded": False,
        }

    # Graceful degradation when visual_media is absent or quota-limited: render styled text card
    degrade_md = (
        f"# 梗图演绎 · {term_name}\n\n"
        f"> {meaning or '本群经典热梗演绎卡片'}\n\n"
        + (f"- 使用场景：{usage}\n" if usage else "")
        + (f"- 起源参考：{origin_text}\n" if origin_text else "")
        + f"\n（当前未接入视觉生图工坊或生图额度受限：{artwork.get('reason') or '已降级为文字卡片'}）"
    )
    fallback_img: str | None = None
    render_mode = str(plugin._cfg("render_mode", "t2i")).strip() or "t2i"
    if render_mode != "off":
        try:
            group_name = await asyncio.to_thread(plugin._group_name_readonly, umo)
            theme = normalize_render_theme(
                plugin._cfg("render_theme", "classic_scroll")
            )
            _t, fallback_img = await render_text_or_image(
                degrade_md,
                group_name,
                os.path.join(plugin.data_dir, "images"),
                render_mode=render_mode,
                html_render_func=getattr(plugin, "html_render", None),
                enable_cache=False,
                render_fallback_playwright=bool(
                    plugin._cfg("render_fallback_playwright", True)
                ),
                theme=theme,
                title_suffix="梗图演绎",
                seal_char="梗",
            )
        except Exception:
            fallback_img = None

    return {
        "ok": True,
        "text": degrade_md,
        "image_path": fallback_img,
        "from_culture": from_culture,
        "degraded": True,
        "reason": artwork.get("reason") or "",
    }
