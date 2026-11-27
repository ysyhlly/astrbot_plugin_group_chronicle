"""Quarterly & annual group/personal retrospective time-machine report cards.

Combines:
- Group Chronicle lifetime facts, member grades/trajectories, and approved group slang;
- Dynamics Memory / LivingMemory read-only highlights via ``core.lm_bridge`` (gracefully
  degrading when memory is absent or unavailable);
- Optional social relation pairs via ``core.influence``;
- Multi-theme card image rendering via ``core.render``.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime
from typing import Any

from astrbot.api import logger

from . import lm_bridge
from .archive_store import private_text
from .influence import relation_pairs
from .quotes import extract_quotes
from .render import normalize_render_theme, render_text_or_image

PERIOD_SPECS: dict[str, tuple[int, str]] = {
    "quarter": (90, "季度时光机"),
    "year": (365, "年度时光机"),
}


def parse_annual_report_args(raw: str = "", *, is_group_alias: bool = False) -> dict[str, Any]:
    """Parse `/我的群年报 [季度|年度] [主题=...]` or `/群年报 [季度|年度] [主题=...]`."""
    text = str(raw or "").strip()
    period = "year"
    if re.search(r"季度|季报|quarter|\b90\b", text, re.IGNORECASE):
        period = "quarter"
    elif re.search(r"年度|年报|year|\b365\b", text, re.IGNORECASE):
        period = "year"

    mode = "group" if is_group_alias else "personal"
    if re.search(r"全群|本群|群整体", text):
        mode = "group"
    elif re.search(r"个人|我的", text):
        mode = "personal"

    theme: str | None = None
    theme_match = re.search(r"主题\s*[=＝]?\s*(\S+)", text)
    if theme_match:
        theme = normalize_render_theme(theme_match.group(1))
    else:
        for kw, canonical in (
            ("classic_scroll", "classic_scroll"),
            ("dark_grimoire", "dark_grimoire"),
            ("modern_card", "modern_card"),
            ("古籍", "classic_scroll"),
            ("深紫", "dark_grimoire"),
            ("典雅", "dark_grimoire"),
            ("现代", "modern_card"),
            ("清爽", "modern_card"),
        ):
            if kw in text.lower() or kw in text:
                theme = canonical
                break

    days, label = PERIOD_SPECS[period]
    return {
        "period": period,
        "days": days,
        "period_label": label,
        "mode": mode,
        "theme": theme,
    }


def _filter_private_lines(lines: list[str], hidden_members: list[dict[str, Any]]) -> list[str]:
    if not hidden_members:
        return lines
    return [
        line
        for line in lines
        if not any(private_text(line, member) for member in hidden_members)
    ]


async def build_annual_report(
    plugin,
    umo: str,
    *,
    actor_id: str,
    actor_name: str,
    mode: str = "personal",
    period: str = "year",
    group_name: str = "群聊",
    theme: str | None = None,
) -> tuple[str, str | None]:
    """Build a multi-dimensional quarterly/annual report markdown & rendered card image.

    Returns:
        ``(markdown_text, image_path_or_None)``.
    """
    days, period_label = PERIOD_SPECS.get(period, PERIOD_SPECS["year"])
    tz = plugin._series_tz() if hasattr(plugin, "_series_tz") else None
    now_ts = time.time()
    since_ts = now_ts - days * 86400
    start_str = datetime.fromtimestamp(since_ts, tz).strftime("%Y-%m-%d")
    end_str = datetime.fromtimestamp(now_ts, tz).strftime("%Y-%m-%d")

    history = getattr(plugin, "history", None)
    store = getattr(history, "store", None) if history is not None else None
    rules = history.rules() if history is not None else {}

    def _load_archive_data():
        if store is None:
            return None, None, [], [], [], []
        hidden = store.hidden(umo)
        target_member = store.member(umo, actor_id) if mode == "personal" else None
        if mode == "personal" and target_member and target_member.get("hidden"):
            return target_member, None, [], [], hidden, []
        events = store.events(
            umo,
            since=since_ts,
            public=True,
            uid=actor_id if mode == "personal" else None,
        )
        grade_info = (
            store.grade(umo, actor_id, rules) if mode == "personal" else None
        )
        top_uids = store.top(umo, since_ts, 5) if mode == "group" else []
        top_members = store.members_by_uids(umo, top_uids) if top_uids else {}
        top_ranked = [
            {
                "uid": uid,
                "name": (top_members.get(uid) or {}).get("name") or uid,
                "grade": store.grade(umo, uid, rules),
            }
            for uid in top_uids
        ]
        return target_member, grade_info, events, top_ranked, hidden, top_uids

    (
        target_member,
        grade_info,
        events,
        top_ranked,
        hidden_members,
        _top_uids,
    ) = await asyncio.to_thread(_load_archive_data)

    if mode == "personal" and target_member and target_member.get("hidden"):
        return (
            "该成员档案当前处于隐藏状态，无法生成年报；如需恢复请先发送「史记恢复」。",
            None,
        )

    hidden_uids = {str(m["uid"]) for m in hidden_members}
    display_name = (
        (target_member or {}).get("name") or actor_name or actor_id
        if mode == "personal"
        else group_name
    )

    # 1. Read-only Dynamics Memory highlights (gracefully degrades if absent)
    mem_highlights = await lm_bridge.query_memory_highlights(
        plugin.context,
        umo,
        member_id=actor_id if mode == "personal" else "",
        since_ts=since_ts,
        limit=6,
    )

    # 2. Approved group slang / culture entries
    culture_terms: list[dict[str, Any]] = []
    try:
        c_res = await plugin.get_culture_context(umo, "", limit=6)
        if isinstance(c_res, dict) and c_res.get("available"):
            culture_terms = list(c_res.get("terms") or [])
    except Exception:
        logger.debug("Annual report culture context unavailable", exc_info=True)

    # 3. Optional social relation pairs (hidden-filtered)
    peer_lines: list[str] = []
    seen_peers: set[str] = set()
    for peer in mem_highlights.get("co_members") or []:
        p_uid = str(peer.get("uid") or "")
        p_name = str(peer.get("name") or p_uid)
        if not p_uid or p_uid in hidden_uids or (mode == "personal" and p_uid == actor_id):
            continue
        if p_uid in seen_peers:
            continue
        seen_peers.add(p_uid)
        cnt = int(peer.get("interactions") or 0)
        peer_lines.append(f"- **{p_name}**（同频互动 {cnt} 次）")
        if len(peer_lines) >= 4:
            break

    if len(peer_lines) < 4:
        try:
            pairs = await relation_pairs(plugin.context, umo, top_k=6, window_days=days)
            for a, b, score in pairs or []:
                sa, sb = str(a), str(b)
                if sa in hidden_uids or sb in hidden_uids:
                    continue
                if mode == "personal":
                    if actor_id not in (sa, sb):
                        continue
                    other = sb if sa == actor_id else sa
                    if other in seen_peers:
                        continue
                    seen_peers.add(other)
                    m_info = (
                        await asyncio.to_thread(store.member, umo, other)
                        if store is not None
                        else None
                    )
                    other_name = (m_info or {}).get("name") or other
                    peer_lines.append(f"- **{other_name}**（关系热度 {score:.1f}）")
                else:
                    pair_key = f"{sa}:{sb}"
                    if pair_key in seen_peers:
                        continue
                    seen_peers.add(pair_key)
                    m_map = (
                        await asyncio.to_thread(store.members_by_uids, umo, [sa, sb])
                        if store is not None
                        else {}
                    )
                    na = (m_map.get(sa) or {}).get("name") or sa
                    nb = (m_map.get(sb) or {}).get("name") or sb
                    peer_lines.append(f"- **{na} ⇄ {nb}**（关系热度 {score:.1f}）")
                if len(peer_lines) >= 4:
                    break
        except Exception:
            logger.debug("Annual report relation pairs unavailable", exc_info=True)

    # 4. Golden quotes from admitted event excerpts + memory samples (privacy-filtered)
    quote_inputs: list[dict[str, str]] = []
    for ev in events:
        for src in ev.get("sources") or []:
            excerpt = str(src.get("excerpt") or "").strip()
            if excerpt:
                quote_inputs.append({"content": excerpt})
    for sample in mem_highlights.get("sample_quotes") or []:
        if isinstance(sample, dict) and sample.get("content"):
            quote_inputs.append(sample)
    raw_quotes = extract_quotes(quote_inputs, max_quotes=5)
    quotes = _filter_private_lines(raw_quotes, hidden_members)

    # 5. Topics from Dynamics Memory (privacy-filtered)
    topics = _filter_private_lines(
        list(mem_highlights.get("topics") or []), hidden_members
    )[:5]

    # 6. Highlight events (ranked by importance desc, then recency desc)
    highlights = sorted(
        events, key=lambda e: (-int(e.get("importance", 1)), -float(e.get("event_at", 0)))
    )[:5]

    # Assemble rich markdown card
    heading = (
        f"# {display_name} 的{period_label}"
        if mode == "personal"
        else f"# {group_name} · {period_label}"
    )
    lines: list[str] = [heading, ""]

    if mode == "personal":
        g_name = (grade_info or {}).get("grade", "小记")
        g_pts = int((grade_info or {}).get("points", 0))
        g_ev = int((grade_info or {}).get("events", 0))
        g_span = (grade_info or {}).get("span_days", 0)
        summary_quote = (
            f"> 统计周期：{start_str} ~ {end_str}（{days} 天） · "
            f"当前等级：**{g_name}** · 累计贡献 {g_pts} 分（{g_ev} 件大事，跨度 {g_span} 天）"
        )
        if mem_highlights.get("available"):
            summary_quote += (
                f" · 留存活跃 {mem_highlights['active_days']} 天 / "
                f"{mem_highlights['message_count']} 条发言"
            )
        lines += [summary_quote, ""]
        lines += ["## 人物等级轨迹"]
        basis = (grade_info or {}).get("basis") or []
        if basis:
            for item in basis[-5:]:
                lines.append(
                    f"- **#{item['id']} {item['title']}**（+{item['points']} 分"
                    + (" · 重大核心贡献" if item.get("major_core") else "")
                    + "）"
                )
        else:
            lines.append("- 目前保持基础小记，持续参与群内协作与活动即可点亮小传与列传。")
        lines.append("")
    else:
        summary_quote = (
            f"> 统计周期：{start_str} ~ {end_str}（{days} 天） · "
            f"已入史大事 **{len(events)}** 件 · 文化热梗 **{len(culture_terms)}** 条"
        )
        if mem_highlights.get("available"):
            summary_quote += (
                f" · 留存活跃 {mem_highlights['active_days']} 天 / "
                f"{mem_highlights['message_count']} 条发言"
            )
        lines += [summary_quote, ""]
        lines += ["## 群星贡献榜"]
        if top_ranked:
            for idx, item in enumerate(top_ranked, 1):
                g = item["grade"]
                lines.append(
                    f"- {idx}. **{item['name']}**（{g['grade']} · {g['points']} 分 / {g['events']} 件大事）"
                )
        else:
            lines.append("- 本阶段尚无入史贡献排行，待群内大事陆续沉淀。")
        lines.append("")

    lines += ["## 阶段高光大事"]
    if highlights:
        for ev in highlights:
            ev_date = datetime.fromtimestamp(float(ev["event_at"]), tz).strftime("%Y-%m-%d")
            names = "、".join(p["name"] for p in ev.get("participants", [])[:5])
            detail = str(ev.get("detail") or "")[:120]
            lines.append(f"- **{ev_date} · {ev['title']}**（{names}）：{detail}")
    else:
        lines.append("- 本周期暂无已确认入史的群大事，可使用「记一笔」记录难忘时刻。")
    lines.append("")

    lines += ["## 共同经历与高频话题"]
    if topics:
        for tp in topics:
            lines.append(f"- {tp}")
    elif mem_highlights.get("available"):
        lines.append("- 记忆库已连接，本周期暂未提炼出独立高频话题摘要。")
    else:
        lines.append("- 群间记忆暂未就绪，本次报告基于群史记与群文化档案生成。")
    lines.append("")

    lines += ["## 默契同行群友"]
    if peer_lines:
        lines.extend(peer_lines)
    else:
        lines.append("- 互动关系样本较少，多在群里聊天互动后将自动点亮同行榜单。")
    lines.append("")

    if quotes:
        lines += ["## 专属金句回响"]
        for q in quotes:
            lines.append(f"- 「{q}」")
        lines.append("")

    if culture_terms:
        lines += ["## 本群热梗词典"]
        for term in culture_terms[:6]:
            lines.append(f"- **{term['name']}**：{term['meaning']}")
        lines.append("")

    if history is not None:
        lines.append(f"—— {await asyncio.to_thread(history.coverage, umo)}")

    markdown_text = "\n".join(lines).strip()
    resolved_theme = normalize_render_theme(
        theme or plugin._cfg("render_theme", "classic_scroll")
    )
    image_path = None
    try:
        render_mode = str(plugin._cfg("render_mode", "t2i")).strip() or "t2i"
        fallback_pw = bool(plugin._cfg("render_fallback_playwright", True))
        t2i_scale = str(plugin._cfg("t2i_scale", "device")).strip() or "device"
        markdown_text, image_path = await render_text_or_image(
            markdown_text,
            group_name,
            f"{plugin.data_dir}/images",
            render_mode=render_mode,
            html_render_func=getattr(plugin, "html_render", None),
            enable_cache=bool(plugin._cfg("enable_render_cache", True)),
            render_fallback_playwright=fallback_pw,
            options={"scale": t2i_scale},
            theme=resolved_theme,
            title_suffix=period_label,
        )
    except Exception as exc:
        logger.warning(f"渲染群年报长图失败，降级为纯文本: {exc}")
        image_path = None
    return markdown_text, image_path
