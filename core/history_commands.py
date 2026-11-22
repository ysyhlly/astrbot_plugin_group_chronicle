"""Group-scoped Chinese commands for lifetime history and moderation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone

from astrbot.api import logger

from .archive_store import encode
from .deliver import deliver_text, split_text
from .lifetime import GROUP_PATTERN, KINDS, RISK_WORDS, scope_lists

HISTORY_TIMEZONE = timezone(timedelta(hours=8))

try:
    from ..series_panels import resolve_series_timezone
except ImportError:
    try:
        from series_panels import resolve_series_timezone
    except Exception:  # pragma: no cover - standalone fallback
        def resolve_series_timezone(
            _context, explicit_tz: str = "", default_tz: str = "Asia/Shanghai"
        ):
            _ = explicit_tz, default_tz
            return HISTORY_TIMEZONE


def _history_tz(plugin):
    return resolve_series_timezone(getattr(plugin, "context", None))

COMMANDS = (
    "群大事",
    "人物等级",
    "记一笔",
    "史记待审",
    "史记确认",
    "史记驳回",
    "史记合并",
    "史记修订",
    "史记更正",
    "史记隐藏",
    "史记进度",
)

def administrator(plugin, event):
    """Accept an AstrBot administrator or a trusted current-group OneBot role.

    Args:
        plugin: Owning plugin.
        event: Invoking event.

    Returns:
        Whether the caller can review facts in this group.
    """
    if plugin._is_admin(event):
        return True
    raw = getattr(getattr(event, "message_obj", None), "raw_message", {})
    if not callable(getattr(raw, "get", None)):
        return False
    sender = raw.get("sender", {})
    if not isinstance(sender, dict) or sender.get("role") not in ("admin", "owner"):
        return False
    return str(raw.get("group_id") or "") == str(event.get_group_id()) and str(
        sender.get("user_id") or ""
    ) == str(event.get_sender_id())


def participant_ids(event, actor):
    """Read explicit mention IDs, excluding the bot and all-members mentions.

    Args:
        event: Invoking event.
        actor: Default participant when nobody else is mentioned.

    Returns:
        Unique member IDs.
    """
    from astrbot.api.message_components import At

    self_id = str(event.get_self_id())
    ids = [
        str(s.qq)
        for s in event.get_messages()
        if isinstance(s, At) and str(s.qq) not in (self_id, "all", "", "None")
    ]
    return list(dict.fromkeys(ids)) or [actor]


def revise_payload(store, umo, fact, text):
    """Apply administrator-friendly detail and contribution fields.

    Args:
        store: Fact database.
        umo: Group session.
        fact: Existing scoped fact.
        text: ``经过｜重要度=3｜核心=ID｜说明=理由``.

    Returns:
        Full replacement payload and audit explanation.

    Raises:
        ValueError: A field is invalid or a participant is not known in this group.
    """
    payload = dict(fact)
    for key in ("id", "status", "revision"):
        payload.pop(key, None)
    fields = re.split(r"[｜|]", text)
    note = ""
    roles = None
    for index, field in enumerate(fields):
        field = field.strip()
        if not field:
            continue
        if "=" not in field and "＝" not in field:
            if index == 0:
                payload["detail"] = field[:3000]
                continue
            raise ValueError(
                "修订格式：史记修订 ID 新经过｜说明=修订理由，可追加 重要度=1~3、核心=成员ID"
            )
        key, value = re.split(r"[=＝]", field, maxsplit=1)
        key, value = key.strip(), value.strip()
        if key == "说明":
            note = value[:1000]
        elif key == "标题":
            if not value:
                raise ValueError("标题不能为空")
            payload["title"] = value[:200]
        elif key == "重要度":
            if value not in ("1", "2", "3"):
                raise ValueError("重要度只能为 1、2、3")
            payload["importance"] = int(value)
            payload["_recompute_credit"] = True
        elif key in ("贡献", "持续"):
            if value not in ("是", "否"):
                raise ValueError(f"{key}请填写 是 或 否")
            payload["positive" if key == "贡献" else "ongoing"] = value == "是"
            if key == "贡献":
                payload["_recompute_credit"] = True
        elif key == "类型":
            kind = {
                "活动": "activity",
                "成果": "achievement",
                "帮助": "help",
                "项目": "project",
                "文化": "culture",
                "转折": "turning_point",
                "争议": "dispute",
                "处分": "discipline",
            }.get(value, value)
            if kind not in KINDS:
                raise ValueError("未知事件类型")
            payload["kind"] = kind
            payload["_recompute_credit"] = True
        elif key in ("参与", "协助", "核心"):
            payload["_recompute_credit"] = True
            if roles is None:
                roles = {}
            for uid in re.split(r"[,，\s]+", value):
                if not uid:
                    continue
                member = store.member(umo, uid)
                if not member:
                    raise ValueError("参与者必须使用本群已经建档的成员 ID")
                role = {"参与": 1, "协助": 2, "核心": 3}[key]
                roles[uid] = {
                    "uid": uid,
                    "name": member["name"],
                    "role": max(role, roles.get(uid, {}).get("role", 0)),
                }
        else:
            raise ValueError(f"不支持修订字段：{key}")
    if not note:
        raise ValueError("修订必须提供 说明=修订理由")
    if roles is not None:
        if not roles:
            raise ValueError("至少需要一个参与者")
        payload["participants"] = list(roles.values())
        payload["unknown_participants"] = []
    if payload["kind"] in ("dispute", "discipline"):
        payload["positive"] = False
    return payload, note


async def handle_command(service, event, supplied=None):
    """Execute one authorized group command and return only public-safe replies.

    Args:
        service: Lifetime history service.
        event: Actual event used for identity, group ownership, and permissions.
        supplied: Optional framework-parsed argument; event text is authoritative.

    Returns:
        A public-safe Chinese reply string.
    """
    plugin, store = service.plugin, service.store
    umo = getattr(event, "unified_msg_origin", "")
    if not GROUP_PATTERN.fullmatch(umo):
        return "此命令只能在群内使用。"
    try:
        message = str(getattr(event, "message_str", "") or "")
        match = re.search(r"(?:^|\s)(" + "|".join(COMMANDS) + r")(?=\s|$)", message)
        if not match:
            return "无法识别史记命令。"
        command = match.group(1)
        privacy_request = command == "史记隐藏"
        if not privacy_request:
            scope_lists(plugin.config)
        if not await service.allowed(umo, recording=not privacy_request):
            return "本群长期记录已停用或不在允许范围；已有卷册仍可通过史记列表查看。"
        tail = message[match.end() :].strip() or str(supplied or "").strip()
        actor = str(event.get_sender_id() or "")
        if not actor:
            return "无法核实调用者身份。"
        admin = administrator(plugin, event)
        if (
            command
            in ("史记待审", "史记确认", "史记驳回", "史记合并", "史记修订", "史记进度")
            and not admin
        ):
            return "此命令仅本群管理员或 AstrBot 管理员可用。"
        if command == "群大事":
            days = 0 if tail in ("", "全部") else {"月": 30, "年": 365}.get(tail)
            if days is None:
                if not tail.isdigit() or int(tail) <= 0:
                    return "用法：群大事 [全部／月／年／天数]"
                days = min(36500, int(tail))
            facts = await asyncio.to_thread(
                store.events, umo, since=time.time() - days * 86400 if days else 0
            )
            if not facts:
                return "暂无已入史的大事。" + await asyncio.to_thread(
                    service.coverage, umo
                )
            tz = _history_tz(plugin)
            lines = ["本群大事记："]
            for fact in facts:
                names = "、".join(p["name"] for p in fact["participants"])
                date_label = {
                    "source": "见载于",
                    "recorded": "记录于（发生日期未明确）",
                    "declared": "发生于（人工记载）",
                }.get(fact.get("date_basis"), "见载于")
                lines.append(
                    f"#{fact['id']} {date_label}{datetime.fromtimestamp(fact['event_at'], tz):%Y-%m-%d} {fact['title']}（{names}）\n{fact['detail']}"
                )
                for update in fact.get("followups", []):
                    lines.append(
                        f"  续记 {datetime.fromtimestamp(update['event_at'], tz):%Y-%m-%d}：{update['detail']}"
                    )
            lines.append(await asyncio.to_thread(service.coverage, umo))
            return "\n".join(lines)
        if command == "人物等级":
            target = plugin._extract_at_sender(event) or (
                tail if tail.isdigit() else actor
            )
            if target != actor and plugin._cfg("bio_admin_only", True) and not admin:
                return "查询其他成员档案需要管理员权限。"

            def _load_grade_info():
                mem = store.member(umo, target)
                if not mem or mem["hidden"]:
                    return mem, None, None
                grd = store.grade(umo, target, service.rules())
                vis = {f["id"] for f in store.events(umo, uid=target)}
                return mem, grd, vis

            member, grade, visible = await asyncio.to_thread(_load_grade_info)
            if not member:
                return "尚未找到该成员档案。"
            if member["hidden"]:
                return "该成员档案已隐藏。"
            basis = [
                f"#{b['id']} {b['title']}：{b['points']} 分"
                for b in grade["basis"]
                if b["id"] in visible
            ]
            if len(basis) < len(grade["basis"]):
                basis.append("部分升级依据涉及已隐藏内容，不公开详情。")
            return (
                f"{member['name']}·{grade['grade']}\n累计贡献 {grade['points']} 分，{grade['events']} 件独立事件；"
                f"重大核心贡献 {grade['major_core']} 件，经历跨度 {grade['span_days']} 天。\n"
                + (
                    "\n".join(basis)
                    if basis
                    else "目前保留基础小记；发言量和在群时长不加分。"
                )
            )
        if command == "史记进度":
            day = datetime.now(_history_tz(plugin)).date().isoformat()

            def _load_progress_info():
                return (
                    store.job(umo),
                    service.coverage(umo),
                    store.quota(day),
                )

            job, cov, used_quota = await asyncio.to_thread(_load_progress_info)
            lines = [
                cov,
                f"今日全局抽取任务 {used_quota}/{plugin._cfg('history_daily_limit', 20)}；失败尝试也占额度。",
            ]
            if job:
                lines.append(
                    f"回溯水位 {job['back_cursor']}/{job['frozen_high']}；新增水位 {job['live_cursor']}；已处理素材片段 {job['processed']}。"
                )
                if job["back_offset"] or job["live_offset"]:
                    lines.append("有超长消息正在分段整理，尚未跳过剩余内容。")
                if job["error"]:
                    lines.append(job["error"])
                lines.extend(str(g) for g in json.loads(job["gaps"]))
            else:
                lines.append(
                    "尚未建立历史任务，等待 LivingMemory 就绪及下一次后台整理。"
                )
            return "\n".join(lines)
        if command == "史记待审":

            def _load_pending_and_corrections():
                return (
                    store.events(umo, status="pending", public=False),
                    store.corrections(umo),
                )

            facts, corrections = await asyncio.to_thread(_load_pending_and_corrections)
            if tail:
                if not tail.isdigit():
                    return "用法：史记待审 [事件ID]，审核详情私发给调用者。"
                facts = [f for f in facts if f["id"] == int(tail)]
            if not facts and not corrections:
                return "暂无待审事件或更正请求。"
            details = ["本群审核详情（仅发给调用者）：", umo]
            for fact in facts[:20]:
                details.append(
                    f"#{fact['id']} {fact['title']}\n{fact['detail']}\n重要度 {fact['importance']}，贡献={fact['positive']}，类型={fact['kind']}\n参与者："
                    + encode(fact["participants"])
                )
                for source in fact["sources"]:
                    details.append(f"来源 {source['id']}：{source.get('excerpt', '')}")
                if fact.get("unknown_participants"):
                    details.append(
                        "身份待核实：" + encode(fact["unknown_participants"])
                    )
            for request in corrections[:20]:
                details.append(
                    f"更正请求 {request['id']} → 事件#{request['event_id']}：{request['note']}"
                )
            private_umo = umo.split(":", 1)[0] + ":FriendMessage:" + actor
            ok = await deliver_text(
                plugin.context,
                None,
                private_umo,
                "\n".join(details),
                None,
                prefer_context=True,
                use_forward_nodes=False,
                max_chars=plugin._cfg("max_reply_chars", 1500),
            )
            if ok:
                return f"待审事件 {len(facts)} 件、更正请求 {len(corrections)} 条；本次详情已私发给你（最多各20条，可指定事件ID）。"
            return "审核详情私发失败，未在群内公开；请先确认机器人能向你发送私聊消息。"
        if command == "史记隐藏":
            if tail and not tail.isdigit():
                return "用法：史记隐藏（隐藏本人档案）或 史记隐藏 事件ID（隐藏涉及自己的事件）。"
            await asyncio.to_thread(store.observe, umo, actor, event.get_sender_name())
            await asyncio.to_thread(store.hide, umo, actor, int(tail) if tail else None)
            service.cancel_operations(umo)
            return "相关内容已隐藏，相关文章、图片和导出缓存已失效；共同事实的修订可提交管理员审核。"
        lock = service.fact_locks.setdefault(umo, asyncio.Lock())
        async with lock:
            if not await service.allowed(umo):
                return "本群记录范围已变化，本次操作已取消。"
            if command == "记一笔":
                if not tail:
                    return "用法：记一笔 标题｜经过与结果｜日期=YYYY-MM-DD（日期可省略，可 @实际参与者）；普通成员提交后等待审核。"
                fields = re.split(r"[｜|]", tail)
                event_at, date_basis = time.time(), "recorded"
                if len(fields) > 1 and re.match(r"^\s*日期[=＝]", fields[-1]):
                    date_text = re.split(r"[=＝]", fields.pop(), maxsplit=1)[1].strip()
                    tz = _history_tz(plugin)
                    try:
                        date = datetime.strptime(date_text, "%Y-%m-%d").replace(
                            tzinfo=tz
                        )
                    except ValueError:
                        return "日期格式应为 YYYY-MM-DD。"
                    if (
                        date.year < 1970
                        or date.date()
                        > datetime.now(tz).date()
                    ):
                        return "日期必须是1970年之后且不能晚于今天。"
                    event_at, date_basis = date.timestamp(), "declared"
                title, detail = (
                    fields[0][:200],
                    ("｜".join(fields[1:]) if len(fields) > 1 else fields[0])[:3000],
                )
                pids = participant_ids(event, actor)

                def _resolve_participants():
                    parts, unk = [], []
                    for uid in pids:
                        member = store.member(umo, uid)
                        if not member:
                            if uid != actor:
                                unk.append(uid)
                                continue
                            store.observe(umo, uid, event.get_sender_name())
                            member = store.member(umo, uid)
                        parts.append({"uid": uid, "name": member["name"], "role": 1})
                    return parts, unk

                participants, unknown = await asyncio.to_thread(_resolve_participants)
                risk = bool(unknown) or any(
                    word in title + detail for word in RISK_WORDS
                )
                msg = getattr(event, "message_obj", None)
                source_id = "manual:" + str(
                    getattr(msg, "message_id", "")
                    or hashlib.sha256(
                        encode([umo, actor, tail, int(time.time() // 60)]).encode()
                    ).hexdigest()
                )
                fact = {
                    "title": title,
                    "detail": detail,
                    "kind": "turning_point",
                    "importance": 1,
                    "positive": not risk,
                    "risk": risk,
                    "ongoing": False,
                    "participants": participants,
                    "unknown_participants": unknown,
                    "event_at": event_at,
                    "date_basis": date_basis,
                    "followups": [],
                    "sources": [
                        {
                            "id": source_id,
                            "uid": actor,
                            "hash": hashlib.sha256(tail.encode()).hexdigest(),
                            "excerpt": tail[:400],
                        }
                    ],
                    "fingerprint": hashlib.sha256(
                        encode([umo, source_id]).encode()
                    ).hexdigest(),
                }
                event_id = await asyncio.to_thread(
                    store.add_event,
                    umo,
                    fact,
                    status="approved" if admin and not risk else "pending",
                    actor=actor,
                )
                return f"已记为事件 #{event_id}。" + (
                    "管理员提交已确认入史。"
                    if admin and not risk
                    else "等待管理员审核，确认前不计入贡献等级。"
                )
            args = tail.split(maxsplit=1)
            if not args or not args[0].isdigit():
                return f"用法：{command} 事件ID" + (
                    " 内容" if command in ("史记更正", "史记修订") else ""
                )
            event_id = int(args[0])
            rest = args[1] if len(args) > 1 else ""
            if command == "史记更正":
                if not rest:
                    return "请在事件ID后填写建议更正的内容。"
                request_id = await asyncio.to_thread(
                    store.correction, umo, event_id, actor, rest
                )
                return f"更正请求 #{request_id} 已提交管理员；正式事实尚未修改。"
            if command in ("史记确认", "史记驳回"):
                await asyncio.to_thread(
                    store.review,
                    umo,
                    event_id,
                    "approve" if command == "史记确认" else "reject",
                    actor,
                    note=rest or command,
                )
                return f"事件 #{event_id} 已" + (
                    "确认入史，贡献等级已重新计算。"
                    if command == "史记确认"
                    else "驳回，相关贡献已移除。"
                )
            if command == "史记合并":
                if not rest.isdigit():
                    return "用法：史记合并 保留事件ID 被合并事件ID"
                await asyncio.to_thread(
                    store.review, umo, int(rest), "merge", actor, target=event_id
                )
                return f"事件 #{rest} 已并入 #{event_id}，贡献按同一事件计算。"
            if command == "史记修订":

                def _prepare_revision():
                    fact_row = store.event(umo, event_id, public=False)
                    if not fact_row:
                        return None, None
                    return revise_payload(store, umo, fact_row, rest)

                payload, note = await asyncio.to_thread(_prepare_revision)
                if payload is None:
                    return "本群没有该事件。"
                await asyncio.to_thread(
                    store.review,
                    umo,
                    event_id,
                    "revise",
                    actor,
                    note=note,
                    payload=payload,
                )
                return f"事件 #{event_id} 已修订，旧版本保留审计，相关文章缓存已失效。"
            return "无法识别史记操作。"
    except (ValueError, RuntimeError) as exc:
        return str(exc)


async def command_reply(plugin, event, supplied=None):
    """Yield length-bounded replies from the command dispatcher.

    Args:
        plugin: Owning plugin.
        event: Invoking event.
        supplied: Optional parsed argument.

    Yields:
        Public-safe framework results.
    """
    umo = getattr(event, "unified_msg_origin", "")
    guarded = bool(
        re.search(
            r"(?:^|\s)(群大事|人物等级)(?=\s|$)", str(getattr(event, "message_str", ""))
        )
    )
    revision = (
        await asyncio.to_thread(plugin.history.store.revision, umo)
        if GROUP_PATTERN.fullmatch(umo)
        else 0
    )
    try:
        reply = await asyncio.wait_for(
            handle_command(plugin.history, event, supplied),
            timeout=float(plugin._cfg("generation_timeout_sec", 180)),
        )
    except asyncio.TimeoutError:
        reply = "史记操作超时，未完成的后台整理会保留进度。"
    except sqlite3.Error:
        logger.error("Lifetime command storage operation failed", exc_info=True)
        reply = "档案存储暂不可用，请管理员检查日志；未完成的事务不会写入一半。"
    for part in split_text(reply, plugin._cfg("max_reply_chars", 1500)):
        if (
            guarded
            and await asyncio.to_thread(plugin.history.store.revision, umo) != revision
        ):
            yield event.plain_result("事实或隐私设置已更新，请重新查询。")
            return
        yield event.plain_result(part)
