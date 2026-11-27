"""Host-authenticated culture administration; no standalone server or token."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime

from . import lm_bridge
from .archive_store import encode


class CultureWeb:
    def __init__(self, plugin):
        self.plugin = plugin
        self.service = plugin.culture
        self.routes = []

    def register(self):
        register = getattr(self.plugin.context, "register_web_api", None)
        if callable(register):
            if __package__ == "core":
                from series_workspace import register_panel_api
            else:
                from ..series_workspace import register_panel_api

            for endpoint, handler, methods in (
                ("culture_overview", self.overview, ["GET"]),
                ("culture", self.handle, ["POST"]),
            ):
                path = "/astrbot_plugin_group_chronicle/" + endpoint
                register_panel_api(self.plugin, register, path, handler, methods, "群文化沿革")
                self.routes.append((path, handler))

    def close(self):
        routes = getattr(self.plugin.context, "registered_web_apis", None)
        if isinstance(routes, list):
            routes[:] = [
                r
                for r in routes
                if not any(
                    handler in (r if isinstance(r, (tuple, list)) else ())
                    for _, handler in self.routes
                )
            ]

    async def authorize(self, *, check_csrf: bool = False):
        from astrbot.api.web import request

        if __package__ == "core":
            from series_panels import require_series_admin
        else:
            from ..series_panels import require_series_admin

        username = await require_series_admin(
            self.plugin.context, request, check_csrf=check_csrf
        )
        return "human:" + str(username)

    async def respond(self, callback, body=False, *, check_csrf=None):
        from astrbot.api.web import json_response, request

        try:
            enforce_csrf = bool(body) if check_csrf is None else bool(check_csrf)
            actor = await self.authorize(check_csrf=enforce_csrf)
            if self.plugin._terminating:
                raise ValueError("插件已卸载")
            data = {}
            if body:
                raw = await request._request.body()
                if len(raw) > 16384:
                    raise ValueError("请求过大")
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("请求必须是对象")
            result = await callback(data, actor)
            return json_response({"status": "ok", "data": result})
        except PermissionError as exc:
            return json_response(
                {"status": "error", "message": str(exc)}, status_code=403
            )
        except (ValueError, TypeError, KeyError) as exc:
            return json_response(
                {"status": "error", "message": str(exc)}, status_code=400
            )
        except Exception:
            return json_response(
                {
                    "status": "error",
                    "message": "文化服务暂不可用，请检查记忆来源与运行日志",
                },
                status_code=503,
            )

    async def overview(self):
        return await self.respond(self.overview_data, check_csrf=False)

    def _quota_snapshot(self):
        day = datetime.now(self.service.history._series_tz()).date().isoformat()
        raw_limit = self.plugin._cfg("history_daily_limit", 20)
        daily_limit = max(0, int(20 if raw_limit is None else raw_limit))
        breakdown = self.service.history.store.quota_breakdown(day)
        reserved_history = daily_limit // 2 if daily_limit >= 2 else 0
        culture_limit = max(0, daily_limit - reserved_history)
        total_used = int(breakdown.get("total", 0))
        history_used = int(breakdown.get("history", 0))
        culture_used = int(breakdown.get("culture", 0))
        total_remaining = max(0, daily_limit - total_used)
        culture_remaining = min(total_remaining, max(0, culture_limit - culture_used))
        return {
            "day": day,
            "daily_limit": daily_limit,
            "total_used": total_used,
            "total_remaining": total_remaining,
            "history_used": history_used,
            "history_remaining": total_remaining,
            "history_reserved": reserved_history,
            "culture_used": culture_used,
            "culture_limit": culture_limit,
            "culture_remaining": culture_remaining,
        }

    async def overview_data(self, body=None, actor=""):
        raw_groups = await asyncio.to_thread(self.service.history.store.groups)
        allowed_umos = [
            umo for umo in raw_groups if await self.service.history.allowed(umo)
        ]
        groups = await asyncio.to_thread(
            lambda: [
                {"scope": umo, "name": self.service.history.store.group(umo)["name"]}
                for umo in allowed_umos
            ]
        )
        quota = await asyncio.to_thread(self._quota_snapshot)
        try:
            owner = lm_bridge.detect_lm(self.plugin.context)
            memory = (
                "已连接"
                if owner
                and lm_bridge.history_connection(self.plugin.context) is not None
                else "未就绪"
            )
        except RuntimeError as exc:
            memory = str(exc)
        getter = getattr(self.plugin.context, "get_registered_star", None)
        star = getter("astrbot_plugin_dynamics_learning") if callable(getter) else None
        learning = bool(
            star
            and getattr(star, "activated", True)
            and callable(
                getattr(getattr(star, "star_cls", None), "get_culture_jargon_v1", None)
            )
        )
        return {
            "enabled": bool(self.plugin._cfg("culture_enabled", False)),
            "groups": groups,
            "memory": memory,
            "learning": learning,
            "daily_limit": quota["daily_limit"],
            "quota": quota,
        }

    async def handle(self):
        return await self.respond(self.dispatch, body=True, check_csrf=True)

    async def inspect_entries(self, umo, items):
        if not items:
            return []
        snapshot = await asyncio.to_thread(
            self.service._privacy_snapshot, umo, {i.get("source") for i in items}
        )
        blocked_ids = set()
        non_blocked = []
        for item in items:
            if self.service._is_item_blocked(item, snapshot):
                blocked_ids.add(id(item))
            else:
                non_blocked.append(item)
        flags = await self.service._batch_valid_sources(umo, non_blocked)
        valid_ids = {id(item) for item, ok in zip(non_blocked, flags) if ok}
        result = []
        for item in items:
            if id(item) in blocked_ids:
                result.append(
                    {
                        "id": item["id"],
                        "revision": item["revision"],
                        "status": "hidden",
                        "name": "已隐藏",
                        "unavailable": True,
                        "can_restore": bool(item["status"] == "hidden"),
                    }
                )
            elif id(item) not in valid_ids:
                result.append(
                    {
                        "id": item["id"],
                        "revision": item["revision"],
                        "status": item["status"],
                        "name": "证据不可用",
                        "unavailable": True,
                        "unknowns": ["原文缺失、已撤回或来源已切换，暂停展示解释"],
                    }
                )
            else:
                result.append(
                    dict(
                        self.service.public_item(item),
                        status=item["status"],
                        kind=item["kind"],
                        proposed_origin=item.get("origin"),
                        subjects=item["subjects"],
                        risk=item["risk"],
                        conflict=item["conflict"],
                        merged_into=item.get("merged_into"),
                        review_note=item.get("review_note", ""),
                    )
                )
        return result

    async def inspect_entry(self, umo, item):
        return (await self.inspect_entries(umo, [item]))[0]

    async def dispatch(self, body, actor):
        if set(body) - {
            "op",
            "scope",
            "query",
            "status",
            "page",
            "id",
            "ids",
            "revision",
            "action",
            "note",
            "patch",
            "target_id",
            "target_revision",
            "batches",
            "verification_extra_quota",
            "event_id",
            "target_event_id",
            "uid",
            "correction_id",
            "report_id",
            "compare_id",
        }:
            raise ValueError("未知请求字段")
        umo = body.get("scope")
        if not isinstance(umo, str) or not await self.service.history.allowed(umo):
            raise PermissionError("当前群不在允许范围")
        op = body.get("op")
        if op == "run_once":
            batches = body.get("batches", 1)
            extra = body.get("verification_extra_quota", 0)
            if (
                type(batches) is not int
                or not 1 <= batches <= 2
                or type(extra) is not int
                or not 0 <= extra <= 2
            ):
                raise ValueError("单次最多验证2批、临时增加2次额度")
            if (
                not await self.service.allowed(umo)
                or (await asyncio.to_thread(self.service.store.state, umo))["paused"]
            ):
                raise ValueError("当前群文化未启用或已暂停")
            lock = self.service.history.cycle_lock
            if lock.locked():
                raise ValueError("后台正在整理，请稍后再验证")
            async with lock:
                key = "history_daily_limit"
                day = datetime.now(self.service.history._series_tz()).date().isoformat()
                before = await asyncio.to_thread(self.service.history.store.quota, day)
                steps = []
                try:
                    for _ in range(batches):
                        advanced = await self.service.step(
                            umo, "back", verification_extra_quota=extra
                        )
                        if not advanced and not self.service.errors.get(umo):
                            advanced = await self.service.step(
                                umo, "live", verification_extra_quota=extra
                            )
                        steps.append(
                            {
                                "advanced": advanced,
                                "error": self.service.errors.get(umo, ""),
                            }
                        )
                        if not advanced:
                            break
                finally:

                    def _record_verification():
                        after = self.service.history.store.quota(day)
                        quota_now = self._quota_snapshot()
                        rec = {
                            "steps": steps,
                            "extra_quota": extra,
                            "used_before": before,
                            "used_after": after,
                            "restored_limit": self.plugin.config.get(key, 20),
                            "quota": quota_now,
                        }
                        with self.service.history.store.connection(write=True) as db:
                            db.execute(
                                "INSERT INTO culture_revisions(umo,entry_id,before_data,actor,action,note,created) VALUES(?,0,'{}',?,'verification',?,?)",
                                (umo, actor, encode(rec), time.time()),
                            )
                        return rec

                    record = await asyncio.to_thread(_record_verification)
                return record
        if op == "progress":

            def _load_progress():
                return (
                    self.service.store.job(umo),
                    self.service.store.state(umo),
                    self._quota_snapshot(),
                )

            job, state, quota = await asyncio.to_thread(_load_progress)
            if job:
                job = {
                    key: value
                    for key, value in job.items()
                    if not key.endswith(("_fragment_id", "_fragment_hash"))
                }
            return {
                "state": state,
                "job": job,
                "error": self.service.errors.get(umo, ""),
                "quota": quota,
            }
        if op == "list":
            status = body.get("status") or None
            if status not in {
                None,
                "approved",
                "pending",
                "disputed",
                "rejected",
                "hidden",
                "merged",
            }:
                raise ValueError("状态无效")
            page = body.get("page", 1)
            query = body.get("query", "")
            if (
                type(page) is not int
                or not 1 <= page <= 100000
                or not isinstance(query, str)
                or len(query) > 160
            ):
                raise ValueError("分页或查询参数无效")

            def _load_list():
                token = self.service.token(umo)
                all_items, _ = self.service.store.entries(
                    umo, status=None, query="", limit=1000000, offset=0
                )
                snapshot = self.service._privacy_snapshot(
                    umo, {i.get("source") for i in all_items}
                )
                q = query.strip()
                matched = []
                for item in all_items:
                    blocked = self.service._is_item_blocked(item, snapshot)
                    eff_status = "hidden" if blocked else item["status"]
                    if status and eff_status != status:
                        continue
                    if q:
                        if blocked:
                            continue
                        haystack = [
                            item["name"],
                            item["meaning"],
                            *(a["name"] for a in item["aliases"]),
                        ]
                        if (
                            not any(q.casefold() in s.casefold() for s in haystack)
                            and q != str(item["id"])
                        ):
                            continue
                    matched.append(item)
                start = (page - 1) * 20
                return token, (matched[start : start + 20], len(matched))

            token, (items, total) = await asyncio.to_thread(_load_list)
            entries = await self.inspect_entries(umo, items)
            if await asyncio.to_thread(
                self.service.token, umo
            ) != token or not await self.service.history.allowed(umo):
                raise ValueError("资料已更新，请刷新")
            return {"entries": entries, "total": total, "page": page}
        if op == "detail":

            def _load_detail():
                return self.service.token(umo), self.service.store.entry(
                    umo, body.get("id")
                )

            token, item = await asyncio.to_thread(_load_detail)
            if not item:
                raise ValueError("词条不存在")
            detail = await self.inspect_entry(umo, item)
            revisions = []
            if not detail.get("unavailable"):
                raw_revs = await asyncio.to_thread(
                    self.service.store.revisions, umo, item["id"]
                )
                befores = [
                    self.service.store.unpack(json.loads(r["before_data"]))
                    for r in raw_revs
                ]
                inspected_befores = await self.inspect_entries(umo, befores)
                for r, old in zip(raw_revs, inspected_befores):
                    revisions.append(
                        {
                            "id": r["id"],
                            "actor": r["actor"],
                            "action": r["action"],
                            "note": r["note"],
                            "created": r["created"],
                            "before": old,
                        }
                    )
            if await asyncio.to_thread(
                self.service.token, umo
            ) != token or not await self.service.history.allowed(umo):
                raise ValueError("资料已更新，请刷新")
            return {"entry": detail, "revisions": revisions}
        if op == "action":
            raw_note = body.get("note", "")
            if raw_note is None:
                raw_note = ""
            if not isinstance(raw_note, str):
                raise ValueError("请填写不超过1000字的处理理由")
            return await self.service.action(
                umo,
                body.get("action"),
                actor,
                admin=True,
                entry_id=body.get("id"),
                revision=body.get("revision"),
                note=raw_note,
                patch=body.get("patch"),
                target_id=body.get("target_id"),
                target_revision=body.get("target_revision"),
            )
        if op == "batch_action":
            action_name = body.get("action")
            if action_name not in {"approve", "reject", "hide"}:
                raise ValueError("批量操作仅支持 approve / reject / hide")
            raw_ids = body.get("ids")
            if (
                not isinstance(raw_ids, list)
                or not raw_ids
                or len(raw_ids) > 50
                or any(type(i) is not int or i <= 0 for i in raw_ids)
            ):
                raise ValueError("请提供 1~50 条有效的词条 ID")
            raw_note = body.get("note", "")
            if not isinstance(raw_note, str) or not raw_note.strip() or len(raw_note) > 1000:
                raise ValueError("请填写不超过1000字的批量处理理由")
            succeeded = []
            failed = []
            for eid in list(dict.fromkeys(raw_ids)):
                try:
                    entry = await asyncio.to_thread(self.service.store.entry, umo, eid)
                    if not entry:
                        failed.append({"id": eid, "error": "词条不存在"})
                        continue
                    res = await self.service.action(
                        umo,
                        action_name,
                        actor,
                        admin=True,
                        entry_id=entry["id"],
                        revision=entry["revision"],
                        note=raw_note.strip(),
                    )
                    succeeded.append(res)
                except (ValueError, PermissionError) as exc:
                    failed.append({"id": eid, "error": str(exc)})
            return {
                "ok": True,
                "action": action_name,
                "succeeded": succeeded,
                "failed": failed,
            }
        if op == "editions":
            report_id = body.get("report_id")
            compare_id = body.get("compare_id")

            def _load_editions():
                reports = self.service.history.store.reports(umo, include_bios=False)
                out = []
                by_id = {}
                for r in reports:
                    text = str(r.get("chronicle_text") or "")
                    period_days = int(r.get("period_days") or 0)
                    period_label = (
                        {7: "周卷", 30: "月卷", 90: "季卷", 365: "年卷"}.get(
                            period_days, f"{period_days}天卷"
                        )
                        if period_days
                        else "周卷"
                    )
                    summary = str(r.get("summary") or "").strip()
                    if not summary:
                        for ln in text.splitlines():
                            s = ln.strip().lstrip("#>-*• ").strip()
                            if s and not s.startswith("覆盖 ") and not s.startswith("——"):
                                summary = s[:90]
                                break
                    item = {
                        "report_id": r.get("report_id"),
                        "iso_year": r.get("iso_year"),
                        "iso_week": r.get("iso_week"),
                        "period_days": period_days or 7,
                        "period_label": period_label,
                        "version": int(r.get("version") or 1),
                        "group_name": str(r.get("group_name") or "群聊"),
                        "created_at": float(r.get("created_at") or 0),
                        "event_count": int(r.get("event_count") or 0),
                        "msg_count": int(r.get("msg_count") or 0),
                        "char_count": len(text),
                        "summary": summary,
                        "event_ids": list(r.get("event_ids") or []),
                        "artwork_path": str(r.get("artwork_path") or ""),
                        "keepsake_deep_link": str(r.get("keepsake_deep_link") or ""),
                        "chronicle_text": text,
                    }
                    out.append(item)
                    by_id[item["report_id"]] = item
                comparison = None
                if report_id is not None and compare_id is not None:
                    left = by_id.get(report_id)
                    right = by_id.get(compare_id)
                    if left and right:
                        left_ids = set(left.get("event_ids") or [])
                        right_ids = set(right.get("event_ids") or [])
                        comparison = {
                            "left_id": left["report_id"],
                            "right_id": right["report_id"],
                            "char_diff": left["char_count"] - right["char_count"],
                            "event_diff": left["event_count"] - right["event_count"],
                            "msg_diff": left["msg_count"] - right["msg_count"],
                            "added_event_ids": sorted(left_ids - right_ids),
                            "removed_event_ids": sorted(right_ids - left_ids),
                        }
                return out, comparison

            editions, comparison = await asyncio.to_thread(_load_editions)
            return {
                "editions": editions[:50],
                "total": len(editions),
                "comparison": comparison,
            }
        if op == "events_list":
            status = body.get("status") or None
            if status not in {None, "approved", "pending", "rejected", "merged"}:
                raise ValueError("事件状态无效")

            def _load_events():
                store = self.service.history.store
                all_events = store.events(umo, status=None, public=False)
                public_ids = {
                    e["id"]
                    for e in store.events(umo, status=None, public=True)
                }
                explicit_hidden_ids = set(store.hidden_event_ids(umo))
                counts = {
                    "approved": 0,
                    "pending": 0,
                    "rejected": 0,
                    "merged": 0,
                }
                rows_out = []
                for ev in reversed(all_events):
                    st = ev.get("status", "pending")
                    if st in counts:
                        counts[st] += 1
                    if status and st != status:
                        continue
                    is_hidden = ev["id"] not in public_ids
                    is_explicit_hidden = ev["id"] in explicit_hidden_ids
                    if is_hidden:
                        rows_out.append(
                            {
                                "id": ev["id"],
                                "status": st,
                                "hidden": True,
                                "can_restore": is_explicit_hidden,
                                "title": "已隐藏（隐私保护中）",
                                "detail": "该事件因隐私隐藏或涉及已隐藏成员，暂不展示详情。",
                                "kind": ev.get("kind", ""),
                                "importance": int(ev.get("importance", 1)),
                                "event_at": float(ev.get("event_at") or 0),
                                "participants": [],
                                "risk": bool(ev.get("risk", False)),
                            }
                        )
                    else:
                        rows_out.append(
                            {
                                "id": ev["id"],
                                "status": st,
                                "hidden": False,
                                "can_restore": False,
                                "title": ev.get("title", ""),
                                "detail": ev.get("detail", ""),
                                "kind": ev.get("kind", ""),
                                "importance": int(ev.get("importance", 1)),
                                "positive": bool(ev.get("positive", True)),
                                "risk": bool(ev.get("risk", False)),
                                "event_at": float(ev.get("event_at") or 0),
                                "date_basis": ev.get("date_basis", "source"),
                                "participants": ev.get("participants", []),
                                "unknown_participants": ev.get("unknown_participants", []),
                                "followups": ev.get("followups", []),
                                "review_note": ev.get("review_note", ""),
                            }
                        )
                corrections = store.corrections(umo)
                hidden_members = [
                    {
                        "uid": m["uid"],
                        "name": m["name"],
                        "first_seen": float(m.get("first_seen") or 0),
                    }
                    for m in store.hidden(umo)
                ]
                counts["corrections"] = len(corrections)
                counts["hidden_members"] = len(hidden_members)
                counts["hidden_events"] = len(explicit_hidden_ids)
                return {
                    "events": rows_out[:100],
                    "total": len(rows_out),
                    "corrections": corrections[:50],
                    "hidden_members": hidden_members,
                    "hidden_event_ids": sorted(explicit_hidden_ids, reverse=True),
                    "counts": counts,
                }

            return await asyncio.to_thread(_load_events)
        if op == "event_action":
            action_name = body.get("action")
            raw_note = body.get("note", "")
            note = (
                raw_note.strip()[:1000]
                if isinstance(raw_note, str) and raw_note.strip()
                else f"管理员面板:{action_name}"
            )
            store = self.service.history.store
            if action_name in {"approve", "reject", "hide"}:
                raw_ids = body.get("ids")
                if raw_ids is None and body.get("event_id") is not None:
                    raw_ids = [body.get("event_id")]
                if (
                    not isinstance(raw_ids, list)
                    or not raw_ids
                    or len(raw_ids) > 50
                    or any(type(i) is not int or i <= 0 for i in raw_ids)
                ):
                    raise ValueError("请提供 1~50 条有效的事件 ID")

                def _run_event_batch():
                    ok_ids = []
                    errs = []
                    for eid in list(dict.fromkeys(raw_ids)):
                        try:
                            if action_name == "hide":
                                store.admin_hide_event(umo, eid, actor, note=note)
                            else:
                                store.review(umo, eid, action_name, actor, note=note)
                            ok_ids.append(eid)
                        except (ValueError, RuntimeError) as exc:
                            errs.append({"id": eid, "error": str(exc)})
                    return ok_ids, errs

                ok_ids, errs = await asyncio.to_thread(_run_event_batch)
                self.service.history.cancel_operations(umo)
                if action_name == "approve" and ok_ids:
                    sched = getattr(
                        self.service.plugin, "_schedule_milestone_keepsake", None
                    )
                    if callable(sched):
                        for eid in ok_ids:
                            ev_row = await asyncio.to_thread(
                                store.event, umo, eid, public=True
                            )
                            if ev_row and int(ev_row.get("importance", 1)) >= 3:
                                sched(
                                    umo,
                                    title=str(ev_row.get("title") or f"重大事件 #{eid}"),
                                    summary=str(ev_row.get("detail") or "")[:260],
                                    kind="major_event",
                                    tab="events",
                                    event_id=eid,
                                    importance=int(ev_row.get("importance", 3)),
                                )
                return {"ok": True, "action": action_name, "succeeded": ok_ids, "failed": errs}
            if action_name == "merge":
                eid = body.get("event_id")
                tid = body.get("target_event_id")
                if type(eid) is not int or type(tid) is not int or eid <= 0 or tid <= 0:
                    raise ValueError("请提供有效的保留事件 ID 与被合并事件 ID")
                await asyncio.to_thread(
                    store.review, umo, eid, "merge", actor, note=note, target=tid
                )
                self.service.history.cancel_operations(umo)
                return {"ok": True, "action": "merge", "event_id": eid, "target_event_id": tid}
            if action_name == "restore_event":
                eid = body.get("event_id")
                if type(eid) is not int or eid <= 0:
                    raise ValueError("请提供有效的事件 ID")
                res = await asyncio.to_thread(
                    store.restore, umo, actor, eid, actor=actor, admin=True, note=note
                )
                self.service.history.cancel_operations(umo)
                return {"ok": True, "action": "restore_event", **res}
            if action_name == "restore_member":
                uid = str(body.get("uid") or "").strip()
                if not uid:
                    raise ValueError("请提供要恢复的成员 ID")
                res = await asyncio.to_thread(
                    store.restore, umo, uid, None, actor=actor, admin=True, note=note
                )
                self.service.history.cancel_operations(umo)
                return {"ok": True, "action": "restore_member", **res}
            if action_name == "resolve_correction":
                cid = body.get("correction_id")
                if type(cid) is not int or cid <= 0:
                    raise ValueError("请提供有效的更正请求 ID")
                await asyncio.to_thread(
                    store.resolve_correction, umo, cid, actor, note=note
                )
                return {"ok": True, "action": "resolve_correction", "correction_id": cid}
            raise ValueError("未知事件操作")
        if op == "members_wall":

            def _load_members():
                store = self.service.history.store
                rules = self.service.history.rules()
                visible_members = store.list_members(umo, include_hidden=False)
                hidden_count = len(store.hidden(umo))
                # Map latest cached biography per member_id
                bio_map: dict[str, str] = {}
                for rep in store.reports(umo, include_bios=True):
                    if rep.get("report_kind") == "biography":
                        mid = str(rep.get("member_id") or "")
                        if mid and mid not in bio_map:
                            bio_map[mid] = str(rep.get("chronicle_text") or "")[:600]
                rows_out = []
                for m in visible_members:
                    uid = str(m["uid"])
                    grd = store.grade(umo, uid, rules)
                    rows_out.append(
                        {
                            "uid": uid,
                            "name": m["name"],
                            "aliases": list(m.get("aliases") or [])[:6],
                            "first_seen": float(m.get("first_seen") or 0),
                            "last_seen": float(m.get("last_seen") or 0),
                            "grade": grd["grade"],
                            "points": int(grd["points"]),
                            "events": int(grd["events"]),
                            "major_core": int(grd["major_core"]),
                            "span_days": float(grd["span_days"]),
                            "biography_excerpt": bio_map.get(uid, ""),
                        }
                    )
                rows_out.sort(
                    key=lambda x: (-x["points"], -x["events"], x["first_seen"], x["uid"])
                )
                return {
                    "members": rows_out[:120],
                    "total": len(rows_out),
                    "hidden_count": hidden_count,
                }

            return await asyncio.to_thread(_load_members)
        raise ValueError("未知文化操作")
