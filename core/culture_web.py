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
            "revision",
            "action",
            "note",
            "patch",
            "target_id",
            "target_revision",
            "batches",
            "verification_extra_quota",
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
        raise ValueError("未知文化操作")
