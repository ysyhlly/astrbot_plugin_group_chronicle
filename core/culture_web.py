"""Host-authenticated culture administration; no standalone server or token."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

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
            for endpoint, handler, methods in (
                ("culture_overview", self.overview, ["GET"]),
                ("culture", self.handle, ["POST"]),
            ):
                path = "/astrbot_plugin_group_chronicle/" + endpoint
                register(path, handler, methods, "群文化沿革")
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

    async def authorize(self):
        from astrbot.api.web import request
        from astrbot.dashboard.api.auth import require_dashboard_user

        raw = getattr(request, "_request", None)
        if raw is None:
            raise PermissionError("需要 Dashboard 管理员登录")
        username = await require_dashboard_user(raw)
        expected = self.plugin.context.get_config().get("dashboard", {}).get("username")
        if not username or (expected and username != expected):
            raise PermissionError("需要 Dashboard 管理员登录")
        return "human:" + str(username)

    async def respond(self, callback, body=False):
        from astrbot.api.web import json_response, request

        try:
            actor = await self.authorize()
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
        return await self.respond(self.overview_data)

    async def overview_data(self, body=None, actor=""):
        groups = []
        for umo in self.service.history.store.groups():
            if await self.service.history.allowed(umo):
                group = self.service.history.store.group(umo)
                groups.append({"scope": umo, "name": group["name"]})
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
            "daily_limit": self.plugin._cfg("history_daily_limit", 20),
        }

    async def handle(self):
        return await self.respond(self.dispatch, body=True)

    async def inspect_entry(self, umo, item):
        if self.service.blocked(umo, item):
            return {
                "id": item["id"],
                "revision": item["revision"],
                "status": "hidden",
                "name": "已隐藏",
                "unavailable": True,
            }
        if not await self.service.valid_sources(umo, item):
            return {
                "id": item["id"],
                "revision": item["revision"],
                "status": item["status"],
                "name": "证据不可用",
                "unavailable": True,
                "unknowns": ["原文缺失、已撤回或来源已切换，暂停展示解释"],
            }
        return dict(
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
                or self.service.store.state(umo)["paused"]
            ):
                raise ValueError("当前群文化未启用或已暂停")
            lock = self.service.history.cycle_lock
            if lock.locked():
                raise ValueError("后台正在整理，请稍后再验证")
            async with lock:
                key = "history_daily_limit"
                day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
                before = self.service.history.store.quota(day)
                steps = []
                try:
                    for _ in range(batches):
                        advanced = await self.service.step(
                            umo, "back", verification_extra_quota=extra
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
                    after = self.service.history.store.quota(day)
                    record = {
                        "steps": steps,
                        "extra_quota": extra,
                        "used_before": before,
                        "used_after": after,
                        "restored_limit": self.plugin.config.get(key, 20),
                    }
                    with self.service.history.store.connection(write=True) as db:
                        db.execute(
                            "INSERT INTO culture_revisions(umo,entry_id,before_data,actor,action,note,created) VALUES(?,0,'{}',?,'verification',?,?)",
                            (umo, actor, encode(record), time.time()),
                        )
                return record
        if op == "progress":
            job = self.service.store.job(umo)
            if job:
                job = {
                    key: value
                    for key, value in job.items()
                    if not key.endswith(("_fragment_id", "_fragment_hash"))
                }
            return {
                "state": self.service.store.state(umo),
                "job": job,
                "error": self.service.errors.get(umo, ""),
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
            token = self.service.token(umo)
            items, total = self.service.store.entries(
                umo, status=status, query=query, limit=20, offset=(page - 1) * 20
            )
            entries = [await self.inspect_entry(umo, e) for e in items]
            if token != self.service.token(
                umo
            ) or not await self.service.history.allowed(umo):
                raise ValueError("资料已更新，请刷新")
            return {"entries": entries, "total": total, "page": page}
        if op == "detail":
            token = self.service.token(umo)
            item = self.service.store.entry(umo, body.get("id"))
            if not item:
                raise ValueError("词条不存在")
            detail = await self.inspect_entry(umo, item)
            revisions = []
            if not detail.get("unavailable"):
                for r in self.service.store.revisions(umo, item["id"]):
                    before = self.service.store.unpack(json.loads(r["before_data"]))
                    old = await self.inspect_entry(umo, before)
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
            if token != self.service.token(
                umo
            ) or not await self.service.history.allowed(umo):
                raise ValueError("资料已更新，请刷新")
            return {"entry": detail, "revisions": revisions}
        if op == "action":
            return await self.service.action(
                umo,
                body.get("action"),
                actor,
                admin=True,
                entry_id=body.get("id"),
                revision=body.get("revision"),
                note=body.get("note", ""),
                patch=body.get("patch"),
                target_id=body.get("target_id"),
                target_revision=body.get("target_revision"),
            )
        raise ValueError("未知文化操作")
