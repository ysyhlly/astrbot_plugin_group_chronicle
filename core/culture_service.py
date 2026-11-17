"""Bounded background extraction, scoped reads, moderation and delivery guards."""

from __future__ import annotations

import asyncio
import inspect
import re
from datetime import datetime, timedelta, timezone

from . import lm_bridge
from .archive_store import encode, private_text
from .culture_contract import PROMPT, digest, validate
from .culture_store import CultureStore
from .lifetime import (
    fragment_checkpoint,
    pack_messages,
    resume_offset,
    tracked_operation,
)
from .llm_call import call_llm


class CultureService:
    def __init__(self, plugin):
        self.plugin = plugin
        self.history = plugin.history
        self.store = CultureStore(self.history.store)
        self.operations = self.history.operations
        self.locks = {}
        self.errors = {}

    async def allowed(self, umo, *, recording=True):
        return bool(
            self.plugin._cfg("culture_enabled", False)
            and await self.history.allowed(umo, recording=recording)
        )

    def token(self, umo):
        return (self.store.state(umo)["revision"], self.history.privacy_key(umo))

    def blocked(self, umo, item):
        excluded = self.history.store.excluded_sources(umo) | self.store.excluded(
            umo, item["source"]
        )
        if item["status"] == "hidden" or any(
            s["id"] in excluded for s in item["sources"]
        ):
            return True
        hidden = self.history.store.hidden(umo)
        involved = set(item["subjects"]) | {s["uid"] for s in item["sources"]}
        return any(
            h["uid"] in involved or private_text(encode(item), h) for h in hidden
        )

    async def valid_sources(self, umo, item):
        if self.blocked(umo, item):
            return False
        try:
            if lm_bridge.source_identity(self.plugin.context) != item["source"]:
                return False
            rows = await lm_bridge.history_sources(
                self.plugin.context, umo, [s["id"] for s in item["sources"]]
            )
            if lm_bridge.source_identity(self.plugin.context) != item["source"]:
                return False
            for s in item["sources"]:
                row = rows.get(s["id"])
                if (
                    not row
                    or row["role"] != "user"
                    or row["uid"] != s["uid"]
                    or row["ts"] != s["ts"]
                    or digest(row["content"]) != s["hash"]
                    or s["quote"] not in row["content"]
                ):
                    return False
                if any(
                    row["metadata"].get(k)
                    for k in (
                        "is_bot_message",
                        "recalled",
                        "withdrawn",
                        "deleted",
                        "hidden",
                    )
                ):
                    return False
            return not self.blocked(umo, item)
        except Exception:
            return False

    async def learning_clues(self, umo, query):
        getter = getattr(self.plugin.context, "get_registered_star", None)
        if not callable(getter):
            return []
        try:
            star = getter("astrbot_plugin_dynamics_learning")
            if not star or getattr(star, "activated", True) is False:
                return []
            export = getattr(
                getattr(star, "star_cls", None), "get_culture_jargon_v1", None
            )
            if not callable(export):
                return []
            result = export(scope=umo, query=query, limit=10)
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, 2)
            if (
                not isinstance(result, dict)
                or result.get("schema_version") != 1
                or result.get("scope") != umo
            ):
                return []
            # No evidence IDs from learning are admitted as original sources.
            return [
                {"name": str(t["name"])[:80], "meaning": str(t["meaning"])[:300]}
                for t in result.get("terms", [])[:10]
            ]
        except Exception:
            return []

    @tracked_operation
    async def process_one(self, umo, mode="back", *, verification_extra_quota=0):
        if not await self.allowed(umo) or self.store.state(umo)["paused"]:
            return False
        lock = self.locks.setdefault(umo, asyncio.Lock())
        if lock.locked():
            return False
        async with lock:
            source = lm_bridge.source_identity(self.plugin.context)
            bounds = await lm_bridge.history_bounds(self.plugin.context, umo)
            job = self.store.init_job(umo, source, bounds["high"])
            high = bounds["high"] if mode == "live" else job["frozen_high"]
            cursor, offset = job[f"{mode}_cursor"], job[f"{mode}_offset"]
            if cursor >= high and not offset:
                return False
            token = self.token(umo)
            rows = await lm_bridge.history_page(
                self.plugin.context,
                umo,
                cursor,
                high,
                self.plugin._cfg("history_batch_size", 200),
            )
            excluded = self.history.store.excluded_sources(umo) | self.store.excluded(
                umo, source
            )
            filtered = [dict(r) for r in rows]
            for row in filtered:
                if any(
                    row.get("metadata", {}).get(k)
                    for k in ("recalled", "withdrawn", "deleted", "hidden")
                ):
                    row["role"] = "excluded"
            pack_offset, resume_gap = resume_offset(job, mode, filtered)
            material, end, next_offset, observed = pack_messages(
                filtered,
                cursor,
                pack_offset,
                hidden=self.history.store.hidden(umo),
                bot_ids=self.history.bot_ids.get(umo.split(":", 1)[0], set()),
                excluded_ids=excluded,
            )
            raw_rows = {str(r["id"]): r for r in rows}
            for m in material:
                original = raw_rows[str(m["id"])]
                m["_source_hash"] = digest(original["content"])
                meta = original.get("metadata", {})
                m["_quoted"] = bool(
                    any(
                        meta.get(k)
                        for k in (
                            "reply_id",
                            "reply_to",
                            "reply_to_message_id",
                            "reply_message_id",
                            "quote_id",
                            "quote",
                            "quoted_message",
                            "forwarded",
                            "forward_id",
                        )
                    )
                    or re.search(r"\[Reply:|\[Forward:|^>", original["content"])
                )
            proposals = []
            if material:
                day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
                limit = (
                    self.plugin._cfg("history_daily_limit", 20)
                    + verification_extra_quota
                )
                if not self.history.store.consume_quota(day, limit):
                    return False
                existing, _ = self.store.entries(umo, limit=20)
                # Disputed/hidden/rejected records never enter the model prompt.
                existing = [
                    e
                    for e in existing
                    if e["status"] in {"approved", "pending"}
                    and await self.valid_sources(umo, e)
                ]
                targets = [
                    {"id": e["id"], "name": e["name"], "meaning": e["meaning"][:200]}
                    for e in existing
                ]
                clues = await self.learning_clues(
                    umo, "\n".join(m["content"] for m in material)
                )
                data = {
                    "existing": targets,
                    "learning_clues": clues,
                    "messages": material,
                }
                raw = await asyncio.wait_for(
                    call_llm(
                        self.plugin.context,
                        umo,
                        encode(data),
                        PROMPT,
                        provider_id=self.plugin._llm_provider_id(),
                    ),
                    timeout=float(self.plugin._cfg("generation_timeout_sec", 180)),
                )
                proposals = validate(raw, material, existing)
                for p in proposals:
                    if p.get("target_id") is None:
                        matches = [
                            e
                            for e in existing
                            if e["status"] in {"approved", "pending"}
                            and e["name"] == p["name"]
                            and e["meaning"] == p["meaning"]
                        ]
                        if len(matches) == 1:
                            p["target_id"] = matches[0]["id"]
                # Refetch source hashes before committing a potentially slow model result.
                for p in proposals:
                    if not await self.valid_sources(
                        umo, {**p, "source": source, "status": p["admission"]}
                    ):
                        raise ValueError("整理期间来源失效，进度保留")
                    target = next(
                        (e for e in existing if e["id"] == p.get("target_id")), None
                    )
                    if target and not await self.valid_sources(umo, target):
                        raise ValueError("整理期间沿革目标来源失效，进度保留")
            if (
                not await self.allowed(umo)
                or self.token(umo) != token
                or lm_bridge.source_identity(self.plugin.context) != source
            ):
                return False
            gap = None
            if not rows:
                end, next_offset = high, 0
                gap = "此ID区间的原文已不可获取，不能恢复起源"
            elif end == cursor and next_offset == offset and not material:
                raise ValueError("文化素材预算不足，未推进进度")
            if bounds.get("first") and not job["processed"]:
                gap = "覆盖仅限仍留存的消息，留存前的起源未知"
            ok = self.store.commit(
                umo,
                source,
                mode,
                (cursor, offset),
                end,
                next_offset,
                proposals,
                observed,
                token[0],
                gap="；".join(v for v in (resume_gap, gap) if v) or None,
                fragment=fragment_checkpoint(filtered, end, next_offset),
            )
            if ok:
                self.errors.pop(umo, None)
            return ok

    async def step(self, umo, mode, *, verification_extra_quota=0):
        try:
            return await self.process_one(
                umo, mode, verification_extra_quota=verification_extra_quota
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Semantic validation errors are safe; provider errors can contain credentials.
            message = (
                str(exc)
                if isinstance(exc, ValueError)
                else f"文化整理失败：{type(exc).__name__}；请检查记忆来源和模型配置"
            )
            self.errors[umo] = message[:300]
            self.store.error(umo, message)
            return False

    @staticmethod
    def public_item(item, detail=True, for_reply=False):
        origin = item.get("origin")
        sources, timeline = item["sources"], item["timeline"] if detail else []
        aliases = item["aliases"]
        if for_reply:
            unique_sources = {}
            for source in sources:
                unique_sources.setdefault(source["id"], source)
            selected = set(list(unique_sources)[:2])
            if origin and origin.get("confirmed"):
                selected.update(origin["source_ids"])
            chosen = []
            for node in timeline[-3:]:
                if len(selected | set(node["source_ids"])) <= 16:
                    chosen.append(node)
                    selected.update(node["source_ids"])
            timeline = chosen
            aliases = []
            for alias in item["aliases"]:
                if len(selected | set(alias["source_ids"])) <= 16:
                    aliases.append(alias)
                    selected.update(alias["source_ids"])
            sources = [
                source for key, source in unique_sources.items() if key in selected
            ]
        result = {
            "id": item["id"],
            "revision": item["revision"],
            "name": item["name"],
            "meaning": item["meaning"],
            "usage": item["usage"],
            "aliases": aliases,
            "first_seen": item["first_seen"],
            "origin": origin if origin and origin.get("confirmed") else None,
            "unknowns": []
            if origin and origin.get("confirmed")
            else ["起源未知；最早留存记录不等于起源"],
            "timeline": timeline,
            "sources": [
                {k: s[k] for k in ("id", "name", "ts", "quote")} for s in sources
            ],
        }
        if for_reply:
            result["timeline_truncated"] = detail and len(timeline) < len(
                item["timeline"]
            )
        return result

    async def query(self, umo, member, query="", kind="search", limit=3):
        if (
            kind not in {"search", "origin"}
            or not isinstance(query, str)
            or len(query) > 160
            or not str(member)
            or len(str(member)) > 128
        ):
            raise ValueError("文化查询参数无效")
        if not await self.allowed(umo):
            return {
                "schema_version": 1,
                "available": False,
                "reason": "当前群未启用群文化",
                "entries": [],
            }
        token = self.token(umo)
        entries, total = self.store.entries(
            umo,
            status="approved",
            query=query.strip(),
            limit=max(1, min(int(limit), 20)),
        )
        valid, missing = [], False
        for item in entries:
            if self.blocked(umo, item):
                continue
            if await self.valid_sources(umo, item):
                compact = self.public_item(
                    item, detail=kind == "origin", for_reply=True
                )
                if len(encode(valid + [compact])) <= 18000:
                    valid.append(compact)
            else:
                missing = True
        if not await self.allowed(umo) or token != self.token(umo):
            return {
                "schema_version": 1,
                "available": False,
                "reason": "查询期间资料发生变化，请重新查询",
                "entries": [],
            }
        return {
            "schema_version": 1,
            "available": True,
            "entries": valid,
            "ambiguous": len(valid) > 1,
            "truncated": total > len(valid),
            "evidence_missing": missing,
            "note": "多义候选请澄清；只引用所给证据，不将最早记录当起源。"
            if valid
            else "没有可公开且证据仍有效的词条；不知道起源时请直接说明未知。",
        }

    async def receipt_current(self, umo, receipt):
        if not await self.allowed(umo) or receipt[
            "privacy"
        ] != self.history.privacy_key(umo):
            return False
        for entry_id, revision in receipt["entries"]:
            item = self.store.entry(umo, entry_id)
            if (
                not item
                or item["revision"] != revision
                or item["status"] != "approved"
                or not await self.valid_sources(umo, item)
            ):
                return False
        # Authorization and revisions can change while the sources are read.
        return (
            await self.allowed(umo)
            and receipt["privacy"] == self.history.privacy_key(umo)
            and all(
                (
                    lambda e: (
                        e is not None
                        and e["revision"] == rev
                        and e["status"] == "approved"
                    )
                )(self.store.entry(umo, eid))
                for eid, rev in receipt["entries"]
            )
        )

    def bind_guard(self, event, entries):
        """Guard this event's direct and streamed sends, including deferred replies."""
        if not entries:
            return
        umo = event.unified_msg_origin
        receipt = {
            "privacy": self.history.privacy_key(umo),
            "entries": [(e["id"], e["revision"]) for e in entries],
        }
        receipts = (
            getattr(event, "_culture_receipts_v1", None)
            if getattr(event, "_culture_guard_installed_v1", False)
            else None
        )
        if receipts is not None:
            receipts.append(receipt)
            return
        original = getattr(event, "send", None)
        if not callable(original):
            raise ValueError("当前宿主无法安装来源送达校验")
        receipts = [receipt]

        async def current():
            return all([await self.receipt_current(umo, r) for r in receipts])

        async def guarded_send(*args, **kwargs):
            if not await current():
                raise RuntimeError("文化依据已失效，取消旧回答发送")
            return await original(*args, **kwargs)

        event.send = guarded_send
        original_stream = getattr(event, "send_streaming", None)
        if callable(original_stream):

            async def guarded_stream(generator, *args, **kwargs):
                # Buffer this evidence-dependent answer so changes during generation
                # cannot release a stale prefix. Nothing is buffered across requests.
                chunks = []
                async for chunk in generator:
                    if len(chunks) >= 2048 or not await current():
                        raise RuntimeError("文化依据失效或回答过长，已取消发送")
                    chunks.append(chunk)
                if not await current():
                    raise RuntimeError("文化依据已失效，取消旧回答发送")

                async def checked():
                    for chunk in chunks:
                        if not await current():
                            raise RuntimeError("文化依据已失效，取消旧回答发送")
                        yield chunk

                return await original_stream(checked(), *args, **kwargs)

            event.send_streaming = guarded_stream
        event._culture_receipts_v1 = receipts
        event._culture_guard_installed_v1 = True

    async def tool(self, event, query="", kind="search"):
        result = await self.query(
            str(event.unified_msg_origin), str(event.get_sender_id()), query, kind
        )
        try:
            self.bind_guard(event, result["entries"])
        except Exception:
            return encode(
                {
                    "schema_version": 1,
                    "available": False,
                    "entries": [],
                    "reason": "来源送达校验不可用，不能提供词条证据",
                }
            )
        return encode(result)

    async def action(
        self,
        umo,
        action,
        actor,
        *,
        entry_id=None,
        revision=None,
        note="",
        patch=None,
        target_id=None,
        target_revision=None,
        admin=False,
    ):
        # Privacy requests remain possible when culture collection is disabled.
        if not await self.history.allowed(umo, recording=action != "hide"):
            raise PermissionError("当前会话不允许此操作")
        if action in {"pause", "resume"}:
            if not admin:
                raise PermissionError("需要管理员权限")
            self.store.set_paused(umo, action == "pause")
            self.history.cancel_operations(umo)
            return {"ok": True}
        item = self.store.entry(umo, entry_id)
        if not item:
            raise ValueError("当前群没有该词条")
        if not admin:
            if action not in {"hide", "correct"}:
                raise PermissionError("需要管理员权限")
            if action == "hide" and actor not in set(item["subjects"]) | {
                s["uid"] for s in item["sources"]
            }:
                raise PermissionError("只能隐藏涉及自己的文化词条")
        if action in {"approve", "edit", "merge"}:
            if not await self.valid_sources(umo, item):
                raise ValueError("词条证据已失效，不能发布、编辑或合并")
            if action == "merge":
                target = self.store.entry(umo, target_id)
                if not target or not await self.valid_sources(umo, target):
                    raise ValueError("合并目标证据不可用")
        result = self.store.mutate(
            umo,
            entry_id,
            revision,
            action,
            actor,
            note,
            patch=patch,
            target_id=target_id,
            target_revision=target_revision,
        )
        self.history.cancel_operations(umo)
        return {
            "ok": True,
            "id": result["id"],
            "revision": result["revision"],
            "status": result["status"],
        }

    async def command(self, event, command, argument=""):
        from .history_commands import administrator

        umo, member = str(event.unified_msg_origin), str(event.get_sender_id())
        if command in {"文化纠正", "文化隐藏"}:
            parts = argument.strip().split(maxsplit=1)
            if (
                not parts
                or not parts[0].isdigit()
                or (command == "文化纠正" and len(parts) < 2)
            ):
                return "用法：/文化纠正 <ID> <说明> 或 /文化隐藏 <ID>"
            item = self.store.entry(umo, int(parts[0]))
            if not item:
                return "当前群没有该词条。"
            result = await self.action(
                umo,
                "correct" if command == "文化纠正" else "hide",
                member,
                entry_id=item["id"],
                revision=item["revision"],
                note=parts[1] if len(parts) > 1 else "本人请求隐藏",
                admin=administrator(self.plugin, event),
            )
            return (
                "已提交纠正，词条暂停公开并等待审核。"
                if result["status"] == "disputed"
                else "已隐藏相关词条与来源。"
            )
        result = await self.query(
            umo,
            member,
            argument,
            "origin" if command == "梗溯源" else "search",
            limit=10,
        )
        self.bind_guard(event, result["entries"])
        if not result["entries"]:
            return result.get("reason") or result["note"]
        lines = []

        def when(value):
            return (
                datetime.fromtimestamp(value, timezone.utc).isoformat(
                    timespec="minutes"
                )
                if value
                else "未知"
            )

        for e in result["entries"]:
            lines += [
                f"#{e['id']} {e['name']}：{e['meaning']}",
                f"用法：{e['usage'] or '未记录'}",
                f"最早留存：{when(e['first_seen'])}",
                "起源："
                + (
                    e["origin"]["text"]
                    if e["origin"]
                    else "未知；最早留存记录不等于起源"
                ),
            ]
            if command == "梗溯源":
                lines += [
                    f"沿革：{n['text']}（记载 {when(n['recorded_at'])}；发生 {when(n['event_at'])}；来源 {','.join(n['source_ids'])}）"
                    for n in e["timeline"]
                ]
                lines += [
                    f"来源 #{s['id']} {s['name']}：{s['quote']}"
                    for s in e["sources"][:4]
                ]
                if e.get("timeline_truncated"):
                    lines.append("仅展示近期沿革，完整时间线可在管理面板查看。")
        if result["ambiguous"]:
            lines.append("找到多个候选，请使用词条ID进一步查询。")
        return "\n".join(lines)
