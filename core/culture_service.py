"""Bounded background extraction, scoped reads, moderation and delivery guards."""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from datetime import datetime, timedelta, timezone

from . import lm_bridge
from .archive_store import encode, private_text
from .culture_contract import (
    build_culture_system_prompt,
    digest,
    validate_candidates,
)
from .culture_store import CultureStore
from .lifetime import (
    fragment_checkpoint,
    has_substantive_signal,
    pack_messages,
    resume_offset,
    tracked_operation,
)
from .llm_call import call_llm, encode_compact

_SOURCE_BATCH_SIZE = 500


def _source_row_valid(row, src):
    """Verify one source row matches its stored quote, author, and visibility."""
    return bool(
        row
        and row["role"] == "user"
        and row["uid"] == src["uid"]
        and row["ts"] == src["ts"]
        and digest(row["content"]) == src["hash"]
        and src["quote"] in row["content"]
        and not any(
            row["metadata"].get(k)
            for k in (
                "is_bot_message",
                "recalled",
                "withdrawn",
                "deleted",
                "hidden",
            )
        )
    )


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

    def _privacy_snapshot(self, umo, sources):
        base_excluded = self.history.store.excluded_sources(umo)
        by_source = {
            src: base_excluded | self.store.excluded(umo, src)
            for src in sorted({s for s in sources if s})
        }
        hidden = self.history.store.hidden(umo)
        return base_excluded, by_source, hidden

    @staticmethod
    def _is_item_blocked(item, snapshot):
        base_excluded, by_source, hidden = snapshot
        excluded = by_source.get(item.get("source"), base_excluded)
        if item["status"] == "hidden" or any(
            s["id"] in excluded for s in item["sources"]
        ):
            return True
        if not hidden:
            return False
        involved = set(item["subjects"]) | {s["uid"] for s in item["sources"]}
        encoded = None
        for h in hidden:
            if h["uid"] in involved:
                return True
            if encoded is None:
                encoded = encode(item)
            if private_text(encoded, h):
                return True
        return False

    def blocked(self, umo, item):
        snapshot = self._privacy_snapshot(umo, [item["source"]])
        return self._is_item_blocked(item, snapshot)

    async def _batch_valid_sources(self, umo, items):
        if not items:
            return []
        try:
            sources = {item.get("source") for item in items}
            pre_snapshot = await asyncio.to_thread(self._privacy_snapshot, umo, sources)
            before_source = lm_bridge.source_identity(self.plugin.context)
            candidates = [
                idx
                for idx, item in enumerate(items)
                if item.get("source") == before_source
                and not self._is_item_blocked(item, pre_snapshot)
            ]
            if not candidates:
                return [False] * len(items)
            all_ids = list(
                dict.fromkeys(
                    s["id"] for idx in candidates for s in items[idx]["sources"]
                )
            )
            rows = {}
            if not all_ids:
                fetched = await lm_bridge.history_sources(self.plugin.context, umo, [])
                if isinstance(fetched, dict):
                    rows.update(fetched)
            else:
                for start in range(0, len(all_ids), _SOURCE_BATCH_SIZE):
                    chunk = all_ids[start : start + _SOURCE_BATCH_SIZE]
                    fetched = await lm_bridge.history_sources(
                        self.plugin.context, umo, chunk
                    )
                    if isinstance(fetched, dict):
                        rows.update(fetched)
            after_source = lm_bridge.source_identity(self.plugin.context)
            post_snapshot = await asyncio.to_thread(
                self._privacy_snapshot, umo, sources
            )
            results = [False] * len(items)
            for idx in candidates:
                item = items[idx]
                if item.get("source") != after_source or self._is_item_blocked(
                    item, post_snapshot
                ):
                    continue
                if all(
                    _source_row_valid(rows.get(s["id"]), s) for s in item["sources"]
                ):
                    results[idx] = True
            return results
        except Exception:
            return [False] * len(items)

    async def valid_sources(self, umo, item):
        return (await self._batch_valid_sources(umo, [item]))[0]

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
        if not await self.allowed(umo):
            return False
        initial_state = await asyncio.to_thread(self.store.state, umo)
        if initial_state["paused"]:
            return False
        lock = self.locks.setdefault(umo, asyncio.Lock())
        if lock.locked():
            return False
        async with lock:
            source = lm_bridge.source_identity(self.plugin.context)
            bounds = await lm_bridge.history_bounds(self.plugin.context, umo)
            job = await asyncio.to_thread(
                self.store.init_job, umo, source, bounds["high"]
            )
            high = bounds["high"] if mode == "live" else job["frozen_high"]
            retry = json.loads(job.get(f"{mode}_retry", "{}"))
            if retry:
                high = min(high, retry["high"])
            cursor, offset = job[f"{mode}_cursor"], job[f"{mode}_offset"]
            if cursor >= high and not offset:
                return False
            token = await asyncio.to_thread(self.token, umo)
            rows = await lm_bridge.history_page(
                self.plugin.context,
                umo,
                cursor,
                high,
                self.plugin._cfg("history_batch_size", 200),
            )

            def _page_privacy():
                return (
                    self.history.store.excluded_sources(umo)
                    | self.store.excluded(umo, source),
                    self.history.store.hidden(umo),
                )

            excluded, hidden_identities = await asyncio.to_thread(_page_privacy)
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
                hidden=hidden_identities,
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
            page_signature = digest(encode([(m["id"], m["_source_hash"], m["content"], m["uid"]) for m in material]))
            if retry.get("page") != page_signature:
                retry = {}
            queue = list(retry.get("targets", []))
            target = queue.pop(0) if queue else None
            rejected_count = retry.get("rejected", 0)
            review_material = material
            if target and target["ids"]:
                review_material = [m for m in material if str(m["id"]) in target["ids"]]
            proposals = []
            if review_material and (
                target is not None or has_substantive_signal(review_material)
            ):
                def _load_culture_context():
                    entries_list, _ = self.store.entries(umo, limit=20)
                    group_row = self.history.store.group(umo) or {}
                    return entries_list, str(group_row.get("name") or "")

                existing_raw, group_name = await asyncio.to_thread(_load_culture_context)
                # Disputed/hidden/rejected records never enter the model prompt.
                existing_candidates = [
                    e for e in existing_raw if e["status"] in {"approved", "pending"}
                ]
                valid_flags = await self._batch_valid_sources(umo, existing_candidates)
                existing = [e for e, ok in zip(existing_candidates, valid_flags) if ok]
                # Deterministic ascending ID order keeps existing entries prefix-stable.
                targets = sorted(
                    (
                        {"id": e["id"], "name": e["name"], "meaning": e["meaning"][:200]}
                        for e in existing
                    ),
                    key=lambda item: item["id"],
                )
                clues = sorted(
                    await self.learning_clues(
                        umo, "\n".join(m["content"] for m in review_material)
                    ),
                    key=lambda item: (item["name"], item["meaning"]),
                )
                # Explicit slow-to-fast key order: existing -> learning_clues -> messages -> review_candidate.
                data = {
                    "existing": targets,
                    "learning_clues": clues,
                    "messages": review_material,
                }
                if target:
                    data["review_candidate"] = target["name"] or "仅复核所附原文，证据不足返回空列表"

                def _pre_llm_ok():
                    return (
                        not self.store.state(umo)["paused"] and self.token(umo) == token
                    )

                if (
                    not await self.allowed(umo)
                    or lm_bridge.source_identity(self.plugin.context) != source
                    or not await asyncio.to_thread(_pre_llm_ok)
                ):
                    return False
                day = datetime.now(timezone(timedelta(hours=8))).date().isoformat()
                raw_limit = self.plugin._cfg("history_daily_limit", 20)
                daily_limit = max(0, int(20 if raw_limit is None else raw_limit))
                reserved = daily_limit // 2 if daily_limit >= 2 else 0
                if not await asyncio.to_thread(
                    self.history.store.consume_quota,
                    day,
                    daily_limit,
                    kind="culture",
                    reserved_history=reserved,
                    extra=verification_extra_quota,
                ):
                    return False
                raw = await asyncio.wait_for(
                    call_llm(
                        self.plugin.context,
                        umo,
                        encode_compact(data),
                        build_culture_system_prompt(umo, group_name),
                        provider_id=self.plugin._llm_provider_id(),
                    ),
                    timeout=float(self.plugin._cfg("generation_timeout_sec", 180)),
                )
                proposals, rejected = validate_candidates(
                    raw, review_material, existing, target_name=target["name"] if target else ""
                )
                if target is None:
                    queue.extend(rejected)
                elif not proposals:
                    rejected_count += 1
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
                proposal_items = [
                    {**p, "source": source, "status": p["admission"]} for p in proposals
                ]
                existing_by_id = {e["id"]: e for e in existing}
                target_items = []
                seen_targets = set()
                for p in proposals:
                    tid = p.get("target_id")
                    if tid in existing_by_id and tid not in seen_targets:
                        seen_targets.add(tid)
                        target_items.append(existing_by_id[tid])
                verify_items = proposal_items + target_items
                if verify_items:
                    flags = await self._batch_valid_sources(umo, verify_items)
                    if not all(flags[: len(proposal_items)]):
                        raise ValueError("整理期间来源失效，进度保留")
                    if not all(flags[len(proposal_items) :]):
                        raise ValueError("整理期间沿革目标来源失效，进度保留")
            if (
                not await self.allowed(umo)
                or lm_bridge.source_identity(self.plugin.context) != source
                or await asyncio.to_thread(self.token, umo) != token
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
            if rejected_count and not queue:
                review_gap = f"本批有 {rejected_count} 个候选经单独复核仍无充分依据"
                gap = "；".join(v for v in (gap, review_gap) if v)
            pending_review = {"page": page_signature, "high": high, "targets": queue,
                              "rejected": rejected_count} if queue else None
            ok = await asyncio.to_thread(
                self.store.commit,
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
                pending_review=pending_review,
            )
            if ok and pending_review:
                self.errors[umo] = f"待单独复核 {len(queue)} 个文化候选"
                return bool(proposals) or target is not None
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
            await asyncio.to_thread(self.store.error, umo, message)
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

        def _load_query_inputs():
            return self.token(umo), self.store.entries(
                umo,
                status="approved",
                query=query.strip(),
                limit=max(1, min(int(limit), 20)),
            )

        token, (entries, total) = await asyncio.to_thread(_load_query_inputs)
        valid, missing = [], False
        if entries:
            snapshot = await asyncio.to_thread(
                self._privacy_snapshot, umo, {e["source"] for e in entries}
            )
            non_blocked = [e for e in entries if not self._is_item_blocked(e, snapshot)]
            flags = await self._batch_valid_sources(umo, non_blocked)
            for item, is_valid in zip(non_blocked, flags):
                if is_valid:
                    compact = self.public_item(
                        item, detail=kind == "origin", for_reply=True
                    )
                    if len(encode(valid + [compact])) <= 18000:
                        valid.append(compact)
                else:
                    missing = True
        if (
            not await self.allowed(umo)
            or await asyncio.to_thread(self.token, umo) != token
        ):
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
        def _read_receipt_state():
            return (
                self.history.privacy_key(umo),
                [self.store.entry(umo, eid) for eid, _ in receipt["entries"]],
            )

        if not await self.allowed(umo):
            return False
        privacy_now, items = await asyncio.to_thread(_read_receipt_state)
        if receipt["privacy"] != privacy_now:
            return False
        for (_, revision), item in zip(receipt["entries"], items):
            if not item or item["revision"] != revision or item["status"] != "approved":
                return False
        if not all(await self._batch_valid_sources(umo, items)):
            return False
        # Authorization and revisions can change while the sources are read.
        if not await self.allowed(umo):
            return False
        privacy_after, items_after = await asyncio.to_thread(_read_receipt_state)
        return privacy_after == receipt["privacy"] and all(
            e is not None and e["revision"] == rev and e["status"] == "approved"
            for (_, rev), e in zip(receipt["entries"], items_after)
        )

    def bind_guard(self, event, entries, *, privacy=None):
        """Guard this event's direct and streamed sends, including deferred replies."""
        if not entries:
            return
        umo = event.unified_msg_origin
        receipt = {
            "privacy": self.history.privacy_key(umo) if privacy is None else privacy,
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
        umo = str(event.unified_msg_origin)
        result = await self.query(umo, str(event.get_sender_id()), query, kind)
        privacy = (
            await asyncio.to_thread(self.history.privacy_key, umo)
            if result["entries"]
            else None
        )
        try:
            self.bind_guard(event, result["entries"], privacy=privacy)
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
            await asyncio.to_thread(self.store.set_paused, umo, action == "pause")
            self.history.cancel_operations(umo)
            return {"ok": True}
        item = await asyncio.to_thread(self.store.entry, umo, entry_id)
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
            if action == "merge":
                target = await asyncio.to_thread(self.store.entry, umo, target_id)
                if not target:
                    raise ValueError("合并目标证据不可用")
                flags = await self._batch_valid_sources(umo, [item, target])
                if not flags[0]:
                    raise ValueError("词条证据已失效，不能发布、编辑或合并")
                if not flags[1]:
                    raise ValueError("合并目标证据不可用")
            elif not (await self._batch_valid_sources(umo, [item]))[0]:
                raise ValueError("词条证据已失效，不能发布、编辑或合并")
        result = await asyncio.to_thread(
            self.store.mutate,
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
            item = await asyncio.to_thread(self.store.entry, umo, int(parts[0]))
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
        privacy = (
            await asyncio.to_thread(self.history.privacy_key, umo)
            if result["entries"]
            else None
        )
        self.bind_guard(event, result["entries"], privacy=privacy)
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
