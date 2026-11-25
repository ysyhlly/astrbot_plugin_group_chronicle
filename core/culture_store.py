"""Additive culture tables in the existing archive, with independent checkpoints."""

from __future__ import annotations

import json
import time
from pathlib import Path

from .archive_store import encode
from .culture_contract import independent_sources


class CultureStore:
    def __init__(self, archive):
        self.archive = archive
        with archive.connection() as db:
            exists = db.execute(
                "SELECT 1 FROM sqlite_master WHERE name='culture_meta'"
            ).fetchone()
            if not exists:
                backup = Path(str(archive.path) + ".pre-culture-v1.bak")
                if not backup.exists():
                    import sqlite3

                    target = sqlite3.connect(backup)
                    try:
                        db.backup(target)
                    finally:
                        target.close()
        with archive.connection(write=True) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS culture_meta (version INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS culture_groups (
                    umo TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0,
                    paused INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS culture_jobs (
                    umo TEXT PRIMARY KEY, source TEXT NOT NULL, frozen_high INTEGER NOT NULL,
                    live_cursor INTEGER NOT NULL, back_cursor INTEGER NOT NULL DEFAULT 0,
                    live_offset INTEGER NOT NULL DEFAULT 0, back_offset INTEGER NOT NULL DEFAULT 0,
                    processed INTEGER NOT NULL DEFAULT 0, coverage_min REAL NOT NULL DEFAULT 0,
                    coverage_max REAL NOT NULL DEFAULT 0, gaps TEXT NOT NULL DEFAULT '[]',
                    error TEXT NOT NULL DEFAULT '', updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS culture_entries (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, source TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, name TEXT NOT NULL, status TEXT NOT NULL,
                    payload TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
                    merged_into INTEGER, created REAL NOT NULL, updated REAL NOT NULL,
                    UNIQUE(umo,source,fingerprint));
                CREATE INDEX IF NOT EXISTS culture_lookup ON culture_entries(umo,status,name);
                CREATE TABLE IF NOT EXISTS culture_revisions (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, entry_id INTEGER NOT NULL,
                    before_data TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
                    note TEXT NOT NULL, created REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS culture_revisions_entry
                    ON culture_revisions(umo,entry_id,id DESC);
                CREATE INDEX IF NOT EXISTS culture_revisions_created
                    ON culture_revisions(created);
                CREATE TABLE IF NOT EXISTS culture_exclusions (
                    umo TEXT NOT NULL, source TEXT NOT NULL, source_id TEXT NOT NULL,
                    PRIMARY KEY(umo,source,source_id));
            """)
            version = db.execute("SELECT version FROM culture_meta").fetchone()
            if version and version[0] != 1:
                raise RuntimeError("文化数据库版本不兼容")
            if not version:
                db.execute("INSERT INTO culture_meta VALUES(1)")
            columns = {row[1] for row in db.execute("PRAGMA table_info(culture_jobs)")}
            for mode in ("back", "live"):
                for field, declaration in (
                    ("retry", "TEXT NOT NULL DEFAULT '{}'"),
                    ("fragment_id", "INTEGER NOT NULL DEFAULT 0"),
                    ("fragment_hash", "TEXT NOT NULL DEFAULT ''"),
                ):
                    name = f"{mode}_{field}"
                    if name not in columns:
                        db.execute(
                            f"ALTER TABLE culture_jobs ADD COLUMN {name} {declaration}"
                        )

    @staticmethod
    def bump(db, umo):
        db.execute("INSERT OR IGNORE INTO culture_groups(umo) VALUES(?)", (umo,))
        db.execute("UPDATE culture_groups SET revision=revision+1 WHERE umo=?", (umo,))

    def state(self, umo):
        with self.archive.connection() as db:
            row = db.execute(
                "SELECT * FROM culture_groups WHERE umo=?", (umo,)
            ).fetchone()
            return dict(row) if row else {"umo": umo, "revision": 0, "paused": 0}

    def set_paused(self, umo, paused):
        with self.archive.connection(write=True) as db:
            self.bump(db, umo)
            db.execute(
                "UPDATE culture_groups SET paused=? WHERE umo=?", (int(paused), umo)
            )

    def init_job(self, umo, source, high):
        with self.archive.connection(write=True) as db:
            db.execute(
                "INSERT OR IGNORE INTO culture_jobs(umo,source,frozen_high,live_cursor,updated) VALUES(?,?,?,?,?)",
                (umo, source, high, high, time.time()),
            )
            row = dict(
                db.execute("SELECT * FROM culture_jobs WHERE umo=?", (umo,)).fetchone()
            )
            if row["source"] != source:
                raise ValueError(
                    "记忆来源已切换，旧回溯进度不能用于新来源；请恢复原来源或迁移文化档案"
                )
            if high < max(row["frozen_high"], row["live_cursor"]):
                raise ValueError(
                    "历史源最高ID回退，可能发生清库；回溯已暂停，请核对来源"
                )
            return row

    def job(self, umo):
        with self.archive.connection() as db:
            row = db.execute(
                "SELECT * FROM culture_jobs WHERE umo=?", (umo,)
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            for mode in ("back", "live"):
                retry = json.loads(result.pop(f"{mode}_retry", "{}"))
                result[f"{mode}_pending_review"] = len(retry.get("targets", []))
            return result

    def error(self, umo, message):
        with self.archive.connection(write=True) as db:
            db.execute(
                "UPDATE culture_jobs SET error=?,updated=? WHERE umo=?",
                (message[:300], time.time(), umo),
            )

    @staticmethod
    def unpack(row):
        return {
            **json.loads(row["payload"]),
            **{
                k: row[k]
                for k in (
                    "id",
                    "umo",
                    "source",
                    "status",
                    "revision",
                    "merged_into",
                    "updated",
                )
            },
        }

    def entries(self, umo, *, status=None, query="", limit=100, offset=0):
        # Search aliases/meanings as literal substrings, not SQL LIKE wildcards.
        with self.archive.connection() as db:
            rows = db.execute(
                "SELECT * FROM culture_entries WHERE umo=? ORDER BY updated DESC,id DESC",
                (umo,),
            )
            result = []
            for row in rows:
                if status and row["status"] != status:
                    continue
                item = self.unpack(row)
                haystack = [
                    item["name"],
                    item["meaning"],
                    *(a["name"] for a in item["aliases"]),
                ]
                if (
                    query
                    and not any(query.casefold() in s.casefold() for s in haystack)
                    and query != str(item["id"])
                ):
                    continue
                result.append(item)
            return result[offset : offset + limit], len(result)

    def entry(self, umo, entry_id):
        with self.archive.connection() as db:
            row = db.execute(
                "SELECT * FROM culture_entries WHERE umo=? AND id=?",
                (umo, int(entry_id)),
            ).fetchone()
            return self.unpack(row) if row else None

    def excluded(self, umo, source):
        with self.archive.connection() as db:
            return {
                r[0]
                for r in db.execute(
                    "SELECT source_id FROM culture_exclusions WHERE umo=? AND source=?",
                    (umo, source),
                )
            }

    @staticmethod
    def save_revision(db, row, actor, action, note):
        db.execute(
            "INSERT INTO culture_revisions(umo,entry_id,before_data,actor,action,note,created) VALUES(?,?,?,?,?,?,?)",
            (
                row["umo"],
                row["id"],
                encode(dict(row)),
                actor,
                action,
                note,
                time.time(),
            ),
        )

    @staticmethod
    def combined(old, new):
        payload = dict(old)
        for field in ("sources", "aliases", "timeline"):
            payload[field] = list(
                {encode(v): v for v in old[field] + new[field]}.values()
            )
        if len(payload["sources"]) > 100:
            raise ValueError("单词条证据超过100条，请拆分沿革阶段后再合并")
        payload["subjects"] = sorted(set(old["subjects"] + new["subjects"]))
        payload["first_seen"] = min(old["first_seen"], new["first_seen"])
        if new.get("origin"):
            payload["origin"] = new["origin"]
        payload["timeline"].sort(key=lambda n: n["event_at"] or n["recorded_at"])
        return payload

    def commit(
        self,
        umo,
        source,
        mode,
        expected,
        end,
        offset,
        proposals,
        observed,
        revision,
        gap=None,
        fragment=None,
        pending_review=None,
    ):
        if mode not in {"live", "back"}:
            raise ValueError("回溯模式无效")
        with self.archive.connection(write=True) as db:
            state = db.execute(
                "SELECT revision,paused FROM culture_groups WHERE umo=?", (umo,)
            ).fetchone()
            if (state[0] if state else 0) != revision or (state and state[1]):
                return False
            job = db.execute(
                "SELECT * FROM culture_jobs WHERE umo=?", (umo,)
            ).fetchone()
            if (
                not job
                or job["source"] != source
                or (job[f"{mode}_cursor"], job[f"{mode}_offset"]) != tuple(expected)
            ):
                return False
            excluded = {
                r[0]
                for r in db.execute(
                    "SELECT source_id FROM culture_exclusions WHERE umo=? AND source=?",
                    (umo, source),
                )
            }
            for proposal in proposals:
                p = dict(proposal)
                if any(s["id"] in excluded for s in p["sources"]):
                    continue
                existing = db.execute(
                    "SELECT id FROM culture_entries WHERE umo=? AND source=? AND fingerprint=?",
                    (umo, source, p["fingerprint"]),
                ).fetchone()
                if existing:
                    continue
                status = p["admission"]
                target = db.execute(
                    "SELECT * FROM culture_entries WHERE umo=? AND id=? AND source=?",
                    (umo, p.get("target_id"), source),
                ).fetchone()
                if target:
                    previous = json.loads(target["payload"])
                    same_sense = (
                        previous["name"] == p["name"]
                        and previous["meaning"] == p["meaning"]
                        and previous["kind"] == p["kind"]
                    )
                    manually_reviewed = db.execute(
                        "SELECT 1 FROM culture_revisions WHERE umo=? AND entry_id=? AND action!='evidence' LIMIT 1",
                        (umo, target["id"]),
                    ).fetchone()
                    simple_evidence = all(
                        not candidate.get(field)
                        for candidate in (previous, p)
                        for field in (
                            "risk",
                            "conflict",
                            "origin",
                            "timeline",
                            "aliases",
                            "subjects",
                        )
                    )
                    can_join = same_sense and (
                        target["status"] == "approved"
                        and status == "approved"
                        or target["status"] in {"approved", "pending"}
                        and simple_evidence
                        and not manually_reviewed
                    )
                    count = len({encode(s) for s in previous["sources"] + p["sources"]})
                    if can_join and count <= 100:
                        self.save_revision(
                            db,
                            target,
                            "system",
                            "evidence",
                            "自动追加独立支持的同义证据与沿革",
                        )
                        payload = self.combined(previous, p)
                        next_status = (
                            "approved"
                            if len(independent_sources(payload["sources"])) >= 2
                            else "pending"
                        )
                        if target["status"] == "approved" and status == "approved":
                            next_status = "approved"
                        db.execute(
                            "UPDATE culture_entries SET payload=?,status=?,revision=revision+1,updated=? WHERE id=?",
                            (encode(payload), next_status, time.time(), target["id"]),
                        )
                        status = "merged"
                    else:
                        status = "pending"
                        if count > 100:
                            p["review_note"] = (
                                "原词条证据已满，保留为独立待审候选；请按沿革阶段整理"
                            )
                # Existing corrections/hidden senses cannot be republished by a new batch.
                other_rows = db.execute(
                    "SELECT status,payload FROM culture_entries WHERE umo=? AND source=? AND name=?",
                    (umo, source, p["name"]),
                ).fetchall()
                if any(
                    r["status"] in ("hidden", "disputed", "rejected")
                    for r in other_rows
                ):
                    status = "pending"
                now = time.time()
                db.execute(
                    "INSERT INTO culture_entries(umo,source,fingerprint,name,status,payload,merged_into,created,updated) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        umo,
                        source,
                        p["fingerprint"],
                        p["name"],
                        status,
                        encode(p),
                        target["id"] if status == "merged" else None,
                        now,
                        now,
                    ),
                )
            if pending_review:
                # Publish verified entries and checkpoint the remaining work atomically.
                # The page is accounted for only once, after all candidates are settled.
                message = f"待单独复核 {len(pending_review['targets'])} 个文化候选"
                db.execute(f"UPDATE culture_jobs SET {mode}_retry=?,error=?,updated=? WHERE umo=?",
                           (encode(pending_review), message, time.time(), umo))
                self.bump(db, umo)
                return True
            times = [m["ts"] for m in observed if m.get("ts") and m["ts"] > 0]
            low = min(times) if times else 0
            if job["coverage_min"]:
                low = min(low, job["coverage_min"]) if low else job["coverage_min"]
            gaps = json.loads(job["gaps"])
            if gap and gap not in gaps:
                gaps.append(gap)
            fragment = fragment or {}
            db.execute(
                f"""UPDATE culture_jobs SET {mode}_cursor=?,{mode}_offset=?,
                       {mode}_fragment_id=?,{mode}_fragment_hash=?,processed=processed+?,
                       coverage_min=?,coverage_max=MAX(coverage_max,?),gaps=?,{mode}_retry='{{}}',error='',updated=? WHERE umo=?""",
                (
                    end,
                    offset,
                    fragment.get("id", 0),
                    fragment.get("sha256", ""),
                    len(observed),
                    low,
                    max(times) if times else 0,
                    encode(gaps),
                    time.time(),
                    umo,
                ),
            )
            self.bump(db, umo)
            return True

    def mutate(
        self,
        umo,
        entry_id,
        revision,
        action,
        actor,
        note,
        *,
        patch=None,
        target_id=None,
        target_revision=None,
    ):
        if not isinstance(note, str) or not note.strip() or len(note) > 1000:
            raise ValueError("请填写不超过1000字的处理理由")
        with self.archive.connection(write=True) as db:
            row = db.execute(
                "SELECT * FROM culture_entries WHERE umo=? AND id=?",
                (umo, int(entry_id)),
            ).fetchone()
            if not row or row["revision"] != revision:
                raise ValueError("词条不存在或已更新，请刷新后重试")
            payload = json.loads(row["payload"])
            if action not in {"approve", "reject", "edit", "hide", "correct", "merge"}:
                raise ValueError("文化操作无效")
            if row["status"] in {"hidden", "merged"} and action != "hide":
                raise ValueError("已隐藏或已合并词条不可直接发布或修改")
            self.save_revision(db, row, actor, action, note)
            status = row["status"]
            merged_into = row["merged_into"]
            if action == "approve":
                status = "approved"
                if payload.get("origin"):
                    payload["origin"]["confirmed"] = True
            elif action == "reject":
                status = "rejected"
            elif action == "correct":
                status = "disputed"
            elif action == "edit":
                if (
                    not isinstance(patch, dict)
                    or not patch
                    or set(patch) - {"meaning", "usage", "origin_text"}
                ):
                    raise ValueError("仅允许编辑含义、场景及已有起源说明")
                from .culture_contract import bounded_text

                for key, value in patch.items():
                    value = bounded_text(
                        value, 1000 if key == "meaning" else 500, key == "meaning"
                    )
                    if key == "origin_text":
                        if not value:
                            payload["origin"] = None
                        elif payload.get("origin"):
                            payload["origin"]["text"] = value
                            payload["origin"]["confirmed"] = False
                        else:
                            raise ValueError("无起源证据，不能仅靠编辑补写起源")
                    else:
                        payload[key] = value
                status = "pending"
            elif action == "hide":
                status = "hidden"
                for s in payload["sources"]:
                    db.execute(
                        "INSERT OR IGNORE INTO culture_exclusions VALUES(?,?,?)",
                        (umo, row["source"], s["id"]),
                    )
            elif action == "merge":
                target = db.execute(
                    "SELECT * FROM culture_entries WHERE umo=? AND id=?",
                    (umo, target_id),
                ).fetchone()
                if (
                    not target
                    or target["id"] == row["id"]
                    or target["source"] != row["source"]
                    or target["status"] in {"hidden", "merged", "rejected"}
                    or target["revision"] != target_revision
                ):
                    raise ValueError("合并目标无效、跨来源或已更新")
                self.save_revision(db, target, actor, "merge", note)
                old = json.loads(target["payload"])
                combined = self.combined(old, payload)
                if old["name"] != payload["name"]:
                    combined["aliases"].append(
                        {
                            "name": payload["name"],
                            "source_ids": [s["id"] for s in payload["sources"]],
                        }
                    )
                db.execute(
                    "UPDATE culture_entries SET payload=?,revision=revision+1,status='pending',updated=? WHERE id=?",
                    (encode(combined), time.time(), target_id),
                )
                status, merged_into = "merged", target_id
            db.execute(
                "UPDATE culture_entries SET payload=?,status=?,merged_into=?,revision=revision+1,updated=? WHERE id=?",
                (encode(payload), status, merged_into, time.time(), entry_id),
            )
            self.bump(db, umo)
        return self.entry(umo, entry_id)

    def revisions(self, umo, entry_id):
        with self.archive.connection() as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT id,actor,action,note,created,before_data FROM culture_revisions WHERE umo=? AND entry_id=? ORDER BY id DESC LIMIT 50",
                    (umo, entry_id),
                )
            ]
