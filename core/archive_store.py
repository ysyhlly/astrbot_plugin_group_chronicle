"""Transactional lifetime facts, membership periods, editions, and checkpoints."""

from __future__ import annotations

import hashlib
import difflib
import json
import math
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


def encode(value):
    """Serialize canonical JSON.

    Args:
        value: JSON-compatible value.

    Returns:
        Canonical UTF-8 text.
    """
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def private_text(text, member):
    """Conservatively detect an excluded identity in unstructured prose.

    Args:
        text: Source or prose text.
        member: Hidden identity and known aliases.

    Returns:
        Whether privacy cannot be assured for this text.
    """
    return any(alias and alias in text for alias in member["aliases"]) or bool(
        re.search(
            r"(?<![A-Za-z0-9])" + re.escape(member["uid"]) + r"(?![A-Za-z0-9])", text
        )
    )


class ArchiveStore:
    """Keep durable facts separate from generated articles."""

    fragment_source_bound = True

    _locks = {}

    def __init__(self, path):
        """Open or initialize the plugin-owned database.

        Args:
            path: SQLite file in the isolated plugin data directory.
        """
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = self._locks.setdefault(str(self.path.resolve()), threading.RLock())
        with self.connection(write=True) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError("长期档案数据库版本不兼容，已停止写入")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS groups (
                    umo TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '群聊',
                    revision INTEGER NOT NULL DEFAULT 0, roster_at REAL DEFAULT 0,
                    unavailable INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS members (
                    umo TEXT NOT NULL, uid TEXT NOT NULL, name TEXT NOT NULL,
                    aliases TEXT NOT NULL, first_seen REAL NOT NULL,
                    last_seen REAL NOT NULL, hidden INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(umo,uid));
                CREATE TABLE IF NOT EXISTS periods (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, uid TEXT NOT NULL,
                    observed REAL NOT NULL, joined REAL, departed REAL,
                    closed INTEGER NOT NULL DEFAULT 0, reason TEXT DEFAULT '');
                CREATE INDEX IF NOT EXISTS periods_member ON periods(umo,uid,observed);
                CREATE TABLE IF NOT EXISTS notices (key TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL, payload TEXT NOT NULL, event_at REAL NOT NULL,
                    last_event_at REAL NOT NULL DEFAULT 0,
                    updated REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
                    merged_into INTEGER, UNIQUE(umo,fingerprint));
                CREATE INDEX IF NOT EXISTS events_group ON events(umo,status,event_at);
                CREATE TABLE IF NOT EXISTS revisions (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, entity TEXT NOT NULL,
                    entity_id TEXT NOT NULL, before_data TEXT NOT NULL,
                    actor TEXT NOT NULL, note TEXT NOT NULL, created REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS hides (
                    umo TEXT NOT NULL, uid TEXT NOT NULL, event_id INTEGER NOT NULL,
                    PRIMARY KEY(umo,uid,event_id));
                CREATE TABLE IF NOT EXISTS corrections (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, event_id INTEGER NOT NULL,
                    uid TEXT NOT NULL, note TEXT NOT NULL, created REAL NOT NULL,
                    resolved INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS jobs (
                    umo TEXT PRIMARY KEY, frozen_high INTEGER NOT NULL,
                    back_cursor INTEGER NOT NULL DEFAULT 0, live_cursor INTEGER NOT NULL,
                    back_offset INTEGER NOT NULL DEFAULT 0, live_offset INTEGER NOT NULL DEFAULT 0,
                    processed INTEGER NOT NULL DEFAULT 0, coverage_min REAL DEFAULT 0,
                    coverage_max REAL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
                    gaps TEXT NOT NULL DEFAULT '[]', updated REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS quota (day TEXT PRIMARY KEY, used INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY, umo TEXT NOT NULL, cache_key TEXT NOT NULL,
                    entry TEXT NOT NULL, created REAL NOT NULL, invalid INTEGER DEFAULT 0,
                    UNIQUE(umo,cache_key));
                PRAGMA user_version=1;
            """)
            if "last_event_at" not in {
                r[1] for r in db.execute("PRAGMA table_info(events)")
            }:
                db.execute(
                    "ALTER TABLE events ADD COLUMN last_event_at REAL NOT NULL DEFAULT 0"
                )
            for row in db.execute(
                "SELECT id,event_at,payload FROM events WHERE last_event_at=0"
            ).fetchall():
                payload = json.loads(row["payload"])
                last = max(
                    [row["event_at"]]
                    + [f["event_at"] for f in payload.get("followups", [])]
                )
                db.execute(
                    "UPDATE events SET last_event_at=? WHERE id=?", (last, row["id"])
                )
            db.execute(
                "CREATE INDEX IF NOT EXISTS events_end ON events(umo,status,last_event_at)"
            )
            columns = {r[1] for r in db.execute('PRAGMA table_info(jobs)')}
            for mode in ('back','live'):
                for field,decl in (('fragment_id','INTEGER NOT NULL DEFAULT 0'),('fragment_hash',"TEXT NOT NULL DEFAULT ''")):
                    name=f'{mode}_{field}'
                    if name not in columns:
                        db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {decl}')

    @contextmanager
    def connection(self, *, write=False):
        """Create a bounded, explicitly closed SQLite connection.

        Args:
            write: Acquire the shared writer lock and start a transaction.

        Yields:
            A connection with named rows.
        """
        with self._lock:
            db = sqlite3.connect(self.path, timeout=3)
            db.row_factory = sqlite3.Row
            try:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("PRAGMA foreign_keys=ON")
                if write:
                    db.execute("BEGIN IMMEDIATE")
                yield db
                if write:
                    db.commit()
            except BaseException:
                db.rollback()
                raise
            finally:
                db.close()

    @staticmethod
    def _bump(db, umo, *, invalidate=False):
        """Advance the fact revision inside an existing transaction.

        Args:
            db: Active transaction.
            umo: Group session.
            invalidate: Suppress previously generated editions after a correction.
        """
        db.execute("INSERT OR IGNORE INTO groups(umo) VALUES(?)", (umo,))
        db.execute("UPDATE groups SET revision=revision+1 WHERE umo=?", (umo,))
        if invalidate:
            db.execute("UPDATE reports SET invalid=1 WHERE umo=?", (umo,))

    def group(self, umo, name=None):
        """Read or register a group.

        Args:
            umo: Group session.
            name: Optional display name.

        Returns:
            Group metadata.
        """
        with self.connection(write=True) as db:
            db.execute("INSERT OR IGNORE INTO groups(umo) VALUES(?)", (umo,))
            if name:
                db.execute("UPDATE groups SET name=? WHERE umo=?", (str(name), umo))
            return dict(
                db.execute("SELECT * FROM groups WHERE umo=?", (umo,)).fetchone()
            )

    def groups(self):
        """List known group sessions.

        Returns:
            Session identifiers.
        """
        with self.connection() as db:
            return [r[0] for r in db.execute("SELECT umo FROM groups ORDER BY umo")]

    def revision(self, umo):
        """Read a fact revision without registering a disallowed group.

        Args:
            umo: Group session.

        Returns:
            Revision number, zero for an unknown group.
        """
        with self.connection() as db:
            row = db.execute(
                "SELECT revision FROM groups WHERE umo=?", (umo,)
            ).fetchone()
            return row[0] if row else 0

    def snapshot(self, umo, *, since=0, uid=None, rules=None):
        """Read related facts and their revision under the same writer lock.

        Args:
            umo: Group session.
            since: Period lower bound.
            uid: Optional member identity.
            rules: Biography grade thresholds.

        Returns:
            Consistent facts, revision, member, and grade data.
        """
        with self._lock:
            return {
                "revision": self.revision(umo),
                "facts": self.events(umo, since=since, uid=uid),
                "member": self.member(umo, uid) if uid is not None else None,
                "grade": self.grade(umo, uid, rules) if uid is not None else None,
            }

    @staticmethod
    def _observe(db, umo, uid, name, ts):
        """Merge a reliable observation without inventing a join date.

        Args:
            db: Active transaction.
            umo: Group session.
            uid: Stable member identifier.
            name: Observed nickname.
            ts: Source timestamp.
        """
        uid = str(uid)
        if not uid or not math.isfinite(ts) or ts <= 0:
            return
        db.execute("INSERT OR IGNORE INTO groups(umo) VALUES(?)", (umo,))
        old = db.execute(
            "SELECT * FROM members WHERE umo=? AND uid=?", (umo, uid)
        ).fetchone()
        name = str(
            name
            if name and (str(name) != uid or not old)
            else old["name"]
            if old
            else uid
        )
        if old:
            aliases = json.loads(old["aliases"])
            if name != uid and name not in aliases:
                aliases.append(name)
            changed = aliases != json.loads(old["aliases"]) or ts < old["first_seen"]
            db.execute(
                """UPDATE members SET aliases=?, name=?, first_seen=MIN(first_seen,?),
                       last_seen=MAX(last_seen,?) WHERE umo=? AND uid=?""",
                (
                    encode(aliases),
                    name if ts >= old["last_seen"] else old["name"],
                    ts,
                    ts,
                    umo,
                    uid,
                ),
            )
            if changed:
                ArchiveStore._bump(db, umo, invalidate=bool(old["hidden"]))
        else:
            db.execute(
                "INSERT INTO members VALUES(?,?,?,?,?,?,0)",
                (umo, uid, name, encode([name] if name != uid else []), ts, ts),
            )
            ArchiveStore._bump(db, umo)
        periods = db.execute(
            "SELECT * FROM periods WHERE umo=? AND uid=? ORDER BY observed", (umo, uid)
        ).fetchall()
        for p in periods:
            start = p["joined"] if p["joined"] is not None else p["observed"]
            if start <= ts and (
                p["departed"] is None
                and not p["closed"]
                or p["departed"] is not None
                and ts <= p["departed"]
            ):
                return
        if periods and ts < periods[0]["observed"] and periods[0]["joined"] is None:
            db.execute(
                "UPDATE periods SET observed=? WHERE id=?", (ts, periods[0]["id"])
            )
        else:
            closed = bool(periods and ts < periods[-1]["observed"])
            db.execute(
                "INSERT INTO periods(umo,uid,observed,closed) VALUES(?,?,?,?)",
                (umo, uid, ts, int(closed)),
            )

    def observe(self, umo, uid, name, ts=None):
        """Record a message or roster observation.

        Args:
            umo: Group session.
            uid: Member identifier.
            name: Nickname.
            ts: Source timestamp, defaulting to now.
        """
        with self.connection(write=True) as db:
            self._observe(db, umo, uid, name, float(ts or time.time()))

    def roster(self, umo, members, now=None):
        """Add silent members without inferring departures from missing entries.

        Args:
            umo: Group session.
            members: Iterable of identifier/name pairs.
            now: Observation time.
        """
        now = float(now or time.time())
        with self.connection(write=True) as db:
            for uid, name in members:
                self._observe(db, umo, uid, name, now)
            db.execute(
                "UPDATE groups SET roster_at=?,unavailable=0 WHERE umo=?", (now, umo)
            )

    def membership(self, umo, uid, kind, ts, key, name=None, reason="", self_id=""):
        """Apply an idempotent OneBot membership notification.

        Args:
            umo: Group session.
            uid: Affected member, not the operator.
            kind: ``join`` or ``leave``.
            ts: Original notification timestamp.
            key: Stable deduplication key.
            name: Optional nickname.
            reason: Reported departure subtype.
            self_id: Bot identifier.

        Returns:
            Whether a new notification was applied.
        """
        if kind not in ("join", "leave") or not uid or not math.isfinite(ts) or ts <= 0:
            raise ValueError("无效的入退群通知")
        with self.connection(write=True) as db:
            if (
                db.execute("INSERT OR IGNORE INTO notices VALUES(?)", (key,)).rowcount
                == 0
            ):
                return False
            db.execute("INSERT OR IGNORE INTO groups(umo) VALUES(?)", (umo,))
            if str(uid) == str(self_id):
                db.execute(
                    "UPDATE groups SET unavailable=? WHERE umo=?",
                    (int(kind == "leave"), umo),
                )
                if kind == "leave":
                    gap = (
                        datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                        + "机器人离群，之后的记录可能缺失；不推断其他成员退群"
                    )
                    db.execute(
                        "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
                        (umo, "coverage", umo, "{}", str(uid), gap, ts),
                    )
                    job = db.execute(
                        "SELECT gaps FROM jobs WHERE umo=?", (umo,)
                    ).fetchone()
                    if job:
                        gaps = json.loads(job[0])
                        if gap not in gaps:
                            gaps.append(gap)
                        db.execute(
                            "UPDATE jobs SET gaps=? WHERE umo=?", (encode(gaps), umo)
                        )
                return True
            if kind == "join":
                old = db.execute(
                    "SELECT * FROM periods WHERE umo=? AND uid=? AND closed=0 ORDER BY observed DESC LIMIT 1",
                    (umo, str(uid)),
                ).fetchone()
                self._observe(db, umo, uid, name, ts)
                fresh = db.execute(
                    "SELECT * FROM periods WHERE umo=? AND uid=? AND closed=0 AND observed=? AND joined IS NULL ORDER BY id DESC LIMIT 1",
                    (umo, str(uid), ts),
                ).fetchone()
                if old is None and fresh:
                    db.execute(
                        "UPDATE periods SET joined=? WHERE id=?", (ts, fresh["id"])
                    )
                elif old and old["joined"] is None and ts <= old["observed"]:
                    db.execute(
                        "UPDATE periods SET joined=?,observed=MIN(observed,?) WHERE id=?",
                        (ts, ts, old["id"]),
                    )
                elif old and old["joined"] == ts:
                    pass
                else:
                    db.execute(
                        "UPDATE periods SET closed=1 WHERE umo=? AND uid=? AND closed=0",
                        (umo, str(uid)),
                    )
                    db.execute(
                        "INSERT INTO periods(umo,uid,observed,joined) VALUES(?,?,?,?)",
                        (umo, str(uid), ts, ts),
                    )
            else:
                member = db.execute(
                    "SELECT 1 FROM members WHERE umo=? AND uid=?", (umo, str(uid))
                ).fetchone()
                if not member:
                    self._observe(db, umo, uid, name, ts)
                p = db.execute(
                    "SELECT * FROM periods WHERE umo=? AND uid=? AND closed=0 AND observed<=? ORDER BY observed DESC LIMIT 1",
                    (umo, str(uid), ts),
                ).fetchone()
                if p:
                    db.execute(
                        "UPDATE periods SET departed=?,closed=1,reason=? WHERE id=?",
                        (ts, reason, p["id"]),
                    )
            self._bump(db, umo)
            return True

    def member(self, umo, uid):
        """Read one identity and all known membership periods.

        Args:
            umo: Group session.
            uid: Member identifier.

        Returns:
            A member dictionary or None.
        """
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM members WHERE umo=? AND uid=?", (umo, str(uid))
            ).fetchone()
            if not row:
                return None
            member = dict(row)
            member["aliases"] = json.loads(member["aliases"])
            member["periods"] = [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM periods WHERE umo=? AND uid=? ORDER BY observed,id",
                    (umo, str(uid)),
                )
            ]
            return member

    def hidden(self, umo):
        """Read identities excluded from public facts and extraction.

        Args:
            umo: Group session.

        Returns:
            Hidden member dictionaries.
        """
        with self.connection() as db:
            return [
                {**dict(r), "aliases": json.loads(r["aliases"])}
                for r in db.execute(
                    "SELECT * FROM members WHERE umo=? AND hidden=1", (umo,)
                )
            ]

    def privacy_active(self, umo):
        """Check whether old unstructured prose needs suppression.

        Args:
            umo: Group session.

        Returns:
            Whether any profile or event exclusion exists.
        """
        with self.connection() as db:
            return bool(
                db.execute(
                    "SELECT 1 FROM members WHERE umo=? AND hidden=1 UNION ALL SELECT 1 FROM hides WHERE umo=? LIMIT 1",
                    (umo, umo),
                ).fetchone()
            )

    def excluded_sources(self, umo):
        """Read source IDs protected by individual-event exclusions.

        Args:
            umo: Group session.

        Returns:
            String source IDs that must not be reintroduced during backfill.
        """
        with self.connection() as db:
            rows = db.execute(
                "SELECT e.payload FROM hides h JOIN events e ON e.id=h.event_id AND e.umo=h.umo WHERE h.umo=?",
                (umo,),
            )
            return {
                str(s["id"])
                for row in rows
                for s in json.loads(row[0]).get("sources", [])
            }

    def _excluded_event(self, db, umo, event):
        """Protect exact sources and uncertain repeats of an excluded event.

        Args:
            db: Active connection.
            umo: Owning group.
            event: New or stored candidate.

        Returns:
            Whether it matches a protected source or event family.
        """
        sources = {str(s["id"]) for s in event.get("sources", [])}
        involved = {p["uid"] for p in event.get("participants", [])} | {
            s.get("uid") for s in event.get("sources", [])
        }
        for row in db.execute(
            "SELECT h.uid,e.payload FROM hides h JOIN events e ON e.id=h.event_id AND e.umo=h.umo WHERE h.umo=?",
            (umo,),
        ):
            hidden = json.loads(row["payload"])
            if sources & {str(s["id"]) for s in hidden.get("sources", [])}:
                return True
            if hidden.get("fingerprint") == event.get("fingerprint"):
                return True
            if row["uid"] not in involved or hidden.get("kind") != event.get("kind"):
                continue
            end = max(
                [hidden["event_at"]]
                + [f["event_at"] for f in hidden.get("followups", [])]
            )
            close = (
                hidden.get("date_basis") == "recorded"
                or hidden.get("ongoing")
                or hidden["event_at"] - 7 * 86400
                <= event["event_at"]
                <= end + 7 * 86400
            )
            if (
                close
                and difflib.SequenceMatcher(
                    None, hidden["title"], event["title"]
                ).ratio()
                >= 0.65
            ):
                return True
        return False

    def _visible(self, db, umo, event):
        """Conservatively suppress facts that cannot safely be anonymized.

        Args:
            db: Active connection.
            umo: Group session.
            event: Decoded fact.

        Returns:
            Whether the event is safe for public use.
        """
        if db.execute(
            "SELECT 1 FROM hides WHERE umo=? AND event_id=?", (umo, event["id"])
        ).fetchone():
            return False
        if self._excluded_event(db, umo, event):
            return False
        identifiers = {str(p["uid"]) for p in event.get("participants", [])}
        identifiers.update(str(s.get("uid", "")) for s in event.get("sources", []))
        text = encode(
            [
                event.get("title", ""),
                event.get("detail", ""),
                event.get("followups", []),
                event.get("sources", []),
            ]
        )
        for row in db.execute(
            "SELECT uid,aliases FROM members WHERE umo=? AND hidden=1", (umo,)
        ):
            if row["uid"] in identifiers or private_text(
                text, {"uid": row["uid"], "aliases": json.loads(row["aliases"])}
            ):
                return False
        return True

    def events(self, umo, *, status="approved", since=0, public=True, uid=None):
        """Read bounded-by-time facts scoped to one group.

        Args:
            umo: Group session.
            status: Status filter, or None for all statuses.
            since: Inclusive timestamp lower bound.
            public: Apply privacy exclusions.
            uid: Optional participant filter.

        Returns:
            Facts in chronological order.
        """
        with self.connection() as db:
            query = "SELECT * FROM events WHERE umo=? AND last_event_at>=?"
            args = [umo, since]
            if status is not None:
                query += " AND status=?"
                args.append(status)
            rows = db.execute(query + " ORDER BY event_at,id", args)
            result = []
            for row in rows:
                event = {
                    **json.loads(row["payload"]),
                    "id": row["id"],
                    "status": row["status"],
                    "event_at": row["event_at"],
                    "revision": row["revision"],
                }
                if uid is not None and str(uid) not in {
                    str(p["uid"]) for p in event.get("participants", [])
                }:
                    continue
                if public and not self._visible(db, umo, event):
                    continue
                result.append(event)
            return result

    def event(self, umo, event_id, *, public=True):
        """Read an event while enforcing group ownership.

        Args:
            umo: Group session.
            event_id: Numeric event identifier.
            public: Apply privacy exclusions.

        Returns:
            The scoped event or None.
        """
        return next(
            (
                e
                for e in self.events(umo, status=None, public=public)
                if e["id"] == int(event_id)
            ),
            None,
        )

    def _put_event(self, db, umo, fact, status, actor="system", merge_into=None):
        """Insert a validated candidate or attach its follow-up atomically.

        Args:
            db: Active transaction.
            umo: Group session.
            fact: Validated fact including evidence.
            status: Initial admission status.
            actor: Audit actor.
            merge_into: Optional existing event in this group.

        Returns:
            Event identifier.
        """
        fingerprint = fact["fingerprint"]
        existing = db.execute(
            "SELECT id FROM events WHERE umo=? AND fingerprint=?", (umo, fingerprint)
        ).fetchone()
        if existing:
            return existing[0]
        now = time.time()
        if self._excluded_event(db, umo, fact):
            fact = {
                **fact,
                "review_note": "疑似已隐藏事件的重复或续记，请核实是否同一事件；不自动计贡献",
            }
            status = "pending"
        new_id = db.execute(
            "INSERT INTO events(umo,fingerprint,status,payload,event_at,last_event_at,updated) VALUES(?,?,?,?,?,?,?)",
            (
                umo,
                fingerprint,
                status,
                encode(fact),
                fact["event_at"],
                fact["event_at"],
                now,
            ),
        ).lastrowid
        if merge_into:
            merged = self._merge(db, umo, int(merge_into), new_id, actor)
            if not merged and status == "approved":
                self._bump(db, umo)
        elif status == "approved":
            self._bump(db, umo)
        return new_id

    def add_event(self, umo, fact, *, status="pending", actor="system"):
        """Store a validated manual clue.

        Args:
            umo: Group session.
            fact: Validated candidate.
            status: Initial admission state.
            actor: Submitting identity.

        Returns:
            Event identifier.
        """
        with self.connection(write=True) as db:
            return self._put_event(db, umo, fact, status, actor)

    def init_job(self, umo, high):
        """Freeze a historical snapshot once.

        Args:
            umo: Group session.
            high: Highest source message ID at registration.

        Returns:
            Durable checkpoint.
        """
        with self.connection(write=True) as db:
            db.execute("INSERT OR IGNORE INTO groups(umo) VALUES(?)", (umo,))
            gaps = [
                r[0]
                for r in db.execute(
                    "SELECT note FROM revisions WHERE umo=? AND entity='coverage' ORDER BY id",
                    (umo,),
                )
            ]
            db.execute(
                "INSERT OR IGNORE INTO jobs(umo,frozen_high,live_cursor,gaps,updated) VALUES(?,?,?,?,?)",
                (umo, int(high), int(high), encode(gaps), time.time()),
            )
            return dict(db.execute("SELECT * FROM jobs WHERE umo=?", (umo,)).fetchone())

    def job(self, umo):
        """Read one checkpoint.

        Args:
            umo: Group session.

        Returns:
            Checkpoint or None.
        """
        with self.connection() as db:
            row = db.execute("SELECT * FROM jobs WHERE umo=?", (umo,)).fetchone()
            return dict(row) if row else None

    def commit_batch(
        self, umo, mode, expected, cursor, offset, messages, facts, *, gap=None, fragment=None
    ):
        """Commit all candidates and the source checkpoint in one transaction.

        Args:
            umo: Group session.
            mode: ``back`` or ``live``.
            expected: Previous cursor/fragment offset pair.
            cursor: Last fully processed message ID.
            offset: Consumed characters in the next message.
            messages: Material actually processed by the extractor.
            facts: Validated candidates and their admission status.
            gap: Optional source deletion or coverage explanation.

        Returns:
            Whether the checkpoint matched and the batch committed.
        """
        if mode not in ("back", "live"):
            raise ValueError("Invalid checkpoint mode")
        with self.connection(write=True) as db:
            job = db.execute("SELECT * FROM jobs WHERE umo=?", (umo,)).fetchone()
            if not job or (job[f"{mode}_cursor"], job[f"{mode}_offset"]) != tuple(
                expected
            ):
                return False
            for m in messages:
                if m.get("uid") and m.get("role") == "user":
                    self._observe(db, umo, m["uid"], m.get("name"), float(m["ts"]))
            for fact in facts:
                self._put_event(
                    db, umo, fact, fact["admission"], merge_into=fact.get("merge_into")
                )
            times = [float(m["ts"]) for m in messages if float(m.get("ts", 0)) > 0]
            low = min(times) if times else 0
            high = max(times) if times else 0
            if job["coverage_min"]:
                low = min(low, job["coverage_min"]) if low else job["coverage_min"]
            gaps = json.loads(job["gaps"])
            if gap and gap not in gaps:
                gaps.append(gap)
            fragment = fragment or {}
            db.execute(
                f"""UPDATE jobs SET {mode}_cursor=?,{mode}_offset=?,{mode}_fragment_id=?,{mode}_fragment_hash=?,
                       processed=processed+?,coverage_min=?,coverage_max=MAX(coverage_max,?),
                       error='',gaps=?,updated=? WHERE umo=?""",
                (
                    int(cursor),
                    int(offset),
                    int(fragment.get('id',0)) if offset else 0,
                    str(fragment.get('sha256','')) if offset else '',
                    len(messages),
                    low,
                    high,
                    encode(gaps),
                    time.time(),
                    umo,
                ),
            )
            return True

    def job_error(self, umo, message):
        """Record an error without advancing progress or exposing raw source text.

        Args:
            umo: Group session.
            message: Public diagnostic summary.
        """
        with self.connection(write=True) as db:
            db.execute(
                "UPDATE jobs SET error=?,updated=? WHERE umo=?",
                (str(message)[:300], time.time(), umo),
            )

    def consume_quota(self, day, limit):
        """Reserve a global daily extraction slot, including failed attempts.

        Args:
            day: Calendar day in the configured timezone.
            limit: Maximum extraction tasks for that day.

        Returns:
            Whether a slot was acquired.
        """
        with self.connection(write=True) as db:
            db.execute("INSERT OR IGNORE INTO quota VALUES(?,0)", (day,))
            if (
                db.execute("SELECT used FROM quota WHERE day=?", (day,)).fetchone()[0]
                >= limit
            ):
                return False
            db.execute("UPDATE quota SET used=used+1 WHERE day=?", (day,))
            return True

    def quota(self, day):
        """Read global extraction usage.

        Args:
            day: Calendar day.

        Returns:
            Number of reserved tasks.
        """
        with self.connection() as db:
            row = db.execute("SELECT used FROM quota WHERE day=?", (day,)).fetchone()
            return row[0] if row else 0

    def _merge(self, db, umo, target_id, source_id, actor):
        """Merge follow-ups and roles without multiplying contribution.

        Args:
            db: Active transaction.
            umo: Group session.
            target_id: Retained event ID.
            source_id: Absorbed event ID.
            actor: Audit actor.
        """
        if target_id == source_id:
            raise ValueError("不能把事件合并到自身")
        rows = db.execute(
            "SELECT * FROM events WHERE umo=? AND id IN (?,?)",
            (umo, target_id, source_id),
        ).fetchall()
        by_id = {r["id"]: r for r in rows}
        if len(by_id) != 2 or any(r["status"] in ("merged", "rejected") for r in rows):
            raise ValueError("事件不存在、属于其他群或已被合并／驳回")
        target, source = by_id[target_id], by_id[source_id]
        a, b = json.loads(target["payload"]), json.loads(source["payload"])
        if actor == "system" and (target["status"] != source["status"]):
            return False
        db.execute(
            "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
            (
                umo,
                "event",
                str(target_id),
                target["payload"],
                actor,
                f"merge:{source_id}",
                time.time(),
            ),
        )
        participants = {}
        for p in a["participants"]:
            p = dict(p)
            credited = a["positive"] and a["kind"] not in ("dispute", "discipline")
            p.setdefault(
                "credit_points", a["importance"] * p["role"] if credited else 0
            )
            p.setdefault(
                "major_core_credit",
                bool(credited and a["importance"] == p["role"] == 3),
            )
            p.setdefault("first_at", a["event_at"])
            participants[p["uid"]] = p
        for p in b["participants"]:
            old = participants.get(p["uid"])
            credited = b["positive"] and b["kind"] not in ("dispute", "discipline")
            points = p.get(
                "credit_points", b["importance"] * p["role"] if credited else 0
            )
            major = p.get(
                "major_core_credit",
                bool(credited and b["importance"] == p["role"] == 3),
            )
            participants[p["uid"]] = {
                **p,
                "role": max(p["role"], old["role"] if old else 0),
                "credit_points": max(points, old["credit_points"] if old else 0),
                "major_core_credit": bool(major or old and old["major_core_credit"]),
                "first_at": min(
                    p.get("first_at", b["event_at"]),
                    old["first_at"] if old else b["event_at"],
                ),
            }
        a["participants"] = list(participants.values())
        a["importance"] = max(a["importance"], b["importance"])
        a["positive"] = bool(a["positive"] or b["positive"])
        a["sources"] = list(
            {
                (str(s.get("id")), s.get("hash", "")): s
                for s in a["sources"] + b["sources"]
            }.values()
        )
        a.setdefault("followups", []).append(
            {"event_at": b["event_at"], "detail": b["detail"], "sources": b["sources"]}
        )
        a["ongoing"] = b.get("ongoing", False)
        status = (
            "pending"
            if "pending" in (target["status"], source["status"])
            else "approved"
        )
        db.execute(
            "UPDATE events SET payload=?,status=?,last_event_at=MAX(last_event_at,?),updated=?,revision=revision+1 WHERE id=?",
            (encode(a), status, source["last_event_at"], time.time(), target_id),
        )
        db.execute(
            "UPDATE events SET status='merged',merged_into=? WHERE id=?",
            (target_id, source_id),
        )
        db.execute(
            "INSERT OR IGNORE INTO hides SELECT umo,uid,? FROM hides WHERE umo=? AND event_id=?",
            (target_id, umo, source_id),
        )
        self._bump(db, umo, invalidate=True)
        return True

    def review(
        self, umo, event_id, action, actor, *, note="", payload=None, target=None
    ):
        """Apply an administrator decision with a preserved previous revision.

        Args:
            umo: Group session.
            event_id: Scoped event ID.
            action: approve, reject, revise, or merge.
            actor: Administrator identifier.
            note: Decision explanation.
            payload: Full validated replacement for a revision.
            target: Retained target ID for a merge.
        """
        with self.connection(write=True) as db:
            row = db.execute(
                "SELECT * FROM events WHERE umo=? AND id=?", (umo, int(event_id))
            ).fetchone()
            if not row or row["status"] == "merged":
                raise ValueError("本群没有该事件，或事件已被合并")
            if action == "merge":
                self._merge(db, umo, int(target), int(event_id), actor)
                return
            if action not in ("approve", "reject", "revise"):
                raise ValueError("未知审核操作")
            before = encode(
                {"status": row["status"], "payload": json.loads(row["payload"])}
            )
            db.execute(
                "INSERT INTO revisions(umo,entity,entity_id,before_data,actor,note,created) VALUES(?,?,?,?,?,?,?)",
                (umo, "event", str(event_id), before, actor, note, time.time()),
            )
            fact = payload if payload is not None else json.loads(row["payload"])
            if action == "approve" and (
                fact.get("unknown_participants")
                or not fact.get("participants")
                or not fact.get("sources")
            ):
                raise ValueError("请先核实参与者身份和来源，再确认入史")
            if action == "revise":
                fact = dict(fact)
                old_fact = json.loads(row["payload"])
                changed = fact.pop("_recompute_credit", False) or any(
                    fact.get(k) != old_fact.get(k)
                    for k in ("importance", "positive", "kind")
                )
                changed |= sorted(
                    (p["uid"], p["role"]) for p in fact["participants"]
                ) != sorted((p["uid"], p["role"]) for p in old_fact["participants"])
                if changed:
                    fact["participants"] = [
                        {
                            k: v
                            for k, v in p.items()
                            if k not in ("credit_points", "major_core_credit")
                        }
                        for p in fact["participants"]
                    ]
            status = {
                "approve": "approved",
                "reject": "rejected",
                "revise": row["status"],
            }[action]
            last = max(
                [fact["event_at"]] + [f["event_at"] for f in fact.get("followups", [])]
            )
            db.execute(
                "UPDATE events SET status=?,payload=?,event_at=?,last_event_at=?,updated=?,revision=revision+1 WHERE id=?",
                (status, encode(fact), fact["event_at"], last, time.time(), event_id),
            )
            db.execute(
                "UPDATE corrections SET resolved=1 WHERE umo=? AND event_id=?",
                (umo, event_id),
            )
            self._bump(db, umo, invalidate=True)

    def correction(self, umo, event_id, uid, note):
        """Submit a correction without changing shared facts.

        Args:
            umo: Group session.
            event_id: Existing scoped event.
            uid: Requesting member.
            note: Suggested correction.

        Returns:
            Request ID.
        """
        if not self.event(umo, event_id, public=False):
            raise ValueError("本群没有该事件")
        with self.connection(write=True) as db:
            return db.execute(
                "INSERT INTO corrections(umo,event_id,uid,note,created) VALUES(?,?,?,?,?)",
                (umo, int(event_id), str(uid), str(note)[:3000], time.time()),
            ).lastrowid

    def corrections(self, umo):
        """List unresolved correction requests for administrators.

        Args:
            umo: Group session.

        Returns:
            Request dictionaries.
        """
        with self.connection() as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM corrections WHERE umo=? AND resolved=0 ORDER BY id",
                    (umo,),
                )
            ]

    def hide(self, umo, uid, event_id=None):
        """Immediately hide the requester's profile or an involved event.

        Args:
            umo: Group session.
            uid: Verified requesting identity.
            event_id: Optional event in which the requester is involved.
        """
        if event_id is not None:
            fact = self.event(umo, event_id, public=False)
            involved = (
                set()
                if not fact
                else {p["uid"] for p in fact["participants"]}
                | {s.get("uid") for s in fact["sources"]}
            )
            if str(uid) not in involved:
                raise ValueError(
                    "只能隐藏涉及自己的事件；共同事实的更正请提交管理员审核"
                )
        with self.connection(write=True) as db:
            if event_id is None:
                row = db.execute(
                    "SELECT 1 FROM members WHERE umo=? AND uid=?", (umo, str(uid))
                ).fetchone()
                if not row:
                    self._observe(db, umo, uid, uid, time.time())
                db.execute(
                    "UPDATE members SET hidden=1 WHERE umo=? AND uid=?", (umo, str(uid))
                )
            else:
                db.execute(
                    "INSERT OR IGNORE INTO hides VALUES(?,?,?)",
                    (umo, str(uid), int(event_id)),
                )
            self._bump(db, umo, invalidate=True)

    def grade(self, umo, uid, rules=None):
        """Derive a transparent lifetime grade from distinct admitted contributions.

        Args:
            umo: Group session.
            uid: Member identifier.
            rules: Optional threshold overrides.

        Returns:
            Grade, score, event IDs, and milestone/span explanations.
        """
        rules = rules or {}
        contributions = []
        for e in self.events(umo, public=False, uid=uid):
            if not e.get("positive") or e.get("kind") in ("dispute", "discipline"):
                continue
            person = next(
                (p for p in e["participants"] if str(p["uid"]) == str(uid)), None
            )
            points = (
                person.get("credit_points", e["importance"] * person["role"])
                if person
                else 0
            )
            if points:
                contributions.append(
                    {
                        "id": e["id"],
                        "title": e["title"],
                        "points": points,
                        "major_core": person.get(
                            "major_core_credit",
                            e["importance"] == 3 and person["role"] == 3,
                        ),
                        "ts": person.get("first_at", e["event_at"]),
                    }
                )
        score = sum(c["points"] for c in contributions)
        cores = sum(c["major_core"] for c in contributions)
        times = [c["ts"] for c in contributions]
        span = (max(times) - min(times)) / 86400 if times else 0
        grade = "小记"
        if (
            cores
            or score >= rules.get("bio_points", 12)
            and len(contributions) >= rules.get("bio_events", 3)
        ):
            grade = "小传"
        if (
            score >= rules.get("full_points", 40)
            and len(contributions) >= rules.get("full_events", 8)
            and span >= rules.get("full_span_days", 90)
            and cores >= rules.get("full_major_core", 2)
        ):
            grade = "列传"
        return {
            "grade": grade,
            "points": score,
            "events": len(contributions),
            "major_core": cores,
            "span_days": round(span, 1),
            "basis": contributions,
        }

    def top(self, umo, since, limit):
        """Choose appendix members from this period's admitted contributions.

        Args:
            umo: Group session.
            since: Period start timestamp.
            limit: Display count, never an enrollment limit.

        Returns:
            Ranked visible member identifiers.
        """
        scores = {}
        for event in self.events(umo, since=since):
            if event.get("positive") and event.get("kind") not in (
                "dispute",
                "discipline",
            ):
                for p in event["participants"]:
                    points = p.get("credit_points", p["role"] * event["importance"])
                    if points:
                        scores[p["uid"]] = scores.get(p["uid"], 0) + points
        return sorted(scores, key=lambda uid: (-scores[uid], uid))[:limit]

    def cache(self, umo, key):
        """Read a reusable article edition.

        Args:
            umo: Group session.
            key: Facts/privacy/style cache fingerprint.

        Returns:
            Stored entry or None.
        """
        with self.connection() as db:
            row = db.execute(
                "SELECT id,entry FROM reports WHERE umo=? AND (cache_key=? OR cache_key LIKE ? OR cache_key LIKE ?) AND invalid=0 ORDER BY created DESC,id DESC LIMIT 1",
                (umo, key, key + ":forced:%", key + ":revision:%"),
            ).fetchone()
            return {**json.loads(row["entry"]), "report_id": row["id"]} if row else None

    def save_report(self, umo, key, entry, *, expected_revision=None):
        """Save a new edition without overwriting previous revisions.

        Args:
            umo: Group session.
            key: Unique cache key.
            entry: Compatible chronicle entry.
            expected_revision: Optional optimistic fact revision checked atomically.

        Returns:
            Stored report ID.
        """
        with self.connection(write=True) as db:
            if expected_revision is not None:
                group = db.execute(
                    "SELECT revision FROM groups WHERE umo=?", (umo,)
                ).fetchone()
                if (group[0] if group else 0) != expected_revision:
                    raise ValueError("档案事实已变化，本次文章未保存，请重新生成")
            row = db.execute(
                "SELECT id,invalid FROM reports WHERE umo=? AND cache_key=?", (umo, key)
            ).fetchone()
            if row and not row["invalid"]:
                return row["id"]
            if row:
                key += ":revision:" + str(time.time_ns())
            entry = {**entry, "umo": umo, "archive": True, "cache_key": key}
            if entry.get("report_kind") == "chronicle":
                entry["version"] = (
                    db.execute(
                        "SELECT COUNT(*) FROM reports WHERE umo=? AND cache_key LIKE 'chronicle:%'",
                        (umo,),
                    ).fetchone()[0]
                    + 1
                )
            return db.execute(
                "INSERT INTO reports(umo,cache_key,entry,created) VALUES(?,?,?,?)",
                (umo, key, encode(entry), float(entry.get("created_at", time.time()))),
            ).lastrowid

    def public_report(self, umo, report_id, text):
        """Check an already fetched edition immediately before export or delivery.

        Args:
            umo: Group session.
            report_id: Immutable edition identifier.
            text: Exact requested content.

        Returns:
            Whether the edition has not been invalidated or hidden.
        """
        with self.connection() as db:
            row = db.execute(
                "SELECT entry FROM reports WHERE umo=? AND id=? AND invalid=0",
                (umo, report_id),
            ).fetchone()
            if not row:
                return False
            entry = json.loads(row[0])
            return entry.get("chronicle_text") == text and not (
                entry.get("legacy") and self.privacy_active(umo)
            )

    def reports(self, umo, *, include_bios=False):
        """List public editions, suppressing legacy prose after a privacy change.

        Args:
            umo: Group session.
            include_bios: Also include biography-only cache entries.

        Returns:
            Compatible entries ordered newest first.
        """
        hidden = self.privacy_active(umo)
        with self.connection() as db:
            result = []
            for row in db.execute(
                "SELECT * FROM reports WHERE umo=? AND invalid=0 ORDER BY created DESC,id DESC",
                (umo,),
            ):
                e = {**json.loads(row["entry"]), "report_id": row["id"]}
                if not include_bios and e.get("report_kind") == "biography":
                    continue
                if hidden and e.get("legacy"):
                    continue
                result.append(e)
            return result

    def mark_delivered(self, umo, text):
        """Record confirmed delivery without adding a content revision.

        Args:
            umo: Group session.
            text: Exact delivered text.

        Returns:
            Whether an edition matched.
        """
        with self.connection(write=True) as db:
            for row in db.execute(
                "SELECT id,entry FROM reports WHERE umo=? AND invalid=0 ORDER BY id DESC",
                (umo,),
            ).fetchall():
                entry = json.loads(row["entry"])
                if entry.get("chronicle_text") == text:
                    entry["delivery_complete"] = True
                    db.execute(
                        "UPDATE reports SET entry=? WHERE id=?",
                        (encode(entry), row["id"]),
                    )
                    return True
            return False

    def import_legacy(self, entries):
        """Import old generated prose as editions, never as factual evidence.

        Args:
            entries: Existing JSON entry dictionaries.

        Returns:
            Newly imported edition count.
        """
        imported = 0
        with self.connection(write=True) as db:
            for entry in entries:
                if not isinstance(entry, dict) or not entry.get("umo"):
                    continue
                e = dict(entry)
                key = "legacy:" + hashlib.sha256(encode(e).encode()).hexdigest()
                if db.execute(
                    "SELECT 1 FROM reports WHERE umo=? AND cache_key=?", (e["umo"], key)
                ).fetchone():
                    continue
                e.update(
                    legacy=True,
                    archive=True,
                    delivery_complete=e.get("delivery_complete", True),
                    cache_key=key,
                    report_kind="legacy",
                )
                if not e.get("created_at"):
                    try:
                        e["created_at"] = datetime.fromisocalendar(
                            int(e["iso_year"]), int(e["iso_week"]), 1
                        ).timestamp()
                    except (ValueError, KeyError, TypeError):
                        e["created_at"] = time.time()
                db.execute(
                    "INSERT OR IGNORE INTO groups(umo,name) VALUES(?,?)",
                    (e["umo"], str(e.get("group_name") or "群聊")),
                )
                db.execute(
                    "INSERT INTO reports(umo,cache_key,entry,created) VALUES(?,?,?,?)",
                    (e["umo"], key, encode(e), e["created_at"]),
                )
                imported += 1
        return imported


class EditionStore:
    """Keep existing list/export/send entrypoints compatible with lifetime editions."""

    def __init__(self, archive, legacy):
        """Attach both stores without modifying the legacy file.

        Args:
            archive: Lifetime database.
            legacy: Existing JSON store.
        """
        self.archive, self.legacy = archive, legacy

    def list(self, umo):
        """List compatible, privacy-filtered editions.

        Args:
            umo: Group session.

        Returns:
            New and unmigrated legacy entries.
        """
        editions = self.archive.reports(umo)
        with self.archive.connection() as db:
            imported = {
                r[0]
                for r in db.execute(
                    "SELECT cache_key FROM reports WHERE umo=? AND cache_key LIKE 'legacy:%'",
                    (umo,),
                )
            }
        if not self.archive.privacy_active(umo):
            for e in self.legacy.list(umo):
                key = "legacy:" + hashlib.sha256(encode(e).encode()).hexdigest()
                if key not in imported:
                    editions.append(e)
        return sorted(
            editions,
            key=lambda e: (float(e.get("created_at", 0)), int(e.get("report_id", 0))),
            reverse=True,
        )

    def get_by_id(self, umo, idx):
        """Resolve the existing one-based public list index.

        Args:
            umo: Group session.
            idx: One-based index.

        Returns:
            Entry or None.
        """
        entries = self.list(umo)
        return entries[idx - 1] if 1 <= idx <= len(entries) else None

    def get(self, umo, year, week):
        """Return the latest edition for the ISO week.

        Args:
            umo: Group session.
            year: ISO year.
            week: ISO week.

        Returns:
            Entry or None.
        """
        return next(
            (
                e
                for e in self.list(umo)
                if e.get("iso_year") == year and e.get("iso_week") == week
            ),
            None,
        )

    def save(self, entry):
        """Route old workflows to their unchanged JSON compatibility store.

        Args:
            entry: Compatible entry dictionary.
        """
        if entry.get("archive"):
            self.archive.save_report(entry["umo"], entry["cache_key"], entry)
        else:
            self.legacy.save(entry)

    def should_skip(self, umo, year, week, newest_ts):
        """Retain the old workflow's deduplication contract.

        Args:
            umo: Group session.
            year: ISO year.
            week: ISO week.
            newest_ts: Source watermark.

        Returns:
            Whether a delivered result covers the watermark.
        """
        entry = self.get(umo, year, week)
        return bool(
            entry
            and entry.get("delivery_complete", True)
            and entry.get("newest_msg_ts", 0) >= newest_ts
        )

    def mark_delivered(self, umo, text):
        """Persist delivery in the matching store.

        Args:
            umo: Group session.
            text: Exact delivered text.

        Returns:
            Whether a result was updated.
        """
        return self.archive.mark_delivered(umo, text) or self.legacy.mark_delivered(
            umo, text
        )
