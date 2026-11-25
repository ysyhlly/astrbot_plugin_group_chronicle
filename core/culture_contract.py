"""Evidence-bound culture proposals. Model assertions never supply source text."""

from __future__ import annotations

import hashlib
import json
import math
import re
import time

from .archive_store import encode
from .lifetime import (
    RISK_WORDS,
    SHARED_ARCHIVE_EXTRACTION_PREAMBLE,
    build_group_framing_block,
)

PROMPT = (
    SHARED_ARCHIVE_EXTRACTION_PREAMBLE
    + """

任务：群文化（梗、黑话、外号与名场面）抽取与沿革归档。
输入 JSON 结构（按变更频率从低到高排列）：
- existing：本群已有文化词条列表（按 id 升序，仅用于判断新材料是否属于同一含义词条的补充/沿革，禁止抄写旧词条未出现在本批的来源 ID）
- learning_clues：可选的外部黑话线索（仅供参考，绝不是原文依据）
- messages：本批待整理的用户消息列表（含 id、uid、name、ts、content、fragment）
- review_candidate（可选，仅单独复核时出现）：本次需聚焦核验的候选词名，证据不足则返回 entries=[]

抽取与溯源细则：
1. 普通词、一次拼写错误、没有特殊含义的人名不收录；仅收录在群内具备明确特指含义或代表性场面的梗、黑话、外号与名场面。
2. 词义与起源严格分开：最早见到不等于起源，不根据流行度推断创造者；起源未知填 null；追述、猜测或冲突不能作为已确认起源。
3. support 分类：只有原消息直接、独立解释或验证同一词义时填 independent；引用、复读、附和填 repeat；转述填 reported；其余场景填 context。
4. kind 分类：slang（黑话）、meme（梗）、nickname（外号）、scene（名场面）。外号 kind=nickname，涉及成员的 subjects 仅填材料中的真实 uid 或明确 [At:uid] 中的 uid；无法确认身份标 risk=true。
5. 合并与建新：不同含义分别建词条；明确同一含义的新证据/后续用 target_id 指向 existing 中对应整数 id，不要凭相似字形合并。
6. aliases 与 timeline：aliases 仅填写来源中明确建立同义关系的称呼并附本批 source_ids；每段解释、时间线与起源都必须引用本批材料，event_at 无充分证据填 null。
7. 学习线索不是证据；未出现在本批 messages 中的历史来源 ID 不能引用。没有符合条件的候选立即直接输出单行 {"entries":[]}，不要过度分析。
8. 简洁约束：meaning（<=40字）、usage（<=30字）、timeline.text（<=40字）、origin.text（<=50字）均用一句短句精炼表述；quote 仅摘取最短必要逐字片段（<=80字，不复述无关长文）。
9. 只输出单行紧凑 JSON 对象：{"entries":[{"name":"词","meaning":"一句含义","usage":"一句场景","kind":"slang|meme|nickname|scene","target_id":null,"subjects":[],"risk":false,"conflict":false,"sources":[{"id":1,"quote":"最短必要逐字原文","support":"independent|repeat|reported|context"}],"aliases":[{"name":"别称","source_ids":[1]}],"origin":null,"timeline":[{"text":"一句变化或代表事件","source_ids":[1],"event_at":null}]}]}；origin 非空时格式为 {"text":"一句起源说明","source_ids":[1,2],"event_at":null}。"""
)


def build_culture_system_prompt(umo: str, group_name: str = "") -> str:
    """Build deterministic static system_prompt for culture extraction."""
    framing = build_group_framing_block(umo, group_name)
    return f"{PROMPT}\n\n{framing}" if framing else PROMPT


def digest(text):
    return hashlib.sha256(str(text).encode()).hexdigest()


def bounded_text(value, limit, required=False):
    if (
        not isinstance(value, str)
        or len(value) > limit
        or (required and not value.strip())
    ):
        raise ValueError("文化文本字段无效或过长")
    return value.strip()


def normalized(text):
    return re.sub(r"[\W_]+", "", str(text)).casefold()


def independent_sources(sources):
    """Require distinct authors and distinct non-quoted original explanations."""
    seen, authors, ids = set(), set(), set()
    for s in sources:
        text = normalized(s["quote"])
        if s.get("support") != "independent" or s.get("quoted") or len(text) < 4:
            continue
        if text in seen or s["uid"] in authors:
            continue
        seen.add(text)
        authors.add(s["uid"])
        ids.add(s["id"])
    return ids


def _candidate_list(raw):
    if isinstance(raw, str):
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.IGNORECASE | re.DOTALL)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.IGNORECASE)
        raw = json.loads(raw)
    if (
        not isinstance(raw, dict)
        or not isinstance(raw.get("entries"), list)
        or len(raw["entries"]) > 20
    ):
        raise ValueError("文化抽取结果不是有效词条列表")
    return raw["entries"]


def validate_candidates(raw, material, existing=(), *, target_name=""):
    """Isolate malformed suggestions without weakening the strict single-candidate validator."""
    accepted, rejected = [], []
    for candidate in _candidate_list(raw):
        candidate = candidate if isinstance(candidate, dict) else {}
        name = candidate.get("name")
        name = name.strip() if isinstance(name, str) and len(name) <= 80 else ""
        if target_name and name != target_name:
            continue
        try:
            accepted.extend(validate({"entries": [candidate]}, material, existing))
        except (ValueError, KeyError, TypeError, AttributeError):
            refs = candidate.get("sources")
            cited = {str(ref.get("id")) for ref in refs if isinstance(ref, dict)} if isinstance(refs, list) else set()
            named = [m for m in material if name and name in m["content"]]
            focus = [str(m["id"]) for m in material if str(m["id"]) in cited or m in named]
            rejected.append({"name": name if named else "", "ids": focus})
    return accepted, rejected


def validate(raw, material, existing=()):
    candidates = _candidate_list(raw)
    rows = {str(m["id"]): m for m in material}
    known = {e["id"]: e for e in existing}
    members = {m["uid"] for m in material}
    for m in material:
        members.update(re.findall(r"\[At:([^\]\s]+)\]", m["content"]))
    entries = []
    for item in candidates:
        if not isinstance(item, dict):
            raise ValueError("文化候选格式无效")
        name = bounded_text(item.get("name"), 80, True)
        meaning = bounded_text(item.get("meaning"), 1000, True)
        usage = bounded_text(item.get("usage", ""), 500)
        kind = item.get("kind")
        if kind not in {"slang", "meme", "nickname", "scene"}:
            raise ValueError("文化类型无效")
        refs = item.get("sources", [])
        if not isinstance(refs, list) or not 1 <= len(refs) <= 12:
            raise ValueError("每个候选需要1至12条原文依据")
        sources = []
        for ref in refs:
            row = rows.get(str(ref.get("id")))
            quote = bounded_text(ref.get("quote"), 400, True)
            if not row or row["role"] != "user" or quote not in row["content"]:
                raise ValueError("文化候选引用了未提供的原文")
            support = ref.get("support")
            if support not in {"independent", "repeat", "reported", "context"}:
                raise ValueError("来源支持类型无效")
            sources.append(
                {
                    "id": str(row["id"]),
                    "uid": row["uid"],
                    "name": row["name"],
                    "ts": row["ts"],
                    "hash": row.get("_source_hash", digest(row["content"])),
                    "quote": quote,
                    "support": support,
                    "quoted": bool(row.get("_quoted")),
                }
            )
        sources = list({(s["id"], s["quote"]): s for s in sources}.values())
        source_ids = {s["id"] for s in sources}
        if not any(
            name in r["content"] for r in rows.values() if str(r["id"]) in source_ids
        ):
            raise ValueError("词语不在引用材料中")

        def node(value):
            if not isinstance(value, dict):
                raise ValueError("沿革节点无效")
            text = bounded_text(value.get("text"), 800, True)
            ids = value.get("source_ids")
            if (
                not isinstance(ids, list)
                or not ids
                or not {str(i) for i in ids} <= source_ids
            ):
                raise ValueError("沿革节点缺少本批原文依据")
            event_at = value.get("event_at")
            if event_at is not None:
                if (
                    isinstance(event_at, bool)
                    or not isinstance(event_at, (float, int))
                    or not math.isfinite(event_at)
                    or not 0 < event_at <= time.time() + 30
                ):
                    raise ValueError("事件发生时间无效")
            return {
                "text": text,
                "source_ids": sorted({str(i) for i in ids}),
                "event_at": event_at,
                "recorded_at": min(
                    s["ts"] for s in sources if s["id"] in {str(i) for i in ids}
                ),
            }

        timeline = item.get("timeline", [])
        aliases = item.get("aliases", [])
        subjects = item.get("subjects", [])
        if (
            not isinstance(timeline, list)
            or len(timeline) > 10
            or not isinstance(aliases, list)
            or len(aliases) > 10
            or not isinstance(subjects, list)
            or len(subjects) > 20
        ):
            raise ValueError("文化字段数量无效")
        subjects = list(dict.fromkeys(str(v) for v in subjects))
        unknown_subject = not set(subjects) <= members
        subjects = [s for s in subjects if s in members]
        alias_results = []
        for alias in aliases:
            alias_name = bounded_text(alias.get("name"), 80, True)
            proof = node({"text": alias_name, "source_ids": alias.get("source_ids")})
            if not any(alias_name in rows[i]["content"] for i in proof["source_ids"]):
                raise ValueError("别称缺少原文依据")
            alias_results.append({"name": alias_name, "source_ids": proof["source_ids"]})
        origin = node(item["origin"]) if item.get("origin") is not None else None
        target = item.get("target_id")
        if target is not None and (type(target) is not int or target not in known):
            raise ValueError("沿革目标不属于本次本群候选")
        conflict = item.get("conflict") is not False
        risk = (
            item.get("risk") is not False
            or unknown_subject
            or kind == "nickname"
            or any(w in encode(item) for w in RISK_WORDS)
        )
        independent = independent_sources(sources)
        if origin:
            # Independent support for a meaning does not independently prove its origin.
            # Origin claims receive a separate human review in v1.
            origin["confirmed"] = False
        status = (
            "approved"
            if len(independent) >= 2 and not risk and not conflict
            else "pending"
        )
        if origin and not origin["confirmed"]:
            status = "pending"
        entries.append(
            {
                "name": name,
                "meaning": meaning,
                "usage": usage,
                "kind": kind,
                "subjects": subjects,
                "aliases": alias_results,
                "origin": origin,
                "timeline": [node(n) for n in timeline],
                "sources": sources,
                "risk": risk,
                "conflict": conflict,
                "target_id": target,
                "admission": status,
                "first_seen": min(s["ts"] for s in sources),
                "fingerprint": digest(
                    encode(
                        [
                            name,
                            meaning,
                            [(s["id"], s["hash"], s["quote"]) for s in sources],
                        ]
                    )
                ),
            }
        )
    return entries
