"""Wave 1 feature and contract tests for astrbot_plugin_group_chronicle.

Covers:
1. Reversible privacy hide with scope preview, 120s TTL confirmation, global sync to
   chat_dynamics.set_series_privacy_muted, and auditable 史记恢复 / 文化恢复 + WebUI restore.
2. Tiered 史记帮助 (/群史记帮助) separating member vs admin commands, plus build_report()
   staged progress callbacks and golden_quotes / slang / relation_arc integration.
3. Culture WebUI quota breakdown and manual op="run_once" trigger (back + live fallback).
4. Series contracts: set_series_scope_enabled (history + culture with optimistic lock),
   activate_series_runtime(), normalized get_culture_context(umo, query, limit), and
   shared provider inheritance from get_series_shared_config().
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from core import culture_service, history_commands, lifetime
from core.history_commands import handle_command
from main import GroupChroniclePlugin

from .test_culture import culture, ingest, proposal, rows  # noqa: F401
from .test_history import Event, U, V, fact, message, plugin, source  # noqa: F401


def _attach_chat_dynamics(plugin_inst, *, shared_cfg=None, privacy_log=None):
    log = privacy_log if privacy_log is not None else []
    cfg = shared_cfg if shared_cfg is not None else {}

    class FakeChatDynamics:
        def get_series_shared_config(self):
            return dict(cfg)

        async def set_series_privacy_muted(
            self,
            umo: str,
            member_id: str,
            muted: bool,
            *,
            scopes=None,
            actor: str = "",
            reason: str = "",
        ):
            log.append(
                {
                    "umo": umo,
                    "member_id": member_id,
                    "muted": muted,
                    "scopes": list(scopes or []),
                    "actor": actor,
                    "reason": reason,
                }
            )
            return {"ok": True, "muted": muted, "scopes": list(scopes or [])}

    owner = FakeChatDynamics()
    star = NS(name="astrbot_plugin_chat_dynamics", activated=True, star_cls=owner)
    plugin_inst.context.get_all_stars = lambda: {"chat": star}
    plugin_inst.context.get_registered_star = (
        lambda ident: star if ident == "astrbot_plugin_chat_dynamics" else None
    )
    return log


# ---------------------------------------------------------------------------
# 1. Privacy hide preview/confirm, global mute sync, and auditable restore
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_hide_preview_confirm_cancel_expiry_and_local_scope(plugin):  # noqa: F811
    plugin.history.store.observe(U, "1", "甲")
    privacy_log = _attach_chat_dynamics(plugin)

    # First call: preview only, does NOT hide yet
    preview = await handle_command(plugin.history, Event("史记隐藏", uid="1"))
    assert "作用域预览" in preview
    assert "全局" in preview
    assert "120 秒" in preview
    assert plugin.history.store.member(U, "1")["hidden"] == 0

    # Cancel clears pending request
    cancelled = await handle_command(plugin.history, Event("史记隐藏 取消", uid="1"))
    assert "已取消" in cancelled
    expired_confirm = await handle_command(plugin.history, Event("史记隐藏 确认", uid="1"))
    assert "没有待确认" in expired_confirm

    # Preview again then simulate TTL expiry
    await handle_command(plugin.history, Event("史记隐藏", uid="1"))
    plugin.history.pending_hides[(U, "1")]["expires_at"] = time.monotonic() - 1.0
    after_ttl = await handle_command(plugin.history, Event("史记隐藏 确认", uid="1"))
    assert "超过 120 秒" in after_ttl
    assert plugin.history.store.member(U, "1")["hidden"] == 0

    # Preview then confirm local-only hide (does NOT call chat_dynamics global mute)
    await handle_command(plugin.history, Event("史记隐藏", uid="1"))
    done = await handle_command(plugin.history, Event("史记隐藏 确认", uid="1"))
    assert "已隐藏" in done
    assert plugin.history.store.member(U, "1")["hidden"] == 1
    assert privacy_log == []

    # Self restore clears hidden and records audit revision
    restored = await handle_command(plugin.history, Event("史记恢复", uid="1"))
    assert "已恢复" in restored and "审计日志" in restored
    assert plugin.history.store.member(U, "1")["hidden"] == 0
    with plugin.history.store.connection() as db:
        entities = [
            r[0]
            for r in db.execute(
                "SELECT entity FROM revisions WHERE umo=? ORDER BY id", (U,)
            ).fetchall()
        ]
    assert "privacy_hide_member" in entities
    assert "privacy_restore_member" in entities


@pytest.mark.asyncio
async def test_history_hide_global_and_admin_restore(plugin):  # noqa: F811
    plugin.history.store.observe(U, "1", "甲")
    plugin.history.store.observe(U, "2", "乙")
    privacy_log = _attach_chat_dynamics(plugin)

    # First local preview, then user switches to 全局 -> requires fresh preview for 全局
    await handle_command(plugin.history, Event("史记隐藏", uid="1"))
    global_preview = await handle_command(plugin.history, Event("史记隐藏 全局", uid="1"))
    assert "全局隐私静音" in global_preview
    assert plugin.history.store.member(U, "1")["hidden"] == 0
    assert privacy_log == []

    # Confirm global hide
    global_done = await handle_command(plugin.history, Event("史记隐藏 确认", uid="1"))
    assert "已隐藏" in global_done and "全局隐私静音" in global_done
    assert plugin.history.store.member(U, "1")["hidden"] == 1
    assert len(privacy_log) == 1
    assert privacy_log[0]["muted"] is True
    assert privacy_log[0]["scopes"] == ["global"]
    assert privacy_log[0]["member_id"] == "1"

    # Non-admin cannot restore another member
    denied = await handle_command(
        plugin.history, Event("史记恢复", uid="2", admin=False, mentions=("1",))
    )
    assert "需要本群管理员" in denied
    assert plugin.history.store.member(U, "1")["hidden"] == 1

    # Admin restores member 1 with 全局 -> restores archive and unmutes series privacy
    admin_ok = await handle_command(
        plugin.history, Event("史记恢复 全局", uid="2", admin=True, mentions=("1",))
    )
    assert "已恢复" in admin_ok and "解除跨插件全局隐私静音" in admin_ok
    assert plugin.history.store.member(U, "1")["hidden"] == 0
    assert len(privacy_log) == 2
    assert privacy_log[1]["muted"] is False
    assert privacy_log[1]["member_id"] == "1"
    assert privacy_log[1]["actor"] == "2"


@pytest.mark.asyncio
async def test_history_event_hide_and_restore(plugin):  # noqa: F811
    plugin.history.store.observe(U, "1", "甲")
    eid = plugin.history.store.add_event(U, fact("ev-hide", uid="1"), status="approved")

    # Preview then repeat-command confirm
    p = await handle_command(plugin.history, Event(f"史记隐藏 {eid}", uid="1"))
    assert "作用域预览" in p and f"#{eid}" in p
    assert len(plugin.history.store.events(U)) == 1

    done = await handle_command(plugin.history, Event(f"史记隐藏 {eid}", uid="1"))
    assert "已隐藏" in done
    assert plugin.history.store.events(U) == []

    # Restore event
    restored = await handle_command(plugin.history, Event(f"史记恢复 {eid}", uid="1"))
    assert "已恢复" in restored
    assert [e["id"] for e in plugin.history.store.events(U)] == [eid]


@pytest.mark.asyncio
async def test_culture_hide_confirm_global_restore_and_webui_restore(
    culture, monkeypatch  # noqa: F811
):
    material = rows()
    raw = proposal(material)
    second = proposal(material, name="蓝蘑菇", meaning="另一个释义")["entries"][0]
    raw["entries"].append(second)
    db, _ = await ingest(culture, monkeypatch, material, raw)
    privacy_log = _attach_chat_dynamics(culture.plugin)

    try:
        entries, _ = culture.store.entries(U)
        e1, e2 = entries[0], entries[1]

        # Preview first, not hidden yet
        prev = await culture.command(Event(uid="1"), "文化隐藏", f"{e1['id']} 全局")
        assert "文化隐藏作用域预览" in prev and "全局隐私静音" in prev
        assert culture.store.entry(U, e1["id"])["status"] == "approved"
        assert privacy_log == []

        # Confirm executes hide + global mute + writes culture_exclusions
        done = await culture.command(Event(uid="1"), "文化隐藏", "确认")
        assert "已隐藏" in done and "全局隐私静音" in done
        assert culture.store.entry(U, e1["id"])["status"] == "hidden"
        assert culture.store.excluded(U, "synthetic-source")
        assert len(privacy_log) == 1 and privacy_log[0]["muted"] is True

        # WebUI detail still hides text while exposing can_restore=True
        detail = await culture.plugin.culture_web.dispatch(
            {"op": "detail", "scope": U, "id": e1["id"]}, "human:admin"
        )
        assert detail["entry"]["unavailable"] is True
        assert detail["entry"]["can_restore"] is True
        assert "蓝蘑菇" not in json.dumps(detail, ensure_ascii=False)

        # Unrelated non-admin cannot restore
        with pytest.raises(PermissionError):
            await culture.command(Event(uid="99"), "文化恢复", str(e1["id"]))

        # Self restore with 全局 lifts hidden, removes exclusions, writes audit revision, unmutes
        restored = await culture.command(Event(uid="1"), "文化恢复", f"{e1['id']} 全局")
        assert "已撤销" in restored and "已公开" in restored and "解除跨插件" in restored
        assert culture.store.entry(U, e1["id"])["status"] == "approved"
        assert culture.store.excluded(U, "synthetic-source") == set()
        assert len(privacy_log) == 2 and privacy_log[1]["muted"] is False
        revs = culture.store.revisions(U, e1["id"])
        assert any(r["action"] == "restore" for r in revs)

        # If BOTH e1 and e2 are hidden and only e1 is restored via WebUI,
        # shared exclusions remain protected for still-hidden e2!
        cur1 = culture.store.entry(U, e1["id"])
        cur2 = culture.store.entry(U, e2["id"])
        await culture.action(U, "hide", "1", entry_id=cur1["id"], revision=cur1["revision"], note="隐藏1")
        await culture.action(U, "hide", "1", entry_id=cur2["id"], revision=cur2["revision"], note="隐藏2")
        hid1 = culture.store.entry(U, e1["id"])
        web_res = await culture.plugin.culture_web.dispatch(
            {
                "op": "action",
                "scope": U,
                "action": "restore",
                "id": hid1["id"],
                "revision": hid1["revision"],
                "note": "管理员面板撤销隐藏",
            },
            "human:admin",
        )
        assert web_res["ok"] is True
        assert web_res["status"] == "approved"
        # Shared sources still excluded because e2 is still hidden
        assert culture.store.excluded(U, "synthetic-source") == {"1", "2"}
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# 2. Tiered 史记帮助 & build_report progress + quotes/slang/relation_arc
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chronicle_help_command_distinguishes_member_and_admin(plugin):  # noqa: F811
    ev = Event("史记帮助", uid="1")
    out = [part async for part in plugin.chronicle_help_command(ev)]
    text = "\n".join(out)
    assert "【群友常用指令】" in text
    assert "【管理员审核 / 运维指令】" in text
    assert "史记恢复" in text
    assert "文化恢复" in text
    assert "史记待审" in text


@pytest.mark.asyncio
async def test_build_report_emits_staged_progress_and_integrates_quotes_slang_and_relations(
    culture, monkeypatch  # noqa: F811
):
    plugin_inst = culture.plugin
    plugin_inst.config.update(
        {
            "history_enabled": True,
            "culture_enabled": True,
            "enable_golden_quotes": True,
            "enable_slang": True,
            "enable_relation_arc": True,
        }
    )
    material = rows()
    db, _ = await ingest(culture, monkeypatch, material)
    try:
        plugin_inst.history.store.observe(U, "1", "甲")
        plugin_inst.history.store.observe(U, "2", "乙")
        plugin_inst.history.store.observe(U, "3", "隐藏人")
        plugin_inst.history.store.hide(U, "3")
        ev_fact = fact("q1", uid="1", role=3, importance=3)
        ev_fact["sources"] = [
            {
                "id": "101",
                "uid": "1",
                "hash": "h",
                "excerpt": "今天大家通力协作把核心版本一口气上线成功了！",
            }
        ]
        plugin_inst.history.store.add_event(U, ev_fact, status="approved")

        captured_prompts = []

        async def fake_llm(ctx, umo, prompt, system_prompt=None, provider_id=None):
            captured_prompts.append(prompt)
            return "正式群史正文内容"

        monkeypatch.setattr(lifetime, "call_llm", fake_llm)
        monkeypatch.setattr(
            lifetime,
            "relation_pairs",
            AsyncMock(return_value=[("1", "2", 4.5), ("1", "3", 9.0)]),
        )

        stages = []

        async def on_progress(msg: str):
            stages.append(msg)

        text, _img, skipped = await plugin_inst.history.build_report(
            U,
            7,
            True,
            "测试群",
            progress=on_progress,
        )
        assert skipped is False
        # All 5 staged progress callbacks fired in order
        assert any("[1/5]" in s for s in stages)
        assert any("[2/5]" in s for s in stages)
        assert any("[3/5]" in s for s in stages)
        assert any("[4/5]" in s for s in stages)
        assert any("[5/5]" in s for s in stages)

        # Prompt payload includes golden_quotes & culture_slang
        first_payload = json.loads(captured_prompts[0])
        assert "golden_quotes" in first_payload
        assert any("一口气上线成功" in q for q in first_payload["golden_quotes"])
        assert "culture_slang" in first_payload
        assert any(s["name"] == "蓝蘑菇" for s in first_payload["culture_slang"])

        # Relation arc appended and hidden member "3" filtered out
        assert "## 本期关系弧" in text
        assert "甲 ⇄ 乙" in text
        assert "隐藏人" not in text
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# 3. Culture WebUI quota breakdown & run_once live fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_culture_web_progress_quota_breakdown_and_run_once_live_fallback(
    culture, monkeypatch  # noqa: F811
):
    material = rows()
    db, _ = await ingest(culture, monkeypatch, material)
    web = culture.plugin.culture_web
    try:
        # Overview & progress include full quota breakdown
        ov = await web.overview_data()
        assert ov["quota"]["daily_limit"] == 20
        assert ov["quota"]["culture_used"] >= 1
        assert ov["quota"]["total_remaining"] == ov["quota"]["daily_limit"] - ov["quota"]["total_used"]
        assert ov["quota"]["culture_limit"] == 10

        prog = await web.dispatch({"op": "progress", "scope": U}, "human:admin")
        assert prog["quota"]["total_used"] == ov["quota"]["total_used"]
        assert "history_used" in prog["quota"] and "culture_remaining" in prog["quota"]

        # Add a fresh live message after backfill completed -> run_once falls back to live!
        await db.execute(
            "INSERT INTO messages VALUES(3,?,'user','新梗红蘑菇表示提前交付大家都懂','1','甲',?,'{}')",
            (U, material[0]["ts"] + 10),
        )
        await db.commit()
        monkeypatch.setattr(
            culture_service, "call_llm", AsyncMock(return_value='{"entries":[]}')
        )
        res = await web.dispatch({"op": "run_once", "scope": U, "batches": 1}, "human:admin")
        assert res["steps"][0]["advanced"] is True
        assert culture.store.job(U)["live_cursor"] == 3
        assert res["quota"]["culture_used"] > prog["quota"]["culture_used"]
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# 4. Series contracts: scope toggle (history + culture), activate_series_runtime,
#    get_culture_context(umo, query, limit), and shared provider inheritance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_series_scope_toggle_covers_history_and_culture_and_activate_runtime(
    culture, monkeypatch  # noqa: F811
):
    plugin_inst = culture.plugin
    db, _ = await ingest(culture, monkeypatch)
    try:
        st = await plugin_inst.get_series_scope_status(U)
        assert st["umo"] == U
        assert st["writable"] is True
        assert st["enabled"] is True
        assert st["mode"] == "history+culture"
        assert st["detail"] == st["summary"]

        # Toggle OFF pauses BOTH history and culture
        off = await plugin_inst.set_series_scope_enabled(
            U, False, expected_version=st["version"]
        )
        assert off["ok"] is True
        assert off["umo"] == U
        assert off["enabled"] is False
        assert plugin_inst.history.store.is_group_paused(U) is True
        assert plugin_inst.culture.store.state(U)["paused"] == 1
        # Paused history process_one returns False immediately
        assert await plugin_inst.history.process_one(U) is False

        # Optimistic lock conflict reports standard reason="version_conflict"
        conflict = await plugin_inst.set_series_scope_enabled(
            U, True, expected_version=st["version"]
        )
        assert conflict["ok"] is False
        assert conflict["conflict"] is True
        assert conflict["reason"] == "version_conflict"

        # Toggle ON resumes BOTH
        on = await plugin_inst.set_series_scope_enabled(
            U, True, expected_version=off["version"]
        )
        assert on["ok"] is True and on["enabled"] is True
        assert plugin_inst.history.store.is_group_paused(U) is False
        assert plugin_inst.culture.store.state(U)["paused"] == 0

        # activate_series_runtime enables history_enabled & culture_enabled
        plugin_inst.config["history_enabled"] = False
        plugin_inst.config["culture_enabled"] = False
        act = plugin_inst.activate_series_runtime()
        assert act["ok"] is True
        assert act["changed"] is True
        assert plugin_inst.config["history_enabled"] is True
        assert plugin_inst.config["culture_enabled"] is True
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_get_culture_context_positional_sentence_match_and_shared_provider(
    culture, monkeypatch  # noqa: F811
):
    material = rows()
    raw = proposal(material)
    # Add a pending nickname entry to ensure only approved entries are returned
    pending = proposal(
        material,
        name="待审外号",
        meaning="不应该公开",
        kind="nickname",
        subjects=["1"],
    )["entries"][0]
    material_with_nick = [
        message(1, "群里说蓝蘑菇，就是项目延期的意思，待审外号别公开。", uid="1"),
        message(2, "蓝蘑菇指延期，我也一直这样用，待审外号待审核。", uid="2"),
    ]
    raw = proposal(material_with_nick)
    pending = proposal(
        material_with_nick,
        name="待审外号",
        meaning="不应该公开",
        kind="nickname",
        subjects=["1"],
    )["entries"][0]
    raw["entries"].append(pending)

    db, _ = await ingest(culture, monkeypatch, material_with_nick, raw)
    try:
        # Positional series contract call with full user sentence query
        res = await culture.plugin.get_culture_context(U, "今天这个需求又要蓝蘑菇了吗？", 6)
        assert res["available"] is True
        assert res["scope"] == U
        assert len(res["entries"]) == 1
        assert res["entries"][0]["name"] == "蓝蘑菇"
        assert res["terms"] == [
            {
                "id": res["entries"][0]["id"],
                "name": "蓝蘑菇",
                "meaning": "项目延期",
                "usage": "讨论延期时",
                "aliases": [],
            }
        ]
        assert "「蓝蘑菇」：项目延期" in res["summary"]
        assert all(t["name"] != "待审外号" for t in res["terms"])

        # Empty query lists approved entries
        all_res = await culture.plugin.get_culture_context(U, "", 6)
        assert [t["name"] for t in all_res["terms"]] == ["蓝蘑菇"]

        # Shared config inheritance when llm_provider_id is empty
        _attach_chat_dynamics(
            culture.plugin,
            shared_cfg={
                "reply_provider": "shared-reply-model",
                "light_analysis_provider": "shared-light-model",
            },
        )
        culture.plugin.config["llm_provider_id"] = ""
        assert culture.plugin._llm_provider_id() == "shared-reply-model"
    finally:
        await db.close()
