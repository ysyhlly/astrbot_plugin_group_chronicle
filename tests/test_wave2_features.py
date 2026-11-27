"""Wave 2 feature tests for astrbot_plugin_group_chronicle.

Covers:
1. Personal/group quarterly & annual report card (/我的群年报, /群年报):
   - arg parsing (季度/年度, 个人/全群, 主题=...)
   - read-only dynamics_memory fusion via lm_bridge.query_memory_highlights
   - graceful degradation when dynamics_memory is unavailable
   - privacy protection for hidden members
   - multi-theme long-card image delivery
2. Unified Chronicle Workbench WebUI API ops:
   - batch_action (multi-select approve/reject/hide)
   - editions (list with period badges & summary + 2-edition comparison diff)
   - events_list (status filter, hidden event privacy masking, corrections, hidden_members)
   - event_action (batch approve/reject/hide, merge, restore_event, restore_member, resolve_correction)
   - members_wall (grades, points, aliases, cached biography excerpt, hidden_count)
3. Presentation consistency, timezone fix, 3 render themes & schema polish:
   - 史记查看 & 群大事 long-text forward nodes delivery
   - 人物传 card image rendering (non-None image_path)
   - /群文化 & /梗溯源 local timezone formatting (no +00:00)
   - 3 themes (classic_scroll / dark_grimoire / modern_card), no hardcoded 夏目安安
   - _conf_schema.json module groups, render_theme, enable_slang <-> culture_enabled docs
   - enhanced 史记列表 (volume badge + summary) & 影响力榜 (breakdown)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

import main as main_module
from core import annual_report, deliver, history_commands, lm_bridge
from core.annual_report import build_annual_report, parse_annual_report_args
from core.render import (
    RENDER_THEMES,
    _render_html_fallback,
    normalize_render_theme,
    render_html,
)

from .test_culture import culture, ingest, proposal, rows  # noqa: F401
from .test_history import Event, U, fact, plugin  # noqa: F401


# ---------------------------------------------------------------------------
# 1. /我的群年报 & /群年报 (Personal / Group Quarterly & Annual Report Card)
# ---------------------------------------------------------------------------


def test_parse_annual_report_args_variants():
    a = parse_annual_report_args("我的群年报")
    assert (a["period"], a["days"], a["mode"], a["theme"]) == (
        "year",
        365,
        "personal",
        None,
    )

    b = parse_annual_report_args("/群年报 季度", is_group_alias=True)
    assert (b["period"], b["days"], b["mode"], b["theme"]) == (
        "quarter",
        90,
        "group",
        None,
    )

    c = parse_annual_report_args("群年报 全群 年度 主题=现代")
    assert (c["period"], c["mode"], c["theme"]) == ("year", "group", "modern_card")

    d = parse_annual_report_args("我的群年报 季报 主题=深紫")
    assert (d["period"], d["mode"], d["theme"]) == (
        "quarter",
        "personal",
        "dark_grimoire",
    )

    e = parse_annual_report_args("我的群年报 本群 主题=古籍")
    assert (e["period"], e["mode"], e["theme"]) == (
        "year",
        "group",
        "classic_scroll",
    )


@pytest.mark.asyncio
async def test_lm_bridge_query_memory_highlights_available_and_degraded(monkeypatch):
    # Case A: dynamics_memory absent -> graceful degradation
    monkeypatch.setattr(lm_bridge, "detect_lm", lambda ctx: None)
    degraded = await lm_bridge.query_memory_highlights(NS(), U, "1")
    assert degraded["available"] is False
    assert degraded["topics"] == []
    assert degraded["co_members"] == []
    assert degraded["message_count"] == 0

    # Case B: dynamics_memory available -> fuses semantic topics + co-members + samples
    now = time.time()

    class FakeEngine:
        async def retrieve(self, **kwargs):
            return [
                {"content": "那次通宵修 Bug 笑死全群", "timestamp": now - 3600},
                {"content": "年末大家一起团建", "timestamp": now - 7200},
            ]

    class FakeConvMgr:
        async def get_recent_messages(self, umo, limit=200):
            return [
                {
                    "role": "user",
                    "sender_id": "1",
                    "sender_name": "张三",
                    "content": "哈哈今天终于上线了！",
                    "timestamp": now - 100,
                },
                {
                    "role": "user",
                    "sender_id": "2",
                    "sender_name": "李四",
                    "content": "太强了兄弟们",
                    "timestamp": now - 60,
                },
            ]

    fake_lm = object()
    monkeypatch.setattr(lm_bridge, "detect_lm", lambda ctx: fake_lm)
    monkeypatch.setattr(lm_bridge, "ensure_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(lm_bridge, "get_engine", lambda p: FakeEngine())
    monkeypatch.setattr(lm_bridge, "get_conversation_manager", lambda p: FakeConvMgr())

    res = await lm_bridge.query_memory_highlights(NS(), U, "1", since_ts=now - 86400)
    assert res["available"] is True
    assert res["message_count"] == 1
    assert res["active_days"] >= 1
    assert any("通宵修 Bug" in t for t in res["topics"])
    assert res["co_members"][0]["uid"] == "2"
    assert res["co_members"][0]["name"] == "李四"
    assert any("上线了" in q["content"] for q in res["sample_quotes"])


@pytest.mark.asyncio
async def test_annual_report_fuses_chronicle_memory_culture_and_respects_privacy(
    plugin, monkeypatch, tmp_path  # noqa: F811
):
    now = time.time()
    store = plugin.history.store
    store.observe(U, "1", "张三", now - 86400 * 40)
    store.observe(U, "2", "李四", now - 86400 * 30)
    eid = store.add_event(
        U,
        {
            **fact("年度大事件", uid="1"),
            "title": "通宵联调上线",
            "detail": "张三和李四通宵排查终于上线成功",
            "importance": 3,
            "event_at": now - 86400 * 5,
            "participants": [
                {"uid": "1", "name": "张三", "role": 3},
                {"uid": "2", "name": "李四", "role": 1},
            ],
        },
        status="approved",
    )
    assert eid > 0

    plugin.get_culture_context = AsyncMock(
        return_value={
            "available": True,
            "terms": [{"name": "上线祭天", "meaning": "每次发版前的传统仪式"}],
        }
    )

    fake_img = tmp_path / "annual_card.png"
    fake_img.write_bytes(b"png")
    captured_render: dict = {}

    async def fake_render(text, group_name, out_dir, **kwargs):
        captured_render["text"] = text
        captured_render["group_name"] = group_name
        captured_render["kwargs"] = kwargs
        return text, str(fake_img)

    monkeypatch.setattr(annual_report, "render_text_or_image", fake_render)
    monkeypatch.setattr(
        lm_bridge,
        "query_memory_highlights",
        AsyncMock(
            return_value={
                "available": True,
                "topics": ["一起庆祝新版本上线"],
                "co_members": [{"uid": "2", "name": "李四", "interactions": 9}],
                "message_count": 18,
                "active_days": 12,
                "sample_quotes": [{"content": "代码跑通啦！哈哈哈太不容易了"}],
            }
        ),
    )

    text, img = await build_annual_report(
        plugin,
        U,
        actor_id="1",
        actor_name="张三",
        group_name="测试群",
        period="quarter",
        mode="personal",
        theme="modern_card",
    )
    assert img == str(fake_img)
    assert "通宵联调上线" in text
    assert "上线祭天" in text
    assert "一起庆祝新版本上线" in text
    assert "李四" in text
    assert captured_render["kwargs"]["theme"] == "modern_card"
    assert captured_render["kwargs"]["title_suffix"] == "季度时光机"

    # Graceful degradation when dynamics_memory unavailable
    monkeypatch.setattr(
        lm_bridge,
        "query_memory_highlights",
        AsyncMock(
            return_value={
                "available": False,
                "topics": [],
                "co_members": [],
                "message_count": 0,
                "active_days": 0,
                "sample_quotes": [],
            }
        ),
    )
    group_text, group_img = await build_annual_report(
        plugin,
        U,
        actor_id="",
        actor_name="",
        group_name="测试群",
        period="year",
        mode="group",
        theme="dark_grimoire",
    )
    assert group_img == str(fake_img)
    assert "群间记忆暂未就绪" in group_text
    assert captured_render["kwargs"]["theme"] == "dark_grimoire"

    # Hidden member personal report is refused cleanly
    store.hide(U, "1")
    blocked_text, blocked_img = await build_annual_report(
        plugin,
        U,
        actor_id="1",
        actor_name="张三",
        group_name="测试群",
        period="year",
        mode="personal",
    )
    assert blocked_img is None
    assert "隐藏状态" in blocked_text


# ---------------------------------------------------------------------------
# 2. Unified Chronicle Workbench WebUI API Ops
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_culture_web_batch_action(culture, monkeypatch):  # noqa: F811
    now = time.time() - 3600
    material = [
        {"id": 1, "role": "user", "content": "大家说蓝蘑菇就是项目延期，而红蘑菇就是紧急回滚。", "uid": "1", "name": "甲", "ts": now, "metadata": {}},
        {"id": 2, "role": "user", "content": "没错，蓝蘑菇指延期，红蘑菇指回滚，听说起源于去年发版。", "uid": "2", "name": "乙", "ts": now + 60, "metadata": {}},
    ]
    raw = proposal(material, name="蓝蘑菇", meaning="项目延期")
    second = proposal(material, name="红蘑菇", meaning="紧急回滚")["entries"][0]
    raw["entries"].append(second)
    for item in raw["entries"]:
        item["proposed_origin"] = {
            "text": "起源于去年发版",
            "source_ids": [2],
        }
    db, _ = await ingest(culture, monkeypatch, material, raw)
    try:
        entries, _ = culture.store.entries(U)
        ids = [e["id"] for e in entries]
        assert len(ids) == 2

        res = await culture.plugin.culture_web.dispatch(
            {
                "op": "batch_action",
                "scope": U,
                "action": "approve",
                "ids": ids + [999999],
                "note": "证据充分，批量通过",
            },
            "human:admin",
        )
        assert res["ok"] is True
        assert len(res["succeeded"]) == 2
        assert len(res["failed"]) == 1 and res["failed"][0]["id"] == 999999
        for eid in ids:
            assert culture.store.entry(U, eid)["status"] == "approved"

        # Batch hide
        hide_res = await culture.plugin.culture_web.dispatch(
            {
                "op": "batch_action",
                "scope": U,
                "action": "hide",
                "ids": [ids[0]],
                "note": "涉及隐私批量隐藏",
            },
            "human:admin",
        )
        assert len(hide_res["succeeded"]) == 1
        assert culture.store.entry(U, ids[0])["status"] == "hidden"

        # Invalid action rejected
        with pytest.raises(ValueError):
            await culture.plugin.culture_web.dispatch(
                {"op": "batch_action", "scope": U, "action": "delete", "ids": ids, "note": "x"},
                "human:admin",
            )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_culture_web_editions_events_and_members_wall(culture):  # noqa: F811
    store = culture.history.store
    now = time.time()
    store.observe(U, "1", "张三", now - 86400 * 60)
    store.observe(U, "1", "三哥", now - 86400 * 10)
    store.observe(U, "2", "李四", now - 86400 * 50)

    e1 = store.add_event(
        U,
        {
            **fact("e1", uid="1"),
            "title": "群主立下军令状",
            "detail": "张三宣布本月完成重制",
            "importance": 4,
        },
        status="pending",
    )
    e2 = store.add_event(
        U,
        {
            **fact("e2", uid="2"),
            "title": "深夜排障奇迹",
            "detail": "李四一小时修复所有线上报错",
            "importance": 3,
        },
        status="pending",
    )
    e3 = store.add_event(
        U,
        {
            **fact("e3", uid="1"),
            "title": "重复记录的军令状",
            "detail": "另一条重复记载",
            "importance": 2,
        },
        status="approved",
    )
    cid = store.correction(U, e1, "2", "时间应为周三晚")

    # Save two editions + a cached biography
    r1 = store.save_report(
        U,
        "2026-W10:days=7",
        {
            "iso_year": 2026,
            "iso_week": 10,
            "group_name": "测试群",
            "chronicle_text": "# 第十周卷\n本周完成了第一轮联调。",
            "event_ids": [e1],
            "event_count": 1,
            "msg_count": 120,
            "period_days": 7,
            "created_at": now - 86400 * 7,
        },
    )
    r2 = store.save_report(
        U,
        "2026-W11:days=30",
        {
            "iso_year": 2026,
            "iso_week": 11,
            "group_name": "测试群",
            "chronicle_text": "# 第十一周卷\n本周完成了月度冲刺并迎来新成员。",
            "event_ids": [e1, e2],
            "event_count": 2,
            "msg_count": 260,
            "period_days": 30,
            "created_at": now,
        },
    )
    store.save_report(
        U,
        "bio:1",
        {
            "iso_year": 2026,
            "iso_week": 11,
            "group_name": "测试群",
            "chronicle_text": "张三，群中老将，素以排障著称。",
            "report_kind": "biography",
            "member_id": "1",
            "created_at": now,
        },
    )

    web = culture.plugin.culture_web

    # 1. editions + comparison diff
    ed_res = await web.dispatch(
        {"op": "editions", "scope": U, "report_id": r2, "compare_id": r1},
        "human:admin",
    )
    assert ed_res["total"] == 2
    labels = {item["report_id"]: item["period_label"] for item in ed_res["editions"]}
    assert labels[r1] == "周卷"
    assert labels[r2] == "月卷"
    cmp_data = ed_res["comparison"]
    assert cmp_data["event_diff"] == 1
    assert cmp_data["msg_diff"] == 140
    assert cmp_data["added_event_ids"] == [e2]
    assert cmp_data["removed_event_ids"] == []

    # 2. events_list + batch approve + merge + hide + restore + resolve_correction
    ev_pending = await web.dispatch(
        {"op": "events_list", "scope": U, "status": "pending"},
        "human:admin",
    )
    assert ev_pending["counts"]["pending"] == 2
    assert ev_pending["counts"]["corrections"] == 1

    batch_ok = await web.dispatch(
        {
            "op": "event_action",
            "scope": U,
            "action": "approve",
            "ids": [e1, e2],
            "note": "事实清晰，批量通过",
        },
        "human:admin",
    )
    assert set(batch_ok["succeeded"]) == {e1, e2}

    merge_ok = await web.dispatch(
        {
            "op": "event_action",
            "scope": U,
            "action": "merge",
            "event_id": e3,
            "target_event_id": e1,
            "note": "重复事件合并",
        },
        "human:admin",
    )
    assert merge_ok["ok"] is True

    # Hide e2 -> events_list must mask its title/detail while setting can_restore=True
    await web.dispatch(
        {"op": "event_action", "scope": U, "action": "hide", "event_id": e2, "note": "隐私隐藏"},
        "human:admin",
    )
    ev_all = await web.dispatch({"op": "events_list", "scope": U, "status": None}, "human:admin")
    e2_row = next(row for row in ev_all["events"] if row["id"] == e2)
    assert e2_row["hidden"] is True
    assert e2_row["can_restore"] is True
    assert "深夜排障奇迹" not in e2_row["title"]
    assert e2 in ev_all["hidden_event_ids"]

    # Restore hidden event
    await web.dispatch(
        {"op": "event_action", "scope": U, "action": "restore_event", "event_id": e2},
        "human:admin",
    )
    assert e2 in [e["id"] for e in store.events(U)]

    # Resolve correction
    await web.dispatch(
        {"op": "event_action", "scope": U, "action": "resolve_correction", "correction_id": cid},
        "human:admin",
    )
    assert store.corrections(U) == []

    # Save fresh cached biography and verify members_wall shows excerpt before member hide
    store.save_report(
        U,
        "bio:1:fresh",
        {
            "iso_year": 2026,
            "iso_week": 11,
            "group_name": "测试群",
            "chronicle_text": "张三，群中老将，素以排障著称。",
            "report_kind": "biography",
            "member_id": "1",
            "created_at": now + 10,
        },
    )
    wall0 = await web.dispatch({"op": "members_wall", "scope": U}, "human:admin")
    assert wall0["hidden_count"] == 0
    m1 = next(m for m in wall0["members"] if m["uid"] == "1")
    assert "三哥" in m1["aliases"]
    assert "素以排障著称" in m1["biography_excerpt"]

    # Hide member 2 -> appears in hidden_members & excluded from members_wall, then restore
    store.hide(U, "2")
    wall = await web.dispatch({"op": "members_wall", "scope": U}, "human:admin")
    assert wall["hidden_count"] == 1
    assert [m["uid"] for m in wall["members"]] == ["1"]

    await web.dispatch(
        {"op": "event_action", "scope": U, "action": "restore_member", "uid": "2"},
        "human:admin",
    )
    wall2 = await web.dispatch({"op": "members_wall", "scope": U}, "human:admin")
    assert wall2["hidden_count"] == 0
    assert {m["uid"] for m in wall2["members"]} == {"1", "2"}


# ---------------------------------------------------------------------------
# 3. Presentation Consistency, Timezone Fix, 3 Themes & Schema Polish
# ---------------------------------------------------------------------------


def test_three_render_themes_and_no_hardcoded_character():
    assert set(RENDER_THEMES) == {"classic_scroll", "dark_grimoire", "modern_card"}
    assert normalize_render_theme("古籍") == "classic_scroll"
    assert normalize_render_theme("深紫") == "dark_grimoire"
    assert normalize_render_theme("现代") == "modern_card"
    assert normalize_render_theme("unknown") == "classic_scroll"

    sample_md = "## 群史纪要\n本周群内达成重要里程碑。"
    for theme_id in ("classic_scroll", "dark_grimoire", "modern_card"):
        html_out = render_html(
            sample_md,
            "测试群",
            theme=theme_id,
            title_suffix="年度时光机",
            epigraph="记录群聊高光",
            footer_title="测试群 · 群史馆",
            seal_char="年",
        )
        assert f'data-theme="{theme_id}"' in html_out
        assert "夏目安安" not in html_out
        assert "年度时光机" in html_out
        assert "{{ " not in html_out

    # Fallback renderer also leaves zero template placeholders and supports themes
    fallback_html = _render_html_fallback(
        sample_md,
        "测试群",
        "2026-11-27 12:00",
        theme="modern_card",
        title_suffix="人物列传",
        seal_char="传",
    )
    assert 'data-theme="modern_card"' in fallback_html
    assert "人物列传" in fallback_html
    assert "夏目安安" not in fallback_html
    assert "{{ " not in fallback_html


@pytest.mark.asyncio
async def test_culture_command_uses_local_timezone_not_utc(culture, monkeypatch):  # noqa: F811
    now = time.time() - 3600
    material = [
        {"id": 1, "role": "user", "content": "群里都管熬夜不睡的叫夜猫子。", "uid": "1", "name": "甲", "ts": now, "metadata": {}},
        {"id": 2, "role": "user", "content": "对，夜猫子就是凌晨还在水群的人。", "uid": "2", "name": "乙", "ts": now + 60, "metadata": {}},
    ]
    raw = proposal(material, name="夜猫子", meaning="凌晨还在水群的人")
    db, _ = await ingest(culture, monkeypatch, material, raw)
    try:
        culture.plugin.config["timezone"] = "Asia/Shanghai"
        out = await culture.command(Event(uid="1"), "群文化", "夜猫子")
        assert "夜猫子" in out
        assert "+00:00" not in out
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_long_events_and_chronicle_view_use_forward_nodes(plugin, monkeypatch):  # noqa: F811
    # 1. 群大事 long output routes through deliver_text(..., use_forward_nodes=True)
    for idx in range(15):
        plugin.history.store.add_event(
            U,
            {
                **fact(f"long-ev-{idx}", uid="1"),
                "title": f"重大群聊事件编号{idx}",
                "detail": "这是一段非常详细的事件经过描述" * 5,
                "importance": 3,
            },
            status="approved",
        )

    deliver_calls = []

    async def fake_deliver_text(context, event, umo, text, image_path=None, **kwargs):
        deliver_calls.append({"text": text, "image_path": image_path, "kwargs": kwargs})
        return True

    plugin.config["max_reply_chars"] = 100
    plugin.config["use_forward_nodes"] = True
    monkeypatch.setattr(deliver, "deliver_text", fake_deliver_text)
    monkeypatch.setattr(deliver, "_build_forward_nodes", lambda *a, **kw: object())
    monkeypatch.setattr(history_commands, "deliver_text", fake_deliver_text)
    monkeypatch.setattr(main_module, "deliver_text", fake_deliver_text)

    ev = Event("群大事", uid="1")
    yielded = []
    async for msg in history_commands.command_reply(plugin, ev):
        yielded.append(msg)
    assert yielded == []
    assert len(deliver_calls) == 1
    assert deliver_calls[0]["kwargs"].get("use_forward_nodes") is True
    assert "重大群聊事件编号" in deliver_calls[0]["text"]

    # 2. 史记查看 long output routes through deliver_text(..., use_forward_nodes=True)
    deliver_calls.clear()
    long_text = "史记长文正文" * 200
    plugin.store.get_by_id = lambda umo, idx=None: {
        "iso_year": 2026,
        "iso_week": 10,
        "chronicle_text": long_text,
        "created_at": time.time(),
    }
    view_ev = Event("史记查看 1", uid="1")
    view_ev.plain_result = lambda text: text
    view_yielded = []
    async for msg in plugin.chronicle_view_command(view_ev):
        view_yielded.append(msg)
    assert view_yielded == []
    assert len(deliver_calls) == 1
    assert deliver_calls[0]["kwargs"].get("use_forward_nodes") is True
    assert deliver_calls[0]["text"] == long_text


@pytest.mark.asyncio
async def test_biography_renders_card_image(plugin, monkeypatch, tmp_path):  # noqa: F811
    fake_img = tmp_path / "bio_card.png"
    fake_img.write_bytes(b"png")
    delivered = []

    async def fake_render(text, group_name, out_dir, **kwargs):
        return text, str(fake_img)

    async def fake_deliver(context, event, umo, text, image_path=None, **kwargs):
        delivered.append({"text": text, "image_path": image_path})
        return True

    plugin.config["render_mode"] = "t2i"
    monkeypatch.setattr(main_module, "render_text_or_image", fake_render)
    monkeypatch.setattr(main_module, "deliver_text", fake_deliver)
    monkeypatch.setattr(main_module, "detect_lm", lambda ctx: None)
    plugin.history.store.observe(U, "1", "张三", time.time() - 86400 * 30)
    plugin.history.store.add_event(
        U,
        {**fact("bio-ev", uid="1"), "title": "群内传奇时刻", "importance": 3},
        status="approved",
    )
    plugin.history.biography = AsyncMock(return_value="## 人物传记：张三\n群内传奇人物。")

    ev = Event("人物传", uid="1")
    await plugin._bio_analyze(ev, U, "1")
    assert len(delivered) == 1
    assert delivered[0]["image_path"] == str(fake_img)


def test_conf_schema_groups_render_theme_and_slang_cross_reference():
    schema_path = Path(__file__).resolve().parent.parent / "_conf_schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    # Every key has a module group
    missing_group = [k for k, v in schema.items() if not v.get("group")]
    assert missing_group == []

    # render_theme has all 3 options
    rt = schema["render_theme"]
    assert rt["default"] == "classic_scroll"
    assert set(rt["options"]) == {"classic_scroll", "dark_grimoire", "modern_card"}

    # enable_slang <-> culture_enabled cross-reference
    assert "culture_enabled" in schema["enable_slang"]["hint"]
    assert "enable_slang" in schema["culture_enabled"]["hint"]


@pytest.mark.asyncio
async def test_chronicle_list_badges_and_influence_breakdown(plugin, monkeypatch):  # noqa: F811
    # Enhanced 史记列表
    plugin.store.list = lambda umo, limit=20: [
        {
            "iso_year": 2026,
            "iso_week": 12,
            "period_days": 30,
            "event_count": 6,
            "chronicle_text": "# 标题\n本月大家通力合作完成了全新面板升级。",
            "created_at": time.time(),
        }
    ]
    ev = Event("史记列表", uid="1")
    ev.plain_result = lambda text: text
    out = [x async for x in plugin.chronicle_list_command(ev)]
    assert "[月卷]" in out[0]
    assert "本月大家通力合作" in out[0]

    # Enhanced 影响力榜 with breakdown
    monkeypatch.setattr(main_module, "detect_lm", lambda ctx: object())
    monkeypatch.setattr(main_module, "ensure_ready", AsyncMock(return_value=True))
    monkeypatch.setattr(main_module, "get_engine", lambda lm: object())
    monkeypatch.setattr(main_module, "get_conversation_manager", lambda lm: object())
    monkeypatch.setattr(
        main_module,
        "gather_evidence",
        AsyncMock(
            return_value=NS(
                chronicle_msgs=[
                    NS(sender_id="1", sender_name="张三", timestamp=time.time())
                ],
                msg_count=1,
                active_members=1,
                memories=[],
                group_name="测试群",
                has_activity=True,
                biography_msgs=[],
                newest_ts=time.time(),
            )
        ),
    )
    monkeypatch.setattr(
        main_module,
        "rank_influential",
        AsyncMock(
            return_value=[
                {
                    "sender_id": "1",
                    "sender_name": "张三",
                    "score": 0.92,
                    "breakdown": {"activity": 0.85, "social": 0.78, "relation": 0.66},
                }
            ]
        ),
    )
    inf_ev = Event("影响力榜", uid="1")
    inf_ev.plain_result = lambda text: text
    inf_out = [x async for x in plugin.influence_command(inf_ev)]
    assert "1. 张三 (0.92)" in inf_out[0]
    assert "活跃 0.85" in inf_out[0]
    assert "社交 0.78" in inf_out[0]
    assert "关系 0.66" in inf_out[0]
