"""Batch D 命令接线测试 —— 投票选题 / 史记网页 / 宇宙史记 / 覆盖提示 /
图鉴接线 / 传记并发 / 定时热加载。

无 astrbot, 依赖 conftest 桩件; 复用 test_commands.py 的假事件与构造器模式。
运行: python -m pytest tests/test_batch_d.py -v
"""

import asyncio
import os
import time
from datetime import datetime
from types import SimpleNamespace

import main as main_module
from core.evidence import NoActivityError
from core.llm_call import LLMUnavailableError
from core.store import ChronicleStore
from main import GroupChroniclePlugin


class FakeEvent:
    """最小事件替身: 记录 event.send 发送的消息链 + 发送者 ID。"""

    def __init__(
        self,
        umo="default:GroupMessage:10001",
        message_str="",
        is_admin=True,
        sender_id="u1",
    ):
        self.unified_msg_origin = umo
        self.message_str = message_str
        self._is_admin = is_admin
        self.sender_id = sender_id
        self.sent_chains = []

    async def send(self, chain):
        self.sent_chains.append(chain)

    def get_messages(self):
        return []

    def get_group_name(self):
        return "测试群"

    def is_admin(self):
        return self._is_admin

    def plain_result(self, text):
        return text

    def sent_texts(self):
        texts = []
        for chain in self.sent_chains:
            for seg in chain:
                if isinstance(seg, tuple) and seg[0] == "plain":
                    texts.append(str(seg[1]))
        return texts


def make_plugin(config=None, context=None):
    if context is None:
        sent = []

        async def send_message(umo, chain):
            sent.append((umo, chain))

        context = SimpleNamespace(
            send_message=send_message, cron_manager=None, _sent=sent
        )
    return GroupChroniclePlugin(context, config or {})


def ctx_sent_texts(plugin):
    sent = getattr(plugin.context, "_sent", None) or []
    texts = []
    for _umo, chain in sent:
        for seg in chain:
            if isinstance(seg, tuple) and seg[0] == "plain":
                texts.append(str(seg[1]))
    return texts


def all_sent_texts(plugin, event):
    return list(event.sent_texts()) + ctx_sent_texts(plugin)


def amock(result):
    async def _f(*args, **kwargs):
        return result

    return _f


def araise(exc):
    async def _f(*args, **kwargs):
        raise exc

    return _f


def run(coro):
    return asyncio.run(coro)


async def collect(agen):
    out = []
    async for item in agen:
        out.append(item)
    return out


def make_evidence(newest_ts=1000.0, msg_count=5):
    return SimpleNamespace(
        chronicle_msgs=[],
        biography_msgs=[],
        memories=[],
        graph_snapshot=None,
        stats={},
        newest_ts=newest_ts,
        coverage_start_ts=1700000000.0,
        coverage_end_ts=1700003600.0,
        msg_count=msg_count,
    )


def patch_lm(monkeypatch, ready=True):
    monkeypatch.setattr(main_module, "detect_lm", lambda ctx: SimpleNamespace())
    monkeypatch.setattr(main_module, "ensure_ready", amock(ready))
    monkeypatch.setattr(main_module, "get_engine", lambda plugin: SimpleNamespace())
    monkeypatch.setattr(
        main_module, "get_conversation_manager", lambda plugin: SimpleNamespace()
    )


async def _identity_render(text, group_name, out_dir, **kwargs):
    return text, None


def patch_full_pipeline(monkeypatch, chronicle_text="新史记", evidence=None):
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(evidence or make_evidence())
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock(chronicle_text))
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)


# ---------------------------------------------------------------------------
# 投票选题: 史记选题 → 史记选票 → 史记选题公布
# ---------------------------------------------------------------------------


def test_vote_propose_stores_state_and_replies(monkeypatch):
    """史记选题: 生成 3 个候选存入 _vote_state, 回复投票提示。"""
    plugin = make_plugin({"enable_vote": True})
    patch_lm(monkeypatch)
    monkeypatch.setattr(main_module, "gather_evidence", amock(make_evidence()))
    monkeypatch.setattr(
        main_module,
        "generate_topic_candidates",
        amock(["年度大会", "烧烤聚会", "摸鱼大赛"]),
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._vote_propose_run(event, "default:GroupMessage:g1"))
    texts = all_sent_texts(plugin, event)
    assert any("📮 本周选题候选" in t for t in texts)
    assert any("1. 年度大会" in t for t in texts)
    assert any("史记选票" in t for t in texts)
    state = plugin._vote_state["default:GroupMessage:g1"]
    assert state["topics"] == ["年度大会", "烧烤聚会", "摸鱼大赛"]
    assert state["voters"] == set()
    assert state["expires_at"] > time.time()  # 10 分钟有效期


def test_vote_topic_command_dispatches(monkeypatch):
    """史记选题 命令 → 后台启动 _vote_propose_run; enable_vote=false 时拒绝。"""
    plugin = make_plugin({"enable_vote": True})
    spawned = []

    def fake_spawn(coro, *, label):
        spawned.append(label)
        coro.close()

    monkeypatch.setattr(plugin, "_spawn_bg", fake_spawn)
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.vote_topic_command(event)))
    assert any("正在生成选题候选" in r for r in results)
    assert spawned == ["vote_propose:default:GroupMessage:g1"]

    plugin2 = make_plugin({"enable_vote": False})
    results2 = run(collect(plugin2.vote_topic_command(event)))
    assert results2 == ["投票选题功能未启用（enable_vote=false）"]


def test_vote_cast_records_voter(monkeypatch, tmp_path):
    """史记选票 N: 记录 (sender, choice), 回复已投票。"""
    plugin = make_plugin({"enable_vote": True})
    plugin._vote_state["default:GroupMessage:g1"] = {
        "topics": ["年度大会", "烧烤聚会", "摸鱼大赛"],
        "expires_at": time.time() + 600,
        "voters": set(),
    }
    event = FakeEvent(umo="default:GroupMessage:g1", sender_id="u1")
    results = run(collect(plugin.vote_cast_command(event, "2")))
    assert results == ["已投票：烧烤聚会"]
    assert plugin._vote_state["default:GroupMessage:g1"]["voters"] == {"u1:2"}


def test_vote_cast_dedupe_by_sender(monkeypatch):
    """同一发送者重复投票 → 拒绝。"""
    plugin = make_plugin({"enable_vote": True})
    plugin._vote_state["default:GroupMessage:g1"] = {
        "topics": ["年度大会", "烧烤聚会", "摸鱼大赛"],
        "expires_at": time.time() + 600,
        "voters": {"u1:2"},
    }
    event = FakeEvent(umo="default:GroupMessage:g1", sender_id="u1")
    results = run(collect(plugin.vote_cast_command(event, "1")))
    assert results == ["你已经投过票了"]
    assert plugin._vote_state["default:GroupMessage:g1"]["voters"] == {"u1:2"}  # 未重复记录


def test_vote_cast_invalid_n(monkeypatch):
    """史记选票 非法 N → 中文提示。"""
    plugin = make_plugin({"enable_vote": True})
    event = FakeEvent(umo="default:GroupMessage:g1")
    assert run(collect(plugin.vote_cast_command(event, "abc"))) == [
        "请回复「史记选票 N」，N 为 1-3"
    ]
    assert run(collect(plugin.vote_cast_command(event, "0"))) == ["N 必须为 1-3"]
    assert run(collect(plugin.vote_cast_command(event, "9"))) == ["N 必须为 1-3"]


def test_vote_cast_no_active_state(monkeypatch):
    """无进行中的投票 → 提示先发起。"""
    plugin = make_plugin({"enable_vote": True})
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.vote_cast_command(event, "1")))
    assert results == ["当前没有进行中的选题投票（可用 史记选题 发起）"]


def test_vote_expired_guarded(monkeypatch):
    """投票过期 → 选票/公布均提示过期, 状态被清理。"""
    plugin = make_plugin({"enable_vote": True})
    plugin._vote_state["default:GroupMessage:g1"] = {
        "topics": ["A", "B", "C"],
        "expires_at": time.time() - 10,
        "voters": {"u1:1"},
    }
    event = FakeEvent(umo="default:GroupMessage:g1", sender_id="u2")
    results = run(collect(plugin.vote_cast_command(event, "2")))
    assert results == ["选题投票已过期，请重新发起 史记选题"]
    assert "default:GroupMessage:g1" not in plugin._vote_state


def test_vote_announce_winner_and_passes_topic(monkeypatch):
    """史记选题公布: 计票 → 公布获胜选题 → 以 topic 参数触发生成。"""
    plugin = make_plugin({"enable_vote": True})
    plugin._vote_state["default:GroupMessage:g1"] = {
        "topics": ["年度大会", "烧烤聚会", "摸鱼大赛"],
        "expires_at": time.time() + 600,
        "voters": {"u1:3", "u2:3", "u3:1"},
    }
    captured = {}

    async def fake_run(event, days=None, include_bios=True, **kwargs):
        captured["topic"] = kwargs.get("topic")
        captured["umo"] = kwargs.get("umo")

    monkeypatch.setattr(plugin, "_run_group_analysis", fake_run)
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=True)
    results = run(collect(plugin.vote_announce_command(event)))
    joined = "\n".join(results)
    assert "🎉 本期选题公布" in joined
    assert "摸鱼大赛（2 票）" in joined
    assert "🏆 获胜选题：摸鱼大赛" in joined
    assert captured["topic"] == "摸鱼大赛"
    assert "default:GroupMessage:g1" not in plugin._vote_state  # 投票结束已清理


def test_vote_announce_admin_only(monkeypatch):
    """史记选题公布 非管理员 → 无权限。"""
    plugin = make_plugin({"enable_vote": True})
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=False)
    results = run(collect(plugin.vote_announce_command(event)))
    assert results == ["无权限"]


def test_topic_flows_into_generate_chronicle(monkeypatch, tmp_path):
    """topic 参数贯通 _build_entry → generate_chronicle。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=5))
    )
    captured = {}

    async def fake_generate(context, umo, evidence, sp, days, **kwargs):
        captured["topic"] = kwargs.get("topic")
        return "正文"

    monkeypatch.setattr(main_module, "generate_chronicle", fake_generate)
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True, topic="年度大会"))
    assert captured["topic"] == "年度大会"


# ---------------------------------------------------------------------------
# 史记网页
# ---------------------------------------------------------------------------


def test_webpage_command_writes_html_file(monkeypatch, tmp_path):
    """史记网页: 写出自包含 HTML 文件; 平台不支持文件时回退路径文本。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    plugin.store.save(
        {
            "umo": "default:GroupMessage:g1",
            "group_name": "测试群",
            "iso_year": datetime.now().isocalendar()[0],
            "iso_week": 1,
            "created_at": time.time(),
            "newest_msg_ts": 100.0,
            "chronicle_text": "# 标题\n\n- 条目\n\n<script>alert(1)</script>",
            "biographies": [],
            "image_path": None,
            "version": 1,
        }
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.chronicle_webpage_command(event, "1")))
    # 桩 MessageChain 无 .file() → 回退路径文本
    assert len(results) == 1
    assert results[0] == "已生成史记网页文件"
    exports_dir = os.path.join(plugin.data_dir, "exports")
    html_files = [f for f in os.listdir(exports_dir) if f.endswith(".html")]
    assert len(html_files) == 1
    path = os.path.join(exports_dir, html_files[0])
    assert path == event.sent_chains[0]._chain[0].file
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "<h1>测试群·群史记</h1>" in content
    assert "<li>条目</li>" in content  # markdown → HTML
    assert "<script>alert" not in content  # HTML 被转义
    assert "&lt;script&gt;" in content


def test_webpage_command_not_found(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.chronicle_webpage_command(event, "1")))
    assert results == ["未找到该条史记"]


# ---------------------------------------------------------------------------
# 宇宙史记
# ---------------------------------------------------------------------------


def test_universe_command_empty_config(monkeypatch):
    """universe_groups 为空 → 提示未配置。"""
    plugin = make_plugin()
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=True)
    results = run(collect(plugin.universe_command(event)))
    assert results == ["未配置宇宙史记群列表（universe_groups）"]


def test_universe_command_admin_only(monkeypatch):
    """宇宙史记 非管理员 → 无权限。"""
    plugin = make_plugin({"universe_groups": ["default:GroupMessage:gA", "default:GroupMessage:gB"]})
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=False)
    results = run(collect(plugin.universe_command(event)))
    assert results == ["无权限"]


def test_universe_run_combines_groups(monkeypatch):
    """宇宙史记主体: 逐群采集 (单群失败跳过), 一次 LLM 调用, 发送结果。"""
    plugin = make_plugin({"universe_groups": ["default:GroupMessage:gA", "default:GroupMessage:gB", "default:GroupMessage:gBad"]})
    gathered = {}

    async def fake_gather(umo, days=None, *, progress=None):
        if umo == "default:GroupMessage:gBad":
            raise NoActivityError("无活动")
        gathered[umo] = True
        return make_evidence(msg_count=3), 7

    monkeypatch.setattr(plugin, "_lm_and_gather", fake_gather)
    captured = {}

    async def fake_universe(context, umo, group_evidences, sp, **kwargs):
        captured["groups"] = [label for label, _ in group_evidences]
        return "宇宙史记正文"

    monkeypatch.setattr(main_module, "generate_universe_chronicle", fake_universe)
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=True)
    run(plugin._universe_run(event, "default:GroupMessage:g1", ["default:GroupMessage:gA", "default:GroupMessage:gB", "default:GroupMessage:gBad"]))
    assert captured["groups"] == ["default:GroupMessage:gA", "default:GroupMessage:gB"]  # gBad 失败被跳过
    assert any("宇宙史记正文" in t for t in all_sent_texts(plugin, event))


def test_universe_run_all_groups_fail(monkeypatch):
    """所有群采集失败 → 提示无法生成, 不调 LLM。"""
    plugin = make_plugin({"universe_groups": ["default:GroupMessage:gBad"]})
    calls = []

    async def fake_gather(umo, days=None, *, progress=None):
        raise NoActivityError("无活动")

    monkeypatch.setattr(plugin, "_lm_and_gather", fake_gather)

    async def fake_universe(*args, **kwargs):
        calls.append(1)
        return "x"

    monkeypatch.setattr(main_module, "generate_universe_chronicle", fake_universe)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._universe_run(event, "default:GroupMessage:g1", ["default:GroupMessage:gBad"]))
    assert calls == []
    assert any("所有群均无可用数据" in t for t in all_sent_texts(plugin, event))


def test_universe_run_llm_unavailable(monkeypatch):
    """宇宙史记 LLM 不可用 → 明确提示。"""
    plugin = make_plugin({"universe_groups": ["default:GroupMessage:gA"]})
    monkeypatch.setattr(
        plugin, "_lm_and_gather", amock((make_evidence(msg_count=3), 7))
    )
    monkeypatch.setattr(
        main_module,
        "generate_universe_chronicle",
        araise(LLMUnavailableError("未配置可用的 LLM")),
    )
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._universe_run(event, "default:GroupMessage:g1", ["default:GroupMessage:gA"]))
    assert any(
        "未配置可用的 LLM，无法生成宇宙史记" in t for t in all_sent_texts(plugin, event)
    )


# ---------------------------------------------------------------------------
# 覆盖范围提示 (max_msgs 截断)
# ---------------------------------------------------------------------------


def test_coverage_note_appears_when_near_cap(monkeypatch, tmp_path):
    """msg_count >= 0.9 * max_msgs → 头部追加 LivingMemory 保留上限提示。"""
    plugin = make_plugin({"max_msgs": 10, "chronicle_days": 7})
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=9))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = all_sent_texts(plugin, event)
    assert any("仅保留最近 10 条消息" in t for t in texts)
    assert any("实际覆盖可能不足 7 天" in t for t in texts)


def test_coverage_note_absent_when_far_below_cap(monkeypatch, tmp_path):
    """msg_count 远低于 max_msgs → 无截断提示。"""
    plugin = make_plugin({"max_msgs": 10})
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=5))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert not any("仅保留最近" in t for t in all_sent_texts(plugin, event))


# ---------------------------------------------------------------------------
# 群友图鉴接线
# ---------------------------------------------------------------------------


def test_gallery_wired_after_bios(monkeypatch, tmp_path):
    """enable_gallery=true 且有 Top-N 成员 → 正文追加「## 群友图鉴」。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=30))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    monkeypatch.setattr(
        main_module,
        "rank_influential",
        amock(
            [{"sender_id": "1", "sender_name": "张三", "score": 0.9, "breakdown": {}}]
        ),
    )
    monkeypatch.setattr(main_module, "generate_biography", amock("张三的传记"))
    monkeypatch.setattr(
        main_module,
        "build_gallery",
        amock("## 群友图鉴\n- 张三｜夜猫子之王｜凌晨发美食"),
    )
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = all_sent_texts(plugin, event)
    assert any("## 群友图鉴" in t for t in texts)
    assert any("夜猫子之王" in t for t in texts)


def test_gallery_skipped_when_disabled(monkeypatch, tmp_path):
    """enable_gallery=false → 不调用 build_gallery。"""
    plugin = make_plugin({"enable_gallery": False})
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=30))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    monkeypatch.setattr(
        main_module,
        "rank_influential",
        amock(
            [{"sender_id": "1", "sender_name": "张三", "score": 0.9, "breakdown": {}}]
        ),
    )
    calls = []

    async def fake_gallery(*args, **kwargs):
        calls.append(1)
        return "## 群友图鉴"

    monkeypatch.setattr(main_module, "build_gallery", fake_gallery)
    monkeypatch.setattr(main_module, "generate_biography", amock("传记"))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert calls == []
    assert not any("群友图鉴" in t for t in all_sent_texts(plugin, event))


def test_gallery_failure_does_not_break_entry(monkeypatch, tmp_path):
    """build_gallery 抛异常 → 静默跳过, 史记仍正常生成。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=30))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    monkeypatch.setattr(
        main_module,
        "rank_influential",
        amock(
            [{"sender_id": "1", "sender_name": "张三", "score": 0.9, "breakdown": {}}]
        ),
    )
    monkeypatch.setattr(main_module, "generate_biography", amock("传记"))
    monkeypatch.setattr(
        main_module, "build_gallery", araise(RuntimeError("gallery boom"))
    )
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert any("## 正文" in t for t in all_sent_texts(plugin, event))


# ---------------------------------------------------------------------------
# 传记并发: gather + Semaphore(2), 顺序保持
# ---------------------------------------------------------------------------


def test_bios_gather_order_preserved(monkeypatch, tmp_path):
    """Top-N 传记并发生成, 结果按 top 顺序追加 (慢成员不改变顺序)。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=30))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    top = [
        {"sender_id": "1", "sender_name": "张三", "score": 0.9, "breakdown": {}},
        {"sender_id": "2", "sender_name": "李四", "score": 0.5, "breakdown": {}},
        {"sender_id": "3", "sender_name": "王五", "score": 0.1, "breakdown": {}},
    ]
    monkeypatch.setattr(main_module, "rank_influential", amock(top))

    started = []

    async def slow_bio(context, umo, evidence, member, sp, days, **kwargs):
        name = member["sender_name"]
        started.append(name)
        # 王五先完成, 张三最慢 → 若顺序不保持会错乱
        delays = {"王五": 0.0, "李四": 0.05, "张三": 0.1}
        await asyncio.sleep(delays.get(name, 0.0))
        return f"{name}的传记"

    monkeypatch.setattr(main_module, "generate_biography", slow_bio)
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = all_sent_texts(plugin, event)
    joined = "\n".join(texts)
    # 顺序: 张三 → 李四 → 王五 (与 top 一致)
    assert joined.index("## 人物传记：张三") < joined.index("## 人物传记：李四")
    assert joined.index("## 人物传记：李四") < joined.index("## 人物传记：王五")
    assert set(started) == {"张三", "李四", "王五"}  # 全部并发执行


def test_bios_gather_failure_handled(monkeypatch, tmp_path):
    """单个传记失败 → 该位标记失败, 其余正常, 不崩溃。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(msg_count=30))
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 正文"))
    top = [
        {"sender_id": "1", "sender_name": "张三", "score": 0.9, "breakdown": {}},
        {"sender_id": "2", "sender_name": "李四", "score": 0.5, "breakdown": {}},
    ]
    monkeypatch.setattr(main_module, "rank_influential", amock(top))

    async def mixed_bio(context, umo, evidence, member, sp, days, **kwargs):
        if member["sender_name"] == "张三":
            raise RuntimeError("bio boom")
        return "李四的传记"

    monkeypatch.setattr(main_module, "generate_biography", mixed_bio)
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = all_sent_texts(plugin, event)
    joined = "\n".join(texts)
    assert "（传记生成失败）" in joined
    assert "李四的传记" in joined


# ---------------------------------------------------------------------------
# 定时热加载 (cron signature)
# ---------------------------------------------------------------------------


class FakeScheduler:
    def __init__(self):
        self.added = []
        self.removed = []

    def add_job(self, func, trigger=None, id=None, **kwargs):
        if id == "group_chronicle_history_hourly":
            return  # Lifetime scheduling is covered by the v0.2.0 suite.
        self.added.append(
            {"func": func, "trigger": trigger, "id": id, "kwargs": kwargs}
        )

    def remove_job(self, job_id):
        if job_id == "group_chronicle_history_hourly":
            return
        self.removed.append(job_id)


def make_scheduler_context(config):
    sent = []

    async def send_message(umo, chain):
        sent.append((umo, chain))

    scheduler = FakeScheduler()
    context = SimpleNamespace(
        cron_manager=SimpleNamespace(scheduler=scheduler),
        send_message=send_message,
        _sent=sent,
    )
    return GroupChroniclePlugin(context, config), scheduler, sent


def test_cron_signature_stored_at_register():
    """注册成功 → _cron_signature 记录当前配置签名。"""
    config = {
        "enable_weekly_push": True,
        "push_groups": ["default:GroupMessage:g1", "g2"],
        "push_time": "20:30",
        "push_weekday": 6,
    }
    plugin, scheduler, _ = make_scheduler_context(config)
    run(plugin.initialize())
    assert len(scheduler.added) == 1
    assert plugin._cron_signature == (True, ("default:GroupMessage:g1", "g2"), "20:30", 6)


def test_cron_signature_change_reregisters(monkeypatch):
    """推送配置变化 → _weekly_push 开头自动重注册 (remove + add 新时间)。"""
    config = {
        "enable_weekly_push": True,
        "push_groups": ["default:GroupMessage:g1", "g2"],
        "push_time": "20:30",
        "push_weekday": 6,
    }
    plugin, scheduler, _ = make_scheduler_context(config)
    run(plugin.initialize())
    assert len(scheduler.added) == 1

    # 修改配置 → 签名变化
    config["push_time"] = "21:00"
    monkeypatch.setattr(plugin, "_weekly_push_one", amock(None))
    run(plugin._weekly_push())
    assert scheduler.removed == ["group_chronicle_weekly"]
    assert len(scheduler.added) == 2
    trigger_text = str(scheduler.added[-1]["trigger"])
    assert "hour='21'" in trigger_text
    assert plugin._cron_signature == (True, ("default:GroupMessage:g1", "g2"), "21:00", 6)


def test_cron_signature_unchanged_no_reregister(monkeypatch):
    """配置未变 → 不重复注册。"""
    config = {
        "enable_weekly_push": True,
        "push_groups": ["default:GroupMessage:g1"],
        "push_time": "20:30",
        "push_weekday": 6,
    }
    plugin, scheduler, _ = make_scheduler_context(config)
    run(plugin.initialize())
    monkeypatch.setattr(plugin, "_weekly_push_one", amock(None))
    run(plugin._weekly_push())
    assert scheduler.removed == []
    assert len(scheduler.added) == 1


def test_cron_reload_disabled(monkeypatch):
    """enable_cron_reload=false → 不检测/不重注册。"""
    config = {
        "enable_weekly_push": True,
        "push_groups": ["default:GroupMessage:g1"],
        "push_time": "20:30",
        "push_weekday": 6,
        "enable_cron_reload": False,
    }
    plugin, scheduler, _ = make_scheduler_context(config)
    run(plugin.initialize())
    config["push_time"] = "22:00"
    monkeypatch.setattr(plugin, "_weekly_push_one", amock(None))
    run(plugin._weekly_push())
    assert scheduler.removed == []
    assert len(scheduler.added) == 1


def test_terminate_clears_vote_state_and_render_cache(monkeypatch):
    """terminate → 清理投票状态, 调用 clear_render_cache。"""
    plugin = make_plugin()
    plugin._vote_state["default:GroupMessage:g1"] = {"topics": ["A"], "expires_at": 1, "voters": set()}
    cleared = []

    def fake_clear():
        cleared.append(1)

    monkeypatch.setattr(main_module, "clear_render_cache", fake_clear)
    run(plugin.terminate())
    assert plugin._vote_state == {}
    assert cleared == [1]
