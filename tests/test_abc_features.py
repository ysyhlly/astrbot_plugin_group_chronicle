"""ABC 批次功能命令测试 —— force / 超时 / sid / 月·年 / 风格 / 续写 / 导出 / 对比 / 关系弧 / 进度。

无 astrbot, 依赖 conftest 桩件; 复用 test_commands.py 的假事件与构造器模式。
运行: python -m pytest tests/test_abc_features.py -v
"""

import asyncio
import os
import time
from datetime import datetime
from types import SimpleNamespace

import main as main_module
from core.store import ChronicleStore
from main import GroupChroniclePlugin


class FakeEvent:
    """最小事件替身: 记录 event.send 发送的消息链。"""

    def __init__(self, umo="default:GroupMessage:10001", message_str="", is_admin=True):
        self.unified_msg_origin = umo
        self.message_str = message_str
        self._is_admin = is_admin
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
    """把生成流水线的 LLM/渲染全部替换为假实现。"""
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(evidence or make_evidence())
    )
    monkeypatch.setattr(main_module, "generate_chronicle", amock(chronicle_text))
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)


def save_entry(store, umo="g1", week=1, msg_count=5, text="史记正文", newest=100.0):
    iso_year = datetime.now().isocalendar()[0]
    store.save(
        {
            "umo": umo,
            "group_name": "测试群",
            "iso_year": iso_year,
            "iso_week": week,
            "created_at": time.time(),
            "newest_msg_ts": newest,
            "chronicle_text": text,
            "biographies": [],
            "image_path": None,
            "msg_count": msg_count,
            "version": 1,
        }
    )


# ---------------------------------------------------------------------------
# A3: force / 参数解析 / sid
# ---------------------------------------------------------------------------


def test_force_skips_should_skip(monkeypatch, tmp_path):
    """强制: 本周已生成且无新消息, force=True → 仍然重新生成。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    save_entry(plugin.store, newest=2000.0, text="旧史记")
    patch_full_pipeline(monkeypatch)

    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True, force=True))
    texts = all_sent_texts(plugin, event)
    assert any("新史记" in t for t in texts)
    assert not any("本周史记已生成" in t for t in texts)


def test_parse_chronicle_args_force_and_style():
    parsed = GroupChroniclePlugin._parse_chronicle_args(
        "3 强制", "群史记 3 强制 风格=八卦"
    )
    assert parsed.days == 3
    assert parsed.force is True
    assert parsed.style == "gossip"
    assert parsed.skip_check is True
    assert parsed.save is True

    parsed2 = GroupChroniclePlugin._parse_chronicle_args(None, "群史记 风格八卦 强制")
    assert parsed2.style == "gossip"
    assert parsed2.force is True
    assert parsed2.days is None


def test_parse_chronicle_args_month_year():
    parsed = GroupChroniclePlugin._parse_chronicle_args("月", "群史记 月")
    assert parsed.days == 30
    assert parsed.skip_check is False
    assert parsed.save is False  # 月史不落盘 (临时视图)
    assert parsed.force is False

    parsed2 = GroupChroniclePlugin._parse_chronicle_args("年", "群史记 年")
    assert parsed2.days == 365
    assert parsed2.skip_check is False
    assert parsed2.save is False


def test_chronicle_command_passes_force_style_skip(monkeypatch):
    plugin = make_plugin()
    calls = []

    async def fake_run(event, days=None, include_bios=True, **kwargs):
        calls.append(
            (
                days,
                kwargs.get("force"),
                kwargs.get("style"),
                kwargs.get("skip_check"),
                kwargs.get("save"),
            )
        )

    monkeypatch.setattr(plugin, "_run_group_analysis", fake_run)
    event = FakeEvent(umo="g1", message_str="群史记 月 强制 风格=文言")

    async def main():
        await collect(plugin.chronicle_command(event, "月 强制 风格=文言"))
        await asyncio.sleep(0.05)

    run(main())
    assert calls == [(30, True, "classical", False, False)]


def test_sid_command_returns_umo():
    plugin = make_plugin()
    event = FakeEvent(umo="aiocqhttp:GroupMessage:12345")
    results = run(collect(plugin.chronicle_sid_command(event)))
    assert len(results) == 1
    assert "aiocqhttp:GroupMessage:12345" in results[0]
    assert "push_groups" in results[0]


def test_sid_command_no_umo():
    plugin = make_plugin()
    event = FakeEvent(umo=None)
    results = run(collect(plugin.chronicle_sid_command(event)))
    assert results == ["无法获取会话标识"]


# ---------------------------------------------------------------------------
# A1: 超时 + 锁释放 + 进度
# ---------------------------------------------------------------------------


def test_generation_timeout_replies_and_releases_lock(monkeypatch):
    """生成超时 → 中文超时提示, 且锁被释放。"""
    plugin = make_plugin({"generation_timeout_sec": 0.05})
    patch_lm(monkeypatch)

    async def slow_gather(*args, **kwargs):
        await asyncio.sleep(5)
        return make_evidence()

    monkeypatch.setattr(main_module, "gather_evidence", slow_gather)
    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = all_sent_texts(plugin, event)
    assert any("生成超时" in t for t in texts)
    assert plugin._gen_locks["g1"].locked() is False  # 锁已释放


def test_progress_stages_sent(monkeypatch, tmp_path):
    """enable_progress=true (默认) → 流水线各阶段发送 ⏳ 提示。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_full_pipeline(monkeypatch)
    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = ctx_sent_texts(plugin)
    assert any(t.startswith("⏳ [1/5]") for t in texts)
    assert any(t.startswith("⏳ [2/5] 已采集") for t in texts)
    assert any(t.startswith("⏳ [3/5]") for t in texts)
    assert any(t.startswith("⏳ [5/5]") for t in texts)


def test_progress_disabled_no_stage_messages(monkeypatch, tmp_path):
    """enable_progress=false → 不发任何进度提示。"""
    plugin = make_plugin({"enable_progress": False})
    plugin.store = ChronicleStore(str(tmp_path))
    patch_full_pipeline(monkeypatch)
    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True))
    assert not any(t.startswith("⏳") for t in ctx_sent_texts(plugin))


# ---------------------------------------------------------------------------
# C1: 导出 / 续写 / 对比
# ---------------------------------------------------------------------------


def test_export_command_writes_file_and_falls_back_to_text(monkeypatch, tmp_path):
    """史记导出: 写 markdown 文件; 平台不支持文件时回退分段文本。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    save_entry(plugin.store, text="本周史记正文内容")
    event = FakeEvent(umo="g1")

    async def unsupported(*args, **kwargs):
        raise RuntimeError("synthetic unsupported file delivery")

    monkeypatch.setattr(plugin, "_send", unsupported)

    results = run(collect(plugin.chronicle_export_command(event, "1")))
    # event.send(chain) 的桩 MessageChain 无 .file() → 回退文本
    assert len(results) == 1
    assert "📄 导出内容" in results[0]
    assert "本周史记正文内容" in results[0]
    exports_dir = os.path.join(plugin.data_dir, "exports")
    assert os.path.isdir(exports_dir)
    assert any(f.endswith(".md") for f in os.listdir(exports_dir))


def test_export_command_not_found(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    event = FakeEvent(umo="g1")
    results = run(collect(plugin.chronicle_export_command(event, "1")))
    assert results == ["未找到该条史记"]


def test_continue_command_generates_from_latest(monkeypatch, tmp_path):
    """史记续写: 以最新一条史记为背景生成续篇。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    save_entry(plugin.store, text="上一期史记正文")
    patch_lm(monkeypatch)
    evidence = make_evidence()
    evidence.chronicle_msgs = [
        SimpleNamespace(timestamp=time.time() - 1, content="new", sender_id="u1")
    ]
    monkeypatch.setattr(main_module, "gather_evidence", amock(evidence))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    captured = {}

    async def fake_continuation(
        context, umo, previous_text, evidence, sp, days, **kwargs
    ):
        captured["previous"] = previous_text
        return "续篇正文内容"

    monkeypatch.setattr(main_module, "generate_continuation", fake_continuation)
    event = FakeEvent(umo="g1")
    run(plugin._continue_run(event, "g1"))
    texts = all_sent_texts(plugin, event)
    assert any("续篇正文内容" in t for t in texts)
    assert captured["previous"] == "上一期史记正文"


def test_continue_command_no_entry(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    event = FakeEvent(umo="g1")
    run(plugin._continue_run(event, "g1"))
    assert any("暂无史记可续写" in t for t in all_sent_texts(plugin, event))


def test_compare_command_two_entries(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    save_entry(plugin.store, week=30, msg_count=10, text="旧" * 50, newest=100.0)
    save_entry(
        plugin.store, week=31, msg_count=15, text="新" * 80, newest=100.0 + 86400
    )
    event = FakeEvent(umo="g1")
    results = run(collect(plugin.chronicle_compare_command(event)))
    assert len(results) == 1
    assert "史记对比" in results[0]
    assert "消息 15 条" in results[0] and "消息 10 条" in results[0]
    assert "+5 条" in results[0]
    assert "1.0 天" in results[0]


def test_compare_command_insufficient_entries(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    save_entry(plugin.store)
    event = FakeEvent(umo="g1")
    results = run(collect(plugin.chronicle_compare_command(event)))
    assert results == ["至少需要两条史记才能对比"]


# ---------------------------------------------------------------------------
# C2: 关系弧
# ---------------------------------------------------------------------------


def test_relation_arc_section_added_when_ra_available(monkeypatch, tmp_path):
    """RA 软来源可用 → 正文追加「本周关系弧」分节 (昵称映射)。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_full_pipeline(monkeypatch)
    evidence = make_evidence()
    evidence.chronicle_msgs = [
        SimpleNamespace(sender_id="1", sender_name="张三", timestamp=time.time()),
        SimpleNamespace(sender_id="2", sender_name="李四", timestamp=time.time()),
    ]
    monkeypatch.setattr(main_module, "gather_evidence", amock(evidence))
    monkeypatch.setattr(
        main_module, "relation_pairs", amock([("1", "2", 5.0), ("9", "8", 0.5)])
    )
    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True))
    texts = all_sent_texts(plugin, event)
    assert any("## 本周关系弧" in t for t in texts)
    assert any("张三 ⇄ 李四（紧密互动）" in t for t in texts)
    assert any("9 ⇄ 8（偶有交集）" in t for t in texts)


def test_relation_arc_skipped_when_ra_missing(monkeypatch, tmp_path):
    """RA 不可用 (None) → 不追加关系弧分节。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_full_pipeline(monkeypatch)
    monkeypatch.setattr(main_module, "relation_pairs", amock(None))
    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True))
    assert not any("本周关系弧" in t for t in all_sent_texts(plugin, event))


def test_relation_arc_skipped_when_ra_raises(monkeypatch, tmp_path):
    """RA 调用抛异常 → 静默跳过, 不中断生成。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_full_pipeline(monkeypatch)

    async def boom(*args, **kwargs):
        raise RuntimeError("ra boom")

    monkeypatch.setattr(main_module, "relation_pairs", boom)
    event = FakeEvent(umo="g1")
    run(plugin._run_group_analysis(event, None, True))
    assert any("新史记" in t for t in all_sent_texts(plugin, event))
