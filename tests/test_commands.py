"""main.py 命令接线 / 并发控制 / 存储接线测试 (无 astrbot, 依赖 conftest 桩件)。

运行: python -m pytest tests/test_commands.py -v
"""

import asyncio
import time
from datetime import datetime
from types import SimpleNamespace

import main as main_module
from astrbot.api.event.filter import PermissionType
from astrbot.api.message_components import At
from core.evidence import NoActivityError
from core.store import ChronicleStore
from main import GroupChroniclePlugin

# ---------------------------------------------------------------------------
# 测试工具
# ---------------------------------------------------------------------------


class FakeEvent:
    """最小事件替身: 记录 event.send 发送的消息链。"""

    def __init__(
        self,
        umo="default:GroupMessage:10001",
        message_str="",
        is_admin=True,
        group_name="测试群",
        messages=None,
    ):
        self.unified_msg_origin = umo
        self.message_str = message_str
        self._is_admin = is_admin
        self._group_name = group_name
        self._messages = messages or []
        self.sent_chains = []

    async def send(self, chain):
        self.sent_chains.append(chain)

    def get_messages(self):
        return self._messages

    def get_group_name(self):
        return self._group_name

    def is_admin(self):
        return self._is_admin

    def plain_result(self, text):
        return text

    def sent_texts(self):
        """提取已发送链中的所有纯文本段。"""
        texts = []
        for chain in self.sent_chains:
            for seg in chain:
                if isinstance(seg, tuple) and seg[0] == "plain":
                    texts.append(str(seg[1]))
        return texts


def make_plugin(config=None, context=None):
    """构造插件实例; config 为 dict, context 默认含 send_message 记录器。"""
    if context is None:
        sent = []

        async def send_message(umo, chain):
            sent.append((umo, chain))

        context = SimpleNamespace(
            send_message=send_message, cron_manager=None, _sent=sent
        )
    return GroupChroniclePlugin(context, config or {})


def ctx_sent_texts(plugin) -> list[str]:
    """提取 context.send_message 发出的纯文本 (后台 prefer_context 路径)。"""
    sent = getattr(plugin.context, "_sent", None) or []
    texts = []
    for _umo, chain in sent:
        for seg in chain:
            if isinstance(seg, tuple) and seg[0] == "plain":
                texts.append(str(seg[1]))
    return texts


def all_sent_texts(plugin, event) -> list[str]:
    """合并 event.send 与 context.send_message 的纯文本。"""
    return list(event.sent_texts()) + ctx_sent_texts(plugin)


def amock(result):
    """async 假函数: 返回固定值。"""

    async def _f(*args, **kwargs):
        return result

    return _f


def araise(exc):
    """async 假函数: 抛出固定异常。"""

    async def _f(*args, **kwargs):
        raise exc

    return _f


async def collect(agen):
    """消费 async 生成器 (命令处理器), 返回 yield 结果列表。"""
    out = []
    async for item in agen:
        out.append(item)
    return out


def run(coro):
    """在独立事件循环中执行协程 (同步测试入口)。"""
    return asyncio.run(coro)


def make_evidence(
    newest_ts=1000.0,
    coverage_start=1700000000.0,
    coverage_end=1700003600.0,
    msg_count=5,
):
    """构造最小 EvidenceBundle 替身。"""
    return SimpleNamespace(
        chronicle_msgs=[],
        biography_msgs=[],
        memories=[],
        graph_snapshot=None,
        stats={},
        newest_ts=newest_ts,
        coverage_start_ts=coverage_start,
        coverage_end_ts=coverage_end,
        msg_count=msg_count,
    )


def patch_lm(monkeypatch, ready=True):
    """把 main 模块里的 LM 桥全部替换为假实现。"""
    monkeypatch.setattr(main_module, "detect_lm", lambda ctx: SimpleNamespace())
    monkeypatch.setattr(main_module, "ensure_ready", amock(ready))
    monkeypatch.setattr(main_module, "get_engine", lambda plugin: SimpleNamespace())
    monkeypatch.setattr(
        main_module, "get_conversation_manager", lambda plugin: SimpleNamespace()
    )


# ---------------------------------------------------------------------------
# 1. 命令分发
# ---------------------------------------------------------------------------


def test_chronicle_command_dispatches_pipeline_with_days(monkeypatch):
    """群史记 命令: _spawn_bg 调用 _run_group_analysis, 天数参数解析。"""
    plugin = make_plugin()
    calls = []

    async def fake_run(event, days=None, include_bios=True, **kwargs):
        calls.append(
            (
                kwargs.get("umo") or getattr(event, "unified_msg_origin", None),
                days,
                include_bios,
            )
        )

    monkeypatch.setattr(plugin, "_run_group_analysis", fake_run)
    event = FakeEvent(umo="default:GroupMessage:g1", message_str="群史记 3")

    async def main():
        results = await collect(plugin.chronicle_command(event, "3"))
        await asyncio.sleep(0.05)  # 让后台任务执行
        return results

    results = run(main())
    assert any("正在生成群史记" in r for r in results)
    assert calls == [("default:GroupMessage:g1", 3, True)]


def test_chronicle_command_default_days_when_missing(monkeypatch):
    """群史记 无天数参数 → days=None (使用配置默认窗口)。"""
    plugin = make_plugin()
    calls = []

    async def fake_run(event, days=None, include_bios=True, **kwargs):
        calls.append(days)

    monkeypatch.setattr(plugin, "_run_group_analysis", fake_run)
    event = FakeEvent(umo="default:GroupMessage:g1")

    async def main():
        await collect(plugin.chronicle_command(event, None))
        await asyncio.sleep(0.05)

    run(main())
    assert calls == [None]


def test_invalid_days_falls_back_to_none(monkeypatch):
    """群史记 非法天数 → days=None。"""
    plugin = make_plugin()
    calls = []

    async def fake_run(event, days=None, include_bios=True, **kwargs):
        calls.append(days)

    monkeypatch.setattr(plugin, "_run_group_analysis", fake_run)
    event = FakeEvent(umo="default:GroupMessage:g1")

    async def main():
        await collect(plugin.chronicle_command(event, "abc"))
        await asyncio.sleep(0.05)

    run(main())
    assert calls == [None]


# ---------------------------------------------------------------------------
# 2. 管理员命令: 权限元数据 + 无权限拒绝
# ---------------------------------------------------------------------------


def test_admin_handlers_carry_permission_metadata():
    """史记周推 带 __permission_type__=ADMIN; 人物传 改为运行时 bio_admin_only 门控。

    bio_admin_only (默认 true) 时人物传仅管理员可用, 但权限检查移到处理器
    内部 (运行时 _is_admin), 因此不再带框架级装饰器元数据 —— 这样
    bio_admin_only=false 时才能放开为任意成员。
    """
    plugin = make_plugin()
    assert (
        getattr(plugin.weekly_manual_command, "__permission_type__", None)
        is PermissionType.ADMIN
    )
    assert getattr(plugin.biography_command, "__permission_type__", None) is None
    assert getattr(plugin.chronicle_command, "__permission_type__", None) is None
    assert getattr(plugin.influence_command, "__permission_type__", None) is None


def test_bio_admin_only_false_allows_non_admin(monkeypatch):
    """bio_admin_only=false → 非管理员也可用 人物传。"""
    plugin = make_plugin({"bio_admin_only": False})
    calls = []

    async def fake_bio(event, umo, target, **kwargs):
        calls.append((umo, target))

    monkeypatch.setattr(plugin, "_bio_only", fake_bio)
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=False)

    async def main():
        results = await collect(plugin.biography_command(event))
        await asyncio.sleep(0.05)
        return results

    results = run(main())
    assert any("正在生成人物传记" in r for r in results)
    assert calls == [("default:GroupMessage:g1", None)]


def test_chronicle_admin_only_gates_command(monkeypatch):
    """chronicle_admin_only=true → 非管理员触发 群史记 直接 无权限, 不启动生成。"""
    plugin = make_plugin({"chronicle_admin_only": True})
    spawned = []

    async def fake_spawn(coro, *, label):
        spawned.append(label)

    monkeypatch.setattr(plugin, "_spawn_bg", fake_spawn)
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=False, message_str="群史记 3")
    results = run(collect(plugin.chronicle_command(event, "3")))
    assert results == ["无权限"]
    assert spawned == []


def test_chronicle_admin_only_false_allows_non_admin(monkeypatch):
    """chronicle_admin_only=false (默认) → 任意成员可触发 群史记。"""
    plugin = make_plugin({})
    spawned = []

    def fake_spawn(coro, *, label):
        spawned.append(label)
        coro.close()  # 测试中不真正运行生成, 关闭协程避免未 await 警告

    monkeypatch.setattr(plugin, "_spawn_bg", fake_spawn)
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=False, message_str="群史记")
    results = run(collect(plugin.chronicle_command(event, None)))
    assert any("正在生成群史记" in r for r in results)
    assert len(spawned) == 1


def test_admin_commands_reject_non_admin():
    """非管理员触发 人物传 / 史记周推 → 无权限, 不执行任何生成。"""
    plugin = make_plugin()
    event = FakeEvent(umo="default:GroupMessage:g1", is_admin=False)

    results = run(collect(plugin.biography_command(event)))
    assert results == ["无权限"]

    results = run(collect(plugin.weekly_manual_command(event)))
    assert results == ["无权限"]


def test_biography_command_dispatches_bio_only(monkeypatch):
    """人物传 @xxx: 解析 At → _bio_only(target)。"""
    plugin = make_plugin()
    calls = []

    async def fake_bio(event, umo, target, **kwargs):
        calls.append((umo, target))

    monkeypatch.setattr(plugin, "_bio_only", fake_bio)
    event = FakeEvent(umo="default:GroupMessage:g1", messages=[At(qq="12345")])

    async def main():
        results = await collect(plugin.biography_command(event))
        await asyncio.sleep(0.05)
        return results

    results = run(main())
    assert any("正在生成人物传记" in r for r in results)
    assert calls == [("default:GroupMessage:g1", "12345")]


def test_biography_command_without_at_dispatches_top_n(monkeypatch):
    """人物传 无 @ → _bio_only(target=None)。"""
    plugin = make_plugin()
    calls = []

    async def fake_bio(event, umo, target, **kwargs):
        calls.append(target)

    monkeypatch.setattr(plugin, "_bio_only", fake_bio)
    event = FakeEvent(umo="default:GroupMessage:g1")

    async def main():
        await collect(plugin.biography_command(event))
        await asyncio.sleep(0.05)

    run(main())
    assert calls == [None]


# ---------------------------------------------------------------------------
# 3. 并发控制 (per-umo 非阻塞锁)
# ---------------------------------------------------------------------------


def test_concurrent_same_umo_second_call_rejected(monkeypatch):
    """同一会话并发: 第二次调用收到 正在生成中, 生成只执行一次。"""
    plugin = make_plugin()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def slow_analyze(event, umo, days, include_bios, **kwargs):
        calls.append(umo)
        entered.set()
        await release.wait()

    monkeypatch.setattr(plugin, "_analyze", slow_analyze)
    event1 = FakeEvent(umo="default:GroupMessage:g1")
    event2 = FakeEvent(umo="default:GroupMessage:g1")

    async def main():
        t1 = asyncio.create_task(plugin._run_group_analysis(event1, None, True))
        await asyncio.wait_for(entered.wait(), timeout=2)
        t2 = asyncio.create_task(plugin._run_group_analysis(event2, None, True))
        done, _ = await asyncio.wait({t2}, timeout=2)
        release.set()
        await t1
        return done

    run(main())
    assert calls == ["default:GroupMessage:g1"]  # 生成只执行一次
    # 后台路径 prefer_context=True → 拒绝提示走 context.send_message
    assert any("正在生成中" in t for t in all_sent_texts(plugin, event2))
    assert event1.sent_texts() == []  # 第一次调用正常执行, 无拒绝提示


def test_different_umos_run_without_mutual_reject():
    """不同会话并发: 互不拒绝, 同时进入生成。"""
    plugin = make_plugin()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def slow_analyze(event, umo, days, include_bios, **kwargs):
        calls.append(umo)
        entered.set()
        await release.wait()

    plugin._analyze = slow_analyze
    event1 = FakeEvent(umo="default:GroupMessage:g1")
    event2 = FakeEvent(umo="g2")

    async def main():
        t1 = asyncio.create_task(plugin._run_group_analysis(event1, None, True))
        await asyncio.wait_for(entered.wait(), timeout=2)
        t2 = asyncio.create_task(plugin._run_group_analysis(event2, None, True))
        await asyncio.sleep(0.05)
        both_entered = len(calls) == 2
        release.set()
        await t1
        await t2
        return both_entered

    assert run(main()) is True
    assert all_sent_texts(plugin, event2) == []  # 未被拒绝


# ---------------------------------------------------------------------------
# 4. 存储接线 (should_skip / 保存)
# ---------------------------------------------------------------------------


def test_should_skip_prevents_llm_call(monkeypatch, tmp_path):
    """本周已生成且无新消息 → 不调用 LLM, 提示 史记查看 回看。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    iso_year, iso_week = datetime.now().isocalendar()[:2]
    plugin.store.save(
        {
            "umo": "default:GroupMessage:g1",
            "group_name": "测试群",
            "iso_year": iso_year,
            "iso_week": iso_week,
            "created_at": time.time(),
            "newest_msg_ts": 2000.0,
            "chronicle_text": "旧史记",
            "biographies": [],
            "image_path": None,
            "version": 1,
        }
    )
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(newest_ts=1000.0))
    )
    gen_calls = []

    async def fake_generate(*args, **kwargs):
        gen_calls.append(1)
        return "新史记"

    monkeypatch.setattr(main_module, "generate_chronicle", fake_generate)

    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert gen_calls == []  # LLM 未被调用
    assert any("本周史记已生成" in t for t in all_sent_texts(plugin, event))


def test_new_messages_do_not_skip(monkeypatch, tmp_path):
    """本周已生成但出现了新消息 → 正常重新生成。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    iso_year, iso_week = datetime.now().isocalendar()[:2]
    plugin.store.save(
        {
            "umo": "default:GroupMessage:g1",
            "group_name": "测试群",
            "iso_year": iso_year,
            "iso_week": iso_week,
            "created_at": time.time(),
            "newest_msg_ts": 100.0,
            "chronicle_text": "旧史记",
            "biographies": [],
            "image_path": None,
            "version": 1,
        }
    )
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module, "gather_evidence", amock(make_evidence(newest_ts=500.0))
    )
    gen_calls = []

    async def fake_generate(*args, **kwargs):
        gen_calls.append(1)
        return "新史记"

    monkeypatch.setattr(main_module, "generate_chronicle", fake_generate)
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(
        main_module,
        "render_text_or_image",
        _identity_render,
    )

    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert gen_calls == [1]
    assert any("新史记" in t for t in all_sent_texts(plugin, event))


def test_success_saves_entry_with_week_key(monkeypatch, tmp_path):
    """生成成功: 以 umo + 当前 ISO 周为键保存, 发送带覆盖头的正文。"""
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    patch_lm(monkeypatch)
    evidence = make_evidence(newest_ts=1000.0, msg_count=5)
    monkeypatch.setattr(main_module, "gather_evidence", amock(evidence))
    monkeypatch.setattr(main_module, "generate_chronicle", amock("## 本周史记"))
    monkeypatch.setattr(main_module, "rank_influential", amock([]))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    monkeypatch.setattr(main_module, "render_text_or_image", _identity_render)

    event = FakeEvent(umo="default:GroupMessage:g1", group_name="测试群")
    run(plugin._run_group_analysis(event, None, True))

    iso_year, iso_week = datetime.now().isocalendar()[:2]
    entries = plugin.store.list("default:GroupMessage:g1")
    assert len(entries) == 1
    entry = entries[0]
    assert entry["umo"] == "default:GroupMessage:g1"
    assert entry["group_name"] == "测试群"
    assert entry["iso_year"] == iso_year
    assert entry["iso_week"] == iso_week
    assert entry["image_path"] is None
    assert entry["biographies"] == []
    # 发送内容: 覆盖头 + 史记正文 (后台路径走 context.send_message)
    texts = all_sent_texts(plugin, event)
    assert any("覆盖" in t for t in texts)
    assert any("## 本周史记" in t for t in texts)


def test_no_activity_replies_error(monkeypatch):
    """无活动 → 友好错误回复, 不崩溃。"""
    plugin = make_plugin()
    patch_lm(monkeypatch)
    monkeypatch.setattr(
        main_module,
        "gather_evidence",
        araise(NoActivityError("该群在时间窗内无足够活动")),
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert any("无足够活动" in t for t in all_sent_texts(plugin, event))


def test_llm_unavailable_replies(monkeypatch):
    """生成时 LLM 不可用 → 明确提示, 不崩溃。"""
    plugin = make_plugin()
    patch_lm(monkeypatch)
    monkeypatch.setattr(main_module, "gather_evidence", amock(make_evidence()))
    monkeypatch.setattr(
        main_module,
        "generate_chronicle",
        araise(main_module.LLMUnavailableError("未配置可用的 LLM")),
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    assert any(
        "未配置可用的 LLM，无法生成群史记" in t for t in all_sent_texts(plugin, event)
    )


def test_lm_not_detected_replies_reason(monkeypatch):
    """LivingMemory 不可用 → 回复 lm_unavailable_reason 的具体原因。"""
    plugin = make_plugin()
    monkeypatch.setattr(main_module, "detect_lm", lambda ctx: None)
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._run_group_analysis(event, None, True))
    expected = main_module.lm_unavailable_reason(plugin.context, "not_detected")
    assert expected  # 非空原因
    assert any(t == expected for t in all_sent_texts(plugin, event))


# ---------------------------------------------------------------------------
# 5. 史记列表 / 史记查看
# ---------------------------------------------------------------------------


def test_chronicle_list_empty():
    plugin = make_plugin()
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.chronicle_list_command(event)))
    assert results == ["暂无史记"]


def test_chronicle_list_format(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    iso_year, iso_week = datetime.now().isocalendar()[:2]
    plugin.store.save(
        {
            "umo": "default:GroupMessage:g1",
            "group_name": "测试群",
            "iso_year": iso_year,
            "iso_week": iso_week,
            "created_at": 1700000000.0,
            "newest_msg_ts": 1.0,
            "chronicle_text": "正文",
            "biographies": [],
            "image_path": None,
            "version": 1,
        }
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.chronicle_list_command(event)))
    assert len(results) == 1
    assert f"1. {iso_year}-W{iso_week:02d}" in results[0]


def test_chronicle_view_not_found():
    plugin = make_plugin()
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.chronicle_view_command(event, "1")))
    assert results == ["未找到该条史记"]


def test_chronicle_view_ok_with_image_note(monkeypatch, tmp_path):
    plugin = make_plugin()
    plugin.store = ChronicleStore(str(tmp_path))
    iso_year, iso_week = datetime.now().isocalendar()[:2]
    plugin.store.save(
        {
            "umo": "default:GroupMessage:g1",
            "group_name": "测试群",
            "iso_year": iso_year,
            "iso_week": iso_week,
            "created_at": 1700000000.0,
            "newest_msg_ts": 1.0,
            "chronicle_text": "本周史记正文",
            "biographies": [],
            "image_path": "/data/images/x.png",
            "version": 1,
        }
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.chronicle_view_command(event, "1")))
    assert len(results) == 1
    assert "本周史记正文" in results[0]
    assert "/data/images/x.png" not in results[0]


def test_influence_command_renders_ranking(monkeypatch):
    """影响力榜: 无 LLM 生成调用, 输出排行文本。"""
    plugin = make_plugin()
    patch_lm(monkeypatch)
    monkeypatch.setattr(main_module, "gather_evidence", amock(make_evidence()))
    monkeypatch.setattr(
        main_module,
        "rank_influential",
        amock(
            [
                {
                    "sender_id": "1",
                    "sender_name": "张三",
                    "score": 0.9,
                    "breakdown": {},
                },
                {
                    "sender_id": "2",
                    "sender_name": "李四",
                    "score": 0.5,
                    "breakdown": {},
                },
            ]
        ),
    )
    event = FakeEvent(umo="default:GroupMessage:g1")
    results = run(collect(plugin.influence_command(event)))
    assert len(results) == 1
    assert "1. 张三 (0.9)" in results[0]
    assert "2. 李四 (0.5)" in results[0]


# ---------------------------------------------------------------------------
# 6. 人物传 路径
# ---------------------------------------------------------------------------


def test_bio_only_with_target(monkeypatch):
    """人物传 @成员: 只生成该成员传记。"""
    plugin = make_plugin()
    patch_lm(monkeypatch)
    evidence = make_evidence()
    evidence.chronicle_msgs = [
        SimpleNamespace(sender_id="123", sender_name="张三", timestamp=time.time())
    ]
    monkeypatch.setattr(main_module, "gather_evidence", amock(evidence))
    monkeypatch.setattr(main_module, "generate_biography", amock("张三的传记正文"))
    monkeypatch.setattr(main_module, "resolve_system_prompt", amock(None))
    event = FakeEvent(umo="default:GroupMessage:g1")

    run(plugin._bio_only(event, "default:GroupMessage:g1", "123"))
    texts = all_sent_texts(plugin, event)
    assert any("## 人物传记：张三" in t for t in texts)
    assert any("张三的传记正文" in t for t in texts)


def test_bio_only_target_not_found(monkeypatch):
    plugin = make_plugin()
    patch_lm(monkeypatch)
    monkeypatch.setattr(main_module, "gather_evidence", amock(make_evidence()))
    event = FakeEvent(umo="default:GroupMessage:g1")
    run(plugin._bio_only(event, "default:GroupMessage:g1", "99999"))
    assert any("未找到该成员的发言记录" in t for t in all_sent_texts(plugin, event))


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------


async def _identity_render(text, group_name, out_dir, **kwargs):
    """渲染替身: 原样返回文本, 不出图。"""
    return text, None
