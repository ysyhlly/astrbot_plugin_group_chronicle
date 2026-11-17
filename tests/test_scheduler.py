"""main.py 调度接线测试 (无 astrbot, 依赖 conftest 桩件 + 本地真实 apscheduler)。

覆盖:
- 周任务仅在 enable_weekly_push=True 且 push_groups 非空时注册;
- push_time / push_weekday / misfire_grace_time / replace_existing 参数正确;
- 非法时间 / 缺 cron_manager → 跳过注册且不崩溃;
- terminate 移除任务;
- 每周推送循环: 单群失败 (NoActivityError / LLMUnavailableError / 其他异常)
  不影响后续群; should_skip 静默跳过; 通过 context.send_message 发送。

运行: python -m pytest tests/test_scheduler.py -v
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from core.evidence import NoActivityError
from core.llm_call import LLMUnavailableError
from main import GroupChroniclePlugin, _SkipWeek

# ---------------------------------------------------------------------------
# 假调度器
# ---------------------------------------------------------------------------


class FakeScheduler:
    """APScheduler AsyncIOScheduler 替身 (add_job 同步, 镜像真实实现)。"""

    def __init__(self):
        self.added = []
        self.removed = []

    def add_job(self, func, trigger=None, id=None, **kwargs):
        if id == "group_chronicle_history_hourly":
            return  # This fixture asserts only the weekly scheduler contract.
        self.added.append(
            {"func": func, "trigger": trigger, "id": id, "kwargs": kwargs}
        )

    def remove_job(self, job_id):
        if job_id == "group_chronicle_history_hourly":
            return
        self.removed.append(job_id)


class AsyncAddScheduler(FakeScheduler):
    """add_job 为协程的替身 (验证 await 路径)。"""

    async def add_job(self, func, trigger=None, id=None, **kwargs):
        if id == "group_chronicle_history_hourly":
            return
        self.added.append(
            {"func": func, "trigger": trigger, "id": id, "kwargs": kwargs}
        )


def make_context(scheduler_cls=FakeScheduler):
    sent = []

    async def send_message(umo, chain):
        sent.append((umo, chain))

    return (
        SimpleNamespace(
            cron_manager=SimpleNamespace(scheduler=scheduler_cls()),
            send_message=send_message,
        ),
        sent,
    )


def make_plugin(config, scheduler_cls=FakeScheduler):
    context, sent = make_context(scheduler_cls)
    return GroupChroniclePlugin(context, config), context, sent


def run(coro):
    return asyncio.run(coro)


def enable_config(**overrides):
    config = {
        "enable_weekly_push": True,
        "push_groups": ["g1", "g2"],
        "push_time": "20:30",
        "push_weekday": 6,
    }
    config.update(overrides)
    return config


# ---------------------------------------------------------------------------
# 1. 注册条件与参数
# ---------------------------------------------------------------------------


def test_job_registered_when_enabled_and_groups_configured():
    """启用 + 配置目标群 → 注册 CronTrigger 周任务。"""
    plugin, context, _ = make_plugin(enable_config())
    run(plugin.initialize())

    scheduler = context.cron_manager.scheduler
    assert len(scheduler.added) == 1
    job = scheduler.added[0]
    assert job["id"] == "group_chronicle_weekly"
    assert job["func"] == plugin._weekly_push
    assert job["kwargs"]["replace_existing"] is True
    assert job["kwargs"]["misfire_grace_time"] == 60
    trigger = job["trigger"]
    trigger_text = str(trigger)
    assert "day_of_week='6'" in trigger_text
    assert "hour='20'" in trigger_text
    assert "minute='30'" in trigger_text


def test_job_not_registered_when_disabled():
    """enable_weekly_push=false → 不注册。"""
    plugin, context, _ = make_plugin(enable_config(enable_weekly_push=False))
    run(plugin.initialize())
    assert context.cron_manager.scheduler.added == []


def test_job_not_registered_when_groups_empty():
    """push_groups 为空 → 不注册。"""
    plugin, context, _ = make_plugin(enable_config(push_groups=[]))
    run(plugin.initialize())
    assert context.cron_manager.scheduler.added == []


def test_job_not_registered_when_invalid_push_time():
    """push_time 非法 → 跳过注册, 不崩溃。"""
    plugin, context, _ = make_plugin(enable_config(push_time="25:99"))
    run(plugin.initialize())
    assert context.cron_manager.scheduler.added == []

    plugin2, context2, _ = make_plugin(enable_config(push_time="abc"))
    run(plugin2.initialize())
    assert context2.cron_manager.scheduler.added == []


def test_job_not_registered_when_cron_manager_missing():
    """context 无 cron_manager → 跳过注册, 不崩溃。"""
    plugin = GroupChroniclePlugin(
        SimpleNamespace(send_message=AsyncMock()), enable_config()
    )
    run(plugin.initialize())  # 不应抛异常


def test_initialize_never_crashes_on_weird_config():
    """畸形配置 (非 dict 形态) → 初始化不崩溃。"""
    plugin, context, _ = make_plugin(enable_config(push_weekday="x", push_time=None))
    run(plugin.initialize())
    assert context.cron_manager.scheduler.added == []


def test_job_registered_with_async_add_job():
    """add_job 为协程时走 await 路径, 同样注册成功。"""
    plugin, context, _ = make_plugin(enable_config(), scheduler_cls=AsyncAddScheduler)
    run(plugin.initialize())
    assert len(context.cron_manager.scheduler.added) == 1


# ---------------------------------------------------------------------------
# 2. terminate 移除任务
# ---------------------------------------------------------------------------


def test_terminate_removes_weekly_job():
    plugin, context, _ = make_plugin(enable_config())
    run(plugin.initialize())
    run(plugin.terminate())
    assert context.cron_manager.scheduler.removed == ["group_chronicle_weekly"]


def test_terminate_safe_without_cron_manager():
    plugin = GroupChroniclePlugin(
        SimpleNamespace(send_message=AsyncMock()), enable_config()
    )
    run(plugin.terminate())  # 不应抛异常
    assert plugin._terminating is True


def test_terminate_safe_when_remove_job_raises():
    class RaisingScheduler(FakeScheduler):
        def remove_job(self, job_id):
            raise RuntimeError("scheduler gone")

    plugin, _, _ = make_plugin(enable_config(), scheduler_cls=RaisingScheduler)
    run(plugin.terminate())  # 异常被吞, 不抛出


# ---------------------------------------------------------------------------
# 3. 每周推送循环
# ---------------------------------------------------------------------------


def test_weekly_push_continues_past_failures(monkeypatch):
    """单群 NoActivityError / LLMUnavailableError / 其他异常 → 继续下一群。"""
    plugin, _, _ = make_plugin(
        {"enable_weekly_push": True, "push_groups": ["g_bad", "g_llm", "g_err", "g_ok"]}
    )
    attempted = []

    async def fake_push_one(umo):
        attempted.append(umo)
        if umo == "g_bad":
            raise NoActivityError("该群本周无活动")
        if umo == "g_llm":
            raise LLMUnavailableError("未配置可用的 LLM")
        if umo == "g_err":
            raise RuntimeError("未知错误")

    monkeypatch.setattr(plugin, "_weekly_push_one", fake_push_one)
    run(plugin._weekly_push())
    assert attempted == ["g_bad", "g_llm", "g_err", "g_ok"]


def test_weekly_push_skips_when_no_change(monkeypatch):
    """should_skip → 静默跳过, 不发送。"""
    plugin, _, sent = make_plugin({"enable_weekly_push": True, "push_groups": ["g1"]})
    monkeypatch.setattr(plugin, "_build_entry", _raise_skip)
    run(plugin._weekly_push())
    assert sent == []


def test_weekly_push_sends_via_context_send_message(monkeypatch):
    """正常推送: context.send_message(umo, MessageChain) 发送正文。"""
    plugin, _, sent = make_plugin(
        {"enable_weekly_push": True, "push_groups": ["g1", "g2"]}
    )
    monkeypatch.setattr(plugin, "_build_entry", _make_entry("本周史记正文"))
    run(plugin._weekly_push())
    assert [umo for umo, _ in sent] == ["g1", "g2"]
    for umo, chain in sent:
        segments = list(chain)
        assert any(seg[0] == "plain" and seg[1] == "本周史记正文" for seg in segments)


def test_weekly_push_sends_image_when_available(monkeypatch):
    """有配图 → context.send_message 发送 file_image。"""
    plugin, _, sent = make_plugin({"enable_weekly_push": True, "push_groups": ["g1"]})
    monkeypatch.setattr(
        plugin, "_build_entry", _make_entry("正文", image_path="/tmp/x.png")
    )
    run(plugin._weekly_push())
    assert len(sent) == 1
    segments = list(sent[0][1])
    assert any(seg[0] == "file_image" and seg[1] == "/tmp/x.png" for seg in segments)


def test_weekly_push_noop_when_terminating(monkeypatch):
    """卸载后触发 → 不推送任何群。"""
    plugin, _, sent = make_plugin({"enable_weekly_push": True, "push_groups": ["g1"]})
    plugin._terminating = True
    attempted = []

    async def fake_push_one(umo):
        attempted.append(umo)

    monkeypatch.setattr(plugin, "_weekly_push_one", fake_push_one)
    run(plugin._weekly_push())
    assert attempted == []
    assert sent == []


def test_weekly_push_one_reuses_existing_lock(monkeypatch):
    """周推与手动生成共用同一把 umo 锁 (setdefault 复用已有锁)。"""
    plugin, _, sent = make_plugin({"enable_weekly_push": True, "push_groups": ["g1"]})
    pre = asyncio.Lock()
    plugin._gen_locks["g1"] = pre
    monkeypatch.setattr(plugin, "_build_entry", _make_entry("正文"))
    run(plugin._weekly_push_one("g1"))
    assert plugin._gen_locks["g1"] is pre  # 复用预置锁, 不重复创建
    assert len(sent) == 1
    assert sent[0][0] == "g1"


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------


async def _raise_skip(*args, **kwargs):
    raise _SkipWeek()


def _make_entry(text, image_path=None):
    """_build_entry 替身工厂: 返回固定 (text, image_path)。"""

    async def _f(*args, **kwargs):
        return text, image_path

    return _f
