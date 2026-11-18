"""群史记插件 —— 命令 / 调度 / 输出接线层。

本文件只负责:
1. 注册命令处理器 (群史记 / 人物传 / 影响力榜 / 史记列表 / 史记查看 /
   史记周推 / 史记sid / 史记续写 / 史记导出 / 史记对比 / 史记网页 /
   史记选题 / 史记选票 / 史记选题公布 / 宇宙史记);
2. 注册每周定时推送任务 (APScheduler CronTrigger, 仅启用且配置了目标群时;
   配置变化自动热加载重注册);
3. 组装共享生成流水线 (证据采集 → 群史记 → 人物传记 → 群友图鉴 → 关系弧
   → 渲染 → 保存 → 发送), 全部实际逻辑都在 core/* 模块, 本文件不做任何
   业务实现。

并发模型: 每个会话 (umo) 一把非阻塞锁 —— 生成中再次触发直接拒绝
("正在生成中，请稍候"), 不加队列; 锁内再阻塞获取, 串行化竞态。

超时: 生成主体以 asyncio.wait_for 包裹, 超时回复中文提示并自动释放锁
(配置 generation_timeout_sec)。

进度: 流水线关键阶段通过 context.send_message 发送短提示
(配置 enable_progress, 仅后台路径)。

长文本: 结果统一走 core/deliver.deliver_text —— 有图先发图, 超长文本优先
合并转发 Nodes, 否则多段纯文本 (配置 max_reply_chars / use_forward_nodes)。

发送模式:
- 命令处理器: yield event.plain_result(...) (镜像 livingmemory / admin_emoji_reply);
- 内部异步流水线: await event.send(MessageChain()...) (镜像 rollpig / qq_tools);
- 定时推送 (无事件): await context.send_message(umo, MessageChain()...)
  (镜像 zhenxunribao main.py:497 的精确签名)。
"""

if __package__:
    from .reload_bootstrap import ensure_current

    ensure_current(
        __package__,
        __name__,
        __file__,
        checks=[("core.archive_store", "ArchiveStore", "fragment_source_bound")],
    )

import asyncio
import copy
import html
import inspect
import math
import os
import re
import time
import types
import uuid
from datetime import datetime
from importlib import import_module, reload
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.event.filter import PermissionType
from astrbot.api.message_components import At, File
from astrbot.api.star import Context, Star, StarTools, register

# AstrBot 以包方式加载插件，必须用相对导入 (.core.*)。
# 本地 pytest 把插件根目录塞进 sys.path 后以顶层模块 import main，
# 相对导入会失败，故回退到绝对 from core.*（仅测试环境）。
# 必须用 if __package__ 判断而不是 try/except ImportError：后者会把
# .core.* 内部真实的缺依赖 ImportError 一并吞掉并伪装成「顶层 import main」，
# 报错退化成 No module named 'core'，掩盖真正的故障。
if __package__:
    from .core.biography import generate_biography
    from .core.culture_service import CultureService
    from .core.culture_web import CultureWeb
    from .core.deliver import build_export_markdown, deliver_text, split_text
    from .core.evidence import NoActivityError, gather_evidence
    from .core.gallery import build_gallery
    from .core.generation import (
        generate_chronicle,
        generate_continuation,
        generate_topic_candidates,
        generate_universe_chronicle,
    )
    from .core.history_capture import (
        HistoryCaptureFilter,
    )
    from .core.history_capture import (
        set_active as set_active_history,
    )
    from .core.history_commands import COMMANDS, command_reply
    from .core.influence import rank_influential, relation_pairs
    from .core.lifetime import HistoryService
    from .core.llm_call import LLMUnavailableError
    from .core.lm_bridge import (
        detect_lm,
        ensure_ready,
        get_conversation_manager,
        get_engine,
        lm_unavailable_reason,
    )
    from .core.persona import resolve_system_prompt
    from .core.render import clear_render_cache, markdown_to_html, render_text_or_image
    from .core.store import ChronicleStore
else:  # Top-level import is reserved for the test harness.
    from core.biography import generate_biography
    from core.culture_service import CultureService
    from core.culture_web import CultureWeb
    from core.deliver import build_export_markdown, deliver_text, split_text
    from core.evidence import NoActivityError, gather_evidence
    from core.gallery import build_gallery
    from core.generation import (
        generate_chronicle,
        generate_continuation,
        generate_topic_candidates,
        generate_universe_chronicle,
    )
    from core.history_capture import (
        HistoryCaptureFilter,
    )
    from core.history_capture import (
        set_active as set_active_history,
    )
    from core.history_commands import COMMANDS, command_reply
    from core.influence import rank_influential, relation_pairs
    from core.lifetime import HistoryService
    from core.llm_call import LLMUnavailableError
    from core.lm_bridge import (
        detect_lm,
        ensure_ready,
        get_conversation_manager,
        get_engine,
        lm_unavailable_reason,
    )
    from core.persona import resolve_system_prompt
    from core.render import clear_render_cache, markdown_to_html, render_text_or_image
    from core.store import ChronicleStore

#: LLMUnavailableError 的固定消息 (core/llm_call.py), 用于区分"生成时 LLM 失败"与
#: "LivingMemory 未就绪" 两类不可用场景, 给出不同的用户提示。
_LLM_UNAVAILABLE_MSG = "未配置可用的 LLM"


class _SkipWeek(Exception):
    """内部控制流: 本周史记已生成且无新消息, 跳过本次生成 (不视为错误)。"""


@register(
    "GroupChronicle",
    "user",
    "群史记: 基于长期记忆与群聊历史生成群史记与人物传记",
    "v0.3.0-rc.3",
    "",
)
class GroupChroniclePlugin(Star):
    """
    群史记插件。

    基于长期记忆（living memory）与群聊历史，定时生成群史记与人物传记，
    并在每周指定时间向配置的群聊推送。
    """

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.config = config
        self.data_dir = str(StarTools.get_data_dir("astrbot_plugin_group_chronicle"))
        self._terminating = False
        #: 史记持久化存储 (ChronicleStore 内部 makedirs, 目录不存在会自动创建)
        self.store = ChronicleStore(os.path.join(self.data_dir, "chronicles"))
        #: 每会话 (umo) 一把非阻塞生成锁
        self._gen_locks: dict[str, asyncio.Lock] = {}
        #: 投票选题状态: umo -> {topics, expires_at, voters} (史记选题/选票/公布)
        self._vote_state: dict[str, dict] = {}
        #: 投票状态互斥锁 (选题后台写入 / 选票·公布读取互斥)
        self._vote_lock = asyncio.Lock()
        #: 每周推送配置签名 (热加载检测: 变化则自动重注册定时任务)
        self._cron_signature: tuple | None = None
        #: 后台任务强引用, 防止 create_task 被 GC 且便于 done_callback 收尸
        self._bg_tasks: set[asyncio.Task] = set()
        #: 每周定时推送任务 ID
        self._weekly_job_id = "group_chronicle_weekly"
        self.history = HistoryService(self, self.store)
        self.culture = CultureService(self)
        self.culture_web = CultureWeb(self)
        self.store = self.history.editions
        set_active_history(self)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def get_dynamics_capabilities(self) -> dict[str, Any]:
        """Expose companion health to the Chat Dynamics series presence bar."""
        return {
            "schema_version": 1,
            "provider_id": "astrbot_plugin_group_chronicle",
            "display_name": "群史记",
            "enabled": bool(self._cfg("history_enabled", True)),
            "ready": not self._terminating
            and getattr(self, "history", None) is not None,
            "capabilities": {"chronicle": 1, "outcomes": 1, "culture": 1},
            "api_version": 1,
        }

    async def record_valuable_outcome(
        self, umo: str, outcome: dict[str, Any]
    ) -> dict[str, Any]:
        """Controlled public entrypoint for Dynamics Games valuable completed outcomes."""
        if self._terminating or getattr(self, "history", None) is None:
            return {
                "schema_version": 1,
                "admitted": False,
                "status": "unavailable",
                "reason": "not_ready",
            }
        return await self.history.record_valuable_outcome(umo, outcome)

    async def initialize(self):
        """插件初始化入口: 注册每周定时推送 (失败只记录日志, 绝不崩溃)。"""
        self.culture_web.register()
        if callable(getattr(self.context, "register_web_api", None)):
            name = f"{__package__}.series_panels" if __package__ else "series_panels"
            series_panels = import_module(name)
            reload(series_panels).register_series_panels(
                self, "astrbot_plugin_group_chronicle"
            )
        try:
            await self.history.initialize()
        except Exception:
            logger.error("Lifetime history initialization failed", exc_info=True)
        try:
            await self._setup_weekly_job()
        except Exception as e:  # noqa: BLE001 - 初始化异常不阻断插件加载
            logger.error(f"初始化失败, 定时任务注册已跳过: {e}", exc_info=True)

    async def _setup_weekly_job(self) -> None:
        """仅当启用每周推送且配置了目标群时, 注册 CronTrigger 周任务。"""
        if not self._cfg("enable_weekly_push", False):
            logger.info("未启用每周定时推送 (enable_weekly_push=false), 跳过注册")
            return
        push_groups = self._cfg("push_groups", []) or []
        if not push_groups:
            logger.info("未配置推送目标群 (push_groups 为空), 跳过注册")
            return

        hour, minute = self._parse_push_time(self._cfg("push_time", "20:00"))
        if hour is None or minute is None:
            logger.error(
                f"推送时间格式无效: {self._cfg('push_time', '20:00')!r} "
                f"(应为 HH:MM), 跳过注册"
            )
            return

        weekday = int(self._cfg("push_weekday", 6))  # 0=周一 .. 6=周日
        scheduler = getattr(self.context, "cron_manager", None)
        if scheduler is None:
            logger.warning("未检测到 context.cron_manager, 无法注册每周定时推送")
            return
        scheduler = getattr(scheduler, "scheduler", None)
        if scheduler is None:
            logger.warning("cron_manager 未暴露 scheduler, 无法注册每周定时推送")
            return

        trigger = CronTrigger(day_of_week=str(weekday), hour=hour, minute=minute)
        add_kwargs = {
            "trigger": trigger,
            "id": self._weekly_job_id,
            "replace_existing": True,
            "misfire_grace_time": 60,
        }
        # APScheduler 的 add_job 是同步方法 (镜像 daily_analysis); 若实现为
        # 协程则 await 之。用 isawaitable 判断可保证 add_job 只执行一次。
        result = scheduler.add_job(self._weekly_push, **add_kwargs)
        if inspect.isawaitable(result):
            await result
        # 注册成功后记录配置签名 (热加载检测基准)
        self._cron_signature = self._compute_cron_signature()
        logger.info(
            f"已注册每周史记推送: 星期{weekday} {hour:02d}:{minute:02d} "
            f"(job id: {self._weekly_job_id}, 群数: {len(push_groups)})"
        )

    def _compute_cron_signature(self) -> tuple:
        """每周推送配置签名: (enable, groups, time, weekday)。"""
        groups = tuple(str(g) for g in (self._cfg("push_groups", []) or []))
        return (
            bool(self._cfg("enable_weekly_push", False)),
            groups,
            str(self._cfg("push_time", "20:00")),
            int(self._cfg("push_weekday", 6)),
        )

    async def _maybe_reload_cron_job(self) -> None:
        """配置变更检测: 签名变化 → 移除旧任务并重新注册 (全程守卫, 绝不崩溃)。

        由 initialize 与 _weekly_push 开头调用 (enable_cron_reload 门控)。
        """
        current = self._compute_cron_signature()
        if self._cron_signature == current:
            return
        logger.info(
            f"每周推送配置变更, 重新注册定时任务: {self._cron_signature} -> {current}"
        )
        scheduler = getattr(self.context, "cron_manager", None)
        if scheduler is not None:
            scheduler = getattr(scheduler, "scheduler", None)
            remove_job = getattr(scheduler, "remove_job", None)
            if callable(remove_job):
                try:
                    remove_job(self._weekly_job_id)
                except Exception as e:  # noqa: BLE001 - 移除失败不影响重注册
                    logger.warning(f"移除旧定时任务失败 (忽略): {e}")
        await self._setup_weekly_job()
        # _setup_weekly_job 成功时会更新签名; 未注册 (被禁用) 也记录当前签名,
        # 避免每次推送都重复尝试
        self._cron_signature = current

    async def terminate(self):
        """插件卸载入口: 移除每周定时任务 / 清理投票状态与渲染缓存
        (任何异常只记录, 不抛出)。"""
        self._terminating = True
        self.culture_web.close()
        panel = getattr(self, "_series_panels", None)
        if panel is not None:
            panel.close()
        set_active_history(None)
        history = getattr(self, "history", None)
        if history is not None:
            await history.terminate()
        tasks = [task for task in self._bg_tasks if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._bg_tasks.clear()
        self._vote_state.clear()
        try:
            clear_render_cache()
        except Exception as e:  # noqa: BLE001 - 缓存清理失败忽略
            logger.warning(f"清理渲染缓存失败 (忽略): {e}")
        try:
            scheduler = getattr(self.context, "cron_manager", None)
            if scheduler is not None:
                scheduler = getattr(scheduler, "scheduler", None)
                remove_job = getattr(scheduler, "remove_job", None)
                if callable(remove_job):
                    remove_job(self._weekly_job_id)
                    logger.info(f"已移除定时任务: {self._weekly_job_id}")
        except Exception as e:  # noqa: BLE001 - 卸载时 cron_manager 可能已销毁
            logger.warning(f"移除定时任务失败 (忽略): {e}")

    # ------------------------------------------------------------------
    # 群文化：所有入口复用同一服务，不另行监听或抢占群聊回复。
    # ------------------------------------------------------------------

    def get_culture_capabilities(self):
        return {
            "schema_version": 1,
            "ready": not self._terminating,
            "enabled": bool(self._cfg("culture_enabled", False)),
            "capabilities": {"culture": 1},
        }

    async def get_culture_context(
        self, *, scope, member, query="", kind="search", limit=3
    ):
        """Read-only v1 public interface. Consumers must revalidate before sending."""
        result = await self.culture.query(scope, member, query, kind, limit)
        privacy = await asyncio.to_thread(self.history.privacy_key, scope)
        result["receipt"] = {
            "schema_version": 1,
            "scope": scope,
            "member": member,
            "privacy": privacy,
            "entries": [(e["id"], e["revision"]) for e in result["entries"]],
        }
        return result

    async def validate_culture_context(self, *, scope, member, receipt):
        """Revalidate a public v1 read receipt immediately before using its evidence."""
        if (
            not isinstance(receipt, dict)
            or receipt.get("schema_version") != 1
            or receipt.get("scope") != scope
            or receipt.get("member") != member
        ):
            return False
        try:
            return await self.culture.receipt_current(scope, receipt)
        except (ValueError, KeyError, TypeError):
            return False

    @filter.llm_tool(name="query_group_culture")
    async def query_group_culture(
        self, event: AstrMessageEvent, query: str = "", kind: str = "search"
    ) -> str:
        """查询当前群梗、黑话、外号或名场面的含义、起源与演变证据。多个候选需澄清，起源未知不能编造。

        Args:
            query(str): 群内词语、关键词或词条ID，留空列出近期文化词条。
            kind(str): search 查询含义，origin 查询起源及演变时间线。
        """
        return await self.culture.tool(event, query, kind)

    @filter.command("群文化")
    async def culture_search(self, event: AstrMessageEvent, query: str = ""):
        try:
            yield event.plain_result(await self.culture.command(event, "群文化", query))
        except (ValueError, PermissionError) as exc:
            yield event.plain_result(str(exc))

    @filter.command("梗溯源")
    async def culture_origin(self, event: AstrMessageEvent, query: str = ""):
        try:
            yield event.plain_result(await self.culture.command(event, "梗溯源", query))
        except (ValueError, PermissionError) as exc:
            yield event.plain_result(str(exc))

    @filter.command("文化纠正")
    async def culture_correct(
        self, event: AstrMessageEvent, entry_id: str = "", note: str = ""
    ):
        try:
            # The host command parser may split multiword reasons; use the original tail.
            tail = (
                str(getattr(event, "message_str", ""))
                .lstrip("/")
                .partition("文化纠正")[2]
                .strip()
            )
            yield event.plain_result(
                await self.culture.command(
                    event, "文化纠正", tail or f"{entry_id} {note}"
                )
            )
        except (ValueError, PermissionError) as exc:
            yield event.plain_result(str(exc))

    @filter.command("文化隐藏")
    async def culture_hide(self, event: AstrMessageEvent, entry_id: str = ""):
        try:
            yield event.plain_result(
                await self.culture.command(event, "文化隐藏", entry_id)
            )
        except (ValueError, PermissionError) as exc:
            yield event.plain_result(str(exc))

    @filter.on_decorating_result(priority=1000)
    async def culture_result_guard(self, event: AstrMessageEvent):
        receipts = getattr(event, "_culture_receipts_v1", [])
        for receipt in receipts:
            if not await self.culture.receipt_current(
                str(event.unified_msg_origin), receipt
            ):
                result = event.get_result()
                if result is not None:
                    result.chain = []
                event.stop_event()
                return

    # ------------------------------------------------------------------
    # 配置助手
    # ------------------------------------------------------------------

    def _cfg(self, key, default=None):
        """Read configuration with finite bounds for resource and scheduling values.

        Args:
            key: Configuration field.
            default: Fallback value used for invalid or missing fields.

        Returns:
            A validated configuration value.
        """
        getter = getattr(self.config, "get", None)
        try:
            value = (
                getter(key, default)
                if callable(getter)
                else getattr(self.config, key, default)
            )
        except Exception:
            value = default
        bounds = {
            "chronicle_days": (1, 3650),
            "biography_days": (1, 3650),
            "max_msgs": (1, 100000),
            "max_memories": (1, 10000),
            "top_n_bios": (1, 20),
            "min_activity_messages": (1, 100000),
            "generation_timeout_sec": (0.01, 3600),
            "max_reply_chars": (1, 6000),
            "push_weekday": (0, 6),
            "influence_w_lm": (0, 1),
            "influence_w_sl": (0, 1),
            "influence_w_ra": (0, 1),
            "history_batch_size": (1, 1000),
            "history_daily_limit": (1, 1000),
            "history_bio_points": (1, 10000),
            "history_bio_events": (1, 1000),
            "history_full_points": (1, 100000),
            "history_full_events": (1, 10000),
            "history_full_span_days": (0, 36500),
            "history_full_major_core": (1, 1000),
        }
        if key in bounds:
            low, high = bounds[key]
            try:
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError("Non-finite configuration")
            except (ValueError, TypeError, OverflowError):
                number = float(default if default is not None else low)
            number = max(low, min(high, number))
            value = (
                number
                if key.startswith("influence_w_") or key == "generation_timeout_sec"
                else int(number)
            )
        return value

    @staticmethod
    def _parse_push_time(value) -> tuple[int | None, int | None]:
        """解析 "HH:MM" 为 (hour, minute); 非法输入返回 (None, None)。"""
        try:
            hour_str, minute_str = str(value).strip().split(":")
            hour, minute = int(hour_str), int(minute_str)
        except Exception:
            return None, None
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None, None
        return hour, minute

    # ------------------------------------------------------------------
    # 发送助手
    # ------------------------------------------------------------------
    # 后台 create_task 场景下 handler 已返回, event.send 常失效/挂起。
    # 因此后台路径一律走 context.send_message(umo, chain); event.send 仅作
    # 同步 handler 内的可选加速, 失败必须回退。

    async def _send(
        self, event, umo: str | None, chain, *, prefer_context: bool = False
    ) -> None:
        """发送消息链。

        prefer_context=True (后台任务): 直接 context.send_message, 不碰 event。
        否则: 先试 event.send, 失败再 context.send_message。
        """
        if not umo and event is not None:
            umo = getattr(event, "unified_msg_origin", None)

        if not prefer_context and event is not None:
            send = getattr(event, "send", None)
            if callable(send):
                try:
                    await send(chain)
                    return
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"event.send 失败, 回退 context.send_message: {e}")

        if not umo:
            raise RuntimeError("无法发送消息: umo 为空且 event.send 不可用")
        if await self.context.send_message(umo, chain) is False:
            raise RuntimeError("未找到可发送消息的平台")

    async def _reply(
        self, event, text: str, *, umo: str | None = None, prefer_context: bool = False
    ) -> None:
        """发送纯文本回复。"""
        target = umo or (getattr(event, "unified_msg_origin", None) if event else None)
        await self._send(
            event, target, MessageChain().message(text), prefer_context=prefer_context
        )

    def _spawn_bg(self, coro, *, label: str) -> None:
        """启动后台任务并挂 done_callback, 防止异常被静默吞掉 / 任务被 GC。"""
        if self._terminating:
            coro.close()
            return
        task = asyncio.create_task(coro, name=f"group_chronicle:{label}")
        self._bg_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._bg_tasks.discard(t)
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                logger.info(f"[群史记] 后台任务已取消: {label}")
                return
            if exc is not None:
                logger.error(f"[群史记] 后台任务异常 ({label}): {exc}", exc_info=exc)

        task.add_done_callback(_done)

    async def _progress(self, umo: str, text: str) -> None:
        """发送进度短提示 (仅 context.send_message; 失败只记录, 绝不阻断流水线)。"""
        try:
            await self.context.send_message(umo, MessageChain().message(text))
        except Exception as e:  # noqa: BLE001 - 进度提示失败不影响生成
            logger.warning(f"[群史记] 进度提示发送失败: {e}")

    def _make_progress(self, umo: str):
        """构造进度回调; enable_progress=false 时返回 None (流水线不发进度)。"""
        if not self._cfg("enable_progress", True):
            return None
        return lambda text: self._progress(umo, text)

    # ------------------------------------------------------------------
    # 核心共享流水线 (群史记 / 史记周推 共用)
    # ------------------------------------------------------------------

    async def _run_locked(self, event, umo, operation, *, prefer_context=True):
        """Bound one operation and reject concurrent work for the same session.

        Args:
            event: Optional originating event.
            umo: Destination session.
            operation: Coroutine to execute under the session lock.
            prefer_context: Whether replies use proactive platform delivery.
        """
        if self._terminating:
            operation.close()
            return
        lock = self._gen_locks.setdefault(umo, asyncio.Lock())
        if lock.locked():
            operation.close()
            await self._reply(
                event, "正在生成中，请稍候", umo=umo, prefer_context=prefer_context
            )
            return
        timeout = float(self._cfg("generation_timeout_sec", 180))
        async with lock:
            try:
                await asyncio.wait_for(operation, timeout=timeout)
            except asyncio.TimeoutError:
                try:
                    await asyncio.wait_for(
                        self._reply(
                            event,
                            f"生成超时（超过 {timeout:g} 秒），已自动终止，请稍后重试",
                            umo=umo,
                            prefer_context=prefer_context,
                        ),
                        timeout=10,
                    )
                except Exception:
                    logger.warning("Failed to report generation timeout", exc_info=True)
            except (LLMUnavailableError, ValueError) as exc:
                try:
                    await asyncio.wait_for(
                        self._reply(
                            event, str(exc), umo=umo, prefer_context=prefer_context
                        ),
                        timeout=10,
                    )
                except Exception:
                    logger.warning(
                        "Failed to report history operation error", exc_info=True
                    )
            except Exception:
                logger.error("History operation failed", exc_info=True)
                try:
                    await asyncio.wait_for(
                        self._reply(
                            event,
                            "史记处理失败，请管理员检查日志后重试",
                            umo=umo,
                            prefer_context=prefer_context,
                        ),
                        timeout=10,
                    )
                except Exception:
                    logger.warning(
                        "Failed to report history operation failure", exc_info=True
                    )

    async def _run_group_analysis(
        self,
        event,
        days: int | None = None,
        include_bios: bool = True,
        *,
        umo: str | None = None,
        prefer_context: bool = True,
        force: bool = False,
        style: str | None = None,
        skip_check: bool = True,
        save: bool = True,
        topic: str | None = None,
    ) -> None:
        """生成群史记并发送 (自行回复结果)。

        并发控制: 每会话非阻塞锁 —— 生成中再次触发直接拒绝, 不加队列;
        锁内阻塞获取, 串行化并发竞态。

        prefer_context=True: 后台任务用 context.send_message 发结果
        (handler 返回后 event 常已失效)。

        force=True: 跳过 should_skip (本周已生成也强制重新生成)。
        style: 本次生成的文风覆盖 (风格= 命令参数), None 用配置默认。
        skip_check=False: 跳过 should_skip 检查 (群史记 月/年 使用)。
        save=False: 结果不写入 store (群史记 月/年 为临时视图)。
        topic: 本期主题 (投票选题获胜项), 追加到生成 prompt 要求围绕其展开。
        """
        umo = umo or (getattr(event, "unified_msg_origin", None) if event else None)
        if self._terminating:
            return
        if not umo:
            logger.error("[群史记] 无法获取会话标识, 中止生成")
            return
        logger.info(
            f"[群史记] 开始生成 umo={umo} days={days} bios={include_bios} force={force} style={style}"
        )
        await self._run_locked(
            event,
            umo,
            self._analyze(
                event,
                umo,
                days,
                include_bios,
                prefer_context=prefer_context,
                force=force,
                style=style,
                skip_check=skip_check,
                save=save,
                topic=topic,
            ),
            prefer_context=prefer_context,
        )

    async def _analyze(
        self,
        event,
        umo: str,
        days: int | None,
        include_bios: bool,
        *,
        prefer_context: bool = True,
        force: bool = False,
        style: str | None = None,
        skip_check: bool = True,
        save: bool = True,
        topic: str | None = None,
    ) -> None:
        """锁内生成主体: 采集 → 生成 → 渲染 → 保存 → 发送。"""
        group_name = "群聊"
        getter = getattr(event, "get_group_name", None) if event is not None else None
        if callable(getter):
            try:
                group_name = str(getter() or "群聊")
            except Exception:
                group_name = "群聊"

        timeout = float(self._cfg("generation_timeout_sec", 180))
        try:
            logger.info(f"[群史记] 采集证据中 umo={umo}")
            text, image_path = await asyncio.wait_for(
                self._build_entry(
                    umo,
                    days,
                    include_bios,
                    group_name,
                    force=force,
                    style=style,
                    skip_check=skip_check,
                    save=save,
                    progress=self._make_progress(umo),
                    topic=topic,
                ),
                timeout=timeout,
            )
            logger.info(
                f"[群史记] 生成完成 umo={umo} text_len={len(text or '')} "
                f"image={'yes' if image_path else 'no'}"
            )
        except asyncio.TimeoutError:
            logger.warning(f"[群史记] 生成超时 (>{timeout:.0f}s), 已终止 umo={umo}")
            await self._reply(
                event,
                f"生成超时（超过 {timeout:.0f} 秒），已自动终止，请稍后重试",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except NoActivityError as e:
            logger.info(f"[群史记] 无活动: {e}")
            await self._reply(event, str(e), umo=umo, prefer_context=prefer_context)
            return
        except _SkipWeek:
            logger.info(f"[群史记] 本周已生成, 跳过 umo={umo}")
            await self._reply(
                event,
                "本周史记已生成，暂无新消息（可用 史记查看 回看）",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except LLMUnavailableError as e:
            msg = self._llm_fail_msg(e, "未配置可用的 LLM，无法生成群史记")
            logger.warning(f"[群史记] LLM/LM 不可用: {msg}")
            await self._reply(event, msg, umo=umo, prefer_context=prefer_context)
            return
        except Exception as e:  # noqa: BLE001 - 任何异常都转化为用户可见的失败提示
            logger.error(f"[群史记] 生成失败 (umo={umo}): {e}", exc_info=True)
            try:
                await self._reply(
                    event, f"生成失败: {e}", umo=umo, prefer_context=prefer_context
                )
            except Exception as send_err:  # noqa: BLE001
                logger.error(
                    f"[群史记] 连失败提示也发不出去: {send_err}", exc_info=True
                )
            return

        try:
            ok = await deliver_text(
                self.context,
                event,
                umo,
                text,
                image_path,
                prefer_context=prefer_context,
                use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                guard=(lambda: self.history.publishable(umo, text))
                if getattr(self, "history", None) is not None
                and self._cfg("history_enabled", True)
                else None,
            )
            if ok:
                if (
                    save
                    or getattr(self, "history", None) is not None
                    and self._cfg("history_enabled", True)
                ):
                    try:
                        await asyncio.to_thread(self.store.mark_delivered, umo, text)
                    except Exception:
                        logger.error(
                            "Result delivered but delivery state could not be saved",
                            exc_info=True,
                        )
                logger.info(
                    f"[群史记] 结果已发送 umo={umo} len={len(text or '')} "
                    f"image={'yes' if image_path else 'no'}"
                )
            else:
                logger.error(f"[群史记] 发送结果失败 (umo={umo})")
        except Exception as e:  # noqa: BLE001
            logger.error(f"[群史记] 发送结果失败 (umo={umo}): {e}", exc_info=True)

    @staticmethod
    def _llm_fail_msg(exc: LLMUnavailableError, fallback: str) -> str:
        """区分"生成时 LLM 失败"(统一提示) 与"LivingMemory 未就绪"(透传具体原因)。"""
        msg = str(exc) or ""
        if msg == _LLM_UNAVAILABLE_MSG:
            return fallback
        return msg or fallback

    async def _build_entry(
        self,
        umo: str,
        days: int | None,
        include_bios: bool,
        group_name: str,
        *,
        force: bool = False,
        style: str | None = None,
        skip_check: bool = True,
        save: bool = True,
        progress=None,
        topic: str | None = None,
    ) -> tuple[str, str | None]:
        """采集证据并生成 (chronicle_text, image_path)。

        Args:
            umo: 会话标识。
            days: 时间窗天数覆盖 (None 用配置)。
            include_bios: 是否生成人物传记。
            group_name: 群名 (渲染/保存用)。
            force: True 时跳过 should_skip (强制重新生成)。
            style: 文风覆盖 (风格= 命令参数); None 用配置 chronicle_style。
            skip_check: False 时跳过 should_skip (群史记 月/年)。
            save: False 时不写入 store (群史记 月/年 为临时视图)。
            progress: 异步进度回调 (可选, 收到 str 提示文本)。
            topic: 本期主题 (投票选题获胜项), 追加到生成 prompt。

        Raises:
            NoActivityError: 时间窗内无足够活动。
            LLMUnavailableError: LivingMemory 未就绪或 LLM 生成失败。
            _SkipWeek: 本周已生成且无新消息。
        """
        history = getattr(self, "history", None)
        if history is not None and self._cfg("history_enabled", True):
            text, image_path, skipped = await history.build_report(
                umo,
                days,
                include_bios,
                group_name,
                force=force,
                style=style,
                skip_check=skip_check,
                topic=topic,
            )
            if skipped:
                raise _SkipWeek()
            if text.startswith("此时间范围尚无已入史的大事。"):
                raise NoActivityError(text)
            return text, image_path
        iso_year, iso_week = datetime.now().isocalendar()[:2]
        existing = await asyncio.to_thread(self.store.get, umo, iso_year, iso_week)
        if (
            save
            and skip_check
            and not force
            and existing
            and existing.get("delivery_complete") is False
        ):
            return str(existing.get("chronicle_text") or ""), existing.get("image_path")
        evidence, chronicle_days = await self._lm_and_gather(
            umo, days, progress=progress
        )
        if progress is not None:
            await progress(f"⏳ [2/5] 已采集 {evidence.msg_count} 条消息…")

        # 周键: 本周已生成且生成后没有新消息 → 跳过 (由调用方决定提示/静默)
        # force / skip_check=False 时绕过 (强制刷新 / 月·年视图)
        if (
            skip_check
            and not force
            and await asyncio.to_thread(
                self.store.should_skip, umo, iso_year, iso_week, evidence.newest_ts
            )
        ):
            raise _SkipWeek()

        # 人设 + 群史记正文 (支持自定义提示词 / 文风 / 两阶段 / 候选金句 /
        # 结构化输出 / 本周黑话 / 投票选题主题)
        sp = await resolve_system_prompt(self.context, self.config, umo)
        provider_id = self._llm_provider_id()
        style = style or str(self._cfg("chronicle_style", "official"))
        text = await generate_chronicle(
            self.context,
            umo,
            evidence,
            sp,
            chronicle_days,
            provider_id=provider_id,
            style=style,
            prompt_override=self._cfg("prompt_chronicle", "") or None,
            enable_two_phase=bool(self._cfg("enable_two_phase", False)),
            enable_golden_quotes=bool(self._cfg("enable_golden_quotes", True)),
            enable_structured=bool(self._cfg("enable_structured", False)),
            enable_slang=bool(self._cfg("enable_slang", True)),
            topic=topic,
        )
        if progress is not None:
            await progress("⏳ [3/5] 群史记正文已完成…")

        # 人物传记 (失败不中断整体生成; asyncio.gather + Semaphore(2) 并发,
        # 结果顺序与 top 一致)
        bios_texts: list[str] = []
        top: list[dict] = []
        if include_bios:
            top = await rank_influential(
                evidence,
                self.context,
                umo,
                top_k=int(self._cfg("top_n_bios", 3)),
                weights=self._influence_weights(),
            )
            total = max(1, len(top))
            sem = asyncio.Semaphore(2)
            done_count = 0

            async def _gen_bio(member: dict):
                nonlocal done_count
                name = (
                    member.get("sender_name") or member.get("sender_id") or "未知成员"
                )
                try:
                    bio = await generate_biography(
                        self.context,
                        umo,
                        evidence,
                        member,
                        sp,
                        int(self._cfg("biography_days", 30)),
                        provider_id=provider_id,
                        style=style,
                        prompt_override=self._cfg("prompt_biography", "") or None,
                    )
                except LLMUnavailableError:
                    bio = "（传记生成失败：未配置 LLM）"
                except Exception as e:  # noqa: BLE001 - 单人生传记失败不中断
                    logger.warning(f"生成成员 {name} 传记失败: {e}")
                    bio = "（传记生成失败）"
                done_count += 1
                if progress is not None:
                    await progress(f"⏳ [4/5] 人物传记 {done_count}/{total}…")
                return name, bio

            async def _limited(member: dict):
                async with sem:
                    return await _gen_bio(member)

            results = await asyncio.gather(*(_limited(m) for m in top))
            for member, (name, bio) in zip(top, results):
                bios_texts.append(bio)
                text += f"\n\n## 人物传记：{name}\n{bio}"

        # 群友图鉴 (enable_gallery 且存在活跃 Top-N 成员时; 失败静默跳过)
        if include_bios and top and self._cfg("enable_gallery", True):
            try:
                gallery = await build_gallery(
                    self.context, umo, evidence, top, provider_id=provider_id
                )
            except Exception as e:  # noqa: BLE001 - 图鉴失败不影响主流程
                logger.warning(f"生成群友图鉴失败: {e}")
                gallery = ""
            if gallery:
                text += "\n\n" + gallery

        # 本周关系弧 (RA 软来源可用时; 失败/缺失静默跳过)
        if self._cfg("enable_relation_arc", True):
            try:
                pairs = await relation_pairs(
                    self.context, umo, top_k=3, window_days=chronicle_days
                )
                if pairs:
                    text += "\n\n" + self._relation_arc_section(evidence, pairs)
            except Exception as e:  # noqa: BLE001 - 关系弧失败不影响主流程
                logger.warning(f"生成关系弧失败: {e}")

        # 覆盖范围头 (时间戳为 0 时省略) + LivingMemory 保留上限提示
        if evidence.coverage_start_ts and evidence.coverage_end_ts:
            header = (
                f"> 覆盖 {datetime.fromtimestamp(evidence.coverage_start_ts):%Y-%m-%d} ~ "
                f"{datetime.fromtimestamp(evidence.coverage_end_ts):%Y-%m-%d}，"
                f"{evidence.msg_count} 条消息"
            )
            max_msgs = int(self._cfg("max_msgs", 800))
            if evidence.msg_count >= int(max_msgs * 0.9):
                header += (
                    "（注：LivingMemory 仅保留最近 "
                    f"{max_msgs} 条消息，若群活跃，实际覆盖可能不足 "
                    f"{chronicle_days} 天）"
                )
            text = header + "\n\n" + text

        # 渲染 (失败自动降级纯文本; enable_render_cache 时复用相同内容)
        # 默认走 AstrBot 内置文转图 (T2I); playwright 为可选降级; 渲染异常绝不影响主流程
        try:
            render_mode = str(self._cfg("render_mode", "t2i")).strip() or "t2i"
            fallback_pw = bool(self._cfg("render_fallback_playwright", True))
            t2i_scale = str(self._cfg("t2i_scale", "device")).strip() or "device"
            text, image_path = await render_text_or_image(
                text,
                group_name,
                os.path.join(self.data_dir, "images"),
                render_mode=render_mode,
                html_render_func=getattr(self, "html_render", None),
                enable_cache=bool(self._cfg("enable_render_cache", True)),
                render_fallback_playwright=fallback_pw,
                options={"scale": t2i_scale},
            )
        except Exception as e:  # noqa: BLE001 - 渲染异常降级为纯文本, 不影响主流程
            logger.warning(f"渲染群史记图片失败，降级为纯文本: {e}")
            image_path = None
        if progress is not None:
            await progress("⏳ [5/5] 渲染完成，正在发送…")

        # 保存 (保存失败只记录, 不影响发送; 月/年视图不落盘)
        if save:
            try:
                await asyncio.to_thread(
                    self.store.save,
                    {
                        "umo": umo,
                        "group_name": group_name,
                        "iso_year": iso_year,
                        "iso_week": iso_week,
                        "created_at": time.time(),
                        "newest_msg_ts": evidence.newest_ts,
                        "chronicle_text": text,
                        "biographies": [
                            {
                                "sender_id": m["sender_id"],
                                "sender_name": m["sender_name"],
                                "text": bio,
                            }
                            for m, bio in zip(top, bios_texts)
                        ]
                        if include_bios
                        else [],
                        "image_path": image_path,
                        "msg_count": evidence.msg_count,
                        "version": 1,
                        "delivery_complete": False,
                        "biographies_embedded": True,
                    },
                )
            except Exception as e:  # noqa: BLE001 - 持久化失败不阻断本次发送
                logger.error(f"保存史记失败 (umo={umo}): {e}", exc_info=True)

        return text, image_path

    @staticmethod
    def _relation_arc_section(evidence, pairs: list) -> str:
        """组装「本周关系弧」分节 (模板法, 无需额外 LLM 调用)。

        pairs: [(a_id, b_id, score)] 由 core.influence.relation_pairs 返回。
        """
        names: dict[str, str] = {}
        for msgs in (
            getattr(evidence, "chronicle_msgs", None) or [],
            getattr(evidence, "biography_msgs", None) or [],
        ):
            for m in msgs:
                sid = str(getattr(m, "sender_id", "") or "")
                if not sid or sid in names:
                    continue
                nm = getattr(m, "sender_name", None)
                if nm:
                    names[sid] = str(nm)

        def _label(score: float) -> str:
            if score >= 3:
                return "紧密互动"
            if score >= 1:
                return "有来往"
            return "偶有交集"

        lines = ["## 本周关系弧"]
        for a, b, score in pairs[:3]:
            na = names.get(str(a), str(a))
            nb = names.get(str(b), str(b))
            lines.append(f"- {na} ⇄ {nb}（{_label(score)}）")
        return "\n".join(lines)

    def _influence_weights(self) -> dict[str, float]:
        """影响力融合权重 (来自配置, 缺省 0.5/0.3/0.2)。"""
        return {
            "lm": float(self._cfg("influence_w_lm", 0.5)),
            "sl": float(self._cfg("influence_w_sl", 0.3)),
            "ra": float(self._cfg("influence_w_ra", 0.2)),
        }

    def _llm_provider_id(self) -> str | None:
        """插件配置的 LLM Provider ID; 空字符串时只读继承 Chat Dynamics 默认回复模型。"""
        pid = str(self._cfg("llm_provider_id", "") or "").strip()
        if not pid:
            try:
                if __package__:
                    from .series_panels import inherit_provider
                else:
                    from series_panels import inherit_provider
                pid = inherit_provider("", getattr(self, "context", None), role="reply")
            except ImportError:
                pid = ""
        return pid or None

    async def _lm_and_gather(
        self, umo: str, days: int | None, *, progress=None
    ) -> tuple[Any, int]:
        """LivingMemory 可用性检查 + 证据收集。

        Args:
            umo: 会话标识。
            days: 时间窗天数覆盖 (None 用配置)。
            progress: 异步进度回调 (可选); 就绪后发送 [1/5] 提示。

        Returns:
            (evidence, 实际使用的 chronicle_days)。

        Raises:
            LLMUnavailableError: LivingMemory 未检测到 / 未初始化。
            NoActivityError: 时间窗内无足够活动或数据异常。
        """
        history = getattr(self, "history", None)
        if history is not None and await asyncio.to_thread(
            history.store.privacy_active, umo
        ):
            raise LLMUnavailableError(
                "本群已有史记隐私保护；原始聊天生成路径已暂停，请使用长期档案命令"
            )
        plugin = detect_lm(self.context)
        if plugin is None:
            reason = lm_unavailable_reason(self.context, "not_detected")
            logger.warning(f"[群史记] LivingMemory 未检测到: {reason}")
            raise LLMUnavailableError(reason)
        logger.info("[群史记] LivingMemory 已检测到, 等待初始化…")
        if not await ensure_ready(plugin):
            reason = lm_unavailable_reason(self.context, "not_ready")
            logger.warning(f"[群史记] LivingMemory 未就绪: {reason}")
            raise LLMUnavailableError(reason)
        if progress is not None:
            await progress("⏳ [1/5] 正在连接记忆…")

        engine = get_engine(plugin)
        conv_manager = get_conversation_manager(plugin)
        if engine is None or conv_manager is None:
            logger.error("[群史记] memory_engine 或 conversation_manager 为空")
            raise LLMUnavailableError("LivingMemory 核心组件未就绪")
        chronicle_days = int(self._cfg("chronicle_days", 7))
        if days is not None:
            chronicle_days = max(1, int(days))
        logger.info(
            f"[群史记] 拉取证据 umo={umo} days={chronicle_days} "
            f"max_msgs={self._cfg('max_msgs', 800)}"
        )
        evidence = await gather_evidence(
            engine,
            conv_manager,
            umo,
            chronicle_days,
            int(self._cfg("biography_days", 30)),
            int(self._cfg("max_msgs", 800)),
            int(self._cfg("max_memories", 80)),
            int(self._cfg("min_activity_messages", 1)),
        )
        logger.info(
            f"[群史记] 证据就绪 msgs={evidence.msg_count} "
            f"memories={len(evidence.memories or [])} newest_ts={evidence.newest_ts}"
        )
        return evidence, chronicle_days

    # ------------------------------------------------------------------
    # 命令处理器
    # ------------------------------------------------------------------

    @filter.command("群史记")
    async def chronicle_command(self, event: AstrMessageEvent, days: str | None = None):
        """生成群史记。

        参数 (空格分隔, 均可选):
        - 天数: `群史记 3` → 最近 3 天; `月` → 最近 30 天; `年` → 最近 365 天
        - 强制: 包含 `强制` 或 `--force` 时跳过"本周已生成"检查
        - 文风: `风格=八卦` / `风格 八卦` / `风格八卦` → 八卦风
          (正史/八卦/文言)
        """
        parsed = self._parse_chronicle_args(
            days, getattr(event, "message_str", None) or ""
        )
        if self._cfg("chronicle_admin_only", False) and not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        # 后台任务: 捕获 umo, 用 context.send_message 回传结果
        # (handler yield 后 event.send 常失效, 会导致"请稍候"后无下文)
        self._spawn_bg(
            self._run_group_analysis(
                event,
                parsed.days,
                include_bios=True,
                umo=umo,
                prefer_context=True,
                force=parsed.force,
                style=parsed.style,
                skip_check=parsed.skip_check,
                save=parsed.save,
            ),
            label=f"chronicle:{umo}",
        )
        yield event.plain_result("正在生成群史记，请稍候…")

    @filter.command("人物传")
    async def biography_command(self, event: AstrMessageEvent):
        """生成人物传记: 人物传 @成员 → 该成员; 人物传 → 活跃 Top-N。

        bio_admin_only=true (默认) 时仅管理员可调用。
        """
        if self._cfg("bio_admin_only", True) and not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        target = self._extract_at_sender(event)
        self._spawn_bg(
            self._bio_only(event, umo, target, prefer_context=True),
            label=f"bio:{umo}",
        )
        yield event.plain_result("正在生成人物传记，请稍候…")

    @filter.command("史记sid")
    async def chronicle_sid_command(self, event: AstrMessageEvent):
        """查看当前会话 UMO 标识 (用于配置 push_groups 定时推送)。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        yield event.plain_result(
            f"当前会话标识 (UMO): {umo}\n"
            f"提示: 将该值填入插件配置的 push_groups 即可对该群定时推送群史记。"
        )

    @filter.command("史记续写")
    async def chronicle_continue_command(self, event: AstrMessageEvent):
        """基于最新一条史记 + 新消息生成续篇 (权限同 群史记)。"""
        if self._cfg("chronicle_admin_only", False) and not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        self._spawn_bg(
            self._continue_run(event, umo, prefer_context=True),
            label=f"continue:{umo}",
        )
        yield event.plain_result("正在续写史记，请稍候…")

    @filter.command("史记导出")
    async def chronicle_export_command(
        self, event: AstrMessageEvent, idx: str | None = None
    ):
        """导出史记为 markdown 文件 (默认最新一条; 可选序号)。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        entry = await asyncio.to_thread(self.store.get_by_id, umo, self._parse_idx(idx))
        if entry is None:
            yield event.plain_result("未找到该条史记")
            return
        md = build_export_markdown(entry)
        out_dir = os.path.join(self.data_dir, "exports")
        try:
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"chronicle_{uuid.uuid4().hex}.md")
            with open(path, "w", encoding="utf-8") as f:
                f.write(md)
        except Exception as e:  # noqa: BLE001
            logger.error(f"导出史记失败: {e}", exc_info=True)
            yield event.plain_result(f"导出失败: {e}")
            return
        # 优先发送文件; 平台/API 不支持时回退分段文本 (含导出头)
        try:
            if not await asyncio.to_thread(self._entry_public, umo, entry):
                yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                return
            await self._send(
                event,
                umo,
                MessageChain(chain=[File(name=os.path.basename(path), file=path)]),
            )
            yield event.plain_result("已导出史记文件")
            return
        except Exception:
            logger.warning("File delivery failed; falling back to text", exc_info=True)
        for segment in split_text(
            f"📄 导出内容：\n\n{md}", int(self._cfg("max_reply_chars", 1500))
        ):
            if not await asyncio.to_thread(self._entry_public, umo, entry):
                yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                return
            yield event.plain_result(segment)

    @filter.command("史记对比")
    async def chronicle_compare_command(self, event: AstrMessageEvent):
        """对比最近两期史记的消息量与篇幅变化。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        entries = await asyncio.to_thread(self.store.list, umo)
        if len(entries) < 2:
            yield event.plain_result("至少需要两条史记才能对比")
            return
        latest, prev = entries[0], entries[1]
        latest_text = str(latest.get("chronicle_text") or "")
        prev_text = str(prev.get("chronicle_text") or "")
        mc_latest = int(latest.get("msg_count", 0) or 0)
        mc_prev = int(prev.get("msg_count", 0) or 0)

        def _line(e, text_len: int, mc: int) -> str:
            created = "?"
            try:
                if e.get("created_at"):
                    created = datetime.fromtimestamp(float(e["created_at"])).strftime(
                        "%Y-%m-%d"
                    )
            except Exception:  # noqa: BLE001
                created = "?"
            return (
                f"{e.get('iso_year')}-W{int(e.get('iso_week', 0) or 0):02d} "
                f"({created}): 消息 {mc} 条, 文本 {text_len} 字"
            )

        lines = ["📊 史记对比："]
        lines.append(f"- 最新: {_line(latest, len(latest_text), mc_latest)}")
        lines.append(f"- 上一期: {_line(prev, len(prev_text), mc_prev)}")
        if mc_latest or mc_prev:
            diff = mc_latest - mc_prev
            lines.append(f"- 消息量变化: {'+' if diff >= 0 else ''}{diff} 条")
        len_diff = len(latest_text) - len(prev_text)
        lines.append(f"- 文本量变化: {'+' if len_diff >= 0 else ''}{len_diff} 字")
        newest_l = float(latest.get("newest_msg_ts", 0) or 0)
        newest_p = float(prev.get("newest_msg_ts", 0) or 0)
        if newest_l and newest_p:
            lines.append(
                f"- 两次覆盖时间跨度: {abs(newest_l - newest_p) / 86400:.1f} 天"
            )
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # 投票选题 (史记选题 / 史记选票 / 史记选题公布)
    # ------------------------------------------------------------------

    @filter.command("史记选题")
    async def vote_topic_command(self, event: AstrMessageEvent):
        """发起本周选题投票: 生成 3 个候选选题, 群成员用 史记选票 N 投票。

        权限: 任意成员 (chronicle_admin_only=true 时仅管理员)。
        """
        if self._cfg("chronicle_admin_only", False) and not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        if not self._cfg("enable_vote", False):
            yield event.plain_result("投票选题功能未启用（enable_vote=false）")
            return
        self._spawn_bg(
            self._vote_propose_run(event, umo, prefer_context=True),
            label=f"vote_propose:{umo}",
        )
        yield event.plain_result("正在生成选题候选，请稍候…")

    @filter.command("史记选票")
    async def vote_cast_command(self, event: AstrMessageEvent, n: str | None = None):
        """投出本周选题票: `史记选票 N` (N=1-3), 每成员一票。

        权限: 任意成员。
        """
        if not self._cfg("enable_vote", False):
            yield event.plain_result("投票选题功能未启用（enable_vote=false）")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        try:
            idx = int(str(n or "").strip())
        except (TypeError, ValueError):
            yield event.plain_result("请回复「史记选票 N」，N 为 1-3")
            return
        if not (1 <= idx <= 3):
            yield event.plain_result("N 必须为 1-3")
            return
        voter = self._sender_id(event)
        result: str | None = None
        async with self._vote_lock:
            state = self._vote_state.get(umo)
            if state is None:
                result = "当前没有进行中的选题投票（可用 史记选题 发起）"
            elif time.time() > float(state.get("expires_at", 0) or 0):
                self._vote_state.pop(umo, None)
                result = "选题投票已过期，请重新发起 史记选题"
            elif voter and any(v.startswith(f"{voter}:") for v in state["voters"]):
                result = "你已经投过票了"
            elif idx > len(state["topics"]):
                result = f"N 必须为 1-{len(state['topics'])}"
            elif not voter:
                result = "无法识别投票者，请稍后重试"
            else:
                state["voters"].add(f"{voter}:{idx}")
                topic = state["topics"][idx - 1] if state["topics"] else str(idx)
                result = f"已投票：{topic}"
        yield event.plain_result(result)

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("史记选题公布")
    async def vote_announce_command(self, event: AstrMessageEvent):
        """公布投票结果并以获胜选题生成本周史记 (仅管理员)。"""
        if not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        if not self._cfg("enable_vote", False):
            yield event.plain_result("投票选题功能未启用（enable_vote=false）")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        topics: list = []
        voters: list = []
        cancel_reason: str | None = None
        async with self._vote_lock:
            state = self._vote_state.get(umo)
            if state is None:
                cancel_reason = "当前没有进行中的选题投票"
            elif time.time() > float(state.get("expires_at", 0) or 0):
                self._vote_state.pop(umo, None)
                cancel_reason = "选题投票已过期，请重新发起 史记选题"
            else:
                topics = list(state["topics"] or [])
                voters = list(state["voters"])
                self._vote_state.pop(umo, None)  # 投票结束, 清理状态
        if cancel_reason is not None:
            yield event.plain_result(cancel_reason)
            return

        counts = [0, 0, 0]
        for entry in voters:
            try:
                choice = int(str(entry).rsplit(":", 1)[-1])
            except (TypeError, ValueError):
                continue
            if 1 <= choice <= 3:
                counts[choice - 1] += 1

        lines = ["🎉 本期选题公布："]
        for i, t in enumerate(topics, 1):
            lines.append(f"{i}. {t}（{counts[i - 1]} 票）")
        if not voters:
            yield event.plain_result(
                "\n".join(lines) + "\n本期投票无人参与，已取消生成"
            )
            return
        winner_idx = max(range(len(topics)), key=lambda i: counts[i]) if topics else 0
        winner = topics[winner_idx] if topics else ""
        lines.append(f"🏆 获胜选题：{winner}（{counts[winner_idx]} 票）")
        yield event.plain_result("\n".join(lines))
        # 以获胜选题生成本周史记 (后台路径)
        self._spawn_bg(
            self._run_group_analysis(
                event,
                None,
                include_bios=True,
                umo=umo,
                prefer_context=True,
                topic=winner,
                force=True,
            ),
            label=f"vote_announce:{umo}",
        )

    async def _vote_propose_run(self, event, umo: str, *, prefer_context: bool = True):
        """Run this generation path with the shared session deadline."""
        await self._run_locked(
            event,
            umo,
            self._vote_propose_analyze(event, umo, prefer_context=prefer_context),
            prefer_context=prefer_context,
        )

    async def _vote_propose_analyze(
        self, event, umo: str, *, prefer_context: bool = True
    ) -> None:
        """选题后台主体: 采集证据 → 一次 LLM 生成 3 个候选 → 存入投票状态。"""
        try:
            evidence, _ = await self._lm_and_gather(umo, None)
        except NoActivityError as e:
            await self._reply(event, str(e), umo=umo, prefer_context=prefer_context)
            return
        except LLMUnavailableError as e:
            await self._reply(
                event,
                self._llm_fail_msg(e, "未配置可用的 LLM，无法生成选题"),
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"选题证据采集失败 (umo={umo}): {e}", exc_info=True)
            await self._reply(
                event, f"生成选题失败: {e}", umo=umo, prefer_context=prefer_context
            )
            return

        topics = await generate_topic_candidates(
            self.context, umo, evidence, provider_id=self._llm_provider_id()
        )
        if not topics:
            await self._reply(
                event,
                "选题生成失败（LLM 返回空），请稍后重试",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        async with self._vote_lock:
            self._vote_state[umo] = {
                "topics": topics,
                "expires_at": time.time() + 600,  # 10 分钟有效期
                "voters": set(),
            }
        lines = ["📮 本周选题候选："]
        lines += [f"{i}. {t}" for i, t in enumerate(topics, 1)]
        lines.append(f"回复「史记选票 N」（N=1-{len(topics)}）投票")
        await self._reply(
            event, "\n".join(lines), umo=umo, prefer_context=prefer_context
        )

    @staticmethod
    def _sender_id(event) -> str | None:
        """提取事件发送者 ID (多形态守卫: get_sender_id() → sender_id)。"""
        getter = getattr(event, "get_sender_id", None)
        if callable(getter):
            try:
                sid = getter()
                if sid:
                    return str(sid)
            except Exception:  # noqa: BLE001
                pass
        sid = getattr(event, "sender_id", None)
        if sid is not None:
            return str(sid)
        return None

    # ------------------------------------------------------------------
    # 史记网页 / 宇宙史记
    # ------------------------------------------------------------------

    @filter.command("史记网页")
    async def chronicle_webpage_command(
        self, event: AstrMessageEvent, idx: str | None = None
    ):
        """把史记导出为自包含 HTML 网页文件 (默认最新一条; 可选序号)。

        平台支持时发送文件, 否则回退本地路径文本。
        """
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        entry = await asyncio.to_thread(self.store.get_by_id, umo, self._parse_idx(idx))
        if entry is None:
            yield event.plain_result("未找到该条史记")
            return
        entry_text = str(entry.get("chronicle_text") or "")
        group_name = str(entry.get("group_name") or "群聊")
        content_html = markdown_to_html(entry_text) if entry_text else ""
        html_text = (
            "<!DOCTYPE html><html lang='zh-CN'><head><meta charset='utf-8'>"
            "<title>群史记</title>"
            "<style>body{font-family:'Noto Serif SC','Songti SC','SimSun',serif;"
            "background:#faf6ec;color:#3d3a34;padding:48px 64px;line-height:1.9;"
            "max-width:900px;margin:0 auto}"
            "h1{color:#9e2b25;letter-spacing:4px}ul{list-style:none;padding-left:1em}"
            "ul li::before{content:'◆';color:#9e2b25;margin-right:6px}"
            "pre{background:#f1e9d6;padding:12px;border-left:4px solid #9e2b25}</style>"
            f"</head><body><h1>{html.escape(group_name)}·群史记</h1>"
            f"{content_html}</body></html>"
        )
        out_dir = os.path.join(self.data_dir, "exports")
        try:
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"chronicle_{uuid.uuid4().hex}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(html_text)
        except Exception as e:  # noqa: BLE001
            logger.error(f"导出史记网页失败: {e}", exc_info=True)
            yield event.plain_result(f"网页导出失败: {e}")
            return
        # 优先发送文件; 平台/API 不支持时回退路径文本
        try:
            if not await asyncio.to_thread(self._entry_public, umo, entry):
                yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                return
            await self._send(
                event,
                umo,
                MessageChain(chain=[File(name=os.path.basename(path), file=path)]),
            )
            yield event.plain_result("已生成史记网页文件")
            return
        except Exception:
            logger.warning(
                "HTML file delivery failed; falling back to readable content",
                exc_info=True,
            )
        for segment in split_text(
            "网页文件发送失败，以下为史记内容：\n\n" + entry_text,
            int(self._cfg("max_reply_chars", 1500)),
        ):
            if not await asyncio.to_thread(self._entry_public, umo, entry):
                yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                return
            yield event.plain_result(segment)

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("宇宙史记")
    async def universe_command(self, event: AstrMessageEvent):
        """跨群「宇宙史记」: 综合 universe_groups 各群材料成文 (仅管理员)。"""
        if not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        groups = self._cfg("universe_groups", []) or []
        if not groups:
            yield event.plain_result("未配置宇宙史记群列表（universe_groups）")
            return
        self._spawn_bg(
            self._universe_run(event, umo, list(groups), prefer_context=True),
            label=f"universe:{umo}",
        )
        yield event.plain_result("正在生成宇宙史记，请稍候…")

    async def _universe_run(
        self, event, umo: str, groups: list, *, prefer_context: bool = True
    ):
        """Run this generation path with the shared session deadline."""
        await self._run_locked(
            event,
            umo,
            self._universe_analyze(event, umo, groups, prefer_context=prefer_context),
            prefer_context=prefer_context,
        )

    async def _universe_analyze(
        self, event, umo: str, groups: list, *, prefer_context: bool = True
    ) -> None:
        """宇宙史记主体: 逐群采集证据 (单群失败跳过), 一次 LLM 调用综合成文。"""
        group_evidences: list = []
        for g in groups:
            g = str(g).strip()
            if not g:
                continue
            try:
                evidence, _ = await self._lm_and_gather(g, None)
                group_evidences.append((g, evidence))
            except NoActivityError as e:
                logger.info(f"宇宙史记: 群 {g} 无活动, 跳过 ({e})")
            except LLMUnavailableError as e:
                logger.warning(f"宇宙史记: 群 {g} LLM 不可用, 跳过 ({e})")
            except Exception as e:  # noqa: BLE001 - 单群失败继续下一群
                logger.error(f"宇宙史记: 群 {g} 采集失败, 跳过: {e}", exc_info=True)
        if not group_evidences:
            await self._reply(
                event,
                "所有群均无可用数据，无法生成宇宙史记",
                umo=umo,
                prefer_context=prefer_context,
            )
            return

        timeout = float(self._cfg("generation_timeout_sec", 180))
        try:
            sp = await resolve_system_prompt(self.context, self.config, umo)
            text = await asyncio.wait_for(
                generate_universe_chronicle(
                    self.context,
                    umo,
                    group_evidences,
                    sp,
                    provider_id=self._llm_provider_id(),
                    style=str(self._cfg("chronicle_style", "official")),
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            await self._reply(
                event,
                f"生成超时（超过 {timeout:.0f} 秒），已自动终止，请稍后重试",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except LLMUnavailableError as e:
            await self._reply(
                event,
                self._llm_fail_msg(e, "未配置可用的 LLM，无法生成宇宙史记"),
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"宇宙史记生成失败 (umo={umo}): {e}", exc_info=True)
            await self._reply(
                event, f"生成失败: {e}", umo=umo, prefer_context=prefer_context
            )
            return

        await deliver_text(
            self.context,
            event,
            umo,
            text,
            None,
            prefer_context=prefer_context,
            use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
            max_chars=int(self._cfg("max_reply_chars", 1500)),
        )
        logger.info(f"[群史记] 宇宙史记已发送 umo={umo} len={len(text or '')}")

    @filter.command("影响力榜")
    async def influence_command(self, event: AstrMessageEvent):
        """查看本周影响力榜 (纯排行, 不调用 LLM 生成)。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        try:
            evidence, _ = await self._lm_and_gather(umo, None)
        except NoActivityError as e:
            yield event.plain_result(str(e))
            return
        except LLMUnavailableError as e:
            yield event.plain_result(
                self._llm_fail_msg(e, "未配置可用的 LLM，无法计算影响力")
            )
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"收集证据失败 (umo={umo}): {e}", exc_info=True)
            yield event.plain_result(f"生成失败: {e}")
            return

        ranking = await rank_influential(
            evidence, self.context, umo, top_k=10, weights=self._influence_weights()
        )
        if not ranking:
            yield event.plain_result("暂无足够数据计算影响力")
            return
        lines = [
            f"{i}. {r.get('sender_name') or r.get('sender_id') or '?'} "
            f"({r.get('score', 0)})"
            for i, r in enumerate(ranking, 1)
        ]
        yield event.plain_result("本周影响力榜：\n" + "\n".join(lines))

    @filter.command("史记列表")
    async def chronicle_list_command(self, event: AstrMessageEvent):
        """查看本群已生成的史记列表。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        entries = await asyncio.to_thread(self.store.list, umo)
        if not entries:
            yield event.plain_result("暂无史记")
            return
        lines = [
            f"{i}. {e.get('iso_year')}-W{int(e.get('iso_week', 0)):02d} "
            f"({datetime.fromtimestamp(e.get('created_at', 0)):%Y-%m-%d})"
            for i, e in enumerate(entries, 1)
        ]
        yield event.plain_result("史记列表：\n" + "\n".join(lines))

    @filter.command("史记查看")
    async def chronicle_view_command(
        self, event: AstrMessageEvent, idx: str | None = None
    ):
        """查看指定序号的史记正文 (默认最新一条)。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        entry = await asyncio.to_thread(self.store.get_by_id, umo, self._parse_idx(idx))
        if entry is None:
            yield event.plain_result("未找到该条史记")
            return
        text = entry.get("chronicle_text") or ""
        image_path = entry.get("image_path")
        if image_path and os.path.isfile(image_path):
            try:
                if not await asyncio.to_thread(self._entry_public, umo, entry):
                    yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                    return
                await self._send(event, umo, MessageChain().file_image(image_path))
                return
            except Exception:
                logger.warning("Stored image delivery failed; falling back to text")
        for segment in split_text(text, int(self._cfg("max_reply_chars", 1500))):
            if not await asyncio.to_thread(self._entry_public, umo, entry):
                yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                return
            yield event.plain_result(segment)

    @filter.permission_type(PermissionType.ADMIN)
    @filter.command("史记周推")
    async def weekly_manual_command(self, event: AstrMessageEvent):
        """手动触发本周史记推送 (仅管理员, 复用命令路径流水线)。"""
        if not self._is_admin(event):
            yield event.plain_result("无权限")
            return
        umo = getattr(event, "unified_msg_origin", None)
        self._spawn_bg(
            self._run_group_analysis(
                event, None, include_bios=True, umo=umo, prefer_context=True
            ),
            label=f"weekly_manual:{umo}",
        )
        yield event.plain_result("正在生成本周史记，请稍候…")

    # ------------------------------------------------------------------
    # 人物传 (仅传记) 路径
    # ------------------------------------------------------------------

    async def _bio_only(
        self,
        event,
        umo: str,
        target_sender_id: str | None,
        *,
        prefer_context: bool = True,
    ):
        """Run this generation path with the shared session deadline."""
        await self._run_locked(
            event,
            umo,
            self._bio_analyze(
                event, umo, target_sender_id, prefer_context=prefer_context
            ),
            prefer_context=prefer_context,
        )

    async def _bio_analyze(
        self,
        event,
        umo: str,
        target_sender_id: str | None,
        *,
        prefer_context: bool = True,
    ) -> None:
        """仅生成人物传记并发送 (人物传命令专用)。"""
        history = getattr(self, "history", None)
        if history is not None and self._cfg("history_enabled", True):
            if not await history.allowed(umo):
                await self._reply(
                    event,
                    "本群长期记录已停用或不在允许范围",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return

            def _load_bio_inputs():
                tgts = (
                    [target_sender_id]
                    if target_sender_id
                    else history.store.top(
                        umo,
                        time.time() - self._cfg("chronicle_days", 7) * 86400,
                        self._cfg("top_n_bios", 3),
                    )
                )
                return tgts, history.privacy_key(umo), history.store.revision(umo)

            targets, privacy, revision = await asyncio.to_thread(_load_bio_inputs)
            text = (
                "\n\n".join([await history.biography(umo, uid) for uid in targets])
                if targets
                else "本期暂无已入史的贡献事件；可以 @成员 查看其基础小记。"
            )

            async def still_public():
                if not await history.allowed(umo):
                    return False

                def _check_bio_state():
                    return (
                        history.privacy_key(umo) == privacy
                        and history.store.revision(umo) == revision
                    )

                return await asyncio.to_thread(_check_bio_state)

            await deliver_text(
                self.context,
                event,
                umo,
                text,
                None,
                prefer_context=prefer_context,
                use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                guard=still_public,
            )
            return
        logger.info(f"[群史记] 人物传开始 umo={umo} target={target_sender_id}")
        try:
            evidence, _ = await self._lm_and_gather(umo, None)
        except NoActivityError as e:
            await self._reply(event, str(e), umo=umo, prefer_context=prefer_context)
            return
        except LLMUnavailableError as e:
            await self._reply(
                event,
                self._llm_fail_msg(e, "未配置可用的 LLM，无法生成人物传记"),
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"收集证据失败 (umo={umo}): {e}", exc_info=True)
            await self._reply(
                event, f"生成失败: {e}", umo=umo, prefer_context=prefer_context
            )
            return

        if target_sender_id:
            member = self._find_member(evidence, target_sender_id)
            if member is None:
                await self._reply(
                    event,
                    "未找到该成员的发言记录",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return
            members = [member]
        else:
            members = await rank_influential(
                evidence,
                self.context,
                umo,
                top_k=int(self._cfg("top_n_bios", 3)),
                weights=self._influence_weights(),
            )
            if not members:
                await self._reply(
                    event,
                    "暂无成员数据可用于生成传记",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return

        sp = await resolve_system_prompt(self.context, self.config, umo)
        biography_days = int(self._cfg("biography_days", 30))
        provider_id = self._llm_provider_id()
        style = str(self._cfg("chronicle_style", "official"))
        timeout = float(self._cfg("generation_timeout_sec", 180))
        parts: list[str] = []
        for member in members:
            name = member.get("sender_name") or member.get("sender_id") or "未知成员"
            try:
                bio = await asyncio.wait_for(
                    generate_biography(
                        self.context,
                        umo,
                        evidence,
                        member,
                        sp,
                        biography_days,
                        provider_id=provider_id,
                        style=style,
                        prompt_override=self._cfg("prompt_biography", "") or None,
                    ),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(f"[群史记] 传记生成超时 (>{timeout:.0f}s) umo={umo}")
                await self._reply(
                    event,
                    f"生成超时（超过 {timeout:.0f} 秒），已自动终止，请稍后重试",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return
            except LLMUnavailableError:
                await self._reply(
                    event,
                    "未配置可用的 LLM，无法生成人物传记",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return
            except Exception as e:  # noqa: BLE001
                logger.warning(f"生成成员 {name} 传记失败: {e}")
                bio = "（传记生成失败）"
            parts.append(f"## 人物传记：{name}\n{bio}")
        await deliver_text(
            self.context,
            event,
            umo,
            "\n\n".join(parts),
            None,
            prefer_context=prefer_context,
            use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
            max_chars=int(self._cfg("max_reply_chars", 1500)),
        )
        logger.info(f"[群史记] 人物传已发送 umo={umo} members={len(parts)}")

    # ------------------------------------------------------------------
    # 史记续写 (基于最新一条史记 + 新消息)
    # ------------------------------------------------------------------

    async def _continue_run(
        self, event, umo: str, *, prefer_context: bool = True
    ) -> None:
        """续写入口: 与生成共用每会话锁 (锁内再阻塞获取)。"""
        await self._run_locked(
            event,
            umo,
            self._continue_analyze(event, umo, prefer_context=prefer_context),
            prefer_context=prefer_context,
        )

    async def _continue_analyze(
        self, event, umo: str, *, prefer_context: bool = True
    ) -> None:
        """续写主体: 取最新一条史记 → 采集新证据 → 生成续篇 → 发送。"""
        prev = await asyncio.to_thread(self.store.get_by_id, umo, 1)
        if prev is None:
            await self._reply(
                event,
                "暂无史记可续写（请先使用 群史记 生成一次）",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        history = getattr(self, "history", None)
        if history is not None and self._cfg("history_enabled", True):
            days = int(prev.get("period_days") or self._cfg("chronicle_days", 7))
            current = await asyncio.to_thread(
                history.store.events, umo, since=time.time() - days * 86400
            )
            old_versions = dict(prev.get("event_versions", []))
            if not current or not any(
                old_versions.get(e["id"]) != e["revision"] for e in current
            ):
                await self._reply(
                    event, "暂无新大事可续写", umo=umo, prefer_context=prefer_context
                )
                return
            text, image_path, skipped = await history.build_report(
                umo,
                prev.get("period_days"),
                True,
                str(prev.get("group_name") or "群聊"),
                skip_check=True,
            )
            if skipped:
                await self._reply(
                    event, "暂无新大事可续写", umo=umo, prefer_context=prefer_context
                )
                return
            ok = await deliver_text(
                self.context,
                event,
                umo,
                text,
                image_path,
                prefer_context=prefer_context,
                use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                guard=lambda: history.publishable(umo, text),
            )
            if ok:
                await asyncio.to_thread(self.store.mark_delivered, umo, text)
            return
        timeout = float(self._cfg("generation_timeout_sec", 180))
        try:
            evidence, chronicle_days = await asyncio.wait_for(
                self._lm_and_gather(umo, None), timeout=timeout
            )
        except asyncio.TimeoutError:
            await self._reply(
                event,
                f"生成超时（超过 {timeout:.0f} 秒），已自动终止，请稍后重试",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except NoActivityError as e:
            await self._reply(event, str(e), umo=umo, prefer_context=prefer_context)
            return
        except LLMUnavailableError as e:
            await self._reply(
                event,
                self._llm_fail_msg(e, "未配置可用的 LLM，无法续写史记"),
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"续写证据采集失败 (umo={umo}): {e}", exc_info=True)
            await self._reply(
                event, f"续写失败: {e}", umo=umo, prefer_context=prefer_context
            )
            return

        cutoff = float(prev.get("newest_msg_ts", 0) or 0)
        fresh = [
            m
            for m in evidence.chronicle_msgs
            if float(getattr(m, "timestamp", 0) or 0) > cutoff
        ]
        if not fresh:
            await self._reply(
                event, "暂无新消息可续写", umo=umo, prefer_context=prefer_context
            )
            return
        evidence = copy.copy(evidence)
        evidence.chronicle_msgs = fresh
        evidence.msg_count = len(fresh)
        evidence.newest_ts = max(float(m.timestamp) for m in fresh)
        sp = await resolve_system_prompt(self.context, self.config, umo)
        try:
            text = await asyncio.wait_for(
                generate_continuation(
                    self.context,
                    umo,
                    str(prev.get("chronicle_text") or ""),
                    evidence,
                    sp,
                    chronicle_days,
                    provider_id=self._llm_provider_id(),
                    style=str(self._cfg("chronicle_style", "official")),
                    prompt_override=self._cfg("prompt_chronicle", "") or None,
                    enable_golden_quotes=bool(self._cfg("enable_golden_quotes", True)),
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            await self._reply(
                event,
                f"生成超时（超过 {timeout:.0f} 秒），已自动终止，请稍后重试",
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except LLMUnavailableError as e:
            await self._reply(
                event,
                self._llm_fail_msg(e, "未配置可用的 LLM，无法续写史记"),
                umo=umo,
                prefer_context=prefer_context,
            )
            return
        except Exception as e:  # noqa: BLE001
            logger.error(f"续写生成失败 (umo={umo}): {e}", exc_info=True)
            await self._reply(
                event, f"续写失败: {e}", umo=umo, prefer_context=prefer_context
            )
            return

        delivered = await deliver_text(
            self.context,
            event,
            umo,
            text,
            None,
            prefer_context=prefer_context,
            use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
            max_chars=int(self._cfg("max_reply_chars", 1500)),
        )
        if delivered:
            entry = dict(prev)
            iso_year, iso_week = datetime.now().isocalendar()[:2]
            entry.update(
                {
                    "iso_year": iso_year,
                    "iso_week": iso_week,
                    "created_at": time.time(),
                    "chronicle_text": str(prev.get("chronicle_text") or "")
                    + "\n\n## 续篇\n"
                    + text,
                    "newest_msg_ts": evidence.newest_ts,
                    "image_path": None,
                    "msg_count": int(prev.get("msg_count", 0) or 0)
                    + evidence.msg_count,
                    "delivery_complete": True,
                }
            )
            await asyncio.to_thread(self.store.save, entry)
            logger.info("Continuation delivered and watermark advanced")
        else:
            logger.warning("Continuation delivery failed; watermark was not advanced")

    # ------------------------------------------------------------------
    # 每周定时推送
    # ------------------------------------------------------------------

    async def _weekly_push(self) -> None:
        """Track the scheduler callback so unload can cancel a running push."""
        if self._terminating:
            return
        task = asyncio.current_task()
        self._bg_tasks.add(task)
        try:
            await self._weekly_push_run()
        finally:
            self._bg_tasks.discard(task)

    async def _weekly_push_run(self) -> None:
        """每周定时推送: 遍历 push_groups 生成并推送, 单群失败不影响其他群。"""
        if self._terminating:
            return
        # 热加载: 推送配置变更时自动重注册定时任务 (守卫, 绝不崩溃)
        if self._cfg("enable_cron_reload", True):
            try:
                await self._maybe_reload_cron_job()
            except Exception as e:  # noqa: BLE001 - 重注册失败不影响本次推送
                logger.warning(f"每周推送配置热加载失败 (忽略): {e}")
        if not self._cfg("enable_weekly_push", False):
            return
        groups = self._cfg("push_groups", []) or []
        logger.info(f"每周史记推送开始, 目标群数: {len(groups)}")
        for umo in groups:
            if self._terminating:
                break
            try:
                await self._weekly_push_one(str(umo))
            except NoActivityError as e:
                logger.info(f"该群本周无活动，跳过: {umo} ({e})")
            except LLMUnavailableError as e:
                logger.warning(f"LLM 不可用，跳过推送: {umo} ({e})")
            except Exception as e:  # noqa: BLE001 - 单群失败继续下一群
                logger.error(f"推送群 {umo} 失败: {e}", exc_info=True)
        logger.info("每周史记推送结束")

    async def _weekly_push_one(self, umo: str) -> None:
        """单群推送: 与命令路径共用生成逻辑, 但跳过时静默、发送走 context。"""
        lock = self._gen_locks.setdefault(umo, asyncio.Lock())
        async with lock:
            started = time.monotonic()
            timeout = float(self._cfg("generation_timeout_sec", 180))
            try:
                text, image_path = await asyncio.wait_for(
                    self._build_entry(umo, None, True, "群聊"),
                    timeout=float(self._cfg("generation_timeout_sec", 180)),
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"群 {umo} 推送生成超时 (>{self._cfg('generation_timeout_sec', 180)}s), 跳过"
                )
                return
            except _SkipWeek:
                logger.info(f"群 {umo} 本周已生成且无新消息，跳过推送")
                return
            # NoActivityError / LLMUnavailableError / 其他异常向上传播, 由 _weekly_push 记录
            ok = await asyncio.wait_for(
                deliver_text(
                    self.context,
                    None,
                    umo,
                    text,
                    image_path,
                    prefer_context=True,
                    use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                    max_chars=int(self._cfg("max_reply_chars", 1500)),
                    guard=(lambda: self.history.publishable(umo, text))
                    if getattr(self, "history", None) is not None
                    and self._cfg("history_enabled", True)
                    else None,
                ),
                timeout=max(0.01, timeout - (time.monotonic() - started)),
            )
            if ok:
                try:
                    await asyncio.to_thread(self.store.mark_delivered, umo, text)
                except Exception:
                    logger.error(
                        "Weekly result delivered but delivery state could not be saved",
                        exc_info=True,
                    )
            else:
                logger.warning("Weekly delivery failed; saved result remains pending")

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def _entry_public(self, umo, entry):
        """Recheck fetched editions after privacy changes or corrections.

        Args:
            umo: Owning group.
            entry: Previously fetched edition.

        Returns:
            Whether it may still be delivered.
        """
        history = getattr(self, "history", None)
        if history is None:
            return True
        if entry.get("archive"):
            return history.store.public_report(
                umo, entry.get("report_id"), entry.get("chronicle_text")
            )
        return not history.store.privacy_active(umo)

    @filter.custom_filter(HistoryCaptureFilter)
    async def lifetime_capture_handler(self, event: AstrMessageEvent):
        """Provide a passive registration; the filter never activates it.

        Args:
            event: Incoming event.
        """
        return None

    @filter.command(COMMANDS[0], alias=set(COMMANDS[1:]))
    async def lifetime_command(self, event: AstrMessageEvent, args: str | None = None):
        """Dispatch scoped history, review, and personal privacy commands.

        Args:
            event: Invoking event.
            args: Optional framework argument.

        Yields:
            Length-bounded public-safe results.
        """
        async for result in command_reply(self, event, args):
            yield result

    @staticmethod
    def _parse_days(value: str | None) -> int | None:
        """解析命令天数参数; 非法/缺失返回 None (使用配置默认窗口)。"""
        if value is None:
            return None
        s = str(value).strip()
        if s.isdigit():
            return max(1, int(s))
        return None

    @staticmethod
    def _parse_chronicle_args(args, message_str: str = ""):
        """解析 群史记 参数 → (days, force, style, skip_check, save)。

        支持的写法 (与事件原文合并解析, 兼容多平台参数透传差异):
        - 天数: 纯数字 → days; `月` → 30 天且跳过周检查/不落盘; `年` → 365 天
        - 强制: 包含 `强制` 或 `--force` → force=True
        - 文风: `风格=八卦` / `风格 八卦` / `风格八卦` → style=gossip
          (正史/八卦/文言 归一化为 official/gossip/classical)

        Returns:
            SimpleNamespace(days, force, style, skip_check, save)
        """
        s = f"{args or ''} {message_str or ''}"
        force = ("强制" in s) or ("--force" in s.lower())

        style: str | None = None
        match = re.search(r"风格\s*[=＝]?\s*(\S+)", s)
        if match:
            raw = match.group(1)
            normalized = {
                "正史": "official",
                "八卦": "gossip",
                "文言": "classical",
            }.get(raw, raw)
            if normalized in ("official", "gossip", "classical"):
                style = normalized

        skip_check = True
        save = True
        days: int | None = None
        if "月" in s and not re.search(r"\d+\s*月", s):
            days, skip_check, save = 30, False, False
        elif "年" in s:
            days, skip_check, save = 365, False, False
        else:
            digit = re.search(r"\d+", s)
            if digit:
                days = max(1, int(digit.group(0)))

        return types.SimpleNamespace(
            days=days, force=force, style=style, skip_check=skip_check, save=save
        )

    @staticmethod
    def _parse_idx(value: str | None) -> int:
        """解析史记查看序号; 非法/缺失默认查看最新一条 (1)。"""
        if value is None:
            return 1
        s = str(value).strip()
        if s.isdigit():
            return max(1, int(s))
        return 1

    @staticmethod
    def _is_admin(event) -> bool:
        """事件发送者是否为管理员; 老版本事件无 is_admin 时放行 (由装饰器过滤兜底)。"""
        is_admin = getattr(event, "is_admin", None)
        if callable(is_admin):
            try:
                return bool(is_admin())
            except Exception:
                return False
        return False

    @staticmethod
    def _extract_at_sender(event) -> str | None:
        """解析第一个 @ 段落的 sender_id; 无 @ 返回 None。"""
        getter = getattr(event, "get_messages", None)
        if not callable(getter):
            return None
        try:
            segments = getter() or []
        except Exception:
            return None
        self_id_getter = getattr(event, "get_self_id", None)
        self_id = str(self_id_getter() or "") if callable(self_id_getter) else ""
        for seg in segments:
            if isinstance(seg, At):
                qq = getattr(seg, "qq", None)
                if qq is not None and str(qq) not in ("", "all", self_id):
                    return str(qq)
        return None

    @staticmethod
    def _find_member(evidence, sender_id: str) -> dict | None:
        """在证据消息中查找指定 sender_id 的成员 (优先 chronicle 窗口)。"""
        for msgs in (
            getattr(evidence, "chronicle_msgs", None) or [],
            getattr(evidence, "biography_msgs", None) or [],
        ):
            for m in msgs:
                sid = getattr(m, "sender_id", None)
                if sid is not None and str(sid) == str(sender_id):
                    name = str(getattr(m, "sender_name", "") or "") or str(sender_id)
                    return {"sender_id": str(sender_id), "sender_name": name}
        return None
