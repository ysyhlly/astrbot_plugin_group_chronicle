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
from datetime import datetime, timedelta, timezone
from importlib import import_module, reload
from typing import Any

from apscheduler.triggers.cron import CronTrigger
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.event.filter import PermissionType
from astrbot.api.message_components import At, File
from astrbot.api.star import Context, Star, StarTools

# AstrBot 以包方式加载插件，必须用相对导入 (.core.*)。
# 本地 pytest 把插件根目录塞进 sys.path 后以顶层模块 import main，
# 相对导入会失败，故回退到绝对 from core.*（仅测试环境）。
# 必须用 if __package__ 判断而不是 try/except ImportError：后者会把
# .core.* 内部真实的缺依赖 ImportError 一并吞掉并伪装成「顶层 import main」，
# 报错退化成 No module named 'core'，掩盖真正的故障。
if __package__:
    from .core.annual_report import build_annual_report, parse_annual_report_args
    from .core.biography import generate_biography
    from .core.blindbox import build_on_this_day, run_blindbox
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
    from .core.lifetime import GROUP_PATTERN, HistoryService, scope_lists
    from .core.llm_call import LLMUnavailableError
    from .core.lm_bridge import (
        detect_lm,
        ensure_ready,
        get_conversation_manager,
        get_engine,
        invalidate_lm_cache,
        lm_unavailable_reason,
    )
    from .core.meme_evolution import build_meme_evolution_card, generate_meme_remix
    from .core.persona import resolve_system_prompt
    from .core.render import (
        clear_render_cache,
        markdown_to_html,
        normalize_render_theme,
        render_text_or_image,
    )
    from .core.series_bridge import (
        ingest_archived_topic_event,
        publish_chronicle_milestone,
        subscribe_topic_archived,
        unsubscribe_event,
    )
    from .core.store import ChronicleStore
else:  # Top-level import is reserved for the test harness.
    from core.annual_report import build_annual_report, parse_annual_report_args
    from core.biography import generate_biography
    from core.blindbox import build_on_this_day, run_blindbox
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
    from core.lifetime import GROUP_PATTERN, HistoryService, scope_lists
    from core.llm_call import LLMUnavailableError
    from core.lm_bridge import (
        detect_lm,
        ensure_ready,
        get_conversation_manager,
        get_engine,
        invalidate_lm_cache,
        lm_unavailable_reason,
    )
    from core.meme_evolution import build_meme_evolution_card, generate_meme_remix
    from core.persona import resolve_system_prompt
    from core.render import (
        clear_render_cache,
        markdown_to_html,
        normalize_render_theme,
        render_text_or_image,
    )
    from core.series_bridge import (
        ingest_archived_topic_event,
        publish_chronicle_milestone,
        subscribe_topic_archived,
        unsubscribe_event,
    )
    from core.store import ChronicleStore

#: LLMUnavailableError 的固定消息 (core/llm_call.py), 用于区分"生成时 LLM 失败"与
#: "LivingMemory 未就绪" 两类不可用场景, 给出不同的用户提示。
_LLM_UNAVAILABLE_MSG = "未配置可用的 LLM"


class _SkipWeek(Exception):
    """内部控制流: 本周史记已生成且无新消息, 跳过本次生成 (不视为错误)。"""


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
        #: Wave 3 事件总线退订令牌 / 话题归档候选队列 / 活跃群史盲盒状态
        self._event_bus_tokens: list[str] = []
        self._archived_topic_cues: dict[str, list[dict[str, Any]]] = {}
        self._active_blindboxes: dict[str, dict[str, Any]] = {}
        self._wave3_keepsake_dedup: set[str] = set()
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

    def _group_name_readonly(self, umo: str) -> str:
        """Read stored group name without creating a row or opening a write txn."""
        try:
            with self.history.store.connection() as db:
                row = db.execute(
                    "SELECT name FROM groups WHERE umo=?", (umo,)
                ).fetchone()
            if row and row["name"]:
                return str(row["name"])
        except Exception:
            pass
        return umo

    def _series_panels_mod(self):
        name = f"{__package__}.series_panels" if __package__ else "series_panels"
        try:
            return import_module(name)
        except Exception:
            return None

    def _series_tz(self):
        mod = self._series_panels_mod()
        if mod is not None and callable(getattr(mod, "resolve_series_timezone", None)):
            return mod.resolve_series_timezone(getattr(self, "context", None))
        return timezone(timedelta(hours=8))

    def _is_excluded_by_core(self, umo: str) -> bool:
        clean = str(umo or "").strip()
        if not clean:
            return False
        mod = self._series_panels_mod()
        if mod is not None and callable(getattr(mod, "is_series_group_excluded", None)):
            return bool(
                mod.is_series_group_excluded(getattr(self, "context", None), umo=clean)
            )
        return False

    def _in_series_quiet_hours(self) -> bool:
        mod = self._series_panels_mod()
        if mod is not None and callable(getattr(mod, "is_series_in_quiet_hours", None)):
            return bool(mod.is_series_in_quiet_hours(getattr(self, "context", None)))
        return False

    def _series_summary_stamp(self, culture_enabled: bool, allowed_umos: list[str]) -> tuple:
        with self.history.store.connection() as db:
            c_row = db.execute(
                "SELECT COALESCE(SUM(revision), 0), COUNT(*) FROM culture_groups"
            ).fetchone()
            g_row = db.execute(
                "SELECT COALESCE(SUM(privacy_revision), 0), COUNT(*) FROM groups"
            ).fetchone()
        return (
            culture_enabled,
            tuple(allowed_umos),
            int(c_row[0] or 0) if c_row else 0,
            int(c_row[1] or 0) if c_row else 0,
            int(g_row[0] or 0) if g_row else 0,
            int(g_row[1] or 0) if g_row else 0,
        )

    def _compute_series_summary(self) -> dict[str, Any]:
        tz = self._series_tz()
        if (
            self._terminating
            or getattr(self, "history", None) is None
            or getattr(self, "culture", None) is None
        ):
            return {
                "badge": {"count": 0, "tone": "muted", "label": "未就绪"},
                "pulse": {"headline": "群文化沿革未就绪", "updated_at": ""},
            }
        culture_enabled = bool(self._cfg("culture_enabled", False))
        raw_groups = self.history.store.groups()
        allowed_umos = [
            umo for umo in raw_groups if self.history.enabled_for(umo)
        ]
        stamp = self._series_summary_stamp(culture_enabled, allowed_umos)
        cached = getattr(self, "_series_summary_cache", None)
        if cached is not None and cached[0] == stamp:
            return cached[1]

        pending_count = 0
        approved_count = 0
        latest_ts = 0.0
        if allowed_umos:
            placeholders = ",".join("?" for _ in allowed_umos)
            with self.history.store.connection() as db:
                # Identify groups with hidden members or source exclusions (only those need row-level JSON unpack)
                hidden_umos = {
                    r[0]
                    for r in db.execute(
                        f"SELECT DISTINCT umo FROM members WHERE umo IN ({placeholders}) AND hidden=1",
                        tuple(allowed_umos),
                    ).fetchall()
                }
                excl_umos = {
                    r[0]
                    for r in db.execute(
                        f"SELECT DISTINCT umo FROM culture_exclusions WHERE umo IN ({placeholders})",
                        tuple(allowed_umos),
                    ).fetchall()
                }
                ev_hide_umos = {
                    r[0]
                    for r in db.execute(
                        f"SELECT DISTINCT umo FROM hides WHERE umo IN ({placeholders})",
                        tuple(allowed_umos),
                    ).fetchall()
                }
                privacy_umos = hidden_umos | excl_umos | ev_hide_umos
                fast_umos = [u for u in allowed_umos if u not in privacy_umos]
                slow_umos = [u for u in allowed_umos if u in privacy_umos]

                if fast_umos:
                    fph = ",".join("?" for _ in fast_umos)
                    for st, cnt, mx in db.execute(
                        f"SELECT status, COUNT(*), COALESCE(MAX(updated), 0) "
                        f"FROM culture_entries WHERE umo IN ({fph}) "
                        "AND status IN ('pending', 'disputed', 'approved') "
                        "GROUP BY status",
                        tuple(fast_umos),
                    ).fetchall():
                        if float(mx or 0) > latest_ts:
                            latest_ts = float(mx or 0)
                        if st in ("pending", "disputed"):
                            pending_count += int(cnt or 0)
                        elif st == "approved":
                            approved_count += int(cnt or 0)

                slow_rows = []
                if slow_umos:
                    sph = ",".join("?" for _ in slow_umos)
                    slow_rows = db.execute(
                        f"SELECT * FROM culture_entries WHERE umo IN ({sph}) "
                        "AND status IN ('pending', 'disputed', 'approved')",
                        tuple(slow_umos),
                    ).fetchall()
            if slow_rows:
                by_umo: dict[str, list[dict[str, Any]]] = {}
                for row in slow_rows:
                    item = self.culture.store.unpack(row)
                    by_umo.setdefault(item["umo"], []).append(item)
                for umo, items in by_umo.items():
                    snapshot = self.culture._privacy_snapshot(
                        umo, {i.get("source") for i in items}
                    )
                    for item in items:
                        if self.culture._is_item_blocked(item, snapshot):
                            continue
                        updated = float(item.get("updated") or 0)
                        if updated > latest_ts:
                            latest_ts = updated
                        if item["status"] in ("pending", "disputed"):
                            pending_count += 1
                        elif item["status"] == "approved":
                            approved_count += 1
        updated_at = (
            datetime.fromtimestamp(latest_ts, tz).isoformat(timespec="seconds")
            if latest_ts > 0
            else ""
        )
        if pending_count > 0:
            badge = {
                "count": pending_count,
                "tone": "warm",
                "label": f"{pending_count} 条待核",
            }
            headline = (
                f"{pending_count} 条待审核/纠正 · 已公开 {approved_count} 条"
            )
        else:
            badge = {
                "count": 0,
                "tone": "accent",
                "label": (
                    f"已收录 {approved_count} 条"
                    if approved_count > 0
                    else "无待核"
                ),
            }
            if approved_count > 0:
                headline = f"已公开 {approved_count} 条文化词条 · 覆盖 {len(allowed_umos)} 个群"
            elif not culture_enabled:
                headline = "群文化整理未开启 · 可在插件配置中启用"
            else:
                headline = f"已就绪 · 监听 {len(allowed_umos)} 个群聊"
        result = {
            "badge": badge,
            "pulse": {"headline": headline, "updated_at": updated_at},
        }
        self._series_summary_cache = (stamp, result)
        return result

    def get_series_summary(self) -> dict[str, Any]:
        """Strictly synchronous, read-only summary for the v2 series status matrix."""
        try:
            return self._compute_series_summary()
        except Exception:
            return {
                "badge": {"count": 0, "tone": "muted", "label": "暂不可用"},
                "pulse": {"headline": "群文化状态暂不可用", "updated_at": ""},
            }

    async def get_series_summary_async(self) -> dict[str, Any]:
        """Offload summary computation to a worker thread for async callers."""
        return await asyncio.to_thread(self.get_series_summary)

    async def get_series_health_async(self) -> dict[str, Any]:
        """Offload health diagnostics to a worker thread for async callers."""
        return await asyncio.to_thread(self.get_series_health)

    def get_series_health(self) -> dict[str, Any]:
        """Strictly synchronous, read-only health diagnostics for series control panel."""
        providers: list[dict[str, Any]] = []
        issues: list[str] = []
        metrics: dict[str, Any] = {}
        history = getattr(self, "history", None)
        culture = getattr(self, "culture", None)
        ready = bool(not self._terminating and history is not None)
        if self._terminating:
            issues.append("群史记正在停止或已关闭")
        elif history is None:
            issues.append("长期历史服务尚未就绪")

        ctx = getattr(self, "context", None)
        mod = self._series_panels_mod()
        own_provider = str(self._cfg("llm_provider_id", "") or "").strip()
        resolved_provider = self._llm_provider_id()
        if not resolved_provider:
            providers.append(
                {
                    "role": "chronicle_llm",
                    "provider_id": "",
                    "status": "session_default",
                    "detail": "跟随会话默认模型",
                }
            )
        else:
            finder = (
                getattr(mod, "find_provider_silently", None)
                if mod is not None
                else None
            )
            prov = finder(ctx, resolved_provider) if callable(finder) else None
            if prov is not None:
                providers.append(
                    {
                        "role": "chronicle_llm",
                        "provider_id": resolved_provider,
                        "status": "ok",
                        "detail": (
                            "显式配置可用" if own_provider else "继承主控回复模型可用"
                        ),
                    }
                )
            else:
                providers.append(
                    {
                        "role": "chronicle_llm",
                        "provider_id": resolved_provider,
                        "status": "missing",
                        "detail": "未在已加载 Provider 中找到（可能冷启动或已移除）",
                    }
                )
                issues.append(f"chronicle_llm 配置的 Provider 不可用: {resolved_provider}")

        memory_detected = False
        memory_ready = False
        try:
            lm_inst = detect_lm(ctx)
            if lm_inst is not None:
                memory_detected = True
                caps_fn = getattr(lm_inst, "get_dynamics_capabilities", None)
                if callable(caps_fn):
                    caps = caps_fn()
                    memory_ready = bool(
                        isinstance(caps, dict) and caps.get("ready") is True
                    )
                else:
                    init = getattr(lm_inst, "initializer", None)
                    memory_ready = bool(getattr(init, "is_initialized", False))
        except RuntimeError as exc:
            issues.append(str(exc)[:160])
        except Exception:
            pass
        metrics["memory_detected"] = memory_detected
        metrics["memory_ready"] = memory_ready
        metrics["history_enabled"] = bool(self._cfg("history_enabled", True))
        metrics["culture_enabled"] = bool(self._cfg("culture_enabled", False))
        try:
            if history is not None and not self._terminating:
                metrics["tracked_groups"] = len(history.store.groups())
            if culture is not None and not self._terminating:
                metrics["culture_ready"] = True
        except Exception:
            pass

        return {
            "ready": ready,
            "degraded": bool(issues) or not ready,
            "providers": providers,
            "issues": issues,
            "metrics": metrics,
        }

    async def get_series_scope_status(self, umo: str) -> dict[str, Any]:
        """Read-only per-group status for the Chat Dynamics control matrix."""
        tz = self._series_tz()
        try:
            clean_umo = str(umo or "").strip()
            persona_id = str(self._cfg("persona_id", "") or "")
            if (
                not clean_umo
                or self._terminating
                or getattr(self, "history", None) is None
                or getattr(self, "culture", None) is None
            ):
                return {
                    "umo": clean_umo,
                    "enabled": False,
                    "supported": False,
                    "writable": False,
                    "mode": "unavailable",
                    "persona_id": persona_id,
                    "activity_today": 0,
                    "summary": "群文化服务未就绪",
                    "detail": "群文化服务未就绪",
                    "version": 0,
                }
            history_allowed = await self.history.allowed(clean_umo)
            culture_global = bool(self._cfg("culture_enabled", False))
            today = datetime.now(tz).date()
            day_start = datetime(
                today.year, today.month, today.day, tzinfo=tz
            ).timestamp()

            def _read_scope():
                state = self.culture.store.state(clean_umo)
                history_paused = self.history.store.is_group_paused(clean_umo)
                entries, _ = self.culture.store.entries(clean_umo, limit=1000)
                snapshot = (
                    self.culture._privacy_snapshot(
                        clean_umo, {i.get("source") for i in entries}
                    )
                    if entries
                    else None
                )
                approved = 0
                pending = 0
                activity_today = 0
                for item in entries:
                    if item["status"] not in ("approved", "pending", "disputed"):
                        continue
                    if snapshot is not None and self.culture._is_item_blocked(
                        item, snapshot
                    ):
                        continue
                    if item["status"] == "approved":
                        approved += 1
                    else:
                        pending += 1
                    if float(item.get("updated") or 0) >= day_start:
                        activity_today += 1
                with self.history.store.connection() as db:
                    row = db.execute(
                        "SELECT COUNT(*) FROM reports WHERE umo=? AND invalid=0 AND created>=?",
                        (clean_umo, day_start),
                    ).fetchone()
                    if row:
                        activity_today += int(row[0] or 0)
                return state, history_paused, approved, pending, activity_today

            (
                state,
                history_paused,
                approved,
                pending,
                activity_today,
            ) = await asyncio.to_thread(_read_scope)
            paused = bool(state.get("paused", 0))
            version = int(state.get("revision", 0))
            if self._is_excluded_by_core(clean_umo):
                summary = "主控排除（群文化与史记采集已静默跳过）"
                return {
                    "umo": clean_umo,
                    "enabled": False,
                    "supported": False,
                    "writable": False,
                    "mode": "excluded_by_core",
                    "excluded_by_core": True,
                    "persona_id": persona_id,
                    "activity_today": activity_today,
                    "summary": summary,
                    "detail": summary,
                    "version": version,
                }
            if not history_allowed:
                summary = "不在允许群范围"
                return {
                    "umo": clean_umo,
                    "enabled": False,
                    "supported": False,
                    "writable": False,
                    "mode": "out_of_scope",
                    "excluded_by_core": False,
                    "persona_id": persona_id,
                    "activity_today": activity_today,
                    "summary": summary,
                    "detail": summary,
                    "version": version,
                }
            if not culture_global:
                summary = "全局群文化整理未开启"
                return {
                    "umo": clean_umo,
                    "enabled": False,
                    "supported": False,
                    "writable": False,
                    "mode": "culture_disabled",
                    "persona_id": persona_id,
                    "activity_today": activity_today,
                    "summary": summary,
                    "detail": summary,
                    "version": version,
                }
            enabled = not (paused or history_paused)
            summary = (
                f"已暂停整理 · 已公开 {approved} / 待核 {pending}"
                if not enabled
                else f"运行中 · 已公开 {approved} / 待核 {pending}"
            )
            return {
                "umo": clean_umo,
                "enabled": enabled,
                "supported": True,
                "writable": True,
                "mode": "history+culture",
                "history_paused": history_paused,
                "culture_paused": paused,
                "persona_id": persona_id,
                "activity_today": activity_today,
                "summary": summary,
                "detail": summary,
                "version": version,
            }
        except Exception:
            return {
                "umo": str(umo or "").strip(),
                "enabled": None,
                "supported": False,
                "writable": False,
                "mode": "error",
                "persona_id": "",
                "activity_today": None,
                "summary": "状态读取失败",
                "detail": "状态读取失败",
                "version": 0,
            }

    async def set_series_scope_enabled(
        self, umo: str, enabled: bool, expected_version: Any = None
    ) -> dict[str, Any]:
        """Atomically toggle per-group lifetime history & culture pause/resume with optimistic locking."""
        try:
            clean_umo = str(umo or "").strip()
            if (
                not clean_umo
                or self._terminating
                or getattr(self, "history", None) is None
                or getattr(self, "culture", None) is None
            ):
                return {
                    "ok": False,
                    "umo": clean_umo,
                    "enabled": False,
                    "version": 0,
                    "reason": "not_ready",
                    "summary": "群文化服务未就绪",
                }
            if self._is_excluded_by_core(clean_umo):
                state = await asyncio.to_thread(self.culture.store.state, clean_umo)
                return {
                    "ok": False,
                    "umo": clean_umo,
                    "enabled": False,
                    "excluded_by_core": True,
                    "version": int(state.get("revision", 0)),
                    "reason": "excluded_by_core",
                    "summary": "主控排除（当前群已被主控排除）",
                }
            if not await self.history.allowed(clean_umo) or not bool(
                self._cfg("culture_enabled", False)
            ):
                state = await asyncio.to_thread(self.culture.store.state, clean_umo)
                return {
                    "ok": False,
                    "umo": clean_umo,
                    "enabled": False,
                    "version": int(state.get("revision", 0)),
                    "reason": "scope_or_culture_disabled",
                    "summary": "当前群不在允许范围或全局群文化未启用",
                }
            target_paused = 0 if bool(enabled) else 1

            def _toggle():
                with self.history.store.connection(write=True) as db:
                    row = db.execute(
                        "SELECT revision, paused FROM culture_groups WHERE umo=?",
                        (clean_umo,),
                    ).fetchone()
                    g_row = db.execute(
                        "SELECT paused FROM groups WHERE umo=?",
                        (clean_umo,),
                    ).fetchone()
                    cur_rev = int(row["revision"]) if row else 0
                    cur_paused = bool(row["paused"]) if row else False
                    cur_hist_paused = bool(g_row["paused"]) if g_row else False
                    if expected_version is not None and str(cur_rev) != str(
                        expected_version
                    ):
                        return (
                            False,
                            True,
                            not (cur_paused or cur_hist_paused),
                            cur_rev,
                        )
                    self.culture.store.bump(db, clean_umo)
                    db.execute(
                        "UPDATE culture_groups SET paused=? WHERE umo=?",
                        (target_paused, clean_umo),
                    )
                    db.execute(
                        "INSERT OR IGNORE INTO groups(umo) VALUES(?)",
                        (clean_umo,),
                    )
                    db.execute(
                        "UPDATE groups SET paused=? WHERE umo=?",
                        (target_paused, clean_umo),
                    )
                    self.history.store._bump(db, clean_umo)
                    after = db.execute(
                        "SELECT revision, paused FROM culture_groups WHERE umo=?",
                        (clean_umo,),
                    ).fetchone()
                    return (
                        True,
                        False,
                        not bool(after["paused"]),
                        int(after["revision"]),
                    )

            ok, conflict, now_enabled, new_rev = await asyncio.to_thread(_toggle)
            if conflict:
                return {
                    "ok": False,
                    "conflict": True,
                    "reason": "version_conflict",
                    "umo": clean_umo,
                    "enabled": now_enabled,
                    "version": new_rev,
                    "summary": "状态已变更，请刷新重试",
                }
            self.history.cancel_operations(clean_umo)
            try:
                mod_name = (
                    f"{__package__}.series_panels" if __package__ else "series_panels"
                )
                import_module(mod_name).invalidate_summary_cache(
                    "astrbot_plugin_group_chronicle"
                )
            except Exception:
                pass
            return {
                "ok": ok,
                "umo": clean_umo,
                "enabled": now_enabled,
                "history_paused": not now_enabled,
                "culture_paused": not now_enabled,
                "version": new_rev,
                "reason": "",
                "summary": "运行中" if now_enabled else "已暂停整理",
            }
        except Exception:
            return {
                "ok": False,
                "umo": str(umo or "").strip(),
                "enabled": False,
                "version": 0,
                "reason": "toggle_failed",
                "summary": "切换群文化状态失败",
            }

    def activate_series_runtime(self) -> dict[str, Any]:
        """Enable lifetime history & culture runtime gates for Setup Wizard step 4."""
        if self._terminating or getattr(self, "history", None) is None:
            return {
                "ok": False,
                "plugin_id": "astrbot_plugin_group_chronicle",
                "ready": False,
                "history_enabled": False,
                "culture_enabled": False,
                "changed": False,
                "reason": "not_ready",
            }
        changed = False
        try:
            if not bool(self._cfg("history_enabled", True)):
                self.config["history_enabled"] = True
                changed = True
            if not bool(self._cfg("culture_enabled", False)):
                self.config["culture_enabled"] = True
                changed = True
            save_fn = getattr(self.config, "save_config", None)
            if changed and callable(save_fn):
                save_fn()
        except Exception as exc:
            logger.warning("activate_series_runtime failed to persist config: %s", exc)
            return {
                "ok": False,
                "plugin_id": "astrbot_plugin_group_chronicle",
                "ready": True,
                "history_enabled": bool(self._cfg("history_enabled", True)),
                "culture_enabled": bool(self._cfg("culture_enabled", False)),
                "changed": changed,
                "reason": f"config_error:{type(exc).__name__}",
            }
        try:
            mod = self._series_panels_mod()
            if mod is not None and callable(
                getattr(mod, "invalidate_summary_cache", None)
            ):
                mod.invalidate_summary_cache("astrbot_plugin_group_chronicle")
        except Exception:
            pass
        try:
            asyncio.get_running_loop()
            self._spawn_bg(self.history.run_cycle(), label="series-runtime-activate")
        except RuntimeError:
            pass
        return {
            "ok": True,
            "plugin_id": "astrbot_plugin_group_chronicle",
            "ready": True,
            "history_enabled": bool(self._cfg("history_enabled", True)),
            "culture_enabled": bool(self._cfg("culture_enabled", False)),
            "changed": changed,
            "reason": "",
        }

    async def series_search_items(
        self, query: str, scope: str = "", limit: int = 8
    ) -> list[dict[str, Any]]:
        """Read-only search over non-hidden/non-blocked culture entries for Ctrl+K."""
        try:
            if (
                self._terminating
                or getattr(self, "history", None) is None
                or getattr(self, "culture", None) is None
            ):
                return []
            q = str(query or "").strip()[:160]
            if not q:
                return []
            bound = max(1, min(int(limit or 8), 20))
            clean_scope = str(scope or "").strip()
            if clean_scope:
                umos = [clean_scope] if await self.history.allowed(clean_scope) else []
            else:
                raw_groups = await asyncio.to_thread(self.history.store.groups)
                umos = [u for u in raw_groups if await self.history.allowed(u)]
            if not umos:
                return []
            status_labels = {
                "approved": "已公开",
                "pending": "待审核",
                "disputed": "待纠正",
            }

            def _load_scope_candidates(umo: str):
                token = self.culture.token(umo)
                items, _ = self.culture.store.entries(umo, query=q, limit=bound * 2)
                if not items:
                    return token, "", []
                group_name = self._group_name_readonly(umo)
                snapshot = self.culture._privacy_snapshot(
                    umo, {i.get("source") for i in items}
                )
                candidates = [
                    item
                    for item in items
                    if item["status"] in ("approved", "pending", "disputed")
                    and not self.culture._is_item_blocked(item, snapshot)
                ]
                return token, group_name, candidates

            hits: list[tuple[float, dict[str, Any]]] = []
            for umo in umos:
                token, group_name, candidates = await asyncio.to_thread(
                    _load_scope_candidates, umo
                )
                if not candidates:
                    continue
                flags = await self.culture._batch_valid_sources(umo, candidates)
                if (
                    not await self.history.allowed(umo)
                    or await asyncio.to_thread(self.culture.token, umo) != token
                ):
                    continue
                for item, is_valid in zip(candidates, flags):
                    if not is_valid:
                        continue
                    status_label = status_labels.get(item["status"], item["status"])
                    meaning = str(item.get("meaning") or "").strip()[:80]
                    hits.append(
                        (
                            float(item.get("updated") or 0),
                            {
                                "kind": "culture_entry",
                                "title": f"#{item['id']} {item['name']}",
                                "subtitle": f"{group_name} · {status_label} · {meaning}",
                                "plugin_id": "astrbot_plugin_group_chronicle",
                                "page": "culture",
                                "deep_link": {
                                    "scope": umo,
                                    "tab": (
                                        "review"
                                        if item["status"] in ("pending", "disputed")
                                        else "library"
                                    ),
                                    "entry_id": item["id"],
                                },
                            },
                        )
                    )
            hits.sort(key=lambda pair: pair[0], reverse=True)
            return [entry for _, entry in hits[:bound]]
        except Exception:
            return []

    async def get_journal_events(
        self,
        persona: str = "",
        scope: str = "",
        date: str = "today",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Read-only daily culture & chronicle edition events for World's Living Journal."""
        tz = self._series_tz()
        try:
            if (
                self._terminating
                or getattr(self, "history", None) is None
                or getattr(self, "culture", None) is None
            ):
                return []
            bound = max(1, min(int(limit or 20), 50))
            raw_date = str(date or "today").strip()
            if not raw_date or raw_date == "today":
                target_day = datetime.now(tz).date()
            else:
                try:
                    target_day = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
                except ValueError:
                    target_day = datetime.now(tz).date()
            day_start = datetime(
                target_day.year, target_day.month, target_day.day, tzinfo=tz
            ).timestamp()
            day_end = day_start + 86400.0
            clean_scope = str(scope or "").strip()
            if clean_scope:
                umos = [clean_scope] if await self.history.allowed(clean_scope) else []
            else:
                raw_groups = await asyncio.to_thread(self.history.store.groups)
                umos = [u for u in raw_groups if await self.history.allowed(u)]
            if not umos:
                return []

            def _load_scope_journal_inputs(umo: str):
                token = self.culture.token(umo)
                group_name = self._group_name_readonly(umo)
                items, _ = self.culture.store.entries(umo, limit=200)
                candidates: list[dict[str, Any]] = []
                if items:
                    snapshot = self.culture._privacy_snapshot(
                        umo, {i.get("source") for i in items}
                    )
                    for item in items:
                        if item["status"] not in (
                            "approved",
                            "pending",
                            "disputed",
                        ):
                            continue
                        ts = float(item.get("updated") or 0)
                        if not (day_start <= ts < day_end):
                            continue
                        if self.culture._is_item_blocked(item, snapshot):
                            continue
                        candidates.append(item)
                try:
                    editions = self.history.editions.list(umo)
                except Exception:
                    editions = []
                return token, group_name, candidates, editions

            events: list[tuple[float, dict[str, Any]]] = []
            for umo in umos:
                token, group_name, candidates, editions = await asyncio.to_thread(
                    _load_scope_journal_inputs, umo
                )
                if candidates:
                    flags = await self.culture._batch_valid_sources(umo, candidates)
                    if (
                        await self.history.allowed(umo)
                        and await asyncio.to_thread(self.culture.token, umo) == token
                    ):
                        for item, is_valid in zip(candidates, flags):
                            if not is_valid:
                                continue
                            ts = float(item.get("updated") or 0)
                            is_pending = item["status"] in ("pending", "disputed")
                            events.append(
                                (
                                    ts,
                                    {
                                        "id": f"culture:{umo}:{item['id']}:r{item['revision']}",
                                        "category": "group_chronicle",
                                        "source_label": "群文化沿革",
                                        "tone": "warm" if is_pending else "accent",
                                        "occurred_at": datetime.fromtimestamp(
                                            ts, tz
                                        ).isoformat(timespec="seconds"),
                                        "title": (
                                            f"待核群文化「{item['name']}」"
                                            if is_pending
                                            else f"收录群文化「{item['name']}」"
                                        ),
                                        "subtitle": f"{group_name} · {str(item.get('meaning') or '')[:90]}",
                                        "target_plugin": "astrbot_plugin_group_chronicle",
                                        "target_route": "culture",
                                        "target_id": str(item["id"]),
                                    },
                                )
                            )
                if not await self.history.allowed(umo):
                    continue
                for ed in editions:
                    ts = float(ed.get("created_at") or 0)
                    if not (day_start <= ts < day_end):
                        continue
                    ed_id = str(
                        ed.get("report_id") or ed.get("cache_key") or int(ts)
                    )
                    ed_title = str(ed.get("title") or "").strip()
                    raw_text = str(
                        ed.get("summary") or ed.get("text") or ""
                    ).strip()
                    preview = " ".join(raw_text.split())[:90]
                    events.append(
                        (
                            ts,
                            {
                                "id": f"edition:{umo}:{ed_id}",
                                "category": "group_chronicle",
                                "source_label": "群文化沿革",
                                "tone": "info",
                                "occurred_at": datetime.fromtimestamp(
                                    ts, tz
                                ).isoformat(timespec="seconds"),
                                "title": (
                                    f"群史记 · {ed_title}"
                                    if ed_title
                                    else f"生成群史记 · {group_name}"
                                ),
                                "subtitle": preview or f"{group_name} 群史记已归档",
                                "target_plugin": "astrbot_plugin_group_chronicle",
                                "target_route": "culture",
                                "target_id": ed_id,
                            },
                        )
                    )
            events.sort(key=lambda pair: pair[0], reverse=True)
            return [ev for _, ev in events[:bound]]
        except Exception:
            return []

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
        invalidate_lm_cache()
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
        try:
            tok = subscribe_topic_archived(
                self.context, self._on_topic_archived_event, owner="group_chronicle"
            )
            if tok:
                self._event_bus_tokens.append(tok)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"订阅 Dynamics Event Bus topic_archived 跳过: {e}")

    async def _on_topic_archived_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Event Bus callback: collect high-signal archived topics as chronicle candidates."""
        return await ingest_archived_topic_event(self, payload)

    def get_archived_topic_cues(self, umo: str) -> list[dict[str, Any]]:
        """Read-only accessor for recently collected topic_archived candidate cues."""
        return list(self._archived_topic_cues.get(str(umo or ""), []))

    def _schedule_milestone_keepsake(
        self,
        umo: str,
        *,
        title: str,
        summary: str,
        kind: str = "chronicle_edition",
        tab: str = "editions",
        event_id: int | None = None,
        report_id: int | None = None,
        existing_image_path: str = "",
        importance: int = 3,
    ) -> None:
        """Schedule non-blocking polaroid artwork + 3D World keepsake + Event Bus milestone."""
        if self._terminating or not self._cfg("enable_world_keepsake_sync", True):
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._spawn_bg(
            publish_chronicle_milestone(
                self,
                umo,
                title=title,
                summary=summary,
                kind=kind,
                tab=tab,
                event_id=event_id,
                report_id=report_id,
                existing_image_path=existing_image_path,
                importance=importance,
            ),
            label=f"keepsake:{kind}:{umo}",
        )

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

        trigger = CronTrigger(
            day_of_week=str(weekday),
            hour=hour,
            minute=minute,
            timezone=self._series_tz(),
        )
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
        for tok in list(getattr(self, "_event_bus_tokens", [])):
            unsubscribe_event(self.context, tok)
        if hasattr(self, "_event_bus_tokens"):
            self._event_bus_tokens.clear()
        if hasattr(self, "_active_blindboxes"):
            self._active_blindboxes.clear()
        invalidate_lm_cache()
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
        self,
        umo: str | None = None,
        query: str = "",
        limit: int = 6,
        *,
        scope: str | None = None,
        member: str | None = None,
        kind: str = "search",
    ) -> dict[str, Any]:
        """Read-only v1 public interface returning approved, verified group slang entries.

        Supports both series callers ``get_culture_context(umo, query="", limit=6)``
        and v1 receipt callers ``get_culture_context(scope=..., member=..., query=..., kind=..., limit=...)``.
        """
        resolved_scope = str(scope if scope is not None else (umo or "")).strip()
        resolved_member = str(member if member is not None else "system").strip() or "system"
        clean_query = str(query or "").strip()[:160]
        try:
            bound = max(1, min(int(limit or 6), 20))
        except (TypeError, ValueError):
            bound = 6
        if (
            self._terminating
            or not resolved_scope
            or getattr(self, "culture", None) is None
            or getattr(self, "history", None) is None
        ):
            return {
                "schema_version": 1,
                "available": False,
                "scope": resolved_scope,
                "reason": "群文化服务未就绪",
                "entries": [],
                "terms": [],
                "summary": "",
            }
        result = await self.culture.query(
            resolved_scope, resolved_member, clean_query, kind, bound
        )
        privacy = await asyncio.to_thread(self.history.privacy_key, resolved_scope)
        entries = list(result.get("entries") or [])
        terms = [
            {
                "id": e["id"],
                "name": e["name"],
                "meaning": e["meaning"],
                "usage": e.get("usage", ""),
                "aliases": [
                    a["name"]
                    for a in e.get("aliases", [])
                    if isinstance(a, dict) and a.get("name")
                ],
            }
            for e in entries
        ]
        summary = "；".join(
            f"「{t['name']}」：{t['meaning']}" for t in terms if t["name"] and t["meaning"]
        )
        result["scope"] = resolved_scope
        result["terms"] = terms
        result["summary"] = summary
        result["receipt"] = {
            "schema_version": 1,
            "scope": resolved_scope,
            "member": resolved_member,
            "privacy": privacy,
            "entries": [(e["id"], e["revision"]) for e in entries],
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
            tail = (
                str(getattr(event, "message_str", ""))
                .lstrip("/")
                .partition("梗溯源")[2]
                .strip()
            )
            res = await build_meme_evolution_card(self, event, tail or query)
            umo = str(getattr(event, "unified_msg_origin", "") or "")
            card_path = res.get("card_image_path")
            meme_imgs = [p for p in (res.get("meme_images") or []) if p and os.path.isfile(p)]
            has_card = bool(card_path and os.path.isfile(card_path))
            if has_card or meme_imgs:
                try:
                    from astrbot.api.message_components import Image as _ImageComp
                except ImportError:
                    _ImageComp = None
                if _ImageComp is not None:
                    chain_items = []
                    if has_card:
                        chain_items.append(_ImageComp.fromFileSystem(card_path))
                    for mp in meme_imgs[:3]:
                        chain_items.append(_ImageComp.fromFileSystem(mp))
                    if chain_items:
                        try:
                            await self._send(
                                event,
                                umo,
                                MessageChain(chain=chain_items),
                                prefer_context=False,
                            )
                        except Exception as exc:
                            logger.debug(f"梗溯源图文发送失败，降级为纯文本: {exc}")
            yield event.plain_result(res["text"])
        except (ValueError, PermissionError) as exc:
            yield event.plain_result(str(exc))

    @filter.command("梗图演绎", alias={"梗演绎"})
    async def meme_remix_command(self, event: AstrMessageEvent, query: str = ""):
        """调用视觉工坊为本群热梗生成角色演绎专属表情图（无视觉插件时降级卡片/文本）。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            yield event.plain_result(scope_reason)
            return
        raw_msg = str(getattr(event, "message_str", "") or "").lstrip("/")
        tail = ""
        for prefix in ("梗图演绎", "梗演绎"):
            if prefix in raw_msg:
                tail = raw_msg.partition(prefix)[2].strip()
                break
        term = tail or str(query or "").strip()
        if not term:
            yield event.plain_result("用法：/梗图演绎 <群梗词或ID>（将结合本群文化词库生成角色演绎表情图）。")
            return
        res = await generate_meme_remix(self, event, term)
        img = res.get("image_path")
        if img and os.path.isfile(img):
            await deliver_text(
                self.context,
                event,
                umo,
                res["text"],
                img,
                prefer_context=False,
                use_forward_nodes=False,
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                node_name="梗图演绎",
            )
            return
        yield event.plain_result(res["text"])

    @filter.command("群史盲盒", alias={"史记盲盒"})
    async def history_blindbox_command(self, event: AstrMessageEvent, arg: str = ""):
        """随机抽取本群历史名场面或老梗出处进行轻量考据互动（支持「揭晓」查看答案）。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        raw_msg = str(getattr(event, "message_str", "") or "").lstrip("/")
        tail = ""
        for prefix in ("群史盲盒", "史记盲盒"):
            if prefix in raw_msg:
                tail = raw_msg.partition(prefix)[2].strip()
                break
        res = await run_blindbox(self, event, tail or arg)
        yield event.plain_result(res["text"])

    @filter.command("那年今日", alias={"历史上的今天"})
    async def on_this_day_command(self, event: AstrMessageEvent, date_arg: str = ""):
        """回顾往年今日的群大事与群梗诞生时刻，支持图文回顾卡。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        raw_msg = str(getattr(event, "message_str", "") or "").lstrip("/")
        tail = ""
        for prefix in ("那年今日", "历史上的今天"):
            if prefix in raw_msg:
                tail = raw_msg.partition(prefix)[2].strip()
                break
        group_name = await asyncio.to_thread(self._group_name_readonly, umo)
        res = await build_on_this_day(
            self,
            umo,
            date_arg=tail or date_arg,
            group_name=group_name,
            render_card=True,
            push_mode=False,
        )
        img = res.get("image_path")
        if res.get("has_items") and img and os.path.isfile(img):
            await deliver_text(
                self.context,
                event,
                umo,
                res["text"],
                img,
                prefer_context=False,
                use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                node_name="那年今日",
            )
            return
        yield event.plain_result(res["text"])

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
            tail = (
                str(getattr(event, "message_str", ""))
                .lstrip("/")
                .partition("文化隐藏")[2]
                .strip()
            )
            yield event.plain_result(
                await self.culture.command(event, "文化隐藏", tail or entry_id)
            )
        except (ValueError, PermissionError) as exc:
            yield event.plain_result(str(exc))

    @filter.command("文化恢复")
    async def culture_restore(self, event: AstrMessageEvent, entry_id: str = ""):
        try:
            tail = (
                str(getattr(event, "message_str", ""))
                .lstrip("/")
                .partition("文化恢复")[2]
                .strip()
            )
            yield event.plain_result(
                await self.culture.command(event, "文化恢复", tail or entry_id)
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
            "history_daily_limit": (0, 1000),
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
                progress=progress,
            )
            if skipped:
                raise _SkipWeek()
            if text.startswith("此时间范围尚无已入史的大事。"):
                raise NoActivityError(text)
            vol_days = int(days or self._cfg("chronicle_days", 7) or 7)
            vol_label = {7: "周卷", 30: "月卷", 90: "季卷", 365: "年卷"}.get(
                vol_days, f"{vol_days}天卷"
            )
            self._schedule_milestone_keepsake(
                umo,
                title=f"{group_name} · 群史记（{vol_label}）",
                summary=text[:260],
                kind="chronicle_edition",
                tab="editions",
                existing_image_path=image_path or "",
                importance=3,
            )
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
            raw_fallback_pw = (
                self.config.get("render_fallback_playwright")
                if isinstance(self.config, dict)
                else None
            )
            fallback_pw = (
                bool(raw_fallback_pw) if raw_fallback_pw is not None else None
            )
            t2i_scale = str(self._cfg("t2i_scale", "device")).strip() or "device"
            render_theme = normalize_render_theme(
                self._cfg("render_theme", "classic_scroll")
            )
            text, image_path = await render_text_or_image(
                text,
                group_name,
                os.path.join(self.data_dir, "images"),
                render_mode=render_mode,
                html_render_func=getattr(self, "html_render", None),
                enable_cache=bool(self._cfg("enable_render_cache", True)),
                render_fallback_playwright=fallback_pw,
                options={"scale": t2i_scale},
                theme=render_theme,
                context=getattr(self, "context", None),
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

        self._schedule_milestone_keepsake(
            umo,
            title=f"{group_name} · {iso_year}-W{iso_week:02d} 群史记",
            summary=text[:260],
            kind="chronicle_edition",
            tab="editions",
            existing_image_path=image_path or "",
            importance=3,
        )
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
        """插件配置的 LLM Provider ID; 空字符串时只读继承 Chat Dynamics 共享模型配置。"""
        pid = str(self._cfg("llm_provider_id", "") or "").strip()
        if not pid:
            mod = self._series_panels_mod()
            ctx = getattr(self, "context", None)
            if mod is not None and callable(
                getattr(mod, "get_series_shared_config", None)
            ):
                shared = mod.get_series_shared_config(ctx)
                if isinstance(shared, dict):
                    pid = str(
                        shared.get("reply_provider")
                        or shared.get("light_analysis_provider")
                        or ""
                    ).strip()
            if not pid and mod is not None and callable(
                getattr(mod, "resolve_series_provider", None)
            ):
                pid = mod.resolve_series_provider(ctx, "", role="reply")
        return pid or None

    async def _check_group_scope(self, umo: str) -> tuple[bool, str]:
        """Enforce group-only pattern, core exclude_groups, allow/block lists, and history.allowed."""
        clean = str(umo or "").strip()
        if not clean or not GROUP_PATTERN.fullmatch(clean):
            return False, "此命令只能在群内使用"
        if self._is_excluded_by_core(clean):
            return False, "本群已在主控排除列表中，无法执行此操作"
        try:
            allow, block = scope_lists(self.config)
        except ValueError:
            return False, "群作用域配置无效，请管理员检查配置"
        if clean in block or (allow and clean not in allow):
            return False, "本群不在允许范围"
        history = getattr(self, "history", None)
        if history is not None:
            recording = bool(self._cfg("history_enabled", True))
            try:
                allowed = await history.allowed(clean, recording=recording)
            except TypeError:
                allowed = await history.allowed(clean)
            if not allowed:
                return False, "本群长期记录已停用或不在允许范围"
        return True, ""

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
            LLMUnavailableError: LivingMemory 未检测到 / 未初始化 / 群不在允许范围。
            NoActivityError: 时间窗内无足够活动或数据异常。
        """
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            raise LLMUnavailableError(scope_reason)
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

    @filter.command("史记帮助", alias={"群史记帮助"})
    async def chronicle_help_command(self, event: AstrMessageEvent):
        """输出分级帮助：区分「群友常用指令」与「管理员审核/运维指令」。"""
        lines = [
            "📜 群史记 · 分级指令帮助",
            "",
            "【群友常用指令】",
            "• 群史记 [天数|月|年] [风格=正史|八卦|文言] [强制] —— 生成或查看本群史记",
            "• 我的群年报 [季度|年度] [主题=古籍|典雅|现代] / 群年报 —— 生成多维时光机年报长图卡片",
            "• 史记列表 / 史记查看 [序号] / 史记对比 —— 回看与对比往期卷册",
            "• 史记导出 [序号] / 史记网页 [序号] —— 导出 Markdown 或网页文件",
            "• 群大事 [全部|月|年|天数] —— 查看已入史的本群大事记",
            "• 人物等级 [@成员] / 人物传 [@成员] / 影响力榜 —— 查看贡献等级、传记与排行",
            "• 记一笔 标题｜经过与结果｜日期=YYYY-MM-DD —— 提交群大事线索（支持 @参与者）",
            "• 史记更正 <事件ID> <建议> —— 向管理员提交事件更正请求",
            "• 群文化 [关键词] / 梗溯源 <词或ID> / 梗图演绎 <词> / 文化纠正 <ID> <说明> —— 查询群梗、图文溯源与演绎表情",
            "• 群史盲盒 [大事|词条|揭晓] / 那年今日 [MMDD] —— 历史名场面考据盲盒与往年今日回顾卡",
            "• 史记隐藏 [事件ID] [全局] / 史记恢复 [事件ID] [全局] —— 预览确认隐藏与可审计恢复",
            "• 文化隐藏 <ID> [全局] / 文化恢复 <ID> [全局] —— 预览确认隐藏文化词条与可审计恢复",
            "  （注：隐藏默认仅作用于本插件，加「全局」可同步静音/解除群间记忆与群间学习）",
            "• 史记选题 / 史记选票 <1-3> —— 参与本周选题投票（需开启 enable_vote）",
            "",
            "【管理员审核 / 运维指令】",
            "• 史记待审 [事件ID] —— 私发待审事件与更正请求详情（不公开到群内）",
            "• 史记确认 <ID> / 史记驳回 <ID> [理由] —— 审核入史或驳回候选事件",
            "• 史记合并 <保留ID> <被合并ID> —— 将重复/续记事件并入同一事件",
            "• 史记修订 <ID> 新经过｜说明=理由 [｜重要度=1~3｜核心=UID] —— 审计修订事件",
            "• 史记恢复 @成员 [全局] / 文化恢复 <ID> [全局] —— 管理员可恢复任意已隐藏记录",
            "• 史记进度 / 史记sid —— 查看回溯水位、每日额度与当前群 UMO 标识",
            "• 史记周推 / 史记续写 / 史记选题公布 / 宇宙史记 —— 定时推送、续写与跨群综合",
            "• 文化工作台：在 AstrBot 面板打开「群史馆 / 群文化沿革」WebUI 可阅览卷册、审核大事与词条、批量处理、查看人物列传墙及手动整理。",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.command("我的群年报", alias={"群年报"})
    async def annual_report_command(self, event: AstrMessageEvent, period: str = ""):
        """生成个人/全群阶段时光机（季度/年度群聊年报长图卡片）并直发群内。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            yield event.plain_result(scope_reason)
            return
        raw_msg = str(getattr(event, "message_str", "") or "").lstrip("/")
        is_group_alias = raw_msg.startswith("群年报")
        tail = ""
        for prefix in ("我的群年报", "群年报"):
            if prefix in raw_msg:
                tail = raw_msg.partition(prefix)[2].strip()
                break
        parsed = parse_annual_report_args(
            tail or period, is_group_alias=is_group_alias
        )
        actor_id = self._sender_id(event) or ""
        if not actor_id:
            yield event.plain_result("无法识别调用者身份")
            return
        mentioned = self._extract_at_sender(event)
        target_uid = actor_id
        if mentioned and mentioned != actor_id:
            if self._cfg("bio_admin_only", True) and not self._is_admin(event):
                yield event.plain_result("查询其他成员的群年报需要管理员权限")
                return
            target_uid = mentioned
            parsed["mode"] = "personal"
        sender_name = ""
        name_getter = getattr(event, "get_sender_name", None)
        if callable(name_getter):
            try:
                sender_name = str(name_getter() or "")
            except Exception:
                sender_name = ""
        group_name = "群聊"
        g_getter = getattr(event, "get_group_name", None)
        if callable(g_getter):
            try:
                group_name = str(g_getter() or "群聊")
            except Exception:
                group_name = "群聊"
        self._spawn_bg(
            self._annual_report_run(
                event,
                umo,
                actor_id=target_uid,
                actor_name=sender_name if target_uid == actor_id else target_uid,
                mode=parsed["mode"],
                period=parsed["period"],
                group_name=group_name,
                theme=parsed["theme"],
                prefer_context=True,
            ),
            label=f"annual_report:{umo}",
        )
        yield event.plain_result(f"正在生成{parsed['period_label']}长图卡片，请稍候…")

    async def _annual_report_run(
        self,
        event,
        umo: str,
        *,
        actor_id: str,
        actor_name: str,
        mode: str,
        period: str,
        group_name: str,
        theme: str | None,
        prefer_context: bool = True,
    ) -> None:
        async def _work():
            text, image_path = await build_annual_report(
                self,
                umo,
                actor_id=actor_id,
                actor_name=actor_name,
                mode=mode,
                period=period,
                group_name=group_name,
                theme=theme,
            )
            await deliver_text(
                self.context,
                event,
                umo,
                text,
                image_path,
                prefer_context=prefer_context,
                use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                node_name="群聊时光机",
            )

        await self._run_locked(event, umo, _work(), prefer_context=prefer_context)

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
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            yield event.plain_result(scope_reason)
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
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            yield event.plain_result(scope_reason)
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
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            await self._reply(
                event, scope_reason, umo=umo, prefer_context=prefer_context
            )
            return
        group_evidences: list = []
        for g in groups:
            g = str(g).strip()
            if not g:
                continue
            g_ok, g_reason = await self._check_group_scope(g)
            if not g_ok:
                logger.info(f"宇宙史记: 群 {g} 不在允许范围, 跳过 ({g_reason})")
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
        scope_ok, scope_reason = await self._check_group_scope(umo)
        if not scope_ok:
            yield event.plain_result(scope_reason)
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
        lines = []
        for i, r in enumerate(ranking, 1):
            base = f"{i}. {r.get('sender_name') or r.get('sender_id') or '?'} ({r.get('score', 0)})"
            bd = r.get("breakdown")
            if isinstance(bd, dict) and bd:
                parts = []
                for keys, label in (
                    (("lm", "activity"), "活跃"),
                    (("sl", "social"), "社交"),
                    (("ra", "relation"), "关系"),
                ):
                    val = next((bd[k] for k in keys if k in bd and bd[k] is not None), None)
                    if val is not None:
                        parts.append(f"{label} {val}")
                if parts:
                    base += " · " + " / ".join(parts)
            lines.append(base)
        yield event.plain_result("本周影响力榜：\n" + "\n".join(lines))

    @filter.command("史记列表")
    async def chronicle_list_command(self, event: AstrMessageEvent):
        """查看本群已生成的史记列表（含周/月/年卷标识与一句话摘要）。"""
        umo = getattr(event, "unified_msg_origin", None)
        if not umo:
            yield event.plain_result("无法获取会话标识")
            return
        entries = await asyncio.to_thread(self.store.list, umo)
        if not entries:
            yield event.plain_result("暂无史记")
            return
        lines = []
        for i, e in enumerate(entries, 1):
            period_days = int(e.get("period_days") or 0)
            vol_tag = (
                {7: "周卷", 30: "月卷", 90: "季卷", 365: "年卷"}.get(
                    period_days, f"{period_days}天卷"
                )
                if period_days
                else "周卷"
            )
            ver = int(e.get("version") or 1)
            ver_suffix = f" v{ver}" if ver > 1 else ""
            raw_summary = str(e.get("summary") or "").strip()
            if not raw_summary:
                heading_fallback = ""
                for ln in str(e.get("chronicle_text") or "").splitlines():
                    stripped = ln.strip()
                    if not stripped:
                        continue
                    cleaned = stripped.lstrip("#>-*• ").strip()
                    if (
                        not cleaned
                        or cleaned.startswith("覆盖 ")
                        or cleaned.startswith("——")
                    ):
                        continue
                    if stripped.startswith("#"):
                        if not heading_fallback:
                            heading_fallback = cleaned
                        continue
                    raw_summary = cleaned
                    break
                if not raw_summary:
                    raw_summary = heading_fallback
            snippet = (
                f" — {raw_summary[:38]}{'…' if len(raw_summary) > 38 else ''}"
                if raw_summary
                else ""
            )
            lines.append(
                f"{i}. {e.get('iso_year')}-W{int(e.get('iso_week', 0)):02d} "
                f"({datetime.fromtimestamp(e.get('created_at', 0)):%Y-%m-%d}) "
                f"[{vol_tag}{ver_suffix}]{snippet}"
            )
        yield event.plain_result("史记列表：\n" + "\n".join(lines))

    @filter.command("史记查看")
    async def chronicle_view_command(
        self, event: AstrMessageEvent, idx: str | None = None
    ):
        """查看指定序号的史记正文 (默认最新一条；长文本走合并转发防刷屏)。"""
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
        max_chars = int(self._cfg("max_reply_chars", 1500))
        use_forward = bool(self._cfg("use_forward_nodes", True))
        if len(text) > max_chars and use_forward:
            if not await asyncio.to_thread(self._entry_public, umo, entry):
                yield event.plain_result("该卷内容已隐藏或修订，请重新查看。")
                return

            async def _view_guard():
                return await asyncio.to_thread(self._entry_public, umo, entry)

            delivered = await deliver_text(
                self.context,
                event,
                umo,
                text,
                None,
                prefer_context=False,
                use_forward_nodes=True,
                max_chars=max_chars,
                node_name="史记查看",
                guard=_view_guard,
            )
            if delivered:
                return
        for segment in split_text(text, max_chars):
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
                return (
                    tgts,
                    history.privacy_key(umo),
                    history.store.privacy_revision(umo),
                )

            targets, privacy, privacy_rev = await asyncio.to_thread(_load_bio_inputs)
            if targets:
                bio_sem = asyncio.Semaphore(2)

                async def _run_history_bio(uid: str) -> str:
                    async with bio_sem:
                        return await history.biography(umo, uid)

                bios = await asyncio.gather(*(_run_history_bio(uid) for uid in targets))
                text = "\n\n".join(bios)
            else:
                text = "本期暂无已入史的贡献事件；可以 @成员 查看其基础小记。"

            async def still_public():
                if not await history.allowed(umo):
                    return False

                def _check_bio_state():
                    return (
                        history.privacy_key(umo) == privacy
                        and history.store.privacy_revision(umo) == privacy_rev
                    )

                return await asyncio.to_thread(_check_bio_state)

            bio_group_name = await asyncio.to_thread(self._group_name_readonly, umo)
            bio_image_path = None
            if targets and await still_public():
                bio_image_path = await self._render_bio_card(text, bio_group_name)

            await deliver_text(
                self.context,
                event,
                umo,
                text,
                bio_image_path,
                prefer_context=prefer_context,
                use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
                max_chars=int(self._cfg("max_reply_chars", 1500)),
                node_name="人物传记",
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
        bio_sem = asyncio.Semaphore(2)

        async def _run_legacy_bio(member: dict) -> tuple[str, str]:
            name = member.get("sender_name") or member.get("sender_id") or "未知成员"
            async with bio_sem:
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
            return name, bio

        results = await asyncio.gather(
            *(_run_legacy_bio(member) for member in members),
            return_exceptions=True,
        )
        parts: list[str] = []
        for member, res in zip(members, results):
            name = member.get("sender_name") or member.get("sender_id") or "未知成员"
            if isinstance(res, asyncio.CancelledError):
                raise res
            if isinstance(res, asyncio.TimeoutError):
                logger.warning(f"[群史记] 传记生成超时 (>{timeout:.0f}s) umo={umo}")
                await self._reply(
                    event,
                    f"生成超时（超过 {timeout:.0f} 秒），已自动终止，请稍后重试",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return
            if isinstance(res, LLMUnavailableError):
                await self._reply(
                    event,
                    "未配置可用的 LLM，无法生成人物传记",
                    umo=umo,
                    prefer_context=prefer_context,
                )
                return
            if isinstance(res, Exception):
                logger.warning(f"生成成员 {name} 传记失败: {res}")
                bio = "（传记生成失败）"
            else:
                name, bio = res
            parts.append(f"## 人物传记：{name}\n{bio}")
        bio_body = "\n\n".join(parts)
        bio_group_name = "群聊"
        g_getter = getattr(event, "get_group_name", None) if event is not None else None
        if callable(g_getter):
            try:
                bio_group_name = str(g_getter() or "群聊")
            except Exception:
                bio_group_name = "群聊"
        bio_image_path = await self._render_bio_card(bio_body, bio_group_name)
        await deliver_text(
            self.context,
            event,
            umo,
            bio_body,
            bio_image_path,
            prefer_context=prefer_context,
            use_forward_nodes=bool(self._cfg("use_forward_nodes", True)),
            max_chars=int(self._cfg("max_reply_chars", 1500)),
            node_name="人物传记",
        )
        logger.info(f"[群史记] 人物传已发送 umo={umo} members={len(parts)}")

    async def _render_bio_card(self, text: str, group_name: str) -> str | None:
        """Render biography markdown into a card image using configured theme (degrades to None)."""
        try:
            render_mode = str(self._cfg("render_mode", "t2i")).strip() or "t2i"
            if render_mode == "off":
                return None
            raw_fallback_pw = (
                self.config.get("render_fallback_playwright")
                if isinstance(self.config, dict)
                else None
            )
            fallback_pw = (
                bool(raw_fallback_pw) if raw_fallback_pw is not None else None
            )
            t2i_scale = str(self._cfg("t2i_scale", "device")).strip() or "device"
            render_theme = normalize_render_theme(
                self._cfg("render_theme", "classic_scroll")
            )
            _t, image_path = await render_text_or_image(
                text,
                group_name,
                os.path.join(self.data_dir, "images"),
                render_mode=render_mode,
                html_render_func=getattr(self, "html_render", None),
                enable_cache=bool(self._cfg("enable_render_cache", True)),
                render_fallback_playwright=fallback_pw,
                options={"scale": t2i_scale},
                theme=render_theme,
                title_suffix="人物传",
                context=getattr(self, "context", None),
            )
            return image_path
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"渲染人物传卡片图失败，降级为纯文本: {exc}")
            return None

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
                progress=self._make_progress(umo),
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
        if self._in_series_quiet_hours():
            logger.info("当前处于群间系列静默时段，跳过本次每周史记定时推送")
            return
        groups = self._cfg("push_groups", []) or []
        logger.info(f"每周史记推送开始, 目标群数: {len(groups)}")
        for umo in groups:
            if self._terminating:
                break
            if self._is_excluded_by_core(str(umo)):
                logger.info(f"该群已被主控排除，跳过每周史记推送: {umo}")
                continue
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
        if getattr(self, "history", None) is not None and await asyncio.to_thread(
            self.history.store.is_group_paused, umo
        ):
            logger.info(f"群 {umo} 已按群暂停，跳过每周史记推送")
            return
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
                if self._cfg("enable_on_this_day_in_push", True):
                    try:
                        otd_group = await asyncio.to_thread(
                            self._group_name_readonly, umo
                        )
                        otd = await build_on_this_day(
                            self,
                            umo,
                            group_name=otd_group,
                            render_card=False,
                            push_mode=True,
                        )
                        if otd.get("has_items") and otd.get("text"):
                            await deliver_text(
                                self.context,
                                None,
                                umo,
                                otd["text"],
                                otd.get("image_path"),
                                prefer_context=True,
                                use_forward_nodes=bool(
                                    self._cfg("use_forward_nodes", True)
                                ),
                                max_chars=int(self._cfg("max_reply_chars", 1500)),
                                node_name="那年今日",
                            )
                    except Exception:
                        logger.debug("Weekly push on-this-day appendix skipped", exc_info=True)
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
