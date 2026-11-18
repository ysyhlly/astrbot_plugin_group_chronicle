"""Passive identity/notice collection without waking the reply pipeline."""

from __future__ import annotations

import hashlib
import math
import time
import weakref

from astrbot.api import logger

from . import lm_bridge
from .archive_store import encode

#: 本模块唯一的可选框架依赖：旧版 AstrBot 的 astrbot.api.event.filter 可能没有
#: CustomFilter 基类。这里**只包裹这一个导入**（而不是像 main.py 那样把整段
#: `.core.*` 相对导入都罩进 `except ImportError`），所以本模块内其它真实的
#: ImportError（例如上面 `.archive_store` 的导入失败）会原样向上抛出，不会再被
#: main.py 的回退分支伪装成 "No module named 'core'" 而掩盖真正缺的东西。
try:
    from astrbot.api.event.filter import CustomFilter
except ImportError:  # pragma: no cover - 旧版框架降级路径
    logger.warning(
        "astrbot.api.event.filter.CustomFilter 不可用，历史采集过滤器以本地基类降级运行"
    )

    class CustomFilter:  # type: ignore[no-redef]
        """最小降级基类；框架只要求实例暴露 filter(event, cfg)。"""

        def __init__(self, *args, **kwargs):
            self.raise_error = (
                bool(args[0]) if args else bool(kwargs.get("raise_error", True))
            )


_ACTIVE = None


def set_active(plugin):
    """Keep a weak reference to the loaded plugin.

    Args:
        plugin: Active plugin or None on unload.
    """
    global _ACTIVE
    _ACTIVE = weakref.ref(plugin) if plugin is not None else None


class HistoryCaptureFilter(CustomFilter):
    """Use a side-effect filter that always returns False, as LivingMemory does."""

    def filter(self, event, cfg):
        """Schedule only copied identity and notification fields.

        Args:
            event: Actual AstrBot event, including empty OneBot notices.
            cfg: Current pipeline configuration.

        Returns:
            False so this handler never wakes the bot.
        """
        plugin = _ACTIVE() if _ACTIVE else None
        umo = getattr(event, "unified_msg_origin", "")
        if not plugin or plugin._terminating or not plugin.history.enabled_for(umo):
            return False
        raw = getattr(getattr(event, "message_obj", None), "raw_message", {})
        getter = getattr(raw, "get", None)
        raw = raw if callable(getter) else {}
        try:
            ts = float(
                raw.get("time") or getattr(event.message_obj, "timestamp", time.time())
            )
            if not math.isfinite(ts) or not 0 < ts <= time.time() + 30:
                return False
            observation = {
                "umo": umo,
                "uid": str(event.get_sender_id() or ""),
                "name": str(event.get_sender_name() or ""),
                "ts": ts,
                "self_id": str(event.get_self_id() or ""),
            }
            group_getter = getattr(event, "get_group_name", None)
            observation["group_name"] = (
                group_getter() if callable(group_getter) else None
            )
            notice = raw.get("notice_type")
            if raw.get("post_type") == "notice":
                if notice not in ("group_increase", "group_decrease"):
                    return False
                observation.update(
                    uid=str(raw.get("user_id") or ""),
                    name=None,
                    kind="join" if notice == "group_increase" else "leave",
                    reason=str(raw.get("sub_type") or ""),
                )
                observation["key"] = hashlib.sha256(
                    encode(
                        [
                            umo,
                            observation["kind"],
                            observation["uid"],
                            ts,
                            raw.get("operator_id"),
                            raw.get("sub_type"),
                        ]
                    ).encode()
                ).hexdigest()
            elif not getattr(event, "message_str", "").strip():
                return False
            if not observation["uid"]:
                return False
            if observation.get("self_id") and umo:
                plugin.history.bot_ids.setdefault(umo.split(":", 1)[0], set()).add(
                    observation["self_id"]
                )
            if "kind" not in observation and lm_bridge.is_dm_capturing_group(
                plugin.context, event
            ):
                return False
            plugin._spawn_bg(
                plugin.history.capture(observation, cfg), label=f"history-observe:{umo}"
            )
        except Exception:
            # A passive filter must not produce a framework error reply.
            return False
        return False
