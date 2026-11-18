"""core/deliver.py —— 长文本分段投递 + 合并转发 + 导出 markdown 组装。

职责:
- split_text: 把超长文本按段落 / 换行边界切成 ≤ max_chars 的段,
  无法找到自然边界时硬切。
- deliver_text: 统一发送入口 —— 先图后文; 超长文本优先走合并转发
  (QQ Nodes, 镜像 astrbot_plugin_portrayal 的 Node/Nodes 用法),
  否则拆成多段纯文本 + 小间隔发送。
- build_export_markdown: 史记导出 markdown 组装 (史记导出 命令使用)。

设计约束:
- astrbot 符号全部守卫导入 (无 astrbot 环境 Node/Nodes 为 None,
  自动降级为多段纯文本路径, 测试环境同理)。
- 发送失败绝不抛出: 逐条尝试, 返回是否至少成功一条。
"""

import asyncio
import inspect
from datetime import datetime

from astrbot.api import logger

try:
    from astrbot.api.event import MessageChain
except Exception:  # pragma: no cover - 无 astrbot 环境 (测试) 由 conftest 桩件提供
    MessageChain = None

try:
    from astrbot.core.message.components import Node, Nodes, Plain  # noqa: F401
except Exception:  # pragma: no cover - 老版本 astrbot 可能没有 core.message.components
    Node = None
    Nodes = None
    Plain = None

#: 分段间最小发送间隔 (秒), 多段纯文本路径用于避免触发平台风控/乱序
_SEGMENT_SLEEP_SECONDS = 0.3


def split_text(text: str, max_chars: int) -> list[str]:
    """把文本按长度切成多段。

    优先在段落边界 ("\\n\\n") 处切, 其次单换行 ("\\n") 处切,
    都没有则按 max_chars 硬切。切点换行符保留在前一段末尾,
    保证 markdown 结构不因分段而错乱。

    Args:
        text: 原始文本 (可为空)。
        max_chars: 单段最大字符数 (≥1)。

    Returns:
        分段列表; 空文本返回空列表; 短文本返回 [原文本]。
    """
    if not text:
        return []
    max_chars = max(1, int(max_chars))
    if len(text) <= max_chars:
        return [text]

    segments: list[str] = []
    remaining = text
    while len(remaining) > max_chars:
        cut = _find_cut(remaining, max_chars)
        segments.append(remaining[:cut])
        remaining = remaining[cut:]
    if remaining:
        segments.append(remaining)
    return segments


def _find_cut(text: str, max_chars: int) -> int:
    """在 text[:max_chars] 范围内找最近的段落/换行切点; 找不到硬切。"""
    for sep in ("\n\n", "\n"):
        idx = text.rfind(sep, 0, max_chars)
        if idx != -1:
            return idx + len(sep)
    return max_chars


async def _send_chain(
    context, event, umo, chain, prefer_context: bool, guard=None
) -> bool:
    """发送一条消息链: event.send → context.send_message 兜底; 全失败返回 False。"""
    if guard is not None:
        allowed = guard()
        if inspect.isawaitable(allowed):
            allowed = await allowed
        if not allowed:
            return False
    if not prefer_context and event is not None:
        send = getattr(event, "send", None)
        if callable(send):
            try:
                await send(chain)
                return True
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"[deliver] event.send 失败, 回退 context.send_message: {e}"
                )

    if umo and context is not None:
        send_message = getattr(context, "send_message", None)
        if callable(send_message):
            try:
                result = await send_message(umo, chain)
                return result is not False
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[deliver] context.send_message 失败: {e}")
    return False


def _build_forward_nodes(segments: list[str], node_name: str, node_uin: str):
    """构造合并转发 Nodes 对象 (Node/Nodes 缺失时返回 None)。"""
    if Nodes is None or Node is None or Plain is None:
        return None
    nodes = Nodes(
        [Node(uin=node_uin, name=node_name, content=[Plain(seg)]) for seg in segments]
    )
    return nodes


async def deliver_text(
    context,
    event,
    umo,
    text: str,
    image_path=None,
    *,
    prefer_context: bool = False,
    use_forward_nodes: bool = True,
    max_chars: int = 1500,
    node_name: str = "群史记",
    node_uin: str = "0",
    guard=None,
) -> bool:
    """统一发送入口: 有图先发图, 长文本走合并转发或多段纯文本。

    Args:
        context: AstrBot 插件上下文 (发送兜底)。
        event: 事件 (event.send 优先); 后台任务可传 None。
        umo: unified_msg_origin; None 且 event 无 send 时发送失败。
        text: 正文。
        image_path: 可选配图; 发送成功即返回, 失败回退纯文本。
        prefer_context: True 时跳过 event.send, 直接 context.send_message。
        use_forward_nodes: 长文本是否优先尝试合并转发 Nodes。
        max_chars: 单段/单条上限。
        node_name / node_uin: 合并转发节点的显示名与 uin。
        guard: Optional current-scope/privacy check before each segment.

    Returns:
        bool: 是否至少成功发送了一条消息。
    """
    if image_path:
        chain = (
            MessageChain().file_image(image_path) if MessageChain is not None else None
        )
        if chain is not None and await _send_chain(
            context, event, umo, chain, prefer_context, guard
        ):
            return True
        logger.warning("[deliver] 图片发送失败或不可用, 回退纯文本")

    if MessageChain is None:
        logger.error("[deliver] MessageChain 不可用, 无法发送")
        return False

    segments = split_text(text, max_chars)
    if len(segments) <= 1:
        return await _send_chain(
            context, event, umo, MessageChain().message(text), prefer_context, guard
        )

    if use_forward_nodes:
        nodes = _build_forward_nodes(segments, node_name, node_uin)
        if nodes is not None:
            ok = await _send_chain(
                context, event, umo, MessageChain(chain=[nodes]), prefer_context, guard
            )
            if ok:
                return True
            logger.warning("[deliver] 合并转发发送失败, 回退多段纯文本")

    # 多段纯文本: 逐条发送, 小间隔防乱序
    all_ok = True
    any_ok = False
    for seg in segments:
        ok = await _send_chain(
            context, event, umo, MessageChain().message(seg), prefer_context, guard
        )
        any_ok = any_ok or ok
        all_ok = all_ok and ok
        if prefer_context or event is None:
            # 后台/定时路径没有事件回显, 始终小间隔
            await asyncio.sleep(_SEGMENT_SLEEP_SECONDS)
    if not all_ok:
        logger.warning(f"[deliver] 多段发送部分失败 segments={len(segments)}")
    return all_ok and any_ok


def build_export_markdown(entry: dict) -> str:
    """把 store 条目组装为可导出的 markdown 文本。

    Args:
        entry: ChronicleStore 条目 (umo/group_name/iso_year/iso_week/
            created_at/chronicle_text/biographies)。

    Returns:
        完整 markdown 字符串。
    """
    week = int(entry.get("iso_week", 0) or 0)
    title = f"群史记 {entry.get('iso_year', '?')}-W{week:02d}"
    if entry.get("period_days"):
        label = {30: "月卷", 365: "年卷"}.get(
            entry["period_days"], f"{entry['period_days']}天卷"
        )
        title += f" · {label} · 第{entry.get('version', 1)}版"
    try:
        created = (
            datetime.fromtimestamp(float(entry["created_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            if entry.get("created_at")
            else "?"
        )
    except Exception:  # noqa: BLE001 - 畸形时间戳降级
        created = "?"

    lines = [
        f"# {title}",
        "",
        f"> 群: {entry.get('group_name') or '未知'}",
        f"> 生成时间: {created}",
        "",
    ]
    text = str(entry.get("chronicle_text") or "")
    if text:
        lines.append(text)
    for bio in entry.get("biographies") or []:
        if not isinstance(bio, dict):
            continue
        name = bio.get("sender_name") or bio.get("sender_id") or "未知成员"
        bio_text = str(bio.get("text") or "")
        if entry.get("biographies_embedded") or (
            bio_text and f"## 人物传记：{name}" in text and bio_text in text
        ):
            continue
        lines += ["", f"## 人物传记：{name}", "", bio_text]
    return "\n".join(lines)
