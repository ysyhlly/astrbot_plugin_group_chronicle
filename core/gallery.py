"""群史记 gallery —— 群友图鉴 + 本周黑话。

职责:
- build_gallery: 为影响力 Top-N 成员生成「群友图鉴」分节 (一次 LLM 调用),
  输出 `名称｜称号｜一句话标签` 行; 发言不足的成员跳过; 任何失败返回 ""。
- append_slang: 收集「本周黑话」条目并追加到生成证据文本末尾 (enable_slang)。
  来源: 结构化 dict 的 slang / 记忆文本含 梗·黑话 关键词者 / self_learning
  公开黑话查询方法 (软读取, 绝不直接读 SQLite)。

设计约束:
- 只依赖 core.llm_call / core.influence._resolve_star_instance (均无 astrbot
  硬依赖), 单元测试可在无 astrbot 环境下运行。
- 绝不崩溃: 全部外部访问 try/except 包裹, 失败返回 "" / 原样文本。
"""

import inspect

from astrbot.api import logger

from .influence import _SL_ALIASES, _resolve_star_instance
from .llm_call import call_llm

#: 图鉴生成系统提示词: 严格按「名称｜称号｜一句话标签」逐行输出
GALLERY_SYSTEM_PROMPT = """你是「群史记」的图鉴作者。请基于提供的群聊材料，为下列每位成员写一行「群友图鉴」条目。
格式严格为（每行一条，不要输出任何其他文字）：
名称｜称号｜一句话标签
例如：张三｜夜猫子之王｜凌晨三点还在群里发美食图
要求：称号与标签须贴合材料中的表现，只基于材料，不编造。"""

#: 图鉴成员最少发言条数 (不足跳过, 避免给潜水成员硬编标签)
GALLERY_MIN_MSGS = 5
#: 图鉴素材行上限 / 单行截断
_GALLERY_MAX_LINES = 60
_GALLERY_MAX_LEN = 100

#: 黑话关键词 (记忆文本命中即视为黑话线索)
_SLANG_KEYWORDS = ("梗", "黑话")
#: 单条黑话线索最长截断
_SLANG_MAX_CHARS = 60
#: self_learning 黑话查询方法候选 (公共 API, 守卫调用)
_JARGON_METHOD_NAMES = ("get_jargon", "query_jargon", "get_slang_terms")


# ---------- 群友图鉴 ----------


def _member_msg_count(evidence, member: dict) -> int:
    """统计成员在 chronicle 窗口内的发言条数 (sender_id 优先, 昵称回退)。"""
    sid = str(member.get("sender_id") or "")
    sname = member.get("sender_name")
    count = 0
    for m in getattr(evidence, "chronicle_msgs", None) or []:
        m_sid = str(getattr(m, "sender_id", "") or "")
        if sid and m_sid == sid:
            count += 1
        elif (
            (not sid or not m_sid)
            and sname
            and str(getattr(m, "sender_name", "") or "") == str(sname)
        ):
            count += 1
    return count


def _compact_evidence(
    evidence, max_lines: int = _GALLERY_MAX_LINES, max_len: int = _GALLERY_MAX_LEN
) -> str:
    """把证据压缩为图鉴素材 (与 generation.compress_evidence 独立的轻量实现)。"""
    lines: list[str] = []
    for m in (getattr(evidence, "chronicle_msgs", None) or [])[-max_lines:]:
        name = getattr(m, "sender_name", None) or getattr(m, "sender_id", None) or "?"
        content = str(getattr(m, "content", "") or "").strip()[:max_len]
        if not content:
            continue
        lines.append(f"[{name}] {content}")
        if len(lines) >= max_lines:
            break
    if not lines:
        return "(无消息素材)"
    return "\n".join(lines)


def _parse_gallery_lines(raw: str) -> list[str]:
    """解析 LLM 输出: 过滤空行/标题行, 保留 `名称｜称号｜标签` 形态的行。"""
    lines: list[str] = []
    for line in str(raw or "").splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith(("##", "#", "-", "*")):
            s = s.lstrip("#*- ").strip()
        if not s:
            continue
        if "｜" not in s and "|" not in s:
            # 允许单行缺分隔符 (LLM 偶发), 但至少要有实质内容
            if len(s) < 3:
                continue
        lines.append(s)
    return lines


async def build_gallery(
    context,
    umo: str,
    evidence,
    members: list,
    provider_id: str | None = None,
) -> str:
    """为影响力 Top-N 成员生成「群友图鉴」分节。

    Args:
        context: AstrBot 插件上下文。
        umo: unified_msg_origin 会话标识。
        evidence: EvidenceBundle。
        members: 影响力排行 [{sender_id, sender_name, ...}]。
        provider_id: 可选 LLM Provider ID。

    Returns:
        Markdown 分节 (含 `## 群友图鉴`); 成员不足/LLM 失败返回 "" (绝不抛异常)。
    """
    if not members:
        return ""
    active = [m for m in members if _member_msg_count(evidence, m) >= GALLERY_MIN_MSGS]
    names = [str(m.get("sender_name") or m.get("sender_id")) for m in active]
    names = [n for n in names if n.strip()]
    if not names:
        return ""
    prompt = (
        "以下是群聊材料:\n"
        f"{_compact_evidence(evidence)}\n\n"
        "请为以下成员各输出一行图鉴条目:\n" + "\n".join(f"- {n}" for n in names)
    )
    try:
        raw = await call_llm(
            context, umo, prompt, GALLERY_SYSTEM_PROMPT, provider_id=provider_id
        )
    except Exception as e:  # noqa: BLE001 - 图鉴失败静默跳过
        logger.warning(f"[gallery] 图鉴生成失败: {e}")
        return ""
    lines = _parse_gallery_lines(raw)
    if not lines:
        return ""
    return "## 群友图鉴\n" + "\n".join(f"- {line}" for line in lines)


# ---------- 本周黑话 ----------


def _slang_from_structured(structured) -> list[str]:
    """从结构化 dict 的 slang 字段提取条目 (dict 形态含含义, 字符串原样)。"""
    if not isinstance(structured, dict):
        return []
    lines: list[str] = []
    for item in structured.get("slang") or []:
        if isinstance(item, dict):
            term = str(item.get("term") or item.get("name") or "").strip()
            if not term:
                continue
            meaning = str(item.get("meaning") or item.get("explain") or "").strip()
            lines.append(f"{term}：{meaning}" if meaning else term)
        else:
            s = str(item or "").strip()
            if s:
                lines.append(s)
    return lines


def _slang_from_memories(evidence) -> list[str]:
    """从记忆文本中收集含 梗/黑话 关键词的线索 (截断)。"""
    lines: list[str] = []
    for mem in getattr(evidence, "memories", None) or []:
        if not isinstance(mem, dict):
            continue
        text = str(mem.get("text", "") or "").strip()
        if not text:
            continue
        if any(kw in text for kw in _SLANG_KEYWORDS):
            snippet = (
                text if len(text) <= _SLANG_MAX_CHARS else text[:_SLANG_MAX_CHARS] + "…"
            )
            lines.append(snippet)
    return lines


def _jargon_to_lines(result) -> list[str]:
    """把 self_learning 黑话查询结果规范化为条目行 (dict / 列表 / 其他)。"""
    lines: list[str] = []
    if isinstance(result, dict):
        for k, v in result.items():
            term = str(k or "").strip()
            if not term:
                continue
            meaning = str(v or "").strip()
            lines.append(f"{term}：{meaning}" if meaning and meaning != term else term)
    elif isinstance(result, (list, tuple)):
        for item in result:
            if isinstance(item, dict):
                term = str(
                    item.get("term") or item.get("name") or item.get("jargon") or ""
                ).strip()
                if not term:
                    continue
                meaning = str(item.get("meaning") or item.get("explain") or "").strip()
                lines.append(f"{term}：{meaning}" if meaning else term)
            else:
                s = str(item or "").strip()
                if s:
                    lines.append(s)
    return lines


async def _soft_read_jargon(context) -> list[str]:
    """经注册星软读取 self_learning 的黑话数据 (仅公开查询方法)。

    绝不直接读 SQLite; 插件缺失 / 无公开方法 / 调用失败一律跳过, 返回空列表。
    """
    if context is None:
        return []
    try:
        plugin = _resolve_star_instance(context, _SL_ALIASES)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[gallery] 定位 self_learning 失败, 跳过黑话读取: {e}")
        return []
    if plugin is None:
        return []
    lines: list[str] = []
    for name in _JARGON_METHOD_NAMES:
        method = getattr(plugin, name, None)
        if not callable(method):
            continue
        try:
            result = method()
            if inspect.isawaitable(result):
                result = await result
        except Exception as e:  # noqa: BLE001 - 单方法失败跳过
            logger.warning(f"[gallery] 读取黑话方法 {name} 失败: {e}")
            continue
        lines.extend(_jargon_to_lines(result))
    return lines


async def append_slang(
    evidence_text: str,
    evidence,
    structured: dict | None,
    context,
    enabled: bool,
) -> str:
    """启用时把「## 本周黑话」分节追加到证据文本末尾; 无内容时原样返回。

    Args:
        evidence_text: 生成证据文本 (compress_evidence 输出)。
        evidence: EvidenceBundle (扫描记忆文本)。
        structured: extract_structured 输出 (可为 None/空 dict)。
        context: AstrBot 插件上下文 (软读取 self_learning 黑话)。
        enabled: enable_slang 配置。

    Returns:
        追加了黑话分节的证据文本; 未启用或无黑话时原样返回。
    """
    if not enabled:
        return evidence_text
    lines = _slang_from_structured(structured)
    lines += _slang_from_memories(evidence)
    try:
        lines += await _soft_read_jargon(context)
    except Exception as e:  # noqa: BLE001 - 黑话软读取失败不影响主流程
        logger.warning(f"[gallery] 黑话软读取失败: {e}")
    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if line not in seen:
            seen.add(line)
            deduped.append(line)
    if not deduped:
        return evidence_text
    section = "\n\n## 本周黑话\n" + "\n".join(
        f"- {line[:120]}" for line in deduped[:30]
    )
    return (evidence_text + section)[:12000]
