"""群史记 LLM 生成 —— 证据压缩 + token 预算 + 统一 LLM 调用。

职责:
- compress_evidence: 把 EvidenceBundle.chronicle_msgs 压缩为带预算的文本证据
  (每条消息一行 `[昵称|时间] 内容`, 按发送者限条数, 内容截断, 总 token 预算),
  并附上长期记忆片段与图谱线索。
- generate_chronicle: 组装 prompt (时间窗 + 压缩证据) 并调用共享助手
  core.llm_call.call_llm 完成生成; 支持文风 (style)、自定义提示词
  (prompt_override)、两阶段摘要 (enable_two_phase) 与候选金句注入。
- generate_continuation: 基于上一期史记 + 新证据生成续篇 (史记续写 命令)。

设计约束:
- 绝不把原始消息未经压缩直接交给 LLM (token 预算兜底)。
- 本模块只依赖 core.llm_call / core.quotes (同样无 astrbot 硬依赖), 单元测试
  可用假 context / monkeypatch 在无 astrbot 环境下运行。
"""

import logging
import re
from datetime import datetime

from .gallery import append_slang
from .llm_call import call_llm
from .quotes import extract_quotes
from .structured import extract_structured, render_structured_markdown

logger = logging.getLogger(__name__)

NEUTRAL_HISTORIAN_PROMPT = """你是「群史记」的史官。请基于以下群聊历史记录与长期记忆，写一篇本周群史记。要求：
1. 按时间线组织，涵盖重要事件、名场面、热点话题；
2. 提及主要参与成员（用昵称）；
3. 语言生动但尊重事实，只基于提供的材料，不得编造；
4. 输出 Markdown 格式，包含标题与分节。"""

#: 群史记文风指令 (chronicle_style 配置 / 风格= 命令参数), 追加到系统提示词末尾
CHRONICLE_STYLE_INSTRUCTIONS = {
    "official": "文风要求：正史笔法，客观严谨，庄重典雅，按时间顺序叙事，用词克制。",
    "gossip": "文风要求：八卦闲聊笔法，轻松活泼、幽默生动，突出名场面、梗与趣味细节。",
    "classical": "文风要求：文言文笔法，用词古雅，句式凝练，可适度化用典故，但仍须基于材料。",
}

_TIME_FORMAT = "%m-%d %H:%M"


def _estimate_tokens(text: str) -> int:
    """CJK 近似 token 估算: 每 2 个字符约 1 token, 每条线加 1 换行开销。"""
    return max(1, len(text) // 2 + 1)


def _msg_sender(m) -> str:
    """提取发送者昵称 (优先 sender_name, 回退 sender_id / 未知)。"""
    name = getattr(m, "sender_name", None)
    if name:
        return str(name)
    sid = getattr(m, "sender_id", None)
    if sid is not None:
        return str(sid)
    return "?"


def _msg_time(m) -> str:
    """格式化消息时间戳 (epoch float -> %m-%d %H:%M), 非法时间戳降级。"""
    ts = getattr(m, "timestamp", None)
    if ts is None:
        return "??-?? ??:??"
    try:
        return datetime.fromtimestamp(float(ts)).strftime(_TIME_FORMAT)
    except Exception:  # noqa: BLE001 - 非法时间戳降级, 不影响整体压缩
        return "??-?? ??:??"


def compress_evidence(
    evidence,
    max_tokens: int = 6000,
    per_sender_cap: int = 8,
    max_msg_len: int = 200,
) -> str:
    """把证据压缩为适合送入 LLM 的文本。

    Args:
        evidence: EvidenceBundle (或任意含 chronicle_msgs / memories /
            graph_snapshot 属性的对象)。
        max_tokens: 消息行总 token 预算 (CJK 近似 len//2)。
        per_sender_cap: 每个发送者最多保留的消息条数 (保留最近的)。
        max_msg_len: 单条消息内容最大字符数, 超出截断。

    Returns:
        压缩后的证据文本 (str)。
    """
    msgs = getattr(evidence, "chronicle_msgs", None) or []
    lines: list[str] = []
    total_tokens = 0
    message_budget = (
        int(max_tokens * 0.7) if getattr(evidence, "memories", None) else max_tokens
    )

    # 每发送者最多 per_sender_cap 条, 保留最近的 (消息本身时间升序, 故逆序取)
    per_sender_count: dict = {}
    for m in reversed(msgs):
        sender = _msg_sender(m)
        identity = str(getattr(m, "sender_id", None) or sender)
        if per_sender_count.get(identity, 0) >= per_sender_cap:
            continue
        per_sender_count[identity] = per_sender_count.get(identity, 0) + 1

        content = str(getattr(m, "content", "") or "")[:max_msg_len]
        line = f"[{sender}|{_msg_time(m)}] {content}"
        line_tokens = _estimate_tokens(line)
        if total_tokens + line_tokens > message_budget:
            break
        lines.append(line)
        total_tokens += line_tokens
    lines.reverse()  # 恢复时间升序

    parts = lines

    # 长期记忆片段 (memories 为 dict 列表, 取 'text' 键, 全程守卫)
    memories = getattr(evidence, "memories", None) or []
    memory_lines = []
    remaining_chars = max(
        0, 2 * (max(1, int(max_tokens)) - 1) - len("\n".join(parts)) - 20
    )
    for mem in memories:
        if not isinstance(mem, dict):
            continue
        text = mem.get("text")
        if not text or not str(text).strip():
            continue
        if remaining_chars < 4:
            break
        line = f"- {str(text).strip()[: min(1200, remaining_chars - 1)]}"
        memory_lines.append(line)
        remaining_chars -= len(line) + 1
    if memory_lines:
        parts.append("## 长期记忆片段")
        parts.extend(memory_lines)

    # 图谱线索 (graph_snapshot 为 dict, 守卫)
    graph_note = _format_graph_note(getattr(evidence, "graph_snapshot", None))
    if graph_note:
        parts.append("## 图谱线索")
        parts.append(graph_note)

    return "\n".join(parts)[: max(0, 2 * (max(1, int(max_tokens)) - 1))]


def _format_graph_note(graph_snapshot) -> str | None:
    """从图谱快照提取节点/边数描述; 缺失或异常时返回 None。"""
    if not isinstance(graph_snapshot, dict):
        return None
    try:
        nodes = graph_snapshot.get("nodes")
        edges = graph_snapshot.get("edges")
        if isinstance(nodes, (list, tuple, dict)):
            nodes = len(nodes)
        if isinstance(edges, (list, tuple, dict)):
            edges = len(edges)
        if nodes is None and edges is None:
            return None
        return f"- nodes: {nodes} / edges: {edges}"
    except Exception as e:  # noqa: BLE001 - 图谱数据异常不影响主流程
        logger.warning(f"格式化图谱线索失败: {e}")
        return None


def _day_bucket_text(evidence, max_msg_len: int = 120, per_day_cap: int = 20) -> str:
    """把 chronicle_msgs 按自然日归类, 每行 `YYYY-MM-DD: 内容 / 内容...`。

    两阶段模式阶段 1 的输入: 相比 compress_evidence 更紧凑, 适合"按天压缩"。
    """
    msgs = getattr(evidence, "chronicle_msgs", None) or []
    buckets: dict[str, list[str]] = {}
    for m in msgs:
        ts = getattr(m, "timestamp", None)
        if ts is None:
            continue
        try:
            day = datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001 - 畸形时间戳跳过该消息
            continue
        content = str(getattr(m, "content", "") or "").strip()[:max_msg_len]
        if not content:
            continue
        buckets.setdefault(day, []).append(content)
    lines = []
    for day in sorted(buckets):
        lines.append(f"{day}: " + " / ".join(buckets[day][:per_day_cap]))
    return "\n".join(lines)[:12000]


def _append_style(prompt: str | None, style: str | None) -> str | None:
    """把文风指令追加到系统提示词末尾; style 未知时原样返回。"""
    note = CHRONICLE_STYLE_INSTRUCTIONS.get(style or "")
    if not note:
        return prompt
    return f"{prompt or ''}\n\n{note}".strip() if prompt else note


def _append_quotes(evidence_text: str, evidence, enabled: bool) -> str:
    """启用候选金句且存在候选时, 在证据文本末尾追加「候选金句」分节。"""
    if not enabled:
        return evidence_text
    quotes = extract_quotes(getattr(evidence, "chronicle_msgs", None) or [])
    if not quotes:
        return evidence_text
    section = "\n\n## 候选金句\n" + "\n".join(f"- {q}" for q in quotes)
    return evidence_text + section


def _append_topic(prompt: str, topic: str | None) -> str:
    """把「本期主题」指令追加到用户侧 prompt 末尾; topic 空时原样返回。"""
    if not topic or not str(topic).strip():
        return prompt
    return f"{prompt}\n\n本期主题：{topic}，请围绕其展开。"


async def generate_chronicle(
    context,
    umo: str,
    evidence,
    system_prompt: str | None,
    window_days: int,
    provider_id: str | None = None,
    *,
    style: str | None = None,
    prompt_override: str | None = None,
    enable_two_phase: bool = False,
    enable_golden_quotes: bool = True,
    enable_structured: bool = False,
    enable_slang: bool = True,
    topic: str | None = None,
) -> str:
    """生成群史记正文。

    Args:
        context: AstrBot 插件上下文 (传给 call_llm)。
        umo: unified_msg_origin 会话标识。
        evidence: EvidenceBundle 证据。
        system_prompt: 史官人设系统提示词; None 时使用 NEUTRAL_HISTORIAN_PROMPT。
        window_days: 时间窗天数, 写入 prompt。
        provider_id: 可选 LLM Provider ID; 留空用会话默认。
        style: 文风 (official/gossip/classical), 未知值忽略。
        prompt_override: 自定义完整系统提示词 (优先于 system_prompt/NEUTRAL)。
        enable_two_phase: 两阶段模式 —— 先按天压缩为要点, 再据要点成文。
        enable_golden_quotes: 是否注入「候选金句」分节。
        enable_structured: 结构化模式 —— 先提取结构化 dict, 再确定性渲染
            (不再走自由散文 LLM 调用)。
        enable_slang: 是否注入「本周黑话」分节 (记忆/结构化/软读取)。
        topic: 本期主题 (投票选题获胜项), 追加到 prompt 要求围绕其展开。

    Returns:
        生成的群史记 Markdown 文本 (str)。

    Raises:
        LLMUnavailableError: 未配置可用的 LLM 时由 call_llm 抛出
            (结构化提取失败本身不抛出, 静默回退自由散文)。
    """
    sp = _append_style(
        prompt_override or system_prompt or NEUTRAL_HISTORIAN_PROMPT, style
    )
    evidence_text = _append_quotes(
        compress_evidence(evidence), evidence, enable_golden_quotes
    )

    # 结构化模式: 一次提取 + 确定性渲染 (失败静默回退自由散文)
    structured: dict | None = None
    if enable_structured:
        structured = await extract_structured(
            context, umo, _append_topic(evidence_text, topic), provider_id
        )
    if enable_slang:
        evidence_text = await append_slang(
            evidence_text, evidence, structured, context, True
        )

    if enable_structured and structured:
        try:
            markdown = render_structured_markdown(structured, window_days)
        except (TypeError, ValueError):
            logger.warning("Invalid structured output; falling back to prose")
            markdown = ""
        if markdown:
            if not (
                style or system_prompt or prompt_override or topic or enable_two_phase
            ):
                return markdown
            prompt = _append_topic(
                f"时间窗: 最近 {window_days} 天\n以下是已经提取的事实要点:\n"
                f"{markdown[:12000]}\n\n请据此按指定文风撰写群史记，勿增加未经证实的事实。",
                topic,
            )
            return await call_llm(context, umo, prompt, sp, provider_id=provider_id)

    if enable_two_phase:
        summary = await _summarize_days(context, umo, evidence, sp, provider_id)
        prompt = _append_topic(
            f"时间窗: 最近 {window_days} 天\n"
            f"以下是按天压缩的要点:\n{summary[:12000]}\n\n请基于要点生成群史记。",
            topic,
        )
        return await call_llm(context, umo, prompt, sp, provider_id=provider_id)

    prompt = _append_topic(
        f"时间窗: 最近 {window_days} 天\n{evidence_text}\n\n请生成群史记。",
        topic,
    )
    return await call_llm(context, umo, prompt, sp, provider_id=provider_id)


async def _summarize_days(
    context, umo: str, evidence, system_prompt: str | None, provider_id: str | None
) -> str:
    """两阶段模式阶段 1: 把按天归类的群聊记录压缩为要点列表。"""
    buckets = _day_bucket_text(evidence)
    prompt = (
        "以下是按天归类的群聊记录(仅素材):\n"
        f"{buckets}\n\n"
        "请按天压缩为要点列表，每行一条，格式「日期 | 要点」，"
        "只保留事实与重要细节，不展开写作、不评价。"
    )
    return await call_llm(context, umo, prompt, system_prompt, provider_id=provider_id)


async def generate_continuation(
    context,
    umo: str,
    previous_text: str,
    evidence,
    system_prompt: str | None,
    window_days: int,
    provider_id: str | None = None,
    *,
    style: str | None = None,
    prompt_override: str | None = None,
    enable_golden_quotes: bool = True,
) -> str:
    """基于上一期史记 + 新证据生成续篇 (史记续写 命令)。

    Args 语义与 generate_chronicle 一致, 额外:
        previous_text: 上一期史记正文 (作为续写背景)。

    Returns:
        续篇 Markdown 文本 (str)。

    Raises:
        LLMUnavailableError: 未配置可用的 LLM 时由 call_llm 抛出。
    """
    sp = _append_style(
        prompt_override or system_prompt or NEUTRAL_HISTORIAN_PROMPT, style
    )
    evidence_text = _append_quotes(
        compress_evidence(evidence), evidence, enable_golden_quotes
    )
    prompt = (
        "以下是上一期群史记(已生成, 作为续写背景):\n"
        f"{previous_text[-6000:]}\n\n"
        f"以下是上一期生成之后的新群聊记录与记忆:\n{evidence_text}\n\n"
        "请撰写本期群史记的续篇：承接上一期内容，"
        f"重点记录最近 {window_days} 天内的新进展、新事件与变化，"
        "如无重大新事件请如实说明。"
    )
    return await call_llm(context, umo, prompt, sp, provider_id=provider_id)


#: 选题候选的 LLM 系统提示词: 只输出选题列表
_TOPIC_SYSTEM_PROMPT = """你是「群史记」的选题策划。请基于提供的群聊材料，提出几个适合写成「本周群史记」主题的候选选题。
每行一条，格式「1. 选题标题」或「- 选题标题」。要求贴近材料中的热点、大事与名场面，标题简洁有吸引力。不要输出任何其他文字。"""


async def generate_topic_candidates(
    context,
    umo: str,
    evidence,
    provider_id: str | None = None,
    num_candidates: int = 3,
) -> list[str]:
    """基于证据生成 N 个本周选题候选 (史记选题 命令)。

    Args:
        context: AstrBot 插件上下文。
        umo: unified_msg_origin 会话标识。
        evidence: EvidenceBundle。
        provider_id: 可选 LLM Provider ID。
        num_candidates: 候选数量。

    Returns:
        去重后的候选列表 (最多 num_candidates); 任何失败返回 [] (绝不抛异常)。
    """
    evidence_text = compress_evidence(
        evidence, max_tokens=3000, per_sender_cap=5, max_msg_len=120
    )
    prompt = (
        f"以下是本周群聊材料:\n{evidence_text}\n\n请提出 {num_candidates} 个候选选题。"
    )
    try:
        raw = await call_llm(
            context, umo, prompt, _TOPIC_SYSTEM_PROMPT, provider_id=provider_id
        )
    except Exception as e:  # noqa: BLE001 - 选题失败返回空, 由调用方提示
        logger.warning(f"[generation] 选题生成失败: {e}")
        return []

    topics: list[str] = []
    for line in str(raw or "").splitlines():
        s = line.strip()
        s = re.sub(r"^\d+[\.、)]\s*", "", s)
        s = re.sub(r"^[-*]\s*", "", s)
        if s and s not in topics:
            topics.append(s)
    return topics[:num_candidates]


async def generate_universe_chronicle(
    context,
    umo: str,
    group_evidences: list,
    system_prompt: str | None,
    provider_id: str | None = None,
    *,
    style: str | None = None,
) -> str:
    """跨群「宇宙史记」: 一次 LLM 调用综合多群材料成文。

    Args:
        context: AstrBot 插件上下文。
        umo: unified_msg_origin 会话标识 (回复目标群)。
        group_evidences: [(group_label, evidence), ...] 逐群证据。
        system_prompt: 史官系统提示词; None 用 NEUTRAL_HISTORIAN_PROMPT。
        provider_id: 可选 LLM Provider ID。
        style: 文风 (official/gossip/classical), 未知值忽略。

    Returns:
        宇宙史记 Markdown 文本 (str)。

    Raises:
        LLMUnavailableError: 未配置可用的 LLM (由 call_llm 抛出)。
    """
    sp = _append_style(system_prompt or NEUTRAL_HISTORIAN_PROMPT, style)
    blocks: list[str] = []
    for label, evidence in group_evidences:
        blocks.append(
            f"### 群: {label}\n"
            + compress_evidence(
                evidence,
                max_tokens=max(100, 6000 // max(1, len(group_evidences))),
                per_sender_cap=5,
                max_msg_len=150,
            )
        )
    prompt = (
        "以下是多个群的群聊记录与长期记忆（各群以 ### 群: xxx 分隔）:\n"
        f"{chr(10).join(blocks)}\n\n"
        "请写一篇跨群的「宇宙史记」：综述各群同期发生的事件、热点与名场面，"
        "找出群与群之间的联动或对比，按时间线分节输出 Markdown。"
    )
    return await call_llm(context, umo, prompt, sp, provider_id=provider_id)
