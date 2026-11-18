"""人物传记生成 —— 为指定群成员基于聊天记录与记忆素材生成 Markdown 传记。

职责边界:
    - 只负责把成员发言 + 相关长期记忆组装成严格"基于证据"的 prompt,
      并调用共享的 :func:`core.llm_call.call_llm` 生成传记;
    - 绝不编造: prompt 明确要求"不得编造", 材料不足部分如实说明;
    - 成员在窗口内无发言记录时**不调用 LLM**, 直接返回友好中文提示;
    - LLM 调用逻辑在 core/llm_call.py (共享), 本模块不重复实现。

参考: astrbot_plugin_portrayal 的 evidence-based 画像 prompt 风格 ——
必须基于聊天记录、分节输出、不虚构。
"""

import re
import time
from datetime import datetime

from .llm_call import call_llm

#: 每成员最多纳入 prompt 的发言条数(保留最近 N 条)
BIO_MAX_MSGS_PER_SENDER = 8
#: 单条发言内容截断长度
BIO_MAX_CONTENT_CHARS = 200
#: 最多纳入的相关长期记忆条数
BIO_MAX_MEMORIES = 10

BIO_PROMPT_TEMPLATE = """你是「群史记」的传记作者。请为群成员「{name}」撰写一份人物传记。要求：
1. 严格基于以下聊天记录与记忆素材，不得编造任何内容；
2. 分节输出：群内角色 / 高光时刻 / 性格侧面 / 名言与名场面；
3. 如某部分材料不足，如实说明"材料不足"；
4. 输出 Markdown 格式。"""

#: system_prompt 未指定时的中立传记作者提示词
NEUTRAL_BIOGRAPHER_PROMPT = """你是一位中立客观的群聊传记作者。你只依据提供的聊天记录与记忆素材写作，绝不虚构、不夸大、不贬损；某部分材料不足时如实说明"材料不足"。输出使用 Markdown 格式。"""

#: 人物传记文风指令 (chronicle_style 配置 / 风格= 命令参数), 追加到系统提示词末尾
BIO_STYLE_INSTRUCTIONS = {
    "official": "文风要求：正史列传笔法，客观庄重，如实记载。",
    "gossip": "文风要求：八卦闲聊笔法，轻松幽默，突出趣味细节与名场面。",
    "classical": "文风要求：文言笔法，古雅凝练，类古人小传。",
}


def _member_matches(m, member: dict) -> bool:
    """成员匹配: 优先 sender_id; sender_id 不匹配时回退 sender_name。"""
    sid = member.get("sender_id")
    sname = member.get("sender_name")
    m_sid = getattr(m, "sender_id", None)
    m_name = getattr(m, "sender_name", None)
    if sid is not None and m_sid is not None:
        return str(m_sid) == str(sid)
    if sname is not None and str(m_name or "") == str(sname):
        return True
    return False


def _msg_ts(m) -> float:
    """提取消息时间戳 (epoch float); 畸形数据按 0.0 处理。"""
    try:
        return float(getattr(m, "timestamp", 0.0))
    except (TypeError, ValueError):
        return 0.0


def _in_window(ts: float, window_days: int) -> bool:
    return 0 <= (time.time() - ts) <= window_days * 86400


def _format_msg(m, member: dict) -> str:
    """格式化单条发言: [{sender_name}|{MM-DD HH:MM}] {content(截断 200 字)}"""
    sender_name = getattr(m, "sender_name", None) or member.get("sender_name") or "未知"
    ts = _msg_ts(m)
    time_str = datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")
    content = str(getattr(m, "content", "") or "")[:BIO_MAX_CONTENT_CHARS]
    return f"[{sender_name}|{time_str}] {content}"


async def build_biography_prompt(
    member: dict, evidence, window_days: int
) -> str | None:
    """组装传记 prompt。

    Args:
        member: {"sender_id": str, "sender_name": str}。
        evidence: EvidenceBundle (biography_msgs / memories)。
        window_days: 传记时间窗(天), 用于二次防御性时间过滤。

    Returns:
        组装好的 prompt 字符串; 成员在窗口内无发言记录时返回 None
        (由调用方处理"该成员在窗口内无发言记录")。
    """
    name = member.get("sender_name") or member.get("sender_id") or "未知成员"

    bio_msgs = [
        m
        for m in (getattr(evidence, "biography_msgs", None) or [])
        if _member_matches(m, member) and _in_window(_msg_ts(m), window_days)
    ]
    if not bio_msgs:
        return None

    # 时间升序, 每成员最多保留最近 BIO_MAX_MSGS_PER_SENDER 条
    bio_msgs.sort(key=_msg_ts)
    bio_msgs = bio_msgs[-BIO_MAX_MSGS_PER_SENDER:]

    # 相关长期记忆: text 包含 sender_name 或 sender_id, 最多 BIO_MAX_MEMORIES 条
    sid = member.get("sender_id")
    sname = member.get("sender_name")
    same_name_ids = {
        str(getattr(m, "sender_id", "") or "")
        for m in (getattr(evidence, "biography_msgs", None) or [])
        if sname
        and getattr(m, "sender_name", None) == sname
        and getattr(m, "sender_id", None)
    }
    ambiguous_name = bool(sid and same_name_ids - {str(sid)})
    memories: list[str] = []
    for mem in getattr(evidence, "memories", None) or []:
        if not isinstance(mem, dict):
            continue
        text = str(mem.get("text", "") or "")
        id_matches = bool(
            sid
            and re.search(
                r"(?<![A-Za-z0-9_])" + re.escape(str(sid)) + r"(?![A-Za-z0-9_])", text
            )
        )
        if id_matches or (sname and not ambiguous_name and sname in text):
            memories.append(text[:800])
        if len(memories) >= BIO_MAX_MEMORIES:
            break

    msg_lines = [_format_msg(m, member) for m in bio_msgs]
    mem_lines = [f"[记忆] {t}" for t in memories]

    material = (
        "\n\n以下是该成员在群内的聊天记录与相关记忆素材(仅作素材, 按时间升序):\n\n"
        "--- 聊天记录开始 ---\n" + "\n".join(msg_lines) + "\n--- 聊天记录结束 ---\n\n"
        "--- 相关长期记忆 ---\n"
        + ("\n".join(mem_lines) if mem_lines else "(无相关长期记忆)")
        + "\n"
    )
    return BIO_PROMPT_TEMPLATE.format(name=name) + material


async def generate_biography(
    context,
    umo: str,
    evidence,
    member: dict,
    system_prompt: str | None,
    window_days: int,
    provider_id: str | None = None,
    *,
    style: str | None = None,
    prompt_override: str | None = None,
) -> str:
    """为成员生成人物传记文本。

    成员在窗口内无发言记录时直接返回友好提示, 不调用 LLM。

    Args:
        context: AstrBot 插件上下文 (透传给 call_llm)。
        umo: unified_msg_origin 会话标识。
        evidence: EvidenceBundle。
        member: {"sender_id": str, "sender_name": str}。
        system_prompt: 系统提示词; None 时使用中立传记作者提示词。
        window_days: 传记时间窗(天)。
        provider_id: 可选 LLM Provider ID; 留空用会话默认。
        style: 文风 (official/gossip/classical), 未知值忽略。
        prompt_override: 自定义完整系统提示词 (优先于 system_prompt/中立提示词)。

    Returns:
        传记 Markdown 文本, 或"该成员在窗口内无发言记录"友好提示。

    Raises:
        LLMUnavailableError: 未配置可用的 LLM (由 call_llm 抛出, 不吞掉)。
    """
    prompt = await build_biography_prompt(member, evidence, window_days)
    if prompt is None:
        return "该成员在窗口内无发言记录，暂无法生成传记。"
    sp = prompt_override or system_prompt or NEUTRAL_BIOGRAPHER_PROMPT
    note = BIO_STYLE_INSTRUCTIONS.get(style or "")
    if note:
        sp = f"{sp}\n\n{note}"
    return await call_llm(context, umo, prompt, sp, provider_id=provider_id)
