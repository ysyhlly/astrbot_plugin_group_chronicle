"""群史记 structured —— 结构化输出提取与确定性渲染。

职责:
- extract_structured: 调 LLM 把压缩证据提取为严格 JSON (事件/金句/新面孔/
  黑话/总览), 供确定性渲染使用 (enable_structured 配置)。
- parse_structured_json: 解析 LLM 返回文本 (兼容 ```json 围栏 / 前后杂文),
  任何失败返回 {} (绝不抛异常)。
- render_structured_markdown: 把结构化 dict 渲染为确定性 Markdown 史记。

设计约束:
- 本模块只依赖 core.llm_call (同样无 astrbot 硬依赖), 单元测试可在
  无 astrbot 环境下用假 context / monkeypatch 运行。
- 绝不崩溃: 解析失败 / LLM 调用失败一律返回 {} 或空串, 由调用方决定回退。
"""

import json
import re

from astrbot.api import logger

from .llm_call import call_llm


#: 结构化提取的系统提示词: 只允许输出一个合法 JSON 对象
_STRUCTURED_SYSTEM_PROMPT = """你是「群史记」的结构化史官。请基于提供的群聊材料，只输出一个严格合法的 JSON 对象（不要输出任何其他文字，不要使用 Markdown 代码块），格式如下：
{
  "events": [{"date": "YYYY-MM-DD", "title": "事件标题", "detail": "一两句话描述", "members": ["成员昵称"]}],
  "quotes": ["金句1", "金句2"],
  "new_members": ["新面孔昵称"],
  "slang": ["梗/黑话 及其含义"],
  "overall": "一段话概括本期群史记"
}
要求：只基于提供的材料，不编造；没有的字段给空列表；overall 用简体中文。"""

#: 代码围栏行 (``` / ```json)
_FENCE_RE = re.compile(r"^```(?:json)?\s*$", re.IGNORECASE)
#: 文本中第一个 JSON 对象 (含花括号与内嵌引号)
_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)

#: 结构化渲染的分节标题
_SECTION_EVENTS = "## 大事记"
_SECTION_QUOTES = "## 本期金句"
_SECTION_NEW_MEMBERS = "## 新面孔"
_SECTION_SLANG = "## 本周黑话"


def _strip_fences(raw: str) -> str:
    """去除 ```json ... ``` 围栏与首尾空白 (兼容 LLM 常见的代码块包装)。"""
    lines = str(raw).strip().splitlines()
    if lines and _FENCE_RE.match(lines[0].strip()):
        lines = lines[1:]
    if lines and _FENCE_RE.match(lines[-1].strip()):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def parse_structured_json(raw) -> dict:
    """解析 LLM 返回的 JSON; 任何失败返回 {} (绝不抛异常)。

    兼容: 直接 JSON / ```json 围栏 / 前后夹带说明文字 (截取首个 {…} 对象)。
    """
    if not raw or not isinstance(raw, str):
        return {}
    text = _strip_fences(raw)
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        match = _JSON_RE.search(text)
        if not match:
            return {}
        try:
            data = json.loads(match.group(0))
        except Exception:
            return {}
    if not isinstance(data, dict):
        return {}
    # 规范化关键字段 (畸形数据一律转成安全形态, 后续渲染绝不崩)
    normalized = {key: [] for key in ("events", "quotes", "new_members", "slang")}
    for key in normalized:
        values = data.get(key)
        if not isinstance(values, list):
            continue
        for value in values[:50]:
            if key == "events":
                if not isinstance(value, dict) or not isinstance(
                    value.get("title"), str
                ):
                    continue
                members = value.get("members")
                normalized[key].append(
                    {
                        "date": str(value.get("date") or "")[:40],
                        "title": value["title"][:200],
                        "detail": str(value.get("detail") or "")[:1200],
                        "members": [m[:100] for m in members[:30] if isinstance(m, str)]
                        if isinstance(members, list)
                        else [],
                    }
                )
            elif isinstance(value, str) and value.strip():
                normalized[key].append(value.strip()[:800])
    overall = data.get("overall")
    normalized["overall"] = overall.strip()[:3000] if isinstance(overall, str) else ""
    return normalized


async def extract_structured(
    context,
    umo: str,
    evidence_text: str,
    provider_id: str | None = None,
) -> dict:
    """调用 LLM 提取结构化事件/金句/新面孔/黑话/总览。

    Args:
        context: AstrBot 插件上下文。
        umo: unified_msg_origin 会话标识。
        evidence_text: 压缩后的证据文本 (compress_evidence 输出)。
        provider_id: 可选 LLM Provider ID; 留空用会话默认。

    Returns:
        规范化 dict; 任何失败 (LLM 不可用 / 解析失败) 返回 {} (绝不抛异常)。
    """
    prompt = f"以下是群聊材料:\n{evidence_text}\n\n请只输出上述要求的 JSON 对象。"
    try:
        raw = await call_llm(
            context, umo, prompt, _STRUCTURED_SYSTEM_PROMPT, provider_id=provider_id
        )
    except Exception as e:  # noqa: BLE001 - 结构化提取失败必须静默降级
        logger.warning(f"[structured] 提取失败, 返回空: {e}")
        return {}
    return parse_structured_json(raw)


def render_structured_markdown(structured: dict, window_days: int) -> str:
    """把结构化 dict 渲染为确定性 Markdown 史记。

    Args:
        structured: extract_structured 的输出 (规范化 dict)。
        window_days: 时间窗天数 (仅用于日志, 不参与渲染)。

    Returns:
        Markdown 文本; 空/畸形 dict 返回 "" (调用方回退自由散文)。
    """
    if not isinstance(structured, dict) or not structured:
        return ""
    structured = parse_structured_json(json.dumps(structured, ensure_ascii=False))

    blocks: list[str] = []  # 每个块 = 标题行 + 条目行 (单换行连接), 块间空行

    overall = str(structured.get("overall") or "").strip()
    if overall:
        blocks.append(overall)

    events = structured.get("events") or []
    if events:
        lines = [_SECTION_EVENTS]
        for ev in events:
            if not isinstance(ev, dict):
                continue
            title = str(ev.get("title") or "").strip()
            if not title:
                continue
            date = str(ev.get("date") or "").strip()
            detail = str(ev.get("detail") or "").strip()
            members = [
                str(m) for m in (ev.get("members") or []) if str(m or "").strip()
            ]
            line = f"- **{date}** {title}" if date else f"- {title}"
            if detail:
                line += f"：{detail}"
            if members:
                line += f"（参与者：{'、'.join(members)}）"
            lines.append(line)
        blocks.append("\n".join(lines))

    quotes = [
        str(q).strip() for q in (structured.get("quotes") or []) if str(q or "").strip()
    ]
    if quotes:
        blocks.append(_SECTION_QUOTES + "\n" + "\n".join(f"- {q}" for q in quotes))

    new_members = [
        str(n).strip()
        for n in (structured.get("new_members") or [])
        if str(n or "").strip()
    ]
    if new_members:
        blocks.append(
            _SECTION_NEW_MEMBERS + "\n" + "\n".join(f"- {n}" for n in new_members)
        )

    slang = [
        str(s).strip() for s in (structured.get("slang") or []) if str(s or "").strip()
    ]
    if slang:
        blocks.append(_SECTION_SLANG + "\n" + "\n".join(f"- {s}" for s in slang))

    return "\n\n".join(blocks)
