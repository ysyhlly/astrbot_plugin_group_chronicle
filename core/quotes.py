"""群史记 quotes —— 候选金句提取。

从群聊消息中挑选"金句候选"注入 chronicle prompt (候选金句 分节),
供 LLM 在撰写史记时酌情引用。

规则:
- 候选长度限制: 8 ~ 80 字符 (太短无信息量, 太长不是"金句")。
- 纯 emoji / 纯数字内容剔除。
- 打分 = 长度得分 (max 60 字封顶) + 唯一性得分 (重复次数越多越低),
  取 Top max_quotes, 自动去重 (保留首次出现顺序)。

设计约束: 纯标准库, 不依赖 astrbot 与任何第三方库。
"""

import re

#: 常见 emoji 区段 (含变体选择符与零宽连接符)
_EMOJI_RE = re.compile(r"[\U0001F000-\U0001FAFF\u2600-\u27BF\uFE0F\u200D]")

#: 金句长度边界
QUOTE_MIN_LEN = 8
QUOTE_MAX_LEN = 80

#: 长度得分封顶字符数 (超过即视为"足够长", 不再加分)
_SCORE_LEN_CAP = 60


def _is_pure_emoji(text: str) -> bool:
    """剔除 emoji 后无剩余可见字符 → 视为纯 emoji。"""
    return not _EMOJI_RE.sub("", text).strip()


def _message_content(m) -> str:
    """提取消息纯文本内容 (dict / 对象均可, 失败返回空串)。"""
    if isinstance(m, dict):
        raw = m.get("content") or m.get("text") or ""
    else:
        raw = getattr(m, "content", None) or ""
    return str(raw).strip()


def extract_quotes(
    msgs,
    max_quotes: int = 5,
    min_len: int = QUOTE_MIN_LEN,
    max_len: int = QUOTE_MAX_LEN,
) -> list[str]:
    """从消息列表提取金句候选。

    Args:
        msgs: 消息列表 (dict 或带 content 属性的对象)。
        max_quotes: 返回候选上限。
        min_len / max_len: 候选长度边界。

    Returns:
        按分数降序的候选金句列表 (自动去重, 无候选时返回空列表)。
    """
    candidates: list[str] = []
    for m in msgs:
        content = _message_content(m)
        if not content:
            continue
        if not (min_len <= len(content) <= max_len):
            continue
        if _is_pure_emoji(content):
            continue
        if content.isdigit():
            continue
        candidates.append(content)

    if not candidates:
        return []

    # 唯一性: 内容出现次数 (仅对候选集内部统计)
    freq: dict[str, int] = {}
    for c in candidates:
        freq[c] = freq.get(c, 0) + 1

    def _score(c: str) -> float:
        length_score = min(len(c), _SCORE_LEN_CAP) / float(_SCORE_LEN_CAP)
        uniqueness_score = 1.0 / (1.0 + freq[c])
        return length_score + uniqueness_score

    # 去重并保序, 再按分数降序取 top
    unique: list[str] = list(dict.fromkeys(candidates))
    unique.sort(key=_score, reverse=True)
    return unique[:max_quotes]
