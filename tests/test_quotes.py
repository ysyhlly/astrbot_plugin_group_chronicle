"""core/quotes.py 单元测试 —— 候选金句提取规则。

运行: python -m pytest tests/test_quotes.py -v
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.quotes import extract_quotes


class FakeMsg:
    def __init__(self, content):
        self.content = content


def test_length_filters():
    """长度越界 (8~80) 的候选被剔除。"""
    msgs = [FakeMsg("短"), FakeMsg("长" * 90), FakeMsg("合格的金句候选内容")]
    quotes = extract_quotes(msgs)
    assert quotes == ["合格的金句候选内容"]


def test_pure_emoji_and_digits_excluded():
    msgs = [
        FakeMsg("😀😀😀😀😀😀😀😀😀😀"),
        FakeMsg("1234567890123"),
        FakeMsg("这是正常的一句话内容"),
    ]
    quotes = extract_quotes(msgs)
    assert quotes == ["这是正常的一句话内容"]


def test_dedupe_preserves_first_seen():
    msgs = [
        FakeMsg("重复的经典语句内容"),
        FakeMsg("另一条经典语句内容"),
        FakeMsg("重复的经典语句内容"),
    ]
    quotes = extract_quotes(msgs)
    assert len(quotes) == 2
    assert quotes.count("重复的经典语句内容") == 1


def test_scoring_prefers_longer_and_unique():
    """更长 + 更独特的候选分数更高, 排前面。"""
    msgs = [
        FakeMsg("短句"),  # 长度不足被剔除
        FakeMsg("这是足够长的独特金句候选内容"),
        FakeMsg("这是一句普通句子内容"),
    ]
    quotes = extract_quotes(msgs)
    assert quotes[0] == "这是足够长的独特金句候选内容"
    assert quotes[1] == "这是一句普通句子内容"


def test_max_quotes_cap():
    msgs = [FakeMsg(f"候选金句内容编号{i:02d}长度足够") for i in range(10)]
    assert len(extract_quotes(msgs, max_quotes=3)) == 3


def test_dict_messages_supported():
    msgs = [{"content": "字典形式的消息内容"}, {"text": "text 键的消息内容"}]
    quotes = extract_quotes(msgs)
    assert len(quotes) == 2


def test_empty_and_junk_returns_empty():
    assert extract_quotes([]) == []
    assert extract_quotes([FakeMsg(""), FakeMsg("   "), None, 42]) == []
