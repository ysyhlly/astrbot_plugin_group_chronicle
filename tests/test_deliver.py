"""core/deliver.py 单元测试 —— 分段 / 发送路径 / 导出 markdown。

无 astrbot 环境: conftest 提供 MessageChain 桩件, deliver 模块的
Node/Nodes/Plain 守卫导入失败后为 None (默认走多段纯文本路径),
Nodes 路径通过 monkeypatch 注入假类覆盖。

运行: python -m pytest tests/test_deliver.py -v
"""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import deliver
from core.deliver import build_export_markdown, deliver_text, split_text


# ---------------------------------------------------------------------------
# 1. split_text
# ---------------------------------------------------------------------------


def test_split_short_text_returns_single():
    assert split_text("短文本", 1500) == ["短文本"]
    assert split_text("", 1500) == []


def test_split_on_paragraph_boundary():
    text = "\n\n".join(f"第{i}段内容" * 30 for i in range(10))
    segments = split_text(text, 200)
    assert len(segments) > 1
    # 每个切点都是段落边界: 段尾以 \n\n 结束 (除最后一段)
    for seg in segments[:-1]:
        assert seg.endswith("\n\n")
    assert all(len(s) <= 200 for s in segments)
    assert "".join(segments) == text  # 无损拼接


def test_split_on_newline_boundary():
    text = "\n".join(f"行{i}" * 50 for i in range(20))
    segments = split_text(text, 120)
    assert len(segments) > 1
    assert all(len(s) <= 120 for s in segments)
    assert "".join(segments) == text


def test_split_hard_cut_when_no_boundary():
    text = "字" * 500  # 无任何换行
    segments = split_text(text, 200)
    assert len(segments) == 3
    assert all(len(s) <= 200 for s in segments)
    assert "".join(segments) == text


# ---------------------------------------------------------------------------
# 发送环境
# ---------------------------------------------------------------------------


class FakeEvent:
    def __init__(self, umo="default:GroupMessage:1"):
        self.unified_msg_origin = umo
        self.sent_chains = []

    async def send(self, chain):
        self.sent_chains.append(chain)


def make_context():
    sent = []

    async def send_message(umo, chain):
        sent.append((umo, chain))

    return SimpleNamespace(send_message=send_message), sent


def run(coro):
    return asyncio.run(coro)


def plain_texts(records):
    texts = []
    for _umo, chain in records:
        for seg in chain:
            if isinstance(seg, tuple) and seg[0] == "plain":
                texts.append(str(seg[1]))
    return texts


# ---------------------------------------------------------------------------
# 2. deliver_text 发送路径
# ---------------------------------------------------------------------------


def test_deliver_short_text_single_plain():
    ctx, sent = make_context()
    ok = run(deliver_text(ctx, None, "g1", "短正文", prefer_context=True))
    assert ok is True
    assert plain_texts(sent) == ["短正文"]


def test_deliver_long_text_segments_without_forward_nodes():
    """use_forward_nodes=False (或 Nodes 不可用) → 多段纯文本 + 间隔。"""
    ctx, sent = make_context()
    long_text = "段" * 4000
    ok = run(
        deliver_text(
            ctx,
            None,
            "g1",
            long_text,
            prefer_context=True,
            use_forward_nodes=False,
            max_chars=1500,
        )
    )
    assert ok is True
    texts = plain_texts(sent)
    assert len(texts) >= 3
    assert all(len(t) <= 1500 for t in texts)
    assert "".join(texts) == long_text  # 无损


def test_deliver_forward_nodes_used_when_available(monkeypatch):
    """Node/Nodes/Plain 可用 → 单次发送合并转发。"""

    class FakeNode:
        def __init__(self, uin=None, name=None, content=None):
            self.uin, self.name, self.content = uin, name, content

    class FakeNodes:
        def __init__(self, children):
            self.children = children

        def __iter__(self):
            return iter(self.children)

    monkeypatch.setattr(deliver, "Node", FakeNode)
    monkeypatch.setattr(deliver, "Nodes", FakeNodes)
    monkeypatch.setattr(deliver, "Plain", lambda t: ("plain", t))

    ctx, sent = make_context()
    long_text = "段" * 4000
    ok = run(
        deliver_text(
            ctx,
            None,
            "g1",
            long_text,
            prefer_context=True,
            max_chars=1500,
        )
    )
    assert ok is True
    assert len(sent) == 1  # 只有一次发送
    chain = sent[0][1]
    nodes = list(chain)[0]
    assert isinstance(nodes, FakeNodes)
    assert len(nodes.children) >= 3
    # 每个节点内容都是 Plain 段, 内容无损拼接
    contents = []
    for node in nodes.children:
        assert isinstance(node, FakeNode)
        assert len(node.content) == 1
        seg = node.content[0]
        assert seg[0] == "plain"
        contents.append(seg[1])
    assert "".join(contents) == long_text
    assert contents[0].startswith("段")


def test_deliver_image_first_then_text_fallback(monkeypatch):
    """有配图: 先发图; 发图失败回退纯文本。"""
    ctx, sent = make_context()
    ok = run(deliver_text(ctx, None, "g1", "正文", "/tmp/x.png", prefer_context=True))
    assert ok is True
    chain = sent[0][1]
    assert any(seg[0] == "file_image" for seg in chain)

    # 图片发送失败 (context.send_message 抛异常) → 回退纯文本
    async def boom(umo, chain):
        raise RuntimeError("send boom")

    ctx2 = SimpleNamespace(send_message=boom)
    ok2 = run(deliver_text(ctx2, None, "g1", "正文", "/tmp/x.png", prefer_context=True))
    assert ok2 is False


def test_deliver_event_send_preferred_without_context():
    """prefer_context=False: 优先 event.send。"""
    event = FakeEvent(umo="g1")
    ok = run(deliver_text(None, event, "g1", "正文", prefer_context=False))
    assert ok is True
    assert len(event.sent_chains) == 1


# ---------------------------------------------------------------------------
# 3. build_export_markdown
# ---------------------------------------------------------------------------


def _entry(**overrides):
    entry = {
        "umo": "g1",
        "group_name": "测试群",
        "iso_year": 2026,
        "iso_week": 32,
        "created_at": 1700000000.0,
        "chronicle_text": "## 本周史记\n正文内容",
        "biographies": [
            {"sender_id": "1", "sender_name": "张三", "text": "张三传"},
        ],
        "image_path": None,
        "version": 1,
    }
    entry.update(overrides)
    return entry


def test_build_export_markdown_contains_all_parts():
    md = build_export_markdown(_entry())
    assert "# 群史记 2026-W32" in md
    assert "> 群: 测试群" in md
    assert "## 本周史记" in md
    assert "## 人物传记：张三" in md
    assert "张三传" in md


def test_build_export_markdown_empty_text_and_bad_ts():
    md = build_export_markdown(
        _entry(chronicle_text="", created_at=None, biographies=[])
    )
    assert "群史记 2026-W32" in md
    assert "生成时间: ?" in md


def test_build_export_markdown_bad_biographies():
    md = build_export_markdown(
        _entry(biographies=[None, "oops", {"sender_name": "李四", "text": "传"}])
    )
    assert "## 人物传记：李四" in md
