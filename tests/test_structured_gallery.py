"""Batch D: structured 结构化输出 + gallery 图鉴/黑话 单元测试。

无 astrbot 环境, 依赖 conftest 桩件; monkeypatch call_llm / append_slang。
运行: python -m pytest tests/test_structured_gallery.py -v
"""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.gallery as gal
import core.structured as st
from core import generation as gen


def run(coro):
    return asyncio.run(coro)


def amock(result):
    async def _f(*args, **kwargs):
        return result

    return _f


# ---------------------------------------------------------------------------
# structured: parse_structured_json 成功/失败
# ---------------------------------------------------------------------------


def test_parse_structured_json_success():
    raw = (
        '{"events": [{"date": "2026-08-01", "title": "群友大会", '
        '"detail": "热闹非凡", "members": ["张三", "李四"]}], '
        '"quotes": ["哈哈哈哈"], "new_members": ["王五"], '
        '"slang": ["流汗黄豆：无语"], "overall": "本周群很热闹"}'
    )
    data = st.parse_structured_json(raw)
    assert data["overall"] == "本周群很热闹"
    assert data["events"][0]["title"] == "群友大会"
    assert data["events"][0]["members"] == ["张三", "李四"]
    assert data["quotes"] == ["哈哈哈哈"]
    assert data["new_members"] == ["王五"]
    assert data["slang"] == ["流汗黄豆：无语"]


def test_parse_structured_json_with_fences():
    """```json 围栏包裹 → 正常解析。"""
    raw = '```json\n{"overall": "围栏内容"}\n```'
    data = st.parse_structured_json(raw)
    assert data["overall"] == "围栏内容"


def test_parse_structured_json_with_surrounding_text():
    """前后夹带说明文字 → 截取首个 JSON 对象。"""
    raw = '好的，以下是结果：\n{"overall": "夹带内容"}\n希望能帮到你'
    data = st.parse_structured_json(raw)
    assert data["overall"] == "夹带内容"


def test_parse_structured_json_failures_return_empty():
    """畸形输入一律返回 {}, 绝不抛异常。"""
    assert st.parse_structured_json("not json at all") == {}
    assert st.parse_structured_json("") == {}
    assert st.parse_structured_json(None) == {}
    assert st.parse_structured_json(123) == {}
    assert st.parse_structured_json('{"broken": }') == {}
    assert st.parse_structured_json("[1,2,3]") == {}  # 非 dict


def test_extract_structured_returns_normalized_dict(monkeypatch):
    """extract_structured: 调 LLM → 解析为规范化 dict, prompt 含材料。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        captured["provider_id"] = provider_id
        return '{"events": [], "quotes": [], "new_members": [], "slang": [], "overall": "ok"}'

    monkeypatch.setattr(st, "call_llm", fake_call_llm)
    result = run(st.extract_structured(None, "g1", "证据材料", "pid123"))
    assert result["overall"] == "ok"
    assert "证据材料" in captured["prompt"]
    assert captured["provider_id"] == "pid123"


def test_extract_structured_llm_failure_returns_empty(monkeypatch):
    """LLM 调用失败 → 返回 {} (绝不崩溃)。"""

    async def boom(*args, **kwargs):
        raise RuntimeError("llm down")

    monkeypatch.setattr(st, "call_llm", boom)
    assert run(st.extract_structured(None, "g1", "材料")) == {}


def test_render_structured_markdown_sections():
    data = {
        "events": [
            {
                "date": "2026-08-01",
                "title": "大事",
                "detail": "细节",
                "members": ["张三"],
            }
        ],
        "quotes": ["金句"],
        "new_members": ["新面孔"],
        "slang": ["黑话"],
        "overall": "总览段",
    }
    md = st.render_structured_markdown(data, 7)
    assert "总览段" in md
    assert "## 大事记" in md
    assert "2026-08-01" in md and "大事" in md
    assert "参与者：张三" in md
    assert "## 本期金句" in md and "- 金句" in md
    assert "## 新面孔" in md and "- 新面孔" in md
    assert "## 本周黑话" in md and "- 黑话" in md


def test_render_structured_markdown_empty():
    assert st.render_structured_markdown({}, 7) == ""
    assert st.render_structured_markdown(None, 7) == ""
    # 全空字段 → 空串 (调用方回退自由散文)
    assert (
        st.render_structured_markdown(
            {"events": [], "quotes": [], "new_members": [], "slang": [], "overall": ""},
            7,
        )
        == ""
    )


# ---------------------------------------------------------------------------
# generation: enable_structured / enable_slang / topic 接线
# ---------------------------------------------------------------------------


def test_generate_chronicle_structured_deterministic(monkeypatch):
    """enable_structured=true 且提取成功 → 直接确定性渲染, 不再做散文 LLM 调用。"""
    calls = []

    async def fake_extract(context, umo, evidence_text, provider_id=None):
        calls.append("extract")
        return {
            "overall": "结构化总览",
            "events": [{"date": "2026-08-01", "title": "E"}],
            "quotes": [],
            "new_members": [],
            "slang": [],
        }

    async def fake_call_llm(*args, **kwargs):
        calls.append("prose")
        return "自由散文"

    monkeypatch.setattr(gen, "extract_structured", fake_extract)
    monkeypatch.setattr(gen, "call_llm", fake_call_llm)
    monkeypatch.setattr(gen, "append_slang", amock("材料"))

    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    text = run(
        gen.generate_chronicle("ctx", "g1", evidence, None, 7, enable_structured=True)
    )
    assert text == "结构化总览\n\n## 大事记\n- **2026-08-01** E"
    assert calls == ["extract"]  # 没有散文 LLM 调用


def test_generate_chronicle_structured_fallback_to_prose(monkeypatch):
    """enable_structured=true 但提取失败 → 回退自由散文 LLM 调用。"""
    calls = []

    async def fake_extract(*args, **kwargs):
        calls.append("extract")
        return {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        calls.append("prose")
        return "自由散文正文"

    monkeypatch.setattr(gen, "extract_structured", fake_extract)
    monkeypatch.setattr(gen, "call_llm", fake_call_llm)
    monkeypatch.setattr(gen, "append_slang", amock("材料"))

    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    text = run(
        gen.generate_chronicle("ctx", "g1", evidence, None, 7, enable_structured=True)
    )
    assert text == "自由散文正文"
    assert calls == ["extract", "prose"]


def test_generate_chronicle_slang_appended(monkeypatch):
    """enable_slang=true (默认) → 证据文本追加黑话分节。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        return "ok"

    async def fake_append_slang(evidence_text, evidence, structured, context, enabled):
        return evidence_text + "\n\n## 本周黑话\n- 测试梗：含义"

    monkeypatch.setattr(gen, "call_llm", fake_call_llm)
    monkeypatch.setattr(gen, "append_slang", fake_append_slang)
    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    run(gen.generate_chronicle("ctx", "g1", evidence, None, 7))
    assert "## 本周黑话\n- 测试梗：含义" in captured["prompt"]


def test_generate_chronicle_topic_appended(monkeypatch):
    """topic 非空 → prompt 追加「本期主题…请围绕其展开」。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        return "ok"

    monkeypatch.setattr(gen, "call_llm", fake_call_llm)
    monkeypatch.setattr(gen, "append_slang", amock("材料"))
    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    run(gen.generate_chronicle("ctx", "g1", evidence, None, 7, topic="年度大会"))
    assert "本期主题：年度大会，请围绕其展开。" in captured["prompt"]


def test_generate_topic_candidates_parses_lines(monkeypatch):
    """选题候选: 解析编号/破折号行, 去重, 最多 num_candidates。"""

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        return "1. 年度大会\n- 烧烤聚会\n2. 年度大会\n3. 摸鱼大赛"

    monkeypatch.setattr(gen, "call_llm", fake_call_llm)
    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    topics = run(gen.generate_topic_candidates("ctx", "g1", evidence))
    assert topics == ["年度大会", "烧烤聚会", "摸鱼大赛"]


def test_generate_topic_candidates_llm_failure_returns_empty(monkeypatch):
    async def boom(*args, **kwargs):
        raise RuntimeError("x")

    monkeypatch.setattr(gen, "call_llm", boom)
    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    assert run(gen.generate_topic_candidates("ctx", "g1", evidence)) == []


def test_generate_universe_chronicle_combines_groups(monkeypatch):
    """宇宙史记: 多群材料合并在一次 LLM 调用中。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        return "宇宙史记正文"

    monkeypatch.setattr(gen, "call_llm", fake_call_llm)
    msgs1 = [
        SimpleNamespace(
            sender_name="甲", sender_id="1", timestamp=1700000000.0, content="群1消息"
        )
    ]
    msgs2 = [
        SimpleNamespace(
            sender_name="乙", sender_id="2", timestamp=1700000000.0, content="群2消息"
        )
    ]
    ev1 = SimpleNamespace(chronicle_msgs=msgs1, memories=[], graph_snapshot=None)
    ev2 = SimpleNamespace(chronicle_msgs=msgs2, memories=[], graph_snapshot=None)
    text = run(
        gen.generate_universe_chronicle("ctx", "g1", [("群A", ev1), ("群B", ev2)], None)
    )
    assert text == "宇宙史记正文"
    assert "### 群: 群A" in captured["prompt"]
    assert "### 群: 群B" in captured["prompt"]
    assert "群1消息" in captured["prompt"] and "群2消息" in captured["prompt"]
    assert "宇宙史记" in captured["prompt"]


# ---------------------------------------------------------------------------
# gallery: 群友图鉴
# ---------------------------------------------------------------------------


def _msg(sender_id, sender_name, content="内容", ts=1700000000.0):
    return SimpleNamespace(
        sender_id=str(sender_id),
        sender_name=sender_name,
        content=content,
        timestamp=ts,
    )


def _evidence_with_msgs():
    msgs = [_msg("1", "张三", f"发言{i}") for i in range(8)]
    msgs += [_msg("2", "李四", f"发言{i}") for i in range(6)]
    return SimpleNamespace(chronicle_msgs=msgs, memories=[], graph_snapshot=None)


def test_build_gallery_prompt_contains_names(monkeypatch):
    """图鉴 prompt 包含活跃成员昵称, 输出含「## 群友图鉴」。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        captured["provider_id"] = provider_id
        return "张三｜夜猫子之王｜凌晨三点发美食\n李四｜潜水冠军｜常年窥屏"

    monkeypatch.setattr(gal, "call_llm", fake_call_llm)
    members = [
        {"sender_id": "1", "sender_name": "张三"},
        {"sender_id": "2", "sender_name": "李四"},
    ]
    result = run(gal.build_gallery(None, "g1", _evidence_with_msgs(), members, "pid"))
    assert "张三" in captured["prompt"] and "李四" in captured["prompt"]
    assert captured["provider_id"] == "pid"
    assert result.startswith("## 群友图鉴")
    assert "夜猫子之王" in result


def test_build_gallery_skips_inactive_members(monkeypatch):
    """发言不足 GALLERY_MIN_MSGS 的成员被跳过 (不出现在成员名单里)。"""
    captured = {}

    async def fake_call_llm(context, umo, prompt, system_prompt=None, provider_id=None):
        captured["prompt"] = prompt
        return "张三｜夜猫子之王｜凌晨三点发美食"

    monkeypatch.setattr(gal, "call_llm", fake_call_llm)
    members = [
        {"sender_id": "1", "sender_name": "张三"},  # 8 条 → 保留
        {"sender_id": "2", "sender_name": "潜水员"},  # 1 条 → 跳过
    ]
    msgs = [_msg("1", "张三", f"发言{i}") for i in range(8)]
    msgs += [_msg("2", "潜水员", "只说过一句")]
    evidence = SimpleNamespace(chronicle_msgs=msgs, memories=[], graph_snapshot=None)
    result = run(gal.build_gallery(None, "g1", evidence, members))
    assert "张三" in captured["prompt"]
    # 成员名单分节 (材料之后) 不应包含被跳过的成员
    member_list = captured["prompt"].split("请为以下成员各输出一行图鉴条目:")[-1]
    assert "潜水员" not in member_list
    assert result.startswith("## 群友图鉴")


def test_build_gallery_never_crashes(monkeypatch):
    """LLM 失败 / 空成员 → 返回 "", 绝不崩溃。"""

    async def boom(*args, **kwargs):
        raise RuntimeError("llm down")

    monkeypatch.setattr(gal, "call_llm", boom)
    assert (
        run(
            gal.build_gallery(
                None,
                "g1",
                _evidence_with_msgs(),
                [{"sender_id": "1", "sender_name": "张三"}],
            )
        )
        == ""
    )
    assert run(gal.build_gallery(None, "g1", _evidence_with_msgs(), [])) == ""


# ---------------------------------------------------------------------------
# gallery: 本周黑话 (append_slang)
# ---------------------------------------------------------------------------


def test_append_slang_from_memories():
    """记忆文本含 梗/黑话 → 追加「## 本周黑话」; 无内容原样返回。"""
    evidence = SimpleNamespace(
        chronicle_msgs=[],
        memories=[{"text": "群友发明了新梗 摆烂大师"}, {"text": "普通记忆"}],
        graph_snapshot=None,
    )
    out = run(gal.append_slang("base", evidence, None, None, True))
    assert "## 本周黑话" in out
    assert "摆烂大师" in out
    assert "普通记忆" not in out  # 不含关键词的不进黑话


def test_append_slang_from_structured():
    """structured.slang (dict 形态含含义) → 追加条目行。"""
    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)
    structured = {"slang": [{"term": "流汗黄豆", "meaning": "表示无语"}]}
    out = run(gal.append_slang("base", evidence, structured, None, True))
    assert "## 本周黑话" in out
    assert "流汗黄豆：表示无语" in out


def test_append_slang_omitted_when_nothing_found():
    """无任何黑话素材 → 原样返回, 不出现黑话分节。"""
    evidence = SimpleNamespace(
        chronicle_msgs=[], memories=[{"text": "平平无奇"}], graph_snapshot=None
    )
    out = run(gal.append_slang("base", evidence, None, None, True))
    assert out == "base"


def test_append_slang_disabled():
    """enable_slang=false → 即使有黑话素材也不追加。"""
    evidence = SimpleNamespace(
        chronicle_msgs=[], memories=[{"text": "新梗大全"}], graph_snapshot=None
    )
    out = run(gal.append_slang("base", evidence, None, None, False))
    assert out == "base"


def test_append_slang_soft_read_jargon_never_crashes(monkeypatch):
    """self_learning 黑话软读取: 无方法 / 抛异常 → 静默跳过。"""
    evidence = SimpleNamespace(chronicle_msgs=[], memories=[], graph_snapshot=None)

    async def boom(*args, **kwargs):
        raise RuntimeError("jargon read failed")

    monkeypatch.setattr(gal, "_soft_read_jargon", boom)
    out = run(gal.append_slang("base", evidence, None, None, True))
    assert out == "base"  # 不崩溃, 无黑话则不追加
