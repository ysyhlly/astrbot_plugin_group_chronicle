"""
core/render T2I (AstrBot 内置文转图) 渲染路径单元测试 —— 无需 astrbot / 浏览器 / 网络。

覆盖:
1. render_mode="off" → (text, None), 不调用 html_render
2. html_render 返回 http URL → 下载 (aiohttp / urllib 双路径) → 本地路径
3. html_render 返回 bytes → 写文件
4. html_render 返回本地路径 str → 原样返回
5. html_render 抛异常 → None; render_fallback_playwright=True 回退假 playwright,
   False 时不启动浏览器
6. 两种 html_render 签名兼容 (kwargs options / 4 位置参数 TypeError 重试)
7. render_mode="playwright" 仍走本地 playwright
8. render_html 输出包含 generated_at (显式 + 默认格式化)
9. 新模板占位符渲染 (jinja2 与字符串替换兜底两路径)
附: T2I 结果同样进渲染缓存; 引用段落提升为咒术卡 blockquote
"""

import asyncio
import importlib.util
import re
import sys
import types
import urllib.request
from pathlib import Path

_PLUGIN_ROOT = str(Path(__file__).resolve().parents[1])
if _PLUGIN_ROOT not in sys.path:
    sys.path.insert(0, _PLUGIN_ROOT)

from core import render as render_mod  # noqa: E402

import tests.test_render as tr  # noqa: E402  (复用假 playwright 夹具)


def _async_counting_func(calls):
    """记录调用并按配置返回的假 html_render。"""

    async def func(html, data, **kw):
        calls.append((html, data, kw))
        if "result" in kw:
            return kw["result"]
        raise RuntimeError("fake html_render 未配置 result")

    return func


# --------------------------------------------------------------------------
# 1. off 模式
# --------------------------------------------------------------------------


def test_off_mode_no_render(monkeypatch, tmp_path):
    """render_mode="off" → (text, None); html_render 不被调用。"""
    calls = []
    func = _async_counting_func(calls)
    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "纯文本",
            "测试群",
            str(tmp_path),
            render_mode="off",
            html_render_func=func,
            enable_cache=False,
        )
    )
    assert text == "纯文本"
    assert image is None
    assert calls == []


def test_off_mode_skips_cache(monkeypatch, tmp_path):
    """off 模式即使存在缓存命中也不返回图片。"""
    render_mod.clear_render_cache()
    key = render_mod._render_cache_key("缓存文本", "测试群")
    render_mod._RENDER_CACHE[key] = str(tmp_path / "fake.png")
    Path(tmp_path, "fake.png").write_bytes(b"x")
    calls = []
    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "缓存文本",
            "测试群",
            str(tmp_path),
            render_mode="off",
            html_render_func=_async_counting_func(calls),
        )
    )
    assert image is None
    assert calls == []


# --------------------------------------------------------------------------
# 2. http URL → 下载 (urllib 兜底 / aiohttp 双路径)
# --------------------------------------------------------------------------


def test_t2i_download_url_via_urllib(monkeypatch, tmp_path):
    """aiohttp 不可用 → urllib 兜底下载; 返回本地 t2i_*.png 路径。"""
    render_mod.clear_render_cache()

    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"urllib-png-bytes"

    monkeypatch.setattr(
        urllib.request, "urlopen", lambda url, timeout=None: _FakeResp()
    )
    monkeypatch.setitem(sys.modules, "aiohttp", None)  # 强制走 urllib

    calls = []

    async def func(html, data, **kw):
        calls.append(kw.get("options"))
        return "http://example.com/x/y.png"

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            enable_cache=False,
        )
    )
    assert image is not None
    assert image.startswith(str(tmp_path))
    assert Path(image).name.startswith("t2i_") and image.endswith(".png")
    assert Path(image).read_bytes() == b"urllib-png-bytes"
    # 默认选项透传
    assert calls[0]["type"] == "png" and calls[0]["full_page"] is True
    assert calls[0]["scale"] == "device" and calls[0]["timeout"] == 30000


def _install_fake_aiohttp(data: bytes):
    """向 sys.modules 注入假 aiohttp (确定性, 不受本机是否安装影响)。"""

    class _FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def raise_for_status(self):
            pass

        async def read(self):
            return self.payload

    class _FakeSession:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def get(self, url, timeout=None):
            # 同步返回异步上下文管理器 (对齐 aiohttp 的 async with session.get(...))
            return _FakeResponse(self.payload)

    aio = types.ModuleType("aiohttp")
    aio.ClientSession = lambda: _FakeSession(data)
    aio.ClientTimeout = lambda **kw: kw
    return aio


def test_t2i_download_url_via_aiohttp(monkeypatch, tmp_path):
    """aiohttp 可用 → 走 aiohttp 下载。"""
    render_mod.clear_render_cache()
    monkeypatch.setitem(sys.modules, "aiohttp", _install_fake_aiohttp(b"aiohttp-png"))

    async def func(html, data, **kw):
        return "https://cdn.example.com/img.png"

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            enable_cache=False,
        )
    )
    assert image is not None and image.endswith(".png")
    assert Path(image).read_bytes() == b"aiohttp-png"


def test_t2i_download_failure_returns_none(monkeypatch, tmp_path):
    """下载失败 (404) → None。"""
    render_mod.clear_render_cache()

    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            raise OSError("404 Not Found")

    monkeypatch.setattr(
        urllib.request, "urlopen", lambda url, timeout=None: _FakeResp()
    )
    monkeypatch.setitem(sys.modules, "aiohttp", None)

    async def func(html, data, **kw):
        return "http://example.com/missing.png"

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            render_fallback_playwright=False,
            enable_cache=False,
        )
    )
    assert image is None
    assert not list(Path(tmp_path).glob("t2i_*.png"))


# --------------------------------------------------------------------------
# 3. bytes / 4. 本地路径 / 未知类型
# --------------------------------------------------------------------------


def test_t2i_bytes_written_to_file(monkeypatch, tmp_path):
    """html_render 返回 bytes → 写入 t2i_*.png 并返回路径。"""
    render_mod.clear_render_cache()

    async def func(html, data, **kw):
        return b"\x89PNG-fake"

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            enable_cache=False,
        )
    )
    assert image is not None and image.endswith(".png")
    assert Path(image).read_bytes() == b"\x89PNG-fake"


def test_t2i_local_path_returned_as_is(monkeypatch, tmp_path):
    """html_render 返回本地路径 str → 原样返回。"""
    render_mod.clear_render_cache()
    existing = tmp_path / "existing.png"
    existing.write_bytes(b"x")

    async def func(html, data, **kw):
        return str(existing)

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            enable_cache=False,
        )
    )
    assert image == str(existing)


def test_t2i_none_result_returns_none(monkeypatch, tmp_path):
    """html_render 返回 None → image None (回退 playwright 关闭时)。"""
    render_mod.clear_render_cache()

    async def func(html, data, **kw):
        return None

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            render_fallback_playwright=False,
            enable_cache=False,
        )
    )
    assert image is None


def test_t2i_missing_local_path_returns_none(monkeypatch, tmp_path):
    """html_render 返回不存在的本地路径 → None。"""
    render_mod.clear_render_cache()

    async def func(html, data, **kw):
        return str(tmp_path / "nope.png")

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
            render_fallback_playwright=False,
            enable_cache=False,
        )
    )
    assert image is None


# --------------------------------------------------------------------------
# 5. 抛异常 → None + 回退 playwright 开关
# --------------------------------------------------------------------------


def test_t2i_exception_falls_back_to_playwright(monkeypatch, tmp_path):
    """html_render 抛异常 → T2I None; render_fallback_playwright=True 回退假浏览器。"""
    render_mod.clear_render_cache()
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    tr._install_fake_playwright()
    try:

        async def func(html, data, **kw):
            raise RuntimeError("T2I 服务不可用")

        text, image = asyncio.run(
            render_mod.render_text_or_image(
                "内容",
                "测试群",
                str(tmp_path),
                html_render_func=func,
            )
        )
    finally:
        tr._uninstall_fake_playwright()

    assert text == "内容"
    assert image is not None and Path(image).exists()
    assert tr._FAKE.last_page.screenshot_called is True


def test_t2i_exception_no_playwright_fallback(monkeypatch, tmp_path):
    """render_fallback_playwright=False → (text, None), 不启动浏览器。"""
    render_mod.clear_render_cache()
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    tr._install_fake_playwright()
    try:
        tr._FAKE.last_browser = None

        async def func(html, data, **kw):
            raise RuntimeError("boom")

        text, image = asyncio.run(
            render_mod.render_text_or_image(
                "内容",
                "测试群",
                str(tmp_path),
                html_render_func=func,
                render_fallback_playwright=False,
            )
        )
    finally:
        tr._uninstall_fake_playwright()

    assert image is None
    assert tr._FAKE.last_browser is None  # 未启动浏览器


# --------------------------------------------------------------------------
# 6. 两种 html_render 签名兼容
# --------------------------------------------------------------------------


def test_t2i_kwargs_signature(monkeypatch, tmp_path):
    """kwargs 签名 (html, data, options=...) 正常工作, options 原样透传。"""
    render_mod.clear_render_cache()
    seen = {}

    async def kw_func(html, data, *, options=None):
        seen["options"] = options
        seen["html"] = html
        return b"kw-png"

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "# 标题",
            "测试群",
            str(tmp_path),
            html_render_func=kw_func,
            enable_cache=False,
        )
    )
    assert image is not None and Path(image).read_bytes() == b"kw-png"
    assert seen["options"]["type"] == "png"
    assert "标题" in seen["html"] and "群史记" in seen["html"]


def test_t2i_positional_signature_retry(monkeypatch, tmp_path):
    """4 位置参数签名: 首次 kwargs 调用 TypeError → 自动重试位置调用。"""
    render_mod.clear_render_cache()
    calls = []

    async def pos_func(html, data, return_url, options):
        calls.append((return_url, options))
        return b"pos-png"

    text, image = asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=pos_func,
            enable_cache=False,
        )
    )
    assert image is not None and Path(image).read_bytes() == b"pos-png"
    assert len(calls) == 1
    assert calls[0][0] is False  # return_url=False
    assert calls[0][1]["scale"] == "device"  # options 透传


def test_t2i_custom_options_merged(monkeypatch, tmp_path):
    """调用方 options 覆盖默认值 (t2i_scale=css 场景)。"""
    render_mod.clear_render_cache()
    seen = {}

    async def kw_func(html, data, *, options=None):
        seen["options"] = options
        return b"x"

    asyncio.run(
        render_mod.render_text_or_image(
            "内容",
            "测试群",
            str(tmp_path),
            html_render_func=kw_func,
            enable_cache=False,
            options={"scale": "css"},
        )
    )
    assert seen["options"]["scale"] == "css"
    assert seen["options"]["type"] == "png"  # 未覆盖项保留默认


# --------------------------------------------------------------------------
# 7. playwright 模式
# --------------------------------------------------------------------------


def test_playwright_mode_still_works(monkeypatch, tmp_path):
    """render_mode="playwright" 直接走本地浏览器, html_render 不参与。"""
    render_mod.clear_render_cache()
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    tr._install_fake_playwright()
    try:
        calls = []
        text, image = asyncio.run(
            render_mod.render_text_or_image(
                "内容",
                "测试群",
                str(tmp_path),
                render_mode="playwright",
                html_render_func=_async_counting_func(calls),
            )
        )
    finally:
        tr._uninstall_fake_playwright()

    assert image is not None and Path(image).exists()
    assert calls == []  # html_render 未被调用
    assert tr._FAKE.last_page.screenshot_called is True


# --------------------------------------------------------------------------
# 8. render_html 的 generated_at
# --------------------------------------------------------------------------


def test_render_html_explicit_generated_at():
    out = render_mod.render_html("# 标题", "测试群", generated_at="2026年08月07日")
    assert "2026年08月07日" in out


def test_render_html_default_generated_at():
    out = render_mod.render_html("# 标题", "测试群")
    assert re.search(r"\d{4}年\d{2}月\d{2}日", out)


# --------------------------------------------------------------------------
# 9. 新模板占位符渲染 (jinja2 与字符串替换兜底)
# --------------------------------------------------------------------------


def test_template_jinja2_renders_all_vars():
    out = render_mod.render_html(
        "# 标题\n\n> 覆盖 2026-01-01 ~ 2026-01-07，500 条消息",
        "测试群",
        generated_at="2026年08月07日",
    )
    assert "chronicle-sheet" in out
    assert "测试群" in out and "群史记" in out
    assert "2026年08月07日" in out
    assert "171034" in out  # 夜空底色
    assert "spell-card" in out  # 引用 → 咒术卡


def test_template_fallback_replace_renders_all_vars(monkeypatch):
    """jinja2 不可用 → 字符串替换兜底, 三个占位符全部替换。"""
    monkeypatch.setitem(sys.modules, "jinja2", None)
    out = render_mod.render_html(
        "## 大事记\n\n- 第一条\n\n> 引用一句",
        "测试群",
        generated_at="2026年08月07日",
    )
    assert "测试群" in out
    assert "2026年08月07日" in out
    assert "<h2>大事记</h2>" in out
    assert "<li>第一条</li>" in out
    assert "{{ " not in out  # 无残留占位符
    assert "spell-card" in out


# --------------------------------------------------------------------------
# 附: T2I 结果进缓存 / 引用咒术卡
# --------------------------------------------------------------------------


def test_t2i_result_cached(monkeypatch, tmp_path):
    """t2i 成功结果进缓存: 二次调用不重复请求 html_render。"""
    render_mod.clear_render_cache()
    count = {"n": 0}

    async def func(html, data, **kw):
        count["n"] += 1
        return b"cached-png"

    text1, image1 = asyncio.run(
        render_mod.render_text_or_image(
            "缓存内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
        )
    )
    text2, image2 = asyncio.run(
        render_mod.render_text_or_image(
            "缓存内容",
            "测试群",
            str(tmp_path),
            html_render_func=func,
        )
    )
    assert image1 == image2 and Path(image2).exists()
    assert count["n"] == 1


def test_render_html_quote_becomes_spell_card():
    out = render_mod.render_html("# 标题\n\n> 本周大事：全体出动", "测试群")
    assert '<blockquote class="spell-card">' in out
    assert "本周大事：全体出动" in out
