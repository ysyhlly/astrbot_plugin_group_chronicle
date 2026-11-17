"""
core/render 单元测试 —— 不需要真实 playwright / 浏览器 / astrbot / pip 安装。

策略：
- 所有 async 测试用 asyncio.run 包装（不依赖 pytest-asyncio）；
- 用 monkeypatch 控制 importlib.util.find_spec 与 sys.modules；
- 用注入 sys.modules 的假 playwright 模块模拟完整渲染管线。
"""

import asyncio
import builtins
import importlib.util
import sys
import types
from pathlib import Path

_PLUGIN_ROOT = str(Path(__file__).resolve().parents[1])
if _PLUGIN_ROOT not in sys.path:
    sys.path.insert(0, _PLUGIN_ROOT)

from core import render as render_mod  # noqa: E402


# --------------------------------------------------------------------------
# 假 playwright（注入 sys.modules，模拟真实 API 形状）
# --------------------------------------------------------------------------


class _FakePage:
    def __init__(self) -> None:
        self.screenshot_called = False
        self.screenshot_path = None
        self.set_content_html = None

    async def set_content(self, content: str) -> None:
        self.set_content_html = content

    async def wait_for_timeout(self, ms: int) -> None:
        pass

    async def query_selector(self, selector: str):
        assert selector == "body"
        return self

    async def screenshot(self, path=None) -> None:
        self.screenshot_called = True
        self.screenshot_path = path
        Path(path).write_bytes(b"fake-png-bytes")


class _FakeBrowser:
    def __init__(self) -> None:
        self.closed = False
        self.page = _FakePage()
        _FAKE.last_browser = self

    async def new_page(self, **kwargs):
        return self.page

    async def close(self) -> None:
        self.closed = True


class _FakeChromium:
    async def launch(self, **kwargs):
        return _FakeBrowser()


class _FakePlaywright:
    def __init__(self) -> None:
        self.chromium = _FakeChromium()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _FakeRegistry:
    """记录假浏览器/页面实例，便于断言。"""

    def __init__(self) -> None:
        self.last_browser = None

    @property
    def last_page(self):
        return self.last_browser.page if self.last_browser else None


_FAKE = _FakeRegistry()


def _install_fake_playwright() -> None:
    pw = types.ModuleType("playwright")
    pw.__path__ = []
    api = types.ModuleType("playwright.async_api")
    api.async_playwright = _FakePlaywright
    sys.modules["playwright"] = pw
    sys.modules["playwright.async_api"] = api
    _FAKE.last_browser = None


def _uninstall_fake_playwright() -> None:
    sys.modules.pop("playwright", None)
    sys.modules.pop("playwright.async_api", None)


# --------------------------------------------------------------------------
# 用例
# --------------------------------------------------------------------------


def test_render_available_reflects_find_spec(monkeypatch):
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    assert render_mod.render_available() is False

    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    assert render_mod.render_available() is True


def test_unavailable_no_playwright_import_attempted(monkeypatch, tmp_path):
    """find_spec → None 时直接返回 None，且绝不应尝试导入 playwright。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    _uninstall_fake_playwright()

    real_import = builtins.__import__

    def spy_import(name, *args, **kwargs):
        assert not (name == "playwright" or name.startswith("playwright.")), (
            f"不应导入 playwright，却尝试了: {name}"
        )
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", spy_import)
    result = asyncio.run(
        render_mod.render_chronicle_image("# 标题", "测试群", str(tmp_path))
    )
    assert result is None


def test_import_error_returns_none(monkeypatch, tmp_path):
    """find_spec 可用但 from playwright.async_api import async_playwright 抛错 → None。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    pw = types.ModuleType("playwright")
    pw.__path__ = []
    api = types.ModuleType("playwright.async_api")  # 故意缺失 async_playwright
    monkeypatch.setitem(sys.modules, "playwright", pw)
    monkeypatch.setitem(sys.modules, "playwright.async_api", api)

    result = asyncio.run(
        render_mod.render_chronicle_image("# 标题", "测试群", str(tmp_path))
    )
    assert result is None
    assert not list(Path(tmp_path).glob("chronicle_*.png"))


def test_full_pipeline_with_fake_playwright(monkeypatch, tmp_path):
    """假浏览器完整管线：应返回路径、截图被调用、浏览器被关闭、markdown 被转换。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    _install_fake_playwright()
    try:
        result = asyncio.run(
            render_mod.render_chronicle_image(
                "# 群史记\n\n本周要点：**加粗** 与正文\n\n- 条目一\n- 条目二\n\n"
                "```\nprint('hi')\n```",
                "测试群",
                str(tmp_path),
            )
        )
    finally:
        _uninstall_fake_playwright()

    assert isinstance(result, str)
    assert result.startswith(str(tmp_path))
    assert Path(result).exists()
    assert Path(result).name.startswith("chronicle_") and result.endswith(".png")

    browser = _FAKE.last_browser
    assert browser is not None and browser.closed is True
    page = _FAKE.last_page
    assert page is not None and page.screenshot_called is True
    assert page.screenshot_path == result

    rendered = page.set_content_html
    assert "<h1>群史记</h1>" in rendered
    assert "<strong>加粗</strong>" in rendered
    assert "<li>条目一</li>" in rendered
    # html.escape(quote=True) 会把单引号转成 &#x27;
    assert "<code>print(&#x27;hi&#x27;)</code>" in rendered
    # 中文模板样式与群名标题应出现在最终 HTML 中 (魔法少女主题夜空底色 #171034)
    assert "测试群" in rendered and "171034" in rendered


def test_render_text_or_image_plain_fallback(monkeypatch, tmp_path):
    """渲染不可用时返回 (text, None)。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    text, image = asyncio.run(
        render_mod.render_text_or_image("纯文本内容", "测试群", str(tmp_path))
    )
    assert text == "纯文本内容"
    assert image is None


def test_render_text_or_image_with_browser(monkeypatch, tmp_path):
    """假浏览器成功时返回 (text, path)。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    _install_fake_playwright()
    try:
        text, image = asyncio.run(
            render_mod.render_text_or_image("带图内容", "测试群", str(tmp_path))
        )
    finally:
        _uninstall_fake_playwright()

    assert text == "带图内容"
    assert isinstance(image, str)
    assert Path(image).exists()
    assert _FAKE.last_page.screenshot_called is True


def test_jinja2_missing_uses_fallback_template(monkeypatch, tmp_path):
    """jinja2 不可用时走字符串替换兜底，仍能渲染出图。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    _install_fake_playwright()
    monkeypatch.setitem(sys.modules, "jinja2", None)
    try:
        result = asyncio.run(
            render_mod.render_chronicle_image(
                "## 大事记\n\n- 第一条", "测试群", str(tmp_path)
            )
        )
    finally:
        _uninstall_fake_playwright()

    assert isinstance(result, str)
    assert Path(result).exists()
    rendered = _FAKE.last_page.set_content_html
    assert "<h2>大事记</h2>" in rendered
    assert "测试群" in rendered and "171034" in rendered


def test_html_escaping_in_markdown(monkeypatch, tmp_path):
    """内容中的 HTML 应被转义，不能注入。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    _install_fake_playwright()
    try:
        result = asyncio.run(
            render_mod.render_chronicle_image(
                "# 标题\n\n<script>alert(1)</script>", "测试群", str(tmp_path)
            )
        )
    finally:
        _uninstall_fake_playwright()

    assert isinstance(result, str)
    rendered = _FAKE.last_page.set_content_html
    assert "<script>alert" not in rendered
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rendered


# --------------------------------------------------------------------------
# P3 渲染缓存
# --------------------------------------------------------------------------


def test_render_cache_reuses_file(monkeypatch, tmp_path):
    """同一 markdown+群名 二次渲染 → 直接复用缓存路径, 不再启动浏览器。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    _install_fake_playwright()
    render_mod.clear_render_cache()
    try:
        text1, image1 = asyncio.run(
            render_mod.render_text_or_image("缓存内容", "测试群", str(tmp_path))
        )
        first_page = _FAKE.last_page
        text2, image2 = asyncio.run(
            render_mod.render_text_or_image("缓存内容", "测试群", str(tmp_path))
        )
    finally:
        _uninstall_fake_playwright()

    assert text1 == text2 == "缓存内容"
    assert image1 == image2
    assert Path(image2).exists()
    # 第二次未启动新浏览器: last_page 仍是第一次的页面
    assert _FAKE.last_page is first_page


def test_render_cache_disabled_rerenders(monkeypatch, tmp_path):
    """enable_cache=False → 每次重新渲染 (启动新浏览器)。"""
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    _install_fake_playwright()
    render_mod.clear_render_cache()
    try:
        _, image1 = asyncio.run(
            render_mod.render_text_or_image("不缓存内容", "测试群", str(tmp_path))
        )
        first_page = _FAKE.last_page
        _, image2 = asyncio.run(
            render_mod.render_text_or_image(
                "不缓存内容", "测试群", str(tmp_path), enable_cache=False
            )
        )
    finally:
        _uninstall_fake_playwright()

    assert image1 != image2  # 重新渲染, 时间戳文件名不同
    assert _FAKE.last_page is not first_page  # 启动了新浏览器


def test_clear_render_cache_empties():
    """clear_render_cache → 缓存清空。"""
    render_mod._RENDER_CACHE["k"] = "/tmp/x.png"
    render_mod.clear_render_cache()
    assert render_mod._RENDER_CACHE == {}
