"""
core/render.py — 群史记图片渲染器（带纯文本降级）。

设计要点
--------
1. 模块必须在没有 playwright / jinja2 的情况下也能被安全导入。
   AstrBot 加载插件时会 import 整个插件包，任何顶层 ImportError 都会导致插件
   加载失败。因此 playwright / jinja2 的导入全部放在函数内部并用 try/except
   保护（参考 astrbot_plugin_zhenxunribao/main.py 第 11 行的顶层导入是反面教材，
   本模块刻意避免该写法）。
2. 任何渲染失败都返回 None，由调用方降级为纯文本输出——绝不向上抛异常。
3. 渲染链路：markdown → HTML（内置极简转换器，无重依赖）→ Jinja2 模板
   （templates/chronicle.html，缺失时用同一模板文件做字符串替换兜底）
   → Playwright Chromium 截图。
"""

import asyncio
import hashlib
import html
import logging
import os
import re
import time
from datetime import datetime

logger = logging.getLogger(__name__)

_PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TEMPLATES_DIR = os.path.join(_PLUGIN_DIR, "templates")
_TEMPLATE_PATH = os.path.join(_TEMPLATES_DIR, "chronicle.html")

#: 渲染缓存: hash(markdown+group) -> 已渲染图片路径 (P3 性能, 复用相同内容)
_RENDER_CACHE: dict[str, str] = {}

#: 渲染模式常量 (render_mode 配置可选值)
RENDER_MODE_T2I = "t2i"
RENDER_MODE_PLAYWRIGHT = "playwright"
RENDER_MODE_OFF = "off"

#: T2I 默认截图选项 (对齐 AstrBot html_render 的 options, 即 Playwright screenshot API)
_T2I_DEFAULT_OPTIONS: dict = {
    "type": "png",
    "full_page": True,
    "scale": "device",  # 高分屏下 device 比例更清晰
    "timeout": 30000,
}

#: markdown_to_html 把引用行 (`> ...`) 输出为 <p>&gt; ...</p>; 渲染层将其提升为
#: 咒术卡 blockquote (不改动 markdown_to_html 契约, 仅 render_html 美化)
_QUOTE_P_RE = re.compile(r"<p>&gt;\s*(.+?)</p>", re.DOTALL)


def clear_render_cache() -> None:
    """清空渲染缓存 (插件 terminate 时调用, 释放旧图片路径引用)。"""
    _RENDER_CACHE.clear()


def _render_cache_key(markdown_text: str, group_name: str) -> str:
    """渲染缓存键: md5(group_name|markdown)。"""
    return hashlib.md5(f"{group_name}|{markdown_text}".encode("utf-8")).hexdigest()


# 极简 markdown 规则（与测试保持一致，勿轻易改动）
_FENCE_RE = re.compile(r"^```[a-zA-Z0-9_+-]*\s*$")
_HEADING_RE = re.compile(r"^(#{1,3})\s+(.+)$")
_LIST_ITEM_RE = re.compile(r"^[-*]\s+(.+)$")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_INLINE_CODE_RE = re.compile(r"`([^`]+)`")


def render_available() -> bool:
    """playwright 是否可用（仅探测不导入）。"""
    import importlib.util

    return importlib.util.find_spec("playwright") is not None


def t2i_available(plugin) -> bool:
    """Star 插件是否具备 AstrBot 内置文转图 (T2I) 能力 (即 html_render 方法)。"""
    return callable(getattr(plugin, "html_render", None))


def _t2i_out_path(out_dir: str) -> str:
    """生成不冲突的 T2I 输出文件名 (t2i_<ts>.png)。"""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"t2i_{int(time.time())}.png")
    suffix = 1
    while os.path.exists(path):  # 防止同秒内重名覆盖
        path = os.path.join(out_dir, f"t2i_{int(time.time())}_{suffix}.png")
        suffix += 1
    return path


def _urllib_fetch_bytes(url: str) -> bytes:
    """urllib 兜底下载 (aiohttp 不可用时, 在 asyncio.to_thread 中运行)。"""
    import urllib.request

    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read()


async def _download_t2i_image(url: str, out_dir: str) -> str | None:
    """把 T2I 服务返回的远程图片 URL 下载到本地文件; 任何失败返回 None。"""
    try:
        path = _t2i_out_path(out_dir)
        try:
            import aiohttp
        except ImportError:
            # aiohttp 未安装: 退化为 urllib (阻塞调用放到线程池)
            data = await asyncio.to_thread(_urllib_fetch_bytes, url)
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.read()
        with open(path, "wb") as f:
            f.write(data)
        return path
    except Exception as exc:  # noqa: BLE001 - 下载失败静默降级
        logger.warning(f"下载 T2I 渲染图片失败: {exc}")
        return None


async def render_via_t2i(
    html_render_func,
    html_content: str,
    out_dir: str,
    options: dict | None = None,
) -> str | None:
    """
    用 AstrBot 内置文转图 (T2I) 服务渲染 HTML → 图片路径 (str)。

    兼容两种 html_render 调用签名 (部分版本支持位置参数 return_url):
      - await html_render(html, data, options=options)   (kwargs 优先)
      - await html_render(html, data, False, options)    (TypeError 后重试)

    结果归一化: http(s) URL → 下载到 out_dir 并返回本地路径; 本地路径 → 原样返回;
    bytes/bytearray → 写入文件; None/其他 → 视为失败。
    任何异常只记录日志并返回 None, 绝不向上抛出。
    """
    opts = dict(_T2I_DEFAULT_OPTIONS)
    if options:
        opts.update(options)
    try:
        try:
            result = await html_render_func(html_content, {}, options=opts)
        except TypeError:
            # 部分版本 html_render 需要位置参数 return_url
            result = await html_render_func(html_content, {}, False, opts)
    except Exception as exc:  # noqa: BLE001 - T2I 失败静默降级
        logger.warning(f"T2I 渲染失败，降级: {exc}")
        return None

    try:
        if result is None:
            return None
        if isinstance(result, (bytes, bytearray)):
            path = _t2i_out_path(out_dir)
            with open(path, "wb") as f:
                f.write(bytes(result))
            return path
        if isinstance(result, str):
            if result.startswith(("http://", "https://")):
                return await _download_t2i_image(result, out_dir)
            if os.path.exists(result):
                return result
            logger.warning(f"T2I 返回的本地路径不存在: {result}")
            return None
        logger.warning(f"T2I 返回未知类型: {type(result).__name__}")
        return None
    except Exception as exc:  # noqa: BLE001 - 处理结果失败静默降级
        logger.warning(f"处理 T2I 渲染结果失败: {exc}")
        return None


def markdown_to_html(md: str) -> str:
    """
    极简 markdown → HTML 转换器（无第三方依赖）。

    支持：# / ## / ### 标题、**加粗**、`行内代码`、- 列表、
    代码围栏（```...```）、普通段落；内容一律先 html.escape 再拼标签。
    """
    lines = md.split("\n")
    parts: list[str] = []
    para: list[str] = []
    list_items: list[str] = []
    i, n = 0, len(lines)

    def flush_para() -> None:
        nonlocal para
        if para:
            text = " ".join(line.strip() for line in para if line.strip())
            parts.append(f"<p>{_inline(html.escape(text))}</p>")
            para = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            items = "".join(f"<li>{_inline(html.escape(it))}</li>" for it in list_items)
            parts.append(f"<ul>{items}</ul>")
            list_items = []

    while i < n:
        stripped = lines[i].strip()
        if not stripped:
            flush_list()
            flush_para()
            i += 1
            continue
        if _FENCE_RE.match(stripped):
            flush_list()
            flush_para()
            buf: list[str] = []
            i += 1
            while i < n and not _FENCE_RE.match(lines[i].strip()):
                buf.append(lines[i])
                i += 1
            i += 1  # 跳过闭合围栏
            body = html.escape("\n".join(buf)).strip("\n")
            parts.append(f"<pre><code>{body}</code></pre>")
            continue
        heading = _HEADING_RE.match(stripped)
        if heading:
            flush_list()
            flush_para()
            level = len(heading.group(1))
            parts.append(
                f"<h{level}>{_inline(html.escape(heading.group(2).strip()))}</h{level}>"
            )
            i += 1
            continue
        item = _LIST_ITEM_RE.match(stripped)
        if item:
            flush_para()
            list_items.append(item.group(1).strip())
            i += 1
            continue
        flush_list()
        para.append(stripped)
        i += 1
    flush_list()
    flush_para()
    return "\n".join(parts)


def _inline(escaped: str) -> str:
    """对已转义的行内文本做加粗 / 行内代码替换（`*`、反引号不受 html.escape 影响）。"""
    s = _BOLD_RE.sub(r"<strong>\1</strong>", escaped)
    s = _INLINE_CODE_RE.sub(r"<code>\1</code>", s)
    return s


def render_html(
    markdown_text: str, group_name: str, generated_at: str | None = None
) -> str:
    """
    用 Jinja2 渲染 templates/chronicle.html；jinja2 缺失或渲染失败时，
    退化为对同一模板文件做字符串替换（内容已转义，不会注入）。

    generated_at 缺省时使用当前日期 (YYYY年MM月DD日)。
    """
    content_html = markdown_to_html(markdown_text)
    # 渲染层美化: 把 markdown 引用段落 (<p>&gt; ...</p>) 提升为咒术卡
    # blockquote (不改动 markdown_to_html 契约)
    content_html = _QUOTE_P_RE.sub(
        lambda m: f'<blockquote class="spell-card"><p>{m.group(1)}</p></blockquote>',
        content_html,
    )
    generated_at = generated_at or datetime.now().strftime("%Y年%m月%d日")
    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape

        env = Environment(
            loader=FileSystemLoader(_TEMPLATES_DIR),
            autoescape=select_autoescape(enabled_extensions=("html",), default=True),
        )
        return env.get_template("chronicle.html").render(
            group_name=group_name, content=content_html, generated_at=generated_at
        )
    except Exception as exc:  # noqa: BLE001 — 渲染失败必须降级，不能阻断调用方
        logger.warning(f"jinja2 模板渲染失败，使用降级模板: {exc}")
        return _render_html_fallback(content_html, group_name, generated_at)


def _render_html_fallback(content_html: str, group_name: str, generated_at: str) -> str:
    """jinja2 不可用时的兜底：读同一模板文件做占位符替换。"""
    try:
        with open(_TEMPLATE_PATH, "r", encoding="utf-8") as f:
            tmpl = f.read()
        return (
            tmpl.replace("{{ group_name }}", html.escape(group_name))
            .replace("{{ generated_at }}", html.escape(generated_at))
            .replace("{{ content|safe }}", content_html)
        )
    except OSError:
        # 模板文件也缺失：极简内联兜底，保证仍能出图
        return (
            "<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<style>body{font-family:'Noto Serif SC','Songti SC','SimSun',serif;"
            "background:linear-gradient(160deg,#171034,#2a1b52 55%,#43276f);"
            "color:#f6ecff;padding:48px 64px;line-height:1.9}"
            "h1{color:#ffd97a;letter-spacing:4px}ul{list-style:none;padding-left:1em}"
            "ul li::before{content:'✧';color:#ffd97a;margin-right:6px}"
            "pre{background:#221543;padding:12px;border-left:4px solid #ff8ac2}"
            "blockquote{color:#ffb3d9;border-left:3px solid #ff8ac2;padding-left:12px}"
            "</style>"
            f"</head><body><h1>{html.escape(group_name)}·群史记</h1>{content_html}</body></html>"
        )


async def render_chronicle_image(
    markdown_text: str, group_name: str, out_dir: str
) -> str | None:
    """
    将 markdown 内容渲染为 PNG 图片并返回路径；任何失败返回 None（纯文本降级）。

    playwright 未安装 / 导入失败 / 启动失败 / 截图失败时均不会抛异常。
    """
    if not render_available():
        logger.warning("playwright 未安装，降级为纯文本输出")
        return None

    try:
        from playwright.async_api import async_playwright
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"playwright 导入失败，降级为纯文本输出: {exc}")
        return None

    out_path: str | None = None
    browser = None
    try:
        html_content = render_html(markdown_text, group_name)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"chronicle_{int(time.time())}.png")
        suffix = 1
        while os.path.exists(out_path):  # 防止同秒内重名覆盖
            out_path = os.path.join(
                out_dir, f"chronicle_{int(time.time())}_{suffix}.png"
            )
            suffix += 1

        async with async_playwright() as p:
            browser = await p.chromium.launch(args=["--no-sandbox"])
            page = await browser.new_page(viewport={"width": 1280, "height": 1400})
            await page.set_content(html_content)
            await page.wait_for_timeout(200)
            element = await page.query_selector("body")
            await element.screenshot(path=out_path)
    except Exception as exc:  # noqa: BLE001 — 任何异常都降级为纯文本
        logger.warning(f"渲染群史记图片失败，降级为纯文本输出: {exc}")
        if out_path and os.path.exists(out_path):
            try:
                os.remove(out_path)
            except OSError:
                pass
        return None
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:  # noqa: BLE001
                pass
    return out_path


async def render_text_or_image(
    text: str,
    group_name: str,
    out_dir: str,
    *,
    enable_cache: bool = True,
    render_mode: str = RENDER_MODE_T2I,
    html_render_func=None,
    render_fallback_playwright: bool = True,
    options: dict | None = None,
) -> tuple[str, str | None]:
    """
    便捷包装：返回 (text, image_path_or_None)。图片渲染失败时 image 为 None，
    调用方应直接把 text 作为纯文本发送。

    render_mode:
      - "off": 不渲染图片, 直接返回 (text, None) (跳过渲染缓存)。
      - "t2i" (默认): 优先用 AstrBot 内置文转图 (html_render_func 需可调用);
        失败且 render_fallback_playwright=True 时回退本地 Playwright 渲染。
      - "playwright": 直接走本地 Playwright 渲染 (html_render 不参与)。

    enable_cache=True (默认) 时按 hash(markdown+group) 缓存图片路径:
    相同内容再次渲染直接复用已有文件, t2i 与 playwright 结果共用同一缓存键。
    """
    mode = str(render_mode or RENDER_MODE_T2I)
    key = _render_cache_key(text, group_name) if enable_cache else None
    if key is not None:
        key += ":" + mode + ":" + str((options or {}).get("scale", "device"))
    if key is not None and mode != RENDER_MODE_OFF:
        cached = _RENDER_CACHE.get(key)
        if cached and os.path.exists(cached):
            return text, cached

    image_path: str | None = None
    if mode not in (RENDER_MODE_OFF, RENDER_MODE_PLAYWRIGHT):
        # t2i (未知模式按 t2i 处理): 具备 html_render 能力时优先
        if callable(html_render_func):
            html_content = render_html(text, group_name)
            image_path = await render_via_t2i(
                html_render_func, html_content, out_dir, options=options
            )
        if not image_path and not render_fallback_playwright:
            return text, None
    if image_path is None and mode != RENDER_MODE_OFF:
        image_path = await render_chronicle_image(text, group_name, out_dir)
    if image_path and key is not None:
        _RENDER_CACHE[key] = image_path
    return text, image_path
