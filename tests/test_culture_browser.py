"""Desktop/mobile browser acceptance with HTTP wired to the actual culture API.

The host page bridge and Dashboard login verifier are synthetic; store/service,
route response handling, authorization decision and frontend are production code.
"""
import asyncio
import functools
import html
import json
import os
import threading
import types
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import sys

import pytest

from core import culture_service
from .test_culture import culture, plugin, proposal, rows  # noqa: F401
from .test_history import U, source


@pytest.fixture
def wire_server(culture, monkeypatch, tmp_path):  # noqa: F811 - pytest fixture injection
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    def run(coro):
        return asyncio.run_coroutine_threadsafe(coro, loop).result(20)

    material = rows()
    raw = proposal(material, kind="nickname", subjects=["1"], timeline=[
        dict(text="成员解释蓝蘑菇用于项目延期", source_ids=[1], event_at=None),
        dict(text="随后另一位成员确认自己的使用方式", source_ids=[2], event_at=None),
    ])
    # Two senses provide a real merge target and exercise preserved revisions.
    other = proposal(material)["entries"][0]
    other["meaning"] = "项目延后的另一种解释"
    raw["entries"].append(other)
    async def model(*args, **kwargs):
        return json.dumps(raw)
    monkeypatch.setattr(culture_service, "call_llm", model)
    culture.plugin.context.get_config = lambda umo=None: {"dashboard": {"username": "tester"}}
    db = run(source(culture.plugin, material))
    memory = types.SimpleNamespace(initializer=types.SimpleNamespace(conversation_manager=types.SimpleNamespace(store=types.SimpleNamespace(connection=db))))
    culture.plugin.context.get_registered_star = lambda name: types.SimpleNamespace(activated=True, star_cls=memory) if name == "astrbot_plugin_dynamics_memory" else None
    run(culture.process_one(U))
    web_module = types.ModuleType("astrbot.api.web")
    web_module.request = types.SimpleNamespace(_request=None)
    web_module.json_response = lambda data, status_code=200: (status_code, data)
    auth_module = types.ModuleType("astrbot.dashboard.api.auth")
    async def require_user(request):
        if request.headers.get("X-Test-Admin") != "synthetic-admin":
            raise PermissionError("需要 Dashboard 管理员登录")
        return "tester"
    auth_module.require_dashboard_user = require_user
    monkeypatch.setitem(sys.modules, "astrbot.api.web", web_module)
    monkeypatch.setitem(sys.modules, "astrbot.dashboard.api.auth", auth_module)
    page_root = Path(__file__).resolve().parents[1] / "pages" / "culture"
    class Request:
        def __init__(self, headers, body):
            self.headers, self.payload = headers, body
        async def body(self):
            return self.payload
    class Handler(SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def api(self, overview=False):
            payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            web_module.request._request = Request(self.headers, payload)
            status, response = run(culture.plugin.culture_web.overview() if overview else culture.plugin.culture_web.handle())
            content = json.dumps(response, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(content)
        def do_GET(self):
            if self.path.startswith("/api/culture_overview"):
                self.api(True)
            elif self.path == "/sandbox":
                document = (page_root / "index.html").read_text(encoding="utf-8")
                document = document.replace('<link rel="stylesheet" href="style.css">', '<style>' + (page_root / "style.css").read_text(encoding="utf-8") + '</style>')
                document = document.replace('<script src="theme.js"></script>', '<script>' + (page_root / "theme.js").read_text(encoding="utf-8") + '</script>')
                bridge = """let seq=0;const waiting=new Map();window.addEventListener('message',e=>{if(e.source===parent&&e.data.cultureResponse&&waiting.has(e.data.id)){waiting.get(e.data.id)(e.data.result);waiting.delete(e.data.id);}});function call(path,body){return new Promise(resolve=>{const id=++seq;waiting.set(id,resolve);parent.postMessage({cultureRequest:true,id,path,body},'*');});}window.AstrBotPluginPage={ready:async()=>{},apiGet:path=>call(path),apiPost:(path,body)=>call(path,body)};"""
                document = document.replace('<script type="module" src="app.js"></script>', '<script>' + bridge + '</script><script type="module">' + (page_root / "app.js").read_text(encoding="utf-8") + '</script>')
                shell = '<!doctype html><html><body style="margin:0"><iframe style="width:100%;height:100vh;border:0" sandbox="allow-scripts allow-forms allow-downloads" srcdoc="' + html.escape(document, quote=True) + '"></iframe><script>'
                shell += "window.addEventListener('message',async e=>{const f=document.querySelector('iframe');if(e.source!==f.contentWindow||!e.data.cultureRequest)return;const d=e.data;const result=await(await fetch('/api/'+d.path,{method:d.body?'POST':'GET',headers:{'X-Test-Admin':'synthetic-admin','Content-Type':'application/json'},body:d.body?JSON.stringify(d.body):undefined})).json();f.contentWindow.postMessage({cultureResponse:true,id:d.id,result},'*');});</script></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(shell.encode())
            else:
                super().do_GET()
        def do_POST(self):
            if self.path == "/api/culture":
                self.api()
            else:
                self.send_error(404)
    server = HTTPServer(("127.0.0.1", 0), functools.partial(Handler, directory=str(page_root)))
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", culture
    finally:
        server.shutdown()
        server.server_close()
        run(db.close())
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()
        server_thread.join(5)


@pytest.mark.parametrize("size", [(1440, 1000), (390, 844)], ids=["desktop", "mobile"])
def test_browser_review_correction_merge_privacy_and_progress(wire_server, size):
    playwright = pytest.importorskip("playwright.sync_api")
    base = Path(os.environ.get("LOCALAPPDATA", "")) / "ms-playwright"
    browsers = sorted(base.glob("chromium-*/chrome-win64/chrome.exe"))
    if not browsers:
        pytest.skip("Local Chromium executable not available")
    url, service = wire_server
    from playwright.sync_api import expect
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch(executable_path=str(browsers[-1]), headless=True)
        page = browser.new_page(viewport={"width": size[0], "height": size[1]})
        errors = []
        page.on("pageerror", lambda err: errors.append(str(err)))
        page.add_init_script("""window.AstrBotPluginPage={ready:async()=>{},apiGet:async path=>(await fetch('/api/'+path,{headers:{'X-Test-Admin':'synthetic-admin'}})).json(),apiPost:async(path,body)=>(await fetch('/api/'+path,{method:'POST',headers:{'Content-Type':'application/json','X-Test-Admin':'synthetic-admin'},body:JSON.stringify(body)})).json()};""")
        page.goto(url)
        expect(page.locator("#entries .entry")).to_have_count(2)
        assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
        response = page.request.get(url + "/api/culture_overview")
        assert response.status == 403
        response = page.request.post(url + "/api/culture", data={"op":"action", "scope":U, "action":"pause"})
        assert response.status == 403
        page.get_by_role("button", name="待审与纠正", exact=True).click()
        expect(page.locator("#review-entries .entry")).to_have_count(1)
        page.locator("#review-entries button").click()
        expect(page.locator("#detail")).to_be_visible()
        expect(page.locator("#detail-body")).to_contain_text("起源未知")
        page.locator("#action-note").fill("已核对两条原文，外号无争议")
        page.locator("[data-action=approve]").click()
        expect(page.locator("#notice")).to_contain_text("审核通过已保存")
        page.locator("#edit-meaning").fill("用来指项目延期")
        page.locator("#action-note").fill("调整含义措辞")
        page.locator("[data-action=edit]").click()
        expect(page.locator("#notice")).to_contain_text("修订已保存")
        expect(page.locator("#detail-body summary")).to_contain_text("修订历史（2）")
        page.get_by_role("button", name="文化词库", exact=True).click()
        page.locator("#query").fill("蓝蘑菇")
        page.locator("#search").click()
        expect(page.locator("#entries .entry")).to_have_count(2)
        items = service.store.entries(U)[0]
        pending = next(e for e in items if e["status"] == "pending")
        target = next(e for e in items if e["status"] == "approved")
        page.locator("#entries .entry").filter(has_text=f"#{pending['id']} ").get_by_role("button").click()
        page.locator("#action-note").fill("核对为同一个含义，保留目标词条")
        page.locator("#merge-id").fill(str(target["id"]))
        page.on("dialog", lambda dialog: dialog.accept())
        page.locator("#merge").click()
        expect(page.locator("#merge-preview")).to_be_visible()
        page.locator("#confirm-merge").click()
        expect(page.locator("#notice")).to_contain_text("合并已保存")
        page.locator("#entries .entry").filter(has_text=f"#{target['id']} ").get_by_role("button").click()
        page.locator("#action-note").fill("合并后重新确认")
        page.locator("[data-action=approve]").click()
        expect(page.locator("#notice")).to_contain_text("审核通过已保存")
        output = Path(__file__).resolve().parents[2] / "browser-evidence"
        output.mkdir(exist_ok=True)
        page.screenshot(path=str(output / f"culture-{size[0]}-detail.png"), full_page=True)
        assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
        page.locator("#action-note").fill("管理员隐藏测试")
        page.locator("[data-action=hide]").click()
        expect(page.locator("#notice")).to_contain_text("隐藏已保存")
        expect(page.locator("#entries")).not_to_contain_text("项目延后")
        page.get_by_role("button", name="整理进度", exact=True).click()
        page.locator("#pause").click()
        expect(page.locator("#progress-content")).to_contain_text("已暂停")
        page.locator("#resume").click()
        expect(page.locator("#progress-content")).to_contain_text("未暂停")
        page.locator("#theme-toggle").click()
        expect(page.locator("html")).to_have_attribute("data-ui-theme", "night")
        page.screenshot(path=str(output / f"culture-{size[0]}-progress.png"), full_page=True)
        assert not errors
        browser.close()


def test_native_opaque_sandbox_inline_merge(wire_server):
    playwright = pytest.importorskip("playwright.sync_api")
    browsers = sorted((Path(os.environ.get("LOCALAPPDATA", "")) / "ms-playwright").glob("chromium-*/chrome-win64/chrome.exe"))
    if not browsers:
        pytest.skip("Local Chromium executable not available")
    from playwright.sync_api import expect
    url, service = wire_server
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch(executable_path=str(browsers[-1]), headless=True)
        page = browser.new_page(viewport={"width":1000,"height":900})
        errors = []
        page.on("pageerror", lambda err: errors.append(str(err)))
        page.goto(url + "/sandbox")
        frame = page.frame_locator("iframe")
        expect(frame.locator("#entries .entry")).to_have_count(2)
        items = service.store.entries(U)[0]
        pending = next(e for e in items if e["status"] == "pending")
        target = next(e for e in items if e["status"] == "approved")
        frame.locator("#entries .entry").filter(has_text=f"#{pending['id']} ").get_by_role("button").click()
        frame.locator("#action-note").fill("在宿主沙箱内确认合并")
        frame.locator("#merge-id").fill(str(target["id"]))
        frame.locator("#merge").click()
        expect(frame.locator("#merge-preview")).to_be_visible()
        frame.locator("#cancel-merge").click()
        expect(frame.locator("#merge-preview")).to_be_hidden()
        frame.locator("#merge").click()
        frame.locator("#confirm-merge").click()
        expect(frame.locator("#notice")).to_contain_text("合并已保存")
        assert service.store.entry(U, pending["id"])["status"] == "merged"
        assert not errors
        browser.close()
