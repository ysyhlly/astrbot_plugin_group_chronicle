"""共享测试夹具 —— 在无 astrbot 环境下通过 sys.modules 桩件导入 main.py。

桩件范围 (最小但足以让 main.py 正常 import):
- astrbot.api.logger / AstrBotConfig
- astrbot.api.event.filter: command / permission_type / PermissionType / EventMessageType
- astrbot.api.event: filter / AstrMessageEvent / MessageChain
- astrbot.api.star: Star / Context / StarTools / register
- astrbot.api.message_components: At

StarTools.get_data_dir 指向系统临时目录, 避免污染插件源码目录。
"""

import logging
import os
import shutil
import sys
import tempfile
import types
from pathlib import Path

import pytest

_PLUGIN_ROOT = Path(__file__).resolve().parent.parent
if str(_PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT))

#: 桩件数据根目录 (StarTools.get_data_dir 指向这里)
_TEST_DATA_ROOT = os.path.join(
    tempfile.gettempdir(), "astrbot_plugin_group_chronicle_test_data"
)


@pytest.fixture(autouse=True, scope="session")
def _clean_test_data_root():
    """会话开始前清空桩件数据目录, 保证测试之间互不污染 (跨运行亦然)。"""
    shutil.rmtree(_TEST_DATA_ROOT, ignore_errors=True)
    yield


class PermissionType:
    """PermissionType 桩件 (真实值为 astrbot str 枚举, 语义等价)。"""

    MASTER = "master"
    ADMIN = "admin"
    GROUP_ADMIN = "group_admin"
    USER = "user"


class EventMessageType:
    """EventMessageType 桩件 (main.py 未使用, 仅为完整性)。"""

    ALL = "all"
    GROUP_MESSAGE = "GroupMessage"
    PRIVATE_MESSAGE = "PrivateMessage"
    NOTICE_MESSAGE = "NoticeMessage"


class At:
    """At 消息段桩件。"""

    def __init__(self, qq=None, name=None):
        self.qq = qq
        self.name = name

    def __repr__(self):
        return f"At(qq={self.qq!r}, name={self.name!r})"


class File:
    """Match the public File component constructor."""

    def __init__(self, name, file="", url=""):
        self.name = name
        self.file = file
        self.url = url


class MessageChain:
    """MessageChain 桩件: 记录 plain / file_image 链段, 可迭代。"""

    def __init__(self, chain=None):
        self._chain = list(chain) if chain else []

    def message(self, text):
        self._chain.append(("plain", str(text)))
        return self

    def file_image(self, path):
        self._chain.append(("file_image", str(path)))
        return self

    def __iter__(self):
        return iter(self._chain)

    def __len__(self):
        return len(self._chain)

    def __repr__(self):
        return f"MessageChain({self._chain!r})"


def _command(command_name, alias=None, **kwargs):
    """filter.command 桩件: 附加命令元数据, 原样返回函数 (镜像真实实现)。"""

    def decorator(func):
        func.__command__ = command_name
        func.__command_alias__ = list(alias or [])
        return func

    return decorator


def _permission_type(permission):
    """filter.permission_type 桩件: 附加权限元数据, 原样返回函数 (镜像真实实现)。"""

    def decorator(func):
        func.__permission_type__ = permission
        return func

    return decorator


class CustomFilter:
    """Match the public custom filter construction surface."""

    def __init__(self, raise_error=True, **kwargs):
        self.raise_error = raise_error


def _custom_filter(cls, **kwargs):
    def decorator(func):
        func.__custom_filter__ = cls
        return func

    return decorator


@pytest.fixture(autouse=True)
def _legacy_mode_by_default(monkeypatch):
    """Run v0.1.1 regression cases in the explicitly retained compatibility mode."""
    from main import GroupChroniclePlugin

    original = GroupChroniclePlugin.__init__

    def construct(self, context, config):
        if isinstance(config, dict):
            config.setdefault("history_enabled", False)
        original(self, context, config)

    monkeypatch.setattr(GroupChroniclePlugin, "__init__", construct)


def _install_astrbot_stubs() -> None:
    """向 sys.modules 注入 astrbot 桩件 (幂等)。"""
    if "astrbot" in sys.modules:
        return

    # --- astrbot.api.event.filter ---
    event_filter = types.ModuleType("astrbot.api.event.filter")
    event_filter.command = _command
    event_filter.permission_type = _permission_type
    event_filter.PermissionType = PermissionType
    event_filter.EventMessageType = EventMessageType
    event_filter.CustomFilter = CustomFilter
    event_filter.custom_filter = _custom_filter
    event_filter.llm_tool = lambda name=None, **kw: _command(name)
    event_filter.on_decorating_result = lambda **kw: lambda func: func

    # --- astrbot.api.event ---
    api_event = types.ModuleType("astrbot.api.event")
    api_event.filter = event_filter
    api_event.AstrMessageEvent = type("AstrMessageEvent", (), {})
    api_event.MessageChain = MessageChain
    api_event.EventMessageType = EventMessageType

    # --- astrbot.api.message_components ---
    message_components = types.ModuleType("astrbot.api.message_components")
    message_components.At = At
    message_components.File = File
    message_components.MessageChain = MessageChain

    # --- astrbot.api.star ---
    star_mod = types.ModuleType("astrbot.api.star")

    class Star:
        def __init__(self, context):
            self.context = context

    class Context:
        pass

    class StarTools:
        _DATA_ROOT = _TEST_DATA_ROOT

        @staticmethod
        def get_data_dir(plugin_name: str) -> str:
            return os.path.join(StarTools._DATA_ROOT, str(plugin_name))

    def register(*args, **kwargs):
        """@register 桩件: 记录元数据, 原样返回类。"""

        def decorator(cls):
            cls.__astrbot_plugin_name__ = args[0] if args else kwargs.get("name")
            return cls

        return decorator

    star_mod.Star = Star
    star_mod.Context = Context
    star_mod.StarTools = StarTools
    star_mod.register = register

    # --- astrbot.api ---
    api_mod = types.ModuleType("astrbot.api")
    api_mod.logger = logging.getLogger("astrbot.api.test")
    api_mod.AstrBotConfig = dict  # AstrBotConfig 与 dict.get 语义兼容
    api_mod.__path__ = []

    async def get_async(**kwargs):
        return kwargs.get("default", {})

    api_mod.sp = types.SimpleNamespace(get_async=get_async)

    # --- astrbot ---
    astrbot_mod = types.ModuleType("astrbot")
    astrbot_mod.__path__ = []

    for name, module in [
        ("astrbot", astrbot_mod),
        ("astrbot.api", api_mod),
        ("astrbot.api.event", api_event),
        ("astrbot.api.event.filter", event_filter),
        ("astrbot.api.star", star_mod),
        ("astrbot.api.message_components", message_components),
    ]:
        sys.modules[name] = module


_install_astrbot_stubs()
