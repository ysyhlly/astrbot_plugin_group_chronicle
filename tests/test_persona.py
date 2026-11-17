"""persona 模块单元测试 —— 仅依赖标准库, 无需安装 astrbot。

运行: python -m pytest tests/test_persona.py -v
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.persona import (
    PERSONA_MODE_CORE,
    PERSONA_MODE_CUSTOM,
    PERSONA_MODE_OFF,
    resolve_system_prompt,
)


class FakePersona:
    """模拟 get_persona 返回的对象, 带 system_prompt 属性。"""

    def __init__(self, system_prompt):
        self.system_prompt = system_prompt


class FakePersonaManager:
    """get_persona / get_default_persona_v3 行为可配置。

    未设置的错误/结果对应行为: 不抛异常 / 返回该值。默认均不配置,
    即两个方法都正常返回 None。
    """

    def __init__(
        self,
        persona_result=None,
        persona_error=None,
        default_result=None,
        default_error=None,
    ):
        self._persona_result = persona_result
        self._persona_error = persona_error
        self._default_result = default_result
        self._default_error = default_error

    async def get_persona(self, persona_id):
        if self._persona_error is not None:
            raise self._persona_error
        return self._persona_result

    async def get_default_persona_v3(self, umo=None):
        if self._default_error is not None:
            raise self._default_error
        return self._default_result


def run(coro):
    return asyncio.run(coro)


def make_config(**kwargs):
    cfg = {"persona_mode": PERSONA_MODE_OFF}
    cfg.update(kwargs)
    return cfg


def make_context(persona_manager=None):
    if persona_manager is None:
        # 完全没有 persona_manager 属性, 模拟老版本 AstrBot
        return SimpleNamespace()
    return SimpleNamespace(persona_manager=persona_manager)


# ---------- 1. mode=off ----------


def test_mode_off_returns_none():
    ctx = make_context(FakePersonaManager())
    cfg = make_config(persona_mode=PERSONA_MODE_OFF, persona_id="p1")
    assert run(resolve_system_prompt(ctx, cfg, umo="g:1")) is None


# ---------- 2. core_persona 命中核心人格 ----------


def test_core_persona_uses_system_prompt():
    mgr = FakePersonaManager(persona_result=FakePersona(system_prompt="X"))
    cfg = make_config(persona_mode=PERSONA_MODE_CORE, persona_id="p1")
    assert run(resolve_system_prompt(make_context(mgr), cfg, umo="g:1")) == "X"


# ---------- 3. get_persona 失败 -> 回退默认人格 ----------


def test_get_persona_fails_falls_back_to_default():
    cfg = make_config(persona_mode=PERSONA_MODE_CORE, persona_id="p1")

    # get_persona 抛异常
    mgr = FakePersonaManager(
        persona_error=RuntimeError("boom"), default_result={"prompt": "Y"}
    )
    assert run(resolve_system_prompt(make_context(mgr), cfg, umo="g:1")) == "Y"

    # get_persona 返回 None
    mgr = FakePersonaManager(persona_result=None, default_result={"prompt": "Y"})
    assert run(resolve_system_prompt(make_context(mgr), cfg, umo="g:1")) == "Y"


# ---------- 4. 两级都失败 -> None, 不崩溃 ----------


def test_both_fail_returns_none():
    mgr = FakePersonaManager(
        persona_error=RuntimeError("boom"), default_error=RuntimeError("bad")
    )
    cfg = make_config(persona_mode=PERSONA_MODE_CORE, persona_id="p1")
    assert run(resolve_system_prompt(make_context(mgr), cfg, umo="g:1")) is None

    # get_persona 不存在 / 默认也失败
    mgr2 = FakePersonaManager(default_error=RuntimeError("bad"))
    assert run(resolve_system_prompt(make_context(mgr2), cfg, umo="g:1")) is None


# ---------- 5. custom_prompt 命中 ----------


def test_custom_prompt_returns_prompt():
    cfg = make_config(persona_mode=PERSONA_MODE_CUSTOM, custom_persona_prompt="Z")
    assert run(resolve_system_prompt(SimpleNamespace(), cfg, umo="g:1")) == "Z"


# ---------- 6. custom_prompt 为空 ----------


def test_custom_prompt_empty_returns_none():
    cfg = make_config(persona_mode=PERSONA_MODE_CUSTOM, custom_persona_prompt="")
    assert run(resolve_system_prompt(SimpleNamespace(), cfg, umo="g:1")) is None

    # 键完全缺失
    cfg2 = make_config(persona_mode=PERSONA_MODE_CUSTOM)
    assert run(resolve_system_prompt(SimpleNamespace(), cfg2, umo="g:1")) is None


# ---------- 7. persona_manager 属性缺失 ----------


def test_no_persona_manager_returns_none():
    cfg = make_config(persona_mode=PERSONA_MODE_CORE, persona_id="p1")
    assert run(resolve_system_prompt(SimpleNamespace(), cfg, umo="g:1")) is None


# ---------- 8. 纯空白提示词 ----------


def test_whitespace_prompt_returns_none():
    # 核心人格返回纯空白 system_prompt
    mgr = FakePersonaManager(persona_result=FakePersona(system_prompt="   \n  "))
    cfg = make_config(persona_mode=PERSONA_MODE_CORE, persona_id="p1")
    assert run(resolve_system_prompt(make_context(mgr), cfg, umo="g:1")) is None

    # 自定义提示词纯空白同样返回 None
    cfg2 = make_config(persona_mode=PERSONA_MODE_CUSTOM, custom_persona_prompt="   ")
    assert run(resolve_system_prompt(SimpleNamespace(), cfg2, umo="g:1")) is None
