"""人格解析器 —— 为史记 / 群友传记生成解析"该用谁的系统提示词"。

职责边界:
    - 本模块只负责解析人格文本(核心人格 / 默认人格 / 自定义提示词);
    - "中立史官"风格的系统提示词由生成模块(T6/T7)自己拼装, 与本模块无关;
    - 绝不注入到所有 LLM 请求 —— 只供本插件自己的生成调用使用(参见
      astrbot_plugin_portrayal 的 on_llm_request 反模式教训)。

模式:
    - off           -> 返回 None(中立史官)
    - core_persona  -> 优先使用配置指定核心人格, 缺失 / 失败时回退 AstrBot 默认人格
    - custom_prompt -> 直接使用配置中的自定义提示词

兼容性: 人格是 AstrBot 核心功能, 老版本可能没有 context.persona_manager,
此时一律返回 None, 保证生成流程不崩。
"""

import logging

logger = logging.getLogger(__name__)

PERSONA_MODE_OFF = "off"
PERSONA_MODE_CORE = "core_persona"
PERSONA_MODE_CUSTOM = "custom_prompt"


def _series_defaults(context) -> dict:
    try:
        if __package__ and "." in __package__:
            from ..series_panels import get_series_defaults
        else:
            from series_panels import get_series_defaults
    except ImportError:
        return {}
    return get_series_defaults(context)


async def resolve_system_prompt(context, config, umo: str | None = None) -> str | None:
    """解析用于史记 / 传记生成的系统提示词; 返回 None 表示按中立史官处理。

    Args:
        context: AstrBot 插件上下文(需含 persona_manager, 老版本可能没有)。
        config: 插件配置, 兼容 dict / AstrBotConfig / 普通对象。
        umo: 统一消息来源标识, 用于加载默认人格时的会话上下文。
    """
    mode = _cfg_get(config, "persona_mode", PERSONA_MODE_OFF)

    if mode == PERSONA_MODE_OFF:
        return None

    if mode == PERSONA_MODE_CUSTOM:
        return _non_empty(_cfg_get(config, "custom_persona_prompt", ""))

    if mode != PERSONA_MODE_CORE:
        logger.warning(f"未知的人格模式: {mode!r}, 本次生成按中立史官处理")
        return None

    # core_persona: 人格是 AstrBot 核心功能, 老版本可能没有 persona_manager
    persona_manager = getattr(context, "persona_manager", None)
    if persona_manager is None:
        logger.warning(
            "未检测到 persona_manager(人格为 AstrBot 核心功能, 可能版本过旧), "
            "本次生成按中立史官处理"
        )
        return None

    persona_id = str(_cfg_get(config, "persona_id", "") or "").strip()
    if not persona_id:
        persona_id = str(_series_defaults(context).get("persona_id", "") or "").strip()
    if not persona_id:
        logger.warning(
            "未配置有效的 persona_id, 无法加载核心人格, 本次生成按中立史官处理"
        )
        return None

    prompt = await _load_named_persona(persona_manager, persona_id)
    if not prompt:
        prompt = await _load_default_persona(persona_manager, umo)
    return _non_empty(prompt)


async def _load_named_persona(persona_manager, persona_id: str) -> str | None:
    """尝试加载指定核心人格的系统提示词; 任何失败都返回 None(绝不抛出)。"""
    get_persona = getattr(persona_manager, "get_persona", None)
    if not callable(get_persona):
        return None
    try:
        persona = await get_persona(persona_id)
    except Exception as e:
        logger.warning(f"获取核心人格 '{persona_id}' 失败: {e}")
        return None
    if persona is None:
        return None
    # 结果可能是 None / 对象 / 畸形数据, 一律用 getattr 提取
    return getattr(persona, "system_prompt", None)


async def _load_default_persona(persona_manager, umo: str | None) -> str | None:
    """回退: 加载 AstrBot 默认人格的 prompt; 任何失败都返回 None(绝不抛出)。"""
    get_default = getattr(persona_manager, "get_default_persona_v3", None)
    if not callable(get_default):
        return None
    try:
        default = await get_default(umo=umo)
    except Exception as e:
        logger.warning(f"获取默认人格失败: {e}")
        return None
    if default is None:
        return None
    get = getattr(default, "get", None)
    if not callable(get):
        logger.warning("默认人格结果不是 dict 形态, 无法提取 prompt")
        return None
    try:
        return get("prompt")
    except Exception as e:
        logger.warning(f"默认人格结果格式异常: {e}")
        return None


def _cfg_get(config, key: str, default):
    """兼容 dict / AstrBotConfig / 普通对象 的配置读取。"""
    getter = getattr(config, "get", None)
    if callable(getter):
        try:
            return getter(key, default)
        except Exception:
            return default
    return getattr(config, key, default)


def _non_empty(prompt) -> str | None:
    """空值 / 非字符串 / 纯空白 -> None, 其余原样返回。"""
    if not isinstance(prompt, str) or not prompt.strip():
        return None
    return prompt
