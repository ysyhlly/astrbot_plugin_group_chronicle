"""共享 LLM 调用助手 —— 群史记 / 成员传记共用的统一入口。

调用策略:
1. 若配置了 provider_id: 优先用该 Provider
   - 新接口: context.llm_generate(chat_provider_id=provider_id, ...)
   - 旧接口: context.get_provider_by_id(provider_id).text_chat(...)
2. 未配置时: 用当前会话 Provider
   - context.get_current_chat_provider_id(umo) + llm_generate
3. 最终回退: context.get_using_provider(umo).text_chat(...)
4. 全部失败重试 1 次后抛 LLMUnavailableError

设计约束:
- 本模块不 import astrbot 任何符号, 纯属性探测 + getattr 守卫,
  因此单元测试可在无 astrbot 环境下用假 context 运行。
"""

from __future__ import annotations

import asyncio
import logging
import inspect

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS = 2  # 首次 + 1 次重试
_RETRY_SLEEP_SECONDS = 1.0


class LLMUnavailableError(Exception):
    """未配置可用的 LLM: 所有调用路径均不可用。"""


async def call_llm(
    context,
    umo: str,
    prompt: str,
    system_prompt: str | None = None,
    provider_id: str | None = None,
) -> str:
    """调用 LLM 生成文本。

    Args:
        context: AstrBot 插件上下文。
        umo: unified_msg_origin 会话标识。
        prompt: 用户侧提示词。
        system_prompt: 系统提示词, 可为 None。
        provider_id: 可选。插件配置的 LLM Provider ID; 留空则用会话默认。

    Returns:
        LLM 生成的文本 (str)。

    Raises:
        LLMUnavailableError: 所有路径均失败且重试后仍失败。
    """
    configured = (provider_id or "").strip() or None
    # Apply a final budget across every generation path, including custom prompts.
    system_prompt = str(system_prompt)[:8000] if system_prompt else None
    prompt = str(prompt)[: max(1, 24000 - len(system_prompt or ""))]
    last_exc: Exception | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            text = await _try_call_once(context, umo, prompt, system_prompt, configured)
        except Exception as e:  # noqa: BLE001
            last_exc = e
            logger.warning(
                f"[llm_call] LLM 调用失败 (第 {attempt}/{_RETRY_ATTEMPTS} 次), "
                f"类型: {type(e).__name__}, 详情: {e}"
            )
            if attempt < _RETRY_ATTEMPTS:
                await asyncio.sleep(_RETRY_SLEEP_SECONDS)
            continue
        # token 计量: CJK 近似估算 (每 2 字符约 1 token)
        est = len(prompt) // 2 + len(text) // 2
        logger.info(f"[llm_call] 调用成功: tokens≈{est}")
        return text

    assert last_exc is not None
    logger.error(f"[llm_call] LLM 调用全部失败: {last_exc}")
    raise LLMUnavailableError("未配置可用的 LLM")


async def _try_call_once(
    context,
    umo: str,
    prompt: str,
    system_prompt: str | None,
    configured_id: str | None,
) -> str:
    """单次尝试: 配置 Provider → 会话 Provider → 默认 Provider。"""
    # 1. 插件配置的 Provider (若有)
    if configured_id:
        logger.info(f"[llm_call] 使用配置的 Provider: {configured_id}")
        text = await _try_llm_generate(context, configured_id, prompt, system_prompt)
        if text is not None:
            return text
        text = await _try_provider_text_chat(
            context, configured_id, prompt, system_prompt, by_id=True
        )
        if text is not None:
            return text
        logger.warning(
            f"[llm_call] 配置的 Provider '{configured_id}' 不可用, 回退会话默认"
        )

    # 2. 当前会话 Provider (新接口)
    session_id = await _resolve_session_provider_id(context, umo)
    text = await _try_llm_generate(context, session_id, prompt, system_prompt)
    if text is not None:
        return text

    # 3. 旧接口: 当前正在使用的 Provider
    text = await _try_provider_text_chat(
        context, umo, prompt, system_prompt, by_id=False
    )
    if text is not None:
        return text

    raise RuntimeError("未找到可用的 LLM Provider")


async def _resolve_session_provider_id(context, umo: str) -> str | None:
    """通过新版 API 获取当前会话 Provider ID。"""
    get_pid = getattr(context, "get_current_chat_provider_id", None)
    if not callable(get_pid):
        return None
    try:
        provider_id = await get_pid(umo=umo)
        return provider_id if provider_id else None
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[llm_call] 获取会话 Provider ID 失败, 将尝试回退: {e}")
        return None


async def _try_llm_generate(
    context, provider_id: str | None, prompt: str, system_prompt: str | None
) -> str | None:
    """新版统一接口调用。"""
    if not provider_id:
        return None
    getter = getattr(context, "get_provider_by_id", None)
    if callable(getter):
        try:
            provider = getter(provider_id)
            if inspect.isawaitable(provider):
                provider = await provider
            if getattr(provider, "is_systemone_provider", False) is True:
                logger.warning("Decision providers cannot generate chronicles")
                return None
        except Exception:
            return None
    llm_generate = getattr(context, "llm_generate", None)
    if not callable(llm_generate):
        return None
    try:
        resp = await llm_generate(
            chat_provider_id=provider_id,
            prompt=prompt,
            system_prompt=system_prompt,
        )
        if resp is None:
            return None
        text = getattr(resp, "completion_text", None)
        if not isinstance(text, str) or not text.strip():
            return None
        return text
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[llm_call] llm_generate 调用失败, 将尝试旧接口回退: {e}")
        return None


async def _try_provider_text_chat(
    context,
    key: str | None,
    prompt: str,
    system_prompt: str | None,
    *,
    by_id: bool,
) -> str | None:
    """旧接口 text_chat。

    by_id=True:  key 为 provider_id, 用 get_provider_by_id
    by_id=False: key 为 umo, 用 get_using_provider(umo=key)
    """
    if by_id:
        if not key:
            return None
        getter = getattr(context, "get_provider_by_id", None)
        if not callable(getter):
            return None
        try:
            provider = getter(key)
            if inspect.isawaitable(provider):
                provider = await provider
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[llm_call] get_provider_by_id('{key}') 失败: {e}")
            return None
    else:
        getter = getattr(context, "get_using_provider", None)
        if not callable(getter):
            return None
        try:
            provider = getter(umo=key) if key else getter()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[llm_call] get_using_provider 失败: {e}")
            return None

    if provider is None or getattr(provider, "is_systemone_provider", False) is True:
        return None
    text_chat = getattr(provider, "text_chat", None)
    if not callable(text_chat):
        return None
    try:
        resp = await text_chat(prompt=prompt, system_prompt=system_prompt)
        if resp is None:
            return None
        text = resp if isinstance(resp, str) else getattr(resp, "completion_text", None)
        return text if isinstance(text, str) and text.strip() else None
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[llm_call] text_chat 失败: {e}")
        return None
