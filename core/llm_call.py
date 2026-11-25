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
import hashlib
import inspect
import json

from astrbot.api import logger

try:
    from ..series_panels import (
        find_provider_silently,
        is_provider_budget_busy_error,
        resolve_series_provider,
        series_llm_call,
    )
except ImportError:
    try:
        from series_panels import (
            find_provider_silently,
            is_provider_budget_busy_error,
            resolve_series_provider,
            series_llm_call,
        )
    except Exception:  # pragma: no cover - standalone fallback
        def find_provider_silently(context, provider_id: str, *, allow_fallback: bool = False):
            _ = allow_fallback
            getter = getattr(context, "get_provider_by_id", None)
            return getter(provider_id) if callable(getter) else None

        def is_provider_budget_busy_error(_exc: BaseException | None) -> bool:
            return False

        def resolve_series_provider(
            _context,
            explicit_provider: str = "",
            fallback_provider: str = "",
            *,
            role: str = "reply",
        ) -> str:
            _ = role
            return (
                str(explicit_provider or "").strip()
                or str(fallback_provider or "").strip()
            )

        async def series_llm_call(
            _context,
            _provider_id: str,
            _plugin_id: str,
            _purpose: str,
            invoke,
            *,
            acquire_timeout: float | None = 15.0,
        ):
            _ = acquire_timeout
            return await invoke()

_RETRY_ATTEMPTS = 2  # 首次 + 1 次重试
_RETRY_SLEEP_SECONDS = 1.0
_DEFAULT_CALL_TIMEOUT_SEC = 120.0
_TRUNCATE_MARKER = "\n...[中间素材已按预算截断]...\n"


def _truncate_prompt_preserving_tail(prompt_text: str, budget: int) -> str:
    """Truncate oversized prompts in the middle so trailing instructions & structure stay intact."""
    text = str(prompt_text)
    limit = max(1, int(budget))
    if len(text) <= limit:
        return text
    marker_len = len(_TRUNCATE_MARKER)
    if limit <= marker_len + 64:
        return text[:limit]
    usable = limit - marker_len
    tail_len = min(2500, max(128, usable // 4))
    head_len = max(1, usable - tail_len)
    return text[:head_len] + _TRUNCATE_MARKER + text[-tail_len:]


def encode_compact(value) -> str:
    """Compact UTF-8 JSON preserving explicit key order for cache-friendly payloads."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def build_cache_anchor_contexts(
    system_prompt: str | None,
    *,
    task: str = "group_chronicle",
    as_messages: bool = False,
) -> list:
    """Build a tiny deterministic 2-message static anchor keyed only on system_prompt hash.

    Enables implicit prefix caching on Gemini 3.8 Flash (which misses when contexts=[]
    and contents[0] changes every call) while remaining compatible with DeepSeek, OpenAI,
    and stock AstrBot providers. When ``as_messages=True``, returns real AstrBot
    ``Message`` instances if available (matching ``Context.llm_generate`` typing).
    """
    text = str(system_prompt or "")
    if not text.strip():
        return []
    sha12 = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    anchor = f"[cache_anchor task={task} prefix={sha12}]"
    if as_messages:
        try:
            from astrbot.core.agent.message import Message

            return [
                Message(role="user", content=anchor),
                Message(role="assistant", content="OK"),
            ]
        except Exception:
            pass
    return [
        {"role": "user", "content": anchor},
        {"role": "assistant", "content": "OK"},
    ]


def _accepts_kwarg(fn, name: str) -> bool:
    try:
        parameters = inspect.signature(fn).parameters
    except (ValueError, TypeError):
        return False
    return name in parameters or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()
    )


class LLMUnavailableError(Exception):
    """未配置可用的 LLM: 所有调用路径均不可用。"""


class _ProviderExecutionError(Exception):
    """显式配置的 Provider 存在但执行失败（超时/5xx/429/策略拒绝/空响应），禁止静默回退主聊模型。"""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


def _is_missing_error(exc: BaseException) -> bool:
    """Distinguish missing provider/API errors from runtime execution failures."""
    if isinstance(exc, (AttributeError, TypeError, NotImplementedError, LookupError)):
        return True
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    if "notfound" in name or "not_found" in name:
        return True
    return any(
        token in msg
        for token in (
            "provider not found",
            "unknown provider",
            "invalid provider",
            "未找到 provider",
            "未找到提供商",
            "provider 不存在",
            "提供商不存在",
        )
    )


def _is_non_retryable_text(text: str) -> bool:
    msg = str(text or "").lower()
    return any(
        token in msg
        for token in (
            "content_filter",
            "content filter",
            "safety",
            "policy",
            "moderation",
            "refused",
            "blocked",
            "unauthorized",
            "forbidden",
            "401",
            "403",
            "timed out",
            "timeout",
        )
    )


def _is_non_retryable_error(exc: BaseException) -> bool:
    """Identify execution failures that should not be amplified via retries."""
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return True
    name = type(exc).__name__.lower()
    if any(k in name for k in ("timeout", "policy", "safety", "moderation")):
        return True
    return _is_non_retryable_text(str(exc))


def is_budget_busy_error(exc: BaseException | None) -> bool:
    """Return True if *exc* or its direct cause is a ProviderBudgetBusyError."""
    cur = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if is_provider_budget_busy_error(cur):
            return True
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return False


def budget_busy_retry_after(exc: BaseException | None) -> float:
    """Extract structured retry_after seconds from a busy error chain."""
    cur = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        val = float(getattr(cur, "retry_after", 0.0) or 0.0)
        if val > 0:
            return val
        cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
    return 0.0


def _is_error_response(resp) -> bool:
    return str(getattr(resp, "role", "") or "").strip().lower() == "err"


async def _lookup_provider_by_id(context, provider_id: str) -> tuple[bool | None, object | None]:
    """Return ``(exists_or_unknown, provider_obj)`` without cold-start WARN spam."""
    if not provider_id or context is None:
        return None, None
    pm = getattr(context, "provider_manager", None)
    if pm is not None:
        prov = find_provider_silently(context, provider_id, allow_fallback=False)
        return (True, prov) if prov is not None else (False, None)
    getter = getattr(context, "get_provider_by_id", None)
    if not callable(getter):
        return None, None
    try:
        prov = getter(provider_id)
        if inspect.isawaitable(prov):
            prov = await prov
    except Exception as exc:  # noqa: BLE001
        if _is_missing_error(exc):
            return False, None
        logger.warning(f"[llm_call] get_provider_by_id('{provider_id}') 失败: {exc}")
        return None, None
    return (True, prov) if prov is not None else (False, None)


async def call_llm(
    context,
    umo: str,
    prompt: str,
    system_prompt: str | None = None,
    provider_id: str | None = None,
    *,
    call_timeout: float | None = None,
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
    explicit_configured = bool(str(provider_id or "").strip())
    configured = resolve_series_provider(context, provider_id or "", role="reply") or None
    # Apply a final budget across every generation path, including custom prompts.
    # Normalize CRLF -> LF on system_prompt so prefix hashes stay byte-identical.
    system_prompt = (
        str(system_prompt).replace("\r\n", "\n").replace("\r", "\n")[:8000]
        if system_prompt
        else None
    )
    prompt = _truncate_prompt_preserving_tail(
        prompt, max(1, 24000 - len(system_prompt or ""))
    )
    effective_timeout = (
        max(0.05, float(call_timeout))
        if call_timeout is not None
        else _DEFAULT_CALL_TIMEOUT_SEC
    )
    last_exc: Exception | None = None
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            text = await _try_call_once(
                context,
                umo,
                prompt,
                system_prompt,
                configured,
                explicit_configured=explicit_configured,
                call_timeout=effective_timeout,
            )
        except _ProviderExecutionError as e:
            last_exc = e
            logger.warning(
                f"[llm_call] 指定 Provider 执行失败 (第 {attempt}/{_RETRY_ATTEMPTS} 次, "
                f"retryable={e.retryable}): {e}"
            )
            if not e.retryable or attempt >= _RETRY_ATTEMPTS:
                break
            await asyncio.sleep(_RETRY_SLEEP_SECONDS)
            continue
        except Exception as e:  # noqa: BLE001
            if is_provider_budget_busy_error(e):
                logger.warning(f"[llm_call] 共享模型队列繁忙，放弃重试并延后: {e}")
                err = LLMUnavailableError("共享模型队列繁忙，已延后本次生成")
                err.is_provider_budget_busy = True
                err.retry_after = float(getattr(e, "retry_after", 0.0) or 0.0)
                err.reason = str(getattr(e, "reason", "busy") or "busy")
                raise err from e
            last_exc = e
            logger.warning(
                f"[llm_call] LLM 调用失败 (第 {attempt}/{_RETRY_ATTEMPTS} 次), "
                f"类型: {type(e).__name__}, 详情: {e}"
            )
            if _is_non_retryable_error(e):
                break
            if attempt < _RETRY_ATTEMPTS:
                await asyncio.sleep(_RETRY_SLEEP_SECONDS)
            continue
        # token 计量: CJK 近似估算 (每 2 字符约 1 token)
        est = len(prompt) // 2 + len(text) // 2
        logger.info(f"[llm_call] 调用成功: tokens≈{est}")
        return text

    assert last_exc is not None
    logger.error(f"[llm_call] LLM 调用全部失败: {last_exc}")
    if isinstance(last_exc, _ProviderExecutionError):
        raise LLMUnavailableError("指定的 LLM 服务暂不可用，请稍后重试") from last_exc
    raise LLMUnavailableError("未配置可用的 LLM")


async def _try_call_once(
    context,
    umo: str,
    prompt: str,
    system_prompt: str | None,
    configured_id: str | None,
    *,
    explicit_configured: bool = False,
    call_timeout: float = _DEFAULT_CALL_TIMEOUT_SEC,
) -> str:
    """单次尝试: 配置 Provider → 会话 Provider → 默认 Provider。

    当用户显式配置了 ``llm_provider_id`` 且该 Provider 存在时，执行失败
    （超时/5xx/429/策略拒绝/空响应）禁止静默回退到会话默认主聊模型。
    """
    # 1. 插件配置的 Provider (若有)
    if configured_id:
        logger.info(f"[llm_call] 使用配置的 Provider: {configured_id}")
        exists, provider_obj = await _lookup_provider_by_id(context, configured_id)
        if exists is True and getattr(provider_obj, "is_systemone_provider", False) is True:
            logger.warning("Decision providers cannot generate chronicles")
            if explicit_configured:
                raise _ProviderExecutionError(
                    f"configured provider '{configured_id}' is a decision provider",
                    retryable=False,
                )
        elif exists is False:
            logger.warning(
                f"[llm_call] 配置的 Provider '{configured_id}' 不存在, 回退会话默认"
            )
        else:
            # Both explicitly configured and series-inherited providers must fail
            # fast on non-retryable execution errors (TimeoutError, 401/403,
            # policy/safety refusals) instead of amplifying into 4-8x fallback retries.
            text, invoked = await _try_llm_generate(
                context,
                configured_id,
                prompt,
                system_prompt,
                raise_on_execution_error=explicit_configured,
                raise_on_non_retryable=True,
                provider_obj=provider_obj,
                return_invoked=True,
                call_timeout=call_timeout,
            )
            if text is not None:
                return text
            text = await _try_provider_text_chat(
                context,
                configured_id,
                prompt,
                system_prompt,
                by_id=True,
                raise_on_execution_error=explicit_configured,
                raise_on_non_retryable=True,
                provider_obj=provider_obj,
                call_timeout=call_timeout,
            )
            if text is not None:
                return text
            if explicit_configured and (exists is True or invoked):
                raise _ProviderExecutionError(
                    f"configured provider '{configured_id}' failed to produce a valid response",
                    retryable=True,
                )
            logger.warning(
                f"[llm_call] 配置的 Provider '{configured_id}' 不可用, 回退会话默认"
            )

    # 2. 当前会话 Provider (新接口)
    session_id = await _resolve_session_provider_id(context, umo)
    if session_id and session_id != configured_id:
        text = await _try_llm_generate(
            context,
            session_id,
            prompt,
            system_prompt,
            raise_on_non_retryable=True,
            call_timeout=call_timeout,
        )
        if text is not None:
            return text

    # 3. 旧接口: 当前正在使用的 Provider
    text = await _try_provider_text_chat(
        context,
        umo,
        prompt,
        system_prompt,
        by_id=False,
        raise_on_non_retryable=True,
        call_timeout=call_timeout,
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
    context,
    provider_id: str | None,
    prompt: str,
    system_prompt: str | None,
    *,
    raise_on_execution_error: bool = False,
    raise_on_non_retryable: bool = False,
    provider_obj=None,
    return_invoked: bool = False,
    call_timeout: float = _DEFAULT_CALL_TIMEOUT_SEC,
):
    """新版统一接口调用。"""
    def _ret(val: str | None, invoked: bool):
        return (val, invoked) if return_invoked else val

    if not provider_id:
        return _ret(None, False)
    if provider_obj is not None:
        if getattr(provider_obj, "is_systemone_provider", False) is True:
            logger.warning("Decision providers cannot generate chronicles")
            return _ret(None, False)
    else:
        getter = getattr(context, "get_provider_by_id", None)
        if callable(getter):
            try:
                provider = getter(provider_id)
                if inspect.isawaitable(provider):
                    provider = await provider
                if getattr(provider, "is_systemone_provider", False) is True:
                    logger.warning("Decision providers cannot generate chronicles")
                    return _ret(None, False)
            except Exception:
                return _ret(None, False)
    llm_generate = getattr(context, "llm_generate", None)
    if not callable(llm_generate):
        return _ret(None, False)
    try:
        gen_kwargs = {
            "chat_provider_id": provider_id,
            "prompt": prompt,
            "system_prompt": system_prompt,
        }
        anchor_contexts = build_cache_anchor_contexts(
            system_prompt, task="group_chronicle", as_messages=True
        )
        if anchor_contexts and _accepts_kwarg(llm_generate, "contexts"):
            gen_kwargs["contexts"] = anchor_contexts
        resp = await asyncio.wait_for(
            series_llm_call(
                context,
                provider_id,
                "astrbot_plugin_group_chronicle",
                "group_chronicle",
                lambda: llm_generate(**gen_kwargs),
            ),
            timeout=call_timeout,
        )
        if resp is None:
            return _ret(None, True)
        if _is_error_response(resp):
            err_text = str(getattr(resp, "completion_text", "") or "")
            non_retryable = _is_non_retryable_text(err_text)
            if raise_on_execution_error or (raise_on_non_retryable and non_retryable):
                raise _ProviderExecutionError(
                    f"llm_generate returned error response for '{provider_id}'",
                    retryable=not non_retryable,
                )
            return _ret(None, True)
        text = getattr(resp, "completion_text", None)
        if not isinstance(text, str) or not text.strip():
            if raise_on_execution_error:
                raise _ProviderExecutionError(
                    f"llm_generate returned empty completion for '{provider_id}'",
                    retryable=True,
                )
            return _ret(None, True)
        return _ret(text, True)
    except _ProviderExecutionError:
        raise
    except Exception as e:  # noqa: BLE001
        if is_provider_budget_busy_error(e):
            raise
        if not _is_missing_error(e) and (
            raise_on_execution_error
            or (raise_on_non_retryable and _is_non_retryable_error(e))
        ):
            raise _ProviderExecutionError(
                f"llm_generate failed for '{provider_id}': {type(e).__name__}: {e}",
                retryable=not _is_non_retryable_error(e),
            ) from e
        logger.warning(f"[llm_call] llm_generate 调用失败, 将尝试旧接口回退: {e}")
        return _ret(None, False)


async def _try_provider_text_chat(
    context,
    key: str | None,
    prompt: str,
    system_prompt: str | None,
    *,
    by_id: bool,
    raise_on_execution_error: bool = False,
    raise_on_non_retryable: bool = False,
    provider_obj=None,
    call_timeout: float = _DEFAULT_CALL_TIMEOUT_SEC,
) -> str | None:
    """旧接口 text_chat。

    by_id=True:  key 为 provider_id, 用 get_provider_by_id
    by_id=False: key 为 umo, 用 get_using_provider(umo=key)
    """
    if by_id:
        if not key:
            return None
        if provider_obj is not None:
            provider = provider_obj
        else:
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
        chat_kwargs = {"prompt": prompt, "system_prompt": system_prompt}
        anchor_contexts = build_cache_anchor_contexts(
            system_prompt, task="group_chronicle", as_messages=False
        )
        if anchor_contexts and _accepts_kwarg(text_chat, "contexts"):
            chat_kwargs["contexts"] = anchor_contexts
        label = (
            str(key or "").strip()
            if by_id
            else str(
                getattr(getattr(provider, "meta", None), "id", None)
                or getattr(provider, "provider_id", None)
                or getattr(provider, "id", None)
                or "default"
            ).strip()
        )
        resp = await asyncio.wait_for(
            series_llm_call(
                context,
                label or "default",
                "astrbot_plugin_group_chronicle",
                "group_chronicle",
                lambda: text_chat(**chat_kwargs),
            ),
            timeout=call_timeout,
        )
        if resp is None or _is_error_response(resp):
            err_text = str(getattr(resp, "completion_text", "") or "") if resp is not None else ""
            non_retryable = bool(resp is not None and _is_error_response(resp) and _is_non_retryable_text(err_text))
            if raise_on_execution_error or (raise_on_non_retryable and non_retryable):
                raise _ProviderExecutionError(
                    f"text_chat returned empty/error response for '{key}'",
                    retryable=not non_retryable,
                )
            return None
        text = resp if isinstance(resp, str) else getattr(resp, "completion_text", None)
        if isinstance(text, str) and text.strip():
            return text
        if raise_on_execution_error:
            raise _ProviderExecutionError(
                f"text_chat returned empty completion for '{key}'",
                retryable=True,
            )
        return None
    except _ProviderExecutionError:
        raise
    except Exception as e:  # noqa: BLE001
        if is_provider_budget_busy_error(e):
            raise
        if not _is_missing_error(e) and (
            raise_on_execution_error
            or (raise_on_non_retryable and _is_non_retryable_error(e))
        ):
            raise _ProviderExecutionError(
                f"text_chat failed for '{key}': {type(e).__name__}: {e}",
                retryable=not _is_non_retryable_error(e),
            ) from e
        logger.warning(f"[llm_call] text_chat 失败: {e}")
        return None
