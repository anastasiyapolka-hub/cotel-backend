from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional, Protocol

from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

try:
    from google import genai as _google_genai
    from google.genai import types as _google_genai_types
    _GENAI_AVAILABLE = True
except ImportError:  # SDK not installed yet — adapter raises clearly on use
    _google_genai = None  # type: ignore[assignment]
    _google_genai_types = None  # type: ignore[assignment]
    _GENAI_AVAILABLE = False

from .usage import (
    LlmUsage,
    TOKENS_SOURCE_API,
    estimate_chars_usage,
)


# ---------------------------------------------------------------------------
# Per-provider adapter layer
# ---------------------------------------------------------------------------
#
# Each adapter knows how to:
#   1) call its own provider's chat-completion API
#   2) extract a normalized `LlmUsage` from the response
#   3) extract a finish/stop reason (best-effort, for diagnostics)
#
# Downstream code (`llm/service.py`) MUST go through this adapter layer
# and MUST NOT touch provider response objects directly. Adding a new
# provider (Gemini, Mistral, DeepSeek, etc.) = write a new adapter class
# and register it in `_ADAPTERS` below. Nothing else in the codebase
# needs to change.
# ---------------------------------------------------------------------------


class LlmProviderAdapter(Protocol):
    """Contract every provider adapter must satisfy."""

    provider_name: str

    async def complete(
        self,
        *,
        provider_model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_output_tokens: int,
    ) -> tuple[str, LlmUsage, Optional[str]]:
        """
        Run a single-turn chat completion.

        Returns: (text, usage, raw_finish_reason)
        """
        ...


# ---------------------------------------------------------------------------
# OpenAI adapter
# ---------------------------------------------------------------------------

_openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

log = logging.getLogger(__name__)


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _is_openai_reasoning_model(provider_model: str) -> bool:
    """
    Detect whether an OpenAI model supports the `reasoning_effort` /
    `reasoning` parameter (i.e. is a GPT-5.x or o-series reasoning model).
    Conservative: we only flip it on for models we KNOW are reasoning
    models, because passing the param to a non-reasoning model returns
    a 400. GPT-4.x / 4.1-* are NOT reasoning models.
    """
    name = (provider_model or "").lower()
    if name.startswith("gpt-5"):
        return True
    if name.startswith("o1") or name.startswith("o3") or name.startswith("o4"):
        return True
    return False


class OpenAiAdapter:
    """
    OpenAI Chat Completions adapter.

    Response usage shape (chat.completions.create):
        completion.usage.prompt_tokens
        completion.usage.completion_tokens
        completion.usage.total_tokens

    Most OpenAI-compatible providers (e.g. DeepSeek, Mistral via OpenAI
    SDK) follow the same shape, but DO NOT assume that — verify on each
    new provider before reusing this adapter.

    Reasoning models (GPT-5.x, o-series): we explicitly set
    `reasoning_effort="minimal"` to avoid the same trap that bit us with
    Gemini 2.5 Flash — reasoning tokens count as output and silently
    truncate the visible answer. For summarization workloads the
    accuracy lift from reasoning is small (~3-7%) and not worth the 2-4x
    cost or the truncation risk. If we later expose a "deep analysis"
    tier, opt back in there explicitly.
    """

    provider_name = "openai"

    async def complete(
        self,
        *,
        provider_model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_output_tokens: int,
    ) -> tuple[str, LlmUsage, Optional[str]]:
        is_reasoning = _is_openai_reasoning_model(provider_model)

        call_kwargs: dict[str, Any] = {
            "model": provider_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        # ===== Output-length cap =====
        # Reasoning models (GPT-5.x, o-series) deprecated `max_tokens` and
        # require `max_completion_tokens` instead. Classic GPT-4.x still
        # uses `max_tokens`. Picking the wrong one returns:
        #   400 unsupported_parameter: 'max_tokens' is not supported
        # so we MUST route on model name. If the API rejects our pick
        # for some reason (older variant, edge case), the except clause
        # below retries with the opposite key.
        #
        # Reasoning models also include hidden chain-of-thought tokens
        # INSIDE the same output-token budget as the visible answer.
        # We observed o4-mini on the Q4-v2 prompt: max=2000 → thinking
        # ate all 2000 → visible answer length = 0. Same failure mode
        # we documented for Gemini 3.5 Flash. Fix: floor the output
        # budget at 8000 for any known reasoning model so reasoning has
        # room to breathe AND there's still budget for a real answer.
        # Callers can pass a larger value; we only bump UP.
        effective_max_output = max_output_tokens
        if is_reasoning:
            REASONING_MIN_OUTPUT = 8000
            effective_max_output = max(max_output_tokens, REASONING_MIN_OUTPUT)
            call_kwargs["max_completion_tokens"] = effective_max_output
        else:
            call_kwargs["max_tokens"] = effective_max_output

        # ===== Temperature =====
        # Reasoning models (o3, o4-mini, GPT-5.x with reasoning) pin
        # temperature to 1 and HARD-reject any other value with a 400:
        #   "Unsupported value: 'temperature' does not support 0.2"
        # We previously relied on the except-clause retry below to strip
        # it on the second attempt, but that's wasteful (extra round
        # trip + risk of double-billing on partial completions) and we
        # observed it fail to recover on some o4-mini calls. Better to
        # never send the field for known-reasoning models.
        if not is_reasoning:
            call_kwargs["temperature"] = temperature

        # ===== Reasoning effort (disable by default on reasoning models) =====
        # See class docstring: we don't want reasoning tokens for
        # summarization. Set effort to minimal; fall back if SDK doesn't
        # know the kwarg.
        used_reasoning_param = False
        if is_reasoning:
            call_kwargs["reasoning_effort"] = "minimal"
            used_reasoning_param = True

        async def _do_call(kwargs):
            return await _openai_client.chat.completions.create(**kwargs)

        try:
            completion = await _do_call(call_kwargs)
        except TypeError:
            # Older openai SDK doesn't know newer kwargs like
            # `reasoning_effort` / `max_completion_tokens`. Strip them
            # and retry with the legacy shape.
            if used_reasoning_param:
                call_kwargs.pop("reasoning_effort", None)
            if "max_completion_tokens" in call_kwargs:
                call_kwargs["max_tokens"] = call_kwargs.pop("max_completion_tokens")
            completion = await _do_call(call_kwargs)
        except Exception as exc:
            # API-level rejection on a specific param. Sniff the message
            # and retry without the offending kwarg. We chain retries
            # (reasoning_effort → max_completion_tokens swap → temperature
            # drop) because some 5.x variants reject more than one.
            msg = str(exc).lower()
            retried = False

            if used_reasoning_param and ("reasoning_effort" in msg or "reasoning" in msg):
                call_kwargs.pop("reasoning_effort", None)
                used_reasoning_param = False
                retried = True
            if "max_completion_tokens" in msg and "max_completion_tokens" in call_kwargs:
                # Provider says it expects max_tokens after all.
                call_kwargs["max_tokens"] = call_kwargs.pop("max_completion_tokens")
                retried = True
            elif "max_tokens" in msg and "max_tokens" in call_kwargs:
                # Provider says it expects max_completion_tokens.
                call_kwargs["max_completion_tokens"] = call_kwargs.pop("max_tokens")
                retried = True
            if "temperature" in msg and "temperature" in call_kwargs:
                call_kwargs.pop("temperature", None)
                retried = True

            if not retried:
                raise
            completion = await _do_call(call_kwargs)

        # Text
        text = ""
        finish_reason: Optional[str] = None
        choices = getattr(completion, "choices", None) or []
        if choices:
            first = choices[0]
            msg = getattr(first, "message", None)
            content = getattr(msg, "content", None) if msg is not None else None
            text = (content or "").strip()
            finish_reason = getattr(first, "finish_reason", None)

        # Usage
        usage_obj = getattr(completion, "usage", None)
        if usage_obj is not None:
            input_tokens = _coerce_int(getattr(usage_obj, "prompt_tokens", 0))
            output_tokens = _coerce_int(getattr(usage_obj, "completion_tokens", 0))
            total_tokens = _coerce_int(
                getattr(usage_obj, "total_tokens", input_tokens + output_tokens)
            )

            # Reasoning tokens (GPT-5 / o-series). OpenAI bundles them inside
            # `completion_tokens` (the total visible+hidden output count) and
            # exposes the hidden subset under
            # `completion_tokens_details.reasoning_tokens`. They are billed at
            # the output token rate, so cost calc has to include them — we
            # carry them through LlmUsage.thinking_tokens. For non-reasoning
            # models or when the SDK doesn't expose the field, this stays 0.
            reasoning_tokens = 0
            details = getattr(usage_obj, "completion_tokens_details", None)
            if details is not None:
                reasoning_tokens = _coerce_int(
                    getattr(details, "reasoning_tokens", 0)
                )
            if reasoning_tokens > 0:
                log.info(
                    "openai.reasoning_tokens  model=%s reasoning_tokens=%d "
                    "visible_output_tokens=%d  total_output_tokens=%d "
                    "input_tokens=%d",
                    provider_model,
                    reasoning_tokens,
                    max(output_tokens - reasoning_tokens, 0),
                    output_tokens,
                    input_tokens,
                )

            usage = LlmUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                tokens_source=TOKENS_SOURCE_API,
                thinking_tokens=reasoning_tokens,
            )
        else:
            usage = estimate_chars_usage(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                output_text=text,
            )

        return text, usage, finish_reason


# ---------------------------------------------------------------------------
# Anthropic adapter
# ---------------------------------------------------------------------------

_ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
_anthropic_client = (
    AsyncAnthropic(api_key=_ANTHROPIC_API_KEY) if _ANTHROPIC_API_KEY else None
)


def _extract_anthropic_text(response: Any) -> str:
    parts: list[str] = []
    for block in getattr(response, "content", []) or []:
        text = getattr(block, "text", None)
        if isinstance(text, str) and text.strip():
            parts.append(text)
    return "\n".join(parts).strip()


class AnthropicAdapter:
    """
    Anthropic Messages API adapter.

    Response usage shape (messages.create):
        response.usage.input_tokens
        response.usage.output_tokens
        (total is NOT returned — we compute it ourselves)

    NOTE: Anthropic also exposes `cache_creation_input_tokens` /
    `cache_read_input_tokens`. We currently fold those silently into
    `input_tokens` if present — billing-accurate cache accounting is a
    v2 concern.
    """

    provider_name = "anthropic"

    async def complete(
        self,
        *,
        provider_model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_output_tokens: int,
    ) -> tuple[str, LlmUsage, Optional[str]]:
        if _anthropic_client is None:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")

        response = await _anthropic_client.messages.create(
            model=provider_model,
            system=system_prompt,
            max_tokens=max_output_tokens,
            temperature=temperature,
            messages=[
                {"role": "user", "content": user_prompt},
            ],
        )

        text = _extract_anthropic_text(response)
        finish_reason = getattr(response, "stop_reason", None)

        usage_obj = getattr(response, "usage", None)
        if usage_obj is not None:
            in_t = _coerce_int(getattr(usage_obj, "input_tokens", 0))
            out_t = _coerce_int(getattr(usage_obj, "output_tokens", 0))
            # Fold cache tokens into input if the provider exposes them.
            cache_create = _coerce_int(
                getattr(usage_obj, "cache_creation_input_tokens", 0)
            )
            cache_read = _coerce_int(
                getattr(usage_obj, "cache_read_input_tokens", 0)
            )
            in_t_total = in_t + cache_create + cache_read

            usage = LlmUsage(
                input_tokens=in_t_total,
                output_tokens=out_t,
                total_tokens=in_t_total + out_t,
                tokens_source=TOKENS_SOURCE_API,
            )
        else:
            usage = estimate_chars_usage(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                output_text=text,
            )

        return text, usage, finish_reason


# ---------------------------------------------------------------------------
# Google Gemini adapter
# ---------------------------------------------------------------------------

_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
_gemini_client = (
    _google_genai.Client(api_key=_GEMINI_API_KEY)
    if (_GENAI_AVAILABLE and _GEMINI_API_KEY)
    else None
)


def _extract_gemini_text(response: Any) -> str:
    """
    Pull text out of a Gemini response without trusting `response.text`,
    which is a convenience property that can RAISE when the response was
    blocked by a safety filter or contained no candidates. We walk the
    canonical path (candidates[*].content.parts[*].text) defensively.
    """
    parts_out: list[str] = []
    for cand in getattr(response, "candidates", None) or []:
        content = getattr(cand, "content", None)
        if content is None:
            continue
        for part in getattr(content, "parts", None) or []:
            t = getattr(part, "text", None)
            if isinstance(t, str) and t.strip():
                parts_out.append(t)
    return "\n".join(parts_out).strip()


def _is_gemini_reasoning_model(provider_model: str) -> bool:
    """
    Detect Gemini models where we WANT reasoning ("thinking") enabled.

    Light and balanced Gemini variants run with thinking_budget=0 to keep
    behaviour parity with OpenAI/Claude (visible-only output) and to avoid
    silent truncation when reasoning tokens eat the output budget.

    Reasoning-tier Gemini variants are routed through the alternate
    branch (`_gemini_thinking_budget`) so we control HOW MUCH they think.
    On Q4 we saw `gemini-3.5-flash` waste 7681 thinking tokens for 315
    visible — the auto budget is unbounded. For new candidates we use
    a small fixed budget instead of `-1`.

    Keep this list explicit and conservative. Add a model only after you
    verify (a) it benefits from thinking on our workloads, (b) the budget
    impact is acceptable.
    """
    name = (provider_model or "").lower()
    if name == "gemini-3.5-flash":
        return True
    if name == "gemini-2.5-pro":
        return True
    return False


def _gemini_thinking_budget(provider_model: str) -> int:
    """
    Per-model thinking budget (tokens of hidden chain-of-thought we
    allow). Used by GoogleGeminiAdapter when reasoning is enabled.

    Per user policy: keep reasoning at MINIMUM or disabled. Notes:
      - `gemini-2.5-pro` supports values in {0, or >= 128}. We use 128
        (smallest legal "on" value) — model gets a tiny reasoning budget
        for filter/rank tasks where some reasoning materially helps, but
        we don't bleed cost on hidden tokens.
      - `gemini-3.5-flash` is on its way out (see test-analysis-Q4.md).
        Until it's removed from the catalog, we cap its budget at 512
        instead of leaving it at -1 (unbounded). This is the fix for
        Q4's 7681-token thinking-waste incident.

    Returns -1 ONLY if we genuinely want the SDK default; that's no
    longer recommended for any model in our catalog.
    """
    name = (provider_model or "").lower()
    if name == "gemini-2.5-pro":
        return 128
    if name == "gemini-3.5-flash":
        return 512
    # Fallback for any future reasoning model we add later: small budget.
    return 256


class GoogleGeminiAdapter:
    """
    Google Gemini adapter using the google-genai SDK.

    Response usage shape (generate_content):
        response.usage_metadata.prompt_token_count
        response.usage_metadata.candidates_token_count
        response.usage_metadata.total_token_count

    Differences from OpenAI/Anthropic worth knowing when debugging:
      - `system_instruction` lives in `GenerateContentConfig`, NOT in
        the messages/contents list. There is no "system role".
      - `contents` can be a plain string for single-turn calls (which
        is what we do here).
      - Finish reason is on `candidates[0].finish_reason` and is an
        enum, not a plain string — we stringify it for diagnostics.
    """

    provider_name = "google"

    async def complete(
        self,
        *,
        provider_model: str,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        max_output_tokens: int,
    ) -> tuple[str, LlmUsage, Optional[str]]:
        if _gemini_client is None:
            if not _GENAI_AVAILABLE:
                raise RuntimeError(
                    "google-genai package is not installed; "
                    "run `pip install google-genai` and add it to requirements.txt"
                )
            raise RuntimeError(
                "Gemini API key is not set (expected env var "
                "GEMINI_API_KEY or GOOGLE_API_KEY)"
            )

        # IMPORTANT: Gemini 2.5+ models (`gemini-2.5-flash`, `gemini-3.5-flash`,
        # etc.) have *reasoning / thinking* enabled by default. Thinking tokens
        # count against `max_output_tokens`, so if we leave it on the model will
        # eat most of the budget on hidden chain-of-thought and the visible
        # answer gets truncated mid-sentence.
        #
        # Default policy: disable thinking (`thinking_budget=0`) so behaviour
        # is on par with OpenAI/Claude (where `max_tokens` means visible
        # output only) and we don't pay for hidden tokens.
        #
        # Exception: for models explicitly marked as reasoning-tier in
        # `_is_gemini_reasoning_model` we leave thinking on with a dynamic
        # budget (`thinking_budget=-1` → model picks how much to think).
        # Reasoning is the main feature of that tier — disabling it would
        # mean paying premium price for a crippled model. Be aware: thinking
        # tokens still count against `max_output_tokens`, so the visible
        # answer can come up short if the budget is tight. Bump tier output
        # limits accordingly if you observe truncation.
        thinking_config = None
        if _google_genai_types is not None and hasattr(
            _google_genai_types, "ThinkingConfig"
        ):
            if _is_gemini_reasoning_model(provider_model):
                # Use a CAPPED per-model budget instead of -1 (unbounded).
                # See `_gemini_thinking_budget` for rationale per model;
                # this is the fix for the Q4 incident where the SDK
                # default (-1) let gemini-3.5-flash burn 7681 hidden
                # tokens for 315 visible.
                budget = _gemini_thinking_budget(provider_model)
                try:
                    thinking_config = _google_genai_types.ThinkingConfig(
                        thinking_budget=budget
                    )
                except Exception:
                    # Older SDK doesn't accept this exact value — fall
                    # back to API default (still thinking-enabled). Log
                    # so we notice if it ever happens.
                    log.warning(
                        "gemini.thinking_config_setup_failed model=%s budget=%d",
                        provider_model,
                        budget,
                    )
                    thinking_config = None
            else:
                try:
                    thinking_config = _google_genai_types.ThinkingConfig(
                        thinking_budget=0
                    )
                except Exception:
                    # Older SDK versions may not accept thinking_budget=0
                    # — fall back to leaving thinking_config unset rather
                    # than crashing.
                    thinking_config = None

        # For reasoning-tier Gemini models we auto-raise the output budget
        # to the deep-tier floor (8000). Reason: thinking tokens compete
        # with the visible answer for max_output_tokens; on the default
        # light budget (2000) the model can spend it all on hidden chain-
        # of-thought and the visible response gets cut off mid-sentence.
        # 8000 gives reasoning room to breathe while still leaving plenty
        # for the visible answer. Callers can override by passing a higher
        # explicit budget — we only bump UP, never down.
        effective_max_output = max_output_tokens
        if _is_gemini_reasoning_model(provider_model):
            REASONING_MIN_OUTPUT = 8000
            effective_max_output = max(max_output_tokens, REASONING_MIN_OUTPUT)

        config_kwargs: dict[str, Any] = {
            "system_instruction": system_prompt,
            "temperature": temperature,
            "max_output_tokens": effective_max_output,
        }
        if thinking_config is not None:
            config_kwargs["thinking_config"] = thinking_config

        config = _google_genai_types.GenerateContentConfig(**config_kwargs)

        response = await _gemini_client.aio.models.generate_content(
            model=provider_model,
            contents=user_prompt,
            config=config,
        )

        text = _extract_gemini_text(response)

        finish_reason: Optional[str] = None
        candidates = getattr(response, "candidates", None) or []
        if candidates:
            fr = getattr(candidates[0], "finish_reason", None)
            if fr is not None:
                # finish_reason is an enum — `.name` is more useful than str(enum)
                finish_reason = getattr(fr, "name", None) or str(fr)

        usage_meta = getattr(response, "usage_metadata", None)
        if usage_meta is not None:
            in_t = _coerce_int(getattr(usage_meta, "prompt_token_count", 0))
            out_t = _coerce_int(getattr(usage_meta, "candidates_token_count", 0))
            total_t = _coerce_int(
                getattr(usage_meta, "total_token_count", in_t + out_t)
            )
            # Reasoning ("thoughts") tokens are reported separately. They
            # are NOT in candidates_token_count but ARE included in
            # total_token_count, so total = input + visible_output + thinking.
            # Defensive fallback: if SDK doesn't expose the field, derive
            # by subtraction. Billed at the output token rate.
            thinking_t = _coerce_int(
                getattr(usage_meta, "thoughts_token_count", 0)
            )
            if thinking_t == 0 and total_t > in_t + out_t:
                thinking_t = max(total_t - in_t - out_t, 0)
            if thinking_t > 0:
                log.info(
                    "gemini.thinking_tokens model=%s thinking_tokens=%d "
                    "visible_output_tokens=%d input_tokens=%d total=%d",
                    provider_model,
                    thinking_t,
                    out_t,
                    in_t,
                    total_t,
                )
            usage = LlmUsage(
                input_tokens=in_t,
                output_tokens=out_t,
                total_tokens=total_t,
                tokens_source=TOKENS_SOURCE_API,
                thinking_tokens=thinking_t,
            )
        else:
            usage = estimate_chars_usage(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                output_text=text,
            )

        return text, usage, finish_reason


# ---------------------------------------------------------------------------
# Error classification + retry-with-backoff (для роутера с fallback-chain)
# ---------------------------------------------------------------------------
#
# См. architecture-router-and-credits.md, разделы 1.2 (fallback chain) и
# Q6 incident в test-analysis-Q6.md (503 UNAVAILABLE от Gemini 2.5 Pro
# 6 раз подряд за час — это серверная перегрузка Google, не наша квота).
#
# Логика:
#   1) Каждый вызов адаптера может выкинуть provider-specific exception
#   2) `_classify_error()` смотрит status_code / текст ошибки и решает:
#         - retryable  (429/500/502/503/504, timeout, "overloaded")
#                     → попробовать снова через backoff
#         - fatal      (400/401/403/404, неверный API-ключ, неверный
#                       параметр) → не повторять, поднять LlmFatalError
#         - unknown    → не повторять (на всякий случай), поднять оригинал
#   3) `complete_with_retry()` пробует адаптер max_retries+1 раз с
#         экспоненциальным backoff (1с → 3с). Если все попытки упали на
#         retryable error → поднимает LlmRetryableError. Вызывающий код
#         (service.py / routing.py) ловит её и переключается на следующую
#         модель в fallback-цепочке.
# ---------------------------------------------------------------------------


class LlmRetryableError(Exception):
    """
    Все попытки исчерпаны, провайдер до сих пор возвращает retryable error
    (429/503/504/timeout). Вызывающий код должен попробовать следующую
    модель в fallback-цепочке (см. routing.get_fallback_chain).
    """

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        provider_model: str,
        status_code: Optional[int] = None,
    ):
        super().__init__(message)
        self.provider = provider
        self.provider_model = provider_model
        self.status_code = status_code


class LlmFatalError(Exception):
    """
    Провайдер вернул non-retryable ошибку (400 bad request, 401 unauthorized,
    403 forbidden, 404 not found). Повторять бессмысленно — лучше сразу
    провалить запрос пользователя или попробовать совсем другую модель.

    Отличается от LlmRetryableError тем, что fallback-chain тут НЕ помогает —
    проблема, скорее всего, в нашей конфигурации (нет ключа, неверный
    параметр), а не в недоступности конкретной модели.
    """

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        provider_model: str,
        status_code: Optional[int] = None,
    ):
        super().__init__(message)
        self.provider = provider
        self.provider_model = provider_model
        self.status_code = status_code


_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_FATAL_STATUS_CODES = frozenset({400, 401, 403, 404})

# Подстроки в тексте ошибки, по которым определяем retryable когда у
# исключения нет численного status_code (например, провайдерные обёртки
# с разной структурой).
_RETRYABLE_MESSAGE_HINTS = (
    "rate limit",
    "rate_limit",
    "resource_exhausted",
    "resource exhausted",
    "unavailable",
    "overloaded",
    "service is currently overloaded",
    "timeout",
    "timed out",
    "deadline exceeded",
    "internal server error",
    "bad gateway",
    "gateway timeout",
)

# Подстроки в тексте, указывающие на fatal-ошибку конфигурации/контента.
_FATAL_MESSAGE_HINTS = (
    "invalid_api_key",
    "permission_denied",
    "permission denied",
    "not_found",
    "invalid_request_error",
    "model_not_found",
    "unsupported_parameter",
)


def _extract_status_code(exc: Exception) -> Optional[int]:
    """
    Достать численный HTTP-статус из исключения, если есть.

    Поддерживает openai SDK (status_code), anthropic SDK (status_code),
    google.api_core / google.genai (code/grpc_status). Возвращает None
    если не удалось определить.
    """
    for attr in ("status_code", "code", "http_status"):
        v = getattr(exc, attr, None)
        if isinstance(v, int) and 100 <= v < 600:
            return v
        # Иногда code приходит как enum/строка с цифрами.
        if v is not None:
            try:
                iv = int(getattr(v, "value", v))
                if 100 <= iv < 600:
                    return iv
            except (TypeError, ValueError):
                pass

    # Google genai иногда кладёт {'error': {'code': 503}} в args[0]
    args = getattr(exc, "args", ())
    for a in args:
        if isinstance(a, dict):
            for key in ("code", "status", "status_code"):
                v = a.get(key) or a.get("error", {}).get(key) if isinstance(a.get("error"), dict) else None
                if isinstance(v, int) and 100 <= v < 600:
                    return v
    return None


def _classify_error(exc: Exception) -> str:
    """
    Вернуть 'retryable' / 'fatal' / 'unknown'.

    Сначала пробуем числовой status_code (надёжнее), потом подстроки.
    """
    code = _extract_status_code(exc)
    if code in _RETRYABLE_STATUS_CODES:
        return "retryable"
    if code in _FATAL_STATUS_CODES:
        return "fatal"

    msg = str(exc).lower()
    for hint in _RETRYABLE_MESSAGE_HINTS:
        if hint in msg:
            return "retryable"
    for hint in _FATAL_MESSAGE_HINTS:
        if hint in msg:
            return "fatal"
    return "unknown"


# Параметры retry — мягкие, чтобы не растягивать ожидание пользователя.
# Итого худший случай: 1с + 3с = 4с задержки перед тем как переключиться
# на следующую модель fallback-цепочки.
_RETRY_MAX_ATTEMPTS = 2  # это additional попытки сверх первой, итого 3 вызова
_RETRY_BASE_DELAY_SEC = 1.0
_RETRY_BACKOFF_MULTIPLIER = 3.0


async def complete_with_retry(
    *,
    provider: str,
    provider_model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_output_tokens: int,
) -> tuple[str, LlmUsage, Optional[str]]:
    """
    Вызвать adapter.complete() с retry-on-5xx логикой.

    Поведение:
      - 1 первичная попытка + до _RETRY_MAX_ATTEMPTS повторов на retryable
        ошибки (429/500/502/503/504, timeout, "overloaded")
      - backoff: 1с, потом 3с
      - на fatal ошибку (400/401/403/404, неверный ключ) → LlmFatalError
        сразу, без retry
      - на неизвестную ошибку → пробрасываем оригинал (паника логируется
        выше по стеку)
      - если все retries исчерпаны на retryable → LlmRetryableError, чтобы
        вызывающий код переключился на следующую модель в цепочке

    Сама retry-логика тут отдельная от внутренней обработки kwargs внутри
    OpenAiAdapter — у того свои retries на 400 (несовместимые параметры).
    Никакого двойного retry не получается: param-fix кейс не классифицируется
    как retryable.
    """
    adapter = get_adapter(provider)
    last_exc: Optional[Exception] = None

    for attempt in range(_RETRY_MAX_ATTEMPTS + 1):
        try:
            return await adapter.complete(
                provider_model=provider_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
        except (LlmRetryableError, LlmFatalError):
            # Уже классифицировано ниже — пробрасываем без повторной
            # классификации (защита от двойной обёртки если адаптер сам
            # пользуется этими типами в будущем).
            raise
        except Exception as exc:  # noqa: BLE001
            kind = _classify_error(exc)
            last_exc = exc
            status = _extract_status_code(exc)

            if kind == "fatal":
                log.warning(
                    "llm.fatal provider=%s model=%s status=%s err=%s",
                    provider, provider_model, status, exc,
                )
                raise LlmFatalError(
                    str(exc),
                    provider=provider,
                    provider_model=provider_model,
                    status_code=status,
                ) from exc

            if kind == "retryable" and attempt < _RETRY_MAX_ATTEMPTS:
                delay = _RETRY_BASE_DELAY_SEC * (_RETRY_BACKOFF_MULTIPLIER ** attempt)
                log.warning(
                    "llm.retry attempt=%d/%d provider=%s model=%s status=%s "
                    "delay=%.1fs err=%s",
                    attempt + 1, _RETRY_MAX_ATTEMPTS + 1,
                    provider, provider_model, status, delay, exc,
                )
                await asyncio.sleep(delay)
                continue

            if kind == "retryable":
                # Все попытки исчерпаны — сигнал «переключайтесь на fallback».
                log.warning(
                    "llm.retryable_exhausted provider=%s model=%s status=%s err=%s",
                    provider, provider_model, status, exc,
                )
                raise LlmRetryableError(
                    str(exc),
                    provider=provider,
                    provider_model=provider_model,
                    status_code=status,
                ) from exc

            # 'unknown' — пробрасываем оригинал, чтобы не маскировать новый
            # тип ошибки от провайдера. Логируем для последующего расширения
            # _RETRYABLE_/_FATAL_ списков.
            log.error(
                "llm.unknown_error provider=%s model=%s status=%s err=%s",
                provider, provider_model, status, exc,
            )
            raise

    # Сюда не должны попасть (выход из цикла только через return/raise),
    # но на всякий случай.
    raise LlmRetryableError(
        str(last_exc) if last_exc else "retry loop ended without resolution",
        provider=provider,
        provider_model=provider_model,
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
#
# To add a new provider:
#   1) implement an adapter class that satisfies LlmProviderAdapter
#   2) instantiate it and register it under its provider name below
#   3) add the model slug to llm/models.py SUPPORTED_MODELS with the
#      correct `provider` value
#
# `get_adapter` is the only resolution point — never instantiate
# adapters anywhere else.
# ---------------------------------------------------------------------------

_ADAPTERS: dict[str, LlmProviderAdapter] = {
    OpenAiAdapter.provider_name: OpenAiAdapter(),
    AnthropicAdapter.provider_name: AnthropicAdapter(),
    GoogleGeminiAdapter.provider_name: GoogleGeminiAdapter(),
}


def get_adapter(provider: str) -> LlmProviderAdapter:
    adapter = _ADAPTERS.get(provider)
    if adapter is None:
        raise RuntimeError(f"Unsupported LLM provider: {provider}")
    return adapter
