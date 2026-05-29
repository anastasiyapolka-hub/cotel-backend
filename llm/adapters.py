from __future__ import annotations

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
        if is_reasoning:
            call_kwargs["max_completion_tokens"] = max_output_tokens
        else:
            call_kwargs["max_tokens"] = max_output_tokens

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
                    "openai.reasoning_tokens model=%s reasoning_tokens=%d "
                    "visible_output_tokens=%d total_output_tokens=%d "
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
