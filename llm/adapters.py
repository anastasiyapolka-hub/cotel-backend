from __future__ import annotations

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
        call_kwargs: dict[str, Any] = {
            "model": provider_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_output_tokens,
        }

        # Disable reasoning by default — only attempt on models that
        # support it. If the SDK rejects the param (older SDK or model
        # variant that doesn't accept it), retry without it rather than
        # failing the whole request.
        used_reasoning_param = False
        if _is_openai_reasoning_model(provider_model):
            call_kwargs["reasoning_effort"] = "minimal"
            used_reasoning_param = True

        try:
            completion = await _openai_client.chat.completions.create(**call_kwargs)
        except TypeError:
            # Older openai SDK doesn't know `reasoning_effort` as a kwarg.
            if used_reasoning_param:
                call_kwargs.pop("reasoning_effort", None)
                completion = await _openai_client.chat.completions.create(**call_kwargs)
            else:
                raise
        except Exception as exc:
            # OpenAI may also reject reasoning_effort at the API level on
            # models that don't support it. The error class differs
            # across SDK versions, so we sniff the message defensively.
            msg = str(exc).lower()
            if used_reasoning_param and (
                "reasoning_effort" in msg or "reasoning" in msg
            ):
                call_kwargs.pop("reasoning_effort", None)
                completion = await _openai_client.chat.completions.create(**call_kwargs)
            else:
                raise

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
            usage = LlmUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
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
        # answer gets truncated mid-sentence. We disable thinking explicitly
        # to keep behaviour parity with OpenAI/Claude (where `max_tokens` means
        # visible output) and to avoid paying for hidden tokens we don't need
        # for summarization. If we later want reasoning for a deep-analysis
        # tier, expose it via task-based routing — do not flip it globally.
        thinking_config = None
        if _google_genai_types is not None and hasattr(
            _google_genai_types, "ThinkingConfig"
        ):
            try:
                thinking_config = _google_genai_types.ThinkingConfig(
                    thinking_budget=0
                )
            except Exception:
                # Older SDK versions may not accept thinking_budget=0 — fall
                # back to leaving thinking_config unset rather than crashing.
                thinking_config = None

        config_kwargs: dict[str, Any] = {
            "system_instruction": system_prompt,
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
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
            usage = LlmUsage(
                input_tokens=in_t,
                output_tokens=out_t,
                total_tokens=total_t,
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
