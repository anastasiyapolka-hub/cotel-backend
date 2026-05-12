from __future__ import annotations

import os
from typing import Any, Optional, Protocol

from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

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
        completion = await _openai_client.chat.completions.create(
            model=provider_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            max_tokens=max_output_tokens,
        )

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
}


def get_adapter(provider: str) -> LlmProviderAdapter:
    adapter = _ADAPTERS.get(provider)
    if adapter is None:
        raise RuntimeError(f"Unsupported LLM provider: {provider}")
    return adapter
