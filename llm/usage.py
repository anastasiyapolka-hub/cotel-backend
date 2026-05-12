from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Normalized LLM result shapes
# ---------------------------------------------------------------------------
#
# Goal: regardless of which provider produced the answer (OpenAI,
# Anthropic, in the future Gemini / Mistral / DeepSeek / ...), the rest
# of the codebase works with a single normalized shape.
#
# Per-provider extraction lives in `llm/adapters.py`. Anything downstream
# of the adapter MUST consume these dataclasses, not provider-specific
# response objects.
# ---------------------------------------------------------------------------


# Allowed values for LlmUsage.tokens_source.
# Kept as a free str (not Literal) so adapters for future providers can
# add their own diagnostic codes without breaking the type signature.
TOKENS_SOURCE_API = "api_usage"          # provider returned a real usage object
TOKENS_SOURCE_ESTIMATED = "estimated_chars"  # we estimated by character count
TOKENS_SOURCE_EMPTY = "empty"            # no API call was made (empty context, short-circuit)


@dataclass(frozen=True)
class LlmUsage:
    """
    Token usage for a single LLM call, normalized across providers.

    `tokens_source` documents how the numbers were obtained:
      - "api_usage": values came from the provider's response.usage object
      - "estimated_chars": values were estimated from character counts
        because the provider did not return usage (or we short-circuited)
      - "empty": no API call was made (e.g. empty context); all counts 0
    """
    input_tokens: int
    output_tokens: int
    total_tokens: int
    tokens_source: str

    @classmethod
    def empty(cls) -> "LlmUsage":
        return cls(
            input_tokens=0,
            output_tokens=0,
            total_tokens=0,
            tokens_source=TOKENS_SOURCE_EMPTY,
        )


@dataclass(frozen=True)
class LlmTextResult:
    """
    Result of a plain-text LLM call (e.g. Q&A summarization).
    """
    text: str
    usage: LlmUsage
    ai_model: str          # user-facing slug, e.g. "openai:gpt-4.1-mini"
    provider: str          # "openai" | "anthropic" | ...
    provider_model: str    # actual model used at the API, e.g. "gpt-4.1-mini"
    raw_finish_reason: Optional[str] = None


@dataclass(frozen=True)
class LlmJsonResult:
    """
    Result of a JSON-producing LLM call (e.g. classify_subscription_matches,
    build_subscription_digest).

    `data` is the parsed JSON; `raw_text` is the original string the model
    produced, kept for diagnostics. Downstream code should consume `data`.
    """
    data: dict
    raw_text: str
    usage: LlmUsage
    ai_model: str
    provider: str
    provider_model: str
    raw_finish_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Character-based usage estimator
# ---------------------------------------------------------------------------
#
# Used as a fallback when the provider does not return a usage object
# (some streaming endpoints, partial errors, providers that haven't
# implemented usage reporting, etc.).
#
# Heuristic: average ~3 characters per token, which is a reasonable
# compromise between English (~4 chars/token) and Russian (~2 chars/token).
# This is intentionally rough — its only job is to be "better than None"
# for cost-tracking. When real usage is available we always prefer it.
# ---------------------------------------------------------------------------

_CHARS_PER_TOKEN_DEFAULT = 3.0


def _approx_tokens_from_chars(char_count: int) -> int:
    if char_count <= 0:
        return 0
    return max(1, int(round(char_count / _CHARS_PER_TOKEN_DEFAULT)))


def estimate_chars_usage(
    *,
    system_prompt: str,
    user_prompt: str,
    output_text: str,
) -> LlmUsage:
    """
    Build an LlmUsage object by estimating tokens from character counts.
    Used as a fallback when the provider response did not include a usage
    object.
    """
    input_chars = len(system_prompt or "") + len(user_prompt or "")
    output_chars = len(output_text or "")

    in_tokens = _approx_tokens_from_chars(input_chars)
    out_tokens = _approx_tokens_from_chars(output_chars)

    return LlmUsage(
        input_tokens=in_tokens,
        output_tokens=out_tokens,
        total_tokens=in_tokens + out_tokens,
        tokens_source=TOKENS_SOURCE_ESTIMATED,
    )


def split_usage_for_meta(usage: LlmUsage) -> dict:
    """
    Map an LlmUsage into the right `record_qa_success` / `record_qa_failure`
    kwargs.

    Logic:
      - tokens_source == "api_usage":       fill input_tokens / output_tokens / total_tokens
      - tokens_source == "estimated_chars": fill estimated_input_tokens / estimated_output_tokens / estimated_total_tokens
      - tokens_source == "empty":           fill nothing except tokens_source

    Callers spread the result with `**split_usage_for_meta(usage)` so the
    route code stays compact and free of branching.
    """
    if usage.tokens_source == TOKENS_SOURCE_API:
        return {
            "input_tokens": int(usage.input_tokens),
            "output_tokens": int(usage.output_tokens),
            "total_tokens": int(usage.total_tokens),
            "tokens_source": usage.tokens_source,
        }
    if usage.tokens_source == TOKENS_SOURCE_ESTIMATED:
        return {
            "estimated_input_tokens": int(usage.input_tokens),
            "estimated_output_tokens": int(usage.output_tokens),
            "estimated_total_tokens": int(usage.total_tokens),
            "tokens_source": usage.tokens_source,
        }
    return {"tokens_source": usage.tokens_source}
