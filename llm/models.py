from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# User-facing model slugs (shown in the UI selector)
# ---------------------------------------------------------------------------
# OpenAI tiers
OPENAI_MODEL_SLUG = "openai:gpt-4.1-mini"           # Light  — fast + cheap
OPENAI_BALANCED_MODEL_SLUG = "openai:gpt-5.4-mini"  # Balanced — reasoning model
OPENAI_PRO_MODEL_SLUG = "openai:gpt-4.1"            # Deep — 1M context, premium (TPM-prone)
OPENAI_O3_SLUG = "openai:o3"                        # Deep — reasoning, 200K ctx, cheaper than Sonnet
OPENAI_O4_MINI_SLUG = "openai:o4-mini"              # Deep budget — cheapest reasoning, 200K ctx

# Anthropic tiers
ANTHROPIC_HAIKU_SLUG = "anthropic:claude-haiku-4-5"  # Light — fast + cheap
ANTHROPIC_MODEL_SLUG = "anthropic:claude-sonnet-4-6"  # Balanced/Deep

# Google Gemini tiers
GEMINI_LITE_MODEL_SLUG = "google:gemini-3.1-flash-lite"  # Light — cheapest
GEMINI_MODEL_SLUG = "google:gemini-2.5-flash"            # Balanced
GEMINI_PRO_MODEL_SLUG = "google:gemini-3.5-flash"        # Deep — DEPRECATED (thinking-waste)
GEMINI_PRO_25_SLUG = "google:gemini-2.5-pro"             # Deep — 1M ctx, controllable thinking, ~½ Sonnet price

DEFAULT_AI_MODEL = OPENAI_MODEL_SLUG


@dataclass(frozen=True)
class ModelConfig:
    slug: str
    provider: str
    provider_model: str
    label: str


SUPPORTED_MODELS: dict[str, ModelConfig] = {
    # ---- OpenAI ----
    OPENAI_MODEL_SLUG: ModelConfig(
        slug=OPENAI_MODEL_SLUG,
        provider="openai",
        provider_model="gpt-4.1-mini",
        label="OpenAI GPT-4.1 mini",
    ),
    OPENAI_BALANCED_MODEL_SLUG: ModelConfig(
        slug=OPENAI_BALANCED_MODEL_SLUG,
        provider="openai",
        provider_model="gpt-5.4-mini",
        label="OpenAI GPT-5.4 mini",
    ),
    OPENAI_PRO_MODEL_SLUG: ModelConfig(
        slug=OPENAI_PRO_MODEL_SLUG,
        provider="openai",
        provider_model="gpt-4.1",
        label="OpenAI GPT-4.1",
    ),
    OPENAI_O3_SLUG: ModelConfig(
        slug=OPENAI_O3_SLUG,
        provider="openai",
        provider_model="o3",
        label="OpenAI o3",
    ),
    OPENAI_O4_MINI_SLUG: ModelConfig(
        slug=OPENAI_O4_MINI_SLUG,
        provider="openai",
        provider_model="o4-mini",
        label="OpenAI o4-mini",
    ),
    # ---- Anthropic ----
    ANTHROPIC_HAIKU_SLUG: ModelConfig(
        slug=ANTHROPIC_HAIKU_SLUG,
        provider="anthropic",
        provider_model="claude-haiku-4-5",
        label="Claude Haiku 4.5",
    ),
    ANTHROPIC_MODEL_SLUG: ModelConfig(
        slug=ANTHROPIC_MODEL_SLUG,
        provider="anthropic",
        provider_model="claude-sonnet-4-6",
        label="Claude Sonnet 4.6",
    ),
    # ---- Google Gemini ----
    GEMINI_LITE_MODEL_SLUG: ModelConfig(
        slug=GEMINI_LITE_MODEL_SLUG,
        provider="google",
        provider_model="gemini-3.1-flash-lite",
        label="Google Gemini 3.1 Flash Lite",
    ),
    GEMINI_MODEL_SLUG: ModelConfig(
        slug=GEMINI_MODEL_SLUG,
        provider="google",
        provider_model="gemini-2.5-flash",
        label="Google Gemini 2.5 Flash",
    ),
    GEMINI_PRO_MODEL_SLUG: ModelConfig(
        slug=GEMINI_PRO_MODEL_SLUG,
        provider="google",
        provider_model="gemini-3.5-flash",
        label="Google Gemini 3.5 Flash",
    ),
    GEMINI_PRO_25_SLUG: ModelConfig(
        slug=GEMINI_PRO_25_SLUG,
        provider="google",
        provider_model="gemini-2.5-pro",
        label="Google Gemini 2.5 Pro",
    ),
}


# Task-based routing table for the Anthropic provider.
#
# Known task codes (must match the `task` argument passed by
# llm.service._chat_text_completion):
#   "qa"        — user Q&A, nuanced long-context analysis → Sonnet
#   "classify"  — event-subscription match filter, high-frequency → Haiku
#   "digest"    — subscription summary, short analytical task → Haiku
#
# Anything not in this table falls through to the user-selected base
# config. Note: Haiku is now also a user-facing model — task routing is
# only applied when the user selected Sonnet but the call is a
# lightweight background task. If the user selected Haiku directly, we
# respect that choice (see resolve_model_config below).
_ANTHROPIC_TASK_ROUTING: dict[str, ModelConfig] = {
    "qa": SUPPORTED_MODELS[ANTHROPIC_MODEL_SLUG],
    "classify": SUPPORTED_MODELS[ANTHROPIC_HAIKU_SLUG],
    "digest": SUPPORTED_MODELS[ANTHROPIC_HAIKU_SLUG],
}


def normalize_ai_model(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in SUPPORTED_MODELS:
        return raw
    return DEFAULT_AI_MODEL


def resolve_model_config(
    value: str | None,
    task: Optional[str] = None,
) -> ModelConfig:
    """
    Resolve a user-facing model slug + task hint into the concrete
    model config to use for the actual API call.

    - OpenAI / Google providers: `task` is ignored — we always return
      the user-selected config.
    - Anthropic provider: applies task-based routing only when the user
      picked SONNET. Background tasks (classify, digest) drop to Haiku
      to save cost. If the user EXPLICITLY picked Haiku (now a public
      option), we never upgrade them to Sonnet — respect the choice.
    """
    normalized = normalize_ai_model(value)
    base = SUPPORTED_MODELS[normalized]

    if base.provider == "anthropic" and task:
        # Never upgrade above the user's explicit choice. If they picked
        # Haiku, all tasks stay on Haiku.
        if base.slug == ANTHROPIC_HAIKU_SLUG:
            return base

        routed = _ANTHROPIC_TASK_ROUTING.get(task)
        if routed is not None:
            return routed

    return base
