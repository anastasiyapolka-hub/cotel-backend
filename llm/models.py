from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# User-facing model slugs (shown in the UI selector)
# ---------------------------------------------------------------------------
OPENAI_MODEL_SLUG = "openai:gpt-4.1-mini"
ANTHROPIC_MODEL_SLUG = "anthropic:claude-sonnet-4-6"

# Internal-only slug (not exposed to users). Used by task-based routing
# inside the Anthropic provider to serve high-frequency lightweight tasks
# on Haiku instead of Sonnet.
ANTHROPIC_HAIKU_SLUG = "anthropic:claude-haiku-4-5"

DEFAULT_AI_MODEL = OPENAI_MODEL_SLUG


@dataclass(frozen=True)
class ModelConfig:
    slug: str
    provider: str
    provider_model: str
    label: str


SUPPORTED_MODELS: dict[str, ModelConfig] = {
    OPENAI_MODEL_SLUG: ModelConfig(
        slug=OPENAI_MODEL_SLUG,
        provider="openai",
        provider_model="gpt-4.1-mini",
        label="OpenAI GPT-4.1 mini",
    ),
    ANTHROPIC_MODEL_SLUG: ModelConfig(
        slug=ANTHROPIC_MODEL_SLUG,
        provider="anthropic",
        provider_model="claude-sonnet-4-6",
        label="Claude Sonnet 4.6",
    ),
}

# Internal-only model configs used by task-based routing. Not exposed in
# `SUPPORTED_MODELS` so the UI selector is unchanged.
_ANTHROPIC_HAIKU = ModelConfig(
    slug=ANTHROPIC_HAIKU_SLUG,
    provider="anthropic",
    provider_model="claude-haiku-4-5",
    label="Claude Haiku 4.5",
)


# Task-based routing table for the Anthropic provider.
#
# Known task codes (must match the `task` argument passed by
# llm.service._chat_text_completion):
#   "qa"        — user Q&A, nuanced long-context analysis → Sonnet
#   "classify"  — event-subscription match filter, high-frequency → Haiku
#   "digest"    — subscription summary, short analytical task → Haiku
#
# Anything not in this table falls through to the user-selected base
# config (Sonnet).
_ANTHROPIC_TASK_ROUTING: dict[str, ModelConfig] = {
    "qa": SUPPORTED_MODELS[ANTHROPIC_MODEL_SLUG],
    "classify": _ANTHROPIC_HAIKU,
    "digest": _ANTHROPIC_HAIKU,
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

    - OpenAI provider has a single model, so `task` is effectively
      ignored — we always return the user-selected config.
    - Anthropic provider applies task-based routing so high-frequency
      lightweight tasks (classify, digest) use Haiku while nuanced
      tasks (qa) use Sonnet. This is transparent to the user: they
      still see "Claude Sonnet 4.6" in the UI.
    """
    normalized = normalize_ai_model(value)
    base = SUPPORTED_MODELS[normalized]

    if base.provider == "anthropic" and task:
        routed = _ANTHROPIC_TASK_ROUTING.get(task)
        if routed is not None:
            return routed

    return base
