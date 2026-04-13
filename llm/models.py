from __future__ import annotations

from dataclasses import dataclass


OPENAI_MODEL_SLUG = "openai:gpt-4.1-mini"
ANTHROPIC_MODEL_SLUG = "anthropic:claude-sonnet-4-6"
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


def normalize_ai_model(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in SUPPORTED_MODELS:
        return raw
    return DEFAULT_AI_MODEL


def resolve_model_config(value: str | None) -> ModelConfig:
    normalized = normalize_ai_model(value)
    return SUPPORTED_MODELS[normalized]