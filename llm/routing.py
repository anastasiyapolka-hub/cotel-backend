"""
Маршрутизатор моделей: tier + категория → конкретная модель.

См. architecture-router-and-credits.md, раздел 1.2.

Решение, какую модель использовать, принимается на двух уровнях:
  1) Пользователь выбирает tier (light/balanced/deep) в UI.
  2) classifier.py определяет category по тексту запроса.
  3) route() здесь возвращает финальную ModelConfig.

Также определяет fallback-цепочку: при retryable ошибке (429/503/504/
timeout) — следующая модель в цепочке. Сам retry-with-backoff делает
adapters.py.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from .classifier import (
    CATEGORY_CROSS_CHAT_ANALYSIS,
    CATEGORY_DIGEST,
    CATEGORY_FILTER_RANK,
    CATEGORY_FIND_CONTRADICTIONS,
    CATEGORY_PLAN_CHECKLIST,
    CATEGORY_SENTIMENT_DYNAMICS,
    CATEGORY_SIMPLE_QA,
    CATEGORY_SOURCE_SYNTHESIS,
    DEFAULT_CATEGORY,
)
from .models import (
    ANTHROPIC_MODEL_SLUG,
    GEMINI_LITE_MODEL_SLUG,
    GEMINI_MODEL_SLUG,
    GEMINI_PRO_25_SLUG,
    ModelConfig,
    OPENAI_BALANCED_MODEL_SLUG,
    OPENAI_MODEL_SLUG,
    OPENAI_O4_MINI_SLUG,
    SUPPORTED_MODELS,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tier'ы (то, что пользователь выбирает в UI)
# ---------------------------------------------------------------------------

TIER_LIGHT = "light"
TIER_BALANCED = "balanced"
TIER_DEEP = "deep"

ALL_TIERS = (TIER_LIGHT, TIER_BALANCED, TIER_DEEP)


# Лимит видимого output по tier'у — конкретные числа см. в архитектуре
# и в adapters.py (там же есть REASONING_MIN_OUTPUT для thinking-моделей).
TIER_MAX_OUTPUT = {
    TIER_LIGHT:    2_000,
    TIER_BALANCED: 4_000,
    TIER_DEEP:     8_000,
}


def normalize_tier(value: Optional[str]) -> str:
    """Привести строку tier'а к одному из ALL_TIERS, fallback на light."""
    raw = str(value or "").strip().lower()
    if raw in ALL_TIERS:
        return raw
    return TIER_LIGHT


# ---------------------------------------------------------------------------
# Decision matrix: (category, tier) → model_slug
# ---------------------------------------------------------------------------
#
# Источник для каждой ячейки — конкретный Q-тест (см. test-analysis-Q*.md).
# Ключевой принцип v2: Gemini 2.5 Flash — default почти везде, в том числе
# на deep. Pro и Sonnet берутся ТОЛЬКО для двух категорий, где они
# объективно сильнее: find_contradictions (Q7) и filter_rank (Q4).
#
# ВАЖНО: добавляя новую категорию — обнови classifier.CLASSIFIER_SYSTEM_PROMPT
# и добавь строку сюда. Иначе route() свалится в _DEFAULT_BY_TIER fallback.
# ---------------------------------------------------------------------------

_DECISION_MATRIX: dict[tuple[str, str], str] = {
    # simple_qa — простой qa, ни одна категория не лучше Flash Lite по value
    (CATEGORY_SIMPLE_QA, TIER_LIGHT):    GEMINI_LITE_MODEL_SLUG,
    (CATEGORY_SIMPLE_QA, TIER_BALANCED): GEMINI_LITE_MODEL_SLUG,
    (CATEGORY_SIMPLE_QA, TIER_DEEP):     GEMINI_MODEL_SLUG,

    # digest — Q3, Q5: Flash 2.5 лидер; deep не даёт ощутимого прироста
    (CATEGORY_DIGEST, TIER_LIGHT):    GEMINI_LITE_MODEL_SLUG,
    (CATEGORY_DIGEST, TIER_BALANCED): GEMINI_MODEL_SLUG,
    (CATEGORY_DIGEST, TIER_DEEP):     GEMINI_MODEL_SLUG,  # + max_output=8K

    # plan_checklist — Q2, Q10: GPT 4.1 mini объективно лучший на «дай план»
    (CATEGORY_PLAN_CHECKLIST, TIER_LIGHT):    OPENAI_MODEL_SLUG,
    (CATEGORY_PLAN_CHECKLIST, TIER_BALANCED): OPENAI_MODEL_SLUG,
    (CATEGORY_PLAN_CHECKLIST, TIER_DEEP):     OPENAI_MODEL_SLUG,  # + max_output=8K

    # find_contradictions — Q7: Pro нашёл уникальную тему НАТО на deep tier
    # На balanced — GPT 5.4 mini единственная даёт явную секцию «Противоречие»
    (CATEGORY_FIND_CONTRADICTIONS, TIER_LIGHT):    OPENAI_MODEL_SLUG,
    (CATEGORY_FIND_CONTRADICTIONS, TIER_BALANCED): OPENAI_BALANCED_MODEL_SLUG,
    (CATEGORY_FIND_CONTRADICTIONS, TIER_DEEP):     GEMINI_PRO_25_SLUG,

    # filter_rank — Q4: единственный сценарий, где Sonnet объективно лучше.
    # На light не рекомендуем (GPT 4.1 mini смыливает фильтр), но если уж
    # пользователь хочет — даём GPT 4.1 mini с предупреждением в UI.
    (CATEGORY_FILTER_RANK, TIER_LIGHT):    OPENAI_MODEL_SLUG,
    (CATEGORY_FILTER_RANK, TIER_BALANCED): GEMINI_MODEL_SLUG,
    (CATEGORY_FILTER_RANK, TIER_DEEP):     ANTHROPIC_MODEL_SLUG,

    # sentiment_dynamics — Q6: Flash 2.5 нашла «обратный сдвиг», Pro не лучше
    (CATEGORY_SENTIMENT_DYNAMICS, TIER_LIGHT):    GEMINI_LITE_MODEL_SLUG,
    (CATEGORY_SENTIMENT_DYNAMICS, TIER_BALANCED): GEMINI_MODEL_SLUG,
    (CATEGORY_SENTIMENT_DYNAMICS, TIER_DEEP):     GEMINI_MODEL_SLUG,  # + max_output=8K

    # source_synthesis — Q8: критическая честность даже у Flash Lite ($0,002)
    (CATEGORY_SOURCE_SYNTHESIS, TIER_LIGHT):    GEMINI_LITE_MODEL_SLUG,
    (CATEGORY_SOURCE_SYNTHESIS, TIER_BALANCED): GEMINI_MODEL_SLUG,
    (CATEGORY_SOURCE_SYNTHESIS, TIER_DEEP):     GEMINI_MODEL_SLUG,  # + max_output=8K

    # cross_chat_analysis — Q9: Flash 2.5 покрыла темы шире deep-моделей
    (CATEGORY_CROSS_CHAT_ANALYSIS, TIER_LIGHT):    OPENAI_MODEL_SLUG,
    (CATEGORY_CROSS_CHAT_ANALYSIS, TIER_BALANCED): GEMINI_MODEL_SLUG,
    (CATEGORY_CROSS_CHAT_ANALYSIS, TIER_DEEP):     GEMINI_MODEL_SLUG,  # + max_output=8K
}


# Дефолты, если категория неизвестна (защита от рассинхрона
# classifier.py vs _DECISION_MATRIX).
_DEFAULT_BY_TIER: dict[str, str] = {
    TIER_LIGHT:    GEMINI_LITE_MODEL_SLUG,
    TIER_BALANCED: GEMINI_MODEL_SLUG,
    TIER_DEEP:     GEMINI_MODEL_SLUG,
}


# ---------------------------------------------------------------------------
# Special-case override: structured multi-block prompt
# ---------------------------------------------------------------------------
#
# Q10 показал: Gemini 2.5 Pro «творчески интерпретирует» жёсткие
# мультиблочные инструкции (типа «1) X, 2) Y, 3) Z, 4) что не покрыто»).
# Если classifier отметил needs_structured_format=true и матрица собиралась
# выбрать Pro → подменяем на Sonnet для deep, на GPT 4.1 mini для остальных.
# ---------------------------------------------------------------------------

def _apply_structured_format_override(slug: str, tier: str) -> str:
    """Защитная подмена Pro на не-Pro, когда нужна жёсткая структура."""
    if slug != GEMINI_PRO_25_SLUG:
        return slug
    if tier == TIER_DEEP:
        return ANTHROPIC_MODEL_SLUG
    return OPENAI_MODEL_SLUG


# ---------------------------------------------------------------------------
# Fallback-цепочки на retryable ошибках (429/503/504)
# ---------------------------------------------------------------------------
#
# Подробнее в adapters.py — там же retry-with-backoff. routing.py отвечает
# только за порядок моделей в цепочке.
# ---------------------------------------------------------------------------

_FALLBACK_CHAINS: dict[str, list[str]] = {
    TIER_DEEP: [
        GEMINI_PRO_25_SLUG,
        ANTHROPIC_MODEL_SLUG,
        OPENAI_O4_MINI_SLUG,
    ],
    TIER_BALANCED: [
        GEMINI_MODEL_SLUG,
        OPENAI_BALANCED_MODEL_SLUG,
        GEMINI_PRO_25_SLUG,
    ],
    TIER_LIGHT: [
        GEMINI_LITE_MODEL_SLUG,
        OPENAI_MODEL_SLUG,
    ],
}


def get_fallback_chain(primary_slug: str, tier: str) -> list[str]:
    """
    Вернуть упорядоченный список slug'ов: primary первая, затем альтернативы.

    Если primary не входит в стандартную цепочку tier'а — он всё равно
    идёт первым, а дальше добавляются альтернативы из tier-цепочки.
    """
    tier_chain = _FALLBACK_CHAINS.get(tier, [])
    result = [primary_slug]
    for slug in tier_chain:
        if slug not in result:
            result.append(slug)
    return result


# ---------------------------------------------------------------------------
# Главная функция: route()
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RoutingDecision:
    """
    Полное решение роутера для одного запроса.

    Передаётся вызывающему (main.py / service.py): какую модель пробовать
    первой, какие альтернативы (для fallback при retryable error), какой
    max_output_tokens передавать адаптеру.
    """
    primary_model: ModelConfig
    fallback_chain: list[ModelConfig]
    max_output_tokens: int
    tier: str
    category: str

    @property
    def primary_slug(self) -> str:
        return self.primary_model.slug


def route(
    *,
    tier: str,
    category: str,
    needs_structured_format: bool = False,
) -> RoutingDecision:
    """
    Главная точка входа в маршрутизатор.

    Параметры:
      tier — light / balanced / deep (из UI пользователя)
      category — одна из categories.* (из classifier.py)
      needs_structured_format — True если запрос явно перечисляет блоки
        ответа («1) X, 2) Y, 3) Z»). Защита от Q10-проблемы с Pro.

    Возврат: RoutingDecision со всем нужным для LLM-вызова.

    Никогда не raise — даже на неизвестные tier/category возвращает
    дефолтный безопасный выбор.
    """
    tier = normalize_tier(tier)
    category = (category or DEFAULT_CATEGORY).strip().lower()

    # 1. Найти модель в матрице (или fallback к дефолту tier'а).
    slug = _DECISION_MATRIX.get((category, tier))
    if slug is None:
        log.warning(
            "routing.unknown_category_fallback category=%s tier=%s",
            category, tier,
        )
        slug = _DEFAULT_BY_TIER[tier]

    # 2. Структурный override (Q10): Pro → Sonnet/4.1mini если нужна жёсткая структура.
    if needs_structured_format:
        new_slug = _apply_structured_format_override(slug, tier)
        if new_slug != slug:
            log.info(
                "routing.structured_override from=%s to=%s tier=%s category=%s",
                slug, new_slug, tier, category,
            )
            slug = new_slug

    # 3. Собрать fallback-цепочку.
    chain_slugs = get_fallback_chain(slug, tier)
    primary = SUPPORTED_MODELS[slug]
    fallback = [SUPPORTED_MODELS[s] for s in chain_slugs if s in SUPPORTED_MODELS]

    decision = RoutingDecision(
        primary_model=primary,
        fallback_chain=fallback,
        max_output_tokens=TIER_MAX_OUTPUT[tier],
        tier=tier,
        category=category,
    )

    log.info(
        "routing.decision tier=%s category=%s primary=%s fallback_chain=%s max_out=%d",
        tier, category, primary.slug,
        [m.slug for m in fallback], decision.max_output_tokens,
    )

    return decision


# ---------------------------------------------------------------------------
# Маршрутизация подписок (отдельная — у них нет «категории» в смысле выше)
# ---------------------------------------------------------------------------

SUBSCRIPTION_TYPE_EVENT = "events"
SUBSCRIPTION_TYPE_DIGEST = "digest"


def route_subscription(subscription_type: str) -> ModelConfig:
    """
    Маршрутизация для подписок. Q2 показал: Flash Lite справляется
    с классификацией событий и короткими дайджестами за копейки.

    Для MVP оба типа идут на Flash Lite. Если в будущем добавим премиум
    digest с Gemini 2.5 Flash — здесь же появится развилка.
    """
    return SUPPORTED_MODELS[GEMINI_LITE_MODEL_SLUG]
