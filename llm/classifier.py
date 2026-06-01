"""
LLM-классификатор категории пользовательского запроса.

См. architecture-router-and-credits.md, раздел 1.2.

Зачем: пользователь выбирает только глубину анализа (light/balanced/deep).
Внутри tier'а конкретная модель определяется по КАТЕГОРИИ запроса
(дайджест, чеклист, поиск противоречий и т.д.). Эту категорию определяет
этот модуль через короткий вызов Flash Lite.

Почему не keyword/regex:
  - Работает на любом языке (русский, грузинский, турецкий, казахский...)
  - Ловит семантику, не только слова
  - ~$0,0001 за вызов — дешевле, чем кажется

Семантика fallback: при ЛЮБОЙ ошибке (сеть, невалидный JSON, неизвестная
категория) возвращаем simple_qa и идём дальше. Никогда не блокируем
запрос пользователя из-за сбоя классификатора.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

from .adapters import get_adapter
from .models import GEMINI_LITE_MODEL_SLUG, SUPPORTED_MODELS

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Категории запросов
# ---------------------------------------------------------------------------
#
# ВАЖНО: эти строки используются как ключи в routing.py decision matrix.
# При добавлении новой категории — добавить и в промпт (CLASSIFIER_PROMPT),
# и в DECISION_MATRIX в routing.py.
# ---------------------------------------------------------------------------

CATEGORY_SIMPLE_QA = "simple_qa"
CATEGORY_DIGEST = "digest"
CATEGORY_PLAN_CHECKLIST = "plan_checklist"
CATEGORY_FIND_CONTRADICTIONS = "find_contradictions"
CATEGORY_FILTER_RANK = "filter_rank"
CATEGORY_SENTIMENT_DYNAMICS = "sentiment_dynamics"
CATEGORY_SOURCE_SYNTHESIS = "source_synthesis"
CATEGORY_CROSS_CHAT_ANALYSIS = "cross_chat_analysis"

ALL_CATEGORIES = frozenset({
    CATEGORY_SIMPLE_QA,
    CATEGORY_DIGEST,
    CATEGORY_PLAN_CHECKLIST,
    CATEGORY_FIND_CONTRADICTIONS,
    CATEGORY_FILTER_RANK,
    CATEGORY_SENTIMENT_DYNAMICS,
    CATEGORY_SOURCE_SYNTHESIS,
    CATEGORY_CROSS_CHAT_ANALYSIS,
})

DEFAULT_CATEGORY = CATEGORY_SIMPLE_QA
"""Fallback при ошибках классификатора. Самая безопасная (≈light QA)."""


@dataclass(frozen=True)
class ClassificationResult:
    """
    Результат LLM-классификации одного запроса.

    Передаётся в routing.route() для выбора модели. Используется также
    для записи в user_query_log (см. db/models.py) — там лежит и
    detected_category, и final_category (если пользователь override).
    """
    category: str
    confidence: float
    needs_structured_format: bool
    is_fallback: bool = False
    """True если результат — fallback (классификатор упал или вернул мусор)."""


# ---------------------------------------------------------------------------
# Промпт классификатора
# ---------------------------------------------------------------------------
#
# Короткий, фиксированный. Категории описаны через примеры на русском,
# но модель отвечает на ЛЮБОМ языке пользователя — она парсит семантику.
# Возвращает строгий JSON.
# ---------------------------------------------------------------------------

CLASSIFIER_SYSTEM_PROMPT = (
    "You are a query classifier for a chat-analysis service. "
    "Given a user query (in any language), classify it into one category "
    "from the list below and return STRICT JSON.\n\n"
    "Categories:\n"
    "- simple_qa            — simple fact extraction, \"что было\", short answer\n"
    "- digest               — thematic digest, \"о чём говорили\", \"main topics\"\n"
    "- plan_checklist       — \"give me a plan\", \"checklist\", \"how should I\", "
    "\"инструкция\", \"советы\", \"шаги\"\n"
    "- find_contradictions  — \"найди противоречия\", \"А vs Б\", "
    "\"спорные мнения\", \"opposing views\"\n"
    "- filter_rank          — \"найди по фильтру X\", \"топ-N\", \"отранжируй\", "
    "\"select best\"\n"
    "- sentiment_dynamics   — \"как менялось настроение\", \"sentiment over time\", "
    "\"динамика обсуждений\"\n"
    "- source_synthesis     — \"оцени источники\", \"авторитетность\", "
    "\"reliable sources\"\n"
    "- cross_chat_analysis  — explicit request to analyze across MULTIPLE chats\n\n"
    "Return STRICT JSON (no markdown, no preamble):\n"
    "{\n"
    "  \"category\": \"<one of above>\",\n"
    "  \"confidence\": <float 0.0–1.0>,\n"
    "  \"needs_structured_format\": <true|false>\n"
    "}\n\n"
    "Set needs_structured_format=true ONLY if the query explicitly enumerates "
    "required answer sections like \"1) X, 2) Y, 3) Z\" or \"give me three blocks: A, B, C\".\n"
    "If uncertain — pick simple_qa with low confidence. Never invent a new category."
)

CLASSIFIER_TEMPERATURE = 0.0
"""Детерминизм классификатора — одинаковый ввод → одинаковая категория."""

CLASSIFIER_MAX_OUTPUT_TOKENS = 100
"""JSON с категорией укладывается в 30-50 токенов. Запас на безопасность."""


# ---------------------------------------------------------------------------
# Главная функция
# ---------------------------------------------------------------------------


async def classify_query(user_query: str) -> ClassificationResult:
    """
    Классифицировать пользовательский запрос в одну из ALL_CATEGORIES.

    Использует Gemini Flash Lite — самая дешёвая и быстрая модель в каталоге.
    Стоимость одного вызова ~$0,0001 (200 input + 50 output).

    Никогда не raise'ит — на любую ошибку возвращает ClassificationResult
    с is_fallback=True и category=DEFAULT_CATEGORY. Запрос пользователя не
    блокируется из-за сбоя классификатора.
    """
    query = (user_query or "").strip()
    if not query:
        return _fallback("empty_query")

    config = SUPPORTED_MODELS[GEMINI_LITE_MODEL_SLUG]
    adapter = get_adapter(config.provider)

    try:
        text, usage, finish_reason = await adapter.complete(
            provider_model=config.provider_model,
            system_prompt=CLASSIFIER_SYSTEM_PROMPT,
            user_prompt=f"User query:\n{query}",
            temperature=CLASSIFIER_TEMPERATURE,
            max_output_tokens=CLASSIFIER_MAX_OUTPUT_TOKENS,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("classifier.llm_call_failed err=%s", exc)
        return _fallback("llm_call_failed")

    parsed = _parse_classifier_json(text)
    if parsed is None:
        log.warning("classifier.json_parse_failed text=%r", text[:200])
        return _fallback("json_parse_failed")

    category = str(parsed.get("category", "")).strip()
    if category not in ALL_CATEGORIES:
        log.warning("classifier.unknown_category got=%r", category)
        return _fallback("unknown_category")

    confidence = _coerce_confidence(parsed.get("confidence"))
    needs_structured = bool(parsed.get("needs_structured_format", False))

    log.info(
        "classifier.ok category=%s confidence=%.2f structured=%s "
        "input_tokens=%s output_tokens=%s",
        category, confidence, needs_structured,
        getattr(usage, "input_tokens", "?"),
        getattr(usage, "output_tokens", "?"),
    )

    return ClassificationResult(
        category=category,
        confidence=confidence,
        needs_structured_format=needs_structured,
        is_fallback=False,
    )


# ---------------------------------------------------------------------------
# Парсинг ответа классификатора
# ---------------------------------------------------------------------------


def _parse_classifier_json(raw: str) -> Optional[dict]:
    """
    Распарсить JSON из ответа LLM. Если модель прилепила markdown-fence,
    выкорчевать его.
    """
    if not raw:
        return None
    text = raw.strip()
    # Уберём ```json … ``` обвязку если есть.
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.S)
    if fence:
        text = fence.group(1).strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # Попытка вытащить {...} из строки на случай мусора вокруг.
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                data = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
        else:
            return None
    return data if isinstance(data, dict) else None


def _coerce_confidence(value) -> float:
    try:
        c = float(value)
    except (TypeError, ValueError):
        return 0.5
    if c < 0.0:
        return 0.0
    if c > 1.0:
        return 1.0
    return c


def _fallback(reason: str) -> ClassificationResult:
    """Создать fallback-результат с понятным reason в логах."""
    log.info("classifier.fallback reason=%s default=%s", reason, DEFAULT_CATEGORY)
    return ClassificationResult(
        category=DEFAULT_CATEGORY,
        confidence=0.0,
        needs_structured_format=False,
        is_fallback=True,
    )
