"""
LLM-парсер свободного текста пользователя для медиафильтра.

Один вызов LLM, превращающий «видео больше 10 МБ за последние 5 минут»
в структурный JSON `ParsedUserQuery`. НЕ смотрит на сообщения Telegram —
работает только с текстом запроса. Дёшево, маленький контекст, light tier.

Модель: `GEMINI_LITE_MODEL_SLUG` (Gemini Flash Lite) с фолбэком на
GPT 4.1 mini через стандартный orchestrator.run().

Если запрос пустой — НЕ вызываем LLM вовсе, возвращаем дефолтный
`ParsedUserQuery` (все фильтры null, без реранкера). См.
architecture-media-filter.md §7 "Вызов №1 — Парсер запроса".
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pydantic import ValidationError

from llm.models import (  # type: ignore[import-not-found]
    GEMINI_LITE_MODEL_SLUG,
    OPENAI_MODEL_SLUG,
    SUPPORTED_MODELS,
)
from llm.orchestrator import LlmAllModelsFailedError, LlmRunResult, run as orchestrator_run  # type: ignore[import-not-found]
from llm.routing import RoutingDecision, TIER_LIGHT  # type: ignore[import-not-found]

from .types import ParsedUserQuery


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Параметры вызова
# ---------------------------------------------------------------------------


# Парсер возвращает короткий JSON (≤512 токенов с запасом). На light
# tier ставим минимальный max_output, чтобы не платить за пустое место.
_PARSER_MAX_OUTPUT_TOKENS = 512
_PARSER_TEMPERATURE = 0.0  # детерминированный извлекатель, не творчество


# Fallback-цепочка для парсера: основная Gemini Flash Lite, запасная
# GPT-4.1 mini (light-tier цепочка проекта). Если упадут обе — это
# реальный инцидент с провайдерами, и оркестратор бросит
# LlmAllModelsFailedError, который мы выше ловим и трактуем как «парсер
# недоступен, идём БЕЗ структурных ограничений».
_PARSER_FALLBACK_CHAIN_SLUGS = [GEMINI_LITE_MODEL_SLUG, OPENAI_MODEL_SLUG]


def _build_parser_decision() -> RoutingDecision:
    """Собрать RoutingDecision вручную: парсеру не нужен classifier.route()."""
    chain = [SUPPORTED_MODELS[s] for s in _PARSER_FALLBACK_CHAIN_SLUGS if s in SUPPORTED_MODELS]
    return RoutingDecision(
        primary_model=chain[0],
        fallback_chain=chain,
        max_output_tokens=_PARSER_MAX_OUTPUT_TOKENS,
        tier=TIER_LIGHT,
        category="media_filter_parser",  # не из CLASSIFIER, синтетическая метка для логов
    )


# ---------------------------------------------------------------------------
# Промпты
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = """Ты — структурный парсер пользовательских уточнений для медиафильтра в Telegram-аналитике.

Тебе на вход придёт:
  • UI_CONTEXT — какие категории медиа и какое окно времени уже выбраны в UI пользователем.
  • USER_QUERY — свободный текст пользователя, в котором он может уточнить запрос (размер, длительность, кто отправитель, и т.п.).
  • NOW — текущее время в UTC ISO 8601, понадобится для расчёта относительных интервалов ("за последние 5 минут" → конкретные from/to).

Твоя задача — вернуть СТРОГО JSON-объект следующей формы (без markdown, без пояснений до или после):

{
  "structured_filters": {
    "file_size_min_bytes": <int|null>,
    "file_size_max_bytes": <int|null>,
    "duration_min_sec":    <int|null>,
    "duration_max_sec":    <int|null>,
    "width_min_px":        <int|null>,
    "height_min_px":       <int|null>,
    "sender_username":     <string|null>,
    "mime_type_contains":  <string|null>,
    "file_name_contains":  <string|null>,
    "time_window_override": {
      "from_iso": <string ISO 8601 UTC|null>,
      "to_iso":   <string ISO 8601 UTC|null>
    }
  },
  "semantic_query":          <string|null>,
  "needs_semantic_rerank":   <bool>
}

ПРАВИЛА:

1. Поля заполняй ТОЛЬКО если соответствующее ограничение есть в USER_QUERY. Если пользователь ничего о размере не сказал — обе file_size_* должны быть null.

2. Все числа — целые. Размер всегда в БАЙТАХ (10 МБ → 10485760), длительность в СЕКУНДАХ, разрешение в ПИКСЕЛЯХ. Никаких строк типа "10MB" — конвертируй сам.

3. Категории медиа и базовое временное окно (UI_CONTEXT) ты НЕ меняешь — их выбрал пользователь в интерфейсе.

4. time_window_override заполняй ТОЛЬКО если пользователь явно сказал про время в USER_QUERY ("за последние 5 минут", "с 13:00 до 13:05 по UTC", "после 10 утра"). Используй NOW для расчёта относительных промежутков. Если в USER_QUERY одна граница — заполни только её, другую оставь null.

5. semantic_query — то, что НЕ свелось к структуре. Например, "видео про митинг" → "митинг". Если в запросе чисто числовые/именные ограничения (размер, имя файла, отправитель) — semantic_query = null.

6. needs_semantic_rerank = true тогда и только тогда, когда semantic_query != null.

7. sender_username — без "@". Если пользователь написал "от @ivanov" — sender_username = "ivanov".

8. mime_type_contains и file_name_contains — подстрока для регистронезависимого поиска. "PDF файлы" → mime_type_contains = "pdf". "файлы report-*.docx" → file_name_contains = "report-".

9. Никаких комментариев, markdown-блоков, скобок-постамбул. Только валидный JSON-объект."""


def _build_user_prompt(
    user_query: str,
    ui_window_from: Optional[datetime],
    ui_window_to: Optional[datetime],
    selected_categories: list[str],
    now: datetime,
) -> str:
    """
    Собрать user-prompt с UI_CONTEXT, NOW и USER_QUERY. UI-окно
    передаётся как опорная точка — LLM должен знать рамки, но не имеет
    права их расширять.
    """
    ui_from_iso = ui_window_from.isoformat() if ui_window_from else "null"
    ui_to_iso = ui_window_to.isoformat() if ui_window_to else "null"
    cats_str = ", ".join(selected_categories) if selected_categories else "all"
    return (
        f"UI_CONTEXT:\n"
        f"  selected_categories: [{cats_str}]\n"
        f"  ui_window_from_iso:  {ui_from_iso}\n"
        f"  ui_window_to_iso:    {ui_to_iso}\n"
        f"\n"
        f"NOW: {now.isoformat()}\n"
        f"\n"
        f"USER_QUERY:\n{user_query}\n"
    )


# ---------------------------------------------------------------------------
# Извлечение JSON из ответа модели
# ---------------------------------------------------------------------------


_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)


def _extract_json_payload(raw_text: str) -> str:
    """
    Очистить от code-fence обёрток (` ```json ... ``` `), которые
    некоторые модели любят добавлять, несмотря на «без markdown».
    """
    text = (raw_text or "").strip()
    m = _JSON_FENCE_RE.match(text)
    if m:
        return m.group(1).strip()
    return text


# ---------------------------------------------------------------------------
# Публичная функция
# ---------------------------------------------------------------------------


@dataclass
class ParseOutcome:
    """
    Полный результат вызова парсера. Содержит и распарсенную структуру,
    и raw LlmRunResult — последний нужен для биллинга в оркестраторе
    более высокого уровня (мы не списываем токены тут сами).
    """
    parsed: ParsedUserQuery
    llm_result: Optional[LlmRunResult]
    used_fallback: bool
    """True если LLM упал/невалидный JSON и мы вернули дефолт без фильтров."""


async def parse_user_query(
    *,
    user_query: str,
    ui_window_from: Optional[datetime],
    ui_window_to: Optional[datetime],
    selected_categories: list[str],
    now: Optional[datetime] = None,
) -> ParseOutcome:
    """
    Если user_query пуст — НЕ дёргаем LLM, отдаём дефолтный ParsedUserQuery.

    Иначе вызываем оркестратор. Если LLM вернул невалидный JSON или
    упал по retryable-ошибкам — отдаём дефолт с used_fallback=True
    (мы не хотим терять весь запрос из-за глюка парсера; пользователь
    хотя бы получит карточки по UI-категориям без структурных уточнений).
    """
    query = (user_query or "").strip()
    if not query:
        return ParseOutcome(
            parsed=ParsedUserQuery(),
            llm_result=None,
            used_fallback=False,
        )

    if now is None:
        now = datetime.now(timezone.utc)

    system_prompt = _SYSTEM_PROMPT
    user_prompt = _build_user_prompt(
        user_query=query,
        ui_window_from=ui_window_from,
        ui_window_to=ui_window_to,
        selected_categories=selected_categories,
        now=now,
    )
    decision = _build_parser_decision()

    try:
        llm_result = await orchestrator_run(
            decision=decision,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=_PARSER_TEMPERATURE,
        )
    except LlmAllModelsFailedError:
        log.error("media_filter.parser.all_models_failed query_len=%d", len(query))
        return ParseOutcome(
            parsed=ParsedUserQuery(),
            llm_result=None,
            used_fallback=True,
        )

    raw_json = _extract_json_payload(llm_result.text)
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as e:
        log.warning(
            "media_filter.parser.invalid_json model=%s err=%s raw=%s",
            llm_result.used_model.slug, e, raw_json[:200],
        )
        return ParseOutcome(
            parsed=ParsedUserQuery(),
            llm_result=llm_result,  # биллим даже невалидный ответ — мы за вызов платили
            used_fallback=True,
        )

    try:
        parsed = ParsedUserQuery.model_validate(data)
    except ValidationError as e:
        log.warning(
            "media_filter.parser.schema_mismatch model=%s err=%s",
            llm_result.used_model.slug, str(e)[:300],
        )
        return ParseOutcome(
            parsed=ParsedUserQuery(),
            llm_result=llm_result,
            used_fallback=True,
        )

    # Пост-валидация инварианта: needs_semantic_rerank ↔ semantic_query
    if parsed.needs_semantic_rerank and not parsed.semantic_query:
        parsed.needs_semantic_rerank = False
    if parsed.semantic_query and not parsed.needs_semantic_rerank:
        parsed.needs_semantic_rerank = True

    return ParseOutcome(
        parsed=parsed,
        llm_result=llm_result,
        used_fallback=False,
    )
