"""
Интеграция медиафильтра с endpoint'ами /tg/analyze_chat и
/tg/analyze_chats_group.

main.py делает только:
  • парсит payload["media_filter"] через `request_from_payload`;
  • если фильтр включён — вызывает `run_and_build_response`,
    получает (response_dict, tokens_charged, used_model_meta);
  • сам пишет UsageEvent + billing.debit как для обычного Q&A;
  • возвращает response_dict.

Биллинг суммирует стоимость всех LLM-вызовов (1 парсер + N батчей
реранкера) по их фактическим моделям и тарифам. Парсер и реранкер
могут попасть на разные модели (Gemini Flash Lite / Flash / OpenAI),
поэтому считаем построчно через get_token_rates.

Если медиафильтр выключен (поля нет или enabled=false) —
`request_from_payload` вернёт None и endpoint идёт обычной веткой.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from billing import compute_tokens_for_llm_call  # type: ignore[import-not-found]
from llm.orchestrator import LlmRunResult  # type: ignore[import-not-found]
from llm.pricing import get_token_rates  # type: ignore[import-not-found]

from .formatter import format_run
from .orchestrator import MediaFilterRun, run_media_filter
from .types import MediaFilterRequest


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Парсинг payload['media_filter']
# ---------------------------------------------------------------------------


def request_from_payload(payload: dict) -> Optional[MediaFilterRequest]:
    """
    Извлечь MediaFilterRequest из payload'а endpoint'а.

    Возвращает None, если:
      • поля 'media_filter' нет;
      • enabled=false (фильтр явно выключен);
      • валидация провалилась (на стороне endpoint'а это будет означать
        — игнорируем фильтр и идём обычной веткой, не падаем 400).
    """
    raw = payload.get("media_filter")
    if not isinstance(raw, dict):
        return None
    if not raw.get("enabled", True):
        return None
    try:
        return MediaFilterRequest.model_validate(raw)
    except ValidationError as e:
        log.warning("media_filter.payload_invalid err=%s raw=%s", str(e)[:200], raw)
        return None


# ---------------------------------------------------------------------------
# Биллинг: сумма по всем LLM-вызовам
# ---------------------------------------------------------------------------


@dataclass
class BillingBreakdown:
    """Полная разбивка стоимости запроса медиафильтра в наших токенах."""
    total_tokens: int
    per_model: dict[str, int]
    """Сколько токенов начислено каждой использованной модели (для meta_json)."""
    raw_input_tokens: int
    raw_output_tokens: int
    raw_thinking_tokens: int
    used_models: list[str]


async def compute_billing(
    db: AsyncSession,
    llm_results: list[LlmRunResult],
) -> BillingBreakdown:
    """
    Просуммировать стоимость всех LLM-вызовов медиафильтра.

    Если для какой-то модели нет строки в llm_pricing — биллим её
    минимумом (1 токен), как делается в обычном Q&A. Это аварийный
    путь; на проде у всех используемых моделей должны быть тарифы.
    """
    per_model: dict[str, int] = {}
    total = 0
    in_tot = out_tot = think_tot = 0
    used: list[str] = []
    # Кэшируем тарифы — один и тот же slug может встретиться в нескольких
    # вызовах (реранкер обычно с одной и той же моделью).
    rate_cache: dict[str, Any] = {}

    for r in llm_results:
        if r is None:
            continue
        slug = r.used_model.slug
        used.append(slug)
        in_tokens = r.usage.input_tokens or 0
        out_tokens = r.usage.output_tokens or 0
        think_tokens = r.usage.thinking_tokens or 0
        in_tot += in_tokens
        out_tot += out_tokens
        think_tot += think_tokens
        if slug not in rate_cache:
            rate_cache[slug] = await get_token_rates(db, slug)
        rates = rate_cache[slug]
        if rates is None:
            log.error(
                "media_filter.billing.no_pricing_row model=%s — fallback к 1 токену",
                slug,
            )
            cost = 1
        else:
            cost = compute_tokens_for_llm_call(
                input_tokens=in_tokens,
                output_tokens=out_tokens,
                thinking_tokens=think_tokens,
                in_per_1k=rates.in_per_1k,
                out_per_1k=rates.out_per_1k,
            )
        per_model[slug] = per_model.get(slug, 0) + cost
        total += cost

    return BillingBreakdown(
        total_tokens=total,
        per_model=per_model,
        raw_input_tokens=in_tot,
        raw_output_tokens=out_tot,
        raw_thinking_tokens=think_tot,
        used_models=used,
    )


# ---------------------------------------------------------------------------
# Верхняя точка: запустить пайплайн и собрать dict-ответ для endpoint'а
# ---------------------------------------------------------------------------


@dataclass
class MediaFilterEndpointResult:
    """
    Что endpoint должен использовать после вызова:
      response_dict — то, что в итоге возвращаем фронту (он рендерит
                      по полю `media_filter`).
      tokens_charged — суммарная стоимость для billing.debit().
      run — сырой MediaFilterRun (если endpoint хочет залогировать
            метрики/per-chat счётчики в meta_json).
      billing — детализация стоимости по моделям (для meta_json).
      llm_results — все LlmRunResult'ы (для логов).
    """
    response_dict: dict
    tokens_charged: int
    run: MediaFilterRun
    billing: BillingBreakdown
    llm_results: list[LlmRunResult]


def _ui_window_from_period(
    *,
    period_seconds: Optional[int],
    days: int,
    now: datetime,
) -> tuple[datetime, Optional[datetime]]:
    """
    Расчёт UI-окна по тарифным параметрам (тем же способом, что делает
    fetch_chat_messages: now − period). max_date = None (= «до сейчас»),
    парсер сможет сузить через time_window_override.
    """
    if period_seconds is not None and int(period_seconds) > 0:
        ui_from = now - timedelta(seconds=int(period_seconds))
    else:
        ui_from = now - timedelta(days=int(days or 1))
    return ui_from, None


async def run_and_build_response(
    db: AsyncSession,
    owner_user_id: int,
    *,
    chat_links: list[str],
    is_group: bool,
    request: MediaFilterRequest,
    period_seconds: Optional[int],
    days: int,
    user_query: Optional[str],
    now: Optional[datetime] = None,
) -> MediaFilterEndpointResult:
    """
    Полный путь: парсер → fetch → пост-фильтр → реранкер → форматтер +
    подсчёт стоимости.

    Возвращает MediaFilterEndpointResult, который endpoint склеит с
    общими полями ответа (usage_snapshot, chat_name, и т.п.) и
    выполнит UsageEvent + billing.debit стандартным путём.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    ui_from, ui_to = _ui_window_from_period(
        period_seconds=period_seconds, days=days, now=now,
    )

    run = await run_media_filter(
        db, owner_user_id,
        chat_links=chat_links,
        is_group=is_group,
        request=request,
        ui_window_from=ui_from,
        ui_window_to=ui_to,
        user_query=user_query,
        now=now,
    )

    # Собираем плоский список LlmRunResult'ов: парсер (если был) + реранкер'ы.
    llm_results: list[LlmRunResult] = []
    if run.parser_llm is not None:
        llm_results.append(run.parser_llm)
    llm_results.extend(run.reranker_llms)

    breakdown = await compute_billing(db, llm_results)

    answer = format_run(run)

    # Ответ endpoint'а. Поля, которые нужны фронту:
    #   media_filter — основной payload рендера карточек.
    #   tokens_charged — для chat-tail отображения.
    # usage_snapshot заполнит main.py из своего helper'а.
    response_dict: dict = {
        "media_filter": answer.model_dump(mode="json"),
        "tokens_charged": breakdown.total_tokens,
        # Удобные побочные метрики (фронт может игнорировать):
        "media_filter_meta": {
            "used_models": breakdown.used_models,
            "per_model_tokens": breakdown.per_model,
            "parser_fallback": run.used_parser_fallback,
            "reranker_fallback": run.used_reranker_fallback,
            "effective_window": {
                "from_iso": run.effective_window.min_date.isoformat(),
                "to_iso": (
                    run.effective_window.max_date.isoformat()
                    if run.effective_window.max_date else None
                ),
            },
            "selected_categories": [c.value for c in run.selected_categories],
            "per_chat": [
                {
                    "chat_link": c.chat_link,
                    "fetched": c.fetched_count,
                    "after_structured": c.after_structured_count,
                    "after_semantic": c.after_semantic_count,
                    "error_code": c.error_code,
                }
                for c in run.chats
            ],
        },
    }

    return MediaFilterEndpointResult(
        response_dict=response_dict,
        tokens_charged=breakdown.total_tokens,
        run=run,
        billing=breakdown,
        llm_results=llm_results,
    )
