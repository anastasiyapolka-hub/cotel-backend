"""
Оркестратор LLM-вызовов: разворачивает fallback-chain из RoutingDecision и
по очереди дёргает каждую модель через complete_with_retry.

См. architecture-router-and-credits.md, разделы 1.2 (fallback chain) и 1.3
(tier_max_output). См. Q6 incident в test-analysis-Q6.md (Gemini 2.5 Pro
выдала 503 шесть раз подряд — ровно для такого случая и нужен fallback).

Логика:
  1. Берём primary модель из decision.fallback_chain
  2. complete_with_retry() — 1 первичная + 2 retries с backoff (1с, 3с)
  3. На LlmRetryableError (все попытки исчерпаны) → следующая модель
  4. На LlmFatalError (400/401/403) → НЕ пробуем следующую — это наша
     ошибка конфига, fallback не поможет
  5. На неизвестную exception → пробрасываем без обёртки
  6. Если все модели цепочки упали → LlmAllModelsFailedError

ВАЖНО: оркестратор возвращает информацию о ФАКТИЧЕСКИ использованной
модели, не о primary. Это критично для биллинга — у разных моделей
разные цены, списание должно идти по реальной модели.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .adapters import (
    LlmFatalError,
    LlmRetryableError,
    complete_with_retry,
)
from .models import ModelConfig
from .routing import RoutingDecision
from .usage import LlmUsage

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Исключения
# ---------------------------------------------------------------------------


class LlmAllModelsFailedError(Exception):
    """
    Все модели в fallback-chain упали на retryable ошибках.

    Это редкий, но критичный кейс — все провайдеры (Google + Anthropic +
    OpenAI) одновременно недоступны. Вызывающий код должен показать
    пользователю «временные проблемы, попробуйте через минуту», логировать
    в Sentry-аналог и алертить админу.
    """

    def __init__(self, message: str, *, attempted_models: list[str]):
        super().__init__(message)
        self.attempted_models = attempted_models


# ---------------------------------------------------------------------------
# Результат вызова
# ---------------------------------------------------------------------------


@dataclass
class LlmRunResult:
    """
    Результат успешного LLM-вызова через оркестратор.

    Передаётся в endpoint для:
      - рендера ответа пользователю (text)
      - биллинга (used_model + usage → расчёт стоимости в наших токенах)
      - записи в usage_events / user_query_log (модель, finish_reason,
        attempted_models, was_fallback)
    """
    text: str
    usage: LlmUsage
    finish_reason: Optional[str]
    used_model: ModelConfig
    """ФАКТИЧЕСКИ сработавшая модель. Важна для биллинга — pricing разный."""

    primary_model: ModelConfig
    """Что роутер выбрал изначально. Может отличаться от used_model при fallback."""

    attempted_models: list[str] = field(default_factory=list)
    """Slug'и всех моделей, которые попробовали (включая успешную). Для метрик."""

    fallback_reasons: list[str] = field(default_factory=list)
    """Краткие текстовки причин падений каждой модели до успешной. Для логов/админки."""

    @property
    def was_fallback(self) -> bool:
        """True если первая модель упала, использовалась альтернативная."""
        return self.used_model.slug != self.primary_model.slug


# ---------------------------------------------------------------------------
# Главная функция: run()
# ---------------------------------------------------------------------------


async def run(
    *,
    decision: RoutingDecision,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
) -> LlmRunResult:
    """
    Прокрутить fallback-chain из RoutingDecision до первой успешной модели.

    Параметры:
      decision — результат routing.route(). Содержит primary_model,
        fallback_chain (упорядоченный список) и max_output_tokens.
      system_prompt, user_prompt, temperature — стандартный набор для LLM.

    Возврат: LlmRunResult с фактической моделью и usage'ом.

    Исключения:
      LlmFatalError    — сразу же, как только одна из моделей вернула
                         non-retryable ошибку (400/401/403). Не пробуем
                         следующую — fallback тут не поможет.
      LlmAllModelsFailedError — все модели цепочки упали на retryable
                         ошибках. Endpoint должен показать пользователю
                         «временные проблемы».
      Прочие Exception — пробрасываем как есть (включая неклассифицированные
                         ошибки от провайдеров).
    """
    attempted: list[str] = []
    fail_reasons: list[str] = []

    for idx, model in enumerate(decision.fallback_chain):
        attempted.append(model.slug)
        is_primary = (idx == 0)

        try:
            text, usage, finish_reason = await complete_with_retry(
                provider=model.provider,
                provider_model=model.provider_model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                max_output_tokens=decision.max_output_tokens,
            )
        except LlmFatalError as exc:
            # Это наша ошибка (неверный ключ, неподдерживаемый параметр).
            # Fallback не поможет — другие модели имеют ту же проблему.
            log.error(
                "orchestrator.fatal_no_fallback model=%s tier=%s category=%s err=%s",
                model.slug, decision.tier, decision.category, exc,
            )
            raise
        except LlmRetryableError as exc:
            # Все retries этой модели исчерпаны. Логируем и идём дальше.
            reason = f"{model.slug}: {exc} (status={exc.status_code})"
            fail_reasons.append(reason)
            log.warning(
                "orchestrator.fallback_triggered position=%s/%s model=%s "
                "is_primary=%s reason=%s",
                idx + 1, len(decision.fallback_chain), model.slug,
                is_primary, reason,
            )
            continue

        # Успех — отдаём результат
        if not is_primary:
            log.info(
                "orchestrator.fallback_success used_model=%s primary=%s "
                "tier=%s category=%s fallback_position=%s/%s "
                "primary_fail_reasons=%s",
                model.slug, decision.primary_model.slug,
                decision.tier, decision.category,
                idx + 1, len(decision.fallback_chain), fail_reasons,
            )

        return LlmRunResult(
            text=text,
            usage=usage,
            finish_reason=finish_reason,
            used_model=model,
            primary_model=decision.primary_model,
            attempted_models=attempted,
            fallback_reasons=fail_reasons,
        )

    # Все модели в цепочке упали на retryable error
    log.error(
        "orchestrator.all_failed tier=%s category=%s attempted=%s reasons=%s",
        decision.tier, decision.category, attempted, fail_reasons,
    )
    raise LlmAllModelsFailedError(
        f"All {len(decision.fallback_chain)} models failed with retryable errors. "
        f"Last reasons: {fail_reasons}",
        attempted_models=attempted,
    )


# ---------------------------------------------------------------------------
# Хелпер для meta_json — что писать в usage_events после вызова
# ---------------------------------------------------------------------------


def routing_meta(result: LlmRunResult, decision: RoutingDecision) -> dict:
    """
    Собрать поля для meta_json в usage_events / token_transactions,
    отражающие принятые роутером решения. Удобно для админ-аналитики.

    Не дублирует поля, которые уже есть в основных колонках usage_events
    (ai_model, провайдер). Содержит только то, что специфично для роутера:
    primary vs used, цепочка попыток, причины фейлов.
    """
    meta = {
        "tier": decision.tier,
        "category": decision.category,
        "primary_model": decision.primary_model.slug,
        "used_model": result.used_model.slug,
        "attempted_models": list(result.attempted_models),
        "max_output_tokens": decision.max_output_tokens,
    }
    if result.was_fallback:
        meta["was_fallback"] = True
        meta["fallback_reasons"] = list(result.fallback_reasons)
    return meta
