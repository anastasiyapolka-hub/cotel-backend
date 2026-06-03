"""
Токенная биллинг-логика: проверка баланса, списание, гранты, top-up.

См. architecture-router-and-credits.md разделы 2.1–2.6.

Терминология: мы работаем с «нашими токенами приложения». Это не LLM-токены.
1 наш токен = $0,001 LLM-стоимости. Маркап 2,5× (маржа 60%) уже зашит в
pricing.get_token_rates(). Здесь мы оперируем уже сконвертированными числами.

Все функции принимают AsyncSession и НЕ делают commit самостоятельно —
commit'ит вызывающий код. Это позволяет атомарно объединять debit() с
записью UsageEvent в одной транзакции.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    Plan,
    TokenTransaction,
    User,
    UserTokenBalance,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Стандартные reason'ы для token_transactions
# ---------------------------------------------------------------------------
#
# Эти строки фиксированы — поиск по ним делается в админке и аналитике.
# Не менять без миграции данных. См. db/models.py TokenTransaction.reason.
# ---------------------------------------------------------------------------

REASON_QA_REQUEST = "qa_request"
REASON_SUBSCRIPTION_EVENT = "subscription_event"
REASON_SUBSCRIPTION_DIGEST = "subscription_digest"
REASON_CLASSIFIER = "classifier"
REASON_MONTHLY_GRANT = "monthly_grant"
REASON_TOPUP_PURCHASE = "topup_purchase"
REASON_REFUND = "refund"
REASON_ADMIN_ADJUSTMENT = "admin_adjustment"

ALL_REASONS = frozenset({
    REASON_QA_REQUEST,
    REASON_SUBSCRIPTION_EVENT,
    REASON_SUBSCRIPTION_DIGEST,
    REASON_CLASSIFIER,
    REASON_MONTHLY_GRANT,
    REASON_TOPUP_PURCHASE,
    REASON_REFUND,
    REASON_ADMIN_ADJUSTMENT,
})


# ---------------------------------------------------------------------------
# Минимальные пороги для soft-блока на старте запроса
# ---------------------------------------------------------------------------
#
# Логика: если у пользователя нет минимум столько-то токенов для tier'а,
# не запускаем запрос. Это защита от ситуации «начал запрос с 5 токенами,
# LLM съел 80, ушёл в минус на 75».
#
# Числа взяты как минимум по нашим Q-тестам: light Q1 ~30, balanced Q5
# ~40, deep Q9 ~200. Берём с двукратным запасом снизу — пусть лучше
# пользователь увидит «нужно докупить» заранее, чем уйдёт в большой минус.
# ---------------------------------------------------------------------------

MIN_TOKENS_PER_TIER = {
    "light":    15,
    "balanced": 25,
    "deep":     100,
}


# ---------------------------------------------------------------------------
# Контейнер ответа
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TokenBalance:
    """
    Снапшот баланса пользователя. Возвращается из get_balance().

    Поля:
      monthly_granted — сколько начислено в текущем месяце (из тарифа на
        момент гранта; смена тарифа в середине месяца не пересчитывает).
      monthly_used    — сколько потрачено из месячного бакета. Не может
        превышать monthly_granted: перерасход уходит в topup_balance.
      topup_balance   — купленные сверх тарифа токены. Могут уйти в минус
        в редких race-конфликтах (post-flight overcharge), но soft-блок
        на check_can_spend это предотвращает в нормальных условиях.

    Производные:
      monthly_remaining — сколько ещё доступно из месячного бакета
      total_remaining   — общий доступный баланс (для UI)
    """
    monthly_granted: int
    monthly_used: int
    topup_balance: int

    @property
    def monthly_remaining(self) -> int:
        return max(0, self.monthly_granted - self.monthly_used)

    @property
    def total_remaining(self) -> int:
        return self.monthly_remaining + max(0, self.topup_balance)


# ---------------------------------------------------------------------------
# 1. get_balance
# ---------------------------------------------------------------------------


async def get_balance(
    db: AsyncSession,
    *,
    user_id: int,
) -> Optional[TokenBalance]:
    """
    Вернуть снапшот баланса пользователя. None если строки в
    user_token_balances нет (это аномалия — должна создаваться при
    регистрации или первой миграции).
    """
    stmt = select(UserTokenBalance).where(UserTokenBalance.user_id == user_id)
    row = (await db.execute(stmt)).scalar_one_or_none()
    if row is None:
        return None
    return TokenBalance(
        monthly_granted=int(row.monthly_granted),
        monthly_used=int(row.monthly_used),
        topup_balance=int(row.topup_balance),
    )


# ---------------------------------------------------------------------------
# 2. check_can_spend (soft-блок на старте запроса)
# ---------------------------------------------------------------------------


async def check_can_spend(
    db: AsyncSession,
    *,
    user_id: int,
    tier: str,
) -> tuple[bool, TokenBalance]:
    """
    Проверка перед стартом LLM-запроса: достаточно ли у пользователя
    токенов хотя бы на минимальную стоимость tier'а.

    Возвращает (can_spend, current_balance). Если can_spend == False,
    вызывающий код отказывает запросу с сообщением «недостаточно токенов,
    докупите или дождитесь начала месяца».

    Если у пользователя НЕТ записи в balances (аномалия — должна быть
    создана при регистрации) — возвращаем (False, дефолт), не запускаем
    запрос. Это сигнал админу, что что-то сломано в подписке регистрации.
    """
    balance = await get_balance(db, user_id=user_id)
    if balance is None:
        log.error(
            "billing.no_balance_row user_id=%s — должна быть создана при регистрации",
            user_id,
        )
        return False, TokenBalance(monthly_granted=0, monthly_used=0, topup_balance=0)

    floor = MIN_TOKENS_PER_TIER.get(tier, MIN_TOKENS_PER_TIER["light"])
    can_spend = balance.total_remaining >= floor
    return can_spend, balance


# ---------------------------------------------------------------------------
# 3. debit — главная функция списания
# ---------------------------------------------------------------------------


async def debit(
    db: AsyncSession,
    *,
    user_id: int,
    amount: int,
    reason: str,
    related_event_id: Optional[int] = None,
    meta: Optional[dict[str, Any]] = None,
) -> Optional[TokenBalance]:
    """
    Атомарно списать `amount` токенов с баланса пользователя.

    Порядок: сначала monthly_used до cap'а, потом topup_balance.
    Это даёт пользователю честный сигнал, когда месячный лимит исчерпан —
    он видит «расходуется из купленных» в UI и осознанно решает докупать.

    Допускает отрицательный topup_balance: если посреди запроса баланс
    кончился, LLM-вызов УЖЕ ПРОИЗОШЁЛ, отменять его бессмысленно. Soft-
    блок на check_can_spend() должен сработать заранее. Минус — сигнал
    для UI «вы превысили лимит, ваш следующий запрос будет отвергнут».

    Параметры:
      amount — положительное число (это сумма списания, не дельта).
        Если передадите 0 или отрицательное — функция вернёт None
        без операции и запишет warning.
      reason — одна из констант REASON_* в этом файле.
      related_event_id — usage_events.id для qa_request / subscription_*.
        NULL для grant / topup / refund.
      meta — произвольный JSONB-снапшот: модель, цены, токены провайдера.

    Возвращает обновлённый снапшот баланса (для логирования / UI),
    либо None если баланс не найден.

    НЕ делает commit — вызывающий код должен закоммитить транзакцию.
    """
    if amount <= 0:
        log.warning("billing.debit_invalid_amount user_id=%s amount=%s reason=%s",
                    user_id, amount, reason)
        return None
    if reason not in ALL_REASONS:
        log.warning("billing.debit_unknown_reason user_id=%s reason=%s",
                    user_id, reason)
        # Не блокируем — пишем как есть, в логах увидим.

    # SELECT FOR UPDATE — защита от race-condition если параллельно идут
    # несколько подписочных вызовов на одного пользователя.
    stmt = (
        select(UserTokenBalance)
        .where(UserTokenBalance.user_id == user_id)
        .with_for_update()
    )
    balance_row = (await db.execute(stmt)).scalar_one_or_none()
    if balance_row is None:
        log.error("billing.debit_no_balance user_id=%s amount=%s reason=%s",
                  user_id, amount, reason)
        return None

    monthly_remaining = max(0, int(balance_row.monthly_granted) - int(balance_row.monthly_used))

    if amount <= monthly_remaining:
        # Хватает месячных — топ-ап не трогаем
        balance_row.monthly_used = int(balance_row.monthly_used) + amount
    else:
        # Расход разделяется: monthly до cap'а + topup
        topup_part = amount - monthly_remaining
        balance_row.monthly_used = int(balance_row.monthly_granted)  # cap
        balance_row.topup_balance = int(balance_row.topup_balance) - topup_part
        # topup_balance может уйти в минус — спецификация это позволяет
        # для post-flight overcharge. Soft-блок предотвращает в норме.

    # Журнал транзакций — delta отрицательная (это списание)
    db.add(TokenTransaction(
        user_id=user_id,
        delta=-amount,
        reason=reason,
        related_event_id=related_event_id,
        meta_json=meta,
    ))

    # Возвращаем свежий снапшот для удобства вызывающего
    return TokenBalance(
        monthly_granted=int(balance_row.monthly_granted),
        monthly_used=int(balance_row.monthly_used),
        topup_balance=int(balance_row.topup_balance),
    )


# ---------------------------------------------------------------------------
# 4. compute_tokens_for_llm_call — пересчёт LLM-токенов в наши токены
# ---------------------------------------------------------------------------


def compute_tokens_for_llm_call(
    *,
    input_tokens: int,
    output_tokens: int,
    thinking_tokens: int,
    in_per_1k: float,
    out_per_1k: float,
) -> int:
    """
    Считаем стоимость одного LLM-вызова в наших токенах приложения.

    Правила (см. architecture-router-and-credits.md, раздел 2.6):
      - вход × in_per_1k / 1000   — округление вверх (CEIL)
      - выход × out_per_1k / 1000 — округление вверх
      - thinking × out_per_1k / 1000 — биллится по output-цене
        (OpenAI и Google так берут с нас в реальности)
      - минимум 1 токен на вызов (защита от пустых ответов = 0 токенов)

    Числа in_per_1k и out_per_1k берутся из pricing.get_token_rates()
    — там уже зашит маркап 2,5× для маржи 60%.
    """
    if input_tokens < 0:
        input_tokens = 0
    if output_tokens < 0:
        output_tokens = 0
    if thinking_tokens < 0:
        thinking_tokens = 0

    in_tokens = math.ceil(input_tokens * in_per_1k / 1000.0)
    out_tokens = math.ceil(output_tokens * out_per_1k / 1000.0)
    think_tokens = math.ceil(thinking_tokens * out_per_1k / 1000.0)
    total = in_tokens + out_tokens + think_tokens
    return max(1, total)


# ---------------------------------------------------------------------------
# 5. grant_monthly — для крона на 1-е число
# ---------------------------------------------------------------------------


async def grant_monthly(
    db: AsyncSession,
    *,
    user_id: int,
    plan_monthly_tokens: int,
    period_start: date,
) -> None:
    """
    Месячный ресет: monthly_used = 0, monthly_granted = plan.monthly_tokens,
    period_start = первое число нового месяца.

    Topup_balance не трогаем — он накапливается между месяцами.

    Если строки в balances не существует — создаём (это спасёт от
    аномалии «зарегался, но баланс не создался»).

    Пишет в журнал транзакций строку monthly_grant с delta = +monthly_tokens.

    НЕ делает commit.
    """
    if plan_monthly_tokens < 0:
        log.warning("billing.grant_negative_tokens user_id=%s tokens=%s",
                    user_id, plan_monthly_tokens)
        return

    stmt = (
        select(UserTokenBalance)
        .where(UserTokenBalance.user_id == user_id)
        .with_for_update()
    )
    balance_row = (await db.execute(stmt)).scalar_one_or_none()

    if balance_row is None:
        # Аномалия — создаём
        balance_row = UserTokenBalance(
            user_id=user_id,
            period_start=period_start,
            monthly_granted=plan_monthly_tokens,
            monthly_used=0,
            topup_balance=0,
        )
        db.add(balance_row)
        log.info("billing.grant_created_balance user_id=%s tokens=%s",
                 user_id, plan_monthly_tokens)
    else:
        balance_row.period_start = period_start
        balance_row.monthly_granted = plan_monthly_tokens
        balance_row.monthly_used = 0
        # topup_balance не трогаем

    db.add(TokenTransaction(
        user_id=user_id,
        delta=plan_monthly_tokens,
        reason=REASON_MONTHLY_GRANT,
        related_event_id=None,
        meta_json={"period_start": period_start.isoformat()},
    ))


# ---------------------------------------------------------------------------
# 6. apply_topup — для Stripe webhook на успешную оплату
# ---------------------------------------------------------------------------


async def apply_topup(
    db: AsyncSession,
    *,
    user_id: int,
    tokens_amount: int,
    payment_id: str,
    package_label: Optional[str] = None,
) -> Optional[TokenBalance]:
    """
    Зачислить top-up токены на баланс пользователя после успешной оплаты.

    Параметры:
      tokens_amount — сколько наших токенов докупил пользователь (по
        прайс-листу: small=1600, medium=5500, large=16000).
      payment_id — Stripe payment_intent.id для идемпотентности и аудита.
      package_label — 'small' / 'medium' / 'large' для логов и админки.

    Атомарно: увеличивает topup_balance, пишет в журнал транзакций
    запись topup_purchase с delta=+tokens_amount и meta = {payment_id, package}.

    НЕ делает commit.

    ВАЖНО: вызывающий webhook-handler обязан до этого проверить
    идемпотентность через payment_id (поиск в token_transactions с
    meta.stripe_payment_id == payment_id), иначе повторный webhook от
    Stripe даст двойное зачисление.
    """
    if tokens_amount <= 0:
        log.warning("billing.topup_invalid_amount user_id=%s amount=%s",
                    user_id, tokens_amount)
        return None

    stmt = (
        select(UserTokenBalance)
        .where(UserTokenBalance.user_id == user_id)
        .with_for_update()
    )
    balance_row = (await db.execute(stmt)).scalar_one_or_none()
    if balance_row is None:
        log.error(
            "billing.topup_no_balance user_id=%s — баланс должен существовать",
            user_id,
        )
        return None

    balance_row.topup_balance = int(balance_row.topup_balance) + tokens_amount

    db.add(TokenTransaction(
        user_id=user_id,
        delta=tokens_amount,
        reason=REASON_TOPUP_PURCHASE,
        related_event_id=None,
        meta_json={
            "stripe_payment_id": payment_id,
            "package": package_label,
        },
    ))

    return TokenBalance(
        monthly_granted=int(balance_row.monthly_granted),
        monthly_used=int(balance_row.monthly_used),
        topup_balance=int(balance_row.topup_balance),
    )


# ---------------------------------------------------------------------------
# 7. admin_adjust — ручная коррекция через админ-панель
# ---------------------------------------------------------------------------


async def admin_adjust(
    db: AsyncSession,
    *,
    user_id: int,
    delta: int,
    bucket: str,
    note: str,
    admin_user_id: int,
) -> Optional[TokenBalance]:
    """
    Ручная коррекция баланса пользователя админом.

    Параметры:
      delta — положительное = добавить токены, отрицательное = снять.
      bucket — 'monthly' (изменяет monthly_used инвертированно) или
        'topup' (изменяет topup_balance напрямую).
      note — обязательное пояснение в meta_json для аудита.
      admin_user_id — id админа, делающего коррекцию.

    НЕ делает commit.
    """
    if delta == 0:
        return None
    if bucket not in ("monthly", "topup"):
        log.error("billing.admin_adjust_bad_bucket bucket=%s", bucket)
        return None

    stmt = (
        select(UserTokenBalance)
        .where(UserTokenBalance.user_id == user_id)
        .with_for_update()
    )
    balance_row = (await db.execute(stmt)).scalar_one_or_none()
    if balance_row is None:
        return None

    if bucket == "monthly":
        # +delta даёт пользователю больше → уменьшает monthly_used
        new_used = int(balance_row.monthly_used) - delta
        balance_row.monthly_used = max(0, new_used)
    else:
        balance_row.topup_balance = int(balance_row.topup_balance) + delta

    db.add(TokenTransaction(
        user_id=user_id,
        delta=delta,
        reason=REASON_ADMIN_ADJUSTMENT,
        related_event_id=None,
        meta_json={
            "bucket": bucket,
            "note": note,
            "admin_user_id": admin_user_id,
        },
    ))

    return TokenBalance(
        monthly_granted=int(balance_row.monthly_granted),
        monthly_used=int(balance_row.monthly_used),
        topup_balance=int(balance_row.topup_balance),
    )
