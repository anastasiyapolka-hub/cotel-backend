# jobs/monthly_grant_runner.py
"""
Ежемесячный grant токенов всем активным пользователям.

Запускается через Render cron на 1-е число каждого месяца UTC (или чуть
позже, чтобы наверняка попасть в новый месяц во всех таймзонах). Должен
быть **идемпотентен** — если запустится повторно в тот же месяц,
ничего не должен сломать (благодаря period_start-проверке).

Логика на одного пользователя:
  1. Найти его план (plan.monthly_tokens)
  2. Сравнить user_token_balances.period_start с началом текущего месяца
  3. Если уже грантован в этом месяце — skip
  4. Иначе: billing.grant_monthly(period_start = первое число текущего месяца)

billing.grant_monthly сам:
  - обнуляет monthly_used
  - переписывает monthly_granted из переданного plan_monthly_tokens
  - НЕ трогает topup_balance (top-up токены накапливаются)
  - записывает в token_transactions запись 'monthly_grant'

Запуск (Render cron):
  python -m jobs.monthly_grant_runner

Cron expression: `5 0 1 * *` — в 00:05 UTC 1-го числа каждого месяца.
5 минут запаса от полуночи UTC, чтобы избежать race-condition с какими-то
другими тиками. Render cron на минимум cron 1 раз в день — это норм.
"""
import asyncio
import sys
import traceback
from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from db.session import AsyncSessionLocal
from db.models import Plan, User, UserTokenBalance

import billing
import subscription_billing

BATCH_SIZE = 100  # сколько пользователей обрабатывать за одну транзакцию


def _month_start_utc(now_utc: Optional[datetime] = None) -> date:
    """Первое число текущего месяца UTC."""
    now = now_utc or datetime.now(timezone.utc)
    return date(now.year, now.month, 1)


async def _process_one_user(
    db,
    *,
    user_id: int,
    plan_monthly_tokens: int,
    period_start: date,
) -> str:
    """
    Сгрантовать токены одному пользователю если ещё не грантовано в
    этом периоде. Возвращает строку-статус для логов:
      'granted'   — успешно начислено
      'skipped'   — этот период уже грантован
      'no_balance'— нет записи в user_token_balances (создаст grant_monthly)
    """
    balance_row = (await db.execute(
        select(UserTokenBalance).where(UserTokenBalance.user_id == user_id)
    )).scalar_one_or_none()

    if balance_row is not None and balance_row.period_start == period_start:
        return "skipped"

    status = "granted" if balance_row is not None else "no_balance_then_granted"
    await billing.grant_monthly(
        db,
        user_id=user_id,
        plan_monthly_tokens=plan_monthly_tokens,
        period_start=period_start,
    )

    # Баланс пополнен → возобновляем подписки, приостановленные из-за
    # нехватки токенов, и сбрасываем флаг разового уведомления, чтобы при
    # следующем исчерпании пользователь снова получил пуш.
    now_utc = datetime.now(timezone.utc)
    await subscription_billing.clear_low_balance_notified(db, user_id=user_id)
    await subscription_billing.resume_user_subscriptions_low_balance(
        db, user_id=user_id, now_utc=now_utc,
    )
    return status


async def run_tick() -> int:
    """
    Главный entry-point. Возврат: 0 если всё ОК, 1 если были per-user
    ошибки (но процесс продолжился), 2 если фатальный сбой.

    Гарантия: одна неудача на одного пользователя НЕ блокирует остальных.
    """
    now_utc = datetime.now(timezone.utc)
    period_start = _month_start_utc(now_utc)
    exit_code = 0

    granted = 0
    skipped = 0
    failed = 0

    print(f"[monthly_grant_runner] START period_start={period_start} now_utc={now_utc.isoformat()}")

    # Кэш planов по коду — чтобы не делать N+1 запросов
    async with AsyncSessionLocal() as db:
        try:
            plans = (await db.execute(select(Plan))).scalars().all()
        except SQLAlchemyError as e:
            print(f"[monthly_grant_runner] FATAL plans_query_failed err={e}")
            return 2
    plan_tokens_by_code: dict[str, int] = {
        p.code: int(p.monthly_tokens or 0) for p in plans
    }

    if not plan_tokens_by_code:
        print("[monthly_grant_runner] WARN no_plans_in_db")
        return 2

    # Берём пользователей батчами; для каждого батча — одна транзакция.
    offset = 0
    while True:
        async with AsyncSessionLocal() as db:
            users = (await db.execute(
                select(User.id, User.plan)
                .where(User.is_active == True)  # noqa: E712
                .order_by(User.id.asc())
                .offset(offset)
                .limit(BATCH_SIZE)
            )).all()

            if not users:
                break

            for user_id, plan_code in users:
                code = str(plan_code or "").strip().lower()
                plan_monthly_tokens = plan_tokens_by_code.get(code)
                if plan_monthly_tokens is None:
                    print(
                        f"[monthly_grant_runner] SKIP user_id={user_id} reason=UNKNOWN_PLAN "
                        f"code={code!r}"
                    )
                    failed += 1
                    continue

                try:
                    status = await _process_one_user(
                        db,
                        user_id=int(user_id),
                        plan_monthly_tokens=plan_monthly_tokens,
                        period_start=period_start,
                    )
                    if status == "skipped":
                        skipped += 1
                    else:
                        granted += 1
                except Exception as e:
                    print(
                        f"[monthly_grant_runner] FAIL user_id={user_id} err={type(e).__name__}: {e}"
                    )
                    traceback.print_exc()
                    failed += 1
                    exit_code = 1

            # Коммитим весь батч одной транзакцией. Если в батче что-то
            # упадёт — try/except выше пропустит конкретного пользователя
            # но залогирует. Сам коммит здесь должен пройти.
            try:
                await db.commit()
            except SQLAlchemyError as e:
                print(f"[monthly_grant_runner] BATCH_COMMIT_FAILED offset={offset} err={e}")
                exit_code = 1
                # Не пытаемся rollback'ить (контекст AsyncSessionLocal сам
                # должен закрыть сессию). Идём в следующий батч.

        offset += BATCH_SIZE

    print(
        f"[monthly_grant_runner] DONE granted={granted} skipped={skipped} "
        f"failed={failed} exit_code={exit_code}"
    )
    return exit_code


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
