"""
Подписки ↔ токенный баланс: пауза/возобновление подписок при
исчерпании/восстановлении токенов и разовое уведомление пользователя в боте.

Зачем: на free-тарифе (300 токенов) активные подписки за несколько дней
выедают баланс. Раньше раннер просто ставил status='no_tokens', но
оставлял подписку is_active=True (она крутилась вхолостую) и НЕ уведомлял
пользователя — в кабинете подписки выглядели активными (зелёный кружок),
а сообщения переставали приходить «молча».

Теперь:
  • при нехватке токенов подписки переводятся в is_active=False,
    status='no_tokens' — раннер их больше не дёргает;
  • пользователю один раз (за «эпизод» исчерпания) приходит сообщение в
    Telegram-бот: токены закончились, подписки приостановлены, дата
    следующего начисления, как возобновить (докупка/апгрейд);
  • в кабинете показывается баннер и серый кружок (через снапшот);
  • при пополнении баланса (monthly_grant / top-up / смена тарифа)
    подписки возобновляются, а флаг уведомления сбрасывается.

Все функции, кроме notify_low_balance_once(), работают на ПЕРЕДАННОЙ
сессии и НЕ коммитят — коммитит вызывающий код. notify_low_balance_once()
открывает СВОЮ сессию (и сама коммитит), потому что вызывается и из
веб-процесса (снапшот), и из джоб-раннера, и не должна мешать их
транзакциям.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

import billing
from db.session import AsyncSessionLocal
from db.models import BotUserLink, Plan, Subscription, SubscriptionState, User, UserTokenBalance
from bot_i18n import t as bot_t, months as bot_months, normalize_language

log = logging.getLogger(__name__)


# Tier, по которому оцениваем минимальную стоимость подписочного вызова.
# Подписки всегда идут на Flash Lite (light). Совпадает с раннером.
SUBSCRIPTION_TIER = "light"

# Статус подписки, приостановленной из-за нехватки токенов. Совпадает с
# тем, что ставит раннер. Отличает системную паузу от ручной ('paused').
STATUS_NO_TOKENS = "no_tokens"
STATUS_ACTIVE = "active"


def _next_monthly_grant_date(now_utc: datetime) -> date:
    """Первое число СЛЕДУЮЩЕГО месяца UTC — когда придёт месячный грант."""
    year = now_utc.year
    month = now_utc.month
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def _fmt_grant_date(d: date, language: str) -> str:
    """«1 июля 2026» (ru) / «1 July 2026» (en)."""
    months_list = bot_months(language)
    return f"{d.day} {months_list[d.month - 1]} {d.year}"


# ---------------------------------------------------------------------------
# Пауза / возобновление подписок
# ---------------------------------------------------------------------------


async def pause_user_subscriptions_low_balance(
    db: AsyncSession,
    *,
    user_id: int,
    now_utc: datetime,
) -> int:
    """
    Перевести все АКТИВНЫЕ подписки пользователя в системную паузу из-за
    нехватки токенов: is_active=False, status='no_tokens'. Раннер их больше
    не подхватывает (его reserve-запрос фильтрует is_active=True).

    Возвращает число затронутых подписок. НЕ коммитит.
    """
    stmt = (
        update(Subscription)
        .where(
            Subscription.owner_user_id == user_id,
            Subscription.is_active == True,  # noqa: E712
        )
        .values(is_active=False, status=STATUS_NO_TOKENS, last_error=None)
    )
    res = await db.execute(stmt)
    paused = int(getattr(res, "rowcount", 0) or 0)

    if paused:
        # Гасим расписание, чтобы не оставлять «висящих» next_run_at.
        sub_ids_subq = (
            select(Subscription.id)
            .where(
                Subscription.owner_user_id == user_id,
                Subscription.status == STATUS_NO_TOKENS,
            )
            .scalar_subquery()
        )
        await db.execute(
            update(SubscriptionState)
            .where(SubscriptionState.subscription_id.in_(sub_ids_subq))
            .values(next_run_at=None, last_checked_at=now_utc)
        )

    return paused


async def resume_user_subscriptions_low_balance(
    db: AsyncSession,
    *,
    user_id: int,
    now_utc: datetime,
) -> int:
    """
    Возобновить подписки, приостановленные СИСТЕМОЙ из-за нехватки токенов
    (status='no_tokens'). Ручные паузы ('paused') не трогаем.

    Пропускаем истёкшие триалы (is_trial && trial_ends_at <= now) — их
    возобновлять нельзя. Ставим is_active=True, status='active' и next_run_at
    = now, чтобы подписка отработала на ближайшем тике.

    Возвращает число возобновлённых. НЕ коммитит.
    """
    not_expired_trial = sa.or_(
        Subscription.is_trial == False,  # noqa: E712
        Subscription.trial_ends_at.is_(None),
        Subscription.trial_ends_at > now_utc,
    )

    # Сначала соберём id, которые реально возобновим (для обновления расписания).
    ids_res = await db.execute(
        select(Subscription.id).where(
            Subscription.owner_user_id == user_id,
            Subscription.status == STATUS_NO_TOKENS,
            not_expired_trial,
        )
    )
    resume_ids = [int(x) for x in ids_res.scalars().all()]
    if not resume_ids:
        return 0

    await db.execute(
        update(Subscription)
        .where(Subscription.id.in_(resume_ids))
        .values(is_active=True, status=STATUS_ACTIVE, last_error=None)
    )
    await db.execute(
        update(SubscriptionState)
        .where(SubscriptionState.subscription_id.in_(resume_ids))
        .values(next_run_at=now_utc)
    )
    return len(resume_ids)


async def count_no_token_subscriptions(db: AsyncSession, *, user_id: int) -> int:
    """Сколько у пользователя подписок в статусе 'no_tokens' (для баннера)."""
    stmt = (
        select(sa.func.count())
        .select_from(Subscription)
        .where(
            Subscription.owner_user_id == user_id,
            Subscription.status == STATUS_NO_TOKENS,
        )
    )
    return int((await db.execute(stmt)).scalar_one() or 0)


async def clear_low_balance_notified(db: AsyncSession, *, user_id: int) -> None:
    """Сбросить флаг разового уведомления (после пополнения). НЕ коммитит."""
    await db.execute(
        update(UserTokenBalance)
        .where(
            UserTokenBalance.user_id == user_id,
            UserTokenBalance.low_balance_notified_at.isnot(None),
        )
        .values(low_balance_notified_at=None)
    )


# ---------------------------------------------------------------------------
# Разовое уведомление в боте (своя сессия)
# ---------------------------------------------------------------------------


def _build_low_balance_message(*, language: str, topup_enabled: bool, now_utc: datetime) -> str:
    lang = normalize_language(language)
    title = bot_t("subs_paused_no_tokens_title", lang)
    grant_date = _fmt_grant_date(_next_monthly_grant_date(now_utc), lang)
    next_grant = bot_t("subs_paused_no_tokens_next_grant", lang, date=grant_date)
    tail = bot_t(
        "subs_paused_no_tokens_topup" if topup_enabled else "subs_paused_no_tokens_upgrade_only",
        lang,
    )
    return f"{title}\n{next_grant}\n\n{tail}"


async def notify_low_balance_once(*, user_id: int, now_utc: Optional[datetime] = None) -> bool:
    """
    Отправить разовое уведомление о приостановке подписок из-за нехватки
    токенов. Идемпотентно по флагу low_balance_notified_at: если уже
    уведомляли в текущем «эпизоде» — ничего не делаем.

    Открывает СВОЮ сессию и коммитит сам — безопасно вызывать из любого
    контекста (веб-снапшот / джоб-раннер). При сбое отправки сбрасывает
    флаг, чтобы следующий тик повторил попытку.

    Возвращает True, если сообщение реально отправлено.
    """
    now = now_utc or datetime.now(timezone.utc)

    async with AsyncSessionLocal() as db:
        # Claim: лочим строку баланса, проверяем флаг.
        bal = (
            await db.execute(
                select(UserTokenBalance)
                .where(UserTokenBalance.user_id == user_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if bal is None or bal.low_balance_notified_at is not None:
            return False

        dest_chat_id = (
            await db.execute(
                select(BotUserLink.telegram_chat_id)
                .where(
                    BotUserLink.owner_user_id == user_id,
                    BotUserLink.is_blocked == False,  # noqa: E712
                )
                .order_by(BotUserLink.id.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        row = (
            await db.execute(
                select(User.language, Plan.topup_enabled)
                .select_from(User)
                .join(Plan, Plan.code == User.plan)
                .where(User.id == user_id)
            )
        ).first()
        language = (row[0] if row else None) or "en"
        topup_enabled = bool(row[1]) if row else False

        # Помечаем claim ДО отправки (чтобы параллельные тики не задвоили).
        bal.low_balance_notified_at = now
        await db.commit()

    if not dest_chat_id:
        # Пользователь не подключил бота — отправить некуда. Флаг уже стоит,
        # повторно не пытаемся (всё равно слать некуда).
        log.info("subs.low_balance.no_bot_link user_id=%s", user_id)
        return False

    text = _build_low_balance_message(
        language=language, topup_enabled=topup_enabled, now_utc=now,
    )

    try:
        # Поздний импорт — bot_send_message живёт в main.py, который импортирует
        # этот модуль косвенно; импорт внутри функции исключает цикл на загрузке.
        from main import bot_send_message  # type: ignore
        await bot_send_message(chat_id=int(dest_chat_id), text=text)
        log.info("subs.low_balance.notified user_id=%s", user_id)
        return True
    except Exception:
        log.exception("subs.low_balance.notify_failed user_id=%s — сбрасываю флаг", user_id)
        # Сбрасываем флаг, чтобы следующий тик повторил попытку.
        try:
            async with AsyncSessionLocal() as db2:
                await db2.execute(
                    update(UserTokenBalance)
                    .where(UserTokenBalance.user_id == user_id)
                    .values(low_balance_notified_at=None)
                )
                await db2.commit()
        except Exception:
            log.exception("subs.low_balance.notify_flag_reset_failed user_id=%s", user_id)
        return False


# ---------------------------------------------------------------------------
# Reconcile — главная точка для снапшота профиля
# ---------------------------------------------------------------------------


async def reconcile_user_subscriptions_balance(
    db: AsyncSession,
    *,
    user_id: int,
    now_utc: Optional[datetime] = None,
) -> dict[str, Any]:
    """
    Привести состояние подписок пользователя в соответствие с балансом.

    Если токенов не хватает (по tier='light'):
      • переводим активные подписки в паузу (is_active=False, 'no_tokens');
      • если есть приостановленные подписки — отправляем разовое уведомление.
    Если токенов достаточно:
      • сбрасываем флаг уведомления;
      • возобновляем подписки, приостановленные системой.

    Изменения на ПЕРЕДАННОЙ сессии (пауза/возобновление/сброс флага) —
    коммитит вызывающий код. Уведомление шлётся через notify_low_balance_once
    (своя сессия).

    Возвращает dict с ключом 'subscriptions_paused_no_tokens' (bool) для
    баннера в UI + диагностикой.
    """
    now = now_utc or datetime.now(timezone.utc)

    can_spend, _balance = await billing.check_can_spend(
        db, user_id=user_id, tier=SUBSCRIPTION_TIER,
    )

    if not can_spend:
        await pause_user_subscriptions_low_balance(db, user_id=user_id, now_utc=now)
        paused_count = await count_no_token_subscriptions(db, user_id=user_id)
        notified = False
        if paused_count > 0:
            try:
                notified = await notify_low_balance_once(user_id=user_id, now_utc=now)
            except Exception:
                log.exception("subs.reconcile.notify_failed user_id=%s", user_id)
        return {
            "low_balance": True,
            "paused_count": paused_count,
            "subscriptions_paused_no_tokens": paused_count > 0,
            "notified": notified,
        }

    # Баланс в норме — сбрасываем флаг и возобновляем системные паузы.
    await clear_low_balance_notified(db, user_id=user_id)
    resumed = await resume_user_subscriptions_low_balance(db, user_id=user_id, now_utc=now)
    return {
        "low_balance": False,
        "resumed_count": resumed,
        "subscriptions_paused_no_tokens": False,
    }
