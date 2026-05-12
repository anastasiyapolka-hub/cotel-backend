from __future__ import annotations

from datetime import datetime, timezone, timedelta, date
from typing import Any, Optional

import sqlalchemy as sa
from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Plan, UsageCounter, UsageEvent, Subscription, User


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def day_period_start(now_utc: datetime) -> date:
    return now_utc.date()


def month_period_start(now_utc: datetime) -> date:
    return date(now_utc.year, now_utc.month, 1)


async def get_user_plan(db: AsyncSession, user: User) -> Plan:
    res = await db.execute(
        select(Plan).where(Plan.code == user.plan)
    )
    plan = res.scalar_one_or_none()
    if not plan:
        raise HTTPException(status_code=500, detail="PLAN_NOT_FOUND")
    return plan


async def get_used_count(
    db: AsyncSession,
    *,
    user_id: int,
    metric_code: str,
    period_type: str,
    period_start: date,
) -> int:
    res = await db.execute(
        select(UsageCounter.used_count).where(
            UsageCounter.user_id == user_id,
            UsageCounter.metric_code == metric_code,
            UsageCounter.period_type == period_type,
            UsageCounter.period_start == period_start,
        )
    )
    value = res.scalar_one_or_none()
    return int(value or 0)


async def increment_usage_counter(
    db: AsyncSession,
    *,
    user_id: int,
    metric_code: str,
    period_type: str,
    period_start: date,
    amount: int = 1,
) -> None:
    stmt = (
        insert(UsageCounter)
        .values(
            user_id=user_id,
            metric_code=metric_code,
            period_type=period_type,
            period_start=period_start,
            used_count=amount,
            updated_at=sa.func.now(),
        )
        .on_conflict_do_update(
            constraint="uq_usage_counter_user_metric_period",
            set_={
                "used_count": UsageCounter.used_count + amount,
                "updated_at": sa.func.now(),
            },
        )
    )
    await db.execute(stmt)


async def add_usage_event(
    db: AsyncSession,
    *,
    user_id: int,
    event_type: str,
    status: str,
    source_mode: Optional[str] = None,
    chat_ref: Optional[str] = None,
    subscription_id: Optional[int] = None,
    meta_json: Optional[dict[str, Any]] = None,
) -> None:
    db.add(
        UsageEvent(
            user_id=user_id,
            event_type=event_type,
            status=status,
            source_mode=source_mode,
            chat_ref=chat_ref,
            subscription_id=subscription_id,
            meta_json=meta_json,
        )
    )


async def count_active_subscriptions(
    db: AsyncSession,
    *,
    user_id: int,
    exclude_subscription_id: Optional[int] = None,
) -> int:
    stmt = select(func.count()).select_from(Subscription).where(
        Subscription.owner_user_id == user_id,
        Subscription.is_active == True,  # noqa: E712
    )

    if exclude_subscription_id is not None:
        stmt = stmt.where(Subscription.id != exclude_subscription_id)

    value = (await db.execute(stmt)).scalar_one()
    return int(value or 0)


async def count_trial_subscriptions_total(
    db: AsyncSession,
    *,
    user_id: int,
) -> int:
    stmt = select(func.count()).select_from(Subscription).where(
        Subscription.owner_user_id == user_id,
        Subscription.is_trial == True,  # noqa: E712
    )
    value = (await db.execute(stmt)).scalar_one()
    return int(value or 0)


def ensure_days_within_plan(*, requested_days: int, plan: Plan) -> None:
    if int(requested_days) > int(plan.qa_history_days):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "PLAN_HISTORY_LIMIT_EXCEEDED",
                "message": f"Ваш тариф позволяет анализировать не более {int(plan.qa_history_days)} дней истории.",
                "plan_limit_days": int(plan.qa_history_days),
            },
        )


def ensure_frequency_within_plan(*, requested_frequency_minutes: int, plan: Plan) -> None:
    min_allowed = int(plan.min_subscription_interval_minutes)
    requested = int(requested_frequency_minutes)

    # Чем меньше минут, тем чаще запуск. Ниже минимума нельзя.
    if requested < min_allowed:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "PLAN_SUBSCRIPTION_FREQUENCY_TOO_HIGH",
                "message": f"Ваш тариф разрешает подписки не чаще 1 раза в {min_allowed} минут.",
                "min_subscription_interval_minutes": min_allowed,
            },
        )

DEFAULT_AI_MODEL = "openai:gpt-4.1-mini"
CLAUDE_AI_MODEL = "anthropic:claude-sonnet-4-6"

def resolve_ai_model_for_user(
    *,
    user: User,
    requested_ai_model: Optional[str] = None,
    fallback_ai_model: Optional[str] = None,
) -> str:
    plan_code = str(getattr(user, "plan", "") or "").strip().lower()

    requested = str(requested_ai_model or "").strip().lower()
    fallback = str(fallback_ai_model or "").strip().lower()

    if plan_code == "free":
        allowed = {DEFAULT_AI_MODEL}
    else:
        allowed = {DEFAULT_AI_MODEL, CLAUDE_AI_MODEL}


    if requested in allowed:
        return requested

    if fallback in allowed:
        return fallback

    return DEFAULT_AI_MODEL

async def expire_trial_subscription_if_needed(
    db: AsyncSession,
    *,
    sub: Subscription,
    now_utc: Optional[datetime] = None,
) -> bool:
    now_utc = now_utc or utc_now()

    if not getattr(sub, "is_trial", False):
        return False

    owner_user_id = getattr(sub, "owner_user_id", None)
    if not owner_user_id:
        return False

    user_res = await db.execute(
        select(User).where(User.id == owner_user_id)
    )
    owner_user = user_res.scalar_one_or_none()
    if not owner_user:
        return False

    # Trial-логика действует только пока пользователь на Free
    if str(owner_user.plan or "").lower() != "free":
        return False

    trial_ends_at = getattr(sub, "trial_ends_at", None)
    if not trial_ends_at:
        return False

    if trial_ends_at <= now_utc:
        sub.is_active = False
        sub.status = "trial_expired"
        sub.last_error = None
        sub.updated_at = sa.func.now()
        return True

    return False

async def enforce_qa_limits(
    db: AsyncSession,
    *,
    user: User,
    requested_days: int,
    source_mode: Optional[str],
    chat_ref: Optional[str],
) -> Plan:
    plan = await get_user_plan(db, user)
    ensure_days_within_plan(requested_days=requested_days, plan=plan)

    now_utc = utc_now()
    day_used = await get_used_count(
        db,
        user_id=user.id,
        metric_code="qa_request",
        period_type="day",
        period_start=day_period_start(now_utc),
    )
    month_used = await get_used_count(
        db,
        user_id=user.id,
        metric_code="qa_request",
        period_type="month",
        period_start=month_period_start(now_utc),
    )

    if day_used >= int(plan.daily_qa_limit):
        await add_usage_event(
            db,
            user_id=user.id,
            event_type="qa_request_rejected",
            status="limit_rejected",
            source_mode=source_mode,
            chat_ref=chat_ref,
            meta_json={
                "reason": "daily_limit",
                "daily_limit": int(plan.daily_qa_limit),
                "daily_used": int(day_used),
            },
        )
        await db.commit()
        raise HTTPException(
            status_code=429,
            detail={
                "code": "PLAN_DAILY_QA_LIMIT_REACHED",
                "message": "Дневной лимит запросов исчерпан.",
                "daily_limit": int(plan.daily_qa_limit),
                "daily_used": int(day_used),
            },
        )

    if month_used >= int(plan.monthly_qa_limit):
        await add_usage_event(
            db,
            user_id=user.id,
            event_type="qa_request_rejected",
            status="limit_rejected",
            source_mode=source_mode,
            chat_ref=chat_ref,
            meta_json={
                "reason": "monthly_limit",
                "monthly_limit": int(plan.monthly_qa_limit),
                "monthly_used": int(month_used),
            },
        )
        await db.commit()
        raise HTTPException(
            status_code=429,
            detail={
                "code": "PLAN_MONTHLY_QA_LIMIT_REACHED",
                "message": "Месячный лимит запросов исчерпан.",
                "monthly_limit": int(plan.monthly_qa_limit),
                "monthly_used": int(month_used),
            },
        )

    return plan


# ---------------------------------------------------------------------------
# Q&A usage logging
# ---------------------------------------------------------------------------
#
# `tokens_source` documents how token counts were obtained:
#   - "api_usage":       provider returned a real usage object
#   - "estimated_chars": values were estimated from character counts
#   - "empty":           no LLM call was made (empty context short-circuit)
# Mirror of the constants in llm.usage; duplicated here as strings so
# plan_limits has no hard dependency on the llm package shape.
# ---------------------------------------------------------------------------

def _build_qa_meta(
    *,
    requested_days: Optional[int],
    ai_model: Optional[str],
    query_chars: Optional[int],
    messages_fetched_count: Optional[int],
    messages_sent_to_llm_count: Optional[int],
    context_chars: Optional[int],
    answer_chars: Optional[int],
    input_tokens: Optional[int],
    output_tokens: Optional[int],
    total_tokens: Optional[int],
    estimated_input_tokens: Optional[int],
    estimated_output_tokens: Optional[int],
    estimated_total_tokens: Optional[int],
    estimated_cost_usd: Optional[float],
    cost_calculation_method: Optional[str],
    input_price_per_1m_usd_snapshot: Optional[float],
    output_price_per_1m_usd_snapshot: Optional[float],
    duration_ms_total: Optional[int],
    duration_ms_fetch: Optional[int],
    duration_ms_llm: Optional[int],
    tokens_source: Optional[str],
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> dict[str, Any]:
    """
    Build a Q&A meta_json payload, dropping keys whose value is None
    so we don't store noise. Privacy guardrail: we do NOT accept the
    user's query text, chat content, or LLM answer here. Counters only.
    """
    raw: dict[str, Any] = {
        "days": int(requested_days) if requested_days is not None else None,
        "ai_model": ai_model,
        "query_chars": query_chars,
        "messages_fetched_count": messages_fetched_count,
        "messages_sent_to_llm_count": messages_sent_to_llm_count,
        "context_chars": context_chars,
        "answer_chars": answer_chars,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "estimated_input_tokens": estimated_input_tokens,
        "estimated_output_tokens": estimated_output_tokens,
        "estimated_total_tokens": estimated_total_tokens,
        "estimated_cost_usd": estimated_cost_usd,
        "cost_calculation_method": cost_calculation_method,
        "input_price_per_1m_usd_snapshot": input_price_per_1m_usd_snapshot,
        "output_price_per_1m_usd_snapshot": output_price_per_1m_usd_snapshot,
        "duration_ms_total": duration_ms_total,
        "duration_ms_fetch": duration_ms_fetch,
        "duration_ms_llm": duration_ms_llm,
        "tokens_source": tokens_source,
        "error_code": error_code,
        "error_message": error_message,
    }
    return {k: v for k, v in raw.items() if v is not None}


async def record_qa_success(
    db: AsyncSession,
    *,
    user: User,
    source_mode: Optional[str],
    chat_ref: Optional[str],
    requested_days: int,
    ai_model: Optional[str] = None,
    query_chars: Optional[int] = None,
    messages_fetched_count: Optional[int] = None,
    messages_sent_to_llm_count: Optional[int] = None,
    context_chars: Optional[int] = None,
    answer_chars: Optional[int] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    total_tokens: Optional[int] = None,
    estimated_input_tokens: Optional[int] = None,
    estimated_output_tokens: Optional[int] = None,
    estimated_total_tokens: Optional[int] = None,
    estimated_cost_usd: Optional[float] = None,
    cost_calculation_method: Optional[str] = None,
    input_price_per_1m_usd_snapshot: Optional[float] = None,
    output_price_per_1m_usd_snapshot: Optional[float] = None,
    duration_ms_total: Optional[int] = None,
    duration_ms_fetch: Optional[int] = None,
    duration_ms_llm: Optional[int] = None,
    tokens_source: Optional[str] = None,
) -> None:
    """
    Record a successful Q&A request.

    Side effects:
      1) Increment daily + monthly UsageCounter (qa_request metric).
      2) Append a UsageEvent(event_type='qa_request_success',
         status='success_counted') with rich meta_json.

    All measurement params are optional — if a caller hasn't been
    updated yet to pass them, the function still does its core work
    (counter + event). meta_json keys with None values are dropped.

    Privacy guarantee: this function never accepts or stores the
    user's query text, chat content, or the LLM answer. Only counts,
    durations, tokens, model name, and cost.
    """
    now_utc = utc_now()

    await increment_usage_counter(
        db,
        user_id=user.id,
        metric_code="qa_request",
        period_type="day",
        period_start=day_period_start(now_utc),
        amount=1,
    )
    await increment_usage_counter(
        db,
        user_id=user.id,
        metric_code="qa_request",
        period_type="month",
        period_start=month_period_start(now_utc),
        amount=1,
    )

    meta = _build_qa_meta(
        requested_days=requested_days,
        ai_model=ai_model,
        query_chars=query_chars,
        messages_fetched_count=messages_fetched_count,
        messages_sent_to_llm_count=messages_sent_to_llm_count,
        context_chars=context_chars,
        answer_chars=answer_chars,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        estimated_input_tokens=estimated_input_tokens,
        estimated_output_tokens=estimated_output_tokens,
        estimated_total_tokens=estimated_total_tokens,
        estimated_cost_usd=estimated_cost_usd,
        cost_calculation_method=cost_calculation_method,
        input_price_per_1m_usd_snapshot=input_price_per_1m_usd_snapshot,
        output_price_per_1m_usd_snapshot=output_price_per_1m_usd_snapshot,
        duration_ms_total=duration_ms_total,
        duration_ms_fetch=duration_ms_fetch,
        duration_ms_llm=duration_ms_llm,
        tokens_source=tokens_source,
    )

    await add_usage_event(
        db,
        user_id=user.id,
        event_type="qa_request_success",
        status="success_counted",
        source_mode=source_mode,
        chat_ref=chat_ref,
        meta_json=meta,
    )


async def record_qa_failure(
    db: AsyncSession,
    *,
    user: User,
    source_mode: Optional[str],
    chat_ref: Optional[str],
    requested_days: Optional[int],
    error_code: str,
    ai_model: Optional[str] = None,
    error_message: Optional[str] = None,
    query_chars: Optional[int] = None,
    messages_fetched_count: Optional[int] = None,
    context_chars: Optional[int] = None,
    duration_ms_total: Optional[int] = None,
    duration_ms_fetch: Optional[int] = None,
    duration_ms_llm: Optional[int] = None,
) -> None:
    """
    Record a failed Q&A request.

    IMPORTANT: this function intentionally does NOT increment
    UsageCounter — failed requests must not eat into the user's daily
    or monthly quota. See TZ section 4.3.

    Writes a UsageEvent(event_type='qa_request_failed',
    status='failed_not_counted') with the available measurements and
    the error_code so the admin tab can show what went wrong without
    leaking the user's query or chat content.
    """
    meta = _build_qa_meta(
        requested_days=requested_days,
        ai_model=ai_model,
        query_chars=query_chars,
        messages_fetched_count=messages_fetched_count,
        messages_sent_to_llm_count=None,
        context_chars=context_chars,
        answer_chars=None,
        input_tokens=None,
        output_tokens=None,
        total_tokens=None,
        estimated_input_tokens=None,
        estimated_output_tokens=None,
        estimated_total_tokens=None,
        estimated_cost_usd=None,
        cost_calculation_method=None,
        input_price_per_1m_usd_snapshot=None,
        output_price_per_1m_usd_snapshot=None,
        duration_ms_total=duration_ms_total,
        duration_ms_fetch=duration_ms_fetch,
        duration_ms_llm=duration_ms_llm,
        tokens_source=None,
        error_code=error_code,
        error_message=error_message,
    )

    await add_usage_event(
        db,
        user_id=user.id,
        event_type="qa_request_failed",
        status="failed_not_counted",
        source_mode=source_mode,
        chat_ref=chat_ref,
        meta_json=meta,
    )


async def ensure_can_create_subscription(
    db: AsyncSession,
    *,
    user: User,
    frequency_minutes: int,
    requested_is_active: bool,
) -> tuple[Plan, bool, Optional[datetime], Optional[datetime]]:
    plan = await get_user_plan(db, user)
    ensure_frequency_within_plan(
        requested_frequency_minutes=frequency_minutes,
        plan=plan,
    )

    now_utc = utc_now()

    is_trial = False
    trial_started_at = None
    trial_ends_at = None

    if user.plan == "free":
        existing_trial_total = await count_trial_subscriptions_total(db, user_id=user.id)
        if existing_trial_total >= int(plan.trial_subscription_limit):
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "FREE_TRIAL_SUBSCRIPTIONS_ALREADY_USED",
                    "message": "Пробный доступ к подпискам уже использован. Перейдите на платный тариф.",
                },
            )

        is_trial = True
        trial_started_at = now_utc
        trial_ends_at = now_utc + timedelta(days=int(plan.trial_subscription_duration_days))

    if requested_is_active:
        active_now = await count_active_subscriptions(db, user_id=user.id)
        if active_now >= int(plan.max_active_subscriptions):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PLAN_ACTIVE_SUBSCRIPTIONS_LIMIT_REACHED",
                    "message": "Достигнут лимит активных подписок по тарифу.",
                    "max_active_subscriptions": int(plan.max_active_subscriptions),
                    "active_subscriptions": int(active_now),
                },
            )

    return plan, is_trial, trial_started_at, trial_ends_at


async def ensure_can_update_subscription(
    db: AsyncSession,
    *,
    user: User,
    sub: Subscription,
    requested_frequency_minutes: int,
    requested_is_active: bool,
) -> Plan:
    plan = await get_user_plan(db, user)
    ensure_frequency_within_plan(
        requested_frequency_minutes=requested_frequency_minutes,
        plan=plan,
    )

    expired = await expire_trial_subscription_if_needed(db, sub=sub)
    if expired and requested_is_active:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TRIAL_SUBSCRIPTION_EXPIRED",
                "message": "Срок trial-подписки истёк. Перейдите на платный тариф.",
            },
        )

    if requested_is_active:
        active_now = await count_active_subscriptions(
            db,
            user_id=user.id,
            exclude_subscription_id=sub.id,
        )
        if active_now >= int(plan.max_active_subscriptions):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PLAN_ACTIVE_SUBSCRIPTIONS_LIMIT_REACHED",
                    "message": "Достигнут лимит активных подписок по тарифу.",
                    "max_active_subscriptions": int(plan.max_active_subscriptions),
                    "active_subscriptions": int(active_now),
                },
            )

    return plan


async def ensure_can_toggle_subscription(
    db: AsyncSession,
    *,
    user: User,
    sub: Subscription,
    target_is_active: bool,
) -> Plan:
    plan = await get_user_plan(db, user)

    expired = await expire_trial_subscription_if_needed(db, sub=sub)
    if expired and target_is_active:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TRIAL_SUBSCRIPTION_EXPIRED",
                "message": "Срок trial-подписки истёк. Перейдите на платный тариф.",
            },
        )

    if target_is_active:
        ensure_frequency_within_plan(
            requested_frequency_minutes=int(sub.frequency_minutes),
            plan=plan,
        )

        active_now = await count_active_subscriptions(
            db,
            user_id=user.id,
            exclude_subscription_id=sub.id,
        )
        if active_now >= int(plan.max_active_subscriptions):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "PLAN_ACTIVE_SUBSCRIPTIONS_LIMIT_REACHED",
                    "message": "Достигнут лимит активных подписок по тарифу.",
                    "max_active_subscriptions": int(plan.max_active_subscriptions),
                    "active_subscriptions": int(active_now),
                },
            )

    return plan


def ensure_can_delete_subscription(*, user: User, sub: Subscription) -> None:
    if str(getattr(user, "plan", "")).lower() == "free":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "FREE_SUBSCRIPTION_DELETE_FORBIDDEN",
                "message": "На бесплатном тарифе удаление подписок недоступно. Вы можете только приостановить или возобновить подписку.",
            },
        )

    if getattr(sub, "is_trial", False):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TRIAL_SUBSCRIPTION_DELETE_FORBIDDEN",
                "message": "Trial-подписки нельзя удалять вручную.",
            },
        )

async def build_usage_snapshot(
    db: AsyncSession,
    *,
    user: User,
) -> dict[str, Any]:
    plan = await get_user_plan(db, user)
    now_utc = utc_now()

    changed = False

    trial_res = await db.execute(
        select(Subscription).where(
            Subscription.owner_user_id == user.id,
            Subscription.is_trial == True,  # noqa: E712
        )
    )
    trial_subs = list(trial_res.scalars().all())

    for sub in trial_subs:
        expired = await expire_trial_subscription_if_needed(db, sub=sub, now_utc=now_utc)
        if expired:
            changed = True

    if changed:
        await db.commit()

    daily_used = await get_used_count(
        db,
        user_id=user.id,
        metric_code="qa_request",
        period_type="day",
        period_start=day_period_start(now_utc),
    )
    monthly_used = await get_used_count(
        db,
        user_id=user.id,
        metric_code="qa_request",
        period_type="month",
        period_start=month_period_start(now_utc),
    )
    active_subscriptions = await count_active_subscriptions(db, user_id=user.id)
    trial_total = await count_trial_subscriptions_total(db, user_id=user.id)

    live_trial_res = await db.execute(
        select(func.count()).select_from(Subscription).where(
            Subscription.owner_user_id == user.id,
            Subscription.is_trial == True,  # noqa: E712
            sa.or_(
                Subscription.trial_ends_at.is_(None),
                Subscription.trial_ends_at > now_utc,
            ),
        )
    )
    live_trial_count = int(live_trial_res.scalar_one() or 0)

    free_trial_limit_reached = (
            str(plan.code or "").lower() == "free"
            and int(plan.trial_subscription_limit or 0) > 0
            and int(trial_total) >= int(plan.trial_subscription_limit)
    )

    free_trial_expired = free_trial_limit_reached and live_trial_count == 0

    return {
        "plan": {
            "code": plan.code,
            "price_usd": float(plan.price_usd),
            "daily_qa_limit": int(plan.daily_qa_limit),
            "monthly_qa_limit": int(plan.monthly_qa_limit),
            "qa_history_days": int(plan.qa_history_days),
            "max_active_subscriptions": int(plan.max_active_subscriptions),
            "min_subscription_interval_minutes": int(plan.min_subscription_interval_minutes),
            "trial_subscription_limit": int(plan.trial_subscription_limit),
            "trial_subscription_duration_days": int(plan.trial_subscription_duration_days),
            "has_chat_history": bool(plan.has_chat_history),
        },
        "usage": {
            "daily_used": int(daily_used),
            "monthly_used": int(monthly_used),
            "active_subscriptions": int(active_subscriptions),
            "trial_subscriptions_total": int(trial_total),
            "free_trial_limit_reached": bool(free_trial_limit_reached),
            "free_trial_expired": bool(free_trial_expired),
        },
    }