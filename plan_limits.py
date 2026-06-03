from __future__ import annotations

from datetime import datetime, timezone, timedelta, date
from typing import Any, Optional

import sqlalchemy as sa
from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Plan, UsageCounter, UsageEvent, Subscription, User, UserTokenBalance


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


# ---------------------------------------------------------------------------
# Период анализа: парсинг минут / часов / дней
# ---------------------------------------------------------------------------
# Минуты и часы не ограничены тарифом — это под-суточные окна и они всегда
# меньше любого тарифного потолка по дням. Дни проверяются отдельно через
# ensure_days_within_plan.
PERIOD_UNIT_SECONDS = {
    "minutes": 60,
    "hours": 3600,
    "days": 86400,
}
PERIOD_BOUNDS = {
    "minutes": {"min": 5, "max": 180},   # 5 мин – 3 часа
    "hours":   {"min": 1, "max": 72},    # 1 ч – 3 дня
    "days":    {"min": 1, "max": None},  # max — из тарифа
}


def parse_period_from_payload(payload: dict) -> tuple[int, str, int]:
    """
    Возвращает (period_value, period_unit, period_seconds).

    Принимает либо новый контракт {"period_value", "period_unit"}, либо
    legacy {"days": int}. Делает серверную валидацию границ для минут
    и часов (для дней проверка против тарифа делается отдельно после
    получения плана пользователя).

    Бросает HTTPException(400) на некорректный ввод.
    """
    if "period_value" in payload or "period_unit" in payload:
        try:
            value = int(payload.get("period_value") or 0)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail={"code": "PERIOD_VALUE_INVALID"})
        unit = str(payload.get("period_unit") or "days").lower().strip()
    else:
        # Legacy путь — старые клиенты шлют только {"days": int}.
        try:
            value = int(payload.get("days") or 7)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail={"code": "DAYS_INVALID"})
        unit = "days"

    if unit not in PERIOD_UNIT_SECONDS:
        raise HTTPException(status_code=400, detail={"code": "PERIOD_UNIT_INVALID"})

    if value <= 0:
        raise HTTPException(status_code=400, detail={"code": "PERIOD_VALUE_INVALID"})

    bounds = PERIOD_BOUNDS.get(unit, {})
    bmin = bounds.get("min")
    bmax = bounds.get("max")
    if bmin is not None and value < bmin:
        raise HTTPException(status_code=400, detail={
            "code": "PERIOD_OUT_OF_RANGE",
            "message": f"Минимум для {unit}: {bmin}",
            "unit": unit, "min": bmin, "max": bmax,
        })
    if bmax is not None and value > bmax:
        raise HTTPException(status_code=400, detail={
            "code": "PERIOD_OUT_OF_RANGE",
            "message": f"Максимум для {unit}: {bmax}",
            "unit": unit, "min": bmin, "max": bmax,
        })

    period_seconds = value * PERIOD_UNIT_SECONDS[unit]
    return value, unit, period_seconds


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

# Per-provider model slugs (kept here for the allowlist; canonical source
# of truth is llm/models.py SUPPORTED_MODELS).
_OPENAI_BALANCED = "openai:gpt-5.4-mini"
_OPENAI_PRO = "openai:gpt-4.1"
_OPENAI_O3 = "openai:o3"
_OPENAI_O4_MINI = "openai:o4-mini"
_ANTHROPIC_HAIKU = "anthropic:claude-haiku-4-5"
_ANTHROPIC_SONNET = "anthropic:claude-sonnet-4-6"
_GEMINI_LITE = "google:gemini-3.1-flash-lite"
_GEMINI_FLASH = "google:gemini-2.5-flash"
_GEMINI_PRO = "google:gemini-3.5-flash"
_GEMINI_PRO_25 = "google:gemini-2.5-pro"

# Backwards-compat aliases (other modules may import these names).
CLAUDE_AI_MODEL = _ANTHROPIC_SONNET
GEMINI_AI_MODEL = _GEMINI_FLASH
GEMINI_LITE_AI_MODEL = _GEMINI_LITE
GEMINI_PRO_AI_MODEL = _GEMINI_PRO


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
        # Paid plans see the full model catalog across 3 providers.
        # IMPORTANT: this allowlist must stay in sync with
        # llm/models.py SUPPORTED_MODELS and with the frontend's
        # normalizeAiModelUi allowed set. If a slug appears in the
        # frontend dropdown but is missing here, the backend will
        # SILENTLY fall back to the user's profile default (and from
        # there to gpt-4.1-mini) instead of returning an error — so
        # the user sees their selection in the UI but the request
        # actually runs on a different model. We hit that bug when
        # we added o3/o4-mini/gemini-2.5-pro to the frontend but
        # forgot this allowlist.
        allowed = {
            # OpenAI: light → balanced → deep (incl. reasoning models)
            DEFAULT_AI_MODEL,        # openai:gpt-4.1-mini
            _OPENAI_BALANCED,        # openai:gpt-5.4-mini
            _OPENAI_PRO,             # openai:gpt-4.1
            _OPENAI_O3,              # openai:o3
            _OPENAI_O4_MINI,         # openai:o4-mini
            # Anthropic: light → deep
            _ANTHROPIC_HAIKU,        # anthropic:claude-haiku-4-5
            _ANTHROPIC_SONNET,       # anthropic:claude-sonnet-4-6
            # Google: light → balanced → deep
            _GEMINI_LITE,            # google:gemini-3.1-flash-lite
            _GEMINI_FLASH,           # google:gemini-2.5-flash
            _GEMINI_PRO,             # google:gemini-3.5-flash
            _GEMINI_PRO_25,          # google:gemini-2.5-pro
        }

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
    slots_required: int = 1,
    skip_days_plan_check: bool = False,
) -> Plan:
    """
    Reject the request if it would exceed the user's daily / monthly QA
    quota. For a single-chat request slots_required=1 (default). For a
    group request slots_required=N where N is the number of chats — the
    user "spends" N qa_request slots in one click (this is intentional;
    we'll likely switch to credits later, see TZ on credit architecture).

    skip_days_plan_check=True — пропускает проверку плана по дням. Это
    нужно для запросов в единицах "минуты" / "часы": сами по себе они
    всегда меньше суток, и тарифный потолок по дням к ним не
    применяется (как договорились в продуктовом решении).

    Note: this is a CHECK, not a reservation — we do not increment the
    counter here. record_qa_success/failure does that. If two group
    requests fire concurrently they could in theory both pass the check.
    Acceptable for v1; a strict atomic reservation is a v2 concern.
    """
    plan = await get_user_plan(db, user)
    if not skip_days_plan_check:
        ensure_days_within_plan(requested_days=requested_days, plan=plan)

    slots_required = max(1, int(slots_required))

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

    if day_used + slots_required > int(plan.daily_qa_limit):
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
                "slots_required": slots_required,
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
                "slots_required": slots_required,
            },
        )

    if month_used + slots_required > int(plan.monthly_qa_limit):
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
                "slots_required": slots_required,
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
                "slots_required": slots_required,
            },
        )

    return plan


# ---------------------------------------------------------------------------
# Group analysis — per-plan chat-count limits
# ---------------------------------------------------------------------------
#
# Independent of qa_request limits. Lives here (not in Plan SQL columns)
# because (a) we want to ship this in v1 without a migration, and (b) we
# expect to revisit it under the credits architecture anyway. Add a real
# column when this stabilizes.
# ---------------------------------------------------------------------------

GROUP_CHATS_LIMIT_BY_PLAN: dict[str, int] = {
    "free": 1,
    "basic": 5,
    "pro": 10,
    "power": 20,
}
DEFAULT_GROUP_CHATS_LIMIT = 20  # any paid plan we haven't named explicitly


def resolve_group_chats_limit(plan_code: Optional[str]) -> int:
    """
    Return the maximum number of chats a user can include in a single
    group-analysis request, based on their plan code.

    DEPRECATED: используется только в админ-endpoint'ах и при отсутствии
    plan-объекта. Endpoint'ы Q&A после cutover'а используют
    `Plan.max_chats_per_group_request` напрямую через get_user_plan().
    Удалим в этапе 4 рефакторинга.
    """
    code = str(plan_code or "").strip().lower()
    return GROUP_CHATS_LIMIT_BY_PLAN.get(code, DEFAULT_GROUP_CHATS_LIMIT)


# ---------------------------------------------------------------------------
# Token-system gating: проверка глубины анализа и размера группового запроса
# ---------------------------------------------------------------------------
#
# Используются endpoint'ами (main.tg_analyze_chat / tg_analyze_chats_group)
# ПЕРЕД вызовом billing.check_can_spend и LLM. Бросают HTTPException с
# понятным JSON для фронта.
#
# Ловит ситуации:
#   - free-пользователь выбрал «balanced» / «deep»
#   - basic-пользователь шлёт групповой запрос на 10 чатов (лимит 5)
# ---------------------------------------------------------------------------


def check_tier_allowed_or_raise(plan: Plan, depth: str) -> None:
    """
    Проверка: разрешена ли выбранная глубина анализа на текущем тарифе.

    На free доступен только light, на остальных — все три.
    Источник истины: plan.allowed_tiers (ARRAY varchar). Заполняется в
    миграции; см. MIGRATION_NOTES_token_system.md.

    На несоответствии — HTTPException 403 с понятным сообщением и
    списком доступных tier'ов для фронта.
    """
    allowed = list(plan.allowed_tiers or ["light"])
    depth_normalized = str(depth or "").strip().lower()
    if depth_normalized not in allowed:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TIER_NOT_ALLOWED",
                "message": (
                    f"Глубина анализа «{depth_normalized}» не доступна на "
                    f"вашем тарифе. Доступно: {', '.join(allowed)}."
                ),
                "requested_tier": depth_normalized,
                "allowed_tiers": allowed,
            },
        )


def check_max_chats_or_raise(plan: Plan, num_chats: int) -> None:
    """
    Проверка: не превышает ли число чатов в запросе тарифный лимит.

    Используется в /tg/analyze_chats_group и потенциально в подписочной
    логике. Free=1 (только single-chat), Basic=5, Pro=10, Power=20.

    На превышении — HTTPException 400 с понятным сообщением.
    """
    plan_limit = int(plan.max_chats_per_group_request or 1)
    if num_chats > plan_limit:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GROUP_CHATS_LIMIT_EXCEEDED",
                "message": (
                    f"Ваш тариф разрешает не более {plan_limit} чатов "
                    f"в одном запросе."
                ),
                "max_chats_per_group_request": plan_limit,
                "requested_chats": num_chats,
            },
        )


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
    thinking_tokens: Optional[int],
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

    `thinking_tokens` are hidden reasoning tokens emitted by reasoning
    models (OpenAI GPT-5/o-series, Google Gemini reasoning tier). They
    are billed at the output rate but are NOT included in `output_tokens`
    (which counts only visible output). Stored separately so the admin
    observability panel can show them as their own column.
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
        "thinking_tokens": thinking_tokens,
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
    thinking_tokens: Optional[int] = None,
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
        thinking_tokens=thinking_tokens,
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
        thinking_tokens=None,
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

    # === Токенная система (новая) — snapshot из user_token_balances ===
    token_balance_row = (await db.execute(
        select(UserTokenBalance).where(UserTokenBalance.user_id == user.id)
    )).scalar_one_or_none()

    if token_balance_row is not None:
        token_monthly_granted = int(token_balance_row.monthly_granted)
        token_monthly_used = int(token_balance_row.monthly_used)
        token_topup_balance = int(token_balance_row.topup_balance)
    else:
        # Аномалия — баланса нет. Логируем выше по стеку через main.py
        # endpoint при first-failure. Здесь возвращаем плановые значения,
        # чтобы UI не показал «0 / 0» и не пугал пользователя.
        token_monthly_granted = int(plan.monthly_tokens or 0)
        token_monthly_used = 0
        token_topup_balance = 0

    token_monthly_remaining = max(0, token_monthly_granted - token_monthly_used)
    token_total_remaining = token_monthly_remaining + max(0, token_topup_balance)

    return {
        "plan": {
            "code": plan.code,
            "price_usd": float(plan.price_usd),

            # === Токенная система ===
            "monthly_tokens": int(plan.monthly_tokens or 0),
            "allowed_tiers": list(plan.allowed_tiers or []),
            "topup_enabled": bool(plan.topup_enabled),
            "max_chats_per_group_request": int(plan.max_chats_per_group_request or 1),

            # === DEPRECATED (старая система счётчиков, удалим в этапе 4) ===
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
            # === Токенный баланс (новое) — что показывать в карточке профиля ===
            "tokens": {
                "monthly_granted": token_monthly_granted,
                "monthly_used": token_monthly_used,
                "monthly_remaining": token_monthly_remaining,
                "topup_balance": token_topup_balance,
                "total_remaining": token_total_remaining,
            },

            # === DEPRECATED (старые счётчики qa-запросов) ===
            "daily_used": int(daily_used),
            "monthly_used": int(monthly_used),

            "active_subscriptions": int(active_subscriptions),
            "trial_subscriptions_total": int(trial_total),
            "free_trial_limit_reached": bool(free_trial_limit_reached),
            "free_trial_expired": bool(free_trial_expired),
        },
    }