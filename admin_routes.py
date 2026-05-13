"""
Admin backend for CoTel — MVP version.

Five endpoints, all read-only on this stage:
  GET /admin/users
  GET /admin/users/{user_id}
  GET /admin/usage-events
  GET /admin/subscriptions
  GET /admin/sessions

Authorization: simple ADMIN_EMAILS env list. The list is comma-separated,
e.g. ADMIN_EMAILS=anastasiya.polka@gmail.com,other.admin@example.com .
Non-admin users get HTTP 403 with {"code": "ADMIN_FORBIDDEN"}.

Privacy guardrails enforced in this file:
  - password_hash NEVER returned
  - telegram session_ciphertext NEVER returned
  - subscription.prompt NEVER returned
  - DigestEvent.digest_text NEVER returned
  - UserChatHistory chat content NEVER returned
  - User.query/answer text NEVER returned (already absent from UsageEvent)
  - phone: masked (first 3 + last 2)
  - IP: masked (first octet + xxx.xxx.xxx, like TZ §15.1)
  - chat_ref for source_mode="personal": SHA-256 hash prefix
  - bot link telegram_chat_id: masked first 3 / last 3 digits
  - user_agent: shortened to 64 chars

The Pricing tab (vkladka) is deferred to Stage 8 — not in this file yet.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, date, timezone
from typing import Any, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user as auth_get_current_user
from db.models import (
    User, Subscription, SubscriptionState,
    MatchEvent, DigestEvent,
    BotUserLink, Session as WebSession, TelegramSession,
    UsageCounter, UsageEvent, Plan,
)
from db.session import get_db


router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Admin auth
# ---------------------------------------------------------------------------

def _parse_admin_emails() -> set[str]:
    """Read ADMIN_EMAILS env var (comma-separated) and normalize to lowercase."""
    raw = os.getenv("ADMIN_EMAILS") or ""
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


async def require_admin(
    user: User = Depends(auth_get_current_user),
) -> User:
    """FastAPI dependency. Allow only users whose email is in ADMIN_EMAILS."""
    admin_emails = _parse_admin_emails()
    user_email = (getattr(user, "email", None) or "").strip().lower()
    if not user_email or user_email not in admin_emails:
        raise HTTPException(
            status_code=403,
            detail={"code": "ADMIN_FORBIDDEN", "message": "Admin access required."},
        )
    return user


def _email_is_admin(email: Optional[str]) -> bool:
    if not email:
        return False
    return email.strip().lower() in _parse_admin_emails()


@router.get("/whoami")
async def admin_whoami(
    user: User = Depends(auth_get_current_user),
) -> dict[str, Any]:
    """Non-throwing admin check for UI gating.

    Returns {"is_admin": bool, "email": str|None} so the frontend can show
    or hide the admin entry button without producing a 403. Authenticated
    user is required; unauthenticated requests are rejected upstream by
    auth_get_current_user.
    """
    email = getattr(user, "email", None)
    return {
        "is_admin": _email_is_admin(email),
        "email": email,
    }


# ---------------------------------------------------------------------------
# Privacy / masking helpers
# ---------------------------------------------------------------------------

def _hash_chat_ref(chat_ref: Optional[str]) -> Optional[str]:
    """SHA-256(chat_ref) first 16 hex chars, prefixed with 'hash:' so the UI
    obviously shows it's an opaque identifier, not the original. Same chat_ref
    always hashes to the same value, which makes it useful for grouping in
    admin without leaking the real value."""
    if not chat_ref:
        return None
    digest = hashlib.sha256(str(chat_ref).encode("utf-8")).hexdigest()[:16]
    return f"hash:{digest}"


def _mask_chat_ref_for_source(chat_ref: Optional[str], source_mode: Optional[str]) -> Optional[str]:
    """For personal subscriptions: hash. For service/public: return as-is
    (these are public chats by design)."""
    if not chat_ref:
        return None
    mode = (source_mode or "").lower()
    if mode == "personal":
        return _hash_chat_ref(chat_ref)
    return chat_ref


def _mask_phone(phone: Optional[str]) -> Optional[str]:
    """+1234567890 -> +12***90. Returns None for empty input."""
    if not phone:
        return None
    s = str(phone)
    if len(s) <= 5:
        return "***"
    return f"{s[:3]}***{s[-2:]}"


def _mask_ip(ip: Optional[str]) -> Optional[str]:
    """Show first octet only, mask the rest. IPv6-aware in a crude way."""
    if not ip:
        return None
    s = str(ip)
    # IPv4 like 178.12.34.56 -> 178.xxx.xxx.xxx
    if "." in s and s.count(".") >= 3:
        head = s.split(".")[0]
        return f"{head}.xxx.xxx.xxx"
    # IPv6 — keep first group, mask the rest
    if ":" in s:
        head = s.split(":")[0] or "::"
        return f"{head}:xxxx::"
    return "xxx"


def _mask_chat_id(chat_id: Optional[int]) -> Optional[str]:
    """Telegram chat_id is a long integer. Show first 3 / last 3 digits."""
    if chat_id is None:
        return None
    s = str(int(chat_id))
    if len(s) <= 7:
        return "***"
    return f"{s[:3]}***{s[-3:]}"


def _short_ua(ua: Optional[str], limit: int = 64) -> Optional[str]:
    if not ua:
        return None
    s = str(ua)
    return s if len(s) <= limit else s[:limit].rstrip() + "…"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


# ---------------------------------------------------------------------------
# Date / period helpers (mirrors plan_limits.py)
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _day_start(now: datetime) -> date:
    return now.date()


def _month_start(now: datetime) -> date:
    return date(now.year, now.month, 1)


# ---------------------------------------------------------------------------
# Aggregate query helpers
# ---------------------------------------------------------------------------

async def _get_used_counter(
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
    return int(res.scalar_one_or_none() or 0)


async def _count_usage_events(
    db: AsyncSession,
    *,
    user_id: int,
    event_type: str,
) -> int:
    res = await db.execute(
        select(func.count()).select_from(UsageEvent).where(
            UsageEvent.user_id == user_id,
            UsageEvent.event_type == event_type,
        )
    )
    return int(res.scalar_one() or 0)


async def _last_usage_event_at(
    db: AsyncSession,
    *,
    user_id: int,
    event_type_like: str,
) -> Optional[datetime]:
    res = await db.execute(
        select(func.max(UsageEvent.created_at)).where(
            UsageEvent.user_id == user_id,
            UsageEvent.event_type.like(event_type_like),
        )
    )
    return res.scalar_one_or_none()


async def _sum_estimated_cost(
    db: AsyncSession,
    *,
    user_id: int,
    since: Optional[datetime] = None,
) -> Optional[float]:
    """SUM of meta_json->>'estimated_cost_usd' cast to float. Returns None if no rows
    have the cost field at all (to distinguish 'no data' from $0)."""
    # JSONB ->> returns text; cast to numeric. NULL-safe SUM.
    expr = sa.cast(
        UsageEvent.meta_json["estimated_cost_usd"].astext,
        sa.Numeric(14, 6),
    )
    stmt = select(func.sum(expr)).where(
        UsageEvent.user_id == user_id,
        UsageEvent.meta_json["estimated_cost_usd"].astext.isnot(None),
    )
    if since is not None:
        stmt = stmt.where(UsageEvent.created_at >= since)
    res = await db.execute(stmt)
    val = res.scalar_one_or_none()
    return float(val) if val is not None else None


async def _count_active_web_sessions(db: AsyncSession, *, user_id: int) -> int:
    now = _utc_now()
    res = await db.execute(
        select(func.count()).select_from(WebSession).where(
            WebSession.user_id == user_id,
            WebSession.revoked_at.is_(None),
            WebSession.expires_at > now,
        )
    )
    return int(res.scalar_one() or 0)


async def _telegram_session_is_active(db: AsyncSession, *, user_id: int) -> bool:
    res = await db.execute(
        select(func.count()).select_from(TelegramSession).where(
            TelegramSession.owner_user_id == user_id,
            TelegramSession.is_active == True,  # noqa: E712
            TelegramSession.revoked_at.is_(None),
        )
    )
    return int(res.scalar_one() or 0) > 0


async def _bot_is_linked(db: AsyncSession, *, user_id: int) -> bool:
    res = await db.execute(
        select(func.count()).select_from(BotUserLink).where(
            BotUserLink.owner_user_id == user_id,
            BotUserLink.is_blocked == False,  # noqa: E712
        )
    )
    return int(res.scalar_one() or 0) > 0


async def _count_subscriptions(
    db: AsyncSession,
    *,
    user_id: int,
    active_only: bool = False,
) -> int:
    stmt = select(func.count()).select_from(Subscription).where(
        Subscription.owner_user_id == user_id,
    )
    if active_only:
        stmt = stmt.where(Subscription.is_active == True)  # noqa: E712
    res = await db.execute(stmt)
    return int(res.scalar_one() or 0)


async def _last_subscription_run_at(
    db: AsyncSession,
    *,
    user_id: int,
) -> Optional[datetime]:
    """Latest run of any subscription for this user.

    Preference order:
      1) max(UsageEvent.created_at) for subscription_run_* events
      2) fallback: max(SubscriptionState.last_checked_at) across user's subs
    """
    res = await db.execute(
        select(func.max(UsageEvent.created_at)).where(
            UsageEvent.user_id == user_id,
            UsageEvent.event_type.in_(
                ["subscription_run_success", "subscription_run_failed"]
            ),
        )
    )
    via_events = res.scalar_one_or_none()
    if via_events is not None:
        return via_events

    res2 = await db.execute(
        select(func.max(SubscriptionState.last_checked_at))
        .join(Subscription, Subscription.id == SubscriptionState.subscription_id)
        .where(Subscription.owner_user_id == user_id)
    )
    return res2.scalar_one_or_none()


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------

@router.get("/users")
async def admin_list_users(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """List users with per-user aggregates.

    MVP: N+1 aggregates per user. Fine while userbase < ~1k. Move to a
    single CTE or materialized view in v2 if it becomes a bottleneck.
    """
    now = _utc_now()
    day_p = _day_start(now)
    mon_p = _month_start(now)
    month_start_dt = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

    res = await db.execute(
        select(User).order_by(User.created_at.desc()).limit(limit).offset(offset)
    )
    users = list(res.scalars().all())

    total_res = await db.execute(select(func.count()).select_from(User))
    total = int(total_res.scalar_one() or 0)

    items: list[dict[str, Any]] = []
    for u in users:
        items.append(
            {
                "id": int(u.id),
                "email": u.email,
                "plan": u.plan,
                "is_active": bool(u.is_active),

                "created_at": _iso(u.created_at),
                "last_login_at": _iso(u.last_login_at),

                "country_code": u.country_code,
                "language": u.language,
                "timezone": u.timezone,
                "default_ai_model": u.default_ai_model,

                "web_sessions_active_count": await _count_active_web_sessions(db, user_id=u.id),
                "telegram_session_active": await _telegram_session_is_active(db, user_id=u.id),
                "bot_linked": await _bot_is_linked(db, user_id=u.id),

                "qa_today": await _get_used_counter(db, user_id=u.id, metric_code="qa_request", period_type="day", period_start=day_p),
                "qa_month": await _get_used_counter(db, user_id=u.id, metric_code="qa_request", period_type="month", period_start=mon_p),
                "qa_total_success": await _count_usage_events(db, user_id=u.id, event_type="qa_request_success"),
                "qa_total_failed": await _count_usage_events(db, user_id=u.id, event_type="qa_request_failed"),
                "qa_total_rejected": await _count_usage_events(db, user_id=u.id, event_type="qa_request_rejected"),

                "active_subscriptions_count": await _count_subscriptions(db, user_id=u.id, active_only=True),
                "total_subscriptions_count": await _count_subscriptions(db, user_id=u.id, active_only=False),

                "last_qa_at": _iso(await _last_usage_event_at(db, user_id=u.id, event_type_like="qa_request_%")),
                "last_subscription_run_at": _iso(await _last_subscription_run_at(db, user_id=u.id)),

                "estimated_cost_usd_total": await _sum_estimated_cost(db, user_id=u.id),
                "estimated_cost_usd_month": await _sum_estimated_cost(db, user_id=u.id, since=month_start_dt),
            }
        )

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# GET /admin/users/{user_id}
# ---------------------------------------------------------------------------

@router.get("/users/{user_id}")
async def admin_user_detail(
    user_id: int,
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    res = await db.execute(select(User).where(User.id == user_id))
    u = res.scalar_one_or_none()
    if not u:
        raise HTTPException(status_code=404, detail={"code": "USER_NOT_FOUND"})

    now = _utc_now()
    day_p = _day_start(now)
    mon_p = _month_start(now)
    month_start_dt = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

    # --- profile ---
    profile = {
        "id": int(u.id),
        "email": u.email,
        "phone_masked": _mask_phone(u.phone),
        "plan": u.plan,
        "is_active": bool(u.is_active),
        "created_at": _iso(u.created_at),
        "last_login_at": _iso(u.last_login_at),
        "country_code": u.country_code,
        "language": u.language,
        "timezone": u.timezone,
        "default_ai_model": u.default_ai_model,
        "logout_revokes_telegram": bool(u.logout_revokes_telegram),
    }

    # --- usage summary ---
    qa_today = await _get_used_counter(db, user_id=u.id, metric_code="qa_request", period_type="day", period_start=day_p)
    qa_month = await _get_used_counter(db, user_id=u.id, metric_code="qa_request", period_type="month", period_start=mon_p)
    qa_success = await _count_usage_events(db, user_id=u.id, event_type="qa_request_success")
    qa_failed = await _count_usage_events(db, user_id=u.id, event_type="qa_request_failed")
    qa_rejected = await _count_usage_events(db, user_id=u.id, event_type="qa_request_rejected")

    models_used: dict[str, int] = {}
    rows = await db.execute(
        select(
            UsageEvent.meta_json["ai_model"].astext.label("ai_model"),
            func.count().label("cnt"),
        )
        .where(
            UsageEvent.user_id == u.id,
            UsageEvent.event_type == "qa_request_success",
            UsageEvent.meta_json["ai_model"].astext.isnot(None),
        )
        .group_by(UsageEvent.meta_json["ai_model"].astext)
    )
    for r in rows.all():
        models_used[str(r.ai_model)] = int(r.cnt)

    source_modes: dict[str, int] = {}
    rows = await db.execute(
        select(UsageEvent.source_mode, func.count())
        .where(
            UsageEvent.user_id == u.id,
            UsageEvent.event_type == "qa_request_success",
            UsageEvent.source_mode.isnot(None),
        )
        .group_by(UsageEvent.source_mode)
    )
    for sm, cnt in rows.all():
        source_modes[str(sm)] = int(cnt)

    avg_durations_q = select(
        func.avg(sa.cast(UsageEvent.meta_json["duration_ms_total"].astext, sa.Integer)).label("avg_total"),
        func.avg(sa.cast(UsageEvent.meta_json["input_tokens"].astext, sa.Integer)).label("avg_in"),
        func.avg(sa.cast(UsageEvent.meta_json["output_tokens"].astext, sa.Integer)).label("avg_out"),
    ).where(
        UsageEvent.user_id == u.id,
        UsageEvent.event_type == "qa_request_success",
    )
    avg_row = (await db.execute(avg_durations_q)).one_or_none()
    avg_duration_ms_total = int(avg_row.avg_total) if avg_row and avg_row.avg_total is not None else None
    avg_input_tokens = int(avg_row.avg_in) if avg_row and avg_row.avg_in is not None else None
    avg_output_tokens = int(avg_row.avg_out) if avg_row and avg_row.avg_out is not None else None

    usage_summary = {
        "qa_today": qa_today,
        "qa_month": qa_month,
        "qa_total_success": qa_success,
        "qa_total_failed": qa_failed,
        "qa_total_rejected": qa_rejected,
        "models_used": models_used,
        "source_modes": source_modes,
        "avg_duration_ms_total": avg_duration_ms_total,
        "avg_input_tokens": avg_input_tokens,
        "avg_output_tokens": avg_output_tokens,
        "estimated_cost_usd_total": await _sum_estimated_cost(db, user_id=u.id),
        "estimated_cost_usd_month": await _sum_estimated_cost(db, user_id=u.id, since=month_start_dt),
    }

    # --- telegram block ---
    web_rows = (
        await db.execute(
            select(WebSession)
            .where(WebSession.user_id == u.id)
            .order_by(WebSession.created_at.desc())
            .limit(20)
        )
    ).scalars().all()
    web_sessions = [
        {
            "created_at": _iso(s.created_at),
            "expires_at": _iso(s.expires_at),
            "revoked_at": _iso(s.revoked_at),
            "last_seen_at": _iso(s.last_seen_at),
            "user_agent": _short_ua(s.user_agent),
            "ip_masked": _mask_ip(s.ip),
            "is_active": bool(s.revoked_at is None and s.expires_at and s.expires_at > now),
        }
        for s in web_rows
    ]

    tg_rows = (
        await db.execute(
            select(TelegramSession)
            .where(TelegramSession.owner_user_id == u.id)
            .order_by(TelegramSession.created_at.desc())
        )
    ).scalars().all()
    telegram_sessions = [
        {
            "is_active": bool(t.is_active),
            "created_at": _iso(t.created_at),
            "updated_at": _iso(t.updated_at),
            "last_used_at": _iso(t.last_used_at),
            "revoked_at": _iso(t.revoked_at),
            # session_ciphertext NEVER returned
        }
        for t in tg_rows
    ]

    bot_rows = (
        await db.execute(
            select(BotUserLink)
            .where(BotUserLink.owner_user_id == u.id)
            .order_by(BotUserLink.created_at.desc())
        )
    ).scalars().all()
    bot_links = [
        {
            "started_at": _iso(b.started_at),
            "is_blocked": bool(b.is_blocked),
            "created_at": _iso(b.created_at),
            "updated_at": _iso(b.updated_at),
            "telegram_chat_id_masked": _mask_chat_id(b.telegram_chat_id),
        }
        for b in bot_rows
    ]

    telegram = {
        "web_sessions": web_sessions,
        "telegram_sessions": telegram_sessions,
        "bot_links": bot_links,
    }

    # --- subscriptions ---
    sub_rows = (
        await db.execute(
            select(Subscription, SubscriptionState)
            .outerjoin(SubscriptionState, SubscriptionState.subscription_id == Subscription.id)
            .where(Subscription.owner_user_id == u.id)
            .order_by(Subscription.created_at.desc())
        )
    ).all()

    subs: list[dict[str, Any]] = []
    for sub, state in sub_rows:
        match_total = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(MatchEvent.subscription_id == sub.id)
        )).scalar_one() or 0)
        digest_total = int((await db.execute(
            select(func.count()).select_from(DigestEvent).where(DigestEvent.subscription_id == sub.id)
        )).scalar_one() or 0)
        notify_queued = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(
                MatchEvent.subscription_id == sub.id,
                MatchEvent.notify_status == "queued",
            )
        )).scalar_one() or 0)
        notify_sent = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(
                MatchEvent.subscription_id == sub.id,
                MatchEvent.notify_status == "sent",
            )
        )).scalar_one() or 0)
        notify_failed = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(
                MatchEvent.subscription_id == sub.id,
                MatchEvent.notify_status == "failed",
            )
        )).scalar_one() or 0)

        subs.append(
            {
                "id": int(sub.id),
                "name": sub.name,
                "source_mode": sub.source_mode,
                "subscription_type": sub.subscription_type,
                "frequency_minutes": int(sub.frequency_minutes),
                "ai_model": sub.ai_model,
                "is_active": bool(sub.is_active),
                "status": sub.status,
                "is_trial": bool(sub.is_trial),
                "trial_started_at": _iso(sub.trial_started_at),
                "trial_ends_at": _iso(sub.trial_ends_at),
                "created_at": _iso(sub.created_at),
                "updated_at": _iso(sub.updated_at),

                "chat_ref_display": _mask_chat_ref_for_source(sub.chat_ref, sub.source_mode),

                "last_checked_at": _iso(getattr(state, "last_checked_at", None)),
                "last_success_at": _iso(getattr(state, "last_success_at", None)),
                "next_run_at": _iso(getattr(state, "next_run_at", None)),

                "match_events_count": match_total,
                "digest_events_count": digest_total,
                "notify_queued_count": notify_queued,
                "notify_sent_count": notify_sent,
                "notify_failed_count": notify_failed,

                "last_error": sub.last_error,
                # prompt NEVER returned
            }
        )

    # --- recent usage events ---
    recent_rows = (
        await db.execute(
            select(UsageEvent)
            .where(UsageEvent.user_id == u.id)
            .order_by(UsageEvent.id.desc())
            .limit(50)
        )
    ).scalars().all()
    recent = [_serialize_usage_event(ev) for ev in recent_rows]

    return {
        "profile": profile,
        "usage_summary": usage_summary,
        "telegram": telegram,
        "subscriptions": subs,
        "recent_usage_events": recent,
    }


# ---------------------------------------------------------------------------
# GET /admin/usage-events
# ---------------------------------------------------------------------------

def _serialize_usage_event(ev: UsageEvent) -> dict[str, Any]:
    """Project an UsageEvent into the admin response shape. The full
    meta_json is returned as-is — it never contains text content per
    record_qa_success/_failure contract."""
    m = ev.meta_json or {}
    return {
        "id": int(ev.id),
        "created_at": _iso(ev.created_at),
        "user_id": int(ev.user_id),
        "event_type": ev.event_type,
        "status": ev.status,
        "source_mode": ev.source_mode,
        "subscription_id": int(ev.subscription_id) if ev.subscription_id is not None else None,
        "chat_ref_display": _mask_chat_ref_for_source(ev.chat_ref, ev.source_mode),
        "ai_model": m.get("ai_model"),
        "days": m.get("days"),
        "frequency_minutes": m.get("frequency_minutes"),
        "messages_fetched_count": m.get("messages_fetched_count"),
        "messages_sent_to_llm_count": m.get("messages_sent_to_llm_count"),
        "matches_written": m.get("matches_written"),
        "digest_events_written": m.get("digest_events_written"),
        "events_in_group": m.get("events_in_group"),
        "input_tokens": m.get("input_tokens") if m.get("input_tokens") is not None else m.get("estimated_input_tokens"),
        "output_tokens": m.get("output_tokens") if m.get("output_tokens") is not None else m.get("estimated_output_tokens"),
        "total_tokens": m.get("total_tokens") if m.get("total_tokens") is not None else m.get("estimated_total_tokens"),
        "tokens_source": m.get("tokens_source"),
        "estimated_cost_usd": m.get("estimated_cost_usd"),
        "cost_calculation_method": m.get("cost_calculation_method"),
        "duration_ms_total": m.get("duration_ms_total"),
        "duration_ms_fetch": m.get("duration_ms_fetch"),
        "duration_ms_llm": m.get("duration_ms_llm"),
        "error_code": m.get("error_code"),
        "error_message": m.get("error_message"),
        "meta_json": m,  # full payload for expandable JSON view in admin UI
    }


@router.get("/usage-events")
async def admin_list_usage_events(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user_id: Optional[int] = None,
    event_type: Optional[str] = None,
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Paginated UsageEvent feed. Sorted by id DESC (newest first).

    Filters supported (all optional):
      - user_id
      - event_type
    """
    stmt = select(UsageEvent).order_by(UsageEvent.id.desc())
    count_stmt = select(func.count()).select_from(UsageEvent)

    if user_id is not None:
        stmt = stmt.where(UsageEvent.user_id == int(user_id))
        count_stmt = count_stmt.where(UsageEvent.user_id == int(user_id))
    if event_type:
        stmt = stmt.where(UsageEvent.event_type == event_type)
        count_stmt = count_stmt.where(UsageEvent.event_type == event_type)

    total = int((await db.execute(count_stmt)).scalar_one() or 0)

    rows = (await db.execute(stmt.limit(limit).offset(offset))).scalars().all()

    return {
        "items": [_serialize_usage_event(ev) for ev in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# GET /admin/subscriptions
# ---------------------------------------------------------------------------

@router.get("/subscriptions")
async def admin_list_subscriptions(
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """All subscriptions across all users with per-sub aggregates."""
    rows = (
        await db.execute(
            select(Subscription, SubscriptionState, User)
            .outerjoin(SubscriptionState, SubscriptionState.subscription_id == Subscription.id)
            .outerjoin(User, User.id == Subscription.owner_user_id)
            .order_by(Subscription.created_at.desc())
            .limit(limit).offset(offset)
        )
    ).all()

    total = int((await db.execute(select(func.count()).select_from(Subscription))).scalar_one() or 0)

    items: list[dict[str, Any]] = []
    for sub, state, owner in rows:
        match_total = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(MatchEvent.subscription_id == sub.id)
        )).scalar_one() or 0)
        digest_total = int((await db.execute(
            select(func.count()).select_from(DigestEvent).where(DigestEvent.subscription_id == sub.id)
        )).scalar_one() or 0)
        notify_queued = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(
                MatchEvent.subscription_id == sub.id,
                MatchEvent.notify_status == "queued",
            )
        )).scalar_one() or 0)
        notify_sent = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(
                MatchEvent.subscription_id == sub.id,
                MatchEvent.notify_status == "sent",
            )
        )).scalar_one() or 0)
        notify_failed = int((await db.execute(
            select(func.count()).select_from(MatchEvent).where(
                MatchEvent.subscription_id == sub.id,
                MatchEvent.notify_status == "failed",
            )
        )).scalar_one() or 0)

        last_run = (await db.execute(
            select(func.max(UsageEvent.created_at)).where(
                UsageEvent.subscription_id == sub.id,
                UsageEvent.event_type.in_(["subscription_run_success", "subscription_run_failed"]),
            )
        )).scalar_one_or_none()

        # Estimated cost for THIS subscription (sum across user's run events
        # that match this sub_id)
        cost_expr = sa.cast(
            UsageEvent.meta_json["estimated_cost_usd"].astext, sa.Numeric(14, 6)
        )
        total_cost_val = (await db.execute(
            select(func.sum(cost_expr)).where(
                UsageEvent.subscription_id == sub.id,
                UsageEvent.meta_json["estimated_cost_usd"].astext.isnot(None),
            )
        )).scalar_one_or_none()
        total_cost = float(total_cost_val) if total_cost_val is not None else None

        last_run_cost_val = (await db.execute(
            select(cost_expr).where(
                UsageEvent.subscription_id == sub.id,
                UsageEvent.event_type == "subscription_run_success",
                UsageEvent.meta_json["estimated_cost_usd"].astext.isnot(None),
            ).order_by(UsageEvent.id.desc()).limit(1)
        )).scalar_one_or_none()
        last_run_cost = float(last_run_cost_val) if last_run_cost_val is not None else None

        items.append({
            "id": int(sub.id),
            "owner_user_id": int(sub.owner_user_id) if sub.owner_user_id is not None else None,
            "owner_email": getattr(owner, "email", None) if owner is not None else None,

            "name": sub.name,
            "source_mode": sub.source_mode,
            "subscription_type": sub.subscription_type,
            "frequency_minutes": int(sub.frequency_minutes),
            "ai_model": sub.ai_model,

            "is_active": bool(sub.is_active),
            "status": sub.status,
            "last_error": sub.last_error,

            "is_trial": bool(sub.is_trial),
            "trial_started_at": _iso(sub.trial_started_at),
            "trial_ends_at": _iso(sub.trial_ends_at),

            "created_at": _iso(sub.created_at),
            "updated_at": _iso(sub.updated_at),

            "chat_ref_display": _mask_chat_ref_for_source(sub.chat_ref, sub.source_mode),

            "last_checked_at": _iso(getattr(state, "last_checked_at", None)),
            "last_success_at": _iso(getattr(state, "last_success_at", None)),
            "next_run_at": _iso(getattr(state, "next_run_at", None)),

            "match_events_count": match_total,
            "digest_events_count": digest_total,

            "notify_queued_count": notify_queued,
            "notify_sent_count": notify_sent,
            "notify_failed_count": notify_failed,

            "last_run_event_at": _iso(last_run),
            "last_run_estimated_cost_usd": last_run_cost,
            "total_estimated_cost_usd": total_cost,
            # prompt NEVER returned
        })

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# GET /admin/sessions
# ---------------------------------------------------------------------------

@router.get("/sessions")
async def admin_list_sessions(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Three sections: web sessions, telegram sessions, bot links."""
    now = _utc_now()

    # --- Web sessions ---
    web_rows = (
        await db.execute(
            select(WebSession, User)
            .outerjoin(User, User.id == WebSession.user_id)
            .order_by(WebSession.created_at.desc())
            .limit(limit).offset(offset)
        )
    ).all()
    web_sessions = [
        {
            "user_id": int(s.user_id),
            "email": getattr(u, "email", None),
            "created_at": _iso(s.created_at),
            "expires_at": _iso(s.expires_at),
            "revoked_at": _iso(s.revoked_at),
            "last_seen_at": _iso(s.last_seen_at),
            "user_agent": _short_ua(s.user_agent),
            "ip_masked": _mask_ip(s.ip),
            "is_active": bool(s.revoked_at is None and s.expires_at and s.expires_at > now),
        }
        for s, u in web_rows
    ]

    # --- Telegram sessions ---
    tg_rows = (
        await db.execute(
            select(TelegramSession, User)
            .outerjoin(User, User.id == TelegramSession.owner_user_id)
            .order_by(TelegramSession.created_at.desc())
            .limit(limit).offset(offset)
        )
    ).all()
    telegram_sessions = [
        {
            "user_id": int(t.owner_user_id),
            "email": getattr(u, "email", None),
            "is_active": bool(t.is_active),
            "created_at": _iso(t.created_at),
            "updated_at": _iso(t.updated_at),
            "last_used_at": _iso(t.last_used_at),
            "revoked_at": _iso(t.revoked_at),
            # session_ciphertext NEVER returned
        }
        for t, u in tg_rows
    ]

    # --- Bot links ---
    bot_rows = (
        await db.execute(
            select(BotUserLink, User)
            .outerjoin(User, User.id == BotUserLink.owner_user_id)
            .order_by(BotUserLink.created_at.desc())
            .limit(limit).offset(offset)
        )
    ).all()
    bot_links = [
        {
            "owner_user_id": int(b.owner_user_id) if b.owner_user_id is not None else None,
            "email": getattr(u, "email", None),
            "telegram_user_id": int(b.telegram_user_id) if b.telegram_user_id is not None else None,
            "telegram_chat_id_masked": _mask_chat_id(b.telegram_chat_id),
            "started_at": _iso(b.started_at),
            "is_blocked": bool(b.is_blocked),
            "created_at": _iso(b.created_at),
            "updated_at": _iso(b.updated_at),
        }
        for b, u in bot_rows
    ]

    return {
        "web_sessions": web_sessions,
        "telegram_sessions": telegram_sessions,
        "bot_links": bot_links,
        "limit": limit,
        "offset": offset,
    }
