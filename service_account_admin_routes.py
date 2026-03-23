from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user as auth_get_current_user
from db.models import (
    User,
    ServicePhoneNumber,
    ServiceTelegramAccount,
    ServiceAccountLog,
)

from db.session import get_db

router = APIRouter()


def utcnow():
    return datetime.now(timezone.utc)


@router.get("/admin/service-accounts/state")
async def service_accounts_admin_state(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # На MVP без ролей, но только для авторизованного пользователя.
    phones_res = await db.execute(
        select(ServicePhoneNumber).order_by(ServicePhoneNumber.id.asc())
    )
    phones = list(phones_res.scalars().all())

    accounts_res = await db.execute(
        select(ServiceTelegramAccount, ServicePhoneNumber.phone_e164)
        .join(ServicePhoneNumber, ServicePhoneNumber.id == ServiceTelegramAccount.phone_number_id)
        .order_by(ServiceTelegramAccount.id.asc())
    )
    account_rows = accounts_res.all()

    logs_res = await db.execute(
        select(ServiceAccountLog)
        .order_by(ServiceAccountLog.id.desc())
        .limit(200)
    )
    logs = list(logs_res.scalars().all())

    one_hour_ago = utcnow() - timedelta(hours=1)

    no_free_res = await db.execute(
        select(func.count())
        .select_from(ServiceAccountLog)
        .where(
            ServiceAccountLog.event_type == "no_free_account",
            ServiceAccountLog.event_at >= one_hour_ago,
        )
    )
    no_free_count = int(no_free_res.scalar() or 0)

    stats = {
        "active_accounts": sum(1 for _, __ in account_rows if _.status == "active"),
        "cooldown_accounts": sum(1 for _, __ in account_rows if _.status == "cooldown"),
        "needs_reauth_accounts": sum(1 for _, __ in account_rows if _.status == "needs_reauth"),
        "busy_accounts": sum(1 for _, __ in account_rows if _.is_busy),
        "no_free_account_last_hour": no_free_count,
        "total_logs": len(logs),
    }

    return {
        "phone_numbers": [
            {
                "id": p.id,
                "phone_e164": p.phone_e164,
                "provider_code": p.provider_code,
                "country_code": p.country_code,
                "monthly_cost": float(p.monthly_cost) if p.monthly_cost is not None else None,
                "currency": p.currency,
                "total_spent": float(p.total_spent) if p.total_spent is not None else 0,
                "last_paid_at": p.last_paid_at.isoformat() if p.last_paid_at else None,
                "is_active": p.is_active,
                "created_at": p.created_at.isoformat() if p.created_at else None,
                "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            }
            for p in phones
        ],
        "accounts": [
            {
                "id": a.id,
                "phone_number_id": a.phone_number_id,
                "phone_e164": phone_e164,
                "telegram_user_id": a.telegram_user_id,
                "telegram_username": a.telegram_username,
                "status": a.status,
                "is_enabled": a.is_enabled,
                "is_busy": a.is_busy,
                "busy_started_at": a.busy_started_at.isoformat() if a.busy_started_at else None,
                "cooldown_until": a.cooldown_until.isoformat() if a.cooldown_until else None,
                "last_used_at": a.last_used_at.isoformat() if a.last_used_at else None,
                "last_auth_at": a.last_auth_at.isoformat() if a.last_auth_at else None,
                "last_error": a.last_error,
                "last_error_at": a.last_error_at.isoformat() if a.last_error_at else None,
                "consecutive_fail_count": a.consecutive_fail_count,
                "requests_last_minute": a.requests_last_minute,
                "requests_last_hour": a.requests_last_hour,
                "requests_last_day": a.requests_last_day,
                "created_at": a.created_at.isoformat() if a.created_at else None,
                "updated_at": a.updated_at.isoformat() if a.updated_at else None,
            }
            for a, phone_e164 in account_rows
        ],
        "logs": [
            {
                "id": l.id,
                "service_account_id": l.service_account_id,
                "event_type": l.event_type,
                "target_ref": l.target_ref,
                "is_success": l.is_success,
                "error_code": l.error_code,
                "error_message": l.error_message,
                "event_at": l.event_at.isoformat() if l.event_at else None,
                "started_at": l.started_at.isoformat() if l.started_at else None,
                "finished_at": l.finished_at.isoformat() if l.finished_at else None,
            }
            for l in logs
        ],
        "stats": stats,
    }
