from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from telethon import TelegramClient
from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError, PhoneNumberInvalidError
from telethon.sessions import StringSession

from auth import get_current_user as auth_get_current_user
from db.models import (
    User,
    ServicePhoneNumber,
    ServiceTelegramAccount,
    ServiceTelegramSession,
    ServiceAccountLog,
    ServiceAccountStatusHistory,
)
from db.session import get_db
from telegram_service import encrypt_session
import os

router = APIRouter()

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]

_admin_auth_clients: dict[int, TelegramClient] = {}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def session_status_payload(s: ServiceTelegramSession) -> dict:
    status = "active" if s.is_active and s.revoked_at is None else "inactive"
    status_label = "Активна" if status == "active" else "Неактивна"

    return {
        "id": s.id,
        "service_account_id": s.service_account_id,
        "session_version": s.session_version,
        "is_active": s.is_active,
        "status": status,
        "status_label": status_label,
        "revoked_at": s.revoked_at.isoformat() if s.revoked_at else None,
        "revoked_reason": s.revoked_reason,
        "last_used_at": s.last_used_at.isoformat() if s.last_used_at else None,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }

def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    if len(s) == 10:
        return datetime.fromisoformat(s + "T00:00:00+00:00")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def dec_or_none(value):
    if value is None or value == "":
        return None
    return Decimal(str(value))


class PhoneSaveRow(BaseModel):
    id: Optional[int] = None
    phone_e164: str
    provider_code: str
    country_code: str
    monthly_cost: Optional[float] = None
    currency: Optional[str] = None
    total_spent: Optional[float] = 0
    last_paid_at: Optional[str] = None
    is_active: bool = True


class PhonesSaveRequest(BaseModel):
    rows: list[PhoneSaveRow] = Field(default_factory=list)


class AccountSaveRow(BaseModel):
    id: Optional[int] = None
    phone_number_id: int
    telegram_user_id: Optional[int] = None
    telegram_username: Optional[str] = None
    usage_role: str = "analysis"
    status: str = "active"
    is_enabled: bool = True
    is_busy: bool = False
    busy_started_at: Optional[str] = None
    cooldown_until: Optional[str] = None
    last_used_at: Optional[str] = None
    last_auth_at: Optional[str] = None
    last_error: Optional[str] = None
    last_error_at: Optional[str] = None
    consecutive_fail_count: int = 0
    requests_last_minute: int = 0
    requests_last_hour: int = 0
    requests_last_day: int = 0


class AccountsSaveRequest(BaseModel):
    rows: list[AccountSaveRow] = Field(default_factory=list)


class ServiceAccountAuthStartRequest(BaseModel):
    service_account_id: int


class ServiceAccountAuthCodeRequest(BaseModel):
    service_account_id: int
    code: str


class ServiceAccountAuthPasswordRequest(BaseModel):
    service_account_id: int
    password: str


async def _get_phone_or_404(db: AsyncSession, phone_id: int) -> ServicePhoneNumber:
    phone = await db.get(ServicePhoneNumber, phone_id)
    if not phone:
        raise HTTPException(status_code=404, detail={"code": "PHONE_NOT_FOUND", "message": "Номер телефона не найден."})
    return phone


async def _get_account_or_404(db: AsyncSession, account_id: int) -> ServiceTelegramAccount:
    account = await db.get(ServiceTelegramAccount, account_id)
    if not account:
        raise HTTPException(status_code=404, detail={"code": "ACCOUNT_NOT_FOUND", "message": "Аккаунт не найден."})
    return account


async def _write_status_history(db: AsyncSession, *, service_account_id: int, old_status: Optional[str], new_status: str, reason: Optional[str]) -> None:
    db.add(ServiceAccountStatusHistory(service_account_id=service_account_id, old_status=old_status, new_status=new_status, reason=reason))
    await db.flush()


async def _serialize_state(db: AsyncSession) -> dict:
    phones_res = await db.execute(select(ServicePhoneNumber).order_by(ServicePhoneNumber.id.asc()))
    phones = list(phones_res.scalars().all())
    accounts_res = await db.execute(
        select(ServiceTelegramAccount, ServicePhoneNumber.phone_e164)
        .join(ServicePhoneNumber, ServicePhoneNumber.id == ServiceTelegramAccount.phone_number_id)
        .order_by(ServiceTelegramAccount.id.asc())
    )
    account_rows = accounts_res.all()
    logs_res = await db.execute(select(ServiceAccountLog).order_by(ServiceAccountLog.id.desc()).limit(300))
    logs = list(logs_res.scalars().all())
    one_hour_ago = utcnow() - timedelta(hours=1)
    no_free_res = await db.execute(select(func.count()).select_from(ServiceAccountLog).where(ServiceAccountLog.event_type == "no_free_account", ServiceAccountLog.event_at >= one_hour_ago))
    no_free_count = int(no_free_res.scalar() or 0)
    return {
        "phone_numbers": [{"id": p.id, "phone_e164": p.phone_e164, "provider_code": p.provider_code, "country_code": p.country_code, "monthly_cost": float(p.monthly_cost) if p.monthly_cost is not None else None, "currency": p.currency, "total_spent": float(p.total_spent) if p.total_spent is not None else 0, "last_paid_at": p.last_paid_at.isoformat() if p.last_paid_at else None, "is_active": p.is_active, "created_at": p.created_at.isoformat() if p.created_at else None, "updated_at": p.updated_at.isoformat() if p.updated_at else None} for p in phones],

        "accounts": [{
                "id": a.id,
                "phone_number_id": a.phone_number_id,
                "phone_e164": phone_e164,
                "telegram_user_id": a.telegram_user_id,
                "telegram_username": a.telegram_username,
                "usage_role": a.usage_role,
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
                "updated_at": a.updated_at.isoformat() if a.updated_at else None
            } for a, phone_e164 in account_rows],

        "logs": [{"id": l.id, "service_account_id": l.service_account_id, "event_type": l.event_type, "target_ref": l.target_ref, "is_success": l.is_success, "error_code": l.error_code, "error_message": l.error_message, "event_at": l.event_at.isoformat() if l.event_at else None, "started_at": l.started_at.isoformat() if l.started_at else None, "finished_at": l.finished_at.isoformat() if l.finished_at else None} for l in logs],
        "stats": {"active_accounts": sum(1 for a, _ in account_rows if a.status == "active"), "cooldown_accounts": sum(1 for a, _ in account_rows if a.status == "cooldown"), "needs_reauth_accounts": sum(1 for a, _ in account_rows if a.status == "needs_reauth"), "busy_accounts": sum(1 for a, _ in account_rows if a.is_busy), "no_free_account_last_hour": no_free_count, "total_logs": len(logs)}
    }


async def _save_service_session(db: AsyncSession, *, service_account_id: int, session_string: str) -> None:
    cipher = encrypt_session(session_string)
    await db.execute(update(ServiceTelegramSession).where(ServiceTelegramSession.service_account_id == service_account_id, ServiceTelegramSession.is_active.is_(True)).values(is_active=False, revoked_at=utcnow(), revoked_reason="replaced_by_new_session"))
    db.add(ServiceTelegramSession(service_account_id=service_account_id, session_ciphertext=cipher, session_version=1, is_active=True, revoked_at=None, revoked_reason=None, last_used_at=utcnow()))
    await db.flush()


@router.get("/admin/service-accounts/state")
async def service_accounts_admin_state(user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    return await _serialize_state(db)


@router.post("/admin/service-accounts/phones/save")
async def service_accounts_admin_save_phones(payload: PhonesSaveRequest, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    try:
        for row in payload.rows:
            phone_e164 = (row.phone_e164 or "").strip()
            provider_code = (row.provider_code or "").strip()
            country_code = (row.country_code or "").strip().upper()
            if not phone_e164:
                raise HTTPException(status_code=400, detail={"code": "PHONE_REQUIRED", "message": "Укажите номер телефона."})
            if not provider_code:
                raise HTTPException(status_code=400, detail={"code": "PROVIDER_REQUIRED", "message": "Укажите провайдера."})
            if not country_code:
                raise HTTPException(status_code=400, detail={"code": "COUNTRY_REQUIRED", "message": "Укажите страну."})
            dup_res = await db.execute(select(ServicePhoneNumber).where(ServicePhoneNumber.phone_e164 == phone_e164))
            existing_same_phone = dup_res.scalar_one_or_none()
            if row.id is None:
                if existing_same_phone is not None:
                    raise HTTPException(status_code=409, detail={"code": "PHONE_ALREADY_EXISTS", "message": f"Номер {phone_e164} уже существует."})
                db.add(ServicePhoneNumber(phone_e164=phone_e164, provider_code=provider_code, country_code=country_code, monthly_cost=dec_or_none(row.monthly_cost), currency=(row.currency or "").strip() or None, total_spent=dec_or_none(row.total_spent) or Decimal("0"), last_paid_at=parse_dt(row.last_paid_at), is_active=row.is_active))
                await db.flush()
            else:
                phone = await _get_phone_or_404(db, row.id)
                if existing_same_phone is not None and existing_same_phone.id != phone.id:
                    raise HTTPException(status_code=409, detail={"code": "PHONE_ALREADY_EXISTS", "message": f"Номер {phone_e164} уже существует."})
                prev_active = phone.is_active
                phone.phone_e164 = phone_e164
                phone.provider_code = provider_code
                phone.country_code = country_code
                phone.monthly_cost = dec_or_none(row.monthly_cost)
                phone.currency = (row.currency or "").strip() or None
                phone.total_spent = dec_or_none(row.total_spent) or Decimal("0")
                phone.last_paid_at = parse_dt(row.last_paid_at)
                phone.is_active = row.is_active
                phone.updated_at = utcnow()
                await db.flush()
                if prev_active and not row.is_active:
                    res = await db.execute(select(ServiceTelegramAccount).where(ServiceTelegramAccount.phone_number_id == phone.id))
                    linked_accounts = list(res.scalars().all())
                    for account in linked_accounts:
                        old_status = account.status
                        account.status = "disabled"
                        account.is_enabled = False
                        account.updated_at = utcnow()
                        await _write_status_history(db, service_account_id=account.id, old_status=old_status, new_status="disabled", reason="phone_deactivated")
        await db.commit()
        return {"status": "ok", "message": "Изменения по номерам сохранены.", "state": await _serialize_state(db)}
    except HTTPException:
        await db.rollback(); raise
    except Exception as e:
        await db.rollback(); raise HTTPException(status_code=500, detail={"code": "PHONE_SAVE_FAILED", "message": str(e)})


@router.post("/admin/service-accounts/accounts/save")
async def service_accounts_admin_save_accounts(payload: AccountsSaveRequest, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    allowed_statuses = {"active", "cooldown", "needs_reauth", "disabled", "banned"}
    allowed_usage_roles = {"analysis", "subscriptions", "shared"}
    try:
        for row in payload.rows:
            phone = await _get_phone_or_404(db, row.phone_number_id)
            if row.status not in allowed_statuses:
                raise HTTPException(status_code=400, detail={"code": "INVALID_STATUS", "message": f"Недопустимый статус: {row.status}"})

            usage_role = (row.usage_role or "analysis").strip().lower()
            if usage_role not in allowed_usage_roles:
                raise HTTPException(
                    status_code=400,
                    detail={"code": "INVALID_USAGE_ROLE", "message": f"Недопустимое назначение: {row.usage_role}"},
                )

            same_tg = None
            if row.telegram_user_id is not None:
                dup_res = await db.execute(select(ServiceTelegramAccount).where(ServiceTelegramAccount.telegram_user_id == row.telegram_user_id))
                same_tg = dup_res.scalar_one_or_none()
            if row.id is None:
                if same_tg is not None:
                    raise HTTPException(status_code=409, detail={"code": "TELEGRAM_USER_ALREADY_EXISTS", "message": f"Telegram user id {row.telegram_user_id} уже используется."})
                db.add(ServiceTelegramAccount(phone_number_id=phone.id, telegram_user_id=row.telegram_user_id, telegram_username=(row.telegram_username or "").strip() or None, usage_role=usage_role, status=row.status, is_enabled=row.is_enabled, is_busy=row.is_busy, busy_started_at=parse_dt(row.busy_started_at), cooldown_until=parse_dt(row.cooldown_until), last_used_at=parse_dt(row.last_used_at), last_auth_at=parse_dt(row.last_auth_at), last_error=(row.last_error or "").strip() or None, last_error_at=parse_dt(row.last_error_at), consecutive_fail_count=row.consecutive_fail_count, requests_last_minute=row.requests_last_minute, requests_last_hour=row.requests_last_hour, requests_last_day=row.requests_last_day))
                await db.flush()
            else:
                account = await _get_account_or_404(db, row.id)
                if same_tg is not None and same_tg.id != account.id:
                    raise HTTPException(status_code=409, detail={"code": "TELEGRAM_USER_ALREADY_EXISTS", "message": f"Telegram user id {row.telegram_user_id} уже используется."})
                old_status = account.status
                account.phone_number_id = phone.id
                account.telegram_user_id = row.telegram_user_id
                account.telegram_username = (row.telegram_username or "").strip() or None
                account.usage_role = usage_role
                account.status = row.status
                account.is_enabled = row.is_enabled
                account.is_busy = row.is_busy
                account.busy_started_at = parse_dt(row.busy_started_at)
                account.cooldown_until = parse_dt(row.cooldown_until)
                account.last_used_at = parse_dt(row.last_used_at)
                account.last_auth_at = parse_dt(row.last_auth_at)
                account.last_error = (row.last_error or "").strip() or None
                account.last_error_at = parse_dt(row.last_error_at)
                account.consecutive_fail_count = row.consecutive_fail_count
                account.requests_last_minute = row.requests_last_minute
                account.requests_last_hour = row.requests_last_hour
                account.requests_last_day = row.requests_last_day
                account.updated_at = utcnow()
                await db.flush()
                if old_status != row.status:
                    await _write_status_history(db, service_account_id=account.id, old_status=old_status, new_status=row.status, reason="manual_admin_update")
        await db.commit()
        return {"status": "ok", "message": "Изменения по аккаунтам сохранены.", "state": await _serialize_state(db)}
    except HTTPException:
        await db.rollback(); raise
    except Exception as e:
        await db.rollback(); raise HTTPException(status_code=500, detail={"code": "ACCOUNT_SAVE_FAILED", "message": str(e)})


@router.post("/admin/service-accounts/auth/send_code")
async def service_account_admin_send_code(payload: ServiceAccountAuthStartRequest, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    account = await _get_account_or_404(db, payload.service_account_id)
    phone = await _get_phone_or_404(db, account.phone_number_id)
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    _admin_auth_clients[account.id] = client
    try:
        await client.send_code_request(phone.phone_e164)
        db.add(ServiceAccountLog(service_account_id=account.id, event_type="admin_auth_send_code", target_ref=phone.phone_e164, is_success=True, event_at=utcnow()))
        await db.commit()
        return {"status": "code_sent", "phone_e164": phone.phone_e164}
    except PhoneNumberInvalidError:
        await client.disconnect(); _admin_auth_clients.pop(account.id, None)
        raise HTTPException(status_code=400, detail={"code": "PHONE_NUMBER_INVALID", "message": "Номер телефона недействителен."})
    except Exception as e:
        await client.disconnect(); _admin_auth_clients.pop(account.id, None)
        raise HTTPException(status_code=500, detail={"code": "SEND_CODE_FAILED", "message": str(e)})


@router.post("/admin/service-accounts/auth/confirm_code")
async def service_account_admin_confirm_code(payload: ServiceAccountAuthCodeRequest, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    account = await _get_account_or_404(db, payload.service_account_id)
    phone = await _get_phone_or_404(db, account.phone_number_id)
    client = _admin_auth_clients.get(account.id)
    if client is None:
        raise HTTPException(status_code=400, detail={"code": "AUTH_FLOW_NOT_STARTED", "message": "Сначала запросите код авторизации."})
    try:
        me = await client.sign_in(phone=phone.phone_e164, code=payload.code.strip())
        await _save_service_session(db, service_account_id=account.id, session_string=client.session.save())
        old_status = account.status
        account.status = "active"
        account.is_enabled = True
        account.last_auth_at = utcnow()
        account.last_error = None
        account.last_error_at = None
        account.updated_at = utcnow()
        await db.flush()
        if old_status != "active":
            await _write_status_history(db, service_account_id=account.id, old_status=old_status, new_status="active", reason="admin_auth_success")
        db.add(ServiceAccountLog(service_account_id=account.id, event_type="admin_auth_success", target_ref=phone.phone_e164, is_success=True, event_at=utcnow()))
        await db.commit()
        await client.disconnect(); _admin_auth_clients.pop(account.id, None)
        return {"status": "ok", "auth_status": "authorized", "service_account_id": account.id, "telegram_username": getattr(me, "username", None), "message": "Сессия создана.", "state": await _serialize_state(db)}
    except SessionPasswordNeededError:
        return {"status": "password_needed", "service_account_id": account.id, "message": "Требуется пароль Telegram (2FA)."}
    except PhoneCodeInvalidError:
        raise HTTPException(status_code=400, detail={"code": "PHONE_CODE_INVALID", "message": "Код введён неверно."})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"code": "CONFIRM_CODE_FAILED", "message": str(e)})


@router.post("/admin/service-accounts/auth/confirm_password")
async def service_account_admin_confirm_password(payload: ServiceAccountAuthPasswordRequest, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    account = await _get_account_or_404(db, payload.service_account_id)
    phone = await _get_phone_or_404(db, account.phone_number_id)
    client = _admin_auth_clients.get(account.id)
    if client is None:
        raise HTTPException(status_code=400, detail={"code": "AUTH_FLOW_NOT_STARTED", "message": "Сначала запросите код авторизации."})
    try:
        me = await client.sign_in(password=payload.password)
        await _save_service_session(db, service_account_id=account.id, session_string=client.session.save())
        old_status = account.status
        account.status = "active"
        account.is_enabled = True
        account.last_auth_at = utcnow()
        account.last_error = None
        account.last_error_at = None
        account.updated_at = utcnow()
        await db.flush()
        if old_status != "active":
            await _write_status_history(db, service_account_id=account.id, old_status=old_status, new_status="active", reason="admin_auth_password_success")
        db.add(ServiceAccountLog(service_account_id=account.id, event_type="admin_auth_password_success", target_ref=phone.phone_e164, is_success=True, event_at=utcnow()))
        await db.commit()
        await client.disconnect(); _admin_auth_clients.pop(account.id, None)
        return {"status": "ok", "auth_status": "authorized", "service_account_id": account.id, "telegram_username": getattr(me, "username", None), "message": "Сессия создана.", "state": await _serialize_state(db)}
    except Exception as e:
        raise HTTPException(status_code=400, detail={"code": "CONFIRM_PASSWORD_FAILED", "message": str(e)})

@router.get("/admin/service-accounts/sessions/{account_id}")
async def service_account_sessions(
    account_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    account_res = await db.execute(
        select(ServiceTelegramAccount).where(ServiceTelegramAccount.id == account_id)
    )
    account = account_res.scalar_one_or_none()

    if not account:
        return {"sessions": []}

    sessions_res = await db.execute(
        select(ServiceTelegramSession)
        .where(ServiceTelegramSession.service_account_id == account_id)
        .order_by(ServiceTelegramSession.id.desc())
    )
    sessions = list(sessions_res.scalars().all())

    return {
        "sessions": [session_status_payload(s) for s in sessions]
    }