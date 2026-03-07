# auth.py
import os
import re
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from passlib.context import CryptContext

from db.session import get_db
from db.models import User, EmailVerificationCode, Session


router = APIRouter(prefix="/auth", tags=["auth"])

pwd_context = CryptContext(
    schemes=["bcrypt_sha256", "bcrypt"],  # поддержим старые хэши, если уже есть
    deprecated="auto",
)

COOKIE_NAME = "cotel_session"
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
EMAIL_CODE_TTL_MIN = int(os.getenv("EMAIL_CODE_TTL_MIN", "15"))
DEV_RETURN_EMAIL_CODE = os.getenv("DEV_RETURN_EMAIL_CODE", "0") == "1"  # на время без email-провайдера

EMAIL_RE = re.compile(r"^.{1,320}$")


# -------------------------
# Pydantic schemas (минимум)
# -------------------------

class RegisterIn(BaseModel):
    email: EmailStr
    password: str
    password_confirm: str


class VerifyEmailIn(BaseModel):
    email: EmailStr
    code: str


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class MeOut(BaseModel):
    id: int
    email: Optional[str] = None
    plan: str
    is_email_verified: bool
    is_active: bool
    last_login_at: Optional[datetime] = None

class CheckEmailIn(BaseModel):
    email: EmailStr


class CheckEmailOut(BaseModel):
    exists: bool
# -------------------------
# Helpers
# -------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _make_email_code() -> str:
    # 6 цифр достаточно на MVP, но мы храним только hash
    return f"{secrets.randbelow(1_000_000):06d}"

def _validate_password_policy(password: str) -> None:
    # MVP-политика (как ты согласовала): длина >= 8, латиница, 2 класса символов
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="PASSWORD_TOO_SHORT")

    has_lower = any("a" <= c <= "z" for c in password)
    has_upper = any("A" <= c <= "Z" for c in password)
    has_digit = any(c.isdigit() for c in password)
    has_symbol = any(not c.isalnum() for c in password)

    classes = sum([has_lower or has_upper, has_digit, has_symbol])  # буквы/цифры/символы
    if classes < 2:
        raise HTTPException(status_code=400, detail="PASSWORD_TOO_WEAK")

def _set_session_cookie(response: Response, raw_session_id: str) -> None:
    # Secure=True будет работать на https; локально можно DEV-условием выключить при необходимости
    secure = os.getenv("COOKIE_SECURE", "1") == "1"
    response.set_cookie(
        key=COOKIE_NAME,
        value=raw_session_id,
        httponly=True,
        secure=secure,
        samesite="lax",
        max_age=SESSION_TTL_DAYS * 24 * 3600,
        path="/",
    )

def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=COOKIE_NAME, path="/")


async def _create_session(db: AsyncSession, user_id: int, request: Request) -> str:
    raw_session_id = secrets.token_urlsafe(32)
    session_hash = _sha256_hex(raw_session_id)

    expires_at = _now() + timedelta(days=SESSION_TTL_DAYS)
    ua = request.headers.get("user-agent")
    ip = request.client.host if request.client else None

    db.add(
        Session(
            user_id=user_id,
            session_hash=session_hash,
            expires_at=expires_at,
            user_agent=ua,
            ip=ip,
        )
    )
    await db.commit()
    return raw_session_id


async def get_current_user_from_cookie(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> User:
    raw = request.cookies.get(COOKIE_NAME)
    if not raw:
        raise HTTPException(status_code=401, detail="NOT_AUTHENTICATED")

    session_hash = _sha256_hex(raw)

    q = (
        select(Session, User)
        .join(User, User.id == Session.user_id)
        .where(
            Session.session_hash == session_hash,
            Session.revoked_at.is_(None),
            Session.expires_at > _now(),
        )
    )
    res = await db.execute(q)
    row = res.first()
    if not row:
        raise HTTPException(status_code=401, detail="SESSION_INVALID")

    sess, user = row[0], row[1]

    if not user.is_active:
        raise HTTPException(status_code=403, detail="USER_INACTIVE")

    return user


# -------------------------
# Endpoints
# -------------------------

@router.post("/register")
async def register(payload: RegisterIn, db: AsyncSession = Depends(get_db)):
    email = payload.email.strip().lower()
    if not EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="EMAIL_INVALID")

    if payload.password != payload.password_confirm:
        raise HTTPException(status_code=400, detail="PASSWORD_MISMATCH")

    _validate_password_policy(payload.password)
    password_hash = pwd_context.hash(payload.password)

    user = User(
        email=email,
        password_hash=password_hash,
        is_email_verified=False,
        is_active=False,  # до verify
        plan="free",
    )

    try:
        db.add(user)
        await db.commit()
        await db.refresh(user)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="EMAIL_ALREADY_EXISTS")

    # создаём код
    code = _make_email_code()
    code_hash = _sha256_hex(code)
    expires_at = _now() + timedelta(minutes=EMAIL_CODE_TTL_MIN)

    # простая модель: 1 активная запись на юзера (unique=True в модели)
    # если уже есть — перезапишем
    res = await db.execute(select(EmailVerificationCode).where(EmailVerificationCode.user_id == user.id))
    row = res.scalar_one_or_none()
    if row:
        row.code_hash = code_hash
        row.expires_at = expires_at
        row.used_at = None
        row.attempts = 0
    else:
        db.add(
            EmailVerificationCode(
                user_id=user.id,
                code_hash=code_hash,
                expires_at=expires_at,
            )
        )

    await db.commit()

    # TODO: здесь будет отправка email через провайдера
    out = {"status": "ok"}
    if DEV_RETURN_EMAIL_CODE:
        out["dev_code"] = code  # только на деве
    return out


@router.post("/verify-email", response_model=MeOut)
async def verify_email(payload: VerifyEmailIn, request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    email = payload.email.strip().lower()
    code = (payload.code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="CODE_REQUIRED")

    r = await db.execute(select(User).where(User.email == email))
    user = r.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="USER_NOT_FOUND")

    r = await db.execute(select(EmailVerificationCode).where(EmailVerificationCode.user_id == user.id))
    rec = r.scalar_one_or_none()
    if not rec:
        raise HTTPException(status_code=400, detail="CODE_NOT_FOUND")

    if rec.used_at is not None:
        raise HTTPException(status_code=400, detail="CODE_ALREADY_USED")

    if rec.expires_at <= _now():
        raise HTTPException(status_code=400, detail="CODE_EXPIRED")

    # анти-перебор (мягко)
    if (rec.attempts or 0) >= 5:
        raise HTTPException(status_code=429, detail="TOO_MANY_ATTEMPTS")

    if _sha256_hex(code) != rec.code_hash:
        rec.attempts = (rec.attempts or 0) + 1
        await db.commit()
        raise HTTPException(status_code=400, detail="CODE_INVALID")

    # success: активируем
    rec.used_at = _now()
    user.is_email_verified = True
    user.is_active = True

    await db.commit()

    # создаём сессию + cookie
    raw_session_id = await _create_session(db, user.id, request)
    _set_session_cookie(response, raw_session_id)

    return MeOut(
        id=user.id,
        email=user.email,
        plan=user.plan,
        is_email_verified=user.is_email_verified,
        is_active=user.is_active,
        last_login_at=user.last_login_at,
    )


@router.post("/login", response_model=MeOut)
async def login(payload: LoginIn, request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    email = payload.email.strip().lower()
    password = payload.password or ""

    r = await db.execute(select(User).where(User.email == email))
    user = r.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    if not user.is_email_verified:
        raise HTTPException(status_code=403, detail="EMAIL_NOT_VERIFIED")

    if not user.is_active:
        raise HTTPException(status_code=403, detail="USER_INACTIVE")

    if not user.password_hash or not pwd_context.verify(password, user.password_hash):
        raise HTTPException(status_code=401, detail="INVALID_CREDENTIALS")

    # last_login_at
    user.last_login_at = _now()
    await db.commit()

    # создать сессию + cookie
    raw_session_id = await _create_session(db, user.id, request)
    _set_session_cookie(response, raw_session_id)

    return MeOut(
        id=user.id,
        email=user.email,
        plan=user.plan,
        is_email_verified=user.is_email_verified,
        is_active=user.is_active,
        last_login_at=user.last_login_at,
    )


@router.post("/logout")
async def logout(request: Request, response: Response, db: AsyncSession = Depends(get_db)):
    raw = request.cookies.get(COOKIE_NAME)
    if raw:
        session_hash = _sha256_hex(raw)
        await db.execute(
            update(Session)
            .where(Session.session_hash == session_hash, Session.revoked_at.is_(None))
            .values(revoked_at=_now())
        )
        await db.commit()

    _clear_session_cookie(response)
    return {"status": "ok"}


@router.get("/me", response_model=MeOut)
async def me(user: User = Depends(get_current_user_from_cookie)):
    return MeOut(
        id=user.id,
        email=user.email,
        plan=user.plan,
        is_email_verified=user.is_email_verified,
        is_active=user.is_active,
        last_login_at=user.last_login_at,
    )

@router.post("/check-email", response_model=CheckEmailOut)
async def check_email(payload: CheckEmailIn, db: AsyncSession = Depends(get_db)):
    email = payload.email.strip().lower()

    r = await db.execute(select(User.id).where(User.email == email))
    user_id = r.scalar_one_or_none()

    return CheckEmailOut(exists=bool(user_id))

get_current_user = get_current_user_from_cookie