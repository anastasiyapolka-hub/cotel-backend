from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request, Depends
from auth import router as auth_router
from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError
from diagnostics import router as diagnostics_router

import os
import math
import logging
import httpx
import json
import hashlib
import secrets

from datetime import datetime, date, timezone, timedelta
import time
import sqlalchemy as sa
import re

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.exc import IntegrityError

import asyncio

from llm.service import (
    summarize_chat_messages,
    summarize_chat_messages_group,
    classify_subscription_matches,
    build_subscription_digest,
    run_qa,
    run_qa_group,
)
from llm.adapters import LlmFatalError, LlmRetryableError
from llm.orchestrator import LlmAllModelsFailedError, LlmEmptyResponseError, routing_meta
from llm.pricing import get_token_rates

import billing
import subscription_billing

from db.models import (
    User,
    Subscription,
    SubscriptionChat,
    SubscriptionChatState,
    SubscriptionState,
    DigestEvent,
    MatchEvent,
    BotUserLink,
    UserChatHistory,
    BotLinkCode,
    Plan,
    UsageCounter,
    UsageEvent,
    UserQueryLog,
    TokenTransaction,
    UserTokenBalance,
    SavedQuery,
)

from db.session import get_db
from auth import get_current_user as auth_get_current_user
from schemas.subscriptions import SubscriptionCreate, SubscriptionOut, ToggleRequest
from schemas.saved_queries import SavedQueryCreate, SavedQueryUpdate, SavedQueryOut
from pydantic import BaseModel
from typing import Literal, List, Optional
from email_service import send_feedback_email, FEEDBACK_RECIPIENT_EMAIL
from service_account_routes import router as service_account_router
from service_account_admin_routes import router as service_account_admin_router
from admin_routes import router as admin_router

from collections import defaultdict
from datetime import datetime, date, timezone, timedelta

from telegram_service import (
    send_login_code,
    confirm_login,
    confirm_password,
    create_password_encryption_context,
    decrypt_password_ciphertext,
    get_current_user as tg_get_current_user,  # <-- переименовали
    fetch_chat_messages,
    list_user_chats,
    get_telegram_structure,
    logout_telegram,
    qr_login_start,
    qr_login_status,
    fetch_chat_messages_for_subscription,
    export_string_session,
    save_user_telegram_session,
)
from service_account_service import (
    ServiceAccountError,
    validate_service_subscription_target,
    fetch_service_chat_messages_for_subscription,
)
from plan_limits import (
    build_usage_snapshot,
    check_max_chats_or_raise,
    check_tier_allowed_or_raise,
    ensure_can_create_subscription,
    ensure_can_delete_subscription,
    ensure_can_toggle_subscription,
    ensure_can_update_subscription,
    enforce_qa_limits,
    get_user_plan,
    parse_period_from_payload,
    parse_date_range_from_payload,
    ensure_range_within_plan,
    record_qa_success,
    record_qa_failure,
    expire_trial_subscription_if_needed,
    resolve_ai_model_for_user,
    resolve_group_chats_limit,
)
from llm import (
    estimate_llm_cost_usd,
    split_usage_for_meta,
    cost_kwargs_for_meta,
    LlmUsage,
    TOKENS_SOURCE_EMPTY,
)
from media_filter import integration as mf_integration

class ChangePlanRequest(BaseModel):
    target_plan: Literal["free", "basic", "pro", "power"]

app = FastAPI()

ALLOWED_ORIGINS = [
    "https://cotel.onrender.com",
    "https://cotel-backend.onrender.com",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=r"^https://([a-z0-9-]+\.)*onrender\.com$",
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=[
        "Accept",
        "Accept-Language",
        "Authorization",
        "Content-Type",
        "Origin",
        "X-Requested-With",
    ],
    expose_headers=["*"],
    max_age=600,
)


# Fallback: ensure CORS headers are attached even when an unhandled exception
# occurs deep in a route (otherwise the browser sees "No CORS headers" and
# reports a misleading CORS error instead of the real 5xx).
@app.exception_handler(Exception)
async def _cors_safe_exception_handler(request, exc):
    from fastapi.responses import JSONResponse
    origin = request.headers.get("origin", "")
    headers = {}
    if origin in ALLOWED_ORIGINS or (
        origin.startswith("https://") and origin.endswith(".onrender.com")
    ):
        headers["Access-Control-Allow-Origin"] = origin
        headers["Access-Control-Allow-Credentials"] = "true"
        headers["Vary"] = "Origin"

    # Re-raise HTTPException so FastAPI's own handler formats it normally
    from fastapi import HTTPException
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=headers,
        )

    return JSONResponse(
        status_code=500,
        content={"detail": f"INTERNAL_ERROR: {type(exc).__name__}"},
        headers=headers,
    )
app.include_router(auth_router)
app.include_router(service_account_router)
app.include_router(service_account_admin_router)
app.include_router(diagnostics_router)
app.include_router(admin_router)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEV_OWNER_USER_ID = int(os.getenv("DEV_OWNER_USER_ID", "1"))


@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Feedback / bug report submission (from in-app "Обратная связь" modal).
#
# The recipient inbox is hardcoded as a constant in `email_service.py` —
# see FEEDBACK_RECIPIENT_EMAIL. Update it there to route feedback elsewhere.
# ---------------------------------------------------------------------------

FEEDBACK_ALLOWED_CATEGORIES = {
    "bug",
    "improvement",
    "support",
    "billing",
    "account",
    "other",
}
FEEDBACK_MAX_SUBJECT_LEN = 200
FEEDBACK_MAX_MESSAGE_LEN = 5000
FEEDBACK_MAX_FILES = 5
FEEDBACK_MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


@app.post("/feedback")
async def submit_feedback(
    subject: str = Form(...),
    category: str = Form("other"),
    message: str = Form(...),
    language: Optional[str] = Form(None),
    files: List[UploadFile] = File(default=[]),
    user: User = Depends(auth_get_current_user),
):
    subject = (subject or "").strip()
    message = (message or "").strip()
    category = (category or "other").strip().lower()

    if not subject:
        raise HTTPException(
            status_code=400,
            detail={"code": "FEEDBACK_SUBJECT_REQUIRED",
                    "message": "Заголовок заявки обязателен."},
        )
    if len(subject) > FEEDBACK_MAX_SUBJECT_LEN:
        raise HTTPException(
            status_code=400,
            detail={"code": "FEEDBACK_SUBJECT_TOO_LONG",
                    "message": "Заголовок слишком длинный."},
        )
    if not message:
        raise HTTPException(
            status_code=400,
            detail={"code": "FEEDBACK_MESSAGE_REQUIRED",
                    "message": "Текст обращения обязателен."},
        )
    if len(message) > FEEDBACK_MAX_MESSAGE_LEN:
        raise HTTPException(
            status_code=400,
            detail={"code": "FEEDBACK_MESSAGE_TOO_LONG",
                    "message": "Сообщение слишком длинное."},
        )
    if category not in FEEDBACK_ALLOWED_CATEGORIES:
        category = "other"

    safe_files = [f for f in (files or []) if f is not None]
    if len(safe_files) > FEEDBACK_MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail={"code": "FEEDBACK_TOO_MANY_FILES",
                    "message": "Можно прикрепить не более 5 файлов."},
        )

    attachments = []
    for upload in safe_files:
        try:
            content_bytes = await upload.read()
        except Exception:
            content_bytes = b""
        if not content_bytes:
            continue
        if len(content_bytes) > FEEDBACK_MAX_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "FEEDBACK_FILE_TOO_LARGE",
                    "message": "Один из файлов превышает допустимый размер (5 МБ).",
                    "filename": upload.filename or "",
                },
            )
        attachments.append({
            "filename": upload.filename or "attachment",
            "content_bytes": content_bytes,
            "content_type": upload.content_type or "application/octet-stream",
        })

    try:
        await send_feedback_email(
            user_email=user.email or "",
            subject=subject,
            category=category,
            message=message,
            attachments=attachments,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "FEEDBACK_SEND_FAILED",
                "message": "Не удалось отправить заявку. Попробуйте позже.",
                "error": type(exc).__name__,
            },
        )

    return {
        "ok": True,
        "recipient": FEEDBACK_RECIPIENT_EMAIL,
        "category": category,
    }

@app.get("/account/plan-usage")
async def get_account_plan_usage(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await build_usage_snapshot(db, user=user)


@app.get("/account/usage-history")
async def get_account_usage_history(
    limit: int = 50,
    offset: int = 0,
    mode: str = "chats",
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    История запросов пользователя для страницы «Мои запросы» в профиле.

    Возвращает список UsageEvent (Q&A + опц. подписки), отсортированных
    от свежих к старым. Для каждой записи отдаём минимальный набор полей,
    нужных UI: дата, tokens_charged, модель, tier, категория, тип события,
    chat_ref (без полного текста запроса — приватность).

    Параметры:
      limit   — макс. 100 записей за один вызов (default 50)
      offset  — пагинация
      mode    — режим списка (default "chats"):
                  "chats"         — только запросы к чатам (Q&A: успех + ошибки)
                  "subscriptions" — только срабатывания подписок
                История работает в двух взаимоисключающих режимах, чтобы
                длинный список было проще читать: подписочные тики могут идти
                раз в 15 минут и иначе забивают ленту запросов к чатам.

    Подгружается фронтом ТОЛЬКО при переходе на вкладку (не на каждый
    запрос плана). См. UX-обсуждение в архитектурном документе.
    """
    # Жёсткий потолок на limit — защита от случайных полных вытяжек
    limit = max(1, min(int(limit or 50), 100))
    offset = max(0, int(offset or 0))

    mode = (mode or "chats").lower()
    if mode == "subscriptions":
        event_types = ["subscription_run_success"]
    else:
        mode = "chats"
        event_types = ["qa_request_success", "qa_request_failure"]

    stmt = (
        select(UsageEvent)
        .where(UsageEvent.user_id == user.id)
        .where(UsageEvent.event_type.in_(event_types))
        .order_by(UsageEvent.created_at.desc())
        .limit(limit)
        .offset(offset)
    )

    rows = (await db.execute(stmt)).scalars().all()

    # Подстраховка для старых записей, у которых tokens_charged не попал в
    # meta_json (например, подписочные события до фикса _build_run_success_meta).
    # Фактическое списание лежит в token_transactions.related_event_id — берём
    # его одним батч-запросом и подставляем там, где meta пустая.
    event_ids = [int(ev.id) for ev in rows]
    charged_by_event: dict[int, int] = {}
    if event_ids:
        tx_stmt = (
            select(
                TokenTransaction.related_event_id,
                sa.func.sum(TokenTransaction.delta),
            )
            .where(TokenTransaction.user_id == user.id)
            .where(TokenTransaction.related_event_id.in_(event_ids))
            .group_by(TokenTransaction.related_event_id)
        )
        for related_id, delta_sum in (await db.execute(tx_stmt)).all():
            if related_id is not None:
                # delta списания отрицательная — отдаём положительное число
                charged_by_event[int(related_id)] = abs(int(delta_sum or 0))

    items: list[dict] = []
    for ev in rows:
        meta = ev.meta_json or {}
        tokens_charged = meta.get("tokens_charged")
        if tokens_charged is None:
            tokens_charged = charged_by_event.get(int(ev.id))
        items.append({
            "id": int(ev.id),
            "created_at": ev.created_at.isoformat() if ev.created_at else None,
            "event_type": ev.event_type,
            "status": ev.status,
            "source_mode": ev.source_mode,
            "chat_ref": ev.chat_ref,
            "subscription_id": ev.subscription_id,
            # tokens_charged: сначала из meta_json, иначе из token_transactions
            "tokens_charged": tokens_charged,
            "used_model": (
                meta.get("used_model")
                or meta.get("ai_model")  # legacy fallback
            ),
            "tier": meta.get("tier") or meta.get("depth"),
            "category": meta.get("category") or meta.get("final_category"),
            "was_fallback": meta.get("was_fallback"),
            "requested_days": meta.get("requested_days"),
            "messages_fetched_count": meta.get("messages_fetched_count"),
            "duration_ms_total": meta.get("duration_ms_total"),
            "duration_ms_llm": meta.get("duration_ms_llm"),
            "error_code": meta.get("error_code"),
        })

    # Сколько всего записей у пользователя — нужно для UI «показано N из M»
    total_stmt = (
        select(sa.func.count())
        .select_from(UsageEvent)
        .where(UsageEvent.user_id == user.id)
        .where(UsageEvent.event_type.in_(event_types))
    )
    total = int((await db.execute(total_stmt)).scalar_one() or 0)

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "mode": mode,
    }


# ---------------------------------------------------------------------------
# Top-up (докупка токенов) — STUB endpoint
# ---------------------------------------------------------------------------
#
# Заглушка для интеграции с платёжной системой (Stripe / другой).
# Сейчас возвращает 501 Not Implemented с описанием доступных пакетов.
# Когда подключим платёжку — заменим реализацию на полную:
#   1. Создать Stripe checkout session по выбранному пакету
#   2. Вернуть URL для редиректа
#   3. Stripe webhook на success → billing.apply_topup(...)
#
# Цены и количество токенов в пакетах — из architecture-router-and-credits.md
# раздел 2.3 (Small $5/1600, Medium $15/5500, Large $40/16000).
# ---------------------------------------------------------------------------

TOPUP_PACKAGES = {
    "small":  {"label": "Small",  "price_usd": 5.0,  "tokens": 1600,  "price_per_token_usd": 0.003125},
    "medium": {"label": "Medium", "price_usd": 15.0, "tokens": 5500,  "price_per_token_usd": 0.002727},
    "large":  {"label": "Large",  "price_usd": 40.0, "tokens": 16000, "price_per_token_usd": 0.002500},
}


@app.get("/account/tokens/topup-packages")
async def get_topup_packages(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Список доступных пакетов докупки токенов.

    Используется фронтом для построения страницы / модалки «Докупить».
    Также проверяет, разрешена ли докупка для тарифа пользователя
    (plan.topup_enabled) — на free возвращает 403.
    """
    from plan_limits import get_user_plan
    plan = await get_user_plan(db, user)
    if not bool(plan.topup_enabled):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TOPUP_NOT_AVAILABLE",
                "message": (
                    "Докупка токенов недоступна на вашем тарифе. "
                    "Перейдите на платный тариф для доступа."
                ),
                "plan_code": plan.code,
            },
        )

    return {
        "packages": [
            {"id": pkg_id, **info}
            for pkg_id, info in TOPUP_PACKAGES.items()
        ],
    }


@app.post("/account/tokens/topup")
async def initiate_topup(
    payload: dict,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    STUB: инициировать докупку выбранного пакета токенов.

    Финальная реализация:
      1. Validate package
      2. Создать Stripe checkout session
      3. Вернуть {checkout_url: "..."}

    Сейчас (MVP без платёжки): возвращает 501 с понятным сообщением,
    что функционал в разработке. Фронт показывает «Скоро будет доступно».

    Когда подключим платёжку — заменим тело на реальную реализацию.
    Структура endpoint'а и payload'а останется той же:
      POST /account/tokens/topup
        body: { "package": "small" | "medium" | "large" }
    """
    from plan_limits import get_user_plan

    package_id = str(payload.get("package") or "").strip().lower()
    if package_id not in TOPUP_PACKAGES:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_PACKAGE",
                "message": "Неизвестный пакет докупки.",
                "available_packages": list(TOPUP_PACKAGES.keys()),
            },
        )

    plan = await get_user_plan(db, user)
    if not bool(plan.topup_enabled):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TOPUP_NOT_AVAILABLE",
                "message": "Докупка токенов недоступна на вашем тарифе.",
            },
        )

    # 501 Not Implemented — платёжная система ещё не подключена
    raise HTTPException(
        status_code=501,
        detail={
            "code": "PAYMENT_PROVIDER_NOT_CONFIGURED",
            "message": (
                "Функционал докупки токенов находится в разработке. "
                "Скоро будет доступно."
            ),
            "requested_package": package_id,
            "package_info": TOPUP_PACKAGES[package_id],
        },
    )

@app.post("/account/change-plan")
async def change_account_plan(
    payload: ChangePlanRequest,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    target_plan = str(payload.target_plan).strip().lower()

    plan_res = await db.execute(
        select(Plan).where(
            Plan.code == target_plan,
            Plan.is_active == True,  # noqa: E712
        )
    )
    plan_row = plan_res.scalar_one_or_none()
    if not plan_row:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "PLAN_NOT_FOUND",
                "message": "Выбранный тариф не найден или неактивен.",
            },
        )

    old_plan = str(user.plan or "").lower()

    user.plan = target_plan
    user.updated_at = sa.func.now()

    # Сбрасываем только счётчики Q&A
    await db.execute(
        delete(UsageCounter).where(
            UsageCounter.user_id == user.id,
            UsageCounter.metric_code == "qa_request",
        )
    )

    # === Токенный баланс под новый тариф ===
    # При смене тарифа месячный грант пересчитывается на порог нового
    # тарифа, а monthly_used обнуляется. То есть после апгрейда пользователь
    # сразу получает увеличенный лимит, после даунгрейда — ровно столько,
    # сколько даёт новый тариф (например, Basic → 3600), независимо от
    # текущего остатка. topup_balance не трогаем — купленные токены не
    # сгорают. См. billing.apply_plan_change.
    now_utc = datetime.now(timezone.utc)
    await billing.apply_plan_change(
        db,
        user_id=user.id,
        new_plan_monthly_tokens=int(plan_row.monthly_tokens or 0),
        period_start=date(now_utc.year, now_utc.month, 1),
        old_plan_code=old_plan,
        new_plan_code=target_plan,
    )

    # Баланс пополнен под новый тариф → возобновляем подписки, снятые из-за
    # нехватки токенов, и сбрасываем флаг разового уведомления.
    await subscription_billing.clear_low_balance_notified(db, user_id=user.id)
    await subscription_billing.resume_user_subscriptions_low_balance(
        db, user_id=user.id, now_utc=now_utc,
    )

    db.add(
        UsageEvent(
            user_id=user.id,
            event_type="plan_changed_manual",
            status="success_counted",
            meta_json={
                "old_plan": old_plan,
                "new_plan": target_plan,
                "new_monthly_tokens": int(plan_row.monthly_tokens or 0),
            },
        )
    )

    await db.commit()
    await db.refresh(user)

    return {
        "ok": True,
        "message": f"Тариф изменён на {target_plan}.",
        "user": {
            "id": user.id,
            "email": user.email,
            "plan": user.plan,
            "is_email_verified": user.is_email_verified,
            "is_active": user.is_active,
            "country_code": user.country_code,
            "language": user.language,
            "language_source": user.language_source,
            "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
            "phone": user.phone,
        },
        "usage": await build_usage_snapshot(db, user=user),
    }

def sha256_hex(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def make_link_code() -> str:
    # короткий, но достаточно случайный
    return secrets.token_urlsafe(12)

def normalize_chat_ref_for_history(chat_ref: str) -> str:
    ref = (chat_ref or "").strip()
    if not ref:
        return ""

    ref = ref.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    ref = ref.strip("/")

    if ref.startswith("@"):
        ref = ref[1:].strip()

    return ref.lower()


async def upsert_user_chat_history(
    db: AsyncSession,
    *,
    owner_user_id: int,
    source_mode: str,
    chat_ref: str,
    chat_title: str | None = None,
    chat_username: str | None = None,
    chat_id: int | None = None,
) -> None:
    normalized_ref = normalize_chat_ref_for_history(chat_ref)
    if not normalized_ref:
        return

    display_ref = (chat_ref or "").strip()

    stmt = (
        insert(UserChatHistory)
        .values(
            owner_user_id=owner_user_id,
            source_mode=source_mode,
            chat_ref=display_ref,
            chat_ref_normalized=normalized_ref,
            chat_title=(chat_title or "").strip() or None,
            chat_username=(chat_username or "").strip() or None,
            chat_id=chat_id,
            last_accessed_at=sa.func.now(),
        )
        .on_conflict_do_update(
            constraint="uq_user_chat_history_owner_source_ref",
            set_={
                "chat_ref": display_ref,
                "chat_title": (chat_title or "").strip() or None,
                "chat_username": (chat_username or "").strip() or None,
                "chat_id": chat_id,
                "last_accessed_at": sa.func.now(),
                "updated_at": sa.func.now(),
            },
        )
    )

    await db.execute(stmt)


def serialize_chat_history_row(row: UserChatHistory) -> dict:
    return {
        "id": row.id,
        "owner_user_id": row.owner_user_id,
        "source_mode": row.source_mode,
        "chat_ref": row.chat_ref,
        "chat_ref_normalized": row.chat_ref_normalized,
        "chat_title": row.chat_title,
        "chat_username": row.chat_username,
        "chat_id": row.chat_id,
        "last_accessed_at": row.last_accessed_at.isoformat() if row.last_accessed_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }

class SubscriptionSwitchModeRequest(BaseModel):
    target_source_mode: Literal["personal", "service"]

async def prepare_subscription_target(
    db: AsyncSession,
    *,
    owner_user_id: int,
    source_mode: str,
    chat_ref: str,
) -> tuple[str, int | None, str | None, str | None]:
    """
    Возвращает:
      normalized_chat_ref_for_save,
      chat_id,
      chat_title,
      chat_username
    """
    source_mode = (source_mode or "personal").strip().lower()
    chat_ref = (chat_ref or "").strip()

    if source_mode not in {"personal", "service"}:
        raise HTTPException(status_code=400, detail="INVALID_SOURCE_MODE")

    if not chat_ref:
        raise HTTPException(status_code=400, detail="CHAT_REF_REQUIRED")

    if source_mode == "personal":
        try:
            entity, _ = await fetch_chat_messages_for_subscription(
                db=db,
                owner_user_id=owner_user_id,
                chat_link=chat_ref,
                since_dt=datetime.now(timezone.utc) - timedelta(days=1),
                min_id=None,
                limit=1,
            )
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"CHAT_VALIDATE_FAILED: {str(e)}")

        chat_title = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or "Без названия"
        )
        chat_username = getattr(entity, "username", None)
        chat_id = getattr(entity, "id", None)

        return chat_ref, int(chat_id) if chat_id is not None else None, chat_title, chat_username

    # service
    try:
        meta = await validate_service_subscription_target(db, chat_link=chat_ref)
    except ServiceAccountError as e:
        raise HTTPException(
            status_code=e.http_status,
            detail={
                "code": e.code,
                "message": e.user_message,
            },
        )

    normalized_ref = (meta.get("chat_ref_normalized") or chat_ref).strip()
    chat_id = meta.get("chat_id")
    chat_title = meta.get("chat_name")
    chat_username = meta.get("chat_username")

    return normalized_ref, int(chat_id) if chat_id is not None else None, chat_title, chat_username

async def reset_subscription_state(
    db: AsyncSession,
    *,
    subscription_id: int,
) -> None:
    res = await db.execute(
        select(SubscriptionState).where(SubscriptionState.subscription_id == subscription_id)
    )
    st = res.scalar_one_or_none()

    if st is None:
        st = SubscriptionState(subscription_id=subscription_id)
        db.add(st)

    st.last_message_id = None
    st.last_checked_at = None
    st.last_success_at = None
    st.next_run_at = None


# ---------------------------------------------------------------------------
# Подготовка списка чатов для ГРУППОВОЙ подписки.
#
# Валидируем каждый чат (через тот же fetch_chat_messages_for_subscription,
# что и одиночная подписка), собираем resolved-метаданные. Если хотя бы
# один чат невалиден — поднимаем 400 с массивом invalid_chats; подписку
# НЕ сохраняем (партиальная подписка с битыми чатами никому не нужна).
#
# Доступно только для personal source_mode — для service групповые
# подписки запрещены (на фронте галочка спрятана; здесь на всякий случай
# отбиваем 400). Здесь же отбиваем free-плана (на фронте тоже спрятано).
# ---------------------------------------------------------------------------
async def prepare_subscription_group_targets(
    db: AsyncSession,
    *,
    owner_user_id: int,
    user: User,
    source_mode: str,
    chat_refs: list[str],
) -> list[dict]:
    """
    Возвращает список dict со ключами:
      chat_ref (нормализованный для сохранения),
      chat_id, chat_title, chat_username.
    Порядок сохраняется ровно как пришёл (фронт даёт порядок выбора юзером).
    """
    mode = (source_mode or "personal").strip().lower()
    if mode != "personal":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GROUP_SUBSCRIPTION_REQUIRES_PERSONAL",
                "message": "Групповые подписки доступны только для личного Telegram-аккаунта.",
            },
        )

    plan_code = str(getattr(user, "plan", "") or "").strip().lower()
    if plan_code == "free":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "GROUP_SUBSCRIPTION_NOT_ALLOWED_FOR_FREE",
                "message": "Групповые подписки недоступны на тарифе Free.",
            },
        )

    # Нормализуем + дедупим, сохраняя порядок (Q&A group делает так же).
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw in chat_refs or []:
        s = str(raw or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        cleaned.append(s)

    if not cleaned:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GROUP_EMPTY",
                "message": "Не выбрано ни одного чата для групповой подписки.",
            },
        )

    group_limit = resolve_group_chats_limit(plan_code)
    if len(cleaned) > group_limit:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GROUP_CHATS_LIMIT_EXCEEDED",
                "message": (
                    f"Ваш тариф разрешает не более {group_limit} чатов "
                    f"в одной групповой подписке."
                ),
                "group_chats_limit": group_limit,
                "requested": len(cleaned),
            },
        )

    # Валидируем чаты ПОСЛЕДОВАТЕЛЬНО: в personal-mode у юзера одна
    # StringSession, параллельный fetch к 20 чатам провоцирует FLOOD_WAIT.
    # Это операция разовая (при создании/редактировании), скорость не критична.
    resolved: list[dict] = []
    invalid: list[dict] = []

    for chat_ref in cleaned:
        try:
            entity, _ = await fetch_chat_messages_for_subscription(
                db=db,
                owner_user_id=owner_user_id,
                chat_link=chat_ref,
                since_dt=datetime.now(timezone.utc) - timedelta(days=1),
                min_id=None,
                limit=1,
            )
        except ValueError as ve:
            invalid.append({"chat_ref": chat_ref, "error": str(ve)})
            continue
        except Exception as e:
            invalid.append({"chat_ref": chat_ref, "error": f"CHAT_VALIDATE_FAILED: {str(e)}"})
            continue

        chat_title = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or "Без названия"
        )
        resolved.append({
            "chat_ref": chat_ref,
            "chat_id": int(getattr(entity, "id", None)) if getattr(entity, "id", None) is not None else None,
            "chat_title": chat_title,
            "chat_username": getattr(entity, "username", None),
        })

    if invalid:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GROUP_INVALID_CHATS",
                "message": (
                    f"Не удалось добавить {len(invalid)} из {len(cleaned)} чатов. "
                    f"Удалите проблемные чаты из списка."
                ),
                "invalid_chats": invalid,
            },
        )

    return resolved


def _normalize_group_marker(sub_id: int) -> str:
    """Синтетический chat_ref для групповой подписки — пишем в Subscription.chat_ref.
    Реальный список чатов хранится в subscription_chats."""
    return f"group:{int(sub_id)}"


async def _replace_subscription_chats(
    db: AsyncSession,
    *,
    subscription_id: int,
    resolved_chats: list[dict],
    reset_state_for_new_chats_only: bool,
) -> None:
    """
    Полностью переписывает subscription_chats для подписки.
    Для subscription_chat_state:
      - если reset_state_for_new_chats_only=True (UPDATE):
        состояние для оставшихся чатов сохраняем (курсор НЕ сбрасываем),
        для удалённых — удаляем, для новых — создаём пустое.
      - если False (CREATE): просто создаём пустые state-записи.
    """
    new_keys = {c["chat_ref"] for c in resolved_chats}

    if reset_state_for_new_chats_only:
        # Получаем текущее состояние
        existing_states_res = await db.execute(
            select(SubscriptionChatState.chat_key).where(
                SubscriptionChatState.subscription_id == subscription_id
            )
        )
        existing_keys = {row[0] for row in existing_states_res.all()}

        # Удалим state для исчезнувших чатов
        to_remove = existing_keys - new_keys
        if to_remove:
            await db.execute(
                delete(SubscriptionChatState).where(
                    SubscriptionChatState.subscription_id == subscription_id,
                    SubscriptionChatState.chat_key.in_(to_remove),
                )
            )
        keys_to_create = new_keys - existing_keys
    else:
        keys_to_create = new_keys

    # Полностью пересоздаём subscription_chats (порядок чатов мог поменяться)
    await db.execute(
        delete(SubscriptionChat).where(SubscriptionChat.subscription_id == subscription_id)
    )

    for position, c in enumerate(resolved_chats):
        # Truncate VARCHAR-полей перед INSERT: Telegram-чаты с эмодзи
        # легко превышают 255/128 символов.
        ct = c.get("chat_title")
        cu = c.get("chat_username")
        if ct is not None and len(ct) > 255:
            ct = ct[:255]
        if cu is not None and len(cu) > 128:
            cu = cu[:128]
        db.add(SubscriptionChat(
            subscription_id=subscription_id,
            position=position,
            chat_ref=c["chat_ref"],
            chat_id=c.get("chat_id"),
            chat_title=ct,
            chat_username=cu,
        ))

    # Создаём state-записи для новых чатов
    for c in resolved_chats:
        if c["chat_ref"] in keys_to_create:
            db.add(SubscriptionChatState(
                subscription_id=subscription_id,
                chat_key=c["chat_ref"],
                last_message_id=None,
                last_success_at=None,
                last_error=None,
            ))


async def _load_subscription_chats(
    db: AsyncSession,
    *,
    subscription_id: int,
) -> list[dict]:
    """Подгрузить список чатов для групповой подписки в виде списка
    dict-ов, отсортированных по position. Используется в SubscriptionOut."""
    res = await db.execute(
        select(SubscriptionChat)
        .where(SubscriptionChat.subscription_id == subscription_id)
        .order_by(SubscriptionChat.position.asc())
    )
    rows = list(res.scalars().all())
    return [
        {
            "chat_ref": r.chat_ref,
            "chat_id": r.chat_id,
            "chat_title": r.chat_title,
            "chat_username": r.chat_username,
            "position": r.position,
        }
        for r in rows
    ]


def _serialize_subscription(sub: Subscription, chats: list[dict] | None = None) -> dict:
    """Превратить Subscription + опциональный список чатов в dict,
    пригодный для SubscriptionOut. Используется в эндпоинтах, где
    нужно вернуть подписку с подгруженным списком чатов."""
    base = {
        "id": sub.id,
        "owner_user_id": sub.owner_user_id,
        "name": sub.name,
        "source_mode": sub.source_mode,
        "chat_ref": sub.chat_ref,
        "chat_id": sub.chat_id,
        "frequency_minutes": sub.frequency_minutes,
        "prompt": sub.prompt,
        "ai_model": sub.ai_model,
        "is_active": sub.is_active,
        "status": sub.status,
        "last_error": sub.last_error,
        "created_at": sub.created_at,
        "updated_at": sub.updated_at,
        "subscription_type": sub.subscription_type,
        "media_filter": sub.media_filter,
        "is_group": bool(getattr(sub, "is_group", False)),
        "chats": chats,
        "is_trial": bool(getattr(sub, "is_trial", False)),
        "trial_started_at": sub.trial_started_at,
        "trial_ends_at": sub.trial_ends_at,
    }
    return base

def extract_text_messages(messages, limit: int = 100000):
    """
    Берём только текстовые сообщения (type == 'message'),
    аккуратно разворачиваем поле text (оно может быть строкой или списком),
    и возвращаем последние `limit` штук.
    """
    text_msgs = []

    for m in messages:
        if not isinstance(m, dict):
            continue
        if m.get("type") != "message":
            continue

        text = m.get("text", "")

        # В экспортировании Telegram text иногда список (строчки + объекты форматирования)
        if isinstance(text, list):
            parts = []
            for item in text:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
            text = "".join(parts)

        if not isinstance(text, str):
            continue

        text = text.strip()
        if not text:
            continue

        text_msgs.append({
            "date": m.get("date"),
            "from": m.get("from"),
            "text": text,
        })

    # берём только последние limit сообщений
    return text_msgs[-limit:]

def parse_iso_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        # поддержка "Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None

def build_tg_message_link(chat_ref: str | None, chat_id: int | None, message_id: int | None) -> str | None:
    if not message_id:
        return None

    ref = (chat_ref or "").strip()

    # 1) username из @username
    if ref.startswith("@") and len(ref) > 1:
        uname = ref[1:]
        return f"https://t.me/{uname}/{message_id}"

    # 2) username из t.me/username или https://t.me/username
    m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{3,})", ref)
    if m:
        uname = m.group(1)
        # если это invite-ссылка вида t.me/+HASH — не подойдет
        if not uname.startswith("+"):
            return f"https://t.me/{uname}/{message_id}"

    # 3) приватный супергрупповой линк через /c/
    if chat_id:
        aid = abs(int(chat_id))
        s = str(aid)
        # Формат бот-API: -100XXXXX → abs → "100XXXXX". Срезаем "100".
        if s.startswith("100") and len(s) > 3:
            internal = s[3:]
            return f"https://t.me/c/{internal}/{message_id}"
        # Формат Telethon: entity.id у каналов/супергрупп — это «сырое»
        # положительное число без префикса -100 (например, 1900836903).
        # subscriptions_runner сохраняет его как есть в sub.chat_id, и
        # без этой ветки media-подписки на приватные/числовые чаты
        # оставались без рабочих ссылок. Для каналов id всегда «крупное»
        # число; защищаемся от случайных малых id (User entity) порогом.
        if aid > 10_000_000:
            return f"https://t.me/c/{s}/{message_id}"

    return None

@app.post("/tg/bot/link/start")
async def tg_bot_link_start(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # 1) генерим код
    code = make_link_code()
    code_hash = sha256_hex(code)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    # 2) сохраняем в bot_link_codes
    db.add(BotLinkCode(
        user_id=user.id,
        code_hash=code_hash,
        expires_at=expires_at,
        used_at=None,
    ))
    await db.commit()

    # 3) вернём код + deeplink (удобно для UI)
    bot_username = os.getenv("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@")
    if not bot_username:
        # На случай, если переменная окружения не выставлена в проде, чтобы фронт не сломался.
        # Фронт всегда получит рабочий deeplink, а мы увидим в логах, что env не выставлен.
        print("WARN: TELEGRAM_BOT_USERNAME is not set, falling back to CoTel_AlertBot")
        bot_username = "CoTel_AlertBot"

    deeplink = f"https://t.me/{bot_username}?start={code}"

    return {
        "status": "ok",
        "code": code,
        "expires_at": expires_at.isoformat(),
        "bot_username": bot_username,
        "deeplink": deeplink,
    }

def _serialize_match_event(ev) -> dict:
    # ev = MatchEvent ORM object
    return {
        "id": ev.id,
        "subscription_id": ev.subscription_id,
        "message_id": ev.message_id,
        "message_ts": ev.message_ts.isoformat() if ev.message_ts else None,
        "author_id": ev.author_id,
        "author_display": ev.author_display,
        "excerpt": ev.excerpt,
        "reason": ev.reason,
        "llm_payload": ev.llm_payload,
        "notify_status": ev.notify_status,
        "created_at": ev.created_at.isoformat() if ev.created_at else None,
    }


@app.post("/subscriptions/run")
async def run_subscriptions(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    t0 = time.perf_counter()
    run_started_at = datetime.now(timezone.utc)
    now = run_started_at

    owner_user_id = user.id

    # 1) Берём активные подписки ТОЛЬКО этого пользователя
    now_utc = datetime.now(timezone.utc)

    res = await db.execute(
        select(Subscription).where(
            Subscription.is_active == True,
            Subscription.owner_user_id == owner_user_id,
            sa.or_(
                Subscription.is_trial == False,  # noqa: E712
                Subscription.trial_ends_at.is_(None),
                Subscription.trial_ends_at > now_utc,
            ),
        )
    )
    subs = list(res.scalars().all())

    results = []
    total_checked = 0
    total_matches = 0

    for sub in subs:

        expired = await expire_trial_subscription_if_needed(db, sub=sub, now_utc=now_utc)
        if expired:
            await db.commit()
            continue

        sub_report = {
            "subscription_id": sub.id,
            "name": getattr(sub, "name", None),
            "chat_ref": getattr(sub, "chat_ref", None),
            "status": "ok",
            "checked": 0,
            "matches_written": 0,
            "error": None,
            "llm_json": None,
            "llm_found": None,
            "llm_confidence": None,
            "llm_summary_reason": None,
            "llm_matches_count": 0,
            "inserted_message_ids": [],
            "match_events": [],
        }

        try:
            st_res = await db.execute(
                select(SubscriptionState).where(SubscriptionState.subscription_id == sub.id)
            )
            st = st_res.scalar_one_or_none()
            last_message_id = getattr(st, "last_message_id", None) if st else None

            freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)

            if last_message_id:
                since_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
                min_id = int(last_message_id)
            else:
                since_dt = now - timedelta(minutes=freq_min)
                min_id = None

            sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
            sub_report["subscription_type"] = sub_type

            source_mode = (getattr(sub, "source_mode", None) or "personal").lower()

            if source_mode == "service":
                entity, msgs = await fetch_service_chat_messages_for_subscription(
                    db=db,
                    chat_link=sub.chat_ref,
                    since_dt=since_dt,
                    min_id=min_id,
                    limit=1000,
                )
            else:
                entity, msgs = await fetch_chat_messages_for_subscription(
                    db,
                    owner_user_id,
                    chat_link=sub.chat_ref,
                    since_dt=since_dt,
                    min_id=min_id,
                    limit=1000,
                )

            # map для восстановления автора/времени по message_id
            msg_by_id = {}
            for mm in msgs:
                try:
                    mid0 = mm.get("message_id")
                    if mid0 is not None:
                        msg_by_id[int(mid0)] = mm
                except Exception:
                    continue

            if getattr(sub, "chat_id", None) is None:
                ent_id = getattr(entity, "id", None)
                if ent_id is not None:
                    sub.chat_id = int(ent_id)
                    await db.flush()

            checked = len(msgs)
            sub_report["checked"] = checked
            total_checked += checked

            # 5) newest_id
            ids = []
            for m in msgs:
                if isinstance(m, dict) and m.get("message_id") is not None:
                    ids.append(int(m["message_id"]))
            newest_id = max(ids) if ids else last_message_id

            matches_written = 0
            inserted_message_ids: list[int] = []

            # 6) LLM — только если есть что анализировать
            if checked > 0:
                chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"

                if sub_type == "events":
                    llm_json = await classify_subscription_matches(
                        prompt=sub.prompt,
                        chat_title=chat_title,
                        messages=msgs,
                        ux_language=user.language,
                        ai_model=sub.ai_model,
                    )

                    sub_report["llm_found"] = bool(llm_json.get("found")) if isinstance(llm_json, dict) else None
                    sub_report["llm_confidence"] = llm_json.get("confidence") if isinstance(llm_json, dict) else None
                    sub_report["llm_summary_reason"] = llm_json.get("summary_reason") if isinstance(llm_json,
                                                                                                    dict) else None
                    sub_report["llm_matches_count"] = len(llm_json.get("matches") or []) if isinstance(llm_json,
                                                                                                       dict) else 0

                    sub_report["llm_json"] = llm_json

                    found = bool(llm_json.get("found"))
                    matches = llm_json.get("matches") or []

                    if found and isinstance(matches, list):
                        for m in matches:
                            mid = m.get("message_id")
                            if not mid:
                                continue

                            # ВАЖНО: message_ts должен быть datetime, не строка
                            # (у тебя уже должен быть parse_dt/parse_iso_dt — используй его)
                            src = msg_by_id.get(int(mid))

                            # timestamp: приоритет — исходное сообщение, fallback — LLM (если вдруг нужно)
                            ts = None
                            try:
                                if src and src.get("message_ts"):
                                    ts = parse_iso_ts(src.get("message_ts"))
                                else:
                                    ts = parse_iso_ts(m.get("message_ts"))
                            except Exception:
                                ts = None

                            # author: строго из исходного сообщения
                            author_id = None
                            author_display = None
                            if src:
                                author_id = src.get("author_id")
                                author_display = src.get("author_display")

                            # excerpt: можно брать из LLM (как “цитату до 300”), но если хочешь “не коверкать” — бери из src["text"]
                            excerpt = (m.get("excerpt") or "").strip()
                            if not excerpt and src:
                                excerpt = (src.get("text") or "").strip()
                            if len(excerpt) > 300:
                                excerpt = excerpt[:300].rstrip() + "…"

                            stmt = (
                                insert(MatchEvent)
                                .values(
                                    subscription_id=sub.id,
                                    message_id=int(mid),
                                    message_ts=ts,
                                    author_id=author_id,
                                    author_display=author_display,
                                    excerpt=excerpt,
                                    reason=m.get("reason"),
                                    llm_payload={},  # ты убрала payload — оставляем так
                                    notify_status="queued",
                                )
                                # constraint= не указываем: миграция group_subscriptions
                                # заменила старый UNIQUE-constraint на функциональный
                                # UNIQUE INDEX (uq_match_sub_chat_msg с COALESCE),
                                # который не является constraint в смысле PG. Пустой
                                # ON CONFLICT DO NOTHING ловит любой unique-конфликт.
                                .on_conflict_do_nothing()
                            )

                            try:
                                r = await db.execute(stmt)
                                if getattr(r, "rowcount", 0) == 1:
                                    matches_written += 1
                                    inserted_message_ids.append(int(mid))

                            except Exception as e:
                                # не валим всю подписку из-за одного матча
                                print("MATCH_INSERT_FAILED", sub.id, mid, str(e))
                                continue

                elif sub_type == "digest":
                    # заглушка на сейчас
                    sub_report["status"] = "todo"
                    sub_report["error"] = "DIGEST_NOT_IMPLEMENTED_YET"
                else:
                    sub_report["status"] = "error"
                    sub_report["error"] = f"UNKNOWN_SUBSCRIPTION_TYPE: {sub_type}"

            sub_report["inserted_message_ids"] = inserted_message_ids
            sub_report["matches_written"] = matches_written
            total_matches += matches_written

            # 7) Обновляем state
            if st is None:
                st = SubscriptionState(subscription_id=sub.id)

            st.last_checked_at = now
            if newest_id:
                st.last_message_id = int(newest_id)
                st.last_success_at = now

            db.add(st)

            # 8) Обновим подписку “ok”
            await db.execute(
                update(Subscription)
                .where(Subscription.id == sub.id)
                .values(status="ok", last_error=None, updated_at=sa.func.now())
            )

            await db.commit()

            # 9) Достаём из БД ровно те MatchEvent, которые реально вставили (без зависимости от времени БД)
            if inserted_message_ids:
                ev_res = await db.execute(
                    select(MatchEvent)
                    .where(
                        MatchEvent.subscription_id == sub.id,
                        MatchEvent.message_id.in_(inserted_message_ids),
                    )
                    .order_by(MatchEvent.message_id.asc())
                )
                evs = list(ev_res.scalars().all())
                sub_report["match_events"] = [_serialize_match_event(ev) for ev in evs]
            else:
                sub_report["match_events"] = []

        except Exception as e:
            sub_report["status"] = "error"
            sub_report["error"] = str(e)

            # на всякий — статус подписки тоже отметим
            try:
                await db.execute(
                    update(Subscription)
                    .where(Subscription.id == sub.id)
                    .values(status="error", last_error=str(e), updated_at=sa.func.now())
                )
                await db.commit()
            except Exception:
                pass

        results.append(sub_report)

    elapsed = round(time.perf_counter() - t0, 2)

    # DEBUG: все строки match_events (лимит), чтобы смотреть что реально в БД
    all_ev_res = await db.execute(
        select(MatchEvent).order_by(MatchEvent.created_at.desc()).limit(200)
    )
    all_evs = list(all_ev_res.scalars().all())
    debug_all_match_events = [_serialize_match_event(ev) for ev in all_evs]

    return {
        "status": "ok",
        "processed_subscriptions": len(subs),
        "checked_messages": total_checked,
        "found_matches": total_matches,
        "elapsed_seconds": elapsed,
        "ui_message": f"Проверено {total_checked} сообщений, найдено {total_matches}",
        "results": results,
        "debug_all_match_events": debug_all_match_events,
    }



@app.post("/analyze")
async def analyze_chat(
    file: UploadFile = File(...),
    params: str = Form("{}"),
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # 1. парсим params из фронта
    try:
        params_dict = json.loads(params or "{}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="PARAMS_INVALID_JSON")

    # поддерживаем и "query", и "user_query" на всякий случай
    user_query = (
        (params_dict.get("user_query") or params_dict.get("query") or "").strip()
    )
    result_type = params_dict.get("result_type", "summary")

    requested_days = 7  # для JSON-экспорта пока считаем как базовое Q&A
    await enforce_qa_limits(
        db,
        user=user,
        requested_days=requested_days,
        source_mode="file",
        chat_ref=(file.filename or "").strip(),
    )

    # 1. Проверяем расширение файла
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TG_JSON_EXPORT_REQUIRED",
                "message": "Ожидается JSON-файл экспорта Telegram (.json).",
            },
        )

    # 2. Читаем файл в память
    raw_bytes = await file.read()

    # 3. Пробуем распарсить JSON
    try:
        data = json.loads(raw_bytes)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TG_JSON_INVALID",
                "message": "Файл не является корректным JSON.",
            },
        )

    # 4. Проверка структуры Telegram экспорта (опционально)
    messages = data.get("messages")
    if messages is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TG_JSON_NO_MESSAGES",
                "message": "JSON не содержит поле 'messages'. Возможно, экспорт выполнен в HTML-формате.",
            },
        )

    if not isinstance(messages, list):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TG_JSON_MESSAGES_NOT_LIST",
                "message": "Поле 'messages' должно быть списком сообщений.",
            },
        )

    # 📌 Извлекаем имя чата
    chat_name = data.get("name") or data.get("title") or "Без названия"

    # 📌 Извлекаем тип чата (сырой) и маппим в человекочитаемый русский
    raw_type = (data.get("type") or "").lower()

    type_map = {
        "personal_chat": "Личный чат",
        "private": "Личный чат",
        "group": "Группа",
        "supergroup": "Супергруппа",
        "channel": "Канал",
    }

    chat_type = type_map.get(raw_type, "Чат")

    # Количество сообщений
    messages_count = len(messages)

    # 5. подготавливаем текстовые сообщения для LLM
    text_messages = extract_text_messages(messages, limit=400)

    summary = None
    # Пока у нас один режим — произвольный запрос → summary
    if user_query:
        try:
            summary = await summarize_chat_messages(
                user_query=user_query,
                chat_name=chat_name,
                text_messages=text_messages,
                fallback_language=user.language,
            )
        except Exception as e:
            # Чтобы фронт получил понятную ошибку
            raise HTTPException(status_code=500, detail=f"LLM_ERROR: {str(e)}")


    await record_qa_success(
        db,
        user=user,
        source_mode="file",
        chat_ref=(file.filename or "").strip(),
        requested_days=requested_days,
    )
    await db.commit()

    usage_snapshot = await build_usage_snapshot(db, user=user)
    # Ответ фронту
    return {
        "status": "ok",
        "message": "Анализ выполнен",
        "filename": file.filename,
        "messages_count": messages_count,
        "chat_name": chat_name,
        "chat_type": chat_type,
        "user_query": user_query,
        "result_type": result_type,
        "usage": usage_snapshot,
        "summary": summary
    }

@app.post("/tg/send_code")
async def tg_send_code(payload: dict, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    owner_user_id = user.id

    phone = (payload.get("phone") or "").strip()
    print(f"[TG SEND CODE] phone received by backend: {phone!r}")
    if not phone:
        raise HTTPException(400, "PHONE_REQUIRED")
    try:
        print(f"[TG SEND CODE] calling Telethon send_code_request with phone: {phone!r}")
        await send_login_code(db, owner_user_id, phone)
    except Exception as e:
        print(f"[TG SEND CODE] ERROR type={type(e).__name__} repr={e!r} phone={phone!r}")
        raise HTTPException(status_code=400, detail=f"TELEGRAM_ERROR: {e}")
    return {"status": "code_sent"}

@app.post("/tg/confirm_code")
async def tg_confirm_code(payload: dict, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    owner_user_id = user.id
    try:
        phone = (payload.get("phone") or "").strip()
        code = (payload.get("code") or "").strip()

        if not phone or not code:
            raise HTTPException(
                status_code=400,
                detail="PHONE_AND_CODE_REQUIRED"
            )

        try:
            # подтверждаем код
            await confirm_login(db, owner_user_id, phone, code)

            # сохранить string session в БД
            ss = await export_string_session(db, owner_user_id)
            await save_user_telegram_session(db, owner_user_id, ss)

            # получаем текущего пользователя
            me = await tg_get_current_user(db, owner_user_id)

        except ValueError as ve:

            err = str(ve)

            if err == "PHONE_CODE_INVALID":
                raise HTTPException(status_code=400, detail="PHONE_CODE_INVALID")

            if err == "PASSWORD_NEEDED":
                raise HTTPException(status_code=400, detail="SESSION_PASSWORD_NEEDED")

            raise HTTPException(status_code=400, detail=f"TELEGRAM_ERROR: {err}")

        return {
            "status": "authorized",
            "me": {
                "id": me.id,
                "username": me.username,
                "first_name": me.first_name,
                "last_name": getattr(me, "last_name", None),
                "phone": me.phone,
            }
        }

    except HTTPException:
        # даём FastAPI вернуть нормальный ответ + CORS
        raise

    except Exception as e:
        # ловим ВСЁ остальное, чтобы не было "No CORS headers"
        raise HTTPException(
            status_code=400,
            detail=f"TG_CONFIRM_FAILED: {str(e)}"
        )

@app.post("/tg/password_encryption/start")
async def tg_password_encryption_start(
    user: User = Depends(auth_get_current_user),
):
    owner_user_id = user.id

    data = create_password_encryption_context(owner_user_id)
    return {
        "status": "ok",
        **data,
    }

@app.post("/tg/confirm_password")
async def tg_confirm_password(
    payload: dict,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        context_id = (payload.get("encryption_context_id") or "").strip()
        password_ciphertext = (payload.get("password_ciphertext") or "").strip()

        if not context_id or not password_ciphertext:
            raise HTTPException(status_code=400, detail="PASSWORD_ENCRYPTED_PAYLOAD_REQUIRED")

        try:
            password = decrypt_password_ciphertext(
                owner_user_id=owner_user_id,
                context_id=context_id,
                ciphertext_b64=password_ciphertext,
            )
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))

        await confirm_password(db, owner_user_id, password)

        ss = await export_string_session(db, owner_user_id)
        await save_user_telegram_session(db, owner_user_id, ss)

        me = await tg_get_current_user(db, owner_user_id)

        return {
            "status": "authorized",
            "me": {
                "id": me.id,
                "username": me.username,
                "first_name": me.first_name,
                "last_name": getattr(me, "last_name", None),
                "phone": me.phone,
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_PASSWORD_CONFIRM_FAILED: {str(e)}")

@app.post("/tg/analyze_chat")
async def tg_analyze_chat(
    payload: dict,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Single-chat Q&A endpoint после cutover'а на токенную систему.

    Pipeline:
      1. depth (light/balanced/deep) от фронта — вместо ai_model
      2. plan_limits.check_tier_allowed_or_raise (free → только light)
      3. billing.check_can_spend — soft-блок по балансу
      4. fetch из Telegram (без изменений)
      5. llm.service.run_qa — classifier → router → orchestrator
         с fallback-chain
      6. pricing.get_token_rates(used_model) + billing.compute_tokens_for_llm_call
      7. atomic: db.add(UsageEvent) + billing.debit(reason='qa_request') +
                 db.add(UserQueryLog)

    Старая система счётчиков qa-запросов (record_qa_success/_failure +
    enforce_qa_limits + resolve_ai_model_for_user) больше не вызывается
    из этого endpoint'а. Удалим в этапе 4 рефакторинга.
    """
    owner_user_id = user.id

    chat_link = (payload.get("chat_link") or "").strip()
    user_query = (payload.get("user_query") or "").strip()

    # Период анализа: новый контракт {period_value, period_unit} либо
    # legacy {days}. parse_period_from_payload бросает 400 на невалидный.
    period_value, period_unit, period_seconds = parse_period_from_payload(payload)
    # days — для логов. Для минут/часов отдаём 1 (под-суточный период).
    days = period_value if period_unit == "days" else 1

    # Абсолютный диапазон {date_from, date_to} (приоритетнее относительного
    # периода, если задан). Лимит тарифа на длину диапазона проверяем ниже,
    # когда получим план пользователя.
    range_since, range_until = parse_date_range_from_payload(payload)

    # === НОВОЕ: depth вместо ai_model ===
    depth = str(payload.get("depth") or "light").strip().lower()
    # Опциональный override категории от фронта (chip «изменить категорию»)
    explicit_category = payload.get("category")

    me = await tg_get_current_user(db, owner_user_id)
    if not me:
        # Pre-condition: no Telegram link. Не Q&A failure — никаких
        # токенов не тратилось.
        raise HTTPException(401, "TELEGRAM_NOT_AUTHORIZED")

    # === Tier check (free → только light) ===
    plan = await get_user_plan(db, user)
    check_tier_allowed_or_raise(plan, depth)

    # Если задан абсолютный диапазон — проверяем его длину против тарифа и
    # используем его как окно анализа (days — для логов берём из длины диапазона).
    if range_since is not None and range_until is not None:
        days = ensure_range_within_plan(since_dt=range_since, until_dt=range_until, plan=plan)

    # === Soft-block по балансу токенов ===
    can_spend, balance = await billing.check_can_spend(
        db, user_id=user.id, tier=depth,
    )
    if not can_spend:
        raise HTTPException(
            status_code=402,
            detail={
                "code": "INSUFFICIENT_TOKENS",
                "message": (
                    "Недостаточно токенов на балансе. Дождитесь начала "
                    "следующего месяца или докупите токены."
                ),
                "monthly_used": balance.monthly_used,
                "monthly_granted": balance.monthly_granted,
                "topup_balance": balance.topup_balance,
            },
        )

    # -------- MEDIA FILTER branch (Этап 8) --------
    # Если пользователь включил «Медиафильтр» в UI — идём отдельным
    # пайплайном (Telethon messages.search + LLM-парсер/реранкер) и
    # выходим раньше, не дёргая обычный fetch_chat_messages + run_qa.
    media_filter_req = mf_integration.request_from_payload(payload)
    if media_filter_req is not None:
        return await _handle_media_filter_branch(
            db,
            user=user,
            source_mode="personal",
            chat_links=[chat_link],
            is_group=False,
            period_seconds=period_seconds,
            days=days,
            depth=depth,
            user_query=user_query,
            request=media_filter_req,
            range_since=range_since,
            range_until=range_until,
        )

    query_chars = len(user_query)
    total_t0 = time.perf_counter()

    # -------- FETCH messages from Telegram --------
    fetch_stats: dict = {}
    fetch_t0 = time.perf_counter()
    try:
        entity, messages = await fetch_chat_messages(
            db, owner_user_id, chat_link, days, period_seconds=period_seconds,
            since_dt=range_since, until_dt=range_until,
            fetch_stats=fetch_stats,
        )
    except ValueError as ve:
        fetch_ms = int((time.perf_counter() - fetch_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            error_code="TELEGRAM_FETCH_FAILED",
            error_message=(str(ve) or "")[:300] or None,
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            fetch_stats=fetch_stats,
        )
        await db.commit()
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        fetch_ms = int((time.perf_counter() - fetch_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            error_code="TELEGRAM_FETCH_FAILED",
            error_message=(str(e) or "")[:300] or None,
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            fetch_stats=fetch_stats,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="TELEGRAM_FETCH_FAILED")
    fetch_ms = int((time.perf_counter() - fetch_t0) * 1000)

    messages_fetched_count = len(messages)
    context_chars = sum(
        len(m.get("text") or "")
        + len(m.get("from") or "")
        + len(m.get("date") or "")
        + 4
        for m in messages
    )

    chat_name = (
        getattr(entity, "title", None) or getattr(entity, "username", "Без названия")
    )

    # -------- LLM call через новый pipeline --------
    llm_t0 = time.perf_counter()
    try:
        qa_result = await run_qa(
            user_query=user_query,
            chat_name=chat_name,
            text_messages=messages,
            fallback_language=user.language,
            depth=depth,
            requested_period_days=days,
            explicit_category=explicit_category,
        )
    except LlmFatalError as exc:
        # 400/401/403 — наша конфиг-ошибка, fallback не помог бы
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            error_code="LLM_FATAL_ERROR",
            error_message=f"{exc.provider}:{exc.provider_model} {exc}"[:300],
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            messages_fetched_count=messages_fetched_count,
            context_chars=context_chars,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
            fetch_stats=fetch_stats,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="LLM_ERROR")
    except LlmAllModelsFailedError as exc:
        # Все модели в fallback-chain недоступны (редкий, но критичный кейс)
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            error_code="LLM_ALL_MODELS_FAILED",
            error_message=f"attempted={exc.attempted_models}"[:300],
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            messages_fetched_count=messages_fetched_count,
            context_chars=context_chars,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(
            status_code=503,
            detail={
                "code": "LLM_TEMPORARILY_UNAVAILABLE",
                "message": "Временные проблемы с AI-провайдерами, попробуйте через минуту.",
            },
        )
    except LlmEmptyResponseError as exc:
        # Модель(и) вернули пустой ответ — некорректно отработанный запрос.
        # Токены НЕ списываем. finish_reason пишем в лог для разбора причины.
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            error_code="EMPTY_LLM_RESPONSE",
            error_message=f"finish_reasons={exc.finish_reasons} attempted={exc.attempted_models}"[:300],
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            messages_fetched_count=messages_fetched_count,
            context_chars=context_chars,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(
            status_code=502,
            detail={
                "code": "EMPTY_LLM_RESPONSE",
                "message": "Модель не вернула ответ на этот запрос. Токены не списаны — попробуйте ещё раз или выберите более глубокий режим анализа.",
                "finish_reasons": exc.finish_reasons,
            },
        )
    except Exception as e:
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            error_code="LLM_ERROR",
            error_message=(str(e) or "")[:300] or None,
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            messages_fetched_count=messages_fetched_count,
            context_chars=context_chars,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="LLM_ERROR")
    llm_ms = int((time.perf_counter() - llm_t0) * 1000)

    summary = qa_result.text

    # -------- Chat history (всегда, даже если is_empty) --------
    await upsert_user_chat_history(
        db,
        owner_user_id=owner_user_id,
        source_mode="personal",
        chat_ref=chat_link,
        chat_title=chat_name,
        chat_username=getattr(entity, "username", None),
        chat_id=getattr(entity, "id", None),
    )

    total_ms = int((time.perf_counter() - total_t0) * 1000)

    # -------- Empty-chat short-circuit (LLM не вызывался, токены не списываем) --------
    if qa_result.is_empty:
        await _record_qa_success_event(
            db, user=user, source_mode="personal", chat_ref=chat_link,
            depth=depth, requested_days=days, query_chars=query_chars,
            messages_fetched_count=messages_fetched_count,
            context_chars=context_chars, answer_chars=len(summary or ""),
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
            qa_result=qa_result, tokens_charged=0,
            fetch_stats=fetch_stats,
        )
        await db.commit()
        return _build_qa_response(
            summary=summary, chat_name=chat_name,
            messages_fetched_count=messages_fetched_count,
            qa_result=qa_result, tokens_charged=0, entity=entity, messages=messages,
            usage_snapshot=await build_usage_snapshot(db, user=user),
        )

    # -------- Расчёт стоимости запроса в наших токенах --------
    used_model_slug = qa_result.llm.used_model.slug
    rates = await get_token_rates(db, used_model_slug)
    if rates is None:
        # llm_pricing не настроен для этой модели — биллим минимум,
        # но логируем для админа. На проде такого быть не должно.
        log = logging.getLogger(__name__)
        log.error("billing.no_pricing_row used_model=%s — отсутствует строка в llm_pricing",
                  used_model_slug)
        tokens_charged = 1
    else:
        tokens_charged = billing.compute_tokens_for_llm_call(
            input_tokens=qa_result.llm.usage.input_tokens or 0,
            output_tokens=qa_result.llm.usage.output_tokens or 0,
            thinking_tokens=qa_result.llm.usage.thinking_tokens or 0,
            in_per_1k=rates.in_per_1k,
            out_per_1k=rates.out_per_1k,
        )

    # -------- UsageEvent + billing.debit + UserQueryLog (атомарно) --------
    usage_event_id = await _record_qa_success_event(
        db, user=user, source_mode="personal", chat_ref=chat_link,
        depth=depth, requested_days=days, query_chars=query_chars,
        messages_fetched_count=messages_fetched_count,
        context_chars=context_chars, answer_chars=len(summary or ""),
        duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
        duration_ms_llm=llm_ms,
        qa_result=qa_result, tokens_charged=tokens_charged,
        fetch_stats=fetch_stats,
    )

    await billing.debit(
        db,
        user_id=user.id,
        amount=tokens_charged,
        reason=billing.REASON_QA_REQUEST,
        related_event_id=usage_event_id,
        meta={
            "used_model": used_model_slug,
            "input_tokens": qa_result.llm.usage.input_tokens,
            "output_tokens": qa_result.llm.usage.output_tokens,
            "thinking_tokens": qa_result.llm.usage.thinking_tokens,
            "tokens_charged": tokens_charged,
            "in_per_1k": float(rates.in_per_1k) if rates else None,
            "out_per_1k": float(rates.out_per_1k) if rates else None,
        },
    )

    # UserQueryLog — для аналитики и калибровки роутера
    db.add(UserQueryLog(
        user_id=user.id,
        usage_event_id=usage_event_id,
        query_text=user_query or "",
        detected_category=(
            qa_result.classification.category if qa_result.classification else None
        ),
        detected_confidence=(
            qa_result.classification.confidence if qa_result.classification else None
        ),
        final_category=qa_result.decision.category if qa_result.decision else None,
        selected_tier=depth,
        selected_model=used_model_slug,
    ))

    await db.commit()

    return _build_qa_response(
        summary=summary, chat_name=chat_name,
        messages_fetched_count=messages_fetched_count,
        qa_result=qa_result, tokens_charged=tokens_charged,
        entity=entity, messages=messages,
        usage_snapshot=await build_usage_snapshot(db, user=user),
    )


# ---------------------------------------------------------------------------
# Helpers для нового Q&A pipeline (используются tg_analyze_chat и группой)
# ---------------------------------------------------------------------------


async def _record_qa_success_event(
    db: AsyncSession,
    *,
    user: User,
    source_mode: str,
    chat_ref: str,
    depth: str,
    requested_days: int,
    query_chars: int,
    messages_fetched_count: int,
    context_chars: int,
    answer_chars: int,
    duration_ms_total: int,
    duration_ms_fetch: int,
    duration_ms_llm: int,
    qa_result,
    tokens_charged: int,
    fetch_stats: Optional[dict] = None,
) -> int:
    """
    Записать UsageEvent об успешном Q&A запросе в новой схеме.

    Возвращает id созданной записи (нужен для token_transactions.related_event_id).

    Отличия от старого record_qa_success:
      - Не инкрементит deprecated UsageCounter
      - Кладёт в meta_json новые поля роутера (через orchestrator.routing_meta)
      - Не требует ai_model отдельным параметром — берёт из qa_result.llm.used_model
    """
    meta: dict = {
        "depth": depth,
        "requested_days": requested_days,
        "query_chars": query_chars,
        "messages_fetched_count": messages_fetched_count,
        "messages_sent_to_llm_count": messages_fetched_count,
        "context_chars": context_chars,
        "answer_chars": answer_chars,
        "duration_ms_total": duration_ms_total,
        "duration_ms_fetch": duration_ms_fetch,
        "duration_ms_llm": duration_ms_llm,
        "tokens_charged": tokens_charged,
    }

    if qa_result.llm is not None:
        meta["ai_model"] = qa_result.llm.used_model.slug
        meta["input_tokens"] = qa_result.llm.usage.input_tokens
        meta["output_tokens"] = qa_result.llm.usage.output_tokens
        meta["total_tokens"] = qa_result.llm.usage.total_tokens
        meta["thinking_tokens"] = qa_result.llm.usage.thinking_tokens
        meta["tokens_source"] = qa_result.llm.usage.tokens_source
        meta["raw_finish_reason"] = qa_result.llm.finish_reason
        # Оценка стоимости запроса в USD (для колонки цены и админ-аналитики).
        # Токенная система (tokens_charged) — это наша внутренняя валюта;
        # estimated_cost_usd — фактическая стоимость LLM-вызова у провайдера.
        try:
            cost = await estimate_llm_cost_usd(
                db,
                ai_model=qa_result.llm.used_model.slug,
                input_tokens=qa_result.llm.usage.input_tokens,
                output_tokens=qa_result.llm.usage.output_tokens,
                tokens_source=qa_result.llm.usage.tokens_source,
                thinking_tokens=qa_result.llm.usage.thinking_tokens,
            )
            meta["estimated_cost_usd"] = cost.estimated_cost_usd
            meta["cost_calculation_method"] = cost.cost_calculation_method
            meta["input_price_per_1m_usd_snapshot"] = cost.input_price_per_1m_usd_snapshot
            meta["output_price_per_1m_usd_snapshot"] = cost.output_price_per_1m_usd_snapshot
        except Exception as e:  # noqa: BLE001
            # Стоимость не критична для ответа — не валим запись события.
            log.warning("estimate_llm_cost_usd failed: %s", e)
        # routing info
        if qa_result.decision is not None:
            meta.update(routing_meta(qa_result.llm, qa_result.decision))
    if qa_result.is_empty:
        meta["is_empty"] = True

    meta.update(_fetch_stats_meta(fetch_stats))

    event = UsageEvent(
        user_id=user.id,
        event_type="qa_request_success",
        status="success_counted",
        source_mode=source_mode,
        chat_ref=chat_ref,
        meta_json=_drop_none(meta),
    )
    db.add(event)
    await db.flush()  # нужен id для token_transactions.related_event_id
    return int(event.id)


async def _record_qa_failure_event(
    db: AsyncSession,
    *,
    user: User,
    source_mode: str,
    chat_ref: str,
    error_code: str,
    error_message: Optional[str],
    query_chars: Optional[int],
    requested_days: int,
    depth: str,
    messages_fetched_count: Optional[int] = None,
    context_chars: Optional[int] = None,
    duration_ms_total: Optional[int] = None,
    duration_ms_fetch: Optional[int] = None,
    duration_ms_llm: Optional[int] = None,
    fetch_stats: Optional[dict] = None,
) -> None:
    """
    Записать UsageEvent о failed Q&A запросе. Биллинг НЕ списывается на
    ошибках (LLM-вызов не состоялся или вернул мусор).
    """
    meta = _drop_none({
        "depth": depth,
        "requested_days": requested_days,
        "query_chars": query_chars,
        "messages_fetched_count": messages_fetched_count,
        "context_chars": context_chars,
        "duration_ms_total": duration_ms_total,
        "duration_ms_fetch": duration_ms_fetch,
        "duration_ms_llm": duration_ms_llm,
        "error_code": error_code,
        "error_message": error_message,
    })
    meta.update(_drop_none(_fetch_stats_meta(fetch_stats)))
    db.add(UsageEvent(
        user_id=user.id,
        event_type="qa_request_failure",
        status="failed_not_counted",
        source_mode=source_mode,
        chat_ref=chat_ref,
        meta_json=meta,
    ))


def _drop_none(d: dict) -> dict:
    """Убрать ключи с None-значениями (чтобы не раздувать meta_json)."""
    return {k: v for k, v in d.items() if v is not None}


def _fetch_stats_meta(fetch_stats: Optional[dict]) -> dict:
    """
    Поля диагностики выгрузки из Telegram для meta_json (видны в админке).
    Заполняются в telegram_service.fetch_chat_messages через переданный
    словарь fetch_stats.
    """
    if not fetch_stats:
        return {}
    return {
        "sender_lookups": fetch_stats.get("sender_lookups"),
        "unique_senders": fetch_stats.get("unique_senders"),
        "flood_wait_count": fetch_stats.get("flood_waits"),
        "flood_wait_total_seconds": fetch_stats.get("flood_seconds"),
    }


def _media_filter_fee_days(*, period_seconds: Optional[int], days: int) -> int:
    """
    Кол-во "дней" для фиксированной платы за медиафильтр без LLM.

    Правило: один день (или любой период меньше дня) считается за 1.
    7 дней → 7. period_seconds (если задан) имеет приоритет над days.
    """
    if period_seconds is not None and int(period_seconds) > 0:
        return max(1, math.ceil(int(period_seconds) / 86400.0))
    return max(1, int(days or 1))


async def _handle_media_filter_branch(
    db: AsyncSession,
    *,
    user: User,
    source_mode: str,
    chat_links: list[str],
    is_group: bool,
    period_seconds: Optional[int],
    days: int,
    depth: str,
    user_query: str,
    request,
    range_since: Optional[datetime] = None,
    range_until: Optional[datetime] = None,
) -> dict:
    """
    Полная обработка одного запроса с включённым медиафильтром.

    Не идёт через стандартный run_qa() — вместо этого вызывает
    integration.run_and_build_response(), который сам тянет медиа
    из Telegram, прогоняет через LLM-парсер/реранкер и форматирует
    карточки.

    Биллинг и UsageEvent оформляются ЗДЕСЬ (а не в integration), чтобы
    логика учёта токенов осталась рядом с обычным Q&A. Используем тот
    же reason=REASON_QA_REQUEST — фронт уже умеет показывать списания
    в этой колонке.
    """
    total_t0 = time.perf_counter()

    # Медиафильтр пока строит окно только «от текущего момента назад»
    # (telethon_search избегает offset_date из-за бага Telethon #1124).
    # Произвольный прошлый диапазон «С–По» здесь не поддержан — явно говорим
    # об этом, чтобы не вводить пользователя в заблуждение. См. BACKLOG.
    if range_since is not None or range_until is not None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "MEDIA_FILTER_RANGE_UNSUPPORTED",
                "message": (
                    "Поиск по медиафильтру пока работает только с периодом "
                    "«за последние…», без произвольного диапазона дат «С–По». "
                    "Уберите даты диапазона или отключите медиафильтр."
                ),
            },
        )

    chat_ref = chat_links[0] if not is_group else f"group:{len(chat_links)}"
    query_chars = len(user_query or "")

    try:
        result = await mf_integration.run_and_build_response(
            db, user.id,
            chat_links=chat_links,
            is_group=is_group,
            request=request,
            period_seconds=period_seconds,
            days=days,
            user_query=user_query,
        )
    except Exception as e:
        # Любой сбой пайплайна логируем и возвращаем 502.
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode=source_mode, chat_ref=chat_ref,
            error_code="MEDIA_FILTER_ERROR",
            error_message=(str(e) or "")[:300] or None,
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="MEDIA_FILTER_ERROR")

    total_ms = int((time.perf_counter() - total_t0) * 1000)

    # --- UsageEvent для media filter ---
    # Используем event_type="qa_request_success", чтобы попасть в те же
    # ленты аналитики. В meta_json кладём флаг и разбивку по моделям.
    used_models = result.billing.used_models
    primary_model_slug = used_models[0] if used_models else None

    # === Сколько токенов списать ===
    # Умный медиафильтр без пользовательского запроса вообще не дёргает LLM
    # (parser/reranker не вызываются) → result.tokens_charged == 0. Но сервис
    # всё равно выполняет работу (читает Telegram, фильтрует), поэтому за
    # такие запросы берём фиксированную плату: кол-во чатов × кол-во дней
    # (минимум 1). Если LLM вызывался хотя бы раз — берём фактическую
    # стоимость по моделям, как раньше.
    llm_call_count = len(result.llm_results)
    if llm_call_count == 0:
        fee_days = _media_filter_fee_days(period_seconds=period_seconds, days=days)
        num_chats = max(1, len(chat_links))
        tokens_to_charge = max(1, num_chats * fee_days)
        flat_fee_applied = True
    else:
        fee_days = None
        num_chats = len(chat_links)
        tokens_to_charge = int(result.tokens_charged)
        flat_fee_applied = False

    meta: dict = {
        "depth": depth,
        "requested_days": days,
        "query_chars": query_chars,
        "messages_fetched_count": sum(c.fetched_count for c in result.run.chats),
        "tokens_charged": tokens_to_charge,
        "media_filter_flat_fee": flat_fee_applied,
        "media_filter_flat_fee_days": fee_days,
        "media_filter_flat_fee_chats": num_chats if flat_fee_applied else None,
        "duration_ms_total": total_ms,
        "is_media_filter": True,
        "media_filter_per_model": result.billing.per_model,
        "input_tokens": result.billing.raw_input_tokens,
        "output_tokens": result.billing.raw_output_tokens,
        "thinking_tokens": result.billing.raw_thinking_tokens,
        "parser_fallback": result.run.used_parser_fallback,
        "reranker_fallback": result.run.used_reranker_fallback,
        "selected_categories": [c.value for c in result.run.selected_categories],
        "per_chat_after_filter": [
            {
                "chat_link": c.chat_link,
                "fetched": c.fetched_count,
                "after_structured": c.after_structured_count,
                "after_semantic": c.after_semantic_count,
                "error_code": c.error_code,
            }
            for c in result.run.chats
        ],
    }
    if primary_model_slug:
        meta["ai_model"] = primary_model_slug

    event = UsageEvent(
        user_id=user.id,
        event_type="qa_request_success",
        status="success_counted",
        source_mode=source_mode,
        chat_ref=chat_ref,
        meta_json=_drop_none(meta),
    )
    db.add(event)
    await db.flush()
    usage_event_id = int(event.id)

    # --- billing.debit (одна транзакция на запрос; токены суммированы
    #     по всем LLM-вызовам через compute_billing, либо фиксированная
    #     плата за медиафильтр без LLM) ---
    if tokens_to_charge > 0:
        await billing.debit(
            db,
            user_id=user.id,
            amount=tokens_to_charge,
            reason=billing.REASON_QA_REQUEST,
            related_event_id=usage_event_id,
            meta={
                "is_media_filter": True,
                "media_filter_flat_fee": flat_fee_applied,
                "media_filter_flat_fee_days": fee_days,
                "media_filter_flat_fee_chats": num_chats if flat_fee_applied else None,
                "used_models": used_models,
                "per_model_tokens": result.billing.per_model,
                "input_tokens": result.billing.raw_input_tokens,
                "output_tokens": result.billing.raw_output_tokens,
                "thinking_tokens": result.billing.raw_thinking_tokens,
                "tokens_charged": tokens_to_charge,
            },
        )

    # --- UserQueryLog (как и обычный Q&A) ---
    db.add(UserQueryLog(
        user_id=user.id,
        usage_event_id=usage_event_id,
        query_text=user_query or "",
        detected_category=None,
        detected_confidence=None,
        final_category="media_filter",
        selected_tier=depth,
        selected_model=primary_model_slug or "n/a",
    ))

    await db.commit()

    # --- Финальный ответ ---
    response = dict(result.response_dict)
    response["status"] = "ok"
    response["source_mode"] = source_mode
    # Перекрываем tokens_charged итоговой суммой (важно для случая
    # фиксированной платы без LLM — иначе фронт покажет "Списано: 0").
    response["tokens_charged"] = tokens_to_charge
    response["usage"] = await build_usage_snapshot(db, user=user)
    return response


def _build_qa_response(
    *,
    summary: str,
    chat_name: str,
    messages_fetched_count: int,
    qa_result,
    tokens_charged: int,
    entity,
    messages: list,
    usage_snapshot: dict,
) -> dict:
    """Собрать JSON-ответ endpoint'а tg_analyze_chat."""
    from telegram_service import build_message_permalink as _build_permalink
    message_links: dict[int, str | None] = {}
    for _m in messages:
        _mid = _m.get("message_id")
        if _mid is None:
            continue
        message_links[int(_mid)] = _build_permalink(entity, _mid)

    body = {
        "status": "ok",
        "summary": summary,
        "chat_name": chat_name,
        "messages_count": messages_fetched_count,
        "source_mode": "personal",
        "message_links": message_links,
        "tokens_charged": tokens_charged,
        "usage": usage_snapshot,
    }

    # Расширенный набор полей для UI «Расшифровка ▾» и фронт-логов.
    if qa_result.llm is not None:
        body["used_model"] = qa_result.llm.used_model.slug
        body["was_fallback"] = qa_result.llm.was_fallback
    if qa_result.decision is not None:
        body["category"] = qa_result.decision.category
        body["tier"] = qa_result.decision.tier
    if qa_result.classification is not None:
        body["detected_category"] = qa_result.classification.category
        body["detected_confidence"] = qa_result.classification.confidence

    return body


# ---------------------------------------------------------------------------
# POST /tg/analyze_chats_group
# ---------------------------------------------------------------------------
#
# Multi-chat group analysis (personal Telegram account only — service
# accounts excluded in v1). The user picks 1..N chats via checkboxes on
# the frontend; we fetch each chat's history in parallel, send a single
# LLM call with a labelled per-chat prompt, and return per-chat results
# plus an overall summary.
#
# Quota accounting: this consumes N qa_request slots (one per chat).
# This is intentional and will be revisited when we move to the credits
# architecture. See plan_limits.enforce_qa_limits(slots_required=N).
#
# Partial-failure model: if some chats fail to fetch we still proceed
# with the rest. The failing chats appear in the `results` array with
# status="fetch_failed" and contribute no LLM context. If ALL chats
# fail, we return 502 like the single-chat endpoint does.
# ---------------------------------------------------------------------------

@app.post("/tg/analyze_chats_group")
async def tg_analyze_chats_group(
    payload: dict,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    raw_links = payload.get("chat_links") or []
    if not isinstance(raw_links, list):
        raise HTTPException(
            status_code=400,
            detail={"code": "BAD_PAYLOAD", "message": "chat_links must be a list"},
        )

    # Normalize: strip + dedup while preserving order.
    chat_links: list[str] = []
    seen: set[str] = set()
    for v in raw_links:
        s = str(v or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        chat_links.append(s)

    if not chat_links:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "GROUP_EMPTY",
                "message": "Не выбрано ни одного чата для группового анализа.",
            },
        )

    user_query = (payload.get("user_query") or "").strip()
    # Период анализа — общий помощник; см. plan_limits.parse_period_from_payload
    period_value, period_unit, period_seconds = parse_period_from_payload(payload)
    days = period_value if period_unit == "days" else 1

    # Абсолютный диапазон {date_from, date_to} (приоритетнее относительного).
    range_since, range_until = parse_date_range_from_payload(payload)

    # === НОВОЕ: depth вместо ai_model ===
    depth = str(payload.get("depth") or "light").strip().lower()
    explicit_category = payload.get("category")

    me = await tg_get_current_user(db, owner_user_id)
    if not me:
        raise HTTPException(401, "TELEGRAM_NOT_AUTHORIZED")

    # === Tier check (free → только light) ===
    plan = await get_user_plan(db, user)
    check_tier_allowed_or_raise(plan, depth)

    if range_since is not None and range_until is not None:
        days = ensure_range_within_plan(since_dt=range_since, until_dt=range_until, plan=plan)

    # === Per-plan group size limit (новое поле max_chats_per_group_request) ===
    group_size = len(chat_links)
    check_max_chats_or_raise(plan, group_size)

    # === Soft-block по балансу токенов ===
    can_spend, balance = await billing.check_can_spend(
        db, user_id=user.id, tier=depth,
    )
    if not can_spend:
        raise HTTPException(
            status_code=402,
            detail={
                "code": "INSUFFICIENT_TOKENS",
                "message": (
                    "Недостаточно токенов на балансе. Дождитесь начала "
                    "следующего месяца или докупите токены."
                ),
                "monthly_used": balance.monthly_used,
                "monthly_granted": balance.monthly_granted,
                "topup_balance": balance.topup_balance,
            },
        )

    # -------- MEDIA FILTER branch (Этап 8) --------
    # См. /tg/analyze_chat выше — та же логика, но с is_group=True.
    media_filter_req = mf_integration.request_from_payload(payload)
    if media_filter_req is not None:
        return await _handle_media_filter_branch(
            db,
            user=user,
            source_mode="personal",
            chat_links=chat_links,
            is_group=True,
            period_seconds=period_seconds,
            days=days,
            depth=depth,
            user_query=user_query,
            request=media_filter_req,
            range_since=range_since,
            range_until=range_until,
        )

    query_chars = len(user_query)
    total_t0 = time.perf_counter()

    # ---- Parallel fetch ----
    fetch_t0 = time.perf_counter()
    fetch_tasks = [
        fetch_chat_messages(
            db, owner_user_id, link, days, period_seconds=period_seconds,
            since_dt=range_since, until_dt=range_until,
        )
        for link in chat_links
    ]
    fetch_outcomes = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    fetch_ms = int((time.perf_counter() - fetch_t0) * 1000)

    # Per-chat status arrays. We track three buckets:
    #   - ok:    successfully fetched, has messages, will be sent to LLM
    #   - empty: fetched but zero messages within the window
    #   - fetch_failed: exception during fetch (private/banned/network/etc.)
    per_chat: list[dict] = []
    chats_for_llm: list[dict] = []
    # We need entity references to build per-chat permalink maps after LLM.
    entities_by_index: dict[int, object] = {}
    fetched_messages_by_index: dict[int, list] = {}

    for idx, (link, outcome) in enumerate(zip(chat_links, fetch_outcomes)):
        if isinstance(outcome, Exception):
            # Note: we silently swallow the underlying exception text into
            # the per-chat error field rather than the response top level —
            # avoids leaking implementation details to the frontend while
            # still letting the user see WHICH chats failed.
            err = type(outcome).__name__
            per_chat.append({
                "chat_link": link,
                "chat_name": None,
                "messages_count": 0,
                "summary": None,
                "message_links": {},
                "status": "fetch_failed",
                "error": "TELEGRAM_FETCH_FAILED",
                "error_detail": err,
            })
            continue

        entity, messages = outcome
        chat_name = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or "Без названия"
        )

        if not messages:
            per_chat.append({
                "chat_link": link,
                "chat_name": chat_name,
                "messages_count": 0,
                "summary": None,
                "message_links": {},
                "status": "empty",
                "error": None,
            })
            continue

        entities_by_index[idx] = entity
        fetched_messages_by_index[idx] = messages
        chats_for_llm.append({
            "_idx": idx,  # bookkeeping; stripped before sending to service
            "chat_link": link,
            "chat_name": chat_name,
            "text_messages": messages,
        })
        per_chat.append({
            "chat_link": link,
            "chat_name": chat_name,
            "messages_count": len(messages),
            "summary": None,         # filled in below (whole-group summary)
            "message_links": {},     # filled in below
            "status": "ok",
            "error": None,
        })

    # If literally every chat failed, treat the request as a global
    # failure (502, mirrors single-chat behaviour on TELEGRAM_FETCH_FAILED).
    if not chats_for_llm:
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal",
            chat_ref=f"group:{group_size}",
            error_code="TELEGRAM_FETCH_FAILED",
            error_message="All chats in group failed to fetch.",
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="TELEGRAM_FETCH_FAILED")

    # ---- LLM call через новый pipeline ----
    chats_for_service = [
        {k: v for k, v in c.items() if k != "_idx"} for c in chats_for_llm
    ]

    llm_t0 = time.perf_counter()
    try:
        qa_result = await run_qa_group(
            user_query=user_query,
            chats=chats_for_service,
            fallback_language=user.language,
            depth=depth,
            requested_period_days=days,
            explicit_category=explicit_category,
        )
    except LlmFatalError as exc:
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal",
            chat_ref=f"group:{group_size}",
            error_code="LLM_FATAL_ERROR",
            error_message=f"{exc.provider}:{exc.provider_model} {exc}"[:300],
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="LLM_ERROR")
    except LlmAllModelsFailedError as exc:
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal",
            chat_ref=f"group:{group_size}",
            error_code="LLM_ALL_MODELS_FAILED",
            error_message=f"attempted={exc.attempted_models}"[:300],
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(
            status_code=503,
            detail={
                "code": "LLM_TEMPORARILY_UNAVAILABLE",
                "message": "Временные проблемы с AI-провайдерами, попробуйте через минуту.",
            },
        )
    except LlmEmptyResponseError as exc:
        # Пустой ответ модели(ей) — не списываем токены, пишем причину в лог.
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal",
            chat_ref=f"group:{group_size}",
            error_code="EMPTY_LLM_RESPONSE",
            error_message=f"finish_reasons={exc.finish_reasons} attempted={exc.attempted_models}"[:300],
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(
            status_code=502,
            detail={
                "code": "EMPTY_LLM_RESPONSE",
                "message": "Модель не вернула ответ на этот запрос. Токены не списаны — попробуйте ещё раз или выберите более глубокий режим анализа.",
                "finish_reasons": exc.finish_reasons,
            },
        )
    except Exception as e:
        llm_ms = int((time.perf_counter() - llm_t0) * 1000)
        total_ms = int((time.perf_counter() - total_t0) * 1000)
        await _record_qa_failure_event(
            db, user=user, source_mode="personal",
            chat_ref=f"group:{group_size}",
            error_code="LLM_ERROR",
            error_message=(str(e) or "")[:300] or None,
            query_chars=query_chars or None,
            requested_days=days, depth=depth,
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
        )
        await db.commit()
        raise HTTPException(status_code=502, detail="LLM_ERROR")
    llm_ms = int((time.perf_counter() - llm_t0) * 1000)

    group_summary = qa_result.text

    # ---- Per-chat permalink maps ----
    # Build a {message_id: permalink} dict for each ok-chat. The
    # frontend uses these to turn [msg:ID] tokens in the summary into
    # clickable links. Failed/empty chats just stay with `{}`.
    from telegram_service import build_message_permalink as _build_permalink
    for c in chats_for_llm:
        idx = c["_idx"]
        entity = entities_by_index.get(idx)
        if entity is None:
            continue
        msgs = fetched_messages_by_index.get(idx) or []
        links_map: dict[int, str | None] = {}
        for m in msgs:
            mid = m.get("message_id")
            if mid is None:
                continue
            links_map[int(mid)] = _build_permalink(entity, mid)
        # Find the matching entry in per_chat (same chat_link) and
        # attach the summary + links. The summary itself is shared
        # across all chats — frontend parses `## Chat: <name>` headers
        # to split it.
        for row in per_chat:
            if row["chat_link"] == c["chat_link"] and row["status"] == "ok":
                row["message_links"] = links_map
                break

    total_ms = int((time.perf_counter() - total_t0) * 1000)

    # === ОДИН UsageEvent + ОДИН debit на групповой запрос ===
    # LLM-вызов был один (combined context), стоимость считается один раз.
    # Это исправляет старый баг с N×списанием за групповой запрос.
    total_messages_in_group = sum(
        len(c.get("text_messages") or []) for c in chats_for_service
    )

    # is_empty branch — все чаты пустые, LLM не вызывался
    if qa_result.is_empty:
        usage_event_id = await _record_qa_success_event(
            db, user=user, source_mode="personal",
            chat_ref=f"group:{group_size}",
            depth=depth, requested_days=days,
            query_chars=query_chars,
            messages_fetched_count=total_messages_in_group,
            context_chars=0,
            answer_chars=len(group_summary or ""),
            duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
            duration_ms_llm=llm_ms,
            qa_result=qa_result, tokens_charged=0,
        )
        await db.commit()
        return _build_group_response(
            group_summary=group_summary, per_chat=per_chat,
            group_size=group_size,
            qa_result=qa_result, tokens_charged=0,
            usage_snapshot=await build_usage_snapshot(db, user=user),
        )

    used_model_slug = qa_result.llm.used_model.slug
    rates = await get_token_rates(db, used_model_slug)
    if rates is None:
        log = logging.getLogger(__name__)
        log.error("billing.no_pricing_row used_model=%s — отсутствует строка в llm_pricing",
                  used_model_slug)
        tokens_charged = 1
    else:
        tokens_charged = billing.compute_tokens_for_llm_call(
            input_tokens=qa_result.llm.usage.input_tokens or 0,
            output_tokens=qa_result.llm.usage.output_tokens or 0,
            thinking_tokens=qa_result.llm.usage.thinking_tokens or 0,
            in_per_1k=rates.in_per_1k,
            out_per_1k=rates.out_per_1k,
        )

    usage_event_id = await _record_qa_success_event(
        db, user=user, source_mode="personal",
        chat_ref=f"group:{group_size}",
        depth=depth, requested_days=days,
        query_chars=query_chars,
        messages_fetched_count=total_messages_in_group,
        context_chars=0,  # для группы context_chars не считаем — sum по секциям бесполезен
        answer_chars=len(group_summary or ""),
        duration_ms_total=total_ms, duration_ms_fetch=fetch_ms,
        duration_ms_llm=llm_ms,
        qa_result=qa_result, tokens_charged=tokens_charged,
    )

    await billing.debit(
        db,
        user_id=user.id,
        amount=tokens_charged,
        reason=billing.REASON_QA_REQUEST,
        related_event_id=usage_event_id,
        meta={
            "group_size": group_size,
            "used_model": used_model_slug,
            "input_tokens": qa_result.llm.usage.input_tokens,
            "output_tokens": qa_result.llm.usage.output_tokens,
            "thinking_tokens": qa_result.llm.usage.thinking_tokens,
            "tokens_charged": tokens_charged,
            "in_per_1k": float(rates.in_per_1k) if rates else None,
            "out_per_1k": float(rates.out_per_1k) if rates else None,
        },
    )

    db.add(UserQueryLog(
        user_id=user.id,
        usage_event_id=usage_event_id,
        query_text=user_query or "",
        detected_category=(
            qa_result.classification.category if qa_result.classification else None
        ),
        detected_confidence=(
            qa_result.classification.confidence if qa_result.classification else None
        ),
        final_category=qa_result.decision.category if qa_result.decision else None,
        selected_tier=depth,
        selected_model=used_model_slug,
    ))

    await db.commit()

    return _build_group_response(
        group_summary=group_summary, per_chat=per_chat,
        group_size=group_size,
        qa_result=qa_result, tokens_charged=tokens_charged,
        usage_snapshot=await build_usage_snapshot(db, user=user),
    )


def _build_group_response(
    *,
    group_summary: str,
    per_chat: list,
    group_size: int,
    qa_result,
    tokens_charged: int,
    usage_snapshot: dict,
) -> dict:
    """Собрать JSON-ответ endpoint'а tg_analyze_chats_group."""
    body = {
        "status": "ok",
        "group_size": group_size,
        "results": per_chat,
        "summary": group_summary,
        "source_mode": "personal",
        "tokens_charged": tokens_charged,
        "usage": usage_snapshot,
    }
    if qa_result.llm is not None:
        body["used_model"] = qa_result.llm.used_model.slug
        body["was_fallback"] = qa_result.llm.was_fallback
    if qa_result.decision is not None:
        body["category"] = qa_result.decision.category
        body["tier"] = qa_result.decision.tier
    if qa_result.classification is not None:
        body["detected_category"] = qa_result.classification.category
        body["detected_confidence"] = qa_result.classification.confidence
    return body


@app.get("/tg/chats")
async def tg_list_chats(
    limit: int = 0,  # 0 / неуказано → тянем все диалоги (limit=None в Telethon)
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    me = await tg_get_current_user(db, owner_user_id)
    if not me:
        raise HTTPException(status_code=401, detail="TELEGRAM_NOT_AUTHORIZED")

    try:
        # limit==0 (или отрицательный) → передаём None, что в Telethon
        # означает "все диалоги". Это критично для корректной сборки
        # иерархии папок: при искусственном лимите хвост менее активных
        # чатов выпадает, и папки выглядят неполными.
        effective_limit = limit if (limit and limit > 0) else None
        structure = await get_telegram_structure(db, owner_user_id, limit=effective_limit)
        chats = structure["chats"]
        folders = structure["folders"]

        return {
            "status": "ok",
            "count": len(chats),
            "chats": chats,
            "folders": folders,
            "me": {
                "id": me.id,
                "username": me.username,
                "first_name": me.first_name,
                "last_name": me.last_name,
            }
        }

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_CHATS_FAILED: {str(e)}")


@app.post("/tg/logout")
async def tg_logout(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        await logout_telegram(db, owner_user_id)
        return {"status": "logged_out"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_LOGOUT_FAILED: {str(e)}")

@app.post("/tg/qr/start")
async def tg_qr_start(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        data = await qr_login_start(db, owner_user_id)
        return {"status": "ok", **data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_QR_START_FAILED: {str(e)}")

@app.get("/tg/qr/status")
async def tg_qr_status(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        data = await qr_login_status(db, owner_user_id)

        if isinstance(data, dict) and data.get("status") == "authorized":
            ss = await export_string_session(db, owner_user_id)
            await save_user_telegram_session(db, owner_user_id, ss)

        return data
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_QR_STATUS_FAILED: {str(e)}")

async def bot_send_message(chat_id: int, text: str, parse_mode: Optional[str] = None):
    """
    Отправить сообщение через нашего Telegram-бота.

    parse_mode="HTML" — включаем разметку (<a href>, <b>, <i>, и т.д.).
    None (по умолчанию) — обычный текст, обратная совместимость с
    существующими вызовами (auth-flow, ручные пинги, и т.д.).
    """
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN_MISSING")

    body: dict = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        body["parse_mode"] = parse_mode

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=body)

    if resp.status_code != 200:
        raise RuntimeError(f"BOT_SEND_FAILED_HTTP_{resp.status_code}: {resp.text}")

    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"BOT_SEND_FAILED: {data}")


@app.post("/tg/bot/webhook")
async def tg_bot_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    # 1) Проверка секрета
    expected = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
    got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not expected or got != expected:
        raise HTTPException(status_code=401, detail="WEBHOOK_SECRET_INVALID")

    update = await request.json()
    message = update.get("message") or update.get("edited_message")
    if not message:
        return {"ok": True}


    chat = message.get("chat") or {}
    user = message.get("from") or {}
    text = (message.get("text") or "").strip()

    telegram_chat_id = chat.get("id")
    telegram_user_id = user.get("id")
    if not telegram_chat_id:
        return {"ok": True}

    # 3) Реакция только на /start (MVP)
    if not text.startswith("/start"):
        return {"ok": True}

        # ожидаем: "/start <code>"
    parts = text.split(maxsplit=1)
    code = parts[1].strip() if len(parts) > 1 else None
    if not code:
        # можно ответить подсказкой
        await bot_send_message(
            telegram_chat_id,
            "Привет! Чтобы привязать бота, открой CoTel → Профиль → Подключить бота и отправь мне /start <код>."
        )
        return {"ok": True}

    code_hash = sha256_hex(code)

    # 1) найти активный код
    now = datetime.now(timezone.utc)
    res = await db.execute(
        select(BotLinkCode).where(
            BotLinkCode.code_hash == code_hash,
            BotLinkCode.used_at.is_(None),
            BotLinkCode.expires_at > now,
        )
    )
    rec = res.scalar_one_or_none()
    if not rec:
        await bot_send_message(telegram_chat_id, "Код недействителен или истёк. Сгенерируй новый в CoTel.")
        return {"ok": True}

    owner_user_id = rec.user_id

    # 2) отметить код использованным
    rec.used_at = now

    # 4) Upsert в bot_user_link по уникальному telegram_chat_id
    stmt = insert(BotUserLink).values(
        owner_user_id=owner_user_id,
        telegram_chat_id=telegram_chat_id,
        telegram_user_id=telegram_user_id,
        is_blocked=False,
    ).on_conflict_do_update(
        index_elements=["telegram_chat_id"],
        set_={
            "owner_user_id": owner_user_id,
            "telegram_user_id": telegram_user_id,
            "is_blocked": False,
            "updated_at": sa.text("now()"),
        },
    )

    await db.execute(stmt)
    await db.commit()

    await bot_send_message(
        telegram_chat_id,
        "👋 Бот CoTel подключён.\n\n"
        "Теперь ты можешь создавать подписки в веб-интерфейсе, "
        "и я буду присылать уведомления, когда в чатах появятся нужные сообщения."
    )

    return {"ok": True}

@app.get("/tg/bot/link/status")
async def tg_bot_link_status(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    q = (
        select(sa.func.count())
        .select_from(BotUserLink)
        .where(
            BotUserLink.owner_user_id == user.id,
            BotUserLink.is_blocked == False,  # noqa: E712
        )
    )
    count = (await db.execute(q)).scalar_one()
    return {"connected": count > 0}

@app.get("/chat-history")
async def list_chat_history(
    source_mode: str = "personal",
    limit: int = 30,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    limit = max(1, min(int(limit or 30), 30))
    source_mode = (source_mode or "personal").strip().lower()

    plan_snapshot = await build_usage_snapshot(db, user=user)
    plan_info = plan_snapshot.get("plan") or {}

    if not plan_info.get("has_chat_history", False):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "PLAN_CHAT_HISTORY_NOT_AVAILABLE",
                "message": "История чатов недоступна на вашем тарифе.",
            },
        )

    if source_mode not in {"personal", "service"}:
        raise HTTPException(status_code=400, detail="INVALID_SOURCE_MODE")

    # personal-mode: показываем personal + service, но без дублей по normalized ref
    if source_mode == "personal":
        res = await db.execute(
            select(UserChatHistory)
            .where(
                UserChatHistory.owner_user_id == user.id,
                UserChatHistory.source_mode.in_(["personal", "service"]),
            )
            .order_by(UserChatHistory.last_accessed_at.desc(), UserChatHistory.id.desc())
        )
        rows = list(res.scalars().all())

        dedup: list[UserChatHistory] = []
        seen: set[str] = set()

        for row in rows:
            key = row.chat_ref_normalized or ""
            if not key or key in seen:
                continue
            seen.add(key)
            dedup.append(row)
            if len(dedup) >= limit:
                break

        items = [serialize_chat_history_row(row) for row in dedup]
        return {"items": items, "count": len(items)}

    # service-mode: только service history
    res = await db.execute(
        select(UserChatHistory)
        .where(
            UserChatHistory.owner_user_id == user.id,
            UserChatHistory.source_mode == "service",
        )
        .order_by(UserChatHistory.last_accessed_at.desc(), UserChatHistory.id.desc())
        .limit(limit)
    )
    rows = list(res.scalars().all())
    items = [serialize_chat_history_row(row) for row in rows]
    return {"items": items, "count": len(items)}


@app.delete("/chat-history/{history_id}")
async def delete_chat_history_item(
    history_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(UserChatHistory, history_id)
    if not row:
        raise HTTPException(status_code=404, detail="CHAT_HISTORY_NOT_FOUND")

    if row.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    await db.delete(row)
    await db.commit()

    return {"status": "ok", "deleted_id": history_id}

@app.get("/subscriptions", response_model=list[SubscriptionOut])
async def list_subscriptions(
    source_mode: str | None = None,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Subscription).where(Subscription.owner_user_id == user.id)

    if source_mode:
      normalized_mode = source_mode.strip().lower()
      if normalized_mode not in {"personal", "service"}:
          raise HTTPException(status_code=400, detail="INVALID_SOURCE_MODE")
      stmt = stmt.where(Subscription.source_mode == normalized_mode)

    stmt = stmt.order_by(Subscription.id.desc())

    res = await db.execute(stmt)
    subs = list(res.scalars().all())

    changed = False
    for sub in subs:
        expired = await expire_trial_subscription_if_needed(db, sub=sub)
        if expired:
            changed = True

    if changed:
        await db.commit()

    # Одним запросом подгружаем все чаты по всем групповым подпискам — N+1 не нужен.
    group_ids = [s.id for s in subs if getattr(s, "is_group", False)]
    chats_by_sub: dict[int, list[dict]] = {gid: [] for gid in group_ids}
    if group_ids:
        chats_res = await db.execute(
            select(SubscriptionChat)
            .where(SubscriptionChat.subscription_id.in_(group_ids))
            .order_by(SubscriptionChat.subscription_id.asc(), SubscriptionChat.position.asc())
        )
        for row in chats_res.scalars().all():
            chats_by_sub.setdefault(int(row.subscription_id), []).append({
                "chat_ref": row.chat_ref,
                "chat_id": row.chat_id,
                "chat_title": row.chat_title,
                "chat_username": row.chat_username,
                "position": row.position,
            })

    return [
        _serialize_subscription(s, chats=chats_by_sub.get(s.id) if getattr(s, "is_group", False) else None)
        for s in subs
    ]

@app.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
async def get_subscription(
    subscription_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    chats_payload = None
    if getattr(sub, "is_group", False):
        chats_payload = await _load_subscription_chats(db, subscription_id=sub.id)
    return _serialize_subscription(sub, chats=chats_payload)

def _normalize_subscription_media_filter(
    *,
    subscription_type: str,
    media_filter_raw: Optional[dict],
    prompt: str,
) -> Optional[dict]:
    """
    Привести payload.media_filter к финальному виду для записи в БД.

    Правила:
      • subscription_type='digest' → media_filter обнуляется
        (фильтр не предусмотрен для саммари).
      • если media_filter присутствует, но enabled=false или невалиден —
        обнуляем.
      • если медиафильтр не задан И prompt пуст → 422 (что-то одно
        должно быть, иначе подписке нечего отслеживать).

    Возвращает None или валидный dict для сохранения.
    """
    if subscription_type != "events":
        return None

    parsed = mf_integration.request_from_payload({"media_filter": media_filter_raw})
    if parsed is None:
        # Медиафильтра нет → классический режим. Текст обязателен.
        if not (prompt or "").strip():
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "SUBSCRIPTION_EMPTY",
                    "message": (
                        "Заполните текст запроса или включите медиафильтр — "
                        "подписке нужно что-то отслеживать."
                    ),
                },
            )
        return None

    # Сохраняем в БД канонический model_dump, чтобы избежать гнили мусорных полей.
    return parsed.model_dump(mode="json")


@app.put("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
async def update_subscription(
    subscription_id: int,
    payload: SubscriptionCreate,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    await ensure_can_update_subscription(
        db,
        user=user,
        sub=sub,
        requested_frequency_minutes=payload.frequency_minutes,
        requested_is_active=payload.is_active,
    )

    old_chat_ref = (sub.chat_ref or "").strip()
    old_source_mode = (sub.source_mode or "").strip().lower()
    old_is_group = bool(getattr(sub, "is_group", False))

    is_group = bool(payload.is_group)
    resolved_group_chats: list[dict] = []
    normalized_chat_ref: str
    chat_id = None
    chat_title = None
    chat_username = None

    if is_group:
        resolved_group_chats = await prepare_subscription_group_targets(
            db,
            owner_user_id=user.id,
            user=user,
            source_mode=payload.source_mode,
            chat_refs=payload.chats or [],
        )
        normalized_chat_ref = _normalize_group_marker(sub.id)
    else:
        normalized_chat_ref, chat_id, chat_title, chat_username = await prepare_subscription_target(
            db,
            owner_user_id=user.id,
            source_mode=payload.source_mode,
            chat_ref=payload.chat_ref or "",
        )

    ai_model = resolve_ai_model_for_user(
        user=user,
        requested_ai_model=payload.ai_model,
        fallback_ai_model=getattr(user, "default_ai_model", None),
    )

    sub_type = payload.subscription_type or "events"
    media_filter_clean = _normalize_subscription_media_filter(
        subscription_type=sub_type,
        media_filter_raw=payload.media_filter,
        prompt=payload.prompt,
    )

    sub.name = payload.name
    sub.source_mode = payload.source_mode
    sub.subscription_type = sub_type
    sub.is_group = is_group
    sub.chat_ref = normalized_chat_ref
    sub.chat_id = chat_id
    sub.frequency_minutes = payload.frequency_minutes
    sub.prompt = payload.prompt
    sub.media_filter = media_filter_clean
    sub.ai_model = ai_model
    sub.is_active = payload.is_active
    sub.status = "active" if payload.is_active else "paused"
    sub.last_error = None
    sub.updated_at = sa.func.now()

    await expire_trial_subscription_if_needed(db, sub=sub)

    # ---- SubscriptionState (общий tick reservation) ----
    state_res = await db.execute(
        select(SubscriptionState).where(SubscriptionState.subscription_id == subscription_id)
    )
    st = state_res.scalar_one_or_none()
    mode_changed = old_source_mode != payload.source_mode
    type_changed = old_is_group != is_group  # переключение одиночная↔групповая
    chat_changed_singleton = (not is_group) and (old_chat_ref != normalized_chat_ref)

    # Сбрасываем общий state, если:
    # - сменился режим (personal↔service);
    # - переключились между одиночной и групповой (логика курсора меняется);
    # - в одиночной сменился чат.
    if st and (mode_changed or type_changed or chat_changed_singleton):
        st.last_message_id = None
        st.last_checked_at = None
        st.last_success_at = None
        st.next_run_at = None

    # ---- subscription_chats + subscription_chat_state ----
    if is_group:
        await _replace_subscription_chats(
            db,
            subscription_id=sub.id,
            # Если переключались с одиночной — все чаты для нас новые,
            # курсор сохранять не от чего. Если уже была группа — сохраняем
            # курсор только для тех чатов, которые остались.
            resolved_chats=resolved_group_chats,
            reset_state_for_new_chats_only=(old_is_group and not type_changed),
        )
        for c in resolved_group_chats:
            await upsert_user_chat_history(
                db,
                owner_user_id=user.id,
                source_mode=payload.source_mode,
                chat_ref=c["chat_ref"],
                chat_title=c.get("chat_title"),
                chat_username=c.get("chat_username"),
                chat_id=c.get("chat_id"),
            )
    else:
        # Если стала одиночной — снести все subscription_chats / state.
        if old_is_group:
            await db.execute(
                delete(SubscriptionChat).where(SubscriptionChat.subscription_id == sub.id)
            )
            await db.execute(
                delete(SubscriptionChatState).where(SubscriptionChatState.subscription_id == sub.id)
            )

        await upsert_user_chat_history(
            db,
            owner_user_id=user.id,
            source_mode=payload.source_mode,
            chat_ref=normalized_chat_ref,
            chat_title=chat_title,
            chat_username=chat_username,
            chat_id=chat_id,
        )

    db.add(
        UsageEvent(
            user_id=user.id,
            event_type="subscription_updated",
            status="success_counted",
            source_mode=sub.source_mode,
            chat_ref=sub.chat_ref,
            subscription_id=sub.id,
            meta_json={
                "is_active": bool(sub.is_active),
                "frequency_minutes": int(sub.frequency_minutes),
                "is_group": bool(is_group),
                "group_size": len(resolved_group_chats) if is_group else None,
            },
        )
    )

    await db.commit()
    await db.refresh(sub)

    chats_payload = None
    if sub.is_group:
        chats_payload = await _load_subscription_chats(db, subscription_id=sub.id)
    return _serialize_subscription(sub, chats=chats_payload)

@app.post("/subscriptions/{subscription_id}/toggle", response_model=SubscriptionOut)
async def toggle_subscription(
    subscription_id: int,
    payload: ToggleRequest,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    # === Ручной запуск (play) запрещён при нехватке токенов ===
    # Если пользователь жмёт play, но баланс исчерпан — не включаем подписку
    # и отдаём понятную ошибку с подсказкой (докупить / апгрейд). На free
    # докупки нет — предлагаем только переход на платный тариф.
    if bool(payload.is_active):
        can_spend, _bal = await billing.check_can_spend(
            db, user_id=user.id, tier="light",
        )
        if not can_spend:
            plan_row = await get_user_plan(db, user)
            topup_enabled = bool(getattr(plan_row, "topup_enabled", False))
            lang = str(user.language or "en").lower()
            if lang.startswith("ru"):
                msg = "Недостаточно токенов для запуска подписки. " + (
                    "Докупите токены или перейдите на расширенный тариф."
                    if topup_enabled
                    else "Перейдите на платный тариф, чтобы запускать подписки."
                )
            else:
                msg = "Not enough tokens to start the subscription. " + (
                    "Buy more tokens or upgrade to a higher plan."
                    if topup_enabled
                    else "Upgrade to a paid plan to run subscriptions."
                )
            raise HTTPException(
                status_code=402,
                detail={"code": "INSUFFICIENT_TOKENS", "message": msg},
            )

    await ensure_can_toggle_subscription(
        db,
        user=user,
        sub=sub,
        target_is_active=payload.is_active,
    )

    sub.is_active = bool(payload.is_active)
    sub.status = "active" if payload.is_active else "paused"
    sub.updated_at = sa.func.now()

    await expire_trial_subscription_if_needed(db, sub=sub)

    db.add(
        UsageEvent(
            user_id=user.id,
            event_type="subscription_resumed" if payload.is_active else "subscription_paused",
            status="success_counted",
            source_mode=sub.source_mode,
            chat_ref=sub.chat_ref,
            subscription_id=sub.id,
            meta_json={
                "is_trial": bool(sub.is_trial),
            },
        )
    )

    await db.commit()
    await db.refresh(sub)

    chats_payload = None
    if getattr(sub, "is_group", False):
        chats_payload = await _load_subscription_chats(db, subscription_id=sub.id)
    return _serialize_subscription(sub, chats=chats_payload)

@app.post("/subscriptions", response_model=SubscriptionOut)
async def create_subscription(
    payload: SubscriptionCreate,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    plan, is_trial, trial_started_at, trial_ends_at = await ensure_can_create_subscription(
        db,
        user=user,
        frequency_minutes=payload.frequency_minutes,
        requested_is_active=payload.is_active,
    )

    # ---- Группа vs одиночная ----
    is_group = bool(payload.is_group)
    resolved_group_chats: list[dict] = []

    if is_group:
        resolved_group_chats = await prepare_subscription_group_targets(
            db,
            owner_user_id=user.id,
            user=user,
            source_mode=payload.source_mode,
            chat_refs=payload.chats or [],
        )
        # Для группы chat_ref в самой Subscription — синтетический "group:<id>",
        # реальный список — в subscription_chats. chat_id/chat_title для
        # Subscription не имеют смысла (NULL).
        normalized_chat_ref = "group:pending"  # перезапишем после flush, когда узнаем sub.id
        chat_id = None
        chat_title = None
        chat_username = None
    else:
        normalized_chat_ref, chat_id, chat_title, chat_username = await prepare_subscription_target(
            db,
            owner_user_id=user.id,
            source_mode=payload.source_mode,
            chat_ref=payload.chat_ref or "",
        )

    ai_model = resolve_ai_model_for_user(
        user=user,
        requested_ai_model=payload.ai_model,
        fallback_ai_model=getattr(user, "default_ai_model", None),
    )

    sub_type = payload.subscription_type or "events"
    media_filter_clean = _normalize_subscription_media_filter(
        subscription_type=sub_type,
        media_filter_raw=payload.media_filter,
        prompt=payload.prompt,
    )

    sub = Subscription(
        owner_user_id=user.id,
        name=payload.name,
        source_mode=payload.source_mode,
        subscription_type=sub_type,
        is_group=is_group,
        chat_ref=normalized_chat_ref,
        chat_id=chat_id,
        frequency_minutes=payload.frequency_minutes,
        prompt=payload.prompt,
        media_filter=media_filter_clean,
        is_active=payload.is_active,
        status="active" if payload.is_active else "paused",
        last_error=None,
        is_trial=is_trial,
        trial_started_at=trial_started_at,
        trial_ends_at=trial_ends_at,
        ai_model=ai_model,
    )

    user_plan_code = str(getattr(user, "plan", "") or "").strip().lower()
    if user_plan_code == "free":
        sub.is_trial = True
        sub.trial_started_at = trial_started_at or datetime.now(timezone.utc)
        sub.trial_ends_at = trial_ends_at or (
                sub.trial_started_at + timedelta(days=int(plan.trial_subscription_duration_days))
        )

    db.add(sub)
    await db.flush()

    # Для группы — перезаписываем chat_ref на финальный "group:<sub.id>"
    if is_group:
        sub.chat_ref = _normalize_group_marker(sub.id)

    st = SubscriptionState(
        subscription_id=sub.id,
        last_message_id=None,
        last_checked_at=None,
        last_success_at=None,
        next_run_at=None,
    )
    db.add(st)

    # Для группы — наполняем subscription_chats и subscription_chat_state
    if is_group:
        await _replace_subscription_chats(
            db,
            subscription_id=sub.id,
            resolved_chats=resolved_group_chats,
            reset_state_for_new_chats_only=False,
        )

        # История чатов — добавляем каждый чат группы, чтобы они подтягивались
        # в подсказках UI наравне с одиночными.
        for c in resolved_group_chats:
            await upsert_user_chat_history(
                db,
                owner_user_id=user.id,
                source_mode=payload.source_mode,
                chat_ref=c["chat_ref"],
                chat_title=c.get("chat_title"),
                chat_username=c.get("chat_username"),
                chat_id=c.get("chat_id"),
            )
    else:
        await upsert_user_chat_history(
            db,
            owner_user_id=user.id,
            source_mode=payload.source_mode,
            chat_ref=normalized_chat_ref,
            chat_title=chat_title,
            chat_username=chat_username,
            chat_id=chat_id,
        )

    db.add(
        UsageEvent(
            user_id=user.id,
            event_type="subscription_created",
            status="success_counted",
            source_mode=payload.source_mode,
            chat_ref=sub.chat_ref,
            subscription_id=sub.id,
            meta_json={
                "is_trial": bool(is_trial),
                "is_active": bool(payload.is_active),
                "frequency_minutes": int(payload.frequency_minutes),
                "is_group": bool(is_group),
                "group_size": len(resolved_group_chats) if is_group else None,
            },
        )
    )

    await db.commit()
    await db.refresh(sub)

    chats_payload = resolved_group_chats if is_group else None
    if is_group:
        # Подгружаем из БД с правильным position, чтобы отдать клиенту в финальной форме.
        chats_payload = await _load_subscription_chats(db, subscription_id=sub.id)
    return _serialize_subscription(sub, chats=chats_payload)

@app.delete("/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    ensure_can_delete_subscription(user=user, sub=sub)

    db.add(
        UsageEvent(
            user_id=user.id,
            event_type="subscription_deleted",
            status="success_counted",
            source_mode=sub.source_mode,
            chat_ref=sub.chat_ref,
            subscription_id=sub.id,
            meta_json={
                "is_trial": bool(sub.is_trial),
            },
        )
    )

    await db.execute(delete(MatchEvent).where(MatchEvent.subscription_id == subscription_id))
    await db.execute(delete(DigestEvent).where(DigestEvent.subscription_id == subscription_id))
    await db.execute(delete(SubscriptionState).where(SubscriptionState.subscription_id == subscription_id))
    # Групповые таблицы — для групповых подписок. Для одиночных строк нет, DELETE no-op.
    await db.execute(delete(SubscriptionChatState).where(SubscriptionChatState.subscription_id == subscription_id))
    await db.execute(delete(SubscriptionChat).where(SubscriptionChat.subscription_id == subscription_id))
    await db.execute(delete(Subscription).where(Subscription.id == subscription_id))

    await db.commit()
    return {"status": "ok", "deleted_subscription_id": subscription_id}

@app.post("/subscriptions/{subscription_id}/switch-mode", response_model=SubscriptionOut)
async def switch_subscription_mode(
    subscription_id: int,
    payload: SubscriptionSwitchModeRequest,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(
        select(Subscription).where(Subscription.id == subscription_id)
    )
    sub = res.scalar_one_or_none()

    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")

    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    # Switch-mode для групповых подписок запрещён.
    # Для service-режима групповые подписки в принципе не поддерживаются
    # (нет fetch-логики, нет UI), а ручной свитч одного формата в другой
    # с N чатами потребовал бы заново валидировать все чаты в новом
    # режиме — задача за пределами MVP.
    if bool(getattr(sub, "is_group", False)):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "SWITCH_MODE_NOT_ALLOWED_FOR_GROUP",
                "message": "Смена режима недоступна для групповой подписки. Создайте новую.",
            },
        )

    target_mode = (payload.target_source_mode or "").strip().lower()
    current_mode = (sub.source_mode or "personal").strip().lower()

    if target_mode not in {"personal", "service"}:
        raise HTTPException(status_code=400, detail="INVALID_SOURCE_MODE")

    if current_mode == target_mode:
        return _serialize_subscription(sub, chats=None)

    normalized_chat_ref, chat_id, chat_title, chat_username = await prepare_subscription_target(
        db,
        owner_user_id=user.id,
        source_mode=target_mode,
        chat_ref=sub.chat_ref,
    )

    sub.source_mode = target_mode
    sub.chat_ref = normalized_chat_ref
    sub.chat_id = chat_id
    sub.last_error = None
    sub.status = "active" if sub.is_active else "paused"
    sub.updated_at = sa.func.now()

    await reset_subscription_state(db, subscription_id=sub.id)

    await upsert_user_chat_history(
        db,
        owner_user_id=user.id,
        source_mode=target_mode,
        chat_ref=normalized_chat_ref,
        chat_title=chat_title,
        chat_username=chat_username,
        chat_id=chat_id,
    )

    await db.commit()
    await db.refresh(sub)
    return _serialize_subscription(sub, chats=None)


# ---------------------------------------------------------------------------
# Сохранённые запросы (пресеты) — saved_queries
# ---------------------------------------------------------------------------
# Привязаны к пользователю. Хранят имя + params_json (снапшот настроек формы
# запроса). Фронт сериализует настройки в params_json и defensive-парсит их
# обратно при применении. Лимит на количество — защита от абьюза.

SAVED_QUERIES_MAX_PER_USER = 50


@app.get("/saved-queries", response_model=list[SavedQueryOut])
async def list_saved_queries(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Список пресетов пользователя. Сортировка: недавно использованные
    сверху (last_used_at desc, NULL — в конец), затем по дате создания."""
    stmt = (
        select(SavedQuery)
        .where(SavedQuery.user_id == user.id)
        .order_by(
            SavedQuery.last_used_at.desc().nullslast(),
            SavedQuery.created_at.desc(),
        )
    )
    res = await db.execute(stmt)
    return list(res.scalars().all())


@app.post("/saved-queries", response_model=SavedQueryOut, status_code=201)
async def create_saved_query(
    payload: SavedQueryCreate,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="SAVED_QUERY_NAME_EMPTY")

    count_res = await db.execute(
        select(sa.func.count())
        .select_from(SavedQuery)
        .where(SavedQuery.user_id == user.id)
    )
    if int(count_res.scalar() or 0) >= SAVED_QUERIES_MAX_PER_USER:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "SAVED_QUERIES_LIMIT_REACHED",
                "message": (
                    f"Достигнут лимит сохранённых запросов "
                    f"({SAVED_QUERIES_MAX_PER_USER}). Удалите ненужные."
                ),
                "limit": SAVED_QUERIES_MAX_PER_USER,
            },
        )

    sq = SavedQuery(user_id=user.id, name=name, params_json=payload.params_json)
    db.add(sq)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "code": "SAVED_QUERY_NAME_TAKEN",
                "message": "Запрос с таким названием уже сохранён.",
            },
        )
    await db.refresh(sq)
    return sq


@app.put("/saved-queries/{saved_query_id}", response_model=SavedQueryOut)
async def update_saved_query(
    saved_query_id: int,
    payload: SavedQueryUpdate,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(SavedQuery).where(SavedQuery.id == saved_query_id))
    sq = res.scalar_one_or_none()
    if not sq:
        raise HTTPException(status_code=404, detail="SAVED_QUERY_NOT_FOUND")
    if sq.user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    if payload.name is not None:
        new_name = payload.name.strip()
        if not new_name:
            raise HTTPException(status_code=422, detail="SAVED_QUERY_NAME_EMPTY")
        sq.name = new_name
    if payload.params_json is not None:
        sq.params_json = payload.params_json

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "code": "SAVED_QUERY_NAME_TAKEN",
                "message": "Запрос с таким названием уже сохранён.",
            },
        )
    await db.refresh(sq)
    return sq


@app.post("/saved-queries/{saved_query_id}/touch", response_model=SavedQueryOut)
async def touch_saved_query(
    saved_query_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Отметить пресет применённым — обновляет last_used_at для сортировки
    «недавние». Вызывается фронтом при выборе пресета."""
    res = await db.execute(select(SavedQuery).where(SavedQuery.id == saved_query_id))
    sq = res.scalar_one_or_none()
    if not sq:
        raise HTTPException(status_code=404, detail="SAVED_QUERY_NOT_FOUND")
    if sq.user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    sq.last_used_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(sq)
    return sq


@app.delete("/saved-queries/{saved_query_id}")
async def delete_saved_query(
    saved_query_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(SavedQuery).where(SavedQuery.id == saved_query_id))
    sq = res.scalar_one_or_none()
    if not sq:
        raise HTTPException(status_code=404, detail="SAVED_QUERY_NOT_FOUND")
    if sq.user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    await db.delete(sq)
    await db.commit()
    return {"ok": True}


@app.post("/tg/bot/dispatch")
async def tg_bot_dispatch(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    t0 = time.perf_counter()

    # 1) куда слать — только для текущего пользователя
    r = await db.execute(
        select(BotUserLink)
        .where(
            BotUserLink.owner_user_id == user.id,
            BotUserLink.is_blocked == False
        )
        .order_by(BotUserLink.id.desc())
    )
    link = r.scalars().first()
    if not link:
        return {"status": "error", "error": "NO_BOT_USER_LINK"}

    dest_chat_id = link.telegram_chat_id

    # 2) queued события — только подписки текущего пользователя
    r2 = await db.execute(
        select(MatchEvent, Subscription)
        .join(Subscription, Subscription.id == MatchEvent.subscription_id)
        .where(
            MatchEvent.notify_status == "queued",
            Subscription.owner_user_id == user.id,
        )
        .order_by(MatchEvent.subscription_id.asc(), MatchEvent.id.asc())
        .limit(200)
    )

    rows = list(r2.all())

    if not rows:
        elapsed = round(time.perf_counter() - t0, 2)
        return {
            "status": "ok",
            "events_total": 0,
            "sent_groups": 0,
            "failed_groups": 0,
            "elapsed_seconds": elapsed
        }

    grouped = {}
    for ev, sub in rows:
        sid = int(ev.subscription_id)
        if sid not in grouped:
            grouped[sid] = {"sub": sub, "events": []}
        grouped[sid]["events"].append(ev)

    sent_groups = 0
    failed_groups = 0
    events_total = len(rows)

    for sid, pack in grouped.items():
        sub = pack["sub"]
        events = pack["events"]

        try:
            max_items = 10
            shown = events[:max_items]
            rest = len(events) - len(shown)

            header = (
                f"Найдены события по подписке: {sub.name or f'#{sid}'}\n"
                f"Совпадений: {len(events)}\n"
            )

            lines = []
            for i, ev in enumerate(shown, start=1):
                author = ev.author_display or (str(ev.author_id) if ev.author_id else "—")
                ts = ev.message_ts.isoformat() if ev.message_ts else "—"

                excerpt = (ev.excerpt or "").strip()
                if len(excerpt) > 300:
                    excerpt = excerpt[:300].rstrip() + "…"

                url = build_tg_message_link(
                    chat_ref=getattr(sub, "chat_ref", None),
                    chat_id=getattr(sub, "chat_id", None),
                    message_id=int(ev.message_id),
                )
                link_text = f"\n{url}" if url else ""

                lines.append(
                    f"\n{i}) {author} • {ts}\n"
                    f"{excerpt or '—'}"
                    f"{link_text}"
                )

            if rest > 0:
                lines.append(f"\n\n…и ещё {rest} совпадений.")

            text = header + "".join(lines)

            await bot_send_message(dest_chat_id, text)

            for ev in events:
                ev.notify_status = "sent"
                db.add(ev)

            sent_groups += 1

        except Exception as e:
            for ev in events:
                ev.notify_status = "failed"
                db.add(ev)

            failed_groups += 1
            print("DISPATCH_GROUP_FAILED", sid, str(e))

    await db.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    return {
        "status": "ok",
        "events_total": events_total,
        "groups_total": len(grouped),
        "sent_groups": sent_groups,
        "failed_groups": failed_groups,
        "elapsed_seconds": elapsed,
    }