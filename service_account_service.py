import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import sqlalchemy as sa
from openai import OpenAI
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.types import Message

from db.models import (
    ServicePhoneNumber,
    ServiceTelegramAccount,
    ServiceTelegramSession,
    ServiceAccountLog,
    ServiceAccountStatusHistory,
)
from telegram_service import decrypt_session


# =========================
# Константы MVP
# =========================

SERVICE_ACCOUNT_LIMIT_PER_MINUTE = 2
SERVICE_ACCOUNT_LIMIT_PER_HOUR = 20
SERVICE_ACCOUNT_LIMIT_PER_DAY = 100
SERVICE_ACCOUNT_MAX_FETCH_MESSAGES = 1000
SERVICE_ACCOUNT_MAX_ATTEMPTS = 2

SERVICE_ACCOUNT_ALLOWED_STATUS = "active"

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]


# =========================
# Ошибки
# =========================

class ServiceAccountError(Exception):
    def __init__(self, code: str, user_message: str, http_status: int = 400):
        super().__init__(code)
        self.code = code
        self.user_message = user_message
        self.http_status = http_status


@dataclass
class AllocatedAccount:
    account: ServiceTelegramAccount
    busy_log_id: int


# =========================
# Кэш Telethon-клиентов
# =========================

_service_clients: dict[int, TelegramClient] = {}


# =========================
# Базовые helper-функции
# =========================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_public_chat_ref(chat_ref: str) -> str:
    """
    Разрешаем только:
    - @username
    - username
    - https://t.me/username
    - http://t.me/username

    Не разрешаем:
    - invite links (+HASH / joinchat)
    - приватные ссылки
    - пустые значения
    """
    ref = (chat_ref or "").strip()
    if not ref:
        raise ServiceAccountError(
            code="CHAT_LINK_REQUIRED",
            user_message="Укажите публичный username или ссылку на публичный чат/канал.",
            http_status=400,
        )

    if "t.me/+" in ref or "joinchat/" in ref:
        raise ServiceAccountError(
            code="SERVICE_PUBLIC_ONLY",
            user_message="Служебный режим работает только с публичными чатами и каналами.",
            http_status=400,
        )

    ref = ref.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    ref = ref.strip("/")
    if ref.startswith("@"):
        ref = ref[1:].strip()

    if not ref:
        raise ServiceAccountError(
            code="CHAT_LINK_REQUIRED",
            user_message="Укажите публичный username или ссылку на публичный чат/канал.",
            http_status=400,
        )

    # На MVP в service-режиме поддерживаем именно публичные username.
    # numeric chat id пока не поддерживаем — он не даёт надёжно понять публичность.
    if ref.isdigit():
        raise ServiceAccountError(
            code="SERVICE_USERNAME_ONLY",
            user_message="В служебном режиме пока поддерживаются только публичные username и публичные t.me ссылки.",
            http_status=400,
        )

    return ref


async def log_event(
    db: AsyncSession,
    *,
    service_account_id: Optional[int],
    event_type: str,
    target_ref: Optional[str] = None,
    is_success: Optional[bool] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    event_at: Optional[datetime] = None,
    started_at: Optional[datetime] = None,
    finished_at: Optional[datetime] = None,
    meta_json: Optional[dict] = None,
) -> ServiceAccountLog:
    row = ServiceAccountLog(
        service_account_id=service_account_id,
        event_type=event_type,
        target_ref=target_ref,
        is_success=is_success,
        error_code=error_code,
        error_message=error_message,
        event_at=event_at,
        started_at=started_at,
        finished_at=finished_at,
        meta_json=meta_json,
    )
    db.add(row)
    await db.flush()
    return row


async def write_status_history(
    db: AsyncSession,
    *,
    service_account_id: int,
    old_status: Optional[str],
    new_status: str,
    reason: Optional[str],
) -> None:
    row = ServiceAccountStatusHistory(
        service_account_id=service_account_id,
        old_status=old_status,
        new_status=new_status,
        reason=reason,
    )
    db.add(row)
    await db.flush()


async def set_account_status(
    db: AsyncSession,
    *,
    account: ServiceTelegramAccount,
    new_status: str,
    reason: Optional[str] = None,
) -> None:
    old_status = account.status
    if old_status == new_status:
        return

    account.status = new_status
    account.updated_at = utcnow()

    await write_status_history(
        db,
        service_account_id=account.id,
        old_status=old_status,
        new_status=new_status,
        reason=reason,
    )


async def recount_request_counters(db: AsyncSession, account: ServiceTelegramAccount) -> None:
    now = utcnow()

    minute_since = now - timedelta(minutes=1)
    hour_since = now - timedelta(hours=1)
    day_since = now - timedelta(days=1)

    stmt_min = select(func.count()).select_from(ServiceAccountLog).where(
        ServiceAccountLog.service_account_id == account.id,
        ServiceAccountLog.event_type == "account_taken",
        ServiceAccountLog.event_at >= minute_since,
    )
    stmt_hour = select(func.count()).select_from(ServiceAccountLog).where(
        ServiceAccountLog.service_account_id == account.id,
        ServiceAccountLog.event_type == "account_taken",
        ServiceAccountLog.event_at >= hour_since,
    )
    stmt_day = select(func.count()).select_from(ServiceAccountLog).where(
        ServiceAccountLog.service_account_id == account.id,
        ServiceAccountLog.event_type == "account_taken",
        ServiceAccountLog.event_at >= day_since,
    )

    account.requests_last_minute = int((await db.execute(stmt_min)).scalar_one() or 0)
    account.requests_last_hour = int((await db.execute(stmt_hour)).scalar_one() or 0)
    account.requests_last_day = int((await db.execute(stmt_day)).scalar_one() or 0)
    account.updated_at = now
    await db.flush()


def account_is_within_limits(account: ServiceTelegramAccount) -> bool:
    return (
        (account.requests_last_minute or 0) < SERVICE_ACCOUNT_LIMIT_PER_MINUTE
        and (account.requests_last_hour or 0) < SERVICE_ACCOUNT_LIMIT_PER_HOUR
        and (account.requests_last_day or 0) < SERVICE_ACCOUNT_LIMIT_PER_DAY
    )


# =========================
# Выбор и блокировка аккаунта
# =========================

async def allocate_service_account(
    db: AsyncSession,
    *,
    target_ref: str,
) -> AllocatedAccount:
    """
    На MVP делаем блокировку через PostgreSQL:
    - выбираем кандидатов
    - берём SELECT ... FOR UPDATE SKIP LOCKED
    - помечаем аккаунт как busy
    """
    now = utcnow()

    stmt = (
        select(ServiceTelegramAccount)
        .join(ServicePhoneNumber, ServicePhoneNumber.id == ServiceTelegramAccount.phone_number_id)
        .where(
            ServiceTelegramAccount.status == SERVICE_ACCOUNT_ALLOWED_STATUS,
            ServiceTelegramAccount.is_enabled.is_(True),
            ServiceTelegramAccount.is_busy.is_(False),
            sa.or_(
                ServiceTelegramAccount.cooldown_until.is_(None),
                ServiceTelegramAccount.cooldown_until <= now,
            ),
            ServicePhoneNumber.is_active.is_(True),
        )
        .order_by(
            ServiceTelegramAccount.requests_last_hour.asc(),
            ServiceTelegramAccount.requests_last_minute.asc(),
            ServiceTelegramAccount.last_used_at.asc().nullsfirst(),
            ServiceTelegramAccount.id.asc(),
        )
        .limit(10)
        .with_for_update(skip_locked=True)
    )

    result = await db.execute(stmt)
    candidates = list(result.scalars().all())

    for account in candidates:
        await recount_request_counters(db, account)

        if not account_is_within_limits(account):
            continue

        account.is_busy = True
        account.busy_started_at = now
        account.updated_at = now

        await log_event(
            db,
            service_account_id=account.id,
            event_type="account_taken",
            target_ref=target_ref,
            is_success=True,
            event_at=now,
        )

        busy_log = await log_event(
            db,
            service_account_id=account.id,
            event_type="account_busy_window",
            target_ref=target_ref,
            started_at=now,
            is_success=None,
        )

        await db.flush()
        return AllocatedAccount(account=account, busy_log_id=busy_log.id)

    await log_event(
        db,
        service_account_id=None,
        event_type="no_free_account",
        target_ref=target_ref,
        is_success=False,
        error_code="NO_FREE_ACCOUNT",
        error_message="Не найден свободный служебный аккаунт.",
        event_at=now,
    )
    await db.flush()

    raise ServiceAccountError(
        code="NO_FREE_ACCOUNT",
        user_message="Сейчас все служебные аккаунты заняты. Попробуйте чуть позже.",
        http_status=503,
    )


async def release_service_account(
    db: AsyncSession,
    *,
    account: ServiceTelegramAccount,
    busy_log_id: int,
    success: bool,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    now = utcnow()

    account.is_busy = False
    account.busy_started_at = None
    account.updated_at = now

    busy_log = await db.get(ServiceAccountLog, busy_log_id)
    if busy_log:
        busy_log.finished_at = now
        busy_log.is_success = success
        if error_code:
            busy_log.error_code = error_code
        if error_message:
            busy_log.error_message = error_message

    await recount_request_counters(db, account)
    await db.flush()


# =========================
# Service session / Telethon client
# =========================

async def load_service_session_string(db: AsyncSession, service_account_id: int) -> str:
    stmt = (
        select(ServiceTelegramSession)
        .where(
            ServiceTelegramSession.service_account_id == service_account_id,
            ServiceTelegramSession.is_active.is_(True),
            ServiceTelegramSession.revoked_at.is_(None),
        )
        .order_by(ServiceTelegramSession.id.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    row = result.scalar_one_or_none()

    if not row:
        raise ServiceAccountError(
            code="SERVICE_SESSION_NOT_FOUND",
            user_message="Для служебного аккаунта не найдена активная сессия.",
            http_status=500,
        )

    try:
        return decrypt_session(row.session_ciphertext)
    except Exception as e:
        raise ServiceAccountError(
            code="SERVICE_SESSION_DECRYPT_FAILED",
            user_message="Не удалось прочитать сессию служебного аккаунта.",
            http_status=500,
        ) from e


async def get_service_tg_client(db: AsyncSession, service_account_id: int) -> TelegramClient:
    client = _service_clients.get(service_account_id)
    if client is not None:
        if not client.is_connected():
            await client.connect()
        return client

    session_string = await load_service_session_string(db, service_account_id)
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.connect()

    _service_clients[service_account_id] = client
    return client


async def invalidate_service_client(service_account_id: int) -> None:
    client = _service_clients.pop(service_account_id, None)
    if client is not None:
        try:
            await client.disconnect()
        except Exception:
            pass


# =========================
# Telegram fetch
# =========================

async def fetch_public_chat_messages(
    db: AsyncSession,
    *,
    service_account_id: int,
    chat_ref: str,
    days: int,
) -> Tuple[object, list[dict]]:
    client = await get_service_tg_client(db, service_account_id)

    if not await client.is_user_authorized():
        raise errors.AuthKeyUnregisteredError(request=None)  # искусственно приводим к общему flow

    username = normalize_public_chat_ref(chat_ref)

    entity = await client.get_entity(username)

    since_dt = utcnow() - timedelta(days=int(days))
    collected: list[dict] = []

    async for msg in client.iter_messages(entity, limit=SERVICE_ACCOUNT_MAX_FETCH_MESSAGES):
        if not isinstance(msg, Message):
            continue

        if not msg.date:
            continue

        msg_dt = msg.date
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=timezone.utc)

        if msg_dt < since_dt:
            break

        text = (msg.message or "").strip()
        if not text:
            continue

        sender_name = "Unknown"
        try:
            sender = await msg.get_sender()
            if sender is not None:
                if getattr(sender, "username", None):
                    sender_name = "@" + sender.username
                else:
                    first = (getattr(sender, "first_name", "") or "").strip()
                    last = (getattr(sender, "last_name", "") or "").strip()
                    sender_name = (first + " " + last).strip() or "Unknown"
        except Exception:
            pass

        collected.append(
            {
                "date": msg_dt.isoformat(),
                "from": sender_name,
                "text": text,
            }
        )

    collected.reverse()
    return entity, collected


# =========================
# LLM summary
# =========================

async def summarize_chat(
    *,
    user_query: str,
    chat_name: str,
    text_messages: list[dict],
) -> str:
    if not text_messages:
        return "В чате нет текстовых сообщений для анализа."

    lines = []
    for msg in text_messages:
        date = msg.get("date") or ""
        sender = msg.get("from") or "Unknown"
        text = msg.get("text") or ""
        lines.append(f"[{date}] {sender}: {text}")

    context = "\n".join(lines)

    system_prompt = (
        "Ты аналитик переписок в Telegram.\n"
        "Тебе даётся фрагмент чата и запрос пользователя.\n"
        "Найди по смыслу релевантные сообщения и дай краткое, структурированное summary по-русски.\n"
        "Если информации мало, честно скажи об этом."
    )

    user_prompt = (
        f"Название чата: {chat_name}\n\n"
        f"Запрос пользователя:\n{user_query}\n\n"
        "Ниже переписка (от старых к новым сообщениям):\n\n"
        f"{context}\n\n"
        "Сделай ответ именно по запросу выше. Структурируй ответ в 3–6 абзацев или списком."
    )

    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    return completion.choices[0].message.content.strip()


# =========================
# Основной orchestration-flow
# =========================

async def analyze_chat_via_service_account(
    db: AsyncSession,
    *,
    chat_link: str,
    user_query: str,
    days: int,
) -> dict:
    last_error: Optional[ServiceAccountError] = None
    normalized_ref = normalize_public_chat_ref(chat_link)

    for _attempt in range(SERVICE_ACCOUNT_MAX_ATTEMPTS):
        allocated = await allocate_service_account(db, target_ref=normalized_ref)
        account = allocated.account

        try:
            entity, messages = await fetch_public_chat_messages(
                db,
                service_account_id=account.id,
                chat_ref=normalized_ref,
                days=days,
            )

            chat_name = (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or "Без названия"
            )

            summary = await summarize_chat(
                user_query=user_query,
                chat_name=chat_name,
                text_messages=messages,
            )

            account.last_used_at = utcnow()
            account.last_error = None
            account.last_error_at = None
            account.consecutive_fail_count = 0
            account.updated_at = utcnow()

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_finished",
                target_ref=normalized_ref,
                is_success=True,
                event_at=utcnow(),
            )

            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=True,
            )

            await db.commit()

            return {
                "status": "ok",
                "summary": summary,
                "chat_name": chat_name,
                "messages_count": len(messages),
                "source_mode": "service",
            }

        except ServiceAccountError as e:
            # это уже user-facing ошибка валидации / конфигурации
            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code=e.code,
                error_message=e.user_message,
            )

            account.last_error = e.code
            account.last_error_at = utcnow()
            account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_failed",
                target_ref=normalized_ref,
                is_success=False,
                error_code=e.code,
                error_message=e.user_message,
                event_at=utcnow(),
            )

            await db.commit()
            raise

        except errors.FloodWaitError as e:
            account.last_error = "FLOOD_WAIT"
            account.last_error_at = utcnow()
            account.cooldown_until = utcnow() + timedelta(seconds=int(e.seconds))
            account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

            await set_account_status(
                db,
                account=account,
                new_status="cooldown",
                reason=f"FLOOD_WAIT_{int(e.seconds)}",
            )

            await log_event(
                db,
                service_account_id=account.id,
                event_type="telegram_error",
                target_ref=normalized_ref,
                is_success=False,
                error_code="FLOOD_WAIT",
                error_message=f"Flood wait: {int(e.seconds)} seconds",
                event_at=utcnow(),
            )

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_failed",
                target_ref=normalized_ref,
                is_success=False,
                error_code="FLOOD_WAIT",
                error_message=f"Flood wait: {int(e.seconds)} seconds",
                event_at=utcnow(),
            )

            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code="FLOOD_WAIT",
                error_message=f"Flood wait: {int(e.seconds)} seconds",
            )

            await db.commit()

            last_error = ServiceAccountError(
                code="FLOOD_WAIT",
                user_message="Сервисные аккаунты временно заняты. Попробуйте чуть позже.",
                http_status=503,
            )
            continue

        except (
            errors.AuthKeyUnregisteredError,
            errors.SessionRevokedError,
        ) as e:
            account.last_error = type(e).__name__
            account.last_error_at = utcnow()
            account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

            await set_account_status(
                db,
                account=account,
                new_status="needs_reauth",
                reason=type(e).__name__,
            )

            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code="NEEDS_REAUTH",
                error_message=type(e).__name__,
            )

            await log_event(
                db,
                service_account_id=account.id,
                event_type="reauth_required",
                target_ref=normalized_ref,
                is_success=False,
                error_code="NEEDS_REAUTH",
                error_message=type(e).__name__,
                event_at=utcnow(),
            )

            await invalidate_service_client(account.id)
            await db.commit()

            last_error = ServiceAccountError(
                code="SERVICE_ACCOUNT_REAUTH_REQUIRED",
                user_message="Один из служебных аккаунтов требует повторной авторизации.",
                http_status=503,
            )
            continue

        except (
            errors.UsernameInvalidError,
            errors.UsernameNotOccupiedError,
        ):
            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code="PUBLIC_CHAT_NOT_FOUND",
                error_message="Публичный чат или канал не найден.",
            )

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_failed",
                target_ref=normalized_ref,
                is_success=False,
                error_code="PUBLIC_CHAT_NOT_FOUND",
                error_message="Публичный чат или канал не найден.",
                event_at=utcnow(),
            )

            await db.commit()

            raise ServiceAccountError(
                code="PUBLIC_CHAT_NOT_FOUND",
                user_message="Публичный чат или канал не найден.",
                http_status=404,
            )

        except (
            errors.ChannelPrivateError,
            errors.ChatAdminRequiredError,
        ):
            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code="SERVICE_PUBLIC_ONLY",
                error_message="Чат недоступен для чтения служебным аккаунтом.",
            )

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_failed",
                target_ref=normalized_ref,
                is_success=False,
                error_code="SERVICE_PUBLIC_ONLY",
                error_message="Чат недоступен для чтения служебным аккаунтом.",
                event_at=utcnow(),
            )

            await db.commit()

            raise ServiceAccountError(
                code="SERVICE_PUBLIC_ONLY",
                user_message="Служебный режим работает только с публичными чатами и каналами.",
                http_status=400,
            )

        except Exception as e:
            account.last_error = type(e).__name__
            account.last_error_at = utcnow()
            account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

            # мягкий cooldown на неожиданные ошибки
            account.cooldown_until = utcnow() + timedelta(minutes=10)
            await set_account_status(
                db,
                account=account,
                new_status="cooldown",
                reason=type(e).__name__,
            )

            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code="SERVICE_ACCOUNT_INTERNAL_ERROR",
                error_message=str(e),
            )

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_failed",
                target_ref=normalized_ref,
                is_success=False,
                error_code="SERVICE_ACCOUNT_INTERNAL_ERROR",
                error_message=str(e),
                event_at=utcnow(),
            )

            await db.commit()

            last_error = ServiceAccountError(
                code="SERVICE_ACCOUNT_INTERNAL_ERROR",
                user_message="Не удалось обработать запрос через служебный аккаунт. Попробуйте чуть позже.",
                http_status=503,
            )
            continue

    if last_error:
        raise last_error

    raise ServiceAccountError(
        code="SERVICE_ACCOUNT_INTERNAL_ERROR",
        user_message="Не удалось обработать запрос через служебный аккаунт.",
        http_status=503,
    )