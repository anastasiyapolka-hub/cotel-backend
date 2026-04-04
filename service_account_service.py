import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import sqlalchemy as sa
from openai import AsyncOpenAI
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import Message, Channel, Chat

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

SERVICE_ACCOUNT_LIMIT_PER_MINUTE = 10
SERVICE_ACCOUNT_LIMIT_PER_HOUR = 100
SERVICE_ACCOUNT_LIMIT_PER_DAY = 300
SERVICE_ACCOUNT_MAX_FETCH_MESSAGES = 1000
SERVICE_ACCOUNT_MAX_ATTEMPTS = 2

SERVICE_ACCOUNT_ALLOWED_STATUS = "active"

SERVICE_USAGE_ROLE_ANALYSIS = "analysis"
SERVICE_USAGE_ROLE_SUBSCRIPTIONS = "subscriptions"
SERVICE_USAGE_ROLE_SHARED = "shared"


def get_usage_role_chain(operation_kind: str) -> list[str]:
    kind = (operation_kind or "analysis").strip().lower()

    if kind == "subscription":
        return [
            SERVICE_USAGE_ROLE_SUBSCRIPTIONS,
            SERVICE_USAGE_ROLE_SHARED,
        ]

    # analysis / validate / interactive
    return [
        SERVICE_USAGE_ROLE_ANALYSIS,
        SERVICE_USAGE_ROLE_SHARED,
        SERVICE_USAGE_ROLE_SUBSCRIPTIONS,
    ]

openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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

async def safe_rollback(db: AsyncSession) -> None:
    try:
        await db.rollback()
    except Exception:
        pass

INVITE_RE = re.compile(r"(?:https?://)?t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)")

def extract_invite_hash(chat_ref: str) -> Optional[str]:
    ref = (chat_ref or "").strip()
    if not ref:
        return None

    m = INVITE_RE.search(ref)
    if m:
        return m.group(1)

    if ref.startswith("+") and len(ref) > 1:
        return ref[1:]

    if ref.startswith("joinchat/"):
        return ref.split("joinchat/", 1)[1].strip("/")

    return None

def normalize_public_chat_ref(chat_ref: str) -> str:
    """
    Теперь разрешаем:
    - @username
    - username
    - https://t.me/username
    - numeric chat id
    - invite links вида t.me/+HASH и t.me/joinchat/HASH
    """
    ref = (chat_ref or "").strip()
    if not ref:
        raise ServiceAccountError(
            code="CHAT_LINK_REQUIRED",
            user_message="Укажите username, ссылку на чат/канал или числовой chat id.",
            http_status=400,
        )

    invite_hash = extract_invite_hash(ref)
    if invite_hash:
        return f"+{invite_hash}"

    ref = ref.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    ref = ref.strip("/")
    if ref.startswith("@"):
        ref = ref[1:].strip()

    if not ref:
        raise ServiceAccountError(
            code="CHAT_LINK_REQUIRED",
            user_message="Укажите username, ссылку на чат/канал или числовой chat id.",
            http_status=400,
        )

    return ref

def map_telethon_exception_to_service_error(exc: Exception) -> ServiceAccountError:
    name = exc.__class__.__name__

    if name in {"UsernameInvalidError", "UsernameNotOccupiedError"}:
        return ServiceAccountError(
            code="PUBLIC_CHAT_NOT_FOUND",
            user_message="Чат или канал не найден. Проверьте username, ссылку или chat id.",
            http_status=404,
        )

    if name in {"InviteHashInvalidError", "InviteHashExpiredError"}:
        return ServiceAccountError(
            code="INVITE_LINK_INVALID_OR_EXPIRED",
            user_message="Ссылка-приглашение недействительна или уже истекла.",
            http_status=400,
        )

    if name in {"InviteRequestSentError"}:
        return ServiceAccountError(
            code="JOIN_REQUEST_SENT",
            user_message="Заявка на вступление отправлена. Читать этот чат можно будет только после одобрения.",
            http_status=403,
        )

    if name in {"ChannelPrivateError"}:
        return ServiceAccountError(
            code="CHAT_PRIVATE_OR_NO_ACCESS",
            user_message="Чат приватный или у служебного аккаунта нет доступа к нему.",
            http_status=403,
        )

    if name in {"ChatAdminRequiredError"}:
        return ServiceAccountError(
            code="CHAT_ADMIN_REQUIRED",
            user_message="Для чтения этого чата или канала у служебного аккаунта недостаточно прав.",
            http_status=403,
        )

    if name in {"ChannelsTooMuchError"}:
        return ServiceAccountError(
            code="SERVICE_ACCOUNT_CHANNEL_LIMIT",
            user_message="Служебный аккаунт достиг лимита по вступлению в каналы. Попробуйте позже.",
            http_status=503,
        )

    if name in {"UsersTooMuchError"}:
        return ServiceAccountError(
            code="CHAT_IS_FULL",
            user_message="В этот чат сейчас нельзя вступить: достигнут лимит участников или есть ограничение Telegram.",
            http_status=400,
        )

    if name in {"AuthKeyUnregisteredError", "SessionRevokedError"}:
        return ServiceAccountError(
            code="SERVICE_ACCOUNT_REAUTH_REQUIRED",
            user_message="Служебный аккаунт требует повторной авторизации.",
            http_status=503,
        )

    return ServiceAccountError(
        code="SERVICE_ACCOUNT_INTERNAL_ERROR",
        user_message="Не удалось обработать запрос через служебный аккаунт. Попробуйте чуть позже.",
        http_status=503,
    )

async def resolve_service_entity(client: TelegramClient, chat_ref: str):
    ref = normalize_public_chat_ref(chat_ref)
    invite_hash = extract_invite_hash(ref)

    # 1) invite-link
    if invite_hash:
        try:
            info = await client(CheckChatInviteRequest(invite_hash))
            chat = getattr(info, "chat", None)
            if chat is not None:
                return chat
        except Exception as e:
            raise map_telethon_exception_to_service_error(e) from e

        try:
            updates = await client(ImportChatInviteRequest(invite_hash))
            chats = getattr(updates, "chats", None) or []
            if chats:
                return chats[0]

            info2 = await client(CheckChatInviteRequest(invite_hash))
            chat2 = getattr(info2, "chat", None)
            if chat2 is not None:
                return chat2

            raise ServiceAccountError(
                code="INVITE_RESOLVE_FAILED",
                user_message="Не удалось открыть чат по ссылке-приглашению.",
                http_status=400,
            )
        except ServiceAccountError:
            raise
        except Exception as e:
            name = e.__class__.__name__
            if name == "UserAlreadyParticipantError":
                info3 = await client(CheckChatInviteRequest(invite_hash))
                chat3 = getattr(info3, "chat", None)
                if chat3 is not None:
                    return chat3

            raise map_telethon_exception_to_service_error(e) from e

    # 2) numeric chat id
    if ref.isdigit():
        target_id = int(ref)

        try:
            dialogs = await client.get_dialogs(limit=500)
            for d in dialogs:
                ent = d.entity
                if getattr(ent, "id", None) == target_id:
                    return ent
        except Exception:
            pass

        try:
            return await client.get_entity(target_id)
        except Exception as e:
            raise ServiceAccountError(
                code="NUMERIC_CHAT_ID_NOT_RESOLVED",
                user_message="Не удалось открыть чат по числовому id. Для неизвестного чата попробуйте username, публичную ссылку или invite-link.",
                http_status=400,
            ) from e

    # 3) username / публичная ссылка
    try:
        return await client.get_entity(ref)
    except Exception as e:
        raise map_telethon_exception_to_service_error(e) from e

async def ensure_join_and_access(client: TelegramClient, chat_ref: str, entity):
    ref = normalize_public_chat_ref(chat_ref)
    invite_hash = extract_invite_hash(ref)

    # invite already imported / can be imported again safely
    if invite_hash:
        try:
            updates = await client(ImportChatInviteRequest(invite_hash))
            chats = getattr(updates, "chats", None) or []
            if chats:
                return chats[0]
            return entity
        except Exception as e:
            name = e.__class__.__name__
            if name == "UserAlreadyParticipantError":
                return entity
            raise map_telethon_exception_to_service_error(e) from e

    # для channel/supergroup пробуем join, чтобы потом читать
    if isinstance(entity, Channel):
        try:
            await client(JoinChannelRequest(entity))
        except Exception as e:
            name = e.__class__.__name__
            if name == "UserAlreadyParticipantError":
                return entity
            raise map_telethon_exception_to_service_error(e) from e

        # после join стараемся получить свежую entity
        try:
            return await resolve_service_entity(client, ref)
        except Exception:
            return entity

    # обычный Chat / уже известный dialog
    return entity

async def read_messages_from_entity(client: TelegramClient, entity, days: int) -> list[dict]:
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
    return collected

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
    operation_kind: str = "analysis",
) -> AllocatedAccount:
    """
    operation_kind:
      - analysis
      - subscription

    analysis:      analysis -> shared -> subscriptions
    subscription:  subscriptions -> shared
    """
    now = utcnow()
    role_chain = get_usage_role_chain(operation_kind)

    for usage_role in role_chain:
        stmt = (
            select(ServiceTelegramAccount)
            .join(ServicePhoneNumber, ServicePhoneNumber.id == ServiceTelegramAccount.phone_number_id)
            .where(
                ServiceTelegramAccount.usage_role == usage_role,
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

        await log_event(
            db,
            service_account_id=None,
            event_type="allocator_candidates_checked",
            target_ref=target_ref,
            is_success=True,
            event_at=now,
            meta_json={
                "operation_kind": operation_kind,
                "usage_role": usage_role,
                "candidate_ids": [int(a.id) for a in candidates],
                "candidate_count": len(candidates),
            },
        )

        for account in candidates:
            await recount_request_counters(db, account)

            if not account_is_within_limits(account):
                await log_event(
                    db,
                    service_account_id=account.id,
                    event_type="account_skipped_limits",
                    target_ref=target_ref,
                    is_success=False,
                    error_code="ACCOUNT_OVER_LIMIT",
                    error_message="Аккаунт пропущен из-за лимитов нагрузки.",
                    event_at=now,
                    meta_json={
                        "operation_kind": operation_kind,
                        "usage_role": usage_role,
                        "requests_last_minute": account.requests_last_minute,
                        "requests_last_hour": account.requests_last_hour,
                        "requests_last_day": account.requests_last_day,
                        "limit_per_minute": SERVICE_ACCOUNT_LIMIT_PER_MINUTE,
                        "limit_per_hour": SERVICE_ACCOUNT_LIMIT_PER_HOUR,
                        "limit_per_day": SERVICE_ACCOUNT_LIMIT_PER_DAY,
                    },
                )
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
                meta_json={
                    "operation_kind": operation_kind,
                    "usage_role": usage_role,
                },
            )

            busy_log = await log_event(
                db,
                service_account_id=account.id,
                event_type="account_busy_window",
                target_ref=target_ref,
                started_at=now,
                is_success=None,
                meta_json={
                    "operation_kind": operation_kind,
                    "usage_role": usage_role,
                },
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
        meta_json={
            "operation_kind": operation_kind,
            "role_chain": role_chain,
        },
    )
    await db.commit()

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

async def touch_active_service_session(
    db: AsyncSession,
    service_account_id: int,
) -> None:
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
    session_row = result.scalar_one_or_none()

    if session_row is not None:
        session_row.last_used_at = utcnow()
        session_row.updated_at = utcnow()
        await db.flush()

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
        raise errors.AuthKeyUnregisteredError(request=None)

    normalized_ref = normalize_public_chat_ref(chat_ref)

    try:
        entity = await resolve_service_entity(client, normalized_ref)

        # новая логика: пробуем вступить / подписаться, если это нужно для чтения
        entity = await ensure_join_and_access(client, normalized_ref, entity)

        messages = await read_messages_from_entity(client, entity, days)
        return entity, messages

    except ServiceAccountError:
        raise
    except Exception as e:
        raise map_telethon_exception_to_service_error(e) from e

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

    completion = await openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    return completion.choices[0].message.content.strip()


async def validate_service_subscription_target(
    db: AsyncSession,
    *,
    chat_link: str,
) -> dict:
    """
    Лёгкая проверка чата для service-subscription.
    Ничего не анализирует через LLM, только:
    - нормализует ссылку
    - через service account проверяет доступ
    - возвращает метаданные чата
    """
    normalized_ref = normalize_public_chat_ref(chat_link)
    allocated = await allocate_service_account(
        db,
        target_ref=normalized_ref,
        operation_kind="subscription",
    )
    account = allocated.account

    # фиксируем reserve сразу
    await db.commit()

    try:
        client = await get_service_tg_client(db, service_account_id=account.id)
        entity = await resolve_service_entity(client, normalized_ref)
        entity = await ensure_join_and_access(client, normalized_ref, entity)

        now = utcnow()
        account.last_used_at = now
        account.last_error = None
        account.last_error_at = None
        account.consecutive_fail_count = 0
        account.updated_at = now

        await touch_active_service_session(db, account.id)

        await log_event(
            db,
            service_account_id=account.id,
            event_type="operation_finished",
            target_ref=normalized_ref,
            is_success=True,
            event_at=now,
        )

        await release_service_account(
            db,
            account=account,
            busy_log_id=allocated.busy_log_id,
            success=True,
        )
        await db.commit()

        chat_title = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or "Без названия"
        )

        return {
            "status": "ok",
            "source_mode": "service",
            "chat_ref_normalized": normalized_ref,
            "chat_id": getattr(entity, "id", None),
            "chat_username": getattr(entity, "username", None),
            "chat_name": chat_title,
        }

    except ServiceAccountError as e:
        await safe_rollback(db)

        account.last_error = e.code
        account.last_error_at = utcnow()
        account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

        await release_service_account(
            db,
            account=account,
            busy_log_id=allocated.busy_log_id,
            success=False,
            error_code=e.code,
            error_message=e.user_message,
        )

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
        await safe_rollback(db)

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

        await release_service_account(
            db,
            account=account,
            busy_log_id=allocated.busy_log_id,
            success=False,
            error_code="FLOOD_WAIT",
            error_message=f"Flood wait {int(e.seconds)} sec",
        )

        await log_event(
            db,
            service_account_id=account.id,
            event_type="telegram_error",
            target_ref=normalized_ref,
            is_success=False,
            error_code="FLOOD_WAIT",
            error_message=f"Flood wait {int(e.seconds)} sec",
            event_at=utcnow(),
        )

        await db.commit()

        raise ServiceAccountError(
            code="FLOOD_WAIT",
            user_message="Служебный аккаунт временно ограничен Telegram. Попробуйте позже.",
            http_status=503,
        )

    except Exception as e:
        await safe_rollback(db)

        account.last_error = "SERVICE_ACCOUNT_INTERNAL_ERROR"
        account.last_error_at = utcnow()
        account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

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

        raise ServiceAccountError(
            code="SERVICE_ACCOUNT_INTERNAL_ERROR",
            user_message="Не удалось проверить чат через служебный аккаунт.",
            http_status=503,
        )


async def fetch_service_chat_messages_for_subscription(
    db: AsyncSession,
    *,
    chat_link: str,
    since_dt: datetime,
    min_id: int | None = None,
    limit: int = 1000,
) -> tuple[object, list[dict]]:
    """
    Service-mode аналог fetch_chat_messages_for_subscription(...).
    Возвращает entity + сообщения в формате:
    {
      message_id,
      message_ts,
      author_id,
      author_display,
      text,
      reply_to
    }
    """
    normalized_ref = normalize_public_chat_ref(chat_link)
    allocated = await allocate_service_account(
        db,
        target_ref=normalized_ref,
        operation_kind="subscription",
    )
    account = allocated.account

    # фиксируем reserve сразу
    await db.commit()

    try:
        client = await get_service_tg_client(db, service_account_id=account.id)
        entity = await resolve_service_entity(client, normalized_ref)
        entity = await ensure_join_and_access(client, normalized_ref, entity)

        rows: list[dict] = []
        async for msg in client.iter_messages(entity, limit=limit):
            if not isinstance(msg, Message):
                continue

            if msg.id is None:
                continue

            if min_id is not None and int(msg.id) <= int(min_id):
                continue

            msg_dt = msg.date
            if msg_dt is None:
                continue
            if msg_dt.tzinfo is None:
                msg_dt = msg_dt.replace(tzinfo=timezone.utc)

            if msg_dt < since_dt:
                break

            text = (msg.message or "").strip()
            if not text:
                continue

            author_id = None
            author_display = "Unknown"
            try:
                sender = await msg.get_sender()
                if sender is not None:
                    author_id = getattr(sender, "id", None)
                    if getattr(sender, "username", None):
                        author_display = "@" + sender.username
                    else:
                        first = (getattr(sender, "first_name", "") or "").strip()
                        last = (getattr(sender, "last_name", "") or "").strip()
                        author_display = (first + " " + last).strip() or "Unknown"
            except Exception:
                pass

            reply_to = getattr(msg, "reply_to_msg_id", None)
            if reply_to is None:
                reply_obj = getattr(msg, "reply_to", None)
                reply_to = getattr(reply_obj, "reply_to_msg_id", None) if reply_obj else None

            rows.append(
                {
                    "message_id": int(msg.id),
                    "message_ts": msg_dt.isoformat(),
                    "author_id": author_id,
                    "author_display": author_display,
                    "text": text,
                    "reply_to": int(reply_to) if reply_to is not None else None,
                }
            )

        rows.reverse()

        now = utcnow()
        account.last_used_at = now
        account.last_error = None
        account.last_error_at = None
        account.consecutive_fail_count = 0
        account.updated_at = now

        await touch_active_service_session(db, account.id)

        await log_event(
            db,
            service_account_id=account.id,
            event_type="operation_finished",
            target_ref=normalized_ref,
            is_success=True,
            event_at=now,
        )

        await release_service_account(
            db,
            account=account,
            busy_log_id=allocated.busy_log_id,
            success=True,
        )
        await db.commit()

        return entity, rows

    except ServiceAccountError as e:
        await safe_rollback(db)

        account.last_error = e.code
        account.last_error_at = utcnow()
        account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

        await release_service_account(
            db,
            account=account,
            busy_log_id=allocated.busy_log_id,
            success=False,
            error_code=e.code,
            error_message=e.user_message,
        )

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
        await safe_rollback(db)

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

        await release_service_account(
            db,
            account=account,
            busy_log_id=allocated.busy_log_id,
            success=False,
            error_code="FLOOD_WAIT",
            error_message=f"Flood wait {int(e.seconds)} sec",
        )

        await log_event(
            db,
            service_account_id=account.id,
            event_type="telegram_error",
            target_ref=normalized_ref,
            is_success=False,
            error_code="FLOOD_WAIT",
            error_message=f"Flood wait {int(e.seconds)} sec",
            event_at=utcnow(),
        )

        await db.commit()

        raise ServiceAccountError(
            code="FLOOD_WAIT",
            user_message="Служебный аккаунт временно ограничен Telegram. Попробуйте позже.",
            http_status=503,
        )

    except Exception as e:
        await safe_rollback(db)

        account.last_error = "SERVICE_ACCOUNT_INTERNAL_ERROR"
        account.last_error_at = utcnow()
        account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

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

        raise ServiceAccountError(
            code="SERVICE_ACCOUNT_INTERNAL_ERROR",
            user_message="Не удалось прочитать сообщения через служебный аккаунт.",
            http_status=503,
        )

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
        allocated = await allocate_service_account(
            db,
            target_ref=normalized_ref,
            operation_kind="analysis",
        )
        account = allocated.account

        # ВАЖНО: сразу фиксируем reserve, чтобы не держать длинную транзакцию
        await db.commit()

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

            now = utcnow()

            account.last_used_at = now
            account.last_error = None
            account.last_error_at = None
            account.consecutive_fail_count = 0
            account.updated_at = now

            await touch_active_service_session(db, account.id)

            await log_event(
                db,
                service_account_id=account.id,
                event_type="operation_finished",
                target_ref=normalized_ref,
                is_success=True,
                event_at=now,
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
                "chat_ref_normalized": normalized_ref,
                "chat_id": getattr(entity, "id", None),
                "chat_username": getattr(entity, "username", None),
            }

        except ServiceAccountError as e:
            await safe_rollback(db)

            account.last_error = e.code
            account.last_error_at = utcnow()
            account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1

            await release_service_account(
                db,
                account=account,
                busy_log_id=allocated.busy_log_id,
                success=False,
                error_code=e.code,
                error_message=e.user_message,
            )

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
            await safe_rollback(db)

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
            await safe_rollback(db)

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
            await safe_rollback(db)

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
            await safe_rollback(db)

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
            await safe_rollback(db)

            account.last_error = type(e).__name__
            account.last_error_at = utcnow()
            account.consecutive_fail_count = (account.consecutive_fail_count or 0) + 1
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