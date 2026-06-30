# telegram_service.py
import asyncio
import logging
import os
import base64
import secrets
from dataclasses import dataclass

from typing import Optional, Tuple, List, Dict
from datetime import datetime, timedelta, timezone
from telethon.tl.types import Message
from telethon import TelegramClient
from telethon.errors import (
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    FloodWaitError,
)
from telethon.errors import InviteHashInvalidError, InviteHashExpiredError, UserAlreadyParticipantError

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from telethon.sessions import StringSession

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func
from db.models import TelegramSession


_qr_login = None  # telethon.tl.custom.qrlogin.QRLogin | None
_qr_wait_task = None  # asyncio.Task | None

# Получаем ключи
api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]

# Создаём клиент сессии
#tg_client = TelegramClient("session_cotel", api_id, api_hash) - старый код
tg_clients: dict[int, TelegramClient] = {}
TG_PASSWORD_CONTEXT_TTL_SEC = 300  # 5 минут

@dataclass
class PasswordEncryptionContext:
    owner_user_id: int
    private_key_pem: str
    expires_at: datetime

_password_encryption_contexts: dict[str, PasswordEncryptionContext] = {}

async def get_tg_client(db: AsyncSession, owner_user_id: int) -> TelegramClient:
    """
    Возвращает подключенный TelegramClient.
    Если в БД есть сессия — поднимаем из неё.
    Если нет — создаём пустую сессию (для шага send_code / login).
    """
    # Проверяем, есть ли уже клиент для этого пользователя
    client = tg_clients.get(owner_user_id)

    if client is not None:
        if not client.is_connected():
            await client.connect()
        return client

    ss = await load_user_telegram_session(db, owner_user_id)

    if ss:
        client = TelegramClient(StringSession(ss), api_id, api_hash)
    else:
        client = TelegramClient(StringSession(), api_id, api_hash)

    await client.connect()

    # сохраняем клиент для конкретного пользователя
    tg_clients[owner_user_id] = client

    return client


# ---- ВСПОМОГАТЕЛЬНЫЕ  ФУНКЦИИ ---------------------------------------

import re
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest

INVITE_RE = re.compile(r"(?:t\.me\/\+|t\.me\/joinchat\/)([A-Za-z0-9_-]+)")


def build_message_permalink(entity, message_id: int) -> Optional[str]:
    """
    Построить публичную ссылку на конкретное сообщение в Telegram
    по entity (объект из Telethon) и message_id.

    Правила:
      - публичный username (канал/супергруппа/чат с username) →
        https://t.me/<username>/<message_id>
      - приватный канал/супергруппа (Telethon Channel без username) →
        https://t.me/c/<channel_id>/<message_id>
        (id у Telethon-Channel положительный — это уже "internal" id
         без префикса -100, который ожидает t.me/c/...)
      - basic Chat (классическая малая группа) и User (личка) →
        permalink не существует → None
    """
    if entity is None or message_id is None:
        return None

    # 1) Публичный — есть username
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{int(message_id)}"

    # 2) Приватный канал / супергруппа — у Telethon это Channel с broadcast/megagroup
    # У него есть .id (положительный) и нет .username.
    # Импортируем локально, чтобы не загромождать верх модуля.
    try:
        from telethon.tl.types import Channel as _Channel
        if isinstance(entity, _Channel):
            channel_id = getattr(entity, "id", None)
            if channel_id is not None:
                return f"https://t.me/c/{int(channel_id)}/{int(message_id)}"
    except Exception:
        pass

    # 3) Всё остальное (basic Chat, User) — permalink невозможен
    return None

def encrypt_session(plain: str) -> str:
    key = os.getenv("TELEGRAM_SESSION_ENC_KEY")
    if not key:
        raise RuntimeError("TELEGRAM_SESSION_ENC_KEY is not set")
    f = Fernet(key.encode())
    return f.encrypt(plain.encode("utf-8")).decode("utf-8")

def decrypt_session(ciphertext: str) -> str:
    key = os.getenv("TELEGRAM_SESSION_ENC_KEY")
    if not key:
        raise RuntimeError("TELEGRAM_SESSION_ENC_KEY is not set")
    f = Fernet(key.encode())
    try:
        return f.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
    except InvalidToken as e:
        raise RuntimeError("Invalid TELEGRAM session ciphertext or key") from e

def _cleanup_expired_password_contexts() -> None:
    now = datetime.now(timezone.utc)
    expired_ids = [
        ctx_id
        for ctx_id, ctx in _password_encryption_contexts.items()
        if ctx.expires_at <= now
    ]
    for ctx_id in expired_ids:
        _password_encryption_contexts.pop(ctx_id, None)


def create_password_encryption_context(owner_user_id: int) -> dict:
    _cleanup_expired_password_contexts()

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    public_key = private_key.public_key()

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")

    context_id = secrets.token_urlsafe(24)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=TG_PASSWORD_CONTEXT_TTL_SEC)

    _password_encryption_contexts[context_id] = PasswordEncryptionContext(
        owner_user_id=owner_user_id,
        private_key_pem=private_key_pem,
        expires_at=expires_at,
    )

    return {
        "context_id": context_id,
        "public_key_pem": public_key_pem,
        "expires_at": expires_at.isoformat(),
    }

def decrypt_password_ciphertext(
    *,
    owner_user_id: int,
    context_id: str,
    ciphertext_b64: str,
) -> str:
    _cleanup_expired_password_contexts()

    ctx = _password_encryption_contexts.pop(context_id, None)
    if not ctx:
        raise ValueError("PASSWORD_ENCRYPTION_CONTEXT_INVALID")

    if ctx.owner_user_id != owner_user_id:
        raise ValueError("PASSWORD_ENCRYPTION_CONTEXT_INVALID")

    if ctx.expires_at <= datetime.now(timezone.utc):
        raise ValueError("PASSWORD_ENCRYPTION_CONTEXT_EXPIRED")

    try:
        private_key = serialization.load_pem_private_key(
            ctx.private_key_pem.encode("utf-8"),
            password=None,
        )
        plaintext = private_key.decrypt(
            base64.b64decode(ciphertext_b64),
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
        password = plaintext.decode("utf-8").strip()
    except Exception as e:
        raise ValueError("PASSWORD_DECRYPT_FAILED") from e

    if not password:
        raise ValueError("PASSWORD_REQUIRED")

    return password


async def save_user_telegram_session(db: AsyncSession, owner_user_id: int, plain_string_session: str) -> None:
    cipher = encrypt_session(plain_string_session)

    # На MVP делаем: "одна активная сессия на user" → перезаписываем.
    res = await db.execute(
        select(TelegramSession).where(
            TelegramSession.owner_user_id == owner_user_id,
            TelegramSession.is_active == True,  # noqa: E712
        )
    )
    row = res.scalar_one_or_none()

    if row:
        row.session_ciphertext = cipher
        row.revoked_at = None
        row.last_used_at = func.now()
    else:
        db.add(
            TelegramSession(
                owner_user_id=owner_user_id,
                session_ciphertext=cipher,
                is_active=True,
            )
        )

    await db.commit()

async def load_user_telegram_session(db: AsyncSession, owner_user_id: int) -> str | None:
    res = await db.execute(
        select(TelegramSession.session_ciphertext).where(
            TelegramSession.owner_user_id == owner_user_id,
            TelegramSession.is_active == True,  # noqa: E712
        )
    )
    cipher = res.scalar_one_or_none()
    if not cipher:
        return None
    return decrypt_session(cipher)

async def resolve_entity_with_invite(client, chat_link: str):
    link = (chat_link or "").strip()

    # 1) invite-ссылка
    m = INVITE_RE.search(link)
    if m:
        invite_hash = m.group(1)

        info = await client(CheckChatInviteRequest(invite_hash))
        # если уже участник — info.chat даст entity
        chat = getattr(info, "chat", None)
        if chat:
            return chat

        # если не участник — пробуем join
        await client(ImportChatInviteRequest(invite_hash))

        # после join повторяем check, чтобы получить chat entity
        info2 = await client(CheckChatInviteRequest(invite_hash))
        chat2 = getattr(info2, "chat", None)
        if chat2:
            return chat2

        raise RuntimeError("INVITE_RESOLVE_FAILED")

    # 2) обычная ссылка / username
    # примеры: https://t.me/xxx, @xxx, xxx
    link = link.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    if link.startswith("@"):
        link = link[1:]

    return await client.get_entity(link)


async def probe_chat_density(
    db: AsyncSession,
    owner_user_id: int,
    chat_link: str,
    *,
    window_hours: int = 2,
    max_sample: int = 400,
) -> Optional[tuple[float, float, int]]:
    """
    Дешёвая «разведка» плотности чата ДО полной выгрузки (B2, red-зона).

    Тянем только последние сообщения за окно `window_hours` (не больше
    `max_sample` штук — потолок на случай чата-водопада) и по ним считаем
    приблизительную плотность. Возвращает кортеж:
        (сообщений_в_день, средняя_длина_текста, размер_выборки)
    либо None, если разведка не удалась (нерезолвимый чат и т.п.) — в этом
    случае вызывающий просто НЕ блокирует запрос.

    iter_messages идёт от новых к старым; останавливаемся, как только сообщение
    старше границы окна. Плотность экстраполируется: count / window_hours * 24.
    """
    client = await get_tg_client(db, owner_user_id)
    entity = await resolve_entity_with_invite(client, chat_link)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    count = 0
    chars = 0
    async for msg in client.iter_messages(entity, limit=max_sample):
        msg_date = getattr(msg, "date", None)
        if msg_date is not None and msg_date < cutoff:
            break
        text = getattr(msg, "message", None) or getattr(msg, "text", None) or ""
        count += 1
        chars += len(text)
    if count == 0:
        return (0.0, 0.0, 0)
    msgs_per_day = count / float(window_hours) * 24.0
    avg_chars = chars / float(count)
    log.warning(
        "QA_DIAG probe chat=%s window_h=%d sample=%d msgs_per_day=%.0f avg_chars=%.0f",
        chat_link, window_hours, count, msgs_per_day, avg_chars,
    )
    return (msgs_per_day, avg_chars, count)


async def ensure_connected(db: AsyncSession, owner_user_id: int):
    client = await get_tg_client(db, owner_user_id)
    if not client.is_connected():
        await client.connect()
    return client

async def export_string_session(db: AsyncSession, owner_user_id: int) -> str:
    client = await ensure_connected(db, owner_user_id)
    return client.session.save()

async def send_login_code(db: AsyncSession, owner_user_id: int, phone: str):
    """
    Отправить код на телефон.
    """
    client = await ensure_connected(db, owner_user_id)
    try:
        return await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        raise ValueError("PHONE_NUMBER_INVALID")

async def confirm_login(db: AsyncSession, owner_user_id: int, phone: str, code: str):
    """
    Подтвердить код и завершить авторизацию.
    """
    client = await ensure_connected(db, owner_user_id)
    try:
        me = await client.sign_in(phone=phone, code=code)
        return me

    except PhoneCodeInvalidError:
        raise ValueError("PHONE_CODE_INVALID")
    except SessionPasswordNeededError:
        raise ValueError("PASSWORD_NEEDED")   # 2FA включена

async def confirm_password(db: AsyncSession, owner_user_id: int, password: str):
    client = await ensure_connected(db, owner_user_id)
    # завершает вход при включённой 2FA
    await client.sign_in(password=password)

async def get_current_user(db: AsyncSession, owner_user_id: int):
    """
    Проверить, авторизованы ли мы в Telegram.
    """
    client = await ensure_connected(db, owner_user_id)
    if not await client.is_user_authorized():
        return None

    return await client.get_me()


log = logging.getLogger(__name__)


async def _resolve_sender_cached(
    msg,
    cache: Dict[int, str],
    stats: dict,
) -> Tuple[Optional[int], str]:
    """
    Имя автора сообщения с кэшем по sender_id.

    Раньше get_sender() вызывался на КАЖДОЕ сообщение — в активных чатах это
    тысячи лишних обращений к Telegram и главный источник FloodWait/долгой
    выгрузки. Уникальных авторов в чате на порядки меньше, поэтому имя по id
    автора узнаём один раз и переиспользуем.

    `stats` — мутабельный словарь со счётчиками для диагностики выгрузки.

    Порядок: (1) кэш по sender_id, (2) БЕСПЛАТНОЕ имя из кэша Telethon
    (`msg.sender`, min-user из той же пачки — без сети), (3) только если пусто —
    сетевой get_sender(). Шаг (2) убирает тысячи лишних сетевых запросов на
    активных чатах (раньше это был главный источник FloodWait и зависания) —
    одинаково для группового запроса, подписок и служебных аккаунтов.
    """
    sid = getattr(msg, "sender_id", None)
    if sid is not None and sid in cache:
        return sid, cache[sid]

    # (2) Бесплатно из кэша Telethon, без сетевого запроса.
    cached_sender = getattr(msg, "sender", None)
    if cached_sender is not None:
        nm = _name_from_entity(cached_sender)
        if nm and nm != "Unknown":
            if sid is not None:
                cache[sid] = nm
            stats["named_from_cache"] = stats.get("named_from_cache", 0) + 1
            return sid, nm

    # (3) Fallback: сетевой запрос (медленно — считаем как lookup).
    stats["sender_lookups"] = stats.get("sender_lookups", 0) + 1
    display = "Unknown"
    try:
        sender = await msg.get_sender()
        if sender is not None:
            display = _name_from_entity(sender)
    except FloodWaitError as e:
        wait_sec = int(getattr(e, "seconds", 0) or 0)
        stats["flood_waits"] = stats.get("flood_waits", 0) + 1
        stats["flood_seconds"] = stats.get("flood_seconds", 0) + wait_sec
        log.warning("QA_DIAG flood_wait stage=get_sender seconds=%d", wait_sec)
        return sid, display  # Unknown не кэшируем — позволяем повторить позже
    except Exception:
        pass

    if sid is not None:
        cache[sid] = display
    return sid, display


def _name_from_entity(ent) -> str:
    """Отображаемое имя автора из Telethon-entity: @логин, иначе Имя Фамилия."""
    if ent is None:
        return "Unknown"
    if getattr(ent, "username", None):
        return "@" + ent.username
    first = (getattr(ent, "first_name", "") or "").strip()
    last = (getattr(ent, "last_name", "") or "").strip()
    name = (first + " " + last).strip()
    if name:
        return name
    return getattr(ent, "title", None) or "Unknown"


async def resolve_sender_logins(
    db: AsyncSession,
    owner_user_id: int,
    sender_ids,
) -> Dict[int, str]:
    """
    Разрешить отображаемые имена для НЕБОЛЬШОГО набора sender_id — тех авторов,
    что LLM процитировала в ответе (Вариант А).

    Резолвим через клиент пользователя; access_hash авторов уже в кэше сессии
    после выгрузки. Неразрешимые id (удалённый аккаунт и т.п.) тихо пропускаем.
    Никогда не бросает исключения наружу.
    """
    result: Dict[int, str] = {}
    ids = [int(s) for s in dict.fromkeys(sender_ids) if s is not None]
    if not ids:
        return result
    try:
        client = await ensure_connected(db, owner_user_id)
    except Exception:
        return result
    for sid in ids:
        try:
            ent = await client.get_entity(sid)
            result[sid] = _name_from_entity(ent)
        except FloodWaitError as e:
            log.warning(
                "QA_DIAG flood_wait stage=resolve_logins seconds=%d",
                int(getattr(e, "seconds", 0) or 0),
            )
        except Exception:
            pass
    return result


# D1: управление FloodWait в обычной/групповой выгрузке.
_FETCH_FLOOD_MAX_SLEEP_SEC = 60   # дольше не ждём — отдаём понятную ошибку
_FETCH_FLOOD_MAX_RETRIES = 5      # защита от бесконечного флуда


async def _iter_with_flood_resume(client, entity, base_kwargs, fetch_stats, stage):
    """
    Обёртка над client.iter_messages с обработкой FloodWait и продолжением
    с места обрыва (resume по offset_id). По умолчанию Telethon при длинном
    FloodWait (> flood_sleep_threshold) бросает FloodWaitError, и выгрузка
    падала. Здесь мы ловим её, логируем (видно в Render + считаем в
    fetch_stats), и при разумной задержке досыпаем и продолжаем со следующего
    (более старого) сообщения, а не с начала. Тело цикла-вызывающего не
    меняется — просто итерируем через эту обёртку.
    """
    last_id = base_kwargs.get("offset_id")
    attempts = 0
    while True:
        kwargs = dict(base_kwargs)
        if last_id is not None:
            kwargs.pop("offset_date", None)  # после первого батча идём по id
            kwargs["offset_id"] = last_id
        try:
            async for msg in client.iter_messages(entity, **kwargs):
                mid = getattr(msg, "id", None)
                if mid is not None:
                    last_id = mid
                yield msg
            return  # генератор исчерпан штатно
        except FloodWaitError as e:
            wait = int(getattr(e, "seconds", 0) or 0)
            fetch_stats["flood_waits"] = fetch_stats.get("flood_waits", 0) + 1
            fetch_stats["flood_seconds"] = fetch_stats.get("flood_seconds", 0) + wait
            attempts += 1
            log.warning(
                "QA_DIAG flood_wait stage=%s seconds=%d resume_from=%s attempt=%d",
                stage, wait, last_id, attempts,
            )
            if wait > _FETCH_FLOOD_MAX_SLEEP_SEC or attempts > _FETCH_FLOOD_MAX_RETRIES:
                raise ValueError(f"FLOOD_WAIT:{wait}")
            await asyncio.sleep(wait + 1)


async def fetch_chat_messages(
    db: AsyncSession,
    owner_user_id: int,
    chat_link: str,
    days: int = 7,
    *,
    period_seconds: Optional[int] = None,
    since_dt: Optional[datetime] = None,
    until_dt: Optional[datetime] = None,
    fetch_stats: Optional[dict] = None,
    resolve_authors: bool = True,
):
    """
    Возвращает:
      entity: объект чата/канала (Telethon entity)
      messages: список в формате [{date, from, text}, ...] для LLM

    Окно анализа задаётся одним из трёх способов (по приоритету):
      - абсолютный диапазон since_dt..until_dt — если задан since_dt
        (until_dt опционален: верхняя граница, передаётся в Telethon как
        offset_date — выбираются сообщения старше until_dt). Нижняя граница
        since_dt останавливает итерацию.
      - period_seconds (в секундах) — относительный период «за последние…»,
        поддерживает минуты/часы/дни.
      - days (legacy) — fallback для совместимости.
    """
    client = await ensure_connected(db, owner_user_id)

    if not await client.is_user_authorized():
        raise ValueError("TELEGRAM_NOT_AUTHORIZED")

    if not chat_link:
        raise ValueError("CHAT_LINK_REQUIRED")

    # Нормализуем ввод: https://t.me/xxx -> xxx, @xxx -> xxx
    link = chat_link.strip()
    if "t.me/" in link:
        link = link.split("t.me/")[-1].split("?")[0].strip("/")
    if link.startswith("@"):
        link = link[1:].strip()

    # Нижняя граница (since_dt). Приоритет: явный абсолютный диапазон →
    # period_seconds → days. until_dt (верхняя граница) используется ниже как
    # offset_date в iter_messages.
    if since_dt is not None:
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
    elif period_seconds is not None and int(period_seconds) > 0:
        since_dt = datetime.now(timezone.utc) - timedelta(seconds=int(period_seconds))
    else:
        since_dt = datetime.now(timezone.utc) - timedelta(days=int(days))

    if until_dt is not None and until_dt.tzinfo is None:
        until_dt = until_dt.replace(tzinfo=timezone.utc)

    # --- INVITE LINKS: t.me/+HASH or t.me/joinchat/HASH ---
    invite_hash = None

    # link после твоей нормализации может быть "+HASH"
    if link.startswith("+"):
        invite_hash = link[1:]

    # или "joinchat/HASH"
    if link.startswith("joinchat/"):
        invite_hash = link.split("joinchat/")[-1].strip("/")

    if invite_hash:
        try:
            invite = await client(CheckChatInviteRequest(invite_hash))

            # Если уже участник — Telethon вернёт объект с чатом
            if hasattr(invite, "chat") and invite.chat:
                entity = invite.chat
            else:
                # Иначе нужно вступить (для чтения приватной истории иначе доступа не будет)
                try:
                    upd = await client(ImportChatInviteRequest(invite_hash))
                    # upd может содержать chats/users; удобнее просто резолвить снова по hash через get_entity не надо
                    # Берём первый чат из upd.chats, если есть
                    if getattr(upd, "chats", None):
                        entity = upd.chats[0]
                    else:
                        # fallback: если чатов нет — попробуем получить через invite.chat (на некоторых типах)
                        entity = getattr(invite, "chat", None)
                except UserAlreadyParticipantError:
                    # если уже участник, но import вернул это — попробуем взять chat из invite
                    entity = getattr(invite, "chat", None)

            if not entity:
                raise ValueError("INVITE_JOIN_FAILED")

        except (InviteHashInvalidError, InviteHashExpiredError):
            raise ValueError("INVITE_LINK_INVALID_OR_EXPIRED")

        except Exception as e:
            raise ValueError(f"INVITE_HANDLE_FAILED: {str(e)}")


    else:

        # --- обычный публичный username / @username / numeric chat_id ---

        entity = None

        # 1) Если link — число, это chat_id из dialogs (частый случай)

        if link.isdigit():

            target_id = int(link)

            try:

                dialogs = await client.get_dialogs(limit=500)

                for d in dialogs:

                    ent = d.entity

                    if getattr(ent, "id", None) == target_id:
                        entity = ent

                        break

            except Exception:

                pass

            # fallback: иногда срабатывает напрямую

            if entity is None:

                try:

                    entity = await client.get_entity(target_id)

                except Exception as e:

                    raise ValueError(f"CHAT_RESOLVE_FAILED: {str(e)}")


        # 2) Иначе — username / @username

        else:

            try:

                entity = await client.get_entity(link)

            except Exception as e:

                raise ValueError(f"CHAT_RESOLVE_FAILED: {str(e)}")

    collected = []
    sender_cache: Dict[int, str] = {}
    if fetch_stats is None:
        fetch_stats = {}
    try:
        # Telethon iter_messages возвращает от новых к старым.
        # Масштабируемый limit: в активных чатах (например, «квартиры в
        # тбилиси», ~750-1000 сообщений в день вместе с медиа/реакциями)
        # фиксированный limit=5000 упирается в потолок раньше, чем мы
        # доходим до since_dt. Симптом: фронт запрашивает 20 дней, а
        # фактическое окно данных получается 7 (модель честно репортит
        # «c 22 по 29 мая» вместо «за последний месяц»). Подняли потолок
        # пропорционально периоду с разумным cap'ом сверху, чтобы случайный
        # `days=365` не уронил систему.
        #
        # Эвристика: 1500 сообщений в день — это потолок плотности даже
        # для самых активных русскоязычных чатов. Минимум 5000 для коротких
        # периодов, максимум 80 000 для очень длинных. Telethon выгружает
        # батчами по 100 — для 50K это ~500 round-trip к Telegram, ~30-60с.
        if until_dt is not None:
            # Абсолютный диапазон: «вес» окна — его длина в днях.
            requested_days = max(int((until_dt - since_dt).total_seconds()) // 86400, 1)
        elif period_seconds is not None and int(period_seconds) > 0:
            requested_days = max(int(period_seconds) // 86400, 1)
        else:
            requested_days = max(int(days), 1)
        dynamic_limit = min(80_000, max(5_000, requested_days * 1_500))
        # offset_date — верхняя граница диапазона: Telethon отдаёт сообщения
        # старше этой даты (двигаемся к since_dt и останавливаемся на нём ниже).
        iter_kwargs = {"limit": dynamic_limit}
        if until_dt is not None:
            iter_kwargs["offset_date"] = until_dt
        # D1: итерируем через обёртку с FloodWait-resume (тело цикла без изменений).
        async for msg in _iter_with_flood_resume(
            client, entity, iter_kwargs, fetch_stats, "fetch_personal"
        ):
            if not isinstance(msg, Message):
                continue

            # Иногда date может быть naive — приводим к UTC
            msg_dt = msg.date
            if msg_dt is None:
                continue
            if msg_dt.tzinfo is None:
                msg_dt = msg_dt.replace(tzinfo=timezone.utc)

            # Как только дошли до сообщений старше периода — выходим
            if msg_dt < since_dt:
                break

            text = (msg.message or "").strip()
            if not text:
                continue

            sender_id = getattr(msg, "sender_id", None)
            if resolve_authors:
                _, sender_name = await _resolve_sender_cached(msg, sender_cache, fetch_stats)
            else:
                # Вариант А: НЕ делаем сетевых запросов за автором на выгрузке
                # (это и есть источник долгого зависания на активных чатах).
                # Но имя автора часто уже лежит в кэше Telethon (min-user из той
                # же пачки сообщений) — читаем его бесплатно через msg.sender.
                # Если там пусто — оставляем None, и @логин подставится после
                # ответа LLM по токену [author:id] (см. resolve_sender_logins).
                sender_name = None
                cached_sender = getattr(msg, "sender", None)
                if cached_sender is not None:
                    nm = _name_from_entity(cached_sender)
                    if nm and nm != "Unknown":
                        sender_name = nm
                        fetch_stats["named_from_cache"] = (
                            fetch_stats.get("named_from_cache", 0) + 1
                        )
                if sender_name is None:
                    fetch_stats["author_fallbacks"] = (
                        fetch_stats.get("author_fallbacks", 0) + 1
                    )

            collected.append({
                "message_id": getattr(msg, "id", None),
                "date": msg_dt.isoformat(),
                "from": sender_name,
                "sender_id": sender_id,
                "text": text,
            })

    except ValueError:
        # Наши осмысленные ошибки (FLOOD_WAIT:… из обёртки, CHAT_RESOLVE_FAILED)
        # пробрасываем как есть — не маскируем под CHAT_FETCH_FAILED.
        raise
    except Exception as e:
        raise ValueError(f"CHAT_FETCH_FAILED: {str(e)}")

    # collected сейчас от новых к старым — разворачиваем, чтобы было "старые -> новые"
    collected.reverse()

    fetch_stats["unique_senders"] = len(sender_cache)
    fetch_stats["kept"] = len(collected)
    log.warning(
        "QA_DIAG fetch path=personal kept=%d sender_lookups=%d unique_senders=%d "
        "named_from_cache=%d author_fallbacks=%d floods=%d flood_sec=%d",
        len(collected), fetch_stats.get("sender_lookups", 0), len(sender_cache),
        fetch_stats.get("named_from_cache", 0), fetch_stats.get("author_fallbacks", 0),
        fetch_stats.get("flood_waits", 0), fetch_stats.get("flood_seconds", 0),
    )

    return entity, collected

from telethon.tl.types import User, Chat, Channel


def _dialog_entity_to_chat_dict(ent) -> Optional[Dict]:
    """
    Преобразовать Telethon-entity (User/Chat/Channel) в обычный dict
    для отдачи на фронт. Возвращает None для неподдерживаемых типов.
    Используется и при выдаче плоского списка чатов, и при сборке
    структуры с папками.
    """
    if isinstance(ent, User):
        chat_type = "user"
        title = " ".join(
            filter(None, [getattr(ent, "first_name", None), getattr(ent, "last_name", None)])
        ).strip()
        if not title:
            title = getattr(ent, "username", None) or f"User {ent.id}"
    elif isinstance(ent, Chat):
        chat_type = "group"
        title = getattr(ent, "title", None) or f"Group {ent.id}"
    elif isinstance(ent, Channel):
        chat_type = "channel" if getattr(ent, "broadcast", False) else "supergroup"
        title = getattr(ent, "title", None) or f"Channel {ent.id}"
    else:
        return None

    return {
        "id": ent.id,
        "title": title,
        "type": chat_type,
        "username": getattr(ent, "username", None),
    }


async def get_telegram_structure(
    db: AsyncSession,
    owner_user_id: int,
    limit: Optional[int] = None,
    include_archived: bool = False,
) -> Dict:
    """
    Возвращает структуру чатов пользователя:
      {
        "chats": [{id,title,type,username}, ...]   # активные диалоги
        "folders": [
          {"id": int, "title": str, "emoticon": str|None, "chat_ids": [int,...]},
          ...
        ]
      }

    Папки берутся из Telegram (messages.getDialogFilters). Сейчас
    учитываем только явные включения (pinned_peers + include_peers) —
    категорийные правила (все группы / все боты / etc.) пока не
    раскрываем; UI про них договорились пока не заморачиваться.

    Архивные чаты (folder_id == 1) по умолчанию скрываются.
    Если ни одной пользовательской папки нет — folders == [],
    фронт рендерит плоский список.

    Про лимит диалогов:
      Telethon get_dialogs(limit=N) возвращает N САМЫХ свежих по
      активности. Если у пользователя 500 диалогов, а лимит 200 —
      хвост из менее активных DM выпадет, и в папке "Важные" мы
      увидим только активную её часть. Поэтому по умолчанию тянем
      все диалоги (limit=None). Для большого аккаунта это будет
      одна развёртка по страницам — секунда-другая, приемлемо.
    """
    client = await ensure_connected(db, owner_user_id)

    if not await client.is_user_authorized():
        raise ValueError("TELEGRAM_NOT_AUTHORIZED")

    # --- 1) Активные диалоги ---
    # limit=None в Telethon означает "все диалоги" — это правильное
    # поведение для иерархии папок, иначе хвост чатов выпадет и в
    # папках появятся "дыры".
    dialogs = await client.get_dialogs(limit=limit)
    chats: List[Dict] = []
    chat_ids_alive: set = set()

    # Параллельно копим: dialog для категорийного матчинга в папках.
    # Чтобы не итерировать `dialogs` дважды на каждую папку, сразу
    # собираем кортежи (entity_id, entity, dialog) для активных чатов.
    alive_dialog_index: List[tuple] = []  # [(id, entity, dialog), ...]

    for d in dialogs:
        # archive lives in folder_id == 1 в Telegram; основной — 0
        if not include_archived and getattr(d, "folder_id", 0) == 1:
            continue
        ent = d.entity
        item = _dialog_entity_to_chat_dict(ent)
        if item is None:
            continue
        chats.append(item)
        chat_ids_alive.add(item["id"])
        alive_dialog_index.append((item["id"], ent, d))

    # --- 2) Папки ---
    folders: List[Dict] = []
    try:
        from telethon.tl.functions.messages import GetDialogFiltersRequest
        from telethon.tl.types import DialogFilter as _DialogFilter
        try:
            from telethon.tl.types import DialogFilterChatlist as _DialogFilterChatlist  # type: ignore
            _FILTER_TYPES = (_DialogFilter, _DialogFilterChatlist)
        except ImportError:
            _FILTER_TYPES = (_DialogFilter,)  # type: ignore

        raw = await client(GetDialogFiltersRequest())
        # В новых Telethon возвращается messages.DialogFilters с .filters,
        # в более старых — сразу список. Поддерживаем оба варианта.
        filters_iter = getattr(raw, "filters", None)
        if filters_iter is None:
            filters_iter = raw if isinstance(raw, (list, tuple)) else []

        def _peer_to_id(peer):
            return (
                getattr(peer, "channel_id", None)
                or getattr(peer, "chat_id", None)
                or getattr(peer, "user_id", None)
            )

        for f in filters_iter:
            if not isinstance(f, _FILTER_TYPES):
                # DialogFilterDefault ("Все чаты") и прочее — пропускаем.
                continue

            # ---- 2a) Явные включения (pinned + include) ----
            explicit_ids: set = set()
            ordered_ids: List[int] = []
            pinned = list(getattr(f, "pinned_peers", None) or [])
            included = list(getattr(f, "include_peers", None) or [])
            for peer in pinned + included:
                peer_id = _peer_to_id(peer)
                if peer_id is None or peer_id in explicit_ids:
                    continue
                explicit_ids.add(peer_id)
                ordered_ids.append(peer_id)

            # ---- 2b) Явные исключения ----
            excluded_ids: set = set()
            for peer in (getattr(f, "exclude_peers", None) or []):
                peer_id = _peer_to_id(peer)
                if peer_id is not None:
                    excluded_ids.add(peer_id)

            # ---- 2c) Категорийные авто-включения ----
            # Telegram-папка может включать чаты по типу: все контакты,
            # все группы, все каналы, все боты, все НЕ-контакты. Этих
            # чатов нет в include_peers — они подцепляются автоматически.
            # Поэтому проходим по нашему dialog-индексу и добавляем
            # совпавшие. Это объясняет случай, когда "Важные" с правилом
            # "все контакты" даёт только частичный список — без этого
            # блока мы видели бы только явно прикреплённые DM.
            has_contacts = bool(getattr(f, "contacts", False))
            has_non_contacts = bool(getattr(f, "non_contacts", False))
            has_groups = bool(getattr(f, "groups", False))
            has_broadcasts = bool(getattr(f, "broadcasts", False))
            has_bots = bool(getattr(f, "bots", False))
            has_any_category = (
                has_contacts or has_non_contacts or has_groups
                or has_broadcasts or has_bots
            )

            category_ids_ordered: List[int] = []
            if has_any_category:
                for ent_id, ent, _d in alive_dialog_index:
                    if ent_id in explicit_ids:
                        continue  # уже учтён в pinned/include
                    matched = False
                    if isinstance(ent, User):
                        is_bot = bool(getattr(ent, "bot", False))
                        is_contact = bool(getattr(ent, "contact", False))
                        if has_bots and is_bot:
                            matched = True
                        elif has_contacts and is_contact and not is_bot:
                            matched = True
                        elif has_non_contacts and (not is_contact) and (not is_bot):
                            matched = True
                    elif isinstance(ent, Chat):
                        if has_groups:
                            matched = True
                    elif isinstance(ent, Channel):
                        is_broadcast = bool(getattr(ent, "broadcast", False))
                        if has_broadcasts and is_broadcast:
                            matched = True
                        elif has_groups and not is_broadcast:
                            matched = True
                    if matched:
                        category_ids_ordered.append(ent_id)

            # ---- 2d) Финальная сборка: явные + категорийные − исключённые ----
            chat_ids: List[int] = []
            seen: set = set()
            for cid in ordered_ids + category_ids_ordered:
                if cid in seen:
                    continue
                if cid in excluded_ids:
                    continue
                if cid not in chat_ids_alive:
                    # подстраховка — мы итерируем по живым, но peer
                    # из include_peers мог ссылаться на архивный/недоступный
                    continue
                seen.add(cid)
                chat_ids.append(cid)

            folders.append({
                "id": int(getattr(f, "id", 0) or 0),
                "title": (getattr(f, "title", "") or "").strip() or "—",
                "emoticon": (getattr(f, "emoticon", None) or None),
                "chat_ids": chat_ids,
            })

    except Exception:
        # На любую ошибку с папками падать на плоский список — это
        # лучше, чем рушить весь /tg/chats. На фронте просто не будет
        # папок, а чаты останутся видимыми.
        folders = []

    return {"chats": chats, "folders": folders}


async def list_user_chats(db: AsyncSession, owner_user_id: int, limit: Optional[int] = None):
    """
    Совместимый shim над get_telegram_structure: возвращает только
    список чатов (без папок), чтобы старый код не сломался.
    По умолчанию limit=None — тянем все диалоги.
    """
    data = await get_telegram_structure(db, owner_user_id, limit=limit)
    return data["chats"]

# --- LOGOUT / CLEAN SESSION ---

import os
from pathlib import Path

async def logout_telegram(db: AsyncSession, owner_user_id: int):
    """
    Корректно завершает сессию Telegram:
    1) log_out() -> чтобы Telegram убрал активную сессию из списка устройств
    2) disconnect()
    3) удаляем локальные файлы session_cotel.session (и journal если есть)
    """
    client = await ensure_connected(db, owner_user_id)

    try:
        # если уже авторизованы — делаем log_out, чтобы сессия исчезла в Telegram
        if await client.is_user_authorized():
            await client.log_out()
    finally:
        # на всякий случай рвём соединение
        try:
            await client.disconnect()
        except Exception:
            pass

        # удаляем клиента именно этого пользователя из runtime-кэша
        tg_clients.pop(owner_user_id, None)

        # чистим локальный файл сессии
        for fname in ["session_cotel.session", "session_cotel.session-journal"]:
            try:
                Path(fname).unlink(missing_ok=True)
            except Exception:
                pass

    return True

async def qr_login_start(db: AsyncSession, owner_user_id: int):
    global _qr_login, _qr_wait_task
    client = await ensure_connected(db, owner_user_id)

    _qr_login = await client.qr_login()

    # создаём ОДИН task ожидания подтверждения
    _qr_wait_task = asyncio.create_task(_qr_login.wait())

    expires = getattr(_qr_login, "expires", None)
    return {
        "url": _qr_login.url,
        "expires": expires.isoformat() if expires else None,
    }


async def fetch_chat_messages_for_subscription(
    db: AsyncSession,
    owner_user_id: int,
    chat_link: str,
    since_dt: datetime,
    min_id: Optional[int] = None,
    limit: int = 3000,
) -> Tuple[object, List[Dict]]:
    """
    Возвращает entity и список сообщений (старые -> новые) со стабильными message_id.
    since_dt: нижняя граница по времени (UTC).
    min_id: если задан — берём только сообщения с id > min_id (cursor).
    """
    client = await ensure_connected(db, owner_user_id)

    if not await client.is_user_authorized():
        raise ValueError("TELEGRAM_NOT_AUTHORIZED")

    # нормализация, как в fetch_chat_messages
    link = chat_link.strip()
    if "t.me/" in link:
        link = link.split("t.me/")[-1].split("?")[0].strip("/")
    if link.startswith("@"):
        link = link[1:].strip()

    # ---- entity resolve ----
    entity = None
    link = (chat_link or "").strip()

    # 1) numeric chat_id (ID из dialogs)
    if link.isdigit():
        target_id = int(link)

        try:
            # сначала пробуем из dialogs
            dialogs = await client.get_dialogs(limit=500)
            for d in dialogs:
                ent = d.entity
                if getattr(ent, "id", None) == target_id:
                    entity = ent
                    break
        except Exception:
            pass

        # fallback: прямой get_entity
        if entity is None:
            try:
                entity = await client.get_entity(target_id)
            except Exception as e:
                raise ValueError(f"CHAT_RESOLVE_FAILED: {str(e)}")

    else:
        # 2) invite / username
        entity = await resolve_entity_with_invite(client, chat_link)

    if not entity:
        raise ValueError("CHAT_ENTITY_NOT_RESOLVED")

    if since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=timezone.utc)

    out = []
    sender_cache: Dict[int, str] = {}
    fetch_stats: dict = {}
    async for msg in client.iter_messages(entity, limit=limit, min_id=min_id or 0):
        if not msg or not getattr(msg, "date", None):
            continue

        msg_dt = msg.date
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=timezone.utc)

        # фильтр по времени используем только для первого запуска (когда cursor не задан)
        if (min_id is None) and (msg_dt < since_dt):
            break

        text = (msg.message or "").strip()
        if not text:
            continue

        author_id, author_display = await _resolve_sender_cached(msg, sender_cache, fetch_stats)

        reply_to = None
        try:
            reply_to = getattr(msg, "reply_to_msg_id", None)
        except Exception:
            reply_to = None

        out.append({
            "message_id": msg.id,
            "message_ts": msg_dt.isoformat(),
            "author_id": author_id,
            "author_display": author_display,
            "text": text,
            "reply_to": int(reply_to) if reply_to else None,
        })

    out.reverse()  # старые -> новые

    log.warning(
        "QA_DIAG fetch path=subscription kept=%d sender_lookups=%d unique_senders=%d "
        "floods=%d flood_sec=%d",
        len(out), fetch_stats.get("sender_lookups", 0), len(sender_cache),
        fetch_stats.get("flood_waits", 0), fetch_stats.get("flood_seconds", 0),
    )

    return entity, out

async def qr_login_status(db: AsyncSession, owner_user_id: int):
    global _qr_login, _qr_wait_task
    client = await ensure_connected(db, owner_user_id)

    if _qr_login is None or _qr_wait_task is None:
        return {"status": "no_qr"}

    # 1) expiry
    expires = getattr(_qr_login, "expires", None)
    if expires is not None:
        now = datetime.now(timezone.utc)
        exp = expires if expires.tzinfo else expires.replace(tzinfo=timezone.utc)
        if now >= exp:
            # отменяем старый task (если ещё жив)
            if _qr_wait_task and not _qr_wait_task.done():
                _qr_wait_task.cancel()
            return {"status": "expired"}

    # 2) если ещё не завершился — ждём
    if not _qr_wait_task.done():
        return {"status": "waiting"}

    # 3) task завершился: либо успех, либо 2FA, либо ошибка
    try:
        _qr_wait_task.result()  # если тут исключение — упадём в except
        me = await get_current_user(db, owner_user_id)
        if not me:
            return {"status": "authorized", "username": None}

        return {
            "status": "authorized",
            "user_id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": me.phone,
        }

    except SessionPasswordNeededError:
        return {"status": "password_needed"}

    except Exception as e:
        return {"status": "error", "detail": str(e)}

async def qr_login_recreate(db: AsyncSession, owner_user_id: int):
    global _qr_login
    client = await ensure_connected(db, owner_user_id)

    if _qr_login is None:
        return await qr_login_start(db, owner_user_id)
    await _qr_login.recreate()
    expires = getattr(_qr_login, "expires", None)
    return {"url": _qr_login.url, "expires": expires.isoformat() if expires else None}

async def disconnect_tg_client():
    """Отключает все runtime-клиенты Telegram и очищает кэш tg_clients.

    Вызывается в конце тика subscriptions_runner, чтобы освободить
    соединения. Раньше функция обращалась к одиночной глобальной
    tg_client, которой больше нет (перешли на пер-юзер словарь
    tg_clients) — отсюда NameError 'tg_client' is not defined.
    """
    for owner_user_id, client in list(tg_clients.items()):
        if client is not None:
            try:
                await client.disconnect()
            except Exception:
                pass
        tg_clients.pop(owner_user_id, None)
