# telegram_service.py
import asyncio
import os

from typing import Optional, Tuple, List, Dict
from datetime import datetime, timedelta, timezone
from telethon.tl.types import Message
from telethon import TelegramClient
from telethon.errors import (
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.errors import InviteHashInvalidError, InviteHashExpiredError, UserAlreadyParticipantError

_qr_login = None  # telethon.tl.custom.qrlogin.QRLogin | None
_qr_wait_task = None  # asyncio.Task | None

# Получаем ключи
api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]

# Создаём клиент сессии
tg_client = TelegramClient("session_cotel", api_id, api_hash)


# ---- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------------------------------

import re
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest

INVITE_RE = re.compile(r"(?:t\.me\/\+|t\.me\/joinchat\/)([A-Za-z0-9_-]+)")

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

async def ensure_connected():
    """Подключаемся к Telegram, если соединение отсутствует."""
    if not tg_client.is_connected():
        await tg_client.connect()

async def send_login_code(phone: str):
    """
    Отправить код на телефон.
    """
    await ensure_connected()
    try:
        return await tg_client.send_code_request(phone)
    except PhoneNumberInvalidError:
        raise ValueError("PHONE_NUMBER_INVALID")

async def confirm_login(phone: str, code: str):
    """
    Подтвердить код и завершить авторизацию.
    """
    await ensure_connected()

    try:
        me = await tg_client.sign_in(phone=phone, code=code)
        return me

    except PhoneCodeInvalidError:
        raise ValueError("PHONE_CODE_INVALID")

    except SessionPasswordNeededError:
        raise ValueError("PASSWORD_NEEDED")   # 2FA включена

async def confirm_password(password: str):
    await ensure_connected()
    # завершает вход при включённой 2FA
    await tg_client.sign_in(password=password)

async def get_current_user():
    """
    Проверить, авторизованы ли мы в Telegram.
    """
    await ensure_connected()
    if not await tg_client.is_user_authorized():
        return None

    return await tg_client.get_me()


async def fetch_chat_messages(chat_link: str, days: int = 7):
    """
    Возвращает:
      entity: объект чата/канала (Telethon entity)
      messages: список в формате [{date, from, text}, ...] для LLM
    """
    await ensure_connected()

    if not await tg_client.is_user_authorized():
        raise ValueError("TELEGRAM_NOT_AUTHORIZED")

    if not chat_link:
        raise ValueError("CHAT_LINK_REQUIRED")

    # Нормализуем ввод: https://t.me/xxx -> xxx, @xxx -> xxx
    link = chat_link.strip()
    if "t.me/" in link:
        link = link.split("t.me/")[-1].split("?")[0].strip("/")
    if link.startswith("@"):
        link = link[1:].strip()

    # Дата отсечения
    since_dt = datetime.now(timezone.utc) - timedelta(days=int(days))

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
            invite = await tg_client(CheckChatInviteRequest(invite_hash))

            # Если уже участник — Telethon вернёт объект с чатом
            if hasattr(invite, "chat") and invite.chat:
                entity = invite.chat
            else:
                # Иначе нужно вступить (для чтения приватной истории иначе доступа не будет)
                try:
                    upd = await tg_client(ImportChatInviteRequest(invite_hash))
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

                dialogs = await tg_client.get_dialogs(limit=500)

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

                    entity = await tg_client.get_entity(target_id)

                except Exception as e:

                    raise ValueError(f"CHAT_RESOLVE_FAILED: {str(e)}")


        # 2) Иначе — username / @username

        else:

            try:

                entity = await tg_client.get_entity(link)

            except Exception as e:

                raise ValueError(f"CHAT_RESOLVE_FAILED: {str(e)}")

    collected = []
    try:
        # Telethon iter_messages возвращает от новых к старым
        async for msg in tg_client.iter_messages(entity, limit=5000):
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

            sender_name = "Unknown"
            try:
                sender = await msg.get_sender()
                if sender is not None:
                    # username предпочтительнее, иначе имя/фамилия
                    if getattr(sender, "username", None):
                        sender_name = "@" + sender.username
                    else:
                        first = (getattr(sender, "first_name", "") or "").strip()
                        last = (getattr(sender, "last_name", "") or "").strip()
                        sender_name = (first + " " + last).strip() or "Unknown"
            except Exception:
                # если не получилось получить отправителя — не критично
                pass

            collected.append({
                "date": msg_dt.isoformat(),
                "from": sender_name,
                "text": text,
            })

    except Exception as e:
        raise ValueError(f"CHAT_FETCH_FAILED: {str(e)}")

    # collected сейчас от новых к старым — разворачиваем, чтобы было "старые -> новые"
    collected.reverse()

    return entity, collected

from telethon.tl.types import User, Chat, Channel

async def list_user_chats(limit: int = 500):
    """
    Возвращает список доступных диалогов пользователя для выбора на фронте.
    Отдаём минимум полей: id, title, type, username (если есть).
    """
    await ensure_connected()

    if not await tg_client.is_user_authorized():
        raise ValueError("TELEGRAM_NOT_AUTHORIZED")

    dialogs = await tg_client.get_dialogs(limit=limit)

    result = []
    for d in dialogs:
        ent = d.entity

        # Тип
        if isinstance(ent, User):
            chat_type = "user"
            title = " ".join(filter(None, [getattr(ent, "first_name", None), getattr(ent, "last_name", None)])).strip()
            if not title:
                title = getattr(ent, "username", None) or f"User {ent.id}"
        elif isinstance(ent, Chat):
            chat_type = "group"
            title = getattr(ent, "title", None) or f"Group {ent.id}"
        elif isinstance(ent, Channel):
            # Channel может быть и каналом, и супергруппой
            chat_type = "channel" if getattr(ent, "broadcast", False) else "supergroup"
            title = getattr(ent, "title", None) or f"Channel {ent.id}"
        else:
            # На всякий случай
            continue

        # Базовые поля
        item = {
            "id": ent.id,
            "title": title,
            "type": chat_type,
            "username": getattr(ent, "username", None),
        }

        # (опционально) признаки для UI
        # is_verified / is_scam / is_fake можно добавить позже, если захочешь

        result.append(item)

    return result

# --- LOGOUT / CLEAN SESSION ---

import os
from pathlib import Path

async def logout_telegram():
    """
    Корректно завершает сессию Telegram:
    1) log_out() -> чтобы Telegram убрал активную сессию из списка устройств
    2) disconnect()
    3) удаляем локальные файлы session_cotel.session (и journal если есть)
    """
    await ensure_connected()

    try:
        # если уже авторизованы — делаем log_out, чтобы сессия исчезла в Telegram
        if await tg_client.is_user_authorized():
            await tg_client.log_out()
    finally:
        # на всякий случай рвём соединение, чтобы не держать sqlite-lock
        try:
            await tg_client.disconnect()
        except Exception:
            pass

        # чистим локальный файл сессии
        for fname in ["session_cotel.session", "session_cotel.session-journal"]:
            try:
                Path(fname).unlink(missing_ok=True)
            except Exception:
                pass

    return True

async def qr_login_start():
    global _qr_login, _qr_wait_task
    await ensure_connected()

    _qr_login = await tg_client.qr_login()

    # создаём ОДИН task ожидания подтверждения
    _qr_wait_task = asyncio.create_task(_qr_login.wait())

    expires = getattr(_qr_login, "expires", None)
    return {
        "url": _qr_login.url,
        "expires": expires.isoformat() if expires else None,
    }


async def fetch_chat_messages_for_subscription(
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
    await ensure_connected()

    if not await tg_client.is_user_authorized():
        raise ValueError("TELEGRAM_NOT_AUTHORIZED")

    # нормализация, как в fetch_chat_messages
    link = chat_link.strip()
    if "t.me/" in link:
        link = link.split("t.me/")[-1].split("?")[0].strip("/")
    if link.startswith("@"):
        link = link[1:].strip()

    # ВАЖНО: вместо get_entity — invite-aware резолвер
    entity = await resolve_entity_with_invite(tg_client, chat_link)

    if since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=timezone.utc)

    out = []
    async for msg in tg_client.iter_messages(entity, limit=limit, min_id=min_id or 0):
        if not msg or not getattr(msg, "date", None):
            continue

        msg_dt = msg.date
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=timezone.utc)

        # фильтр по времени (работает и для первого запуска)
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

        out.append({
            "message_id": msg.id,
            "message_ts": msg_dt.isoformat(),
            "author_id": author_id,
            "author_display": author_display,
            "text": text,
        })

    out.reverse()  # старые -> новые
    return entity, out

async def qr_login_status():
    global _qr_login, _qr_wait_task
    await ensure_connected()

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
        me = await get_current_user()
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

async def qr_login_recreate():
    global _qr_login
    await ensure_connected()
    if _qr_login is None:
        return await qr_login_start()
    await _qr_login.recreate()
    expires = getattr(_qr_login, "expires", None)
    return {"url": _qr_login.url, "expires": expires.isoformat() if expires else None}
