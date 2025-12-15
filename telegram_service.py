# telegram_service.py

import os
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

# Получаем ключи
api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]

# Создаём клиент сессии
tg_client = TelegramClient("session_cotel", api_id, api_hash)


# ---- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------------------------------

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
