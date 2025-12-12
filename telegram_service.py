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

    try:
        entity = await tg_client.get_entity(link)
    except Exception as e:
        # сюда попадают: неверный username, приватный канал без доступа, и т.д.
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
