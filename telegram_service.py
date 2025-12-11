# telegram_service.py

import os
from datetime import datetime, timedelta, timezone

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


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------------------------------

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


async def get_current_user():
    """
    Проверить, авторизованы ли мы в Telegram.
    """
    await ensure_connected()
    if not await tg_client.is_user_authorized():
        return None

    return await tg_client.get_me()
