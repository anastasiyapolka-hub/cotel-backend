from telethon import TelegramClient
import asyncio

api_id = 33443696        # вставь свой
api_hash = "f31c0e300ea6a51fbd17251b3b019322"  # вставь свой

async def main():
    client = TelegramClient("test_session", api_id, api_hash)

    try:
        await client.connect()
        print("Connected to Telegram.")

        # Проверяем, доступен ли аккаунт (конечно, он не авторизован)
        authorized = await client.is_user_authorized()
        print("Is authorized:", authorized)

    except Exception as e:
        print("ERROR:", e)

    finally:
        await client.disconnect()

asyncio.run(main())