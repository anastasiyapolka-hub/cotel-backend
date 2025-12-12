from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError
import json
from openai import OpenAI
import os

from telegram_service import (
    send_login_code,
    confirm_login,
    confirm_password,
    get_current_user,
    fetch_chat_messages,
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://cotel.onrender.com",
        "http://localhost:3000",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))



@app.get("/health")
async def health():
    return {"status": "ok"}


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


async def call_openai_summary(user_query: str, chat_name: str, text_messages):
    """
    Вызывает OpenAI gpt-4.1-mini, чтобы ответить на произвольный запрос по чату.
    """
    # Собираем текст переписки в удобный вид
    lines = []
    for msg in text_messages:
        date = msg.get("date") or ""
        sender = msg.get("from") or "Unknown"
        text = msg.get("text") or ""
        # Для LLM важен только текст, но немного метаданных не помешает
        lines.append(f"[{date}] {sender}: {text}")

    context = "\n".join(lines)

    if not context:
        return "В чате нет текстовых сообщений для анализа."

    system_prompt = (
        "Ты аналитик переписок в Telegram.\n"
        "Тебе даётся фрагмент чата и запрос пользователя.\n"
        "Найди по смыслу релевантные сообщения и дай краткое, структурированное "
        "summary по-русски. Если информации мало, честно скажи об этом."
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

@app.post("/analyze")
async def analyze_chat(
    file: UploadFile = File(...),
        params: str = Form("{}"),
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


    # 1. Проверяем расширение файла
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(
            status_code=400,
            detail="Ожидается JSON-файл экспорта Telegram (.json)",
        )

    # 2. Читаем файл в память
    raw_bytes = await file.read()

    # 3. Пробуем распарсить JSON
    try:
        data = json.loads(raw_bytes)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Ошибка: Файл не является корректным JSON."
        )

    # 4. Проверка структуры Telegram экспорта (опционально)
    messages = data.get("messages")
    if messages is None:
        raise HTTPException(
            status_code=400,
            detail="JSON не содержит поле 'messages'. Возможно, экспорт выполнен в HTML-формате."
        )

    if not isinstance(messages, list):
        raise HTTPException(
            status_code=400,
            detail="Поле 'messages' должно быть списком сообщений"
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
            summary = await call_openai_summary(
                user_query=user_query,
                chat_name=chat_name,
                text_messages=text_messages,
            )
        except Exception as e:
            # Чтобы фронт получил понятную ошибку
            raise HTTPException(status_code=500, detail=f"LLM_ERROR: {str(e)}")

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
        "summary": summary
    }

@app.post("/tg/send_code")
async def tg_send_code(payload: dict):
    phone = (payload.get("phone") or "").strip()
    if not phone:
        raise HTTPException(400, "PHONE_REQUIRED")
    try:
        await send_login_code(phone)
    except Exception as e:
        raise HTTPException(400, f"TELEGRAM_ERROR: {e}")
    return {"status": "code_sent"}

@app.post("/tg/confirm_code")
async def tg_confirm_code(payload: dict):
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
            await confirm_login(phone, code)

            # получаем текущего пользователя
            me = await get_current_user()


        except ValueError as ve:

            err = str(ve)

            if err == "PHONE_CODE_INVALID":
                raise HTTPException(status_code=400, detail="PHONE_CODE_INVALID")

            if err == "PASSWORD_NEEDED":
                raise HTTPException(status_code=400, detail="SESSION_PASSWORD_NEEDED")

            raise HTTPException(status_code=400, detail=f"TELEGRAM_ERROR: {err}")

        return {
            "status": "authorized",
            "user_id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": me.phone,
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

@app.post("/tg/confirm_password")
async def tg_confirm_password(payload: dict):
    try:
        password = (payload.get("password") or "").strip()

        if not password:
            raise HTTPException(
                status_code=400,
                detail="PASSWORD_REQUIRED"
            )

        # завершаем 2FA-авторизацию
        await confirm_password(password)

        me = await get_current_user()

        return {
            "status": "authorized",
            "user_id": me.id,
            "username": me.username,
            "first_name": me.first_name,
            "phone": me.phone,
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"TG_PASSWORD_CONFIRM_FAILED: {str(e)}"
        )


@app.post("/tg/analyze_chat")
async def tg_analyze_chat(payload: dict):
    chat_link = (payload.get("chat_link") or "").strip()
    user_query = (payload.get("user_query") or "").strip()
    days = int(payload.get("days") or 7)

    me = await get_current_user()
    if not me:
        raise HTTPException(401, "TELEGRAM_NOT_AUTHORIZED")

    entity, messages = await fetch_chat_messages(chat_link, days)
    chat_name = getattr(entity, "title", None) or getattr(entity, "username", "Без названия")

    summary = await call_openai_summary(
        user_query=user_query,
        chat_name=chat_name,
        text_messages=messages,
    )

    return {
        "status": "ok",
        "summary": summary,
        "chat_name": chat_name,
        "messages_count": len(messages),
    }


