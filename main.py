from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request, Depends

from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError

from openai import OpenAI
import os
import httpx
import json
import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError


from db.models import Subscription, SubscriptionState, MatchEvent  # как у тебя называется
from db.session import get_db  # как у тебя называется
from db.models import BotUserLink

from schemas.subscriptions import SubscriptionCreate, SubscriptionOut, ToggleRequest

import time
from datetime import datetime, timedelta, timezone

from telegram_service import (
    send_login_code,
    confirm_login,
    confirm_password,
    get_current_user,
    fetch_chat_messages,
    list_user_chats,
    logout_telegram,
    qr_login_start,
    qr_login_status,
    fetch_chat_messages_for_subscription,
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
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


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

async def call_openai_subscription_match(prompt: str, chat_title: str, messages: list[dict]) -> dict:
    """
    Возвращает JSON строго по контракту:
    {found: bool, matches: [...], summary_reason: str, confidence: float}
    """
    # ограничим контекст, чтобы не сжечь токены на MVP
    tail = messages[-250:]

    lines = []
    for m in tail:
        mid = m.get("message_id")
        ts = m.get("message_ts")
        a = m.get("author_display") or "Unknown"
        txt = m.get("text") or ""
        lines.append(f"[{mid}] [{ts}] {a}: {txt}")

    context = "\n".join(lines)
    if not context:
        return {"found": False, "matches": [], "summary_reason": "Нет текстовых сообщений.", "confidence": 0.0}

    system_prompt = (
        "Ты ассистент, который ищет смысловые совпадения в новых сообщениях Telegram-чата.\n"
        "Ответь СТРОГО валидным JSON без markdown/кода/комментариев.\n"
        "Если совпадений нет — found=false и matches=[]\n"
        "message_id бери только из входных строк вида [12345].\n"
    )

    user_prompt = (
        f"Название чата: {chat_title}\n\n"
        f"Запрос подписки пользователя:\n{prompt}\n\n"
        f"Новые сообщения (каждая строка содержит message_id в квадратных скобках):\n{context}\n\n"
        "Верни JSON формата:\n"
        "{\n"
        '  "found": true/false,\n'
        '  "matches": [\n'
        "    {\n"
        '      "message_id": 123,\n'
        '      "message_ts": "ISO8601",\n'
        '      "author_display": "string",\n'
        '      "author_id": 123,\n'
        '      "excerpt": "string",\n'
        '      "reason": "string"\n'
        "    }\n"
        "  ],\n"
        '  "summary_reason": "string",\n'
        '  "confidence": 0.0\n'
        "}\n"
    )

    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )

    raw = completion.choices[0].message.content.strip()

    # простая, но рабочая защита от “лишнего текста”
    import json
    try:
        return json.loads(raw)
    except Exception:
        # попытка вытащить JSON-блок
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start:end+1])
        raise

@app.post("/subscriptions/run")
async def run_subscriptions(db: AsyncSession = Depends(get_db)):
    t0 = time.perf_counter()

    # 1) Берём активные подписки (MVP: без owner_user_id фильтра)
    res = await db.execute(
        select(Subscription).where(Subscription.is_active == True)
    )
    subs = list(res.scalars().all())

    total_checked = 0
    total_matches = 0
    processed = 0

    now = datetime.now(timezone.utc)

    for sub in subs:
        processed += 1

        # 2) Берём state
        st_res = await db.execute(
            select(SubscriptionState).where(SubscriptionState.subscription_id == sub.id)
        )
        st = st_res.scalar_one_or_none()

        last_message_id = getattr(st, "last_message_id", None) if st else None

        # 3) Определяем окно чтения
        #    - если last_message_id нет => читаем за frequency_minutes назад (первый запуск)
        #    - если есть => читаем только новые по min_id, since_dt оставляем тоже как “страховку”
        freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)
        since_dt = now - timedelta(minutes=freq_min)

        # 4) Читаем сообщения
        try:
            entity, msgs = await fetch_chat_messages_for_subscription(
                chat_link=sub.chat_ref,
                since_dt=since_dt,
                min_id=int(last_message_id) if last_message_id else None,
                limit=3000,
            )
        except Exception as e:
            # пишем ошибку в подписку и идём дальше
            await db.execute(
                update(Subscription)
                .where(Subscription.id == sub.id)
                .values(status="error", last_error=str(e), updated_at=sa.func.now())
            )
            await db.commit()
            continue

        checked = len(msgs)
        total_checked += checked

        # 5) Обновим last_message_id (если что-то прочитали)
        newest_id = max([m["message_id"] for m in msgs], default=last_message_id)

        # 6) LLM вызываем только если есть что анализировать
        matches_written = 0
        if checked > 0:
            chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"
            try:
                llm_json = await call_openai_subscription_match(
                    prompt=sub.prompt,
                    chat_title=chat_title,
                    messages=msgs,
                )
            except Exception as e:
                await db.execute(
                    update(Subscription)
                    .where(Subscription.id == sub.id)
                    .values(status="error", last_error=f"LLM_ERROR: {str(e)}", updated_at=sa.func.now())
                )
                await db.commit()
                continue

            found = bool(llm_json.get("found"))
            matches = llm_json.get("matches") or []

            if found and isinstance(matches, list):
                for m in matches:
                    mid = m.get("message_id")
                    if not mid:
                        continue

                    stmt = insert(MatchEvent).values(
                        subscription_id=sub.id,
                        message_id=int(mid),
                        message_ts=m.get("message_ts"),
                        author_id=m.get("author_id"),
                        author_display=m.get("author_display"),
                        excerpt=m.get("excerpt"),
                        reason=m.get("reason"),
                        llm_payload=llm_json,
                        notify_status="queued",
                    ).on_conflict_do_nothing(
                        constraint="uq_match_subscription_message"
                    )

                    r = await db.execute(stmt)
                    # rowcount может быть 0 при конфликте (дедуп)
                    if getattr(r, "rowcount", 0) == 1:
                        matches_written += 1

        total_matches += matches_written

        # 7) Обновляем state
        # state должен быть уже создан у тебя при создании подписки, но на всякий:
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

    elapsed = round(time.perf_counter() - t0, 2)

    return {
        "status": "ok",
        "processed_subscriptions": processed,
        "checked_messages": total_checked,
        "found_matches": total_matches,
        "elapsed_seconds": elapsed,
        "ui_message": f"Проверено {total_checked} сообщений, найдено {total_matches}",
    }

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

    try:
        entity, messages = await fetch_chat_messages(chat_link, days)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

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

@app.get("/tg/chats")
async def tg_list_chats(limit: int = 200):
    me = await get_current_user()
    if not me:
        raise HTTPException(status_code=401, detail="TELEGRAM_NOT_AUTHORIZED")

    try:
        chats = await list_user_chats(limit=limit)
        return {
            "status": "ok",
            "count": len(chats),
            "chats": chats,
        }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_CHATS_FAILED: {str(e)}")

@app.post("/tg/logout")
async def tg_logout():
    try:
        await logout_telegram()
        return {"status": "logged_out"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_LOGOUT_FAILED: {str(e)}")

@app.post("/tg/qr/start")
async def tg_qr_start():
    try:
        data = await qr_login_start()
        return {"status": "ok", **data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_QR_START_FAILED: {str(e)}")

@app.get("/tg/qr/status")
async def tg_qr_status():
    try:
        data = await qr_login_status()
        # если авторизованы — это уже готовая сессия Telethon, ничего отдельно сохранять не надо:
        # tg_client сам пишет session файл "session_cotel.session" как и раньше :contentReference[oaicite:4]{index=4}
        return data
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_QR_STATUS_FAILED: {str(e)}")


async def bot_send_message(chat_id: int, text: str):
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        await client.post(url, json={
            "chat_id": chat_id,
            "text": text,
        })

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

    # 2) Извлечь message/chat/user/text
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

    # 4) Upsert в bot_user_link по уникальному telegram_chat_id
    stmt = insert(BotUserLink).values(
        owner_user_id=None,  # пока нет auth — оставляем пустым
        telegram_chat_id=telegram_chat_id,
        telegram_user_id=telegram_user_id,
        is_blocked=False,
    ).on_conflict_do_update(
        index_elements=["telegram_chat_id"],
        set_={
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
async def tg_bot_link_status(db: AsyncSession = Depends(get_db)):
    q = select(sa.func.count()).select_from(BotUserLink).where(BotUserLink.is_blocked == False)  # noqa: E712
    count = (await db.execute(q)).scalar_one()
    return {"connected": count > 0}

@app.post("/subscriptions", response_model=SubscriptionOut)
async def create_subscription(payload: SubscriptionCreate, db: AsyncSession = Depends(get_db)):
    # 1) создаём подписку
    sub = Subscription(
        owner_user_id=payload.owner_user_id,
        name=payload.name,
        source_mode=payload.source_mode,
        chat_ref=payload.chat_ref,
        chat_id=None,  # пока не резолвим в числовой id
        frequency_minutes=payload.frequency_minutes,
        prompt=payload.prompt,
        is_active=payload.is_active,
        status="active" if payload.is_active else "paused",
        last_error=None,
    )

    db.add(sub)

    try:
        await db.flush()  # чтобы получить sub.id без commit
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="SUBSCRIPTION_CONFLICT")

    # 2) сразу создаём subscription_state
    state = SubscriptionState(
        subscription_id=sub.id,
        last_message_id=None,
        last_checked_at=None,
        last_success_at=None,
    )
    db.add(state)

    await db.commit()
    await db.refresh(sub)

    return sub


@app.get("/subscriptions", response_model=list[SubscriptionOut])
async def list_subscriptions(db: AsyncSession = Depends(get_db)):
    # пока без фильтра по owner_user_id — так как один пользователь
    res = await db.execute(select(Subscription).order_by(Subscription.id.desc()))
    return res.scalars().all()


@app.post("/subscriptions/{subscription_id}/toggle", response_model=SubscriptionOut)
async def toggle_subscription(subscription_id: int, payload: ToggleRequest, db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")

    sub.is_active = payload.is_active
    sub.status = "active" if payload.is_active else "paused"
    sub.last_error = None
    # updated_at у тебя server_default now() — но  при update лучше руками:
    sub.updated_at = sa.text("now()")  # или просто не трогать, если триггер/orm делает

    await db.commit()
    await db.refresh(sub)
    return sub

