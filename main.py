from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request, Depends
from auth import router as auth_router
from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError

from openai import OpenAI
import os
import httpx
import json
import hashlib
import secrets

from datetime import datetime, timezone, timedelta
import time
import sqlalchemy as sa
import re

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.exc import IntegrityError

from db.models import Subscription, SubscriptionState, DigestEvent, MatchEvent, BotUserLink
from db.session import get_db
from auth import get_current_user as auth_get_current_user
from db.models import User
from db.models import BotLinkCode
from schemas.subscriptions import SubscriptionCreate, SubscriptionOut, ToggleRequest

from collections import defaultdict
from datetime import datetime, timezone, timedelta

from telegram_service import (
    send_login_code,
    confirm_login,
    confirm_password,
    get_current_user as tg_get_current_user,  # <-- переименовали
    fetch_chat_messages,
    list_user_chats,
    logout_telegram,
    qr_login_start,
    qr_login_status,
    fetch_chat_messages_for_subscription,
    export_string_session,
    save_user_telegram_session,
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://cotel.onrender.com",
        "http://localhost:3000",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEV_OWNER_USER_ID = int(os.getenv("DEV_OWNER_USER_ID", "1"))


@app.get("/health")
async def health():
    return {"status": "ok"}

def sha256_hex(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def make_link_code() -> str:
    # короткий, но достаточно случайный
    return secrets.token_urlsafe(12)

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

def parse_iso_ts(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        # поддержка "Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None

async def call_openai_subscription_match(prompt: str, chat_title: str, messages: list[dict]) -> dict:
    """
    Возвращает JSON строго по контракту:
    {found: bool, matches: [...], summary_reason: str, confidence: float}
    """
    # ограничим контекст, чтобы не сжечь токены на MVP
    tail = messages

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


    system_prompt = """
    Ты — классификатор сообщений Telegram для событийных подписок.
    Твоя задача: по списку сообщений найти те, которые соответствуют запросу пользователя ПО СМЫСЛУ.

    Учитывай:
    - синонимы, опечатки, латиницу/кириллицу, разговорные формулировки, сокращения
    - НЕ требуй точного совпадения ключевых слов
    - НЕ выдумывай автора/время/текст: используй только то, что дано во входных данных

    Если сообщение релевантно — верни его как match.
    В match:
    - excerpt: это цитата сообщения (обрезай до 300 символов, без пересказа)
    - reason: 1 короткая причина “почему подходит”

    Если совпадений нет — found=false и matches=[]
    message_id бери только из входных строк вида [12345].
    Формат ответа: СТРОГО JSON без пояснений/markdown/кода/комментариев..
    """

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

async def call_openai_subscription_digest(prompt: str, chat_title: str, messages: list[dict]) -> dict:
    """
    Возвращает JSON:
    {
      "digest_text": "string",
      "confidence": 0.0
    }
    """
    # Собираем контекст (со связями reply)
    lines = []
    for m in messages:
        mid = m.get("message_id")
        ts = m.get("message_ts")
        a = m.get("author_display") or "Unknown"
        txt = m.get("text") or ""
        r = m.get("reply_to")
        reply_tag = f" reply_to={int(r)}" if r else ""
        lines.append(f"[{mid}] [{ts}] {a}{reply_tag}: {txt}")

    context = "\n".join(lines)
    if not context:
        return {"digest_text": "", "confidence": 0.0}

    system_prompt = (
        "Ты — аналитик Telegram-диалогов.\n"
        "Тебе дан фрагмент чата (сообщения с reply_to=<id>, если это ответ) и запрос пользователя.\n"
        "Сделай резюме строго по запросу пользователя, без домыслов.\n"
        "Важно: не пересказывай весь чат подряд; выделяй только главное, группируй, делай выводы о почитанном.\n"
        "Если данных недостаточно — так и скажи.\n"
        "Ответ дай СТРОГО JSON без markdown.\n"
    )

    user_prompt = (
        f"Название чата: {chat_title}\n\n"
        f"Запрос пользователя для summary:\n{prompt}\n\n"
        "Сообщения (каждая строка содержит message_id в квадратных скобках, reply_to если есть):\n"
        f"{context}\n\n"
        "Верни JSON:\n"
        "{\n"
        '  "digest_text": "строка 500..4096 символов, максимум 4096",\n'
        '  "confidence": 0.0\n'
        "}\n"
    )

    completion = openai_client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
    )

    raw = completion.choices[0].message.content.strip()
    import json
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start:end+1])
        raise

def build_tg_message_link(chat_ref: str | None, chat_id: int | None, message_id: int | None) -> str | None:
    if not message_id:
        return None

    ref = (chat_ref or "").strip()

    # 1) username из @username
    if ref.startswith("@") and len(ref) > 1:
        uname = ref[1:]
        return f"https://t.me/{uname}/{message_id}"

    # 2) username из t.me/username или https://t.me/username
    m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{3,})", ref)
    if m:
        uname = m.group(1)
        # если это invite-ссылка вида t.me/+HASH — не подойдет
        if not uname.startswith("+"):
            return f"https://t.me/{uname}/{message_id}"

    # 3) приватный супергрупповой линк через /c/
    if chat_id:
        aid = abs(int(chat_id))
        s = str(aid)
        if s.startswith("100") and len(s) > 3:
            internal = s[3:]
            return f"https://t.me/c/{internal}/{message_id}"

    return None

@app.post("/tg/bot/link/start")
async def tg_bot_link_start(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # 1) генерим код
    code = make_link_code()
    code_hash = sha256_hex(code)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    # 2) сохраняем в bot_link_codes
    db.add(BotLinkCode(
        user_id=user.id,
        code_hash=code_hash,
        expires_at=expires_at,
        used_at=None,
    ))
    await db.commit()

    # 3) вернём код + deeplink (удобно для UI)
    bot_username = os.getenv("TELEGRAM_BOT_USERNAME", "").strip()  # например "CoTelBot"
    deeplink = None
    if bot_username:
        deeplink = f"https://t.me/{bot_username}?start={code}"

    return {
        "status": "ok",
        "code": code,
        "expires_at": expires_at.isoformat(),
        "deeplink": deeplink,
    }

def _serialize_match_event(ev) -> dict:
    # ev = MatchEvent ORM object
    return {
        "id": ev.id,
        "subscription_id": ev.subscription_id,
        "message_id": ev.message_id,
        "message_ts": ev.message_ts.isoformat() if ev.message_ts else None,
        "author_id": ev.author_id,
        "author_display": ev.author_display,
        "excerpt": ev.excerpt,
        "reason": ev.reason,
        "llm_payload": ev.llm_payload,
        "notify_status": ev.notify_status,
        "created_at": ev.created_at.isoformat() if ev.created_at else None,
    }


@app.post("/subscriptions/run")
async def run_subscriptions(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    t0 = time.perf_counter()
    run_started_at = datetime.now(timezone.utc)
    now = run_started_at

    owner_user_id = user.id

    # 1) Берём активные подписки ТОЛЬКО этого пользователя
    res = await db.execute(
        select(Subscription).where(
            Subscription.is_active == True,
            Subscription.owner_user_id == owner_user_id,
        )
    )
    subs = list(res.scalars().all())

    results = []
    total_checked = 0
    total_matches = 0

    for sub in subs:
        sub_report = {
            "subscription_id": sub.id,
            "name": getattr(sub, "name", None),
            "chat_ref": getattr(sub, "chat_ref", None),
            "status": "ok",
            "checked": 0,
            "matches_written": 0,
            "error": None,
            "llm_json": None,
            "llm_found": None,
            "llm_confidence": None,
            "llm_summary_reason": None,
            "llm_matches_count": 0,
            "inserted_message_ids": [],
            "match_events": [],
        }

        try:
            st_res = await db.execute(
                select(SubscriptionState).where(SubscriptionState.subscription_id == sub.id)
            )
            st = st_res.scalar_one_or_none()
            last_message_id = getattr(st, "last_message_id", None) if st else None

            freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)

            if last_message_id:
                since_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
                min_id = int(last_message_id)
            else:
                since_dt = now - timedelta(minutes=freq_min)
                min_id = None

            sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
            sub_report["subscription_type"] = sub_type

            entity, msgs = await fetch_chat_messages_for_subscription(
                db,
                owner_user_id,
                chat_link=sub.chat_ref,
                since_dt=since_dt,
                min_id=min_id,
                limit=1000,
            )

            # map для восстановления автора/времени по message_id
            msg_by_id = {}
            for mm in msgs:
                try:
                    mid0 = mm.get("message_id")
                    if mid0 is not None:
                        msg_by_id[int(mid0)] = mm
                except Exception:
                    continue

            if getattr(sub, "chat_id", None) is None:
                ent_id = getattr(entity, "id", None)
                if ent_id is not None:
                    sub.chat_id = int(ent_id)
                    await db.flush()

            checked = len(msgs)
            sub_report["checked"] = checked
            total_checked += checked

            # 5) newest_id
            ids = []
            for m in msgs:
                if isinstance(m, dict) and m.get("message_id") is not None:
                    ids.append(int(m["message_id"]))
            newest_id = max(ids) if ids else last_message_id

            matches_written = 0
            inserted_message_ids: list[int] = []

            # 6) LLM — только если есть что анализировать
            if checked > 0:
                chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"

                if sub_type == "events":
                    llm_json = await call_openai_subscription_match(
                        prompt=sub.prompt,
                        chat_title=chat_title,
                        messages=msgs,
                    )

                    sub_report["llm_found"] = bool(llm_json.get("found")) if isinstance(llm_json, dict) else None
                    sub_report["llm_confidence"] = llm_json.get("confidence") if isinstance(llm_json, dict) else None
                    sub_report["llm_summary_reason"] = llm_json.get("summary_reason") if isinstance(llm_json,
                                                                                                    dict) else None
                    sub_report["llm_matches_count"] = len(llm_json.get("matches") or []) if isinstance(llm_json,
                                                                                                       dict) else 0

                    sub_report["llm_json"] = llm_json

                    found = bool(llm_json.get("found"))
                    matches = llm_json.get("matches") or []

                    if found and isinstance(matches, list):
                        for m in matches:
                            mid = m.get("message_id")
                            if not mid:
                                continue

                            # ВАЖНО: message_ts должен быть datetime, не строка
                            # (у тебя уже должен быть parse_dt/parse_iso_dt — используй его)
                            src = msg_by_id.get(int(mid))

                            # timestamp: приоритет — исходное сообщение, fallback — LLM (если вдруг нужно)
                            ts = None
                            try:
                                if src and src.get("message_ts"):
                                    ts = parse_iso_ts(src.get("message_ts"))
                                else:
                                    ts = parse_iso_ts(m.get("message_ts"))
                            except Exception:
                                ts = None

                            # author: строго из исходного сообщения
                            author_id = None
                            author_display = None
                            if src:
                                author_id = src.get("author_id")
                                author_display = src.get("author_display")

                            # excerpt: можно брать из LLM (как “цитату до 300”), но если хочешь “не коверкать” — бери из src["text"]
                            excerpt = (m.get("excerpt") or "").strip()
                            if not excerpt and src:
                                excerpt = (src.get("text") or "").strip()
                            if len(excerpt) > 300:
                                excerpt = excerpt[:300].rstrip() + "…"

                            stmt = (
                                insert(MatchEvent)
                                .values(
                                    subscription_id=sub.id,
                                    message_id=int(mid),
                                    message_ts=ts,
                                    author_id=author_id,
                                    author_display=author_display,
                                    excerpt=excerpt,
                                    reason=m.get("reason"),
                                    llm_payload={},  # ты убрала payload — оставляем так
                                    notify_status="queued",
                                )
                                .on_conflict_do_nothing(constraint="uq_match_subscription_message")
                            )

                            try:
                                r = await db.execute(stmt)
                                if getattr(r, "rowcount", 0) == 1:
                                    matches_written += 1
                                    inserted_message_ids.append(int(mid))

                            except Exception as e:
                                # не валим всю подписку из-за одного матча
                                print("MATCH_INSERT_FAILED", sub.id, mid, str(e))
                                continue

                elif sub_type == "digest":
                    # заглушка на сейчас
                    sub_report["status"] = "todo"
                    sub_report["error"] = "DIGEST_NOT_IMPLEMENTED_YET"
                else:
                    sub_report["status"] = "error"
                    sub_report["error"] = f"UNKNOWN_SUBSCRIPTION_TYPE: {sub_type}"

            sub_report["inserted_message_ids"] = inserted_message_ids
            sub_report["matches_written"] = matches_written
            total_matches += matches_written

            # 7) Обновляем state
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

            # 9) Достаём из БД ровно те MatchEvent, которые реально вставили (без зависимости от времени БД)
            if inserted_message_ids:
                ev_res = await db.execute(
                    select(MatchEvent)
                    .where(
                        MatchEvent.subscription_id == sub.id,
                        MatchEvent.message_id.in_(inserted_message_ids),
                    )
                    .order_by(MatchEvent.message_id.asc())
                )
                evs = list(ev_res.scalars().all())
                sub_report["match_events"] = [_serialize_match_event(ev) for ev in evs]
            else:
                sub_report["match_events"] = []

        except Exception as e:
            sub_report["status"] = "error"
            sub_report["error"] = str(e)

            # на всякий — статус подписки тоже отметим
            try:
                await db.execute(
                    update(Subscription)
                    .where(Subscription.id == sub.id)
                    .values(status="error", last_error=str(e), updated_at=sa.func.now())
                )
                await db.commit()
            except Exception:
                pass

        results.append(sub_report)

    elapsed = round(time.perf_counter() - t0, 2)

    # DEBUG: все строки match_events (лимит), чтобы смотреть что реально в БД
    all_ev_res = await db.execute(
        select(MatchEvent).order_by(MatchEvent.created_at.desc()).limit(200)
    )
    all_evs = list(all_ev_res.scalars().all())
    debug_all_match_events = [_serialize_match_event(ev) for ev in all_evs]

    return {
        "status": "ok",
        "processed_subscriptions": len(subs),
        "checked_messages": total_checked,
        "found_matches": total_matches,
        "elapsed_seconds": elapsed,
        "ui_message": f"Проверено {total_checked} сообщений, найдено {total_matches}",
        "results": results,
        "debug_all_match_events": debug_all_match_events,
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
async def tg_send_code(payload: dict, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    owner_user_id = user.id

    phone = (payload.get("phone") or "").strip()
    if not phone:
        raise HTTPException(400, "PHONE_REQUIRED")
    try:
        await send_login_code(db, owner_user_id, phone)
    except Exception as e:
        raise HTTPException(400, f"TELEGRAM_ERROR: {e}")
    return {"status": "code_sent"}

@app.post("/tg/confirm_code")
async def tg_confirm_code(payload: dict, user: User = Depends(auth_get_current_user), db: AsyncSession = Depends(get_db)):
    owner_user_id = user.id
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
            await confirm_login(db, owner_user_id, phone, code)

            # сохранить string session в БД
            ss = await export_string_session(db, owner_user_id)
            await save_user_telegram_session(db, owner_user_id, ss)

            # получаем текущего пользователя
            me = await tg_get_current_user(db, owner_user_id)

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
async def tg_confirm_password(
    payload: dict,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        password = (payload.get("password") or "").strip()
        if not password:
            raise HTTPException(status_code=400, detail="PASSWORD_REQUIRED")

        await confirm_password(db, owner_user_id, password)

        ss = await export_string_session(db, owner_user_id)
        await save_user_telegram_session(db, owner_user_id, ss)

        me = await tg_get_current_user(db, owner_user_id)

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
        raise HTTPException(status_code=400, detail=f"TG_PASSWORD_CONFIRM_FAILED: {str(e)}")

@app.post("/tg/analyze_chat")
async def tg_analyze_chat(
    payload: dict,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    chat_link = (payload.get("chat_link") or "").strip()
    user_query = (payload.get("user_query") or "").strip()
    days = int(payload.get("days") or 7)

    me = await tg_get_current_user(db, owner_user_id)
    if not me:
        raise HTTPException(401, "TELEGRAM_NOT_AUTHORIZED")

    try:
        entity, messages = await fetch_chat_messages(db, owner_user_id, chat_link, days)
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
async def tg_list_chats(
    limit: int = 200,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    me = await tg_get_current_user(db, owner_user_id)
    if not me:
        raise HTTPException(status_code=401, detail="TELEGRAM_NOT_AUTHORIZED")

    try:
        chats = await list_user_chats(db, owner_user_id, limit=limit)
        return {"status": "ok", "count": len(chats), "chats": chats}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_CHATS_FAILED: {str(e)}")
@app.post("/tg/logout")
async def tg_logout(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        await logout_telegram(db, owner_user_id)
        return {"status": "logged_out"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_LOGOUT_FAILED: {str(e)}")

@app.post("/tg/qr/start")
async def tg_qr_start(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        data = await qr_login_start(db, owner_user_id)
        return {"status": "ok", **data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_QR_START_FAILED: {str(e)}")


async def tg_qr_status(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    owner_user_id = user.id

    try:
        data = await qr_login_status(db, owner_user_id)

        if isinstance(data, dict) and data.get("status") == "authorized":
            ss = await export_string_session(db, owner_user_id)
            await save_user_telegram_session(db, owner_user_id, ss)

        return data
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"TG_QR_STATUS_FAILED: {str(e)}")

async def bot_send_message(chat_id: int, text: str):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN_MISSING")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        })

    if resp.status_code != 200:
        raise RuntimeError(f"BOT_SEND_FAILED_HTTP_{resp.status_code}: {resp.text}")

    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"BOT_SEND_FAILED: {data}")


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

        # ожидаем: "/start <code>"
    parts = text.split(maxsplit=1)
    code = parts[1].strip() if len(parts) > 1 else None
    if not code:
        # можно ответить подсказкой
        await bot_send_message(
            telegram_chat_id,
            "Привет! Чтобы привязать бота, открой CoTel → Профиль → Подключить бота и отправь мне /start <код>."
        )
        return {"ok": True}

    code_hash = sha256_hex(code)

    # 1) найти активный код
    now = datetime.now(timezone.utc)
    res = await db.execute(
        select(BotLinkCode).where(
            BotLinkCode.code_hash == code_hash,
            BotLinkCode.used_at.is_(None),
            BotLinkCode.expires_at > now,
        )
    )
    rec = res.scalar_one_or_none()
    if not rec:
        await bot_send_message(telegram_chat_id, "Код недействителен или истёк. Сгенерируй новый в CoTel.")
        return {"ok": True}

    owner_user_id = rec.user_id

    # 2) отметить код использованным
    rec.used_at = now

    # 4) Upsert в bot_user_link по уникальному telegram_chat_id
    stmt = insert(BotUserLink).values(
        owner_user_id=owner_user_id,
        telegram_chat_id=telegram_chat_id,
        telegram_user_id=telegram_user_id,
        is_blocked=False,
    ).on_conflict_do_update(
        index_elements=["telegram_chat_id"],
        set_={
            "owner_user_id": owner_user_id,
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
async def tg_bot_link_status(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    q = (
        select(sa.func.count())
        .select_from(BotUserLink)
        .where(
            BotUserLink.owner_user_id == user.id,
            BotUserLink.is_blocked == False,  # noqa: E712
        )
    )
    count = (await db.execute(q)).scalar_one()
    return {"connected": count > 0}
@app.post("/subscriptions", response_model=SubscriptionOut)
async def create_subscription(
    payload: SubscriptionCreate,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    sub = Subscription(
        owner_user_id=user.id,     # <-- ВАЖНО
        name=payload.name,
        source_mode=payload.source_mode,
        chat_ref=payload.chat_ref,
        chat_id=None,
        frequency_minutes=payload.frequency_minutes,
        prompt=payload.prompt,
        subscription_type=payload.subscription_type,
        is_active=payload.is_active,
        status="active" if payload.is_active else "paused",
        last_error=None,
    )
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

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
async def list_subscriptions(
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(
        select(Subscription)
        .where(Subscription.owner_user_id == user.id)
        .order_by(Subscription.id.desc())
    )
    return res.scalars().all()

@app.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
async def get_subscription(
    subscription_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    return sub

@app.put("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
async def update_subscription(
    subscription_id: int,
    payload: SubscriptionCreate,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    sub.name = payload.name
    sub.source_mode = payload.source_mode
    sub.subscription_type = payload.subscription_type
    sub.chat_ref = payload.chat_ref
    sub.frequency_minutes = payload.frequency_minutes
    sub.prompt = payload.prompt
    sub.is_active = payload.is_active
    sub.status = "active" if payload.is_active else "paused"
    sub.last_error = None
    sub.updated_at = sa.func.now()

    await db.commit()
    await db.refresh(sub)
    return sub

@app.post("/subscriptions/{subscription_id}/toggle", response_model=SubscriptionOut)
async def toggle_subscription(
    subscription_id: int,
    payload: ToggleRequest,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    sub.is_active = payload.is_active
    sub.status = "active" if payload.is_active else "paused"
    sub.last_error = None
    sub.updated_at = sa.func.now()

    await db.commit()
    await db.refresh(sub)
    return sub

@app.delete("/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: int,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
    sub = res.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="SUBSCRIPTION_NOT_FOUND")
    if sub.owner_user_id != user.id:
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    await db.execute(delete(MatchEvent).where(MatchEvent.subscription_id == subscription_id))
    await db.execute(delete(DigestEvent).where(DigestEvent.subscription_id == subscription_id))
    await db.execute(delete(SubscriptionState).where(SubscriptionState.subscription_id == subscription_id))
    await db.execute(delete(Subscription).where(Subscription.id == subscription_id))

    await db.commit()
    return {"status": "ok", "deleted_subscription_id": subscription_id}
@app.post("/tg/bot/dispatch")
async def tg_bot_dispatch(db: AsyncSession = Depends(get_db)):
    t0 = time.perf_counter()

    # 1) куда слать (MVP: первый живой линк)
    r = await db.execute(
        select(BotUserLink).where(BotUserLink.is_blocked == False).order_by(BotUserLink.id.desc())
    )
    link = r.scalars().first()
    if not link:
        return {"status": "error", "error": "NO_BOT_USER_LINK"}

    dest_chat_id = link.telegram_chat_id

    # 2) queued события + сразу подтягиваем Subscription, чтобы знать name/chat_ref/chat_id
    r2 = await db.execute(
        select(MatchEvent, Subscription)
        .join(Subscription, Subscription.id == MatchEvent.subscription_id)
        .where(MatchEvent.notify_status == "queued")
        .order_by(MatchEvent.subscription_id.asc(), MatchEvent.id.asc())
        .limit(200)
    )

    rows = list(r2.all())  # [(MatchEvent, Subscription), ...]

    if not rows:
        elapsed = round(time.perf_counter() - t0, 2)
        return {"status": "ok", "events_total": 0, "sent_groups": 0, "failed_groups": 0, "elapsed_seconds": elapsed}

    # 3) группировка по subscription_id
    grouped: dict[int, dict] = {}
    for ev, sub in rows:
        sid = int(ev.subscription_id)
        if sid not in grouped:
            grouped[sid] = {"sub": sub, "events": []}
        grouped[sid]["events"].append(ev)

    sent_groups = 0
    failed_groups = 0
    events_total = len(rows)

    # 4) одна отправка на подписку
    for sid, pack in grouped.items():
        sub: Subscription = pack["sub"]
        events: list[MatchEvent] = pack["events"]

        try:
            # ограничим, чтобы не упереться в лимит Telegram (4096)
            # покажем первые 10, остальное свернём
            max_items = 10
            shown = events[:max_items]
            rest = len(events) - len(shown)

            header = f"Найдены события по подписке: {sub.name or f'#{sid}'}\n" \
                     f"Совпадений: {len(events)}\n"

            lines = []
            for i, ev in enumerate(shown, start=1):
                author = ev.author_display or (str(ev.author_id) if ev.author_id else "—")
                ts = ev.message_ts.isoformat() if ev.message_ts else "—"

                excerpt = (ev.excerpt or "").strip()
                if len(excerpt) > 300:
                    excerpt = excerpt[:300].rstrip() + "…"

                url = build_tg_message_link(
                    chat_ref=getattr(sub, "chat_ref", None),
                    chat_id=getattr(sub, "chat_id", None),
                    message_id=int(ev.message_id),
                )
                link_text = f"\n{url}" if url else ""

                lines.append(
                    f"\n{i}) {author} • {ts}\n"
                    f"{excerpt or '—'}"
                    f"{link_text}"
                )

            if rest > 0:
                lines.append(f"\n\n…и ещё {rest} совпадений (свернуто для компактности).")

            text = header + "".join(lines)

            await bot_send_message(dest_chat_id, text)  # sendMessage один раз

            # помечаем все события группы как sent
            for ev in events:
                ev.notify_status = "sent"
                db.add(ev)

            sent_groups += 1

        except Exception as e:
            for ev in events:
                ev.notify_status = "failed"
                db.add(ev)

            failed_groups += 1
            print("DISPATCH_GROUP_FAILED", sid, str(e))

    await db.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    return {
        "status": "ok",
        "events_total": events_total,
        "groups_total": len(grouped),
        "sent_groups": sent_groups,
        "failed_groups": failed_groups,
        "elapsed_seconds": elapsed,
    }
