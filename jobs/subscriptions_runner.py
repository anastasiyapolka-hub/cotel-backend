# jobs/subscriptions_runner.py
import asyncio
import sys
from datetime import datetime, timezone, timedelta

import sqlalchemy as sa
from sqlalchemy import select, update, or_
from sqlalchemy.exc import SQLAlchemyError

from db.session import AsyncSessionLocal  # см. db/session.py :contentReference[oaicite:4]{index=4}
from db.models import Subscription, SubscriptionState, MatchEvent, DigestEvent
import os
from main import (
    fetch_chat_messages_for_subscription,
    call_openai_subscription_match,
    call_openai_subscription_digest,
)

from main import call_openai_subscription_match  # оставить можно, но лучше тоже вынести (не обязательно сейчас)
from main import parse_iso_ts
from telegram_service import fetch_chat_messages_for_subscription, disconnect_tg_client

DEV_OWNER_USER_ID = int(os.getenv("DEV_OWNER_USER_ID", "1"))
BATCH_SIZE = 20
EVENTS_READ_LIMIT = 1000  # как ты утвердила ранее для events
LEASE_MINUTES = 5      # сколько держим "замок" на время обработки
RETRY_MINUTES = 2      # через сколько повторять при ошибке


def _is_due(last_success_at, freq_min: int, now_utc: datetime) -> bool:
    if last_success_at is None:
        return True
    return (now_utc - last_success_at) >= timedelta(minutes=freq_min)

async def _reserve_due_subscriptions(db, now_utc: datetime) -> list[int]:
    """
    Короткая транзакция:
    - выбираем due подписки по next_run_at
    - лочим строки subscription_state FOR UPDATE SKIP LOCKED
    - резервируем: last_checked_at=now, next_run_at=now+freq
    - коммит
    """
    async with db.begin():
        q = (
            select(SubscriptionState, Subscription)
            .join(Subscription, Subscription.id == SubscriptionState.subscription_id)
            .where(Subscription.is_active == True)  # noqa: E712
            .where(
                or_(
                    SubscriptionState.next_run_at.is_(None),
                    SubscriptionState.next_run_at <= now_utc,
                )
            )
            # важно: блокируем state-строки
            .with_for_update(skip_locked=True)
            # чтобы сначала брались самые "просроченные"
            .order_by(
                SubscriptionState.next_run_at.asc().nullsfirst(),
                SubscriptionState.subscription_id.asc(),
            )

            .limit(BATCH_SIZE)
        )

        rows = (await db.execute(q)).all()
        if not rows:
            return []

        due_ids: list[int] = []
        for st, sub in rows:
            freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)

            # Резервирование: сразу двигаем next_run_at вперёд, чтобы другие инстансы не взяли
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=LEASE_MINUTES)

            due_ids.append(int(sub.id))

        return due_ids

async def _process_one_subscription(db, sub_id: int, now_utc: datetime) -> None:
    # Берём sub + state
    sub = (await db.execute(select(Subscription).where(Subscription.id == sub_id))).scalar_one()
    st = (await db.execute(select(SubscriptionState).where(SubscriptionState.subscription_id == sub_id))).scalar_one_or_none()

    last_message_id = getattr(st, "last_message_id", None) if st else None
    freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)

    sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
    if sub_type == "summary":
        sub_type = "digest"

    # Сейчас реализуем реально только events, summary/digest — заглушка (как ты просила)
    if sub_type == "digest":
        # окно чтения: для digest можно тоже использовать cursor-first, но обычно логичнее "окно времени"
        since_dt = now_utc - timedelta(minutes=freq_min)
        min_id = None  # digest читаем окном, а не курсором

        owner_user_id = int(getattr(sub, "owner_user_id", None) or DEV_OWNER_USER_ID)

        entity, msgs = await fetch_chat_messages_for_subscription(
            db=db,
            owner_user_id=owner_user_id,
            chat_link=sub.chat_ref,
            since_dt=since_dt,
            min_id=min_id,
            limit=EVENTS_READ_LIMIT,  # у тебя 1000
        )

        # обновим chat_id если нужно
        async with db.begin():
            if getattr(sub, "chat_id", None) is None:
                ent_id = getattr(entity, "id", None)
                if ent_id is not None:
                    sub.chat_id = int(ent_id)

        if not msgs:
            async with db.begin():
                if st is None:
                    st = SubscriptionState(subscription_id=sub.id)
                    db.add(st)
                st.last_success_at = now_utc
                st.last_checked_at = now_utc
                st.next_run_at = now_utc + timedelta(minutes=freq_min)
            return

        # end_message_id — самый новый id в окне
        ids = [int(m["message_id"]) for m in msgs if isinstance(m, dict) and m.get("message_id") is not None]
        newest_id = max(ids) if ids else None
        oldest_id = min(ids) if ids else None

        chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"

        llm_json = await call_openai_subscription_digest(
            prompt=sub.prompt,  # ВАЖНО: prompt пользователя обязателен, как ты и хотела
            chat_title=chat_title,
            messages=msgs,
        )

        digest_text = ""
        confidence = None
        if isinstance(llm_json, dict):
            digest_text = (llm_json.get("digest_text") or "").strip()
            confidence = llm_json.get("confidence")

        # защита по длине
        if len(digest_text) > 4096:
            digest_text = digest_text[:4096].rstrip() + "…"

        async with db.begin():
            # пишем одну строку digest_events
            ev = DigestEvent(
                subscription_id=sub.id,
                # рекомендую хранить границы окна:
                window_start_at=since_dt,
                window_end_at=now_utc,
                start_message_id=int(oldest_id) if oldest_id else None,
                end_message_id=int(newest_id) if newest_id else None,
                digest_text=digest_text,
                llm_payload={"confidence": confidence} if confidence is not None else None,
                notify_status="queued",
            )
            db.add(ev)

            if st is None:
                st = SubscriptionState(subscription_id=sub.id)
                db.add(st)

            # для digest можно обновлять last_message_id как "newest_id" — чтобы в будущем перейти на cursor-mode
            if newest_id:
                st.last_message_id = int(newest_id)
            st.last_success_at = now_utc
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=freq_min)

        return

    # cursor-first (как у тебя уже сделано в /subscriptions/run) :contentReference[oaicite:5]{index=5}
    if last_message_id:
        since_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
        min_id = int(last_message_id)
    else:
        since_dt = now_utc - timedelta(minutes=freq_min)
        min_id = None

    # 1) Читаем TG
    owner_user_id = sub.owner_user_id or DEV_OWNER_USER_ID  # временно, пока один юзер
    entity, msgs = await fetch_chat_messages_for_subscription(
        db=db,
        owner_user_id=owner_user_id,
        chat_link=sub.chat_ref,
        since_dt=since_dt,
        min_id=min_id,
        limit=EVENTS_READ_LIMIT,
    )

    # 2) Обновим chat_id при необходимост и (как в /subscriptions/run) :contentReference[oaicite:6]{index=6}
    if getattr(sub, "chat_id", None) is None:
        ent_id = getattr(entity, "id", None)
        if ent_id is not None:
            sub.chat_id = int(ent_id)

    if not msgs:
        if st:
            st.last_success_at = now_utc
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=freq_min)
        return

    msg_by_id = {}
    for m in msgs:
        try:
            mid0 = m.get("message_id")
            if mid0 is not None:
                msg_by_id[int(mid0)] = m
        except Exception:
            continue

    # newest_id
    ids = [int(m["message_id"]) for m in msgs if isinstance(m, dict) and m.get("message_id") is not None]
    newest_id = max(ids) if ids else last_message_id

    # 3) LLM
    chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"
    llm_json = await call_openai_subscription_match(
        prompt=sub.prompt,
        chat_title=chat_title,
        messages=msgs,
    )

    found = bool(llm_json.get("found")) if isinstance(llm_json, dict) else False
    matches = (llm_json.get("matches") or []) if isinstance(llm_json, dict) else []

    # 4) Пишем match_events + обновляем state
    # upsert-ish дедуп у тебя обеспечивается unique(subscription_id, message_id) на уровне БД
    # (вставки, которые нарушают unique, должны отлавливаться в твоей вставке; если сейчас ловишь IntegrityError — ок)
    for item in matches:
        mid = item.get("message_id")
        if mid is None:
            continue

        # источник истины — исходное сообщение из Telegram
        src = msg_by_id.get(int(mid))

        # 2) author: строго из Telegram msgs (LLM не доверяем)
        author_id = src.get("author_id") if src else None
        author_display = src.get("author_display") if src else None

        # 3) excerpt: лучше из Telegram текста (чтобы LLM “не коверкала”)
        excerpt = ""
        if src:
            excerpt = (src.get("text") or "").strip()
        if not excerpt:
            excerpt = (item.get("excerpt") or "").strip()

        if len(excerpt) > 300:
            excerpt = excerpt[:300].rstrip() + "…"

        ts = None
        try:
            if src and src.get("message_ts"):
                ts = parse_iso_ts(src.get("message_ts"))
            else:
                ts = parse_iso_ts(item.get("message_ts"))
        except Exception:
            ts = None

        ev = MatchEvent(
            subscription_id=sub.id,
            message_id=int(mid),
            message_ts=ts,  # <-- datetime или None
            author_id=author_id,
            author_display=author_display,
            excerpt=excerpt,
            reason=item.get("reason"),
            notify_status="queued",
            llm_payload=None,
        )
        db.add(ev)

    # state
    if st is None:
        st = SubscriptionState(subscription_id=sub.id)
        db.add(st)

    st.last_message_id = int(newest_id) if newest_id else st.last_message_id
    st.last_success_at = now_utc
    st.last_checked_at = now_utc
    st.next_run_at = now_utc + timedelta(minutes=freq_min)


async def run_tick() -> int:
    now_utc = datetime.now(timezone.utc)
    exit_code = 0

    try:
        # 1) Резервируем due подписки (короткая транзакция внутри _reserve_due_subscriptions)
        async with AsyncSessionLocal() as db:
            try:
                due_ids = await _reserve_due_subscriptions(db, now_utc)
            except SQLAlchemyError as e:
                print(f"[subscriptions_runner] RESERVE_FAILED: {e}")
                return 2  # системная ошибка

        if not due_ids:
            print("[subscriptions_runner] No due subscriptions")
            return 0

        # 2) Обрабатываем подписки по одной: commit/rollback ТОЛЬКО ЗДЕСЬ
        async with AsyncSessionLocal() as db:
            for sub_id in due_ids:
                try:
                    # ВАЖНО: _process_one_subscription НЕ должен открывать db.begin()
                    await _process_one_subscription(db, sub_id, now_utc)

                    # Успех: фиксируем все изменения одним коммитом
                    await db.commit()
                    print(f"[subscriptions_runner] OK sub_id={sub_id}")

                except Exception as e:
                    print(f"[subscriptions_runner] FAILED sub_id={sub_id} err={e}")
                    exit_code = 1

                    # ВАЖНО: сбрасываем текущую транзакцию, чтобы сессия была пригодна дальше
                    try:
                        await db.rollback()
                    except Exception as rb_e:
                        print(f"[subscriptions_runner] ROLLBACK_FAILED sub_id={sub_id} err={rb_e}")

                    # Быстрый ретрай: выставляем next_run_at в ОТДЕЛЬНОЙ сессии/транзакции
                    try:
                        async with AsyncSessionLocal() as db2:
                            async with db2.begin():
                                st = (
                                    await db2.execute(
                                        select(SubscriptionState).where(
                                            SubscriptionState.subscription_id == sub_id
                                        )
                                    )
                                ).scalar_one_or_none()
                                if st:
                                    st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
                    except Exception as e2:
                        print(f"[subscriptions_runner] FAILED to schedule retry sub_id={sub_id} err={e2}")

        return exit_code

    finally:
        # 3) Всегда закрываем Telethon соединение
        try:
            await disconnect_tg_client()
        except Exception as e:
            print(f"[subscriptions_runner] disconnect_tg_client FAILED: {e}")


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
