# jobs/subscriptions_runner.py
import asyncio
import sys
from datetime import datetime, timezone, timedelta

import sqlalchemy as sa
from sqlalchemy import select, update, or_
from sqlalchemy.exc import SQLAlchemyError

from db.session import AsyncSessionLocal  # см. db/session.py :contentReference[oaicite:4]{index=4}
from db.models import Subscription, SubscriptionState, MatchEvent  # предполагаю стандартный импорт моделей
from main import (
    fetch_chat_messages_for_subscription,
    call_openai_subscription_match,
)


BATCH_SIZE = 20
EVENTS_READ_LIMIT = 1000  # как ты утвердила ранее для events


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
            .order_by(SubscriptionState.next_run_at.asc().nullsfirst(), SubscriptionState.id.asc())
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
            st.next_run_at = now_utc + timedelta(minutes=freq_min)

            due_ids.append(int(sub.id))

        return due_ids

async def _process_one_subscription(db, sub_id: int, now_utc: datetime) -> None:
    # Берём sub + state
    sub = (await db.execute(select(Subscription).where(Subscription.id == sub_id))).scalar_one()
    st = (await db.execute(select(SubscriptionState).where(SubscriptionState.subscription_id == sub_id))).scalar_one_or_none()

    last_message_id = getattr(st, "last_message_id", None) if st else None
    freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)

    sub_type = (getattr(sub, "subscription_type", None) or "events").lower()

    # Сейчас реализуем реально только events, summary/digest — заглушка (как ты просила)
    if sub_type != "events":
        # Заглушка: помечаем success, чтобы не крутить пустое
        async with db.begin():
            if st:
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
    entity, msgs = await fetch_chat_messages_for_subscription(
        chat_link=sub.chat_ref,
        since_dt=since_dt,
        min_id=min_id,
        limit=EVENTS_READ_LIMIT,
    )

    # 2) Обновим chat_id при необходимост и (как в /subscriptions/run) :contentReference[oaicite:6]{index=6}
    async with db.begin():
        if getattr(sub, "chat_id", None) is None:
            ent_id = getattr(entity, "id", None)
            if ent_id is not None:
                sub.chat_id = int(ent_id)

    if not msgs:
        async with db.begin():
            if st:
                st.last_success_at = now_utc
                st.last_checked_at = now_utc
                st.next_run_at = now_utc + timedelta(minutes=freq_min)
        return

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
    async with db.begin():
        # upsert-ish дедуп у тебя обеспечивается unique(subscription_id, message_id) на уровне БД
        # (вставки, которые нарушают unique, должны отлавливаться в твоей вставке; если сейчас ловишь IntegrityError — ок)
        for item in matches:
            mid = item.get("message_id")
            if mid is None:
                continue

            ev = MatchEvent(
                subscription_id=sub.id,
                message_id=int(mid),
                message_ts=item.get("message_ts"),
                author_id=item.get("author_id"),
                author_display=item.get("author_display"),
                excerpt=item.get("excerpt"),
                reason=item.get("reason"),
                notify_status="queued",
                llm_payload=None,  # если ты решила не сохранять payload
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

    async with AsyncSessionLocal() as db:
        try:
            due_ids = await _reserve_due_subscriptions(db, now_utc)
        except SQLAlchemyError as e:
            print(f"[subscriptions_runner] RESERVE_FAILED: {e}")
            return 2

    if not due_ids:
        print("[subscriptions_runner] No due subscriptions")
        return 0

    # Обрабатываем по одной, но ошибки не валят весь тик
    async with AsyncSessionLocal() as db:
        for sub_id in due_ids:
            try:
                await _process_one_subscription(db, sub_id, now_utc)
                print(f"[subscriptions_runner] OK sub_id={sub_id}")
            except Exception as e:
                # Важно: не валим весь runner
                print(f"[subscriptions_runner] FAILED sub_id={sub_id} err={e}")

    return 0


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
