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
    parse_iso_ts,
    call_openai_subscription_match,
    call_openai_subscription_digest,
)

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

from sqlalchemy.dialects.postgresql import insert

async def _process_one_subscription(db, sub_id: int, now_utc: datetime) -> None:
    # Берём sub + state
    sub = (await db.execute(select(Subscription).where(Subscription.id == sub_id))).scalar_one()
    st = (
        await db.execute(
            select(SubscriptionState).where(SubscriptionState.subscription_id == sub_id)
        )
    ).scalar_one_or_none()

    last_message_id = getattr(st, "last_message_id", None) if st else None
    freq_min = int(getattr(sub, "frequency_minutes", 60) or 60)

    sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
    # если у тебя на фронте summary, а в коде считаешь digest — оставим твой маппинг
    if sub_type == "summary":
        sub_type = "digest"

    owner_user_id = int(getattr(sub, "owner_user_id", None) or DEV_OWNER_USER_ID)

    # -------------------------
    # DIGEST / SUMMARY
    # -------------------------
    if sub_type == "digest":
        # digest читаем окном времени
        since_dt = now_utc - timedelta(minutes=freq_min)
        min_id = None

        entity, msgs = await fetch_chat_messages_for_subscription(
            db=db,
            owner_user_id=owner_user_id,
            chat_link=sub.chat_ref,
            since_dt=since_dt,
            min_id=min_id,
            limit=EVENTS_READ_LIMIT,  # 1000
        )

        # chat_id — просто обновим объект (без begin)
        if getattr(sub, "chat_id", None) is None:
            ent_id = getattr(entity, "id", None)
            if ent_id is not None:
                sub.chat_id = int(ent_id)

        # state гарантируем
        if st is None:
            st = SubscriptionState(subscription_id=sub.id)
            db.add(st)

        if not msgs:
            # ничего не нашли — просто двигаем state
            st.last_success_at = now_utc
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=freq_min)
            return

        ids = [
            int(m["message_id"])
            for m in msgs
            if isinstance(m, dict) and m.get("message_id") is not None
        ]
        newest_id = max(ids) if ids else None
        oldest_id = min(ids) if ids else None

        chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"

        llm_json = await call_openai_subscription_digest(
            prompt=sub.prompt,  # prompt пользователя обязателен
            chat_title=chat_title,
            messages=msgs,
        )

        digest_text = ""
        confidence = None
        if isinstance(llm_json, dict):
            digest_text = (llm_json.get("digest_text") or "").strip()
            confidence = llm_json.get("confidence")

        if len(digest_text) > 4096:
            digest_text = digest_text[:4096].rstrip() + "…"

        # ВАЖНО: вставка digest_event с ON CONFLICT DO NOTHING
        # uq_digest_subscription_endmsg: (subscription_id, end_message_id)
        stmt = (
            insert(DigestEvent)
            .values(
                subscription_id=sub.id,
                window_start=since_dt,
                window_end=now_utc,
                start_message_id=int(oldest_id) if oldest_id else None,
                end_message_id=int(newest_id) if newest_id else None,
                messages_seen=len(msgs),
                digest_text=digest_text,
                llm_payload={"confidence": confidence} if confidence is not None else None,
                notify_status="queued",
            )
            .on_conflict_do_nothing(constraint="uq_digest_subscription_endmsg")
        )
        await db.execute(stmt)

        # state
        if newest_id:
            st.last_message_id = int(newest_id)
        st.last_success_at = now_utc
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=freq_min)

        return

    # -------------------------
    # EVENTS
    # -------------------------
    if last_message_id:
        since_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
        min_id = int(last_message_id)
    else:
        since_dt = now_utc - timedelta(minutes=freq_min)
        min_id = None

    entity, msgs = await fetch_chat_messages_for_subscription(
        db=db,
        owner_user_id=owner_user_id,
        chat_link=sub.chat_ref,
        since_dt=since_dt,
        min_id=min_id,
        limit=EVENTS_READ_LIMIT,
    )

    if getattr(sub, "chat_id", None) is None:
        ent_id = getattr(entity, "id", None)
        if ent_id is not None:
            sub.chat_id = int(ent_id)

    if st is None:
        st = SubscriptionState(subscription_id=sub.id)
        db.add(st)

    if not msgs:
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

    ids = [
        int(m["message_id"])
        for m in msgs
        if isinstance(m, dict) and m.get("message_id") is not None
    ]
    newest_id = max(ids) if ids else last_message_id

    llm_json = await call_openai_subscription_match(
        prompt=sub.prompt,
        chat_title=getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat",
        messages=msgs,
    )

    matches = (llm_json.get("matches") or []) if isinstance(llm_json, dict) else []

    for item in matches:
        mid = item.get("message_id")
        if mid is None:
            continue

        src = msg_by_id.get(int(mid))
        author_id = src.get("author_id") if src else None
        author_display = src.get("author_display") if src else None

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

        db.add(
            MatchEvent(
                subscription_id=sub.id,
                message_id=int(mid),
                message_ts=ts,
                author_id=author_id,
                author_display=author_display,
                excerpt=excerpt,
                reason=item.get("reason"),
                notify_status="queued",
                llm_payload=None,
            )
        )

    st.last_message_id = int(newest_id) if newest_id else st.last_message_id
    st.last_success_at = now_utc
    st.last_checked_at = now_utc
    st.next_run_at = now_utc + timedelta(minutes=freq_min)

async def run_tick() -> int:
    now_utc = datetime.now(timezone.utc)
    exit_code = 0

    try:
        # 1) Reserve (короткая сессия)
        async with AsyncSessionLocal() as db:
            try:
                due_ids = await _reserve_due_subscriptions(db, now_utc)
            except SQLAlchemyError as e:
                print(f"[subscriptions_runner] RESERVE_FAILED: {e}")
                return 2

        if not due_ids:
            print("[subscriptions_runner] No due subscriptions")
            return 0

        # 2) Process each subscription in one shared db session,
        # commit/rollback only here (НЕ внутри _process_one_subscription)
        async with AsyncSessionLocal() as db:
            for sub_id in due_ids:
                try:
                    await _process_one_subscription(db, sub_id, now_utc)
                    await db.commit()
                    print(f"[subscriptions_runner] OK sub_id={sub_id}")

                except Exception as e:
                    print(f"[subscriptions_runner] FAILED sub_id={sub_id} err={e}")
                    exit_code = 1

                    # сбрасываем транзакцию
                    try:
                        await db.rollback()
                    except Exception as rb_e:
                        print(f"[subscriptions_runner] ROLLBACK_FAILED sub_id={sub_id} err={rb_e}")

                    # ретрай — в отдельной сессии
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
        try:
            await disconnect_tg_client()
        except Exception as e:
            print(f"[subscriptions_runner] disconnect_tg_client FAILED: {e}")


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
