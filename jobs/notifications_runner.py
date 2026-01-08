# jobs/notifications_runner.py
import asyncio
import os
import time
import re
from datetime import datetime, timezone

import httpx
import sqlalchemy as sa
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased

from db.session import AsyncSessionLocal
from db.models import MatchEvent, Subscription, BotUserLink

# ====== CONFIG ======
BATCH_SIZE = 200
MAX_ITEMS_PER_SUB = 10          # сколько пунктов показываем в одном уведомлении
MAX_EXCERPT_LEN = 300
TELEGRAM_TEXT_LIMIT = 4096

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEV_OWNER_USER_ID = int(os.getenv("DEV_OWNER_USER_ID", "1"))

# Статусы notify_status
STATUS_QUEUED = "queued"
STATUS_SENDING = "sending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[:n].rstrip() + "…"


def build_tg_message_link(chat_ref: str | None, chat_id: int | None, message_id: int | None) -> str | None:
    """
    Повторяем логику из main.py, чтобы runner не импортировал main.py.
    """
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
        # invite-ссылка t.me/+HASH — не подходит для прямых /{message_id}
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


async def bot_send_message(chat_id: int, text: str) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN_MISSING")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json=payload)

    if resp.status_code != 200:
        raise RuntimeError(f"BOT_SEND_FAILED_HTTP_{resp.status_code}: {resp.text}")

    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"BOT_SEND_FAILED: {data}")


async def _reserve_queued_events(db, now_utc: datetime) -> list[int]:
    """
    Короткая транзакция:
    - берём самые старые queued события (events-подписки),
    - лочим их FOR UPDATE SKIP LOCKED,
    - помечаем как sending,
    - commit,
    - возвращаем ids событий для дальнейшей обработки.
    """
    async with db.begin():
        # IMPORTANT:
        # Берём сначала самые старые: MatchEvent.id ASC (или created_at ASC, если уверена что индекс есть).
        q = (
            select(MatchEvent.id)
            .select_from(MatchEvent)
            .join(Subscription, Subscription.id == MatchEvent.subscription_id)
            .where(MatchEvent.notify_status == STATUS_QUEUED)
            .where(Subscription.subscription_type == "events")   # сейчас реализуем только events
            # опционально: можно фильтровать по active
            # .where(Subscription.is_active == True)
            .order_by(MatchEvent.id.asc())
            .with_for_update(skip_locked=True)
            .limit(BATCH_SIZE)
        )

        ids = [row[0] for row in (await db.execute(q)).all()]
        if not ids:
            return []

        # Резервирование: queued -> sending
        await db.execute(
            update(MatchEvent)
            .where(MatchEvent.id.in_(ids))
            .values(notify_status=STATUS_SENDING)
        )

        return ids


async def _load_events_with_subscriptions(db, event_ids: list[int]):
    """
    Достаём зарезервированные события вместе с подпиской.
    """
    q = (
        select(MatchEvent, Subscription)
        .join(Subscription, Subscription.id == MatchEvent.subscription_id)
        .where(MatchEvent.id.in_(event_ids))
        .order_by(Subscription.owner_user_id.asc().nullsfirst(),
                  MatchEvent.subscription_id.asc(),
                  MatchEvent.id.asc())
    )
    return list((await db.execute(q)).all())


async def _get_dest_chat_id(db, owner_user_id: int) -> int | None:
    """
    Куда слать (BotUserLink) — по owner_user_id.
    На MVP owner_user_id берём из подписки, fallback на DEV_OWNER_USER_ID.
    """
    q = (
        select(BotUserLink)
        .where(BotUserLink.is_blocked == False)  # noqa: E712
        .where(BotUserLink.owner_user_id == owner_user_id)
        .order_by(BotUserLink.id.desc())
        .limit(1)
    )
    link = (await db.execute(q)).scalars().first()
    if not link:
        return None
    return int(link.telegram_chat_id)


def _format_events_message(sub: Subscription, events: list[MatchEvent]) -> str:
    """
    Формируем 1 сообщение на подписку (с ограничением по длине).
    """
    sid = int(sub.id)
    title = sub.name or f"#{sid}"
    header = (
        f"Найдены события по подписке: {title}\n"
        f"Совпадений: {len(events)}\n"
    )

    shown = events[:MAX_ITEMS_PER_SUB]
    rest = len(events) - len(shown)

    lines = []
    for i, ev in enumerate(shown, start=1):
        author = ev.author_display or (str(ev.author_id) if ev.author_id else "—")
        ts = ev.message_ts.isoformat() if ev.message_ts else "—"

        excerpt = _truncate(ev.excerpt or "", MAX_EXCERPT_LEN)
        if not excerpt:
            excerpt = "—"

        url = build_tg_message_link(
            chat_ref=getattr(sub, "chat_ref", None),
            chat_id=getattr(sub, "chat_id", None),
            message_id=int(ev.message_id) if ev.message_id else None,
        )
        link_text = f"\n{url}" if url else ""

        lines.append(
            f"\n{i}) {author} • {ts}\n"
            f"{excerpt}"
            f"{link_text}"
        )

    if rest > 0:
        lines.append(f"\n\n…и ещё {rest} совпадений (свернуто для компактности).")

    text = header + "".join(lines)

    # Telegram лимит 4096. Если внезапно перелетели — режем хвост.
    if len(text) > TELEGRAM_TEXT_LIMIT:
        text = text[: (TELEGRAM_TEXT_LIMIT - 1)] + "…"

    return text


async def run_tick() -> int:
    now_utc = _utc_now()
    exit_code = 0

    # 1) Reserve
    async with AsyncSessionLocal() as db:
        try:
            event_ids = await _reserve_queued_events(db, now_utc)
        except SQLAlchemyError as e:
            print(f"[notifications_runner] RESERVE_FAILED: {e}")
            return 2

    if not event_ids:
        print("[notifications_runner] No queued events")
        return 0

    # 2) Load reserved and send
    async with AsyncSessionLocal() as db:
        rows = await _load_events_with_subscriptions(db, event_ids)

        # Группировка: (owner_user_id, subscription_id) -> {sub, events[]}
        grouped: dict[tuple[int, int], dict] = {}

        for ev, sub in rows:
            owner_user_id = int(getattr(sub, "owner_user_id", None) or DEV_OWNER_USER_ID)
            sid = int(ev.subscription_id)
            key = (owner_user_id, sid)

            if key not in grouped:
                grouped[key] = {"sub": sub, "events": []}
            grouped[key]["events"].append(ev)

        sent_groups = 0
        failed_groups = 0

        for (owner_user_id, sid), pack in grouped.items():
            sub: Subscription = pack["sub"]
            events: list[MatchEvent] = pack["events"]

            # В будущем сюда добавишь:
            # if sub.subscription_type == "digest": ... (таблица digest_events)
            # сейчас реализуем только events -> match_events
            sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
            if sub_type != "events":
                # Неизвестный тип: помечаем failed и идём дальше
                async with db.begin():
                    await db.execute(
                        update(MatchEvent)
                        .where(MatchEvent.id.in_([int(e.id) for e in events]))
                        .values(notify_status=STATUS_FAILED)
                    )
                failed_groups += 1
                exit_code = 1
                continue

            try:
                dest_chat_id = await _get_dest_chat_id(db, owner_user_id)
                if not dest_chat_id:
                    raise RuntimeError(f"NO_BOT_USER_LINK owner_user_id={owner_user_id}")

                text = _format_events_message(sub, events)
                await bot_send_message(dest_chat_id, text)

                # success -> sent
                async with db.begin():
                    await db.execute(
                        update(MatchEvent)
                        .where(MatchEvent.id.in_([int(e.id) for e in events]))
                        .values(notify_status=STATUS_SENT)
                    )

                sent_groups += 1
                print(f"[notifications_runner] SENT owner_user_id={owner_user_id} sub_id={sid} events={len(events)}")

            except Exception as e:
                # fail -> failed
                async with db.begin():
                    await db.execute(
                        update(MatchEvent)
                        .where(MatchEvent.id.in_([int(e.id) for e in events]))
                        .values(notify_status=STATUS_FAILED)
                    )

                failed_groups += 1
                exit_code = 1
                print(f"[notifications_runner] FAILED owner_user_id={owner_user_id} sub_id={sid} err={e}")

        print(f"[notifications_runner] done groups_total={len(grouped)} sent_groups={sent_groups} failed_groups={failed_groups}")

    return exit_code


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
