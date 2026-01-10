# jobs/notifications_runner.py
import asyncio
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from db.session import AsyncSessionLocal
from db.models import Subscription, MatchEvent, DigestEvent, BotUserLink

# используем уже существующие функции из main.py
from main import build_tg_message_link, bot_send_message


# -----------------------------
# Config / constants
# -----------------------------
DEV_OWNER_USER_ID = int(os.getenv("DEV_OWNER_USER_ID", "1"))

BATCH_LIMIT = 200
DETAIL_TEXT_LIMIT = 3800  # сколько максимум тратим на "подробную" часть; остальное оставляем под ссылки/хвост

STATUS_QUEUED = "queued"
STATUS_SENDING = "sending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"

# Для Telegram ограничение ~4096 символов, но в твоей задаче — digest и events уже форматируются отдельно.
# Здесь можно оставить запас, но это не обязательно для digest.
TG_MSG_HARD_LIMIT = 4096


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# -----------------------------
# Shared helpers
# -----------------------------
async def _get_dest_chat_id(db, owner_user_id: int) -> int | None:
    """
    Берём последний активный BotUserLink для owner_user_id.
    Предполагаем (как ты сказала), что owner_user_id в bot_user_link заполнен.
    """
    q = (
        select(BotUserLink.telegram_chat_id)
        .where(
            BotUserLink.owner_user_id == owner_user_id,
            BotUserLink.is_blocked == False,  # noqa: E712
        )
        .order_by(BotUserLink.id.desc())
        .limit(1)
    )
    return (await db.execute(q)).scalar_one_or_none()

RU_MONTHS = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]
RU_WEEKDAYS = [
    "понедельник", "вторник", "среда", "четверг",
    "пятница", "суббота", "воскресенье",
]

def _tz_gmt_label(tz: ZoneInfo, dt_utc: datetime) -> str:
    # dt_utc ожидаем aware UTC
    offset = tz.utcoffset(dt_utc)
    if offset is None:
        return "GMT"
    total_min = int(offset.total_seconds() // 60)
    sign = "+" if total_min >= 0 else "-"
    total_min = abs(total_min)
    hh = total_min // 60
    mm = total_min % 60
    return f"GMT{sign}{hh}" if mm == 0 else f"GMT{sign}{hh}:{mm:02d}"

def _fmt_date_ru(dt_local: datetime) -> str:
    # dt_local aware (уже в нужной TZ)
    return f"{dt_local.day} {RU_MONTHS[dt_local.month - 1]} {dt_local.year} ({RU_WEEKDAYS[dt_local.weekday()]})"

def format_period_variant_f(window_start_utc: datetime, window_end_utc: datetime, tz_name: str) -> str:
    """
    Вариант F:
    - один день: "10 января 2026 (суббота), 04:00–10:00 (GMT+5)"
    - разные дни: "9 января 2026 (пятница) 23:00 — 10 января 2026 (суббота) 11:00 (GMT+5)"
    """
    tz = ZoneInfo(tz_name)

    # нормализуем: должны быть aware
    if window_start_utc.tzinfo is None:
        window_start_utc = window_start_utc.replace(tzinfo=timezone.utc)
    if window_end_utc.tzinfo is None:
        window_end_utc = window_end_utc.replace(tzinfo=timezone.utc)

    s = window_start_utc.astimezone(tz)
    e = window_end_utc.astimezone(tz)

    gmt = _tz_gmt_label(tz, window_start_utc)

    if s.date() == e.date():
        return f"{_fmt_date_ru(s)}, {s:%H:%M}–{e:%H:%M} ({gmt})"

    return f"{_fmt_date_ru(s)} {s:%H:%M} — {_fmt_date_ru(e)} {e:%H:%M} ({gmt})"


# -----------------------------
# MATCH EVENTS pipeline
# -----------------------------
async def _reserve_match_events(db, now_utc: datetime) -> list[int]:
    """
    Reserve oldest queued match_events:
      queued -> sending
    Возвращаем список id зарезервированных событий.
    """
    q = (
        select(MatchEvent.id)
        .where(MatchEvent.notify_status == STATUS_QUEUED)
        .order_by(MatchEvent.id.asc())
        .limit(BATCH_LIMIT)
    )
    ids = [int(x) for x in (await db.execute(q)).scalars().all()]
    if not ids:
        return []

    # переводим в sending только те, что всё ещё queued
    await db.execute(
        update(MatchEvent)
        .where(MatchEvent.id.in_(ids), MatchEvent.notify_status == STATUS_QUEUED)
        .values(notify_status=STATUS_SENDING)
    )
    await db.commit()
    return ids


async def _load_match_events_with_subscriptions(db, event_ids: list[int]):
    """
    Подтягиваем MatchEvent + Subscription одним запросом.
    """
    q = (
        select(MatchEvent, Subscription)
        .join(Subscription, Subscription.id == MatchEvent.subscription_id)
        .where(MatchEvent.id.in_(event_ids))
        .order_by(MatchEvent.subscription_id.asc(), MatchEvent.id.asc())
    )
    return list((await db.execute(q)).all())


def _format_match_events_message(sub: Subscription, events: list[MatchEvent]) -> str:
    """
    Формат:
      - подробная часть заполняется до DETAIL_TEXT_LIMIT
      - остаток выводим ссылками (11), (12) ... пока не упремся в TG_MSG_HARD_LIMIT
    """
    sid = int(sub.id)
    header = (
        f"Найдены события по подписке: {sub.name or f'#{sid}'}\n"
        f"Совпадений: {len(events)}\n"
    )

    text_parts: list[str] = [header]
    used = len(header)

    detailed_indexes: list[int] = []
    remaining_indexes: list[int] = []

    # 1) Сначала пытаемся набить подробную часть
    for idx, ev in enumerate(events, start=1):
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

        block = f"\n{idx}) {author} • {ts}\n{excerpt or '—'}{link_text}"

        # ограничение на подробную часть (оставляем запас под секцию ссылок)
        if used + len(block) <= DETAIL_TEXT_LIMIT:
            text_parts.append(block)
            used += len(block)
            detailed_indexes.append(idx)
        else:
            remaining_indexes.append(idx)

    # 2) Если осталось что-то — добавляем секцию ссылок
    if remaining_indexes:
        tail_header = "\n\nОстальные совпадения (ссылками):"
        if used + len(tail_header) < TG_MSG_HARD_LIMIT:
            text_parts.append(tail_header)
            used += len(tail_header)

        for idx in remaining_indexes:
            ev = events[idx - 1]
            url = build_tg_message_link(
                chat_ref=getattr(sub, "chat_ref", None),
                chat_id=getattr(sub, "chat_id", None),
                message_id=int(ev.message_id),
            )
            # если ссылку построить нельзя — хотя бы покажем message_id
            line = f"\n{idx}) {url}" if url else f"\n{idx}) message_id={int(ev.message_id)}"

            if used + len(line) <= TG_MSG_HARD_LIMIT:
                text_parts.append(line)
                used += len(line)
            else:
                # если даже ссылки уже не влезают — честно сообщаем
                ell = "\n…(дальше не влезло по лимиту Telegram)"
                if used + len(ell) <= TG_MSG_HARD_LIMIT:
                    text_parts.append(ell)
                break

    text = "".join(text_parts)

    # финальная страховка
    if len(text) > TG_MSG_HARD_LIMIT:
        text = text[: TG_MSG_HARD_LIMIT - 1] + "…"

    return text


async def _mark_match_events(db, ids: list[int], status: str) -> None:
    if not ids:
        return
    await db.execute(
        update(MatchEvent)
        .where(MatchEvent.id.in_(ids))
        .values(notify_status=status)
    )
    await db.commit()


# -----------------------------
# DIGEST EVENTS pipeline (Summary)
# -----------------------------
async def _reserve_digest_events(db, now_utc: datetime) -> list[int]:
    """
    Reserve oldest queued digest_events:
      queued -> sending
    Возвращаем список id зарезервированных digest событий.
    """
    q = (
        select(DigestEvent.id)
        .where(DigestEvent.notify_status == STATUS_QUEUED)
        .order_by(DigestEvent.id.asc())
        .limit(BATCH_LIMIT)
    )
    ids = [int(x) for x in (await db.execute(q)).scalars().all()]
    if not ids:
        return []

    await db.execute(
        update(DigestEvent)
        .where(DigestEvent.id.in_(ids), DigestEvent.notify_status == STATUS_QUEUED)
        .values(notify_status=STATUS_SENDING)
    )
    await db.commit()
    return ids


async def _load_digest_events_with_subscriptions(db, event_ids: list[int]):
    q = (
        select(DigestEvent, Subscription)
        .join(Subscription, Subscription.id == DigestEvent.subscription_id)
        .where(DigestEvent.id.in_(event_ids))
        .order_by(DigestEvent.subscription_id.asc(), DigestEvent.id.asc())
    )
    return list((await db.execute(q)).all())


def _format_digest_message(sub: Subscription, ev: DigestEvent) -> str:
    """
    Формат:
      заголовок: “Резюме по подписке: {name}”
      период: {window_start} — {window_end}
      тело: digest_text
    """
    title = f"Резюме по подписке: {sub.name or f'#{sub.id}'}"

    ws_dt = getattr(ev, "window_start", None)
    we_dt = getattr(ev, "window_end", None)

    # MVP: TZ пока константой (позже возьмём из users.timezone)
    tz_name = os.getenv("DEV_USER_TZ", "Asia/Almaty")  # UTC+5

    if ws_dt and we_dt:
        period_human = format_period_variant_f(ws_dt, we_dt, tz_name)
        period = f"Период: {period_human}"
    else:
        period = "Период: —"

    body = (ev.digest_text or "").strip() or "—"

    text = f"{title}\n{period}\n\n{body}"

    if len(text) > TG_MSG_HARD_LIMIT:
        text = text[: TG_MSG_HARD_LIMIT - 1] + "…"

    return text


async def _mark_digest_events(db, ids: list[int], status: str) -> None:
    if not ids:
        return
    await db.execute(
        update(DigestEvent)
        .where(DigestEvent.id.in_(ids))
        .values(notify_status=status)
    )
    await db.commit()


# -----------------------------
# Runner
# -----------------------------
async def run_tick() -> int:
    """
    ВАЖНО: без вложенных db.begin().

    Схема:
      1) match_events: reserve -> load -> send -> mark sent/failed
      2) digest_events: reserve -> load -> send -> mark sent/failed
    """
    now_utc = _utc_now()
    exit_code = 0

    # -------------------------
    # 1) MATCH EVENTS
    # -------------------------
    async with AsyncSessionLocal() as db:
        try:
            match_ids = await _reserve_match_events(db, now_utc)
        except SQLAlchemyError as e:
            print(f"[notifications_runner] MATCH_RESERVE_FAILED: {e}")
            return 2

    if match_ids:
        async with AsyncSessionLocal() as db:
            rows = await _load_match_events_with_subscriptions(db, match_ids)

            # группировка: (owner_user_id, subscription_id) -> {sub, events[]}
            grouped: dict[tuple[int, int], dict] = {}
            for ev, sub in rows:
                owner_user_id = int(getattr(sub, "owner_user_id", None) or DEV_OWNER_USER_ID)
                sid = int(ev.subscription_id)
                key = (owner_user_id, sid)
                grouped.setdefault(key, {"sub": sub, "events": []})
                grouped[key]["events"].append(ev)

            for (owner_user_id, sid), pack in grouped.items():
                sub: Subscription = pack["sub"]
                events: list[MatchEvent] = pack["events"]

                # если тип подписки не events — помечаем failed
                sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
                if sub_type != "events":
                    await _mark_match_events(db, [int(e.id) for e in events], STATUS_FAILED)
                    exit_code = 1
                    print(f"[notifications_runner] MATCH_FAILED_UNKNOWN_TYPE owner_user_id={owner_user_id} sub_id={sid}")
                    continue

                try:
                    dest_chat_id = await _get_dest_chat_id(db, owner_user_id)
                    if not dest_chat_id:
                        raise RuntimeError(f"NO_BOT_USER_LINK owner_user_id={owner_user_id}")

                    text = _format_match_events_message(sub, events)
                    await bot_send_message(chat_id=int(dest_chat_id), text=text)

                    await _mark_match_events(db, [int(e.id) for e in events], STATUS_SENT)
                    print(f"[notifications_runner] MATCH_SENT owner_user_id={owner_user_id} sub_id={sid} events={len(events)}")

                except Exception as e:
                    await _mark_match_events(db, [int(e.id) for e in events], STATUS_FAILED)
                    exit_code = 1
                    print(f"[notifications_runner] MATCH_SEND_FAILED owner_user_id={owner_user_id} sub_id={sid} err={e}")

    else:
        print("[notifications_runner] No queued match_events")

    # -------------------------
    # 2) DIGEST EVENTS (Summary)
    # -------------------------
    async with AsyncSessionLocal() as db:
        try:
            digest_ids = await _reserve_digest_events(db, now_utc)
        except SQLAlchemyError as e:
            print(f"[notifications_runner] DIGEST_RESERVE_FAILED: {e}")
            return 2

    if digest_ids:
        async with AsyncSessionLocal() as db:
            rows = await _load_digest_events_with_subscriptions(db, digest_ids)

            # Здесь можно отправлять 1 сообщение на DigestEvent (как ты и сказала — проще)
            for ev, sub in rows:
                owner_user_id = int(getattr(sub, "owner_user_id", None) or DEV_OWNER_USER_ID)
                sid = int(ev.subscription_id)

                sub_type = (getattr(sub, "subscription_type", None) or "").lower()
                # ВАЖНО: название типа у тебя может быть "summary" или "digest" — оставляю поддержку обоих
                # (ты сама решишь итоговый enum; если у тебя строго "summary" — можно оставить только его)
                if sub_type not in ("summary", "digest"):
                    await _mark_digest_events(db, [int(ev.id)], STATUS_FAILED)
                    exit_code = 1
                    print(f"[notifications_runner] DIGEST_FAILED_UNKNOWN_TYPE owner_user_id={owner_user_id} sub_id={sid} ev_id={ev.id}")
                    continue

                try:
                    dest_chat_id = await _get_dest_chat_id(db, owner_user_id)
                    if not dest_chat_id:
                        raise RuntimeError(f"NO_BOT_USER_LINK owner_user_id={owner_user_id}")

                    text = _format_digest_message(sub, ev)
                    await bot_send_message(chat_id=int(dest_chat_id), text=text)

                    await _mark_digest_events(db, [int(ev.id)], STATUS_SENT)
                    print(f"[notifications_runner] DIGEST_SENT owner_user_id={owner_user_id} sub_id={sid} ev_id={ev.id}")

                except Exception as e:
                    await _mark_digest_events(db, [int(ev.id)], STATUS_FAILED)
                    exit_code = 1
                    print(f"[notifications_runner] DIGEST_SEND_FAILED owner_user_id={owner_user_id} sub_id={sid} ev_id={ev.id} err={e}")

    else:
        print("[notifications_runner] No queued digest_events")

    return exit_code


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
