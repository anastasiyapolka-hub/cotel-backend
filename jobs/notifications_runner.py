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
from db.models import Subscription, MatchEvent, DigestEvent, BotUserLink, User, UsageEvent
# используем уже существующие функции из main.py
from main import build_tg_message_link, bot_send_message
from bot_i18n import t as bot_t, months as bot_months, weekdays as bot_weekdays


# -----------------------------
# Config / constants
# -----------------------------

BATCH_LIMIT = 200
DETAIL_TEXT_LIMIT = 3800  # сколько максимум тратим на "подробную" часть; остальное оставляем под ссылки/хвост

STATUS_QUEUED = "queued"
STATUS_SENDING = "sending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"

# Для Telegram ограничение ~4096 символов, но в твоей задаче — digest и events уже форматируются отдельно.
# Здесь можно оставить запас, но это не обязательно для digest.
TG_MSG_HARD_LIMIT = 4096


# -----------------------------
# UsageEvent helpers (bot dispatch)
# -----------------------------
#
# Per TZ §7, every bot-dispatch attempt must produce a UsageEvent:
#   bot_dispatch_success  (status='success_counted')
#   bot_dispatch_failed   (status='failed_not_counted')
#
# Granularity for MVP: one UsageEvent per "group" — that is, per
# (owner_user_id, subscription_id) for MatchEvent dispatch, and one per
# DigestEvent for digest dispatch. We chose per-group instead of
# per-tick because UsageEvent.user_id is NOT NULL — a single
# tick-wide aggregate would have no natural user to attach to. The
# per-group event satisfies that AND gives the admin tab better
# failure granularity. The meta_json fields still match TZ §7
# ("events_in_group", "subscription_id", "error_code", "error_message").

def _derive_dispatch_error_code(error: BaseException) -> str:
    msg = str(error) if error is not None else ""
    if msg.startswith("NO_BOT_USER_LINK"):
        return "NO_BOT_USER_LINK"
    return "BOT_SEND_FAILED"


async def _record_bot_dispatch_event(
    db,
    *,
    owner_user_id: int,
    subscription_id: int,
    source_mode: str | None,
    chat_ref: str | None,
    success: bool,
    meta: dict,
) -> None:
    """
    Add a bot_dispatch_success / bot_dispatch_failed UsageEvent to the
    current session and commit. Called right after _mark_match_events /
    _mark_digest_events so it shares their transaction boundary.

    Meta keys with None values are stripped to keep storage compact.
    """
    clean_meta = {k: v for k, v in (meta or {}).items() if v is not None}
    try:
        db.add(
            UsageEvent(
                user_id=int(owner_user_id),
                event_type="bot_dispatch_success" if success else "bot_dispatch_failed",
                status="success_counted" if success else "failed_not_counted",
                source_mode=source_mode,
                chat_ref=chat_ref,
                subscription_id=int(subscription_id) if subscription_id is not None else None,
                meta_json=clean_meta,
            )
        )
        await db.commit()
    except Exception as log_err:  # noqa: BLE001
        # Don't let UsageEvent failure mask the actual dispatch outcome.
        print(
            f"[notifications_runner] failed to log bot_dispatch event "
            f"(success={success}) sub_id={subscription_id} owner={owner_user_id} err={log_err}"
        )
        try:
            await db.rollback()
        except Exception:
            pass


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


def _fmt_date(dt_local: datetime, language: str) -> str:
    """
    Форматирует дату в виде "10 января 2026 (суббота)" (RU) или
    "10 January 2026 (Saturday)" (EN). Формат нейтральный ISO-like,
    не US-стиль.
    """
    months_list = bot_months(language)
    weekdays_list = bot_weekdays(language)
    return (
        f"{dt_local.day} {months_list[dt_local.month - 1]} {dt_local.year}"
        f" ({weekdays_list[dt_local.weekday()]})"
    )


def format_period_variant_f(
    window_start_utc: datetime,
    window_end_utc: datetime,
    tz_name: str,
    language: str = "en",
) -> str:
    """
    Вариант F (language-aware):
    - один день: "10 января 2026 (суббота), 04:00–10:00 (GMT+5)" / EN-аналог
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
        return f"{_fmt_date(s, language)}, {s:%H:%M}–{e:%H:%M} ({gmt})"

    return (
        f"{_fmt_date(s, language)} {s:%H:%M}"
        f" — {_fmt_date(e, language)} {e:%H:%M} ({gmt})"
    )


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
    Подтягиваем MatchEvent + Subscription + User одним запросом.
    User нужен, чтобы знать `user.language` для wrapper-литералов.
    """
    q = (
        select(MatchEvent, Subscription, User)
        .join(Subscription, Subscription.id == MatchEvent.subscription_id)
        .join(User, User.id == Subscription.owner_user_id)
        .where(MatchEvent.id.in_(event_ids))
        .order_by(MatchEvent.subscription_id.asc(), MatchEvent.id.asc())
    )
    return list((await db.execute(q)).all())


def _format_match_events_message(
    sub: Subscription,
    events: list[MatchEvent],
    language: str = "en",
) -> str:
    """
    Формат:
      - подробная часть заполняется до DETAIL_TEXT_LIMIT
      - остаток выводим ссылками (11), (12) ... пока не упремся в TG_MSG_HARD_LIMIT

    Язык wrapper-литералов (шапка, счётчик, «Остальные совпадения»,
    «дальше не влезло…») определяется параметром `language`. Excerpt
    и ссылки — raw.
    """
    sid = int(sub.id)
    name = sub.name or f"#{sid}"
    header = (
        bot_t("match_header", language, name=name) + "\n"
        + bot_t("match_count", language, count=len(events)) + "\n"
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
        tail_header = bot_t("remaining_links_header", language)
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
                ell = bot_t("tg_limit_truncated", language)
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
        select(DigestEvent, Subscription, User)
        .join(Subscription, Subscription.id == DigestEvent.subscription_id)
        .join(User, User.id == Subscription.owner_user_id)
        .where(DigestEvent.id.in_(event_ids))
        .order_by(DigestEvent.subscription_id.asc(), DigestEvent.id.asc())
    )
    return list((await db.execute(q)).all())


def _format_digest_message(
    sub: Subscription,
    ev: DigestEvent,
    user: User,
    language: str = "en",
) -> str:
    """
    Формат:
      заголовок: "Summary for your subscription: {name}" / «Резюме по подписке: {name}»
      период:   "Period: {window_start} — {window_end}" / «Период: …»
      тело:     digest_text (язык narration уже выставлен LLM по `user.language`)
    """
    name = sub.name or f"#{sub.id}"
    title = bot_t("digest_title", language, name=name)

    ws_dt = getattr(ev, "window_start", None)
    we_dt = getattr(ev, "window_end", None)

    tz_name = getattr(user, "timezone", None) or "UTC"

    if ws_dt and we_dt:
        period_human = format_period_variant_f(ws_dt, we_dt, tz_name, language)
        period = bot_t("digest_period", language, period=period_human)
    else:
        period = bot_t("digest_period_empty", language)

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

            # группировка: (owner_user_id, subscription_id) -> {sub, user, events[]}
            grouped: dict[tuple[int, int], dict] = {}
            for ev, sub, user in rows:
                owner_user_id = getattr(sub, "owner_user_id", None)
                if not owner_user_id:
                    # Некорректная подписка — нельзя понять, кому слать.
                    print(f"[notifications_runner] MATCH_SKIP sub_id={sub.id} reason=NO_OWNER_USER_ID")
                    # Помечаем события failed, чтобы не зациклились
                    await _mark_match_events(db, [int(ev.id)], STATUS_FAILED)
                    continue
                owner_user_id = int(owner_user_id)
                sid = int(ev.subscription_id)
                key = (owner_user_id, sid)
                grouped.setdefault(key, {"sub": sub, "user": user, "events": []})
                grouped[key]["events"].append(ev)

            for (owner_user_id, sid), pack in grouped.items():
                sub: Subscription = pack["sub"]
                user: User = pack["user"]
                events: list[MatchEvent] = pack["events"]
                language = getattr(user, "language", None) or "en"
                events_in_group = len(events)
                sub_source_mode = getattr(sub, "source_mode", None)
                sub_chat_ref = getattr(sub, "chat_ref", None)

                # если тип подписки не events — помечаем failed
                sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
                if sub_type != "events":
                    await _mark_match_events(db, [int(e.id) for e in events], STATUS_FAILED)
                    exit_code = 1
                    print(f"[notifications_runner] MATCH_FAILED_UNKNOWN_TYPE owner_user_id={owner_user_id} sub_id={sid}")
                    await _record_bot_dispatch_event(
                        db,
                        owner_user_id=owner_user_id,
                        subscription_id=sid,
                        source_mode=sub_source_mode,
                        chat_ref=sub_chat_ref,
                        success=False,
                        meta={
                            "subscription_type": sub_type,
                            "events_in_group": events_in_group,
                            "error_code": "WRONG_SUBSCRIPTION_TYPE_FOR_MATCH_DISPATCH",
                            "error_message": f"expected events, got {sub_type}",
                        },
                    )
                    continue

                group_t0 = time.perf_counter()
                try:
                    dest_chat_id = await _get_dest_chat_id(db, owner_user_id)
                    if not dest_chat_id:
                        raise RuntimeError(f"NO_BOT_USER_LINK owner_user_id={owner_user_id}")

                    text = _format_match_events_message(sub, events, language=language)
                    await bot_send_message(chat_id=int(dest_chat_id), text=text)

                    await _mark_match_events(db, [int(e.id) for e in events], STATUS_SENT)
                    elapsed_ms = int((time.perf_counter() - group_t0) * 1000)
                    print(
                        f"[notifications_runner] MATCH_SENT owner_user_id={owner_user_id} "
                        f"sub_id={sid} events={events_in_group}"
                    )
                    await _record_bot_dispatch_event(
                        db,
                        owner_user_id=owner_user_id,
                        subscription_id=sid,
                        source_mode=sub_source_mode,
                        chat_ref=sub_chat_ref,
                        success=True,
                        meta={
                            "subscription_type": "events",
                            "events_in_group": events_in_group,
                            "elapsed_ms": elapsed_ms,
                        },
                    )

                except Exception as e:
                    await _mark_match_events(db, [int(e.id) for e in events], STATUS_FAILED)
                    elapsed_ms = int((time.perf_counter() - group_t0) * 1000)
                    exit_code = 1
                    print(
                        f"[notifications_runner] MATCH_SEND_FAILED owner_user_id={owner_user_id} "
                        f"sub_id={sid} err={e}"
                    )
                    await _record_bot_dispatch_event(
                        db,
                        owner_user_id=owner_user_id,
                        subscription_id=sid,
                        source_mode=sub_source_mode,
                        chat_ref=sub_chat_ref,
                        success=False,
                        meta={
                            "subscription_type": "events",
                            "events_in_group": events_in_group,
                            "elapsed_ms": elapsed_ms,
                            "error_code": _derive_dispatch_error_code(e),
                            "error_message": (str(e) or "")[:300] or None,
                        },
                    )

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

            for ev, sub, user in rows:
                owner_user_id = getattr(sub, "owner_user_id", None)
                sub_source_mode = getattr(sub, "source_mode", None)
                sub_chat_ref = getattr(sub, "chat_ref", None)
                digest_event_id = int(ev.id) if ev is not None else None

                if not owner_user_id:
                    # Misconfigured subscription — can't even attribute a
                    # UsageEvent (user_id NOT NULL). Just mark failed.
                    print(f"[notifications_runner] DIGEST_SKIP sub_id={sub.id} ev_id={ev.id} reason=NO_OWNER_USER_ID")
                    await _mark_digest_events(db, [int(ev.id)], STATUS_FAILED)
                    continue
                owner_user_id = int(owner_user_id)
                sid = int(ev.subscription_id)

                sub_type = (getattr(sub, "subscription_type", None) or "").lower()
                # ВАЖНО: название типа у тебя может быть "summary" или "digest" — оставляю поддержку обоих
                # (ты сама решишь итоговый enum; если у тебя строго "summary" — можно оставить только его)
                if sub_type not in ("summary", "digest"):
                    await _mark_digest_events(db, [int(ev.id)], STATUS_FAILED)
                    exit_code = 1
                    print(f"[notifications_runner] DIGEST_FAILED_UNKNOWN_TYPE owner_user_id={owner_user_id} sub_id={sid} ev_id={ev.id}")
                    await _record_bot_dispatch_event(
                        db,
                        owner_user_id=owner_user_id,
                        subscription_id=sid,
                        source_mode=sub_source_mode,
                        chat_ref=sub_chat_ref,
                        success=False,
                        meta={
                            "subscription_type": "digest",
                            "digest_event_id": digest_event_id,
                            "error_code": "WRONG_SUBSCRIPTION_TYPE_FOR_DIGEST_DISPATCH",
                            "error_message": f"expected summary/digest, got {sub_type or 'empty'}",
                        },
                    )
                    continue

                group_t0 = time.perf_counter()
                try:
                    dest_chat_id = await _get_dest_chat_id(db, owner_user_id)
                    if not dest_chat_id:
                        raise RuntimeError(f"NO_BOT_USER_LINK owner_user_id={owner_user_id}")

                    language = getattr(user, "language", None) or "en"
                    text = _format_digest_message(sub, ev, user, language=language)
                    await bot_send_message(chat_id=int(dest_chat_id), text=text)

                    await _mark_digest_events(db, [int(ev.id)], STATUS_SENT)
                    elapsed_ms = int((time.perf_counter() - group_t0) * 1000)
                    print(
                        f"[notifications_runner] DIGEST_SENT owner_user_id={owner_user_id} "
                        f"sub_id={sid} ev_id={ev.id}"
                    )
                    await _record_bot_dispatch_event(
                        db,
                        owner_user_id=owner_user_id,
                        subscription_id=sid,
                        source_mode=sub_source_mode,
                        chat_ref=sub_chat_ref,
                        success=True,
                        meta={
                            "subscription_type": "digest",
                            "digest_event_id": digest_event_id,
                            "elapsed_ms": elapsed_ms,
                        },
                    )

                except Exception as e:
                    await _mark_digest_events(db, [int(ev.id)], STATUS_FAILED)
                    elapsed_ms = int((time.perf_counter() - group_t0) * 1000)
                    exit_code = 1
                    print(
                        f"[notifications_runner] DIGEST_SEND_FAILED owner_user_id={owner_user_id} "
                        f"sub_id={sid} ev_id={ev.id} err={e}"
                    )
                    await _record_bot_dispatch_event(
                        db,
                        owner_user_id=owner_user_id,
                        subscription_id=sid,
                        source_mode=sub_source_mode,
                        chat_ref=sub_chat_ref,
                        success=False,
                        meta={
                            "subscription_type": "digest",
                            "digest_event_id": digest_event_id,
                            "elapsed_ms": elapsed_ms,
                            "error_code": _derive_dispatch_error_code(e),
                            "error_message": (str(e) or "")[:300] or None,
                        },
                    )

    else:
        print("[notifications_runner] No queued digest_events")

    return exit_code


def main():
    code = asyncio.run(run_tick())
    raise SystemExit(code)


if __name__ == "__main__":
    main()
