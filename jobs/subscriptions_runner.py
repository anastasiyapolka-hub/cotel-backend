# jobs/subscriptions_runner.py
import asyncio
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import select, update, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

from db.session import AsyncSessionLocal
from db.models import (
    Subscription, SubscriptionState, MatchEvent, DigestEvent, User, UsageEvent,
)
import os
from main import parse_iso_ts
from llm.service import (
    classify_subscription_matches,
    build_subscription_digest,
)
from llm.usage import LlmUsage, split_usage_for_meta, TOKENS_SOURCE_EMPTY
from llm.pricing import estimate_llm_cost_usd, cost_kwargs_for_meta
from plan_limits import utc_now
from telegram_service import fetch_chat_messages_for_subscription, disconnect_tg_client
from service_account_service import fetch_service_chat_messages_for_subscription

BATCH_SIZE = 20
EVENTS_READ_LIMIT = 1000  # как ты утвердила ранее для events
LEASE_MINUTES = 5      # сколько держим "замок" на время обработки
RETRY_MINUTES = 2      # через сколько повторять при ошибке


# ---------------------------------------------------------------------------
# UsageEvent helpers for subscription runtime
# ---------------------------------------------------------------------------
#
# Per TZ §6, every subscription run that actually invokes an LLM must
# produce one of:
#   subscription_run_success  (status='success_counted')
#   subscription_run_failed   (status='failed_not_counted')
#
# We skip UsageEvent in these cases (MVP, per TZ note in §6.5):
#   - trial expired (no work was done)
#   - NO_OWNER_USER_ID (subscription is misconfigured, not a runtime fail)
#   - no messages fetched (LLM was not called)
#
# Failure events MUST be written in a fresh AsyncSession because the
# main session is rolled back by run_tick() before the next sub runs.
# ---------------------------------------------------------------------------


def _ms_since(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)


def _new_metrics(sub_id: int, run_t0: float) -> dict[str, Any]:
    """Mutable bag the run-helper populates as it goes. Reused by both
    success and failure logging paths."""
    return {
        "subscription_id": int(sub_id),
        "run_t0": run_t0,
        "phase": "init",
        # Cached subscription summary fields (filled as soon as we read sub):
        "owner_user_id": None,
        "subscription_type": None,  # "events" | "digest"
        "source_mode": None,        # "personal" | "service"
        "frequency_minutes": None,
        "ai_model": None,
        "chat_ref": None,
        # Measurements:
        "fetch_duration_ms": None,
        "llm_duration_ms": None,
        "messages_fetched_count": None,
        "messages_sent_to_llm_count": None,
        "context_chars": None,
        "answer_chars": None,
        # Outcome counts:
        "matches_written": 0,
        "digest_events_written": 0,
        # LLM:
        "llm_usage": None,
        "llm_provider": None,
        "llm_provider_model": None,
        # Final total duration (set right before we write the event):
        "duration_ms_total": None,
    }


def _derive_error_code(metrics: dict, error: BaseException) -> str:
    phase = metrics.get("phase")
    source = (metrics.get("source_mode") or "personal").lower()

    if phase == "fetching":
        return "SERVICE_FETCH_FAILED" if source == "service" else "TELEGRAM_FETCH_FAILED"
    if phase == "llm_calling":
        return "LLM_ERROR"
    if phase == "post_llm":
        return "DB_WRITE_FAILED"
    return "INTERNAL_ERROR"


def _build_run_success_meta(
    metrics: dict,
    *,
    llm_usage: LlmUsage,
    cost_kwargs: dict,
) -> dict[str, Any]:
    """Build UsageEvent.meta_json for subscription_run_success.
    Drops keys with None values to keep storage compact."""
    raw: dict[str, Any] = {
        "subscription_type": metrics.get("subscription_type"),
        "source_mode": metrics.get("source_mode"),
        "frequency_minutes": metrics.get("frequency_minutes"),
        "ai_model": metrics.get("ai_model"),
        "messages_fetched_count": metrics.get("messages_fetched_count"),
        "messages_sent_to_llm_count": metrics.get("messages_sent_to_llm_count"),
        "matches_written": metrics.get("matches_written"),
        "digest_events_written": metrics.get("digest_events_written"),
        "context_chars": metrics.get("context_chars"),
        "answer_chars": metrics.get("answer_chars"),
        "duration_ms_total": metrics.get("duration_ms_total"),
        "duration_ms_fetch": metrics.get("fetch_duration_ms"),
        "duration_ms_llm": metrics.get("llm_duration_ms"),
    }
    raw.update(split_usage_for_meta(llm_usage))
    raw.update(cost_kwargs)
    return {k: v for k, v in raw.items() if v is not None}


def _build_run_failed_meta(
    metrics: dict,
    *,
    error_code: str,
    error_message: Optional[str],
    duration_ms_total: int,
) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "subscription_type": metrics.get("subscription_type"),
        "source_mode": metrics.get("source_mode"),
        "frequency_minutes": metrics.get("frequency_minutes"),
        "ai_model": metrics.get("ai_model"),
        "error_code": error_code,
        "error_message": error_message,
        "messages_fetched_count": metrics.get("messages_fetched_count"),
        "duration_ms_total": duration_ms_total,
        "duration_ms_fetch": metrics.get("fetch_duration_ms"),
        "duration_ms_llm": metrics.get("llm_duration_ms"),
    }
    return {k: v for k, v in raw.items() if v is not None}


async def _record_subscription_run_failed_new_session(
    metrics: dict,
    *,
    error: BaseException,
) -> None:
    """Write subscription_run_failed in a fresh AsyncSession.

    The caller's session is poisoned after the exception (rolled back
    by run_tick), so we cannot reuse it. If we can't determine
    owner_user_id we skip — UsageEvent requires user_id NOT NULL.
    """
    owner = metrics.get("owner_user_id")
    if not owner:
        return

    duration_ms_total = _ms_since(metrics["run_t0"])
    error_code = _derive_error_code(metrics, error)
    error_message = (str(error) or "")[:300] or None

    meta = _build_run_failed_meta(
        metrics,
        error_code=error_code,
        error_message=error_message,
        duration_ms_total=duration_ms_total,
    )

    try:
        async with AsyncSessionLocal() as db_log:
            db_log.add(
                UsageEvent(
                    user_id=int(owner),
                    event_type="subscription_run_failed",
                    status="failed_not_counted",
                    source_mode=metrics.get("source_mode"),
                    chat_ref=metrics.get("chat_ref"),
                    subscription_id=metrics.get("subscription_id"),
                    meta_json=meta,
                )
            )
            await db_log.commit()
    except Exception as log_err:  # noqa: BLE001
        # Don't let logging hide the original error — print and move on.
        print(
            f"[subscriptions_runner] failed to log subscription_run_failed "
            f"sub_id={metrics.get('subscription_id')} err={log_err}"
        )


async def _record_subscription_run_success_same_session(
    db,
    metrics: dict,
) -> None:
    """Write subscription_run_success on the same session as the run.

    Computes cost on the same session (read-only). Cost helper is safe
    against missing llm_pricing table.
    """
    owner = metrics.get("owner_user_id")
    if not owner:
        return

    llm_usage = metrics.get("llm_usage") or LlmUsage(0, 0, 0, TOKENS_SOURCE_EMPTY)

    cost = await estimate_llm_cost_usd(
        db,
        ai_model=metrics.get("ai_model") or "",
        input_tokens=llm_usage.input_tokens,
        output_tokens=llm_usage.output_tokens,
        tokens_source=llm_usage.tokens_source,
    )

    metrics["duration_ms_total"] = _ms_since(metrics["run_t0"])
    meta = _build_run_success_meta(
        metrics,
        llm_usage=llm_usage,
        cost_kwargs=cost_kwargs_for_meta(cost),
    )

    db.add(
        UsageEvent(
            user_id=int(owner),
            event_type="subscription_run_success",
            status="success_counted",
            source_mode=metrics.get("source_mode"),
            chat_ref=metrics.get("chat_ref"),
            subscription_id=metrics.get("subscription_id"),
            meta_json=meta,
        )
    )


# ---------------------------------------------------------------------------
# Existing reservation logic — unchanged
# ---------------------------------------------------------------------------

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
            .with_for_update(skip_locked=True)
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
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=LEASE_MINUTES)
            due_ids.append(int(sub.id))

        return due_ids


# ---------------------------------------------------------------------------
# Core per-subscription processing
# ---------------------------------------------------------------------------

async def _process_one_subscription(db, sub_id: int, now_utc: datetime) -> None:
    metrics = _new_metrics(sub_id, run_t0=time.perf_counter())

    sub = (await db.execute(select(Subscription).where(Subscription.id == sub_id))).scalar_one()

    # Cache sub fields into metrics ASAP so the failure path has them.
    metrics["owner_user_id"] = getattr(sub, "owner_user_id", None)
    metrics["chat_ref"] = getattr(sub, "chat_ref", None)
    metrics["ai_model"] = getattr(sub, "ai_model", None)
    metrics["frequency_minutes"] = int(getattr(sub, "frequency_minutes", 60) or 60)

    sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
    if sub_type == "summary":
        sub_type = "digest"
    metrics["subscription_type"] = sub_type

    metrics["source_mode"] = (getattr(sub, "source_mode", None) or "personal").lower()

    try:
        # ---- Trial expiry: no UsageEvent ----
        if getattr(sub, "is_trial", False):
            trial_ends_at = getattr(sub, "trial_ends_at", None)
            if trial_ends_at and trial_ends_at <= now_utc:
                st = (
                    await db.execute(
                        select(SubscriptionState).where(SubscriptionState.subscription_id == sub_id)
                    )
                ).scalar_one_or_none()

                sub.is_active = False
                sub.status = "trial_expired"
                sub.last_error = None

                if st is None:
                    st = SubscriptionState(subscription_id=sub.id)
                    db.add(st)

                st.last_checked_at = now_utc
                st.next_run_at = None
                return

        st = (
            await db.execute(
                select(SubscriptionState).where(SubscriptionState.subscription_id == sub_id)
            )
        ).scalar_one_or_none()

        last_message_id = getattr(st, "last_message_id", None) if st else None
        freq_min = metrics["frequency_minutes"]

        owner_user_id = metrics["owner_user_id"]
        if not owner_user_id:
            # Misconfigured subscription — not a runtime failure. Park it.
            print(f"[subscriptions_runner] SKIP sub_id={sub.id} reason=NO_OWNER_USER_ID")

            if st is None:
                st = SubscriptionState(subscription_id=sub.id)
                db.add(st)

            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
            sub.last_error = "NO_OWNER_USER_ID"
            return

        owner_user_id = int(owner_user_id)

        owner = (
            await db.execute(select(User).where(User.id == owner_user_id))
        ).scalar_one_or_none()
        owner_language = getattr(owner, "language", None) or "en"

        source_mode = metrics["source_mode"]

        # =====================================================================
        # DIGEST / SUMMARY
        # =====================================================================
        if sub_type == "digest":
            since_dt = now_utc - timedelta(minutes=freq_min)
            min_id = None

            fetch_t0 = time.perf_counter()
            metrics["phase"] = "fetching"
            try:
                if source_mode == "service":
                    entity, msgs = await fetch_service_chat_messages_for_subscription(
                        db=db,
                        chat_link=sub.chat_ref,
                        since_dt=since_dt,
                        min_id=min_id,
                        limit=EVENTS_READ_LIMIT,
                    )
                else:
                    entity, msgs = await fetch_chat_messages_for_subscription(
                        db=db,
                        owner_user_id=owner_user_id,
                        chat_link=sub.chat_ref,
                        since_dt=since_dt,
                        min_id=min_id,
                        limit=EVENTS_READ_LIMIT,
                    )
                metrics["phase"] = "fetched"
            finally:
                metrics["fetch_duration_ms"] = _ms_since(fetch_t0)

            if getattr(sub, "chat_id", None) is None:
                ent_id = getattr(entity, "id", None)
                if ent_id is not None:
                    sub.chat_id = int(ent_id)

            if st is None:
                st = SubscriptionState(subscription_id=sub.id)
                db.add(st)

            metrics["messages_fetched_count"] = len(msgs or [])

            if not msgs:
                # No messages → no LLM call → no UsageEvent (per TZ MVP).
                st.last_success_at = now_utc
                st.last_checked_at = now_utc
                st.next_run_at = now_utc + timedelta(minutes=freq_min)
                return

            metrics["messages_sent_to_llm_count"] = len(msgs)
            metrics["context_chars"] = _approx_context_chars(msgs)

            ids = [
                int(m["message_id"])
                for m in msgs
                if isinstance(m, dict) and m.get("message_id") is not None
            ]
            newest_id = max(ids) if ids else None
            oldest_id = min(ids) if ids else None

            chat_title = getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat"

            llm_t0 = time.perf_counter()
            metrics["phase"] = "llm_calling"
            try:
                llm_result = await build_subscription_digest(
                    prompt=sub.prompt,
                    chat_title=chat_title,
                    messages=msgs,
                    answer_language=owner_language,
                    ai_model=sub.ai_model,
                    return_usage=True,
                )
                metrics["phase"] = "post_llm"
            finally:
                metrics["llm_duration_ms"] = _ms_since(llm_t0)

            metrics["llm_usage"] = llm_result.usage
            metrics["llm_provider"] = llm_result.provider
            metrics["llm_provider_model"] = llm_result.provider_model

            digest_text = ""
            confidence = None
            llm_json = llm_result.data or {}
            if isinstance(llm_json, dict):
                digest_text = (llm_json.get("digest_text") or "").strip()
                confidence = llm_json.get("confidence")

            if len(digest_text) > 4096:
                digest_text = digest_text[:4096].rstrip() + "…"

            metrics["answer_chars"] = len(digest_text)

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
            r = await db.execute(stmt)
            metrics["digest_events_written"] = 1 if getattr(r, "rowcount", 0) == 1 else 0

            if newest_id:
                st.last_message_id = int(newest_id)
            st.last_success_at = now_utc
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=freq_min)

            await _record_subscription_run_success_same_session(db, metrics)
            return

        # =====================================================================
        # EVENTS
        # =====================================================================
        if last_message_id:
            since_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            min_id = int(last_message_id)
        else:
            since_dt = now_utc - timedelta(minutes=freq_min)
            min_id = None

        fetch_t0 = time.perf_counter()
        metrics["phase"] = "fetching"
        try:
            if source_mode == "service":
                entity, msgs = await fetch_service_chat_messages_for_subscription(
                    db=db,
                    chat_link=sub.chat_ref,
                    since_dt=since_dt,
                    min_id=min_id,
                    limit=EVENTS_READ_LIMIT,
                )
            else:
                entity, msgs = await fetch_chat_messages_for_subscription(
                    db=db,
                    owner_user_id=owner_user_id,
                    chat_link=sub.chat_ref,
                    since_dt=since_dt,
                    min_id=min_id,
                    limit=EVENTS_READ_LIMIT,
                )
            metrics["phase"] = "fetched"
        finally:
            metrics["fetch_duration_ms"] = _ms_since(fetch_t0)

        if getattr(sub, "chat_id", None) is None:
            ent_id = getattr(entity, "id", None)
            if ent_id is not None:
                sub.chat_id = int(ent_id)

        if st is None:
            st = SubscriptionState(subscription_id=sub.id)
            db.add(st)

        metrics["messages_fetched_count"] = len(msgs or [])

        if not msgs:
            # No messages → no LLM → no UsageEvent (per TZ MVP).
            st.last_success_at = now_utc
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=freq_min)
            return

        metrics["messages_sent_to_llm_count"] = len(msgs)
        metrics["context_chars"] = _approx_context_chars(msgs)

        msg_by_id: dict[int, dict] = {}
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

        llm_t0 = time.perf_counter()
        metrics["phase"] = "llm_calling"
        try:
            llm_result = await classify_subscription_matches(
                prompt=sub.prompt,
                chat_title=getattr(entity, "title", None) or getattr(entity, "username", None) or "Chat",
                messages=msgs,
                ux_language=owner_language,
                ai_model=sub.ai_model,
                return_usage=True,
            )
            metrics["phase"] = "post_llm"
        finally:
            metrics["llm_duration_ms"] = _ms_since(llm_t0)

        metrics["llm_usage"] = llm_result.usage
        metrics["llm_provider"] = llm_result.provider
        metrics["llm_provider_model"] = llm_result.provider_model

        llm_json = llm_result.data or {}
        matches = (llm_json.get("matches") or []) if isinstance(llm_json, dict) else []

        # Track total answer chars roughly — concatenated reasons + excerpts.
        # The classify LLM produces JSON not free text; "answer_chars" is more
        # diagnostic than meaningful here, but we still log it for symmetry.
        answer_chars = 0

        matches_written = 0
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

            reason = item.get("reason")
            answer_chars += len(excerpt) + len(reason or "")

            db.add(
                MatchEvent(
                    subscription_id=sub.id,
                    message_id=int(mid),
                    message_ts=ts,
                    author_id=author_id,
                    author_display=author_display,
                    excerpt=excerpt,
                    reason=reason,
                    notify_status="queued",
                    llm_payload=None,
                )
            )
            matches_written += 1

        metrics["matches_written"] = matches_written
        metrics["answer_chars"] = answer_chars or None

        st.last_message_id = int(newest_id) if newest_id else st.last_message_id
        st.last_success_at = now_utc
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=freq_min)

        await _record_subscription_run_success_same_session(db, metrics)

    except Exception as err:
        # Write subscription_run_failed in a SEPARATE session — main session
        # is about to be rolled back by run_tick(). Then re-raise so the
        # retry-scheduling logic in run_tick can fire.
        await _record_subscription_run_failed_new_session(metrics, error=err)
        raise


def _approx_context_chars(msgs: list[dict]) -> int:
    """Approximation of LLM context size in characters. Informational —
    the provider returns exact input_tokens via usage."""
    total = 0
    for m in msgs:
        if not isinstance(m, dict):
            continue
        total += (
            len(m.get("text") or "")
            + len(m.get("author_display") or "")
            + len(str(m.get("message_ts") or ""))
            + 4
        )
    return total


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

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
