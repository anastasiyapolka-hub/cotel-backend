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
    Subscription, SubscriptionState,
    SubscriptionChat, SubscriptionChatState,
    MatchEvent, DigestEvent, User, UsageEvent,
)
import os
from main import parse_iso_ts
from llm.service import (
    classify_subscription_matches,
    build_subscription_digest,
)
from llm.routing import route_subscription
from llm.usage import LlmUsage, split_usage_for_meta, TOKENS_SOURCE_EMPTY
from llm.pricing import estimate_llm_cost_usd, cost_kwargs_for_meta, get_token_rates
from plan_limits import utc_now

import billing
from telegram_service import fetch_chat_messages_for_subscription, disconnect_tg_client
from service_account_service import fetch_service_chat_messages_for_subscription
from media_filter.types import MediaFilterRequest
from media_filter.telethon_search import fetch_chat_media
from media_filter.llm_parser import parse_user_query
from media_filter.post_filter import apply_structured_filters, compute_effective_window
from media_filter.reranker import rerank_messages

BATCH_SIZE = 20
EVENTS_READ_LIMIT = 1000  # как ты утвердила ранее для events
LEASE_MINUTES = 5      # сколько держим "замок" на время обработки одиночной
LEASE_MINUTES_GROUP = 40  # групповая до 20 чатов — берём с запасом, чтобы другой раннер не подхватил
RETRY_MINUTES = 2      # через сколько повторять при ошибке

# Пауза между fetch'ами разных чатов в групповой подписке.
# Достаточно мала, чтобы для журналиста оставался лайв-мониторинг,
# и достаточна, чтобы не словить FLOOD_WAIT на одной StringSession.
GROUP_INTER_CHAT_SLEEP_SEC = 0.3


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
    Drops keys with None values to keep storage compact.

    Для групповой подписки (metrics["is_group"] = True) добавляем поля:
      is_group, group_size, chats_ok, chats_failed, chats_empty
      и per_chat: [...] с расшифровкой по каждому чату.
    """
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

    if metrics.get("is_group"):
        raw["is_group"] = True
        raw["group_size"] = metrics.get("group_size")
        raw["chats_ok"] = metrics.get("chats_ok")
        raw["chats_failed"] = metrics.get("chats_failed")
        raw["chats_empty"] = metrics.get("chats_empty")
        per_chat = metrics.get("per_chat_results")
        if per_chat:
            raw["per_chat"] = per_chat

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
    if metrics.get("is_group"):
        raw["is_group"] = True
        raw["group_size"] = metrics.get("group_size")
        raw["chats_ok"] = metrics.get("chats_ok")
        raw["chats_failed"] = metrics.get("chats_failed")
        raw["chats_empty"] = metrics.get("chats_empty")
        per_chat = metrics.get("per_chat_results")
        if per_chat:
            raw["per_chat"] = per_chat
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

    После cutover'а: также списывает токены с баланса пользователя через
    billing.debit. Сумма и reason берутся из subscription_type:
      - events  → reason='subscription_event'
      - digest  → reason='subscription_digest'
    """
    owner = metrics.get("owner_user_id")
    if not owner:
        return

    llm_usage = metrics.get("llm_usage") or LlmUsage(0, 0, 0, TOKENS_SOURCE_EMPTY)
    ai_model = metrics.get("ai_model") or ""

    cost = await estimate_llm_cost_usd(
        db,
        ai_model=ai_model,
        input_tokens=llm_usage.input_tokens,
        output_tokens=llm_usage.output_tokens,
        tokens_source=llm_usage.tokens_source,
        thinking_tokens=llm_usage.thinking_tokens,
    )

    # === Биллинг (cutover) ===
    # Считаем стоимость одного вызова в наших токенах. Минимум 1 токен.
    # На is_empty / failed_to_call_llm (нет llm_usage) сюда не попадаем —
    # _process_one_subscription делает early return перед вызовом этой функции.
    # Media-filter ветка уже посчитала точные tokens_charged по разбивке
    # моделей (парсер на одной, реранкер на другой) — уважаем её число
    # и не пересчитываем по тарифам одной модели, что было бы неточно.
    precomputed = metrics.get("mf_tokens_charged_precomputed")
    if precomputed is not None:
        tokens_charged = int(precomputed)
    else:
        rates = await get_token_rates(db, ai_model)
        if rates is None or not llm_usage.input_tokens and not llm_usage.output_tokens:
            # Нет прайса для модели или пустой usage (трейс/сетевая ошибка
            # вернула пустой ответ) — списываем минимум.
            tokens_charged = 1
        else:
            tokens_charged = billing.compute_tokens_for_llm_call(
                input_tokens=llm_usage.input_tokens or 0,
                output_tokens=llm_usage.output_tokens or 0,
                thinking_tokens=llm_usage.thinking_tokens or 0,
                in_per_1k=rates.in_per_1k,
                out_per_1k=rates.out_per_1k,
            )

    metrics["tokens_charged"] = tokens_charged
    metrics["duration_ms_total"] = _ms_since(metrics["run_t0"])
    meta = _build_run_success_meta(
        metrics,
        llm_usage=llm_usage,
        cost_kwargs=cost_kwargs_for_meta(cost),
    )

    # Создаём UsageEvent и flush'имся, чтобы получить id для FK в token_transactions.
    usage_event = UsageEvent(
        user_id=int(owner),
        event_type="subscription_run_success",
        status="success_counted",
        source_mode=metrics.get("source_mode"),
        chat_ref=metrics.get("chat_ref"),
        subscription_id=metrics.get("subscription_id"),
        meta_json=meta,
    )
    db.add(usage_event)
    await db.flush()

    # Определяем reason по subscription_type. После cutover'а:
    #   events → subscription_event, digest → subscription_digest.
    sub_type = (metrics.get("subscription_type") or "events").lower()
    if sub_type == "digest":
        reason = billing.REASON_SUBSCRIPTION_DIGEST
    else:
        reason = billing.REASON_SUBSCRIPTION_EVENT

    await billing.debit(
        db,
        user_id=int(owner),
        amount=tokens_charged,
        reason=reason,
        related_event_id=int(usage_event.id),
        meta={
            "used_model": ai_model,
            "subscription_id": metrics.get("subscription_id"),
            "input_tokens": llm_usage.input_tokens,
            "output_tokens": llm_usage.output_tokens,
            "thinking_tokens": llm_usage.thinking_tokens,
            "tokens_charged": tokens_charged,
        },
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
    - резервируем: last_checked_at=now, next_run_at=now+lease
    - lease = LEASE_MINUTES_GROUP для is_group, иначе LEASE_MINUTES
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
            lease = LEASE_MINUTES_GROUP if bool(getattr(sub, "is_group", False)) else LEASE_MINUTES
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=lease)
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
    metrics["frequency_minutes"] = int(getattr(sub, "frequency_minutes", 60) or 60)

    sub_type = (getattr(sub, "subscription_type", None) or "events").lower()
    if sub_type == "summary":
        sub_type = "digest"
    metrics["subscription_type"] = sub_type

    # === Routing-based модель для подписок (после cutover'а) ===
    # Поле sub.ai_model в БД больше НЕ используется для выбора модели —
    # вся подписочная нагрузка идёт на Flash Lite через routing.route_subscription.
    # Само поле sub.ai_model дропнем в этапе 4 рефакторинга.
    subscription_model = route_subscription(sub_type)
    metrics["ai_model"] = subscription_model.slug

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

        # === Soft-block по балансу токенов (cutover) ===
        # Подписки на каждом tick'е проверяют, есть ли у пользователя минимум
        # токенов на запрос. Если нет — НЕ вызываем Telegram fetch и LLM,
        # ставим sub.status='no_tokens'. Подписка не «ломается», просто бездействует.
        # Когда баланс восстановится (top-up, monthly_grant) — следующий tick
        # пройдёт проверку и подписка снова заработает. status вернётся в 'ok'.
        can_spend, balance = await billing.check_can_spend(
            db, user_id=owner_user_id, tier="light",
        )
        if not can_spend:
            print(
                f"[subscriptions_runner] SKIP sub_id={sub.id} reason=NO_TOKENS "
                f"monthly_used={balance.monthly_used}/{balance.monthly_granted} "
                f"topup={balance.topup_balance}"
            )
            if st is None:
                st = SubscriptionState(subscription_id=sub.id)
                db.add(st)
            sub.status = "no_tokens"
            sub.last_error = None  # не ошибка, нет токенов — это нормальная ситуация
            st.last_checked_at = now_utc
            # Обычная периодичность — на следующий tick проверим снова
            st.next_run_at = now_utc + timedelta(minutes=metrics["frequency_minutes"])
            return

        # Если статус был 'no_tokens', а сейчас баланс восстановился —
        # возвращаем подписку в нормальное состояние.
        if sub.status == "no_tokens":
            sub.status = "ok"

        # =====================================================================
        # ГРУППОВАЯ ПОДПИСКА — отдельная ветка
        # =====================================================================
        if bool(getattr(sub, "is_group", False)):
            metrics["is_group"] = True
            await _process_group_subscription(
                db=db,
                sub=sub,
                st=st,
                metrics=metrics,
                now_utc=now_utc,
                freq_min=freq_min,
                owner_user_id=owner_user_id,
                owner_language=owner_language,
                source_mode=source_mode,
                subscription_model_slug=subscription_model.slug,
                sub_type=sub_type,
            )
            return

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
                    ai_model=subscription_model.slug,
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
        # EVENTS — медиа-фильтр (новая ветка)
        # =====================================================================
        # Если подписка хранит media_filter — идём отдельным пайплайном:
        #   • тянем через messages.search ТОЛЬКО выбранные типы медиа
        #     (а не весь текст, как обычная events-подписка);
        #   • опционально парсим sub.prompt через LLM (как user_query в Q&A);
        #   • применяем структурный пост-фильтр;
        #   • при наличии semantic_query — реранкер;
        #   • каждое выжившее сообщение → MatchEvent.
        sub_media_filter = getattr(sub, "media_filter", None)
        if isinstance(sub_media_filter, dict) and sub_media_filter.get("enabled", True):
            await _run_media_filter_events_branch(
                db=db,
                sub=sub,
                st=st,
                metrics=metrics,
                now_utc=now_utc,
                freq_min=freq_min,
                last_message_id=last_message_id,
                owner_user_id=owner_user_id,
                sub_media_filter=sub_media_filter,
            )
            return

        # =====================================================================
        # EVENTS — классическая текстовая ветка
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
                ai_model=subscription_model.slug,
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


async def _run_media_filter_events_branch(
    *,
    db,
    sub,
    st,
    metrics: dict,
    now_utc: datetime,
    freq_min: int,
    last_message_id: Optional[int],
    owner_user_id: int,
    sub_media_filter: dict,
):
    """
    Медиа-ветка для events-подписок. Тянет ТОЛЬКО выбранные типы медиа
    через messages.search, опционально применяет LLM-парсер/реранкер
    по sub.prompt, и каждое выжившее сообщение пишет как MatchEvent.

    Биллинг суммирует токены парсера + реранкеров и списывает одним
    debit (как в Q&A-ветке).
    """
    # 1) Распарсить media_filter
    try:
        request = MediaFilterRequest.model_validate(sub_media_filter)
    except Exception as e:
        # Невалидный media_filter в БД — парковать подписку.
        sub.status = "error"
        sub.last_error = f"BAD_MEDIA_FILTER: {type(e).__name__}"
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
        metrics["phase"] = "bad_media_filter"
        return

    # 2) Окно времени: при первом запуске = now − freq_min,
    #    при последующих — будем полагаться на min_id курсор,
    #    но min_date оставляем равным 1970 (Telegram min_id режет всё).
    if last_message_id:
        min_date_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
        cursor_min_id: Optional[int] = int(last_message_id)
    else:
        min_date_dt = now_utc - timedelta(minutes=freq_min)
        cursor_min_id = None

    # 3) LLM-парсер sub.prompt (если есть)
    prompt = (sub.prompt or "").strip()
    metrics["phase"] = "media_filter_parsing"
    parser_outcome = await parse_user_query(
        user_query=prompt,
        ui_window_from=min_date_dt,
        ui_window_to=None,
        selected_categories=[c.value for c in request.effective_categories()],
        now=now_utc,
    )
    parsed = parser_outcome.parsed

    # 4) Эффективное окно с учётом time_window_override
    effective_window = compute_effective_window(
        ui_window_from=min_date_dt,
        ui_window_to=None,
        override=parsed.structured_filters.time_window_override,
    )

    # 5) Telethon search
    metrics["phase"] = "media_filter_fetching"
    fetch_t0 = time.perf_counter()
    try:
        fetched = await fetch_chat_media(
            db, owner_user_id, sub.chat_ref,
            request=request,
            min_date=effective_window.min_date,
            max_date=effective_window.max_date,
            min_id=cursor_min_id,
        )
    finally:
        metrics["fetch_duration_ms"] = _ms_since(fetch_t0)
    metrics["phase"] = "media_filter_fetched"

    if fetched.error_code:
        # Любая ошибка fetch — парковка с retry.
        sub.status = "error"
        sub.last_error = f"MEDIA_FETCH_FAILED: {fetched.error_code}"
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
        metrics["phase"] = "media_fetch_failed"
        return

    if getattr(sub, "chat_id", None) is None:
        ent_id = getattr(fetched.entity, "id", None)
        if ent_id is not None:
            sub.chat_id = int(ent_id)

    raw_messages = fetched.messages
    metrics["messages_fetched_count"] = len(raw_messages)

    if not raw_messages:
        # Нет новых медиа — обновляем счётчики, без LLM, без UsageEvent.
        st.last_success_at = now_utc
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=freq_min)
        return

    # 6) Структурный пост-фильтр
    after_structured = apply_structured_filters(raw_messages, parsed.structured_filters)
    metrics["messages_after_structured"] = len(after_structured)

    # 7) Семантический реранкер (опц.)
    survivors = after_structured
    reranker_outcome = None
    if (
        parsed.needs_semantic_rerank
        and parsed.semantic_query
        and after_structured
    ):
        metrics["phase"] = "media_filter_rerank"
        reranker_outcome = await rerank_messages(
            messages=after_structured,
            semantic_query=parsed.semantic_query,
        )
        survivors = reranker_outcome.survivors
    metrics["messages_after_semantic"] = len(survivors)

    # 8) Биллинг: токены парсера + реранкеров.
    # Складываем usage всех LLM-вызовов (парсер + N батчей реранкера),
    # вычисляем стоимость по тарифам каждой модели и сохраняем сумму
    # в metrics. Списание (billing.debit) делает дальше один общий вызов
    # _record_subscription_run_success_same_session — там же создаётся
    # UsageEvent. ВАЖНО: тут НИЧЕГО не списываем — иначе будет double-debit.
    llm_results = []
    if parser_outcome.llm_result is not None:
        llm_results.append(parser_outcome.llm_result)
    if reranker_outcome is not None:
        llm_results.extend(reranker_outcome.llm_results)

    used_models: list[str] = []
    total_input_tokens = 0
    total_output_tokens = 0
    total_thinking_tokens = 0
    tokens_charged_total = 0
    for r in llm_results:
        if r is None:
            continue
        slug = r.used_model.slug
        used_models.append(slug)
        in_tok = r.usage.input_tokens or 0
        out_tok = r.usage.output_tokens or 0
        think_tok = r.usage.thinking_tokens or 0
        total_input_tokens += in_tok
        total_output_tokens += out_tok
        total_thinking_tokens += think_tok
        rates = await get_token_rates(db, slug)
        if rates is None:
            print(
                f"[subscriptions_runner] media_filter.no_pricing_row model={slug}"
            )
            tokens_charged_total += 1
        else:
            tokens_charged_total += billing.compute_tokens_for_llm_call(
                input_tokens=in_tok,
                output_tokens=out_tok,
                thinking_tokens=think_tok,
                in_per_1k=rates.in_per_1k,
                out_per_1k=rates.out_per_1k,
            )

    # Формируем LlmUsage из суммы и кладём в metrics. Так общая
    # запись UsageEvent (в _record_subscription_run_success_same_session)
    # увидит реальный usage, а не пустой → cost_calculation_method
    # перестанет быть "no_llm_call". `ai_model` — primary (parser),
    # его тарифы будут использованы для оценочной стоимости в USD;
    # списание в наших токенах уже точное (мы посчитали выше по моделям).
    if llm_results:
        primary_slug = llm_results[0].used_model.slug
        primary_source = llm_results[0].usage.tokens_source
        metrics["ai_model"] = primary_slug
        metrics["llm_usage"] = LlmUsage(
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            total_tokens=total_input_tokens + total_output_tokens,
            tokens_source=primary_source,
            thinking_tokens=total_thinking_tokens,
        )
        # Префикс mf_ — чтобы _build_run_success_meta при необходимости
        # клал детали разбивки по моделям рядом, не перетирая ai_model.
        metrics["mf_used_models"] = used_models
        metrics["mf_tokens_charged_precomputed"] = tokens_charged_total
        metrics["is_media_filter"] = True

    # 9) MatchEvent на каждое выжившее сообщение
    matches_written = 0
    newest_id = last_message_id
    for m in survivors:
        excerpt_src = (m.caption or m.text or "").strip()
        if len(excerpt_src) > 300:
            excerpt_src = excerpt_src[:300].rstrip() + "…"
        kind_label = m.kind.value
        # reason формируем самостоятельно — фронт/бот сможет показать тип медиа.
        reason = f"media:{kind_label}"
        if m.file_size:
            reason += f"|size={m.file_size}"
        if m.duration_sec:
            reason += f"|dur={m.duration_sec}s"
        db.add(
            MatchEvent(
                subscription_id=sub.id,
                message_id=int(m.message_id),
                message_ts=m.date,
                author_id=m.sender_id,
                author_display=m.sender_username or m.sender_display_name,
                excerpt=excerpt_src,
                reason=reason,
                notify_status="queued",
                llm_payload={
                    "kind": kind_label,
                    "permalink": m.permalink,
                    "file_size": m.file_size,
                    "duration_sec": m.duration_sec,
                    "mime_type": m.mime_type,
                    "file_name": m.file_name,
                    "forwarded": bool(m.forward_info),
                    "ttl_period_sec": m.ttl_period_sec,
                },
            )
        )
        matches_written += 1
        if m.message_id and (newest_id is None or m.message_id > newest_id):
            newest_id = int(m.message_id)

    metrics["matches_written"] = matches_written

    # 9) Финал: курсор + расписание. UsageEvent + billing.debit делает
    # одним общим вызовом _record_subscription_run_success_same_session,
    # читая llm_usage/ai_model/mf_tokens_charged_precomputed из metrics.
    st.last_message_id = int(newest_id) if newest_id else st.last_message_id
    st.last_success_at = now_utc
    st.last_checked_at = now_utc
    st.next_run_at = now_utc + timedelta(minutes=freq_min)

    await _record_subscription_run_success_same_session(db, metrics)


# ---------------------------------------------------------------------------
# Групповая подписка — обработка нескольких чатов одной подпиской
# ---------------------------------------------------------------------------
#
# Стратегия:
# 1. Загружаем список чатов из subscription_chats + текущий курсор
#    каждого чата из subscription_chat_state.
# 2. ПОСЛЕДОВАТЕЛЬНО (микропауза между чатами, чтобы не словить
#    FLOOD_WAIT на одной StringSession юзера):
#      • fetch чата по cursor;
#      • если есть сообщения — LLM (events.classify_subscription_matches
#        или digest.build_subscription_digest);
#      • пишем MatchEvent / DigestEvent с chat_id/chat_ref/chat_title;
#      • обновляем per-chat курсор.
# 3. Частичные фейлы по отдельным чатам не валят всю подписку:
#    error пишем в subscription_chat_state.last_error,
#    продолжаем обрабатывать оставшиеся.
# 4. UsageEvent — ОДНА запись subscription_run_success на тик,
#    в meta_json.per_chat — расшифровка по чатам;
#    токены суммируются по N LLM-вызовам, debit — один.
# 5. Если ВСЕ чаты упали — пробрасываем исключение, чтобы run_tick
#    написал subscription_run_failed.
# ---------------------------------------------------------------------------
async def _process_group_subscription(
    *,
    db,
    sub: Subscription,
    st: Optional[SubscriptionState],
    metrics: dict,
    now_utc: datetime,
    freq_min: int,
    owner_user_id: int,
    owner_language: str,
    source_mode: str,
    subscription_model_slug: str,
    sub_type: str,
) -> None:
    # Медиа-фильтр для групповой подписки: для каждого чата вызываем
    # отдельный pipeline (Telethon messages.search + LLM-парсер/реранкер),
    # MatchEvent с chat_id/chat_ref. Парсер sub.prompt вызываем ОДИН раз
    # перед циклом (он не зависит от чата) — экономия токенов.
    media_filter_dict = getattr(sub, "media_filter", None)
    is_media_group = (
        isinstance(media_filter_dict, dict)
        and media_filter_dict.get("enabled", True)
        and sub_type == "events"
    )

    # Group subscriptions работают только в personal-mode (на бэке мы
    # это валидируем при создании, но runner защищается от старых данных).
    if source_mode != "personal":
        sub.status = "error"
        sub.last_error = "GROUP_REQUIRES_PERSONAL"
        if st is None:
            st = SubscriptionState(subscription_id=sub.id)
            db.add(st)
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
        metrics["phase"] = "group_requires_personal"
        return

    # Загружаем список чатов в правильном порядке.
    chats_res = await db.execute(
        select(SubscriptionChat)
        .where(SubscriptionChat.subscription_id == sub.id)
        .order_by(SubscriptionChat.position.asc())
    )
    chats: list[SubscriptionChat] = list(chats_res.scalars().all())

    if not chats:
        # Группа без чатов — некорректное состояние. Парк.
        sub.status = "error"
        sub.last_error = "GROUP_NO_CHATS"
        if st is None:
            st = SubscriptionState(subscription_id=sub.id)
            db.add(st)
        st.last_checked_at = now_utc
        st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
        metrics["phase"] = "group_empty"
        return

    # Подгружаем per-chat курсоры одним запросом.
    state_res = await db.execute(
        select(SubscriptionChatState)
        .where(SubscriptionChatState.subscription_id == sub.id)
    )
    state_by_key = {s.chat_key: s for s in state_res.scalars().all()}

    # Накопители для агрегированных метрик по группе.
    per_chat_results: list[dict] = []
    total_input_tokens = 0
    total_output_tokens = 0
    total_thinking_tokens = 0
    total_matches_written = 0
    total_digest_events_written = 0
    total_messages_fetched = 0
    total_messages_sent = 0
    total_context_chars = 0
    total_answer_chars = 0
    total_tokens_charged = 0
    primary_tokens_source = TOKENS_SOURCE_EMPTY
    chats_ok = 0
    chats_failed = 0
    chats_empty = 0

    fetch_total_ms = 0
    llm_total_ms = 0

    # Время для last_success_at в SubscriptionState — обновим только если
    # хотя бы один чат отработал успешно.
    any_success = False

    # === Pre-loop: общий парсер media-filter (один на всю группу) ===
    # Парсер sub.prompt вызываем ОДИН раз перед циклом — он не зависит
    # от чата. Это даёт ровно один LLM-вызов на парсер для всей группы,
    # а не N идентичных вызовов. Реранкер вызываем уже per-chat (его
    # вход — сообщения конкретного чата).
    media_parsed = None
    media_request = None
    if is_media_group:
        try:
            media_request = MediaFilterRequest.model_validate(media_filter_dict)
        except Exception as e:
            sub.status = "error"
            sub.last_error = f"BAD_MEDIA_FILTER: {type(e).__name__}"
            if st is None:
                st = SubscriptionState(subscription_id=sub.id)
                db.add(st)
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
            metrics["phase"] = "bad_media_filter"
            return

        parser_t0 = time.perf_counter()
        metrics["phase"] = "group_media_filter_parsing"
        try:
            parser_outcome = await parse_user_query(
                user_query=(sub.prompt or "").strip(),
                ui_window_from=now_utc - timedelta(minutes=freq_min),
                ui_window_to=None,
                selected_categories=[c.value for c in media_request.effective_categories()],
                now=now_utc,
            )
            media_parsed = parser_outcome.parsed
        except Exception as e:
            sub.status = "error"
            sub.last_error = f"MEDIA_PARSER_FAILED: {type(e).__name__}"
            if st is None:
                st = SubscriptionState(subscription_id=sub.id)
                db.add(st)
            st.last_checked_at = now_utc
            st.next_run_at = now_utc + timedelta(minutes=RETRY_MINUTES)
            metrics["phase"] = "group_media_parser_failed"
            return
        llm_total_ms += _ms_since(parser_t0)

        # Учёт токенов парсера — отдельной строкой "__parser__" в per_chat,
        # чтобы был виден в admin-логе и не путался с per-chat реранкерами.
        if parser_outcome.llm_result is not None:
            p_usage = parser_outcome.llm_result.usage or LlmUsage(0, 0, 0, TOKENS_SOURCE_EMPTY)
            p_in = p_usage.input_tokens or 0
            p_out = p_usage.output_tokens or 0
            p_think = getattr(p_usage, "thinking_tokens", 0) or 0
            total_input_tokens += p_in
            total_output_tokens += p_out
            total_thinking_tokens += p_think
            if p_usage.tokens_source and primary_tokens_source == TOKENS_SOURCE_EMPTY:
                primary_tokens_source = p_usage.tokens_source
            p_slug = parser_outcome.llm_result.used_model.slug
            p_rates = await get_token_rates(db, p_slug)
            if p_rates is None or (not p_in and not p_out):
                p_tokens = 1
            else:
                p_tokens = billing.compute_tokens_for_llm_call(
                    input_tokens=p_in,
                    output_tokens=p_out,
                    thinking_tokens=p_think,
                    in_per_1k=p_rates.in_per_1k,
                    out_per_1k=p_rates.out_per_1k,
                )
            total_tokens_charged += p_tokens
            per_chat_results.append({
                "chat_ref": None,
                "chat_id": None,
                "chat_title": None,
                "stage": "media_parser_shared",
                "status": "ok",
                "messages_fetched": 0,
                "matches_written": 0,
                "input_tokens": p_in,
                "output_tokens": p_out,
                "thinking_tokens": p_think or None,
                "tokens_charged": p_tokens,
                "error_code": None,
                "error_message": None,
            })

    for chat in chats:
        chat_state = state_by_key.get(chat.chat_ref)
        if chat_state is None:
            chat_state = SubscriptionChatState(
                subscription_id=sub.id,
                chat_key=chat.chat_ref,
                last_message_id=None,
                last_success_at=None,
                last_error=None,
            )
            db.add(chat_state)
            state_by_key[chat.chat_ref] = chat_state

        last_msg_id = chat_state.last_message_id

        if sub_type == "digest":
            since_dt = now_utc - timedelta(minutes=freq_min)
            min_id = None
        elif last_msg_id:
            since_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            min_id = int(last_msg_id)
        else:
            since_dt = now_utc - timedelta(minutes=freq_min)
            min_id = None

        chat_per: dict[str, Any] = {
            "chat_ref": chat.chat_ref,
            "chat_id": chat.chat_id,
            "chat_title": chat.chat_title,
            "status": "ok",
            "messages_fetched": 0,
            "matches_written": 0,
            "digest_events_written": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "thinking_tokens": 0,
            "tokens_charged": 0,
            "error_code": None,
            "error_message": None,
        }

        # =================================================================
        # MEDIA-FILTER ВЕТКА для группового чата
        # =================================================================
        # Используем messages.search (через fetch_chat_media) вместо
        # обычного fetch+classify. Реранкер вызываем per-chat (его вход —
        # сообщения конкретного чата), парсер уже отработал ОДИН раз
        # перед циклом (см. блок выше).
        if is_media_group:
            # Окно: при наличии курсора берём всё после min_id,
            # иначе — последний период (как в _run_media_filter_events_branch).
            if last_msg_id:
                mf_min_date_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
                mf_cursor_min_id = int(last_msg_id)
            else:
                mf_min_date_dt = now_utc - timedelta(minutes=freq_min)
                mf_cursor_min_id = None

            mf_effective_window = compute_effective_window(
                ui_window_from=mf_min_date_dt,
                ui_window_to=None,
                override=media_parsed.structured_filters.time_window_override,
            )

            # ---- messages.search ----
            mf_fetch_t0 = time.perf_counter()
            try:
                mf_fetched = await fetch_chat_media(
                    db, owner_user_id, chat.chat_ref,
                    request=media_request,
                    min_date=mf_effective_window.min_date,
                    max_date=mf_effective_window.max_date,
                    min_id=mf_cursor_min_id,
                )
            except Exception as fetch_err:
                fetch_total_ms += _ms_since(mf_fetch_t0)
                chats_failed += 1
                chat_per["status"] = "failed"
                chat_per["error_code"] = "MEDIA_FETCH_FAILED"
                chat_per["error_message"] = (str(fetch_err) or "")[:300] or None
                chat_state.last_error = chat_per["error_message"]
                per_chat_results.append(chat_per)
                print(
                    f"[subscriptions_runner] GROUP_MF sub_id={sub.id} "
                    f"chat={chat.chat_ref} fetch_failed err={fetch_err}"
                )
                await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
                continue
            fetch_total_ms += _ms_since(mf_fetch_t0)

            if mf_fetched.error_code:
                chats_failed += 1
                chat_per["status"] = "failed"
                chat_per["error_code"] = f"MEDIA_FETCH_FAILED: {mf_fetched.error_code}"
                chat_per["error_message"] = mf_fetched.error_code
                chat_state.last_error = chat_per["error_message"]
                per_chat_results.append(chat_per)
                await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
                continue

            # Кэшируем chat_id/chat_title, если впервые узнали.
            if chat.chat_id is None:
                ent_id = getattr(mf_fetched.entity, "id", None)
                if ent_id is not None:
                    chat.chat_id = int(ent_id)
                    chat_per["chat_id"] = chat.chat_id
            if not chat.chat_title:
                t = getattr(mf_fetched.entity, "title", None) or getattr(mf_fetched.entity, "username", None)
                if t:
                    chat.chat_title = t
                    chat_per["chat_title"] = t

            mf_raw = mf_fetched.messages
            chat_per["messages_fetched"] = len(mf_raw)
            total_messages_fetched += len(mf_raw)

            if not mf_raw:
                chats_empty += 1
                chat_per["status"] = "empty"
                chat_state.last_success_at = now_utc
                chat_state.last_error = None
                per_chat_results.append(chat_per)
                any_success = True
                await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
                continue

            # ---- Структурный пост-фильтр ----
            mf_after_struct = apply_structured_filters(mf_raw, media_parsed.structured_filters)

            # ---- Семантический реранкер (опц., per-chat) ----
            mf_survivors = mf_after_struct
            mf_reranker_llm_results = []
            if (
                media_parsed.needs_semantic_rerank
                and media_parsed.semantic_query
                and mf_after_struct
            ):
                rerank_t0 = time.perf_counter()
                try:
                    mf_reranker_outcome = await rerank_messages(
                        messages=mf_after_struct,
                        semantic_query=media_parsed.semantic_query,
                    )
                    mf_survivors = mf_reranker_outcome.survivors
                    mf_reranker_llm_results = mf_reranker_outcome.llm_results or []
                except Exception as rerank_err:
                    llm_total_ms += _ms_since(rerank_t0)
                    chats_failed += 1
                    chat_per["status"] = "failed"
                    chat_per["error_code"] = "MEDIA_RERANK_FAILED"
                    chat_per["error_message"] = (str(rerank_err) or "")[:300] or None
                    chat_state.last_error = chat_per["error_message"]
                    per_chat_results.append(chat_per)
                    await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
                    continue
                llm_total_ms += _ms_since(rerank_t0)

            # ---- Биллинг реранкера для этого чата ----
            ch_in_tok = 0
            ch_out_tok = 0
            ch_think_tok = 0
            ch_tokens = 0
            for r in mf_reranker_llm_results:
                if r is None:
                    continue
                slug = r.used_model.slug
                rusage = r.usage or LlmUsage(0, 0, 0, TOKENS_SOURCE_EMPTY)
                r_in = rusage.input_tokens or 0
                r_out = rusage.output_tokens or 0
                r_think = getattr(rusage, "thinking_tokens", 0) or 0
                ch_in_tok += r_in
                ch_out_tok += r_out
                ch_think_tok += r_think
                if rusage.tokens_source and primary_tokens_source == TOKENS_SOURCE_EMPTY:
                    primary_tokens_source = rusage.tokens_source
                rates = await get_token_rates(db, slug)
                if rates is None or (not r_in and not r_out):
                    ch_tokens += 1
                else:
                    ch_tokens += billing.compute_tokens_for_llm_call(
                        input_tokens=r_in,
                        output_tokens=r_out,
                        thinking_tokens=r_think,
                        in_per_1k=rates.in_per_1k,
                        out_per_1k=rates.out_per_1k,
                    )

            total_input_tokens += ch_in_tok
            total_output_tokens += ch_out_tok
            total_thinking_tokens += ch_think_tok
            total_tokens_charged += ch_tokens

            chat_per["input_tokens"] = ch_in_tok
            chat_per["output_tokens"] = ch_out_tok
            chat_per["thinking_tokens"] = ch_think_tok or None
            chat_per["tokens_charged"] = ch_tokens

            # ---- Запись MatchEvent на каждое выжившее медиа ----
            newest_in_chat = last_msg_id
            written_this_chat = 0
            try:
                for m in mf_survivors:
                    excerpt_src = (m.caption or m.text or "").strip()
                    if len(excerpt_src) > 300:
                        excerpt_src = excerpt_src[:300].rstrip() + "…"
                    kind_label = m.kind.value
                    reason = f"media:{kind_label}"
                    if m.file_size:
                        reason += f"|size={m.file_size}"
                    if m.duration_sec:
                        reason += f"|dur={m.duration_sec}s"
                    me_stmt = (
                        insert(MatchEvent)
                        .values(
                            subscription_id=sub.id,
                            chat_ref=chat.chat_ref,
                            chat_id=chat.chat_id,
                            chat_title=chat.chat_title,
                            message_id=int(m.message_id),
                            message_ts=m.date,
                            author_id=m.sender_id,
                            author_display=m.sender_username or m.sender_display_name,
                            excerpt=excerpt_src,
                            reason=reason,
                            notify_status="queued",
                            llm_payload={
                                "kind": kind_label,
                                "permalink": m.permalink,
                                "file_size": m.file_size,
                                "duration_sec": m.duration_sec,
                                "mime_type": m.mime_type,
                                "file_name": m.file_name,
                                "forwarded": bool(m.forward_info),
                                "ttl_period_sec": m.ttl_period_sec,
                            },
                        )
                        .on_conflict_do_nothing(constraint="uq_match_sub_chat_msg")
                    )
                    rmw = await db.execute(me_stmt)
                    if getattr(rmw, "rowcount", 0) == 1:
                        written_this_chat += 1
                    if m.message_id and (newest_in_chat is None or m.message_id > newest_in_chat):
                        newest_in_chat = int(m.message_id)
            except Exception as write_err:
                chats_failed += 1
                chat_per["status"] = "failed"
                chat_per["error_code"] = "DB_WRITE_FAILED"
                chat_per["error_message"] = (str(write_err) or "")[:300] or None
                chat_state.last_error = chat_per["error_message"]
                per_chat_results.append(chat_per)
                await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
                continue

            chat_per["matches_written"] = written_this_chat
            total_matches_written += written_this_chat

            # Успешный чат — обновляем курсор и last_success_at.
            if newest_in_chat:
                chat_state.last_message_id = int(newest_in_chat)
            chat_state.last_success_at = now_utc
            chat_state.last_error = None
            chats_ok += 1
            any_success = True
            per_chat_results.append(chat_per)

            await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
            continue

        # ---- Fetch ----
        fetch_t0 = time.perf_counter()
        try:
            entity, msgs = await fetch_chat_messages_for_subscription(
                db=db,
                owner_user_id=owner_user_id,
                chat_link=chat.chat_ref,
                since_dt=since_dt,
                min_id=min_id,
                limit=EVENTS_READ_LIMIT,
            )
        except Exception as fetch_err:
            fetch_total_ms += _ms_since(fetch_t0)
            chats_failed += 1
            chat_per["status"] = "failed"
            chat_per["error_code"] = "TELEGRAM_FETCH_FAILED"
            chat_per["error_message"] = (str(fetch_err) or "")[:300] or None
            chat_state.last_error = chat_per["error_message"]
            per_chat_results.append(chat_per)
            print(
                f"[subscriptions_runner] GROUP sub_id={sub.id} "
                f"chat={chat.chat_ref} fetch_failed err={fetch_err}"
            )
            # Микропауза между чатами — даже при ошибке (вдруг это flood/network).
            await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
            continue
        fetch_total_ms += _ms_since(fetch_t0)

        # Кэшируем chat_id, если впервые узнали (для приватных каналов).
        if chat.chat_id is None:
            ent_id = getattr(entity, "id", None)
            if ent_id is not None:
                chat.chat_id = int(ent_id)
                chat_per["chat_id"] = chat.chat_id

        if not chat.chat_title:
            t = getattr(entity, "title", None) or getattr(entity, "username", None)
            if t:
                chat.chat_title = t
                chat_per["chat_title"] = t

        chat_per["messages_fetched"] = len(msgs or [])
        total_messages_fetched += len(msgs or [])

        if not msgs:
            chats_empty += 1
            chat_per["status"] = "empty"
            # Курсор не двигаем (двигать некуда), last_success_at — обновим.
            chat_state.last_success_at = now_utc
            chat_state.last_error = None
            per_chat_results.append(chat_per)
            any_success = True
            await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
            continue

        ids_in_chat = [
            int(m["message_id"])
            for m in msgs
            if isinstance(m, dict) and m.get("message_id") is not None
        ]
        newest_in_chat = max(ids_in_chat) if ids_in_chat else last_msg_id
        oldest_in_chat = min(ids_in_chat) if ids_in_chat else None

        chat_title_for_llm = (
            chat.chat_title
            or getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or "Chat"
        )

        total_messages_sent += len(msgs)
        total_context_chars += _approx_context_chars(msgs)

        # ---- LLM ----
        llm_t0 = time.perf_counter()
        try:
            if sub_type == "digest":
                llm_result = await build_subscription_digest(
                    prompt=sub.prompt,
                    chat_title=chat_title_for_llm,
                    messages=msgs,
                    answer_language=owner_language,
                    ai_model=subscription_model_slug,
                    return_usage=True,
                )
            else:
                llm_result = await classify_subscription_matches(
                    prompt=sub.prompt,
                    chat_title=chat_title_for_llm,
                    messages=msgs,
                    ux_language=owner_language,
                    ai_model=subscription_model_slug,
                    return_usage=True,
                )
        except Exception as llm_err:
            llm_total_ms += _ms_since(llm_t0)
            chats_failed += 1
            chat_per["status"] = "failed"
            chat_per["error_code"] = "LLM_ERROR"
            chat_per["error_message"] = (str(llm_err) or "")[:300] or None
            chat_state.last_error = chat_per["error_message"]
            per_chat_results.append(chat_per)
            print(
                f"[subscriptions_runner] GROUP sub_id={sub.id} "
                f"chat={chat.chat_ref} llm_failed err={llm_err}"
            )
            await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
            continue
        llm_total_ms += _ms_since(llm_t0)

        usage = llm_result.usage or LlmUsage(0, 0, 0, TOKENS_SOURCE_EMPTY)
        in_tok = usage.input_tokens or 0
        out_tok = usage.output_tokens or 0
        think_tok = getattr(usage, "thinking_tokens", 0) or 0
        total_input_tokens += in_tok
        total_output_tokens += out_tok
        total_thinking_tokens += think_tok
        if usage.tokens_source and primary_tokens_source == TOKENS_SOURCE_EMPTY:
            primary_tokens_source = usage.tokens_source

        # Списание в наших токенах — точное по этому вызову.
        rates = await get_token_rates(db, subscription_model_slug)
        if rates is None or (not in_tok and not out_tok):
            tokens_for_call = 1
        else:
            tokens_for_call = billing.compute_tokens_for_llm_call(
                input_tokens=in_tok,
                output_tokens=out_tok,
                thinking_tokens=think_tok,
                in_per_1k=rates.in_per_1k,
                out_per_1k=rates.out_per_1k,
            )
        total_tokens_charged += tokens_for_call

        chat_per["input_tokens"] = in_tok
        chat_per["output_tokens"] = out_tok
        chat_per["thinking_tokens"] = think_tok or None
        chat_per["tokens_charged"] = tokens_for_call

        # ---- Запись MatchEvent / DigestEvent ----
        try:
            if sub_type == "digest":
                digest_text = ""
                confidence = None
                llm_json = llm_result.data or {}
                if isinstance(llm_json, dict):
                    digest_text = (llm_json.get("digest_text") or "").strip()
                    confidence = llm_json.get("confidence")
                if len(digest_text) > 4096:
                    digest_text = digest_text[:4096].rstrip() + "…"

                stmt = (
                    insert(DigestEvent)
                    .values(
                        subscription_id=sub.id,
                        chat_ref=chat.chat_ref,
                        chat_id=chat.chat_id,
                        chat_title=chat.chat_title,
                        window_start=since_dt,
                        window_end=now_utc,
                        start_message_id=int(oldest_in_chat) if oldest_in_chat else None,
                        end_message_id=int(newest_in_chat) if newest_in_chat else None,
                        messages_seen=len(msgs),
                        digest_text=digest_text,
                        llm_payload={"confidence": confidence} if confidence is not None else None,
                        notify_status="queued",
                    )
                    .on_conflict_do_nothing(index_elements=None, constraint="uq_digest_sub_chat_endmsg")
                )
                r = await db.execute(stmt)
                written = 1 if getattr(r, "rowcount", 0) == 1 else 0
                chat_per["digest_events_written"] = written
                total_digest_events_written += written
                total_answer_chars += len(digest_text)
            else:
                # events
                msg_by_id = {
                    int(m["message_id"]): m
                    for m in msgs
                    if isinstance(m, dict) and m.get("message_id") is not None
                }
                llm_json = llm_result.data or {}
                matches = (llm_json.get("matches") or []) if isinstance(llm_json, dict) else []
                written_this_chat = 0
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
                    total_answer_chars += len(excerpt) + len(reason or "")

                    me_stmt = (
                        insert(MatchEvent)
                        .values(
                            subscription_id=sub.id,
                            chat_ref=chat.chat_ref,
                            chat_id=chat.chat_id,
                            chat_title=chat.chat_title,
                            message_id=int(mid),
                            message_ts=ts,
                            author_id=author_id,
                            author_display=author_display,
                            excerpt=excerpt,
                            reason=reason,
                            notify_status="queued",
                            llm_payload=None,
                        )
                        .on_conflict_do_nothing(constraint="uq_match_sub_chat_msg")
                    )
                    r = await db.execute(me_stmt)
                    if getattr(r, "rowcount", 0) == 1:
                        written_this_chat += 1
                chat_per["matches_written"] = written_this_chat
                total_matches_written += written_this_chat
        except Exception as write_err:
            chats_failed += 1
            chat_per["status"] = "failed"
            chat_per["error_code"] = "DB_WRITE_FAILED"
            chat_per["error_message"] = (str(write_err) or "")[:300] or None
            chat_state.last_error = chat_per["error_message"]
            per_chat_results.append(chat_per)
            await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)
            continue

        # Успешный чат — обновляем курсор и last_success_at.
        if newest_in_chat:
            chat_state.last_message_id = int(newest_in_chat)
        chat_state.last_success_at = now_utc
        chat_state.last_error = None
        chats_ok += 1
        any_success = True
        per_chat_results.append(chat_per)

        await asyncio.sleep(GROUP_INTER_CHAT_SLEEP_SEC)

    # ---- Финальный апдейт state и метрик ----
    if st is None:
        st = SubscriptionState(subscription_id=sub.id)
        db.add(st)

    if any_success:
        st.last_success_at = now_utc
    st.last_checked_at = now_utc
    st.next_run_at = now_utc + timedelta(minutes=freq_min)

    metrics["messages_fetched_count"] = total_messages_fetched
    metrics["messages_sent_to_llm_count"] = total_messages_sent
    metrics["context_chars"] = total_context_chars or None
    metrics["answer_chars"] = total_answer_chars or None
    metrics["matches_written"] = total_matches_written
    metrics["digest_events_written"] = total_digest_events_written
    metrics["fetch_duration_ms"] = fetch_total_ms
    metrics["llm_duration_ms"] = llm_total_ms
    metrics["group_size"] = len(chats)
    metrics["chats_ok"] = chats_ok
    metrics["chats_failed"] = chats_failed
    metrics["chats_empty"] = chats_empty
    metrics["per_chat_results"] = per_chat_results
    if is_media_group:
        metrics["is_media_filter"] = True

    # Если ни одного успеха — поднимаем ошибку наружу, чтобы run_tick
    # написал subscription_run_failed и поставил retry.
    if not any_success:
        raise RuntimeError(
            f"GROUP_ALL_CHATS_FAILED chats={len(chats)} failed={chats_failed}"
        )

    # Накопленные токены и tokens_charged_precomputed — это сигнал
    # _record_subscription_run_success_same_session: не пересчитывать
    # стоимость по тарифам, а уважать наше число (точное per-chat).
    if total_input_tokens or total_output_tokens or total_tokens_charged:
        metrics["llm_usage"] = LlmUsage(
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            total_tokens=total_input_tokens + total_output_tokens,
            tokens_source=primary_tokens_source,
            thinking_tokens=total_thinking_tokens,
        )
        # Reuse существующего поля precomputed — уже понимается биллингом.
        metrics["mf_tokens_charged_precomputed"] = total_tokens_charged

    # Если в группе не было ни одного LLM-вызова (все чаты empty) —
    # не пишем UsageEvent, чтобы не плодить пустых записей (как и
    # в одиночной ветке). Иначе — общая запись с per-chat расшифровкой.
    if chats_ok > 0 and (total_input_tokens or total_output_tokens or total_tokens_charged):
        await _record_subscription_run_success_same_session(db, metrics)


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
