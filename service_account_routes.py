import time as _time

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy as sa

from auth import get_current_user as auth_get_current_user
from db.models import User, UserChatHistory
from db.session import get_db
from service_account_service import (
    ServiceAccountError,
    analyze_chat_via_service_account,
    normalize_public_chat_ref,
)
from plan_limits import (
    enforce_qa_limits,
    record_qa_success,
    record_qa_failure,
    build_usage_snapshot,
    resolve_ai_model_for_user,
)
from llm import (
    estimate_llm_cost_usd,
    split_usage_for_meta,
    cost_kwargs_for_meta,
    LlmUsage,
    TOKENS_SOURCE_EMPTY,
)

router = APIRouter()


class ServiceAnalyzeRequest(BaseModel):
    chat_link: str = Field(min_length=1)
    user_query: str = Field(default="")
    days: int = Field(default=7, ge=1, le=30)
    ai_model: str | None = None


@router.post("/tg/service/analyze_chat")
async def tg_service_analyze_chat(
    payload: ServiceAnalyzeRequest,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")

    # enforce_qa_limits writes qa_request_rejected itself on 429.
    await enforce_qa_limits(
        db,
        user=user,
        requested_days=payload.days,
        source_mode="service",
        chat_ref=payload.chat_link,
    )

    ai_model = resolve_ai_model_for_user(
        user=user,
        requested_ai_model=payload.ai_model,
        fallback_ai_model=getattr(user, "default_ai_model", None),
    )

    query_chars = len((payload.user_query or "").strip())

    total_t0 = _time.perf_counter()

    try:
        result = await analyze_chat_via_service_account(
            db,
            chat_link=payload.chat_link,
            user_query=payload.user_query.strip(),
            days=payload.days,
            ai_model=ai_model,
            fallback_language=user.language,
        )
    except ServiceAccountError as e:
        # Telegram/service-account failure — no LLM call happened (or LLM
        # internally raised the same ServiceAccountError shape — currently
        # only the fetch path raises this). Log qa_request_failed without
        # incrementing the counter, then re-raise as HTTPException.
        total_ms = int((_time.perf_counter() - total_t0) * 1000)
        await record_qa_failure(
            db,
            user=user,
            source_mode="service",
            chat_ref=payload.chat_link,
            requested_days=payload.days,
            ai_model=ai_model,
            error_code=str(e.code or "SERVICE_ACCOUNT_ERROR"),
            error_message=(e.user_message or "")[:300] or None,
            query_chars=query_chars or None,
            duration_ms_total=total_ms,
        )
        await db.commit()
        raise HTTPException(
            status_code=e.http_status,
            detail={"code": e.code, "message": e.user_message},
        )
    except Exception as e:
        # Unknown failure (network blip, DB error, etc.) — log generic
        # qa_request_failed so the admin tab still sees it.
        total_ms = int((_time.perf_counter() - total_t0) * 1000)
        await record_qa_failure(
            db,
            user=user,
            source_mode="service",
            chat_ref=payload.chat_link,
            requested_days=payload.days,
            ai_model=ai_model,
            error_code="INTERNAL_ERROR",
            error_message=(str(e) or "")[:300] or None,
            query_chars=query_chars or None,
            duration_ms_total=total_ms,
        )
        await db.commit()
        raise

    total_ms = int((_time.perf_counter() - total_t0) * 1000)

    # Pop internal metrics — never send them to the frontend.
    qa_metrics = result.pop("_qa_metrics", None) or {}
    llm_usage: LlmUsage = qa_metrics.get("llm_usage") or LlmUsage(0, 0, 0, TOKENS_SOURCE_EMPTY)

    # Cost estimation. Safe against missing llm_pricing table.
    cost = await estimate_llm_cost_usd(
        db,
        ai_model=ai_model,
        input_tokens=llm_usage.input_tokens,
        output_tokens=llm_usage.output_tokens,
        tokens_source=llm_usage.tokens_source,
    )

    # Chat history upsert (unchanged behavior).
    normalized_ref = result.get("chat_ref_normalized") or normalize_public_chat_ref(payload.chat_link)
    chat_title = (result.get("chat_name") or "").strip() or None
    chat_username = (result.get("chat_username") or "").strip() or None
    chat_id = result.get("chat_id")

    stmt = (
        insert(UserChatHistory)
        .values(
            owner_user_id=user.id,
            source_mode="service",
            chat_ref=(payload.chat_link or "").strip(),
            chat_ref_normalized=normalized_ref,
            chat_title=chat_title,
            chat_username=chat_username,
            chat_id=chat_id,
            last_accessed_at=sa.func.now(),
        )
        .on_conflict_do_update(
            constraint="uq_user_chat_history_owner_source_ref",
            set_={
                "chat_ref": (payload.chat_link or "").strip(),
                "chat_title": chat_title,
                "chat_username": chat_username,
                "chat_id": chat_id,
                "last_accessed_at": sa.func.now(),
                "updated_at": sa.func.now(),
            },
        )
    )

    await record_qa_success(
        db,
        user=user,
        source_mode="service",
        chat_ref=payload.chat_link,
        requested_days=payload.days,
        ai_model=ai_model,
        query_chars=query_chars or None,
        messages_fetched_count=qa_metrics.get("messages_fetched_count"),
        messages_sent_to_llm_count=qa_metrics.get("messages_sent_to_llm_count"),
        context_chars=qa_metrics.get("context_chars"),
        answer_chars=qa_metrics.get("answer_chars"),
        duration_ms_total=total_ms,
        duration_ms_fetch=qa_metrics.get("fetch_duration_ms"),
        duration_ms_llm=qa_metrics.get("llm_duration_ms"),
        **split_usage_for_meta(llm_usage),
        **cost_kwargs_for_meta(cost),
    )

    await db.execute(stmt)
    await db.commit()

    result["usage"] = await build_usage_snapshot(db, user=user)
    result["ai_model"] = ai_model
    return result
