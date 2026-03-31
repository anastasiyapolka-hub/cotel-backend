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

router = APIRouter()


class ServiceAnalyzeRequest(BaseModel):
    chat_link: str = Field(min_length=1)
    user_query: str = Field(default="")
    days: int = Field(default=7, ge=1, le=30)


@router.post("/tg/service/analyze_chat")
async def tg_service_analyze_chat(
    payload: ServiceAnalyzeRequest,
    user: User = Depends(auth_get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # user нужен для общей авторизации в CoTel и будущего учёта лимитов
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")

    try:
        result = await analyze_chat_via_service_account(
            db,
            chat_link=payload.chat_link,
            user_query=payload.user_query.strip(),
            days=payload.days,
        )

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

        await db.execute(stmt)
        await db.commit()

        return result

    except ServiceAccountError as e:
        raise HTTPException(
            status_code=e.http_status,
            detail={
                "code": e.code,
                "message": e.user_message,
            },
        )