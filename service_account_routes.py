from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user as auth_get_current_user
from db.models import User
from db.session import get_db
from service_account_service import (
    ServiceAccountError,
    analyze_chat_via_service_account,
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
        return result

    except ServiceAccountError as e:
        raise HTTPException(
            status_code=e.http_status,
            detail={
                "code": e.code,
                "message": e.user_message,
            },
        )