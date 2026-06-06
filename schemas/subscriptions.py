from pydantic import BaseModel, Field
from typing import Any, Optional, Literal
from datetime import datetime

SourceMode = Literal["personal", "service"]
SubscriptionType = Literal["events", "digest"]

class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    source_mode: SourceMode
    chat_ref: str = Field(min_length=1)
    frequency_minutes: int = Field(ge=5, le=7 * 24 * 60)
    # Текст запроса. Для медиа-подписки (media_filter != None) опционален;
    # min_length=1 убран, чтобы можно было создать «чистый» медиа-фильтр
    # без свободного текста. Для обычной events/digest подписки в endpoint'е
    # main.py есть отдельная валидация на непустой prompt.
    prompt: str = ""

    subscription_type: Optional[SubscriptionType] = None
    ai_model: Optional[str] = "openai:gpt-4.1-mini"

    # Параметры медиафильтра — структура совпадает с
    # backend/media_filter/types.py MediaFilterRequest. Не валидируем
    # здесь жёстко (Any), потому что детальная валидация происходит в
    # mf_integration.request_from_payload — она терпима к неполным данным.
    media_filter: Optional[dict[str, Any]] = None

    is_active: bool = True


class SubscriptionOut(BaseModel):
    id: int
    owner_user_id: Optional[int]
    name: str
    source_mode: str
    chat_ref: str
    chat_id: Optional[int]
    frequency_minutes: int
    prompt: str
    ai_model: str

    is_active: bool
    status: str
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime
    subscription_type: Optional[str] = None

    media_filter: Optional[dict[str, Any]] = None

    is_trial: bool = False
    trial_started_at: Optional[datetime] = None
    trial_ends_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ToggleRequest(BaseModel):
    is_active: bool