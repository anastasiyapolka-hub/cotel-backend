from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime

SourceMode = Literal["personal", "service"]
SubscriptionType = Literal["events", "digest"]

class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    source_mode: SourceMode
    chat_ref: str = Field(min_length=1)
    frequency_minutes: int = Field(ge=5, le=7*24*60)  # от 5 минут до 7 дней
    prompt: str = Field(min_length=1)

    subscription_type: Optional[SubscriptionType] = None
    owner_user_id: Optional[int] = None  # пока можно не использовать
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
    is_active: bool
    status: str
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime
    subscription_type: Optional[str] = None

    class Config:
        from_attributes = True

class ToggleRequest(BaseModel):
    is_active: bool
