from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime


class SavedQueryCreate(BaseModel):
    """Создание пресета. params_json — снапшот настроек формы запроса
    (формат зеркалит payload /tg/analyze_chat и /tg/analyze_chats_group,
    см. SavedQuery.params_json в db/models.py). Глубоко не валидируем —
    фронт сериализует, фронт же defensive-парсит при применении."""
    name: str = Field(min_length=1, max_length=255)
    params_json: dict[str, Any]


class SavedQueryUpdate(BaseModel):
    """Частичное обновление: переименование и/или перезапись настроек.
    Оба поля опциональны — можно поменять только имя или только params."""
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    params_json: Optional[dict[str, Any]] = None


class SavedQueryOut(BaseModel):
    id: int
    name: str
    params_json: dict[str, Any]
    last_used_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
