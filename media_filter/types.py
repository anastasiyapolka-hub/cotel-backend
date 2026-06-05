"""
Контракты данных для медиафильтра.

Здесь только модели и enum'ы — никакой логики, никаких побочных эффектов.
Импортируется из main.py для валидации payload'а, из telethon_search.py
для перевода в фильтры MTProto, из llm_parser.py для JSON-schema ответа
LLM, из formatter.py для типизации карточек ответа.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Категории и подтипы медиа (то, что выбирает пользователь в UI)
# ---------------------------------------------------------------------------


class MediaCategory(str, Enum):
    """Верхнеуровневая категория, чекбокс в UI."""
    VIDEO = "video"
    PHOTO = "photo"
    AUDIO = "audio"
    DOCUMENT = "document"
    URL = "url"


class VideoSubtype(str, Enum):
    """Подтип для категории Видео (выпадающий список в UI)."""
    FILES = "video_files"           # обычные видеофайлы
    ROUND = "video_round"           # видеокружки
    FILES_AND_ROUND = "video_both"  # оба сразу


class AudioSubtype(str, Enum):
    """Подтип для категории Аудио (выпадающий список в UI)."""
    FILES = "audio_files"            # музыкальные аудиофайлы
    VOICE = "audio_voice"            # голосовые сообщения
    FILES_AND_VOICE = "audio_both"   # оба сразу


class MediaItemKind(str, Enum):
    """
    Атомарный тип медиа конкретного сообщения, который МЫ присваиваем
    после fetch'а. В одну категорию UI могут попасть несколько kind'ов
    (например, Видео.«оба» → kind=video_file + kind=video_round).

    Используется в карточках ответа для группировки секций.
    """
    VIDEO_FILE = "video_file"
    VIDEO_ROUND = "video_round"
    PHOTO = "photo"
    AUDIO_FILE = "audio_file"
    VOICE = "voice"
    DOCUMENT = "document"
    URL = "url"


# Порядок секций в финальном ответе — зафиксирован в спеке.
MEDIA_KIND_DISPLAY_ORDER: tuple[MediaItemKind, ...] = (
    MediaItemKind.VIDEO_FILE,
    MediaItemKind.VIDEO_ROUND,
    MediaItemKind.PHOTO,
    MediaItemKind.AUDIO_FILE,
    MediaItemKind.VOICE,
    MediaItemKind.DOCUMENT,
    MediaItemKind.URL,
)


# ---------------------------------------------------------------------------
# Запрос пользователя: что приходит из UI
# ---------------------------------------------------------------------------


class MediaFilterRequest(BaseModel):
    """
    Содержимое поля `media_filter` в payload'е /tg/analyze_chat и
    /tg/analyze_chats_group. Отсутствие этого поля = медиафильтр выключен,
    идём обычным Q&A-путём.

    `categories` — пустой список означает «все типы» (поведение «галочка
    включена, но конкретные категории не выбраны» по спеке).
    `video_subtype` и `audio_subtype` имеют смысл только если в
    `categories` есть VIDEO / AUDIO соответственно. Иначе игнорируются.
    """
    enabled: bool = True
    categories: list[MediaCategory] = Field(default_factory=list)
    video_subtype: VideoSubtype = VideoSubtype.FILES
    audio_subtype: AudioSubtype = AudioSubtype.FILES

    @field_validator("categories", mode="before")
    @classmethod
    def _dedup_categories(cls, v):
        if not isinstance(v, list):
            return v
        seen: set[str] = set()
        out: list = []
        for item in v:
            key = item.value if isinstance(item, MediaCategory) else str(item)
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def effective_categories(self) -> list[MediaCategory]:
        """Если пользователь ничего не выбрал — берём все категории."""
        return list(self.categories) if self.categories else list(MediaCategory)


# ---------------------------------------------------------------------------
# Контракт LLM-парсера (вызов №1): структурные ограничения из текста
# ---------------------------------------------------------------------------


class TimeWindowOverride(BaseModel):
    """
    Переуточнение временного окна из текста пользователя.
    Заполняется ТОЛЬКО если в тексте явно указано окно.
    Применяется поверх UI-окна только если получившееся окно уже.
    """
    from_iso: Optional[datetime] = None
    to_iso: Optional[datetime] = None


class StructuredFilters(BaseModel):
    """
    Структурные ограничения, которые LLM-парсер достаёт из свободного
    текста пользователя. Применяются детерминированным пост-фильтром на
    нашей стороне, БЕЗ повторного вызова LLM.

    Все поля nullable: null = «не задано пользователем».
    """
    file_size_min_bytes: Optional[int] = None
    file_size_max_bytes: Optional[int] = None
    duration_min_sec: Optional[int] = None
    duration_max_sec: Optional[int] = None
    width_min_px: Optional[int] = None
    height_min_px: Optional[int] = None
    sender_username: Optional[str] = None
    mime_type_contains: Optional[str] = None
    file_name_contains: Optional[str] = None
    time_window_override: TimeWindowOverride = Field(default_factory=TimeWindowOverride)

    def is_empty(self) -> bool:
        """True если ни одного структурного ограничения не задано."""
        if self.time_window_override.from_iso or self.time_window_override.to_iso:
            return False
        return all(
            getattr(self, f) is None
            for f in (
                "file_size_min_bytes", "file_size_max_bytes",
                "duration_min_sec", "duration_max_sec",
                "width_min_px", "height_min_px",
                "sender_username", "mime_type_contains", "file_name_contains",
            )
        )


class ParsedUserQuery(BaseModel):
    """Полный ответ LLM-парсера. Строго совпадает с JSON-schema промпта."""
    structured_filters: StructuredFilters = Field(default_factory=StructuredFilters)
    semantic_query: Optional[str] = None
    needs_semantic_rerank: bool = False


# ---------------------------------------------------------------------------
# Нормализованное сообщение из Telegram (после fetch'а)
# ---------------------------------------------------------------------------


class ForwardInfo(BaseModel):
    """Информация о пересылке. None если сообщение оригинальное."""
    from_chat_title: Optional[str] = None
    from_chat_username: Optional[str] = None
    from_sender_name: Optional[str] = None


class MediaMessage(BaseModel):
    """
    Нормализованное представление одного сообщения с медиа после
    извлечения из Telegram. Поля заполняются по максимуму того, что
    Telegram отдаёт — даже если карточка их не покажет, они нужны
    LLM-реранкеру и пост-фильтру.
    """
    # Идентификаторы
    message_id: int
    chat_id: int
    chat_title: str
    chat_username: Optional[str] = None
    permalink: str

    # Время + автор
    date: datetime
    sender_id: Optional[int] = None
    sender_username: Optional[str] = None
    sender_display_name: Optional[str] = None

    # Содержимое
    text: Optional[str] = None       # для сообщений-ссылок
    caption: Optional[str] = None    # подпись под медиа

    # Тип
    kind: MediaItemKind

    # Файловые поля (применимы к большинству kind'ов)
    mime_type: Optional[str] = None
    file_size: Optional[int] = None      # в байтах
    file_name: Optional[str] = None

    # Видео / голос / аудио
    duration_sec: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    supports_streaming: Optional[bool] = None

    # Только аудио
    performer: Optional[str] = None
    title: Optional[str] = None

    # Только ссылки (kind=URL)
    extracted_urls: list[str] = Field(default_factory=list)

    # Контекст и пометки
    forward_info: Optional[ForwardInfo] = None
    reply_to_msg_id: Optional[int] = None
    views: Optional[int] = None
    forwards_count: Optional[int] = None
    has_spoiler: bool = False
    ttl_period_sec: Optional[int] = None  # для самоудаляющихся


# ---------------------------------------------------------------------------
# Контракт ответа: карточки, секции, группировка
# ---------------------------------------------------------------------------


class MediaCard(BaseModel):
    """
    Одна карточка в ответе. Фронт рендерит её по `kind` — какие поля
    показывать, что прятать. Бэк просто отдаёт всё, что есть.
    """
    message_id: int
    chat_id: int
    chat_title: str
    permalink: str
    date_iso: str
    sender_label: str           # «@username» или display name
    caption: Optional[str] = None
    kind: MediaItemKind

    # Поля, специфичные для kind'а — фронт выбирает что показать
    file_size: Optional[int] = None
    duration_sec: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    mime_type: Optional[str] = None
    file_name: Optional[str] = None
    performer: Optional[str] = None
    title: Optional[str] = None
    extracted_urls: list[str] = Field(default_factory=list)

    # Флажки для UI
    is_forwarded: bool = False
    forward_label: Optional[str] = None     # «forwarded from …»
    ttl_period_sec: Optional[int] = None
    has_spoiler: bool = False


class MediaSection(BaseModel):
    """Секция в карточном ответе: одна категория, упорядоченные карточки."""
    kind: MediaItemKind
    count: int
    cards: list[MediaCard]


class MediaChatBlock(BaseModel):
    """Блок одного чата в групповом ответе (содержит секции по типам)."""
    chat_link: str
    chat_title: str
    chat_username: Optional[str] = None
    total_count: int
    sections: list[MediaSection]
    error_code: Optional[str] = None    # если чат недоступен / упал


class MediaFilterAnswer(BaseModel):
    """
    Финальная структура ответа из media_filter-ветки.

    Один из двух разделов заполнен:
      • `sections` для одиночного запроса (группировка только по типу),
      • `chat_blocks` для группового (по чату → по типу).
    """
    is_group: bool
    total_count: int
    sections: Optional[list[MediaSection]] = None
    chat_blocks: Optional[list[MediaChatBlock]] = None

    # Метаинформация для UI — короткий текст-заголовок выше карточек.
    headline: Optional[str] = None
