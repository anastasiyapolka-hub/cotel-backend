"""
Telethon search-слой медиафильтра.

Тянет из Telegram только медиа-сообщения выбранных пользователем
категорий за указанное временное окно, нормализуя их в MediaMessage.

Архитектурные решения:

  • Используем `client.iter_messages(entity, filter=X)`, итерируя от
    новых к старым и обрываясь, когда дошли до сообщений старше окна.
    Это обходит баг Telethon #1124 (offset_date + filter): мы не задаём
    offset_date вообще, окно строится естественным порядком итерации.
    Для верхней границы окна (max_date) пропускаем сообщения новее
    границы, прежде чем начать собирать. Подробности — в
    architecture-media-filter.md, раздел "Telethon".

  • Combined-подтипы (Видео.«оба», Аудио.«оба») — это ДВА параллельных
    iter_messages с разными InputMessagesFilter*, потом merge.

  • Параллельность по чатам и по фильтрам — asyncio.gather с
    Semaphore (MAX_PARALLEL_FETCHES) против FloodWait.

  • FloodWaitError → ждём указанные секунды и ретраим (один раз),
    дальше пробрасываем как PER_CHAT_FAILED.

  • Per-chat ошибки (ChannelPrivate, ChatAdminRequired, AuthKey…) не
    падают весь запрос — возвращаются в виде объектов `ChatFetchResult`
    с заполненным `error_code`. Оркестратор отдаёт частичный успех.

Возвращаемая структура для одного чата:

    ChatFetchResult(
        chat_link=...,
        entity=<Telethon entity>,
        messages=[MediaMessage, ...],     # пусто если error_code не None
        error_code=None | "PRIVATE" | "ADMIN_REQUIRED" | "NOT_FOUND" |
                   "FLOOD_WAIT" | "RESOLVE_FAILED" | "FETCH_FAILED",
    )
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from telethon import errors as tg_errors
from telethon.tl.types import (
    Channel,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputMessagesFilterDocument,
    InputMessagesFilterMusic,
    InputMessagesFilterPhotos,
    InputMessagesFilterRoundVideo,
    InputMessagesFilterUrl,
    InputMessagesFilterVideo,
    InputMessagesFilterVoice,
    Message,
    MessageEntityTextUrl,
    MessageEntityUrl,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)
from sqlalchemy.ext.asyncio import AsyncSession

# Импорты внутри проекта — лежат в backend/ и работают из cwd запуска main.py.
# Те же пути используются всеми существующими модулями (см. main.py imports).
from telegram_service import (  # type: ignore[import-not-found]
    build_message_permalink,
    ensure_connected,
    resolve_entity_with_invite,
)

from .types import (
    AudioSubtype,
    ForwardInfo,
    MediaCategory,
    MediaFilterRequest,
    MediaItemKind,
    MediaMessage,
    VideoSubtype,
)


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Константы и конфигурация
# ---------------------------------------------------------------------------


# Максимальное количество одновременных fetch-запросов к Telegram через
# одного клиента. Telegram банит за частые запросы; 5 — консервативный
# баланс между скоростью группового запроса и риском FloodWait.
MAX_PARALLEL_FETCHES = 5

# Сколько раз ретраим FloodWait в пределах одной fetch-операции.
# Дольше — пропускаем, чат уходит в error_code=FLOOD_WAIT.
FLOOD_WAIT_RETRIES = 1
# Максимальная длительность FloodWait, которую мы готовы переждать (сек).
# Дольше — отдаём ошибку, пусть пользователь повторит позже.
FLOOD_WAIT_MAX_SLEEP_SEC = 30

# Telethon iter_messages с filter использует messages.Search; limit
# управляет ВЕРХНИМ числом возвращаемых сообщений. None = «все», но
# мы всё равно прерываемся, когда хронологически вышли из окна.
# Оставляем None по требованию пользователя: лимиты регулируются тарифом
# (period_seconds, max_chats_per_group_request), а не нашим хард-капом.
ITER_LIMIT = None


# Маппинг "категория + подтип" → список InputMessagesFilter* классов.
# Если в списке 2 элемента — это combined-подтип, делаем 2 запроса и
# мерджим. Внутри dict — нестрогий контракт, ключи строятся из
# (MediaCategory, subtype-или-None).
def _filters_for_category(
    category: MediaCategory,
    video_subtype: VideoSubtype,
    audio_subtype: AudioSubtype,
) -> list[tuple[type, MediaItemKind]]:
    """
    Возвращает список (Telethon filter class, MediaItemKind) для одной
    категории UI. У combined-подтипов длина 2, у остальных — 1.

    Kind в кортеже — это атомарный тип, который мы присвоим всем
    сообщениям, пришедшим из этого конкретного фильтра. Это надёжнее,
    чем угадывать kind по msg.media post-factum (например, кружок и
    обычное видео технически оба MessageMediaDocument с разными
    attributes — но в SearchRequest мы уже точно знаем, какой фильтр
    они прошли).
    """
    if category == MediaCategory.VIDEO:
        if video_subtype == VideoSubtype.FILES:
            return [(InputMessagesFilterVideo, MediaItemKind.VIDEO_FILE)]
        if video_subtype == VideoSubtype.ROUND:
            return [(InputMessagesFilterRoundVideo, MediaItemKind.VIDEO_ROUND)]
        # FILES_AND_ROUND → два запроса, два разных kind'а
        return [
            (InputMessagesFilterVideo, MediaItemKind.VIDEO_FILE),
            (InputMessagesFilterRoundVideo, MediaItemKind.VIDEO_ROUND),
        ]

    if category == MediaCategory.PHOTO:
        return [(InputMessagesFilterPhotos, MediaItemKind.PHOTO)]

    if category == MediaCategory.AUDIO:
        if audio_subtype == AudioSubtype.FILES:
            return [(InputMessagesFilterMusic, MediaItemKind.AUDIO_FILE)]
        if audio_subtype == AudioSubtype.VOICE:
            return [(InputMessagesFilterVoice, MediaItemKind.VOICE)]
        return [
            (InputMessagesFilterMusic, MediaItemKind.AUDIO_FILE),
            (InputMessagesFilterVoice, MediaItemKind.VOICE),
        ]

    if category == MediaCategory.DOCUMENT:
        return [(InputMessagesFilterDocument, MediaItemKind.DOCUMENT)]

    if category == MediaCategory.URL:
        return [(InputMessagesFilterUrl, MediaItemKind.URL)]

    # Не должно случаться — enum закрытый.
    return []


def build_filter_plan(
    request: MediaFilterRequest,
) -> list[tuple[type, MediaItemKind]]:
    """
    Развернуть запрос пользователя в плоский список (filter_class, kind)
    для одного чата. Без дублей (на случай если в request.categories
    случайно попали повторы или overlapping подтипы).

    Спец-случай "все типы": если categories пуст, расширяем Видео и
    Аудио до combined-подтипа (видеофайлы + кружки, аудиофайлы +
    голосовые), чтобы выполнить требование "галочка включена,
    подтипы не выбраны = любое медиа".
    """
    if not request.categories:
        # «Все типы» — игнорируем переданные subtype'ы (которые в этом
        # случае равны дефолтам) и принудительно берём combined.
        video_subtype = VideoSubtype.FILES_AND_ROUND
        audio_subtype = AudioSubtype.FILES_AND_VOICE
    else:
        video_subtype = request.video_subtype
        audio_subtype = request.audio_subtype

    plan: list[tuple[type, MediaItemKind]] = []
    seen_kinds: set[MediaItemKind] = set()
    for category in request.effective_categories():
        for filter_cls, kind in _filters_for_category(
            category, video_subtype, audio_subtype
        ):
            if kind in seen_kinds:
                continue
            plan.append((filter_cls, kind))
            seen_kinds.add(kind)
    return plan


# ---------------------------------------------------------------------------
# Результаты
# ---------------------------------------------------------------------------


@dataclass
class ChatFetchResult:
    """Результат fetch'а одного чата. error_code != None → fetched пустой."""
    chat_link: str
    entity: object = None
    chat_title: Optional[str] = None
    chat_username: Optional[str] = None
    messages: list[MediaMessage] = field(default_factory=list)
    error_code: Optional[str] = None
    error_detail: Optional[str] = None


# ---------------------------------------------------------------------------
# Извлечение метаданных из Telethon Message
# ---------------------------------------------------------------------------


_URL_RE = re.compile(r"https?://\S+")


def _extract_document_metadata(doc) -> dict:
    """
    Из telethon.tl.types.Document достаём mime, size, file_name,
    duration, width/height, performer/title, supports_streaming.
    """
    out: dict = {
        "mime_type": getattr(doc, "mime_type", None),
        "file_size": getattr(doc, "size", None),
        "file_name": None,
        "duration_sec": None,
        "width": None,
        "height": None,
        "supports_streaming": None,
        "performer": None,
        "title": None,
    }
    for attr in getattr(doc, "attributes", []) or []:
        if isinstance(attr, DocumentAttributeFilename):
            out["file_name"] = getattr(attr, "file_name", None)
        elif isinstance(attr, DocumentAttributeVideo):
            out["duration_sec"] = int(getattr(attr, "duration", 0) or 0) or None
            out["width"] = getattr(attr, "w", None)
            out["height"] = getattr(attr, "h", None)
            out["supports_streaming"] = getattr(attr, "supports_streaming", False)
        elif isinstance(attr, DocumentAttributeAudio):
            out["duration_sec"] = int(getattr(attr, "duration", 0) or 0) or None
            out["performer"] = getattr(attr, "performer", None)
            out["title"] = getattr(attr, "title", None)
    return out


def _extract_photo_size(media: MessageMediaPhoto) -> dict:
    """
    Достать лучшее (width, height) и приблизительный size из
    photo.sizes. Telegram возвращает несколько PhotoSize-вариантов;
    берём максимальный по площади.
    """
    out = {"width": None, "height": None, "file_size": None}
    photo = getattr(media, "photo", None)
    if photo is None:
        return out
    sizes = getattr(photo, "sizes", None) or []
    best_w = best_h = 0
    best_bytes = None
    for s in sizes:
        w = getattr(s, "w", 0) or 0
        h = getattr(s, "h", 0) or 0
        if w * h > best_w * best_h:
            best_w, best_h = w, h
            # size есть только у PhotoSize, не у PhotoSizeProgressive
            best_bytes = getattr(s, "size", None) or (
                sum(getattr(s, "sizes", []) or []) or None
            )
    out["width"] = best_w or None
    out["height"] = best_h or None
    out["file_size"] = best_bytes
    return out


def _extract_urls(msg: Message) -> list[str]:
    """
    Собрать ссылки из текста сообщения: из entities (MessageEntityUrl,
    MessageEntityTextUrl) и из webpage-превью (MessageMediaWebPage.url).
    """
    urls: list[str] = []
    text = (getattr(msg, "message", None) or "")
    for ent in getattr(msg, "entities", None) or []:
        if isinstance(ent, MessageEntityUrl):
            offset, length = ent.offset, ent.length
            urls.append(text[offset: offset + length])
        elif isinstance(ent, MessageEntityTextUrl):
            url = getattr(ent, "url", None)
            if url:
                urls.append(url)
    # Webpage preview — там объект Telegram с .url
    media = getattr(msg, "media", None)
    if isinstance(media, MessageMediaWebPage):
        wp = getattr(media, "webpage", None)
        wp_url = getattr(wp, "url", None)
        if wp_url:
            urls.append(wp_url)
    # Fallback: если entities почему-то нет (старые сообщения), грубо
    # вытащим через regex по тексту.
    if not urls and text:
        urls.extend(_URL_RE.findall(text))
    # Уникализация с сохранением порядка
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _build_forward_info(msg: Message) -> Optional[ForwardInfo]:
    """Извлечь ForwardInfo из msg.fwd_from, если есть."""
    fwd = getattr(msg, "fwd_from", None)
    if fwd is None:
        return None
    # from_name — есть, если форвард от пользователя без публичного
    # username/без доступа к profile.
    from_name = getattr(fwd, "from_name", None)
    # Полное разрешение из fwd.from_id потребовало бы дополнительных
    # API-запросов на разрешение peer'ов. Для MVP оставляем минимум.
    return ForwardInfo(
        from_chat_title=None,
        from_chat_username=None,
        from_sender_name=from_name,
    )


async def _sender_label(msg: Message) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Возвращает (sender_id, sender_username, sender_display_name).
    msg.get_sender() лениво подтягивает из кеша Telethon — в рамках
    одного iter_messages обычно cheap (уже в response.users/chats).
    """
    sender_id = None
    username = None
    display = None
    try:
        sender = await msg.get_sender()
    except Exception:
        sender = None
    if sender is None:
        return sender_id, username, display
    sender_id = getattr(sender, "id", None)
    username = getattr(sender, "username", None)
    first = (getattr(sender, "first_name", "") or "").strip()
    last = (getattr(sender, "last_name", "") or "").strip()
    title = (getattr(sender, "title", "") or "").strip()
    display = (first + " " + last).strip() or title or None
    return sender_id, username, display


def _normalize_message(
    msg: Message,
    *,
    kind: MediaItemKind,
    entity,
    sender_id: Optional[int],
    sender_username: Optional[str],
    sender_display_name: Optional[str],
) -> Optional[MediaMessage]:
    """
    Превратить Telethon Message в MediaMessage. Возвращает None если
    сообщение не подходит (нет даты, медиа отсутствует — на всякий
    случай, хотя SearchRequest с фильтром не должен возвращать такие).
    """
    msg_dt = getattr(msg, "date", None)
    if msg_dt is None:
        return None
    if msg_dt.tzinfo is None:
        msg_dt = msg_dt.replace(tzinfo=timezone.utc)

    message_id = getattr(msg, "id", None)
    if message_id is None:
        return None

    chat_id = getattr(entity, "id", None) or 0
    chat_title = (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or "Без названия"
    )
    chat_username = getattr(entity, "username", None)
    permalink = build_message_permalink(entity, message_id) or ""

    text_field = (getattr(msg, "message", None) or "").strip() or None

    # Поля медиа — выбираем извлечение по kind'у и типу msg.media.
    mime_type = None
    file_size = None
    file_name = None
    duration_sec = None
    width = None
    height = None
    supports_streaming = None
    performer = None
    title = None
    extracted_urls: list[str] = []

    media = getattr(msg, "media", None)

    if kind == MediaItemKind.URL:
        # Сообщение-ссылка: текст + извлечённые URL.
        extracted_urls = _extract_urls(msg)
    elif isinstance(media, MessageMediaPhoto):
        photo_meta = _extract_photo_size(media)
        width = photo_meta["width"]
        height = photo_meta["height"]
        file_size = photo_meta["file_size"]
        mime_type = "image/jpeg"  # Telegram отдаёт фото как jpeg
    elif isinstance(media, MessageMediaDocument):
        doc = getattr(media, "document", None)
        if doc is not None:
            meta = _extract_document_metadata(doc)
            mime_type = meta["mime_type"]
            file_size = meta["file_size"]
            file_name = meta["file_name"]
            duration_sec = meta["duration_sec"]
            width = meta["width"]
            height = meta["height"]
            supports_streaming = meta["supports_streaming"]
            performer = meta["performer"]
            title = meta["title"]

    # caption vs text: у фото/видео msg.message — это подпись; у URL —
    # это собственно текст. Делим по kind'у.
    caption = None
    text = None
    if kind == MediaItemKind.URL:
        text = text_field
    else:
        caption = text_field

    forward_info = _build_forward_info(msg)

    return MediaMessage(
        message_id=int(message_id),
        chat_id=int(chat_id),
        chat_title=str(chat_title),
        chat_username=chat_username,
        permalink=permalink,
        date=msg_dt,
        sender_id=sender_id,
        sender_username=sender_username,
        sender_display_name=sender_display_name,
        text=text,
        caption=caption,
        kind=kind,
        mime_type=mime_type,
        file_size=file_size,
        file_name=file_name,
        duration_sec=duration_sec,
        width=width,
        height=height,
        supports_streaming=supports_streaming,
        performer=performer,
        title=title,
        extracted_urls=extracted_urls,
        forward_info=forward_info,
        reply_to_msg_id=(
            getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None)
        ),
        views=getattr(msg, "views", None),
        forwards_count=getattr(msg, "forwards", None),
        has_spoiler=bool(getattr(msg, "media", None) and getattr(media, "spoiler", False)),
        ttl_period_sec=getattr(msg, "ttl_period", None),
    )


# ---------------------------------------------------------------------------
# Fetch одного чата с одним фильтром (с FloodWait-ретраями)
# ---------------------------------------------------------------------------


async def _fetch_one_filter(
    client,
    entity,
    *,
    filter_cls: type,
    kind: MediaItemKind,
    min_date: datetime,
    max_date: Optional[datetime],
) -> list[MediaMessage]:
    """
    Один проход iter_messages с одним фильтром. Возвращает список
    нормализованных MediaMessage за окно [min_date, max_date].

    Итерация идёт от новых к старым:
      • Пока msg.date > max_date — пропускаем.
      • Когда max_date >= msg.date >= min_date — собираем.
      • Как только msg.date < min_date — break.

    FloodWait ретраим до FLOOD_WAIT_RETRIES раз, если задержка
    <= FLOOD_WAIT_MAX_SLEEP_SEC. Дольше — RuntimeError("FLOOD_WAIT").
    """
    attempts = 0
    while True:
        try:
            collected: list[MediaMessage] = []
            async for msg in client.iter_messages(
                entity, filter=filter_cls(), limit=ITER_LIMIT,
            ):
                if not isinstance(msg, Message):
                    continue
                msg_dt = getattr(msg, "date", None)
                if msg_dt is None:
                    continue
                if msg_dt.tzinfo is None:
                    msg_dt = msg_dt.replace(tzinfo=timezone.utc)

                # Верхняя граница (если задана) — пропускаем более новые
                if max_date is not None and msg_dt > max_date:
                    continue
                # Нижняя граница — досчитали окно, обрываемся
                if msg_dt < min_date:
                    break

                sender_id, sender_username, sender_display = await _sender_label(msg)
                normalized = _normalize_message(
                    msg,
                    kind=kind,
                    entity=entity,
                    sender_id=sender_id,
                    sender_username=sender_username,
                    sender_display_name=sender_display,
                )
                if normalized is not None:
                    collected.append(normalized)
            return collected
        except tg_errors.FloodWaitError as e:
            wait_sec = int(getattr(e, "seconds", 0) or 0)
            if attempts >= FLOOD_WAIT_RETRIES or wait_sec > FLOOD_WAIT_MAX_SLEEP_SEC:
                raise RuntimeError(f"FLOOD_WAIT:{wait_sec}") from e
            log.warning(
                "media_filter.flood_wait filter=%s wait_sec=%d retrying",
                filter_cls.__name__, wait_sec,
            )
            await asyncio.sleep(wait_sec + 1)
            attempts += 1


# ---------------------------------------------------------------------------
# Fetch одного чата (резолв + параллельный fetch всех фильтров + merge)
# ---------------------------------------------------------------------------


async def fetch_chat_media(
    db: AsyncSession,
    owner_user_id: int,
    chat_link: str,
    *,
    request: MediaFilterRequest,
    min_date: datetime,
    max_date: Optional[datetime] = None,
) -> ChatFetchResult:
    """
    Собрать все MediaMessage из одного чата по request за окно.

    Никогда не бросает исключения наружу — все ошибки уходят в
    `error_code` результата. Это нужно для группового запроса, где
    мы не хотим валить весь ответ из-за одного приватного чата.
    """
    result = ChatFetchResult(chat_link=chat_link)

    # 1) Подключение
    try:
        client = await ensure_connected(db, owner_user_id)
    except Exception as e:
        result.error_code = "RESOLVE_FAILED"
        result.error_detail = str(e)[:300]
        return result

    if not await client.is_user_authorized():
        result.error_code = "NOT_AUTHORIZED"
        return result

    # 2) Резолв entity
    try:
        entity = await resolve_entity_with_invite(client, chat_link)
    except (tg_errors.ChannelPrivateError,):
        result.error_code = "PRIVATE"
        return result
    except (tg_errors.ChatAdminRequiredError,):
        result.error_code = "ADMIN_REQUIRED"
        return result
    except (tg_errors.UsernameInvalidError, tg_errors.UsernameNotOccupiedError):
        result.error_code = "NOT_FOUND"
        return result
    except Exception as e:
        result.error_code = "RESOLVE_FAILED"
        result.error_detail = str(e)[:300]
        return result

    result.entity = entity
    result.chat_title = (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or "Без названия"
    )
    result.chat_username = getattr(entity, "username", None)

    # 3) План: какие фильтры дёрнуть для этого чата
    plan = build_filter_plan(request)
    if not plan:
        return result  # пустой messages, ошибки нет

    # 4) Параллельный fetch по фильтрам внутри одного чата.
    # Telegram не блокирует параллельные SearchRequest от одного клиента
    # в рамках адекватных лимитов; общий концурренси-контроль — на уровне
    # оркестрации множества чатов.
    fetch_tasks = [
        _fetch_one_filter(
            client, entity,
            filter_cls=filter_cls, kind=kind,
            min_date=min_date, max_date=max_date,
        )
        for filter_cls, kind in plan
    ]
    outcomes = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    flood_wait_hit = False
    fetch_failed = False
    fetch_failed_detail: Optional[str] = None
    for (filter_cls, kind), outcome in zip(plan, outcomes):
        if isinstance(outcome, RuntimeError) and str(outcome).startswith("FLOOD_WAIT"):
            flood_wait_hit = True
            log.warning(
                "media_filter.flood_wait_unrecoverable chat=%s filter=%s",
                chat_link, filter_cls.__name__,
            )
            continue
        if isinstance(outcome, Exception):
            fetch_failed = True
            fetch_failed_detail = (
                f"{type(outcome).__name__}: {str(outcome)[:200]}"
            )
            log.warning(
                "media_filter.fetch_failed chat=%s filter=%s err=%s",
                chat_link, filter_cls.__name__, outcome,
            )
            continue
        # outcome — list[MediaMessage]
        result.messages.extend(outcome)

    # Если ВСЕ фильтры упали на FloodWait/ошибку — отдаём error_code,
    # чтобы UI понял, что показывать «попробуйте позже».
    if not result.messages and (flood_wait_hit or fetch_failed):
        result.error_code = "FLOOD_WAIT" if flood_wait_hit else "FETCH_FAILED"
        result.error_detail = fetch_failed_detail
        return result

    # 5) Дедупликация по (chat_id, message_id) на случай если сообщение
    # попало под два разных фильтра (например, теоретически — голосовое
    # которое прошло бы и под Voice, и под другой фильтр). Для безопасности.
    seen: set[tuple[int, int]] = set()
    deduped: list[MediaMessage] = []
    for m in result.messages:
        key = (m.chat_id, m.message_id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(m)
    # Сортировка — новые сверху. Применяем глобально по чату; формирование
    # секций сделает formatter уже после фильтрации.
    deduped.sort(key=lambda m: m.date, reverse=True)
    result.messages = deduped

    return result


# ---------------------------------------------------------------------------
# Параллельный fetch множества чатов (для group endpoint'а)
# ---------------------------------------------------------------------------


async def fetch_many_chats_media(
    db: AsyncSession,
    owner_user_id: int,
    chat_links: list[str],
    *,
    request: MediaFilterRequest,
    min_date: datetime,
    max_date: Optional[datetime] = None,
) -> list[ChatFetchResult]:
    """
    Запустить `fetch_chat_media` параллельно по списку чатов с
    semaphore-ограничением одновременных запросов.

    Возвращает список ChatFetchResult в исходном порядке chat_links.
    """
    semaphore = asyncio.Semaphore(MAX_PARALLEL_FETCHES)

    async def _bounded(link: str) -> ChatFetchResult:
        async with semaphore:
            return await fetch_chat_media(
                db, owner_user_id, link,
                request=request, min_date=min_date, max_date=max_date,
            )

    return await asyncio.gather(*[_bounded(link) for link in chat_links])
