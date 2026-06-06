"""
Форматирование ответа медиафильтра под фронт.

Превращает «сырой» MediaFilterRun (от orchestrator.run_media_filter) в
структуру MediaCard / MediaSection / MediaChatBlock / MediaFilterAnswer,
которую UI рендерит карточками.

Правила группировки и сортировки — из спеки (architecture-media-filter.md
§4 «Структура ответа»):

  • Одиночный запрос: верхний уровень — секции по MediaItemKind в
    фиксированном порядке (MEDIA_KIND_DISPLAY_ORDER). Пустые секции не
    показываем.
  • Групповой запрос: верхний уровень — чаты в порядке chat_links,
    внутри каждого — те же секции по типу с тем же порядком.

Какие поля показываем на карточке — определяется kind'ом
(см. architecture-media-filter.md §3 «Параметры карточек»):

  • Видеофайл — size + duration
  • Видеокружок — только duration
  • Фото — size
  • Аудиофайл — duration + size + performer/title
  • Голосовое — только duration
  • Документ — file_name + size + mime
  • Ссылка — text + extracted_urls (size/duration не показываем)

Бэк отдаёт всё, что есть. Фронт сам решает, что прятать. Это упрощает
тест и снимает зависимость от пары «контракт фронта ↔ бэк».
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

from .orchestrator import MediaChatRun, MediaFilterRun
from .types import (
    MEDIA_KIND_DISPLAY_ORDER,
    MediaCard,
    MediaChatBlock,
    MediaFilterAnswer,
    MediaItemKind,
    MediaMessage,
    MediaSection,
)


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Сборка одной карточки из MediaMessage
# ---------------------------------------------------------------------------


def _build_sender_label(msg: MediaMessage) -> str:
    """
    Что показать как «автор». Приоритет: @username → display name → fallback.
    """
    if msg.sender_username:
        return f"@{msg.sender_username}"
    if msg.sender_display_name:
        return msg.sender_display_name
    return "Без автора"


def _build_forward_label(msg: MediaMessage) -> Optional[str]:
    """
    «forwarded from …» строка. None если это не пересланное сообщение.
    """
    fwd = msg.forward_info
    if fwd is None:
        return None
    parts: list[str] = []
    if fwd.from_chat_username:
        parts.append(f"@{fwd.from_chat_username}")
    elif fwd.from_chat_title:
        parts.append(fwd.from_chat_title)
    if fwd.from_sender_name and fwd.from_sender_name not in parts:
        parts.append(fwd.from_sender_name)
    if not parts:
        return "forwarded"
    return "forwarded from " + " · ".join(parts)


def build_card(msg: MediaMessage) -> MediaCard:
    """
    Превратить нормализованное сообщение в карточку для UI.
    Бэк всегда заполняет все поля, которые есть в исходнике; UI сам
    выбирает что отображать по kind'у согласно спеке.
    """
    fwd_label = _build_forward_label(msg)
    # Подпись: у URL-сообщений семантически это text, у медиа — caption.
    # На карточке показываем то, что есть.
    caption = msg.text if msg.kind == MediaItemKind.URL else msg.caption

    return MediaCard(
        message_id=msg.message_id,
        chat_id=msg.chat_id,
        chat_title=msg.chat_title,
        permalink=msg.permalink,
        date_iso=msg.date.isoformat(),
        sender_label=_build_sender_label(msg),
        caption=caption,
        kind=msg.kind,
        file_size=msg.file_size,
        duration_sec=msg.duration_sec,
        width=msg.width,
        height=msg.height,
        mime_type=msg.mime_type,
        file_name=msg.file_name,
        performer=msg.performer,
        title=msg.title,
        extracted_urls=list(msg.extracted_urls),
        is_forwarded=fwd_label is not None,
        forward_label=fwd_label,
        ttl_period_sec=msg.ttl_period_sec,
        has_spoiler=msg.has_spoiler,
    )


# ---------------------------------------------------------------------------
# Группировка в секции (одиночный или внутричатный уровень)
# ---------------------------------------------------------------------------


def _group_by_kind(messages: list[MediaMessage]) -> list[MediaSection]:
    """
    Группировка сообщений по MediaItemKind с фиксированным порядком
    секций (MEDIA_KIND_DISPLAY_ORDER). Пустые секции пропускаются.
    Сообщения внутри секции сортируются по дате убыв.
    """
    buckets: dict[MediaItemKind, list[MediaMessage]] = defaultdict(list)
    for m in messages:
        buckets[m.kind].append(m)

    sections: list[MediaSection] = []
    for kind in MEDIA_KIND_DISPLAY_ORDER:
        bucket = buckets.get(kind)
        if not bucket:
            continue
        bucket.sort(key=lambda m: m.date, reverse=True)
        cards = [build_card(m) for m in bucket]
        sections.append(MediaSection(kind=kind, count=len(cards), cards=cards))
    return sections


def _format_chat_block(chat: MediaChatRun) -> MediaChatBlock:
    sections = _group_by_kind(chat.messages) if not chat.error_code else []
    total = sum(s.count for s in sections)
    return MediaChatBlock(
        chat_link=chat.chat_link,
        chat_title=chat.chat_title or chat.chat_link,
        chat_username=chat.chat_username,
        total_count=total,
        sections=sections,
        error_code=chat.error_code,
    )


# ---------------------------------------------------------------------------
# Headline (короткий статусный текст над карточками)
# ---------------------------------------------------------------------------


_ERROR_LABELS_RU = {
    "PRIVATE": "Чат приватный, нет доступа.",
    "ADMIN_REQUIRED": "Для доступа к этому чату требуются права администратора.",
    "NOT_FOUND": "Чат не найден.",
    "FLOOD_WAIT": "Telegram временно ограничил запросы. Попробуйте через минуту.",
    "RESOLVE_FAILED": "Не удалось определить чат.",
    "NOT_AUTHORIZED": "Telegram не подключен.",
    "FETCH_FAILED": "Ошибка загрузки сообщений из Telegram.",
}


def _build_headline(run: MediaFilterRun, total: int) -> str:
    """
    Короткий заголовок над списком карточек. Журналисту полезно увидеть
    общее число и понять, ходила ли LLM-фильтрация. Без эмоций и
    рекомендаций — нейтрально.

    Если все чаты упали (в одиночном это один чат) — пишем причину,
    а не голое «ничего не найдено».
    """
    if total == 0:
        # Все чаты с ошибкой → даём пользователю реальную причину.
        all_errored = bool(run.chats) and all(c.error_code for c in run.chats)
        if all_errored:
            codes = [c.error_code for c in run.chats if c.error_code]
            # Если одна ошибка одного типа — берём её русский ярлык.
            unique = list(dict.fromkeys(codes))
            if len(unique) == 1:
                code = unique[0]
                msg = _ERROR_LABELS_RU.get(code, f"Ошибка: {code}.")
                return msg
            # Разные ошибки в разных чатах (group) → нейтральная сводка.
            return "Не удалось получить данные из чатов: " + ", ".join(unique) + "."
        return "За указанный период по выбранным фильтрам ничего не найдено."

    base = f"Найдено медиа-сообщений: {total}."
    extras: list[str] = []
    parsed = run.parsed
    if parsed.semantic_query:
        extras.append(f"семантический фильтр: «{parsed.semantic_query}»")
    if not parsed.structured_filters.is_empty():
        extras.append("применены структурные ограничения")
    if extras:
        base += " (" + "; ".join(extras) + ")"
    return base


# ---------------------------------------------------------------------------
# Главная точка входа
# ---------------------------------------------------------------------------


def format_run(run: MediaFilterRun) -> MediaFilterAnswer:
    """
    Превратить MediaFilterRun в MediaFilterAnswer.

      • is_group = False → собираем `sections` (карточки из messages
        ВСЕХ чатов — формально chats всегда одна штука в одиночном
        запросе, но на всякий случай поддерживаем merge).
      • is_group = True → собираем `chat_blocks` в исходном порядке
        run.chats (= порядок chat_links endpoint'а).
    """
    if run.is_group:
        chat_blocks = [_format_chat_block(c) for c in run.chats]
        total = sum(b.total_count for b in chat_blocks)
        return MediaFilterAnswer(
            is_group=True,
            total_count=total,
            chat_blocks=chat_blocks,
            sections=None,
            headline=_build_headline(run, total),
        )

    # Одиночный режим — секции по типу. Если по какой-то причине в
    # run.chats несколько чатов (защитный кейс), мерджим их сообщения.
    merged: list[MediaMessage] = []
    for c in run.chats:
        if c.error_code:
            continue
        merged.extend(c.messages)
    sections = _group_by_kind(merged)
    total = sum(s.count for s in sections)
    return MediaFilterAnswer(
        is_group=False,
        total_count=total,
        sections=sections,
        chat_blocks=None,
        headline=_build_headline(run, total),
    )
