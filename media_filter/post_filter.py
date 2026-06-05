"""
Детерминированный пост-фильтр медиафильтра.

Применяет `StructuredFilters` (вытащенные LLM-парсером из текста
пользователя) к собранному из Telegram списку MediaMessage. Никаких
LLM-вызовов — чистая арифметика и сравнение строк.

Эта функция должна работать одинаково и при наличии LLM-парсера, и
без него (пустой StructuredFilters — все сообщения проходят).

См. architecture-media-filter.md §5 шаг "Детерминированный пост-фильтр".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .types import MediaMessage, StructuredFilters, TimeWindowOverride


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Расчёт эффективного временного окна
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EffectiveTimeWindow:
    """
    Финальное окно после клампинга UI-окна с time_window_override
    от LLM-парсера. Используется в Telethon-слое (min_date/max_date).

    Правила клампинга см. в architecture-media-filter.md §7:
      • если получившееся окно ШИРЕ UI — игнорируем (UI/тариф главнее);
      • если УЖЕ — применяем;
      • если задана только одна граница — вторую берём из UI.
    """
    min_date: datetime
    max_date: Optional[datetime]


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_effective_window(
    *,
    ui_window_from: datetime,
    ui_window_to: Optional[datetime],
    override: TimeWindowOverride,
) -> EffectiveTimeWindow:
    """
    Применить time_window_override поверх UI-окна по правилам спеки.

    UI всегда задаёт минимально возможный from (по тарифу).
    Override может ТОЛЬКО сужать окно — не расширять.
    """
    eff_from = _ensure_utc(ui_window_from)
    eff_to = _ensure_utc(ui_window_to) if ui_window_to else None

    if override.from_iso is not None:
        ovr_from = _ensure_utc(override.from_iso)
        # Сужение: берём бОльшую из двух нижних границ.
        if ovr_from > eff_from:
            eff_from = ovr_from
        # else: override шире → игнорируем

    if override.to_iso is not None:
        ovr_to = _ensure_utc(override.to_iso)
        # Сужение: берём меньшую из двух верхних границ (если UI-верх не задан,
        # любой override-to уже сужение → применяем).
        if eff_to is None or ovr_to < eff_to:
            eff_to = ovr_to

    # Защита от пустого окна (override mог дать from > to)
    if eff_to is not None and eff_from > eff_to:
        # Возвращаем нулевое окно: [eff_to, eff_to] — fetch вернёт пусто.
        # Это корректное поведение, если пользователь задал бессмысленный
        # промежуток — лучше пустой ответ, чем тихая интерпретация.
        log.info(
            "media_filter.empty_window after_override from=%s > to=%s — "
            "returning zero-width window",
            eff_from.isoformat(), eff_to.isoformat(),
        )
        return EffectiveTimeWindow(min_date=eff_to, max_date=eff_to)

    return EffectiveTimeWindow(min_date=eff_from, max_date=eff_to)


# ---------------------------------------------------------------------------
# Применение StructuredFilters к одному MediaMessage
# ---------------------------------------------------------------------------


def _match_substring(haystack: Optional[str], needle: Optional[str]) -> bool:
    """
    True если needle является подстрокой haystack без учёта регистра.
    Если needle null/пустой — фильтр не активен, любое сообщение проходит.
    Если haystack null — фильтр НЕ матчит (т.к. искать нечего).
    """
    if needle is None or needle == "":
        return True
    if haystack is None:
        return False
    return needle.lower() in haystack.lower()


def _matches_username(message_username: Optional[str], target: Optional[str]) -> bool:
    """
    sender_username = "ivanov" (без @). Сравнение нечувствительно к регистру.
    Если у сообщения нет username (анонимный канал, личка от User без юзернейма) —
    фильтр НЕ матчит.
    """
    if target is None or target == "":
        return True
    if not message_username:
        return False
    return message_username.lstrip("@").lower() == target.lstrip("@").lower()


def message_passes_structured(
    msg: MediaMessage, filters: StructuredFilters
) -> bool:
    """
    Проверить, удовлетворяет ли одно сообщение всем заданным структурным
    ограничениям. null-поля фильтра трактуются как «ограничения нет».

    Важно: time_window_override НЕ применяем здесь — он уже учтён при
    fetch'е на уровне Telethon-слоя (compute_effective_window). Дублирующая
    проверка тут была бы лишней работой и могла бы вызвать рассогласование
    при граничных миллисекундах.
    """
    # --- file_size ---
    if filters.file_size_min_bytes is not None:
        if msg.file_size is None or msg.file_size < filters.file_size_min_bytes:
            return False
    if filters.file_size_max_bytes is not None:
        if msg.file_size is None or msg.file_size > filters.file_size_max_bytes:
            return False

    # --- duration ---
    if filters.duration_min_sec is not None:
        if msg.duration_sec is None or msg.duration_sec < filters.duration_min_sec:
            return False
    if filters.duration_max_sec is not None:
        if msg.duration_sec is None or msg.duration_sec > filters.duration_max_sec:
            return False

    # --- разрешение (фото / видео) ---
    if filters.width_min_px is not None:
        if msg.width is None or msg.width < filters.width_min_px:
            return False
    if filters.height_min_px is not None:
        if msg.height is None or msg.height < filters.height_min_px:
            return False

    # --- отправитель ---
    if not _matches_username(msg.sender_username, filters.sender_username):
        return False

    # --- MIME / file_name ---
    if not _match_substring(msg.mime_type, filters.mime_type_contains):
        return False
    if not _match_substring(msg.file_name, filters.file_name_contains):
        return False

    return True


def apply_structured_filters(
    messages: list[MediaMessage], filters: StructuredFilters
) -> list[MediaMessage]:
    """
    Прогон через message_passes_structured. Сохраняет исходный порядок
    (новые сверху — это уже задано fetch-слоем).
    """
    if filters.is_empty():
        return list(messages)
    return [m for m in messages if message_passes_structured(m, filters)]
