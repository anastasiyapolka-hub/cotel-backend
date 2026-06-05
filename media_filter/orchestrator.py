"""
Оркестратор медиафильтра — склейка всех слоёв в одну точку входа.

run_media_filter() собирает пайплайн:

  1) compute_effective_window — пересечь UI-окно с time_window_override
     (которое заполнит LLM-парсер, если найдёт в тексте упоминание времени).
  2) parse_user_query    — LLM-парсер свободного текста (опциональный).
  3) fetch_chat_media / fetch_many_chats_media — Telethon search-слой.
  4) apply_structured_filters — детерминированный пост-фильтр.
  5) rerank_messages     — семантический LLM-реранкер (опциональный).

Парсер вызывается ДО fetch'а, чтобы time_window_override сразу попал
в SearchRequest (узкое окно = меньше данных из Telegram = быстрее +
дешевле). Реранкер вызывается ПОСЛЕ структурного фильтра — на
меньшем наборе сообщений = меньше токенов в LLM.

Этот модуль НЕ форматирует ответ под UI (это делает formatter.py в
Этапе 6) и НЕ списывает токены (это делает endpoint в Этапе 5
интеграции). Он возвращает «сырой» MediaFilterRun со списками
выживших сообщений + полным LLM usage'ом для биллинга на верхнем уровне.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from llm.orchestrator import LlmRunResult  # type: ignore[import-not-found]

from .llm_parser import ParseOutcome, parse_user_query
from .post_filter import (
    EffectiveTimeWindow,
    apply_structured_filters,
    compute_effective_window,
)
from .reranker import RerankOutcome, rerank_messages
from .telethon_search import (
    ChatFetchResult,
    fetch_chat_media,
    fetch_many_chats_media,
)
from .types import MediaCategory, MediaFilterRequest, MediaMessage, ParsedUserQuery


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Результат пайплайна
# ---------------------------------------------------------------------------


@dataclass
class MediaChatRun:
    """Результат прогона одного чата через все слои фильтра."""
    chat_link: str
    chat_title: Optional[str]
    chat_username: Optional[str]
    entity: object  # Telethon entity (для permalink-карт в endpoint'е)
    messages: list[MediaMessage] = field(default_factory=list)
    # Сколько было ДО / ПОСЛЕ каждого слоя — для логов/диагностики
    fetched_count: int = 0
    after_structured_count: int = 0
    after_semantic_count: int = 0
    error_code: Optional[str] = None
    error_detail: Optional[str] = None


@dataclass
class MediaFilterRun:
    """Полный результат пайплайна. Используется formatter'ом для сборки UI-ответа."""
    is_group: bool
    chats: list[MediaChatRun]
    parsed: ParsedUserQuery
    effective_window: EffectiveTimeWindow
    selected_categories: list[MediaCategory]
    # LLM usage — для биллинга в endpoint'е.
    parser_llm: Optional[LlmRunResult] = None
    reranker_llms: list[LlmRunResult] = field(default_factory=list)
    used_parser_fallback: bool = False
    used_reranker_fallback: bool = False

    @property
    def total_messages(self) -> int:
        return sum(len(c.messages) for c in self.chats)


# ---------------------------------------------------------------------------
# Подкомпоненты пайплайна
# ---------------------------------------------------------------------------


async def _apply_structured_and_semantic(
    chat_run: MediaChatRun,
    parsed: ParsedUserQuery,
    *,
    semantic_query: Optional[str],
) -> tuple[list[LlmRunResult], bool]:
    """
    Прогон одного чата через структурный фильтр + (опционально) реранкер.

    Мутирует chat_run.messages и счётчики. Возвращает (llm_results,
    used_reranker_fallback) — для агрегации в MediaFilterRun.
    """
    chat_run.fetched_count = len(chat_run.messages)

    # 1. Структурный пост-фильтр
    chat_run.messages = apply_structured_filters(
        chat_run.messages, parsed.structured_filters
    )
    chat_run.after_structured_count = len(chat_run.messages)

    # 2. Семантический реранкер
    if semantic_query and chat_run.messages:
        outcome: RerankOutcome = await rerank_messages(
            messages=chat_run.messages,
            semantic_query=semantic_query,
        )
        chat_run.messages = outcome.survivors
        chat_run.after_semantic_count = len(chat_run.messages)
        return outcome.llm_results, outcome.used_fallback

    chat_run.after_semantic_count = chat_run.after_structured_count
    return [], False


def _chat_fetch_to_run(fetched: ChatFetchResult) -> MediaChatRun:
    """Адаптер ChatFetchResult → MediaChatRun (одинаковая семантика, разные поля)."""
    return MediaChatRun(
        chat_link=fetched.chat_link,
        chat_title=fetched.chat_title,
        chat_username=fetched.chat_username,
        entity=fetched.entity,
        messages=list(fetched.messages),
        error_code=fetched.error_code,
        error_detail=fetched.error_detail,
    )


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------


async def run_media_filter(
    db: AsyncSession,
    owner_user_id: int,
    *,
    chat_links: list[str],
    is_group: bool,
    request: MediaFilterRequest,
    ui_window_from: datetime,
    ui_window_to: Optional[datetime] = None,
    user_query: Optional[str] = None,
    now: Optional[datetime] = None,
) -> MediaFilterRun:
    """
    Главная точка входа в ветку медиафильтра.

    Параметры:
      chat_links: list[str] — один или несколько чатов. Для одиночного
        запроса — список из одного. is_group задаётся явно (даже если
        len(chat_links)==1, поведение endpoint'а может быть разным).
      request: MediaFilterRequest — что выбрал пользователь в UI.
      ui_window_from/to — окно времени, рассчитанное endpoint'ом по
        period_value/period_unit и тарифным лимитам.
      user_query — свободный текст пользователя (может быть None/пуст).
      now — текущее время для парсера (по умолчанию datetime.now(UTC)).

    Возврат: MediaFilterRun с per-chat результатами + LLM usage.
    Никогда не raise: ошибки попадают в chat_run.error_code, пайплайн
    отдаёт частичный результат.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    selected_categories = request.effective_categories()

    # --- 1. LLM-парсер (если есть свободный текст) ---
    parser_outcome: ParseOutcome = await parse_user_query(
        user_query=user_query or "",
        ui_window_from=ui_window_from,
        ui_window_to=ui_window_to,
        selected_categories=[c.value for c in selected_categories],
        now=now,
    )
    parsed = parser_outcome.parsed

    # --- 2. Эффективное окно (UI ∩ override) ---
    effective_window = compute_effective_window(
        ui_window_from=ui_window_from,
        ui_window_to=ui_window_to,
        override=parsed.structured_filters.time_window_override,
    )

    # --- 3. Telethon fetch ---
    if is_group or len(chat_links) > 1:
        fetched_list = await fetch_many_chats_media(
            db, owner_user_id, chat_links,
            request=request,
            min_date=effective_window.min_date,
            max_date=effective_window.max_date,
        )
    else:
        # Одиночный запрос: один await вместо лишнего semaphore-обёрта.
        single = await fetch_chat_media(
            db, owner_user_id, chat_links[0],
            request=request,
            min_date=effective_window.min_date,
            max_date=effective_window.max_date,
        )
        fetched_list = [single]

    chats: list[MediaChatRun] = [_chat_fetch_to_run(f) for f in fetched_list]

    # --- 4-5. Структурный фильтр + опциональный реранкер по каждому чату ---
    semantic_query = parsed.semantic_query if parsed.needs_semantic_rerank else None

    all_reranker_llms: list[LlmRunResult] = []
    any_reranker_fallback = False

    # Реранкер делает свои LLM-вызовы; делать их параллельно по чатам
    # внутри одного group-запроса повысит давление на провайдера, поэтому
    # тут идём последовательно — стабильнее.
    for chat_run in chats:
        if chat_run.error_code:
            # Пропускаем: ошибка fetch'а уже зафиксирована.
            chat_run.fetched_count = 0
            continue
        llm_results, used_fb = await _apply_structured_and_semantic(
            chat_run, parsed, semantic_query=semantic_query,
        )
        all_reranker_llms.extend(llm_results)
        any_reranker_fallback = any_reranker_fallback or used_fb

    return MediaFilterRun(
        is_group=is_group,
        chats=chats,
        parsed=parsed,
        effective_window=effective_window,
        selected_categories=selected_categories,
        parser_llm=parser_outcome.llm_result,
        reranker_llms=all_reranker_llms,
        used_parser_fallback=parser_outcome.used_fallback,
        used_reranker_fallback=any_reranker_fallback,
    )
