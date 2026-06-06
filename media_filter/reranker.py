"""
Семантический реранкер медиафильтра (LLM-вызов №2, опциональный).

Дёргается ТОЛЬКО когда:
  • LLM-парсер вернул needs_semantic_rerank = true и непустой semantic_query;
  • после детерминированного пост-фильтра в наборе остались сообщения
    с подписями/текстом, по которым есть что искать.

Принципы:
  • Реранкер НЕ видит file_size/duration и прочие структурные поля —
    они уже отработали в post_filter. Сюда подаются только id, дата,
    caption/text и kind (хватает, чтобы оценить релевантность по смыслу).
  • Реранкер ВОЗВРАЩАЕТ список message_id'ов в порядке убывания
    релевантности, плюс булев «релевантно/нет». Мы у себя фильтруем
    набор и пересортировываем.
  • Если LLM упал или вернул мусор → fallback: оставить весь набор
    без изменений (лучше показать больше, чем потерять весь результат).

См. architecture-media-filter.md §7 «Вызов №2 — Семантический реранкер».
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, Field, ValidationError

from llm.models import (  # type: ignore[import-not-found]
    GEMINI_LITE_MODEL_SLUG,
    GEMINI_MODEL_SLUG,
    OPENAI_MODEL_SLUG,
    SUPPORTED_MODELS,
)
from llm.orchestrator import LlmAllModelsFailedError, LlmRunResult, run as orchestrator_run  # type: ignore[import-not-found]
from llm.routing import RoutingDecision, TIER_LIGHT  # type: ignore[import-not-found]

from .types import MediaMessage


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Параметры вызова
# ---------------------------------------------------------------------------


# Реранкер посложнее парсера: смотрит на N сообщений с текстом, возвращает
# список ID. На 50 коротких сообщений +200-300 токенов вывода — берём с
# запасом 1500.
_RERANKER_MAX_OUTPUT_TOKENS = 1500
_RERANKER_TEMPERATURE = 0.0

# Полный набор подписей может быть большим. Для одного вызова режем на
# окна по N сообщений; если их больше — несколько последовательных
# вызовов с одним и тем же semantic_query, потом склеиваем результаты.
# 60 сообщений с короткими подписями — комфортный размер контекста
# для light модели; для длинных подписей ограничиваем число символов
# одного caption'а (см. _format_candidate).
_BATCH_SIZE = 60

# Реранкер чуть дороже парсера — берём Flash (не Lite) как primary,
# так как качество семантической фильтрации заметно лучше при
# минимальной разнице в цене. Fallback на Lite — на случай downtime.
_RERANKER_FALLBACK_CHAIN_SLUGS = [
    GEMINI_MODEL_SLUG,
    GEMINI_LITE_MODEL_SLUG,
    OPENAI_MODEL_SLUG,
]

# Максимум символов одной подписи в промпте — длинные подписи режем,
# чтобы один батч помещался в light-tier max_output после рассуждения.
_MAX_CAPTION_CHARS = 280


def _build_reranker_decision() -> RoutingDecision:
    chain = [
        SUPPORTED_MODELS[s] for s in _RERANKER_FALLBACK_CHAIN_SLUGS
        if s in SUPPORTED_MODELS
    ]
    return RoutingDecision(
        primary_model=chain[0],
        fallback_chain=chain,
        max_output_tokens=_RERANKER_MAX_OUTPUT_TOKENS,
        tier=TIER_LIGHT,
        category="media_filter_reranker",
    )


# ---------------------------------------------------------------------------
# Промпт
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = """Ты — семантический фильтр и реранкер для медиа-сообщений из Telegram.

Тебе на вход придёт:
  • SEMANTIC_QUERY — смысловой запрос пользователя (например, "митинг").
  • CANDIDATES — пронумерованный список сообщений, каждое содержит id, дату,
    тип медиа (видео/фото/...) и подпись/текст (или пометку "no_caption").

Твоя задача — оценить, какие из этих сообщений действительно соответствуют SEMANTIC_QUERY по смыслу подписи/текста, и вернуть СТРОГО JSON следующей формы (без markdown):

{
  "relevant_ids": [<id1>, <id2>, ...],   // только id релевантных, в порядке убывания релевантности
  "rejected_ids": [<id3>, <id4>, ...]    // все остальные (для self-check)
}

ПРАВИЛА:

1. Релевантность оцениваешь ТОЛЬКО по подписи/тексту. Если подписи нет (no_caption) — сообщение НЕ может пройти семантический фильтр, отправляй его в rejected_ids.

2. relevant_ids + rejected_ids в сумме должны давать ВСЕ id из CANDIDATES — не теряй ни одного.

3. Не выдумывай id, которых нет в CANDIDATES.

4. Если ни одно сообщение не подходит — relevant_ids = [].

5. Никаких пояснений, никаких markdown-блоков. Только валидный JSON-объект."""


# ---------------------------------------------------------------------------
# Внутренние модели для парсинга LLM-ответа
# ---------------------------------------------------------------------------


class _RerankerResponse(BaseModel):
    relevant_ids: list[int] = Field(default_factory=list)
    rejected_ids: list[int] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)


def _extract_json_payload(raw_text: str) -> str:
    text = (raw_text or "").strip()
    m = _JSON_FENCE_RE.match(text)
    if m:
        return m.group(1).strip()
    return text


def _candidate_caption(msg: MediaMessage) -> str:
    """
    Текст для оценки релевантности — всё, что может нести смысл,
    собирается в один блок. Идея: пользователь спросил «о еде»;
    смысл может прятаться в подписи, в имени файла (для документа),
    в названии трека (для аудио), в тексте ссылки. Не теряем ничего.

    Что собираем по типам:
      • Все: caption (подпись под медиа) если есть.
      • URL: text (текст самого сообщения со ссылкой).
      • Документ/Видео/Аудио: file_name если есть — это часто
        полноценный носитель смысла («report-ужин-2026.pdf»).
      • Аудио: performer и title (метаданные трека).

    Все источники джойнятся через "; ", пустые пропускаются.
    Обрезаем итог по _MAX_CAPTION_CHARS, чтобы не раздуть промпт.
    """
    parts: list[str] = []
    if msg.caption:
        parts.append(msg.caption.strip())
    if msg.text and msg.text.strip() and msg.text.strip() not in parts:
        parts.append(msg.text.strip())
    if msg.file_name and msg.file_name.strip():
        parts.append(msg.file_name.strip())
    if msg.title and msg.title.strip():
        parts.append(msg.title.strip())
    if msg.performer and msg.performer.strip():
        parts.append(msg.performer.strip())

    if not parts:
        return ""
    src = "; ".join(parts).strip()
    if len(src) > _MAX_CAPTION_CHARS:
        return src[: _MAX_CAPTION_CHARS - 1] + "…"
    return src


def _format_candidate(idx: int, msg: MediaMessage) -> str:
    caption = _candidate_caption(msg)
    caption_part = caption if caption else "no_caption"
    # message_id — уникален в пределах одного чата; для группового
    # ответа возможны коллизии. Используем (chat_id, message_id) как
    # ключ извне, но LLM передаём ВНЕШНИЙ idx — позиционный, его и
    # сопоставим обратно при склейке.
    return (
        f"{idx}. [{msg.kind.value}] {msg.date.isoformat()}\n"
        f"   {caption_part}"
    )


@dataclass
class RerankOutcome:
    """
    Полный результат реранка. survivors — итоговый отфильтрованный/
    переранжированный список MediaMessage. llm_results — список
    LlmRunResult'ов всех вызовов (для биллинга, если батчей было больше
    одного). used_fallback — если LLM упал и мы вернули исходный set.
    """
    survivors: list[MediaMessage]
    llm_results: list[LlmRunResult]
    used_fallback: bool


def _chunk(seq: list, size: int):
    """Делит список на пачки по `size` элементов."""
    for i in range(0, len(seq), size):
        yield seq[i: i + size]


# ---------------------------------------------------------------------------
# Публичная функция
# ---------------------------------------------------------------------------


async def rerank_messages(
    *,
    messages: list[MediaMessage],
    semantic_query: str,
) -> RerankOutcome:
    """
    Применить семантическую фильтрацию реранкером к списку MediaMessage.

    Сообщения без подписи (caption/text пустой) — НЕ отправляются на
    LLM, они автоматом отсекаются (по правилу №1 промпта). Это
    экономит токены и устраняет ложноположительные срабатывания.
    """
    query = (semantic_query or "").strip()
    if not query or not messages:
        return RerankOutcome(survivors=list(messages), llm_results=[], used_fallback=False)

    # Делим на «с подписью» и «без подписи». Без подписи сразу отсекаем.
    with_caption: list[MediaMessage] = []
    for m in messages:
        if _candidate_caption(m):
            with_caption.append(m)
    if not with_caption:
        return RerankOutcome(survivors=[], llm_results=[], used_fallback=False)

    decision = _build_reranker_decision()
    llm_results: list[LlmRunResult] = []
    relevant_in_order: list[MediaMessage] = []  # упорядоченные по релевантности от LLM

    for batch in _chunk(with_caption, _BATCH_SIZE):
        # Индексы внутри батча: 1..len(batch). LLM возвращает ИХ.
        idx_to_msg: dict[int, MediaMessage] = {
            i + 1: m for i, m in enumerate(batch)
        }
        candidates_block = "\n".join(
            _format_candidate(i, m) for i, m in idx_to_msg.items()
        )
        user_prompt = (
            f"SEMANTIC_QUERY: {query}\n\n"
            f"CANDIDATES:\n{candidates_block}\n"
        )

        try:
            llm_result = await orchestrator_run(
                decision=decision,
                system_prompt=_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                temperature=_RERANKER_TEMPERATURE,
            )
        except LlmAllModelsFailedError:
            log.error("media_filter.reranker.all_models_failed batch_size=%d", len(batch))
            # Фолбэк только для этого батча: добавляем всех с подписью
            # в выживших, не меняя порядок. Без LLM мы не знаем, кто
            # релевантнее, но и терять весь батч из-за глюка не хотим.
            relevant_in_order.extend(batch)
            continue
        llm_results.append(llm_result)

        raw_json = _extract_json_payload(llm_result.text)
        try:
            data = json.loads(raw_json)
            parsed = _RerankerResponse.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as e:
            log.warning(
                "media_filter.reranker.bad_response model=%s err=%s raw=%s",
                llm_result.used_model.slug, str(e)[:200], raw_json[:200],
            )
            relevant_in_order.extend(batch)  # тот же фолбэк
            continue

        # Применяем порядок LLM, фильтруя несуществующие id и дубли.
        seen: set[int] = set()
        for idx in parsed.relevant_ids:
            if not isinstance(idx, int):
                continue
            if idx in seen:
                continue
            seen.add(idx)
            msg = idx_to_msg.get(idx)
            if msg is not None:
                relevant_in_order.append(msg)

    used_fallback = bool(messages) and not llm_results
    return RerankOutcome(
        survivors=relevant_in_order,
        llm_results=llm_results,
        used_fallback=used_fallback,
    )
