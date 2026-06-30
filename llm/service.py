from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional, Union

# C3: колбэк прогресса чанкования (done, total) — воркер пишет его в задачу,
# фронт показывает «часть N из M». Необязателен (синхронные пути не передают).
ProgressCb = Optional[Callable[[int, int], Awaitable[None]]]

from .models import resolve_model_config, DEFAULT_AI_MODEL, ModelConfig
from .adapters import get_adapter, LlmFatalError
from .classifier import (
    ALL_CATEGORIES,
    CATEGORY_DIGEST,
    CATEGORY_FILTER_RANK,
    CATEGORY_SIMPLE_QA,
    CATEGORY_SOURCE_SYNTHESIS,
    ClassificationResult,
    DEFAULT_CATEGORY,
    classify_query,
)
from .orchestrator import (
    LlmRunResult,
    LlmAllModelsFailedError,
    run as orchestrator_run,
)
from .preprocessing import clean_telegram_messages
from .routing import (
    RoutingDecision,
    TIER_LIGHT,
    normalize_tier,
    route,
)
from .usage import (
    LlmUsage,
    LlmTextResult,
    LlmJsonResult,
    TOKENS_SOURCE_API,
    TOKENS_SOURCE_EMPTY,
)
from dataclasses import dataclass, replace as _dc_replace

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Time-context block
# ---------------------------------------------------------------------------
#
# Why this exists:
# Without an explicit "today" reference, models interpret relative phrases
# like «за последний месяц» / «last week» against the *data they see* —
# which is just the fetched chat window. On Q4 we observed Gemini-family
# models (2.5 Flash, 3.1 Flash Lite) report «период с 21 по 28 мая» when
# the user actually asked for the last month — because the messages they
# received were stamped in that window. OpenAI/Anthropic happen to be
# more forgiving on this, but it's a real ambiguity, not a Gemini bug:
# the prompt simply does not tell the model what "today" means or what
# period was requested.
#
# Fix: prepend a small TIME CONTEXT block to the user prompt that states
# (a) today's UTC date, (b) the fetch window in days the backend
# actually pulled, (c) the actual oldest/newest message timestamps in
# the data. The model can then either answer for the requested period
# or, if data falls short, say so explicitly instead of silently
# narrowing the question.
# ---------------------------------------------------------------------------

def _extract_message_date_window(
    messages: list[dict],
) -> tuple[Optional[str], Optional[str]]:
    """
    Return (oldest_date, newest_date) ISO strings from a list of cleaned
    Telegram messages. Used to render the actual-data window in the
    time-context block. Returns (None, None) if no usable dates.
    """
    dates: list[str] = []
    for m in messages:
        d = m.get("date") or m.get("message_ts")
        if isinstance(d, str) and d.strip():
            dates.append(d.strip())
    if not dates:
        return None, None
    # Cleaned messages are already in chronological order, but defensive
    # sort doesn't hurt and is cheap on the typical 100-500 msg payload.
    dates_sorted = sorted(dates)
    return dates_sorted[0], dates_sorted[-1]


def _build_time_context_block(
    *,
    requested_period_days: Optional[int],
    oldest_msg_date: Optional[str],
    newest_msg_date: Optional[str],
    fallback_lang_code: str,
) -> str:
    """
    Render the time-context block that goes at the top of the user
    prompt. Bilingual (RU/EN) because the model uses these as factual
    anchors regardless of question language — keep them in plain English
    so they tokenize cheaply on all providers and don't fight Cyrillic
    tokenizers.
    """
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    parts: list[str] = [
        "TIME CONTEXT (use this to resolve relative time phrases like",
        "«last week», «last month», «за последний месяц», «за неделю»",
        "in the user's question):",
        f"- Today's date (UTC): {today_iso}",
    ]
    if requested_period_days and requested_period_days > 0:
        parts.append(
            f"- Period the user requested: last {int(requested_period_days)} "
            f"day(s) of chat history"
        )
    if oldest_msg_date and newest_msg_date:
        parts.append(
            f"- Actual data you have: messages from {oldest_msg_date} "
            f"to {newest_msg_date}"
        )
    parts.append(
        "If the data window is shorter than what the user asked for, "
        "say so explicitly in your answer (e.g. «в чате найдены сообщения "
        "только за период X-Y, более старая история отсутствует») — do "
        "not silently narrow the question to the data you happen to "
        "have. If the data window matches or exceeds the request, "
        "interpret the question against the requested period, not the "
        "raw message window."
    )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Language helpers
# ---------------------------------------------------------------------------

# Supported wrapper/UI languages. Note: LLM *output* language for Q&A is
# not constrained to this list — the LLM detects the user's question
# language natively and can respond in any language it knows. This map
# only covers languages that flow through OUR UX copy (fallback,
# classify reason, digest narration) which must match user.language.
_LANG_NAMES: dict[str, str] = {
    "en": "English",
    "ru": "Russian",
}


def _normalize_lang_code(value: Any) -> str:
    """Normalize to 'en' or 'ru', defaulting to 'en'."""
    if not value:
        return "en"
    v = str(value).strip().lower()
    if v.startswith("ru"):
        return "ru"
    return "en"


def _lang_name(value: Any) -> str:
    """Return a human-readable language name for use inside LLM prompts."""
    return _LANG_NAMES.get(_normalize_lang_code(value), "English")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _safe_parse_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start:end + 1])
        raise


async def _chat_text_completion_rich(
    *,
    ai_model: str,
    task: Optional[str] = None,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_output_tokens: int | None = None,
) -> LlmTextResult:
    """
    Run a single-turn chat completion against the configured provider
    and return a normalized `LlmTextResult` that includes token usage.

    `task` is a hint used by Anthropic task-based routing
    (see llm.models.resolve_model_config). It is safely ignored by
    the OpenAI provider.
    """
    config = resolve_model_config(ai_model, task=task)
    adapter = get_adapter(config.provider)

    text, usage, finish_reason = await adapter.complete(
        provider_model=config.provider_model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=temperature,
        max_output_tokens=max_output_tokens or 1500,
    )

    return LlmTextResult(
        text=text,
        usage=usage,
        ai_model=ai_model,
        provider=config.provider,
        provider_model=config.provider_model,
        raw_finish_reason=finish_reason,
    )


def _empty_text_result(*, ai_model: str, text: str) -> LlmTextResult:
    """Build a short-circuit LlmTextResult for cases where no LLM call was made."""
    config = resolve_model_config(ai_model)
    return LlmTextResult(
        text=text,
        usage=LlmUsage.empty(),
        ai_model=ai_model,
        provider=config.provider,
        provider_model=config.provider_model,
        raw_finish_reason=None,
    )


def _empty_json_result(*, ai_model: str, data: dict) -> LlmJsonResult:
    config = resolve_model_config(ai_model)
    return LlmJsonResult(
        data=data,
        raw_text="",
        usage=LlmUsage.empty(),
        ai_model=ai_model,
        provider=config.provider,
        provider_model=config.provider_model,
        raw_finish_reason=None,
    )


# ---------------------------------------------------------------------------
# Q&A — summarize_chat_messages
# ---------------------------------------------------------------------------

# Output-token caps per analysis tier. Light is the default;
# balanced and deep tiers will be wired through routing later.
TIER_OUTPUT_LIMITS: dict[str, int] = {
    "light":    2000,
    "balanced": 4000,
    "deep":     8000,
}
DEFAULT_TIER = "light"


_EMPTY_CHAT_MESSAGES: dict[str, str] = {
    "en": "No text messages available for analysis.",
    "ru": "В чате нет текстовых сообщений для анализа.",
}


async def summarize_chat_messages(
    *,
    user_query: str,
    chat_name: str,
    text_messages: list[dict],
    fallback_language: str = "en",
    ai_model: str = DEFAULT_AI_MODEL,
    tier: str = DEFAULT_TIER,
    return_usage: bool = False,
    requested_period_days: Optional[int] = None,
) -> Union[str, LlmTextResult]:
    """
    Answer a user's question grounded in a Telegram chat fragment.

    LLM-native language detection: the model responds in the same
    language as `user_query`. `fallback_language` (expected to be
    `user.language`) is used only when the question's language is
    ambiguous (too short, emoji-only, mixed).

    Return contract:
      - default (`return_usage=False`): returns the plain text str —
        backward-compatible with the original signature.
      - `return_usage=True`: returns an `LlmTextResult` with .text,
        .usage (input/output/total tokens + tokens_source), .ai_model,
        .provider, .provider_model. Use this in code paths that need
        to write a UsageEvent.
    """
    # Preprocess: drop emoji-only / short reactions / system events,
    # and compact ISO timestamps. This is the QA flow's only token
    # optimization layer — see llm/preprocessing.py for details.
    cleaned_messages, _preproc_stats = clean_telegram_messages(text_messages)

    lines = []
    for msg in cleaned_messages:
        date = msg.get("date") or ""
        sender = msg.get("from") or "Unknown"
        text = msg.get("text") or ""
        msg_id = msg.get("message_id")
        reply_to = msg.get("reply_to")
        # Включаем стабильный токен [msg:ID], если есть id —
        # модель будет ссылаться им же в цитатах, а фронт превратит
        # токен в кликабельную иконку-ссылку на сообщение в Telegram.
        # Если сообщение является ответом, добавляем [reply→msg:PARENT]
        # — это даёт модели структуру дискуссий без дублирования текста
        # родительского сообщения (Telethon его и не дублирует, но мы
        # явно показываем связь).
        prefix_parts = [f"[{date}]"]
        if msg_id is not None:
            prefix_parts.append(f"[msg:{int(msg_id)}]")
        if reply_to:
            try:
                prefix_parts.append(f"[reply→msg:{int(reply_to)}]")
            except (TypeError, ValueError):
                pass
        prefix = " ".join(prefix_parts)
        lines.append(f"{prefix} {sender}: {text}")

    context = "\n".join(lines)

    if not context:
        empty_text = _EMPTY_CHAT_MESSAGES[_normalize_lang_code(fallback_language)]
        if return_usage:
            return _empty_text_result(ai_model=ai_model, text=empty_text)
        return empty_text

    fallback_lang_name = _lang_name(fallback_language)

    system_prompt = (
        "You are CoTel, an expert analyst of Telegram chat conversations. "
        "Users come to you to find specific information, patterns, or "
        "insights in their chat history that would be tedious to find "
        "manually.\n\n"
        "For this query: read the provided chat fragment, find messages "
        "that are semantically relevant to the user's question, and "
        "produce a focused answer grounded in those messages.\n\n"
        "HOW TO ANSWER\n"
        "1. Identify messages that are semantically relevant (not just "
        "keyword matches). Consider synonyms, paraphrases, emoji, "
        "transliteration.\n"
        "2. Organize findings by theme or timeline — whichever better "
        "fits the question.\n"
        "3. When referencing a specific message, cite it with this "
        "format:\n"
        "       @username: \"short verbatim quote\" [msg:ID]\n"
        "   - ID is the exact numeric id taken from the [msg:ID] token "
        "that precedes the message in the chat fragment below. Copy it "
        "verbatim. Do NOT invent ids and do NOT cite a message that has "
        "no [msg:ID] token.\n"
        "   - Place the [msg:ID] token AT THE END of the citation, "
        "immediately after the closing quote — not before the username "
        "and not before the quote itself.\n"
        "   - If the same message is referenced multiple times in your "
        "answer, repeat the same [msg:ID] each time.\n"
        "   - Keep quotes short and in the original language of the "
        "message.\n"
        "   - Some messages have an extra [reply→msg:PARENT_ID] token "
        "after their own [msg:ID]. This means the message is a reply "
        "to the message with that PARENT_ID. Use this to reconstruct "
        "conversation threads when relevant, but do NOT cite the "
        "[reply→msg:...] token itself — only the message's own "
        "[msg:ID].\n"
        "4. If the chat contains conflicting information (different "
        "people say different things), surface the conflict — do not "
        "flatten it.\n"
        "5. If relevant messages are sparse (e.g. only 3 out of 400 are "
        "actually relevant), say so up front so the user calibrates "
        "expectations.\n\n"
        "CITATIONS\n"
        "- No more than 3 citations per sub-topic. If more relevant "
        "messages exist, pick the most representative ones.\n"
        "- For the remaining (un-cited) relevant messages on the same "
        "sub-topic, summarize what they add in the conclusion or "
        "wrap-up of that sub-topic — so the user knows what the "
        "uncited messages say without seeing each one quoted.\n\n"
        "RULES\n"
        "- Ground every claim in the provided messages. Never invent "
        "participants, dates, events, or details that are not in the "
        "input.\n"
        "- If the input is insufficient, say so plainly. Do not "
        "speculate.\n"
        "- Quote messages verbatim in their original language. Write "
        "your own analysis and conclusions in the SAME LANGUAGE as the "
        "user's question. If the language of the question is ambiguous "
        "(one word, only emoji, mixed languages, too short to tell), "
        f"respond in {fallback_lang_name}.\n"
        "- No preamble. Do not restate the question.\n\n"
        "LENGTH\n"
        "- Target: 1000-1500 characters. HARD LIMIT: 2000 characters.\n"
        "- If you would exceed the limit, prioritize: direct answer "
        "first, citations second, context-setting last.\n"
        "- Structure: 3–6 short paragraphs OR a bulleted list of 3–8 "
        "items, whichever better suits the question.\n\n"
        "OUTPUT FORMAT: plain text. No Markdown headings, no JSON "
        "wrapper."
    )

    oldest_date, newest_date = _extract_message_date_window(cleaned_messages)
    time_block = _build_time_context_block(
        requested_period_days=requested_period_days,
        oldest_msg_date=oldest_date,
        newest_msg_date=newest_date,
        fallback_lang_code=_normalize_lang_code(fallback_language),
    )

    user_prompt = (
        f"{time_block}\n\n"
        f"Chat name: {chat_name}\n\n"
        f"Chat messages (oldest to newest):\n{context}\n\n"
        f"User question:\n{user_query}"
    )

    result = await _chat_text_completion_rich(
        ai_model=ai_model,
        task="qa",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
        max_output_tokens=TIER_OUTPUT_LIMITS.get(tier, TIER_OUTPUT_LIMITS[DEFAULT_TIER]),
    )

    if return_usage:
        return result
    return result.text


# ---------------------------------------------------------------------------
# Q&A — summarize_chat_messages_group (multi-chat / group request)
# ---------------------------------------------------------------------------

_EMPTY_GROUP_MESSAGE: dict[str, str] = {
    "en": "No chats had any text messages to analyze.",
    "ru": "Ни в одном из выбранных чатов нет сообщений для анализа.",
}


def _build_group_chat_section(
    *,
    chat_index: int,
    chat_name: str,
    cleaned_messages: list[dict],
) -> str:
    """
    Render a single chat as a labelled section for the group prompt.
    Same line format as single-chat: `[date] [msg:ID] [reply→msg:PID] sender: text`.
    msg-IDs are unique within a chat but may collide across chats — the
    LLM is instructed to keep IDs scoped to their parent section, and
    the frontend uses per-chat message_links maps anyway.
    """
    if not cleaned_messages:
        return ""

    lines: list[str] = [f"=== CHAT {chat_index}: «{chat_name}» ==="]
    for msg in cleaned_messages:
        date = msg.get("date") or ""
        sender = msg.get("from") or "Unknown"
        text = msg.get("text") or ""
        msg_id = msg.get("message_id")
        reply_to = msg.get("reply_to")

        prefix_parts = [f"[{date}]"]
        if msg_id is not None:
            prefix_parts.append(f"[msg:{int(msg_id)}]")
        if reply_to:
            try:
                prefix_parts.append(f"[reply→msg:{int(reply_to)}]")
            except (TypeError, ValueError):
                pass
        prefix = " ".join(prefix_parts)
        lines.append(f"{prefix} {sender}: {text}")

    return "\n".join(lines)


async def summarize_chat_messages_group(
    *,
    user_query: str,
    chats: list[dict],
    fallback_language: str = "en",
    ai_model: str = DEFAULT_AI_MODEL,
    return_usage: bool = True,
    requested_period_days: Optional[int] = None,
) -> LlmTextResult:
    """
    Answer a user's question across multiple Telegram chats in a single
    LLM call.

    `chats` is a list of dicts with shape:
        {
          "chat_name": str,
          "text_messages": list[dict],   # same shape as single-chat
        }

    Output is a single markdown document with one `## Чат: <name>` (or
    `## Chat: <name>`) section per non-empty chat, plus a closing
    `## Общий вывод` / `## Summary` block. The frontend parses the
    headers to render collapsible per-chat blocks.

    Notes for callers (`tg_analyze_chats_group`):
    - We pre-filter chats with empty message lists; the LLM only sees
      chats that have actual content. Empty chats should be marked
      `status="empty"` in the endpoint's response BEFORE calling us.
    - `max_output_tokens=10000` is intentionally large to allow ~500
      words per chat at 20-chat group. This is paired with thinking
      disabled (see adapters.py) so the full 10K goes to visible text.
    """
    if not chats:
        return _empty_text_result(
            ai_model=ai_model,
            text=_EMPTY_GROUP_MESSAGE[_normalize_lang_code(fallback_language)],
        )

    # Build per-chat sections after preprocessing each chat. Stats are
    # currently discarded — admin observability for group preprocessing
    # can be added later if it proves useful.
    sections: list[str] = []
    chat_names: list[str] = []
    all_cleaned: list[dict] = []  # for global time-window computation
    for idx, c in enumerate(chats, start=1):
        chat_name = (c.get("chat_name") or "").strip() or f"Chat {idx}"
        text_messages = c.get("text_messages") or []
        cleaned, _stats = clean_telegram_messages(text_messages)
        section = _build_group_chat_section(
            chat_index=idx,
            chat_name=chat_name,
            cleaned_messages=cleaned,
        )
        if section:
            sections.append(section)
            chat_names.append(chat_name)
            all_cleaned.extend(cleaned)

    if not sections:
        return _empty_text_result(
            ai_model=ai_model,
            text=_EMPTY_GROUP_MESSAGE[_normalize_lang_code(fallback_language)],
        )

    combined_context = "\n\n".join(sections)
    fallback_lang_name = _lang_name(fallback_language)
    chat_count = len(sections)

    # System prompt: instruct the model to produce one labelled section
    # per chat plus a short overall conclusion. Cite [msg:ID] tokens
    # verbatim, but only within the parent chat's section — msg IDs are
    # not globally unique across chats.
    system_prompt = (
        "You are CoTel, an expert analyst of Telegram chat "
        "conversations. The user has selected MULTIPLE chats and asked "
        "one question. Your job: answer the question SEPARATELY for "
        "each chat, plus give a short overall conclusion.\n\n"
        "INPUT FORMAT\n"
        "The chat history below is divided into sections, each starting "
        "with a marker line of the form:\n"
        "    === CHAT N: «chat name» ===\n"
        "Each section contains messages from that single chat, in the "
        "same per-message format as single-chat analysis: "
        "[date] [msg:ID] [reply→msg:PARENT_ID] sender: text.\n\n"
        "OUTPUT FORMAT (markdown)\n"
        "Produce one section per chat, in the same order they appear in "
        "the input, using THIS EXACT heading format:\n"
        "    ## Chat: <chat name>\n"
        "Use the chat name verbatim from the «...» marker. After all "
        "chat sections, add a final section:\n"
        "    ## Summary\n"
        "with a 2-3 sentence overall conclusion across all chats.\n\n"
        "If the user's question is in Russian, translate the section "
        "labels accordingly: use `## Чат: ...` and `## Общий вывод`.\n\n"
        "ANSWER RULES (per chat section)\n"
        "- 200-400 words per chat. Be concrete, not generic.\n"
        "- Ground every claim in messages from THAT chat only. Do not "
        "mix evidence between chats.\n"
        "- Cite using `[msg:ID]` exactly as it appears in the chat's "
        "section. IDs are unique within a chat but may collide across "
        "chats — never carry an ID from one chat into another section.\n"
        "- If a chat has no information relevant to the question, "
        "write a single short sentence saying so. Do not pad.\n"
        "- Do not duplicate the same point across multiple chats; if "
        "two chats discuss the same thing, say so in the Summary, not "
        "in each chat section.\n\n"
        "LANGUAGE\n"
        "- Respond in the SAME LANGUAGE as the user's question. If the "
        "question's language is ambiguous (one word, only emoji, mixed "
        f"languages, too short to tell), respond in {fallback_lang_name}.\n"
        "- Quote messages verbatim in their original language.\n\n"
        "NO PREAMBLE. Do not restate the question. Do not list the "
        "chats up front — just start with the first `## Chat: ...` "
        "section."
    )

    oldest_date, newest_date = _extract_message_date_window(all_cleaned)
    time_block = _build_time_context_block(
        requested_period_days=requested_period_days,
        oldest_msg_date=oldest_date,
        newest_msg_date=newest_date,
        fallback_lang_code=_normalize_lang_code(fallback_language),
    )

    user_prompt = (
        f"{time_block}\n\n"
        f"Number of chats: {chat_count}\n"
        f"Chat names (in order): {'; '.join(chat_names)}\n\n"
        f"Chat history (each chat in its own labelled section):\n\n"
        f"{combined_context}\n\n"
        f"User question:\n{user_query}"
    )

    result = await _chat_text_completion_rich(
        ai_model=ai_model,
        task="qa",  # group qa stays on Sonnet for Anthropic (no Haiku downgrade)
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
        max_output_tokens=10000,
    )

    return result


# ---------------------------------------------------------------------------
# Event-subscription classifier
# ---------------------------------------------------------------------------

_EMPTY_CLASSIFY_SUMMARY: dict[str, str] = {
    "en": "No text messages provided.",
    "ru": "Нет текстовых сообщений.",
}


async def classify_subscription_matches(
    *,
    prompt: str,
    chat_title: str,
    messages: list[dict],
    ux_language: str = "en",
    ai_model: str = DEFAULT_AI_MODEL,
    return_usage: bool = False,
) -> Union[dict, LlmJsonResult]:
    """
    Filter a batch of messages against an event-subscription query.

    `ux_language` (expected to be `user.language`) controls the
    language of the service fields (`reason`, `summary_reason`) which
    are OUR UX copy shown in the dispatched Telegram digest.
    `excerpt` is always a verbatim raw quote.

    Return contract:
      - default (`return_usage=False`): returns parsed JSON dict
        (backward-compatible).
      - `return_usage=True`: returns an `LlmJsonResult` with .data
        (the same dict), .usage, .ai_model, .provider, .provider_model.
    """
    lines = []
    for m in messages:
        mid = m.get("message_id")
        ts = m.get("message_ts")
        a = m.get("author_display") or "Unknown"
        aid = m.get("author_id")
        txt = m.get("text") or ""
        lines.append(f"[{mid}] [{ts}] {a} (author_id={aid}): {txt}")

    context = "\n".join(lines)
    if not context:
        empty_data = {
            "found": False,
            "matches": [],
            "summary_reason": _EMPTY_CLASSIFY_SUMMARY[_normalize_lang_code(ux_language)],
            "confidence": 0.0,
        }
        if return_usage:
            return _empty_json_result(ai_model=ai_model, data=empty_data)
        return empty_data

    ux_lang_name = _lang_name(ux_language)

    system_prompt = (
        "You are a Telegram message classifier for CoTel event-based "
        "subscriptions.\n\n"
        "Goal: given a subscription query (what the user is watching "
        "for) and a batch of new chat messages, return the messages "
        "that match the query by meaning.\n\n"
        "MATCHING LOGIC\n"
        "- Match by semantics, not keywords. Account for synonyms, "
        "typos, transliteration (Cyrillic/Latin), colloquialisms, "
        "abbreviations, emoji-only expressions.\n"
        "- Be strict: return only messages where a real human reviewing "
        "the subscription would say \"yes, this is it.\" When in doubt, "
        "do NOT match.\n"
        "- NEVER invent author, timestamp, or message text. Use ONLY "
        "what the input provides for each message.\n\n"
        "CONFIDENCE SCALE (the \"confidence\" field)\n"
        "- 0.9–1.0: matches are very clear; minimal ambiguity.\n"
        "- 0.6–0.9: matches are plausible but require human judgement.\n"
        "- 0.0–0.6: reserved for found=false or weak/no matches.\n\n"
        "OUTPUT SCHEMA (strict JSON, no Markdown, no comments):\n"
        "{\n"
        "  \"found\": true | false,\n"
        "  \"matches\": [\n"
        "    {\n"
        "      \"message_id\": <int, copied from input [brackets]>,\n"
        "      \"message_ts\": \"<ISO8601, copied from input>\",\n"
        "      \"author_display\": \"<copied from input>\",\n"
        "      \"author_id\": <int copied from input, or null if missing>,\n"
        "      \"excerpt\": \"<verbatim quote, ≤300 chars, original language>\",\n"
        f"      \"reason\": \"<one short sentence ≤140 chars in {ux_lang_name}>\"\n"
        "    }\n"
        "  ],\n"
        f"  \"summary_reason\": \"<one sentence ≤200 chars in {ux_lang_name}>\",\n"
        "  \"confidence\": <float 0.0–1.0>\n"
        "}\n\n"
        "FIELD RULES\n"
        "- \"excerpt\" is a VERBATIM quote. Do not translate, "
        "paraphrase, or clean up. If longer than 300 chars, truncate at "
        "the NEAREST WORD BOUNDARY and append \"…\".\n"
        "- \"reason\" and \"summary_reason\" are OUR UX copy shown to "
        f"the user alongside the cited message. They MUST be in {ux_lang_name}.\n"
        "- If nothing matches: found=false, matches=[]."
    )

    user_prompt = (
        f"Chat name: {chat_title}\n\n"
        f"Subscription query:\n{prompt}\n\n"
        "New messages (each line: [message_id] [message_ts] "
        "author_display (author_id=NNN): text):\n"
        f"{context}"
    )

    rich = await _chat_text_completion_rich(
        ai_model=ai_model,
        task="classify",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.1,
        max_output_tokens=2000,
    )
    parsed = _safe_parse_json(rich.text)

    if return_usage:
        return LlmJsonResult(
            data=parsed,
            raw_text=rich.text,
            usage=rich.usage,
            ai_model=rich.ai_model,
            provider=rich.provider,
            provider_model=rich.provider_model,
            raw_finish_reason=rich.raw_finish_reason,
        )
    return parsed


# ---------------------------------------------------------------------------
# Summary-subscription digest builder
# ---------------------------------------------------------------------------

async def build_subscription_digest(
    *,
    prompt: str,
    chat_title: str,
    messages: list[dict],
    answer_language: str = "en",
    ai_model: str = DEFAULT_AI_MODEL,
    return_usage: bool = False,
) -> Union[dict, LlmJsonResult]:
    """
    Build a summary-style digest for a subscription window.

    `answer_language` (expected to be `user.language`) controls the
    narration language. Verbatim quotes inside the digest remain in
    their source language per our i18n rules.

    Return contract:
      - default (`return_usage=False`): returns parsed JSON dict
        (backward-compatible).
      - `return_usage=True`: returns an `LlmJsonResult` with .data,
        .usage, .ai_model, .provider, .provider_model.
    """
    lines = []
    for m in messages:
        mid = m.get("message_id")
        ts = m.get("message_ts")
        a = m.get("author_display") or "Unknown"
        aid = m.get("author_id")
        txt = m.get("text") or ""
        r = m.get("reply_to")
        reply_tag = f" reply_to={int(r)}" if r else ""
        lines.append(f"[{mid}] [{ts}] {a} (author_id={aid}){reply_tag}: {txt}")

    context = "\n".join(lines)
    if not context:
        empty_data = {"digest_text": "", "confidence": 0.0}
        if return_usage:
            return _empty_json_result(ai_model=ai_model, data=empty_data)
        return empty_data

    answer_lang_name = _lang_name(answer_language)

    system_prompt = (
        "You are CoTel, an analyst of Telegram chat conversations.\n\n"
        "Task: given a slice of chat messages (some with reply_to=<id> "
        "indicating replies) and the user's description of what kind of "
        "summary they want, produce a concise digest that directly "
        "answers their request.\n\n"
        "SCOPE / RELEVANCE — THIS IS THE MOST IMPORTANT RULE\n"
        "- The user's query defines the SCOPE of the digest. Treat it as a "
        "topic filter, not just a hint.\n"
        "- Include ONLY messages relevant to what the user asked about. "
        "Deliberately EXCLUDE everything unrelated, even if it looks like "
        "interesting news. A digest about 'beach events in Batumi' must NOT "
        "mention unrelated topics (road works, boating laws, weather in other "
        "regions, wine articles, etc.).\n"
        "- If the user named several facets (e.g. 'events, activities, "
        "entertainment'), treat them as the allowed scope, not as a request "
        "to cover all chat news.\n"
        "- NEVER substitute adjacent or 'related' topics when the exact scope "
        "is thin. If the user asked about beach recreation in Batumi, do NOT "
        "report general culture news from Batumi, nor any news from other "
        "cities (e.g. Tbilisi), as a stand-in.\n"
        "- Do NOT even MENTION excluded topics — not as context, not as a "
        "'meanwhile' aside, and not to state that you skipped them.\n"
        "- If NOTHING in this window matches the user's query, the ENTIRE "
        "digest_text must be ONLY a short note that there were no relevant "
        "updates in this period — no other items, no substitutes, nothing "
        "appended before or after that note — and set a low confidence.\n\n"
        "STRUCTURE YOUR OUTPUT (only for messages that passed the scope filter)\n"
        "- If the user asked a specific question, answer it directly.\n"
        "- If the user asked for an overview of a topic, organize by SUB-TOPIC "
        "within that scope (not chronologically, message-by-message). 2–4 "
        "sub-topics is usually right.\n"
        "- Surface the signal: who said what important thing, what "
        "decisions were made, what questions remain open.\n\n"
        "LENGTH GUIDANCE\n"
        "- Target: 500–1500 characters total. HARD LIMIT: 4096 "
        "characters.\n"
        "- If the chat is very sparse (<10 messages), a 300–500 char "
        "summary is fine — do not pad.\n"
        "- If the chat is very dense, pick the 3–5 most important "
        "threads rather than trying to cover everything.\n\n"
        "RULES\n"
        "- Ground every claim in the provided messages. Never invent.\n"
        "- Quote verbatim (in the original language) ONLY when it "
        "materially helps the summary — short quotes, and no "
        "quote-padding.\n"
        "- When referencing a specific message, use this format so we "
        "can later render a link back to it:\n"
        "       @author said X (msg #<message_id>)\n"
        "- If the data is insufficient to produce a meaningful summary, "
        "say so explicitly and stop.\n\n"
        f"OUTPUT LANGUAGE for your narration: {answer_lang_name}.\n"
        "OUTPUT FORMAT: strict JSON, no Markdown:\n"
        "{\n"
        "  \"digest_text\": \"<the summary, ≤4096 chars>\",\n"
        "  \"confidence\": <float 0.0–1.0>\n"
        "}"
    )

    user_prompt = (
        f"Chat name: {chat_title}\n\n"
        "Messages (each line: [message_id] [message_ts] author_display "
        "(author_id=NNN) reply_to=<id?>: text):\n"
        f"{context}\n\n"
        "User query — this defines the TOPIC SCOPE of the digest; include only "
        f"messages relevant to it and exclude everything else:\n{prompt}"
    )

    rich = await _chat_text_completion_rich(
        ai_model=ai_model,
        task="digest",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
        max_output_tokens=2000,
    )
    parsed = _safe_parse_json(rich.text)

    if return_usage:
        return LlmJsonResult(
            data=parsed,
            raw_text=rich.text,
            usage=rich.usage,
            ai_model=rich.ai_model,
            provider=rich.provider,
            provider_model=rich.provider_model,
            raw_finish_reason=rich.raw_finish_reason,
        )
    return parsed


# ===========================================================================
# NEW: маршрутизированный Q&A pipeline (classifier → routing → orchestrator)
# ===========================================================================
#
# Эти функции — публичный API нового pipeline'а. Используются endpoint'ами
# (main.tg_analyze_chat / tg_analyze_chats_group) после миграции на токены.
#
# Старые `summarize_chat_messages` / `summarize_chat_messages_group` пока
# оставлены — на них завязаны подписочный runner и admin/service endpoint'ы.
# Удалим в этапе 4 рефакторинга.
# ===========================================================================


@dataclass
class QaRunResult:
    """
    Результат высокоуровневого Q&A вызова через router + orchestrator.

    Endpoint использует:
      - .text                — текст ответа (рендер пользователю)
      - .is_empty            — True если LLM не вызывался (пустой чат)
                               → billing не списывает, usage_event без счётчиков
      - .llm.used_model      — фактическая модель (для pricing.get_token_rates)
      - .llm.usage           — input/output/thinking tokens (для billing.debit)
      - .llm.finish_reason   — для diagnostics
      - .classification      — для записи в user_query_log
      - .decision            — для логирования в meta_json usage_event
      - .llm.was_fallback    — для метрик/админки
    """
    text: str
    is_empty: bool
    llm: Optional[LlmRunResult]
    classification: Optional[ClassificationResult]
    decision: Optional[RoutingDecision]
    chunks_count: int = 1  # >1 если ответ собран чанкованием (map-reduce)
    # Разбивка по вызовам LLM (slug, usage) — для биллинга КАЖДОГО вызова своей
    # моделью (важно при чанковании + fallback на другую модель). None = один
    # вызов (биллим как раньше по qa_result.llm).
    llm_calls: Optional[list] = None


def _resolve_category(
    classification: Optional[ClassificationResult],
    explicit_category: Optional[str],
) -> tuple[str, bool]:
    """
    Определить финальную категорию для роутера.

    Если пользователь явно переопределил через UI (chip-override) →
    explicit_category побеждает. Иначе берём из classification.
    Если ни того, ни того нет → DEFAULT_CATEGORY (simple_qa).

    Возвращает (final_category, was_overridden).
    """
    if explicit_category and explicit_category.strip() in ALL_CATEGORIES:
        detected = classification.category if classification else None
        was_overridden = detected is not None and detected != explicit_category.strip()
        return explicit_category.strip(), was_overridden
    if classification is not None:
        return classification.category, False
    return DEFAULT_CATEGORY, False


# ---------------------------------------------------------------------------
# Чанкование больших запросов (map-reduce)
# ---------------------------------------------------------------------------
#
# Если контекст не влезает в безопасный бюджет модели — режем сообщения на
# части (хронологически, в рамках ОДНОГО чата), на каждой части извлекаем
# релевантное (map), затем отдельным вызовом собираем единый ответ с
# дедупликацией (reduce). Включено только для хорошо делящихся категорий.
# ---------------------------------------------------------------------------

_CHUNKABLE_CATEGORIES = {
    CATEGORY_FILTER_RANK,
    CATEGORY_DIGEST,
    CATEGORY_SIMPLE_QA,
    CATEGORY_SOURCE_SYNTHESIS,
}
_CHUNK_BUDGET_FACTOR = 0.5     # доля лимита контекста модели на один кусок
_CHARS_PER_TOKEN = 2.0         # консервативная оценка для кириллицы
_MAX_CHUNKS = 16               # потолок числа частей (стоимость/время)
_MAX_CHUNKS_HEAVY = 64         # потолок, когда пользователь ПОДТВЕРДИЛ тяжёлый
                               # запрос (B2 red-зона): режем всё, но в разумных
                               # пределах, без молчаливой обрезки хвостом
_MAX_PARALLEL_CHUNKS = 3       # сколько частей гоним одновременно (волнами)

# Подстроки в тексте ошибки 400, по которым опознаём «промпт не влез в окно
# модели» (а не другой bad-request). Нужно для B1: когда основная модель
# (большое окно) упала, а fallback-модель с маленьким окном отвергает наш
# крупный кусок — мы его пере-режем под её окно.
_CONTEXT_OVERFLOW_HINTS = (
    "context length",
    "context window",
    "maximum context",
    "too long",
    "too many tokens",
    "input is too large",
    "exceeds the maximum",
    "reduce the length",
    "string too long",
)


def _chain_min_context_limit(decision: RoutingDecision) -> int:
    """Минимальное окно среди всех моделей fallback-цепочки (в токенах LLM).

    По нему считается «безопасный» размер куска, который влезет в ЛЮБУЮ модель
    цепочки — нужен для до-нарезки куска при fallback на маленькую модель."""
    limits = [
        int(getattr(m, "context_limit", 1_000_000) or 1_000_000)
        for m in decision.fallback_chain
    ]
    return min(limits) if limits else 1_000_000


def _budget_chars_for_tokens(context_limit_tokens: int) -> int:
    """Бюджет куска в символах из окна модели в токенах."""
    return int(context_limit_tokens * _CHUNK_BUDGET_FACTOR * _CHARS_PER_TOKEN)


def _decision_without_primary(
    decision: RoutingDecision,
) -> Optional[RoutingDecision]:
    """Копия decision без первой (упавшей) модели цепочки.

    Нужно при до-нарезке: если основная модель недоступна, под-куски не должны
    снова платить retry-таймаут по мёртвой основной — гоним их сразу по
    оставшимся моделям. Возвращает None, если альтернатив не осталось."""
    rest = list(decision.fallback_chain[1:])
    if not rest:
        return None
    return _dc_replace(decision, primary_model=rest[0], fallback_chain=rest)


def _is_context_overflow_fatal(
    exc: LlmFatalError, primary_model: ModelConfig
) -> bool:
    """True, если fatal — это «промпт не влез» у НЕ-основной модели.

    То есть основная модель (большое окно) упала retryable-ошибкой, fallback
    переключился на модель с меньшим окном, и та отвергла крупный кусок 400-кой
    про длину контекста. Именно этот случай лечится до-нарезкой (B1).
    Конфиг-ошибку самой основной модели сюда не пускаем."""
    if getattr(exc, "provider_model", None) == getattr(primary_model, "provider_model", None):
        return False
    msg = str(exc).lower()
    return any(hint in msg for hint in _CONTEXT_OVERFLOW_HINTS)


def _chunk_messages_by_chars(
    cleaned_messages: list[dict],
    budget_chars: int,
) -> list[list[dict]]:
    """Разбить сообщения (в хронологическом порядке) на куски так, чтобы объём
    текста в каждом куске не превышал budget_chars. Границы — между
    сообщениями, сами сообщения не дробим."""
    chunks: list[list[dict]] = []
    cur: list[dict] = []
    cur_chars = 0
    for m in cleaned_messages:
        c = len(m.get("text") or "") + len(m.get("from") or "") + 40
        if cur and cur_chars + c > budget_chars:
            chunks.append(cur)
            cur = []
            cur_chars = 0
        cur.append(m)
        cur_chars += c
    if cur:
        chunks.append(cur)
    return chunks


def _build_map_user_prompt(
    time_block: str,
    chat_name: str,
    context: str,
    user_query: str,
    part_idx: int,
    parts_total: int,
) -> str:
    return (
        f"{time_block}\n\n"
        f"Chat name: {chat_name}\n\n"
        f"This is PART {part_idx} of {parts_total} of the chat history "
        "(a contiguous time slice of ONE chat). Extract EVERY item relevant "
        "to the user's question from THIS part only, using the same citation "
        "format (author tag + short quote + [msg:ID]). Output findings only — "
        "no intro, no conclusion, no 'nothing found' notes. If nothing in this "
        "part is relevant, return an empty answer.\n\n"
        f"Chat messages (oldest to newest):\n{context}\n\n"
        f"User question:\n{user_query}"
    )


def _build_reduce_prompts(
    user_query: str,
    fallback_lang_name: str,
    partial_texts: list[str],
) -> tuple[str, str]:
    system = (
        "You are CoTel. You are given partial findings extracted independently "
        "from consecutive parts of ONE Telegram chat, all answering the SAME "
        "user question. Your job is to ASSEMBLE them into one complete final "
        "answer to that question.\n"
        "- Treat this as a MERGE, not a re-summary. Preserve every distinct "
        "point, item, contact, topic or finding from the parts. Do NOT drop, "
        "sample, or compress the content into just a few examples.\n"
        "- Merge only TRUE duplicates — entries from different parts that refer "
        "to the SAME thing (same person / item / topic). Keep such an entry "
        "once. Distinct things stay as separate entries even if similar.\n"
        "- Keep each entry in the SAME rich format the parts use: the author "
        "tag, a short verbatim quote, and the [msg:ID] citation token. Keep "
        "[msg:ID] tokens and author tags EXACTLY as-is — never invent, drop, or "
        "renumber them. Do NOT reduce entries to bare names or logins: every "
        "entry MUST keep its short quote and its [msg:ID] so the user can click "
        "through to the source message.\n"
        "- Match the FORM the question implies: a list stays a list; a digest "
        "of topics stays organized by topic. Drop only entries clearly "
        "off-topic for the question.\n"
        "- Apply any explicit limit from the question (e.g. 'take 30 last', "
        "'top 5') ONLY at the very end, after assembling the full set.\n"
        f"- Write narration in {fallback_lang_name} unless the question clearly "
        "implies another language. No preamble, no restating the question."
    )
    joined = "\n\n".join(
        f"--- Part {i + 1} ---\n{t}"
        for i, t in enumerate(partial_texts)
        if (t or "").strip()
    )
    user = (
        f"User question:\n{user_query}\n\n"
        f"Partial findings to merge into one answer:\n{joined}"
    )
    return system, user


def _build_group_reduce_prompts(
    user_query: str,
    fallback_lang_name: str,
    per_chat_partials: list[tuple[str, list[str]]],
) -> tuple[str, str]:
    """Reduce-промпт для ГРУППОВОГО чанкования.

    На входе — частичные находки, сгруппированные ПО ЧАТАМ (каждый чат резался
    отдельно, чаты не смешивались). На выходе — тот же формат, что у обычного
    группового ответа: секции `## Chat: <name>` по порядку + `## Summary`."""
    system = (
        "You are CoTel. You are given partial findings extracted independently "
        "from consecutive parts of SEVERAL Telegram chats, all answering the "
        "SAME user question. Findings are grouped PER CHAT. Assemble them into "
        "one final multi-chat answer.\n"
        "OUTPUT FORMAT (markdown): one section per chat, in the given order, "
        "with the heading `## Chat: <chat name>` (use the name verbatim). After "
        "all chat sections add a final `## Summary` with a 2-3 sentence overall "
        "conclusion across all chats. If the user's question is in Russian, use "
        "`## Чат: ...` and `## Общий вывод`.\n"
        "- This is a MERGE, not a re-summary: preserve every distinct point, "
        "item, contact, topic or finding. Merge only TRUE duplicates WITHIN the "
        "same chat.\n"
        "- NEVER mix evidence between chats; never carry a `[msg:ID]` from one "
        "chat into another section. Keep `[msg:ID]` tokens and author tags "
        "EXACTLY as-is, with their short quotes — do not reduce entries to bare "
        "names.\n"
        "- If a chat's findings are empty, write one short sentence saying "
        "nothing relevant was found in that chat.\n"
        f"- Respond in the user's language; if ambiguous use {fallback_lang_name}.\n"
        "NO PREAMBLE. Start directly with the first chat section."
    )
    blocks: list[str] = []
    for name, parts in per_chat_partials:
        joined = "\n\n".join(
            f"--- Part {i + 1} ---\n{t}"
            for i, t in enumerate(parts) if (t or "").strip()
        ) or "(no relevant findings in this chat)"
        blocks.append(f"=== CHAT: «{name}» ===\n{joined}")
    user = (
        f"User question:\n{user_query}\n\n"
        "Per-chat partial findings to assemble (keep chats strictly separate):\n\n"
        + "\n\n".join(blocks)
    )
    return system, user


def _sum_usages(usages: list[LlmUsage]) -> LlmUsage:
    """Сумма usage по всем вызовам map+reduce — для биллинга по факту."""
    if not usages:
        return LlmUsage.empty()
    in_t = sum(int(u.input_tokens) for u in usages)
    out_t = sum(int(u.output_tokens) for u in usages)
    think = sum(int(getattr(u, "thinking_tokens", 0) or 0) for u in usages)
    total = sum(int(u.total_tokens) for u in usages)
    source = (
        TOKENS_SOURCE_API
        if any(u.tokens_source == TOKENS_SOURCE_API for u in usages)
        else usages[0].tokens_source
    )
    return LlmUsage(
        input_tokens=in_t,
        output_tokens=out_t,
        total_tokens=total,
        tokens_source=source,
        thinking_tokens=think,
    )


async def _emit_progress(progress_cb: ProgressCb, done: int, total: int) -> None:
    """Безопасно вызвать колбэк прогресса (C3). Сбой прогресса не валит запрос."""
    if progress_cb is None:
        return
    try:
        await progress_cb(done, total)
    except Exception:  # noqa: BLE001
        log.warning("progress_cb failed", exc_info=True)


async def _map_chunk_adaptive(
    *,
    decision: RoutingDecision,
    system_prompt: str,
    time_block: str,
    chat_name: str,
    user_query: str,
    chunk_msgs: list[dict],
    part_idx: int,
    parts_total: int,
    safe_decision: Optional[RoutingDecision],
    safe_budget_chars: int,
) -> list[LlmRunResult]:
    """Один map-вызов с адаптивной до-нарезкой (B1).

    Обычно возвращает один результат. Если основная модель упала и fallback-
    модель с меньшим окном отвергла крупный кусок (overflow 400), кусок
    пере-режется под безопасный бюджет и под-куски гонятся по оставшимся
    моделям (без мёртвой основной). Используется и одиночным, и групповым
    чанкером (для группы chat_name всегда один — чаты не смешиваются)."""
    ctx = _format_qa_chat_context(chunk_msgs)
    up = _build_map_user_prompt(
        time_block, chat_name, ctx, user_query, part_idx, parts_total
    )
    try:
        return [await orchestrator_run(
            decision=decision, system_prompt=system_prompt,
            user_prompt=up, temperature=0.2,
        )]
    except LlmFatalError as exc:
        if not _is_context_overflow_fatal(exc, decision.primary_model):
            raise  # настоящая конфиг-ошибка — не маскируем до-нарезкой
        sub_decision = safe_decision or decision
        sub_chunks = _chunk_messages_by_chars(chunk_msgs, safe_budget_chars)
        log.warning(
            "QA_DIAG chunk_resize part=%d/%d sub_parts=%d "
            "safe_budget_chars=%d reason=fallback_window_overflow",
            part_idx, parts_total, len(sub_chunks), safe_budget_chars,
        )
        out: list[LlmRunResult] = []
        for sub in sub_chunks:
            sub_ctx = _format_qa_chat_context(sub)
            sub_up = _build_map_user_prompt(
                time_block, chat_name, sub_ctx, user_query, part_idx, parts_total
            )
            out.append(await orchestrator_run(
                decision=sub_decision, system_prompt=system_prompt,
                user_prompt=sub_up, temperature=0.2,
            ))
        return out


async def _run_qa_chunked(
    *,
    cleaned_messages: list[dict],
    user_query: str,
    chat_name: str,
    fallback_lang_name: str,
    fallback_language: str,
    requested_period_days: Optional[int],
    decision: RoutingDecision,
    classification: Optional[ClassificationResult],
    chunk_budget_chars: int,
    max_chunks: int = _MAX_CHUNKS,
    progress_cb: ProgressCb = None,
) -> "QaRunResult":
    chunks = _chunk_messages_by_chars(cleaned_messages, chunk_budget_chars)
    if len(chunks) > max_chunks:
        # Потолок: берём самые свежие части (хвост). При подтверждённом тяжёлом
        # запросе (B2) потолок поднят до _MAX_CHUNKS_HEAVY — обрезка почти не
        # срабатывает; для неподтверждённых это последняя защита от перегруза.
        log.warning(
            "qa.chunked over_cap parts=%d cap=%d — берём последние %d",
            len(chunks), max_chunks, max_chunks,
        )
        chunks = chunks[-max_chunks:]
    parts_total = len(chunks)

    oldest_date, newest_date = _extract_message_date_window(cleaned_messages)
    time_block = _build_time_context_block(
        requested_period_days=requested_period_days,
        oldest_msg_date=oldest_date,
        newest_msg_date=newest_date,
        fallback_lang_code=_normalize_lang_code(fallback_language),
    )
    system_prompt = _build_qa_single_system_prompt(fallback_lang_name)

    # B1: бюджет «под самую маленькую модель цепочки» и копия decision без
    # упавшей основной — на случай, если кусок (резанный под Gemini) не влезет
    # в fallback-модель с меньшим окном.
    safe_decision = _decision_without_primary(decision)
    safe_budget_chars = max(
        1, _budget_chars_for_tokens(_chain_min_context_limit(decision))
    )

    async def _map_one(chunk_msgs: list[dict], idx: int) -> list[LlmRunResult]:
        return await _map_chunk_adaptive(
            decision=decision, system_prompt=system_prompt, time_block=time_block,
            chat_name=chat_name, user_query=user_query, chunk_msgs=chunk_msgs,
            part_idx=idx + 1, parts_total=parts_total,
            safe_decision=safe_decision, safe_budget_chars=safe_budget_chars,
        )

    # MAP — волнами по _MAX_PARALLEL_CHUNKS. Каждый кусок может дать несколько
    # результатов (если был пере-резан под fallback) — расплющиваем.
    map_results: list[LlmRunResult] = []
    await _emit_progress(progress_cb, 0, parts_total)
    for wave_start in range(0, parts_total, _MAX_PARALLEL_CHUNKS):
        wave_idx = range(wave_start, min(wave_start + _MAX_PARALLEL_CHUNKS, parts_total))
        wave = [_map_one(chunks[i], i) for i in wave_idx]
        for part_results in await asyncio.gather(*wave):
            map_results.extend(part_results)
        await _emit_progress(progress_cb, min(parts_total, wave_start + _MAX_PARALLEL_CHUNKS), parts_total)

    partial_texts = [r.text for r in map_results]
    usages = [r.usage for r in map_results]

    # Диагностика: размеры выхода каждой части и модель — чтобы видеть, где
    # теряются данные (в map'ах или на сборке).
    for i, r in enumerate(map_results):
        log.warning(
            "QA_DIAG chunk_map part=%d/%d model=%s out_chars=%d",
            i + 1, parts_total, r.used_model.slug, len(r.text or ""),
        )

    # REDUCE — отдельный вызов: объединить, убрать дубли.
    red_system, red_user = _build_reduce_prompts(
        user_query, fallback_lang_name, partial_texts
    )
    reduce_in_chars = sum(len(t or "") for t in partial_texts)
    # Лимит вывода reduce = лимит тарифа (как у одиночного вызова): light 2000 /
    # balanced 4000 / deep 8000. Балансу 8000 не даём — это привилегия deep.
    reduce_result = await orchestrator_run(
        decision=decision,
        system_prompt=red_system,
        user_prompt=red_user,
        temperature=0.2,
    )
    usages.append(reduce_result.usage)
    log.warning(
        "QA_DIAG chunk_reduce model=%s in_chars=%d out_chars=%d finish=%s",
        reduce_result.used_model.slug, reduce_in_chars,
        len(reduce_result.text or ""), reduce_result.finish_reason,
    )

    combined_usage = _sum_usages(usages)
    combined_llm = LlmRunResult(
        text=reduce_result.text,
        usage=combined_usage,
        finish_reason=reduce_result.finish_reason,
        used_model=reduce_result.used_model,
        primary_model=decision.primary_model,
        attempted_models=reduce_result.attempted_models,
        fallback_reasons=reduce_result.fallback_reasons,
    )
    # Разбивка по вызовам: каждый map + reduce со СВОЕЙ моделью и usage —
    # чтобы биллинг тарифицировал каждый вызов по своей цене (важно при
    # fallback на другую модель).
    llm_calls = [(r.used_model.slug, r.usage) for r in map_results]
    llm_calls.append((reduce_result.used_model.slug, reduce_result.usage))
    log.warning(
        "qa.chunked parts=%d reduce_model=%s in_tok=%d out_tok=%d",
        parts_total, reduce_result.used_model.slug,
        combined_usage.input_tokens, combined_usage.output_tokens,
    )
    return QaRunResult(
        text=reduce_result.text,
        is_empty=False,
        llm=combined_llm,
        classification=classification,
        decision=decision,
        chunks_count=parts_total,
        llm_calls=llm_calls,
    )


async def run_qa(
    *,
    user_query: str,
    chat_name: str,
    text_messages: list[dict],
    fallback_language: str = "en",
    depth: str = TIER_LIGHT,
    requested_period_days: Optional[int] = None,
    explicit_category: Optional[str] = None,
    allow_heavy: bool = False,
    progress_cb: ProgressCb = None,
) -> QaRunResult:
    """
    Высокоуровневый Q&A вызов с автоматическим выбором модели.

    allow_heavy=True — пользователь подтвердил тяжёлый запрос (B2 red-зона):
    поднимаем потолок числа частей чанкования (_MAX_CHUNKS_HEAVY), чтобы
    обработать весь объём, а не молча обрезать хвостом.

    Шаги:
      1. Препроцессинг сообщений (drop emoji/short reactions, compact ts)
      2. Если контекст пуст → ранний выход с QaRunResult(is_empty=True)
      3. classifier.classify_query(user_query) → category
      4. routing.route(tier=depth, category, needs_structured) → decision
      5. Сборка system_prompt + user_prompt (тот же, что в старой
         summarize_chat_messages — те же системные инструкции про цитаты,
         msg-id, time-context, и т.д.)
      6. orchestrator.run(decision, ...) → LlmRunResult с фактической моделью
         после fallback'а если был

    Не делает billing — это ответственность endpoint'а:
      tokens = compute_tokens_for_llm_call(usage, rates)
      billing.debit(reason='qa_request', related_event_id=usage_event.id)
    """
    tier = normalize_tier(depth)
    fallback_lang_name = _lang_name(fallback_language)

    cleaned_messages, _stats = clean_telegram_messages(text_messages)
    context = _format_qa_chat_context(cleaned_messages)

    if not context:
        empty_text = _EMPTY_CHAT_MESSAGES[_normalize_lang_code(fallback_language)]
        return QaRunResult(
            text=empty_text,
            is_empty=True,
            llm=None,
            classification=None,
            decision=None,
        )

    classification = await classify_query(user_query)
    final_category, _was_overridden = _resolve_category(classification, explicit_category)

    decision = route(
        tier=tier,
        category=final_category,
        needs_structured_format=classification.needs_structured_format,
    )

    # Чанкование: если контекст не влезает в безопасный бюджет выбранной
    # модели И категория хорошо делится — режем на части, обрабатываем
    # волнами и собираем единый ответ (map-reduce). Иначе — один вызов.
    max_chunks = _MAX_CHUNKS_HEAVY if allow_heavy else _MAX_CHUNKS
    model_limit = getattr(decision.primary_model, "context_limit", 1_000_000)
    chunk_budget_chars = int(model_limit * _CHUNK_BUDGET_FACTOR * _CHARS_PER_TOKEN)
    if final_category in _CHUNKABLE_CATEGORIES and len(context) > chunk_budget_chars:
        return await _run_qa_chunked(
            cleaned_messages=cleaned_messages,
            user_query=user_query,
            chat_name=chat_name,
            fallback_lang_name=fallback_lang_name,
            fallback_language=fallback_language,
            requested_period_days=requested_period_days,
            decision=decision,
            classification=classification,
            chunk_budget_chars=chunk_budget_chars,
            max_chunks=max_chunks,
            progress_cb=progress_cb,
        )

    system_prompt = _build_qa_single_system_prompt(fallback_lang_name)
    oldest_date, newest_date = _extract_message_date_window(cleaned_messages)
    time_block = _build_time_context_block(
        requested_period_days=requested_period_days,
        oldest_msg_date=oldest_date,
        newest_msg_date=newest_date,
        fallback_lang_code=_normalize_lang_code(fallback_language),
    )
    user_prompt = (
        f"{time_block}\n\n"
        f"Chat name: {chat_name}\n\n"
        f"Chat messages (oldest to newest):\n{context}\n\n"
        f"User question:\n{user_query}"
    )

    try:
        llm_result = await orchestrator_run(
            decision=decision,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.2,
        )
    except LlmFatalError as exc:
        # B1 (одиночный путь): контекст влезал в основную модель одним вызовом,
        # но та упала, а fallback-модель с меньшим окном не вместила весь
        # контекст. Для делимых категорий переходим на чанкование под
        # безопасный бюджет цепочки. Для неделимых — пробрасываем (нужна
        # двухпроходная сборка, см. B3).
        if final_category in _CHUNKABLE_CATEGORIES and _is_context_overflow_fatal(
            exc, decision.primary_model
        ):
            safe_budget = max(
                1, _budget_chars_for_tokens(_chain_min_context_limit(decision))
            )
            log.warning(
                "qa.single_to_chunked reason=fallback_window_overflow "
                "safe_budget_chars=%d",
                safe_budget,
            )
            return await _run_qa_chunked(
                cleaned_messages=cleaned_messages,
                user_query=user_query,
                chat_name=chat_name,
                fallback_lang_name=fallback_lang_name,
                fallback_language=fallback_language,
                requested_period_days=requested_period_days,
                decision=decision,
                classification=classification,
                chunk_budget_chars=safe_budget,
                max_chunks=max_chunks,
                progress_cb=progress_cb,
            )
        raise

    return QaRunResult(
        text=llm_result.text,
        is_empty=False,
        llm=llm_result,
        classification=classification,
        decision=decision,
    )


async def _run_qa_group_chunked(
    *,
    chats_clean: list[tuple[str, list[dict]]],
    user_query: str,
    fallback_lang_name: str,
    fallback_language: str,
    requested_period_days: Optional[int],
    decision: RoutingDecision,
    classification: Optional[ClassificationResult],
    chunk_budget_chars: int,
    max_chunks: int = _MAX_CHUNKS,
    progress_cb: ProgressCb = None,
) -> "QaRunResult":
    """Групповое чанкование (map-reduce). ПРАВИЛО: каждый чат режется отдельно,
    сообщения разных чатов НИКОГДА не попадают в один кусок. Map — по кускам
    внутри чатов (волнами, с адаптивной до-нарезкой B1); reduce собирает ответ
    в групповом формате (## Chat / ## Summary), не смешивая чаты."""
    num_chats = len(chats_clean)
    # Потолок частей делим по чатам, чтобы суммарно не превысить max_chunks.
    per_chat_cap = max(1, max_chunks // max(1, num_chats))

    all_cleaned = [m for _, msgs in chats_clean for m in msgs]
    oldest_date, newest_date = _extract_message_date_window(all_cleaned)
    time_block = _build_time_context_block(
        requested_period_days=requested_period_days,
        oldest_msg_date=oldest_date,
        newest_msg_date=newest_date,
        fallback_lang_code=_normalize_lang_code(fallback_language),
    )
    # Map использует одиночный extraction-промпт (находки по ОДНОМУ чату).
    system_prompt = _build_qa_single_system_prompt(fallback_lang_name)
    safe_decision = _decision_without_primary(decision)
    safe_budget_chars = max(
        1, _budget_chars_for_tokens(_chain_min_context_limit(decision))
    )

    # Единицы map: (chat_index, chat_name, part_idx, parts_total, chunk_msgs).
    units: list[tuple] = []
    for ci, (name, msgs) in enumerate(chats_clean):
        chs = _chunk_messages_by_chars(msgs, chunk_budget_chars)
        if len(chs) > per_chat_cap:
            log.warning(
                "qa.group_chunked chat=%s over_cap parts=%d cap=%d — хвост",
                name, len(chs), per_chat_cap,
            )
            chs = chs[-per_chat_cap:]
        for pi, ch in enumerate(chs):
            units.append((ci, name, pi + 1, len(chs), ch))

    async def _map_unit(u_idx: int) -> tuple[int, list[LlmRunResult]]:
        ci, name, pi, pt, ch = units[u_idx]
        res = await _map_chunk_adaptive(
            decision=decision, system_prompt=system_prompt, time_block=time_block,
            chat_name=name, user_query=user_query, chunk_msgs=ch,
            part_idx=pi, parts_total=pt,
            safe_decision=safe_decision, safe_budget_chars=safe_budget_chars,
        )
        return u_idx, res

    results_by_unit: dict[int, list[LlmRunResult]] = {}
    units_total = len(units)
    await _emit_progress(progress_cb, 0, units_total)
    for wave_start in range(0, units_total, _MAX_PARALLEL_CHUNKS):
        wave = [
            _map_unit(i)
            for i in range(wave_start, min(wave_start + _MAX_PARALLEL_CHUNKS, units_total))
        ]
        for u_idx, res_list in await asyncio.gather(*wave):
            results_by_unit[u_idx] = res_list
        await _emit_progress(progress_cb, min(units_total, wave_start + _MAX_PARALLEL_CHUNKS), units_total)

    # Все map-результаты (для биллинга) + частичные находки, сгруппированные
    # по чату в порядке частей.
    map_results_all: list[LlmRunResult] = []
    for u_idx in range(len(units)):
        map_results_all.extend(results_by_unit.get(u_idx, []))

    per_chat_partials: list[tuple[str, list[str]]] = []
    for ci, (name, _msgs) in enumerate(chats_clean):
        texts: list[str] = []
        for u_idx, u in enumerate(units):
            if u[0] != ci:
                continue
            for r in results_by_unit.get(u_idx, []):
                if (r.text or "").strip():
                    texts.append(r.text)
        per_chat_partials.append((name, texts))

    for i, r in enumerate(map_results_all):
        log.warning(
            "QA_DIAG group_chunk_map part=%d/%d model=%s out_chars=%d",
            i + 1, len(map_results_all), r.used_model.slug, len(r.text or ""),
        )

    red_system, red_user = _build_group_reduce_prompts(
        user_query, fallback_lang_name, per_chat_partials
    )
    reduce_result = await orchestrator_run(
        decision=decision, system_prompt=red_system,
        user_prompt=red_user, temperature=0.2,
    )
    log.warning(
        "QA_DIAG group_chunk_reduce model=%s chats=%d parts=%d out_chars=%d finish=%s",
        reduce_result.used_model.slug, num_chats, len(units),
        len(reduce_result.text or ""), reduce_result.finish_reason,
    )

    usages = [r.usage for r in map_results_all] + [reduce_result.usage]
    combined_usage = _sum_usages(usages)
    combined_llm = LlmRunResult(
        text=reduce_result.text,
        usage=combined_usage,
        finish_reason=reduce_result.finish_reason,
        used_model=reduce_result.used_model,
        primary_model=decision.primary_model,
        attempted_models=reduce_result.attempted_models,
        fallback_reasons=reduce_result.fallback_reasons,
    )
    llm_calls = [(r.used_model.slug, r.usage) for r in map_results_all]
    llm_calls.append((reduce_result.used_model.slug, reduce_result.usage))
    return QaRunResult(
        text=reduce_result.text,
        is_empty=False,
        llm=combined_llm,
        classification=classification,
        decision=decision,
        chunks_count=len(units),
        llm_calls=llm_calls,
    )


async def run_qa_group(
    *,
    user_query: str,
    chats: list[dict],
    fallback_language: str = "en",
    depth: str = TIER_LIGHT,
    requested_period_days: Optional[int] = None,
    explicit_category: Optional[str] = None,
    allow_heavy: bool = False,
    progress_cb: ProgressCb = None,
) -> QaRunResult:
    """
    Высокоуровневый групповой Q&A вызов (несколько чатов в одном запросе).

    chats — список dict'ов формата {"chat_name": str, "text_messages": list[dict]}.

    Логика system_prompt'а на multi-chat ответ (## Chat: <name> + ## Summary)
    сохранена из старой summarize_chat_messages_group.
    """
    tier = normalize_tier(depth)
    fallback_lang_name = _lang_name(fallback_language)

    if not chats:
        return QaRunResult(
            text=_EMPTY_GROUP_MESSAGE[_normalize_lang_code(fallback_language)],
            is_empty=True,
            llm=None,
            classification=None,
            decision=None,
        )

    sections: list[str] = []
    chat_names: list[str] = []
    all_cleaned: list[dict] = []
    chats_clean: list[tuple[str, list[dict]]] = []  # для чанкования (B4)
    for idx, c in enumerate(chats, start=1):
        chat_name = (c.get("chat_name") or "").strip() or f"Chat {idx}"
        text_messages = c.get("text_messages") or []
        cleaned, _stats = clean_telegram_messages(text_messages)
        section = _build_group_chat_section(
            chat_index=idx,
            chat_name=chat_name,
            cleaned_messages=cleaned,
        )
        if section:
            sections.append(section)
            chat_names.append(chat_name)
            all_cleaned.extend(cleaned)
            chats_clean.append((chat_name, cleaned))

    if not sections:
        return QaRunResult(
            text=_EMPTY_GROUP_MESSAGE[_normalize_lang_code(fallback_language)],
            is_empty=True,
            llm=None,
            classification=None,
            decision=None,
        )

    classification = await classify_query(user_query)
    final_category, _was_overridden = _resolve_category(classification, explicit_category)

    # Safety-net для группового: если классификатор почему-то выдал simple_qa,
    # а реально пользователь шлёт N≥2 чатов — это явно cross_chat_analysis.
    if final_category == DEFAULT_CATEGORY and len(sections) >= 2:
        from .classifier import CATEGORY_CROSS_CHAT_ANALYSIS
        final_category = CATEGORY_CROSS_CHAT_ANALYSIS

    decision = route(
        tier=tier,
        category=final_category,
        needs_structured_format=classification.needs_structured_format,
    )

    system_prompt = _build_qa_group_system_prompt(fallback_lang_name)
    combined_context = "\n\n".join(sections)

    # B4: если суммарный контекст не влезает в бюджет модели — чанкуем, строго
    # ВНУТРИ каждого чата (чаты не смешиваем), затем сводим. Для группы это
    # применимо к любой категории: ответ всё равно структурирован по чатам.
    max_chunks = _MAX_CHUNKS_HEAVY if allow_heavy else _MAX_CHUNKS
    model_limit = getattr(decision.primary_model, "context_limit", 1_000_000)
    chunk_budget_chars = int(model_limit * _CHUNK_BUDGET_FACTOR * _CHARS_PER_TOKEN)
    if len(combined_context) > chunk_budget_chars:
        return await _run_qa_group_chunked(
            chats_clean=chats_clean,
            user_query=user_query,
            fallback_lang_name=fallback_lang_name,
            fallback_language=fallback_language,
            requested_period_days=requested_period_days,
            decision=decision,
            classification=classification,
            chunk_budget_chars=chunk_budget_chars,
            max_chunks=max_chunks,
            progress_cb=progress_cb,
        )

    oldest_date, newest_date = _extract_message_date_window(all_cleaned)
    time_block = _build_time_context_block(
        requested_period_days=requested_period_days,
        oldest_msg_date=oldest_date,
        newest_msg_date=newest_date,
        fallback_lang_code=_normalize_lang_code(fallback_language),
    )
    user_prompt = (
        f"{time_block}\n\n"
        f"Number of chats: {len(sections)}\n"
        f"Chat names (in order): {'; '.join(chat_names)}\n\n"
        f"Chat history (each chat in its own labelled section):\n\n"
        f"{combined_context}\n\n"
        f"User question:\n{user_query}"
    )

    llm_result = await orchestrator_run(
        decision=decision,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
    )

    return QaRunResult(
        text=llm_result.text,
        is_empty=False,
        llm=llm_result,
        classification=classification,
        decision=decision,
    )


# ---------------------------------------------------------------------------
# Helpers для нового pipeline'а (используются run_qa и run_qa_group)
# ---------------------------------------------------------------------------


def _format_qa_chat_context(cleaned_messages: list[dict]) -> str:
    """
    Сформировать context-строку «по строке на сообщение».
    Формат: `[date] [msg:ID] [reply→msg:PID] [author:SENDER_ID]: text`.

    Автор передаётся НЕ именем, а токеном `[author:<sender_id>]` (числовой id,
    бесплатный из Telegram). Имя/логин не разрешается на этапе выгрузки (это
    тысячи лишних запросов и долгое зависание) — `@логин` подставляется уже
    ПОСЛЕ ответа LLM только для процитированных авторов (см. main.py,
    подстановку через resolve_sender_logins). LLM инструктируется копировать
    токен `[author:ID]` дословно, как и `[msg:ID]`.

    Fallback: если sender_id отсутствует — используем имя из поля `from`
    (совместимость со старым форматом).
    """
    lines: list[str] = []
    for msg in cleaned_messages:
        date = msg.get("date") or ""
        sender_name = msg.get("from")
        sender_id = msg.get("sender_id")
        if sender_name:
            # Имя уже известно (резолв на выгрузке или бесплатный кэш) —
            # отдаём его как есть, ответ выглядит как раньше.
            author_token = sender_name
        elif sender_id is not None:
            # Имени нет — кладём токен, @логин подставится после ответа LLM.
            try:
                author_token = f"[author:{int(sender_id)}]"
            except (TypeError, ValueError):
                author_token = "Unknown"
        else:
            author_token = "Unknown"
        text = msg.get("text") or ""
        msg_id = msg.get("message_id")
        reply_to = msg.get("reply_to")
        prefix_parts = [f"[{date}]"]
        if msg_id is not None:
            prefix_parts.append(f"[msg:{int(msg_id)}]")
        if reply_to:
            try:
                prefix_parts.append(f"[reply→msg:{int(reply_to)}]")
            except (TypeError, ValueError):
                pass
        prefix = " ".join(prefix_parts)
        lines.append(f"{prefix} {author_token}: {text}")
    return "\n".join(lines)


def _build_qa_single_system_prompt(fallback_lang_name: str) -> str:
    """
    System-prompt для одиночного Q&A. Идентичен тексту в старой
    summarize_chat_messages — извлечён для переиспользования.
    Менять разрешено только синхронно с регрессионными тестами Q1-Q10.
    """
    return (
        "You are CoTel, an expert analyst of Telegram chat conversations. "
        "Users come to you to find specific information, patterns, or "
        "insights in their chat history that would be tedious to find "
        "manually.\n\n"
        "For this query: read the provided chat fragment, find messages "
        "that are semantically relevant to the user's question, and "
        "produce a focused answer grounded in those messages.\n\n"
        "HOW TO ANSWER\n"
        "1. Identify messages that are semantically relevant (not just "
        "keyword matches). Consider synonyms, paraphrases, emoji, "
        "transliteration.\n"
        "2. Organize findings by theme or timeline — whichever better "
        "fits the question.\n"
        "3. When referencing a specific message, cite it with this "
        "format:\n"
        "       @username: \"short verbatim quote\" [msg:ID]\n"
        "   - ID is the exact numeric id taken from the [msg:ID] token "
        "that precedes the message in the chat fragment below. Copy it "
        "verbatim. Do NOT invent ids and do NOT cite a message that has "
        "no [msg:ID] token.\n"
        "   - Place the [msg:ID] token AT THE END of the citation, "
        "immediately after the closing quote — not before the username "
        "and not before the quote itself.\n"
        "   - If the same message is referenced multiple times in your "
        "answer, repeat the same [msg:ID] each time.\n"
        "   - Keep quotes short and in the original language of the "
        "message.\n"
        "   - Some messages have an extra [reply→msg:PARENT_ID] token "
        "after their own [msg:ID]. This means the message is a reply "
        "to the message with that PARENT_ID. Use this to reconstruct "
        "conversation threads when relevant, but do NOT cite the "
        "[reply→msg:...] token itself — only the message's own "
        "[msg:ID].\n"
        "4. If the chat contains conflicting information (different "
        "people say different things), surface the conflict — do not "
        "flatten it.\n"
        "5. If relevant messages are sparse (e.g. only 3 out of 400 are "
        "actually relevant), say so up front so the user calibrates "
        "expectations.\n\n"
        "CITATIONS\n"
        "- No more than 3 citations per sub-topic. If more relevant "
        "messages exist, pick the most representative ones.\n"
        "- For the remaining (un-cited) relevant messages on the same "
        "sub-topic, summarize what they add in the conclusion or "
        "wrap-up of that sub-topic — so the user knows what the "
        "uncited messages say without seeing each one quoted.\n\n"
        "RULES\n"
        "- Ground every claim in the provided messages. Never invent "
        "participants, dates, events, or details that are not in the "
        "input.\n"
        "- If the input is insufficient, say so plainly. Do not "
        "speculate.\n"
        "- Quote messages verbatim in their original language. Write "
        "your own analysis and conclusions in the SAME LANGUAGE as the "
        "user's question. If the language of the question is ambiguous "
        "(one word, only emoji, mixed languages, too short to tell), "
        f"respond in {fallback_lang_name}.\n"
        "- No preamble. Do not restate the question.\n\n"
        "LENGTH\n"
        "- Target: 1000-1500 characters. HARD LIMIT: 2000 characters.\n"
        "- If you would exceed the limit, prioritize: direct answer "
        "first, citations second, context-setting last.\n"
        "- Structure: 3–6 short paragraphs OR a bulleted list of 3–8 "
        "items, whichever better suits the question.\n\n"
        "OUTPUT FORMAT: plain text. No Markdown headings, no JSON "
        "wrapper."
    )


def _build_qa_group_system_prompt(fallback_lang_name: str) -> str:
    """
    System-prompt для группового (multi-chat) Q&A.
    Идентичен тексту в старой summarize_chat_messages_group.
    """
    return (
        "You are CoTel, an expert analyst of Telegram chat "
        "conversations. The user has selected MULTIPLE chats and asked "
        "one question. Your job: answer the question SEPARATELY for "
        "each chat, plus give a short overall conclusion.\n\n"
        "INPUT FORMAT\n"
        "The chat history below is divided into sections, each starting "
        "with a marker line of the form:\n"
        "    === CHAT N: «chat name» ===\n"
        "Each section contains messages from that single chat, in the "
        "same per-message format as single-chat analysis: "
        "[date] [msg:ID] [reply→msg:PARENT_ID] sender: text.\n\n"
        "OUTPUT FORMAT (markdown)\n"
        "Produce one section per chat, in the same order they appear in "
        "the input, using THIS EXACT heading format:\n"
        "    ## Chat: <chat name>\n"
        "Use the chat name verbatim from the «...» marker. After all "
        "chat sections, add a final section:\n"
        "    ## Summary\n"
        "with a 2-3 sentence overall conclusion across all chats.\n\n"
        "If the user's question is in Russian, translate the section "
        "labels accordingly: use `## Чат: ...` and `## Общий вывод`.\n\n"
        "ANSWER RULES (per chat section)\n"
        "- 200-400 words per chat. Be concrete, not generic.\n"
        "- Ground every claim in messages from THAT chat only. Do not "
        "mix evidence between chats.\n"
        "- Cite using `[msg:ID]` exactly as it appears in the chat's "
        "section. IDs are unique within a chat but may collide across "
        "chats — never carry an ID from one chat into another section.\n"
        "- If a chat has no information relevant to the question, "
        "write a single short sentence saying so. Do not pad.\n"
        "- Do not duplicate the same point across multiple chats; if "
        "two chats discuss the same thing, say so in the Summary, not "
        "in each chat section.\n\n"
        "LANGUAGE\n"
        "- Respond in the SAME LANGUAGE as the user's question. If the "
        "question's language is ambiguous (one word, only emoji, mixed "
        f"languages, too short to tell), respond in {fallback_lang_name}.\n"
        "- Quote messages verbatim in their original language.\n\n"
        "NO PREAMBLE. Do not restate the question. Do not list the "
        "chats up front — just start with the first `## Chat: ...` "
        "section."
    )
