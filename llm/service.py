from __future__ import annotations

import json
from typing import Any, Optional, Union

from .models import resolve_model_config, DEFAULT_AI_MODEL
from .adapters import get_adapter
from .preprocessing import clean_telegram_messages
from .usage import (
    LlmUsage,
    LlmTextResult,
    LlmJsonResult,
    TOKENS_SOURCE_EMPTY,
)


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
    return_usage: bool = False,
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
        "- Keep the answer tight: 3–6 short paragraphs OR a bulleted "
        "list of 3–8 items, whichever better suits the question.\n"
        "- No preamble. Do not restate the question.\n\n"
        "OUTPUT FORMAT: plain text. No Markdown headings, no JSON "
        "wrapper."
    )

    user_prompt = (
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
        max_output_tokens=1500,
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

    user_prompt = (
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
        "STRUCTURE YOUR OUTPUT\n"
        "- If the user asked a specific question, answer it directly.\n"
        "- If the user asked for a general overview, organize by TOPIC "
        "(not chronologically, message-by-message). 2–4 topics is "
        "usually right.\n"
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
        f"User query for the summary:\n{prompt}"
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
