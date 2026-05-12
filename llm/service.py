from __future__ import annotations

import json
from typing import Any, Optional, Union

from .models import resolve_model_config, DEFAULT_AI_MODEL
from .adapters import get_adapter
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
    lines = []
    for msg in text_messages:
        date = msg.get("date") or ""
        sender = msg.get("from") or "Unknown"
        text = msg.get("text") or ""
        lines.append(f"[{date}] {sender}: {text}")

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
        "       @username: \"short verbatim quote\"\n"
        "   Keep quotes short and in the original language of the "
        "message.\n"
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
