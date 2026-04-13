from __future__ import annotations

import json
import os
from typing import Any

from anthropic import AsyncAnthropic
from openai import AsyncOpenAI

from .models import resolve_model_config, DEFAULT_AI_MODEL


openai_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
anthropic_client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


def _extract_anthropic_text(response: Any) -> str:
    parts: list[str] = []

    for block in getattr(response, "content", []) or []:
        text = getattr(block, "text", None)
        if isinstance(text, str) and text.strip():
            parts.append(text)

    return "\n".join(parts).strip()


def _safe_parse_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start:end + 1])
        raise


async def _chat_text_completion(
    *,
    ai_model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_output_tokens: int | None = None,
) -> str:
    config = resolve_model_config(ai_model)

    if config.provider == "openai":
        completion = await openai_client.chat.completions.create(
            model=config.provider_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        text = completion.choices[0].message.content or ""
        return text.strip()

    if config.provider == "anthropic":
        if anthropic_client is None:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")

        response = await anthropic_client.messages.create(
            model=config.provider_model,
            system=system_prompt,
            max_tokens=max_output_tokens or 1200,
            temperature=temperature,
            messages=[
                {
                    "role": "user",
                    "content": user_prompt,
                }
            ],
        )
        return _extract_anthropic_text(response)

    raise RuntimeError(f"Unsupported provider: {config.provider}")


async def summarize_chat_messages(
    *,
    user_query: str,
    chat_name: str,
    text_messages: list[dict],
    ai_model: str = DEFAULT_AI_MODEL,
) -> str:
    lines = []
    for msg in text_messages:
        date = msg.get("date") or ""
        sender = msg.get("from") or "Unknown"
        text = msg.get("text") or ""
        lines.append(f"[{date}] {sender}: {text}")

    context = "\n".join(lines)

    if not context:
        return "В чате нет текстовых сообщений для анализа."

    system_prompt = (
        "Ты аналитик переписок в Telegram.\n"
        "Тебе даётся фрагмент чата и запрос пользователя.\n"
        "Найди по смыслу релевантные сообщения и дай краткое, структурированное "
        "summary по-русски. Если информации мало, честно скажи об этом."
    )

    user_prompt = (
        f"Название чата: {chat_name}\n\n"
        f"Запрос пользователя:\n{user_query}\n\n"
        "Ниже переписка (от старых к новым сообщениям):\n\n"
        f"{context}\n\n"
        "Сделай ответ именно по запросу выше. Структурируй ответ в 3–6 абзацев или списком."
    )

    return await _chat_text_completion(
        ai_model=ai_model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
        max_output_tokens=1400,
    )


async def classify_subscription_matches(
    *,
    prompt: str,
    chat_title: str,
    messages: list[dict],
    ai_model: str = DEFAULT_AI_MODEL,
) -> dict:
    lines = []
    for m in messages:
        mid = m.get("message_id")
        ts = m.get("message_ts")
        a = m.get("author_display") or "Unknown"
        txt = m.get("text") or ""
        lines.append(f"[{mid}] [{ts}] {a}: {txt}")

    context = "\n".join(lines)
    if not context:
        return {
            "found": False,
            "matches": [],
            "summary_reason": "Нет текстовых сообщений.",
            "confidence": 0.0,
        }

    system_prompt = """
Ты — классификатор сообщений Telegram для событийных подписок.
Твоя задача: по списку сообщений найти те, которые соответствуют запросу пользователя ПО СМЫСЛУ.

Учитывай:
- синонимы, опечатки, латиницу/кириллицу, разговорные формулировки, сокращения
- НЕ требуй точного совпадения ключевых слов
- НЕ выдумывай автора/время/текст: используй только то, что дано во входных данных

Если сообщение релевантно — верни его как match.
В match:
- excerpt: это цитата сообщения (обрезай до 300 символов, без пересказа)
- reason: 1 короткая причина “почему подходит”

Если совпадений нет — found=false и matches=[]
message_id бери только из входных строк вида [12345].
Формат ответа: СТРОГО JSON без пояснений/markdown/кода/комментариев.
""".strip()

    user_prompt = (
        f"Название чата: {chat_title}\n\n"
        f"Запрос подписки пользователя:\n{prompt}\n\n"
        f"Новые сообщения (каждая строка содержит message_id в квадратных скобках):\n{context}\n\n"
        "Верни JSON формата:\n"
        "{\n"
        '  "found": true/false,\n'
        '  "matches": [\n'
        "    {\n"
        '      "message_id": 123,\n'
        '      "message_ts": "ISO8601",\n'
        '      "author_display": "string",\n'
        '      "author_id": 123,\n'
        '      "excerpt": "string",\n'
        '      "reason": "string"\n'
        "    }\n"
        "  ],\n"
        '  "summary_reason": "string",\n'
        '  "confidence": 0.0\n'
        "}\n"
    )

    raw = await _chat_text_completion(
        ai_model=ai_model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.1,
        max_output_tokens=1800,
    )
    return _safe_parse_json(raw)


async def build_subscription_digest(
    *,
    prompt: str,
    chat_title: str,
    messages: list[dict],
    ai_model: str = DEFAULT_AI_MODEL,
) -> dict:
    lines = []
    for m in messages:
        mid = m.get("message_id")
        ts = m.get("message_ts")
        a = m.get("author_display") or "Unknown"
        txt = m.get("text") or ""
        r = m.get("reply_to")
        reply_tag = f" reply_to={int(r)}" if r else ""
        lines.append(f"[{mid}] [{ts}] {a}{reply_tag}: {txt}")

    context = "\n".join(lines)
    if not context:
        return {"digest_text": "", "confidence": 0.0}

    system_prompt = (
        "Ты — аналитик Telegram-диалогов.\n"
        "Тебе дан фрагмент чата (сообщения с reply_to=<id>, если это ответ) и запрос пользователя.\n"
        "Сделай резюме строго по запросу пользователя, без домыслов.\n"
        "Важно: не пересказывай весь чат подряд; выделяй только главное, группируй, делай выводы о почитанном.\n"
        "Если данных недостаточно — так и скажи.\n"
        "Ответ дай СТРОГО JSON без markdown.\n"
    )

    user_prompt = (
        f"Название чата: {chat_title}\n\n"
        f"Запрос пользователя для summary:\n{prompt}\n\n"
        "Сообщения (каждая строка содержит message_id в квадратных скобках, reply_to если есть):\n"
        f"{context}\n\n"
        "Верни JSON:\n"
        "{\n"
        '  "digest_text": "строка 500..4096 символов, максимум 4096",\n'
        '  "confidence": 0.0\n'
        "}\n"
    )

    raw = await _chat_text_completion(
        ai_model=ai_model,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        temperature=0.2,
        max_output_tokens=1800,
    )
    return _safe_parse_json(raw)