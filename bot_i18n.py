"""
CoTel — bot wrapper i18n.

Лёгкий модуль без зависимостей. Содержит:
- словари `_STRINGS` для EN и RU: шапка, счётчик, хвост, имена месяцев и дней недели;
- `normalize_language` — приводит любой вход к 'en' или 'ru';
- `t(key, language, **params)` — ключ → строка, с подстановкой;
- `months(language)`, `weekdays(language)` — для форматирования дат.

UI-язык бота жёстко EN/RU. Цитаты из чатов, ссылки, digest_text от LLM —
остаются в своём языке и через этот модуль не проходят.
"""
from __future__ import annotations

from typing import Any, Optional


_STRINGS: dict[str, dict[str, Any]] = {
    "en": {
        "match_header": "New matches for your subscription: {name}",
        "match_count": "Matches: {count}",
        "remaining_links_header": "\n\nMore matches (links):",
        "tg_limit_truncated": "\n…(truncated due to Telegram message size limit)",
        "digest_title": "Summary for your subscription: {name}",
        "digest_period": "Period: {period}",
        "digest_period_empty": "Period: —",
        "months": [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ],
        "weekdays": [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ],
    },
    "ru": {
        "match_header": "Найдены события по подписке: {name}",
        "match_count": "Совпадений: {count}",
        "remaining_links_header": "\n\nОстальные совпадения (ссылками):",
        "tg_limit_truncated": "\n…(дальше не влезло по лимиту Telegram)",
        "digest_title": "Резюме по подписке: {name}",
        "digest_period": "Период: {period}",
        "digest_period_empty": "Период: —",
        "months": [
            "января", "февраля", "марта", "апреля", "мая", "июня",
            "июля", "августа", "сентября", "октября", "ноября", "декабря",
        ],
        "weekdays": [
            "понедельник", "вторник", "среда", "четверг",
            "пятница", "суббота", "воскресенье",
        ],
    },
}


def normalize_language(value: Optional[str]) -> str:
    """Приводит любой вход к 'en' или 'ru'. По умолчанию 'en'."""
    if not value:
        return "en"
    v = str(value).strip().lower()
    if v.startswith("ru"):
        return "ru"
    return "en"


def t(key: str, language: Optional[str], **params: Any) -> str:
    """
    Достаёт шаблон по ключу и подставляет параметры.
    Фолбэки: язык → 'en' → сам ключ.
    """
    lang = normalize_language(language)
    tmpl = _STRINGS[lang].get(key)
    if tmpl is None:
        tmpl = _STRINGS["en"].get(key)
    if tmpl is None:
        return key
    if params:
        return tmpl.format(**params)
    return tmpl


def months(language: Optional[str]) -> list[str]:
    return _STRINGS[normalize_language(language)]["months"]


def weekdays(language: Optional[str]) -> list[str]:
    return _STRINGS[normalize_language(language)]["weekdays"]
