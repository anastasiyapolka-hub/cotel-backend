"""
Preprocessing for Telegram message history before it goes into an LLM.

Purpose: shrink the token footprint of chat history without losing
analytical signal. Applied at the LLM-service boundary so every provider
(OpenAI, Anthropic, Gemini) benefits uniformly.

What this module does:
  - Drops emoji-only messages ("🔥🔥", "👍")
  - Drops short reactions ("ага", "ок", "+", "lol", "yes", "kk")
  - Drops Telegram system-message text patterns that occasionally leak
    through (in case a service-message text isn't filtered upstream)
  - Compacts ISO timestamps to "YYYY-MM-DD HH:MM" (drops seconds and
    timezone offset)
  - Reports stats so callers can measure savings

What this module does NOT do (yet):
  - Reply-quote dedup — Telethon's `msg.text` does NOT duplicate the
    parent message's text, so there is nothing to strip here. The
    `reply_to` linkage is preserved as a structural field in the
    caller's format string.
  - Ad/news detection — out of scope for v1
  - URL stripping — kept verbatim (CoTel surfaces them in answers)
  - Sticker placeholders — Telethon already returns empty `.message`
    for sticker-only messages, which the fetch layer drops

Design notes:
  - Pure functions, no I/O, no DB, no Telethon dependency.
  - Key names are configurable so the same cleaner serves QA (uses
    `date`) and subscription flows (use `message_ts`) if/when we
    decide to wire it in there too.
  - The reaction lists are intentionally short and conservative.
    Better to leave a borderline message in than to remove something
    meaningful — the goal is cheap token reduction, not perfect
    classification.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Short-reaction dictionaries
# ---------------------------------------------------------------------------
#
# Words that, when they are the ENTIRE message (after stripping
# punctuation), carry no analytical information for a chat-history
# query. Conservative on purpose — when in doubt, leave the message.

_REACTIONS_RU = {
    "ага", "угу", "уга", "ну", "нуу", "нуу", "не", "неа", "нет", "да",
    "ок", "окей", "окк", "оке", "ок-ок",
    "понял", "поняла", "понятно", "ясно", "пон",
    "лол", "лоол", "лоооол", "лолки", "кек", "кеек", "ору", "ржу",
    "хах", "хаха", "ахах", "ахаха", "хахаха", "ахахаха", "хе", "хех",
    "норм", "норма", "норманн", "хорошо", "хор", "плюс", "плюсую",
    "круто", "класс", "топ", "топчик", "офф", "офк",
    "так", "вот", "хм", "хмм", "хммм", "ммм",
    "спс", "спасибо", "сэнкс", "пасиб",
}

_REACTIONS_EN = {
    "ok", "okay", "k", "kk", "kkk",
    "yes", "yep", "yup", "yeah", "ye",
    "no", "nope", "nah", "naw",
    "thanks", "thx", "ty", "tysm",
    "lol", "lmao", "rofl", "lel", "kek",
    "haha", "hehe", "hahaha", "hahah",
    "wow", "woah", "whoa", "cool", "nice", "great", "good", "gg",
    "hi", "hello", "hey", "yo", "bye", "cya",
    "true", "fr", "frfr", "facts", "based",
    "+1", "+", "-1",
}

_REACTIONS = _REACTIONS_RU | _REACTIONS_EN

# Trailing punctuation to ignore when matching a reaction word.
# We do NOT strip leading punctuation — "!ok" looks weirder than "ok!"
# and is more likely to be intentional emphasis.
_TRAILING_PUNCT = ".,!?;:)(\"'`~-_…"

# A reaction must be short: anything beyond this length is treated as
# real content even if the word matches the dictionary.
_REACTION_MAX_LEN = 12


# ---------------------------------------------------------------------------
# Telegram system-message patterns
# ---------------------------------------------------------------------------
#
# Telethon usually returns these as MessageService, which the fetch
# layer already drops via `isinstance(msg, Message)`. But occasionally
# they leak through (e.g. Bot API exports, edge cases in older
# Telethon versions) — this is a defensive net.

_SYSTEM_PATTERNS = [
    # Russian
    re.compile(r"^.{1,80}\s+(присоедин|вошл|вступил|подключил)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+(покинул|вышел|удал[её]н|исключ[её]н|удалил[ао]?\s+чат)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+(был[аи]?\s+)?(добавл[её]н|удал[её]н|исключ[её]н)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+изменил[ао]?\s+(фото|название|имя|описание|тему)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+(закрепил|открепил|удалил)[ао]?\s+сообщ", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+создал[ао]?\s+(групп|канал|чат|тему)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+обновил[ао]?\s+фото", re.IGNORECASE),
    # English
    re.compile(r"^.{1,80}\s+joined\s+(the\s+)?(group|channel|chat)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+left\s+(the\s+)?(group|channel|chat)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+(was|were)\s+(added|removed|kicked|banned)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+(added|removed|kicked|banned|invited)\s+\S+", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+changed\s+(the\s+)?(group|chat)?\s*(photo|title|name|description)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+(pinned|unpinned)\s+(a\s+|the\s+)?message", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+created\s+(the\s+|a\s+)?(group|channel|chat|topic)", re.IGNORECASE),
    re.compile(r"^.{1,80}\s+deleted\s+(a\s+|the\s+)?message", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Stats container
# ---------------------------------------------------------------------------

@dataclass
class PreprocessStats:
    """Counters returned alongside the cleaned messages."""
    input_count: int = 0
    output_count: int = 0
    dropped_empty: int = 0
    dropped_emoji_only: int = 0
    dropped_short_reaction: int = 0
    dropped_system: int = 0
    timestamps_normalized: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def dropped_total(self) -> int:
        return (
            self.dropped_empty
            + self.dropped_emoji_only
            + self.dropped_short_reaction
            + self.dropped_system
        )

    @property
    def reduction_pct(self) -> float:
        """Percentage of messages dropped, 0.0 if input was empty."""
        if not self.input_count:
            return 0.0
        return round(100.0 * self.dropped_total / self.input_count, 1)

    def to_dict(self) -> dict:
        return {
            "input_count": self.input_count,
            "output_count": self.output_count,
            "dropped_empty": self.dropped_empty,
            "dropped_emoji_only": self.dropped_emoji_only,
            "dropped_short_reaction": self.dropped_short_reaction,
            "dropped_system": self.dropped_system,
            "dropped_total": self.dropped_total,
            "reduction_pct": self.reduction_pct,
            "timestamps_normalized": self.timestamps_normalized,
        }


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

def is_emoji_only(text: str) -> bool:
    """
    True if `text` contains no Unicode letters and no digits.

    Pure emoji / punctuation / symbols → True (drop).
    Anything with at least one letter or digit → False (keep).

    Why "no letters AND no digits": "5" or "100500" without context
    is rare in chats but occasionally meaningful (timestamps, prices).
    We keep it just in case. Pure emoji like "🔥🔥🔥" → all chars are
    symbols → no letters/digits → True.
    """
    stripped = text.strip()
    if not stripped:
        return True
    for ch in stripped:
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("N"):
            return False
    return True


def is_short_reaction(text: str) -> bool:
    """
    True if `text` is a single short word that matches the reaction
    dictionary (case-insensitive, trailing punctuation ignored).

    We deliberately do NOT match multi-word phrases — once it's two
    words, it's almost always content.
    """
    cleaned = text.strip().lower()
    if not cleaned or len(cleaned) > _REACTION_MAX_LEN:
        return False
    # Don't match if it contains internal whitespace — that means
    # multiple words, which is real content.
    if any(c.isspace() for c in cleaned):
        return False
    # Strip trailing punctuation only; "ok!" → "ok"
    word = cleaned.rstrip(_TRAILING_PUNCT)
    if not word:
        return False
    return word in _REACTIONS


def is_system_message(text: str) -> bool:
    """True if the text matches a known Telegram service-event pattern."""
    cleaned = text.strip()
    if not cleaned:
        return False
    return any(p.search(cleaned) for p in _SYSTEM_PATTERNS)


def normalize_timestamp(value: Optional[str]) -> Optional[str]:
    """
    Compact an ISO 8601 timestamp to "YYYY-MM-DD HH:MM".

    Drops seconds, microseconds, and timezone offset — these don't
    help LLM analysis of chat history and waste tokens.

    Examples:
      "2026-05-20T14:30:45+00:00" → "2026-05-20 14:30"
      "2026-05-20T14:30:00.000Z"  → "2026-05-20 14:30"
      "2026-05-20 14:30"          → "2026-05-20 14:30" (already compact)

    If the input can't be parsed, returns it untouched — better to
    keep a working but verbose timestamp than to corrupt it.
    """
    if not value:
        return value
    raw = str(value).strip()
    if not raw:
        return raw

    # Fast path: try to parse as ISO 8601 (datetime.fromisoformat
    # handles most of what Telethon's .isoformat() produces).
    try:
        # Normalize trailing 'Z' since fromisoformat <3.11 chokes on it.
        candidate = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        dt = datetime.fromisoformat(candidate)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        pass

    # Fallback: best-effort textual truncation.
    head = raw.split(".")[0]  # drop microseconds
    for sep in ("+", "Z"):
        idx = head.find(sep, 10)  # skip the "YYYY-MM-DD" part
        if idx != -1:
            head = head[:idx]
            break
    head = head.replace("T", " ")
    # Trim "HH:MM:SS" → "HH:MM"
    if len(head) >= 16 and head[13] == ":" and head[16:17] in (":", ""):
        head = head[:16]
    return head


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def clean_telegram_messages(
    messages: list[dict],
    *,
    text_key: str = "text",
    date_key: str = "date",
    normalize_dates: bool = True,
) -> tuple[list[dict], PreprocessStats]:
    """
    Apply CoTel's chat-history preprocessing pipeline.

    Returns a NEW list (does not mutate input) and a PreprocessStats
    object with counts of what was dropped.

    Filtering order matters for stats accuracy:
      1) empty text     → dropped_empty
      2) emoji-only     → dropped_emoji_only
      3) short reaction → dropped_short_reaction
      4) system message → dropped_system

    `normalize_dates=True` (default) compacts ISO timestamps to
    "YYYY-MM-DD HH:MM". Set to False if downstream code re-parses the
    timestamp string with strict ISO assumptions (we do this for the
    subscription classify/digest flows that round-trip ts through the
    LLM and into the DB).
    """
    stats = PreprocessStats(input_count=len(messages))
    cleaned: list[dict] = []

    for msg in messages:
        text = (msg.get(text_key) or "").strip() if msg else ""
        if not text:
            stats.dropped_empty += 1
            continue
        if is_emoji_only(text):
            stats.dropped_emoji_only += 1
            continue
        if is_short_reaction(text):
            stats.dropped_short_reaction += 1
            continue
        if is_system_message(text):
            stats.dropped_system += 1
            continue

        new_msg = dict(msg)
        # Re-write the stripped text back so downstream gets the
        # trimmed version (some upstream callers don't strip).
        new_msg[text_key] = text

        if normalize_dates:
            raw_date = msg.get(date_key)
            if raw_date:
                compact = normalize_timestamp(raw_date)
                if compact != raw_date:
                    stats.timestamps_normalized += 1
                new_msg[date_key] = compact

        cleaned.append(new_msg)

    stats.output_count = len(cleaned)
    return cleaned, stats
