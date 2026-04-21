import os
import asyncio
from typing import Optional

import resend

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


# ---------------------------------------------------------------------------
# Language helpers
# ---------------------------------------------------------------------------

SUPPORTED_LANGUAGES = ("en", "ru")
DEFAULT_LANGUAGE = "en"


def _normalize_language(value: Optional[str]) -> str:
    """Return a supported language code, defaulting to DEFAULT_LANGUAGE."""
    if not value:
        return DEFAULT_LANGUAGE
    code = str(value).strip().lower()
    # accept "ru", "ru-RU", "ru_ru", etc.
    if code.startswith("ru"):
        return "ru"
    if code.startswith("en"):
        return "en"
    return DEFAULT_LANGUAGE


# ---------------------------------------------------------------------------
# Email verification (signup / resend code)
# ---------------------------------------------------------------------------

_VERIFY_SUBJECTS = {
    "en": "Your CoTel verification code",
    "ru": "Ваш код подтверждения CoTel",
}


def _build_verify_email_html(language: str, code: str, ttl_minutes: int) -> str:
    if language == "ru":
        heading = "Подтверждение почты в CoTel"
        lead = "Ваш код подтверждения:"
        validity = f"Код действует {ttl_minutes} минут."
        ignore = "Если это были не вы, просто проигнорируйте это письмо."
    else:
        heading = "Verify your email for CoTel"
        lead = "Your verification code:"
        validity = f"This code is valid for {ttl_minutes} minutes."
        ignore = "If you didn’t request this, you can safely ignore this email."

    return f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">
      <h2 style="margin-bottom: 12px;">{heading}</h2>
      <p>{lead}</p>
      <div style="
        display: inline-block;
        padding: 12px 18px;
        font-size: 28px;
        font-weight: 700;
        letter-spacing: 4px;
        border-radius: 12px;
        background: #f3f4f6;
        border: 1px solid #e5e7eb;
        margin: 8px 0 16px;
      ">
        {code}
      </div>
      <p>{validity}</p>
      <p>{ignore}</p>
    </div>
    """


def _build_verify_email_text(language: str, code: str, ttl_minutes: int) -> str:
    if language == "ru":
        return (
            f"Подтверждение почты в CoTel\n\n"
            f"Ваш код подтверждения: {code}\n"
            f"Код действует {ttl_minutes} минут.\n\n"
            f"Если это были не вы, просто проигнорируйте это письмо."
        )
    return (
        f"Verify your email for CoTel\n\n"
        f"Your verification code: {code}\n"
        f"This code is valid for {ttl_minutes} minutes.\n\n"
        f"If you didn’t request this, you can safely ignore this email."
    )


# ---------------------------------------------------------------------------
# Password reset
# ---------------------------------------------------------------------------

_RESET_SUBJECTS = {
    "en": "Your CoTel password reset code",
    "ru": "Ваш код для сброса пароля CoTel",
}


def _build_password_reset_email_html(language: str, code: str, ttl_minutes: int) -> str:
    if language == "ru":
        heading = "Сброс пароля в CoTel"
        intro = "Мы получили запрос на сброс пароля для вашего аккаунта."
        lead = "Ваш код для сброса пароля:"
        validity = f"Код действует {ttl_minutes} минут."
        ignore = "Если это были не вы, просто проигнорируйте это письмо."
    else:
        heading = "Reset your CoTel password"
        intro = "We received a request to reset the password for your account."
        lead = "Your password reset code:"
        validity = f"This code is valid for {ttl_minutes} minutes."
        ignore = "If you didn’t request this, you can safely ignore this email."

    return f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">
      <h2 style="margin-bottom: 12px;">{heading}</h2>
      <p>{intro}</p>
      <p>{lead}</p>
      <div style="
        display: inline-block;
        padding: 12px 18px;
        font-size: 28px;
        font-weight: 700;
        letter-spacing: 4px;
        border-radius: 12px;
        background: #f3f4f6;
        border: 1px solid #e5e7eb;
        margin: 8px 0 16px;
      ">
        {code}
      </div>
      <p>{validity}</p>
      <p>{ignore}</p>
    </div>
    """


def _build_password_reset_email_text(language: str, code: str, ttl_minutes: int) -> str:
    if language == "ru":
        return (
            f"Сброс пароля в CoTel\n\n"
            f"Мы получили запрос на сброс пароля для вашего аккаунта.\n\n"
            f"Ваш код для сброса пароля: {code}\n"
            f"Код действует {ttl_minutes} минут.\n\n"
            f"Если это были не вы, просто проигнорируйте это письмо."
        )
    return (
        f"Reset your CoTel password\n\n"
        f"We received a request to reset the password for your account.\n\n"
        f"Your password reset code: {code}\n"
        f"This code is valid for {ttl_minutes} minutes.\n\n"
        f"If you didn’t request this, you can safely ignore this email."
    )


# ---------------------------------------------------------------------------
# Public senders
# ---------------------------------------------------------------------------

async def send_verification_email(
    to_email: str,
    code: str,
    ttl_minutes: int,
    language: Optional[str] = None,
) -> None:
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is not set")

    if not EMAIL_FROM:
        raise RuntimeError("EMAIL_FROM is not set")

    lang = _normalize_language(language)

    params = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": _VERIFY_SUBJECTS[lang],
        "html": _build_verify_email_html(lang, code, ttl_minutes),
        "text": _build_verify_email_text(lang, code, ttl_minutes),
    }

    # Resend SDK is synchronous; offload to a worker thread so the event loop
    # stays responsive.
    await asyncio.to_thread(resend.Emails.send, params)


async def send_password_reset_email(
    to_email: str,
    code: str,
    ttl_minutes: int,
    language: Optional[str] = None,
) -> None:
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is not set")

    if not EMAIL_FROM:
        raise RuntimeError("EMAIL_FROM is not set")

    lang = _normalize_language(language)

    params = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": _RESET_SUBJECTS[lang],
        "html": _build_password_reset_email_html(lang, code, ttl_minutes),
        "text": _build_password_reset_email_text(lang, code, ttl_minutes),
    }

    await asyncio.to_thread(resend.Emails.send, params)
