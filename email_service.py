import os
import asyncio
import base64
from typing import Optional, List, Dict, Any

import resend

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")

# ---------------------------------------------------------------------------
# Recipient of user feedback / bug reports / support requests submitted from
# the in-app "Обратная связь" modal. Hardcoded by request — change here to
# route feedback to a different inbox.
# ---------------------------------------------------------------------------
FEEDBACK_RECIPIENT_EMAIL = "anastasiya.polka@gmail.com"

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


# ---------------------------------------------------------------------------
# Feedback (user-submitted requests / bug reports / improvement ideas)
# ---------------------------------------------------------------------------

# Human-friendly category labels that map onto the dropdown values used by
# the frontend (see locales/{ru,en}/auth.json -> feedback.categories).
_FEEDBACK_CATEGORY_LABELS = {
    "bug": "Сообщение об ошибке",
    "improvement": "Предложение об улучшении",
    "support": "Запрос на поддержку",
    "billing": "Спорные вопросы по оплате",
    "account": "Вопрос по аккаунту",
    "other": "Другое",
}


def _feedback_category_label(category: Optional[str]) -> str:
    if not category:
        return _FEEDBACK_CATEGORY_LABELS["other"]
    key = str(category).strip().lower()
    return _FEEDBACK_CATEGORY_LABELS.get(key, key)


def _escape_html(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _build_feedback_email_html(
    *,
    user_email: str,
    category_label: str,
    subject: str,
    message: str,
    files_summary: List[Dict[str, Any]],
) -> str:
    files_html = ""
    if files_summary:
        rows = "".join(
            f"<li>{_escape_html(item['filename'])} "
            f"<span style='color:#6b7280;'>"
            f"({_escape_html(item['size_human'])})</span></li>"
            for item in files_summary
        )
        files_html = f"""
        <p style='margin-top:18px;'><strong>Вложения:</strong></p>
        <ul style='padding-left:18px; margin:6px 0 0;'>{rows}</ul>
        """

    safe_message = _escape_html(message).replace("\n", "<br/>")

    return f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">
      <h2 style="margin: 0 0 12px;">Новая заявка обратной связи (CoTel)</h2>
      <table cellspacing="0" cellpadding="6" style="border-collapse: collapse;">
        <tr>
          <td style="color:#6b7280; padding-right:16px;">От пользователя:</td>
          <td><strong>{_escape_html(user_email)}</strong></td>
        </tr>
        <tr>
          <td style="color:#6b7280; padding-right:16px;">Категория:</td>
          <td>{_escape_html(category_label)}</td>
        </tr>
        <tr>
          <td style="color:#6b7280; padding-right:16px;">Заголовок:</td>
          <td><strong>{_escape_html(subject)}</strong></td>
        </tr>
      </table>
      <p style="margin: 18px 0 6px;"><strong>Сообщение:</strong></p>
      <div style="
        background: #f9fafb;
        border: 1px solid #e5e7eb;
        border-radius: 10px;
        padding: 12px 14px;
        white-space: pre-wrap;
      ">{safe_message}</div>
      {files_html}
    </div>
    """


def _build_feedback_email_text(
    *,
    user_email: str,
    category_label: str,
    subject: str,
    message: str,
    files_summary: List[Dict[str, Any]],
) -> str:
    parts = [
        "Новая заявка обратной связи (CoTel)",
        "",
        f"От пользователя: {user_email}",
        f"Категория: {category_label}",
        f"Заголовок: {subject}",
        "",
        "Сообщение:",
        message,
    ]
    if files_summary:
        parts.append("")
        parts.append("Вложения:")
        for item in files_summary:
            parts.append(f"  - {item['filename']} ({item['size_human']})")
    return "\n".join(parts)


def _human_filesize(num_bytes: int) -> str:
    try:
        n = int(num_bytes or 0)
    except (TypeError, ValueError):
        n = 0
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


async def send_feedback_email(
    *,
    user_email: str,
    subject: str,
    category: Optional[str],
    message: str,
    attachments: Optional[List[Dict[str, Any]]] = None,
    recipient: Optional[str] = None,
) -> None:
    """Send a user feedback request to the support inbox.

    `attachments` is a list of dicts: {"filename": str, "content_bytes": bytes,
    "content_type": str}. They are forwarded as Resend email attachments
    (base64-encoded inline).
    """
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is not set")

    if not EMAIL_FROM:
        raise RuntimeError("EMAIL_FROM is not set")

    target = (recipient or FEEDBACK_RECIPIENT_EMAIL or "").strip()
    if not target:
        raise RuntimeError("FEEDBACK_RECIPIENT_EMAIL is not set")

    category_label = _feedback_category_label(category)

    files_summary: List[Dict[str, Any]] = []
    resend_attachments: List[Dict[str, Any]] = []

    for item in attachments or []:
        try:
            content_bytes: bytes = item.get("content_bytes") or b""
            filename: str = (item.get("filename") or "attachment").strip() or "attachment"
            content_type: str = item.get("content_type") or "application/octet-stream"
            if not content_bytes:
                continue
            encoded = base64.b64encode(content_bytes).decode("ascii")
            resend_attachments.append({
                "filename": filename,
                "content": encoded,
                "content_type": content_type,
            })
            files_summary.append({
                "filename": filename,
                "size_human": _human_filesize(len(content_bytes)),
            })
        except Exception:
            # Skip malformed attachments rather than failing the whole email.
            continue

    subject_line = f"[CoTel · {category_label}] {subject}".strip()

    params: Dict[str, Any] = {
        "from": EMAIL_FROM,
        "to": [target],
        "reply_to": user_email,
        "subject": subject_line,
        "html": _build_feedback_email_html(
            user_email=user_email,
            category_label=category_label,
            subject=subject,
            message=message,
            files_summary=files_summary,
        ),
        "text": _build_feedback_email_text(
            user_email=user_email,
            category_label=category_label,
            subject=subject,
            message=message,
            files_summary=files_summary,
        ),
    }
    if resend_attachments:
        params["attachments"] = resend_attachments

    await asyncio.to_thread(resend.Emails.send, params)
