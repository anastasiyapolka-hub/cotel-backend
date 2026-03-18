import os
import asyncio
import resend

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


def build_verify_email_html(code: str, ttl_minutes: int) -> str:
    return f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #111827;">
      <h2 style="margin-bottom: 12px;">Подтверждение почты в CoTel</h2>
      <p>Ваш код подтверждения:</p>
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
      <p>Код действует {ttl_minutes} минут.</p>
      <p>Если это были не вы, просто проигнорируйте это письмо.</p>
    </div>
    """


def build_verify_email_text(code: str, ttl_minutes: int) -> str:
    return (
        f"Подтверждение почты в CoTel\n\n"
        f"Ваш код подтверждения: {code}\n"
        f"Код действует {ttl_minutes} минут.\n\n"
        f"Если это были не вы, просто проигнорируйте это письмо."
    )


async def send_verification_email(to_email: str, code: str, ttl_minutes: int) -> None:
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is not set")

    if not EMAIL_FROM:
        raise RuntimeError("EMAIL_FROM is not set")

    params = {
        "from": EMAIL_FROM,
        "to": [to_email],
        "subject": "Ваш код подтверждения CoTel",
        "html": build_verify_email_html(code, ttl_minutes),
        "text": build_verify_email_text(code, ttl_minutes),
    }

    # SDK синхронный, поэтому уводим в thread, чтобы не блокировать event loop
    await asyncio.to_thread(resend.Emails.send, params)