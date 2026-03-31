from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)


from sqlalchemy.dialects.postgresql import JSONB
from .base import Base
import sqlalchemy as sa

class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True)

    owner_user_id = Column(BigInteger, nullable=True, index=True)

    name = Column(String(200), nullable=False)
    source_mode = Column(String(20), nullable=False, default="personal")
    subscription_type = Column(String(30), nullable=False, server_default="events")

    chat_ref = Column(Text, nullable=False)  # username/link/invite как ввёл пользователь
    chat_id = Column(BigInteger, nullable=True)  # нормализованный peer id (когда распарсим)

    frequency_minutes = Column(Integer, nullable=False, default=60)  # 60=час, 1440=день
    prompt = Column(Text, nullable=False)

    is_active = Column(Boolean, nullable=False, default=True)
    status = Column(String(30), nullable=False, default="ok")  # ok/auth_required/error
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

class SubscriptionState(Base):
    __tablename__ = "subscription_state"

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        primary_key=True,
    )

    last_message_id = Column(BigInteger, nullable=True)
    last_checked_at = Column(DateTime(timezone=True), nullable=True)
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(sa.DateTime(timezone=True), nullable=True)

class MatchEvent(Base):
    __tablename__ = "match_events"

    id = Column(Integer, primary_key=True)

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        nullable=False,
    )

    message_id = Column(BigInteger, nullable=False)
    message_ts = Column(DateTime(timezone=True), nullable=True)

    author_id = Column(BigInteger, nullable=True)
    author_display = Column(String(200), nullable=True)

    excerpt = Column(Text, nullable=True)
    reason = Column(Text, nullable=True)
    llm_payload = Column(JSONB, nullable=True)

    notify_status = Column(String(20), nullable=False, default="queued")  # queued/sent/failed
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("subscription_id", "message_id", name="uq_match_subscription_message"),
    )

class DigestEvent(Base):
    __tablename__ = "digest_events"

    id = Column(Integer, primary_key=True)
    subscription_id = Column(Integer, ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False)

    window_start = Column(DateTime(timezone=True), nullable=True)
    window_end = Column(DateTime(timezone=True), nullable=True)

    start_message_id = Column(BigInteger, nullable=True)
    end_message_id = Column(BigInteger, nullable=True)

    messages_seen = Column(Integer, nullable=False, server_default="0")
    digest_text = Column(Text, nullable=False, server_default="")
    llm_payload = Column(JSONB, nullable=True)

    notify_status = Column(String(20), nullable=False, server_default="queued")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("subscription_id", "end_message_id", name="uq_digest_subscription_endmsg"),
        sa.Index("ix_digest_subscription_created", "subscription_id", "created_at"),
    )


class BotUserLink(Base):
    __tablename__ = "bot_user_link"

    id = Column(Integer, primary_key=True)

    owner_user_id = Column(BigInteger, nullable=True, index=True)

    telegram_chat_id = Column(BigInteger, nullable=False, unique=True)
    telegram_user_id = Column(BigInteger, nullable=True)

    started_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    is_blocked = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

class BotLinkCode(Base):
    __tablename__ = "bot_link_codes"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False, index=True)

    code_hash = Column(String(64), nullable=False, unique=True, index=True)

    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, index=True)

    email = Column(String(320), nullable=True, unique=True, index=True)
    phone = Column(String(32), nullable=True, unique=True, index=True)

    password_hash = Column(String(255), nullable=True)
    is_email_verified = Column(Boolean, nullable=False, server_default=sa.text("false"))

    plan = Column(String(32), nullable=False, server_default="free")
    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))

    country_code = Column(String(2), nullable=True, index=True)
    language = Column(String(5), nullable=True, server_default="en")
    language_source = Column(String(10), nullable=True, server_default="auto")

    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

class EmailVerificationCode(Base):
    __tablename__ = "email_verification_codes"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,  # 1 активная запись на пользователя (простая модель)
    )

    # храним НЕ код, а hash(код)
    code_hash = Column(String(64), nullable=False)

    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    # (опционально, но полезно против перебора)
    attempts = Column(Integer, nullable=False, server_default="0")

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

class Session(Base):
    __tablename__ = "sessions"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # В cookie будет raw session_id, в БД храним hash(session_id)
    session_hash = Column(String(64), nullable=False, unique=True, index=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)

    revoked_at = Column(DateTime(timezone=True), nullable=True)

    # опциональные поля для аналитики/безопасности
    user_agent = Column(String(512), nullable=True)
    ip = Column(String(64), nullable=True)

    # полезно, нно обновлять не чаще чем раз в N минут, чтобы не грузить БД
    last_seen_at = Column(DateTime(timezone=True), nullable=True)

class TelegramSession(Base):
    __tablename__ = "telegram_sessions"

    id = Column(BigInteger, primary_key=True, index=True)

    owner_user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)

    # Зашифрованная StringSession (Fernet ciphertext)
    session_ciphertext = Column(Text, nullable=False)

    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))
    revoked_at = Column(DateTime(timezone=True), nullable=True)
    last_used_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        # На MVP  удобно иметь максимум одну активную сессию на пользователя.
        # В Postgres "partial unique index" делается отдельно миграцией.
        # Поэтому тут оставляем просто обычный индекс через owner_user_id.
        {},
    )

class UserChatHistory(Base):
    __tablename__ = "user_chat_history"

    id = Column(BigInteger, primary_key=True, index=True)

    owner_user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    source_mode = Column(String(20), nullable=False, index=True)  # personal / service

    chat_ref = Column(Text, nullable=False)  # как пользователь вводил / что подставляем обратно в поле
    chat_ref_normalized = Column(Text, nullable=False)

    chat_title = Column(String(255), nullable=True)
    chat_username = Column(String(128), nullable=True, index=True)
    chat_id = Column(BigInteger, nullable=True)

    last_accessed_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "owner_user_id",
            "source_mode",
            "chat_ref_normalized",
            name="uq_user_chat_history_owner_source_ref",
        ),
        sa.Index(
            "ix_user_chat_history_owner_source_last",
            "owner_user_id",
            "source_mode",
            "last_accessed_at",
        ),
    )

class ServicePhoneNumber(Base):
    __tablename__ = "service_phone_numbers"

    id = Column(BigInteger, primary_key=True, index=True)

    phone_e164 = Column(String(32), nullable=False, unique=True, index=True)
    provider_code = Column(String(32), nullable=False, index=True)
    country_code = Column(String(8), nullable=False, index=True)

    monthly_cost = Column(Numeric(12, 2), nullable=True)
    currency = Column(String(8), nullable=True)

    total_spent = Column(Numeric(12, 2), nullable=False, server_default="0")
    last_paid_at = Column(DateTime(timezone=True), nullable=True)

    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        sa.Index("ix_service_phone_numbers_provider_country", "provider_code", "country_code"),
    )

class ServiceTelegramAccount(Base):
    __tablename__ = "service_telegram_accounts"

    id = Column(BigInteger, primary_key=True, index=True)

    phone_number_id = Column(
        BigInteger,
        ForeignKey("service_phone_numbers.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    telegram_user_id = Column(BigInteger, nullable=True, unique=True, index=True)
    telegram_username = Column(String(128), nullable=True, index=True)

    status = Column(String(32), nullable=False, server_default="active", index=True)
    is_enabled = Column(Boolean, nullable=False, server_default=sa.text("true"), index=True)
    is_busy = Column(Boolean, nullable=False, server_default=sa.text("false"), index=True)

    busy_started_at = Column(DateTime(timezone=True), nullable=True)
    cooldown_until = Column(DateTime(timezone=True), nullable=True, index=True)

    last_used_at = Column(DateTime(timezone=True), nullable=True)
    last_auth_at = Column(DateTime(timezone=True), nullable=True)

    last_error = Column(Text, nullable=True)
    last_error_at = Column(DateTime(timezone=True), nullable=True)

    consecutive_fail_count = Column(Integer, nullable=False, server_default="0")

    requests_last_minute = Column(Integer, nullable=False, server_default="0")
    requests_last_hour = Column(Integer, nullable=False, server_default="0")
    requests_last_day = Column(Integer, nullable=False, server_default="0")

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        sa.Index(
            "ix_service_telegram_accounts_select",
            "status",
            "is_enabled",
            "is_busy",
            "cooldown_until",
        ),
    )

class ServiceTelegramSession(Base):
    __tablename__ = "service_telegram_sessions"

    id = Column(BigInteger, primary_key=True, index=True)

    service_account_id = Column(
        BigInteger,
        ForeignKey("service_telegram_accounts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    session_ciphertext = Column(Text, nullable=False)
    session_version = Column(Integer, nullable=False, server_default="1")

    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"), index=True)

    revoked_at = Column(DateTime(timezone=True), nullable=True)
    revoked_reason = Column(Text, nullable=True)

    last_used_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        sa.Index("ix_service_telegram_sessions_account_active", "service_account_id", "is_active"),
    )

class ServiceAccountStatusHistory(Base):
    __tablename__ = "service_account_status_history"

    id = Column(BigInteger, primary_key=True, index=True)

    service_account_id = Column(
        BigInteger,
        ForeignKey("service_telegram_accounts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    old_status = Column(String(32), nullable=True)
    new_status = Column(String(32), nullable=False)

    reason = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_service_account_status_history_account_created", "service_account_id", "created_at"),
    )

class ServiceAccountLog(Base):
    __tablename__ = "service_account_logs"

    id = Column(BigInteger, primary_key=True, index=True)

    service_account_id = Column(
        BigInteger,
        ForeignKey("service_telegram_accounts.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    event_type = Column(String(64), nullable=False, index=True)
    target_ref = Column(Text, nullable=True)

    is_success = Column(Boolean, nullable=True)

    error_code = Column(String(64), nullable=True)
    error_message = Column(Text, nullable=True)

    event_at = Column(DateTime(timezone=True), nullable=True, index=True)

    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)

    meta_json = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_service_account_logs_account_event_at", "service_account_id", "event_at"),
        sa.Index("ix_service_account_logs_account_started_at", "service_account_id", "started_at"),
    )