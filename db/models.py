from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
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

class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, index=True)

    email = Column(String(320), nullable=True, unique=True, index=True)
    phone = Column(String(32), nullable=True, unique=True, index=True)

    password_hash = Column(String(255), nullable=True)

    plan = Column(String(32), nullable=False, server_default="free")
    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


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
