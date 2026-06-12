from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)


from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from .base import Base
import sqlalchemy as sa

class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True)

    owner_user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    name = Column(String(200), nullable=False)
    source_mode = Column(String(20), nullable=False, default="personal")
    subscription_type = Column(String(30), nullable=False, server_default="events")

    chat_ref = Column(Text, nullable=False)  # username/link/invite как ввёл пользователь (для одиночной — реальный чат; для групповой — "group:<sub_id>")
    chat_id = Column(BigInteger, nullable=True)  # нормализованный peer id (только для одиночной; для групповой — NULL)

    # Групповая подписка — мониторит сразу несколько чатов.
    # Если True, реальный список чатов лежит в таблице subscription_chats,
    # а chat_ref содержит синтетический маркер "group:<sub_id>". Только
    # для personal source_mode (для service групповые подписки запрещены).
    # MIGRATION REQUIRED:
    #   ALTER TABLE subscriptions
    #     ADD COLUMN is_group BOOLEAN NOT NULL DEFAULT FALSE;
    #   CREATE INDEX ix_subscriptions_owner_is_group
    #     ON subscriptions(owner_user_id, is_group);
    is_group = Column(
        Boolean,
        nullable=False,
        server_default=sa.text("false"),
        index=True,
    )

    frequency_minutes = Column(Integer, nullable=False, default=60)  # 60=час, 1440=день
    prompt = Column(Text, nullable=False)
    ai_model = Column(String(64), nullable=False, server_default="openai:gpt-4.1-mini")

    # Медиафильтр (опционально, только для events-подписок).
    # JSON-структура совпадает с backend/media_filter/types.py
    # MediaFilterRequest: { enabled, categories, video_subtype, audio_subtype }.
    # NULL = подписка работает в классическом текстовом режиме (через prompt).
    # Для digest-подписок поле всегда NULL.
    media_filter = Column(JSONB, nullable=True)

    # Главное поле для расчёта "активных подписок"
    is_active = Column(Boolean, nullable=False, default=True)

    # Текстовый статус для UI / диагностики
    # active / paused / ok / auth_required / error / trial_expired
    status = Column(String(30), nullable=False, default="ok")
    last_error = Column(Text, nullable=True)

    # Trial-подписки для free
    is_trial = Column(Boolean, nullable=False, server_default=sa.text("false"), index=True)
    trial_started_at = Column(DateTime(timezone=True), nullable=True)
    trial_ends_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        sa.Index("ix_subscriptions_owner_active", "owner_user_id", "is_active"),
        sa.Index("ix_subscriptions_owner_trial", "owner_user_id", "is_trial"),
    )

class SubscriptionState(Base):
    __tablename__ = "subscription_state"

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # Для одиночной подписки — курсор по сообщениям. Для групповой не
    # используется (см. SubscriptionChatState), но запись всё равно
    # создаётся, чтобы reservation-логика по next_run_at работала
    # единообразно для обоих типов.
    last_message_id = Column(BigInteger, nullable=True)
    last_checked_at = Column(DateTime(timezone=True), nullable=True)
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(sa.DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Групповые подписки: один subscription -> много чатов.
# ---------------------------------------------------------------------------
# MIGRATION REQUIRED — новая таблица subscription_chats:
#   CREATE TABLE subscription_chats (
#     subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
#     position INTEGER NOT NULL,
#     chat_ref TEXT NOT NULL,
#     chat_id BIGINT NULL,
#     chat_title VARCHAR(255) NULL,
#     chat_username VARCHAR(128) NULL,
#     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
#     PRIMARY KEY (subscription_id, position)
#   );
#   CREATE INDEX ix_subscription_chats_subscription ON subscription_chats(subscription_id);
# ---------------------------------------------------------------------------
class SubscriptionChat(Base):
    __tablename__ = "subscription_chats"

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        primary_key=True,
    )
    # Порядковый номер чата в группе — нужен и как часть PK, и чтобы
    # стабильно показывать пользователю те же чаты в том же порядке,
    # в котором он их выбрал на фронте.
    position = Column(Integer, primary_key=True)

    chat_ref = Column(Text, nullable=False)
    chat_id = Column(BigInteger, nullable=True)
    chat_title = Column(String(255), nullable=True)
    chat_username = Column(String(128), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        sa.Index("ix_subscription_chats_subscription", "subscription_id"),
    )


# ---------------------------------------------------------------------------
# Состояние групповой подписки на уровне отдельного чата.
# Для групповой подписки на каждый чат — отдельный курсор last_message_id.
# Это критично: без per-chat курсора либо теряем сообщения, либо шлём дубли
# (message_id из разных чатов не сопоставимы).
# ---------------------------------------------------------------------------
# MIGRATION REQUIRED — новая таблица subscription_chat_state:
#   CREATE TABLE subscription_chat_state (
#     subscription_id INTEGER NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
#     chat_key TEXT NOT NULL,
#     last_message_id BIGINT NULL,
#     last_success_at TIMESTAMPTZ NULL,
#     last_error TEXT NULL,
#     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
#     updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
#     PRIMARY KEY (subscription_id, chat_key)
#   );
# chat_key — это chat_ref нормализованный (то же, что лежит в
# subscription_chats.chat_ref). Берём именно ref, а не chat_id, потому что
# для приватных каналов chat_id может прийти позже первого фетча, а ref
# известен сразу при создании.
# ---------------------------------------------------------------------------
class SubscriptionChatState(Base):
    __tablename__ = "subscription_chat_state"

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        primary_key=True,
    )
    chat_key = Column(Text, primary_key=True)

    last_message_id = Column(BigInteger, nullable=True)
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class MatchEvent(Base):
    __tablename__ = "match_events"

    id = Column(Integer, primary_key=True)

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Поля чата — нужны для групповых подписок (чтобы в боте показать,
    # в каком именно чате нашлось совпадение, и построить ссылку).
    # Для одиночной подписки — NULL (берём из самой Subscription).
    # MIGRATION REQUIRED:
    #   ALTER TABLE match_events ADD COLUMN chat_ref TEXT NULL;
    #   ALTER TABLE match_events ADD COLUMN chat_id BIGINT NULL;
    #   ALTER TABLE match_events ADD COLUMN chat_title VARCHAR(255) NULL;
    chat_ref = Column(Text, nullable=True)
    chat_id = Column(BigInteger, nullable=True)
    chat_title = Column(String(255), nullable=True)

    message_id = Column(BigInteger, nullable=False)
    message_ts = Column(DateTime(timezone=True), nullable=True)

    author_id = Column(BigInteger, nullable=True)
    author_display = Column(String(200), nullable=True)

    excerpt = Column(Text, nullable=True)
    reason = Column(Text, nullable=True)
    llm_payload = Column(JSONB, nullable=True)

    notify_status = Column(String(20), nullable=False, default="queued")  # queued/sent/failed
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # MIGRATION REQUIRED — пересоздать unique constraint так, чтобы он
    # учитывал chat_id (для групповой подписки message_id из разных чатов
    # МОГУТ совпадать, иначе будут IntegrityError):
    #   ALTER TABLE match_events DROP CONSTRAINT uq_match_subscription_message;
    #   CREATE UNIQUE INDEX uq_match_sub_chat_msg
    #     ON match_events (subscription_id, COALESCE(chat_id, 0), message_id);
    # COALESCE нужен, чтобы старые одиночные подписки (chat_id IS NULL)
    # не нарушали уникальность.
    __table_args__ = (
        sa.Index(
            "uq_match_sub_chat_msg",
            "subscription_id",
            sa.func.coalesce(sa.text("chat_id"), sa.text("0")),
            "message_id",
            unique=True,
        ),
    )

class DigestEvent(Base):
    __tablename__ = "digest_events"

    id = Column(Integer, primary_key=True)
    subscription_id = Column(Integer, ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False)

    # Поля чата — для групповых саммари (бот шлёт N сообщений, по одному
    # на чат, и для каждого нужны название/ссылка). Для одиночной — NULL.
    # MIGRATION REQUIRED:
    #   ALTER TABLE digest_events ADD COLUMN chat_ref TEXT NULL;
    #   ALTER TABLE digest_events ADD COLUMN chat_id BIGINT NULL;
    #   ALTER TABLE digest_events ADD COLUMN chat_title VARCHAR(255) NULL;
    chat_ref = Column(Text, nullable=True)
    chat_id = Column(BigInteger, nullable=True)
    chat_title = Column(String(255), nullable=True)

    window_start = Column(DateTime(timezone=True), nullable=True)
    window_end = Column(DateTime(timezone=True), nullable=True)

    start_message_id = Column(BigInteger, nullable=True)
    end_message_id = Column(BigInteger, nullable=True)

    messages_seen = Column(Integer, nullable=False, server_default="0")
    digest_text = Column(Text, nullable=False, server_default="")
    llm_payload = Column(JSONB, nullable=True)

    notify_status = Column(String(20), nullable=False, server_default="queued")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # MIGRATION REQUIRED — пересоздать unique constraint так, чтобы он
    # учитывал chat_id (для групповой подписки end_message_id из разных
    # чатов могут совпасть):
    #   ALTER TABLE digest_events DROP CONSTRAINT uq_digest_subscription_endmsg;
    #   CREATE UNIQUE INDEX uq_digest_sub_chat_endmsg
    #     ON digest_events (subscription_id, COALESCE(chat_id, 0), end_message_id);
    __table_args__ = (
        sa.Index(
            "uq_digest_sub_chat_endmsg",
            "subscription_id",
            sa.func.coalesce(sa.text("chat_id"), sa.text("0")),
            "end_message_id",
            unique=True,
        ),
        sa.Index("ix_digest_subscription_created", "subscription_id", "created_at"),
    )

class BotUserLink(Base):
    __tablename__ = "bot_user_link"

    id = Column(Integer, primary_key=True)

    owner_user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

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
    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    code_hash = Column(String(64), nullable=False, unique=True, index=True)

    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class Plan(Base):
    __tablename__ = "plans"

    code = Column(String(32), primary_key=True)  # free / basic / pro / super_pro

    price_usd = Column(Numeric(10, 2), nullable=False, server_default="0")
    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))

    # === Tокенная система (новая, см. architecture-router-and-credits.md) ===
    # Месячный грант токенов на тариф. Источник цифр — раздел 2.3 архитектурного документа.
    # free=300, basic=3600, pro=10000, super_pro=25000
    monthly_tokens = Column(Integer, nullable=False, server_default="0")

    # Какие tier'ы анализа доступны на этом тарифе.
    # free → ['light'], остальные → ['light','balanced','deep']
    allowed_tiers = Column(
        ARRAY(String(16)),
        nullable=False,
        server_default=sa.text("ARRAY['light']::varchar[]"),
    )

    # Включена ли возможность докупки top-up токенов сверх месячного лимита.
    # free → false, остальные → true
    topup_enabled = Column(Boolean, nullable=False, server_default=sa.text("false"))

    # Максимум чатов в одном групповом запросе/подписке.
    # Free=1 (только single-chat), Basic=5, Pro=10, Power=20.
    # Логика проверки: if num_chats > plan.max_chats_per_group_request → reject.
    # Применяется одинаково к Q&A и подпискам (общий лимит групповой обработки).
    max_chats_per_group_request = Column(Integer, nullable=False, server_default="1")

    # === DEPRECATED: старая система счётчиков запросов ===
    # Эти поля остаются в БД один релиз для backward compat. После того как
    # токенная система отработает на проде ≥1 неделю — удалим их и поля,
    # которые их используют (plan_limits.check_qa_quota и т.п.). Не читать
    # их в новом коде — использовать monthly_tokens вместо.
    daily_qa_limit = Column(Integer, nullable=False)
    monthly_qa_limit = Column(Integer, nullable=False)

    qa_history_days = Column(Integer, nullable=False)

    max_active_subscriptions = Column(Integer, nullable=False)
    min_subscription_interval_minutes = Column(Integer, nullable=False)

    trial_subscription_limit = Column(Integer, nullable=False, server_default="0")
    trial_subscription_duration_days = Column(Integer, nullable=False, server_default="0")

    has_chat_history = Column(Boolean, nullable=False, server_default=sa.text("false"))

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, index=True)

    email = Column(String(320), nullable=True, unique=True, index=True)
    phone = Column(String(32), nullable=True, unique=True, index=True)

    password_hash = Column(String(255), nullable=True)
    is_email_verified = Column(Boolean, nullable=False, server_default=sa.text("false"))

    # Строковый код тарифа, но уже с FK на plans.code
    plan = Column(
        String(32),
        ForeignKey("plans.code", ondelete="RESTRICT"),
        nullable=False,
        server_default="free",
        index=True,
    )

    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))

    country_code = Column(String(2), nullable=True, index=True)
    language = Column(String(5), nullable=True, server_default="en")
    language_source = Column(String(10), nullable=True, server_default="auto")

    timezone = Column(String(64), nullable=False, server_default="UTC")
    logout_revokes_telegram = Column(Boolean, nullable=False, server_default=sa.text("false"))
    default_ai_model = Column(String(64), nullable=False, server_default="openai:gpt-4.1-mini")

    # Сохранять ли историю запросов (лента «запрос → ответ» в query_history)
    # между сессиями. false = лента живёт только в текущей сессии, после
    # перезагрузки очищается; true = пишем в query_history и подтягиваем при
    # входе. Управляется галочкой в настройках профиля.
    # Дефолт opt-in (false). Чтобы сделать opt-out — поменять на sa.text("true").
    save_query_history = Column(Boolean, nullable=False, server_default=sa.text("false"))

    last_login_at = Column(DateTime(timezone=True), nullable=True)

    # === Защита логина от перебора пароля (brute-force) ===
    # Счётчик неудачных попыток входа подряд (сбрасывается при успешном входе
    # и при «остывании» — если давно не было промахов).
    failed_login_count = Column(Integer, nullable=False, server_default="0")
    # До какого момента вход в аккаунт временно заблокирован (NULL = не
    # заблокирован). Проверяется в начале /auth/login.
    lockout_until = Column(DateTime(timezone=True), nullable=True)
    # Время последней неудачной попытки — для «остывания» счётчика/эскалации.
    last_failed_login_at = Column(DateTime(timezone=True), nullable=True)
    # Уровень эскалации блокировки (сколько раз подряд срабатывала блокировка):
    # чем выше, тем длиннее следующая пауза (15 → 30 → 60 → 120 мин).
    lockout_level = Column(Integer, nullable=False, server_default="0")
    # MIGRATION REQUIRED:
    #   ALTER TABLE users
    #     ADD COLUMN failed_login_count INTEGER NOT NULL DEFAULT 0,
    #     ADD COLUMN lockout_until TIMESTAMPTZ NULL,
    #     ADD COLUMN last_failed_login_at TIMESTAMPTZ NULL,
    #     ADD COLUMN lockout_level INTEGER NOT NULL DEFAULT 0;

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Акцепт юр. документов (Privacy Policy + Terms of Service + подтверждение
    # возраста 16+) при регистрации. Заполняется единоразово при создании
    # аккаунта; обновляется только если пользователь повторно принимает
    # обновлённую версию документов.
    terms_accepted_at = Column(DateTime(timezone=True), nullable=True)
    terms_accepted_version = Column(String(16), nullable=True)

class LLMPricing(Base):
    __tablename__ = "llm_pricing"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Slug модели ровно в том формате, который используется в коде:
    # "openai:gpt-4.1-mini", "anthropic:claude-sonnet-4-6" и т.д.
    ai_model = Column(String(64), nullable=False)

    # Цена за 1M input/output токенов в USD
    input_price_per_1m_usd = Column(Numeric(12, 6), nullable=False)
    output_price_per_1m_usd = Column(Numeric(12, 6), nullable=False)

    # На MVP используем только USD, но поле оставляем для будущего
    currency = Column(String(8), nullable=False, server_default="USD")

    # false = прайс устарел / не используется для новых расчётов
    is_active = Column(Boolean, nullable=False, server_default=sa.text("true"))

    # Комментарий админа: источник, дата проверки, пояснения
    note = Column(Text, nullable=True)

    # Кто последний изменил запись
    updated_by_user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        sa.Index(
            "ix_llm_pricing_ai_model_unique",
            "ai_model",
            unique=True,
        ),
        sa.Index(
            "ix_llm_pricing_active",
            "is_active",
        ),
    )

# ++ПЛАТЕЖНЫЕ ДАННЫЕ
class BillingSubscription(Base):
    __tablename__ = "billing_subscriptions"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    provider = Column(String(32), nullable=False)  # lemonsqueezy / stripe / etc
    provider_customer_id = Column(String(128), nullable=True, index=True)
    provider_subscription_id = Column(String(128), nullable=True, unique=True, index=True)
    provider_variant_id = Column(String(128), nullable=True)
    provider_product_id = Column(String(128), nullable=True)

    plan_code = Column(
        String(32),
        ForeignKey("plans.code", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    status = Column(String(32), nullable=False, index=True)  # active / canceled / past_due / trialing / expired
    cancel_at_period_end = Column(Boolean, nullable=False, server_default=sa.text("false"))

    current_period_start = Column(DateTime(timezone=True), nullable=True)
    current_period_end = Column(DateTime(timezone=True), nullable=True)

    last_payment_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        sa.Index("ix_billing_subscriptions_user_status", "user_id", "status"),
    )

class Payment(Base):
    __tablename__ = "payments"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    billing_subscription_id = Column(
        BigInteger,
        ForeignKey("billing_subscriptions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    provider = Column(String(32), nullable=False, index=True)
    provider_order_id = Column(String(128), nullable=True, index=True)
    provider_payment_id = Column(String(128), nullable=True, unique=True, index=True)

    amount = Column(Numeric(12, 2), nullable=False)
    currency = Column(String(8), nullable=False)

    status = Column(String(32), nullable=False, index=True)  # paid / failed / refunded / pending
    raw_payload = Column(JSONB, nullable=True)

    paid_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_payments_user_created", "user_id", "created_at"),
    )
# --ПЛАТЕЖНЫЕ ДАННЫЕ

# ++Счетчики лимитов
class UsageCounter(Base):
    __tablename__ = "usage_counters"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    metric_code = Column(String(32), nullable=False)  # qa_request
    period_type = Column(String(16), nullable=False)  # day / month
    period_start = Column(Date, nullable=False)

    used_count = Column(Integer, nullable=False, server_default="0")

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "metric_code",
            "period_type",
            "period_start",
            name="uq_usage_counter_user_metric_period",
        ),
        sa.Index(
            "ix_usage_counters_user_metric_period",
            "user_id",
            "metric_code",
            "period_type",
            "period_start",
        ),
    )

class UsageEvent(Base):
    __tablename__ = "usage_events"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    event_type = Column(String(32), nullable=False, index=True)
    # qa_request_success / qa_request_rejected / subscription_created / subscription_resumed / subscription_paused

    source_mode = Column(String(20), nullable=True)  # personal / service
    chat_ref = Column(Text, nullable=True)

    subscription_id = Column(
        Integer,
        ForeignKey("subscriptions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    status = Column(String(32), nullable=False, index=True)
    # success_counted / failed_not_counted / limit_rejected

    meta_json = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_usage_events_user_created", "user_id", "created_at"),
        sa.Index("ix_usage_events_user_type_created", "user_id", "event_type", "created_at"),
    )
# --Счетчики лимитов

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

class PasswordResetCode(Base):
    __tablename__ = "password_reset_codes"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,  # одна активная reset-запись на пользователя
    )

    code_hash = Column(String(64), nullable=False)

    expires_at = Column(DateTime(timezone=True), nullable=False)
    used_at = Column(DateTime(timezone=True), nullable=True)

    attempts = Column(Integer, nullable=False, server_default="0")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

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

    usage_role = Column(String(20), nullable=False, server_default="analysis", index=True)

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
            "usage_role",
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


# ---------------------------------------------------------------------------
# Токенная система (см. architecture-router-and-credits.md)
# ---------------------------------------------------------------------------
#
# Архитектурно: 1 наш токен = $0,001 LLM-стоимости. Маркап 2.5× (маржа 60%).
# Списание делается post-flight (после LLM-ответа) на основе фактических
# input_tokens / output_tokens / thinking_tokens из API провайдера.
#
# Три таблицы:
#   user_token_balances — текущий баланс пользователя (1 строка на пользователя)
#   token_transactions  — журнал всех движений (грант, списание, top-up, refund)
#   user_query_log      — тексты пользовательских запросов для аналитики/калибровки
#                         роутера. Отдельная таблица из соображений privacy
#                         (легко отключить или очистить).
#
# usage_events (LLM-вызовы) остаётся как есть — token_transactions
# ссылается на него через related_event_id для LLM-списаний.
# ---------------------------------------------------------------------------


class UserTokenBalance(Base):
    """
    Баланс токенов пользователя — месячный (обновляется кроном на 1-е число)
    + докупленный top-up (накапливается, не сгорает в конце месяца).

    1 строка на пользователя. Соглашение: на регистрации пользователя
    создаём запись с monthly_granted = plan.monthly_tokens.
    """
    __tablename__ = "user_token_balances"

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # Начало текущего расчётного периода (1-е число месяца UTC).
    # Используется кроном для обнаружения, что пора ресетить monthly_used.
    period_start = Column(Date, nullable=False, server_default=func.current_date())

    # Сколько токенов начислено на этот период (из plan.monthly_tokens на
    # момент гранта — снапшот, чтобы смена тарифа не пересчитывала задним
    # числом текущий месяц).
    monthly_granted = Column(Integer, nullable=False, server_default="0")

    # Сколько токенов израсходовано за текущий период (сумма списаний
    # qa_request + subscription_* минус возвраты).
    monthly_used = Column(Integer, nullable=False, server_default="0")

    # Купленные сверх тарифа top-up токены. Не сгорают в конце месяца.
    # Списываются ПОСЛЕ исчерпания monthly (см. раздел 2.6 архитектуры).
    topup_balance = Column(Integer, nullable=False, server_default="0")

    # Когда пользователю было отправлено разовое уведомление о том, что
    # подписки приостановлены из-за нехватки токенов. NULL = уведомление
    # ещё не отправлялось (или сброшено после пополнения баланса). Нужно,
    # чтобы НЕ долбить пользователя пушами на каждом тике раннера/снапшоте,
    # а уведомить ровно один раз за «эпизод» исчерпания. Сбрасывается в NULL
    # при пополнении (monthly_grant / top-up / смена тарифа).
    # MIGRATION REQUIRED:
    #   ALTER TABLE user_token_balances
    #     ADD COLUMN low_balance_notified_at TIMESTAMPTZ NULL;
    low_balance_notified_at = Column(DateTime(timezone=True), nullable=True)

    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    # Примечание: поля daily_used / daily_reset_at сознательно не добавлены
    # в MVP — daily-лимит включается только если увидим abuse (раздел 2.5).


class TokenTransaction(Base):
    """
    Журнал всех движений токенов на счёте пользователя.

    Источник истины для биллинга и пользовательской истории расхода.
    На каждый LLM-вызов будет 1 запись (reason='qa_request' и т.п.) +
    related_event_id на usage_events.id для двойного аудита.

    Гранты, top-up, refund, admin adjustments — тоже сюда.
    """
    __tablename__ = "token_transactions"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Отрицательное = списание (расход), положительное = пополнение.
    # Формула: списано в сумме = sum(delta WHERE delta<0).
    delta = Column(Integer, nullable=False)

    # Причина транзакции. Стандартные значения:
    #   'qa_request'             — пользовательский Q&A запрос
    #   'subscription_event'     — event-подписка (поиск совпадений)
    #   'subscription_digest'    — digest-подписка
    #   'classifier'             — LLM-классификатор категории (опц., если решим биллить)
    #   'monthly_grant'          — начисление в начале месяца (cron)
    #   'topup_purchase'         — докупка через Stripe
    #   'refund'                 — возврат при ошибке
    #   'admin_adjustment'       — ручная коррекция через админ-панель
    reason = Column(String(64), nullable=False, index=True)

    # Для LLM-списаний — ссылка на usage_events.id. NULL для грантов/топ-апов.
    related_event_id = Column(
        BigInteger,
        ForeignKey("usage_events.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Свободная мета: snapshot цены модели, исходные input/output_tokens,
    # Stripe payment_id для top-up, причина refund и т.д.
    meta_json = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_token_tx_user_created", "user_id", "created_at"),
    )


class UserQueryLog(Base):
    """
    Лог текстов запросов пользователя для аналитики и калибровки роутера.

    Отдельная таблица от usage_events потому что:
      - Privacy: легко отключить или удалить (TTL, GDPR-эрэз)
      - Размер: query тексты могут быть длинными, не раздуваем основную таблицу
      - Возможность опционально шифровать / анонимизировать в будущем

    Используется для:
      - Калибровки LLM-классификатора (видим где пользователь переопределил
        категорию вручную → дотюнить промпт)
      - Аналитики: какие категории чаще, какие tier'ы чаще
      - Debug кейсов «модель плохо ответила»
    """
    __tablename__ = "user_query_log"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Связь с конкретным LLM-вызовом. NULL только если запрос отказан
    # на этапе классификации (баланс кончился) и LLM не вызывался.
    usage_event_id = Column(
        BigInteger,
        ForeignKey("usage_events.id", ondelete="CASCADE"),
        nullable=True,
    )

    # Сам текст запроса пользователя (без preprocessing'a, как ввёл).
    query_text = Column(Text, nullable=False)

    # Что LLM-классификатор определил автоматически.
    detected_category = Column(String(32), nullable=True)

    # Уверенность классификатора 0.00–1.00.
    detected_confidence = Column(Numeric(3, 2), nullable=True)

    # Что в итоге было использовано: либо detected, либо то, на что
    # пользователь переопределил вручную (override через категорию-чипс в UI).
    # Если совпадает с detected_category — авто-классификатор сработал.
    # Если отличается — это сигнал для дотюна промпта классификатора.
    final_category = Column(String(32), nullable=True)

    # Выбранный пользователем tier: 'light' / 'balanced' / 'deep'.
    selected_tier = Column(String(16), nullable=False)

    # Какая модель в итоге попала в роутер (после fallback, если был).
    # Slug формата 'google:gemini-2.5-flash' и т.д.
    selected_model = Column(String(64), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_query_log_user_created", "user_id", "created_at"),
        sa.Index("ix_query_log_final_category", "final_category"),
    )


class SavedQuery(Base):
    """
    Сохранённый пресет запроса пользователя («Сохранённые запросы» в UI).

    Пользователь настраивает форму запроса (чат/чаты, период, уровень анализа,
    групповой режим, медиафильтр, текст вопроса), даёт пресету имя и сохраняет.
    Позже выбирает его из списка — фронт парсит params_json и подставляет все
    настройки обратно в форму.

    params_json — снапшот настроек в формате, зеркалящем payload эндпоинтов
    /tg/analyze_chat и /tg/analyze_chats_group, чтобы «применить пресет» и
    «повторить из истории» использовали одну и ту же сериализацию. Внутри —
    schema_version для forward-совместимости при изменении формата настроек.
    Парсинг на стороне фронта должен быть defensive: если пресет ссылается на
    протухший чат/модель/категорию — подставляем дефолт, а не падаем.
    """
    __tablename__ = "saved_queries"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Человекочитаемое имя пресета (по нему пользователь ищет в списке).
    name = Column(String(255), nullable=False)

    # Снапшот всех настроек запроса. Формат (schema_version=1):
    #   {
    #     "schema_version": 1,
    #     "is_group": false,
    #     "chat_link": "...",          # одиночный запрос
    #     "chats": [],                  # групповой запрос (is_group=true)
    #     "user_query": "...",
    #     "period_value": 1,
    #     "period_unit": "days",        # minutes / hours / days
    #     "depth": "light",             # light / balanced / deep
    #     "category": null,             # опциональный override категории
    #     "media_filter": null          # либо объект MediaFilterRequest:
    #       # {
    #       #   "enabled": true,
    #       #   "categories": ["video", "audio", ...],
    #       #   "video_subtype": "video_files",  # video_files/video_round/video_both
    #       #   "audio_subtype": "audio_files"   # audio_files/audio_voice/audio_both
    #       # }
    #   }
    params_json = Column(JSONB, nullable=False)

    # Когда пресет последний раз применяли — для сортировки «недавние».
    last_used_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "name",
            name="uq_saved_queries_user_name",
        ),
        sa.Index(
            "ix_saved_queries_user_last_used",
            "user_id",
            "last_used_at",
        ),
    )


class QueryHistory(Base):
    """
    История запросов пользователя — лента «запрос → ответ» в окне вывода
    (простыня в стиле ChatGPT, скроллится вверх/вниз).

    Каждая строка — одна пара «запрос пользователя + ответ сервиса».
    Окно вывода при загрузке тянет последние N строк, скролл вверх подгружает
    старые (курсорная пагинация по (user_id, created_at)).

    Отдельная таблица от user_query_log (который — внутренняя аналитика
    роутера и хранит только текст запроса без ответа): у истории другой
    ретеншен (free 30 дней / платные 90) и она user-facing. Связь с
    аналитикой/биллингом — через usage_event_id, аналитику не дублируем.

    Ретеншен реализуется отдельной джобой: удаляет строки старше cutoff,
    где cutoff = now - (30 или 90 дней) по текущему тарифу пользователя.
    Отдельная колонка под срок хранения не нужна.
    """
    __tablename__ = "query_history"

    id = Column(BigInteger, primary_key=True, index=True)

    user_id = Column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Задел под будущие «диалоги»/«чаты» (правая сворачиваемая панель со
    # списком тредов, переключение между ними). Пока всегда NULL — лента
    # показывает все запросы пользователя единым потоком. Когда введём
    # диалоги, лента начнёт фильтроваться по conversation_id без миграции.
    conversation_id = Column(BigInteger, nullable=True, index=True)

    # Текст запроса пользователя (как ввёл).
    query_text = Column(Text, nullable=False)

    # Снапшот настроек запроса в том же формате, что SavedQuery.params_json.
    # Используется иконкой «повторить» — подставляет настройки обратно в форму
    # (тот же код, что применение пресета). Внутри media_filter — какие чаты
    # участвовали, поэтому групповой запрос хранится без отдельной логики.
    params_json = Column(JSONB, nullable=True)

    # Канонический объект для рендера ответа в ленте: текст обычного Q&A либо
    # структурные медиа-карточки (formatter.py). Лента рисует его так же, как
    # при первом ответе. NULL, если запрос упал и ответа нет.
    response_payload = Column(JSONB, nullable=True)

    # Плоский текст ответа — для превью/поиска. Необязательное дублирование.
    response_text = Column(Text, nullable=True)

    source_mode = Column(String(20), nullable=False, index=True)  # personal / service

    status = Column(String(32), nullable=False)  # success / failed

    # Связь с биллинговым событием (чтобы не дублировать аналитику).
    # NULL, если запрос упал до списания токенов.
    usage_event_id = Column(
        BigInteger,
        ForeignKey("usage_events.id", ondelete="SET NULL"),
        nullable=True,
    )

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        sa.Index("ix_query_history_user_created", "user_id", "created_at"),
        sa.Index(
            "ix_query_history_conversation_created",
            "conversation_id",
            "created_at",
        ),
    )