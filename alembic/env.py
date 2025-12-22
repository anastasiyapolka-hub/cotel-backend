import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

from db.base import Base
import db.models  # важно: импорт нужен, чтобы модели зарегистрировались в metadata


# Alembic Config object
config = context.config

# Configure Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _get_database_url_for_alembic() -> str:
    """
    Alembic обычно работает через sync-драйвер.
    Поэтому здесь мы конвертируем DATABASE_URL к sync-формату:
      postgres://...  -> postgresql://...
      postgresql+asyncpg://... -> postgresql+psycopg://...
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set (needed for alembic migrations)")

    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    # Если вдруг кто-то передал async URL, конвертируем в psycopg
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)

    return url


def run_migrations_offline() -> None:
    url = _get_database_url_for_alembic()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    # Подставляем URL из ENV в alembic config
    alembic_url = _get_database_url_for_alembic()
    config.set_main_option("sqlalchemy.url", alembic_url)

    connectable = engine_from_config(
        config.get_section(config.config_ini_section) or {},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
