"""add account role

Revision ID: 27f999b61012
Revises: 5c2e7106924d
Create Date: 2026-04-02 12:42:26.387013

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '27f999b61012'
down_revision: Union[str, Sequence[str], None] = '5c2e7106924d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column(
        "service_telegram_accounts",
        sa.Column(
            "usage_role",
            sa.String(length=20),
            nullable=False,
            server_default="analysis",
        ),
    )

    op.create_index(
        "ix_service_telegram_accounts_usage_role",
        "service_telegram_accounts",
        ["usage_role"],
        unique=False,
    )

    op.drop_index(
        "ix_service_telegram_accounts_select",
        table_name="service_telegram_accounts",
    )

    op.create_index(
        "ix_service_telegram_accounts_select",
        "service_telegram_accounts",
        ["usage_role", "status", "is_enabled", "is_busy", "cooldown_until"],
        unique=False,
    )


def downgrade():
    op.drop_index(
        "ix_service_telegram_accounts_select",
        table_name="service_telegram_accounts",
    )

    op.create_index(
        "ix_service_telegram_accounts_select",
        "service_telegram_accounts",
        ["status", "is_enabled", "is_busy", "cooldown_until"],
        unique=False,
    )

    op.drop_index(
        "ix_service_telegram_accounts_usage_role",
        table_name="service_telegram_accounts",
    )

    op.drop_column("service_telegram_accounts", "usage_role")