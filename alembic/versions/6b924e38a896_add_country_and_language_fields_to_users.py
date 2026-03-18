"""add country and language fields to users

Revision ID: 6b924e38a896
Revises: 22d4a550d547
Create Date: 2026-03-18 19:39:40.525033

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6b924e38a896'
down_revision: Union[str, Sequence[str], None] = '22d4a550d547'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("country_code", sa.String(length=2), nullable=True))
    op.add_column("users", sa.Column("language", sa.String(length=5), nullable=True, server_default="en"))
    op.add_column("users", sa.Column("language_source", sa.String(length=10), nullable=True, server_default="auto"))

    op.create_index("ix_users_country_code", "users", ["country_code"], unique=False)

def downgrade() -> None:
    op.drop_index("ix_users_country_code", table_name="users")

    op.drop_column("users", "language_source")
    op.drop_column("users", "language")
    op.drop_column("users", "country_code")