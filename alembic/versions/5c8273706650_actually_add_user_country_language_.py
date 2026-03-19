"""actually add user country language columns

Revision ID: 5c8273706650
Revises: 6b924e38a896
Create Date: 2026-03-19 08:26:00.729716

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5c8273706650'
down_revision: Union[str, Sequence[str], None] = '6b924e38a896'
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