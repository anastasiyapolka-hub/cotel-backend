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
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass