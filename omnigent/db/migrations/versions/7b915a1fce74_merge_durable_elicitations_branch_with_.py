"""merge durable elicitations branch with upstream host index

Revision ID: 7b915a1fce74
Revises: ac1b2c3d4e5f, m019dd1021a
Create Date: 2026-11-06 15:23:35.449070
"""

from __future__ import annotations

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "7b915a1fce74"
down_revision: tuple[str, str] = ("ac1b2c3d4e5f", "m019dd1021a")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
