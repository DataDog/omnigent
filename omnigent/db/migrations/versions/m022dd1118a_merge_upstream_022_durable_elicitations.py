"""Merge upstream schema changes with durable elicitations.

Revision ID: m022dd1118a
Revises: 7b915a1fce74, a7d4c2e9f610
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "m022dd1118a"
down_revision: tuple[str, str] = ("7b915a1fce74", "a7d4c2e9f610")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
