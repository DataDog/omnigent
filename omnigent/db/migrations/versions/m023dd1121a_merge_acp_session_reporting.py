"""Merge ACP session reporting with upstream durable elicitations.

Revision ID: m023dd1121a
Revises: ab13c7d9e026, m022dd1118a
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "m023dd1121a"
down_revision: tuple[str, str] = ("ab13c7d9e026", "m022dd1118a")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
