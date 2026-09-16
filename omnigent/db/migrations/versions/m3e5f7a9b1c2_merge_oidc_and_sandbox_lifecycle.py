"""Merge OIDC refresh and managed-sandbox lifecycle migrations.

Revision ID: m3e5f7a9b1c2
Revises: c8d9e0f1a2b3, h2a4b6c8d0e2
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "m3e5f7a9b1c2"
down_revision: tuple[str, str] = ("c8d9e0f1a2b3", "h2a4b6c8d0e2")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Join the two independent schema branches."""


def downgrade() -> None:
    """Split back to the two independent schema branches."""
