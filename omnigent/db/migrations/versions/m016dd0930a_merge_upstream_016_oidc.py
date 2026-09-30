"""Join the upstream schema and Datadog OIDC credential branches.

Revision ID: m016dd0930a
Revises: ll1a2b3c4d5e, r6a8c0e2f4b6
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "m016dd0930a"
down_revision: tuple[str, str] = ("ll1a2b3c4d5e", "r6a8c0e2f4b6")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Both parent migrations have applied their schema changes."""


def downgrade() -> None:
    """Each parent branch handles its own schema rollback."""
