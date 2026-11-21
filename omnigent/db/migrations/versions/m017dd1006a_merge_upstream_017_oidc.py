"""Join the upstream schema and Datadog OIDC credential branches.

Revision ID: m017dd1006a
Revises: mm1a2b3c4d5e, m016dd0930a
"""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "m017dd1006a"
down_revision: tuple[str, str] = ("mm1a2b3c4d5e", "m016dd0930a")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Both parent migrations have applied their schema changes."""


def downgrade() -> None:
    """Each parent branch handles its own schema rollback."""
