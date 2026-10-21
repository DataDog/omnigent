"""Join the upstream v0.19 schema with the Datadog host index branch."""

from __future__ import annotations

from collections.abc import Sequence

revision: str = "m019dd1021a"
down_revision: tuple[str, str] = ("m017dd1006a", "nf12ab34cd56")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Both parent migrations have applied their schema changes."""


def downgrade() -> None:
    """Each parent branch handles its own schema rollback."""
