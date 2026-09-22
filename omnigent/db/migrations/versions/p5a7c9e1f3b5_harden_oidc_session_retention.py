"""Harden OIDC session lookup and bounded expiry maintenance.

Revision ID: p5a7c9e1f3b5
Revises: n4f6a8b0c2d4
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "p5a7c9e1f3b5"
down_revision: str | None = "n4f6a8b0c2d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Make opaque handles unique and make expiry cleanup index-backed."""
    op.drop_index("ix_oidc_sessions_handle_digest", table_name="oidc_sessions")
    op.create_unique_constraint(
        "uq_oidc_sessions_handle_digest", "oidc_sessions", ["workspace_id", "handle_digest"]
    )
    op.create_index(
        "ix_oidc_sessions_expiry_id",
        "oidc_sessions",
        ["workspace_id", "absolute_expiry", "id"],
    )


def downgrade() -> None:
    """Restore the non-unique lookup index."""
    op.drop_index("ix_oidc_sessions_expiry_id", table_name="oidc_sessions")
    op.drop_constraint("uq_oidc_sessions_handle_digest", "oidc_sessions", type_="unique")
    op.create_index(
        "ix_oidc_sessions_handle_digest", "oidc_sessions", ["workspace_id", "handle_digest"]
    )
