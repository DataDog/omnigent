"""add durable managed-sandbox lifecycle bindings

Revision ID: h2a4b6c8d0e2
Revises: a7c3e9f1b2d4
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "h2a4b6c8d0e2"
down_revision: str | None = "a7c3e9f1b2d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Store lifecycle references only; encrypted OIDC credentials stay separate."""
    with op.batch_alter_table("hosts") as batch_op:
        batch_op.add_column(sa.Column("sandbox_session_id", sa.String(length=64), nullable=True))
        batch_op.add_column(
            sa.Column("sandbox_credential_session_id", sa.String(length=64), nullable=True)
        )
        batch_op.add_column(
            sa.Column("sandbox_lifecycle_state", sa.String(length=32), nullable=True)
        )
        batch_op.add_column(
            sa.Column("sandbox_cleanup_attempts", sa.Integer(), nullable=False, server_default="0")
        )


def downgrade() -> None:
    """Remove the non-secret lifecycle bindings."""
    with op.batch_alter_table("hosts") as batch_op:
        batch_op.drop_column("sandbox_cleanup_attempts")
        batch_op.drop_column("sandbox_lifecycle_state")
        batch_op.drop_column("sandbox_credential_session_id")
        batch_op.drop_column("sandbox_session_id")
