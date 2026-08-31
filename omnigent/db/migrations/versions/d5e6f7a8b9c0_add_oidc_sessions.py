"""Add oidc_sessions table for encrypted provider session storage.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d5e6f7a8b9c0"
down_revision: str | None = "c4d5e6f7a8b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the oidc_sessions table."""
    op.create_table(
        "oidc_sessions",
        sa.Column("workspace_id", sa.BigInteger(), primary_key=True, nullable=False, server_default="0"),
        sa.Column("id", sa.LargeBinary(16), primary_key=True, nullable=False),
        sa.Column("handle_digest", sa.String(64), nullable=False),
        sa.Column("user_id", sa.String(256), nullable=False),
        sa.Column("provider_subject", sa.String(256), nullable=True),
        sa.Column("credential_ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("id_token_expiry", sa.Integer(), nullable=True),
        sa.Column("absolute_expiry", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
        sa.Column("revoked_at", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_oidc_sessions_handle_digest",
        "oidc_sessions",
        ["workspace_id", "handle_digest"],
    )
    op.create_index(
        "ix_oidc_sessions_user_id",
        "oidc_sessions",
        ["workspace_id", "user_id"],
    )


def downgrade() -> None:
    """Drop the oidc_sessions table."""
    op.drop_index("ix_oidc_sessions_user_id", table_name="oidc_sessions")
    op.drop_index("ix_oidc_sessions_handle_digest", table_name="oidc_sessions")
    op.drop_table("oidc_sessions")
