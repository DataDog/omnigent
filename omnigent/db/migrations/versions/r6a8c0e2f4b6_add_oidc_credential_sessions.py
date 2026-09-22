"""Add OIDC credential sessions and CLI delegation.

Revision ID: r6a8c0e2f4b6
Revises: gg1b2c3d4e5f

The managed-Hab work has not shipped, so this revision deliberately replaces
the branch's former OIDC/host-lifecycle migration history with the final,
linear schema.  Provider credentials are held only in ``oidc_sessions``;
durable hosts do not retain a browser or CLI session reference.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "r6a8c0e2f4b6"
down_revision: str | None = "gg1b2c3d4e5f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID16 = sa.LargeBinary(16).with_variant(mysql.BINARY(16), "mysql")


def upgrade() -> None:
    """Create encrypted OIDC credential storage and CLI delegation binding."""
    op.create_table(
        "oidc_sessions",
        sa.Column(
            "workspace_id", sa.BigInteger(), primary_key=True, nullable=False, server_default="0"
        ),
        sa.Column("id", _UUID16, primary_key=True, nullable=False),
        sa.Column("handle_digest", sa.String(64), nullable=False),
        sa.Column("user_id", sa.String(256), nullable=False),
        sa.Column("provider_subject", sa.String(256), nullable=True),
        sa.Column("provider_issuer", sa.String(2048), nullable=True),
        sa.Column("provider_client_id", sa.String(512), nullable=True),
        sa.Column("credential_ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("id_token_expiry", sa.Integer(), nullable=True),
        sa.Column("absolute_expiry", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
        sa.Column("credential_version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("refresh_lease_id", sa.String(64), nullable=True),
        sa.Column("refresh_lease_expires_at", sa.Integer(), nullable=True),
        sa.Column("revoked_at", sa.Integer(), nullable=True),
        sa.UniqueConstraint(
            "workspace_id", "handle_digest", name="uq_oidc_sessions_handle_digest"
        ),
    )
    op.create_index("ix_oidc_sessions_user_id", "oidc_sessions", ["workspace_id", "user_id"])
    op.create_index(
        "ix_oidc_sessions_expiry_id",
        "oidc_sessions",
        ["workspace_id", "absolute_expiry", "id"],
    )
    with op.batch_alter_table("device_grants") as batch_op:
        batch_op.add_column(sa.Column("oidc_session_id", _UUID16, nullable=True))


def downgrade() -> None:
    """Remove the unshipped OIDC credential and delegation schema."""
    with op.batch_alter_table("device_grants") as batch_op:
        batch_op.drop_column("oidc_session_id")
    op.drop_index("ix_oidc_sessions_expiry_id", table_name="oidc_sessions")
    op.drop_index("ix_oidc_sessions_user_id", table_name="oidc_sessions")
    op.drop_table("oidc_sessions")
