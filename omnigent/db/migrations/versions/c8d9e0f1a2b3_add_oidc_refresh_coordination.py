"""Add durable OIDC refresh coordination and provider binding fields.

Revision ID: c8d9e0f1a2b3
Revises: a7c3e9f1b2d4
Create Date: 2026-09-16

The encrypted credential blob remains the only token-bearing field. These
columns record the non-secret OIDC identity binding and coordinate one refresh
request across application processes without holding a transaction open while
calling the provider.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c8d9e0f1a2b3"
down_revision: str | None = "a7c3e9f1b2d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add non-secret identity binding, version, and lease columns."""
    with op.batch_alter_table("oidc_sessions") as batch_op:
        batch_op.add_column(sa.Column("provider_issuer", sa.String(2048), nullable=True))
        batch_op.add_column(sa.Column("provider_client_id", sa.String(512), nullable=True))
        batch_op.add_column(
            sa.Column("credential_version", sa.Integer(), nullable=False, server_default="0")
        )
        batch_op.add_column(sa.Column("refresh_lease_id", sa.String(64), nullable=True))
        batch_op.add_column(sa.Column("refresh_lease_expires_at", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Remove refresh coordination and provider binding fields."""
    with op.batch_alter_table("oidc_sessions") as batch_op:
        batch_op.drop_column("refresh_lease_expires_at")
        batch_op.drop_column("refresh_lease_id")
        batch_op.drop_column("credential_version")
        batch_op.drop_column("provider_client_id")
        batch_op.drop_column("provider_issuer")
