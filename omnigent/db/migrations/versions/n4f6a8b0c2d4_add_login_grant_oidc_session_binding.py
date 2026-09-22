"""Bind first-party CLI login grants to encrypted OIDC sessions.

Revision ID: n4f6a8b0c2d4
Revises: m3e5f7a9b1c2

The nullable column is intentionally not a database foreign key: Omnigent's
tenant-scoped credential rows follow the existing no-FK convention.  It is an
internal, non-bearer ID; opaque ``sess_`` handles are never persisted here.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "n4f6a8b0c2d4"
down_revision: str | None = "m3e5f7a9b1c2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the optional OIDC delegation binding to device grants."""
    with op.batch_alter_table("device_grants") as batch_op:
        batch_op.add_column(sa.Column("oidc_session_id", sa.BINARY(16), nullable=True))


def downgrade() -> None:
    """Remove the OIDC delegation binding."""
    with op.batch_alter_table("device_grants") as batch_op:
        batch_op.drop_column("oidc_session_id")
