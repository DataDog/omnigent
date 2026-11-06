"""Add the paused_call checkpoint to durable_elicitations.

Revision ID: ac1b2c3d4e5f
Revises: ab1b2c3d4e5f
Create Date: 2026-08-24 00:00:00.000000

Adds a nullable ``paused_call`` column: the JSON checkpoint of the tool call
parked on the durable question (response/turn id, call id, tool name,
arguments). A runner process restart kills the parked harness turn; recovery
uses this checkpoint plus the committed history to continue the same logical
execution exactly once instead of replaying the prompt.

Additive. No existing data needs backfill.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "ac1b2c3d4e5f"
down_revision: str | None = "ab1b2c3d4e5f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add ``paused_call`` to ``durable_elicitations`` (idempotent)."""
    existing = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("durable_elicitations")}
    if "paused_call" not in existing:
        op.add_column(
            "durable_elicitations",
            sa.Column("paused_call", sa.LargeBinary(), nullable=True),
        )


def downgrade() -> None:
    """Remove ``paused_call`` from ``durable_elicitations``."""
    with op.batch_alter_table("durable_elicitations") as batch_op:
        batch_op.drop_column("paused_call")
