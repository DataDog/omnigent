"""Add the durable_elicitations table.

Revision ID: ab1b2c3d4e5f
Revises: m017dd1006a
Create Date: 2026-08-24 00:00:00.000000

Adds the ``durable_elicitations`` table — the persisted pending state for
``sys_ask_user`` questions. A hosted agent parks on this row until a human
answers, surviving client disconnects and Omnigent server restarts. Legacy
policy approvals and external MCP elicitations keep their in-memory
lifecycle; only explicit durable-user-question registrations are stored
here.

Additive. No existing data needs backfill.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from omnigent.db.db_models import Uuid16

revision: str = "ab1b2c3d4e5f"
down_revision: str | None = "m017dd1006a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the ``durable_elicitations`` table and its indexes."""
    existing_tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "durable_elicitations" in existing_tables:
        return
    op.create_table(
        "durable_elicitations",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("id", Uuid16(), nullable=False),
        # Owning session whose parked execution the answer resumes.
        sa.Column("conversation_id", Uuid16(), nullable=False),
        # Presentation session (user-facing parent for child questions).
        sa.Column("target_conversation_id", Uuid16(), nullable=True),
        sa.Column("invocation_id", sa.String(128), nullable=False),
        sa.Column("fingerprint", sa.String(512), nullable=False),
        sa.Column("tool_name", sa.String(64), nullable=False),
        # Opaque free text stored compressed (CompressedText → LargeBinary).
        sa.Column("message", sa.LargeBinary(), nullable=False),
        sa.Column("requested_schema", sa.LargeBinary(), nullable=False),
        # Lifecycle as stable int codes (DURABLE_ELICITATION_STATE /
        # DURABLE_ELICITATION_ACTION in omnigent.db.enum_codecs):
        # state pending=1 answered=2 declined=3 canceled=4 consumed=5;
        # answer_action accept=1 decline=2 cancel=3.
        sa.Column("state", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column("answer_action", sa.SmallInteger(), nullable=True),
        sa.Column("answer_content", sa.LargeBinary(), nullable=True),
        sa.Column("delivered_at", sa.Integer(), nullable=True),
        sa.Column("consumed_at", sa.Integer(), nullable=True),
        sa.Column("delivery_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=True),
        sa.CheckConstraint("state IN (1, 2, 3, 4, 5)", name="ck_durable_elicitations_state"),
        sa.CheckConstraint("answer_action IN (1, 2, 3)", name="ck_durable_elicitations_action"),
        sa.PrimaryKeyConstraint("workspace_id", "id"),
        sa.UniqueConstraint(
            "workspace_id",
            "conversation_id",
            "invocation_id",
            name="uq_durable_elicitations_invocation",
        ),
    )
    op.create_index(
        "ix_durable_elicitations_conversation_state",
        "durable_elicitations",
        ["workspace_id", "conversation_id", "state", "id"],
        unique=False,
    )
    op.create_index(
        "ix_durable_elicitations_target_state",
        "durable_elicitations",
        ["workspace_id", "target_conversation_id", "state", "id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop the ``durable_elicitations`` table."""
    op.drop_table("durable_elicitations")
