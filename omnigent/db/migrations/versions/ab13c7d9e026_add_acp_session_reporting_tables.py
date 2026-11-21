"""Add durable storage for normalized ACP session reports.

Revision ID: ab13c7d9e026
Revises: a7d4c2e9f610
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from omnigent.db.db_models import BoundedUtf8Identity, Uuid16

revision = "ab13c7d9e026"
down_revision = "a7d4c2e9f610"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "conversations",
        sa.Column("acp_tool_call_binding_count", sa.Integer(), nullable=True),
    )

    op.create_table(
        "acp_session_report_receipts",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("journal_id", Uuid16(), nullable=False),
        sa.Column("journal_sequence", sa.BigInteger(), nullable=False),
        sa.Column("epoch_ordinal", sa.BigInteger(), nullable=False),
        sa.Column(
            "native_epoch",
            BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column("ingress_sequence", sa.BigInteger(), nullable=False),
        sa.Column("connection_admission_id", Uuid16(), nullable=False),
        sa.Column("runtime_incarnation", sa.BigInteger(), nullable=False),
        sa.Column("source_id", sa.String(69), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("applied", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint(
            "workspace_id",
            "root_conversation_id",
            "journal_id",
            "journal_sequence",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "journal_id",
            "native_epoch",
            "ingress_sequence",
            name="uq_acp_report_journal_epoch_ingress",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "source_id",
            name="uq_acp_report_receipt_source",
        ),
    )

    op.create_table(
        "acp_report_root_state",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("runner_id", sa.String(256), nullable=False),
        sa.Column("authenticated_principal", sa.String(256), nullable=True),
        sa.Column("principal_bound", sa.Boolean(), nullable=False),
        sa.Column("source_revision", sa.BigInteger(), nullable=False),
        sa.Column("current_connection_admission_id", Uuid16(), nullable=True),
        sa.Column("current_runtime_incarnation", sa.BigInteger(), nullable=True),
        sa.Column("current_journal_id", Uuid16(), nullable=True),
        sa.Column("current_epoch_ordinal", sa.BigInteger(), nullable=True),
        sa.Column(
            "current_native_epoch",
            BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("workspace_id", "root_conversation_id"),
    )

    op.create_table(
        "acp_report_connection_attempts",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", Uuid16(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("server_connection_id", Uuid16(), nullable=False),
        sa.Column("authenticated_runner_id", sa.String(256), nullable=False),
        sa.Column("authenticated_principal", sa.String(256), nullable=True),
        sa.Column("expected_source_revision", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("admission_revision", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("workspace_id", "attempt_id"),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "server_connection_id",
            name="uq_acp_report_attempt_connection",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "attempt_id",
            name="uq_acp_report_attempt_root_id",
        ),
        sa.CheckConstraint(
            "state IN ('prepared', 'admitted', 'rejected')",
            name="ck_acp_report_attempt_state",
        ),
    )

    op.create_table(
        "acp_report_journals",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("journal_id", Uuid16(), nullable=False),
        sa.Column("highest_epoch_ordinal", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("workspace_id", "root_conversation_id", "journal_id"),
        sa.UniqueConstraint("workspace_id", "journal_id", name="uq_acp_report_journal_global_id"),
    )

    op.create_table(
        "acp_report_owners",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("connection_admission_id", Uuid16(), nullable=False),
        sa.Column("runtime_incarnation", sa.BigInteger(), nullable=False),
        sa.Column("journal_id", Uuid16(), nullable=False),
        sa.PrimaryKeyConstraint(
            "workspace_id",
            "root_conversation_id",
            "connection_admission_id",
            "runtime_incarnation",
        ),
    )

    op.create_table(
        "acp_report_epochs",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("journal_id", Uuid16(), nullable=False),
        sa.Column("epoch_ordinal", sa.BigInteger(), nullable=False),
        sa.Column(
            "native_epoch",
            BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "workspace_id", "root_conversation_id", "journal_id", "epoch_ordinal"
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            name="uq_acp_report_epoch_root_native_id",
        ),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "journal_id",
            "native_epoch",
            name="uq_acp_report_epoch_native_id",
        ),
    )

    op.create_table(
        "acp_session_associations",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column(
            "native_epoch",
            BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column(
            "native_session_id",
            BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column("context_generation", sa.String(256), nullable=False),
        sa.Column(
            "parent_native_session_id",
            BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512),
            nullable=True,
        ),
        sa.Column("conversation_id", Uuid16(), nullable=False),
        sa.Column("association_source_id", sa.String(69), nullable=True),
        sa.Column("association_json", sa.String(1_024), nullable=False),
        sa.Column("observation_json", sa.String(1_024), nullable=False),
        sa.Column("state_ingress_sequence", sa.BigInteger(), nullable=True),
        sa.Column("state_observed_at", sa.BigInteger(), nullable=True),
        sa.Column("last_reported_at", sa.BigInteger(), nullable=True),
        sa.Column("action_origin_json", sa.String(1_024), nullable=True),
        sa.Column("last_ingress_sequence", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint(
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            "native_session_id",
        ),
    )
    op.create_index(
        "ix_acp_session_associations_conversation",
        "acp_session_associations",
        [
            "workspace_id",
            "conversation_id",
            "root_conversation_id",
            "native_epoch",
            "native_session_id",
        ],
    )
    op.create_index(
        "ix_acp_session_associations_root_epoch_parent",
        "acp_session_associations",
        [
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            "parent_native_session_id",
            "native_session_id",
        ],
    )

    op.create_table(
        "acp_session_entities",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column(
            "native_epoch",
            BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column(
            "native_session_id",
            BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column("entity_type", sa.String(16), nullable=False),
        sa.Column(
            "entity_id",
            BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column("conversation_id", Uuid16(), nullable=False),
        sa.Column("item_type", sa.String(64), nullable=True),
        sa.Column("response_id", sa.String(64), nullable=True),
        sa.Column("item_data_json", sa.String(1_024), nullable=False),
        sa.Column("pending_json", sa.String(1_024), nullable=False),
        sa.Column("references_json", sa.String(1_024), nullable=True),
        sa.Column("stable_item_id", Uuid16(), nullable=False),
        sa.Column("last_ingress_sequence", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint(
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            "native_session_id",
            "entity_type",
            "entity_id",
        ),
    )

    op.create_table(
        "acp_tool_call_item_bindings",
        sa.Column("workspace_id", sa.BigInteger(), nullable=False),
        sa.Column("root_conversation_id", Uuid16(), nullable=False),
        sa.Column("source_id", sa.String(69), nullable=False),
        sa.Column(
            "native_epoch",
            BoundedUtf8Identity(max_characters=128, max_utf8_bytes=512),
            nullable=True,
        ),
        sa.Column("journal_id", Uuid16(), nullable=True),
        sa.Column("epoch_ordinal", sa.BigInteger(), nullable=True),
        sa.Column("context_generation", sa.String(256), nullable=True),
        sa.Column(
            "native_call_id",
            BoundedUtf8Identity(max_characters=512, max_utf8_bytes=512),
            nullable=False,
        ),
        sa.Column("action_origin_token", sa.String(256), nullable=False),
        sa.Column("action_origin_generation", sa.String(256), nullable=False),
        sa.Column("conversation_id", Uuid16(), nullable=False),
        sa.Column("canonical_item_id", Uuid16(), nullable=True),
        sa.Column("response_id", sa.String(64), nullable=True),
        sa.PrimaryKeyConstraint("workspace_id", "root_conversation_id", "source_id"),
        sa.UniqueConstraint(
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            "native_call_id",
            name="uq_acp_tool_call_item_epoch_call",
        ),
    )


def downgrade() -> None:
    op.drop_table("acp_tool_call_item_bindings")
    op.drop_index(
        "ix_acp_session_associations_root_epoch_parent",
        table_name="acp_session_associations",
    )
    op.drop_index(
        "ix_acp_session_associations_conversation",
        table_name="acp_session_associations",
    )
    op.drop_table("acp_session_entities")
    op.drop_table("acp_session_associations")
    op.drop_table("acp_report_epochs")
    op.drop_table("acp_report_owners")
    op.drop_table("acp_report_journals")
    op.drop_table("acp_report_connection_attempts")
    op.drop_table("acp_report_root_state")
    op.drop_table("acp_session_report_receipts")
    op.drop_column("conversations", "acp_tool_call_binding_count")
