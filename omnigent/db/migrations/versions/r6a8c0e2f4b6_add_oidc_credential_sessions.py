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

_OIDC_SESSION_BASELINE_COLUMNS = frozenset(
    {
        "workspace_id",
        "id",
        "handle_digest",
        "user_id",
        "provider_subject",
        "credential_ciphertext",
        "id_token_expiry",
        "absolute_expiry",
        "created_at",
        "updated_at",
        "revoked_at",
    }
)


def _upgrade_cockroachdb_baseline() -> bool:
    """Upgrade the schema CRDB bootstrapped at the supported baseline.

    CockroachDB bootstraps from ORM metadata and stamps
    ``gf1b2c3d4e5f``.  That metadata already contained the original OIDC
    table on feature-development databases, so replaying this linearized
    migration must evolve that known table instead of creating it again.  A
    table with a different shape is not a supported baseline and fails closed.
    """
    bind = op.get_bind()
    if bind.dialect.name != "cockroachdb":
        return False

    inspector = sa.inspect(bind)
    if "oidc_sessions" not in inspector.get_table_names():
        return False

    existing_columns = {column["name"] for column in inspector.get_columns("oidc_sessions")}
    missing_baseline_columns = _OIDC_SESSION_BASELINE_COLUMNS - existing_columns
    if missing_baseline_columns:
        raise RuntimeError(
            "CockroachDB has an unsupported oidc_sessions table; missing baseline columns: "
            + ", ".join(sorted(missing_baseline_columns))
        )

    # These are the only columns introduced after the original OIDC table.
    # IF NOT EXISTS makes the upgrade work both for a historical bootstrap and
    # for a schema bootstrapped from current metadata then stamped at baseline.
    op.execute("ALTER TABLE oidc_sessions ADD COLUMN IF NOT EXISTS provider_issuer STRING")
    op.execute("ALTER TABLE oidc_sessions ADD COLUMN IF NOT EXISTS provider_client_id STRING")
    op.execute(
        "ALTER TABLE oidc_sessions ADD COLUMN IF NOT EXISTS credential_version INT8 "
        "NOT NULL DEFAULT 0"
    )
    op.execute("ALTER TABLE oidc_sessions ADD COLUMN IF NOT EXISTS refresh_lease_id STRING")
    op.execute("ALTER TABLE oidc_sessions ADD COLUMN IF NOT EXISTS refresh_lease_expires_at INT8")
    op.execute("ALTER TABLE device_grants ADD COLUMN IF NOT EXISTS oidc_session_id BYTES")

    unique_constraints = {
        constraint["name"] for constraint in inspector.get_unique_constraints("oidc_sessions")
    }
    if "uq_oidc_sessions_handle_digest" not in unique_constraints:
        op.execute(
            "ALTER TABLE oidc_sessions ADD CONSTRAINT uq_oidc_sessions_handle_digest "
            "UNIQUE (workspace_id, handle_digest)"
        )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_oidc_sessions_user_id "
        "ON oidc_sessions (workspace_id, user_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_oidc_sessions_expiry_id "
        "ON oidc_sessions (workspace_id, absolute_expiry, id)"
    )
    return True


def upgrade() -> None:
    """Create encrypted OIDC credential storage and CLI delegation binding."""
    if _upgrade_cockroachdb_baseline():
        return
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
