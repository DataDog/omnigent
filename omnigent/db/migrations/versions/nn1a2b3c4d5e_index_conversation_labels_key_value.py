"""index conversation labels by key and value

Revision ID: nn1a2b3c4d5e
Revises: m019dd1021a
Create Date: 2026-11-04 00:00:00.000000

Adds ``ix_conversation_labels_key_value`` on ``(workspace_id, key, value,
conversation_id)`` so a label lookup (e.g. the git worktree attempt label that
finds the session a retried create already made) is an index seek instead of a
table scan. See ``designs/SESSION_GIT_WORKTREE.md``.

Deployment: additive; older application code ignores it. On PostgreSQL it
builds ``CONCURRENTLY`` so label writes continue during the build. Roll back by
downgrading this revision.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "nn1a2b3c4d5e"
down_revision: str | None = "m019dd1021a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = "ix_conversation_labels_key_value"
_TABLE = "conversation_labels"


_COLUMNS = ["workspace_id", "key", "value", "conversation_id"]


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        op.create_index(_INDEX, _TABLE, _COLUMNS)
        return
    # CONCURRENTLY can't run in a transaction; autocommit lets writers proceed.
    with op.get_context().autocommit_block():
        # A failed concurrent build leaves an INVALID index that IF NOT EXISTS would keep.
        # Match it only on the table this connection resolves, never a same-named index elsewhere.
        invalid = (
            op.get_bind()
            .execute(
                sa.text(
                    "SELECT i.indexrelid::regclass::text FROM pg_index i "
                    "JOIN pg_class c ON c.oid = i.indexrelid "
                    "WHERE i.indrelid = to_regclass(:table) AND c.relname = :name "
                    "AND NOT i.indisvalid"
                ),
                {"table": _TABLE, "name": _INDEX},
            )
            .scalar()
        )
        if invalid:
            # regclass text is quoted and schema-qualified as needed; CONCURRENTLY
            # avoids blocking writers behind an exclusive lock.
            op.execute(f"DROP INDEX CONCURRENTLY {invalid}")
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {_INDEX} ON {_TABLE} ({', '.join(_COLUMNS)})"
        )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_TABLE)
