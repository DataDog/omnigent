"""Existing release databases gain usable ACP report storage on upgrade."""

from __future__ import annotations

import hashlib
import json
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.schema import CreateTable, PrimaryKeyConstraint, UniqueConstraint

from omnigent.db.cockroachdb import _crdb_server_version, _prepare_crdb_schema_transaction
from omnigent.db.db_models import (
    BoundedUtf8Identity,
    SqlAcpReportConnectionAttempt,
    SqlAcpReportEpoch,
    SqlAcpReportJournal,
    SqlAcpReportOwner,
    SqlAcpReportRootState,
    SqlAcpSessionAssociation,
    SqlAcpSessionEntity,
    SqlAcpSessionReportReceipt,
    SqlAcpToolCallItemBinding,
    SqlConversation,
    Uuid16,
    current_workspace_id,
)
from omnigent.db.utils import _build_alembic_config, get_or_create_engine
from omnigent.inner.acp_standard_subagents import acp_standard_report_source_id
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore

_RELEASE_REVISION = "a7d4c2e9f610"
_NATIVE_EPOCH = "migration-native-epoch"
_ROOT_NATIVE_SESSION_ID = "migration-native-root"
_SERVER_IDENTITY = "migration-server"
_CONTEXT_GENERATION = "migration-context"
_RUNNER_ID = "migration-runner"
_PRINCIPAL = "migration-owner@example.test"
_REPORTING_TABLES = {
    "acp_session_report_receipts": SqlAcpSessionReportReceipt.__table__,
    "acp_report_root_state": SqlAcpReportRootState.__table__,
    "acp_report_connection_attempts": SqlAcpReportConnectionAttempt.__table__,
    "acp_report_journals": SqlAcpReportJournal.__table__,
    "acp_report_owners": SqlAcpReportOwner.__table__,
    "acp_report_epochs": SqlAcpReportEpoch.__table__,
    "acp_session_associations": SqlAcpSessionAssociation.__table__,
    "acp_session_entities": SqlAcpSessionEntity.__table__,
    "acp_tool_call_item_bindings": SqlAcpToolCallItemBinding.__table__,
}


def _capture_reporting_migration(monkeypatch: pytest.MonkeyPatch):
    migration = import_module(
        "omnigent.db.migrations.versions.ab13c7d9e026_add_acp_session_reporting_tables"
    )
    metadata = sa.MetaData()
    tables: dict[str, sa.Table] = {}
    columns: dict[tuple[str, str], sa.Column[Any]] = {}
    indexes: dict[str, tuple[str, tuple[str, ...]]] = {}

    class CapturingOperations:
        def add_column(self, table_name: str, column: sa.Column[Any], **_kwargs: Any) -> None:
            assert column.name is not None
            columns[(table_name, column.name)] = column

        def create_table(self, table_name: str, *elements: Any, **kwargs: Any) -> None:
            tables[table_name] = sa.Table(table_name, metadata, *elements, **kwargs)

        def create_index(
            self,
            index_name: str,
            table_name: str,
            columns: list[str],
            **_kwargs: Any,
        ) -> None:
            indexes[index_name] = (table_name, tuple(columns))

    monkeypatch.setattr(migration, "op", CapturingOperations())
    migration.upgrade()
    return tables, columns, indexes


def _key_byte_width(column: sa.Column[Any]) -> int:
    if isinstance(column.type, BoundedUtf8Identity):
        return column.type.max_utf8_bytes
    if isinstance(column.type, Uuid16):
        return 16
    if isinstance(column.type, sa.String):
        assert column.type.length is not None
        return column.type.length * 4
    if isinstance(column.type, sa.BigInteger):
        return 8
    if isinstance(column.type, sa.Boolean):
        return 1
    raise AssertionError(f"unaccounted indexed column type: {column.type!r}")


def _constraint_signature(table: sa.Table) -> list[tuple[str, str, tuple[str, ...]]]:
    constraints = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint))
    ]
    signatures = [
        (
            type(constraint).__name__,
            constraint.name or "",
            tuple(column.name for column in constraint.columns),
        )
        for constraint in constraints
    ]
    return sorted(signatures)


def test_reporting_models_and_migration_keep_bounded_mysql_identity_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Model and migration declarations retain bounded keys and root count parity."""
    migration_tables, migration_columns, migration_indexes = _capture_reporting_migration(
        monkeypatch
    )
    assert set(migration_tables) == set(_REPORTING_TABLES)
    assert set(migration_columns) == {("conversations", "acp_tool_call_binding_count")}
    model_binding_count = SqlConversation.__table__.c.acp_tool_call_binding_count
    migration_binding_count = migration_columns[("conversations", "acp_tool_call_binding_count")]
    assert isinstance(model_binding_count.type, sa.Integer)
    assert isinstance(migration_binding_count.type, sa.Integer)
    assert model_binding_count.nullable is True
    assert migration_binding_count.nullable is True
    assert model_binding_count.default is not None
    assert model_binding_count.default.arg == 0
    assert migration_binding_count.default is None
    assert model_binding_count.server_default is None
    assert migration_binding_count.server_default is None

    identity_columns = {
        ("acp_session_report_receipts", "native_epoch"): (128, 512, False),
        ("acp_report_root_state", "current_native_epoch"): (128, 512, True),
        ("acp_report_epochs", "native_epoch"): (128, 512, False),
        ("acp_session_associations", "native_epoch"): (128, 512, False),
        ("acp_session_associations", "native_session_id"): (512, 512, False),
        ("acp_session_associations", "parent_native_session_id"): (512, 512, True),
        ("acp_session_entities", "native_epoch"): (128, 512, False),
        ("acp_session_entities", "native_session_id"): (512, 512, False),
        ("acp_session_entities", "entity_id"): (512, 512, False),
        ("acp_tool_call_item_bindings", "native_epoch"): (128, 512, True),
        ("acp_tool_call_item_bindings", "native_call_id"): (512, 512, False),
    }
    assert len(identity_columns) == 11
    dialect_expectations = (
        (mysql.dialect(), "mysql"),
        (postgresql.dialect(), "text"),
        (sqlite.dialect(), "text"),
    )
    for (table_name, column_name), (characters, byte_limit, nullable) in identity_columns.items():
        for table in (_REPORTING_TABLES[table_name], migration_tables[table_name]):
            column = table.c[column_name]
            assert isinstance(column.type, BoundedUtf8Identity)
            assert (column.type.max_characters, column.type.max_utf8_bytes) == (
                characters,
                byte_limit,
            )
            assert column.nullable is nullable
            for dialect, representation in dialect_expectations:
                compiled = column.type.compile(dialect=dialect)
                expected = (
                    f"VARBINARY({byte_limit})"
                    if representation == "mysql"
                    else f"VARCHAR({characters})"
                )
                assert compiled == expected

    reference_columns = {
        ("acp_session_associations", column_name)
        for column_name in ("association_json", "observation_json", "action_origin_json")
    } | {
        ("acp_session_entities", column_name)
        for column_name in ("item_data_json", "pending_json", "references_json")
    }
    assert len(reference_columns) == 6
    for table_name, column_name in reference_columns:
        for table in (_REPORTING_TABLES[table_name], migration_tables[table_name]):
            column = table.c[column_name]
            assert isinstance(column.type, sa.String)
            assert column.type.length == 1_024
            assert column.type.compile(dialect=mysql.dialect()) == "VARCHAR(1024)"

    model_indexes = {
        index.name: (table.name, tuple(column.name for column in index.columns))
        for table in _REPORTING_TABLES.values()
        for index in table.indexes
    }
    assert migration_indexes == model_indexes
    for table_name, model_table in _REPORTING_TABLES.items():
        migration_table = migration_tables[table_name]
        assert _constraint_signature(model_table) == _constraint_signature(migration_table)
        model_mysql_ddl = str(CreateTable(model_table).compile(dialect=mysql.dialect()))
        migration_mysql_ddl = str(CreateTable(migration_table).compile(dialect=mysql.dialect()))
        assert "CREATE TABLE" in model_mysql_ddl
        assert "CREATE TABLE" in migration_mysql_ddl

    key_byte_widths: dict[str, int] = {}
    for table in _REPORTING_TABLES.values():
        for constraint in table.constraints:
            if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
                name = constraint.name or f"{table.name}.pk"
                key_byte_widths[name] = sum(
                    _key_byte_width(column) for column in constraint.columns
                )
        for index in table.indexes:
            assert index.name is not None
            key_byte_widths[index.name] = sum(_key_byte_width(column) for column in index.columns)
    assert key_byte_widths["ix_acp_session_associations_root_epoch_parent"] == 1_560
    assert key_byte_widths["acp_session_entities.pk"] == 1_624
    assert key_byte_widths["uq_acp_tool_call_item_epoch_call"] == 1_048
    assert all(byte_width <= 3_072 for byte_width in key_byte_widths.values())


def _migrate(uri: str, engine: sa.Engine, step: str, revision: str) -> None:
    config = _build_alembic_config(uri)
    with engine.connect() as connection:
        if engine.dialect.name == "cockroachdb":
            _prepare_crdb_schema_transaction(connection, _crdb_server_version(engine))
        config.attributes["connection"] = connection
        getattr(command, step)(config, revision)
        connection.commit()


def _server_connection_id(root_id: str) -> str:
    """Return a stable UUID16 value for generated server connection identity."""
    return hashlib.sha256(f"migration:{root_id}".encode()).hexdigest()[:32]


def _admit_report_owner(store: SqlAlchemyConversationStore, root_id: str) -> tuple[str, str]:
    server_connection_id = _server_connection_id(root_id)
    attempt_id = store.prepare_acp_report_connection_attempt(
        root_id,
        _RUNNER_ID,
        _PRINCIPAL,
        server_connection_id,
    )
    admission = store.admit_acp_report_connection(
        attempt_id,
        SimpleNamespace(
            runner_id=_RUNNER_ID,
            owner=_PRINCIPAL,
            server_connection_id=server_connection_id,
            superseded=False,
        ),
    )
    connection_admission_id = admission["connection_admission_id"]
    journal_id = hashlib.sha256(root_id.encode()).hexdigest()[:32]
    store.admit_acp_report_owner(root_id, connection_admission_id, 1, journal_id)
    return connection_admission_id, journal_id


def _apply_report(
    store: SqlAlchemyConversationStore,
    *,
    root_id: str,
    sequence: int,
    target_id: str,
    parent_id: str | None,
    conversation_id: str,
    kind: str,
    operation: dict[str, object],
) -> dict[str, object]:
    connection_admission_id, journal_id = _admit_report_owner(store, root_id)
    store.activate_acp_report_epoch(
        root_id,
        connection_admission_id,
        1,
        journal_id,
        _NATIVE_EPOCH,
        1,
    )
    source_id = acp_standard_report_source_id(_SERVER_IDENTITY, root_id, _NATIVE_EPOCH, sequence)
    payload_hash = hashlib.sha256(
        json.dumps(
            {"kind": kind, "operation": operation, "sequence": sequence},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return store.apply_acp_session_report(
        root_conversation_id=root_id,
        connection_admission_id=connection_admission_id,
        runtime_incarnation=1,
        journal_id=journal_id,
        journal_sequence=sequence,
        epoch_ordinal=1,
        native_epoch=_NATIVE_EPOCH,
        ingress_sequence=sequence,
        source_id=source_id,
        payload_hash=payload_hash,
        context_generation=_CONTEXT_GENERATION,
        root_native_session_id=_ROOT_NATIVE_SESSION_ID,
        target_session_id=target_id,
        parent_session_id=parent_id,
        conversation_id=conversation_id,
        kind=kind,
        operation=operation,
    )


def test_release_schema_upgrade_preserves_data_and_applies_reports(
    db_uri: str, tmp_path: Path
) -> None:
    """The release migration head upgrades in place to readable report tables.

    The default SQLite fixture and the PostgreSQL test database both start at
    Alembic head. Rewind only the new reporting migration, retain an existing
    session row, then re-apply the migration and exercise association/entity
    writes through the production store API.
    """
    engine = get_or_create_engine(db_uri)
    store = SqlAlchemyConversationStore(
        db_uri,
        artifact_store=LocalArtifactStore(str(tmp_path / "artifacts")),
    )
    root = store.create_conversation(title="existing release session", runner_id=_RUNNER_ID)
    child = store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=root.id,
        title="worker:existing child",
    )

    try:
        _migrate(db_uri, engine, "downgrade", _RELEASE_REVISION)
        table_names = set(sa.inspect(engine).get_table_names())
        assert "acp_session_report_receipts" not in table_names
        assert "acp_session_associations" not in table_names
        assert "acp_session_entities" not in table_names
        assert "acp_tool_call_item_bindings" not in table_names
        assert "acp_tool_call_binding_count" not in {
            column["name"] for column in sa.inspect(engine).get_columns("conversations")
        }

        _migrate(db_uri, engine, "upgrade", "head")
        table_names = set(sa.inspect(engine).get_table_names())
        assert set(_REPORTING_TABLES).issubset(table_names)
        inspector = sa.inspect(engine)
        conversation_columns = {
            column["name"]: column for column in inspector.get_columns("conversations")
        }
        binding_count_column = conversation_columns["acp_tool_call_binding_count"]
        assert binding_count_column["nullable"] is True
        assert binding_count_column["default"] is None
        with store._conv_session("test_acp_migration_existing_binding_count") as session:
            existing_root = session.get(SqlConversation, (current_workspace_id(), root.id))
            assert existing_root is not None
            assert existing_root.acp_tool_call_binding_count is None
        assert any(
            constraint["name"] == "uq_acp_report_epoch_root_native_id"
            and constraint["column_names"]
            == ["workspace_id", "root_conversation_id", "native_epoch"]
            for constraint in inspector.get_unique_constraints("acp_report_epochs")
        )
        for table_name in _REPORTING_TABLES:
            assert all(
                column["default"] is None for column in inspector.get_columns(table_name)
            ), table_name

        assert inspector.get_pk_constraint("acp_session_associations")["constrained_columns"] == [
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            "native_session_id",
        ]
        assert inspector.get_pk_constraint("acp_session_entities")["constrained_columns"] == [
            "workspace_id",
            "root_conversation_id",
            "native_epoch",
            "native_session_id",
            "entity_type",
            "entity_id",
        ]
        association_indexes = {
            index["name"]: index["column_names"]
            for index in inspector.get_indexes("acp_session_associations")
        }
        assert association_indexes["ix_acp_session_associations_conversation"] == [
            "workspace_id",
            "conversation_id",
            "root_conversation_id",
            "native_epoch",
            "native_session_id",
        ]
        assert not inspector.get_indexes("acp_session_entities")

        _apply_report(
            store,
            root_id=root.id,
            sequence=1,
            target_id=_ROOT_NATIVE_SESSION_ID,
            parent_id=None,
            conversation_id=root.id,
            kind="observation.patch",
            operation={"observation": {"session_info": {"title": "existing release session"}}},
        )
        _apply_report(
            store,
            root_id=root.id,
            sequence=2,
            target_id="migration-native-child",
            parent_id=_ROOT_NATIVE_SESSION_ID,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": "migration-native-child",
                "parent_session_id": _ROOT_NATIVE_SESSION_ID,
                "patch": {"title": "worker", "capabilities": {"cancel": {}}},
            },
        )
        _apply_report(
            store,
            root_id=root.id,
            sequence=3,
            target_id="migration-native-child",
            parent_id=_ROOT_NATIVE_SESSION_ID,
            conversation_id=child.id,
            kind="entity.upsert",
            operation={
                "entity_type": "message",
                "entity_id": "migration-child-message",
                "value": {
                    "item_type": "message",
                    "response_id": "migration-child-response",
                    "item_data": {
                        "role": "assistant",
                        "agent": "unknown",
                        "acp_agent_identity": "unknown",
                        "content": [{"type": "text", "text": "upgraded report"}],
                    },
                },
            },
        )

        association = store.get_acp_session_association(
            root.id, _NATIVE_EPOCH, "migration-native-child"
        )
        assert association is not None
        assert association["conversation_id"] == child.id
        assert association["root_conversation_id"] == root.id
        assert association["association"]["capabilities"] == {"cancel": {}}
        assert store.get_conversation(root.id) is not None
        item = store.list_items(child.id).data[0]
        assert item.data.content[0]["text"] == "upgraded report"
        with store._conv_session("test_acp_migration_uuid16_round_trip") as session:
            entity = session.get(
                SqlAcpSessionEntity,
                (
                    current_workspace_id(),
                    root.id,
                    _NATIVE_EPOCH,
                    "migration-native-child",
                    "message",
                    "migration-child-message",
                ),
            )
            assert entity is not None
            assert entity.stable_item_id == item.id
    finally:
        # The shared PostgreSQL worker database is reused by subsequent tests.
        # Leave it at the current head even if an assertion fails.
        _migrate(db_uri, engine, "upgrade", "head")
