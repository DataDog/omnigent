"""Durable identity and nested-ownership checks for standard ACP reports."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from omnigent.db.db_models import (
    SqlAcpReportJournal,
    SqlAcpReportRootState,
    SqlAcpSessionAssociation,
    SqlAcpSessionEntity,
    SqlAcpSessionReportReceipt,
    SqlAcpToolCallItemBinding,
    SqlConversation,
    SqlConversationItem,
    current_workspace_id,
)
from omnigent.entities import FunctionCallData, FunctionCallOutputData, NewConversationItem
from omnigent.inner.acp_executor import AcpAgentConfig, AcpExecutor
from omnigent.inner.acp_standard_subagents import (
    acp_standard_canonical_tool_item_id,
    acp_standard_report_source_id,
    acp_standard_tool_entity_id,
    acp_standard_tool_output_entity_id,
)
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.conversation_store import AcpToolCallAppendScope
from omnigent.stores.conversation_store import sqlalchemy_store as conversation_store_module
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore

_SERVER = "server-acp-store-test"
_CONTEXT = "context-fingerprint-test"
_EPOCH = "native-epoch-store-test"
_ROOT_NATIVE = "native-root-store-test"
_RUNNER_ID = "runner-acp-store-test"
_PRINCIPAL = "owner@example.test"


def test_canonical_tool_item_id_is_role_and_anchor_specific() -> None:
    source_id = acp_standard_report_source_id(_SERVER, "root-canonical-id-test", _EPOCH, 1)
    call_id = acp_standard_canonical_tool_item_id(
        "root-canonical-id-test", source_id, "function_call"
    )
    output_id = acp_standard_canonical_tool_item_id(
        "root-canonical-id-test", source_id, "function_call_output"
    )
    assert len(call_id) == len(output_id) == 32
    assert call_id != output_id
    assert call_id == acp_standard_canonical_tool_item_id(
        "root-canonical-id-test", source_id, "function_call"
    )
    assert call_id != acp_standard_canonical_tool_item_id(
        "another-root", source_id, "function_call"
    )
    assert call_id != acp_standard_canonical_tool_item_id(
        "root-canonical-id-test",
        acp_standard_report_source_id(_SERVER, "root-canonical-id-test", _EPOCH, 2),
        "function_call",
    )
    with pytest.raises(ValueError, match="role"):
        acp_standard_canonical_tool_item_id("root-canonical-id-test", source_id, "message")
    with pytest.raises(ValueError, match="anchor"):
        acp_standard_canonical_tool_item_id(
            "root-canonical-id-test", "invalid-source", "function_call"
        )


def _server_connection_id(label: str) -> str:
    """Return a stable UUID16 value for generated server connection identity."""
    return hashlib.sha256(label.encode()).hexdigest()[:32]


def _new_root(store: SqlAlchemyConversationStore):
    return store.create_conversation(runner_id=_RUNNER_ID)


def _connection_context(
    server_connection_id: str,
    *,
    runner_id: str = _RUNNER_ID,
    owner: str | None = _PRINCIPAL,
    superseded: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        runner_id=runner_id,
        owner=owner,
        server_connection_id=server_connection_id,
        superseded=superseded,
    )


def _report_owner(store: SqlAlchemyConversationStore, root_id: str) -> tuple[str, str, str]:
    server_connection_id = _server_connection_id(f"connection:{root_id}")
    attempt_id = store.prepare_acp_report_connection_attempt(
        root_id,
        _RUNNER_ID,
        _PRINCIPAL,
        server_connection_id,
    )
    context = _connection_context(server_connection_id)
    admission = store.admit_acp_report_connection(attempt_id, context)
    connection_admission_id = admission["connection_admission_id"]
    journal_id = hashlib.sha256(root_id.encode()).hexdigest()[:32]
    store.admit_acp_report_owner(root_id, connection_admission_id, 1, journal_id)
    return connection_admission_id, journal_id, server_connection_id


def _reconnect_report_owner(
    store: SqlAlchemyConversationStore, root_id: str, journal_id: str
) -> str:
    server_connection_id = _server_connection_id(f"reconnected:{root_id}")
    attempt_id = store.prepare_acp_report_connection_attempt(
        root_id, _RUNNER_ID, _PRINCIPAL, server_connection_id
    )
    admission = store.admit_acp_report_connection(
        attempt_id, _connection_context(server_connection_id)
    )
    assert admission == {
        "connection_admission_id": attempt_id,
        "current": True,
        "selector_changed": True,
    }
    store.admit_acp_report_owner(root_id, attempt_id, 1, journal_id)
    return attempt_id


def _report_selection_state(
    store: SqlAlchemyConversationStore, root_id: str, journal_id: str
) -> tuple[str | None, int | None, str | None, int]:
    with store._conv_session("test_acp_report_selection_state") as session:
        state = session.get(SqlAcpReportRootState, (current_workspace_id(), root_id))
        journal = session.get(SqlAcpReportJournal, (current_workspace_id(), root_id, journal_id))
        assert state is not None
        assert journal is not None
        return (
            state.current_journal_id,
            state.current_epoch_ordinal,
            state.current_native_epoch,
            journal.highest_epoch_ordinal,
        )


def test_artifact_store_binding_is_idempotent_and_rejects_rebinding(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    tmp_path: Path,
) -> None:
    """Reporting readiness requires one explicitly bound artifact instance."""
    assert conversation_store.acp_session_reporting_ready is True

    conversation_store.bind_artifact_store(artifact_store)
    assert conversation_store.acp_session_reporting_ready is True

    other_artifact_store = LocalArtifactStore(str(tmp_path / "different-artifacts"))
    with pytest.raises(
        RuntimeError,
        match="already bound to a different ArtifactStore instance",
    ):
        conversation_store.bind_artifact_store(other_artifact_store)

    assert conversation_store.acp_session_reporting_ready is True


def test_unconfigured_store_keeps_ordinary_operations_but_rejects_reports_before_mutation(
    conversation_store: SqlAlchemyConversationStore,
    db_uri: str,
) -> None:
    """ACP storage fails clearly before writing while normal conversations remain usable."""
    root = _new_root(conversation_store)
    admission_id, journal_id, _connection_id = _report_owner(conversation_store, root.id)
    conversation_store.activate_acp_report_epoch(
        root.id,
        admission_id,
        1,
        journal_id,
        _EPOCH,
        1,
    )
    unconfigured_store = SqlAlchemyConversationStore(db_uri)
    assert unconfigured_store.acp_session_reporting_ready is False

    with pytest.raises(
        RuntimeError,
        match="ACP session reporting requires an explicitly configured ArtifactStore",
    ):
        unconfigured_store.apply_acp_session_report(
            root_conversation_id=root.id,
            connection_admission_id=admission_id,
            runtime_incarnation=1,
            journal_id=journal_id,
            journal_sequence=1,
            epoch_ordinal=1,
            native_epoch=_EPOCH,
            ingress_sequence=1,
            source_id=acp_standard_report_source_id(_SERVER, root.id, _EPOCH, 1),
            payload_hash="a" * 64,
            context_generation=_CONTEXT,
            root_native_session_id=_ROOT_NATIVE,
            target_session_id="native-child-unconfigured",
            parent_session_id=_ROOT_NATIVE,
            conversation_id=root.id,
            kind="association.patch",
            operation={
                "session_id": "native-child-unconfigured",
                "parent_session_id": _ROOT_NATIVE,
                "patch": {"title": "worker"},
            },
        )

    assert (
        conversation_store.get_acp_session_association(
            root.id, _EPOCH, "native-child-unconfigured"
        )
        is None
    )
    ordinary = unconfigured_store.create_conversation(title="ordinary session")
    assert ordinary.title == "ordinary session"


def _acp_tool_call_binding_count(store: SqlAlchemyConversationStore, root_id: str) -> int | None:
    with store._conv_session("test_acp_tool_call_binding_count") as session:
        root = session.get(SqlConversation, (current_workspace_id(), root_id))
        assert root is not None
        return root.acp_tool_call_binding_count


def _set_acp_tool_call_binding_count(
    store: SqlAlchemyConversationStore, root_id: str, count: int
) -> None:
    with store._conv_session_immediate("test_seed_acp_tool_call_binding_count") as session:
        root = session.get(SqlConversation, (current_workspace_id(), root_id))
        assert root is not None
        root.acp_tool_call_binding_count = count


def _acp_tool_call_binding_exists(
    store: SqlAlchemyConversationStore, root_id: str, source_id: str
) -> bool:
    with store._conv_session("test_acp_tool_call_binding_exists") as session:
        return (
            session.get(
                SqlAcpToolCallItemBinding,
                (current_workspace_id(), root_id, source_id),
            )
            is not None
        )


def test_prepared_connection_attempts_compare_the_captured_predecessor_and_tombstone_old_work(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    connection_a = _server_connection_id("server-connection-a")
    connection_b = _server_connection_id("server-connection-b")
    connection_c = _server_connection_id("server-connection-c")
    attempt_a = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, connection_a
    )
    attempt_b = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, connection_b
    )
    context_b = _connection_context(connection_b)

    winner = conversation_store.admit_acp_report_connection(attempt_b, context_b)
    retry = conversation_store.admit_acp_report_connection(attempt_b, context_b)
    assert winner == {
        "connection_admission_id": attempt_b,
        "current": True,
        "selector_changed": False,
    }
    assert retry == winner

    with pytest.raises(ValueError, match="predecessor revision changed"):
        conversation_store.admit_acp_report_connection(
            attempt_a, _connection_context(connection_a)
        )
    with pytest.raises(ValueError, match="permanently rejected"):
        conversation_store.admit_acp_report_connection(
            attempt_a, _connection_context(connection_a)
        )

    attempt_c = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, connection_c
    )
    winner_c = conversation_store.admit_acp_report_connection(
        attempt_c, _connection_context(connection_c)
    )
    assert winner_c["current"] is True
    assert conversation_store.admit_acp_report_connection(attempt_b, context_b) == {
        "connection_admission_id": attempt_b,
        "current": False,
        "selector_changed": False,
    }
    with pytest.raises(ValueError, match="current connection admission"):
        conversation_store.admit_acp_report_owner(
            root.id, attempt_b, 1, hashlib.sha256(root.id.encode()).hexdigest()[:32]
        )


@pytest.mark.parametrize(
    ("runner_id", "owner", "server_connection_id", "superseded"),
    [
        ("another-runner", _PRINCIPAL, _server_connection_id("wrong-context"), False),
        (_RUNNER_ID, "another-owner@example.test", _server_connection_id("wrong-context"), False),
        (_RUNNER_ID, _PRINCIPAL, _server_connection_id("different-connection"), False),
        (_RUNNER_ID, _PRINCIPAL, _server_connection_id("wrong-context"), True),
    ],
)
def test_connection_attempt_rejects_unbound_or_superseded_context(
    conversation_store: SqlAlchemyConversationStore,
    runner_id: str,
    owner: str,
    server_connection_id: str,
    superseded: bool,
) -> None:
    root = _new_root(conversation_store)
    expected_connection = _server_connection_id("wrong-context")
    attempt_id = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, expected_connection
    )

    with pytest.raises(ValueError, match=r"superseded|does not match"):
        conversation_store.admit_acp_report_connection(
            attempt_id,
            _connection_context(
                server_connection_id,
                runner_id=runner_id,
                owner=owner,
                superseded=superseded,
            ),
        )


def test_connection_attempt_allows_current_cross_runner_principal_rebind_and_fences_old_attempt(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    first_connection = _server_connection_id("server-connection-before-rebind")
    first_attempt = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, first_connection
    )
    assert (
        conversation_store.admit_acp_report_connection(
            first_attempt, _connection_context(first_connection)
        )["current"]
        is True
    )

    stale_connection = _server_connection_id("server-connection-stale-before-rebind")
    stale_attempt = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, stale_connection
    )

    rebound_runner_id = "runner-acp-store-rebound"
    rebound_principal = "rebound-owner@example.test"
    conversation_store.replace_runner_id(root.id, rebound_runner_id, expected_runner_id=_RUNNER_ID)
    rebound_connection = _server_connection_id("server-connection-after-rebind")
    rebound_attempt = conversation_store.prepare_acp_report_connection_attempt(
        root.id,
        rebound_runner_id,
        rebound_principal,
        rebound_connection,
    )
    admission = conversation_store.admit_acp_report_connection(
        rebound_attempt,
        _connection_context(
            rebound_connection,
            runner_id=rebound_runner_id,
            owner=rebound_principal,
        ),
    )
    assert admission == {
        "connection_admission_id": rebound_attempt,
        "current": True,
        "selector_changed": False,
    }

    with pytest.raises(ValueError, match="predecessor revision changed"):
        conversation_store.admit_acp_report_connection(
            stale_attempt,
            _connection_context(stale_connection),
        )


def test_connection_attempt_rejects_same_runner_principal_change(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    first_connection = _server_connection_id("server-connection-principal-bound")
    attempt_id = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, first_connection
    )
    conversation_store.admit_acp_report_connection(
        attempt_id, _connection_context(first_connection)
    )

    with pytest.raises(ValueError, match="principal binding changed"):
        conversation_store.prepare_acp_report_connection_attempt(
            root.id,
            _RUNNER_ID,
            "different-owner@example.test",
            _server_connection_id("server-connection-changed-principal"),
        )


def test_never_admitted_runtime_incarnation_cannot_reclaim_current_journal(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    server_connection_id = _server_connection_id(f"connection:{root.id}")
    attempt_id = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, server_connection_id
    )
    conversation_store.admit_acp_report_connection(
        attempt_id, _connection_context(server_connection_id)
    )
    journal_one = hashlib.sha256(b"journal-one").hexdigest()[:32]
    journal_two = hashlib.sha256(b"journal-two").hexdigest()[:32]
    conversation_store.admit_acp_report_owner(root.id, attempt_id, 2, journal_two)

    with pytest.raises(ValueError, match="runtime incarnation is stale"):
        conversation_store.admit_acp_report_owner(root.id, attempt_id, 1, journal_one)


def _apply(
    store: SqlAlchemyConversationStore,
    *,
    root_id: str,
    sequence: int,
    target: str,
    parent: str | None,
    conversation_id: str,
    kind: str,
    operation: dict[str, object],
    journal_sequence: int | None = None,
    native_epoch: str = _EPOCH,
    root_native_session_id: str = _ROOT_NATIVE,
    context_generation: str = _CONTEXT,
    epoch_ordinal: int = 1,
    activate_epoch: bool = True,
    connection_admission_id: str | None = None,
    runtime_incarnation: int = 1,
    journal_id: str | None = None,
    action_origin: dict[str, object] | None = None,
) -> dict[str, object]:
    if connection_admission_id is None:
        if journal_id is not None:
            raise ValueError("journal_id requires an explicit connection admission")
        connection_admission_id, journal_id, _server_connection_id = _report_owner(store, root_id)
    elif journal_id is None:
        raise ValueError("an explicit connection admission requires its journal_id")
    if activate_epoch:
        store.activate_acp_report_epoch(
            root_id,
            connection_admission_id,
            runtime_incarnation,
            journal_id,
            native_epoch,
            epoch_ordinal,
        )
    source_id = acp_standard_report_source_id(_SERVER, root_id, native_epoch, sequence)
    payload_hash = hashlib.sha256(
        json.dumps(
            {
                "sequence": sequence,
                "target": target,
                "parent": parent,
                "kind": kind,
                "operation": operation,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return store.apply_acp_session_report(
        root_conversation_id=root_id,
        connection_admission_id=connection_admission_id,
        runtime_incarnation=runtime_incarnation,
        journal_id=journal_id,
        journal_sequence=sequence if journal_sequence is None else journal_sequence,
        epoch_ordinal=epoch_ordinal,
        native_epoch=native_epoch,
        ingress_sequence=sequence,
        source_id=source_id,
        payload_hash=payload_hash,
        context_generation=context_generation,
        root_native_session_id=root_native_session_id,
        target_session_id=target,
        parent_session_id=parent,
        conversation_id=conversation_id,
        kind=kind,
        operation=operation,
        action_origin=action_origin,
    )


def _tool_scope(
    root_id: str,
    *,
    call_id: str,
    anchor_sequence: int,
    response_id: str,
    token: str | None = None,
    generation: str | None = None,
) -> tuple[AcpToolCallAppendScope, dict[str, str]]:
    action_origin_token = (
        token or hashlib.sha256(f"{root_id}:{call_id}:origin".encode()).hexdigest()[:32]
    )
    action_origin_generation = generation or f"action-generation-{call_id}"
    source_id = acp_standard_report_source_id(_SERVER, root_id, _EPOCH, anchor_sequence)
    scope = AcpToolCallAppendScope(
        source_id=source_id,
        native_call_id=call_id,
        action_origin_token=action_origin_token,
        action_origin_generation=action_origin_generation,
        response_id=response_id,
    )
    action_origin = {
        "token": action_origin_token,
        "agent": "test-agent",
        "generation": action_origin_generation,
    }
    return scope, action_origin


def _anchor_root_tool(
    store: SqlAlchemyConversationStore,
    *,
    root_id: str,
    call_id: str,
    sequence: int,
    response_id: str,
    token: str | None = None,
    generation: str | None = None,
) -> tuple[AcpToolCallAppendScope, dict[str, str]]:
    scope, action_origin = _tool_scope(
        root_id,
        call_id=call_id,
        anchor_sequence=sequence,
        response_id=response_id,
        token=token,
        generation=generation,
    )
    result = _apply(
        store,
        root_id=root_id,
        sequence=sequence,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root_id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "response_id": response_id,
        },
        action_origin=action_origin,
    )
    assert result["duplicate"] is False
    return scope, action_origin


def _root_tool_upsert(
    *,
    call_id: str,
    response_id: str,
    item_type: str,
    item_data: dict[str, object],
) -> dict[str, object]:
    entity_id = (
        acp_standard_tool_entity_id(call_id)
        if item_type == "function_call"
        else acp_standard_tool_output_entity_id(call_id)
    )
    return {
        "entity_type": "item",
        "entity_id": entity_id,
        "call_id": call_id,
        "value": {
            "item_type": item_type,
            "response_id": response_id,
            "item_data": item_data,
        },
    }


def _canonical_call_item(
    call_id: str,
    response_id: str,
    *,
    name: str = "generic_tool",
    arguments: str = "{}",
) -> NewConversationItem:
    return NewConversationItem(
        type="function_call",
        response_id=response_id,
        data=FunctionCallData(
            acp_agent_identity="unknown",
            call_id=call_id,
            name=name,
            arguments=arguments,
            tool_status="completed",
        ),
    )


def _canonical_output_item(
    call_id: str,
    response_id: str,
    *,
    output: str | None = "generic result",
) -> NewConversationItem:
    return NewConversationItem(
        type="function_call_output",
        response_id=response_id,
        data=FunctionCallOutputData(
            acp_agent_identity="unknown",
            call_id=call_id,
            output=output,
        ),
    )


def _insert_duplicate_physical_item(
    store: SqlAlchemyConversationStore,
    *,
    conversation_id: str,
    item_id: str,
    created_at: int,
) -> None:
    """Insert the same logical item key under a second physical timestamp key."""
    with store._conv_session_immediate("test_duplicate_physical_item") as session:
        item_row = session.get(
            SqlConversationItem,
            (current_workspace_id(), conversation_id, item_id, created_at),
        )
        assert item_row is not None
        session.add(
            SqlConversationItem(
                workspace_id=item_row.workspace_id,
                conversation_id=item_row.conversation_id,
                id=item_row.id,
                response_id=item_row.response_id,
                created_at=item_row.created_at + 1,
                status=item_row.status,
                position=item_row.position + 1,
                type=item_row.type,
                data=item_row.data,
                search_text=item_row.search_text,
                created_by=item_row.created_by,
            )
        )
        rows = (
            session.execute(
                select(SqlConversationItem).where(
                    SqlConversationItem.workspace_id == current_workspace_id(),
                    SqlConversationItem.conversation_id == conversation_id,
                    SqlConversationItem.id == item_id,
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 2


def test_surviving_harness_reconnect_reuses_journal_epoch_and_report_origin(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    journal_id = hashlib.sha256(root.id.encode()).hexdigest()[:32]
    first_connection_id = _server_connection_id(f"first:{root.id}")
    first_attempt = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, first_connection_id
    )
    conversation_store.admit_acp_report_connection(
        first_attempt, _connection_context(first_connection_id)
    )
    conversation_store.admit_acp_report_owner(root.id, first_attempt, 1, journal_id)
    conversation_store.activate_acp_report_epoch(root.id, first_attempt, 1, journal_id, _EPOCH, 1)

    operation = {"observation": {"usage": {"tokens": 5}}}
    source_id = acp_standard_report_source_id(_SERVER, root.id, _EPOCH, 1)
    payload_hash = hashlib.sha256(
        json.dumps(operation, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    first = conversation_store.apply_acp_session_report(
        root_conversation_id=root.id,
        connection_admission_id=first_attempt,
        runtime_incarnation=1,
        journal_id=journal_id,
        journal_sequence=1,
        epoch_ordinal=1,
        native_epoch=_EPOCH,
        ingress_sequence=1,
        source_id=source_id,
        payload_hash=payload_hash,
        context_generation=_CONTEXT,
        root_native_session_id=_ROOT_NATIVE,
        target_session_id=_ROOT_NATIVE,
        parent_session_id=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation=operation,
    )
    assert first["duplicate"] is False

    second_connection_id = _server_connection_id(f"reconnected:{root.id}")
    second_attempt = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, second_connection_id
    )
    conversation_store.admit_acp_report_connection(
        second_attempt, _connection_context(second_connection_id)
    )
    conversation_store.admit_acp_report_owner(root.id, second_attempt, 1, journal_id)
    conversation_store.activate_acp_report_epoch(root.id, second_attempt, 1, journal_id, _EPOCH, 1)
    replay = conversation_store.apply_acp_session_report(
        root_conversation_id=root.id,
        connection_admission_id=second_attempt,
        runtime_incarnation=1,
        journal_id=journal_id,
        journal_sequence=1,
        epoch_ordinal=1,
        native_epoch=_EPOCH,
        ingress_sequence=1,
        source_id=source_id,
        payload_hash=payload_hash,
        context_generation=_CONTEXT,
        root_native_session_id=_ROOT_NATIVE,
        target_session_id=_ROOT_NATIVE,
        parent_session_id=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation=operation,
    )

    assert replay["duplicate"] is True
    assert conversation_store.get_acp_session_observation(root.id)["usage"] == {"tokens": 5}


def test_replacement_journal_cannot_reuse_native_epoch_or_select_old_history(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:old-journal"
    )
    child_native_id = "native-child-old-journal"
    call_id = "root-tool-with-old-journal-reference"
    operation = {"observation": {"usage": {"tokens": 41}}}

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation=operation,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "references": [child_native_id],
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": child_native_id,
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker", "description": "old journal"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation={
            "entity_type": "message",
            "entity_id": "old-journal-child-history",
            "value": {
                "item_type": "message",
                "response_id": "old-journal-child-response",
                "item_data": {
                    "role": "assistant",
                    "agent": "unknown",
                    "acp_agent_identity": "unknown",
                    "content": [{"type": "text", "text": "Old child history."}],
                },
            },
        },
    )

    second_connection_id = _server_connection_id(f"replacement:{root.id}")
    second_attempt = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _PRINCIPAL, second_connection_id
    )
    conversation_store.admit_acp_report_connection(
        second_attempt, _connection_context(second_connection_id)
    )
    journal_two = hashlib.sha256(f"replacement:{root.id}".encode()).hexdigest()[:32]
    conversation_store.admit_acp_report_owner(root.id, second_attempt, 2, journal_two)

    with pytest.raises(ValueError, match=r"journal|epoch"):
        conversation_store.activate_acp_report_epoch(
            root.id, second_attempt, 2, journal_two, _EPOCH, 1
        )

    assert conversation_store.get_acp_session_observation(root.id) is None
    appended = conversation_store.append(
        root.id,
        [
            NewConversationItem(
                type="function_call",
                response_id="replacement-tool-response",
                data=FunctionCallData(
                    name="inspect_child",
                    arguments="{}",
                    call_id=call_id,
                    acp_agent_identity="unknown",
                ),
            )
        ],
    )
    assert appended[0].data.acp_session_references is None

    old_child = conversation_store.get_acp_session_association(root.id, _EPOCH, child_native_id)
    assert old_child is not None
    assert old_child["association"]["description"] == "old journal"
    items = conversation_store.list_items(child.id)
    assert len(items.data) == 1
    assert items.data[0].data.content[0]["text"] == "Old child history."


def test_first_delayed_report_from_retired_epoch_never_selects_root_or_pending_references(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )
    epoch_a = "native-epoch-never-reported-a"
    epoch_b = "native-epoch-current-empty-b"
    root_native_a = "native-root-a"
    root_native_b = "native-root-b"
    child_native_a = "native-child-never-reported-a"
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:retired-epoch"
    )

    # A exists in the client journal, but the server has not learned its
    # binding before B becomes current. A's first reports arrive historically.
    conversation_store.activate_acp_report_epoch(
        root.id, connection_admission_id, 1, journal_id, epoch_b, 2
    )
    selected_before_history = _report_selection_state(conversation_store, root.id, journal_id)
    assert selected_before_history == (journal_id, 2, epoch_b, 2)
    assert conversation_store.get_acp_session_observation(root.id) is None

    historical_binding = conversation_store.activate_acp_report_epoch(
        root.id,
        connection_admission_id,
        1,
        journal_id,
        epoch_a,
        1,
        historical_only=True,
    )
    assert historical_binding == {
        "bound": True,
        "selected": False,
        "selector_changed": False,
    }
    assert (
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            epoch_a,
            1,
            historical_only=True,
        )
        == historical_binding
    )
    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        selected_before_history
    )

    with pytest.raises(ValueError, match="ordinal was reused"):
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            "native-epoch-conflicts-with-a",
            1,
            historical_only=True,
        )
    with pytest.raises(ValueError, match="rebound to another ordinal"):
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            epoch_a,
            3,
            historical_only=True,
        )

    call_id = "root-tool-with-delayed-session-references"
    tool_references = {
        "entity_type": "item",
        "entity_id": acp_standard_tool_entity_id(call_id),
        "call_id": call_id,
        "references": [child_native_a],
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=root_native_a,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation=tool_references,
        journal_sequence=1,
        native_epoch=epoch_a,
        root_native_session_id=root_native_a,
        epoch_ordinal=1,
        activate_epoch=False,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=child_native_a,
        parent=root_native_a,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": child_native_a,
            "parent_session_id": root_native_a,
            "patch": {"title": "retired worker"},
        },
        journal_sequence=2,
        native_epoch=epoch_a,
        root_native_session_id=root_native_a,
        epoch_ordinal=1,
        activate_epoch=False,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=child_native_a,
        parent=root_native_a,
        conversation_id=child.id,
        kind="entity.upsert",
        operation={
            "entity_type": "message",
            "entity_id": "retired-child-history",
            "value": {
                "item_type": "message",
                "response_id": "retired-child-response",
                "item_data": {
                    "role": "assistant",
                    "agent": "unknown",
                    "acp_agent_identity": "unknown",
                    "content": [{"type": "text", "text": "Readable retired history."}],
                },
            },
        },
        journal_sequence=3,
        native_epoch=epoch_a,
        root_native_session_id=root_native_a,
        epoch_ordinal=1,
        activate_epoch=False,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target=root_native_a,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"usage": {"tokens": 99}}},
        journal_sequence=4,
        native_epoch=epoch_a,
        root_native_session_id=root_native_a,
        epoch_ordinal=1,
        activate_epoch=False,
    )

    old_root = conversation_store.get_acp_session_association(root.id, epoch_a, root_native_a)
    assert old_root is not None
    assert old_root["observation"]["usage"] == {"tokens": 99}
    assert conversation_store.get_acp_session_observation(root.id) is None
    child_items = conversation_store.list_items(child.id).data
    assert len(child_items) == 1
    assert child_items[0].data.content[0]["text"] == "Readable retired history."

    appended = conversation_store.append(
        root.id,
        [
            NewConversationItem(
                type="function_call",
                response_id="root-tool-response",
                data=FunctionCallData(
                    name="inspect_child",
                    arguments="{}",
                    call_id=call_id,
                    acp_agent_identity="unknown",
                ),
            )
        ],
    )
    assert len(appended) == 1
    assert appended[0].data.acp_session_references is None

    assert conversation_store.get_acp_session_association(root.id, epoch_b, root_native_b) is None


def test_live_epoch_activation_keeps_identity_and_high_water_guards(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )
    epoch_b = "native-epoch-live-high-water-guard"
    conversation_store.activate_acp_report_epoch(
        root.id, connection_admission_id, 1, journal_id, epoch_b, 2
    )

    with pytest.raises(ValueError, match="below the journal high-water"):
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            "native-epoch-live-too-old",
            1,
        )
    with pytest.raises(ValueError, match="ordinal was reused"):
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            "native-epoch-live-ordinal-conflict",
            2,
        )
    with pytest.raises(ValueError, match="rebound to another ordinal"):
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            epoch_b,
            3,
        )


def test_historical_epoch_at_equal_high_water_stays_unselected_after_same_journal_reconnect(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )
    epoch_a = "native-epoch-equal-high-water"
    conversation_store.activate_acp_report_epoch(
        root.id, connection_admission_id, 1, journal_id, epoch_a, 1
    )
    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        journal_id,
        1,
        epoch_a,
        1,
    )

    reconnected_admission_id = _reconnect_report_owner(conversation_store, root.id, journal_id)
    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        journal_id,
        None,
        None,
        1,
    )

    binding = conversation_store.activate_acp_report_epoch(
        root.id,
        reconnected_admission_id,
        1,
        journal_id,
        epoch_a,
        1,
        historical_only=True,
    )
    assert binding == {"bound": True, "selected": False, "selector_changed": False}
    assert (
        conversation_store.activate_acp_report_epoch(
            root.id,
            reconnected_admission_id,
            1,
            journal_id,
            epoch_a,
            1,
            historical_only=True,
        )
        == binding
    )
    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        journal_id,
        None,
        None,
        1,
    )
    assert conversation_store.get_acp_session_observation(root.id) is None


def test_first_historical_epoch_does_not_advance_zero_high_water_or_select_root(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )
    epoch_a = "native-epoch-first-historical"

    binding = conversation_store.activate_acp_report_epoch(
        root.id,
        connection_admission_id,
        1,
        journal_id,
        epoch_a,
        1,
        historical_only=True,
    )
    assert binding == {"bound": True, "selected": False, "selector_changed": False}
    assert (
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            epoch_a,
            1,
            historical_only=True,
        )
        == binding
    )
    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        journal_id,
        None,
        None,
        0,
    )
    assert conversation_store.get_acp_session_observation(root.id) is None


def test_above_high_water_historical_epoch_keeps_live_observation_and_watermark(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )
    epoch_b = "native-epoch-live-before-future-history"
    epoch_a = "native-epoch-future-history"
    conversation_store.activate_acp_report_epoch(
        root.id, connection_admission_id, 1, journal_id, epoch_b, 2
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target="native-root-live-before-future-history",
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"usage": {"tokens": 12}}},
        native_epoch=epoch_b,
        root_native_session_id="native-root-live-before-future-history",
        epoch_ordinal=2,
        activate_epoch=False,
        connection_admission_id=connection_admission_id,
        journal_id=journal_id,
    )

    binding = conversation_store.activate_acp_report_epoch(
        root.id,
        connection_admission_id,
        1,
        journal_id,
        epoch_a,
        3,
        historical_only=True,
    )
    assert binding == {"bound": True, "selected": False, "selector_changed": False}
    assert (
        conversation_store.activate_acp_report_epoch(
            root.id,
            connection_admission_id,
            1,
            journal_id,
            epoch_a,
            3,
            historical_only=True,
        )
        == binding
    )
    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        journal_id,
        2,
        epoch_b,
        2,
    )

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        journal_sequence=2,
        target="native-root-future-history",
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"usage": {"tokens": 99}}},
        native_epoch=epoch_a,
        root_native_session_id="native-root-future-history",
        epoch_ordinal=3,
        activate_epoch=False,
        connection_admission_id=connection_admission_id,
        journal_id=journal_id,
    )

    assert _report_selection_state(conversation_store, root.id, journal_id) == (
        journal_id,
        2,
        epoch_b,
        2,
    )
    assert conversation_store.get_acp_session_observation(root.id)["usage"] == {"tokens": 12}
    historical_root = conversation_store.get_acp_session_association(
        root.id, epoch_a, "native-root-future-history"
    )
    assert historical_root is not None
    assert historical_root["observation"]["usage"] == {"tokens": 99}


def test_nested_associations_keep_native_and_omnigent_identity_separate(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    first_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:first"
    )
    nested_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=first_child.id, title="worker:nested"
    )

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"state": "idle"}},
    )
    first = _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-worker-one",
        parent=_ROOT_NATIVE,
        conversation_id=first_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-worker-one",
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker", "cwd": "/workspace/one"},
        },
    )
    nested = _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="native-worker-nested",
        parent="native-worker-one",
        conversation_id=nested_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-worker-nested",
            "parent_session_id": "native-worker-one",
            "patch": {"title": "worker", "cwd": "/workspace/one/nested"},
        },
    )

    first_assoc = conversation_store.get_acp_session_association(
        root.id, _EPOCH, "native-worker-one"
    )
    nested_assoc = conversation_store.get_acp_session_association(
        root.id, _EPOCH, "native-worker-nested"
    )
    assert first_assoc is not None and nested_assoc is not None
    assert first["association"]["conversation_id"] == first_child.id
    assert nested["association"]["conversation_id"] == nested_child.id
    assert first_assoc["parent_native_session_id"] == _ROOT_NATIVE
    assert nested_assoc["parent_native_session_id"] == "native-worker-one"
    assert first_assoc["conversation_id"] != "native-worker-one"
    assert nested_assoc["conversation_id"] != "native-worker-nested"


def test_sibling_acp_owners_under_one_conversation_keep_independent_report_roots(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    global_parent = conversation_store.create_conversation()
    owner_a = conversation_store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=global_parent.id,
        runner_id=_RUNNER_ID,
        title="worker:acp-owner-a",
    )
    owner_b = conversation_store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=global_parent.id,
        runner_id=_RUNNER_ID,
        title="worker:acp-owner-b",
    )
    child_a = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=owner_a.id, title="worker:a-child"
    )
    child_b = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=owner_b.id, title="worker:b-child"
    )

    for index, owner, child, epoch, root_native, child_native in (
        (
            1,
            owner_a,
            child_a,
            "native-epoch-owner-a",
            "native-root-owner-a",
            "native-child-owner-a",
        ),
        (
            2,
            owner_b,
            child_b,
            "native-epoch-owner-b",
            "native-root-owner-b",
            "native-child-owner-b",
        ),
    ):
        _apply(
            conversation_store,
            root_id=owner.id,
            sequence=1,
            target=root_native,
            parent=None,
            conversation_id=owner.id,
            kind="observation.patch",
            operation={"observation": {"usage": {"tokens": index}}},
            native_epoch=epoch,
            root_native_session_id=root_native,
        )
        _apply(
            conversation_store,
            root_id=owner.id,
            sequence=2,
            target=child_native,
            parent=root_native,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": child_native,
                "parent_session_id": root_native,
                "patch": {"title": f"worker {index}"},
            },
            native_epoch=epoch,
            root_native_session_id=root_native,
        )

    assert conversation_store.get_acp_session_observation(owner_a.id)["usage"] == {"tokens": 1}
    assert conversation_store.get_acp_session_observation(owner_b.id)["usage"] == {"tokens": 2}
    assert conversation_store.get_acp_session_observation(global_parent.id) is None
    assert (
        conversation_store.get_acp_session_association(
            owner_a.id, "native-epoch-owner-a", "native-child-owner-a"
        )["conversation_id"]
        == child_a.id
    )
    assert (
        conversation_store.get_acp_session_association(
            owner_b.id, "native-epoch-owner-b", "native-child-owner-b"
        )["conversation_id"]
        == child_b.id
    )
    assert (
        conversation_store.get_conversation(owner_a.id).parent_conversation_id == global_parent.id
    )
    assert conversation_store.get_conversation(child_a.id).parent_conversation_id == owner_a.id
    assert (
        conversation_store.get_conversation(owner_b.id).parent_conversation_id == global_parent.id
    )
    assert conversation_store.get_conversation(child_b.id).parent_conversation_id == owner_b.id


def test_entity_ids_are_scoped_by_native_session_and_replay_returns_snapshot(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:entity"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-child-entity",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": "native-child-entity",
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
    )
    operation = {
        "entity_type": "message",
        "entity_id": "shared-native-message-id",
        "value": {
            "item_type": "message",
            "response_id": "response-child",
            "item_data": {
                "role": "assistant",
                "agent": "unknown",
                "content": [{"type": "text", "text": "child result"}],
                "acp_agent_identity": "unknown",
            },
        },
    }
    first = _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="native-child-entity",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation=operation,
    )
    replay = _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="native-child-entity",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation=operation,
    )

    assert first["duplicate"] is False
    assert replay["duplicate"] is True
    assert replay["item"].data.content[0]["text"] == "child result"
    items = conversation_store.list_items(child.id)
    assert len(items.data) == 1
    assert items.data[0].id == replay["item"].id


def test_report_order_rejects_root_rebinding_and_conflicting_replay(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"state": "idle"}},
    )
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )

    with pytest.raises(ValueError, match="root native session identity changed"):
        conversation_store.validate_acp_session_report_order(
            root_conversation_id=root.id,
            connection_admission_id=connection_admission_id,
            runtime_incarnation=1,
            journal_id=journal_id,
            journal_sequence=2,
            epoch_ordinal=1,
            native_epoch=_EPOCH,
            ingress_sequence=2,
            source_id=acp_standard_report_source_id(_SERVER, root.id, _EPOCH, 2),
            payload_hash=hashlib.sha256(b"unused report").hexdigest(),
            root_native_session_id="different-native-root",
        )


def test_pending_association_reservation_accepts_exact_replay_for_recovery(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A retry can finish an association after the process dies between reserve and apply."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:pending-recovery"
    )
    target = "native-child-pending-recovery"
    source_id = acp_standard_report_source_id(_SERVER, root.id, _EPOCH, 1)
    operation = {
        "session_id": target,
        "parent_session_id": _ROOT_NATIVE,
        "patch": {"title": "worker"},
    }
    payload_hash = hashlib.sha256(
        json.dumps(operation, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    connection_admission_id, journal_id, _server_connection_id = _report_owner(
        conversation_store, root.id
    )
    conversation_store.activate_acp_report_epoch(
        root.id, connection_admission_id, 1, journal_id, _EPOCH, 1
    )
    reservation = {
        "root_conversation_id": root.id,
        "connection_admission_id": connection_admission_id,
        "runtime_incarnation": 1,
        "journal_id": journal_id,
        "journal_sequence": 1,
        "epoch_ordinal": 1,
        "native_epoch": _EPOCH,
        "ingress_sequence": 1,
        "source_id": source_id,
        "payload_hash": payload_hash,
        "context_generation": _CONTEXT,
        "root_native_session_id": _ROOT_NATIVE,
        "target_session_id": target,
        "parent_session_id": _ROOT_NATIVE,
    }

    conversation_store.reserve_acp_session_report(**reservation)
    # This is an exact retry of a durable receipt whose application was interrupted.
    conversation_store.reserve_acp_session_report(**reservation)
    result = conversation_store.apply_acp_session_report(
        **reservation,
        conversation_id=child.id,
        kind="association.patch",
        operation=operation,
    )

    association = conversation_store.get_acp_session_association(root.id, _EPOCH, target)
    assert result["duplicate"] is False
    assert association is not None
    assert association["conversation_id"] == child.id
    assert association["association_source_id"] == source_id


def test_message_patch_materializes_a_directed_unknown_agent_transcript_item(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    sender = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:sender"
    )
    recipient = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:recipient"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    for sequence, native_id, child in (
        (2, "native-sender", sender),
        (3, "native-recipient", recipient),
    ):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=sequence,
            target=native_id,
            parent=_ROOT_NATIVE,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": native_id,
                "parent_session_id": _ROOT_NATIVE,
                "patch": {"title": "worker"},
            },
        )

    result = _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target="native-sender",
        parent=_ROOT_NATIVE,
        conversation_id=sender.id,
        kind="entity.patch",
        operation={
            "entity_type": "message",
            "entity_id": "directed-message",
            "patch": {
                "role": "assistant",
                "agent": "unknown",
                "acp_agent_identity": "unknown",
                "content": [{"type": "text", "text": "Please review this."}],
                "acp_session_message": {
                    "kind": "directed",
                    "sender_session_id": "native-sender",
                    "recipient_session_id": "native-recipient",
                    "sender_message_id": "message-sender",
                    "recipient_message_id": "message-recipient",
                },
            },
        },
    )

    assert result["item"] is not None
    items = conversation_store.list_items(sender.id).data
    assert len(items) == 1
    assert items[0].data.agent == "unknown"
    assert items[0].data.user_authored is False
    assert items[0].data.acp_session_message.sender_session_id == "native-sender"
    assert items[0].data.acp_session_message.recipient_session_id == "native-recipient"


def test_sparse_directed_message_patch_preserves_unmentioned_fields(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    sender = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:directed-patches"
    )
    recipient = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:directed-recipient"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    for sequence, native_id, child in (
        (2, "native-sender", sender),
        (3, "native-recipient", recipient),
    ):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=sequence,
            target=native_id,
            parent=_ROOT_NATIVE,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": native_id,
                "parent_session_id": _ROOT_NATIVE,
                "patch": {"title": "worker"},
            },
        )

    initial_content = [{"type": "text", "text": "Original message."}]
    initial_metadata = {"source": "original", "retained": True}
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target="native-sender",
        parent=_ROOT_NATIVE,
        conversation_id=sender.id,
        kind="entity.upsert",
        operation={
            "entity_type": "message",
            "entity_id": "directed-message-patches",
            "value": {
                "item_type": "message",
                "response_id": "response-directed-message-patches",
                "item_data": {
                    "role": "assistant",
                    "agent": "unknown",
                    "acp_agent_identity": "unknown",
                    "content": initial_content,
                    "metadata": initial_metadata,
                    "acp_session_message": {
                        "kind": "directed",
                        "sender_session_id": "native-sender",
                        "recipient_session_id": "native-recipient",
                        "sender_message_id": "sender-message-one",
                        "recipient_message_id": "recipient-message-one",
                    },
                },
            },
        },
    )

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=5,
        target="native-sender",
        parent=_ROOT_NATIVE,
        conversation_id=sender.id,
        kind="entity.patch",
        operation={
            "entity_type": "message",
            "entity_id": "directed-message-patches",
            "patch": {
                "acp_session_message": {
                    "kind": "directed",
                    "recipient_message_id": "recipient-message-two",
                }
            },
        },
    )
    sparse = conversation_store.list_items(sender.id).data[0].data
    assert sparse.content == initial_content
    assert sparse.metadata == initial_metadata
    assert sparse.acp_session_message.sender_session_id == "native-sender"
    assert sparse.acp_session_message.recipient_session_id == "native-recipient"
    assert sparse.acp_session_message.sender_message_id == "sender-message-one"
    assert sparse.acp_session_message.recipient_message_id == "recipient-message-two"

    replacement_operation = {
        "entity_type": "message",
        "entity_id": "directed-message-patches",
        "patch": {
            "content": [{"type": "text", "text": "Replacement message."}],
            "metadata": {"source": "replacement"},
            "acp_session_message": {
                "kind": "directed",
                "recipient_message_id": None,
            },
        },
    }
    result = _apply(
        conversation_store,
        root_id=root.id,
        sequence=6,
        target="native-sender",
        parent=_ROOT_NATIVE,
        conversation_id=sender.id,
        kind="entity.patch",
        operation=replacement_operation,
    )
    replay = _apply(
        conversation_store,
        root_id=root.id,
        sequence=6,
        target="native-sender",
        parent=_ROOT_NATIVE,
        conversation_id=sender.id,
        kind="entity.patch",
        operation=replacement_operation,
    )

    assert result["duplicate"] is False
    assert result["item"] is not None
    assert result["item"].data.content == [{"type": "text", "text": "Replacement message."}]
    assert replay["duplicate"] is True
    assert replay["item"].id == result["item"].id
    assert replay["item"].data.content == [{"type": "text", "text": "Replacement message."}]

    updated_items = conversation_store.list_items(sender.id).data
    assert len(updated_items) == 1
    assert updated_items[0].id == replay["item"].id
    updated = updated_items[0].data
    assert updated.content == [{"type": "text", "text": "Replacement message."}]
    assert updated.metadata == {"source": "replacement"}
    assert updated.acp_session_message.sender_session_id == "native-sender"
    assert updated.acp_session_message.recipient_session_id == "native-recipient"
    assert updated.acp_session_message.sender_message_id == "sender-message-one"
    assert updated.acp_session_message.recipient_message_id is None


def test_generated_tool_output_id_cannot_alias_a_later_opaque_tool_id(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A generated output identity and a later opaque tool id remain distinct items."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:identity-collision"
    )
    executor = AcpExecutor(AcpAgentConfig(command="unused"))
    native_child_id = "native-tool-output-collision"

    first_call = executor._standard_update_operations(
        native_child_id,
        {
            "sessionUpdate": "tool_call",
            "toolCallId": "x",
            "title": "first tool",
            "rawInput": {"value": 1},
        },
    )[0][1]
    output_update = executor._standard_update_operations(
        native_child_id,
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": "x",
            "status": "completed",
            "rawOutput": "first result",
        },
    )
    output_upsert = output_update[-1][1]
    generated_output_id = output_upsert["entity_id"]

    colliding_call = executor._standard_update_operations(
        native_child_id,
        {
            "sessionUpdate": "tool_call",
            "toolCallId": generated_output_id,
            "title": "second tool",
            "rawInput": {"value": 2},
        },
    )[0][1]
    assert output_upsert["entity_type"] == colliding_call["entity_type"]
    assert output_upsert["entity_id"] != colliding_call["entity_id"]
    assert colliding_call["value"]["item_data"]["call_id"] == generated_output_id

    first_update = executor._standard_update_operations(
        native_child_id,
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": "x",
            "status": "completed",
            "rawOutput": "updated first result",
        },
    )
    second_update = executor._standard_update_operations(
        native_child_id,
        {
            "sessionUpdate": "tool_call_update",
            "toolCallId": generated_output_id,
            "status": "completed",
            "rawOutput": "second result",
        },
    )

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=native_child_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": native_child_id,
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=native_child_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation=first_call,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target=native_child_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.patch",
        operation=output_update[0][1],
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=5,
        target=native_child_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation=output_upsert,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=6,
        target=native_child_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation=colliding_call,
    )

    next_sequence = 7
    for operation_kind, operation in first_update:
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=next_sequence,
            target=native_child_id,
            parent=_ROOT_NATIVE,
            conversation_id=child.id,
            kind=operation_kind,
            operation=operation,
        )
        next_sequence += 1
    for operation_kind, operation in second_update:
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=next_sequence,
            target=native_child_id,
            parent=_ROOT_NATIVE,
            conversation_id=child.id,
            kind=operation_kind,
            operation=operation,
        )
        next_sequence += 1

    # A retry of both final updates is replayed from the durable report identity
    # and must not add or overwrite either call/output pair.
    for sequence, operation_list in ((7, first_update), (9, second_update)):
        for offset, (operation_kind, operation) in enumerate(operation_list):
            _apply(
                conversation_store,
                root_id=root.id,
                sequence=sequence + offset,
                target=native_child_id,
                parent=_ROOT_NATIVE,
                conversation_id=child.id,
                kind=operation_kind,
                operation=operation,
            )

    items = conversation_store.list_items(child.id).data
    item_pairs = {(item.type, item.data.call_id) for item in items}
    assert item_pairs == {
        ("function_call", "x"),
        ("function_call_output", "x"),
        ("function_call", generated_output_id),
        ("function_call_output", generated_output_id),
    }
    output_by_call_id = {
        item.data.call_id: item.data.output
        for item in items
        if item.type == "function_call_output"
    }
    assert output_by_call_id == {
        "x": "updated first result",
        generated_output_id: "second result",
    }


def test_near_limit_multibyte_native_and_entity_ids_fit_bounded_store_keys(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:wide-ids"
    )
    native_epoch = "📨" * 128
    root_native = "🧭" * 128
    child_native = "📍" * 128
    entity_id = "📨" * 128
    assert len(native_epoch) == len(root_native) == len(child_native) == len(entity_id) == 128
    assert all(
        len(value.encode("utf-8")) == 512
        for value in (native_epoch, root_native, child_native, entity_id)
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=root_native,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
        native_epoch=native_epoch,
        root_native_session_id=root_native,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=child_native,
        parent=root_native,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": child_native,
            "parent_session_id": root_native,
            "patch": {"title": "wide"},
        },
        native_epoch=native_epoch,
        root_native_session_id=root_native,
    )
    result = _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=child_native,
        parent=root_native,
        conversation_id=child.id,
        kind="entity.patch",
        operation={
            "entity_type": "message",
            "entity_id": entity_id,
            "patch": {
                "role": "assistant",
                "agent": "unknown",
                "acp_agent_identity": "unknown",
                "content": [{"type": "text", "text": "wide id"}],
            },
        },
        native_epoch=native_epoch,
        root_native_session_id=root_native,
    )

    assert result["association"]["native_session_id"] == child_native
    assert (
        conversation_store.get_acp_session_association(root.id, native_epoch, child_native)[
            "conversation_id"
        ]
        == child.id
    )
    with conversation_store._conv_session(
        "test_near_limit_native_entity_key_round_trip"
    ) as session:
        entity = session.get(
            SqlAcpSessionEntity,
            (
                current_workspace_id(),
                root.id,
                native_epoch,
                child_native,
                "message",
                entity_id,
            ),
        )
    assert entity is not None
    assert (entity.native_epoch, entity.native_session_id, entity.entity_id) == (
        native_epoch,
        child_native,
        entity_id,
    )


def test_opaque_native_identity_distinctions_round_trip_through_store(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """Native identity equality preserves case, accents, normalization, and spaces."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:opaque-identities"
    )
    native_epoch = "Epoch"
    root_native = "NativeRoot"
    session_ids = (
        "Tool",
        "tool",
        "cafe",
        "café",
        "é",
        "e\u0301",
        "native",
        "native ",
    )
    entity_ids = (
        "Message",
        "message",
        "cafe-message",
        "café-message",
        "é-message",
        "e\u0301-message",
        "trailing-message",
        "trailing-message ",
    )
    assert len(set(session_ids)) == len(session_ids)
    assert len(set(entity_ids)) == len(entity_ids)

    for sequence, native_session_id in enumerate(session_ids, start=1):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=sequence,
            target=native_session_id,
            parent=root_native,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": native_session_id,
                "parent_session_id": root_native,
                "patch": {"title": "opaque identity"},
            },
            native_epoch=native_epoch,
            root_native_session_id=root_native,
        )

    for sequence, (native_session_id, entity_id) in enumerate(
        zip(session_ids, entity_ids, strict=True), start=len(session_ids) + 1
    ):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=sequence,
            target=native_session_id,
            parent=root_native,
            conversation_id=child.id,
            kind="entity.upsert",
            operation={
                "entity_type": "message",
                "entity_id": entity_id,
                "value": {
                    "item_type": "message",
                    "response_id": f"response-{sequence}",
                    "item_data": {
                        "role": "assistant",
                        "agent": "unknown",
                        "acp_agent_identity": "unknown",
                        "content": [{"type": "text", "text": entity_id}],
                    },
                },
            },
            native_epoch=native_epoch,
            root_native_session_id=root_native,
        )

    assert [
        conversation_store.get_acp_session_association(root.id, native_epoch, native_session_id)[
            "native_session_id"
        ]
        for native_session_id in session_ids
    ] == list(session_ids)
    with conversation_store._conv_session("test_opaque_native_identity_round_trip") as session:
        entities = [
            session.get(
                SqlAcpSessionEntity,
                (
                    current_workspace_id(),
                    root.id,
                    native_epoch,
                    native_session_id,
                    "message",
                    entity_id,
                ),
            )
            for native_session_id, entity_id in zip(session_ids, entity_ids, strict=True)
        ]
    assert [entity.entity_id for entity in entities if entity is not None] == list(entity_ids)


@pytest.mark.parametrize("item_type", ["function_call", "function_call_output"])
def test_acp_materialized_item_lookups_reject_physical_ambiguity_for_patch_and_replay(
    conversation_store: SqlAlchemyConversationStore,
    item_type: str,
) -> None:
    """Canonical call/output updates and receipt replay reject duplicate physical items."""
    root = _new_root(conversation_store)
    call_id = f"ambiguous-materialized-{item_type}"
    response_id = f"response-ambiguous-materialized-{item_type}"
    scope, action_origin = _anchor_root_tool(
        conversation_store,
        root_id=root.id,
        call_id=call_id,
        sequence=1,
        response_id=response_id,
    )
    item_data = (
        {
            "acp_agent_identity": "unknown",
            "call_id": call_id,
            "name": "worker",
            "arguments": "{}",
            "tool_status": "completed",
        }
        if item_type == "function_call"
        else {
            "acp_agent_identity": "unknown",
            "call_id": call_id,
            "output": "initial output",
        }
    )
    upsert = _root_tool_upsert(
        call_id=call_id,
        response_id=response_id,
        item_type=item_type,
        item_data=item_data,
    )
    applied = _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.upsert",
        operation=upsert,
        action_origin=action_origin,
    )
    assert applied["duplicate"] is False
    item = applied["item"]
    assert item is not None
    assert item.id == acp_standard_canonical_tool_item_id(root.id, scope.source_id, item_type)
    assert item.type == item_type
    assert item.response_id == response_id
    _insert_duplicate_physical_item(
        conversation_store,
        conversation_id=root.id,
        item_id=item.id,
        created_at=item.created_at,
    )

    journal_id = hashlib.sha256(root.id.encode()).hexdigest()[:32]

    def state_snapshot() -> tuple[object, ...]:
        with conversation_store._conv_session("test_ambiguous_canonical_item_snapshot") as session:
            rows = (
                session.execute(
                    select(SqlConversationItem)
                    .where(
                        SqlConversationItem.workspace_id == current_workspace_id(),
                        SqlConversationItem.conversation_id == root.id,
                        SqlConversationItem.id == item.id,
                    )
                    .order_by(SqlConversationItem.created_at)
                )
                .scalars()
                .all()
            )
            entity = session.get(
                SqlAcpSessionEntity,
                (
                    current_workspace_id(),
                    root.id,
                    _EPOCH,
                    _ROOT_NATIVE,
                    "item",
                    upsert["entity_id"],
                ),
            )
            applied_receipt = session.get(
                SqlAcpSessionReportReceipt,
                (current_workspace_id(), root.id, journal_id, 2),
            )
            patch_receipt = session.get(
                SqlAcpSessionReportReceipt,
                (current_workspace_id(), root.id, journal_id, 3),
            )
            association = session.get(
                SqlAcpSessionAssociation,
                (current_workspace_id(), root.id, _EPOCH, _ROOT_NATIVE),
            )
            assert entity is not None
            assert applied_receipt is not None
            assert association is not None
            assert len(rows) == 2
            return (
                tuple(
                    (
                        row.created_at,
                        row.position,
                        row.response_id,
                        row.status,
                        row.type,
                        row.data,
                        row.search_text,
                        row.created_by,
                    )
                    for row in rows
                ),
                (
                    entity.item_type,
                    entity.response_id,
                    entity.item_data_json,
                    entity.pending_json,
                    entity.references_json,
                    entity.stable_item_id,
                    entity.last_ingress_sequence,
                ),
                (applied_receipt.applied, applied_receipt.payload_hash),
                patch_receipt is None,
                (association.last_ingress_sequence, association.last_reported_at),
            )

    before_state = state_snapshot()
    before_public_items = [
        value.model_dump(mode="json") for value in conversation_store.list_items(root.id).data
    ]
    assert len(before_public_items) == 2
    patch = (
        {"tool_status": "failed"}
        if item_type == "function_call"
        else {"output": "replacement output"}
    )
    entity_id = (
        acp_standard_tool_entity_id(call_id)
        if item_type == "function_call"
        else acp_standard_tool_output_entity_id(call_id)
    )
    patch_operation = {
        "entity_type": "item",
        "entity_id": entity_id,
        "call_id": call_id,
        "patch": patch,
    }
    with pytest.raises(ValueError):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=3,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="entity.patch",
            operation=patch_operation,
            action_origin=action_origin,
        )
    assert state_snapshot() == before_state
    assert [
        value.model_dump(mode="json") for value in conversation_store.list_items(root.id).data
    ] == before_public_items

    with pytest.raises(ValueError):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=2,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="entity.upsert",
            operation=upsert,
            action_origin=action_origin,
        )
    assert state_snapshot() == before_state
    assert [
        value.model_dump(mode="json") for value in conversation_store.list_items(root.id).data
    ] == before_public_items


@pytest.mark.parametrize("corruption", ["role", "call", "response", "status"])
def test_canonical_append_rejects_wrong_existing_rows_before_binding_proof(
    conversation_store: SqlAlchemyConversationStore,
    corruption: str,
) -> None:
    """A canonical stable-ID collision cannot establish append proof from bad data."""
    root = _new_root(conversation_store)
    call_id = f"canonical-existing-row-{corruption}"
    response_id = f"response-canonical-existing-row-{corruption}"
    scope, action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    canonical_id = acp_standard_canonical_tool_item_id(
        root.id, scope.source_id, "function_call_output"
    )
    if corruption == "role":
        stored_item = _canonical_call_item(call_id, response_id).model_copy(
            update={"stable_id": canonical_id}
        )
    elif corruption == "call":
        stored_item = _canonical_output_item(f"{call_id}-other", response_id).model_copy(
            update={"stable_id": canonical_id}
        )
    elif corruption == "response":
        stored_item = _canonical_output_item(call_id, f"{response_id}-other").model_copy(
            update={"stable_id": canonical_id}
        )
    else:
        stored_item = _canonical_output_item(call_id, response_id).model_copy(
            update={"stable_id": canonical_id, "status": "in_progress"}
        )

    seeded = conversation_store.append(root.id, [stored_item])[0]
    with conversation_store._conv_session("test_canonical_missing_output_entity") as session:
        assert (
            session.get(
                SqlAcpSessionEntity,
                (
                    current_workspace_id(),
                    root.id,
                    _EPOCH,
                    _ROOT_NATIVE,
                    "item",
                    acp_standard_tool_output_entity_id(call_id),
                ),
            )
            is None
        )
    anchor = _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "response_id": response_id,
        },
        action_origin=action_origin,
    )
    assert anchor["duplicate"] is False
    before = [item.model_dump(mode="json") for item in conversation_store.list_items(root.id).data]
    with pytest.raises(ValueError):
        conversation_store.append(
            root.id,
            [_canonical_output_item(call_id, response_id)],
            acp_tool_call_scope=scope,
        )

    after = [item.model_dump(mode="json") for item in conversation_store.list_items(root.id).data]
    assert after == before
    assert after[0]["id"] == seeded.id
    with conversation_store._conv_session("test_canonical_bad_duplicate_binding") as session:
        binding = session.get(
            SqlAcpToolCallItemBinding,
            (current_workspace_id(), root.id, scope.source_id),
        )
        assert binding is not None
        assert binding.canonical_item_id is None


def test_preanchor_canonical_append_rejects_wrong_existing_output_without_proof(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A preexisting unscoped row cannot seed append proof before its report anchor."""
    root = _new_root(conversation_store)
    call_id = "preanchor-canonical-output-collision"
    response_id = "response-preanchor-canonical-output-collision"
    scope, _action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    canonical_id = acp_standard_canonical_tool_item_id(
        root.id, scope.source_id, "function_call_output"
    )
    seeded = conversation_store.append(
        root.id,
        [
            _canonical_output_item("unrelated-preanchor-call", response_id).model_copy(
                update={"stable_id": canonical_id}
            )
        ],
    )[0]
    before = [item.model_dump(mode="json") for item in conversation_store.list_items(root.id).data]

    with pytest.raises(ValueError):
        conversation_store.append(
            root.id,
            [_canonical_output_item(call_id, response_id)],
            acp_tool_call_scope=scope,
        )

    after = [item.model_dump(mode="json") for item in conversation_store.list_items(root.id).data]
    assert after == before
    assert after[0]["id"] == seeded.id
    with conversation_store._conv_session("test_preanchor_canonical_output_proof") as session:
        binding = session.get(
            SqlAcpToolCallItemBinding,
            (current_workspace_id(), root.id, scope.source_id),
        )
        assert binding is None


def test_pending_canonical_binding_rejects_corrupt_item_without_promotion(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A corrupt persisted canonical row cannot be promoted by pending append proof."""
    root = _new_root(conversation_store)
    call_id = "pending-canonical-corrupt-item"
    response_id = "response-pending-canonical-corrupt-item"
    scope, _action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    canonical_call_id = acp_standard_canonical_tool_item_id(
        root.id, scope.source_id, "function_call"
    )
    appended_call = conversation_store.append(
        root.id,
        [_canonical_call_item(call_id, response_id)],
        acp_tool_call_scope=scope,
    )[0]
    assert appended_call.id == canonical_call_id
    with conversation_store._conv_session("test_pending_canonical_call_proof") as session:
        binding = session.get(
            SqlAcpToolCallItemBinding,
            (current_workspace_id(), root.id, scope.source_id),
        )
        assert binding is not None
        assert binding.native_epoch is None
        assert binding.canonical_item_id == canonical_call_id

    with conversation_store._conv_session_immediate(
        "test_corrupt_pending_canonical_item"
    ) as session:
        item_row = session.get(
            SqlConversationItem,
            (
                current_workspace_id(),
                root.id,
                appended_call.id,
                appended_call.created_at,
            ),
        )
        assert item_row is not None
        decoded = conversation_store._decode_item_data_batch([item_row.data])[0]
        corrupt_data = json.loads(decoded)
        corrupt_data["call_id"] = "corrupted-pending-call"
        item_row.data = conversation_store._encode_item_data(
            json.dumps(
                corrupt_data,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            )
        )

    def pending_binding_state() -> tuple[object, ...]:
        with conversation_store._conv_session("test_pending_canonical_binding_state") as session:
            row = session.get(
                SqlAcpToolCallItemBinding,
                (current_workspace_id(), root.id, scope.source_id),
            )
            assert row is not None
            return (
                row.native_epoch,
                row.journal_id,
                row.epoch_ordinal,
                row.context_generation,
                row.native_call_id,
                row.action_origin_token,
                row.action_origin_generation,
                row.conversation_id,
                row.canonical_item_id,
                row.response_id,
            )

    before_items = [
        item.model_dump(mode="json") for item in conversation_store.list_items(root.id).data
    ]
    before_binding = pending_binding_state()
    with pytest.raises(ValueError):
        conversation_store.append(
            root.id,
            [_canonical_call_item(call_id, response_id)],
            acp_tool_call_scope=scope,
        )

    assert [
        item.model_dump(mode="json") for item in conversation_store.list_items(root.id).data
    ] == before_items
    assert pending_binding_state() == before_binding


@pytest.mark.parametrize("item_type", ["function_call", "function_call_output"])
def test_canonical_append_rejects_ambiguous_physical_item_rows(
    conversation_store: SqlAlchemyConversationStore,
    item_type: str,
) -> None:
    """Scoped append rejects two physical rows for one canonical logical identity."""
    root = _new_root(conversation_store)
    call_id = f"ambiguous-canonical-append-{item_type}"
    response_id = f"response-ambiguous-canonical-append-{item_type}"
    scope, _action_origin = _anchor_root_tool(
        conversation_store,
        root_id=root.id,
        call_id=call_id,
        sequence=1,
        response_id=response_id,
    )
    item = (
        _canonical_call_item(call_id, response_id)
        if item_type == "function_call"
        else _canonical_output_item(call_id, response_id)
    )
    first = conversation_store.append(
        root.id,
        [item],
        acp_tool_call_scope=scope,
    )[0]
    assert first.id == acp_standard_canonical_tool_item_id(root.id, scope.source_id, item_type)
    _insert_duplicate_physical_item(
        conversation_store,
        conversation_id=root.id,
        item_id=first.id,
        created_at=first.created_at,
    )
    before_items = [
        value.model_dump(mode="json") for value in conversation_store.list_items(root.id).data
    ]
    assert len(before_items) == 2

    def binding_state() -> tuple[object, ...]:
        with conversation_store._conv_session(
            "test_ambiguous_canonical_append_binding"
        ) as session:
            binding = session.get(
                SqlAcpToolCallItemBinding,
                (current_workspace_id(), root.id, scope.source_id),
            )
            assert binding is not None
            return (
                binding.native_epoch,
                binding.journal_id,
                binding.epoch_ordinal,
                binding.context_generation,
                binding.native_call_id,
                binding.action_origin_token,
                binding.action_origin_generation,
                binding.conversation_id,
                binding.canonical_item_id,
                binding.response_id,
            )

    before_binding = binding_state()
    with pytest.raises(ValueError):
        conversation_store.append(
            root.id,
            [item],
            acp_tool_call_scope=scope,
        )

    assert [
        value.model_dump(mode="json") for value in conversation_store.list_items(root.id).data
    ] == before_items
    assert binding_state() == before_binding


def test_canonical_append_rejects_wrong_first_unscoped_batch_duplicate(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A later scoped row cannot bless an earlier same-batch stable-ID collision."""
    root = _new_root(conversation_store)
    call_id = "canonical-wrong-unscoped-batch-duplicate"
    response_id = "response-canonical-wrong-unscoped-batch-duplicate"
    scope, _action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    canonical_id = acp_standard_canonical_tool_item_id(root.id, scope.source_id, "function_call")
    wrong_first = _canonical_call_item("unrelated-batch-call", response_id).model_copy(
        update={"stable_id": canonical_id}
    )
    before = conversation_store.list_items(root.id).data

    with pytest.raises(ValueError):
        conversation_store.append(
            root.id,
            [wrong_first, _canonical_call_item(call_id, response_id)],
            acp_tool_call_scope=scope,
        )

    assert conversation_store.list_items(root.id).data == before
    with conversation_store._conv_session("test_canonical_wrong_batch_duplicate") as session:
        binding = session.get(
            SqlAcpToolCallItemBinding,
            (current_workspace_id(), root.id, scope.source_id),
        )
        assert binding is None


def test_canonical_output_append_allows_accepted_anchor_without_output_entity(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """An accepted call anchor can append output before its native output entity arrives."""
    root = _new_root(conversation_store)
    call_id = "canonical-anchor-without-entity"
    response_id = "response-canonical-anchor-without-entity"
    scope, action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    accepted_anchor = _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "response_id": response_id,
        },
        action_origin=action_origin,
    )
    assert accepted_anchor["duplicate"] is False
    with conversation_store._conv_session("test_canonical_missing_output_entity") as session:
        output_entity = session.get(
            SqlAcpSessionEntity,
            (
                current_workspace_id(),
                root.id,
                _EPOCH,
                _ROOT_NATIVE,
                "item",
                acp_standard_tool_output_entity_id(call_id),
            ),
        )
        assert output_entity is None

    before = conversation_store.list_items(root.id).data
    appended = conversation_store.append(
        root.id,
        [_canonical_output_item(call_id, response_id)],
        acp_tool_call_scope=scope,
    )[0]
    assert appended.id == acp_standard_canonical_tool_item_id(
        root.id, scope.source_id, "function_call_output"
    )
    assert appended.deduplicated is False

    assert len(conversation_store.list_items(root.id).data) == len(before) + 1
    with conversation_store._conv_session("test_canonical_missing_entity_binding") as session:
        binding = session.get(
            SqlAcpToolCallItemBinding,
            (current_workspace_id(), root.id, scope.source_id),
        )
        assert binding is not None
        assert binding.canonical_item_id is None


def test_overbyte_native_identity_is_rejected_before_association_mutation(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:overbyte"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    associations_before_rejection = conversation_store.list_acp_session_associations(root.id).data
    oversized_native_id = "🧭" * 129
    with pytest.raises(ValueError, match="storage limit"):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=2,
            target=oversized_native_id,
            parent=_ROOT_NATIVE,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": oversized_native_id,
                "parent_session_id": _ROOT_NATIVE,
                "patch": {"title": "oversized"},
            },
        )

    assert conversation_store.list_acp_session_associations(root.id).data == (
        associations_before_rejection
    )


def test_read_only_observation_snapshots_round_trip_with_sparse_replacements(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:observations"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-observations",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": "native-observations",
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="native-observations",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="observation.patch",
        operation={
            "observation": {
                "session_info": {"title": "Native worker title"},
                "usage": {"tokens": 5},
                "config_options": {"thinking": ["low", "high"]},
                "mode": {"currentModeId": "plan"},
                "plan": {"entries": [{"content": "Inspect"}]},
                "transport": {"state": "connected"},
            }
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target="native-observations",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="observation.patch",
        operation={
            "observation": {
                "usage": {"tokens": 8},
                "config_options": None,
            }
        },
    )

    observation = conversation_store.get_acp_session_observation(child.id)
    assert observation is not None
    assert observation["session_info"] == {"title": "Native worker title"}
    assert observation["usage"] == {"tokens": 8}
    assert observation["config_options"] is None
    assert observation["mode"] == {"currentModeId": "plan"}
    assert observation["plan"] == {"entries": [{"content": "Inspect"}]}
    assert observation["transport"] == {"state": "connected"}
    assert conversation_store.get_acp_session_observation(root.id).get("usage") is None


def test_large_acp_payloads_and_references_survive_replica_reopen_and_replay(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    db_uri: str,
) -> None:
    """Large valid snapshots hydrate exactly from a separate store instance."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:large-payload"
    )
    large_opaque_value = base64.b64encode(os.urandom(24 * 1024)).decode("ascii")
    child_native_id = "native-child-large-payload"
    association_operation = {
        "session_id": child_native_id,
        "parent_session_id": _ROOT_NATIVE,
        "patch": {"title": "worker", "metadata": {"opaque": large_opaque_value}},
    }
    action_origin = {
        "token": "private-origin-for-large-payload-test",
        "agent": "test-agent",
        "generation": "large-payload-generation",
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation=association_operation,
        action_origin=action_origin,
    )
    initial_observation = {
        "session_info": {"opaque": large_opaque_value},
        "usage": {"tokens": 1, "opaque": large_opaque_value},
        "config_options": {"opaque": large_opaque_value},
        "mode": {"opaque": large_opaque_value},
        "plan": {"opaque": large_opaque_value},
        "transport": {"state": "connected", "opaque": large_opaque_value},
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="observation.patch",
        operation={"observation": initial_observation},
    )
    replacement_observation = {
        "usage": {"tokens": 2, "opaque": large_opaque_value},
        "config_options": None,
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="observation.patch",
        operation={"observation": replacement_observation},
    )
    message_operation = {
        "entity_type": "message",
        "entity_id": "large-message-entity",
        "value": {
            "item_type": "message",
            "response_id": "large-message-response",
            "item_data": {
                "role": "assistant",
                "agent": "unknown",
                "acp_agent_identity": "unknown",
                "content": [{"type": "text", "text": large_opaque_value}],
            },
        },
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=5,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="entity.upsert",
        operation=message_operation,
    )
    call_id = "large-reference-list-call"
    reference_operation = {
        "entity_type": "item",
        "entity_id": acp_standard_tool_entity_id(call_id),
        "call_id": call_id,
        # The count remains within the public limit; repeated references are
        # intentionally preserved in order by the entity snapshot.
        "references": [child_native_id] * 128,
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=6,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation=reference_operation,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=7,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.upsert",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "value": {
                "item_type": "function_call",
                "response_id": "large-reference-call-response",
                "item_data": {
                    "acp_agent_identity": "unknown",
                    "call_id": call_id,
                    "name": "worker",
                    "arguments": "{}",
                    "tool_status": "completed",
                },
            },
        },
    )

    replica_artifact_store = LocalArtifactStore(artifact_store.storage_location)
    replica_store = SqlAlchemyConversationStore(
        db_uri,
        artifact_store=replica_artifact_store,
    )

    observation = replica_store.get_acp_session_observation(child.id)
    assert observation is not None
    assert observation["source_current"] is True
    assert observation["session_info"] == initial_observation["session_info"]
    assert observation["usage"] == replacement_observation["usage"]
    assert observation["config_options"] is None
    assert observation["mode"] == initial_observation["mode"]
    assert observation["plan"] == initial_observation["plan"]
    assert observation["transport"] == initial_observation["transport"]

    items = replica_store.list_items(child.id).data
    assert len(items) == 1
    assert items[0].data.content == [{"type": "text", "text": large_opaque_value}]

    replay = _apply(
        replica_store,
        root_id=root.id,
        sequence=4,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="observation.patch",
        operation={"observation": replacement_observation},
    )
    assert replay["duplicate"] is True
    assert replica_store.get_acp_session_observation(child.id) == observation

    with replica_store._conv_session("test_acp_large_payload_reference_bounds") as session:
        association = session.get(
            SqlAcpSessionAssociation,
            (current_workspace_id(), root.id, _EPOCH, child_native_id),
        )
        message_entity = session.get(
            SqlAcpSessionEntity,
            (
                current_workspace_id(),
                root.id,
                _EPOCH,
                child_native_id,
                "message",
                "large-message-entity",
            ),
        )
        references_entity = session.get(
            SqlAcpSessionEntity,
            (
                current_workspace_id(),
                root.id,
                _EPOCH,
                _ROOT_NATIVE,
                "item",
                acp_standard_tool_entity_id(call_id),
            ),
        )
        assert association is not None
        assert message_entity is not None
        assert references_entity is not None
        for reference in (
            association.association_json,
            association.observation_json,
            association.action_origin_json,
            message_entity.item_data_json,
            message_entity.pending_json,
            references_entity.references_json,
        ):
            assert reference is not None
            assert len(reference.encode("utf-8")) < 16_384
        assert "private-origin-for-large-payload-test" not in association.action_origin_json
        stored_references = json.loads(references_entity.references_json)
        assert set(stored_references) == {"v", "key", "length", "sha256"}
        assert stored_references["v"] == 1
        assert 0 < stored_references["length"] < 16_384

    root_items = replica_store.list_items(root.id).data
    assert len(root_items) == 1
    assert isinstance(root_items[0].data, FunctionCallData)
    assert [
        reference.model_dump() for reference in root_items[0].data.acp_session_references or []
    ] == [{"native_session_id": child_native_id, "conversation_id": child.id}] * 128


def test_acp_artifact_rollback_keeps_orphan_and_retries_same_immutable_blob(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed database commit cannot ACK or delete a successfully stored blob."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:rollback"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )

    large_payload = base64.b64encode(os.urandom(24 * 1024)).decode("ascii")
    operation = {
        "session_id": "native-child-artifact-rollback",
        "parent_session_id": _ROOT_NATIVE,
        "patch": {"title": "worker", "metadata": {"opaque": large_payload}},
    }
    real_put = artifact_store.put
    real_delete = artifact_store.delete
    put_keys: list[str] = []
    deleted_keys: list[str] = []

    def record_put(key: str, data: bytes) -> None:
        put_keys.append(key)
        real_put(key, data)

    def record_delete(key: str) -> None:
        deleted_keys.append(key)
        real_delete(key)

    def fail_database_commit(session: Session) -> None:
        if session.get_bind() is conversation_store._conv_engine and put_keys:
            raise RuntimeError("simulated database commit failure")

    monkeypatch.setattr(artifact_store, "put", record_put)
    monkeypatch.setattr(artifact_store, "delete", record_delete)
    event.listen(Session, "before_commit", fail_database_commit)
    try:
        with pytest.raises(RuntimeError, match="simulated database commit failure"):
            _apply(
                conversation_store,
                root_id=root.id,
                sequence=2,
                target="native-child-artifact-rollback",
                parent=_ROOT_NATIVE,
                conversation_id=child.id,
                kind="association.patch",
                operation=operation,
                activate_epoch=False,
            )
    finally:
        event.remove(Session, "before_commit", fail_database_commit)

    assert put_keys
    assert deleted_keys == []
    assert all(artifact_store.exists(key) for key in put_keys)
    assert (
        conversation_store.get_acp_session_association(
            root.id, _EPOCH, "native-child-artifact-rollback"
        )
        is None
    )
    with conversation_store._conv_session("test_acp_artifact_rollback_receipt") as session:
        receipt = session.get(
            SqlAcpSessionReportReceipt,
            (
                current_workspace_id(),
                root.id,
                hashlib.sha256(root.id.encode()).hexdigest()[:32],
                2,
            ),
        )
    assert receipt is None

    retry = _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-child-artifact-rollback",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation=operation,
        activate_epoch=False,
    )
    assert retry["duplicate"] is False
    association = conversation_store.get_acp_session_association(
        root.id, _EPOCH, "native-child-artifact-rollback"
    )
    assert association is not None
    assert association["association"]["metadata"]["opaque"] == large_payload
    assert put_keys == [*dict.fromkeys(put_keys)]
    assert deleted_keys == []


def test_acp_artifact_ambiguous_commit_retry_hydrates_committed_payload(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lost commit response leaves a readable receipt and reuses its blob."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:ambiguous-commit"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {}},
    )
    large_payload = base64.b64encode(os.urandom(24 * 1024)).decode("ascii")
    operation = {
        "session_id": "native-child-ambiguous-commit",
        "parent_session_id": _ROOT_NATIVE,
        "patch": {"title": "worker", "metadata": {"opaque": large_payload}},
    }
    real_put = artifact_store.put
    real_delete = artifact_store.delete
    put_keys: list[str] = []
    deleted_keys: list[str] = []

    def record_put(key: str, data: bytes) -> None:
        put_keys.append(key)
        real_put(key, data)

    def record_delete(key: str) -> None:
        deleted_keys.append(key)
        real_delete(key)

    lost_response = False

    real_run_write_transaction = conversation_store_module.run_write_transaction

    def lose_first_commit_response(
        session_maker: Any,
        operation_name: str,
        callback: Any,
        **kwargs: Any,
    ) -> Any:
        nonlocal lost_response
        result = real_run_write_transaction(
            session_maker,
            operation_name,
            callback,
            **kwargs,
        )
        if operation_name == "apply_acp_session_report" and put_keys and not lost_response:
            lost_response = True
            raise RuntimeError("simulated lost commit response")
        return result

    monkeypatch.setattr(artifact_store, "put", record_put)
    monkeypatch.setattr(artifact_store, "delete", record_delete)
    monkeypatch.setattr(
        conversation_store_module,
        "run_write_transaction",
        lose_first_commit_response,
    )
    try:
        with pytest.raises(RuntimeError, match="simulated lost commit response"):
            _apply(
                conversation_store,
                root_id=root.id,
                sequence=2,
                target="native-child-ambiguous-commit",
                parent=_ROOT_NATIVE,
                conversation_id=child.id,
                kind="association.patch",
                operation=operation,
                activate_epoch=False,
            )
    finally:
        monkeypatch.setattr(
            conversation_store_module,
            "run_write_transaction",
            real_run_write_transaction,
        )

    assert lost_response is True
    assert len(put_keys) >= 1
    assert deleted_keys == []
    assert all(artifact_store.exists(key) for key in put_keys)
    with conversation_store._conv_session("test_acp_ambiguous_receipt") as session:
        receipt = session.get(
            SqlAcpSessionReportReceipt,
            (
                current_workspace_id(),
                root.id,
                hashlib.sha256(root.id.encode()).hexdigest()[:32],
                2,
            ),
        )
        assert receipt is not None and receipt.applied is True

    retry = _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-child-ambiguous-commit",
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation=operation,
        activate_epoch=False,
    )
    assert retry["duplicate"] is True
    association = conversation_store.get_acp_session_association(
        root.id, _EPOCH, "native-child-ambiguous-commit"
    )
    assert association is not None
    assert association["association"]["metadata"]["opaque"] == large_payload
    assert all(artifact_store.exists(key) for key in put_keys)
    assert deleted_keys == []


def test_acp_artifact_reference_accepts_1024_bytes_and_rejects_oversize(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Artifact descriptors keep the exact 1,024-byte boundary on write and read."""
    root = _new_root(conversation_store)
    payload = '{"usage":{"tokens":7}}'
    canonical_payload = json.dumps(
        json.loads(payload),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(canonical_payload).hexdigest()

    def reference_for(key: str) -> str:
        return json.dumps(
            {
                "v": 1,
                "key": key,
                "length": len(canonical_payload),
                "sha256": digest,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    descriptor_limit = conversation_store_module._ACP_REPORT_REF_MAX_BYTES
    empty_key_length = len(reference_for("").encode("utf-8"))
    exact_key = "k" * (descriptor_limit - empty_key_length)
    oversized_key = exact_key + "k"
    exact_reference = reference_for(exact_key)
    oversized_reference = reference_for(oversized_key)
    assert len(exact_reference.encode("utf-8")) == descriptor_limit == 1_024
    assert len(oversized_reference.encode("utf-8")) == descriptor_limit + 1

    key_value = [exact_key]
    monkeypatch.setattr(
        conversation_store,
        "_acp_payload_key",
        lambda **_kwargs: key_value[0],
    )
    stored_blobs: dict[str, bytes] = {}

    def put_blob(key: str, data: bytes) -> None:
        stored_blobs[key] = data

    def get_blob(key: str) -> bytes:
        try:
            return stored_blobs[key]
        except KeyError:
            raise KeyError(key) from None

    monkeypatch.setattr(artifact_store, "put", put_blob)
    monkeypatch.setattr(artifact_store, "get", get_blob)
    identity = {
        "workspace_id": current_workspace_id(),
        "root_conversation_id": root.id,
        "row_identity": (_EPOCH, _ROOT_NATIVE, "observation"),
        "field_name": "observation",
    }
    stored_reference = conversation_store._store_acp_payload_json(
        **identity,
        serialized_json=payload,
    )
    assert stored_reference == exact_reference
    assert conversation_store._load_acp_payload_json(
        stored_reference,
        **identity,
    ) == json.dumps(
        json.loads(payload),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    key_value[0] = oversized_key
    with pytest.raises(ValueError, match="reference exceeds its storage limit"):
        conversation_store._store_acp_payload_json(
            **identity,
            serialized_json=payload,
        )
    with pytest.raises(ValueError, match="reference exceeds its storage limit"):
        conversation_store._load_acp_payload_json(
            oversized_reference,
            **identity,
        )


@pytest.mark.parametrize("damage", ["missing", "reference_version", "envelope"])
def test_acp_artifact_reads_fail_closed_for_missing_or_corrupt_references(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    damage: str,
) -> None:
    """A bad artifact reference cannot turn committed evidence into defaults."""
    root = _new_root(conversation_store)
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"usage": {"tokens": 7, "detail": "preserved"}}},
    )

    with conversation_store._conv_session("test_acp_payload_reference") as session:
        row = session.get(
            SqlAcpSessionAssociation,
            (current_workspace_id(), root.id, _EPOCH, _ROOT_NATIVE),
        )
        assert row is not None
        reference = json.loads(row.observation_json)
        assert set(reference) == {"v", "key", "length", "sha256"}
        key = reference["key"]

    if damage == "missing":
        artifact_store.delete(key)
        expected_exception = KeyError
    elif damage == "reference_version":
        reference["v"] = 2
        with conversation_store._conv_session_immediate(
            "corrupt_acp_reference_version"
        ) as session:
            row = session.get(
                SqlAcpSessionAssociation,
                (current_workspace_id(), root.id, _EPOCH, _ROOT_NATIVE),
            )
            assert row is not None
            row.observation_json = json.dumps(reference, sort_keys=True, separators=(",", ":"))
        expected_exception = ValueError
    else:
        artifact_store.put(key, b"invalid-envelope-version")
        expected_exception = ValueError

    with pytest.raises(expected_exception):
        conversation_store.get_acp_session_observation(root.id)


def test_acp_payload_reference_is_hydrated_only_through_configured_store_instance(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    db_uri: str,
) -> None:
    """A second store instance must bind its own backend to hydrate persisted refs."""
    root = _new_root(conversation_store)
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"usage": {"tokens": 12}}},
    )

    unconfigured_replica = SqlAlchemyConversationStore(db_uri)
    assert unconfigured_replica.acp_session_reporting_ready is False
    with pytest.raises(
        RuntimeError,
        match="ACP session reporting requires an explicitly configured ArtifactStore",
    ):
        unconfigured_replica.get_acp_session_observation(root.id)

    configured_replica = SqlAlchemyConversationStore(
        db_uri,
        artifact_store=LocalArtifactStore(artifact_store.storage_location),
    )
    observation = configured_replica.get_acp_session_observation(root.id)
    assert observation is not None
    assert observation["usage"] == {"tokens": 12}


def test_current_root_observation_does_not_fall_back_to_retired_epoch(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """An undated fresh epoch remains current after a delayed old-epoch tail."""
    root = _new_root(conversation_store)
    old_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:old-epoch"
    )
    new_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:new-epoch"
    )
    old_epoch = "native-epoch-store-old"
    old_root_native = "native-root-store-old"
    new_epoch = "native-epoch-store-new"
    new_root_native = "native-root-store-new"

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=old_root_native,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={
            "observation": {
                "state": {"present": False},
                "transport": {"state": "disconnected", "reason": "stdout_eof"},
                "session_info": {"title": "Retired root"},
            }
        },
        journal_sequence=1,
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-child-old",
        parent=old_root_native,
        conversation_id=old_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-child-old",
            "parent_session_id": old_root_native,
            "patch": {"title": "worker", "description": "old assignment"},
        },
        journal_sequence=2,
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="native-child-old",
        parent=old_root_native,
        conversation_id=old_child.id,
        kind="entity.upsert",
        operation={
            "entity_type": "message",
            "entity_id": "old-child-history",
            "value": {
                "item_type": "message",
                "response_id": "old-child-response",
                "item_data": {
                    "role": "assistant",
                    "agent": "unknown",
                    "content": [{"type": "text", "text": "retained old result"}],
                    "acp_agent_identity": "unknown",
                },
            },
        },
        journal_sequence=3,
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )

    # The new association creates an empty, undated root row for this epoch.
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target="native-child-new",
        parent=new_root_native,
        conversation_id=new_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-child-new",
            "parent_session_id": new_root_native,
            "patch": {"title": "worker", "description": "new assignment"},
        },
        journal_sequence=4,
        native_epoch=new_epoch,
        root_native_session_id=new_root_native,
        epoch_ordinal=2,
    )

    current_observation = conversation_store.get_acp_session_observation(root.id)
    assert current_observation is not None
    assert current_observation["state"] is None
    assert current_observation["state_ingress_sequence"] is None
    assert current_observation["state_observed_at"] is None
    assert current_observation["last_ingress_sequence"] == 0
    assert current_observation["last_reported_at"] is None
    assert "transport" not in current_observation
    assert "session_info" not in current_observation
    assert "usage" not in current_observation
    new_root = conversation_store.get_acp_session_association(root.id, new_epoch, new_root_native)
    new_child_association = conversation_store.get_acp_session_association(
        root.id, new_epoch, "native-child-new"
    )
    assert new_root is not None
    assert new_root["conversation_id"] == root.id
    assert new_child_association is not None
    assert new_child_association["conversation_id"] == new_child.id

    # A delayed durable tail from epoch A updates its own root history but
    # must not change the public root's current-epoch observation.
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target=old_root_native,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={
            "observation": {
                "state": {"present": False},
                "transport": {"state": "disconnected", "reason": "delayed_old_epoch_tail"},
                "usage": {"tokens": 13},
            }
        },
        # This old-epoch tail arrived after B became current, but keeps a
        # contiguous journal position after the new-epoch association.
        journal_sequence=5,
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
        epoch_ordinal=1,
        activate_epoch=False,
    )

    current_observation = conversation_store.get_acp_session_observation(root.id)
    assert current_observation is not None
    assert current_observation["state"] is None
    assert current_observation["state_ingress_sequence"] is None
    assert current_observation["last_ingress_sequence"] == 0
    assert current_observation["last_reported_at"] is None
    assert "transport" not in current_observation
    assert "session_info" not in current_observation
    assert "usage" not in current_observation

    old_history = conversation_store.list_items(old_child.id).data
    assert len(old_history) == 1
    assert "retained old result" in json.dumps(old_history[0].data.model_dump())


def test_bulk_observations_match_singular_current_and_historical_projection(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    root = _new_root(conversation_store)
    old_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:bulk-old"
    )
    current_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:bulk-current"
    )
    old_epoch = "native-epoch-bulk-old"
    old_root_native = "native-root-bulk-old"
    current_epoch = "native-epoch-bulk-current"
    current_root_native = "native-root-bulk-current"

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=old_root_native,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"state": {"present": True, "value": "running"}}},
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target="native-child-bulk-old",
        parent=old_root_native,
        conversation_id=old_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-child-bulk-old",
            "parent_session_id": old_root_native,
            "patch": {"title": "worker"},
        },
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="native-child-bulk-old",
        parent=old_root_native,
        conversation_id=old_child.id,
        kind="observation.patch",
        operation={"observation": {"state": {"present": True, "value": "running"}}},
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )

    # A sparse association in the selected epoch still yields an observation,
    # while the retained old child remains historical.
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        journal_sequence=4,
        target="native-child-bulk-current",
        parent=current_root_native,
        conversation_id=current_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-child-bulk-current",
            "parent_session_id": current_root_native,
            "patch": {"title": "worker"},
        },
        native_epoch=current_epoch,
        root_native_session_id=current_root_native,
        epoch_ordinal=2,
    )

    missing_conversation_id = hashlib.sha256(b"missing-acp-observation").hexdigest()[:32]
    observations = conversation_store.get_acp_session_observations(
        [root.id, old_child.id, current_child.id, missing_conversation_id]
    )

    assert set(observations) == {root.id, old_child.id, current_child.id}
    assert observations[root.id] == conversation_store.get_acp_session_observation(root.id)
    assert observations[old_child.id] == conversation_store.get_acp_session_observation(
        old_child.id
    )
    assert observations[current_child.id] == conversation_store.get_acp_session_observation(
        current_child.id
    )
    assert observations[old_child.id]["source_current"] is False
    assert observations[old_child.id]["state"] == {
        "present": True,
        "value": "running",
    }
    assert observations[current_child.id]["source_current"] is True
    assert observations[current_child.id]["state"] is None


def test_detached_root_tool_call_accepts_late_origin_free_references(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A report-owned root call can receive references after its parent turn ends."""
    root = _new_root(conversation_store)
    call_id = "detached-root-call"
    call_operation = {
        "entity_type": "item",
        "entity_id": acp_standard_tool_entity_id(call_id),
        "call_id": call_id,
        "value": {
            "item_type": "function_call",
            "response_id": "response-detached-root-call",
            "item_data": {
                "acp_agent_identity": "unknown",
                "call_id": call_id,
                "name": "worker",
                "arguments": "{}",
                "tool_status": "in_progress",
            },
        },
    }
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.upsert",
        operation=call_operation,
    )

    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:detached-reference"
    )
    child_native_id = "native-detached-reference"
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": child_native_id,
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "references": [child_native_id],
        },
    )

    items = conversation_store.list_items(root.id).data
    assert len(items) == 1
    assert isinstance(items[0].data, FunctionCallData)
    assert items[0].data.call_id == call_id
    assert [
        reference.model_dump() for reference in items[0].data.acp_session_references or []
    ] == [{"native_session_id": child_native_id, "conversation_id": child.id}]


@pytest.mark.parametrize(
    "report_anchor_first", [True, False], ids=["report-first", "append-first"]
)
def test_foreground_root_tool_call_scope_accepts_references_after_completion(
    conversation_store: SqlAlchemyConversationStore,
    report_anchor_first: bool,
) -> None:
    """Exact journal scope updates the same completed root call in either arrival order."""
    root = _new_root(conversation_store)
    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:foreground-reference"
    )
    child_native_id = "native-foreground-reference"
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": child_native_id,
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
    )

    call_id = "foreground-root-call"
    response_id = "response-foreground-root-call"
    origin_token = "a" * 32
    origin_generation = "foreground-action-generation"
    source_id = acp_standard_report_source_id(_SERVER, root.id, _EPOCH, 2)
    anchor_operation = {
        "entity_type": "item",
        "entity_id": acp_standard_tool_entity_id(call_id),
        "call_id": call_id,
        "response_id": response_id,
    }
    action_origin = {
        "token": origin_token,
        "agent": "test-agent",
        "generation": origin_generation,
    }
    append_scope = AcpToolCallAppendScope(
        source_id=source_id,
        native_call_id=call_id,
        action_origin_token=origin_token,
        action_origin_generation=origin_generation,
        response_id=response_id,
    )
    canonical_call = NewConversationItem(
        type="function_call",
        response_id=response_id,
        data=FunctionCallData(
            acp_agent_identity="unknown",
            call_id=call_id,
            name="worker",
            arguments="{}",
            tool_status="completed",
        ),
    )

    if report_anchor_first:
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=2,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation=anchor_operation,
            action_origin=action_origin,
        )
    conversation_store.append(root.id, [canonical_call], acp_tool_call_scope=append_scope)
    if not report_anchor_first:
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=2,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation=anchor_operation,
            action_origin=action_origin,
        )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "references": [child_native_id],
        },
    )

    items = conversation_store.list_items(root.id).data
    assert len(items) == 1
    assert items[0].response_id == response_id
    assert items[0].status == "completed"
    assert isinstance(items[0].data, FunctionCallData)
    assert items[0].data.call_id == call_id
    assert [
        reference.model_dump() for reference in items[0].data.acp_session_references or []
    ] == [{"native_session_id": child_native_id, "conversation_id": child.id}]


@pytest.mark.parametrize("admission_order", ["report-first", "append-first"])
def test_root_tool_binding_capacity_counts_unique_bindings_at_boundary(
    conversation_store: SqlAlchemyConversationStore,
    db_uri: str,
    admission_order: str,
) -> None:
    """Retries and role promotion use one root binding slot in either arrival order."""
    root = _new_root(conversation_store)
    capacity = conversation_store_module._ACP_TOOL_CALL_BINDING_MAX_ROWS
    _set_acp_tool_call_binding_count(conversation_store, root.id, capacity - 1)

    call_id = "binding-capacity-call"
    response_id = "response-binding-capacity"
    scope, action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    canonical_call = _canonical_call_item(call_id, response_id)
    canonical_output = _canonical_output_item(call_id, response_id)

    def anchor_current_call() -> dict[str, object]:
        return _apply(
            conversation_store,
            root_id=root.id,
            sequence=1,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation={
                "entity_type": "item",
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "response_id": response_id,
            },
            action_origin=action_origin,
        )

    if admission_order == "report-first":
        first_report = anchor_current_call()
        assert first_report["duplicate"] is False
        conversation_store.append(root.id, [canonical_call], acp_tool_call_scope=scope)
    else:
        conversation_store.append(root.id, [canonical_call], acp_tool_call_scope=scope)
        assert _acp_tool_call_binding_count(conversation_store, root.id) == capacity
        first_report = anchor_current_call()
        assert first_report["duplicate"] is False

    conversation_store.append(root.id, [canonical_output], acp_tool_call_scope=scope)
    assert _acp_tool_call_binding_count(conversation_store, root.id) == capacity
    assert anchor_current_call()["duplicate"] is True
    conversation_store.append(root.id, [canonical_call], acp_tool_call_scope=scope)
    conversation_store.append(root.id, [canonical_output], acp_tool_call_scope=scope)
    assert _acp_tool_call_binding_count(conversation_store, root.id) == capacity

    extra_call_id = "binding-capacity-overflow-call"
    extra_response_id = "response-binding-capacity-overflow"
    extra_scope, extra_origin = _tool_scope(
        root.id,
        call_id=extra_call_id,
        anchor_sequence=2,
        response_id=extra_response_id,
    )
    if admission_order == "report-first":
        with pytest.raises(ValueError, match="binding capacity"):
            _apply(
                conversation_store,
                root_id=root.id,
                sequence=2,
                target=_ROOT_NATIVE,
                parent=None,
                conversation_id=root.id,
                kind="tool.references",
                operation={
                    "entity_type": "item",
                    "entity_id": acp_standard_tool_entity_id(extra_call_id),
                    "call_id": extra_call_id,
                    "response_id": extra_response_id,
                },
                action_origin=extra_origin,
            )
    else:
        with pytest.raises(ValueError, match="binding capacity"):
            conversation_store.append(
                root.id,
                [_canonical_call_item(extra_call_id, extra_response_id)],
                acp_tool_call_scope=extra_scope,
            )

    assert _acp_tool_call_binding_count(conversation_store, root.id) == capacity
    assert _acp_tool_call_binding_exists(conversation_store, root.id, scope.source_id)
    assert not _acp_tool_call_binding_exists(conversation_store, root.id, extra_scope.source_id)
    reopened = SqlAlchemyConversationStore(db_uri)
    assert _acp_tool_call_binding_count(reopened, root.id) == capacity


@pytest.mark.parametrize("admission_order", ["report-first", "append-first"])
def test_root_tool_binding_capacity_reservation_rolls_back_on_commit_failure(
    conversation_store: SqlAlchemyConversationStore,
    db_uri: str,
    admission_order: str,
) -> None:
    """A failed binding transaction rolls back its reservation and can retry."""
    root = _new_root(conversation_store)
    call_id = f"binding-rollback-{admission_order}"
    response_id = f"response-binding-rollback-{admission_order}"
    scope, action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    canonical_call = _canonical_call_item(call_id, response_id)
    if admission_order == "report-first":
        connection_admission_id, journal_id, _connection_id = _report_owner(
            conversation_store, root.id
        )
        conversation_store.activate_acp_report_epoch(
            root.id, connection_admission_id, 1, journal_id, _EPOCH, 1
        )

        def create_binding() -> object:
            return _apply(
                conversation_store,
                root_id=root.id,
                sequence=1,
                target=_ROOT_NATIVE,
                parent=None,
                conversation_id=root.id,
                kind="tool.references",
                operation={
                    "entity_type": "item",
                    "entity_id": acp_standard_tool_entity_id(call_id),
                    "call_id": call_id,
                    "response_id": response_id,
                },
                action_origin=action_origin,
                activate_epoch=False,
                connection_admission_id=connection_admission_id,
                journal_id=journal_id,
            )
    else:

        def create_binding() -> object:
            return conversation_store.append(root.id, [canonical_call], acp_tool_call_scope=scope)

    def fail_database_commit(session: Session) -> None:
        if session.get_bind() is conversation_store._conv_engine:
            raise RuntimeError("simulated canonical binding commit failure")

    event.listen(Session, "before_commit", fail_database_commit)
    try:
        with pytest.raises(RuntimeError, match="simulated canonical binding commit failure"):
            create_binding()
    finally:
        event.remove(Session, "before_commit", fail_database_commit)

    assert _acp_tool_call_binding_count(conversation_store, root.id) == 0
    assert conversation_store.list_items(root.id).data == []
    assert not _acp_tool_call_binding_exists(conversation_store, root.id, scope.source_id)

    create_binding()
    assert _acp_tool_call_binding_count(conversation_store, root.id) == 1
    assert _acp_tool_call_binding_exists(conversation_store, root.id, scope.source_id)
    reopened = SqlAlchemyConversationStore(db_uri)
    assert _acp_tool_call_binding_count(reopened, root.id) == 1


@pytest.mark.parametrize("delivery", ["generic-first", "report-first", "interleaved"])
@pytest.mark.parametrize(
    "role_order",
    [("function_call", "function_call_output"), ("function_call_output", "function_call")],
    ids=["call-before-output", "output-before-call"],
)
def test_canonical_root_call_and_output_join_across_delivery_orders(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    db_uri: str,
    delivery: str,
    role_order: tuple[str, str],
) -> None:
    """Both canonical roles join the original response through retries and reloads."""
    root = _new_root(conversation_store)
    call_id = f"canonical-{delivery}-{role_order[0]}"
    response_id = "response-canonical-original"
    scope, action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
    )
    items = {
        "function_call": _canonical_call_item(call_id, response_id),
        "function_call_output": _canonical_output_item(call_id, response_id),
    }
    operations = {
        "function_call": _root_tool_upsert(
            call_id=call_id,
            response_id=response_id,
            item_type="function_call",
            item_data={
                "acp_agent_identity": "unknown",
                "call_id": call_id,
                "name": "native_worker",
                "arguments": '{"task":"native"}',
                "tool_status": "completed",
            },
        ),
        "function_call_output": _root_tool_upsert(
            call_id=call_id,
            response_id=response_id,
            item_type="function_call_output",
            item_data={
                "acp_agent_identity": "unknown",
                "call_id": call_id,
                "output": "native result",
            },
        ),
    }

    def append_role(role: str):
        return conversation_store.append(root.id, [items[role]], acp_tool_call_scope=scope)[0]

    if delivery == "generic-first":
        for role in role_order:
            append_role(role)
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=1,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation={
                "entity_type": "item",
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "response_id": response_id,
            },
            action_origin=action_origin,
        )
        report_order = role_order
        report_start = 2
    elif delivery == "report-first":
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=1,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation={
                "entity_type": "item",
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "response_id": response_id,
            },
            action_origin=action_origin,
        )
        report_order = role_order
        report_start = 2
    else:
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=1,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation={
                "entity_type": "item",
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "response_id": response_id,
            },
            action_origin=action_origin,
        )
        append_role(role_order[0])
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=2,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="entity.upsert",
            operation=operations[role_order[0]],
        )
        append_role(role_order[1])
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=3,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="entity.upsert",
            operation=operations[role_order[1]],
        )
        report_order = ()
        report_start = 4

    if delivery != "interleaved":
        for index, role in enumerate(report_order, start=report_start):
            _apply(
                conversation_store,
                root_id=root.id,
                sequence=index,
                target=_ROOT_NATIVE,
                parent=None,
                conversation_id=root.id,
                kind="entity.upsert",
                operation=operations[role],
            )
        if delivery == "report-first":
            for role in role_order:
                append_role(role)

    expected_ids = {
        role: acp_standard_canonical_tool_item_id(root.id, scope.source_id, role)
        for role in ("function_call", "function_call_output")
    }
    for role in role_order:
        duplicate = append_role(role)
        assert duplicate.id == expected_ids[role]
        assert duplicate.deduplicated is True

    reopened = SqlAlchemyConversationStore(db_uri, artifact_store=artifact_store)
    persisted = reopened.list_items(root.id).data
    assert len(persisted) == 2
    by_role = {item.type: item for item in persisted}
    assert set(by_role) == set(expected_ids)
    assert {role: item.id for role, item in by_role.items()} == expected_ids
    assert all(item.response_id == response_id for item in persisted)
    call = by_role["function_call"]
    output = by_role["function_call_output"]
    assert isinstance(call.data, FunctionCallData)
    assert (call.data.call_id, call.data.name, call.data.arguments) == (
        call_id,
        "native_worker",
        '{"task":"native"}',
    )
    assert isinstance(output.data, FunctionCallOutputData)
    assert (output.data.call_id, output.data.output) == (call_id, "native result")
    public_items = json.dumps([item.model_dump(mode="json") for item in persisted])
    assert "_omnigent_acp_tool_call_scope" not in public_items


def test_canonical_append_validates_proof_for_pure_and_same_batch_duplicates(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A stable-ID hit still checks immutable provenance and dedupes within a batch."""
    root = _new_root(conversation_store)
    call_id = "canonical-duplicate-proof"
    response_id = "response-canonical-duplicate-proof"
    scope, _action_origin = _anchor_root_tool(
        conversation_store,
        root_id=root.id,
        call_id=call_id,
        sequence=1,
        response_id=response_id,
    )
    item = _canonical_call_item(call_id, response_id)
    first = conversation_store.append(root.id, [item], acp_tool_call_scope=scope)[0]
    retry = conversation_store.append(root.id, [item], acp_tool_call_scope=scope)[0]
    assert retry.id == first.id
    assert retry.deduplicated is True

    conflicting_scope = scope.__class__(
        source_id=scope.source_id,
        native_call_id=scope.native_call_id,
        action_origin_token="b" * 32,
        action_origin_generation=scope.action_origin_generation,
        response_id=scope.response_id,
    )
    with pytest.raises(ValueError, match="scope was rebound"):
        conversation_store.append(root.id, [item], acp_tool_call_scope=conflicting_scope)

    second_root = _new_root(conversation_store)
    second_call_id = "canonical-same-batch-proof"
    second_response_id = "response-canonical-same-batch-proof"
    second_scope, _second_action_origin = _anchor_root_tool(
        conversation_store,
        root_id=second_root.id,
        call_id=second_call_id,
        sequence=1,
        response_id=second_response_id,
    )
    stable_id = acp_standard_canonical_tool_item_id(
        second_root.id, second_scope.source_id, "function_call"
    )
    first_item = _canonical_call_item(second_call_id, second_response_id).model_copy(
        update={"stable_id": stable_id}
    )
    duplicate_item = _canonical_call_item(
        "unrelated-batch-call", second_response_id, name="unrelated"
    ).model_copy(update={"stable_id": stable_id})
    batch = conversation_store.append(
        second_root.id,
        [first_item, duplicate_item],
        acp_tool_call_scope=second_scope,
    )
    assert len(batch) == 2
    assert [result.id for result in batch] == [stable_id, stable_id]
    assert batch[1].deduplicated is True
    stored = conversation_store.list_items(second_root.id).data
    assert len(stored) == 1
    assert isinstance(stored[0].data, FunctionCallData)
    assert stored[0].data.call_id == second_call_id


def test_late_native_completion_keeps_a_original_scope_after_begins(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """A's delayed terminal and references update keep A's name, result, and response."""
    root = _new_root(conversation_store)
    call_a = "late-call-a"
    call_b = "late-call-b"
    response_a = "response-turn-a"
    response_b = "response-turn-b"
    scope_a, _action_origin_a = _anchor_root_tool(
        conversation_store,
        root_id=root.id,
        call_id=call_a,
        sequence=1,
        response_id=response_a,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=2,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.upsert",
        operation=_root_tool_upsert(
            call_id=call_a,
            response_id=response_a,
            item_type="function_call",
            item_data={
                "acp_agent_identity": "unknown",
                "call_id": call_a,
                "name": "worker_a",
                "arguments": '{"task":"from A"}',
                "tool_status": "in_progress",
            },
        ),
    )
    _anchor_root_tool(
        conversation_store,
        root_id=root.id,
        call_id=call_b,
        sequence=3,
        response_id=response_b,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.upsert",
        operation=_root_tool_upsert(
            call_id=call_b,
            response_id=response_b,
            item_type="function_call",
            item_data={
                "acp_agent_identity": "unknown",
                "call_id": call_b,
                "name": "worker_b",
                "arguments": '{"task":"from B"}',
                "tool_status": "in_progress",
            },
        ),
    )

    child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:late-a-reference"
    )
    child_native_id = "native-child-late-a-reference"
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=5,
        target=child_native_id,
        parent=_ROOT_NATIVE,
        conversation_id=child.id,
        kind="association.patch",
        operation={
            "session_id": child_native_id,
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=6,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.patch",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_a),
            "call_id": call_a,
            "patch": {"tool_status": "completed"},
        },
    )
    # This sparse clear precedes the first output baseline. The later ordered
    # upsert supersedes it; a later clear below then supersedes that replacement.
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=7,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.patch",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_output_entity_id(call_a),
            "call_id": call_a,
            "patch": {"output": None, "acp_agent_identity": "unknown"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=8,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.upsert",
        operation=_root_tool_upsert(
            call_id=call_a,
            response_id=response_a,
            item_type="function_call_output",
            item_data={
                "acp_agent_identity": "unknown",
                "call_id": call_a,
                "output": "first native result",
            },
        ),
    )
    after_first_output = {item.type: item for item in conversation_store.list_items(root.id).data}[
        "function_call_output"
    ]
    assert isinstance(after_first_output.data, FunctionCallOutputData)
    assert after_first_output.data.output == "first native result"

    _apply(
        conversation_store,
        root_id=root.id,
        sequence=9,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.patch",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_output_entity_id(call_a),
            "call_id": call_a,
            "patch": {"output": "terminal replacement"},
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=10,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_a),
            "call_id": call_a,
            "references": [child_native_id],
        },
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=11,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="entity.patch",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_output_entity_id(call_a),
            "call_id": call_a,
            "patch": {"output": None, "acp_agent_identity": "unknown"},
        },
    )

    call = conversation_store.append(
        root.id,
        [_canonical_call_item(call_a, response_a)],
        acp_tool_call_scope=scope_a,
    )[0]
    output = conversation_store.append(
        root.id,
        [_canonical_output_item(call_a, response_a, output="old generic result")],
        acp_tool_call_scope=scope_a,
    )[0]
    assert call.id == acp_standard_canonical_tool_item_id(
        root.id, scope_a.source_id, "function_call"
    )
    assert output.id == acp_standard_canonical_tool_item_id(
        root.id, scope_a.source_id, "function_call_output"
    )
    assert call.response_id == output.response_id == response_a
    assert isinstance(call.data, FunctionCallData)
    assert (call.data.call_id, call.data.name, call.data.arguments, call.data.tool_status) == (
        call_a,
        "worker_a",
        '{"task":"from A"}',
        "completed",
    )
    assert [reference.model_dump() for reference in call.data.acp_session_references or []] == [
        {"native_session_id": child_native_id, "conversation_id": child.id}
    ]
    assert isinstance(output.data, FunctionCallOutputData)
    assert (output.data.call_id, output.data.output) == (call_a, None)

    persisted = conversation_store.list_items(root.id).data
    call_b_item = next(
        item
        for item in persisted
        if isinstance(item.data, FunctionCallData) and item.data.call_id == call_b
    )
    assert call_b_item.response_id == response_b
    assert isinstance(call_b_item.data, FunctionCallData)
    assert (call_b_item.data.name, call_b_item.data.arguments) == (
        "worker_b",
        '{"task":"from B"}',
    )


def test_delayed_canonical_append_uses_accepted_anchor_after_origin_release(
    conversation_store: SqlAlchemyConversationStore,
    artifact_store: LocalArtifactStore,
    db_uri: str,
) -> None:
    """An acknowledged anchor permits late append after the mutable origin is released."""
    root = _new_root(conversation_store)
    call_id = "delayed-canonical-after-release"
    response_id = "response-delayed-canonical"
    token = "c" * 32
    generation = "settled-harness-generation"
    scope, action_origin = _tool_scope(
        root.id,
        call_id=call_id,
        anchor_sequence=1,
        response_id=response_id,
        token=token,
        generation=generation,
    )
    conversation_store.register_session_action_origin(
        root.id,
        token,
        {
            "runner_id": _RUNNER_ID,
            "actor": _PRINCIPAL,
            "agent_id": "ag_0123456789abcdef0123456789abcdef",
            "bundle_location": "agent/test-agent",
            "agent_provenance": "tenant",
            "operator_authored": False,
            "effective_spec_digest": "1" * 64,
            "operator_environment_digest": "2" * 64,
            "mcp_selection_snapshot": {},
        },
    )
    bound_origin = conversation_store.bind_session_action_origin(
        root.id, token, _RUNNER_ID, generation, _PRINCIPAL
    )
    assert bound_origin is not None

    early_output = _canonical_output_item(call_id, response_id)
    early = conversation_store.append(root.id, [early_output], acp_tool_call_scope=scope)[0]
    expected_output_id = acp_standard_canonical_tool_item_id(
        root.id, scope.source_id, "function_call_output"
    )
    assert early.id == expected_output_id
    with conversation_store._conv_session("test_pending_acp_tool_binding") as session:
        pending_binding = session.get(
            SqlAcpToolCallItemBinding,
            (current_workspace_id(), root.id, scope.source_id),
        )
        assert pending_binding is not None
        assert pending_binding.native_epoch is None

    with pytest.raises(ValueError, match=r"anchor was rebound|response"):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=1,
            target=_ROOT_NATIVE,
            parent=None,
            conversation_id=root.id,
            kind="tool.references",
            operation={
                "entity_type": "item",
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "response_id": "another-response",
            },
            action_origin=action_origin,
        )
    assert conversation_store.get_session_action_origin(root.id, token) is not None

    accepted_anchor = _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        target=_ROOT_NATIVE,
        parent=None,
        conversation_id=root.id,
        kind="tool.references",
        operation={
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "response_id": response_id,
        },
        action_origin=action_origin,
    )
    assert accepted_anchor["duplicate"] is False
    assert conversation_store.release_session_action_origin(
        root.id, token, _RUNNER_ID, generation, _PRINCIPAL
    )
    assert conversation_store.get_session_action_origin(root.id, token) is None

    late_call = conversation_store.append(
        root.id,
        [_canonical_call_item(call_id, response_id, name="delayed_native_call")],
        acp_tool_call_scope=scope,
    )[0]
    late_output = conversation_store.append(
        root.id,
        [_canonical_output_item(call_id, response_id, output="delayed native result")],
        acp_tool_call_scope=scope,
    )[0]
    assert late_call.id == acp_standard_canonical_tool_item_id(
        root.id, scope.source_id, "function_call"
    )
    assert late_output.id == expected_output_id
    assert late_output.deduplicated is True

    reopened = SqlAlchemyConversationStore(db_uri, artifact_store=artifact_store)
    persisted = reopened.list_items(root.id).data
    assert len(persisted) == 2
    assert all(item.response_id == response_id for item in persisted)
    public_items = json.dumps([item.model_dump(mode="json") for item in persisted])
    assert "_omnigent_acp_tool_call_scope" not in public_items


@pytest.mark.parametrize(
    "conflict",
    ["source", "call", "role", "entity", "response", "root", "epoch", "provenance"],
)
def test_canonical_report_rejects_conflicting_anchor_scope(
    conversation_store: SqlAlchemyConversationStore,
    conflict: str,
) -> None:
    """Report joins fail closed when any immutable call-anchor component changes."""
    root = _new_root(conversation_store)
    call_id = f"canonical-scope-conflict-{conflict}"
    response_id = "response-canonical-scope-conflict"
    scope, action_origin = _anchor_root_tool(
        conversation_store,
        root_id=root.id,
        call_id=call_id,
        sequence=1,
        response_id=response_id,
    )

    if conflict == "source":
        operation: dict[str, object] = {
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "response_id": response_id,
        }
        kind = "tool.references"
        target = _ROOT_NATIVE
        action_origin_for_update = action_origin
        native_epoch = _EPOCH
        epoch_ordinal = 1
    elif conflict == "call":
        operation = {
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": f"{call_id}-different",
            "patch": {"tool_status": "completed"},
        }
        kind = "entity.patch"
        target = _ROOT_NATIVE
        action_origin_for_update = None
        native_epoch = _EPOCH
        epoch_ordinal = 1
    elif conflict == "entity":
        operation = {
            "entity_type": "item",
            "entity_id": "acp2t_" + "f" * 64,
            "call_id": call_id,
            "patch": {"tool_status": "completed"},
        }
        kind = "entity.patch"
        target = _ROOT_NATIVE
        action_origin_for_update = None
        native_epoch = _EPOCH
        epoch_ordinal = 1
    elif conflict == "role":
        operation = {
            "entity_type": "item",
            "entity_id": acp_standard_tool_output_entity_id(call_id),
            "call_id": call_id,
            "value": {
                "item_type": "function_call",
                "response_id": response_id,
                "item_data": {
                    "acp_agent_identity": "unknown",
                    "call_id": call_id,
                    "name": "wrong-role",
                    "arguments": "{}",
                },
            },
        }
        kind = "entity.upsert"
        target = _ROOT_NATIVE
        action_origin_for_update = None
        native_epoch = _EPOCH
        epoch_ordinal = 1
    elif conflict == "response":
        operation = _root_tool_upsert(
            call_id=call_id,
            response_id="another-response",
            item_type="function_call",
            item_data={
                "acp_agent_identity": "unknown",
                "call_id": call_id,
                "name": "worker",
                "arguments": "{}",
            },
        )
        kind = "entity.upsert"
        target = _ROOT_NATIVE
        action_origin_for_update = None
        native_epoch = _EPOCH
        epoch_ordinal = 1
    elif conflict == "root":
        operation = {
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "patch": {"tool_status": "completed"},
        }
        kind = "entity.patch"
        target = f"{_ROOT_NATIVE}-other"
        action_origin_for_update = None
        native_epoch = _EPOCH
        epoch_ordinal = 1
    elif conflict == "epoch":
        operation = {
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "patch": {"tool_status": "completed"},
        }
        kind = "entity.patch"
        target = _ROOT_NATIVE
        action_origin_for_update = None
        native_epoch = _EPOCH
        epoch_ordinal = 2
    else:
        operation = {
            "entity_type": "item",
            "entity_id": acp_standard_tool_entity_id(call_id),
            "call_id": call_id,
            "patch": {"tool_status": "completed"},
        }
        kind = "entity.patch"
        target = _ROOT_NATIVE
        action_origin_for_update = {
            **action_origin,
            "token": "d" * 32,
            "generation": "another-generation",
        }
        native_epoch = _EPOCH
        epoch_ordinal = 1

    with pytest.raises(ValueError):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=2,
            target=target,
            parent=None,
            conversation_id=root.id,
            kind=kind,
            operation=operation,
            action_origin=action_origin_for_update,
            native_epoch=native_epoch,
            epoch_ordinal=epoch_ordinal,
        )

    # The failed attempt cannot change the accepted canonical baseline.
    assert conversation_store.list_items(root.id).data == []
    assert scope.source_id


def test_association_inventory_keyset_pages_recover_late_and_historical_rows(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """SQL keyset cursors retain opaque IDs and recover late and historical rows."""
    root = _new_root(conversation_store)
    child_a = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:inventory-a"
    )
    child_b = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:inventory-b"
    )
    cursor_epoch = "Epoch é "
    for sequence, native_id, child in (
        (1, "Tool", child_a),
        (2, "tool", child_b),
    ):
        _apply(
            conversation_store,
            root_id=root.id,
            sequence=sequence,
            target=native_id,
            parent=_ROOT_NATIVE,
            conversation_id=child.id,
            kind="association.patch",
            operation={
                "session_id": native_id,
                "parent_session_id": _ROOT_NATIVE,
                "patch": {"title": "worker"},
            },
            native_epoch=cursor_epoch,
        )

    first_page = conversation_store.list_acp_session_associations(root.id, limit=2)
    assert len(first_page.data) == 2
    assert first_page.has_more is True
    cursor_pair = json.loads(first_page.last_id or "null")
    assert cursor_pair == [
        first_page.data[-1]["native_epoch"],
        first_page.data[-1]["native_session_id"],
    ]

    late_parent = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=root.id, title="worker:late-parent"
    )
    late_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=late_parent.id, title="worker:late-child"
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=3,
        target="zz-inventory-parent",
        parent=_ROOT_NATIVE,
        conversation_id=late_parent.id,
        kind="association.patch",
        operation={
            "session_id": "zz-inventory-parent",
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker"},
        },
        native_epoch=cursor_epoch,
    )
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=4,
        target="zz-inventory-child",
        parent="zz-inventory-parent",
        conversation_id=late_child.id,
        kind="association.patch",
        operation={
            "session_id": "zz-inventory-child",
            "parent_session_id": "zz-inventory-parent",
            "patch": {"title": "worker"},
        },
        native_epoch=cursor_epoch,
    )

    new_epoch = "native-epoch-inventory-current"
    new_root_native = "native-root-inventory-current"
    _apply(
        conversation_store,
        root_id=root.id,
        sequence=1,
        journal_sequence=5,
        target=new_root_native,
        parent=None,
        conversation_id=root.id,
        kind="observation.patch",
        operation={"observation": {"state": {"present": True, "value": "running"}}},
        native_epoch=new_epoch,
        root_native_session_id=new_root_native,
        epoch_ordinal=2,
    )

    recovered_ids: list[tuple[str, str]] = []
    after = first_page.last_id
    while after is not None:
        page = conversation_store.list_acp_session_associations(root.id, after=after, limit=2)
        recovered_ids.extend((row["native_epoch"], row["native_session_id"]) for row in page.data)
        if not page.has_more:
            break
        assert page.last_id and page.last_id != after
        after = page.last_id

    assert (cursor_epoch, "zz-inventory-parent") in recovered_ids
    assert (cursor_epoch, "zz-inventory-child") in recovered_ids
    assert (new_epoch, new_root_native) in recovered_ids

    full_inventory = []
    after = None
    while True:
        page = conversation_store.list_acp_session_associations(root.id, after=after, limit=2)
        full_inventory.extend(page.data)
        if not page.has_more:
            break
        assert page.last_id and page.last_id != after
        after = page.last_id
    identities = {(row["native_epoch"], row["native_session_id"]) for row in full_inventory}
    assert len(identities) == len(full_inventory)
    assert (cursor_epoch, "Tool") in identities
    assert (cursor_epoch, "tool") in identities
    assert (cursor_epoch, "zz-inventory-parent") in identities
    assert (cursor_epoch, "zz-inventory-child") in identities
    assert (new_epoch, new_root_native) in identities
    assert all(row["root_conversation_id"] == root.id for row in full_inventory)

    with pytest.raises(ValueError, match="cursor is malformed"):
        conversation_store.list_acp_session_associations(root.id, after="not-json")
    with pytest.raises(ValueError, match="cursor is malformed"):
        conversation_store.list_acp_session_associations(
            root.id, after='["native-epoch-store-test"]'
        )


def test_nested_report_owner_keeps_its_own_current_observation_selector(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    """Nested harness state follows its report root, not the outer spawn-tree root."""
    tree_root = _new_root(conversation_store)
    nested_root = conversation_store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=tree_root.id,
        runner_id=_RUNNER_ID,
        title="worker:nested-report-root",
    )
    old_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=nested_root.id, title="worker:nested-old"
    )
    current_child = conversation_store.create_conversation(
        kind="sub_agent", parent_conversation_id=nested_root.id, title="worker:nested-current"
    )
    old_epoch = "native-epoch-nested-owner-old"
    old_root_native = "native-root-nested-owner-old"
    current_epoch = "native-epoch-nested-owner-current"
    current_root_native = "native-root-nested-owner-current"

    _apply(
        conversation_store,
        root_id=nested_root.id,
        sequence=1,
        target=old_root_native,
        parent=None,
        conversation_id=nested_root.id,
        kind="observation.patch",
        operation={"observation": {"state": {"present": True, "value": "running"}}},
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=nested_root.id,
        sequence=2,
        target="native-nested-old-child",
        parent=old_root_native,
        conversation_id=old_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-nested-old-child",
            "parent_session_id": old_root_native,
            "patch": {"title": "worker"},
        },
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=nested_root.id,
        sequence=3,
        target="native-nested-old-child",
        parent=old_root_native,
        conversation_id=old_child.id,
        kind="observation.patch",
        operation={"observation": {"state": {"present": True, "value": "requires_action"}}},
        native_epoch=old_epoch,
        root_native_session_id=old_root_native,
    )
    _apply(
        conversation_store,
        root_id=nested_root.id,
        sequence=1,
        journal_sequence=4,
        target=current_root_native,
        parent=None,
        conversation_id=nested_root.id,
        kind="observation.patch",
        operation={"observation": {"usage": {"tokens": 1}}},
        native_epoch=current_epoch,
        root_native_session_id=current_root_native,
        epoch_ordinal=2,
    )
    _apply(
        conversation_store,
        root_id=nested_root.id,
        sequence=2,
        journal_sequence=5,
        target="native-nested-current-child",
        parent=current_root_native,
        conversation_id=current_child.id,
        kind="association.patch",
        operation={
            "session_id": "native-nested-current-child",
            "parent_session_id": current_root_native,
            "patch": {"title": "worker"},
        },
        native_epoch=current_epoch,
        root_native_session_id=current_root_native,
        epoch_ordinal=2,
    )

    singular_old = conversation_store.get_acp_session_observation(old_child.id)
    singular_current = conversation_store.get_acp_session_observation(current_child.id)
    bulk = conversation_store.get_acp_session_observations([old_child.id, current_child.id])

    assert nested_root.root_conversation_id == tree_root.id
    assert singular_old is not None and singular_old["source_current"] is False
    assert singular_old["state"] == {"present": True, "value": "requires_action"}
    assert singular_current is not None and singular_current["source_current"] is True
    assert singular_current["state"] is None
    assert bulk == {old_child.id: singular_old, current_child.id: singular_current}
