"""Schema-v3 durable standard ACP report journal and adapter contracts."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from omnigent.inner.acp_standard_subagents import (
    ACTION_FEATURES,
    REPORT_FEATURES,
    REPORT_PROTOCOL,
    REPORT_PROTOCOL_VERSION,
    acp_standard_report_source_id,
    acp_standard_tool_entity_id,
)
from omnigent.inner.executor import (
    ExecutorConfig,
    Message,
    MockExecutor,
    SessionReport,
    SessionReportSink,
    SessionReportStatus,
    ToolSpec,
    TurnComplete,
)
from omnigent.runtime.harnesses._executor_adapter import ExecutorAdapter
from omnigent.runtime.harnesses.session_reports import (
    SessionReportJournal,
    SessionReportJournalConflict,
    SessionReportJournalCorrupt,
    SessionReportJournalVersionMismatch,
)

_PARENT_SESSION_ID = "conv_standard_report_parent"
_SERVER_IDENTITY = "server-scope-test"
_SUBSCRIBER_GENERATION = "spawn-generation-one"
_NATIVE_EPOCH = "native-epoch-one"
_ROOT_NATIVE_SESSION_ID = "native-root-one"
_CONTEXT_GENERATION = "a" * 64
_OMITTED = object()


def _report(
    ingress_sequence: int,
    *,
    operation: dict[str, Any] | None = None,
    target: str = _ROOT_NATIVE_SESSION_ID,
) -> SessionReport:
    resolved_operation = operation or {"observation": {"state": "idle"}}
    return SessionReport(
        native_epoch=_NATIVE_EPOCH,
        ingress_sequence=ingress_sequence,
        source_id=acp_standard_report_source_id(
            _SERVER_IDENTITY, _PARENT_SESSION_ID, _NATIVE_EPOCH, ingress_sequence
        ),
        kind="observation.patch",
        data={
            "server_identity": _SERVER_IDENTITY,
            "root_conversation_id": _PARENT_SESSION_ID,
            "context_generation": _CONTEXT_GENERATION,
            "root_native_session_id": _ROOT_NATIVE_SESSION_ID,
            "target_session_id": target,
            "parent_session_id": None,
            "operation": resolved_operation,
        },
    )


def _tool_references_report(
    ingress_sequence: int,
    *,
    call_id: str = "root-tool-reference-call",
    response_id: str | None = None,
    references: object = _OMITTED,
    action_origin: dict[str, Any] | None = None,
    kind: str = "tool.references",
) -> SessionReport:
    """Build a root tool-reference report, preserving omitted vs explicit references."""
    operation: dict[str, Any] = {
        "entity_id": acp_standard_tool_entity_id(call_id),
        "call_id": call_id,
    }
    if response_id is not None:
        operation["response_id"] = response_id
    if references is not _OMITTED:
        operation["references"] = references
    data: dict[str, Any] = {
        "server_identity": _SERVER_IDENTITY,
        "root_conversation_id": _PARENT_SESSION_ID,
        "context_generation": _CONTEXT_GENERATION,
        "root_native_session_id": _ROOT_NATIVE_SESSION_ID,
        "target_session_id": _ROOT_NATIVE_SESSION_ID,
        "parent_session_id": None,
        "operation": operation,
    }
    if action_origin is not None:
        data["_omnigent_action_origin"] = action_origin
    return SessionReport(
        native_epoch=_NATIVE_EPOCH,
        ingress_sequence=ingress_sequence,
        source_id=acp_standard_report_source_id(
            _SERVER_IDENTITY, _PARENT_SESSION_ID, _NATIVE_EPOCH, ingress_sequence
        ),
        kind=kind,
        data=data,
    )


def _journal(root: Path, *, generation: str = _SUBSCRIBER_GENERATION) -> SessionReportJournal:
    return SessionReportJournal(
        _PARENT_SESSION_ID,
        server_identity=_SERVER_IDENTITY,
        subscriber_generation=generation,
        root=root,
    )


def test_schema_v3_journal_replays_operations_and_acknowledges_only_exposed_prefix(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    journal.acquire_owner()
    assert journal.root_conversation_id == _PARENT_SESSION_ID
    journal.begin_native_epoch(_NATIVE_EPOCH)
    journal_id = journal.journal_id
    assert journal.identity_for_epoch(_NATIVE_EPOCH) == (journal_id, 1)
    journal.bind_native_session(_NATIVE_EPOCH, _ROOT_NATIVE_SESSION_ID)

    first = _report(1)
    second = _report(2, operation={"observation": {"usage": {"tokens": 3}}})
    first_result = journal.append(first)
    duplicate = journal.append(first)
    second_result = journal.append(second)

    assert first_result.inserted is True
    assert first_result.record.journal_id == journal_id
    assert first_result.record.epoch_ordinal == 1
    assert duplicate.inserted is False
    assert duplicate.record == first_result.record
    assert second_result.record.sequence == first_result.record.sequence + 1
    with pytest.raises(SessionReportJournalConflict, match="different report content"):
        journal.append(_report(1, operation={"observation": {"usage": {"tokens": 4}}}))
    with pytest.raises(SessionReportJournalConflict, match="contiguous"):
        journal.append(_report(4))

    first_page = journal.get_after(0, subscriber_generation=_SUBSCRIBER_GENERATION, limit=1)
    assert first_page.records == (first_result.record,)
    assert first_page.highest_sequence == second_result.record.sequence
    assert first_page.exposed_through == first_result.record.sequence
    assert first_page.more is True
    with pytest.raises(SessionReportJournalConflict, match="exposed"):
        journal.ack(second_result.record.sequence, subscriber_generation=_SUBSCRIBER_GENERATION)
    assert (
        journal.ack(first_result.record.sequence, subscriber_generation=_SUBSCRIBER_GENERATION)
        == 1
    )

    second_page = journal.get_after(1, subscriber_generation=_SUBSCRIBER_GENERATION, limit=10)
    assert second_page.records == (second_result.record,)
    assert (
        journal.ack(second_result.record.sequence, subscriber_generation=_SUBSCRIBER_GENERATION)
        == 2
    )
    assert journal.snapshot(subscriber_generation=_SUBSCRIBER_GENERATION) == (2, 2)
    journal.close()

    replacement = _journal(tmp_path, generation="spawn-generation-two")
    replacement.acquire_owner()
    assert replacement.journal_id == journal_id
    assert replacement.epoch_ordinal(_NATIVE_EPOCH) == 1
    replacement.begin_native_epoch("native-epoch-two")
    assert replacement.epoch_ordinal("native-epoch-two") == 2
    replacement.bind_native_session("native-epoch-two", "native-root-two")
    pending = SessionReport(
        native_epoch="native-epoch-two",
        ingress_sequence=1,
        source_id=acp_standard_report_source_id(
            _SERVER_IDENTITY, _PARENT_SESSION_ID, "native-epoch-two", 1
        ),
        kind="observation.patch",
        data={
            "server_identity": _SERVER_IDENTITY,
            "root_conversation_id": _PARENT_SESSION_ID,
            "context_generation": _CONTEXT_GENERATION,
            "root_native_session_id": "native-root-two",
            "target_session_id": "native-root-two",
            "parent_session_id": None,
            "operation": {"observation": {"mode": "plan"}},
        },
    )
    appended = replacement.append(pending)
    assert appended.record.journal_id == journal_id
    assert appended.record.epoch_ordinal == 2
    replacement.close()

    replay = _journal(tmp_path, generation="spawn-generation-three")
    replay.acquire_owner()
    replayed_page = replay.get_after(0, subscriber_generation="spawn-generation-three")
    assert [record.source_id for record in replayed_page.records] == [pending.source_id]
    assert replayed_page.records[0].sequence == appended.record.sequence
    assert replayed_page.records[0].journal_id == journal_id
    assert replayed_page.records[0].epoch_ordinal == 2
    assert replay.append(pending).inserted is False
    replay.close()


def test_root_tool_reference_anchor_and_explicit_updates_replay_without_losing_presence(
    tmp_path: Path,
) -> None:
    """An identity anchor permits exact-value array updates with or without provenance."""
    journal = _journal(tmp_path)
    journal.acquire_owner()
    journal.begin_native_epoch(_NATIVE_EPOCH)
    journal.bind_native_session(_NATIVE_EPOCH, _ROOT_NATIVE_SESSION_ID)
    origin = {
        "token": "a" * 32,
        "agent": "worker-agent",
        "generation": "trusted-root-tool-generation",
    }
    anchor = _tool_references_report(
        1,
        response_id="original-root-response",
        action_origin=origin,
    )
    explicit_empty = _tool_references_report(2, references=[])
    explicit_array = _tool_references_report(
        3,
        references=["native-child-reference"],
        action_origin=origin,
    )

    first = journal.append(anchor)
    assert first.inserted is True
    assert journal.append(anchor).inserted is False
    empty_result = journal.append(explicit_empty)
    array_result = journal.append(explicit_array)
    assert empty_result.inserted is True
    assert array_result.inserted is True

    records = journal.get_after(
        0,
        subscriber_generation=_SUBSCRIBER_GENERATION,
        limit=10,
    ).records
    assert [record.kind for record in records] == [
        "tool.references",
        "tool.references",
        "tool.references",
    ]
    assert records[0].data["operation"]["response_id"] == "original-root-response"
    assert "references" not in records[0].data["operation"]
    assert records[0].data["_omnigent_action_origin"] == origin
    assert records[1].data["operation"]["references"] == []
    assert "_omnigent_action_origin" not in records[1].data
    assert records[2].data["operation"]["references"] == ["native-child-reference"]
    assert records[2].data["_omnigent_action_origin"] == origin
    journal.close()

    reopened = _journal(tmp_path, generation="spawn-generation-two")
    reopened.acquire_owner()
    replayed = reopened.get_after(
        0,
        subscriber_generation="spawn-generation-two",
        limit=10,
    ).records
    assert replayed == records
    assert reopened.append(anchor).inserted is False
    assert reopened.append(explicit_empty).inserted is False
    assert reopened.append(explicit_array).inserted is False
    reopened.close()


@pytest.mark.parametrize(
    "case",
    [
        "origin_on_unrelated_kind",
        "malformed_origin_on_explicit_references",
        "missing_origin_token",
        "unexpected_origin_field",
        "missing_anchor_response",
        "oversized_anchor_response",
    ],
)
def test_private_tool_origin_is_limited_to_well_formed_identity_only_root_anchor(
    tmp_path: Path,
    case: str,
) -> None:
    """Unrelated operations and malformed origins or anchors are rejected."""
    journal = _journal(tmp_path)
    journal.acquire_owner()
    journal.begin_native_epoch(_NATIVE_EPOCH)
    journal.bind_native_session(_NATIVE_EPOCH, _ROOT_NATIVE_SESSION_ID)
    origin = {
        "token": "a" * 32,
        "agent": "worker-agent",
        "generation": "trusted-root-tool-generation",
    }
    kind = "tool.references"
    response_id: str | None = "original-root-response"
    references: object = _OMITTED
    sequence = 1
    if case == "origin_on_unrelated_kind":
        kind = "entity.upsert"
    elif case == "malformed_origin_on_explicit_references":
        journal.append(
            _tool_references_report(
                1,
                response_id=response_id,
                action_origin=origin,
            )
        )
        sequence = 2
        references = ["native-child-reference"]
        origin = {**origin, "token": "b" * 31}
    elif case == "missing_origin_token":
        origin.pop("token")
    elif case == "unexpected_origin_field":
        origin["unexpected"] = "not-allowed"
    elif case == "missing_anchor_response":
        response_id = None
    elif case == "oversized_anchor_response":
        response_id = "r" * 65

    report = _tool_references_report(
        sequence,
        response_id=response_id,
        references=references,
        action_origin=origin,
        kind=kind,
    )
    try:
        with pytest.raises(ValueError):
            journal.append(report)
    finally:
        journal.close()


def test_epoch_ordinal_is_allocated_before_native_session_creation_and_survives_failure(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    journal.acquire_owner()

    journal.begin_native_epoch("native-session-create-failed")
    failed_ordinal = journal.epoch_ordinal("native-session-create-failed")
    journal.close()

    replacement = _journal(tmp_path, generation="spawn-generation-two")
    replacement.acquire_owner()
    replacement.begin_native_epoch("native-session-created-next")
    replacement.bind_native_session("native-session-created-next", "native-root-next")

    assert failed_ordinal == 1
    assert replacement.epoch_ordinal("native-session-create-failed") == 1
    assert replacement.epoch_ordinal("native-session-created-next") == 2
    replacement.close()


def test_schema_one_report_journal_is_rejected_without_reinterpretation(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    journal.acquire_owner()
    path = journal.path
    journal.close()
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "parent_session_id": _PARENT_SESSION_ID,
                "server_identity_hash": journal.server_identity_hash,
                "subscriber_generation": _SUBSCRIBER_GENERATION,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SessionReportJournalVersionMismatch, match=r"schema 1.*schema 3"):
        _journal(tmp_path, generation="spawn-generation-two").acquire_owner()

    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == 1


@pytest.mark.parametrize("field", ["journal_id", "next_epoch_ordinal"])
def test_schema_three_journal_requires_persisted_lineage_and_ordinal_metadata(
    tmp_path: Path, field: str
) -> None:
    journal = _journal(tmp_path)
    journal.acquire_owner()
    journal.begin_native_epoch(_NATIVE_EPOCH)
    path = journal.path
    journal.close()

    value = json.loads(path.read_text(encoding="utf-8"))
    value.pop(field)
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(SessionReportJournalCorrupt):
        _journal(tmp_path, generation="spawn-generation-two").acquire_owner()


def test_true_journal_recreation_allocates_a_new_lineage_and_epoch_ordinal(
    tmp_path: Path,
) -> None:
    original = _journal(tmp_path)
    original.acquire_owner()
    original.begin_native_epoch(_NATIVE_EPOCH)
    old_journal_id = original.journal_id
    path = original.path
    original.close()
    path.unlink()

    recreated = _journal(tmp_path, generation="spawn-generation-two")
    recreated.acquire_owner()
    recreated.begin_native_epoch("native-epoch-recreated")

    assert recreated.journal_id != old_journal_id
    assert recreated.epoch_ordinal("native-epoch-recreated") == 1
    recreated.close()


class _NegotiatingExecutor(MockExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.negotiation: tuple[tuple[str, ...], tuple[str, ...]] | None = None
        self.sink: SessionReportSink | None = None
        self.events: list[str] = []

    def supports_session_reports(self) -> bool:
        return True

    def enable_standard_subagents(
        self, report_features: tuple[str, ...], action_features: tuple[str, ...]
    ) -> None:
        self.events.append("enable")
        self.negotiation = (report_features, action_features)

    def bind_session_action_origin(
        self,
        token: str | None,
        agent: str | None,
        generation: str | None = None,
        context_generation: str | None = None,
        *,
        response_id: str | None = None,
    ) -> None:
        del token, agent, generation, context_generation, response_id

    def set_session_report_sink(self, sink: SessionReportSink) -> None:
        self.events.append("sink")
        self.sink = sink

    def session_report_status(self) -> SessionReportStatus:
        return SessionReportStatus(state="ready", prompt_started=False)

    async def run_turn(
        self,
        messages: list[Message],
        tools: list[ToolSpec],
        system_prompt: str,
        config: ExecutorConfig | None = None,
    ) -> Any:
        del messages, tools, system_prompt, config
        if self.sink is None:
            raise AssertionError("the trusted v3 report journal must be bound before a turn")
        yield TurnComplete(response="ready")


@pytest.mark.asyncio
async def test_adapter_negotiates_report_v3_and_action_v2_before_injecting_sink(
    tmp_path: Path,
) -> None:
    from omnigent.runtime.harnesses._scaffold import TurnContext
    from omnigent.server.schemas import CreateResponseRequest

    executor = _NegotiatingExecutor()
    adapter = ExecutorAdapter(
        executor_factory=lambda: executor,
        session_key=_PARENT_SESSION_ID,
        session_report_protocol="acp-session-reports/3",
        session_report_server_identity=_SERVER_IDENTITY,
        session_report_subscriber_generation=_SUBSCRIBER_GENERATION,
        session_report_root=tmp_path,
    )

    await adapter.on_startup(_PARENT_SESSION_ID)
    assert executor.events == []

    request = CreateResponseRequest(model="agent", input="hello")
    request._standard_acp_report_features = tuple(REPORT_FEATURES)
    request._standard_acp_action_features = tuple(ACTION_FEATURES)
    context = TurnContext(
        response_id="response-standard-report-negotiation",
        event_queue=asyncio.Queue(),
        cancelled=asyncio.Event(),
        session_id=_PARENT_SESSION_ID,
    )
    await adapter.run_turn(request, context)

    assert executor.events == ["enable", "sink"]
    assert executor.negotiation == (
        tuple(REPORT_FEATURES),
        tuple(ACTION_FEATURES),
    )
    assert adapter._session_report_journal is executor.sink
    assert adapter._session_report_journal.root_conversation_id == _PARENT_SESSION_ID
    assert REPORT_PROTOCOL == "acp-session-reports"
    assert REPORT_PROTOCOL_VERSION == 3
    await adapter.on_shutdown()
