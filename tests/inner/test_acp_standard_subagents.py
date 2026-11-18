"""Lowest-layer contracts for the ACP standard subagent preview."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from omnigent.inner.acp_executor import AcpAgentConfig, AcpExecutor, _StandardSessionAssociation
from omnigent.inner.acp_standard_subagents import (
    ACTION_FEATURES,
    REPORT_FEATURES,
    AcpSessionActionBinding,
    AcpSessionActionIdentity,
    acp_standard_child_key,
    acp_standard_report_source_id,
    read_session_tool_references,
    read_standard_session_action,
    read_standard_subagent_frame,
)
from omnigent.inner.executor import SessionReport, SessionReportAppendResult
from omnigent.runtime.harnesses.session_reports import SessionReportJournal


def _frame(session_id: str, update: dict[str, object]) -> dict[str, object]:
    return {"sessionId": session_id, "update": update}


def test_private_v3_feature_sets_are_explicit_and_vendor_neutral() -> None:
    assert {
        "standard-acp-subagents-v1",
        "session-entities-v1",
        "session-owned-actions-v2",
        "root-source-epochs-v1",
    } == REPORT_FEATURES
    assert {"standard-acp-child-requests-v1"} == ACTION_FEATURES


@pytest.mark.parametrize(
    ("report_features", "action_features", "message"),
    [
        ((), ACTION_FEATURES, "report channel"),
        (REPORT_FEATURES, (), "action channel"),
    ],
)
def test_executor_requires_both_complete_private_v3_feature_sets(
    report_features: tuple[str, ...] | frozenset[str],
    action_features: tuple[str, ...] | frozenset[str],
    message: str,
) -> None:
    executor = AcpExecutor(AcpAgentConfig(command="unused"))

    with pytest.raises(RuntimeError, match=message):
        executor.enable_standard_subagents(report_features, action_features)
    assert executor.supports_session_reports() is True
    assert executor._standard_subagents_enabled is False


@pytest.mark.asyncio
async def test_initialize_omits_subagents_until_private_v3_channels_are_enabled(
    tmp_path: Path,
) -> None:
    disabled = AcpExecutor(AcpAgentConfig(command="unused"))
    disabled_requests: list[dict[str, object]] = []

    async def capture_disabled(_method: str, params: dict[str, object], timeout: float = 30.0):
        del timeout
        disabled_requests.append(params)
        return {"result": {}}

    disabled._rpc = capture_disabled  # type: ignore[assignment]
    await disabled._ensure_initialized()
    assert "subagents" not in disabled_requests[0]["clientCapabilities"]

    enabled, sink = _enabled_executor(tmp_path)
    enabled._rpc = AsyncMock(return_value={"result": {}})  # type: ignore[assignment]
    await enabled._ensure_initialized()
    assert enabled.supports_session_reports() is True
    capabilities = enabled._rpc.await_args.args[1]["clientCapabilities"]
    assert capabilities["subagents"] == {}
    assert "_meta" not in capabilities
    assert sink.reports == []


def test_report_source_id_is_bounded_and_length_prefixes_ambiguous_components() -> None:
    first = acp_standard_report_source_id("a:b", "c", "d", 1)
    second = acp_standard_report_source_id("a", "b:c", "d", 1)

    assert first.startswith("acp2:")
    assert len(first) == 69
    assert first != second
    assert first == acp_standard_report_source_id("a:b", "c", "d", 1)


@pytest.mark.parametrize("sequence", [0, -1, True])
def test_report_source_id_requires_a_positive_non_boolean_sequence(sequence: int) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        acp_standard_report_source_id("server", "root", "epoch", sequence)


def test_report_source_id_accepts_opaque_multibyte_identity_values() -> None:
    result = acp_standard_report_source_id("服務器", "根🙂", "epoch/child:1", 23)

    assert len(result.encode("ascii")) == 69


def test_child_key_uses_an_unambiguous_scoped_encoding() -> None:
    assert acp_standard_child_key("a:b", "c", "d") != acp_standard_child_key("a", "b:c", "d")
    assert acp_standard_child_key("parent", "epoch", "child") == acp_standard_child_key(
        "parent", "epoch", "child"
    )


def test_association_frame_retains_sparse_fields_and_outer_immediate_parent() -> None:
    decoded = read_standard_subagent_frame(
        _frame(
            "native-parent",
            {
                "sessionUpdate": "subagent_update",
                "sessionId": "native-child",
                "title": None,
                "cwd": "/workspace/child",
            },
        )
    )

    assert decoded.recognized is True
    assert decoded.error is None
    assert decoded.operation is not None
    assert decoded.operation.kind == "association.patch"
    assert decoded.operation.target_session_id == "native-child"
    assert decoded.operation.parent_session_id == "native-parent"
    assert decoded.operation.operation == {
        "session_id": "native-child",
        "parent_session_id": "native-parent",
        "patch": {"title": None, "cwd": "/workspace/child"},
    }


def test_association_frame_rejects_invalid_values_without_losing_ownership() -> None:
    decoded = read_standard_subagent_frame(
        _frame(
            "native-parent",
            {
                "sessionUpdate": "subagent_update",
                "sessionId": "native-child",
                "cwd": "relative/path",
            },
        )
    )

    assert decoded.recognized is True
    assert decoded.error == "standard ACP association cwd must be an absolute path or null"
    assert decoded.target_session_id == "native-child"
    assert decoded.parent_session_id == "native-parent"


@pytest.mark.parametrize(
    ("params", "expected_error"),
    [
        (
            {"update": {"sessionUpdate": "subagent_update", "sessionId": "child"}},
            "no enclosing sessionId",
        ),
        (
            {"sessionId": "parent", "update": {"sessionUpdate": "subagent_update"}},
            "no child sessionId",
        ),
        ({"sessionId": "parent", "update": {"sessionUpdate": "session_message"}}, "no messageId"),
        (
            {
                "sessionId": "parent",
                "update": {
                    "sessionUpdate": "session_message_chunk",
                    "messageId": "m1",
                    "content": "not-object",
                },
            },
            "invalid content",
        ),
    ],
)
def test_recognized_malformed_frames_fail_closed(params, expected_error: str) -> None:
    decoded = read_standard_subagent_frame(params)

    assert decoded.recognized is True
    assert expected_error in (decoded.error or "")
    assert decoded.operation is None


def test_unknown_update_is_not_claimed_by_standard_preview() -> None:
    decoded = read_standard_subagent_frame(
        _frame("native-parent", {"sessionUpdate": "future_preview_update", "value": 1})
    )

    assert decoded.recognized is False
    assert decoded.error is None


def test_directed_message_patch_keeps_raw_content_and_explicit_nulls() -> None:
    content = [{"type": "text", "text": "worker result"}, {"type": "image", "uri": "file:test"}]
    decoded = read_standard_subagent_frame(
        _frame(
            "native-child",
            {
                "sessionUpdate": "session_message",
                "messageId": "native-message",
                "senderSessionId": "native-child",
                "recipientSessionId": None,
                "senderMessageId": "native-parent-message",
                "recipientMessageId": None,
                "content": content,
                "_meta": {"future": "retained"},
            },
        )
    )

    assert decoded.operation is not None
    assert decoded.operation.kind == "entity.patch"
    assert decoded.operation.operation == {
        "entity_type": "message",
        "entity_id": "native-message",
        "patch": {
            "role": "assistant",
            "acp_agent_identity": "unknown",
            "content": content,
            "metadata": {"future": "retained"},
            "acp_session_message": {
                "kind": "directed",
                "sender_session_id": "native-child",
                "recipient_session_id": None,
                "sender_message_id": "native-parent-message",
                "recipient_message_id": None,
            },
        },
    }


def test_chunk_before_upsert_preserves_native_message_id_and_repeated_content() -> None:
    chunk = {"type": "text", "text": "same chunk"}
    decoded = read_standard_subagent_frame(
        _frame(
            "native-child",
            {
                "sessionUpdate": "session_message_chunk",
                "messageId": "native-message",
                "content": chunk,
            },
        )
    )

    assert decoded.operation is not None
    assert decoded.operation.kind == "entity.append"
    assert decoded.operation.operation == {
        "entity_type": "message",
        "entity_id": "native-message",
        "content": [chunk],
    }
    repeated = read_standard_subagent_frame(
        _frame(
            "native-child",
            {
                "sessionUpdate": "session_message_chunk",
                "messageId": "native-message",
                "content": chunk,
            },
        )
    )
    assert repeated.operation == decoded.operation


def test_unknown_message_fields_are_preserved_as_opaque_protocol_extensions() -> None:
    decoded = read_standard_subagent_frame(
        _frame(
            "native-child",
            {
                "sessionUpdate": "session_message",
                "messageId": "m1",
                "futureField": {"shape": [1, 2]},
            },
        )
    )

    assert decoded.operation is not None
    patch = decoded.operation.operation["patch"]
    assert patch["protocol_extensions"] == {"futureField": {"shape": [1, 2]}}


def test_tool_references_distinguish_omitted_from_explicit_clear() -> None:
    assert read_session_tool_references({}) is None
    assert read_session_tool_references({"content": None}) == ()
    assert read_session_tool_references(
        {
            "content": [
                {"type": "text", "text": "details"},
                {"type": "session", "sessionId": "child-a"},
                {"type": "session", "sessionId": "child-b"},
            ]
        }
    ) == ("child-a", "child-b")


@pytest.mark.parametrize(
    "update",
    [
        {"content": "not-array"},
        {"content": [{"type": "session"}]},
        {"content": [{"type": "session", "sessionId": ""}]},
    ],
)
def test_invalid_tool_references_raise(update: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        read_session_tool_references(update)


def test_session_owned_action_preserves_outer_owner_and_rpc_identity() -> None:
    decoded = read_standard_session_action(
        {
            "jsonrpc": "2.0",
            "id": 17,
            "method": "session/request_permission",
            "params": {"sessionId": "native-child", "_meta": {"untrusted": True}},
        }
    )

    assert decoded.recognized is True
    assert decoded.error is None
    assert decoded.identity == AcpSessionActionIdentity(
        target_session_id="native-child",
        request_id=17,
        method="session/request_permission",
    )


@pytest.mark.parametrize("request_id", [True, None, [], {}])
def test_invalid_action_id_is_claimed_and_rejected(request_id: object) -> None:
    decoded = read_standard_session_action(
        {
            "id": request_id,
            "method": "session/request_permission",
            "params": {"sessionId": "child"},
        }
    )

    assert decoded.recognized is True
    assert decoded.identity is None
    assert "invalid JSON-RPC id" in (decoded.error or "")


def test_request_without_outer_session_id_is_not_claimed_as_child_action() -> None:
    decoded = read_standard_session_action(
        {"id": 1, "method": "session/request_permission", "params": {"path": "/tmp/file"}}
    )

    assert decoded.recognized is False
    assert decoded.identity is None


class _SessionReportSink:
    protocol_version = 3
    features = tuple(sorted(REPORT_FEATURES))
    server_identity = "server-test"
    root_conversation_id = "conversation-test"

    def __init__(self, root: Path) -> None:
        self.reports: list[SessionReport] = []
        self.journal = SessionReportJournal(
            self.root_conversation_id,
            server_identity=self.server_identity,
            subscriber_generation="executor-contract-test",
            root=root / "session-reports",
        )
        self.journal.acquire_owner()

    def begin_native_epoch(self, native_epoch: str) -> None:
        self.journal.begin_native_epoch(native_epoch)

    def bind_native_session(self, native_epoch: str, native_session_id: str) -> None:
        self.journal.bind_native_session(native_epoch, native_session_id)

    def append(self, report: SessionReport) -> SessionReportAppendResult:
        result = self.journal.append(report)
        self.reports.append(report)
        return result


def _enabled_executor(root: Path) -> tuple[AcpExecutor, _SessionReportSink]:
    executor = AcpExecutor(AcpAgentConfig(command="unused"))
    sink = _SessionReportSink(root)
    sink.begin_native_epoch("epoch-test")
    sink.bind_native_session("epoch-test", "root-native")
    executor.enable_standard_subagents(REPORT_FEATURES, ACTION_FEATURES)
    executor.set_session_report_sink(sink)
    executor._session_reports_negotiated = True
    executor._native_epoch = "epoch-test"
    executor._session_id = "root-native"
    executor._standard_associations["root-native"] = _StandardSessionAssociation(
        parent_session_id=None,
        association_source_id="root-source",
        origin=None,
    )
    executor.bind_session_action_origin(
        "trusted-origin-token",
        "root-agent",
        "origin-generation",
        "context-generation",
        response_id="response-root",
    )
    return executor, sink


@pytest.mark.asyncio
async def test_executor_assigns_distinct_ingress_ids_to_expanded_operations_and_replay(
    tmp_path: Path,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    association = {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "root-native",
            "update": {
                "sessionUpdate": "subagent_update",
                "sessionId": "child-native",
                "title": "worker",
                "state": {"state": "running"},
            },
        },
    }

    assert await executor._dispatch_session_report(association) is True
    first_pass = list(sink.reports)
    assert [report.kind for report in first_pass] == [
        "association.patch",
        "observation.patch",
    ]
    assert [report.ingress_sequence for report in first_pass] == [1, 2]
    assert len({report.source_id for report in first_pass}) == 2
    assert all(report.source_id.startswith("acp2:") for report in first_pass)
    assert first_pass[0].data["server_identity"] == sink.server_identity
    assert first_pass[0].data["root_conversation_id"] == sink.root_conversation_id
    assert first_pass[0].data["root_native_session_id"] == "root-native"
    assert first_pass[0].data["target_session_id"] == "child-native"
    assert first_pass[0].data["parent_session_id"] == "root-native"
    assert first_pass[0].data["_omnigent_action_origin"] == {
        "token": "trusted-origin-token",
        "agent": "root-agent",
        "generation": "origin-generation",
    }

    # Replaying the same native operation is a new ingress operation. The
    # journal's source identity, not payload hashing, is what makes a retry
    # idempotent after the append boundary.
    assert await executor._dispatch_session_report(association) is True
    assert [report.ingress_sequence for report in sink.reports] == [1, 2, 3, 4]
    assert len({report.source_id for report in sink.reports}) == 4


@pytest.mark.asyncio
async def test_executor_demultiplexes_nested_children_and_scopes_native_message_ids(
    tmp_path: Path,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    for parent, child, title in (
        ("root-native", "child-a", "worker"),
        ("root-native", "child-b", "worker"),
        ("child-a", "grandchild", "worker"),
    ):
        assert await executor._dispatch_session_report(
            {
                "method": "session/update",
                "params": {
                    "sessionId": parent,
                    "update": {
                        "sessionUpdate": "subagent_update",
                        "sessionId": child,
                        "title": title,
                    },
                },
            }
        )

    for child in ("child-a", "child-b"):
        assert await executor._dispatch_session_report(
            {
                "method": "session/update",
                "params": {
                    "sessionId": child,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "same-native-message",
                        "content": {"type": "text", "text": f"from {child}"},
                    },
                },
            }
        )

    child_messages = [
        report
        for report in sink.reports
        if report.kind == "entity.append"
        and report.data["operation"].get("entity_id") == "same-native-message"
    ]
    assert len(child_messages) == 2
    assert {report.data["target_session_id"] for report in child_messages} == {
        "child-a",
        "child-b",
    }
    assert {report.data["parent_session_id"] for report in child_messages} == {"root-native"}
    assert [report.data["operation"]["content"][0]["text"] for report in child_messages] == [
        "from child-a",
        "from child-b",
    ]

    grandchildren = [
        report
        for report in sink.reports
        if report.kind == "association.patch" and report.data["target_session_id"] == "grandchild"
    ]
    assert len(grandchildren) == 1
    assert grandchildren[0].data["parent_session_id"] == "child-a"


@pytest.mark.asyncio
async def test_executor_idle_root_unknown_state_is_recorded_without_false_completion(
    tmp_path: Path,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    executor._active_prompt_request_id = None
    update = {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "root-native",
            "update": {"sessionUpdate": "session_state_update", "state": {"state": "future"}},
        },
    }

    assert await executor._dispatch_session_report(update) is True
    assert sink.reports[-1].kind == "observation.patch"
    assert sink.reports[-1].data["operation"] == {
        "observation": {"state": {"present": True, "value": {"state": "future"}}}
    }

    missing_state = {
        "params": {
            "sessionId": "root-native",
            "update": {"sessionUpdate": "session_state_update"},
        }
    }
    assert await executor._dispatch_session_report(missing_state) is True
    assert sink.reports[-1].data["operation"] == {"observation": {"state": {"present": False}}}


@pytest.mark.asyncio
async def test_executor_preserves_legacy_and_preview_plan_updates_and_removal(
    tmp_path: Path,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    assert await executor._dispatch_session_report(
        {
            "params": {
                "sessionId": "root-native",
                "update": {
                    "sessionUpdate": "subagent_update",
                    "sessionId": "child-native",
                    "title": "worker",
                },
            }
        }
    )

    legacy_entries = [
        {"content": "Inspect the fixture", "priority": "high", "status": "in_progress"}
    ]
    preview_plan = {
        "type": "items",
        "planId": "worker-plan",
        "entries": [{"content": "Inspect the fixture", "priority": "high", "status": "completed"}],
    }
    updates = (
        (
            {"sessionUpdate": "plan", "entries": legacy_entries},
            {"observation": {"plan": {"entries": legacy_entries}}},
        ),
        (
            {"sessionUpdate": "plan_update", "plan": preview_plan},
            {"observation": {"plan": {"plan": preview_plan}}},
        ),
        (
            {"sessionUpdate": "plan_removed", "planId": "worker-plan"},
            {"observation": {"plan": None}},
        ),
    )
    for update, expected in updates:
        assert await executor._dispatch_session_report(
            {"params": {"sessionId": "child-native", "update": update}}
        )
        report = sink.reports[-1]
        assert report.kind == "observation.patch"
        assert report.data["target_session_id"] == "child-native"
        assert report.data["operation"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reader_result", "expected_state", "expected_reason"),
    [
        (b"", "disconnected", "stdout_eof"),
        (OSError("reader failed"), "unconfirmed", "stdout_reader_error"),
    ],
)
async def test_stdout_loss_records_uncertainty_for_root_and_known_children_before_failure(
    tmp_path: Path,
    reader_result: bytes | OSError,
    expected_state: str,
    expected_reason: str,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    executor._standard_associations["child-native"] = _StandardSessionAssociation(
        parent_session_id="root-native",
        association_source_id="child-association-source",
        origin=("trusted-origin-token", "root-agent", "origin-generation", "context-generation"),
    )

    class ScriptedStdout:
        async def readline(self) -> bytes:
            if isinstance(reader_result, OSError):
                raise reader_result
            return reader_result

    executor._proc = SimpleNamespace(stdout=ScriptedStdout(), returncode=0)  # type: ignore[assignment]
    await executor._read_stdout()
    if executor._report_failure_shutdown_task is not None:
        await executor._report_failure_shutdown_task

    diagnostic_reports = sink.reports
    assert [report.data["target_session_id"] for report in diagnostic_reports] == [
        "root-native",
        "child-native",
    ]
    for report in diagnostic_reports:
        assert report.kind == "observation.patch"
        assert report.data["operation"] == {
            "observation": {
                "state": {"present": False},
                "transport": {"state": expected_state, "reason": expected_reason},
            }
        }
    status = executor.session_report_status()
    assert status is not None and status.state == "failed"
    # Both uncertainty snapshots have crossed the durable append boundary before
    # the producer publishes its failed source status.
    assert len(sink.reports) == 2
    executor._proc = None
    await executor.close()


@pytest.mark.asyncio
async def test_numeric_and_string_child_request_ids_keep_separate_policy_and_response_identity(
    tmp_path: Path,
) -> None:
    executor, _sink = _enabled_executor(tmp_path)
    assert await executor._dispatch_session_report(
        {
            "params": {
                "sessionId": "root-native",
                "update": {
                    "sessionUpdate": "subagent_update",
                    "sessionId": "child-native",
                    "title": "worker",
                },
            }
        }
    )

    policy_tools: list[str] = []
    bindings: list[AcpSessionActionBinding] = []
    responses: list[dict[str, object]] = []

    async def evaluate_policy(_phase: str, data: dict[str, object]) -> object:
        tool_call = data.get("arguments")
        assert isinstance(tool_call, dict)
        policy_tools.append(str(tool_call.get("tool")))
        return SimpleNamespace(action="POLICY_ACTION_ALLOW")

    async def capture_send(message: dict[str, object]) -> None:
        responses.append(message)

    async def handle_request(request: dict[str, object], binding: AcpSessionActionBinding) -> None:
        bindings.append(binding)
        await executor._respond_to_agent_request(request)  # type: ignore[arg-type]

    executor._policy_evaluator = evaluate_policy  # type: ignore[assignment]
    executor._session_action_request_handler = handle_request  # type: ignore[assignment]
    executor._send = capture_send  # type: ignore[method-assign]

    requests = [
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "session/request_permission",
            "params": {
                "sessionId": "child-native",
                "toolCall": {"title": "shell", "rawInput": {"tool": label}},
                "options": [
                    {"optionId": "allow-once", "kind": "allow_once"},
                    {"optionId": "reject-once", "kind": "reject_once"},
                ],
            },
        }
        for request_id, label in ((1, "numeric"), ("1", "string"))
    ]
    dispatched = await asyncio.gather(
        *(executor._dispatch_session_action_request(request) for request in requests)
    )
    assert all(dispatched)
    await asyncio.gather(*tuple(executor._detached_request_tasks))

    assert policy_tools == ["numeric", "string"]
    assert [binding.request_id for binding in bindings] == [1, "1"]
    assert [binding.request_invocation_id for binding in bindings] == sorted(
        {binding.request_invocation_id for binding in bindings}
    )
    assert bindings[0].request_invocation_id < bindings[1].request_invocation_id
    assert [response["id"] for response in responses] == [1, "1"]
    assert [response["result"]["outcome"]["optionId"] for response in responses] == [
        "allow-once",
        "allow-once",
    ]
    executor._proc = None
    await executor.close()


@pytest.mark.asyncio
async def test_reused_raw_child_request_id_gets_new_invocation_and_rechecks_revoked_origin(
    tmp_path: Path,
) -> None:
    """A settled raw id may be reused, with policy re-evaluated for the new request."""
    executor, _sink = _enabled_executor(tmp_path)
    assert await executor._dispatch_session_report(
        {
            "params": {
                "sessionId": "root-native",
                "update": {
                    "sessionUpdate": "subagent_update",
                    "sessionId": "child-native",
                    "title": "worker",
                },
            }
        }
    )

    policy_actions = iter(("POLICY_ACTION_ALLOW", "POLICY_ACTION_DENY"))
    policy_phases: list[str] = []
    bindings: list[AcpSessionActionBinding] = []
    writes: list[str] = []
    responses: list[dict[str, object]] = []

    async def evaluate_policy(phase: str, _data: dict[str, object]) -> object:
        policy_phases.append(phase)
        return SimpleNamespace(action=next(policy_actions), reason="origin was revoked")

    async def write_file(params: dict[str, object], **_kwargs: object) -> dict[str, object]:
        writes.append(str(params["content"]))
        return {"written": True}

    async def capture_send(message: dict[str, object]) -> None:
        responses.append(message)

    async def handle_request(request: dict[str, object], binding: AcpSessionActionBinding) -> None:
        bindings.append(binding)
        await executor._respond_to_agent_request(
            request,
            detached_child_action=True,  # type: ignore[arg-type]
        )

    executor._policy_evaluator = evaluate_policy  # type: ignore[assignment]
    executor._fs_delegation = True
    executor._handle_fs_write = write_file  # type: ignore[method-assign]
    executor._session_action_request_handler = handle_request  # type: ignore[method-assign]
    executor._send = capture_send  # type: ignore[method-assign]

    async def submit(content: str) -> None:
        request = {
            "jsonrpc": "2.0",
            "id": 41,
            "method": "fs/write_text_file",
            "params": {
                "sessionId": "child-native",
                "path": "/workspace/child/result.txt",
                "content": content,
            },
        }
        assert await executor._dispatch_session_action_request(request) is True
        await asyncio.gather(*tuple(executor._detached_request_tasks))

    await submit("first invocation")
    await submit("after policy revocation")

    assert [binding.request_id for binding in bindings] == [41, 41]
    assert bindings[0].request_invocation_id < bindings[1].request_invocation_id
    assert policy_phases == ["ACP_ACTION_ORIGIN_CHECK", "ACP_ACTION_ORIGIN_CHECK"]
    assert writes == ["first invocation"]
    assert [response["id"] for response in responses] == [41, 41]
    assert responses[0].get("result") == {"written": True}
    assert responses[1].get("error", {}).get("code") == -32603
    assert "origin is no longer valid" in responses[1]["error"]["message"]
    executor._proc = None
    await executor.close()


@pytest.mark.asyncio
async def test_executor_rejects_reparent_and_unknown_child_update_without_append(
    tmp_path: Path,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    assert await executor._dispatch_session_report(
        {
            "params": {
                "sessionId": "root-native",
                "update": {"sessionUpdate": "subagent_update", "sessionId": "another-parent"},
            }
        }
    )
    announce = {
        "params": {
            "sessionId": "root-native",
            "update": {"sessionUpdate": "subagent_update", "sessionId": "child-native"},
        }
    }
    assert await executor._dispatch_session_report(announce)
    count = len(sink.reports)

    with pytest.raises(RuntimeError, match="reparent"):
        await executor._dispatch_session_report(
            {
                "params": {
                    "sessionId": "another-parent",
                    "update": {"sessionUpdate": "subagent_update", "sessionId": "child-native"},
                }
            }
        )
    with pytest.raises(RuntimeError, match="unannounced native session"):
        await executor._dispatch_session_report(
            {
                "params": {
                    "sessionId": "unannounced-child",
                    "update": {
                        "sessionUpdate": "session_message_chunk",
                        "messageId": "m1",
                        "content": {"type": "text", "text": "no"},
                    },
                }
            }
        )
    assert len(sink.reports) == count


@pytest.mark.asyncio
async def test_child_action_uses_captured_origin_after_same_context_credential_refresh(
    tmp_path: Path,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    await executor._dispatch_session_report(
        {
            "params": {
                "sessionId": "root-native",
                "update": {
                    "sessionUpdate": "subagent_update",
                    "sessionId": "child-native",
                    "cwd": "/workspace/child",
                    "additionalDirectories": ["/workspace/shared"],
                },
            }
        }
    )
    association_report = sink.reports[0]

    # A refreshed bearer token and turn generation may change while the
    # security context fingerprint remains the same. Existing child work keeps
    # the origin captured when its association was first announced.
    executor.bind_session_action_origin(
        "refreshed-token",
        "refreshed-agent",
        "refreshed-origin-generation",
        "context-generation",
        response_id="response-refreshed",
    )
    captured: list[AcpSessionActionBinding] = []
    handled = asyncio.Event()

    async def record_binding(
        _request: dict[str, object], binding: AcpSessionActionBinding
    ) -> None:
        captured.append(binding)
        handled.set()

    executor._session_action_request_handler = record_binding  # type: ignore[method-assign]
    request = {
        "jsonrpc": "2.0",
        "id": 29,
        "method": "session/request_permission",
        "params": {
            "sessionId": "child-native",
            "_meta": {"origin_token": "request-controlled-token"},
        },
    }

    assert await executor._dispatch_session_action_request(request) is True
    await asyncio.wait_for(handled.wait(), timeout=1.0)
    await asyncio.sleep(0)
    binding = captured[0]

    assert association_report.data["_omnigent_action_origin"] == {
        "token": "trusted-origin-token",
        "agent": "root-agent",
        "generation": "origin-generation",
    }
    assert binding.root_conversation_id == sink.root_conversation_id
    assert binding.native_epoch == "epoch-test"
    assert binding.context_generation == "context-generation"
    assert binding.root_native_session_id == "root-native"
    assert binding.target_session_id == "child-native"
    assert binding.parent_session_id == "root-native"
    assert binding.association_source_id == association_report.source_id
    assert binding.origin_token == "trusted-origin-token"
    assert binding.origin_agent == "root-agent"
    assert binding.origin_generation == "origin-generation"
    assert binding.cwd == "/workspace/child"
    assert binding.additional_directories == ("/workspace/shared",)
    assert executor._detached_request_tasks == set()

    with pytest.raises(RuntimeError, match="different trusted security context"):
        executor.bind_session_action_origin(
            "different-caller-token",
            "different-agent",
            "different-generation",
            "different-context-generation",
            response_id="response-different-context",
        )


@pytest.mark.asyncio
async def test_child_action_captures_effective_filesystem_overrides_at_ingress(
    tmp_path: Path,
) -> None:
    executor, _sink = _enabled_executor(tmp_path)
    for parent_id, child_id, patch in (
        (
            "root-native",
            "parent-native",
            {
                "cwd": "/workspace/parent",
                "additionalDirectories": ["/workspace/parent-shared"],
            },
        ),
        ("parent-native", "child-native", {"cwd": None, "additionalDirectories": None}),
        ("child-native", "independent-native", {"cwd": "/workspace/independent"}),
        ("child-native", "empty-native", {"additionalDirectories": []}),
    ):
        assert await executor._dispatch_session_report(
            {
                "method": "session/update",
                "params": {
                    "sessionId": parent_id,
                    "update": {
                        "sessionUpdate": "subagent_update",
                        "sessionId": child_id,
                        **patch,
                    },
                },
            }
        )

    captured = []
    delivered = asyncio.Event()
    release_empty_child = asyncio.Event()

    async def record_binding(
        _request: dict[str, object], binding: AcpSessionActionBinding
    ) -> None:
        captured.append(binding)
        delivered.set()
        if binding.target_session_id == "empty-native":
            await release_empty_child.wait()

    executor._session_action_request_handler = record_binding  # type: ignore[method-assign]

    async def capture_binding(target_session_id: str, request_id: int) -> AcpSessionActionBinding:
        delivered.clear()
        assert await executor._dispatch_session_action_request(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "fs/read_text_file",
                "params": {"sessionId": target_session_id, "path": "file.txt"},
            }
        )
        await asyncio.wait_for(delivered.wait(), timeout=1.0)
        if target_session_id != "empty-native" and executor._detached_request_tasks:
            await asyncio.gather(*tuple(executor._detached_request_tasks))
        return captured[-1]

    null_child = await capture_binding("child-native", 1)
    independent_child = await capture_binding("independent-native", 2)
    empty_child = await capture_binding("empty-native", 3)

    assert (null_child.cwd_present, null_child.cwd) == (True, "/workspace/parent")
    assert (null_child.additional_directories_present, null_child.additional_directories) == (
        True,
        ("/workspace/parent-shared",),
    )
    assert (independent_child.cwd_present, independent_child.cwd) == (
        True,
        "/workspace/independent",
    )
    assert (
        independent_child.additional_directories_present,
        independent_child.additional_directories,
    ) == (True, ("/workspace/parent-shared",))
    assert (empty_child.cwd_present, empty_child.cwd) == (True, "/workspace/parent")
    assert (empty_child.additional_directories_present, empty_child.additional_directories) == (
        True,
        (),
    )

    # The request owns its ingress-time snapshot even if an ancestor is patched
    # before the detached callback finishes.
    assert await executor._dispatch_session_report(
        {
            "method": "session/update",
            "params": {
                "sessionId": "root-native",
                "update": {
                    "sessionUpdate": "subagent_update",
                    "sessionId": "parent-native",
                    "cwd": "/workspace/retargeted",
                    "additionalDirectories": ["/workspace/retargeted-shared"],
                },
            },
        }
    )
    release_empty_child.set()
    if executor._detached_request_tasks:
        await asyncio.gather(*tuple(executor._detached_request_tasks))
    assert empty_child.cwd == "/workspace/parent"
    assert empty_child.additional_directories == ()


@pytest.mark.parametrize(
    ("message_order", "expected_origin", "expected_response_received"),
    [
        ("response-first", "trusted-origin-token", True),
        ("notification-first", "active-turn-token", False),
    ],
)
@pytest.mark.asyncio
async def test_root_association_uses_prompt_origin_at_response_boundary(
    tmp_path: Path,
    message_order: str,
    expected_origin: str,
    expected_response_received: bool,
) -> None:
    executor, sink = _enabled_executor(tmp_path)
    executor.bind_session_action_origin(
        "active-turn-token",
        "active-turn-agent",
        "active-turn-generation",
        "context-generation",
        response_id="response-active-turn",
    )
    executor._active_prompt_request_id = 41
    prompt_future = asyncio.get_running_loop().create_future()
    executor._pending[41] = prompt_future
    response = {"jsonrpc": "2.0", "id": 41, "result": {"stopReason": "end_turn"}}
    notification = {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": "root-native",
            "update": {
                "sessionUpdate": "subagent_update",
                "sessionId": "child-native",
                "title": "worker",
            },
        },
    }
    messages = (
        [response, notification] if message_order == "response-first" else [notification, response]
    )

    class ScriptedStdout:
        def __init__(self) -> None:
            self.lines = [json.dumps(message).encode("utf-8") + b"\n" for message in messages]

        async def readline(self) -> bytes:
            if self.lines:
                return self.lines.pop(0)
            await asyncio.Future()
            return b""

    association_seen = asyncio.Event()
    received_at_association: list[bool] = []
    dispatch_report = executor._dispatch_session_report

    async def record_ordered_report(message: dict[str, object]) -> bool:
        received_at_association.append(executor._active_prompt_response_received)
        result = await dispatch_report(message)
        association_seen.set()
        return result

    executor._dispatch_session_report = record_ordered_report  # type: ignore[method-assign]
    executor._proc = SimpleNamespace(stdout=ScriptedStdout())  # type: ignore[assignment]
    reader = asyncio.create_task(executor._read_stdout())
    try:
        await asyncio.wait_for(association_seen.wait(), timeout=1.0)
    finally:
        reader.cancel()
        await reader
        executor._proc = None
        await executor.close()

    association_report = next(
        report for report in sink.reports if report.kind == "association.patch"
    )
    assert association_report.data["_omnigent_action_origin"]["token"] == expected_origin
    assert received_at_association == [expected_response_received]
    assert prompt_future.result() == response
