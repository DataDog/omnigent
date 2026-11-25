"""Runner consumption of the durable standard ACP report protocol."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest

from omnigent.inner.acp_standard_subagents import (
    ACTION_FEATURES,
    REPORT_FEATURES,
    acp_standard_report_source_id,
    acp_standard_tool_entity_id,
)
from omnigent.runner.acp_session_actions import AcpSessionActionSubscriber
from omnigent.runner.acp_session_reports import AcpSessionReportSubscriber, acp_session_child_key

_PARENT_ID = "parent-report-test"
_SERVER_IDENTITY = "server-report-test"
_GENERATION = "subscriber-generation-test"
_CONTEXT = "context-generation-test"
_EPOCH = "native-epoch-test"
_JOURNAL_ID = "a" * 32
_CONNECTION_ADMISSION_ID = "connection-admission-test"
_RUNTIME_INCARNATION = 1
_ROOT_NATIVE = "native-root-test"
_CHILD_NATIVE = "native-child-test"


class _ReportServer:
    """HTTP contract double that models durable writes and lost responses."""

    def __init__(self) -> None:
        self.reports: list[dict[str, Any]] = []
        self.attempts: list[dict[str, Any]] = []
        self.persisted: dict[str, dict[str, Any]] = {}
        self.events: list[tuple[str, Any]] = []
        self.children: dict[str, dict[str, Any]] = {}
        self.acknowledged = 0
        self.fail_after_commit: Callable[[dict[str, Any]], bool] | None = None
        self.failed_sources: set[str] = set()
        self.source_status: dict[str, Any] = {"state": "initialized", "prompt_started": False}
        self.report_page_size: int | None = None
        self.fail_reports_after: int | None = None
        self.associations: dict[str, dict[str, Any]] = {}
        self.native_epoch = _EPOCH
        self.epoch_ordinal = 1
        self.source_events: list[dict[str, Any]] = []
        self.accepted_source_events: list[dict[str, Any]] = []
        self.fail_epoch_controls: set[tuple[str, str | None]] = set()
        self.capabilities_gate: asyncio.Event | None = None

    def capabilities(self) -> dict[str, Any]:
        return {
            "protocol": "acp-session-reports",
            "version": 3,
            "features": sorted(REPORT_FEATURES | ACTION_FEATURES),
            "subscriber_generation": _GENERATION,
            "journal_id": _JOURNAL_ID,
            "acknowledged_sequence": self.acknowledged,
            "highest_sequence": len(self.reports),
            "native_epoch": self.native_epoch,
            "epoch_ordinal": self.epoch_ordinal if self.native_epoch is not None else None,
            "source_status": self.source_status,
        }

    async def harness_request(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/reports/capabilities"):
            if self.capabilities_gate is not None:
                await self.capabilities_gate.wait()
            return httpx.Response(200, json=self.capabilities())
        if request.method == "GET" and request.url.path.endswith("/reports"):
            after = int(request.url.params["after"])
            records = [record for record in self.reports if record["sequence"] > after]
            if not records and int(request.url.params.get("wait_ms", "0")) > 0:
                await asyncio.sleep(0.001)
            if self.fail_reports_after == after:
                self.fail_reports_after = None
                return httpx.Response(503, json={"error": "temporary report page failure"})
            if self.report_page_size is not None:
                records = records[: self.report_page_size]
            more = bool(records) and any(
                record["sequence"] > records[-1]["sequence"] for record in self.reports
            )
            return httpx.Response(
                200, json={**self.capabilities(), "records": records, "more": more}
            )
        if request.method == "POST" and request.url.path.endswith("/reports/ack"):
            payload = json.loads(request.content)
            assert payload["subscriber_generation"] == _GENERATION
            self.acknowledged = max(self.acknowledged, payload["through_sequence"])
            self.events.append(("ack", self.acknowledged))
            return httpx.Response(200, json={"acknowledged_sequence": self.acknowledged})
        return httpx.Response(404, request=request)

    async def server_request(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        if payload.get("type") in {
            "external_acp_report_owner_admit",
            "external_acp_report_epoch_activate",
        }:
            self.source_events.append(payload)
            if payload["type"] == "external_acp_report_epoch_activate":
                data = payload["data"]
                failed_control = (data["native_epoch"], data.get("mode"))
                if failed_control in self.fail_epoch_controls:
                    self.fail_epoch_controls.remove(failed_control)
                    return httpx.Response(
                        503, json={"error": "temporary epoch binding failure"}, request=request
                    )
            self.accepted_source_events.append(payload)
            return httpx.Response(200, json={"accepted": True}, request=request)
        assert payload["type"] == "external_acp_session_report"
        report = payload["data"]
        assert report["connection_admission_id"] == _CONNECTION_ADMISSION_ID
        assert report["runtime_incarnation"] == _RUNTIME_INCARNATION
        source_id = report["source_id"]
        self.attempts.append(report)
        self.persisted.setdefault(source_id, report)
        self.events.append(("report", report["kind"]))
        response: dict[str, Any] = {"durable": True}
        if report["kind"] == "association.patch":
            root_key = f"root:{report['native_epoch']}"
            self.associations.setdefault(
                root_key,
                {
                    "native_epoch": report["native_epoch"],
                    "native_session_id": report["root_native_session_id"],
                    "parent_native_session_id": None,
                    "conversation_id": report["root_conversation_id"],
                    "context_generation": report["context_generation"],
                    "association_source_id": None,
                    "action_origin": None,
                },
            )
            key = acp_session_child_key(
                report["root_conversation_id"],
                report["native_epoch"],
                report["target_session_id"],
            )
            child = self.children.setdefault(
                key,
                {
                    "id": f"child-{len(self.children) + 1}",
                    "title": f"worker:{key}",
                    "native_epoch": report["native_epoch"],
                    "native_session_id": report["target_session_id"],
                    "association_source_id": source_id,
                    "context_generation": report["context_generation"],
                    "action_origin": report.get("_omnigent_action_origin"),
                },
            )
            self.associations.setdefault(
                key,
                {
                    "native_epoch": report["native_epoch"],
                    "native_session_id": report["target_session_id"],
                    "parent_native_session_id": report["parent_session_id"],
                    "conversation_id": child["id"],
                    "context_generation": report["context_generation"],
                    "association_source_id": source_id,
                    "action_origin": report.get("_omnigent_action_origin"),
                },
            )
            response["child_session_id"] = child["id"]
        if (
            source_id not in self.failed_sources
            and self.fail_after_commit is not None
            and self.fail_after_commit(report)
        ):
            self.failed_sources.add(source_id)
            return httpx.Response(500, json={"error": "response lost after commit"})
        return httpx.Response(200, json=response)


def _report(
    sequence: int,
    kind: str,
    operation: dict[str, Any],
    *,
    target: str = _CHILD_NATIVE,
    parent: str | None = _ROOT_NATIVE,
    action_origin: dict[str, Any] | None = None,
    native_epoch: str = _EPOCH,
    epoch_ordinal: int = 1,
    ingress_sequence: int | None = None,
    context_generation: str = _CONTEXT,
    root_native_session_id: str = _ROOT_NATIVE,
) -> dict[str, Any]:
    report_ingress_sequence = sequence if ingress_sequence is None else ingress_sequence
    source_id = acp_standard_report_source_id(
        _SERVER_IDENTITY, _PARENT_ID, native_epoch, report_ingress_sequence
    )
    data: dict[str, Any] = {
        "server_identity": _SERVER_IDENTITY,
        "root_conversation_id": _PARENT_ID,
        "context_generation": context_generation,
        "root_native_session_id": root_native_session_id,
        "target_session_id": target,
        "parent_session_id": parent,
        "operation": operation,
    }
    if action_origin is not None:
        data["_omnigent_action_origin"] = action_origin
    return {
        "sequence": sequence,
        "journal_id": _JOURNAL_ID,
        "epoch_ordinal": epoch_ordinal,
        "native_epoch": native_epoch,
        "ingress_sequence": report_ingress_sequence,
        "source_id": source_id,
        "kind": kind,
        "data": data,
    }


def _association(
    sequence: int = 1,
    *,
    native_epoch: str = _EPOCH,
    epoch_ordinal: int = 1,
    ingress_sequence: int | None = None,
    target: str = _CHILD_NATIVE,
    parent: str = _ROOT_NATIVE,
    action_origin_token: str = "trusted-action-origin",
) -> dict[str, Any]:
    return _report(
        sequence,
        "association.patch",
        {
            "session_id": target,
            "parent_session_id": parent,
            "patch": {"title": "worker", "state": {"state": "running"}},
        },
        target=target,
        parent=parent,
        native_epoch=native_epoch,
        epoch_ordinal=epoch_ordinal,
        ingress_sequence=ingress_sequence,
        action_origin={
            "token": action_origin_token,
            "agent": "parent-agent",
            "generation": _GENERATION,
        },
    )


@asynccontextmanager
async def _subscriber(
    server: _ReportServer,
    *,
    lease_callback: Callable[[str, str, bool], Awaitable[None]] | None = None,
    connection_admission_id: str = _CONNECTION_ADMISSION_ID,
    runtime_incarnation: int = _RUNTIME_INCARNATION,
) -> AsyncIterator[AcpSessionReportSubscriber]:
    async with (
        httpx.AsyncClient(
            transport=httpx.MockTransport(server.harness_request), base_url="http://harness"
        ) as harness_client,
        httpx.AsyncClient(
            transport=httpx.MockTransport(server.server_request), base_url="http://server"
        ) as server_client,
    ):
        subscriber = AcpSessionReportSubscriber(
            parent_id=_PARENT_ID,
            harness_client=harness_client,
            server_client=server_client,
            generation=_GENERATION,
            connection_admission_id=connection_admission_id,
            runtime_incarnation=runtime_incarnation,
            activity_callback=lambda _parent_id, running: _record(_ACTIVITY, running),
            lease_callback=lease_callback
            or (lambda _parent_id, _child_key, running: _record(_LEASES, running)),
            note_activity=lambda _parent_id: None,
            lookup_children=lambda _parent_id: asyncio.sleep(0, result=server.children),
            lookup_associations=lambda _parent_id: asyncio.sleep(
                0, result=list(server.associations.values())
            ),
            recover_open_child=lambda _key, _child_id, _reason: asyncio.sleep(0),
        )
        try:
            yield subscriber
        finally:
            await subscriber.stop(settle=False)


_ACTIVITY: list[bool] = []
_LEASES: list[bool] = []


async def _record(target: list[bool], value: bool) -> None:
    target.append(value)


@pytest.mark.asyncio
async def test_report_replay_after_commit_before_response_delays_ack_until_retry() -> None:
    server = _ReportServer()
    server.reports = [_association()]
    server.fail_after_commit = lambda report: report["kind"] == "entity.upsert"
    _ACTIVITY.clear()
    _LEASES.clear()

    async with _subscriber(server) as subscriber:
        await subscriber._read_available_reports(wait_ms=0)
        child_key = acp_session_child_key(_PARENT_ID, _EPOCH, _CHILD_NATIVE)
        child_id = server.children[child_key]["id"]
        assert server.acknowledged == 1
        assert subscriber.has_child_session_id(child_id)
        assert subscriber.active_origin_tokens == {"trusted-action-origin"}
        assert _LEASES == [True]
        assert _ACTIVITY == [True]

        server.reports.append(
            _report(
                2,
                "entity.upsert",
                {
                    "entity_type": "message",
                    "entity_id": "child-message",
                    "value": {
                        "item_type": "message",
                        "response_id": "child-response",
                        "item_data": {
                            "role": "assistant",
                            "agent": "unknown",
                            "acp_agent_identity": "unknown",
                            "content": [{"type": "text", "text": "Child result."}],
                        },
                    },
                },
            )
        )
        with pytest.raises(httpx.HTTPStatusError, match="500"):
            await subscriber._read_available_reports(wait_ms=0)
        source_id = server.reports[1]["source_id"]
        assert server.acknowledged == 1
        assert source_id in server.persisted
        assert sum(report["source_id"] == source_id for report in server.attempts) == 1

        await subscriber._read_available_reports(wait_ms=0)
        assert server.acknowledged == 2
        assert sum(report["source_id"] == source_id for report in server.attempts) == 2
        assert server.events.index(("report", "entity.upsert")) < server.events.index(("ack", 2))

        server.reports.append(
            _report(
                3,
                "observation.patch",
                {"observation": {"state": {"present": True, "value": "idle"}}},
            )
        )
        await subscriber._read_available_reports(wait_ms=0)
        assert server.acknowledged == 3
        assert _LEASES == [True, False]
        assert _ACTIVITY == [True, False]


@pytest.mark.asyncio
async def test_start_replays_v3_prefix_and_waits_for_association_ack() -> None:
    server = _ReportServer()
    server.reports = [_association()]
    _ACTIVITY.clear()
    _LEASES.clear()

    async with _subscriber(server) as subscriber:
        await subscriber.start()
        assert subscriber.is_ready
        source_id = server.reports[0]["source_id"]
        child_id = await subscriber.wait_for_association_source(
            source_id,
            native_epoch=_EPOCH,
            target_session_id=_CHILD_NATIVE,
            context_generation=_CONTEXT,
            origin_token="trusted-action-origin",
            timeout_s=0.1,
        )
        child_key = acp_session_child_key(_PARENT_ID, _EPOCH, _CHILD_NATIVE)
        assert child_id == server.children[child_key]["id"]
        assert server.acknowledged == 1
        assert subscriber.active_origin_tokens == {"trusted-action-origin"}


@pytest.mark.asyncio
async def test_validator_accepts_root_reference_updates_after_origin_epoch_retires() -> None:
    """A retired anchor accepts exact references with matching provenance."""
    retired_epoch = _EPOCH
    current_epoch = "native-epoch-current-after-root-tool-origin"
    call_id = "root-tool-reference-after-origin-release"
    origin = {
        "token": "a" * 32,
        "agent": "root-agent",
        "generation": "root-tool-origin-generation",
    }
    server = _ReportServer()
    server.native_epoch = current_epoch
    server.epoch_ordinal = 2
    server.reports = [
        _report(
            1,
            "tool.references",
            {
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "response_id": "original-root-response",
            },
            target=_ROOT_NATIVE,
            parent=None,
            action_origin=origin,
            native_epoch=retired_epoch,
            epoch_ordinal=1,
            root_native_session_id=_ROOT_NATIVE,
        ),
        _report(
            2,
            "tool.references",
            {
                "entity_id": acp_standard_tool_entity_id(call_id),
                "call_id": call_id,
                "references": [],
            },
            target=_ROOT_NATIVE,
            parent=None,
            action_origin=origin,
            native_epoch=retired_epoch,
            epoch_ordinal=1,
            ingress_sequence=2,
            root_native_session_id=_ROOT_NATIVE,
        ),
    ]

    async with _subscriber(server) as subscriber:
        await subscriber.start()

        assert subscriber.is_ready
        assert server.acknowledged == 2
        forwarded = [report for report in server.attempts if report["kind"] == "tool.references"]
        assert len(forwarded) == 2
        assert forwarded[0]["_omnigent_action_origin"] == origin
        assert forwarded[0]["operation"]["response_id"] == "original-root-response"
        assert forwarded[1]["operation"]["references"] == []
        assert forwarded[1]["_omnigent_action_origin"] == origin
        assert [
            (event["data"]["native_epoch"], event["data"].get("mode"))
            for event in server.accepted_source_events
            if event["type"] == "external_acp_report_epoch_activate"
        ] == [(current_epoch, None), (retired_epoch, "historical")]


@pytest.mark.parametrize(
    ("scope", "expected_error"),
    [
        ("root", "different root conversation"),
        ("journal", "different journal lineage"),
        ("epoch", "invalid source identity"),
    ],
)
@pytest.mark.asyncio
async def test_validator_rejects_report_outside_exact_root_journal_or_epoch_scope(
    scope: str,
    expected_error: str,
) -> None:
    server = _ReportServer()
    record = _association()
    if scope == "root":
        record["data"]["root_conversation_id"] = "different-root"
    elif scope == "journal":
        record["journal_id"] = "b" * 32
    elif scope == "epoch":
        record["native_epoch"] = "different-native-epoch"
    server.reports = [record]

    async with _subscriber(server) as subscriber:
        with pytest.raises(RuntimeError, match=expected_error):
            await subscriber.start()


@pytest.mark.asyncio
async def test_reconnect_refreshes_delivery_grant_without_relabeling_history() -> None:
    server = _ReportServer()
    server.reports = [_association()]

    async with _subscriber(server) as subscriber:
        await subscriber.refresh_delivery_owner(_CONNECTION_ADMISSION_ID, 1)
        await subscriber._read_available_reports(wait_ms=0)
        original = server.persisted[server.reports[0]["source_id"]]
        assert server.acknowledged == 1

        refreshed_admission = "connection-admission-reconnected"
        await subscriber.refresh_delivery_owner(refreshed_admission, 1)

        assert server.persisted[original["source_id"]] == original
        assert len(server.attempts) == 1
        assert server.source_events[-2]["data"] == {
            "connection_admission_id": refreshed_admission,
            "runtime_incarnation": 1,
            "journal_id": _JOURNAL_ID,
        }
        assert server.source_events[-1]["data"] == {
            "connection_admission_id": refreshed_admission,
            "runtime_incarnation": 1,
            "journal_id": _JOURNAL_ID,
            "native_epoch": _EPOCH,
            "epoch_ordinal": 1,
        }


@pytest.mark.asyncio
async def test_child_action_waits_out_reconnect_epoch_reactivation() -> None:
    """A child action during a reconnect's epoch refresh waits instead of reading stale."""
    server = _ReportServer()
    server.reports = [_association()]

    async with _subscriber(server) as subscriber:
        await subscriber.refresh_delivery_owner(_CONNECTION_ADMISSION_ID, 1)
        await subscriber._read_available_reports(wait_ms=0)
        assert subscriber.active_native_epoch == _EPOCH

        # Hold the reconnect inside the window where the live epoch is cleared.
        server.capabilities_gate = asyncio.Event()
        refresh = asyncio.create_task(
            subscriber.refresh_delivery_owner("connection-admission-reconnected", 1)
        )
        for _ in range(100):
            if subscriber.active_native_epoch is None:
                break
            await asyncio.sleep(0)
        assert subscriber.active_native_epoch is None
        assert await subscriber.wait_for_live_epoch("other-native-epoch") is False

        waiter = asyncio.create_task(subscriber.wait_for_live_epoch(_EPOCH, timeout_s=5.0))
        await asyncio.sleep(0.05)
        assert not waiter.done()

        server.capabilities_gate.set()
        await refresh
        assert await waiter is True
        assert subscriber.active_native_epoch == _EPOCH


@pytest.mark.asyncio
async def test_failed_live_epoch_retries_historical_bind_without_selecting_a() -> None:
    epoch_a = "native-epoch-failed-activation"
    epoch_b = "native-epoch-after-failed-activation"
    server = _ReportServer()
    server.native_epoch = epoch_a
    server.fail_epoch_controls.update({(epoch_a, None), (epoch_a, "historical")})

    async with _subscriber(server) as subscriber:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await subscriber._get_capabilities()
        assert subscriber.active_native_epoch == epoch_a

        server.native_epoch = epoch_b
        server.epoch_ordinal = 2
        await subscriber._get_capabilities()
        assert subscriber.active_native_epoch == epoch_b

        old_report = _association(native_epoch=epoch_a, epoch_ordinal=1)
        server.reports = [old_report]
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await subscriber._read_available_reports(wait_ms=0)
        assert server.acknowledged == 0
        assert old_report["source_id"] not in server.persisted
        assert subscriber.active_native_epoch == epoch_b

        await subscriber._read_available_reports(wait_ms=0)

        epoch_events = [
            event
            for event in server.source_events
            if event["type"] == "external_acp_report_epoch_activate"
        ]
        assert [
            (event["data"]["native_epoch"], event["data"].get("mode")) for event in epoch_events
        ] == [
            (epoch_a, None),
            (epoch_b, None),
            (epoch_a, "historical"),
            (epoch_a, "historical"),
        ]
        accepted_epoch_events = [
            event
            for event in server.accepted_source_events
            if event["type"] == "external_acp_report_epoch_activate"
        ]
        assert [
            (event["data"]["native_epoch"], event["data"].get("mode"))
            for event in accepted_epoch_events
        ] == [(epoch_b, None), (epoch_a, "historical")]
        assert server.acknowledged == 1
        assert old_report["source_id"] in server.persisted
        assert subscriber.active_native_epoch == epoch_b


@pytest.mark.asyncio
async def test_late_root_tool_output_keeps_its_epoch_and_response_after_new_epoch_starts() -> None:
    """A delayed A terminal is delivered as A history while B remains current."""
    epoch_a = "native-epoch-root-tool-a"
    epoch_b = "native-epoch-root-tool-b"
    root_native_a = "native-root-tool-a"
    root_native_b = "native-root-tool-b"
    call_id_a = "root-tool-call-a"
    response_id_a = "response-root-tool-a"
    response_id_b = "response-root-tool-b"
    server = _ReportServer()
    server.native_epoch = epoch_a

    async with _subscriber(server) as subscriber:
        await subscriber._get_capabilities()

        # A finished its prompt, then B became the selected native process
        # before A's output arrived in the durable report journal.
        server.native_epoch = epoch_b
        server.epoch_ordinal = 2
        await subscriber._get_capabilities()
        server.reports = [
            _report(
                1,
                "entity.upsert",
                {
                    "entity_type": "item",
                    "entity_id": "root-call-a",
                    "call_id": call_id_a,
                    "value": {
                        "item_type": "function_call",
                        "response_id": response_id_a,
                        "item_data": {
                            "call_id": call_id_a,
                            "name": "worker",
                            "arguments": "{}",
                        },
                    },
                },
                target=root_native_a,
                parent=None,
                native_epoch=epoch_a,
                epoch_ordinal=1,
                ingress_sequence=1,
                root_native_session_id=root_native_a,
            ),
            _report(
                2,
                "entity.upsert",
                {
                    "entity_type": "item",
                    "entity_id": "root-call-b",
                    "call_id": "root-tool-call-b",
                    "value": {
                        "item_type": "function_call",
                        "response_id": response_id_b,
                        "item_data": {
                            "call_id": "root-tool-call-b",
                            "name": "worker",
                            "arguments": "{}",
                        },
                    },
                },
                target=root_native_b,
                parent=None,
                native_epoch=epoch_b,
                epoch_ordinal=2,
                ingress_sequence=1,
                root_native_session_id=root_native_b,
            ),
            _report(
                3,
                "entity.upsert",
                {
                    "entity_type": "item",
                    "entity_id": "root-output-a",
                    "call_id": call_id_a,
                    "value": {
                        "item_type": "function_call_output",
                        "response_id": response_id_a,
                        "item_data": {
                            "call_id": call_id_a,
                            "output": "A completed after B started",
                        },
                    },
                },
                target=root_native_a,
                parent=None,
                native_epoch=epoch_a,
                epoch_ordinal=1,
                ingress_sequence=2,
                root_native_session_id=root_native_a,
            ),
        ]

        await subscriber._read_available_reports(wait_ms=0)

        assert subscriber.active_native_epoch == epoch_b
        assert server.acknowledged == 3
        output_reports = [
            report
            for report in server.attempts
            if report["kind"] == "entity.upsert"
            and report["native_epoch"] == epoch_a
            and report["operation"]["value"]["item_type"] == "function_call_output"
        ]
        assert len(output_reports) == 1
        output_report = output_reports[0]
        assert output_report["root_native_session_id"] == root_native_a
        assert output_report["target_session_id"] == root_native_a
        assert output_report["operation"]["value"]["response_id"] == response_id_a
        assert output_report["operation"]["call_id"] == call_id_a
        assert "_omnigent_action_origin" not in output_report
        assert [
            (event["data"]["native_epoch"], event["data"].get("mode"))
            for event in server.accepted_source_events
            if event["type"] == "external_acp_report_epoch_activate"
        ] == [(epoch_a, None), (epoch_b, None), (epoch_a, "historical")]


@pytest.mark.asyncio
async def test_restart_replays_nested_old_epoch_tail_and_bound_action_once() -> None:
    """A replacement restores an ACKed parent before replaying its nested tail."""
    server = _ReportServer()
    association = _association()
    nested_association = _report(
        2,
        "association.patch",
        {
            "session_id": "native-grandchild-test",
            "parent_session_id": _CHILD_NATIVE,
            "patch": {"title": "nested worker", "state": {"state": "running"}},
        },
        target="native-grandchild-test",
        parent=_CHILD_NATIVE,
        action_origin={
            "token": "trusted-nested-origin",
            "agent": "child-agent",
            "generation": _GENERATION,
        },
    )
    server.reports = [
        association,
        nested_association,
        _report(
            3,
            "entity.upsert",
            {
                "entity_type": "message",
                "entity_id": "nested-message-after-restart",
                "value": {
                    "item_type": "message",
                    "response_id": "nested-response-after-restart",
                    "item_data": {
                        "role": "assistant",
                        "agent": "unknown",
                        "acp_agent_identity": "unknown",
                        "content": [{"type": "text", "text": "nested tail applied"}],
                    },
                },
            },
            target="native-grandchild-test",
            parent=_CHILD_NATIVE,
        ),
    ]
    server.report_page_size = 1
    server.fail_reports_after = 1

    async with _subscriber(server) as first_subscriber:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await first_subscriber.start()
        assert server.acknowledged == 1
        assert first_subscriber.child_session_id_for_native_session(_EPOCH, _CHILD_NATIVE)
        assert first_subscriber.active_origin_tokens == {"trusted-action-origin"}

    # A replacement can restore acknowledged inventory, but it must not expose
    # that inventory as an action barrier until the retained tail is ACKed.
    server.fail_reports_after = 1
    async with _subscriber(server) as interrupted_replacement:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await interrupted_replacement.start()
        assert association["source_id"] not in interrupted_replacement._associations_by_source
        assert association["source_id"] in interrupted_replacement._restored_association_sources
        assert interrupted_replacement.active_origin_tokens == set()

    async with _subscriber(server) as replacement:
        _LEASES.clear()
        _ACTIVITY.clear()
        await replacement.start()
        child_key = acp_session_child_key(_PARENT_ID, _EPOCH, _CHILD_NATIVE)
        child_id = server.children[child_key]["id"]
        nested_key = acp_session_child_key(_PARENT_ID, _EPOCH, "native-grandchild-test")
        nested_id = server.children[nested_key]["id"]
        assert replacement.is_ready
        assert replacement.child_session_id_for_native_session(_EPOCH, _CHILD_NATIVE) == child_id
        assert (
            replacement.child_session_id_for_native_session(_EPOCH, "native-grandchild-test")
            == nested_id
        )
        assert replacement.active_origin_tokens == {
            "trusted-action-origin",
            "trusted-nested-origin",
        }
        assert _LEASES == []
        assert _ACTIVITY == []
        assert replacement._active_children == set()
        assert (
            await replacement.wait_for_association_source(
                association["source_id"],
                native_epoch=_EPOCH,
                target_session_id=_CHILD_NATIVE,
                context_generation=_CONTEXT,
                origin_token="trusted-action-origin",
                timeout_s=0.1,
            )
            == child_id
        )
        assert (
            await replacement.wait_for_association_source(
                nested_association["source_id"],
                native_epoch=_EPOCH,
                target_session_id="native-grandchild-test",
                context_generation=_CONTEXT,
                origin_token="trusted-nested-origin",
                timeout_s=0.1,
            )
            == nested_id
        )
        assert server.acknowledged == 3

        dispatches: list[str] = []
        result_posts: list[dict[str, Any]] = []

        async def action_request(request: httpx.Request) -> httpx.Response:
            result_posts.append(json.loads(request.content))
            return httpx.Response(200, json={"accepted": True}, request=request)

        async def dispatch(action: dict[str, Any], deliver: Any) -> None:
            native_session_id = action["child"]["native_session_id"]
            dispatches.append(native_session_id)
            await deliver(
                {
                    "type": "tool_result",
                    "call_id": action["event"]["item"]["call_id"],
                    "output": "recovered action complete",
                }
            )

        async def unused_policy(_action: dict[str, Any], _deliver: Any) -> None:
            raise AssertionError("unexpected policy action")

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(action_request), base_url="http://harness"
        ) as action_client:
            action_subscriber = AcpSessionActionSubscriber(
                parent_id=_PARENT_ID,
                harness_client=action_client,
                generation=_GENERATION,
                publish_event=lambda _session_id, _event: None,
                remember_approval=lambda _elicitation_id: None,
                dispatch_tool=dispatch,
                evaluate_policy=unused_policy,
            )
            action_subscriber.register_origin("trusted-nested-origin", dispatch, unused_policy)
            recovered_action = {
                "action_id": "recovered-nested-action",
                "event_id": "recovered-nested-event",
                "child": {
                    "native_epoch": _EPOCH,
                    "native_session_id": "native-grandchild-test",
                    "run_id": "session",
                    "origin_token": "trusted-nested-origin",
                    "origin_generation": _GENERATION,
                },
                "event": {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "function_call",
                        "status": "action_required",
                        "name": "inspect_nested",
                        "call_id": "nested-call",
                        "arguments": "{}",
                    },
                },
            }
            try:
                assert action_subscriber._start_action(recovered_action)
                action_task = action_subscriber._action_tasks["recovered-nested-action"]
                done, pending = await asyncio.wait({action_task}, timeout=2)
                if pending:
                    stack = [frame.f_code.co_name for frame in action_task.get_stack()]
                    raise AssertionError(
                        "recovered action did not settle: "
                        f"dispatches={dispatches!r}, posts={result_posts!r}, stack={stack!r}"
                    )
                assert done == {action_task}
                await action_task
                await asyncio.sleep(0)
                assert not action_subscriber._start_action(copy.deepcopy(recovered_action))
                assert dispatches == ["native-grandchild-test"]
                assert len(result_posts) == 1
                assert result_posts[0]["result"]["output"] == "recovered action complete"
            finally:
                await action_subscriber.stop()


@pytest.mark.asyncio
async def test_new_epoch_recovery_applies_old_tail_without_restoring_old_live_origin() -> None:
    """A new epoch can drain retained history but cannot reuse old action authority."""
    old_epoch = "native-epoch-before-restart"
    new_epoch = "native-epoch-after-restart"
    server = _ReportServer()
    server.native_epoch = old_epoch
    old_association = _association(
        native_epoch=old_epoch,
        action_origin_token="old-epoch-origin",
    )
    old_tail = _report(
        2,
        "association.patch",
        {
            "session_id": _CHILD_NATIVE,
            "parent_session_id": _ROOT_NATIVE,
            "patch": {"title": "worker", "state": {"state": "running"}},
        },
        target=_CHILD_NATIVE,
        parent=_ROOT_NATIVE,
        action_origin={
            "token": "old-epoch-origin",
            "agent": "parent-agent",
            "generation": _GENERATION,
        },
        native_epoch=old_epoch,
    )
    new_association = _association(
        3,
        native_epoch=new_epoch,
        epoch_ordinal=2,
        ingress_sequence=1,
        action_origin_token="current-epoch-origin",
    )
    new_tail = _report(
        4,
        "entity.upsert",
        {
            "entity_type": "message",
            "entity_id": "new-epoch-child-tail",
            "value": {
                "item_type": "message",
                "response_id": "new-epoch-response",
                "item_data": {
                    "role": "assistant",
                    "agent": "unknown",
                    "acp_agent_identity": "unknown",
                    "content": [{"type": "text", "text": "current tail"}],
                },
            },
        },
        target=_CHILD_NATIVE,
        parent=_ROOT_NATIVE,
        native_epoch=new_epoch,
        epoch_ordinal=2,
        ingress_sequence=2,
    )
    server.reports = [old_association, old_tail, new_association, new_tail]
    server.report_page_size = 1
    server.fail_reports_after = 1

    async with _subscriber(server) as first_subscriber:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            await first_subscriber.start()
        assert server.acknowledged == 1
        assert first_subscriber.active_origin_tokens == {"old-epoch-origin"}

    server.native_epoch = new_epoch
    server.epoch_ordinal = 2
    lease_changes: list[tuple[str, bool]] = []

    async def record_lease(_parent_id: str, child_key: str, active: bool) -> None:
        lease_changes.append((child_key, active))

    async with _subscriber(server, lease_callback=record_lease) as replacement:
        await replacement.start()
        old_child_id = replacement.child_session_id_for_native_session(old_epoch, _CHILD_NATIVE)
        new_child_id = replacement.child_session_id_for_native_session(new_epoch, _CHILD_NATIVE)
        old_child_key = acp_session_child_key(_PARENT_ID, old_epoch, _CHILD_NATIVE)
        new_child_key = acp_session_child_key(_PARENT_ID, new_epoch, _CHILD_NATIVE)
        assert old_child_id is not None
        assert new_child_id is not None
        assert old_child_id != new_child_id
        assert replacement.active_origin_tokens == {"current-epoch-origin"}
        assert old_child_key not in replacement._active_children
        assert new_child_key not in replacement._active_children
        assert (old_child_key, True) not in lease_changes
        assert (new_child_key, True) not in lease_changes

        poller = replacement._task
        if poller is not None:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
            replacement._task = None
        server.reports.append(
            _report(
                5,
                "observation.patch",
                {"observation": {"state": {"present": True, "value": {"state": "running"}}}},
                target=_CHILD_NATIVE,
                parent=_ROOT_NATIVE,
                native_epoch=new_epoch,
                epoch_ordinal=2,
                ingress_sequence=3,
            )
        )
        await replacement._read_available_reports(wait_ms=0)
        assert new_child_key in replacement._active_children
        assert (new_child_key, True) in lease_changes
        with pytest.raises(RuntimeError, match="inactive native epoch"):
            await replacement.wait_for_association_source(
                old_association["source_id"],
                native_epoch=old_epoch,
                target_session_id=_CHILD_NATIVE,
                context_generation=_CONTEXT,
                origin_token="old-epoch-origin",
                timeout_s=0.1,
            )
        assert (
            await replacement.wait_for_association_source(
                new_association["source_id"],
                native_epoch=new_epoch,
                target_session_id=_CHILD_NATIVE,
                context_generation=_CONTEXT,
                origin_token="current-epoch-origin",
                timeout_s=0.1,
            )
            == new_child_id
        )
        assert server.acknowledged == 5
        assert old_tail["source_id"] in server.persisted
        assert new_tail["source_id"] in server.persisted

        dispatches: list[str] = []
        action_results: list[dict[str, Any]] = []

        async def action_request(request: httpx.Request) -> httpx.Response:
            action_results.append(json.loads(request.content))
            return httpx.Response(200, json={"accepted": True}, request=request)

        async def dispatch(action: dict[str, Any], deliver: Any) -> None:
            dispatches.append(action["event"]["item"]["call_id"])
            await deliver(
                {
                    "type": "tool_result",
                    "call_id": action["event"]["item"]["call_id"],
                    "output": "current child action complete",
                }
            )

        async def unused_policy(_action: dict[str, Any], _deliver: Any) -> None:
            raise AssertionError("unexpected policy action")

        def make_action(action_id: str, native_epoch: str, origin_token: str) -> dict[str, Any]:
            call_id = f"{action_id}-call"
            return {
                "action_id": action_id,
                "event_id": f"{action_id}-event",
                "child": {
                    "native_epoch": native_epoch,
                    "native_session_id": _CHILD_NATIVE,
                    "run_id": "session",
                    "origin_token": origin_token,
                    "origin_generation": _GENERATION,
                },
                "event": {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "function_call",
                        "status": "action_required",
                        "name": "inspect_child",
                        "call_id": call_id,
                        "arguments": "{}",
                    },
                },
            }

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(action_request), base_url="http://harness"
        ) as action_client:
            action_subscriber = AcpSessionActionSubscriber(
                parent_id=_PARENT_ID,
                harness_client=action_client,
                generation=_GENERATION,
                publish_event=lambda _session_id, _event: None,
                remember_approval=lambda _elicitation_id: None,
                dispatch_tool=dispatch,
                evaluate_policy=unused_policy,
            )
            action_subscriber.register_origin("current-epoch-origin", dispatch, unused_policy)
            old_action = make_action("old-epoch-action", old_epoch, "old-epoch-origin")
            current_action = make_action("current-epoch-action", new_epoch, "current-epoch-origin")
            try:
                assert action_subscriber._start_action(old_action)
                await action_subscriber._action_tasks["old-epoch-action"]
                await asyncio.sleep(0)
                assert dispatches == []
                assert "no registered parent-turn origin" in action_results[0]["result"]["output"]

                assert action_subscriber._start_action(current_action)
                await action_subscriber._action_tasks["current-epoch-action"]
                await asyncio.sleep(0)
                assert not action_subscriber._start_action(copy.deepcopy(current_action))
                assert dispatches == ["current-epoch-action-call"]
                assert len(action_results) == 2
            finally:
                await action_subscriber.stop()


@pytest.mark.asyncio
async def test_failed_source_releases_activity_lease_without_inventing_outcome() -> None:
    server = _ReportServer()
    server.source_status = {"state": "initialized", "prompt_started": False}
    _ACTIVITY.clear()
    _LEASES.clear()

    async with _subscriber(server) as subscriber:
        await subscriber.start()
        # Let start establish readiness on an empty prefix, then stop its poller
        # so the test can drive the later live report and source failure in order.
        poller = subscriber._task
        if poller is not None:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
            subscriber._task = None

        server.reports = [_association()]
        await subscriber._read_available_reports(wait_ms=0)
        assert _LEASES == [True]
        assert _ACTIVITY == [True]

        server.source_status = {"state": "failed", "error": "source exited"}
        await subscriber._read_available_reports(wait_ms=0)
        assert subscriber.source_failed
        assert not subscriber.is_ready
        assert server.acknowledged == 1
        assert _LEASES == [True, False]
        assert _ACTIVITY == [True, False]
        assert len(server.attempts) == 1


@pytest.mark.asyncio
async def test_start_rejects_report_source_without_private_v3_features() -> None:
    server = _ReportServer()
    original = server.capabilities
    server.capabilities = lambda: {**original(), "features": ["standard-acp-subagents-v1"]}

    async with _subscriber(server) as subscriber:
        with pytest.raises(RuntimeError, match="lacks required standard ACP report features"):
            await subscriber.start()
