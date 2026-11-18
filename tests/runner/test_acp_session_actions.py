"""Contract tests for generation-bound ACP actions and child approvals."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import Mapping
from contextlib import suppress
from typing import Any

import httpx
import pytest
import yaml

from omnigent.inner.acp_standard_subagents import (
    ACTION_FEATURES,
    REPORT_FEATURES,
    acp_standard_report_source_id,
)
from omnigent.runner import acp_session_actions
from omnigent.runner.acp_session_actions import AcpSessionActionSubscriber
from omnigent.runner.acp_session_reports import AcpSessionReportSubscriber, acp_session_child_key
from omnigent.runner.app import create_runner_app
from omnigent.spec.types import AgentSpec, ExecutorSpec
from tests._helpers.polling import poll_until_async
from tests.runner.conftest import _FakeProcessManager, _runner_client

_PARENT_ID = "parent-action-test"
_GENERATION = "action-generation-test"
_SERVER_IDENTITY = "action-test-server"
_JOURNAL_ID = "a" * 32
_ROOT_NATIVE_SESSION_ID = "native-action-root"


def _association_report(
    *,
    sequence: int,
    root_id: str,
    native_epoch: str,
    native_session_id: str,
    action_token: str,
    context_generation: str,
    patch: dict[str, Any],
    action_agent: str = "agent-1",
    root_native_session_id: str = _ROOT_NATIVE_SESSION_ID,
) -> dict[str, Any]:
    """Build a v3 association record using the same identity envelope as ACP."""
    source_id = acp_standard_report_source_id(_SERVER_IDENTITY, root_id, native_epoch, sequence)
    return {
        "sequence": sequence,
        "journal_id": _JOURNAL_ID,
        "epoch_ordinal": 1,
        "native_epoch": native_epoch,
        "ingress_sequence": sequence,
        "source_id": source_id,
        "kind": "association.patch",
        "data": {
            "server_identity": _SERVER_IDENTITY,
            "root_conversation_id": root_id,
            "context_generation": context_generation,
            "root_native_session_id": root_native_session_id,
            "target_session_id": native_session_id,
            "parent_session_id": root_native_session_id,
            "operation": {
                "session_id": native_session_id,
                "parent_session_id": root_native_session_id,
                "patch": patch,
            },
            "_omnigent_action_origin": {
                "token": action_token,
                "agent": action_agent,
                "generation": _GENERATION,
            },
        },
    }


class _ActionHarness:
    """Capture action results and optionally lose the first HTTP response."""

    def __init__(self, *, lose_first_response: bool = False) -> None:
        self.lose_first_response = lose_first_response
        self.result_attempts: list[dict[str, Any]] = []
        self.first_attempt = asyncio.Event()
        self.accepted = asyncio.Event()

    async def request(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        body = json.loads(request.content)
        self.result_attempts.append(body)
        if len(self.result_attempts) == 1:
            self.first_attempt.set()
            if self.lose_first_response:
                raise httpx.ReadError("result response lost", request=request)
        self.accepted.set()
        return httpx.Response(200, json={"accepted": True}, request=request)


def _tool_action(
    *,
    action_id: str = "action-1",
    event_id: str = "event-1",
    origin_token: str = "origin-1",
    origin_generation: str = _GENERATION,
    context_generation: str = "action-context-test",
    call_id: str = "call-1",
    arguments: str = "{}",
) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "event_id": event_id,
        "child": {
            "native_epoch": "epoch-1",
            "native_session_id": "native-session-1",
            "run_id": "run-1",
            "origin_token": origin_token,
            "origin_generation": origin_generation,
            "context_generation": context_generation,
        },
        "event": {
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "status": "action_required",
                "name": "example_tool",
                "call_id": call_id,
                "arguments": arguments,
            },
        },
    }


def _approval_action(
    *,
    action_id: str = "approval-action",
    elicitation_id: str = "elicitation-1",
    origin_token: str = "origin-1",
    origin_generation: str = _GENERATION,
    context_generation: str = "action-context-test",
) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "event_id": f"event-{action_id}",
        "child": {
            "native_epoch": "epoch-1",
            "native_session_id": "native-session-1",
            "run_id": "run-1",
            "origin_token": origin_token,
            "origin_generation": origin_generation,
            "context_generation": context_generation,
        },
        "event": {
            "type": "response.elicitation_request",
            "elicitation_id": elicitation_id,
            "message": "May I continue?",
        },
    }


def _subscriber(
    client: httpx.AsyncClient,
    *,
    publish_event: Any | None = None,
    remember_approval: Any | None = None,
    dispatch_tool: Any | None = None,
    resolve_approval_target: Any | None = None,
    ask_timeout_s: float = 86400,
) -> AcpSessionActionSubscriber:
    async def _unused_tool(_action: dict[str, Any], _deliver: Any) -> None:
        raise AssertionError("unexpected default tool dispatch")

    async def _unused_policy(_action: dict[str, Any], _deliver: Any) -> None:
        raise AssertionError("unexpected policy dispatch")

    tool_handler = dispatch_tool or _unused_tool
    subscriber = AcpSessionActionSubscriber(
        parent_id=_PARENT_ID,
        harness_client=client,
        generation=_GENERATION,
        publish_event=publish_event or (lambda _session_id, _event: None),
        remember_approval=remember_approval or (lambda _elicitation_id: None),
        dispatch_tool=tool_handler,
        evaluate_policy=_unused_policy,
        resolve_approval_target=resolve_approval_target,
    )
    subscriber.register_origin(
        "origin-1", tool_handler, _unused_policy, ask_timeout_s=ask_timeout_s
    )
    return subscriber


def _start_action(
    subscriber: AcpSessionActionSubscriber, action: dict[str, Any]
) -> tuple[asyncio.Task[None], asyncio.Event]:
    assert subscriber._start_action(action)
    task = subscriber._action_tasks[action["action_id"]]
    completed = asyncio.Event()
    # The subscriber's own completion callback was registered first and has
    # already finalized the action by the time this barrier is released.
    task.add_done_callback(lambda _task: completed.set())
    return task, completed


@pytest.mark.asyncio
async def test_settled_origin_releases_registration_capacity_and_keeps_action_tombstones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settled handler slots are reusable while completed action ids stay fenced."""
    monkeypatch.setattr(acp_session_actions, "_MAX_ACTION_ORIGINS", 2)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_ActionHarness().request),
        base_url="http://harness",
    )

    async def _handler(_action: dict[str, Any], _deliver: Any) -> None:
        return

    subscriber = AcpSessionActionSubscriber(
        parent_id=_PARENT_ID,
        harness_client=client,
        generation=_GENERATION,
        publish_event=lambda _session_id, _event: None,
        remember_approval=lambda _elicitation_id: None,
        dispatch_tool=_handler,
        evaluate_policy=_handler,
    )
    action = _tool_action(action_id="completed-action", origin_token="origin-a")
    subscriber._completed_actions["completed-action"] = acp_session_actions._action_identity(
        action
    )
    subscriber.register_origin("origin-a", _handler, _handler)
    subscriber.register_origin("origin-b", _handler, _handler)
    with pytest.raises(RuntimeError, match="capacity"):
        subscriber.register_origin("origin-c", _handler, _handler)

    assert subscriber.forget_origin("origin-a")
    subscriber.register_origin("origin-c", _handler, _handler)
    assert not subscriber.forget_origin("origin-a")
    assert "completed-action" in subscriber._completed_actions
    assert not subscriber._start_action(action)
    # The test has no poller or active tasks; close the client directly.
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ask_timeout_s",
    [0, -1, True, float("nan"), float("inf"), "soon", None],
)
async def test_origin_registration_rejects_invalid_trusted_approval_timeouts(
    ask_timeout_s: Any,
) -> None:
    async def _handler(_action: dict[str, Any], _deliver: Any) -> None:
        return

    client = httpx.AsyncClient(base_url="http://harness")
    subscriber = AcpSessionActionSubscriber(
        parent_id=_PARENT_ID,
        harness_client=client,
        generation=_GENERATION,
        publish_event=lambda _session_id, _event: None,
        remember_approval=lambda _elicitation_id: None,
        dispatch_tool=_handler,
        evaluate_policy=_handler,
    )
    with pytest.raises(ValueError, match="finite and positive"):
        subscriber.register_origin(
            "trusted-origin", _handler, _handler, ask_timeout_s=ask_timeout_s
        )
    assert "trusted-origin" not in subscriber._origin_handlers
    # No request or poller is started by this construction-only validation test.
    await client.aclose()


@pytest.mark.asyncio
async def test_short_trusted_timeout_ignores_native_deadline_and_fences_only_its_approval() -> (
    None
):
    harness = _ActionHarness()
    published: list[dict[str, Any]] = []
    remembered: list[str] = []

    def publish(_session_id: str, event: Mapping[str, Any]) -> None:
        published.append(dict(event))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(
            client,
            publish_event=publish,
            remember_approval=remembered.append,
            ask_timeout_s=0.05,
        )
        subscriber.register_origin(
            "sibling-origin",
            subscriber._dispatch_tool,
            subscriber._evaluate_policy,
            ask_timeout_s=1.0,
        )
        timed_out = _approval_action(
            action_id="timed-out-approval",
            elicitation_id="timed-out-elicitation",
        )
        timed_out["event"]["params"] = {
            "policy_name": "native-selected-long-policy",
            "ask_timeout": 86400,
            "timeout": 86400,
        }
        timed_out_task, timed_out_completed = _start_action(subscriber, timed_out)
        fresh = _approval_action(
            action_id="fresh-approval",
            elicitation_id="fresh-elicitation",
            origin_token="sibling-origin",
        )
        fresh["event"]["params"] = {
            "policy_name": "native-selected-policy",
            "timeout": 86400,
        }
        fresh_task, fresh_completed = _start_action(subscriber, fresh)
        await asyncio.wait_for(timed_out_completed.wait(), timeout=1.0)
        await timed_out_task

        assert set(remembered) == {"timed-out-elicitation", "fresh-elicitation"}
        assert published[-1] == {
            "type": "response.elicitation_resolved",
            "elicitation_id": "timed-out-elicitation",
            "reason": "unanswered",
        }
        assert harness.result_attempts[0]["result"] == {
            "type": "approval",
            "elicitation_id": "timed-out-elicitation",
            "action": "cancel",
        }
        assert not subscriber.resolve_approval(
            {"elicitation_id": "timed-out-elicitation", "action": "accept"}
        )
        assert "fresh-elicitation" in subscriber._approval_waiters
        assert subscriber.resolve_approval(
            {"elicitation_id": "fresh-elicitation", "action": "accept"}
        )
        await asyncio.wait_for(fresh_completed.wait(), timeout=1.0)
        await fresh_task

        assert [attempt["result"]["action"] for attempt in harness.result_attempts] == [
            "cancel",
            "accept",
        ]
        assert published[-1] == {
            "type": "response.elicitation_resolved",
            "elicitation_id": "fresh-elicitation",
            "action": "accept",
        }
        assert not any(event.get("type") == "session/cancel" for event in published)
        await subscriber.stop()


@pytest.mark.asyncio
async def test_pending_action_page_backs_off_without_blocking_other_tasks() -> None:
    """A page repeating an active action cannot hot-poll the harness."""
    action = _tool_action()
    dispatch_started = asyncio.Event()
    release_dispatch = asyncio.Event()
    repeated_page_seen = asyncio.Event()
    poll_requests = 0

    async def request(request: httpx.Request) -> httpx.Response:
        nonlocal poll_requests
        if request.method != "GET":
            raise AssertionError(f"unexpected request: {request.method} {request.url}")
        if request.url.params.get("wait_ms") == "0":
            actions: list[dict[str, Any]] = []
        else:
            poll_requests += 1
            actions = [copy.deepcopy(action)]
            if poll_requests == 2:
                repeated_page_seen.set()
        return httpx.Response(
            200,
            json={
                "protocol": "acp-session-actions",
                "version": 2,
                "features": sorted(ACTION_FEATURES),
                "generation": _GENERATION,
                "actions": actions,
            },
            request=request,
        )

    async def dispatch(_action: dict[str, Any], _deliver: Any) -> None:
        dispatch_started.set()
        await release_dispatch.wait()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(client, dispatch_tool=dispatch)
        try:
            await subscriber.start()
            await asyncio.wait_for(dispatch_started.wait(), timeout=1.0)
            await asyncio.wait_for(repeated_page_seen.wait(), timeout=1.0)

            loop_progressed = asyncio.Event()
            asyncio.get_running_loop().call_later(0.05, loop_progressed.set)
            await asyncio.wait_for(loop_progressed.wait(), timeout=1.0)

            assert poll_requests == 2
            assert not subscriber._stopped.is_set()
        finally:
            release_dispatch.set()
            await subscriber.stop()


@pytest.mark.asyncio
async def test_action_tombstone_overflow_fails_generation_without_executing_pending_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Overflow preserves every accepted identity and rejects new side effects."""
    monkeypatch.setattr(acp_session_actions, "_MAX_ACTION_ID_TOMBSTONES", 1)
    first = _tool_action(action_id="action-first", call_id="call-first")
    overflow = _tool_action(
        action_id="action-overflow", event_id="event-overflow", call_id="call-overflow"
    )
    pages = [first, overflow]
    result_attempts: list[dict[str, Any]] = []
    dispatches: list[str] = []
    fatal_generations: list[tuple[str, str, str]] = []
    fatal = asyncio.Event()
    first_result_delivered = asyncio.Event()

    async def request(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            payload = json.loads(request.content)
            result_attempts.append(payload)
            return httpx.Response(200, json={"accepted": True}, request=request)
        if request.url.params.get("wait_ms") == "0":
            actions: list[dict[str, Any]] = []
        elif pages:
            next_action = pages[0]
            if next_action is overflow:
                await first_result_delivered.wait()
                # Let the subscriber's completion callback reserve its durable
                # identity before the next action page is delivered.
                await asyncio.sleep(0)
            actions = [pages.pop(0)]
        else:
            await asyncio.Event().wait()
            actions = []
        return httpx.Response(
            200,
            json={
                "protocol": "acp-session-actions",
                "version": 2,
                "features": sorted(ACTION_FEATURES),
                "generation": _GENERATION,
                "actions": actions,
            },
            request=request,
        )

    async def dispatch(action: dict[str, Any], deliver: Any) -> None:
        call_id = action["event"]["item"]["call_id"]
        dispatches.append(call_id)
        await deliver({"type": "tool_result", "call_id": call_id, "output": "done"})
        first_result_delivered.set()

    async def on_fatal_generation(session_id: str, generation: str, reason: str) -> None:
        fatal_generations.append((session_id, generation, reason))
        fatal.set()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(request), base_url="http://harness"
    ) as client:
        subscriber = AcpSessionActionSubscriber(
            parent_id=_PARENT_ID,
            harness_client=client,
            generation=_GENERATION,
            publish_event=lambda _session_id, _event: None,
            remember_approval=lambda _elicitation_id: None,
            dispatch_tool=dispatch,
            evaluate_policy=lambda _action, _deliver: asyncio.sleep(0),
            on_fatal_generation=on_fatal_generation,
        )
        subscriber.register_origin("origin-1", dispatch, subscriber._evaluate_policy)
        try:
            await subscriber.start()
            await asyncio.wait_for(fatal.wait(), timeout=1.0)

            assert dispatches == ["call-first"]
            assert [attempt["event_id"] for attempt in result_attempts] == ["event-1"]
            assert len(fatal_generations) == 1
            assert fatal_generations[0][:2] == (_PARENT_ID, _GENERATION)
            assert "capacity" in fatal_generations[0][2].lower()
            assert set(subscriber._completed_actions) == {"action-first"}
            assert not subscriber._start_action(copy.deepcopy(overflow))
            assert not subscriber._start_action(copy.deepcopy(first))
            await asyncio.sleep(0)
            assert dispatches == ["call-first"]
            assert [attempt["event_id"] for attempt in result_attempts] == ["event-1"]
            assert len(fatal_generations) == 1
        finally:
            await subscriber.stop()


@pytest.mark.asyncio
async def test_stale_action_page_cannot_repeat_completed_or_conflicting_side_effects() -> None:
    """A replayed page is ignored even after the first result was accepted."""
    harness = _ActionHarness()
    started = asyncio.Event()
    release = asyncio.Event()
    dispatches: list[str] = []

    async def dispatch(action: dict[str, Any], deliver: Any) -> None:
        dispatches.append(action["event"]["item"]["call_id"])
        started.set()
        await release.wait()
        await deliver(
            {
                "type": "tool_result",
                "call_id": action["event"]["item"]["call_id"],
                "output": "complete",
            }
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(client, dispatch_tool=dispatch)
        action = _tool_action()
        task, completed = _start_action(subscriber, action)
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # A stale in-flight page may expose the same action while dispatch is
        # running, and another replay may arrive after result 200.
        assert not subscriber._start_action(copy.deepcopy(action))
        release.set()
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        assert not subscriber._start_action(copy.deepcopy(action))

        conflicting = copy.deepcopy(action)
        conflicting["event"]["item"]["arguments"] = '{"different":true}'
        assert not subscriber._start_action(conflicting)
        await task
        assert dispatches == ["call-1"]
        assert len(harness.result_attempts) == 1
        await subscriber.stop()


@pytest.mark.asyncio
async def test_lost_result_response_retries_cached_result_without_repeating_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The result is retried after a lost response; the side effect runs once."""
    harness = _ActionHarness(lose_first_response=True)
    monkeypatch.setattr(acp_session_actions, "_RETRY_INITIAL_S", 0.0)
    dispatches: list[str] = []

    async def dispatch(action: dict[str, Any], deliver: Any) -> None:
        dispatches.append(action["event"]["item"]["call_id"])
        await deliver(
            {
                "type": "tool_result",
                "call_id": action["event"]["item"]["call_id"],
                "output": "saved once",
            }
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(client, dispatch_tool=dispatch)
        task, completed = _start_action(subscriber, _tool_action())
        await asyncio.wait_for(harness.accepted.wait(), timeout=1.0)
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        await task

        assert dispatches == ["call-1"]
        assert len(harness.result_attempts) == 2
        assert harness.result_attempts[0] == harness.result_attempts[1]
        assert harness.result_attempts[0]["result"] == {
            "type": "tool_result",
            "call_id": "call-1",
            "output": "saved once",
        }
        await subscriber.stop()


@pytest.mark.asyncio
async def test_same_generation_actions_use_their_registered_parent_turn_origins() -> None:
    """A later parent turn cannot replace a child action's captured handlers."""
    harness = _ActionHarness()
    dispatched: list[tuple[str, str]] = []

    def make_handler(origin: str):
        async def dispatch(action: dict[str, Any], deliver: Any) -> None:
            call_id = action["event"]["item"]["call_id"]
            dispatched.append((origin, call_id))
            await deliver({"type": "tool_result", "call_id": call_id, "output": origin})

        return dispatch

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(client)
        origin_a = make_handler("turn-a")
        origin_b = make_handler("turn-b")
        subscriber.register_origin("origin-a", origin_a, subscriber._evaluate_policy)
        subscriber.register_origin("origin-b", origin_b, subscriber._evaluate_policy)
        first_task, first_done = _start_action(
            subscriber,
            _tool_action(action_id="action-a", event_id="event-a", origin_token="origin-a"),
        )
        second_task, second_done = _start_action(
            subscriber,
            _tool_action(action_id="action-b", event_id="event-b", origin_token="origin-b"),
        )
        await asyncio.wait_for(first_done.wait(), timeout=1.0)
        await asyncio.wait_for(second_done.wait(), timeout=1.0)
        await asyncio.gather(first_task, second_task)

        assert set(dispatched) == {("turn-a", "call-1"), ("turn-b", "call-1")}
        assert {
            attempt["event_id"]: attempt["result"]["output"] for attempt in harness.result_attempts
        } == {"event-a": "turn-a", "event-b": "turn-b"}
        await subscriber.stop()


@pytest.mark.asyncio
async def test_unregistered_action_origin_fails_closed_without_dispatch() -> None:
    """Unknown turn tokens return an error result without using a newer handler."""
    harness = _ActionHarness()
    dispatches: list[str] = []

    async def dispatch(action: dict[str, Any], deliver: Any) -> None:
        dispatches.append(action["event"]["item"]["call_id"])
        await deliver({"type": "tool_result", "call_id": "call-1", "output": "wrong"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(client)
        subscriber.register_origin("current-turn", dispatch, subscriber._evaluate_policy)
        task, completed = _start_action(
            subscriber,
            _tool_action(
                action_id="old-action", event_id="old-event", origin_token="unknown-turn"
            ),
        )
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        await task

        assert dispatches == []
        result = harness.result_attempts[0]["result"]
        assert result["type"] == "tool_result"
        assert result["call_id"] == "call-1"
        assert "no registered parent-turn origin" in json.loads(result["output"])["error"]
        await subscriber.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["accept", "decline", "cancel"])
async def test_child_approval_resolution_publishes_resolved_for_every_choice(
    choice: str,
) -> None:
    """Answers, denials, and explicit cancellations all close the elicitation."""
    harness = _ActionHarness()
    published: list[dict[str, Any]] = []
    remembered: list[str] = []
    resolved_children: list[dict[str, str]] = []
    request_published = asyncio.Event()
    verified_child_session_id = "persisted-child-session-1"

    def publish(_session_id: str, event: Mapping[str, Any]) -> None:
        published.append(dict(event))
        if event.get("type") == "response.elicitation_request":
            request_published.set()

    async def resolve_approval_target(action: dict[str, Any]) -> str:
        child = action.get("child")
        assert isinstance(child, dict)
        resolved_children.append(child)
        return verified_child_session_id

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(
            client,
            publish_event=publish,
            remember_approval=remembered.append,
            resolve_approval_target=resolve_approval_target,
        )
        action = _approval_action(elicitation_id=f"elicitation-{choice}")
        action["event"]["params"] = {
            "requestedSchema": {
                "type": "object",
                "properties": {"answer": {"type": "string", "enum": ["Allow once"]}},
            },
            "content_preview": "Allow this worker to write its requested file?",
        }
        task, completed = _start_action(subscriber, action)
        await asyncio.wait_for(request_published.wait(), timeout=1.0)

        content = {"confirmation": "yes"} if choice == "accept" else None
        assert subscriber.resolve_approval(
            {
                "elicitation_id": f"elicitation-{choice}",
                "action": choice,
                "content": content,
            }
        )
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        await task

        assert remembered == [f"elicitation-{choice}"]
        assert resolved_children == [
            {
                "native_epoch": "epoch-1",
                "native_session_id": "native-session-1",
                "run_id": "run-1",
                "origin_token": "origin-1",
                "origin_generation": _GENERATION,
                "context_generation": "action-context-test",
            }
        ]
        assert published == [
            {
                "type": "response.elicitation_request",
                "elicitation_id": f"elicitation-{choice}",
                "message": "May I continue?",
                "params": {
                    **action["event"]["params"],
                    "target_session_id": verified_child_session_id,
                },
            },
            {
                "type": "response.elicitation_resolved",
                "elicitation_id": f"elicitation-{choice}",
                "action": choice,
            },
        ]
        assert harness.result_attempts[0]["result"] == {
            "type": "approval",
            "elicitation_id": f"elicitation-{choice}",
            "action": choice,
            **({"content": content} if content is not None else {}),
        }
        await subscriber.stop()


@pytest.mark.asyncio
async def test_approval_publish_failure_cancels_child_and_marks_request_unanswered() -> None:
    """A failed handoff cancels the child without claiming a user verdict."""
    harness = _ActionHarness()
    published: list[dict[str, Any]] = []
    remembered: list[str] = []

    def publish(_session_id: str, event: Mapping[str, Any]) -> None:
        published.append(dict(event))
        if event.get("type") == "response.elicitation_request":
            raise RuntimeError("approval transport unavailable")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(harness.request), base_url="http://harness"
    ) as client:
        subscriber = _subscriber(
            client,
            publish_event=publish,
            remember_approval=remembered.append,
        )
        task, completed = _start_action(subscriber, _approval_action())
        await asyncio.wait_for(completed.wait(), timeout=1.0)
        await task

        assert remembered == ["elicitation-1"]
        assert [event["type"] for event in published] == [
            "response.elicitation_request",
            "response.elicitation_resolved",
        ]
        assert published[-1] == {
            "type": "response.elicitation_resolved",
            "elicitation_id": "elicitation-1",
            "reason": "unanswered",
        }
        assert harness.result_attempts[0]["result"] == {
            "type": "approval",
            "elicitation_id": "elicitation-1",
            "action": "cancel",
        }
        await subscriber.stop()


class _ActionAppHarnessClient:
    """Serve one prompt process and the private report/action channels."""

    def __init__(self, *, held_prompt_turns: set[int] | None = None) -> None:
        self.held_prompt_turns = {2} if held_prompt_turns is None else set(held_prompt_turns)
        self.action_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.action_poll_task: asyncio.Task[Any] | None = None
        self.report_poll_task: asyncio.Task[Any] | None = None
        self.action_poll_started = asyncio.Event()
        self.report_poll_started = asyncio.Event()
        self.report_poll_release = asyncio.Event()
        self.report_poll_calls = 0
        self.report_records: list[dict[str, Any]] = []
        self.report_acknowledged = 0
        self.report_journal_id = _JOURNAL_ID
        self.native_epoch: str | None = None
        self.report_source_status: dict[str, Any] = {
            "state": "initialized",
            "prompt_started": False,
        }
        self.release_second_turn = asyncio.Event()
        self.release_first_turn = asyncio.Event()
        self.first_turn_started = asyncio.Event()
        self.second_turn_started = asyncio.Event()
        self.prompt_bodies: list[dict[str, Any]] = []
        self.forwarded_events: list[dict[str, Any]] = []
        self.action_result = asyncio.Event()
        self.action_results: list[dict[str, Any]] = []
        self.completed_turn_count = 0
        self.completed_turn = asyncio.Event()

    def stream(self, method: str, url: str, *, json: dict[str, Any], timeout: Any) -> Any:
        del method, url, timeout
        self.prompt_bodies.append(json)
        turn = len(self.prompt_bodies)
        owner = self

        class _Stream:
            status_code = 200

            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(self, *_: Any) -> None:
                return None

            async def aiter_text(self):
                response_id = f"response-{turn}"
                yield _sse({"type": "response.created", "response": {"id": response_id}})
                if turn == 1:
                    owner.first_turn_started.set()
                if turn in owner.held_prompt_turns:
                    if turn == 1:
                        await owner.release_first_turn.wait()
                    else:
                        owner.second_turn_started.set()
                        await owner.release_second_turn.wait()
                yield _sse({"type": "response.completed", "response": {"id": response_id}})
                owner.completed_turn_count += 1
                owner.completed_turn.set()

        return _Stream()

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        timeout: Any = None,
    ) -> httpx.Response:
        del timeout
        params = params or {}
        request = httpx.Request("GET", f"http://harness{url}")
        if url.endswith("/reports/capabilities"):
            return httpx.Response(
                200,
                json={
                    "protocol": "acp-session-reports",
                    "version": 3,
                    "features": sorted(REPORT_FEATURES),
                    "subscriber_generation": _GENERATION,
                    "journal_id": self.report_journal_id,
                    "native_epoch": self.native_epoch,
                    "epoch_ordinal": 1 if self.native_epoch is not None else None,
                    "acknowledged_sequence": self.report_acknowledged,
                    "highest_sequence": max(
                        (record["sequence"] for record in self.report_records), default=0
                    ),
                    "source_status": self.report_source_status,
                },
                request=request,
            )
        if url.endswith("/reports"):
            if int(params.get("wait_ms", 0)) > 0:
                self.report_poll_calls += 1
                self.report_poll_task = asyncio.current_task()
                self.report_poll_started.set()
                await self.report_poll_release.wait()
                self.report_poll_release.clear()
            records = [
                record
                for record in self.report_records
                if record["sequence"] > self.report_acknowledged
            ]
            return httpx.Response(
                200,
                json={
                    "protocol": "acp-session-reports",
                    "version": 3,
                    "features": sorted(REPORT_FEATURES),
                    "subscriber_generation": _GENERATION,
                    "journal_id": self.report_journal_id,
                    "native_epoch": self.native_epoch,
                    "epoch_ordinal": 1 if self.native_epoch is not None else None,
                    "records": records,
                    "highest_sequence": max(
                        (record["sequence"] for record in self.report_records), default=0
                    ),
                    "exposed_through": max(
                        (record["sequence"] for record in records),
                        default=self.report_acknowledged,
                    ),
                    "more": False,
                    "source_status": self.report_source_status,
                },
                request=request,
            )
        if url.endswith("/actions"):
            if int(params.get("wait_ms", 0)) == 0:
                return httpx.Response(
                    200,
                    json={
                        "protocol": "acp-session-actions",
                        "version": 2,
                        "features": sorted(ACTION_FEATURES),
                        "generation": _GENERATION,
                        "actions": [],
                    },
                    request=request,
                )
            self.action_poll_task = asyncio.current_task()
            self.action_poll_started.set()
            action = await self.action_queue.get()
            return httpx.Response(
                200,
                json={
                    "protocol": "acp-session-actions",
                    "version": 2,
                    "features": sorted(ACTION_FEATURES),
                    "generation": _GENERATION,
                    "actions": [action],
                },
                request=request,
            )
        return httpx.Response(404, request=request)

    async def post(self, url: str, *, json: dict[str, Any], timeout: Any = None) -> httpx.Response:
        del timeout
        request = httpx.Request("POST", f"http://harness{url}")
        if "/actions/" in url and url.endswith("/result"):
            self.action_results.append(json)
            self.action_result.set()
            return httpx.Response(200, json={"accepted": True}, request=request)
        if url.endswith("/reports/ack"):
            self.report_acknowledged = json["through_sequence"]
            return httpx.Response(
                200,
                json={"acknowledged_sequence": self.report_acknowledged},
                request=request,
            )
        self.forwarded_events.append(json)
        return httpx.Response(204, request=request)


class _AssociationAckHeldHarness(_ActionAppHarnessClient):
    """Hold a report ACK after server persistence to race a child action."""

    def __init__(self) -> None:
        super().__init__(held_prompt_turns={1})
        self.association_ack_started = asyncio.Event()
        self.release_association_ack = asyncio.Event()

    async def post(self, url: str, *, json: dict[str, Any], timeout: Any = None) -> httpx.Response:
        if url.endswith("/reports/ack") and json.get("through_sequence", 0) >= 1:
            self.association_ack_started.set()
            await self.release_association_ack.wait()
        return await super().post(url, json=json, timeout=timeout)


class _ActionAppProcessManager(_FakeProcessManager):
    def __init__(self, client: _ActionAppHarnessClient) -> None:
        super().__init__(client)
        self.background_lease_changes: list[tuple[str, str, bool]] = []
        self.report_opt_in: dict[str, bool] = {}
        self._report_runtime_incarnations: dict[str, int] = {}

    async def get_client(self, conversation_id: str, harness: str, env: Any = None) -> Any:
        had_process = self.has_session(conversation_id)
        client = await super().get_client(conversation_id, harness, env)
        if not had_process and self.report_opt_in.get(conversation_id, False):
            self._report_runtime_incarnations[conversation_id] = (
                self._report_runtime_incarnations.get(conversation_id, 0) + 1
            )
        return client

    async def set_session_report_opt_in(self, parent_id: str, enabled: bool) -> None:
        self.report_opt_in[parent_id] = enabled

    def get_session_report_subscriber_generation(self, parent_id: str) -> str | None:
        if not self.has_session(parent_id) or parent_id not in self._report_runtime_incarnations:
            return None
        return _GENERATION

    def get_session_report_runtime_incarnation(self, parent_id: str) -> int | None:
        if not self.has_session(parent_id):
            return None
        return self._report_runtime_incarnations.get(parent_id)

    async def register_background_activity(self, _parent_id: str, _activity_id: str) -> bool:
        self.background_lease_changes.append((_parent_id, _activity_id, True))
        return True

    async def clear_background_activity(self, parent_id: str, activity_id: str) -> None:
        self.background_lease_changes.append((parent_id, activity_id, False))


class _ActionAppServer:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.child_sessions: list[dict[str, Any]] = []
        self.association_child_ids: dict[str, str] = {}
        self.acp_session_associations: list[dict[str, Any]] = []
        self.acp_association_lookups: list[str] = []
        self.acp_association_page_requests: list[dict[str, Any]] = []
        self.session_workspace: str | None = None
        self.prepared_report_attempts: dict[str, str] = {}
        self.admitted_report_attempts: dict[str, str] = {}
        self.report_delivery_owners: dict[str, tuple[str, int, str]] = {}
        self.active_report_epochs: dict[str, tuple[str, int, str]] = {}

    def prepare_report_connection_attempt(self, session_id: str) -> str:
        """Provide one stable server-prepared admission identity for session init."""
        return self.prepared_report_attempts.setdefault(
            session_id,
            hashlib.sha256(f"acp-report-attempt:{session_id}".encode()).hexdigest()[:32],
        )

    def _has_admitted_report_delivery(self, session_id: str, report: dict[str, Any]) -> bool:
        owner = self.report_delivery_owners.get(session_id)
        if owner != (
            report.get("connection_admission_id"),
            report.get("runtime_incarnation"),
            report.get("journal_id"),
        ):
            return False
        return self.active_report_epochs.get(session_id) == (
            report.get("journal_id"),
            report.get("epoch_ordinal"),
            report.get("native_epoch"),
        )

    async def get(self, url: str, **_kwargs: Any) -> httpx.Response:
        request = httpx.Request("GET", f"http://server{url}")
        if url == "/v1/info":
            return httpx.Response(
                200,
                json={
                    "background_work_reporting": True,
                    "acp_session_reporting_version": 3,
                },
                request=request,
            )
        if url == "/v1/sessions":
            return httpx.Response(200, json={"data": [], "has_more": False}, request=request)
        if url.split("?", 1)[0].endswith("/child_sessions"):
            return httpx.Response(
                200,
                json={
                    "data": self.child_sessions,
                    "has_more": False,
                    "last_id": self.child_sessions[-1].get("id") if self.child_sessions else None,
                },
                request=request,
            )
        if url.split("?", 1)[0].endswith("/acp_session_associations"):
            self.acp_association_lookups.append(url.split("?", 1)[0])
            params = _kwargs.get("params")
            params = params if isinstance(params, dict) else {}
            self.acp_association_page_requests.append(dict(params))
            rows = sorted(
                self.acp_session_associations,
                key=lambda row: (row["native_epoch"], row["native_session_id"]),
            )
            after = params.get("after")
            start = 0
            if isinstance(after, str):
                after_identity = json.loads(after)
                start = next(
                    (
                        index + 1
                        for index, row in enumerate(rows)
                        if [row["native_epoch"], row["native_session_id"]] == after_identity
                    ),
                    len(rows),
                )
            limit = params.get("limit", 128)
            page_rows = rows[start : start + limit]
            first_id = (
                json.dumps(
                    [page_rows[0]["native_epoch"], page_rows[0]["native_session_id"]],
                    separators=(",", ":"),
                )
                if page_rows
                else None
            )
            last_id = (
                json.dumps(
                    [page_rows[-1]["native_epoch"], page_rows[-1]["native_session_id"]],
                    separators=(",", ":"),
                )
                if page_rows
                else None
            )
            return httpx.Response(
                200,
                json={
                    "data": page_rows,
                    "first_id": first_id,
                    "last_id": last_id,
                    "has_more": start + len(page_rows) < len(rows),
                },
                request=request,
            )
        if url.split("?", 1)[0].endswith("/action-origins"):
            return httpx.Response(200, json={"origins": []}, request=request)
        return httpx.Response(
            200,
            json={
                "id": "conv_child_action",
                "agent_id": "agent-1",
                "workspace": self.session_workspace,
            },
            request=request,
        )

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        request = httpx.Request("POST", f"http://server{url}")
        body = kwargs.get("json")
        if isinstance(body, dict):
            self.posts.append((url, body))
        if isinstance(body, dict) and body.get("type") == "external_acp_report_connection_admit":
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            data = body.get("data")
            attempt_id = data.get("attempt_id") if isinstance(data, dict) else None
            if attempt_id != self.prepared_report_attempts.get(session_id):
                return httpx.Response(
                    409,
                    json={"error": "report connection attempt was not prepared"},
                    request=request,
                )
            self.admitted_report_attempts[session_id] = attempt_id
            return httpx.Response(202, json={"admitted": True}, request=request)
        if isinstance(body, dict) and body.get("type") == "external_acp_report_owner_admit":
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            data = body.get("data")
            if not isinstance(data, dict):
                return httpx.Response(400, request=request)
            connection_id = data.get("connection_admission_id")
            incarnation = data.get("runtime_incarnation")
            journal_id = data.get("journal_id")
            if (
                connection_id != self.admitted_report_attempts.get(session_id)
                or isinstance(incarnation, bool)
                or not isinstance(incarnation, int)
                or incarnation < 1
                or not isinstance(journal_id, str)
                or len(journal_id) != 32
            ):
                return httpx.Response(409, request=request)
            self.report_delivery_owners[session_id] = (
                connection_id,
                incarnation,
                journal_id,
            )
            return httpx.Response(202, json={"admitted": True}, request=request)
        if isinstance(body, dict) and body.get("type") == "external_acp_report_epoch_activate":
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            data = body.get("data")
            if not isinstance(data, dict):
                return httpx.Response(400, request=request)
            owner = (
                data.get("connection_admission_id"),
                data.get("runtime_incarnation"),
                data.get("journal_id"),
            )
            epoch_ordinal = data.get("epoch_ordinal")
            native_epoch = data.get("native_epoch")
            if (
                owner != self.report_delivery_owners.get(session_id)
                or isinstance(epoch_ordinal, bool)
                or not isinstance(epoch_ordinal, int)
                or epoch_ordinal < 1
                or not isinstance(native_epoch, str)
                or not native_epoch
            ):
                return httpx.Response(409, request=request)
            self.active_report_epochs[session_id] = (
                str(data["journal_id"]),
                epoch_ordinal,
                native_epoch,
            )
            return httpx.Response(202, json={"active": True}, request=request)
        if url.endswith("/bind"):
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            return httpx.Response(
                200,
                json=_complete_action_origin_bind_result(session_id),
                request=request,
            )
        if url.endswith(("/settle", "/discard")):
            return httpx.Response(200, json={"settled": True}, request=request)
        if isinstance(body, dict) and body.get("type") == "external_acp_session_report":
            report = body.get("data")
            if not isinstance(report, dict):
                return httpx.Response(400, request=request)
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            if not self._has_admitted_report_delivery(session_id, report):
                return httpx.Response(409, request=request)
            if isinstance(report, dict) and report.get("kind") == "association.patch":
                target_id = report.get("target_session_id")
                if not isinstance(target_id, str) or not target_id:
                    raise AssertionError(body)
                child_id = self.association_child_ids.setdefault(target_id, f"child-{target_id}")
                return httpx.Response(
                    202,
                    json={"durable": True, "child_session_id": child_id},
                    request=request,
                )
            return httpx.Response(202, json={"durable": True}, request=request)
        if isinstance(body, dict) and body.get("type") == "external_acp_subagent_start":
            return httpx.Response(
                200,
                json={"child_session_id": "child-decline-test", "durable": True},
                request=request,
            )
        if isinstance(body, dict) and body.get("type") == "external_session_status":
            return httpx.Response(200, json={"durable": True}, request=request)
        return httpx.Response(202, request=request)


class _PagedAssociationInventoryServer(_ActionAppServer):
    """Serve keyset pages with one optional malformed-page behavior."""

    def __init__(self, rows: list[dict[str, Any]], *, fault: str | None = None) -> None:
        super().__init__()
        self.acp_session_associations = rows
        self.fault = fault
        self._sorted_rows = sorted(
            rows, key=lambda row: (row["native_epoch"], row["native_session_id"])
        )
        self._first_page: dict[str, Any] | None = None

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        if not url.split("?", 1)[0].endswith("/acp_session_associations"):
            return await super().get(url, **kwargs)

        request = httpx.Request("GET", f"http://server{url}")
        params = kwargs.get("params")
        params = params if isinstance(params, dict) else {}
        self.acp_association_lookups.append(url.split("?", 1)[0])
        self.acp_association_page_requests.append(dict(params))
        after = params.get("after")
        start = 0
        if isinstance(after, str):
            after_identity = json.loads(after)
            start = next(
                (
                    index + 1
                    for index, row in enumerate(self._sorted_rows)
                    if [row["native_epoch"], row["native_session_id"]] == after_identity
                ),
                len(self._sorted_rows),
            )
        limit = params.get("limit", 128)
        page_rows = [dict(row) for row in self._sorted_rows[start : start + limit]]
        page: dict[str, Any] = {
            "data": page_rows,
            "first_id": (
                json.dumps(
                    [page_rows[0]["native_epoch"], page_rows[0]["native_session_id"]],
                    separators=(",", ":"),
                )
                if page_rows
                else None
            ),
            "last_id": (
                json.dumps(
                    [page_rows[-1]["native_epoch"], page_rows[-1]["native_session_id"]],
                    separators=(",", ":"),
                )
                if page_rows
                else None
            ),
            "has_more": start + len(page_rows) < len(self._sorted_rows),
        }
        request_number = len(self.acp_association_page_requests)
        if request_number == 1:
            self._first_page = page
        if self.fault == "malformed_shape" and request_number == 1:
            page["data"] = None
        elif self.fault == "missing_cursor" and request_number == 1:
            page["last_id"] = None
            page["has_more"] = True
        elif self.fault == "repeated_cursor" and request_number == 2:
            assert self._first_page is not None
            page = self._first_page
        elif self.fault == "wrong_root" and request_number == 1:
            page_rows[0]["root_conversation_id"] = "different-root"
        return httpx.Response(200, json=page, request=request)


def _association_inventory_rows(
    parent_id: str,
    native_epoch: str,
    context_generation: str,
    *,
    child_count: int = 1,
) -> list[dict[str, Any]]:
    """Build a root row and the requested number of valid child bindings."""
    return [
        {
            "root_conversation_id": parent_id,
            "native_epoch": native_epoch,
            "native_session_id": _ROOT_NATIVE_SESSION_ID,
            "parent_native_session_id": None,
            "conversation_id": parent_id,
            "context_generation": context_generation,
            "association_source_id": None,
            "action_origin": None,
        },
        *[
            {
                "root_conversation_id": parent_id,
                "native_epoch": native_epoch,
                "native_session_id": f"native-inventory-{index:05d}",
                "parent_native_session_id": _ROOT_NATIVE_SESSION_ID,
                "conversation_id": f"conversation-inventory-{index:05d}",
                "context_generation": context_generation,
                "association_source_id": f"inventory-source-{index:05d}",
                "action_origin": None,
            }
            for index in range(child_count)
        ],
    ]


async def _post_report_session_init(
    client: httpx.AsyncClient,
    server: _ActionAppServer,
    session_id: str,
    *,
    agent_id: str = "agent-1",
) -> tuple[str, httpx.Response]:
    """Post one trusted init envelope and return its admission id and response."""
    from omnigent.entities import Conversation
    from omnigent.runner.session_init_protocol import build_runner_session_init_payload

    attempt_id = server.prepare_report_connection_attempt(session_id)
    conversation = Conversation(
        id=session_id,
        created_at=1,
        updated_at=1,
        agent_id=agent_id,
        root_conversation_id=session_id,
        workspace=server.session_workspace,
    )
    response = await client.post(
        "/v1/sessions",
        json=build_runner_session_init_payload(
            conversation,
            server_version="0.22.0-test",
            acp_report_connection_attempt_id=attempt_id,
        ),
    )
    return attempt_id, response


async def _initialize_report_session(
    client: httpx.AsyncClient,
    server: _ActionAppServer,
    session_id: str,
    *,
    agent_id: str = "agent-1",
) -> str:
    """Initialize through the trusted server envelope and admit its prepared attempt."""
    attempt_id, response = await _post_report_session_init(
        client, server, session_id, agent_id=agent_id
    )
    assert response.status_code == 201, response.text
    assert server.admitted_report_attempts[session_id] == attempt_id
    return attempt_id


def _complete_action_origin_bind_result(session_id: str) -> dict[str, Any]:
    """Return a bind snapshot matching the standard test agent's resolved context."""
    from omnigent.session_action_origin import (
        effective_policy_scope_digest,
        session_action_context_generation,
    )

    spec = AgentSpec(
        spec_version=1,
        name="test-agent",
        executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
    )
    policy_scope_digest = effective_policy_scope_digest(spec)
    access_scope: dict[str, Any] = {
        "conversation_id": session_id,
        "required_level": "edit",
        "permissions_enforced": False,
    }
    credential_identity_refs = {
        "authenticated_principal": "alice@example.com",
        "runner_id": "runner-1",
        "agent_id": "agent-1",
        "bundle_location": "captured-bundle.tar.gz",
        "mcp_selection_snapshot": {},
    }
    context_generation = session_action_context_generation(
        workspace_id=1,
        root_conversation_id=session_id,
        runner_id="runner-1",
        actor="alice@example.com",
        credential_principal="alice@example.com",
        agent_id="agent-1",
        bundle_location="captured-bundle.tar.gz",
        agent_provenance="tenant",
        operator_authored=False,
        effective_spec_digest="captured-spec-digest",
        operator_environment_digest="captured-environment-digest",
        policy_scope_digest=policy_scope_digest,
        mcp_selection_snapshot={},
        access_scope=access_scope,
        credential_identity_refs=credential_identity_refs,
    )
    return {
        "bound": True,
        "session_id": session_id,
        "workspace_id": 1,
        "runner_id": "runner-1",
        "credential_principal": "alice@example.com",
        "agent_id": "agent-1",
        "bundle_location": "captured-bundle.tar.gz",
        "agent_provenance": "tenant",
        "operator_authored": False,
        "operator_environment_digest": "captured-environment-digest",
        "policy_scope_digest": policy_scope_digest,
        "mcp_selection_snapshot": {},
        "effective_spec_digest": "captured-spec-digest",
        "context_generation": context_generation,
        "actor": "alice@example.com",
        "access_scope": access_scope,
        "credential_identity_refs": credential_identity_refs,
    }


def _build_acp_recovery_app(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    parent_id: str,
    harness: _ActionAppHarnessClient,
    server: _ActionAppServer,
) -> tuple[Any, _ActionAppProcessManager]:
    """Build a runner app with report recovery enabled for one fake session."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    process_manager = _ActionAppProcessManager(harness)
    config_home = tmp_path / f"{parent_id}-config"
    config_home.mkdir()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(config_home))
    (config_home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    return app, process_manager


def _acknowledged_association_harness(
    parent_id: str,
    native_epoch: str,
    native_session_id: str,
    context_generation: str,
    root_native_session_id: str = _ROOT_NATIVE_SESSION_ID,
) -> _ActionAppHarnessClient:
    """Provide one ACKed association so startup must restore server inventory."""
    harness = _ActionAppHarnessClient()
    harness.native_epoch = native_epoch
    harness.report_records.append(
        _association_report(
            sequence=1,
            root_id=parent_id,
            native_epoch=native_epoch,
            native_session_id=native_session_id,
            action_token="f" * 32,
            context_generation=context_generation,
            patch={"title": "worker", "state": {"state": "running"}},
            root_native_session_id=root_native_session_id,
        )
    )
    harness.report_acknowledged = 1
    return harness


class _SlowActionOriginInventoryServer(_ActionAppServer):
    """Hold the first settlement wave returned by the origin inventory."""

    def __init__(self, origins: list[dict[str, str]], slow_tokens: set[str]) -> None:
        super().__init__()
        self.origins = origins
        self.slow_tokens = slow_tokens
        self.settlement_attempts: dict[str, int] = {}
        self.settlement_bodies: dict[str, list[dict[str, Any]]] = {}
        self.settlement_task_names: dict[str, list[str]] = {}
        self.initial_settles_started: set[str] = set()
        self.all_initial_settles_started = asyncio.Event()
        self.release_settlements = asyncio.Event()

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        if url.split("?", 1)[0].endswith("/action-origins"):
            request = httpx.Request("GET", f"http://server{url}")
            return httpx.Response(200, json={"origins": self.origins}, request=request)
        return await super().get(url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        if url.endswith("/settle") and "/action-origins/" in url:
            token = url.split("/action-origins/", 1)[1].split("/", 1)[0]
            attempt = self.settlement_attempts.get(token, 0) + 1
            self.settlement_attempts[token] = attempt
            body = kwargs.get("json")
            assert isinstance(body, dict), kwargs
            self.settlement_bodies.setdefault(token, []).append(dict(body))
            task = asyncio.current_task()
            task_name = task.get_name() if task is not None else ""
            self.settlement_task_names.setdefault(token, []).append(task_name)
            request = httpx.Request("POST", f"http://server{url}")
            if token in self.slow_tokens:
                if attempt == 1:
                    self.initial_settles_started.add(token)
                    if self.initial_settles_started == self.slow_tokens:
                        self.all_initial_settles_started.set()
                await self.release_settlements.wait()
            return httpx.Response(200, json={"settled": True}, request=request)
        return await super().post(url, **kwargs)


@pytest.mark.asyncio
async def test_retired_origin_inventory_settlement_is_bounded_and_retried_before_first_prompt(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Slow stale-origin cleanup cannot hold startup, and pending tokens retry."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    conversation_id = "conv_slow_action_origin_inventory"
    retired_generation = "retired-action-generation"
    stale_tokens = {"a" * 28 + f"{index:04x}" for index in range(1, 4)}
    current_token = "c" * 32
    unbound_token = "d" * 32
    prompt_token = "e" * 32
    inventory = [
        {"token": token, "generation": retired_generation} for token in sorted(stale_tokens)
    ] + [{"token": current_token, "generation": _GENERATION}]
    server = _SlowActionOriginInventoryServer(inventory, stale_tokens)
    harness = _ActionAppHarnessClient(held_prompt_turns={1})
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    # Keep the same shared aggregate timeout semantics while making this test
    # fast. Per-origin HTTP timeouts are still much longer than this deadline.
    app.state.acp_action_origin_settlement_deadline_s = 0.2
    config_home = tmp_path / "inventory-config"
    config_home.mkdir()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(config_home))
    (config_home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "extension": "gemini",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )
    retry_waits: asyncio.Queue[float] = asyncio.Queue()
    release_retries = asyncio.Event()

    async def hold_retry(_delay: float) -> None:
        retry_waits.put_nowait(_delay)
        await release_retries.wait()

    app.state.acp_action_origin_retry_wait = hold_retry
    origin_states = app.state.acp_action_origin_generations
    origin_states[conversation_id] = {
        current_token: (_GENERATION, False),
        unbound_token: ("unbound-local-generation", False),
    }
    request_body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "test-agent",
        "content": [{"type": "input_text", "text": "start the parent turn"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
        "_omnigent_action_origin_token": prompt_token,
    }

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            started_at = asyncio.get_running_loop().time()
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events", json=request_body
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(harness.first_turn_started.wait(), timeout=1.5)
            assert asyncio.get_running_loop().time() - started_at < 1.5
            await asyncio.wait_for(server.all_initial_settles_started.wait(), timeout=1.0)

            for _ in stale_tokens:
                await asyncio.wait_for(retry_waits.get(), timeout=1.0)

            states = origin_states[conversation_id]
            assert all(states[token] == (retired_generation, True) for token in stale_tokens)
            assert states[current_token] == (_GENERATION, False)
            assert states[unbound_token] == ("unbound-local-generation", False)
            assert all(server.settlement_attempts[token] >= 1 for token in stale_tokens)
            assert current_token not in server.settlement_attempts
            assert unbound_token not in server.settlement_attempts

            release_retries.set()
            expected_retry_task_names = {
                token: f"acp-action-origin-retry:{conversation_id}:{token}"
                for token in stale_tokens
            }
            await poll_until_async(
                lambda: all(
                    expected_retry_task_names[token] in server.settlement_task_names.get(token, [])
                    for token in stale_tokens
                ),
                timeout_s=1.0,
                interval_s=0.001,
            )
            assert all(
                origin_states[conversation_id][token] == (retired_generation, True)
                for token in stale_tokens
            )
            assert set(server.settlement_bodies) == stale_tokens
            for token in stale_tokens:
                bodies = server.settlement_bodies[token]
                task_names = server.settlement_task_names[token]
                retry_index = task_names.index(expected_retry_task_names[token])
                assert bodies[0] == {"generation": retired_generation}, (token, bodies)
                assert bodies[retry_index] == {"generation": retired_generation}, (
                    token,
                    bodies,
                    task_names,
                )
                assert all(body == {"generation": retired_generation} for body in bodies), (
                    token,
                    bodies,
                )

            server.release_settlements.set()
            await poll_until_async(
                lambda: all(
                    token not in origin_states.get(conversation_id, {}) for token in stale_tokens
                ),
                timeout_s=2.0,
                interval_s=0.001,
            )
            assert all(server.settlement_attempts[token] >= 2 for token in stale_tokens)
            assert origin_states[conversation_id][current_token] == (_GENERATION, False)
            assert origin_states[conversation_id][unbound_token] == (
                "unbound-local-generation",
                False,
            )
            assert current_token not in server.settlement_attempts
            assert unbound_token not in server.settlement_attempts
            await poll_until_async(
                lambda: (
                    not any(
                        task.get_name().startswith(f"acp-action-origin-retry:{conversation_id}:")
                        for task in asyncio.all_tasks()
                    )
                ),
                timeout_s=2.0,
                interval_s=0.001,
            )

            harness.release_first_turn.set()
            await poll_until_async(
                lambda: harness.completed_turn_count == 1,
                timeout_s=2.0,
                interval_s=0.001,
            )
        finally:
            release_retries.set()
            server.release_settlements.set()
            harness.release_first_turn.set()
            with suppress(Exception):
                await asyncio.wait_for(
                    client.delete(f"/v1/sessions/{conversation_id}"), timeout=2.0
                )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


class _DeleteAppServer(_ActionAppServer):
    """Record durable child writes and action-origin settlement during DELETE."""

    def __init__(self) -> None:
        super().__init__()

    async def post(self, url: str, *, json: dict[str, Any], **_kwargs: Any) -> httpx.Response:
        if json.get("type") in {
            "external_acp_report_connection_admit",
            "external_acp_report_owner_admit",
            "external_acp_report_epoch_activate",
        }:
            return await super().post(url, json=json, **_kwargs)
        if json.get("type") == "external_acp_session_report":
            report = json.get("data")
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            if not isinstance(report, dict) or not self._has_admitted_report_delivery(
                session_id, report
            ):
                return httpx.Response(
                    409,
                    request=httpx.Request("POST", f"http://server{url}"),
                )
        self.posts.append((url, json))
        payload: dict[str, Any] = {"durable": True}
        if url.endswith("/bind"):
            session_id = url.split("/v1/sessions/", 1)[1].split("/", 1)[0]
            payload.update(_complete_action_origin_bind_result(session_id))
        if json.get("type") == "external_acp_session_report":
            report = json.get("data")
            if isinstance(report, dict) and report.get("kind") == "association.patch":
                target_id = report["target_session_id"]
                child_id = self.association_child_ids.setdefault(target_id, "child-delete-test")
                payload["child_session_id"] = child_id
                child_key = acp_session_child_key(
                    report["root_conversation_id"], report["native_epoch"], target_id
                )
                patch = report["operation"].get("patch", {})
                child_summary = {
                    "id": child_id,
                    "parent_session_id": report["root_conversation_id"],
                    "title": f"{patch.get('title', 'worker')}:{child_key}",
                    "labels": {"omnigent.acp.subagent_id": child_key},
                    "status": "running",
                    "busy": True,
                    "current_task_status": "in_progress",
                }
                existing = next(
                    (row for row in self.child_sessions if row.get("id") == child_id), None
                )
                if existing is None:
                    self.child_sessions.append(child_summary)
                else:
                    existing.update(child_summary)
        if url.endswith(("/settle", "/discard")):
            payload["settled"] = True
        return httpx.Response(
            200,
            json=payload,
            request=httpx.Request("POST", f"http://server{url}"),
        )


class _OriginCleanupServer(_DeleteAppServer):
    """Record origin settlement and optionally fail one token once."""

    def __init__(self) -> None:
        super().__init__()
        self.settlement_attempts: list[str] = []
        self.fail_once: set[str] = set()
        self._failed_tokens: set[str] = set()

    async def post(self, url: str, *, json: dict[str, Any], **kwargs: Any) -> httpx.Response:
        if url.endswith("/settle"):
            token = url.split("/action-origins/", 1)[1].split("/", 1)[0]
            self.settlement_attempts.append(token)
            if token in self.fail_once and token not in self._failed_tokens:
                self._failed_tokens.add(token)
                return httpx.Response(
                    503,
                    json={"error": "temporary settlement failure"},
                    request=httpx.Request("POST", f"http://server{url}"),
                )
        return await super().post(url, json=json, **kwargs)


class _BoundedSettlementServer(_DeleteAppServer):
    """Fail one origin repeatedly and hold another request until teardown ends."""

    def __init__(self, retry_token: str, held_token: str) -> None:
        super().__init__()
        self.retry_token = retry_token
        self.held_token = held_token
        self.settlement_attempts: dict[str, int] = {}
        self.held_request_started = asyncio.Event()
        self.release_held_request = asyncio.Event()

    async def post(self, url: str, *, json: dict[str, Any], **kwargs: Any) -> httpx.Response:
        if url.endswith("/settle"):
            token = url.split("/action-origins/", 1)[1].split("/", 1)[0]
            attempt = self.settlement_attempts.get(token, 0) + 1
            self.settlement_attempts[token] = attempt
            request = httpx.Request("POST", f"http://server{url}")
            if token == self.retry_token and attempt <= 4:
                return httpx.Response(
                    503,
                    json={"error": "settlement unavailable"},
                    request=request,
                )
            if token == self.held_token and attempt == 1:
                self.held_request_started.set()
                await self.release_held_request.wait()
            return httpx.Response(200, json={"settled": True}, request=request)
        return await super().post(url, json=json, **kwargs)


@pytest.mark.asyncio
async def test_action_origin_snapshot_mismatch_fails_before_harness_prompt() -> None:
    """A queued turn cannot prompt with a bundle that differs from its origin."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    harness = _ActionAppHarnessClient()
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]

    class MismatchingActionOriginServer(_ActionAppServer):
        def __init__(self) -> None:
            super().__init__()
            self.bound = asyncio.Event()
            self.cleaned = asyncio.Event()
            self.calls: list[tuple[str, dict[str, Any] | None]] = []

        async def post(self, url: str, **kwargs: Any) -> httpx.Response:
            body = kwargs.get("json")
            self.calls.append((url, body if isinstance(body, dict) else None))
            request = httpx.Request("POST", f"http://server{url}")
            if isinstance(body, dict) and body.get("type") in {
                "external_acp_report_connection_admit",
                "external_acp_report_owner_admit",
                "external_acp_report_epoch_activate",
                "external_acp_session_report",
            }:
                return await super().post(url, **kwargs)
            if url.endswith("/bind"):
                self.bound.set()
                return httpx.Response(
                    200,
                    json={
                        "bound": True,
                        "agent_id": "agent-1",
                        "bundle_location": "captured-bundle.tar.gz",
                        "agent_provenance": "tenant",
                        "operator_authored": False,
                        "operator_environment_digest": "captured-environment-digest",
                        "mcp_selection_snapshot": {},
                        "effective_spec_digest": "captured-spec-digest",
                        "actor": "alice@example.com",
                    },
                    request=request,
                )
            if url.endswith(("/settle", "/discard")):
                self.cleaned.set()
                return httpx.Response(200, json={"settled": True}, request=request)
            return httpx.Response(202, request=request)

    server = MismatchingActionOriginServer()

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(spec_version=1, name="current-agent"),
            workdir=None,
            bundle_location="newer-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="current-spec-digest",
            operator_environment_digest="current-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    conversation_id = "conv_action_origin_mismatch"
    origin_token = "a" * 32
    request_body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "current-agent",
        "content": [{"type": "input_text", "text": "queued work"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
        "_omnigent_action_origin_token": origin_token,
    }

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events", json=request_body
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(server.bound.wait(), timeout=5.0)
            await asyncio.wait_for(server.cleaned.wait(), timeout=5.0)

            assert harness.prompt_bodies == []
            assert any(
                event.get("type") == "response.failed"
                and event.get("error", {}).get("type") == "action_origin_spec_mismatch"
                for event in app.state.session_event_queues[conversation_id]._queue
            )
            assert any(url.endswith("/settle") for url, _body in server.calls)
            assert next(body for url, body in server.calls if url.endswith("/bind")) == {
                "generation": _GENERATION
            }
            assert next(body for url, body in server.calls if url.endswith("/settle")) == {
                "generation": _GENERATION
            }
            assert not process_manager.has_active_turn(conversation_id)
        finally:
            app.state.session_event_queues.pop(conversation_id, None)
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task


@pytest.mark.asyncio
async def test_detached_child_tool_keeps_captured_session_workspace(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct and MCP child-tool dispatch keep the bound session workspace."""
    from omnigent.runner import tool_dispatch
    from omnigent.runner.native.orchestration import ResolvedSpec

    conversation_id = "conv_action_workspace_snapshot"
    origin_token = "f" * 32
    runner_root = tmp_path / "runner-root"
    captured_workspace = tmp_path / "session-workspace-before"
    changed_workspace = tmp_path / "session-workspace-after"
    bundle_workdir = tmp_path / "bundle-workdir"
    for directory in (runner_root, captured_workspace, changed_workspace, bundle_workdir):
        directory.mkdir()

    harness = _ActionAppHarnessClient(held_prompt_turns={1})
    harness.native_epoch = "epoch-1"
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]
    server = _ActionAppServer()
    server.session_workspace = str(captured_workspace)

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=bundle_workdir,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    config_home = tmp_path / "workspace-config"
    config_home.mkdir()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(config_home))
    (config_home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "extension": "gemini",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
        runner_workspace=runner_root,
    )
    direct_calls: list[dict[str, Any]] = []

    async def capture_direct_dispatch(**kwargs: Any) -> None:
        direct_calls.append(kwargs)
        await kwargs["on_result_delivery"](
            {"type": "tool_result", "call_id": kwargs["call_id"], "output": "ok"}
        )

    monkeypatch.setattr(tool_dispatch, "dispatch_tool_locally", capture_direct_dispatch)
    request_body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "test-agent",
        "content": [{"type": "input_text", "text": "start the parent turn"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
        "_omnigent_action_origin_token": origin_token,
    }

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events", json=request_body
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(harness.first_turn_started.wait(), timeout=5.0)
            assert app.state.session_workspace_cache[conversation_id] == str(captured_workspace)

            association = _association_report(
                sequence=1,
                root_id=conversation_id,
                native_epoch="epoch-1",
                native_session_id="native-session-1",
                action_token=origin_token,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
                patch={"title": "worker", "state": {"state": "running"}},
            )
            harness.report_records.append(association)
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: (
                    harness.report_acknowledged == 1
                    and any(
                        body.get("type") == "external_acp_session_report"
                        for _url, body in server.posts
                    )
                ),
                timeout_s=5.0,
            )

            # Model a server workspace change after the ACP origin has captured
            # its immutable runtime context.
            server.session_workspace = str(changed_workspace)
            app.state.session_workspace_cache[conversation_id] = str(changed_workspace)
            await asyncio.wait_for(harness.action_poll_started.wait(), timeout=5.0)
            action = _tool_action(
                origin_token=origin_token,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
            )
            action["child"]["association_source_id"] = association["source_id"]
            await harness.action_queue.put(action)
            await asyncio.wait_for(harness.action_result.wait(), timeout=5.0)

            assert direct_calls[0]["runner_workspace"] == captured_workspace.resolve()
            assert direct_calls[0]["runner_workspace"] != runner_root.resolve()
            assert direct_calls[0]["local_tool_workdir"] == bundle_workdir

            action_context = app.state.acp_session_action_contexts[conversation_id][origin_token]
            assert action_context.runner_workspace == captured_workspace.resolve()
            assert action_context.workdir == bundle_workdir
            fallback_calls: list[dict[str, Any]] = []

            async def capture_mcp_fallback(**kwargs: Any) -> str:
                fallback_calls.append(kwargs)
                return "ok"

            monkeypatch.setattr(tool_dispatch, "execute_tool", capture_mcp_fallback)
            fallback = await client.post(
                f"/v1/sessions/{conversation_id}/mcp/execute",
                json={
                    "method": "tools/call",
                    "params": {
                        "name": "example_tool",
                        "arguments": {},
                        "_omnigent_action_context_token": origin_token,
                        "_omnigent_action_context_generation": _GENERATION,
                    },
                },
            )

            assert fallback.status_code == 200, fallback.text
            assert fallback_calls[0]["runner_workspace"] == captured_workspace.resolve()
            assert fallback_calls[0]["local_tool_workdir"] == bundle_workdir
        finally:
            harness.release_first_turn.set()
            await poll_until_async(
                lambda: not process_manager.has_active_turn(conversation_id), timeout_s=5.0
            )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


def _sse(event: dict[str, Any]) -> str:
    return f"data: {json.dumps(event)}\n\n"


@pytest.mark.asyncio
async def test_child_decline_resolves_without_interrupting_a_newer_parent_turn(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A child approval decline cannot route through the generic parent interrupt."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    harness = _ActionAppHarnessClient(held_prompt_turns={1, 2})
    harness.native_epoch = "epoch-1"
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    monkeypatch_config = tmp_path / "origin-config"
    monkeypatch_config.mkdir(parents=True)
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(monkeypatch_config))
    (monkeypatch_config / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "extension": "gemini",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )

    server = _ActionAppServer()
    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    conversation_id = "conv_child_action"
    child_key = acp_session_child_key(conversation_id, "epoch-1", "native-session-1")
    child_id = "child-decline-test"
    server.association_child_ids["native-session-1"] = child_id
    first_origin = "c" * 32
    second_origin = "d" * 32
    request_body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "test-agent",
        "content": [{"type": "input_text", "text": "parent work"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
        "_omnigent_action_origin_token": first_origin,
    }
    second_request_body = {
        **request_body,
        "_omnigent_action_origin_token": second_origin,
    }

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            first = await client.post(f"/v1/sessions/{conversation_id}/events", json=request_body)
            assert first.status_code == 202, first.text
            await asyncio.wait_for(harness.first_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.report_poll_started.wait(), timeout=5.0)
            # Model a real detached child that has already started before the
            # parent's first turn returns. The reporter's active source status
            # keeps the first origin live across the subsequent parent turn.
            harness.report_source_status = {"state": "active", "prompt_started": True}
            association = _association_report(
                sequence=1,
                root_id=conversation_id,
                native_epoch="epoch-1",
                native_session_id="native-session-1",
                action_token=first_origin,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
                patch={
                    "title": "worker",
                    "description": "inspect the active child fixture",
                    "state": {"state": "running"},
                },
            )
            harness.report_records.append(association)
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: (
                    harness.report_acknowledged == 1
                    and any(
                        body.get("type") == "external_acp_session_report"
                        for _url, body in server.posts
                    )
                ),
                timeout_s=5.0,
            )
            start_body = next(
                body
                for _url, body in server.posts
                if body.get("type") == "external_acp_session_report"
            )
            assert start_body["data"]["_omnigent_action_origin"] == {
                "token": first_origin,
                "agent": "agent-1",
                "generation": _GENERATION,
            }
            server.child_sessions.append(
                {
                    "id": child_id,
                    "parent_session_id": conversation_id,
                    "title": f"worker:{child_key}",
                    "labels": {"omnigent.acp.subagent_id": child_key},
                    "status": "running",
                    "busy": True,
                    "current_task_status": "in_progress",
                }
            )
            harness.release_first_turn.set()
            await poll_until_async(
                lambda: conversation_id in process_manager.cleared_in_flight,
                timeout_s=5.0,
            )
            assert first_origin in app.state.acp_action_origin_generations[conversation_id]
            assert not any(
                url.endswith(f"/action-origins/{first_origin}/settle")
                for url, _body in server.posts
            )

            second = await client.post(
                f"/v1/sessions/{conversation_id}/events", json=second_request_body
            )
            assert second.status_code == 202, second.text
            await asyncio.wait_for(harness.second_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.action_poll_started.wait(), timeout=5.0)
            assert process_manager.has_active_turn(conversation_id)

            assert harness.prompt_bodies[0]["_omnigent_action_origin_token"] == first_origin
            approval_action = _approval_action(
                action_id="child-mcp-ask",
                elicitation_id="child-mcp-elicitation",
                origin_token=first_origin,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
            )
            approval_action["event"]["params"] = {
                "requestedSchema": {
                    "type": "object",
                    "properties": {"answer": {"type": "string", "enum": ["Allow once"]}},
                },
                "content_preview": "Allow this child to continue?",
            }
            approval_action["child"]["association_source_id"] = association["source_id"]
            await harness.action_queue.put(approval_action)
            queue = app.state.session_event_queues[conversation_id]
            while True:
                event = await asyncio.wait_for(queue.get(), timeout=5.0)
                if event.get("type") == "response.elicitation_request":
                    assert event.get("elicitation_id") == "child-mcp-elicitation"
                    assert event.get("params", {}).get("target_session_id") == child_id
                    break

            decline = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={
                    "type": "approval",
                    "elicitation_id": "child-mcp-elicitation",
                    "action": "decline",
                },
            )
            assert decline.status_code == 204, decline.text
            await asyncio.wait_for(harness.action_result.wait(), timeout=5.0)

            assert process_manager.has_active_turn(conversation_id)
            assert not any(event.get("type") == "interrupt" for event in harness.forwarded_events)
            assert harness.action_results[-1]["result"] == {
                "type": "approval",
                "elicitation_id": "child-mcp-elicitation",
                "action": "decline",
            }
        finally:
            harness.release_first_turn.set()
            harness.release_second_turn.set()
            await poll_until_async(
                lambda: not process_manager.has_active_turn(conversation_id), timeout_s=5.0
            )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


@pytest.mark.asyncio
async def test_init_reconciles_acknowledged_active_child_as_unconfirmed_without_prompt(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical ACKed child linkage stays unconfirmed without a status guess or lease."""
    parent_id = "conv_acknowledged_active_child"
    native_epoch = "epoch-acknowledged-child"
    root_native_session_id = "native-z-acknowledged-root"
    native_session_id = "native-a-acknowledged-child"
    child_key = acp_session_child_key(parent_id, native_epoch, native_session_id)
    child_id = "conv_acknowledged_child"
    context_generation = _complete_action_origin_bind_result(parent_id)["context_generation"]
    harness = _acknowledged_association_harness(
        parent_id,
        native_epoch,
        native_session_id,
        context_generation,
        root_native_session_id,
    )
    harness.report_records[0]["data"]["operation"]["patch"].update(
        {"description": "already acknowledged before runner restart"}
    )
    harness.report_records.append(
        {
            "sequence": 2,
            "journal_id": _JOURNAL_ID,
            "epoch_ordinal": 1,
            "native_epoch": native_epoch,
            "ingress_sequence": 2,
            "source_id": acp_standard_report_source_id(
                _SERVER_IDENTITY, parent_id, native_epoch, 2
            ),
            "kind": "entity.upsert",
            "data": {
                "server_identity": _SERVER_IDENTITY,
                "root_conversation_id": parent_id,
                "context_generation": context_generation,
                "root_native_session_id": root_native_session_id,
                "target_session_id": native_session_id,
                "parent_session_id": root_native_session_id,
                "operation": {
                    "entity_type": "message",
                    "entity_id": "paged-inventory-child-message",
                    "value": {
                        "item_type": "message",
                        "response_id": "paged-inventory-child-response",
                        "item_data": {
                            "role": "assistant",
                            "agent": "agent-1",
                            "acp_agent_identity": "unknown",
                            "content": [{"type": "text", "text": "Recovered child update."}],
                        },
                    },
                },
            },
        }
    )
    # The prior generation already ACKed this association. Its old active
    # summary is unconfirmed until the current generation emits a fresh report.
    harness.report_acknowledged = 1
    server = _ActionAppServer()
    server.association_child_ids[native_session_id] = child_id
    server.child_sessions = [
        {
            "id": child_id,
            "parent_session_id": parent_id,
            "title": f"worker:{child_key}",
            "labels": {"omnigent.acp.subagent_id": child_key},
            # The global status may lag the parent-scoped task summary.
            "status": "completed",
            "busy": True,
            "current_task_status": "in_progress",
        }
    ]
    server.acp_session_associations = [
        {
            "root_conversation_id": parent_id,
            "native_epoch": native_epoch,
            "native_session_id": root_native_session_id,
            "parent_native_session_id": None,
            "conversation_id": parent_id,
            "context_generation": context_generation,
            "association_source_id": None,
            "action_origin": None,
        },
        *[
            {
                "root_conversation_id": parent_id,
                "native_epoch": native_epoch,
                "native_session_id": f"native-inventory-{index:03d}",
                "parent_native_session_id": root_native_session_id,
                "conversation_id": f"conv_inventory_{index:03d}",
                "context_generation": context_generation,
                "association_source_id": f"inventory-source-{index:03d}",
                "action_origin": None,
            }
            for index in range(128)
        ],
        {
            "root_conversation_id": parent_id,
            "native_epoch": native_epoch,
            "native_session_id": native_session_id,
            "parent_native_session_id": root_native_session_id,
            "conversation_id": child_id,
            "context_generation": context_generation,
            "association_source_id": harness.report_records[0]["source_id"],
            "action_origin": {
                "token": "f" * 32,
                "agent": "agent-1",
                "generation": _GENERATION,
            },
        },
    ]

    app, process_manager = _build_acp_recovery_app(
        tmp_path, monkeypatch, parent_id, harness, server
    )

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, parent_id)
            assert harness.prompt_bodies == []
            assert harness.report_acknowledged == 2
            association_path = f"/v1/sessions/{parent_id}/acp_session_associations"
            assert server.acp_association_lookups == [association_path, association_path]
            assert server.acp_association_page_requests == [
                {"limit": 128},
                {
                    "limit": 128,
                    "after": json.dumps(
                        [native_epoch, "native-inventory-126"], separators=(",", ":")
                    ),
                },
            ]
            assert not any(
                body.get("type") == "external_session_status" for _url, body in server.posts
            )
            recovered_child_reports = [
                body["data"]
                for _url, body in server.posts
                if body.get("type") == "external_acp_session_report"
            ]
            assert len(recovered_child_reports) == 1
            assert recovered_child_reports[0]["sequence"] == 2
            assert recovered_child_reports[0]["target_session_id"] == native_session_id
            assert not any(
                body.get("type") == "external_acp_subagent_start" for _url, body in server.posts
            )
            assert process_manager.background_lease_changes == []
            assert server.child_sessions[0]["current_task_status"] == "in_progress"
        finally:
            with suppress(Exception):
                await client.delete(f"/v1/sessions/{parent_id}")
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(parent_id, None)


@pytest.mark.parametrize(
    ("fault", "expected_error"),
    [
        ("malformed_shape", "malformed ACP association page"),
        ("missing_cursor", "invalid ACP association page cursors"),
        ("repeated_cursor", "server repeated an ACP association inventory row"),
        ("wrong_root", "invalid ACP association inventory row"),
        ("wrong_epoch", "no durable parent"),
    ],
)
@pytest.mark.asyncio
async def test_report_recovery_rejects_malformed_or_out_of_scope_inventory_pages(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
    expected_error: str,
) -> None:
    """Startup refuses stalled pages and inventory rows outside the report tree."""
    import omnigent.runner.app as runner_app

    parent_id = f"conv_acp_inventory_{fault}"
    native_epoch = "epoch-acp-inventory-recovery"
    native_session_id = "native-inventory-00000"
    context_generation = _complete_action_origin_bind_result(parent_id)["context_generation"]
    harness = _acknowledged_association_harness(
        parent_id, native_epoch, native_session_id, context_generation
    )
    rows = _association_inventory_rows(
        parent_id,
        native_epoch,
        context_generation,
        child_count=129 if fault == "repeated_cursor" else 1,
    )
    rows[1]["association_source_id"] = harness.report_records[0]["source_id"]
    rows[1]["action_origin"] = harness.report_records[0]["data"]["_omnigent_action_origin"]
    if fault == "wrong_epoch":
        rows[1]["native_epoch"] = "different-native-epoch"
    server = _PagedAssociationInventoryServer(rows, fault=fault)
    monkeypatch.setattr(
        runner_app, "_client_safe_error_detail", lambda error, **_kwargs: str(error)
    )
    app, _process_manager = _build_acp_recovery_app(
        tmp_path, monkeypatch, parent_id, harness, server
    )

    async with _runner_client(app) as client:
        try:
            _attempt_id, response = await _post_report_session_init(client, server, parent_id)
            assert response.status_code == 503, response.text
            assert expected_error in response.json()["detail"]
            assert server.acp_association_page_requests
            if fault == "repeated_cursor":
                assert len(server.acp_association_page_requests) == 2
        finally:
            with suppress(Exception):
                await client.delete(f"/v1/sessions/{parent_id}")
            app.state.session_event_queues.pop(parent_id, None)


@pytest.mark.asyncio
async def test_report_recovery_refuses_inventory_over_ten_thousand_rows(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An oversized inventory fails after bounded 128-row pages are consumed."""
    import omnigent.runner.app as runner_app

    parent_id = "conv_acp_inventory_capacity"
    native_epoch = "epoch-acp-inventory-capacity"
    native_session_id = "native-inventory-00000"
    context_generation = _complete_action_origin_bind_result(parent_id)["context_generation"]
    harness = _acknowledged_association_harness(
        parent_id, native_epoch, native_session_id, context_generation
    )
    rows = _association_inventory_rows(
        parent_id,
        native_epoch,
        context_generation,
        child_count=10_000,
    )
    rows[1]["association_source_id"] = harness.report_records[0]["source_id"]
    rows[1]["action_origin"] = harness.report_records[0]["data"]["_omnigent_action_origin"]
    server = _PagedAssociationInventoryServer(rows)
    monkeypatch.setattr(
        runner_app, "_client_safe_error_detail", lambda error, **_kwargs: str(error)
    )
    app, _process_manager = _build_acp_recovery_app(
        tmp_path, monkeypatch, parent_id, harness, server
    )

    async with _runner_client(app) as client:
        try:
            _attempt_id, response = await _post_report_session_init(client, server, parent_id)
            assert response.status_code == 503, response.text
            assert "inventory exceeds its recovery capacity" in response.json()["detail"]
            assert len(server.acp_association_page_requests) > 1
        finally:
            with suppress(Exception):
                await client.delete(f"/v1/sessions/{parent_id}")
            app.state.session_event_queues.pop(parent_id, None)


@pytest.mark.asyncio
async def test_child_action_waits_for_its_association_source_ack(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persisted child row is not enough until its exact association ACK arrives."""
    import omnigent.runner.tool_dispatch as tool_dispatch
    from omnigent.runner.native.orchestration import ResolvedSpec

    conversation_id = "conv_action_association_ack_barrier"
    native_epoch = "epoch-action-association-ack"
    native_session_id = "native-child-action-association-ack"
    origin_token = "b" * 32
    context_generation = _complete_action_origin_bind_result(conversation_id)["context_generation"]
    association = _association_report(
        sequence=1,
        root_id=conversation_id,
        native_epoch=native_epoch,
        native_session_id=native_session_id,
        action_token=origin_token,
        context_generation=context_generation,
        patch={"title": "worker", "state": {"state": "running"}},
    )
    association_source_id = association["source_id"]
    harness = _AssociationAckHeldHarness()
    harness.native_epoch = native_epoch
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]
    server = _ActionAppServer()

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    config_home = tmp_path / "association-ack-config"
    config_home.mkdir()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(config_home))
    (config_home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "extension": "gemini",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )
    direct_calls: list[dict[str, Any]] = []

    async def capture_direct_dispatch(**kwargs: Any) -> None:
        direct_calls.append(kwargs)
        await kwargs["on_result_delivery"](
            {"type": "tool_result", "call_id": kwargs["call_id"], "output": "ok"}
        )

    monkeypatch.setattr(tool_dispatch, "dispatch_tool_locally", capture_direct_dispatch)
    request_body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "test-agent",
        "content": [{"type": "input_text", "text": "start the parent turn"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
        "_omnigent_action_origin_token": origin_token,
    }
    action = _tool_action(
        action_id="association-ack-action",
        event_id="association-ack-event",
        origin_token=origin_token,
        context_generation=context_generation,
    )
    action["child"].update(
        {
            "native_epoch": native_epoch,
            "native_session_id": native_session_id,
            "run_id": "session",
            "association_source_id": association_source_id,
        }
    )

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events", json=request_body
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(harness.first_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.report_poll_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.action_poll_started.wait(), timeout=5.0)

            harness.report_records.append(association)
            await harness.action_queue.put(action)
            harness.report_poll_release.set()
            await asyncio.wait_for(harness.association_ack_started.wait(), timeout=5.0)

            action_subscriber = app.state.acp_session_action_subscribers[conversation_id]
            await poll_until_async(lambda: action_subscriber.is_busy, timeout_s=2.0)
            await asyncio.sleep(0.05)
            assert direct_calls == []
            assert not harness.action_result.is_set()

            harness.release_association_ack.set()
            await asyncio.wait_for(harness.action_result.wait(), timeout=5.0)
            assert len(direct_calls) == 1
            assert harness.action_results[-1]["result"]["call_id"] == "call-1"
        finally:
            harness.release_first_turn.set()
            harness.release_association_ack.set()
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: not process_manager.has_active_turn(conversation_id), timeout_s=5.0
            )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


@pytest.mark.asyncio
async def test_child_approval_waits_for_its_association_source_ack(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Approval prompts are not routed to a child before its exact association ACK."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    conversation_id = "conv_approval_association_ack_barrier"
    native_epoch = "epoch-approval-association-ack"
    native_session_id = "native-child-approval-association-ack"
    origin_token = "c" * 32
    context_generation = _complete_action_origin_bind_result(conversation_id)["context_generation"]
    association = _association_report(
        sequence=1,
        root_id=conversation_id,
        native_epoch=native_epoch,
        native_session_id=native_session_id,
        action_token=origin_token,
        context_generation=context_generation,
        patch={"title": "worker", "state": {"state": "running"}},
    )
    harness = _AssociationAckHeldHarness()
    harness.native_epoch = native_epoch
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]
    server = _ActionAppServer()

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    config_home = tmp_path / "approval-association-ack-config"
    config_home.mkdir()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(config_home))
    (config_home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "extension": "gemini",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )
    approval_action = _approval_action(
        action_id="association-ack-approval",
        elicitation_id="association-ack-elicitation",
        origin_token=origin_token,
        context_generation=context_generation,
    )
    approval_action["child"].update(
        {
            "native_epoch": native_epoch,
            "native_session_id": native_session_id,
            "run_id": "session",
            "association_source_id": association["source_id"],
        }
    )
    approval_action["event"]["params"] = {
        "requestedSchema": {"type": "object", "properties": {}},
        "content_preview": "Allow the worker to continue?",
    }

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={
                    "type": "message",
                    "role": "user",
                    "agent_id": "agent-1",
                    "model": "test-agent",
                    "content": [{"type": "input_text", "text": "start the parent turn"}],
                    "harness": "acp",
                    "caller_user_id": "alice@example.com",
                    "_omnigent_action_origin_token": origin_token,
                },
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(harness.first_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.report_poll_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.action_poll_started.wait(), timeout=5.0)

            harness.report_records.append(association)
            await harness.action_queue.put(approval_action)
            harness.report_poll_release.set()
            await asyncio.wait_for(harness.association_ack_started.wait(), timeout=5.0)

            subscriber = app.state.acp_session_action_subscribers[conversation_id]
            await poll_until_async(lambda: subscriber.is_busy, timeout_s=2.0)
            await asyncio.sleep(0.05)
            event_queue = app.state.session_event_queues[conversation_id]
            assert not any(
                isinstance(event, dict) and event.get("type") == "response.elicitation_request"
                for event in event_queue._queue
            )

            harness.release_association_ack.set()
            while True:
                event = await asyncio.wait_for(event_queue.get(), timeout=5.0)
                if event.get("type") == "response.elicitation_request":
                    assert event.get("elicitation_id") == "association-ack-elicitation"
                    assert (
                        event.get("params", {}).get("target_session_id")
                        == (server.association_child_ids[native_session_id])
                    )
                    break

            declined = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={
                    "type": "approval",
                    "elicitation_id": "association-ack-elicitation",
                    "action": "decline",
                },
            )
            assert declined.status_code == 204, declined.text
            await asyncio.wait_for(harness.action_result.wait(), timeout=5.0)
            assert harness.action_results[-1]["result"] == {
                "type": "approval",
                "elicitation_id": "association-ack-elicitation",
                "action": "decline",
            }
        finally:
            harness.release_first_turn.set()
            harness.release_association_ack.set()
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: not process_manager.has_active_turn(conversation_id), timeout_s=5.0
            )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


@pytest.mark.asyncio
async def test_delete_settles_durable_acp_children_before_releasing_parent_harness(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deletion releases child leases and origins without guessing child outcomes."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    harness = _ActionAppHarnessClient(held_prompt_turns={1, 2})
    harness.native_epoch = "epoch-delete-test"
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]
    server = _DeleteAppServer()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Gemini Test",
                            "command": "gemini --experimental-acp",
                            "extension": "gemini",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:gemini-test"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    conversation_id = "conv_delete_acp_child"
    origin_token = "d" * 32
    second_origin_token = "e" * 32
    request_body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "test-agent",
        "content": [{"type": "input_text", "text": "parent work"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
        "_omnigent_action_origin_token": origin_token,
    }
    second_request_body = {
        **request_body,
        "_omnigent_action_origin_token": second_origin_token,
    }
    origins_settled_at_release: set[str] = set()
    original_release = process_manager.release

    async def record_release(*args: Any, **kwargs: Any) -> None:
        origins_settled_at_release.update(
            url.split("/action-origins/", 1)[1].split("/", 1)[0]
            for url, _payload in server.posts
            if "/action-origins/" in url and url.endswith("/settle")
        )
        await original_release(*args, **kwargs)

    monkeypatch.setattr(process_manager, "release", record_release)
    deleted = False

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            first = await client.post(f"/v1/sessions/{conversation_id}/events", json=request_body)
            assert first.status_code == 202, first.text
            await asyncio.wait_for(harness.first_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.report_poll_started.wait(), timeout=5.0)

            harness.report_source_status = {"state": "active", "prompt_started": True}
            harness.report_records.append(
                _association_report(
                    sequence=1,
                    root_id=conversation_id,
                    native_epoch="epoch-delete-test",
                    native_session_id="native-delete-test",
                    action_token=origin_token,
                    context_generation=_complete_action_origin_bind_result(conversation_id)[
                        "context_generation"
                    ],
                    patch={
                        "title": "worker",
                        "description": "complete one bounded task",
                        "state": {"state": "running"},
                    },
                )
            )
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: (
                    harness.report_acknowledged == 1
                    and any(
                        payload.get("type") == "external_acp_session_report"
                        for _url, payload in server.posts
                    )
                ),
                timeout_s=5.0,
            )
            harness.release_first_turn.set()
            await poll_until_async(
                lambda: (
                    harness.completed_turn_count == 1
                    and process_manager.cleared_in_flight.count(conversation_id) >= 1
                ),
                timeout_s=5.0,
            )
            second = await client.post(
                f"/v1/sessions/{conversation_id}/events", json=second_request_body
            )
            assert second.status_code == 202, second.text
            await asyncio.wait_for(harness.second_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.action_poll_started.wait(), timeout=5.0)
            assert [
                prompt["_omnigent_action_origin_token"] for prompt in harness.prompt_bodies
            ] == [origin_token, second_origin_token]

            delete = await asyncio.wait_for(
                client.delete(f"/v1/sessions/{conversation_id}"), timeout=2.0
            )
            assert delete.status_code == 200, delete.text
            deleted = True

            statuses = [
                payload["data"]["status"]
                for _url, payload in server.posts
                if payload.get("type") == "external_session_status"
            ]
            assert statuses == []
            assert len(server.child_sessions) == 1
            assert server.child_sessions[0]["current_task_status"] == "in_progress"
            assert origin_token in origins_settled_at_release
            assert harness.report_acknowledged == 1
            assert len(process_manager.background_lease_changes) == 2
            assert [change[2] for change in process_manager.background_lease_changes] == [
                True,
                False,
            ]
            assert (
                process_manager.background_lease_changes[0][:2]
                == (process_manager.background_lease_changes[1][:2])
            )
            assert harness.prompt_bodies[0]["_omnigent_action_origin_token"] == origin_token
            assert any(
                url.endswith(f"/action-origins/{origin_token}/settle")
                for url, _payload in server.posts
            )
            assert process_manager.released == [conversation_id]
            assert not process_manager.has_session(conversation_id)
        finally:
            harness.release_second_turn.set()
            if not deleted:
                with suppress(Exception):
                    await asyncio.wait_for(
                        client.delete(f"/v1/sessions/{conversation_id}"), timeout=2.0
                    )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


@pytest.mark.asyncio
async def test_finished_origins_wait_for_report_and_action_barriers_across_260_turns(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-child origins drain after report/action barriers and survive transient failure."""
    from omnigent.runner.native.orchestration import ResolvedSpec

    conversation_id = "conv_origin_cleanup_churn"
    origin_tokens = [f"{value:032x}" for value in range(1, 261)]
    assert len(set(origin_tokens)) == 260
    assert all(len(token.encode("ascii")) == 32 for token in origin_tokens)
    assert sum(len(token.encode("ascii")) for token in origin_tokens) == 8_320

    harness = _ActionAppHarnessClient(held_prompt_turns={260, 261})
    harness.native_epoch = "epoch-1"
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]
    server = _OriginCleanupServer()
    persisted_child_key = acp_session_child_key(conversation_id, "epoch-1", "native-session-1")
    persisted_child_id = "child-origin-cleanup-approval"
    server.association_child_ids["native-session-1"] = persisted_child_id
    server.fail_once.add(origin_tokens[-1])
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Synthetic ACP",
                            "command": "synthetic-acp --stdio",
                            "extension": "synthetic",
                            "inject_system_prompt": False,
                            "omnigent_mcp": False,
                        }
                    ]
                }
            }
        )
    )

    async def resolve_spec(_agent_id: str, _session_id: str | None = None) -> ResolvedSpec:
        return ResolvedSpec(
            spec=AgentSpec(
                spec_version=1,
                name="test-agent",
                executor=ExecutorSpec(type="omnigent", config={"harness": "acp:synthetic-acp"}),
            ),
            workdir=None,
            bundle_location="captured-bundle.tar.gz",
            operator_authored=False,
            effective_spec_digest="captured-spec-digest",
            operator_environment_digest="captured-environment-digest",
            mcp_selection_snapshot={},
        )

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        spec_resolver=resolve_spec,
        server_client=server,  # type: ignore[arg-type]
    )
    hold_report_drain = False
    watch_action_drain = False
    report_drain_started = asyncio.Event()
    release_report_drain = asyncio.Event()
    action_drain_started = asyncio.Event()
    original_report_drain = AcpSessionReportSubscriber.drain_to_high_water
    original_action_drain = AcpSessionActionSubscriber.drain_origins

    async def gated_report_drain(
        subscriber: AcpSessionReportSubscriber, *, timeout_s: float = 10.0
    ) -> bool:
        if subscriber.parent_id == conversation_id and hold_report_drain:
            report_drain_started.set()
            await release_report_drain.wait()
        return await original_report_drain(subscriber, timeout_s=timeout_s)

    async def gated_action_drain(
        subscriber: AcpSessionActionSubscriber,
        origin_tokens: set[str],
        *,
        timeout_s: float = 10.0,
    ) -> bool:
        if (
            subscriber.parent_id == conversation_id
            and watch_action_drain
            and final_token in origin_tokens
        ):
            action_drain_started.set()
        return await original_action_drain(subscriber, origin_tokens, timeout_s=timeout_s)

    monkeypatch.setattr(AcpSessionReportSubscriber, "drain_to_high_water", gated_report_drain)
    monkeypatch.setattr(AcpSessionActionSubscriber, "drain_origins", gated_action_drain)
    origin_state = app.state.acp_action_origin_generations
    body = {
        "type": "message",
        "role": "user",
        "agent_id": "agent-1",
        "model": "test-agent",
        "content": [{"type": "input_text", "text": "successful no-child turn"}],
        "harness": "acp",
        "caller_user_id": "alice@example.com",
    }

    async with _runner_client(app) as client:
        try:
            await _initialize_report_session(client, server, conversation_id)
            for turn, token in enumerate(origin_tokens[:-1], start=1):
                response = await client.post(
                    f"/v1/sessions/{conversation_id}/events",
                    json={**body, "_omnigent_action_origin_token": token},
                )
                assert response.status_code == 202, response.text
                await poll_until_async(
                    lambda turn=turn, token=token: (
                        harness.completed_turn_count >= turn
                        and process_manager.cleared_in_flight.count(conversation_id) >= turn
                        and token not in origin_state.get(conversation_id, {})
                    ),
                    timeout_s=5.0,
                    interval_s=0.001,
                )

            final_token = origin_tokens[-1]
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={**body, "_omnigent_action_origin_token": final_token},
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(harness.second_turn_started.wait(), timeout=5.0)
            await asyncio.wait_for(harness.action_poll_started.wait(), timeout=5.0)

            first_association = _association_report(
                sequence=1,
                root_id=conversation_id,
                native_epoch="epoch-1",
                native_session_id="native-session-1",
                action_token=final_token,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
                patch={"title": "worker", "state": {"state": "running"}},
            )
            harness.report_records.append(first_association)
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: (
                    harness.report_acknowledged == 1
                    and any(
                        payload.get("type") == "external_acp_session_report"
                        for _url, payload in server.posts
                    )
                ),
                timeout_s=5.0,
            )
            assert any(
                child.get("id") == persisted_child_id
                and child.get("labels", {}).get("omnigent.acp.subagent_id") == persisted_child_key
                and child.get("current_task_status") == "in_progress"
                for child in server.child_sessions
            )

            approval_action = _approval_action(
                action_id="held-cleanup-approval",
                elicitation_id="held-cleanup-elicitation",
                origin_token=final_token,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
            )
            approval_action["event"]["params"] = {
                "requestedSchema": {
                    "type": "object",
                    "properties": {"answer": {"type": "string", "enum": ["Allow once"]}},
                },
                "content_preview": "Allow the persisted child to continue?",
            }
            approval_action["child"]["association_source_id"] = first_association["source_id"]
            await harness.action_queue.put(approval_action)
            event_queue = app.state.session_event_queues[conversation_id]
            await poll_until_async(
                lambda: any(
                    event.get("type") == "response.elicitation_request"
                    and event.get("elicitation_id") == "held-cleanup-elicitation"
                    for event in event_queue._queue
                ),
                timeout_s=5.0,
                interval_s=0.001,
            )
            approval_event = next(
                event
                for event in event_queue._queue
                if event.get("type") == "response.elicitation_request"
                and event.get("elicitation_id") == "held-cleanup-elicitation"
            )
            assert approval_event.get("params", {}).get("target_session_id") == persisted_child_id

            harness.release_second_turn.set()
            await poll_until_async(
                lambda: (
                    harness.completed_turn_count == 260
                    and process_manager.cleared_in_flight.count(conversation_id) >= 260
                ),
                timeout_s=5.0,
                interval_s=0.001,
            )
            assert final_token in origin_state.get(conversation_id, {})
            assert final_token not in server.settlement_attempts

            later_token = f"{261:032x}"
            harness.second_turn_started.clear()
            harness.release_second_turn.clear()
            response = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={**body, "_omnigent_action_origin_token": later_token},
            )
            assert response.status_code == 202, response.text
            await asyncio.wait_for(harness.second_turn_started.wait(), timeout=5.0)
            refreshed_association = _association_report(
                sequence=2,
                root_id=conversation_id,
                native_epoch="epoch-1",
                native_session_id="native-session-1",
                action_token=later_token,
                context_generation=_complete_action_origin_bind_result(conversation_id)[
                    "context_generation"
                ],
                patch={"title": "worker", "state": {"state": "running"}},
            )
            harness.report_records.append(refreshed_association)
            harness.report_poll_release.set()
            await poll_until_async(
                lambda: (
                    harness.report_acknowledged == 2
                    and sum(
                        payload.get("type") == "external_acp_session_report"
                        for _url, payload in server.posts
                    )
                    == 2
                ),
                timeout_s=5.0,
            )

            hold_report_drain = True
            watch_action_drain = True
            harness.release_second_turn.set()
            await asyncio.wait_for(report_drain_started.wait(), timeout=5.0)
            await poll_until_async(
                lambda: (
                    harness.completed_turn_count == 261
                    and process_manager.cleared_in_flight.count(conversation_id) >= 261
                ),
                timeout_s=5.0,
                interval_s=0.001,
            )
            assert final_token in origin_state.get(conversation_id, {})
            assert final_token not in server.settlement_attempts

            release_report_drain.set()
            await asyncio.wait_for(action_drain_started.wait(), timeout=5.0)
            assert final_token in origin_state.get(conversation_id, {})
            assert final_token not in server.settlement_attempts

            approval = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={
                    "type": "approval",
                    "elicitation_id": "held-cleanup-elicitation",
                    "action": "accept",
                    "content": {"confirmation": "yes"},
                },
            )
            assert approval.status_code == 204, approval.text
            await asyncio.wait_for(harness.action_result.wait(), timeout=5.0)
            await poll_until_async(
                lambda: final_token not in origin_state.get(conversation_id, {}),
                timeout_s=5.0,
                interval_s=0.001,
            )
            action_subscriber = app.state.acp_session_action_subscribers[conversation_id]
            assert final_token not in action_subscriber._origin_handlers
            assert final_token not in app.state.acp_session_action_contexts.get(
                conversation_id, {}
            )
            assert server.settlement_attempts.count(final_token) == 2
            assert later_token in origin_state.get(conversation_id, {})
            assert not any(
                payload.get("type") == "external_acp_subagent_start"
                for _url, payload in server.posts
            )

            later_token = f"{262:032x}"
            later = await client.post(
                f"/v1/sessions/{conversation_id}/events",
                json={**body, "_omnigent_action_origin_token": later_token},
            )
            assert later.status_code == 202, later.text
            await poll_until_async(
                lambda: (
                    harness.completed_turn_count >= 262
                    and process_manager.cleared_in_flight.count(conversation_id) >= 262
                    and later_token not in origin_state.get(conversation_id, {})
                ),
                timeout_s=5.0,
                interval_s=0.001,
            )
        finally:
            release_report_drain.set()
            harness.release_second_turn.set()
            with suppress(Exception):
                await asyncio.wait_for(
                    client.delete(f"/v1/sessions/{conversation_id}"), timeout=2.0
                )
            for task in (harness.action_poll_task, harness.report_poll_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            app.state.session_event_queues.pop(conversation_id, None)


@pytest.mark.asyncio
async def test_origin_cleanup_bounds_release_and_retries_retired_tokens() -> None:
    """Teardown releases a held harness promptly and retains origins for retry."""
    retry_token = "a" * 32
    held_token = "b" * 32
    conversation_id = "conv_bounded_origin_cleanup"
    harness = _ActionAppHarnessClient()
    process_manager = _ActionAppProcessManager(harness)  # type: ignore[arg-type]
    server = _BoundedSettlementServer(retry_token, held_token)

    app = create_runner_app(
        process_manager=process_manager,  # type: ignore[arg-type]
        server_client=server,  # type: ignore[arg-type]
    )
    app.state.acp_action_origin_settlement_deadline_s = 3.0
    retry_release = asyncio.Event()
    retry_wait_calls: asyncio.Queue[float] = asyncio.Queue()

    async def controlled_retry_wait(delay: float) -> None:
        retry_wait_calls.put_nowait(delay)
        await retry_release.wait()

    app.state.acp_action_origin_retry_wait = controlled_retry_wait
    origins = app.state.acp_action_origin_generations
    origins[conversation_id] = {
        retry_token: (_GENERATION, True),
        held_token: (_GENERATION, True),
    }

    async with _runner_client(app) as client:
        try:
            delete = await asyncio.wait_for(
                client.delete(f"/v1/sessions/{conversation_id}"), timeout=5.0
            )
            assert delete.status_code == 200, delete.text
            assert server.held_request_started.is_set()
            assert process_manager.released == [conversation_id]
            assert server.settlement_attempts[retry_token] == 4
            assert server.settlement_attempts[held_token] == 1
            assert set(origins[conversation_id]) == {retry_token, held_token}

            await asyncio.wait_for(retry_wait_calls.get(), timeout=1.0)
            await asyncio.wait_for(retry_wait_calls.get(), timeout=1.0)
            assert set(origins[conversation_id]) == {retry_token, held_token}

            server.release_held_request.set()
            retry_release.set()
            await poll_until_async(
                lambda: conversation_id not in origins,
                timeout_s=2.0,
                interval_s=0.001,
            )
            assert server.settlement_attempts[retry_token] == 5
            assert server.settlement_attempts[held_token] == 2
        finally:
            server.release_held_request.set()
            retry_release.set()
            origins.pop(conversation_id, None)
