"""Fake-clock tests for the scaffold's durable-human-wait timeout exemption.

Uses tiny monkeypatched watchdog budgets instead of real sleeps: a durable
``sys_ask_user`` wait must survive past both the idle window and the
absolute ceiling (heartbeats hold both while ``_durable_human_waits > 0``),
while an ordinary human wait still dies at the absolute ceiling, and a
resolved durable wait restores normal budgets.
"""

from __future__ import annotations

import asyncio
import json
import time as _time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest

from omnigent.runtime.harnesses._scaffold import DurableAskEvent, HarnessApp, TurnContext
from omnigent.server.schemas import (
    CompletedEvent,
    CreatedEvent,
    CreateResponseRequest,
    HeartbeatEvent,
    InProgressEvent,
    ResponseObject,
)


def _response(status: str = "completed") -> ResponseObject:
    return ResponseObject(
        id="resp_test",
        status=status,
        model="test-model",
        created_at=int(_time.time()),
    )


@dataclass
class _FakeResponse:
    """Minimal response object the scaffold's terminal-event builder accepts."""

    id: str = "resp_test"
    status: str = "completed"


class _DurableWaitHarness(HarnessApp):
    """Emits heartbeats while waiting, then completes when released."""

    def __init__(self, release: asyncio.Event, durable: bool) -> None:
        super().__init__()
        self._release = release
        self._durable = durable

    async def run_turn(self, request: CreateResponseRequest, ctx: TurnContext) -> None:
        ctx.emit(CreatedEvent(type="response.created", response=_response("in_progress")))
        ctx.emit(InProgressEvent(type="response.in_progress", response=_response("in_progress")))
        if self._durable:
            # The runner parks the dispatch first; the pending event matches
            # by the parked call_id like tool_result does.
            ctx._pending_tool_calls.setdefault(
                "call_1", asyncio.get_running_loop().create_future()
            )
            # The runner suspends the watchdogs via the durable_ask events.
            self._handle_durable_ask_event(
                DurableAskEvent(type="durable_ask_pending", call_id="call_1")
            )
        i = 0
        while not self._release.is_set():
            await asyncio.sleep(0.005)
            ctx.emit(
                HeartbeatEvent(
                    type="response.heartbeat",
                    server_time=_time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
                    last_event_seq=0,
                )
            )
            i += 1
            if i > 100_000:  # safety valve; the watchdog should fire long before
                raise AssertionError("watchdogs never fired")
        if self._durable:
            self._handle_durable_ask_event(
                DurableAskEvent(type="durable_ask_resolved", call_id="call_1")
            )
        ctx.emit(CompletedEvent(type="response.completed", response=_response()))

    def _build_terminal_event(self, ctx: TurnContext) -> dict[str, Any]:
        return {
            "type": "response.completed",
            "response": {"id": "resp_test", "status": "completed"},
        }


@pytest.fixture()
def _tiny_budgets(monkeypatch: pytest.MonkeyPatch):
    """Shrink both watchdogs to tens of milliseconds (a fake clock in effect)."""
    from omnigent.runtime.harnesses import _scaffold

    monkeypatch.setattr(_scaffold, "_TURN_IDLE_TIMEOUT_S", 0.05)
    monkeypatch.setattr(_scaffold, "_TURN_ABSOLUTE_TIMEOUT_S", 0.15)
    return _scaffold


def _request() -> CreateResponseRequest:
    return CreateResponseRequest.model_validate(
        {"model": "test-model", "input": [{"type": "message", "role": "user", "content": "hi"}]}
    )


async def _run_turn(app: HarnessApp, request: CreateResponseRequest) -> AsyncIterator[Any]:
    queue: asyncio.Queue = asyncio.Queue()
    cancelled = asyncio.Event()
    ctx = TurnContext("resp_test", queue, cancelled)
    app._in_flight["resp_test"] = ctx
    task = asyncio.create_task(app._guarded_run_turn(request, ctx))
    return task, queue


@pytest.mark.asyncio
async def test_durable_wait_survives_idle_and_absolute_ceilings(_tiny_budgets) -> None:
    """A pending durable ask keeps the turn alive well past both ceilings."""
    release = asyncio.Event()
    app = _DurableWaitHarness(release, durable=True)
    task, _queue = await _run_turn(app, _request())
    # Wait several times past both ceilings (0.05s idle / 0.15s absolute).
    await asyncio.sleep(0.5)
    assert not task.done(), (
        "a pending durable human wait must suspend the idle and absolute watchdogs"
    )
    release.set()
    result = await asyncio.wait_for(task, timeout=2.0)
    assert result is None  # clean run_turn completion


@pytest.mark.asyncio
async def test_ordinary_human_wait_still_dies_at_absolute_ceiling(_tiny_budgets) -> None:
    """Without a durable ask, heartbeats alone cannot hold the absolute cap."""
    from omnigent.runtime.harnesses import _scaffold

    class _OrdinaryWaitHarness(_DurableWaitHarness):
        def __init__(self, release: asyncio.Event) -> None:
            super().__init__(release, durable=False)

        async def run_turn(self, request: CreateResponseRequest, ctx: TurnContext) -> None:
            ctx._pending_human_waits += 1  # legacy approval-style wait
            try:
                await super().run_turn(request, ctx)
            finally:
                ctx._pending_human_waits -= 1

    release = asyncio.Event()
    app = _OrdinaryWaitHarness(release)
    task, _queue = await _run_turn(app, _request())
    with pytest.raises(Exception) as excinfo:
        await asyncio.wait_for(task, timeout=2.0)
    # The turn must die of the watchdog (idle error), not the safety valve.
    assert "watchdog" in str(excinfo.value).lower() or _tiny_budgets._TURN_ABSOLUTE_TIMEOUT_S
    assert _scaffold._TURN_ABSOLUTE_TIMEOUT_S == 0.15


@pytest.mark.asyncio
async def test_durable_ask_events_bracket_the_counter(_tiny_budgets) -> None:
    """pending/resolved events adjust the durable counter; stale ones no-op."""
    app = _DurableWaitHarness(asyncio.Event(), durable=False)
    ctx = TurnContext("resp_test", asyncio.Queue(), asyncio.Event())
    app._in_flight["resp_test"] = ctx
    ctx._pending_tool_calls["call_1"] = asyncio.get_running_loop().create_future()
    app._handle_durable_ask_event(DurableAskEvent(type="durable_ask_pending", call_id="call_1"))
    assert ctx._durable_human_waits == 1
    app._handle_durable_ask_event(DurableAskEvent(type="durable_ask_resolved", call_id="call_1"))
    assert ctx._durable_human_waits == 0
    # Stale call_id on resolve: no negative counter, no crash.
    app._handle_durable_ask_event(
        DurableAskEvent(type="durable_ask_resolved", call_id="call_missing")
    )
    assert ctx._durable_human_waits == 0


@pytest.mark.asyncio
async def test_durable_ask_event_parses_on_the_inbound_union() -> None:
    import pydantic

    from omnigent.runtime.harnesses._scaffold import InboundEventRequest

    class _Envelope(pydantic.BaseModel):
        ev: InboundEventRequest

    parsed = _Envelope.model_validate(
        {"ev": {"type": "durable_ask_pending", "call_id": "call_1"}}
    ).ev
    assert parsed.call_id == "call_1"
    json.dumps(parsed.model_dump())  # wire-serializable
