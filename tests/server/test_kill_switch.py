"""Tests for the service kill switch: controller semantics and app enforcement."""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.server.app import _wire_kill_switch_sweep
from omnigent.server.kill_switch import (
    DEFAULT_MESSAGE,
    FlagReading,
    KillSwitchController,
    resolve_kill_switch,
)
from tests.server.helpers import create_test_agent


class FakeSource:
    """Flag source whose reading the test sets directly."""

    def __init__(self, reading: FlagReading, delay_s: float = 0.0) -> None:
        self.reading = reading
        self.delay_s = delay_s
        self.closed = False

    def evaluate(self, last_known: bool) -> FlagReading:
        if self.delay_s:
            import time

            time.sleep(self.delay_s)
        return self.reading

    def close(self) -> None:
        self.closed = True


class RecordingMeter:
    """Minimal OTel meter capturing counter adds by (name, attributes)."""

    def __init__(self) -> None:
        self.adds: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self.gauges: dict[str, Any] = {}

    def create_counter(self, name: str, **_kwargs: Any) -> Any:
        meter = self

        class _Counter:
            def add(self, amount: int, attributes: dict[str, str] | None = None) -> None:
                meter.adds[(name, tuple(sorted((attributes or {}).items())))] += amount

        return _Counter()

    def create_observable_gauge(self, name: str, callbacks: list[Any], **_kwargs: Any) -> None:
        self.gauges[name] = callbacks[0]

    def gauge(self, name: str) -> list[float]:
        return [observation.value for observation in self.gauges[name](None)]


def _controller(source: FakeSource | None, **kwargs: Any) -> tuple[KillSwitchController, Any]:
    meter = RecordingMeter()
    return KillSwitchController(source, meter=meter, poll_interval_s=0.01, **kwargs), meter


@pytest.mark.asyncio
async def test_unconfigured_is_off_and_never_rejects() -> None:
    controller, _meter = _controller(None)
    await controller.start()
    assert controller.state.source_status == "disabled"
    assert not controller.active
    controller.require_enabled("create_session")
    await controller.stop()


@pytest.mark.asyncio
async def test_engage_disengage_and_keep_last_known_when_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = FakeSource(FlagReading(engaged=False))
    controller, meter = _controller(source, message="maintenance")
    await controller.refresh()
    assert controller.state.source_status == "ready"
    assert meter.gauge("omnigent.kill_switch.engaged") == [0]

    source.reading = FlagReading(engaged=True)
    with caplog.at_level("INFO"):
        await controller.refresh()
    assert "kill_switch_engaged" in caplog.text
    assert meter.gauge("omnigent.kill_switch.engaged") == [1]
    with pytest.raises(OmnigentError) as excinfo:
        controller.require_enabled("create_session")
    assert excinfo.value.code == ErrorCode.SERVICE_DISABLED
    assert excinfo.value.http_status == 503
    assert str(excinfo.value) == "maintenance"
    assert (
        meter.adds[("omnigent.kill_switch.rejected_requests", (("operation", "create_session"),))]
        == 1
    )

    # Datadog unreachable: stay engaged, report the source as unavailable.
    source.reading = FlagReading(engaged=None, error="PROVIDER_NOT_READY: down")
    await controller.refresh()
    assert controller.engaged
    assert controller.state.source_status == "unavailable"
    assert controller.state.last_error == "PROVIDER_NOT_READY: down"
    assert meter.adds[("omnigent.kill_switch.flag_refresh", (("outcome", "unavailable"),))] == 1

    source.reading = FlagReading(engaged=False)
    with caplog.at_level("INFO"):
        await controller.refresh()
    assert "kill_switch_disengaged" in caplog.text
    controller.require_enabled("create_session")
    await controller.stop()
    assert source.closed


@pytest.mark.asyncio
async def test_init_timeout_starts_disengaged_then_polls() -> None:
    source = FakeSource(FlagReading(engaged=True), delay_s=0.2)
    controller, _meter = _controller(source, init_timeout_s=0.01)
    await controller.start()
    assert not controller.engaged
    source.delay_s = 0.0
    for _ in range(100):
        if controller.engaged:
            break
        await asyncio.sleep(0.01)
    assert controller.engaged
    await controller.stop()


def test_resolve_env_force_and_validation() -> None:
    forced = resolve_kill_switch({"OMNIGENT_KILL_SWITCH_FORCE": "engaged"})
    assert forced.engaged and forced.state.source_status == "forced"
    assert forced.state.message == DEFAULT_MESSAGE
    assert not resolve_kill_switch({}).active
    with pytest.raises(ValueError, match="FORCE"):
        resolve_kill_switch({"OMNIGENT_KILL_SWITCH_FORCE": "on"})
    with pytest.raises(ValueError, match="POLL_INTERVAL"):
        resolve_kill_switch({"OMNIGENT_KILL_SWITCH_POLL_INTERVAL_S": "0"})


def test_datadog_source_maps_openfeature_details() -> None:
    pytest.importorskip("openfeature")
    from openfeature.provider.in_memory_provider import InMemoryFlag, InMemoryProvider

    from omnigent.server.kill_switch_datadog import DatadogFlagSource

    provider = InMemoryProvider({"omnigent-kill": InMemoryFlag("on", {"on": True, "off": False})})
    source = DatadogFlagSource("omnigent-kill", environ={}, provider=provider)
    assert source.evaluate(False) == FlagReading(engaged=True)
    missing = DatadogFlagSource("absent", environ={}, provider=provider)
    reading = missing.evaluate(True)
    assert reading.engaged is None and reading.error is not None
    source.close()


@pytest.mark.asyncio
async def test_app_refuses_new_work_halts_turns_and_keeps_reads(
    app: FastAPI,
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omnigent.server.routes import sessions as sessions_module
    from omnigent.server.routes._sessions.common import _session_status_cache

    interrupts: list[str] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/events"):
            interrupts.append(request.url.path)
        return httpx.Response(204)

    fake_runner = httpx.AsyncClient(transport=httpx.MockTransport(_handler), base_url="http://r")

    async def _fake_get_runner_client(
        session_id: str, runner_router: object, *, conversation: Any = None
    ) -> httpx.AsyncClient:
        return fake_runner

    monkeypatch.setattr(sessions_module, "_get_runner_client", _fake_get_runner_client)

    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201, created.text
    session_id = created.json()["id"]

    source = FakeSource(FlagReading(engaged=False))
    controller, meter = _controller(source)
    app.state.kill_switch = controller
    _wire_kill_switch_sweep(app)
    _session_status_cache[session_id] = "running"
    try:
        source.reading = FlagReading(engaged=True)
        await controller.refresh()
        # Engaging halts the in-flight turn, and each later poll sweeps again.
        assert interrupts == [f"/v1/sessions/{session_id}/events"]
        await controller.refresh()
        assert len(interrupts) == 2
        assert meter.adds[("omnigent.kill_switch.halted_turns", (("outcome", "delivered"),))] == 2

        message = {
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
        }
        for method, path, body in (
            ("POST", "/v1/sessions", {"agent_id": agent["id"]}),
            ("POST", f"/v1/sessions/{session_id}/fork", {}),
            ("POST", f"/v1/sessions/{session_id}/events", message),
        ):
            resp = await client.request(method, path, json=body)
            assert resp.status_code == 503, (path, resp.text)
            assert "service_disabled" in resp.text

        health = await client.get("/health")
        assert health.status_code == 200
        assert health.json()["kill_switch"] == "engaged"
        status = await client.get("/v1/kill-switch")
        assert status.json()["engaged"] is True
        assert status.json()["message"] == DEFAULT_MESSAGE
        assert (await client.get(f"/v1/sessions/{session_id}")).status_code == 200
        interrupt = await client.post(
            f"/v1/sessions/{session_id}/events", json={"type": "interrupt", "data": {}}
        )
        assert interrupt.status_code == 202

        source.reading = FlagReading(engaged=False)
        _session_status_cache.pop(session_id, None)
        await controller.refresh()
        resp = await client.post(f"/v1/sessions/{session_id}/events", json=message)
        assert resp.status_code != 503, resp.text
        assert (await client.get("/health")).json()["kill_switch"] == "disengaged"
    finally:
        _session_status_cache.pop(session_id, None)
        await fake_runner.aclose()
