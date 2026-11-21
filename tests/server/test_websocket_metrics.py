"""Tests for WebSocket connection metrics published through OpenTelemetry."""

from __future__ import annotations

from dataclasses import dataclass, field

from opentelemetry.util.types import Attributes

from omnigent.server.websocket_metrics import (
    WebSocketMetricsOtelPublisher,
    ws_route_template,
)

# ---------------------------------------------------------------------------
# Fake OTEL instruments (mirrors the pattern in test_performance_metrics.py)
# ---------------------------------------------------------------------------


@dataclass
class _MetricRecord:
    """Recorded metric value from a fake OpenTelemetry instrument."""

    amount: int | float
    attributes: Attributes


@dataclass
class _FakeCounter:
    """Fake OpenTelemetry counter that records ``add`` calls."""

    name: str
    records: list[_MetricRecord] = field(default_factory=list)

    def add(self, amount: int | float, attributes: Attributes = None) -> None:
        self.records.append(_MetricRecord(amount=amount, attributes=attributes))


@dataclass
class _FakeHistogram:
    """Fake OpenTelemetry histogram that records ``record`` calls."""

    name: str
    records: list[_MetricRecord] = field(default_factory=list)

    def record(self, amount: int | float, attributes: Attributes = None) -> None:
        self.records.append(_MetricRecord(amount=amount, attributes=attributes))


@dataclass
class _FakeMeter:
    """Fake OpenTelemetry meter that creates recording instruments."""

    counters: dict[str, _FakeCounter] = field(default_factory=dict)
    histograms: dict[str, _FakeHistogram] = field(default_factory=dict)

    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _FakeCounter:
        counter = _FakeCounter(name)
        self.counters[name] = counter
        return counter

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _FakeHistogram:
        histogram = _FakeHistogram(name)
        self.histograms[name] = histogram
        return histogram


# ---------------------------------------------------------------------------
# ws_route_template tests
# ---------------------------------------------------------------------------


def _ws_scope(path: str) -> dict:
    """Build a minimal ASGI WebSocket scope for route resolver tests."""
    return {
        "type": "websocket",
        "path": path,
        "raw_path": path.encode("ascii"),
    }


def test_ws_route_template_dictation_stream() -> None:
    """Dictation stream route resolves to its template."""
    assert ws_route_template(_ws_scope("/dictation/stream")) == "/dictation/stream"


def test_ws_route_template_sessions_updates() -> None:
    """Session updates route resolves to its template."""
    assert ws_route_template(_ws_scope("/sessions/updates")) == "/sessions/updates"


def test_ws_route_template_terminal_attach() -> None:
    """Terminal attach route resolves to a parameterized template."""
    scope = _ws_scope("/sessions/abc123/resources/terminals/t1/attach")
    result = ws_route_template(scope)
    assert result == "/sessions/{session_id}/resources/terminals/{terminal_id}/attach"


def test_ws_route_template_terminal_attach_no_leaked_ids() -> None:
    """The resolved template does not contain the raw session or terminal ID."""
    scope = _ws_scope("/sessions/abc123/resources/terminals/t1/attach")
    result = ws_route_template(scope)
    assert "abc123" not in result
    assert "t1" not in result


def test_ws_route_template_runner_tunnel() -> None:
    """Runner tunnel route resolves to its template."""
    assert ws_route_template(_ws_scope("/runner/tunnel")) == "/runner/tunnel"


def test_ws_route_template_host_tunnel() -> None:
    """Host tunnel route resolves to a parameterized template."""
    scope = _ws_scope("/hosts/my-host-42/tunnel")
    result = ws_route_template(scope)
    assert result == "/hosts/{host_id}/tunnel"
    assert "my-host-42" not in result


def test_ws_route_template_unknown_path() -> None:
    """An unrecognized path resolves to 'unknown'."""
    assert ws_route_template(_ws_scope("/v1/some/other/path")) == "unknown"


def test_ws_route_template_uses_matched_route_object() -> None:
    """When scope carries a matched route object, its path is used directly."""

    class _FakeRoute:
        path = "/custom/route"

    scope = _ws_scope("/custom/route")
    scope["route"] = _FakeRoute()
    assert ws_route_template(scope) == "/custom/route"


# ---------------------------------------------------------------------------
# WebSocketMetricsOtelPublisher tests
# ---------------------------------------------------------------------------


def test_record_connection_emits_expected_attributes() -> None:
    """record_connection emits a histogram point with all expected attributes."""
    meter = _FakeMeter()
    publisher = WebSocketMetricsOtelPublisher(meter=meter)

    publisher.record_connection(
        route="/dictation/stream",
        actor_user_id="user@example.com",
        duration_seconds=12.5,
        close_code=1000,
        outcome="closed",
    )

    records = meter.histograms["omnigent.server.websocket.connection.duration"].records
    assert len(records) == 1
    attrs = records[0].attributes
    assert attrs["ws.route"] == "/dictation/stream"
    assert attrs["omnigent.actor.user_id"] == "user@example.com"
    assert attrs["ws.close_code"] == 1000
    assert attrs["ws.outcome"] == "closed"
    assert records[0].amount == 12.5


def test_record_connection_user_id_fallback_to_anonymous() -> None:
    """When actor_user_id is None, the attribute falls back to 'anonymous'."""
    meter = _FakeMeter()
    publisher = WebSocketMetricsOtelPublisher(meter=meter)

    publisher.record_connection(
        route="/sessions/updates",
        actor_user_id=None,
        duration_seconds=5.0,
        close_code=1006,
        outcome="disconnected",
    )

    attrs = meter.histograms["omnigent.server.websocket.connection.duration"].records[0].attributes
    assert attrs["omnigent.actor.user_id"] == "anonymous"


def test_record_connection_close_code_none_uses_sentinel() -> None:
    """When close_code is None, the attribute uses -1 as a sentinel."""
    meter = _FakeMeter()
    publisher = WebSocketMetricsOtelPublisher(meter=meter)

    publisher.record_connection(
        route="/runner/tunnel",
        actor_user_id="runner-owner",
        duration_seconds=1.0,
        close_code=None,
        outcome="error",
    )

    attrs = meter.histograms["omnigent.server.websocket.connection.duration"].records[0].attributes
    assert attrs["ws.close_code"] == -1
    assert attrs["ws.outcome"] == "error"


def test_record_message_sent_emits_expected_attributes() -> None:
    """record_message_sent increments the sent counter with correct attributes."""
    meter = _FakeMeter()
    publisher = WebSocketMetricsOtelPublisher(meter=meter)

    publisher.record_message_sent(
        route="/dictation/stream",
        actor_user_id="user@example.com",
    )

    records = meter.counters["omnigent.server.websocket.messages.sent"].records
    assert len(records) == 1
    assert records[0].amount == 1
    attrs = records[0].attributes
    assert attrs["ws.route"] == "/dictation/stream"
    assert attrs["omnigent.actor.user_id"] == "user@example.com"


def test_record_message_received_emits_expected_attributes() -> None:
    """record_message_received increments the received counter with correct attributes."""
    meter = _FakeMeter()
    publisher = WebSocketMetricsOtelPublisher(meter=meter)

    publisher.record_message_received(
        route="/sessions/updates",
        actor_user_id=None,
    )

    records = meter.counters["omnigent.server.websocket.messages.received"].records
    assert len(records) == 1
    assert records[0].amount == 1
    attrs = records[0].attributes
    assert attrs["ws.route"] == "/sessions/updates"
    assert attrs["omnigent.actor.user_id"] == "anonymous"


def test_publisher_multiple_messages_accumulate() -> None:
    """Multiple message calls accumulate in the counter records."""
    meter = _FakeMeter()
    publisher = WebSocketMetricsOtelPublisher(meter=meter)

    for _ in range(3):
        publisher.record_message_sent(route="/dictation/stream", actor_user_id="u")
    publisher.record_message_received(route="/dictation/stream", actor_user_id="u")

    assert len(meter.counters["omnigent.server.websocket.messages.sent"].records) == 3
    assert len(meter.counters["omnigent.server.websocket.messages.received"].records) == 1
