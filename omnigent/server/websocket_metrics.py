"""WebSocket connection metrics published through OpenTelemetry.

Records per-connection duration, message counts, and close reason —
all tagged with a bounded route template and the authenticated actor
user ID.  Attribute values are bounded by design (see
``designs/HTTP_WEBSOCKET_METRICS_EXTENSION.md``); no session IDs,
terminal IDs, or user-provided content ever enter metric attributes.
"""

from __future__ import annotations

import re
import time
from typing import Protocol

from opentelemetry import metrics as otel_metrics
from opentelemetry.util.types import Attributes

_OTEL_METER_NAME = "omnigent.server.websocket"
_WS_DURATION_NAME = "omnigent.server.websocket.connection.duration"
_WS_MESSAGES_SENT_NAME = "omnigent.server.websocket.messages.sent"
_WS_MESSAGES_RECEIVED_NAME = "omnigent.server.websocket.messages.received"

# Bounded route-template patterns for WebSocket endpoints.  Each entry
# is (regex_or_exact, template).  Exact strings are matched by equality;
# strings starting with ``^`` are treated as regex patterns.  The
# ``/v1`` API prefix is stripped before matching (see
# :func:`ws_route_template`).
_WS_ROUTE_PATTERNS: list[tuple[str, str]] = [
    ("/dictation/stream", "/dictation/stream"),
    ("/sessions/updates", "/sessions/updates"),
    (
        r"^/sessions/[^/]+/resources/terminals/[^/]+/attach$",
        "/sessions/{session_id}/resources/terminals/{terminal_id}/attach",
    ),
    ("/runner/tunnel", "/runner/tunnel"),
    (r"^/runners/[^/]+/tunnel$", "/runner/tunnel"),
    (r"^/hosts/[^/]+/tunnel$", "/hosts/{host_id}/tunnel"),
]


class _CounterInstrument(Protocol):
    """Subset of an OpenTelemetry counter used by this module."""

    def add(self, amount: int | float, attributes: Attributes = None) -> None: ...


class _HistogramInstrument(Protocol):
    """Subset of an OpenTelemetry histogram used by this module."""

    def record(self, amount: int | float, attributes: Attributes = None) -> None: ...


class _MeterLike(Protocol):
    """Subset of an OpenTelemetry meter used by this module."""

    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _CounterInstrument: ...

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _HistogramInstrument: ...


class _NoopCounter:
    """Counter used when OpenTelemetry instrumentation cannot initialize."""

    def add(self, amount: int | float, attributes: Attributes = None) -> None:
        """Discard a counter measurement."""
        del amount, attributes


class _NoopHistogram:
    """Histogram used when OpenTelemetry instrumentation cannot initialize."""

    def record(self, amount: int | float, attributes: Attributes = None) -> None:
        """Discard a histogram measurement."""
        del amount, attributes


def ws_route_template(scope: dict) -> str:
    """
    Resolve an ASGI WebSocket scope to a bounded route template.

    Tries the matched FastAPI/Starlette route object first, then falls
    back to pattern matching against known WebSocket endpoints.  Never
    leaks session IDs, terminal IDs, or host IDs into the returned
    template.

    :param scope: ASGI connection scope.
    :returns: Route template such as ``"/dictation/stream"``, or
        ``"unknown"`` when no known route matches.
    """
    path = scope.get("path", "")
    route_obj = scope.get("route")
    if route_obj is not None and hasattr(route_obj, "path"):
        return route_obj.path
    # Strip the /v1 API prefix so patterns match production paths.
    if path.startswith("/v1/"):
        path = path[3:]
    for pattern, template in _WS_ROUTE_PATTERNS:
        if pattern == path:
            return template
        if pattern.startswith("^") and re.match(pattern, path):
            return template
    return "unknown"


class WebSocketMetricsOtelPublisher:
    """
    Publish WebSocket connection metrics through OTEL instruments.

    All methods are safe to call even when OpenTelemetry is not
    initialized — the constructor falls back to no-op instruments on
    any exception.

    :param meter: Optional injected OpenTelemetry meter.
    :param clock: Monotonic clock used to measure connection duration.
    """

    def __init__(
        self,
        meter: _MeterLike | None = None,
        *,
        clock: callable = time.monotonic,
    ) -> None:
        """Initialize WebSocket metric instruments."""
        self._clock = clock
        try:
            effective_meter = meter or otel_metrics.get_meter(_OTEL_METER_NAME)
            self._duration = effective_meter.create_histogram(
                _WS_DURATION_NAME,
                unit="s",
                description="WebSocket connection duration in seconds.",
            )
            self._messages_sent = effective_meter.create_counter(
                _WS_MESSAGES_SENT_NAME,
                unit="{message}",
                description="WebSocket messages sent to the client.",
            )
            self._messages_received = effective_meter.create_counter(
                _WS_MESSAGES_RECEIVED_NAME,
                unit="{message}",
                description="WebSocket messages received from the client.",
            )
        except Exception:  # noqa: BLE001
            self._duration = _NoopHistogram()
            self._messages_sent = _NoopCounter()
            self._messages_received = _NoopCounter()

    def record_connection(
        self,
        *,
        route: str,
        actor_user_id: str | None,
        duration_seconds: float,
        close_code: int | None,
        outcome: str,
    ) -> None:
        """
        Record one completed WebSocket connection.

        :param route: Bounded route template from :func:`ws_route_template`.
        :param actor_user_id: Canonical authenticated user ID, or ``None``.
        :param duration_seconds: Connection duration in seconds.
        :param close_code: WebSocket close code, or ``None``.
        :param outcome: ``"closed"``, ``"error"``, or ``"disconnected"``.
        """
        attributes: dict[str, str | int] = {
            "ws.route": route,
            "omnigent.actor.user_id": actor_user_id or "anonymous",
            "ws.close_code": close_code if close_code is not None else -1,
            "ws.outcome": outcome,
        }
        self._duration.record(duration_seconds, attributes=attributes)

    def record_message_sent(
        self,
        *,
        route: str,
        actor_user_id: str | None,
    ) -> None:
        """
        Record one outbound WebSocket message.

        :param route: Bounded route template.
        :param actor_user_id: Canonical authenticated user ID, or ``None``.
        """
        self._messages_sent.add(
            1,
            attributes={
                "ws.route": route,
                "omnigent.actor.user_id": actor_user_id or "anonymous",
            },
        )

    def record_message_received(
        self,
        *,
        route: str,
        actor_user_id: str | None,
    ) -> None:
        """
        Record one inbound WebSocket message.

        :param route: Bounded route template.
        :param actor_user_id: Canonical authenticated user ID, or ``None``.
        """
        self._messages_received.add(
            1,
            attributes={
                "ws.route": route,
                "omnigent.actor.user_id": actor_user_id or "anonymous",
            },
        )
