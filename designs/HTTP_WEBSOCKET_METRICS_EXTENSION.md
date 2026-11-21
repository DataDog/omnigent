# Extension Plan: HTTP Route and WebSocket Metrics with User Attribution

## Context

The initial feature-usage telemetry (PR #4, merged) instruments nine
semantic feature boundaries with `omnigent.feature.usage` and
`omnigent.feature.operation.duration`, both carrying
`omnigent.actor.user_id`.  That gives us *what* the user did.

What we lack is *where* and *how often* at the transport layer:

- **HTTP routes**: The existing `ServerMetricsOtelPublisher` already
  records `omnigent.server.http.request.duration` with `http.route`,
  `http.request.method`, `http.response.status_code`, and `failed` —
  but **no user attribution**.  We confirmed via web search that
  OTEL's FastAPI auto-instrumentation cannot inject per-request
  dynamic attributes into its built-in metrics, so retrofitting
  `user_id` onto auto-instrumented HTTP metrics is not an option.
  However, the server already has a **custom** HTTP middleware
  (`_record_server_metrics` in `app.py`) that calls
  `server_metrics_otel.record_request_duration()`.  This is our seam.

- **WebSocket calls**: The existing `_WebSocketMetricsMiddleware`
  tracks only connection count (`websocket_connected` /
  `websocket_disconnected`) — no route, no user, no message count, no
  duration.  WebSocket traffic comprises a large fraction of the
  application's real-time surface (dictation, session updates,
  terminal attach, runner tunnels, host tunnels).

## Goals

1. Add `omnigent.actor.user_id` to the existing HTTP request duration
   metric so every HTTP data point is attributable to a user (or
   `anonymous` / `local`).
2. Add a new WebSocket metrics layer that records per-connection
   duration, message count, and close reason — all tagged with route
   template and user ID.
3. Maintain the existing failure-isolation guarantee: telemetry never
   breaks user-facing functionality.

### Cardinality is not a concern

All attributes introduced by this plan are bounded by design.  The
user ID tag (`omnigent.actor.user_id`) is the only potentially
high-cardinality dimension, and it is capped by the number of
authenticated users on the deployment — a small, knowable population
(internal staging has ~50 active users).  Every other attribute is a
fixed enumeration: HTTP methods (~7), route templates (~40), status
codes (~15), WebSocket routes (6), close codes (~10), outcomes (3).

The product of these bounds is well within Datadog's custom metric
budget for an internal deployment.  No env-gates, cardinality
limits, or opt-in flags are needed.  The plan does not introduce any
mechanism to suppress or gate the user tag.

## Non-goals

- Retrofitting user_id onto OTEL auto-instrumented HTTP metrics (not
  possible today; confirmed via web search).
- Per-message WebSocket latency histograms (cardinality and volume
  concerns; defer to a future profiling need).
- Replacing the existing `ServerPerformanceMetrics` in-process
  counters — those remain for the benchmark harness and offline
  analysis.  We extend the OTEL publisher, not the in-process tracker.
- Dashboards, monitors, and alerting (out of scope for this extension).

---

## Part 1: HTTP Route Metrics with User Attribution

### Current state

```python
# app.py — existing middleware (simplified)
@app.middleware("http")
async def _record_server_metrics(request, call_next):
    ...
    server_metrics_otel.record_request_duration(
        duration_seconds=...,
        failed=...,
        method=request.method,
        route=request_route_template_for_metrics(request),
        status_code=...,
    )
```

```python
# performance_metrics.py — existing OTEL publisher
def record_request_duration(self, *, duration_seconds, failed, method, route, status_code):
    attributes = {
        "failed": failed,
        "http.request.method": method,
        "http.route": route,
    }
    if status_code is not None:
        attributes["http.response.status_code"] = status_code
    self._request.duration.record(duration_seconds, attributes=attributes)
```

### What changes

**One file: `omnigent/server/performance_metrics.py`**

Add `actor_user_id` as a parameter to `record_request_duration()` and
include it in the attribute dict:

```python
def record_request_duration(
    self,
    *,
    duration_seconds: float,
    failed: bool,
    method: str,
    route: str,
    status_code: int | None,
    actor_user_id: str | None = None,   # ← new
) -> None:
    attributes: dict[str, str | bool | int] = {
        "failed": failed,
        "http.request.method": method,
        "http.route": route,
    }
    if status_code is not None:
        attributes["http.response.status_code"] = status_code
    attributes["omnigent.actor.user_id"] = actor_user_id or "anonymous"
    self._request.duration.record(duration_seconds, attributes=attributes)
```

**One file: `omnigent/server/app.py`**

In `_record_server_metrics`, resolve the user ID from the auth provider
and pass it through:

```python
# After the existing request_id / session_id extraction:
actor_user_id = None
if auth_provider is not None:
    try:
        actor_user_id = auth_provider.get_user_id(request)
    except Exception:
        actor_user_id = None

# In the finally block, add:
server_metrics_otel.record_request_duration(
    duration_seconds=duration_seconds,
    failed=request_failed,
    method=request.method,
    route=route,
    status_code=metrics_status_code,
    actor_user_id=actor_user_id,   # ← new
)
```

### Health-check exclusion

`/health` requests have no authenticated user and would all get
`anonymous`.  To avoid noise, the middleware should skip adding the
user tag for health-check routes:

```python
if route != "/health":
    attributes["omnigent.actor.user_id"] = actor_user_id or "anonymous"
```

This keeps health-check series clean and avoids polluting
user-attributed queries.

### Tests

- Unit test `record_request_duration` with `actor_user_id` set, unset,
  and `None` — verify the attribute appears / falls back to
  `anonymous` / is omitted for health routes.
- Integration test: mock `AuthProvider.get_user_id` returning a known
  value, send a test request, assert the OTEL test meter received the
  attribute.
- Integration test: `/health` request does not carry the user tag.

### Datadog query examples after this change

```text
# Request duration by user
percentile:omnigent.server.http.request.duration{env:staging,service:omni-server}
  by {omnigent.actor.user_id}

# Error rate by user and route
sum:omnigent.server.http.request.duration{env:staging,service:omni-server,failed:true}
  by {omnigent.actor.user_id,http.route}
```

---

## Part 2: WebSocket Metrics with User Attribution

### Current state

```python
# app.py — existing WS middleware (simplified)
class _WebSocketMetricsMiddleware:
    async def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            await self._app(scope, receive, send)
            return
        # Counts accept/disconnect only — no route, no user, no messages
        ...
        self._metrics.websocket_connected()
        ...
        self._metrics.websocket_disconnected()
```

No OTEL instruments exist for WebSocket metrics.  The in-process
counter feeds `ServerPerformanceMetrics` (used by the benchmark
harness) but is never exported to Datadog.

### WebSocket routes in the codebase

| Route template | File | Purpose |
|---|---|---|
| `/dictation/stream` | `routes/dictation.py` | Streaming dictation / transcription |
| `/sessions/updates` | `routes/sessions/routes_core.py` | Real-time session event stream |
| `/sessions/{session_id}/resources/terminals/{terminal_id}/attach` | `routes/terminal_attach.py` | Terminal attach (PTY bridge) |
| `/runner/tunnel` | `routes/runner_tunnel.py` | Runner-to-server tunnel |
| `/hosts/{host_id}/tunnel` | `routes/host_tunnel.py` | Host-to-server tunnel |

All five already call `auth_provider.get_user_id(websocket)` inside
the route handler.  The ASGI scope carries the matched route in
`scope["route"]` (or we can derive it from `scope["path"]` with a
template resolver).

### What changes

#### New file: `omnigent/server/websocket_metrics.py`

A dedicated OTEL publisher for WebSocket connection metrics, mirroring
the `ServerMetricsOtelPublisher` pattern:

```python
"""WebSocket connection metrics published through OpenTelemetry."""

from __future__ import annotations

import time
from opentelemetry import metrics as otel_metrics
from opentelemetry.util.types import Attributes

_OTEL_METER_NAME = "omnigent.server.websocket"
_WS_DURATION_NAME = "omnigent.server.websocket.connection.duration"
_WS_MESSAGES_NAME = "omnigent.server.websocket.messages"

# Bounded set of WebSocket route templates — not user-provided.
_WS_ROUTES = frozenset({
    "/dictation/stream",
    "/sessions/updates",
    "/sessions/{session_id}/resources/terminals/{terminal_id}/attach",
    "/runner/tunnel",
    "/hosts/{host_id}/tunnel",
})


class WebSocketMetricsOtelPublisher:
    """Publish WebSocket connection metrics through OTEL instruments."""

    def __init__(self, meter=None, *, clock=time.monotonic):
        effective_meter = meter or otel_metrics.get_meter(_OTEL_METER_NAME)
        try:
            self._duration = effective_meter.create_histogram(
                _WS_DURATION_NAME,
                unit="s",
                description="WebSocket connection duration in seconds.",
            )
            self._messages_sent = effective_meter.create_counter(
                f"{_WS_MESSAGES_NAME}.sent",
                unit="{message}",
                description="WebSocket messages sent to the client.",
            )
            self._messages_received = effective_meter.create_counter(
                f"{_WS_MESSAGES_NAME}.received",
                unit="{message}",
                description="WebSocket messages received from the client.",
            )
        except Exception:
            self._duration = _NoopHistogram()
            self._messages_sent = _NoopCounter()
            self._messages_received = _NoopCounter()
        self._clock = clock

    def record_connection(
        self,
        *,
        route: str,
        actor_user_id: str | None,
        duration_seconds: float,
        close_code: int | None,
        outcome: str,  # "closed", "error", "disconnected"
    ) -> None:
        attributes = {
            "ws.route": route,
            "omnigent.actor.user_id": actor_user_id or "anonymous",
            "ws.close_code": close_code if close_code is not None else -1,
            "ws.outcome": outcome,
        }
        self._duration.record(duration_seconds, attributes=attributes)

    def record_message_sent(self, *, route: str, actor_user_id: str | None) -> None:
        self._messages_sent.add(1, attributes={
            "ws.route": route,
            "omnigent.actor.user_id": actor_user_id or "anonymous",
        })

    def record_message_received(self, *, route: str, actor_user_id: str | None) -> None:
        self._messages_received.add(1, attributes={
            "ws.route": route,
            "omnigent.actor.user_id": actor_user_id or "anonymous",
        })
```

#### Modify: `omnigent/server/app.py` — extend `_WebSocketMetricsMiddleware`

The existing ASGI middleware already intercepts all WebSocket
connections.  We extend it to:

1. Resolve the route template from the ASGI scope.
2. Resolve the user ID from the auth provider.
3. Track connection duration (accept → disconnect/close).
4. Wrap `send` to count outbound messages.
5. Wrap `receive` to count inbound messages.
6. Record the terminal close code and outcome.

```python
class _WebSocketMetricsMiddleware:
    def __init__(self, app, metrics, *, ws_otel: WebSocketMetricsOtelPublisher | None = None,
                 auth_provider=None):
        self._app = app
        self._metrics = metrics
        self._ws_otel = ws_otel
        self._auth_provider = auth_provider

    async def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            await self._app(scope, receive, send)
            return

        route = _ws_route_template(scope)
        actor_user_id = None
        if self._auth_provider is not None:
            try:
                # WebSocket is an HTTPConnection — get_user_id works.
                from starlette.requests import HTTPConnection
                actor_user_id = self._auth_provider.get_user_id(
                    HTTPConnection(scope)
                )
            except Exception:
                pass

        started_at = time.monotonic()
        accepted = False
        close_code: int | None = None
        msg_count_sent = 0
        msg_count_received = 0

        async def send_with_metrics(message):
            nonlocal accepted, close_code, msg_count_sent
            if not accepted and message["type"] == "websocket.accept":
                self._metrics.websocket_connected()
                accepted = True
            if message["type"] == "websocket.close":
                close_code = message.get("code", 1000)
            if message["type"] == "websocket.send":
                msg_count_sent += 1
                if self._ws_otel:
                    self._ws_otel.record_message_sent(
                        route=route, actor_user_id=actor_user_id
                    )
            await send(message)

        async def receive_with_metrics():
            nonlocal msg_count_received
            message = await receive()
            if message["type"] == "websocket.receive":
                msg_count_received += 1
                if self._ws_otel:
                    self._ws_otel.record_message_received(
                        route=route, actor_user_id=actor_user_id
                    )
            return message

        try:
            await self._app(scope, receive_with_metrics, send_with_metrics)
        except Exception:
            outcome = "error"
            raise
        else:
            outcome = "disconnected" if close_code in (1001, 1006) else "closed"
        finally:
            if accepted:
                self._metrics.websocket_disconnected()
                if self._ws_otel:
                    duration = time.monotonic() - started_at
                    self._ws_otel.record_connection(
                        route=route,
                        actor_user_id=actor_user_id,
                        duration_seconds=duration,
                        close_code=close_code,
                        outcome=outcome,
                    )
```

#### Route template resolution

ASGI scope does not always carry the matched route template the way
FastAPI's `request.scope["route"]` does for HTTP.  We resolve it with
a bounded matcher:

```python
_WS_ROUTE_PATTERNS = [
    ("/dictation/stream", "/dictation/stream"),
    ("/sessions/updates", "/sessions/updates"),
    # Terminal attach: /sessions/{id}/resources/terminals/{id}/attach
    (r"^/sessions/[^/]+/resources/terminals/[^/]+/attach$",
     "/sessions/{session_id}/resources/terminals/{terminal_id}/attach"),
    ("/runner/tunnel", "/runner/tunnel"),
    (r"^/hosts/[^/]+/tunnel$", "/hosts/{host_id}/tunnel"),
]

def _ws_route_template(scope: dict) -> str:
    path = scope.get("path", "")
    # Try FastAPI's matched route first (available in scope after routing).
    route_obj = scope.get("route")
    if route_obj is not None and hasattr(route_obj, "path"):
        return route_obj.path
    # Fall back to pattern matching.
    for pattern, template in _WS_ROUTE_PATTERNS:
        if pattern == path or (pattern.startswith("^") and re.match(pattern, path)):
            return template
    return "unknown"
```

This keeps the route attribute bounded to 6 values (5 known routes +
`unknown`), never leaking session IDs or terminal IDs into metric
attributes.

### Tunnel inclusion

Runner tunnels (`/runner/tunnel`) and host tunnels
(`/hosts/{host_id}/tunnel`) are long-lived infrastructure connections,
not user-initiated actions.  Their user ID is the runner/host owner,
but the connections persist for hours, producing a single duration
sample per connection.

**Decision**: Include them.  The `ws.route` tag lets you filter them
out in queries (`ws.route:/dictation/stream` vs
`ws.route:/runner/tunnel`).  Excluding them from the middleware would
require route-specific logic in a general-purpose middleware, which
violates the simplicity principle.

### Tests

- Unit test `WebSocketMetricsOtelPublisher` with a recording meter:
  verify `record_connection`, `record_message_sent`, and
  `record_message_received` emit the expected attributes.
- Unit test `_ws_route_template` for all 5 known routes + an unknown
  path → `unknown`.
- Integration test: open a WebSocket to `/dictation/stream`, send and
  receive a message, close — verify the test meter received a
  connection duration point, a sent message count, and a received
  message count, all tagged with the correct route and user ID.
- Integration test: WebSocket that errors mid-connection → outcome
  is `error`.
- Integration test: unauthenticated WebSocket → user tag is
  `anonymous`.

### Datadog query examples after this change

```text
# WS connection duration by route
percentile:omnigent.server.websocket.connection.duration{env:staging,service:omni-server}
  by {ws.route}

# WS message volume by user and route
sum:omnigent.server.websocket.messages.sent{env:staging,service:omni-server}
  by {omnigent.actor.user_id,ws.route}

# WS error rate by route
sum:omnigent.server.websocket.connection.duration{env:staging,service:omni-server,ws.outcome:error}
  by {ws.route}
```

---

## Implementation sequence

### Step 1: HTTP user attribution (small, low-risk)

1. Add `actor_user_id` parameter to `ServerMetricsOtelPublisher.record_request_duration()`.
2. Resolve `auth_provider.get_user_id(request)` in `_record_server_metrics` middleware.
3. Skip user tag for `/health` route.
4. Add tests.
5. Update `designs/OBSERVABILITY.md` with the new attribute.

**Estimated diff**: ~30 lines in `performance_metrics.py`, ~15 lines in
`app.py`, ~80 lines of tests.

#### Verification: Step 1

| Check | Command | Pass criteria |
|---|---|---|
| Unit tests (new + existing) | `pytest tests/server/test_performance_metrics.py tests/server/test_app.py -x -q` | All pass, no regressions |
| Ruff lint | `ruff check omnigent/server/performance_metrics.py omnigent/server/app.py` | No errors |
| Ruff format | `ruff format --check omnigent/server/performance_metrics.py omnigent/server/app.py` | No diff |
| Type check | `mypy omnigent/server/performance_metrics.py omnigent/server/app.py` | No new errors |
| Attribute assertion | Unit test: `record_request_duration(actor_user_id="user@example.com")` emits `omnigent.actor.user_id` in attributes | Attribute present and correct |
| Fallback assertion | Unit test: `record_request_duration(actor_user_id=None)` emits `anonymous` | Fallback value is `anonymous` |
| Health exclusion | Unit test: `record_request_duration(route="/health", actor_user_id=None)` does NOT emit the user tag | Attribute absent |
| Auth failure isolation | Unit test: `auth_provider.get_user_id` raises → `actor_user_id` is `None`, request still succeeds | No exception propagated |
| Existing HTTP metrics | Integration test: send GET `/v1/sessions`, verify OTEL test meter received duration point with user tag | Metric recorded with `omnigent.actor.user_id` |

### Step 2: WebSocket OTEL publisher (new module, medium)

1. Create `omnigent/server/websocket_metrics.py` with
   `WebSocketMetricsOtelPublisher`.
2. Extend `_WebSocketMetricsMiddleware` to wrap send/receive and
   record connection lifecycle.
3. Wire `WebSocketMetricsOtelPublisher` and `auth_provider` into the
   middleware at app construction time.
4. Add `_ws_route_template` resolver.
5. Add tests.
6. Update `designs/OBSERVABILITY.md` and
   `designs/FEATURE_USAGE_METRICS.md`.

**Estimated diff**: ~150 lines new file, ~80 lines modified in
`app.py`, ~200 lines of tests.

#### Verification: Step 2

| Check | Command | Pass criteria |
|---|---|---|
| Unit tests (publisher) | `pytest tests/server/test_websocket_metrics.py -x -q` | All pass |
| Unit tests (route resolver) | `pytest tests/server/test_websocket_metrics.py::test_ws_route_template -x -q` | All 5 known routes resolve correctly; unknown path → `unknown` |
| Unit tests (middleware) | `pytest tests/server/test_app.py -k websocket -x -q` | All pass, no regressions |
| Ruff lint | `ruff check omnigent/server/websocket_metrics.py omnigent/server/app.py` | No errors |
| Ruff format | `ruff format --check omnigent/server/websocket_metrics.py omnigent/server/app.py` | No diff |
| Type check | `mypy omnigent/server/websocket_metrics.py omnigent/server/app.py` | No new errors |
| Connection lifecycle | Integration test: open WS to `/dictation/stream`, send + receive message, close → test meter received duration point, sent counter, received counter | All three metrics recorded with correct route + user tags |
| Error path | Integration test: WS that raises mid-connection → outcome is `error`, duration still recorded | `ws.outcome` is `error` |
| Unauthenticated WS | Integration test: WS with no auth provider → user tag is `anonymous` | `omnigent.actor.user_id` is `anonymous` |
| Close code capture | Integration test: WS closed with code 1000 → `ws.close_code` is 1000 | Close code recorded correctly |
| No leaked IDs | Unit test: route resolver on `/sessions/abc123/resources/terminals/t1/attach` → template is `/sessions/{session_id}/resources/terminals/{terminal_id}/attach` | No session or terminal ID in attribute |
| Existing WS counter | Integration test: existing `websocket_connected` / `websocket_disconnected` still fires | In-process counter unaffected |
| Full suite | `pytest tests/server/ -x -q` | No regressions |

### Step 3: Feature-usage ↔ HTTP correlation (optional, future)

If we later want to join feature-usage metrics with HTTP metrics for
the same request, add a shared `omnigent.request.correlation_id` to
both the HTTP middleware and the feature-usage recorder.  This is
deferred — the `omnigent.actor.user_id` tag already provides
user-level correlation, and route + timestamp is usually sufficient
for operational analysis.

#### Verification: Step 3 (when implemented)

| Check | Command | Pass criteria |
|---|---|---|
| Correlation ID present | Integration test: HTTP request that triggers a feature-usage point → both metrics carry the same `omnigent.request.correlation_id` | IDs match |
| Ruff + mypy | Same as Steps 1–2 | Clean |
| Full suite | `pytest tests/server/ -x -q` | No regressions |

---

## Attribute reference (after both steps)

### HTTP request duration metric

| Attribute | Source | Bound |
|---|---|---|
| `http.request.method` | `request.method` | Fixed enumeration (~7) |
| `http.route` | `request_route_template_for_metrics(request)` | Fixed route templates (~40) |
| `http.response.status_code` | `response.status_code` | Fixed enumeration (~15) |
| `failed` | exception or 5xx | Boolean (2) |
| `omnigent.actor.user_id` | `AuthProvider.get_user_id()` | Bounded by authenticated user population (~50) |

### WebSocket connection duration metric

| Attribute | Source | Bound |
|---|---|---|
| `ws.route` | `_ws_route_template(scope)` | Fixed enumeration (6) |
| `omnigent.actor.user_id` | `AuthProvider.get_user_id()` | Bounded by authenticated user population (~50) |
| `ws.close_code` | ASGI close message | Fixed enumeration (~10) |
| `ws.outcome` | `closed` / `error` / `disconnected` | Fixed enumeration (3) |

### WebSocket message counters

| Attribute | Source | Bound |
|---|---|---|
| `ws.route` | `_ws_route_template(scope)` | Fixed enumeration (6) |
| `omnigent.actor.user_id` | `AuthProvider.get_user_id()` | Bounded by authenticated user population (~50) |

---

## Metric names summary

| Metric name | Type | Unit | Source |
|---|---|---|---|
| `omnigent.feature.usage` | counter | `{operation}` | FeatureUsageRecorder (existing) |
| `omnigent.feature.operation.duration` | histogram | `s` | FeatureUsageRecorder (existing) |
| `omnigent.server.http.request.duration` | histogram | `s` | ServerMetricsOtelPublisher (existing, +user tag) |
| `omnigent.server.websocket.connection.duration` | histogram | `s` | WebSocketMetricsOtelPublisher (new) |
| `omnigent.server.websocket.messages.sent` | counter | `{message}` | WebSocketMetricsOtelPublisher (new) |
| `omnigent.server.websocket.messages.received` | counter | `{message}` | WebSocketMetricsOtelPublisher (new) |

---

## Risk and mitigation

| Risk | Mitigation |
|---|---|
| WS middleware wrapping receive/send adds latency | Both wrappers are O(1) dict checks + counter add; negligible vs. network I/O |
| Auth provider throws in WS scope | Wrapped in try/except; falls back to `anonymous` |
| Route template resolver misses a new WS route | Falls back to `unknown`; bounded set is updated when routes are added |
| OTEL SDK not initialized (telemetry disabled) | All instruments are no-op by default; publisher constructor catches exceptions |

---

## Open questions

1. **Should runner/host tunnel connections be excluded from WS
   metrics?** They're infrastructure, not user actions.  Currently
   included with a `ws.route` tag for filtering.  Alternative: skip
   recording for routes matching `/runner/tunnel` or
   `/hosts/{host_id}/tunnel`.

2. **Should WS message counters also tag message type?** E.g.
   `text` vs `binary`.  Low cardinality (2 values) and potentially
   useful for debugging dictation vs. terminal streams.  Currently
   omitted for simplicity.
