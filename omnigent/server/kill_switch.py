"""Service-wide kill switch driven by an operator feature flag.

When engaged, the server refuses new sessions and turns (HTTP 503
``service_disabled``) and interrupts in-flight turns. Reads, health and
interrupts stay available. See ``designs/KILL_SWITCH.md``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal, Protocol

from opentelemetry import metrics as otel_metrics

from omnigent.errors import ErrorCode, OmnigentError

_logger = logging.getLogger(__name__)

_METER_NAME = "omnigent.server.kill_switch"
DEFAULT_MESSAGE = "Omnigent is temporarily disabled by an operator. Please try again later."
DEFAULT_POLL_INTERVAL_S = 5.0
DEFAULT_INIT_TIMEOUT_S = 10.0

SourceStatus = Literal["disabled", "ready", "unavailable", "forced"]
HaltOutcome = Literal["delivered", "not_delivered", "failed"]


@dataclass(frozen=True)
class FlagReading:
    """One evaluation: ``engaged`` is ``None`` when the source could not answer."""

    engaged: bool | None
    error: str | None = None


class KillSwitchSource(Protocol):
    """A flag backend evaluated synchronously (called off the event loop)."""

    def evaluate(self, last_known: bool) -> FlagReading:
        """Return the current flag value, defaulting to *last_known*."""
        ...

    def close(self) -> None:
        """Release backend resources."""
        ...


@dataclass(frozen=True)
class KillSwitchState:
    """Immutable snapshot served to request handlers and ``GET /v1/kill-switch``."""

    engaged: bool = False
    since: float | None = None
    source_status: SourceStatus = "disabled"
    last_success_at: float | None = None
    last_error: str | None = None
    message: str = DEFAULT_MESSAGE

    def to_dict(self) -> dict[str, Any]:
        """Public, secret-free representation."""
        return {
            "engaged": self.engaged,
            "since": self.since,
            "source_status": self.source_status,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "message": self.message if self.engaged else None,
        }


class _Metrics:
    """OpenTelemetry instruments; any creation failure degrades to no-ops."""

    def __init__(self, controller: KillSwitchController, meter: Any | None) -> None:
        self._refresh: Any = None
        self._rejected: Any = None
        self._halted: Any = None
        try:
            meter = meter or otel_metrics.get_meter(_METER_NAME)
            self._refresh = meter.create_counter("omnigent.kill_switch.flag_refresh")
            self._rejected = meter.create_counter("omnigent.kill_switch.rejected_requests")
            self._halted = meter.create_counter("omnigent.kill_switch.halted_turns")
            meter.create_observable_gauge(
                "omnigent.kill_switch.engaged",
                callbacks=[lambda _options: [otel_metrics.Observation(int(controller.engaged))]],
            )
            meter.create_observable_gauge(
                "omnigent.kill_switch.staleness",
                unit="s",
                callbacks=[lambda _options: controller.staleness_observations()],
            )
        except Exception:  # noqa: BLE001 - metrics must never break the server
            _logger.warning("kill switch metrics unavailable", exc_info=True)

    def _add(self, instrument: Any, attributes: dict[str, str]) -> None:
        if instrument is None:
            return
        try:
            instrument.add(1, attributes)
        except Exception:  # noqa: BLE001 - metrics must never break the server
            _logger.debug("kill switch metric add failed", exc_info=True)

    def refresh(self, outcome: str) -> None:
        self._add(self._refresh, {"outcome": outcome})

    def rejected(self, operation: str) -> None:
        self._add(self._rejected, {"operation": operation})

    def halted(self, outcome: HaltOutcome) -> None:
        self._add(self._halted, {"outcome": outcome})


TurnInterrupter = Callable[[str], Awaitable[bool]]
SweepTargets = Callable[[], list[tuple[int, str]]]


class KillSwitchController:
    """Polls a :class:`KillSwitchSource` and enforces the engaged state."""

    def __init__(
        self,
        source: KillSwitchSource | None,
        *,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        init_timeout_s: float = DEFAULT_INIT_TIMEOUT_S,
        message: str = DEFAULT_MESSAGE,
        force: bool | None = None,
        meter: Any | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._source = source
        self._poll_interval_s = poll_interval_s
        self._init_timeout_s = init_timeout_s
        self._clock = clock
        status: SourceStatus = "disabled" if source is None else "unavailable"
        if force is not None:
            status = "forced"
        self._state = KillSwitchState(
            engaged=bool(force),
            since=clock() if force else None,
            source_status=status,
            message=message,
        )
        self._force = force
        self._metrics = _Metrics(self, meter)
        self._task: asyncio.Task[None] | None = None
        self.interrupter: TurnInterrupter | None = None
        self.sweep_targets: SweepTargets | None = None

    @property
    def state(self) -> KillSwitchState:
        return self._state

    @property
    def engaged(self) -> bool:
        return self._state.engaged

    @property
    def active(self) -> bool:
        """Whether a flag source or force override is configured."""
        return self._source is not None or self._force is not None

    def staleness_observations(self) -> list[Any]:
        if self._source is None or self._force is not None:
            return []
        last = self._state.last_success_at
        if last is None:
            return []
        return [otel_metrics.Observation(max(0.0, self._clock() - last))]

    def require_enabled(self, operation: str) -> None:
        """Raise ``service_disabled`` (503) while engaged."""
        if not self._state.engaged:
            return
        self._metrics.rejected(operation)
        raise OmnigentError(self._state.message, code=ErrorCode.SERVICE_DISABLED)

    async def refresh(self) -> KillSwitchState:
        """Evaluate the source once and apply the result (fail-safe)."""
        if self._source is None or self._force is not None:
            return self._state
        previous = self._state
        try:
            reading = await asyncio.to_thread(self._source.evaluate, previous.engaged)
        except Exception as exc:  # noqa: BLE001 - fail-safe: keep last known state
            reading = FlagReading(engaged=None, error=f"{type(exc).__name__}: {exc}")
        now = self._clock()
        if reading.engaged is None or reading.error is not None:
            self._metrics.refresh("unavailable")
            if previous.last_error != reading.error:
                _logger.warning(
                    "kill_switch_flag_unavailable keeping engaged=%s: %s",
                    previous.engaged,
                    reading.error,
                )
            self._state = replace(previous, source_status="unavailable", last_error=reading.error)
        else:
            self._metrics.refresh("ok")
            changed = reading.engaged != previous.engaged
            self._state = replace(
                previous,
                engaged=reading.engaged,
                since=now if changed else previous.since,
                source_status="ready",
                last_success_at=now,
                last_error=None,
            )
            if changed and reading.engaged:
                _logger.warning("kill_switch_engaged: refusing new work, halting turns")
            elif changed:
                _logger.info("kill_switch_disengaged: accepting new work")
        if self._state.engaged:
            await self.sweep()
        return self._state

    async def sweep(self) -> None:
        """Interrupt every running turn this replica owns; isolate failures."""
        if self.interrupter is None or self.sweep_targets is None:
            return
        from omnigent.db.db_models import workspace_scope

        for workspace_id, session_id in self.sweep_targets():
            outcome: HaltOutcome
            try:
                with workspace_scope(workspace_id):
                    delivered = await self.interrupter(session_id)
                outcome = "delivered" if delivered else "not_delivered"
            except Exception:
                _logger.exception("kill switch failed to halt session %r", session_id)
                outcome = "failed"
            self._metrics.halted(outcome)

    async def start(self) -> None:
        """Bounded first evaluation, then background polling."""
        if self._source is None or self._force is not None:
            if self._force:
                _logger.warning("kill_switch_engaged: forced by OMNIGENT_KILL_SWITCH_FORCE")
            return
        try:
            await asyncio.wait_for(self.refresh(), timeout=self._init_timeout_s)
        except TimeoutError:
            _logger.warning(
                "kill switch flag not evaluated within %.1fs; starting disengaged",
                self._init_timeout_s,
            )
        self._task = asyncio.create_task(self._poll_loop(), name="kill-switch-poll")

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval_s)
            try:
                await self.refresh()
            except Exception:
                _logger.exception("kill switch refresh failed")

    async def stop(self) -> None:
        """Stop polling and close the source."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._source is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self._source.close)


def require_service_enabled(request: Any, operation: str) -> None:
    """Route guard: raise 503 when the app's kill switch is engaged."""
    state = getattr(getattr(request, "app", None), "state", None)
    controller = getattr(state, "kill_switch", None)
    if isinstance(controller, KillSwitchController):
        controller.require_enabled(operation)


def kill_switch_engaged(app_state: Any) -> KillSwitchState | None:
    """Return the engaged state for *app_state*, or ``None`` when disengaged."""
    controller = getattr(app_state, "kill_switch", None)
    if isinstance(controller, KillSwitchController) and controller.engaged:
        return controller.state
    return None


def _positive_float(environ: Mapping[str, str], name: str, default: float) -> float:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = -1.0
    if value <= 0:
        raise ValueError(f"{name} must be a positive number of seconds, got {raw!r}")
    return value


def resolve_kill_switch(environ: Mapping[str, str] | None = None) -> KillSwitchController:
    """Build the controller from ``OMNIGENT_KILL_SWITCH_*`` env; fail on bad values."""
    env = os.environ if environ is None else environ
    force_raw = env.get("OMNIGENT_KILL_SWITCH_FORCE", "").strip().lower()
    if force_raw not in ("", "engaged", "disengaged"):
        raise ValueError(
            f"OMNIGENT_KILL_SWITCH_FORCE must be 'engaged' or 'disengaged', got {force_raw!r}"
        )
    force = None if not force_raw else force_raw == "engaged"
    poll = _positive_float(env, "OMNIGENT_KILL_SWITCH_POLL_INTERVAL_S", DEFAULT_POLL_INTERVAL_S)
    init = _positive_float(env, "OMNIGENT_KILL_SWITCH_INIT_TIMEOUT_S", DEFAULT_INIT_TIMEOUT_S)
    message = env.get("OMNIGENT_KILL_SWITCH_MESSAGE", "").strip() or DEFAULT_MESSAGE
    flag_key = env.get("OMNIGENT_KILL_SWITCH_FLAG_KEY", "").strip()
    source: KillSwitchSource | None = None
    if flag_key and force is None:
        from omnigent.server.kill_switch_datadog import DatadogFlagSource

        source = DatadogFlagSource(flag_key, environ=env)
    return KillSwitchController(
        source, poll_interval_s=poll, init_timeout_s=init, message=message, force=force
    )
