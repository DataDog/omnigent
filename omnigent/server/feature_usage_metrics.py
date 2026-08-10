"""Feature-usage metrics published through the server OpenTelemetry provider.

This module deliberately records only low-cardinality feature metadata plus the
authenticated actor and, where relevant, session owner.  Callers must not pass
session IDs or user-provided content as metric attributes.
"""

from __future__ import annotations

import asyncio
import time
from threading import Lock
from typing import Callable, Literal, Mapping, Protocol

from opentelemetry import metrics as otel_metrics
from opentelemetry.util.types import Attributes

from omnigent.errors import OmnigentError

_OTEL_METER_NAME = "omnigent.server.feature_usage"
_USAGE_COUNTER_NAME = "omnigent.feature.usage"
_DURATION_HISTOGRAM_NAME = "omnigent.feature.operation.duration"

UsageOutcome = Literal[
    "success",
    "failed",
    "cancelled",
    "abandoned",
    "rejected",
    "no_change",
]
FailureReason = Literal["validation", "permission", "capacity", "backend", "unknown"]
FEATURE_ATTRIBUTE_KEYS = frozenset(
    {
        "omnigent.dictation.engine",
        "omnigent.attachment.category",
        "omnigent.attachment.size_bucket",
        "omnigent.sharing.target_type",
        "omnigent.sharing.access_level",
        "omnigent.fork.history_scope",
        "omnigent.policy.scope",
        "omnigent.policy.type",
        "omnigent.approval.decision",
    }
)


def classify_feature_usage_exception(
    error: BaseException,
) -> tuple[UsageOutcome, FailureReason]:
    """Map route exceptions to bounded usage outcomes without changing them."""
    status_code = (
        error.http_status
        if isinstance(error, OmnigentError)
        else getattr(error, "status_code", None)
    )
    if status_code in (401, 403):
        return "rejected", "permission"
    if isinstance(status_code, int) and 400 <= status_code < 500:
        return "rejected", "validation"
    return "failed", "backend"


class CounterInstrument(Protocol):
    """Subset of an OpenTelemetry counter used by this module."""

    def add(self, amount: int | float, attributes: Attributes = None) -> None:
        """Add a non-negative value to the counter."""
        ...


class HistogramInstrument(Protocol):
    """Subset of an OpenTelemetry histogram used by this module."""

    def record(self, amount: int | float, attributes: Attributes = None) -> None:
        """Record one histogram sample."""
        ...


class MeterLike(Protocol):
    """Subset of an OpenTelemetry meter used by this module."""

    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> CounterInstrument:
        """Create a monotonic counter instrument."""
        ...

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> HistogramInstrument:
        """Create a histogram instrument."""
        ...


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


class FeatureUsageRecorder:
    """Record feature operations without allowing telemetry to affect requests.

    A recorder uses the process-wide OpenTelemetry meter provider by default,
    which is configured by :mod:`omnigent.runtime.telemetry`.  Tests may pass a
    recording meter.  The operation context supports both ``with`` and
    ``async with`` so it can wrap HTTP and WebSocket route bodies.
    """

    def __init__(
        self,
        meter: MeterLike | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize feature-usage instruments.

        :param meter: Optional injected OpenTelemetry meter.
        :param clock: Monotonic clock used to measure operation duration.
        """
        self._clock = clock
        try:
            effective_meter = meter or otel_metrics.get_meter(_OTEL_METER_NAME)
            self._usage = effective_meter.create_counter(
                _USAGE_COUNTER_NAME,
                unit="{operation}",
                description="Feature operations completed by the Omnigent server.",
            )
            self._duration = effective_meter.create_histogram(
                _DURATION_HISTOGRAM_NAME,
                unit="s",
                description="Feature operation duration in seconds.",
            )
        except Exception:  # noqa: BLE001
            # Metrics must never make an application operation fail.
            self._usage = _NoopCounter()
            self._duration = _NoopHistogram()

    def operation(
        self,
        *,
        feature_name: str,
        operation: str,
        actor_user_id: str,
        session_owner_id: str | None = None,
    ) -> FeatureUsageOperation:
        """Start one feature operation.

        :param feature_name: Low-cardinality feature name, such as ``"attachment"``.
        :param operation: Low-cardinality operation name, such as ``"upload"``.
        :param actor_user_id: Canonical authenticated user ID from ``AuthProvider``.
        :param session_owner_id: Canonical owner ID, included only if it differs
            from the actor.
        :returns: A synchronous and asynchronous operation context.
        """
        return FeatureUsageOperation(
            recorder=self,
            feature_name=feature_name,
            operation=operation,
            actor_user_id=actor_user_id,
            session_owner_id=session_owner_id,
        )

    def _record(
        self,
        *,
        feature_name: str,
        operation: str,
        actor_user_id: str,
        session_owner_id: str | None,
        outcome: UsageOutcome,
        duration_seconds: float,
        failure_reason: FailureReason | None,
        feature_attributes: Mapping[str, str],
    ) -> None:
        """Safely publish a completed operation measurement."""
        attributes: dict[str, str] = {
            "omnigent.feature.name": feature_name,
            "omnigent.feature.operation": operation,
            "omnigent.feature.outcome": outcome,
            "omnigent.actor.user_id": actor_user_id,
        }
        if session_owner_id is not None and session_owner_id != actor_user_id:
            attributes["omnigent.session.owner_id"] = session_owner_id
        if failure_reason is not None:
            attributes["omnigent.failure.reason"] = failure_reason
        attributes.update(feature_attributes)

        try:
            self._usage.add(1, attributes=attributes)
            self._duration.record(max(0.0, duration_seconds), attributes=attributes)
        except Exception:  # noqa: BLE001
            # Exporter or instrument failures are observational only.
            return


class FeatureUsageOperation:
    """One idempotently completed feature operation."""

    def __init__(
        self,
        *,
        recorder: FeatureUsageRecorder,
        feature_name: str,
        operation: str,
        actor_user_id: str,
        session_owner_id: str | None,
    ) -> None:
        """Initialize the operation context."""
        self._recorder = recorder
        self._feature_name = feature_name
        self._operation = operation
        self._actor_user_id = actor_user_id
        self._session_owner_id = session_owner_id
        self._feature_attributes: dict[str, str] = {}
        self._started_at = self._now()
        self._finished = False

    def __enter__(self) -> FeatureUsageOperation:
        """Enter a synchronous operation context."""
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object | None,
    ) -> Literal[False]:
        """Complete the operation while preserving an application exception."""
        del exception, traceback
        self._finish_from_exception(exception_type)
        return False

    async def __aenter__(self) -> FeatureUsageOperation:
        """Enter an asynchronous operation context."""
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object | None,
    ) -> Literal[False]:
        """Complete an asynchronous operation while preserving exceptions."""
        del exception, traceback
        self._finish_from_exception(exception_type)
        return False

    def succeed(self) -> None:
        """Record successful completion now, at most once."""
        self.finish("success")

    def fail(self, reason: FailureReason = "unknown") -> None:
        """Record a failed completion with a bounded reason."""
        self.finish("failed", failure_reason=reason)

    def cancel(self) -> None:
        """Record cancellation now, at most once."""
        self.finish("cancelled")

    def abandon(self) -> None:
        """Record abandonment now, at most once."""
        self.finish("abandoned")

    def reject(self, reason: FailureReason = "validation") -> None:
        """Record a rejected operation with a bounded reason."""
        self.finish("rejected", failure_reason=reason)

    def set_attribute(self, key: object, value: object) -> None:
        """Set one allowlisted, string-valued feature attribute.

        Unknown keys and non-string values are ignored.  Keeping the allowlist
        here prevents call sites from accidentally sending session identifiers
        or user-provided content to metrics.

        :param key: Candidate metric attribute key.
        :param value: Candidate bounded attribute value.
        """
        try:
            if (
                not self._finished
                and isinstance(key, str)
                and key in FEATURE_ATTRIBUTE_KEYS
                and isinstance(value, str)
            ):
                self._feature_attributes[key] = value
        except Exception:  # noqa: BLE001
            # Attribute collection is observational and must not affect work.
            return

    def finish(
        self,
        outcome: UsageOutcome,
        *,
        failure_reason: FailureReason | None = None,
    ) -> None:
        """Record a completion exactly once.

        Explicit completion is useful for WebSocket disconnect and validation
        paths.  The enclosing context becomes a no-op after this call.
        """
        if self._finished:
            return
        self._finished = True
        self._recorder._record(
            feature_name=self._feature_name,
            operation=self._operation,
            actor_user_id=self._actor_user_id,
            session_owner_id=self._session_owner_id,
            outcome=outcome,
            duration_seconds=self._elapsed_seconds(),
            failure_reason=failure_reason,
            feature_attributes=self._feature_attributes,
        )

    def _finish_from_exception(self, exception_type: type[BaseException] | None) -> None:
        """Classify context exit without suppressing the underlying exception."""
        if exception_type is None:
            self.succeed()
        elif issubclass(exception_type, asyncio.CancelledError):
            self.cancel()
        elif issubclass(exception_type, Exception):
            self.fail("backend")
        else:
            self.abandon()

    def _elapsed_seconds(self) -> float:
        """Return a non-negative elapsed duration without exposing clock errors."""
        return max(0.0, self._now() - self._started_at)

    def _now(self) -> float:
        """Read the monotonic clock without allowing telemetry support to fail work."""
        try:
            return self._recorder._clock()
        except Exception:  # noqa: BLE001
            return 0.0


_process_recorder: FeatureUsageRecorder | None = None
_process_recorder_lock = Lock()


def get_feature_usage_recorder() -> FeatureUsageRecorder:
    """Return the lazily initialized process-wide feature-usage recorder."""
    global _process_recorder

    with _process_recorder_lock:
        if _process_recorder is None:
            _process_recorder = FeatureUsageRecorder()
        return _process_recorder


def set_feature_usage_recorder_for_testing(recorder: FeatureUsageRecorder | None) -> None:
    """Replace the process-wide recorder for tests; ``None`` resets it."""
    global _process_recorder

    with _process_recorder_lock:
        _process_recorder = recorder
