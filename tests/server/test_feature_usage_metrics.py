"""Tests for server-side OpenTelemetry feature-usage metrics."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest
from opentelemetry.util.types import Attributes

from omnigent.errors import ErrorCode, OmnigentError
from omnigent.server.feature_usage_metrics import (
    FEATURE_ATTRIBUTE_KEYS,
    FeatureUsageRecorder,
    classify_feature_usage_exception,
    get_feature_usage_recorder,
    set_feature_usage_recorder_for_testing,
)


@dataclass(frozen=True)
class _MetricRecord:
    """One value recorded by a fake OpenTelemetry instrument."""

    amount: int | float
    attributes: Attributes


@dataclass
class _FakeCounter:
    """Recording counter used by tests."""

    records: list[_MetricRecord] = field(default_factory=list)
    raise_error: bool = False

    def add(self, amount: int | float, attributes: Attributes = None) -> None:
        """Record a counter call or simulate an instrumentation failure."""
        if self.raise_error:
            raise RuntimeError("counter unavailable")
        self.records.append(_MetricRecord(amount, attributes))


@dataclass
class _FakeHistogram:
    """Recording histogram used by tests."""

    records: list[_MetricRecord] = field(default_factory=list)
    raise_error: bool = False

    def record(self, amount: int | float, attributes: Attributes = None) -> None:
        """Record a histogram call or simulate an instrumentation failure."""
        if self.raise_error:
            raise RuntimeError("histogram unavailable")
        self.records.append(_MetricRecord(amount, attributes))


@dataclass
class _FakeMeter:
    """Meter that creates the two recording instruments."""

    counter: _FakeCounter = field(default_factory=_FakeCounter)
    histogram: _FakeHistogram = field(default_factory=_FakeHistogram)

    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _FakeCounter:
        """Return the fake counter."""
        assert name == "omnigent.feature.usage"
        assert unit == "{operation}"
        assert description
        return self.counter

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _FakeHistogram:
        """Return the fake histogram."""
        assert name == "omnigent.feature.operation.duration"
        assert unit == "s"
        assert description
        return self.histogram


@dataclass
class _Clock:
    """Deterministic monotonic clock."""

    value: float = 0.0

    def __call__(self) -> float:
        """Return the current fake time."""
        return self.value


def test_operation_records_once_with_common_attributes_and_duration() -> None:
    """A normal operation emits one counter increment and duration sample."""
    meter = _FakeMeter()
    clock = _Clock()
    recorder = FeatureUsageRecorder(meter, clock=clock)

    with recorder.operation(
        feature_name="attachment",
        operation="upload",
        actor_user_id="actor@example.com",
    ):
        clock.value = 2.5

    assert meter.counter.records == [
        _MetricRecord(
            1,
            {
                "omnigent.feature.name": "attachment",
                "omnigent.feature.operation": "upload",
                "omnigent.feature.outcome": "success",
                "omnigent.actor.user_id": "actor@example.com",
            },
        )
    ]
    assert meter.histogram.records == [
        _MetricRecord(2.5, meter.counter.records[0].attributes),
    ]


def test_owner_is_omitted_for_actor_and_recorded_for_collaborator() -> None:
    """Owner attribution only adds a series dimension for shared sessions."""
    meter = _FakeMeter()
    recorder = FeatureUsageRecorder(meter)

    with recorder.operation(
        feature_name="sharing",
        operation="grant",
        actor_user_id="owner@example.com",
        session_owner_id="owner@example.com",
    ):
        pass
    with recorder.operation(
        feature_name="sharing",
        operation="grant",
        actor_user_id="collaborator@example.com",
        session_owner_id="owner@example.com",
    ):
        pass

    assert "omnigent.session.owner_id" not in meter.counter.records[0].attributes
    assert meter.counter.records[1].attributes["omnigent.session.owner_id"] == "owner@example.com"


def test_explicit_completion_is_exactly_once() -> None:
    """Explicit WebSocket-style completion does not double count on context exit."""
    meter = _FakeMeter()
    recorder = FeatureUsageRecorder(meter)

    with recorder.operation(
        feature_name="dictation",
        operation="take",
        actor_user_id="user@example.com",
    ) as usage:
        usage.abandon()
        usage.succeed()

    assert len(meter.counter.records) == 1
    assert meter.counter.records[0].attributes["omnigent.feature.outcome"] == "abandoned"


def test_allowlisted_feature_attributes_are_emitted_after_context_entry() -> None:
    """Route code can add each approved bounded attribute before completion."""
    meter = _FakeMeter()
    recorder = FeatureUsageRecorder(meter)
    allowed_attributes = {
        "omnigent.dictation.engine": "whisper",
        "omnigent.attachment.category": "image",
        "omnigent.attachment.size_bucket": "1mib_to_5mib",
        "omnigent.sharing.target_type": "user",
        "omnigent.sharing.access_level": "write",
        "omnigent.fork.history_scope": "full",
        "omnigent.policy.scope": "session",
        "omnigent.policy.type": "approval",
        "omnigent.approval.decision": "approved",
    }

    with recorder.operation(
        feature_name="attachment",
        operation="upload",
        actor_user_id="user@example.com",
    ) as usage:
        for key, value in allowed_attributes.items():
            usage.set_attribute(key, value)

    assert set(allowed_attributes) == FEATURE_ATTRIBUTE_KEYS
    assert all(
        meter.counter.records[0].attributes[key] == value
        for key, value in allowed_attributes.items()
    )


def test_unknown_sensitive_and_unsupported_feature_attributes_are_ignored() -> None:
    """The feature-attribute API cannot add arbitrary data to metric series."""
    meter = _FakeMeter()
    recorder = FeatureUsageRecorder(meter)

    with recorder.operation(
        feature_name="attachment",
        operation="upload",
        actor_user_id="user@example.com",
    ) as usage:
        usage.set_attribute("omnigent.session.id", "conv_secret")
        usage.set_attribute("filename", "private.pdf")
        usage.set_attribute("omnigent.attachment.category", 10)
        usage.set_attribute(object(), "image")

    attributes = meter.counter.records[0].attributes
    assert "omnigent.session.id" not in attributes
    assert "filename" not in attributes
    assert "omnigent.attachment.category" not in attributes


def test_process_wide_recorder_can_be_replaced_for_tests() -> None:
    """Production call sites share a recorder while tests can inject one."""
    recorder = FeatureUsageRecorder(_FakeMeter())
    set_feature_usage_recorder_for_testing(recorder)
    try:
        assert get_feature_usage_recorder() is recorder
    finally:
        set_feature_usage_recorder_for_testing(None)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (OmnigentError("denied", code=ErrorCode.FORBIDDEN), ("rejected", "permission")),
        (OmnigentError("bad", code=ErrorCode.INVALID_INPUT), ("rejected", "validation")),
        (OmnigentError("broken", code=ErrorCode.INTERNAL_ERROR), ("failed", "backend")),
    ],
)
def test_route_error_classification_is_bounded(
    error: OmnigentError,
    expected: tuple[str, str],
) -> None:
    """Route exceptions never leak unbounded failure details into metrics."""
    assert classify_feature_usage_exception(error) == expected


@pytest.mark.asyncio
async def test_async_operation_preserves_cancellation_and_records_it() -> None:
    """An async route cancellation remains visible to the caller."""
    meter = _FakeMeter()
    recorder = FeatureUsageRecorder(meter)

    with pytest.raises(asyncio.CancelledError):
        async with recorder.operation(
            feature_name="context",
            operation="compact",
            actor_user_id="user@example.com",
        ):
            raise asyncio.CancelledError()

    assert meter.counter.records[0].attributes["omnigent.feature.outcome"] == "cancelled"


def test_operation_preserves_application_exception_and_classifies_failure() -> None:
    """Application exceptions escape unchanged after a failed usage record."""
    meter = _FakeMeter()
    recorder = FeatureUsageRecorder(meter)

    with pytest.raises(ValueError, match="application failure"):
        with recorder.operation(
            feature_name="forking",
            operation="fork",
            actor_user_id="user@example.com",
        ):
            raise ValueError("application failure")

    assert meter.counter.records[0].attributes["omnigent.feature.outcome"] == "failed"
    assert meter.counter.records[0].attributes["omnigent.failure.reason"] == "backend"


def test_instrumentation_failure_is_swallowed() -> None:
    """A broken OpenTelemetry instrument cannot fail an application operation."""
    meter = _FakeMeter(counter=_FakeCounter(raise_error=True))
    recorder = FeatureUsageRecorder(meter)

    with recorder.operation(
        feature_name="policy",
        operation="register",
        actor_user_id="user@example.com",
    ):
        pass

    assert meter.counter.records == []
    assert meter.histogram.records == []
