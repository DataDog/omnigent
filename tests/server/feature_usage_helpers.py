"""Small OpenTelemetry fakes shared by feature-usage route tests."""

from __future__ import annotations

from typing import Any


class RecordingCounter:
    """Counter that keeps only the attributes relevant to route assertions."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def add(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        assert amount == 1
        self.records.append(dict(attributes or {}))


class RecordingHistogram:
    """No-op duration instrument required by the recorder."""

    def record(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        del amount, attributes


class RecordingMeter:
    """OpenTelemetry meter fake exposing a usage counter."""

    def __init__(self) -> None:
        self.counter = RecordingCounter()

    def create_counter(self, *args: Any, **kwargs: Any) -> RecordingCounter:
        del args, kwargs
        return self.counter

    def create_histogram(self, *args: Any, **kwargs: Any) -> RecordingHistogram:
        del args, kwargs
        return RecordingHistogram()
