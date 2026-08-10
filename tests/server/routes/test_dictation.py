"""Tests for the dictation stream WS and its ``/v1/info`` capability bit.

The WebSocket tests drive the real route against the deterministic
:class:`FakeDictationEngine` injected through ``engine_provider`` — no
sherpa-onnx dependency, no models, no microphone. The engine reveals one
word of ``FAKE_SCRIPT`` per 100 ms of audio fed, so tests control the
transcript by the number of PCM bytes they send.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from omnigent.server import dictation as dictation_engine
from omnigent.server.dictation import FAKE_SCRIPT, MAX_STREAMS_ENV, FakeDictationEngine
from omnigent.server.feature_usage_metrics import (
    FeatureUsageRecorder,
    set_feature_usage_recorder_for_testing,
)
from omnigent.server.routes.dictation import create_dictation_router

# One fake-engine "word" of audio: 100 ms of 16 kHz mono s16le.
_WORD_BYTES = b"\x00" * (16000 * 2 // 10)
_SCRIPT_WORDS = FAKE_SCRIPT.split()


class _UsageCounter:
    """Minimal metric counter for route instrumentation tests."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def add(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        assert amount == 1
        self.records.append(dict(attributes or {}))


class _UsageHistogram:
    """Minimal histogram required by the recorder."""

    def record(self, amount: int | float, attributes: dict[str, Any] | None = None) -> None:
        del amount, attributes


class _UsageMeter:
    """Metric meter exposing a recording usage counter."""

    def __init__(self) -> None:
        self.counter = _UsageCounter()

    def create_counter(self, *args: Any, **kwargs: Any) -> _UsageCounter:
        del args, kwargs
        return self.counter

    def create_histogram(self, *args: Any, **kwargs: Any) -> _UsageHistogram:
        del args, kwargs
        return _UsageHistogram()


class _NoIdentityAuthProvider:
    """Auth provider whose handshake yields no identity."""

    def get_user_id(self, request: object) -> None:
        """Always return ``None`` (unauthenticated)."""
        del request
        return


def _fake_app(**router_kwargs: object) -> FastAPI:
    """Bare app carrying only the dictation router with a fake engine."""
    app = FastAPI()
    router_kwargs.setdefault("engine_provider", FakeDictationEngine)
    app.include_router(create_dictation_router(**router_kwargs), prefix="/v1")
    return app


async def test_info_carries_dictation_capability(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /v1/info advertises dictation for the web UI capability probe."""
    monkeypatch.setenv(dictation_engine.ENGINE_ENV, dictation_engine.ENGINE_FAKE)
    resp = await client.get("/v1/info")
    assert resp.status_code == 200
    assert resp.json()["dictation_available"] is True


async def test_info_reports_dictation_unavailable(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: object,
) -> None:
    """Without an engine (no extra or no models) /v1/info advertises false."""
    monkeypatch.setenv(dictation_engine.MODEL_DIR_ENV, str(tmp_path))
    monkeypatch.delenv(dictation_engine.ENGINE_ENV, raising=False)
    resp = await client.get("/v1/info")
    assert resp.status_code == 200
    assert resp.json()["dictation_available"] is False


def test_stream_partial_final_stop_flow() -> None:
    """Audio in → ready, partial, final, stopped events out."""
    with TestClient(_fake_app()) as tc, tc.websocket_connect("/v1/dictation/stream") as ws:
        assert json.loads(ws.receive_text()) == {"type": "ready"}

        # Two words of audio → a partial with the first two script words.
        ws.send_bytes(_WORD_BYTES * 2)
        partial = json.loads(ws.receive_text())
        assert partial == {"type": "partial", "text": " ".join(_SCRIPT_WORDS[:2])}

        # The rest of the script → the fake finalizes the sentence.
        ws.send_bytes(_WORD_BYTES * (len(_SCRIPT_WORDS) - 2))
        final = json.loads(ws.receive_text())
        assert final == {"type": "final", "text": FAKE_SCRIPT}

        ws.send_text(json.dumps({"type": "stop"}))
        stopped = json.loads(ws.receive_text())
        assert stopped == {"type": "stopped", "text": ""}


def test_stream_stop_flushes_tail() -> None:
    """stop mid-utterance returns the un-finalized words as the tail."""
    with TestClient(_fake_app()) as tc, tc.websocket_connect("/v1/dictation/stream") as ws:
        assert json.loads(ws.receive_text())["type"] == "ready"
        ws.send_bytes(_WORD_BYTES * 3)
        assert json.loads(ws.receive_text())["type"] == "partial"
        ws.send_text(json.dumps({"type": "stop"}))
        stopped = json.loads(ws.receive_text())
        assert stopped == {"type": "stopped", "text": " ".join(_SCRIPT_WORDS[:3])}


def test_stream_stop_records_one_feature_usage_metric() -> None:
    """A normal take emits exactly one bounded feature-usage measurement."""
    meter = _UsageMeter()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    try:
        with TestClient(_fake_app()) as tc, tc.websocket_connect("/v1/dictation/stream") as ws:
            assert json.loads(ws.receive_text())["type"] == "ready"
            ws.send_text(json.dumps({"type": "stop"}))
            assert json.loads(ws.receive_text())["type"] == "stopped"
    finally:
        set_feature_usage_recorder_for_testing(None)

    assert meter.counter.records == [
        {
            "omnigent.feature.name": "dictation",
            "omnigent.feature.operation": "take",
            "omnigent.feature.outcome": "success",
            "omnigent.actor.user_id": "local",
            "omnigent.dictation.engine": "other",
        }
    ]


def test_stream_ignores_unknown_control_messages() -> None:
    """Unknown text frames are ignored for forward compatibility."""
    with TestClient(_fake_app()) as tc, tc.websocket_connect("/v1/dictation/stream") as ws:
        assert json.loads(ws.receive_text())["type"] == "ready"
        ws.send_text(json.dumps({"type": "does-not-exist"}))
        ws.send_text("not json at all")
        # The stream is still alive and transcribing after both.
        ws.send_bytes(_WORD_BYTES)
        assert json.loads(ws.receive_text()) == {
            "type": "partial",
            "text": _SCRIPT_WORDS[0],
        }


def test_stream_closes_take_on_abrupt_disconnect() -> None:
    """A vanished client still releases the take (worker-slot safety).

    The remote relay engine holds a worker capacity slot until its
    handle is closed; the route must close handles on the disconnect
    path, not just on a clean stop.
    """
    engine = FakeDictationEngine()
    app = _fake_app(engine_provider=lambda: engine)
    with TestClient(app) as tc:
        with tc.websocket_connect("/v1/dictation/stream") as ws:
            assert json.loads(ws.receive_text())["type"] == "ready"
            ws.send_bytes(_WORD_BYTES)
            # Exit without stop: an abrupt browser disconnect.
        assert engine.last_stream is not None
        deadline = time.monotonic() + 5
        while not engine.last_stream.closed and time.monotonic() < deadline:
            time.sleep(0.02)
        assert engine.last_stream.closed


def test_stream_rejects_unauthenticated_handshake() -> None:
    """With an auth provider and no identity, the handshake is refused."""
    app = _fake_app(auth_provider=_NoIdentityAuthProvider())
    meter = _UsageMeter()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    try:
        with TestClient(app) as tc:
            with pytest.raises(WebSocketDisconnect):
                with tc.websocket_connect("/v1/dictation/stream") as ws:
                    ws.receive_text()
    finally:
        set_feature_usage_recorder_for_testing(None)

    assert meter.counter.records == []


def test_stream_capacity_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Connections beyond the stream cap close with 1013 (try later)."""
    monkeypatch.setenv(MAX_STREAMS_ENV, "1")
    with TestClient(_fake_app()) as tc:
        with tc.websocket_connect("/v1/dictation/stream") as first:
            assert json.loads(first.receive_text())["type"] == "ready"
            with tc.websocket_connect("/v1/dictation/stream") as second:
                with pytest.raises(WebSocketDisconnect) as excinfo:
                    second.receive_text()
                assert excinfo.value.code == 1013
