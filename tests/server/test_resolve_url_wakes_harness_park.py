"""The resolve-URL path (UI Approve) must wake a parked harness elicitation via
its ``resolved_elsewhere`` event, not only its Future.

Before the fix, ``_resolve_elicitation`` set the Future but never signalled
``_harness_parked_elicitations``, so an ASK-gated tool call whose long-poll had
severed/re-parked never woke on UI Approve → indefinite hang. Only the
``/events`` path signalled it. This locks the resolve-URL path in.
"""

from __future__ import annotations

import asyncio

import pytest

from omnigent.runtime import pending_elicitations
from omnigent.server.feature_usage_metrics import (
    FeatureUsageRecorder,
    set_feature_usage_recorder_for_testing,
)
from omnigent.server.routes import sessions as S
from omnigent.server.routes._sessions import orchestration as O
from tests.server.feature_usage_helpers import RecordingMeter


@pytest.mark.asyncio
async def test_resolve_elicitation_signals_parked_harness_elicitation():
    sid = "conv_resolveurl_test"
    eid = "elicit_evaluate_deadbeefdeadbeefdeadbeefdeadbeef"
    parked = S._ParkedHarnessElicitation(
        session_id=sid,
        tool_name="mcp_example__apply_change",
        tool_input={},
        resolved_elsewhere=asyncio.Event(),
    )
    S._harness_parked_elicitations[eid] = parked
    S._harness_elicitation_owners[eid] = sid
    try:
        assert not parked.resolved_elsewhere.is_set()
        # runner_router=None → the runner forward is skipped; we only assert the
        # server-side parked-elicitation wake.
        await S._resolve_elicitation(sid, {"elicitation_id": eid, "action": "accept"}, None)
        assert parked.resolved_elsewhere.is_set(), (
            "resolve-URL must signal the parked harness elicitation (resolved_elsewhere), "
            "otherwise an ASK-gated tool hangs on UI Approve"
        )
    finally:
        S._harness_parked_elicitations.pop(eid, None)
        S._harness_elicitation_owners.pop(eid, None)


@pytest.mark.asyncio
async def test_not_parked_resolve_keeps_verdict_tombstone():
    # Regression (#62 review): when nothing is parked (severed long-poll, before
    # the retry re-parks), resolve-URL must store a pre-resolved tombstone
    # carrying the ACTUAL verdict, so the re-park returns it. The resolved_elsewhere
    # wake must NOT clobber it with a verdict-less tombstone.
    sid = "conv_tombstone"
    eid = "elicit_evaluate_22222222222222222222222222222222"
    S._harness_parked_elicitations.pop(eid, None)  # ensure NOT parked
    S._harness_pre_resolved_elicitations.pop(eid, None)
    try:
        await S._resolve_elicitation(sid, {"elicitation_id": eid, "action": "accept"}, None)
        tomb = S._harness_pre_resolved_elicitations.get(eid)
        assert tomb is not None, "a not-parked resolve must leave a pre-resolved tombstone"
        assert tomb.result is not None, "the tombstone must carry the verdict (not clobbered)"
    finally:
        S._harness_pre_resolved_elicitations.pop(eid, None)


@pytest.mark.asyncio
async def test_resolve_elicitation_wrong_session_does_not_wake():
    # Ownership guard: a resolve for a DIFFERENT session must not wake this park.
    sid = "conv_owner"
    eid = "elicit_evaluate_11111111111111111111111111111111"
    parked = S._ParkedHarnessElicitation(
        session_id=sid, tool_name="t", tool_input={}, resolved_elsewhere=asyncio.Event()
    )
    S._harness_parked_elicitations[eid] = parked
    S._harness_elicitation_owners[eid] = sid
    try:
        await S._resolve_elicitation(
            "conv_other", {"elicitation_id": eid, "action": "accept"}, None
        )
        assert not parked.resolved_elsewhere.is_set()
    finally:
        S._harness_parked_elicitations.pop(eid, None)
        S._harness_elicitation_owners.pop(eid, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["accept", "decline", "cancel", "timeout"])
async def test_resolve_elicitation_records_terminal_decision_for_actual_resolver(
    action: str,
) -> None:
    """All terminal approval decisions retain the resolver, not the owner, as actor."""
    sid = f"conv_metric_{action}"
    eid = f"elicit_metric_{action}"
    meter = RecordingMeter()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    pending_elicitations.record_publish(
        sid,
        {
            "type": "response.elicitation_request",
            "elicitation_id": eid,
            "params": {},
        },
    )
    try:
        await S._resolve_elicitation(
            sid,
            {"elicitation_id": eid, "action": action},
            None,
            actor_user_id="resolver@example.com",
            session_owner_id="owner@example.com",
        )
        assert meter.counter.records == [
            {
                "omnigent.feature.name": "approval",
                "omnigent.feature.operation": "resolve",
                "omnigent.feature.outcome": "success",
                "omnigent.actor.user_id": "resolver@example.com",
                "omnigent.session.owner_id": "owner@example.com",
                "omnigent.approval.decision": action,
            }
        ]
    finally:
        pending_elicitations.resolve(sid, eid)
        set_feature_usage_recorder_for_testing(None)


class _OwnerStore:
    """Minimal conversation-store double for approval attribution."""

    def get_session_owner(self, session_id: str) -> str:
        assert session_id == "conv_shared"
        return "owner@example.com"


class _TimeoutOwnerStore:
    """Minimal owner lookup for a retried timeout lifecycle."""

    def get_session_owner(self, session_id: str) -> str:
        assert session_id == "conv_timeout_retry"
        return "owner@example.com"


class _FlakyOwnerStore:
    """Fails once so telemetry can retry ownership lookup without losing usage."""

    def __init__(self) -> None:
        self._calls = 0

    def get_session_owner(self, session_id: str) -> str:
        assert session_id == "conv_flaky_owner"
        self._calls += 1
        if self._calls == 1:
            raise RuntimeError("temporary owner-store failure")
        return "owner@example.com"


@pytest.mark.asyncio
async def test_approval_request_preserves_collaborator_actor_and_owner() -> None:
    """A native-hook initiator is not collapsed to the session owner."""
    meter = RecordingMeter()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    try:
        await O._record_approval_request(
            "conv_shared",
            "elicit_collaborator",
            _OwnerStore(),  # type: ignore[arg-type]
            "collaborator@example.com",
        )
        # A hook retry with the stable elicitation ID republishes the card but
        # is still the same approval request.
        await O._record_approval_request(
            "conv_shared",
            "elicit_collaborator",
            _OwnerStore(),  # type: ignore[arg-type]
            "collaborator@example.com",
        )
        assert meter.counter.records == [
            {
                "omnigent.feature.name": "approval",
                "omnigent.feature.operation": "request",
                "omnigent.feature.outcome": "success",
                "omnigent.actor.user_id": "collaborator@example.com",
                "omnigent.session.owner_id": "owner@example.com",
            }
        ]
    finally:
        set_feature_usage_recorder_for_testing(None)


@pytest.mark.asyncio
async def test_retried_harness_timeout_records_one_terminal_resolution() -> None:
    """A re-park timeout cannot duplicate the terminal approval metric."""
    meter = RecordingMeter()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    try:
        for _ in range(2):
            await O._record_approval_timeout(
                "conv_timeout_retry",
                "elicit_timeout_retry",
                _TimeoutOwnerStore(),  # type: ignore[arg-type]
            )
        assert meter.counter.records == [
            {
                "omnigent.feature.name": "approval",
                "omnigent.feature.operation": "resolve",
                "omnigent.feature.outcome": "success",
                "omnigent.actor.user_id": "owner@example.com",
                "omnigent.approval.decision": "timeout",
            }
        ]
    finally:
        set_feature_usage_recorder_for_testing(None)


@pytest.mark.asyncio
async def test_owner_lookup_failure_does_not_claim_approval_request() -> None:
    """A telemetry-only owner lookup failure leaves the stable ID retryable."""
    meter = RecordingMeter()
    owner_store = _FlakyOwnerStore()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    try:
        await O._record_approval_request(
            "conv_flaky_owner", "elicit_flaky_owner", owner_store  # type: ignore[arg-type]
        )
        await O._record_approval_request(
            "conv_flaky_owner", "elicit_flaky_owner", owner_store  # type: ignore[arg-type]
        )
        assert meter.counter.records == [
            {
                "omnigent.feature.name": "approval",
                "omnigent.feature.operation": "request",
                "omnigent.feature.outcome": "success",
                "omnigent.actor.user_id": "owner@example.com",
            }
        ]
    finally:
        set_feature_usage_recorder_for_testing(None)


@pytest.mark.asyncio
async def test_late_user_verdict_after_timeout_does_not_duplicate_resolution() -> None:
    """The deferred-clear window cannot count both timeout and late accept."""
    sid = "conv_timeout_then_accept"
    eid = "elicit_timeout_then_accept"
    meter = RecordingMeter()
    set_feature_usage_recorder_for_testing(FeatureUsageRecorder(meter))
    pending_elicitations.record_publish(
        sid,
        {"type": "response.elicitation_request", "elicitation_id": eid, "params": {}},
    )
    try:
        await O._record_approval_timeout(sid, eid, None)
        await S._resolve_elicitation(
            sid,
            {"elicitation_id": eid, "action": "accept"},
            None,
            actor_user_id="resolver@example.com",
        )
        assert meter.counter.records == [
            {
                "omnigent.feature.name": "approval",
                "omnigent.feature.operation": "resolve",
                "omnigent.feature.outcome": "success",
                "omnigent.actor.user_id": "local",
                "omnigent.approval.decision": "timeout",
            }
        ]
    finally:
        pending_elicitations.resolve(sid, eid)
        set_feature_usage_recorder_for_testing(None)
