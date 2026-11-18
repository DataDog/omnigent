"""
:class:`HarnessApp` scaffold — base class every per-harness wrap
inherits.

The scaffold owns the boilerplate every harness needs:

- A FastAPI app exposing the harness API subset
  (POST /v1/sessions/{conversation_id}/events for the discriminated
  downward-event surface, GET /health). See
  ``designs/SERVER_HARNESS_CONTRACT.md`` §The Harness API Subset
  and ``designs/session_rearchitecture.md`` §3.
- Per-turn in-memory state: dicts of Futures the event handlers
  resolve when a ``tool_result`` event (tool output) or
  ``approval`` event (elicitation reply) arrives. Layer 2
  (per-turn) and Layer 3 (per-tool-dispatch) bookkeeping per
  §Harness in-memory state.
- Heartbeat emitter at :data:`_HEARTBEAT_INTERVAL_S` cadence.
- Cancellation event set by an ``interrupt`` event — the
  per-harness ``run_turn`` should poll it before each LLM
  iteration / native tool call.
- In-band routing for steering: a ``message`` event with
  ``previous_response_id`` matching the in-flight turn becomes
  an injection rather than starting a new turn (also covers
  async function-call completions).
- Graceful SIGTERM / SIGINT handling: cancel the in-flight turn,
  drain pending tool results for a short grace window, then shut
  down the FastAPI app.

A subclass implements just two things: an ``async def run_turn(
request, ctx) -> None`` method that runs the LLM-tool loop using
``ctx`` to interact with upstream, and a ``create_app()``-style
free function that instantiates the subclass and returns its
FastAPI app for the runner to serve.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from fastapi import APIRouter, FastAPI, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, TypeAdapter

from omnigent.errors import ErrorCode, HarnessTransportClosedError, OmnigentError
from omnigent.inner.acp_standard_subagents import (
    ACTION_FEATURES as _SESSION_ACTION_FEATURES,
)
from omnigent.inner.acp_standard_subagents import (
    ACTION_PROTOCOL as _SESSION_ACTION_PROTOCOL,
)
from omnigent.inner.acp_standard_subagents import (
    ACTION_PROTOCOL_VERSION as _SESSION_ACTION_VERSION,
)
from omnigent.inner.executor import (
    SESSION_REPORT_FEATURES,
    LiveMcpHarnessNotRunning,
    LiveMcpReconnectRefused,
    LiveMcpServerStatus,
    LiveMcpServerUnknown,
    SessionReportPage,
)
from omnigent.native import _native_forwarder_health as native_forwarder_health
from omnigent.policies.types import FAIL_CLOSED_PHASES
from omnigent.runtime.harnesses.session_reports import (
    SessionReportJournalBusy,
    SessionReportJournalCapacity,
    SessionReportJournalConflict,
    SessionReportJournalCorrupt,
    SessionReportJournalError,
    SessionReportJournalFenced,
    SessionReportJournalUnavailable,
    SessionReportJournalVersionMismatch,
)
from omnigent.runtime.tool_output import cap_tool_output
from omnigent.server.schemas import (
    CompletedEvent,
    CreatedEvent,
    CreateResponseRequest,
    ElicitationRequestEvent,
    ElicitationRequestParams,
    ElicitationResolvedEvent,
    ElicitationResult,
    FailedEvent,
    HarnessStreamEvent,
    HeartbeatEvent,
    InProgressEvent,
    OutputItemDoneEvent,
    PolicyEvaluationRequestEvent,
    ResponseObject,
    RetryErrorDetail,
    RetryEvent,
    ServerStreamEvent,
    Usage,
)

_logger = logging.getLogger(__name__)

# Cadence for ``response.heartbeat`` SSE events while a turn is
# streaming. Matches the existing Omnigent cadence at
# ``omnigent/runtime/workflow.py:3732`` — 15 seconds is short
# enough that ~3 missed intervals (45s) is a reasonable
# dead-detection threshold for AP, long enough that the
# per-process emit overhead is negligible.
_HEARTBEAT_INTERVAL_S = 15.0

# Grace period between SIGTERM / SIGINT arrival and the FastAPI
# app exiting. Long enough to let a well-behaved harness flush
# pending responses + close the SDK session cleanly; short enough
# that a wedged subprocess doesn't block process-manager release
# (which has its own SIGKILL escalation at
# ``_RELEASE_GRACE_S = 5.0`` in process_manager.py).
_SHUTDOWN_GRACE_S = 4.5

# Timeout for the policy evaluation round-trip (harness → runner →
# Omnigent server → runner → harness). Held at one day (86400s) — matching
# the deciding policy's default ``ask_timeout``: a TOOL_CALL/REQUEST ASK parks
# server-side until a human answers, and this gate must block until the
# verdict arrives rather than auto-resolve on a short cut (the cost-policy
# bug). The server caps the real wait via the policy's ``ask_timeout``. On the
# (now rare) expiry the fallback below is phase-aware — TOOL_CALL fails CLOSED
# (DENY), advisory LLM/TOOL_RESULT phases fail OPEN (ALLOW).
_POLICY_EVAL_TIMEOUT_S = 86400.0

# Only these phases can park server-side on a human ASK (see the
# ``policies/evaluate`` route). The others answer in seconds, so a verdict
# lost in delivery must not stall the turn for the full human-wait budget.
_HUMAN_GATED_POLICY_PHASES = frozenset({"PHASE_TOOL_CALL", "PHASE_LLM_REQUEST", "PHASE_REQUEST"})
_UNGATED_POLICY_EVAL_TIMEOUT_S = 120.0

# Session-owned actions are ephemeral (unlike the durable report journal), but
# the pending set and retry tombstones must stay bounded for the lifetime of a
# harness process. Refuse new work at the limits rather than evicting an
# idempotency record and risking the same action being executed twice.
_MAX_PENDING_SESSION_ACTIONS = 512
_MAX_SESSION_ACTION_TOMBSTONES = 2048
_MAX_SESSION_ACTION_POLLS = 4
_SESSION_ACTION_POLL_LIMIT = 512
_SESSION_ACTION_POLL_WAIT_MS = 25_000

# Stable, client-visible error code for a turn-context desync (the inner SDK
# generation outlived its turn — an orphaned tool/policy callback, or a turn
# torn down on a dead harness channel). Deliberately ABSENT from AP's
# retryable-harness-error allowlist so the L2 retry classifier treats it as
# terminal rather than retry-looping into the same wedge. Mirrors the runner's
# ``_RUNNER_TURN_CONTEXT_DESYNC_CODE`` and the harness adapter's orphaned-
# callback safe-fail ``code``.
_TURN_CONTEXT_DESYNC_CODE = "runner_turn_context_desync"

# Per-turn IDLE watchdog: max gap WITHOUT progress before a wedged
# ``run_turn`` becomes ``response.failed`` (vs heartbeating forever).
# Every non-heartbeat ``ctx.emit`` resets the deadline (see
# ``_guarded_run_turn``), so a long-but-active turn is never killed.
# The window must clear the longest single progress-free ``await`` a
# healthy turn can make — notably context compaction, whose summarizing
# LLM call runs as one long ``await`` emitting no non-heartbeat events
# (see ``runtime/compaction.py``). On a near-full context that call can
# exceed the old 240s cap, tripping the watchdog and wedging the session
# in a "Prompt is too long" → compaction → 240s-timeout loop.
# Env var name kept for the ops knob; ``<= 0`` disables.
_DEFAULT_TURN_IDLE_TIMEOUT_S = 3600.0
_TURN_IDLE_TIMEOUT_S = float(
    os.environ.get("HARNESS_TURN_TIMEOUT_S", _DEFAULT_TURN_IDLE_TIMEOUT_S)
)

# Absolute per-turn ceiling on TOTAL turn duration. Progress-aware when
# the idle watchdog is enabled: every real progress event extends the
# ceiling to at least one idle window past now, so a turn that keeps
# emitting is never killed mid-work merely for running long — a stalled
# turn past the ceiling dies via the idle watchdog instead. Deliberate
# tradeoff: under the default config a steadily-emitting turn has NO
# wall-clock cap (this constant does not bound active turns); only when
# the idle watchdog is disabled (``HARNESS_TURN_TIMEOUT_S <= 0``) does
# this act as a strict wall-clock cap. ``<= 0`` disables.
_TURN_ABSOLUTE_TIMEOUT_S = float(os.environ.get("HARNESS_TURN_ABSOLUTE_TIMEOUT_S", "10800"))

# Retry only pre-output wedges after confirmed teardown. Replaying a turn
# with progress can duplicate tool effects; retries never extend the hard cap.
_WEDGED_TURN_RECOVERY_RETRIES = 1

# Set by the harness subprocess entrypoint. Executors import their SDKs lazily
# on the first turn, so that graph is only static once a turn has completed.
_freeze_gc_after_first_turn = False


def arm_gc_freeze_after_first_turn() -> None:
    """Collect and freeze GC-tracked objects once, after this process's first completed turn.

    Only the harness subprocess arms this; an embedding process keeps normal
    GC. A failed or cancelled turn leaves the freeze armed, so a later turn
    still freezes the lazily imported SDK.
    """
    global _freeze_gc_after_first_turn
    _freeze_gc_after_first_turn = True


def _freeze_gc_if_armed(run_task: asyncio.Task[None], ctx: TurnContext) -> None:
    """Run the armed freeze once *run_task* has completed without error.

    Called after the turn's terminal event is sent and its state torn down, so
    the one-time collection (~25 ms) is off the turn's path and frees the
    turn's cycles instead of pinning them. A harness that observes
    ``ctx.cancelled`` and returns normally ended a cancelled turn, not a completed one.
    """
    global _freeze_gc_after_first_turn
    if not _freeze_gc_after_first_turn or not run_task.done() or run_task.cancelled():
        return
    if ctx.cancelled.is_set():
        return
    if run_task.exception() is not None:
        return
    _freeze_gc_after_first_turn = False
    gc.collect()
    gc.freeze()


@dataclass(frozen=True)
class PolicyVerdictPayload:
    """
    Result of a policy evaluation round-trip.

    Returned by :meth:`TurnContext.evaluate_policy` after the runner
    evaluates the policy on the Omnigent server and sends the verdict back
    as a ``policy_verdict`` inbound event.

    :param action: Proto-style verdict string, e.g.
        ``"POLICY_ACTION_ALLOW"`` or ``"POLICY_ACTION_DENY"``.
    :param reason: Human-readable reason from the policy engine.
        ``None`` on ALLOW.
    :param data: Optional data dict from content-rewriting
        policies. ``None`` when the policy does not rewrite.
    """

    action: str
    reason: str | None = None
    data: dict[str, Any] | None = None


# ── Inbound event schemas (POST /v1/sessions/{id}/events) ─────────
#
# The session-keyed surface collapses the four turn-keyed routes
# (start turn / cancel / PATCH tool_results / elicitation reply)
# into a single ``POST /v1/sessions/{conversation_id}/events``
# endpoint discriminated by the body's ``type`` field. Wire shapes
# match ``designs/session_rearchitecture.md`` §3 "Event types and
# direction" — the downward (client → server → runner → harness)
# block. Carrying these as a Pydantic discriminated union gives
# fail-loud rejection of unknown types (422) and per-variant field
# validation in one place.


class MessageEvent(BaseModel):
    """
    Downward ``message`` event — start a new turn or steer an in-flight one.

    A ``message`` arriving while no turn is in flight starts a fresh
    turn (the scaffold allocates a ``response_id`` and runs
    ``run_turn``). A ``message`` arriving while a turn is in flight
    is enqueued onto that turn's injection queue (the harness
    delivers it to ``run_turn`` via :meth:`TurnContext.next_injection`).
    The harness has at most one in-flight turn per conversation, so
    no explicit turn id is needed on the wire.

    Extra fields are accepted (``model_config = extra="allow"``) so
    the runner can pass through optional knobs that
    :class:`CreateResponseRequest` understands (``instructions``,
    ``conversation``, ``reasoning``, ``context_management``,
    ``tools``, ignored-but-tolerated controls like ``temperature``)
    without forcing this schema to mirror every field individually.

    :param type: Event discriminator; always ``"message"``.
    :param role: Always ``"user"`` for downward messages — assistant
        messages flow upward via the runner→server persistence
        channel, not through this endpoint. Constrained at the
        type level so a stray ``role: "assistant"`` POST fails 422
        rather than silently starting a turn with bad provenance.
    :param content: The message body. Either a plain string
        shorthand (converted to a single ``input_text`` block on
        the way to ``run_turn``) or a list of content blocks,
        e.g. ``[{"type": "input_text", "text": "Hello"}]``.
    :param model: Agent name to invoke, e.g. ``"research-agent"``.
        Threaded into the synthesized :class:`CreateResponseRequest`
        so the initial / terminal :class:`ResponseObject` carries
        a real model identifier (no empty-string sentinel per the
        project's anti-pattern checklist).
    :param previous_response_id: Optional id of the prior response
        in the conversation thread. The scaffold today uses a
        match against the in-flight ``response_id`` as the
        injection signal; passing it here keeps that path open
        during the migration window. Spec long-term: any
        ``message`` while a turn is in flight is steering, no
        explicit correlation id needed — once all callers stop
        sending it, this field can be dropped.
    """

    type: Literal["message"]
    role: Literal["user"]
    content: str | list[dict[str, Any]]
    model: str
    previous_response_id: str | None = None
    # Allow runner-side passthrough of CreateResponseRequest fields
    # (instructions, conversation, reasoning, context_management,
    # tools, plus the ignored-but-tolerated controls). Forwarded
    # verbatim into the synthesized CreateResponseRequest by
    # :meth:`MessageEvent.to_create_request`.
    model_config = ConfigDict(extra="allow")

    def to_create_request(self) -> CreateResponseRequest:
        """
        Synthesize a :class:`CreateResponseRequest` from this event.

        Maps ``content`` → ``input`` and forwards every extra field
        verbatim. Used by the scaffold's ``/events`` handler to
        adapt the wire shape onto :meth:`HarnessApp._start_or_inject_turn`.

        :returns: A populated :class:`CreateResponseRequest` ready
            to hand off to :meth:`HarnessApp._start_or_inject_turn`.
        """
        # ``model_dump`` includes extras; rename ``content`` →
        # ``input`` and drop the discriminator + role fields the
        # legacy schema doesn't carry.
        payload = self.model_dump()
        action_origin_token = payload.pop("_omnigent_action_origin_token", None)
        action_origin_generation = payload.pop("_omnigent_action_context_generation", None)
        action_context_fingerprint = payload.pop("_omnigent_action_context_fingerprint", None)
        report_features = payload.pop("_omnigent_acp_report_features", None)
        action_features = payload.pop("_omnigent_acp_action_features", None)
        payload.pop("type", None)
        payload.pop("role", None)
        payload["input"] = payload.pop("content")
        request = CreateResponseRequest(**payload)
        if isinstance(action_origin_token, str) and action_origin_token:
            request._session_action_origin_token = action_origin_token
        if isinstance(action_origin_generation, str) and action_origin_generation:
            request._session_action_origin_generation = action_origin_generation
        if isinstance(action_context_fingerprint, str) and action_context_fingerprint:
            request._session_action_context_generation = action_context_fingerprint
        if isinstance(report_features, list) and all(
            isinstance(feature, str) for feature in report_features
        ):
            request._standard_acp_report_features = tuple(report_features)
        if isinstance(action_features, list) and all(
            isinstance(feature, str) for feature in action_features
        ):
            request._standard_acp_action_features = tuple(action_features)
        return request


class InterruptEvent(BaseModel):
    """
    Downward ``interrupt`` event — cancel the in-flight turn.

    The harness has at most one in-flight turn per conversation
    so no turn id is needed on the wire — the scaffold cancels
    every entry in :attr:`HarnessApp._in_flight`. If no turn is
    in flight, the route 404s — fail loud rather than silently
    accept a no-op cancel so a stray interrupt after a turn ended
    surfaces as an obvious operator error.

    :param type: Event discriminator; always ``"interrupt"``.
    """

    type: Literal["interrupt"]


class ToolResultEvent(BaseModel):
    """
    Downward ``tool_result`` event — deliver a server-dispatched
    tool's output back to the parked turn.

    Resolves the parked Future on the in-flight turn whose
    ``call_id`` matches. Stale ids silently no-op — submit
    results, apply what you can — so benign races (turn
    cancelled mid-event, Future already resolved on a different
    path) don't surface as hard failures upstream.

    :param type: Event discriminator; always ``"tool_result"``.
    :param call_id: The tool call id that this result corresponds
        to, e.g. ``"call_abc123"``. Must match an outstanding
        :meth:`TurnContext.dispatch_tool` Future.
    :param output: The tool's stringified output, e.g.
        ``'["paper1.pdf"]'``.
    """

    type: Literal["tool_result"]
    call_id: str
    output: str


class ApprovalEvent(BaseModel):
    """
    Downward ``approval`` event — reply to an outstanding
    elicitation.

    Resolves the parked Future on whichever in-flight turn
    registered the elicitation. Single-shot — an unknown
    ``elicitation_id`` is a hard 404 because the URL path's
    correlation token must match an outstanding elicitation.

    Wire shape mirrors MCP's ``elicitation/create`` response per
    Principle 8 — same ``action`` + ``content`` fields as
    :class:`ElicitationResult`.

    :param type: Event discriminator; always ``"approval"``.
    :param elicitation_id: Correlation id from the prior
        ``approval_required`` upward event,
        e.g. ``"elicit_abc123"``.
    :param action: User action per MCP semantics.
        ``"accept"`` = approved, ``"decline"`` = explicit refusal,
        ``"cancel"`` = dismissed.
    :param content: Optional form data when ``action == "accept"``
        and the prompt requested fields. Values restricted to JSON
        scalars and string lists, mirroring MCP.
    """

    type: Literal["approval"]
    elicitation_id: str
    action: Literal["accept", "decline", "cancel"]
    content: dict[str, str | int | float | bool | list[str] | None] | None = None

    def to_elicitation_result(self) -> ElicitationResult:
        """
        Adapt this event onto the legacy :class:`ElicitationResult`.

        :returns: An :class:`ElicitationResult` with the same
            ``action`` / ``content`` so
            :meth:`HarnessApp._resolve_elicitation` can resolve
            the parked Future.
        """
        return ElicitationResult(action=self.action, content=self.content)


class PolicyVerdictEvent(BaseModel):
    """
    Downward ``policy_verdict`` event — deliver a policy evaluation
    result back to the parked turn.

    Resolves the parked Future on whichever in-flight turn has a
    matching ``evaluation_id``. Stale ids silently no-op — same
    semantics as :class:`ToolResultEvent`.

    :param type: Event discriminator; always ``"policy_verdict"``.
    :param evaluation_id: Correlation id from the prior
        ``policy_evaluation.requested`` upward event,
        e.g. ``"poleval_abc123"``.
    :param action: Proto-style verdict string, e.g.
        ``"POLICY_ACTION_ALLOW"`` or ``"POLICY_ACTION_DENY"``.
    :param reason: Human-readable reason from the policy engine,
        e.g. ``"Denied by cost-limit policy"``. ``None`` on ALLOW.
    :param data: Optional data dict from content-rewriting
        policies. ``None`` when the policy does not rewrite.
    """

    type: Literal["policy_verdict"]
    evaluation_id: str
    action: str
    reason: str | None = None
    data: dict[str, Any] | None = None


class SessionReportAckRequest(BaseModel):
    """Acknowledge a contiguous, already-exposed report prefix."""

    subscriber_generation: str = Field(min_length=1, max_length=256)
    through_sequence: StrictInt = Field(ge=0)


@dataclass(frozen=True)
class SessionActionChild:
    """Trusted, runner-visible identity for one registered native child run."""

    native_epoch: str
    native_session_id: str
    run_id: str
    parent_tool_call_id: str
    background: bool
    child_scheduler_id: str | None = None
    child_tool_call_id: str | None = None
    approval_correlation_id: str | None = None
    request_id: str | int = ""
    request_invocation_id: int = 1
    origin_token: str = ""
    origin_agent: str | None = None
    origin_generation: str = ""
    root_native_session_id: str = ""
    parent_native_session_id: str | None = None
    association_source_id: str = ""
    context_generation: str = ""
    cwd_present: bool = False
    cwd: str | None = None
    additional_directories_present: bool = False
    additional_directories: tuple[str, ...] | None = ()

    def __post_init__(self) -> None:
        if (
            isinstance(self.request_invocation_id, bool)
            or not isinstance(self.request_invocation_id, int)
            or not 1 <= self.request_invocation_id <= 2**63 - 1
        ):
            raise ValueError("session action request invocation id is invalid")

    def to_wire(self) -> dict[str, Any]:
        return {
            "native_epoch": self.native_epoch,
            "native_session_id": self.native_session_id,
            "run_id": self.run_id,
            "parent_tool_call_id": self.parent_tool_call_id,
            "background": self.background,
            "child_scheduler_id": self.child_scheduler_id,
            "child_tool_call_id": self.child_tool_call_id,
            "approval_correlation_id": self.approval_correlation_id,
            "origin_token": self.origin_token,
            "origin_agent": self.origin_agent,
            "origin_generation": self.origin_generation,
            "root_native_session_id": self.root_native_session_id,
            "parent_native_session_id": self.parent_native_session_id,
            "association_source_id": self.association_source_id,
            "request_id": self.request_id,
            "request_invocation_id": self.request_invocation_id,
            "context_generation": self.context_generation,
            "cwd_present": self.cwd_present,
            "cwd": self.cwd,
            "additional_directories_present": self.additional_directories_present,
            "additional_directories": (
                list(self.additional_directories)
                if self.additional_directories is not None
                else None
            ),
        }


@dataclass
class _PendingSessionAction:
    action_id: str
    event_id: str
    generation: str
    child: dict[str, Any]
    event: dict[str, Any]
    expected_type: str
    expected_correlation: str
    future: asyncio.Future[BaseModel]


class SessionActionResultRequest(BaseModel):
    """Generation and event fence around one existing inbound harness event."""

    model_config = ConfigDict(extra="forbid")

    generation: str = Field(min_length=1, max_length=256)
    event_id: str = Field(min_length=1, max_length=512)
    result: dict[str, Any]


# Discriminated union of every downward event the harness accepts on
# ``POST /v1/sessions/{conversation_id}/events``. FastAPI / Pydantic
# v2 dispatches by the ``type`` field at request-validation time;
# unknown values raise 422 (fail-loud per
# ``designs/DESIGN_PRINCIPLES.md``).
InboundEventRequest = Annotated[
    MessageEvent | InterruptEvent | ToolResultEvent | ApprovalEvent | PolicyVerdictEvent,
    Field(discriminator="type"),
]


def _session_action_expected_result(event: HarnessStreamEvent) -> tuple[str, str]:
    """Return the only inbound result variant that may settle *event*."""
    if isinstance(event, PolicyEvaluationRequestEvent):
        return "policy_verdict", event.evaluation_id
    if isinstance(event, ElicitationRequestEvent):
        return "approval", event.elicitation_id
    if isinstance(event, OutputItemDoneEvent):
        item = event.item
        if (
            item.get("type") == "function_call"
            and item.get("status") == "action_required"
            and isinstance(item.get("call_id"), str)
        ):
            return "tool_result", item["call_id"]
    raise ValueError(f"unsupported child session action event {event.type!r}")


def _session_action_result_correlation(event: BaseModel) -> str:
    """Get the event's response correlation field after discriminated validation."""
    if isinstance(event, PolicyVerdictEvent):
        return event.evaluation_id
    if isinstance(event, ApprovalEvent):
        return event.elicitation_id
    if isinstance(event, ToolResultEvent):
        return event.call_id
    return ""


class TurnContext:
    """
    Per-turn interaction surface the scaffold hands to ``run_turn``.

    The subclass uses ``ctx`` to:

    - Push SSE events upstream (``ctx.emit(event)``).
    - Park on a tool dispatch (``await ctx.dispatch_tool(...)``).
    - Park on an elicitation (``await ctx.elicit(...)``).
    - Poll for cancellation (``ctx.cancelled.is_set()``).
    - Receive in-band steering / async-completion injections
      (``await ctx.next_injection(timeout=...)``).

    All state is per-turn — a fresh ``TurnContext`` is constructed
    for each fresh ``message`` event that starts a turn. Cross-turn
    state belongs on the harness subclass instance (Layer 1 in the
    in-memory state hierarchy).

    :param response_id: Server-allocated id for this turn,
        e.g. ``"resp_abc123"``. Surfaced on the SSE
        ``response.created`` envelope so Omnigent can correlate
        replays / heartbeat-event-seq tracking.
    :param session_id: Omnigent session validated by the API path, independent of telemetry.
    :param event_queue: The :class:`asyncio.Queue` the SSE
        streaming response reads from. ``ctx.emit`` puts
        events onto this queue; the streaming response
        formats and yields them.
    :param cancelled: Set by the cancel route handler. The
        subclass should poll this between LLM iterations / native
        tool calls so a cancel arriving mid-LLM-call interrupts
        promptly.
    """

    def __init__(
        self,
        response_id: str,
        event_queue: asyncio.Queue[HarnessStreamEvent | None],
        cancelled: asyncio.Event,
        session_id: str | None = None,
        *,
        session_action_child: SessionActionChild | None = None,
        session_action_handler: Callable[[HarnessStreamEvent], Any] | None = None,
        session_action_agent: str | None = None,
        session_action_origin_token: str | None = None,
        session_action_origin_generation: str | None = None,
        session_action_context_generation: str | None = None,
    ) -> None:
        self.session_id = session_id
        self.response_id = response_id
        self._event_queue = event_queue
        self.cancelled = cancelled
        # Layer 3 per-tool-dispatch state: ``call_id`` →
        # Future[ToolResult-output-string]. Populated by
        # ``dispatch_tool``; resolved by the ``tool_result``
        # /events handler.
        self._pending_tool_calls: dict[str, asyncio.Future[str]] = {}
        # Per-turn elicitation state: ``elicitation_id`` →
        # Future[ElicitationResult]. Populated by ``elicit``;
        # resolved by the ``approval`` /events handler.
        self._pending_elicitations: dict[str, asyncio.Future[ElicitationResult]] = {}
        # Human-approval waits in flight (elicitation replies, policy
        # verdicts). While > 0 heartbeats hold the idle window open so an
        # unanswered approval isn't failed as a wedged turn; the absolute
        # ceiling still applies.
        self._pending_human_waits = 0
        # Layer 3 per-policy-evaluation state:
        # ``evaluation_id`` → Future[PolicyVerdictPayload].
        # Populated by ``evaluate_policy``; resolved by the
        # ``policy_verdict`` /events handler.
        self._pending_policy_evaluations: dict[str, asyncio.Future[PolicyVerdictPayload]] = {}
        # In-band injections (steering, async completions) the
        # scaffold's ``message`` /events handler pushes here when
        # ``previous_response_id`` matches this turn. Subclass
        # consumes via ``next_injection``.
        self._injection_queue: asyncio.Queue[CreateResponseRequest] = asyncio.Queue()
        # Provider-reported token usage for this turn, set by the
        # subclass (e.g. ExecutorAdapter) when it observes a
        # TurnComplete with usage data. Read by _build_terminal_event
        # to populate the usage field on the response.completed SSE.
        # ``None`` when the inner executor does not report usage.
        self.provider_usage: dict[str, Any] | None = None
        # Idle-watchdog reset hook. ``_guarded_run_turn`` sets this to
        # push the per-turn idle deadline forward on each real progress
        # event so a long-but-active turn isn't killed mid-turn.
        # ``None`` disables it (watchdog off, or outside a guarded run).
        self._reset_idle_watchdog: Callable[[], None] | None = None
        # Idle-only keep-alive hook. Unlike ``_reset_idle_watchdog`` it
        # never extends the absolute ceiling; used by heartbeats while a
        # human wait is pending so the hard cap still bounds the turn.
        self._hold_idle_watchdog: Callable[[], None] | None = None
        self._has_progress = False
        # Detached ACP child requests get a short-lived context that is not
        # installed in HarnessApp._in_flight. Its bridge sends approval,
        # policy, and tool events through the private action channel.
        self._session_action_child = session_action_child
        self._session_action_handler = session_action_handler
        self._session_action_agent = session_action_agent
        self._session_action_origin_token = session_action_origin_token
        self._session_action_origin_generation = session_action_origin_generation
        self._session_action_context_generation = session_action_context_generation
        self._session_action_counter = 0

    def session_action_correlation(self, prefix: str) -> str:
        """Create a retry-stable event correlation id for a detached child request."""
        child = self._session_action_child
        if child is None:
            return f"{prefix}_{uuid.uuid4().hex[:32]}"
        self._session_action_counter += 1
        material = json.dumps(
            [
                child.native_epoch,
                child.native_session_id,
                child.run_id,
                child.request_id,
                child.request_invocation_id,
                self._session_action_counter,
                prefix,
            ],
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        suffix = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
        return f"{prefix}_{suffix}"

    async def _send_session_action(self, event: HarnessStreamEvent) -> BaseModel:
        """Publish a child-originated existing harness event and await its result."""
        if self._session_action_handler is None:
            raise RuntimeError("session action bridge is unavailable")
        return await self._session_action_handler(event)

    def emit(self, event: HarnessStreamEvent) -> None:
        """
        Push an SSE event upstream.

        Non-blocking. The event lands on the per-turn queue the
        streaming response is consuming. Producers leave
        ``sequence_number`` unset; the streaming wrapper
        assigns it monotonically.

        Output and side-effectful activity permanently prevent turn replay.
        Heartbeats, retry notices, and the pre-LLM policy handshake do not.

        :param event: A typed event from
            :data:`omnigent.server.schemas.ServerStreamEvent`,
            e.g.
            ``OutputTextDeltaEvent(type="response.output_text.delta",
            delta="hi")``.
        """
        # Treat any non-heartbeat event as progress and push the idle
        # watchdog deadline forward. Heartbeats are keep-alive, NOT
        # progress — letting them reset the deadline would defeat the
        # watchdog (a wedged turn's 15s heartbeats would keep it alive
        # forever). Exception: while a human approval (elicitation reply,
        # policy verdict) is pending, the turn can legitimately emit nothing
        # but heartbeats for as long as the human takes, so heartbeats hold
        # the idle window open then — via the idle-only hook, so the
        # absolute ceiling stays the hard cap even for an ignored approval.
        if not isinstance(event, HeartbeatEvent):
            if not isinstance(event, RetryEvent) and not (
                isinstance(event, PolicyEvaluationRequestEvent)
                and event.phase == "PHASE_LLM_REQUEST"
            ):
                self._has_progress = True
            if self._reset_idle_watchdog is not None:
                self._reset_idle_watchdog()
        elif self._pending_human_waits > 0 and self._hold_idle_watchdog is not None:
            self._hold_idle_watchdog()
        self._event_queue.put_nowait(event)

    async def dispatch_tool(
        self,
        call_id: str,
        name: str,
        arguments: str,
        agent: str,
        *,
        suppress_completed_mirror: bool = False,
    ) -> str:
        """
        Emit a server-dispatched tool call and park until the result.

        Surfaces a ``response.output_item.done`` carrying a
        ``function_call`` item with ``status: "action_required"``;
        the scaffold's ``tool_result`` /events handler resolves
        the parked Future when Omnigent delivers an event carrying the
        matching ``call_id``. See §How Omnigent resolves action_required
        tool calls in the design doc.

        :param call_id: Unique identifier for this tool call,
            e.g. ``"call_abc123"``. Omnigent echoes it back in the
            ``tool_result`` event so the scaffold can route the
            result.
        :param name: Tool name the LLM called, e.g.
            ``"search.web"``.
        :param arguments: JSON-encoded argument string, e.g.
            ``'{"q": "foo"}'``.
        :param agent: Agent name that invoked the tool — required
            on the function_call item per
            :class:`omnigent.entities.conversation.FunctionCallData`.
        :param suppress_completed_mirror: Let the native observation publish
            the completed call and output instead of emitting duplicates.
        :returns: The tool's output string from
            :class:`omnigent.server.schemas.ToolResult`.
        :raises asyncio.CancelledError: If the turn is cancelled
            (an ``interrupt`` event cancels every pending Future
            on the in-flight :class:`TurnContext`).
        """
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._pending_tool_calls[call_id] = future
        # Build the function_call item shape that AP's resolver
        # expects (per omnigent/entities/conversation.py:FunctionCallData
        # plus the API-shape common fields ``id``, ``type``,
        # ``status``).
        item: dict[str, Any] = {
            "id": f"fc_{uuid.uuid4().hex[:12]}",
            "type": "function_call",
            "status": "action_required",
            "name": name,
            "arguments": arguments,
            "call_id": call_id,
            "agent": agent,
        }
        if self._session_action_handler is not None:
            result = await self._send_session_action(
                OutputItemDoneEvent(type="response.output_item.done", item=item)
            )
            if not isinstance(result, ToolResultEvent) or result.call_id != call_id:
                raise RuntimeError("session action returned a mismatched tool result")
            return result.output
        self.emit(OutputItemDoneEvent(type="response.output_item.done", item=item))
        try:
            result = await future
            if not suppress_completed_mirror:
                item["status"] = "completed"
                self.emit(OutputItemDoneEvent(type="response.output_item.done", item=item))
                self.emit(
                    OutputItemDoneEvent(
                        type="response.output_item.done",
                        item={
                            "id": f"fco_{uuid.uuid4().hex[:12]}",
                            "type": "function_call_output",
                            "call_id": call_id,
                            "output": cap_tool_output(result),
                            "arguments": arguments,
                            "status": "completed",
                        },
                    )
                )
            return result
        finally:
            # Always remove — whether the Future resolved cleanly
            # or was cancelled, holding it in the dict would
            # leak memory across turns.
            self._pending_tool_calls.pop(call_id, None)

    async def elicit(
        self, elicitation_id: str, params: ElicitationRequestParams
    ) -> ElicitationResult:
        """
        Emit an elicitation request and park until the reply.

        Wire shape adopts MCP's ``elicitation/create`` per
        Principle 8. The scaffold's ``approval`` /events handler
        resolves the parked Future when upstream sends the reply.
        See §Elicitation in the design doc.

        Always emits ``response.elicitation_resolved`` on exit; idempotent.

        :param elicitation_id: Unique correlation id for this
            elicitation, e.g. ``"elicit_abc123"``. Must appear
            in the URL of the upstream reply.
        :param params: MCP-shaped params block (mode, message,
            requestedSchema / url, plus AP-specific extensions).
        :returns: The MCP-shaped
            :class:`ElicitationResult` reply.
        :raises asyncio.CancelledError: If the turn is cancelled.
        """
        future: asyncio.Future[ElicitationResult] = asyncio.get_running_loop().create_future()
        self._pending_elicitations[elicitation_id] = future
        self._pending_human_waits += 1
        if self._session_action_handler is not None:
            try:
                response = await self._send_session_action(
                    ElicitationRequestEvent(
                        type="response.elicitation_request",
                        elicitation_id=elicitation_id,
                        params=params,
                    )
                )
                if (
                    not isinstance(response, ApprovalEvent)
                    or response.elicitation_id != elicitation_id
                ):
                    raise RuntimeError("session action returned a mismatched approval")
                return response.to_elicitation_result()
            finally:
                self._pending_elicitations.pop(elicitation_id, None)
                self._pending_human_waits -= 1
        self.emit(
            ElicitationRequestEvent(
                type="response.elicitation_request",
                elicitation_id=elicitation_id,
                params=params,
            )
        )
        result: ElicitationResult | None = None
        try:
            result = await future
            return result
        finally:
            self._pending_elicitations.pop(elicitation_id, None)
            self._pending_human_waits -= 1
            self.emit(
                ElicitationResolvedEvent(
                    type="response.elicitation_resolved",
                    elicitation_id=elicitation_id,
                    action=result.action if result is not None else None,
                    reason="unanswered" if result is None else None,
                )
            )

    async def next_injection(self, timeout: float | None = None) -> CreateResponseRequest | None:
        """
        Wait for an in-band steering / async-completion injection.

        Blocks until either a ``message`` event with
        ``previous_response_id == self.response_id`` arrives (the
        scaffold's /events handler enqueues it here) or the timeout
        elapses. Subclasses that want to react to mid-turn
        injections (e.g., a sub-agent OOB return delivered as a
        ``function_call_output`` item) call this in a poll loop.

        :param timeout: Max seconds to wait, or ``None`` to wait
            indefinitely. Subclasses commonly use a short timeout
            (~1s) inside the LLM-tool loop so injections get
            picked up between iterations without blocking.
        :returns: The injected request body (whose ``input`` field
            carries the new items to fold into the running turn),
            or ``None`` on timeout.
        """
        try:
            if timeout is None:
                return await self._injection_queue.get()
            return await asyncio.wait_for(self._injection_queue.get(), timeout=timeout)
        except TimeoutError:
            return None

    def _complete_tool(self, call_id: str, output: str) -> bool:
        """
        Internal: resolve a pending tool-call Future from a
        ``tool_result`` event.

        :param call_id: The call id from the event body.
        :param output: The tool's result output string.
        :returns: ``True`` if a pending call was resolved, ``False``
            if the id didn't match anything outstanding. The
            ``tool_result`` handler ignores the False case (stale
            id = silent no-op).
        """
        future = self._pending_tool_calls.get(call_id)
        if future is None or future.done():
            return False
        future.set_result(output)
        return True

    def _complete_elicitation(self, elicitation_id: str, result: ElicitationResult) -> bool:
        """
        Internal: resolve a pending elicitation Future.

        :param elicitation_id: The id from the URL path.
        :param result: The MCP-shaped reply body.
        :returns: ``True`` if a pending elicitation was resolved,
            ``False`` if the id didn't match anything outstanding
            (the route returns 404 in that case).
        """
        future = self._pending_elicitations.get(elicitation_id)
        if future is None or future.done():
            return False
        future.set_result(result)
        return True

    async def evaluate_policy(
        self, evaluation_id: str, phase: str, data: dict[str, Any]
    ) -> PolicyVerdictPayload:
        """
        Emit a policy evaluation request and park until the verdict.

        Surfaces a ``policy_evaluation.requested`` SSE event; the
        runner intercepts it, evaluates the policy via the AP
        server's ``POST /sessions/{id}/policies/evaluate``, and
        posts the verdict back as a ``policy_verdict`` inbound
        event. The scaffold's event handler resolves the parked
        Future when the verdict arrives.

        :param evaluation_id: Unique correlation id, e.g.
            ``"poleval_abc123"``. The runner echoes it back in the
            verdict event.
        :param phase: Proto-style phase string, e.g.
            ``"PHASE_LLM_REQUEST"`` or ``"PHASE_LLM_RESPONSE"``.
        :param data: Event data dict for the policy engine, e.g.
            ``{"model": "gpt-4o", "messages_count": 42}``.
        :returns: The verdict payload from the Omnigent server.
        :raises asyncio.CancelledError: If the turn is cancelled.
        """
        future: asyncio.Future[PolicyVerdictPayload] = asyncio.get_running_loop().create_future()
        self._pending_policy_evaluations[evaluation_id] = future
        human_gated = phase in _HUMAN_GATED_POLICY_PHASES
        timeout_s = (
            _POLICY_EVAL_TIMEOUT_S
            if human_gated
            else min(_POLICY_EVAL_TIMEOUT_S, _UNGATED_POLICY_EVAL_TIMEOUT_S)
        )
        # Unlike elicit's unbounded park, this wait is already bounded by
        # the wait_for below; the counter only holds the idle window open,
        # and only for phases that can actually wait on a human.
        if human_gated:
            self._pending_human_waits += 1
        _logger.info("Policy evaluation %s requested (phase=%s)", evaluation_id, phase)
        if self._session_action_handler is not None:
            try:
                response = await self._send_session_action(
                    PolicyEvaluationRequestEvent(
                        type="policy_evaluation.requested",
                        evaluation_id=evaluation_id,
                        phase=phase,
                        data=data,
                    )
                )
                if (
                    not isinstance(response, PolicyVerdictEvent)
                    or response.evaluation_id != evaluation_id
                ):
                    raise RuntimeError("session action returned a mismatched policy verdict")
                return PolicyVerdictPayload(
                    action=response.action,
                    reason=response.reason,
                    data=response.data,
                )
            finally:
                self._pending_policy_evaluations.pop(evaluation_id, None)
                if human_gated:
                    self._pending_human_waits -= 1
        self.emit(
            PolicyEvaluationRequestEvent(
                type="policy_evaluation.requested",
                evaluation_id=evaluation_id,
                phase=phase,
                data=data,
            )
        )
        try:
            verdict = await asyncio.wait_for(future, timeout=timeout_s)
            _logger.info("Policy evaluation %s resolved: %s", evaluation_id, verdict.action)
            return verdict
        except TimeoutError:
            # Phase-aware default: advisory LLM phases and TOOL_RESULT (the
            # tool already ran) fail OPEN so a missing verdict never hangs the
            # turn, but TOOL_CALL is the authoritative gate for
            # connector-native tools and fails CLOSED.
            _fail_closed = phase in FAIL_CLOSED_PHASES
            _action = "POLICY_ACTION_DENY" if _fail_closed else "POLICY_ACTION_ALLOW"
            _logger.warning(
                "Policy evaluation %s timed out after %ds; defaulting to %s",
                evaluation_id,
                timeout_s,
                _action,
            )
            return PolicyVerdictPayload(
                action=_action,
                reason=(
                    f"Policy evaluation timed out; failing closed for {phase}."
                    if _fail_closed
                    else None
                ),
            )
        finally:
            self._pending_policy_evaluations.pop(evaluation_id, None)
            if human_gated:
                self._pending_human_waits -= 1

    def _complete_policy_evaluation(
        self, evaluation_id: str, verdict: PolicyVerdictPayload
    ) -> bool:
        """
        Internal: resolve a pending policy-evaluation Future.

        :param evaluation_id: Correlation id from the verdict event.
        :param verdict: The policy verdict payload.
        :returns: ``True`` if a pending evaluation was resolved,
            ``False`` if the id didn't match (stale = silent no-op).
        """
        future = self._pending_policy_evaluations.get(evaluation_id)
        if future is None or future.done():
            return False
        future.set_result(verdict)
        return True

    def _push_injection(self, request: CreateResponseRequest) -> None:
        """
        Internal: push an injection request onto the queue.

        :param request: The decoded ``message`` /events body whose
            ``previous_response_id`` matched this turn.
        """
        self._injection_queue.put_nowait(request)

    def _cancel_pending(self) -> None:
        """
        Internal: cancel every pending tool / elicitation Future.

        Called by the ``interrupt`` handler so subclass coroutines
        parked on ``dispatch_tool`` / ``elicit`` unblock with
        :class:`asyncio.CancelledError`. The subclass's
        ``run_turn`` then unwinds to its termination path.
        """
        for tool_future in self._pending_tool_calls.values():
            if not tool_future.done():
                tool_future.cancel()
        for elicitation_future in self._pending_elicitations.values():
            if not elicitation_future.done():
                elicitation_future.cancel()
        for policy_future in self._pending_policy_evaluations.values():
            if not policy_future.done():
                policy_future.cancel()


# Long enough for a harness's reason, short enough for one row of the /mcp panel.
_LIVE_MCP_DETAIL_MAX_CHARS = 300


def _live_mcp_body(statuses: list[LiveMcpServerStatus] | None) -> dict[str, Any]:
    """The wire body for a harness's MCP connections; ``None`` means none are running."""
    return {
        "running": statuses is not None,
        "servers": [
            {
                key: value
                for key, value in (
                    ("name", entry.name),
                    ("status", entry.status),
                    ("error", entry.error),
                    ("scope", entry.scope),
                )
                if value is not None
            }
            for entry in statuses or []
        ],
    }


async def _live_mcp_server_name(request: Request) -> str | None:
    """The non-empty ``name`` string in a JSON request body, else ``None``."""
    try:
        body = await request.json()
    except ValueError:
        return None
    name = body.get("name") if isinstance(body, dict) else None
    return name if isinstance(name, str) and name.strip() else None


def _live_mcp_detail(exc: BaseException) -> str:
    return str(exc).strip()[:_LIVE_MCP_DETAIL_MAX_CHARS]


def _live_mcp_failure(status_code: int, error: str) -> JSONResponse:
    return JSONResponse({"error": error}, status_code=status_code)


def _session_report_failure(exception: BaseException) -> JSONResponse:
    """Map report-channel failures to explicit private-control responses."""
    if isinstance(exception, SessionReportJournalVersionMismatch):
        return JSONResponse({"error": "report_journal_version_mismatch"}, status_code=409)
    if isinstance(exception, SessionReportJournalFenced):
        return JSONResponse({"error": "report_subscriber_replaced"}, status_code=409)
    if isinstance(exception, SessionReportJournalConflict):
        return JSONResponse({"error": "report_sequence_conflict"}, status_code=409)
    if isinstance(exception, SessionReportJournalUnavailable):
        return JSONResponse({"error": "report_journal_unavailable"}, status_code=404)
    if isinstance(exception, SessionReportJournalBusy):
        return JSONResponse({"error": "report_journal_busy"}, status_code=503)
    if isinstance(exception, SessionReportJournalCapacity):
        return JSONResponse({"error": "report_journal_capacity"}, status_code=507)
    if isinstance(exception, SessionReportJournalCorrupt):
        return JSONResponse({"error": "report_journal_corrupt"}, status_code=500)
    if isinstance(exception, ValueError):
        return JSONResponse({"error": "invalid_report_request"}, status_code=422)
    if isinstance(exception, SessionReportJournalError):
        return JSONResponse({"error": "report_journal_failure"}, status_code=500)
    return JSONResponse({"error": "report_channel_failure"}, status_code=500)


class HarnessApp:
    """
    Base class for harness wraps. Subclasses implement
    :meth:`run_turn`.

    Use the class as a context manager around a harness wrap's
    ``create_app()`` factory:

    .. code-block:: python

        class MyHarnessApp(HarnessApp):
            async def run_turn(self, request: CreateResponseRequest, ctx: TurnContext) -> None:
                ctx.emit(OutputTextDeltaEvent(type="response.output_text.delta", delta="hi"))


        def create_app() -> FastAPI:
            return MyHarnessApp().build()

    The scaffold owns:

    - The FastAPI app (built by :meth:`build`).
    - Per-turn :class:`TurnContext` allocation + lifecycle.
    - SSE streaming of events the subclass emits.
    - Heartbeat emission at :data:`_HEARTBEAT_INTERVAL_S` cadence.
    - Discriminated /events handler that resolves the right
      pending Futures on the right :class:`TurnContext` for
      ``tool_result``, ``approval``, and ``interrupt`` variants.
    - In-band injection routing: a ``message`` event with
      ``previous_response_id`` matching the in-flight turn
      enqueues onto that turn's injection queue rather than
      starting a new turn.
    - Graceful SIGTERM / SIGINT shutdown.

    The subclass owns:

    - :meth:`run_turn` — the LLM-tool loop, using ``ctx`` to
      interact upstream.
    - Layer 1 in-memory state (SDK / CLI process handle, last-known
      history for reconciliation) on the subclass instance.
    """

    def __init__(self) -> None:
        # In-flight turns keyed by ``response_id``. Single-entry in
        # practice (AP enforces one in-flight turn per conversation
        # at its REST boundary), but stored as a dict so the route
        # handlers can look up by id without assuming uniqueness.
        self._in_flight: dict[str, TurnContext] = {}
        # The currently active turn context, or ``None`` when idle.
        # Set under ``_lock`` when a turn starts, cleared
        # synchronously when the streaming generator finishes
        # (before async teardown). Used by sessions-native
        # injection: a message arriving while this is set is
        # steering, not a new turn.
        self._active_turn_ctx: TurnContext | None = None
        # Lock for ``_in_flight`` / ``_active_turn_ctx`` mutations —
        # route handlers run concurrently in FastAPI's event loop.
        self._lock = asyncio.Lock()
        self._action_lock = asyncio.Lock()
        self._session_actions: OrderedDict[str, _PendingSessionAction] = OrderedDict()
        self._session_action_event_index: dict[str, str] = {}
        # (event_id, expected type, correlation id, result dump); a None result
        # marks a cancelled action. Tombstones are retained for this harness
        # generation, and new actions fail closed when the bound is reached.
        self._session_action_tombstones: OrderedDict[
            str, tuple[str, str, str, dict[str, Any] | None]
        ] = OrderedDict()
        self._session_action_changed = asyncio.Event()
        self._session_action_pollers = 0
        self._bound_conversation_id: str | None = None
        # Set by graceful-shutdown signal handlers; checked by the
        # ``message`` /events handler to refuse new turns once
        # shutdown started.
        self._shutting_down = asyncio.Event()

    async def on_startup(self, conversation_id: str | None) -> None:
        """Subclass hook invoked after the runner binds the served conversation."""

    async def session_report_capabilities(self, conversation_id: str) -> dict[str, object] | None:
        """Return private report-channel capabilities, or ``None`` when unsupported."""
        return None

    async def read_session_reports(
        self,
        conversation_id: str,
        *,
        after_sequence: int,
        subscriber_generation: str,
        limit: int,
    ) -> SessionReportPage | None:
        """Read a durable report page, or ``None`` when the channel is unsupported."""
        return None

    async def acknowledge_session_reports(
        self,
        conversation_id: str,
        *,
        through_sequence: int,
        subscriber_generation: str,
    ) -> int | None:
        """Acknowledge a durable report prefix, or ``None`` when unsupported."""
        return None

    async def session_action_generation(self, conversation_id: str) -> str | None:
        """Return the active report generation when the private action channel is enabled."""
        return None

    async def cancel_session_actions(self) -> None:
        """Cancel parked actions while retaining bounded replay tombstones."""
        async with self._action_lock:
            for action_id, action in tuple(self._session_actions.items()):
                self._session_actions.pop(action_id, None)
                self._session_action_tombstones[action_id] = (
                    action.event_id,
                    action.expected_type,
                    action.expected_correlation,
                    None,
                )
                if not action.future.done():
                    action.future.cancel()
            self._session_action_changed.set()

    async def publish_session_action(
        self, ctx: TurnContext, event: HarnessStreamEvent
    ) -> BaseModel:
        """Expose one existing harness event and await its fenced inbound result."""
        child = ctx._session_action_child
        if child is None or ctx.session_id is None:
            raise RuntimeError("session action has no trusted child scope")
        generation = await self.session_action_generation(ctx.session_id)
        if generation is None:
            raise RuntimeError("session action channel is not enabled")
        expected_type, expected_correlation = _session_action_expected_result(event)
        event_dump = event.model_dump(mode="json", exclude_none=True)
        identity = child.to_wire()
        event_key = json.dumps(
            {
                "generation": generation,
                "child": identity,
                "request_id": child.request_id,
                "request_invocation_id": child.request_invocation_id,
                "event": event_dump,
            },
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        event_id = "evt_" + hashlib.sha256(event_key.encode("utf-8")).hexdigest()
        action_id = "act_" + hashlib.sha256(f"{generation}\0{event_id}".encode()).hexdigest()

        async with self._action_lock:
            existing_id = self._session_action_event_index.get(event_id)
            if existing_id is not None:
                existing = self._session_actions.get(existing_id)
                if existing is not None:
                    if (
                        existing.event != event_dump
                        or existing.child != identity
                        or existing.expected_type != expected_type
                        or existing.expected_correlation != expected_correlation
                    ):
                        raise RuntimeError("session action event identity was reused")
                    future = existing.future
                else:
                    tombstone = self._session_action_tombstones.get(existing_id)
                    if tombstone is None or tombstone[3] is None:
                        raise RuntimeError("session action was cancelled")
                    if tombstone[1] != expected_type or tombstone[2] != expected_correlation:
                        raise RuntimeError("session action event identity was reused")
                    return TypeAdapter(InboundEventRequest).validate_python(tombstone[3])
            else:
                if len(self._session_actions) >= _MAX_PENDING_SESSION_ACTIONS:
                    raise RuntimeError("pending session action capacity reached")
                if (
                    len(self._session_action_tombstones) + len(self._session_actions)
                    >= _MAX_SESSION_ACTION_TOMBSTONES
                ):
                    raise RuntimeError("session action retry-record capacity reached")
                future = asyncio.get_running_loop().create_future()
                pending = _PendingSessionAction(
                    action_id=action_id,
                    event_id=event_id,
                    generation=generation,
                    child=identity,
                    event=event_dump,
                    expected_type=expected_type,
                    expected_correlation=expected_correlation,
                    future=future,
                )
                self._session_actions[action_id] = pending
                self._session_action_event_index[event_id] = action_id
                self._session_action_changed.set()
        try:
            return await future
        except asyncio.CancelledError:
            await self._cancel_session_action(action_id)
            raise

    async def _cancel_session_action(self, action_id: str) -> None:
        async with self._action_lock:
            pending = self._session_actions.pop(action_id, None)
            if pending is None:
                return
            self._session_action_tombstones[action_id] = (
                pending.event_id,
                pending.expected_type,
                pending.expected_correlation,
                None,
            )
            if not pending.future.done():
                pending.future.cancel()
            self._session_action_changed.set()

    def make_session_action_context(
        self,
        child: SessionActionChild,
        *,
        agent: str | None = None,
        origin_generation: str | None = None,
    ) -> TurnContext:
        """Create an unregistered turn context whose waits use the action channel."""
        queue: asyncio.Queue[HarnessStreamEvent | None] = asyncio.Queue()
        ctx = TurnContext(
            response_id=f"action_{uuid.uuid4().hex}",
            event_queue=queue,
            cancelled=asyncio.Event(),
            session_id=self._bound_conversation_id,
            session_action_child=child,
            session_action_agent=agent,
            session_action_origin_token=child.origin_token,
            session_action_origin_generation=origin_generation,
            session_action_context_generation=child.context_generation,
        )
        ctx._session_action_handler = lambda event: self.publish_session_action(ctx, event)
        return ctx

    async def on_shutdown(self) -> None:
        """
        Subclass hook invoked during lifespan teardown.

        Called from ``_lifespan``'s ``finally`` block after
        in-flight turns have been cancelled but before the
        drain grace period. Subclasses (e.g.
        :class:`ExecutorAdapter`) override this to close inner
        executor resources, SDK sessions, and child processes
        that would otherwise leak on process exit.

        The base implementation is a no-op.
        """

    def supports_live_mcp_control(self) -> bool:
        """Whether this harness reports and reconnects its own MCP connections."""
        return False

    async def live_mcp_statuses(self) -> list[LiveMcpServerStatus] | None:
        """The harness's MCP connections, or ``None`` while it has none running.

        Only called when :meth:`supports_live_mcp_control` is true.
        """
        raise NotImplementedError

    async def reconnect_live_mcp_server(self, name: str) -> None:
        """Reconnect one of the harness's MCP servers.

        Only called when :meth:`supports_live_mcp_control` is true.

        :raises LiveMcpHarnessNotRunning: The harness has no MCP connections running.
        :raises LiveMcpServerUnknown: The harness reports no server named *name*.
        :raises LiveMcpReconnectRefused: The harness tried and gave its reason.
        """
        raise NotImplementedError

    async def run_turn(self, request: CreateResponseRequest, ctx: TurnContext) -> None:
        """
        Per-harness turn execution.

        Subclasses MUST override. The scaffold has already wired
        up the SSE response, allocated ``response_id``, and
        constructed ``ctx``. The subclass's job is to run the
        LLM-tool loop using ``ctx.emit`` for events,
        ``ctx.dispatch_tool`` / ``ctx.elicit`` for round-trips,
        ``ctx.cancelled`` for cancellation polling, and
        ``ctx.next_injection`` for steering / async completions.

        On normal completion, the scaffold emits a
        :class:`CompletedEvent` automatically once the subclass
        returns. Subclasses do NOT need to emit the terminal
        event themselves.

        :param request: The decoded
            :class:`CreateResponseRequest` body.
        :param ctx: Per-turn :class:`TurnContext` providing the
            interaction surface.
        """
        raise NotImplementedError

    def _build_error_detail(self, exception: BaseException) -> Any:
        """
        Translate a ``run_turn`` exception into an :class:`ErrorDetail`.

        Default implementation: uses the exception class name as
        the error code (e.g. ``"RuntimeError"``, ``"ValueError"``)
        and ``str(exception)`` as the message. AP's retryable-error
        allowlist at
        :data:`omnigent.runtime.harnesses._client_executor._RETRYABLE_HARNESS_ERROR_CODES`
        uses semantic names (``"rate_limit_exceeded"``,
        ``"timeout"``, etc.), so the default mapping makes every
        failure non-retryable — safe but coarse.

        Per-harness subclasses (e.g.
        :class:`omnigent.runtime.harnesses._executor_adapter.ExecutorAdapter`)
        override this to translate SDK-specific exception types
        onto the semantic allowlist so the retry-classification
        promise from §Error envelopes / step 5j actually fires
        for known retryable failures.

        :param exception: The exception :meth:`run_turn` raised.
        :returns: An :class:`ErrorDetail` whose ``code`` field
            ideally matches one of the contract-recognized codes
            so AP-side retry decisions can act on it.
        """
        from omnigent.server.schemas import ErrorDetail

        # P2.11: a turn-context desync (the inner generation outlived its turn)
        # carries the stable ``runner_turn_context_desync`` code. Surface it
        # verbatim — it is intentionally absent from AP's retryable-harness-
        # error allowlist, so the L2 classifier treats it as terminal instead
        # of retry-looping into the same wedge. Keyed off the exception's
        # ``code`` attribute so a harness-side raise (vs. the class name) maps
        # consistently.
        if getattr(exception, "code", None) == _TURN_CONTEXT_DESYNC_CODE:
            return ErrorDetail(code=_TURN_CONTEXT_DESYNC_CODE, message=str(exception))

        return ErrorDetail(code=type(exception).__name__, message=str(exception))

    def build(self) -> FastAPI:
        """
        Build the FastAPI app exposing the harness API subset.

        Mounts:

        - ``GET /health`` (liveness probe — at root, NOT /v1).
        - ``POST /v1/sessions/{conversation_id}/events`` —
          discriminated downward-event endpoint per
          ``designs/session_rearchitecture.md`` §3. The body's
          ``type`` field selects the variant (``message`` /
          ``interrupt`` / ``tool_result`` / ``approval``); FastAPI
          / Pydantic rejects unknown values with 422.

        Harness-specific shutdown runs from the FastAPI lifespan
        teardown path after uvicorn receives SIGTERM/SIGINT, so
        in-flight turns are cancelled gracefully without competing
        with uvicorn's process-level signal handlers.

        :returns: A configured :class:`FastAPI` instance ready to
            be served by ``omnigent.runtime.harnesses._runner``.
        """
        app = FastAPI(title="omnigent-harness", lifespan=self._lifespan)
        # Instrument the harness ASGI app so HTTP calls from the runner
        # (over the per-conversation Unix socket) continue the caller's
        # trace rather than rooting a fresh one.
        from omnigent.runtime import telemetry

        telemetry.instrument_fastapi_app(app)
        # FastAPI / Starlette types add_exception_handler narrowly
        # (Callable[[Request, Exception], ...]) — the OmnigentError
        # subclass annotation is what we actually want at runtime, but
        # the static type doesn't accept it. Cast at the boundary.
        app.add_exception_handler(OmnigentError, _handle_omnigent_error)  # type: ignore[arg-type]
        # Health probe lives at root (matches AP's app.py:125).
        app.add_api_route("/health", _health, methods=["GET"])
        app.include_router(self._build_v1_router(), prefix="/v1")
        return app

    def _check_auth(self, request: Request) -> Response | None:
        """
        Authenticate a ``/v1`` request with the per-spawn bearer token (S1).

        The harness control channel is a uid-isolated Unix socket on POSIX but a
        loopback-TCP listener on Windows, where any local process can connect.
        The ``/v1`` event endpoint starts turns, runs tools, and satisfies
        approval / policy verdicts, so it must not be reachable by an
        unauthenticated peer that merely learns the (non-secret)
        ``conversation_id``. The runner stashes the expected token on
        ``app.state.harness_auth_token``; the parent (process_manager) presents
        it as ``Authorization: Bearer <token>``. Comparison is constant-time.

        Checked in the event handler rather than via middleware so the SSE
        ``StreamingResponse`` and lifespan teardown are untouched
        (``BaseHTTPMiddleware`` interferes with both). ``/health`` is never
        gated. When no token is configured — the app was built outside the
        runner, e.g. a unit test calling ``create_app()`` directly — the gate is
        inert, preserving those callers' direct access.

        :param request: The inbound request, for ``Authorization`` + app.state.
        :returns: A 401 :class:`Response` to short-circuit on failure, or
            ``None`` to proceed.
        """
        expected = getattr(request.app.state, "harness_auth_token", None)
        if not expected:
            return None
        header = request.headers.get("Authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(token, expected):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        return None

    @contextlib.asynccontextmanager
    async def _lifespan(self, _app: FastAPI) -> AsyncIterator[None]:
        """
        FastAPI lifespan: cancel, close, and drain on exit.

        Uvicorn owns process-level SIGTERM/SIGINT handling. The
        scaffold intentionally does not install competing signal
        handlers here: doing so can prevent uvicorn from setting its
        own shutdown flag, leaving the harness runner alive after a
        plain ``SIGTERM``. Instead, uvicorn enters lifespan teardown
        and this ``finally`` block performs harness-specific cleanup:
        cancel in-flight Futures, invoke subclass shutdown hooks, and
        drain briefly for graceful completion.

        :param _app: The FastAPI app carrying the runner-bound
            conversation id.
        """
        try:
            self._bound_conversation_id = getattr(_app.state, "conversation_id", None)
            await self.on_startup(getattr(_app.state, "conversation_id", None))
            yield
        finally:
            self._on_shutdown_signal()
            await self.cancel_session_actions()
            await self.on_shutdown()
            await self._drain_for_shutdown()

    def _on_shutdown_signal(self) -> None:
        """
        Signal handler: kick off graceful shutdown.

        Marks the scaffold as shutting down (so new ``message``
        events get refused) and cancels every
        in-flight turn's pending Futures. The actual app exit
        happens after :meth:`_drain_for_shutdown` runs.
        """
        if self._shutting_down.is_set():
            # Second signal — escalate immediately. uvicorn's
            # default SIGTERM handler will take over.
            return
        self._shutting_down.set()
        for ctx in self._in_flight.values():
            ctx.cancelled.set()
            ctx._cancel_pending()

    async def _drain_for_shutdown(self) -> None:
        """
        Wait briefly for in-flight turns to finalize on shutdown.

        Bounded by :data:`_SHUTDOWN_GRACE_S`; if turns haven't
        cleared by then the process exits and AP's workflow
        handles the disconnect via the standard
        ``executor_died`` retry path.
        """
        deadline = time.monotonic() + _SHUTDOWN_GRACE_S
        while self._in_flight and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

    def _build_v1_router(self) -> APIRouter:
        """
        Build the ``/v1`` router with the route handlers.

        The router is bound to a bound method on ``self`` so the
        handler closes over the scaffold instance — no FastAPI
        dependency-injection plumbing needed.

        Mounts the session-keyed surface per
        ``designs/session_rearchitecture.md`` §3 "Endpoints":

        - ``POST /v1/sessions/{conversation_id}/events`` —
          downward events (``message``, ``interrupt``,
          ``tool_result``, ``approval``). The body's ``type``
          field discriminates the variant; FastAPI / Pydantic
          rejects unknown values with 422.

        The handler validates the path's ``conversation_id``
        against :attr:`fastapi.applications.FastAPI.state.conversation_id`
        (stashed by the runner per ``_runner.py``) and 404s on
        mismatch.

        :returns: A configured :class:`APIRouter`.
        """
        router = APIRouter()
        router.add_api_route(
            "/sessions/{conversation_id}/events",
            self._post_session_event,
            methods=["POST"],
            # MessageEvent variants stream a turn back as SSE
            # (StreamingResponse) while the other variants return
            # 204; declaring response_model=None disables OpenAPI
            # introspection that would otherwise reject the
            # heterogeneous return type.
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/mcp-status",
            self._get_live_mcp_status,
            methods=["GET"],
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/mcp-reconnect",
            self._post_live_mcp_reconnect,
            methods=["POST"],
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/reports/capabilities",
            self._get_session_report_capabilities,
            methods=["GET"],
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/reports",
            self._get_session_reports,
            methods=["GET"],
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/reports/ack",
            self._post_session_reports_ack,
            methods=["POST"],
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/actions",
            self._get_session_actions,
            methods=["GET"],
            response_model=None,
        )
        router.add_api_route(
            "/sessions/{conversation_id}/actions/{action_id}/result",
            self._post_session_action_result,
            methods=["POST"],
            response_model=None,
        )
        return router

    async def _session_actions_request_error(
        self, request: Request, conversation_id: str, generation: str
    ) -> Response | None:
        """Authenticate and bind an ephemeral action-channel request."""
        if not getattr(request.app.state, "harness_auth_token", None):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        denied = self._check_auth(request)
        if denied is not None:
            return denied
        self._check_conversation_id(request, conversation_id)
        try:
            active_generation = await self.session_action_generation(conversation_id)
        except (SessionReportJournalError, ValueError) as exc:
            return _session_report_failure(exc)
        if active_generation is None:
            return JSONResponse(
                {"error": "unsupported", "detail": "Session actions are not enabled."},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        if not hmac.compare_digest(active_generation, generation):
            return JSONResponse(
                {"error": "stale_generation", "detail": "Session action generation is stale."},
                status_code=status.HTTP_409_CONFLICT,
            )
        return None

    async def _get_session_actions(
        self,
        conversation_id: str,
        request: Request,
        generation: str = Query(min_length=1, max_length=256),
        wait_ms: int = Query(default=0, ge=0, le=_SESSION_ACTION_POLL_WAIT_MS),
    ) -> Response:
        """Return pending child actions, optionally waiting for new work."""
        refused = await self._session_actions_request_error(request, conversation_id, generation)
        if refused is not None:
            return refused
        if self._session_action_pollers >= _MAX_SESSION_ACTION_POLLS:
            return JSONResponse(
                {"error": "busy", "detail": "Too many concurrent action polls."},
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            )
        self._session_action_pollers += 1
        try:
            deadline = asyncio.get_running_loop().time() + wait_ms / 1000
            while not self._session_actions and asyncio.get_running_loop().time() < deadline:
                self._session_action_changed.clear()
                if self._session_actions:
                    break
                remaining = deadline - asyncio.get_running_loop().time()
                try:
                    await asyncio.wait_for(self._session_action_changed.wait(), timeout=remaining)
                except TimeoutError:
                    break
            actions = list(self._session_actions.values())[:_SESSION_ACTION_POLL_LIMIT]
            return JSONResponse(
                {
                    "protocol": _SESSION_ACTION_PROTOCOL,
                    "version": _SESSION_ACTION_VERSION,
                    "features": sorted(_SESSION_ACTION_FEATURES),
                    "generation": generation,
                    "actions": [
                        {
                            "action_id": action.action_id,
                            "event_id": action.event_id,
                            "child": action.child,
                            "event": action.event,
                        }
                        for action in actions
                    ],
                }
            )
        finally:
            self._session_action_pollers -= 1

    async def _post_session_action_result(
        self,
        conversation_id: str,
        action_id: str,
        body: SessionActionResultRequest,
        request: Request,
    ) -> Response:
        """Deliver one generation-fenced existing inbound event to its child request."""
        refused = await self._session_actions_request_error(
            request, conversation_id, body.generation
        )
        if refused is not None:
            return refused
        try:
            result = TypeAdapter(InboundEventRequest).validate_python(body.result)
        except Exception as exc:
            return JSONResponse(
                {"error": "invalid_result", "detail": str(exc)},
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )
        if not isinstance(result, (ApprovalEvent, PolicyVerdictEvent, ToolResultEvent)):
            return JSONResponse(
                {"error": "invalid_result", "detail": "Unsupported session action result type."},
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )
        result_dump = result.model_dump(mode="json", exclude_none=True)
        async with self._action_lock:
            pending = self._session_actions.get(action_id)
            if pending is None:
                tombstone = self._session_action_tombstones.get(action_id)
                if tombstone is None:
                    return JSONResponse(
                        {
                            "error": "action_not_found",
                            "detail": "Session action is no longer pending.",
                        },
                        status_code=status.HTTP_409_CONFLICT,
                    )
                expected_event_id, _expected_type, _expected_correlation, prior = tombstone
                if body.event_id != expected_event_id:
                    return JSONResponse(
                        {"error": "event_mismatch", "detail": "Action event id does not match."},
                        status_code=status.HTTP_409_CONFLICT,
                    )
                if prior is None:
                    return JSONResponse(
                        {"error": "action_cancelled", "detail": "Session action was cancelled."},
                        status_code=status.HTTP_409_CONFLICT,
                    )
                if prior != result_dump:
                    return JSONResponse(
                        {
                            "error": "conflicting_result",
                            "detail": "Action already has a different result.",
                        },
                        status_code=status.HTTP_409_CONFLICT,
                    )
                return JSONResponse({"accepted": True, "duplicate": True})

            if body.event_id != pending.event_id:
                return JSONResponse(
                    {"error": "event_mismatch", "detail": "Action event id does not match."},
                    status_code=status.HTTP_409_CONFLICT,
                )
            correlation = _session_action_result_correlation(result)
            if result.type != pending.expected_type or correlation != pending.expected_correlation:
                return JSONResponse(
                    {
                        "error": "result_mismatch",
                        "detail": "Result does not match the pending action.",
                    },
                    status_code=status.HTTP_409_CONFLICT,
                )
            if len(self._session_action_tombstones) >= _MAX_SESSION_ACTION_TOMBSTONES:
                return JSONResponse(
                    {"error": "capacity", "detail": "Session action retry records are full."},
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            self._session_actions.pop(action_id, None)
            self._session_action_tombstones[action_id] = (
                pending.event_id,
                pending.expected_type,
                pending.expected_correlation,
                result_dump,
            )
            if not pending.future.done():
                pending.future.set_result(result)
            self._session_action_changed.set()
        return JSONResponse({"accepted": True, "duplicate": False})

    def _session_reports_request_error(
        self, request: Request, conversation_id: str
    ) -> Response | None:
        """Authenticate and bind every private report-channel request."""
        # Unlike legacy harness routes, report data is private and these
        # endpoints must fail closed even when the runner did not configure
        # its per-spawn bearer token.
        if not getattr(request.app.state, "harness_auth_token", None):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        denied = self._check_auth(request)
        if denied is not None:
            return denied
        self._check_conversation_id(request, conversation_id)
        return None

    async def _get_session_report_capabilities(
        self, conversation_id: str, request: Request
    ) -> Response:
        """Return the negotiated private report protocol for this harness session."""
        refused = self._session_reports_request_error(request, conversation_id)
        if refused is not None:
            return refused
        try:
            capabilities = await self.session_report_capabilities(conversation_id)
        except (SessionReportJournalError, ValueError) as exc:
            return _session_report_failure(exc)
        if capabilities is None:
            return JSONResponse(
                {"error": "unsupported", "detail": "Session reports are not enabled."},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return JSONResponse(capabilities)

    async def _get_session_reports(
        self,
        conversation_id: str,
        request: Request,
        after: int = Query(default=0, ge=0),
        subscriber_generation: str = Query(min_length=1, max_length=256),
        limit: int = Query(default=100, ge=1, le=1000),
        wait_ms: int = Query(default=0, ge=0, le=25_000),
    ) -> Response:
        """Read ordered reports with an optional bounded long poll."""
        refused = self._session_reports_request_error(request, conversation_id)
        if refused is not None:
            return refused
        deadline = asyncio.get_running_loop().time() + wait_ms / 1000
        while True:
            try:
                page = await self.read_session_reports(
                    conversation_id,
                    after_sequence=after,
                    subscriber_generation=subscriber_generation,
                    limit=limit,
                )
            except (SessionReportJournalError, ValueError) as exc:
                return _session_report_failure(exc)
            if page is None:
                return JSONResponse(
                    {"error": "unsupported", "detail": "Session reports are not enabled."},
                    status_code=status.HTTP_404_NOT_FOUND,
                )
            if (
                page.records
                or (page.source_status is not None and page.source_status.state == "failed")
                or asyncio.get_running_loop().time() >= deadline
            ):
                break
            await asyncio.sleep(min(0.1, max(0.0, deadline - asyncio.get_running_loop().time())))
        return JSONResponse(
            {
                "protocol": "acp-session-reports",
                "version": 3,
                "features": list(SESSION_REPORT_FEATURES),
                "subscriber_generation": subscriber_generation,
                "records": [
                    {
                        "sequence": record.sequence,
                        "journal_id": record.journal_id,
                        "epoch_ordinal": record.epoch_ordinal,
                        "native_epoch": record.native_epoch,
                        "ingress_sequence": record.ingress_sequence,
                        "source_id": record.source_id,
                        "kind": record.kind,
                        "data": record.data,
                    }
                    for record in page.records
                ],
                "highest_sequence": page.highest_sequence,
                "exposed_through": page.exposed_through,
                "more": page.more,
                "source_status": (
                    None
                    if page.source_status is None
                    else {
                        "state": page.source_status.state,
                        "error": page.source_status.error,
                    }
                ),
            }
        )

    async def _post_session_reports_ack(
        self,
        conversation_id: str,
        body: SessionReportAckRequest,
        request: Request,
    ) -> Response:
        """Acknowledge a contiguous report prefix after durable forwarding."""
        refused = self._session_reports_request_error(request, conversation_id)
        if refused is not None:
            return refused
        try:
            acknowledged = await self.acknowledge_session_reports(
                conversation_id,
                through_sequence=body.through_sequence,
                subscriber_generation=body.subscriber_generation,
            )
        except (SessionReportJournalError, ValueError) as exc:
            return _session_report_failure(exc)
        if acknowledged is None:
            return JSONResponse(
                {"error": "unsupported", "detail": "Session reports are not enabled."},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return JSONResponse({"acknowledged_sequence": acknowledged})

    def _live_mcp_request_error(self, request: Request, conversation_id: str) -> Response | None:
        """Authorize a live-MCP request; refuse one this harness cannot answer."""
        denied = self._check_auth(request)
        if denied is not None:
            return denied
        self._check_conversation_id(request, conversation_id)
        if not self.supports_live_mcp_control():
            return JSONResponse(
                {"error": "unsupported", "detail": "This harness does not report MCP status."},
                status_code=status.HTTP_404_NOT_FOUND,
            )
        return None

    async def _get_live_mcp_status(self, conversation_id: str, request: Request) -> Response:
        """Handle ``GET /v1/sessions/{conversation_id}/mcp-status``."""
        refused = self._live_mcp_request_error(request, conversation_id)
        if refused is not None:
            return refused
        try:
            statuses = await self.live_mcp_statuses()
        except TimeoutError:
            return _live_mcp_failure(status.HTTP_504_GATEWAY_TIMEOUT, "mcp_status_timeout")
        except Exception:
            _logger.warning("Reading live MCP status failed", exc_info=True)
            return _live_mcp_failure(status.HTTP_502_BAD_GATEWAY, "mcp_status_failed")
        return JSONResponse(_live_mcp_body(statuses))

    async def _post_live_mcp_reconnect(self, conversation_id: str, request: Request) -> Response:
        """Handle ``POST /v1/sessions/{conversation_id}/mcp-reconnect`` with ``{"name": ...}``.

        Answers with the connections as they stand after the reconnect.
        """
        refused = self._live_mcp_request_error(request, conversation_id)
        if refused is not None:
            return refused
        server_name = await _live_mcp_server_name(request)
        if server_name is None:
            return _live_mcp_failure(status.HTTP_400_BAD_REQUEST, "invalid_request")
        reconnect_error: str | None = None
        try:
            await self.reconnect_live_mcp_server(server_name)
        except LiveMcpHarnessNotRunning:
            return _live_mcp_failure(status.HTTP_409_CONFLICT, "harness_not_running")
        except LiveMcpServerUnknown:
            return _live_mcp_failure(status.HTTP_404_NOT_FOUND, "mcp_server_unknown")
        except TimeoutError:
            return _live_mcp_failure(status.HTTP_504_GATEWAY_TIMEOUT, "mcp_reconnect_timeout")
        except LiveMcpReconnectRefused as exc:
            _logger.info("The harness refused to reconnect MCP server %r: %s", server_name, exc)
            reconnect_error = _live_mcp_detail(exc) or "Reconnect failed."
        except Exception:
            # Other failures can carry internal detail, so only the log gets it.
            _logger.warning("Reconnecting MCP server %r failed", server_name, exc_info=True)
            return _live_mcp_failure(status.HTTP_502_BAD_GATEWAY, "mcp_reconnect_failed")
        body: dict[str, Any]
        try:
            body = _live_mcp_body(await self.live_mcp_statuses())
        except Exception:
            _logger.warning("Reading live MCP status after reconnect failed", exc_info=True)
            # The harness is still running; only its status could not be read.
            body = {"running": True, "servers": [], "status_unavailable": True}
        if reconnect_error is not None:
            body["reconnect_error"] = reconnect_error
        return JSONResponse(body)

    def _check_conversation_id(self, request: Request, conversation_id: str) -> None:
        """
        Validate that a session-keyed URL targets this scaffold's
        conversation.

        The harness scaffold is per-conversation: a single
        subprocess serves exactly one ``conversation_id`` (set on
        ``app.state.conversation_id`` by the runner — see
        ``omnigent/runtime/harnesses/_runner.py``). Any
        session-keyed request whose path does not match is
        addressed at the wrong subprocess; failing with 404 is the
        same shape the Omnigent side uses for unknown conversations
        (per ``omnigent/server/routes/sessions.py``).

        Boot ordering: ``app.state.conversation_id`` is set in
        ``_runner._load_harness_app`` AFTER the FastAPI app is
        constructed but BEFORE uvicorn starts serving requests, so
        any handler that runs has guaranteed access to it. A
        missing attribute means the scaffold is being driven by a
        custom embedder that skipped that step — fail loud with
        500 rather than silently accept any conversation_id.

        :param request: The FastAPI request, used to read
            ``app.state.conversation_id``.
        :param conversation_id: The id from the URL path.
        :raises OmnigentError: 500 if ``app.state.conversation_id``
            is unset (misconfiguration); 404 if the path id does
            not match the scaffold's bound id.
        """
        bound = getattr(request.app.state, "conversation_id", None)
        if bound is None:
            raise OmnigentError(
                "harness scaffold has no conversation_id bound on app.state — "
                "the runner must set app.state.conversation_id before serving",
                code=ErrorCode.INTERNAL_ERROR,
            )
        if bound != conversation_id:
            raise OmnigentError(
                f"conversation {conversation_id!r} not served by this harness "
                f"(bound to {bound!r})",
                code=ErrorCode.NOT_FOUND,
            )

    async def _post_session_event(
        self,
        conversation_id: str,
        body: InboundEventRequest,
        request: Request,
    ) -> StreamingResponse | Response:
        """
        Handle ``POST /v1/sessions/{conversation_id}/events``.

        Single discriminated entry point for every downward event
        (``message``, ``interrupt``, ``tool_result``, ``approval``)
        per ``designs/session_rearchitecture.md`` §3. After
        validating the path's ``conversation_id`` matches the
        scaffold's bound conversation, dispatches by ``body.type``
        onto the matching per-action implementation.

        Dispatch table:

        - :class:`MessageEvent` → :meth:`_start_or_inject_turn`
          after adapting to :class:`CreateResponseRequest`
          (streaming SSE for new turns, 204 for in-band
          injections).
        - :class:`InterruptEvent` → cancel every in-flight turn
          (single-entry in practice; 404 if none in flight).
        - :class:`ToolResultEvent` → resolve the parked Future
          on whichever in-flight turn has the matching
          ``call_id``; stale ids silently no-op.
        - :class:`ApprovalEvent` → :meth:`_resolve_elicitation`
          after adapting to :class:`ElicitationResult` (404 on
          unknown elicitation_id — single-shot correlation).

        Unknown ``type`` values fail at request validation with
        422 (Pydantic discriminator), per ``designs/DESIGN_PRINCIPLES.md``
        fail-loud.

        :param conversation_id: AP-allocated conversation id from
            the URL, e.g. ``"conv_abc123"``. Must match
            ``app.state.conversation_id``.
        :param body: Decoded discriminated-union event body.
        :param request: The FastAPI request, used to validate
            ``conversation_id``.
        :returns: A :class:`StreamingResponse` for a fresh
            ``message`` event that starts a turn, or a 204
            :class:`Response` for every other case (injection,
            interrupt, tool_result, approval).
        :raises OmnigentError: 404 on conversation_id mismatch,
            unknown elicitation_id, or interrupt with no
            in-flight turn; 503 on shutdown for fresh turns.
        """
        denied = self._check_auth(request)
        if denied is not None:
            return denied
        self._check_conversation_id(request, conversation_id)
        if isinstance(body, MessageEvent):
            return await self._start_or_inject_turn(
                body.to_create_request(), session_id=conversation_id
            )
        if isinstance(body, InterruptEvent):
            return await self._handle_interrupt_event()
        if isinstance(body, ToolResultEvent):
            return await self._handle_tool_result_event(body)
        if isinstance(body, ApprovalEvent):
            return await self._resolve_elicitation(
                body.elicitation_id, body.to_elicitation_result()
            )
        if isinstance(body, PolicyVerdictEvent):
            return await self._handle_policy_verdict_event(body)
        # Pydantic's discriminated-union validator should reject
        # unknown variants before we reach this branch; if it ever
        # falls through, fail loud rather than silently no-op.
        raise OmnigentError(
            f"unsupported inbound event type {type(body).__name__!r}",
            code=ErrorCode.INVALID_INPUT,
        )

    async def _handle_interrupt_event(self) -> Response:
        """
        Apply an :class:`InterruptEvent` to the in-flight turn.

        The harness has at most one in-flight turn per conversation
        in practice, but the registry is a dict so this iterates
        defensively. If no turn is in flight, 404s — fail loud
        rather than silently no-op so a stray interrupt arriving
        after a turn ended surfaces as an obvious operator error.

        :returns: 204 on success.
        :raises OmnigentError: 404 if no turn is in flight.
        """
        if not self._in_flight:
            raise OmnigentError(
                "no in-flight turn to interrupt",
                code=ErrorCode.NOT_FOUND,
            )
        for ctx in self._in_flight.values():
            ctx.cancelled.set()
            ctx._cancel_pending()
        # Drop the cancelled turn as the inject target so the next message
        # starts a fresh turn (rebuilds with the marker), not into the dying one
        # — otherwise it resumes the abandoned turn and the agent runs one behind.
        async with self._lock:
            self._active_turn_ctx = None
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def _handle_tool_result_event(self, body: ToolResultEvent) -> Response:
        """
        Apply a :class:`ToolResultEvent` to whichever in-flight
        turn has a matching parked tool-call Future.

        Loose-by-default semantics — stale ``call_id`` entries
        silently no-op. Benign races (turn cancelled mid-event,
        Future already resolved on a different path) MUST NOT
        surface as hard failures upstream; the streaming response
        is the source of truth for whether the result actually
        landed.

        :param body: The decoded :class:`ToolResultEvent`.
        :returns: 204 No Content.
        """
        for ctx in self._in_flight.values():
            if ctx._complete_tool(body.call_id, body.output):
                break
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def _handle_policy_verdict_event(self, body: PolicyVerdictEvent) -> Response:
        """
        Apply a :class:`PolicyVerdictEvent` to whichever in-flight
        turn has a matching parked policy-evaluation Future.

        Loose semantics — stale ``evaluation_id`` entries silently
        no-op, same as :meth:`_handle_tool_result_event`.

        :param body: The decoded :class:`PolicyVerdictEvent`.
        :returns: 204 No Content.
        """
        verdict = PolicyVerdictPayload(
            action=body.action,
            reason=body.reason,
            data=body.data,
        )
        for ctx in self._in_flight.values():
            if ctx._complete_policy_evaluation(body.evaluation_id, verdict):
                break
        else:
            # A dropped verdict leaves its turn parked until the eval timeout.
            _logger.warning(
                "Policy verdict %s matched no pending evaluation (%d in-flight turns)",
                body.evaluation_id,
                len(self._in_flight),
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def _start_or_inject_turn(
        self, request: CreateResponseRequest, *, session_id: str | None = None
    ) -> StreamingResponse | Response:
        """
        Start a new turn or inject into the in-flight one.

        Invoked by :meth:`_post_session_event` for ``message``
        events. Three cases:

        1. ``previous_response_id`` matches an in-flight turn →
           in-band injection: enqueue the request body on that
           turn's injection queue, return 204.
        2. A runner steer (``injection_id`` set) with no live turn to
           inject into → refuse with 409; the runner replays it as its
           continuation turn.
        3. Scaffold is shutting down → refuse with 503.
        4. Otherwise → start a new turn: allocate ``response_id``,
           build a :class:`TurnContext`, register it in
           ``_in_flight``, and return a streaming SSE response
           that runs ``run_turn`` to completion.

        :param request: The decoded request body.
        :returns: Either a :class:`StreamingResponse` for the new
            turn or a 204 :class:`Response` for an in-band
            injection.
        :raises OmnigentError: 409 for a steer with no live turn; 503 on
            shutdown.
        """
        if (
            request.previous_response_id is not None
            and request.previous_response_id in self._in_flight
            and not self._in_flight[request.previous_response_id].cancelled.is_set()
        ):
            ctx = self._in_flight[request.previous_response_id]
            ctx._push_injection(request)
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        async with self._lock:
            # Sessions-native steering: no previous_response_id on
            # the wire, but a turn is actively streaming. Inject.
            # Serialized under _lock so two concurrent requests
            # can't both pass the guard before either sets
            # _active_turn_ctx.
            if (
                request.previous_response_id is None
                and self._active_turn_ctx is not None
                and not self._active_turn_ctx.cancelled.is_set()
            ):
                self._active_turn_ctx._push_injection(request)
                return Response(status_code=status.HTTP_204_NO_CONTENT)

            if request.injection_id is not None:
                # The steered turn already ended or was interrupted. The runner
                # keeps the message buffered for its continuation turn, so a turn
                # started here would run unseen and make that continuation a 204.
                raise OmnigentError(
                    "no active turn to inject into",
                    code=ErrorCode.CONFLICT,
                )

            if self._shutting_down.is_set():
                raise OmnigentError(
                    "harness is shutting down; refusing new turn",
                    code=ErrorCode.CONFLICT,
                )

            model = request.model
            if model is None:
                raise OmnigentError(
                    "model is required when starting a new turn",
                    code=ErrorCode.INVALID_INPUT,
                )

            response_id = f"resp_{uuid.uuid4().hex[:24]}"
            event_queue: asyncio.Queue[HarnessStreamEvent | None] = asyncio.Queue()
            cancelled = asyncio.Event()
            ctx = TurnContext(
                response_id=response_id,
                event_queue=event_queue,
                cancelled=cancelled,
                session_id=session_id,
                session_action_origin_token=request._session_action_origin_token,
                session_action_origin_generation=request._session_action_origin_generation,
                session_action_context_generation=request._session_action_context_generation,
            )
            self._in_flight[response_id] = ctx
            self._active_turn_ctx = ctx

        return StreamingResponse(
            self._stream_turn(request, ctx, model),
            media_type="text/event-stream",
        )

    async def _stream_turn(
        self, request: CreateResponseRequest, ctx: TurnContext, model: str
    ) -> AsyncIterator[bytes]:
        """
        Drive ``run_turn`` and yield SSE-formatted events.

        Three phases:

        1. Emit the initial response.created + response.in_progress
           frames carrying the freshly-allocated response_id.
        2. Spawn ``run_turn`` + heartbeat as concurrent tasks;
           consume the event queue, assigning monotonic sequence
           numbers, and yield each event as an SSE frame.
        3. On run_turn termination (sentinel from
           ``_guarded_run_turn``), emit the terminal event
           (completed / failed / cancelled) and tear down both
           tasks + the in-flight registry entry.

        :param request: The request body to pass to ``run_turn``.
        :param ctx: The per-turn :class:`TurnContext`.
        :yields: SSE-formatted ``bytes`` ready to ship over the
            HTTP response.
        """
        sequence = 0
        run_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        try:
            for initial_event in self._initial_envelope_events(
                ctx, model=model, start_seq=sequence
            ):
                yield _format_sse_event(initial_event)
                sequence += 1

            run_task = asyncio.create_task(
                self._guarded_run_turn(request, ctx),
                name=f"harness-run-turn:{ctx.response_id}",
            )
            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(ctx),
                name=f"harness-heartbeat:{ctx.response_id}",
            )
            # Track the sequence_number of the last non-heartbeat
            # event emitted on this stream so we can stamp the
            # heartbeat's ``last_event_seq`` field at yield time.
            # Per the contract (§Heartbeats), heartbeats carry the
            # *previous* user-visible event's sequence number so
            # consumers can detect dropped events. ``None`` until
            # the first non-heartbeat event lands; the initial
            # response.created / response.in_progress envelope
            # events count, so this populates immediately.
            last_event_seq: int | None = sequence - 1 if sequence > 0 else None
            while True:
                event = await ctx._event_queue.get()
                if event is None:
                    # Sentinel pushed by ``_guarded_run_turn`` when
                    # ``run_turn`` returns; emit terminal event
                    # below.
                    break
                event.sequence_number = sequence
                if isinstance(event, HeartbeatEvent):
                    # Stamp timing metadata at emit time, not
                    # construction time, so ``server_time`` reflects
                    # when the event actually leaves the queue and
                    # ``last_event_seq`` reflects the actual stream
                    # state at that moment.
                    event.server_time = _utc_now_iso()
                    event.last_event_seq = last_event_seq
                else:
                    last_event_seq = sequence
                sequence += 1
                yield _format_sse_event(event)
            terminal = await self._build_terminal_event(
                ctx, model=model, run_task=run_task, sequence=sequence
            )
            # Clear before yielding the terminal event so the next
            # request (continuation turn) sees _active_turn_ctx as
            # None and starts a new turn instead of injecting into
            # this completed one.
            async with self._lock:
                if self._active_turn_ctx is ctx:
                    self._active_turn_ctx = None
            yield _format_sse_event(terminal)
        finally:
            await self._teardown_turn(ctx, run_task, heartbeat_task)
            if run_task is not None:
                _freeze_gc_if_armed(run_task, ctx)

    def _initial_envelope_events(
        self, ctx: TurnContext, model: str, start_seq: int
    ) -> list[ServerStreamEvent]:
        """
        Build the response.created + response.in_progress events
        emitted at the start of every turn.

        Matches AP's existing streaming contract — both events
        carry the freshly-allocated :class:`ResponseObject` so
        consumers can correlate subsequent PATCH / cancel /
        elicitation calls back to this turn via the response_id.

        :param ctx: The per-turn context (provides ``response_id``).
        :param model: The agent name from the incoming
            :class:`CreateResponseRequest.model`, e.g.
            ``"research-agent"``. Required because
            :class:`ResponseObject` requires a real ``model`` value
            (no empty-string sentinels per the project's
            anti-pattern checklist).
        :param start_seq: Sequence number to assign to the first
            event; the second is ``start_seq + 1``.
        :returns: A two-event list:
            ``[CreatedEvent, InProgressEvent]``.
        """
        initial_response = ResponseObject(
            id=ctx.response_id,
            status="queued",
            model=model,
            created_at=int(time.time()),
        )
        in_progress_response = initial_response.model_copy(update={"status": "in_progress"})
        return [
            CreatedEvent(
                type="response.created",
                response=initial_response,
                sequence_number=start_seq,
            ),
            InProgressEvent(
                type="response.in_progress",
                response=in_progress_response,
                sequence_number=start_seq + 1,
            ),
        ]

    async def _teardown_turn(
        self,
        ctx: TurnContext,
        run_task: asyncio.Task[None] | None,
        heartbeat_task: asyncio.Task[None] | None,
    ) -> None:
        """
        Cancel the heartbeat + (defensively) the run task, then
        unregister the turn from ``_in_flight``.

        Always called from ``_stream_turn``'s finally block —
        guarantees no asyncio task leaks per turn even on
        cancellation / exception paths.

        :param ctx: The per-turn context (used for the response_id
            registry key).
        :param run_task: The completed (or to-be-cancelled) run_turn task,
            or ``None`` if the stream closed during its initial envelope.
        :param heartbeat_task: The heartbeat emitter task, if started.
        """
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
        if run_task is not None and not run_task.done():
            # Defensive: run_turn might still be parked on a
            # Future the cleanup didn't release. Cancel here as a
            # last resort so the task doesn't leak.
            # ``CancelledError`` is the expected exit; don't
            # blanket-suppress unrelated exceptions because a
            # crashed run_turn at this stage indicates a real bug
            # worth surfacing in the logs.
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task
        async with self._lock:
            self._in_flight.pop(ctx.response_id, None)
            if self._active_turn_ctx is ctx:
                self._active_turn_ctx = None

    async def _guarded_run_turn(self, request: CreateResponseRequest, ctx: TurnContext) -> None:
        """
        Wrap ``run_turn`` so its termination always pushes the
        sentinel onto the event queue.

        Without this, an exception or early return in ``run_turn``
        would leave the streaming loop blocked on
        ``event_queue.get()`` forever.

        Enforces two per-turn watchdogs, whichever trips first:

        - IDLE (:data:`_TURN_IDLE_TIMEOUT_S`): each non-heartbeat
          ``ctx.emit`` reschedules its deadline via the
          ``_reset_idle_watchdog`` hook, so a turn that keeps making
          progress (an orchestrator running tests + build + many tool
          calls) is never killed — only one that emits nothing for the
          whole window. This replaces the prior fixed *cumulative* cap,
          which guillotined long-but-healthy turns mid-stream.
        - ABSOLUTE (:data:`_TURN_ABSOLUTE_TIMEOUT_S`): a ceiling on total
          duration that real progress extends: each non-heartbeat emit
          pushes the deadline to at least one idle window past now, so an
          actively-progressing turn is never killed merely for running
          long. A turn that outlives the ceiling and then stalls dies via
          the idle watchdog; only with the idle watchdog disabled does
          this act as a strict wall-clock cap.

        Retry a wedged invocation or confirmed transport loss only before
        progress and after the subclass confirms executor cleanup.
        Replaying the original request after progress would duplicate output
        and tool effects. Recovery is bounded by the retry count and the
        remaining absolute budget; it never extends that budget.

        :param request: Forwarded to ``run_turn``.
        :param ctx: Forwarded to ``run_turn``.
        """
        idle_timeout = _TURN_IDLE_TIMEOUT_S
        absolute_timeout = _TURN_ABSOLUTE_TIMEOUT_S
        # ``asyncio.timeout(None)`` is a no-op, so ``<= 0`` disables each.
        absolute_wd = asyncio.timeout(absolute_timeout if absolute_timeout > 0 else None)
        attempts = 1 + max(0, _WEDGED_TURN_RECOVERY_RETRIES)
        idle_wd: asyncio.Timeout | None = None
        attempt = 0
        loop = asyncio.get_running_loop()
        try:
            # Absolute outer (spans every recovery attempt), idle inner
            # (fresh per attempt): ``.expired()`` on each tells which
            # ceiling tripped so the error message is accurate.
            async with absolute_wd:
                for attempt in range(attempts):
                    idle_wd = asyncio.timeout(idle_timeout if idle_timeout > 0 else None)
                    if idle_timeout > 0:

                        def _reset(idle_wd: asyncio.Timeout = idle_wd) -> None:
                            # Push the idle deadline ``idle_timeout`` s past
                            # now. Called from ``ctx.emit`` during
                            # ``run_turn`` (inside the active context).
                            # ``expired()`` guards a late emit racing a
                            # just-fired timeout: rescheduling an
                            # expiring/expired ``asyncio.Timeout`` raises
                            # RuntimeError out of ``emit``.
                            now = loop.time()
                            if not idle_wd.expired():
                                idle_wd.reschedule(now + idle_timeout)
                            # Real progress also extends the absolute ceiling
                            # to at least one idle window past now, so an
                            # actively-emitting turn is never guillotined
                            # mid-work for total duration alone. A turn that
                            # outlives the original ceiling and then stalls
                            # is failed by the idle watchdog one window later.
                            absolute_deadline = absolute_wd.when()
                            if (
                                not absolute_wd.expired()
                                and absolute_deadline is not None
                                and absolute_deadline < now + idle_timeout
                            ):
                                absolute_wd.reschedule(now + idle_timeout)

                        def _hold_idle(idle_wd: asyncio.Timeout = idle_wd) -> None:
                            # Keep-alive during a pending human wait: push
                            # ONLY the idle deadline, never the absolute
                            # ceiling — an approval that is never answered
                            # must still terminate at the hard cap.
                            if not idle_wd.expired():
                                idle_wd.reschedule(loop.time() + idle_timeout)

                        ctx._reset_idle_watchdog = _reset
                        ctx._hold_idle_watchdog = _hold_idle
                    try:
                        async with idle_wd:
                            await self.run_turn(request, ctx)
                        return
                    except (TimeoutError, HarnessTransportClosedError) as exc:
                        transport_closed = isinstance(exc, HarnessTransportClosedError)
                        if transport_closed and not exc.replay_safe:
                            raise
                        if not transport_closed and not idle_wd.expired():
                            # An inner ``run_turn`` TimeoutError (not the
                            # watchdog); pass it through unchanged.
                            raise
                        # No retry when the absolute ceiling leaves less
                        # than one idle window: the retry could not even
                        # wedge again before the hard cap kills it, so a
                        # post-ceiling stall keeps dying promptly via the
                        # idle error instead of bouncing into the cap.
                        absolute_deadline = absolute_wd.when()
                        out_of_absolute_budget = absolute_wd.expired() or (
                            absolute_deadline is not None
                            and absolute_deadline - loop.time() < idle_timeout
                        )
                        last_attempt = attempt + 1 >= attempts
                        if (
                            last_attempt
                            or out_of_absolute_budget
                            or ctx.cancelled.is_set()
                            or ctx._has_progress
                        ):
                            if transport_closed:
                                raise
                            raise self._idle_watchdog_error(ctx, idle_timeout, attempt) from exc
                        # Cleanup and retry notices must not extend the absolute ceiling.
                        ctx._reset_idle_watchdog = None
                        ctx._hold_idle_watchdog = None
                        cleanup_complete = await self._prepare_turn_retry()
                        absolute_deadline = absolute_wd.when()
                        if (
                            not cleanup_complete
                            or ctx.cancelled.is_set()
                            or ctx._has_progress
                            or absolute_wd.expired()
                            or (
                                absolute_deadline is not None
                                and absolute_deadline - loop.time() < idle_timeout
                            )
                        ):
                            if transport_closed:
                                raise
                            raise self._idle_watchdog_error(ctx, idle_timeout, attempt) from exc
                        if transport_closed:
                            retry_message = (
                                f"native harness transport closed before progress ({exc}); "
                                "the stopped invocation is being retried"
                            )
                        else:
                            retry_message = (
                                f"turn made no progress for {idle_timeout:.0f}s "
                                "(likely a wedged LLM or tool call); the wedged call "
                                "was abandoned and the turn is being retried"
                            )
                        _logger.warning(
                            "%s for %s (attempt %d/%d)",
                            retry_message,
                            ctx.response_id,
                            attempt + 2,
                            attempts,
                        )
                        ctx.emit(
                            RetryEvent(
                                type="response.retry",
                                source="llm",
                                attempt=attempt + 2,
                                max_attempts=attempts,
                                delay_seconds=0.0,
                                error=RetryErrorDetail(
                                    code="connection_error" if transport_closed else "timeout",
                                    message=retry_message,
                                ),
                            )
                        )
                    finally:
                        # Detach the hooks before this attempt's timeout
                        # context unwinds so a stray late ``emit`` can't
                        # reschedule a finished timeout.
                        ctx._reset_idle_watchdog = None
                        ctx._hold_idle_watchdog = None
        except TimeoutError as exc:
            # Prefer the idle cause when both timers fire or cleanup reaches
            # the absolute ceiling after an idle expiry.
            if idle_wd is not None and idle_wd.expired():
                raise self._idle_watchdog_error(ctx, idle_timeout, attempt) from exc
            if absolute_wd.expired():
                _logger.warning(
                    "run_turn for %s exceeded the %.0fs absolute turn ceiling; "
                    "marking the turn failed",
                    ctx.response_id,
                    absolute_timeout,
                )
                raise RuntimeError(
                    f"turn exceeded the {absolute_timeout:.0f}s harness absolute watchdog "
                    f"(total turn duration cap)"
                ) from exc
            # Neither ceiling tripped — an inner ``run_turn`` TimeoutError;
            # pass it through unchanged.
            raise
        except asyncio.CancelledError:
            # Re-raised so the streaming-side ``run_task.exception()``
            # check sees it; the terminal event handling
            # downgrades a cancelled run_turn to a
            # ``response.cancelled`` event.
            raise
        finally:
            # Sentinel that tells ``_stream_turn`` to stop reading
            # the queue and emit the terminal event.
            ctx._event_queue.put_nowait(None)

    async def _prepare_turn_retry(self) -> bool:
        """Confirm abandoned work is stopped before replaying a no-progress turn.

        Subclasses with detached cleanup must await it here and return False
        if teardown could not be confirmed. Otherwise cancellation of run_turn
        must have stopped all work belonging to that invocation.
        """
        return True

    def _idle_watchdog_error(
        self, ctx: TurnContext, idle_timeout: float, attempt: int
    ) -> RuntimeError:
        """
        Build the terminal error for an idle-watchdog expiry.

        :param ctx: The per-turn context (used for logging).
        :param idle_timeout: The idle window that expired, in seconds.
        :param attempt: 0-based index of the attempt that wedged, so the
            message can say recovery was already tried.
        :returns: The ``RuntimeError`` to surface as ``response.failed``.
        """
        _logger.warning(
            "run_turn for %s made no progress for %.0fs (idle turn watchdog); "
            "marking the turn failed",
            ctx.response_id,
            idle_timeout,
        )
        # A native forwarder that can't reach the server (e.g.
        # ``No route to host``) starves the turn of progress events, so
        # the idle watchdog — not the connectivity error — is what fails
        # the turn. If such a POST failure was recorded recently, attach
        # it so the user sees the real cause rather than a generic
        # "wedged LLM" reason. The window is one idle window per wedged
        # attempt plus one of slack: the failure that began the stall is
        # already ~idle_timeout × attempts old when the final watchdog
        # fires (each recovery retry consumed another window), so a
        # window equal to the stall would race past it; the extra window
        # captures it while still ignoring a long-resolved earlier blip.
        forwarder_failure = native_forwarder_health.recent_post_failure(
            idle_timeout * (attempt + 2)
        )
        cause = (
            f"likely a wedged LLM or tool call; "
            f"recent forwarder POST failure ({forwarder_failure})"
            if forwarder_failure is not None
            else "likely a wedged LLM or tool call"
        )
        retried = (
            f"; {attempt} recovery retr{'y was' if attempt == 1 else 'ies were'} "
            f"attempted and also wedged"
            if attempt > 0
            else ""
        )
        return RuntimeError(
            f"turn exceeded the {idle_timeout:.0f}s harness idle watchdog "
            f"(run_turn emitted no events for {idle_timeout:.0f}s; {cause}{retried})"
        )

    async def _heartbeat_loop(self, ctx: TurnContext) -> None:
        """
        Emit ``response.heartbeat`` on the queue every
        :data:`_HEARTBEAT_INTERVAL_S`.

        Cancellation is the normal exit path — the streaming
        wrapper cancels this task when the turn finalizes.

        :param ctx: The per-turn context whose queue receives
            heartbeats.
        """
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL_S)
            ctx.emit(HeartbeatEvent(type="response.heartbeat"))

    async def _build_terminal_event(
        self,
        ctx: TurnContext,
        model: str,
        run_task: asyncio.Task[None],
        sequence: int,
    ) -> ServerStreamEvent:
        """
        Construct the terminal SSE event after ``run_turn`` returns.

        :param ctx: The per-turn context (used to read
            ``response_id``).
        :param model: The agent name from the incoming
            :class:`CreateResponseRequest.model`. Required so the
            terminal :class:`ResponseObject` carries a real model
            value (no empty-string sentinels per the project's
            anti-pattern checklist).
        :param run_task: The completed (or failed) ``run_turn``
            task whose exception state determines the event type.
        :param sequence: The sequence number to assign to the
            terminal event.
        :returns: A :class:`CompletedEvent`, :class:`FailedEvent`,
            or cancelled-event variant carrying a synthesized
            :class:`ResponseObject`. The synthesized object is
            minimal — AP's persistence path constructs the
            authoritative ResponseObject.
        """
        # The sentinel that gets us here is queued from ``run_turn``'s
        # ``finally`` block, so the streaming side can observe it before the
        # task is fully terminal. Wait for that last scheduling tick before
        # inspecting task state; otherwise a cancel/error race can make the
        # terminal-event builder raise while trying to classify the result.
        exception: BaseException | None = None
        if not run_task.done():
            try:
                await asyncio.shield(run_task)
            except asyncio.CancelledError:
                if not run_task.done():
                    raise
            except Exception as exc:
                exception = exc
        if exception is None and run_task.done() and not run_task.cancelled():
            try:
                # ``run_task.exception()`` is ``None`` on clean return, an
                # exception instance otherwise. It can still raise
                # CancelledError for cancellation races; classify that as a
                # cancelled terminal instead of letting terminal synthesis fail.
                exception = run_task.exception()
            except asyncio.CancelledError:
                exception = None
        cancelled = run_task.cancelled() or ctx.cancelled.is_set()
        if cancelled:
            status_value = "cancelled"
        elif exception is not None:
            status_value = "failed"
        else:
            status_value = "completed"

        # Convert the inner executor's usage dict to a ResponseObject
        # Usage instance so the harness client can read it from the
        # serialized response.completed SSE payload.
        usage: Usage | None = None
        if ctx.provider_usage is not None:
            u = ctx.provider_usage
            usage = Usage(
                input_tokens=u.get("input_tokens") or 0,
                output_tokens=u.get("output_tokens") or 0,
                total_tokens=u.get("total_tokens") or 0,
                context_tokens=u.get("context_tokens"),
                # Carry the cache breakdown through to response.completed
                # so the server-side cost path can price cache reads /
                # writes at their own rates. Anthropic-style executors
                # (e.g. claude-sdk) report these; others omit them (0).
                cache_read_input_tokens=u.get("cache_read_input_tokens") or 0,
                cache_creation_input_tokens=u.get("cache_creation_input_tokens") or 0,
                # Harness-reported model for cost pricing (ResponseObject.model is the agent name).
                model=u.get("model"),
                # Authoritative per-turn cost reported by the harness (e.g. Copilot
                # AI credits); preferred over the catalog estimate. None if absent.
                cost_usd=u.get("cost_usd"),
            )
        response = ResponseObject(
            id=ctx.response_id,
            status=status_value,
            model=model,
            created_at=int(time.time()),
            error=None if exception is None else self._build_error_detail(exception),
            usage=usage,
        )
        terminal: ServerStreamEvent
        if status_value == "completed":
            terminal = CompletedEvent(
                type="response.completed", response=response, sequence_number=sequence
            )
        elif status_value == "failed":
            terminal = FailedEvent(
                type="response.failed",
                source="harness",
                response=response,
                sequence_number=sequence,
            )
        else:
            from omnigent.server.schemas import CancelledEvent

            terminal = CancelledEvent(
                type="response.cancelled",
                response=response,
                sequence_number=sequence,
            )
        return terminal

    async def _resolve_elicitation(
        self,
        elicitation_id: str,
        body: ElicitationResult,
    ) -> Response:
        """
        Resolve a parked elicitation Future from an
        :class:`ApprovalEvent`.

        Invoked by :meth:`_post_session_event` for ``approval``
        events. The id MUST belong to an outstanding elicitation
        on one of the in-flight turns; otherwise raises 404.

        :param elicitation_id: Correlation id from the event body.
        :param body: MCP-shape :class:`ElicitationResult` adapted
            from the :class:`ApprovalEvent`.
        :returns: 204 on success.
        :raises OmnigentError: 404 if no pending elicitation
            matches the id (across all in-flight turns).
        """
        for ctx in self._in_flight.values():
            if ctx._complete_elicitation(elicitation_id, body):
                return Response(status_code=status.HTTP_204_NO_CONTENT)
        raise OmnigentError(
            f"no outstanding elicitation {elicitation_id!r}",
            code=ErrorCode.NOT_FOUND,
        )


def _format_sse_event(event: HarnessStreamEvent) -> bytes:
    """
    Serialize a typed event to an SSE wire frame.

    Wire shape: ``event: <name>\\ndata: <json>\\n\\n``.

    :param event: Any variant from
        :data:`omnigent.server.schemas.ServerStreamEvent`.
    :returns: UTF-8-encoded bytes ready to ship over the HTTP
        response.
    """
    payload = event.model_dump_json(exclude_none=True)
    return f"event: {event.type}\ndata: {payload}\n\n".encode()


def _utc_now_iso() -> str:
    """
    Return the current UTC wall-clock as an ISO 8601 string.

    Used for ``HeartbeatEvent.server_time`` so consumers can
    detect clock drift between producer and consumer. Format
    matches the contract example: ``"2026-04-27T15:30:00Z"``
    (trailing ``Z`` for UTC, microseconds elided to keep the
    wire compact).

    :returns: An ISO 8601 timestamp like
        ``"2026-04-27T15:30:00Z"``.
    """
    import datetime

    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# Note: ``HarnessApp._build_error_detail`` is the canonical
# implementation. Per-harness wraps that need to map SDK-specific
# exceptions (e.g. ``anthropic.RateLimitError``) onto the
# AP-side retryable allowlist override the method on their
# subclass — see
# :meth:`omnigent.runtime.harnesses._executor_adapter.ExecutorAdapter._build_error_detail`.


async def _health() -> dict[str, str]:
    """
    Liveness probe — matches AP's ``/health`` shape.

    :returns: ``{"status": "ok"}``.
    """
    return {"status": "ok"}


async def _handle_omnigent_error(_request: Request, exc: OmnigentError) -> Response:
    """
    Convert :class:`OmnigentError` into a JSON response with the
    correct HTTP status.

    Mirrors AP's exception handler at
    ``omnigent/server/app.py``.

    :param _request: Unused (FastAPI signature requirement).
    :param exc: The application error to render.
    :returns: A JSON response with the error code + message.
    """
    from fastapi.responses import JSONResponse

    return JSONResponse(
        status_code=exc.http_status,
        content={"error": {"code": exc.code, "message": exc.message}},
    )
