"""Runner-side ``sys_ask_user`` executor — durable ask-and-park.

Intercepted by ``dispatch_tool_locally`` before the generic tool dispatch.
The flow:

1. **Register** — POST a ``durable_ask`` event to the Omnigent server. The
   server validates the arguments, persists the question (workspace-scoped,
   keyed on session + ``invocation_id``), publishes the form card, and
   returns the stable elicitation id. Persistence precedes publication.
2. **Suspend deadlines** — POST a ``durable_ask_pending`` event to the
   harness so the turn's idle and absolute watchdogs stop ticking while a
   human is deciding. Ordinary tools keep their budgets.
3. **Park** — wait for the verdict on ``pending_approvals`` (the server
   forwards the answer as a canonical ``approval`` event). A server
   reconnect re-fetches the durable state instead of re-asking: the
   question is server-persisted, so an answer submitted while the tunnel
   was down is adopted on re-check.
4. **Resume** — format the tool result, mark the durable question consumed
   (exactly-once via the server's claimed gate), and let
   ``dispatch_tool_locally`` deliver the tool_result.

Client/tunnel loss and server restarts are detachment, not cancellation.
Only an explicit session interrupt cancels the question — the parked task
is cancelled, and the pending durable question is canceled server-side so
a later answer cannot resurrect a dead execution.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx

from omnigent.runner import pending_approvals
from omnigent.tools.builtins.sys_ask_user import (
    format_tool_result,
    validate_ask_user_args,
)

_logger = logging.getLogger(__name__)

#: Poll interval while re-fetching durable state after a server reconnect.
_RECHECK_INTERVAL_S = 5.0

#: How long a single state-fetch request may take during re-checks.
_FETCH_TIMEOUT_S = 15.0

# call_id → elicitation_id for in-flight durable asks. ``dispatch_tool_locally``
# reads it after the tool_result POST lands so consumption is marked only
# after the harness actually received the result (a consumed-but-undelivered
# answer would strand the parked harness turn). Entries are removed on exit.
_active_asks: dict[str, str] = {}

# conversation_id → dispatch tasks parked on a durable ask. An explicit
# interrupt cancels these — the harness-side future alone does NOT unwind the
# runner-side park, and the park's cancellation handler is what cancels the
# question server-side so a late answer cannot resurrect the stopped turn.
_active_ask_tasks: dict[str, set[asyncio.Task]] = {}


def cancel_active_asks(conversation_id: str) -> int:
    """
    Cancel every durable-ask dispatch parked for a session (explicit interrupt).

    Cancelling the dispatch task triggers its cancellation handler, which
    cancels the pending question server-side — the turn stays stopped.

    :param conversation_id: Session id.
    :returns: The number of parked asks cancelled.
    """
    tasks = _active_ask_tasks.get(conversation_id)
    if not tasks:
        return 0
    count = len(tasks)
    for task in list(tasks):
        task.cancel()
    return count


def reset_for_tests() -> None:
    """Clear in-flight ask bookkeeping. Test isolation only."""
    _active_asks.clear()


def invocation_id_for_call(call_id: str | None) -> str:
    """
    Stable invocation identity for a durable ask.

    The LLM tool-call id is the logical invocation: replays and re-deliveries
    of the same call must map to the same durable question. A synthetic id
    is only used when the harness supplied none (which forfeits
    replay-dedup for that call).

    :param call_id: The dispatch call id, e.g. ``"call_abc123"``.
    :returns: The invocation id string.
    """
    return call_id or f"askuser_{id(asyncio.current_task()):x}"


async def _post_server_event(
    server_client: httpx.AsyncClient,
    conversation_id: str,
    payload: dict[str, Any],
    *,
    timeout: float = 30.0,
) -> dict[str, Any] | None:
    """
    POST one control event to the Omnigent server; return the JSON body.

    :param server_client: Runner → server HTTP client.
    :param conversation_id: Session id, e.g. ``"conv_abc123"``.
    :param payload: Event body (``type`` + ``data``).
    :param timeout: Request timeout in seconds.
    :returns: Parsed JSON body, or ``None`` on failure.
    """
    try:
        resp = await server_client.post(
            f"/v1/sessions/{conversation_id}/events",
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}
    except Exception as exc:  # noqa: BLE001 — surfaced by callers as tool errors
        _logger.warning("sys_ask_user server event failed (%s): %s", payload.get("type"), exc)
        return None


async def _post_harness_event(
    harness_client: httpx.AsyncClient,
    conversation_id: str,
    payload: dict[str, Any],
) -> None:
    """
    POST one control event to the harness app; best-effort.

    :param harness_client: Runner → harness HTTP client.
    :param conversation_id: Session id.
    :param payload: Event body.
    """
    try:
        resp = await harness_client.post(
            f"/v1/sessions/{conversation_id}/events",
            json=payload,
            timeout=15.0,
        )
        resp.raise_for_status()
    except Exception:  # noqa: BLE001 — watchdog suspension is best-effort
        _logger.warning(
            "sys_ask_user harness event failed (%s)", payload.get("type"), exc_info=True
        )


async def _fetch_durable_state(
    server_client: httpx.AsyncClient,
    conversation_id: str,
    elicitation_id: str,
) -> dict[str, Any] | None:
    """
    Fetch a durable question's state from the server.

    :param server_client: Runner → server HTTP client.
    :param conversation_id: Session id.
    :param elicitation_id: The durable question's correlation id.
    :returns: The JSON body (``status`` + optional ``action``/``content``),
        or ``None`` when unreachable.
    """
    try:
        resp = await server_client.get(
            f"/v1/sessions/{conversation_id}/elicitations/{elicitation_id}",
            timeout=_FETCH_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception as exc:  # noqa: BLE001 — callers retry on the next poll
        _logger.warning("sys_ask_user state fetch failed for %s: %s", elicitation_id, exc)
        return None


def _answer_from_state(state: dict[str, Any]) -> str | None:
    """
    Extract the tool result from a resolved durable-state payload.

    :param state: The ``GET /elicitations/{eid}`` body with ``status ==
        "resolved"`` and the committed ``action``/``content``.
    :returns: The formatted tool-result JSON, or ``None`` when the state
        carries no usable answer.
    """
    action = state.get("action")
    if action not in {"accept", "decline", "cancel"}:
        return None
    content = state.get("content") if isinstance(state.get("content"), dict) else None
    answer = content.get("answer") if content else None
    if not isinstance(answer, (str, list)):
        answer = None
    return format_tool_result(
        action=action,
        answer=answer if action == "accept" else None,
    )


async def execute_ask_user(
    *,
    args: dict[str, Any],
    call_id: str,
    conversation_id: str,
    server_client: httpx.AsyncClient,
    harness_client: httpx.AsyncClient,
    response_id: str | None = None,
    publish_event: Any = None,
) -> str:
    """
    Execute one ``sys_ask_user`` call durably; returns the tool output.

    Parks indefinitely while the question is pending — no timeout. The
    durable server-side state is authoritative: a verdict delivered via
    ``pending_approvals`` wins, and after a server reconnect the parked
    wait is rebuilt from the persisted state (an answer submitted while
    the tunnel was down is adopted, not re-asked).

    :param args: Parsed tool arguments.
    :param call_id: The dispatch call id — the stable invocation identity.
    :param conversation_id: Session id.
    :param server_client: Runner → server HTTP client.
    :param harness_client: Runner → harness HTTP client.
    :param response_id: The in-flight turn's response id, recorded in the
        paused-call checkpoint so a restarted runner can identify the
        interrupted logical execution.
    :param publish_event: Runner SSE publish callable (unused; kept for
        dispatch-signature parity).
    :returns: JSON tool output (``{"action": ..., "answer": ...}`` or
        ``{"error": ...}``).
    :raises asyncio.CancelledError: On explicit interrupt — the pending
        durable question is canceled server-side before propagating.
    """
    del publish_event  # signature parity with dispatch; the server publishes
    invocation_id = invocation_id_for_call(call_id)
    # Validate BEFORE any pending state exists, using the shared contract.
    validated = validate_ask_user_args(args)
    if isinstance(validated, str):
        return json.dumps({"error": validated})
    registered = await _post_server_event(
        server_client,
        conversation_id,
        {
            "type": "durable_ask",
            "data": {
                "invocation_id": invocation_id,
                "arguments": args,
                # Paused-call checkpoint: lets a restarted runner continue
                # the same logical execution from committed history.
                "paused_call": {
                    "call_id": call_id,
                    "tool_name": "sys_ask_user",
                    "arguments": args,
                    "response_id": response_id,
                },
            },
        },
    )
    if registered is None:
        return json.dumps(
            {"error": "sys_ask_user could not register the question with the server"}
        )
    if registered.get("error"):
        return json.dumps({"error": registered["error"]})
    elicitation_id = registered.get("elicitation_id")
    if not isinstance(elicitation_id, str) or not elicitation_id:
        return json.dumps({"error": "server returned no elicitation_id"})

    _active_asks[call_id] = elicitation_id
    _me = asyncio.current_task()
    assert _me is not None
    _active_ask_tasks.setdefault(conversation_id, set()).add(_me)
    # Suspend the harness turn's deadlines while the human decides.
    await _post_harness_event(
        harness_client,
        conversation_id,
        {"type": "durable_ask_pending", "call_id": call_id},
    )
    fut = pending_approvals.register(
        elicitation_id,
        retry_on_server_reconnect=True,
    )
    try:
        verdict: pending_approvals.Verdict | None = None
        while True:
            try:
                verdict = await fut
                break
            except pending_approvals.ServerReconnected:
                # The server (or its tunnel) was replaced. The question is
                # durably persisted server-side: re-fetch state and adopt
                # an answer submitted in the gap; otherwise keep waiting.
                while True:
                    state = await _fetch_durable_state(
                        server_client, conversation_id, elicitation_id
                    )
                    if state is not None and state.get("status") == "resolved":
                        adopted_action = state.get("action")
                        answer = _answer_from_state(state)
                        if answer is not None:
                            verdict = pending_approvals.Verdict(
                                approved=adopted_action == "accept",
                                content=(
                                    state.get("content")
                                    if isinstance(state.get("content"), dict)
                                    and adopted_action == "accept"
                                    else None
                                ),
                                # Carry the TRUE committed action through
                                # adoption: a cancel adopted after a
                                # reconnect must format as cancel, not
                                # decline.
                                action=(
                                    adopted_action
                                    if adopted_action in {"accept", "decline", "cancel"}
                                    else None
                                ),
                            )
                        else:
                            # Terminal without a usable answer (decline/
                            # cancel committed by another surface).
                            verdict = pending_approvals.Verdict(
                                approved=False,
                                action=(
                                    adopted_action
                                    if adopted_action in {"decline", "cancel"}
                                    else "decline"
                                ),
                            )
                        break
                    await asyncio.sleep(_RECHECK_INTERVAL_S)
                fut = pending_approvals.register(
                    elicitation_id,
                    retry_on_server_reconnect=True,
                )
                if verdict is not None:
                    break
        assert verdict is not None
        if verdict.approved:
            raw = verdict.content.get("answer") if isinstance(verdict.content, dict) else None
            answer = raw if isinstance(raw, (str, list)) else None
            output = format_tool_result(action="accept", answer=answer)
        else:
            # A cancel is NOT a decline: the model must see the truthful
            # terminal action so it does not read the question as refused.
            output = format_tool_result(
                action=verdict.action if verdict.action in {"decline", "cancel"} else "decline"
            )
        return output
    except asyncio.CancelledError:
        # Explicit interrupt: cancel the pending question server-side so a
        # late answer cannot resurrect a dead execution.
        _active_asks.pop(call_id, None)
        await _post_server_event(
            server_client,
            conversation_id,
            {
                "type": "approval",
                "data": {
                    "elicitation_id": elicitation_id,
                    "action": "cancel",
                },
            },
        )
        raise
    finally:
        pending_approvals.cleanup(elicitation_id)
        _me = asyncio.current_task()
        _tasks = _active_ask_tasks.get(conversation_id)
        if _tasks is not None and _me is not None:
            _tasks.discard(_me)
            if not _tasks:
                _active_ask_tasks.pop(conversation_id, None)
        # _active_asks[call_id] is intentionally kept: dispatch_tool_locally
        # reads it after the tool_result POST lands to mark consumption, and
        # mark_consumed_if_ask removes the entry itself.
        # Restore normal execution budgets before the tool result flows.
        await _post_harness_event(
            harness_client,
            conversation_id,
            {"type": "durable_ask_resolved", "call_id": call_id},
        )


async def mark_consumed_if_ask(
    *,
    tool_name: str,
    call_id: str,
    conversation_id: str,
    server_client: httpx.AsyncClient,
) -> None:
    """
    Mark a delivered ``sys_ask_user`` result consumed on the server.

    Called by ``dispatch_tool_locally`` AFTER the harness acknowledged the
    tool_result, so consumption can never outpace delivery: a crashed
    delivery leaves the answer unconsumed and recoverable.

    :param tool_name: The executed tool's name.
    :param call_id: The dispatch call id.
    :param conversation_id: Session id.
    :param server_client: Runner → server HTTP client.
    """
    if tool_name != "sys_ask_user":
        return
    elicitation_id = _active_asks.pop(call_id, None)
    if elicitation_id is None:
        return
    await _post_server_event(
        server_client,
        conversation_id,
        {
            "type": "durable_ask_consumed",
            "data": {"elicitation_id": elicitation_id},
        },
    )


# ── Runner-restart recovery ────────────────────────────────────────────


async def recover_answered_ask(
    *,
    elicitation_id: str,
    conversation_id: str,
    action: str,
    content: dict[str, Any] | None,
    server_client: httpx.AsyncClient,
    paused_call: dict[str, Any] | None = None,
) -> bool:
    """
    Continue a parked ``sys_ask_user`` execution whose harness turn died.

    A runner process restart kills the harness turn parked in
    ``dispatch_tool``; the answer may arrive afterwards (this runner's
    approval-event handler saw an unknown elicitation id) or already be
    committed (adopted at reconnect). Recovery continues the SAME logical
    execution without replaying anything: the original prompt and every
    preceding tool result are already in the committed history, so a
    single hidden meta message — the established wake mechanism (timers
    use it) — starts a continuation turn where the model reads its own
    dangling ``sys_ask_user`` call from history plus the answer, and
    proceeds. Nothing is re-executed.

    Ordering is claim-first: the durable ask is marked consumed BEFORE the
    wake is posted, so a crash can strand the continuation (a stuck turn,
    visible and recoverable by re-asking) but can never duplicate it —
    delivery retries are acceptable, duplicate logical continuation is not.

    :param elicitation_id: The durable question's correlation id.
    :param conversation_id: Owning session id.
    :param action: The committed answer action (``accept``/``decline``/
        ``cancel``).
    :param content: The committed content map on accept.
    :param server_client: Runner → server HTTP client.
    :param paused_call: The persisted paused-call checkpoint (call id, tool
        name, arguments, response id). When present, the tool result is
        first persisted as a proper ``function_call_output`` item keyed to
        the dangling call, so the committed history is complete before the
        wake starts the continuation turn.
    :returns: ``True`` when this call claimed the consumption AND posted
        the continuation wake; ``False`` when the ask was already consumed
        (a duplicate recovery is a no-op).
    """
    from omnigent.runtime.durable_ask_user import continuation_wake_text

    tool_output = format_tool_result(
        action=action if action in {"accept", "decline", "cancel"} else "decline",
        answer=content.get("answer") if action == "accept" and isinstance(content, dict) else None,
    )
    if action == "cancel":
        # Explicit user cancellation: the parked turn is dead by intent.
        # Never wake — an interrupt must stay stopped, and a canceled row
        # must never be resurrected by a forwarded verdict or a sweep.
        _logger.info(
            "sys_ask_user %s was explicitly canceled; recovery skipped",
            elicitation_id,
            extra={"session_id": conversation_id},
        )
        return False
    wake_text = continuation_wake_text(action=action, content=content, tool_output=tool_output)
    # Claim FIRST (exactly-once); a duplicate recovery attempt is a no-op.
    claimed = await _post_server_event(
        server_client,
        conversation_id,
        {
            "type": "durable_ask_consumed",
            "data": {"elicitation_id": elicitation_id},
        },
    )
    if claimed is None or not claimed.get("consumed"):
        return False
    if paused_call and isinstance(paused_call.get("call_id"), str):
        # Persist the tool result against the dangling call so the history
        # is structurally complete (function_call + function_call_output)
        # before the continuation turn starts.
        await _post_server_event(
            server_client,
            conversation_id,
            {
                "type": "external_conversation_item",
                "data": {
                    "item_type": "function_call_output",
                    "item_data": {
                        "call_id": paused_call["call_id"],
                        "output": tool_output,
                    },
                    "response_id": paused_call.get("response_id")
                    or f"resp_durable_{elicitation_id[:12]}",
                },
            },
        )
    # Wake the session with one hidden meta message (bounded retries —
    # delivery failures leave a stuck turn, never a duplicate continuation).
    for attempt in range(3):
        delivered = await _post_server_event(
            server_client,
            conversation_id,
            {
                "type": "message",
                "data": {
                    "role": "user",
                    "is_meta": True,
                    "content": [{"type": "input_text", "text": wake_text}],
                },
            },
            timeout=30.0,
        )
        if delivered is not None:
            _logger.info(
                "sys_ask_user %s recovered after runner restart; continuation wake delivered",
                elicitation_id,
                extra={"session_id": conversation_id},
            )
            return True
        await asyncio.sleep(2.0 * (attempt + 1))
    _logger.error(
        "sys_ask_user %s consumed but the continuation wake could not be "
        "delivered after retries; the session turn is stuck",
        elicitation_id,
        extra={"session_id": conversation_id},
    )
    return False
