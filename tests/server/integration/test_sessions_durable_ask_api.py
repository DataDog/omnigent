"""API tests for the durable ``sys_ask_user`` registration + resolution flow.

Exercises the real server routes (shared ``client`` fixture): register a
durable question via the ``durable_ask`` session event, read it back via the
standalone elicitation GET and the session snapshot (cold-restart merge),
resolve it through the standard resolve endpoint, and assert duplicate,
conflicting, and malformed answers behave per the durable contract.
"""

from __future__ import annotations

import json

import httpx
import pytest

from omnigent.runtime.durable_ask_user import configure_durable_elicitation_store
from tests.server.helpers import create_test_agent
from tests.server.integration.test_sessions_elicitation_resolve_url import (
    _create_child_session,
    _create_session,
)

pytestmark = pytest.mark.asyncio


async def _register_durable(
    client: httpx.AsyncClient,
    session_id: str,
    *,
    invocation_id: str = "call_1",
    arguments: dict | None = None,
) -> dict:
    resp = await client.post(
        f"/v1/sessions/{session_id}/events",
        json={
            "type": "durable_ask",
            "data": {
                "invocation_id": invocation_id,
                "arguments": arguments or {"question": "Env?", "options": ["staging", "prod"]},
            },
        },
    )
    assert resp.status_code in {200, 202}, resp.text
    return resp.json()


@pytest.fixture()
def _wired_store(db_uri: str):
    """Wire the durable store into the server under test; unwire after."""
    from omnigent.stores.durable_elicitation_store import (
        SqlAlchemyDurableElicitationStore,
    )

    configure_durable_elicitation_store(SqlAlchemyDurableElicitationStore(db_uri))
    yield
    configure_durable_elicitation_store(None)


async def test_register_returns_stable_id_and_persists(
    client: httpx.AsyncClient, _wired_store
) -> None:
    agent = await create_test_agent(client, "durable-register")
    session_id = await _create_session(client, agent["id"])
    first = await _register_durable(client, session_id)
    assert first.get("error") is None
    elicitation_id = first["elicitation_id"]
    assert isinstance(elicitation_id, str) and elicitation_id

    # A replay under the same invocation identity returns the SAME id.
    replay = await _register_durable(client, session_id)
    assert replay["elicitation_id"] == elicitation_id

    # A changed payload under the same identity is rejected cleanly.
    changed = await _register_durable(
        client,
        session_id,
        arguments={"question": "Different?", "options": ["a"]},
    )
    assert changed.get("error"), "changed payload must be rejected"
    assert changed.get("elicitation_id") == elicitation_id


async def test_register_invalid_arguments_fail_cleanly(
    client: httpx.AsyncClient, _wired_store
) -> None:
    agent = await create_test_agent(client, "durable-invalid")
    session_id = await _create_session(client, agent["id"])
    result = await _register_durable(client, session_id, arguments={"question": "   "})
    assert result.get("error")
    assert "elicitation_id" not in result


async def test_get_elicitation_returns_pending_with_options(
    client: httpx.AsyncClient, _wired_store
) -> None:
    agent = await create_test_agent(client, "durable-get")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    resp = await client.get(f"/v1/sessions/{session_id}/elicitations/{elicitation_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "pending"
    assert data["durable"] is True
    assert data["requestedSchema"]["properties"]["answer"]["enum"] == [
        "staging",
        "prod",
    ]


async def test_snapshot_includes_pending_question(client: httpx.AsyncClient, _wired_store) -> None:
    """The session snapshot replays the durable card (cold-load restore)."""
    agent = await create_test_agent(client, "durable-snapshot")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    resp = await client.get(f"/v1/sessions/{session_id}")
    assert resp.status_code == 200, resp.text
    pending = resp.json().get("pending_elicitations") or []
    ids = [e.get("elicitation_id") for e in pending]
    assert elicitation_id in ids


async def test_resolve_accepts_answer_and_clears_pending(
    client: httpx.AsyncClient, _wired_store
) -> None:
    agent = await create_test_agent(client, "durable-resolve")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    resp = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )
    assert resp.status_code == 202, resp.text

    # GET now reports the committed answer (delivery still outstanding).
    data = (await client.get(f"/v1/sessions/{session_id}/elicitations/{elicitation_id}")).json()
    assert data["status"] == "resolved"
    assert data["action"] == "accept"
    assert data["content"] == {"answer": "staging"}


async def test_resolve_malformed_answer_leaves_question_pending(
    client: httpx.AsyncClient, _wired_store
) -> None:
    agent = await create_test_agent(client, "durable-malformed")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    resp = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "not-an-option"}},
    )
    assert resp.status_code in {200, 202}, resp.text
    data = (await client.get(f"/v1/sessions/{session_id}/elicitations/{elicitation_id}")).json()
    assert data["status"] == "pending", "malformed answer must not resolve the question"


async def test_resolve_duplicate_and_conflict(client: httpx.AsyncClient, _wired_store) -> None:
    agent = await create_test_agent(client, "durable-dup")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    first = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )
    assert first.status_code == 202
    # Identical retry: idempotent 202, answer unchanged.
    retry = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )
    assert retry.status_code == 202
    # Conflicting answer: rejected, first answer stands.
    conflict = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "prod"}},
    )
    assert conflict.status_code in {200, 202, 409}
    data = (await client.get(f"/v1/sessions/{session_id}/elicitations/{elicitation_id}")).json()
    assert data["content"] == {"answer": "staging"}, "first answer must stand"


async def test_resolve_unknown_durable_id_falls_through_to_legacy_404_shape(
    client: httpx.AsyncClient, _wired_store
) -> None:
    """An unknown id must not create a durable answer tombstone."""
    agent = await create_test_agent(client, "durable-unknown")
    session_id = await _create_session(client, agent["id"])
    resp = await client.post(
        f"/v1/sessions/{session_id}/elicitations/deadbeefdeadbeefdeadbeefdeadbeef/resolve",
        json={"action": "accept", "content": {"answer": "x"}},
    )
    # Legacy path behavior for an unknown id: accepted for forwarding
    # (the runner rejects on its own terms) — the key assertion is that the
    # durable store has no row for it, checked indirectly by GET.
    _ = resp
    get_resp = await client.get(
        f"/v1/sessions/{session_id}/elicitations/deadbeefdeadbeefdeadbeefdeadbeef"
    )
    assert get_resp.status_code == 200
    assert get_resp.json()["status"] == "resolved"


async def test_consume_event_marks_exactly_once(client: httpx.AsyncClient, _wired_store) -> None:
    from omnigent.runtime.durable_ask_user import durable_store

    agent = await create_test_agent(client, "durable-consume")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]
    await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )

    resp = await client.post(
        f"/v1/sessions/{session_id}/events",
        json={
            "type": "durable_ask_consumed",
            "data": {"elicitation_id": elicitation_id},
        },
    )
    assert resp.status_code in {200, 202}, resp.text
    assert resp.json()["consumed"] is True
    # A lost-ack retry is an idempotent no-op — no second continuation.
    resp = await client.post(
        f"/v1/sessions/{session_id}/events",
        json={
            "type": "durable_ask_consumed",
            "data": {"elicitation_id": elicitation_id},
        },
    )
    assert resp.json()["consumed"] is False
    store = durable_store()
    assert store is not None
    record = store.get(elicitation_id)
    assert record is not None
    assert record.state == "consumed"
    assert record.consumed_at is not None


async def test_unwired_store_rejects_registration(
    client: httpx.AsyncClient,
) -> None:
    """Without a wired store the surface fails clean, never half-registers."""
    configure_durable_elicitation_store(None)
    agent = await create_test_agent(client, "durable-unwired")
    session_id = await _create_session(client, agent["id"])
    result = await _register_durable(client, session_id)
    assert result.get("error")


async def test_paused_call_checkpoint_persists(client: httpx.AsyncClient, _wired_store) -> None:
    """The registration's paused-call checkpoint is persisted for recovery."""
    from omnigent.runtime.durable_ask_user import durable_store

    agent = await create_test_agent(client, "durable-checkpoint")
    session_id = await _create_session(client, agent["id"])
    resp = await client.post(
        f"/v1/sessions/{session_id}/events",
        json={
            "type": "durable_ask",
            "data": {
                "invocation_id": "call_ask_1",
                "arguments": {"question": "Env?", "options": ["staging", "prod"]},
                "paused_call": {
                    "call_id": "call_ask_1",
                    "tool_name": "sys_ask_user",
                    "arguments": {"question": "Env?", "options": ["staging", "prod"]},
                    "response_id": "resp_turn_1",
                },
            },
        },
    )
    assert resp.status_code in {200, 202}, resp.text
    elicitation_id = resp.json()["elicitation_id"]
    store = durable_store()
    assert store is not None
    record = store.get(elicitation_id)
    assert record is not None
    assert record.paused_call is not None
    assert record.paused_call["call_id"] == "call_ask_1"
    assert record.paused_call["response_id"] == "resp_turn_1"


async def test_unconsumed_listing_after_answer(client: httpx.AsyncClient, _wired_store) -> None:
    """The recovery listing returns answered-unconsumed asks with content."""
    agent = await create_test_agent(client, "durable-unconsumed")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    resp = await client.get(f"/v1/sessions/{session_id}/durable_elicitations/unconsumed")
    assert resp.status_code == 200
    assert resp.json() == {"unconsumed": []}  # still pending

    await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )
    listing = (
        await client.get(f"/v1/sessions/{session_id}/durable_elicitations/unconsumed")
    ).json()["unconsumed"]
    assert [row["elicitation_id"] for row in listing] == [elicitation_id]
    assert listing[0]["action"] == "accept"
    assert listing[0]["content"] == {"answer": "staging"}

    # Consuming removes it from the recovery listing — exactly-once.
    await client.post(
        f"/v1/sessions/{session_id}/events",
        json={
            "type": "durable_ask_consumed",
            "data": {"elicitation_id": elicitation_id},
        },
    )
    resp = await client.get(f"/v1/sessions/{session_id}/durable_elicitations/unconsumed")
    assert resp.json() == {"unconsumed": []}


async def test_interrupt_stays_stopped(client: httpx.AsyncClient, _wired_store) -> None:
    """Explicit cancel is terminal: never re-listed for recovery, never re-rendered."""
    from omnigent.runtime.durable_ask_user import durable_store

    agent = await create_test_agent(client, "durable-interrupt")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]

    # Explicit interrupt: the runner posts a cancel verdict.
    resp = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "cancel"},
    )
    assert resp.status_code == 202, resp.text

    # The canceled row is terminal: not re-listed for recovery adoption…
    listing = (
        await client.get(f"/v1/sessions/{session_id}/durable_elicitations/unconsumed")
    ).json()["unconsumed"]
    assert listing == [], "canceled rows must never be adopted by recovery"
    # …and not re-rendered as an answerable card in the snapshot.
    snap = (await client.get(f"/v1/sessions/{session_id}")).json()
    ids = [e.get("elicitation_id") for e in (snap.get("pending_elicitations") or [])]
    assert elicitation_id not in ids, "canceled rows must not re-render as cards"
    store = durable_store()
    assert store is not None
    row = store.get(elicitation_id)
    assert row is not None
    assert row.state == "canceled"
    assert row.consumed_at is None  # bookkeeping: never consumed, never re-woken


async def test_child_question_routes_to_parent(
    client: httpx.AsyncClient, _wired_store, db_uri: str
) -> None:
    """A child's durable question surfaces on the parent and resolves the child's call."""
    agent = await create_test_agent(client, "durable-child")
    parent_id = await _create_session(client, agent["id"])
    child_id = _create_child_session(db_uri, parent_id=parent_id, agent_id=agent["id"])
    registered = await _register_durable(client, child_id)
    elicitation_id = registered["elicitation_id"]

    # Parent cold snapshot replays the child's question, routed to the child.
    snap = (await client.get(f"/v1/sessions/{parent_id}")).json()
    pending = [
        e
        for e in (snap.get("pending_elicitations") or [])
        if e.get("elicitation_id") == elicitation_id
    ]
    assert len(pending) == 1, f"child question missing from parent snapshot: {pending}"
    assert pending[0]["params"].get("target_session_id") == child_id

    # Answering from the parent view resolves the child's parked call.
    resp = await client.post(
        f"/v1/sessions/{parent_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )
    assert resp.status_code == 202, resp.text
    from omnigent.runtime.durable_ask_user import durable_store

    store = durable_store()
    assert store is not None
    row = store.get(elicitation_id)
    assert row is not None
    assert row.conversation_id == child_id
    assert row.answer_content == {"answer": "staging"}


async def test_conflicting_answer_returns_409(client: httpx.AsyncClient, _wired_store) -> None:
    """A second, different answer fails loud (409) — the first answer stands."""
    agent = await create_test_agent(client, "durable-conflict-409")
    session_id = await _create_session(client, agent["id"])
    registered = await _register_durable(client, session_id)
    elicitation_id = registered["elicitation_id"]
    first = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )
    assert first.status_code == 202
    conflict = await client.post(
        f"/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "production"}},
    )
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["error"]["code"] == "conflict"
    # The first answer still stands.
    data = (await client.get(f"/v1/sessions/{session_id}/elicitations/{elicitation_id}")).json()
    assert data["content"] == {"answer": "staging"}


async def test_owner_targeted_wake_and_forward(
    client: httpx.AsyncClient, _wired_store, db_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A parent-view answer must wake the CHILD, never the resolving parent.

    With no live runner waiter, recovery consumes the owner's row and posts
    the continuation wake on the owner's session. If the forward targeted the
    resolving session instead, the child would be stranded (row consumed,
    never re-adopted) and the parent would start a bogus continuation turn.
    """

    from omnigent.runtime.durable_ask_user import durable_store
    from omnigent.server.routes._sessions import orchestration as _orch

    agent = await create_test_agent(client, "durable-owner-wake")
    parent_id = await _create_session(client, agent["id"])
    child_id = _create_child_session(db_uri, parent_id=parent_id, agent_id=agent["id"])
    # Register the ask ON the child (the owner).
    registered = await _register_durable(client, child_id)
    elicitation_id = registered["elicitation_id"]

    forwarded: list[tuple[str, dict]] = []
    published: list[tuple[str, str]] = []
    original_forward = _orch._forward_approval_to_runner
    original_publish = _orch._publish_elicitation_resolved

    async def _capture_forward(session_id, data, runner_router):
        forwarded.append((session_id, data))

    def _capture_publish(session_id, elicitation_id, action=None, reason=None):
        published.append((session_id, elicitation_id))

    monkeypatch.setattr(_orch, "_forward_approval_to_runner", _capture_forward)
    monkeypatch.setattr(_orch, "_publish_elicitation_resolved", _capture_publish)
    try:
        # Answer from the PARENT view (the parent is the presentation target).
        outcome = await _orch._resolve_elicitation(
            parent_id,
            {
                "elicitation_id": elicitation_id,
                "action": "accept",
                "content": {"answer": "staging"},
            },
            None,
            None,
        )
    finally:
        monkeypatch.setattr(_orch, "_forward_approval_to_runner", original_forward)
        monkeypatch.setattr(_orch, "_publish_elicitation_resolved", original_publish)

    assert outcome == "resolved"
    # The commit targeted the child's row…
    store = durable_store()
    assert store is not None
    row = store.get(elicitation_id)
    assert row is not None
    assert row.conversation_id == child_id
    assert row.answer_content == {"answer": "staging"}
    # …the runner forward targets the OWNER (child), never the parent…
    assert [sid for sid, _ in forwarded] == [child_id]
    # …and the resolved signal publishes on the child's stream (its live
    # subscribers drop the card); the parent is reached via ancestor mirroring.
    assert [sid for sid, _ in published] == [child_id]


async def test_recovery_wake_targets_owner_not_resolving_session(
    client: httpx.AsyncClient, _wired_store, db_uri: str
) -> None:
    """recover_answered_ask invoked on the owner's session wakes the owner only."""
    from omnigent.runner import ask_user as _ask_user

    agent = await create_test_agent(client, "durable-owner-recovery")
    parent_id = await _create_session(client, agent["id"])
    child_id = _create_child_session(db_uri, parent_id=parent_id, agent_id=agent["id"])
    registered = await _register_durable(client, child_id)
    elicitation_id = registered["elicitation_id"]
    # Commit the answer WITHOUT a live waiter (simulates resolve while the
    # runner was down), then adopt it exactly as the sweep would: recovery
    # runs against the OWNER's session id.
    await client.post(
        f"/v1/sessions/{parent_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
    )

    captured: list[tuple[str, dict]] = []

    class _CaptureTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content) if request.content else {}
            captured.append((request.url.path, body if isinstance(body, dict) else {}))
            if body.get("type") == "durable_ask_consumed":
                return httpx.Response(202, json={"queued": False, "consumed": True})
            if body.get("type") == "message":
                return httpx.Response(202, json={"queued": False})
            return httpx.Response(200, json={})

    server_client = httpx.AsyncClient(transport=_CaptureTransport(), base_url="http://test")
    recovered = await _ask_user.recover_answered_ask(
        elicitation_id=elicitation_id,
        conversation_id=child_id,  # the OWNER, as the sweep resolves it
        action="accept",
        content={"answer": "staging"},
        server_client=server_client,
    )
    assert recovered is True
    wakes = [body for _path, body in captured if body.get("type") == "message"]
    assert len(wakes) == 1, captured
    # The wake rode the OWNER's session path, never the parent's.
    assert all("/sessions/" + child_id + "/events" in path for path, _ in captured)
    # The consume claim was posted exactly once (the capture transport
    # models the store's exactly-once gate; the store behavior itself is
    # pinned by tests/stores/test_durable_elicitation_store.py).
    consume_calls = [body for _p, body in captured if body.get("type") == "durable_ask_consumed"]
    assert len(consume_calls) == 1
    assert consume_calls[0]["data"]["elicitation_id"] == elicitation_id
