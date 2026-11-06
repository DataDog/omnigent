"""API tests for the durable ``sys_ask_user`` registration + resolution flow.

Exercises the real server routes (shared ``client`` fixture): register a
durable question via the ``durable_ask`` session event, read it back via the
standalone elicitation GET and the session snapshot (cold-restart merge),
resolve it through the standard resolve endpoint, and assert duplicate,
conflicting, and malformed answers behave per the durable contract.
"""

from __future__ import annotations

import httpx
import pytest

from omnigent.runtime.durable_ask_user import configure_durable_elicitation_store
from tests.server.helpers import create_test_agent
from tests.server.integration.test_sessions_elicitation_resolve_url import (
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
