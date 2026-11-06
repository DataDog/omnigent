"""Runner-side ``sys_ask_user`` executor tests.

Drives :func:`omnigent.runner.ask_user.execute_ask_user` against a fake
server (httpx MockTransport standing in for the Omnigent server) and a
fake harness, covering: registration + park + verdict, duplicate-answer
adoption after a server reconnect, malformed-argument rejection, explicit
interrupt canceling the pending question server-side, and consumption
bookkeeping after delivery.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from omnigent.runner import ask_user, pending_approvals
from omnigent.tools.builtins.sys_ask_user import format_tool_result

hexid = lambda c: ((c * 33)[:32]).replace("p", "a").replace("q", "b")  # noqa: E731


class _FakeServer:
    """In-memory Omnigent-server stand-in for the durable_ask surface."""

    def __init__(self) -> None:
        self.questions: dict[str, dict] = {}
        self.durable_state: dict[str, dict] = {}
        self.consumed: list[str] = []
        self.canceled: list[str] = []
        self.wakes: list[str] = []
        self.server_down = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if request.method == "POST" and path.endswith("/events"):
            event_type = body.get("type")
            data = body.get("data") or {}
            if event_type == "durable_ask":
                if self.server_down:
                    return httpx.Response(503)
                args = data.get("arguments") or {}
                if not isinstance(args.get("question"), str) or not args["question"].strip():
                    return httpx.Response(
                        200, json={"error": "question must be a non-empty string"}
                    )
                elicitation_id = hexid("e") + str(len(self.questions))
                self.questions[elicitation_id] = data
                return httpx.Response(
                    202, json={"queued": False, "elicitation_id": elicitation_id}
                )
            if event_type == "approval":
                self.canceled.append(data.get("elicitation_id"))
                self.durable_state[data["elicitation_id"]] = {
                    "status": "resolved",
                    "action": data.get("action"),
                    "content": data.get("content"),
                }
                return httpx.Response(202, json={"queued": False})
            if event_type == "durable_ask_consumed":
                # Model the store's exactly-once gate: only the FIRST claim
                # of a given ask transitions it; later claims ack consumed=False.
                first_claim = data["elicitation_id"] not in self.consumed
                self.consumed.append(data["elicitation_id"])
                return httpx.Response(202, json={"queued": False, "consumed": first_claim})
            if event_type == "message":
                self.wakes.append(data["content"][0]["text"])
                return httpx.Response(202, json={"queued": False})
            return httpx.Response(400, json={"error": f"unexpected {event_type}"})
        if request.method == "GET" and "/elicitations/" in path:
            elicitation_id = path.rsplit("/", 1)[-1]
            state = self.durable_state.get(elicitation_id)
            if state is not None:
                return httpx.Response(200, json=state)
            return httpx.Response(200, json={"status": "pending"})
        return httpx.Response(404)


@pytest.fixture(autouse=True)
def _clean_registries():
    ask_user.reset_for_tests()
    pending_approvals.reset_for_tests()
    yield
    ask_user.reset_for_tests()
    pending_approvals.reset_for_tests()


@pytest.fixture()
def fake_server() -> _FakeServer:
    return _FakeServer()


def _clients(fake_server: _FakeServer) -> tuple[httpx.AsyncClient, httpx.AsyncClient]:
    server_client = httpx.AsyncClient(
        transport=httpx.MockTransport(fake_server.handler), base_url="https://server.test"
    )
    harness_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _req: httpx.Response(204)),
        base_url="https://harness.test",
    )
    return server_client, harness_client


def _run(fake_server: _FakeServer, args: dict, call_id: str = "call_1"):
    server_client, harness_client = _clients(fake_server)
    return ask_user.execute_ask_user(
        args=args,
        call_id=call_id,
        conversation_id=hexid("c"),
        server_client=server_client,
        harness_client=harness_client,
    )


async def test_register_park_and_verdict(fake_server: _FakeServer) -> None:
    task = asyncio.create_task(
        _run(fake_server, {"question": "Env?", "options": ["staging", "prod"]})
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    (elicitation_id,) = fake_server.questions.keys()
    assert pending_approvals.resolve(elicitation_id, approved=True, content={"answer": "staging"})
    output = json.loads(await asyncio.wait_for(task, timeout=5))
    assert output == {"action": "accept", "answer": "staging"}
    # The invocation id is the call id: replay-dedup is stable.
    registered = fake_server.questions[elicitation_id]
    assert registered["invocation_id"] == "call_1"


async def test_decline_returns_non_answer(fake_server: _FakeServer) -> None:
    task = asyncio.create_task(_run(fake_server, {"question": "Proceed?"}))
    await asyncio.sleep(0.01)
    (elicitation_id,) = fake_server.questions.keys()
    pending_approvals.resolve(elicitation_id, approved=False)
    output = json.loads(await asyncio.wait_for(task, timeout=5))
    assert output == {"action": "decline"}


async def test_reconnect_adopts_answer_submitted_while_offline(
    fake_server: _FakeServer,
) -> None:
    """An answer committed while the tunnel was down is adopted, not re-asked."""
    task = asyncio.create_task(
        _run(fake_server, {"question": "Env?", "options": ["staging", "prod"]})
    )
    await asyncio.sleep(0.02)
    (elicitation_id,) = fake_server.questions.keys()
    # The verdict is committed server-side while the runner is not parked on
    # a live Future path (simulate: resolve fired ServerReconnected instead).
    fut = pending_approvals._pending.get(elicitation_id)
    assert fut is not None
    fut.set_exception(pending_approvals.ServerReconnected())
    fake_server.durable_state[elicitation_id] = {
        "status": "resolved",
        "action": "accept",
        "content": {"answer": "prod"},
    }
    output = json.loads(await asyncio.wait_for(task, timeout=5))
    assert output == {"action": "accept", "answer": "prod"}
    assert len(fake_server.questions) == 1, "the question must not be re-registered"


async def test_malformed_arguments_fail_before_registration(
    fake_server: _FakeServer,
) -> None:
    output = json.loads(
        await asyncio.wait_for(
            asyncio.create_task(_run(fake_server, {"question": "   "})), timeout=5
        )
    )
    assert output.get("error") == "question must be a non-empty string"
    assert fake_server.questions == {}


async def test_interrupt_cancels_the_pending_question(
    fake_server: _FakeServer,
) -> None:
    task = asyncio.create_task(_run(fake_server, {"question": "Proceed?"}))
    await asyncio.sleep(0.02)
    (elicitation_id,) = fake_server.questions.keys()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fake_server.canceled == [elicitation_id]
    state = fake_server.durable_state[elicitation_id]
    assert state["action"] == "cancel"


async def test_consumption_bookkeeping_after_delivery(
    fake_server: _FakeServer,
) -> None:
    task = asyncio.create_task(_run(fake_server, {"question": "Env?", "options": ["a", "b"]}))
    await asyncio.sleep(0.02)
    (elicitation_id,) = fake_server.questions.keys()
    pending_approvals.resolve(elicitation_id, approved=True, content={"answer": "a"})
    await asyncio.wait_for(task, timeout=5)
    # The id is still tracked for the delivery-confirm step; marking consumed
    # posts the durable_ask_consumed event and clears the entry.
    server_client, _harness_client = _clients(fake_server)
    await ask_user.mark_consumed_if_ask(
        tool_name="sys_ask_user",
        call_id="call_1",
        conversation_id=hexid("c"),
        server_client=server_client,
    )
    assert fake_server.consumed == [elicitation_id]
    assert "call_1" not in ask_user._active_asks
    # Non-ask tools never post consumption.
    await ask_user.mark_consumed_if_ask(
        tool_name="sys_os_shell",
        call_id="call_2",
        conversation_id=hexid("c"),
        server_client=server_client,
    )
    assert fake_server.consumed == [elicitation_id]


def test_result_format_never_invents_an_answer() -> None:
    assert format_tool_result(action="decline") == json.dumps({"action": "decline"})


class TestRecovery:
    """Runner-restart recovery claims exactly-once, then wakes once."""

    async def test_recovery_claims_then_wakes(self, fake_server: _FakeServer) -> None:
        server_client, _ = _clients(fake_server)
        fake_server.durable_state[hexid("a")] = {
            "status": "resolved",
            "action": "accept",
            "content": {"answer": "staging"},
        }
        # Unconsumed: the consume event succeeds.
        recovered = await ask_user.recover_answered_ask(
            elicitation_id=hexid("a"),
            conversation_id=hexid("c"),
            action="accept",
            content={"answer": "staging"},
            server_client=server_client,
        )
        assert recovered is True
        assert fake_server.consumed == [hexid("a")]
        # The wake rode exactly one hidden meta message.
        assert len(fake_server.wakes) == 1
        assert "answered: 'staging'" in fake_server.wakes[0]
        assert "sys_ask_user" in fake_server.wakes[0]

    async def test_duplicate_recovery_is_a_noop(self, fake_server: _FakeServer) -> None:
        server_client, _ = _clients(fake_server)
        fake_server.durable_state[hexid("a")] = {
            "status": "resolved",
            "action": "accept",
            "content": {"answer": "staging"},
        }
        first = await ask_user.recover_answered_ask(
            elicitation_id=hexid("a"),
            conversation_id=hexid("c"),
            action="accept",
            content={"answer": "staging"},
            server_client=server_client,
        )
        second = await ask_user.recover_answered_ask(
            elicitation_id=hexid("a"),
            conversation_id=hexid("c"),
            action="accept",
            content={"answer": "staging"},
            server_client=server_client,
        )
        assert first is True and second is False
        # The second recovery claimed nothing, so it never re-attempted the
        # wake — exactly-once at the recovery layer too (the store's consume
        # gate enforces the same property behind the real server).
        assert len(fake_server.wakes) == 1
