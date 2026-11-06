"""Process-level proof: a durable sys_ask_user survives a runner restart.

The runner process is SIGKILLed while a hosted turn is parked on a durable
``sys_ask_user`` question (the harness turn and its parked Future die with
the process). The client stays disconnected the whole time; the answer is
submitted via the REST resolve endpoint while the runner is down. The
respawned runner adopts the answered-but-unconsumed question through its
tunnel-reconnect recovery sweep and continues the SAME logical execution:
exactly one persisted ``sys_ask_user`` tool result, exactly one completion,
the pre-question side effect never re-executed, and a stable question id.

Real server + runner + mock model, no browser. Invoke with::

    pytest tests/e2e/test_ask_user_runner_restart_e2e.py -v --timeout=600
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import httpx
import pytest

from tests._helpers.session import bundle_files, post_session_bundle
from tests.e2e.conftest import configure_mock_llm, set_fallback_mock_llm
from tests.e2e.test_subagent_wake_loss_e2e import (
    _WakeLossStack,
)

pytestmark = [pytest.mark.timeout(600, method="signal")]

_SIDE_EFFECT_OUTPUT = "side-effect-done-once"
_FINAL_REPLY = "Continuation finished after runner restart."

_AGENT_YAML = """\
spec_version: 1
name: ask_user_runner_restart_probe
prompt: |
  You deploy things. Before deploying, run the safety command, then ask the
  user which environment to deploy to, then reply with one short sentence.

executor:
  model: {model}
  config:
    harness: openai-agents
  auth:
    type: api_key
    api_key: mock-key
    base_url: {mock_base_url}

os_env:
  type: caller_process
  cwd: .
  sandbox:
    type: none
"""


class _AskUserStack(_WakeLossStack):
    """The wake-loss stack plus a runner kill/restart."""

    def kill_runner(self) -> None:
        """SIGKILL the runner and wait until the server marks it offline."""
        assert self._runner_proc is not None
        self._runner_proc.kill()
        self._runner_proc.wait(timeout=10)
        if self._runner_log_handle is not None:
            self._runner_log_handle.close()
            self._runner_log_handle = None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                status = httpx.get(
                    f"{self.base_url}/v1/runners/{self.runner_id}/status",
                    timeout=2,
                    trust_env=False,
                )
                if status.status_code == 200 and status.json().get("online") is False:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.25)
        raise RuntimeError("Runner still online after kill.")

    def start_runner(self) -> None:
        """Spawn a fresh runner with the same identity and wait for it."""
        self._spawn_runner()
        self.wait_healthy()


def _snapshot(base_url: str, session_id: str) -> dict:
    return httpx.get(f"{base_url}/v1/sessions/{session_id}", timeout=10.0, trust_env=False).json()


def _durable_pending(base_url: str, session_id: str) -> list[dict]:
    pending = _snapshot(base_url, session_id).get("pending_elicitations") or []
    return [e for e in pending if isinstance(e.get("params"), dict) and e["params"].get("durable")]


def _tool_outputs(base_url: str, session_id: str, tool_name: str) -> list[str]:
    items = httpx.get(
        f"{base_url}/v1/sessions/{session_id}/items",
        params={"limit": 200},
        timeout=10.0,
        trust_env=False,
    ).json()["data"]
    call_ids = {
        item.get("call_id") or (item.get("data") or {}).get("call_id")
        for item in items
        if item.get("type") == "function_call"
        and (item.get("name") or (item.get("data") or {}).get("name")) == tool_name
    }
    return [
        str(item.get("output") or (item.get("data") or {}).get("output") or "")
        for item in items
        if item.get("type") == "function_call_output"
        and (item.get("call_id") or (item.get("data") or {}).get("call_id")) in call_ids
    ]


def _wait(predicate, base_url: str, session_id: str, timeout_s: float, what: str):
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate(base_url, session_id)
        if last:
            return last
        time.sleep(0.5)
    raise AssertionError(f"{what} not reached within {timeout_s}s; last={last!r}")


@pytest.fixture
def stack(mock_llm_server_url: str, tmp_path: Path):
    s = _AskUserStack(mock_llm_server_url, tmp_path)
    s.start()
    try:
        yield s
    finally:
        s.teardown()


def test_durable_ask_survives_runner_restart(
    stack: _AskUserStack, mock_llm_server_url: str
) -> None:
    model = f"ask-restart-{uuid.uuid4().hex[:8]}"
    configure_mock_llm(
        mock_llm_server_url,
        [
            {
                "tool_calls": [
                    {
                        "call_id": "call_side_effect",
                        "name": "sys_os_shell",
                        "arguments": json.dumps({"command": f"echo {_SIDE_EFFECT_OUTPUT}"}),
                    }
                ]
            },
            {
                "tool_calls": [
                    {
                        "call_id": "call_ask_1",
                        "name": "sys_ask_user",
                        "arguments": json.dumps(
                            {
                                "question": "Which environment should I deploy to?",
                                "options": ["staging", "production"],
                            }
                        ),
                    }
                ]
            },
        ],
        key=model,
    )
    set_fallback_mock_llm(mock_llm_server_url, model, _FINAL_REPLY)

    yaml_text = _AGENT_YAML.format(
        model=model, mock_base_url=json.dumps(f"{mock_llm_server_url}/v1")
    )
    create = post_session_bundle(
        httpx.post,
        f"{stack.base_url}/v1/sessions",
        bundle_files({"config.yaml": yaml_text.encode()}),
        timeout=30.0,
        trust_env=False,
    )
    create.raise_for_status()
    session_id = str(create.json()["session_id"])
    bind = httpx.patch(
        f"{stack.base_url}/v1/sessions/{session_id}",
        json={"runner_id": stack.runner_id},
        timeout=10.0,
        trust_env=False,
    )
    bind.raise_for_status()

    posted = httpx.post(
        f"{stack.base_url}/v1/sessions/{session_id}/events",
        json={
            "type": "message",
            "data": {
                "role": "user",
                "content": [{"type": "input_text", "text": "Deploy please"}],
            },
        },
        timeout=30.0,
        trust_env=False,
    )
    posted.raise_for_status()

    # The side effect runs, then the durable question parks the turn.
    def _side_effect_done(base_url: str, session_id: str):
        outputs = _tool_outputs(base_url, session_id, "sys_os_shell")
        return outputs if any(_SIDE_EFFECT_OUTPUT in o for o in outputs) else None

    _wait(_side_effect_done, stack.base_url, session_id, 60, "side effect")
    pending = _wait(_durable_pending, stack.base_url, session_id, 60, "durable pending question")
    question_id = pending[0]["elicitation_id"]
    options = pending[0]["params"]["requestedSchema"]["properties"]["answer"]["enum"]
    assert options == ["staging", "production"], options

    # The runner dies mid-question; the client (this test) stays "disconnected"
    # throughout — the only interaction left is the REST resolve below.
    stack.kill_runner()

    # Answer while the runner is down. The server commits the answer
    # (commit-before-publish) even though delivery to the dead runner fails.
    resolved = httpx.post(
        f"{stack.base_url}/v1/sessions/{session_id}/elicitations/{question_id}/resolve",
        json={"action": "accept", "content": {"answer": "staging"}},
        timeout=30.0,
        trust_env=False,
    )
    resolved.raise_for_status()

    # Respawn the runner with the SAME identity: the reconnect recovery
    # sweep adopts the answered-but-unconsumed ask and wakes the session.
    stack.start_runner()

    def _completed(base_url: str, session_id: str):
        snap = _snapshot(base_url, session_id)
        if snap.get("status") in {"idle", "completed", "succeeded"}:
            items = httpx.get(
                f"{base_url}/v1/sessions/{session_id}/items",
                params={"limit": 200},
                timeout=10.0,
                trust_env=False,
            ).json()["data"]
            text = json.dumps(items)
            if _FINAL_REPLY in text:
                return text
        return None

    final_items = _wait(_completed, stack.base_url, session_id, 120, "continuation completion")

    # Exactly one continuation: one ask output carrying the answer, one
    # completion, and the pre-question side effect was never re-executed.
    # The ask's own function_call item dies with the crashed turn; the
    # recovered tool result is keyed to the paused-call checkpoint's call id.
    items = httpx.get(
        f"{stack.base_url}/v1/sessions/{session_id}/items",
        params={"limit": 200},
        timeout=10.0,
        trust_env=False,
    ).json()["data"]
    # The items API flattens item fields to the top level.
    ask_outputs = [
        str(item.get("output") or "")
        for item in items
        if item.get("type") == "function_call_output" and item.get("call_id") == "call_ask_1"
    ]
    if len(ask_outputs) != 1:
        raise AssertionError(
            f"expected exactly one sys_ask_user output: {ask_outputs}; "
            f"items={json.dumps(items)[:4000]}"
        )
    assert "staging" in ask_outputs[0]
    assert final_items.count(_FINAL_REPLY) == 1, (
        f"expected exactly one completion; items={final_items[:2000]}"
    )
    shell_outputs = _tool_outputs(stack.base_url, session_id, "sys_os_shell")
    assert len(shell_outputs) == 1, f"pre-question side effect re-executed: {shell_outputs}"
    # The question id stayed stable end-to-end.
    body = httpx.get(
        f"{stack.base_url}/v1/sessions/{session_id}/elicitations/{question_id}",
        timeout=10.0,
        trust_env=False,
    ).json()
    assert body["status"] == "resolved"
    assert body["action"] == "accept"


def _run_ask_turn(stack: _AskUserStack, model: str, yaml_text: str, prompt: str) -> str:
    """Start a session, run the prompt, park it on the durable ask; return (session, eid)."""
    create = post_session_bundle(
        httpx.post,
        f"{stack.base_url}/v1/sessions",
        bundle_files({"config.yaml": yaml_text.encode()}),
        timeout=30.0,
        trust_env=False,
    )
    create.raise_for_status()
    session_id = str(create.json()["session_id"])
    bind = httpx.patch(
        f"{stack.base_url}/v1/sessions/{session_id}",
        json={"runner_id": stack.runner_id},
        timeout=10.0,
        trust_env=False,
    )
    bind.raise_for_status()
    posted = httpx.post(
        f"{stack.base_url}/v1/sessions/{session_id}/events",
        json={
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        },
        timeout=30.0,
        trust_env=False,
    )
    posted.raise_for_status()
    pending = _wait(_durable_pending, stack.base_url, session_id, 60, "durable pending question")
    return session_id, pending[0]["elicitation_id"]


def test_durable_decline_reaches_the_model(stack: _AskUserStack, mock_llm_server_url: str) -> None:
    """A decline must arrive as the structured tool result, not kill the turn."""
    model = f"ask-decline-{uuid.uuid4().hex[:8]}"
    configure_mock_llm(
        mock_llm_server_url,
        [
            {
                "tool_calls": [
                    {
                        "call_id": "call_ask_decline",
                        "name": "sys_ask_user",
                        "arguments": json.dumps(
                            {
                                "question": "Deploy to production?",
                                "options": ["yes", "no"],
                            }
                        ),
                    }
                ]
            }
        ],
        key=model,
    )
    set_fallback_mock_llm(
        mock_llm_server_url, model, "DECLINE_PATH_DONE production deployment refused."
    )
    yaml_text = _AGENT_YAML.format(
        model=model, mock_base_url=json.dumps(f"{mock_llm_server_url}/v1")
    )
    session_id, elicitation_id = _run_ask_turn(stack, model, yaml_text, "Deploy please")

    # Decline through the real resolve endpoint with the runner ALIVE — the
    # structured {"action": "decline"} must reach the parked waiter and flow
    # to the model as the tool result; the legacy decline→interrupt gate must
    # NOT cancel the parked turn first.
    resolved = httpx.post(
        f"{stack.base_url}/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "decline"},
        timeout=30.0,
        trust_env=False,
    )
    resolved.raise_for_status()

    def _completed(base_url: str, session_id: str):
        snap = _snapshot(base_url, session_id)
        if snap.get("status") in {"idle", "completed", "succeeded"}:
            items = httpx.get(
                f"{base_url}/v1/sessions/{session_id}/items",
                params={"limit": 200},
                timeout=10.0,
                trust_env=False,
            ).json()["data"]
            text = json.dumps(items)
            if "DECLINE_PATH_DONE" in text:
                return text
        return None

    final_items = _wait(
        _completed, stack.base_url, session_id, 120, "decline continuation completion"
    )

    # The structured non-answer reached the model: exactly one decline tool
    # result, the session completed (not failed/interrupted), one completion.
    ask_outputs = [
        str(item.get("output") or "")
        for item in httpx.get(
            f"{stack.base_url}/v1/sessions/{session_id}/items",
            params={"limit": 200},
            timeout=10.0,
            trust_env=False,
        ).json()["data"]
        if item.get("type") == "function_call_output" and item.get("call_id") == "call_ask_decline"
    ]
    assert ask_outputs == ['{"action": "decline"}'], ask_outputs
    assert final_items.count("DECLINE_PATH_DONE") == 1
    snap = _snapshot(stack.base_url, session_id)
    assert snap.get("status") == "idle", snap.get("last_task_error")


def test_interrupt_while_pending_stays_stopped(
    stack: _AskUserStack, mock_llm_server_url: str
) -> None:
    """An explicit interrupt stops the turn; the canceled ask is never re-woken."""
    model = f"ask-interrupt-{uuid.uuid4().hex[:8]}"
    configure_mock_llm(
        mock_llm_server_url,
        [
            {
                "tool_calls": [
                    {
                        "call_id": "call_ask_interrupt",
                        "name": "sys_ask_user",
                        "arguments": json.dumps(
                            {"question": "Proceed?", "options": ["yes", "no"]}
                        ),
                    }
                ]
            }
        ],
        key=model,
    )
    # A DIFFERENT response queued behind the cancel — if recovery resurrected
    # the canceled execution, the model would run again and this would appear.
    set_fallback_mock_llm(mock_llm_server_url, model, "RESURRECTED_AFTER_INTERRUPT")
    yaml_text = _AGENT_YAML.format(
        model=model, mock_base_url=json.dumps(f"{mock_llm_server_url}/v1")
    )
    session_id, elicitation_id = _run_ask_turn(stack, model, yaml_text, "Deploy please")

    # Explicit interrupt while the question is pending.
    interrupted = httpx.post(
        f"{stack.base_url}/v1/sessions/{session_id}/events",
        json={"type": "interrupt"},
        timeout=30.0,
        trust_env=False,
    )
    interrupted.raise_for_status()

    # The canceled row is terminal and invisible to recovery + snapshots.
    deadline = time.monotonic() + 30
    row = None
    while time.monotonic() < deadline:
        resp = httpx.get(
            f"{stack.base_url}/v1/sessions/{session_id}/elicitations/{elicitation_id}",
            timeout=10.0,
            trust_env=False,
        )
        body = resp.json()
        if body.get("status") == "resolved":
            row = body
            break
        time.sleep(0.5)
    assert row is not None, "interrupt must cancel the pending question"
    assert row["action"] == "cancel"
    listing = httpx.get(
        f"{stack.base_url}/v1/sessions/{session_id}/durable_elicitations/unconsumed",
        timeout=10.0,
        trust_env=False,
    ).json()["unconsumed"]
    assert all(r["elicitation_id"] != elicitation_id for r in listing), (
        "canceled rows must never be adopted by recovery"
    )
    snap = _snapshot(stack.base_url, session_id)
    ids = [e.get("elicitation_id") for e in (snap.get("pending_elicitations") or [])]
    assert elicitation_id not in ids, "canceled rows must not re-render as cards"

    # No resurrection: give the runner ample time to (wrongly) continue; the
    # queued RESURRECTED_AFTER_INTERRUPT reply must never appear, and the
    # session must not start a new turn.
    time.sleep(15)
    snap = _snapshot(stack.base_url, session_id)
    items = httpx.get(
        f"{stack.base_url}/v1/sessions/{session_id}/items",
        params={"limit": 200},
        timeout=10.0,
        trust_env=False,
    ).json()["data"]
    text = json.dumps(items)
    assert "RESURRECTED_AFTER_INTERRUPT" not in text, (
        "the canceled execution was resurrected after an explicit interrupt"
    )
    assert snap.get("status") in {"failed", "idle", "interrupted", "cancelled"}, snap.get("status")


def test_parent_view_answer_resumes_child_exactly_once(
    stack: _AskUserStack, mock_llm_server_url: str
) -> None:
    """A parent-view answer to a child's question resumes the CHILD, never the parent."""
    model = f"ask-child-{uuid.uuid4().hex[:8]}"
    configure_mock_llm(
        mock_llm_server_url,
        [
            {
                "tool_calls": [
                    {
                        "call_id": "call_ask_child",
                        "name": "sys_ask_user",
                        "arguments": json.dumps(
                            {"question": "Proceed?", "options": ["yes", "no"]}
                        ),
                    }
                ]
            }
        ],
        key=model,
    )
    set_fallback_mock_llm(mock_llm_server_url, model, "CHILD_RESUMED_ONCE deployment confirmed.")
    yaml_text = _AGENT_YAML.format(
        model=model, mock_base_url=json.dumps(f"{mock_llm_server_url}/v1")
    )

    # Parent session through the normal flow.
    create = post_session_bundle(
        httpx.post,
        f"{stack.base_url}/v1/sessions",
        bundle_files({"config.yaml": yaml_text.encode()}),
        timeout=30.0,
        trust_env=False,
    )
    create.raise_for_status()
    parent_id = str(create.json()["session_id"])
    bind = httpx.patch(
        f"{stack.base_url}/v1/sessions/{parent_id}",
        json={"runner_id": stack.runner_id},
        timeout=10.0,
        trust_env=False,
    )
    bind.raise_for_status()

    # Child session row owned by the parent, bound to the same runner.
    from omnigent.stores.conversation_store.sqlalchemy_store import (
        SqlAlchemyConversationStore,
    )

    agent_id = httpx.get(
        f"{stack.base_url}/v1/sessions/{parent_id}/agent",
        timeout=10.0,
        trust_env=False,
    ).json()["id"]
    child_id = (
        SqlAlchemyConversationStore(f"sqlite:///{stack._db_path}")
        .create_conversation(
            kind="sub_agent",
            title="child:ask",
            parent_conversation_id=parent_id,
            agent_id=agent_id,
        )
        .id
    )
    bind_child = httpx.patch(
        f"{stack.base_url}/v1/sessions/{child_id}",
        json={"runner_id": stack.runner_id},
        timeout=10.0,
        trust_env=False,
    )
    bind_child.raise_for_status()

    # Park the durable ask on the CHILD.
    posted = httpx.post(
        f"{stack.base_url}/v1/sessions/{child_id}/events",
        json={
            "type": "message",
            "data": {"role": "user", "content": [{"type": "input_text", "text": "go"}]},
        },
        timeout=30.0,
        trust_env=False,
    )
    posted.raise_for_status()
    pending = _wait(_durable_pending, stack.base_url, child_id, 60, "child durable ask")
    elicitation_id = pending[0]["elicitation_id"]

    # No live waiter: kill the runner, then answer from the PARENT view.
    stack.kill_runner()
    resolved = httpx.post(
        f"{stack.base_url}/v1/sessions/{parent_id}/elicitations/{elicitation_id}/resolve",
        json={"action": "accept", "content": {"answer": "yes"}},
        timeout=30.0,
        trust_env=False,
    )
    resolved.raise_for_status()

    # Respawn: recovery must resume the CHILD exactly once; the parent is
    # untouched (no continuation wake, no completion).
    stack.start_runner()

    def _child_done(base_url: str, session_id: str):
        snap = _snapshot(base_url, child_id)
        if snap.get("status") in {"idle", "completed", "succeeded"}:
            items = httpx.get(
                f"{base_url}/v1/sessions/{child_id}/items",
                params={"limit": 200},
                timeout=10.0,
                trust_env=False,
            ).json()["data"]
            text = json.dumps(items)
            if "CHILD_RESUMED_ONCE" in text:
                return text
        return None

    child_items = _wait(_child_done, stack.base_url, child_id, 120, "child continuation")
    assert child_items.count("CHILD_RESUMED_ONCE") == 1

    # Exactly one tool result on the child, keyed to the parked call.
    child_ask_outputs = [
        item
        for item in httpx.get(
            f"{stack.base_url}/v1/sessions/{child_id}/items",
            params={"limit": 200},
            timeout=10.0,
            trust_env=False,
        ).json()["data"]
        if item.get("type") == "function_call_output" and item.get("call_id") == "call_ask_child"
    ]
    assert len(child_ask_outputs) == 1

    # The parent stream/history carries NO continuation wake or completion.
    parent_items = httpx.get(
        f"{stack.base_url}/v1/sessions/{parent_id}/items",
        params={"limit": 200},
        timeout=10.0,
        trust_env=False,
    ).json()["data"]
    parent_text = json.dumps(parent_items)
    assert "CHILD_RESUMED_ONCE" not in parent_text
    assert "sys_ask_user continuation" not in parent_text

    # The child row was consumed exactly once: nothing left for re-adoption.
    listing = httpx.get(
        f"{stack.base_url}/v1/sessions/{child_id}/durable_elicitations/unconsumed",
        timeout=10.0,
        trust_env=False,
    ).json()["unconsumed"]
    assert all(r["elicitation_id"] != elicitation_id for r in listing)
