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
