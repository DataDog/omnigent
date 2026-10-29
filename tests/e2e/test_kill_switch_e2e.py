"""E2E: the service kill switch cancels a real in-flight turn and recovers.

Real server and runner processes and the gated mock LLM. Only the flag backend
is replaced: the server bootstrap installs a file-backed kill-switch source, so
the test flips the flag at runtime the way an operator flips the Datadog flag.

Run: uv run --no-sync pytest tests/e2e/test_kill_switch_e2e.py -v
"""

from __future__ import annotations

import textwrap
import time
import uuid
from pathlib import Path

import httpx
import pytest

from tests._helpers.server_runner import server_runner
from tests.e2e.conftest import (
    configure_mock_llm,
    create_runner_bound_session,
    poll_session_until_terminal,
    register_inline_agent,
    release_mock_gate,
    reset_mock_llm,
    send_user_message_to_session,
)

pytestmark = pytest.mark.timeout(300)

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Installs a file-backed flag source before the CLI builds the app.
_SERVER_BOOTSTRAP = textwrap.dedent(
    """\
    import os
    from pathlib import Path

    import omnigent.server.app as app_module
    from omnigent.server.kill_switch import FlagReading, KillSwitchController

    flag_file = Path(os.environ["KILL_SWITCH_E2E_FLAG_FILE"])


    class FileFlagSource:
        def evaluate(self, last_known):
            return FlagReading(engaged=flag_file.read_text().strip() == "on")

        def close(self):
            pass


    app_module.resolve_kill_switch = lambda environ=None: KillSwitchController(
        FileFlagSource(), poll_interval_s=0.2
    )

    from omnigent.cli import main

    main()
    """
)


def _wait(check, description: str, timeout: float = 60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.2)
    raise AssertionError(f"Timed out waiting for {description}")


def _message(text: str) -> dict:
    return {
        "type": "message",
        "data": {"role": "user", "content": [{"type": "input_text", "text": text}]},
    }


def test_kill_switch_cancels_in_flight_turn_then_resumes(
    tmp_path: Path, mock_llm_server_url: str
) -> None:
    flag_file = tmp_path / "kill-switch-flag"
    flag_file.write_text("off")
    model = f"mock-kill-switch-{uuid.uuid4().hex[:6]}"
    reset_mock_llm(mock_llm_server_url)
    configure_mock_llm(
        mock_llm_server_url,
        [
            {"text": "A very long answer that never finishes...", "block": True},
            {"text": "RESUMED"},
        ],
        key=model,
    )
    env = {
        "KILL_SWITCH_E2E_FLAG_FILE": str(flag_file),
        "OPENAI_BASE_URL": f"{mock_llm_server_url}/v1",
        "OPENAI_API_KEY": "mock-key",
    }
    with server_runner(
        tmp_path / "stack",
        server_bootstrap=_SERVER_BOOTSTRAP,
        server_env=env,
        server_cwd=_REPO_ROOT,
        poll_interval=0.5,
    ) as stack:
        stack.start_runner(cwd=_REPO_ROOT, env=env)
        with httpx.Client(base_url=stack.base_url, trust_env=False, timeout=30) as client:
            agent_name = register_inline_agent(
                client,
                name=f"kill-switch-{uuid.uuid4().hex[:6]}",
                harness="openai-agents",
                model=model,
                profile="",
                prompt="You are a helpful assistant.",
                mock_llm_base_url=f"{mock_llm_server_url}/v1",
            )
            session_id = create_runner_bound_session(
                client, agent_name=agent_name, runner_id=stack.runner_id
            )
            send_user_message_to_session(client, session_id=session_id, content="Write an essay.")
            _wait(
                lambda: (
                    httpx.get(f"{mock_llm_server_url}/gate/pending", timeout=2)
                    .json()
                    .get("pending")
                ),
                "the turn to block on the mock LLM",
            )
            assert client.get(f"/v1/sessions/{session_id}").json()["status"] == "running"

            # Operator engages: the server's sweep interrupts the real runner turn.
            flag_file.write_text("on")
            _wait(lambda: client.get("/v1/kill-switch").json()["engaged"], "engage")
            # Release only after the interrupt landed, so the turn cannot complete first.
            _wait(
                lambda: client.get(f"/v1/sessions/{session_id}").json()["status"] != "running",
                "the sweep to interrupt the running turn",
            )
            release_mock_gate(mock_llm_server_url)
            _wait(
                lambda: client.get(f"/v1/sessions/{session_id}").json()["status"] == "idle",
                "the cancelled session to recover to idle",
            )
            snapshot = client.get(f"/v1/sessions/{session_id}").json()
            assert not snapshot.get("last_task_error"), snapshot
            items = client.get(f"/v1/sessions/{session_id}/items", params={"limit": 1000})
            assert "interrupted" in items.text.lower(), items.text
            assert "never finishes" not in items.text, "the blocked turn must not complete"

            refused = client.post(f"/v1/sessions/{session_id}/events", json=_message("hi"))
            assert refused.status_code == 503, refused.text
            assert refused.json()["error"]["code"] == "service_disabled"
            assert client.get("/health").json()["kill_switch"] == "engaged"

            # Operator disengages: new work is accepted and completes.
            flag_file.write_text("off")
            _wait(lambda: not client.get("/v1/kill-switch").json()["engaged"], "disengage")
            followup_id = send_user_message_to_session(
                client, session_id=session_id, content="Are you back?"
            )
            followup = poll_session_until_terminal(
                client, session_id=session_id, response_id=followup_id, timeout=120
            )
            assert followup["status"] == "completed", followup.get("error")
