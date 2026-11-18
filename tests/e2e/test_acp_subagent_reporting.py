"""Full server/runner/process coverage for standard ACP child reporting.

Current implementation:

    pytest tests/e2e/test_acp_subagent_reporting.py -v --timeout=180

Protocol-only negative control against an older server and runner install:

    OMNIGENT_ACP_REPORTING_E2E_BASELINE=1 \
    OMNIGENT_COMPAT_SERVER_PYTHON=/path/to/baseline/bin/python \
    OMNIGENT_COMPAT_RUNNER_PYTHON=/path/to/baseline/bin/python \
        pytest tests/e2e/test_acp_subagent_reporting.py -v --timeout=180

The baseline uses the same raw standard ACP process and input. It must
initialize, return the parent reply, and reach idle before the test checks that
the protocol-only implementation did not expose child sessions.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import psutil
import pytest
import yaml

from omnigent.host.daemon_lifecycle import daemon_record_path
from omnigent.runner.identity import OMNIGENT_INTERNAL_WS_ORIGIN
from tests._helpers.live_server import terminate_process
from tests._helpers.polling import poll_until
from tests._helpers.server_runner import server_runner
from tests._helpers.session import (
    bind_session_runner,
    bundle_files,
    post_session_bundle,
    send_user_text,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PARENT_REPLY = "STANDARD_PARENT_REPLY_FINISHED"
_CHILD_OUTPUT = {
    "one": "standard child one result",
    "two": "standard child two result",
}
_HEALTH_TIMEOUT_S = 60.0
_TURN_TIMEOUT_S = 45.0
_CHILD_TIMEOUT_S = 30.0

_REPORT_REPLAY_SERVER_BOOTSTRAP = r"""
import asyncio
import hashlib
import json
import os
from pathlib import Path

import omnigent.server.app as server_app


class ReportResponseFaultMiddleware:
    def __init__(self, app):
        self.app = app
        self.state_dir = Path(os.environ["OMNIGENT_TEST_ACP_REPORT_FAULT_DIR"])
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.held = self.state_dir / "held.json"
        self.release = self.state_dir / "release"
        self.attempts = self.state_dir / "attempts.jsonl"

    async def __call__(self, scope, receive, send):
        if (
            scope.get("type") != "http"
            or scope.get("method") != "POST"
            or not scope.get("path", "").endswith("/events")
        ):
            await self.app(scope, receive, send)
            return

        request_chunks = []
        response_messages = []

        async def capture_receive():
            message = await receive()
            if message.get("type") == "http.request":
                request_chunks.append(message.get("body", b""))
            return message

        async def capture_send(message):
            response_messages.append(message)

        await self.app(scope, capture_receive, capture_send)
        raw_body = b"".join(request_chunks)
        try:
            envelope = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            envelope = None
        report = envelope.get("data") if isinstance(envelope, dict) else None
        is_report = (
            isinstance(envelope, dict)
            and envelope.get("type") == "external_acp_session_report"
            and isinstance(report, dict)
        )
        start = next(
            (
                message
                for message in response_messages
                if message.get("type") == "http.response.start"
            ),
            None,
        )
        status = start.get("status") if isinstance(start, dict) else None
        if not is_report or not isinstance(status, int) or not 200 <= status < 300:
            for message in response_messages:
                await send(message)
            return

        response_body = b"".join(
            message.get("body", b"")
            for message in response_messages
            if message.get("type") == "http.response.body"
        )
        try:
            committed_response = json.loads(response_body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            committed_response = None
        record = {
            "envelope": envelope,
            "raw_body": raw_body.decode("utf-8"),
            "wire_payload_sha256": hashlib.sha256(raw_body).hexdigest(),
            "source_id": report.get("source_id"),
            "sequence": report.get("sequence"),
            "reported_payload_hash": report.get("payload_hash"),
            "committed_status": status,
            "committed_response": committed_response,
        }
        with self.attempts.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, sort_keys=True) + "\n")
            output.flush()

        should_hold = report.get("kind") == "association.patch" and not self.held.exists()
        if should_hold:
            self.held.write_text(json.dumps(record, sort_keys=True))

            async def wait_for_release():
                while not self.release.exists():
                    await asyncio.sleep(0.02)

            await asyncio.wait_for(wait_for_release(), timeout=60.0)
            headers = [
                (key, value)
                for key, value in start.get("headers", [])
                if key.lower() not in {b"content-length", b"content-type"}
            ]
            await send(
                {
                    **start,
                    "status": 503,
                    "headers": [*headers, (b"content-type", b"text/plain")],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b"injected post-commit report response loss",
                    "more_body": False,
                }
            )
            return

        for message in response_messages:
            await send(message)


real_create_app = server_app.create_app


def create_app(*args, **kwargs):
    app = real_create_app(*args, **kwargs)
    app.add_middleware(ReportResponseFaultMiddleware)
    return app


server_app.create_app = create_app
from omnigent.cli import main

main()
"""


_STANDARD_ACP_AGENT = r"""
import json, os, sys, threading, time
from pathlib import Path

state = Path(sys.argv[1])
state.mkdir(parents=True, exist_ok=True)
(state / "agent.pid").write_text(str(os.getpid()))
root_session_id = "standard-root-" + str(os.getpid())
output_lock = threading.Lock()
prompt_count = 0
standard_subagents_enabled = False

def send(value):
    with output_lock:
        with (state / "wire.jsonl").open("a") as trace:
            trace.write(json.dumps({"direction":"agent_to_client", "message":value},
                                   separators=(",", ":")) + "\n")
            trace.flush()
        sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
        sys.stdout.flush()

def notify(session_id, update):
    send({"jsonrpc":"2.0", "method":"session/update",
          "params":{"sessionId":session_id, "update":update}})

def child_work(child):
    if child["key"] == "one":
        notify(child["session_id"], {
            "sessionUpdate":"session_info_update",
            "title":"Native worker title",
        })
        notify(child["session_id"], {
            "sessionUpdate":"usage_update", "used":5, "size":200000,
        })
        notify(child["session_id"], {
            "sessionUpdate":"config_option_update",
            "configOptions":[{
                "id":"thinking", "category":"mode", "currentValue":"high",
                "options":[{"value":"low"}, {"value":"high"}],
            }],
        })
        notify(child["session_id"], {
            "sessionUpdate":"current_mode_update", "currentModeId":"plan",
        })
        notify(child["session_id"], {
            "sessionUpdate":"plan",
            "entries":[{
                "content":"Inspect the fixture", "priority":"high", "status":"in_progress",
            }],
        })
        notify(child["session_id"], {
            "sessionUpdate":"plan_removed", "planId":"obsolete-plan",
        })
        notify(child["session_id"], {
            "sessionUpdate":"plan_update",
            "plan":{
                "type":"items", "planId":"worker-plan",
                "entries":[{
                    "content":"Inspect the fixture", "priority":"high", "status":"completed",
                }],
            },
        })
        notify(child["session_id"], {
            "sessionUpdate":"usage_update", "used":8, "size":200000,
        })
        notify(child["session_id"], {
            "sessionUpdate":"config_option_update", "configOptions":None,
        })
    gate = state / ("start-child-" + child["key"])
    wait_for_file(gate, "child start gate", timeout=120)
    gate.unlink()
    if child["key"] == "one":
        notify(child["session_id"], {
            "sessionUpdate":"session_message",
            "messageId":"native-directed-message-one",
            "senderSessionId":child["session_id"],
            "recipientSessionId":"standard-child-two",
            "senderMessageId":"native-sender-message-one",
            "recipientMessageId":"native-recipient-message-two",
            "content":[{"type":"text", "text":"Directed hello"}],
        })
        notify(child["session_id"], {
            "sessionUpdate":"session_message_chunk",
            "messageId":"native-directed-message-one",
            "content":{"type":"text", "text":" with a chunk."},
        })
    permission_id = "permission-" + child["key"]
    permission_params = {
        "sessionId":child["session_id"], "toolCall":{
            "toolCallId":"tool-" + child["key"], "title":child["tool"],
            "rawInput":{"task":child["task"]},
        }, "options":[
            {"optionId":"allow-once", "kind":"allow_once"},
            {"optionId":"reject-once", "kind":"reject_once"},
        ],
    }
    if (state / "hostile-approval-timeout").exists():
        permission_params.update({
            "policy_name":"native-long-timeout-policy",
            "ask_timeout":300,
            "timeout_s":300,
        })
    send({"jsonrpc":"2.0", "id":permission_id, "method":"session/request_permission",
          "params":permission_params})
    permission_reply = state / (permission_id + "-reply.json")
    wait_for_file(permission_reply, "permission response")
    permission_result = json.loads(permission_reply.read_text())
    (state / ("permission-reply-" + child["key"] + ".json")).write_text(
        json.dumps(permission_result)
    )
    permission_outcome = permission_result.get("result", {}).get("outcome", {})
    if permission_outcome.get("optionId") not in {"allow-once", "allow-always"}:
        notify(root_session_id, {
            "sessionUpdate":"subagent_update",
            "sessionId":child["session_id"],
            "state":{"state":"idle"},
        })
        (state / ("permission-denied-" + child["key"])).touch()
        (state / ("finished-child-" + child["key"])).touch()
        return
    fs_id = "fs-write-" + child["key"]
    send({"jsonrpc":"2.0", "id":fs_id, "method":"fs/write_text_file",
          "params":{"sessionId":child["session_id"],
                    "path":str(state / ("child-" + child["key"] + ".txt")),
                    "content":child["output"]}})
    fs_reply = state / (fs_id + "-reply.json")
    wait_for_file(fs_reply, "filesystem response")
    (state / ("fs-reply-" + child["key"] + ".json")).write_text(fs_reply.read_text())
    notify(child["session_id"], {
        "sessionUpdate":"agent_thought_chunk",
        "content":{"type":"text", "text":child["thought"]},
    })
    notify(child["session_id"], {
        "sessionUpdate":"tool_call",
        "toolCallId":"tool-" + child["key"],
        "title":child["tool"],
        "kind":"other",
        "rawInput":{"task":child["task"]},
        "status":"in_progress",
        **({"content":[{"type":"session", "sessionId":"standard-child-two"}]}
           if child["key"] == "one" else {}),
    })
    notify(child["session_id"], {
        "sessionUpdate":"tool_call_update",
        "toolCallId":"tool-" + child["key"],
        "status":"completed",
        "rawOutput":child["output"],
    })
    notify(child["session_id"], {
        "sessionUpdate":"agent_message_chunk",
        "content":{"type":"text", "text":child["output"]},
    })
    if child["key"] == "one":
        notify(child["session_id"], {
            "sessionUpdate":"subagent_update",
            "sessionId":"standard-grandchild-one",
            "state":{"state":"idle"},
        })
    notify(root_session_id, {
        "sessionUpdate":"subagent_update",
        "sessionId":child["session_id"],
        "state":{"state":"idle"},
    })
    if child["key"] == "one":
        notify(root_session_id, {
            "sessionUpdate":"subagent_update",
            "sessionId":child["session_id"],
            "title":"Reused worker",
            "description":"same native child, later patch",
            "state":{"state":"idle"},
        })
    (state / ("finished-child-" + child["key"])).touch()

def terminal_probe():
    gate = state / "start-terminal-probe"
    wait_for_file(gate, "terminal probe gate", timeout=120)
    gate.unlink()
    send({"jsonrpc":"2.0", "id":"child-terminal-probe", "method":"terminal/create",
          "params":{"sessionId":"standard-child-one",
                    "command":"touch " + str(state / "forbidden-marker")}})

def exit_for_transport_probe():
    wait_for_file(state / "stop-agent-for-transport-probe", "transport probe gate", timeout=120)
    os._exit(0)

def wait_for_file(path, label, timeout=30):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for " + label)
        time.sleep(0.02)

children = [
    {
        "key":"one", "session_id":"standard-child-one", "title":"worker",
        "task":"inspect the first fixture", "thought":"checking the first fixture",
        "tool":"inspect_first_fixture", "output":"standard child one result",
    },
    {
        "key":"two", "session_id":"standard-child-two", "title":"worker",
        "task":"inspect the second fixture", "thought":"checking the second fixture",
        "tool":"inspect_second_fixture", "output":"standard child two result",
    },
]
active_children = children[:1] if (state / "single-child-mode").exists() else children
reused_work = {
    "key":"reuse", "session_id":"standard-child-one", "title":"worker",
    "task":"check the followup fixture", "thought":"checking the followup fixture",
    "tool":"inspect_followup_fixture", "output":"standard child one followup result",
}

def handle_prompt(request_id, prompt_number):
    try:
        if prompt_number == 1:
            if standard_subagents_enabled:
                for child in active_children:
                    notify(root_session_id, {
                        "sessionUpdate":"subagent_update",
                        "sessionId":child["session_id"],
                        "title":child["title"],
                        "description":child["task"],
                        "cwd":str(state),
                        "capabilities":{"cancel":{}},
                        "state":{"state":"running"},
                    })
                    threading.Thread(target=child_work, args=(child,), daemon=True).start()
                threading.Thread(target=exit_for_transport_probe, daemon=True).start()
                notify(children[0]["session_id"], {
                    "sessionUpdate":"subagent_update",
                    "sessionId":"standard-grandchild-one",
                    "title":"worker",
                    "description":"nested fixture task",
                    "cwd":str(state),
                    "capabilities":{"cancel":{}},
                    "state":{"state":"running"},
                })
                threading.Thread(target=terminal_probe, daemon=True).start()
            else:
                # Without negotiated child support, complete the same work in
                # the parent process and keep child lifecycle frames private.
                (state / "internal-task-results.json").write_text(
                    json.dumps({child["key"]: child["output"] for child in children})
                )
            if (state / "hold-parent-response").exists():
                wait_for_file(
                    state / "release-parent-response",
                    "held parent response release gate",
                    timeout=120,
                )
            notify(root_session_id, {
                "sessionUpdate":"tool_call", "toolCallId":"parent-tool",
                "title":"parent_only_tool", "kind":"other",
                "rawInput":{"scope":"parent"}, "status":"in_progress",
            })
            notify(root_session_id, {
                "sessionUpdate":"tool_call_update", "toolCallId":"parent-tool",
                "status":"completed", "rawOutput":"parent tool completed",
            })
            notify(root_session_id, {
                "sessionUpdate":"agent_message_chunk",
                "content":{"type":"text", "text":(
                    "STANDARD_REPLACEMENT_PARENT_REPLY"
                    if (state / "replacement-mode").exists()
                    else "STANDARD_PARENT_REPLY_FINISHED"
                )},
            })
            send({"jsonrpc":"2.0", "id":request_id,
                  "result":{"stopReason":"end_turn"}})
            (state / "parent-prompt-replied").touch()
        elif prompt_number == 2 and standard_subagents_enabled:
            notify(root_session_id, {
                "sessionUpdate":"subagent_update",
                "sessionId":reused_work["session_id"],
                "title":"worker",
                "description":"followup assignment label; transcript is a separate frame",
                "state":{"state":"running"},
            })
            notify(reused_work["session_id"], {
                "sessionUpdate":"session_message",
                "messageId":"native-followup-assignment",
                "senderSessionId":root_session_id,
                "recipientSessionId":reused_work["session_id"],
                "senderMessageId":"native-parent-followup",
                "recipientMessageId":"native-child-followup",
                "content":[{"type":"text", "text":"STANDARD_FOLLOWUP_ASSIGNMENT_TRANSCRIPT"}],
            })
            threading.Thread(target=child_work, args=(reused_work,), daemon=True).start()
            notify(root_session_id, {
                "sessionUpdate":"agent_message_chunk",
                "content":{"type":"text", "text":"STANDARD_PARENT_FOLLOWUP_REPLY"},
            })
            send({"jsonrpc":"2.0", "id":request_id,
                  "result":{"stopReason":"end_turn"}})
            (state / "parent-followup-replied").touch()
        else:
            notify(root_session_id, {
                "sessionUpdate":"agent_message_chunk",
                "content":{"type":"text", "text":"unexpected second parent prompt"},
            })
            send({"jsonrpc":"2.0", "id":request_id,
                  "result":{"stopReason":"end_turn"}})
    except Exception as exc:
        send({"jsonrpc":"2.0", "id":request_id,
              "error":{"code":-32603, "message":str(exc)}})

for line in sys.stdin:
    message = json.loads(line)
    with (state / "wire.jsonl").open("a") as trace:
        trace.write(json.dumps({"direction":"client_to_agent", "message":message},
                               separators=(",", ":")) + "\n")
        trace.flush()
    request_id = message.get("id")
    method = message.get("method")
    if method == "initialize":
        requested = message.get("params", {}).get("clientCapabilities", {})
        standard_subagents_enabled = isinstance(requested, dict) and isinstance(
            requested.get("subagents"), dict
        )
        (state / "initialized.json").write_text(json.dumps(requested))
        send({"jsonrpc":"2.0", "id":request_id, "result":{
            "protocolVersion":1,
            "agentCapabilities":{"loadSession":False},
        }})
    elif method == "session/new":
        send({"jsonrpc":"2.0", "id":request_id,
              "result":{"sessionId":root_session_id}})
    elif method == "session/prompt":
        prompt_count += 1
        (state / "prompt-count.txt").write_text(str(prompt_count))
        threading.Thread(
            target=handle_prompt,
            args=(request_id, prompt_count),
            daemon=True,
        ).start()
    elif "method" not in message and request_id in {
        "permission-one", "permission-two", "fs-write-one", "fs-write-two",
        "permission-reuse", "fs-write-reuse", "child-terminal-probe",
    }:
        (state / (str(request_id) + "-reply.json")).write_text(json.dumps(message))
"""


def _write_standard_acp_fixture(root: Path) -> Path:
    probe = root / "standard-acp"
    probe.mkdir()
    (probe / "agent.py").write_text(_STANDARD_ACP_AGENT)
    return probe


def _write_acp_config(
    config_home: Path,
    probe: Path,
    *,
    runner_idle_timeout_s: float = 2.0,
) -> str:
    config_home.mkdir(parents=True)
    (config_home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "acp": {
                    "agents": [
                        {
                            "name": "Standard ACP Reporter",
                            "command": shlex.join(
                                [sys.executable, str(probe / "agent.py"), str(probe)]
                            ),
                            "omnigent_mcp": False,
                            "inject_system_prompt": False,
                        }
                    ]
                },
                "runner": {"idle_timeout_s": runner_idle_timeout_s},
            }
        )
    )
    return "standard-acp-reporter"


def _session_bundle(
    acp_name: str,
    *,
    guardrails: dict[str, object] | None = None,
    permission_mode: str = "bypassPermissions",
) -> bytes:
    spec = {
        "spec_version": 1,
        "name": "standard-acp-reporting-e2e",
        "executor": {
            "type": "omnigent",
            "config": {
                "harness": f"acp:{acp_name}",
                "permission_mode": permission_mode,
            },
        },
        "os_env": {
            "type": "caller_process",
            "cwd": ".",
            "sandbox": {
                "type": "none",
                "read_paths": ["."],
                "write_paths": ["."],
            },
        },
        "prompt": "Start two background workers and return the parent response.",
    }
    if guardrails is not None:
        spec["guardrails"] = guardrails
    return bundle_files({"config.yaml": yaml.safe_dump(spec).encode()})


def _snapshot(client: httpx.Client, session_id: str) -> dict:
    response = client.get(f"/v1/sessions/{session_id}", timeout=10.0)
    response.raise_for_status()
    return response.json()


def _children(client: httpx.Client, session_id: str, *, order: str = "asc") -> list[dict]:
    response = client.get(
        f"/v1/sessions/{session_id}/child_sessions",
        params={"order": order},
        timeout=10.0,
    )
    response.raise_for_status()
    return response.json().get("data", [])


def _descendants(client: httpx.Client, session_id: str) -> list[dict]:
    response = client.get(
        f"/v1/sessions/{session_id}/child_sessions",
        params={"descendants": "true", "order": "asc"},
        timeout=10.0,
    )
    response.raise_for_status()
    return response.json().get("data", [])


def _all_descendants_idle(client: httpx.Client, session_id: str) -> bool:
    descendants = _descendants(client, session_id)
    return bool(descendants) and all(child.get("busy") is False for child in descendants)


def _items(client: httpx.Client, session_id: str) -> list[dict]:
    items: list[dict] = []
    after: str | None = None
    while True:
        params: dict[str, object] = {"limit": 20, "order": "asc"}
        if after is not None:
            params["after"] = after
        response = client.get(f"/v1/sessions/{session_id}/items", params=params, timeout=10.0)
        response.raise_for_status()
        page = response.json()
        items.extend(page.get("data", []))
        if page.get("has_more") is not True:
            return items
        after = page.get("last_id")
        assert isinstance(after, str) and after


def _message_text(item: dict) -> str:
    return " ".join(
        block.get("text", "")
        for block in item.get("content", [])
        if isinstance(block, dict) and isinstance(block.get("text"), str)
    )


def _mcp_call(
    client: httpx.Client,
    session_id: str,
    tool_name: str,
    arguments: dict[str, object],
) -> str:
    """Call a runner-owned tool through the real server MCP proxy."""
    response = client.post(
        f"/v1/sessions/{session_id}/mcp",
        headers={"Origin": OMNIGENT_INTERNAL_WS_ORIGIN},
        json={
            "jsonrpc": "2.0",
            "id": f"acp-reporting-{tool_name}-{time.monotonic_ns()}",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        },
        timeout=90,
    )
    response.raise_for_status()
    body = response.json()
    assert "error" not in body, body
    result = body["result"]
    assert not result.get("isError"), result
    output = "\n".join(block["text"] for block in result["content"] if block.get("type") == "text")
    assert not output.startswith("Error:"), output
    return output


def _wait_parent_reply(
    client: httpx.Client,
    parent_id: str,
    marker: str = _PARENT_REPLY,
) -> None:
    def replied_and_idle() -> bool:
        snapshot = _snapshot(client, parent_id)
        replied = any(
            item.get("type") == "message"
            and item.get("data", {}).get("role") == "assistant"
            and marker in _message_text(item.get("data", {}))
            for item in snapshot.get("items", [])
        )
        return replied and snapshot.get("status") == "idle"

    poll_until(replied_and_idle, timeout_s=_TURN_TIMEOUT_S)


_HELD_SHELL_PROCESS = """\
import os, sys, time
from pathlib import Path

started, release = (Path(value) for value in sys.argv[1:])
started.write_text(str(os.getpid()))
deadline = time.monotonic() + 45
while not release.exists():
    if time.monotonic() >= deadline:
        raise TimeoutError("shell process release gate was not opened")
    time.sleep(0.05)
print("ACP_REPORTING_HELD_SHELL_RELEASED", flush=True)
"""


@pytest.mark.parametrize("completed_first", ["shell", "acp"])
def test_real_async_shell_process_and_acp_children_keep_runner_alive(
    tmp_path: Path,
    completed_first: str,
) -> None:
    """A real async shell process and ACP workers independently keep the runner alive."""
    if os.environ.get("OMNIGENT_ACP_REPORTING_E2E_BASELINE") == "1":
        pytest.skip("standard ACP session reporting is disabled in the protocol-only baseline")

    probe = _write_standard_acp_fixture(tmp_path)
    shell_script = probe / "held_shell_process.py"
    shell_script.write_text(_HELD_SHELL_PROCESS)
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe, runner_idle_timeout_s=2.0)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                # Let ACP initialization and session binding finish before the
                # process manager considers the idle harness reapable.
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "60.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name),
                metadata={"workspace": str(probe)},
                filename="acp-shell-overlap.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            shell_started = probe / "held-shell.pid"
            shell_release = probe / "release-held-shell"
            shell_pid: int | None = None
            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(
                    client,
                    session_id=parent_id,
                    text=(
                        "Start both workers, answer me, then continue their work "
                        "in the background."
                    ),
                )
                _wait_parent_reply(client, parent_id)
                poll_until(
                    lambda: (
                        len(_children(client, parent_id)) == 2
                        and all(child.get("busy") for child in _children(client, parent_id))
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )

                command = shlex.join(
                    [sys.executable, str(shell_script), str(shell_started), str(shell_release)]
                )
                handle = json.loads(
                    _mcp_call(
                        client,
                        parent_id,
                        "sys_call_async",
                        {
                            "tool": "sys_os_shell",
                            "args": json.dumps({"command": command, "timeout": 60}),
                        },
                    )
                )
                assert handle.get("tool_name") == "sys_os_shell", handle
                assert handle.get("status") == "in_progress", handle
                poll_until(shell_started.exists, timeout_s=15.0, interval_s=0.1)
                shell_pid = int(shell_started.read_text())
                assert psutil.pid_exists(shell_pid), "the gated shell child process is not live"
                agent_pid = int((probe / "agent.pid").read_text())
                runner_pid = stack.runner.pid
                assert runner_pid is not None

                def runner_stays_alive_while(source: str) -> None:
                    deadline = time.monotonic() + 3.0
                    while time.monotonic() < deadline:
                        assert stack.runner is not None and stack.runner.poll() is None, (
                            stack.log_tail()
                        )
                        assert stack.runner.pid == runner_pid
                        assert _snapshot(client, parent_id).get("status") == "idle"
                        if source == "shell":
                            assert shell_pid is not None and psutil.pid_exists(shell_pid)
                        else:
                            assert psutil.pid_exists(agent_pid), "the ACP harness process exited"
                            children = _children(client, parent_id)
                            assert len(children) == 2 and all(
                                child.get("busy") for child in children
                            ), children
                        time.sleep(0.1)

                def shell_result_is_ready() -> bool:
                    return "ACP_REPORTING_HELD_SHELL_RELEASED" in _mcp_call(
                        client, parent_id, "sys_read_inbox", {}
                    )

                if completed_first == "shell":
                    shell_release.touch()
                    poll_until(shell_result_is_ready, timeout_s=30.0, interval_s=0.2)
                    assert all(child.get("busy") for child in _children(client, parent_id))
                    runner_stays_alive_while("acp")
                    (probe / "start-child-one").touch()
                    (probe / "start-child-two").touch()
                    poll_until(
                        lambda: all(
                            _snapshot(client, str(child["id"])).get("status") == "idle"
                            for child in _children(client, parent_id)
                        ),
                        timeout_s=_CHILD_TIMEOUT_S,
                    )
                else:
                    (probe / "start-child-one").touch()
                    (probe / "start-child-two").touch()
                    poll_until(
                        lambda: all(
                            _snapshot(client, str(child["id"])).get("status") == "idle"
                            for child in _children(client, parent_id)
                        ),
                        timeout_s=_CHILD_TIMEOUT_S,
                    )
                    poll_until(
                        lambda: _all_descendants_idle(client, parent_id),
                        timeout_s=_CHILD_TIMEOUT_S,
                    )
                    descendants = _descendants(client, parent_id)
                    assert descendants and all(
                        child.get("busy") is False for child in descendants
                    ), descendants
                    runner_stays_alive_while("shell")
                    shell_release.touch()
                    poll_until(shell_result_is_ready, timeout_s=30.0, interval_s=0.2)
            finally:
                shell_release.touch()
                (probe / "start-child-one").touch()
                (probe / "start-child-two").touch()
                if shell_started.exists():
                    with suppress(Exception):
                        poll_until(
                            lambda: (
                                "ACP_REPORTING_HELD_SHELL_RELEASED"
                                in _mcp_call(client, parent_id, "sys_read_inbox", {})
                            ),
                            timeout_s=20.0,
                            interval_s=0.2,
                        )
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_child_approval_survives_new_parent_turn_and_stale_answer_is_ignored(
    tmp_path: Path,
) -> None:
    """An older child approval remains scoped after a newer parent turn starts."""
    probe = _write_standard_acp_fixture(tmp_path)
    (probe / "single-child-mode").touch()
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "60.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name, permission_mode="auto"),
                metadata={"workspace": str(probe)},
                filename="acp-child-approval-replacement.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(client, session_id=parent_id, text="Start a worker and return.")
                _wait_parent_reply(client, parent_id)
                poll_until(
                    lambda: (
                        len(_children(client, parent_id)) == 1
                        and _children(client, parent_id)[0].get("busy")
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                child_id = str(_children(client, parent_id)[0]["id"])
                (probe / "start-child-one").touch()

                def pending_for_child(exclude: str | None = None) -> list[dict]:
                    return [
                        item
                        for item in _snapshot(client, parent_id).get("pending_elicitations", [])
                        if isinstance(item, dict)
                        and item.get("elicitation_id") != exclude
                        and isinstance(item.get("params"), dict)
                        and item["params"].get("target_session_id") == child_id
                    ]

                poll_until(
                    lambda: bool(pending_for_child()),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                first_approval = pending_for_child()[0]
                first_id = first_approval.get("elicitation_id")
                assert isinstance(first_id, str) and first_id, first_approval

                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Give the same worker a follow-up while its first approval is pending.",
                )
                _wait_parent_reply(
                    client,
                    parent_id,
                    marker="STANDARD_PARENT_FOLLOWUP_REPLY",
                )
                (probe / "start-child-reuse").touch()
                poll_until(
                    lambda: bool(pending_for_child(exclude=first_id)),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                second_approval = pending_for_child(exclude=first_id)[0]
                second_id = second_approval.get("elicitation_id")
                assert isinstance(second_id, str) and second_id and second_id != first_id
                assert _snapshot(client, parent_id).get("status") == "idle"

                first_answer = client.post(
                    f"/v1/sessions/{child_id}/elicitations/{first_id}/resolve",
                    json={"action": "accept", "content": {"confirmation": "first"}},
                    timeout=20.0,
                )
                assert first_answer.status_code == 202, first_answer.text
                poll_until((probe / "permission-reply-one.json").exists, timeout_s=15.0)
                first_reply = json.loads((probe / "permission-reply-one.json").read_text())
                assert first_reply.get("id") == "permission-one", first_reply
                assert first_reply.get("result", {}).get("outcome") == {
                    "outcome": "selected",
                    "optionId": "allow-once",
                }, first_reply
                assert _snapshot(client, parent_id).get("status") == "idle"

                stale_answer = client.post(
                    f"/v1/sessions/{child_id}/elicitations/{first_id}/resolve",
                    json={"action": "accept", "content": {"confirmation": "late"}},
                    timeout=20.0,
                )
                assert stale_answer.status_code == 202, stale_answer.text
                second_pending = pending_for_child(exclude=first_id)
                assert any(item.get("elicitation_id") == second_id for item in second_pending), (
                    second_pending
                )
                assert not (probe / "permission-reply-reuse.json").exists()
                assert _snapshot(client, parent_id).get("status") == "idle"

                second_answer = client.post(
                    f"/v1/sessions/{child_id}/elicitations/{second_id}/resolve",
                    json={"action": "decline", "content": {"confirmation": "second"}},
                    timeout=20.0,
                )
                assert second_answer.status_code == 202, second_answer.text
                poll_until((probe / "permission-reply-reuse.json").exists, timeout_s=15.0)
                second_reply = json.loads((probe / "permission-reply-reuse.json").read_text())
                assert second_reply.get("id") == "permission-reuse", second_reply
                assert second_reply.get("result", {}).get("outcome") == {
                    "outcome": "selected",
                    "optionId": "reject-once",
                }, second_reply
                poll_until(
                    (probe / "permission-denied-reuse").exists,
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                assert (probe / "permission-denied-reuse").exists()
                assert not (probe / "child-reuse.txt").exists()
            finally:
                for name in (
                    "start-child-one",
                    "start-child-two",
                    "start-child-reuse",
                    "start-terminal-probe",
                    "stop-agent-for-transport-probe",
                ):
                    (probe / name).touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_unanswered_child_approval_uses_trusted_timeout_without_interrupting_parent(
    tmp_path: Path,
) -> None:
    """A trusted short timeout cancels only the child action and ignores late answers."""
    from concurrent.futures import ThreadPoolExecutor

    probe = _write_standard_acp_fixture(tmp_path)
    (probe / "hold-parent-response").touch()
    (probe / "hostile-approval-timeout").touch()
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "60.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=30.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(
                    acp_name,
                    guardrails={"ask_timeout": 1},
                    permission_mode="auto",
                ),
                metadata={"workspace": str(probe)},
                filename="acp-child-approval-timeout.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            release_parent = probe / "release-parent-response"
            stream_ready = threading.Event()
            stream_stop = threading.Event()
            resolution_seen = threading.Event()
            target_elicitation_id: str | None = None
            observed_resolution: dict[str, object] = {}
            stream_errors: list[str] = []
            stream_thread: threading.Thread | None = None

            def tail_parent_stream() -> None:
                try:
                    with httpx.Client(
                        base_url=stack.base_url,
                        timeout=httpx.Timeout(60.0, connect=10.0),
                        trust_env=False,
                    ) as stream_client:
                        with stream_client.stream(
                            "GET", f"/v1/sessions/{parent_id}/stream"
                        ) as response:
                            if response.status_code != 200:
                                stream_errors.append(f"SSE stream returned {response.status_code}")
                                stream_ready.set()
                                return
                            event_type: str | None = None
                            for line in response.iter_lines():
                                if stream_stop.is_set():
                                    return
                                if line.startswith("event:"):
                                    event_type = line.removeprefix("event:").strip()
                                    if event_type == "session.heartbeat":
                                        stream_ready.set()
                                elif line.startswith("data:") and event_type == (
                                    "response.elicitation_resolved"
                                ):
                                    try:
                                        event = json.loads(line.removeprefix("data:").strip())
                                    except json.JSONDecodeError:
                                        event = None
                                    if (
                                        isinstance(event, dict)
                                        and target_elicitation_id is not None
                                        and event.get("elicitation_id") == target_elicitation_id
                                    ):
                                        observed_resolution.update(event)
                                        resolution_seen.set()
                                        return
                                elif not line:
                                    event_type = None
                except Exception as exc:
                    stream_errors.append(f"{type(exc).__name__}: {exc}")
                    stream_ready.set()

            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                stream_thread = threading.Thread(target=tail_parent_stream, daemon=True)
                stream_thread.start()
                assert stream_ready.wait(timeout=15.0), "parent SSE stream did not connect"
                assert not stream_errors, stream_errors
                with ThreadPoolExecutor(max_workers=1) as pool:
                    parent_request = pool.submit(
                        send_user_text,
                        client,
                        session_id=parent_id,
                        text="Start two workers and wait for my release signal.",
                    )
                    try:
                        poll_until(
                            lambda: (
                                len(_children(client, parent_id)) == 2
                                and all(
                                    child.get("busy") for child in _children(client, parent_id)
                                )
                            ),
                            timeout_s=_CHILD_TIMEOUT_S,
                        )
                        children = _children(client, parent_id)
                        child_one = next(
                            str(child["id"])
                            for child in children
                            if "first fixture"
                            in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
                        )
                        child_two = next(
                            str(child["id"])
                            for child in children
                            if "second fixture"
                            in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
                        )
                        (probe / "start-child-one").touch()

                        def child_approval() -> list[dict]:
                            return [
                                event
                                for event in _snapshot(client, parent_id).get(
                                    "pending_elicitations", []
                                )
                                if isinstance(event, dict)
                                and isinstance(event.get("params"), dict)
                                and event["params"].get("target_session_id") == child_one
                            ]

                        poll_until(
                            lambda: bool(child_approval()),
                            timeout_s=_CHILD_TIMEOUT_S,
                        )
                        approval = child_approval()[0]
                        elicitation_id = approval.get("elicitation_id")
                        assert isinstance(elicitation_id, str) and elicitation_id, approval
                        target_elicitation_id = elicitation_id
                        assert approval["params"].get("policy_name") == (
                            "standard acp reporter_sdk_permission"
                        ), approval
                        assert "ask_timeout" not in approval["params"], approval
                        assert "timeout_s" not in approval["params"], approval
                        assert _snapshot(client, parent_id).get("status") == "running"

                        timeout_started = time.monotonic()
                        poll_until(
                            (probe / "permission-reply-one.json").exists,
                            timeout_s=8.0,
                            interval_s=0.1,
                        )
                        assert time.monotonic() - timeout_started < 8.0
                        permission_reply = json.loads(
                            (probe / "permission-reply-one.json").read_text()
                        )
                        assert permission_reply.get("result", {}).get("outcome") == {
                            "outcome": "selected",
                            "optionId": "reject-once",
                        }, permission_reply
                        poll_until(
                            (probe / "permission-denied-one").exists,
                            timeout_s=_CHILD_TIMEOUT_S,
                        )
                        assert (probe / "permission-denied-one").exists()
                        assert not (probe / "child-one.txt").exists()
                        poll_until(
                            lambda: _snapshot(client, child_one).get("status") == "idle",
                            timeout_s=_CHILD_TIMEOUT_S,
                        )
                        assert _snapshot(client, parent_id).get("status") == "running"
                        child_two_row = next(
                            row
                            for row in _children(client, parent_id)
                            if str(row.get("id")) == child_two
                        )
                        assert child_two_row.get("busy") is True, child_two_row
                        assert child_two_row.get("current_task_status") == "in_progress", (
                            child_two_row
                        )
                        child_two_observation = child_two_row.get("acp_session_observation") or {}
                        assert child_two_observation.get("state") == {
                            "present": True,
                            "value": {"state": "running"},
                        }, child_two_row

                        assert resolution_seen.wait(timeout=45.0), stream_errors
                        assert observed_resolution.get("type") == "response.elicitation_resolved"
                        assert observed_resolution.get("elicitation_id") == elicitation_id
                        assert observed_resolution.get("reason") == "unanswered"

                        late_answer = client.post(
                            f"/v1/sessions/{child_one}/elicitations/{elicitation_id}/resolve",
                            json={"action": "accept"},
                            timeout=20.0,
                        )
                        assert late_answer.status_code == 202, late_answer.text
                        assert _snapshot(client, parent_id).get("status") == "running"
                        child_two_row = next(
                            row
                            for row in _children(client, parent_id)
                            if str(row.get("id")) == child_two
                        )
                        assert child_two_row.get("busy") is True, child_two_row
                        assert child_two_row.get("current_task_status") == "in_progress", (
                            child_two_row
                        )
                        child_two_observation = child_two_row.get("acp_session_observation") or {}
                        assert child_two_observation.get("state") == {
                            "present": True,
                            "value": {"state": "running"},
                        }, child_two_row
                        assert not (probe / "child-one.txt").exists()

                        release_parent.touch()
                        parent_request.result(timeout=30.0)
                        _wait_parent_reply(client, parent_id)
                    finally:
                        release_parent.touch()
            finally:
                stream_stop.set()
                if stream_thread is not None:
                    stream_thread.join(timeout=2.0)
                for name in (
                    "start-child-one",
                    "start-child-two",
                    "start-child-reuse",
                    "start-terminal-probe",
                    "stop-agent-for-transport-probe",
                ):
                    (probe / name).touch()
                release_parent.touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_pending_child_approval_is_fenced_by_runner_replacement(tmp_path: Path) -> None:
    """A pending child approval from a retired runtime cannot answer its replacement."""
    probe = _write_standard_acp_fixture(tmp_path)
    (probe / "single-child-mode").touch()
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "60.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name, permission_mode="auto"),
                metadata={"workspace": str(probe)},
                filename="acp-stale-approval-replacement.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(client, session_id=parent_id, text="Start a worker and return.")
                _wait_parent_reply(client, parent_id)
                poll_until(
                    lambda: (
                        len(_children(client, parent_id)) == 1
                        and _children(client, parent_id)[0].get("busy")
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                old_child_id = str(_children(client, parent_id)[0]["id"])
                old_agent_pid = int((probe / "agent.pid").read_text())
                old_runner_pid = stack.runner.pid
                assert old_runner_pid is not None
                (probe / "start-child-one").touch()

                def pending_for(session_id: str) -> list[dict]:
                    return [
                        item
                        for item in _snapshot(client, parent_id).get("pending_elicitations", [])
                        if isinstance(item, dict)
                        and isinstance(item.get("params"), dict)
                        and item["params"].get("target_session_id") == session_id
                    ]

                poll_until(
                    lambda: bool(pending_for(old_child_id)),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                old_approval = pending_for(old_child_id)[0]
                old_elicitation_id = old_approval.get("elicitation_id")
                assert isinstance(old_elicitation_id, str) and old_elicitation_id
                assert _snapshot(client, parent_id).get("status") == "idle"

                stack.restart_runner()
                assert stack.runner is not None and stack.runner.pid != old_runner_pid
                poll_until(
                    lambda: not psutil.pid_exists(old_agent_pid), timeout_s=_CHILD_TIMEOUT_S
                )
                old_child_ids = {old_child_id}
                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Continue after the worker runtime was replaced.",
                )
                _wait_parent_reply(client, parent_id)
                poll_until(
                    lambda: (
                        int((probe / "agent.pid").read_text()) != old_agent_pid
                        and len(_children(client, parent_id)) == 2
                        and any(
                            child.get("busy")
                            for child in _children(client, parent_id)
                            if str(child.get("id")) not in old_child_ids
                        )
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                new_child_id = next(
                    str(child["id"])
                    for child in _children(client, parent_id)
                    if str(child.get("id")) not in old_child_ids
                )
                assert new_child_id != old_child_id
                # The old worker consumed and removed this gate before replacement.
                (probe / "start-child-one").touch()
                poll_until(
                    lambda: bool(pending_for(new_child_id)),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                new_approval = pending_for(new_child_id)[0]
                new_elicitation_id = new_approval.get("elicitation_id")
                assert isinstance(new_elicitation_id, str) and new_elicitation_id
                assert new_elicitation_id != old_elicitation_id
                assert _snapshot(client, parent_id).get("status") == "idle"

                stale = client.post(
                    f"/v1/sessions/{old_child_id}/elicitations/{old_elicitation_id}/resolve",
                    json={"action": "accept"},
                    timeout=20.0,
                )
                assert stale.status_code == 202, stale.text
                current_pending = pending_for(new_child_id)
                assert any(
                    item.get("elicitation_id") == new_elicitation_id for item in current_pending
                ), current_pending
                assert not (probe / "permission-reply-one.json").exists()
                assert _snapshot(client, parent_id).get("status") == "idle"

                answer = client.post(
                    f"/v1/sessions/{new_child_id}/elicitations/{new_elicitation_id}/resolve",
                    json={"action": "accept", "content": {"confirmation": "fresh"}},
                    timeout=20.0,
                )
                assert answer.status_code == 202, answer.text
                poll_until(
                    lambda: (
                        (probe / "finished-child-one").exists()
                        and (probe / "child-one.txt").exists()
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                poll_until((probe / "permission-reply-one.json").exists, timeout_s=15.0)
                reply = json.loads((probe / "permission-reply-one.json").read_text())
                assert reply.get("id") == "permission-one", reply
                assert reply.get("result", {}).get("outcome") == {
                    "outcome": "selected",
                    "optionId": "allow-once",
                }, reply
                assert (probe / "child-one.txt").read_text() == _CHILD_OUTPUT["one"]
            finally:
                for name in (
                    "start-child-one",
                    "start-child-two",
                    "start-child-reuse",
                    "start-terminal-probe",
                    "stop-agent-for-transport-probe",
                ):
                    (probe / name).touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_committed_acp_report_is_retried_before_journal_ack_advances(tmp_path: Path) -> None:
    """A committed report whose response is lost stays unacked until exact replay."""
    if os.environ.get("OMNIGENT_ACP_REPORTING_E2E_BASELINE") == "1":
        pytest.skip("standard ACP session reporting is disabled in the protocol-only baseline")

    probe = _write_standard_acp_fixture(tmp_path)
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe)
    fault_dir = tmp_path / "post-commit-fault"
    server_env = {
        "OMNIGENT_AUTH_PROVIDER": "header",
        "OMNIGENT_AUTH_ENABLED": "0",
        "OMNIGENT_TEST_ACP_REPORT_FAULT_DIR": str(fault_dir),
    }
    with server_runner(
        tmp_path / "stack",
        server_bootstrap=_REPORT_REPLAY_SERVER_BOOTSTRAP,
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=server_env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                "OMNIGENT_AUTH_PROVIDER": "header",
                "OMNIGENT_AUTH_ENABLED": "0",
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "60.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name),
                metadata={"workspace": str(probe)},
                filename="acp-report-retry.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            release_path = fault_dir / "release"
            held_path = fault_dir / "held.json"
            attempts_path = fault_dir / "attempts.jsonl"

            def attempts() -> list[dict]:
                if not attempts_path.exists():
                    return []
                return [
                    json.loads(line)
                    for line in attempts_path.read_text().splitlines()
                    if line.strip()
                ]

            def journal_state() -> dict:
                journal_root = stack.runner_home / ".omnigent" / "acp-subagent-reports"
                paths = journal_root.rglob("*.json") if journal_root.exists() else ()
                for path in paths:
                    try:
                        state = json.loads(path.read_text())
                    except (OSError, json.JSONDecodeError):
                        continue
                    if state.get("parent_session_id") == parent_id:
                        return state
                return {}

            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Start two workers and return the parent response.",
                )
                _wait_parent_reply(client, parent_id)
                poll_until(held_path.exists, timeout_s=_CHILD_TIMEOUT_S)
                held = json.loads(held_path.read_text())
                assert held.get("source_id")
                assert held.get("sequence", 0) > 0, held
                assert held.get("committed_status", 0) in range(200, 300), held
                assert held.get("committed_response", {}).get("durable") is True, held

                sequence = held["sequence"]
                poll_until(
                    lambda: (
                        bool(journal_state())
                        and journal_state().get("acknowledged_sequence", 0) < sequence
                    ),
                    timeout_s=15.0,
                    interval_s=0.1,
                )
                before_retry = journal_state()
                assert before_retry.get("acknowledged_sequence", 0) < sequence, before_retry

                release_path.touch()
                poll_until(
                    lambda: (
                        sum(record.get("source_id") == held["source_id"] for record in attempts())
                        >= 2
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                    interval_s=0.1,
                )
                retries = [
                    record for record in attempts() if record.get("source_id") == held["source_id"]
                ]
                assert len(retries) == 2, retries
                assert all(record["envelope"] == retries[0]["envelope"] for record in retries), (
                    retries
                )
                assert all(
                    record["wire_payload_sha256"] == retries[0]["wire_payload_sha256"]
                    for record in retries
                ), retries
                assert retries[1].get("committed_response", {}).get("durable") is True, retries
                poll_until(
                    lambda: (
                        journal_state().get("acknowledged_sequence", 0)
                        == journal_state().get("next_sequence", 1) - 1
                        and len(_children(client, parent_id)) == 2
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                    interval_s=0.1,
                )
                final_state = journal_state()
                high_water = final_state["next_sequence"] - 1
                report_attempts = attempts()
                assert final_state.get("acknowledged_sequence") == high_water, final_state
                assert len(report_attempts) == high_water + 1, report_attempts
                attempt_sequences = [record.get("sequence") for record in report_attempts]
                assert attempt_sequences.count(sequence) == 2, attempt_sequences
                assert [
                    value
                    for index, value in enumerate(attempt_sequences)
                    if index == 0 or value != attempt_sequences[index - 1]
                ] == list(range(1, high_water + 1)), attempt_sequences
                source_counts: dict[str, int] = {}
                for record in report_attempts:
                    source_id = record.get("source_id")
                    assert isinstance(source_id, str) and source_id
                    source_counts[source_id] = source_counts.get(source_id, 0) + 1
                assert len(source_counts) == high_water, source_counts
                assert all(
                    count == (2 if source_id == held["source_id"] else 1)
                    for source_id, count in source_counts.items()
                ), source_counts
                child_ids = {
                    record.get("committed_response", {}).get("child_session_id")
                    for record in retries
                }
                assert len(child_ids) == 1 and None not in child_ids, retries
                persisted_children = _children(client, parent_id)
                assert len(persisted_children) == 2, persisted_children
                assert (
                    sum(str(child.get("id")) in child_ids for child in persisted_children) == 1
                ), persisted_children
            finally:
                release_path.touch()
                for name in (
                    "start-child-one",
                    "start-child-two",
                    "start-child-reuse",
                    "start-terminal-probe",
                    "stop-agent-for-transport-probe",
                ):
                    (probe / name).touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def _isolated_cli_env(*, home: Path, config_home: Path, data_dir: Path) -> dict[str, str]:
    home.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    bin_dir = data_dir / "test-bin"
    bin_dir.mkdir(exist_ok=True)

    for name, source in (
        ("python", sys.executable),
        ("python3", sys.executable),
        ("docker", shutil.which("docker")),
        ("node", shutil.which("node")),
        ("realnode", shutil.which("realnode")),
    ):
        if source is None:
            continue
        target = bin_dir / name
        with suppress(FileNotFoundError):
            target.unlink()
        target.symlink_to(Path(source).resolve())

    env = {
        name: os.environ[name]
        for name in ("LANG", "LC_ALL", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR")
        if name in os.environ
    }
    env.update(
        {
            "PATH": os.pathsep.join(
                [
                    str(bin_dir),
                    "/usr/bin",
                    "/bin",
                    "/usr/sbin",
                    "/sbin",
                ]
            ),
            "HOME": str(home),
            "OMNIGENT_CONFIG_HOME": str(config_home),
            "OMNIGENT_DATA_DIR": str(data_dir),
            "OMNIGENT_AUTH_PROVIDER": "header",
            "OMNIGENT_AUTH_ENABLED": "0",
            "OMNIGENT_LOCAL_SINGLE_USER": "1",
            "OMNIGENT_DISABLE_CATALOG_LOOKUP": "1",
            "OMNIGENT_DISABLE_TELEMETRY": "1",
            "OMNIGENT_SKIP_ONBOARD": "1",
            "OMNIGENT_NO_UPDATE_CHECK": "1",
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "PYTHONPATH": os.pathsep.join(
                [str(_REPO_ROOT), str(_REPO_ROOT / "sdks" / "python-client")]
            ),
        }
    )
    return env


def _stop_local_cli_runtime(cli_env: dict[str, str], data_dir: Path) -> None:
    """Stop an isolated CLI-owned local host/server runtime and reap its PIDs."""
    host_record_path = daemon_record_path("local", base_dir=data_dir)
    local_server_pidfile = data_dir / "local_server.pid"
    process_ids: set[int] = set()
    if host_record_path.is_file():
        record = json.loads(host_record_path.read_text())
        if isinstance(record.get("pid"), int):
            process_ids.add(record["pid"])
    if local_server_pidfile.is_file():
        first_line = local_server_pidfile.read_text().splitlines()[0:1]
        if first_line and first_line[0].isdigit():
            process_ids.add(int(first_line[0]))

    stop = subprocess.run(
        [sys.executable, "-m", "omnigent", "stop", "--force"],
        cwd=_REPO_ROOT,
        env=cli_env,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert stop.returncode == 0, stop.stdout + stop.stderr
    poll_until(
        lambda: all(not psutil.pid_exists(pid) for pid in process_ids),
        timeout_s=10.0,
    )


def test_standard_acp_children_reach_persistence_and_cli(tmp_path: Path) -> None:
    """A raw ACP process reports two children after the parent turn is idle."""
    baseline = os.environ.get("OMNIGENT_ACP_REPORTING_E2E_BASELINE") == "1"
    probe = _write_standard_acp_fixture(tmp_path)
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "2.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name),
                metadata={"workspace": str(probe)},
                filename="standard-acp.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(
                    client,
                    session_id=parent_id,
                    text=(
                        "Start both workers, answer me, then continue their work "
                        "in the background."
                    ),
                )
                _wait_parent_reply(client, parent_id)

                assert (probe / "initialized.json").exists(), "ACP initialize did not complete"
                assert (probe / "parent-prompt-replied").exists(), "parent reply was not sent"
                initialized = json.loads((probe / "initialized.json").read_text())
                assert initialized.get("terminal") is False, initialized

                if baseline:
                    assert "subagents" not in initialized, initialized
                    assert json.loads((probe / "internal-task-results.json").read_text()) == {
                        "one": _CHILD_OUTPUT["one"],
                        "two": _CHILD_OUTPUT["two"],
                    }
                    assert _children(client, parent_id) == [], (
                        "protocol-only baseline initialized and answered the parent, but exposed "
                        "children without standard ACP reporting support"
                    )
                    return
                assert "subagents" in initialized, initialized
                assert initialized.get("terminal") is False, initialized

                def both_children_are_running() -> bool:
                    children = _children(client, parent_id)
                    return len(children) == 2 and all(child.get("busy") for child in children)

                poll_until(both_children_are_running, timeout_s=_CHILD_TIMEOUT_S)
                children = _children(client, parent_id)
                runner_pid = stack.runner.pid
                agent_pid = int((probe / "agent.pid").read_text())
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    assert stack.runner.poll() is None, stack.log_tail()
                    assert _snapshot(client, parent_id).get("status") == "idle"
                    assert all(child.get("busy") for child in _children(client, parent_id))
                    assert int((probe / "agent.pid").read_text()) == agent_pid
                    time.sleep(0.1)
                assert stack.runner.pid == runner_pid
                assert [child.get("tool") for child in children] == ["worker", "worker"], children
                child_keys = {
                    child.get("labels", {}).get("omnigent.acp.subagent_id") for child in children
                }
                assert len(child_keys) == 2, children
                assert child_keys == {child.get("session_name") for child in children}, children
                first_child = next(
                    child
                    for child in children
                    if "first fixture"
                    in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
                )
                second_child = next(
                    child
                    for child in children
                    if "second fixture"
                    in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
                )
                child_one = str(first_child["id"])
                child_two = str(second_child["id"])
                assert child_one != child_two
                descendant_response = client.get(
                    f"/v1/sessions/{parent_id}/child_sessions",
                    params={"descendants": "true", "order": "asc"},
                    timeout=10.0,
                )
                descendant_response.raise_for_status()
                descendants = descendant_response.json().get("data", [])
                grandchild = next(
                    item for item in descendants if item.get("parent_session_id") == child_one
                )
                assert grandchild.get("parent_session_id") == child_one, descendants
                assert grandchild.get("depth") == 2, descendants
                assert "nested fixture task" in grandchild.get("labels", {}).get(
                    "omnigent.acp.subagent_description", ""
                ), descendants
                assert _snapshot(client, parent_id).get("status") == "idle"

                (probe / "start-terminal-probe").touch()
                poll_until(
                    lambda: (probe / "child-terminal-probe-reply.json").exists(),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                terminal_reply = json.loads(
                    (probe / "child-terminal-probe-reply.json").read_text()
                )
                assert terminal_reply.get("id") == "child-terminal-probe", terminal_reply
                assert terminal_reply.get("error", {}).get("code") == -32601, terminal_reply
                assert not (probe / "forbidden-marker").exists()
                assert (probe / "prompt-count.txt").read_text() == "1"
                wire_messages = [
                    json.loads(line)
                    for line in (probe / "wire.jsonl").read_text().splitlines()
                    if line.strip()
                ]
                client_methods = [
                    frame["message"].get("method")
                    for frame in wire_messages
                    if frame.get("direction") == "client_to_agent"
                ]
                assert "session/cancel" not in client_methods, client_methods
                assert "terminal/create" not in client_methods, client_methods
                assert not any(
                    frame.get("direction") == "client_to_agent"
                    and frame.get("message", {}).get("id") == "child-terminal-probe"
                    and frame.get("message", {}).get("error", {}).get("code") != -32601
                    for frame in wire_messages
                ), wire_messages

                (probe / "start-child-one").touch()
                poll_until(
                    lambda: (
                        (probe / "finished-child-one").exists()
                        and _snapshot(client, child_one).get("status") == "idle"
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                one_items = _items(client, child_one)
                two_items_before_restart = _items(client, child_two)
                assert any(
                    item.get("type") == "reasoning"
                    and "checking the first fixture" in json.dumps(item)
                    for item in one_items
                ), one_items
                assert any(
                    item.get("type") == "function_call"
                    and item.get("name") == "inspect_first_fixture"
                    for item in one_items
                ), one_items
                assert any(
                    item.get("type") == "function_call_output"
                    and _CHILD_OUTPUT["one"] in json.dumps(item)
                    for item in one_items
                ), one_items
                first_tool = next(
                    item
                    for item in one_items
                    if item.get("type") == "function_call"
                    and item.get("name") == "inspect_first_fixture"
                )
                assert first_tool["acp_session_references"] == [
                    {"native_session_id": "standard-child-two", "conversation_id": child_two}
                ], first_tool
                directed_message = next(
                    item
                    for item in one_items
                    if item.get("type") == "message"
                    and item.get("acp_session_message", {}).get("sender_message_id")
                    == "native-sender-message-one"
                )
                assert directed_message["content"] == [
                    {"type": "text", "text": "Directed hello"},
                    {"type": "text", "text": " with a chunk."},
                ], directed_message
                assert directed_message["acp_session_message"] == {
                    "kind": "directed",
                    "sender_session_id": "standard-child-one",
                    "recipient_session_id": "standard-child-two",
                    "sender_message_id": "native-sender-message-one",
                    "recipient_message_id": "native-recipient-message-two",
                    "sender_conversation_id": child_one,
                    "recipient_conversation_id": child_two,
                }, directed_message
                poll_until(
                    lambda: any(
                        child["id"] == child_one
                        and str(child.get("title", "")).startswith("Reused worker:")
                        for child in _children(client, parent_id)
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                reused_children = _children(client, parent_id)
                assert len(reused_children) == 2, reused_children
                assert {str(item["id"]) for item in reused_children} == {
                    child_one,
                    child_two,
                }, reused_children
                assert not any(_CHILD_OUTPUT["two"] in json.dumps(item) for item in one_items), (
                    one_items
                )
                assert _snapshot(client, parent_id).get("status") == "idle"
                assert _snapshot(client, child_two).get("status") == "running"
                permission_one = json.loads((probe / "permission-reply-one.json").read_text())
                assert permission_one.get("id") == "permission-one", permission_one
                assert permission_one.get("result", {}).get("outcome") == {
                    "outcome": "selected",
                    "optionId": "allow-once",
                }, permission_one
                fs_reply_one = json.loads((probe / "fs-reply-one.json").read_text())
                assert fs_reply_one.get("id") == "fs-write-one", fs_reply_one
                assert "error" not in fs_reply_one, fs_reply_one
                assert (probe / "child-one.txt").read_text() == _CHILD_OUTPUT["one"]

                expected_one_items = json.loads(json.dumps(one_items, sort_keys=True))
                stack.restart_server()
                assert [str(child["id"]) for child in _children(client, parent_id)] == [
                    str(child["id"]) for child in children
                ]
                assert _items(client, child_one) == expected_one_items
                assert _items(client, child_two) == two_items_before_restart
                assert _snapshot(client, child_two).get("status") == "running"

                (probe / "start-child-two").touch()
                poll_until(
                    lambda: (
                        (probe / "finished-child-two").exists()
                        and _snapshot(client, child_two).get("status") == "idle"
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                two_items = _items(client, child_two)
                assert any(
                    item.get("type") == "function_call_output"
                    and _CHILD_OUTPUT["two"] in json.dumps(item)
                    for item in two_items
                ), two_items

                def observation_ready() -> bool:
                    return all(
                        field
                        in (_snapshot(client, child_one).get("acp_session_observation") or {})
                        for field in (
                            "session_info",
                            "usage",
                            "config_options",
                            "mode",
                            "plan",
                        )
                    )

                poll_until(observation_ready, timeout_s=_CHILD_TIMEOUT_S)
                child_observation = _snapshot(client, child_one)["acp_session_observation"]
                assert child_observation["session_info"] == {"title": "Native worker title"}
                assert child_observation["usage"] == {"used": 8, "size": 200000}
                assert child_observation["config_options"] is None
                assert child_observation["mode"] == {"currentModeId": "plan"}
                assert child_observation["plan"] == {
                    "plan": {
                        "type": "items",
                        "planId": "worker-plan",
                        "entries": [
                            {
                                "content": "Inspect the fixture",
                                "priority": "high",
                                "status": "completed",
                            }
                        ],
                    }
                }
                second_observation = _snapshot(client, child_two).get(
                    "acp_session_observation", {}
                )
                assert "usage" not in second_observation, second_observation
                assert "config_options" not in second_observation, second_observation
                permission_two = json.loads((probe / "permission-reply-two.json").read_text())
                assert permission_two.get("id") == "permission-two", permission_two
                assert permission_two.get("result", {}).get("outcome") == {
                    "outcome": "selected",
                    "optionId": "allow-once",
                }, permission_two
                fs_reply_two = json.loads((probe / "fs-reply-two.json").read_text())
                assert fs_reply_two.get("id") == "fs-write-two", fs_reply_two
                assert "error" not in fs_reply_two, fs_reply_two
                assert (probe / "child-two.txt").read_text() == _CHILD_OUTPUT["two"]
                assert _snapshot(client, parent_id).get("status") == "idle"
                parent_items = _items(client, parent_id)
                assert _PARENT_REPLY in json.dumps(parent_items, ensure_ascii=False)
                assert "standard child one result" not in json.dumps(
                    parent_items, ensure_ascii=False
                )
                assert "standard child two result" not in json.dumps(
                    parent_items, ensure_ascii=False
                )

                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Give the first worker a followup assignment after it became idle.",
                )
                _wait_parent_reply(
                    client,
                    parent_id,
                    marker="STANDARD_PARENT_FOLLOWUP_REPLY",
                )
                poll_until(
                    lambda: _snapshot(client, child_one).get("status") == "running",
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                assert {str(child["id"]) for child in _children(client, parent_id)} == {
                    child_one,
                    child_two,
                }
                (probe / "start-child-reuse").touch()
                poll_until(
                    lambda: (
                        (probe / "finished-child-reuse").exists()
                        and _snapshot(client, child_one).get("status") == "idle"
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                reused_items = _items(client, child_one)
                followup_message = next(
                    item
                    for item in reused_items
                    if item.get("type") == "message"
                    and item.get("acp_session_message", {}).get("sender_message_id")
                    == "native-parent-followup"
                )
                assert followup_message["acp_session_message"] == {
                    "kind": "directed",
                    "sender_session_id": "standard-root-" + str(agent_pid),
                    "recipient_session_id": "standard-child-one",
                    "sender_message_id": "native-parent-followup",
                    "recipient_message_id": "native-child-followup",
                    "sender_conversation_id": parent_id,
                    "recipient_conversation_id": child_one,
                }, followup_message
                assert "STANDARD_FOLLOWUP_ASSIGNMENT_TRANSCRIPT" in json.dumps(
                    followup_message, ensure_ascii=False
                ), followup_message
                assert (
                    "followup assignment label; transcript is a separate frame"
                    not in json.dumps(reused_items, ensure_ascii=False)
                ), reused_items
                assert any(
                    item.get("type") == "function_call_output"
                    and "standard child one followup result" in json.dumps(item)
                    for item in reused_items
                ), reused_items

                original_agent_pid = int((probe / "agent.pid").read_text())
                (probe / "stop-agent-for-transport-probe").touch()

                def transport_loss_is_persisted(session_ids: tuple[str, ...]) -> bool:
                    for session_id in session_ids:
                        observation = _snapshot(client, session_id).get(
                            "acp_session_observation", {}
                        )
                        if observation.get("state") != {"present": False}:
                            return False
                        if observation.get("transport") != {
                            "state": "disconnected",
                            "reason": "stdout_eof",
                        }:
                            return False
                    return True

                poll_until(
                    lambda: transport_loss_is_persisted((parent_id, child_one, child_two)),
                    timeout_s=_CHILD_TIMEOUT_S,
                )

                for name in (
                    "stop-agent-for-transport-probe",
                    "parent-prompt-replied",
                    "parent-followup-replied",
                    "finished-child-one",
                    "finished-child-two",
                    "finished-child-reuse",
                    "permission-one-reply.json",
                    "permission-two-reply.json",
                    "permission-reuse-reply.json",
                    "fs-write-one-reply.json",
                    "fs-write-two-reply.json",
                    "fs-write-reuse-reply.json",
                    "permission-reply-one.json",
                    "permission-reply-two.json",
                    "permission-reply-reuse.json",
                    "fs-reply-one.json",
                    "fs-reply-two.json",
                    "fs-reply-reuse.json",
                    "child-one.txt",
                    "child-two.txt",
                    "child-reuse.txt",
                    "prompt-count.txt",
                    "start-child-one",
                    "start-child-two",
                    "start-child-reuse",
                ):
                    with suppress(FileNotFoundError):
                        (probe / name).unlink()
                (probe / "replacement-mode").touch()
                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Continue in a fresh ACP connection after transport loss.",
                )
                _wait_parent_reply(
                    client,
                    parent_id,
                    marker="STANDARD_REPLACEMENT_PARENT_REPLY",
                )
                poll_until(
                    lambda: (
                        (probe / "agent.pid").exists()
                        and int((probe / "agent.pid").read_text()) != original_agent_pid
                        and len(_children(client, parent_id)) == 4
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                replaced_children = _children(client, parent_id)
                replaced_ids = {str(child["id"]) for child in replaced_children}
                assert {child_one, child_two}.issubset(replaced_ids), replaced_children
                new_children = replaced_ids - {child_one, child_two}
                assert len(new_children) == 2, replaced_children
                assert all(
                    child.get("busy")
                    for child in replaced_children
                    if str(child["id"]) in new_children
                ), replaced_children
                assert all(
                    not child.get("busy")
                    for child in replaced_children
                    if str(child["id"]) in {child_one, child_two}
                ), replaced_children
                assert transport_loss_is_persisted((child_one, child_two))
                current_root_observation = _snapshot(client, parent_id).get(
                    "acp_session_observation", {}
                )
                assert current_root_observation.get("state") != {"present": False}
                assert (
                    current_root_observation.get("transport", {}).get("state") != "disconnected"
                ), current_root_observation
                assert _items(client, child_one) == reused_items
                for name in ("start-child-one", "start-child-two"):
                    (probe / name).touch()
                poll_until(
                    lambda: (
                        (probe / "finished-child-one").exists()
                        and (probe / "finished-child-two").exists()
                        and all(
                            _snapshot(client, child_id).get("status") == "idle"
                            for child_id in new_children
                        )
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )

                wire = [
                    json.loads(line)
                    for line in (probe / "wire.jsonl").read_text().splitlines()
                    if line.strip()
                ]
                assert not any(
                    "gemini-cli/subagent" in json.dumps(frame)
                    or "HARNESS_ACP_EXTENSION" in json.dumps(frame)
                    for frame in wire
                ), wire

                cli_env = _isolated_cli_env(
                    home=tmp_path / "export-cli-home",
                    config_home=config_home,
                    data_dir=tmp_path / "stack" / "export-cli-data",
                )

                def export_and_compare(session_id: str, filename: str) -> tuple[dict, list[dict]]:
                    export_path = tmp_path / filename
                    export = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "omnigent",
                            "session",
                            "export",
                            "--id",
                            session_id,
                            "--server",
                            stack.base_url,
                            "--output",
                            str(export_path),
                        ],
                        cwd=_REPO_ROOT,
                        env=cli_env,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    assert export.returncode == 0, export.stderr
                    rows = [
                        json.loads(line)
                        for line in export_path.read_text().splitlines()
                        if line.strip()
                    ]
                    metadata = next(
                        row for row in rows if row.get("record_type") == "session_meta"
                    )
                    exported_rows = [row for row in rows if row.get("record_type") == "item"]
                    api_items = _items(client, session_id)
                    # Export adds only the record_type wrapper. Compare the full
                    # ordered records so IDs, counts, metadata and payloads agree.
                    assert [
                        {key: value for key, value in item.items() if key != "record_type"}
                        for item in exported_rows
                    ] == api_items
                    assert [item["id"] for item in exported_rows] == [
                        item["id"] for item in api_items
                    ]
                    api_observation = _snapshot(client, session_id).get("acp_session_observation")
                    assert metadata.get("acp_session_observation") == api_observation
                    return metadata, exported_rows

                exported_meta, exported_items = export_and_compare(
                    child_one, "child-one-export.jsonl"
                )
                exported_observation = exported_meta["acp_session_observation"]
                assert exported_observation["session_info"] == {"title": "Native worker title"}
                assert exported_observation["usage"] == {"used": 8, "size": 200000}
                assert exported_observation["config_options"] is None
                assert exported_observation["mode"] == {"currentModeId": "plan"}
                assert exported_observation["transport"] == {
                    "state": "disconnected",
                    "reason": "stdout_eof",
                }
                exported_tool = next(
                    item
                    for item in exported_items
                    if item.get("type") == "function_call"
                    and item.get("name") == "inspect_first_fixture"
                )
                assert exported_tool["acp_session_references"] == [
                    {"native_session_id": "standard-child-two", "conversation_id": child_two}
                ], exported_tool
                exported_message = next(
                    item
                    for item in exported_items
                    if item.get("type") == "message"
                    and item.get("acp_session_message", {}).get("sender_message_id")
                    == "native-sender-message-one"
                )
                assert (
                    exported_message["acp_session_message"]
                    == directed_message["acp_session_message"]
                ), exported_message
                assert any(
                    _CHILD_OUTPUT["one"] in json.dumps(item, ensure_ascii=False)
                    for item in exported_items
                ), exported_items
                exported_two_meta, exported_two_items = export_and_compare(
                    child_two, "child-two-export.jsonl"
                )
                exported_two_observation = exported_two_meta.get("acp_session_observation", {})
                assert "usage" not in exported_two_observation, exported_two_observation
                assert "config_options" not in exported_two_observation, exported_two_observation
                assert any(
                    _CHILD_OUTPUT["two"] in json.dumps(item, ensure_ascii=False)
                    for item in exported_two_items
                ), exported_two_items
            finally:
                # Release the subprocess's background workers before deleting the session.
                (probe / "start-child-one").touch()
                (probe / "start-child-two").touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_runner_replacement_keeps_old_history_unconfirmed_and_uses_new_epoch(
    tmp_path: Path,
) -> None:
    """A replacement runner recovers the parent without reviving old child work."""
    probe = _write_standard_acp_fixture(tmp_path)
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "2.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name),
                metadata={"workspace": str(probe)},
                filename="runner-replacement.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Start two workers and return while they remain active.",
                )
                _wait_parent_reply(client, parent_id)
                poll_until(
                    lambda: (
                        len(_children(client, parent_id)) == 2
                        and all(child.get("busy") for child in _children(client, parent_id))
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                old_children = _children(client, parent_id)
                old_ids = {str(child["id"]) for child in old_children}
                assert len(old_ids) == 2, old_children
                original_agent_pid = int((probe / "agent.pid").read_text())
                original_runner_pid = stack.runner.pid
                assert original_runner_pid is not None

                stack.restart_runner()
                assert stack.runner is not None and stack.runner.pid != original_runner_pid
                assert stack.runner.poll() is None, stack.log_tail()
                import psutil

                poll_until(
                    lambda: not psutil.pid_exists(original_agent_pid),
                    timeout_s=_CHILD_TIMEOUT_S,
                )

                def old_children_are_disconnected() -> bool:
                    for child_id in old_ids:
                        snapshot = _snapshot(client, child_id)
                        observation = snapshot.get("acp_session_observation", {})
                        if observation.get("state") != {"present": False}:
                            return False
                        if observation.get("transport", {}).get("state") != "disconnected":
                            return False
                        if snapshot.get("status") == "failed":
                            return False
                    return True

                poll_until(old_children_are_disconnected, timeout_s=_CHILD_TIMEOUT_S)
                saved_old_items = {child_id: _items(client, child_id) for child_id in old_ids}
                for name in (
                    "parent-prompt-replied",
                    "finished-child-one",
                    "finished-child-two",
                    "permission-one-reply.json",
                    "permission-two-reply.json",
                    "fs-write-one-reply.json",
                    "fs-write-two-reply.json",
                    "permission-reply-one.json",
                    "permission-reply-two.json",
                    "fs-reply-one.json",
                    "fs-reply-two.json",
                    "child-one.txt",
                    "child-two.txt",
                    "prompt-count.txt",
                    "start-child-one",
                    "start-child-two",
                ):
                    with suppress(FileNotFoundError):
                        (probe / name).unlink()
                (probe / "replacement-mode").touch()

                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Continue with a new native connection after runner replacement.",
                )
                _wait_parent_reply(
                    client,
                    parent_id,
                    marker="STANDARD_REPLACEMENT_PARENT_REPLY",
                )
                poll_until(
                    lambda: (
                        int((probe / "agent.pid").read_text()) != original_agent_pid
                        and len(_children(client, parent_id)) == 4
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                replacement_children = _children(client, parent_id)
                replacement_ids = {str(child["id"]) for child in replacement_children}
                new_ids = replacement_ids - old_ids
                assert old_ids.issubset(replacement_ids), replacement_children
                assert len(new_ids) == 2, replacement_children
                assert all(
                    child.get("busy")
                    for child in replacement_children
                    if str(child["id"]) in new_ids
                ), replacement_children
                assert {
                    child_id: _items(client, child_id) for child_id in old_ids
                } == saved_old_items
                assert old_children_are_disconnected()
                current_root_observation = _snapshot(client, parent_id).get(
                    "acp_session_observation", {}
                )
                assert current_root_observation.get("state") != {"present": False}
                assert (
                    current_root_observation.get("transport", {}).get("state") != "disconnected"
                ), current_root_observation

                for name in ("start-child-one", "start-child-two"):
                    (probe / name).touch()
                poll_until(
                    lambda: all(
                        _snapshot(client, child_id).get("status") == "idle" for child_id in new_ids
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
            finally:
                (probe / "start-child-one").touch()
                (probe / "start-child-two").touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_harness_transport_loss_while_child_is_held_keeps_old_epoch_uncertain(
    tmp_path: Path,
) -> None:
    """An ACP EOF preserves a held child's history and opens a fresh child epoch."""
    import psutil

    probe = _write_standard_acp_fixture(tmp_path)
    config_home = tmp_path / "runner-config"
    acp_name = _write_acp_config(config_home, probe, runner_idle_timeout_s=60.0)
    env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    with server_runner(
        tmp_path / "stack",
        server_cwd=_REPO_ROOT,
        workspace=probe,
        server_env=env,
        health_timeout=_HEALTH_TIMEOUT_S,
        poll_interval=0.2,
    ) as stack:
        stack.start_runner(
            cwd=probe,
            env={
                **env,
                "OMNIGENT_CONFIG_HOME": str(config_home),
                "OMNIGENT_HARNESS_IDLE_TIMEOUT_S": "2.0",
            },
        )
        with httpx.Client(base_url=stack.base_url, timeout=20.0, trust_env=False) as client:
            created = post_session_bundle(
                client.post,
                "/v1/sessions",
                _session_bundle(acp_name),
                metadata={"workspace": str(probe)},
                filename="transport-loss-held-child.tar.gz",
                timeout=30.0,
            )
            created.raise_for_status()
            parent_id = str(created.json()["session_id"])
            try:
                bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Start both workers, finish one, and leave the other running.",
                )
                _wait_parent_reply(client, parent_id)
                poll_until(
                    lambda: (
                        len(_children(client, parent_id)) == 2
                        and all(child.get("busy") for child in _children(client, parent_id))
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                initial_children = _children(client, parent_id)
                child_one = next(
                    child
                    for child in initial_children
                    if "first fixture"
                    in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
                )
                child_two = next(
                    child
                    for child in initial_children
                    if "second fixture"
                    in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
                )
                old_ids = {str(child_one["id"]), str(child_two["id"])}
                original_agent_pid = int((probe / "agent.pid").read_text())

                (probe / "start-child-one").touch()
                poll_until(
                    lambda: (
                        (probe / "finished-child-one").exists()
                        and _snapshot(client, str(child_one["id"])).get("status") == "idle"
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                saved_old_items = {child_id: _items(client, child_id) for child_id in old_ids}
                assert _snapshot(client, str(child_two["id"])).get("status") == "running"

                (probe / "stop-agent-for-transport-probe").touch()

                def transport_loss_is_persisted(session_ids: tuple[str, ...]) -> bool:
                    for session_id in session_ids:
                        observation = _snapshot(client, session_id).get(
                            "acp_session_observation", {}
                        )
                        if observation.get("state") != {"present": False}:
                            return False
                        if observation.get("transport") != {
                            "state": "disconnected",
                            "reason": "stdout_eof",
                        }:
                            return False
                    return True

                poll_until(
                    lambda: transport_loss_is_persisted((parent_id, *sorted(old_ids))),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                assert (
                    _items(client, str(child_two["id"])) == saved_old_items[str(child_two["id"])]
                )
                assert _snapshot(client, str(child_two["id"])).get("status") != "failed"
                poll_until(
                    lambda: not psutil.pid_exists(original_agent_pid),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                original_runner_pid = stack.runner.pid
                assert original_runner_pid is not None
                stack.restart_runner()
                assert stack.runner is not None and stack.runner.pid != original_runner_pid

                for name in (
                    "parent-prompt-replied",
                    "finished-child-one",
                    "finished-child-two",
                    "permission-one-reply.json",
                    "permission-two-reply.json",
                    "fs-write-one-reply.json",
                    "fs-write-two-reply.json",
                    "permission-reply-one.json",
                    "permission-reply-two.json",
                    "fs-reply-one.json",
                    "fs-reply-two.json",
                    "child-one.txt",
                    "child-two.txt",
                    "prompt-count.txt",
                    "start-child-one",
                    "start-child-two",
                    "stop-agent-for-transport-probe",
                ):
                    with suppress(FileNotFoundError):
                        (probe / name).unlink()
                (probe / "replacement-mode").touch()
                send_user_text(
                    client,
                    session_id=parent_id,
                    text="Continue after the old child connection was lost.",
                )
                _wait_parent_reply(
                    client,
                    parent_id,
                    marker="STANDARD_REPLACEMENT_PARENT_REPLY",
                )
                poll_until(
                    lambda: (
                        int((probe / "agent.pid").read_text()) != original_agent_pid
                        and len(_children(client, parent_id)) == 4
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
                replacement_children = _children(client, parent_id)
                replacement_ids = {str(child["id"]) for child in replacement_children}
                new_ids = replacement_ids - old_ids
                assert len(new_ids) == 2, replacement_children
                assert all(
                    child.get("busy")
                    for child in replacement_children
                    if str(child["id"]) in new_ids
                ), replacement_children
                assert {
                    child_id: _items(client, child_id) for child_id in old_ids
                } == saved_old_items
                assert transport_loss_is_persisted(tuple(sorted(old_ids)))
                current_root_observation = _snapshot(client, parent_id).get(
                    "acp_session_observation", {}
                )
                assert current_root_observation.get("state") != {"present": False}
                assert (
                    current_root_observation.get("transport", {}).get("state") != "disconnected"
                ), current_root_observation

                for name in ("start-child-one", "start-child-two"):
                    (probe / name).touch()
                poll_until(
                    lambda: all(
                        _snapshot(client, child_id).get("status") == "idle" for child_id in new_ids
                    ),
                    timeout_s=_CHILD_TIMEOUT_S,
                )
            finally:
                (probe / "start-child-one").touch()
                (probe / "start-child-two").touch()
                with suppress(Exception):
                    client.delete(f"/v1/sessions/{parent_id}", timeout=10.0)


def test_run_cli_loads_setup_acp_agent_in_a_fresh_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The setup wizard's saved ACP agent can be selected by a real CLI run."""
    from click.testing import CliRunner

    from omnigent.cli import cli

    probe = _write_standard_acp_fixture(tmp_path)
    config_home = tmp_path / "cli-config"
    cli_home = tmp_path / "cli-home"
    config_home.mkdir()
    cli_home.mkdir()
    monkeypatch.setenv("OMNIGENT_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("HOME", str(cli_home))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("omnigent.cli_config._warn_missing_harness_dependencies", lambda: None)
    monkeypatch.setattr("omnigent.cli_config._adopt_ambient_credentials", list)

    configured = False

    def choose_acp_agent(title: str, options: list[str], **_kwargs: object) -> int:
        assert title == "Configure harnesses"
        if not configured:
            return next(
                index for index, option in enumerate(options) if "Custom ACP agent" in option
            )
        return next(index for index, option in enumerate(options) if option == "Quit")

    def answer_setup_prompt(label: str, **_kwargs: object) -> str:
        nonlocal configured
        if label.startswith("Agent name"):
            configured = True
            return "Standard Process Reporter"
        if label.startswith("Command to launch"):
            return shlex.join([sys.executable, str(probe / "agent.py"), str(probe)])
        if label.startswith("Model"):
            return ""
        raise AssertionError(f"Unexpected ACP setup prompt: {label}")

    monkeypatch.setattr("omnigent.onboarding.interactive.select", choose_acp_agent)
    monkeypatch.setattr("omnigent.onboarding.interactive.prompt_text", answer_setup_prompt)
    setup = CliRunner().invoke(cli, ["setup"])
    assert setup.exit_code == 0, setup.output
    persisted = yaml.safe_load((config_home / "config.yaml").read_text())
    assert persisted["acp"]["agents"] == [
        {
            "name": "Standard Process Reporter",
            "command": shlex.join([sys.executable, str(probe / "agent.py"), str(probe)]),
        }
    ]

    data_dir = tmp_path / "cli-data"
    cli_env = _isolated_cli_env(home=cli_home, config_home=config_home, data_dir=data_dir)
    cli_env["OMNIGENT_HARNESS_IDLE_TIMEOUT_S"] = "2.0"
    stdout_path = tmp_path / "run.stdout"
    stderr_path = tmp_path / "run.stderr"
    run_process: subprocess.Popen[bytes] | None = None
    try:
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            run_process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "omnigent",
                    "run",
                    "--harness",
                    "acp:standard-process-reporter",
                    "-p",
                    "Start the workers and return the parent response.",
                ],
                cwd=_REPO_ROOT,
                env=cli_env,
                stdout=stdout,
                stderr=stderr,
            )
            try:
                return_code = run_process.wait(timeout=150)
            except subprocess.TimeoutExpired as exc:
                terminate_process(run_process, timeout_s=5)
                captured = stdout_path.read_text(errors="replace") + stderr_path.read_text(
                    errors="replace"
                )
                raise AssertionError(
                    "The one-shot ACP CLI did not return within its bound.\n" + captured[-5000:]
                ) from exc

        combined_output = stdout_path.read_text(errors="replace") + stderr_path.read_text(
            errors="replace"
        )
        assert return_code == 0, combined_output[-5000:]
        assert _PARENT_REPLY in combined_output, combined_output[-5000:]
        session_url_match = re.search(r"Omnigent session:\s+(https?://[^\s]+)", combined_output)
        assert session_url_match is not None, combined_output[-5000:]
        session_url = session_url_match.group(1)
        url = urlsplit(session_url)
        parent_match = re.search(r"/c/([^/?#\s]+)", url.path)
        assert parent_match is not None, session_url
        parent_id = parent_match.group(1)
        base_url = f"{url.scheme}://{url.netloc}"

        assert (probe / "initialized.json").exists()
        initialized = json.loads((probe / "initialized.json").read_text())
        assert "subagents" in initialized, initialized
        assert initialized.get("terminal") is False, initialized
        agent_pid = int((probe / "agent.pid").read_text())
        poll_until(lambda: not psutil.pid_exists(agent_pid), timeout_s=_CHILD_TIMEOUT_S)
        host_record = json.loads(daemon_record_path("local", base_dir=data_dir).read_text())
        host_pid = int(host_record["pid"])
        assert psutil.pid_exists(host_pid), host_record

        with httpx.Client(base_url=base_url, timeout=20.0, trust_env=False) as client:
            parent = _snapshot(client, parent_id)
            runner_id = parent.get("runner_id")
            assert isinstance(runner_id, str) and runner_id
            assert parent.get("status") != "running", parent
            parent_items = _items(client, parent_id)
            assert _PARENT_REPLY in json.dumps(parent_items, ensure_ascii=False), parent_items
            children = _children(client, parent_id)
            assert len(children) == 2, children
            # The fixture workers remain gated when this one-shot exits.
            assert all(child.get("busy") is True for child in children), children
            child_snapshots = [_snapshot(client, str(child["id"])) for child in children]
            assert all(snapshot.get("status") != "failed" for snapshot in child_snapshots)
            child_observations = [
                snapshot.get("acp_session_observation") or {} for snapshot in child_snapshots
            ]
            assert all(
                observation.get("transport_unconfirmed") is True
                and observation.get("state", {}).get("present") is True
                and observation.get("state", {}).get("value") == {"state": "running"}
                for observation in child_observations
            ), child_snapshots
            runner_status = client.get(f"/v1/runners/{runner_id}/status", timeout=10.0)
            runner_status.raise_for_status()
            assert runner_status.json().get("online") is False, runner_status.json()
    finally:
        for name in ("start-child-one", "start-child-two"):
            (probe / name).touch()
        if run_process is not None and run_process.poll() is None:
            terminate_process(run_process, timeout_s=5)
        _stop_local_cli_runtime(cli_env, data_dir)


def test_interactive_cli_stays_open_through_acp_child_completion(tmp_path: Path) -> None:
    """A local interactive run stays attached while ACP children finish."""
    pexpect = pytest.importorskip("pexpect")
    from tests._helpers.terminal_screen import TerminalScreen

    probe = _write_standard_acp_fixture(tmp_path)
    config_home = tmp_path / "interactive-config"
    acp_name = _write_acp_config(config_home, probe, runner_idle_timeout_s=60.0)
    persisted = yaml.safe_load((config_home / "config.yaml").read_text())
    persisted["auto_open_conversation"] = False
    persisted["tui"] = {"theme": "dark"}
    (config_home / "config.yaml").write_text(yaml.safe_dump(persisted))
    interactive_agent_spec = tmp_path / "interactive-agent"
    interactive_agent_spec.mkdir()
    (interactive_agent_spec / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "spec_version": 1,
                "name": "interactive-acp-child-lifetime-test",
                "os_env": {
                    "type": "caller_process",
                    "cwd": ".",
                    "sandbox": {
                        "type": "none",
                        "read_paths": ["."],
                        "write_paths": ["."],
                    },
                },
                "executor": {
                    "type": "omnigent",
                    "config": {
                        "harness": f"acp:{acp_name}",
                        "permission_mode": "bypassPermissions",
                    },
                },
            }
        )
    )
    cli_home = tmp_path / "interactive-home"
    data_dir = tmp_path / "interactive-data"
    cli_env = _isolated_cli_env(
        home=cli_home,
        config_home=config_home,
        data_dir=data_dir,
    )
    cli_env["OMNIGENT_HARNESS_IDLE_TIMEOUT_S"] = "2.0"
    rows, columns = 35, 100
    cli_env.update(
        {
            "TERM": "xterm-256color",
            "LINES": str(rows),
            "COLUMNS": str(columns),
            "PROMPT_TOOLKIT_NO_CPR": "1",
        }
    )
    repl = pexpect.spawn(
        sys.executable,
        ["-m", "omnigent", "run", str(interactive_agent_spec)],
        env=cli_env,
        cwd=str(probe),
        encoding="utf-8",
        codec_errors="replace",
        timeout=120,
        dimensions=(rows, columns),
    )
    screen = TerminalScreen(repl)
    transcript_path = tmp_path / "interactive-cli-pty.log"
    transcript_file = transcript_path.open("w", encoding="utf-8")
    transcript_chunks: list[str] = []

    class _TranscriptFanout:
        def write(self, data: str) -> None:
            screen.write(data)
            transcript_chunks.append(data)
            transcript_file.write(data)
            transcript_file.flush()

        def flush(self) -> None:
            transcript_file.flush()

    repl.logfile_read = _TranscriptFanout()
    agent_pid: int | None = None
    try:
        screen.wait_for(
            lambda text: "❯" in text and "interactive acp child lifetime test" in text,
            timeout=120,
            what="the agent title and interactive input prompt",
        )

        cli_log_dir = data_dir / "logs" / "cli"

        def cli_session_link() -> str | None:
            process_logs = list(cli_log_dir.glob(f"cli-*-{repl.pid:05d}-*.log"))
            if len(process_logs) != 1:
                return None
            cli_log_path = process_logs[0]
            cli_log_text = cli_log_path.read_text(encoding="utf-8", errors="replace")
            if (
                f"pid={repl.pid}" not in cli_log_text
                or str(interactive_agent_spec) not in cli_log_text
            ):
                return None
            session_match = re.search(r"Omnigent session:\s+(https?://[^\s]+)", cli_log_text)
            if session_match is None:
                return None
            return session_match.group(1)

        poll_until(lambda: cli_session_link() is not None, timeout_s=15.0, interval_s=0.1)
        session_url_text = cli_session_link()
        assert session_url_text is not None
        session_url = urlsplit(session_url_text)
        parent_match = re.search(r"/c/([^/?#\s]+)", session_url.path)
        assert parent_match is not None, session_url_text
        parent_id = parent_match.group(1)
        base_url = f"{session_url.scheme}://{session_url.netloc}"

        with httpx.Client(base_url=base_url, timeout=20.0, trust_env=False) as client:
            session = _snapshot(client, parent_id)
            assert session.get("id") == parent_id, session
            assert session.get("agent_name") == "interactive-acp-child-lifetime-test", session
            assert session.get("harness") == "acp", session
            workspace = session.get("workspace")
            assert isinstance(workspace, str) and Path(workspace).resolve() == probe.resolve(), (
                session
            )
            assert session.get("status") == "idle", session
            assert _items(client, parent_id) == []

            repl.send("Start background workers and return the parent response.")
            repl.send("\r")
            screen.wait_for(
                lambda text: _PARENT_REPLY in text and "❯" in text,
                timeout=_TURN_TIMEOUT_S,
                what="the parent response and next interactive prompt",
            )
            _wait_parent_reply(client, parent_id)
            poll_until(
                lambda: (
                    len(_children(client, parent_id)) == 2
                    and all(child.get("busy") for child in _children(client, parent_id))
                ),
                timeout_s=_CHILD_TIMEOUT_S,
            )
            children = _children(client, parent_id)
            child_one = next(
                str(child["id"])
                for child in children
                if "first fixture"
                in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
            )
            child_two = next(
                str(child["id"])
                for child in children
                if "second fixture"
                in child.get("labels", {}).get("omnigent.acp.subagent_description", "")
            )
            agent_pid = int((probe / "agent.pid").read_text())
            parent = _snapshot(client, parent_id)
            runner_id = parent.get("runner_id")
            assert isinstance(runner_id, str) and runner_id, parent
            runner_status = client.get(f"/v1/runners/{runner_id}/status", timeout=10.0)
            runner_status.raise_for_status()
            assert runner_status.json().get("online") is True, runner_status.json()
            host_record = json.loads(daemon_record_path("local", base_dir=data_dir).read_text())
            assert psutil.pid_exists(int(host_record["pid"])), host_record
            assert repl.isalive(), "interactive run exited while ACP children were active"

            (probe / "start-child-one").touch()
            poll_until(
                lambda: (
                    (probe / "finished-child-one").exists()
                    and _snapshot(client, child_one).get("status") == "idle"
                    and _snapshot(client, child_two).get("status") == "running"
                ),
                timeout_s=_CHILD_TIMEOUT_S,
            )
            one_items = _items(client, child_one)
            assert any(
                item.get("type") == "function_call_output"
                and _CHILD_OUTPUT["one"] in json.dumps(item)
                for item in one_items
            ), one_items
            assert repl.isalive(), "interactive run exited as background child work completed"
            assert "❯" in screen.text(), screen.text()

            (probe / "start-child-two").touch()
            poll_until(
                lambda: (
                    (probe / "finished-child-two").exists()
                    and _snapshot(client, child_two).get("status") == "idle"
                ),
                timeout_s=_CHILD_TIMEOUT_S,
            )
            two_items = _items(client, child_two)
            assert any(
                item.get("type") == "function_call_output"
                and _CHILD_OUTPUT["two"] in json.dumps(item)
                for item in two_items
            ), two_items
            poll_until(
                lambda: _all_descendants_idle(client, parent_id),
                timeout_s=_CHILD_TIMEOUT_S,
            )
            descendants = _descendants(client, parent_id)
            assert descendants and all(child.get("busy") is False for child in descendants), (
                descendants
            )
            assert repl.isalive(), "interactive run exited after both children completed"
            assert "❯" in screen.text(), screen.text()
            assert psutil.pid_exists(agent_pid), "the interactive ACP process exited too early"
    finally:
        (probe / "start-child-one").touch()
        (probe / "start-child-two").touch()
        with suppress(pexpect.ExceptionPexpect, OSError):
            repl.sendcontrol("d")
            repl.expect(pexpect.EOF, timeout=15)
        if repl.isalive():
            repl.terminate(force=True)
        _stop_local_cli_runtime(cli_env, data_dir)
        if agent_pid is not None:
            poll_until(lambda: not psutil.pid_exists(agent_pid), timeout_s=_CHILD_TIMEOUT_S)
        transcript_file.close()
