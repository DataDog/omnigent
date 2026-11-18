"""Run the built Vitrine ACP producer through Omnigent's server and runner.

Set ``OMNIGENT_TEST_VIT_RUNTIME_BINARY`` to the Linux arm64 runtime artifact,
``OMNIGENT_TEST_VIT_RUNTIME_DRIVER`` to Vitrine's stdio gateway driver, and
``OMNIGENT_TEST_VIT_GATEWAY_HOST`` to the gateway hostname used by that driver.
Set ``OMNIGENT_TEST_VIT_FLAG_HOST`` to the hostname used for its precomputed
feature flag. The test maps that hostname to loopback in its network-isolated
container and uses a deterministic local fake model gateway.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from tests._helpers.polling import poll_until
from tests._helpers.server_runner import server_runner
from tests._helpers.session import (
    bind_session_runner,
    bundle_files,
    post_session_bundle,
    send_user_text,
)

_APP_ROOT = Path(__file__).resolve().parents[2]
_BINARY_ENV = "OMNIGENT_TEST_VIT_RUNTIME_BINARY"
_DRIVER_ENV = "OMNIGENT_TEST_VIT_RUNTIME_DRIVER"
_GATEWAY_HOST_ENV = "OMNIGENT_TEST_VIT_GATEWAY_HOST"
_FLAG_HOST_ENV = "OMNIGENT_TEST_VIT_FLAG_HOST"
_HOSTNAME_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_NODE_IMAGE = "node:26.12.0"
_INITIAL_PROMPT = "Read the two marker files with the guided workflow. Keep the files unchanged."
_PARENT_REPLY = "Working has started with both Workers active."
_FEEDBACK = "Continue working on the task."
_CHILD_MARKERS = ("ALPHA-447", "BETA-882")


def _docker(args: list[str], *, timeout: float = 60.0) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )
    assert result.returncode == 0, (
        f"docker {' '.join(args)} failed with {result.returncode}:\n"
        f"stdout:\n{result.stdout[-3000:]}\nstderr:\n{result.stderr[-3000:]}"
    )
    return result


def _docker_endpoint_for_isolated_runner(docker_bin: str) -> str:
    """Resolve the caller's local Docker socket for the runner's isolated HOME."""
    context = os.environ.get("DOCKER_CONTEXT") or None
    endpoint = (os.environ.get("DOCKER_HOST") or None) if context is None else None
    if context is None and endpoint is None:
        selected = subprocess.run(
            [docker_bin, "context", "show"], capture_output=True, text=True, check=False
        )
        if selected.returncode != 0 or not selected.stdout.strip():
            pytest.fail("could not resolve the selected Docker context for the isolated runner")
        context = selected.stdout.strip()
    if context is not None:
        inspected = subprocess.run(
            [
                docker_bin,
                "context",
                "inspect",
                context,
                "--format",
                '{{ (index .Endpoints "docker").Host }}',
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        endpoints = [line.strip() for line in inspected.stdout.splitlines() if line.strip()]
        if inspected.returncode != 0 or len(endpoints) != 1:
            pytest.fail("the selected Docker context has no single usable Docker endpoint")
        endpoint = endpoints[0]
    if endpoint is None or not endpoint.startswith("unix://"):
        pytest.fail(
            "the isolated Vit runner supports only a local unix:// Docker endpoint; "
            "remote or credential-backed Docker contexts are unsupported"
        )
    socket_path = Path(endpoint.removeprefix("unix://"))
    if not socket_path.is_absolute() or not socket_path.is_socket():
        pytest.fail("the selected Docker unix socket is not available to the isolated runner")
    return endpoint


def _docker_exec(container: str, *args: str, timeout: float = 30.0) -> str:
    return _docker(["exec", container, *args], timeout=timeout).stdout


def _container_has_file(container: str, path: str) -> bool:
    result = subprocess.run(
        ["docker", "exec", container, "test", "-f", path],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode in (0, 1), result.stderr
    return result.returncode == 0


def _copy_numbered_capture(source: Path, destination: Path) -> None:
    rows = []
    for ordinal, line in enumerate(source.read_text().splitlines(), start=1):
        if line.strip():
            rows.append({"ordinal": ordinal, "frame": json.loads(line)})
    destination.write_text("".join(f"{json.dumps(row, ensure_ascii=False)}\n" for row in rows))


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _snapshot(client: httpx.Client, session_id: str) -> dict[str, Any]:
    response = client.get(f"/v1/sessions/{session_id}", timeout=10.0)
    response.raise_for_status()
    return response.json()


def _has_retired_acp_observation(snapshot: dict[str, Any]) -> bool:
    observation = snapshot.get("acp_session_observation")
    return (
        isinstance(observation, dict)
        and observation.get("state") == {"present": False}
        and observation.get("transport") == {"state": "disconnected", "reason": "stdout_eof"}
        and observation.get("source_current") is False
    )


def _children(client: httpx.Client, session_id: str) -> list[dict[str, Any]]:
    response = client.get(
        f"/v1/sessions/{session_id}/child_sessions",
        params={"order": "asc"},
        timeout=10.0,
    )
    response.raise_for_status()
    return response.json().get("data", [])


def _items(client: httpx.Client, session_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
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


def _message_text(item: dict[str, Any]) -> str:
    return " ".join(
        block.get("text", "")
        for block in item.get("content", [])
        if isinstance(block, dict) and isinstance(block.get("text"), str)
    )


def _assistant_has(snapshot: dict[str, Any], text: str) -> bool:
    return any(
        item.get("type") == "message"
        and item.get("data", {}).get("role") == "assistant"
        and text in _message_text(item.get("data", {}))
        for item in snapshot.get("items", [])
    )


def _contains(items: list[dict[str, Any]], text: str) -> bool:
    return text in json.dumps(items, ensure_ascii=False)


def _vitrine_root(driver: Path) -> Path:
    for parent in driver.parents:
        package = parent / "package.json"
        if package.is_file():
            metadata = json.loads(package.read_text())
            if metadata.get("name") == "vitrine":
                return parent
    raise AssertionError(f"could not find Vitrine package root for {driver}")


def _prepare_container(
    *,
    container: str,
    binary: Path,
    driver: Path,
    vitrine_root: Path,
    workspace: Path,
    fixture: Path,
    flag_host: str,
) -> None:
    try:
        _docker(
            [
                "run",
                "--detach",
                "--network",
                "none",
                "--add-host",
                f"{flag_host}:127.0.0.1",
                "--name",
                container,
                "--platform",
                "linux/arm64",
                "--entrypoint",
                "sleep",
                _NODE_IMAGE,
                "infinity",
            ],
            timeout=120,
        )
        _docker_exec(
            container,
            "mkdir",
            "-p",
            "/probe/src/vit/acp/test-support",
            "/probe/e2e/helpers",
            "/probe/scripts/vit-package/omni",
            "/probe/scripts/vit-package/omni/test-support",
            "/probe/project-fixture",
            "/probe/control",
            "/probe/capture",
            "/probe/home",
            str(workspace),
        )
        copy_pairs = [
            (binary, "/probe/vit-runtime"),
            (driver, "/probe/src/vit/acp/test-support/subagent-delegation-stdio-driver.mjs"),
            (
                driver.parent / "subagent-delegation-gateway.mjs",
                "/probe/src/vit/acp/test-support/subagent-delegation-gateway.mjs",
            ),
            (
                vitrine_root / "scripts/vit-package/omni/test-support/precompute-flag-fixture.mjs",
                "/probe/scripts/vit-package/omni/test-support/precompute-flag-fixture.mjs",
            ),
            (
                vitrine_root / "scripts/vit-package/omni/test-support/precompute-flag-smoke.mjs",
                "/probe/scripts/vit-package/omni/test-support/precompute-flag-smoke.mjs",
            ),
            (
                vitrine_root / "e2e/helpers/fake-ai-gateway.ts",
                "/probe/e2e/helpers/fake-ai-gateway.ts",
            ),
            (
                vitrine_root / "scripts/vit-package/omni/acp-tap.mjs",
                "/probe/scripts/vit-package/omni/acp-tap.mjs",
            ),
            (
                vitrine_root / "scripts/vit-package/omni/acp-tap-request-gate.mjs",
                "/probe/scripts/vit-package/omni/acp-tap-request-gate.mjs",
            ),
        ]
        for source, destination in copy_pairs:
            assert source.is_file(), f"missing Vitrine test dependency: {source}"
            _docker(["cp", str(source), f"{container}:{destination}"])
        _docker(["cp", f"{fixture}/.", f"{container}:/probe/project-fixture"])
        _docker_exec(container, "chmod", "700", "/probe/vit-runtime")
        _docker_exec(container, "cp", "-R", "/probe/project-fixture/.", str(workspace))
        _docker_exec(
            container, "git", "config", "--global", "--add", "safe.directory", str(workspace)
        )
        _docker_exec(container, "git", "-C", str(workspace), "init", "-q")
        _docker_exec(container, "git", "-C", str(workspace), "config", "user.name", "agent")
        _docker_exec(
            container,
            "git",
            "-C",
            str(workspace),
            "config",
            "user.email",
            "agent@example.test",
        )
        _docker_exec(
            container,
            "git",
            "-C",
            str(workspace),
            "config",
            "core.fsmonitor",
            "false",
        )
        _docker_exec(
            container,
            "git",
            "-C",
            str(workspace),
            "add",
            "--",
            "AGENTS.md",
            "alpha.txt",
            "beta.txt",
            ".vitrine/roles.json",
        )
        _docker_exec(container, "git", "-C", str(workspace), "commit", "-m", "Add marker fixture")
    except BaseException:
        subprocess.run(
            ["docker", "rm", "--force", container],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        raise


def _set_stage(container: str, tmp_path: Path, stage: str) -> None:
    local = tmp_path / f"stage-{stage}"
    local.write_text(f"{stage}\n")
    _docker(["cp", str(local), f"{container}:/probe/control/stage"])


def _release_worker(container: str, key: str) -> None:
    assert key in {"alpha", "beta"}
    _docker_exec(container, "touch", f"/probe/control/release-worker-{key}")


def _release_all_workers(container: str) -> None:
    subprocess.run(
        [
            "docker",
            "exec",
            container,
            "touch",
            "/probe/control/release-worker-alpha",
            "/probe/control/release-worker-beta",
            "/probe/control/release-bash-beta",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def _kill_producer(container: str) -> dict[str, Any]:
    process = json.loads(_docker_exec(container, "cat", "/probe/control/producer-process.json"))
    pid = process.get("pid")
    assert isinstance(pid, int) and pid > 0, process
    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "node",
            "-e",
            "process.kill(-Number(process.argv[1]), 'SIGKILL')",
            str(pid),
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, (
        f"could not kill Vitrine producer group {pid}: {result.stdout}\n{result.stderr}"
    )
    return process


def _open_tool_calls(items: list[dict[str, Any]], marker: str) -> list[dict[str, Any]]:
    """Return ACP calls whose native lifecycle status is still in progress."""
    return [
        item
        for item in items
        if item.get("type") == "function_call"
        and marker in json.dumps(item, ensure_ascii=False)
        and item.get("tool_status") == "in_progress"
    ]


def _collect_container_evidence(container: str, evidence: Path) -> None:
    evidence.mkdir(parents=True, exist_ok=True)
    for source, destination in (
        ("/probe/capture", evidence / "capture"),
        ("/probe/control", evidence / "control"),
    ):
        result = subprocess.run(
            ["docker", "cp", f"{container}:{source}", str(destination)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if result.returncode != 0:
            (evidence / f"{Path(source).name}-copy-error.txt").write_text(
                f"{result.stdout}\n{result.stderr}"
            )
    client_wire = evidence / "capture/acp.client-to-agent.ndjson"
    agent_wire = evidence / "capture/acp.agent-to-client.ndjson"
    if client_wire.is_file():
        _copy_numbered_capture(client_wire, evidence / "acp-client-numbered.jsonl")
    if agent_wire.is_file():
        _copy_numbered_capture(agent_wire, evidence / "acp-agent-numbered.jsonl")


def _isolated_cli_env(*, home: Path, config_home: Path, data_dir: Path) -> dict[str, str]:
    home.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    env = {
        name: os.environ[name]
        for name in ("LANG", "LC_ALL", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR")
        if name in os.environ
    }
    path_entries = [str(Path(sys.executable).parent), *os.defpath.split(os.pathsep)]
    env["PATH"] = os.pathsep.join(dict.fromkeys(path_entries))
    env.update(
        {
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
                [str(_APP_ROOT), str(_APP_ROOT / "sdks" / "python-client")]
            ),
        }
    )
    return env


def _session_bundle(harness_name: str) -> bytes:
    spec = {
        "spec_version": 1,
        "name": "vit-runtime-subagent-reporting",
        "executor": {
            "type": "omnigent",
            "config": {"harness": f"acp:{harness_name}", "permission_mode": "bypassPermissions"},
        },
        "os_env": {
            "type": "caller_process",
            "cwd": ".",
            "sandbox": {"type": "none", "read_paths": ["."], "write_paths": ["."]},
        },
        "prompt": "Read the two marker files with the guided workflow.",
    }
    return bundle_files({"config.yaml": yaml.safe_dump(spec).encode()})


def test_built_vit_runtime_reports_reused_children_through_omnigent(
    tmp_path: Path,
) -> None:
    binary_value = os.environ.get(_BINARY_ENV)
    driver_value = os.environ.get(_DRIVER_ENV)
    gateway_host = os.environ.get(_GATEWAY_HOST_ENV)
    flag_host = os.environ.get(_FLAG_HOST_ENV)
    if not binary_value or not driver_value or not gateway_host or not flag_host:
        pytest.skip(
            f"set {_BINARY_ENV}, {_DRIVER_ENV}, {_GATEWAY_HOST_ENV}, and {_FLAG_HOST_ENV} "
            "to run the real-producer lane"
        )
    if len(flag_host) > 253 or not all(
        _HOSTNAME_LABEL.fullmatch(label) for label in flag_host.split(".")
    ):
        pytest.fail(f"{_FLAG_HOST_ENV} must be a valid hostname")
    if shutil.which("docker") is None:
        pytest.skip("Docker is required for the Linux arm64 Vitrine runtime process")

    binary = Path(binary_value).expanduser().resolve()
    driver = Path(driver_value).expanduser().resolve()
    assert binary.is_file(), f"{_BINARY_ENV} does not point to a file: {binary}"
    assert driver.is_file(), f"{_DRIVER_ENV} does not point to a file: {driver}"
    vitrine_root = _vitrine_root(driver)
    fixture = driver.parent / "subagent-delegation-fixture"
    assert fixture.is_dir(), f"missing Vitrine scenario fixture: {fixture}"

    evidence = tmp_path / "vit-runtime-evidence"
    evidence.mkdir()
    workspace = (tmp_path / "stack" / "workspace").resolve()
    workspace.mkdir(parents=True)
    for name in ("AGENTS.md", "alpha.txt", "beta.txt"):
        shutil.copy2(fixture / name, workspace / name)

    stack_root = tmp_path / "stack"
    config_home = tmp_path / "runner-config"
    config_home.mkdir()
    container = f"omnigent-vit-runtime-{os.getpid()}-{tmp_path.name[-8:]}"
    docker_bin = shutil.which("docker")
    assert docker_bin is not None
    docker_endpoint = _docker_endpoint_for_isolated_runner(docker_bin)
    command = shlex.join(
        [
            docker_bin,
            "--host",
            docker_endpoint,
            "exec",
            "-i",
            container,
            "node",
            "/probe/src/vit/acp/test-support/subagent-delegation-stdio-driver.mjs",
            "--binary",
            "/probe/vit-runtime",
            "--cwd",
            str(workspace),
            "--gateway-host",
            gateway_host,
            "--control-dir",
            "/probe/control",
            "--capture-dir",
            "/probe/capture",
            "--home",
            "/probe/home",
        ]
    )
    harness_name = "vit-runtime-test"
    config = {
        "acp": {
            "agents": [
                {
                    "name": "Vit runtime test",
                    "command": command,
                    "env_passthrough": [],
                    "inject_system_prompt": False,
                    "omnigent_mcp": False,
                }
            ]
        }
    }
    (config_home / "config.yaml").write_text(yaml.safe_dump(config))
    _prepare_container(
        container=container,
        binary=binary,
        driver=driver,
        vitrine_root=vitrine_root,
        workspace=workspace,
        fixture=fixture,
        flag_host=flag_host,
    )

    server_env = {"OMNIGENT_AUTH_PROVIDER": "header", "OMNIGENT_AUTH_ENABLED": "0"}
    parent_id: str | None = None
    try:
        with server_runner(
            stack_root,
            server_env=server_env,
            server_cwd=_APP_ROOT,
            workspace=workspace,
            health_timeout=60,
            poll_interval=0.2,
        ) as stack:
            stack.start_runner(
                cwd=workspace, env={**server_env, "OMNIGENT_CONFIG_HOME": str(config_home)}
            )
            with httpx.Client(base_url=stack.base_url, timeout=20, trust_env=False) as client:
                created = post_session_bundle(
                    client.post,
                    "/v1/sessions",
                    _session_bundle(harness_name),
                    filename="vit-runtime-acp.tar.gz",
                    timeout=30,
                )
                assert created.is_success, created.text
                parent_id = str(created.json()["session_id"])
                try:
                    bind_session_runner(client.patch, stack.base_url, parent_id, stack.runner_id)
                    send_user_text(client, session_id=parent_id, text=_INITIAL_PROMPT)

                    poll_until(
                        lambda: _container_has_file(
                            container, "/probe/control/precompute-flag-smoke.json"
                        ),
                        timeout_s=90,
                        interval_s=0.2,
                    )
                    flag_smoke = json.loads(
                        _docker_exec(
                            container,
                            "cat",
                            "/probe/control/precompute-flag-smoke.json",
                        )
                    )
                    assert flag_smoke.get("outcome") == "passed", flag_smoke
                    assert (
                        flag_smoke.get("binarySha256")
                        == hashlib.sha256(binary.read_bytes()).hexdigest()
                    ), flag_smoke
                    flag_request = flag_smoke.get("request", {})
                    assert flag_request.get("accepted") is True, flag_smoke
                    assert flag_request.get("method") == "POST", flag_smoke
                    assert flag_request.get("sourceSdkName") == "vitrine-server-host", flag_smoke
                    assert flag_request.get("sourceSdkVersion") == "1.0.0", flag_smoke
                    assert flag_request.get("environment") == "dev", flag_smoke
                    assert flag_request.get("clientTokenHeaderPresent") is True, flag_smoke
                    assert flag_request.get("applicationIdHeaderPresent") is True, flag_smoke
                    assert flag_smoke.get("snapshot", {}).get("testConfig") is False, flag_smoke
                    preflight_organic = flag_smoke.get("snapshot", {}).get("organic", {})
                    assert preflight_organic.get("status") == "ready", flag_smoke
                    assert preflight_organic.get("value") is True, flag_smoke
                    assert preflight_organic.get("reason") == "TARGETING_MATCH", flag_smoke
                    assert (
                        preflight_organic.get("allocationKey") == "vit-test-organic-flow-fixture"
                    ), flag_smoke
                    assert preflight_organic.get("overridden") is False, flag_smoke
                    assert preflight_organic.get("receivedAt", 0) >= flag_smoke.get(
                        "startedAt", 0
                    ), flag_smoke

                    def actual_flag_request_is_ready() -> bool:
                        rows = _read_ndjson_from_container(
                            container, "/probe/control/precompute-fixture-events.jsonl"
                        )
                        return any(
                            row.get("accepted") is True
                            and row.get("method") == "POST"
                            and row.get("sourceSdkName") == "vitrine-server-host"
                            and row.get("clientTokenHeaderPresent") is True
                            and row.get("applicationIdHeaderPresent") is True
                            for row in rows
                        )

                    poll_until(actual_flag_request_is_ready, timeout_s=90, interval_s=0.2)
                    actual_flag_events = _read_ndjson_from_container(
                        container, "/probe/control/precompute-fixture-events.jsonl"
                    )
                    (evidence / "precompute-flag-gate.json").write_text(
                        json.dumps(
                            {
                                "preflight": flag_smoke,
                                "actualRuntimeRequests": actual_flag_events,
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                    def intent_is_visible() -> bool:
                        snapshot = _snapshot(client, parent_id)
                        serialized = json.dumps(snapshot.get("items", []), ensure_ascii=False)
                        return "Does this capture what you want?" in serialized

                    poll_until(intent_is_visible, timeout_s=120, interval_s=0.2)
                    _set_stage(container, tmp_path, "intent-approval")
                    send_user_text(client, session_id=parent_id, text="Yes, that’s right.")
                    poll_until(
                        lambda: _container_has_file(container, "/probe/control/workers-held"),
                        timeout_s=180,
                        interval_s=0.2,
                    )

                    observed: dict[str, Any] = {}

                    def parent_idle_with_workers() -> bool:
                        snapshot = _snapshot(client, parent_id)
                        if snapshot.get("status") != "idle" or not _assistant_has(
                            snapshot, _PARENT_REPLY
                        ):
                            return False
                        children = _children(client, parent_id)
                        if len(children) != 2 or not all(
                            child.get("busy") is True for child in children
                        ):
                            return False
                        observed.update(parent=snapshot, children=children)
                        return True

                    poll_until(parent_idle_with_workers, timeout_s=45, interval_s=0.2)
                    initial_children = observed["children"]
                    initial_ids = {str(child["id"]) for child in initial_children}
                    assert len(initial_ids) == 2, initial_children
                    before_release = {
                        child["id"]: _items(client, str(child["id"])) for child in initial_children
                    }
                    assert all(before_release.values()), before_release
                    child_id_by_key: dict[str, str] = {}
                    for child in initial_children:
                        child_id = str(child["id"])
                        transcript = json.dumps(
                            {"child": child, "items": before_release[child["id"]]},
                            ensure_ascii=False,
                        )
                        for key in ("alpha", "beta"):
                            if f"{key}.txt" in transcript:
                                assert key not in child_id_by_key, (key, child_id_by_key, child)
                                child_id_by_key[key] = child_id
                    assert set(child_id_by_key) == {"alpha", "beta"}, (
                        child_id_by_key,
                        initial_children,
                        before_release,
                    )
                    assert all(child.get("busy") is True for child in _children(client, parent_id))
                    (evidence / "parent-idle-workers-held.json").write_text(
                        json.dumps(
                            {
                                "parent": observed["parent"],
                                "children": initial_children,
                                "child_items": before_release,
                                "workers_held": _docker_exec(
                                    container, "cat", "/probe/control/workers-held"
                                ),
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                    _release_worker(container, "alpha")

                    observed_alpha_pass: dict[str, Any] = {}

                    def alpha_finished_while_beta_held() -> bool:
                        events = _read_ndjson_from_container(
                            container, "/probe/control/model-progress.jsonl"
                        )
                        release = next(
                            (
                                row
                                for row in events
                                if row.get("kind") == "worker-released"
                                and row.get("key") == "alpha"
                                and row.get("stage") == "intent-approval"
                            ),
                            None,
                        )
                        completion = next(
                            (
                                row
                                for row in events
                                if row.get("kind") == "worker-completed"
                                and row.get("key") == "alpha"
                                and row.get("stage") == "intent-approval"
                                and release is not None
                                and row.get("ordinal", 0) > release.get("ordinal", 0)
                            ),
                            None,
                        )
                        current_children = {
                            str(child["id"]): child for child in _children(client, parent_id)
                        }
                        alpha_snapshot = _snapshot(client, child_id_by_key["alpha"])
                        beta_snapshot = _snapshot(client, child_id_by_key["beta"])
                        if (
                            release is None
                            or completion is None
                            or alpha_snapshot.get("status") != "idle"
                            or current_children.get(child_id_by_key["alpha"], {}).get("busy")
                            is not False
                            or beta_snapshot.get("status") == "idle"
                            or current_children.get(child_id_by_key["beta"], {}).get("busy")
                            is not True
                        ):
                            return False
                        observed_alpha_pass.update(
                            events=events,
                            release=release,
                            completion=completion,
                            alpha=alpha_snapshot,
                            beta=beta_snapshot,
                            children=list(current_children.values()),
                        )
                        return True

                    poll_until(alpha_finished_while_beta_held, timeout_s=120, interval_s=0.2)
                    assert not any(
                        row.get("kind") in {"worker-released", "worker-completed"}
                        and row.get("key") == "beta"
                        and row.get("stage") == "intent-approval"
                        for row in observed_alpha_pass["events"]
                    ), observed_alpha_pass["events"]
                    (evidence / "after-alpha-release.json").write_text(
                        json.dumps(observed_alpha_pass, indent=2, ensure_ascii=False) + "\n"
                    )

                    _release_worker(container, "beta")

                    def initial_children_finished() -> bool:
                        children_idle = all(
                            _snapshot(client, str(child["id"])).get("status") == "idle"
                            for child in initial_children
                        )
                        events = _read_ndjson_from_container(
                            container, "/probe/control/model-progress.jsonl"
                        )
                        completed = {
                            row.get("key")
                            for row in events
                            if row.get("kind") == "worker-completed"
                            and row.get("stage") == "intent-approval"
                        }
                        return children_idle and completed == {"alpha", "beta"}

                    poll_until(initial_children_finished, timeout_s=120, interval_s=0.2)
                    first_pass_events = _read_ndjson_from_container(
                        container, "/probe/control/model-progress.jsonl"
                    )
                    alpha_release = next(
                        row
                        for row in first_pass_events
                        if row.get("kind") == "worker-released"
                        and row.get("key") == "alpha"
                        and row.get("stage") == "intent-approval"
                    )
                    alpha_completion = next(
                        row
                        for row in first_pass_events
                        if row.get("kind") == "worker-completed"
                        and row.get("key") == "alpha"
                        and row.get("stage") == "intent-approval"
                    )
                    beta_release = next(
                        row
                        for row in first_pass_events
                        if row.get("kind") == "worker-released"
                        and row.get("key") == "beta"
                        and row.get("stage") == "intent-approval"
                    )
                    beta_completion = next(
                        row
                        for row in first_pass_events
                        if row.get("kind") == "worker-completed"
                        and row.get("key") == "beta"
                        and row.get("stage") == "intent-approval"
                    )
                    assert (
                        alpha_release["ordinal"]
                        < alpha_completion["ordinal"]
                        < beta_release["ordinal"]
                        < beta_completion["ordinal"]
                    ), first_pass_events
                    (evidence / "after-beta-release.json").write_text(
                        json.dumps(
                            {
                                "events": first_pass_events,
                                "alpha_release": alpha_release,
                                "alpha_completion": alpha_completion,
                                "beta_release": beta_release,
                                "beta_completion": beta_completion,
                                "children": _children(client, parent_id),
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    initial_items = {
                        child["id"]: _items(client, str(child["id"])) for child in initial_children
                    }
                    assert all(
                        any(
                            marker in json.dumps(items, ensure_ascii=False)
                            for items in initial_items.values()
                        )
                        for marker in _CHILD_MARKERS
                    ), initial_items
                    assert _snapshot(client, parent_id).get("status") == "idle"
                    (evidence / "children-after-first-pass.json").write_text(
                        json.dumps(initial_items, indent=2, ensure_ascii=False) + "\n"
                    )

                    _set_stage(container, tmp_path, "validation-open")
                    send_user_text(
                        client,
                        session_id=parent_id,
                        text="Please ask the Validation and finish question now.",
                    )
                    poll_until(
                        lambda: (
                            _snapshot(client, parent_id).get("status") == "idle"
                            and "Validation"
                            in json.dumps(_snapshot(client, parent_id).get("items", []))
                        ),
                        timeout_s=90,
                        interval_s=0.2,
                    )

                    _set_stage(container, tmp_path, "validation-feedback")

                    def read_live_acp_capture(path: str) -> list[dict[str, Any]]:
                        capture = _docker_exec(container, "cat", path)
                        lines = capture.splitlines()
                        frames = []
                        for index, line in enumerate(lines):
                            try:
                                frames.append(json.loads(line))
                            except json.JSONDecodeError:
                                if index == len(lines) - 1 and not capture.endswith("\n"):
                                    break
                                raise
                        return frames

                    agent_before_feedback = read_live_acp_capture(
                        "/probe/capture/acp.agent-to-client.ndjson"
                    )
                    client_before_feedback = read_live_acp_capture(
                        "/probe/capture/acp.client-to-agent.ndjson"
                    )
                    progress_before_feedback = _read_ndjson_from_container(
                        container, "/probe/control/model-progress.jsonl"
                    )
                    children_before_feedback = _children(client, parent_id)
                    before_feedback_ids = {str(child["id"]) for child in children_before_feedback}
                    assert before_feedback_ids == initial_ids, (
                        initial_ids,
                        children_before_feedback,
                    )
                    before_feedback_items = {
                        child_id: _items(client, child_id) for child_id in before_feedback_ids
                    }
                    assert before_feedback_items == initial_items

                    session_new = next(
                        frame
                        for frame in client_before_feedback
                        if frame.get("method") == "session/new"
                    )
                    session_new_reply = next(
                        frame
                        for frame in agent_before_feedback
                        if frame.get("id") == session_new.get("id")
                        and isinstance(frame.get("result"), dict)
                    )
                    root_native_session_id = session_new_reply["result"].get("sessionId")
                    assert isinstance(root_native_session_id, str) and root_native_session_id

                    def update_from_frame(frame: dict[str, Any]) -> dict[str, Any]:
                        params = frame.get("params", {})
                        update = params.get("update", {}) if isinstance(params, dict) else {}
                        return update if isinstance(update, dict) else {}

                    def matching_raw_call(
                        items: list[dict[str, Any]],
                        *,
                        call_id: str,
                        name: str,
                        raw_input: Any,
                    ) -> dict[str, Any] | None:
                        matches = [
                            item
                            for item in items
                            if item.get("type") == "function_call"
                            and item.get("call_id") == call_id
                        ]
                        if len(matches) != 1:
                            return None
                        item = matches[0]
                        try:
                            arguments = json.loads(item.get("arguments", ""))
                        except (TypeError, json.JSONDecodeError):
                            return None
                        if item.get("name") != name or arguments != raw_input:
                            return None
                        return item

                    association_frames_before = [
                        frame
                        for frame in agent_before_feedback
                        if frame.get("method") == "session/update"
                        and frame.get("params", {}).get("sessionId") == root_native_session_id
                        and update_from_frame(frame).get("sessionUpdate") == "subagent_update"
                    ]
                    native_to_api_child: dict[str, str] = {}
                    api_association_labels: dict[str, str] = {}
                    native_state_watermarks: dict[str, str] = {}
                    association_by_native_id = {}
                    for association_frame in association_frames_before:
                        association = update_from_frame(association_frame)
                        native_child_id = association.get("sessionId")
                        if isinstance(native_child_id, str):
                            association_by_native_id.setdefault(native_child_id, association_frame)
                    for association_frame in association_by_native_id.values():
                        association = update_from_frame(association_frame)
                        native_child_id = association.get("sessionId")
                        actor = association.get("_meta", {}).get("com.datadog.vitrine.actor", {})
                        worker_id = actor.get("agentId") if isinstance(actor, dict) else None
                        if (
                            not isinstance(native_child_id, str)
                            or not isinstance(worker_id, str)
                            or native_child_id.rsplit("/", 1)[-1] != worker_id
                        ):
                            continue
                        raw_calls = [
                            update_from_frame(frame)
                            for frame in agent_before_feedback
                            if frame.get("method") == "session/update"
                            and frame.get("params", {}).get("sessionId") == native_child_id
                            and update_from_frame(frame).get("sessionUpdate") == "tool_call"
                        ]
                        raw_call_ids = {
                            call.get("toolCallId")
                            for call in raw_calls
                            if isinstance(call.get("toolCallId"), str)
                        }
                        matching_children = []
                        for child_id, items in before_feedback_items.items():
                            matched_ids = {
                                call_id
                                for call_id in raw_call_ids
                                if any(
                                    item.get("type") == "function_call"
                                    and item.get("call_id") == call_id
                                    for item in items
                                )
                            }
                            if raw_call_ids and matched_ids == raw_call_ids:
                                matching_children.append(child_id)
                        assert len(matching_children) == 1, (
                            native_child_id,
                            raw_call_ids,
                            matching_children,
                            before_feedback_items,
                        )
                        child_id = matching_children[0]
                        for raw_call in raw_calls:
                            assert (
                                matching_raw_call(
                                    before_feedback_items[child_id],
                                    call_id=str(raw_call.get("toolCallId")),
                                    name=str(raw_call.get("name")),
                                    raw_input=raw_call.get("rawInput"),
                                )
                                is not None
                            ), (native_child_id, child_id, raw_call)
                        child = next(
                            child
                            for child in children_before_feedback
                            if str(child["id"]) == child_id
                        )
                        native_states = [
                            update_from_frame(frame).get("state", {}).get("state")
                            for frame in agent_before_feedback
                            if frame.get("method") == "session/update"
                            and frame.get("params", {}).get("sessionId") == root_native_session_id
                            and update_from_frame(frame).get("sessionUpdate") == "subagent_update"
                            and update_from_frame(frame).get("sessionId") == native_child_id
                        ]
                        assert native_states and native_states[-1] == "idle", (
                            native_child_id,
                            native_states,
                        )
                        association_label = child.get("labels", {}).get("omnigent.acp.subagent_id")
                        assert isinstance(association_label, str) and association_label, child
                        assert association_label not in api_association_labels.values(), (
                            association_label,
                            api_association_labels,
                        )
                        native_to_api_child[native_child_id] = child_id
                        api_association_labels[native_child_id] = association_label
                        native_state_watermarks[native_child_id] = native_states[-1]
                    assert len(native_to_api_child) == 2, (
                        native_to_api_child,
                        association_frames_before,
                    )

                    api_state_watermarks: dict[str, dict[str, Any]] = {}
                    for child in children_before_feedback:
                        child_id = str(child["id"])
                        child_snapshot = _snapshot(client, child_id)
                        assert (
                            child.get("busy") is False and child_snapshot.get("status") == "idle"
                        ), (
                            child,
                            child_snapshot,
                        )
                        api_state_watermarks[child_id] = {
                            "state": "idle",
                            "busy": child.get("busy"),
                            "snapshot_status": child_snapshot.get("status"),
                            "item_ids": [
                                item.get("id") for item in before_feedback_items[child_id]
                            ],
                        }
                    raw_agent_watermark = len(agent_before_feedback)
                    provider_request_watermark = max(
                        (int(row.get("ordinal", 0)) for row in progress_before_feedback),
                        default=0,
                    )
                    api_item_watermarks = {
                        child_id: [item.get("id") for item in items]
                        for child_id, items in before_feedback_items.items()
                    }
                    client_frame_watermark = len(client_before_feedback)
                    (evidence / "feedback-watermarks.json").write_text(
                        json.dumps(
                            {
                                "raw_agent_frame_count": raw_agent_watermark,
                                "provider_request_ordinal": provider_request_watermark,
                                "api_child_ids": sorted(before_feedback_ids),
                                "api_item_ids": api_item_watermarks,
                                "api_child_states": api_state_watermarks,
                                "client_frame_count": client_frame_watermark,
                                "native_to_api_child": native_to_api_child,
                                "api_association_labels": api_association_labels,
                                "native_child_states": native_state_watermarks,
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                    feedback_api_state_samples: dict[str, list[dict[str, Any]]] = {
                        child_id: [{"state": "idle", "source": "pre-feedback watermark"}]
                        for child_id in before_feedback_ids
                    }
                    observed_feedback: dict[str, Any] = {}

                    def feedback_run_is_mirrored() -> bool:
                        agent_frames = read_live_acp_capture(
                            "/probe/capture/acp.agent-to-client.ndjson"
                        )
                        client_frames = read_live_acp_capture(
                            "/probe/capture/acp.client-to-agent.ndjson"
                        )
                        progress = _read_ndjson_from_container(
                            container, "/probe/control/model-progress.jsonl"
                        )
                        current_children = _children(client, parent_id)
                        current_child_map = {str(child["id"]): child for child in current_children}
                        assert set(current_child_map) == before_feedback_ids, current_children
                        assert all(
                            current_child_map[api_child_id]
                            .get("labels", {})
                            .get("omnigent.acp.subagent_id")
                            == api_association_labels[native_child_id]
                            for native_child_id, api_child_id in native_to_api_child.items()
                        ), current_children
                        current_items = {
                            child_id: _items(client, child_id) for child_id in before_feedback_ids
                        }
                        for child_id, child in current_child_map.items():
                            state = (
                                "running"
                                if child.get("busy") is True
                                else ("idle" if child.get("busy") is False else None)
                            )
                            if (
                                state is not None
                                and state != feedback_api_state_samples[child_id][-1]["state"]
                            ):
                                child_snapshot = _snapshot(client, child_id)
                                if child_snapshot.get("status") != state:
                                    continue
                                feedback_api_state_samples[child_id].append(
                                    {
                                        "state": state,
                                        "snapshot_status": child_snapshot.get("status"),
                                        "item_ids": [
                                            item.get("id") for item in current_items[child_id]
                                        ],
                                    }
                                )

                        next_step_calls: dict[str, dict[str, Any]] = {}
                        next_step_results: dict[str, tuple[int, str]] = {}
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= raw_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != root_native_session_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            if (
                                update.get("sessionUpdate") == "tool_call"
                                and update.get("name") == "next_step"
                                and update.get("rawInput", {}).get("outcome") == "feedback"
                                and update.get("rawInput", {}).get("session") == "reuse"
                            ):
                                call_id = update.get("toolCallId")
                                if isinstance(call_id, str):
                                    next_step_calls[call_id] = update
                            elif (
                                update.get("sessionUpdate") == "tool_call_update"
                                and update.get("status") == "completed"
                                and update.get("toolCallId") in next_step_calls
                            ):
                                text = "\n".join(
                                    block.get("content", {}).get("text", "")
                                    for block in update.get("content", [])
                                    if isinstance(block, dict)
                                    and isinstance(block.get("content"), dict)
                                    and isinstance(block["content"].get("text"), str)
                                )
                                next_step_results[update["toolCallId"]] = (ordinal, text)

                        reused_worker_id = None
                        next_step_result_frame = None
                        for call_id, (ordinal, text) in next_step_results.items():
                            call = next_step_calls[call_id]
                            raw_input = call.get("rawInput", {})
                            if raw_input.get("message") != _FEEDBACK:
                                continue
                            match = re.search(
                                r"Working started: the worker works on ([A-Za-z0-9_-]+)\.",
                                text,
                            )
                            if match is not None:
                                reused_worker_id = match.group(1)
                                next_step_result_frame = {"ordinal": ordinal, "text": text}
                                break
                        if reused_worker_id is None:
                            return False

                        native_matches = [
                            native_id
                            for native_id in native_to_api_child
                            if native_id.rsplit("/", 1)[-1] == reused_worker_id
                        ]
                        assert len(native_matches) == 1, (
                            reused_worker_id,
                            native_to_api_child,
                        )
                        reused_native_child_id = native_matches[0]
                        reused_api_child_id = native_to_api_child[reused_native_child_id]
                        assert reused_api_child_id in before_feedback_ids

                        native_statuses = []
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= raw_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != root_native_session_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            state_data = update.get("state")
                            state = (
                                state_data.get("state") if isinstance(state_data, dict) else None
                            )
                            if (
                                update.get("sessionUpdate") == "subagent_update"
                                and update.get("sessionId") == reused_native_child_id
                                and state in {"running", "idle"}
                            ):
                                native_statuses.append({"ordinal": ordinal, "state": state})
                        native_state_sequence = [row["state"] for row in native_statuses]

                        worker_requests = [
                            row
                            for row in progress
                            if int(row.get("ordinal", 0)) > provider_request_watermark
                            and row.get("kind") == "model-request"
                            and row.get("role") == "worker"
                            and isinstance(row.get("latestUser"), str)
                            and _FEEDBACK in row["latestUser"]
                        ]
                        post_watermark_tool_calls: dict[str, dict[str, Any]] = {}
                        post_watermark_tool_results: dict[str, dict[str, Any]] = {}
                        post_watermark_messages: list[dict[str, Any]] = []
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= raw_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != reused_native_child_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            update_type = update.get("sessionUpdate")
                            if update_type == "tool_call":
                                call_id = update.get("toolCallId")
                                if isinstance(call_id, str):
                                    post_watermark_tool_calls[call_id] = {
                                        "ordinal": ordinal,
                                        "name": update.get("name"),
                                        "rawInput": update.get("rawInput"),
                                    }
                            elif (
                                update_type == "tool_call_update"
                                and update.get("status") == "completed"
                            ):
                                call_id = update.get("toolCallId")
                                if isinstance(call_id, str):
                                    post_watermark_tool_results[call_id] = {
                                        "ordinal": ordinal,
                                        "content": update.get("content"),
                                    }
                            elif update_type == "agent_message_chunk":
                                content = update.get("content", {})
                                message_id = update.get("messageId")
                                if (
                                    isinstance(message_id, str)
                                    and isinstance(content, dict)
                                    and isinstance(content.get("text"), str)
                                ):
                                    post_watermark_messages.append(
                                        {
                                            "ordinal": ordinal,
                                            "message_id": message_id,
                                            "text": content["text"],
                                        }
                                    )

                        before_worker_messages = [
                            update_from_frame(frame).get("messageId")
                            for frame in agent_before_feedback
                            if frame.get("method") == "session/update"
                            and frame.get("params", {}).get("sessionId") == reused_native_child_id
                            and update_from_frame(frame).get("sessionUpdate")
                            == "agent_message_chunk"
                        ]
                        feedback_items = current_items[reused_api_child_id]
                        watermark_item_ids = set(api_item_watermarks[reused_api_child_id])
                        new_api_items = [
                            item
                            for item in feedback_items
                            if item.get("id") not in watermark_item_ids
                        ]
                        if (
                            feedback_items[: len(before_feedback_items[reused_api_child_id])]
                            != before_feedback_items[reused_api_child_id]
                        ):
                            return False
                        if (
                            not worker_requests
                            or not post_watermark_tool_calls
                            or not post_watermark_messages
                        ):
                            return False
                        if set(post_watermark_tool_calls) != set(post_watermark_tool_results):
                            return False
                        if any(
                            matching_raw_call(
                                new_api_items,
                                call_id=call_id,
                                name=str(call.get("name")),
                                raw_input=call.get("rawInput"),
                            )
                            is None
                            for call_id, call in post_watermark_tool_calls.items()
                        ):
                            return False
                        matched_tool_pairs = []
                        for call_id, raw_call in post_watermark_tool_calls.items():
                            output_matches = [
                                item
                                for item in new_api_items
                                if item.get("type") == "function_call_output"
                                and item.get("call_id") == call_id
                            ]
                            if len(output_matches) != 1:
                                return False
                            output = output_matches[0]
                            raw_result = post_watermark_tool_results[call_id]["content"]
                            if output.get("content") != raw_result:
                                return False
                            matched_tool_pairs.append(
                                {
                                    "native_tool_call_id": call_id,
                                    "native_tool_name": raw_call["name"],
                                    "api_function_call_item_id": next(
                                        item["id"]
                                        for item in new_api_items
                                        if item.get("type") == "function_call"
                                        and item.get("call_id") == call_id
                                    ),
                                    "api_output_item_id": output["id"],
                                    "result": raw_result,
                                }
                            )
                        api_messages = [
                            item
                            for item in new_api_items
                            if item.get("type") == "message" and item.get("role") == "assistant"
                        ]
                        if len(api_messages) != len(post_watermark_messages):
                            return False
                        message_pairs = []
                        for raw_message, api_message in zip(
                            post_watermark_messages, api_messages, strict=True
                        ):
                            if _message_text(api_message) != raw_message["text"]:
                                return False
                            message_pairs.append(
                                {
                                    "native_message_id": raw_message["message_id"],
                                    "native_text": raw_message["text"],
                                    "api_message_item_id": api_message["id"],
                                    "api_text": _message_text(api_message),
                                }
                            )
                        if not before_worker_messages:
                            return False
                        prior_message_numbers = [
                            int(message_id.rsplit(":", 1)[1])
                            for message_id in before_worker_messages
                            if isinstance(message_id, str)
                            and message_id.startswith(f"{reused_worker_id}:")
                            and message_id.rsplit(":", 1)[1].isdigit()
                        ]
                        new_message_numbers = [
                            int(message["message_id"].rsplit(":", 1)[1])
                            for message in post_watermark_messages
                            if message["message_id"].startswith(f"{reused_worker_id}:")
                            and message["message_id"].rsplit(":", 1)[1].isdigit()
                        ]
                        if (
                            len(prior_message_numbers) != len(before_worker_messages)
                            or len(new_message_numbers) != len(post_watermark_messages)
                            or min(new_message_numbers) <= max(prior_message_numbers)
                        ):
                            return False
                        expected_new_ids = {
                            item_id
                            for pair in matched_tool_pairs
                            for item_id in (
                                pair["api_function_call_item_id"],
                                pair["api_output_item_id"],
                            )
                        } | {pair["api_message_item_id"] for pair in message_pairs}
                        if {item.get("id") for item in new_api_items} != expected_new_ids:
                            return False
                        api_state_sequence = [
                            row["state"] for row in feedback_api_state_samples[reused_api_child_id]
                        ]
                        client_prompts = [
                            frame
                            for frame in client_frames
                            if frame.get("method") == "session/prompt"
                        ]
                        new_client_prompts = client_prompts[
                            sum(
                                1
                                for frame in client_before_feedback
                                if frame.get("method") == "session/prompt"
                            ) :
                        ]
                        if (
                            not all(
                                frame.get("params", {}).get("sessionId") == root_native_session_id
                                for frame in client_prompts
                            )
                            or len(new_client_prompts) != 1
                            or _FEEDBACK
                            not in json.dumps(
                                new_client_prompts[0].get("params", {}).get("prompt", []),
                                ensure_ascii=False,
                            )
                        ):
                            return False
                        if native_state_sequence != ["running", "idle"] or api_state_sequence != [
                            "idle",
                            "running",
                            "idle",
                        ]:
                            return False
                        if _snapshot(client, reused_api_child_id).get("status") != "idle":
                            return False
                        observed_feedback.update(
                            reused_worker_id=reused_worker_id,
                            reused_native_child_id=reused_native_child_id,
                            reused_api_child_id=reused_api_child_id,
                            next_step_result=next_step_result_frame,
                            provider_requests=worker_requests,
                            native_statuses=native_statuses,
                            api_statuses=feedback_api_state_samples[reused_api_child_id],
                            api_items=feedback_items,
                            matched_tool_pairs=matched_tool_pairs,
                            matched_message_pairs=message_pairs,
                            prior_message_ids=before_worker_messages,
                            new_message_ids=[
                                message["message_id"] for message in post_watermark_messages
                            ],
                            client_prompts=new_client_prompts,
                            raw_agent_watermark=raw_agent_watermark,
                            provider_request_watermark=provider_request_watermark,
                            api_item_watermark=api_item_watermarks[reused_api_child_id],
                        )
                        return True

                    send_user_text(client, session_id=parent_id, text=_FEEDBACK)
                    poll_until(feedback_run_is_mirrored, timeout_s=120, interval_s=0.01)
                    feedback_prompt_id = observed_feedback["client_prompts"][0].get("id")
                    assert isinstance(feedback_prompt_id, (str, int)), observed_feedback
                    observed_parent_completion: dict[str, Any] = {}

                    def feedback_parent_prompt_finished() -> bool:
                        agent_frames = read_live_acp_capture(
                            "/probe/capture/acp.agent-to-client.ndjson"
                        )
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            result = frame.get("result")
                            if (
                                ordinal <= raw_agent_watermark
                                or frame.get("id") != feedback_prompt_id
                                or not isinstance(result, dict)
                                or result.get("stopReason") != "end_turn"
                            ):
                                continue
                            parent_snapshot = _snapshot(client, parent_id)
                            if parent_snapshot.get("status") != "idle":
                                continue
                            observed_parent_completion.update(
                                raw_agent_ordinal=ordinal,
                                raw_agent_response=frame,
                                parent_snapshot=parent_snapshot,
                            )
                            return True
                        return False

                    poll_until(feedback_parent_prompt_finished, timeout_s=90, interval_s=0.05)
                    observed_feedback["parent_completion"] = observed_parent_completion
                    after_feedback = _children(client, parent_id)
                    after_feedback_ids = {str(child["id"]) for child in after_feedback}
                    assert after_feedback_ids == before_feedback_ids, (
                        children_before_feedback,
                        after_feedback,
                    )
                    feedback_items_by_child_id = {
                        str(child["id"]): _items(client, str(child["id"]))
                        for child in after_feedback
                    }
                    reused_api_child_id = observed_feedback["reused_api_child_id"]
                    assert (
                        feedback_items_by_child_id[reused_api_child_id]
                        == observed_feedback["api_items"]
                    ), (
                        feedback_items_by_child_id[reused_api_child_id],
                        observed_feedback,
                    )
                    assert {
                        child_id: items
                        for child_id, items in feedback_items_by_child_id.items()
                        if child_id != reused_api_child_id
                    } == {
                        child_id: items
                        for child_id, items in before_feedback_items.items()
                        if child_id != reused_api_child_id
                    }, (
                        before_feedback_items,
                        feedback_items_by_child_id,
                    )
                    (evidence / "feedback-history-parity.json").write_text(
                        json.dumps(observed_feedback, indent=2, ensure_ascii=False) + "\n"
                    )
                    (evidence / "children-after-feedback.json").write_text(
                        json.dumps(feedback_items_by_child_id, indent=2, ensure_ascii=False) + "\n"
                    )

                    _set_stage(container, tmp_path, "reuse-report")
                    reuse_agent_watermark = len(
                        read_live_acp_capture("/probe/capture/acp.agent-to-client.ndjson")
                    )
                    reuse_client_before = read_live_acp_capture(
                        "/probe/capture/acp.client-to-agent.ndjson"
                    )
                    reuse_prompt_watermark = sum(
                        frame.get("method") == "session/prompt" for frame in reuse_client_before
                    )
                    reused_worker_id = observed_feedback["reused_worker_id"]
                    send_user_text(
                        client,
                        session_id=parent_id,
                        text="Read the primary Worker transcript after that feedback pass.",
                    )
                    observed_reuse_report: dict[str, Any] = {}

                    def reuse_report_and_validation_opened() -> bool:
                        agent_frames = read_live_acp_capture(
                            "/probe/capture/acp.agent-to-client.ndjson"
                        )
                        client_frames = read_live_acp_capture(
                            "/probe/capture/acp.client-to-agent.ndjson"
                        )
                        new_prompts = [
                            frame
                            for frame in client_frames
                            if frame.get("method") == "session/prompt"
                        ][reuse_prompt_watermark:]
                        if len(new_prompts) != 1:
                            return False
                        prompt_id = new_prompts[0].get("id")
                        get_sub_chat_calls: dict[str, tuple[int, dict[str, Any]]] = {}
                        validate_calls: dict[str, tuple[int, dict[str, Any]]] = {}
                        completed_calls: dict[str, tuple[int, str]] = {}
                        parent_messages: list[tuple[int, str]] = []
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= reuse_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != root_native_session_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            call_id = update.get("toolCallId")
                            raw_input = update.get("rawInput", {})
                            if update.get("sessionUpdate") == "agent_message_chunk":
                                content = update.get("content", {})
                                if isinstance(content, dict) and isinstance(
                                    content.get("text"), str
                                ):
                                    parent_messages.append((ordinal, content["text"]))
                            if update.get("sessionUpdate") == "tool_call" and isinstance(
                                call_id, str
                            ):
                                if (
                                    update.get("name") == "get_sub_chat"
                                    and isinstance(raw_input, dict)
                                    and raw_input.get("role") == "worker"
                                    and raw_input.get("channelId") == reused_worker_id
                                ):
                                    get_sub_chat_calls[call_id] = (ordinal, update)
                                elif (
                                    update.get("name") == "next_step"
                                    and isinstance(raw_input, dict)
                                    and raw_input.get("expect") == "validate"
                                ):
                                    validate_calls[call_id] = (ordinal, update)
                            elif (
                                update.get("sessionUpdate") == "tool_call_update"
                                and update.get("status") == "completed"
                                and isinstance(call_id, str)
                            ):
                                result_text = "\n".join(
                                    block.get("content", {}).get("text", "")
                                    for block in update.get("content", [])
                                    if isinstance(block, dict)
                                    and isinstance(block.get("content"), dict)
                                    and isinstance(block["content"].get("text"), str)
                                )
                                completed_calls[call_id] = (ordinal, result_text)
                        transcript_reads = {
                            call_id: (ordinal, completed_calls[call_id][1])
                            for call_id, (ordinal, _) in get_sub_chat_calls.items()
                            if call_id in completed_calls
                            and completed_calls[call_id][0] > ordinal
                            and "ambiguous" not in completed_calls[call_id][1].lower()
                            and f"# worker chat ({reused_worker_id})"
                            in completed_calls[call_id][1]
                            and _FEEDBACK in completed_calls[call_id][1]
                        }
                        validation_gates: dict[str, tuple[int, str]] = {}
                        for call_id, (call_ordinal, _) in validate_calls.items():
                            gate_text = "\n".join(
                                text
                                for message_ordinal, text in parent_messages
                                if message_ordinal > call_ordinal
                            )
                            if (
                                "How would you like to proceed?" in gate_text
                                and "The Worker finished its pass." in gate_text
                                and "Next in this flow: a review pass." in gate_text
                                and "Continue working" in gate_text
                            ):
                                validation_gates[call_id] = (call_ordinal, gate_text)
                        prompt_finished = any(
                            ordinal > reuse_agent_watermark
                            and frame.get("id") == prompt_id
                            and isinstance(frame.get("result"), dict)
                            and frame["result"].get("stopReason") == "end_turn"
                            for ordinal, frame in enumerate(agent_frames, start=1)
                        )
                        if not transcript_reads or not validation_gates or not prompt_finished:
                            return False
                        parent_snapshot = _snapshot(client, parent_id)
                        parent_items = _items(client, parent_id)
                        if (
                            parent_snapshot.get("status") != "idle"
                            or not _contains(parent_items, _FEEDBACK)
                            or not _contains(parent_items, "How would you like to proceed?")
                            or not _contains(parent_items, "Next in this flow: a review pass.")
                            or not _contains(parent_items, "Continue working")
                        ):
                            return False
                        validation_call_id = next(iter(validation_gates))
                        observed_reuse_report.update(
                            prompt_id=prompt_id,
                            raw_agent_watermark=reuse_agent_watermark,
                            transcript_read=next(iter(transcript_reads.values()))[1],
                            transcript_worker_channel=reused_worker_id,
                            validation_call_id=validation_call_id,
                            validation_call_input=validate_calls[validation_call_id][1].get(
                                "rawInput"
                            ),
                            validation_open=validation_gates[validation_call_id][1],
                            parent_snapshot=parent_snapshot,
                            parent_items=parent_items,
                        )
                        return True

                    poll_until(reuse_report_and_validation_opened, timeout_s=90, interval_s=0.05)
                    (evidence / "reuse-report-validation-open.json").write_text(
                        json.dumps(observed_reuse_report, indent=2, ensure_ascii=False) + "\n"
                    )

                    _set_stage(container, tmp_path, "producer-loss")
                    producer_loss_agent_before = read_live_acp_capture(
                        "/probe/capture/acp.agent-to-client.ndjson"
                    )
                    producer_loss_client_before = read_live_acp_capture(
                        "/probe/capture/acp.client-to-agent.ndjson"
                    )
                    producer_loss_progress_before = _read_ndjson_from_container(
                        container, "/probe/control/model-progress.jsonl"
                    )
                    producer_loss_agent_watermark = len(producer_loss_agent_before)
                    producer_loss_prompt_watermark = sum(
                        frame.get("method") == "session/prompt"
                        for frame in producer_loss_client_before
                    )
                    producer_loss_request_watermark = max(
                        (int(row.get("ordinal", 0)) for row in producer_loss_progress_before),
                        default=0,
                    )
                    producer_loss_item_watermarks = {
                        child_id: [item.get("id") for item in _items(client, child_id)]
                        for child_id in before_feedback_ids
                    }
                    send_user_text(
                        client,
                        session_id=parent_id,
                        text="Check beta.txt once more and verify its marker.",
                    )
                    observed_open_tool: dict[str, Any] = {}

                    def beta_has_open_bash() -> bool:
                        events = _read_ndjson_from_container(
                            container, "/probe/control/model-progress.jsonl"
                        )
                        agent_frames = read_live_acp_capture(
                            "/probe/capture/acp.agent-to-client.ndjson"
                        )
                        client_frames = read_live_acp_capture(
                            "/probe/capture/acp.client-to-agent.ndjson"
                        )
                        new_prompts = [
                            frame
                            for frame in client_frames
                            if frame.get("method") == "session/prompt"
                        ][producer_loss_prompt_watermark:]
                        open_tool_events = [
                            row
                            for row in events
                            if row.get("kind") == "worker-open-tool"
                            and row.get("key") == "beta"
                            and row.get("stage") == "producer-loss"
                        ]
                        if len(new_prompts) != 1 or _FEEDBACK in json.dumps(
                            new_prompts[0].get("params", {}).get("prompt", []), ensure_ascii=False
                        ):
                            return False
                        prompt_text = json.dumps(
                            new_prompts[0].get("params", {}).get("prompt", []),
                            ensure_ascii=False,
                        )
                        if "Check beta.txt once more and verify its marker." not in prompt_text:
                            return False
                        next_step_calls: dict[str, tuple[int, dict[str, Any]]] = {}
                        next_step_results: dict[str, tuple[int, str]] = {}
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= producer_loss_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != root_native_session_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            call_id = update.get("toolCallId")
                            raw_input = update.get("rawInput", {})
                            if (
                                update.get("sessionUpdate") == "tool_call"
                                and update.get("name") == "next_step"
                                and isinstance(call_id, str)
                                and isinstance(raw_input, dict)
                                and raw_input.get("outcome") == "feedback"
                                and raw_input.get("expect") == "build"
                                and raw_input.get("session") == "reuse"
                                and raw_input.get("message")
                                == "Check beta.txt once more and verify its marker."
                            ):
                                next_step_calls[call_id] = (ordinal, update)
                            elif (
                                update.get("sessionUpdate") == "tool_call_update"
                                and update.get("status") == "completed"
                                and isinstance(call_id, str)
                                and call_id in next_step_calls
                            ):
                                result_text = "\n".join(
                                    block.get("content", {}).get("text", "")
                                    for block in update.get("content", [])
                                    if isinstance(block, dict)
                                    and isinstance(block.get("content"), dict)
                                    and isinstance(block["content"].get("text"), str)
                                )
                                next_step_results[call_id] = (ordinal, result_text)
                        completed_reuse = [
                            (call_id, next_step_calls[call_id], result)
                            for call_id, result in next_step_results.items()
                            if "Working started: the worker works on " in result[1]
                        ]
                        if len(completed_reuse) != 1:
                            return False
                        (
                            next_step_call_id,
                            (_, next_step_call),
                            (next_step_result_ordinal, next_step_result_text),
                        ) = completed_reuse[0]
                        match = re.search(
                            r"Working started: the worker works on ([A-Za-z0-9_-]+)\.",
                            next_step_result_text,
                        )
                        if match is None:
                            return False
                        loss_worker_id = match.group(1)
                        native_matches = [
                            native_id
                            for native_id in native_to_api_child
                            if native_id.rsplit("/", 1)[-1] == loss_worker_id
                        ]
                        if len(native_matches) != 1:
                            return False
                        loss_native_child_id = native_matches[0]
                        loss_child_id = native_to_api_child[loss_native_child_id]
                        children = _children(client, parent_id)
                        child_map = {str(child["id"]): child for child in children}
                        if set(child_map) != before_feedback_ids:
                            return False
                        expected_label = api_association_labels[loss_native_child_id]
                        if (
                            child_map[loss_child_id]
                            .get("labels", {})
                            .get("omnigent.acp.subagent_id")
                            != expected_label
                        ):
                            return False
                        raw_open_calls = []
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= producer_loss_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != loss_native_child_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            raw_input = update.get("rawInput", {})
                            if (
                                update.get("sessionUpdate") == "tool_call"
                                and update.get("title") == "Bash"
                                and isinstance(raw_input, dict)
                                and "bash-beta-started" in raw_input.get("command", "")
                            ):
                                raw_open_calls.append(
                                    {
                                        "ordinal": ordinal,
                                        "tool_call_id": update.get("toolCallId"),
                                        "raw_input": raw_input,
                                        "name": update.get("name"),
                                        "status": update.get("status"),
                                        "native_output": {
                                            key: update[key]
                                            for key in ("content", "rawOutput")
                                            if key in update
                                        },
                                    }
                                )
                        if len(raw_open_calls) != 1:
                            return False
                        open_call_id = raw_open_calls[0]["tool_call_id"]
                        if not isinstance(open_call_id, str) or not open_tool_events:
                            return False
                        worker_requests = [
                            row
                            for row in events
                            if int(row.get("ordinal", 0)) > producer_loss_request_watermark
                            and row.get("stage") == "producer-loss"
                            and row.get("kind") == "model-request"
                            and row.get("role") == "worker"
                            and "Check beta.txt once more and verify its marker."
                            in row.get("latestUser", "")
                        ]
                        if not worker_requests:
                            return False
                        native_call_updates = []
                        for ordinal, frame in enumerate(agent_frames, start=1):
                            if (
                                ordinal <= producer_loss_agent_watermark
                                or frame.get("method") != "session/update"
                            ):
                                continue
                            params = frame.get("params", {})
                            if (
                                not isinstance(params, dict)
                                or params.get("sessionId") != loss_native_child_id
                            ):
                                continue
                            update = update_from_frame(frame)
                            if update.get("toolCallId") == open_call_id and update.get(
                                "sessionUpdate"
                            ) in ("tool_call", "tool_call_update"):
                                native_call_updates.append(update)
                        if (
                            not native_call_updates
                            or native_call_updates[-1].get("status") != "in_progress"
                            or any(
                                update.get("status") in ("completed", "failed")
                                for update in native_call_updates
                            )
                        ):
                            return False
                        native_output_updates = [
                            update
                            for update in native_call_updates
                            if "content" in update or "rawOutput" in update
                        ]
                        if not native_output_updates:
                            return False
                        native_output_update = native_output_updates[-1]
                        items = _items(client, loss_child_id)
                        open_calls = _open_tool_calls(items, "bash-beta-started")
                        matched_open_call = [
                            call for call in open_calls if call.get("call_id") == open_call_id
                        ]
                        open_output_items = [
                            item
                            for item in items
                            if item.get("type") == "function_call_output"
                            and item.get("call_id") == open_call_id
                        ]
                        if (
                            len(open_output_items) != 1
                            or (
                                "content" in native_output_update
                                and open_output_items[0].get("content")
                                != native_output_update["content"]
                            )
                            or (
                                "rawOutput" in native_output_update
                                and open_output_items[0].get("raw_output")
                                != native_output_update["rawOutput"]
                            )
                        ):
                            return False
                        if (
                            not _container_has_file(container, "/probe/control/bash-beta-started")
                            or len(matched_open_call) != 1
                            or matching_raw_call(
                                items,
                                call_id=open_call_id,
                                name=str(raw_open_calls[0].get("name")),
                                raw_input=raw_open_calls[0]["raw_input"],
                            )
                            is None
                            or matched_open_call[0].get("tool_status") != "in_progress"
                            or _snapshot(client, loss_child_id).get("status") != "running"
                            or child_map[loss_child_id].get("busy") is not True
                        ):
                            return False
                        observed_open_tool.update(
                            events=events,
                            raw_open_call=raw_open_calls[0],
                            next_step_call_id=next_step_call_id,
                            next_step_input=next_step_call.get("rawInput"),
                            next_step_result_ordinal=next_step_result_ordinal,
                            next_step_result=next_step_result_text,
                            worker_id=loss_worker_id,
                            native_child_id=loss_native_child_id,
                            child_id=loss_child_id,
                            association_label=expected_label,
                            provider_requests=worker_requests,
                            client_prompt=new_prompts[0],
                            items=items,
                            open_call=matched_open_call[0],
                            native_call_statuses=[
                                update.get("status") for update in native_call_updates
                            ],
                            native_output={
                                key: native_output_update[key]
                                for key in ("content", "rawOutput")
                                if key in native_output_update
                            },
                            open_output_item=open_output_items[0],
                            snapshot=_snapshot(client, loss_child_id),
                        )
                        return True

                    poll_until(beta_has_open_bash, timeout_s=120, interval_s=0.2)
                    loss_child_id = observed_open_tool["child_id"]
                    assert loss_child_id in producer_loss_item_watermarks
                    open_call_id = observed_open_tool["open_call"].get("call_id")
                    assert isinstance(open_call_id, str) and open_call_id
                    (evidence / "open-bash-before-producer-loss.json").write_text(
                        json.dumps(observed_open_tool, indent=2, ensure_ascii=False) + "\n"
                    )

                    producer_process = _kill_producer(container)
                    poll_until(
                        lambda: _container_has_file(container, "/probe/control/driver-exit.json"),
                        timeout_s=15,
                        interval_s=0.1,
                    )
                    driver_exit = json.loads(
                        _docker_exec(container, "cat", "/probe/control/driver-exit.json")
                    )
                    assert driver_exit.get("signal") == "SIGKILL", driver_exit
                    agent_frames_after_loss = read_live_acp_capture(
                        "/probe/capture/acp.agent-to-client.ndjson"
                    )
                    native_statuses_after_loss = [
                        update.get("status")
                        for frame in agent_frames_after_loss
                        if frame.get("method") == "session/update"
                        and isinstance(frame.get("params"), dict)
                        and frame["params"].get("sessionId")
                        == observed_open_tool["native_child_id"]
                        for update in [update_from_frame(frame)]
                        if update.get("toolCallId") == open_call_id
                        and update.get("sessionUpdate") in ("tool_call", "tool_call_update")
                    ]
                    assert native_statuses_after_loss
                    assert native_statuses_after_loss[-1] == "in_progress", (
                        native_statuses_after_loss,
                        agent_frames_after_loss,
                    )
                    assert not any(
                        status in ("completed", "failed") for status in native_statuses_after_loss
                    ), native_statuses_after_loss

                    def beta_cleanup_finished() -> bool:
                        children = {
                            str(child["id"]): child for child in _children(client, parent_id)
                        }
                        loss_snapshot = _snapshot(client, loss_child_id)
                        loss_child = children.get(loss_child_id)
                        return (
                            loss_child is not None
                            and loss_child.get("busy") is False
                            and loss_child.get("current_task_status") is None
                            and _has_retired_acp_observation(loss_snapshot)
                        )

                    poll_until(beta_cleanup_finished, timeout_s=30, interval_s=0.2)
                    children_after_loss = _children(client, parent_id)
                    loss_child_summary_after_loss = next(
                        child for child in children_after_loss if str(child["id"]) == loss_child_id
                    )
                    loss_snapshot_after_loss = _snapshot(client, loss_child_id)
                    assert loss_child_summary_after_loss.get("busy") is False
                    assert loss_child_summary_after_loss.get("current_task_status") is None
                    assert _has_retired_acp_observation(loss_snapshot_after_loss), (
                        loss_snapshot_after_loss.get("acp_session_observation")
                    )
                    assert loss_child_summary_after_loss.get(
                        "acp_session_observation"
                    ) == loss_snapshot_after_loss.get("acp_session_observation")
                    after_loss_items = {
                        child_id: _items(client, child_id) for child_id in initial_ids
                    }
                    for child_id, watermarked_item_ids in producer_loss_item_watermarks.items():
                        assert [item.get("id") for item in after_loss_items[child_id]][
                            : len(watermarked_item_ids)
                        ] == watermarked_item_ids, (
                            child_id,
                            watermarked_item_ids,
                            after_loss_items[child_id],
                        )
                    open_calls_after_loss = _open_tool_calls(
                        after_loss_items[loss_child_id], "bash-beta-started"
                    )
                    matched_open_call_after_loss = [
                        call
                        for call in open_calls_after_loss
                        if call.get("call_id") == open_call_id
                    ]
                    assert len(matched_open_call_after_loss) == 1, after_loss_items[loss_child_id]
                    assert matched_open_call_after_loss[0].get("tool_status") == "in_progress", (
                        matched_open_call_after_loss[0],
                        after_loss_items[loss_child_id],
                    )
                    open_output_items_after_loss = [
                        item
                        for item in after_loss_items[loss_child_id]
                        if item.get("type") == "function_call_output"
                        and item.get("call_id") == open_call_id
                    ]
                    assert open_output_items_after_loss == [
                        observed_open_tool["open_output_item"]
                    ], after_loss_items[loss_child_id]
                    (evidence / "after-producer-loss.json").write_text(
                        json.dumps(
                            {
                                "producer_process": producer_process,
                                "driver_exit": driver_exit,
                                "parent": _snapshot(client, parent_id),
                                "children": children_after_loss,
                                "loss_child": loss_snapshot_after_loss,
                                "loss_child_summary": loss_child_summary_after_loss,
                                "generic_status_after_producer_loss": loss_snapshot_after_loss.get(
                                    "status"
                                ),
                                "child_items": after_loss_items,
                                "open_tool_call_id": open_call_id,
                                "native_statuses_after_loss": native_statuses_after_loss,
                                "open_tool_call": matched_open_call_after_loss[0],
                                "open_output_item": open_output_items_after_loss[0],
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                    assert stack.server is not None
                    server_process = stack.server
                    server_process.kill()
                    server_process.wait(timeout=10)
                    stack.server = None
                    stack.start_server()

                    def runner_reconnected() -> bool:
                        response = client.get(f"/v1/runners/{stack.runner_id}/status")
                        return (
                            response.status_code == 200 and response.json().get("online") is True
                        )

                    poll_until(runner_reconnected, timeout_s=60, interval_s=0.2)
                    children_after_restart = _children(client, parent_id)
                    assert {str(child["id"]) for child in children_after_restart} == initial_ids
                    loss_child_summary_after_restart = next(
                        child
                        for child in children_after_restart
                        if str(child["id"]) == loss_child_id
                    )
                    loss_snapshot_after_restart = _snapshot(client, loss_child_id)
                    assert loss_child_summary_after_restart.get("busy") is False
                    assert loss_child_summary_after_restart.get("current_task_status") is None
                    assert _has_retired_acp_observation(loss_snapshot_after_restart), (
                        loss_snapshot_after_restart.get("acp_session_observation")
                    )
                    assert loss_child_summary_after_restart.get(
                        "acp_session_observation"
                    ) == loss_snapshot_after_restart.get("acp_session_observation")
                    items_after_restart = {
                        child_id: _items(client, child_id) for child_id in initial_ids
                    }
                    for child_id in initial_ids:
                        assert items_after_restart[child_id] == after_loss_items[child_id]
                    open_calls_after_restart = _open_tool_calls(
                        items_after_restart[loss_child_id], "bash-beta-started"
                    )
                    matched_open_calls_after_restart = [
                        call
                        for call in open_calls_after_restart
                        if call.get("call_id") == open_call_id
                    ]
                    assert len(matched_open_calls_after_restart) == 1, items_after_restart[
                        loss_child_id
                    ]
                    assert matched_open_calls_after_restart[0].get("tool_status") == "in_progress"
                    open_output_items_after_restart = [
                        item
                        for item in items_after_restart[loss_child_id]
                        if item.get("type") == "function_call_output"
                        and item.get("call_id") == open_call_id
                    ]
                    assert open_output_items_after_restart == [
                        observed_open_tool["open_output_item"]
                    ], items_after_restart[loss_child_id]
                    (evidence / "after-server-kill-restart.json").write_text(
                        json.dumps(
                            {
                                "parent": _snapshot(client, parent_id),
                                "children": children_after_restart,
                                "loss_child": loss_snapshot_after_restart,
                                "generic_status_after_restart": loss_snapshot_after_restart.get(
                                    "status"
                                ),
                                "child_items": items_after_restart,
                                "open_tool_call_id": open_call_id,
                                "open_tool_call": matched_open_calls_after_restart[0],
                                "open_output_item": open_output_items_after_restart[0],
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

                    cli_home = tmp_path / "export-cli-home"
                    cli_env = _isolated_cli_env(
                        home=cli_home,
                        config_home=config_home,
                        data_dir=tmp_path / "export-cli-data",
                    )
                    export_path = evidence / "child-export.jsonl"
                    export = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "omnigent",
                            "session",
                            "export",
                            "--id",
                            loss_child_id,
                            "--server",
                            stack.base_url,
                            "--output",
                            str(export_path),
                        ],
                        cwd=_APP_ROOT,
                        env=cli_env,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    assert export.returncode == 0, export.stderr
                    exported_items = [
                        json.loads(line)
                        for line in export_path.read_text().splitlines()
                        if line.strip()
                    ]
                    exported_session_metadata = [
                        item
                        for item in exported_items
                        if item.get("record_type") == "session_meta"
                    ]
                    assert len(exported_session_metadata) == 1, exported_items
                    exported_metadata = exported_session_metadata[0]
                    assert _has_retired_acp_observation(exported_metadata), exported_metadata.get(
                        "acp_session_observation"
                    )
                    assert (
                        exported_metadata["acp_session_observation"]
                        == loss_snapshot_after_restart["acp_session_observation"]
                    )
                    exported_item_rows = [
                        item for item in exported_items if item.get("record_type") == "item"
                    ]
                    api_items_after_restart = items_after_restart[loss_child_id]
                    assert [
                        {key: value for key, value in item.items() if key != "record_type"}
                        for item in exported_item_rows
                    ] == api_items_after_restart
                    assert any(
                        marker in json.dumps(item, ensure_ascii=False)
                        for marker in _CHILD_MARKERS
                        for item in exported_items
                    ), exported_items
                    exported_open_calls = _open_tool_calls(exported_item_rows, "bash-beta-started")
                    matched_exported_open_calls = [
                        call for call in exported_open_calls if call.get("call_id") == open_call_id
                    ]
                    assert len(matched_exported_open_calls) == 1, exported_item_rows
                    assert matched_exported_open_calls[0].get("tool_status") == "in_progress"
                    exported_open_outputs = [
                        {key: value for key, value in item.items() if key != "record_type"}
                        for item in exported_item_rows
                        if item.get("type") == "function_call_output"
                        and item.get("call_id") == open_call_id
                    ]
                    assert exported_open_outputs == [observed_open_tool["open_output_item"]], (
                        exported_item_rows
                    )
                finally:
                    _release_all_workers(container)
                    if parent_id is not None:
                        with httpx.Client(
                            base_url=stack.base_url, timeout=10, trust_env=False
                        ) as cleanup_client:
                            with contextlib.suppress(httpx.HTTPError):
                                cleanup_client.delete(f"/v1/sessions/{parent_id}")
    finally:
        _collect_container_evidence(container, evidence)
        subprocess.run(
            ["docker", "rm", "--force", container],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    client_wire = evidence / "capture/acp.client-to-agent.ndjson"
    if client_wire.is_file():
        initialize = next(
            frame for frame in _read_ndjson(client_wire) if frame.get("method") == "initialize"
        )
        subagents = initialize.get("params", {}).get("clientCapabilities", {}).get("subagents")
        assert isinstance(subagents, dict), initialize
        (evidence / "initialize-opt-in.json").write_text(
            json.dumps(
                {"clientCapabilities": initialize["params"]["clientCapabilities"]}, indent=2
            )
            + "\n"
        )
    else:
        pytest.fail(f"Vitrine ACP capture was not written; inspect {evidence}")

    (evidence / "run-provenance.json").write_text(
        json.dumps(
            {
                "vitrine_runtime_binary": str(binary),
                "vitrine_runtime_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "vitrine_driver": str(driver),
                "vitrine_revision": subprocess.run(
                    ["git", "-C", str(vitrine_root), "rev-parse", "HEAD"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip(),
                "gateway": "deterministic local fake model; no vendor model called",
                "container_image": _NODE_IMAGE,
            },
            indent=2,
        )
        + "\n"
    )


def _read_ndjson_from_container(container: str, path: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["docker", "exec", container, "cat", path],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
