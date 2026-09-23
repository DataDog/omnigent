"""Tests for the fail-closed, secret-safe real Habitat preflight."""

from __future__ import annotations

import base64
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError

import pytest

_MODULE_PATH = Path(__file__).parents[3] / "dev/ticino_local_e2e/real_check.py"
_SPEC = importlib.util.spec_from_file_location("ticino_real_check", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
real_check = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = real_check
_SPEC.loader.exec_module(real_check)


def _write_workload_file(
    path: Path, *, audience: object = "identity", exp: int | None = None
) -> str:
    claims = {"aud": audience, "exp": exp or int(time.time()) + 3600}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    token = f"header.{payload}.signature"
    path.write_text(token)
    path.chmod(0o600)
    return token


def _real_environment(workload_file: Path) -> dict[str, str]:
    return {
        "TICINO_E2E_HABITAT_MODE": "real",
        "TICINO_E2E_ALLOW_REAL_HABITAT": "1",
        "HAB_APISERVER": "https://nickisaacs.habvm.dev",
        "OMNIGENT_HAB_EXCHANGE_MODE": "file",
        "HAB_WORKLOAD_TOKEN_FILE": str(workload_file),
        "OMNIGENT_HAB_PROFILE": "approved-dev-profile",
        "OMNIGENT_HAB_IMAGE": "registry.example/omnigent@sha256:" + "a" * 64,
        "OMNIGENT_HAB_ALLOW_ALL_EGRESS": "true",
        "OMNIGENT_PUBLIC_URL": "https://approved-tunnel.example",
        "OMNIGENT_OIDC_REDIRECT_URI": "http://127.0.0.1:6767/auth/callback",
    }


def test_real_environment_accepts_secure_explicit_inputs(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)

    assert real_check.validate_real_environment(_real_environment(workload_file), port=6767) == []


def test_real_environment_requires_both_opt_ins(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)
    environment = _real_environment(workload_file)
    environment["TICINO_E2E_ALLOW_REAL_HABITAT"] = ""

    errors = real_check.validate_real_environment(environment, port=6767)

    assert "TICINO_E2E_ALLOW_REAL_HABITAT must be exactly 1" in errors


def test_real_environment_rejects_fake_and_loopback_launch_inputs(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)
    environment = _real_environment(workload_file)
    environment.update(
        {
            "OMNIGENT_HAB_PROFILE": "local-fake-profile",
            "OMNIGENT_HAB_IMAGE": "local-fake-image",
            "OMNIGENT_HAB_ALLOW_ALL_EGRESS": "false",
            "OMNIGENT_PUBLIC_URL": "http://127.0.0.1:6767",
        }
    )

    errors = real_check.validate_real_environment(environment, port=6767)

    assert "OMNIGENT_HAB_PROFILE must name a real approved profile" in errors
    assert "OMNIGENT_HAB_IMAGE must be a non-fake immutable digest reference" in errors
    assert (
        "OMNIGENT_HAB_ALLOW_ALL_EGRESS must be explicitly true for this temporary test" in errors
    )
    assert "OMNIGENT_PUBLIC_URL must be a non-loopback HTTPS tunnel origin" in errors


@pytest.mark.parametrize("path", ["/callback", "/nested/path"])
def test_real_environment_rejects_public_url_paths(tmp_path: Path, path: str) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)
    environment = _real_environment(workload_file)
    environment["OMNIGENT_PUBLIC_URL"] = f"https://approved-tunnel.example{path}"

    errors = real_check.validate_real_environment(environment, port=6767)

    assert "OMNIGENT_PUBLIC_URL must be a non-loopback HTTPS tunnel origin" in errors


def test_real_environment_allows_a_root_public_url_path(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)
    environment = _real_environment(workload_file)
    environment["OMNIGENT_PUBLIC_URL"] = "https://approved-tunnel.example/"

    assert real_check.validate_real_environment(environment, port=6767) == []


class _Response:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool:
        return False


def _status_responder(statuses: dict[str, int], calls: list[str]):
    def responder(url: str, **_kwargs: object) -> _Response:
        calls.append(url)
        status = statuses[url]
        if status >= 400:
            raise HTTPError(url, status, "test status", hdrs=None, fp=None)
        return _Response(status)

    return responder


def test_network_check_verifies_the_callback_origin_is_healthy_and_protected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)
    environment = _real_environment(workload_file)
    public_url = environment["OMNIGENT_PUBLIC_URL"]
    calls: list[str] = []
    monkeypatch.setattr(
        real_check,
        "urlopen",
        _status_responder(
            {
                environment["HAB_APISERVER"]: 200,
                f"{public_url}/health": 200,
                f"{public_url}/v1/sessions": 401,
            },
            calls,
        ),
    )

    assert real_check.run_check(environment, port=6767, check_network=True) == []
    assert calls == [
        environment["HAB_APISERVER"],
        f"{public_url}/health",
        f"{public_url}/v1/sessions",
    ]


@pytest.mark.parametrize(
    ("health_status", "protected_status", "expected_error"),
    [
        (404, 401, "OMNIGENT_PUBLIC_URL/health is not HTTPS-reachable"),
        (
            200,
            200,
            "OMNIGENT_PUBLIC_URL/v1/sessions must return 401 or 403 without credentials",
        ),
    ],
)
def test_network_check_rejects_a_wrong_or_unprotected_callback_origin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    health_status: int,
    protected_status: int,
    expected_error: str,
) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)
    environment = _real_environment(workload_file)
    public_url = environment["OMNIGENT_PUBLIC_URL"]
    monkeypatch.setattr(
        real_check,
        "urlopen",
        _status_responder(
            {
                environment["HAB_APISERVER"]: 200,
                f"{public_url}/health": health_status,
                f"{public_url}/v1/sessions": protected_status,
            },
            [],
        ),
    )

    errors = real_check.run_check(environment, port=6767, check_network=True)

    assert expected_error in errors


def test_workload_file_rejects_insecure_or_near_expiry_tokens(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file, exp=int(time.time()) + 10)
    assert real_check.validate_workload_token_file(str(workload_file)) == (
        "HAB_WORKLOAD_TOKEN_FILE expires too soon for a real run"
    )

    _write_workload_file(workload_file)
    workload_file.chmod(0o640)
    assert real_check.validate_workload_token_file(str(workload_file)) == (
        "HAB_WORKLOAD_TOKEN_FILE must not allow group or other access"
    )


def test_preflight_errors_never_echo_bearer_content(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    token = _write_workload_file(workload_file, audience="wrong")
    environment = _real_environment(workload_file)

    errors = real_check.validate_real_environment(environment, port=6767)

    assert any("unexpected audience" in error for error in errors)
    assert all(token not in error for error in errors)


def test_workload_file_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.jwt"
    _write_workload_file(target)
    link = tmp_path / "workload.jwt"
    link.symlink_to(target)

    assert real_check.validate_workload_token_file(str(link)) == (
        "HAB_WORKLOAD_TOKEN_FILE must be a regular file"
    )


def test_workload_file_validation_uses_fd_based_nofollow_read(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.jwt"
    _write_workload_file(workload_file)

    assert real_check.validate_workload_token_file(str(workload_file)) is None
    source = _MODULE_PATH.read_text()
    assert "os.open(path_value, flags)" in source
    assert 'getattr(os, "O_NOFOLLOW", 0)' in source
    assert "os.fstat(file_descriptor)" in source
    assert ".read_bytes()" not in source


def test_harness_keeps_fake_mode_and_invokes_real_check_before_state_creation() -> None:
    script = (Path(__file__).parents[3] / "dev/ticino_local_e2e/run.sh").read_text()

    assert "habitat_mode=${TICINO_E2E_HABITAT_MODE:-fake}" in script
    assert 'if [[ "$habitat_mode" == real ]]; then' in script
    assert "real_check" in script
    assert 'write_runtime_env "$habitat_mode"' in script
    assert "OMNIGENT_HAB_EXCHANGE_MODE=file" in script
    assert "EMISSARY_ENABLED=false" in script
    assert "real-check) real_check ;;" in script
    assert '--remove-workload-file "$workload_file"' in script


def test_down_removes_recorded_real_bearer_and_warns_about_remote_cleanup(tmp_path: Path) -> None:
    state_dir = Path("/tmp") / f"omnigent-ticino-token-handoff-e2e-test-{tmp_path.name}"
    state_dir.mkdir(mode=0o700)
    workload_file = Path("/tmp") / f"omnigent-workload-bearer-test-{tmp_path.name}.jwt"
    _write_workload_file(workload_file)
    (state_dir / "real-workload-token-file").write_text(str(workload_file) + "\n")

    script = Path(__file__).parents[3] / "dev/ticino_local_e2e/run.sh"
    result = subprocess.run(
        [str(script), "down"],
        env=os.environ | {"TICINO_E2E_STATE_DIR": str(state_dir)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert not workload_file.exists()
    assert "did not delete or verify any remote Hab" in result.stderr
    assert not state_dir.exists()


def test_down_never_removes_arbitrary_recorded_user_file(tmp_path: Path) -> None:
    state_dir = Path("/tmp") / f"omnigent-ticino-token-handoff-e2e-test-{tmp_path.name}"
    state_dir.mkdir(mode=0o700)
    arbitrary_file = tmp_path / "important-user-file.jwt"
    _write_workload_file(arbitrary_file)
    (state_dir / "real-workload-token-file").write_text(str(arbitrary_file) + "\n")

    script = Path(__file__).parents[3] / "dev/ticino_local_e2e/run.sh"
    result = subprocess.run(
        [str(script), "down"],
        env=os.environ | {"TICINO_E2E_STATE_DIR": str(state_dir)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert arbitrary_file.exists()
    assert "could not safely remove" in result.stderr
    assert not state_dir.exists()


def test_reconfigure_real_preserves_state_and_checks_before_stopping_pids() -> None:
    script = (Path(__file__).parents[3] / "dev/ticino_local_e2e/run.sh").read_text()
    start = script.index("reconfigure_real()")
    command = script[start : script.index('case "${1:-}"', start)]

    assert '[[ -f "$runtime_env" && -f "$server_config" && -f "$pgpass_file" ]]' in command
    assert "load_runtime_env" in command
    assert "pg_isready" in command
    assert command.index("real_check") < command.index("stop_recorded_pid")
    assert 'stop_recorded_pid "$state_dir/server.pid" "Omnigent server"' in command
    assert 'stop_recorded_pid "$state_dir/fake-services.pid" "fake service"' in command
    assert "start_real_server" in command
    assert "docker compose" in command
    assert " down" not in command
