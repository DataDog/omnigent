"""ACP environment exclusions survive each process launch boundary."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from omnigent.cli import _build_host_daemon_env
from omnigent.host.connect import _build_runner_env
from omnigent.inner.acp_harness import _build_acp_executor
from omnigent.runtime.workflow import _build_acp_spawn_env
from omnigent.spec.types import AgentSpec, ExecutorSpec


@pytest.mark.parametrize("server_url", [None, "https://example.databricksapps.com"])
def test_acp_env_exclusion_reaches_vendor_through_daemon_and_runner(
    tmp_path: Path, server_url: str | None
) -> None:
    """Keep requested credentials available to other harnesses, then strip them for ACP."""
    # Remote daemons inherit Databricks credentials; local daemons also inherit API keys.
    prefix = "DATABRICKS_" if server_url else ""
    excluded_name = f"{prefix}OPENAI_API_KEY"
    retained_name = f"{prefix}ANTHROPIC_API_KEY"
    spec = AgentSpec(
        spec_version=1,
        name="test-acp",
        instructions="Test agent",
        executor=ExecutorSpec(
            type="omnigent",
            config={
                "harness": "acp:helper",
                "acp_agent": {
                    "name": "Helper",
                    "command": "helper --acp",
                    "env_passthrough": [excluded_name, retained_name],
                },
            },
        ),
    )
    host_env = {
        "OMNIGENT_CONFIG_HOME": str(tmp_path),
        "OMNIGENT_ACP_ENV_UNSET": f"{excluded_name},UNRELATED_SECRET",
        "OMNIGENT_RUNNER_ENV_PASSTHROUGH": f"{excluded_name},{retained_name}",
        excluded_name: "synthetic-excluded-key",
        retained_name: "synthetic-retained-key",
        "UNRELATED_SECRET": "synthetic-unrelated-secret",
    }
    with patch.dict(os.environ, host_env, clear=True):
        daemon_env = _build_host_daemon_env(server_url=server_url)
        runner_env = _build_runner_env(
            daemon_env,
            server_url=server_url or "http://localhost:6767",
            runner_id="runner_acp_env",
            binding_token="synthetic-binding",
            workspace=str(tmp_path),
            parent_pid=12345,
        )
    for env in (daemon_env, runner_env):
        assert env["OMNIGENT_ACP_ENV_UNSET"] == host_env["OMNIGENT_ACP_ENV_UNSET"]
        assert env[excluded_name] == host_env[excluded_name]
        assert "UNRELATED_SECRET" not in env

    with patch.dict(os.environ, runner_env, clear=True):
        overrides = _build_acp_spawn_env(spec)
        with patch.dict(os.environ, overrides):
            executor = _build_acp_executor()
            vendor_env = executor._build_spawn_env()
    assert excluded_name not in vendor_env
    assert vendor_env[retained_name] == host_env[retained_name]
    assert "OMNIGENT_ACP_ENV_UNSET" not in vendor_env
    assert "UNRELATED_SECRET" not in vendor_env


_VIT_CONFIG = (
    "acp:\n  agents:\n    - name: vit\n      command: vit acp\n"
    "      env_passthrough: [VIT_PASSTHROUGH_TOKEN]\n"
)


def _vit_spec(os_env: object | None = None) -> AgentSpec:
    return AgentSpec(
        spec_version=1,
        name="test-vit",
        instructions="Test agent",
        executor=ExecutorSpec(type="omnigent", config={"harness": "acp:vit"}),
        os_env=os_env,
    )


def _runner_env(daemon_env: dict[str, str], tmp_path: Path, harness: str | None) -> dict[str, str]:
    return _build_runner_env(
        daemon_env,
        server_url="http://localhost:6767",
        runner_id="runner_acp_env",
        binding_token="synthetic-binding",
        workspace=str(tmp_path),
        parent_pid=12345,
        harness=harness,
    )


def _vendor_env(runner_env: dict[str, str], spec: AgentSpec) -> dict[str, str]:
    with patch.dict(os.environ, runner_env, clear=True):
        overrides = _build_acp_spawn_env(spec)
        with patch.dict(os.environ, overrides):
            return _build_acp_executor()._build_spawn_env()


@pytest.mark.parametrize("server_url", [None, "https://example.databricksapps.com"])
@pytest.mark.parametrize("runner_passthrough", [False, True])
def test_acp_agent_config_passthrough_reaches_vendor(
    tmp_path: Path, server_url: str | None, runner_passthrough: bool
) -> None:
    """An acp.agents env_passthrough name crosses daemon, runner and executor."""
    (tmp_path / "config.yaml").write_text(_VIT_CONFIG)
    host_env = {
        "OMNIGENT_CONFIG_HOME": str(tmp_path),
        "VIT_PASSTHROUGH_TOKEN": "synthetic-gpg-opts",
        "UNRELATED_SECRET": "synthetic-unrelated-secret",
    }
    if runner_passthrough:
        host_env["OMNIGENT_RUNNER_ENV_PASSTHROUGH"] = "VIT_PASSTHROUGH_TOKEN"
    with patch.dict(os.environ, host_env, clear=True):
        daemon_env = _build_host_daemon_env(server_url=server_url)
        runner_env = _runner_env(daemon_env, tmp_path, "acp:vit")
        other_env = _runner_env(daemon_env, tmp_path, "claude-native")
    vendor_env = _vendor_env(runner_env, _vit_spec())
    for env in (daemon_env, runner_env, vendor_env):
        assert env["VIT_PASSTHROUGH_TOKEN"] == "synthetic-gpg-opts"
        assert "UNRELATED_SECRET" not in env
    assert daemon_env["OMNIGENT_ACP_ENV_PASSTHROUGH"] == "VIT_PASSTHROUGH_TOKEN"
    # Without an explicit runner passthrough, other harnesses never see it.
    assert ("VIT_PASSTHROUGH_TOKEN" in other_env) is runner_passthrough


def test_spec_declared_passthrough_reaches_vendor(tmp_path: Path) -> None:
    """A spec os_env.sandbox.env_passthrough name forwarded by the CLI reaches the vendor."""
    from omnigent.inner.datamodel import OSEnvSandboxSpec, OSEnvSpec

    host_env = {
        "OMNIGENT_CONFIG_HOME": str(tmp_path),
        "VIT_PASSTHROUGH_TOKEN": "synthetic-gpg-opts",
        "UNRELATED_SECRET": "synthetic-unrelated-secret",
    }
    spec = AgentSpec(
        spec_version=1,
        name="test-acp",
        instructions="Test agent",
        executor=ExecutorSpec(
            type="omnigent",
            config={"harness": "acp:helper", "acp_agent": {"name": "Helper", "command": "h"}},
        ),
        os_env=OSEnvSpec(sandbox=OSEnvSandboxSpec(env_passthrough=["VIT_PASSTHROUGH_TOKEN"])),
    )
    with patch.dict(os.environ, host_env, clear=True):
        no_extra = _build_host_daemon_env(server_url=None)
        daemon_env = _build_host_daemon_env(
            server_url=None, extra_acp_passthrough=("VIT_PASSTHROUGH_TOKEN",)
        )
        runner_env = _runner_env(daemon_env, tmp_path, "acp:helper")
    assert "VIT_PASSTHROUGH_TOKEN" not in no_extra
    vendor_env = _vendor_env(runner_env, spec)
    assert vendor_env["VIT_PASSTHROUGH_TOKEN"] == "synthetic-gpg-opts"
    assert "UNRELATED_SECRET" not in vendor_env


def test_acp_env_unset_still_wins_over_agent_passthrough(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(_VIT_CONFIG)
    host_env = {
        "OMNIGENT_CONFIG_HOME": str(tmp_path),
        "OMNIGENT_ACP_ENV_UNSET": "VIT_PASSTHROUGH_TOKEN",
        "VIT_PASSTHROUGH_TOKEN": "synthetic-gpg-opts",
    }
    with patch.dict(os.environ, host_env, clear=True):
        runner_env = _runner_env(_build_host_daemon_env(server_url=None), tmp_path, "acp:vit")
    assert runner_env["VIT_PASSTHROUGH_TOKEN"] == "synthetic-gpg-opts"
    assert "VIT_PASSTHROUGH_TOKEN" not in _vendor_env(runner_env, _vit_spec())


@pytest.mark.parametrize("harness", ["claude-native", "codex", None])
def test_non_acp_runner_env_excludes_acp_only_names(tmp_path: Path, harness: str | None) -> None:
    (tmp_path / "config.yaml").write_text(_VIT_CONFIG)
    base = {
        "OMNIGENT_CONFIG_HOME": str(tmp_path),
        "OMNIGENT_ACP_ENV_PASSTHROUGH": "VIT_PASSTHROUGH_TOKEN",
        "VIT_PASSTHROUGH_TOKEN": "synthetic-gpg-opts",
    }
    with patch.dict(os.environ, base, clear=True):
        assert "VIT_PASSTHROUGH_TOKEN" not in _runner_env(base, tmp_path, harness)


def test_malformed_acp_config_does_not_block_daemon_env(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        "acp:\n  agents:\n    - name: vit\n      command: vit\n      env_passthrough: [A=b]\n"
    )
    with patch.dict(os.environ, {"OMNIGENT_CONFIG_HOME": str(tmp_path), "A": "x"}, clear=True):
        env = _build_host_daemon_env(server_url=None)
        runner_env = _runner_env(env, tmp_path, "acp:vit")
    assert "A" not in env and "A" not in runner_env
    assert "OMNIGENT_ACP_ENV_PASSTHROUGH" not in env
