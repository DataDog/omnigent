"""The contributor scan must only exempt PRs from the base repository."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _ROOT / ".github/scripts/security-scan/should-scan.sh"


@pytest.mark.parametrize(
    ("head_repo", "expected_scan"),
    [
        ("DataDog/omnigent", "false"),
        ("datadog/OMNIGENT", "false"),
        ("someone/omnigent", "true"),
        ("", "true"),
    ],
)
def test_same_repository_is_trusted_but_forks_are_scanned(
    tmp_path: Path, head_repo: str, expected_scan: str
) -> None:
    output = tmp_path / "github-output"
    env = os.environ.copy()
    for key in ("GH_TOKEN", "MAINTAINERS", "PR", "PR_AUTHOR"):
        env.pop(key, None)
    env.update(
        EVENT_NAME="pull_request",
        AUTHOR_ASSOCIATION="NONE",
        HEAD_REPO=head_repo,
        REPO="DataDog/omnigent",
        GITHUB_OUTPUT=str(output),
    )
    result = subprocess.run(
        ["bash", str(_SCRIPT)], env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert f"scan={expected_scan}" in output.read_text().splitlines()


@pytest.mark.parametrize("workflow", ["security-gate.yml", "security-scan.yml"])
def test_workflows_pass_github_head_repository(workflow: str) -> None:
    path = _ROOT / ".github/workflows" / workflow
    data = yaml.safe_load(path.read_text())
    steps = next(iter(data["jobs"].values()))["steps"]
    trust_gate = next(step for step in steps if step["name"] == "Trust gate")
    assert trust_gate["env"]["HEAD_REPO"] == (
        "${{ github.event.pull_request.head.repo.full_name }}"
    )
    assert trust_gate["env"]["REPO"] == "${{ github.repository }}"
