"""Filesystem requests from detached ACP children stay inside both grants."""

from __future__ import annotations

from pathlib import Path

import pytest

from omnigent.inner.acp_executor import AcpAgentConfig, AcpExecutor
from omnigent.inner.acp_standard_subagents import AcpSessionActionBinding
from omnigent.inner.datamodel import OSEnvSandboxSpec, OSEnvSpec
from omnigent.runtime.harnesses._executor_adapter import ExecutorAdapter
from omnigent.runtime.harnesses._scaffold import PolicyVerdictEvent
from omnigent.server.schemas import PolicyEvaluationRequestEvent


@pytest.mark.asyncio
async def test_detached_child_fs_requests_intersect_child_and_host_roots(tmp_path: Path) -> None:
    """A child root narrows file access and each request checks current policy."""
    workspace = tmp_path / "workspace"
    child_root = workspace / "child"
    sibling = workspace / "sibling"
    child_shared = workspace / "child-shared"
    host_grant = tmp_path / "host-grant"
    child_only = tmp_path / "child-only"
    for directory in (child_root, sibling, child_shared, host_grant, child_only):
        directory.mkdir(parents=True)

    (child_root / "seed.txt").write_text("child data", encoding="utf-8")
    (sibling / "private.txt").write_text("parent data", encoding="utf-8")
    (host_grant / "private.txt").write_text("host grant data", encoding="utf-8")
    (child_only / "private.txt").write_text("outside host policy", encoding="utf-8")

    executor = AcpExecutor(
        AcpAgentConfig(command="fake-agent"),
        os_env=OSEnvSpec(
            cwd=str(workspace),
            sandbox=OSEnvSandboxSpec(
                type="none",
                read_paths=[str(host_grant)],
                write_paths=[str(host_grant)],
            ),
        ),
    )
    replies: list[dict] = []

    async def capture_reply(reply: dict) -> None:
        replies.append(reply)

    executor._send = capture_reply  # type: ignore[method-assign]
    adapter = ExecutorAdapter(executor_factory=lambda: executor)
    adapter._executor = executor
    binding = AcpSessionActionBinding(
        root_conversation_id="root",
        native_epoch="epoch",
        context_generation="context",
        root_native_session_id="root-session",
        target_session_id="child-session",
        parent_session_id="parent-session",
        association_source_id="association",
        origin_token="origin",
        origin_agent="agent",
        origin_generation="generation",
        request_id="request",
        method="fs/write_text_file",
        cwd=str(child_root),
        additional_directories=(str(child_shared), str(child_only)),
    )

    async def request(method: str, path: str, content: str | None = None) -> dict:
        request_id = len(replies) + 1
        params: dict[str, str] = {"path": path}
        if content is not None:
            params["content"] = content
        await adapter._handle_detached_session_request(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            binding,
        )
        return replies[-1]

    unavailable_path = child_root / "without-policy-bridge.txt"
    try:
        unavailable = await request(
            "fs/write_text_file", str(unavailable_path), "must fail closed"
        )
        assert "ACP child action validation is unavailable" in unavailable["error"]["message"]
        assert not unavailable_path.exists()

        policy_action = "POLICY_ACTION_ALLOW"
        policy_checks: list[str] = []

        async def evaluate_current_policy(
            _ctx: object, event: PolicyEvaluationRequestEvent
        ) -> PolicyVerdictEvent:
            policy_checks.append(event.phase)
            return PolicyVerdictEvent(
                type="policy_verdict",
                evaluation_id=event.evaluation_id,
                action=policy_action,
                reason="Child origin access was revoked."
                if policy_action != "POLICY_ACTION_ALLOW"
                else None,
            )

        # run_turn normally installs this bridge before detached requests arrive.
        # Exercise the same evaluator in this direct adapter-level filesystem test.
        executor._policy_evaluator = adapter._stable_policy_evaluator
        adapter.publish_session_action = evaluate_current_policy  # type: ignore[method-assign]

        inside_write = await request("fs/write_text_file", "created.txt", "child write")
        shared_write = await request(
            "fs/write_text_file", str(child_shared / "created.txt"), "shared write"
        )
        sibling_read = await request("fs/read_text_file", "../sibling/private.txt")
        sibling_write = await request(
            "fs/write_text_file", "../sibling/created.txt", "must not escape"
        )
        host_grant_read = await request("fs/read_text_file", str(host_grant / "private.txt"))
        host_grant_write = await request(
            "fs/write_text_file", str(host_grant / "created.txt"), "must be child-granted"
        )
        child_only_write = await request(
            "fs/write_text_file", str(child_only / "created.txt"), "must be host-granted"
        )
        policy_action = "POLICY_ACTION_DENY"
        revoked_path = child_root / "revoked.txt"
        revoked_write = await request(
            "fs/write_text_file", str(revoked_path), "must fail after revocation"
        )
    finally:
        await executor.close()

    observed = {
        "child_root_write_succeeded": "error" not in inside_write,
        "captured_additional_directory_write_succeeded": "error" not in shared_write,
        "sibling_read_denied": "error" in sibling_read,
        "sibling_write_denied_without_side_effect": (
            "error" in sibling_write and not (sibling / "created.txt").exists()
        ),
        "host_grant_read_denied_without_child_grant": "error" in host_grant_read,
        "host_grant_write_denied_without_child_grant": (
            "error" in host_grant_write and not (host_grant / "created.txt").exists()
        ),
        "child_grant_write_denied_by_host_policy": (
            "error" in child_only_write and not (child_only / "created.txt").exists()
        ),
        "origin_validation_fails_closed_without_evaluator": (
            "error" in unavailable and not unavailable_path.exists()
        ),
        "revocation_denies_later_child_write": (
            "error" in revoked_write and not revoked_path.exists()
        ),
        "current_policy_checked_for_each_request": len(policy_checks) == 8,
    }
    assert observed == {
        "child_root_write_succeeded": True,
        "captured_additional_directory_write_succeeded": True,
        "sibling_read_denied": True,
        "sibling_write_denied_without_side_effect": True,
        "host_grant_read_denied_without_child_grant": True,
        "host_grant_write_denied_without_child_grant": True,
        "child_grant_write_denied_by_host_policy": True,
        "origin_validation_fails_closed_without_evaluator": True,
        "revocation_denies_later_child_write": True,
        "current_policy_checked_for_each_request": True,
    }
