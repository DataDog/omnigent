"""Stable security-context fingerprint contract for detached ACP sessions."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from omnigent.session_action_origin import session_action_context_generation


def _context() -> dict[str, Any]:
    return {
        "workspace_id": 3,
        "root_conversation_id": "root-session",
        "runner_id": "runner-session",
        "actor": "alice@example.test",
        "credential_principal": "runner@example.test",
        "agent_id": "agent-version-one",
        "bundle_location": "bundle://tenant/one",
        "agent_provenance": "tenant",
        "operator_authored": False,
        "effective_spec_digest": "spec-digest-one",
        "operator_environment_digest": "environment-digest-one",
        "policy_scope_digest": "policy-digest-one",
        "mcp_selection_snapshot": {"server-a": "selected"},
        "access_scope": {"kind": "owner", "user_id": "alice@example.test"},
        "credential_identity_refs": {"provider": "account-42", "principal": "alice"},
    }


def _fingerprint(context: dict[str, Any]) -> str:
    fields = {
        "workspace_id",
        "root_conversation_id",
        "runner_id",
        "actor",
        "credential_principal",
        "agent_id",
        "bundle_location",
        "agent_provenance",
        "operator_authored",
        "effective_spec_digest",
        "operator_environment_digest",
        "policy_scope_digest",
        "mcp_selection_snapshot",
        "access_scope",
        "credential_identity_refs",
    }
    return session_action_context_generation(**{key: context[key] for key in fields})


@pytest.mark.parametrize(
    "field, replacement",
    [
        ("workspace_id", 4),
        ("root_conversation_id", "other-root"),
        ("runner_id", "other-runner"),
        ("actor", "bob@example.test"),
        ("credential_principal", "other-runner@example.test"),
        ("agent_id", "agent-version-two"),
        ("bundle_location", "bundle://tenant/two"),
        ("agent_provenance", "operator"),
        ("effective_spec_digest", "spec-digest-two"),
        ("operator_environment_digest", "environment-digest-two"),
        ("policy_scope_digest", "policy-digest-two"),
        ("mcp_selection_snapshot", {"server-a": "unselected"}),
        ("access_scope", {"kind": "shared", "user_id": "alice@example.test"}),
        ("credential_identity_refs", {"provider": "account-43", "principal": "alice"}),
    ],
)
def test_context_fingerprint_changes_for_each_security_boundary(
    field: str, replacement: object
) -> None:
    baseline = _context()
    changed = deepcopy(baseline)
    changed[field] = replacement
    if field == "agent_provenance":
        changed["operator_authored"] = True

    assert _fingerprint(baseline) != _fingerprint(changed)


def test_context_fingerprint_is_stable_across_turn_and_harness_rotation() -> None:
    """Per-turn tokens and process generations are deliberately not inputs."""
    first_turn = _context()
    second_turn = deepcopy(first_turn)
    first_turn.update({"_turn_token": "turn-token-one", "_harness_generation": "gen-one"})
    second_turn.update({"_turn_token": "turn-token-two", "_harness_generation": "gen-two"})

    assert _fingerprint(first_turn) == _fingerprint(second_turn)


@pytest.mark.parametrize(
    "update",
    [
        {"workspace_id": True},
        {"agent_provenance": "other"},
        {"operator_authored": True},
        {"credential_identity_refs": {1: "bad-key"}},
        {"mcp_selection_snapshot": {"server-a": False}},
    ],
)
def test_context_fingerprint_rejects_malformed_identity_inputs(update: dict[str, object]) -> None:
    context = _context()
    context.update(update)

    with pytest.raises(ValueError):
        _fingerprint(context)
