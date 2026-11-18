"""Canonical identities for private, generation-bound ACP action contexts."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections.abc import Mapping
from enum import Enum
from pathlib import Path

from omnigent.spec.types import AgentSpec


def _canonical_value(value: object) -> object:
    """Convert parsed spec values to a deterministic JSON-compatible tree."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _canonical_value(getattr(value, field.name))
            for field in dataclasses.fields(value)
            # Skill directories point into process-local extraction folders.
            # Their contents/metadata are part of the bundle identity; the
            # temporary absolute path is not and differs across replicas.
            if field.name != "skill_dir"
        }
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("spec mapping keys must be strings")
        return {key: _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        canonical = [_canonical_value(item) for item in value]
        return sorted(
            canonical,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("spec contains a non-finite number")
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, bytes):
        return {"$bytes": value.hex()}
    raise TypeError(f"unsupported value in parsed spec: {type(value).__name__}")


def _digest(value: object) -> str:
    payload = json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def effective_spec_digest(spec: AgentSpec) -> str:
    """Digest the complete parsed, unselected bundle spec."""
    return _digest(spec)


def operator_environment_digest(spec: AgentSpec, *, operator_authored: bool) -> str:
    """Digest the operator-expanded spec surface without persisting credentials.

    Agent specs are parsed after ``${VAR}`` expansion. Hashing the complete
    parsed operator-authored spec therefore detects changes to any expanded
    value while keeping secret material out of the metadata record. Tenant
    bundles are not expanded against the operator environment and use a stable
    marker instead.
    """
    return _digest(
        {
            "operator_authored": operator_authored,
            "resolved_spec": spec if operator_authored else None,
        }
    )


def effective_policy_scope_digest(spec: AgentSpec) -> str:
    """Digest the typed policy surfaces used by this spec and its child agents.

    Current ``AgentSpec`` stores policy declarations under ``guardrails``;
    older typed spec variants exposed a top-level ``policies`` field. Read
    either shape without assuming a field every supported variant lacks.
    """

    def policy_surface(candidate: object) -> object:
        surface: dict[str, object] = {}
        guardrails = getattr(candidate, "guardrails", None)
        if guardrails is not None:
            surface["guardrails"] = guardrails
        if hasattr(candidate, "policies"):
            surface["policies"] = candidate.policies
        children = getattr(candidate, "sub_agents", None)
        if isinstance(children, (list, tuple)):
            surface["sub_agents"] = [policy_surface(child) for child in children]
        return surface

    return _digest(policy_surface(spec))


def session_action_spec_cache_id(
    *,
    agent_id: str,
    bundle_location: str,
    operator_authored: bool,
    environment_digest: str,
    spec_digest: str,
) -> str:
    """Return a cache identity bound to provenance and all resolved inputs."""
    return _digest(
        {
            "agent_id": agent_id,
            "bundle_location": bundle_location,
            "operator_authored": operator_authored,
            "environment_digest": environment_digest,
            "spec_digest": spec_digest,
        }
    )


def session_action_context_generation(
    *,
    workspace_id: int,
    root_conversation_id: str,
    runner_id: str,
    actor: str,
    credential_principal: str,
    agent_id: str,
    bundle_location: str,
    agent_provenance: str,
    operator_authored: bool,
    effective_spec_digest: str,
    operator_environment_digest: str,
    policy_scope_digest: str,
    mcp_selection_snapshot: Mapping[str, str],
    access_scope: Mapping[str, object],
    credential_identity_refs: Mapping[str, object],
) -> str:
    """Digest the validated, stable security context for ACP child sessions.

    Per-turn origin tokens and runner generations are deliberately excluded:
    they rotate while the same actor, bundle, access, and credential identities
    remain valid. Credential material itself must never be supplied here; only
    its stable identity references belong in the digest.
    """
    if isinstance(workspace_id, bool) or not isinstance(workspace_id, int) or workspace_id < 0:
        raise ValueError("action context workspace id is invalid")
    required_strings = (
        root_conversation_id,
        runner_id,
        actor,
        credential_principal,
        agent_id,
        bundle_location,
        effective_spec_digest,
        operator_environment_digest,
        policy_scope_digest,
    )
    if any(not isinstance(value, str) or not value for value in required_strings):
        raise ValueError("action context identity fields must be non-empty strings")
    if agent_provenance not in {"operator", "tenant"}:
        raise ValueError("action context agent provenance is invalid")
    if not isinstance(operator_authored, bool):
        raise ValueError("action context operator provenance is invalid")
    if operator_authored != (agent_provenance == "operator"):
        raise ValueError("action context provenance fields disagree")
    if not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in mcp_selection_snapshot.items()
    ):
        raise ValueError("action context MCP selection is malformed")
    if not all(isinstance(key, str) for key in access_scope):
        raise ValueError("action context access scope is malformed")
    if not all(isinstance(key, str) for key in credential_identity_refs):
        raise ValueError("action context credential references are malformed")
    return _digest(
        {
            "kind": "acp-session-action-context-v1",
            "workspace_id": workspace_id,
            "root_conversation_id": root_conversation_id,
            "runner_id": runner_id,
            "actor": actor,
            "credential_principal": credential_principal,
            "agent": {
                "id": agent_id,
                "bundle_location": bundle_location,
                "provenance": agent_provenance,
                "operator_authored": operator_authored,
                "effective_spec_digest": effective_spec_digest,
                "operator_environment_digest": operator_environment_digest,
                "policy_scope_digest": policy_scope_digest,
                "mcp_selection_snapshot": dict(mcp_selection_snapshot),
            },
            "access_scope": dict(access_scope),
            "credential_identity_refs": dict(credential_identity_refs),
        }
    )
