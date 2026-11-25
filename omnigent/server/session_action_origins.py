"""Private, authenticated resolution of detached ACP child-action origins."""

from __future__ import annotations

import copy
import hashlib
import logging
import os
import re
import threading
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

from fastapi import Request
from starlette.datastructures import Headers

from omnigent.db.db_models import current_workspace_id
from omnigent.entities import Conversation
from omnigent.errors import ErrorCode, OmnigentError
from omnigent.inner.acp_standard_subagents import acp_standard_child_key
from omnigent.mcp_selection import MCP_SELECTION_PREFIX, selected_mcp_spec
from omnigent.runner.identity import RUNNER_TUNNEL_TOKEN_HEADER
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.runner_tunnel_auth import (
    is_loopback_peer_host,
    validate_runner_token_for_declared_runner,
)
from omnigent.session_action_origin import (
    effective_spec_digest,
    operator_environment_digest,
    session_action_spec_cache_id,
)
from omnigent.spec.types import AgentSpec

_logger = logging.getLogger(__name__)
ACTION_ORIGIN_TOKEN_FIELD = "_omnigent_action_context_token"
ACTION_ORIGIN_GENERATION_FIELD = "_omnigent_action_context_generation"
ACTION_CHILD_KEY_FIELD = "_omnigent_action_child_key"
ACTION_CHILD_SESSION_ID_FIELD = "_omnigent_action_child_session_id"
_ACTION_ORIGIN_CACHE_SIZE = 128
_pinned_specs: OrderedDict[str, AgentSpec] = OrderedDict()
_pinned_specs_lock = threading.Lock()


def _forbidden() -> OmnigentError:
    return OmnigentError("Detached child origin context is unavailable.", code=ErrorCode.FORBIDDEN)


def action_context_fields(payload: object) -> tuple[str | None, str | None]:
    """Read the optional action token and generation from a request object."""
    if not isinstance(payload, dict):
        return None, None
    token = payload.get(ACTION_ORIGIN_TOKEN_FIELD)
    generation = payload.get(ACTION_ORIGIN_GENERATION_FIELD)
    if token is None and generation is None:
        return None, None
    if (
        not isinstance(token, str)
        or not token
        or len(token) > 256
        or not isinstance(generation, str)
        or not generation
        or len(generation) > 256
    ):
        raise OmnigentError("Invalid detached child origin context.", code=ErrorCode.INVALID_INPUT)
    return token, generation


def action_child_target_fields(payload: object) -> tuple[str | None, str | None]:
    """Read a detached child's private approval target pair, if supplied."""
    if not isinstance(payload, dict):
        return None, None
    child_key = payload.get(ACTION_CHILD_KEY_FIELD)
    child_id = payload.get(ACTION_CHILD_SESSION_ID_FIELD)
    if child_key is None and child_id is None:
        return None, None
    if (
        not isinstance(child_key, str)
        or not child_key
        or len(child_key) > 512
        or not isinstance(child_id, str)
        or not child_id
        or len(child_id) > 256
    ):
        raise OmnigentError(
            "Invalid detached child approval target.", code=ErrorCode.INVALID_INPUT
        )
    return child_key, child_id


def validate_action_origin_child_target(
    origin: dict[str, Any],
    *,
    child_key: str,
    child_id: str,
    parent_id: str,
    child: Conversation | None,
    child_association: dict[str, Any] | None = None,
    parent_association: dict[str, Any] | None = None,
) -> str:
    """Require the child target to match its persisted private binding and row."""
    children = origin.get("children")
    if (
        not isinstance(children, dict)
        or children.get(child_key) != child_id
        or child is None
        or child.id != child_id
        or child.labels.get("omnigent.acp.subagent_id") != child_key
    ):
        raise _forbidden()
    if child_association is None:
        if child.parent_conversation_id != parent_id:
            raise _forbidden()
        return child_id

    contexts = origin.get("child_context_generations")
    expected_context_generation = contexts.get(child_key) if isinstance(contexts, dict) else None
    native_epoch = child_association.get("native_epoch")
    native_session_id = child_association.get("native_session_id")
    parent_native_session_id = child_association.get("parent_native_session_id")
    if (
        expected_context_generation != origin.get("bound_context_generation")
        or not isinstance(expected_context_generation, str)
        or child_association.get("root_conversation_id") != parent_id
        or child_association.get("conversation_id") != child_id
        or child_association.get("context_generation") != expected_context_generation
        or not isinstance(native_epoch, str)
        or not isinstance(native_session_id, str)
        or not isinstance(parent_native_session_id, str)
        or child_key != acp_standard_child_key(parent_id, native_epoch, native_session_id)
        or parent_association is None
        or parent_association.get("root_conversation_id") != parent_id
        or parent_association.get("native_epoch") != native_epoch
        or parent_association.get("context_generation") != expected_context_generation
        or parent_association.get("native_session_id") != parent_native_session_id
        or parent_association.get("conversation_id") != child.parent_conversation_id
    ):
        raise _forbidden()
    return child_id


def _runner_for_request(request: object, conversation: Conversation) -> str:
    """Bind the HTTP caller to the conversation's authenticated runner."""
    raw_headers = getattr(request, "headers", None)
    if isinstance(raw_headers, Headers):
        headers = raw_headers
    elif isinstance(raw_headers, Mapping):
        normalized_headers: list[tuple[bytes, bytes]] = []
        for name, value in raw_headers.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise _forbidden()
            try:
                normalized_headers.append(
                    (name.lower().encode("latin-1"), value.encode("latin-1"))
                )
            except UnicodeEncodeError as exc:
                raise _forbidden() from exc
        headers = Headers(raw=normalized_headers)
    else:
        raise _forbidden()
    token = (headers.get(RUNNER_TUNNEL_TOKEN_HEADER) or "").strip()
    app = getattr(request, "app", None)
    allowed = getattr(getattr(app, "state", None), "runner_tunnel_tokens", None)
    client = getattr(request, "client", None)
    is_loopback = is_loopback_peer_host(getattr(client, "host", None))
    declared_runner_id = conversation.runner_id
    if not isinstance(declared_runner_id, str) or not declared_runner_id:
        raise _forbidden()
    context_runner_id = getattr(request, "runner_id", None)
    if isinstance(context_runner_id, str):
        if context_runner_id != declared_runner_id:
            raise _forbidden()
        if token:
            try:
                validate_runner_token_for_declared_runner(
                    headers,
                    declared_runner_id=context_runner_id,
                    allowed_tunnel_tokens=allowed,
                    is_loopback=is_loopback,
                )
            except ValueError as exc:
                raise _forbidden() from exc
        return context_runner_id
    try:
        runner_id = validate_runner_token_for_declared_runner(
            headers,
            declared_runner_id=declared_runner_id,
            allowed_tunnel_tokens=allowed,
            is_loopback=is_loopback,
        )
    except ValueError as exc:
        raise _forbidden() from exc
    if runner_id != conversation.runner_id:
        raise _forbidden()
    return runner_id


def bound_runner_for_request(request: object, *, conversation: Conversation) -> str:
    """Return the authenticated runner bound to *conversation* or reject the caller."""
    runner_id = _runner_for_request(request, conversation)
    if not runner_id:
        raise _forbidden()
    return runner_id


def validate_action_origin_request(
    request: object,
    *,
    conversation: Conversation,
    origin: dict[str, Any] | None,
    token: str,
    generation: str,
    credential_principal: str | None,
) -> dict[str, Any]:
    """Verify the runner tunnel, principal, conversation, and bound generation."""
    if (
        origin is None
        or credential_principal is None
        or re.fullmatch(r"[0-9a-f]{32}", token) is None
    ):
        raise _forbidden()
    runner_id = _runner_for_request(request, conversation)
    if (
        origin.get("session_id") != conversation.id
        or origin.get("workspace_id") != current_workspace_id()
        or origin.get("runner_id") != runner_id
        or origin.get("bound_runner_id") != runner_id
        or origin.get("bound_harness_generation") != generation
        or origin.get("bound_credential_principal") != credential_principal
    ):
        raise _forbidden()
    if not isinstance(origin.get("agent_id"), str):
        raise _forbidden()
    return origin


def bind_action_origin_request(
    request: object,
    *,
    conversation: Conversation,
    credential_principal: str | None,
) -> str:
    """Validate a first bind request and return the conversation's runner ID."""
    if credential_principal is None:
        raise _forbidden()
    runner_id = _runner_for_request(request, conversation)
    if not runner_id:
        raise _forbidden()
    return runner_id


def mcp_selection_snapshot(labels: dict[str, str]) -> dict[str, str]:
    """Copy only the session-scoped MCP selection labels into an origin."""
    return dict(
        sorted(
            (key, value) for key, value in labels.items() if key.startswith(MCP_SELECTION_PREFIX)
        )
    )


def action_origin_eligible_from_spec(spec: Any) -> bool:
    """Whether a bundle explicitly selects the generic ACP harness family."""
    executor = getattr(spec, "executor", None)
    config = getattr(executor, "config", None)
    harness = config.get("harness") if isinstance(config, dict) else None
    return isinstance(harness, str) and harness.startswith("acp")


def load_pinned_action_spec(
    origin: dict[str, Any],
    *,
    conversation: Conversation,
    agent_cache: AgentCache,
) -> AgentSpec:
    """Load the captured bundle by immutable cache identity; fail closed on drift."""
    agent_id = origin.get("agent_id")
    bundle_location = origin.get("bundle_location")
    operator_authored = origin.get("operator_authored")
    provenance = origin.get("agent_provenance")
    expected_spec_digest = origin.get("effective_spec_digest")
    expected_environment_digest = origin.get("operator_environment_digest")
    selection = origin.get("mcp_selection_snapshot")
    workspace_id = origin.get("workspace_id")
    if (
        not isinstance(agent_id, str)
        or not isinstance(bundle_location, str)
        or not isinstance(operator_authored, bool)
        or provenance != ("operator" if operator_authored else "tenant")
        or not isinstance(expected_spec_digest, str)
        or not isinstance(expected_environment_digest, str)
        or not isinstance(selection, dict)
        or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in selection.items()
        )
        or workspace_id != current_workspace_id()
    ):
        raise _forbidden()
    cache_id = session_action_spec_cache_id(
        agent_id=agent_id,
        bundle_location=bundle_location,
        operator_authored=operator_authored,
        environment_digest=expected_environment_digest,
        spec_digest=expected_spec_digest,
    )
    if operator_authored:
        # AgentCache stores the parsed spec by its first argument. Include a
        # hash of the live environment in that actual cache key so a changed
        # operator environment is reparsed and then rejected by the digest.
        live_environment = hashlib.sha256(
            repr(sorted(os.environ.items())).encode("utf-8")
        ).hexdigest()
    else:
        live_environment = "tenant"
    cache_id = hashlib.sha256(f"{workspace_id}:{cache_id}:{live_environment}".encode()).hexdigest()
    with _pinned_specs_lock:
        cached = _pinned_specs.get(cache_id)
        if cached is not None:
            _pinned_specs.move_to_end(cache_id)
            base_spec = copy.deepcopy(cached)
        else:
            base_spec = None
    if base_spec is None:
        try:
            loaded = agent_cache.load(
                f"action-origin-{cache_id}",
                bundle_location,
                expand_env=operator_authored,
            )
            actual_spec_digest = effective_spec_digest(loaded.spec)
            actual_environment_digest = operator_environment_digest(
                loaded.spec, operator_authored=operator_authored
            )
        except Exception as exc:
            _logger.warning(
                "Pinned ACP action spec could not be loaded",
                extra={"session_id": conversation.id, "exception_type": type(exc).__name__},
            )
            raise _forbidden() from exc
        if (
            actual_spec_digest != expected_spec_digest
            or actual_environment_digest != expected_environment_digest
        ):
            raise _forbidden()
        base_spec = copy.deepcopy(loaded.spec)
        with _pinned_specs_lock:
            _pinned_specs[cache_id] = copy.deepcopy(base_spec)
            _pinned_specs.move_to_end(cache_id)
            while len(_pinned_specs) > _ACTION_ORIGIN_CACHE_SIZE:
                _pinned_specs.popitem(last=False)
    try:
        return selected_mcp_spec(base_spec, selection)
    except OmnigentError as exc:
        raise _forbidden() from exc


def resolve_action_origin_context(
    request: Request,
    *,
    conversation: Conversation,
    conversation_store: Any,
    token: str,
    generation: str,
    credential_principal: str | None,
    agent_cache: AgentCache,
) -> tuple[dict[str, Any], AgentSpec, dict[str, str]]:
    """Authenticate an action origin and return its pinned spec and actor."""
    origin = conversation_store.get_session_action_origin(conversation.id, token)
    validated = validate_action_origin_request(
        request,
        conversation=conversation,
        origin=origin,
        token=token,
        generation=generation,
        credential_principal=credential_principal,
    )
    actor = validated.get("actor")
    if not isinstance(actor, str) or not actor:
        raise _forbidden()
    spec = load_pinned_action_spec(
        validated,
        conversation=conversation,
        agent_cache=agent_cache,
    )
    return validated, spec, {"run_as": actor}
