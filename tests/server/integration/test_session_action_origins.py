"""Server integration coverage for detached ACP action origins."""

from __future__ import annotations

import io
import json
import tarfile
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Response
from mcp.types import CallToolResult, TextContent, Tool

from omnigent.db.db_models import current_workspace_id
from omnigent.errors import ErrorCode, OmnigentError
from omnigent.mcp_selection import MCP_SELECTION_PREFIX
from omnigent.runner.identity import RUNNER_TUNNEL_TOKEN_HEADER, token_bound_runner_id
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.session_action_origins import (
    load_pinned_action_spec,
)
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.conversation_store.sqlalchemy_store import (
    SqlAlchemyConversationStore,
)
from omnigent.stores.permission_store.sqlalchemy_store import (
    SqlAlchemyPermissionStore,
)

_FORWARDED_ORIGIN_TOKEN_FIELD = "_omnigent_action_origin_token"

ACTOR = "alice@example.com"
RUNNER_PRINCIPAL = "runner-service@example.com"
RUNNER_TOKEN = "action-origin-runner-token"
RUNNER_ID = token_bound_runner_id(RUNNER_TOKEN)
GENERATION = "harness-generation-1"


def _new_agent_id() -> str:
    return f"ag_{uuid.uuid4().hex}"


def _bundle_bytes(
    revision: str,
    *,
    api_key: str = "test-key",
    harness: str = "claude-sdk",
) -> bytes:
    """Build a tenant bundle with policy, MCP, tool, and skill files."""
    files = {
        "config.yaml": (
            "spec_version: 1\n"
            f"name: action-origin-{revision}\n"
            "llm:\n"
            "  model: origin-test-model\n"
            "  connection:\n"
            f"    api_key: {api_key}\n"
            "executor:\n"
            "  type: omnigent\n"
            "  config:\n"
            f"    harness: {harness}\n"
            "guardrails:\n"
            "  policies:\n"
            "    ask_os:\n"
            "      type: function\n"
            "      on: [tool_call]\n"
            "      handler: omnigent.policies.builtins.safety.ask_on_os_tools\n"
            "tools:\n"
            f"  {revision}:\n"
            "    type: mcp\n"
            "    transport: registry\n"
            "    tools: [read_ticket]\n"
        ),
        f"tools/python/{revision}_tool.py": (
            f"def {revision}_tool() -> str:\n    return '{revision}'\n"
        ),
        f"skills/{revision}/SKILL.md": (
            f"---\nname: {revision}\ndescription: {revision} skill\n---\n\n"
            f"Use the {revision} skill.\n"
        ),
    }
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def _capture_origin(
    *,
    session_id: str,
    agent_id: str,
    bundle_location: str,
    spec_digest: str,
    environment_digest: str,
    selection: dict[str, str],
    actor: str = ACTOR,
    runner_id: str = RUNNER_ID,
    policy_scope_digest: str = "3" * 64,
) -> dict[str, Any]:
    """Return the immutable, server-captured portion of an origin record."""
    return {
        "session_id": session_id,
        "workspace_id": current_workspace_id(),
        "runner_id": runner_id,
        "turn_id": "turn-from-alice",
        "agent_id": agent_id,
        "bundle_location": bundle_location,
        "agent_provenance": "tenant",
        "operator_authored": False,
        "operator_environment_digest": environment_digest,
        "policy_scope_digest": policy_scope_digest,
        "mcp_selection_snapshot": selection,
        "effective_spec_digest": spec_digest,
        "actor": actor,
    }


def _runner_headers(
    identity: str = RUNNER_PRINCIPAL, *, token: str = RUNNER_TOKEN
) -> dict[str, str]:
    return {
        "X-Forwarded-Email": identity,
        RUNNER_TUNNEL_TOKEN_HEADER: token,
    }


def _find_origin_token(payload: object) -> str | None:
    if isinstance(payload, dict):
        token = payload.get(_FORWARDED_ORIGIN_TOKEN_FIELD)
        if isinstance(token, str):
            return token
        for value in payload.values():
            token = _find_origin_token(value)
            if token is not None:
                return token
    elif isinstance(payload, list):
        for value in payload:
            token = _find_origin_token(value)
            if token is not None:
                return token
    return None


def _allow_and_record_policy_actor(event: dict[str, Any]) -> dict[str, Any]:
    context = event.get("context")
    actor = context.get("actor") if isinstance(context, dict) else None
    run_as = actor.get("run_as") if isinstance(actor, dict) else None
    event_data = event.get("data")
    tool_name = event_data.get("name") if isinstance(event_data, dict) else None
    return {
        "result": "ALLOW",
        "set_labels": {
            "action_origin_policy_actor": str(run_as or "missing"),
            "action_origin_policy_tool": str(tool_name or event.get("type") or "missing"),
        },
    }


def _deny_marked_policy_call(event: dict[str, Any]) -> dict[str, Any]:
    data = event.get("data")
    arguments = data.get("arguments") if isinstance(data, dict) else None
    if isinstance(arguments, dict) and arguments.get("deny") is True:
        return {"result": "DENY", "reason": "Denied by current session policy."}
    return {"result": "ALLOW"}


def _ask_marked_policy_call(event: dict[str, Any]) -> dict[str, Any]:
    data = event.get("data")
    arguments = data.get("arguments") if isinstance(data, dict) else None
    if isinstance(arguments, dict) and arguments.get("ask") is True:
        return {"result": "ASK", "reason": "Approval required for this child call."}
    return {"result": "ALLOW"}


POLICY_REGISTRY = [
    {
        "handler": f"{__name__}._allow_and_record_policy_actor",
        "kind": "callable",
        "name": "Record detached action actor",
        "description": "Test policy that records the detached action actor.",
    },
    {
        "handler": f"{__name__}._deny_marked_policy_call",
        "kind": "callable",
        "name": "Deny marked detached action calls",
        "description": "Test policy that denies calls marked by arguments.",
    },
    {
        "handler": f"{__name__}._ask_marked_policy_call",
        "kind": "callable",
        "name": "Ask for marked detached action calls",
        "description": "Test policy that asks before marked calls.",
    },
]


class _Response202:
    status_code = 202
    headers: dict[str, str] = {}
    text = ""

    def raise_for_status(self) -> None:
        """A 202 acceptance never raises."""

    def json(self) -> dict[str, Any]:
        """A bodyless 202 acceptance."""
        return {}


class _OriginCheckingRunnerClient:
    """Check that the origin is durable before the server forwards a turn."""

    def __init__(self, store: SqlAlchemyConversationStore, session_id: str) -> None:
        self.store = store
        self.session_id = session_id
        self.origin: dict[str, Any] | None = None
        self.token: str | None = None
        self.forwarded: dict[str, Any] | None = None

    async def post(self, path: str, *, json: dict[str, Any], **_: Any) -> _Response202:
        self.forwarded = json
        self.token = _find_origin_token(json)
        if self.token is not None:
            # A fresh store instance proves registration committed before forwarding.
            reopened = SqlAlchemyConversationStore(self.store.storage_location)
            self.origin = reopened.get_session_action_origin(self.session_id, self.token)
        return _Response202()

    async def get(self, *_: Any, **__: Any) -> Any:
        raise NotImplementedError


async def _noop_relay_ready(*_: Any, **__: Any) -> None:
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("harness", "should_capture"),
    [("acp:gemini", True), ("claude-sdk", False)],
    ids=["gemini-acp-captures", "claude-sdk-does-not-capture"],
)
async def test_post_event_limits_capture_to_report_enabled_acp(
    auth_client: httpx.AsyncClient,
    auth_app: FastAPI,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    harness: str,
    should_capture: bool,
) -> None:
    """Only ACP turns capture an origin, and they do so before runner forwarding."""
    from omnigent.server.routes import sessions as sessions_mod
    from omnigent.server.routes.sessions import routes_events as events_mod

    bundle = _bundle_bytes("original", harness=harness)
    agent_id = _new_agent_id()
    bundle_location = f"{agent_id}/original.tar.gz"
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    artifact_store.put(bundle_location, bundle)
    agent = SqlAlchemyAgentStore(db_uri).create(
        agent_id, "origin integration agent", bundle_location
    )
    # The shared test app's agent store uses the same database; the row is a
    # trusted template for this test's session and its bundle is tenant-shaped.
    assert auth_app.state.agent_store.get(agent.id) is not None
    conv_store = SqlAlchemyConversationStore(db_uri)
    conversation = conv_store.create_conversation(
        agent_id=agent.id,
        runner_id=RUNNER_ID,
        labels={MCP_SELECTION_PREFIX + "original": "1"},
    )
    SqlAlchemyPermissionStore(db_uri).grant(ACTOR, conversation.id, 3)

    runner = _OriginCheckingRunnerClient(conv_store, conversation.id)

    async def _get_runner(*_: Any, **__: Any) -> _OriginCheckingRunnerClient:
        return runner

    monkeypatch.setattr(sessions_mod, "_get_runner_client", _get_runner)
    monkeypatch.setattr(events_mod, "_ensure_runner_relay_ready", _noop_relay_ready)
    response = await auth_client.post(
        f"/v1/sessions/{conversation.id}/events",
        json={
            "type": "message",
            "data": {
                "role": "user",
                "content": [{"type": "input_text", "text": "Start a detached child task"}],
            },
        },
        headers={"X-Forwarded-Email": ACTOR},
    )

    assert response.status_code == 202, response.text
    assert runner.forwarded is not None
    assert (runner.token is not None) is should_capture
    if should_capture:
        assert runner.origin is not None
        assert runner.origin["actor"] == ACTOR
        assert runner.origin["session_id"] == conversation.id
        assert runner.origin["bundle_location"] == bundle_location
        assert runner.origin["mcp_selection_snapshot"] == {MCP_SELECTION_PREFIX + "original": "1"}
        assert runner.origin.get("bound_credential_principal") is None
        assert runner.origin.get("bound_harness_generation") is None
    else:
        assert runner.origin is None


@pytest.mark.asyncio
async def test_initial_items_capture_origin_before_runner_dispatch(
    auth_client: httpx.AsyncClient,
    auth_app: FastAPI,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The initial-items path captures ACP context before forwarding its message."""
    from omnigent.server import mcp_compatibility
    from omnigent.server.auth import LEVEL_OWNER
    from omnigent.server.mcp_registry import McpRegistry, McpRegistryConfig, McpService
    from omnigent.server.routes._sessions import orchestration as orchestration_mod

    bundle_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    agent_id = _new_agent_id()
    bundle_location = f"{agent_id}/initial-items.tar.gz"
    bundle_store.put(bundle_location, _bundle_bytes("initial", harness="acp:gemini"))
    agent = SqlAlchemyAgentStore(db_uri).create_user_agent(
        agent_id, "initial-items action origin agent", bundle_location, owner=ACTOR
    )
    conv_store = SqlAlchemyConversationStore(db_uri)
    parent = conv_store.create_conversation(
        agent_id=agent.id,
        runner_id=RUNNER_ID,
        labels={MCP_SELECTION_PREFIX + "initial": "1"},
    )
    permissions = SqlAlchemyPermissionStore(db_uri)
    permissions.ensure_user(ACTOR)
    permissions.grant(ACTOR, parent.id, LEVEL_OWNER)
    auth_app.state.mcp_registry = McpRegistry(
        McpRegistryConfig(
            services=[
                McpService(
                    id="initial",
                    title="Initial item service",
                    url="https://initial.example.test/mcp",
                    auth="none",
                    allowed_users=[ACTOR],
                    tools=["read_ticket"],
                )
            ]
        ),
        None,
    )

    class InitialItemsOriginRunner:
        def __init__(self) -> None:
            self.forwarded: list[dict[str, Any]] = []
            self.origins: list[dict[str, Any] | None] = []

        async def post(self, path: str, *, json: dict[str, Any], **_: Any) -> _Response202:
            self.forwarded.append(json)
            session_id = path.split("/")[3]
            token = _find_origin_token(json)
            origin = None
            if token is not None:
                # Re-open storage before the mocked runner accepts the event.
                origin = SqlAlchemyConversationStore(db_uri).get_session_action_origin(
                    session_id, token
                )
            self.origins.append(origin)
            return _Response202()

    runner = InitialItemsOriginRunner()

    async def _get_runner(*_: Any, **__: Any) -> InitialItemsOriginRunner:
        return runner

    monkeypatch.setattr(orchestration_mod, "_get_runner_client", _get_runner)
    monkeypatch.setattr(orchestration_mod, "_ensure_runner_relay_ready", _noop_relay_ready)
    monkeypatch.setattr(
        mcp_compatibility,
        "require_registry_mcp_runtime",
        lambda *_args, **_kwargs: None,
    )
    initial_item = {
        "type": "message",
        "data": {
            "role": "user",
            "content": [{"type": "input_text", "text": "Start from this initial item"}],
        },
    }
    response = await auth_client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "parent_session_id": parent.id,
            "initial_items": [initial_item],
            "mcp_registry_services": ["initial"],
        },
        headers={"X-Forwarded-Email": ACTOR},
    )

    assert response.status_code == 201, response.text
    session_id = response.json()["id"]
    assert len(runner.forwarded) == 1
    dispatch = runner.forwarded[0]
    token = _find_origin_token(dispatch)
    assert token is not None
    assert len(runner.origins) == 1
    captured = runner.origins[0]
    assert captured is not None
    created_conversation = conv_store.get_conversation(session_id)
    assert created_conversation is not None
    assert captured["session_id"] == session_id
    assert captured["workspace_id"] == current_workspace_id()
    assert captured["runner_id"] == RUNNER_ID
    assert captured["agent_id"] == created_conversation.agent_id
    assert captured["bundle_location"] == bundle_location
    assert captured["agent_provenance"] == "tenant"
    assert captured["operator_authored"] is False
    assert captured["mcp_selection_snapshot"] == {MCP_SELECTION_PREFIX + "initial": "1"}
    assert captured["actor"] == ACTOR
    assert captured.get("bound_runner_id") is None
    assert dispatch["_omnigent_action_origin_token"] == token
    assert "bundle_location" not in dispatch
    assert "mcp_selection_snapshot" not in dispatch


@pytest.mark.asyncio
async def test_action_origin_bind_requires_runner_and_keeps_actor_separate(
    auth_client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Runner binding adds service identity without replacing the user actor."""
    from omnigent.server.auth import LEVEL_EDIT

    conv_store = SqlAlchemyConversationStore(db_uri)
    agent_id = _new_agent_id()
    conversation = conv_store.create_conversation(agent_id=agent_id, runner_id=RUNNER_ID)
    token = "a" * 32
    origin = _capture_origin(
        session_id=conversation.id,
        agent_id=agent_id,
        bundle_location=f"{agent_id}/immutable-bundle",
        spec_digest="1" * 64,
        environment_digest="2" * 64,
        selection={},
    )
    conv_store.register_session_action_origin(conversation.id, token, origin)

    url = f"/v1/sessions/{conversation.id}/action-origins/{token}/bind"
    missing_runner = await auth_client.post(
        url,
        json={"generation": GENERATION},
        headers={"X-Forwarded-Email": RUNNER_PRINCIPAL},
    )
    assert missing_runner.status_code == 403, missing_runner.text
    assert conv_store.get_session_action_origin(conversation.id, token) == origin

    wrong_runner = await auth_client.post(
        url,
        json={"generation": GENERATION},
        headers={
            "X-Forwarded-Email": RUNNER_PRINCIPAL,
            RUNNER_TUNNEL_TOKEN_HEADER: "different-runner-token",
        },
    )
    assert wrong_runner.status_code == 403, wrong_runner.text
    assert conv_store.get_session_action_origin(conversation.id, token) == origin

    wrong_token = await auth_client.post(
        f"/v1/sessions/{conversation.id}/action-origins/{'b' * 32}/bind",
        json={"generation": GENERATION},
        headers=_runner_headers(),
    )
    assert wrong_token.status_code == 403, wrong_token.text

    other_conversation = conv_store.create_conversation(agent_id=agent_id, runner_id=RUNNER_ID)
    wrong_session = await auth_client.post(
        f"/v1/sessions/{other_conversation.id}/action-origins/{token}/bind",
        json={"generation": GENERATION},
        headers=_runner_headers(),
    )
    assert wrong_session.status_code == 403, wrong_session.text

    permissions = SqlAlchemyPermissionStore(db_uri)
    permissions.ensure_user(ACTOR)
    permissions.grant(ACTOR, conversation.id, LEVEL_EDIT)
    bound = await auth_client.post(
        url,
        json={"generation": GENERATION},
        headers=_runner_headers(),
    )
    assert bound.status_code == 200, bound.text
    stored = SqlAlchemyConversationStore(db_uri).get_session_action_origin(conversation.id, token)
    assert stored is not None
    assert stored["actor"] == ACTOR
    assert stored["bound_runner_id"] == RUNNER_ID
    assert stored["bound_harness_generation"] == GENERATION
    assert stored["bound_credential_principal"] == RUNNER_PRINCIPAL

    different_principal = await auth_client.post(
        url,
        json={"generation": GENERATION},
        headers=_runner_headers("another-runner@example.com"),
    )
    assert different_principal.status_code in {403, 409}, different_principal.text


@pytest.mark.asyncio
async def test_action_origin_routes_accept_lowercase_runner_tunnel_header(
    auth_app: FastAPI,
    auth_client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Runner auth preserves Starlette's case-insensitive header lookup."""
    from omnigent.server.auth import LEVEL_EDIT

    conv_store = SqlAlchemyConversationStore(db_uri)
    permission_store = SqlAlchemyPermissionStore(db_uri)
    permission_store.ensure_user(ACTOR)

    configured_token = "configured-managed-runner-token"
    wire_token = "lowercase-header-loopback-runner-token"
    runner_id = token_bound_runner_id(wire_token)
    auth_app.state.runner_tunnel_tokens = frozenset({configured_token})
    conversation = conv_store.create_conversation(runner_id=runner_id)
    origin_token = uuid.uuid4().hex
    origin = _capture_origin(
        session_id=conversation.id,
        agent_id=_new_agent_id(),
        bundle_location="agent/immutable-bundle",
        spec_digest="1" * 64,
        environment_digest="2" * 64,
        selection={},
        runner_id=runner_id,
    )
    conv_store.register_session_action_origin(conversation.id, origin_token, origin)
    permission_store.grant(ACTOR, conversation.id, LEVEL_EDIT)
    headers = {
        "x-forwarded-email": RUNNER_PRINCIPAL,
        RUNNER_TUNNEL_TOKEN_HEADER.lower(): wire_token,
    }

    bind = await auth_client.post(
        f"/v1/sessions/{conversation.id}/action-origins/{origin_token}/bind",
        json={"generation": GENERATION},
        headers=headers,
    )
    assert bind.status_code == 200, bind.text

    bound_origin = conv_store.get_session_action_origin(conversation.id, origin_token)
    assert bound_origin is not None
    assert bound_origin["bound_runner_id"] == runner_id
    assert bound_origin["bound_harness_generation"] == GENERATION
    assert bound_origin["bound_credential_principal"] == RUNNER_PRINCIPAL

    inventory = await auth_client.get(
        f"/v1/sessions/{conversation.id}/action-origins",
        headers=headers,
    )
    assert inventory.status_code == 200, inventory.text
    assert inventory.json()["origins"] == [{"token": origin_token, "generation": GENERATION}]


@pytest.mark.asyncio
async def test_loopback_external_runner_binds_only_its_token_bound_origin(
    auth_app: FastAPI,
    auth_client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Loopback CLI runners may use their own binding token under an allow-list."""
    from omnigent.server.auth import LEVEL_EDIT

    conv_store = SqlAlchemyConversationStore(db_uri)
    permission_store = SqlAlchemyPermissionStore(db_uri)
    permission_store.ensure_user(ACTOR)
    agent_id = _new_agent_id()
    configured_token = "configured-server-runner-token"
    external_token = "one-shot-cli-runner-token"
    foreign_token = "another-runner-token"
    arbitrary_token = "arbitrary-unbound-token"
    non_loopback_token = "non-loopback-runner-token"
    auth_app.state.runner_tunnel_tokens = frozenset({configured_token})

    def create_unbound_origin(binding_token: str) -> tuple[Any, str, str]:
        runner_id = token_bound_runner_id(binding_token)
        conversation = conv_store.create_conversation(
            agent_id=agent_id,
            runner_id=runner_id,
        )
        origin_token = uuid.uuid4().hex
        origin = _capture_origin(
            session_id=conversation.id,
            agent_id=agent_id,
            bundle_location=f"{agent_id}/immutable-bundle",
            spec_digest="1" * 64,
            environment_digest="2" * 64,
            selection={},
            runner_id=runner_id,
        )
        conv_store.register_session_action_origin(conversation.id, origin_token, origin)
        permission_store.grant(ACTOR, conversation.id, LEVEL_EDIT)
        return conversation, origin_token, runner_id

    loopback_conversation, origin_token, loopback_runner_id = create_unbound_origin(external_token)
    assert external_token not in auth_app.state.runner_tunnel_tokens
    assert token_bound_runner_id(external_token) == loopback_conversation.runner_id

    bind_url = f"/v1/sessions/{loopback_conversation.id}/action-origins/{origin_token}/bind"
    # A valid token for a different runner and an arbitrary token cannot use
    # the authenticated principal header to bind this origin.
    foreign_conversation, _, _ = create_unbound_origin(foreign_token)
    assert foreign_conversation.runner_id == token_bound_runner_id(foreign_token)
    for rejected_token in (foreign_token, arbitrary_token):
        rejected = await auth_client.post(
            bind_url,
            json={"generation": GENERATION},
            headers=_runner_headers(token=rejected_token),
        )
        assert rejected.status_code == 403, rejected.text
        rejected_origin = conv_store.get_session_action_origin(
            loopback_conversation.id, origin_token
        )
        assert rejected_origin is not None
        assert rejected_origin.get("bound_runner_id") is None
        assert rejected_origin.get("bound_harness_generation") is None
        assert rejected_origin.get("bound_credential_principal") is None

    loopback_bind = await auth_client.post(
        bind_url,
        json={"generation": GENERATION},
        headers=_runner_headers(token=external_token),
    )
    assert loopback_bind.status_code == 200, loopback_bind.text
    bound_origin = conv_store.get_session_action_origin(loopback_conversation.id, origin_token)
    assert bound_origin is not None
    assert bound_origin["bound_runner_id"] == loopback_runner_id
    assert bound_origin["bound_harness_generation"] == GENERATION
    assert bound_origin["bound_credential_principal"] == RUNNER_PRINCIPAL
    assert bound_origin["actor"] == ACTOR

    # A remote caller remains subject to the explicit token allow-list even
    # when its token matches the target runner exactly.
    remote_conversation, remote_origin_token, remote_runner_id = create_unbound_origin(
        non_loopback_token
    )
    assert remote_runner_id == token_bound_runner_id(non_loopback_token)
    remote_transport = httpx.ASGITransport(
        app=auth_app,
        client=("198.51.100.44", 12345),
    )
    async with httpx.AsyncClient(
        transport=remote_transport,
        base_url="http://test",
    ) as remote_client:
        remote_bind = await remote_client.post(
            f"/v1/sessions/{remote_conversation.id}/action-origins/{remote_origin_token}/bind",
            json={"generation": GENERATION},
            headers=_runner_headers(token=non_loopback_token),
        )
    assert remote_bind.status_code == 403, remote_bind.text
    unbound_remote_origin = conv_store.get_session_action_origin(
        remote_conversation.id, remote_origin_token
    )
    assert unbound_remote_origin is not None
    assert unbound_remote_origin.get("bound_runner_id") is None
    assert unbound_remote_origin.get("bound_harness_generation") is None
    assert unbound_remote_origin.get("bound_credential_principal") is None


@pytest.mark.asyncio
async def test_action_origin_inventory_is_private_and_stale_settlement_is_exact(
    auth_client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Inventory exposes only the caller's bound records and stale cleanup is exact."""
    from omnigent.server.auth import LEVEL_EDIT

    conversations = SqlAlchemyConversationStore(db_uri)
    conversation = conversations.create_conversation(runner_id=RUNNER_ID)
    permissions = SqlAlchemyPermissionStore(db_uri)
    permissions.ensure_user(ACTOR)
    permissions.grant(ACTOR, conversation.id, LEVEL_EDIT)
    tokens = {name: uuid.uuid4().hex for name in ("stale", "current", "queued", "other-principal")}
    for token in tokens.values():
        conversations.register_session_action_origin(
            conversation.id,
            token,
            _capture_origin(
                session_id=conversation.id,
                agent_id=_new_agent_id(),
                bundle_location="agent/origin.tar.gz",
                spec_digest="1" * 64,
                environment_digest="2" * 64,
                selection={},
            ),
        )

    async def bind(token: str, generation: str, identity: str = RUNNER_PRINCIPAL):
        response = await auth_client.post(
            f"/v1/sessions/{conversation.id}/action-origins/{token}/bind",
            json={"generation": generation},
            headers=_runner_headers(identity),
        )
        assert response.status_code == 200, response.text
        return response

    await bind(tokens["stale"], "retired-generation")
    await bind(tokens["current"], "current-generation")
    await bind(tokens["other-principal"], "other-generation", "another-runner@example.com")

    inventory = await auth_client.get(
        f"/v1/sessions/{conversation.id}/action-origins",
        headers=_runner_headers(),
    )
    assert inventory.status_code == 200, inventory.text
    assert sorted(inventory.json()["origins"], key=lambda origin: origin["token"]) == sorted(
        [
            {"token": tokens["stale"], "generation": "retired-generation"},
            {"token": tokens["current"], "generation": "current-generation"},
        ],
        key=lambda origin: origin["token"],
    )

    stale_settlement = await auth_client.post(
        f"/v1/sessions/{conversation.id}/action-origins/{tokens['stale']}/settle",
        json={"generation": "retired-generation"},
        headers=_runner_headers(),
    )
    assert stale_settlement.status_code == 200, stale_settlement.text

    wrong_generation_settlement = await auth_client.post(
        f"/v1/sessions/{conversation.id}/action-origins/{tokens['current']}/settle",
        json={"generation": "retired-generation"},
        headers=_runner_headers(),
    )
    assert wrong_generation_settlement.status_code in {403, 409}, wrong_generation_settlement.text
    remaining = await auth_client.get(
        f"/v1/sessions/{conversation.id}/action-origins",
        headers=_runner_headers(),
    )
    assert remaining.status_code == 200, remaining.text
    assert remaining.json()["origins"] == [
        {"token": tokens["current"], "generation": "current-generation"}
    ]
    assert conversations.get_session_action_origin(conversation.id, tokens["queued"]) is not None
    assert (
        conversations.get_session_action_origin(conversation.id, tokens["other-principal"])
        is not None
    )


@pytest.mark.asyncio
async def test_action_origin_routes_keep_captured_actor_and_bundle_after_newer_turn(
    auth_client: httpx.AsyncClient,
    auth_app: FastAPI,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Detached calls use the original user/spec while credentials stay with runner B."""
    from omnigent.policies.registry import load_registry
    from omnigent.runtime.caps import RuntimeCaps
    from omnigent.server.auth import LEVEL_EDIT, LEVEL_READ
    from omnigent.server.mcp_registry import McpRegistry, McpRegistryConfig, McpService
    from omnigent.server.routes import sessions as sessions_mod
    from omnigent.server.routes.sessions import routes_agent as agent_routes
    from omnigent.server.routes.sessions import routes_hooks as hooks_routes
    from omnigent.session_action_origin import (
        effective_spec_digest,
        operator_environment_digest,
    )
    from omnigent.stores.policy_store.sqlalchemy_store import SqlAlchemyPolicyStore

    load_registry(extra_modules=[__name__])
    request.addfinalizer(load_registry)

    bundle_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    agent_id = _new_agent_id()
    original_location = f"{agent_id}/original.tar.gz"
    newer_location = f"{agent_id}/newer.tar.gz"
    bundle_store.put(original_location, _bundle_bytes("original", harness="acp:gemini"))
    bundle_store.put(newer_location, _bundle_bytes("newer", harness="acp:gemini"))
    agent_store = SqlAlchemyAgentStore(db_uri)
    agent_store.create(agent_id, "action origin integration agent", original_location)

    original_spec = (
        AgentCache(bundle_store, tmp_path / "spec-cache").load(agent_id, original_location).spec
    )
    original_selection = {MCP_SELECTION_PREFIX + "original": "1"}
    conv_store = SqlAlchemyConversationStore(db_uri)
    conversation = conv_store.create_conversation(
        agent_id=agent_id,
        runner_id=RUNNER_ID,
        labels={**original_selection, "omnigent.turn_actor": ACTOR},
    )
    permission_store = SqlAlchemyPermissionStore(db_uri)
    permission_store.ensure_user(ACTOR)
    permission_store.grant(ACTOR, conversation.id, LEVEL_EDIT)

    token = uuid.uuid4().hex
    origin = _capture_origin(
        session_id=conversation.id,
        agent_id=agent_id,
        bundle_location=original_location,
        spec_digest=effective_spec_digest(original_spec),
        environment_digest=operator_environment_digest(original_spec, operator_authored=False),
        selection=original_selection,
    )
    conv_store.register_session_action_origin(conversation.id, token, origin)
    bind = await auth_client.post(
        f"/v1/sessions/{conversation.id}/action-origins/{token}/bind",
        json={"generation": GENERATION},
        headers=_runner_headers(),
    )
    assert bind.status_code == 200, bind.text
    assert bind.json()["actor"] == ACTOR
    assert bind.json()["mcp_selection_snapshot"] == original_selection

    # Simulate a newer turn that changes both the active agent revision and
    # session MCP selection. Detached actions must continue using Alice's
    # captured spec and selection.
    agent_store.update(agent_id, newer_location)
    current_selection = {
        MCP_SELECTION_PREFIX + "original": "0",
        MCP_SELECTION_PREFIX + "newer": "1",
        "omnigent.turn_actor": "bob@example.com",
    }
    conv_store.set_labels(conversation.id, current_selection)

    original_caps = sessions_mod.get_caps()
    patched_caps = RuntimeCaps(
        execution_timeout=original_caps.execution_timeout,
        default_policies=[],
    )
    monkeypatch.setattr(sessions_mod, "get_caps", lambda: patched_caps)
    policy_store = SqlAlchemyPolicyStore(db_uri)
    from omnigent.runtime import _globals

    monkeypatch.setattr(_globals, "_policy_store", policy_store)
    # Add current session policies after the origin has been captured and
    # bound. Their visible effects prove the evaluator reads live policy state
    # while retaining the captured bundle and actor.
    policy_store.create(
        policy_id=uuid.uuid4().hex,
        session_id=conversation.id,
        name="record_current_action_actor",
        type="python",
        handler=f"{__name__}._allow_and_record_policy_actor",
    )
    policy_store.create(
        policy_id=uuid.uuid4().hex,
        session_id=conversation.id,
        name="deny_marked_current_action",
        type="python",
        handler=f"{__name__}._deny_marked_policy_call",
    )
    policy_store.create(
        policy_id=uuid.uuid4().hex,
        session_id=conversation.id,
        name="ask_marked_current_action",
        type="python",
        handler=f"{__name__}._ask_marked_policy_call",
    )
    policy_headers = _runner_headers()
    policy_payload = {
        "event": {
            "type": "PHASE_TOOL_CALL",
            "target": "",
            "data": {"name": "origin_context_probe", "arguments": {}},
            "context": {},
        },
        "_omnigent_action_context_token": token,
        "_omnigent_action_context_generation": GENERATION,
    }
    child_key = "acp-child-action-origin"

    async def post_child_start(
        target_parent_id: str,
        token_value: str,
        generation_value: str,
    ) -> httpx.Response:
        return await auth_client.post(
            f"/v1/sessions/{target_parent_id}/events",
            json={
                "type": "external_acp_subagent_start",
                "data": {
                    "subagent_id": child_key,
                    "title": "origin child",
                    "description": "exercise child-target validation",
                    "require_durable": True,
                    "_omnigent_action_context_token": token_value,
                    "_omnigent_action_context_generation": generation_value,
                },
            },
            headers=_runner_headers(),
        )

    other_parent = conv_store.create_conversation(agent_id=agent_id, runner_id=RUNNER_ID)
    child_start = await post_child_start(conversation.id, token, GENERATION)
    assert child_start.status_code in {200, 202}, child_start.text
    assert child_start.json().get("durable") is True, child_start.text
    child_id = child_start.json()["child_session_id"]
    bound_origin = conv_store.get_session_action_origin(conversation.id, token)
    assert bound_origin is not None
    assert bound_origin["children"] == {child_key: child_id}

    duplicate_child_start = await post_child_start(conversation.id, token, GENERATION)
    assert duplicate_child_start.status_code in {200, 202}, duplicate_child_start.text
    assert duplicate_child_start.json().get("durable") is True, duplicate_child_start.text
    assert duplicate_child_start.json()["child_session_id"] == child_id

    wrong_generation_start = await post_child_start(conversation.id, token, "stale-generation")
    assert wrong_generation_start.status_code in {400, 403, 409}, wrong_generation_start.text
    wrong_token_start = await post_child_start(conversation.id, uuid.uuid4().hex, GENERATION)
    assert wrong_token_start.status_code in {400, 403, 409}, wrong_token_start.text
    foreign_parent_start = await post_child_start(other_parent.id, token, GENERATION)
    assert foreign_parent_start.status_code in {400, 403, 409}, foreign_parent_start.text
    assert conv_store.get_session_action_origin(conversation.id, token)["children"] == {
        child_key: child_id
    }
    assert (
        conv_store.list_conversations(
            kind="sub_agent", parent_conversation_id=other_parent.id, limit=10
        ).data
        == []
    )

    # A missing current agent row must not skip policies in the captured spec.
    with monkeypatch.context() as missing_agent:
        missing_agent.setattr(auth_app.state.agent_store, "get", lambda _agent_id: None)
        evaluated = await auth_client.post(
            f"/v1/sessions/{conversation.id}/policies/evaluate",
            json=policy_payload,
            headers=policy_headers,
        )
    assert evaluated.status_code == 200, evaluated.text
    assert evaluated.json()["result"] == "POLICY_ACTION_ALLOW"
    updated_conversation = conv_store.get_conversation(conversation.id)
    assert updated_conversation is not None
    assert updated_conversation.labels["omnigent.turn_actor"] == "bob@example.com"
    assert updated_conversation.labels["action_origin_policy_actor"] == ACTOR

    async def evaluate_child_target(
        child_key_value: str | None,
        child_id_value: str | None,
        *,
        deny_if_evaluated: bool = True,
    ) -> httpx.Response:
        payload = json.loads(json.dumps(policy_payload))
        if deny_if_evaluated:
            payload["event"]["data"]["arguments"] = {"deny": True}
        if child_key_value is not None:
            payload["_omnigent_action_child_key"] = child_key_value
        if child_id_value is not None:
            payload["_omnigent_action_child_session_id"] = child_id_value
        return await auth_client.post(
            f"/v1/sessions/{conversation.id}/policies/evaluate",
            json=payload,
            headers=policy_headers,
        )

    invalid_child_targets = [
        ("unregistered-child-key", child_id),
        (child_key, uuid.uuid4().hex),
        (child_key, None),
        (None, child_id),
    ]
    for invalid_key, invalid_id in invalid_child_targets:
        rejected = await evaluate_child_target(invalid_key, invalid_id)
        assert rejected.status_code in {400, 403, 409}, rejected.text

    foreign_child = conv_store.create_conversation(
        title="foreign origin child",
        parent_conversation_id=other_parent.id,
        agent_id=agent_id,
        runner_id=RUNNER_ID,
        labels={"omnigent.acp.subagent_id": child_key},
    )
    foreign_target = await evaluate_child_target(child_key, foreign_child.id)
    assert foreign_target.status_code in {400, 403, 409}, foreign_target.text

    valid_child_target = await evaluate_child_target(child_key, child_id, deny_if_evaluated=False)
    assert valid_child_target.status_code == 200, valid_child_target.text
    assert valid_child_target.json()["result"] == "POLICY_ACTION_ALLOW"

    from omnigent.errors import ElicitationDeclinedError

    approval_targets: list[str | None] = []
    parent_interrupts: list[tuple[Any, ...]] = []

    async def decline_child_approval(*_args: Any, **kwargs: Any) -> bool:
        approval_targets.append(kwargs.get("target_session_id"))
        raise ElicitationDeclinedError("User declined the child approval.")

    async def record_parent_interrupt(*args: Any, **_kwargs: Any) -> None:
        parent_interrupts.append(args)

    monkeypatch.setattr(hooks_routes, "_hold_native_ask_gate", decline_child_approval)
    monkeypatch.setattr(hooks_routes, "_forward_session_change_to_runner", record_parent_interrupt)
    child_ask = json.loads(json.dumps(policy_payload))
    child_ask["event"]["data"]["arguments"] = {"ask": True}
    child_ask["_omnigent_action_child_key"] = child_key
    child_ask["_omnigent_action_child_session_id"] = child_id
    declined = await auth_client.post(
        f"/v1/sessions/{conversation.id}/policies/evaluate",
        json=child_ask,
        headers=policy_headers,
    )
    assert declined.status_code == 200, declined.text
    assert declined.json()["result"] == "POLICY_ACTION_DENY"
    assert approval_targets == [child_id]
    assert parent_interrupts == []

    class RecordingBackend:
        def __init__(self) -> None:
            self.list_calls: list[tuple[str, str]] = []
            self.call_calls: list[tuple[str, str, str, dict[str, Any]]] = []

        async def list_tools(self, service: Any, user_id: str) -> list[Tool]:
            self.list_calls.append((service.id, user_id))
            return [
                Tool(
                    name="read_ticket",
                    inputSchema={"type": "object", "properties": {}},
                )
            ]

        async def call_tool(
            self,
            service: Any,
            user_id: str,
            tool: str,
            arguments: dict[str, Any],
        ) -> CallToolResult:
            self.call_calls.append((service.id, user_id, tool, dict(arguments)))
            return CallToolResult(
                content=[TextContent(type="text", text=f"{service.id}:{user_id}:{tool}")]
            )

    backend = RecordingBackend()
    registry = McpRegistry(
        McpRegistryConfig(
            services=[
                McpService(
                    id="original",
                    title="Original service",
                    url="https://original.example.test/mcp",
                    auth="none",
                    allowed_users=[RUNNER_PRINCIPAL],
                    tools=["read_ticket"],
                ),
                McpService(
                    id="newer",
                    title="Newer service",
                    url="https://newer.example.test/mcp",
                    auth="none",
                    allowed_users=[RUNNER_PRINCIPAL],
                    tools=["read_ticket"],
                ),
            ]
        ),
        None,
    )
    auth_app.state.mcp_registry = registry
    auth_app.state.mcp_gateway_backend = backend

    async def _empty_runner_tool_list(*_: Any, **__: Any) -> Response:
        return Response(
            content=json.dumps({"result": {"tools": []}}),
            media_type="application/json",
        )

    # Keep this focused on the server resolver and registry adapter. The
    # runner's independent stdio tools/list path is covered elsewhere.
    monkeypatch.setattr(agent_routes, "_handle_mcp_tools_list", _empty_runner_tool_list)
    action_params = {
        "_omnigent_action_context_token": token,
        "_omnigent_action_context_generation": GENERATION,
    }
    session_list = await auth_client.post(
        f"/v1/sessions/{conversation.id}/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": action_params},
        headers=policy_headers,
    )
    assert session_list.status_code == 200, session_list.text
    session_tools = session_list.json()["result"]["tools"]
    assert [tool["name"] for tool in session_tools] == ["original__read_ticket"]
    assert backend.list_calls[-1] == ("original", RUNNER_PRINCIPAL)

    session_call = await auth_client.post(
        f"/v1/sessions/{conversation.id}/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                **action_params,
                "name": "original__read_ticket",
                "arguments": {"ticket_id": "TEST-123"},
            },
        },
        headers=policy_headers,
    )
    assert session_call.status_code == 200, session_call.text
    assert session_call.json()["result"]["content"][0]["text"] == (
        f"original:{RUNNER_PRINCIPAL}:read_ticket"
    )
    assert backend.call_calls[-1] == (
        "original",
        RUNNER_PRINCIPAL,
        "read_ticket",
        {"ticket_id": "TEST-123"},
    )
    after_session_call = conv_store.get_conversation(conversation.id)
    assert after_session_call is not None
    assert after_session_call.labels["action_origin_policy_actor"] == ACTOR

    # The live policy store now denies a marked call before the registry
    # backend can execute it.
    denied_session_call = await auth_client.post(
        f"/v1/sessions/{conversation.id}/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                **action_params,
                "name": "original__read_ticket",
                "arguments": {"deny": True},
            },
        },
        headers=policy_headers,
    )
    assert "Denied by current session policy" in denied_session_call.text
    assert len(backend.call_calls) == 1

    registry_list = await auth_client.post(
        "/v1/mcp/original",
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": action_params},
        headers={**policy_headers, "X-Omnigent-Session-Id": conversation.id},
    )
    assert registry_list.status_code == 200, registry_list.text
    assert [tool["name"] for tool in registry_list.json()["result"]["tools"]] == ["read_ticket"]
    assert backend.list_calls[-1] == ("original", RUNNER_PRINCIPAL)

    registry_call = await auth_client.post(
        "/v1/mcp/original",
        json={
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                **action_params,
                "name": "read_ticket",
                "arguments": {"ticket_id": "TEST-456"},
            },
        },
        headers={**policy_headers, "X-Omnigent-Session-Id": conversation.id},
    )
    assert registry_call.status_code == 200, registry_call.text
    assert registry_call.json()["result"]["content"][0]["text"] == (
        f"original:{RUNNER_PRINCIPAL}:read_ticket"
    )
    assert backend.call_calls[-1] == (
        "original",
        RUNNER_PRINCIPAL,
        "read_ticket",
        {"ticket_id": "TEST-456"},
    )

    denied_registry_call = await auth_client.post(
        "/v1/mcp/original",
        json={
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {**action_params, "name": "read_ticket", "arguments": {"deny": True}},
        },
        headers={**policy_headers, "X-Omnigent-Session-Id": conversation.id},
    )
    assert denied_registry_call.status_code == 200, denied_registry_call.text
    assert "Denied by current session policy" in denied_registry_call.text
    assert len(backend.call_calls) == 2

    newer_registry_list = await auth_client.post(
        "/v1/mcp/newer",
        json={"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": action_params},
        headers={**policy_headers, "X-Omnigent-Session-Id": conversation.id},
    )
    assert newer_registry_list.status_code == 403, newer_registry_list.text
    assert backend.list_calls == [("original", RUNNER_PRINCIPAL)] * 2

    # The captured actor's current edit ACL controls detached operations; B's
    # backend credential remains unchanged and cannot substitute for Alice.
    permission_store.grant(ACTOR, conversation.id, LEVEL_READ)
    denied = await auth_client.post(
        "/v1/mcp/original",
        json={"jsonrpc": "2.0", "id": 4, "method": "tools/list", "params": action_params},
        headers={**policy_headers, "X-Omnigent-Session-Id": conversation.id},
    )
    assert denied.status_code == 403, denied.text
    assert len(backend.list_calls) == 2

    read_only_session_call = await auth_client.post(
        f"/v1/sessions/{conversation.id}/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                **action_params,
                "name": "original__read_ticket",
                "arguments": {"ticket_id": "NO-EXECUTE"},
            },
        },
        headers=policy_headers,
    )
    assert "permission" in read_only_session_call.text.lower()
    assert len(backend.call_calls) == 2

    read_only_registry_call = await auth_client.post(
        "/v1/mcp/original",
        json={
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                **action_params,
                "name": "read_ticket",
                "arguments": {"ticket_id": "NO-EXECUTE"},
            },
        },
        headers={**policy_headers, "X-Omnigent-Session-Id": conversation.id},
    )
    assert read_only_registry_call.status_code == 403, read_only_registry_call.text
    assert len(backend.call_calls) == 2


def test_effective_digest_and_pinned_load_are_stable_across_cache_roots(
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persisted child origins resolve old tools, policy, skill, and MCP choices."""
    from omnigent.server import session_action_origins as action_origins
    from omnigent.session_action_origin import (
        effective_spec_digest,
        operator_environment_digest,
    )

    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    agent_id = _new_agent_id()
    original_location = f"{agent_id}/original.tar.gz"
    newer_location = f"{agent_id}/newer.tar.gz"
    artifact_store.put(original_location, _bundle_bytes("original"))
    artifact_store.put(newer_location, _bundle_bytes("newer"))
    cache_a = AgentCache(artifact_store, tmp_path / "cache-a")
    original_spec = cache_a.load(agent_id, original_location).spec
    expected_digest = effective_spec_digest(original_spec)
    # `operator_authored=False` is important: the tenant bundle is parsed
    # without resolving process environment variables.
    expected_environment_digest = operator_environment_digest(
        original_spec, operator_authored=False
    )
    assert original_spec.local_tools[0].name == "original_tool"
    assert original_spec.skills[0].content == "Use the original skill."

    conversations = SqlAlchemyConversationStore(db_uri)
    conversation = conversations.create_conversation(
        agent_id=agent_id,
        runner_id=RUNNER_ID,
        labels={MCP_SELECTION_PREFIX + "original": "1"},
    )
    token = uuid.uuid4().hex
    origin = _capture_origin(
        session_id=conversation.id,
        agent_id=agent_id,
        bundle_location=original_location,
        spec_digest=expected_digest,
        environment_digest=expected_environment_digest,
        selection={MCP_SELECTION_PREFIX + "original": "1"},
    )
    conversations.register_session_action_origin(conversation.id, token, origin)
    # The current parent agent revision and session MCP selection can advance
    # while an older child still holds this token.
    conversations.set_labels(
        conversation.id,
        {
            MCP_SELECTION_PREFIX + "original": "0",
            MCP_SELECTION_PREFIX + "newer": "1",
        },
    )
    agent_store = SqlAlchemyAgentStore(db_uri)
    agent_store.create(agent_id, f"origin-{uuid.uuid4().hex}", original_location)
    agent_store.update(agent_id, newer_location)
    newer_spec = cache_a.load(agent_id, newer_location).spec
    newer_token = uuid.uuid4().hex
    newer_origin = _capture_origin(
        session_id=conversation.id,
        agent_id=agent_id,
        bundle_location=newer_location,
        spec_digest=effective_spec_digest(newer_spec),
        environment_digest=operator_environment_digest(newer_spec, operator_authored=False),
        selection={MCP_SELECTION_PREFIX + "newer": "1"},
        actor="bob@example.com",
    )
    conversations.register_session_action_origin(conversation.id, newer_token, newer_origin)

    restored_origin = SqlAlchemyConversationStore(db_uri).get_session_action_origin(
        conversation.id, token
    )
    assert restored_origin == origin
    assert restored_origin["actor"] == ACTOR
    assert (
        SqlAlchemyConversationStore(db_uri).get_session_action_origin(conversation.id, newer_token)
        == newer_origin
    )
    current_conversation = SqlAlchemyConversationStore(db_uri).get_conversation(conversation.id)
    assert current_conversation is not None
    assert current_conversation.labels.get(MCP_SELECTION_PREFIX + "original") == "0"
    assert current_conversation.labels.get(MCP_SELECTION_PREFIX + "newer") == "1"

    class RecordingAgentCache(AgentCache):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.loads: list[tuple[str, str, bool]] = []

        def load(self, agent_id: str, bundle_location: str, *, expand_env: bool = False):
            self.loads.append((agent_id, bundle_location, expand_env))
            return super().load(agent_id, bundle_location, expand_env=expand_env)

    monkeypatch.setattr(action_origins, "_pinned_specs", OrderedDict())
    cache_b = RecordingAgentCache(artifact_store, tmp_path / "cache-b")
    pinned = load_pinned_action_spec(
        restored_origin,
        conversation=current_conversation,
        agent_cache=cache_b,
    )
    assert len(cache_b.loads) == 1
    assert cache_b.loads[0][1:] == (original_location, False)
    assert cache_b.loads[0][0].startswith("action-origin-")
    assert [tool.name for tool in pinned.local_tools] == ["original_tool"]
    assert [server.name for server in pinned.mcp_servers] == ["original"]
    assert pinned.skills[0].content == "Use the original skill."
    assert [policy.name for policy in pinned.guardrails.policies] == ["ask_os"]

    # A second cache root models a fresh server process. The stored digest must
    # describe the bundle, not the absolute extraction path of a skill file.
    cache_c = AgentCache(artifact_store, tmp_path / "cache-c")
    restored_spec = cache_c.load("digest-check", original_location).spec
    assert effective_spec_digest(restored_spec) == expected_digest
    assert original_spec.skills[0].skill_dir is not None
    assert restored_spec.skills[0].skill_dir is not None
    assert restored_spec.skills[0].skill_dir != original_spec.skills[0].skill_dir


def test_pinned_operator_bundle_fails_closed_after_environment_drift(
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator bundle cannot silently resolve against a changed environment."""
    from omnigent.session_action_origin import (
        effective_spec_digest,
        operator_environment_digest,
    )

    env_key = "OMNIGENT_ACTION_ORIGIN_TEST_SECRET"
    monkeypatch.setenv(env_key, "first-value")
    artifact_store = LocalArtifactStore(str(tmp_path / "operator-artifacts"))
    agent_id = _new_agent_id()
    location = f"{agent_id}/operator.tar.gz"
    artifact_store.put(location, _bundle_bytes("operator", api_key=f"${{{env_key}}}"))
    cache = AgentCache(artifact_store, tmp_path / "operator-cache")
    spec = cache.load(agent_id, location, expand_env=True).spec
    origin = {
        "session_id": "operator-session",
        "workspace_id": current_workspace_id(),
        "runner_id": RUNNER_ID,
        "turn_id": "operator-turn",
        "agent_id": agent_id,
        "bundle_location": location,
        "agent_provenance": "operator",
        "operator_authored": True,
        "operator_environment_digest": operator_environment_digest(spec, operator_authored=True),
        "mcp_selection_snapshot": {},
        "effective_spec_digest": effective_spec_digest(spec),
        "actor": ACTOR,
    }
    conversations = SqlAlchemyConversationStore(db_uri)
    conversation = conversations.create_conversation(agent_id=agent_id, runner_id=RUNNER_ID)
    origin["session_id"] = conversation.id

    # First resolution is from this exact captured environment.
    from omnigent.server import session_action_origins as action_origins

    action_origins._pinned_specs.clear()
    assert (
        load_pinned_action_spec(origin, conversation=conversation, agent_cache=cache).name
        == "action-origin-operator"
    )

    monkeypatch.setenv(env_key, "changed-value")
    action_origins._pinned_specs.clear()
    with pytest.raises(OmnigentError) as exc_info:
        load_pinned_action_spec(
            origin,
            conversation=conversation,
            agent_cache=AgentCache(artifact_store, tmp_path / "operator-cache-after-drift"),
        )
    assert exc_info.value.code == ErrorCode.FORBIDDEN
