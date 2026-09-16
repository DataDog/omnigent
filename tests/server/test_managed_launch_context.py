"""Red tests for managed-launch identity-context capture (Slice 3).

Pins the ContextVar-based identity propagation the managed-host create
path must provide: the background launch task (and the launcher it
constructs/provisions through ``asyncio.to_thread``) must observe the
``ManagedSandboxContext`` bound to the created session and its verified
owner, while the request task that scheduled the launch must NOT retain
it. The zero-argument launcher-factory seam stays unchanged — the fake
records the ambient context without any provider signature change.

Red state: the create route never scopes ``asyncio.create_task``, so
every context observed inside the background launch is ``None`` and each
test fails on that missing context.
"""

from __future__ import annotations

import asyncio
import threading

import pytest
from httpx import ASGITransport, AsyncClient

from omnigent.onboarding.sandboxes.context import (
    IdentityToken,
    ManagedSandboxContext,
    current_managed_sandbox_context,
)
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.server.auth import AuthProvider
from omnigent.server.managed_hosts import parse_sandbox_config
from omnigent.server.routes._sessions.common import _managed_launch_tasks
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.comment_store.sqlalchemy_store import SqlAlchemyCommentStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
from omnigent.stores.host_store import HostStore
from tests.server.helpers import FakeSandboxLauncher, create_test_agent

pytestmark = pytest.mark.asyncio

_HEADER = "X-Forwarded-Email"
_ALICE = "alice@example.com"
_BOB = "bob@example.com"
_SETTLE_TIMEOUT_S = 15.0


class _StubIdentityTokenProvider:
    """IdentityTokenProvider stand-in recognized by object identity."""

    def get_identity_token(self) -> IdentityToken:
        return IdentityToken(value="stub-id-token", expires_at=0)


class _HeaderAuthProvider(AuthProvider):
    """Header auth that hands out per-user identity-token providers.

    Mirrors how a deployment injects a custom provider: user extraction
    reads the trusted identity header; ``get_identity_token_provider``
    returns a bound provider only for users it was configured for.
    """

    def __init__(self, token_providers: dict[str, object] | None = None) -> None:
        self._token_providers = token_providers if token_providers is not None else {}

    def get_user_id(self, request: object) -> str | None:
        return getattr(request, "headers", {}).get(_HEADER)

    def get_identity_token_provider(self, request: object, expected_user_id: str) -> object | None:
        return self._token_providers.get(expected_user_id)


class _ContextRecord:
    """One observation of the ambient managed-sandbox context."""

    def __init__(
        self, phase: str, name: str | None, context: ManagedSandboxContext | None
    ) -> None:
        self.phase = phase
        self.name = name
        self.context = context


class _RecordingFakeSandboxLauncher(FakeSandboxLauncher):
    """FakeSandboxLauncher that records the ambient context at provision.

    Records once at provision entry and once after the provision gate
    releases, so tests can prove the context outlives the create request
    even while provisioning is held mid-flight.
    """

    def __init__(self, records, lock, **kwargs):
        super().__init__(**kwargs)
        self._records = records
        self._lock = lock

    def provision(self, name):
        before = current_managed_sandbox_context()
        with self._lock:
            self._records.append(_ContextRecord("provision-before", name, before))
        out = super().provision(name)  # blocks on the gate when set
        after = current_managed_sandbox_context()
        with self._lock:
            self._records.append(_ContextRecord("provision-after", name, after))
        return out


def _install_recording_modal_launcher(monkeypatch, records, lock, **launcher_kwargs) -> None:
    """Modal ctor shim recording construction-time ambient context.

    Same seam as ``install_fake_modal_launcher`` (the modal factory
    resolves ``ModalSandboxLauncher`` at call time): production code
    constructs the launcher through the zero-argument factory unchanged.
    """
    import omnigent.onboarding.sandboxes.modal as modal_mod

    def _ctor(*, image=None, secrets=None):
        with lock:
            records.append(_ContextRecord("constructor", None, current_managed_sandbox_context()))
        launcher = _RecordingFakeSandboxLauncher(records, lock, **launcher_kwargs)
        launcher.image = image
        launcher.secrets = secrets
        return launcher

    monkeypatch.setattr(modal_mod, "ModalSandboxLauncher", _ctor)


class _Env:
    """Assembled managed-session context-capture test environment."""

    def __init__(self, client, records, lock, token_providers):
        self.client = client
        self.records = records
        self.lock = lock
        self.token_providers = token_providers


@pytest.fixture()
async def env(db_uri, tmp_path, monkeypatch) -> _Env:
    """App wired like the managed-session integration harness, plus header auth.

    The fake sandbox never registers a host, so each background launch
    settles as failed after provision — exactly the window these tests
    observe. Online/runner waits are shrunk so tasks settle in-test.
    """
    monkeypatch.setattr("omnigent.server.managed_hosts.MANAGED_HOST_ONLINE_TIMEOUT_S", 0.2)
    monkeypatch.setattr(
        "omnigent.server.routes.sessions._HOST_RELAUNCH_RUNNER_CONNECT_TIMEOUT_S", 0.2
    )
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    host_store = HostStore(db_uri)
    token_providers: dict[str, object] = {}
    app = create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(artifact_store=artifact_store, cache_dir=tmp_path / "cache"),
        comment_store=SqlAlchemyCommentStore(db_uri),
        host_store=host_store,
        auth_provider=_HeaderAuthProvider(token_providers),
        sandbox_config=parse_sandbox_config(
            {
                "provider": "modal",
                "server_url": "https://managed-context-test.example.com",
                "modal": {"image": "docker.io/test/omnigent-host:latest"},
            }
        ),
    )
    records: list = []
    lock = threading.Lock()
    _install_recording_modal_launcher(monkeypatch, records, lock)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield _Env(client, records, lock, token_providers)


async def _create_managed_session(env: _Env, agent_id: str, user: str) -> dict:
    """POST a managed session as *user* and return the 201 body."""
    resp = await env.client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "host_type": "managed"},
        headers={_HEADER: user},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["host_id"] is None, "managed create must return before provisioning"
    return body


async def _await_settled(before: set) -> None:
    """Wait until the launches scheduled after *before* leave the task set."""
    deadline = asyncio.get_running_loop().time() + _SETTLE_TIMEOUT_S
    while asyncio.get_running_loop().time() < deadline:
        if not (set(_managed_launch_tasks) - before):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("background managed launch never settled")


def _records_of(env: _Env, phase: str) -> list[_ContextRecord]:
    with env.lock:
        return [r for r in env.records if r.phase == phase]


async def test_background_launch_sees_session_and_owner(env: _Env) -> None:
    """The provisioned launcher observes the exact session id and owner."""
    before = set(_managed_launch_tasks)
    agent = await create_test_agent(env.client, name="ctx-agent", user=_ALICE)
    session = await _create_managed_session(env, agent["id"], _ALICE)

    await _await_settled(before)

    records = _records_of(env, "provision-before")
    assert records, "background launch never reached provision"
    context = records[0].context
    assert context is not None, "no managed-sandbox context reached the launch"
    assert context.session_id == session["id"]
    assert context.user_id == _ALICE


async def test_context_carries_request_bound_identity_token_provider(env: _Env) -> None:
    """The context exposes the auth provider's bound identity-token provider."""
    stub = _StubIdentityTokenProvider()
    env.token_providers[_ALICE] = stub
    before = set(_managed_launch_tasks)
    agent = await create_test_agent(env.client, name="ctx-tp-agent", user=_ALICE)
    await _create_managed_session(env, agent["id"], _ALICE)

    await _await_settled(before)

    records = _records_of(env, "provision-before")
    assert records, "background launch never reached provision"
    context = records[0].context
    assert context is not None, "no managed-sandbox context reached the launch"
    assert context.identity_token_provider is stub


async def test_context_survives_create_request_return(env: _Env, monkeypatch) -> None:
    """Provisioning held mid-flight still sees its context after the POST returns."""
    gate = threading.Event()
    # Replace the launcher shim with one holding the provision gate.
    env.records.clear()
    _install_recording_modal_launcher(monkeypatch, env.records, env.lock, provision_gate=gate)

    before = set(_managed_launch_tasks)
    agent = await create_test_agent(env.client, name="ctx-gate-agent", user=_ALICE)
    session = await _create_managed_session(env, agent["id"], _ALICE)

    # The create request has returned; give the background task time to
    # reach (and block inside) provision, then keep waiting a beat.
    for _ in range(20):
        if _records_of(env, "provision-before"):
            break
        await asyncio.sleep(0.05)
    assert _records_of(env, "provision-before"), "provision never started"

    gate.set()
    await _await_settled(before)

    # The context was still bound when provision resumed — after the
    # request task that scheduled the launch had long returned.
    after_records = _records_of(env, "provision-after")
    assert after_records, "provision never resumed past the gate"
    context = after_records[0].context
    assert context is not None, "context vanished before provisioning resumed"
    assert context.session_id == session["id"]
    assert context.user_id == _ALICE


async def test_launcher_factory_and_provision_share_context(env: _Env) -> None:
    """Launcher construction and provision observe the same context object."""
    before = set(_managed_launch_tasks)
    agent = await create_test_agent(env.client, name="ctx-factory-agent", user=_ALICE)
    session = await _create_managed_session(env, agent["id"], _ALICE)

    await _await_settled(before)

    ctor_records = _records_of(env, "constructor")
    assert ctor_records, "launcher factory never constructed a launcher"
    ctor_context = ctor_records[0].context
    assert ctor_context is not None, "no context at launcher construction"

    provision_records = _records_of(env, "provision-before")
    assert provision_records, "background launch never reached provision"
    provision_context = provision_records[0].context
    assert provision_context is not None, "no context at provision time"
    assert provision_context is ctor_context, "factory and provision saw different contexts"
    assert provision_context.session_id == session["id"]


async def test_request_task_context_is_none_after_scheduling(env: _Env) -> None:
    """The request task does not retain the context after create returns."""
    before = set(_managed_launch_tasks)
    agent = await create_test_agent(env.client, name="ctx-req-agent", user=_ALICE)
    session = await _create_managed_session(env, agent["id"], _ALICE)

    # The scheduling happened inside this task's await; the scope must
    # have been closed when the route returned.
    assert current_managed_sandbox_context() is None, (
        "create route leaked the managed-sandbox context into the request task"
    )

    await _await_settled(before)

    # The background launch DID carry the context (the point of the scope).
    records = _records_of(env, "provision-before")
    assert records, "background launch never reached provision"
    context = records[0].context
    assert context is not None, "no managed-sandbox context reached the launch"
    assert context.session_id == session["id"]
    # And the request task still does not see it.
    assert current_managed_sandbox_context() is None


async def test_concurrent_user_launches_are_isolated(env: _Env, monkeypatch) -> None:
    """Two in-flight launches for two owners never observe each other's context."""
    gate = threading.Event()
    env.records.clear()
    _install_recording_modal_launcher(monkeypatch, env.records, env.lock, provision_gate=gate)

    before = set(_managed_launch_tasks)
    alice_agent = await create_test_agent(env.client, name="ctx-iso-alice", user=_ALICE)
    bob_agent = await create_test_agent(env.client, name="ctx-iso-bob", user=_BOB)
    alice_session = await _create_managed_session(env, alice_agent["id"], _ALICE)
    bob_session = await _create_managed_session(env, bob_agent["id"], _BOB)

    # Both provisions are held at the gate with their contexts captured.
    deadline = asyncio.get_running_loop().time() + _SETTLE_TIMEOUT_S
    while asyncio.get_running_loop().time() < deadline:
        if len(_records_of(env, "provision-before")) >= 2:
            break
        await asyncio.sleep(0.05)
    assert len(_records_of(env, "provision-before")) >= 2, "both launches never reached provision"

    gate.set()
    await _await_settled(before)

    by_session: dict[str, ManagedSandboxContext] = {}
    for record in _records_of(env, "provision-before"):
        assert record.context is not None, "no context for one of the launches"
        by_session[record.context.session_id] = record.context

    assert set(by_session) == {alice_session["id"], bob_session["id"]}, (
        "launches observed unexpected sessions"
    )
    assert by_session[alice_session["id"]].user_id == _ALICE
    assert by_session[bob_session["id"]].user_id == _BOB
    assert by_session[alice_session["id"]] is not by_session[bob_session["id"]], (
        "the two launches shared one context object"
    )
