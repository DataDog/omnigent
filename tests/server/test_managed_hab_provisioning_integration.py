"""Cross-boundary integration test for managed Habitat provisioning (Slice 7).

Proves the full delegation chain composes end to end:

    OIDC request -> ContextVar -> background launch task -> asyncio.to_thread
      -> omnigent_hab_launcher -> renewable Ticino ID token -> OBO exchange
      -> Habitat CreateHab

Every layer is the real production implementation except the Habitat gRPC
service (a local fake servicer), the token-exchange client, and the IdP
(RSA-signed ID tokens stored in the real encrypted OIDC session store).
The test fails if any link breaks: the fake Habitat service must see the
exact exchanged OBO bearer as its CreateHab authorization.

Requires the external ``hab_launcher`` wheel source; point
``OMNIGENT_HAB_LAUNCHER_PATH`` at the directory containing the package
(a dd-source checkout). Skips cleanly when unset.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sys
import threading
import uuid
from collections.abc import Iterator
from concurrent import futures
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import ASGITransport, AsyncClient
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from omnigent.db.db_models import OmnigentBase, SqlOidcSession
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.server.auth import UnifiedAuthProvider
from omnigent.server.managed_hosts import ManagedSandboxConfig, ManagedSandboxDeployment
from omnigent.server.oidc import OIDCConfig
from omnigent.server.oidc_session_store import OidcSessionStore
from omnigent.server.routes._sessions.common import _managed_launch_tasks
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.comment_store.sqlalchemy_store import SqlAlchemyCommentStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
from omnigent.stores.host_store import HostStore
from tests.server.helpers import create_test_agent

pytestmark = pytest.mark.asyncio

_HAB_PATH_ENV = "OMNIGENT_HAB_LAUNCHER_PATH"

if not os.environ.get(_HAB_PATH_ENV):
    pytest.skip(
        "OMNIGENT_HAB_LAUNCHER_PATH is not set — point it at the directory "
        "containing the hab_launcher package (dd-source checkout)",
        allow_module_level=True,
    )

grpc = pytest.importorskip("grpc")

_TEST_KEY = bytes.fromhex("aa" * 32)
_ISSUER = "https://idp.example.com"
_CLIENT_ID = "public-client"
_ALICE = "alice@example.com"
_BOB = "bob@example.com"
_WORKLOAD_BEARER = "workload-bearer-INTEGRATION-SECRET"
_SETTLE_TIMEOUT_S = 15.0


# ---------------------------------------------------------------------------
# IdP stand-in: RSA-signed Ticino-style ID tokens
# ---------------------------------------------------------------------------


class _IdpKeys:
    """RSA keypair for signing test ID tokens."""

    def __init__(self) -> None:
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk_dict = json.loads(RSAAlgorithm.to_jwk(self.private_key.public_key()))
        jwk_dict["alg"] = "RS256"
        self.signing_key = jwt.PyJWK.from_dict(jwk_dict)


def _sign(keys: _IdpKeys, email: str, subject: str, ttl_s: int = 3600) -> str:
    """Sign a fresh ID token for *email*."""
    import time as _time

    now = int(_time.time())
    payload = {
        "iss": _ISSUER,
        "aud": _CLIENT_ID,
        "iat": now,
        "exp": now + ttl_s,
        "sub": subject,
        "email": email,
        "email_verified": True,
    }
    return jwt.encode(payload, keys.private_key, algorithm="RS256")


@pytest.fixture()
def keys() -> _IdpKeys:
    return _IdpKeys()


@pytest.fixture(autouse=True)
def _mock_jwks(monkeypatch: pytest.MonkeyPatch, keys: _IdpKeys) -> None:
    """Stub JWKS lookup so any jwt.decode uses the test key."""
    monkeypatch.setattr(
        jwt.PyJWKClient,
        "get_signing_key_from_jwt",
        lambda self, token: keys.signing_key,
    )


# ---------------------------------------------------------------------------
# External hab_launcher package (dd-source checkout)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def hab() -> Any:
    """Import the external hab_launcher package, skipping when unavailable."""
    root = os.environ.get(_HAB_PATH_ENV, "")
    if not root:
        pytest.skip(
            "OMNIGENT_HAB_LAUNCHER_PATH is not set — point it at the directory "
            "containing the hab_launcher package (dd-source checkout)"
        )
    if not (Path(root) / "hab_launcher" / "__init__.py").is_file():
        pytest.skip(f"no hab_launcher package under OMNIGENT_HAB_LAUNCHER_PATH={root!r}")
    if root not in sys.path:
        sys.path.insert(0, root)

    import hab_launcher.config as hab_config_mod
    import hab_launcher.lifecycle as hab_lifecycle_mod
    import hab_launcher.omnigent_context as hab_context_mod
    import hab_launcher.production as hab_production_mod
    import hab_launcher.registry as hab_registry_mod
    from hab_launcher.generated.hab.v1 import hab_pb2, hab_pb2_grpc

    return SimpleNamespace(
        config=hab_config_mod,
        lifecycle=hab_lifecycle_mod,
        registry=hab_registry_mod,
        context=hab_context_mod,
        production=hab_production_mod,
        pb2=hab_pb2,
        pb2_grpc=hab_pb2_grpc,
    )


# ---------------------------------------------------------------------------
# Fake Habitat gRPC service
# ---------------------------------------------------------------------------


def _fake_hab_servicer(hab: Any) -> Any:
    """Build a fake HabService recording CreateHab authorizations."""

    class _FakeHabServicer(hab.pb2_grpc.HabServiceServicer):
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._counter = 0
            self.createhab_calls: list[dict[str, Any]] = []

        def CreateHab(self, request: Any, context: Any) -> Iterator[Any]:
            metadata = dict(context.invocation_metadata())
            with self._lock:
                self._counter += 1
                call_no = self._counter
                self.createhab_calls.append(
                    {
                        "call": call_no,
                        "authorization": metadata.get("authorization"),
                        "name": request.name,
                        "image": request.image,
                    }
                )
            hab_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"omnigent-integration-hab-{call_no}"))
            yield hab.pb2.CreateHabResponse(
                hab_id=hab_id, phase="created", message="fake habitat", terminal=False
            )
            yield hab.pb2.CreateHabResponse(hab_id=hab_id, phase="created", terminal=True)

        def GetHab(self, request: Any, context: Any) -> Any:
            return hab.pb2.GetHabResponse(
                hab_id=request.hab_id,
                status=hab.pb2.HAB_STATUS_RUNNING,
                runtime="container",
                image="fake-image",
            )

        def Exec(self, request_iterator: Any, context: Any) -> Iterator[Any]:
            for _ in request_iterator:
                pass
            yield hab.pb2.ExecResponse(stdout=b"/root")
            yield hab.pb2.ExecResponse(exit=hab.pb2.ExecExit(exit_code=0))

        def DeleteHab(self, request: Any, context: Any) -> Iterator[Any]:
            yield hab.pb2.DeleteHabResponse(hab_id=request.hab_id, terminal=True)

        def ListHabs(self, request: Any, context: Any) -> Any:
            return hab.pb2.ListHabsResponse()

    return _FakeHabServicer()


# ---------------------------------------------------------------------------
# Fake exchange client
# ---------------------------------------------------------------------------


class _RecordingExchange:
    """Exchange fake recording every RFC 8693 call.

    Returns a bearer stable per subject — like the real exchange, one
    subject maps to one OBO token, so the launcher's per-build exchange
    and its credential-keyed client cache compose like production.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.calls: list[dict[str, str]] = []

    def exchange(self, *, subject_token: str, workload_bearer: str, audience: str) -> str:
        with self._lock:
            self.calls.append(
                {
                    "subject_token": subject_token,
                    "workload_bearer": workload_bearer,
                    "audience": audience,
                }
            )
        return self.bearer_for(subject_token)

    @staticmethod
    def bearer_for(subject_token: str) -> str:
        digest = hashlib.sha256(subject_token.encode()).hexdigest()[:8]
        return f"obo-INTEGRATION-{digest}"

    def by_subject(self, subject: str) -> dict[str, str]:
        matching = [call for call in self.calls if call["subject_token"] == subject]
        assert matching, f"no exchange call for subject {subject!r}"
        return matching[0]


# ---------------------------------------------------------------------------
# Harness: OIDC store + app + production launcher + fake Habitat service
# ---------------------------------------------------------------------------


def _make_oidc_config() -> OIDCConfig:
    return OIDCConfig(
        issuer=_ISSUER,
        client_id=_CLIENT_ID,
        client_secret=None,
        redirect_uri="http://localhost:8000/auth/callback",
        cookie_secret=_TEST_KEY,
        scopes="openid email profile",
        session_ttl_hours=24,
        logout_redirect_uri=None,
        allowed_domains=None,
        provider_type="oidc",
        authorization_endpoint=f"{_ISSUER}/authorize",
        token_endpoint=f"{_ISSUER}/token",
        jwks_uri=f"{_ISSUER}/jwks",
        userinfo_endpoint=None,
        allow_invites=False,
    )


class _Harness:
    """Assembled cross-boundary environment for one app instance."""

    def __init__(
        self,
        app: Any,
        oidc_config: OIDCConfig,
        store: OidcSessionStore,
        exchange: _RecordingExchange,
        hab_service: Any,
        context_records: list[dict[str, Any]],
        workload_file: Path,
    ) -> None:
        self.app = app
        self.oidc_config = oidc_config
        self.store = store
        self.exchange = exchange
        self.hab_service = hab_service
        self.context_records = context_records
        self.workload_file = workload_file

    def client_for(self, cookie_handle: str) -> AsyncClient:
        return AsyncClient(
            transport=ASGITransport(app=self.app),
            base_url="http://test",
            cookies={self.oidc_config.session_cookie_name: cookie_handle},
        )

    def login(self, keys: _IdpKeys, email: str) -> tuple[str, str]:
        """Create an OIDC credential session; return (handle, signed id token)."""
        import time as _time

        now = int(_time.time())
        id_token = _sign(keys, email, f"idp-subject-{email}")
        handle = self.store.create(
            user_id=email,
            provider_subject=f"idp-subject-{email}",
            provider_issuer=_ISSUER,
            provider_client_id=_CLIENT_ID,
            id_token=id_token,
            refresh_token=f"refresh-SECRET-{email}",
            id_token_expiry=now + 3600,
            absolute_expiry=now + 86400,
        )
        return handle, id_token


@pytest.fixture()
async def harness(
    hab: Any,
    db_uri: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[_Harness]:
    """Real app + real production hab launcher + fake Habitat gRPC service."""
    # The launch settles as failed at the online wait: the fake Hab never
    # dials back. CreateHab (and the exchange) happen during provision.
    monkeypatch.setattr("omnigent.server.managed_hosts.MANAGED_HOST_ONLINE_TIMEOUT_S", 0.2)
    monkeypatch.setattr(
        "omnigent.server.routes.sessions._HOST_RELAUNCH_RUNNER_CONNECT_TIMEOUT_S", 0.2
    )

    oidc_engine = create_engine(f"sqlite:///{tmp_path / 'oidc-sessions.db'}")
    OmnigentBase.metadata.create_all(oidc_engine, tables=[SqlOidcSession.__table__])
    oidc_engine.dispose()
    oidc_engine = create_engine(f"sqlite:///{tmp_path / 'oidc-sessions.db'}")
    session_factory = sessionmaker(bind=oidc_engine, expire_on_commit=False)
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    oidc_config = _make_oidc_config()
    auth_provider = UnifiedAuthProvider(
        source="oidc", oidc_config=oidc_config, oidc_session_store=store
    )

    hab_service = _fake_hab_servicer(hab)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    hab.pb2_grpc.add_HabServiceServicer_to_server(hab_service, grpc_server)
    port = grpc_server.add_insecure_port("localhost:0")
    grpc_server.start()

    workload_file = tmp_path / "workload-bearer"
    workload_file.write_text(f"{_WORKLOAD_BEARER}\n")

    hab_config = hab.config.HabConfig(
        enabled=True,
        apiserver=f"http://localhost:{port}",
        token_file=None,  # the service identity is deliberately absent
        image="docker.io/test/omnigent-host:latest",
        profile="default",
        runtime="container",
        public_url="https://srv.example.com",
        registry_path=str(tmp_path / "hab-registry.json"),
        cleanup_mode=hab.config.CLEANUP_DRAIN,
        max_per_user=10,
        max_service_wide=10,
        max_global=10,
        create_timeout_s=5.0,
        connect_timeout_s=1.0,
        start_timeout_s=1.0,
        max_session_lifetime_s=3600,
        max_idle_lifetime_s=3600,
        orphan_grace_period_s=60,
        allow_all_egress=False,
        workload_token_file=str(workload_file),
    )
    registry = hab.registry.HabRegistry(tmp_path / "hab-registry.json")
    lifecycle = hab.lifecycle.LifecycleManager(hab_config, registry)
    exchange = _RecordingExchange()
    context_records: list[dict[str, Any]] = []
    records_lock = threading.Lock()

    class _ContextRecordingProductionLauncher(hab.production.ProductionHabSandboxLauncher):
        """Production launcher recording the ambient context at provision."""

        def provision(self, name: str) -> str:
            context = hab.context.current_context()
            with records_lock:
                context_records.append({"name": name, "context": context})
            return super().provision(name)

    launcher = _ContextRecordingProductionLauncher(
        config=hab_config,
        registry=registry,
        lifecycle=lifecycle,
        exchange=exchange,
    )

    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    app = create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(artifact_store=artifact_store, cache_dir=tmp_path / "cache"),
        comment_store=SqlAlchemyCommentStore(db_uri),
        host_store=HostStore(db_uri),
        auth_provider=auth_provider,
        sandbox_config=ManagedSandboxDeployment.single(
            ManagedSandboxConfig(
                server_url="https://srv.example.com",
                launcher_factory=lambda: launcher,
                token_ttl_s=3600,
            )
        ),
    )

    yield _Harness(
        app=app,
        oidc_config=oidc_config,
        store=store,
        exchange=exchange,
        hab_service=hab_service,
        context_records=context_records,
        workload_file=workload_file,
    )

    grpc_server.stop(0)
    oidc_engine.dispose()


async def _create_managed_session(client: AsyncClient, agent_id: str) -> str:
    """POST a managed session; return the created conversation id."""
    resp = await client.post(
        "/v1/sessions",
        json={"agent_id": agent_id, "host_type": "managed"},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["host_id"] is None, "managed create must return before provisioning"
    return body["id"]


async def _await_settled(before: set) -> None:
    """Wait until the launches scheduled after *before* leave the task set."""
    deadline = asyncio.get_running_loop().time() + _SETTLE_TIMEOUT_S
    while asyncio.get_running_loop().time() < deadline:
        if not (set(_managed_launch_tasks) - before):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("background managed launch never settled")


def _assert_no_token_material(caplog: pytest.LogCaptureFixture, *secrets: str) -> None:
    for record in caplog.records:
        message = record.getMessage()
        for secret in secrets:
            assert secret not in message, f"token material leaked into logs: {message!r}"


# ---------------------------------------------------------------------------
# The chain, end to end
# ---------------------------------------------------------------------------


async def test_createhab_receives_exchanged_obo_bearer(
    harness: _Harness,
    keys: _IdpKeys,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The full chain: CreateHab's authorization is exactly the exchanged OBO."""
    caplog.set_level(logging.WARNING)
    handle, alice_token = harness.login(keys, _ALICE)
    async with harness.client_for(handle) as client:
        agent = await create_test_agent(client, name="hab-int-agent")
        before = set(_managed_launch_tasks)
        session_id = await _create_managed_session(client, agent["id"])
        await _await_settled(before)

    # Every exchange call carried exactly the signed-in user's ID token
    # as the RFC 8693 subject, the workload bearer supplied separately,
    # and exactly the Habitat audience.
    assert harness.exchange.calls, "the exchange never ran"
    for call in harness.exchange.calls:
        assert call["subject_token"] == alice_token
        assert call["workload_bearer"] == _WORKLOAD_BEARER
        assert call["audience"] == "hab"

    # Habitat received ONLY the exchanged OBO bearer.
    assert len(harness.hab_service.createhab_calls) == 1
    authorization = harness.hab_service.createhab_calls[0]["authorization"]
    expected_obo = _RecordingExchange.bearer_for(alice_token)
    assert authorization == f"Bearer {expected_obo}"
    assert authorization != f"Bearer {alice_token}", "raw ID token reached Habitat"
    assert _WORKLOAD_BEARER not in (authorization or ""), "workload bearer reached Habitat"

    # The launcher observed the created session and its verified owner.
    assert len(harness.context_records) == 1
    context = harness.context_records[0]["context"]
    assert context is not None, "no identity context reached the launcher"
    assert context.session_id == session_id
    assert context.user_id == _ALICE

    # No credential material leaked into logs.
    _assert_no_token_material(caplog, alice_token, _WORKLOAD_BEARER, expected_obo)


async def test_two_users_never_share_credentials(
    harness: _Harness,
    keys: _IdpKeys,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Two signed-in users provision through their own tokens, never shared."""
    caplog.set_level(logging.WARNING)
    alice_handle, alice_token = harness.login(keys, _ALICE)
    bob_handle, bob_token = harness.login(keys, _BOB)

    session_ids: dict[str, str] = {}
    before = set(_managed_launch_tasks)
    async with harness.client_for(alice_handle) as alice_client:
        alice_agent = await create_test_agent(alice_client, name="hab-int-alice")
    async with harness.client_for(bob_handle) as bob_client:
        bob_agent = await create_test_agent(bob_client, name="hab-int-bob")

    async with (
        harness.client_for(alice_handle) as alice_client,
        harness.client_for(bob_handle) as bob_client,
    ):
        session_ids[_ALICE] = await _create_managed_session(alice_client, alice_agent["id"])
        session_ids[_BOB] = await _create_managed_session(bob_client, bob_agent["id"])
    await _await_settled(before)

    # Every exchange call belongs to exactly one user's subject.
    assert harness.exchange.calls
    for call in harness.exchange.calls:
        assert call["subject_token"] in (alice_token, bob_token)
        assert call["workload_bearer"] == _WORKLOAD_BEARER
        assert call["audience"] == "hab"
    alice_call = harness.exchange.by_subject(alice_token)
    bob_call = harness.exchange.by_subject(bob_token)
    assert alice_call["workload_bearer"] == _WORKLOAD_BEARER
    assert bob_call["workload_bearer"] == _WORKLOAD_BEARER

    # Habitat saw two creates, each authorized by its own exchanged bearer.
    assert len(harness.hab_service.createhab_calls) == 2
    authorizations = {entry["authorization"] for entry in harness.hab_service.createhab_calls}
    expected = {
        f"Bearer {_RecordingExchange.bearer_for(alice_token)}",
        f"Bearer {_RecordingExchange.bearer_for(bob_token)}",
    }
    assert authorizations == expected
    assert f"Bearer {alice_token}" not in authorizations
    assert f"Bearer {bob_token}" not in authorizations

    # Each launcher observation carried its own session and owner.
    assert len(harness.context_records) == 2
    by_user = {record["context"].user_id: record["context"] for record in harness.context_records}
    assert set(by_user) == {_ALICE, _BOB}
    assert by_user[_ALICE].session_id == session_ids[_ALICE]
    assert by_user[_BOB].session_id == session_ids[_BOB]
    assert by_user[_ALICE] is not by_user[_BOB], "the two users shared one context"

    # Neither user's material appears in the other's credentials or the logs.
    _assert_no_token_material(caplog, alice_token, bob_token, _WORKLOAD_BEARER, "obo-INTEGRATION-")
