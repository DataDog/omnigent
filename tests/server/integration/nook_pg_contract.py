"""Nook adapter wire/SQL tests; db_uri must target a disposable PostgreSQL DB.

Uses real installed app/backend/native client and synthetic TLS Nook + issuer.
The local Nook fixture models deterministic responses, not server semantics.
Run with OMNIGENT_TEST_DB_URI pointing to a task-owned disposable DB.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
import sysconfig
from pathlib import Path

import pytest
from sqlalchemy import text


@pytest.fixture(scope="session", autouse=True)
def require_installed_artifact_origins():
    """The integration lane must exercise installed candidate bytes, not source."""
    site = Path(sysconfig.get_path("purelib")).resolve()
    assert site.is_relative_to(Path(sys.prefix).resolve())
    for name in (
        "omnigent",
        "omnigent_hab_launcher",
        "libs.py.nook",
        "dd_internal_authentication",
    ):
        module = importlib.import_module(name)
        origin = Path(module.__file__).resolve()
        assert origin.is_relative_to(site), (
            f"{name} loaded outside installed site-packages: {origin}"
        )


@pytest.fixture
def local_services(monkeypatch):
    # The installed auth package caches managers per audience. Each test owns a
    # fresh synthetic issuer, so do not reuse another test's token manager.
    from dd_internal_authentication.client import JWTInternalServiceAuthClientTokenManager

    monkeypatch.syspath_prepend(os.environ["OMNIGENT_NOOK_TEST_FIXTURE_DIR"])
    issuer_type = importlib.import_module("fake_issuer").FakeIssuer
    nook_type = importlib.import_module("local_nook_fixture").LocalNook
    monkeypatch.setattr(JWTInternalServiceAuthClientTokenManager, "_instances", {})
    with (
        issuer_type().running() as issuer,
        nook_type(
            Path(os.environ["OMNIGENT_NOOK_TEST_CERT"]), Path(os.environ["OMNIGENT_NOOK_TEST_KEY"])
        ).running() as nook,
    ):
        monkeypatch.setenv("VAULT_ADDR", issuer.vault_addr)
        monkeypatch.setenv("OMNIGENT_NOOK_ADDR", nook.address)
        monkeypatch.setenv("OMNIGENT_NOOK_CA_BUNDLE", os.environ["OMNIGENT_NOOK_TEST_CERT"])
        monkeypatch.setenv("OMNIGENT_NOOK_PATH_PREFIX", "sandbox/omnigent/test/release")
        monkeypatch.setenv("VAULT_TOKEN", "synthetic-ignored-token")
        yield issuer, nook


def test_nook_roundtrip_metadata_and_native_wire(db_uri, local_services):
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.db.utils import get_or_create_engine

    issuer, nook = local_services
    backend = build(storage_location=db_uri)
    assert nook.requests == [] and issuer.requests == []
    secret = {
        "access_token": "synthetic-access",
        "refresh_token": "synthetic-refresh",
        "extra": {"pkce": "synthetic-verifier"},
    }
    backend.upsert("alice", "github", secret=secret, metadata={"label": "synthetic"})
    assert [
        (request["method"], request["path"].split("?")[0].split("/")[3])
        for request in nook.requests
    ] == [("PUT", "metadata"), ("PUT", "data")]
    metadata, write = nook.requests
    assert metadata["body"] == {
        "delete_version_after": "-0h0m1s",
        "cas_required": True,
        "max_versions": 1,
    }
    assert write["body"]["options"] == {"cas": 0}
    assert issuer.requests[0]["path"] == "/v1/identity/oidc/token/nook"
    for request in nook.requests:
        assert request["authorization"].startswith("Bearer ")
        token = request["authorization"].removeprefix("Bearer ")
        assert token != "synthetic-ignored-token"
        issuer.verify(token, "nook")
    with get_or_create_engine(db_uri).connect() as connection:
        reference = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='alice' AND provider='github'")
        ).scalar_one()
    assert reference.startswith("onook:v1:")
    assert all(
        value not in reference
        for value in ("synthetic-access", "synthetic-refresh", "synthetic-verifier")
    )
    counts = (len(nook.requests), len(issuer.requests))
    assert backend.get("alice", "github").secret is None
    assert len(backend.list_for_user("alice")) == 1
    assert len(backend.list_all()) == 1
    assert counts == (len(nook.requests), len(issuer.requests))
    assert backend.get("alice", "github", with_secret=True).secret == secret
    assert nook.requests[-1]["path"].endswith("?version=1")


def test_nook_nonfinite_data_fails_before_network(db_uri, local_services):
    from omnigent_hab_launcher.nook_store.backend import build

    issuer, nook = local_services
    backend = build(storage_location=db_uri)
    with pytest.raises((ValueError, TypeError)):
        backend.upsert("alice", "github", secret={"value": float("nan")}, metadata={})
    assert nook.requests == [] and issuer.requests == []


def test_nook_definite_precommit_failure_destroys_new_key(db_uri, local_services, monkeypatch):
    from omnigent_hab_launcher.nook_store import backend as backend_module

    from omnigent.db.utils import get_or_create_engine

    _issuer, nook = local_services
    backend = backend_module.build(storage_location=db_uri)
    original_authority = backend_module.require_active_account
    calls = 0

    def fail_inside_transaction(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("synthetic definite precommit failure")
        return original_authority(*args, **kwargs)

    monkeypatch.setattr(backend_module, "require_active_account", fail_inside_transaction)
    with pytest.raises(ValueError, match="synthetic definite precommit failure"):
        backend.upsert("synthetic-user", "github", secret={"token": "synthetic"}, metadata={})
    with get_or_create_engine(db_uri).connect() as connection:
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM connections WHERE user_id='synthetic-user'")
            ).scalar_one()
            == 0
        )
    assert nook.records == {}, "definite rollback left the newly created remote key"
    assert any(
        request["method"] == "DELETE" and "/metadata/" in request["path"]
        for request in nook.requests
    )


@pytest.mark.parametrize("outcome", ["rollback_ack_lost", "commit_ack_lost"])
def test_nook_unknown_commit_retains_both_keys_and_reports_failure(
    db_uri, local_services, monkeypatch, outcome
):
    from omnigent_hab_launcher.nook_store import backend as backend_module

    from omnigent.db.utils import get_or_create_engine

    _issuer, nook = local_services
    backend = backend_module.build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "old-synthetic"}, metadata={})
    engine = get_or_create_engine(db_uri)
    with engine.connect() as connection:
        old_ref = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
        ).scalar_one()
    old_keys = set(nook.records)
    original = backend_module.run_write_transaction

    def lost_ack(maker, name, operation):
        if outcome == "rollback_ack_lost":

            def abort(session):
                operation(session)
                raise RuntimeError("synthetic uncertain commit")

            return original(maker, name, abort)
        original(maker, name, operation)
        raise RuntimeError("synthetic uncertain commit")

    monkeypatch.setattr(backend_module, "run_write_transaction", lost_ack)
    with pytest.raises(RuntimeError, match="synthetic uncertain commit"):
        backend.upsert("synthetic-user", "github", secret={"token": "new-synthetic"}, metadata={})
    with engine.connect() as connection:
        current_ref = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
        ).scalar_one()
    assert (current_ref == old_ref) == (outcome == "rollback_ack_lost")
    assert old_keys < set(nook.records) and len(nook.records) == 2
    assert not any(request["method"] == "DELETE" for request in nook.requests)


def test_nook_committed_replacement_retires_only_old_key(db_uri, local_services):
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.db.utils import get_or_create_engine

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "old-synthetic"}, metadata={})
    old_keys = set(nook.records)
    assert len(old_keys) == 1
    backend.upsert("synthetic-user", "github", secret={"token": "new-synthetic"}, metadata={})
    with get_or_create_engine(db_uri).connect() as connection:
        current_ref = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
        ).scalar_one()
    assert len(nook.records) == 1 and not (old_keys & set(nook.records))
    new_key = next(iter(nook.records))
    assert current_ref == f"onook:v1:{new_key}:1"
    deletes = [request["path"] for request in nook.requests if request["method"] == "DELETE"]
    assert deletes == [f"/v1/nook/metadata/{next(iter(old_keys))}"]
    assert backend.get("synthetic-user", "github", with_secret=True).secret == {
        "token": "new-synthetic"
    }


def test_admin_delete_survives_native_cleanup_failure(
    db_uri, local_services, tmp_path, monkeypatch
):
    """Real local accounts route and Nook backend; every identity is synthetic."""
    from fastapi.testclient import TestClient
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.server.accounts_store import SqlAlchemyAccountStore
    from tests.server import test_accounts as account_tests

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    monkeypatch.setattr(
        account_tests,
        "SqlAlchemyAccountStore",
        lambda uri: SqlAlchemyAccountStore(uri, retire_deleted=backend.retire_deleted),
    )
    app_context = account_tests._build_accounts_app(
        tmp_path, monkeypatch, init_admin_password="synthetic-admin-pass", db_url_override=db_uri
    )
    client = next(app_context)
    try:
        admin = account_tests._login(client, "admin", "synthetic-admin-pass")
        invitation = admin.post("/auth/invite", json={}).json()["token"]
        synthetic = TestClient(client.app)
        registered = synthetic.post(
            "/auth/register",
            json={
                "invite": invitation,
                "username": "synthetic-user",
                "password": "synthetic-password",
            },
        )
        assert registered.status_code == 200
        backend.upsert("synthetic-user", "github", secret={"token": "synthetic-one"}, metadata={})
        backend.upsert(
            "synthetic-user", "mcp-pending:fixture", secret={"nonce": "synthetic-two"}, metadata={}
        )
        old_keys = set(nook.records)
        assert len(old_keys) == 2
        nook.faults.append(("DELETE", "/metadata/", 503, {"errors": ["synthetic outage"]}))
        response = admin.delete("/auth/users/synthetic-user")
        assert response.status_code == 204
        assert backend.get("synthetic-user", "github") is None
        assert backend.get("synthetic-user", "mcp-pending:fixture") is None
        assert len(nook.records) == 1 and set(nook.records) < old_keys
    finally:
        app_context.close()


def test_nook_read_retries_once_when_authoritative_reference_changes(
    db_uri, local_services, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from libs.py.nook import Nook
    from omnigent_hab_launcher.nook_store.backend import build

    _issuer, _nook = local_services
    backend = build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "old-synthetic"}, metadata={})
    read_complete, release = Event(), Event()
    original_get = Nook.get_version
    calls = []

    def paused_get(client, path, version):
        result = original_get(client, path, version)
        calls.append(path)
        if len(calls) == 1:
            read_complete.set()
            assert release.wait(10)
        return result

    monkeypatch.setattr(Nook, "get_version", paused_get)
    with ThreadPoolExecutor(max_workers=1) as executor:
        read = executor.submit(backend.get, "synthetic-user", "github", with_secret=True)
        try:
            assert read_complete.wait(10)
            backend.upsert(
                "synthetic-user", "github", secret={"token": "new-synthetic"}, metadata={}
            )
        finally:
            release.set()
        assert read.result(timeout=10).secret == {"token": "new-synthetic"}
    assert len(calls) == 2 and calls[0] != calls[1]


def test_nook_references_are_isolated_before_remote_reads(db_uri, local_services):
    from omnigent_hab_launcher.nook_store.backend import build
    from sqlalchemy.orm import Session

    from omnigent.db.db_models import SqlConnection, workspace_scope
    from omnigent.db.utils import get_or_create_engine
    from omnigent.stores.credential_store.backend import CredentialBackendUnavailable

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    cases = [
        (1, "synthetic-a", "github", ""),
        (1, "synthetic-b", "github", ""),
        (1, "synthetic-a", "databricks", ""),
        (1, "synthetic-a", "github", "other"),
        (2, "synthetic-a", "github", ""),
    ]
    for index, (workspace, user, provider, account) in enumerate(cases):
        with workspace_scope(workspace):
            backend.upsert(
                user, provider, account_id=account, secret={"value": index}, metadata={}
            )
    for index, (workspace, user, provider, account) in enumerate(cases):
        with workspace_scope(workspace):
            assert backend.get(user, provider, account_id=account, with_secret=True).secret == {
                "value": index
            }
    with Session(get_or_create_engine(db_uri)) as session, session.begin():
        source = session.get(SqlConnection, cases[0])
        target = session.get(SqlConnection, cases[1])
        assert source.secret_enc != target.secret_enc
        target.secret_enc = source.secret_enc
    before = len(nook.requests)
    with workspace_scope(1), pytest.raises(CredentialBackendUnavailable):
        backend.get("synthetic-b", "github", with_secret=True)
    assert len(nook.requests) == before


def test_admin_delete_delayed_native_cleanup_preserves_new_generation_refresh(
    db_uri, local_services, tmp_path, monkeypatch
):
    """Local HTTP route + PG + native TLS, synthetic identity and provider only."""
    import asyncio
    import time
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from fastapi.testclient import TestClient
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.connections.github import GithubConnectionStore
    from omnigent.db.account_authority import account_authority_scope
    from omnigent.server.accounts_store import SqlAlchemyAccountStore
    from omnigent.server.github_app import GitHubTokenSet
    from omnigent.server.github_identity import resolve_access_token
    from tests.server import test_accounts as account_tests

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    entered, release = Event(), Event()
    refresh_entered, refresh_release = Event(), Event()
    holder, snapshots = {}, []

    def paused_retirement(**snapshot):
        snapshots.append(snapshot)
        entered.set()
        assert release.wait(15)
        backend.retire_deleted(**snapshot)

    def make_store(uri):
        store = SqlAlchemyAccountStore(uri, retire_deleted=paused_retirement)
        holder["accounts"] = store
        return store

    monkeypatch.setattr(account_tests, "SqlAlchemyAccountStore", make_store)
    app_context = account_tests._build_accounts_app(
        tmp_path, monkeypatch, init_admin_password="synthetic-admin-pass", db_url_override=db_uri
    )
    client = next(app_context)
    try:
        admin = account_tests._login(client, "admin", "synthetic-admin-pass")

        def register():
            invitation = admin.post("/auth/invite", json={}).json()["token"]
            user = TestClient(client.app)
            response = user.post(
                "/auth/register",
                json={
                    "invite": invitation,
                    "username": "synthetic-user",
                    "password": "synthetic-password",
                },
            )
            assert response.status_code == 200

        register()
        old_generation = holder["accounts"].get_user("synthetic-user").account_generation
        backend.upsert(
            "synthetic-user", "github", secret={"access_token": "synthetic-old"}, metadata={}
        )
        old_keys = set(nook.records)
        with ThreadPoolExecutor(max_workers=2) as executor:
            deleting = executor.submit(admin.delete, "/auth/users/synthetic-user")
            try:
                assert entered.wait(15)
                register()
                generation = holder["accounts"].get_user("synthetic-user").account_generation
                assert generation != old_generation
                now = int(time.time())
                backend.upsert(
                    "synthetic-user",
                    "github",
                    secret={"access_token": "synthetic-new", "refresh_token": "synthetic-refresh"},
                    metadata={
                        "github_login": "synthetic",
                        "issued_at": now - 100,
                        "token_expires_at": now + 10,
                    },
                )

                async def paused_refresh(*_):
                    refresh_entered.set()
                    assert await asyncio.to_thread(refresh_release.wait, 15)
                    return GitHubTokenSet(
                        "synthetic-rotated", "synthetic-next", now + 3600, None, "repo"
                    )

                client_fixture = SimpleNamespace(
                    refresh_token=AsyncMock(side_effect=paused_refresh)
                )
                store = GithubConnectionStore(backend=backend)

                def resolve_new():
                    with account_authority_scope("synthetic-user", generation):
                        return asyncio.run(
                            resolve_access_token(
                                "synthetic-user", store=store, client=client_fixture
                            )
                        )

                refreshing = executor.submit(resolve_new)
                assert refresh_entered.wait(15)
                release.set()
                assert deleting.result(timeout=15).status_code == 204
                refresh_release.set()
                assert refreshing.result(timeout=15) == "synthetic-rotated"
                client_fixture.refresh_token.assert_awaited_once()
            finally:
                release.set()
                refresh_release.set()
        assert len(snapshots) == 1
        assert not (old_keys & set(nook.records))
        with account_authority_scope("synthetic-user", generation):
            assert (
                backend.get("synthetic-user", "github", with_secret=True).secret["access_token"]
                == "synthetic-rotated"
            )
    finally:
        app_context.close()


def test_nook_server_reported_commit_rollback_retires_new_key(db_uri, local_services, request):
    """A deferred PostgreSQL trigger fails actual COMMIT with SQLSTATE 40001."""
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.db.utils import get_or_create_engine

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "old-synthetic"}, metadata={})
    engine = get_or_create_engine(db_uri)
    with engine.begin() as connection:
        old_ref = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
        ).scalar_one()
        connection.execute(
            text("""
            CREATE FUNCTION synthetic_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              RAISE EXCEPTION 'synthetic serialization rollback' USING ERRCODE = '40001';
            END $$
        """)
        )
        connection.execute(
            text("""
            CREATE CONSTRAINT TRIGGER synthetic_fail_commit_trigger
            AFTER UPDATE ON connections DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION synthetic_fail_commit()
        """)
        )

    def drop_trigger():
        with engine.begin() as connection:
            connection.execute(
                text("DROP TRIGGER IF EXISTS synthetic_fail_commit_trigger ON connections")
            )
            connection.execute(text("DROP FUNCTION IF EXISTS synthetic_fail_commit()"))

    request.addfinalizer(drop_trigger)
    old_keys = set(nook.records)
    with pytest.raises(Exception) as failure:
        backend.upsert("synthetic-user", "github", secret={"token": "new-synthetic"}, metadata={})
    observed = failure.value
    while (
        observed is not None
        and getattr(getattr(observed, "orig", None), "sqlstate", None) != "40001"
    ):
        observed = observed.__cause__ or observed.__context__
    assert observed is not None, "expected PostgreSQL SQLSTATE 40001 at COMMIT"
    with engine.connect() as connection:
        current_ref = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
        ).scalar_one()
    assert current_ref == old_ref
    assert set(nook.records) == old_keys
    assert backend.get("synthetic-user", "github", with_secret=True).secret == {
        "token": "old-synthetic"
    }


def test_nook_concurrency_capacity_and_bounded_saturation(db_uri, local_services, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Lock

    from libs.py.nook import Nook
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.stores.credential_store.backend import CredentialBackendUnavailable

    monkeypatch.setenv("OMNIGENT_NOOK_MAX_CONCURRENCY", "2")
    monkeypatch.setenv("OMNIGENT_NOOK_REQUEST_TIMEOUT_SECONDS", "2")
    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    for user in ("synthetic-one", "synthetic-two", "synthetic-three"):
        backend.upsert(user, "github", secret={"token": user}, metadata={})
    both_entered, release, guard = Event(), Event(), Lock()
    active, clients = [], []
    original = Nook.get_version

    def observe_client(client, path, version):
        with guard:
            clients.append(client)
        return original(client, path, version)

    def gate_wire(method, path):
        if method != "GET" or "/data/" not in path:
            return
        with guard:
            active.append(path)
            if len(active) == 2:
                both_entered.set()
        assert release.wait(10)

    monkeypatch.setattr(Nook, "get_version", observe_client)
    nook.before_request = gate_wire
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(backend.get, "synthetic-one", "github", with_secret=True)
        second = executor.submit(backend.get, "synthetic-two", "github", with_secret=True)
        try:
            assert both_entered.wait(5), "two TLS requests did not reach the fixture concurrently"
        finally:
            release.set()
        assert first.result(timeout=10).secret == {"token": "synthetic-one"}
        assert second.result(timeout=10).secret == {"token": "synthetic-two"}
    assert len(clients) == 2 and clients[0]._session is not clients[1]._session

    # Gate before requests starts each HTTP timer. The third caller waits for a
    # permit and fails, while the two admitted requests remain viable.
    nook.before_request = None
    both_entered, release = Event(), Event()
    clients.clear()

    def gate_client(client, path, version):
        with guard:
            clients.append(client)
            if len(clients) == 2:
                both_entered.set()
        assert release.wait(10)
        return original(client, path, version)

    monkeypatch.setattr(Nook, "get_version", gate_client)
    with ThreadPoolExecutor(max_workers=3) as executor:
        first = executor.submit(backend.get, "synthetic-one", "github", with_secret=True)
        second = executor.submit(backend.get, "synthetic-two", "github", with_secret=True)
        try:
            assert both_entered.wait(5)
            third = executor.submit(backend.get, "synthetic-three", "github", with_secret=True)
            with pytest.raises(CredentialBackendUnavailable):
                third.result(timeout=5)
        finally:
            release.set()
        assert first.result(timeout=10).secret == {"token": "synthetic-one"}
        assert second.result(timeout=10).secret == {"token": "synthetic-two"}
    assert len(clients) == 2 and clients[0]._session is not clients[1]._session


def test_nook_rechecks_malformed_reference_if_row_changes(db_uri, local_services, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from omnigent_hab_launcher.nook_store.backend import build
    from sqlalchemy.orm import Session

    from omnigent.db.db_models import SqlConnection, current_workspace_id
    from omnigent.db.utils import get_or_create_engine

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "synthetic-valid"}, metadata={})
    engine = get_or_create_engine(db_uri)
    key = (current_workspace_id(), "synthetic-user", "github", "")
    with Session(engine) as session, session.begin():
        row = session.get(SqlConnection, key)
        valid_ref = row.secret_enc
        row.secret_enc = "malformed-synthetic-reference"
    captured, release = Event(), Event()
    original = backend._reference_path

    def pause_before_validation(identity, reference):
        if reference == "malformed-synthetic-reference":
            captured.set()
            assert release.wait(10)
        return original(identity, reference)

    monkeypatch.setattr(backend, "_reference_path", pause_before_validation)
    before = len(nook.requests)
    with ThreadPoolExecutor(max_workers=1) as executor:
        reading = executor.submit(backend.get, "synthetic-user", "github", with_secret=True)
        try:
            assert captured.wait(10)
            with Session(engine) as session, session.begin():
                session.get(SqlConnection, key).secret_enc = valid_ref
        finally:
            release.set()
        assert reading.result(timeout=10).secret == {"token": "synthetic-valid"}
    reads = [request for request in nook.requests[before:] if request["method"] == "GET"]
    assert len(reads) == 1 and "malformed-synthetic-reference" not in reads[0]["path"]


def test_nook_deferred_fk_commit_rejection_retires_new_key(db_uri, local_services, request):
    """A PostgreSQL deferred FK produces definite 23503 at actual COMMIT."""
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.db.utils import get_or_create_engine

    _issuer, nook = local_services
    backend = build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "old-synthetic"}, metadata={})
    engine = get_or_create_engine(db_uri)
    with engine.begin() as connection:
        old_ref = connection.execute(
            text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
        ).scalar_one()
        connection.execute(text("CREATE TABLE synthetic_valid_refs (ref TEXT PRIMARY KEY)"))
        connection.execute(
            text("INSERT INTO synthetic_valid_refs (ref) VALUES (:ref)"), {"ref": old_ref}
        )
        connection.execute(
            text(
                "ALTER TABLE connections ADD CONSTRAINT synthetic_deferred_fk "
                "FOREIGN KEY (secret_enc) REFERENCES synthetic_valid_refs(ref) "
                "DEFERRABLE INITIALLY DEFERRED"
            )
        )

    def drop_constraint():
        with engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE connections DROP CONSTRAINT IF EXISTS synthetic_deferred_fk")
            )
            connection.execute(text("DROP TABLE IF EXISTS synthetic_valid_refs"))

    request.addfinalizer(drop_constraint)
    old_keys = set(nook.records)
    with pytest.raises(Exception) as failure:
        backend.upsert("synthetic-user", "github", secret={"token": "new-synthetic"}, metadata={})
    observed = failure.value
    while (
        observed is not None
        and getattr(getattr(observed, "orig", None), "sqlstate", None) != "23503"
    ):
        observed = observed.__cause__ or observed.__context__
    assert observed is not None, "expected PostgreSQL SQLSTATE 23503 at COMMIT"
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT secret_enc FROM connections WHERE user_id='synthetic-user'")
            ).scalar_one()
            == old_ref
        )
    assert set(nook.records) == old_keys
    assert backend.get("synthetic-user", "github", with_secret=True).secret == {
        "token": "old-synthetic"
    }


def test_installed_auth_refresh_and_issuer_failure_are_sanitized(db_uri, local_services, caplog):
    """Actual installed auth manager and issuer over local synthetic fixtures."""
    from dd_internal_authentication.client import JWTInternalServiceAuthClientTokenManager
    from omnigent_hab_launcher.nook_store.backend import build

    from omnigent.stores.credential_store.backend import CredentialBackendUnavailable

    issuer, nook = local_services
    backend = build(storage_location=db_uri)
    backend.upsert("synthetic-user", "github", secret={"token": "synthetic-one"}, metadata={})
    assert len(issuer.requests) == 1
    first = nook.requests[-1]["authorization"].removeprefix("Bearer ")
    assert issuer.verify(first, "nook")["aud"] == "nook"
    manager = JWTInternalServiceAuthClientTokenManager.instance("nook")
    # Exercise the actual TTLCache expiry path deterministically without waiting.
    manager.keys["nook"].expire(time=float("inf"))
    backend.upsert("synthetic-user", "databricks", secret={"token": "synthetic-two"}, metadata={})
    assert len(issuer.requests) == 2
    second = nook.requests[-1]["authorization"].removeprefix("Bearer ")
    assert second != first and issuer.verify(second, "nook")["aud"] == "nook"
    manager.keys["nook"].expire(time=float("inf"))
    issuer.fail_next(count=10, status=503)
    prior = len(nook.requests)
    with pytest.raises(CredentialBackendUnavailable) as failure:
        backend.upsert(
            "synthetic-user", "mcp:fixture", secret={"token": "synthetic-three"}, metadata={}
        )
    assert len(nook.requests) == prior
    assert "synthetic-three" not in str(failure.value) + caplog.text
    assert first not in caplog.text and second not in caplog.text
    assert "issuer unavailable" not in caplog.text
