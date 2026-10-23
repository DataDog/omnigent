"""Deterministic resolution cases shared by SQL and an installed fixture backend."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from omnigent.db.db_models import workspace_scope
from omnigent.stores.credential_store import token_cache as module
from omnigent.stores.credential_store.backend import CredentialBackendUnavailable
from omnigent.stores.credential_store.token_cache import (
    CredentialRefreshRejected,
    managed_backend,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
def cached(contract_backend, monkeypatch, db_uri):
    backend = managed_backend(contract_backend, storage_location=db_uri)
    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(module.random, "uniform", lambda *_: 0)
    backend.upsert(
        "alice",
        "mcp:tracker",
        secret={"access_token": "old", "refresh_token": "refresh"},
        metadata={"issued_at": 1000, "expires_at": 2000},
    )
    return backend, clock


def refresh_result():
    return {"access_token": "new"}, {"issued_at": 1600, "expires_at": 2600}


async def test_half_lifetime_cache_hit_and_refresh_preserves_omitted_token(cached, monkeypatch):
    backend, clock = cached
    read = Mock(wraps=backend.cache.backend.get)
    monkeypatch.setattr(backend.cache.backend, "get", read)
    refresh = AsyncMock(return_value=refresh_result())
    for now in [1000, 1200, 1499]:
        clock.now = now
        assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
            "access_token"
        ] == "old"
    assert sum(call.kwargs.get("with_secret", False) for call in read.call_args_list) == 1
    assert read.call_count == 4  # metadata on every request plus one secret read
    clock.now = 1600
    result = await backend.cache.resolve("alice", "mcp:tracker", refresh)
    assert result.secret == {"access_token": "new", "refresh_token": "refresh"}
    assert backend.get("alice", "mcp:tracker", with_secret=True).secret == result.secret
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret == result.secret
    refresh.assert_awaited_once()


@pytest.mark.parametrize("jitter", [-1, 1])
async def test_bounded_jitter(cached, monkeypatch, jitter):
    backend, _ = cached
    monkeypatch.setattr(module.random, "uniform", lambda *_: jitter)
    conn = backend.get("alice", "mcp:tracker", with_secret=True)
    assert backend.cache._entry(conn).refresh_at == 1500 + jitter * 30
    short = module.replace(conn, metadata={"issued_at": 1000, "expires_at": 1100})
    assert backend.cache._entry(short).refresh_at == 1050 + jitter * 5


async def test_non_expiring_and_bearer_only_valid_until_actual_expiry(cached):
    backend, clock = cached
    backend.upsert("alice", "bearer", secret={"access_token": "forever"}, metadata={})
    refresh = AsyncMock()
    assert (await backend.cache.resolve("alice", "bearer", refresh)).secret[
        "access_token"
    ] == "forever"
    backend.update_secret("alice", "mcp:tracker", secret={"access_token": "bearer"})
    clock.now = 1999
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "bearer"
    clock.now = 2000
    assert await backend.cache.resolve("alice", "mcp:tracker", refresh) is None
    refresh.assert_not_awaited()


async def test_user_provider_account_workspace_isolation_and_mutation_invalidation(cached):
    backend, _ = cached
    assert await backend.cache.resolve("alice", "mcp:tracker", None)
    for user, provider, account in [
        ("bob", "mcp:tracker", ""),
        ("alice", "other", ""),
        ("alice", "mcp:tracker", "other"),
    ]:
        assert await backend.cache.resolve(user, provider, None, account_id=account) is None
    with workspace_scope(2):
        assert await backend.cache.resolve("alice", "mcp:tracker", None) is None
    backend.upsert("alice", "mcp:tracker", secret={"access_token": "reconnect"}, metadata={})
    assert (await backend.cache.resolve("alice", "mcp:tracker", None)).secret[
        "access_token"
    ] == "reconnect"
    backend.update_secret("alice", "mcp:tracker", secret={"access_token": "update"})
    assert (await backend.cache.resolve("alice", "mcp:tracker", None)).secret[
        "access_token"
    ] == "update"
    backend.delete("alice", "mcp:tracker")
    assert await backend.cache.resolve("alice", "mcp:tracker", None) is None


@pytest.mark.parametrize("failure", ["unavailable", "exception", "removed"])
async def test_persistence_failure_never_publishes_new_token_or_repeats_refresh(
    cached, monkeypatch, failure
):
    backend, clock = cached
    clock.now = 1600
    refresh = AsyncMock(return_value=refresh_result())
    if failure == "removed":

        def removed(*args, **kwargs):
            backend.cache.backend.delete("alice", "mcp:tracker")
            return False

        monkeypatch.setattr(backend.cache.backend, "update_secret", removed)
        assert await backend.cache.resolve("alice", "mcp:tracker", refresh) is None
        assert await backend.cache.resolve("alice", "mcp:tracker", refresh) is None
    else:
        error = (
            CredentialBackendUnavailable("private")
            if failure == "unavailable"
            else RuntimeError("private")
        )
        monkeypatch.setattr(backend.cache.backend, "update_secret", Mock(side_effect=error))
        for _ in range(3):
            assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
                "access_token"
            ] == "old"
        clock.now = 2000
        with pytest.raises(CredentialBackendUnavailable, match="temporarily unavailable"):
            await backend.cache.resolve("alice", "mcp:tracker", refresh)
    refresh.assert_awaited_once()


async def test_refresh_completion_after_old_expiry_cannot_fallback(cached, monkeypatch):
    backend, clock = cached
    clock.now = 1600

    async def refresh(_conn):
        clock.now = 2001
        return refresh_result()

    monkeypatch.setattr(
        backend.cache.backend, "update_secret", Mock(side_effect=CredentialBackendUnavailable())
    )
    with pytest.raises(CredentialBackendUnavailable):
        await backend.cache.resolve("alice", "mcp:tracker", refresh)


@pytest.mark.parametrize("cancel_leader", [False, True])
async def test_simultaneous_refresh_and_cancellation_are_single_flight(cached, cancel_leader):
    backend, clock = cached
    clock.now = 1600
    started, release = asyncio.Event(), asyncio.Event()

    async def exchange(_conn):
        started.set()
        await release.wait()
        return refresh_result()

    refresh = AsyncMock(side_effect=exchange)
    leader = asyncio.create_task(backend.cache.resolve("alice", "mcp:tracker", refresh))
    await started.wait()
    waiter = asyncio.create_task(backend.cache.resolve("alice", "mcp:tracker", refresh))
    if cancel_leader:
        leader.cancel()
    release.set()
    if cancel_leader:
        with pytest.raises(asyncio.CancelledError):
            await leader
    else:
        assert (await leader).secret["access_token"] == "new"
    assert (await waiter).secret["access_token"] == "new"
    refresh.assert_awaited_once()


@pytest.mark.parametrize("mutation", ["delete", "reconnect"])
async def test_refresh_cannot_restore_disconnected_or_replace_reconnected_account(
    cached, mutation
):
    backend, clock = cached
    clock.now = 1600

    async def refresh(_conn):
        if mutation == "delete":
            backend.delete("alice", "mcp:tracker")
        else:
            backend.upsert(
                "alice", "mcp:tracker", secret={"access_token": "reconnected"}, metadata={}
            )
        return refresh_result()

    assert await backend.cache.resolve("alice", "mcp:tracker", refresh) is None
    saved = backend.get("alice", "mcp:tracker", with_secret=True)
    assert saved is None if mutation == "delete" else saved.secret["access_token"] == "reconnected"


async def test_metadata_outage_on_cache_hit_is_unavailability(cached, monkeypatch):
    backend, _ = cached
    await backend.cache.resolve("alice", "mcp:tracker", None)
    monkeypatch.setattr(
        backend.cache.backend, "get", Mock(side_effect=CredentialBackendUnavailable("private"))
    )
    with pytest.raises(CredentialBackendUnavailable) as failure:
        await backend.cache.resolve("alice", "mcp:tracker", None)
    assert "private" not in str(failure.value)


async def test_explicit_reauth_failure_blocks_old_cached_grant(cached):
    backend, clock = cached
    clock.now = 1600
    refresh = AsyncMock(side_effect=CredentialRefreshRejected("private-provider-message"))
    for _ in range(3):
        with pytest.raises(CredentialRefreshRejected, match="reauthorize") as failure:
            await backend.cache.resolve("alice", "mcp:tracker", refresh)
        assert "private-provider-message" not in str(failure.value)
    refresh.assert_awaited_once()
    backend.upsert("alice", "mcp:tracker", secret={"access_token": "reconnected"}, metadata={})
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "reconnected"


async def test_transient_provider_failure_keeps_only_valid_old_grant(cached):
    backend, clock = cached
    clock.now = 1600
    refresh = AsyncMock(side_effect=httpx.ConnectError("private-provider-message"))
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "old"
    clock.now = 2001
    with pytest.raises(module.CredentialProviderUnavailable, match="temporarily unavailable"):
        await backend.cache.resolve("alice", "mcp:tracker", refresh)
    refresh.assert_awaited_once()


async def test_cached_token_still_checks_service_permissions(cached):
    from omnigent.server.mcp_registry import McpRegistry, McpRegistryConfig
    from omnigent.server.routes.connections_base import ConnectionError
    from tests.server.test_mcp_registry import service

    backend, _ = cached
    entry = service(allowed_users=["alice"])
    registry = McpRegistry(McpRegistryConfig(services=[entry]), backend)
    assert await registry.token(entry, "alice", SimpleNamespace()) == "old"
    entry.allowed_users = ["bob"]
    with pytest.raises(ConnectionError, match="unavailable"):
        await registry.token(entry, "alice", SimpleNamespace())


async def test_active_account_is_checked_on_cache_hits(cached, db_uri):
    from sqlalchemy.orm import Session

    from omnigent.db.account_authority import require_active_account
    from omnigent.db.db_models import SqlUser, current_workspace_id
    from omnigent.db.utils import get_or_create_engine
    from omnigent.errors import OmnigentError

    backend, _ = cached
    engine = get_or_create_engine(db_uri)

    def authorize(user_id):
        with Session(engine) as session:
            require_active_account(session, user_id)

    backend.cache.authorize = authorize
    assert await backend.cache.resolve("alice", "mcp:tracker", None)
    with Session(engine) as session:
        session.add(SqlUser(workspace_id=current_workspace_id(), id="alice", deleted_at=1200))
        session.commit()
    with pytest.raises(OmnigentError, match="revoked"):
        await backend.cache.resolve("alice", "mcp:tracker", None)
    assert backend.cache.key("alice", "mcp:tracker") not in backend.cache._entries
    with pytest.raises(OmnigentError, match="revoked"):
        backend.upsert("alice", "other", secret={"access_token": "new"}, metadata={})


@pytest.mark.parametrize("during_refresh", [False, True])
@pytest.mark.parametrize("refresh_fails", [False, True])
async def test_transactional_account_deletion_rejects_warm_hits_and_refresh(
    cached, db_uri, during_refresh, refresh_fails
):
    from omnigent.errors import OmnigentError
    from omnigent.server.accounts_store import SqlAlchemyAccountStore

    backend, clock = cached
    accounts = SqlAlchemyAccountStore(db_uri)
    accounts.create_user_with_password("alice", "synthetic-password-hash")
    assert await backend.cache.resolve("alice", "mcp:tracker", None)
    raw = backend.cache.backend
    records = len(raw.remote.records) if hasattr(raw, "remote") else None
    clock.now = 1600

    async def refresh(_conn):
        assert await asyncio.to_thread(accounts.delete_user, "alice") is True
        if refresh_fails:
            raise httpx.ConnectError("private-provider-failure")
        return refresh_result()

    if during_refresh:
        with pytest.raises(OmnigentError, match="revoked"):
            await backend.cache.resolve("alice", "mcp:tracker", refresh)
    else:
        assert await asyncio.to_thread(accounts.delete_user, "alice") is True
    with pytest.raises(OmnigentError, match="revoked"):
        await backend.cache.resolve("alice", "mcp:tracker", None)
    assert backend.get("alice", "mcp:tracker") is None
    assert backend.cache.key("alice", "mcp:tracker") not in backend.cache._entries
    # Transactional raw SQL revocation cannot invoke remote cleanup through the
    # six-method contract after removing its reference. Orphaning is documented.
    if records is not None:
        assert len(raw.remote.records) == records


async def test_401_discards_cached_token_and_blocks_refresh_until_reconnect(cached):
    backend, clock = cached
    refresh = AsyncMock(return_value=refresh_result())
    assert await backend.cache.resolve("alice", "mcp:tracker", refresh)
    backend.cache.reject("alice", "mcp:tracker")
    clock.now = 1600
    for _ in range(2):
        with pytest.raises(CredentialRefreshRejected, match="reauthorize"):
            await backend.cache.resolve("alice", "mcp:tracker", refresh)
    refresh.assert_not_awaited()
    assert (
        backend.cache._entries[backend.cache.key("alice", "mcp:tracker")].connection.secret is None
    )
    backend.upsert("alice", "mcp:tracker", secret={"access_token": "reconnected"}, metadata={})
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "reconnected"


async def test_failed_rotation_is_not_retried_after_positive_cache_eviction(cached, monkeypatch):
    from cachetools import LRUCache

    backend, clock = cached
    backend.cache._entries = LRUCache(1)
    clock.now = 1600
    refresh = AsyncMock(return_value=refresh_result())
    monkeypatch.setattr(
        backend.cache.backend, "update_secret", Mock(side_effect=CredentialBackendUnavailable())
    )
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "old"
    backend.upsert("bob", "other", secret={"access_token": "other"}, metadata={})
    await backend.cache.resolve("bob", "other", None)
    assert backend.cache.key("alice", "mcp:tracker") not in backend.cache._entries
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "old"
    clock.now = 2001
    with pytest.raises(CredentialBackendUnavailable):
        await backend.cache.resolve("alice", "mcp:tracker", refresh)
    refresh.assert_awaited_once()
    assert all(entry.connection.secret is None for entry in backend.cache._failures.values())


async def test_cancellation_during_synchronous_save_keeps_single_flight(cached, monkeypatch):
    import threading

    backend, clock = cached
    clock.now = 1600
    started, release = threading.Event(), threading.Event()
    update = backend.cache.backend.update_secret

    def persist(*args, **kwargs):
        started.set()
        assert release.wait(10), "test did not release synchronous persistence"
        return update(*args, **kwargs)

    monkeypatch.setattr(backend.cache.backend, "update_secret", persist)
    refresh = AsyncMock(return_value=refresh_result())
    leader = asyncio.create_task(backend.cache.resolve("alice", "mcp:tracker", refresh))
    assert await asyncio.to_thread(started.wait, 5)
    leader.cancel()
    await asyncio.sleep(0)
    leader.cancel()  # repeated cancellation must not interrupt persistence coordination
    waiter = asyncio.create_task(backend.cache.resolve("alice", "mcp:tracker", refresh))
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await leader
    assert (await waiter).secret["access_token"] == "new"
    refresh.assert_awaited_once()


async def test_backend_io_for_one_user_does_not_block_another(cached, monkeypatch):
    import threading

    backend, _ = cached
    backend.upsert("bob", "other", secret={"access_token": "bob"}, metadata={})
    started, release = threading.Event(), threading.Event()
    original = backend.cache.backend.get

    def get(user_id, *args, **kwargs):
        if user_id == "alice":
            started.set()
            assert release.wait(10), "test did not release backend read"
        return original(user_id, *args, **kwargs)

    monkeypatch.setattr(backend.cache.backend, "get", get)
    alice = asyncio.create_task(backend.cache.resolve("alice", "mcp:tracker", None))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        bob = await asyncio.wait_for(backend.cache.resolve("bob", "other", None), 5)
        assert bob.secret["access_token"] == "bob"
    finally:
        release.set()
        await alice


async def test_failed_reconnect_does_not_allow_spent_refresh_retry(cached, monkeypatch):
    backend, clock = cached
    clock.now = 1600
    refresh = AsyncMock(return_value=refresh_result())
    monkeypatch.setattr(
        backend.cache.backend, "update_secret", Mock(side_effect=CredentialBackendUnavailable())
    )
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "old"
    monkeypatch.setattr(
        backend.cache.backend, "upsert", Mock(side_effect=CredentialBackendUnavailable())
    )
    with pytest.raises(CredentialBackendUnavailable):
        backend.upsert("alice", "mcp:tracker", secret={"access_token": "not-saved"}, metadata={})
    assert (await backend.cache.resolve("alice", "mcp:tracker", refresh)).secret[
        "access_token"
    ] == "old"
    refresh.assert_awaited_once()


async def test_expired_refreshed_grant_is_not_vended_or_repeated(cached):
    backend, clock = cached
    clock.now = 1600
    refresh = AsyncMock(
        return_value=({"access_token": "expired-new"}, {"issued_at": 1600, "expires_at": 1600})
    )
    for _ in range(2):
        with pytest.raises(module.CredentialProviderUnavailable, match="expired grant"):
            await backend.cache.resolve("alice", "mcp:tracker", refresh)
    refresh.assert_awaited_once()


@pytest.mark.parametrize("provider", ["github", "databricks"])
async def test_delegated_parser_distinguishes_zero_expiry_from_non_expiring(provider):
    import importlib
    import time

    app = importlib.import_module(f"omnigent.server.{provider}_app")
    before = int(time.time())
    expired = app.token_set_from_payload(
        {"access_token": "synthetic", "expires_in": 0, "refresh_token_expires_in": 0}
    )
    assert before <= expired.expires_at <= int(time.time())
    assert expired.refresh_token_expires_at == expired.expires_at
    assert app.token_set_from_payload({"access_token": "synthetic"}).expires_at is None


@pytest.mark.parametrize("provider", ["github", "databricks"])
@pytest.mark.parametrize("error", ["invalid_grant", "temporarily_unavailable"])
async def test_provider_errors_preserve_reauth_vs_temporary_failure_without_secrets(
    provider, error
):
    import importlib

    app = importlib.import_module(f"omnigent.server.{provider}_app")
    with pytest.raises(Exception) as failure:
        app.token_set_from_payload({"error": error, "error_description": "private-marker"})
    assert failure.value.reauth_required is (error == "invalid_grant")
    assert "private-marker" not in str(failure.value)
