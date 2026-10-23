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
def cached(contract_backend, monkeypatch):
    backend = managed_backend(contract_backend)
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
