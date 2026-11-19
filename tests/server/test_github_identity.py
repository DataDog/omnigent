"""Delegated GitHub and Databricks token paths use the same persistence rules."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from omnigent.connections.databricks import DatabricksConnectionStore
from omnigent.connections.github import GithubConnectionStore
from omnigent.server.databricks_app import DatabricksTokenSet
from omnigent.server.databricks_identity import resolve_databricks_token
from omnigent.server.github_app import GitHubTokenSet
from omnigent.server.github_identity import resolve_access_token
from omnigent.stores.credential_store import token_cache as cache_module
from omnigent.stores.credential_store.backend import CredentialBackendUnavailable
from omnigent.stores.credential_store.token_cache import CredentialRefreshRejected

pytestmark = pytest.mark.asyncio


@pytest.fixture(params=["github", "databricks"])
def delegated(request, contract_backend, monkeypatch):
    provider = request.param
    store = (
        GithubConnectionStore(backend=contract_backend)
        if provider == "github"
        else DatabricksConnectionStore(backend=contract_backend)
    )
    backend = store._store
    clock = SimpleNamespace(now=1000)
    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    backend.upsert(
        "alice",
        provider,
        secret={"access_token": "old", "refresh_token": "keep"},
        metadata={
            "issued_at": 1000,
            "token_expires_at": 2000,
            "workspace_host": "https://workspace.test",
            "github_login": "octocat",
        },
    )
    token_type = GitHubTokenSet if provider == "github" else DatabricksTokenSet
    client = SimpleNamespace(
        refresh_token=AsyncMock(return_value=token_type("new", None, 2600, None, "repo"))
    )

    async def resolve():
        if provider == "github":
            return await resolve_access_token("alice", store=store, client=client)
        result = await resolve_databricks_token("alice", store=store, client=client)
        return result[0] if result else None

    return provider, store, backend, clock, client, resolve


async def test_delegated_refresh_single_flight_preserves_omitted_refresh(delegated):
    provider, store, backend, clock, client, resolve = delegated
    assert await resolve() == "old"
    clock.now = 1600
    assert await asyncio.gather(*(resolve() for _ in range(5))) == ["new"] * 5
    client.refresh_token.assert_awaited_once()
    assert store.get("alice", with_tokens=True).refresh_token == "keep"
    backend.delete("alice", provider)
    assert await resolve() is None


@pytest.mark.parametrize("failure", ["raise", "false"])
async def test_delegated_never_vends_failed_save(delegated, monkeypatch, failure):
    provider, _, backend, clock, client, resolve = delegated
    clock.now = 1600
    if failure == "false":

        def removed(*args, **kwargs):
            backend.cache.backend.delete("alice", provider)
            return False

        monkeypatch.setattr(backend.cache.backend, "update_secret", removed)
        assert await resolve() is None
    else:
        monkeypatch.setattr(
            backend.cache.backend,
            "update_secret",
            Mock(side_effect=CredentialBackendUnavailable("private-marker")),
        )
        assert await resolve() == "old"
        assert await resolve() == "old"
        clock.now = 2000
        with pytest.raises(CredentialBackendUnavailable):
            await resolve()
    client.refresh_token.assert_awaited_once()


async def test_delegated_refresh_disconnect_race(delegated):
    provider, _, backend, clock, client, resolve = delegated
    clock.now = 1600

    async def disconnect(*args):
        backend.delete("alice", provider)
        return GitHubTokenSet("too-late", "rotated", 2600, None, "repo")

    client.refresh_token.side_effect = disconnect
    assert await resolve() is None
    assert backend.get("alice", provider) is None


async def test_delegated_outage_and_reauth_are_distinct(delegated, monkeypatch):
    _, _, backend, clock, client, resolve = delegated
    assert await resolve() == "old"
    clock.now = 1600
    client.refresh_token.side_effect = httpx.ConnectError("private-marker")
    assert await resolve() == "old"
    backend.cache.invalidate("alice", delegated[0])
    client.refresh_token.side_effect = CredentialRefreshRejected("private-marker")
    with pytest.raises(CredentialRefreshRejected, match="reauthorize"):
        await resolve()
    backend.cache.invalidate("alice", delegated[0])
    monkeypatch.setattr(
        backend.cache.backend, "get", Mock(side_effect=CredentialBackendUnavailable())
    )
    with pytest.raises(CredentialBackendUnavailable):
        await resolve()


@pytest.mark.parametrize("status", [429, 503])
async def test_delegated_definite_refresh_failure_recovers_after_backoff(delegated, status):
    from omnigent.server.databricks_app_client import DatabricksAppClient
    from omnigent.server.github_app_client import GitHubAppClient
    from tests.server.github_app_fixtures import make_config
    from tests.server.test_databricks_app import _config

    provider, _, backend, clock, client, resolve = delegated
    calls = []

    def respond(request):
        calls.append(request)
        assert "keep" in request.content.decode()
        if len(calls) == 1:
            return httpx.Response(status, json={"error": "temporarily_unavailable"})
        return httpx.Response(200, json={"access_token": "recovered"})

    transport = httpx.MockTransport(respond)
    real_client = (
        GitHubAppClient(make_config(), transport=transport)
        if provider == "github"
        else DatabricksAppClient(_config(), transport=transport)
    )
    client.refresh_token = real_client.refresh_token
    clock.now = 1999
    assert await resolve() == "old"
    # Neither hot calls nor cache eviction can bypass the backoff.
    backend.cache._entries.clear()
    assert await resolve() == "old"
    clock.now = 2000
    with pytest.raises(cache_module.CredentialProviderUnavailable):
        await resolve()
    assert len(calls) == 1
    clock.now = 2004
    assert await resolve() == "recovered"
    assert len(calls) == 2
    assert backend.get("alice", provider, with_secret=True).secret == {
        "access_token": "recovered",
        "refresh_token": "keep",
    }
    assert backend.cache.key("alice", provider) not in backend.cache._failures


async def test_databricks_owner_margin_refreshes_warm_ordinary_cache(
    contract_backend, monkeypatch
):
    clock = SimpleNamespace(now=1000)
    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    store = DatabricksConnectionStore(backend=contract_backend)
    store._store.upsert(
        "alice",
        "databricks",
        secret={"access_token": "old", "refresh_token": "refresh"},
        metadata={
            "issued_at": 1000,
            "token_expires_at": 1600,
            "workspace_host": "https://workspace.test",
        },
    )
    client = SimpleNamespace(
        refresh_token=AsyncMock(return_value=DatabricksTokenSet("new", None, 2600, None, "repo"))
    )
    assert await resolve_databricks_token("alice", store=store, client=client) == (
        "old",
        "https://workspace.test",
    )
    assert await resolve_databricks_token(
        "alice", store=store, client=client, refresh_margin_s=900
    ) == ("new", "https://workspace.test")
    client.refresh_token.assert_awaited_once()
    assert store.get("alice", with_tokens=True).access_token == "new"


async def test_databricks_owner_margin_requires_refresh_token(contract_backend, monkeypatch):
    clock = SimpleNamespace(now=1000)
    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    store = DatabricksConnectionStore(backend=contract_backend)
    store._store.upsert(
        "alice",
        "databricks",
        secret={"access_token": "old"},
        metadata={
            "issued_at": 1000,
            "token_expires_at": 1600,
            "workspace_host": "https://workspace.test",
        },
    )
    client = SimpleNamespace(refresh_token=AsyncMock())
    assert await resolve_databricks_token("alice", store=store, client=client) == (
        "old",
        "https://workspace.test",
    )
    assert (
        await resolve_databricks_token("alice", store=store, client=client, refresh_margin_s=900)
        is None
    )
    client.refresh_token.assert_not_awaited()


async def test_same_second_secret_change_invalidates_cached_token(db_uri, monkeypatch):
    from omnigent.stores.credential_store import sqlalchemy_store as sql_module
    from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore
    from tests.server.test_credential_store import _FakeCipher

    monkeypatch.setattr(sql_module, "now_epoch", lambda: 1000)
    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: 1000))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    raw = CredentialStore(db_uri, _FakeCipher())
    store = GithubConnectionStore(backend=raw)
    raw.upsert(
        "alice", "github", secret={"access_token": "old"}, metadata={"github_login": "alice"}
    )
    client = SimpleNamespace(refresh_token=AsyncMock())
    assert await resolve_access_token("alice", store=store, client=client) == "old"
    before = raw.get("alice", "github")
    assert raw.update_secret("alice", "github", secret={"access_token": "new"})
    after = raw.get("alice", "github")
    assert (before.created_at, before.updated_at, before.metadata) == (
        after.created_at,
        after.updated_at,
        after.metadata,
    )
    assert await resolve_access_token("alice", store=store, client=client) == "new"


async def test_same_second_secret_change_invalidates_spent_grant_marker(db_uri, monkeypatch):
    from omnigent.stores.credential_store import sqlalchemy_store as sql_module
    from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore
    from tests.server.test_credential_store import _FakeCipher

    monkeypatch.setattr(sql_module, "now_epoch", lambda: 1000)
    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: 1060))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    raw = CredentialStore(db_uri, _FakeCipher())
    store = GithubConnectionStore(backend=raw)
    metadata = {"github_login": "alice", "issued_at": 1000, "token_expires_at": 1100}
    raw.upsert(
        "alice",
        "github",
        secret={"access_token": "old", "refresh_token": "spent"},
        metadata=metadata,
    )
    client = SimpleNamespace(
        refresh_token=AsyncMock(side_effect=CredentialRefreshRejected("rejected"))
    )
    with pytest.raises(CredentialRefreshRejected):
        await resolve_access_token("alice", store=store, client=client)
    assert raw.update_secret("alice", "github", secret={"access_token": "reconnected"})
    assert await resolve_access_token("alice", store=store, client=client) == "reconnected"
    client.refresh_token.assert_awaited_once()


async def test_refresh_serializes_private_and_server_event_loops(db_uri, monkeypatch):
    import threading

    from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore
    from tests.server.test_credential_store import _FakeCipher

    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: 1000))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    raw = CredentialStore(db_uri, _FakeCipher())
    store = DatabricksConnectionStore(backend=raw)
    raw.upsert(
        "alice",
        "databricks",
        secret={"access_token": "old", "refresh_token": "refresh"},
        metadata={
            "issued_at": 1000,
            "token_expires_at": 1600,
            "workspace_host": "https://workspace.test",
        },
    )
    provider_entered, release, second_waiting = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    results, errors = [], []

    async def refresh(*_):
        provider_entered.set()
        assert await asyncio.to_thread(release.wait, 10)
        return DatabricksTokenSet("new", "next", 2600, None, "repo")

    client = SimpleNamespace(refresh_token=AsyncMock(side_effect=refresh))

    def private_loop(signal_waiting=False):
        async def run():
            task = asyncio.create_task(
                resolve_databricks_token("alice", store=store, client=client, refresh_margin_s=900)
            )
            await asyncio.sleep(0)
            if signal_waiting:
                second_waiting.set()
            return await task

        try:
            results.append(asyncio.run(run()))
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=private_loop, daemon=True)
    second = threading.Thread(target=private_loop, kwargs={"signal_waiting": True}, daemon=True)
    first.start()
    try:
        assert await asyncio.to_thread(provider_entered.wait, 5)
        second.start()
        assert await asyncio.to_thread(second_waiting.wait, 5)
        server = asyncio.create_task(resolve_databricks_token("alice", store=store, client=client))
        await asyncio.sleep(0)
        release.set()
        server_result = await asyncio.wait_for(server, 5)
        await asyncio.to_thread(first.join, 5)
        await asyncio.to_thread(second.join, 5)
        assert not first.is_alive() and not second.is_alive(), "private loop waiter hung"
        assert not errors
        assert server_result == ("new", "https://workspace.test")
        assert results == [("new", "https://workspace.test")] * 2
        client.refresh_token.assert_awaited_once()
        assert raw.get("alice", "databricks", with_secret=True).secret["access_token"] == "new"
    finally:
        release.set()


async def test_cross_loop_cancellation_preserves_refresh_and_workspace_context(
    db_uri, monkeypatch
):
    import threading

    from omnigent.db.db_models import workspace_scope
    from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore
    from tests.server.test_credential_store import _FakeCipher

    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: 1000))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    raw = CredentialStore(db_uri, _FakeCipher())
    store = DatabricksConnectionStore(backend=raw)
    for workspace in (1, 2):
        with workspace_scope(workspace):
            raw.upsert(
                "alice",
                "databricks",
                secret={
                    "access_token": f"old-{workspace}",
                    "refresh_token": f"refresh-{workspace}",
                },
                metadata={
                    "issued_at": 1000,
                    "token_expires_at": 1600,
                    "workspace_host": f"https://ws{workspace}.test",
                },
            )
    entered, release, waiter_started = threading.Event(), threading.Event(), threading.Event()
    state, outcomes = {}, []

    async def refresh(host, token):
        if host == "https://ws1.test":
            assert token == "refresh-1"
            entered.set()
            assert await asyncio.to_thread(release.wait, 10)
            return DatabricksTokenSet("new-1", "next-1", 2600, None, "repo")
        assert (host, token) == ("https://ws2.test", "refresh-2")
        return DatabricksTokenSet("new-2", "next-2", 2600, None, "repo")

    client = SimpleNamespace(refresh_token=AsyncMock(side_effect=refresh))

    def owner(leader=False):
        async def run():
            with workspace_scope(1):
                task = asyncio.create_task(
                    resolve_databricks_token(
                        "alice", store=store, client=client, refresh_margin_s=900
                    )
                )
                if leader:
                    state.update(loop=asyncio.get_running_loop(), leader=task)
                else:
                    await asyncio.sleep(0)
                    waiter_started.set()
                try:
                    outcomes.append(await task)
                except asyncio.CancelledError:
                    outcomes.append("cancelled")

        asyncio.run(run())

    leader = threading.Thread(target=owner, kwargs={"leader": True}, daemon=True)
    waiter = threading.Thread(target=owner, daemon=True)
    leader.start()
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        waiter.start()
        assert await asyncio.to_thread(waiter_started.wait, 5)
        with workspace_scope(1):
            canceled_waiter = asyncio.create_task(
                resolve_databricks_token("alice", store=store, client=client)
            )
            await asyncio.sleep(0)
            canceled_waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await canceled_waiter
        state["loop"].call_soon_threadsafe(state["leader"].cancel)
        with workspace_scope(2):
            assert await asyncio.wait_for(
                resolve_databricks_token(
                    "alice", store=store, client=client, refresh_margin_s=900
                ),
                5,
            ) == ("new-2", "https://ws2.test")
        release.set()
        await asyncio.to_thread(leader.join, 5)
        await asyncio.to_thread(waiter.join, 5)
        assert not leader.is_alive() and not waiter.is_alive()
        assert sorted(outcomes, key=str) == sorted(
            ["cancelled", ("new-1", "https://ws1.test")], key=str
        )
        with workspace_scope(1):
            assert (
                raw.get("alice", "databricks", with_secret=True).secret["access_token"] == "new-1"
            )
        with workspace_scope(2):
            assert (
                raw.get("alice", "databricks", with_secret=True).secret["access_token"] == "new-2"
            )
        assert client.refresh_token.await_count == 2
    finally:
        release.set()


@pytest.mark.parametrize("retry_safe", [False, True])
async def test_databricks_owner_and_ordinary_share_failure_with_distinct_validity(
    contract_backend, monkeypatch, retry_safe
):
    clock = SimpleNamespace(now=1000)
    monkeypatch.setattr(cache_module, "time", SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(cache_module.random, "uniform", lambda *_: 0)
    store = DatabricksConnectionStore(backend=contract_backend)
    store._store.upsert(
        "alice",
        "databricks",
        secret={"access_token": "old", "refresh_token": "refresh"},
        metadata={
            "issued_at": 1000,
            "token_expires_at": 1600,
            "workspace_host": "https://workspace.test",
        },
    )

    class SyntheticFailure(Exception):
        refresh_retry_safe = retry_safe

    client = SimpleNamespace(
        refresh_token=AsyncMock(side_effect=SyntheticFailure("synthetic failure"))
    )
    assert (
        await resolve_databricks_token("alice", store=store, client=client, refresh_margin_s=900)
        is None
    )
    assert await resolve_databricks_token("alice", store=store, client=client) == (
        "old",
        "https://workspace.test",
    )
    assert (
        await resolve_databricks_token("alice", store=store, client=client, refresh_margin_s=900)
        is None
    )
    client.refresh_token.assert_awaited_once()
