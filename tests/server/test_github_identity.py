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
