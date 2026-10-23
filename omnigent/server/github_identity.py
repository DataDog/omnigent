"""Resolve a user's GitHub access token for the credential broker.

Bridges the connection store and the GitHub App client: reads the stored
connection and transparently refreshes an expired access token, so the broker
always vends a currently-valid token. See ``designs/CREDENTIAL_STORE.md``.
"""

from __future__ import annotations

from omnigent.connections.github import GithubConnectionStore
from omnigent.entities import ProviderConnection
from omnigent.server.github_app_client import GitHubAppClient

# The username git uses with a token over HTTPS (``https://<user>:<token>@…``);
# GitHub ignores the value but requires a non-empty one.
_GIT_TOKEN_USERNAME = "x-access-token"


async def resolve_access_token(
    user_id: str,
    *,
    store: GithubConnectionStore,
    client: GitHubAppClient,
) -> str | None:
    """Resolve a cached grant, publishing refreshes only after synchronous persistence."""
    connection = await _resolve_connection(user_id, store=store, client=client)
    return connection.secret.get("access_token") if connection and connection.secret else None


async def _resolve_connection(
    user_id: str, *, store: GithubConnectionStore, client: GitHubAppClient
) -> ProviderConnection | None:
    async def refresh(connection):
        tokens = await client.refresh_token(connection.secret["refresh_token"])
        metadata = dict(connection.metadata)
        metadata["token_expires_at"] = tokens.expires_at
        if tokens.refresh_token:
            metadata["refresh_token_expires_at"] = tokens.refresh_token_expires_at
        if tokens.scopes:
            metadata["scopes"] = tokens.scopes
        return store._secret(tokens), metadata

    return await store._store.cache.resolve(user_id, "github", refresh)


async def resolve_github_credential(
    user_id: str,
    *,
    store: GithubConnectionStore,
    client: GitHubAppClient,
) -> dict[str, object] | None:
    """Resolve the GitHub broker payload for *user_id*, or ``None``.

    The provider adapter the generic credential broker
    (:mod:`omnigent.server.routes.host_credentials`) calls: returns the vended
    token plus the attribution metadata git needs (``username``/``login``), or
    ``None`` when the owner has not linked GitHub. The ``owner``/``login`` let
    the host attribute commits to the human, decoupled from the push credential.
    """
    connection = await _resolve_connection(user_id, store=store, client=client)
    if connection is None or not connection.secret or not connection.secret.get("access_token"):
        return None
    return {
        "username": _GIT_TOKEN_USERNAME,
        "token": connection.secret["access_token"],
        "login": connection.metadata.get("github_login"),
    }
