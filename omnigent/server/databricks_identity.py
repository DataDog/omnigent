"""Resolve a user's Databricks access token for the credential broker.

Bridges the connection store and the Databricks OAuth client: reads the stored
connection and transparently refreshes an expired access token, so the broker
always vends a currently-valid token together with its workspace host (needed to
build the Unity Gateway MCP URL). Mirrors :mod:`omnigent.server.github_identity`.
See ``designs/DATABRICKS_CONNECT.md``.
"""

from __future__ import annotations

from omnigent.connections.databricks import DatabricksConnectionStore
from omnigent.server.databricks_app_client import DatabricksAppClient


async def resolve_databricks_token(
    user_id: str,
    *,
    store: DatabricksConnectionStore,
    client: DatabricksAppClient,
) -> tuple[str, str] | None:
    """Resolve the workspace grant through the shared cache and persistence boundary."""

    async def refresh(connection):
        tokens = await client.refresh_token(
            connection.metadata["workspace_host"], connection.secret["refresh_token"]
        )
        metadata = dict(connection.metadata)
        metadata["token_expires_at"] = tokens.expires_at
        if tokens.refresh_token:
            metadata["refresh_token_expires_at"] = tokens.refresh_token_expires_at
        if tokens.scopes:
            metadata["scopes"] = tokens.scopes
        return store._secret(tokens), metadata

    connection = await store._store.cache.resolve(user_id, "databricks", refresh)
    if (
        not connection
        or not connection.secret
        or not connection.secret.get("access_token")
        or not connection.metadata.get("workspace_host")
    ):
        return None
    return connection.secret["access_token"], connection.metadata["workspace_host"]


async def resolve_databricks_credential(
    user_id: str,
    *,
    store: DatabricksConnectionStore,
    client: DatabricksAppClient,
) -> dict[str, object] | None:
    """Resolve the Databricks broker payload for *user_id*, or ``None``.

    The provider adapter the generic credential broker
    (:mod:`omnigent.server.routes.host_credentials`) calls: returns the vended
    (server-refreshed) access token plus the ``workspace_host`` the sandbox
    needs to target the Unity Gateway, or ``None`` when the owner has not linked
    Databricks. Mirrors
    :func:`omnigent.server.github_identity.resolve_github_credential`.
    """
    resolved = await resolve_databricks_token(user_id, store=store, client=client)
    if resolved is None:
        return None
    access_token, workspace_host = resolved
    return {"token": access_token, "workspace_host": workspace_host}
