"""Synchronous credential backend contract and strict entry-point selection."""

from __future__ import annotations

import importlib.metadata
import os
from typing import Any, Protocol, runtime_checkable

from omnigent.entities import ProviderConnection

ENTRY_POINT_GROUP = "omnigent.credential_backends"
BACKEND_ENV_VAR = "OMNIGENT_MCP_CREDENTIAL_BACKEND"


class CredentialBackendUnavailable(Exception):
    """Required credential storage is temporarily unavailable; details are private."""

    def __init__(self, *_args: object) -> None:
        super().__init__("Credential backend temporarily unavailable. Please try again later.")


@runtime_checkable
class CredentialBackend(Protocol):
    def get(
        self, user_id: str, provider: str, *, account_id: str = "", with_secret: bool = False
    ) -> ProviderConnection | None: ...
    def upsert(
        self,
        user_id: str,
        provider: str,
        *,
        secret: dict[str, Any],
        metadata: dict[str, Any],
        account_id: str = "",
    ) -> ProviderConnection: ...
    def update_secret(
        self,
        user_id: str,
        provider: str,
        *,
        secret: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        account_id: str = "",
    ) -> bool: ...
    def delete(self, user_id: str, provider: str, *, account_id: str = "") -> bool: ...
    def list_for_user(self, user_id: str) -> list[ProviderConnection]: ...
    def list_all(self, *, provider: str | None = None) -> list[ProviderConnection]: ...


def build_credential_backend(*, storage_location: str) -> CredentialBackend | None:
    """Build the selected backend once, retaining SQL's optional cipher behavior.

    External factories validate their own configuration and require no SQL cipher.
    Registration and construction failures are fatal and never fall back to SQL.
    """
    from omnigent.stores.credential_store.token_cache import managed_backend

    name = os.environ.get(BACKEND_ENV_VAR, "sql")
    if name == "sql":
        from omnigent.stores.credential_store.secret_cipher import build_secret_cipher
        from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore

        cipher = build_secret_cipher()
        return (
            managed_backend(
                CredentialStore(storage_location, cipher), storage_location=storage_location
            )
            if cipher
            else None
        )
    try:
        matches = [
            ep
            for ep in importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)
            if ep.name == name
        ]
    except Exception:  # noqa: BLE001 — plugin exceptions may contain secrets
        raise ValueError("Credential backend discovery failed") from None
    if len(matches) != 1:
        raise ValueError("Selected credential backend must have exactly one registration")
    try:
        factory = matches[0].load()
        backend = factory(storage_location=storage_location)
        if not isinstance(backend, CredentialBackend) or not all(
            callable(getattr(backend, method, None))
            for method in ("get", "upsert", "update_secret", "delete", "list_for_user", "list_all")
        ):
            raise TypeError
    except Exception:  # noqa: BLE001 — plugin exceptions may contain secrets
        raise ValueError(
            "Selected credential backend could not be constructed; "
            "check its installation and configuration"
        ) from None
    return managed_backend(backend, storage_location=storage_location)
