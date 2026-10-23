"""Process-local token resolution with synchronous persistence before publication.

Metadata is checked on every request. Secret reads are cached, refresh is single
flight, and a spent refresh is never retried after uncertain persistence.
"""

from __future__ import annotations

import asyncio
import random
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Any
from weakref import WeakValueDictionary

import httpx
from cachetools import LRUCache
from opentelemetry import metrics
from sqlalchemy.exc import SQLAlchemyError

from omnigent.db.db_models import current_workspace_id
from omnigent.entities import ProviderConnection
from omnigent.errors import OmnigentError
from omnigent.server.routes.connections_base import ConnectionError
from omnigent.stores.credential_store.backend import (
    CredentialBackend,
    CredentialBackendUnavailable,
)

_counter = metrics.get_meter(__name__).create_counter("omnigent.credentials.operations")


def record(operation: str, result: str) -> None:
    _counter.add(1, {"operation": operation, "result": result})


class CredentialRefreshRejected(ConnectionError):
    """The provider explicitly requires new authorization."""


class CredentialProviderUnavailable(ConnectionError):
    """The provider could not refresh a grant temporarily."""


@dataclass(repr=False)
class _Entry:
    connection: ProviderConnection
    refresh_at: float | None
    failure: Exception | None = field(default=None, repr=False)


@dataclass
class _Generation:
    version: object = field(default_factory=object)
    readers: int = 0


class TokenCache:
    def __init__(self, backend: CredentialBackend) -> None:
        self.backend = backend
        self._entries: LRUCache[tuple[int, str, str, str], _Entry] = LRUCache(2048)
        self._locks: WeakValueDictionary[tuple[int, str, str, str], asyncio.Lock] = (
            WeakValueDictionary()
        )
        self._guard = threading.RLock()
        self._generations: dict[tuple[int, str, str, str], _Generation] = {}
        self.authorize: Callable[[str], None] | None = None

    @staticmethod
    def key(user_id: str, provider: str, account_id: str = "") -> tuple[int, str, str, str]:
        return current_workspace_id(), user_id, provider, account_id

    def invalidate(self, user_id: str, provider: str, account_id: str = "") -> None:
        with self._guard:
            key = self.key(user_id, provider, account_id)
            self._entries.pop(key, None)
            # Only retain generations while a resolver is in flight.
            if key in self._generations:
                self._generations[key].version = object()

    def begin(self, key) -> object:
        with self._guard:
            state = self._generations.setdefault(key, _Generation())
            state.readers += 1
            return state.version

    def end(self, key) -> None:
        with self._guard:
            state = self._generations[key]
            state.readers -= 1
            if not state.readers:
                del self._generations[key]

    def current(self, key, generation) -> bool:
        state = self._generations.get(key)
        return state is not None and state.version is generation

    def upsert_if_current(self, key, generation, *, secret, metadata) -> None:
        with self._guard:
            if not self.current(key, generation):
                raise ConnectionError("Account connection changed; start authorization again")
            self.mutate(
                "upsert", key[1], key[2], account_id=key[3], secret=secret, metadata=metadata
            )

    def call(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        try:
            result = getattr(self.backend, operation)(*args, **kwargs)
        except (CredentialBackendUnavailable, SQLAlchemyError):
            record(operation, "unavailable")
            raise CredentialBackendUnavailable() from None
        except Exception:
            record(operation, "error")
            raise
        record(operation, "removed" if result is False else "success")
        return result

    def mutate(self, operation: str, user_id: str, provider: str, **kwargs: Any) -> Any:
        with self._guard:
            self.invalidate(user_id, provider, kwargs.get("account_id", ""))
            return self.call(operation, user_id, provider, **kwargs)

    @staticmethod
    def expiry(conn: ProviderConnection) -> float | None:
        value = conn.metadata.get("expires_at", conn.metadata.get("token_expires_at"))
        return float(value) if value is not None else None

    def _valid(self, conn: ProviderConnection) -> bool:
        expiry = self.expiry(conn)
        return expiry is None or expiry > time.time()

    def _entry(self, conn: ProviderConnection) -> _Entry:
        expiry = self.expiry(conn)
        issued = conn.metadata.get("issued_at", conn.updated_at)
        # Lifetime known at issuance: 45–55%, bounded to at most 30s jitter.
        lifetime = max(0.0, expiry - float(issued)) if expiry is not None else None
        refresh_at = (
            None
            if lifetime is None
            else float(issued) + lifetime / 2 + random.uniform(-1, 1) * min(30, lifetime * 0.05)
        )
        return _Entry(conn, refresh_at)

    def _read(self, key: tuple[int, str, str, str]) -> _Entry | None:
        _, user_id, provider, account_id = key
        with self._guard:
            if self.authorize is not None:
                try:
                    self.authorize(user_id)
                except OmnigentError:
                    self.invalidate(user_id, provider, account_id)
                    raise
            meta = self.call("get", user_id, provider, account_id=account_id)
            entry = self._entries.get(key)
            if meta is None:
                self._entries.pop(key, None)
                return None
            if entry and (meta.updated_at, meta.created_at, meta.metadata) == (
                entry.connection.updated_at,
                entry.connection.created_at,
                entry.connection.metadata,
            ):
                record("cache", "hit")
                return entry
            record("cache", "miss")
            conn = self.call("get", user_id, provider, account_id=account_id, with_secret=True)
            if conn is None or not conn.secret:
                self._entries.pop(key, None)
                return None
            entry = self._entry(conn)
            self._entries[key] = entry
            return entry

    async def resolve(
        self,
        user_id: str,
        provider: str,
        refresh: Callable[[ProviderConnection], Awaitable[tuple[dict[str, Any], dict[str, Any]]]]
        | None,
        *,
        account_id: str = "",
    ) -> ProviderConnection | None:
        key = self.key(user_id, provider, account_id)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            # Cancellation must not allow another refresh while the first is persisting.
            task = asyncio.create_task(self._resolve(key, refresh))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                try:
                    await task
                finally:
                    raise

    async def _resolve(self, key, refresh):
        generation = self.begin(key)
        try:
            entry = await asyncio.to_thread(self._read, key)
            if entry is None or not self.current(key, generation):
                return None
            conn = entry.connection
            assert conn.secret is not None
            expiry = self.expiry(conn)
            valid = expiry is None or expiry > time.time()
            if entry.failure:
                if self._valid(conn) and not isinstance(entry.failure, CredentialRefreshRejected):
                    return conn
                raise entry.failure
            if refresh is None or entry.refresh_at is None or entry.refresh_at > time.time():
                return conn if valid else None
            if not conn.secret.get("refresh_token"):
                return conn if valid else None
            try:
                secret, metadata = await refresh(conn)
            except Exception as exc:  # noqa: BLE001 — provider errors may contain secrets
                record("refresh", "provider_failure")
                rejected = isinstance(exc, CredentialRefreshRejected) or getattr(
                    exc, "reauth_required", False
                )
                if isinstance(exc, httpx.HTTPStatusError):
                    rejected = exc.response.status_code in (400, 401)
                failure = (
                    CredentialRefreshRejected(
                        "Credential refresh rejected; reauthorize the account"
                    )
                    if rejected
                    else CredentialProviderUnavailable(
                        "Credential provider temporarily unavailable; try again later"
                    )
                )
                entry.failure = failure
                if not self.current(key, generation):
                    return None
                if rejected or not self._valid(conn):
                    raise failure from None
                return conn
            if secret.get("refresh_token") is None:
                secret["refresh_token"] = conn.secret.get("refresh_token")
            try:
                updated = await asyncio.to_thread(self._persist, key, generation, secret, metadata)
            except OmnigentError:
                self.invalidate(key[1], key[2], key[3])
                raise
            except Exception as exc:  # noqa: BLE001 — never publish an unpersisted grant
                record("persistence", "failure")
                from omnigent.stores.credential_store.secret_cipher import SecretTooLargeError

                entry.failure = (
                    ConnectionError(str(exc))
                    if isinstance(exc, SecretTooLargeError)
                    else CredentialBackendUnavailable()
                )
                # Keep the old grant, never the unpersisted newly minted token.
                with self._guard:
                    if self.current(key, generation):
                        self._entries[key] = entry
                if self._valid(conn) and self.current(key, generation):
                    return conn
                raise entry.failure from None
            if not updated:
                self.invalidate(key[1], key[2], key[3])
                return None
            record("refresh", "success")
            return updated.connection if self.current(key, generation) else None
        finally:
            self.end(key)

    def _persist(self, key, generation, secret, metadata):
        with self._guard:
            if not self.current(key, generation):
                return None
            if not self.call(
                "update_secret",
                key[1],
                key[2],
                account_id=key[3],
                secret=secret,
                metadata=metadata,
            ):
                return None
            persisted = self.call("get", key[1], key[2], account_id=key[3])
            if persisted is None:
                return None
            conn = replace(persisted, secret=secret)
            entry = self._entry(conn)
            self._entries[key] = entry
            return entry


class ManagedCredentialBackend:
    """Application boundary for mutation invalidation and bounded operation metrics."""

    def __init__(self, backend: CredentialBackend, storage_location: str | None = None) -> None:
        self.cache = TokenCache(backend)
        if storage_location:
            from omnigent.db.account_authority import require_active_account
            from omnigent.db.utils import get_or_create_engine, make_named_managed_session_maker

            session = make_named_managed_session_maker(
                get_or_create_engine(storage_location),
                query_name_prefix="omnigent.credential_authority",
            )

            def authorize(user_id: str) -> None:
                with session("resolve") as transaction:
                    require_active_account(transaction, user_id)

            self.cache.authorize = authorize

    def get(self, user_id, provider, *, account_id="", with_secret=False):
        return self._read("get", user_id, provider, account_id=account_id, with_secret=with_secret)

    def _read(self, operation, *args, **kwargs):
        return self.cache.call(operation, *args, **kwargs)

    def upsert(self, user_id, provider, *, secret, metadata, account_id=""):
        return self.cache.mutate(
            "upsert", user_id, provider, secret=secret, metadata=metadata, account_id=account_id
        )

    def update_secret(self, user_id, provider, *, secret, metadata=None, account_id=""):
        return self.cache.mutate(
            "update_secret",
            user_id,
            provider,
            secret=secret,
            metadata=metadata,
            account_id=account_id,
        )

    def delete(self, user_id, provider, *, account_id=""):
        return self.cache.mutate("delete", user_id, provider, account_id=account_id)

    def list_for_user(self, user_id):
        return self._read("list_for_user", user_id)

    def list_all(self, *, provider=None):
        return self._read("list_all", provider=provider)


_managed: WeakValueDictionary[int, ManagedCredentialBackend] = WeakValueDictionary()
_managed_guard = threading.Lock()


def managed_backend(
    backend: CredentialBackend, *, storage_location: str | None = None
) -> ManagedCredentialBackend:
    if isinstance(backend, ManagedCredentialBackend):
        return backend
    with _managed_guard:
        wrapper = _managed.get(id(backend))
        if wrapper is None:
            wrapper = ManagedCredentialBackend(backend, storage_location)
            _managed[id(backend)] = wrapper
        return wrapper
