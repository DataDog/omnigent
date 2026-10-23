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
from omnigent.stores.credential_store.refresh_retry import refresh_retry_safe

CacheKey = tuple[int, str, str, str]
Refresh = Callable[[ProviderConnection], Awaitable[tuple[dict[str, Any], dict[str, Any]]]]

_counter = metrics.get_meter(__name__).create_counter("omnigent.credentials.operations")


def record(operation: str, result: str) -> None:
    _counter.add(1, {"operation": operation, "result": result})


class CredentialRefreshRejected(ConnectionError):
    """The provider explicitly requires new authorization."""


class CredentialProviderUnavailable(ConnectionError):
    """The provider could not refresh a grant temporarily."""

    def __init__(self, message: str, *, refresh_retry_safe: bool = False) -> None:
        super().__init__(message)
        self.refresh_retry_safe = refresh_retry_safe


@dataclass(repr=False)
class _Entry:
    connection: ProviderConnection
    refresh_at: float | None
    failure: Exception | None = field(default=None, repr=False)
    retry_at: float | None = None
    retry_attempts: int = 0


@dataclass
class _Generation:
    version: object = field(default_factory=object)
    readers: int = 0


class TokenCache:
    def __init__(self, backend: CredentialBackend) -> None:
        self.backend = backend
        self._entries: LRUCache[CacheKey, _Entry] = LRUCache(2048)
        # Denial markers contain no secrets and survive positive-cache eviction.
        # They live only until the connection changes or disappears.
        self._failures: dict[CacheKey, _Entry] = {}
        self._locks: WeakValueDictionary[CacheKey, asyncio.Lock] = WeakValueDictionary()
        self._guard = threading.RLock()
        self._row_locks: WeakValueDictionary[CacheKey, Any] = WeakValueDictionary()
        self._generations: dict[CacheKey, _Generation] = {}
        self.authorize: Callable[[str], None] | None = None

    @staticmethod
    def key(user_id: str, provider: str, account_id: str = "") -> CacheKey:
        return current_workspace_id(), user_id, provider, account_id

    def mutation_lock(self, key: CacheKey) -> Any:
        # Keep backend calls independent across users; the global guard protects
        # only small in-memory maps, never SQL or remote I/O.
        with self._guard:
            lock = self._row_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._row_locks[key] = lock
            return lock

    def _cached(self, key: CacheKey) -> _Entry | None:
        with self._guard:
            return self._entries.get(key)

    def _cache(self, key: CacheKey, entry: _Entry) -> None:
        with self._guard:
            self._entries[key] = entry

    def invalidate(self, user_id: str, provider: str, account_id: str = "") -> None:
        key = self.key(user_id, provider, account_id)
        with self.mutation_lock(key), self._guard:
            self._entries.pop(key, None)
            self._failures.pop(key, None)
            # Only retain generations while a resolver is in flight.
            if key in self._generations:
                self._generations[key].version = object()

    def finish_mutation(self, key: CacheKey) -> None:
        # A callback may begin while synchronous I/O holds the row lock.
        with self._guard:
            if key in self._generations:
                self._generations[key].version = object()

    def reject(self, user_id: str, provider: str, account_id: str = "") -> None:
        """Discard a rejected access token without retrying a spent refresh grant."""
        key = self.key(user_id, provider, account_id)
        with self.mutation_lock(key):
            entry = self._cached(key) or self._failures.get(key)
            self.invalidate(user_id, provider, account_id)
            if entry is not None:
                entry.connection = replace(entry.connection, secret=None)
                entry.failure = CredentialRefreshRejected(
                    "Credential rejected; reauthorize the account"
                )
                self._cache(key, entry)
                self._remember_failure(key, entry)

    def _remember_failure(
        self, key: CacheKey, entry: _Entry, generation: object | None = None
    ) -> None:
        with self.mutation_lock(key), self._guard:
            if generation is not None and not self.current(key, generation):
                return
            self._failures[key] = _Entry(
                replace(entry.connection, secret=None),
                None,
                entry.failure,
                entry.retry_at,
                entry.retry_attempts,
            )

    @staticmethod
    def _same_row(left: ProviderConnection, right: ProviderConnection) -> bool:
        return (left.created_at, left.updated_at, left.metadata) == (
            right.created_at,
            right.updated_at,
            right.metadata,
        )

    def begin(self, key: CacheKey) -> object:
        with self._guard:
            state = self._generations.setdefault(key, _Generation())
            state.readers += 1
            return state.version

    def end(self, key: CacheKey) -> None:
        with self._guard:
            state = self._generations[key]
            state.readers -= 1
            if not state.readers:
                del self._generations[key]

    def current(self, key: CacheKey, generation: object) -> bool:
        state = self._generations.get(key)
        return state is not None and state.version is generation

    def upsert_if_current(
        self,
        key: CacheKey,
        generation: object,
        *,
        secret: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        with self.mutation_lock(key):
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
        except OmnigentError:
            record(operation, "error")
            raise
        except Exception as exc:
            from omnigent.stores.credential_store.secret_cipher import SecretTooLargeError

            record(operation, "error")
            if isinstance(exc, SecretTooLargeError):
                raise
            raise RuntimeError("Credential backend operation failed") from None
        record(operation, "removed" if result is False else "success")
        return result

    def mutate(self, operation: str, user_id: str, provider: str, **kwargs: Any) -> Any:
        key = self.key(user_id, provider, kwargs.get("account_id", ""))
        with self.mutation_lock(key):
            failed_grant = self._failures.get(key)
            self.invalidate(user_id, provider, kwargs.get("account_id", ""))
            try:
                return self.call(operation, user_id, provider, **kwargs)
            except Exception:
                # Failed reconnects must not make a previously spent refresh
                # eligible again. A changed durable row clears this on read.
                if failed_grant is not None:
                    self._failures[key] = failed_grant
                raise
            finally:
                self.finish_mutation(key)

    @staticmethod
    def expiry(conn: ProviderConnection) -> float | None:
        value = conn.metadata.get("expires_at", conn.metadata.get("token_expires_at"))
        return float(value) if value is not None else None

    def _valid(self, conn: ProviderConnection) -> bool:
        expiry = self.expiry(conn)
        return expiry is None or expiry > time.time()

    def _revalidate(self, key: CacheKey, generation: object, expected: ProviderConnection) -> bool:
        with self.mutation_lock(key):
            if not self.current(key, generation):
                return False
            if self.authorize is not None:
                try:
                    self.authorize(key[1])
                except OmnigentError:
                    self.invalidate(key[1], key[2], key[3])
                    raise
            row = self.call("get", key[1], key[2], account_id=key[3])
            if row is None or not self._same_row(row, expected):
                self.invalidate(key[1], key[2], key[3])
                return False
            return True

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

    def _read(self, key: CacheKey) -> _Entry | None:
        _, user_id, provider, account_id = key
        with self.mutation_lock(key):
            if self.authorize is not None:
                try:
                    self.authorize(user_id)
                except OmnigentError:
                    self.invalidate(user_id, provider, account_id)
                    raise
            meta = self.call("get", user_id, provider, account_id=account_id)
            entry = self._cached(key)
            if meta is None:
                self.invalidate(user_id, provider, account_id)
                return None
            failure = self._failures.get(key)
            if failure and not self._same_row(meta, failure.connection):
                self._failures.pop(key, None)
                failure = None
            if failure and (
                isinstance(failure.failure, CredentialRefreshRejected)
                or (
                    not self._valid(failure.connection)
                    and (failure.retry_at is None or failure.retry_at > time.time())
                )
            ):
                assert failure.failure is not None
                raise failure.failure
            if entry and self._same_row(meta, entry.connection):
                record("cache", "hit")
                return entry
            record("cache", "miss")
            conn = self.call("get", user_id, provider, account_id=account_id, with_secret=True)
            if conn is None or not conn.secret:
                self.invalidate(user_id, provider, account_id)
                return None
            entry = self._entry(conn)
            if failure:
                entry.failure = failure.failure
                entry.retry_at = failure.retry_at
                entry.retry_attempts = failure.retry_attempts
            self._cache(key, entry)
            return entry

    async def resolve(
        self,
        user_id: str,
        provider: str,
        refresh: Refresh | None,
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
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                    except Exception:  # noqa: BLE001 — the canceled caller cannot consume errors
                        break
                if not task.cancelled():
                    task.exception()
                raise

    async def _resolve(self, key: CacheKey, refresh: Refresh | None) -> ProviderConnection | None:
        generation = self.begin(key)
        try:
            entry = await asyncio.to_thread(self._read, key)
            if entry is None or not self.current(key, generation):
                return None
            conn = entry.connection
            expiry = self.expiry(conn)
            valid = expiry is None or expiry > time.time()
            if entry.failure and (entry.retry_at is None or entry.retry_at > time.time()):
                if self._valid(conn) and not isinstance(entry.failure, CredentialRefreshRejected):
                    return conn
                raise entry.failure
            assert conn.secret is not None
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
                retry_safe = not rejected and (
                    refresh_retry_safe(exc.response)
                    if isinstance(exc, httpx.HTTPStatusError)
                    else getattr(exc, "refresh_retry_safe", False)
                )
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
                entry.retry_at = None
                if retry_safe:
                    entry.retry_attempts = min(entry.retry_attempts + 1, 7)
                    entry.retry_at = time.time() + min(300, 5 * 2 ** (entry.retry_attempts - 1))
                    record("refresh", "retry_deferred")
                if not await asyncio.to_thread(self._revalidate, key, generation, conn):
                    return None
                self._remember_failure(key, entry, generation)
                if rejected or not self._valid(conn):
                    raise failure from None
                return conn
            if secret.get("refresh_token") is None:
                secret["refresh_token"] = conn.secret.get("refresh_token")
            try:
                updated = await asyncio.to_thread(
                    self._persist, key, generation, conn, secret, metadata
                )
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
                # Once an exchange succeeds, failed persistence is uncertain
                # even when an earlier provider rejection was safe to retry.
                entry.retry_at = None
                # Keep the old grant, never the unpersisted newly minted token.
                with self.mutation_lock(key):
                    if self.current(key, generation):
                        self._cache(key, entry)
                        self._remember_failure(key, entry, generation)
                if self._valid(conn) and await asyncio.to_thread(
                    self._revalidate, key, generation, conn
                ):
                    return conn
                raise entry.failure from None
            if not updated:
                record("refresh", "discarded")
                self.invalidate(key[1], key[2], key[3])
                return None
            if not self.current(key, generation):
                return None
            if not self._valid(updated.connection):
                updated.failure = CredentialProviderUnavailable(
                    "Credential provider returned an expired grant"
                )
                self._remember_failure(key, updated, generation)
                record("refresh", "expired_grant")
                raise updated.failure
            record("refresh", "success")
            return updated.connection
        finally:
            self.end(key)

    def _persist(
        self,
        key: CacheKey,
        generation: object,
        expected: ProviderConnection,
        secret: dict[str, Any],
        metadata: dict[str, Any],
    ) -> _Entry | None:
        with self.mutation_lock(key):
            if not self._revalidate(key, generation, expected):
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
            self._failures.pop(key, None)
            self._cache(key, entry)
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
                try:
                    with session("resolve") as transaction:
                        require_active_account(transaction, user_id)
                except SQLAlchemyError:
                    record("authority", "unavailable")
                    raise CredentialBackendUnavailable() from None

            self.cache.authorize = authorize

    def get(
        self, user_id: str, provider: str, *, account_id: str = "", with_secret: bool = False
    ) -> ProviderConnection | None:
        return self._read("get", user_id, provider, account_id=account_id, with_secret=with_secret)

    def _read(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        return self.cache.call(operation, *args, **kwargs)

    def upsert(
        self,
        user_id: str,
        provider: str,
        *,
        secret: dict[str, Any],
        metadata: dict[str, Any],
        account_id: str = "",
    ) -> ProviderConnection:
        return self.cache.mutate(
            "upsert", user_id, provider, secret=secret, metadata=metadata, account_id=account_id
        )

    def update_secret(
        self,
        user_id: str,
        provider: str,
        *,
        secret: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        account_id: str = "",
    ) -> bool:
        return self.cache.mutate(
            "update_secret",
            user_id,
            provider,
            secret=secret,
            metadata=metadata,
            account_id=account_id,
        )

    def delete(self, user_id: str, provider: str, *, account_id: str = "") -> bool:
        return self.cache.mutate("delete", user_id, provider, account_id=account_id)

    def list_for_user(self, user_id: str) -> list[ProviderConnection]:
        return self._read("list_for_user", user_id)

    def list_all(self, *, provider: str | None = None) -> list[ProviderConnection]:
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
