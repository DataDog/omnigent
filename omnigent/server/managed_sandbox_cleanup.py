"""Bounded reconciliation for persisted managed-sandbox cleanup tombstones."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass

from omnigent.db.db_models import workspace_scope
from omnigent.db.utils import now_epoch
from omnigent.onboarding.sandboxes.context import managed_sandbox_context_scope
from omnigent.server.managed_hosts import ManagedSandboxConfig, terminate_managed_host
from omnigent.server.managed_sandbox_identity import (
    ManagedSandboxIdentityResolver,
    ManagedSandboxIdentityUnavailable,
)
from omnigent.stores.host_store import Host, HostStore

_logger = logging.getLogger(__name__)

# A local server has one reconciler task.  Bound each scan and retry delay so a
# provider outage cannot create an unbounded work queue or a tight retry loop.
MANAGED_CLEANUP_BATCH_SIZE = 64
MANAGED_CLEANUP_POLL_INTERVAL_S = 30
MANAGED_CLEANUP_RETRY_BASE_S = 5
MANAGED_CLEANUP_RETRY_MAX_S = 300


def managed_cleanup_retry_delay_s(attempts: int) -> int:
    """Return capped exponential backoff for a persisted failure count."""
    exponent = min(max(attempts - 1, 0), 6)
    return min(MANAGED_CLEANUP_RETRY_BASE_S * (2**exponent), MANAGED_CLEANUP_RETRY_MAX_S)


@dataclass(frozen=True)
class ManagedCleanupReconcileResult:
    """Counts from one bounded reconciliation pass."""

    attempted: int = 0
    deleted: int = 0
    deferred: int = 0


class ManagedSandboxCleanupReconciler:
    """Retry exact-ID cleanup using only the tombstone owner's credential.

    The reconciler deliberately has no create, relaunch, provider-list, or
    identity-search capability.  Every operation begins from a durable row and
    uses ``terminate_managed_host`` to re-read that same host id before issuing
    the provider's exact ``sandbox_id`` delete.
    """

    def __init__(
        self,
        *,
        host_store: HostStore,
        config: ManagedSandboxConfig,
        identity_resolver: ManagedSandboxIdentityResolver,
        batch_size: int = MANAGED_CLEANUP_BATCH_SIZE,
        poll_interval_s: float = MANAGED_CLEANUP_POLL_INTERVAL_S,
    ) -> None:
        self._host_store = host_store
        self._config = config
        self._identity_resolver = identity_resolver
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._reconcile_lock = asyncio.Lock()

    @property
    def is_started(self) -> bool:
        """Whether this app instance currently owns the local retry task."""
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Start one local retry loop; repeated calls are idempotent."""
        if self.is_started:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="managed-sandbox-cleanup")

    async def stop(self) -> None:
        """Cancel the local loop before app shutdown can outlive it."""
        self._stop_event.set()
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Startup and cleanup debt must never bring down the server.
                _logger.exception("managed sandbox cleanup reconciliation failed")
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_s)

    async def reconcile_once(self, *, now: int | None = None) -> ManagedCleanupReconcileResult:
        """Run one bounded pass over due tombstones.

        ``now`` exists for deterministic tests.  Normal app operation uses the
        server clock, and derives retry eligibility from the persisted failure
        count plus ``updated_at`` so a process restart retains its backoff.
        """
        async with self._reconcile_lock:
            reference_time = now if now is not None else now_epoch()
            tombstones = await asyncio.to_thread(
                self._host_store.list_managed_cleanup_pending_all_workspaces,
                limit=self._batch_size,
            )
            attempted = deleted = deferred = 0
            for host in tombstones:
                if not _cleanup_due(host, reference_time):
                    deferred += 1
                    continue
                attempted += 1
                if await self._reconcile_host(host):
                    deleted += 1
            return ManagedCleanupReconcileResult(
                attempted=attempted, deleted=deleted, deferred=deferred
            )

    async def _reconcile_host(self, tombstone: Host) -> bool:
        """Retry one row inside its owning workspace and identity context."""
        # This scope makes every store mutation target the same tenant returned
        # by the cross-workspace read; it does not search any other tenant.
        with workspace_scope(tombstone.workspace_id):
            try:
                context = self._identity_resolver.for_host(tombstone)
            except ManagedSandboxIdentityUnavailable:
                # No current credential is an authorization failure, not a
                # reason to guess another login.  Persist it as a failed pass
                # so repeated startup/reload cycles remain rate limited.
                await asyncio.to_thread(
                    self._host_store.mark_managed_cleanup_pending, tombstone.host_id
                )
                _logger.info(
                    "Managed sandbox cleanup remains pending for host %s: "
                    "owner credential unavailable",
                    tombstone.host_id,
                )
                return False

            try:
                with managed_sandbox_context_scope(context):
                    return await terminate_managed_host(tombstone, self._host_store, self._config)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                # Keep the tombstone even for an unexpected provider boundary
                # failure.  No create or replacement is permitted here.
                await asyncio.to_thread(
                    self._host_store.mark_managed_cleanup_pending, tombstone.host_id
                )
                _logger.warning(
                    "Managed sandbox cleanup retry failed for host %s",
                    tombstone.host_id,
                    exc_info=True,
                )
                return False


def _cleanup_due(host: Host, reference_time: int) -> bool:
    """Whether a tombstone's persisted backoff has elapsed."""
    return reference_time >= host.updated_at + managed_cleanup_retry_delay_s(
        host.sandbox_cleanup_attempts
    )
