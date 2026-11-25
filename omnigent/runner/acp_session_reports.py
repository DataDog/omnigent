"""Subscribe to a harness session's durable standard ACP report stream."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import quote

import httpx

from omnigent.inner.acp_standard_subagents import (
    acp_standard_child_key,
    acp_standard_report_source_id,
    acp_standard_root_tool_reference_kind,
)
from omnigent.runtime.harnesses.session_reports import SESSION_REPORT_MAX_RECORDS

_logger = logging.getLogger("omnigent.runner.app")
_PROTOCOL = "acp-session-reports"
_VERSION = 3
_REQUIRED_FEATURES = frozenset(
    {
        "standard-acp-subagents-v1",
        "session-entities-v1",
        "session-owned-actions-v2",
        "root-source-epochs-v1",
    }
)
_REPORT_KINDS = frozenset(
    {
        "association.patch",
        "observation.patch",
        "entity.upsert",
        "entity.patch",
        "entity.append",
        "tool.references",
    }
)
_READ_LIMIT = 100
_WAIT_MS = 25_000
_RETRY_INITIAL_S = 0.5
_RETRY_MAX_S = 15.0
_MAX_PENDING_RECORDS = SESSION_REPORT_MAX_RECORDS


ActivityCallback = Callable[[str, bool], Awaitable[None]]
LeaseCallback = Callable[[str, str, bool], Awaitable[None]]
RecoveryCallback = Callable[[str, str, str], Awaitable[None]]
ChildLookupCallback = Callable[[str], Awaitable[dict[str, dict[str, Any]]]]
AssociationLookupCallback = Callable[[str], Awaitable[list[dict[str, Any]]]]
ChildTerminalCallback = Callable[[str], Awaitable[None]]
SourceFailureCallback = Callable[[str, str, str], Awaitable[None]]


def acp_session_child_key(parent_id: str, native_epoch: str, native_session_id: str) -> str:
    """Build the stable runner key for one reusable native session association."""
    return acp_standard_child_key(parent_id, native_epoch, native_session_id)


def _required_string(value: object, name: str, *, max_length: int = 512) -> str:
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise ValueError(f"ACP session report {name} is invalid")
    return value


def _source_id(server_identity: str, root_id: str, epoch: str, ingress_sequence: int) -> str:
    return acp_standard_report_source_id(server_identity, root_id, epoch, ingress_sequence)


class AcpSessionReportSubscriber:
    """Consume standard ACP reports for one harness-owning Omnigent session.

    ``parent_id`` is the private report root, even when this session has an
    ordinary Omnigent parent in the larger conversation tree.
    """

    def __init__(
        self,
        *,
        parent_id: str,
        harness_client: httpx.AsyncClient,
        server_client: httpx.AsyncClient,
        generation: str,
        connection_admission_id: str | None = None,
        runtime_incarnation: int | None = None,
        activity_callback: ActivityCallback,
        lease_callback: LeaseCallback,
        note_activity: Callable[[str], None],
        lookup_children: ChildLookupCallback,
        recover_open_child: RecoveryCallback,
        lookup_associations: AssociationLookupCallback | None = None,
        child_terminal_callback: ChildTerminalCallback | None = None,
        source_failure_callback: SourceFailureCallback | None = None,
    ) -> None:
        self.parent_id = parent_id
        self._harness_client = harness_client
        self._server_client = server_client
        self.generation = generation
        self._connection_admission_id = connection_admission_id
        self._runtime_incarnation = runtime_incarnation
        self._journal_id: str | None = None
        self._epoch_ordinal: int | None = None
        self._delivery_grant: tuple[str, int, str] | None = None
        self._active_epoch_grant: tuple[str, int, str, int, str] | None = None
        self._historical_epoch_bindings: set[tuple[str, int, str, int, str]] = set()
        self._delivery_grant_lock = asyncio.Lock()
        self._historical_epoch_binding_lock = asyncio.Lock()
        self._activity_callback = activity_callback
        self._lease_callback = lease_callback
        self._note_activity = note_activity
        self._lookup_children = lookup_children
        self._lookup_associations = lookup_associations
        # These callbacks remain constructor-compatible with app lifecycle code.
        # A missing report or idle state never invents a terminal outcome.
        self._recover_open_child = recover_open_child
        self._child_terminal_callback = child_terminal_callback
        self._source_failure_callback = source_failure_callback
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        self._child_id_available = asyncio.Event()
        self._association_changed = asyncio.Event()
        self._active_children: set[str] = set()
        self._pending: dict[int, dict[str, Any]] = {}
        self._processed: set[int] = set()
        self._read_through = 0
        self._acknowledged = 0
        self._child_ids: dict[str, str] = {}
        self._native_child_keys: dict[tuple[str, str], str] = {}
        self._child_native_epochs: dict[str, str] = {}
        self._child_origin_tokens: dict[str, str] = {}
        self._associations_by_source: dict[str, dict[str, Any]] = {}
        self._restored_association_sources: dict[str, dict[str, Any]] = {}
        self._native_epoch: str | None = None
        self._reactivation_epoch: str | None = None
        self._historical_native_epochs: set[str] = set()
        self._source_failed = False
        self._source_failure_settled = False
        self._source_failure_reason: str | None = None
        self._ready = False
        self._replaying_startup_prefix = False
        self._features: frozenset[str] = frozenset()

    @property
    def is_busy(self) -> bool:
        return bool(self._active_children or self._pending)

    @property
    def source_failed(self) -> bool:
        """Whether this generation's report producer failed and must be replaced."""
        return self._source_failed

    @property
    def source_failure_reason(self) -> str | None:
        return self._source_failure_reason

    @property
    def source_failure_settled(self) -> bool:
        """Whether the accepted durable prefix was applied and local leases released."""
        return self._source_failure_settled

    @property
    def is_ready(self) -> bool:
        """Whether capability negotiation and durable-prefix replay completed."""
        return self._ready and not self._source_failed and not self._stopped.is_set()

    @property
    def active_origin_tokens(self) -> set[str]:
        """Trusted origins pinned to reusable associations, including currently idle ones."""
        if self._native_epoch is None:
            return set()
        return {
            token
            for child_key, token in self._child_origin_tokens.items()
            if self._child_native_epochs.get(child_key) == self._native_epoch
        }

    @property
    def active_native_epoch(self) -> str | None:
        """Native process epoch currently authorized for live actions and activity."""
        return self._native_epoch

    @property
    def features(self) -> tuple[str, ...]:
        """Features proven by the live report-capability endpoint."""
        return tuple(sorted(self._features))

    def child_session_id_for_key(self, child_key: str) -> str | None:
        """Return the persisted Omnigent conversation for one reusable child key."""
        return self._child_ids.get(child_key)

    def child_session_id_for_native_session(
        self, native_epoch: str, native_session_id: str
    ) -> str | None:
        """Resolve an already validated epoch/session association."""
        key = self._native_child_keys.get((native_epoch, native_session_id))
        return self._child_ids.get(key) if key is not None else None

    def has_active_origin(self, child_key: str, token: str) -> bool:
        """Whether this exact child retains a trusted origin in the active epoch."""
        return (
            self._native_epoch is not None
            and self._child_native_epochs.get(child_key) == self._native_epoch
            and self._child_origin_tokens.get(child_key) == token
        )

    async def wait_for_child_session_id(self, child_key: str, *, timeout_s: float = 30.0) -> str:
        """Wait until a strict association report creates its Omnigent child."""
        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            child_id = self._child_ids.get(child_key)
            if child_id is not None:
                return child_id
            if self._stopped.is_set():
                raise RuntimeError("ACP report subscriber stopped before association was durable")
            self._child_id_available.clear()
            child_id = self._child_ids.get(child_key)
            if child_id is not None:
                return child_id
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("ACP child association did not become durable in time")
            try:
                await asyncio.wait_for(self._child_id_available.wait(), timeout=remaining)
            except TimeoutError as exc:
                raise TimeoutError("ACP child association did not become durable in time") from exc

    async def wait_for_association_source(
        self,
        source_id: str,
        *,
        native_epoch: str,
        target_session_id: str,
        context_generation: str,
        origin_token: str,
        timeout_s: float = 30.0,
    ) -> str:
        """Wait until the named association report has passed the harness ACK barrier."""
        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            if self._native_epoch != native_epoch:
                raise RuntimeError("ACP action refers to an inactive native epoch")
            record = self._associations_by_source.get(source_id)
            if record is not None:
                data = record["data"]
                origin = data.get("_omnigent_action_origin")
                if (
                    record.get("kind") != "association.patch"
                    or record.get("source_id") != source_id
                    or record.get("native_epoch") != native_epoch
                    or data.get("target_session_id") != target_session_id
                    or data.get("context_generation") != context_generation
                    or not isinstance(origin, dict)
                    or origin.get("token") != origin_token
                ):
                    raise RuntimeError("ACP action refers to a different association report")
                child_id = self.child_session_id_for_native_session(
                    native_epoch, target_session_id
                )
                if child_id is None:
                    raise RuntimeError("acknowledged ACP association has no persisted child")
                return child_id
            if self._stopped.is_set():
                raise RuntimeError("ACP report subscriber stopped before the association ACK")
            self._association_changed.clear()
            if source_id in self._associations_by_source:
                continue
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("ACP association report did not cross its ACK barrier in time")
            try:
                await asyncio.wait_for(self._association_changed.wait(), timeout=remaining)
            except TimeoutError as exc:
                raise TimeoutError(
                    "ACP association report did not cross its ACK barrier in time"
                ) from exc

    async def wait_for_live_epoch(self, native_epoch: str, *, timeout_s: float = 30.0) -> bool:
        """Wait out a reconnect's reactivation of *native_epoch* before judging an action.

        A server reconnect clears the live epoch until capabilities re-activate it,
        so a child action in that window must wait rather than read as stale.
        Returns whether *native_epoch* is live; any other mismatch returns at once.
        """
        deadline = asyncio.get_running_loop().time() + timeout_s
        while True:
            if self._native_epoch == native_epoch:
                return True
            if (
                self._stopped.is_set()
                or self._native_epoch is not None
                or self._reactivation_epoch != native_epoch
            ):
                return False
            self._association_changed.clear()
            if self._native_epoch == native_epoch:
                return True
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            try:
                await asyncio.wait_for(self._association_changed.wait(), timeout=remaining)
            except TimeoutError:
                return False

    def has_child_session_id(self, child_id: str) -> bool:
        """Whether this subscriber owns the given child conversation."""
        return child_id in self._child_ids.values()

    async def refresh_source_status(self) -> bool:
        """Check whether the current producer failed since the last report page."""
        capabilities = await self._get_capabilities()
        source_status = capabilities.get("source_status")
        if not isinstance(source_status, dict):
            raise RuntimeError("harness returned malformed ACP source status")
        if source_status.get("state") == "failed":
            self._mark_source_failed(source_status.get("error"))
        return self._source_failed

    async def refresh_delivery_owner(
        self, connection_admission_id: str, runtime_incarnation: int
    ) -> None:
        """Refresh the tunnel-bound report delivery grant after server reconnect."""
        if (
            not isinstance(connection_admission_id, str)
            or not connection_admission_id
            or isinstance(runtime_incarnation, bool)
            or not isinstance(runtime_incarnation, int)
            or runtime_incarnation < 1
        ):
            raise ValueError("ACP report delivery owner identity is invalid")
        if (
            connection_admission_id != self._connection_admission_id
            or runtime_incarnation != self._runtime_incarnation
        ):
            if self._native_epoch is not None:
                self._reactivation_epoch = self._native_epoch
            await self._set_native_epoch(None)
        self._connection_admission_id = connection_admission_id
        self._runtime_incarnation = runtime_incarnation
        await self._get_capabilities()

    async def start(self) -> None:
        """Negotiate v3, replay the advertised prefix, and begin polling."""
        capabilities = await self._get_capabilities()
        acknowledged = _nonnegative_int(
            capabilities.get("acknowledged_sequence"), "acknowledgment"
        )
        high_water = _nonnegative_int(capabilities.get("highest_sequence"), "high-water mark")
        if high_water < acknowledged:
            raise RuntimeError("harness returned an invalid ACP report high-water mark")
        source_status = capabilities.get("source_status")
        if not isinstance(source_status, dict):
            raise RuntimeError("harness returned malformed ACP source status")
        if source_status.get("state") == "failed":
            self._mark_source_failed(source_status.get("error"))
        self._acknowledged = acknowledged
        self._read_through = acknowledged
        self._replaying_startup_prefix = True

        if acknowledged:
            if self._lookup_associations is None:
                raise RuntimeError("ACP report recovery has no durable association inventory")
            associations = await self._lookup_associations(self.parent_id)
            known_sessions: dict[str, set[str]] = {}
            root_native_sessions: dict[str, str] = {}
            child_associations: list[dict[str, Any]] = []
            for association in associations:
                association_epoch = association.get("native_epoch")
                session_id = association.get("native_session_id")
                parent_session_id = association.get("parent_native_session_id")
                if (
                    not isinstance(association_epoch, str)
                    or not association_epoch
                    or not isinstance(session_id, str)
                    or not session_id
                ):
                    raise RuntimeError("server returned an invalid ACP association identity")
                known_sessions.setdefault(association_epoch, set()).add(session_id)
                if parent_session_id is None:
                    root_native_sessions[association_epoch] = session_id
                    continue
                child_associations.append(association)
            for association in child_associations:
                association_epoch = association["native_epoch"]
                session_id = association["native_session_id"]
                parent_session_id = association.get("parent_native_session_id")
                conversation_id = association.get("conversation_id")
                context_generation = association.get("context_generation")
                source_id = association.get("association_source_id")
                if (
                    not isinstance(parent_session_id, str)
                    or not parent_session_id
                    or not isinstance(conversation_id, str)
                    or not conversation_id
                    or not isinstance(context_generation, str)
                    or not context_generation
                    or not isinstance(source_id, str)
                    or not source_id
                ):
                    raise RuntimeError("server returned an incomplete ACP association binding")
                if parent_session_id not in known_sessions.get(association_epoch, set()):
                    raise RuntimeError("server returned an ACP association with no durable parent")
                if association_epoch not in root_native_sessions:
                    raise RuntimeError("server returned an ACP association with no epoch root")
                child_key = acp_session_child_key(self.parent_id, association_epoch, session_id)
                self._child_ids[child_key] = conversation_id
                self._native_child_keys[(association_epoch, session_id)] = child_key
                self._child_native_epochs[child_key] = association_epoch
                origin = association.get("action_origin")
                barrier_data = {
                    "root_native_session_id": root_native_sessions.get(association_epoch),
                    "target_session_id": session_id,
                    "parent_session_id": parent_session_id,
                    "context_generation": context_generation,
                }
                if origin is not None:
                    barrier_data["_omnigent_action_origin"] = origin
                self._restored_association_sources[source_id] = {
                    "kind": "association.patch",
                    "native_epoch": association_epoch,
                    "source_id": source_id,
                    "data": barrier_data,
                }
                if origin is not None:
                    if (
                        not isinstance(origin, dict)
                        or not isinstance(origin.get("token"), str)
                        or not origin["token"]
                    ):
                        raise RuntimeError("server returned an invalid ACP action origin")
            self._child_id_available.set()

        while self._acknowledged < high_water:
            before = self._acknowledged
            await self._read_available_reports(wait_ms=0, notify_source_failure=False)
            if self._acknowledged == before:
                raise RuntimeError(
                    "ACP startup report prefix ended before its advertised high-water"
                )
        if self._acknowledged >= high_water:
            # The remaining inventory rows predate the harness's advertised ACK
            # cursor. Rows replayed from its unacknowledged tail were promoted by
            # _ack_ready_prefix only after the matching harness ACK succeeded.
            for source_id, record in self._restored_association_sources.items():
                self._associations_by_source.setdefault(source_id, record)
            self._restored_association_sources.clear()
            self._refresh_active_origins()
        self._replaying_startup_prefix = False
        self._ready = not self._source_failed
        if self._source_failed:
            await self._settle_source_failure(
                self._source_failure_reason or "ACP report source failed before readiness."
            )
            return
        self._task = asyncio.create_task(
            self._poll(), name=f"acp-session-reports:{self.parent_id}"
        )

    async def drain_to_high_water(self, *, timeout_s: float = 10.0) -> bool:
        """Wait for a stable advertised report prefix to be persisted and ACKed."""
        deadline = asyncio.get_running_loop().time() + timeout_s
        previous_high_water: int | None = None
        current = asyncio.current_task()
        for _ in range(16):
            capabilities = await self._get_capabilities()
            high_water = _nonnegative_int(capabilities.get("highest_sequence"), "high-water mark")
            source_status = capabilities.get("source_status")
            if not isinstance(source_status, dict):
                raise RuntimeError("harness returned malformed ACP source status")
            if source_status.get("state") == "failed":
                self._mark_source_failed(source_status.get("error"))
                return False
            if self._acknowledged < high_water:
                if current is self._task:
                    await self._read_available_reports(wait_ms=0)
                    if self._acknowledged < high_water:
                        return False
                else:
                    while self._acknowledged < high_water and not self._source_failed:
                        remaining = deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            return False
                        await asyncio.sleep(min(0.05, remaining))
                    if self._source_failed:
                        return False
                previous_high_water = high_water
                continue
            if previous_high_water == high_water:
                return True
            previous_high_water = high_water
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.05, remaining))
        return False

    async def stop(self, *, settle: bool = False, reason: str | None = None) -> None:
        """Stop polling; release local activity leases without inventing outcomes."""
        if reason is not None:
            _logger.debug("Stopping ACP session-report subscriber: %s", reason)
        task = self._task
        self._task = None
        if settle:
            with contextlib.suppress(Exception):
                await self._read_available_reports(wait_ms=0, notify_source_failure=False)
        self._stopped.set()
        self._child_id_available.set()
        self._association_changed.set()
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(task, timeout=3.0)
        for child_key in tuple(self._active_children):
            with contextlib.suppress(Exception):
                await self._lease_callback(self.parent_id, child_key, False)
        self._active_children.clear()
        with contextlib.suppress(Exception):
            await self._publish_activity(False)
        self._ready = False

    async def _set_native_epoch(self, native_epoch: str | None) -> None:
        if native_epoch == self._native_epoch:
            await self._release_inactive_epoch_children()
            return
        reactivating = native_epoch is not None and native_epoch == self._reactivation_epoch
        if (
            native_epoch is not None
            and native_epoch in self._historical_native_epochs
            and not reactivating
        ):
            raise RuntimeError("harness reused a retired ACP native epoch")
        previous_epoch = self._native_epoch
        if previous_epoch is not None:
            self._historical_native_epochs.add(previous_epoch)
        if reactivating:
            self._historical_native_epochs.discard(native_epoch)
        self._native_epoch = native_epoch
        self._refresh_active_origins()
        await self._release_inactive_epoch_children()
        self._association_changed.set()

    def _refresh_active_origins(self) -> None:
        self._child_origin_tokens.clear()
        native_epoch = self._native_epoch
        if native_epoch is None:
            return
        for record in self._associations_by_source.values():
            if record.get("native_epoch") != native_epoch:
                continue
            data = record.get("data")
            origin = data.get("_omnigent_action_origin") if isinstance(data, dict) else None
            target_session_id = data.get("target_session_id") if isinstance(data, dict) else None
            if (
                not isinstance(target_session_id, str)
                or not target_session_id
                or not isinstance(origin, dict)
                or not isinstance(origin.get("token"), str)
                or not origin["token"]
            ):
                continue
            child_key = acp_session_child_key(self.parent_id, native_epoch, target_session_id)
            self._child_origin_tokens[child_key] = origin["token"]

    async def _release_inactive_epoch_children(self) -> None:
        changed = False
        for child_key in tuple(self._active_children):
            if self._child_native_epochs.get(child_key) == self._native_epoch:
                continue
            await self._lease_callback(self.parent_id, child_key, False)
            self._active_children.discard(child_key)
            changed = True
        if changed:
            await self._activity_callback(self.parent_id, bool(self._active_children))

    async def _get_capabilities(self) -> dict[str, Any]:
        response = await self._harness_client.get(
            f"/v1/sessions/{quote(self.parent_id, safe='')}/reports/capabilities",
            timeout=10.0,
        )
        response.raise_for_status()
        value = response.json()
        if (
            not isinstance(value, dict)
            or value.get("protocol") != _PROTOCOL
            or value.get("version") != _VERSION
            or value.get("subscriber_generation") != self.generation
        ):
            raise RuntimeError("harness returned incompatible ACP report capabilities")
        if "native_epoch" not in value:
            raise RuntimeError("harness returned ACP report capabilities without a native epoch")
        native_epoch = value.get("native_epoch")
        if native_epoch is not None and (not isinstance(native_epoch, str) or not native_epoch):
            raise RuntimeError("harness returned an invalid active native epoch")
        journal_id = _required_string(value.get("journal_id"), "journal_id", max_length=64)
        if len(journal_id) != 32 or any(
            character not in "0123456789abcdef" for character in journal_id
        ):
            raise RuntimeError("harness returned an invalid ACP report journal identity")
        epoch_ordinal_raw = value.get("epoch_ordinal")
        epoch_ordinal = (
            None
            if epoch_ordinal_raw is None
            else _positive_int(epoch_ordinal_raw, "epoch ordinal")
        )
        if (native_epoch is None) != (epoch_ordinal is None):
            raise RuntimeError("harness returned an incomplete ACP epoch identity")
        features = value.get("features")
        if not isinstance(features, list):
            raise RuntimeError("harness returned malformed ACP report features")
        proven_features = frozenset(feature for feature in features if isinstance(feature, str))
        if not _REQUIRED_FEATURES.issubset(proven_features):
            raise RuntimeError("harness lacks required standard ACP report features")
        self._journal_id = journal_id
        self._epoch_ordinal = epoch_ordinal
        await self._admit_delivery_owner(journal_id)
        if native_epoch is None and self._native_epoch is not None:
            # A transient null capability is not authority to clear the epoch
            # that this same admitted owner already activated.
            return value
        await self._set_native_epoch(native_epoch)
        if native_epoch is not None and epoch_ordinal is not None:
            await self._activate_delivery_epoch(journal_id, native_epoch, epoch_ordinal)
            if native_epoch == self._reactivation_epoch:
                self._reactivation_epoch = None
        self._features = proven_features
        return value

    async def _admit_delivery_owner(self, journal_id: str) -> None:
        admission_id = self._connection_admission_id
        incarnation = self._runtime_incarnation
        if admission_id is None or incarnation is None:
            raise RuntimeError("ACP report delivery has no server-admitted runtime owner")
        grant = (admission_id, incarnation, journal_id)
        if grant == self._delivery_grant:
            return
        async with self._delivery_grant_lock:
            if grant == self._delivery_grant:
                return
            response = await self._server_client.post(
                f"/v1/sessions/{quote(self.parent_id, safe='')}/events",
                json={
                    "type": "external_acp_report_owner_admit",
                    "data": {
                        "connection_admission_id": admission_id,
                        "runtime_incarnation": incarnation,
                        "journal_id": journal_id,
                    },
                },
                timeout=30.0,
            )
            response.raise_for_status()
            self._delivery_grant = grant
            self._active_epoch_grant = None

    async def _activate_delivery_epoch(
        self, journal_id: str, native_epoch: str, epoch_ordinal: int
    ) -> None:
        admission_id = self._connection_admission_id
        incarnation = self._runtime_incarnation
        if admission_id is None or incarnation is None:
            raise RuntimeError("ACP report epoch has no admitted runtime owner")
        grant = (admission_id, incarnation, journal_id, epoch_ordinal, native_epoch)
        if grant == self._active_epoch_grant:
            return
        response = await self._server_client.post(
            f"/v1/sessions/{quote(self.parent_id, safe='')}/events",
            json={
                "type": "external_acp_report_epoch_activate",
                "data": {
                    "connection_admission_id": admission_id,
                    "runtime_incarnation": incarnation,
                    "journal_id": journal_id,
                    "native_epoch": native_epoch,
                    "epoch_ordinal": epoch_ordinal,
                },
            },
            timeout=30.0,
        )
        response.raise_for_status()
        self._active_epoch_grant = grant

    async def _bind_historical_delivery_epoch(
        self, journal_id: str, native_epoch: str, epoch_ordinal: int
    ) -> None:
        admission_id = self._connection_admission_id
        incarnation = self._runtime_incarnation
        if admission_id is None or incarnation is None:
            raise RuntimeError("ACP report history has no admitted runtime owner")
        grant = (admission_id, incarnation, journal_id, epoch_ordinal, native_epoch)
        if grant in self._historical_epoch_bindings:
            return
        async with self._historical_epoch_binding_lock:
            if grant in self._historical_epoch_bindings:
                return
            response = await self._server_client.post(
                f"/v1/sessions/{quote(self.parent_id, safe='')}/events",
                json={
                    "type": "external_acp_report_epoch_activate",
                    "data": {
                        "connection_admission_id": admission_id,
                        "runtime_incarnation": incarnation,
                        "journal_id": journal_id,
                        "native_epoch": native_epoch,
                        "epoch_ordinal": epoch_ordinal,
                        "mode": "historical",
                    },
                },
                timeout=30.0,
            )
            response.raise_for_status()
            self._historical_epoch_bindings.add(grant)

    async def _poll(self) -> None:
        delay = _RETRY_INITIAL_S
        while not self._stopped.is_set():
            try:
                await self._read_available_reports(wait_ms=_WAIT_MS)
                delay = _RETRY_INITIAL_S
                if self._source_failed:
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                _logger.exception(
                    "ACP session-report delivery failed",
                    extra={"session_id": self.parent_id},
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), timeout=delay)
                delay = min(delay * 2, _RETRY_MAX_S)

    async def _read_available_reports(
        self, *, wait_ms: int, notify_source_failure: bool = True
    ) -> None:
        await self._get_capabilities()
        while True:
            response = await self._harness_client.get(
                f"/v1/sessions/{quote(self.parent_id, safe='')}/reports",
                params={
                    "after": self._read_through,
                    "subscriber_generation": self.generation,
                    "limit": _READ_LIMIT,
                    "wait_ms": wait_ms,
                },
                timeout=wait_ms / 1000 + 10.0,
            )
            response.raise_for_status()
            page = response.json()
            records = self._validate_page(page)
            for record in records:
                sequence = record["sequence"]
                if sequence != self._read_through + 1:
                    raise RuntimeError("harness returned a non-contiguous ACP report prefix")
                self._pending[sequence] = record
                if len(self._pending) > _MAX_PENDING_RECORDS:
                    raise RuntimeError("ACP report backlog exceeded its configured limit")
                await self._forward_record(record)
                self._read_through = sequence
            await self._ack_ready_prefix()
            source_status = page.get("source_status") if isinstance(page, dict) else None
            if isinstance(source_status, dict) and source_status.get("state") == "failed":
                self._mark_source_failed(source_status.get("error"))
            if not records or page.get("more") is not True:
                if self._source_failed and not self._source_failure_settled:
                    await self._settle_source_failure(
                        self._source_failure_reason or "ACP report source failed."
                    )
                    if notify_source_failure and self._source_failure_callback is not None:
                        await self._source_failure_callback(
                            self.parent_id,
                            self.generation,
                            self._source_failure_reason or "ACP report source failed.",
                        )
                return
            wait_ms = 0

    def _validate_page(self, page: Any) -> list[dict[str, Any]]:
        if (
            not isinstance(page, dict)
            or page.get("protocol") != _PROTOCOL
            or page.get("version") != _VERSION
            or page.get("subscriber_generation") != self.generation
        ):
            raise RuntimeError("harness returned an incompatible ACP session-report page")
        features = page.get("features")
        if not isinstance(features, list) or not _REQUIRED_FEATURES.issubset(
            {feature for feature in features if isinstance(feature, str)}
        ):
            raise RuntimeError("harness returned an incomplete ACP report feature set")
        records = page.get("records")
        if not isinstance(records, list):
            raise RuntimeError("harness returned malformed ACP report records")
        return [self._validate_record(record) for record in records]

    def _validate_record(self, record: Any) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise RuntimeError("harness returned a malformed ACP session-report record")
        _positive_int(record.get("sequence"), "journal sequence")
        journal_id = _required_string(record.get("journal_id"), "journal_id", max_length=64)
        _positive_int(record.get("epoch_ordinal"), "epoch ordinal")
        ingress_sequence = _positive_int(record.get("ingress_sequence"), "ingress sequence")
        native_epoch = _required_string(record.get("native_epoch"), "native_epoch", max_length=128)
        source_id = _required_string(record.get("source_id"), "source_id", max_length=69)
        kind = record.get("kind")
        data = record.get("data")
        if not isinstance(kind, str) or kind not in _REPORT_KINDS or not isinstance(data, dict):
            raise RuntimeError("harness returned an invalid ACP report kind or data object")
        for name, limit in (
            ("server_identity", 512),
            ("root_conversation_id", 512),
            ("context_generation", 256),
            ("root_native_session_id", 512),
            ("target_session_id", 512),
        ):
            _required_string(data.get(name), name, max_length=limit)
        parent_session_id = data.get("parent_session_id")
        if parent_session_id is not None:
            _required_string(parent_session_id, "parent_session_id", max_length=512)
        if data["root_conversation_id"] != self.parent_id:
            raise RuntimeError("harness report belongs to a different root conversation")
        if journal_id != self._journal_id:
            raise RuntimeError("harness report belongs to a different journal lineage")
        if not isinstance(data.get("operation"), dict):
            raise RuntimeError("harness report has no operation object")
        if source_id != _source_id(
            data["server_identity"],
            data["root_conversation_id"],
            native_epoch,
            ingress_sequence,
        ):
            raise RuntimeError("harness report has an invalid source identity")
        origin = data.get("_omnigent_action_origin")
        root_tool_reference_kind = acp_standard_root_tool_reference_kind(kind, data)
        operation = data["operation"]
        root_tool_reference_target = (
            kind == "tool.references"
            and data.get("target_session_id") == data.get("root_native_session_id")
            and parent_session_id is None
        )
        if root_tool_reference_target and root_tool_reference_kind is None:
            raise RuntimeError("harness root tool reference operation is malformed")
        if root_tool_reference_kind == "identity_only" and origin is None:
            raise RuntimeError("harness identity-only root tool anchor has no origin")
        if origin is not None and (
            (kind != "association.patch" and root_tool_reference_kind is None)
            or not isinstance(origin, dict)
            or set(origin) != {"token", "agent", "generation"}
            or not isinstance(origin.get("token"), str)
            or not origin["token"]
            or not isinstance(origin.get("generation"), str)
            or not origin["generation"]
            or (origin.get("agent") is not None and not isinstance(origin.get("agent"), str))
        ):
            raise RuntimeError("harness report has a malformed private action origin")
        if (
            root_tool_reference_kind is not None
            and origin is not None
            and (
                len(origin["token"]) != 32
                or any(char not in "0123456789abcdef" for char in origin["token"])
            )
        ):
            raise RuntimeError("harness root tool reference has an invalid origin token")
        if kind == "association.patch" and (
            operation.get("session_id") != data["target_session_id"]
            or operation.get("parent_session_id") != parent_session_id
            or not isinstance(operation.get("patch"), dict)
        ):
            raise RuntimeError("harness association operation conflicts with its envelope")
        return record

    async def _forward_record(self, record: dict[str, Any]) -> None:
        sequence = record["sequence"]
        if sequence <= self._acknowledged or sequence in self._processed:
            return
        native_epoch = record["native_epoch"]
        if (
            native_epoch != self._native_epoch
            and native_epoch not in self._historical_native_epochs
        ):
            await self._get_capabilities()
            if native_epoch != self._native_epoch:
                self._historical_native_epochs.add(native_epoch)
        if native_epoch != self._native_epoch:
            await self._bind_historical_delivery_epoch(
                record["journal_id"], native_epoch, record["epoch_ordinal"]
            )
        data = record["data"]
        target_session_id = data["target_session_id"]
        conversation_id = self.parent_id
        child_key: str | None = None
        if record["kind"] == "association.patch":
            child_key = acp_session_child_key(self.parent_id, native_epoch, target_session_id)
        elif target_session_id != data["root_native_session_id"]:
            child_key = self._native_child_keys.get((native_epoch, target_session_id))
            if child_key is None:
                raise RuntimeError("ACP report target has no validated association")
            conversation_id = self._child_ids.get(child_key, "")
            if not conversation_id:
                raise RuntimeError("ACP report target has no persisted child conversation")

        report_payload = {
            "sequence": sequence,
            "journal_id": record["journal_id"],
            "epoch_ordinal": record["epoch_ordinal"],
            "native_epoch": native_epoch,
            "ingress_sequence": record["ingress_sequence"],
            "source_id": record["source_id"],
            "kind": record["kind"],
            "connection_admission_id": self._connection_admission_id,
            "runtime_incarnation": self._runtime_incarnation,
            **data,
        }
        response = await self._server_client.post(
            f"/v1/sessions/{quote(self.parent_id, safe='')}/events",
            json={"type": "external_acp_session_report", "data": report_payload},
            timeout=30.0,
        )
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict) or result.get("durable") is not True:
            raise RuntimeError("server did not durably acknowledge the ACP report operation")
        persisted_child_id = result.get("child_session_id")
        if record["kind"] == "association.patch":
            if not isinstance(persisted_child_id, str) or not persisted_child_id:
                raise RuntimeError("server did not bind the ACP association to a child")
            assert child_key is not None
            existing_child_id = self._child_ids.get(child_key)
            if existing_child_id is not None and existing_child_id != persisted_child_id:
                raise RuntimeError("server changed the child conversation for an ACP association")
            self._child_ids[child_key] = persisted_child_id
            self._native_child_keys[(native_epoch, target_session_id)] = child_key
            self._child_native_epochs[child_key] = native_epoch
            self._child_id_available.set()
        elif persisted_child_id is not None and persisted_child_id != conversation_id:
            raise RuntimeError("server applied an ACP report to a different child conversation")

        await self._apply_observed_activity(record, child_key)
        self._note_activity(self.parent_id)
        self._processed.add(sequence)

    async def _apply_observed_activity(
        self, record: Mapping[str, Any], child_key: str | None
    ) -> None:
        if (
            child_key is None
            or self._replaying_startup_prefix
            or record.get("native_epoch") != self._native_epoch
        ):
            return
        data = record["data"]
        kind = record["kind"]
        operation = data["operation"]
        state_value: object = None
        if kind == "observation.patch":
            observation = operation.get("observation")
            state = observation.get("state") if isinstance(observation, dict) else None
            if isinstance(state, dict) and state.get("present") is True:
                state_value = state.get("value")
        elif kind == "association.patch":
            patch = operation.get("patch")
            state_value = patch.get("state") if isinstance(patch, dict) else None
        state_name = (
            state_value.get("state", state_value.get("kind"))
            if isinstance(state_value, dict)
            else state_value
        )
        if state_name not in {"running", "idle"}:
            return
        active = state_name == "running"
        was_active = child_key in self._active_children
        if active == was_active:
            return
        if active:
            self._active_children.add(child_key)
        else:
            self._active_children.discard(child_key)
        await self._lease_callback(self.parent_id, child_key, active)
        await self._activity_callback(self.parent_id, bool(self._active_children))

    async def _ack_ready_prefix(self) -> None:
        through = self._acknowledged
        while through + 1 in self._processed:
            through += 1
        if through == self._acknowledged:
            return
        response = await self._harness_client.post(
            f"/v1/sessions/{quote(self.parent_id, safe='')}/reports/ack",
            json={"through_sequence": through, "subscriber_generation": self.generation},
            timeout=10.0,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("acknowledged_sequence") != through:
            raise RuntimeError("harness did not confirm the ACP report acknowledgment")
        for sequence in range(self._acknowledged + 1, through + 1):
            record = self._pending.get(sequence)
            if record is not None and record.get("kind") == "association.patch":
                source_id = record["source_id"]
                self._associations_by_source[source_id] = record
                self._restored_association_sources.pop(source_id, None)
            self._processed.discard(sequence)
            self._pending.pop(sequence, None)
        self._acknowledged = through
        self._refresh_active_origins()
        self._association_changed.set()

    async def _settle_source_failure(self, reason: str) -> None:
        if self._source_failure_settled:
            return
        for child_key in tuple(self._active_children):
            with contextlib.suppress(Exception):
                await self._lease_callback(self.parent_id, child_key, False)
        self._active_children.clear()
        with contextlib.suppress(Exception):
            await self._activity_callback(self.parent_id, False)
        self._source_failure_reason = reason
        self._source_failure_settled = True

    async def _publish_activity(self, _running: bool) -> None:
        await self._activity_callback(self.parent_id, bool(self._active_children))

    def _mark_source_failed(self, reason: object) -> None:
        self._source_failed = True
        self._source_failure_reason = (
            reason if isinstance(reason, str) and reason else "ACP report source failed."
        )


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 2**63 - 1:
        raise RuntimeError(f"harness returned an invalid ACP {name}")
    return value


def _nonnegative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"harness returned an invalid ACP {name}")
    return value


async def ensure_session_report_capability(
    *,
    parent_id: str,
    harness_client: httpx.AsyncClient,
    generation: str,
    require_full_reporting: bool = False,
) -> bool:
    """Probe optional v3 support; only explicitly required reporting fails closed."""
    try:
        response = await harness_client.get(
            f"/v1/sessions/{quote(parent_id, safe='')}/reports/capabilities", timeout=10.0
        )
        if response.status_code in {404, 501}:
            if require_full_reporting:
                raise RuntimeError("configured ACP reporting requires private report protocol v3")
            return False
        response.raise_for_status()
    except httpx.RequestError as exc:
        raise RuntimeError("could not verify ACP report-channel capabilities") from exc
    except httpx.HTTPStatusError as exc:
        raise RuntimeError("ACP report-channel capability request failed") from exc
    try:
        capabilities = response.json()
    except ValueError as exc:
        raise RuntimeError("harness returned malformed ACP report capabilities") from exc
    if isinstance(capabilities, dict) and capabilities.get("subscriber_generation") != generation:
        raise RuntimeError("harness returned a stale ACP report capability generation")
    supported = (
        isinstance(capabilities, dict)
        and capabilities.get("protocol") == _PROTOCOL
        and capabilities.get("version") == _VERSION
        and capabilities.get("subscriber_generation") == generation
        and isinstance(capabilities.get("features"), list)
        and _REQUIRED_FEATURES.issubset(
            {feature for feature in capabilities["features"] if isinstance(feature, str)}
        )
    )
    if not supported and require_full_reporting:
        raise RuntimeError("configured ACP reporting requires private report protocol v3")
    return bool(supported)
