"""Durable, session-scoped reports for the private harness control channel."""

from __future__ import annotations

import contextlib
import errno
import hashlib
import json
import os
import re
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

from filelock import FileLock
from filelock import Timeout as FileLockTimeout
from pydantic import BaseModel, ConfigDict, Field, StrictInt, ValidationError

from omnigent.inner.acp_standard_subagents import acp_standard_root_tool_reference_kind
from omnigent.inner.executor import (
    SESSION_REPORT_FEATURES,
    SESSION_REPORT_PROTOCOL_VERSION,
    SessionReport,
    SessionReportAppendResult,
    SessionReportPage,
    SessionReportRecord,
    SessionReportSink,
)
from omnigent.process_logging import data_dir

_SCHEMA_VERSION = 3
_MAX_EPOCH_ORDINAL = 2**63 - 1
_LOCK_TIMEOUT_S = 1.0
_MAX_KIND_LENGTH = 128
_MAX_SOURCE_ID_LENGTH = 256
_MAX_NATIVE_EPOCH_LENGTH = 128
_MAX_NATIVE_SESSION_ID_LENGTH = 512
_MAX_CONTEXT_GENERATION_LENGTH = 256
_MAX_INGRESS_SEQUENCE = 2**63 - 1
_MAX_REPORT_DATA_BYTES = 512 * 1024
# Shared with the runner's startup replay bound. The journal's separate default
# 32 MiB serialized-size cap bounds the retained startup payload.
SESSION_REPORT_MAX_RECORDS = 10_000
_DEFAULT_MAX_REPORTS = SESSION_REPORT_MAX_RECORDS
_DEFAULT_MAX_SEEN_IDS = 50_000
_DEFAULT_MAX_JOURNAL_BYTES = 32 * 1024 * 1024
_KIND_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
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


class SessionReportJournalError(RuntimeError):
    """Base class for explicit report-journal failures."""


class SessionReportJournalBusy(SessionReportJournalError):
    """The journal lock could not be acquired before its deadline."""


class SessionReportJournalCorrupt(SessionReportJournalError):
    """The persisted report journal is malformed or belongs to another scope."""


class SessionReportJournalVersionMismatch(SessionReportJournalError):
    """The durable report journal uses an incompatible private schema version."""


class SessionReportJournalFenced(SessionReportJournalError):
    """This journal instance no longer owns the session report stream."""


class SessionReportJournalConflict(SessionReportJournalError):
    """A report identity, cursor, or acknowledgment conflicts with durable state."""


class SessionReportJournalCapacity(SessionReportJournalError):
    """The journal reached a configured storage limit."""


class SessionReportJournalUnavailable(SessionReportJournalError):
    """The journal has not been initialized by an explicit owner claim."""


class _EpochBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    epoch_ordinal: StrictInt = Field(ge=1, le=_MAX_EPOCH_ORDINAL)
    native_session_id: str | None = None
    context_generation: str | None = None
    next_ingress_sequence: StrictInt = Field(default=1, ge=1, le=_MAX_INGRESS_SEQUENCE + 1)


class _StoredReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: StrictInt = Field(ge=1)
    native_epoch: str
    ingress_sequence: StrictInt = Field(ge=1, le=_MAX_INGRESS_SEQUENCE)
    source_id: str
    kind: str
    data: dict[str, object]


class _SeenSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: StrictInt = Field(ge=1)
    payload_hash: str
    native_epoch: str
    ingress_sequence: StrictInt = Field(ge=1, le=_MAX_INGRESS_SEQUENCE)
    source_id: str


class _JournalState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[3] = 3
    parent_session_id: str
    server_identity_hash: str
    subscriber_generation: str
    journal_id: str
    next_epoch_ordinal: StrictInt = Field(ge=1, le=_MAX_EPOCH_ORDINAL + 1)
    next_sequence: StrictInt = Field(default=1, ge=1)
    acknowledged_sequence: StrictInt = Field(default=0, ge=0)
    exposed_sequence: StrictInt = Field(default=0, ge=0)
    native_epochs: dict[str, _EpochBinding] = Field(default_factory=dict)
    reports: list[_StoredReport] = Field(default_factory=list)
    seen_sources: dict[str, _SeenSource] = Field(default_factory=dict)


class SessionReportJournal(SessionReportSink):
    """Single-owner append-only report journal stored under the runtime data dir.

    File names contain only hashes of the server identity and parent session.
    Mutations use a process-independent lock and an fsync/replace write. A new
    subscriber generation may take ownership explicitly; reads never claim an
    owner as a side effect.
    """

    protocol_version = SESSION_REPORT_PROTOCOL_VERSION
    features = SESSION_REPORT_FEATURES

    def __init__(
        self,
        parent_session_id: str,
        *,
        server_identity: str,
        subscriber_generation: str,
        root: str | Path | None = None,
        max_reports: int = _DEFAULT_MAX_REPORTS,
        max_seen_ids: int = _DEFAULT_MAX_SEEN_IDS,
        max_native_epochs: int = _DEFAULT_MAX_SEEN_IDS,
        max_journal_bytes: int = _DEFAULT_MAX_JOURNAL_BYTES,
    ) -> None:
        if not parent_session_id or len(parent_session_id) > 512:
            raise ValueError("parent_session_id must contain 1 to 512 characters")
        if not server_identity:
            raise ValueError("server_identity must not be empty")
        if not subscriber_generation or len(subscriber_generation) > 256:
            raise ValueError("subscriber_generation must contain 1 to 256 characters")
        if (
            max_reports < 1
            or max_seen_ids < 1
            or max_native_epochs < 1
            or max_journal_bytes < 1024
        ):
            raise ValueError("journal capacities must be positive")

        self.parent_session_id = parent_session_id
        self.server_identity = server_identity
        self.subscriber_generation = subscriber_generation
        self.server_identity_hash = hashlib.sha256(server_identity.encode("utf-8")).hexdigest()
        self._max_reports = max_reports
        self._max_seen_ids = max_seen_ids
        self._max_native_epochs = max_native_epochs
        self._max_journal_bytes = max_journal_bytes
        root_path = (
            Path(root).expanduser() if root is not None else data_dir() / "acp-subagent-reports"
        )
        self.path = (
            root_path
            / self.server_identity_hash
            / f"{hashlib.sha256(parent_session_id.encode('utf-8')).hexdigest()}.json"
        )
        self._closed = False

    @property
    def root_conversation_id(self) -> str:
        """Return the Omnigent root id under the report contract name."""
        return self.parent_session_id

    def acquire_owner(self) -> None:
        """Create the journal or fence an earlier subscriber generation."""
        self._ensure_open()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            with FileLock(str(self.path) + ".lock", timeout=_LOCK_TIMEOUT_S):
                is_new = not self.path.exists()
                state = self._read_or_create(allow_missing=True)
                self._validate_scope(state)
                self._validate_state(state)
                if is_new or state.subscriber_generation != self.subscriber_generation:
                    state.subscriber_generation = self.subscriber_generation
                    # A replacement must replay from the durable acknowledgment,
                    # not trust the previous subscriber's in-memory cursor.
                    state.exposed_sequence = state.acknowledged_sequence
                    self._write(state)
        except FileLockTimeout as exc:
            raise SessionReportJournalBusy("report journal is busy") from exc
        except OSError as exc:
            raise SessionReportJournalError("report journal ownership claim failed") from exc

    @property
    def journal_id(self) -> str:
        """Return the immutable persisted identity of this journal lineage."""
        with self._locked() as state:
            return state.journal_id

    def epoch_ordinal(self, native_epoch: str) -> int:
        """Return the ordinal allocated for a previously started native epoch."""
        self._validate_native_epoch(native_epoch)
        with self._locked() as state:
            binding = state.native_epochs.get(native_epoch)
            if binding is None:
                raise SessionReportJournalConflict("native epoch was not started")
            return binding.epoch_ordinal

    def identity_for_epoch(self, native_epoch: str | None) -> tuple[str, int | None]:
        """Return the journal lineage and optional bound ordinal atomically."""
        if native_epoch is not None:
            self._validate_native_epoch(native_epoch)
        with self._locked() as state:
            if native_epoch is None:
                return state.journal_id, None
            binding = state.native_epochs.get(native_epoch)
            if binding is None:
                raise SessionReportJournalConflict("native epoch was not started")
            return state.journal_id, binding.epoch_ordinal

    def begin_native_epoch(self, native_epoch: str) -> None:
        """Persist a unique epoch ordinal before ACP session creation."""
        self._validate_native_epoch(native_epoch)
        with self._locked() as state:
            if native_epoch in state.native_epochs:
                raise SessionReportJournalConflict("native epoch has already been used")
            if len(state.native_epochs) >= self._max_native_epochs:
                raise SessionReportJournalCapacity("native epoch limit reached")
            ordinal = state.next_epoch_ordinal
            if ordinal > _MAX_EPOCH_ORDINAL:
                raise SessionReportJournalCapacity("native epoch ordinal limit reached")
            state.next_epoch_ordinal = ordinal + 1
            state.native_epochs[native_epoch] = _EpochBinding(epoch_ordinal=ordinal)
            self._write(state)

    def bind_native_session(self, native_epoch: str, native_session_id: str) -> None:
        """Bind the server-assigned ACP session id to its previously saved nonce."""
        self._validate_native_epoch(native_epoch)
        if not native_session_id or len(native_session_id) > _MAX_NATIVE_SESSION_ID_LENGTH:
            raise ValueError("native_session_id must contain 1 to 512 characters")
        with self._locked() as state:
            binding = state.native_epochs.get(native_epoch)
            if binding is None:
                raise SessionReportJournalConflict("native epoch was not started")
            if binding.native_session_id is not None:
                if binding.native_session_id == native_session_id:
                    return
                raise SessionReportJournalConflict("native epoch is already bound")
            binding.native_session_id = native_session_id
            self._write(state)

    def append(self, report: SessionReport) -> SessionReportAppendResult:
        """Persist a report before it can be returned by the read route."""
        self._validate_report(report)
        canonical_data = self._canonical_json(report.data)
        if len(canonical_data) > _MAX_REPORT_DATA_BYTES:
            raise SessionReportJournalCapacity("report data exceeds the per-record limit")
        source_key = self._source_key(report.native_epoch, report.source_id)
        with self._locked() as state:
            binding = state.native_epochs.get(report.native_epoch)
            if binding is None or binding.native_session_id is None:
                raise SessionReportJournalConflict("report native epoch is not bound")
            if binding.context_generation is None:
                binding.context_generation = report.data["context_generation"]
            self._validate_bound_identity(report, binding)
            payload_hash = self._payload_hash(
                state.journal_id,
                binding.epoch_ordinal,
                report.native_epoch,
                report.ingress_sequence,
                report.source_id,
                report.kind,
                report.data,
            )

            # The acknowledgment is committed before its payload-pruning write.
            # Finish any interrupted pruning before applying the pending-report
            # capacity limit, while retaining seen_sources as replay tombstones.
            unacknowledged = [
                stored for stored in state.reports if stored.sequence > state.acknowledged_sequence
            ]
            if len(unacknowledged) != len(state.reports):
                state.reports = unacknowledged
                self._write(state)

            previous = state.seen_sources.get(source_key)
            if previous is not None:
                if previous.payload_hash != payload_hash:
                    raise SessionReportJournalConflict(
                        "source_id was replayed with different report content"
                    )
                return SessionReportAppendResult(
                    record=SessionReportRecord(
                        sequence=previous.sequence,
                        journal_id=state.journal_id,
                        native_epoch=report.native_epoch,
                        epoch_ordinal=binding.epoch_ordinal,
                        ingress_sequence=report.ingress_sequence,
                        source_id=report.source_id,
                        kind=report.kind,
                        data=dict(report.data),
                    ),
                    inserted=False,
                )
            if binding.next_ingress_sequence > _MAX_INGRESS_SEQUENCE:
                raise SessionReportJournalCapacity("native epoch ingress sequence limit reached")
            if report.ingress_sequence != binding.next_ingress_sequence:
                raise SessionReportJournalConflict(
                    "ACP ingress sequence is not the next contiguous value for this native epoch"
                )
            if len(state.reports) >= self._max_reports:
                raise SessionReportJournalCapacity("pending report limit reached")
            if len(state.seen_sources) >= self._max_seen_ids:
                raise SessionReportJournalCapacity("report identity limit reached")

            sequence = state.next_sequence
            state.next_sequence += 1
            stored = _StoredReport(
                sequence=sequence,
                native_epoch=report.native_epoch,
                ingress_sequence=report.ingress_sequence,
                source_id=report.source_id,
                kind=report.kind,
                data=dict(report.data),
            )
            state.reports.append(stored)
            state.seen_sources[source_key] = _SeenSource(
                sequence=sequence,
                payload_hash=payload_hash,
                native_epoch=report.native_epoch,
                ingress_sequence=report.ingress_sequence,
                source_id=report.source_id,
            )
            binding.next_ingress_sequence += 1
            self._write(state)
            return SessionReportAppendResult(record=self._record(stored, state), inserted=True)

    def get_after(
        self,
        after_sequence: int,
        *,
        subscriber_generation: str,
        limit: int = 100,
    ) -> SessionReportPage:
        """Persist exposure and return a contiguous page after a known cursor."""
        self._validate_sequence(after_sequence, "after_sequence")
        self._validate_generation(subscriber_generation)
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._locked(subscriber_generation=subscriber_generation) as state:
            if after_sequence > state.exposed_sequence:
                raise SessionReportJournalConflict("report cursor skips unexposed records")
            cursor = max(after_sequence, state.acknowledged_sequence)
            pending = [report for report in state.reports if report.sequence > cursor]
            batch = pending[:limit]
            if batch:
                state.exposed_sequence = max(state.exposed_sequence, batch[-1].sequence)
                # The exposure frontier must survive a lost HTTP response so an
                # acknowledgment cannot authorize records that were never read.
                self._write(state)
            return SessionReportPage(
                records=tuple(self._record(report, state) for report in batch),
                highest_sequence=state.next_sequence - 1,
                exposed_through=state.exposed_sequence,
                more=len(pending) > len(batch),
            )

    def ack(self, through_sequence: int, *, subscriber_generation: str) -> int:
        """Persist a monotonic acknowledgment, then prune acknowledged payloads."""
        self._validate_sequence(through_sequence, "through_sequence")
        self._validate_generation(subscriber_generation)
        with self._locked(subscriber_generation=subscriber_generation) as state:
            if through_sequence <= state.acknowledged_sequence:
                # An earlier ACK may have been persisted even though the
                # following prune write failed. A retry must finish that prune.
                unacknowledged = [
                    report
                    for report in state.reports
                    if report.sequence > state.acknowledged_sequence
                ]
                if len(unacknowledged) != len(state.reports):
                    state.reports = unacknowledged
                    self._write(state)
                return state.acknowledged_sequence
            if through_sequence > state.exposed_sequence:
                raise SessionReportJournalConflict("acknowledgment exceeds exposed reports")

            state.acknowledged_sequence = through_sequence
            # Commit the acknowledgment first. If pruning then fails, recovery
            # can safely skip these records because the acknowledgment is durable.
            self._write(state)
            state.reports = [
                report for report in state.reports if report.sequence > through_sequence
            ]
            self._write(state)
            return state.acknowledged_sequence

    def snapshot(self, *, subscriber_generation: str) -> tuple[int, int]:
        """Return append and acknowledgment watermarks without claiming ownership."""
        self._validate_generation(subscriber_generation)
        with self._locked(subscriber_generation=subscriber_generation) as state:
            return state.next_sequence - 1, state.acknowledged_sequence

    def close(self) -> None:
        """Disable this local instance without deleting durable reports."""
        self._closed = True

    @contextlib.contextmanager
    def _locked(self, *, subscriber_generation: str | None = None) -> Iterator[_JournalState]:
        self._ensure_open()
        if not self.path.exists():
            raise SessionReportJournalUnavailable("report journal has no explicit owner claim")
        try:
            with FileLock(str(self.path) + ".lock", timeout=_LOCK_TIMEOUT_S):
                if not self.path.exists():
                    raise SessionReportJournalUnavailable(
                        "report journal has no explicit owner claim"
                    )
                state = self._read_or_create()
                self._validate_scope(state)
                self._validate_state(state)
                self._require_owner(state, self.subscriber_generation)
                if (
                    subscriber_generation is not None
                    and subscriber_generation != self.subscriber_generation
                ):
                    raise SessionReportJournalFenced(
                        "request subscriber generation does not match this journal owner"
                    )
                yield state
        except FileLockTimeout as exc:
            raise SessionReportJournalBusy("report journal is busy") from exc
        except OSError as exc:
            raise SessionReportJournalError("report journal lock failed") from exc

    def _read_or_create(self, *, allow_missing: bool = False) -> _JournalState:
        try:
            if self.path.stat().st_size > self._max_journal_bytes:
                raise SessionReportJournalCapacity("report journal byte limit reached")
            raw = self.path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            if not allow_missing:
                raise SessionReportJournalUnavailable(
                    "report journal has no explicit owner claim"
                ) from exc
            return _JournalState(
                parent_session_id=self.parent_session_id,
                server_identity_hash=self.server_identity_hash,
                subscriber_generation=self.subscriber_generation,
                journal_id=uuid.uuid4().hex,
                next_epoch_ordinal=1,
            )
        except OSError as exc:
            raise SessionReportJournalError("report journal could not be read") from exc
        try:
            value = json.loads(raw)
            schema_version = value.get("schema_version") if isinstance(value, dict) else None
            if (
                isinstance(schema_version, int)
                and not isinstance(schema_version, bool)
                and (schema_version != _SCHEMA_VERSION)
            ):
                raise SessionReportJournalVersionMismatch(
                    f"ACP report journal schema {schema_version} is incompatible with "
                    f"schema {_SCHEMA_VERSION}; "
                    "the existing journal was preserved and was not reinterpreted"
                )
            return _JournalState.model_validate(value)
        except SessionReportJournalVersionMismatch:
            raise
        except (ValidationError, ValueError) as exc:
            raise SessionReportJournalCorrupt("report journal is corrupt") from exc

    def _validate_scope(self, state: _JournalState) -> None:
        if (
            state.parent_session_id != self.parent_session_id
            or state.server_identity_hash != self.server_identity_hash
        ):
            raise SessionReportJournalCorrupt("report journal scope does not match its path")

    def _validate_state(self, state: _JournalState) -> None:
        if len(state.journal_id) != 32 or any(
            character not in "0123456789abcdef" for character in state.journal_id
        ):
            raise SessionReportJournalCorrupt("report journal lineage identity is invalid")
        high_water = state.next_sequence - 1
        if high_water > self._max_seen_ids or len(state.seen_sources) > self._max_seen_ids:
            raise SessionReportJournalCapacity("report identity limit reached")
        if len(state.reports) > self._max_reports:
            raise SessionReportJournalCapacity("pending report limit reached")
        if not (state.acknowledged_sequence <= state.exposed_sequence <= high_water):
            raise SessionReportJournalCorrupt("report sequence watermarks are inconsistent")
        report_sequences: set[int] = set()
        report_order: list[int] = []
        for report in state.reports:
            try:
                self._validate_report(
                    SessionReport(
                        native_epoch=report.native_epoch,
                        ingress_sequence=report.ingress_sequence,
                        source_id=report.source_id,
                        kind=report.kind,
                        data=report.data,
                    )
                )
            except ValueError as exc:
                raise SessionReportJournalCorrupt("stored report fields are invalid") from exc
            if report.sequence > high_water or report.sequence in report_sequences:
                raise SessionReportJournalCorrupt("stored report sequence is invalid")
            binding = state.native_epochs.get(report.native_epoch)
            if binding is None or binding.native_session_id is None:
                raise SessionReportJournalCorrupt("report references an unknown native epoch")
            try:
                self._validate_bound_identity(
                    SessionReport(
                        native_epoch=report.native_epoch,
                        ingress_sequence=report.ingress_sequence,
                        source_id=report.source_id,
                        kind=report.kind,
                        data=report.data,
                    ),
                    binding,
                )
            except SessionReportJournalConflict as exc:
                raise SessionReportJournalCorrupt("report epoch identity is inconsistent") from exc
            report_sequences.add(report.sequence)
            report_order.append(report.sequence)
            source_key = self._source_key(report.native_epoch, report.source_id)
            seen = state.seen_sources.get(source_key)
            if seen is None or seen.sequence != report.sequence:
                raise SessionReportJournalCorrupt("stored report identity is inconsistent")
            try:
                expected_hash = self._payload_hash(
                    state.journal_id,
                    binding.epoch_ordinal,
                    report.native_epoch,
                    report.ingress_sequence,
                    report.source_id,
                    report.kind,
                    report.data,
                )
            except ValueError as exc:
                raise SessionReportJournalCorrupt("stored report data is not JSON") from exc
            if seen.payload_hash != expected_hash:
                raise SessionReportJournalCorrupt("stored report content hash is inconsistent")
        if report_order != sorted(report_order):
            raise SessionReportJournalCorrupt("stored reports are out of order")
        seen_sequences: set[int] = set()
        for source_key, seen in state.seen_sources.items():
            try:
                self._validate_native_epoch(seen.native_epoch)
            except ValueError as exc:
                raise SessionReportJournalCorrupt("stored report tombstone is invalid") from exc
            if (
                not seen.source_id
                or len(seen.source_id) > _MAX_SOURCE_ID_LENGTH
                or len(seen.payload_hash) != 64
            ):
                raise SessionReportJournalCorrupt("stored report tombstone is invalid")
            binding = state.native_epochs.get(seen.native_epoch)
            if binding is None or binding.native_session_id is None:
                raise SessionReportJournalCorrupt("report tombstone references an unknown epoch")
            if (
                source_key != self._source_key(seen.native_epoch, seen.source_id)
                or seen.sequence > high_water
                or seen.sequence in seen_sequences
            ):
                raise SessionReportJournalCorrupt("stored report tombstone is inconsistent")
            try:
                self._validate_source_identity(
                    seen.native_epoch, seen.ingress_sequence, seen.source_id
                )
            except ValueError as exc:
                raise SessionReportJournalCorrupt(
                    "stored report source identity is invalid"
                ) from exc
            if seen.ingress_sequence >= binding.next_ingress_sequence:
                raise SessionReportJournalCorrupt("native epoch ingress sequence is inconsistent")
            seen_sequences.add(seen.sequence)
        if sorted(seen_sequences) != list(range(1, state.next_sequence)):
            raise SessionReportJournalCorrupt("report sequence history is incomplete")
        pending_sequences = sorted(
            report.sequence
            for report in state.reports
            if report.sequence > state.acknowledged_sequence
        )
        if pending_sequences != list(range(state.acknowledged_sequence + 1, state.next_sequence)):
            raise SessionReportJournalCorrupt("pending reports are not contiguous")
        if len(state.native_epochs) > self._max_native_epochs:
            raise SessionReportJournalCapacity("native epoch limit reached")
        ordinals: set[int] = set()
        for epoch, native_session in state.native_epochs.items():
            try:
                self._validate_native_epoch(epoch)
            except ValueError as exc:
                raise SessionReportJournalCorrupt("native epoch binding is invalid") from exc
            if native_session.epoch_ordinal in ordinals:
                raise SessionReportJournalCorrupt("native epoch ordinal is reused")
            ordinals.add(native_session.epoch_ordinal)
            if native_session.native_session_id is not None and (
                not native_session.native_session_id
                or len(native_session.native_session_id) > _MAX_NATIVE_SESSION_ID_LENGTH
            ):
                raise SessionReportJournalCorrupt("native-session binding is invalid")
            if native_session.context_generation is not None and (
                not native_session.context_generation
                or len(native_session.context_generation) > _MAX_CONTEXT_GENERATION_LENGTH
            ):
                raise SessionReportJournalCorrupt("native-epoch context binding is invalid")
            if native_session.next_ingress_sequence > _MAX_INGRESS_SEQUENCE + 1:
                raise SessionReportJournalCorrupt("native-epoch ingress sequence is invalid")
            ingress_sequences = sorted(
                seen.ingress_sequence
                for seen in state.seen_sources.values()
                if seen.native_epoch == epoch
            )
            if ingress_sequences != list(range(1, len(ingress_sequences) + 1)) or (
                native_session.next_ingress_sequence != len(ingress_sequences) + 1
            ):
                raise SessionReportJournalCorrupt("native epoch ingress history is incomplete")
        if sorted(ordinals) != list(range(1, len(ordinals) + 1)) or (
            state.next_epoch_ordinal != len(ordinals) + 1
        ):
            raise SessionReportJournalCorrupt("native epoch ordinal history is incomplete")

    @staticmethod
    def _require_owner(state: _JournalState, generation: str) -> None:
        if state.subscriber_generation != generation:
            raise SessionReportJournalFenced("report subscriber generation has been replaced")

    def _write(self, state: _JournalState) -> None:
        self._validate_state(state)
        try:
            payload = state.model_dump_json().encode("utf-8") + b"\n"
        except (TypeError, ValueError) as exc:
            raise SessionReportJournalCorrupt("report journal could not be serialized") from exc
        if len(payload) > self._max_journal_bytes:
            raise SessionReportJournalCapacity("report journal byte limit reached")
        temporary = ""
        try:
            fd, temporary = tempfile.mkstemp(prefix=".acp-reports-", dir=self.path.parent)
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            temporary = ""
            if os.name != "nt":
                try:
                    directory_fd = os.open(self.path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                except OSError as exc:
                    unsupported_directory_sync = {
                        errno.EINVAL,
                        getattr(errno, "ENOTSUP", errno.EINVAL),
                        getattr(errno, "EOPNOTSUPP", errno.EINVAL),
                    }
                    if exc.errno not in unsupported_directory_sync:
                        raise
        except OSError as exc:
            raise SessionReportJournalError("report journal write failed") from exc
        finally:
            if temporary:
                Path(temporary).unlink(missing_ok=True)

    @staticmethod
    def _canonical_json(value: object) -> bytes:
        try:
            return json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("report data must contain only finite JSON values") from exc

    def _validate_report(self, report: SessionReport) -> None:
        self._validate_native_epoch(report.native_epoch)
        if (
            isinstance(report.ingress_sequence, bool)
            or not isinstance(report.ingress_sequence, int)
            or report.ingress_sequence < 1
            or report.ingress_sequence > _MAX_INGRESS_SEQUENCE
        ):
            raise ValueError("ingress_sequence must be a positive bounded integer")
        if not report.source_id or len(report.source_id) > _MAX_SOURCE_ID_LENGTH:
            raise ValueError("source_id must contain 1 to 256 characters")
        if (
            not report.kind
            or len(report.kind) > _MAX_KIND_LENGTH
            or _KIND_PATTERN.fullmatch(report.kind) is None
            or report.kind not in _REPORT_KINDS
        ):
            raise ValueError("kind is not a supported v3 ACP session-report operation")
        if not isinstance(report.data, dict):
            raise ValueError("report data must be a JSON object")
        required_strings = (
            "server_identity",
            "root_conversation_id",
            "context_generation",
            "root_native_session_id",
            "target_session_id",
        )
        for name in required_strings:
            value = report.data.get(name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"report data.{name} must be a non-empty string")
        parent_session_id = report.data.get("parent_session_id")
        if parent_session_id is not None and (
            not isinstance(parent_session_id, str) or not parent_session_id
        ):
            raise ValueError("report data.parent_session_id must be null or a non-empty string")
        if report.data["server_identity"] != self.server_identity:
            raise ValueError("report server identity does not match the trusted journal scope")
        if report.data["root_conversation_id"] != self.parent_session_id:
            raise ValueError("report root conversation does not match the trusted journal scope")
        if len(report.data["context_generation"]) > _MAX_CONTEXT_GENERATION_LENGTH:
            raise ValueError("report context generation exceeds its size limit")
        for name in ("root_native_session_id", "target_session_id"):
            if len(report.data[name]) > _MAX_NATIVE_SESSION_ID_LENGTH:
                raise ValueError(f"report data.{name} exceeds its size limit")
        if (
            parent_session_id is not None
            and len(parent_session_id) > _MAX_NATIVE_SESSION_ID_LENGTH
        ):
            raise ValueError("report data.parent_session_id exceeds its size limit")
        operation = report.data.get("operation")
        if not isinstance(operation, dict):
            raise ValueError("report data.operation must be a JSON object")
        action_origin = report.data.get("_omnigent_action_origin")
        root_tool_reference_kind = acp_standard_root_tool_reference_kind(report.kind, report.data)
        root_tool_reference_target = (
            report.kind == "tool.references"
            and report.data.get("target_session_id") == report.data.get("root_native_session_id")
            and parent_session_id is None
        )
        if root_tool_reference_target and root_tool_reference_kind is None:
            raise ValueError("root tool reference operation is malformed")
        if root_tool_reference_kind == "identity_only" and action_origin is None:
            raise ValueError("identity-only root tool anchor has no origin")
        if action_origin is not None:
            if (
                report.kind != "association.patch" and root_tool_reference_kind is None
            ) or not isinstance(action_origin, dict):
                raise ValueError(
                    "private action origin is only valid on an association or root tool reference"
                )
            if set(action_origin) != {"token", "agent", "generation"}:
                raise ValueError("private action origin has an invalid shape")
            for name in ("token", "generation"):
                value = action_origin.get(name)
                if not isinstance(value, str) or not value or len(value) > 256:
                    raise ValueError(f"private action origin {name} is invalid")
            if root_tool_reference_kind is not None and (
                len(action_origin["token"]) != 32
                or any(char not in "0123456789abcdef" for char in action_origin["token"])
            ):
                raise ValueError("root tool reference origin token is invalid")
            if action_origin.get("agent") is not None and (
                not isinstance(action_origin.get("agent"), str)
                or len(action_origin["agent"]) > 256
            ):
                raise ValueError("private action origin agent is invalid")
        if report.kind == "association.patch":
            if (
                operation.get("session_id") != report.data["target_session_id"]
                or operation.get("parent_session_id") != parent_session_id
            ):
                raise ValueError("association patch identity does not match its report envelope")
        self._validate_source_identity(
            report.native_epoch, report.ingress_sequence, report.source_id
        )

    @staticmethod
    def _validate_bound_identity(report: SessionReport, binding: _EpochBinding) -> None:
        if report.data["root_native_session_id"] != binding.native_session_id:
            raise SessionReportJournalConflict(
                "report root native session does not match its bound epoch"
            )
        context_generation = report.data["context_generation"]
        if binding.context_generation != context_generation:
            raise SessionReportJournalConflict(
                "report context generation changed within a native epoch"
            )

    def _validate_source_identity(
        self, native_epoch: str, ingress_sequence: int, source_id: str
    ) -> None:
        components = (
            self.server_identity.encode("utf-8"),
            self.parent_session_id.encode("utf-8"),
            native_epoch.encode("utf-8"),
            str(ingress_sequence).encode("utf-8"),
        )
        encoded = b"".join(
            len(component).to_bytes(4, "big") + component for component in components
        )
        expected = "acp2:" + hashlib.sha256(encoded).hexdigest()
        if len(source_id) != 69 or source_id != expected:
            raise ValueError("source_id does not match the durable ACP ingress identity")

    @staticmethod
    def _validate_native_epoch(native_epoch: str) -> None:
        if not native_epoch or len(native_epoch) > _MAX_NATIVE_EPOCH_LENGTH:
            raise ValueError("native_epoch must contain 1 to 128 characters")

    @staticmethod
    def _validate_sequence(sequence: int, name: str) -> None:
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise ValueError(f"{name} must be a non-negative integer")

    @staticmethod
    def _validate_generation(generation: str) -> None:
        if not generation or len(generation) > 256:
            raise ValueError("subscriber_generation must contain 1 to 256 characters")

    @staticmethod
    def _source_key(native_epoch: str, source_id: str) -> str:
        payload = f"{len(native_epoch)}:{native_epoch}{source_id}"
        return hashlib.sha256(payload.encode()).hexdigest()

    @classmethod
    def _payload_hash(
        cls,
        journal_id: str,
        epoch_ordinal: int,
        native_epoch: str,
        ingress_sequence: int,
        source_id: str,
        kind: str,
        data: dict[str, object],
    ) -> str:
        return hashlib.sha256(
            cls._canonical_json(
                {
                    "journal_id": journal_id,
                    "epoch_ordinal": epoch_ordinal,
                    "native_epoch": native_epoch,
                    "ingress_sequence": ingress_sequence,
                    "source_id": source_id,
                    "kind": kind,
                    "data": data,
                }
            )
        ).hexdigest()

    @staticmethod
    def _record(report: _StoredReport, state: _JournalState) -> SessionReportRecord:
        binding = state.native_epochs.get(report.native_epoch)
        if binding is None:
            raise SessionReportJournalCorrupt("report references an unknown native epoch")
        return SessionReportRecord(
            sequence=report.sequence,
            journal_id=state.journal_id,
            native_epoch=report.native_epoch,
            epoch_ordinal=binding.epoch_ordinal,
            ingress_sequence=report.ingress_sequence,
            source_id=report.source_id,
            kind=report.kind,
            data=dict(report.data),
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionReportJournalFenced("report journal instance is closed")
