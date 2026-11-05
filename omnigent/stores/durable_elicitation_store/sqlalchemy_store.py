"""SQLAlchemy-backed durable-elicitation store."""

from __future__ import annotations

import json

from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError

from omnigent.db.db_models import SqlDurableElicitation, current_workspace_id
from omnigent.db.enum_codecs import (
    decode_durable_elicitation_action,
    decode_durable_elicitation_state,
    encode_durable_elicitation_action,
    encode_durable_elicitation_state,
)
from omnigent.db.utils import (
    get_or_create_engine,
    make_named_managed_session_maker,
    now_epoch,
)
from omnigent.stores.durable_elicitation_store._model import (
    AnswerConflictError,
    DuplicateRegistrationError,
    DurableElicitation,
)

_TERMINAL_STATES = frozenset({"answered", "declined", "canceled"})


def _to_entity(row: SqlDurableElicitation) -> DurableElicitation:
    """Convert an ORM row to the package's frozen dataclass record."""
    return DurableElicitation(
        id=row.id,
        conversation_id=row.conversation_id,
        target_conversation_id=row.target_conversation_id,
        invocation_id=row.invocation_id,
        fingerprint=row.fingerprint,
        tool_name=row.tool_name,
        message=row.message,
        requested_schema=json.loads(row.requested_schema),
        state=decode_durable_elicitation_state(row.state),
        answer_action=(
            decode_durable_elicitation_action(row.answer_action)
            if row.answer_action is not None
            else None
        ),
        answer_content=(
            json.loads(row.answer_content) if row.answer_content is not None else None
        ),
        delivered_at=row.delivered_at,
        consumed_at=row.consumed_at,
        delivery_attempts=row.delivery_attempts,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class SqlAlchemyDurableElicitationStore:
    """
    SQLAlchemy-backed implementation of the durable-elicitation store.

    Every operation is workspace-scoped (the ``current_workspace_id``
    contextvar binds the tenant partition) and every state transition is
    a single conditional UPDATE/INSERT inside one named managed session,
    so concurrent answers, replays, and consumption claims race safely.
    """

    def __init__(self, storage_location: str) -> None:
        """
        Initialize the store.

        :param storage_location: SQLAlchemy database URI,
            e.g. ``"sqlite:///chat.db"``.
        """
        self.storage_location = storage_location
        self._engine = get_or_create_engine(storage_location)
        self._session = make_named_managed_session_maker(
            self._engine,
            query_name_prefix="omnigent.durable_elicitation_store",
        )

    # ── Registration ────────────────────────────────────────────

    def create_or_get(
        self,
        *,
        elicitation_id: str,
        conversation_id: str,
        invocation_id: str,
        fingerprint: str,
        tool_name: str,
        message: str,
        requested_schema: dict,
        target_conversation_id: str | None = None,
    ) -> tuple[DurableElicitation, bool]:
        """
        Register a durable question, or return the existing one.

        Keyed on workspace + owning session + ``invocation_id``. A fresh
        insert wins the race; a concurrent or replayed registration under
        the same identity lands on the same row. The existing row's
        fingerprint must match — a changed payload under a used identity
        raises :class:`DuplicateRegistrationError` instead of silently
        rewriting the question someone may already see.

        :param elicitation_id: Server-minted stable correlation id.
        :param conversation_id: Owning session id.
        :param invocation_id: Stable caller invocation identity.
        :param fingerprint: Payload fingerprint
            (:func:`question_fingerprint`).
        :param tool_name: Registering tool name.
        :param message: Question text.
        :param requested_schema: Decoded ``requestedSchema``.
        :param target_conversation_id: Presentation session, when the
            question routes to a user-facing parent.
        :returns: ``(record, created)`` — ``created`` is ``True`` only for
            the call that inserted the row.
        :raises DuplicateRegistrationError: The identity is already
            registered with a different payload.
        """
        with self._session("durable_elicitation.create_or_get") as session:
            existing = session.scalars(
                select(SqlDurableElicitation).where(
                    SqlDurableElicitation.workspace_id == current_workspace_id(),
                    SqlDurableElicitation.conversation_id == conversation_id,
                    SqlDurableElicitation.invocation_id == invocation_id,
                )
            ).first()
            if existing is not None:
                record = _to_entity(existing)
                if record.fingerprint != fingerprint:
                    raise DuplicateRegistrationError(existing=record)
                return record, False
            now = now_epoch()
            row = SqlDurableElicitation(
                id=elicitation_id,
                conversation_id=conversation_id,
                target_conversation_id=target_conversation_id,
                invocation_id=invocation_id,
                fingerprint=fingerprint,
                tool_name=tool_name,
                message=message,
                requested_schema=json.dumps(requested_schema, sort_keys=True),
                created_at=now,
            )
            session.add(row)
            try:
                session.flush()
            except IntegrityError:
                # Concurrent create won the unique key; re-read and compare.
                session.rollback()
                raced = session.scalars(
                    select(SqlDurableElicitation).where(
                        SqlDurableElicitation.conversation_id == conversation_id,
                        SqlDurableElicitation.invocation_id == invocation_id,
                    )
                ).first()
                if raced is None:
                    raise
                record = _to_entity(raced)
                if record.fingerprint != fingerprint:
                    raise DuplicateRegistrationError(existing=record) from None
                return record, False
            return _to_entity(row), True

    # ── Reads ───────────────────────────────────────────────────

    def get(self, elicitation_id: str) -> DurableElicitation | None:
        """
        Return one question by id, or ``None`` when unknown, malformed, or
        out-of-workspace.

        :param elicitation_id: The correlation id.
        :returns: The record, or ``None``.
        """
        import re as _re

        if not _re.fullmatch(r"[0-9a-f]{32}", elicitation_id):
            # A non-hex id can never name a durable question — report it as
            # unknown instead of surfacing a driver error to the caller.
            return None
        try:
            with self._session("durable_elicitation.get") as session:
                row = session.get(SqlDurableElicitation, (current_workspace_id(), elicitation_id))
                return _to_entity(row) if row is not None else None
        except ValueError:
            # A non-hex id can never name a durable question — report it as
            # unknown instead of surfacing a driver error to the caller.
            return None

    def list_for_conversation(
        self,
        conversation_id: str,
        *,
        include_consumed: bool = False,
    ) -> list[DurableElicitation]:
        """
        List a session's durable questions, newest first.

        Snapshot/restore path: returns pending and terminal rows so
        ``GET /v1/sessions/{id}`` can replay outstanding cards after a
        cold start without double-counting resolved ones.

        :param conversation_id: Owning session id.
        :param include_consumed: Also return already-consumed rows
            (default ``False`` — consumed rows are history, not pending
            state).
        :returns: Records ordered by ``created_at`` descending.
        """
        with self._session("durable_elicitation.list_for_conversation") as session:
            conditions = [
                SqlDurableElicitation.workspace_id == current_workspace_id(),
                SqlDurableElicitation.conversation_id == conversation_id,
            ]
            if not include_consumed:
                conditions.append(SqlDurableElicitation.state != 5)
            rows = session.scalars(
                select(SqlDurableElicitation)
                .where(and_(*conditions))
                .order_by(SqlDurableElicitation.created_at.desc(), SqlDurableElicitation.id)
            ).all()
            return [_to_entity(row) for row in rows]

    def list_pending_for_target(self, conversation_id: str) -> list[DurableElicitation]:
        """
        List pending questions *presented* on a session.

        Ancestor views (a child session asking, presented on the
        user-facing parent) route through ``target_conversation_id``;
        answering from the parent view resolves against the owner.

        :param conversation_id: Presentation session id.
        :returns: Pending records ordered newest first.
        """
        with self._session("durable_elicitation.list_pending_for_target") as session:
            rows = session.scalars(
                select(SqlDurableElicitation)
                .where(
                    SqlDurableElicitation.workspace_id == current_workspace_id(),
                    SqlDurableElicitation.target_conversation_id == conversation_id,
                    SqlDurableElicitation.state == 1,
                )
                .order_by(SqlDurableElicitation.created_at.desc(), SqlDurableElicitation.id)
            ).all()
            return [_to_entity(row) for row in rows]

    def list_unconsumed(self) -> list[DurableElicitation]:
        """
        List answered-but-unconsumed questions across the workspace.

        Server-startup reconcile: these rows carry a delivery obligation —
        the answer is committed but the runner has not yet acknowledged
        consumption, so delivery must be retried.

        :returns: Records in ``answered``/``declined``/``canceled`` state
            with no ``consumed_at``, oldest first.
        """
        with self._session("durable_elicitation.list_unconsumed") as session:
            rows = session.scalars(
                select(SqlDurableElicitation)
                .where(
                    SqlDurableElicitation.workspace_id == current_workspace_id(),
                    SqlDurableElicitation.state.in_([2, 3, 4]),
                    SqlDurableElicitation.consumed_at.is_(None),
                )
                .order_by(SqlDurableElicitation.delivered_at.asc(), SqlDurableElicitation.id)
            ).all()
            return [_to_entity(row) for row in rows]

    # ── Resolution ──────────────────────────────────────────────

    def resolve(
        self,
        elicitation_id: str,
        *,
        action: str,
        content: dict | None = None,
    ) -> tuple[DurableElicitation, bool]:
        """
        Atomically transition a pending question to its terminal state.

        Only a ``pending`` row transitions. The commit records the action,
        the content (accept only), and the delivery obligation
        (``delivered_at``) in one transaction — commit-before-publish:
        the resolved state is only announced after this commit lands.
        A crash between commit and delivery leaves an answerable
        ``answered`` row that :meth:`list_unconsumed` recovers.

        :param elicitation_id: The correlation id.
        :param action: ``accept`` / ``decline`` / ``cancel``.
        :param content: The content map on accept (e.g.
            ``{"answer": "staging"}``); ``None`` otherwise.
        :returns: ``(record, accepted)`` — ``accepted`` is ``True`` when
            THIS call performed the transition; ``False`` when the row was
            already terminal with an identical answer (idempotent ack).
        :raises AnswerConflictError: The row was already terminal with a
            *different* action or content. The committed answer is never
            altered.
        :raises KeyError: Unknown or out-of-workspace id — callers must
            not create answer tombstones for unknown ids.
        """
        with self._session("durable_elicitation.resolve") as session:
            row = session.get(SqlDurableElicitation, (current_workspace_id(), elicitation_id))
            if row is None:
                raise KeyError(elicitation_id)
            if row.state != 1:
                record = _to_entity(row)
                if record.answer_action == action and record.answer_content == (
                    content if action == "accept" else None
                ):
                    return record, False
                raise AnswerConflictError(existing=record)
            now = now_epoch()
            state_name = {"accept": "answered", "decline": "declined", "cancel": "canceled"}[
                action
            ]
            row.state = encode_durable_elicitation_state(state_name)
            row.answer_action = encode_durable_elicitation_action(action)
            row.answer_content = (
                json.dumps(content, sort_keys=True) if action == "accept" else None
            )
            # Delivery obligation commits with the answer: an answered row is
            # deliverable whether or not the runner is reachable right now.
            row.delivered_at = now
            row.updated_at = now
            session.flush()
            return _to_entity(row), True

    def mark_consumed(self, elicitation_id: str) -> tuple[DurableElicitation, bool]:
        """
        Claim consumption of a terminal answer exactly once.

        The runner calls this after it has persisted the tool result and
        resumed the parked execution. Only the first claim transitions the
        state; later claims are idempotent no-ops — a duplicate delivery
        can retry, but a duplicate logical continuation cannot happen
        through this gate.

        :param elicitation_id: The correlation id.
        :returns: ``(record, claimed)`` — ``claimed`` is ``True`` only for
            the first successful claim.
        :raises KeyError: Unknown or out-of-workspace id.
        :raises ValueError: The row is still pending (nothing to consume).
        """
        with self._session("durable_elicitation.mark_consumed") as session:
            row = session.get(SqlDurableElicitation, (current_workspace_id(), elicitation_id))
            if row is None:
                raise KeyError(elicitation_id)
            if row.state == 1:
                raise ValueError("durable elicitation is still pending")
            if row.consumed_at is not None:
                return _to_entity(row), False
            row.state = 5
            row.consumed_at = now_epoch()
            row.updated_at = row.consumed_at
            session.flush()
            return _to_entity(row), True

    def record_delivery_attempt(self, elicitation_id: str) -> None:
        """
        Increment the delivery-attempt counter for an unconsumed answer.

        Informational (diagnostics see retry pressure); delivery itself is
        retryable and idempotent through :meth:`mark_consumed`.

        :param elicitation_id: The correlation id.
        """
        with self._session("durable_elicitation.record_delivery_attempt") as session:
            row = session.get(SqlDurableElicitation, (current_workspace_id(), elicitation_id))
            if row is None or row.consumed_at is not None:
                return
            row.delivery_attempts += 1
            row.updated_at = now_epoch()

    # ── Cancellation ────────────────────────────────────────────

    def cancel_pending(
        self,
        conversation_id: str,
        *,
        invocation_id: str | None = None,
        elicitation_id: str | None = None,
    ) -> list[DurableElicitation]:
        """
        Cancel pending questions on explicit interrupt/stop/delete.

        Client/tunnel loss is NOT cancellation — only an explicit
        session interruption calls this, so a later answer cannot
        resurrect a dead execution. Already-terminal rows are untouched;
        cancelling something that was never registered is a no-op.

        :param conversation_id: Owning session id.
        :param invocation_id: Cancel only this invocation when given.
        :param elicitation_id: Cancel only this question when given.
        :returns: The rows this call transitioned to ``canceled``.
        """
        with self._session("durable_elicitation.cancel_pending") as session:
            conditions = [
                SqlDurableElicitation.workspace_id == current_workspace_id(),
                SqlDurableElicitation.conversation_id == conversation_id,
                SqlDurableElicitation.state == 1,
            ]
            if invocation_id is not None:
                conditions.append(SqlDurableElicitation.invocation_id == invocation_id)
            if elicitation_id is not None:
                conditions.append(SqlDurableElicitation.id == elicitation_id)
            rows = session.scalars(select(SqlDurableElicitation).where(and_(*conditions))).all()
            now = now_epoch()
            for row in rows:
                row.state = 4
                row.answer_action = 3
                row.delivered_at = now
                row.updated_at = now
            return [_to_entity(row) for row in rows]

    def reset_for_tests(self) -> None:  # pragma: no cover — test isolation helper
        """Delete every row. For test isolation only."""
        with self._session("durable_elicitation.reset") as session:
            for row in session.scalars(select(SqlDurableElicitation)).all():
                session.delete(row)
