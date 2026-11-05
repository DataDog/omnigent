"""Durable ``sys_ask_user`` service — the server-side lifecycle owner.

Bridges the durable store (:mod:`omnigent.stores.durable_elicitation_store`)
to the request surface:

- **Registration** — the runner's ``sys_ask_user`` intercept POSTs here
  (via the REST route in :mod:`omnigent.server.routes._sessions`); the
  question is persisted BEFORE the form card is published, so a crash
  between publish and answer leaves an answerable pending row.
- **Resolution** — the resolve path validates the answer against the
  persisted schema and commits the terminal state atomically before the
  resolved event is announced (commit-before-publish).
- **Snapshot restore** — pending rows replay into
  ``GET /v1/sessions/{id}`` after a cold server start, merged with the
  in-memory index without duplicates.

Legacy policy approvals and external MCP elicitations never touch this
module — they keep their in-memory lifecycle.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from omnigent.stores.durable_elicitation_store import (
    AnswerConflictError,
    DurableElicitation,
    DurableElicitationError,
    SqlAlchemyDurableElicitationStore,
)
from omnigent.tools.builtins.sys_ask_user import (
    ANSWER_FIELD,
    AskUserQuestion,
    build_requested_schema,
    question_fingerprint,
    validate_answer_content,
    validate_ask_user_args,
)

_logger = logging.getLogger(__name__)

_store: SqlAlchemyDurableElicitationStore | None = None

#: Result values for :func:`resolve_durable_question`.
RESOLVED = "resolved"  # This call committed the terminal state.
IDEMPOTENT = "idempotent"  # Already terminal with an identical answer.
CONFLICT = "conflict"  # Already terminal with a different answer.
NOT_FOUND = "not_found"  # Unknown or out-of-workspace id.
FORBIDDEN = "forbidden"  # Resolved against a session that neither owns nor
#   presents the question.


def configure_durable_elicitation_store(
    store: SqlAlchemyDurableElicitationStore | None,
) -> None:
    """
    Wire (or clear) the durable-elicitation store.

    The server app calls this at startup alongside the other store wiring;
    ``None`` disables the durable surface (tests / non-server processes).

    :param store: The store instance, or ``None``.
    """
    global _store
    _store = store


def durable_store() -> SqlAlchemyDurableElicitationStore | None:
    """
    Return the wired store, or ``None`` when the durable surface is off.

    :returns: The configured store or ``None``.
    """
    return _store


class DurableAskUserUnavailable(RuntimeError):
    """The durable-elicitation store is not wired into this process."""


_ELICITATION_ID_RE = __import__("re").compile(r"^[0-9a-f]{32}$")


def _is_elicitation_id(elicitation_id: str) -> bool:
    """Whether an id is shaped like a durable elicitation id (bare hex UUID)."""
    return bool(_ELICITATION_ID_RE.match(elicitation_id))


def _require_store() -> SqlAlchemyDurableElicitationStore:
    store = _store
    if store is None:
        raise DurableAskUserUnavailable("durable elicitation store is not configured")
    return store


def register_durable_question(
    *,
    conversation_id: str,
    invocation_id: str,
    arguments: dict[str, Any],
    tool_name: str = "sys_ask_user",
    target_conversation_id: str | None = None,
) -> tuple[DurableElicitation, bool]:
    """
    Validate arguments and persist the question; return the record.

    Invalid arguments raise :class:`ValueError` BEFORE any pending state is
    created. A replayed registration under the same invocation identity
    with the same payload returns the existing row; with a changed payload
    it raises :class:`DuplicateRegistrationError`.

    :param conversation_id: Owning session id.
    :param invocation_id: Stable caller invocation identity (the LLM
        tool-call id).
    :param arguments: Parsed tool arguments.
    :param tool_name: Registering tool name.
    :param target_conversation_id: Presentation session for child questions.
    :returns: ``(record, created)``.
    :raises ValueError: Invalid arguments.
    """
    validated = validate_ask_user_args(arguments)
    if isinstance(validated, str):
        raise ValueError(validated)
    question: AskUserQuestion = validated
    store = _require_store()
    return store.create_or_get(
        elicitation_id=uuid.uuid4().hex,
        conversation_id=conversation_id,
        invocation_id=invocation_id,
        fingerprint=question_fingerprint(question),
        tool_name=tool_name,
        message=question.question,
        requested_schema=build_requested_schema(question),
        target_conversation_id=target_conversation_id,
    )


def durable_question_from_record(record: DurableElicitation) -> AskUserQuestion:
    """
    Rebuild the validated question from a persisted record.

    The persisted ``requestedSchema`` is authoritative for answer checks;
    the options are recovered from its ``enum`` members so the resolve path
    validates against exactly what was registered.

    :param record: The persisted record.
    :returns: The reconstructed question.
    """
    schema = record.requested_schema
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    answer = properties.get(ANSWER_FIELD, {}) if isinstance(properties, dict) else {}
    answer_type = answer.get("type")
    if answer_type == "array":
        return AskUserQuestion(
            question=record.message,
            options=tuple(answer.get("items", {}).get("enum", [])),
            multiple=True,
        )
    enum = answer.get("enum")
    if isinstance(enum, list) and enum:
        return AskUserQuestion(question=record.message, options=tuple(enum))
    return AskUserQuestion(question=record.message)


def resolve_durable_question(
    *,
    elicitation_id: str,
    session_id: str,
    action: str,
    content: dict[str, Any] | None,
) -> tuple[str, DurableElicitation | None, str | None]:
    """
    Resolve one durable question from a session-authorized verdict.

    Authorization: the resolving session must be the owning session OR the
    presentation target (a parent resolving a child's question). The
    answer is validated against the persisted schema before the atomic
    transition; a malformed answer leaves the question pending.

    :param elicitation_id: The correlation id from the verdict payload.
    :param session_id: Session the verdict arrived on.
    :param action: ``accept`` / ``decline`` / ``cancel``.
    :param content: Content map on accept (e.g. ``{"answer": "staging"}``).
    :returns: ``(outcome, record, error)`` — ``outcome`` is one of
        :data:`RESOLVED` / :data:`IDEMPOTENT` / :data:`CONFLICT` /
        :data:`NOT_FOUND` / :data:`FORBIDDEN`; ``error`` carries a
        human-readable validation message for malformed answers (the
        question stays pending).
    """
    if not _is_elicitation_id(elicitation_id):
        return NOT_FOUND, None, None
    store = _require_store()
    try:
        record = store.get(elicitation_id)
    except DurableElicitationError:
        record = None
    if record is None:
        return NOT_FOUND, None, None
    if session_id not in {record.conversation_id, record.target_conversation_id}:
        return FORBIDDEN, record, None
    if record.state != "pending":
        # Idempotent vs. conflicting — identical answers re-acknowledge.
        if action == record.answer_action and (
            action != "accept" or content == record.answer_content
        ):
            return IDEMPOTENT, record, None
        return CONFLICT, record, None
    if action == "accept":
        question = durable_question_from_record(record)
        checked = validate_answer_content(content, question)
        if isinstance(checked, str):
            return "invalid", record, checked
        content = checked
    elif action not in {"decline", "cancel"}:
        return "invalid", record, f"unsupported action: {action!r}"
    try:
        record, _ = store.resolve(
            elicitation_id,
            action=action,
            content=content if action == "accept" else None,
        )
    except AnswerConflictError:
        return CONFLICT, record, None
    return RESOLVED, record, None


def pending_event_for_record(
    record: DurableElicitation,
    *,
    mirror_on: str | None = None,
) -> dict[str, Any]:
    """
    Shape a persisted pending question as a ``response.elicitation_request``
    event dict.

    Same wire shape the in-memory index replays, so the UI renders the
    durable card with one renderer. Child questions carry
    ``params.target_session_id`` pointing at the owning (child) session so
    an ancestor view resolves the child's parked call.

    :param record: A pending (or terminal, for replay) record.
    :param mirror_on: Session the snapshot is rendered on. When it differs
        from the owning session (a parent view of a child's question),
        ``params.target_session_id`` points at the owning child so the
        answer resolves the child's parked call.
    :returns: Event dict suitable for ``SessionResponse.pending_elicitations``.
    """
    params: dict[str, Any] = {
        "mode": "form",
        "message": record.message,
        "requestedSchema": record.requested_schema,
        "durable": True,
        "invocation_id": record.invocation_id,
    }
    if mirror_on is not None and mirror_on != record.conversation_id:
        params["target_session_id"] = record.conversation_id
    return {
        "type": "response.elicitation_request",
        "elicitation_id": record.id,
        "params": params,
    }


def snapshot_for_session(conversation_id: str) -> list[dict[str, Any]]:
    """
    Return durable pending questions presented on a session.

    Merges questions the session OWNS (pending or answered-but-unconsumed —
    a cold server restart replays the card for anything the runner has not
    consumed) with pending questions it merely PRESENTS for a child.
    Duplicate ids are dropped.

    :param conversation_id: Session id.
    :returns: Event dicts; empty when the durable surface is off or
        nothing is pending.
    """
    store = _store
    if store is None:
        return []
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in store.list_for_conversation(conversation_id):
        if record.state == "consumed":
            continue
        seen.add(record.id)
        events.append(pending_event_for_record(record))
    for record in store.list_pending_for_target(conversation_id):
        if record.id in seen:
            continue
        seen.add(record.id)
        events.append(pending_event_for_record(record, mirror_on=conversation_id))
    return events
