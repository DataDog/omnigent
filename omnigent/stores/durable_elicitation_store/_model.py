"""Durable-elicitation record and error types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DurableElicitation:
    """
    One persisted durable question, at rest or in flight.

    Mirrors ``SqlDurableElicitation`` without the tenant-partition column.

    :param id: Stable elicitation correlation id (bare 32-char hex).
    :param conversation_id: Owning session whose parked execution the
        answer resumes.
    :param target_conversation_id: Presentation session (user-facing
        parent for child questions); ``None`` when the owner is itself
        user-facing.
    :param invocation_id: Stable caller invocation identity (tool-call id).
    :param fingerprint: Canonical payload fingerprint for replay checks.
    :param tool_name: Registering tool, e.g. ``"sys_ask_user"``.
    :param message: The question text.
    :param requested_schema: Decoded ``requestedSchema`` dict.
    :param state: ``pending`` / ``answered`` / ``declined`` / ``canceled`` /
        ``consumed``.
    :param answer_action: ``accept`` / ``decline`` / ``cancel``; ``None``
        while pending.
    :param answer_content: Decoded content map of an accepted answer.
    :param delivered_at: Epoch seconds the answer became deliverable.
    :param consumed_at: Epoch seconds the runner acknowledged consumption.
    :param delivery_attempts: Delivery attempts made toward the runner.
    :param created_at / updated_at: Epoch-second bookkeeping stamps.
    """

    id: str
    conversation_id: str
    invocation_id: str
    fingerprint: str
    tool_name: str
    message: str
    requested_schema: dict[str, Any]
    paused_call: dict[str, Any] | None = None
    target_conversation_id: str | None = None
    state: str = "pending"
    answer_action: str | None = None
    answer_content: dict[str, Any] | None = None
    delivered_at: int | None = None
    consumed_at: int | None = None
    delivery_attempts: int = 0
    created_at: int = 0
    updated_at: int | None = None


class DurableElicitationError(Exception):
    """Base class for durable-elicitation store errors."""


@dataclass
class DuplicateRegistrationError(DurableElicitationError):
    """
    A replayed registration under a used invocation identity with a
    different payload.

    An intentionally new invocation asking identical text is a new
    question (fresh invocation id); only identity-keyed replays are
    compared. Carries the existing record so callers can surface its id.

    :param existing: The already-registered question under that identity.
    """

    existing: DurableElicitation | None = None


@dataclass
class AnswerConflictError(DurableElicitationError):
    """
    A second, different terminal answer arrived after one was committed.

    First valid terminal answer wins; the conflicting one changes nothing.
    Carries the already-committed record.

    :param existing: The question with its committed answer.
    """

    existing: DurableElicitation | None = None
