"""Store tests for the durable-elicitation store.

Covers persistence across a fresh store reopen (server-restart survival),
create-or-get identity/replay semantics, atomic resolution with
duplicate/conflict handling, exactly-once consumption, cancellation,
and target-session routing for child questions.
"""

from __future__ import annotations

from typing import Any

import pytest

from omnigent.stores.durable_elicitation_store import (
    AnswerConflictError,
    DuplicateRegistrationError,
    SqlAlchemyDurableElicitationStore,
)
from omnigent.tools.builtins.sys_ask_user import (
    AskUserQuestion,
    build_requested_schema,
    question_fingerprint,
    validate_ask_user_args,
)

hexid = lambda c: ((c * 33)[:32]).replace("p", "a").replace("q", "b")  # noqa: E731

OWNER = hexid("c")
CHILD_OWNER = hexid("f")
PARENT_TARGET = hexid("d")


@pytest.fixture()
def store(db_uri: str) -> SqlAlchemyDurableElicitationStore:
    """:returns: A store backed by the per-test migrated database."""
    return SqlAlchemyDurableElicitationStore(db_uri)


def _question(options: list[str] | None = None, multiple: bool = False) -> Any:
    args: dict[str, Any] = {"question": "Which environment?"}
    if options is not None:
        args["options"] = options
        args["multiple"] = multiple
    result = validate_ask_user_args(args)
    assert isinstance(result, AskUserQuestion)
    return result


def _register(
    store: SqlAlchemyDurableElicitationStore,
    *,
    invocation_id: str = "call_1",
    conversation_id: str = OWNER,
    question: Any | None = None,
    elicitation_id: str = hexid("e"),
    target: str | None = None,
) -> tuple[Any, bool]:
    question = question or _question(["staging", "prod"])
    return store.create_or_get(
        elicitation_id=elicitation_id,
        conversation_id=conversation_id,
        invocation_id=invocation_id,
        fingerprint=question_fingerprint(question),
        tool_name="sys_ask_user",
        message=question.question,
        requested_schema=build_requested_schema(question),
        target_conversation_id=target,
    )


class TestCreateOrGet:
    def test_register_creates_pending_question(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        record, created = _register(store)
        assert created
        assert record.state == "pending"
        assert record.conversation_id == OWNER
        assert record.invocation_id == "call_1"
        assert record.message == "Which environment?"
        schema = record.requested_schema
        assert schema["properties"]["answer"]["enum"] == ["staging", "prod"]

    def test_replayed_identity_returns_same_question(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        original, created = _register(store)
        replay, replay_created = _register(store, elicitation_id=hexid("b"))
        assert created and not replay_created
        assert replay.id == original.id

    def test_changed_payload_under_same_identity_is_rejected(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        _register(store)
        changed = _question(["qa", "prod"])
        with pytest.raises(DuplicateRegistrationError):
            _register(store, question=changed, elicitation_id=hexid("b"))

    def test_identical_text_under_new_identity_is_a_new_question(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        first, _ = _register(store, invocation_id="call_1")
        second, created = _register(store, invocation_id="call_2", elicitation_id=hexid("b"))
        assert created
        assert second.id != first.id


class TestResolve:
    def test_first_valid_answer_wins(self, store: SqlAlchemyDurableElicitationStore) -> None:
        record, _ = _register(store)
        resolved, accepted = store.resolve(
            record.id, action="accept", content={"answer": "staging"}
        )
        assert accepted
        assert resolved.state == "answered"
        assert resolved.answer_content == {"answer": "staging"}
        assert resolved.delivered_at is not None

    def test_identical_retry_acknowledges_idempotently(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        record, _ = _register(store)
        store.resolve(record.id, action="accept", content={"answer": "staging"})
        _, again = store.resolve(record.id, action="accept", content={"answer": "staging"})
        assert again is False

    def test_conflicting_answer_does_not_alter_the_first(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        record, _ = _register(store)
        store.resolve(record.id, action="accept", content={"answer": "staging"})
        with pytest.raises(AnswerConflictError):
            store.resolve(record.id, action="accept", content={"answer": "prod"})
        current = store.get(record.id)
        assert current is not None
        assert current.answer_content == {"answer": "staging"}

    def test_unknown_id_raises_keyerror_not_tombstone(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        with pytest.raises(KeyError):
            store.resolve(hexid("9"), action="accept", content={"answer": "x"})

    def test_decline_and_cancel_are_terminal_non_answers(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        decline_record, _ = _register(store, invocation_id="c1", elicitation_id=hexid("a"))
        store.resolve(decline_record.id, action="decline")
        resolved = store.get(decline_record.id)
        assert resolved is not None
        assert resolved.state == "declined"
        assert resolved.answer_content is None
        cancel_record, _ = _register(store, invocation_id="c2", elicitation_id=hexid("b"))
        store.resolve(cancel_record.id, action="cancel")
        resolved = store.get(cancel_record.id)
        assert resolved is not None
        assert resolved.state == "canceled"


class TestConsumption:
    def test_consume_claims_exactly_once(self, store: SqlAlchemyDurableElicitationStore) -> None:
        record, _ = _register(store)
        store.resolve(record.id, action="accept", content={"answer": "staging"})
        _, first = store.mark_consumed(record.id)
        _, second = store.mark_consumed(record.id)
        assert first is True
        assert second is False
        final = store.get(record.id)
        assert final is not None
        assert final.state == "consumed"
        assert final.consumed_at is not None

    def test_pending_cannot_be_consumed(self, store: SqlAlchemyDurableElicitationStore) -> None:
        record, _ = _register(store)
        with pytest.raises(ValueError):
            store.mark_consumed(record.id)

    def test_answered_unconsumed_survives_reopen(
        self, store: SqlAlchemyDurableElicitationStore, db_uri: str
    ) -> None:
        # Server-restart survival: a fresh store instance over the same
        # database must see the committed answer as a delivery obligation.
        record, _ = _register(store)
        store.resolve(record.id, action="accept", content={"answer": "staging"})
        reopened = SqlAlchemyDurableElicitationStore(db_uri)
        unconsumed = reopened.list_unconsumed()
        assert [r.id for r in unconsumed] == [record.id]
        fetched = reopened.get(record.id)
        assert fetched is not None
        assert fetched.answer_content == {"answer": "staging"}

    def test_pending_question_survives_reopen(
        self, store: SqlAlchemyDurableElicitationStore, db_uri: str
    ) -> None:
        record, _ = _register(store)
        reopened = SqlAlchemyDurableElicitationStore(db_uri)
        rows = reopened.list_for_conversation(OWNER)
        assert [r.id for r in rows] == [record.id]
        fetched = reopened.get(record.id)
        assert fetched is not None
        assert fetched.state == "pending"
        assert fetched.requested_schema["properties"]["answer"]["enum"] == [
            "staging",
            "prod",
        ]


class TestCancellationAndRouting:
    def test_cancel_pending_only_touches_pending_rows(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        pending, _ = _register(store, invocation_id="c1", elicitation_id=hexid("a"))
        answered, _ = _register(store, invocation_id="c2", elicitation_id=hexid("b"))
        store.resolve(answered.id, action="accept", content={"answer": "prod"})
        canceled = store.cancel_pending(OWNER)
        assert [r.id for r in canceled] == [pending.id]
        final = store.get(answered.id)
        assert final is not None
        assert final.state == "answered"

    def test_cancel_then_answer_is_a_conflict(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        record, _ = _register(store)
        store.cancel_pending(OWNER, elicitation_id=record.id)
        with pytest.raises(AnswerConflictError):
            store.resolve(record.id, action="accept", content={"answer": "prod"})

    def test_child_question_routes_through_target(
        self, store: SqlAlchemyDurableElicitationStore
    ) -> None:
        record, _ = _register(
            store, conversation_id=CHILD_OWNER, target=PARENT_TARGET, elicitation_id=hexid("a")
        )
        assert store.list_pending_for_target(PARENT_TARGET)[0].id == record.id
        assert store.list_for_conversation(PARENT_TARGET) == []
        # The parent view may resolve the child's question...
        resolved, accepted = store.resolve(record.id, action="accept", content={"answer": "a"})
        assert accepted
        assert resolved.conversation_id == CHILD_OWNER
        # ...and the child (owner) view may consume it.
        _, claimed = store.mark_consumed(record.id)
        assert claimed
