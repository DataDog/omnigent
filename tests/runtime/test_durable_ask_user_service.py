"""Service tests for the durable ``sys_ask_user`` lifecycle.

Covers registration (validate-before-persist, replay, duplicate rejection),
resolution (authorization, schema validation, atomic commit, malformed
answers leaving the question pending), snapshot restore after a simulated
cold restart, and store-not-wired failure.
"""

from __future__ import annotations

import pytest

from omnigent.runtime.durable_ask_user import (
    CONFLICT,
    FORBIDDEN,
    IDEMPOTENT,
    NOT_FOUND,
    RESOLVED,
    configure_durable_elicitation_store,
    pending_event_for_record,
    register_durable_question,
    resolve_durable_question,
    snapshot_for_session,
)
from omnigent.stores.durable_elicitation_store import (
    DuplicateRegistrationError,
    SqlAlchemyDurableElicitationStore,
)

hexid = lambda c: ((c * 33)[:32]).replace("p", "a").replace("q", "b")  # noqa: E731

OWNER = hexid("c")
PARENT = hexid("d")


@pytest.fixture(autouse=True)
def _wired(db_uri: str):
    """Wire a fresh store per test and unwire afterwards."""
    configure_durable_elicitation_store(SqlAlchemyDurableElicitationStore(db_uri))
    yield
    configure_durable_elicitation_store(None)


def _register(invocation_id: str = "call_1", **kwargs) -> tuple[str, bool]:
    record, created = register_durable_question(
        conversation_id=kwargs.pop("conversation_id", OWNER),
        invocation_id=invocation_id,
        arguments=kwargs.pop("arguments", {"question": "Env?", "options": ["staging", "prod"]}),
        **kwargs,
    )
    return record, created


class TestRegistration:
    def test_invalid_arguments_fail_before_pending_state(self) -> None:
        with pytest.raises(ValueError, match="question"):
            register_durable_question(
                conversation_id=OWNER,
                invocation_id="call_bad",
                arguments={"question": "  "},
            )
        assert snapshot_for_session(OWNER) == []

    def test_replayed_invocation_is_idempotent(self) -> None:
        first, created = _register()
        replay, replay_created = _register()
        assert created and not replay_created
        assert replay.id == first.id

    def test_duplicate_identity_with_changed_payload_rejected(self) -> None:
        _register()
        with pytest.raises(DuplicateRegistrationError):
            register_durable_question(
                conversation_id=OWNER,
                invocation_id="call_1",
                arguments={"question": "Different?", "options": ["a"]},
            )

    def test_child_question_carries_target(self) -> None:
        record, _ = _register(conversation_id=hexid("f"), target_conversation_id=PARENT)
        assert record.target_conversation_id == PARENT

    def test_unwired_store_raises(self) -> None:
        configure_durable_elicitation_store(None)
        with pytest.raises(RuntimeError, match="not configured"):
            _register()


class TestResolution:
    def test_owner_session_resolves(self) -> None:
        record, _ = _register()
        outcome, resolved, error = resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="accept",
            content={"answer": "staging"},
        )
        assert outcome == RESOLVED
        assert error is None
        assert resolved is not None
        assert resolved.answer_content == {"answer": "staging"}

    def test_target_parent_may_resolve_child_question(self) -> None:
        child = hexid("f")
        record, _ = _register(conversation_id=child, target_conversation_id=PARENT)
        outcome, resolved, _ = resolve_durable_question(
            elicitation_id=record.id,
            session_id=PARENT,
            action="accept",
            content={"answer": "prod"},
        )
        assert outcome == RESOLVED
        assert resolved is not None
        assert resolved.conversation_id == child

    def test_unrelated_session_is_forbidden(self) -> None:
        record, _ = _register()
        outcome, _, _ = resolve_durable_question(
            elicitation_id=record.id,
            session_id=hexid("z"),
            action="accept",
            content={"answer": "staging"},
        )
        assert outcome == FORBIDDEN

    def test_malformed_answer_leaves_question_pending(self) -> None:
        record, _ = _register()
        outcome, _, error = resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="accept",
            content={"answer": "not-an-option"},
        )
        assert outcome == "invalid"
        assert error is not None
        from omnigent.runtime.durable_ask_user import durable_store

        store = durable_store()
        assert store is not None
        current = store.get(record.id)
        assert current is not None
        assert current.state == "pending"

    def test_duplicate_answer_is_idempotent_conflicting_is_conflict(self) -> None:
        record, _ = _register()
        resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="accept",
            content={"answer": "staging"},
        )
        outcome, _, _ = resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="accept",
            content={"answer": "staging"},
        )
        assert outcome == IDEMPOTENT
        outcome, _, _ = resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="accept",
            content={"answer": "prod"},
        )
        assert outcome == CONFLICT

    def test_unknown_id_is_not_found(self) -> None:
        outcome, _, _ = resolve_durable_question(
            elicitation_id=hexid("9"),
            session_id=OWNER,
            action="accept",
            content={"answer": "staging"},
        )
        assert outcome == NOT_FOUND

    def test_unsupported_action_is_invalid(self) -> None:
        record, _ = _register()
        outcome, _, _ = resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="timeout",
            content=None,
        )
        assert outcome == "invalid"

    def test_decline_is_terminal(self) -> None:
        record, _ = _register()
        outcome, resolved, _ = resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="decline",
            content=None,
        )
        assert outcome == RESOLVED
        assert resolved is not None
        assert resolved.state == "declined"


class TestSnapshot:
    def test_snapshot_shapes_pending_question(self) -> None:
        record, _ = _register()
        events = snapshot_for_session(OWNER)
        assert len(events) == 1
        event = events[0]
        assert event["type"] == "response.elicitation_request"
        assert event["elicitation_id"] == record.id
        params = event["params"]
        assert params["message"] == "Env?"
        assert params["durable"] is True
        assert params["requestedSchema"]["properties"]["answer"]["enum"] == [
            "staging",
            "prod",
        ]
        # The owner view does not route to itself.
        assert "target_session_id" not in params

    def test_snapshot_after_restart_excludes_consumed(self, db_uri: str) -> None:
        record, _ = _register()
        resolve_durable_question(
            elicitation_id=record.id,
            session_id=OWNER,
            action="accept",
            content={"answer": "staging"},
        )
        # Simulate a cold restart: a brand-new store over the same DB.
        configure_durable_elicitation_store(SqlAlchemyDurableElicitationStore(db_uri))
        assert len(snapshot_for_session(OWNER)) == 1  # answered, unconsumed
        from omnigent.runtime.durable_ask_user import durable_store

        store = durable_store()
        assert store is not None
        store.mark_consumed(record.id)
        assert snapshot_for_session(OWNER) == []

    def test_child_snapshot_routes_to_parent(self) -> None:
        child = hexid("f")
        _child_record, _ = _register(conversation_id=child, target_conversation_id=PARENT)
        parent_events = snapshot_for_session(PARENT)
        assert len(parent_events) == 1
        params = parent_events[0]["params"]
        # Answering from the parent view must resolve the child's call.
        assert params["target_session_id"] == child

    def test_pending_event_shape_matches_index_events(self) -> None:
        record, _ = _register()
        event = pending_event_for_record(record)
        assert event["elicitation_id"] == record.id
        assert event["params"]["mode"] == "form"


class TestContinuationWake:
    """The continuation wake must be answerable without replaying history."""

    def test_wake_names_the_answer_and_tool_output(self) -> None:
        from omnigent.runtime.durable_ask_user import continuation_wake_text
        from omnigent.tools.builtins.sys_ask_user import format_tool_result

        text = continuation_wake_text(
            action="accept",
            content={"answer": "staging"},
            tool_output=format_tool_result(action="accept", answer="staging"),
        )
        assert "answered: 'staging'" in text
        assert "sys_ask_user" in text
        assert "Do not repeat any earlier work" in text

    def test_decline_wake_is_terminal(self) -> None:
        from omnigent.runtime.durable_ask_user import continuation_wake_text

        text = continuation_wake_text(
            action="decline", content=None, tool_output='{"action": "decline"}'
        )
        assert "decline" in text
