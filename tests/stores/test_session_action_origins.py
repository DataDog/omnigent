"""Persistence and isolation contract for private session-action origins."""

from __future__ import annotations

import json

import pytest

from omnigent.db.db_models import SqlConversationMetadata, workspace_scope
from omnigent.stores.conversation_store import sqlalchemy_store as store_module
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore

_RUNNER_ID = "runner-origin-test"
_GENERATION = "generation-origin-test"
_PRINCIPAL = "runner-credential-origin-test"
_TOKEN = "origin-token-test"
_ORIGIN = {
    "runner_id": _RUNNER_ID,
    "actor": "alice@example.com",
    "agent_id": "ag_0123456789abcdef0123456789abcdef",
    "bundle_location": "agent/original-bundle",
    "agent_provenance": "tenant",
    "operator_authored": False,
    "effective_spec_digest": "1" * 64,
    "operator_environment_digest": "2" * 64,
    "mcp_selection_snapshot": {"omnigent.mcp_service.original": "1"},
    "metadata": {"revision": "first"},
}


def _bind(
    store: SqlAlchemyConversationStore,
    conversation_id: str,
    token: str = _TOKEN,
    *,
    runner_id: str = _RUNNER_ID,
    runner_generation: str = _GENERATION,
    credential_principal: str = _PRINCIPAL,
) -> dict[str, object] | None:
    return store.bind_session_action_origin(
        conversation_id,
        token,
        runner_id,
        runner_generation,
        credential_principal,
    )


def test_origin_registration_binding_and_release_are_exact_and_durable(
    conversation_store: SqlAlchemyConversationStore, db_uri: str
) -> None:
    conversation = conversation_store.create_conversation(title="Origin durability")
    conversation_store.register_session_action_origin(conversation.id, _TOKEN, _ORIGIN)
    with pytest.raises(ValueError, match="rebound"):
        conversation_store.register_session_action_origin(
            conversation.id, _TOKEN, {**_ORIGIN, "actor": "bob@example.com"}
        )
    # Exact registration retry is safe; token reuse for a different origin is not.
    conversation_store.register_session_action_origin(conversation.id, _TOKEN, _ORIGIN)
    with pytest.raises(ValueError, match="rebound"):
        conversation_store.register_session_action_origin(
            conversation.id,
            _TOKEN,
            {**_ORIGIN, "bundle_location": "agent/other-bundle"},
        )

    reopened = SqlAlchemyConversationStore(db_uri)
    assert reopened.get_session_action_origin(conversation.id, _TOKEN) == _ORIGIN

    assert _bind(reopened, conversation.id, runner_id="other-runner") is None
    bound = _bind(reopened, conversation.id)
    assert bound is not None
    assert bound["bound_runner_id"] == _RUNNER_ID
    assert bound["bound_harness_generation"] == _GENERATION
    assert bound["bound_credential_principal"] == _PRINCIPAL
    assert _bind(reopened, conversation.id) == bound
    assert _bind(reopened, conversation.id, runner_generation="new-generation") is None
    assert _bind(reopened, conversation.id, credential_principal="different-principal") is None

    # Reads return detached data, including nested bundle metadata.
    bound["metadata"]["revision"] = "caller-mutated"  # type: ignore[index]
    reread = reopened.get_session_action_origin(conversation.id, _TOKEN)
    assert reread is not None
    assert reread["metadata"] == {"revision": "first"}

    assert not reopened.release_session_action_origin(
        conversation.id, _TOKEN, _RUNNER_ID, "wrong-generation", _PRINCIPAL
    )
    assert not reopened.release_session_action_origin(
        conversation.id, _TOKEN, _RUNNER_ID, _GENERATION, "wrong-principal"
    )
    assert reopened.get_session_action_origin(conversation.id, _TOKEN) is not None
    assert reopened.release_session_action_origin(
        conversation.id, _TOKEN, _RUNNER_ID, _GENERATION, _PRINCIPAL
    )
    assert reopened.get_session_action_origin(conversation.id, _TOKEN) is None
    assert not reopened.release_session_action_origin(
        conversation.id, _TOKEN, _RUNNER_ID, _GENERATION, _PRINCIPAL
    )


def test_child_bind_is_idempotent_and_requires_exact_parent_child_and_generation(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    parent = conversation_store.create_conversation(title="Child origin parent")
    foreign_parent = conversation_store.create_conversation(title="Foreign child parent")
    child = conversation_store.create_conversation(
        title="worker",
        parent_conversation_id=parent.id,
        labels={"omnigent.acp.subagent_id": "stable-child-key"},
    )
    other_child = conversation_store.create_conversation(
        title="other worker",
        parent_conversation_id=parent.id,
        labels={"omnigent.acp.subagent_id": "other-child-key"},
    )
    foreign_child = conversation_store.create_conversation(
        title="foreign worker",
        parent_conversation_id=foreign_parent.id,
        labels={"omnigent.acp.subagent_id": "stable-child-key"},
    )
    token = "child-origin-token"
    origin = {
        **_ORIGIN,
        "session_id": parent.id,
        "workspace_id": store_module.current_workspace_id(),
    }
    conversation_store.register_session_action_origin(parent.id, token, origin)
    assert _bind(conversation_store, parent.id, token) is not None

    bind_child = conversation_store.bind_session_action_origin_child
    args = (
        parent.id,
        token,
        _RUNNER_ID,
        _GENERATION,
        _PRINCIPAL,
        "stable-child-key",
        child.id,
    )
    assert bind_child(*args)
    assert bind_child(*args)  # Retrying the exact binding is safe.
    assert not bind_child(*args[:-1], other_child.id)  # A key cannot be rebound.
    assert not bind_child(*args[:-2], "wrong-child-key", child.id)
    assert not bind_child(*args[:-1], foreign_child.id)
    assert not bind_child(
        parent.id,
        token,
        _RUNNER_ID,
        "stale-generation",
        _PRINCIPAL,
        "stable-child-key",
        child.id,
    )
    assert not bind_child(
        parent.id,
        "unregistered-token",
        _RUNNER_ID,
        _GENERATION,
        _PRINCIPAL,
        "stable-child-key",
        child.id,
    )
    assert not bind_child(
        foreign_parent.id,
        token,
        _RUNNER_ID,
        _GENERATION,
        _PRINCIPAL,
        "stable-child-key",
        child.id,
    )

    listed = conversation_store.list_session_action_origins(parent.id)
    assert listed[token]["children"] == {"stable-child-key": child.id}
    listed[token]["children"]["stable-child-key"] = "mutated-child"  # type: ignore[index]
    stored = conversation_store.get_session_action_origin(parent.id, token)
    assert stored is not None
    assert stored["children"] == {"stable-child-key": child.id}


def test_retired_generation_release_preserves_current_and_unbound_origins(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    conversation = conversation_store.create_conversation(title="Origin generation retirement")
    old_token, current_token, queued_token = "retired", "current", "queued"
    for token in (old_token, current_token, queued_token):
        conversation_store.register_session_action_origin(
            conversation.id,
            token,
            {**_ORIGIN, "session_id": conversation.id},
        )
    assert _bind(
        conversation_store, conversation.id, old_token, runner_generation="old-generation"
    )
    assert _bind(
        conversation_store, conversation.id, current_token, runner_generation="new-generation"
    )

    assert conversation_store.release_session_action_origin(
        conversation.id,
        old_token,
        _RUNNER_ID,
        "old-generation",
        _PRINCIPAL,
    )
    assert not conversation_store.release_session_action_origin(
        conversation.id,
        current_token,
        _RUNNER_ID,
        "old-generation",
        _PRINCIPAL,
    )
    assert not conversation_store.release_session_action_origin(
        conversation.id,
        queued_token,
        _RUNNER_ID,
        "old-generation",
        _PRINCIPAL,
    )
    assert conversation_store.get_session_action_origin(conversation.id, old_token) is None
    assert conversation_store.get_session_action_origin(conversation.id, current_token) is not None
    assert conversation_store.get_session_action_origin(conversation.id, queued_token) is not None


def test_failed_bind_can_release_its_pending_record_without_stranding_capacity(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    conversation = conversation_store.create_conversation(title="Bind overhead")
    large_origin = {
        "runner_id": _RUNNER_ID,
        "payload": "x" * 3_980,
    }
    encoded_map = json.dumps(
        {"reserved": large_origin, "target": large_origin},
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(encoded_map) < store_module._SESSION_ACTION_ORIGINS_MAX_BYTES
    conversation_store.register_session_action_origin(conversation.id, "reserved", large_origin)
    conversation_store.register_session_action_origin(conversation.id, "target", large_origin)

    with pytest.raises(ValueError, match="storage limit"):
        _bind(conversation_store, conversation.id, "target")
    pending = conversation_store.get_session_action_origin(conversation.id, "target")
    assert pending == large_origin

    assert not conversation_store.release_session_action_origin(
        conversation.id, "target", "other-runner", None, _PRINCIPAL
    )
    # Registration has no credential principal; exact runner proof is enough
    # to discard an origin whose bind failed before generation admission.
    assert conversation_store.release_session_action_origin(
        conversation.id, "target", _RUNNER_ID, None, "cleanup-runner-principal"
    )
    assert conversation_store.get_session_action_origin(conversation.id, "target") is None
    conversation_store.register_session_action_origin(
        conversation.id, "replacement", {"runner_id": _RUNNER_ID}
    )


def test_origins_are_private_and_client_state_cannot_inject_or_replace_them(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    conversation = conversation_store.create_conversation(title="Private origin")
    conversation_store.set_session_todos(
        conversation.id,
        [{"content": "Keep this plan", "status": "pending", "activeForm": "Keeping"}],
    )
    conversation_store.register_session_action_origin(conversation.id, _TOKEN, _ORIGIN)

    public_state = {
        "policy_counter": 4,
        store_module._SESSION_ACTION_ORIGINS_KEY: {"spoofed-token": {"runner_id": "attacker"}},
    }
    conversation_store.set_session_state(conversation.id, public_state)
    loaded = conversation_store.get_conversation(conversation.id)

    assert loaded is not None
    assert loaded.session_state == {"policy_counter": 4}
    assert loaded.session_todos == [
        {"content": "Keep this plan", "status": "pending", "activeForm": "Keeping"}
    ]
    assert conversation_store.get_session_action_origin(conversation.id, _TOKEN) == _ORIGIN
    assert conversation_store.get_session_action_origin(conversation.id, "spoofed-token") is None

    # A client-supplied reserved map cannot create an origin on a fresh session.
    clean = conversation_store.create_conversation(title="No injected origin")
    conversation_store.set_session_state(clean.id, public_state)
    clean_loaded = conversation_store.get_conversation(clean.id)
    assert clean_loaded is not None
    assert clean_loaded.session_state == {"policy_counter": 4}
    assert conversation_store.get_session_action_origin(clean.id, "spoofed-token") is None


def test_policy_and_todos_writes_preserve_origins_and_forks_drop_them(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    source = conversation_store.create_conversation(title="Origin fork")
    conversation_store.set_session_todos(
        source.id,
        [{"content": "Source plan", "status": "pending", "activeForm": "Keeping"}],
    )
    conversation_store.register_session_action_origin(source.id, _TOKEN, _ORIGIN)

    conversation_store.set_session_state(source.id, {"policy_counter": 5})
    conversation_store.set_session_todos(
        source.id,
        [{"content": "Updated plan", "status": "in_progress", "activeForm": "Updating"}],
    )
    assert conversation_store.get_session_action_origin(source.id, _TOKEN) == _ORIGIN

    fork = conversation_store.fork_conversation(source.id, title="Fork without origin")
    assert conversation_store.get_session_action_origin(fork.id, _TOKEN) is None
    fork_loaded = conversation_store.get_conversation(fork.id)
    assert fork_loaded is not None
    assert fork_loaded.session_state == {}
    assert fork_loaded.session_todos == []


def test_origin_operations_are_scoped_to_the_conversation_and_workspace(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    with workspace_scope(101):
        conversation = conversation_store.create_conversation(title="Workspace origin")
        conversation_store.register_session_action_origin(conversation.id, _TOKEN, _ORIGIN)

    with workspace_scope(102):
        assert conversation_store.get_session_action_origin(conversation.id, _TOKEN) is None
        assert _bind(conversation_store, conversation.id) is None
        assert not conversation_store.release_session_action_origin(
            conversation.id, _TOKEN, _RUNNER_ID, None, _PRINCIPAL
        )
        another = conversation_store.create_conversation(title="Different conversation")
        assert conversation_store.get_session_action_origin(another.id, _TOKEN) is None

    with workspace_scope(101):
        assert conversation_store.get_session_action_origin(conversation.id, _TOKEN) == _ORIGIN


def test_origin_capacity_and_payload_limits_fail_without_eviction(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    conversation = conversation_store.create_conversation(title="Bounded origins")
    for index in range(store_module._SESSION_ACTION_ORIGINS_MAX_COUNT):
        conversation_store.register_session_action_origin(conversation.id, str(index), {})

    with pytest.raises(ValueError, match="capacity"):
        conversation_store.register_session_action_origin(conversation.id, "overflow", {})
    for index in (0, 64, 127):
        assert conversation_store.get_session_action_origin(conversation.id, str(index)) == {}

    total_limited = conversation_store.create_conversation(title="Origin map byte limit")
    first = {"payload": "a" * 4_400}
    second = {"payload": "b" * 4_400}
    conversation_store.register_session_action_origin(total_limited.id, "first", first)
    with pytest.raises(ValueError, match="storage limit"):
        conversation_store.register_session_action_origin(total_limited.id, "second", second)
    assert conversation_store.get_session_action_origin(total_limited.id, "first") == first
    assert conversation_store.get_session_action_origin(total_limited.id, "second") is None


def test_corrupt_private_origin_map_fails_closed(
    conversation_store: SqlAlchemyConversationStore,
) -> None:
    conversation = conversation_store.create_conversation(title="Corrupt origin map")
    with conversation_store._session("test_seed_corrupt_session_action_origins") as session:
        row = session.get(
            SqlConversationMetadata,
            (store_module.current_workspace_id(), conversation.id),
        )
        assert row is not None
        row.session_state = json.dumps(
            {store_module._SESSION_ACTION_ORIGINS_KEY: ["not", "a", "map"]}
        )

    with pytest.raises(TypeError, match="origin"):
        conversation_store.get_session_action_origin(conversation.id, _TOKEN)


def test_conversation_metadata_can_be_reloaded_after_store_recreation(
    db_uri: str,
) -> None:
    first_store = SqlAlchemyConversationStore(db_uri)
    conversation = first_store.create_conversation(title="Store restart")
    first_store.register_session_action_origin(conversation.id, _TOKEN, _ORIGIN)

    after_restart = SqlAlchemyConversationStore(db_uri)
    assert after_restart.get_session_action_origin(conversation.id, _TOKEN) == _ORIGIN
