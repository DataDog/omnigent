"""Authenticated server-boundary checks for standard ACP report writes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from omnigent.db.db_models import current_workspace_id
from omnigent.inner.acp_standard_subagents import (
    acp_standard_child_key,
    acp_standard_report_source_id,
    acp_standard_tool_entity_id,
)
from omnigent.runner.identity import RUNNER_TUNNEL_TOKEN_HEADER, token_bound_runner_id
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from tests.server.helpers import create_test_agent

_RUNNER_TOKEN = "standard-acp-report-runner-token"
_RUNNER_ID = token_bound_runner_id(_RUNNER_TOKEN)
_OWNER = "report-owner@example.test"
_ROOT_NATIVE = "native-root-route-test"
_NATIVE_EPOCH = "native-epoch-route-test"
_SERVER_IDENTITY = "server-route-test"
_AGENT_ID = "ag_0123456789abcdef0123456789abcdef"


def _report_event(root_id: str, *, cwd: str) -> dict[str, Any]:
    source_id = acp_standard_report_source_id(_SERVER_IDENTITY, root_id, _NATIVE_EPOCH, 1)
    return {
        "type": "external_acp_session_report",
        "data": {
            "sequence": 1,
            **_admitted_delivery(root_id),
            "native_epoch": _NATIVE_EPOCH,
            "ingress_sequence": 1,
            "source_id": source_id,
            "kind": "association.patch",
            "server_identity": _SERVER_IDENTITY,
            "root_conversation_id": root_id,
            "context_generation": "stable-context-fingerprint",
            "root_native_session_id": _ROOT_NATIVE,
            "target_session_id": "native-child-route-test",
            "parent_session_id": _ROOT_NATIVE,
            "operation": {
                "session_id": "native-child-route-test",
                "parent_session_id": _ROOT_NATIVE,
                "patch": {"title": "worker", "cwd": cwd},
            },
        },
    }


def _association_report_event(
    root_id: str,
    *,
    sequence: int,
    patch: dict[str, Any],
    target: str = "native-child-route-test",
    context_generation: str = "stable-context-fingerprint",
    delivery: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_id = acp_standard_report_source_id(_SERVER_IDENTITY, root_id, _NATIVE_EPOCH, sequence)
    return {
        "type": "external_acp_session_report",
        "data": {
            "sequence": sequence,
            **(delivery if delivery is not None else _admitted_delivery(root_id)),
            "native_epoch": _NATIVE_EPOCH,
            "ingress_sequence": sequence,
            "source_id": source_id,
            "kind": "association.patch",
            "server_identity": _SERVER_IDENTITY,
            "root_conversation_id": root_id,
            "context_generation": context_generation,
            "root_native_session_id": _ROOT_NATIVE,
            "target_session_id": target,
            "parent_session_id": _ROOT_NATIVE,
            "operation": {
                "session_id": target,
                "parent_session_id": _ROOT_NATIVE,
                "patch": patch,
            },
        },
    }


def _observation_report_event(
    root_id: str,
    *,
    sequence: int,
    observation: dict[str, Any],
    target: str = "native-child-route-test",
    context_generation: str = "stable-context-fingerprint",
) -> dict[str, Any]:
    return {
        "type": "external_acp_session_report",
        "data": {
            "sequence": sequence,
            **_admitted_delivery(root_id),
            "native_epoch": _NATIVE_EPOCH,
            "ingress_sequence": sequence,
            "source_id": acp_standard_report_source_id(
                _SERVER_IDENTITY, root_id, _NATIVE_EPOCH, sequence
            ),
            "kind": "observation.patch",
            "server_identity": _SERVER_IDENTITY,
            "root_conversation_id": root_id,
            "context_generation": context_generation,
            "root_native_session_id": _ROOT_NATIVE,
            "target_session_id": target,
            "parent_session_id": _ROOT_NATIVE,
            "operation": {"observation": observation},
        },
    }


def _tool_report_event(
    root_id: str,
    *,
    sequence: int,
    operation: dict[str, Any],
    action_origin: dict[str, Any] | None = None,
    context_generation: str = "stable-context-fingerprint",
    native_epoch: str = _NATIVE_EPOCH,
    epoch_ordinal: int = 1,
    delivery: dict[str, Any] | None = None,
    kind: str = "tool.references",
) -> dict[str, Any]:
    """Build a root tool report using one journal position as its ingress position."""
    report_data: dict[str, Any] = {
        "sequence": sequence,
        **(delivery if delivery is not None else _admitted_delivery(root_id)),
        "epoch_ordinal": epoch_ordinal,
        "native_epoch": native_epoch,
        "ingress_sequence": sequence,
        "source_id": acp_standard_report_source_id(
            _SERVER_IDENTITY, root_id, native_epoch, sequence
        ),
        "kind": kind,
        "server_identity": _SERVER_IDENTITY,
        "root_conversation_id": root_id,
        "context_generation": context_generation,
        "root_native_session_id": _ROOT_NATIVE,
        "target_session_id": _ROOT_NATIVE,
        "parent_session_id": None,
        "operation": operation,
    }
    if action_origin is not None:
        report_data["_omnigent_action_origin"] = action_origin
    return {"type": "external_acp_session_report", "data": report_data}


_DELIVERY_BY_ROOT: dict[str, dict[str, Any]] = {}


def _configured_conversation_store(db_uri: str, tmp_path: Path) -> SqlAlchemyConversationStore:
    """Use the same isolated artifact namespace as the app fixture."""
    return SqlAlchemyConversationStore(
        db_uri,
        artifact_store=LocalArtifactStore(str(tmp_path / "artifacts")),
    )


def _server_connection_id(label: str) -> str:
    """Return a stable UUID16 value for generated server connection identity."""
    return hashlib.sha256(label.encode()).hexdigest()[:32]


def _admit_report_source(
    conversation_store: SqlAlchemyConversationStore,
    root_id: str,
    *,
    runner_id: str = _RUNNER_ID,
    principal: str = _OWNER,
    native_epoch: str = _NATIVE_EPOCH,
    server_connection_id: str | None = None,
) -> dict[str, Any]:
    connection_id = server_connection_id or _server_connection_id(
        f"route-test-connection:{root_id}"
    )
    attempt_id = conversation_store.prepare_acp_report_connection_attempt(
        root_id, runner_id, principal, connection_id
    )
    context = SimpleNamespace(
        runner_id=runner_id,
        owner=principal,
        server_connection_id=connection_id,
        superseded=False,
    )
    conversation_store.admit_acp_report_connection(attempt_id, context)
    journal_id = hashlib.sha256(root_id.encode()).hexdigest()[:32]
    conversation_store.admit_acp_report_owner(root_id, attempt_id, 1, journal_id)
    conversation_store.activate_acp_report_epoch(
        root_id, attempt_id, 1, journal_id, native_epoch, 1
    )
    delivery = {
        "connection_admission_id": attempt_id,
        "runtime_incarnation": 1,
        "journal_id": journal_id,
        "epoch_ordinal": 1,
    }
    _DELIVERY_BY_ROOT[root_id] = delivery
    return delivery


def _admitted_delivery(root_id: str) -> dict[str, Any]:
    return _DELIVERY_BY_ROOT[root_id]


def _persist_private_association(
    conversation_store: SqlAlchemyConversationStore,
    *,
    runner_id: str = _RUNNER_ID,
    action_origin_token: str = "private-origin-token-for-test",
) -> tuple[str, str, str]:
    """Create a durable child association carrying a private origin token."""
    root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=runner_id)
    child = conversation_store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=root.id,
        agent_id=_AGENT_ID,
        runner_id=runner_id,
        title="worker:private-inventory-test",
    )
    native_epoch = "native-epoch-private-inventory"
    root_native_session_id = "native-root-private-inventory"
    target_session_id = "native-child-private-inventory"
    context_generation = "private-inventory-context"
    source_id = acp_standard_report_source_id("private-inventory-server", root.id, native_epoch, 1)
    child_key = acp_standard_child_key(root.id, native_epoch, target_session_id)
    conversation_store.set_labels(child.id, {"omnigent.acp.subagent_id": child_key})
    operation = {
        "session_id": target_session_id,
        "parent_session_id": root_native_session_id,
        "patch": {"title": "worker"},
    }
    payload_hash = hashlib.sha256(
        json.dumps(operation, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    delivery = _admit_report_source(
        conversation_store,
        root.id,
        runner_id=runner_id,
        principal=_OWNER,
        native_epoch=native_epoch,
    )
    conversation_store.apply_acp_session_report(
        root_conversation_id=root.id,
        **delivery,
        journal_sequence=1,
        native_epoch=native_epoch,
        ingress_sequence=1,
        source_id=source_id,
        payload_hash=payload_hash,
        context_generation=context_generation,
        root_native_session_id=root_native_session_id,
        target_session_id=target_session_id,
        parent_session_id=root_native_session_id,
        conversation_id=child.id,
        kind="association.patch",
        operation=operation,
        action_origin={
            "token": action_origin_token,
            "agent": "test-agent",
            "generation": "private-inventory-generation",
        },
    )
    return root.id, child.id, action_origin_token


async def test_invalid_first_association_does_not_mint_child_or_metadata(
    app, db_uri: str, tmp_path: Path
) -> None:
    """Rejected association content must fail before publishing a child row."""
    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }

    valid_root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=_RUNNER_ID)
    _admit_report_source(conversation_store, valid_root.id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        valid_response = await client.post(
            f"/v1/sessions/{valid_root.id}/events",
            headers=headers,
            json=_report_event(valid_root.id, cwd="/workspace/child"),
        )
    assert valid_response.status_code == 202, valid_response.text
    assert conversation_store.list_child_conversation_ids_page(valid_root.id)
    assert (
        conversation_store.get_acp_session_association(
            valid_root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        is not None
    )

    invalid_root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=_RUNNER_ID)
    _admit_report_source(conversation_store, invalid_root.id)
    children_before = conversation_store.list_child_conversation_ids_page(invalid_root.id)
    root_items_before = conversation_store.list_items(invalid_root.id).data

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        invalid_response = await client.post(
            f"/v1/sessions/{invalid_root.id}/events",
            headers=headers,
            json=_report_event(invalid_root.id, cwd="relative/path"),
        )

    assert invalid_response.status_code == 400, invalid_response.text
    assert "standard ACP association cwd" in invalid_response.json()["error"]["message"]
    assert conversation_store.list_child_conversation_ids_page(invalid_root.id) == children_before
    assert conversation_store.list_items(invalid_root.id).data == root_items_before
    assert (
        conversation_store.get_acp_session_association(
            invalid_root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        is None
    )


async def test_ordinary_http_cannot_admit_a_prepared_report_connection(
    app, db_uri: str, tmp_path: Path
) -> None:
    """Only the captured runner tunnel may consume a source-admission ticket."""
    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=_RUNNER_ID)
    attempt_a = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _OWNER, _server_connection_id("http-attempt-a")
    )
    attempt_b = conversation_store.prepare_acp_report_connection_attempt(
        root.id, _RUNNER_ID, _OWNER, _server_connection_id("http-attempt-b")
    )
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json={
                "type": "external_acp_report_connection_admit",
                "data": {"attempt_id": attempt_a},
            },
        )

    assert response.status_code != 202, response.text
    winner = conversation_store.admit_acp_report_connection(
        attempt_b,
        SimpleNamespace(
            runner_id=_RUNNER_ID,
            owner=_OWNER,
            server_connection_id=_server_connection_id("http-attempt-b"),
            superseded=False,
        ),
    )
    assert winner["current"] is True
    with pytest.raises(ValueError, match="predecessor revision changed"):
        conversation_store.admit_acp_report_connection(
            attempt_a,
            SimpleNamespace(
                runner_id=_RUNNER_ID,
                owner=_OWNER,
                server_connection_id=_server_connection_id("http-attempt-a"),
                superseded=False,
            ),
        )


async def test_report_body_cannot_introduce_an_unadmitted_journal(
    app, db_uri: str, tmp_path: Path
) -> None:
    """Caller-supplied delivery metadata cannot establish report-source authority."""
    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=_RUNNER_ID)
    _DELIVERY_BY_ROOT[root.id] = {
        "connection_admission_id": "f" * 32,
        "runtime_incarnation": 1,
        "journal_id": "e" * 32,
        "epoch_ordinal": 1,
    }
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=_association_report_event(root.id, sequence=1, patch={"title": "unadmitted"}),
        )

    assert response.status_code == 409, response.text
    assert conversation_store.list_child_conversation_ids_page(root.id) == []
    assert (
        conversation_store.get_acp_session_association(
            root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        is None
    )


async def test_root_tool_references_replay_after_origin_release_only_with_exact_anchor(
    auth_app, auth_client: httpx.AsyncClient, db_uri: str, tmp_path: Path
) -> None:
    """An accepted root call anchor authorizes later explicit reference updates."""
    from omnigent.runtime.agent_cache import AgentCache
    from omnigent.session_action_origin import (
        effective_policy_scope_digest,
        effective_spec_digest,
        operator_environment_digest,
    )

    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    agent_data = await create_test_agent(
        auth_client,
        executor={"type": "omnigent", "config": {"harness": "acp:gemini"}},
        user=_OWNER,
    )
    root = conversation_store.get_conversation(agent_data["_session_id"])
    assert root is not None
    agent_id = agent_data["id"]
    assert conversation_store.set_runner_id(root.id, _RUNNER_ID)
    _admit_report_source(conversation_store, root.id)
    action_token = "a" * 32
    action_generation = "root-tool-origin-generation"
    call_id = "root-call-after-origin-release"
    original_response_id = "original-root-response"
    stored_agent = auth_app.state.agent_store.get(agent_id)
    assert stored_agent is not None
    origin_spec = (
        AgentCache(
            LocalArtifactStore(str(tmp_path / "artifacts")),
            tmp_path / "action-origin-cache",
        )
        .load(agent_id, stored_agent.bundle_location)
        .spec
    )
    action_origin = {
        "token": action_token,
        "agent": "test-agent",
        "generation": action_generation,
    }
    conversation_store.register_session_action_origin(
        root.id,
        action_token,
        {
            "session_id": root.id,
            "workspace_id": current_workspace_id(),
            "runner_id": _RUNNER_ID,
            "turn_id": "root-tool-reference-turn",
            "agent_id": agent_id,
            "bundle_location": stored_agent.bundle_location,
            "agent_provenance": "tenant",
            "operator_authored": False,
            "operator_environment_digest": operator_environment_digest(
                origin_spec, operator_authored=False
            ),
            "policy_scope_digest": effective_policy_scope_digest(origin_spec),
            "mcp_selection_snapshot": {},
            "effective_spec_digest": effective_spec_digest(origin_spec),
            "actor": _OWNER,
        },
    )
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }
    bind_response = await auth_client.post(
        f"/v1/sessions/{root.id}/action-origins/{action_token}/bind",
        headers=headers,
        json={"generation": action_generation},
    )
    assert bind_response.status_code == 200, bind_response.text
    context_generation = bind_response.json()["context_generation"]
    assert isinstance(context_generation, str) and context_generation
    call_entity_id = acp_standard_tool_entity_id(call_id)
    anchor = _tool_report_event(
        root.id,
        sequence=1,
        context_generation=context_generation,
        operation={
            "entity_id": call_entity_id,
            "call_id": call_id,
            "response_id": original_response_id,
        },
        action_origin=action_origin,
    )
    entity_upsert = _tool_report_event(
        root.id,
        sequence=2,
        context_generation=context_generation,
        kind="entity.upsert",
        operation={
            "entity_type": "item",
            "entity_id": call_entity_id,
            "call_id": call_id,
            "value": {
                "item_type": "function_call",
                "response_id": original_response_id,
                "item_data": {
                    "agent": "test-agent",
                    "call_id": call_id,
                    "name": "worker",
                    "arguments": "{}",
                },
            },
        },
    )
    references_update = _tool_report_event(
        root.id,
        sequence=3,
        context_generation=context_generation,
        operation={
            "entity_id": call_entity_id,
            "call_id": call_id,
            "references": ["native-child-reference"],
        },
    )
    matching_provenance_update = _tool_report_event(
        root.id,
        sequence=4,
        context_generation=context_generation,
        operation={
            "entity_id": call_entity_id,
            "call_id": call_id,
            "references": ["native-child-reference"],
        },
        action_origin=action_origin,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=auth_app), base_url="http://test"
    ) as client:
        anchored = await client.post(
            f"/v1/sessions/{root.id}/events", headers=headers, json=anchor
        )
        assert anchored.status_code == 202, anchored.text
        settled = await auth_client.post(
            f"/v1/sessions/{root.id}/action-origins/{action_token}/settle",
            headers=headers,
            json={"generation": action_generation},
        )
        assert settled.status_code == 200, settled.text

        upserted = await client.post(
            f"/v1/sessions/{root.id}/events", headers=headers, json=entity_upsert
        )
        assert upserted.status_code == 202, upserted.text
        updated = await client.post(
            f"/v1/sessions/{root.id}/events", headers=headers, json=references_update
        )
        assert updated.status_code == 202, updated.text

        matching_provenance = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=matching_provenance_update,
        )
        assert matching_provenance.status_code == 202, matching_provenance.text

        wrong_provenance = _tool_report_event(
            root.id,
            sequence=5,
            context_generation=context_generation,
            operation={
                "entity_id": call_entity_id,
                "call_id": call_id,
                "references": [],
            },
            action_origin={**action_origin, "token": "b" * 32},
        )
        rejected = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=wrong_provenance,
        )
        assert rejected.status_code == 409, rejected.text

        after_rejection = conversation_store.list_items(root.id).data
        persisted_call = next(
            item for item in after_rejection if getattr(item.data, "call_id", None) == call_id
        )
        assert persisted_call.response_id == original_response_id
        assert [
            reference.native_session_id
            for reference in persisted_call.data.acp_session_references or []
        ] == ["native-child-reference"]

        wrong_context = _tool_report_event(
            root.id,
            sequence=5,
            context_generation=f"different-{context_generation}",
            operation={
                "entity_id": call_entity_id,
                "call_id": call_id,
                "references": ["native-child-reference"],
            },
        )
        context_rejected = await client.post(
            f"/v1/sessions/{root.id}/events", headers=headers, json=wrong_context
        )
        assert context_rejected.status_code == 409, context_rejected.text

        cleared = _tool_report_event(
            root.id,
            sequence=5,
            context_generation=context_generation,
            operation={
                "entity_id": call_entity_id,
                "call_id": call_id,
                "references": [],
            },
        )
        cleared_response = await client.post(
            f"/v1/sessions/{root.id}/events", headers=headers, json=cleared
        )
        assert cleared_response.status_code == 202, cleared_response.text
        after_clear = conversation_store.list_items(root.id).data
        cleared_call = next(
            item for item in after_clear if getattr(item.data, "call_id", None) == call_id
        )
        assert cleared_call.data.acp_session_references == []

        missing_live_anchor = _tool_report_event(
            root.id,
            sequence=6,
            context_generation=context_generation,
            action_origin=action_origin,
            operation={
                "entity_id": acp_standard_tool_entity_id("unanchored-call"),
                "call_id": "unanchored-call",
                "response_id": "unanchored-response",
            },
        )
        anchor_rejected = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=missing_live_anchor,
        )
        assert anchor_rejected.status_code == 403, anchor_rejected.text

        association_rejected = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json={
                **_association_report_event(
                    root.id,
                    sequence=6,
                    patch={"title": "worker"},
                    context_generation=context_generation,
                ),
                "data": {
                    **_association_report_event(
                        root.id,
                        sequence=6,
                        patch={"title": "worker"},
                        context_generation=context_generation,
                    )["data"],
                    "_omnigent_action_origin": action_origin,
                },
            },
        )
        assert association_rejected.status_code == 403, association_rejected.text


@pytest.mark.parametrize(
    ("second_title", "expected_statuses"),
    [
        ("worker", (202, 202)),
        ("competing worker", (202, 409)),
    ],
)
async def test_concurrent_association_ingress_has_one_child_and_activity(
    app,
    db_uri: str,
    tmp_path: Path,
    monkeypatch,
    second_title: str,
    expected_statuses: tuple[int, ...],
) -> None:
    """Same-content retries or conflicting ingress cannot duplicate child side effects."""
    from omnigent.server.routes._sessions import helpers
    from omnigent.server.routes.sessions import routes_events

    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }
    root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=_RUNNER_ID)
    _admit_report_source(conversation_store, root.id)
    first_report = _association_report_event(
        root.id,
        sequence=1,
        patch={"title": "worker", "state": {"state": "running"}},
    )
    second_target = (
        "native-child-route-test"
        if second_title == "worker"
        else "native-competing-child-route-test"
    )
    second_report = _association_report_event(
        root.id,
        sequence=1,
        patch={"title": second_title, "state": {"state": "running"}},
        target=second_target,
    )

    original_reserve = SqlAlchemyConversationStore.reserve_acp_session_report
    reservation_lock = threading.Lock()
    reservation_count = 0
    first_at_reservation = threading.Event()
    second_at_reservation = threading.Event()
    release_first_reservation = threading.Event()

    def synchronized_reserve(store, *args, **kwargs):
        nonlocal reservation_count
        if kwargs.get("root_conversation_id") == root.id:
            with reservation_lock:
                reservation_count += 1
                current_reservation = reservation_count
            if current_reservation == 1:
                first_at_reservation.set()
                if not release_first_reservation.wait(timeout=5):
                    raise TimeoutError("concurrent reservation was not released")
            elif current_reservation == 2:
                second_at_reservation.set()
        return original_reserve(store, *args, **kwargs)

    monkeypatch.setattr(
        SqlAlchemyConversationStore,
        "reserve_acp_session_report",
        synchronized_reserve,
    )

    created_children: list[str] = []
    original_publish_created = helpers._publish_session_created

    async def record_session_created(parent_id, child_id, agent_id, store, **kwargs):
        created_children.append(child_id)
        await original_publish_created(parent_id, child_id, agent_id, store, **kwargs)

    monkeypatch.setattr(helpers, "_publish_session_created", record_session_created)

    published_statuses: list[tuple[str, str]] = []
    original_publish_status = routes_events._publish_status

    def record_status(session_id: str, status: str, *args, **kwargs):
        published_statuses.append((session_id, status))
        return original_publish_status(session_id, status, *args, **kwargs)

    monkeypatch.setattr(routes_events, "_publish_status", record_status)

    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://test") as first_client,
        httpx.AsyncClient(transport=transport, base_url="http://test") as second_client,
    ):
        first_request = asyncio.create_task(
            first_client.post(f"/v1/sessions/{root.id}/events", headers=headers, json=first_report)
        )
        assert await asyncio.to_thread(first_at_reservation.wait, 5)

        second_request = asyncio.create_task(
            second_client.post(
                f"/v1/sessions/{root.id}/events", headers=headers, json=second_report
            )
        )
        second_reservation_wait = asyncio.create_task(
            asyncio.to_thread(second_at_reservation.wait, 5)
        )
        done, _pending = await asyncio.wait(
            {second_request, second_reservation_wait},
            timeout=5,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert done, "second request neither reserved nor returned a pre-mint conflict"
        second_reached_reservation = second_at_reservation.is_set()
        release_first_reservation.set()
        second_at_reservation.set()
        first_response, second_response = await asyncio.gather(first_request, second_request)

    responses = [
        (first_report, first_response),
        (second_report, second_response),
    ]
    assert sorted(response.status_code for _report, response in responses) == sorted(
        expected_statuses
    )
    if second_title == "worker":
        assert second_reached_reservation
        assert all(response.status_code == 202 for _report, response in responses)
    accepted = [
        (accepted_report, response)
        for accepted_report, response in responses
        if response.status_code == 202
    ]
    child_ids = {response.json()["child_session_id"] for _report, response in accepted}
    assert len(child_ids) == 1
    child_id = next(iter(child_ids))
    accepted_titles = {
        report_data["data"]["operation"]["patch"]["title"] for report_data, _response in accepted
    }
    accepted_targets = {
        report_data["data"]["target_session_id"] for report_data, _response in accepted
    }
    assert len(accepted_titles) == 1
    assert len(accepted_targets) == 1
    children = conversation_store.list_child_conversation_ids_page(root.id)
    assert [item[0] for item in children] == [child_id]
    assert created_children == [child_id]
    assert published_statuses == [(child_id, "running")]
    persisted_child = conversation_store.get_conversation(child_id)
    assert persisted_child is not None
    assert any(persisted_child.title.startswith(f"{title}:") for title in accepted_titles)
    accepted_target = next(iter(accepted_targets))
    association = conversation_store.get_acp_session_association(
        root.id, _NATIVE_EPOCH, accepted_target
    )
    assert association is not None
    assert association["conversation_id"] == child_id
    rejected_targets = {
        report_data["data"]["target_session_id"]
        for report_data, response in responses
        if response.status_code == 409
    }
    for rejected_target in rejected_targets:
        assert (
            conversation_store.get_acp_session_association(root.id, _NATIVE_EPOCH, rejected_target)
            is None
        )
        rejected_child_key = acp_standard_child_key(root.id, _NATIVE_EPOCH, rejected_target)
        assert (
            helpers._find_acp_subagent_child(conversation_store, root.id, rejected_child_key)
            is None
        )
    delegated_activity_child_ids = {
        item.data.resource_id
        for item in conversation_store.list_items(root.id).data
        if getattr(item.data, "event_type", None) == "session.subagent.delegated"
    }
    assert delegated_activity_child_ids == {child_id}


async def test_cancel_capability_is_observed_without_enabling_child_controls(
    app, db_uri: str, tmp_path: Path, monkeypatch
) -> None:
    """Capability snapshots survive association patches while mirrors stay read-only."""
    from omnigent.server.routes.sessions import routes_events

    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }
    root = conversation_store.create_conversation(agent_id=_AGENT_ID, runner_id=_RUNNER_ID)
    _admit_report_source(conversation_store, root.id)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=_association_report_event(
                root.id,
                sequence=1,
                patch={
                    "title": "worker",
                    "capabilities": {"cancel": {}},
                    "state": {"state": "running"},
                },
            ),
        )
        assert first.status_code == 202, first.text
        association = conversation_store.get_acp_session_association(
            root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        assert association is not None
        child_id = association["conversation_id"]
        assert association["association"]["capabilities"] == {"cancel": {}}

        async def unexpected_effect(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("a read-only ACP mirror action reached runner work")

        with monkeypatch.context() as effects:
            for function_name in (
                "_get_runner_client",
                "_dispatch_session_event_to_runner",
                "_launch_runner_on_host",
                "ensure_runner_connected",
                "_retry_session_single_flight",
                "_stop_session_via_runner",
            ):
                effects.setattr(routes_events, function_name, unexpected_effect)

            before_session = await client.get(
                f"/v1/sessions/{child_id}", headers={"X-Forwarded-Email": _OWNER}
            )
            before_items = await client.get(
                f"/v1/sessions/{child_id}/items", headers={"X-Forwarded-Email": _OWNER}
            )
            assert before_session.status_code == 200, before_session.text
            assert before_items.status_code == 200, before_items.text
            for payload in ({"type": "interrupt"}, {"type": "stop_session"}):
                refused = await client.post(
                    f"/v1/sessions/{child_id}/events",
                    headers={"X-Forwarded-Email": _OWNER},
                    json=payload,
                )
                assert refused.status_code == 409, refused.text
                assert "ACP sub-agent sessions mirror work" in refused.text

            stale = await client.post(
                f"/v1/sessions/{root.id}/events",
                headers=headers,
                json=_association_report_event(
                    root.id,
                    sequence=2,
                    context_generation="stale-context-fingerprint",
                    patch={"capabilities": None},
                ),
            )
            assert stale.status_code == 409, stale.text
            association = conversation_store.get_acp_session_association(
                root.id, _NATIVE_EPOCH, "native-child-route-test"
            )
            assert association is not None
            assert association["association"]["capabilities"] == {"cancel": {}}

        sparse = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=_association_report_event(
                root.id, sequence=2, patch={"description": "still observed"}
            ),
        )
        assert sparse.status_code == 202, sparse.text
        association = conversation_store.get_acp_session_association(
            root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        assert association is not None
        assert association["association"]["capabilities"] == {"cancel": {}}

        cleared = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=_association_report_event(root.id, sequence=3, patch={"capabilities": None}),
        )
        assert cleared.status_code == 202, cleared.text
        association = conversation_store.get_acp_session_association(
            root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        assert association is not None
        assert association["association"]["capabilities"] is None

        removed = await client.post(
            f"/v1/sessions/{root.id}/events",
            headers=headers,
            json=_association_report_event(root.id, sequence=4, patch={"capabilities": {}}),
        )
        assert removed.status_code == 202, removed.text
        association = conversation_store.get_acp_session_association(
            root.id, _NATIVE_EPOCH, "native-child-route-test"
        )
        assert association is not None
        assert association["association"]["capabilities"] == {}

        after_session = await client.get(
            f"/v1/sessions/{child_id}", headers={"X-Forwarded-Email": _OWNER}
        )
        after_items = await client.get(
            f"/v1/sessions/{child_id}/items", headers={"X-Forwarded-Email": _OWNER}
        )
        assert after_session.status_code == 200, after_session.text
        assert after_items.status_code == 200, after_items.text
        assert after_session.json()["status"] == before_session.json()["status"]
        assert after_items.json()["data"] == before_items.json()["data"]


async def test_child_session_route_recovers_historical_acp_state_after_epoch_reconnect(
    auth_client: httpx.AsyncClient,
    db_uri: str,
    tmp_path: Path,
) -> None:
    """Current idle observations stay raw and nonbusy; old sources lose eligibility."""
    conversation_store = _configured_conversation_store(db_uri, tmp_path)
    agent = await create_test_agent(auth_client, user=_OWNER)
    root_id = agent["_session_id"]
    assert conversation_store.set_runner_id(root_id, _RUNNER_ID)
    delivery = _admit_report_source(
        conversation_store,
        root_id,
        native_epoch=_NATIVE_EPOCH,
    )
    headers = {
        "X-Forwarded-Email": _OWNER,
        RUNNER_TUNNEL_TOKEN_HEADER: _RUNNER_TOKEN,
    }

    first = await auth_client.post(
        f"/v1/sessions/{root_id}/events",
        headers=headers,
        json=_association_report_event(
            root_id,
            sequence=1,
            patch={"title": "worker", "state": {"state": "running"}},
        ),
    )
    assert first.status_code == 202, first.text
    child_id = first.json()["child_session_id"]

    current_roster = await auth_client.get(
        f"/v1/sessions/{root_id}/child_sessions",
        headers={"X-Forwarded-Email": _OWNER},
    )
    assert current_roster.status_code == 200, current_roster.text
    current_row = next(row for row in current_roster.json()["data"] if row["id"] == child_id)
    assert current_row["busy"] is True
    assert current_row["current_task_status"] == "in_progress"
    assert current_row["acp_session_observation"]["source_current"] is True
    assert current_row["acp_session_observation"]["state"] == {
        "present": True,
        "value": {"state": "running"},
    }

    from omnigent.server.routes.sessions import routes_events
    from tests.server.helpers import start_session_stream_collector

    child_updates = await start_session_stream_collector(root_id)
    try:
        idle_event = _observation_report_event(
            root_id,
            sequence=2,
            observation={
                "state": {
                    "present": True,
                    "value": {"state": "idle", "stopReason": "unknown native reason"},
                },
                "transport": {"state": "connected"},
            },
        )
        idle_response = await auth_client.post(
            f"/v1/sessions/{root_id}/events",
            headers=headers,
            json=idle_event,
        )
        assert idle_response.status_code == 202, idle_response.text

        live_idle_update = None
        while live_idle_update is None:
            event = await asyncio.wait_for(child_updates.queue.get(), timeout=5.0)
            child = event.get("child")
            observation = child.get("acp_session_observation") if isinstance(child, dict) else None
            if (
                event.get("type") == "session.child_session.updated"
                and event.get("child_session_id") == child_id
                and isinstance(observation, dict)
                and observation.get("state_ingress_sequence") == 2
            ):
                live_idle_update = child
        assert live_idle_update["busy"] is False
        assert live_idle_update["current_task_status"] is None
        assert live_idle_update["acp_session_observation"]["state"] == {
            "present": True,
            "value": {"state": "idle", "stopReason": "unknown native reason"},
        }
        assert live_idle_update["acp_session_observation"]["state_ingress_sequence"] == 2
        assert live_idle_update["acp_session_observation"]["transport"] == {"state": "connected"}

        idle_roster = await auth_client.get(
            f"/v1/sessions/{root_id}/child_sessions",
            headers={"X-Forwarded-Email": _OWNER},
        )
        idle_row = next(row for row in idle_roster.json()["data"] if row["id"] == child_id)
        assert idle_row["busy"] is False
        assert idle_row["current_task_status"] is None
        assert idle_row["acp_session_observation"]["source_current"] is True
        assert idle_row["acp_session_observation"] == live_idle_update["acp_session_observation"]

        # Unrecognized and null states and a disconnected idle report keep
        # the last relay cache value from becoming an observed lifecycle edge.
        routes_events._session_status_cache[child_id] = "waiting"
        for sequence, observation in (
            (3, {"state": {"present": True, "value": {"state": "unknown"}}}),
            (4, {"state": None}),
            (
                5,
                {
                    "state": {
                        "present": True,
                        "value": {"state": "idle", "stopReason": "unknown native reason"},
                    },
                    "transport": {"state": "disconnected"},
                },
            ),
        ):
            guarded = await auth_client.post(
                f"/v1/sessions/{root_id}/events",
                headers=headers,
                json=_observation_report_event(
                    root_id,
                    sequence=sequence,
                    observation=observation,
                ),
            )
            assert guarded.status_code == 202, guarded.text
            assert routes_events._session_status_cache[child_id] == "waiting"
            guarded_roster = await auth_client.get(
                f"/v1/sessions/{root_id}/child_sessions",
                headers={"X-Forwarded-Email": _OWNER},
            )
            guarded_row = next(
                row for row in guarded_roster.json()["data"] if row["id"] == child_id
            )
            assert guarded_row["busy"] is False
            assert guarded_row["current_task_status"] is None
            assert guarded_row["acp_session_observation"]["source_current"] is True
    finally:
        await child_updates.stop()
        routes_events._session_status_cache.pop(child_id, None)

    server_connection_id = _server_connection_id(f"reconnected:{root_id}")
    attempt_id = conversation_store.prepare_acp_report_connection_attempt(
        root_id,
        _RUNNER_ID,
        _OWNER,
        server_connection_id,
    )
    conversation_store.admit_acp_report_connection(
        attempt_id,
        SimpleNamespace(
            runner_id=_RUNNER_ID,
            owner=_OWNER,
            server_connection_id=server_connection_id,
            superseded=False,
        ),
    )
    conversation_store.admit_acp_report_owner(
        root_id,
        attempt_id,
        delivery["runtime_incarnation"],
        delivery["journal_id"],
    )
    conversation_store.activate_acp_report_epoch(
        root_id,
        attempt_id,
        delivery["runtime_incarnation"],
        delivery["journal_id"],
        "native-epoch-after-reconnect",
        2,
    )

    recovered_roster = await auth_client.get(
        f"/v1/sessions/{root_id}/child_sessions",
        headers={"X-Forwarded-Email": _OWNER},
    )
    assert recovered_roster.status_code == 200, recovered_roster.text
    recovered_row = next(row for row in recovered_roster.json()["data"] if row["id"] == child_id)
    assert recovered_row["busy"] is False
    assert recovered_row["current_task_status"] is None
    assert recovered_row["acp_session_observation"]["source_current"] is False
    assert recovered_row["acp_session_observation"]["state"] == {
        "present": True,
        "value": {"state": "idle", "stopReason": "unknown native reason"},
    }
    assert recovered_row["acp_session_observation"]["transport"] == {"state": "disconnected"}

    from omnigent.runtime import session_stream

    stream_task = asyncio.create_task(
        auth_client.get(
            f"/v1/sessions/{root_id}/stream",
            headers={"X-Forwarded-Email": _OWNER},
        )
    )
    try:
        for _ in range(200):
            session_stream.close(root_id)
            if stream_task.done():
                break
            await asyncio.sleep(0.01)
        stream_response = await asyncio.wait_for(stream_task, timeout=2.0)
        assert stream_response.status_code == 200, stream_response.text
    finally:
        if not stream_task.done():
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)
        session_stream.close(root_id)

    snapshot_events = [
        json.loads(line[len("data: ") :])
        for line in stream_response.text.splitlines()
        if line.startswith("data: ") and line != "data: [DONE]"
    ]
    assert any(
        event.get("type") == "session.child_sessions.invalidated" for event in snapshot_events
    )
    recovered_snapshot = next(
        event
        for event in snapshot_events
        if event.get("type") == "session.child_session.updated"
        and event.get("child_session_id") == child_id
    )
    assert recovered_snapshot["child"]["acp_session_observation"]["source_current"] is False
    assert recovered_snapshot["child"]["acp_session_observation"]["state"] == {
        "present": True,
        "value": {"state": "idle", "stopReason": "unknown native reason"},
    }
    assert recovered_snapshot["child"]["acp_session_observation"]["transport"] == {
        "state": "disconnected"
    }
