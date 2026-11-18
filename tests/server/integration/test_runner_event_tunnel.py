"""Runner event ingestion across a real server tunnel and a lost ACK."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

import httpx
import pytest
from asgiref.testing import ApplicationCommunicator
from fastapi import FastAPI

from omnigent.runner.transports.ws_tunnel.event_delivery import (
    RunnerEventDispatcher,
    TunnelEventClient,
)
from omnigent.runner.transports.ws_tunnel.frames import (
    EVENT_INGEST_CAPABILITY,
    EventAckFrame,
    EventBatchFrame,
    EventReadyFrame,
    HelloFrame,
    decode_frame,
    encode_frame,
)
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from tests.budgets import budget
from tests.server.helpers import create_test_agent

_RUNNER_ID = "runner-event-ingress"
_ITEM = {
    "type": "external_conversation_item",
    "data": {
        "source_id": "native-record-1",
        "item_type": "message",
        "response_id": "response-1",
        "item_data": {
            "role": "assistant",
            "agent": "claude-native-ui",
            "content": [{"type": "output_text", "text": "recovered after restart"}],
        },
    },
}


async def _connect(app: FastAPI, runner_id: str = _RUNNER_ID) -> ApplicationCommunicator:
    path = f"/v1/runners/{runner_id}/tunnel"
    comm = ApplicationCommunicator(
        app,
        {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "scheme": "ws",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 50000),
            "server": ("testserver", 80),
            "subprotocols": [],
        },
    )
    await comm.send_input({"type": "websocket.connect"})
    assert (await comm.receive_output(timeout=budget(2.0)))["type"] == "websocket.accept"
    await comm.send_input(
        {
            "type": "websocket.receive",
            "text": encode_frame(
                HelloFrame(
                    runner_version="test",
                    frame_protocol_version=1,
                    capabilities=[EVENT_INGEST_CAPABILITY],
                )
            ),
        }
    )
    ready = await comm.receive_output(timeout=budget(2.0))
    assert isinstance(decode_frame(ready["text"]), EventReadyFrame)
    return comm


async def _read_ack(comm: ApplicationCommunicator) -> EventAckFrame:
    # Runner recovery RPCs share this socket with event acknowledgements.
    for _ in range(10):
        frame = decode_frame((await comm.receive_output(timeout=budget(3.0)))["text"])
        if isinstance(frame, EventAckFrame):
            return frame
    raise AssertionError("server never acknowledged the event batch")


async def _send_control_event(
    comm: ApplicationCommunicator,
    *,
    session_id: str,
    batch_id: str,
    event_type: str,
    data: dict[str, object],
) -> EventAckFrame:
    await comm.send_input(
        {
            "type": "websocket.receive",
            "text": encode_frame(
                EventBatchFrame(
                    id=batch_id,
                    session_id=session_id,
                    events=[{"type": event_type, "data": data}],
                )
            ),
        }
    )
    return await _read_ack(comm)


async def _close(comm: ApplicationCommunicator) -> None:
    await comm.send_input({"type": "websocket.disconnect", "code": 1000})
    with contextlib.suppress(asyncio.TimeoutError):
        await comm.wait(timeout=budget(2.0))


class _DirectSessionStreamSubscriber:
    """Queue events from one direct session-stream subscription."""

    def __init__(self, session_id: str) -> None:
        self._session_id = session_id
        self._ready_event = {"type": "test.subscribed"}
        self._queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        self._registered = asyncio.Event()
        self._task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        from omnigent.runtime import session_stream

        async for event in session_stream.subscribe(
            self._session_id, ready_event=self._ready_event
        ):
            if event is self._ready_event:
                self._registered.set()
            else:
                self._queue.put_nowait(event)

    async def wait_until_registered(self) -> None:
        await asyncio.wait_for(self._registered.wait(), timeout=2.0)

    async def next_event(self, timeout: float = 2.0) -> dict[str, object]:
        return await asyncio.wait_for(self._queue.get(), timeout=timeout)

    async def assert_no_event(self, within: float) -> None:
        try:
            event = await asyncio.wait_for(self._queue.get(), timeout=within)
        except TimeoutError:
            return
        raise AssertionError(f"unexpected direct stream event: {event!r}")

    async def stop(self) -> None:
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)


async def _subscribe_session_stream(session_id: str) -> _DirectSessionStreamSubscriber:
    """Subscribe directly and wait until the stream slot is registered."""
    subscriber = _DirectSessionStreamSubscriber(session_id)
    await subscriber.wait_until_registered()
    return subscriber


async def test_acp_report_admission_controls_retry_over_registered_runner_tunnel(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201
    root_id = created.json()["id"]
    store = SqlAlchemyConversationStore(db_uri)
    assert store.set_runner_id(root_id, _RUNNER_ID)
    root = store.get_conversation(root_id)
    assert root is not None

    comm = await _connect(app)
    try:
        registry = app.state.tunnel_registry
        connection = registry.get(_RUNNER_ID)
        assert connection is not None
        initializer = app.state.runner_session_initializer
        attempt_id = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(attempt_id, str) and attempt_id

        connection_event = {
            "attempt_id": attempt_id,
        }
        for batch_id in ("connection-admit", "connection-admit-retry"):
            ack = await _send_control_event(
                comm,
                session_id=root_id,
                batch_id=batch_id,
                event_type="external_acp_report_connection_admit",
                data=connection_event,
            )
            assert ack.applied == 1 and ack.error is None

        owner_event = {
            "connection_admission_id": attempt_id,
            "runtime_incarnation": 3,
            "journal_id": "b" * 32,
        }
        for batch_id in ("owner-admit", "owner-admit-retry"):
            ack = await _send_control_event(
                comm,
                session_id=root_id,
                batch_id=batch_id,
                event_type="external_acp_report_owner_admit",
                data=owner_event,
            )
            assert ack.applied == 1 and ack.error is None

        epoch_event = {
            **owner_event,
            "native_epoch": "native-epoch-tunnel-test",
            "epoch_ordinal": 1,
        }
        for batch_id in ("epoch-activate", "epoch-activate-retry"):
            ack = await _send_control_event(
                comm,
                session_id=root_id,
                batch_id=batch_id,
                event_type="external_acp_report_epoch_activate",
                data=epoch_event,
            )
            assert ack.applied == 1 and ack.error is None
    finally:
        await _close(comm)


async def test_acp_report_admission_rejects_stale_copied_and_wrong_runner_contexts(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    agent = await create_test_agent(client)
    store = SqlAlchemyConversationStore(db_uri)
    session_ids: list[str] = []
    for _ in range(2):
        created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
        assert created.status_code == 201
        session_id = created.json()["id"]
        assert store.set_runner_id(session_id, _RUNNER_ID)
        session_ids.append(session_id)
    root_id, copied_root_id = session_ids
    root = store.get_conversation(root_id)
    assert root is not None
    initializer = app.state.runner_session_initializer

    first = await _connect(app)
    first_open = True
    replacement: ApplicationCommunicator | None = None
    replacement_open = False
    latest: ApplicationCommunicator | None = None
    wrong_runner: ApplicationCommunicator | None = None
    try:
        registry = app.state.tunnel_registry
        superseded_connection = registry.get(_RUNNER_ID)
        assert superseded_connection is not None
        stale_attempt_id = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(stale_attempt_id, str) and stale_attempt_id

        replacement = await _connect(app)
        first_open = False
        replacement_open = True
        current_connection = registry.get(_RUNNER_ID)
        assert current_connection is not None
        assert current_connection is not superseded_connection
        assert superseded_connection.superseded
        current_attempt_id = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(current_attempt_id, str) and current_attempt_id
        assert current_attempt_id != stale_attempt_id

        accepted = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="current-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": current_attempt_id},
        )
        assert accepted.applied == 1 and accepted.error is None

        stale = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="stale-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": stale_attempt_id},
        )
        assert stale.applied == 0 and stale.error is not None

        copied = await _send_control_event(
            replacement,
            session_id=copied_root_id,
            batch_id="copied-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": current_attempt_id},
        )
        assert copied.applied == 0 and copied.error is not None

        owner_event = {
            "connection_admission_id": current_attempt_id,
            "runtime_incarnation": 3,
            "journal_id": "c" * 32,
        }
        owner_admit = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="current-owner-admit",
            event_type="external_acp_report_owner_admit",
            data=owner_event,
        )
        assert owner_admit.applied == 1 and owner_admit.error is None
        epoch_event = {
            **owner_event,
            "native_epoch": "native-epoch-stale-context-test",
            "epoch_ordinal": 1,
        }
        epoch_activate = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="current-epoch-activate",
            event_type="external_acp_report_epoch_activate",
            data=epoch_event,
        )
        assert epoch_activate.applied == 1 and epoch_activate.error is None

        wrong_runner_id = "runner-event-ingress-other"
        wrong_runner = await _connect(app, wrong_runner_id)
        wrong_runner_attempt = await _send_control_event(
            wrong_runner,
            session_id=root_id,
            batch_id="wrong-runner-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": current_attempt_id},
        )
        assert wrong_runner_attempt.applied == 0 and wrong_runner_attempt.error is not None

        wrong_owner = await _send_control_event(
            wrong_runner,
            session_id=root_id,
            batch_id="wrong-runner-owner-admit",
            event_type="external_acp_report_owner_admit",
            data=owner_event,
        )
        assert wrong_owner.applied == 0 and wrong_owner.error is not None
        wrong_epoch = await _send_control_event(
            wrong_runner,
            session_id=root_id,
            batch_id="wrong-runner-epoch-activate",
            event_type="external_acp_report_epoch_activate",
            data=epoch_event,
        )
        assert wrong_epoch.applied == 0 and wrong_epoch.error is not None

        latest = await _connect(app)
        replacement_open = False
        latest_attempt_id = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(latest_attempt_id, str) and latest_attempt_id
        assert latest_attempt_id != current_attempt_id

        copied_owner = await _send_control_event(
            latest,
            session_id=root_id,
            batch_id="new-connection-copied-owner-admit",
            event_type="external_acp_report_owner_admit",
            data=owner_event,
        )
        assert copied_owner.applied == 0 and copied_owner.error is not None
        copied_epoch = await _send_control_event(
            latest,
            session_id=root_id,
            batch_id="new-connection-copied-epoch-activate",
            event_type="external_acp_report_epoch_activate",
            data=epoch_event,
        )
        assert copied_epoch.applied == 0 and copied_epoch.error is not None

        latest_admission = await _send_control_event(
            latest,
            session_id=root_id,
            batch_id="latest-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": latest_attempt_id},
        )
        assert latest_admission.applied == 1 and latest_admission.error is None
    finally:
        if wrong_runner is not None:
            await _close(wrong_runner)
        if latest is not None:
            await _close(latest)
        if replacement is not None:
            if replacement_open:
                await _close(replacement)
            else:
                with contextlib.suppress(asyncio.TimeoutError):
                    await replacement.wait(timeout=budget(2.0))
        if first_open:
            await _close(first)
        else:
            with contextlib.suppress(asyncio.TimeoutError):
                await first.wait(timeout=budget(2.0))


async def test_acp_report_attempt_can_follow_same_principal_runner_rebind(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201
    root_id = created.json()["id"]
    store = SqlAlchemyConversationStore(db_uri)
    old_runner_id = _RUNNER_ID
    new_runner_id = "runner-event-ingress-rebound"
    assert store.set_runner_id(root_id, old_runner_id)
    old_root = store.get_conversation(root_id)
    assert old_root is not None

    old_runner = await _connect(app, old_runner_id)
    new_runner: ApplicationCommunicator | None = None
    try:
        initializer = app.state.runner_session_initializer
        old_attempt_id = await initializer.prepare_report_connection_attempt(old_root)
        assert isinstance(old_attempt_id, str) and old_attempt_id

        new_runner = await _connect(app, new_runner_id)
        # A request field cannot authorize the new runner to consume the old
        # runner's prepared attempt.
        forged = await _send_control_event(
            new_runner,
            session_id=root_id,
            batch_id="forged-runner-rebind",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": old_attempt_id, "runner_id": new_runner_id},
        )
        assert forged.applied == 0 and forged.error is not None

        rebound = store.replace_runner_id(root_id, new_runner_id, expected_runner_id=old_runner_id)
        assert rebound.runner_id == new_runner_id
        rebound_root = store.get_conversation(root_id)
        assert rebound_root is not None
        new_attempt_id = await initializer.prepare_report_connection_attempt(rebound_root)
        assert isinstance(new_attempt_id, str) and new_attempt_id
        assert new_attempt_id != old_attempt_id

        rebound = await _send_control_event(
            new_runner,
            session_id=root_id,
            batch_id="rebound-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": new_attempt_id},
        )
        assert rebound.applied == 1 and rebound.error is None

        stale_old_runner = await _send_control_event(
            old_runner,
            session_id=root_id,
            batch_id="stale-old-runner-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": old_attempt_id},
        )
        assert stale_old_runner.applied == 0 and stale_old_runner.error is not None
    finally:
        if new_runner is not None:
            await _close(new_runner)
        await _close(old_runner)


async def test_acp_epoch_change_invalidates_subscribed_rosters_in_same_tree_only(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A selected report epoch change refreshes all live rosters in its tree."""
    from omnigent.runtime import session_stream

    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    other_created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201
    assert other_created.status_code == 201
    root_id = created.json()["id"]
    other_root_id = other_created.json()["id"]
    store = SqlAlchemyConversationStore(db_uri)
    assert store.set_runner_id(root_id, _RUNNER_ID)
    child = store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=root_id,
        agent_id=agent["id"],
        runner_id=_RUNNER_ID,
        title="worker:roster-invalidation",
    )
    root = store.get_conversation(root_id)
    assert root is not None

    comm = await _connect(app)
    parent_stream = None
    child_stream = None
    other_stream = None
    try:
        initializer = app.state.runner_session_initializer
        attempt_id = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(attempt_id, str) and attempt_id
        connection = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="roster-connection-admit",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": attempt_id},
        )
        assert connection.applied == 1 and connection.error is None
        owner_event = {
            "connection_admission_id": attempt_id,
            "runtime_incarnation": 1,
            "journal_id": "d" * 32,
        }
        owner = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="roster-owner-admit",
            event_type="external_acp_report_owner_admit",
            data=owner_event,
        )
        assert owner.applied == 1 and owner.error is None

        from omnigent.server import session_live_state

        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )
        parent_stream = await _subscribe_session_stream(root_id)
        child_stream = await _subscribe_session_stream(child.id)
        other_stream = await _subscribe_session_stream(other_root_id)
        epoch_event = {
            **owner_event,
            "native_epoch": "native-epoch-roster-change",
            "epoch_ordinal": 1,
        }
        activated = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="roster-epoch-activate",
            event_type="external_acp_report_epoch_activate",
            data=epoch_event,
        )
        assert activated.applied == 1 and activated.error is None

        parent_invalidation = await parent_stream.next_event(timeout=5.0)
        child_invalidation = await child_stream.next_event(timeout=5.0)
        assert parent_invalidation == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": root_id,
        }
        assert child_invalidation == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": child.id,
        }
        await other_stream.assert_no_event(within=0.1)

        retry = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="roster-epoch-activate-retry",
            event_type="external_acp_report_epoch_activate",
            data=epoch_event,
        )
        assert retry.applied == 1 and retry.error is None
        for stream in (parent_stream, child_stream, other_stream):
            await stream.assert_no_event(within=0.1)
    finally:
        if parent_stream is not None:
            await parent_stream.stop()
        if child_stream is not None:
            await child_stream.stop()
        if other_stream is not None:
            await other_stream.stop()
        session_stream.close(root_id)
        session_stream.close(child.id)
        session_stream.close(other_root_id)
        await _close(comm)


async def test_acp_roster_invalidation_finds_grandchild_only_subscriber(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
    tmp_path: Path,
) -> None:
    """A deep descendant stream is invalidated even without ancestor listeners."""
    from omnigent.runtime import session_stream
    from omnigent.stores.artifact_store.local import LocalArtifactStore

    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201
    root_id = created.json()["id"]
    store = SqlAlchemyConversationStore(
        db_uri,
        artifact_store=LocalArtifactStore(str(tmp_path / "artifacts")),
    )
    assert store.set_runner_id(root_id, _RUNNER_ID)
    child = store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=root_id,
        agent_id=agent["id"],
        runner_id=_RUNNER_ID,
        title="worker:deep-roster",
    )
    grandchild = store.create_conversation(
        kind="sub_agent",
        parent_conversation_id=child.id,
        agent_id=agent["id"],
        runner_id=_RUNNER_ID,
        title="worker:grandchild-roster",
    )
    root = store.get_conversation(root_id)
    assert root is not None
    comm = await _connect(app)
    grandchild_stream = None
    try:
        grandchild_stream = await _subscribe_session_stream(grandchild.id)
        subscribed_ids = set(session_stream.subscribed_conversation_ids())
        assert grandchild.id in subscribed_ids
        assert root_id not in subscribed_ids
        assert child.id not in subscribed_ids

        initializer = app.state.runner_session_initializer
        attempt_id = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(attempt_id, str) and attempt_id
        connection = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="grandchild-roster-connection",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": attempt_id},
        )
        assert connection.applied == 1 and connection.error is None
        owner_event = {
            "connection_admission_id": attempt_id,
            "runtime_incarnation": 1,
            "journal_id": "e" * 32,
        }
        owner = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="grandchild-roster-owner",
            event_type="external_acp_report_owner_admit",
            data=owner_event,
        )
        assert owner.applied == 1 and owner.error is None
        epoch = await _send_control_event(
            comm,
            session_id=root_id,
            batch_id="grandchild-roster-epoch",
            event_type="external_acp_report_epoch_activate",
            data={
                **owner_event,
                "native_epoch": "native-epoch-grandchild-roster",
                "epoch_ordinal": 1,
            },
        )
        assert epoch.applied == 1 and epoch.error is None
        from omnigent.server import session_live_state

        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )

        assert await grandchild_stream.next_event(timeout=5.0) == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": grandchild.id,
        }
    finally:
        if grandchild_stream is not None:
            await grandchild_stream.stop()
        session_stream.close(grandchild.id)
        await _close(comm)


async def test_acp_control_rejections_keep_prior_selector_fanout(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
    tmp_path: Path,
) -> None:
    """Committed selector changes fan out even when the next control is rejected."""
    from types import SimpleNamespace

    from omnigent.runtime import session_stream
    from omnigent.server import session_live_state
    from omnigent.stores.artifact_store.local import LocalArtifactStore

    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    other_created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == other_created.status_code == 201
    root_id = created.json()["id"]
    other_root_id = other_created.json()["id"]
    store = SqlAlchemyConversationStore(
        db_uri,
        artifact_store=LocalArtifactStore(str(tmp_path / "artifacts")),
    )
    assert store.set_runner_id(root_id, _RUNNER_ID)
    assert store.set_runner_id(other_root_id, _RUNNER_ID)
    root = store.get_conversation(root_id)
    assert root is not None
    stream = None
    first_comm = await _connect(app)
    replacement = None
    try:
        stream = await _subscribe_session_stream(root_id)
        initializer = app.state.runner_session_initializer
        first_attempt = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(first_attempt, str) and first_attempt
        connected = await _send_control_event(
            first_comm,
            session_id=root_id,
            batch_id="substep-initial-connection",
            event_type="external_acp_report_connection_admit",
            data={"attempt_id": first_attempt},
        )
        assert connected.applied == 1 and connected.error is None
        initial_owner = {
            "connection_admission_id": first_attempt,
            "runtime_incarnation": 1,
            "journal_id": "f" * 32,
        }
        owner = await _send_control_event(
            first_comm,
            session_id=root_id,
            batch_id="substep-initial-owner",
            event_type="external_acp_report_owner_admit",
            data=initial_owner,
        )
        assert owner.applied == 1 and owner.error is None
        first_epoch = {
            **initial_owner,
            "native_epoch": "native-epoch-substep-initial",
            "epoch_ordinal": 1,
        }
        activated = await _send_control_event(
            first_comm,
            session_id=root_id,
            batch_id="substep-initial-epoch",
            event_type="external_acp_report_epoch_activate",
            data=first_epoch,
        )
        assert activated.applied == 1 and activated.error is None
        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )
        assert await stream.next_event(timeout=5.0) == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": root_id,
        }

        # The new tunnel's connection selector commits first inside the owner
        # event. Its journal then fails lineage validation, but that committed
        # selector change still refreshes subscribers.
        replacement = await _connect(app)
        current_connection = app.state.tunnel_registry.get(_RUNNER_ID)
        assert current_connection is not None
        other_attempt = store.prepare_acp_report_connection_attempt(
            other_root_id,
            _RUNNER_ID,
            current_connection.owner,
            current_connection.server_connection_id,
        )
        store.admit_acp_report_connection(
            other_attempt,
            SimpleNamespace(
                runner_id=_RUNNER_ID,
                owner=current_connection.owner,
                server_connection_id=current_connection.server_connection_id,
                superseded=False,
            ),
            expected_root_id=other_root_id,
        )
        other_journal_id = "d" * 32
        store.admit_acp_report_owner(other_root_id, other_attempt, 1, other_journal_id)
        replacement_attempt = await initializer.prepare_report_connection_attempt(root)
        assert isinstance(replacement_attempt, str) and replacement_attempt
        rejected_owner = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-connection-then-owner-rejects",
            event_type="external_acp_report_owner_admit",
            data={
                "connection_admission_id": replacement_attempt,
                "runtime_incarnation": 1,
                "journal_id": other_journal_id,
            },
        )
        assert rejected_owner.applied == 0 and rejected_owner.error is not None
        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )
        assert await stream.next_event(timeout=5.0) == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": root_id,
        }

        replacement_owner = {
            "connection_admission_id": replacement_attempt,
            "runtime_incarnation": 1,
            "journal_id": "c" * 32,
        }
        recovered_owner = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-recovery-owner",
            event_type="external_acp_report_owner_admit",
            data=replacement_owner,
        )
        assert recovered_owner.applied == 1 and recovered_owner.error is None
        recovered_epoch = {
            **replacement_owner,
            "native_epoch": "native-epoch-substep-recovered",
            "epoch_ordinal": 1,
        }
        recovered = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-recovery-epoch",
            event_type="external_acp_report_epoch_activate",
            data=recovered_epoch,
        )
        assert recovered.applied == 1 and recovered.error is None
        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )
        assert await stream.next_event(timeout=5.0) == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": root_id,
        }

        # Changing the owner selector is committed and fans out. A later epoch
        # activation for the prior owner fails without undoing that change.
        changed_owner = {
            "connection_admission_id": replacement_attempt,
            "runtime_incarnation": 2,
            "journal_id": "b" * 32,
        }
        owner_changed = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-owner-selector-change",
            event_type="external_acp_report_owner_admit",
            data=changed_owner,
        )
        assert owner_changed.applied == 1 and owner_changed.error is None
        stale_epoch = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-prior-owner-epoch-rejected",
            event_type="external_acp_report_epoch_activate",
            data=recovered_epoch,
        )
        assert stale_epoch.applied == 0 and stale_epoch.error is not None
        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )
        assert await stream.next_event(timeout=5.0) == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": root_id,
        }

        historical = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-historical-epoch-bind",
            event_type="external_acp_report_epoch_activate",
            data={
                **changed_owner,
                "native_epoch": "native-epoch-substep-historical",
                "epoch_ordinal": 2,
                "mode": "historical",
            },
        )
        assert historical.applied == 1 and historical.error is None
        await stream.assert_no_event(within=0.1)

        current_epoch = {
            **changed_owner,
            "native_epoch": "native-epoch-substep-current",
            "epoch_ordinal": 1,
        }
        current = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-current-epoch",
            event_type="external_acp_report_epoch_activate",
            data=current_epoch,
        )
        assert current.applied == 1 and current.error is None
        session_live_state.submit("drain_acp_roster_control_events", lambda: None).result(
            timeout=10
        )
        assert await stream.next_event(timeout=5.0) == {
            "type": "session.child_sessions.invalidated",
            "conversation_id": root_id,
        }
        retry = await _send_control_event(
            replacement,
            session_id=root_id,
            batch_id="substep-current-epoch-retry",
            event_type="external_acp_report_epoch_activate",
            data=current_epoch,
        )
        assert retry.applied == 1 and retry.error is None
        await stream.assert_no_event(within=0.1)
    finally:
        if stream is not None:
            await stream.stop()
        session_stream.close(root_id)
        if replacement is not None:
            await _close(replacement)
        await _close(first_comm)


async def test_lost_ack_replays_without_duplicate_item(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201
    session_id = created.json()["id"]
    assert SqlAlchemyConversationStore(db_uri).set_runner_id(session_id, _RUNNER_ID)

    dispatcher = RunnerEventDispatcher()
    first = await _connect(app)
    first_open = True
    second: ApplicationCommunicator | None = None
    posting: asyncio.Task[httpx.Response] | None = None
    try:

        async def first_send(text: str) -> None:
            await first.send_input({"type": "websocket.receive", "text": text})

        dispatcher.ready(first_send)
        async with TunnelEventClient(
            base_url="http://server",
            event_dispatcher=dispatcher,
            transport=httpx.MockTransport(lambda _: httpx.Response(599)),
        ) as forwarder:
            posting = asyncio.create_task(
                forwarder.post(f"/v1/sessions/{session_id}/events", json=_ITEM)
            )
            ack = await _read_ack(first)
            assert ack.applied == 1
            # Server committed the item; the connection dropped before the
            # runner received the acknowledgement.
            dispatcher.disconnected()
            await _close(first)
            first_open = False

            second = await _connect(app)

            async def second_send(text: str) -> None:
                assert second is not None
                await second.send_input({"type": "websocket.receive", "text": text})

            dispatcher.ready(second_send)
            replay_ack = await _read_ack(second)
            assert replay_ack.applied == 1
            dispatcher.acknowledge(replay_ack)
            response = await asyncio.wait_for(posting, timeout=budget(3.0))
            assert response.status_code == 202
    finally:
        dispatcher.disconnected()
        if posting is not None:
            if not posting.done():
                posting.cancel()
            await asyncio.gather(posting, return_exceptions=True)
        try:
            if second is not None:
                await _close(second)
        finally:
            if first_open:
                await _close(first)

    items = (await client.get(f"/v1/sessions/{session_id}/items")).json()["data"]
    assert [item["content"][0]["text"] for item in items] == ["recovered after restart"]


async def test_events_from_a_runner_the_session_moved_off_are_dropped_and_acknowledged(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """A superseded runner's late item is acknowledged, so its forwarder posts no failure."""
    agent = await create_test_agent(client)
    created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
    assert created.status_code == 201
    session_id = created.json()["id"]
    # The session now belongs to a replacement runner; the tunnel below is the old one.
    assert SqlAlchemyConversationStore(db_uri).set_runner_id(session_id, "runner-replacement")

    dispatcher = RunnerEventDispatcher()
    comm = await _connect(app)
    posting: asyncio.Task[httpx.Response] | None = None
    try:

        async def send(text: str) -> None:
            await comm.send_input({"type": "websocket.receive", "text": text})

        dispatcher.ready(send)
        async with TunnelEventClient(
            base_url="http://server",
            event_dispatcher=dispatcher,
            transport=httpx.MockTransport(lambda _: httpx.Response(599)),
        ) as forwarder:
            posting = asyncio.create_task(
                forwarder.post(f"/v1/sessions/{session_id}/events", json=_ITEM)
            )
            ack = await _read_ack(comm)
            assert ack == EventAckFrame(ack.id, 1)
            dispatcher.acknowledge(ack)
            response = await asyncio.wait_for(posting, timeout=budget(3.0))
            assert response.status_code == 202
    finally:
        dispatcher.disconnected()
        if posting is not None:
            if not posting.done():
                posting.cancel()
            await asyncio.gather(posting, return_exceptions=True)
        await _close(comm)

    items = (await client.get(f"/v1/sessions/{session_id}/items")).json()["data"]
    assert items == []


@pytest.mark.timeout(180)
async def test_numbered_items_survive_tunnel_disconnect_stages(
    app: FastAPI,
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """Queue before ready, disconnect mid-run, and lose an applied ACK under load."""
    agent = await create_test_agent(client)
    session_ids: list[str] = []
    store = SqlAlchemyConversationStore(db_uri)
    for _ in range(4):
        created = await client.post("/v1/sessions", json={"agent_id": agent["id"]})
        assert created.status_code == 201
        session_id = created.json()["id"]
        assert store.set_runner_id(session_id, _RUNNER_ID)
        session_ids.append(session_id)

    dispatcher = RunnerEventDispatcher()
    received_acks = 0

    async def produce(part: int) -> None:
        session_id = session_ids[part]
        for number in range(part * 250 + 1, (part + 1) * 250 + 1):
            event = {
                "type": "external_conversation_item",
                "data": {
                    "source_id": f"number-{number}",
                    "item_type": "message",
                    "response_id": f"response-{number}",
                    "item_data": {
                        "role": "assistant",
                        "agent": "claude-native-ui",
                        "content": [{"type": "output_text", "text": str(number)}],
                    },
                },
            }
            ack = await dispatcher.submit(session_id, [event])
            assert ack.applied == 1, f"number {number} was not accepted: {ack!r}"
            await asyncio.sleep(0.005)

    producers = [asyncio.create_task(produce(part)) for part in range(4)]
    await asyncio.sleep(0)
    assert dispatcher.has_pending  # Started before the first tunnel was ready.
    current: ApplicationCommunicator | None = None
    pump_task: asyncio.Task[None] | None = None

    async def connect_and_pump(stop_after: int | None = None, lose_ack: bool = False) -> None:
        nonlocal current, pump_task, received_acks
        current = await _connect(app)
        comm = current

        async def send(text: str) -> None:
            await comm.send_input({"type": "websocket.receive", "text": text})

        dispatcher.ready(send)

        async def pump() -> None:
            nonlocal received_acks
            while True:
                message = await comm.receive_output(timeout=budget(30.0))
                if message.get("type") != "websocket.send":
                    continue
                frame = decode_frame(message["text"])
                if not isinstance(frame, EventAckFrame):
                    continue  # Runner recovery RPCs also share the tunnel.
                if frame.applied != 1:
                    dispatcher.acknowledge(frame)
                    continue  # Busy responses do not advance the source.
                received_acks += 1
                if received_acks == stop_after:
                    if not lose_ack:
                        dispatcher.acknowledge(frame)
                    dispatcher.disconnected()
                    return
                dispatcher.acknowledge(frame)

        pump_task = asyncio.create_task(pump())
        if stop_after is not None:
            await asyncio.wait_for(pump_task, timeout=budget(120.0))
            await _close(comm)
            current = None
            pump_task = None

    try:
        await connect_and_pump(stop_after=400)
        await connect_and_pump(stop_after=700, lose_ack=True)
        await connect_and_pump()
        await asyncio.wait_for(asyncio.gather(*producers), timeout=budget(120.0))
    finally:
        dispatcher.disconnected()
        for producer in producers:
            if not producer.done():
                producer.cancel()
        await asyncio.gather(*producers, return_exceptions=True)
        if pump_task is not None:
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
        if current is not None:
            await _close(current)

    numbers: list[int] = []
    for session_id in session_ids:
        items = await client.get(f"/v1/sessions/{session_id}/items", params={"limit": 1000})
        items.raise_for_status()
        numbers.extend(int(item["content"][0]["text"]) for item in items.json()["data"])
    assert len(numbers) == 1000
    assert sorted(numbers) == list(range(1, 1001))
