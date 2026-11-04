"""
Attempt-mode git worktree creation through ``POST /v1/sessions`` and
``POST /v1/hosts/{id}/runners``, with a fake tunnel whose create/list
answers come from the real host handlers running real git in a temp repo.
"""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

import omnigent.host.git_worktree as gw
from omnigent.host.connect import HostProcess
from omnigent.host.frames import (
    HOST_CAPABILITIES,
    HostCreateWorktreeFrame,
    HostHelloFrame,
    HostLaunchRunnerFrame,
    HostListWorktreesFrame,
    HostRemoveWorktreeFrame,
    HostStatFrame,
    decode_host_frame,
    encode_host_frame,
)
from omnigent.host.identity import HostIdentity
from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.server.auth import RESERVED_USER_LOCAL
from omnigent.server.host_registry import HostConnection
from omnigent.server.routes.host_tunnel import create_worktree_result_payload
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.comment_store.sqlalchemy_store import SqlAlchemyCommentStore
from omnigent.stores.conversation_store.sqlalchemy_store import SqlAlchemyConversationStore
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
from omnigent.stores.host_store import HostStore
from tests.server.helpers import create_test_agent

pytestmark = pytest.mark.asyncio

_HOST_ID = "3c8753b34a61b09af35a01136d40fade"
ATTEMPT = "attempt-aaaaaaaaaaaaaaaa"


@pytest.fixture()
def app(runtime_init: None, db_uri: str, tmp_path: Path) -> FastAPI:
    """App with ``host_store`` so host routes (listing, runner launch) are mounted."""
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    return create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(artifact_store=artifact_store, cache_dir=tmp_path / "cache"),
        comment_store=SqlAlchemyCommentStore(db_uri),
        host_store=HostStore(db_uri),
    )


@pytest.fixture(autouse=True)
def _isolated_attempt_index(tmp_path_factory: pytest.TempPathFactory, monkeypatch) -> None:
    """Fresh host-wide attempt→repository index per test (attempt ids repeat across tests)."""
    monkeypatch.setenv("OMNIGENT_DATA_DIR", str(tmp_path_factory.mktemp("omnigent-data")))


class _FakeWebSocket:
    async def send_text(self, data: str) -> None:
        """Frames flow through the outbound queue instead."""


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    root = (tmp_path / "repo").resolve()
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    (root / "a.txt").write_text("a\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "init")
    return root


Register = Callable[..., list[Any]]


@pytest_asyncio.fixture()
async def register_host(app: FastAPI, db_uri: str) -> AsyncIterator[Register]:
    conns: list[HostConnection] = []
    host = HostProcess(identity=HostIdentity(host_id=_HOST_ID, name="t"), server_url="http://x")

    def _register(*, capabilities: list[str] | None = None, drop_first_reply: bool = False):
        HostStore(db_uri).upsert_on_connect(_HOST_ID, "wt-host", RESERVED_USER_LOCAL)
        conn = app.state.host_registry.register(
            host_id=_HOST_ID,
            ws=_FakeWebSocket(),  # type: ignore[arg-type]
            hello=HostHelloFrame(
                version="0.1.0-test",
                frame_protocol_version=1,
                name="wt-host",
                capabilities=list(HOST_CAPABILITIES if capabilities is None else capabilities),
            ),
            owner=RESERVED_USER_LOCAL,
        )
        frames: list[Any] = []
        dropped = {"done": not drop_first_reply}

        async def _drain() -> None:
            while True:
                text = await conn.outbound_queue.get()
                if text is None:
                    return
                frame = decode_host_frame(text)
                frames.append(frame)
                if isinstance(frame, HostStatFrame):
                    fut = conn.pending_stats.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(
                            {
                                "status": "ok",
                                "exists": True,
                                "type": "directory",
                                "canonical_path": frame.path,
                                "error": None,
                            }
                        )
                elif isinstance(frame, HostCreateWorktreeFrame):
                    result = await host._handle_create_worktree(frame)
                    fut = conn.pending_create_worktrees.pop(frame.request_id, None)
                    if not dropped["done"]:
                        dropped["done"] = True
                        if fut is not None and not fut.done():
                            fut.set_exception(asyncio.TimeoutError())
                        continue
                    if fut is not None and not fut.done():
                        # Same wire + tunnel path as a live host.
                        wire = decode_host_frame(encode_host_frame(result))
                        fut.set_result(create_worktree_result_payload(wire))
                elif isinstance(frame, HostListWorktreesFrame):
                    result = await host._handle_list_worktrees(frame)
                    fut = conn.pending_list_worktrees.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(
                            {
                                "status": result.status,
                                "worktrees": result.worktrees,
                                "error": result.error,
                            }
                        )
                elif isinstance(frame, HostLaunchRunnerFrame):
                    fut = conn.pending_launches.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result({"status": "launched", "runner_id": "r1", "error": None})
                elif isinstance(frame, HostRemoveWorktreeFrame):
                    fut = conn.pending_remove_worktrees.pop(frame.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result({"status": "ok", "error": None})

        conn._drain_task_for_test = asyncio.create_task(_drain())  # type: ignore[attr-defined]
        conns.append(conn)
        return frames

    yield _register
    for conn in conns:
        conn.outbound_queue.put_nowait(None)
        task = conn._drain_task_for_test  # type: ignore[attr-defined]
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        if not task.done():
            task.cancel()


async def _post(client: httpx.AsyncClient, agent_id: str, repo: Path, **git: Any):
    return await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent_id,
            "host_id": _HOST_ID,
            "workspace": str(repo),
            "git": {"branch_name": "vitrine/run-a", "attempt_id": ATTEMPT, **git},
        },
    )


def _creates(frames: list[Any]) -> list[HostCreateWorktreeFrame]:
    return [f for f in frames if isinstance(f, HostCreateWorktreeFrame)]


async def test_create_returns_receipt_and_labels_session(register_host, client, repo) -> None:
    frames = register_host()
    agent = await create_test_agent(client, name="wt-attempt-agent")
    resp = await _post(client, agent["id"], repo)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    receipt = body["git_worktree"]
    assert receipt["outcome"] == "created" and receipt["state"] == "ready"
    assert receipt["session_reused"] is False
    assert body["workspace"] == receipt["workspace"]
    assert body["labels"]["omnigent.git.worktree_attempt"] == ATTEMPT
    assert _creates(frames)[0].attempt_id == ATTEMPT
    listing = await client.get(f"/v1/hosts/{_HOST_ID}/worktrees", params={"path": str(repo)})
    assert listing.status_code == 200, listing.text
    row = next(r for r in listing.json()["data"] if r["branch"] == "vitrine/run-a")
    assert row["creation"]["state"] == "ready"


async def test_dropped_reply_retry_returns_same_session(register_host, client, repo) -> None:
    """The first POST's host reply is lost; the retry gets the finished worktree."""
    register_host(drop_first_reply=True)
    agent = await create_test_agent(client, name="wt-attempt-drop")
    first = await _post(client, agent["id"], repo)
    assert first.status_code == 409
    assert first.json()["error"]["code"] == "worktree_creation_incomplete"
    second = await _post(client, agent["id"], repo)
    assert second.status_code == 201, second.text
    assert second.json()["git_worktree"]["outcome"] == "existing"
    third = await _post(client, agent["id"], repo)
    assert third.json()["id"] == second.json()["id"]
    assert third.json()["git_worktree"]["session_reused"] is True


async def test_concurrent_same_attempt_posts_create_one_session(
    register_host, client, repo
) -> None:
    register_host()
    agent = await create_test_agent(client, name="wt-attempt-race")
    a, b = await asyncio.gather(_post(client, agent["id"], repo), _post(client, agent["id"], repo))
    assert a.status_code == b.status_code == 201, (a.text, b.text)
    assert a.json()["id"] == b.json()["id"]
    assert sorted(r.json()["git_worktree"]["session_reused"] for r in (a, b)) == [False, True]


async def test_preexisting_branch_is_a_conflict(register_host, client, repo) -> None:
    register_host()
    _git(repo, "branch", "vitrine/run-a")
    agent = await create_test_agent(client, name="wt-attempt-conflict")
    resp = await _post(client, agent["id"], repo)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "worktree_attempt_conflict"


async def test_mismatch_and_removed(register_host, client, repo) -> None:
    register_host()
    agent = await create_test_agent(client, name="wt-attempt-removed")
    created = await _post(client, agent["id"], repo)
    assert created.status_code == 201
    mismatch = await _post(client, agent["id"], repo, base_branch="main")
    assert mismatch.json()["error"]["code"] == "worktree_attempt_mismatch"
    gw.remove_worktree(worktree_path=created.json()["git_worktree"]["worktree_path"])
    gone = await _post(client, agent["id"], repo)
    assert gone.status_code == 410
    assert gone.json()["error"]["code"] == "worktree_attempt_removed"


async def test_old_host_gets_412(register_host, client, repo) -> None:
    frames = register_host(capabilities=[])
    agent = await create_test_agent(client, name="wt-attempt-old")
    resp = await _post(client, agent["id"], repo)
    assert resp.status_code == 412
    assert resp.json()["error"]["code"] == "worktree_attempts_unsupported"
    assert _creates(frames) == []


async def test_attempt_mode_skips_rollback(register_host, client, repo, monkeypatch) -> None:
    frames = register_host()
    agent = await create_test_agent(client, name="wt-attempt-rollback")
    from omnigent.stores.conversation_store.sqlalchemy_store import (
        SqlAlchemyConversationStore as store,
    )

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("db down")

    monkeypatch.setattr(store, "create_conversation", boom)
    with contextlib.suppress(Exception):
        await _post(client, agent["id"], repo)
    assert not any(isinstance(f, HostRemoveWorktreeFrame) for f in frames)
    assert any(p.name == "vitrine-run-a" for p in (repo.parent / "repo-worktrees").iterdir())


async def test_bind_rejects_partial_and_allows_plain_worktree(
    register_host, client, repo, monkeypatch
) -> None:
    register_host()
    agent = await create_test_agent(client, name="wt-attempt-bind")
    real = gw._run_git

    def timeout_on_add(args: list[str], *, cwd: str, **kw):
        if args[:2] == ["worktree", "add"]:
            # Leave a registered, unpopulated checkout like a killed host would.
            real([*args[:2], "--no-checkout", *args[2:]], cwd=cwd, **kw)
            raise gw.GitTimeoutError("git command timed out after 120s")
        return real(args, cwd=cwd, **kw)

    monkeypatch.setattr(gw, "_run_git", timeout_on_add)
    partial = await _post(client, agent["id"], repo)
    assert partial.json()["error"]["code"] == "worktree_creation_incomplete"
    monkeypatch.setattr(gw, "_run_git", real)
    partial_path = repo.parent / "repo-worktrees" / "vitrine-run-a"
    bind = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent["id"],
            "host_id": _HOST_ID,
            "workspace": str(partial_path),
            "git": {"branch_name": "vitrine/run-a", "existing_worktree": True},
        },
    )
    assert bind.status_code == 409
    assert bind.json()["error"]["code"] == "worktree_not_ready"
    plain = repo.parent / "plain"
    _git(repo, "worktree", "add", "-q", "-b", "plain", str(plain))
    ok = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent["id"],
            "host_id": _HOST_ID,
            "workspace": str(plain),
            "git": {"branch_name": "plain", "existing_worktree": True},
        },
    )
    assert ok.status_code == 201, ok.text


async def test_attempt_id_with_existing_worktree_is_rejected(register_host, client, repo) -> None:
    register_host()
    agent = await create_test_agent(client, name="wt-attempt-422")
    resp = await client.post(
        "/v1/sessions",
        json={
            "agent_id": agent["id"],
            "host_id": _HOST_ID,
            "workspace": str(repo),
            "git": {"branch_name": "x", "existing_worktree": True, "attempt_id": ATTEMPT},
        },
    )
    assert resp.status_code in (400, 422)


async def test_info_advertises_attempts(client) -> None:
    resp = await client.get("/v1/info")
    assert resp.json()["worktree_creation_attempts"] is True


async def test_launch_runner_attempt_receipt_and_removed(register_host, client, repo) -> None:
    """The runner-bind create path returns the receipt and the same 410 once removed."""
    register_host()
    agent = await create_test_agent(client, name="wt-attempt-launch")

    async def launch(session_id: str):
        return await client.post(
            f"/v1/hosts/{_HOST_ID}/runners",
            json={
                "session_id": session_id,
                "workspace": str(repo),
                "git": {"branch_name": "vitrine/run-l", "attempt_id": ATTEMPT},
            },
        )

    first_session = (await client.post("/v1/sessions", json={"agent_id": agent["id"]})).json()
    first = await launch(first_session["id"])
    assert first.status_code == 200, first.text
    receipt = first.json()["git_worktree"]
    assert receipt["outcome"] == "created"
    gw.remove_worktree(worktree_path=receipt["worktree_path"])
    second_session = (await client.post("/v1/sessions", json={"agent_id": agent["id"]})).json()
    gone = await launch(second_session["id"])
    assert gone.status_code == 410
    assert gone.json()["error"]["code"] == "worktree_attempt_removed"


def _launches(frames: list[Any]) -> list[HostLaunchRunnerFrame]:
    return [f for f in frames if isinstance(f, HostLaunchRunnerFrame)]


async def _bare_session(client: httpx.AsyncClient, agent_id: str) -> str:
    return (await client.post("/v1/sessions", json={"agent_id": agent_id})).json()["id"]


async def _launch(client: httpx.AsyncClient, session_id: str, repo: Path, attempt: str = ATTEMPT):
    return await client.post(
        f"/v1/hosts/{_HOST_ID}/runners",
        json={
            "session_id": session_id,
            "workspace": str(repo),
            "git": {"branch_name": "vitrine/run-a", "attempt_id": attempt},
        },
    )


async def test_runner_route_retry_is_idempotent(register_host, client, repo) -> None:
    """A same-session, same-attempt runner retry returns the first binding; one launch."""
    frames = register_host()
    agent = await create_test_agent(client, name="wt-attempt-runner-retry")
    session_id = await _bare_session(client, agent["id"])
    first = await _launch(client, session_id, repo)
    assert first.status_code == 200, first.text
    again = await _launch(client, session_id, repo)
    assert again.status_code == 200, again.text
    assert again.json()["runner_id"] == first.json()["runner_id"]
    assert again.json()["git_worktree"]["session_reused"] is True
    assert len(_launches(frames)) == 1
    labels = (await client.get(f"/v1/sessions/{session_id}")).json()["labels"]
    assert labels["omnigent.git.worktree_attempt"] == ATTEMPT


async def test_cross_route_retry_reuses_runner_session(register_host, client, repo) -> None:
    """Runner-route create, then POST /v1/sessions with the same attempt: one session/launch."""
    frames = register_host()
    agent = await create_test_agent(client, name="wt-attempt-cross")
    session_id = await _bare_session(client, agent["id"])
    assert (await _launch(client, session_id, repo)).status_code == 200
    resp = await _post(client, agent["id"], repo)
    assert resp.status_code == 201, resp.text
    assert resp.json()["id"] == session_id
    assert resp.json()["git_worktree"]["session_reused"] is True
    assert len(_launches(frames)) == 1
    other = await _bare_session(client, agent["id"])
    taken = await _launch(client, other, repo)
    assert taken.status_code == 409
    assert taken.json()["error"]["code"] == "worktree_attempt_conflict"


async def test_attempt_with_another_repository_is_a_mismatch(
    register_host, client, repo, tmp_path
) -> None:
    register_host()
    agent = await create_test_agent(client, name="wt-attempt-other-repo")
    assert (await _post(client, agent["id"], repo)).status_code == 201
    other = (tmp_path / "repo2").resolve()
    other.mkdir()
    _git(other, "init", "-q", "-b", "main")
    (other / "a.txt").write_text("a\n")
    _git(other, "add", "-A")
    _git(other, "commit", "-q", "-m", "init")
    resp = await _post(client, agent["id"], other)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "worktree_attempt_mismatch"
    assert not (other.parent / "repo2-worktrees").exists()


def test_label_lookup_is_not_truncated_by_archived_sessions(db_uri: str) -> None:
    store = SqlAlchemyConversationStore(db_uri)
    HostStore(db_uri).upsert_on_connect(_HOST_ID, "wt-host", RESERVED_USER_LOCAL)
    label = {"omnigent.git.worktree_attempt": ATTEMPT}
    for _ in range(40):
        conv = store.create_conversation(
            agent_id=None, host_id=_HOST_ID, workspace="/w/run-a", labels=label
        )
        store.update_conversation(conv.id, archived=True)
    live = store.create_conversation(
        agent_id=None, host_id=_HOST_ID, workspace="/w/run-a", labels=label
    )
    found = store.find_live_sessions_with_label(
        host_id=_HOST_ID, key="omnigent.git.worktree_attempt", value=ATTEMPT
    )
    assert found == [live.id]


def test_label_lookup_is_not_hidden_by_other_hosts(db_uri: str) -> None:
    """Same-label live sessions on another host never crowd out this host's match."""
    store = SqlAlchemyConversationStore(db_uri)
    other_host = "4d8753b34a61b09af35a01136d40fadf"
    HostStore(db_uri).upsert_on_connect(_HOST_ID, "wt-host", RESERVED_USER_LOCAL)
    HostStore(db_uri).upsert_on_connect(other_host, "other", RESERVED_USER_LOCAL)
    label = {"omnigent.git.worktree_attempt": ATTEMPT}
    for _ in range(65):
        store.create_conversation(
            agent_id=None, host_id=other_host, workspace="/w/run-a", labels=label
        )
    mine = store.create_conversation(
        agent_id=None, host_id=_HOST_ID, workspace="/w/run-a", labels=label
    )
    found = store.find_live_sessions_with_label(
        host_id=_HOST_ID, key="omnigent.git.worktree_attempt", value=ATTEMPT
    )
    assert found == [mine.id]


def test_label_page_query_is_an_index_range(db_uri: str, monkeypatch) -> None:
    """Each label page seeks the label index in order: no temp sort, no OFFSET."""
    from sqlalchemy import event

    import omnigent.stores.conversation_store.sqlalchemy_store as sql_store

    monkeypatch.setattr(sql_store, "_LABELED_SESSION_PAGE_SIZE", 16)

    if not db_uri.startswith("sqlite"):
        pytest.skip("EXPLAIN QUERY PLAN is SQLite syntax")

    store = SqlAlchemyConversationStore(db_uri)
    other_host = "5e8753b34a61b09af35a01136d40fadf"
    HostStore(db_uri).upsert_on_connect(_HOST_ID, "wt-host", RESERVED_USER_LOCAL)
    HostStore(db_uri).upsert_on_connect(other_host, "other", RESERVED_USER_LOCAL)
    for _ in range(20):  # off-host matches force a keyset continuation page
        store.create_conversation(
            agent_id=None,
            host_id=other_host,
            workspace="/w/run-a",
            labels={"omnigent.git.worktree_attempt": ATTEMPT},
        )
    engine = store._conv_engine
    seen: list[tuple[str, object]] = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if "FROM conversation_labels" in statement:
            seen.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", capture)
    try:
        store.find_live_sessions_with_label(
            host_id=_HOST_ID, key="omnigent.git.worktree_attempt", value=ATTEMPT
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert len(seen) == 2  # a full page, then the keyset continuation
    with engine.connect() as conn:
        for statement, parameters in seen:
            if "OFFSET" in statement.upper():  # SQLite always renders it; keyset keeps it 0
                assert list(parameters)[-1] == 0
            plan = " ".join(
                str(row[-1])
                for row in conn.exec_driver_sql(f"EXPLAIN QUERY PLAN {statement}", parameters)
            )
            assert "ix_conversation_labels_key_value" in plan, plan
            assert "TEMP B-TREE" not in plan.upper(), plan


def _label_off_host(db_uri: str, count: int) -> SqlAlchemyConversationStore:
    store = SqlAlchemyConversationStore(db_uri)
    other_host = "5e8753b34a61b09af35a01136d40fadf"
    HostStore(db_uri).upsert_on_connect(_HOST_ID, "wt-host", RESERVED_USER_LOCAL)
    HostStore(db_uri).upsert_on_connect(other_host, "other", RESERVED_USER_LOCAL)
    for _ in range(count):
        store.create_conversation(
            agent_id=None,
            host_id=other_host,
            workspace="/w/run-a",
            labels={"omnigent.git.worktree_attempt": ATTEMPT},
        )
    return store


def test_label_lookup_finds_target_past_1024_off_host_matches(db_uri: str) -> None:
    store = _label_off_host(db_uri, 1100)
    mine = store.create_conversation(
        agent_id=None,
        host_id=_HOST_ID,
        workspace="/w/run-a",
        labels={"omnigent.git.worktree_attempt": ATTEMPT},
    )
    found = store.find_live_sessions_with_label(
        host_id=_HOST_ID, key="omnigent.git.worktree_attempt", value=ATTEMPT
    )
    assert found == [mine.id]


def test_label_lookup_fails_loudly_past_its_hard_cap(db_uri: str, monkeypatch) -> None:
    import omnigent.stores.conversation_store.sqlalchemy_store as sql_store

    store = _label_off_host(db_uri, 40)
    monkeypatch.setattr(sql_store, "_LABELED_SESSION_PAGE_SIZE", 8)
    monkeypatch.setattr(sql_store, "_LABELED_SESSION_MAX_ROWS", 16)
    from omnigent.stores.conversation_store import LabelLookupExhaustedError

    with pytest.raises(LabelLookupExhaustedError, match="refusing to return a partial answer"):
        store.find_live_sessions_with_label(
            host_id=_HOST_ID, key="omnigent.git.worktree_attempt", value=ATTEMPT
        )


def _exhaust_label_lookup(monkeypatch) -> None:
    from omnigent.stores.conversation_store import LabelLookupExhaustedError

    def exhausted(self, **kwargs):
        raise LabelLookupExhaustedError("label lookup matched more than 100000 sessions")

    monkeypatch.setattr(SqlAlchemyConversationStore, "find_live_sessions_with_label", exhausted)


async def test_lookup_exhaustion_is_a_clean_409_on_sessions(
    register_host, client, repo, monkeypatch
) -> None:
    frames = register_host()
    agent = await create_test_agent(client, name="wt-attempt-exhaust")
    before = (await client.get("/v1/sessions", params={"visibility": "all"})).json()
    assert "data" in before, before
    _exhaust_label_lookup(monkeypatch)
    resp = await _post(client, agent["id"], repo)
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "conflict"
    assert _creates(frames) == []
    after = (await client.get("/v1/sessions", params={"visibility": "all"})).json()
    assert len(after.get("data", [])) == len(before.get("data", []))


async def test_lookup_exhaustion_is_a_clean_409_on_runner_route(
    register_host, client, repo, monkeypatch
) -> None:
    frames = register_host()
    agent = await create_test_agent(client, name="wt-attempt-exhaust-runner")
    session = (await client.post("/v1/sessions", json={"agent_id": agent["id"]})).json()
    _exhaust_label_lookup(monkeypatch)
    resp = await client.post(
        f"/v1/hosts/{_HOST_ID}/runners",
        json={
            "session_id": session["id"],
            "workspace": str(repo),
            "git": {"branch_name": "vitrine/run-x", "attempt_id": ATTEMPT},
        },
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["error"]["code"] == "conflict"
    assert _creates(frames) == []
    assert not any(isinstance(f, HostLaunchRunnerFrame) for f in frames)
