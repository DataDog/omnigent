"""
Server-side proxies for the host git-worktree tunnel frames.

Like ``_workspace_validation._ask_host_stat``: enqueue a
``host.create_worktree`` / ``host.remove_worktree`` frame, register a
future on the host connection, and await the result with a timeout. The
host (not the server) runs git. See designs/SESSION_GIT_WORKTREE.md.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import weakref
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

from omnigent.host.frames import (
    CAP_GIT_WORKTREE_ATTEMPTS,
    HostCreateWorktreeFrame,
    HostListWorktreesFrame,
    HostRemoveWorktreeFrame,
    encode_host_frame,
)
from omnigent.server.host_registry import HostConnection, HostRegistry

_logger = logging.getLogger(__name__)

# Above the host's own git timeout (120 s) so the host's specific error
# surfaces instead of a generic server-side timeout.
_WORKTREE_TIMEOUT_S: float = 150.0


WORKTREE_ROOT_LABEL_KEY = "omnigent.git.worktree_root_sha256"


def worktree_root_fingerprint(path: str) -> str:
    """Identify a canonical host root within the 256-character label limit.

    :param path: Canonical absolute worktree root returned by the host.
    :returns: Stable digest, normalizing Windows casing and separators.
    """
    if PureWindowsPath(path).is_absolute():
        path = path.replace("\\", "/").lower()
    return hashlib.sha256(path.rstrip("/").encode()).hexdigest()


def recorded_worktree_root(workspace: str, fingerprint: str | None) -> str | None:
    """Recover the recorded root from canonical workspace ancestors.

    :param workspace: Stored canonical session directory on the host.
    :param fingerprint: Recorded root digest, absent for legacy root sessions.
    :returns: Matching root, or None when the directory belongs to another worktree.
    """
    if fingerprint is None:
        return workspace
    path = (
        PureWindowsPath(workspace)
        if PureWindowsPath(workspace).is_absolute()
        else PurePosixPath(workspace)
    )
    return next(
        (
            str(parent)
            for parent in (path, *path.parents)
            if worktree_root_fingerprint(str(parent)) == fingerprint
        ),
        None,
    )


class WorktreeProxyError(Exception):
    """
    Raised when the host reports a worktree operation failure.

    These are typically user-correctable input problems (branch
    already exists, not a git repo, bad base ref), so the route layer
    maps this to ``INVALID_INPUT`` (400).

    :param message: Human-readable error suitable for the API
        response body, e.g.
        ``"worktree creation failed: branch already exists"``.
    """

    def __init__(self, message: str) -> None:
        """
        Initialize with the user-facing error message.

        :param message: Error string surfaced to the API caller.
        """
        super().__init__(message)
        self.message = message


class WorktreeHostUnavailableError(WorktreeProxyError):
    """
    Raised when the host can't be reached for a worktree operation.

    Connection loss or no reply within the timeout — an infrastructure
    condition, not user input. The route layer maps this to
    ``CONFLICT`` (409). Subclasses :class:`WorktreeProxyError` so
    best-effort callers that catch the base type still catch it.
    """


class WorktreeAttemptError(WorktreeProxyError):
    """
    A coded attempt-mode failure; ``code`` is an ``ErrorCode`` value.

    :param code: e.g. ``"worktree_creation_incomplete"``.
    :param message: User-facing detail from the host (or the server).
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


# Host codes the server passes through verbatim (all are ErrorCode values).
_ATTEMPT_ERROR_CODES = frozenset(
    {
        "worktree_creation_in_progress",
        "worktree_creation_incomplete",
        "worktree_attempt_conflict",
        "worktree_attempt_mismatch",
        "worktree_needs_attention",
        "worktree_attempt_removed",
    }
)


def host_supports_worktree_attempts(host_conn: HostConnection) -> bool:
    """Whether the host build honors ``attempt_id`` on worktree creation."""
    hello = getattr(host_conn, "hello", None)
    return hello is not None and CAP_GIT_WORKTREE_ATTEMPTS in (hello.capabilities or [])


@dataclass
class CreatedWorktree:
    """
    Result of a successful host worktree creation.

    :param worktree_path: Absolute path of the created worktree
        directory on the host, e.g.
        ``"/Users/alice/myrepo-worktrees/feature-login"``. Used for rollback.
    :param branch: The branch checked out in the worktree, e.g.
        ``"feature/login"``.
    :param workspace: Selected directory relocated into the new worktree.
        ``None`` for results from older hosts.
    """

    worktree_path: str
    branch: str
    workspace: str | None = None
    creation: dict[str, object] | None = None


async def _await_host_worktree_result(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    pending: dict[str, asyncio.Future[dict[str, object]]],
    request_id: str,
    frame: str,
    op: str,
) -> dict[str, object]:
    """
    Send a worktree frame and await its matching result over the tunnel.

    Shared plumbing for the create/remove proxies: register a future on
    ``pending`` keyed by ``request_id``, enqueue ``frame``, await the
    reply, and clean up on every path.

    :param host_registry: Registry used to enqueue the outbound frame.
    :param host_conn: Live host connection.
    :param pending: The connection's pending-future map for this op
        (``pending_create_worktrees`` or ``pending_remove_worktrees``).
    :param request_id: Correlation id already embedded in ``frame``.
    :param frame: Encoded host frame to send.
    :param op: Short label for error messages, e.g.
        ``"worktree creation"``.
    :returns: The host's result dict (``status`` plus op-specific
        fields).
    :raises WorktreeHostUnavailableError: On connection loss or no
        reply within :data:`_WORKTREE_TIMEOUT_S`.
    """
    future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
    pending[request_id] = future
    try:
        try:
            host_registry.send_text(host_conn, frame)
        except ConnectionError as exc:
            raise WorktreeHostUnavailableError(
                f"host '{host_conn.host_id}' connection lost during {op}"
            ) from exc
        try:
            return await asyncio.wait_for(future, timeout=_WORKTREE_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            raise WorktreeHostUnavailableError(
                f"host '{host_conn.host_id}' did not respond to {op} within "
                f"{_WORKTREE_TIMEOUT_S:.0f}s (it may be running an older version "
                "that does not support worktrees)"
            ) from exc
    finally:
        pending.pop(request_id, None)


async def create_worktree_on_host(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    repo_path: str,
    branch_name: str,
    base_branch: str | None,
    existing_branch: bool = False,
    attempt_id: str | None = None,
) -> CreatedWorktree:
    """
    Send a ``host.create_worktree`` frame and await the result.

    :param host_registry: Server-side registry; used to enqueue the
        outbound frame on the host's send queue.
    :param host_conn: Live host connection to create the worktree on.
    :param repo_path: Absolute path inside the source repo on the
        host — the canonical picked directory, e.g.
        ``"/Users/alice/myrepo"``.
    :param branch_name: New branch to create, e.g. ``"feature/login"``.
    :param base_branch: Optional base ref, e.g. ``"main"``. ``None``
        branches from the repo's current ``HEAD``.
    :param existing_branch: When ``True``, the host checks out the
        pre-existing ``branch_name`` into a fresh worktree (the
        deleted-worktree recreate path) instead of creating a branch.
    :param attempt_id: Client attempt id; enables idempotent, resumable
        creation on hosts with :data:`CAP_GIT_WORKTREE_ATTEMPTS`.
    :returns: The created worktree's path and branch (plus ``creation``
        receipt in attempt mode).
    :raises WorktreeAttemptError: Coded attempt-mode failures.
    :raises WorktreeHostUnavailableError: If the host connection drops
        or doesn't respond within :data:`_WORKTREE_TIMEOUT_S`.
    :raises WorktreeProxyError: If the host reports a worktree failure.
    """
    if attempt_id is not None and not host_supports_worktree_attempts(host_conn):
        raise WorktreeAttemptError(
            "worktree_attempts_unsupported",
            f"host '{host_conn.host_id}' does not support worktree creation attempts; "
            "upgrade the host or retry without attempt_id",
        )
    request_id = secrets.token_hex(8)
    frame = encode_host_frame(
        HostCreateWorktreeFrame(
            request_id=request_id,
            repo_path=repo_path,
            branch_name=branch_name,
            base_branch=base_branch,
            existing_branch=existing_branch,
            attempt_id=attempt_id,
        )
    )
    try:
        result = await _await_host_worktree_result(
            host_registry=host_registry,
            host_conn=host_conn,
            pending=host_conn.pending_create_worktrees,
            request_id=request_id,
            frame=frame,
            op="worktree creation",
        )
    except WorktreeHostUnavailableError as exc:
        if attempt_id is None:
            raise
        raise WorktreeAttemptError(
            "worktree_creation_incomplete",
            f"{exc.message}; the host may still be creating the worktree. "
            "Retry with the same attempt_id to resume or pick up the result.",
        ) from exc
    if result.get("status") != "ok":
        error_code = result.get("error_code")
        if attempt_id is not None and error_code in _ATTEMPT_ERROR_CODES:
            raise WorktreeAttemptError(
                str(error_code), str(result.get("error") or "host reported no detail")
            )
        raise WorktreeProxyError(
            f"worktree creation failed: {result.get('error') or 'host reported no detail'}"
        )
    worktree_path = result.get("worktree_path")
    branch = result.get("branch")
    if not isinstance(worktree_path, str) or not isinstance(branch, str):
        raise WorktreeProxyError("host returned an incomplete worktree result")
    workspace = result.get("workspace")
    creation = result.get("creation")
    return CreatedWorktree(
        worktree_path=worktree_path,
        branch=branch,
        workspace=workspace if isinstance(workspace, str) else None,
        creation=creation if attempt_id is not None and isinstance(creation, dict) else None,
    )


async def remove_worktree_on_host(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    worktree_path: str,
    branch: str | None,
    delete_branch: bool,
) -> None:
    """
    Send a ``host.remove_worktree`` frame and await the result.

    :param host_registry: Server-side registry; used to enqueue the
        outbound frame on the host's send queue.
    :param host_conn: Live host connection that owns the worktree.
    :param worktree_path: Absolute path of the worktree to remove on
        the host, e.g. ``"/Users/alice/myrepo-worktrees/feature-login"``.
    :param branch: Branch to delete when ``delete_branch`` is
        ``True``, e.g. ``"feature/login"``. ``None`` skips branch
        deletion.
    :param delete_branch: When ``True``, delete ``branch`` after
        removing the worktree directory.
    :raises WorktreeHostUnavailableError: If the host connection drops
        or doesn't respond within :data:`_WORKTREE_TIMEOUT_S`.
    :raises WorktreeProxyError: If the host reports a removal failure.
    """
    request_id = secrets.token_hex(8)
    frame = encode_host_frame(
        HostRemoveWorktreeFrame(
            request_id=request_id,
            worktree_path=worktree_path,
            branch=branch,
            delete_branch=delete_branch,
        )
    )
    result = await _await_host_worktree_result(
        host_registry=host_registry,
        host_conn=host_conn,
        pending=host_conn.pending_remove_worktrees,
        request_id=request_id,
        frame=frame,
        op="worktree removal",
    )
    if result.get("status") != "ok":
        raise WorktreeProxyError(
            f"worktree removal failed: {result.get('error') or 'host reported no detail'}"
        )


async def list_worktrees_on_host(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    repo_path: str,
    for_cleanup: bool = False,
) -> list[dict[str, object]]:
    """
    Send a ``host.list_worktrees`` frame and await the result.

    :param host_registry: Server-side registry; used to enqueue the
        outbound frame on the host's send queue.
    :param host_conn: Live host connection to list worktrees on.
    :param repo_path: Absolute path inside the source repo on the
        host — the canonical picked directory, e.g.
        ``"/Users/alice/myrepo"``.
    :param for_cleanup: Recover a canonical workspace without following replacement symlinks.
    :returns: One dict per worktree with keys ``path``, ``branch``,
        ``is_main``, ``detached``, and optional ``updated_at``
        (main first).
    :raises WorktreeHostUnavailableError: If the host connection drops
        or doesn't respond within :data:`_WORKTREE_TIMEOUT_S`.
    :raises WorktreeProxyError: If the host reports a listing failure.
    """
    request_id = secrets.token_hex(8)
    frame = encode_host_frame(
        HostListWorktreesFrame(
            request_id=request_id,
            repo_path=repo_path,
            for_cleanup=for_cleanup,
        )
    )
    result = await _await_host_worktree_result(
        host_registry=host_registry,
        host_conn=host_conn,
        pending=host_conn.pending_list_worktrees,
        request_id=request_id,
        frame=frame,
        op="worktree listing",
    )
    if result.get("status") != "ok":
        raise WorktreeProxyError(
            f"worktree listing failed: {result.get('error') or 'host reported no detail'}"
        )
    worktrees = result.get("worktrees")
    if not isinstance(worktrees, list):
        raise WorktreeProxyError("host returned an incomplete worktree list")
    return worktrees


async def ensure_bind_target_ready(
    *,
    host_registry: HostRegistry,
    host_conn: HostConnection,
    workspace: str,
) -> None:
    """
    Refuse binding a session to a worktree whose attempt receipt is partial.

    No receipt (legacy or hand-made worktree) is allowed. Old hosts and
    listing failures keep the legacy behavior with a warning.

    :param workspace: Canonical session directory on the host.
    :raises WorktreeAttemptError: ``worktree_not_ready`` for a partial worktree.
    """
    from omnigent.server.routes._workspace_validation import _is_subpath_of

    if not host_supports_worktree_attempts(host_conn):
        return
    try:
        worktrees = await list_worktrees_on_host(
            host_registry=host_registry, host_conn=host_conn, repo_path=workspace
        )
    except WorktreeProxyError:
        _logger.warning("Could not check worktree readiness for %s", workspace, exc_info=True)
        return
    containing = [
        tree
        for tree in worktrees
        if isinstance(path := tree.get("path"), str) and _is_subpath_of(workspace, path)
    ]
    if not containing:
        return
    entry = max(containing, key=lambda tree: len(str(tree["path"])))
    creation = entry.get("creation")
    if isinstance(creation, dict) and creation.get("state") == "partial":
        raise WorktreeAttemptError(
            "worktree_not_ready",
            f"worktree {entry['path']} is a partial creation from attempt "
            f"{creation.get('attempt_id')}; retry its create with the same attempt_id "
            "before binding a session to it",
        )


def git_worktree_receipt(
    creation: dict[str, object], *, session_reused: bool = False
) -> dict[str, object]:
    """API ``git_worktree`` receipt from the host's ``creation`` result."""
    return {
        "attempt_id": creation.get("attempt_id"),
        "state": creation.get("state"),
        "outcome": creation.get("outcome"),
        "worktree_path": creation.get("worktree_path"),
        "branch": creation.get("branch"),
        "workspace": creation.get("workspace"),
        "session_reused": session_reused,
    }


# Label stamped on every session created with a git worktree attempt id.
WORKTREE_ATTEMPT_LABEL_KEY = "omnigent.git.worktree_attempt"

# Per-process only: concurrent same-attempt POSTs on one replica serialize here.
# Across replicas the host receipt still prevents a duplicate worktree, but two
# replicas racing the very first create can each create a session.
# custom-lint: disable-next=workspace-scoped-cache -- keyed by globally unique host id
_attempt_locks: weakref.WeakValueDictionary[tuple[str, str], asyncio.Lock] = (
    weakref.WeakValueDictionary()
)


def attempt_create_lock(host_id: str, attempt_id: str) -> asyncio.Lock:
    """Lock serializing worktree + session creation for one attempt."""
    key = (host_id, attempt_id)
    lock = _attempt_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _attempt_locks[key] = lock
    return lock


# Fingerprint of the source repository an attempt-labeled session was created from.
WORKTREE_ATTEMPT_REPO_LABEL_KEY = "omnigent.git.worktree_attempt_repo_sha256"


def attempt_labels(attempt_id: str, source_repo: str) -> dict[str, str]:
    """Labels stamped on a session bound to an attempt-mode worktree."""
    return {
        WORKTREE_ATTEMPT_LABEL_KEY: attempt_id,
        WORKTREE_ATTEMPT_REPO_LABEL_KEY: worktree_root_fingerprint(source_repo),
    }


async def find_attempt_session(
    conversation_store: Any,
    permission_store: Any,
    *,
    user_id: str | None,
    host_id: str,
    attempt_id: str,
    source_repo: str,
) -> Any:
    """
    The live session an earlier request bound to ``attempt_id`` on this host.

    :param source_repo: Canonical source repository of this request.
    :returns: The caller-owned session, or ``None``.
    :raises WorktreeAttemptError: ``worktree_attempt_mismatch`` when the attempt
        was used with another repository; ``worktree_attempt_conflict`` when
        another user's session owns it.
    """
    from omnigent.server.auth import LEVEL_OWNER
    from omnigent.stores.conversation_store import LabelLookupExhaustedError

    try:
        ids = await asyncio.to_thread(
            conversation_store.find_live_sessions_with_label,
            host_id=host_id,
            key=WORKTREE_ATTEMPT_LABEL_KEY,
            value=attempt_id,
        )
    except LabelLookupExhaustedError as exc:
        # Never guess "no session" here: refuse cleanly before anything is created.
        raise WorktreeAttemptError(
            "conflict",
            f"too many sessions are labeled with attempt {attempt_id} to check safely; "
            "nothing was created. Use a new attempt_id.",
        ) from exc
    expected = worktree_root_fingerprint(source_repo)
    for conversation_id in ids:
        conv = await asyncio.to_thread(conversation_store.get_conversation, conversation_id)
        if conv is None:
            continue
        recorded = conv.labels.get(WORKTREE_ATTEMPT_REPO_LABEL_KEY)
        if recorded is not None and recorded != expected:
            raise WorktreeAttemptError(
                "worktree_attempt_mismatch",
                f"attempt {attempt_id} was used with a different source repository; "
                "use a new attempt_id for a different worktree",
            )
        if user_id is not None and permission_store is not None:
            grant = await asyncio.to_thread(permission_store.get, user_id, conversation_id)
            if grant is None or grant.level < LEVEL_OWNER:
                raise WorktreeAttemptError(
                    "worktree_attempt_conflict",
                    f"attempt {attempt_id} belongs to another user's session",
                )
        return conv
    return None
