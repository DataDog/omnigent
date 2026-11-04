"""Attempt-mode worktree creation against real git: retries, residue, conflicts."""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

import omnigent.host.git_worktree as gw
import omnigent.host.git_worktree_attempts as gwa
from omnigent.host.git_worktree_attempts import (
    ATTEMPT_CONFLICT,
    ATTEMPT_MISMATCH,
    ATTEMPT_REMOVED,
    IN_PROGRESS,
    INCOMPLETE,
    NEEDS_ATTENTION,
    WorktreeAttemptError,
    create_worktree_attempt,
    store_for_repo,
)

_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}
A1 = "attempt-0000000000000001"
A2 = "attempt-0000000000000002"


@pytest.fixture(autouse=True)
def _isolated_attempt_index(tmp_path_factory: pytest.TempPathFactory, monkeypatch) -> None:
    """Fresh host-wide attempt→repository index per test (attempt ids repeat across tests)."""
    monkeypatch.setenv("OMNIGENT_DATA_DIR", str(tmp_path_factory.mktemp("omnigent-data")))


def _git(cwd: Path | str, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "commit.gpgsign=false", *args],
        cwd=cwd,
        env={**os.environ, **_ENV},
        check=True,
        capture_output=True,
        text=True,
    ).stdout


@pytest.fixture()
def repo(tmp_path: Path) -> Iterator[Path]:
    root = (tmp_path / "repo").resolve()
    root.mkdir()
    _git(root, "init", "-q", "-b", "main")
    for i in range(20):
        (root / f"f{i}.txt").write_text(f"content {i}\n")
    (root / "sub").mkdir()
    (root / "sub" / "x.txt").write_text("x\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "init")
    yield root


def _create(repo: Path, attempt: str = A1, branch: str = "vitrine/run-a", **kw: object):
    return create_worktree_attempt(
        repo_path=str(repo), branch_name=branch, attempt_id=attempt, **kw
    )


def _code(exc: pytest.ExceptionInfo[WorktreeAttemptError]) -> str:
    return exc.value.code


def _timeout_at(monkeypatch: pytest.MonkeyPatch, prefix: list[str]) -> None:
    real = gw._run_git

    def fake(args: list[str], *, cwd: str, **kw):
        if args[: len(prefix)] == prefix:
            raise gw.GitTimeoutError("git command timed out after 120s")
        return real(args, cwd=cwd, **kw)

    monkeypatch.setattr(gw, "_run_git", fake)


def _make_partial(repo: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Leave the residue of a host killed mid-checkout: locked registration, some files."""
    with monkeypatch.context() as m:
        _timeout_at(m, ["worktree", "add"])
        with pytest.raises(WorktreeAttemptError) as exc:
            _create(repo)
    assert _code(exc) == INCOMPLETE
    receipt = store_for_repo(str(repo)).load(A1)
    assert receipt is not None and receipt.phase == gwa.PHASE_BRANCH_CREATED
    path = Path(receipt.worktree_path)
    _git(
        repo,
        "worktree",
        "add",
        "--no-checkout",
        "--lock",
        "--reason",
        "initializing",
        str(path),
        "vitrine/run-a",
    )
    for i in range(5):
        (path / f"f{i}.txt").write_text(f"content {i}\n")
    return path


def test_fresh_create_writes_ready_receipt_and_listing(repo: Path) -> None:
    result = _create(repo, base_branch="main")
    assert result.outcome == "created"
    assert result.receipt.state == "ready"
    path = Path(result.worktree.worktree_path)
    assert (path / "f19.txt").read_text() == "content 19\n"
    rows = gw.list_worktrees(repo_path=str(repo))
    row = next(r for r in rows if r.branch == "vitrine/run-a")
    assert row.creation is not None
    assert row.creation["attempt_id"] == A1 and row.creation["state"] == "ready"
    assert next(r for r in rows if r.is_main).creation is None


def test_retry_returns_existing_and_keeps_user_work(repo: Path) -> None:
    first = _create(repo)
    path = Path(first.worktree.worktree_path)
    (path / "f0.txt").write_text("user edit\n")
    (path / "new.txt").write_text("untracked\n")
    _git(path, "add", "new.txt")
    _git(path, "commit", "-q", "-m", "user commit")
    again = _create(repo)
    assert again.outcome == "existing"
    assert again.worktree.worktree_path == str(path)
    assert (path / "f0.txt").read_text() == "user edit\n"
    assert _git(path, "log", "-1", "--format=%s").strip() == "user commit"


def test_subdirectory_workspace(repo: Path) -> None:
    result = create_worktree_attempt(
        repo_path=str(repo / "sub"), branch_name="b-sub", attempt_id=A1
    )
    assert result.worktree.workspace == str(Path(result.worktree.worktree_path) / "sub/")


def test_mismatched_retry(repo: Path) -> None:
    _create(repo)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo, branch="other")
    assert _code(exc) == ATTEMPT_MISMATCH


def test_preexisting_branch_without_receipt_is_refused(repo: Path) -> None:
    _git(repo, "branch", "vitrine/run-a")
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == ATTEMPT_CONFLICT
    assert store_for_repo(str(repo)).load(A1) is None


def test_branch_owned_by_another_attempt_is_refused(repo: Path, monkeypatch) -> None:
    with monkeypatch.context() as m:
        _timeout_at(m, ["branch"])
        with pytest.raises(WorktreeAttemptError):
            _create(repo, attempt=A2)
    # A2 reserved the branch but never created it; A1 must not take it.
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == ATTEMPT_CONFLICT


def test_unregistered_nonempty_dir_at_reserved_path_is_refused(repo: Path, monkeypatch) -> None:
    with monkeypatch.context() as m:
        _timeout_at(m, ["worktree", "add"])
        with pytest.raises(WorktreeAttemptError):
            _create(repo)
    path = Path(store_for_repo(str(repo)).load(A1).worktree_path)
    path.mkdir(parents=True)
    (path / "mine.txt").write_text("someone else's\n")
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == ATTEMPT_CONFLICT
    assert (path / "mine.txt").read_text() == "someone else's\n"


@pytest.mark.parametrize("step", [["branch"], ["worktree", "add"]])
def test_timeout_residue_resumes(repo: Path, monkeypatch, step: list[str]) -> None:
    """Branch-only and reserved-only residue (no list row) recover via the same id."""
    with monkeypatch.context() as m:
        _timeout_at(m, step)
        with pytest.raises(WorktreeAttemptError) as exc:
            _create(repo)
    assert _code(exc) == INCOMPLETE
    assert "retry with the same attempt_id" in str(exc.value)
    result = _create(repo)
    assert result.outcome == "resumed"
    assert result.receipt.state == "ready"
    assert (Path(result.worktree.worktree_path) / "f19.txt").exists()


def test_sigkill_residue_completes_without_overwriting(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    rows = gw.list_worktrees(repo_path=str(repo))
    assert next(r for r in rows if r.branch == "vitrine/run-a").creation["state"] == "partial"
    before = {p.name: p.stat().st_mtime_ns for p in path.glob("f*.txt")}
    result = _create(repo)
    assert result.outcome == "resumed"
    for name, mtime in before.items():
        assert (path / name).stat().st_mtime_ns == mtime  # existing files untouched
    assert len(list(path.glob("f*.txt"))) == 20
    assert _git(path, "status", "--porcelain") == ""
    assert "locked" not in _git(repo, "worktree", "list", "--porcelain")


def test_modified_and_untracked_files_need_attention(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    (path / "f1.txt").write_text("edited\n")
    (path / "stray.txt").write_text("stray\n")
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert "f1.txt" in str(exc.value) and "stray.txt" in str(exc.value)
    assert (path / "f1.txt").read_text() == "edited\n"
    assert (path / "stray.txt").read_text() == "stray\n"
    assert not (path / "f10.txt").exists()


def test_edit_between_inspection_and_completion_is_never_rewritten(
    repo: Path, monkeypatch
) -> None:
    path = _make_partial(repo, monkeypatch)
    real_fill = gwa._fill_missing_files

    def racing_fill(worktree: str) -> None:
        # The user creates a still-missing file right after inspection.
        (Path(worktree) / "f10.txt").write_text("user wrote this\n")
        real_fill(worktree)

    monkeypatch.setattr(gwa, "_fill_missing_files", racing_fill)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert "f10.txt" in str(exc.value)
    assert (path / "f10.txt").read_text() == "user wrote this\n"
    assert store_for_repo(str(repo)).load(A1).phase != gwa.PHASE_READY


def test_moved_tip_needs_attention(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    other = _git(repo, "commit-tree", "-m", "x", "-p", "HEAD", "HEAD^{tree}").strip()
    _git(repo, "update-ref", "refs/heads/vitrine/run-a", other)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert len(list(path.glob("f*.txt"))) == 5


def test_index_lock_needs_attention(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    git_dir = Path(_git(path, "rev-parse", "--absolute-git-dir").strip())
    (git_dir / "index.lock").write_text("")
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert (git_dir / "index.lock").exists()


def test_restart_resumes_from_receipt_alone(repo: Path, monkeypatch) -> None:
    _make_partial(repo, monkeypatch)
    script = (
        "import sys; from omnigent.host.git_worktree_attempts import create_worktree_attempt as c;"
        f"r=c(repo_path={str(repo)!r}, branch_name='vitrine/run-a', attempt_id={A1!r});"
        "print(r.outcome)"
    )
    out = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "resumed"


def test_concurrent_same_attempt_yields_one_worktree(repo: Path) -> None:
    results: list[object] = []

    def run() -> None:
        results.append(_create(repo))

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(r.outcome for r in results) == ["created", "existing"]
    assert len({r.worktree.worktree_path for r in results}) == 1
    assert _git(repo, "worktree", "list").count("\n") == 2


def test_concurrent_different_attempts_same_branch_one_winner(repo: Path) -> None:
    outcomes: list[str] = []

    def run(attempt: str) -> None:
        try:
            outcomes.append(_create(repo, attempt=attempt).outcome)
        except WorktreeAttemptError as exc:
            outcomes.append(exc.code)

    threads = [threading.Thread(target=run, args=(a,)) for a in (A1, A2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(outcomes) == sorted(["created", ATTEMPT_CONFLICT])


def test_lock_held_reports_in_progress(repo: Path, monkeypatch) -> None:
    monkeypatch.setattr(gwa, "LOCK_TIMEOUT_S", 0.2)
    holder_ready = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with store_for_repo(str(repo)).lock():
            holder_ready.set()
            release.wait(5)

    t = threading.Thread(target=hold)
    t.start()
    holder_ready.wait(5)
    try:
        with pytest.raises(WorktreeAttemptError) as exc:
            _create(repo)
        assert _code(exc) == IN_PROGRESS
    finally:
        release.set()
        t.join()
    assert store_for_repo(str(repo)).load(A1) is None


def test_ready_then_removed_is_reported(repo: Path) -> None:
    result = _create(repo)
    gw.remove_worktree(worktree_path=result.worktree.worktree_path)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == ATTEMPT_REMOVED


def test_existing_branch_mode(repo: Path) -> None:
    _git(repo, "branch", "keep")
    result = _create(repo, branch="keep", existing_branch=True)
    assert result.outcome == "created"
    assert _create(repo, branch="keep", existing_branch=True).outcome == "existing"


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-only")
def test_timeout_kills_checkout_process_group(repo: Path, monkeypatch, tmp_path: Path) -> None:
    marker = tmp_path / "smudge-finished"
    _git(repo, "config", "filter.slow.smudge", f"sh -c 'sleep 3; touch {marker}; cat'")
    _git(repo, "config", "filter.slow.clean", "cat")
    (repo / ".gitattributes").write_text("f0.txt filter=slow\n")
    _git(repo, "add", ".gitattributes")
    _git(repo, "commit", "-q", "-m", "attrs")
    monkeypatch.setenv("OMNIGENT_HOST_GIT_TIMEOUT_S", "1")
    monkeypatch.setattr(gw, "_GIT_TERM_GRACE_S", 1.0)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == INCOMPLETE
    time.sleep(4)
    assert not marker.exists(), "a checkout grandchild kept running after the timeout"
    monkeypatch.delenv("OMNIGENT_HOST_GIT_TIMEOUT_S")
    result = _create(repo)
    assert result.outcome == "resumed"
    assert (Path(result.worktree.worktree_path) / "f0.txt").read_text() == "content 0\n"


def _install_filter(repo: Path, tmp_path: Path, body: str) -> None:
    script = tmp_path / "smudge.sh"
    script.write_text("#!/bin/sh\n" + body + "\ncat\n")
    script.chmod(0o755)
    _git(repo, "config", "filter.side.smudge", f"{script} %f")
    _git(repo, "config", "filter.side.clean", "cat")
    (repo / ".gitattributes").write_text("*.txt filter=side\n")
    _git(repo, "add", ".gitattributes")
    _git(repo, "commit", "-q", "-m", "attrs")


@pytest.mark.parametrize("branch_only_retry", [False, True])
def test_file_written_during_fill_is_never_overwritten(
    repo: Path, monkeypatch, tmp_path: Path, branch_only_retry: bool
) -> None:
    """A file that appears while missing files are written is kept and reported."""
    _install_filter(
        repo,
        tmp_path,
        'if [ "$1" = f0.txt ] && [ ! -e f1.txt ]; then printf "user edit\\n" > f1.txt; fi',
    )
    if branch_only_retry:
        with monkeypatch.context() as m:
            _timeout_at(m, ["worktree", "add"])
            with pytest.raises(WorktreeAttemptError):
                _create(repo)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert "f1.txt" in str(exc.value)
    path = Path(store_for_repo(str(repo)).load(A1).worktree_path)
    assert (path / "f1.txt").read_text() == "user edit\n"


def test_staged_snapshot_is_never_replaced(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    _git(path, "read-tree", "HEAD")
    (path / "f2.txt").write_text("staged\n")
    _git(path, "add", "f2.txt")
    (path / "f2.txt").write_text("content 2\n")  # working tree matches the commit again
    tree_before = _git(path, "write-tree").strip()
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION and "f2.txt" in str(exc.value)
    assert _git(path, "write-tree").strip() == tree_before


def _point_at_foreign_clone(path: Path, tmp_path: Path) -> None:
    other = (tmp_path / "other").resolve()
    other.mkdir()
    _git(other, "init", "-q", "-b", "main")
    (other / "x").write_text("x")
    _git(other, "add", "-A")
    _git(other, "commit", "-q", "-m", "x")
    (path / ".git").write_text(f"gitdir: {other / '.git'}\n")


def test_foreign_pointer_in_partial_is_not_adopted(repo: Path, monkeypatch, tmp_path) -> None:
    path = _make_partial(repo, monkeypatch)
    _point_at_foreign_clone(path, tmp_path)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert "different repository" in str(exc.value)
    assert len(list(path.glob("f*.txt"))) == 5


def test_foreign_pointer_after_ready_is_not_existing(repo: Path, tmp_path: Path) -> None:
    path = Path(_create(repo).worktree.worktree_path)
    _point_at_foreign_clone(path, tmp_path)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION


def test_same_attempt_in_another_repository_is_a_mismatch(repo: Path, tmp_path: Path) -> None:
    other = (tmp_path / "repo2").resolve()
    other.mkdir()
    _git(other, "init", "-q", "-b", "main")
    (other / "a").write_text("a")
    _git(other, "add", "-A")
    _git(other, "commit", "-q", "-m", "a")
    _create(repo)
    with pytest.raises(WorktreeAttemptError) as exc:
        create_worktree_attempt(repo_path=str(other), branch_name="vitrine/run-a", attempt_id=A1)
    assert _code(exc) == ATTEMPT_MISMATCH
    assert not (other.parent / "repo2-worktrees").exists()


@pytest.mark.skipif(os.name != "posix", reason="process groups are POSIX-only")
def test_restart_stops_git_group_left_by_crashed_host(repo: Path, tmp_path: Path) -> None:
    """Host-only SIGKILL leaves git (own process group) running; the restarted host stops it."""
    entered = tmp_path / "smudge-entered"
    late = tmp_path / "late-write"
    _install_filter(
        repo,
        tmp_path,
        f'if [ -n "$SLOW_SMUDGE" ] && [ "$1" = f0.txt ]; then touch {entered}; sleep 30; '
        f"touch {late}; fi",
    )
    script = (
        "from omnigent.host.git_worktree_attempts import create_worktree_attempt as c;"
        f"c(repo_path={str(repo)!r}, branch_name='vitrine/run-a', attempt_id={A1!r})"
    )
    host = subprocess.Popen([sys.executable, "-c", script], env={**os.environ, "SLOW_SMUDGE": "1"})
    try:
        # Barrier: git is now blocked inside the smudge filter of the real checkout.
        deadline = time.monotonic() + 60
        while not entered.exists() and time.monotonic() < deadline:
            assert host.poll() is None, "host exited before reaching the checkout"
            time.sleep(0.02)
        assert entered.exists(), "checkout never reached the smudge filter"
        groups = [
            int(g["pgid"])
            for g in json.loads(store_for_repo(str(repo)).pgids_path(A1).read_text())
        ]
        assert groups, "no git process group was recorded"
    finally:
        host.kill()  # host only: the git group survives
        host.wait()
    assert any(_group_alive(pgid) for pgid in groups), "the git group died with the host"
    restart = subprocess.run(
        [sys.executable, "-c", script.replace("c(", "r=c(") + ";print(r.outcome)"],
        capture_output=True,
        text=True,
    )
    assert restart.returncode == 0, restart.stderr
    assert restart.stdout.strip() == "resumed"
    assert not any(_group_alive(pgid) for pgid in groups), "the orphaned git group survived"
    assert not late.exists(), "the orphaned git group kept writing after the restart"
    path = Path(store_for_repo(str(repo)).load(A1).worktree_path)
    assert (path / "f0.txt").read_text() == "content 0\n"
    assert not (_git_dir(path) / "index.lock").exists()


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def _git_dir(path: Path) -> Path:
    return Path(_git(path, "rev-parse", "--absolute-git-dir").strip())


def test_index_staged_after_inspection_is_never_replaced(repo: Path, monkeypatch) -> None:
    """No index at inspection; the user stages before our read-tree: their snapshot wins."""
    path = _make_partial(repo, monkeypatch)
    real_lock = gwa._hold_index_lock

    def user_stages_first(receipt, git_dir):
        (path / "f2.txt").write_text("USER STAGED SNAPSHOT\n")
        _git(path, "add", "f2.txt")
        (path / "f2.txt").write_text("content 2\n")  # worktree text restored
        return real_lock(receipt, git_dir)

    monkeypatch.setattr(gwa, "_hold_index_lock", user_stages_first)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert _git(path, "show", ":f2.txt") == "USER STAGED SNAPSHOT\n"
    assert store_for_repo(str(repo)).load(A1).phase != gwa.PHASE_READY


def test_index_lock_held_by_another_git_blocks_index_creation(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    real_lock = gwa._hold_index_lock

    def foreign_lock_first(receipt, git_dir):
        (git_dir / "index.lock").write_text("")
        return real_lock(receipt, git_dir)

    monkeypatch.setattr(gwa, "_hold_index_lock", foreign_lock_first)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION
    assert (_git_dir(path) / "index.lock").exists()
    assert not (_git_dir(path) / "index").exists()


def test_index_lock_appearing_after_fill_is_never_ready(repo: Path, monkeypatch) -> None:
    path = _make_partial(repo, monkeypatch)
    real_fill = gwa._fill_missing_files

    def fill_then_lock(worktree: str) -> None:
        real_fill(worktree)
        (_git_dir(path) / "index.lock").write_text("")

    monkeypatch.setattr(gwa, "_fill_missing_files", fill_then_lock)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == NEEDS_ATTENTION and "index.lock" in str(exc.value)
    assert (_git_dir(path) / "index.lock").exists()
    assert store_for_repo(str(repo)).load(A1).phase != gwa.PHASE_READY


_RECORDED = {
    "pid": 4242,
    "pgid": 4242,
    "started": "Wed Nov  4 13:40:12 2026",
    "command": "git checkout-index --all --quiet",
    "exe": "/usr/bin/git",
    "cwd": "/repo-worktrees/vitrine-run-a",
}
_PS_KEYS = ("pid", "pgid", "started", "command")


class _FakeGroup:
    """A fake process table entry: no real process is ever signaled."""

    def __init__(self, probes: list, *, alive=True, dies_on=(), probe_error=None, term_error=None):
        self.probes = list(probes)
        self.current = self.probes[0] if self.probes else None
        self.alive = alive
        self.dies_on = set(dies_on)
        self.probe_error = probe_error
        self.term_error = term_error
        self.sent: list[int] = []

    def probe(self, pid: int):
        if self.probes:
            self.current = self.probes.pop(0) if len(self.probes) > 1 else self.probes[0]
        return None if self.current is None else dict(self.current)

    def ps(self, pid: int):
        if self.current is None:
            return None
        return {key: self.current[key] for key in _PS_KEYS}

    def killpg(self, pgid: int, sig: int) -> None:
        if sig == 0:
            if self.probe_error is not None and self.sent:
                raise self.probe_error()
            if not self.alive:
                raise ProcessLookupError()
            return
        if self.term_error is not None:
            raise self.term_error()
        self.sent.append(sig)
        if sig in self.dies_on:
            self.alive = False


def _reap(monkeypatch, repo: Path, group: _FakeGroup, recorded: dict | None = None):
    store = store_for_repo(str(repo))
    store.dir.mkdir(parents=True, exist_ok=True)
    store.pgids_path(A1).write_text(json.dumps([recorded or _RECORDED]))
    monkeypatch.setattr(gwa, "_probe_process", group.probe)
    monkeypatch.setattr(gwa, "_ps_snapshot", group.ps)
    monkeypatch.setattr(gwa.os, "killpg", group.killpg)
    monkeypatch.setattr(gw, "_GIT_TERM_GRACE_S", 0.05)
    try:
        return store.reap_orphan_git_groups(A1), None
    except WorktreeAttemptError as exc:
        return None, exc


def _record_kept(repo: Path) -> bool:
    return store_for_repo(str(repo)).pgids_path(A1).exists()


def test_reaper_stops_a_proven_group_and_drops_its_record(repo: Path, monkeypatch) -> None:
    group = _FakeGroup([dict(_RECORDED)], dies_on={signal.SIGTERM})
    stopped, exc = _reap(monkeypatch, repo, group)
    assert exc is None and stopped == [4242] and group.sent == [signal.SIGTERM]
    assert not _record_kept(repo)


@pytest.mark.parametrize(
    "current",
    [
        {**_RECORDED, "started": "Thu Nov  5 09:00:00 2026", "command": "/usr/bin/git status"},
        {**_RECORDED, "command": "/usr/local/bin/not-git-helper"},
        {**_RECORDED, "pgid": 999},
        {**_RECORDED, "exe": "/usr/local/bin/other"},
        {**_RECORDED, "cwd": "/unrelated"},
        None,
    ],
    ids=[
        "reused-pgid-git-status",
        "not-git-helper",
        "left-group",
        "other-exe",
        "other-cwd",
        "no-leader",
    ],
)
def test_reaper_fails_closed_on_a_live_unproven_group(repo: Path, monkeypatch, current) -> None:
    """The group still exists but can't be proven ours: no signal, record kept, no resume."""
    group = _FakeGroup([current])
    _, exc = _reap(monkeypatch, repo, group)
    assert group.sent == []
    assert exc is not None and exc.code == INCOMPLETE and "4242" in exc.message
    assert _record_kept(repo)


def test_reaper_drops_the_record_of_a_group_that_is_gone(repo: Path, monkeypatch) -> None:
    group = _FakeGroup([None], alive=False)
    stopped, exc = _reap(monkeypatch, repo, group)
    assert exc is None and stopped == [] and group.sent == []
    assert not _record_kept(repo)


def test_reaper_keeps_the_record_when_signaling_is_refused(
    repo: Path, monkeypatch, caplog
) -> None:
    group = _FakeGroup([dict(_RECORDED)], term_error=PermissionError)
    with caplog.at_level("WARNING", logger=gw._logger.name):
        _, exc = _reap(monkeypatch, repo, group)
    assert exc is not None and exc.code == INCOMPLETE
    assert "not signaling process group 4242: permission denied" in caplog.text
    assert _record_kept(repo)


def test_reaper_probe_eperm_after_signal_is_logged_and_treated_as_gone(
    repo: Path, monkeypatch, caplog
) -> None:
    """macOS reports EPERM polling a group whose members exited; that ends the wait."""
    group = _FakeGroup([dict(_RECORDED)], probe_error=PermissionError)
    with caplog.at_level("DEBUG", logger=gw._logger.name):
        stopped, exc = _reap(monkeypatch, repo, group)
    assert exc is None and stopped == [4242] and group.sent == [signal.SIGTERM]
    assert "process group 4242 no longer signalable (EPERM)" in caplog.text


def test_reaper_rechecks_identity_before_kill(repo: Path, monkeypatch) -> None:
    """Identity changes after SIGTERM (an unrelated git took the pid): no SIGKILL, fail closed."""
    changed = {**_RECORDED, "command": "/unrelated/git status", "cwd": "/unrelated"}
    group = _FakeGroup([dict(_RECORDED), dict(_RECORDED), changed])
    _, exc = _reap(monkeypatch, repo, group)
    assert group.sent == [signal.SIGTERM]
    assert exc is not None and exc.code == INCOMPLETE and _record_kept(repo)


def test_reaper_escalates_while_identity_holds(repo: Path, monkeypatch) -> None:
    group = _FakeGroup([dict(_RECORDED)], dies_on={signal.SIGKILL})
    stopped, exc = _reap(monkeypatch, repo, group)
    assert exc is None and group.sent == [signal.SIGTERM, signal.SIGKILL] and stopped == [4242]


def test_reaper_never_signals_a_record_without_exe_and_cwd(repo: Path, monkeypatch) -> None:
    legacy = {key: value for key, value in _RECORDED.items() if key not in ("exe", "cwd")}
    group = _FakeGroup([dict(_RECORDED)])
    _, exc = _reap(monkeypatch, repo, group, recorded=legacy)
    assert group.sent == [] and exc is not None and exc.code == INCOMPLETE and _record_kept(repo)


def test_reaper_skips_the_signal_when_ps_changes_at_the_last_moment(
    repo: Path, monkeypatch
) -> None:
    group = _FakeGroup([dict(_RECORDED)])
    monkeypatch.setattr(gwa, "_ps_still_matches", lambda identity: False)
    _, exc = _reap(monkeypatch, repo, group)
    assert group.sent == [] and exc is not None and exc.code == INCOMPLETE


def test_probe_rejects_a_torn_ps_snapshot(monkeypatch) -> None:
    """The ps fields changed while exe/cwd were read: no identity is reported."""
    snapshots = iter(
        [
            {key: _RECORDED[key] for key in _PS_KEYS},
            {**{key: _RECORDED[key] for key in _PS_KEYS}, "started": "Thu Nov  5 09:00:00 2026"},
        ]
    )
    monkeypatch.setattr(gwa, "_ps_snapshot", lambda pid: next(snapshots))
    monkeypatch.setattr(gwa, "_process_exe_and_cwd", lambda pid: ("/usr/bin/git", "/w"))
    assert gwa._probe_process(4242) is None


def test_replaced_index_lock_is_left_for_the_foreign_git(repo: Path, monkeypatch) -> None:
    """Our lock is unlinked and a live `git add` takes a new one: we must not delete it."""
    path = _make_partial(repo, monkeypatch)
    common = Path(_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir").strip())
    (common / "info").mkdir(exist_ok=True)
    (common / "info" / "attributes").write_text("f3.txt filter=slowclean\n")
    _git(repo, "config", "filter.slowclean.clean", "sh -c 'sleep 2; cat'")
    (path / "f3.txt").write_text("foreign change\n")
    lock = _git_dir(path) / "index.lock"
    foreign: list[subprocess.Popen[str]] = []
    real = gw._run_git

    def replace_lock_after_read_tree(args: list[str], *, cwd: str, **kw):
        result = real(args, cwd=cwd, **kw)
        if args[0] == "read-tree" and not foreign:
            lock.unlink()
            foreign.append(
                subprocess.Popen(
                    ["git", "add", "f3.txt"], cwd=path, stderr=subprocess.PIPE, text=True
                )
            )
            deadline = time.monotonic() + 10
            while not lock.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert lock.exists(), "foreign git add never took the lock"
        return result

    monkeypatch.setattr(gw, "_run_git", replace_lock_after_read_tree)
    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    foreign_lock_survived = lock.exists() and foreign[0].poll() is None
    assert foreign_lock_survived, "our cleanup removed the foreign git's index.lock"
    assert _code(exc) == NEEDS_ATTENTION
    assert "index.lock was replaced by another process" in exc.value.message
    _, stderr = foreign[0].communicate(timeout=20)
    assert foreign[0].returncode == 0, stderr
    assert store_for_repo(str(repo)).load(A1).phase != gwa.PHASE_READY


def test_probe_reports_exe_and_cwd_of_a_real_process(tmp_path: Path) -> None:
    proc = subprocess.Popen(["sleep", "5"], cwd=tmp_path)
    try:
        identity = gwa._probe_process(proc.pid)
        assert identity is not None
        assert identity["exe"] == os.path.realpath(shutil.which("sleep") or "")
        assert os.path.realpath(str(identity["cwd"])) == os.path.realpath(tmp_path)
    finally:
        proc.kill()
        proc.wait()


def test_live_unprovable_writer_blocks_resume_and_later_edit_survives(
    repo: Path, tmp_path: Path
) -> None:
    """A legacy record (no exe/cwd) of a still-running git: the retry must not resume."""
    entered = tmp_path / "smudge-entered"
    _install_filter(
        repo,
        tmp_path,
        f'if [ -n "$SLOW_SMUDGE" ] && [ "$1" = f0.txt ]; then touch {entered}; sleep 3; '
        'printf "filter wrote\\n" > f1.txt; fi',
    )
    script = (
        "from omnigent.host.git_worktree_attempts import create_worktree_attempt as c;"
        f"c(repo_path={str(repo)!r}, branch_name='vitrine/run-a', attempt_id={A1!r})"
    )
    host = subprocess.Popen([sys.executable, "-c", script], env={**os.environ, "SLOW_SMUDGE": "1"})
    store = store_for_repo(str(repo))
    try:
        deadline = time.monotonic() + 60
        while not entered.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert entered.exists()
    finally:
        host.kill()
        host.wait()
    records = json.loads(store.pgids_path(A1).read_text())
    legacy = [{k: v for k, v in r.items() if k not in ("exe", "cwd")} for r in records]
    store.pgids_path(A1).write_text(json.dumps(legacy))
    groups = [int(r["pgid"]) for r in legacy]
    assert any(_group_alive(pgid) for pgid in groups)

    with pytest.raises(WorktreeAttemptError) as exc:
        _create(repo)
    assert _code(exc) == INCOMPLETE
    assert store.load(A1).phase != gwa.PHASE_READY
    assert store.pgids_path(A1).exists()

    deadline = time.monotonic() + 30
    while any(_group_alive(pgid) for pgid in groups) and time.monotonic() < deadline:
        time.sleep(0.05)
    path = Path(store.load(A1).worktree_path)
    (path / "f1.txt").write_text("user edit\n")
    with contextlib.suppress(WorktreeAttemptError):
        _create(repo)
    assert (path / "f1.txt").read_text() == "user edit\n"
