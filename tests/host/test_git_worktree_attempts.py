"""Attempt-mode worktree creation against real git: retries, residue, conflicts."""

from __future__ import annotations

import os
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

    def fake(args: list[str], *, cwd: str):
        if args[: len(prefix)] == prefix:
            raise gw.GitTimeoutError("git command timed out after 120s")
        return real(args, cwd=cwd)

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
    _git(repo, "worktree", "add", "--no-checkout", "--lock", "--reason", "initializing",
         str(path), "vitrine/run-a")
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
    out = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
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
