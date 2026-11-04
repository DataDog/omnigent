"""Idempotent, resumable worktree creation keyed by a client attempt id.

Each attempt writes a receipt under the repository's git common dir
(``<common>/omnigent/worktree-attempts/<attempt_id>.json``) before touching
git, so a retry after a timeout or a dropped reply can prove the branch and
checkout it finds are its own. Recovery never deletes, resets, cleans, or
overwrites files. See designs/SESSION_GIT_WORKTREE.md.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path

from filelock import FileLock, Timeout

from omnigent.host import git_worktree as gw

ATTEMPT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
RECEIPT_VERSION = 1
LOCK_TIMEOUT_S: float = 10.0
_MAX_LISTED_FILES = 10

PHASE_RESERVED = "reserved"
PHASE_BRANCH_CREATED = "branch_created"
PHASE_READY = "ready"

# Error codes surfaced as ``error_code`` on host.create_worktree_result.
IN_PROGRESS = "worktree_creation_in_progress"
INCOMPLETE = "worktree_creation_incomplete"
ATTEMPT_CONFLICT = "worktree_attempt_conflict"
ATTEMPT_MISMATCH = "worktree_attempt_mismatch"
NEEDS_ATTENTION = "worktree_needs_attention"
ATTEMPT_REMOVED = "worktree_attempt_removed"


class WorktreeAttemptError(gw.WorktreeError):
    """An attempt-mode failure carrying a stable ``code`` for the API.

    :param code: One of the module-level error codes, e.g. ``INCOMPLETE``.
    :param message: User-facing message naming the path/branch involved.
    :param resume_started: Whether this request was resuming a partial attempt.
    """

    def __init__(self, code: str, message: str, *, resume_started: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.resume_started = resume_started


@dataclass
class Receipt:
    """Durable record of one creation attempt (JSON on disk)."""

    attempt_id: str
    phase: str
    repo_root: str
    branch: str
    base_branch: str | None
    existing_branch: bool
    relative_directory: str
    worktree_path: str
    base_commit: str
    start_commit: str | None = None
    created_at: int = field(default_factory=lambda: int(time.time()))
    completed_at: int | None = None
    version: int = RECEIPT_VERSION

    @property
    def state(self) -> str:
        """External readiness: ``ready`` or ``partial``."""
        return "ready" if self.phase == PHASE_READY else "partial"

    def creation(self) -> dict[str, object]:
        """The ``creation`` object attached to worktree listings."""
        return {
            "attempt_id": self.attempt_id,
            "state": self.state,
            "branch": self.branch,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


@dataclass
class AttemptResult:
    """Successful attempt-mode creation."""

    worktree: gw.CreatedWorktree
    outcome: str  # "created" | "resumed" | "existing"
    receipt: Receipt
    resume_started: bool

    def creation(self) -> dict[str, object]:
        """The ``creation`` receipt carried on the result frame."""
        return {
            "attempt_id": self.receipt.attempt_id,
            "state": self.receipt.state,
            "outcome": self.outcome,
            "worktree_path": self.worktree.worktree_path,
            "branch": self.worktree.branch,
            "workspace": self.worktree.workspace,
            "resume_started": self.resume_started,
        }


# --------------------------------------------------------------------------- store


def _common_dir(repo_root: str) -> Path:
    result = gw._run_git(["rev-parse", "--git-common-dir"], cwd=repo_root)
    if result.returncode != 0:
        raise gw._git_error("could not resolve git common dir", result)
    common = Path(result.stdout.strip())
    if not common.is_absolute():
        common = (Path(repo_root) / common).resolve()
    return common


class ReceiptStore:
    """Receipt files plus the per-repository creation lock."""

    def __init__(self, common_dir: Path) -> None:
        self.base = common_dir / "omnigent"
        self.dir = self.base / "worktree-attempts"

    @contextmanager
    def lock(self, timeout: float | None = None) -> Iterator[None]:
        """Hold the repository lock; the OS releases it if the host dies."""
        self.dir.mkdir(parents=True, exist_ok=True)
        wait = LOCK_TIMEOUT_S if timeout is None else timeout
        lock = FileLock(str(self.base / "worktree-attempts.lock"), timeout=wait)
        try:
            lock.acquire()
        except Timeout as exc:
            raise WorktreeAttemptError(
                IN_PROGRESS,
                "another worktree creation in this repository is still running; "
                "nothing was changed. Retry with the same attempt_id shortly.",
            ) from exc
        try:
            yield
        finally:
            lock.release()

    def path(self, attempt_id: str) -> Path:
        return self.dir / f"{attempt_id}.json"

    def load(self, attempt_id: str) -> Receipt | None:
        return _read_receipt(self.path(attempt_id))

    def load_all(self) -> list[Receipt]:
        if not self.dir.is_dir():
            return []
        receipts = []
        for entry in sorted(self.dir.glob("*.json")):
            receipt = _read_receipt(entry)
            if receipt is not None:
                receipts.append(receipt)
        return receipts

    def write(self, receipt: Receipt) -> None:
        """Atomically persist ``receipt`` (temp file, fsync, replace, fsync dir)."""
        self.dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".tmp-", suffix=".json")
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(asdict(receipt), handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path(receipt.attempt_id))
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        if os.name == "posix":
            dir_fd = os.open(self.dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)


def _read_receipt(path: Path) -> Receipt | None:
    try:
        data = json.loads(path.read_text())
        return Receipt(**data)
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError):
        gw._logger.warning("ignoring unreadable worktree attempt receipt %s", path)
        return None


def store_for_repo(repo_root: str) -> ReceiptStore:
    """Receipt store of the repository whose main work tree is ``repo_root``."""
    return ReceiptStore(_common_dir(repo_root))


# --------------------------------------------------------------------------- helpers


def _same_path(a: str, b: str) -> bool:
    return os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))


def _rev(repo: str, revision: str) -> str | None:
    result = gw._run_git(
        ["rev-parse", "--verify", "--quiet", "--end-of-options", f"{revision}^{{commit}}"],
        cwd=repo,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _check(result: object, label: str) -> None:
    if result.returncode != 0:  # type: ignore[attr-defined]
        raise gw._git_error(label, result)  # type: ignore[arg-type]


def _validate_directory(repo_root: str, commit: str, relative_directory: str) -> None:
    if not relative_directory:
        return
    probe = gw._run_git(
        ["cat-file", "-t", f"{commit}:{relative_directory.rstrip('/')}"], cwd=repo_root
    )
    if probe.returncode != 0 or probe.stdout.strip() != "tree":
        raise gw.WorktreeError(
            f"selected directory {relative_directory!r} is not a directory in the "
            "worktree's starting commit; choose another directory or base branch"
        )


def _pick_path(repo_root: str, branch: str, reserved: set[str]) -> Path:
    root = Path(repo_root)
    base_dir = root.parent / f"{root.name}-worktrees"
    name = gw._sanitize_dirname(branch)
    for suffix in range(1, gw._MAX_DIR_COLLISION_SUFFIX + 1):
        candidate = base_dir / (name if suffix == 1 else f"{name}-{suffix}")
        key = os.path.normcase(os.path.realpath(candidate))
        if not candidate.exists() and key not in reserved:
            return candidate
    raise gw.WorktreeError(
        f"could not find a free worktree directory under {base_dir} "
        f"after {gw._MAX_DIR_COLLISION_SUFFIX} attempts"
    )


def _lines(repo: str, args: list[str]) -> list[str]:
    result = gw._run_git(args, cwd=repo)
    _check(result, f"git {args[0]} failed")
    return [line for line in result.stdout.split("\0" if "-z" in args else "\n") if line]


def _attention(
    receipt: Receipt, reason: str, files: list[str] | None = None
) -> WorktreeAttemptError:
    detail = ""
    if files:
        shown = ", ".join(files[:_MAX_LISTED_FILES])
        more = (
            f" (+{len(files) - _MAX_LISTED_FILES} more)" if len(files) > _MAX_LISTED_FILES else ""
        )
        detail = f"; files: {shown}{more}"
    return WorktreeAttemptError(
        NEEDS_ATTENTION,
        f"the partial worktree at {receipt.worktree_path} (branch {receipt.branch!r}, "
        f"attempt {receipt.attempt_id}) can't be finished safely: {reason}{detail}. "
        "Nothing was changed.",
        resume_started=True,
    )


def _result(receipt: Receipt, outcome: str, resume_started: bool) -> AttemptResult:
    return AttemptResult(
        worktree=gw.CreatedWorktree(
            worktree_path=receipt.worktree_path,
            branch=receipt.branch,
            workspace=str(Path(receipt.worktree_path) / receipt.relative_directory),
        ),
        outcome=outcome,
        receipt=receipt,
        resume_started=resume_started,
    )


# --------------------------------------------------------------------------- entry point


def create_worktree_attempt(
    *,
    repo_path: str,
    branch_name: str,
    attempt_id: str,
    base_branch: str | None = None,
    existing_branch: bool = False,
) -> AttemptResult:
    """Create, resume, or return the worktree owned by ``attempt_id``.

    :raises WorktreeAttemptError: For the coded attempt outcomes.
    :raises WorktreeError: For request validation failures (no receipt written).
    """
    if not ATTEMPT_ID_RE.match(attempt_id):
        raise gw.WorktreeError("attempt_id must match ^[A-Za-z0-9_-]{16,128}$")
    gw.validate_branch_name(branch_name)
    if existing_branch and base_branch is not None:
        raise gw.WorktreeError("base_branch cannot be set when checking out an existing branch")
    repo_root = gw._main_work_tree(repo_path)
    prefix = gw._run_git(["rev-parse", "--show-prefix"], cwd=repo_path)
    _check(prefix, "could not resolve selected repository directory")
    relative_directory = prefix.stdout.rstrip("\n")
    store = store_for_repo(repo_root)
    with store.lock():
        receipt = store.load(attempt_id)
        if receipt is None:
            receipt = _reserve(
                store,
                attempt_id=attempt_id,
                repo_root=repo_root,
                branch=branch_name,
                base_branch=base_branch,
                existing_branch=existing_branch,
                relative_directory=relative_directory,
            )
            return _advance(store, receipt, resume_started=False)
        requested = (repo_root, branch_name, base_branch, existing_branch, relative_directory)
        recorded = (
            receipt.repo_root,
            receipt.branch,
            receipt.base_branch,
            receipt.existing_branch,
            receipt.relative_directory,
        )
        if not _same_path(requested[0], recorded[0]) or requested[1:] != recorded[1:]:
            raise WorktreeAttemptError(
                ATTEMPT_MISMATCH,
                f"attempt {attempt_id} was created for branch {receipt.branch!r} "
                f"(base {receipt.base_branch!r}, existing_branch={receipt.existing_branch}, "
                f"directory {receipt.relative_directory!r}) in {receipt.repo_root}; "
                "this request differs. Use a new attempt_id for a different worktree.",
            )
        if receipt.phase == PHASE_READY:
            return _ready(receipt)
        return _advance(store, receipt, resume_started=True)


def _reserve(
    store: ReceiptStore,
    *,
    attempt_id: str,
    repo_root: str,
    branch: str,
    base_branch: str | None,
    existing_branch: bool,
    relative_directory: str,
) -> Receipt:
    others = store.load_all()
    if existing_branch:
        if not gw._local_branch_exists(repo_root, branch):
            raise gw.WorktreeError(
                f"branch {branch!r} does not exist; cannot recreate its worktree"
            )
        # Drop registrations whose directories are gone (live worktrees untouched).
        gw._run_git(["worktree", "prune"], cwd=repo_root)
        live = next(
            (wt for wt in gw.list_worktrees(repo_path=repo_root) if wt.branch == branch), None
        )
        if live is not None:
            raise WorktreeAttemptError(
                ATTEMPT_CONFLICT,
                f"branch {branch!r} is already checked out at {live.path}; it is not adopted",
            )
    elif gw._local_branch_exists(repo_root, branch):
        raise WorktreeAttemptError(
            ATTEMPT_CONFLICT,
            f"a branch named {branch!r} already exists and no receipt for this attempt "
            "owns it; it is not adopted. Choose a different branch name.",
        )
    for other in others:
        if other.branch == branch and other.phase != PHASE_READY:
            raise WorktreeAttemptError(
                ATTEMPT_CONFLICT,
                f"branch {branch!r} is reserved by another creation attempt "
                f"({other.attempt_id}); it is not adopted",
            )
    if base_branch is not None:
        gw._ensure_base_resolvable(repo_root, base_branch)
    revision = f"refs/heads/{branch}" if existing_branch else (base_branch or "HEAD")
    base_commit = _rev(repo_root, revision)
    if base_commit is None:
        raise gw.WorktreeError(f"could not resolve worktree revision {revision!r}")
    _validate_directory(repo_root, base_commit, relative_directory)
    reserved = {os.path.normcase(os.path.realpath(o.worktree_path)) for o in others}
    path = _pick_path(repo_root, branch, reserved)
    receipt = Receipt(
        attempt_id=attempt_id,
        phase=PHASE_RESERVED,
        repo_root=repo_root,
        branch=branch,
        base_branch=base_branch,
        existing_branch=existing_branch,
        relative_directory=relative_directory,
        worktree_path=str(path),
        base_commit=base_commit,
    )
    store.write(receipt)
    return receipt


def _ready(receipt: Receipt) -> AttemptResult:
    entry = _entry_at(receipt)
    if entry is None or entry.branch != receipt.branch or not Path(receipt.worktree_path).is_dir():
        raise WorktreeAttemptError(
            ATTEMPT_REMOVED,
            f"attempt {receipt.attempt_id} finished creating {receipt.worktree_path} "
            f"(branch {receipt.branch!r}), but that worktree was removed later; "
            "it is not recreated automatically",
        )
    # Later edits and commits are the user's work: no tip or clean checks.
    return _result(receipt, "existing", resume_started=False)


def _entry_at(receipt: Receipt) -> gw.WorktreeInfo | None:
    for wt in gw.list_worktrees(repo_path=receipt.repo_root, attach_creation=False):
        if _same_path(wt.path, receipt.worktree_path):
            return wt
    return None


def _advance(store: ReceiptStore, receipt: Receipt, *, resume_started: bool) -> AttemptResult:
    try:
        return _advance_steps(store, receipt, resume_started=resume_started)
    except WorktreeAttemptError as exc:
        exc.resume_started = exc.resume_started or resume_started
        raise
    except gw.WorktreeError as exc:
        if resume_started:
            message = (
                f"resuming the partial worktree from attempt {receipt.attempt_id} did not "
                f"finish: {exc.message}; nothing was deleted. Retry with the same attempt_id."
            )
        else:
            message = (
                f"{exc.message} while creating {receipt.worktree_path}; the partial worktree "
                "and branch are kept and a retry with the same attempt_id resumes it"
            )
        raise WorktreeAttemptError(INCOMPLETE, message, resume_started=resume_started) from exc


def _advance_steps(
    store: ReceiptStore, receipt: Receipt, *, resume_started: bool
) -> AttemptResult:
    repo = receipt.repo_root
    ref = f"refs/heads/{receipt.branch}"
    if receipt.phase == PHASE_RESERVED:
        tip = _rev(repo, ref)
        if receipt.existing_branch:
            if tip != receipt.base_commit:
                raise _attention(receipt, f"branch {receipt.branch!r} moved since it was reserved")
        elif tip is None:
            start = receipt.base_branch or receipt.base_commit
            _check(
                gw._run_git(["branch", "--end-of-options", receipt.branch, start], cwd=repo),
                "git branch failed",
            )
            tip = _rev(repo, ref)
            if tip is None:
                raise gw.WorktreeError(f"branch {receipt.branch!r} was not created")
        elif tip != receipt.base_commit:
            # Only a branch that holds nothing beyond our base can be ours to adopt.
            raise _attention(
                receipt, f"branch {receipt.branch!r} points at {tip}, not the reserved base"
            )
        if tip is None:
            raise gw.WorktreeError(f"branch {receipt.branch!r} does not exist")
        _validate_directory(repo, tip, receipt.relative_directory)
        receipt.start_commit = tip
        receipt.phase = PHASE_BRANCH_CREATED
        store.write(receipt)
    assert receipt.start_commit is not None
    tip = _rev(repo, ref)
    if tip is None:
        _check(
            gw._run_git(
                ["branch", "--end-of-options", receipt.branch, receipt.start_commit], cwd=repo
            ),
            "git branch failed",
        )
    elif tip != receipt.start_commit:
        raise _attention(receipt, f"branch {receipt.branch!r} moved to {tip}")

    worktrees = gw.list_worktrees(repo_path=repo, attach_creation=False)
    ours = next((wt for wt in worktrees if _same_path(wt.path, receipt.worktree_path)), None)
    foreign = next(
        (
            wt
            for wt in worktrees
            if wt.branch == receipt.branch and not _same_path(wt.path, receipt.worktree_path)
        ),
        None,
    )
    if foreign is not None:
        raise WorktreeAttemptError(
            ATTEMPT_CONFLICT,
            f"branch {receipt.branch!r} is checked out at {foreign.path}, not at this "
            f"attempt's path {receipt.worktree_path}; it is not adopted",
        )
    path = Path(receipt.worktree_path)
    if ours is not None and not path.is_dir():
        # Registration whose directory is gone: unlock git's own init lock, then prune.
        if ours.locked == "initializing":
            gw._run_git(["worktree", "unlock", str(path)], cwd=repo)
        gw._run_git(["worktree", "prune"], cwd=repo)
        ours = None
    if ours is None:
        if path.exists() and (not path.is_dir() or any(path.iterdir())):
            raise WorktreeAttemptError(
                ATTEMPT_CONFLICT,
                f"{path} already exists and is not a worktree registered to this attempt; "
                "it is not adopted",
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        _check(
            gw._run_git(
                ["worktree", "add", str(path), "--end-of-options", receipt.branch], cwd=repo
            ),
            "git worktree add failed",
        )
        _verify_head(receipt)
        return _finish(store, receipt, "resumed" if resume_started else "created", resume_started)
    if ours.detached or ours.branch != receipt.branch:
        raise _attention(
            receipt, f"the registration at that path has {ours.branch or 'a detached HEAD'}"
        )
    _complete_partial(receipt, locked=ours.locked)
    return _finish(store, receipt, "resumed", True)


def _verify_head(receipt: Receipt) -> None:
    head = _rev(receipt.worktree_path, "HEAD")
    if head != receipt.start_commit:
        raise _attention(receipt, f"HEAD is {head}, expected {receipt.start_commit}")


def _finish(
    store: ReceiptStore, receipt: Receipt, outcome: str, resume_started: bool
) -> AttemptResult:
    workspace = Path(receipt.worktree_path) / receipt.relative_directory
    if not workspace.is_dir():
        raise _attention(receipt, f"selected directory {receipt.relative_directory!r} is missing")
    receipt.phase = PHASE_READY
    receipt.completed_at = int(time.time())
    store.write(receipt)
    return _result(receipt, outcome, resume_started)


def _dirty_files(worktree: str) -> tuple[list[str], list[str]]:
    """Return (changed-or-untracked files, missing files) relative to the index."""
    gw._run_git(["update-index", "-q", "--refresh"], cwd=worktree)
    changed: list[str] = []
    missing: list[str] = []
    for line in _lines(worktree, ["diff-files", "--name-status", "--no-renames"]):
        status, _, name = line.partition("\t")
        (missing if status == "D" else changed).append(name)
    changed += _lines(worktree, ["ls-files", "--others", "--exclude-standard", "-z"])
    return changed, missing


def _fill_missing_files(worktree: str) -> None:
    """Write only missing tracked files; never ``--force`` (existing files are kept)."""
    # --quiet silences "already exists" skips (still exit 1); other stderr is a real failure.
    result = gw._run_git(["checkout-index", "--all", "--quiet"], cwd=worktree)
    if result.returncode != 0 and result.stderr.strip():
        raise gw._git_error("git checkout-index failed", result)


def _complete_partial(receipt: Receipt, *, locked: str | None) -> None:
    """Finish a registered, partly checked-out worktree without overwriting anything."""
    worktree = receipt.worktree_path
    git_dir_result = gw._run_git(["rev-parse", "--absolute-git-dir"], cwd=worktree)
    _check(git_dir_result, "could not resolve worktree git dir")
    git_dir = Path(git_dir_result.stdout.strip())
    if (git_dir / "index.lock").exists():
        raise _attention(
            receipt, "an index.lock is present, so a git process may still be writing"
        )
    _verify_head(receipt)
    if not (git_dir / "index").exists():
        # Only the index changes; working-tree files are untouched.
        _check(gw._run_git(["read-tree", "HEAD"], cwd=worktree), "git read-tree failed")
    changed, missing = _dirty_files(worktree)
    if changed:
        raise _attention(receipt, "files differ from the branch", changed)
    if missing:
        _fill_missing_files(worktree)
    # Re-verify under the lock: anything edited after inspection is reported, never rewritten.
    changed, missing = _dirty_files(worktree)
    if changed or missing:
        raise _attention(
            receipt, "files changed while the checkout was being finished", changed + missing
        )
    _verify_head(receipt)
    if locked == "initializing":
        gw._run_git(["worktree", "unlock", worktree], cwd=receipt.repo_root)


def attach_creation(repo_root: str, worktrees: list[gw.WorktreeInfo]) -> None:
    """Attach each receipt's ``creation`` to the listing row at its path."""
    try:
        receipts = store_for_repo(repo_root).load_all()
    except gw.WorktreeError:
        return
    for receipt in receipts:
        for wt in worktrees:
            if _same_path(wt.path, receipt.worktree_path):
                wt.creation = receipt.creation()
                break
