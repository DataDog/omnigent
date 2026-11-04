# Session git worktrees

`POST /v1/sessions` and `POST /v1/hosts/{id}/runners` accept a `git` block.
In **create** mode the server asks the host (`host.create_worktree`) to make a
sibling worktree at `<repo>-worktrees/<branch>` and runs the session there. In
**bind** mode (`existing_worktree: true`) the workspace already is a worktree.
The host runs git; the server never does.

This document covers idempotent, resumable creation via `attempt_id`.

## Why

A create on a large repository can exceed the host's 120 s git timeout (or
the server's 150 s wait). A timed-out `git worktree add` leaves a branch, a
registration locked `initializing`, and/or a half-filled directory, with no
session and no record of who made them. A plain retry then fails with
"branch already exists". The client needs a way to say "this is the same
create as before" and to get proof that what it finds is its own.

## Contract (v1)

### Discovery

- `GET /v1/info` → `worktree_creation_attempts: true`. Absent/false means the
  server ignores `attempt_id`; stay on the legacy path.
- Hosts advertise the hello capability `git_worktree_attempts`. Clients never
  see it; a host without it yields `412 worktree_attempts_unsupported`.

### Request

```json
"git": { "branch_name": "vitrine/run-abc", "base_branch": "main", "attempt_id": "3f6c1d1e-…" }
```

- `attempt_id` matches `^[A-Za-z0-9_-]{16,128}$`; create mode only (422 with
  `existing_worktree`); allowed with `existing_branch`.
- Generate once per run, persist before the first POST, reuse on every retry.
  A new id for the same branch is a different owner.
- Omitted → exactly today's behavior (errors, rollback).

### Success

`SessionResponse.git_worktree` (also `git_worktree` on the runners response):

```json
{ "attempt_id": "…", "state": "ready", "outcome": "created|resumed|existing",
  "worktree_path": "…", "branch": "…", "workspace": "…", "session_reused": false }
```

- `created`: made by this request. `resumed`: this request finished a partial
  creation from the same attempt. `existing`: an earlier request finished it;
  returned as-is (later edits and commits are the user's work).
- `session_reused: true`: an earlier request with this attempt also created the
  session (dropped reply). The same live session — caller-owned, same host,
  label `omnigent.git.worktree_attempt=<attempt_id>` — is returned;
  `initial_items` are not replayed.
- `null` on reads and legacy creates.

### Listing

`GET /v1/hosts/{id}/worktrees` entries may carry
`creation: {attempt_id, state: "ready"|"partial", branch, created_at, completed_at}`.
Absent when no receipt owns the path. A branch-only or reserved attempt has no
listing row; it is still recoverable by a same-id POST.

### Errors

| code | HTTP | retry same id? | meaning |
|---|---|---|---|
| `worktree_creation_in_progress` | 409 | yes, back off | another creation in this repo holds the host lock; nothing changed |
| `worktree_creation_incomplete` | 409 | yes | a git step timed out/failed, or the host didn't confirm in 150 s; residue kept, next retry resumes |
| `worktree_attempt_conflict` | 409 | no | branch/path exists without a matching receipt, or another attempt owns it; never adopted |
| `worktree_attempt_mismatch` | 409 | no | same id, different repo/branch/base/mode/subdirectory |
| `worktree_needs_attention` | 409 | no | a partial from this attempt can't be proven safe to finish; nothing touched; message lists up to 10 files |
| `worktree_attempt_removed` | 410 | no | finished earlier, worktree removed since; not recreated silently (both create routes) |
| `worktree_not_ready` | 409 | no | bind target is a partial attempt worktree |
| `worktree_attempts_unsupported` | 412 | no | host predates the capability; retry without `attempt_id` or upgrade |

## Host design

### Receipts

`<git-common-dir>/omnigent/worktree-attempts/<attempt_id>.json`, written
atomically (temp file, fsync, `os.replace`, fsync dir). It lives in the
repository, so it survives host restarts and reinstalls, and git ignores it.
Fields: `attempt_id`, `phase` (`reserved` → `branch_created` → `ready`),
`repo_root`, `branch`, `base_branch`, `existing_branch`, `relative_directory`,
`worktree_path` (fixed at reservation), `base_commit`, `start_commit`,
`created_at`, `completed_at`. Only `ready` reports as ready.

### Lock

`filelock.FileLock` on `<common-dir>/omnigent/worktree-attempts.lock` covers
every attempt-mode create and resume, including the final re-verification.
Waiting more than 10 s → `worktree_creation_in_progress`. The OS releases it
when the host dies.

### Fresh attempt

Refuse (`conflict`) if the branch exists, another non-ready receipt reserves
the branch, or the path is taken/reserved. Write `reserved`; `git branch
<branch> <base>`; record the tip as `start_commit` (`branch_created`);
`git worktree add <path> <branch>`; verify HEAD and the subdirectory; write
`ready`.

### Resume

- **ready**: registration with our branch still at `worktree_path` →
  `existing`; otherwise `removed`.
- **branch**: in `reserved`, an existing branch is adopted only if its tip is
  exactly `base_commit` (it holds no work). In `branch_created` the tip must
  equal `start_commit`. A checkout of the branch elsewhere → `conflict`.
- **registration**: none and the path missing/empty → redo the add; none and
  the path non-empty → `conflict`; registered but directory gone → unlock
  git's own `initializing` lock, `git worktree prune`, redo the add;
  registered with another branch/detached → `needs_attention`.
- **partial checkout**: an `index.lock` → `needs_attention` (never deleted).
  If the index is absent, `git read-tree HEAD` (index only). Any modified or
  untracked file → `needs_attention`. Otherwise `git checkout-index --all`
  (no `--force`: it never overwrites an existing file) fills only missing
  files, then the tree is **re-verified under the same lock**. A file the user
  created or edited after inspection is reported, never rewritten.

Attempt mode never runs `reset`, `clean`, `checkout --force`, `branch -D`,
`worktree remove`, or deletes files or locks. The server skips create
rollback for attempt creates; the ready worktree stays for the retry.

### Timeouts

Every git call runs in its own process group (POSIX). On timeout the group
gets SIGTERM, then SIGKILL after 5 s, so no checkout grandchild keeps writing
after a timeout is reported. `OMNIGENT_HOST_GIT_TIMEOUT_S` overrides the
120 s default.

## Server design

- Per-process `asyncio.Lock` keyed by `(host_id, attempt_id)` spans worktree
  creation, session creation and launch on `POST /v1/sessions`.
- On `outcome: existing` the server looks up a live session in that exact
  workspace with the attempt label, owned by the caller, and returns it.
- Bind backstop: with `existing_worktree`, a capable host lists worktrees;
  the longest containing entry with `creation.state == "partial"` →
  `worktree_not_ready`. No receipt, an old host, or a listing failure keeps
  today's behavior.

## Rollout and compatibility

Server and hosts deploy independently.

- New server + old host → 412; client falls back to legacy or asks for an
  upgrade.
- Old server → ignores `attempt_id`, no `git_worktree`, no info flag; client
  stays legacy (gate on `/v1/info.worktree_creation_attempts`).
- New host + old server → no attempt id is ever sent; nothing changes.
- Legacy requests (no `attempt_id`) are unchanged; responses gain only a
  `git_worktree: null` field.

## Known limits

- **Session dedupe is per server replica.** The attempt lock is in-process.
  Two replicas racing the *first* create of one attempt can each create a
  session (the host lock and receipt still guarantee one worktree; the second
  sees `existing`). Once one session exists, later retries on any replica find
  it via the label. A cross-replica fix needs a DB uniqueness constraint on
  the attempt label.
- Receipts are not garbage-collected (a few hundred bytes each).
- A resume via `checkout-index` does not run the `post-checkout` hook.

## Live end-to-end check

`uv run python dev/worktree_attempts_e2e.py [--root DIR] [--port N]` builds an
isolated server + host + slow-checkout repository and checks: timeout →
`incomplete` → host restart → `resumed`; a repeat → `existing` with
`session_reused`; a host-only SIGKILL mid-checkout → restart → `resumed` with
no late writes from the orphaned git group; a pre-existing branch → `conflict`.
It stops only the process groups it started.
