"""Live end-to-end check for attempt-mode git worktree creation.

Builds a fully isolated stack (own config/data dirs, own server port, own host
identity), a scratch repository whose checkout is slow, and drives
``POST /v1/sessions`` with ``git.attempt_id`` through:

1. host git timeout -> 409 worktree_creation_incomplete -> host restart -> resumed
2. same attempt again -> existing + session_reused
3. host-only SIGKILL mid-checkout -> restart -> resumed, no orphan writes
4. pre-existing branch under a new attempt -> 409 worktree_attempt_conflict

Only processes this script started (their own process groups) are stopped.

Usage: uv run python dev/worktree_attempts_e2e.py [--root /tmp/omni-wt-e2e] [--port 6871]
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

_GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]


def _http(
    method: str, url: str, body: dict | None = None, timeout: float = 400
) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


class Stack:
    """An isolated server + host pair; every process runs in its own group."""

    def __init__(self, root: Path, port: int) -> None:
        self.root = root
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        self.env = {
            **os.environ,
            "OMNIGENT_SKIP_WEB_UI": "true",
            "OMNIGENT_CONFIG_HOME": str(root / "home"),
            "OMNIGENT_DATA_DIR": str(root / "data"),
        }
        self.env.pop("OMNIGENT_HOST_GIT_TIMEOUT_S", None)
        self.server: subprocess.Popen[bytes] | None = None
        self.host: subprocess.Popen[bytes] | None = None
        self.sessions: set[str] = set()

    def _spawn(self, argv: list[str], log: str, env: dict[str, str]) -> subprocess.Popen[bytes]:
        out = open(self.root / log, "ab")  # noqa: SIM115 — lives as long as the process
        return subprocess.Popen(argv, env=env, stdout=out, stderr=out, start_new_session=True)

    def start_server(self) -> None:
        self.server = self._spawn(
            [
                sys.executable,
                "-m",
                "omnigent.cli",
                "server",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--database-uri",
                f"sqlite:///{self.root / 'data' / 'chat.db'}",
                "--artifact-location",
                str(self.root / "data" / "artifacts"),
            ],
            "server.log",
            self.env,
        )
        _wait(lambda: _http("GET", f"{self.url}/health", timeout=2)[0] == 200, 90, "server")

    def start_host(self, git_timeout_s: float | None = None) -> None:
        env = dict(self.env)
        if git_timeout_s is not None:
            env["OMNIGENT_HOST_GIT_TIMEOUT_S"] = str(git_timeout_s)
        self.host = self._spawn(
            [sys.executable, "-m", "omnigent.cli", "host", self.url, "--no-open"], "host.log", env
        )
        _wait(lambda: self.host_id() is not None, 90, "host")

    def host_id(self) -> str | None:
        status, body = _http("GET", f"{self.url}/v1/hosts", timeout=5)
        online = (
            [h for h in body.get("hosts", []) if h.get("status") == "online"]
            if status == 200
            else []
        )
        return online[0]["host_id"] if online else None

    def stop_host(self, sig: int = signal.SIGTERM, host_only: bool = False) -> None:
        if self.host is None:
            return
        if host_only:
            self.host.send_signal(sig)  # git groups the host started keep running
        else:
            _killpg(self.host.pid, sig)
        self.host.wait(timeout=30)
        self.host = None
        _wait(lambda: self.host_id() is None, 30, "host offline")

    def teardown(self) -> None:
        # Deleting sessions first lets the runner close the terminals it spawned.
        for session_id in self.sessions:
            with contextlib.suppress(OSError, ValueError):
                _http("DELETE", f"{self.url}/v1/sessions/{session_id}", timeout=30)
        for proc in (self.host, self.server):
            if proc is not None and proc.poll() is None:
                _killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    _killpg(proc.pid, signal.SIGKILL)


def _killpg(pid: int, sig: int) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pid, sig)


def _wait(pred, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if pred():
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    raise SystemExit(f"timed out waiting for {what}")


def make_repo(root: Path, files: int) -> Path:
    repo = root / "repo"
    (repo / "src").mkdir(parents=True)
    for i in range(files):
        (repo / "src" / f"f{i}.txt").write_text(f"file {i}\n")
    (repo / ".gitattributes").write_text("src/*.txt filter=slow\n")
    subprocess.run([*_GIT, "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run([*_GIT, "config", "filter.slow.clean", "cat"], cwd=repo, check=True)
    subprocess.run(
        [*_GIT, "config", "filter.slow.smudge", "sh -c 'sleep 0.02; cat'"], cwd=repo, check=True
    )
    subprocess.run([*_GIT, "add", "-A"], cwd=repo, check=True)
    subprocess.run([*_GIT, "commit", "-q", "-m", "init"], cwd=repo, check=True)
    return repo.resolve()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/tmp/omni-wt-e2e")
    parser.add_argument("--port", type=int, default=6871)
    parser.add_argument("--files", type=int, default=400)
    args = parser.parse_args()
    root = Path(args.root)
    if root.exists():
        shutil.rmtree(root)
    (root / "home").mkdir(parents=True)
    (root / "data").mkdir()
    repo = make_repo(root, args.files)
    stack = Stack(root, args.port)
    results: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str) -> None:
        results.append((name, ok, detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)

    try:
        stack.start_server()
        stack.start_host(git_timeout_s=3)
        host_id = stack.host_id()
        _, agents = _http("GET", f"{stack.url}/v1/agents?limit=200")
        agent_id = next(a["id"] for a in agents["data"] if a["name"] == "debby")
        print(f"server={stack.url} host_id={host_id} repo={repo}", flush=True)

        def post(attempt: str, branch: str) -> tuple[int, dict]:
            status, body = _http(
                "POST",
                f"{stack.url}/v1/sessions",
                {
                    "agent_id": agent_id,
                    "host_id": stack.host_id() or host_id,
                    "workspace": str(repo),
                    "git": {"branch_name": branch, "attempt_id": attempt},
                },
            )
            if isinstance(body.get("id"), str):
                stack.sessions.add(body["id"])
            return status, body

        def receipt(attempt: str) -> dict:
            return json.loads(
                (repo / ".git/omnigent/worktree-attempts" / f"{attempt}.json").read_text()
            )

        a1 = "e2e-attempt-00000001"
        status, body = post(a1, "e2e/timeout")
        code = body.get("error", {}).get("code")
        check(
            "timeout -> incomplete",
            status == 409 and code == "worktree_creation_incomplete",
            f"HTTP {status} {code}; receipt phase={receipt(a1)['phase']}",
        )

        stack.stop_host()
        stack.start_host()
        status, body = post(a1, "e2e/timeout")
        wt = body.get("git_worktree") or {}
        files = len(list((Path(wt.get("worktree_path", "/nonexistent")) / "src").glob("*.txt")))
        check(
            "restart + retry -> resumed",
            status == 201 and wt.get("outcome") == "resumed",
            f"HTTP {status} outcome={wt.get('outcome')} state={wt.get('state')} "
            f"files={files}/{args.files}",
        )
        first_session = body.get("id")

        status, body = post(a1, "e2e/timeout")
        wt = body.get("git_worktree") or {}
        check(
            "repeat -> existing + reused",
            status == 201
            and wt.get("outcome") == "existing"
            and wt.get("session_reused") is True
            and body.get("id") == first_session,
            f"HTTP {status} outcome={wt.get('outcome')} reused={wt.get('session_reused')} "
            f"same_session={body.get('id') == first_session}",
        )

        a2 = "e2e-attempt-00000002"
        crash_path = repo.parent / "repo-worktrees" / "e2e-crash"
        thread = threading.Thread(target=lambda: post(a2, "e2e/crash"), daemon=True)
        thread.start()
        _wait(lambda: len(list((crash_path / "src").glob("*.txt"))) > 20, 120, "partial checkout")
        stack.stop_host(signal.SIGKILL, host_only=True)
        before = len(list((crash_path / "src").glob("*.txt")))
        stack.start_host()
        status, body = post(a2, "e2e/crash")
        wt = body.get("git_worktree") or {}
        settled = len(list((crash_path / "src").glob("*.txt")))
        time.sleep(3)
        late = len(list((crash_path / "src").glob("*.txt"))) - settled
        check(
            "host-only SIGKILL + restart -> resumed, no orphan writes",
            status == 201 and wt.get("outcome") in ("resumed", "existing") and late == 0,
            f"HTTP {status} outcome={wt.get('outcome') or body.get('error', {}).get('code')} "
            f"files at kill={before} after={settled}/{args.files} late_writes={late}",
        )

        subprocess.run([*_GIT, "branch", "e2e/hand-made"], cwd=repo, check=True)
        status, body = post("e2e-attempt-00000003", "e2e/hand-made")
        code = body.get("error", {}).get("code")
        check(
            "pre-existing branch -> conflict",
            status == 409 and code == "worktree_attempt_conflict",
            f"HTTP {status} {code}",
        )
    finally:
        stack.teardown()
    time.sleep(3)
    leftovers = subprocess.run(
        ["ps", "-A", "-o", "pid=,command="], capture_output=True, text=True, check=False
    ).stdout
    markers = (f"127.0.0.1:{stack.port}", f"--port {stack.port}", str(root))
    leaked = [line for line in leftovers.splitlines() if any(m in line for m in markers)]
    check("teardown leaves no stack processes", not leaked, f"{len(leaked)} left")
    failed = [name for name, ok, _ in results if not ok]
    print(f"{len(results) - len(failed)}/{len(results)} passed", flush=True)
    return 1 if failed or not results else 0


if __name__ == "__main__":
    raise SystemExit(main())
