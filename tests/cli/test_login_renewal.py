"""Stored-login renewal: forced refresh, outcomes, concurrency, CLI retry.

A real local HTTP server stands in for the Omnigent server's ``/oauth/token``
and an authenticated API route, so the real cli_auth / chat / cli code paths
run end to end without mocking httpx.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import stat
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

import click
import httpx
import pytest

from omnigent import cli_auth
from omnigent.cli_auth import RefreshOutcome

REFRESH = "refresh-SECRET-abc"
OLD = "access-OLD-SECRET"


class _Stub:
    """Counts refresh exchanges; issues access-NEW-<n> tokens."""

    def __init__(self) -> None:
        self.refresh_hits = 0
        self.refresh_status = 200
        self.delay_s = 0.0
        self.valid: set[str] = set()
        self.lock = threading.Lock()


def _make_handler(stub: _Stub) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:  # quiet
            return

        def _send(self, status: int, payload: object) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            auth = self.headers.get("Authorization", "")
            return auth.removeprefix("Bearer ") in stub.valid

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            if self.path == "/oauth/token":
                form = parse_qs(raw.decode())
                time.sleep(stub.delay_s)
                with stub.lock:
                    stub.refresh_hits += 1
                    n = stub.refresh_hits
                    if stub.refresh_status != 200 or form.get("refresh_token") != [REFRESH]:
                        self._send(
                            stub.refresh_status if stub.refresh_status != 200 else 400,
                            {"error": "invalid_grant"},
                        )
                        return
                    token = f"access-NEW-{n}"
                    stub.valid.add(token)
                self._send(200, {"access_token": token, "expires_in": 3600})
                return
            if not self._authorized():
                self._send(401, {"detail": "Authentication required"})
                return
            self._send(200, {"echo_len": len(raw)})

        def do_GET(self) -> None:
            if not self._authorized():
                self._send(401, {"detail": "Authentication required"})
                return
            self._send(200, {"ok": True})

    return Handler


@pytest.fixture
def server() -> Iterator[tuple[str, _Stub]]:
    stub = _Stub()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(stub))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", stub
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.fixture
def token_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("OMNIGENT_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli_auth, "_token_file_path", lambda: tmp_path / "auth_tokens.json")
    monkeypatch.delenv("OMNIGENT_REMOTE_AUTH_TOKEN", raising=False)
    cli_auth._warned_expired_servers.clear()
    return tmp_path


def _store(url: str, *, token: str = OLD, expires_in: float = 3600, refresh: str | None = REFRESH):
    cli_auth.store_token(
        url, token=token, user_id="a@x", expires_at=time.time() + expires_in, refresh_token=refresh
    )


def _assert_no_secrets(*texts: str) -> None:
    blob = "\n".join(texts)
    assert REFRESH not in blob
    assert OLD not in blob
    assert "access-NEW-" not in blob


# --- cli_auth -----------------------------------------------------------


def test_rejected_token_forces_refresh_even_when_locally_fresh(token_dir, server, caplog):
    url, stub = server
    _store(url)
    caplog.set_level(logging.DEBUG)
    token, outcome = cli_auth.refresh_stored_token_with_outcome(url, rejected_token=OLD)
    assert (token, outcome) == ("access-NEW-1", RefreshOutcome.RENEWED)
    assert cli_auth.load_token(url) == "access-NEW-1"
    # Without rejected_token a fresh token is left alone.
    assert cli_auth.refresh_stored_token_with_outcome(url) == (
        "access-NEW-1",
        RefreshOutcome.ALREADY_FRESH,
    )
    assert stub.refresh_hits == 1
    _assert_no_secrets(caplog.text)


def test_already_renewed_by_other_process_skips_network(token_dir, server):
    url, stub = server
    _store(url, token="someone-elses-fresh")
    token, outcome = cli_auth.refresh_stored_token_with_outcome(url, rejected_token=OLD)
    assert (token, outcome) == ("someone-elses-fresh", RefreshOutcome.ALREADY_FRESH)
    assert stub.refresh_hits == 0


def test_outcomes(token_dir, server, caplog):
    url, stub = server
    _store(url, expires_in=-10, refresh=None)
    assert cli_auth.refresh_stored_token_with_outcome(url)[1] is RefreshOutcome.NO_REFRESH_MATERIAL
    _store(url, expires_in=-10, refresh="wrong")
    assert cli_auth.refresh_stored_token_with_outcome(url)[1] is RefreshOutcome.REFUSED
    assert "omnigent login" in caplog.text or "login" in caplog.text
    _store(url, expires_in=-10)
    stub.refresh_status = 404
    assert cli_auth.refresh_stored_token_with_outcome(url)[1] is RefreshOutcome.UNSUPPORTED
    _store("http://127.0.0.1:9", expires_in=-10)
    assert (
        cli_auth.refresh_stored_token_with_outcome("http://127.0.0.1:9", timeout=2)[1]
        is RefreshOutcome.UNREACHABLE
    )
    assert RefreshOutcome.REFUSED.needs_relogin
    assert not RefreshOutcome.UNREACHABLE.needs_relogin
    _assert_no_secrets(caplog.text)


def test_load_or_refresh_token(token_dir, server):
    url, stub = server
    _store(url)
    assert cli_auth.load_or_refresh_token(url) == OLD
    assert stub.refresh_hits == 0  # valid login: no exchange
    _store(url, expires_in=-10)
    assert cli_auth.load_or_refresh_token(url) == "access-NEW-1"


def test_lock_is_reentrant(token_dir):
    with cli_auth._token_file_lock(), cli_auth._token_file_lock():
        _store("http://a")  # takes the lock again: must not deadlock
    assert cli_auth.load_token("http://a") == OLD


def test_relogin_message_is_sanitized():
    msg = cli_auth.relogin_message("https://user:pw@ex.com/api?access_token=x")
    assert "pw" not in msg and "access_token" not in msg
    assert "login https://ex.com/api" in msg


# --- concurrency ----------------------------------------------------------


def _proc_refresh(data_dir: str, url: str, queue: multiprocessing.Queue) -> None:
    os.environ["OMNIGENT_DATA_DIR"] = data_dir
    from omnigent import cli_auth as ca

    ca._token_file_path = lambda: Path(data_dir) / "auth_tokens.json"  # type: ignore[assignment]
    queue.put(ca.refresh_stored_token(url, rejected_token=OLD))


def test_concurrent_processes_and_threads_exchange_once(token_dir, server):
    url, stub = server
    stub.delay_s = 0.3
    _store(url)
    _store("http://other", token="other-token")
    ctx = multiprocessing.get_context("spawn")
    queue: multiprocessing.Queue = ctx.Queue()
    procs = [
        ctx.Process(target=_proc_refresh, args=(str(token_dir), url, queue)) for _ in range(4)
    ]
    for p in procs:
        p.start()
    thread_results: list[str | None] = []
    threads = [
        threading.Thread(
            target=lambda: thread_results.append(
                cli_auth.refresh_stored_token(url, rejected_token=OLD)
            )
        )
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    results = thread_results + [queue.get(timeout=5) for _ in procs]
    assert stub.refresh_hits == 1
    assert set(results) == {"access-NEW-1"}
    path = token_dir / "auth_tokens.json"
    data = json.loads(path.read_text())
    assert data["http://other"]["token"] == "other-token"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_concurrent_store_and_refresh_lose_no_write(token_dir, server):
    url, stub = server
    stub.delay_s = 0.3
    _store(url, expires_in=-10)
    t = threading.Thread(target=cli_auth.refresh_stored_token, args=(url,))
    t.start()
    time.sleep(0.1)  # refresh holds the lock mid-exchange
    _store("http://second", token="second-token")
    t.join(10)
    data = json.loads((token_dir / "auth_tokens.json").read_text())
    assert data["http://second"]["token"] == "second-token"
    assert data[url]["token"] == "access-NEW-1"


# --- chat.py server client ----------------------------------------------


def test_expired_refreshable_login_gets_auth_and_renewed_bearer(token_dir, server):
    from omnigent.chat import _server_auth

    url, stub = server
    _store(url, expires_in=-10)
    auth = _server_auth(url, session_id=None)
    assert auth is not None
    with httpx.Client(base_url=url, auth=auth) as client:
        assert client.get("/v1/me").status_code == 200
    assert stub.refresh_hits == 1


def test_401_on_locally_valid_token_renews_and_replays_multipart(
    token_dir, server, capsys, caplog
):
    from omnigent.chat import _DatabricksTokenAuth

    url, stub = server
    _store(url)  # locally fresh, but the server does not accept OLD
    auth = _DatabricksTokenAuth(url)
    files = {"bundle": ("agent.tar.gz", b"x" * 2048, "application/gzip")}
    with httpx.Client(base_url=url, auth=auth) as client:
        resp = client.post("/v1/sessions", files=files)
    assert resp.status_code == 200
    assert resp.json()["echo_len"] > 2048  # body replayed in full
    assert stub.refresh_hits == 1
    out = capsys.readouterr()
    _assert_no_secrets(caplog.text, out.out, out.err)


async def test_async_client_renews_off_loop(token_dir, server):
    from omnigent.chat import _DatabricksTokenAuth

    url, stub = server
    _store(url)
    async with httpx.AsyncClient(base_url=url, auth=_DatabricksTokenAuth(url)) as client:
        assert (await client.get("/v1/me")).status_code == 200
    assert stub.refresh_hits == 1


def test_refused_grant_retries_at_most_once_then_relogin_hint(token_dir, server):
    from omnigent.chat import _DatabricksTokenAuth, relogin_hint_for

    url, stub = server
    _store(url, refresh="revoked")
    auth = _DatabricksTokenAuth(url)
    with httpx.Client(base_url=url, auth=auth) as client:
        assert client.get("/v1/me").status_code == 401
    assert stub.refresh_hits == 1
    hint = relogin_hint_for(url, auth)
    assert hint is not None and "login" in hint


def test_env_static_token_is_never_refreshed(token_dir, server, monkeypatch):
    from omnigent.chat import _DatabricksTokenAuth

    url, stub = server
    _store(url)
    monkeypatch.setenv("OMNIGENT_REMOTE_AUTH_TOKEN", "static-env")
    with httpx.Client(base_url=url, auth=_DatabricksTokenAuth(url)) as client:
        assert client.get("/v1/me").status_code == 401
    assert stub.refresh_hits == 0


def test_valid_login_makes_no_refresh_calls(token_dir, server):
    from omnigent.chat import _DatabricksTokenAuth, _remote_headers

    url, stub = server
    _store(url)
    stub.valid.add(OLD)
    with httpx.Client(base_url=url, auth=_DatabricksTokenAuth(url)) as client:
        for _ in range(3):
            assert client.get("/v1/me").status_code == 200
    assert _remote_headers(url, host_id=None)["Authorization"] == f"Bearer {OLD}"
    assert stub.refresh_hits == 0


async def test_session_start_401_raises_relogin_click_exception(token_dir, server, monkeypatch):
    """The real ``omni run`` session-start path surfaces the re-login message."""
    import contextlib

    import omnigent.host.daemon_launch as daemon_launch
    from omnigent.chat import _prepare_chat_session_via_daemon, _server_auth

    url, stub = server
    _store(url, expires_in=-10, refresh="revoked")

    @contextlib.asynccontextmanager
    async def _no_daemon(*_a, **_kw):
        yield None  # the daemon channel is not under test

    async def _host_online(*_a, **_kw):
        return None

    monkeypatch.setattr(daemon_launch, "open_daemon_client", _no_daemon)
    monkeypatch.setattr(daemon_launch, "wait_for_host_online", _host_online)
    with pytest.raises(click.ClickException) as info:
        await _prepare_chat_session_via_daemon(
            base_url=url,
            headers={},
            auth=_server_auth(url, session_id=None),
            host_id="host_x",
            bundle=b"x",
            resume_conversation_id=None,
            fork_session_id=None,
            workspace="/tmp",
        )
    assert "could not be renewed" in info.value.message
    assert f"Could not start a session on {url}" in info.value.message
    assert stub.refresh_hits == 1  # one refused exchange, not two


def test_expired_login_with_revoked_grant_spends_one_exchange(token_dir, server):
    from omnigent.chat import _DatabricksTokenAuth

    url, stub = server
    _store(url, expires_in=-10, refresh="revoked")
    auth = _DatabricksTokenAuth(url)
    with httpx.Client(base_url=url, auth=auth) as client:
        assert client.get("/v1/me").status_code == 401
    assert stub.refresh_hits == 1
    assert auth.last_refresh_outcome is RefreshOutcome.REFUSED


# --- cli.py management calls (host status) --------------------------------


def test_host_http_json_401_renews_retries_and_invalidates_cache(token_dir, server):
    import omnigent.cli as cli

    url, stub = server
    _store(url)
    cli._host_http_headers_cache.clear()
    result = cli._host_http_json(base_url=url, method="GET", path="/v1/hosts")
    assert result.status_code == 200
    assert stub.refresh_hits == 1
    assert cli._host_http_headers_cache[(url, None)]["Authorization"] == "Bearer access-NEW-1"
    # The renewed headers are reused; no further exchange.
    assert cli._host_http_json(base_url=url, method="GET", path="/v1/hosts").status_code == 200
    assert stub.refresh_hits == 1


def test_host_http_json_renewed_then_401_is_plain_error(token_dir, server, monkeypatch):
    import omnigent.cli as cli

    url, stub = server
    _store(url)
    cli._host_http_headers_cache.clear()
    real_refresh = cli_auth.refresh_stored_token_with_outcome

    def _renew_but_server_still_refuses(*a, **kw):
        token, outcome = real_refresh(*a, **kw)
        stub.valid.clear()  # e.g. the account was disabled meanwhile
        return token, outcome

    monkeypatch.setattr(
        cli_auth, "refresh_stored_token_with_outcome", _renew_but_server_still_refuses
    )
    result = cli._host_http_json(base_url=url, method="GET", path="/v1/hosts")
    assert result.status_code == 401
    assert "could not be renewed" not in str(result.body)
    assert stub.refresh_hits == 1


def test_host_http_json_refused_returns_relogin_text(token_dir, server):
    import omnigent.cli as cli

    url, _stub = server
    _store(url, expires_in=-10, refresh="revoked")
    cli._host_http_headers_cache.clear()
    result = cli._host_http_json(base_url=url, method="GET", path="/v1/hosts")
    assert result.status_code == 401
    assert isinstance(result.body, str) and "could not be renewed" in result.body
    _assert_no_secrets(result.body)


# --- runner callbacks (acp:vit sessions run in host-launched runners) ------


def test_runner_auth_401_invalidates_factory_and_force_renews(token_dir, server):
    from omnigent.runner._entry import _make_auth_token_factory, _RunnerDatabricksAuth

    url, stub = server
    _store(url)  # locally fresh, rejected by the server
    factory = _make_auth_token_factory(server_url=url)
    assert factory is not None
    with httpx.Client(base_url=url, auth=_RunnerDatabricksAuth(factory, url)) as client:
        assert client.get("/v1/me").status_code == 200
    assert stub.refresh_hits == 1
    assert factory() == "access-NEW-1"


@pytest.mark.parametrize("status", [503, 429])
def test_transient_refresh_failure_is_unreachable_without_relogin(token_dir, server, status):
    import omnigent.cli as cli
    from omnigent.chat import _DatabricksTokenAuth, relogin_hint_for

    url, stub = server
    stub.refresh_status = status
    _store(url, expires_in=-10)
    assert cli_auth.refresh_stored_token_with_outcome(url)[1] is RefreshOutcome.UNREACHABLE
    auth = _DatabricksTokenAuth(url)
    with httpx.Client(base_url=url, auth=auth) as client:
        assert client.get("/v1/me").status_code == 401
    assert auth.last_refresh_outcome is RefreshOutcome.UNREACHABLE
    assert relogin_hint_for(url, auth) is None
    cli._host_http_headers_cache.clear()
    result = cli._host_http_json(base_url=url, method="GET", path="/v1/hosts")
    assert "could not be renewed" not in str(result.body)
