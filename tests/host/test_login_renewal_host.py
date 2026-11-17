"""Host daemon stored-login renewal on tunnel rejection and before expiry."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from websockets.exceptions import InvalidStatus

from omnigent import cli_auth
from omnigent.host.connect import HostConnectError, HostProcess
from omnigent.host.identity import HostIdentity
from tests.cli import test_login_renewal as _shared

OLD = _shared.OLD
REFRESH = _shared.REFRESH
_assert_no_secrets = _shared._assert_no_secrets
# Reuse the shared local server and token-dir fixtures.
server = _shared.server
token_dir = _shared.token_dir


def _host(url: str) -> HostProcess:
    return HostProcess(HostIdentity(host_id="host_renew", name="laptop"), url)


def _store(url: str, *, expires_in: float = 3600, refresh: str = REFRESH) -> None:
    cli_auth.store_token(
        url, token=OLD, user_id="a@x", expires_at=time.time() + expires_in, refresh_token=refresh
    )


def _invalid_status(status: int) -> InvalidStatus:
    return InvalidStatus(SimpleNamespace(status_code=status, body=b"", headers={}))  # type: ignore[arg-type]


class _Stop(Exception):
    pass


def _always_reject(sent: list[str | None], status: int = 401):
    def _connect(_url, *, additional_headers, **_kw):
        sent.append(additional_headers.get("Authorization"))

        class _Cm:
            async def __aenter__(self):
                if len(sent) > 3:
                    raise _Stop  # a missing retry guard would loop forever
                raise _invalid_status(status)

        return _Cm()

    return _connect


async def test_upgrade_401_renews_and_retries_exactly_once(
    token_dir, server, monkeypatch, capsys, caplog
):
    import websockets.asyncio.client as ws_client

    url, stub = server
    _store(url)
    host = _host(url)
    sent: list[str | None] = []
    monkeypatch.setattr(ws_client, "connect", _always_reject(sent))
    with pytest.raises(InvalidStatus):
        await host._connect_and_serve()
    assert sent == [f"Bearer {OLD}", "Bearer access-NEW-1"]
    assert stub.refresh_hits == 1
    out = capsys.readouterr()
    _assert_no_secrets(caplog.text, out.out, out.err)


async def test_managed_host_401_never_refreshes(token_dir, server, monkeypatch):
    import websockets.asyncio.client as ws_client

    from omnigent.host.identity import HOST_TOKEN_ENV_VAR

    url, stub = server
    _store(url)
    monkeypatch.setenv(HOST_TOKEN_ENV_VAR, "managed-launch-token")
    host = _host(url)
    sent: list[str | None] = []
    monkeypatch.setattr(ws_client, "connect", _always_reject(sent))
    with pytest.raises(InvalidStatus):
        await host._connect_and_serve()
    assert sent == [None]  # no bearer, one attempt
    assert stub.refresh_hits == 0


async def test_plain_403_renews_once_per_outage_not_every_reconnect(
    token_dir, server, monkeypatch
):
    """Omnigent's tunnel answers a bad bearer with 403 too, so the first 403
    renews; a persistent permission 403 must not refresh on every reconnect."""
    import websockets.asyncio.client as ws_client

    url, stub = server
    _store(url)
    host = _host(url)
    sent: list[str | None] = []
    monkeypatch.setattr(ws_client, "connect", _always_reject(sent, status=403))
    for _ in range(3):  # three reconnect attempts in one outage
        with pytest.raises((InvalidStatus, HostConnectError)):
            await host._connect_and_serve()
        sent.clear()
    assert stub.refresh_hits == 1


async def test_login_renewal_loop_renews_in_window_and_cancels(token_dir, server, monkeypatch):
    import asyncio

    import omnigent.host.connect as connect_mod

    url, stub = server
    monkeypatch.setattr(connect_mod, "_LOGIN_RENEW_CHECK_INTERVAL_S", 0.05)
    _store(url, expires_in=300)  # inside the 10-minute window
    host = _host(url)
    task = asyncio.create_task(host._login_renewal_loop())
    for _ in range(100):
        if stub.refresh_hits:
            break
        await asyncio.sleep(0.05)
    assert stub.refresh_hits == 1
    await asyncio.sleep(0.2)  # renewed token is outside the window now
    assert stub.refresh_hits == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


async def test_upgrade_401_with_refused_grant_is_fatal_relogin(token_dir, server, monkeypatch):
    import websockets.asyncio.client as ws_client

    url, stub = server
    _store(url, refresh="revoked")
    host = _host(url)

    def _connect(_url, **_kw):
        class _Cm:
            async def __aenter__(self):
                raise _invalid_status(401)

        return _Cm()

    monkeypatch.setattr(ws_client, "connect", _connect)
    with pytest.raises(HostConnectError, match="could not be renewed"):
        await host._connect_and_serve()
    assert stub.refresh_hits == 1


def test_connected_host_prints_relogin_once_and_keeps_retrying(token_dir, server, capsys):
    url, _stub = server
    _store(url, refresh="revoked")
    host = _host(url)
    host._ever_connected = True
    host._last_sent_token = OLD
    assert host._renew_rejected_login() is False
    assert host._classify_http_status(401) is None
    assert host._classify_http_status(401) is None
    assert capsys.readouterr().err.count("could not be renewed") == 1


def test_unreachable_refresh_keeps_vpn_text(token_dir, capsys):
    url = "http://127.0.0.1:9"
    _store(url)
    host = _host(url)
    host._ever_connected = True
    host._last_sent_token = OLD
    assert host._renew_rejected_login() is False
    assert host._classify_http_status(401) is None
    err = capsys.readouterr().err
    assert "network dropped" in err and "could not be renewed" not in err


def test_proactive_renewal_window(token_dir, server, monkeypatch):
    url, stub = server
    host = _host(url)
    _store(url, expires_in=3600)
    assert host._renew_login_if_expiring() is None  # outside the window
    _store(url, expires_in=300)
    assert host._renew_login_if_expiring() is cli_auth.RefreshOutcome.RENEWED
    assert stub.refresh_hits == 1
    # Managed host: never touches the user login.
    _store(url, expires_in=300)
    monkeypatch.setenv("OMNIGENT_HOST_TOKEN", "managed")
    from omnigent.host.identity import HOST_TOKEN_ENV_VAR

    monkeypatch.setenv(HOST_TOKEN_ENV_VAR, "managed")
    assert host._renew_login_if_expiring() is None
    monkeypatch.delenv(HOST_TOKEN_ENV_VAR)
    # No refresh material: skipped.
    cli_auth.store_token(url, token=OLD, user_id="a@x", expires_at=time.time() + 300)
    assert host._renew_login_if_expiring() is None
    assert stub.refresh_hits == 1


@pytest.mark.parametrize("status", [503, 429])
async def test_transient_refresh_failure_is_not_fatal_relogin(
    token_dir, server, monkeypatch, status
):
    import websockets.asyncio.client as ws_client

    url, stub = server
    stub.refresh_status = status
    _store(url)
    host = _host(url)
    sent: list[str | None] = []
    monkeypatch.setattr(ws_client, "connect", _always_reject(sent))
    with pytest.raises(InvalidStatus):  # transient: retried, not fatal
        await host._connect_and_serve()
    assert host._last_refresh_outcome is cli_auth.RefreshOutcome.UNREACHABLE


async def test_plain_403_renewal_budget_resets_after_successful_upgrade(
    token_dir, server, monkeypatch
):
    import websockets.asyncio.client as ws_client

    url, stub = server
    _store(url)
    host = _host(url)
    script = iter([403, "ok", 403])
    sent: list[str | None] = []

    def _connect(_url, *, additional_headers, **_kw):
        sent.append(additional_headers.get("Authorization"))
        step = next(script, 403)

        class _Cm:
            async def __aenter__(self):
                if step == "ok":
                    return SimpleNamespace(protocol=None, ping_interval=None, ping_timeout=None)
                raise _invalid_status(step)

            async def __aexit__(self, *_a):
                return False

        return _Cm()

    async def _serve_stops(*_a, **_kw):
        raise _Stop

    monkeypatch.setattr(ws_client, "connect", _connect)
    monkeypatch.setattr(host, "_ensure_owner_user_id", _serve_stops)
    # 403 -> renewal -> retry succeeds (upgrade accepted, then serving stops).
    with pytest.raises(_Stop):
        await host._connect_and_serve()
    assert stub.refresh_hits == 1
    # A later outage's plain 403 gets a fresh renewal.
    with pytest.raises((InvalidStatus, HostConnectError)):
        await host._connect_and_serve()
    assert stub.refresh_hits == 2
