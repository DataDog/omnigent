"""Red tests for the OIDC-bound identity-token provider boundary (Slice 2).

Pins ``AuthProvider.get_identity_token_provider`` — the optional,
non-breaking method the managed-sandbox path uses to hand provider code a
renewable ID token bound to the authenticated OIDC credential session.

Fail-closed semantics (design decision 6): expired, revoked, or cross-user
sessions never yield a usable identity; only OIDC mode with a live
``sess_`` credential session produces a provider. Refresh credentials
must never cross the boundary.

These tests fail before the behavior exists: ``AuthProvider`` has no
``get_identity_token_provider`` method (AttributeError).
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from omnigent.db.db_models import OmnigentBase, SqlOidcSession
from omnigent.onboarding.sandboxes.context import IdentityToken
from omnigent.server.accounts_config import AccountsConfig
from omnigent.server.auth import AuthProvider, UnifiedAuthProvider
from omnigent.server.oidc import OIDCConfig, mint_session_token
from omnigent.server.oidc_session_store import OidcSessionStore
from omnigent.server.oidc_token_manager import ReauthenticationError

_TEST_KEY = bytes.fromhex("aa" * 32)
_ISSUER = "https://idp.example.com"
_CLIENT_ID = "public-client"
_ALICE = "alice@example.com"
_BOB = "bob@example.com"
_REFRESH_SENTINEL = "rt-SECRET-REFRESH-SENTINEL"


class _IdpKeys:
    """RSA keypair for signing test ID tokens."""

    def __init__(self) -> None:
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk_dict = json.loads(RSAAlgorithm.to_jwk(self.private_key.public_key()))
        jwk_dict["alg"] = "RS256"
        self.signing_key = jwt.PyJWK.from_dict(jwk_dict)

    def sign_id_token(self, claims: dict[str, object]) -> str:
        now = int(time.time())
        payload: dict[str, object] = {
            "iss": _ISSUER,
            "aud": _CLIENT_ID,
            "iat": now,
            "exp": now + 3600,
            "sub": "idp-subject-123",
            "email": _ALICE,
            "email_verified": True,
            **claims,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")


@pytest.fixture()
def session_factory(tmp_path: Path):
    db_path = tmp_path / "test_identity_provider.db"
    engine = create_engine(f"sqlite:///{db_path}")
    OmnigentBase.metadata.create_all(engine, tables=[SqlOidcSession.__table__])
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


@pytest.fixture()
def keys() -> _IdpKeys:
    return _IdpKeys()


@pytest.fixture(autouse=True)
def _mock_jwks(monkeypatch: pytest.MonkeyPatch, keys: _IdpKeys) -> None:
    """Stub JWKS lookup so jwt.decode uses our test key."""
    monkeypatch.setattr(
        jwt.PyJWKClient,
        "get_signing_key_from_jwt",
        lambda self, token: keys.signing_key,
    )


def _make_config() -> OIDCConfig:
    return OIDCConfig(
        issuer=_ISSUER,
        client_id=_CLIENT_ID,
        client_secret=None,
        redirect_uri="http://localhost:8000/auth/callback",
        cookie_secret=_TEST_KEY,
        scopes="openid email profile",
        session_ttl_hours=24,
        logout_redirect_uri=None,
        allowed_domains=None,
        provider_type="oidc",
        authorization_endpoint=f"{_ISSUER}/authorize",
        token_endpoint=f"{_ISSUER}/token",
        jwks_uri=f"{_ISSUER}/jwks",
        userinfo_endpoint=None,
        allow_invites=False,
    )


def _make_provider(store: OidcSessionStore) -> UnifiedAuthProvider:
    return UnifiedAuthProvider(source="oidc", oidc_config=_make_config(), oidc_session_store=store)


def _mock_request(
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> MagicMock:
    """Minimal stand-in HTTPConnection (same pattern as test_oidc.py)."""
    mock: MagicMock = MagicMock()
    mock.headers = headers or {}
    mock.cookies = cookies or {}
    return mock


def _create_session(
    store: OidcSessionStore,
    keys: _IdpKeys,
    *,
    user_id: str,
    id_token: str,
    refresh_token: str = _REFRESH_SENTINEL,
    id_token_expiry: int | None = None,
    absolute_expiry: int | None = None,
) -> tuple[str, str]:
    """Create a provider session and return (handle, session_id)."""
    now = int(time.time())
    handle = store.create(
        user_id=user_id,
        provider_subject="idp-subject-123",
        provider_issuer=_ISSUER,
        provider_client_id=_CLIENT_ID,
        id_token=id_token,
        refresh_token=refresh_token,
        id_token_expiry=id_token_expiry if id_token_expiry is not None else now + 3600,
        absolute_expiry=absolute_expiry if absolute_expiry is not None else now + 86400,
    )
    result = store.resolve(handle)
    assert result is not None
    return handle, result[1]


def _request_with_handle(config: OIDCConfig, handle: str) -> MagicMock:
    return _mock_request(cookies={config.session_cookie_name: handle})


# ── non-breaking default and unsupported modes ────────────────────


def test_auth_provider_default_returns_none() -> None:
    """The ABC default is non-breaking: a minimal provider returns None."""
    alice = _ALICE

    class _MinimalProvider(AuthProvider):
        def get_user_id(self, request: Any) -> str | None:
            return alice

    provider = _MinimalProvider()
    request = _mock_request()
    assert provider.get_user_id(request) == alice
    assert provider.get_identity_token_provider(request, alice) is None


def test_header_mode_returns_no_provider() -> None:
    """Header mode has no IdP credential session, so no provider."""
    provider = UnifiedAuthProvider(source="header")
    request = _mock_request(headers={"X-Forwarded-Email": _ALICE})
    assert provider.get_user_id(request) == _ALICE
    assert provider.get_identity_token_provider(request, _ALICE) is None


def test_accounts_mode_returns_no_provider() -> None:
    """Accounts mode authenticates but carries no renewable IdP token."""
    config = AccountsConfig(
        cookie_secret=_TEST_KEY,
        session_ttl_hours=8,
        base_url="http://localhost:8000",
        init_admin_password=None,
        invite_ttl_seconds=3600,
        magic_ttl_seconds=600,
    )
    provider = UnifiedAuthProvider(source="accounts", accounts_config=config)
    token = mint_session_token(_ALICE, config.cookie_secret, 3600, "accounts")
    request = _mock_request(cookies={config.session_cookie_name: token})
    assert provider.get_user_id(request) == _ALICE
    assert provider.get_identity_token_provider(request, _ALICE) is None


# ── OIDC mode: provider bound to the credential session ───────────


def test_valid_oidc_request_yields_provider_bound_to_credential_session(
    session_factory, keys
) -> None:
    """The provider serves exactly the token of the request's session."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    alice_token = keys.sign_id_token({})
    bob_token = keys.sign_id_token({"email": _BOB, "sub": "idp-subject-bob"})
    alice_handle, _ = _create_session(store, keys, user_id=_ALICE, id_token=alice_token)
    bob_handle, _ = _create_session(store, keys, user_id=_BOB, id_token=bob_token)

    # Alice's request yields a provider serving alice's — never bob's — token.
    alice_tp = provider.get_identity_token_provider(
        _request_with_handle(config, alice_handle), _ALICE
    )
    assert alice_tp is not None
    token = alice_tp.get_identity_token()
    assert isinstance(token, IdentityToken)
    assert token.value == alice_token
    assert token.value != bob_token

    # Bob's request yields a provider serving bob's token: sessions isolated.
    bob_tp = provider.get_identity_token_provider(_request_with_handle(config, bob_handle), _BOB)
    assert bob_tp is not None
    assert bob_tp.get_identity_token().value == bob_token
    assert bob_tp.get_identity_token().value != alice_token


@pytest.mark.asyncio
async def test_oidc_request_resolution_runs_off_event_loop(
    session_factory, keys, monkeypatch
) -> None:
    """ASGI preparation resolves the opaque session in a worker thread once."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)
    handle, _ = _create_session(
        store,
        keys,
        user_id=_ALICE,
        id_token=keys.sign_id_token({}),
    )
    event_loop_thread = threading.get_ident()
    resolve_threads: list[int] = []
    real_resolve = store.resolve

    def tracked_resolve(candidate: str):
        resolve_threads.append(threading.get_ident())
        return real_resolve(candidate)

    monkeypatch.setattr(store, "resolve", tracked_resolve)
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "path": "/v1/me",
            "raw_path": b"/v1/me",
            "query_string": b"",
            "headers": [
                (
                    b"cookie",
                    f"{config.session_cookie_name}={handle}".encode(),
                )
            ],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        }
    )

    await provider.prepare_connection(request)

    assert resolve_threads and resolve_threads[0] != event_loop_thread
    assert provider.get_user_id(request) == _ALICE
    assert provider.get_identity_token_provider(request, _ALICE) is not None
    assert len(resolve_threads) == 1


def test_provider_returns_current_id_token_without_refresh(
    session_factory, keys, monkeypatch
) -> None:
    """A still-valid ID token is returned as-is, with no network call."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    now = int(time.time())
    id_token = keys.sign_id_token({})
    handle, _ = _create_session(store, keys, user_id=_ALICE, id_token=id_token)

    def _fail_post(*args: object, **kwargs: object) -> None:
        raise AssertionError("Should not call the token endpoint")

    monkeypatch.setattr(httpx, "post", _fail_post)

    tp = provider.get_identity_token_provider(_request_with_handle(config, handle), _ALICE)
    assert tp is not None
    token = tp.get_identity_token()
    assert isinstance(token, IdentityToken)
    assert token.value == id_token
    assert token.expires_at == now + 3600


def test_near_expiry_id_token_refreshes_through_token_manager(
    session_factory, keys, monkeypatch
) -> None:
    """A near-expiry stored ID token is refreshed before being returned."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    handle, _ = _create_session(
        store, keys, user_id=_ALICE, id_token=old_token, id_token_expiry=now + 30
    )

    new_exp = now + 7200
    new_token = keys.sign_id_token({"exp": new_exp})
    captured: dict[str, object] = {}

    def _fake_post(url: object, *, data: dict | None = None, **kwargs: object) -> httpx.Response:
        if data is not None:
            captured.update(data)
        return httpx.Response(200, json={"id_token": new_token})

    monkeypatch.setattr(httpx, "post", _fake_post)

    tp = provider.get_identity_token_provider(_request_with_handle(config, handle), _ALICE)
    assert tp is not None
    token = tp.get_identity_token()
    assert isinstance(token, IdentityToken)
    assert token.value == new_token
    assert token.value != old_token
    assert token.expires_at == new_exp
    assert captured.get("grant_type") == "refresh_token"
    assert captured.get("refresh_token") == _REFRESH_SENTINEL


# ── fail-closed: revoked, cross-user, missing sessions ────────────


def test_revoked_session_fails_closed(session_factory, keys) -> None:
    """Revoking the credential session after binding fails the next call."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    id_token = keys.sign_id_token({})
    handle, session_id = _create_session(store, keys, user_id=_ALICE, id_token=id_token)

    tp = provider.get_identity_token_provider(_request_with_handle(config, handle), _ALICE)
    assert tp is not None

    # Logout revokes the session; the already-bound provider must fail closed
    # on its next call rather than serve stale identity.
    assert store.revoke(session_id)
    with pytest.raises(ReauthenticationError):
        tp.get_identity_token()


def test_cross_user_request_yields_no_provider(session_factory, keys) -> None:
    """A provider is never issued for a user who does not own the session."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    bob_token = keys.sign_id_token({"email": _BOB, "sub": "idp-subject-bob"})
    bob_handle, _ = _create_session(store, keys, user_id=_BOB, id_token=bob_token)

    request = _request_with_handle(config, bob_handle)
    assert provider.get_user_id(request) == _BOB
    # Bob's credential session can never produce alice's identity.
    assert provider.get_identity_token_provider(request, _ALICE) is None


def test_persisted_credential_session_resolves_only_its_active_owner(
    session_factory, keys
) -> None:
    """Restart recovery cannot substitute another owner or a revoked credential."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)
    alice_token = keys.sign_id_token({})
    _, session_id = _create_session(store, keys, user_id=_ALICE, id_token=alice_token)

    recovered = provider.get_identity_token_provider_for_credential_session(session_id, _ALICE)
    assert recovered is not None
    assert recovered.get_identity_token().value == alice_token
    assert provider.get_identity_token_provider_for_credential_session(session_id, _BOB) is None
    assert store.revoke(session_id)
    assert provider.get_identity_token_provider_for_credential_session(session_id, _ALICE) is None


def test_refresh_credentials_never_leave_provider(session_factory, keys) -> None:
    """The refresh token never crosses the provider boundary."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    id_token = keys.sign_id_token({})
    handle, _ = _create_session(store, keys, user_id=_ALICE, id_token=id_token)

    tp = provider.get_identity_token_provider(_request_with_handle(config, handle), _ALICE)
    assert tp is not None
    token = tp.get_identity_token()

    # The sentinel refresh token must not surface anywhere on the boundary.
    assert _REFRESH_SENTINEL not in token.value
    assert _REFRESH_SENTINEL not in repr(token)
    assert _REFRESH_SENTINEL not in repr(tp)


def test_user_extraction_and_provider_share_one_credential_path(session_factory, keys) -> None:
    """get_user_id and get_identity_token_provider agree on every request."""
    config = _make_config()
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    provider = _make_provider(store)

    id_token = keys.sign_id_token({})
    handle, _ = _create_session(store, keys, user_id=_ALICE, id_token=id_token)

    # Valid session: both resolve, to the same credential session.
    valid_request = _request_with_handle(config, handle)
    assert provider.get_user_id(valid_request) == _ALICE
    tp = provider.get_identity_token_provider(valid_request, _ALICE)
    assert tp is not None
    assert tp.get_identity_token().value == id_token

    # Unknown handle: both fail — never a user without a provider session.
    unknown_request = _request_with_handle(config, "sess_unknown_handle")
    assert provider.get_user_id(unknown_request) is None
    assert provider.get_identity_token_provider(unknown_request, _ALICE) is None

    # No cookie: both fail the same way.
    bare_request = _mock_request()
    assert provider.get_user_id(bare_request) is None
    assert provider.get_identity_token_provider(bare_request, _ALICE) is None
