"""Tests for the OIDC token manager (on-demand ID token refresh).

Uses a fake Ticino token endpoint and controllable clock to verify
refresh behaviour, concurrency safety, identity-change rejection, and
absolute-session capping.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

from omnigent.db.db_models import OmnigentBase, SqlOidcSession
from omnigent.server.oidc import OIDCConfig
from omnigent.server.oidc_session_store import OidcSessionStore
from omnigent.server.oidc_token_manager import (
    IdTokenResult,
    OidcTokenManager,
    ReauthenticationError,
)

_TEST_KEY = bytes.fromhex("aa" * 32)
_ISSUER = "https://idp.example.com"
_CLIENT_ID = "public-client"


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
            "email": "alice@example.com",
            "email_verified": True,
            **claims,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")


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


@pytest.fixture()
def session_factory(tmp_path: Path):
    db_path = tmp_path / "test_token_manager.db"
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
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


def _create_session(
    store: OidcSessionStore,
    keys: _IdpKeys,
    *,
    id_token: str | None = None,
    refresh_token: str = "rt-initial",
    id_token_expiry: int | None = None,
    absolute_expiry: int | None = None,
) -> tuple[str, str]:
    """Create a provider session and return (handle, session_id)."""
    now = int(time.time())
    if id_token is None:
        id_token = keys.sign_id_token({})
    if id_token_expiry is None:
        id_token_expiry = now + 3600
    if absolute_expiry is None:
        absolute_expiry = now + 86400
    handle = store.create(
        user_id="alice@example.com",
        provider_subject="idp-subject-123",
        provider_issuer=_ISSUER,
        provider_client_id=_CLIENT_ID,
        id_token=id_token,
        refresh_token=refresh_token,
        id_token_expiry=id_token_expiry,
        absolute_expiry=absolute_expiry,
    )
    result = store.resolve(handle)
    assert result is not None
    return handle, result[1]


def test_still_valid_id_token_returned_without_network(session_factory, keys, monkeypatch) -> None:
    """A still-valid ID token is returned without a network call."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    _, session_id = _create_session(store, keys)

    # Patch httpx.post to fail if called.
    def _fail_post(*args, **kwargs):
        raise AssertionError("Should not call the token endpoint")

    monkeypatch.setattr(httpx, "post", _fail_post)

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert isinstance(result, IdTokenResult)
    assert result.id_token  # non-empty


def test_expiring_id_token_refreshed(session_factory, keys, monkeypatch) -> None:
    """An expiring ID token is refreshed with grant_type=refresh_token."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    # Create a session with an ID token that expires in 30 seconds
    # (within the 60-second refresh margin).
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(store, keys, id_token=old_token, id_token_expiry=now + 30)

    new_token = keys.sign_id_token({})
    captured_data: dict = {}

    def _fake_post(url, *, data=None, **kwargs):
        if data is not None:
            captured_data.update(data)
        return httpx.Response(
            200,
            json={
                "id_token": new_token,
                "refresh_token": "rt-rotated",
            },
        )

    monkeypatch.setattr(httpx, "post", _fake_post)

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert result.id_token == new_token
    assert captured_data.get("grant_type") == "refresh_token"
    assert captured_data.get("client_id") == _CLIENT_ID
    assert "client_secret" not in captured_data
    assert captured_data.get("refresh_token") == "rt-initial"


def test_rotated_refresh_token_replaces_old(session_factory, keys, monkeypatch) -> None:
    """A rotated refresh token replaces the old token atomically."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        refresh_token="rt-old",
    )

    new_token = keys.sign_id_token({})
    call_count = [0]

    def _fake_post(url, *, data=None, **kwargs):
        call_count[0] += 1
        return httpx.Response(
            200,
            json={
                "id_token": new_token,
                "refresh_token": "rt-new-rotated",
            },
        )

    monkeypatch.setattr(httpx, "post", _fake_post)

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert result.id_token == new_token

    # The store now has the rotated refresh token.
    creds = store.get_credentials(session_id, "alice@example.com")
    assert creds is not None
    assert creds[1] == "rt-new-rotated"


def test_missing_new_refresh_token_retains_current(session_factory, keys, monkeypatch) -> None:
    """Omission of a new refresh token retains the current one."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        refresh_token="rt-keep-me",
    )

    new_token = keys.sign_id_token({})

    def _fake_post(url, *, data=None, **kwargs):
        return httpx.Response(200, json={"id_token": new_token})

    monkeypatch.setattr(httpx, "post", _fake_post)

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert result.id_token == new_token

    creds = store.get_credentials(session_id, "alice@example.com")
    assert creds is not None
    assert creds[1] == "rt-keep-me"


def test_invalid_grant_clears_credentials(session_factory, keys, monkeypatch) -> None:
    """invalid_grant clears credentials and reports reauthentication."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
    )

    def _fake_post(url, *, data=None, **kwargs):
        return httpx.Response(400, json={"error": "invalid_grant"})

    monkeypatch.setattr(httpx, "post", _fake_post)

    with pytest.raises(ReauthenticationError):
        manager.get_current_id_token(session_id, "alice@example.com")

    # Session is revoked.
    assert store.get_credentials(session_id, "alice@example.com") is None


@pytest.mark.parametrize("status_code", [400, 408, 425, 429])
def test_nonpermanent_refresh_http_failure_releases_lease_for_retry(
    session_factory, keys, monkeypatch, status_code: int
) -> None:
    """Non-permanent HTTP failures leave the encrypted refresh token usable."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager = OidcTokenManager(store, _make_config())
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        refresh_token="rt-retryable",
    )
    refreshed_token = keys.sign_id_token({})
    responses = [
        httpx.Response(status_code, json={"error": "temporarily_unavailable"}),
        httpx.Response(200, json={"id_token": refreshed_token}),
    ]

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: responses.pop(0))

    with pytest.raises(ReauthenticationError, match="retry"):
        manager.get_current_id_token(session_id, "alice@example.com")

    credentials = store.get_refresh_credentials(session_id, "alice@example.com")
    assert credentials is not None
    assert credentials.refresh_token == "rt-retryable"
    assert credentials.refresh_lease_expires_at is None

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert result.id_token == refreshed_token


def test_identity_change_clears_credentials(session_factory, keys, monkeypatch) -> None:
    """An identity change during refresh clears credentials."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
    )

    # Refreshed token has a different email.
    new_token = keys.sign_id_token(
        {
            "email": "eve@evil.example",
            "email_verified": True,
        }
    )

    def _fake_post(url, *, data=None, **kwargs):
        return httpx.Response(200, json={"id_token": new_token})

    monkeypatch.setattr(httpx, "post", _fake_post)

    with pytest.raises(ReauthenticationError, match="Identity changed"):
        manager.get_current_id_token(session_id, "alice@example.com")

    assert store.get_credentials(session_id, "alice@example.com") is None


def test_absolute_expiry_caps_use(session_factory, keys) -> None:
    """A session past its absolute expiry requires sign-in again."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    # Create with expired absolute expiry; resolve will return None,
    # so get the session_id directly from the database.
    handle = store.create(
        user_id="alice@example.com",
        provider_subject="idp-subject-123",
        provider_issuer=_ISSUER,
        provider_client_id=_CLIENT_ID,
        id_token=keys.sign_id_token({}),
        refresh_token="rt-initial",
        id_token_expiry=now + 3600,
        absolute_expiry=now - 1,
    )
    import hashlib

    digest = hashlib.sha256(handle.encode("utf-8")).hexdigest()
    with session_factory() as session:
        from sqlalchemy import select

        row = session.execute(
            select(SqlOidcSession).where(SqlOidcSession.handle_digest == digest)
        ).scalar_one()
        session_id = row.id

    with pytest.raises(ReauthenticationError):
        manager.get_current_id_token(session_id, "alice@example.com")


def test_revoked_session_requires_reauthentication(session_factory, keys) -> None:
    """A revoked session raises ReauthenticationError."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    _, session_id = _create_session(store, keys)
    store.revoke(session_id)

    with pytest.raises(ReauthenticationError):
        manager.get_current_id_token(session_id, "alice@example.com")


def test_no_refresh_token_requires_reauthentication(session_factory, keys) -> None:
    """A session without a refresh token raises ReauthenticationError when
    the ID token is expiring."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        refresh_token="",  # no refresh token
    )

    with pytest.raises(ReauthenticationError):
        manager.get_current_id_token(session_id, "alice@example.com")


def test_refresh_never_extends_absolute_expiry(session_factory, keys, monkeypatch) -> None:
    """Refresh never moves the provider session's absolute expiry."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    absolute = int(time.time()) + 7200  # 2 hours
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        absolute_expiry=absolute,
    )

    new_token = keys.sign_id_token({"exp": now + 3600})

    def _fake_post(url, *, data=None, **kwargs):
        return httpx.Response(200, json={"id_token": new_token})

    monkeypatch.setattr(httpx, "post", _fake_post)

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert result.id_token == new_token

    # The absolute expiry must not have changed — the session is still valid.
    creds = store.get_credentials(session_id, "alice@example.com")
    assert creds is not None  # still valid — absolute expiry unchanged


def test_malformed_response_clears_credentials(session_factory, keys, monkeypatch) -> None:
    """A malformed refresh response clears credentials."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    config = _make_config()
    manager = OidcTokenManager(store, config)

    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
    )

    def _fake_post(url, *, data=None, **kwargs):
        return httpx.Response(200, text="not-json-at-all")

    monkeypatch.setattr(httpx, "post", _fake_post)

    with pytest.raises(ReauthenticationError):
        manager.get_current_id_token(session_id, "alice@example.com")


def test_legacy_session_binds_verified_provider_identity(
    session_factory, keys, monkeypatch
) -> None:
    """A pre-migration row is bound only after its current JWT validates."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager = OidcTokenManager(store, _make_config())
    _, session_id = _create_session(store, keys)

    with session_factory() as session:
        session.execute(
            update(SqlOidcSession)
            .where(SqlOidcSession.id == session_id)
            .values(
                provider_subject="",
                provider_issuer=None,
                provider_client_id=None,
            )
        )
        session.commit()

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: pytest.fail("must not refresh"))
    result = manager.get_current_id_token(session_id, "alice@example.com")

    assert result.id_token
    credentials = store.get_refresh_credentials(session_id, "alice@example.com")
    assert credentials is not None
    assert credentials.provider_subject == "idp-subject-123"
    assert credentials.provider_issuer == _ISSUER
    assert credentials.provider_client_id == _CLIENT_ID


def test_legacy_session_with_conflicting_subject_requires_reauthentication(
    session_factory, keys, monkeypatch
) -> None:
    """A nullable issuer/client never overrides a conflicting stored subject."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager = OidcTokenManager(store, _make_config())
    _, session_id = _create_session(store, keys)

    with session_factory() as session:
        session.execute(
            update(SqlOidcSession)
            .where(SqlOidcSession.id == session_id)
            .values(
                provider_subject="different-subject",
                provider_issuer=None,
                provider_client_id=None,
            )
        )
        session.commit()

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: pytest.fail("must not refresh"))
    with pytest.raises(ReauthenticationError, match="verified provider identity"):
        manager.get_current_id_token(session_id, "alice@example.com")


def test_changed_subject_with_same_email_clears_credentials(
    session_factory, keys, monkeypatch
) -> None:
    """Refresh rejects a different provider subject even when email is unchanged."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager = OidcTokenManager(store, _make_config())
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
    )
    changed_subject_token = keys.sign_id_token({"sub": "different-idp-subject"})

    monkeypatch.setattr(
        httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(200, json={"id_token": changed_subject_token}),
    )

    with pytest.raises(ReauthenticationError, match="Identity changed"):
        manager.get_current_id_token(session_id, "alice@example.com")
    assert store.get_credentials(session_id, "alice@example.com") is None


def test_concurrent_rotating_refresh_has_one_owner_and_loser_rereads_winner(
    session_factory, keys, monkeypatch
) -> None:
    """Separate managers share a DB lease instead of consuming a token twice."""
    store1 = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store1,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        refresh_token="rotating-rt",
    )
    # A reconstructed store/manager represents a second server process.
    store2 = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager1 = OidcTokenManager(store1, _make_config())
    manager2 = OidcTokenManager(store2, _make_config())
    request_started = threading.Event()
    release_response = threading.Event()
    calls: list[int] = []
    calls_lock = threading.Lock()
    refreshed_token = keys.sign_id_token({})

    def _fake_post(*args, **kwargs):
        with calls_lock:
            calls.append(1)
        leased = store2.get_refresh_credentials(session_id, "alice@example.com")
        assert leased is not None
        assert leased.refresh_lease_expires_at is not None
        request_started.set()
        assert release_response.wait(timeout=5)
        return httpx.Response(
            200,
            json={"id_token": refreshed_token, "refresh_token": "rotated-rt"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    results: list[IdTokenResult] = []
    errors: list[BaseException] = []

    def _get_token(manager: OidcTokenManager) -> None:
        try:
            results.append(manager.get_current_id_token(session_id, "alice@example.com"))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first = threading.Thread(target=_get_token, args=(manager1,))
    second = threading.Thread(target=_get_token, args=(manager2,))
    first.start()
    assert request_started.wait(timeout=5)
    second.start()
    time.sleep(0.1)
    release_response.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert calls == [1]
    assert [result.id_token for result in results] == [refreshed_token, refreshed_token]
    credentials = store1.get_credentials(session_id, "alice@example.com")
    assert credentials is not None
    assert credentials[1] == "rotated-rt"


def test_revoke_during_refresh_does_not_resurrect_credentials(
    session_factory, keys, monkeypatch
) -> None:
    """A refresh commit loses to logout and cannot write its returned tokens."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager = OidcTokenManager(store, _make_config())
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(store, keys, id_token=old_token, id_token_expiry=now + 30)
    refreshed_token = keys.sign_id_token({})

    def _fake_post(*args, **kwargs):
        assert store.revoke(session_id)
        return httpx.Response(200, json={"id_token": refreshed_token})

    monkeypatch.setattr(httpx, "post", _fake_post)

    with pytest.raises(ReauthenticationError, match="revoked during refresh"):
        manager.get_current_id_token(session_id, "alice@example.com")
    assert store.get_credentials(session_id, "alice@example.com") is None


def test_absolute_expiry_during_refresh_does_not_resurrect_credentials(
    session_factory, keys, monkeypatch
) -> None:
    """Commit rechecks absolute expiry after the remote refresh returns."""
    store = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    now = int(time.time())
    clock = [now]
    manager = OidcTokenManager(store, _make_config(), clock=lambda: clock[0])
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(
        store,
        keys,
        id_token=old_token,
        id_token_expiry=now + 30,
        absolute_expiry=now + 120,
    )
    refreshed_token = keys.sign_id_token({})

    def _fake_post(*args, **kwargs):
        clock[0] = now + 121
        return httpx.Response(200, json={"id_token": refreshed_token})

    monkeypatch.setattr(httpx, "post", _fake_post)

    with pytest.raises(ReauthenticationError, match="revoked during refresh"):
        manager.get_current_id_token(session_id, "alice@example.com")
    assert (
        store.get_refresh_credentials(
            session_id,
            "alice@example.com",
            now_epoch_seconds=clock[0],
        )
        is None
    )
    # The rejected refresh never overwrites the old encrypted credentials.
    credentials = store.get_credentials(session_id, "alice@example.com")
    assert credentials is not None
    assert credentials[0] == old_token


def test_refresh_survives_store_reconstruction(session_factory, keys, monkeypatch) -> None:
    """A restarted process can coordinate and commit from persisted state."""
    store1 = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    now = int(time.time())
    old_token = keys.sign_id_token({"exp": now + 30})
    _, session_id = _create_session(store1, keys, id_token=old_token, id_token_expiry=now + 30)
    store_after_restart = OidcSessionStore(session_factory, credential_key=_TEST_KEY)
    manager = OidcTokenManager(store_after_restart, _make_config())
    refreshed_token = keys.sign_id_token({})
    monkeypatch.setattr(
        httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(200, json={"id_token": refreshed_token}),
    )

    result = manager.get_current_id_token(session_id, "alice@example.com")
    assert result.id_token == refreshed_token
