"""Tests for the encrypted OIDC provider session store.

Verifies that raw tokens never appear in the database, handles resolve
to exactly one session, unknown/revoked handles fail closed, and
encryption key changes invalidate stored credentials.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from omnigent.db.db_models import OmnigentBase, SqlOidcSession
from omnigent.server.oidc_session_store import OidcSessionStore

_TEST_KEY = bytes.fromhex("aa" * 32)
_TEST_KEY_2 = bytes.fromhex("bb" * 32)


@pytest.fixture()
def session_factory(tmp_path: Path):
    """Create a fresh SQLite database with the oidc_sessions table."""
    db_path = tmp_path / "test_oidc_sessions.db"
    engine = create_engine(f"sqlite:///{db_path}")
    OmnigentBase.metadata.create_all(engine, tables=[SqlOidcSession.__table__])
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def _make_store(session_factory, key: bytes = _TEST_KEY) -> OidcSessionStore:
    return OidcSessionStore(session_factory, credential_key=key)


def test_raw_tokens_never_in_database(session_factory) -> None:
    """Raw ID and refresh tokens never appear in database columns."""
    store = _make_store(session_factory)
    id_token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.signature"
    refresh_token = "rt_secret_refresh_value_12345"

    store.create(
        user_id="alice@example.com",
        provider_subject="idp-sub-123",
        id_token=id_token,
        refresh_token=refresh_token,
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )

    with session_factory() as session:
        rows = session.execute(
            __import__("sqlalchemy").select(SqlOidcSession)
        ).scalars().all()
        assert len(rows) == 1
        row = rows[0]
        # Raw tokens must not appear in any column.
        for col_val in [
            row.handle_digest,
            row.user_id,
            row.provider_subject or "",
        ]:
            assert id_token not in col_val
            assert refresh_token not in col_val
        # Ciphertext is binary, not the raw token text.
        if row.credential_ciphertext is not None:
            assert id_token.encode() not in row.credential_ciphertext
            assert refresh_token.encode() not in row.credential_ciphertext


def test_handle_resolves_to_one_user_and_session(session_factory) -> None:
    """A random browser handle resolves to exactly one user and session ID."""
    store = _make_store(session_factory)
    handle = store.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="id-token-1",
        refresh_token="rt-1",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )

    result = store.resolve(handle)
    assert result is not None
    user_id, session_id, provider_subject = result
    assert user_id == "alice@example.com"
    assert session_id  # non-empty
    assert provider_subject == "sub-1"


def test_unknown_handle_fails_closed(session_factory) -> None:
    """A copied or unknown handle fails closed."""
    store = _make_store(session_factory)
    assert store.resolve("sess_nonexistent") is None
    assert store.resolve("not-a-sess-handle") is None
    assert store.resolve("") is None


def test_revoked_handle_fails_closed(session_factory) -> None:
    """A revoked handle fails closed immediately."""
    store = _make_store(session_factory)
    handle = store.create(
        user_id="bob@example.com",
        provider_subject="sub-2",
        id_token="id-token-2",
        refresh_token="rt-2",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )
    result = store.resolve(handle)
    assert result is not None
    session_id = result[1]

    assert store.revoke(session_id) is True
    assert store.resolve(handle) is None
    assert store.get_credentials(session_id, "bob@example.com") is None


def test_expired_session_fails_closed(session_factory) -> None:
    """An expired session fails closed."""
    store = _make_store(session_factory)
    past = int(time.time()) - 1
    handle = store.create(
        user_id="carol@example.com",
        provider_subject="sub-3",
        id_token="id-token-3",
        refresh_token="rt-3",
        id_token_expiry=past,
        absolute_expiry=past,
    )
    assert store.resolve(handle) is None


def test_get_credentials_roundtrip(session_factory) -> None:
    """get_credentials returns the same tokens that were stored."""
    store = _make_store(session_factory)
    id_token = "id-token-roundtrip"
    refresh_token = "rt-roundtrip"
    expiry = int(time.time()) + 3600

    handle = store.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token=id_token,
        refresh_token=refresh_token,
        id_token_expiry=expiry,
        absolute_expiry=int(time.time()) + 86400,
    )
    result = store.resolve(handle)
    assert result is not None
    _, session_id, _ = result

    creds = store.get_credentials(session_id, "alice@example.com")
    assert creds is not None
    retrieved_id, retrieved_rt, retrieved_exp = creds
    assert retrieved_id == id_token
    assert retrieved_rt == refresh_token
    assert retrieved_exp == expiry


def test_update_credentials_atomic(session_factory) -> None:
    """update_credentials replaces tokens atomically."""
    store = _make_store(session_factory)
    handle = store.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="old-id-token",
        refresh_token="old-rt",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )
    result = store.resolve(handle)
    assert result is not None
    _, session_id, _ = result

    new_expiry = int(time.time()) + 7200
    assert store.update_credentials(
        session_id, "alice@example.com", "new-id-token", "new-rt", new_expiry
    ) is True

    creds = store.get_credentials(session_id, "alice@example.com")
    assert creds is not None
    assert creds[0] == "new-id-token"
    assert creds[1] == "new-rt"
    assert creds[2] == new_expiry


def test_survives_reconstruction(session_factory) -> None:
    """A stored session survives store reconstruction with same DB and key."""
    store1 = _make_store(session_factory)
    handle = store1.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="persisted-id-token",
        refresh_token="persisted-rt",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )

    # Reconstruct with the same session factory and key.
    store2 = _make_store(session_factory)
    result = store2.resolve(handle)
    assert result is not None
    _, session_id, _ = result
    creds = store2.get_credentials(session_id, "alice@example.com")
    assert creds is not None
    assert creds[0] == "persisted-id-token"
    assert creds[1] == "persisted-rt"


def test_key_change_invalidates_credentials(session_factory) -> None:
    """Changing the encryption key makes token recovery fail closed."""
    store1 = _make_store(session_factory, key=_TEST_KEY)
    handle = store1.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="secret-id-token",
        refresh_token="secret-rt",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )
    result = store1.resolve(handle)
    assert result is not None
    _, session_id, _ = result

    # Reconstruct with a different key.
    store2 = _make_store(session_factory, key=_TEST_KEY_2)
    # resolve still works (it only checks the handle digest, not ciphertext).
    result2 = store2.resolve(handle)
    assert result2 is not None
    # But credential decryption fails closed.
    creds = store2.get_credentials(session_id, "alice@example.com")
    assert creds is None


def test_delete_expired_removes_old_sessions(session_factory) -> None:
    """delete_expired removes expired and revoked sessions."""
    store = _make_store(session_factory)
    past = int(time.time()) - 100

    # Create an expired session.
    store.create(
        user_id="expired@example.com",
        provider_subject="sub-x",
        id_token="id-x",
        refresh_token="rt-x",
        id_token_expiry=past,
        absolute_expiry=past,
    )
    # Create an active session.
    handle_active = store.create(
        user_id="active@example.com",
        provider_subject="sub-a",
        id_token="id-a",
        refresh_token="rt-a",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )

    count = store.delete_expired()
    assert count == 1  # only the expired one

    # Active session still resolves.
    assert store.resolve(handle_active) is not None


def test_wrong_user_id_fails_closed(session_factory) -> None:
    """get_credentials with wrong user_id fails closed."""
    store = _make_store(session_factory)
    handle = store.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="id-token",
        refresh_token="rt",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )
    result = store.resolve(handle)
    assert result is not None
    session_id = result[1]

    # Wrong user_id must not return credentials.
    assert store.get_credentials(session_id, "eve@example.com") is None


def test_revoke_does_not_delete_other_sessions(session_factory) -> None:
    """Revoking one session does not affect another for the same user."""
    store = _make_store(session_factory)
    handle1 = store.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="id-1",
        refresh_token="rt-1",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )
    handle2 = store.create(
        user_id="alice@example.com",
        provider_subject="sub-1",
        id_token="id-2",
        refresh_token="rt-2",
        id_token_expiry=int(time.time()) + 3600,
        absolute_expiry=int(time.time()) + 86400,
    )

    result1 = store.resolve(handle1)
    result2 = store.resolve(handle2)
    assert result1 is not None
    assert result2 is not None

    # Revoke session 1.
    assert store.revoke(result1[1]) is True

    # Session 1 is gone.
    assert store.resolve(handle1) is None
    # Session 2 is still active.
    assert store.resolve(handle2) is not None
    creds2 = store.get_credentials(result2[1], "alice@example.com")
    assert creds2 is not None
    assert creds2[0] == "id-2"
