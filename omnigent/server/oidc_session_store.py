"""Encrypted OIDC provider session store.

Persists IdP-issued ID and refresh tokens as versioned AES-GCM ciphertext behind
an opaque ``sess_…`` handle. The browser/CLI receives only the handle;
its SHA-256 digest is stored for lookup, never the handle itself.

Uses a separate stable ``OMNIGENT_OIDC_CREDENTIAL_KEY`` (distinct from the
cookie/state signing key) for encryption. Ciphertext is bound to the internal
session ID, user ID, and workspace as associated data.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
import uuid
from dataclasses import dataclass

from sqlalchemy import delete, or_, select, update

from omnigent.db.db_models import SqlOidcSession, current_workspace_id
from omnigent.db.query_context import query_name_scope
from omnigent.stores.credential_store.local_aes_cipher import (
    LocalAesGcmSecretCipher,
    build_oidc_credential_cipher_from_env,
)
from omnigent.stores.credential_store.secret_cipher import SecretCipher

_logger = logging.getLogger(__name__)

_HANDLE_PREFIX = "sess_"
_KEY_SIZE = 32  # AES-256


@dataclass(frozen=True, repr=False)
class OidcSessionCredentials:
    """A decrypted credential snapshot plus non-secret refresh metadata.

    This value is deliberately not printable because it contains the ID and
    refresh tokens. Its ``credential_version`` is the compare-and-swap value
    used to coordinate a refresh across processes.
    """

    id_token: str
    refresh_token: str | None
    id_token_expiry: int
    absolute_expiry: int
    provider_subject: str | None
    provider_issuer: str | None
    provider_client_id: str | None
    credential_version: int
    refresh_lease_expires_at: int | None


def _generate_handle() -> tuple[str, str]:
    """Generate a random external handle and its digest.

    :returns: ``(handle, handle_digest)`` — the opaque ``sess_…``
        string for the browser/CLI and its SHA-256 hex digest for
        database lookup.
    """
    raw = secrets.token_bytes(32)
    handle = _HANDLE_PREFIX + raw.hex()
    digest = hashlib.sha256(handle.encode("utf-8")).hexdigest()
    return handle, digest


def _credential_context(
    session_id: str,
    user_id: str,
) -> dict[str, str]:
    return {
        "workspace_id": str(current_workspace_id()),
        "oidc_session_id": session_id,
        "user_id": user_id,
    }


@dataclass(frozen=True)
class CreatedOidcSession:
    """Non-secret result of creating a provider credential session."""

    handle: str
    session_id: str


class OidcSessionStore:
    """Encrypted OIDC provider session store backed by ``SqlOidcSession``.

    All operations are synchronous and take a SQLAlchemy session factory
    (``sessionmaker``) so the caller controls transaction boundaries.
    """

    def __init__(
        self,
        session_factory,
        credential_key: bytes | None = None,
        credential_cipher: SecretCipher | None = None,
    ):
        """Construct the store.

        :param session_factory: A SQLAlchemy ``sessionmaker`` or
            callable returning a ``Session``.
        :param credential_key: Legacy test-injection shortcut for one AES-256
            key. Production uses the versioned/keyed environment configuration.
        :param credential_cipher: Explicit cipher injection for tests and
            alternate self-hosted key-management adapters.
        """
        if credential_key is not None and credential_cipher is not None:
            raise ValueError("provide either credential_key or credential_cipher, not both")
        self._session_factory = session_factory
        if credential_cipher is not None:
            self._cipher = credential_cipher
        elif credential_key is not None:
            if len(credential_key) != _KEY_SIZE:
                raise ValueError("OIDC credential key must be exactly 32 bytes")
            self._cipher = LocalAesGcmSecretCipher("test", {"test": credential_key})
        else:
            self._cipher = build_oidc_credential_cipher_from_env()

    def _encrypt_credentials(
        self,
        session_id: str,
        user_id: str,
        id_token: str,
        refresh_token: str | None,
    ) -> bytes:
        plaintext = json.dumps(
            {"id_token": id_token, "refresh_token": refresh_token},
            separators=(",", ":"),
        )
        return self._cipher.encrypt(
            plaintext, context=_credential_context(session_id, user_id)
        ).encode("ascii")

    def _decrypt_credentials(
        self, session_id: str, user_id: str, blob: bytes
    ) -> tuple[str, str | None] | None:
        try:
            ciphertext = blob.decode("ascii")
        except UnicodeDecodeError:
            return None
        plaintext = self._cipher.decrypt(
            ciphertext, context=_credential_context(session_id, user_id)
        )
        if plaintext is None:
            _logger.warning(
                "OIDC session credentials cannot be decrypted; reauthentication is required"
            )
            return None
        try:
            data = json.loads(plaintext)
            id_token = data["id_token"]
            refresh_token = data.get("refresh_token")
        except (json.JSONDecodeError, KeyError, TypeError):
            return None
        if not isinstance(id_token, str) or not isinstance(refresh_token, (str, type(None))):
            return None
        return id_token, refresh_token

    def create(
        self,
        user_id: str,
        provider_subject: str | None,
        id_token: str,
        refresh_token: str | None,
        id_token_expiry: int,
        absolute_expiry: int,
        *,
        provider_issuer: str,
        provider_client_id: str,
    ) -> str:
        """Create a new encrypted provider session.

        :returns: The opaque ``sess_…`` handle for the browser/CLI.
        """
        return self.create_with_id(
            user_id,
            provider_subject,
            id_token,
            refresh_token,
            id_token_expiry,
            absolute_expiry,
            provider_issuer=provider_issuer,
            provider_client_id=provider_client_id,
        ).handle

    def create_with_id(
        self,
        user_id: str,
        provider_subject: str | None,
        id_token: str,
        refresh_token: str | None,
        id_token_expiry: int,
        absolute_expiry: int,
        *,
        provider_issuer: str,
        provider_client_id: str,
    ) -> CreatedOidcSession:
        """Create a session and return its opaque handle plus internal ID.

        The ID is only for server-side durable delegation; callers must not
        expose it as a browser credential.
        """
        session_id = uuid.uuid4().hex
        handle, handle_digest = _generate_handle()
        now = int(time.time())
        ciphertext = self._encrypt_credentials(session_id, user_id, id_token, refresh_token)
        with (
            query_name_scope("omnigent.oidc_session_store.create_oidc_session"),
            self._session_factory() as session,
        ):
            row = SqlOidcSession(
                workspace_id=current_workspace_id(),
                id=session_id,
                handle_digest=handle_digest,
                user_id=user_id,
                provider_subject=provider_subject,
                provider_issuer=provider_issuer,
                provider_client_id=provider_client_id,
                credential_ciphertext=ciphertext,
                id_token_expiry=id_token_expiry,
                absolute_expiry=absolute_expiry,
                created_at=now,
                updated_at=now,
                credential_version=0,
                refresh_lease_id=None,
                refresh_lease_expires_at=None,
                revoked_at=None,
            )
            session.add(row)
            session.commit()
        return CreatedOidcSession(handle=handle, session_id=session_id)

    def resolve(self, handle: str) -> tuple[str, str, str] | None:
        """Resolve an opaque handle to (user_id, session_id, provider_subject).

        Returns ``None`` for unknown, revoked, or expired handles.

        :param handle: The ``sess_…`` string from the browser/CLI.
        :returns: ``(user_id, session_id, provider_subject)`` or ``None``.
        """
        if not handle.startswith(_HANDLE_PREFIX):
            return None
        digest = hashlib.sha256(handle.encode("utf-8")).hexdigest()
        now = int(time.time())
        with (
            query_name_scope("omnigent.oidc_session_store.resolve_session_handle"),
            self._session_factory() as session,
        ):
            row = session.execute(
                select(SqlOidcSession).where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.handle_digest == digest,
                )
            ).scalar_one_or_none()
            if row is None or row.revoked_at is not None:
                return None
            if row.absolute_expiry <= now:
                # Credentials are disposable secrets. Expiry observation is a
                # deletion opportunity rather than leaving a dead row behind.
                session.delete(row)
                session.commit()
                return None
            return row.user_id, row.id, row.provider_subject or ""

    def get_credentials(
        self,
        session_id: str,
        user_id: str,
    ) -> tuple[str, str | None, int] | None:
        """Retrieve and decrypt the current ID token, refresh token, and expiry.

        :returns: ``(id_token, refresh_token, id_token_expiry)`` or
            ``None`` when the session is revoked, expired, or the
            ciphertext cannot be decrypted.
        """
        credentials = self.get_refresh_credentials(session_id, user_id)
        if credentials is None:
            return None
        return (
            credentials.id_token,
            credentials.refresh_token,
            credentials.id_token_expiry,
        )

    def get_refresh_credentials(
        self,
        session_id: str,
        user_id: str,
        *,
        now_epoch_seconds: int | None = None,
    ) -> OidcSessionCredentials | None:
        """Retrieve a refresh-ready credential snapshot.

        The snapshot includes a non-secret credential generation and lease
        observation. A caller must claim a lease and compare the generation
        before using its refresh token, rather than holding this read
        transaction open while contacting the IdP.
        """
        now = now_epoch_seconds if now_epoch_seconds is not None else int(time.time())
        with (
            query_name_scope("omnigent.oidc_session_store.select_refresh_credentials"),
            self._session_factory() as session,
        ):
            row = session.execute(
                select(SqlOidcSession).where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.absolute_expiry > now,
                )
            ).scalar_one_or_none()
            if row is None or row.credential_ciphertext is None:
                return None
            result = self._decrypt_credentials(session_id, user_id, row.credential_ciphertext)
            if result is None:
                return None
            id_token, refresh_token = result
            return OidcSessionCredentials(
                id_token=id_token,
                refresh_token=refresh_token,
                id_token_expiry=row.id_token_expiry or 0,
                absolute_expiry=row.absolute_expiry,
                provider_subject=row.provider_subject,
                provider_issuer=row.provider_issuer,
                provider_client_id=row.provider_client_id,
                credential_version=row.credential_version,
                refresh_lease_expires_at=row.refresh_lease_expires_at,
            )

    def try_acquire_refresh_lease(
        self,
        session_id: str,
        user_id: str,
        *,
        expected_credential_version: int,
        lease_id: str,
        now_epoch_seconds: int,
        lease_expires_at: int,
    ) -> bool:
        """Claim the short refresh lease for one credential generation.

        The conditional update is the cross-process single-flight boundary:
        only one caller can claim a non-expired session/version. The lease is
        persisted and committed before the remote refresh, so no database
        transaction is held across provider network latency.
        """
        with (
            query_name_scope("omnigent.oidc_session_store.claim_refresh_lease"),
            self._session_factory() as session,
        ):
            result = session.execute(
                update(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.absolute_expiry > now_epoch_seconds,
                    SqlOidcSession.credential_version == expected_credential_version,
                    or_(
                        SqlOidcSession.refresh_lease_id.is_(None),
                        SqlOidcSession.refresh_lease_expires_at.is_(None),
                        SqlOidcSession.refresh_lease_expires_at <= now_epoch_seconds,
                    ),
                )
                .values(
                    refresh_lease_id=lease_id,
                    refresh_lease_expires_at=lease_expires_at,
                    updated_at=now_epoch_seconds,
                )
            )
            session.commit()
            return result.rowcount == 1

    def bind_provider_identity(
        self,
        session_id: str,
        user_id: str,
        *,
        expected_credential_version: int,
        provider_subject: str,
        provider_issuer: str,
        provider_client_id: str,
        now_epoch_seconds: int,
    ) -> bool:
        """Safely fill the stable binding for a pre-coordination session.

        Only a caller that has independently validated the already encrypted
        ID token may invoke this compatibility path. The conditional update
        neither accepts a conflicting prior binding nor races a credential
        refresh generation.
        """
        with (
            query_name_scope("omnigent.oidc_session_store.bind_provider_identity"),
            self._session_factory() as session,
        ):
            result = session.execute(
                update(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.absolute_expiry > now_epoch_seconds,
                    SqlOidcSession.credential_version == expected_credential_version,
                    or_(
                        SqlOidcSession.provider_subject.is_(None),
                        SqlOidcSession.provider_subject == "",
                        SqlOidcSession.provider_subject == provider_subject,
                    ),
                    or_(
                        SqlOidcSession.provider_issuer.is_(None),
                        SqlOidcSession.provider_issuer == provider_issuer,
                    ),
                    or_(
                        SqlOidcSession.provider_client_id.is_(None),
                        SqlOidcSession.provider_client_id == provider_client_id,
                    ),
                )
                .values(
                    provider_subject=provider_subject,
                    provider_issuer=provider_issuer,
                    provider_client_id=provider_client_id,
                    credential_version=expected_credential_version + 1,
                    updated_at=now_epoch_seconds,
                )
            )
            session.commit()
            return result.rowcount == 1

    def commit_refreshed_credentials(
        self,
        session_id: str,
        user_id: str,
        *,
        expected_credential_version: int,
        lease_id: str,
        id_token: str,
        refresh_token: str | None,
        id_token_expiry: int,
        now_epoch_seconds: int,
    ) -> bool:
        """Commit a lease owner's refresh only if the session is still active.

        Revocation, absolute expiry, a new credential generation, or lease
        replacement all make this conditional update fail. In particular, an
        in-flight refresh cannot resurrect credentials after logout or expiry.
        """
        ciphertext = self._encrypt_credentials(session_id, user_id, id_token, refresh_token)
        with (
            query_name_scope("omnigent.oidc_session_store.commit_refreshed_credentials"),
            self._session_factory() as session,
        ):
            result = session.execute(
                update(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.absolute_expiry > now_epoch_seconds,
                    SqlOidcSession.credential_version == expected_credential_version,
                    SqlOidcSession.refresh_lease_id == lease_id,
                )
                .values(
                    credential_ciphertext=ciphertext,
                    id_token_expiry=id_token_expiry,
                    credential_version=expected_credential_version + 1,
                    refresh_lease_id=None,
                    refresh_lease_expires_at=None,
                    updated_at=now_epoch_seconds,
                )
            )
            session.commit()
            return result.rowcount == 1

    def release_refresh_lease(
        self,
        session_id: str,
        user_id: str,
        *,
        lease_id: str,
        now_epoch_seconds: int,
    ) -> None:
        """Release this caller's lease after a retryable refresh failure."""
        with (
            query_name_scope("omnigent.oidc_session_store.release_refresh_lease"),
            self._session_factory() as session,
        ):
            session.execute(
                update(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.refresh_lease_id == lease_id,
                )
                .values(
                    refresh_lease_id=None,
                    refresh_lease_expires_at=None,
                    updated_at=now_epoch_seconds,
                )
            )
            session.commit()

    def revoke_refresh_lease(
        self,
        session_id: str,
        user_id: str,
        *,
        expected_credential_version: int,
        lease_id: str,
        now_epoch_seconds: int,
    ) -> bool:
        """Revoke only the credentials still owned by this refresh lease.

        A late loser cannot clear a healthy winner's rotated credentials:
        lease and generation must still match before credentials are erased.
        """
        with (
            query_name_scope("omnigent.oidc_session_store.revoke_refresh_lease"),
            self._session_factory() as session,
        ):
            result = session.execute(
                update(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.credential_version == expected_credential_version,
                    SqlOidcSession.refresh_lease_id == lease_id,
                )
                .values(
                    revoked_at=now_epoch_seconds,
                    credential_ciphertext=None,
                    credential_version=expected_credential_version + 1,
                    refresh_lease_id=None,
                    refresh_lease_expires_at=None,
                    updated_at=now_epoch_seconds,
                )
            )
            session.commit()
            return result.rowcount == 1

    def revoke_credentials_at_version(
        self,
        session_id: str,
        user_id: str,
        *,
        expected_credential_version: int,
        now_epoch_seconds: int,
    ) -> bool:
        """Clear a non-refreshable snapshot without racing a newer update."""
        with (
            query_name_scope("omnigent.oidc_session_store.revoke_credentials_at_version"),
            self._session_factory() as session,
        ):
            result = session.execute(
                update(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.credential_version == expected_credential_version,
                )
                .values(
                    revoked_at=now_epoch_seconds,
                    credential_ciphertext=None,
                    credential_version=expected_credential_version + 1,
                    refresh_lease_id=None,
                    refresh_lease_expires_at=None,
                    updated_at=now_epoch_seconds,
                )
            )
            session.commit()
            return result.rowcount == 1

    def update_credentials(
        self,
        session_id: str,
        user_id: str,
        id_token: str,
        refresh_token: str | None,
        id_token_expiry: int,
    ) -> bool:
        """Atomically update the encrypted credentials.

        This legacy helper is not the refresh coordination path; callers that
        refresh a rotating token must use the lease and generation methods
        above. It still rejects revoked and absolutely expired sessions.

        :returns: ``True`` on success, ``False`` if the row was not
            found or already revoked.
        """
        now = int(time.time())
        ciphertext = self._encrypt_credentials(session_id, user_id, id_token, refresh_token)
        with (
            query_name_scope("omnigent.oidc_session_store.update_session_credentials"),
            self._session_factory() as session,
        ):
            row = session.execute(
                select(SqlOidcSession)
                .where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                    SqlOidcSession.user_id == user_id,
                    SqlOidcSession.revoked_at.is_(None),
                    SqlOidcSession.absolute_expiry > now,
                )
                .with_for_update()
            ).scalar_one_or_none()
            if row is None:
                return False
            row.credential_ciphertext = ciphertext
            row.id_token_expiry = id_token_expiry
            row.credential_version += 1
            row.refresh_lease_id = None
            row.refresh_lease_expires_at = None
            row.updated_at = now
            session.commit()
        return True

    def revoke(self, session_id: str) -> bool:
        """Mark a session revoked and erase its credential ciphertext.

        :returns: ``True`` if a row was updated, ``False`` if not found.
        """
        now = int(time.time())
        with (
            query_name_scope("omnigent.oidc_session_store.revoke_session"),
            self._session_factory() as session,
        ):
            row = session.execute(
                select(SqlOidcSession).where(
                    SqlOidcSession.workspace_id == current_workspace_id(),
                    SqlOidcSession.id == session_id,
                )
            ).scalar_one_or_none()
            if row is None:
                return False
            row.revoked_at = now
            row.credential_ciphertext = None
            row.credential_version += 1
            row.refresh_lease_id = None
            row.refresh_lease_expires_at = None
            row.updated_at = now
            session.commit()
        return True

    def delete_expired_batch(self, now: int | None = None, limit: int = 100) -> int:
        """Delete at most ``limit`` expired or revoked disposable sessions."""
        if limit <= 0:
            raise ValueError("limit must be positive")
        now = int(time.time()) if now is None else now
        with (
            query_name_scope("omnigent.oidc_session_store.delete_expired_sessions"),
            self._session_factory() as session,
        ):
            ids = (
                session.execute(
                    select(SqlOidcSession.id)
                    .where(
                        SqlOidcSession.workspace_id == current_workspace_id(),
                        (SqlOidcSession.absolute_expiry <= now)
                        | (SqlOidcSession.revoked_at.is_not(None)),
                    )
                    .order_by(SqlOidcSession.absolute_expiry, SqlOidcSession.id)
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            if ids:
                session.execute(
                    delete(SqlOidcSession).where(
                        SqlOidcSession.workspace_id == current_workspace_id(),
                        SqlOidcSession.id.in_(ids),
                    )
                )
            session.commit()
        return len(ids)

    def delete_expired(self) -> int:
        """Compatibility wrapper for one bounded expiry-maintenance batch."""
        return self.delete_expired_batch()
