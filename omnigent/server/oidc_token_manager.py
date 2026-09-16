"""On-demand OIDC ID token refresh without extending the absolute session.

Returns the current signed ID token for a provider session, refreshing
it from the IdP's token endpoint only when the cached token is about
to expire. Refresh never extends the provider session's absolute
expiry; invalid credentials signal reauthentication without letting a
stale caller clear a newer credential generation.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import httpx

from omnigent.server.oidc import OIDCConfig
from omnigent.server.oidc_session_store import OidcSessionCredentials, OidcSessionStore
from omnigent.server.routes.auth import _extract_verified_email, _validate_oidc_id_token

_logger = logging.getLogger(__name__)

# Refresh when the ID token has less than this many seconds remaining.
_REFRESH_MARGIN_SECONDS = 60
_REFRESH_REQUEST_TIMEOUT_SECONDS = 10.0
# This exceeds the bounded refresh request timeout so another process cannot
# consume a rotating refresh token while a healthy owner is still waiting on
# the IdP. A crashed owner becomes recoverable without a transaction spanning
# the remote call.
_REFRESH_LEASE_SECONDS = 30
_REFRESH_WAIT_SECONDS = 0.05
# OAuth token endpoints use ``invalid_grant`` when the refresh token has
# expired, been revoked, or was already consumed. A few providers use the
# equivalent non-standard spelling. Other HTTP failures do not prove that the
# stored credential is invalid, so they must leave it available for retry.
_PERMANENT_REFRESH_ERROR_CODES = frozenset({"invalid_grant", "invalid_refresh_token"})


class ReauthenticationError(Exception):
    """The user must sign in again; credentials have been cleared."""

    def __init__(self, message: str = "Sign in again") -> None:
        super().__init__(message)


@dataclass(frozen=True)
class IdTokenResult:
    """The current signed ID token and its expiry timestamp.

    :param id_token: The signed JWT ID token string.
    :param expiry: Unix timestamp when the ID token expires.
    """

    id_token: str
    expiry: int


class OidcTokenManager:
    """Manages on-demand ID token refresh for encrypted provider sessions.

    :param session_store: The encrypted OIDC session store.
    :param config: The OIDC configuration (issuer, audience, JWKS URI,
        token endpoint, client_id).
    :param clock: Callable returning the current Unix timestamp.
        Defaults to :func:`time.time`.
    """

    def __init__(
        self,
        session_store: OidcSessionStore,
        config: OIDCConfig,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._store = session_store
        self._config = config
        self._clock = clock or time.time

    def get_current_id_token(
        self,
        oidc_session_id: str,
        expected_user_id: str,
    ) -> IdTokenResult:
        """Return the current signed ID token, refreshing on demand.

        :param oidc_session_id: Internal session ID from the store.
        :param expected_user_id: The verified user email; the
            refreshed ID token must match.
        :returns: An :class:`IdTokenResult` with the current ID token.
        :raises ReauthenticationError: When the session is revoked,
            expired, the refresh fails, or the identity changed.
        """
        wait_deadline = time.monotonic() + _REFRESH_LEASE_SECONDS
        while True:
            now = int(self._clock())
            credentials = self._store.get_refresh_credentials(
                oidc_session_id,
                expected_user_id,
                now_epoch_seconds=now,
            )
            if credentials is None:
                raise ReauthenticationError("Session not found or expired")
            credentials = self._ensure_stable_provider_binding(
                oidc_session_id,
                expected_user_id,
                credentials,
                now,
            )

            # Return the cached token if it has enough remaining lifetime.
            if (
                credentials.id_token
                and credentials.id_token_expiry - now > _REFRESH_MARGIN_SECONDS
            ):
                return IdTokenResult(
                    id_token=credentials.id_token,
                    expiry=credentials.id_token_expiry,
                )

            if not credentials.refresh_token:
                self._store.revoke_credentials_at_version(
                    oidc_session_id,
                    expected_user_id,
                    expected_credential_version=credentials.credential_version,
                    now_epoch_seconds=now,
                )
                raise ReauthenticationError("No refresh token available")

            lease_id = uuid.uuid4().hex
            if self._store.try_acquire_refresh_lease(
                oidc_session_id,
                expected_user_id,
                expected_credential_version=credentials.credential_version,
                lease_id=lease_id,
                now_epoch_seconds=now,
                lease_expires_at=now + _REFRESH_LEASE_SECONDS,
            ):
                return self._refresh(
                    oidc_session_id,
                    expected_user_id,
                    credentials,
                    lease_id,
                )

            # Another process may have consumed a rotating refresh token. Do
            # not revoke based on this stale snapshot: reread the winner's
            # generation instead. The timeout bounds a stale crashed lease.
            if time.monotonic() >= wait_deadline:
                raise ReauthenticationError("Token refresh is already in progress")
            time.sleep(_REFRESH_WAIT_SECONDS)

    def _refresh(
        self,
        oidc_session_id: str,
        expected_user_id: str,
        credentials: OidcSessionCredentials,
        lease_id: str,
    ) -> IdTokenResult:
        """Refresh the ID token from the IdP's token endpoint.

        :returns: The refreshed ID token and expiry.
        :raises ReauthenticationError: On any refresh failure.
        """
        token_data = {
            "grant_type": "refresh_token",
            "client_id": self._config.client_id,
            "refresh_token": credentials.refresh_token,
        }
        if self._config.client_secret is not None:
            token_data["client_secret"] = self._config.client_secret

        try:
            resp = httpx.post(
                self._config.token_endpoint,
                data=token_data,
                timeout=_REFRESH_REQUEST_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            self._store.release_refresh_lease(
                oidc_session_id,
                expected_user_id,
                lease_id=lease_id,
                now_epoch_seconds=int(self._clock()),
            )
            _logger.warning("Token refresh request failed")
            raise ReauthenticationError("Token refresh request failed") from exc

        if resp.status_code != 200:
            _logger.warning("Token refresh failed: %d", resp.status_code)
            if _is_permanent_refresh_rejection(resp):
                self._revoke_owned_refresh(
                    oidc_session_id,
                    expected_user_id,
                    credentials,
                    lease_id,
                )
            else:
                self._store.release_refresh_lease(
                    oidc_session_id,
                    expected_user_id,
                    lease_id=lease_id,
                    now_epoch_seconds=int(self._clock()),
                )
            raise ReauthenticationError("Token refresh failed; retry the request")

        try:
            token_json = resp.json()
        except ValueError as exc:
            _logger.warning("Token refresh returned non-JSON response")
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Token refresh returned invalid response") from exc

        if not isinstance(token_json, dict):
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Token refresh returned invalid response")

        # Validate the refreshed ID token.
        claims = _validate_oidc_id_token(token_json, self._config)
        if claims is None:
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Refreshed ID token validation failed")

        # Verify the identity hasn't changed.
        email = _extract_verified_email(claims, self._config)
        if email is None or email.lower() != expected_user_id.lower():
            _logger.warning("Refreshed ID token identity mismatch")
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Identity changed during refresh")

        refreshed_subject = claims.get("sub")
        if (
            not isinstance(refreshed_subject, str)
            or not refreshed_subject
            or refreshed_subject != credentials.provider_subject
        ):
            _logger.warning("Refreshed ID token subject mismatch")
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Identity changed during refresh")

        new_id_token = token_json.get("id_token")
        if not isinstance(new_id_token, str) or not new_id_token:
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Refresh response missing ID token")

        new_refresh_token = token_json.get("refresh_token")
        if not isinstance(new_refresh_token, str) or not new_refresh_token:
            new_refresh_token = credentials.refresh_token  # Keep the old one if not rotated

        raw_expiry = claims.get("exp", 0)
        try:
            if isinstance(raw_expiry, bool):
                raise ValueError("boolean NumericDate")
            new_expiry = int(cast("float | str", raw_expiry)) if raw_expiry else 0
        except (TypeError, ValueError) as exc:
            self._revoke_owned_refresh(
                oidc_session_id,
                expected_user_id,
                credentials,
                lease_id,
            )
            raise ReauthenticationError("Refreshed ID token has an invalid expiry") from exc

        updated = self._store.commit_refreshed_credentials(
            oidc_session_id,
            expected_user_id,
            expected_credential_version=credentials.credential_version,
            lease_id=lease_id,
            id_token=new_id_token,
            refresh_token=new_refresh_token,
            id_token_expiry=new_expiry,
            now_epoch_seconds=int(self._clock()),
        )
        if not updated:
            # Revocation, expiry, or a lease recovery can race the remote
            # request. A new healthy generation is safe to return; otherwise
            # fail closed without clearing someone else's credentials.
            winner = self._store.get_refresh_credentials(
                oidc_session_id,
                expected_user_id,
                now_epoch_seconds=int(self._clock()),
            )
            if (
                winner is not None
                and winner.id_token_expiry - int(self._clock()) > _REFRESH_MARGIN_SECONDS
                and self._has_stable_provider_binding(winner)
            ):
                return IdTokenResult(id_token=winner.id_token, expiry=winner.id_token_expiry)
            raise ReauthenticationError("Session was revoked during refresh")

        return IdTokenResult(id_token=new_id_token, expiry=new_expiry)

    def _ensure_stable_provider_binding(
        self,
        oidc_session_id: str,
        expected_user_id: str,
        credentials: OidcSessionCredentials,
        now_epoch_seconds: int,
    ) -> OidcSessionCredentials:
        """Validate and bind a legacy session before it can refresh.

        Rows created before the issuer/client columns existed are never trusted
        merely because those fields are null. Their encrypted current ID token
        must still validate for this exact issuer/client, email, and any stored
        subject before a generation-CAS backfill can bind it.
        """
        if self._has_stable_provider_binding(credentials):
            return credentials
        claims = _validate_oidc_id_token({"id_token": credentials.id_token}, self._config)
        email = _extract_verified_email(claims, self._config) if claims is not None else None
        subject = claims.get("sub") if claims is not None else None
        token_expiry = claims.get("exp") if claims is not None else None
        if (
            email is None
            or email.lower() != expected_user_id.lower()
            or not isinstance(subject, str)
            or not subject
            or isinstance(token_expiry, bool)
            or not isinstance(token_expiry, int)
            or (
                credentials.provider_subject is not None
                and credentials.provider_subject != ""
                and credentials.provider_subject != subject
            )
            or (
                credentials.provider_issuer is not None
                and credentials.provider_issuer != self._config.issuer
            )
            or (
                credentials.provider_client_id is not None
                and credentials.provider_client_id != self._config.client_id
            )
        ):
            raise ReauthenticationError("Session is missing its verified provider identity")
        bound = self._store.bind_provider_identity(
            oidc_session_id,
            expected_user_id,
            expected_credential_version=credentials.credential_version,
            provider_subject=subject,
            provider_issuer=self._config.issuer,
            provider_client_id=self._config.client_id,
            now_epoch_seconds=now_epoch_seconds,
        )
        rebound = self._store.get_refresh_credentials(
            oidc_session_id,
            expected_user_id,
            now_epoch_seconds=now_epoch_seconds,
        )
        if rebound is None or not self._has_stable_provider_binding(rebound):
            raise ReauthenticationError("Session changed while verifying provider identity")
        if not bound:
            # A concurrent process may have performed the same verified bind.
            # Use its durable generation, never the stale decrypted snapshot.
            return rebound
        return rebound

    def _has_stable_provider_binding(self, credentials: OidcSessionCredentials) -> bool:
        """Whether the stored binding exactly matches this OIDC client."""
        return (
            bool(credentials.provider_subject)
            and credentials.provider_issuer == self._config.issuer
            and credentials.provider_client_id == self._config.client_id
        )

    def _revoke_owned_refresh(
        self,
        oidc_session_id: str,
        expected_user_id: str,
        credentials: OidcSessionCredentials,
        lease_id: str,
    ) -> None:
        """Clear only credentials still owned by this refresh request."""
        self._store.revoke_refresh_lease(
            oidc_session_id,
            expected_user_id,
            expected_credential_version=credentials.credential_version,
            lease_id=lease_id,
            now_epoch_seconds=int(self._clock()),
        )


def _is_permanent_refresh_rejection(response: httpx.Response) -> bool:
    """Whether a token endpoint explicitly rejected this refresh credential.

    Do not infer this from an HTTP 4xx alone: a timeout, rate limit, or client
    configuration failure is not evidence that the encrypted refresh token is
    unusable. The response body is intentionally neither logged nor retained.
    """
    try:
        payload = response.json()
    except ValueError:
        return False
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    return isinstance(error, str) and error.lower() in _PERMANENT_REFRESH_ERROR_CODES
