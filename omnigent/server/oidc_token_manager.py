"""On-demand OIDC ID token refresh without extending the absolute session.

Returns the current signed ID token for a provider session, refreshing
it from the IdP's token endpoint only when the cached token is about
to expire. Refresh never extends the provider session's absolute
expiry; a refresh failure clears credentials and signals reauthentication.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

from omnigent.server.oidc import OIDCConfig
from omnigent.server.oidc_session_store import OidcSessionStore
from omnigent.server.routes.auth import _extract_verified_email, _validate_oidc_id_token

_logger = logging.getLogger(__name__)

# Refresh when the ID token has less than this many seconds remaining.
_REFRESH_MARGIN_SECONDS = 60


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
        clock: object | None = None,
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
        now = int(self._clock())

        creds = self._store.get_credentials(oidc_session_id, expected_user_id)
        if creds is None:
            raise ReauthenticationError("Session not found or expired")

        id_token, refresh_token, id_token_expiry = creds

        # Return the cached token if it has enough remaining lifetime.
        if id_token and id_token_expiry - now > _REFRESH_MARGIN_SECONDS:
            return IdTokenResult(id_token=id_token, expiry=id_token_expiry)

        # Need to refresh.
        if not refresh_token:
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("No refresh token available")

        new_id_token, _new_refresh_token, new_expiry = self._refresh(
            oidc_session_id, expected_user_id, refresh_token
        )

        return IdTokenResult(id_token=new_id_token, expiry=new_expiry)

    def _refresh(
        self,
        oidc_session_id: str,
        expected_user_id: str,
        refresh_token: str,
    ) -> tuple[str, str | None, int]:
        """Refresh the ID token from the IdP's token endpoint.

        :returns: ``(new_id_token, new_refresh_token, new_expiry)``.
        :raises ReauthenticationError: On any refresh failure.
        """
        token_data = {
            "grant_type": "refresh_token",
            "client_id": self._config.client_id,
            "refresh_token": refresh_token,
        }
        if self._config.client_secret is not None:
            token_data["client_secret"] = self._config.client_secret

        try:
            resp = httpx.post(
                self._config.token_endpoint,
                data=token_data,
                timeout=10.0,
            )
        except Exception as exc:
            _logger.warning("Token refresh request failed: %s", exc)
            raise ReauthenticationError("Token refresh request failed") from exc

        if resp.status_code != 200:
            _logger.warning("Token refresh failed: %d", resp.status_code)
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("Token refresh rejected by IdP")

        try:
            token_json = resp.json()
        except ValueError as exc:
            _logger.warning("Token refresh returned non-JSON response")
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("Token refresh returned invalid response") from exc

        if not isinstance(token_json, dict):
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("Token refresh returned invalid response")

        # Validate the refreshed ID token.
        claims = _validate_oidc_id_token(token_json, self._config)
        if claims is None:
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("Refreshed ID token validation failed")

        # Verify the identity hasn't changed.
        email = _extract_verified_email(claims, self._config)
        if email is None or email.lower() != expected_user_id.lower():
            _logger.warning("Refreshed ID token identity mismatch")
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("Identity changed during refresh")

        new_id_token = token_json.get("id_token")
        if not isinstance(new_id_token, str) or not new_id_token:
            self._store.revoke(oidc_session_id)
            raise ReauthenticationError("Refresh response missing ID token")

        new_refresh_token = token_json.get("refresh_token")
        if not isinstance(new_refresh_token, str) or not new_refresh_token:
            new_refresh_token = None  # Keep the old one if not rotated

        new_expiry = claims.get("exp", 0)
        if not isinstance(new_expiry, int):
            new_expiry = int(new_expiry) if new_expiry else 0

        # Atomically update the stored credentials. The store's
        # compare-and-swap prevents concurrent refresh races.
        updated = self._store.update_credentials(
            oidc_session_id,
            expected_user_id,
            new_id_token,
            new_refresh_token or refresh_token,
            new_expiry,
        )
        if not updated:
            raise ReauthenticationError("Session was revoked during refresh")

        return new_id_token, new_refresh_token, new_expiry
