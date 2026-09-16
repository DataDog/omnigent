"""Lifecycle identity resolution is durable, owner-bound, and fail-closed."""

from __future__ import annotations

import pytest

from omnigent.onboarding.sandboxes.context import IdentityToken
from omnigent.server.managed_sandbox_identity import (
    ManagedSandboxIdentityResolver,
    ManagedSandboxIdentityUnavailable,
)
from omnigent.stores.host_store import Host


class _TokenProvider:
    credential_session_id = "oidc-session-alice"

    def get_identity_token(self) -> IdentityToken:
        return IdentityToken(value="test-token", expires_at=2_000_000_000)


class _Auth:
    def __init__(self, result: _TokenProvider | None) -> None:
        self.result = result
        self.calls: list[tuple[str, str]] = []

    def get_identity_token_provider_for_credential_session(
        self, credential_session_id: str, expected_user_id: str
    ) -> _TokenProvider | None:
        self.calls.append((credential_session_id, expected_user_id))
        return self.result


def _host(
    *, owner: str = "alice@example.com", credential_session_id: str | None = "oidc-session-alice"
) -> Host:
    return Host(
        host_id="host-a",
        name="managed-a",
        user_id=owner,
        status="offline",
        created_at=1,
        updated_at=1,
        sandbox_provider="hab",
        sandbox_id="hab-exact-uuid",
        sandbox_session_id="conv-a",
        sandbox_credential_session_id=credential_session_id,
    )


def test_later_operation_resolves_the_exact_owner_credential_session() -> None:
    auth = _Auth(_TokenProvider())
    context = ManagedSandboxIdentityResolver(auth).for_host(_host())

    assert auth.calls == [("oidc-session-alice", "alice@example.com")]
    assert context.session_id == "conv-a"
    assert context.user_id == "alice@example.com"
    assert context.credential_session_id == "oidc-session-alice"


def test_missing_or_revoked_owner_credential_fails_closed() -> None:
    auth = _Auth(None)
    with pytest.raises(ManagedSandboxIdentityUnavailable, match="reauthentication"):
        ManagedSandboxIdentityResolver(auth).for_host(_host())
    with pytest.raises(ManagedSandboxIdentityUnavailable, match="reauthentication"):
        ManagedSandboxIdentityResolver(_Auth(_TokenProvider())).for_host(
            _host(credential_session_id=None)
        )


def test_second_user_cannot_substitute_their_credential_for_the_owner() -> None:
    auth = _Auth(_TokenProvider())
    ManagedSandboxIdentityResolver(auth).for_host(_host(owner="alice@example.com"))

    assert auth.calls == [("oidc-session-alice", "alice@example.com")]
