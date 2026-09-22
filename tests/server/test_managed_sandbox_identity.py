"""Managed-operation identity is request-bound and fail-closed."""

from __future__ import annotations

import pytest
from starlette.requests import Request

from omnigent.onboarding.sandboxes.context import IdentityToken
from omnigent.onboarding.sandboxes.types import ManagedIdentityRequirement
from omnigent.server.managed_sandbox_identity import (
    ManagedSandboxIdentityNotSupported,
    ManagedSandboxIdentityUnavailable,
    context_for_managed_sandbox_operation,
)


class _TokenProvider:
    def get_identity_token(self) -> IdentityToken:
        return IdentityToken(value="test-token", expires_at=2_000_000_000)


class _RequestAuth:
    supports_oidc_identity_tokens = True

    def __init__(self, result: _TokenProvider | None) -> None:
        self.result = result
        self.calls: list[str] = []

    def get_identity_token_provider(
        self, request: Request, expected_user_id: str
    ) -> _TokenProvider | None:
        self.calls.append(expected_user_id)
        return self.result


def _request() -> Request:
    return Request({"type": "http", "headers": []})


def test_create_without_identity_requirement_does_not_delegate_user_credentials() -> None:
    auth = _RequestAuth(_TokenProvider())
    context = context_for_managed_sandbox_operation(
        _request(),
        auth,  # type: ignore[arg-type]
        session_id="conv-a",
        owner="alice@example.com",
    )
    assert context.identity_token_provider is None
    assert auth.calls == []


def test_create_requires_a_request_bound_oidc_identity_before_side_effects() -> None:
    auth = _RequestAuth(_TokenProvider())
    context = context_for_managed_sandbox_operation(
        _request(),
        auth,  # type: ignore[arg-type]
        session_id="conv-a",
        owner="alice@example.com",
        requirement=ManagedIdentityRequirement.OIDC_USER,
    )
    assert context.identity_token_provider is auth.result
    assert auth.calls == ["alice@example.com"]


def test_create_fails_closed_when_auth_mode_cannot_delegate_oidc() -> None:
    with pytest.raises(ManagedSandboxIdentityNotSupported, match="cannot provide"):
        context_for_managed_sandbox_operation(
            _request(),
            None,
            session_id="conv-a",
            owner="alice@example.com",
            requirement=ManagedIdentityRequirement.OIDC_USER,
        )


def test_create_requires_reauthentication_when_request_has_no_oidc_session() -> None:
    with pytest.raises(ManagedSandboxIdentityUnavailable, match="reauthentication"):
        context_for_managed_sandbox_operation(
            _request(),
            _RequestAuth(None),  # type: ignore[arg-type]
            session_id="conv-a",
            owner="alice@example.com",
            requirement=ManagedIdentityRequirement.OIDC_USER,
        )


def test_operation_context_has_no_durable_credential_session_pointer() -> None:
    context = context_for_managed_sandbox_operation(
        _request(),
        _RequestAuth(_TokenProvider()),  # type: ignore[arg-type]
        session_id="conv-a",
        owner="alice@example.com",
        requirement=ManagedIdentityRequirement.OIDC_USER,
    )

    assert context.user_id == "alice@example.com"
    assert not hasattr(context, "credential_session_id")
