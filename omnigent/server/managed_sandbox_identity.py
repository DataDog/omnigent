"""Owner-bound, non-secret identity contexts for managed sandbox operations."""

from __future__ import annotations

from starlette.requests import Request

from omnigent.onboarding.sandboxes.context import ManagedSandboxContext
from omnigent.onboarding.sandboxes.types import ManagedIdentityRequirement
from omnigent.server.auth import AuthProvider


class ManagedSandboxIdentityUnavailable(RuntimeError):
    """The owner credential required for a lifecycle operation is unavailable."""


class ManagedSandboxIdentityNotSupported(RuntimeError):
    """The active authentication mode cannot provide a required OIDC identity."""


def context_for_managed_sandbox_operation(
    request: Request,
    auth_provider: AuthProvider | None,
    *,
    session_id: str,
    owner: str,
    requirement: ManagedIdentityRequirement = ManagedIdentityRequirement.NONE,
) -> ManagedSandboxContext:
    """Capture authority for one request-bound managed operation.

    The context is deliberately passed to the background task rather than
    persisted on the durable host.  A later resume/relaunch must therefore be
    initiated by a currently authenticated owner.
    """
    if requirement is ManagedIdentityRequirement.NONE:
        provider = None
    elif requirement is ManagedIdentityRequirement.OIDC_USER:
        if auth_provider is None or not auth_provider.supports_oidc_identity_tokens:
            raise ManagedSandboxIdentityNotSupported(
                "this sandbox provider requires an OIDC identity token, but the active "
                "authentication mode cannot provide one"
            )
        provider = auth_provider.get_identity_token_provider(request, expected_user_id=owner)
        if provider is None:
            raise ManagedSandboxIdentityUnavailable(
                "reauthentication is required before this managed sandbox can be created"
            )
    else:
        raise ManagedSandboxIdentityNotSupported(
            f"unsupported managed sandbox identity requirement: {requirement!r}"
        )
    return ManagedSandboxContext(
        session_id=session_id,
        user_id=owner,
        identity_token_provider=provider,
    )


# Compatibility alias for integrations during the request-context migration.
context_for_managed_sandbox_create = context_for_managed_sandbox_operation
