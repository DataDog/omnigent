"""Owner-bound, non-secret identity contexts for managed sandbox operations."""

from __future__ import annotations

from dataclasses import dataclass

from starlette.requests import Request

from omnigent.db.db_models import InvalidUuidError, uuid_to_bytes
from omnigent.onboarding.sandboxes.context import IdentityTokenProvider, ManagedSandboxContext
from omnigent.onboarding.sandboxes.types import ManagedIdentityRequirement
from omnigent.server.auth import RESERVED_USER_LOCAL, AuthProvider
from omnigent.stores.host_store import Host


class ManagedSandboxIdentityUnavailable(RuntimeError):
    """The owner credential required for a lifecycle operation is unavailable."""


class ManagedSandboxIdentityNotSupported(RuntimeError):
    """The active authentication mode cannot provide a required OIDC identity."""


def _credential_session_id(provider: IdentityTokenProvider | None) -> str | None:
    value = getattr(provider, "credential_session_id", None)
    return value if isinstance(value, str) and value else None


def _canonical_credential_session_id(value: object) -> str | None:
    """Return the canonical OIDC session UUID, or ``None`` for stale bindings."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return uuid_to_bytes(value).hex()
    except InvalidUuidError:
        return None


def context_for_managed_sandbox_create(
    request: Request,
    auth_provider: AuthProvider | None,
    *,
    session_id: str,
    owner: str,
    requirement: ManagedIdentityRequirement = ManagedIdentityRequirement.NONE,
) -> ManagedSandboxContext:
    """Build a request-bound create context, rejecting required OIDC early."""
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
        credential_session_id=_credential_session_id(provider),
    )


@dataclass(frozen=True)
class ManagedSandboxIdentityResolver:
    """Resolve a durable lifecycle binding without retaining bearer material."""

    auth_provider: AuthProvider | None

    def for_host(self, host: Host) -> ManagedSandboxContext:
        credential_session_id = _canonical_credential_session_id(
            getattr(host, "sandbox_credential_session_id", None)
        )
        if credential_session_id is None and host.user_id == RESERVED_USER_LOCAL:
            return ManagedSandboxContext(
                session_id=getattr(host, "sandbox_session_id", None)
                or getattr(host, "host_id", "managed"),
                user_id=host.user_id,
                identity_token_provider=None,
                credential_session_id=None,
            )
        if credential_session_id is None:
            raise ManagedSandboxIdentityUnavailable(
                "owner reauthentication is required before this managed sandbox can be operated"
            )
        # TODO(POC): Rebind after verified owner reauthentication; today an
        # expired original credential session permanently strands the sandbox.
        provider = (
            self.auth_provider.get_identity_token_provider_for_credential_session(
                credential_session_id, host.user_id
            )
            if self.auth_provider is not None
            else None
        )
        if provider is None:
            raise ManagedSandboxIdentityUnavailable(
                "owner reauthentication is required before this managed sandbox can be operated"
            )
        return ManagedSandboxContext(
            session_id=getattr(host, "sandbox_session_id", None)
            or getattr(host, "host_id", "managed"),
            user_id=host.user_id,
            identity_token_provider=provider,
            credential_session_id=credential_session_id,
        )
