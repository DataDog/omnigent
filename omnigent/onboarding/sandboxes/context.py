"""Portable managed-sandbox identity context shared with sandbox-provider wheels."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class IdentityToken:
    value: str = field(repr=False)
    expires_at: int


class IdentityTokenProvider(Protocol):
    def get_identity_token(self) -> IdentityToken: ...


@dataclass(frozen=True)
class ManagedSandboxContext:
    session_id: str
    user_id: str
    identity_token_provider: IdentityTokenProvider | None = field(repr=False)
    # Opaque database reference only; never a cookie, access token, or refresh token.
    credential_session_id: str | None = None


_current_managed_sandbox_context: ContextVar[ManagedSandboxContext | None] = ContextVar(
    "omnigent_managed_sandbox_context", default=None
)


@contextmanager
def managed_sandbox_context_scope(context: ManagedSandboxContext) -> Iterator[None]:
    reset_token = _current_managed_sandbox_context.set(context)
    try:
        yield
    finally:
        _current_managed_sandbox_context.reset(reset_token)


def current_managed_sandbox_context() -> ManagedSandboxContext | None:
    return _current_managed_sandbox_context.get()
