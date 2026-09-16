"""Tests for :mod:`omnigent.onboarding.sandboxes.context`.

Red tests for the ambient managed-sandbox context primitive. The production
module does not exist yet, so these fail at collection with ImportError.
"""

from __future__ import annotations

import asyncio
import dataclasses

import pytest

from omnigent.onboarding.sandboxes.context import (
    IdentityToken,
    IdentityTokenProvider,
    ManagedSandboxContext,
    current_managed_sandbox_context,
    managed_sandbox_context_scope,
)

SECRET_TOKEN_VALUE = "SECRET-TOKEN-XYZ"


class _StaticIdentityTokenProvider:
    """Structural stand-in for ``IdentityTokenProvider``."""

    def __init__(self, value: str = SECRET_TOKEN_VALUE, expires_at: int = 1234567890):
        self._token = IdentityToken(value=value, expires_at=expires_at)

    def get_identity_token(self) -> IdentityToken:
        return self._token


def _make_context(
    session_id: str = "sess-1",
    user_id: str = "user-1",
    provider: IdentityTokenProvider | None = None,
) -> ManagedSandboxContext:
    if provider is None:
        provider = _StaticIdentityTokenProvider()
    return ManagedSandboxContext(
        session_id=session_id,
        user_id=user_id,
        identity_token_provider=provider,
    )


def test_current_context_is_none_by_default() -> None:
    """Outside any scope, the ambient context is None."""
    assert current_managed_sandbox_context() is None


def test_scope_exposes_context_and_restores_previous_value() -> None:
    """A scope makes its context current, then restores the prior value."""
    ctx = _make_context()
    assert current_managed_sandbox_context() is None
    with managed_sandbox_context_scope(ctx):
        assert current_managed_sandbox_context() is ctx
    assert current_managed_sandbox_context() is None


def test_nested_scopes_restore_correctly() -> None:
    """Exiting an inner scope restores the outer scope's context."""
    outer = _make_context(session_id="outer-sess", user_id="outer-user")
    inner = _make_context(session_id="inner-sess", user_id="inner-user")
    with managed_sandbox_context_scope(outer):
        with managed_sandbox_context_scope(inner):
            assert current_managed_sandbox_context() is inner
        assert current_managed_sandbox_context() is outer
    assert current_managed_sandbox_context() is None


async def test_concurrent_tasks_do_not_see_each_other() -> None:
    """Two tasks running in different scopes only observe their own context."""
    ctx_a = _make_context(session_id="sess-a", user_id="user-a")
    ctx_b = _make_context(session_id="sess-b", user_id="user-b")
    seen: dict[str, ManagedSandboxContext | None] = {}

    async def runner(ctx: ManagedSandboxContext, key: str) -> None:
        with managed_sandbox_context_scope(ctx):
            # Yield so both tasks are inside their scopes simultaneously.
            await asyncio.sleep(0.05)
            seen[key] = current_managed_sandbox_context()

    await asyncio.gather(
        asyncio.create_task(runner(ctx_a, "a")),
        asyncio.create_task(runner(ctx_b, "b")),
    )
    assert seen["a"] is ctx_a
    assert seen["b"] is ctx_b


async def test_task_created_in_scope_retains_context_after_parent_exits() -> None:
    """A task created inside a scope keeps the copied context after exit."""
    ctx = _make_context(session_id="sess-child", user_id="user-child")
    release = asyncio.Event()
    observed: dict[str, object] = {}

    async def child() -> None:
        await release.wait()
        observed["context"] = current_managed_sandbox_context()

    with managed_sandbox_context_scope(ctx):
        task = asyncio.create_task(child())

    assert current_managed_sandbox_context() is None
    release.set()
    await task
    assert observed["context"] is ctx


async def test_to_thread_propagates_task_context() -> None:
    """``asyncio.to_thread`` sees the calling task's ambient context."""
    ctx = _make_context(session_id="sess-thread", user_id="user-thread")
    with managed_sandbox_context_scope(ctx):
        direct = await asyncio.to_thread(current_managed_sandbox_context)
    assert direct is ctx

    # A task spawned in a scope still propagates through to_thread after the
    # parent scope exits.
    release = asyncio.Event()
    from_thread: dict[str, object] = {}

    async def child() -> None:
        await release.wait()
        from_thread["context"] = await asyncio.to_thread(current_managed_sandbox_context)

    with managed_sandbox_context_scope(ctx):
        task = asyncio.create_task(child())
    assert current_managed_sandbox_context() is None
    release.set()
    await task
    assert from_thread["context"] is ctx


def test_token_material_absent_from_repr() -> None:
    """Neither token values nor providers appear in generated reprs."""
    token = IdentityToken(value=SECRET_TOKEN_VALUE, expires_at=1234567890)
    assert SECRET_TOKEN_VALUE not in repr(token)

    provider = _StaticIdentityTokenProvider()
    ctx = ManagedSandboxContext(
        session_id="sess-repr",
        user_id="user-repr",
        identity_token_provider=provider,
    )
    ctx_repr = repr(ctx)
    assert SECRET_TOKEN_VALUE not in ctx_repr
    assert "StaticIdentityTokenProvider" not in ctx_repr


def test_context_and_token_are_frozen() -> None:
    """Both dataclasses reject attribute mutation."""
    ctx = _make_context()
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.session_id = "mutated"

    token = IdentityToken(value=SECRET_TOKEN_VALUE, expires_at=1234567890)
    with pytest.raises(dataclasses.FrozenInstanceError):
        token.expires_at = 0
