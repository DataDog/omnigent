"""Shared peer and token checks for runner tunnel requests."""

from __future__ import annotations

from collections.abc import Mapping
from ipaddress import ip_address

from omnigent.runner.identity import RUNNER_TUNNEL_TOKEN_HEADER, token_bound_runner_id


def is_loopback_peer_host(host: str | None) -> bool:
    """Return whether a request peer is a loopback address."""
    if host == "localhost":
        return True
    if not host:
        return False
    try:
        addr = ip_address(host)
    except ValueError:
        return False
    mapped_ipv4 = getattr(addr, "ipv4_mapped", None)
    return addr.is_loopback or (mapped_ipv4 is not None and mapped_ipv4.is_loopback)


def expected_runner_id_from_headers(
    headers: Mapping[str, str],
    *,
    allowed_tunnel_tokens: frozenset[str] | None = None,
) -> str | None:
    """Return the token-bound runner ID, or ``None`` for an allowlisted token."""
    token = headers.get(RUNNER_TUNNEL_TOKEN_HEADER)
    if token is None:
        if allowed_tunnel_tokens is not None:
            raise ValueError("runner tunnel token is required")
        return None
    stripped = token.strip()
    if not stripped:
        raise ValueError("runner tunnel token must not be empty")
    if allowed_tunnel_tokens is not None:
        if stripped not in allowed_tunnel_tokens:
            raise ValueError("runner tunnel token is not authorized")
        return None
    return token_bound_runner_id(stripped)


def expected_runner_id_for_peer(
    headers: Mapping[str, str],
    *,
    allowed_tunnel_tokens: frozenset[str] | None,
    is_loopback: bool,
) -> str | None:
    """Apply the tunnel allow-list with the established loopback fallback."""
    try:
        expected = expected_runner_id_from_headers(
            headers, allowed_tunnel_tokens=allowed_tunnel_tokens
        )
    except ValueError:
        if not is_loopback:
            raise
        # External CLI runners can use their token-bound identity from localhost
        # even when the server also has an allow-list for managed runners.
        expected = expected_runner_id_from_headers(headers)
    if expected is None and allowed_tunnel_tokens is None and not is_loopback:
        raise ValueError("runner tunnel token is required")
    return expected


def validate_runner_token_for_declared_runner(
    headers: Mapping[str, str],
    *,
    declared_runner_id: str,
    allowed_tunnel_tokens: frozenset[str] | None,
    is_loopback: bool,
) -> str:
    """Require a token authorized for the peer and bound to the declared runner."""
    token = (headers.get(RUNNER_TUNNEL_TOKEN_HEADER) or "").strip()
    if not token:
        raise ValueError("runner tunnel token is required")
    expected = expected_runner_id_for_peer(
        headers,
        allowed_tunnel_tokens=allowed_tunnel_tokens,
        is_loopback=is_loopback,
    )
    if expected is not None and expected != declared_runner_id:
        raise ValueError("runner token does not match the declared runner")
    return declared_runner_id
