#!/usr/bin/env python3
"""Secret-safe preflight checks for the opt-in real Habitat harness.

This module intentionally validates only local configuration and read-only
network reachability.  It never creates, lists, modifies, or deletes a Hab,
and it never includes a bearer value in an error or receipt.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import ssl
import stat
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

_HABITAT_API = "https://nickisaacs.habvm.dev"
_MIN_TOKEN_TTL_S = 300


def _decode_jwt_claims(value: bytes) -> dict[str, object] | None:
    """Decode untrusted local readiness claims without surfacing the token."""
    try:
        token = value.decode("ascii").strip()
        parts = token.split(".")
        if len(parts) != 3 or not all(parts):
            return None
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None
    return claims if isinstance(claims, dict) else None


def validate_workload_token_file(path_value: str, *, now: float | None = None) -> str | None:
    """Return a safe failure reason, or ``None`` for a usable local file."""
    try:
        path = Path(path_value)
        file_stat = path.lstat()
        if not stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
            return "HAB_WORKLOAD_TOKEN_FILE must be a regular file"
        if file_stat.st_uid != os.getuid():
            return "HAB_WORKLOAD_TOKEN_FILE must be owned by the current user"
        if stat.S_IMODE(file_stat.st_mode) & 0o077:
            return "HAB_WORKLOAD_TOKEN_FILE must not allow group or other access"
        if file_stat.st_nlink != 1:
            return "HAB_WORKLOAD_TOKEN_FILE must not have multiple hard links"
        if not 0 < file_stat.st_size <= 32 * 1024:
            return "HAB_WORKLOAD_TOKEN_FILE has an invalid size"
        value = path.read_bytes()
    except OSError:
        return "HAB_WORKLOAD_TOKEN_FILE is not safely readable"

    claims = _decode_jwt_claims(value)
    if claims is None:
        return "HAB_WORKLOAD_TOKEN_FILE is malformed"
    audience = claims.get("aud")
    if not (audience == "identity" or (isinstance(audience, list) and "identity" in audience)):
        return "HAB_WORKLOAD_TOKEN_FILE has an unexpected audience"
    expires_at = claims.get("exp")
    if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
        return "HAB_WORKLOAD_TOKEN_FILE has no valid expiration"
    if expires_at - (time.time() if now is None else now) < _MIN_TOKEN_TTL_S:
        return "HAB_WORKLOAD_TOKEN_FILE expires too soon for a real run"
    return None


def _is_public_https_url(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.hostname:
        return False
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return False
    hostname = parsed.hostname.lower()
    return hostname not in {"localhost", "127.0.0.1", "::1"} and not hostname.endswith(
        ".localhost"
    )


def validate_real_environment(
    environment: Mapping[str, str], *, port: int, now: float | None = None
) -> list[str]:
    """Return only safe errors for an explicitly opted-in real invocation."""
    errors: list[str] = []
    if environment.get("TICINO_E2E_HABITAT_MODE") != "real":
        errors.append("TICINO_E2E_HABITAT_MODE must be real")
    if environment.get("TICINO_E2E_ALLOW_REAL_HABITAT") != "1":
        errors.append("TICINO_E2E_ALLOW_REAL_HABITAT must be exactly 1")
    if environment.get("HAB_APISERVER") != _HABITAT_API:
        errors.append(f"HAB_APISERVER must be {_HABITAT_API}")
    if environment.get("OMNIGENT_HAB_EXCHANGE_MODE") != "file":
        errors.append("OMNIGENT_HAB_EXCHANGE_MODE must be file")
    token_file = environment.get("HAB_WORKLOAD_TOKEN_FILE", "")
    if not token_file:
        errors.append("HAB_WORKLOAD_TOKEN_FILE is required")
    else:
        token_error = validate_workload_token_file(token_file, now=now)
        if token_error:
            errors.append(token_error)
    profile = environment.get("OMNIGENT_HAB_PROFILE", "")
    if not profile or "fake" in profile.lower():
        errors.append("OMNIGENT_HAB_PROFILE must name a real approved profile")
    image = environment.get("OMNIGENT_HAB_IMAGE", "")
    if "@sha256:" not in image or "fake" in image.lower():
        errors.append("OMNIGENT_HAB_IMAGE must be a non-fake immutable digest reference")
    if environment.get("OMNIGENT_HAB_ALLOW_ALL_EGRESS") != "true":
        errors.append(
            "OMNIGENT_HAB_ALLOW_ALL_EGRESS must be explicitly true for this temporary test"
        )
    public_url = environment.get("OMNIGENT_PUBLIC_URL", "")
    if not _is_public_https_url(public_url):
        errors.append("OMNIGENT_PUBLIC_URL must be a non-loopback HTTPS tunnel origin")
    expected_redirect = f"http://127.0.0.1:{port}/auth/callback"
    if environment.get("OMNIGENT_OIDC_REDIRECT_URI") != expected_redirect:
        errors.append(f"OMNIGENT_OIDC_REDIRECT_URI must be {expected_redirect}")
    return errors


def _check_https(url: str) -> str | None:
    """Verify DNS/TLS/HTTP reachability without exposing response bodies."""
    try:
        with urlopen(url, timeout=5, context=ssl.create_default_context()):
            pass
    except HTTPError:
        # A 401/403/404 still proves the HTTPS endpoint and certificate route.
        return None
    except (OSError, URLError):
        return "unreachable"
    return None


def run_check(environment: Mapping[str, str], *, port: int, check_network: bool) -> list[str]:
    errors = validate_real_environment(environment, port=port)
    if errors or not check_network:
        return errors
    if shutil.which("ssh") is None:
        errors.append("ssh is required for real Habitat direct-connect validation")
    for name in ("HAB_APISERVER", "OMNIGENT_PUBLIC_URL"):
        if _check_https(environment[name]):
            errors.append(f"{name} is not HTTPS-reachable")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--network", action="store_true", help="perform read-only HTTPS checks")
    args = parser.parse_args()
    errors = run_check(os.environ, port=args.port, check_network=args.network)
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
    checks = (
        "local configuration, bearer-file safety, and HTTPS reachability"
        if args.network
        else "local configuration and bearer-file safety"
    )
    print(f"PASS: real Habitat preflight verified {checks}; no Hab was created.")


if __name__ == "__main__":
    main()
