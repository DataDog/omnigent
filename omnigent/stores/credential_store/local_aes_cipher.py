"""Versioned local AES-GCM implementation of :class:`SecretCipher`.

This backend is for self-hosted OIDC credential sessions.  Its envelope carries
the encrypting key ID so deployments can retain decrypt-only keys while moving
new writes to an active key.  Key material never appears in an envelope, repr,
or error message.
"""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from omnigent.stores.credential_store.secret_cipher import SecretContext

_NONCE_SIZE = 12
_KEY_SIZE = 32
_ENVELOPE_VERSION = "v1"
_PREFIX = "oaesgcm"


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _aad(context: SecretContext) -> bytes:
    """Return a stable, complete encoding of the caller-provided row context."""
    return json.dumps(dict(sorted(context.items())), separators=(",", ":")).encode("utf-8")


class LocalAesGcmSecretCipher:
    """Keyed local AES-GCM cipher with context-bound, versioned envelopes."""

    def __init__(self, active_key_id: str, keys: Mapping[str, bytes]) -> None:
        if not active_key_id:
            raise ValueError("OIDC credential active key ID must not be empty")
        if active_key_id not in keys:
            raise ValueError("OIDC credential active key ID is not configured")
        invalid = [key_id for key_id, key in keys.items() if not key_id or len(key) != _KEY_SIZE]
        if invalid:
            raise ValueError("OIDC credential keys must each be exactly 32 bytes")
        self._active_key_id = active_key_id
        self._keys = dict(keys)

    def encrypt(self, plaintext: str, *, context: SecretContext) -> str:
        nonce = os.urandom(_NONCE_SIZE)
        ciphertext = AESGCM(self._keys[self._active_key_id]).encrypt(
            nonce, plaintext.encode("utf-8"), _aad(context)
        )
        fields = (
            _PREFIX,
            _ENVELOPE_VERSION,
            _b64encode(self._active_key_id.encode()),
            _b64encode(nonce),
            _b64encode(ciphertext),
        )
        return ":".join(fields)

    def decrypt(self, ciphertext: str, *, context: SecretContext) -> str | None:
        parts = ciphertext.split(":")
        if len(parts) != 5 or parts[0] != _PREFIX or parts[1] != _ENVELOPE_VERSION:
            return None
        try:
            key_id = _b64decode(parts[2]).decode("utf-8")
            nonce = _b64decode(parts[3])
            encrypted = _b64decode(parts[4])
            key = self._keys.get(key_id)
            if key is None or len(nonce) != _NONCE_SIZE:
                return None
            return AESGCM(key).decrypt(nonce, encrypted, _aad(context)).decode("utf-8")
        except (InvalidTag, UnicodeDecodeError, ValueError):
            return None


def _parse_key(value: str, *, variable: str) -> bytes:
    try:
        key = bytes.fromhex(value)
    except ValueError as exc:
        raise RuntimeError(f"{variable} must be a valid 64-character hex key") from exc
    if len(key) != _KEY_SIZE:
        raise RuntimeError(f"{variable} must be exactly 32 bytes (64 hex chars)")
    return key


def build_oidc_credential_cipher_from_env() -> LocalAesGcmSecretCipher:
    """Build the stable OIDC cipher from explicit active and decrypt-only keys.

    ``OMNIGENT_OIDC_CREDENTIAL_KEY`` is the active AES-256 key.  The optional
    ``OMNIGENT_OIDC_CREDENTIAL_ACTIVE_KEY_ID`` (default ``v1`` for backwards
    compatible deployments) labels new ciphertext.  The optional JSON mapping
    ``OMNIGENT_OIDC_CREDENTIAL_DECRYPTION_KEYS`` supplies decrypt-only keys.
    Runtime never creates keys: malformed or missing active configuration fails
    closed during OIDC store construction.
    """
    raw_active = os.environ.get("OMNIGENT_OIDC_CREDENTIAL_KEY", "").strip()
    if not raw_active:
        raise RuntimeError(
            "Missing required environment variable OMNIGENT_OIDC_CREDENTIAL_KEY "
            "(OIDC mode requires a stable 32-byte key for provider credential encryption)"
        )
    active_key_id = os.environ.get("OMNIGENT_OIDC_CREDENTIAL_ACTIVE_KEY_ID", "v1").strip()
    if not active_key_id:
        raise RuntimeError("OMNIGENT_OIDC_CREDENTIAL_ACTIVE_KEY_ID must not be empty")
    keys = {active_key_id: _parse_key(raw_active, variable="OMNIGENT_OIDC_CREDENTIAL_KEY")}
    raw_ring = os.environ.get("OMNIGENT_OIDC_CREDENTIAL_DECRYPTION_KEYS", "").strip()
    if raw_ring:
        try:
            configured = json.loads(raw_ring)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "OMNIGENT_OIDC_CREDENTIAL_DECRYPTION_KEYS must be a JSON object"
            ) from exc
        if not isinstance(configured, dict) or not all(
            isinstance(key_id, str) and isinstance(key, str) for key_id, key in configured.items()
        ):
            raise RuntimeError(
                "OMNIGENT_OIDC_CREDENTIAL_DECRYPTION_KEYS must map key IDs to hex keys"
            )
        for key_id, key in configured.items():
            if not key_id or key_id == active_key_id:
                raise RuntimeError(
                    "OIDC decrypt-only key IDs must be non-empty and differ from the active key ID"
                )
            keys[key_id] = _parse_key(key, variable="OMNIGENT_OIDC_CREDENTIAL_DECRYPTION_KEYS")
    return LocalAesGcmSecretCipher(active_key_id, keys)
