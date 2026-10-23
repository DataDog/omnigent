"""Provider-agnostic per-user credential store.

The default SQLAlchemy backend (:class:`CredentialStore`) uses the
:class:`SecretCipher` port. External plugins implement :class:`CredentialBackend`.
Provider façades live in
:mod:`omnigent.connections`. See ``designs/CREDENTIAL_STORE.md``.
"""

from omnigent.stores.credential_store.backend import (
    CredentialBackend,
    CredentialBackendUnavailable,
    build_credential_backend,
)
from omnigent.stores.credential_store.secret_cipher import (
    CREDENTIAL_KMS_KEY_ENV_VAR,
    KmsSecretCipher,
    SecretCipher,
    SecretContext,
    build_secret_cipher,
)
from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore

__all__ = [
    "CREDENTIAL_KMS_KEY_ENV_VAR",
    "CredentialBackend",
    "CredentialBackendUnavailable",
    "CredentialStore",
    "KmsSecretCipher",
    "SecretCipher",
    "SecretContext",
    "build_credential_backend",
    "build_secret_cipher",
]
