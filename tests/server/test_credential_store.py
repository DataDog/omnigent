"""Tests for the provider-agnostic CredentialStore."""

from __future__ import annotations

import base64
import json
from typing import Any
from unittest.mock import Mock

import pytest
import sqlalchemy as sa

from omnigent.db.db_models import workspace_scope
from omnigent.db.utils import get_or_create_engine
from omnigent.stores.credential_store.secret_cipher import SecretTooLargeError
from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore


class _FakeCipher:
    """In-memory SecretCipher standing in for the KMS cipher.

    Binds ciphertext to ``(key, context)`` so a wrong key or a mismatched
    identity decrypts to ``None`` — the same observable contract as
    :class:`~omnigent.stores.credential_store.secret_cipher.KmsSecretCipher`, without AWS. The
    plaintext is base64-wrapped (not stored in the clear), so the at-rest test
    is still meaningful.
    """

    def __init__(self, key: str = "test-key") -> None:
        self._key = key

    def encrypt(self, plaintext: str, *, context: Any) -> str:
        payload = {"key": self._key, "ctx": dict(context), "pt": plaintext}
        return base64.b64encode(json.dumps(payload).encode()).decode("ascii")

    def decrypt(self, ciphertext: str, *, context: Any) -> str | None:
        try:
            payload = json.loads(base64.b64decode(ciphertext.encode("ascii")))
        except ValueError:
            return None
        if payload["key"] != self._key or payload["ctx"] != dict(context):
            return None
        return payload["pt"]


def _store(db_uri: str) -> CredentialStore:
    return CredentialStore(db_uri, _FakeCipher())


def test_upsert_get_roundtrip(db_uri: str) -> None:
    store = _store(db_uri)
    store.upsert(
        "alice@example.com",
        "github",
        secret={"access_token": "ghu_1", "refresh_token": "ghr_1"},
        metadata={"github_login": "alice"},
    )
    # Metadata-only view hides the secret.
    meta_only = store.get("alice@example.com", "github")
    assert meta_only is not None
    assert meta_only.secret is None
    assert meta_only.metadata["github_login"] == "alice"
    # With-secret view decrypts.
    full = store.get("alice@example.com", "github", with_secret=True)
    assert full is not None and full.secret == {"access_token": "ghu_1", "refresh_token": "ghr_1"}


def test_provider_isolation(db_uri: str) -> None:
    store = _store(db_uri)
    store.upsert("alice", "github", secret={"k": "gh"}, metadata={})
    store.upsert("alice", "datadog", secret={"k": "dd"}, metadata={})
    gh = store.get("alice", "github", with_secret=True)
    dd = store.get("alice", "datadog", with_secret=True)
    assert gh is not None and gh.secret == {"k": "gh"}
    assert dd is not None and dd.secret == {"k": "dd"}
    assert store.get("alice", "slack") is None
    assert {c.provider for c in store.list_for_user("alice")} == {"github", "datadog"}


def test_update_secret_and_delete(db_uri: str) -> None:
    store = _store(db_uri)
    store.upsert("bob", "github", secret={"access_token": "old"}, metadata={"scopes": "repo"})
    assert store.update_secret("bob", "github", secret={"access_token": "new"}) is True
    conn = store.get("bob", "github", with_secret=True)
    assert conn is not None and conn.secret == {"access_token": "new"}
    assert conn.metadata["scopes"] == "repo"  # metadata preserved when not patched
    assert store.delete("bob", "github") is True
    assert store.get("bob", "github") is None


def test_update_secret_missing_row_returns_false(db_uri: str) -> None:
    # A refresh racing a disconnect must report the drop, not silently discard
    # the freshly-minted (and now-spent) token.
    store = _store(db_uri)
    assert store.update_secret("ghost", "github", secret={"access_token": "x"}) is False


@pytest.mark.parametrize("operation", ["insert", "upsert", "update_secret"])
@pytest.mark.parametrize("unit", ["x", "é"], ids=["ascii", "utf8"])
def test_encrypted_size_limit_preserves_existing_connection(db_uri, monkeypatch, operation, unit):
    cipher = _FakeCipher()
    store = CredentialStore(db_uri, cipher)
    if operation != "insert":
        store.upsert("alice", "github", secret={"access_token": "old"}, metadata={"scope": "old"})
    before = store.get("alice", "github", with_secret=True)
    monkeypatch.setattr(store._engine.dialect, "name", "mysql")
    ciphertext = unit * (65_536 // len(unit.encode("utf-8")))
    encrypt = Mock(return_value=ciphertext)
    monkeypatch.setattr(cipher, "encrypt", encrypt)
    write = store.update_secret if operation == "update_secret" else store.upsert
    with pytest.raises(SecretTooLargeError, match=r"65535 bytes.*smaller credential"):
        write("alice", "github", secret={"access_token": "new"}, metadata={"scope": "new"})
    assert store.get("alice", "github", with_secret=True) == before

    # Exactly 65,535 stored bytes fit, including non-ASCII cipher envelopes.
    encrypt.return_value = ciphertext[:-1] + ("x" if unit == "é" else "")
    write("alice", "github", secret={"access_token": "new"}, metadata={"scope": "new"})
    with get_or_create_engine(db_uri).connect() as connection:
        stored = connection.execute(sa.text("SELECT secret_enc FROM connections")).scalar_one()
    assert stored == encrypt.return_value
    assert len(stored.encode("utf-8")) == 65_535


def test_secret_never_stored_in_plaintext(db_uri: str) -> None:
    # The actual at-rest guarantee: the token bytes must not appear in the
    # column, only the cipher's ciphertext.
    store = _store(db_uri)
    store.upsert("dave", "github", secret={"access_token": "ghu_plaintext_leak"}, metadata={})
    engine = get_or_create_engine(db_uri)
    with engine.connect() as conn:
        stored = conn.execute(sa.text("SELECT secret_enc FROM connections")).scalar_one()
    assert "ghu_plaintext_leak" not in stored
    # And it still round-trips back to the plaintext through the cipher.
    full = store.get("dave", "github", with_secret=True)
    assert full is not None and full.secret == {"access_token": "ghu_plaintext_leak"}


def test_wrong_key_decrypts_to_none(db_uri: str) -> None:
    _store(db_uri).upsert("carol", "github", secret={"access_token": "x"}, metadata={})
    other = CredentialStore(db_uri, _FakeCipher("different-key"))
    conn = other.get("carol", "github", with_secret=True)
    assert conn is not None and conn.secret is None  # soft-fail, not a crash


def test_list_all_filters_by_provider(db_uri: str) -> None:
    store = _store(db_uri)
    store.upsert("a", "github", secret={"k": "1"}, metadata={})
    store.upsert("b", "github", secret={"k": "2"}, metadata={})
    store.upsert("a", "datadog", secret={"k": "3"}, metadata={})
    assert len(store.list_all(provider="github")) == 2
    assert len(store.list_all()) == 3


def test_workspace_isolation(db_uri: str) -> None:
    # The core multi-tenant guarantee: the same (user, provider) in two
    # workspaces are distinct rows and never read across the boundary.
    store = _store(db_uri)
    with workspace_scope(1):
        store.upsert("alice", "github", secret={"access_token": "ws1"}, metadata={})
    with workspace_scope(2):
        store.upsert("alice", "github", secret={"access_token": "ws2"}, metadata={})
        w2 = store.get("alice", "github", with_secret=True)
        assert w2 is not None and w2.secret == {"access_token": "ws2"}
        assert [c.user_id for c in store.list_all(provider="github")] == [
            "alice"
        ]  # only ws2's row
    with workspace_scope(1):
        w1 = store.get("alice", "github", with_secret=True)
        assert w1 is not None and w1.secret == {"access_token": "ws1"}
        assert len(store.list_all(provider="github")) == 1


def test_non_empty_account_id(db_uri: str) -> None:
    # account_id is in the PK, so two accounts of one provider coexist per user.
    store = _store(db_uri)
    store.upsert("alice", "github", secret={"k": "org1"}, metadata={}, account_id="org1")
    store.upsert("alice", "github", secret={"k": "org2"}, metadata={}, account_id="org2")
    a1 = store.get("alice", "github", account_id="org1", with_secret=True)
    a2 = store.get("alice", "github", account_id="org2", with_secret=True)
    assert a1 is not None and a1.secret == {"k": "org1"}
    assert a2 is not None and a2.secret == {"k": "org2"}
    # The default-account view ("") is a distinct, absent row.
    assert store.get("alice", "github") is None
    assert len(store.list_for_user("alice")) == 2


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
def test_other_databases_keep_their_existing_credential_capacity(dialect):
    from omnigent.stores.credential_store.sqlalchemy_store import _check_encrypted_size

    _check_encrypted_size("x" * 65_536, dialect)


def test_shared_backend_contract(contract_backend):
    store = contract_backend
    payload = {
        "access_token": "private-marker",
        "refresh_token": "rotate",
        "nested": [True, None, {"x": 3}],
        "verifier": "pkce",
        "nonce": "state",
    }
    first = store.upsert(
        "alice", "mcp-pending:tracker", secret=payload, metadata={"login": "alice"}
    )
    assert store.get("alice", "mcp-pending:tracker", with_secret=True).secret == payload
    assert "private-marker" not in repr(first)
    assert store.get("alice", "mcp-pending:tracker").secret is None
    assert all(row.secret is None for row in store.list_for_user("alice") + store.list_all())
    assert store.update_secret("alice", "mcp-pending:tracker", secret={"changed": [1, 2]})
    changed = store.get("alice", "mcp-pending:tracker", with_secret=True)
    assert changed.created_at == first.created_at and changed.metadata == first.metadata
    for user, provider, account in [
        ("bob", "mcp-pending:tracker", ""),
        ("alice", "other", ""),
        ("alice", "mcp-pending:tracker", "account"),
    ]:
        assert store.get(user, provider, account_id=account) is None
    with workspace_scope(2):
        assert store.get("alice", "mcp-pending:tracker") is None
        assert store.list_all() == []
    assert store.delete("alice", "mcp-pending:tracker") is True
    assert store.delete("alice", "mcp-pending:tracker") is False
    assert store.update_secret("alice", "mcp-pending:tracker", secret=payload) is False


def test_plugin_metadata_reads_and_sanitized_outages(
    db_uri, monkeypatch, installed_credential_plugin
):
    from omnigent.stores.credential_store.backend import (
        CredentialBackendUnavailable,
        build_credential_backend,
    )

    monkeypatch.setenv("OMNIGENT_MCP_CREDENTIAL_BACKEND", "fixture")
    store = build_credential_backend(storage_location=db_uri)
    remote = store.cache.backend.remote
    store.upsert("alice", "mcp:tracker", secret={"access_token": "private"}, metadata={})
    remote.unavailable = True
    assert store.get("alice", "mcp:tracker").secret is None
    assert len(store.list_for_user("alice")) == len(store.list_all()) == 1
    assert remote.reads == 0
    with pytest.raises(CredentialBackendUnavailable) as failure:
        store.get("alice", "mcp:tracker", with_secret=True)
    assert "synthetic-private" not in str(failure.value)
    assert store.get("bob", "mcp:tracker", with_secret=True) is None


@pytest.mark.parametrize(
    "mode", ["missing", "duplicate", "import", "factory", "invalid", "discovery"]
)
def test_plugin_registration_errors_fail_closed(db_uri, monkeypatch, mode):
    from omnigent.stores.credential_store import backend

    monkeypatch.setenv(backend.BACKEND_ENV_VAR, "selected")
    entry = Mock(name="entry")
    entry.name = "selected"
    if mode == "import":
        entry.load.side_effect = ImportError("private-marker")
    elif mode == "factory":
        entry.load.return_value = Mock(side_effect=RuntimeError("private-marker"))
    else:
        entry.load.return_value = lambda **kwargs: object()
    discovery = Mock(
        return_value=[]
        if mode == "missing"
        else [entry, entry]
        if mode == "duplicate"
        else [entry]
    )
    if mode == "discovery":
        discovery.side_effect = RuntimeError("private-marker")
    monkeypatch.setattr(backend.importlib.metadata, "entry_points", discovery)
    with pytest.raises(ValueError, match=r"[Cc]redential backend") as failure:
        backend.build_credential_backend(storage_location=db_uri)
    assert "private-marker" not in str(failure.value)


def test_selected_plugin_does_not_construct_sql_cipher(
    db_uri, monkeypatch, installed_credential_plugin
):
    from omnigent.connections.databricks import DatabricksConnectionStore
    from omnigent.connections.github import GithubConnectionStore
    from omnigent.stores.credential_store import secret_cipher
    from omnigent.stores.credential_store.backend import build_credential_backend

    monkeypatch.setenv("OMNIGENT_MCP_CREDENTIAL_BACKEND", "fixture")
    cipher = Mock(side_effect=AssertionError("SQL cipher must not be required"))
    monkeypatch.setattr(secret_cipher, "build_secret_cipher", cipher)
    store = build_credential_backend(storage_location=db_uri)
    assert GithubConnectionStore(backend=store)._store is store
    assert DatabricksConnectionStore(backend=store)._store is store
    cipher.assert_not_called()


def test_sql_factory_compatibility(db_uri, monkeypatch):
    from omnigent.stores.credential_store import secret_cipher
    from omnigent.stores.credential_store.backend import build_credential_backend

    monkeypatch.delenv("OMNIGENT_MCP_CREDENTIAL_BACKEND", raising=False)
    cipher = Mock(return_value=None)
    monkeypatch.setattr(secret_cipher, "build_secret_cipher", cipher)
    assert build_credential_backend(storage_location=db_uri) is None
    cipher.return_value = _FakeCipher()
    backend = build_credential_backend(storage_location=db_uri)
    backend.upsert("alice", "github", secret={"arbitrary": [1]}, metadata={})
    assert backend.get("alice", "github", with_secret=True).secret == {"arbitrary": [1]}


def test_shared_concurrent_replacement_and_created_at(contract_backend):
    from concurrent.futures import ThreadPoolExecutor

    from omnigent.db.db_models import current_workspace_id

    store = contract_backend
    workspace = current_workspace_id()
    original = store.upsert("alice", "mcp:tracker", secret={"nested": ["original"]}, metadata={})

    def replace(index):
        with workspace_scope(workspace):
            return store.upsert(
                "alice",
                "mcp:tracker",
                secret={"nested": [index, {"ok": True}]},
                metadata={"version": index},
            )

    with ThreadPoolExecutor(max_workers=4) as workers:
        rows = list(workers.map(replace, range(8)))
    final = store.get("alice", "mcp:tracker", with_secret=True)
    assert final.secret == {"nested": [final.metadata["version"], {"ok": True}]}
    assert all(row.created_at == original.created_at for row in rows)
    assert len(store.list_all()) == 1


def test_backend_metrics_are_bounded_and_exceptions_are_secret_safe(db_uri, monkeypatch, caplog):
    from omnigent.stores.credential_store import token_cache

    counter = Mock()
    monkeypatch.setattr(token_cache, "_counter", counter)
    raw = _store(db_uri)
    store = token_cache.managed_backend(raw)
    marker = "private-user-token-marker"
    store.upsert(marker, "mcp:arbitrary-provider", secret={"access_token": marker}, metadata={})
    store.get(marker, "mcp:arbitrary-provider")
    monkeypatch.setattr(raw, "get", Mock(side_effect=RuntimeError(marker)))
    with pytest.raises(RuntimeError, match="backend operation failed") as failure:
        store.get(marker, "mcp:arbitrary-provider", with_secret=True)
    assert marker not in str(failure.value) + caplog.text
    for call in counter.add.call_args_list:
        attributes = call.args[1]
        assert set(attributes) == {"operation", "result"}
        assert marker not in repr(attributes)
        assert "arbitrary-provider" not in repr(attributes)


@pytest.mark.parametrize("kind", ["factory", "method"])
def test_asynchronous_plugins_are_rejected(db_uri, monkeypatch, kind):
    from omnigent.stores.credential_store import backend

    monkeypatch.setenv(backend.BACKEND_ENV_VAR, "selected")

    async def asynchronous(*args, **kwargs):
        return None

    store = _store(db_uri)
    if kind == "method":
        monkeypatch.setattr(store, "get", asynchronous)
    entry = Mock()
    entry.name = "selected"
    entry.load.return_value = asynchronous if kind == "factory" else lambda **kwargs: store
    monkeypatch.setattr(backend.importlib.metadata, "entry_points", Mock(return_value=[entry]))
    with pytest.raises(ValueError, match="synchronous"):
        backend.build_credential_backend(storage_location=db_uri)


@pytest.mark.parametrize("operation", ["read", "upsert", "update"])
def test_sql_cipher_outage_is_sanitized_unavailability(db_uri, monkeypatch, operation):
    from omnigent.stores.credential_store.backend import CredentialBackendUnavailable

    store = _store(db_uri)
    store.upsert("alice", "github", secret={"access_token": "old"}, metadata={})
    target = "decrypt" if operation == "read" else "encrypt"
    monkeypatch.setattr(store._cipher, target, Mock(side_effect=RuntimeError("private-marker")))
    with pytest.raises(CredentialBackendUnavailable) as failure:
        if operation == "read":
            store.get("alice", "github", with_secret=True)
        elif operation == "upsert":
            store.upsert("alice", "github", secret={"access_token": "new"}, metadata={})
        else:
            store.update_secret("alice", "github", secret={"access_token": "new"})
    assert "private-marker" not in str(failure.value)
    assert store.get("alice", "github").secret is None


def test_installed_fixture_is_worker_local_with_valid_record(
    installed_credential_plugin, tmp_path_factory
):
    import csv
    import hashlib
    import importlib.metadata
    import inspect
    import io
    import sys
    from pathlib import Path

    target = installed_credential_plugin.resolve()
    assert target.is_relative_to(tmp_path_factory.getbasetemp())
    assert not target.is_relative_to(Path(sys.prefix))
    distribution = importlib.metadata.distribution("omnigent-test-credential-backend")
    assert distribution.locate_file("").resolve() == target
    entry = next(ep for ep in distribution.entry_points if ep.name == "fixture")
    assert entry.group == "omnigent.credential_backends"
    assert Path(inspect.getfile(entry.load())).resolve().is_relative_to(target)
    rows = list(csv.reader(io.StringIO(distribution.read_text("RECORD"))))
    assert rows
    for path, digest, size in rows:
        if not digest:
            assert path.endswith(("RECORD", ".pyc"))
            continue
        data = distribution.locate_file(path).read_bytes()
        assert size == str(len(data))
        expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        assert digest == f"sha256={expected}"
