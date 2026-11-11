"""Installed test plugin: SQL identities and an in-memory secret service.

The opaque references and identity-bound payloads exercise remote-secret read
avoidance and outage handling without a cloud dependency. Not a production store.
"""

from __future__ import annotations

import secrets
import threading

from omnigent.stores.credential_store.backend import CredentialBackendUnavailable
from omnigent.stores.credential_store.sqlalchemy_store import CredentialStore


class FixtureSecrets:
    def __init__(self):
        self.records = {}
        self.reads = 0
        self.writes = 0
        self.unavailable = False
        self.lock = threading.Lock()

    def _available(self):
        if self.unavailable:
            raise CredentialBackendUnavailable("synthetic-private-transport-detail")

    def encrypt(self, plaintext, *, context):
        self._available()
        with self.lock:
            ref = secrets.token_hex(16)
            self.records[ref] = (dict(context), plaintext)
            self.writes += 1
            return ref

    def decrypt(self, ciphertext, *, context):
        self._available()
        with self.lock:
            self.reads += 1
            value = self.records.get(ciphertext)
            if value is None or value[0] != dict(context):
                return None
            return value[1]


class FixtureBackend(CredentialStore):
    def __init__(self, storage_location):
        self.remote = FixtureSecrets()
        super().__init__(storage_location, self.remote)


def build(*, storage_location: str):
    return FixtureBackend(storage_location)
