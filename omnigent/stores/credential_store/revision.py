"""Opaque identity for a persisted credential row."""

import hashlib
import json


def credential_revision(
    workspace_id: int, user_id: str, provider: str, account_id: str, secret_enc: str
) -> str:
    payload = json.dumps(
        [
            "omnigent.credential-revision.v1",
            workspace_id,
            user_id,
            provider,
            account_id,
            secret_enc,
        ],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
