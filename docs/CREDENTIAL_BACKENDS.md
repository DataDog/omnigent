# Credential backends

`OMNIGENT_MCP_CREDENTIAL_BACKEND` selects the shared credential backend used by
MCP accounts, GitHub and Databricks connections. Despite the historical `MCP`
prefix, it applies to all three. The default is `sql`, retaining existing cipher
selection and configuration. SQL integrations remain disabled without a cipher.
An explicitly selected external backend does not require a SQL cipher or KMS key.

Install a plugin into the **server's Python environment** and select its entry
point name. For example, an independently distributed package can register:

```toml
[project.entry-points."omnigent.credential_backends"]
example = "example_credentials.backend:build"
```

```python
from omnigent.stores.credential_store.backend import CredentialBackend


def build(*, storage_location: str) -> CredentialBackend:
    # Validate local configuration here; construction must not need remote RPC.
    return ExampleBackend(storage_location=storage_location)
```

Unknown, duplicate selected, unloadable, invalid or failing registrations stop
startup. There is no fallback store. Plugin exceptions are suppressed in startup
errors because they may contain credentials; check installation and local
configuration without logging secret values.

The synchronous `CredentialBackend` Protocol in
`omnigent.stores.credential_store.backend` defines six methods: `get`, `upsert`,
`update_secret`, `delete`, `list_for_user` and `list_all`. Existing typed connection
facades still accept `(storage_location, secret_cipher)` and additionally accept
`backend=` for dependency injection. Server startup constructs one shared backend.

Backends must partition **every operation** by `current_workspace_id()` plus user,
provider and account. A full secret is an arbitrary JSON object: access tokens,
refresh tokens, bearer-only grants and separate `mcp-pending:<service>` PKCE state
all round-trip unchanged. Metadata-only reads and lists must not fetch remote
secrets. Missing rows return `None`; existing rows with absent or corrupt secrets
return `secret=None`. Writes preserve active-account checks. `upsert` replaces
secret and metadata while preserving `created_at`; `update_secret` preserves
metadata when omitted and returns `False` for a removed row. It must never insert.

Required backend authentication, transport or service outages must raise
`CredentialBackendUnavailable`. Its public message is sanitized; never include
raw provider responses, references, users or tokens in errors or logs. The app
returns HTTP 503 with a safe message where appropriate. OAuth redirects use
`<provider>=temporarily_unavailable`; JSON-RPC retains its error envelope with
code `-32001`. An outage must never become `connected:false` or successful saving.
Metadata-only status describes the connection row without probing secret storage.

# Token resolution

The server keeps a bounded process-local credential cache. Every resolution still
checks account authority and connection metadata, and MCP service and session
permissions remain enforced on cache hits. Remote secret reads happen on misses,
not on every tool call. Keys include workspace, user, provider and account.

When issuance and expiry are known, on-demand refresh starts around half the
lifetime, with jitter limited to 5% of lifetime and at most 30 seconds. Tokens with
no expiry are not refreshed; bearer-only grants remain usable until actual expiry.
There is no background worker. Native MCP OAuth and delegated provider resolvers
share single-flight refresh and mutation invalidation. Cancellation waits for an
in-flight refresh/persist operation before releasing its lock.

Refresh persistence is synchronous: a newly minted token is returned and cached
only after a successful existing-row update. Omitted refresh tokens are preserved.
Disconnect or reconnect during refresh invalidates the result; callbacks also
check mutations before saving. Failed persistence can leave a still-valid old
access token usable until actual expiry, but never publishes the unsaved grant.
An uncertain or rejected refresh is suppressed for that cached connection so a
spent rotating refresh token is not repeatedly exchanged. Explicit rejection
requires reauthorization; transient provider failures remain distinct from
backend outages. Reconnect replaces the cached failure state.

This in-process coordination assumes one server replica. It is not distributed
refresh locking or a durable recovery queue. Plugin implementations own their
storage consistency and cleanup. There are no pending writes or fallback secrets.

The OpenTelemetry counter `omnigent.credentials.operations` has bounded
`operation` and `result` labels for backend operations, cache hits/misses, refresh
outcomes and persistence failures. No tenant, user, provider or token labels are
emitted.

# Local verification

```sh
uv sync --frozen --group test --group lint --extra all
uv run --no-sync pytest tests/server/test_credential_store.py tests/server/test_credential_token_cache.py tests/server/test_mcp_registry.py tests/server/test_github_identity.py tests/server/test_github_store.py tests/server/test_databricks_store.py tests/server/routes/test_host_credentials.py tests/server/integration/test_mcp_registry.py --reruns 0
```

The test suite installs a small local fixture plugin with real entry-point
metadata, requiring no cloud credentials. Its secret service is in memory and
intended only for testing. This proves application discovery and route behavior;
it does not establish compatibility with any separately distributed backend wheel.
