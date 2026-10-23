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
spent rotating refresh token is not repeatedly exchanged. Secret-free denial
markers survive token-cache eviction and are cleared when the row changes or
disappears. Explicit rejection
requires reauthorization; transient provider failures remain distinct from
backend outages. Reconnect replaces the cached failure state.

Definite unsuccessful refresh responses can recover without reconnecting:
HTTP 429 rate-limit rejections and HTTP 503 responses with an OAuth
`temporarily_unavailable` or `server_error` object are retried on demand after
5 seconds, doubling to a maximum delay of 300 seconds. Neither response may
contain an `access_token` or `refresh_token` field. A bare 503 can originate from
a gateway after an upstream rotation, so it does not establish safe retry.
An OAuth `error` field in either response must name one of those two temporary
errors; other error codes do not qualify for retry. Other statuses, timeouts,
transport failures, malformed success responses and
failed persistence remain suppressed for that row; they can leave refresh-token
rotation uncertain. Explicit reauthorization failures are never retried.
During backoff the old access token remains usable until its actual expiry;
after expiry requests report temporary provider unavailability until the next
allowed attempt. Retry state survives positive-cache eviction and retains the
same authorization, mutation and single-flight checks.

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
uv run --no-sync pytest \
  tests/server/integration/test_mcp_registry.py \
  tests/server/test_credential_store.py tests/server/test_credential_token_cache.py \
  tests/server/test_mcp_registry.py tests/server/test_github_identity.py \
  tests/server/test_github_store.py tests/server/test_databricks_store.py \
  tests/server/routes/test_host_credentials.py \
  tests/server/test_secretbox.py tests/server/test_vault_cipher.py \
  tests/server/test_github_app.py tests/server/test_github_app_config.py \
  tests/server/test_databricks_app.py tests/server/routes/test_integrations_github.py \
  tests/server/test_account_revocation.py tests/server/test_oauth_shared.py \
  --reruns 0 -q
```

The test suite builds a small local fixture plugin with a stdlib-only PEP 517
backend and installs it offline with real entry-point metadata. Each xdist worker
uses a private temporary install target rather than changing the shared test
virtualenv; the CLI scenario explicitly passes that target to its subprocess.
No setuptools, lint group or network build bootstrap is required. Its secret service is in memory and
intended only for testing. This proves application discovery and route behavior;
it does not establish compatibility with any separately distributed backend wheel.
The optional live Vault cases require a local Vault dev server and its Python
extra; they skip when those prerequisites are absent. Mock KMS and SQL coverage
still runs. Chart/image startup verification requires the deployment artifacts
and capability validator; the CLI subprocess check does not substitute for it.

# Account deletion and remote record lifecycle

Account deletion in `accounts_store._revoke_durable_authority` removes credential
rows directly inside the transaction that tombstones the user and revokes other
durable authority. That transaction remains authoritative. Warm cache hits check
account authority and metadata; refresh completion and fallback after a provider
failure recheck authority before returning tokens. A deletion racing refresh
cannot restore a row, publish a new token, or return an old cached token.

For a remote backend with permanent records, this raw SQL deletion can orphan
remote records. The six-method backend contract offers no reference-level cleanup
operation after row removal. Calling `backend.delete` before committing account
revocation could destroy secrets for a transaction that later rolls back; calling
it afterward cannot recover deleted references. The application therefore keeps
hard transactional revocation and does **not** attempt unsafe cleanup or add a
queue. Remote orphan cleanup after account deletion is a documented lifecycle
limitation, distinct from account access: the former remote secret is no longer
reachable through application identity rows. Separately distributed backend
implementations and operational verification must account for this limitation.
