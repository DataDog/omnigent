# Local Ticino → launcher token-handoff harness

This is an opt-in, loopback-only manual integration harness for the live
Ticino public client:

- OAuth client ID: `omnigent-local`
- Redirect URI: `http://127.0.0.1:6767/auth/callback`
- Database: disposable Docker Postgres
- Exchange: a local fake Emissary endpoint
- Habitat: a local fake gRPC Habitat endpoint that stops the launch after it
  sees an authorization header

It establishes the narrow handoff we need before using Habitat: a browser
login produces a signed Ticino ID token, Omnigent persists it in its encrypted
OIDC provider session, the managed-session context gives it to the launcher,
and the launcher sends it to the shared Ticino exchange client with
`audience=hab`. The fake service records only booleans; it never writes,
prints, or hashes any token.

The fake Habitat fails the create request on purpose. It cannot create a Hab,
and it makes the successful receipt stronger: the static fake exchange output
was passed by the launcher as a Habitat authorization bearer without exposing
the actual ID token.

## Prerequisites

- Docker Desktop with `docker compose`
- `uv`, `openssl`, and `curl`
- Either a locally built wheel from dd-source PR #91876 in
  `HAB_LAUNCHER_WHEEL`, or the matching source directory in
  `HAB_LAUNCHER_SOURCE_DIR` (for example,
  `/path/to/dd-source/domains/ai-devx/omnigent`). Source mode adds that
  directory and its sibling `dd_internal_authentication` library to the
  isolated harness process only; it does not modify the source checkout.

The PR #91876 launcher imports as `hab_launcher`, which is the harness
default. Set `HAB_LAUNCHER_MODULE` only when validating a deliberately
different package layout.

For example, from a clean dd-source worktree checked out at the PR's current
head (`9ce4ffa9fc4abb691a23e08f85431ec440f6ec44` when this harness was
written):

```sh
bzl build //domains/ai-devx/omnigent/hab_launcher:omnigent_hab_launcher_wheel
bzl cquery --output=files //domains/ai-devx/omnigent/hab_launcher:omnigent_hab_launcher_wheel
```

Use the resulting `.whl` path as `HAB_LAUNCHER_WHEEL`. Do not build from the
shared dirty dd-source checkout. Source mode is preferred while a Bazel wheel
build is unavailable or slow.

## Run the fake-Habitat proof

```sh
cd /Users/nick.isaacs/go/src/github.com/DataDog/omnigent-token-handoff-e2e
export HAB_LAUNCHER_SOURCE_DIR='/path/to/dd-source/domains/ai-devx/omnigent'
# Or: export HAB_LAUNCHER_WHEEL='/absolute/path/to/omnigent_hab_launcher-*.whl'
dev/ticino_local_e2e/run.sh up
```

Open `http://127.0.0.1:6767`, choose Sign in, and complete the real browser
login. Create a session using **Hab Sandbox**. The expected UI/server outcome
is a bounded launch error saying the local fake Habitat stopped after
validation. Then verify only the non-secret receipt:

```sh
dev/ticino_local_e2e/run.sh status
dev/ticino_local_e2e/run.sh down
```

`status` passes only when all of these were observed without saving values:

1. the launcher reached `/ticino/agent/...` through the Emissary selection
   path;
2. it asked for `desired_audience=hab`;
3. its subject token was JWT-shaped and had `aud=omnigent-local`;
4. the launcher used the exchange response as the fake Habitat authorization
   bearer.

All state, including the random Postgres password, cookie-signing key,
credential-encryption key, and fake workload-bearer file is in a mode-0700
directory under `/tmp` by default. `down` stops the processes, removes the
Docker volume, and deletes that directory. Override the location only with
`TICINO_E2E_STATE_DIR` under `/tmp/omnigent-ticino-token-handoff-e2e-`.
Teardown rejects empty, broad, workspace, home-directory, symlink, and other
out-of-prefix targets before it can delete anything.

## Ticino endpoints

The harness defaults to the known staging layout. It keeps the canonical
Fabric issuer (the expected `iss` claim) while explicitly using
browser-reachable staging endpoints, because discovery at the canonical Fabric
issuer is not reachable from a local browser. It also sets
`OMNIGENT_OIDC_SKIP_EMAIL_VERIFICATION=1`, required for this Ticino setup.

```text
issuer:                  https://ticino.identity.local-cluster.local-dc.fabric.dog:8443/v1/issuer/sycamore
authorization endpoint:  https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/authorize
token endpoint:          https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/oauth/token
JWKS URI:                https://ticino.us1.ddbuild.staging.dog/v1/issuer/sycamore/.well-known/keys
```

Override all four only when testing a different issuer: `TICINO_ISSUER`,
`TICINO_AUTHORIZATION_ENDPOINT`, `TICINO_TOKEN_ENDPOINT`, and
`TICINO_JWKS_URI`.

## Real Habitat switch

The default is always fake and cannot provision a Hab. A real-Habitat run is
available only with two explicit opt-ins: `TICINO_E2E_HABITAT_MODE=real` and
`TICINO_E2E_ALLOW_REAL_HABITAT=1`. It reuses the same local browser/OIDC and
Docker Postgres setup, but it does not start either fake service. The caller
must supply approved values for `HAB_APISERVER`, `EMISSARY_BIND_ADDRESS`, and
the readable `HAB_WORKLOAD_TOKEN_FILE` mount:

```sh
export TICINO_E2E_HABITAT_MODE=real
export TICINO_E2E_ALLOW_REAL_HABITAT=1
export HAB_APISERVER='https://<approved Habitat API endpoint>'
export EMISSARY_BIND_ADDRESS='<approved local Emissary bind address>'
export HAB_WORKLOAD_TOKEN_FILE='/approved/private/workload-token-file'
dev/ticino_local_e2e/run.sh up
```

This mode can create external state. Run it only with the approved workload
identity, Habitat policy, and operator authorization; `down` removes local
state but does not delete any externally provisioned Hab.

The source launcher currently calls `dd_internal_authentication.ticino.Client`.
With `EMISSARY_ENABLED=true` and a reachable `EMISSARY_BIND_ADDRESS`, that
client routes to `http://<bind>/ticino/agent`; the actual workload identity is
attached by Emissary. `HAB_WORKLOAD_TOKEN_FILE` is read by the launcher at
request time and is fail-closed when absent, but the current shared client API
does not send that file's value explicitly. A loopback fake proves the route
and user-token handoff, not genuine Emissary workload attestation.

`pup` is not a local Emissary provisioner (it is the Datadog API CLI), and no
local Emissary binary or injection workflow was found in dd-source. This is
the remaining blocker for a full local real-Habitat test.
