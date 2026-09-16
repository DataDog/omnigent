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
and the launcher exchanges it with RFC 8693. The fake service accepts only a
form-encoded `POST /ticino/agent/v1/issuer/sycamore/oauth/token` with
`X-Emissary-Request: true`, the workload bearer in `Authorization`, the browser
ID token in `subject_token`, the exact ID-token type URN in both token-type
fields, and `audience=hab`. It returns that ID-token type as
`issued_token_type`. It records only booleans; it never writes, prints, or
hashes any token.

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

1. the launcher sent the RFC 8693 POST with the exact
   `X-Emissary-Request: true` routing header;
2. its workload bearer was present in `Authorization` and its form used the
   exact grant, subject-token type, requested-token type, and `audience=hab`;
3. its subject token was JWT-shaped and had `aud=omnigent-local`;
4. the launcher used the JSON `access_token` response as the fake Habitat
   authorization bearer.

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

The default is always `fake` and cannot provision a Hab. Real mode is an
intentional local-only escape hatch for Nick's Habitat DevEnv at
`https://nickisaacs.habvm.dev`; it fails closed unless **both** of these are
set exactly:

```sh
export TICINO_E2E_HABITAT_MODE=real
export TICINO_E2E_ALLOW_REAL_HABITAT=1
```

Real mode uses the browser's loopback callback separately from the guest's
public callback. Do not register the tunnel URL as an OAuth redirect and do
not use a loopback URL for the guest callback. The launcher must be the
file-backed exchange implementation from the corresponding dd-source change.

```sh
export HAB_APISERVER='https://nickisaacs.habvm.dev'
export OMNIGENT_OIDC_REDIRECT_URI='http://127.0.0.1:6767/auth/callback'
export OMNIGENT_PUBLIC_URL='https://<approved-temporary-https-wss-tunnel>'
export OMNIGENT_HAB_EXCHANGE_MODE='file'
export HAB_WORKLOAD_TOKEN_FILE='/tmp/omnigent-workload-bearer.jwt'
export OMNIGENT_HAB_PROFILE='<verified-non-fake-profile>'
export OMNIGENT_HAB_IMAGE='<pinned-image-reference>@sha256:<digest>'
# Airlock is currently off in this test tenant. This broad egress setting is
# temporary and is deliberately required rather than silently defaulted.
export OMNIGENT_HAB_ALLOW_ALL_EGRESS='true'
```

Generate the workload bearer immediately before the run with the reviewed
temporary exporter. It must atomically write a current-user-owned regular
file with mode `0600`; never put its value in an environment variable, command
argument, receipt, issue, or chat.

```sh
KUBE_NAMESPACE=workspaces \
KUBE_POD=omnigent-server-0 \
KUBE_CONTAINER=omnigent-server \
TOKEN_OUTPUT="$HAB_WORKLOAD_TOKEN_FILE" \
OVERWRITE=1 \
/tmp/get-sycamore-workload-token.sh
```

Before Docker or Omnigent starts, run the secret-safe, read-only preflight:

```sh
dev/ticino_local_e2e/run.sh real-check
```

It verifies the dual opt-in, exact API endpoint, loopback OIDC callback,
file-exchange mode, user-owned private JWT file (including `aud=identity` and
freshness), immutable non-fake image, real profile, explicit broad egress,
local `ssh`, and TLS reachability of the Habitat API and public tunnel. It
does not create, list, or delete a Hab, and never prints bearer content.

Only after it passes may you start the local server:

```sh
dev/ticino_local_e2e/run.sh up
```

`up` runs the same preflight again before it creates Docker state. It writes a
secret-free local readiness receipt; `status` can display it but it is not a
remote cleanup receipt. A real run can create external state. Keep the owner
session and exact Hab ID available until deletion is confirmed through
Habitat. `down` stops local processes and Docker Postgres, removes the
recorded workload-token file when it is still a safe regular file, and warns
that it has neither deleted nor verified any remote Hab. Do not interpret
local teardown as remote cleanup.

The public tunnel must support HTTPS and WSS from the guest and should be
removed after confirmed cleanup. Real mode does not drive a local Emissary;
the launcher reads `HAB_WORKLOAD_TOKEN_FILE` for each exchange. The temporary
file workflow is short-lived test scaffolding, not a production renewal
mechanism.
