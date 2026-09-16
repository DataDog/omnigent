# Managed sandbox provisioning context

## Status

Proposed implementation plan.

The first delivery targets initial Habitat provisioning end to end. Request-driven
lifecycle operations, restart recovery, and same-user OIDC-session rebinding follow
in later phases.

## Motivation

An Omnigent-managed Habitat must be created as the signed-in user, with Omnigent
recorded as the actor. The Habitat launcher therefore needs access to the user's
renewable Ticino ID token so it can exchange that token, together with Omnigent's
workload credential, for a short-lived Habitat on-behalf-of (OBO) token.

Today, the managed-sandbox path loses the authentication session before provider
code runs:

1. The session create route authenticates the request and obtains a user ID.
2. `_schedule_managed_launch` starts provisioning in an `asyncio` background task.
3. `ManagedSandboxConfig.launcher_factory` constructs a launcher without arguments.
4. `SandboxHostLauncher.provision` receives only a display name.
5. Provider calls execute in worker threads through `asyncio.to_thread`.

Passing a live request through this stack would couple provider code to FastAPI and
would not provide a durable identity boundary. Changing every launcher interface at
once would also create a large migration unrelated to Habitat.

Python `ContextVar` values are copied into a new `asyncio` task and propagated by
`asyncio.to_thread`. We will use that behavior as a small, explicit compatibility
bridge for the first delivery, then migrate lifecycle operations to explicit context
parameters incrementally.

## Decisions

1. The token provider returns the current signed Ticino ID token. The Habitat
   plugin, not Omnigent core, performs the RFC 8693 exchange.
2. Initial provisioning is the first end-to-end milestone.
3. Existing zero-argument launcher factories and `provision(name)` signatures remain
   unchanged in the first milestone.
4. A dedicated managed-sandbox `ContextVar` carries immutable identity context.
   Logging or workspace context variables are not authorization inputs.
5. The context contains a token-provider object, not raw ID, refresh, workload, or
   OBO tokens.
6. Missing, expired, revoked, or mismatched identity context fails closed. Production
   Habitat provisioning must not silently fall back to service identity.
7. The first milestone does not persist context and does not support rebinding an
   existing Hab to a new OIDC login session. That path may fail clearly until the
   later rebinding phase.
8. Context propagation must use `asyncio.create_task` and `asyncio.to_thread`.
   Arbitrary executors require an explicit copied context and are outside the initial
   contract.

## Proposed public API

Add `omnigent/onboarding/sandboxes/context.py` as the portable public boundary used
by Omnigent and external sandbox-provider wheels.

```python
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class IdentityToken:
    value: str = field(repr=False)
    expires_at: int


class IdentityTokenProvider(Protocol):
    def get_identity_token(self) -> IdentityToken: ...


@dataclass(frozen=True)
class ManagedSandboxContext:
    session_id: str
    user_id: str
    identity_token_provider: IdentityTokenProvider | None = field(repr=False)


_current_managed_sandbox_context: ContextVar[ManagedSandboxContext | None] = (
    ContextVar("omnigent_managed_sandbox_context", default=None)
)


@contextmanager
def managed_sandbox_context_scope(
    context: ManagedSandboxContext,
) -> Iterator[None]:
    reset_token = _current_managed_sandbox_context.set(context)
    try:
        yield
    finally:
        _current_managed_sandbox_context.reset(reset_token)


def current_managed_sandbox_context() -> ManagedSandboxContext | None:
    return _current_managed_sandbox_context.get()
```

The exact implementation may validate non-empty identifiers, but the contract must
remain synchronous and framework-neutral. `IdentityToken.value` and
`identity_token_provider` must be excluded from generated representations.

The module may be re-exported from `omnigent.onboarding.sandboxes`, but external
providers should be able to import the explicit module path:

```python
from omnigent.onboarding.sandboxes.context import (
    ManagedSandboxContext,
    current_managed_sandbox_context,
)
```

## Context lifecycle for initial provisioning

The create route is the last boundary that has both the authenticated request and
the newly created Omnigent session ID. It constructs the context immediately before
creating the background task:

```python
context = ManagedSandboxContext(
    session_id=session_id,
    user_id=owner,
    identity_token_provider=auth_provider.get_identity_token_provider(
        request,
        expected_user_id=owner,
    ),
)

with managed_sandbox_context_scope(context):
    task = asyncio.create_task(_run_managed_launch(...))
```

Leaving the scope immediately resets the request task. The newly created launch task
retains its copied context. Its calls through `asyncio.to_thread`, including launcher
construction and `provision`, see that same context.

The background task must not retain the `Request`, cookie, authorization header, or
raw bearer token.

## OIDC token-provider boundary

`AuthProvider` gains an optional, non-breaking method:

```python
def get_identity_token_provider(
    self,
    request: HTTPConnection,
    expected_user_id: str,
) -> IdentityTokenProvider | None:
    return None
```

The default supports existing header, accounts, and local modes without behavior
changes. The OIDC implementation returns a provider bound to:

- the validated OIDC credential-session identifier;
- the verified user ID; and
- the OIDC session store/token manager.

Each `get_identity_token` call validates the session and expected user again, then
returns the current ID token or refreshes it. It never exposes the refresh token.

User extraction and provider-session extraction must share one validated credential
path. They must not independently decode a cookie with different validation rules.

## Habitat plugin boundary

`omnigent_hab_launcher` is a separate wheel installed alongside Omnigent in the
server image. Its Omnigent-dependent integration layer imports the public context
module at runtime. Low-level modules such as `client.py`, `config.py`, `registry.py`,
and `lifecycle.py` remain importable without Omnigent.

Use a small lazy adapter, for example `hab_launcher/omnigent_context.py`, so version
skew produces a clear error only when the Omnigent integration is used:

```python
def current_context() -> ManagedSandboxContext | None:
    try:
        from omnigent.onboarding.sandboxes.context import (
            current_managed_sandbox_context,
        )
    except ImportError as exc:
        raise RuntimeError(
            "omnigent_hab_launcher requires managed sandbox identity-context support"
        ) from exc
    return current_managed_sandbox_context()
```

The production launcher reads the context when it needs Habitat credentials, not at
package import or server startup. It passes the renewable ID token to the shared
`dd_internal_authentication` exchange client, requests only `audience=hab`, and gives
Habitat only the resulting OBO bearer.

## Red/green implementation sequence

Every slice starts with one narrowly scoped failing behavior. Confirm the failure is
caused by the missing behavior, implement the smallest green change, run the broader
affected suite, and refactor only while green.

### Slice 1: ambient context primitive

**Red**

Add `tests/onboarding/sandboxes/test_context.py` with tests proving:

- the current context is `None` by default;
- a scope exposes its context and restores the previous value;
- nested scopes restore correctly;
- concurrent tasks do not see each other's values;
- a task created in a scope retains the copied context after the parent exits;
- `asyncio.to_thread` propagates the task's context; and
- token material is absent from `repr` output.

**Green**

Add the public types, scope manager, and accessor. Do not touch managed-host
orchestration in this slice.

### Slice 2: OIDC-bound identity-token provider

**Red**

Add auth tests proving:

- a valid OIDC request yields a provider bound to its exact credential session and
  expected user;
- a provider returns the current ID token;
- a near-expiry ID token refreshes through the token manager;
- expired, revoked, and cross-user sessions fail;
- refresh credentials never leave the provider; and
- unsupported auth modes return no provider.

**Green**

Add the optional `AuthProvider` method and implement it for OIDC using the encrypted
OIDC session store and token manager. Integrate with the in-progress OIDC credential
storage work rather than introducing a second store.

### Slice 3: managed-launch task capture

**Red**

Extend managed-host route tests to prove:

- the background launch observes the exact conversation ID and verified owner;
- it observes the request's bound identity-token provider;
- the context remains available after the create request returns;
- launcher construction and `provision` inside `asyncio.to_thread` see it;
- the request task no longer sees it after scheduling; and
- two concurrent user launches remain isolated.

Use the existing zero-argument fake launcher factory. The test must demonstrate that
no provider signature change is required.

**Green**

Resolve the provider and scope only the `asyncio.create_task` call in
`_schedule_managed_launch`. Do not thread new arguments through
`_run_managed_launch`, `_provision_managed_sandbox`, `launch_managed_host`, or
`SandboxHostLauncher.provision`.

### Slice 4: Habitat context adapter

**Red**

In `dd-source`, add tests proving:

- the launcher can read the public Omnigent context;
- context access is lazy;
- low-level Habitat modules still import without Omnigent;
- an older Omnigent produces a clear compatibility error; and
- missing context fails at operation time rather than server startup.

**Green**

Add the lazy adapter and consume it only from the Omnigent-dependent production
launcher path.

### Slice 5: fail-closed provisioning identity

**Red**

Add Habitat launcher tests for missing context, missing token provider, expired or
revoked login session, user mismatch, and refresh failure. Assert that every case
fails the session without using `HAB_TOKEN`, `HAB_TOKEN_FILE`, or another service
identity.

**Green**

Require delegated identity for production Habitat provisioning and translate
credential failures into the existing per-session error surface. Preserve non-fatal
server startup.

### Slice 6: OBO exchange during create

**Red**

With fake identity and exchange clients, prove:

- the current Ticino ID token is the RFC 8693 subject token;
- the Omnigent workload bearer is supplied separately;
- the audience is exactly Habitat;
- Habitat receives only the exchanged OBO bearer;
- two users do not share credentials; and
- ID, refresh, workload, and OBO tokens are absent from logs and errors.

**Green**

Wire the shared exchange client into production provisioning and make the Habitat
client use the resulting OBO bearer for `CreateHab`. Avoid generalized OBO caching
until a later operation needs it.

### Slice 7: initial-provisioning integration

**Red**

Create a cross-boundary integration test with an authenticated OIDC request, the real
background managed-launch path, the external Habitat provider module, a fake token
exchange client, and a fake Habitat gRPC service. It must fail until `CreateHab`
receives the expected OBO bearer.

**Green**

Complete only the wiring needed for:

```text
OIDC request
  -> ContextVar
  -> background launch task
  -> asyncio.to_thread
  -> omnigent_hab_launcher
  -> renewable Ticino ID token
  -> OBO exchange
  -> Habitat CreateHab
```

### Slice 8: staging acceptance

**Red**

Run the acceptance flow before its Ticino, workload-policy, or Habitat prerequisite
is deployed. Record which external boundary fails; do not compensate with a service
credential.

**Green**

Deploy compatible versions and verify:

1. Sign in through Ticino.
2. Create a managed Habitat session.
3. Confirm Habitat records the user as owner and Omnigent as actor.
4. Confirm the Hab connects back and its runner becomes usable.
5. Confirm no delegated credential is placed inside the Hab.
6. Expire the original ID token and confirm another provisioning request refreshes
   it successfully.
7. Confirm a second user cannot reuse the first user's identity path.

## Later phases

After initial provisioning is green, add one red/green track per lifecycle operation:

1. Request-driven get, list, and connect.
2. Delete and cleanup.
3. Relaunch with a still-valid original OIDC session.
4. Resume with a still-valid original OIDC session.
5. Restart recovery without service-identity fallback.
6. Revocation and logout cleanup.
7. Same-user reauthentication and atomic rebinding.

The ambient accessor remains the compatibility fallback while these callers migrate
to explicit context arguments or an additive context-aware launcher factory. New
code should prefer explicit context once the relevant lifecycle signature supports
it.

For rebinding, the first red test documents the initial behavior: a new login session
cannot operate the existing Hab. The later green implementation may replace the
credential-session binding only after verifying that the new login's user exactly
matches the durable Hab owner. A different user always fails.

## Phase 1 non-goals

- Changing `SandboxHostLauncher.provision(name)`.
- Passing a live FastAPI request into provider code.
- Persisting a callable or any raw credential.
- Supporting restart recovery or credential-session rebinding.
- Completing relaunch, resume, status, list, connect, or delete through OBO.
- Making Omnigent core aware of Ticino token exchange or Habitat audiences.
- Removing all legacy service credentials before request-driven lifecycle support
  exists.

## Compatibility and rollout

- New Omnigent with an old launcher remains compatible, but delegated Habitat
  provisioning is unavailable.
- A new launcher with an old Omnigent fails with a clear context-API compatibility
  error when Habitat provisioning is attempted.
- Built-in and community providers continue using their zero-argument factories.
- The deployment must pair compatible Omnigent and `omnigent_hab_launcher` versions
  before enabling OBO provisioning or removing the production Habitat service token.
- The first release should keep the feature behind the existing Habitat deployment
  gate until staging acceptance passes.

## Verification

Targeted Omnigent checks during red/green development:

```bash
uv run pytest tests/onboarding/sandboxes/test_context.py
uv run pytest tests/server/test_managed_hosts.py
uv run pytest tests/server/test_oidc_session_store.py tests/server/test_oidc_token_manager.py
```

Targeted Habitat launcher checks:

```bash
cd ~/dd/dd-source
bzl test //domains/ai-devx/omnigent/hab_launcher/tests:all
```

Before each commit, run the repository-required checks:

```bash
pre-commit run --all-files
```

The final human verification is the staging acceptance sequence in Slice 8; unit
tests alone cannot prove Habitat records the intended owner and actor chain.
