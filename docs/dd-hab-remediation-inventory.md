# Datadog Hab remediation inventory

Reviewed 2026-09-22 against
`origin/datadog/patch/WRK-2948-managed-sandbox-context` at
`060cfd1bbe8e0ac89b3eccf0ceec5843a74e078d` and `origin/main` at
`5fe7ddf432eeacaae49b4eaf163cb2150e972c66`.

This is the baseline inventory for the managed-Hab remediation. It records
which inputs are deployment configuration, which are narrow laptop harness
substitutes, and which must remain security or persistence concerns regardless
of local-development mode. It does not make an input safe merely because it
uses a loopback address or is used with `OMNIGENT_LOCAL_SINGLE_USER`.

## Classification

| Inputs / location | Classification | Required direction |
| --- | --- | --- |
| OIDC issuer, client ID, redirect URI, authorization/token/JWKS endpoints, allowed domains, and `OMNIGENT_AUTH_*` in `dev/ticino_local_e2e/run.sh` | Test-only process inputs for the local Ticino harness | Keep scoped to the harness; normal deployments configure their IdP independently. |
| `OMNIGENT_OIDC_CREDENTIAL_KEY`, OIDC ciphertext format/version, credential-session expiry/revocation, and all OIDC schema/migrations | Persisted-data/security requirement | Never conditional on Datadog local mode. Keys must be supplied, stable, and shared by replicas; runtime must not generate deployment keys. |
| `OMNIGENT_HAB_ENABLED`, image, profile, runtime, egress policy, registry path, `HAB_APISERVER`, and Ticino exchange address | Production provider configuration | Validate independently of naming and do not classify real endpoint/image/profile values as local substitutes. |
| `TICINO_E2E_HABITAT_MODE`, `TICINO_E2E_ALLOW_REAL_HABITAT`, `HAB_WORKLOAD_TOKEN_FILE`, `OMNIGENT_HAB_EXCHANGE_MODE=file`, and `HAB_LAUNCHER_SOURCE_DIR` / `HAB_LAUNCHER_WHEEL` | Test-only process inputs / local-development harness | These select a laptop proof environment or an explicitly approved real test environment. They are not normal deployment configuration. |
| `dev/ticino_local_e2e/fake_services.py`, fake Emissary and Habitat loopback endpoints, `local-fake-image`, `local-fake-profile`, fake workload bearer, and the source-tree launcher injection | Local-development substitutes | Require the one typed `OMNIGENT_DD_HAB_LOCAL_DEV` gate before a runtime adapter can select them. The flag is not an authorization, migration, or encryption bypass. |
| `HARNESS_LAUNCH_READINESS_GRACE_S = 2.0` and polling in `omnigent/host/connect.py` | Local-development substitute currently embedded in the host process | The normal launch path should perform its one-shot readiness check. Only a centrally resolved local launch policy may enable this retry grace. |
| Docker Postgres, `/tmp/omnigent-ticino-token-handoff-e2e-*`, generated harness cookie/database keys, ports, and `PGPASSFILE` | Test-only process inputs | Keep them harness-private and secret-safe. They do not define production credential retention or key-management behavior. |

## Implementation boundary

`OMNIGENT_DD_HAB_LOCAL_DEV` is the sole future selector for Datadog-Hab
development substitutes. Its resolver must parse once, reject malformed values,
reject subordinate substitute inputs while off, and log only selected substitute
names. It must not gate migrations, credential encryption, OIDC validation,
authorization/owner matching, cleanup ownership, or persistent data correctness.

The local Ticino harness must set that flag before selecting fake/file adapters.
The external Hab launcher should receive the resolved typed adapter policy,
rather than treating loopback endpoints or a token file as intrinsically safe.
Production deployment validation must reject the flag and subordinate
development-only inputs.

## Rollout note

The local gate is a temporary Datadog development compatibility mechanism,
owned by the managed-sandbox integration and targeted for removal in the first
release after maintained dependency injection or a developer plugin replaces
the substitutes. Release notes and the implementation PR must repeat that
target when the gate is introduced.
