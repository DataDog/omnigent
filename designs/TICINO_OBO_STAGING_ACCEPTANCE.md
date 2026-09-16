# Ticino OBO staging acceptance runbook

Acceptance sequence for the managed-sandbox delegated-identity work
(`designs/MANAGED_SANDBOX_PROVISIONING_CONTEXT.md`, Slice 8). Unit and
integration tests prove the wiring; only this sequence proves Habitat
records the intended owner and actor chain.

## Phase 1 — record the pre-deployment red state

Run the flow below BEFORE the Ticino, workload-policy, and Habitat
prerequisites are deployed. Record which external boundary fails at
each step. Do NOT compensate with a service credential
(`HAB_TOKEN`/`HAB_TOKEN_FILE`) — a "green" achieved that way is a
failure of this acceptance, not a pass.

Expected pre-deployment failures:

1. Login cannot complete without the Ticino discovery and ID-token
   work — the OIDC discovery document must publish
   `authorization_endpoint`, `token_endpoint`, and `jwks_uri` under
   the issuer used in signed tokens, and the authorization-code flow
   must return a signed ID token with `aud` = the Omnigent OAuth
   client ID.
2. Habitat provisioning cannot exchange without the workload bearer
   and Habitat audience policy — the launcher calls the exchange
   with the Omnigent workload bearer supplied separately and the
   user's current Ticino ID token as `subject_token` (RFC 8693),
   requesting only `audience=hab`.
3. Habitat rejects or mis-attributes the OBO bearer without its
   verifier and audit changes (user as owner, Omnigent as actor).

## Phase 2 — acceptance sequence

1. Sign in through Ticino (Google → Ticino → Omnigent).
2. Create a managed Habitat session.
3. Confirm Habitat records the user as owner and Omnigent as actor.
4. Confirm the Hab connects back and its runner becomes usable.
5. Confirm no delegated credential (ID token, refresh token, workload
   bearer, OBO token) is placed inside the Hab.
6. Expire the original ID token and confirm another provisioning
   request refreshes it successfully.
7. Confirm a second user cannot reuse the first user's identity path.

## Prerequisites (tracked outside this repo)

- Ticino: discovery, public-client login, renewable ID token,
  client/workload binding.
- dd-source: `dd_internal_authentication` exchange client; the
  `omnigent_hab_launcher` wheel with the context adapter, fail-closed
  identity, and the OBO exchange (branch
  `nicky-isaacs-awoo/managed-sandbox-obo-context`).
- Habitat: OBO verifier and audit changes.
- Chart/deployment: cookie signing key, credential encryption key,
  `HAB_WORKLOAD_TOKEN_FILE` provisioning, exact egress, Habitat
  service token removal.
- Workload policy: Habitat-only audience for the Omnigent workload
  identity.

## Pairing and rollout gates

- Pair compatible Omnigent and `omnigent_hab_launcher` versions in
  the server image before enabling OBO provisioning; a new launcher
  against an old Omnigent fails with the context-API compatibility
  error at operation time (by design).
- Keep the feature behind the existing Habitat deployment gate until
  this acceptance passes.
- Do not remove the production Habitat service token until
  request-driven lifecycle support exists (later phases).
