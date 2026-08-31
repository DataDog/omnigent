# Ticino OBO Staging Acceptance

## Expected staging failures (before prerequisites are deployed)

1. **Login cannot complete** without the Ticino discovery and ID-token
   work — the OIDC discovery document must publish `authorization_endpoint`,
   `token_endpoint`, and `jwks_uri` under the same issuer used in signed
   tokens, and the authorization-code flow must return a signed ID token
   with `aud=<Omnigent OAuth client ID>`.

2. **Habitat provisioning cannot exchange** without the `dd-source`
   `dd_internal_authentication` client and workload policy — the
   `omnigent_hab_launcher` calls the shared Python token-exchange client
   with the workload bearer and the user ID token as `subject_token`
   (RFC 8693), requesting only `audience=hab`.

3. **Operation after refresh expiry cannot continue** — when the
   provider session's absolute expiry (roughly 24 hours) is reached or
   Ticino rejects a refresh, the token manager clears credentials and
   returns a reauthentication error; no silent service-identity fallback
   exists.

## Staging acceptance sequence

1. Google → Ticino → Omnigent login.
2. Create a managed Habitat session; confirm Habitat records the user
   as owner and Omnigent as actor.
3. List/connect/delete through the launcher without placing provider or
   OBO tokens in the Hab.
4. Expire the original user ID token; prove an operation refreshes it
   and succeeds.
5. Restart Omnigent; prove the same provider session and Hab binding
   still work.
6. Log out or revoke refresh; prove future Habitat operations require
   sign-in.
7. Sign in again as the same user and operate the existing Hab.
8. Prove a second user cannot operate it.
9. Confirm Omnigent ↔ sandbox and Habitat/hablet ↔ sandbox connectivity
   is unchanged; OIDC/OBO only changes API identity.

## Prerequisites (tracked in other codebases)

- Ticino: discovery, public-client login, renewable ID token,
  client/workload binding.
- dd-source: `dd_internal_authentication` exchange client and
  `omnigent_hab_launcher` changes.
- Habitat: OBO verifier and audit changes.
- Chart: cookie signing key, credential encryption key, exact egress,
  Habitat token removal.
- Balto policy: Habitat-only workload identity.
