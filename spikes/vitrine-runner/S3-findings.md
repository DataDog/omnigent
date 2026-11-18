# S3 findings: prod probe for Vitrine as a hostless Omnigent runner

**Outcome: inconclusive. The stored CLI bearer had expired, and prod rejected it before either check could run.**
Nothing was created on prod and nothing needed cleanup.

## Approval
Nick approved the 9 calls below through the Lead, with bearer source (b): the omnigent CLI login,
`~/.omnigent/auth_tokens.json["https://omnigent.us1.ddbuild.io"].token`, read into memory only.
The run was `S3_PROD_APPROVED=1 S3_BEARER_FROM_OMNIGENT_CLI=1 npm run probe:prod`, executed exactly once, with no retries.

Prod base URL: `https://omnigent.us1.ddbuild.io`. Source: `omnigent-ops/k8s/omnigent-server/deploy.yaml:59` (`auth.baseUrl`).

## Approved call list (and what ran)
| # | Call | Ran? | Result |
|---|------|------|--------|
| 1 | WS `GET /v1/runners/{runner_token_…}/tunnel` + hello | yes | **HTTP 403** on upgrade; no frames exchanged |
| 2 | `GET /v1/runners/{id}/status` | yes | **401** `unauthorized: Authentication required` |
| 3 | `GET /v1/agents` | yes | **401** |
| 4 | `POST /v1/sessions` (hostless, acp:vitrine, spike label) | no | skipped: no agent_id (step 3 failed) |
| 5 | `GET /v1/sessions/{id}/permissions` | no | no session |
| 6 | `DELETE /v1/sessions/{id}` | no | no session was created |
| 7 | WS close 1000 "runner idle shutdown" | n/a | the socket never opened; the client recorded close 1006 |
| 8 | `GET /v1/sessions/{id}` read-back | no | no session |
| 9 | `GET /v1/runners/{id}/status` read-back | yes | **401**, so offline state couldn't be read with this bearer |

## Evidence (redacted, UTC 2026-11-18)
```
20:34:15.254Z B1.connect   runnerId=runner_token_18d1cc4d572ef1b14583220d41afb9a3
20:34:15.527Z B1.handshake opened=error:HTTP 403
20:34:18.793Z B1.status    401 {"error":{"code":"unauthorized","message":"Authentication required"}}
20:34:18.874Z B8.agents    401
20:34:18.875Z error        no agent id available
20:34:18.875Z tunnel.close 1006 ""
20:34:20.964Z readback.runner 401 {"error":{"code":"unauthorized","message":"Authentication required"}}
```
Root cause: the stored token's `expires_at` is 1794879749 (about 2026-11-17 00:22Z), about 44 hours before the run.
The probe deliberately has no token refresh. The REST 401s show that the bearer itself was rejected, so the tunnel 403 is
the same auth failure. It is not evidence of a runner allow-list.

## Cleanup proof
- Step 4 never ran, so no session was created. The tunnel never opened (the upgrade was refused), so no runner was ever registered.
- Read-back 9 returned 401, so the server has no authenticated view to confirm offline state. But a refused upgrade cannot leave a runner online.

## Go / no-go
- **B1 (prod accepts Vitrine's tunnel with a real bearer):** **no verdict.** This run says nothing about the allow-list.
  It needs a re-run with a fresh login (`omnigent login --server https://omnigent.us1.ddbuild.io`) and fresh approval.
- **B8 (a `__public__` READ grant is added automatically to a hostless session):** **no verdict.** No session was created.

## Caveats
- **Auth type:** the CLI token may be an Omnigent session token issued after OIDC login, not the raw OIDC id_token.
  In this run the server accepted neither form, because the token had expired. A re-run should note which form a fresh token is.
- **S1 contrast:** S1 authenticated with an accounts JWT on a local server, not real OIDC. Its allow-list refusal was simulated with
  `X-Forwarded-For`, because loopback bypasses the allow-list. A live S3 handshake with a real prod bearer is still the remaining auth check, and it is **still open**.
- **Scope:** the probe has no reconnect/backoff, token refresh or `event.batch`. S1's no-op handling of non-message requests is
  not a production stance. Interrupts and approvals will need real handling later.
