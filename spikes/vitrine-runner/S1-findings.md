# S1 findings: Vitrine as a hostless Omnigent runner

Throwaway spike. Do not merge. All citations are `main@86258587` unless noted.
The desk report is `omnigent-local-session-mirroring-report.md` (Vitrine repo); its
citations are at `c702c5e22e`.

## 1. Verdict: conditional GO for Phase 2 egress

- **Go on the protocol.** A ~600-line Node runner using only public surfaces (runner
  tunnel, `POST/PATCH /v1/sessions`, `POST /events` with `external_*` events) passed
  all 9 checks against an auth-enabled local server. No server change was needed.
- **Condition B1 (prod posture).** Prod must keep `OMNIGENT_RUNNER_TUNNEL_TOKEN` unset.
  With the allow-list set, a non-loopback Vitrine runner is refused (4004 "runner tunnel
  token is not authorized"). Only loopback peers fall back to token-bound ids.
- **Condition B5 (author attribution).** Use `GET /items` to resolve `created_by`
  for `persisted_item_id` (p50 26 ms, max 31 ms, visible on the first GET in 20/20).
  Do **not** use `omnigent.turn_actor`: it was wrong in a back-to-back race.
- **Known web gaps (B3/B4).** Proxied routes the runner 404s look like plain
  "not found" or a 502 for `/resources`. Root sessions show `local_stranded` when
  Vitrine is closed. Phase 2 should either serve stubs or accept these states.
- **Real OIDC is not exercised.** The bearer is an accounts-mode session JWT on the
  same `Authorization: Bearer` path OIDC uses. S3 must confirm against the prod IdP.

## 2. Environment

| Item | Value |
|------|-------|
| Base | `86258587` (= `origin/main` at start) |
| Checks 1–3 commit | `616fd4252c` |
| Node | 26.12 (`--experimental-strip-types`; the floor is 22.6) |
| Dependencies | `ws@8.18.0` only (plus `@types/*` dev deps) |
| Server | `omnigent server`, `OMNIGENT_AUTH_PROVIDER=accounts`, port 18766, isolated DB and data dir under `.run/` |
| Users | `alice` (first-run admin, session owner), `bob` (invited, EDIT grant level 2) |
| Agent | first built-in agent that accepts `harness_override:"acp:vitrine"` (`jcode`) |

## 3. Results

| # | Check | Result | Evidence | Citations |
|---|-------|--------|----------|-----------|
| 1 | Handshake: bearer + self-minted token | PASS | Opened with `Origin: omnigent://internal`; `runner_id == tokenBoundRunnerId(token)` (Python vector matches); alice lists it online, bob does not; a wrong path id gives 4004 "runner_id does not match tunnel token" | `runner_tunnel.py:194-226,430-475`; `runner/identity.py:226-243` |
| 2 | hello, init, heartbeat, ping/miss | PASS | Log line "Runner … connected (… harnesses=['acp', 'acp:vitrine'])"; stream ready 16 ms after bind; pings at 30 s and 60 s; online throughout 70 s; a silent runner got 4003 "ping timeout" 90 s after its last frame | `runner_tunnel.py:64-67,1073-1160`; `_sessions/common.py:581` |
| 3 | Hostless create + bind + fence | PASS | Create 201, `host_id` null; PATCH `runner_id` 200, then init POST and `GET /stream?accepts_inferred_status=true`; `external_session_id` first 200, same 200, different 400 `invalid_input` | `routes_core.py:3648+,4347-4371`; `orchestration.py:9740` |
| 4 | Web-send round trip (owner + EDIT) | PASS | Both sends 202 `{queued,item_id}`; forwarded body has `persisted_item_id`, no `created_by`; runner 202; 5 ingest posts all 202; items user (`created_by`=sender) then assistant; status idle; follow-up forwarded in 8–11 ms; same `source_id` repost 202 with an unchanged count | `orchestration.py:7725-7774,3238-3660` |
| 5 | Offline | PASS | Graceful 1000 "runner idle shutdown" leaves idle; send gives 503 `runner_unavailable` "No runner bound for session" with items 1→1; abrupt drop mid-turn gives `failed` after 91 s (grace = `RUNNER_LIVENESS_TTL_S` 90); root → `local_stranded`, child (`kind:sub_agent`) → `runner_asleep` but child sends also 503 | `server/app.py:4600-4680`; `routes_events.py:3905-3908`; `web/src/hooks/useSessionLiveness.ts:320-347` |
| 6 | 404 tolerance | PASS (with gaps) | See table §4.6. No crash, no ERROR logs; WARN for `/resources` and `/mcp/execute` | §4.6 |
| 7 | B5 author lookup | PASS | n=20: p50 25.8 ms, p95/max 30.8 ms (one session GET + one items GET); 20/20 visible on first GET; race: `turn_actor` at forward = alice for **bob's** item | `orchestration.py:7755,7774` |
| 8 | B1 allow-list | PASS (behavior recorded) | Allow-list set: non-loopback peer (`X-Forwarded-For`) gets 4004 "runner tunnel token is not authorized"; loopback is accepted and listed (fallback). Control without allow-list: same remote peer accepted. B2: prod is one replica, so not tested | `runner_tunnel.py:211-226,447-468`; `cli.py:4986-4989` |
| 9 | Echo / replay | PASS | Runner's own `external_session_status` (root and child) is forwarded back to it with `created_by`, answered 202, no turn; `subagent.status` not forwarded. Drop mid-turn and reconnect with the same token: new init (`recovery_id` null, `resume_interrupted_turn` false), **no** replay of the message; ledger executions stayed 1; finished turn → idle | `runner/app.py:3619-3662` |

## 4. Per-check evidence (redacted)

Raw evidence lands in `.run/evidence/checkN.jsonl` and `.run/frames.jsonl` (gitignored).

### 4.1 Handshake extras
- An empty token over loopback gives **HTTP 500** on the upgrade, not a clean close. The
  loopback fallback re-calls `_expected_runner_id_from_headers` with no allow-list, which
  raises `ValueError` again uncaught (`runner_tunnel.py:453-468`). Minor server bug.
- A missing Origin is accepted, and so is `Origin: https://evil.example`.
  `origin_allowed` passes everything in non-local modes when no allow-list is set
  (`server/ws_origin.py:211-270`). The sentinel is required only for local single-user mode.
- No bearer over loopback is accepted; the owner is `local`, so alice does not see it.

### 4.3 Frames seen after bind
`POST /v1/sessions` (init: `session_id, agent_id, sub_agent_name, session_init`), then
`GET /stream?accepts_inferred_status=true`, then **`GET /v1/sessions/{id}` on every
PATCH and snapshot read**. The spike 404s that GET; the server tolerates it silently.

### 4.4 Forwarded message body keys
`type, role, content, agent_id, model, has_mcp_servers, browser_renderer_available,
persisted_item_id, agent_revision, harness_override`.
Shapes that worked:
- the web message: `{type:"message", data:{role:"user", content:[{type:"input_text",…}]}}`;
- the assistant item: `{type:"external_conversation_item", data:{item_type:"message",
  item_data:{role:"assistant", agent:"vitrine", content:[{type:"output_text",…}]}, source_id}}`.
  The `agent` field is required.
- `--relay-status` (`session.status` on the relay stream) was not needed: HTTP idle settled dispatch.

### 4.6 Proxied routes when the runner 404s

| Family | Caller path | Reached runner | Caller sees | Server log | Class |
|--------|-------------|----------------|-------------|------------|-------|
| resources | `GET …/resources` | yes | **502** "runner session-resources endpoint failed" | WARN "runner returned 404" | user-visible error |
| resource by id | `GET …/resources/{rid}` | yes | 404 not_found | none | user-visible (empty) |
| terminals | `GET …/resources/terminals` | yes | 404 | none | user-visible (empty) |
| terminal create | `POST …/resources/terminals` | no | 400 "not declared by this agent" | none | nothing (gated by agent spec) |
| environments | `GET …/resources/environments` | yes | 404 | none | user-visible (empty) |
| github | `GET …/resources/github` | yes | 404 | none | user-visible (empty) |
| repository-branches | `GET …/resources/repository-branches` | no | 200 `{repositories:[]}` | none | nothing |
| /mcp/execute | `POST …/mcp` (tools/list, tools/call) | yes | 200 JSON-RPC error -32000 "Runner MCP execute failed." | WARN | user/agent-visible error |
| /skills/resolve | skill slash command only | no (agent has no skills) | — | — | code-read: `helpers.py:8154-8300`, fails the slash command |
| /models, /model-options | native-wrapper label only | no | — | — | code-read: `orchestration.py:7180-7230`, `common.py:1042`; not used for `acp:vitrine` |
| /sign-in-link | `GET …/sign-in-link` | yes | 404 | none | user-visible (empty) |
| session GET | `GET /v1/sessions/{id}` | yes (every read) | 200 (server snapshot) | none | nothing |

### 4.7 Race detail
alice and bob posted at the same time. Both items were forwarded; each was resolved to the
correct `created_by` via `/items`, while the `turn_actor` label read at forward time was
"alice" for both.

### 4.9 Echo detail
Echoed bodies carry `type, data, model_override, tools, created_by`. A Vitrine runner must
treat any non-`message` forward as a no-op 2xx. The ledger keyed on `persisted_item_id`
was never hit because the server did not replay. Keep it anyway as cheap insurance.
Reconnect here was the same process with a fresh route table and the on-disk ledger, not
a separate OS process.

## 5. Surprises and drift vs `c702c5e22e`

| Report cite / claim | On `main@86258587` |
|---|---|
| PING constants `runner_tunnel.py:58-60` | `:64-67` (30 s, 3 misses, 4004 mismatch code) |
| PATCH bind and init `routes_core.py:2586-2591,2679-2690,2995-3010` | bind branch at `:3648+`; MCP-registry support gate and `WRONG_REPLICA` checks added (`:406-431`) |
| `external_session_id` set-once | `routes_core.py:4347-4371`; the same value is idempotent 200 |
| Runner body with `persisted_item_id` `orchestration.py:7023-7070` | `:7725-7756`; `turn_actor` write at `:7774` |
| Relay stream | now `/stream?accepts_inferred_status=true` (`:9740`), 45 s read timeout; ready timeout 5 s (`common.py:581`) |
| Q8 `initial_input` trap | blocked by schema: requires `host_type=managed` (`schemas.py:1815,2061`) |
| hello / pong shape | hello has `capabilities`, `connection_id`, `direct_attach_*`; pong has `keep_warm` |
| HTTP-only ingest | tunnel `event.ready/batch/ack` ingest exists under `session-event-ingest-v1` (`frames.py:214`), a Phase 2 option |
| `_authorized_conversation` `routes_events.py:819-879` | `:1078-1117` |
| Allow-list | loopback fallback at `runner_tunnel.py:453-468` makes B1 unreproducible from loopback |
| Disconnect grace | `RUNNER_DISCONNECT_GRACE_S = RUNNER_LIVENESS_TTL_S = 90` (`orchestration.py:8980`, `conversation_store/__init__.py:369`) |
| (new) | the server proxies `GET /v1/sessions/{id}` to the runner on reads |
| (new) | assistant `MessageData` requires `agent` |
| (new) | 503 message is "No runner bound for session" (`routes_events.py:3905`) rather than the host-offline text |

## 6. Server gaps (findings only, no changes)

- **B1.** The allow-list posture refuses remote hostless runners; prod must stay allow-list-free.
- **B3.** `/resources` turns a runner 404 into a 502; other resource families show an empty 404.
- **B4.** A root hostless session reads `local_stranded` offline. Child sends 503 even
  though the web shows `runner_asleep` with the composer open.
- **B5.** The forwarded body has no `created_by`; `turn_actor` is racy. Suggested fix:
  add `created_by` to the runner body at `orchestration.py:7725-7756`.
- **Minor.** An empty tunnel token over loopback gives an HTTP 500 on the upgrade. Origin
  is unchecked in non-local modes. Runner echoes include `created_by`, which the
  message forward lacks.

## 7. Run instructions

Prerequisites: `uv`, Node ≥ 22.6, the repo venv (`OMNIGENT_SKIP_WEB_UI=true uv sync` is
enough), and `npm ci` in `spikes/vitrine-runner/`.

```bash
cd spikes/vitrine-runner
npm ci && npm test                     # runner-id vector + frame round trip
scripts/start-server.sh &              # accounts auth, 127.0.0.1:18766, state in .run/
npm run bootstrap                      # creates alice (admin) + bob; creds in .run/users.json
npm run check:1   # … through check:9 except check:8
# check 8a: allow-list set -> remote peer refused, loopback accepted
kill %1; TUNNEL_ALLOWLIST=$(openssl rand -hex 16) scripts/start-server.sh &
npm run check:8
# check 8b (control): no allow-list -> remote peer accepted, loopback accepted
kill %1; scripts/start-server.sh &
EXPECT_ALLOWLIST=0 npm run check:8
rm -rf .run                            # cleanup
```

`check:2` and `check:5` take about 2.5 minutes each (ping-miss and disconnect grace).

## 8. Recommendation and S3 hand-off

Proceed with Phase 2 egress as a hostless runner. Resolve authors via `/items` until the
server adds `created_by` to forwards. Treat every non-`message` forward as a no-op 2xx,
keep a `persisted_item_id` ledger, and serve `GET /v1/sessions/{id}` and the resource
families with empty 200 stubs to avoid the 502.

`src/tunnel.ts` already covers:
- `mintBindingToken` and `tokenBoundRunnerId` (tested against Python);
- the bearer, token and Origin headers, and the hello with `acp` and `acp:vitrine`;
- ping/pong, framed HTTP dispatch with streaming and cancel, and `ws.open` refusal;
- graceful (1000 "runner idle shutdown") and abrupt closes.

S3 should add reconnect with backoff, real OIDC bearer refresh, and optionally tunnel
`event.batch` ingest.
