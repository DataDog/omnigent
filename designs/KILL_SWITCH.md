# Service kill switch

A service-wide switch that stops Omnigent from doing new agent work without a
restart or redeploy. It is backed by a boolean flag in Datadog Feature Flags.

## What it does

While **engaged**:

- `POST /v1/sessions` (including runner-created sub-agent sessions and managed
  startup inputs), `POST /v1/sessions/{id}/fork` and turn-starting events
  (`message`, `compact`, `slash_command`, `retry_session`, `approval`,
  `mcp_elicitation`) return **HTTP 503** with code `service_disabled` and the
  configured message.
- Running turns are interrupted as soon as the switch engages, and again on
  every poll, so a turn started later (for example typed into a native
  terminal) is stopped too. Each replica halts the sessions whose runner
  tunnel it holds. Codex side chats are interrupted directly through their
  parent's runner, even when the parent is interrupted too. Forced mode sweeps
  the same way.
- Scheduled task fires are recorded as `skipped` with
  `error_code=kill_switch_engaged`.
- `POST /v1/sessions/{id}/policies/evaluate` denies tool-call and LLM-request
  phases, so native harnesses that consult server policy stop mid-turn.

Always available: `/health` (always 200), `/v1/info`, `/v1/kill-switch`, every
GET/read endpoint, `interrupt` and `stop_session` events, and runner
`external_*` event ingestion (so output from halted turns is still saved).

## Configuration

Install the optional extra: `omnigent[datadog-flags]`, or
`OMNIGENT_EXTRAS=datadog-flags` for deploy images. It is not part of `all`.
Only `ddtrace.openfeature` is imported; tracing is not enabled.

| Variable | Default | Meaning |
|---|---|---|
| `OMNIGENT_KILL_SWITCH_FLAG_KEY` | unset | Boolean flag key. Unset turns the feature off. |
| `OMNIGENT_KILL_SWITCH_POLL_INTERVAL_S` | `5` | Seconds between evaluations. |
| `OMNIGENT_KILL_SWITCH_INIT_TIMEOUT_S` | `10` | Max wait for the first evaluation at startup. |
| `OMNIGENT_KILL_SWITCH_MESSAGE` | generic text | Client-facing reason in the 503 body. |
| `OMNIGENT_KILL_SWITCH_FORCE` | unset | Break-glass: `engaged` or `disengaged`, ignores the flag. |

Invalid values fail startup. A flag key set without the extra installed also
fails startup with an install hint.

Datadog environment used by ddtrace: `DD_API_KEY`, `DD_SITE`, `DD_ENV`,
`DD_SERVICE`, optionally `DD_VERSION`. The flag configuration comes from the
Datadog CDN by default; set `DD_FEATURE_FLAGS_CONFIGURATION_SOURCE=remote_config`
to read it through Agent Remote Config instead.

## Creating the flag

1. In Datadog Feature Flags, create a **boolean** flag (for example
   `omnigent-kill-switch`) with default `false` in every environment.
2. Evaluation context: `targeting_key` = `DD_SERVICE` (or `omnigent-server`),
   attributes `env`, `version` and `replica` (hostname). Use targeting rules
   on these to engage one environment, version or replica.
3. Set `OMNIGENT_KILL_SWITCH_FLAG_KEY` on the server and redeploy once.

## Runbook

**Engage**: set the flag to `true` for the target environment.

**Verify** (within one poll interval):

- `GET /v1/kill-switch` shows `"engaged": true`, `"source_status": "ready"`.
- `GET /health` shows `"kill_switch": "engaged"`.
- A new message returns 503 with `service_disabled`.
- Metric `omnigent.kill_switch.engaged` is 1 and `halted_turns` increments.
- Server log line `kill_switch_engaged` (WARNING).

**Disengage**: set the flag back to `false`; verify the same signals revert
and look for the `kill_switch_disengaged` log line.

## Metrics and monitors

Meter `omnigent.server.kill_switch`:

- `omnigent.kill_switch.engaged` (gauge, 0/1)
- `omnigent.kill_switch.flag_refresh` (counter, `outcome=ok|unavailable`)
- `omnigent.kill_switch.staleness` (gauge, seconds since last successful evaluation, or since startup if none succeeded)
- `omnigent.kill_switch.rejected_requests` (counter, by `operation`)
- `omnigent.kill_switch.halted_turns` (counter, `outcome=delivered|not_delivered|failed`)

Recommended monitors:

- `engaged == 1` for any replica: notify the owning team.
- `staleness > 300s` while configured: the provider is not ready or reports
  stale/error evaluations (see Limits for outages it cannot see).
- Spike in `rejected_requests`: measures client impact while engaged.

## Failure modes

- **Provider not ready or evaluations stale/erroring**: the last known state
  is kept; `source_status` becomes `unavailable`, `last_error` is set and
  staleness grows. A server that never got a reading stays disengaged.
- **Datadog unreachable after the provider got a config**: ddtrace keeps
  serving its cached flag config, so readings keep succeeding with the cached
  value and staleness does not grow. Flag changes made during the outage are
  not seen until the config refreshes.
- **Restart while engaged**: startup waits up to the init timeout for the first
  evaluation, retrying while the Datadog provider is still initializing, so the
  service does not briefly reopen. The scheduler starts only after that. On
  timeout it starts disengaged and logs a warning.
- **Feature Flags itself broken**: set `OMNIGENT_KILL_SWITCH_FORCE` and restart.

## Limits

- Halting depends on the runner accepting the interrupt; refusals are counted
  as `halted_turns{outcome=not_delivered}` and retried on the next poll.
- Native-terminal turns are stopped by the sweep and the policy deny, not
  prevented.
- The switch is service-wide; per-workspace control is out of scope.
- Staleness counts a reading as fresh when the provider is READY and the
  evaluation reason is not `STALE` or `ERROR`. ddtrace exposes no signal for
  "config fetch failing while cached config is served", so an outage that keeps
  serving cached flags with a normal reason is not visible as staleness.
