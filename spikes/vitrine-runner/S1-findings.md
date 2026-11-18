# S1 findings: Vitrine as a hostless Omnigent runner

Throwaway spike. Do not merge. Base: `main@86258587`.

## Results (partial: checks 1 to 3)

| # | Check | Result | Evidence |
|---|-------|--------|----------|
| 1 | Tunnel handshake | PASS | Opened with bearer, binding token and `Origin: omnigent://internal`; `runner_id == tokenBoundRunnerId(token)`; alice lists it online, bob does not; a wrong path id closes 4004 "runner_id does not match tunnel token" |
| 2 | Liveness | PASS | Heartbeat stream ready 16 ms after bind; pings at 30 s and 60 s; a silent runner closed 4003 "ping timeout" after 90 s |
| 3 | Hostless create and bind | PASS | Create 201 with `host_id` null; PATCH `runner_id` 200 then init POST and `/stream?accepts_inferred_status=true`; `external_session_id` same value 200, different value 400 `invalid_input` |
| 4–9 | | pending | |
