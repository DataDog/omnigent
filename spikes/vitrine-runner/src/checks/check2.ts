// Check 2: hello + init + heartbeat stream keep the runner online; ping cadence; silent -> 4003.
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { RUN_DIR, loginAs } from "../env.ts";
import { evidence, sleep, waitFor } from "../evidence.ts";
import { createHostlessSession, startRunner } from "../runner.ts";

const ev = evidence(2);
const alice = await loginAs("alice");

// Runner A: bound session, answers pings.
const openedAt = Date.now();
const runnerA = await startRunner(alice);
const { session } = await createHostlessSession(alice);
const bindAt = Date.now();
await alice.patchSession(session.id, { runner_id: runnerA.tunnel.runnerId });
const firstBeat = await waitFor(() => runnerA.routes.streams.has(session.id) && Date.now(), 5000, 20);
ev.expect("heartbeat stream ready within 5s of bind", Boolean(firstBeat), { ms: firstBeat ? firstBeat - bindAt : null });

// Runner B: unbound, silent from the start (no pongs, no frames at all).
const runnerB = await startRunner(alice);
runnerB.tunnel.setSilent(true);
const silentAt = Date.now();

const logDir = join(RUN_DIR, "data", "logs", "server");
const connectedLine = readdirSync(logDir).flatMap((file) => readFileSync(join(logDir, file), "utf-8").split("\n"))
  .find((line) => line.includes(runnerA.tunnel.runnerId) && line.includes("connected"));
ev.log("server log on hello", { line: connectedLine?.replace(/^.*\| /, "") });

for (let tick = 0; tick < 7; tick += 1) {
  await sleep(10_000);
  const snap = (await alice.getSession(session.id)).body;
  ev.log("tick", { s: Math.round((Date.now() - openedAt) / 1000), runner_online: snap.runner_online, status: snap.status, pingsA: runnerA.tunnel.pings.length });
}
const pingOffsets = runnerA.tunnel.pings.map((at) => Math.round((at - openedAt) / 100) / 10);
ev.log("runner A ping times (s since open)", { pingOffsets });
ev.expect("first ping ~30s after open", pingOffsets.length > 0 && pingOffsets[0] >= 28 && pingOffsets[0] <= 33, { first: pingOffsets[0] });
ev.expect("runner A still online after 70s", (await alice.getSession(session.id)).body.runner_online === true);

const closeB = await Promise.race([runnerB.tunnel.closed, sleep(80_000).then(() => null)]);
ev.log("runner B silent close", closeB ? { code: closeB.code, reason: closeB.reason, silentForS: Math.round((closeB.atMs - silentAt) / 1000) } : "no close within 150s");
ev.expect("silent runner closed 4003 ping timeout between 90 and 125s",
  closeB !== null && closeB.code === 4003 && closeB.reason === "ping timeout" && closeB.atMs - silentAt >= 90_000 && closeB.atMs - silentAt <= 125_000);
runnerA.tunnel.closeGraceful();
await runnerA.tunnel.closed;
ev.finish();
