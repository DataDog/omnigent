// Check 5: offline behavior: graceful close vs abrupt drop; 503 with nothing persisted; liveness.
import { loginAs } from "../env.ts";
import { evidence, sleep, waitFor } from "../evidence.ts";
import { createHostlessSession, startRunner } from "../runner.ts";

const ev = evidence(5);
const alice = await loginAs("alice");
const message = (text: string) => ({ type: "message", data: { role: "user", content: [{ type: "input_text", text }] } });
const count = async (id: string) => ((await alice.items(id)).body.data ?? []).length;

// Mirrors web/src/hooks/useSessionLiveness.ts steps 7a / 7 for a dead, non-host-bound runner.
function classify(snap: any): string {
  if (snap.runner_online) return "online";
  if (snap.host_id) return "host-bound";
  if (snap.kind === "sub_agent") return "runner_asleep";
  return "local_stranded";
}

// (a) Graceful close of an idle session.
const graceful = await startRunner(alice);
const { session: idleSession } = await createHostlessSession(alice);
await alice.patchSession(idleSession.id, { runner_id: graceful.tunnel.runnerId });
const child = await graceful.runnerApi.postEvent(idleSession.id, { type: "external_acp_subagent_start", data: { subagent_id: "sub-offline", title: "offline child" } });
const childId = child.body?.child_session_id as string;
await graceful.runnerApi.postEvent(childId, { type: "external_session_status", data: { status: "idle" } });
graceful.tunnel.closeGraceful();
const gracefulClose = await graceful.tunnel.closed;
await sleep(1500);
const before = await count(idleSession.id);
const offline = await alice.postEvent(idleSession.id, message("while offline"));
const after = await count(idleSession.id);
const idleSnap = (await alice.getSession(idleSession.id)).body;
const childSnap = (await alice.getSession(childId)).body;
ev.log("graceful close", { code: gracefulClose.code, reason: gracefulClose.reason, status: idleSnap.status });
ev.log("send while offline", { status: offline.status, body: offline.body, itemsBefore: before, itemsAfter: after });
ev.expect("graceful close leaves session idle", idleSnap.status === "idle");
ev.expect("offline send -> 503 RUNNER_UNAVAILABLE", offline.status === 503 && JSON.stringify(offline.body).toLowerCase().includes("runner_unavailable"));
ev.expect("nothing persisted on offline send", before === after);
const childOffline = await alice.postEvent(childId, message("child while offline"));
ev.log("liveness inputs", {
  root: { runner_online: idleSnap.runner_online, host_id: idleSnap.host_id, kind: idleSnap.kind, host_online: idleSnap.host_online, classified: classify(idleSnap) },
  child: { runner_online: childSnap.runner_online, host_id: childSnap.host_id, kind: childSnap.kind, classified: classify(childSnap) },
  childSendWhileOffline: { status: childOffline.status, body: childOffline.body },
});
ev.expect("root classifies local_stranded", classify(idleSnap) === "local_stranded");
ev.expect("child classifies runner_asleep", classify(childSnap) === "runner_asleep");

// (b) Abrupt drop mid-turn.
const abrupt = await startRunner(alice);
const { session: busy } = await createHostlessSession(alice);
await alice.patchSession(busy.id, { runner_id: abrupt.tunnel.runnerId });
await alice.postEvent(busy.id, message("start turn"));
await abrupt.runnerApi.postEvent(busy.id, { type: "external_session_status", data: { status: "running" } });
abrupt.tunnel.dropAbrupt();
const droppedAt = Date.now();
const failed = await waitFor(async () => {
  const snap = (await alice.getSession(busy.id)).body;
  return snap.status === "failed" ? snap : undefined;
}, 150_000, 2000);
ev.log("abrupt drop", {
  failedAfterS: failed ? Math.round((Date.now() - droppedAt) / 1000) : null,
  status: failed?.status, error: failed?.error ?? failed?.last_error ?? failed?.failure_reason, labelsWithDisconnect: Object.entries(failed?.labels ?? {}).filter(([key]) => key.includes("disconnect") || key.includes("fail")),
});
ev.expect("abrupt drop -> failed after grace", Boolean(failed));
ev.finish();
