// Check 3: hostless create + PATCH runner_id bind + external_session_id set-once fence.
import { loginAs } from "../env.ts";
import { evidence, waitFor } from "../evidence.ts";
import { createHostlessSession, startRunner } from "../runner.ts";

const ev = evidence(3);
const alice = await loginAs("alice");
const runner = await startRunner(alice);
const { session, channelId, agentId, status } = await createHostlessSession(alice);
ev.log("create", { status, id: session.id, agentId, host_id: session.host_id, runner_id: session.runner_id, harness_override: session.harness_override });
ev.expect("create 2xx without host_id/initial_input", status < 300 && !session.host_id);

const bind = await alice.patchSession(session.id, { runner_id: runner.tunnel.runnerId });
ev.log("PATCH runner_id", { status: bind.status, body: bind.status >= 300 ? bind.body : { runner_id: bind.body.runner_id } });
ev.expect("PATCH runner_id 2xx", bind.status < 300);
const init = await waitFor(() => runner.routes.inits.find((entry) => entry.sessionId === session.id), 5000);
ev.expect("bind triggered init POST on runner", Boolean(init), { initKeys: init ? Object.keys(init.body) : [] });
const streamOpen = await waitFor(() => runner.routes.streams.has(session.id), 5000);
ev.expect("relay opened heartbeat stream", Boolean(streamOpen), {
  streamFrame: runner.routes.frames.find((frame) => frame.path.endsWith("/stream")),
});

const externalId = `vitrine:${channelId}`;
const first = await alice.patchSession(session.id, { external_session_id: externalId });
const same = await alice.patchSession(session.id, { external_session_id: externalId });
const other = await alice.patchSession(session.id, { external_session_id: "vitrine:other-channel" });
ev.log("external_session_id PATCHes", {
  first: first.status, same: same.status, other: { status: other.status, body: other.body },
});
ev.expect("set-once: first 2xx", first.status < 300);
ev.expect("set-once: different value rejected 400", other.status === 400);

const snapshot = await alice.getSession(session.id);
const snap = snapshot.body;
ev.log("snapshot", {
  host_id: snap.host_id, runner_id: snap.runner_id, runner_online: snap.runner_online,
  external_session_id: snap.external_session_id, harness_override: snap.harness_override, status: snap.status,
});
ev.expect("snapshot host_id null and runner bound", !snap.host_id && snap.runner_id === runner.tunnel.runnerId);
ev.log("frames seen", runner.routes.frames.map((frame) => `${frame.method} ${frame.path}${frame.query ? "?" + frame.query : ""}`));
runner.tunnel.closeGraceful();
await runner.tunnel.closed;
ev.finish();
