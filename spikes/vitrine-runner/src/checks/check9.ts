// Check 9: echo of runner-posted status edges; recovery replay after an abrupt drop.
import { join } from "node:path";
import { RUN_DIR, loginAs } from "../env.ts";
import { evidence, sleep, waitFor } from "../evidence.ts";
import { runFakeTurn } from "../fake-turn.ts";
import { createHostlessSession, startRunner } from "../runner.ts";
import { mintBindingToken } from "../tunnel.ts";

const ev = evidence(9);
const alice = await loginAs("alice");
const ledgerFile = join(RUN_DIR, `ledger-check9-${Date.now()}.json`);
const bindingToken = mintBindingToken();
let executions = 0;
const message = (text: string) => ({ type: "message", data: { role: "user", content: [{ type: "input_text", text }] } });

// (a) Echo: root and child status edges the runner posts itself.
const first = await startRunner(alice, { bindingToken, ledgerFile, onMessage: () => { executions += 1; } });
const { session, channelId } = await createHostlessSession(alice);
await alice.patchSession(session.id, { runner_id: first.tunnel.runnerId });
await first.runnerApi.postEvent(session.id, { type: "external_session_status", data: { status: "running" } });
await first.runnerApi.postEvent(session.id, { type: "external_session_status", data: { status: "idle" } });
await sleep(500);
const rootEchoes = first.routes.events.filter((event) => event.sessionId === session.id && event.body.type !== "message");
ev.log("root echoes forwarded to runner", rootEchoes.map((event) => ({ type: event.body.type, status: event.body.data?.status ?? event.body.status, keys: Object.keys(event.body) })));
const child = await first.runnerApi.postEvent(session.id, { type: "external_acp_subagent_start", data: { subagent_id: "sub-1", title: "spike child" } });
const childId = child.body?.child_session_id as string | undefined;
ev.log("child create", { status: child.status, childId });
if (childId) {
  const childStatus = await first.runnerApi.postEvent(childId, { type: "external_session_status", data: { status: "running" } });
  const childIdle = await first.runnerApi.postEvent(childId, { type: "subagent.status", data: { idle: true } });
  const childDone = await first.runnerApi.postEvent(childId, { type: "external_session_status", data: { status: "idle" } });
  await sleep(500);
  ev.log("child posts", { running: childStatus.status, subagentIdle: childIdle.status, idle: childDone.status });
  ev.log("child echoes forwarded to runner", first.routes.events.filter((event) => event.sessionId === childId).map((event) => ({ type: event.body.type, keys: Object.keys(event.body) })));
}
ev.expect("echo forwards answered 2xx and started no turn", executions === 0);

// (b) Abrupt drop mid-turn, reconnect as a fresh route table with the ledger on disk.
const sent = await alice.postEvent(session.id, message("replay me"));
const forwarded = await waitFor(() => first.routes.events.find((event) => event.body.type === "message"), 5000);
const itemId = forwarded?.body.persisted_item_id;
await first.runnerApi.postEvent(session.id, { type: "external_session_status", data: { status: "running" } });
ev.log("sent + forwarded", { status: sent.status, itemId, executions });
first.tunnel.dropAbrupt();
await first.tunnel.closed;
await sleep(2000);
const midSnap = (await alice.getSession(session.id)).body;
ev.log("after drop", { status: midSnap.status, runner_online: midSnap.runner_online });

const second = await startRunner(alice, { bindingToken, ledgerFile, onMessage: () => { executions += 1; } });
const reinit = await waitFor(() => second.routes.inits.find((entry) => entry.sessionId === session.id), 15_000);
const recovery = reinit?.body.session_init;
ev.log("recovery init", {
  received: Boolean(reinit),
  sessionInitKeys: recovery ? Object.keys(recovery) : null,
  recovery_id: recovery?.recovery_id,
  resume_interrupted_turn: recovery?.resume_interrupted_turn,
  snapshotKeys: recovery?.snapshot ? Object.keys(recovery.snapshot) : null,
  mentionsItemId: JSON.stringify(reinit?.body ?? {}).includes(itemId ?? "none"),
});
await sleep(5000);
const replays = second.routes.events.filter((event) => event.body.type === "message");
ev.log("replayed message forwards", replays.map((event) => ({ persisted_item_id: event.body.persisted_item_id, deduped: event.replayed })));
ev.expect("replayed persisted_item_id not re-executed", executions === 1, { executions });
await runFakeTurn(second.runnerApi, session.id, channelId, "finished after reconnect");
await sleep(500);
ev.log("final", { status: (await alice.getSession(session.id)).body.status });
second.tunnel.closeGraceful();
await second.tunnel.closed;
ev.finish();
