// Check 7 (B5): cost of resolving created_by for a forwarded persisted_item_id.
import { loginAs } from "../env.ts";
import { evidence, sleep } from "../evidence.ts";
import { runFakeTurn } from "../fake-turn.ts";
import type { ForwardedEvent } from "../runner-routes.ts";
import { createHostlessSession, startRunner } from "../runner.ts";

const ev = evidence(7);
const alice = await loginAs("alice");
const bob = await loginAs("bob");
const lookups: Promise<{ firstHit: boolean; ms: number; createdBy: string | null; turnActor: string | null; itemId: string }>[] = [];
let runnerApi: any;
let channel = "";
const runFake = process.argv.includes("--no-turn") ? false : true;

async function resolveAuthor(event: ForwardedEvent) {
  const started = performance.now();
  const itemId = event.body.persisted_item_id as string;
  const turnActor = (await alice.getSession(event.sessionId)).body.labels?.["omnigent.turn_actor"] ?? null;
  let firstHit = true;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const items = (await runnerApi.items(event.sessionId)).body.data ?? [];
    const item = items.find((candidate: any) => candidate.id === itemId);
    if (item) return { firstHit, ms: performance.now() - started, createdBy: item.created_by ?? null, turnActor, itemId };
    firstHit = false;
    await sleep(20);
  }
  return { firstHit: false, ms: performance.now() - started, createdBy: null, turnActor, itemId };
}

const runner = await startRunner(alice, {
  onMessage: (event) => {
    lookups.push(resolveAuthor(event));
    if (runFake) void runFakeTurn(runnerApi, event.sessionId, channel);
  },
});
runnerApi = runner.runnerApi;
const { session, channelId } = await createHostlessSession(alice);
channel = channelId;
await alice.patchSession(session.id, { runner_id: runner.tunnel.runnerId });
await alice.grant(session.id, "bob", 2);
const message = (text: string) => ({ type: "message", data: { role: "user", content: [{ type: "input_text", text }] } });

const senders: string[] = [];
for (let index = 0; index < 20; index += 1) {
  const [name, api] = index % 2 === 0 ? ["alice", alice] as const : ["bob", bob] as const;
  senders.push(name);
  await api.postEvent(session.id, message(`b5 ${index}`));
  await sleep(400);
}
const results = await Promise.all(lookups);
const sorted = results.map((result) => result.ms).sort((left, right) => left - right);
const pct = (fraction: number) => Math.round(sorted[Math.min(sorted.length - 1, Math.floor(fraction * sorted.length))] * 10) / 10;
ev.log("lookup latency ms (GET /items as runner)", { n: sorted.length, p50: pct(0.5), p95: pct(0.95), max: pct(1) });
ev.expect("item visible on first GET at forward time", results.every((result) => result.firstHit), { firstHits: results.filter((result) => result.firstHit).length });
ev.expect("created_by matches sender", results.every((result, index) => result.createdBy === senders[index]));
ev.expect("forwarded bodies carry no created_by", runner.routes.events.filter((event) => event.body.type === "message").every((event) => !("created_by" in event.body)));

// Race: alice and bob back to back without waiting.
const raceStart = lookups.length;
const [first, second] = await Promise.all([alice.postEvent(session.id, message("race alice")), bob.postEvent(session.id, message("race bob"))]);
await sleep(3000);
const race = await Promise.all(lookups.slice(raceStart));
ev.log("race", {
  posts: [first.status, second.status],
  forwarded: race.length,
  rows: race.map((result) => ({ createdBy: result.createdBy, turnActorAtForward: result.turnActor })),
  queuedBodies: [first.body, second.body],
});
ev.expect("race: created_by always right; turn_actor compared", race.every((result) => result.createdBy !== null));
runner.tunnel.closeGraceful();
await runner.tunnel.closed;
ev.finish();
