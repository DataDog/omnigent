// Check 4: owner + EDIT-grant user web sends reach the runner; fake turn round trip; dedupe.
import { loginAs } from "../env.ts";
import { evidence, sleep, waitFor } from "../evidence.ts";
import { assistantItemEvent, runFakeTurn } from "../fake-turn.ts";
import { createHostlessSession, startRunner } from "../runner.ts";

const ev = evidence(4);
const relayStatus = process.argv.includes("--relay-status");
const alice = await loginAs("alice");
const bob = await loginAs("bob");
let runnerApi: any;
let channel = "";
const turns: Promise<unknown>[] = [];
const runner = await startRunner(alice, {
  relayStatus,
  onMessage: (event) => {
    turns.push(runFakeTurn(runnerApi, event.sessionId, channel).then((result) => {
      ev.log("fake turn posted", { statuses: result.statuses, sourceId: result.sourceId });
      lastTurn = result;
    }));
  },
});
let lastTurn: any;
runnerApi = runner.runnerApi;
const { session, channelId } = await createHostlessSession(alice);
channel = channelId;
await alice.patchSession(session.id, { runner_id: runner.tunnel.runnerId });
const grant = await alice.grant(session.id, "bob", 2);
ev.log("grant bob EDIT", { status: grant.status, body: grant.status >= 300 ? grant.body : undefined });

const message = (text: string) => ({ type: "message", data: { role: "user", content: [{ type: "input_text", text }] } });
const itemsOf = async () => ((await alice.items(session.id)).body.data ?? []) as any[];

for (const [name, api] of [["alice", alice], ["bob", bob]] as const) {
  const before = runner.routes.events.length;
  const sent = await api.postEvent(session.id, message(`hello from ${name}`));
  ev.log(`${name} POST /events`, { status: sent.status, body: sent.body, ms: Math.round(sent.ms) });
  const forwarded = await waitFor(() => runner.routes.events.slice(before).find((event) => event.body.type === "message"), 5000);
  ev.expect(`${name}: message forwarded to runner`, Boolean(forwarded), {
    keys: forwarded ? Object.keys(forwarded.body) : [],
    persisted_item_id: forwarded?.body.persisted_item_id,
    created_by_in_body: forwarded ? "created_by" in forwarded.body : null,
  });
  await Promise.all(turns);
  await sleep(300);
  const snap = (await alice.getSession(session.id)).body;
  const items = await itemsOf();
  const userItem = items.find((item) => item.id === forwarded?.body.persisted_item_id);
  ev.log(`${name} after turn`, {
    status: snap.status,
    itemCount: items.length,
    userItem: userItem ? { id: userItem.id, type: userItem.type, created_by: userItem.created_by } : null,
    lastItem: items.at(-1) ? { type: items.at(-1).type, role: items.at(-1).role ?? items.at(-1).data?.role, id: items.at(-1).id } : null,
    turn_actor: snap.labels?.["omnigent.turn_actor"],
  });
  ev.expect(`${name}: user item created_by = sender`, userItem?.created_by === name);
  ev.expect(`${name}: session idle after fake turn`, snap.status === "idle");
  ev.expect(`${name}: assistant item persisted after user item`, items.at(-1)?.type === "message" && (items.at(-1).role ?? items.at(-1).data?.role) === "assistant");
  ev.expect(`${name}: all fake-turn posts 2xx`, lastTurn?.statuses.every((code: number) => code < 300), { statuses: lastTurn?.statuses });
}

// Follow-up dispatch settles immediately after HTTP idle.
const followStart = Date.now();
const followBefore = runner.routes.events.length;
const follow = await alice.postEvent(session.id, message("follow-up"));
const followForwarded = await waitFor(() => runner.routes.events.slice(followBefore).find((event) => event.body.type === "message"), 5000, 10);
ev.expect("follow-up dispatched right away", Boolean(followForwarded) && follow.status < 300, {
  status: follow.status, forwardMs: followForwarded ? followForwarded.receivedAtMs - followStart : null,
});
await Promise.all(turns);
await sleep(300);

// Dedupe: re-post the last assistant item with the same source_id.
const countBefore = (await itemsOf()).length;
const repost = await runner.runnerApi.postEvent(session.id, assistantItemEvent(lastTurn.sourceId, "spike reply"));
const countAfter = (await itemsOf()).length;
ev.expect("resend with same source_id dedupes", countAfter === countBefore && repost.status < 300, { repostStatus: repost.status, countBefore, countAfter });
ev.log("non-message events seen by runner", runner.routes.events.filter((event) => event.body.type !== "message").map((event) => event.body.type));
runner.tunnel.closeGraceful();
await runner.tunnel.closed;
ev.finish();
