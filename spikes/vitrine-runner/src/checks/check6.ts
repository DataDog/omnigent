// Check 6: how the server surfaces runner 404s on proxied route families.
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { RUN_DIR, loginAs } from "../env.ts";
import { evidence, sleep } from "../evidence.ts";
import { createHostlessSession, startRunner } from "../runner.ts";

const ev = evidence(6);
const alice = await loginAs("alice");
const runner = await startRunner(alice);
const { session } = await createHostlessSession(alice);
await alice.patchSession(session.id, { runner_id: runner.tunnel.runnerId });
await sleep(500);
const id = session.id;
const logDir = join(RUN_DIR, "data", "logs", "server");
const logLines = () => readdirSync(logDir).flatMap((file) => readFileSync(join(logDir, file), "utf-8").split("\n"));

const probes: [string, string, string, unknown?][] = [
  ["resources", "GET", `/v1/sessions/${id}/resources`],
  ["resource by id", "GET", `/v1/sessions/${id}/resources/res_spike`],
  ["terminals list", "GET", `/v1/sessions/${id}/resources/terminals`],
  ["terminal create", "POST", `/v1/sessions/${id}/resources/terminals`, {}],
  ["environments", "GET", `/v1/sessions/${id}/resources/environments`],
  ["github", "GET", `/v1/sessions/${id}/resources/github`],
  ["github prs", "GET", `/v1/sessions/${id}/resources/github/prs`],
  ["repository-branches", "GET", `/v1/sessions/${id}/resources/repository-branches`],
  ["mcp proxy tools/list -> runner /mcp/execute", "POST", `/v1/sessions/${id}/mcp`, { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }],
  ["mcp proxy tools/call -> runner /mcp/execute", "POST", `/v1/sessions/${id}/mcp`, { jsonrpc: "2.0", id: 2, method: "tools/call", params: { name: "spike__noop", arguments: {} } }],
  ["skills/resolve", "POST", `/v1/sessions/${id}/skills/resolve`, { name: "spike" }],
  ["models", "GET", `/v1/sessions/${id}/models`],
  ["model-options", "GET", `/v1/sessions/${id}/model-options`],
  ["sign-in-link", "GET", `/v1/sessions/${id}/sign-in-link`],
  ["session GET", "GET", `/v1/sessions/${id}`],
];

const rows = [];
for (const [family, method, path, body] of probes) {
  const framesBefore = runner.routes.frames.length;
  const linesBefore = logLines().length;
  const result = await alice.call(method, path, body);
  await sleep(300);
  const runnerFrames = runner.routes.frames.slice(framesBefore).map((frame) => `${frame.method} ${frame.path}`);
  const newErrors = logLines().slice(linesBefore).filter((line) => /ERROR|WARN/.test(line)).map((line) => line.replace(/^.*\| /, "").slice(0, 200));
  const row = {
    family, method, path: path.replace(id, "{id}"),
    status: result.status,
    body: JSON.stringify(result.body).slice(0, 160),
    runnerFrames: runnerFrames.map((frame) => frame.replace(id, "{id}")),
    serverLog: newErrors.slice(0, 3),
  };
  rows.push(row);
  ev.log("probe", row);
}
await sleep(2000);
ev.log("server WARN/ERROR lines for this session", logLines()
  .filter((line) => line.includes(id) && /^(WARN|ERROR)/.test(line))
  .map((line) => line.replace(/^(\S+).*\| /, "$1 ").replace(id, "{id}").slice(0, 220)));
ev.expect("server stays healthy after all probes", (await alice.getSession(id)).status === 200);
ev.log("summary", rows.map((row) => `${row.family}: ${row.status} runner=${row.runnerFrames.length} log=${row.serverLog.length}`));
runner.tunnel.closeGraceful();
await runner.tunnel.closed;
ev.finish();
