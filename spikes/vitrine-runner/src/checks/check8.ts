// Check 8 (B1): tunnel-token allow-list refusal for non-loopback peers; loopback fallback.
// Run against a server started with HOST=0.0.0.0 TUNNEL_ALLOWLIST=<unrelated token>,
// and pass SPIKE_LAN_URL=http://<lan-ip>:18766.
import { loginAs, serverBaseUrl } from "../env.ts";
import { evidence, sleep } from "../evidence.ts";
import { Api } from "../api.ts";
import { connectRunnerTunnel, mintBindingToken } from "../tunnel.ts";

const ev = evidence(8);
const lanUrl = process.env.SPIKE_LAN_URL;
if (!lanUrl) throw new Error("set SPIKE_LAN_URL=http://<lan-ip>:18766");
const alice = await loginAs("alice");

async function attempt(label: string, baseUrl: string) {
  const token = mintBindingToken();
  const tunnel = connectRunnerTunnel({ baseUrl, bearer: alice.bearer, bindingToken: token, handler: () => {} });
  const opened = await tunnel.opened.then(() => "open", (error: Error) => `error:${error.message}`);
  const closed = await Promise.race([tunnel.closed, sleep(3000).then(() => null)]);
  const runners = (await alice.runners()).body.data ?? [];
  const listed = runners.some((runner: any) => runner.runner_id === tunnel.runnerId && runner.online);
  ev.log(label, { opened, close: closed ? { code: closed.code, reason: closed.reason } : "open after 3s", listedOnline: listed });
  return { tunnel, token, closed, listed };
}

const lan = await attempt("LAN peer, token not in allow-list", lanUrl);
ev.expect("LAN peer refused 4004 'runner tunnel token is not authorized'",
  lan.closed?.code === 4004 && lan.closed.reason === "runner tunnel token is not authorized");
const loopback = await attempt("loopback peer, token not in allow-list", serverBaseUrl());
ev.expect("loopback fallback accepts token-bound id", loopback.closed === null && loopback.listed);

// HTTP ingest authority while the allow-list is set, using the loopback runner's token.
const lanApi = new Api(lanUrl, alice.bearer, loopback.token);
const ingest = await lanApi.call("GET", "/v1/runners");
ev.log("HTTP over LAN with unlisted tunnel token header", { status: ingest.status });
loopback.tunnel.closeGraceful();
await loopback.tunnel.closed;
ev.finish();
