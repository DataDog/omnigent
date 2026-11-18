// Check 8 (B1): tunnel-token allow-list refusal for non-loopback peers; loopback fallback.
// Run against a server started with TUNNEL_ALLOWLIST=<unrelated token>. A non-loopback
// peer is simulated with X-Forwarded-For, which uvicorn trusts from 127.0.0.1 by default
// (the same path a reverse-proxied production peer takes).
import { loginAs, serverBaseUrl } from "../env.ts";
import { evidence, sleep } from "../evidence.ts";
import { connectRunnerTunnel, mintBindingToken } from "../tunnel.ts";

const ev = evidence(8);
const alice = await loginAs("alice");
const remotePeer = { "X-Forwarded-For": "203.0.113.7" };

async function attempt(label: string, extraHeaders: Record<string, string>, token = mintBindingToken()) {
  const tunnel = connectRunnerTunnel({ baseUrl: serverBaseUrl(), bearer: alice.bearer, bindingToken: token, extraHeaders, handler: () => {} });
  const opened = await tunnel.opened.then(() => "open", (error: Error) => `error:${error.message}`);
  const closed = await Promise.race([tunnel.closed, sleep(3000).then(() => null)]);
  const runners = (await alice.runners()).body.data ?? [];
  const listed = runners.some((runner: any) => runner.runner_id === tunnel.runnerId && runner.online);
  ev.log(label, { opened, close: closed ? { code: closed.code, reason: closed.reason } : "open after 3s", listedOnline: listed });
  return { tunnel, closed, listed };
}

const allowListed = Boolean(process.env.EXPECT_ALLOWLIST ?? "1");
const remote = await attempt("non-loopback peer (XFF), token not in allow-list", remotePeer);
if (allowListed) {
  ev.expect("non-loopback peer refused 4004 'runner tunnel token is not authorized'",
    remote.closed?.code === 4004 && remote.closed.reason === "runner tunnel token is not authorized");
}
const loopback = await attempt("loopback peer, token not in allow-list", {});
ev.expect("loopback fallback accepts token-bound id", loopback.closed === null && loopback.listed);
for (const entry of [remote, loopback]) if (entry.closed === null) entry.tunnel.closeGraceful();
ev.finish();
