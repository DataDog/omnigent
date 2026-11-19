// Check 1: tunnel handshake with bearer + self-minted binding token.
import { loginAs, serverBaseUrl } from "../env.ts";
import { evidence, sleep, waitFor } from "../evidence.ts";
import { connectRunnerTunnel, mintBindingToken, tokenBoundRunnerId } from "../tunnel.ts";

const ev = evidence(1);
const baseUrl = serverBaseUrl();
const alice = await loginAs("alice");
const bob = await loginAs("bob");
const handler = () => {};

async function attempt(label: string, options: Partial<Parameters<typeof connectRunnerTunnel>[0]>) {
  const tunnel = connectRunnerTunnel({ baseUrl, handler, ...options } as Parameters<typeof connectRunnerTunnel>[0]);
  const opened = await tunnel.opened.then(() => "open", (error: Error) => `error:${error.message}`);
  const closed = await Promise.race([tunnel.closed, sleep(3000).then(() => null)]);
  if (!closed) tunnel.dropAbrupt();
  ev.log(label, { opened, close: closed ? { code: closed.code, reason: closed.reason } : "still-open-after-3s" });
  return { opened, closed };
}

// Happy path.
const token = mintBindingToken();
const tunnel = connectRunnerTunnel({ baseUrl, bearer: alice.bearer, bindingToken: token, handler });
await tunnel.opened;
ev.expect("runner_id == tokenBoundRunnerId(token)", tunnel.runnerId === tokenBoundRunnerId(token), { runnerId: tunnel.runnerId });
const listed = await waitFor(async () => {
  const runners = await alice.runners();
  return runners.body.data?.find((runner: any) => runner.runner_id === tunnel.runnerId && runner.online);
}, 5000);
ev.expect("alice GET /v1/runners lists runner online", Boolean(listed), { listed });
const bobRunners = await bob.runners();
ev.expect("bob GET /v1/runners does not list it",
  !JSON.stringify(bobRunners.body).includes(tunnel.runnerId), { bobCount: bobRunners.body.data?.length });
tunnel.closeGraceful();
await tunnel.closed;

// Negatives.
const wrongPath = await attempt("wrong runner_id in path", { bearer: alice.bearer, bindingToken: mintBindingToken(), runnerId: tokenBoundRunnerId("other") });
ev.expect("wrong path id refused with 4004 mismatch", wrongPath.closed?.code === 4004 && wrongPath.closed.reason === "runner_id does not match tunnel token");
const emptyToken = await attempt("empty binding token (loopback)", { bearer: alice.bearer, bindingToken: "  ", runnerId: "runner_token_empty" });
ev.expect("empty token refused", emptyToken.opened !== "open" || (emptyToken.closed?.code ?? 0) >= 1002);
await attempt("missing Origin header", { bearer: alice.bearer, bindingToken: mintBindingToken(), origin: null });
await attempt("browser-like Origin header", { bearer: alice.bearer, bindingToken: mintBindingToken(), origin: "https://evil.example" });
const noBearer = connectRunnerTunnel({ baseUrl, bindingToken: mintBindingToken(), handler });
const noBearerOpened = await noBearer.opened.then(() => "open", (error: Error) => error.message);
await sleep(1000);
const aliceSeesNoBearer = JSON.stringify((await alice.runners()).body).includes(noBearer.runnerId);
ev.log("no bearer over loopback", { opened: noBearerOpened, aliceSeesIt: aliceSeesNoBearer });
noBearer.dropAbrupt();
ev.finish();
