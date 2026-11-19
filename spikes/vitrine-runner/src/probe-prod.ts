// S3 prod probe: B1 (runner tunnel accepted live) and B8 (default __public__ grant
// on a hostless session). Throwaway, run once on an approved account. Never logs secrets.
import { appendFileSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { Api, type ApiResult } from "./api.ts";
import { RUN_DIR } from "./env.ts";
import { sleep } from "./evidence.ts";
import { connectRunnerTunnel, mintBindingToken, type RunnerTunnel } from "./tunnel.ts";

export const PROD_BASE_URL = "https://omnigent.us1.ddbuild.io";
const SPIKE_LABEL = { "vitrine.mirror.spike": "s3" };
const SPIKE_TITLE = "[spike S3] Vitrine hostless probe — safe to delete";

export function makeRedactor(secrets: string[]): (text: string) => string {
  const live = secrets.filter((secret) => secret && secret.length >= 8);
  return (text) => {
    let out = text;
    for (const secret of live) out = out.split(secret).join("<redacted>");
    return out.replace(/eyJ[A-Za-z0-9_-]{8,}(\.[A-Za-z0-9_-]+)*/g, "<redacted-jwt>");
  };
}

function loadBearer(baseUrl: string): string {
  if (process.env.S3_BEARER) return process.env.S3_BEARER.trim();
  if (process.env.S3_BEARER_FROM_OMNIGENT_CLI === "1") {
    const tokens = JSON.parse(readFileSync(join(homedir(), ".omnigent", "auth_tokens.json"), "utf-8"));
    const token = tokens[baseUrl]?.token;
    if (typeof token === "string" && token) return token;
    throw new Error(`no omnigent CLI token stored for ${baseUrl}`);
  }
  throw new Error("set S3_BEARER or S3_BEARER_FROM_OMNIGENT_CLI=1");
}

async function main(): Promise<void> {
  if (process.env.S3_PROD_APPROVED !== "1") {
    console.error("refusing: S3_PROD_APPROVED=1 is required (human approval gate)");
    process.exit(2);
  }
  const baseUrl = process.env.S3_BASE_URL ?? PROD_BASE_URL;
  const bearer = loadBearer(baseUrl);
  const bindingToken = mintBindingToken();
  const redact = makeRedactor([bearer, bindingToken]);
  const evidenceFile = join(RUN_DIR, `s3-evidence-${Date.now()}.jsonl`);
  const log = (step: string, data: unknown = {}): void => {
    const line = redact(JSON.stringify({ at: new Date().toISOString(), step, data }));
    appendFileSync(evidenceFile, line + "\n");
    console.log(line);
  };
  const brief = (result: ApiResult) => ({ status: result.status, ms: Math.round(result.ms), body: result.body });
  const api = new Api(baseUrl, bearer);
  let tunnel: RunnerTunnel | undefined;
  let sessionId: string | undefined;
  log("start", { baseUrl, evidenceFile });
  try {
    // 1. B1: open runner tunnel.
    const inbound: unknown[] = [];
    tunnel = connectRunnerTunnel({
      baseUrl,
      bearer,
      bindingToken,
      handler: (_request, respond) => respond.json(404, { error: "s3 probe serves no routes" }),
      onFrame: (direction, frame) => {
        if (frame.kind === "ping" || frame.kind === "pong") return;
        log(`frame.${direction}`, frame);
        if (direction === "in") inbound.push(frame);
      },
      onClose: (info) => log("tunnel.close", info),
    });
    log("B1.connect", { runnerId: tunnel.runnerId, connectionId: tunnel.connectionId });
    const opened = await tunnel.opened.then(() => "open", (error: Error) => `error:${error.message}`);
    log("B1.handshake", { opened });
    await sleep(3000);
    const status = await api.call("GET", `/v1/runners/${tunnel.runnerId}/status`);
    log("B1.status", brief(status));
    log("B1.result", { accepted: opened === "open" && status.body?.online === true, inboundFrames: inbound.length });

    // 2. B8: one hostless session, then read its grants.
    const agents = await api.call("GET", "/v1/agents");
    const agentId = process.env.S3_AGENT_ID ?? agents.body?.data?.[0]?.id;
    log("B8.agents", { status: agents.status, count: agents.body?.data?.length, agentId });
    if (!agentId) throw new Error("no agent id available");
    const created = await api.createSession({ agent_id: agentId, harness_override: "acp:vitrine", title: SPIKE_TITLE, labels: SPIKE_LABEL });
    sessionId = created.status < 300 ? created.body?.id : undefined;
    log("B8.create", { status: created.status, id: sessionId, host_id: created.body?.host_id,
      harness_override: created.body?.harness_override, labels: created.body?.labels, error: created.status >= 300 ? created.body : undefined });
    if (sessionId) {
      const grants = await api.call("GET", `/v1/sessions/${sessionId}/permissions`);
      log("B8.grants", brief(grants));
      const publicGrant = JSON.stringify(grants.body).includes("__public__");
      log("B8.result", { publicGrantPresent: publicGrant });
    }
  } catch (error) {
    log("error", { message: String(error) });
    process.exitCode = 1;
  } finally {
    // 3. Cleanup, always.
    if (sessionId) {
      try { log("cleanup.delete", brief(await api.call("DELETE", `/v1/sessions/${sessionId}`))); }
      catch (error) { log("cleanup.delete.error", { message: String(error) }); }
    }
    if (tunnel) {
      tunnel.closeGraceful();
      const closed = await Promise.race([tunnel.closed, sleep(5000).then(() => null)]);
      if (!closed) tunnel.dropAbrupt();
      log("cleanup.tunnel", { closed: closed ?? "forced-terminate" });
    }
    if (sessionId) {
      try { log("readback.session", brief(await api.call("GET", `/v1/sessions/${sessionId}`))); }
      catch (error) { log("readback.session.error", { message: String(error) }); }
    }
    if (tunnel) {
      await sleep(2000);
      try { log("readback.runner", brief(await api.call("GET", `/v1/runners/${tunnel.runnerId}/status`))); }
      catch (error) { log("readback.runner.error", { message: String(error) }); }
    }
    log("done");
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await main();
