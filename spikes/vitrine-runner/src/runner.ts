// Spike runner process: tunnel + route table + helpers shared by checks.
import { randomUUID } from "node:crypto";
import { Api } from "./api.ts";
import { createRunnerRoutes, type RunnerRoutesOptions } from "./runner-routes.ts";
import { connectRunnerTunnel, mintBindingToken, type CloseInfo } from "./tunnel.ts";

export async function startRunner(owner: Api, options: RunnerRoutesOptions & { bindingToken?: string; onClose?: (info: CloseInfo) => void } = {}) {
  const bindingToken = options.bindingToken ?? mintBindingToken();
  const routes = createRunnerRoutes(options);
  const tunnel = connectRunnerTunnel({
    baseUrl: owner.baseUrl,
    bearer: owner.bearer,
    bindingToken,
    handler: routes.handler,
    onClose: options.onClose,
  });
  await tunnel.opened;
  const runnerApi = new Api(owner.baseUrl, owner.bearer, bindingToken);
  return { bindingToken, routes, tunnel, runnerApi };
}

let cachedAgentId: string | undefined;

export async function createHostlessSession(owner: Api, channelId = randomUUID()) {
  const agents = (await owner.agents()).body.data as { id: string; name: string }[];
  const candidates = cachedAgentId ? [cachedAgentId] : agents.map((agent) => agent.id);
  let lastError: unknown;
  for (const agentId of candidates) {
    const created = await owner.createSession({
      agent_id: agentId,
      harness_override: "acp:vitrine",
      labels: {
        "vitrine.mirror.install": "spike-install",
        "vitrine.channel_id": channelId,
        "vitrine.create_nonce": randomUUID(),
      },
    });
    if (created.status < 300) {
      cachedAgentId = agentId;
      return { session: created.body, channelId, agentId, status: created.status };
    }
    lastError = { agentId, status: created.status, body: created.body };
  }
  throw new Error(`no agent accepted harness_override acp:vitrine: ${JSON.stringify(lastError)}`);
}
