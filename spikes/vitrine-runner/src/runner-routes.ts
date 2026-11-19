// Hostless runner route table served over the tunnel: init, heartbeat stream, events.
import { appendFileSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { RUN_DIR } from "./env.ts";
import type { TunnelHandler, TunnelRequest, TunnelResponder } from "./tunnel.ts";

export interface ForwardedEvent {
  sessionId: string;
  body: Record<string, any>;
  receivedAtMs: number;
  replayed: boolean;
}

export interface RunnerRoutesOptions {
  relayStatus?: boolean;
  heartbeatMs?: number;
  onMessage?: (event: ForwardedEvent) => void;
  ledgerFile?: string;
}

export interface RunnerRoutes {
  handler: TunnelHandler;
  frames: { method: string; path: string; query: string; bodyKeys: string[]; atMs: number }[];
  inits: { sessionId: string; body: Record<string, any> }[];
  events: ForwardedEvent[];
  streams: Map<string, TunnelResponder>;
  pushStatus(sessionId: string, status: string): void;
}

const FRAME_LOG = join(RUN_DIR, "frames.jsonl");

function loadLedger(file: string): Set<string> {
  return new Set(existsSync(file) ? (JSON.parse(readFileSync(file, "utf-8")) as string[]) : []);
}

export function createRunnerRoutes(options: RunnerRoutesOptions = {}): RunnerRoutes {
  const ledgerFile = options.ledgerFile ?? join(RUN_DIR, "ledger.json");
  const ledger = loadLedger(ledgerFile);
  const frames: RunnerRoutes["frames"] = [];
  const inits: RunnerRoutes["inits"] = [];
  const events: ForwardedEvent[] = [];
  const streams = new Map<string, TunnelResponder>();

  const serveStream = (sessionId: string, request: TunnelRequest, respond: TunnelResponder): void => {
    respond.head(200, [["content-type", "text/event-stream"], ["cache-control", "no-cache"]]);
    const beat = () => respond.write('data: {"type": "session.heartbeat"}\n\n');
    beat();
    streams.set(sessionId, respond);
    const timer = setInterval(beat, options.heartbeatMs ?? 15_000);
    request.signal.addEventListener("abort", () => {
      clearInterval(timer);
      if (streams.get(sessionId) === respond) streams.delete(sessionId);
    });
  };

  const handleEvent = (sessionId: string, body: Record<string, any>, respond: TunnelResponder): void => {
    const type = body?.type;
    if (type === "message") {
      const itemId = body.persisted_item_id as string | undefined;
      const replayed = itemId !== undefined && ledger.has(itemId);
      const event = { sessionId, body, receivedAtMs: Date.now(), replayed };
      events.push(event);
      if (replayed) {
        respond.json(202, { status: "accepted", deduped: true });
        return;
      }
      if (itemId) {
        ledger.add(itemId);
        writeFileSync(ledgerFile, JSON.stringify([...ledger]));
      }
      respond.json(202, { status: "accepted" });
      options.onMessage?.(event);
      return;
    }
    events.push({ sessionId, body, receivedAtMs: Date.now(), replayed: false });
    respond.json(202, { status: "accepted" });
  };

  const handler: TunnelHandler = (request, respond) => {
    const body = (() => { try { return request.json() as Record<string, any>; } catch { return null; } })();
    const record = {
      method: request.method,
      path: request.path,
      query: request.query,
      bodyKeys: body && typeof body === "object" ? Object.keys(body) : [],
      atMs: Date.now(),
    };
    frames.push(record);
    appendFileSync(FRAME_LOG, JSON.stringify(record) + "\n");

    if (request.method === "POST" && request.path === "/v1/sessions") {
      const sessionId = String(body?.session_id ?? "");
      inits.push({ sessionId, body: body ?? {} });
      respond.json(201, {
        id: sessionId, agent_id: body?.agent_id ?? null, status: "idle",
        created_at: Math.floor(Date.now() / 1000), title: null, labels: {}, runner_id: null,
        reasoning_effort: null, items: [], permission_level: null,
        session_init_protocol_version: body?.protocol_version ?? null, terminal_ready: false,
      });
      return;
    }
    const streamMatch = request.path.match(/^\/v1\/sessions\/([^/]+)\/stream$/);
    if (request.method === "GET" && streamMatch) {
      serveStream(streamMatch[1], request, respond);
      return;
    }
    const eventsMatch = request.path.match(/^\/v1\/sessions\/([^/]+)\/events$/);
    if (request.method === "POST" && eventsMatch) {
      handleEvent(eventsMatch[1], body ?? {}, respond);
      return;
    }
    respond.json(404, { error: "not_found", detail: `vitrine runner does not serve ${request.method} ${request.path}` });
  };

  return {
    handler, frames, inits, events, streams,
    pushStatus(sessionId, status) {
      if (!options.relayStatus) return;
      streams.get(sessionId)?.write(`data: ${JSON.stringify({ type: "session.status", status })}\n\n`);
    },
  };
}
