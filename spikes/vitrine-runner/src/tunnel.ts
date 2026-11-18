// Reusable runner-tunnel client: binding-token identity, handshake, frame loop.
// Mirrors omnigent/runner/identity.py and the server side in
// omnigent/server/routes/runner_tunnel.py.
import { createHash, randomBytes, randomUUID } from "node:crypto";
import WebSocket from "ws";
import {
  FRAME_PROTOCOL_MAJOR,
  RUNNER_IDLE_SHUTDOWN_CLOSE_REASON,
  decodeBody,
  decodeFrame,
  encodeFrame,
  type Frame,
  type Header,
  type RequestFrame,
} from "./frames.ts";

export const RUNNER_TUNNEL_TOKEN_HEADER = "X-Omnigent-Runner-Tunnel-Token";
export const OMNIGENT_INTERNAL_WS_ORIGIN = "omnigent://internal";

export function mintBindingToken(): string {
  return randomBytes(32).toString("base64url");
}

export function tokenBoundRunnerId(token: string): string {
  const stripped = token.trim();
  if (!stripped) throw new Error("tunnel binding token must not be empty");
  const digest = createHash("sha256").update(`omnigent-runner:${stripped}`).digest("hex");
  return `runner_token_${digest.slice(0, 32)}`;
}

export interface TunnelRequest {
  id: string;
  method: string;
  path: string;
  query: string;
  headers: Header[];
  body: Buffer;
  json(): unknown;
  signal: AbortSignal;
}

export interface TunnelResponder {
  head(status: number, headers?: Header[]): void;
  write(chunk: string): void;
  end(error?: string): void;
  json(status: number, value: unknown): void;
  readonly ended: boolean;
}

export type TunnelHandler = (request: TunnelRequest, respond: TunnelResponder) => void | Promise<void>;

export interface CloseInfo { code: number; reason: string; atMs: number }

export interface TunnelOptions {
  baseUrl: string;
  bearer?: string;
  bindingToken?: string;
  runnerId?: string;
  origin?: string | null;
  harnesses?: string[];
  capabilities?: string[];
  handler: TunnelHandler;
  onFrame?: (direction: "in" | "out", frame: Frame) => void;
  onClose?: (info: CloseInfo) => void;
}

export interface RunnerTunnel {
  runnerId: string;
  connectionId: string;
  opened: Promise<void>;
  closed: Promise<CloseInfo>;
  closeGraceful(): void;
  dropAbrupt(): void;
  setSilent(silent: boolean): void;
  pings: number[];
}

export function connectRunnerTunnel(options: TunnelOptions): RunnerTunnel {
  const runnerId = options.runnerId ?? tokenBoundRunnerId(options.bindingToken ?? "");
  const connectionId = randomUUID();
  const wsUrl = options.baseUrl.replace(/^http/, "ws") + `/v1/runners/${runnerId}/tunnel`;
  const headers: Record<string, string> = {};
  if (options.bearer) headers.Authorization = `Bearer ${options.bearer}`;
  if (options.bindingToken !== undefined) headers[RUNNER_TUNNEL_TOKEN_HEADER] = options.bindingToken;
  const origin = options.origin === undefined ? OMNIGENT_INTERNAL_WS_ORIGIN : options.origin;
  const socket = new WebSocket(wsUrl, origin === null ? { headers } : { headers, origin });
  let silent = false;
  const pings: number[] = [];
  const inflight = new Map<string, AbortController>();

  const send = (frame: Frame): void => {
    if (socket.readyState !== WebSocket.OPEN) return;
    options.onFrame?.("out", frame);
    socket.send(encodeFrame(frame));
  };

  const opened = new Promise<void>((resolve, reject) => {
    socket.once("open", () => {
      send({
        kind: "hello",
        runner_version: "vitrine-spike-0.0.0",
        frame_protocol_version: FRAME_PROTOCOL_MAJOR,
        harnesses: options.harnesses ?? ["acp", "acp:vitrine"],
        envs: [],
        telemetry_opt_out: true,
        ...(options.capabilities ? { capabilities: options.capabilities } : {}),
        connection_id: connectionId,
      });
      resolve();
    });
    socket.once("error", reject);
    socket.once("unexpected-response", (_req, res) => reject(new Error(`HTTP ${res.statusCode}`)));
  });

  const closed = new Promise<CloseInfo>((resolve) => {
    socket.on("close", (code, reason) => {
      for (const controller of inflight.values()) controller.abort();
      const info = { code, reason: reason.toString(), atMs: Date.now() };
      options.onClose?.(info);
      resolve(info);
    });
    socket.on("error", () => {});
  });

  const dispatch = (frame: RequestFrame): void => {
    const controller = new AbortController();
    inflight.set(frame.id, controller);
    const body = decodeBody(frame.body, frame.encoding);
    let headSent = false;
    let ended = false;
    const responder: TunnelResponder = {
      head(status, responseHeaders = []) {
        if (headSent) return;
        headSent = true;
        send({ kind: "response.head", id: frame.id, status, headers: responseHeaders });
      },
      write(chunk) {
        if (ended) return;
        if (!headSent) responder.head(200);
        send({ kind: "response.body", id: frame.id, body: chunk, encoding: "utf-8" });
      },
      end(error) {
        if (ended) return;
        if (!headSent) responder.head(error ? 500 : 200);
        ended = true;
        inflight.delete(frame.id);
        send(error ? { kind: "response.end", id: frame.id, error } : { kind: "response.end", id: frame.id });
      },
      json(status, value) {
        responder.head(status, [["content-type", "application/json"]]);
        responder.write(JSON.stringify(value));
        responder.end();
      },
      get ended() { return ended; },
    };
    const request: TunnelRequest = {
      id: frame.id,
      method: frame.method,
      path: frame.path,
      query: frame.query_string,
      headers: frame.headers,
      body,
      json: () => (body.length ? JSON.parse(body.toString("utf-8")) : null),
      signal: controller.signal,
    };
    Promise.resolve(options.handler(request, responder)).catch((error: unknown) => {
      responder.end(String(error));
    });
  };

  socket.on("message", (data) => {
    let frame: Frame;
    try {
      frame = decodeFrame(data.toString());
    } catch {
      return;
    }
    options.onFrame?.("in", frame);
    switch (frame.kind) {
      case "ping":
        pings.push(Date.now());
        if (!silent) send({ kind: "pong", ts: frame.ts });
        break;
      case "request":
        dispatch(frame);
        break;
      case "request.cancel":
        inflight.get(frame.id)?.abort();
        inflight.delete(frame.id);
        break;
      case "ws.open":
        send({ kind: "ws.close", ch_id: frame.ch_id, code: 1011, reason: "terminals not served" });
        break;
      default:
        break;
    }
  });

  return {
    runnerId,
    connectionId,
    opened,
    closed,
    pings,
    closeGraceful: () => socket.close(1000, RUNNER_IDLE_SHUTDOWN_CLOSE_REASON),
    dropAbrupt: () => socket.terminate(),
    setSilent: (value) => { silent = value; },
  };
}
