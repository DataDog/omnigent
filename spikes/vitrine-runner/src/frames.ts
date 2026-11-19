// TS port of omnigent/runner/transports/ws_tunnel/frames.py (wire shapes only).

export const FRAME_PROTOCOL_MAJOR = 1;
export const RUNNER_IDLE_SHUTDOWN_CLOSE_REASON = "runner idle shutdown";

export type Header = [string, string];

export interface HelloFrame {
  kind: "hello";
  runner_version: string;
  frame_protocol_version: number;
  harnesses: string[];
  envs: string[];
  telemetry_opt_out: boolean;
  capabilities?: string[];
  connection_id?: string;
}
export interface RequestFrame {
  kind: "request";
  id: string;
  method: string;
  path: string;
  query_string: string;
  headers: Header[];
  body: string | null;
  encoding: "utf-8" | "base64";
  stream: boolean;
}
export interface ResponseHeadFrame { kind: "response.head"; id: string; status: number; headers: Header[] }
export interface ResponseBodyFrame { kind: "response.body"; id: string; body: string; encoding: "utf-8" | "base64" }
export interface ResponseEndFrame { kind: "response.end"; id: string; error?: string }
export interface RequestCancelFrame { kind: "request.cancel"; id: string; reason: string }
export interface PingFrame { kind: "ping"; ts: number }
export interface PongFrame { kind: "pong"; ts: number; keep_warm?: boolean }
export interface WsOpenFrame { kind: "ws.open"; ch_id: string; path: string; query_string: string }
export interface WsDataFrame { kind: "ws.frame"; ch_id: string; data: string; encoding: string }
export interface WsCloseFrame { kind: "ws.close"; ch_id: string; code: number; reason: string }
export interface EventReadyFrame { kind: "event.ready" }
export interface EventBatchFrame { kind: "event.batch"; id: string; session_id: string; events: Record<string, unknown>[] }
export interface EventAckFrame { kind: "event.ack"; id: string; applied: number; error: string | null; retryable?: boolean }

export type Frame =
  | HelloFrame | RequestFrame | ResponseHeadFrame | ResponseBodyFrame | ResponseEndFrame
  | RequestCancelFrame | PingFrame | PongFrame | WsOpenFrame | WsDataFrame | WsCloseFrame
  | EventReadyFrame | EventBatchFrame | EventAckFrame;

const KNOWN_KINDS = new Set<string>([
  "hello", "request", "response.head", "response.body", "response.end", "request.cancel",
  "ping", "pong", "ws.open", "ws.frame", "ws.close", "event.ready", "event.batch", "event.ack",
]);

export function encodeFrame(frame: Frame): string {
  return JSON.stringify(frame);
}

export function decodeFrame(text: string): Frame {
  const parsed: unknown = JSON.parse(text);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("frame must be a JSON object");
  }
  const kind = (parsed as { kind?: unknown }).kind;
  if (typeof kind !== "string" || !KNOWN_KINDS.has(kind)) {
    throw new Error(`unknown frame kind: ${String(kind)}`);
  }
  const frame = parsed as Frame;
  if (frame.kind === "request") {
    frame.query_string ??= "";
    frame.headers ??= [];
    frame.encoding ??= "utf-8";
    frame.stream ??= false;
    if (typeof frame.id !== "string" || typeof frame.path !== "string") {
      throw new Error("request frame missing id/path");
    }
  }
  return frame;
}

export function decodeBody(body: string | null, encoding: string): Buffer {
  if (body === null) return Buffer.alloc(0);
  return encoding === "base64" ? Buffer.from(body, "base64") : Buffer.from(body, "utf-8");
}
