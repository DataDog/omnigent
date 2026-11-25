// Minimal Omnigent HTTP client for the spike.
import { RUNNER_TUNNEL_TOKEN_HEADER } from "./tunnel.ts";

export interface ApiResult<T = any> { status: number; body: T; ms: number }

export class Api {
  baseUrl: string;
  bearer?: string;
  bindingToken?: string;

  constructor(baseUrl: string, bearer?: string, bindingToken?: string) {
    this.baseUrl = baseUrl;
    this.bearer = bearer;
    this.bindingToken = bindingToken;
  }

  async call<T = any>(method: string, path: string, body?: unknown): Promise<ApiResult<T>> {
    const headers: Record<string, string> = { accept: "application/json" };
    if (this.bearer) headers.authorization = `Bearer ${this.bearer}`;
    if (this.bindingToken) headers[RUNNER_TUNNEL_TOKEN_HEADER] = this.bindingToken;
    if (body !== undefined) headers["content-type"] = "application/json";
    const started = performance.now();
    const response = await fetch(this.baseUrl + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    let parsed: any = text;
    try { parsed = text ? JSON.parse(text) : null; } catch { /* keep text */ }
    return { status: response.status, body: parsed as T, ms: performance.now() - started };
  }

  login(username: string, password: string) { return this.call("POST", "/auth/login", { username, password }); }
  invite() { return this.call("POST", "/auth/invite", { is_admin: false }); }
  register(invite: string, username: string, password: string) {
    return this.call("POST", "/auth/register", { invite, username, password });
  }
  agents() { return this.call("GET", "/v1/agents"); }
  runners() { return this.call("GET", "/v1/runners"); }
  createSession(body: Record<string, unknown>) { return this.call("POST", "/v1/sessions", body); }
  patchSession(id: string, body: Record<string, unknown>) { return this.call("PATCH", `/v1/sessions/${id}`, body); }
  getSession(id: string) { return this.call("GET", `/v1/sessions/${id}`); }
  items(id: string) { return this.call("GET", `/v1/sessions/${id}/items?limit=200`); }
  postEvent(id: string, body: Record<string, unknown>) { return this.call("POST", `/v1/sessions/${id}/events`, body); }
  grant(id: string, userId: string, level: number) {
    return this.call("PUT", `/v1/sessions/${id}/permissions`, { user_id: userId, level });
  }
}
