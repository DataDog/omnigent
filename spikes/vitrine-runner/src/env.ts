// Shared spike environment: server address and test-user credentials under .run/.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Api } from "./api.ts";

export const SPIKE_DIR = join(dirname(fileURLToPath(import.meta.url)), "..");
export const RUN_DIR = join(SPIKE_DIR, ".run");
mkdirSync(RUN_DIR, { recursive: true });

export function serverBaseUrl(): string {
  if (process.env.SPIKE_BASE_URL) return process.env.SPIKE_BASE_URL;
  const addrFile = join(RUN_DIR, "server_addr");
  const addr = existsSync(addrFile) ? readFileSync(addrFile, "utf-8").trim() : "127.0.0.1:18766";
  return `http://${addr}`;
}

interface Users { alice: { password: string }; bob: { password: string } }

export function usersFile(): string { return join(RUN_DIR, "users.json"); }

export function readUsers(): Users {
  return JSON.parse(readFileSync(usersFile(), "utf-8")) as Users;
}

export function writeUsers(users: Users): void {
  writeFileSync(usersFile(), JSON.stringify(users, null, 2));
}

export async function loginAs(name: "alice" | "bob", baseUrl = serverBaseUrl()): Promise<Api> {
  const api = new Api(baseUrl);
  const password = readUsers()[name].password;
  const result = await api.login(name, password);
  if (result.status !== 200 || !result.body?.token) {
    throw new Error(`login ${name} failed: ${result.status} ${JSON.stringify(result.body)}`);
  }
  api.bearer = result.body.token;
  return api;
}
