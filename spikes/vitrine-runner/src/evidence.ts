// Append-only evidence log per check under .run/evidence/.
import { appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { RUN_DIR } from "./env.ts";

const EVIDENCE_DIR = join(RUN_DIR, "evidence");
mkdirSync(EVIDENCE_DIR, { recursive: true });

export function evidence(check: number) {
  const file = join(EVIDENCE_DIR, `check${check}.jsonl`);
  const results: { name: string; pass: boolean }[] = [];
  return {
    log(event: string, data: unknown = {}): void {
      appendFileSync(file, JSON.stringify({ at: new Date().toISOString(), event, data }) + "\n");
      console.log(`[check${check}] ${event}`, JSON.stringify(data));
    },
    expect(name: string, pass: boolean, data: unknown = {}): void {
      results.push({ name, pass });
      this.log(pass ? "PASS" : "FAIL", { name, ...(data as object) });
    },
    finish(): void {
      const failed = results.filter((result) => !result.pass);
      console.log(`\n[check${check}] ${failed.length ? "FAIL" : "PASS"} (${results.length - failed.length}/${results.length})`);
      for (const result of failed) console.log("  failed:", result.name);
      process.exitCode = failed.length ? 1 : 0;
    },
  };
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function waitFor<T>(probe: () => Promise<T | undefined> | T | undefined, timeoutMs: number, stepMs = 100): Promise<T | undefined> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await probe();
    if (value !== undefined && value !== false) return value;
    await sleep(stepMs);
  }
  return undefined;
}
