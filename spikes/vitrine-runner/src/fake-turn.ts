// Fake assistant turn posted back via external_* ingest events.
import { randomUUID } from "node:crypto";
import type { Api } from "./api.ts";

export interface FakeTurnResult { sourceId: string; statuses: number[]; batch: Record<string, unknown>[] }

export function assistantItemEvent(sourceId: string, text: string): Record<string, unknown> {
  return {
    type: "external_conversation_item",
    data: {
      item_type: "message",
      item_data: { role: "assistant", agent: "vitrine", content: [{ type: "output_text", text }] },
      source_id: sourceId,
    },
  };
}

export async function runFakeTurn(runnerApi: Api, sessionId: string, channelId: string, text = "spike reply"): Promise<FakeTurnResult> {
  const sourceId = `vitrine:${channelId}:${randomUUID()}`;
  const batch: Record<string, unknown>[] = [
    { type: "external_session_status", data: { status: "running" } },
    { type: "external_output_text_delta", data: { delta: text } },
    assistantItemEvent(sourceId, text),
    { type: "external_session_usage", data: { context_tokens: 1200, context_window: 200000 } },
    { type: "external_session_status", data: { status: "idle" } },
  ];
  const statuses: number[] = [];
  for (const event of batch) {
    const result = await runnerApi.postEvent(sessionId, event);
    statuses.push(result.status);
    if (result.status >= 300) console.log("fake-turn post failed", event.type, result.status, JSON.stringify(result.body));
  }
  return { sourceId, statuses, batch };
}
