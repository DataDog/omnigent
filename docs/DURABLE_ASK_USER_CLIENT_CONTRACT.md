# Durable `sys_ask_user` — client integration contract

How an Omnigent client renders, answers, and reconnects a durable human
question asked by a hosted agent via the `sys_ask_user` tool. This is the
integrator contract shared with the VIT-1695 integration; it is generic to
any Omnigent client (web SPA, native terminal, third-party app).

Everything below is ordinary elicitation flow: a durable question is just an
elicitation with `params.durable = true`. A client that already renders
approval cards and elicitations needs only the `requestedSchema` shapes and
the `durable` / `invocation_id` / `target_session_id` fields described here.

## Where pending questions appear

- **Session snapshot** — `GET /v1/sessions/{id}` returns
  `pending_elicitations: [event]`. Each event is a
  `response.elicitation_request` shaped like the live SSE event, so one
  renderer serves both. Pending questions replay from the snapshot on every
  cold load; durable questions also replay after a server restart.
- **Live stream** — the same event is published on the session's SSE stream
  (`response.elicitation_request`) when the question is asked, and a
  `response.elicitation_resolved` event carries the `elicitation_id` (plus
  the answering `action`) when it is resolved.
- **Child sessions** — a question asked by a child session surfaces in
  ancestor views with `params.target_session_id` set to the owning child
  session id. Answering from any view that sees the card resolves the
  child's parked call; the `elicitation_id` is the correlation key
  everywhere.

### Event shape

```jsonc
{
  "type": "response.elicitation_request",
  "elicitation_id": "2591b6659a8a4818bce4f2580f284e5a",   // stable correlation id
  "params": {
    "mode": "form",
    "message": "Which environment should I deploy to?",
    "requestedSchema": { /* see shapes below */ },
    "durable": true,                       // marks a durable sys_ask_user question
    "invocation_id": "call_ask_user_pass", // the model tool-call id that parked it
    "target_session_id": "conv_child_…"    // only on ancestor views of child questions
  }
}
```

`durable: true` tells the client the question survives disconnects and
server restarts — never render it as transient (no auto-dismiss, no
"connection lost = gone"). `invocation_id` is informational for clients
(the model call identity; replays of the same call map to the same
question).

## `requestedSchema` shapes

All three shapes share one primitive: an `answer` property under
`properties.answer`, listed in `required`.

**Free-form question** — `options` omitted:

```jsonc
{
  "type": "object",
  "properties": { "answer": { "type": "string", "minLength": 1 } },
  "required": ["answer"]
}
```

**Single choice** — rendered as option buttons or a select:

```jsonc
{
  "type": "object",
  "properties": {
    "answer": { "type": "string", "enum": ["staging", "production"] }
  },
  "required": ["answer"]
}
```

**Multiple choice** — `multiple: true` in the tool arguments; rendered as a
checkbox group, at least one selection:

```jsonc
{
  "type": "object",
  "properties": {
    "answer": {
      "type": "array",
      "items": { "type": "string", "enum": ["tests", "docs", "chore"] },
      "minItems": 1,
      "uniqueItems": true
    }
  },
  "required": ["answer"]
}
```

## Answering

Answer through the existing elicitation resolve endpoint — the same one a
client already uses for approvals and MCP elicitations:

```
POST /v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve
{ "action": "accept" | "decline" | "cancel", "content": { "answer": … } }
```

- `action` is MCP `ElicitationResult` semantics: `accept` = answered,
  `decline` = explicit refusal, `cancel` = dismissed. Resolves return 202.
- `content.answer` must match the schema above: a non-empty string for
  free-form, one of the enum values for single choice, an array of distinct
  enum values (non-empty) for multiple choice. A malformed answer is
  rejected and the question stays pending — safe to re-prompt.
- The equivalent in-band path — a `type: "approval"` event on
  `POST /v1/sessions/{id}/events` with the same
  `{elicitation_id, action, content}` payload — resolves the identical
  question, so both delivery styles work.

Resolution semantics (server-enforced, clients can rely on them):

- **First valid answer wins.** A second, different answer for the same
  `elicitation_id` is rejected and changes nothing.
- **Identical retries are idempotent** — an answer re-submitted after a
  network failure (same action + content) is acknowledged, not an error.
- **Decline/cancel are terminal** and carry no content.

## Reconnect is just re-reading the snapshot

Pending state lives server-side, so reconnecting never requires a
re-render protocol beyond what a cold load already does:

1. Client disconnects (or the whole client quits) while the question is
   pending — the server keeps the question, the parked agent turn, and the
   execution watchdogs suspended.
2. Client reconnects and loads `GET /v1/sessions/{id}`; the snapshot's
   `pending_elicitations` replays the identical card (same `elicitation_id`,
   same schema) because the question is durable.
3. The answer submitted at any time — before, during, or after the
   disconnect — resumes the agent's execution exactly once. Late answers are
   not lost: they are committed server-side and delivered when the runner
   is reachable.
4. A resolved question's state is also readable:
   `GET /v1/sessions/{id}/elicitations/{elicitation_id}` returns
   `{"status": "pending", …}` with the schema while pending, or
   `{"status": "resolved", "action": …, "content": …}` once answered —
   useful for a standalone answer page or a stale-card race.

One exception clients should not paper over: an explicit session interrupt
cancels a pending durable question (state `canceled`); a later answer to it
is rejected. Server or runner restarts do NOT cancel — those are exactly
the disconnects the durable surface exists for.
