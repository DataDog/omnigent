# ACP subagent sessions

Omnigent can mirror an ACP agent's delegated sessions into persistent child
sessions. The integration uses the ACP v1 subagents preview, negotiated with
`clientCapabilities.subagents: {}`. It does not require a vendor extension or
infer delegation from tool titles.

## Configure an agent

Configure the normal ACP harness on the execution host:

```yaml
acp:
  agents:
    - name: Background agent
      command: /absolute/path/to/agent --acp
```

Use the producer's normal authentication. If it needs additional environment
variables, configure `env_passthrough` with their names and keep their values
in the execution host environment. The command must exist on the host running
the runner.

Start an interactive session against your server:

```sh
omnigent run --harness acp:background-agent --server http://localhost:8000
```

Ask the agent to delegate two distinct tasks. A compatible producer announces
each child before sending its activity. Children have separate transcripts and
identities, even when their titles match. A later assignment can reuse the same
child and extend its history.

## Background activity and compatibility

Upgrade the server and owning runner together to enable child reporting.
Older combinations keep the parent-only session view when they cannot
negotiate the reporting support.
The server must have its configured artifact store available as well as the
conversation database. ACP snapshots use private, immutable artifacts; replicas
reading the same conversations must use the same persistent artifact backend.
Missing or corrupt snapshots fail explicitly rather than discarding history.
Set `OMNIGENT_ACP_SESSION_REPORTING_REQUIRED=true` in the runner's environment
to require reporting; an incompatible combination then fails with a
compatibility error before the agent initializes.

The producer must support the preview's `subagent_update` associations and
child-addressed notifications. Activity after the parent prompt finishes also
requires the producer to keep observing its children between prompts. Earlier
Vit builds observe children only during an active prompt; use a build with
session-owned child observation for background completion.

Keep the owning runner alive to receive background activity. An interactive
session or a running daemon can own that runner. A one-shot `run -p` command
stops any runner it owns when it exits and does not detach native work.

A child's `idle` state describes its current work; it does not permanently close
the child session. An unknown or missing state is unconfirmed. A lost connection
does not prove success or failure. Association descriptions are metadata and
are not treated as task messages. Directed task messages appear only when the
producer actually sends them.

Mirrored children are read-only in this release. Independent prompting,
resuming, and cancellation are unavailable even when the producer advertises
child controls. Parent controls and replies to child permission requests remain
available. Omnigent does not advertise ACP terminal delegation; terminal
requests receive an unsupported-method response.

## Inspect persisted children

Use the same authentication as for the parent:

- List children with `GET /v1/sessions/{parent_id}/child_sessions`.
- Inspect a child with `GET /v1/sessions/{child_id}`.
- Read its transcript with `GET /v1/sessions/{child_id}/items`.

Export a child using its Omnigent session ID:

```sh
omnigent session export --id <child-session-id> \
  --server http://localhost:8000 --output child.jsonl
```

Children inherit the parent's access controls. A server restart preserves
persisted identities and history. Runner recovery replays durable reports;
it does not resume a killed producer. A new producer process has a new native
execution epoch, even if it repeats a previous native child ID.

Private ACP artifacts have no downloadable file attachments or public artifact
URLs. Failed transactions can leave unreferenced immutable artifacts; this
release retains them under the configured backend's retention and adds no ACP
garbage collector.

Child requests retain their trusted execution context, including the initiating
actor, workspace, and policies. Later parent prompts cannot silently grant old
children a different caller's authority. Current access revocation still applies
when an action executes.
