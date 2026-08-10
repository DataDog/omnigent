# Server feature-usage metrics

## Purpose

This is a small, server-side usage signal for answering which Omnigent
features are actually used and by whom.  It uses the existing OpenTelemetry
meter provider; the normal OTLP configuration therefore sends these metrics
through the same collector pipeline already used to ingest metrics into
Datadog.  It does not add a second telemetry client, dashboard, or monitor.

Metrics are no-op unless the runtime's existing metrics provider is enabled.
In the standard configuration, set `OMNIGENT_TELEMETRY_ENABLED=1` and an
`OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_METRICS_EXPORTER=otlp`).  Standard
OTLP exporter and resource settings, including `OTEL_SERVICE_NAME`, continue
to control delivery and service attribution.

Existing anonymous OSS telemetry is unchanged.  This is a separate,
authenticated server metric contract.

## Metric contract

Every completed operation produces both measurements with the same attributes:

| Metric | Instrument | Unit | Meaning |
| --- | --- | --- | --- |
| `omnigent.feature.usage` | Counter | `{operation}` | One completed feature operation. |
| `omnigent.feature.operation.duration` | Histogram | `s` | Elapsed operation duration. |

All measurements contain:

| Attribute | Values / meaning |
| --- | --- |
| `omnigent.feature.name` | A bounded feature name from the matrix below. |
| `omnigent.feature.operation` | A bounded operation name from the matrix below. |
| `omnigent.feature.outcome` | `success`, `failed`, `cancelled`, `abandoned`, `rejected`, or `no_change`. |
| `omnigent.actor.user_id` | The canonical user identifier returned by the configured `AuthProvider`. In single-user/auth-disabled mode it is `local`. |

`omnigent.session.owner_id` is included only when a session owner is known and
different from the actor.  This preserves the distinction between a
collaborator who initiated or resolved an action and the owner, without
duplicating the common owner-as-actor case.  The identifier may be an email or
another canonical auth credential identifier; it is intentionally high
cardinality and authorized for this internal fork.

Failures add `omnigent.failure.reason`, one of `validation`, `permission`,
`capacity`, `backend`, or `unknown`.  An explicitly handled route error is
classified before it is re-raised; unhandled operation failures are `backend`.

Only the following feature-specific attributes can be added:

| Attribute | Bounded values |
| --- | --- |
| `omnigent.dictation.engine` | `remote`, `whisper`, `sherpa`, or `other` |
| `omnigent.attachment.category` | `image`, `pdf`, `text`, or `unknown` |
| `omnigent.attachment.size_bucket` | `lt_1mib`, `1mib_to_10mib`, `gte_10mib`, or `unknown` |
| `omnigent.sharing.target_type` | `public` or `user` |
| `omnigent.sharing.access_level` | `read`, `edit`, `manage`, `owner`, or `unknown` |
| `omnigent.fork.history_scope` | `full` or `partial` |
| `omnigent.policy.scope` | `session` or `admin` |
| `omnigent.policy.type` | The bounded policy type supplied by the route, or `unknown` |
| `omnigent.approval.decision` | `accept`, `decline`, `cancel`, or `timeout` |

The recorder drops attributes outside this allowlist.  In particular, metrics
must never include session IDs, elicitation IDs, conversation or attachment
content, filenames, model IDs, policy names/handlers, tool names/inputs, or
other user-provided values.

## Covered feature operations

| Feature | Operations | Measurement boundary |
| --- | --- | --- |
| `dictation` | `take` | Each authenticated WebSocket take: normal stop, disconnect, capacity rejection, or engine failure. |
| `attachment` | `upload` | Each session attachment upload attempt. |
| `sharing` | `grant`, `update`, `revoke` | Each permission mutation; a grant to an existing target is `update`. |
| `fork` | `fork` | Each source-session fork request, tagged with full or partial history. |
| `policy` | `register`, `delete` | Each session- or admin-scoped policy mutation. |
| `model` | `switch` | An explicit, non-silent `model_override` PATCH only; unchanged effective values are `no_change`. |
| `context` | `compact` | Server-owned compact terminal outcome, or a native harness's terminal external compaction status. |
| `sub_agent` | `spawn` | Successful creation of a session with a durable parent link; root sessions and later child dispatches are excluded. |
| `approval` | `request`, `resolve` | Publication of an actionable approval and its terminal accept, decline, cancel, or timeout. |

The actor is the authenticated request principal where that boundary has one.
Background approval and native terminal edges fall back to the session owner,
then `local`.  This is deliberate: no guessed or request-unrelated identity is
emitted.

## Completion and deduplication

The operation context is idempotent: explicit terminal completion and context
exit cannot emit twice.  Instrumentation is placed at the authoritative
operation boundary rather than an initial request whenever an asynchronous
terminal result exists.  The implementation also avoids model PATCH echo
events, counts sub-agent creation rather than generic session-created events,
and routes both approval-resolution endpoints through the same terminal seam.

Native compaction status and approval lifecycle retries use bounded,
in-process correlation records to suppress repeated terminal frames or repeat
delivery paths.  This is intentionally not a distributed deduplication
protocol: retries handled by a different server replica, or after the bounded
record is evicted/restarted, can still be counted again.  Native compaction
frames have no reliable correlation ID, so their duration measures terminal
handling rather than a full server-to-native compaction duration.

## Initial limitations and future extensions

Shared sessions initially expose the actor and, only when it differs, owner.
Role, organization, and team dimensions are deliberately omitted; add them
only after a concrete analysis need establishes bounded semantics.  Likewise,
cross-replica deduplication, a durable compaction correlation ID, dashboards,
and monitors are out of scope for this initial implementation.
