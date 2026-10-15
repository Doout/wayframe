# Gateway configuration

The [MCP example](../examples/mcp-gateway/README.md) owns its executable, TOML configuration,
listeners, and state. The libraries remain usable without that application.
Start with [notes.toml](../examples/mcp-gateway/notes.toml) or
[notes-lb.toml](../examples/mcp-gateway/notes-lb.toml). The
[configuration types](../examples/mcp-gateway/src/config.rs) define all fields and validation.

## Credentials and limits

Set `WAYFRAME_CONFIG` to a trusted TOML file. `upstream` is the full, fixed HTTP(S) MCP endpoint.
Client entries map token environment variables to subjects, tenants, and explicit permissions.
`[tools]` maps each tool to its required permission. Unknown tools and operations deny.
Discovery filters results against current grants and denied-tool rules.

Upstream credentials must differ from client and admin credentials. TLS verification stays enabled;
`upstream_ca` adds private CA trust. The HTTP client disables redirects and ambient proxies.

| Setting | Default |
| --- | --- |
| `requests_per_second`, `burst` | 100, 200 per identity |
| `max_in_flight` | 128, held through response disposal |
| `max_request_bytes`, `max_response_bytes` | 32 KiB, 1 MiB |
| `timeout_seconds` | 15, including body collection and response consumption |
| `tenant_concurrency` | Optional separate per-tenant admission cap |

Optional `[auth]` verifies RS256 tokens using an explicit issuer, audience, and JWKS URL.
Only `permission_map` grants permissions from trusted claims; matching local users further
restrict grants and enabled state. The gateway publishes protected-resource metadata.
Upstream `[upstream_oauth]` uses separate client credentials, resource, scopes, and a private
secret file. It refreshes service tokens without exchanging caller tokens.

## Transports

| Binding | Configuration and behavior |
| --- | --- |
| HTTP/1 and HTTP/2 | Default TCP listener; JSON/SSE responses |
| TLS/mTLS | `WAYFRAME_TLS_CERT`, `WAYFRAME_TLS_KEY`, optional `WAYFRAME_TLS_CLIENT_CA` |
| HTTP over Unix | `WAYFRAME_UNIX_SOCKET`; unused absolute path, mode 0600 |
| stdio frontend | `WAYFRAME_TRANSPORT=stdio`, `WAYFRAME_LOCAL_TOKEN`; one request at a time |
| WebSocket | Authenticated `GET /ws`; custom bounded JSON messages |
| Framed TCP | `WAYFRAME_TRANSPORT=tcp`; custom length-prefixed messages over loopback or a TLS tunnel |
| stdio upstream | `[stdio_upstream]`; supervised process with bounded pending work |

Non-loopback HTTP listeners require TLS. The generic HTTP adapter preserves HTTP/2 trailers,
including gRPC status; the MCP example still validates MCP messages. A request deadline may
close its HTTP/2 connection, requiring other streams on that connection to reconnect.
Stdio keeps stdout for protocol messages and logs to stderr.

WebSocket messages contain JSON-RPC. Framed TCP uses a four-byte big-endian length and
`{"authorization":"Bearer ...","message":{...}}`. Both return bounded
`{"status":200,"session":null,"messages":[...]}` records and authenticate each message.
These are Wayframe bindings; standard MCP uses stdio and Streamable HTTP.

A supervised upstream requires an absolute executable and argument array, never a shell.
Its environment starts empty; only configured `environment` and named `inherit_env` entries
are passed through. A private authenticated loopback bridge connects HTTP callers to it.
Cancellation may terminate the process and fail its pending calls. Reload prepares a replacement;
failed calls are never replayed. See the [stdio configuration](../crates/wayframe-transport/src/stdio.rs).

## Routing and load balancing

The optional `lb` library selects registered resources without opening network connections.
A typed application implements `RoutingContext` and retains `Selection` through completion;
its final drop releases the reservation. The [native example](../crates/wayframe/examples/lb_cost.rs)
shows that contract.

| Strategy | Selection |
| --- | --- |
| `round_robin` | Next eligible backend |
| `least_connections` | Fewest active reservations, rotating ties |
| `weighted_round_robin` | Smooth weighted distribution |
| `quota_priority` | First backend below quota by priority; overflow to the last eligible backend |

Quotas are preferences; quota zero means unlimited. `hard_limit` separately caps active leases.
The gateway supports one `[load_balancer]` or named `[groups.<name>]` with ordered `[[routes]]`.
Routes match configured paths, hosts, methods, tenants, operations, and targets. Unmatched routes
deny. Backend IDs are globally unique, and every endpoint is operator-registered.
Per-backend `token_env` and `ca` override upstream defaults.

Reservations last through streamed response disposal. Passive cooldown affects later selections;
`health_path` and `health_interval_ms` enable bounded active probes. Probed groups need an enabled,
healthy backend before publication. Disabled or unhealthy pinned backends return unavailable.
Session affinity takes precedence over quota preferences. Load balancing does not replicate data;
the [two-replica example](../examples/mcp-gateway/README.md#two-replicas) explicitly shares SQLite.

## MCP ownership

The tested profile covers `2026-07-28` discovery/per-request metadata and `2025-11-25`
initialization/session transport. Mirrored method/target headers must agree with the bounded body.
Duplicate keys, ambiguous envelopes, invalid IDs, and unknown targets fail closed.

Legacy sessions bind tenant, subject, and backend. They expire after 30 idle minutes, are removed
on successful DELETE or upstream expiration, and use a bounded 4096-entry local registry.
Tasks and opaque continuations also bind their owner, original grants, backend, and generation.
Unowned access and task listing deny. Resource/prompt rules and subscription notification filters
require explicit configuration. Notification delivery rechecks current grants.

The gateway passes through modern input-required results. It does not execute client sampling,
elicitation, or roots requests. Legacy stdio callbacks receive method-not-found. Batch envelopes
and resumable SSE replay are outside this profile. Restart or reload requires reinitialization.

## Users and rate rules

Set `WAYFRAME_STATE_FILE` and a separate `WAYFRAME_ADMIN_TOKEN` together to enable management.
The [gateway example](../examples/mcp-gateway/README.md) includes both. With neither, TOML supplies
static clients and rules. First startup seeds users from TOML; later starts restore persistent
users and rules. State stores token digests in a private, atomically replaced file, bounded to
4 MiB and exclusively locked to one process.

```bash
source .env
python3 scripts/admin.py show
python3 scripts/admin.py create-user examples/mcp-gateway/user.json
python3 scripts/admin.py rotate-token alice
python3 scripts/admin.py update-user examples/mcp-gateway/user.json
python3 scripts/admin.py apply-rules examples/mcp-gateway/rules.json
python3 scripts/admin.py delete-user alice
```

Create and rotate return the token once. Protect that output. User identity fields are immutable;
permissions and `enabled` can change. Revocation rejects subsequent checks but cannot undo an
upstream side effect. After a lost create response, inspect state and rotate the user's token.

Rate rules use `scope` values `user`, `tenant`, or `global`. A null user selects a separate bucket
per user by default. Optional user, tenant, and tool selectors narrow a rule. `tool: null` applies
before body collection; a named tool applies after authorization. Every matching rule must have
capacity. Up to 1024 users and 128 rules are supported. Unrelated edits preserve bucket balances;
restart resets them. These limits and quotas are process-local.

| Admin endpoint | Operation |
| --- | --- |
| `GET /admin/state` | Redacted state, revision, ETag |
| `POST /admin/users` | Create a user and issue a token |
| `PUT`, `DELETE /admin/users/{id}` | Update or remove a user |
| `POST /admin/users/{id}/token` | Rotate a token |
| `PUT /admin/rules` | Replace the full rules document |
| `GET /admin/deployment`, `POST /admin/reload` | Inspect or reload a deployment |

Writes require `If-Match` from the last state/generation read. Missing preconditions return 428;
stale edits return 412. The CLI reads revisions before writing. Admin bodies are bounded to
64 KiB and reject duplicate keys. Browser Origin headers deny, and Host must match the listener.
`[[admins]]` assigns `reader`, `users`, `rules`, `deployment`, or `owner` roles. Bootstrap has owner
access; mutations require the matching role. Use `--url` for another HTTP(S) listener. Stdio can
bind `WAYFRAME_ADMIN_LISTEN` on loopback. Remote administration requires HTTPS.

## Traces and health

Set these before starting the gateway with an OTLP collector listening:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_SERVICE_NAME=wayframe-gateway
export OTEL_TRACES_SAMPLER=traceidratio
export OTEL_TRACES_SAMPLER_ARG=1
```

`http/json` is also supported. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` sets a complete trace URL;
the general endpoint appends `/v1/traces`. No endpoint means no export worker. Sampling is local;
client flags cannot force collection. Valid W3C context is propagated, and the response includes
`x-wayframe-trace-id`.

| Spans | Timing |
| --- | --- |
| `gateway.request`, `gateway.admission` | Request lifetime through body disposal and permit acquisition |
| `policy.step`, `gateway.body.inspect` | Individual controls and bounded body collection |
| `workflow.select`, `workflow.path`, `workflow.node` | Path choice, dependencies, scheduler wait, execution |
| `gateway.upstream`, `gateway.response.stream` | Upstream headers and streamed response lifetime |
| `admin.request`, `admin.persist` | Administration and durable state writes |

Tool names, denial codes, backend selections, and generations are recorded. Credentials, headers,
arguments, and response bodies are excluded. Timings are wall time, not CPU time. The export queue
is bounded; collector pressure can drop telemetry. Graceful shutdown flushes it.

`/healthz` reports liveness. `/readyz` checks required prepared resources. `/metrics` requires
`gateway:metrics` permission and includes latency histograms, plugin/cache state, routing, and
workflow selections. Generation-scoped measurements reset on reload. OTLP exports traces;
audit records use a separate private file.

## Reloads and audit

```bash
python3 scripts/admin.py deployment
python3 scripts/admin.py reload
```

Reload reads the original trusted config path. It prepares clients, credentials, probes, processes,
plugins, and policies before atomic publication. Failure or a stale generation leaves the current
deployment active. In-flight responses retain their old resources; sessions and continuation IDs
are generation-scoped. New TLS handshakes use the replacement certificate.

Restore the old config and reload to roll back. Changes to `listen`, `max_in_flight`,
`timeout_seconds`, and admin roles require restart. Update managed users and rules through the
admin API; TOML client/tool/rate edits cannot be reloaded. Environment values belong to the
process; use configured private credential files for rotation. OAuth secret files are reread on refresh.

State persistence finishes before publication, even if an admin client disconnects. Audit records
are fsynced before and after mutations. A failed commit-record write may report failure after the
state changed, so read the current revision before retrying. The bounded, locked digest-chain file
detects corruption but has no external tamper-proof anchor. Archive it during planned restarts.
