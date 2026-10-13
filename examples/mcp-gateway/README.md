# MCP gateway

A configurable Wayframe application that fronts an MCP server with authenticated identities,
tool permissions, request limits, session ownership, and optional Wasm policy. The framework
remains a library; this executable is one application built from it.

## Run against a server

Install the workspace's Rust toolchain. For the SDK example and integration tests, install
`requirements-dev.txt` in a Python virtual environment.

```bash
umask 077
python3 scripts/dev_tokens.py > .env
source .env
export WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml
```

Start the official-SDK notes server in one terminal:

```bash
source .env
python3 examples/notes-server/server.py --database var/notes.sqlite3
```

Start the gateway in another:

```bash
source .env
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml \
  cargo run --locked -p wayframe-example-mcp-gateway
```

The client endpoint is `http://127.0.0.1:8080/mcp`. The configured upstream endpoint is
`http://127.0.0.1:9001/api/mcp`. Set `upstream` to your MCP server's full HTTP(S) endpoint,
configure tool permissions, and map client token environment variables to identities.
The upstream credential is optional and must be distinct from all client credentials.

The example reader can read notes; the operator can write/delete them, inspect maintenance
statistics, and read gateway metrics. Tool authorization uses the inspected JSON body.
Unknown tools and unsupported operations are denied. Tool, resource and prompt discovery is
filtered against the caller's current grants, including permission edits and denied-tool rules.

`notes.toml` contains no secrets. Configuration rejects unknown fields, invalid identities,
ambiguous endpoint paths, and unbounded limits. Optional settings are:

| Setting | Default |
| --- | --- |
| `requests_per_second`, `burst` | 100, 200 per identity |
| `max_in_flight` | 128, retained through response completion |
| `max_request_bytes`, `max_response_bytes` | 32 KiB, 1 MiB |
| `timeout_seconds` | 15, covering policy, upload, upstream work, and response consumption |
| `allowed_origins`, `allowed_hosts` | Exact loopback listener values |
| `upstream_ca` | Public trust roots; add a PEM CA file for a private server |

TLS certificate and hostname verification remain enabled. TCP listeners default to loopback;
non-loopback listeners require configured TLS. The gateway supports service bearer tokens,
JWKS-backed resource-server authentication and independent upstream OAuth client credentials.

## Frontends and execution boundaries

| Path | Behavior |
| --- | --- |
| HTTP over TCP | POST tools, JSON/SSE results, authenticated legacy session GET/DELETE |
| HTTP over a Unix socket | Same HTTP semantics; socket mode 0600 and bearer authentication |
| MCP stdio | Newline-delimited JSON-RPC; streams SSE messages back to the local client |
| HTTPS upstream | Verified TLS with public or configured private CA roots |
| TLS/mTLS frontend | Verified TLS, optional client certificates, HTTP/1 and HTTP/2 |
| Supervised stdio upstream | Isolated child credentials, bounded calls, cancellation and progress |
| Custom WebSocket/framed TCP | Bounded message bindings through the same authorization path |

For a Unix socket on macOS/Linux:

```bash
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml \
WAYFRAME_UNIX_SOCKET="$PWD/target/wayframe.sock" \
  cargo run --locked -p wayframe-example-mcp-gateway
```

The socket path must be an unused absolute path. Normal shutdown removes it; a stale socket
from an abrupt process kill requires operator cleanup.

For a stdio client, configure its command to run the gateway binary with:

```bash
export WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml
export WAYFRAME_TRANSPORT=stdio
export WAYFRAME_LOCAL_TOKEN="$WAYFRAME_READER_TOKEN"
target/debug/wayframe-example-mcp-gateway
```

Stdout carries only MCP messages; logs go to stderr. The stdio adapter processes one request
at a time and handles both modern requests and legacy initialization. It owns any upstream
HTTP session for the lifetime of its local client and sends DELETE when stdin closes.
It does not bridge unsolicited server requests that require concurrent input handling.
Each HTTP session is bound
to its authenticated tenant and subject, expires after 30 minutes idle, and is removed on successful
DELETE or upstream 404. The registry is bounded to 4096 entries and is local to one gateway process.
Restarting the gateway invalidates its session ownership records; clients must initialize again.

The framework also exposes native calls, bounded task bridges, Unix policy RPC, HTTPS policy RPC,
and Wasm policies. These are execution boundaries, distinct from MCP wire transports. Bounded custom TCP/WebSocket bindings are available alongside standard MCP transports. QUIC, UDP
and shared-memory adapters remain separate expansion points. See [transport contracts](../../docs/transports.md).

## Wasm policy

```bash
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml \
WAYFRAME_PLUGIN_MANIFEST=plugins/path-control/plugin.toml \
  cargo run --locked -p wayframe-example-mcp-gateway --features wasm
```

The sample policy denies `admin_preview` even to an operator. Native permission denials remain
terminal. Compilation and clean-instance preparation finish before a listener is bound.

## Load-balanced replicas

[Load-balancing setup](../../docs/load-balancing.md) covers all four routing strategies and the
complete [two-replica configuration](notes-lb.toml). The same TCP/Unix/stdio frontends route to
registered endpoints, preserving backend credentials, TLS verification, and legacy session affinity.
Reservations remain active until response disposal; active probes and passive cooldown affect
subsequent calls. Local deadlines and downstream cancellation do not eject healthy backends.

## Operational endpoints

`GET /healthz` reports listener liveness; it does not probe upstream health. `GET /metrics` requires
a configured client with `gateway:metrics` permission and returns request, active, denial,
overload, upstream-error, byte counters, latency histograms and plugin status. `/readyz` checks
required identity/plugin/backend resources. These endpoints are not forwarded upstream.

## Users, controls, and traces

Enable persistent users and live policy changes with the [admin API and CLI](../../docs/management.md).
Tokens can be issued, rotated, disabled, and revoked; tool controls and user/tool rate rules take
effect without a restart. Management uses a separate admin credential and private state file.

[OTEL tracing](../../docs/observability.md) exports request, admission, individual policy, body,
upstream, response-stream, and management-persistence durations. It supports W3C correlation and
optional sampling. Export is opt-in, and arguments/credentials are excluded.

## Test and benchmark

```bash
bash scripts/verify.sh
bash scripts/benchmark.sh
```

Verification includes the original fixture plus official SDK calls against the running Rust
binary, database persistence after restart, streamed progress, session isolation, permission
and parser failures, Unix/stdio frontends, trusted/untrusted TLS, and Wasm denials.
See [performance methodology](../../docs/performance.md) and [MCP scope](../../docs/mcp-profile.md).

The original small fixture remains available through `bash scripts/demo.sh [--wasm]`.


## Extending the gateway

Use `[[plugins]]` for native schema/redaction/CORS/cache hooks or multiple Wasm policies.
`[[async_policies]]` selects linear or graph execution per inspected request, using exact optional
`operations`/`targets` selectors and `async_parallel_threshold` (default 2). Parallel gates join before
final grant checks. Selection reasons and timings are traced. See [workflows](../../docs/workflows.md) and
[plugins](../../docs/plugins.md) for contracts and examples.

Named `[groups.<name>]` configurations use the same fields as `[load_balancer]`.
`[[routes]]` entries select a group using exact/prefix path, optional hosts/methods/tenants and
verified operations/targets. Backend IDs must be globally unique. Unmatched routes deny; an owned
session/task pins a registered backend and never silently fails over. A backend's `hard_limit`
is a hard in-flight cap independent of quota-priority overflow. `health_path` and
`health_interval_ms` enable active bounded probes. `tenant_concurrency` rejects excess requests
before body collection. See [routing and balancing](../../docs/load-balancing.md).

Enable TLS using `WAYFRAME_TLS_CERT` and `WAYFRAME_TLS_KEY`; add `WAYFRAME_TLS_CLIENT_CA` to require
verified client certificates. Bearer/JWT authorization remains required. Non-loopback listeners
require TLS. The private admin listener stays loopback-only.

OIDC configuration uses an explicit `[auth]` issuer, audience/resource URI, `jwks_uri`,
`tenant_claim`, `permissions_claim` and `[auth.permission_map]`. The gateway publishes protected-resource
metadata at the root and MCP well-known path and challenges unauthorized clients. It does not run
an authorization server. Optional `[upstream_oauth]` or backend `oauth` configuration specifies a
fixed HTTPS `token_endpoint`, `client_id`, private `secret_file`, `resource`, `scopes`, and optional
CA file. Provider tokens are refreshed independently; caller tokens are never exchanged.

For operations, [scoped administration](../../docs/management.md) and
[prepared reloads](../../docs/reloads.md) cover audit files, ETags, credential/certificate changes,
rollback and the settings that require restart. `scripts/admin.py deployment` and `reload` are the
corresponding CLI operations.
