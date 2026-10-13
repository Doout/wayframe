# Load-balancing blocks

Wayframe provides four routing strategies: `round_robin`,
`least_connections`, `weighted_round_robin`, and `quota_priority`. The optional `lb` facade feature
exports transport-independent routing blocks. Backend identifiers select registered application
resources; the routing library does not parse client URLs or open network connections.

| Block | Selection |
| --- | --- |
| `RoundRobin` | Rotate through eligible backends |
| `LeastConnections` | Fewest active reservations; rotate equal-load ties |
| `WeightedRoundRobin` | Smooth weighted round robin over positive backend weights |
| `QuotaPriority` | First backend below quota, ordered by priority then configuration order; overflow to the last eligible backend |

`LoadBalance::new(id, pool, strategy)` selects the same algorithms at runtime. Weights apply only
to weighted round robin; quotas and priorities apply only to quota priority. Quota zero means
unlimited. Quotas are routing preferences, not hard rejection limits: overflow is intentional and
reported in metrics/traces. Unknown algorithms, duplicate/invalid IDs, nonpositive weights, and
oversized pools fail configuration. Pools support 1..128 backends.

## Native typed applications

```toml
[dependencies]
wayframe = { path = "../wayframe/crates/wayframe", features = ["lb"] }
```

Implement `lb::RoutingContext` alongside `Context`. Its `set_selection` method stores a
`lb::Selection`; optionally provide `pinned_backend` for trusted affinity. Map `backend_id()` to
your worker, channel, native service, or registered network endpoint. Keep the selection until
execution finishes. [The native benchmark](../crates/wayframe/examples/lb_cost.rs) contains a small
complete typed context and pipeline.

All blocks run in `Phase::Dispatch`, after authentication, inspection, permissions, and limits.
Selection and reservation happen in one critical section, so concurrent callers cannot choose
based on unreserved counts. Cloning a selection keeps the same reservation; its final drop
releases exactly once. Request-context copies and queued work must retain that clone through
completion or cancellation. `Pool::set_enabled` supports application-controlled activation,
health-check input, and draining: disabled backends receive no new selections, while admitted
work remains counted. `Selection::failed` reports a failure with cooldown; stale successes cannot
clear a later failure. Eligibility is checked when selecting, including pinned selections.

This implementation tracks quotas and health locally within a shared `Pool`. Separate pools or
processes have separate counters. It does not implement the Go proxy's Redis/Badger shared quota
stores, leases across processes, provider registry, model aliases, or active health probes.

## MCP replicas

A complete example is [notes-lb.toml](../examples/mcp-gateway/notes-lb.toml). Generate credentials
as in [gateway setup](../examples/mcp-gateway/README.md), then run two compatible MCP replicas:

```bash
source .env
python3 examples/notes-server/server.py --port 9001 --database var/notes.sqlite3
```

In another terminal:

```bash
source .env
python3 examples/notes-server/server.py --port 9002 --database var/notes.sqlite3
```

Start the gateway:

```bash
source .env
WAYFRAME_CONFIG=examples/mcp-gateway/notes-lb.toml \
  cargo run --locked -p wayframe-example-mcp-gateway
```

The sample uses weighted round robin with weights 1:3. Change `load_balancer.strategy` to another
listed strategy. The configured quotas are used when selecting `quota_priority`; smaller
`priority` values are tried first. `enabled = false` removes a backend from eligibility.
The original single-upstream configuration remains valid without a load-balancer section.

Each backend has a full fixed HTTP(S) `endpoint`, optional `token_env` and optional PEM `ca`.
Without overrides, credentials and additional CA trust inherit from the original upstream settings.
Credentials must differ from client/admin secrets. Certificate/hostname verification, disabled
redirects and ambient proxies, and bounded requests/responses remain in effect. Registration is
mandatory: an absent or unknown selection fails closed rather than using an arbitrary destination.

The HTTP adapter retains the reservation through response consumption/disposal, including SSE,
errors, and disconnects. Transport failures, request/stream deadlines, oversized responses, and
HTTP 502/503/504 place a backend in a configurable cooldown (`failure_cooldown_ms`, default 1000,
range 0..600000). Zero disables cooling. Eligible backends recover after that interval; there is
no background probe. HTTP application errors and MCP tool errors are not decoded into health.
The failed request is returned to the caller; subsequent requests can select another backend.
The gateway does not retry a dispatched MCP call or replay a write.

Legacy MCP sessions bind both identity and backend when initialization succeeds. POST, GET,
DELETE, and notifications stay on that backend. A pinned unavailable/disabled backend returns 503,
even if another replica is available. Affinity takes precedence over quota preferences and can
produce overflow. Clients may reinitialize after loss; moving an existing session would corrupt
upstream state. Client-supplied backend headers cannot override affinity or choose an origin.
The local session registry remains bounded and expires after 30 minutes idle.

TCP, Unix-socket, and stdio frontends use the same routing block. HTTPS backend trust and credentials
are configured separately per destination. Replicas must offer compatible tools/protocols. Load
balancing does not replicate application data: the notes setup shares SQLite explicitly. Use an
appropriate shared store or application-specific affinity when balancing other stateful services.

## Observation and verification

Authenticated `/metrics` includes `load_balancer.backends`: backend ID, enabled/healthy state,
active reservations, selections, failures, and overflow counts. Counters are process lifetime values.
[OTEL traces](observability.md) include `lb.backend.id`, `lb.strategy`, and `lb.overflow` on the
request and load-balancer policy-step spans; the span timestamps measure selection overhead.
Endpoints, credentials, and tool arguments are not recorded.

`bash scripts/verify.sh` runs algorithm/concurrency/lease tests and two real official-SDK MCP servers.
Tests check physical backend results, weighted distribution, legacy affinity, SSE-held reservations,
cooldown failover, all-disabled rejection, TLS/credential isolation, Unix/stdio routing, and actual
OTLP exports. The reusable HTTP tests also cover missing/unregistered selections, a later denial,
and dropping an unpolled response.

```bash
cargo run --locked --release -p wayframe --no-default-features --features lb --example lb_cost
```

This isolates native selection and final lease release in repeated pipeline batches over 2, 8,
and 64 backends, with a separate empty-plan baseline. It validates selection totals and zero active
reservations afterward. It excludes network, MCP, TLS, exporter, and saturation effects.
`bash scripts/benchmark.sh` also saves this report in `target/benchmarks/native-load-balancer.json`.
Use the full gateway workload to measure network/application effects; native batch timing alone
is not a capacity measurement or production latency guarantee.
