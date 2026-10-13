# Request traces

The reference gateway exports OpenTelemetry traces through OTLP/HTTP, using the official Rust
SDK and exporter. Set an endpoint to enable it; without an endpoint there is no export worker.
Logs stay on stderr, including with stdio. Logging filters do not disable exported spans.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_SERVICE_NAME=wayframe-gateway
export OTEL_TRACES_SAMPLER=traceidratio
export OTEL_TRACES_SAMPLER_ARG=1
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml \
  cargo run --locked -p wayframe-example-mcp-gateway
```

Point this at an OpenTelemetry Collector, Jaeger OTLP endpoint, or another compatible backend.
`http/json` is also supported. `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` sets the complete trace URL;
the general endpoint appends `/v1/traces`. Exporter headers, TLS verification, and timeout follow
[the official exporter configuration](https://docs.rs/opentelemetry-otlp/0.33.1/opentelemetry_otlp/).
This application deliberately enables HTTP exporters only; gRPC export is not configured.

Sampling supports `always_on`, `always_off`, or `traceidratio` with a ratio in 0..1. The default is
ratio 1. Sampling is decided locally; a client's sampled flag cannot force collection. Valid W3C
trace context is accepted for correlation. Arbitrary baggage is stripped. The gateway creates an
upstream client span and injects its W3C context, allowing an instrumented upstream to continue
that trace. `x-wayframe-trace-id` on the response helps find it in the backend.

| Span | Measured interval / attributes |
| --- | --- |
| `gateway.request` | Request processing through response-body disposal; method, status, request ID, MCP method/tool, stream outcome |
| `gateway.admission` | In-flight permit acquisition |
| `policy.step` | Each native/deferred/Wasm/bridge block; block ID, phase, continue/deny, denial code |
| `gateway.body.inspect` | Bounded body collection and deadline |
| `gateway.upstream` | Upload and upstream response headers; client kind, status/error |
| `gateway.response.stream` | Consumption/stream disposal, including deadlines and disconnects |
| `admin.request` | Admin API operation |
| `admin.persist` | Durable state write before publication; new revision |

With load balancing enabled, request and selection-step spans also record `lb.backend.id`,
`lb.strategy`, and `lb.overflow`. See [routing controls](load-balancing.md).

Span start/end timestamps provide elapsed time, including waits in deferred blocks. Requests denied
before dispatch have no upstream span; later policy steps do not run. Root status records the HTTP
result; server/upstream errors mark OTEL error status. Streaming errors mark the root as an error.
Stream outcome is `complete`, `error`, or `cancelled`. Known-length completion is counted when all
bytes have been consumed, even if the HTTP server never polls again for EOF. The root lasts through
stream disposal, rather than ending at response headers. Wall time is not CPU time; individual
network and upstream execution time require instrumentation in those services.

Authentication tokens, admin secrets, headers, argument values, note values, and response bodies
are excluded from trace attributes. Tool names and internal denial codes are recorded; operators
should decide whether these metadata are acceptable in their telemetry backend. SDK diagnostic
errors remain visible on stderr. The batch queue is bounded to 4096 spans, exporting batches of
up to 512. Collector failure does not participate in policy decisions; sustained pressure can
lose telemetry. Graceful shutdown flushes the provider; abrupt process termination cannot.
This exports traces and exposes HTTP metrics/histograms. Audit records use a separate durable
local file; OTLP metrics/log export is not enabled by this adapter.

The reusable runtime enables per-block tracing with its optional `tracing` feature; HTTP enables
it. Bare typed applications can opt in without choosing an exporter. The dependency-free core
remains unchanged. HTTP W3C propagation is behind `wayframe-http`'s `otel` feature, enabled by this
reference application, and exposed as `wayframe`'s optional `otel` feature.

## Verify and measure

`bash scripts/verify.sh` runs a real gateway and official MCP upstream, receives actual OTLP JSON
and protobuf exports, decodes protobuf with the official schema, and checks parentage, durations,
success/denial/upstream errors, management persistence, and credential exclusion. This uses a
bounded local test receiver rather than asserting on log lines. Set `WAYFRAME_TRACE_REPORT` to an
output directory to retain normalized spans during verification.

```bash
bash scripts/benchmark.sh --managed --trace-protocol http/protobuf \
  --native-only --skip-boundaries --requests 500 --rounds 3 \
  --transports tcp,unix,https,stdio --formats json --concurrency 1 8 --payloads 64
```

Run the identical workload without `--trace-protocol` for a disabled-export baseline, and optionally
repeat with `--trace-sample-ratio 0` to measure unsampled instrumentation. Reports record the selected
sampling/storage mode and aggregate exported step durations. Step aggregates include setup,
warmup, and timed calls; end-to-end latency samples exclude setup/warmup. Each run still pairs direct and
gateway calls. Comparing differences between separate runs estimates added tracing overhead;
retain spread and CPU alongside latency. The local receiver itself consumes resources. See
[performance methodology](performance.md) for measurement limits.


## Graphs, protocol outcomes and lifecycle

Graph traces include execution and node outcomes, dependency edges, dependency wait, scheduler wait,
and execution nanoseconds. Branch spans overlap when work is parallel, while the join follows their
completion. Cancellation closes node spans; CPU work may outlive caller timeout while retaining its
pool slot. Source/cancellation/budget errors are distinguished, and RPC errors inside HTTP 200 plus
gRPC error trailers mark the request as a protocol error.

Metrics include bounded histograms for admitted request lifetime, preflight, body inspection, policy
and upstream response headers, plus cache state and configured plugin instances. They are scoped to
the active generation and restart on reload; spans include deployment generation for correlation.
`/healthz` is liveness and `/readyz` checks required prepared resources.

Audit records live separately from traces and contain redacted actor/action/object/revision/outcome.
They never include issued credentials or request arguments. See [management](management.md) and
[reloads](reloads.md) for persistence and retry behavior.
