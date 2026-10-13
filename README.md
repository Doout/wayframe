# Wayframe

**Small blocks. Any gateway.**

Wayframe is a library-first Rust framework for building gateways. Applications choose their
protocol, policy blocks, listeners, and deployment model. A dependency-free core defines typed
blocks and linear plans. Optional crates add typed async graphs, plugin factories, HTTP forwarding,
identity and administration, routing, resilience, caching, MCP inspection, and sandboxed Wasm policies.

This is an early alpha. See [security boundaries](SECURITY.md) before deploying it.

```text
Your application
  ├── application-owned context and trusted native blocks
  ├── typed policy plan + cooperative runtime
  ├── optional HTTP / MCP / authentication adapters
  └── optional Wasm policies or task / IPC / network bridges
```

## Setup

Use macOS or Linux with Git, Rust installed through rustup, Python 3.11 or later, and OpenSSL
on your `PATH` for the TLS tests and benchmarks. Rust needs the platform's C/C++ build tools
(Xcode Command Line Tools on macOS; a compiler and linker on Linux). The workspace pins Rust
1.99.0, rustfmt, and Clippy in `rust-toolchain.toml` and commits its dependency lockfiles.

```bash
git clone https://github.com/doout/wayframe.git
cd wayframe
rustup show active-toolchain
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
```

Activate `.venv` in each new terminal used for the examples, tests, or benchmarks. Python is
needed for those tools; the Rust library can be used independently. Packages are not yet
published to crates.io.

## Library quick start

```bash
cargo check --locked -p wayframe --no-default-features
cargo run --locked -p wayframe --example typed_message
cargo run --locked -p wayframe --example live_generation
```

A sibling application can use the local facade and select the adapters it needs:

```toml
[dependencies]
wayframe = { path = "../wayframe/crates/wayframe", features = ["http", "controls"] }
```

The facade has no default optional features. Implement `Block<MyContext>` for an
application-owned context. Immediate blocks return a decision without allocating a boxed future;
deferred blocks opt into async work. Plans validate block identifiers and phase order, and a deny
decision stops evaluation. See the [typed-message example](crates/wayframe/examples/typed_message.rs)
and [block authoring guide](docs/blocks.md).

## MCP gateway

The reference gateway includes persistent users and rules, scoped admin roles, audit records,
OIDC/JWKS authentication, OAuth client credentials for upstream services, named backend groups,
health probes, tenant concurrency limits, and prepared deployment reloads. Native and Wasm plugins
can run alongside parallel external policies. OTLP traces show each phase, graph node, queue wait,
upstream call, filter and response stream; metrics include latency histograms and plugin readiness.

Frontends include HTTP/1 and HTTP/2, TLS/mTLS, HTTP over Unix sockets, and stdio. Optional WebSocket
and length-framed TCP bindings run the same policy path. Upstreams may use HTTP(S) or a supervised
stdio process. The generic HTTP adapter preserves HTTP/2 request/response trailers, including gRPC
status. WebSocket/TCP message bindings are Wayframe extensions; standard MCP transports remain
stdio and Streamable HTTP. See the [transport guide](docs/transports.md).

The [notes example](examples/notes-server/README.md) uses the official MCP SDK and performs real
SQLite writes, reads, and deletes. The integration tests verify persisted results, streaming,
session isolation, denied writes, and the supported transports.

After setup, generate distinct local development credentials once:

```bash
umask 077
python3 scripts/dev_tokens.py > .env
```

In the first terminal, start the notes server:

```bash
source .venv/bin/activate
source .env
python3 examples/notes-server/server.py --database var/notes.sqlite3
```

In the second terminal, start the gateway:

```bash
source .env
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml \
  cargo run --locked -p wayframe-example-mcp-gateway
```

Connect an MCP client to `http://127.0.0.1:8080/mcp` with
`Authorization: Bearer <WAYFRAME_OPERATOR_TOKEN>` to write/read/delete notes, or use
`WAYFRAME_READER_TOKEN` for read access. The gateway connects to the authenticated notes server
at `http://127.0.0.1:9001/api/mcp`. Stop both processes with Ctrl-C. `.env` and the persistent
`var/` directory are ignored by Git; keep them private. The example database is shared by
authorized users, without per-user note ownership.

See [graph workflows](docs/workflows.md) and [plugin authoring](docs/plugins.md) to extend the framework.

See [gateway setup](examples/mcp-gateway/README.md) to run it against the notes service or your
own MCP tools server. See [user management](docs/management.md) and [OTEL tracing](docs/observability.md)
for operational controls. Configure full upstream URLs, client identities, tool grants, and limits in
TOML; credentials come from named environment variables.

The smaller read-only fixture is also available:

```bash
bash scripts/demo.sh
bash scripts/demo.sh --wasm
```

Both commands build the Rust gateway, generate independent temporary credentials, exercise MCP
requests, and stop the local processes. See the [MCP profile](docs/mcp-profile.md) for capability
and transport boundaries.

## Load balancing

The optional `lb` feature provides transport-independent blocks for round robin, least connections,
smooth weighted round robin, and quota priority. The MCP gateway can route to registered replicas,
pin legacy sessions and task continuations, enforce hard concurrency caps, and probe or cool down
failed backends while retaining reservations through streaming.
See [load balancing](docs/load-balancing.md) for configuration, typed contexts, and measurement.

## Workspace

| Package | Responsibility |
| --- | --- |
| `wayframe` | Public facade with optional adapters |
| `wayframe-core` | `Context`, `Block<C>`, `Execution`, `Decision`, and validated `Plan<C>` |
| `wayframe-runtime` | Pipeline evaluation, deadlines, and atomic `Live<T>` generations |
| `wayframe-flow` | Typed DAGs, parallel async nodes, joins, choices, mandatory gates and CPU bounds |
| `wayframe-plugins` | Factories, settings schemas, dependencies, capabilities and lifecycle bundles |
| `wayframe-policies` | Configurable schema, redaction, CORS and scoped response-cache plugins |
| `wayframe-management` | Persistent users/rules, scoped admin API, sessions and continuation ownership |
| `wayframe-auth` | JWKS refresh, JWT validation, resource metadata and upstream OAuth tokens |
| `wayframe-listeners` | HTTP/1+HTTP/2, TLS/mTLS, connection bounds, I/O deadlines and drain |
| `wayframe-codec` | Duplicate-safe JSON and incremental SSE records |
| `wayframe-routing` | Tenant/operation route tables and named backend groups |
| `wayframe-resilience` | Circuit breakers, keyed bounded admission and active health probes |
| `wayframe-cache` | Scoped bounded storage, TTL, generation invalidation and single-flight |
| `wayframe-observability` | OTLP export, histograms and durable redacted audit records |
| `wayframe-http` | Embedded HTTP gateway, streaming, and metadata/body limits |
| `wayframe-blocks` | Bearer tokens, RS256 JWT, origin controls, and local token buckets |
| `wayframe-transport` | Task/Unix/HTTPS bridges, supervised stdio and framed TCP/WebSocket adapters |
| `wayframe-lb` | Native routing blocks: round robin, least connections, smooth weighted round robin, quota priority |
| `wayframe-mcp` | Bounded MCP inspection and tool permissions |
| `wayframe-wasm` | WIT policy runtime, metadata capabilities, and clean prewarming |
| `wayframe-example-mcp-gateway` | Reference application under `examples/` |

The plugin contract is in [wit/policy.wit](wit/policy.wit), with a guest example under
[plugins/path-control](plugins/path-control/src/lib.rs). Each warmed guest instance is used once
and replenished, so mutable guest state is not reused between requests.

## Testing

After [setup](#setup), run the full verification suite:

```bash
source .venv/bin/activate
bash scripts/verify.sh
# Build and execute the Rust-authored component, beyond the portable WAT fixture:
cargo install wasm-tools --version 1.261.0 --locked
bash scripts/build-wasm-guest.sh
python3 scripts/smoke.py --wasm --plugin-manifest target/path-control/plugin.toml
```

Verification checks Python and Rust formatting, the Python fixture, the native
facade, all workspace features and targets, doctests, Clippy, both fixture gateway modes, bounded input mutation tests, and
official SDK integration across the supported frontends.
It uses the committed lockfile and writes local reports under `target/verification/`.
Live tests start isolated gateway/upstream processes with temporary credentials and data,
exercise persistent users, rate rules, load balancing, verified TLS, and actual OTLP exports,
then clean up. GitHub Actions runs the same suite on Linux and macOS, plus a benchmark smoke
run on Linux. Full verification builds Wasmtime and can take several minutes on a fresh checkout.

For focused checks:

```bash
cargo test --locked --workspace --all-features --all-targets
python3 -m unittest discover -s examples/mcp-server/tests -v
# Live gateway tests need the gateway binary with Wasm enabled:
cargo build --locked -p wayframe-example-mcp-gateway --features wasm
python3 -m unittest discover -s scripts/tests -v
```

## Benchmarks

Run the full release-build suite from the repository root with `.venv` active:

```bash
bash scripts/benchmark.sh
```

This measures paired direct-versus-gateway MCP calls across TCP, Unix, HTTPS, and stdio paths,
with JSON/SSE responses, native/Wasm controls, varying payloads, and concurrency. It retains raw
samples, p50/p95/p99, throughput, CPU, memory, and failures. Separate measurements cover native
pipelines and task, Unix, HTTPS, and Wasm policy boundaries.

For a short harness check (200 timed calls, plus warmup):

```bash
bash scripts/benchmark.sh --requests 100 --rounds 1 --concurrency 1 \
  --payloads 64 --transports tcp --formats json --native-only --skip-boundaries
```

For fixed arrival rates and an overload/soak workload:

```bash
bash scripts/benchmark.sh --requests 5000 --rounds 3 --concurrency 32 \
  --arrival-rate 500 --transports tcp --formats json --payloads 64 \
  --native-only --skip-boundaries --output target/benchmarks/open-loop
```

Open-loop latency starts at the scheduled arrival. Every offered request is counted; generator
capacity drops and request failures remain in the report. Use a rate your load generator can
sustain, then increase it deliberately to measure overload. JSON and CSV survive failed rounds.

Reports are written under ignored `target/benchmarks/`; pass `--output <directory>` to retain
separate runs. The command exits with an error if any scenario records request failures.

| Report | Contents |
| --- | --- |
| `mcp-overhead.json` / `.csv` | Raw request timings, paired overhead, percentiles, throughput, CPU, RSS, errors, and environment |
| `policy-boundaries.json` | Native, task, Unix, HTTPS, and Wasm policy invocation measurements |
| `native-pipeline.json` | Repeated native pipeline batches against an empty-plan baseline |
| `native-load-balancer.json` | Four routing algorithms with 2/8/64 backends and reservation/release checks |
| `graph-scheduling.json` | Graph scheduling and node waits against equivalent linear plans |

To compare the lightweight linear runtime with graph scheduling:

```bash
mkdir -p target/benchmarks
cargo run --locked --release -p wayframe --no-default-features --features flow \
  --example flow_cost > target/benchmarks/flow-cost.json
```

`--skip-boundaries` omits the four microbenchmark reports. To isolate native load balancing
without other facade features:

```bash
mkdir -p target/benchmarks
cargo run --locked --release -p wayframe --no-default-features --features lb \
  --example lb_cost > target/benchmarks/lb-only.json
```

For durable user controls and step timing through a local OTLP protobuf receiver:

```bash
bash scripts/benchmark.sh --managed --trace-protocol http/protobuf \
  --requests 300 --rounds 3 --concurrency 1 --payloads 64 \
  --transports tcp --formats json --native-only --skip-boundaries \
  --output target/benchmarks/managed-tracing
```

Repeat the same workload without `--trace-protocol` and with `--trace-sample-ratio 0`, using
different output directories, to compare disabled, sampled, and unsampled tracing. Results
depend on the machine, workload, and build features; native batch cost excludes network and
MCP execution, and does not establish deployment capacity.

See [performance methodology](docs/performance.md) before interpreting overhead differences.

## Scope

The framework now supports linear policies and typed local graphs. Async tasks are bounded,
cancelled cooperatively, and joined explicitly before dependent sync jobs. Native plugins are
trusted application code; Wasm grants and signed publisher trust are separate decisions.

The reference control plane, rate buckets, session/task ownership and health state are local to
one process. It does not provide fleet-wide quotas or durable distributed job execution. It never
replays tool calls automatically. Legacy server-to-client callbacks on a stdio upstream receive a
method-not-found response; modern input-required results pass through the owned continuation path.
See [MCP profile](docs/mcp-profile.md), [reload behavior](docs/reloads.md) and
[remaining qualification work](docs/roadmap.md) for the supported contracts.

Further reading: [Architecture](docs/architecture.md), [Plugins](docs/plugins.md),
[Roadmap](docs/roadmap.md) and [Contributing](CONTRIBUTING.md).

Licensed under [Apache-2.0](LICENSE).
