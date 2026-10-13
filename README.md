# Wayframe

**Small blocks. Any gateway.**

Wayframe is a library-first Rust framework for building gateways. Applications choose their
protocol, policy blocks, listeners, and deployment model. A dependency-free core defines typed
blocks and linear plans; optional crates add HTTP forwarding, authentication, MCP inspection,
transport bridges, and sandboxed Wasm policies.

This is an early alpha. See [security boundaries](SECURITY.md) before deploying it.

```text
Your application
  ├── application-owned context and trusted native blocks
  ├── typed policy plan + cooperative runtime
  ├── optional HTTP / MCP / authentication adapters
  └── optional Wasm policies or task / IPC / network bridges
```

## Quick start

Install Rust with rustup and Python 3.11 or later. The workspace selects Rust 1.99.0 and includes
a lockfile. Packages are not yet published to crates.io.

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

The configurable gateway fronts an MCP server with authentication, tool permissions, limits,
session ownership, and optional Wasm policy. An authenticated admin API and CLI manage persistent
users, tool controls, and scoped rate rules. OTLP traces time each gateway and policy step. It supports TCP HTTP, HTTP over Unix sockets, a
stdio frontend, and verified HTTPS upstreams.

The [notes example](examples/notes-server/README.md) uses the official MCP SDK and performs real
SQLite writes, reads, and deletes. The integration tests verify persisted results, streaming,
session isolation, denied writes, and the supported transports.

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
pin legacy sessions, and cool down failed backends while retaining reservations through streaming.
See [load balancing](docs/load-balancing.md) for configuration, typed contexts, and measurement.

## Workspace

| Package | Responsibility |
| --- | --- |
| `wayframe` | Public facade with optional adapters |
| `wayframe-core` | `Context`, `Block<C>`, `Execution`, `Decision`, and validated `Plan<C>` |
| `wayframe-runtime` | Pipeline evaluation, deadlines, and atomic `Live<T>` generations |
| `wayframe-http` | Embedded HTTP gateway, streaming, and metadata/body limits |
| `wayframe-blocks` | Bearer tokens, RS256 JWT, origin controls, and local token buckets |
| `wayframe-transport` | Bounded task bridge and Unix/HTTPS policy bridges |
| `wayframe-lb` | Native routing blocks: round robin, least connections, smooth weighted round robin, quota priority |
| `wayframe-mcp` | Bounded MCP inspection and tool permissions |
| `wayframe-wasm` | WIT policy runtime, metadata capabilities, and clean prewarming |
| `wayframe-example-mcp-gateway` | Reference application under `examples/` |

The plugin contract is in [wit/policy.wit](wit/policy.wit), with a guest example under
[plugins/path-control](plugins/path-control/src/lib.rs). Each warmed guest instance is used once
and replenished, so mutable guest state is not reused between requests.

## Development

Use a virtual environment for the verification dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
bash scripts/verify.sh
```

Verification checks Python and Rust formatting, the Python fixture, the native
facade, all workspace features and targets, doctests, Clippy, both fixture gateway modes, and
official SDK integration across the supported frontends.
It uses the committed lockfile and writes local reports under `target/verification/`.
GitHub Actions runs the same checks on Linux and macOS.

For fixture-only checks:

```bash
python3 -m unittest discover -s examples/mcp-server/tests -v
```

## Benchmarks

```bash
bash scripts/benchmark.sh
```

This measures paired direct-versus-gateway MCP calls across TCP, Unix, HTTPS, and stdio paths,
with JSON/SSE responses, native/Wasm controls, varying payloads, and concurrency. It retains raw
samples, p50/p95/p99, throughput, CPU, memory, and failures. Separate measurements cover native
pipelines and task, Unix, HTTPS, and Wasm policy boundaries.

See [performance methodology](docs/performance.md) before interpreting overhead differences.

## Scope

This version supports linear policy pipelines and registered upstream endpoints per HTTP gateway.
Production ingress TLS, connection-level admission, fleet quotas, OIDC/JWKS refresh, plugin
signatures and registries, external-worker supervision, distributed graphs, and complete MCP SDK
conformance are future work. Static demo tokens are for loopback development.

Further reading: [Architecture](docs/architecture.md), [Plugins](docs/plugins.md),
[Roadmap](docs/roadmap.md) and [Contributing](CONTRIBUTING.md).

Licensed under [Apache-2.0](LICENSE).
