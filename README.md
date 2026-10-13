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

## MCP demonstration

The example gateway fronts a read-only Python MCP server with authentication, request limits,
envelope validation, and tool permissions. It needs no model API key or paid service.

```bash
bash scripts/demo.sh
bash scripts/demo.sh --wasm
```

Both commands build the Rust gateway, start it and the Python upstream on local ephemeral ports,
exercise modern and legacy MCP requests, and stop both processes. The harness checks independent
client/upstream credentials, tool access, mirrored headers, SSE responses, and rate limiting.
The Wasm mode also checks the sample policy's denial of `admin_preview`.

```text
MCP client
   │ client bearer token
   ▼
Rust gateway
   Origin → Authentication → Request limit
   → MCP validation → Tool permissions → Optional Wasm policy
   │ separate upstream credential
   ▼
Read-only Python MCP server
   add · search_docs · admin_preview
```

Readers can call `add` and `search_docs`; operators can also call the harmless `admin_preview`.
With the plugin enabled, `admin_preview` is denied to everyone. A plugin's continue decision never
bypasses another control. See the [gateway example](examples/mcp-gateway/README.md) for interactive
setup and the [MCP profile](docs/mcp-profile.md) for protocol scope.

## Workspace

| Package | Responsibility |
| --- | --- |
| `wayframe` | Public facade with optional adapters |
| `wayframe-core` | `Context`, `Block<C>`, `Execution`, `Decision`, and validated `Plan<C>` |
| `wayframe-runtime` | Pipeline evaluation, deadlines, and atomic `Live<T>` generations |
| `wayframe-http` | Embedded HTTP gateway, streaming, and metadata/body limits |
| `wayframe-blocks` | Bearer tokens, RS256 JWT, origin controls, and local token buckets |
| `wayframe-transport` | Bounded task bridge and Unix/HTTPS policy bridges |
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
facade, all workspace features and targets, doctests, Clippy, and both live gateway modes.
It uses the committed lockfile and writes local reports under `target/verification/`.
GitHub Actions runs the same checks on Linux and macOS.

For fixture-only checks:

```bash
python3 -m unittest discover -s examples/mcp-server/tests -v
```

A native microbenchmark is included for local measurements:

```bash
cargo run --locked --release -p wayframe --example pipeline_cost
```

No latency or throughput target is claimed. See [performance notes](docs/performance.md).

## Scope

This version supports linear policy pipelines and a fixed upstream origin per HTTP gateway.
Production ingress TLS, connection-level admission, fleet quotas, OIDC/JWKS refresh, plugin
signatures and registries, external-worker supervision, distributed graphs, and complete MCP SDK
conformance are future work. Static demo tokens are for loopback development.

Further reading: [Architecture](docs/architecture.md), [Plugins](docs/plugins.md),
[Roadmap](docs/roadmap.md) and [Contributing](CONTRIBUTING.md).

Licensed under [Apache-2.0](LICENSE).
