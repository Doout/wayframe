# Wayframe

Rust libraries for building gateways from typed blocks, linear pipelines, and async graphs.
Applications choose their protocols, listeners, controls, and storage. `wayframe-core` depends
only on the standard library; adapters are optional.

The [MCP gateway](examples/mcp-gateway/README.md) is a runnable reference application with
users, tool permissions, rate limits, load balancing, plugins, and OpenTelemetry traces.
Wayframe is an alpha, and its crates are not yet published.

## Setup

Use macOS or Linux with Git, rustup, Python 3.11+, OpenSSL, and C/C++ build tools.
On macOS, install Xcode Command Line Tools. The repository pins Rust 1.99.0, rustfmt,
and Clippy in [rust-toolchain.toml](rust-toolchain.toml).

```bash
git clone https://github.com/Doout/wayframe.git
cd wayframe
rustup show active-toolchain
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
```

Keep the virtual environment active for examples, tests, and benchmarks. Python is not
required by the Rust libraries.

## Try it

```bash
cargo run --locked -p wayframe --example typed_message
bash scripts/demo.sh
```

The first command runs a native block without networking. The second builds the gateway,
starts a local MCP fixture, checks allowed and denied calls, and stops both processes.
Add `--wasm` to the demo command to run a sandboxed policy. No model API key is needed.

For persisted MCP writes, follow the [notes gateway example](examples/mcp-gateway/README.md).
To embed Wayframe in a sibling Rust project, select the adapters you need:

```toml
[dependencies]
wayframe = { path = "../wayframe/crates/wayframe", features = ["http", "controls"] }
```

The facade has no default optional features. See its [feature definitions](crates/wayframe/Cargo.toml)
and the [typed block example](crates/wayframe/examples/typed_message.rs).

## Test and benchmark

```bash
bash scripts/verify.sh
bash scripts/benchmark.sh --requests 100 --rounds 1 --concurrency 1 \
  --payloads 64 --transports tcp --formats json --native-only --skip-boundaries
```

Verification covers the libraries and live gateway on Linux and macOS. The short benchmark
compares direct MCP calls with calls through the gateway. Reports go under `target/`.
See [testing and benchmarks](docs/testing.md) for the full suite and measurement limits.

## Guides

| Guide | Contents |
| --- | --- |
| [Blocks and plugins](docs/plugins.md) | Native blocks, plugin factories, response hooks, Wasm |
| [Workflows](docs/workflows.md) | Parallel branches, joins, pruning, measured scheduling |
| [Gateway](docs/gateway.md) | Transports, users, rates, routing, tracing, reloads |
| [Testing](docs/testing.md) | Verification, overhead comparisons, offered load |

Workflows, rate buckets, health state, and session ownership are local to one process.
Wayframe does not provide durable distributed jobs or shared quotas. Native blocks are
trusted code, and tool calls are never replayed automatically.

[Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Apache-2.0](LICENSE)
