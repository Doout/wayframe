# Testing and benchmarks

Run commands from the repository root in a fresh shell after [setup](../README.md#setup).
Keep `.venv` active. The fixtures generate their own configuration and credentials.

## Verification

```bash
bash scripts/verify.sh
```

This checks Python/Rust formatting, fixture tests, facade feature combinations, workspace tests,
doctests, Clippy, native/Wasm gateway smoke, live SDK integration, and 10,000 bounded input
mutations. Live tests cover persistence, denials, rates, routing, TLS, streaming cancellation,
reloads, and actual OTLP JSON/protobuf exports. Reports go to `target/verification/`.
CI runs this on Linux and macOS, plus a Linux benchmark smoke test. A fresh build compiles Wasmtime
and can take several minutes.

To build and execute the Rust-authored guest in addition to the checked-in WAT fixture:

```bash
cargo install wasm-tools --version 1.261.0 --locked
bash scripts/build-wasm-guest.sh
python3 scripts/smoke.py --wasm --plugin-manifest target/path-control/plugin.toml
```

For focused library or live-gateway checks:

```bash
cargo test --locked --workspace --all-features --all-targets
cargo build --locked -p wayframe-example-mcp-gateway --features wasm
python3 -m unittest discover -s scripts/tests -v
```

## MCP overhead

```bash
bash scripts/benchmark.sh
```

The suite builds release binaries and pairs direct calls with gateway calls reading the same
persisted SQLite value/revision. It covers TCP, Unix, verified HTTPS, and stdio; JSON/SSE;
native/Wasm controls; payload sizes; and concurrency. Every call checks its correlated result.
Warmup is excluded, direct/gateway order alternates, connections are pooled, and authorization
stays enabled. Stdio runs at concurrency 1 because its frontend processes requests serially.

For a short check with 100 timed calls on each path:

```bash
bash scripts/benchmark.sh --requests 100 --rounds 1 --concurrency 1 \
  --payloads 64 --transports tcp --formats json --native-only --skip-boundaries
```

Outputs default to `target/benchmarks/`. Use a separate `--output` directory for each comparison.
Failures remain in the report and make the command fail.

| Report | Contents |
| --- | --- |
| `mcp-overhead.json`, `.csv` | Environment, binary digest, raw timings, p50/p95/p99, throughput, CPU, RSS, errors |
| `policy-boundaries.json` | Native, task, Unix RPC, HTTPS RPC, and Wasm invocation costs |
| `native-pipeline.json` | Native block batches against an empty plan |
| `graph-scheduling.json` | Graphs against equivalent linear plans |
| `native-load-balancer.json` | Four routing strategies across 2/8/64 backends |

`--skip-boundaries` omits the four microbenchmark reports. Added p50 is the median of paired
per-round gateway-minus-direct p50 differences. Negative differences under contention are noise
or whole-path scheduling effects. Retain raw samples, spread, environment, and configuration.
A short local result does not establish deployment capacity.

CPU is process CPU during the timed interval. RSS is sampled every 50 ms. First-body-byte latency
can include an SSE primer and is not model time-to-first-token. Stdio's comparison includes its
bridge and SDK client cost. Wasm invocation timing excludes clean-instance waits; its elapsed
measurement includes replenishment. Native batch costs exclude networking and MCP work.

## Offered load and tracing

```bash
bash scripts/benchmark.sh --arrival-rate 500 --requests 5000 --rounds 3 --concurrency 32 \
  --transports tcp --formats json --payloads 64 --native-only --skip-boundaries \
  --output target/benchmarks/offered-load
```

At a fixed arrival rate, latency starts at the scheduled arrival, including generator delay.
Generator overload and request failures stay in offered totals. JSON/CSV survive failed rounds.
Without `--arrival-rate`, throughput is closed-loop at the selected concurrency.

Add `--managed --trace-protocol http/protobuf` to include persistent controls and a local OTLP
receiver. Compare the same workload with export disabled and with `--trace-sample-ratio 0`, using
different output directories. Exported step aggregates include setup and warmup; timed latency
samples exclude them. The receiver consumes resources too.
See [trace settings](gateway.md#traces-and-health).

## Graph and routing costs

```bash
mkdir -p target/benchmarks
cargo run --locked --release -p wayframe --no-default-features --features flow \
  --example flow_cost > target/benchmarks/flow-cost.json
cargo run --locked --release -p wayframe --no-default-features --features flow \
  --example flow_delays > target/benchmarks/flow-delays.json
cargo run --locked --release -p wayframe --no-default-features --features lb \
  --example lb_cost > target/benchmarks/lb-cost.json
```

`flow_cost` measures warm scheduling, branch pruning, prepared plans, and adaptive path selection.
`flow_delays` uses real timers to compare stable, critical-path, and learned ordering with limited
parallelism. Reports include starts, waits, and observed costs. `lb_cost` checks selection totals
and final lease release. These isolate local execution costs; use the full gateway workload for
network latency and capacity measurements.

## Dependency cache costs

```bash
cargo test --locked -p wayframe-cache
cargo run --locked --release -p wayframe-cache --example cache_cost
cargo run --locked --release -p wayframe-cache --example dependency_cost
```

The first example measures TTL lookup in 30 batches of 10,000 operations. Its source also
runs against the pre-change cache at `fd5efd8`. Copy it into a separate checkout and use a
separate `CARGO_TARGET_DIR` when comparing binaries. The second measures dependency lookup
through a diamond, invalidating its four nodes, and invalidating a 512-entry fan-out.
Both print raw JSON; CI uploads current results with verification reports. These exclude
remote evidence acquisition and application work. Single invalidation timings are descriptive,
not latency percentiles.
