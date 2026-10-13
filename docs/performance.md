# Performance measurement

Wayframe has repeatable release-build measurements for both end-to-end MCP traffic and individual
execution boundaries. Results are observations from the recorded machine and workload, not a
latency or throughput guarantee.

## Run the suite

```bash
bash scripts/benchmark.sh
```

This builds optimized binaries and runs authenticated MCP reads against the same persisted SQLite
record directly and through the gateway. It covers HTTP over TCP, HTTP over Unix sockets, verified
HTTPS upstreams, and the MCP stdio adapter, with JSON and SSE responses, native controls, and native
controls plus Wasm. Defaults are 1000 requests per round, three rounds, 64/4096-byte stored values,
and concurrency 1/16. Stdio is measured at concurrency 1 because each stdio gateway process handles
one request at a time.

For a shorter run or a focused comparison:

```bash
bash scripts/benchmark.sh --requests 300 --rounds 3 --concurrency 1 8
bash scripts/benchmark.sh --transports tcp --formats json --native-only --skip-boundaries
```

Outputs are under ignored `target/benchmarks/`:

| File | Contents |
| --- | --- |
| `mcp-overhead.json` | Environment, binary digest, workload, per-round metrics, raw timings, and errors |
| `mcp-overhead.csv` | Latency, paired added p50, throughput, gateway CPU, memory, and error counts |
| `policy-boundaries.json` | Native, task, Unix RPC, HTTPS RPC, and Wasm invocation distributions |
| `native-pipeline.json` | Repeated batches of 0/1/4/16/64 blocks, including the empty-plan baseline |

Every timed request validates its correlated MCP result and exact persisted value/revision.
Failures remain in the report, and the command fails when a scenario records errors. Warmup is
excluded. Direct/gateway order alternates between rounds, and connections stay pooled. TLS chain
and hostname verification remain enabled. Authentication, permissions, body inspection, admission,
and rate-limit controls remain enabled; benchmark quotas are explicitly raised to avoid measuring
intentional rate rejection.

## Interpreting results

End-to-end p50/p95/p99 include the client, network, SDK server, SQLite read, and response parsing.
The added p50 is the median of paired per-round gateway-minus-direct p50 differences. Min/max
paired differences are retained to expose run variation. Negative differences under contention
are noise or whole-path scheduling effects; they do not establish a framework speedup.

First response-body-byte timings are recorded for HTTP paths. An SSE primer may be that first
byte; this metric is not model time-to-first-token. Successful requests/sec are closed-loop
throughput at the selected concurrency, not an open-loop capacity or saturation limit.

CPU is process CPU time consumed during the timed interval, including Wasm replenishment where
enabled. RSS is sampled every 50 ms, not an OS high-water mark. Stdio uses the same HTTP upstream
as the direct baseline, but its client transport is different; its difference includes the bridge
and SDK stdio cost. Benchmark and upstream Python CPU can dominate local results at concurrency.

The policy suite isolates scheduling/IPC/sandbox boundaries using an allow decision and fixed
metadata. Its per-invocation timer has measurable cost, especially for the native baseline. Wasm
invocation latency excludes waiting for a clean instance; elapsed time includes replenishment
waits and the report includes pool counters. This is distinct from the fully admitted gateway
measurement, which counts pool misses as request errors.

The native microbenchmark times batches to reduce timer overhead. It reports repeated batch means,
spread, checksum, and added nanoseconds above an empty plan. It excludes HTTP, TLS, MCP, and Wasm.

Run on an otherwise idle machine and retain environment, raw samples, and configuration when
comparing changes. Fuzzing, overload/fault tests, deployment-scale measurements, and independent
security review remain release qualification work.

To measure durable user management and sampled/unsampled OTLP instrumentation, see the
[tracing benchmark](observability.md#verify-and-measure). Exported step timings are wall time
and include exporter/instrumentation effects; compare the same workload and sampling settings.

The [load-balancing benchmark](load-balancing.md#observation-and-verification) isolates native
selection/reservation/release across four algorithms and 2/8/64 backends. The gateway integration
suite separately verifies real routing, affinity, and streaming lifetime; batch cost is not wire latency.
