# Performance model and measurement plan

There are **no measured Wayframe latency, throughput or Wasm prewarm claims** in this source alpha.
Choosing Rust or moving preparation off the request path is not a benchmark result.

## Designed fast path

Native blocks communicate by direct virtual calls on one mutable typed context. Immediate decisions
avoid a per-block boxed future. Graph validation happens during plan construction. Channels, RPC,
Wasm imports and JSON parsing are optional boundaries. HTTP bodies stream unless inspection is
explicitly requested; MCP inspection intentionally buffers a small bounded JSON envelope.

The initial native limiter is a mutex-protected token bucket map. It is bounded, but it can contend
at scale. Profile before replacing it with shards/per-worker counters and document the resulting
quota semantics. This source does not promise zero allocations, zero copies, lock-free requests,
exact global quotas or peak packet-processing performance.

## Included microbenchmark

```bash
mkdir -p verification/local
cargo run --release -p wayframe --example pipeline_cost > verification/local/pipeline-cost.json
```

The harness evaluates 0, 1, 4, 16 and 64 native blocks with reused work items. It reports mean
nanoseconds per evaluation and a checksum. This isolates one small overhead dimension; it does
not report p99, HTTP overhead, guest overhead, network latency, or a statistical confidence interval.
Repeat runs on the same otherwise idle pinned CPU cohort and retain compiler/configuration/raw data.
Compare the increase relative to the empty pipeline, not just absolute totals across machines.

## Full benchmark matrix before release

Measure no-op direct baseline, native controls, bounded task hops, Unix/HTTPS policy calls and
Wasm policy calls. Measure cold compile, pre-link, clean instantiate, warm invocation and refill
separately. Include saturation, pool depletion, denied requests, traps and overlapping generations.
Record p50/p95/p99, throughput, CPU/request, bytes/sec, allocations, process RSS, active connections,
queue lengths and admission rejections. Make TLS/auth/body/protocol settings identical for any
comparison to another proxy; Python fixture performance is not a proxy benchmark baseline.

A clean single-use pool must replenish at least as quickly as steady admitted invocations consume
slots. A rough sizing estimate is `warm headroom >= burst + peak arrival rate * refill delay`,
subject to explicit memory and CPU ceilings. Queueing, variance and competing initializations make
this a starting estimate, not an SLO proof. Full reserve, large pools or same-tenant Store reuse are
not substitutes for measurements and isolation tests.

No kernel bypass, io_uring, shared-memory transport or eBPF path is implemented. Add a specialized
adapter only after equivalent-workload measurement shows a benefit worth its maintenance burden.
