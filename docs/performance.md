# Performance

No latency or throughput target has been established. Native blocks use direct virtual calls
on one mutable context; immediate decisions avoid boxed futures. HTTP bodies stream unless
bounded inspection is requested. Task, network, JSON, and Wasm boundaries add their own costs.

## Native microbenchmark

```bash
cargo run --locked --release -p wayframe --example pipeline_cost
```

This evaluates 0, 1, 4, 16, and 64 native blocks with reused work items, reporting mean nanoseconds
per evaluation and a checksum. It measures pipeline overhead, excluding HTTP, network, and guest
execution. Repeat runs under equivalent conditions and retain the compiler, configuration, and
raw results. Compare each pipeline with the empty baseline.

## Gateway measurements

Compare native controls, task hops, Unix/HTTPS policy calls, and Wasm calls under equivalent
TLS/auth/body/protocol settings. Measure compile, pre-link, clean instantiation, warm invocation,
and refill separately. Include saturation, pool depletion, denied requests, traps, and overlapping
generations. Record latency percentiles, throughput, CPU, allocations, RSS, queue lengths, and
admission rejections.

The local token bucket uses a bounded mutex-protected map; profile contention before changing its
quota semantics. A single-use Wasm pool must replenish fast enough to sustain admitted calls.
A starting estimate is `warm headroom >= burst + peak arrival rate * refill delay`, subject to
memory and CPU budgets. Validate it under load before choosing operational limits.
