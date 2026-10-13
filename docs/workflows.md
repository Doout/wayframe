# Async workflows

Enable the `flow` feature for a typed DAG while keeping `Pipeline<C>` for inexpensive linear policy.
Each node reads immutable typed inputs and returns its own output. It never races to merge mutable
request contexts. `join` names the merge; `choice` requires exactly one active input. `when` adds
conditional execution, and skipped dependencies propagate until an explicit choice.

```rust,ignore
use wayframe::{flow::Builder, prelude::*};
let mut builder = Builder::<MyContext>::new();
let input = builder.input();
let first = builder.asynchronous("first", Phase::Authorize, &input, |ctx| async move {
    fetch_first(&ctx).await
})?;
let second = builder.asynchronous("second", Phase::Authorize, &input, |ctx| async move {
    fetch_second(&ctx).await
})?;
let joined = builder.join("join", Phase::Authorize, &first, &second, |a, b| {
    Ok(merge(&a, &b))
})?;
let after = builder.sync("followup", Phase::Limit, &joined, |result| {
    Ok(sync_followup(&result))
})?;
let flow = builder.compile()?;
let run = flow.run(context).await?;
let value = run.get(&after);
```

Use `gate`/`gate_async` for mandatory policy decisions and `dispatch` for protected work. Compilation
rejects cycles, foreign ports, reversed phases, oversized graphs, and dispatch nodes without every
mandatory gate as a dependency. Gates cannot be conditional. Runtime validation also rejects a
skipped gate at a conditional join. A denial or failed node cancels siblings and prevents dispatch.
A quorum helper is available for ordinary independent work; it cannot replace mandatory gates.

The default limits bound executions, concurrent nodes per execution, and concurrent nodes across
all executions of a flow. A node budget includes scheduler admission and is capped by the request
deadline. Reports and OTEL spans distinguish dependency wait, scheduler wait and execution time.

Async nodes must cooperate with cancellation. Dropping an execution drops its futures and resource
leases; no work is detached by the scheduler. `run_finally` runs one synchronous cleanup callback on
success, failure or cancellation, without replacing a denial. Keep sync nodes short. Use `CpuPool`
for blocking/CPU work: running closures retain capacity after caller timeout until the work exits.
Queued CPU closures are cancelled when their caller exits and check the deadline before starting.
`Exclusive<T>` provides single-use ownership for a body, frame stream or other exclusive resource.

`wayframe-http::workflow::FlowBlock` connects a graph through an explicit immutable projection and
merge function. The MCP example uses this for the graph path of `[[async_policies]]`: applicable checks run in
parallel, a sync join waits for them, and current grants are rechecked before dispatch. Live tests
use two HTTPS policy servers and a barrier, then verify overlapping node spans from an actual OTLP
export. `flow_cost` compares warm graph scheduling against the same number of linear no-op blocks.

## Runtime path selection

`wayframe::adaptive::Adaptive<C>` prepares a linear plan and a graph adapter for one phase. A short,
trusted selector chooses a path when execution reaches that boundary, using the context produced
by earlier steps. A linear inspection stage can reveal independent work and select the graph;
after its join, evaluation continues with the next synchronous step. Several boundaries can make
separate choices as more information becomes available.

Both paths must implement the same required controls, with shared mandatory gates outside the
selector. Paths must declare the same phase; mismatches fail preparation. Selection, execution and
all child work retain the request deadline. A denial, timeout or overload is terminal; completed
operations are never replayed to try the other path. Native selectors and adapters remain trusted.

The reference gateway selects per inspected MCP operation. An external policy's optional
`operations` and `targets` are exact selectors; empty lists apply to every operation/target.
`async_parallel_threshold` defaults to 2. Below the threshold, applicable policies execute linearly;
at or above it, the same policy checks execute through the prepared graph. No graph compilation or
speculative policy call happens on the request path. Inactive graph gates return Continue, while
every applicable gate is mandatory.

```toml
async_parallel_threshold = 2

[[async_policies]]
id = "risk"
url = "https://policy.example/risk"
token_env = "RISK_POLICY_TOKEN"
operations = ["tools/call"]
targets = ["put_note", "delete_note"]

[[async_policies]]
id = "approval"
url = "https://policy.example/approval"
token_env = "APPROVAL_POLICY_TOKEN"
targets = ["delete_note"]
```

Here `put_note` selects the linear path and `delete_note` selects the graph. Authorization still
runs before this boundary and current grants are checked again afterward. OTLP `workflow.select`
and `workflow.path` spans record `execution.path` and `selection.reason`; graph nodes retain their
dependency/admission/execution timings. `/metrics` reports selection counts per generation.

Use measured workload costs to choose thresholds or an application selector. Tiny native operations
often cost more to schedule than to run sequentially. Live barrier tests prove both runtime choices,
parallel overlap and terminal denials; microbenchmarks measure selection overhead separately.
