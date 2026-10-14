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
skipped gate, including in an embedded graph that dispatches afterward through its parent pipeline.
A denial or failed node cancels siblings and prevents dispatch.
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
parallel, graph completion waits for all required gates, and current grants are rechecked before dispatch. Live tests
use two HTTPS policy servers and a barrier, then verify overlapping node spans from an actual OTLP
export. `flow_cost` compares warm graph scheduling against the same number of linear no-op blocks.

`when_value` selects branches from any immutable `Eq + Send + Sync` fact, including an enum.
`choice_many` rejoins several alternatives and requires exactly one output. A selector can depend
on earlier async work, so later facts can resolve nested branches during the same invocation.

```rust,ignore
#[derive(PartialEq, Eq)]
enum Route { Read, Write, Admin }
let route = builder.sync("route", Phase::Inspect, &input, |ctx| Ok(classify(ctx)))?;
let read = builder.asynchronous("read", Phase::Inspect, &input, read_request)?;
let write = builder.asynchronous("write", Phase::Inspect, &input, write_request)?;
let admin = builder.asynchronous("admin", Phase::Inspect, &input, admin_request)?;
builder.when_value(&read, &route, Route::Read)?;
builder.when_value(&write, &route, Route::Write)?;
builder.when_value(&admin, &route, Route::Admin)?;
let selected = builder.choice_many("selected", Phase::Inspect, &[read, write, admin])?;
let after = builder.sync("after", Phase::Authorize, &selected, sync_followup)?;
```

## Specializing a graph as facts arrive

Compilation prepares dependency and selector indexes once per graph generation. Execution visits
ready nodes through those indexes. As soon as a selector resolves, its inactive branches and their
ordinary descendants are removed from the executable set, even if other dependencies are still
running. Pruned actions never run, allocate an action future or take a node execution permit.
Choices wait for their alternatives to settle, validate exactly one value, and resume synchronous
or async work afterward. Independent selected work still runs up to the configured parallel limit.

This is partial evaluation of a prepared graph: current facts simplify the remaining work. It
does not generate machine code. Each invocation owns its facts, values and ready set; the compiled
topology can be reused concurrently. Request identities, policy outcomes and branch decisions are
evaluated afresh. Configuration reloads prepare a new topology before publication.

Guard the first optional producer on each path. An unguarded producer remains required work, even
when one of its consumers is pruned, since it might have side effects or other consumers. A nested
choice also needs its parent guard when the whole nested path is optional. Skipping every nested
alternative without that guard is an empty choice and fails. Selector functions and equality
comparisons are trusted native code and must be short; panics fail the invocation.

`NodeReport::skip_reason` distinguishes `BranchNotSelected` from `DependencySkipped`, with zero
scheduler and execution time for pruned nodes. OTLP exports `workflow.branch` selector decisions,
`workflow.node` skip reasons, and `workflow.execution` counts for `workflow.branch_checks` and
`workflow.pruned_nodes`. These measurements describe work saved; spans and reporting still have a
cost. Use the release benchmark to assess that cost for your graph and tracing configuration.

The scheduler adapts to the ready frontier within one invocation. A ready synchronous node executes
inline after acquiring capacity. One async path uses serial scheduling; independent async paths use
parallel scheduling. A join can therefore return directly to synchronous work without allocating an
executor future for each step. `NodeReport::scheduling` and OTLP `node.scheduling` identify `Inline`,
`Serial` or `Parallel`. All modes enforce the same node/global limits, deadlines and mandatory gates.
Late synchronous results are rejected before any dependent work can start.

## Prepared output plans

Ordinary work is retained by default because it can have side effects or required errors. An author
can explicitly mark a node `discardable` when its action, errors and effects may be omitted if no
requested output or retained control needs it. Gates, dispatch and input cannot be marked discardable.
Preparation retains every unannotated node and control, then includes all of their ancestors and all
requested output ancestors, including selectors. Runtime branch decisions still use current facts.

```rust,ignore
builder.discardable(&optional_enrichment)?;
let flow = builder.compile()?;
let prepared = flow.prepare(&[response.output()])?;
let run = flow.run_plan(context, &prepared).await?;
```

`Prepared` can be reused concurrently. A generation-owned 32-entry LRU caches only structural masks
for exact sets of requested outputs. It stores no request values, identities or policy decisions.
Preparing the same output set again is cheap; `plan_statistics` exposes hits, misses and evictions.
Eviction does not invalidate an already prepared plan. A new compiled generation rejects plans and
ports from the previous generation. `run_plan_finally` retains success/failure/cancellation cleanup.
`UnrequestedOutput` reports identify omitted work with zero execution and wait time.

`FlowBlock::with_plan` validates and embeds a prepared plan. The reference gateway removes its unused
policy-join value this way: graph completion already waits for all retained gates. Every applicable
policy result and mandatory gate still runs, and current permissions are checked after the graph.

## Ordering slow work

`Builder::priority(Priority::CriticalPath)` starts ready work on the longest remaining path earlier,
using optional `cost_hint` durations. `Priority::Measured` uses successful node execution timings
after three observations and falls back to hints before that. Observations are bounded by the
compiled node set, contain durations and sample counts, and start fresh on a new generation.
`node_costs` exposes them. No node crosses a dependency. Ready mandatory gates take precedence;
ready node budgets take precedence over cost hints. The original request deadline remains fixed.

With two slots, jobs of 10/10/80 ms started in creation order take 90 ms. Starting the 80 ms job
earlier takes 80 ms. Deterministic delayed-node tests demonstrate an 85 ms deadline expiring for
creation order and passing for critical-path order, then learn the same ordering without hints.
Other tests cover several budgets, an omitted slow node, ready gate denial, and an impossible
required path that still expires. Cost-aware scheduling is optional because scanning priorities and
collecting observations adds overhead to small native graphs. Timings are estimates, not guarantees.

The MCP gateway exposes `async_max_parallel` (1–16, default 16), `async_priority` (`stable`,
`critical_path`, `measured`) and each policy's optional `estimated_ms` hint (at most 120000).
Use a smaller parallel limit to measure ordering under admission pressure. Node spans export
`node.remaining_cost_ns`; execution spans identify `workflow.priority`.

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
speculative policy call happens on the request path. The graph resolves each policy's applicability,
prunes its inactive call branch, and joins the selected result into an unconditional mandatory gate.
An inapplicable policy joins Continue; every applicable policy result must pass its gate.

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
`flow_cost` also reports `branch_cases` for selected and pruned optional chains of 1, 16, 64 and 128
nodes, with seven timing batches and the executed/pruned counts. These are warm native no-op graphs
with tracing disabled, rather than network latency or production capacity estimates.

## Measured choice of linear or graph paths

`Adaptive::with_profile` optionally compares equivalent prepared paths using a trusted `Workload`
classifier with a bounded integer class key and independent width. Success timings feed a rolling
cost estimate. Warmup samples one path per request; after both paths have enough samples, a margin
prevents switching for small timing differences. Occasional exploration can refresh estimates.
Each invocation executes one path. Errors, denials and timeouts remain terminal and do not train a
path as cheaper. Explicit operator rules, admission-pressure choices and small workloads stay
authoritative. Selector/classifier panics reject the request before either path runs.

The default model holds at most 64 classes, requires eight successful samples per path, explores
every 64 eligible calls, uses a 10% improvement margin, and expires stale estimates after 60 seconds.
Exploration requires remaining budget; a conservative bound scales a known parallel timing by the
independent width before trying an unproven serial path. Estimates cannot guarantee completion, so
the shared deadline still caps execution. Eviction/expiry ignores late observations from old class
instances. No identities, request values, branch outcomes or permission decisions are cached.

The gateway enables this with an optional top-level `async_profile` table and classifies by the exact
applicable policy bitmask. `/metrics` exposes bounded class counts, warmup/exploration selections,
budget skips, successes, failures and evictions. Models are local to one prepared generation.

```toml
async_max_parallel = 2
async_priority = "measured"

[async_profile]
min_samples = 8
explore_every = 64
improvement_percent = 10
ttl_seconds = 60
capacity = 64
min_budget_ms = 10
```
