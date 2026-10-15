# Workflows

Use `Pipeline<C>` for ordered controls and the `flow` feature for typed graphs.
A graph node reads immutable inputs and owns its output. Independent async nodes can run
in parallel, then join before a synchronous follow-up.

```text
input -> first async check --+-> join -> sync follow-up
      -> second async check -+
```

## Building a graph

`flow::Builder<C>` provides these operations:

| Operation | Behavior |
| --- | --- |
| `sync`, `asynchronous` | Produce an output from typed inputs |
| `join` | Combine two completed outputs |
| `when`, `when_value` | Guard a node with a boolean or typed selector |
| `choice`, `choice_many` | Rejoin alternatives; exactly one must produce a value |
| `gate`, `gate_async` | Add an unconditional mandatory control |
| `dispatch` | Run protected work after every mandatory gate |

Compilation rejects cycles, foreign ports, reversed phases, oversized graphs, and dispatch
without all required gates. A denial or node failure cancels sibling work and prevents dispatch.
Conditional gates and skipped mandatory gates fail. Quorum helpers apply to ordinary work,
not mandatory controls.

See the [branch tests](../crates/wayframe-flow/tests/branches.rs) for typed selectors and
[delayed-node example](../crates/wayframe/examples/flow_delays.rs) for complete runnable graphs.
`http::workflow::FlowBlock` embeds a graph through explicit context projection and output merging.

## Limits and cancellation

Limits bound active executions, parallel nodes within an execution, and nodes across executions.
Node budgets include scheduler admission and cannot exceed the captured request deadline.
Expired ready nodes never start or poll their action. Late synchronous results cannot start
dependent work.

Async actions must cooperate with cancellation. Dropping a run drops its futures and leases;
the scheduler does not detach them. Keep synchronous callbacks short. `CpuPool` bounds blocking
work, but a running closure keeps its slot after caller timeout until it exits. Queued closures
check cancellation and deadline before starting. `Exclusive<T>` handles single-use stream ownership.
`run_finally` runs one synchronous cleanup on success, failure, or cancellation.
Cleanup cannot override a denial.

## Pruning branches and outputs

A selector may depend on earlier async work. As soon as it resolves, the runtime prunes inactive
branches and their ordinary descendants. Pruned actions create no future and take no execution
permit. Each run owns its facts and values; concurrent runs share only the compiled topology.
This simplifies a graph at runtime. It does not generate machine code.

Guard the first optional producer on each path. An unguarded producer remains required because
it may have effects or other consumers. Guard a nested choice when its entire path is optional;
otherwise skipping all its alternatives leaves an invalid empty choice.

Ordinary nodes remain required by default. Mark a node `discardable` only if its action, errors,
and effects may all be omitted. `Flow::prepare` retains requested output ancestors, selectors,
all unannotated work, and controls. Gates, dispatch, and input cannot be discardable.
`run_plan` executes that prepared selection.

Prepared plans belong to one compiled generation. A 32-entry LRU caches structural masks for
output sets, never identities, request values, or policy decisions. A new compiled generation
rejects old plans and ports. `FlowBlock::with_plan` embeds a prepared selection
without removing mandatory gates.
See the [prepared-plan tests](../crates/wayframe-flow/tests/plans.rs).

## Retaining computation results

`wayframe-cache` provides explicit dependency receipts alongside its scoped TTL cache.
Applications supply resource evidence and decide which computations may be retained; graph
execution does not infer either from traces or automatically retain node outputs.

Use `Cache::retain` with a scoped `Key`, payload size, TTL, parent `Dependency` receipts,
and an `Evidence` map. It returns an `Arc` value and an opaque computation receipt.
`Cache::lookup` requires the same ordered direct parent receipts and freshly obtained evidence.
It checks ancestor receipts and expiry under the cache lock. Changed evidence, replacement,
expiry, eviction, or explicit `invalidate` removes retained descendants. A replacement gets a
new receipt even when its value is unchanged; a late child cannot retain against the old one.
Foreign cache, identity, or generation dependencies fail. The ordinary `get` method cannot return an
entry protected by evidence or parent dependencies.

Keep authorization and the original deadline outside retained values. Evidence is an exact
comparison of application-provided observations, not proof that an external resource is
immutable or that several remote reads form a transaction. Bound evidence acquisition and
check again before publishing results under the application's consistency contract.
Keys, evidence, and receipts count toward the byte budget. See the
[dependency tests](../crates/wayframe-cache/tests/dependencies.rs) and
[cache benchmarks](testing.md#dependency-cache-costs).

## Ordering slow work

`Priority::Stable` preserves creation order among ready nodes. `CriticalPath` uses `cost_hint`
to start the longest remaining path earlier. `Measured` learns successful node durations after
three observations and uses hints before then. Neither mode crosses a dependency. Ready gates
and expiring budgets take precedence over cost estimates.

With two slots and independent jobs of 10, 10, and 80 ms, starting the 80 ms job earlier can
reduce completion from 90 to 80 ms. Costs are estimates; the request deadline still applies.
Priority scanning adds overhead, so keep stable order for tiny graphs unless measurements justify
changing it. [Scheduling tests](../crates/wayframe-flow/tests/priority.rs) cover delayed work and gates.

## Choosing linear or graph execution

`adaptive::Adaptive<C>` prepares both paths for one phase. A trusted selector uses the context
available at that boundary. Earlier linear work can reveal independent checks, select a graph,
and continue synchronously after its join. Each invocation executes one path. Failure never
replays completed work through the other path.

Both paths must implement equivalent controls and declare the same phase. Keep shared mandatory
controls outside the selector. Selectors and classifiers are trusted native code; a panic rejects
the request.

The MCP gateway selects applicable `[[async_policies]]` by exact `operations` and `targets`.
Empty selectors apply to all. Fewer than `async_parallel_threshold` checks run linearly;
the rest use the prepared graph. Applicable policy results join into mandatory gates, and the
gateway rechecks current grants before dispatch.

```toml
async_parallel_threshold = 2
async_max_parallel = 2
async_priority = "measured"

[async_profile]
min_samples = 8
explore_every = 64
improvement_percent = 10
ttl_seconds = 60
capacity = 64
min_budget_ms = 10

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

Without `async_profile`, this selects linear execution for `put_note` and graph execution for
`delete_note`. The optional profile learns costs for equivalent paths by applicable-policy bitmask.
The profile values shown are defaults. It uses successful samples, a switching margin, and occasional
exploration within the remaining budget. Errors, denials, and timeouts do not train a cheaper path.
Operator choices, admission pressure, and small-workload rules still take precedence.

Profiles contain bounded durations and counts, never permission decisions. They reset with a new
generation. `async_max_parallel` supports 1..16 nodes and defaults to 16; `async_priority` defaults
to `stable`. Per-policy `estimated_ms` supplies a hint up to 120000 ms.

Node reports distinguish dependency wait, scheduler wait, and execution time, plus skip reasons
and inline/serial/parallel scheduling. OTLP adds branch decisions, path choices, and pruned counts.
Use the [graph benchmarks](testing.md#graph-and-routing-costs) to measure the tradeoffs.

## Research inputs

The [research note](research.md) records relevant papers, their assumptions, our current
implementation boundary, and proposed experiments for scheduling, preparation, and reuse.
Paper results do not establish this project's correctness, novelty, or performance.
