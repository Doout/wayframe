# Runtime research

Reviewed 2026-10-15. This is a selected reading list for Wayframe and Waypilot, not a
novelty claim or a reproduction. Paper results below are author-reported. The new
entries were checked against primary abstracts and selected design/evaluation sections;
section references identify the assumptions most relevant to us. Versioned links record
the reading used here; later revisions need another review.

## Direction

The literature supports investigating dependency-aware execution, preparation, and reuse.
Combining these ideas is already explored, especially by TomasuLLM and Speculative Macro
Commit. A useful focus for us is making their correctness conditions explicit for remote
MCP tools, where the gateway may lack snapshots, rollback, or visibility into external
state. That is a design hypothesis to test, not an established research gap.

Keep scheduling, result validity, and permission decisions separate. A learned predictor
can nominate work; it cannot establish independence, authorize execution, or make an
external effect reversible. Start with validity and incremental reuse, then evaluate
bounded speculation against the same resource budget.

## Closest systems

| Paper and reading | Mechanism and evidence limits | Implication for our runtime |
| --- | --- | --- |
| [TomasuLLM: Out-of-Order Speculative Execution for LLM Agents](https://arxiv.org/abs/2609.38201v2), 2026. §§3, 5.4, App. A.1. | Executes drafted calls in copy-on-write sandboxes; validates actions, dependencies, observations, and effects before ordered commit. Reports 1.31× and 1.35× on two coding benchmarks with extra compute. Its prototype has no remote-endpoint wrapper; unversioned or non-idempotent network calls are barriers. | Closest correctness model. Capture dependency/effect evidence, reject stale work, and fall back when validation is unavailable. Its sandbox results do not establish remote MCP correctness or throughput gains. |
| [Speculative Macro Commit for Faster Tool-Using Agents](https://arxiv.org/abs/2609.03236v1), 2026. §§3, 4, 5. | Mines recurring action patterns and commits drafted suffixes after actor confirmation of the first action. Explicitly approximate: skipped actions lack individual actor confirmation. AppWorld completion falls from 70/168 to 68/168. Speculative runs use three GPUs versus one for the sequential baseline. | Close overlap with trace-derived branches plus speculation. Keep exact execution as the baseline; distinguish tool-effect validity from whether a model would have chosen every skipped action. |
| [TVCache: A Stateful Tool-Value Cache for Post-Training LLM Agents](https://arxiv.org/abs/2602.10986v1), 2026. §3, App. B. | Matches tool-history prefixes and reuses results or sandbox snapshots during RL rollouts. Its stateful matching argument assumes outputs depend only on sandbox state and arguments, with correct mutation annotations. | Arguments alone cannot identify mutable state. History matching also needs an external-state contract for remote tools; identical local history cannot detect someone else's write. |
| [Speculative Actions: A Lossless Framework for Faster Agentic Systems](https://arxiv.org/abs/2510.04371v2), 2025/2026. Method and experiments, including §4. | Drafts future actions and reuses matching executions. Despite the title, its OS experiment explicitly uses lossy speculation through provisional configuration updates. Other evaluations use controlled execution settings. | Specify guarantees per tool and environment. Discarding a prediction does not undo a request already sent. Measure prediction misses and wasted calls alongside latency. |

## Graph execution and controls

| Paper and reading | Mechanism and evidence limits | Implication for our runtime |
| --- | --- | --- |
| [An LLM Compiler for Parallel Function Calling](https://arxiv.org/abs/2312.04511v3), 2024. §§3, 4, 5. | Separates a dependency planner, task fetching, and parallel execution, with replanning. Comparisons with sequential ReAct include changes in planning and model calls. | Parallel tool DAGs are established prior work. Compare equivalent plans and outputs to isolate our scheduling overhead; report planning separately. |
| [Sherlock: Reliable and Efficient Agentic Workflow Execution](https://arxiv.org/abs/2511.00330v1), 2025. Runtime design and §7.2. | Selects quality verifiers and executes downstream work during verification. Retention uses ROUGE-L thresholds for instruction/tool tasks; code/math default to full rollback. | Useful for optional quality checks and descendant invalidation. Approximate answer similarity cannot replace exact tool-result validity or mandatory authorization before effects. |
| [AgentSpec: Customizable Runtime Enforcement for Safe and Reliable LLM Agents](https://arxiv.org/abs/2503.18666v3), ICSE 2026. Rule language and runtime semantics. | Defines triggers, predicates, and enforcement actions around agent events. Evaluations cover particular coding, embodied, and driving tasks, not all tool behavior. | Give controls explicit execution boundaries. Test denial and revocation on every optimized path; successful predictions must never remove mandatory checks. |

## Incremental computation foundations

| Paper and reading | Mechanism and evidence limits | Implication for our runtime |
| --- | --- | --- |
| [Build Systems à la Carte](https://www.microsoft.com/en-us/research/wp-content/uploads/2018/03/build-systems-final.pdf), ICFP 2018. §§4, 6.3, 6.4, 6.5. | Separates dependency scheduling from deciding what needs rebuilding. Shows how deep traces plus nondeterministic tasks can combine inconsistent intermediate and final artifacts, a "Frankenbuild." | Separate cost policy from validity. Record immediate dependency versions and tool implementation identity; test diamond graphs and nondeterministic intermediate results before retaining whole branches. |
| [Adapton: Composable, Demand-Driven Incremental Computation](https://plum-umd.github.io/adapton/), PLDI 2014. Demand-driven graph and inner/outer computation model; [artifact](https://www.cs.umd.edu/projects/PL/adapton/). | Repairs demanded computations after changes, with reuse across sharing, swapping, and switching. Separates inner incremental computations from outer observers and mutation. | Recompute only affected demanded work under tracked dependencies. Remote effects need additional contracts; omitted outputs alone do not make an effect safe to omit. |

## Earlier inputs

- [Agent JIT Compilation](https://agent-jit-compilation.github.io/): checked programs,
  retained semantic decisions, and task-dependent scheduling. Its evaluation targets web applications.
- [TraceCompiler](https://arxiv.org/abs/2608.02680): workflow extraction with argument provenance.
  It measures call reduction but claims no net efficiency result.
- [PASTE](https://arxiv.org/abs/2603.18897): speculative tool calls, isolated results, and
  joint scheduling with model serving. External effects still need explicit contracts in our design.
- [SpecBox](https://arxiv.org/abs/2607.23933): sandbox prewarming. Our design takeaway is to
  separate preparation from tool execution.
- [Ghost Tool Calls](https://arxiv.org/abs/2606.02483): speculative calls can disclose intent
  when issued, even if their results are discarded.
- [Salsa](https://github.com/salsa-rs/salsa), an implementation reference: dependency-aware
  reuse of pure query results. Mutable tools need additional validity and effect contracts.

## Current boundary

[Wayframe](workflows.md) provides authored graphs, pruning, prepared structural plans,
and scheduling policies. Prepared plans retain neither results nor authorization.
[Waypilot](https://github.com/Doout/waypilot/blob/main/docs/gateway.md#workflows-reuse-and-prewarming)
provides configured MCP workflows, explicit per-tool TTL reuse, and initialization/catalog
prewarming. It does not yet mine workflows, speculatively invoke tools, validate remote
resource versions, or retain arbitrary whole-branch executions. TTL limits staleness;
it cannot discover an external update. Initialization itself remains observable remotely.

## Tests that should decide the next step

These are proposed local MCP experiments, not completed validation or paper results.

| Question | Experiment | Evidence required |
| --- | --- | --- |
| Can a retained result still be used? | Change a resource outside the gateway while arguments and local history stay identical. Also change schema, implementation revision, grants, and generation. Include a change between validation and publication. | No result outside its declared validity contract is published. Missing or expired evidence causes authoritative execution or rejection. Define the consistency contract before claiming freshness. |
| Does incremental reuse beat per-tool caching? | Use diamonds, nested branches, changed leaves, nondeterministic producers, and changing output demand. Compare full execution, current per-tool reuse, and dependency-aware reuse. | Equivalent outputs and required effects; only affected demanded computations rerun. Report metadata/memory cost and validate intermediate-to-output consistency. |
| When is preparation or speculation worthwhile? | Compare disabled optimization, initialization-only prewarm, and a future explicitly eligible read-only speculative path. Vary tool delay, prediction accuracy, failures, and load. | Report p50/p95/p99, throughput, CPU/memory, upstream calls, wasted work, and remotely observed requests. Enforce a work budget and fall back when gains disappear. Read-only calls still require disclosure permission. |
| Do controls survive every optimization? | Revoke access, expire the original deadline, cancel, fail a verifier, or switch branches while work is queued or running. | No protected dispatch or result release bypasses required checks. Discarded work cannot silently replay effects; cancellation must not imply remote rollback. |

Use deterministic local MCP fixtures first, with no paid model dependency. Run cold and
warm cases against an exact execution baseline, holding resources fixed. Attribute model,
preparation, dependency wait, scheduler wait, execution, validation, and release separately
in traces without recording sensitive payloads. Repeat at the agreed 1,000-client,
500-concurrent-call target. A speedup that disappears under equal resources, stale
dependencies, or incorrect predictions should narrow or stop that optimization.

When adding a source, record its reviewed version, assumptions, and one falsifiable local
test. Update this assessment from measured results; keep implementation and performance
claims in the [testing guide](testing.md).
