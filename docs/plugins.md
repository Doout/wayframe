# Blocks and plugins

## Native blocks

Implement `Context` for an application-owned work item and `Block<C>` for each control.
The [typed-message example](../crates/wayframe/examples/typed_message.rs) is a complete,
runnable implementation. `Pipeline::compile` accepts shared blocks, validates unique IDs
and phase order, and supports up to 64 blocks.

Phases run in order: Admit, Authenticate, Inspect, Authorize, Limit, Dispatch.
`Execution::Immediate` returns a synchronous decision; `Execution::Deferred` runs async work.
A denial stops evaluation. `Continue` cannot override an earlier denial. The runtime captures
the request deadline before evaluation, so a block cannot extend it.

One evaluation owns its mutable context. Blocks can serve concurrent evaluations, so synchronize
their shared state. Native code must yield and cooperate with cancellation. Use the workflow
[CPU pool](workflows.md#limits-and-cancellation) for blocking work.

HTTP blocks use `http::RequestContext`. Trusted authentication supplies identity and permissions;
protocol inspection supplies the operation and target. An inspected body is bounded input,
not a general streaming transformation API. Native blocks compile into the application;
the framework does not load arbitrary native shared libraries.

## Plugin factories

The `plugins` feature adds factories with versioned kinds, settings schemas, capabilities,
phases, and explicit `after` dependencies. Preparation rejects unknown kinds, duplicate IDs,
cycles, invalid settings, and missing capabilities. Services start in dependency order;
failed preparation shuts them down in reverse order. Implementations must also release owned
resources on drop, including cancelled preparation.

The reference gateway registers these kinds at version `0.1`:

| Kind | Settings and behavior |
| --- | --- |
| `tool-schema` | `tools` maps names to JSON Schemas; remote schema retrieval is disabled |
| `response-redaction` | `pointers` selects fields to replace in bounded JSON/SSE records |
| `cors` | `origin` sets one exact browser origin, alongside the gateway Origin allowlist |
| `response-cache` | `tools` explicitly allows caching; `ttl_ms` sets expiry |
| `wasm-policy` | `manifest`, optional `trusted_keys` and `approved_capabilities`; requires `wasm` |

```toml
[[plugins]]
id = "note-cache"
kind = "response-cache"
version = "0.1"
settings = { tools = ["get_note"], ttl_ms = 1000 }
```

Cache entries include identity, permissions, route, backend, and generation in their scope.
Hits still run current authorization and rate checks and receive the caller's request ID.
Errors, continuations, task creation, SSE, and legacy sessions bypass storage. Tool annotations
do not establish that an operation is safe to cache.

Native response hooks run at header, record, and completion stages. Transforms retain byte
limits, cancellation, and source errors. See the [factory API](../crates/wayframe-plugins/src/lib.rs)
and [built-in policies](../crates/wayframe-policies/src/lib.rs) to add a kind.

## Wasm policies

The optional `wasm` feature uses the [WIT contract](../wit/policy.wit),
`wayframe:gateway/policy@0.1.0`. `authorize` receives granted method, path, identity,
operation, and target strings. Unapproved fields are empty. Return `0` to continue or
400, 401, 403, 404, 409, 413, or 429 to deny. Unknown results, traps, and runtime failures fail closed.
A guest cannot override a native denial.

The operator approves the manifest's metadata grants and limits. Local loading checks the
artifact digest; `load_with_trust` also checks an Ed25519 publisher signature. `load_remote`
requires signed trust and a fixed HTTPS origin. Signed data includes the digest, ABI,
version, grants, and resource limits. See [security](../SECURITY.md) for containment limits.

Compilation and clean-instance preparation happen before publication. Each call consumes
one clean instance, destroys it, and schedules bounded replenishment. A used instance never
returns to the pool. An empty pool returns unavailable instead of compiling on demand or
bypassing the policy. `Pool::stats()` reports ready slots, misses, refills, and degradation.

Manifest limits allow 1..64 warm instances, 1..64 MiB per guest, up to 10 million fuel,
1..1000 ms call budgets, and 1..5000 ms initialization budgets. A runtime reserves at most
256 guest slots and 512 MiB of configured guest memory across pools. Host compilation has
one blocking worker and cannot be cancelled once running.

The [path-control guest](../plugins/path-control/src/lib.rs) denies admin paths and the
`admin_preview` tool. Its checked-in WAT fixture uses single-use instances. Run
`bash scripts/demo.sh --wasm`, or [build the Rust guest](testing.md#verification).
