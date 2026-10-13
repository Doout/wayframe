# Plugins and lifecycle

The `plugins` feature provides a typed factory registry. A descriptor identifies a kind/version,
settings schema, phase, required capabilities and provided capabilities. Each configuration has
its own instance ID and explicit `after` dependencies. Preparation rejects unknown kinds, duplicate
IDs, cycles, missing capabilities, invalid settings, and phase mismatches before publication.
Startup/readiness is bounded; a failure shuts started instances down in reverse order. `Bundle`
provides introspection, typed exports, a block pipeline, readiness, and explicit drain/shutdown.
Implementations must cancel their owned resources on Drop, including during cancelled preparation.

The reference application registers these native kinds, all at version `0.1`:

| Kind | Settings | Behavior |
| --- | --- | --- |
| `tool-schema` | `tools` map of name to JSON Schema | Compile operator schemas; validate arguments before dispatch; external schema retrieval disabled |
| `response-redaction` | `pointers` array | Replace configured JSON result fields in bounded JSON/SSE records |
| `cors` | one exact `origin` | Browser response/preflight headers, with the gateway's separate Origin allowlist |
| `response-cache` | explicit `tools` allowlist, `ttl_ms` | Successful JSON tool results, identity/permission/route/backend/generation scope, single-flight |
| `wasm-policy` | `manifest`, optional `trusted_keys` and `approved_capabilities` | Prepared sandbox policies; requires the binary's `wasm` feature |

```toml
[[plugins]]
id = "note-cache"
kind = "response-cache"
version = "0.1"
settings = { tools = ["get_note"], ttl_ms = 1000 }

[[plugins]]
id = "secret-filter"
kind = "response-redaction"
version = "0.1"
settings = { pointers = ["/result/structuredContent/secret"] }
```

Only explicitly approved operations should be cached. Tool annotations do not establish idempotency
or authorize caching. Every hit runs current policy/rate checks, and the response ID is rewritten for
the requesting call. Errors, MRTR continuations, task creation, SSE and legacy-session calls bypass
storage. The native cache has entry, byte and in-flight bounds and a generation-scoped lifetime.

Response hooks have explicit header, record and completion stages. JSON/SSE transforms are bounded
and retain cancellation/source-error provenance. Trusted native hooks may inspect bounded request
JSON; authorization headers are excluded from the response context. Native hooks are process code,
while Wasm remains a narrow decision ABI.

The optional `wasm` feature embeds Wasmtime using a narrow Component Model policy ABI.
It does not make every block a WebAssembly component or require HTTP services to be encoded as
policy decisions. An application can embed the framework without compiling a Wasm engine.

## Contract and permissions

`wit/policy.wit` exports `authorize(method, path, subject, tenant, operation, target) -> u16`.
The ABI identifier is `wayframe:gateway/policy@0.1.0`. `0` means continue; 400, 401, 403,
404, 409, 413 and 429 are denials. Every other value, trap or runtime failure becomes unavailable.
Returning 200 cannot override mandatory authentication or native tool permissions.

The operator-reviewed manifest grants individual metadata fields. Non-granted strings are empty.
The SDK exposes no raw headers, bearer tokens, bodies, filesystem, outbound HTTP, clocks,
process handles or arbitrary host calls. Unknown imports fail pre-linking. The manifest itself
must be trusted operator configuration; a publisher requesting a capability does not grant it.
SHA-256 checks artifact identity against that trusted manifest. A digest is **not** a publisher
signature. `load_with_trust` additionally verifies an Ed25519 publisher signature and the operator's
approved grant. `load_remote` requires signature/trust and downloads only bounded artifacts from one
fixed HTTPS origin without redirects or ambient proxies. Unsigned local loading remains an explicit
operator-owned development path.

## Preparation and execution

```text
trusted manifest → bounded artifact read → digest check → compile once
       → typed pre-link → reserve capacity → instantiate clean Stores
       → publish prepared application generation

invocation → try-lease clean Store → project permitted metadata → authorize
       → copy narrow decision to host → destroy Store → bounded replenishment
```

A warm hit does not compile or instantiate on the request path. It still performs dispatch,
metadata projection/copying, fuel accounting and sandbox boundary work. Replenishment consumes
real CPU and memory even though it is scheduled outside the request path. An empty pool returns
503; it never bypasses the control or silently starts cold compilation.

A used Store is **never returned to the clean pool**. Fresh stores are built from shared compiled
code and a prelinked component. This avoids trusting guest `reset()` routines with request/tenant
data. Persistent guest caches/state and arbitrary continuation checkpoints are intentionally absent.

Pool dimensions are bounded by the manifest: 1..64 warm instances; 1..64 MiB memory per guest;
1..10,000,000 fuel; 1..1000 ms call deadlines; 1..5000 ms initialization deadlines. The v0.1 runtime
reserves at most 256 guest slots and 512 MiB of aggregate configured guest memory across its pools.
It supports one guest memory per component and bounds instances/tables. These reservations do not
include all host/runtime stacks, compiled code, buffers or virtual address mappings. Use process
or container budgets for total host resource containment.

Fuel is replenished per invocation; epoch interruption has a 5 ms ticker and is not a precise
hard-real-time millisecond cutoff. Cooperative async fuel yields help the host notice deadlines.
Guest instantiation has its own budget. Host compilation runs under a single semaphore in a
blocking worker; dropping its caller cannot cancel native compilation. Compile only bounded,
admitted artifacts in an appropriate OS sandbox for hostile publishers.

Each consumed slot schedules one replenishment task. At most four initializations per pool run
concurrently, retries are bounded, and persistent refill failure marks the pool degraded.
`Pool::stats()` exposes ready/target, warm hits/misses, execution errors, refills and degradation.
`/healthz` is listener liveness. `/readyz` checks required plugin pools, JWKS and configured backend
health. Metrics include pool readiness/depletion/refill state.

## Example

`plugins/path-control/plugin.toml` grants path and operation target only. The guest denies
`/admin`, `/admin/...`, and the `admin_preview` tool. It does not deny `/administer`.

```bash
bash scripts/demo.sh --wasm
```

The reference WAT component is included so the host test needs no guest compiler. Its tiny
allocator is intentionally valid only for a single-use instance with the host's 8192-byte
projected-metadata bound. Do not reuse that guest as a long-lived application worker.

The equivalent Rust guest is built, converted and executed separately from the portable WAT fixture:

```bash
cargo install wasm-tools --version 1.261.0 --locked
bash scripts/build-wasm-guest.sh
python3 scripts/smoke.py --wasm --plugin-manifest target/path-control/plugin.toml
```

The script uses the guest lockfile and writes its component/digest manifest under ignored `target/`.
Sign `Manifest::signing_bytes()` with Ed25519, attach `signature.key_id` and the hex signature, and
configure the corresponding public key and approved capability set. The signed bytes include the
component digest, ABI, version and every resource limit. Reordering manifest text does not change
that canonical typed serialization. Signature, digest and grant mismatches fail preparation.

Runtime references: [typed functions](https://docs.rs/wasmtime/49.0.2/wasmtime/component/struct.TypedFunc.html),
[store limits](https://docs.rs/wasmtime/49.0.2/wasmtime/struct.Store.html), and
[pre-instantiation](https://docs.rs/wasmtime/49.0.2/wasmtime/component/struct.InstancePre.html).
