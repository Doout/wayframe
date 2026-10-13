# Sandboxed policy plugins and prewarming

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
signature, and this version has no signed registry or artifact-download API.

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
The demo's `/healthz` is a simple listener liveness check; it is not a pool-readiness SLO endpoint.

## Example

`plugins/path-control/plugin.toml` grants path and operation target only. The guest denies
`/admin`, `/admin/...`, and the `admin_preview` tool. It does not deny `/administer`.

```bash
bash scripts/demo.sh --wasm
```

The reference WAT component is included so the host test needs no guest compiler. Its tiny
allocator is intentionally valid only for a single-use instance with the host's 8192-byte
projected-metadata bound. Do not reuse that guest as a long-lived application worker.

The equivalent Rust guest source is also included. After installing a compatible `wasm-tools`
CLI, build the guest with:

```bash
rustup target add wasm32-unknown-unknown
cargo build --manifest-path plugins/path-control/Cargo.toml --target wasm32-unknown-unknown --release
wasm-tools component new \
  plugins/path-control/target/wasm32-unknown-unknown/release/wayframe_example_path_control.wasm \
  -o plugins/path-control/path-control.component.wasm
```

The verification suite uses the bundled WAT; it does not build or convert the Rust guest.
To activate a built component, change the manifest's `component` field, recompute SHA-256 over
those exact bytes, and review the manifest before loading it.

Runtime references: [typed functions](https://docs.rs/wasmtime/49.0.2/wasmtime/component/struct.TypedFunc.html),
[store limits](https://docs.rs/wasmtime/49.0.2/wasmtime/struct.Store.html), and
[pre-instantiation](https://docs.rs/wasmtime/49.0.2/wasmtime/component/struct.InstancePre.html).
