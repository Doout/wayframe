# Architecture

## The boundary: framework versus application

Wayframe is an embedded library and SDK surface. It does not own a mandatory daemon, management
service, database, listener, product configuration format, or executable called `wayframe`.
`examples/mcp-gateway` owns those application choices. Its gateway is one composition of the libraries.

The kernel uses an application-owned `Context` with a deadline. The type can represent a message,
connection or another work item. HTTP status codes, body types, MCP methods and plugin runtimes
are absent from `wayframe-core`. The core depends only on the Rust standard library.

## Typed native composition

`Block<C>` implementations in one plan all accept the same concrete context type. This prevents
connecting an HTTP-context block directly to an unrelated message-context block at compile time.
It does not provide a full typed graph-port language or automatic protocol conversion.

A native block returns `Execution::Immediate` for synchronous decisions or a boxed, Send deferred
future for I/O. Native blocks are trusted, cooperative code: they must not perform blocking
network calls, long computation or infinite loops on executor threads. No Rust trait can sandbox
a hostile native implementation.

A plan holds `Arc<dyn Block<C>>` objects. Native dispatch therefore has a virtual-call cost; there
is no claim of universal compile-time fusion or zero abstraction overhead. There are no default
channels, per-block spawned tasks or serializations between adjacent native blocks.

Plans support at most 64 blocks. Identifiers are bounded and unique within the plan. Phases move
forward: Admit, Authenticate, Inspect, Authorize, Limit, Dispatch. The HTTP adapter intentionally
has two independently validated plans: a preflight plan before collection and a policy plan after
bounded inspection. Each plan validates its own ordering; the application owns how the two stages
are composed. Authentication and preflight limiting can therefore happen before MCP body parsing.

Denials are terminal. `Continue` is never a final allow override. `Reason` is transport-neutral;
the HTTP adapter maps it to a status. The engine captures the deadline before evaluating a plan
and checks it around native steps and around awaited work. Cooperative native execution can
still overrun a deadline; only explicit isolated execution has an enforceable guest budget.

## Ownership and lifecycle

Construct and validate native blocks before building a plan. Shared state, such as a local token
bucket, is owned by its block. `Arc` owns resources across readers; `Drop` and explicit admission
closure provide the initial lifecycle. There is not yet a generalized plugin event bus or
start/stop dependency graph.

`Live<T>` publishes an application-defined complete deployment as an atomic generation. Prepare
configuration, upstream clients and plugin pools before publication. Capture the current generation
ID, prepare outside the publish lock, then compare-and-publish. A candidate based on a stale
ID is rejected rather than overwriting a newer deployment. A request/stream keeps its old snapshot
until it completes. The sample `live_generation` demonstrates the ownership rule without HTTP.

The HTTP object itself is immutable and held by the streamed response, along with an admission
permit. Old policy objects and resources therefore remain alive during forwarding. The MCP app
has no admin reload endpoint; applications can wire `Live<Arc<HttpGateway>>` to their own
control plane, preserving the generation through the complete response.

## HTTP adapter

The builder requires an explicit preflight pipeline and a fixed upstream origin. It rejects URL
credentials, unsupported schemes, query strings and configured upstream path prefixes. The
application owns routing and listener configuration. Public-facing TLS is not implemented here.

Requests use bounded in-flight admission, deadline, metadata and body budgets. Without inspection,
the adapter streams body chunks. Inspection is opt-in and bounded; MCP uses a 32 KiB envelope.
This is deliberate buffering, not a zero-copy claim. Inspected-body framing is normalized before
forwarding. Body mutation through the inspection context is not a supported transformation API;
the bytes sent upstream remain the inspected original body.

The path policy conservatively rejects ambiguous encodings, encoded separators, double-decoding,
dot segments and control characters. The HTTP adapter forwards the final canonical path in the context. A trusted application dispatch
block can map an approved client path to a fixed upstream path; the MCP application maps `/mcp`
to its configured endpoint. Earlier controls inspect the incoming path and verified operation.
Hop-by-hop headers, client authorization and spoofable forwarding/identity headers are removed.
A separately configured gateway-to-upstream credential can be attached. The HTTP client disables
ambient proxies and redirects. The upstream origin is operator-owned, not a request-selected URL.

Responses remain streaming. Byte limits and optional per-stream pacing apply to both directions.
Request admission is retained until the response is consumed or dropped. A late size/deadline
failure aborts the stream; it cannot change an HTTP status that has already been sent.
The example implements no transparent HTTP upgrades, CONNECT tunnels, retry or replay.
Body trailers are not forwarded by the byte-stream adapter; it is not a gRPC-transparent proxy.
Byte counters measure application payload chunks passed through the adapter, not Ethernet/TCP/TLS
wire usage or every rejected input byte.

## Explicit connection boundaries

| Boundary | v0.1 behavior |
| --- | --- |
| Direct | Native trait call; default path |
| Task | `TaskBridge<C>` for cloneable typed contexts; bounded mailbox and one cooperative worker |
| Unix socket | Length-delimited JSON policy request with peer-UID verification |
| HTTPS | Fixed authenticated policy endpoint; deadlines and bounded responses |
| Wasm | Narrow metadata ABI in an in-process sandbox; no WASI imports |

Task bridges copy/clone context and add scheduling; bounded queues reject overload rather than
waiting without a limit. External policy calls carry only projected metadata, identity, operation,
request ID and remaining deadline—not raw bearer credentials or arbitrary host objects. Responses
must correlate version and request ID. External bridges open one exchange per invocation; connection
multiplexing is a later optimization. They are policy decisions, not arbitrary distributed stream
or message transports. Shared memory, raw TCP/QUIC and cross-language general block contracts are
future adapters, not hidden fallback paths.

## Resource scope

Local request buckets are per gateway process and bounded in key count. Byte pacing is per stream
and direction; multiple streams add together. The HTTP application-level in-flight cap is not a
hard cap on sockets accepted by the example Axum listener. Optional plugin reservations cap ready,
leased and replenishing guest slots across live pools sharing one runtime. These are guest-memory
ceilings, not whole-process RSS caps. Compilation itself requires OS containment for untrusted
artifact admission. See SECURITY.md and the plugin document for exact limits.
