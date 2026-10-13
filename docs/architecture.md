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
The optional `wayframe-flow` crate adds typed graph ports, parallel branches and explicit joins.
Protocol conversion remains an application or adapter choice.

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
closure handle direct composition. The optional plugin registry validates factory schemas,
capabilities and dependencies, starts services in dependency order, and rolls failed preparation
back in reverse order. Applications retain ownership of lifecycle events and shutdown.

`Live<T>` publishes an application-defined complete deployment as an atomic generation. Prepare
configuration, upstream clients and plugin pools before publication. Capture the current generation
ID, prepare outside the publish lock, then compare-and-publish. A candidate based on a stale
ID is rejected rather than overwriting a newer deployment. A request/stream keeps its old snapshot
until it completes. The sample `live_generation` demonstrates the ownership rule without HTTP.

The HTTP object itself is immutable and held by the streamed response, along with an admission
permit. Old policy objects and resources therefore remain alive during forwarding. The MCP app
exposes a scoped admin reload endpoint using `Live<Deployment>`, preserving the generation through
the complete response. Other applications can attach their own control plane.

## HTTP adapter

The builder requires an explicit preflight pipeline and a fixed default upstream origin. The optional
LB adapter registers full endpoints and per-backend credentials/trust, selected by a routing block.
URL credentials, unsupported schemes, query strings, fragments, and ambiguous endpoint paths are
rejected. The application owns routing and listener configuration. TLS/mTLS and bounded listeners are supplied by the opt-in listener crate.

Requests use bounded in-flight admission, deadline, metadata and body budgets. Without inspection,
the adapter streams body chunks. Inspection is opt-in and bounded; MCP uses a 32 KiB envelope.
This is deliberate buffering, not a zero-copy claim. Inspected-body framing is normalized before
forwarding. Body mutation through the inspection context is not a supported transformation API;
the bytes sent upstream remain the inspected original body.

The path policy conservatively rejects ambiguous encodings, encoded separators, double-decoding,
dot segments and control characters. The default forwarding path uses the final canonical path in the context. Registered LB endpoints
use their fixed configured paths. A trusted application dispatch block can map an approved client
path to a fixed upstream path; the MCP application maps `/mcp`
to its configured endpoint. Earlier controls inspect the incoming path and verified operation.
Hop-by-hop headers, client authorization and spoofable forwarding/identity headers are removed.
A separately configured gateway-to-upstream credential can be attached. The HTTP client disables
ambient proxies and redirects. The upstream origin is operator-owned, not a request-selected URL.

Responses remain streaming. Byte limits and optional per-stream pacing apply to both directions.
Request admission is retained until the response is consumed or dropped. A late size/deadline
failure aborts the stream; it cannot change an HTTP status that has already been sent.
The example implements no transparent HTTP upgrades, CONNECT tunnels, retry or replay.
HTTP/2 body frames and trailers survive forwarding, including gRPC status; no protobuf decoding
is implied. The built-in listener normalizes HTTP/2 pseudo-headers and cancels expired connections
so unpolled bodies cannot retain request admission.
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
or message transports. The transport crate also provides supervised stdio and bounded custom WebSocket/framed TCP
bindings. Shared memory, QUIC and a universal cross-language block ABI remain separate adapters.

## Resource scope

Local request buckets are per gateway process and bounded in key count. Byte pacing is per stream
and direction; multiple streams add together. The built-in listener also bounds accepted sockets, HTTP/2 streams, header/TLS deadlines and
stalled I/O. Optional plugin reservations cap ready,
leased and replenishing guest slots across live pools sharing one runtime. These are guest-memory
ceilings, not whole-process RSS caps. Compilation itself requires OS containment for untrusted
artifact admission. See SECURITY.md and the plugin document for exact limits.


## Graphs and prepared services

`wayframe-flow` owns typed DAG execution and explicit joins/choices. Immutable branch inputs and
owned outputs avoid racing updates to an HTTP request. Mandatory gates dominate dispatch; failure
cancels siblings. A graph's node/execution/CPU limits remain independent of HTTP listener limits.

`wayframe-plugins` owns settings/dependency/lifecycle validation. `wayframe-management` owns durable
local users/rules and ephemeral session/continuation ownership. Routing, admission, probes, identity,
cache and OTLP services are optional crates. The reference deployment builder prepares them before
publishing a `Live<Deployment>`; response bodies retain their generation through disposal.
