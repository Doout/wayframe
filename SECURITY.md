# Security

**Do not deploy this package as an internet-facing or hostile-tenant gateway before completing
build verification and an independent security review. No security certification is claimed.**

Trusted: application/native block code, operator configuration, plugin manifests and artifact
directories, selected upstream/policy endpoints, authentication configuration, runtime/build tools.
Untrusted: client headers/bodies, MCP metadata/tool arguments, guest plugin execution and external
policy responses. Privileged plugin installation and configuration publication are operator tasks.

## Implemented controls

Terminal denial; bounded pipeline identifiers/cardinality; preflight authentication before optional
body collection; separate upstream credentials; exact origin checks in the demo; registered upstream
origins; removal of spoofed identity/hop headers; no automatic redirects/proxies/retries; conservative
path normalization; per-request and stream limits; bounded local token-bucket cardinality; bounded
external frames with correlation and peer validation; narrow Wasm metadata grants; no WASI or
arbitrary host imports; guest fuel/memory/deadline budgets; fresh single-use guest instances.

See [verification instructions](CONTRIBUTING.md#verification) for development checks.

## Deliberate limitations

Native blocks can execute arbitrary process code and must be trusted. A cooperative future timeout
is not a process kill. Host compilation can consume resources and cannot be preempted by dropping
its async caller. A malicious runtime exploit is outside WIT capability isolation. Use distinct,
unprivileged OS processes/containers with owned cgroups, network policy and filesystem isolation
for mutually hostile tenants or untrusted compiler inputs. The stdio supervisor bounds its own child and pending calls; it does not claim total process-tree,
container or hostile-compiler containment.

SHA-256 detects artifact mismatches. Optional Ed25519 trust authenticates the publisher and binds
the manifest limits/grants; remote HTTPS loading requires it. Publisher trust does not grant
additional host capabilities.
The loader admits operator-owned regular files with relative component paths and bounded reads;
it is not a hardened shared writable artifact store. Do not let tenants replace files, directories
or manifests. There is no unsafe native-AOT deserialization cache in this version.

The example supports independent development tokens and optional managed users with hashed
random bearer tokens, rotation/revocation, an authenticated admin API, and local persistent rules.
Management uses a distinct credential, revision preconditions, bounded private files, and one
process per state file; it is a local control plane rather than a distributed identity store. Never commit them. Do not
reuse client tokens for an upstream service, accept client-supplied `x-*` identity headers, or grant
permissions from MCP `clientInfo` or annotations. The reusable identity adapter validates RS256 signatures, issuer/audience/expiry, refreshes bounded
JWKS, and maps only explicitly trusted claims into permissions. Protected-resource metadata and
separate upstream OAuth client credentials are available; the gateway is not an authorization server.

The listener supports TLS/mTLS, connection/HTTP2 admission, handshake/header/I/O deadlines and
bounded draining. Expired request transports are cancelled even when body polling is backpressured.
Native callbacks must cooperate with cancellation; use the CPU pool for blocking work.
The original standard-library Python server is a read-only fixture. The SDK notes server is a
loopback persistence example; its shared database is not partitioned by tenant. Neither example
is a hardened production HTTP service.

Load-balanced endpoints are operator registered; client metadata cannot select arbitrary URLs.
MCP sessions pin their backend and fail closed when unavailable. Dispatch is never retried.
Quota-priority preferences allow overflow, while separate hard backend caps remain enforced.
Active health probes and single half-open admission are local; shared quota stores are not included.

Per-stream byte pacing is not aggregate bandwidth enforcement. Per-process limits multiply across
replicas. Bodies exceeding a limit after response headers are sent terminate the stream. Work with
side effects requires an explicit idempotency/retry policy; the notes example performs actual writes and the gateway retries no upstream tool calls. Request/response credentials and argument payloads are not logged.

## Release gates

Run formatter, compiler, unit and live gateway tests; run guest trap/state-leak tests; pin and scan
dependencies; fuzz HTTP/JSON/WIT boundaries; test streaming cancellation, overload, reload and
worker failure; perform protocol differential tests and security review. Set an actual private
vulnerability-reporting channel before a public release. Until one is configured, avoid posting
exploit details or credentials in public issues.
