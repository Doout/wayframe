# Security

**Do not deploy this package as an internet-facing or hostile-tenant gateway before completing
build verification and an independent security review. No security certification is claimed.**

Trusted: application/native block code, operator configuration, plugin manifests and artifact
directories, selected upstream/policy endpoints, authentication configuration, runtime/build tools.
Untrusted: client headers/bodies, MCP metadata/tool arguments, guest plugin execution and external
policy responses. Privileged plugin installation and configuration publication are operator tasks.

## Implemented controls

Terminal denial; bounded pipeline identifiers/cardinality; preflight authentication before optional
body collection; separate upstream credentials; exact origin checks in the demo; fixed upstream
origin; removal of spoofed identity/hop headers; no automatic redirects/proxies/retries; conservative
path normalization; per-request and stream limits; bounded local token-bucket cardinality; bounded
external frames with correlation and peer validation; narrow Wasm metadata grants; no WASI or
arbitrary host imports; guest fuel/memory/deadline budgets; fresh single-use guest instances.

See [verification instructions](CONTRIBUTING.md#verification) for development checks.

## Deliberate limitations

Native blocks can execute arbitrary process code and must be trusted. A cooperative future timeout
is not a process kill. Host compilation can consume resources and cannot be preempted by dropping
its async caller. A malicious runtime exploit is outside WIT capability isolation. Use distinct,
unprivileged OS processes/containers with owned cgroups, network policy and filesystem isolation
for mutually hostile tenants or untrusted compiler inputs. The package does not implement that
supervisor or claim total CPU/RSS/process-tree containment.

SHA-256 plus a trusted manifest detects mismatched artifacts; it does not authenticate the publisher.
The loader admits operator-owned regular files with relative component paths and bounded reads;
it is not a hardened shared writable artifact store. Do not let tenants replace files, directories
or manifests. There is no unsafe native-AOT deserialization cache in this version.

The example supports independent development tokens and optional managed users with hashed
random bearer tokens, rotation/revocation, an authenticated admin API, and local persistent rules.
Management uses a distinct credential, revision preconditions, bounded private files, and one
process per state file; it is not OAuth or a distributed identity store. Never commit them. Do not
reuse client tokens for an upstream service, accept client-supplied `x-*` identity headers, or grant
permissions from MCP `clientInfo` or annotations. JWT support uses one configured RS256 public key
and explicit issuer/audience validation; there is no OIDC discovery or JWKS refresh and JWT claims
do not automatically grant the demo's tool permissions.

The example Axum listener does not impose a pre-header hard socket cap or ingress TLS. Its
in-flight request budget does not replace connection/slow-client controls. Put proper ingress,
OS resource budgets, downstream deadlines and audit controls in place before public deployment.
The original standard-library Python server is a read-only fixture. The SDK notes server is a
loopback persistence example; its shared database is not partitioned by tenant. Neither example
is a hardened production HTTP service.

Per-stream byte pacing is not aggregate bandwidth enforcement. Per-process limits multiply across
replicas. Bodies exceeding a limit after response headers are sent terminate the stream. Work with
side effects requires an explicit idempotency/retry policy; the notes example performs actual writes and the gateway retries no upstream tool calls. Request/response credentials and argument payloads are not logged.

## Release gates

Run formatter, compiler, unit and live gateway tests; run guest trap/state-leak tests; pin and scan
dependencies; fuzz HTTP/JSON/WIT boundaries; test streaming cancellation, overload, reload and
worker failure; perform protocol differential tests and security review. Set an actual private
vulnerability-reporting channel before a public release. Until one is configured, avoid posting
exploit details or credentials in public issues.
