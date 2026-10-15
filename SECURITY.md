# Security

Wayframe is an alpha. Run [verification](docs/testing.md#verification) and obtain an
independent security review before exposing it on the public internet or to hostile tenants.

## Trust boundaries

Native blocks, configuration, plugin manifests, artifact directories, and selected upstreams
are trusted. Native code can run arbitrary process code. Cooperative cancellation cannot stop
a blocking callback or native compilation. The stdio supervisor bounds its child and pending
calls; it does not contain an entire process tree. Use OS isolation for untrusted native code
or compiler inputs.

Client input, MCP arguments, guest execution, and external policy responses are untrusted.
Denials are terminal. The gateway bounds metadata, bodies, admission, deadlines, and external
frames; strips spoofable identity headers; and uses separate upstream credentials.
It disables automatic redirects, ambient proxies, and tool-call retries.

Wasm receives only granted metadata. It has no WASI, filesystem, network, or arbitrary host
imports. Each invocation consumes a fresh instance under fuel, memory, and deadline budgets.
SHA-256 checks artifact identity; Ed25519 trust verifies its publisher and signed limits.
Publisher trust does not grant capabilities. Artifact directories must remain operator-owned.
Guest memory reservations do not cap total process RSS, and epoch deadlines are approximate.

## Deployment limits

Managed credentials are stored as token digests in a private, exclusively locked state file.
Keep client, admin, and upstream secrets separate. Permissions come from trusted configuration
or verified JWT claims, never client headers, annotations, or MCP client metadata.
The gateway validates RS256 tokens but does not run an authorization server.

Admission, rate buckets, health, sessions, and continuation ownership are process-local.
Quota-priority overflow is a routing preference; use backend hard limits to cap active work.
Pinned sessions fail when their backend is unavailable. A deadline cannot undo a dispatched
side effect. Stream failures after headers terminate the body without changing its status.
Per-stream pacing is not aggregate bandwidth enforcement.

The Python fixture and shared SQLite notes server are development examples. The notes database
has no per-user ownership or tenant partitioning.

## Reporting and release checks

No private vulnerability-reporting channel is configured yet. Do not post credentials or exploit
details in public issues. Configure a private channel before a public release.

Release qualification still needs deployment-scale failure testing, dependency scanning,
coverage-guided fuzzing, protocol differential tests, and independent security review.
