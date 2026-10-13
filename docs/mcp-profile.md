# MCP interoperability

The core framework is protocol-independent. `wayframe-mcp` adds bounded JSON-RPC inspection and
tool authorization. The configurable gateway is tested against the official Python MCP SDK
2.3.0 as both client and server. Its notes example performs real persisted SQLite operations.
The original standard-library fixture remains useful for malformed-envelope tests.

## Supported profile

| Capability | 2026-07-28 | 2025-11-25 |
| --- | --- | --- |
| Lifecycle | `server/discover`, per-request metadata | `initialize`, `notifications/initialized` |
| Methods | Tools, configured resources/prompts, modern subscriptions, owned MRTR/tasks | Configured tools/resources/prompts and cancellation/session transport |
| HTTP transport | POST with JSON or SSE results | POST, session GET and DELETE |
| Sessions | Stateless requests | IDs bound to authenticated tenant/subject |
| HTTP over Unix socket | Supported | Supported |
| MCP stdio adapter | Supported | Owns upstream session for the local client |
| Upstream TLS | Verified public/private CA roots | Same |

Modern requests carry protocol-version and capability metadata. Mirrored method/name headers must
agree with the bounded JSON body. Base64-sentinel names are decoded before comparison. Legacy
requests are also authorized from their body, rather than optional headers. Duplicate JSON keys,
ambiguous headers, excessive envelopes, invalid IDs, and mismatched targets are rejected.

Authenticated identities and permissions come from operator configuration. Each tool has an
explicit required permission; unknown tools and other operations are denied. Discovery/list results are filtered against current grants; denied/unknown tools and resources
are removed. The upstream service credential
is distinct from every client credential, and client authorization/identity headers are stripped
before forwarding. A Wasm continue decision cannot override a native denial.

Legacy sessions use a bounded gateway ownership registry. Another identity cannot POST, GET,
or DELETE using a session it does not own. Upstream expiration and successful DELETE remove the
record. Ownership is local to a gateway process; clients reinitialize after gateway restart.

## Validation

`scripts/verify.sh` runs the real Rust gateway and SDK server/client to check:

- Writes, reads, revisions, and persistence after upstream restart.
- SSE progress followed by the committed structured result, and JSON responses.
- TCP, Unix socket, and stdio frontends; trusted and untrusted HTTPS upstream certificates.
- Native/guest tool denials, client/upstream credential separation, Host/Origin checks, malformed
  envelopes, request-size limits, unknown tools, and cross-identity session reuse.

Resources and prompts require explicit URI/name-to-permission rules. Subscriptions require an
approved notification filter; records are checked against those selections and current grants.
Opaque MRTR request state is hashed and bound to owner, original operation/parameters, backend and
generation. Task IDs are bound to their creator and original permissions; unowned task access and
task listing are denied. Ownership is bounded and process/generation-local.

Modern input-required results pass through; the gateway does not execute client elicitation,
sampling or roots requests itself. Legacy server-initiated stdio callbacks receive method-not-found.
Batch envelopes, arbitrary client responses, transparent retries and resumable SSE replay remain
outside the profile. Generic custom TCP/WebSocket bindings do not claim standard MCP conformance.

The listener may use verified TLS/mTLS, and OIDC/JWKS plus protected-resource metadata are optional.
An upstream OAuth provider uses its own client credentials/resource. It never exchanges the caller's
token. See [transport contracts](transports.md), [reload behavior](reloads.md),
[gateway setup](../examples/mcp-gateway/README.md) and [security boundaries](../SECURITY.md).

## References

- [Official Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28)
- [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [2025-11-25 transport and sessions](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
