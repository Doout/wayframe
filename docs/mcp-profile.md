# MCP interoperability

The core framework is protocol-independent. `wayframe-mcp` adds bounded JSON-RPC inspection and
tool authorization. The configurable gateway is tested against the official Python MCP SDK
2.3.0 as both client and server. Its notes example performs real persisted SQLite operations.
The original standard-library fixture remains useful for malformed-envelope tests.

## Supported profile

| Capability | 2026-07-28 | 2025-11-25 |
| --- | --- | --- |
| Lifecycle | `server/discover`, per-request metadata | `initialize`, `notifications/initialized` |
| Methods | `ping`, `tools/list`, `tools/call` | Same plus cancellation notification |
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
explicit required permission; unknown tools and other operations are denied. `tools/list` exposes
the upstream catalog, including tools the caller cannot execute. The upstream service credential
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

This qualifies the documented tools profile. Resources, prompts, elicitation, sampling, client
responses to server requests, extensions, batch calls, resumable SSE, OAuth discovery, and general
bidirectional server-initiated stdio requests are not implemented by this application. Upstream
servers requiring those capabilities need additional adapters and authorization rules.

The listener binds loopback; public deployment requires ingress TLS and production identity,
connection, and resource controls. See [gateway setup](../examples/mcp-gateway/README.md) and
[security boundaries](../SECURITY.md).

## References

- [Official Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- [MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28)
- [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [2025-11-25 transport and sessions](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
