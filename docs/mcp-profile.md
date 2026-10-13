# MCP showcase: deliberately scoped interoperability

The core framework is protocol-independent. `wayframe-mcp` is an optional inspection/policy
adapter, and `examples/mcp-server/server.py` is a small executable fixture, **not a full MCP SDK**.
The current official specification inspected for this package is dated **2026-07-28**.
The demo also implements a stateless subset of **2025-11-25** for legacy clients.

## Supported profile

| Capability | 2026-07-28 | 2025-11-25 compatibility |
| --- | --- | --- |
| HTTP endpoint | POST `/mcp` | POST `/mcp` |
| Discovery/lifecycle | `server/discover`; per-request metadata | `initialize`, then `notifications/initialized` |
| Operations | `ping`, `tools/list`, `tools/call` | Same |
| Responses | JSON and request-scoped SSE progress + final result | JSON and request-scoped SSE |
| State | No protocol session | No session ID is assigned |
| GET stream / DELETE session | 405 | 405; neither feature used |
| Tool inventory | Three bundled, read-only tools | Same |

Modern requests send `MCP-Protocol-Version`, `Mcp-Method` and, for `tools/call`, `Mcp-Name`.
The body carries `params._meta.io.modelcontextprotocol/protocolVersion` and client capabilities.
The gateway parses the bounded body and compares the mirrored values before extracting an
operation. Base64-sentinel names are decoded first. A header declaring `add` cannot hide a body
calling `admin_preview`. Legacy policy reads the body too; it never authorizes solely on optional
legacy headers.

Modern successful results include `resultType: complete` and server identity metadata. The
upstream emits protocol JSON-RPC errors where applicable; gateway policy failures may be plain
HTTP error JSON, as permitted for intermediaries. Protocol-level error details are not implemented
in the generic core `Decision` type.

The example client supports these built-in tools only. It is not a complete schema-driven MCP
client and does not implement arbitrary `x-mcp-header` tool annotations or OAuth negotiation.
The listed tools do not declare such annotations.

## Tools and permissions

`add` accepts two bounded numbers. `search_docs` searches five included snippets, not a directory
or remote URL. `admin_preview` returns a simulated maintenance description and makes no change.
All three are read-only; tool annotations are descriptive hints, not access-control authority.

The gateway's authenticated identities grant `tools:read` and optionally `tools:admin`.
`tools/list` exposes the complete demonstration catalog; denied tools are not hidden. All
execution permission checks are enforced on `tools/call`. The upstream service token authorizes
the gateway to the fixture; it is different from either client token. The fixture is not a separate
per-user authorization engine and must not be exposed as an unauthenticated bypass.

The optional Wasm plugin is defense in depth: it denies `admin_preview` for everyone, including
an operator. The native permission check still runs first and cannot be overridden by the guest.

## Bounds and exclusions

The fixture limits envelopes to 32 KiB, JSON depth to 64, JSON nodes to 4096, metadata to 16 KiB,
and request IDs/target lengths. Duplicate keys and duplicate security metadata headers are rejected.
The Python fixture accepts bounded Content-Length requests and closes each response connection.
The gateway normalizes inspected request framing, including originally chunked bodies. These are
intentional demonstration choices, not maximum-throughput claims.

No batch calls, prompts, resources, sampling, elicitation, roots, subscriptions, notifications from
extensions, resumable SSE, protocol session management, full MCP OAuth, SSE GET endpoint,
transparent uploads, tool retries, or arbitrary server-side actions are provided. One request-scoped
progress event is emitted when a tool call includes `_meta.progressToken`; the work itself is small
and immediate. The client buffers at most 1 MiB for its display parser, while the Rust gateway
streams response chunks.

The example binds loopback, checks exact Host and present Origin, and uses development bearer
credentials. It is not ready to be configured as a public remote MCP endpoint in an arbitrary AI
client. Public deployments need production ingress, an appropriate identity provider, protected
resource discovery/authorization, credential audience checks and host resource hardening.

## References

- [MCP 2026-07-28 specification](https://modelcontextprotocol.io/specification/2026-07-28)
- [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [Discovery](https://modelcontextprotocol.io/specification/2026-07-28/server/discover)
- [Versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning)
- [Metadata](https://modelcontextprotocol.io/specification/2026-07-28/basic/metadata)
- [Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
- [Authorization](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization)
- [2025-11-25 transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)

Fixture tests cover the supported profile. Independent SDK interoperability remains unverified.
