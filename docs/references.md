# References

Checked against official documentation on 2026-10-13. These sources explain external protocol/API
contracts; the package's own behavior must still pass the compiler and integration gates.

- MCP 2026-07-28 specification: https://modelcontextprotocol.io/specification/2026-07-28
- Streamable HTTP and mirrored-header validation: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http
- Discovery: https://modelcontextprotocol.io/specification/2026-07-28/server/discover
- Versioning: https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning
- Metadata: https://modelcontextprotocol.io/specification/2026-07-28/basic/metadata
- Tools: https://modelcontextprotocol.io/specification/2026-07-28/server/tools
- Authorization: https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization
- Legacy transport: https://modelcontextprotocol.io/specification/2025-11-25/basic/transports
- Wasmtime current TypedFunc documentation (49.0.2 at review): https://docs.rs/wasmtime/49.0.2/wasmtime/component/struct.TypedFunc.html
- Wasmtime Store (fuel/resource controls): https://docs.rs/wasmtime/49.0.2/wasmtime/struct.Store.html
- Wasmtime pre-instantiation: https://docs.rs/wasmtime/49.0.2/wasmtime/component/struct.InstancePre.html
- Axum documentation: https://docs.rs/axum/0.8.9/axum/

A metadata-only WIT policy and a WASI HTTP service use different execution contracts.
Wayframe currently implements the policy contract.
