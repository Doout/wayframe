# Transport adapters

The policy engine is typed and independent of HTTP. The reference application chooses these adapters:

| Binding | Configuration | Contract |
| --- | --- | --- |
| HTTP/1+HTTP/2 | Default listener | Streamable HTTP, bounded headers/body/connection admission |
| TLS/mTLS | `WAYFRAME_TLS_CERT`, `WAYFRAME_TLS_KEY`, optional `WAYFRAME_TLS_CLIENT_CA` | Verified ingress TLS; mTLS certificate fingerprint in a trusted request extension |
| HTTP over Unix | `WAYFRAME_UNIX_SOCKET` | Absolute unused socket path, mode 0600 |
| stdio frontend | `WAYFRAME_TRANSPORT=stdio`, `WAYFRAME_LOCAL_TOKEN` | Standard JSON lines, one configured local identity, clean stdout |
| WebSocket | `GET /ws` with bearer authentication | Custom JSON message binding, bounded frames and I/O deadlines |
| framed TCP | `WAYFRAME_TRANSPORT=tcp` | Custom 4-byte big-endian length binding; loopback/TLS tunnel only |
| HTTP(S) upstream | `upstream` or registered backends | Fixed destinations, separate service credentials, verified CA, no redirects/proxies |
| stdio upstream | `[stdio_upstream]` | Supervised operator-selected process, correlated JSON lines, bounded pending work |

The built-in listener cancels a connection at an admitted request deadline, including when an
HTTP/2 body is unpolled behind a full flow-control window. Other streams on that connection may
need to reconnect. Normal completed requests cancel their deadline timer.

HTTP/2/gRPC request and response trailers are preserved by the generic HTTP library. `upstream_http2`
and backend `http2` enable HTTP/2 prior knowledge when needed; HTTPS also negotiates HTTP/2 through
ALPN. The MCP example validates MCP messages; applications can embed the generic adapter for other
protocols. It does not decode protobuf or impose a gRPC application schema.

JSON/SSE record filters request identity encoding and reject an encoded upstream representation
that cannot be inspected within their budgets. Transformed bodies remove stale validators/range
metadata, preserve safe trailers, and are checked again against output limits.

WebSocket messages contain a JSON-RPC message; connection authentication is rechecked for each one.
Framed TCP messages contain `{"authorization":"Bearer ...","message":{...}}` and authenticate per
message. Both custom bindings return `{"status":200,"session":null,"messages":[...]}`. Responses are
bounded collections; use standard Streamable HTTP for long-lived SSE/subscriptions. They are optional
Wayframe bindings, not additional standard MCP transports. Standard transports are documented in the
[MCP transport specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports).

A supervised upstream uses an absolute executable and argument array, never a shell. Its environment
starts empty. Literal `environment` entries and explicitly named `inherit_env` values are the only
inputs. Keep inherited service credentials separate from client/admin credentials. Example:

```toml
[stdio_upstream]
command = "/absolute/path/to/.venv/bin/python3"
args = ["/absolute/path/to/wayframe/examples/notes-server/server.py", "--stdio", "--database", "/private/notes.sqlite3"]
inherit_env = ["WAYFRAME_UPSTREAM_TOKEN"]
max_in_flight = 8
max_message_bytes = 1048576
```

HTTP clients use a private authenticated loopback bridge to this process. Request IDs, progress tokens
and subscription IDs are correlated and restored. A disconnect/deadline terminates the process if its
work cannot be safely cancelled, preventing hidden work from escaping its concurrency budget. Existing
pending calls fail; reload prepares a replacement. Calls are never replayed. Legacy server callbacks
receive method-not-found; modern MRTR results are handled as ordinary owned responses.
