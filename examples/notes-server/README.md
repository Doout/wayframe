# Persistent MCP notes server

The official Python MCP SDK serves `put_note`, `get_note`, and `delete_note` over SQLite.
Writes return persisted revisions. `admin_preview` reports stored counts and payload sizes.
Follow the [gateway example](../mcp-gateway/README.md) to run both services.

The server defaults to `127.0.0.1:9001/api/mcp` and requires `WAYFRAME_UPSTREAM_TOKEN`.
Responses use SSE with save progress; `--json` selects JSON. `--stdio` runs a stdio server.
The database survives restart and is shared by authorized users, without tenant partitioning.
Back up `var/` when retaining data.

`/healthz` is liveness. `/policy` is a separate authenticated endpoint for policy RPC benchmarks.
