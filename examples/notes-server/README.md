# Persistent MCP notes server

This example uses the official Python MCP SDK. `put_note`, `get_note`, and `delete_note` perform
real SQLite operations with parameterized queries. Writes return a persisted revision; the database
survives process restart. `admin_preview` reads stored record counts and payload sizes.

```bash
python3 -m pip install -r requirements-dev.txt
source .env
python3 examples/notes-server/server.py --database var/notes.sqlite3
```

`var/` is ignored by Git and kept outside Cargo's build outputs, so `cargo clean` does not remove
the database. Back up that directory when retaining example data.

It binds `127.0.0.1:9001`, serves `/api/mcp`, and requires `WAYFRAME_UPSTREAM_TOKEN`.
The gateway uses a separate service credential; clients authenticate to the gateway with their own
configured tokens. The notes database is shared by the example's authorized users; the example
does not implement per-user note ownership or tenant-partitioned storage.

The default responses use SSE, including progress while saving a note. Use `--json` for JSON
responses. Port, endpoint path, and certificate/key paths are configurable for integration tests.
`/healthz` is a local liveness probe. `/policy` is a minimal authenticated policy endpoint used to
measure the framework's HTTPS RPC boundary; it is separate from MCP tool execution.

Follow [gateway setup](../mcp-gateway/README.md) for permissions and transport options.
