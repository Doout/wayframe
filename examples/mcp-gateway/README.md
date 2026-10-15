# MCP gateway example

Run all commands from the repository root after [setup](../../README.md#setup).
The gateway fronts an MCP server with users, permissions, rate limits, routing, and tracing.

## Run the notes service

Generate development credentials once. `.env` and `var/` are ignored by Git.

```bash
umask 077
python3 scripts/dev_tokens.py --admin > .env
source .env
python3 examples/notes-server/server.py --database var/notes.sqlite3
```

In another terminal, activate `.venv` and start the gateway:

```bash
source .venv/bin/activate
source .env
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml \
WAYFRAME_STATE_FILE="$PWD/var/gateway-state.json" \
  cargo run --locked -p wayframe-example-mcp-gateway
```

Connect an MCP client to `http://127.0.0.1:8080/mcp` with
`Authorization: Bearer <WAYFRAME_OPERATOR_TOKEN>` to write, read, and delete notes.
`WAYFRAME_READER_TOKEN` permits reads. The upstream is `http://127.0.0.1:9001/api/mcp`.
The SQLite database is shared by authorized example users. Stop both processes with Ctrl-C.

In a terminal with `.env` loaded, `python3 scripts/admin.py show` reads managed state.
[User management](../../docs/gateway.md#users-and-rate-rules) covers issuing and revoking tokens.

## Use your server

Copy [notes.toml](notes.toml), set `upstream` to the full MCP endpoint, configure `[tools]`,
and map client identities to token environment variables. Keep upstream credentials separate.
Unknown tools deny. Use `WAYFRAME_CONFIG` to select the file. Configuration and trust settings
are covered in the [gateway guide](../../docs/gateway.md).

Stop the HTTP gateway before reusing its state file from a local stdio client.
Run the built binary with the same config/state environment and
`WAYFRAME_TRANSPORT=stdio`, `WAYFRAME_LOCAL_TOKEN="$WAYFRAME_READER_TOKEN"`.
For HTTP over Unix, set `WAYFRAME_UNIX_SOCKET` to an unused absolute path.

## Two replicas

Start a second notes server with `--port 9002 --database var/notes.sqlite3` and the same `.env`.
Use [notes-lb.toml](notes-lb.toml) instead of `notes.toml`. It routes to ports 9001/9002 with
weights 1:3. Both servers explicitly share the database; the gateway does not replicate it.
See [routing](../../docs/gateway.md#routing-and-load-balancing) for affinity and hard limits.

[Plugins](../../docs/plugins.md) · [Workflows](../../docs/workflows.md) ·
[Tests and benchmarks](../../docs/testing.md)
