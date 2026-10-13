# A gateway built with Wayframe

This application demonstrates the framework. It is **not** the framework's mandatory daemon.
It fronts the read-only Python MCP fixture with authentication, request limiting, protocol inspection,
tool permission checks and an optional prewarmed Wasm policy.

See [verification instructions](../../CONTRIBUTING.md#verification) for development checks and
[security boundaries](../../SECURITY.md) for deployment limitations.

## Automated demonstration

From the workspace root:

```bash
bash scripts/demo.sh          # native control path
bash scripts/demo.sh --wasm   # adds the sandboxed control
```

Each command builds the actual binary, generates temporary independent credentials, binds two
local ephemeral ports, tests both MCP profiles and streaming, then cleans up. The Python tests
alone do not exercise this gateway. The smoke script explicitly refuses to run without its binary.

## Keep it running interactively

Generate development secrets once in a local ignored file, then source it in each terminal.
The generated file contains secrets: do not paste it into issues or commit it.

```bash
umask 077
python3 scripts/dev_tokens.py > .env
source .env
cargo build -p wayframe-example-mcp-gateway
```

Terminal A, from the workspace root:

```bash
source .env
env -u WAYFRAME_READER_TOKEN -u WAYFRAME_OPERATOR_TOKEN \
  python3 examples/mcp-server/server.py --port 9001
```

Terminal B:

```bash
source .env
cargo run -p wayframe-example-mcp-gateway
```

Terminal C:

```bash
source .env
python3 examples/mcp-server/client.py
python3 examples/mcp-server/client.py --legacy
```

The default endpoints are `http://127.0.0.1:8080/mcp` (gateway) and
`http://127.0.0.1:9001/mcp` (upstream). Both have `/healthz` for local liveness only.
No metrics/admin service is exposed publicly. Press Ctrl-C to stop either foreground process.

## Enable the Wasm control

Stop Terminal B's gateway, then run:

```bash
source .env
export WAYFRAME_PLUGIN_MANIFEST=plugins/path-control/plugin.toml
cargo run -p wayframe-example-mcp-gateway --features wasm
```

Re-run the client with `--expect-plugin-denial`. The plugin now also denies `admin_preview`
for the operator. The native reader denial remains in force. Guest code and clean instances
prepare before the listener binds; a warm pool miss returns 503 rather than skipping the control.

Expected assertions (not measured production guarantees):

| Request | Native controls | With plugin |
| --- | --- | --- |
| Reader calls `add` | Sum result | Sum result |
| Reader searches bundled docs | Bounded matches | Bounded matches |
| Reader calls `admin_preview` | 403 | 403 |
| Operator calls `admin_preview` | Harmless simulated preview | 403 |
| Body calls admin, header says add | 400 | 400 |
| Wrong client credential | 401 | 401 |
| Reader burst exhausts local bucket | 429 | 429 |
| Required clean plugin pool empty | Not applicable | 503 |

## Environment

`WAYFRAME_READER_TOKEN`, `WAYFRAME_OPERATOR_TOKEN`, and `WAYFRAME_UPSTREAM_TOKEN` are required
and must be distinct, strong development secrets. `WAYFRAME_LISTEN` defaults to `127.0.0.1:8080`;
`WAYFRAME_UPSTREAM` defaults to `http://127.0.0.1:9001`. The optional manifest requires the `wasm`
feature. `RUST_LOG` controls metadata-level logs. For a changed gateway port, configure the
Python server's `WAYFRAME_ALLOWED_ORIGINS` as appropriate; the smoke harness does so automatically.

Static tokens are not a standards-complete OAuth deployment. This is a loopback profile, not
an instruction to expose a public MCP URL. See ../../SECURITY.md and ../../docs/mcp-profile.md.
