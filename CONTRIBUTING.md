# Contributing

Follow [setup](README.md#setup), then run [verification](docs/testing.md#verification).
Format code before submitting:

```bash
cargo fmt --all
python3 -m ruff format scripts examples/mcp-server examples/notes-server
```

Keep the core independent of protocols, networking, telemetry, and plugin runtimes.
Adapters belong in optional crates; runnable applications belong in `examples/`.
Commit dependency updates with their lockfiles. The workspace forbids unsafe Rust.

Test the behavior a change affects, including denials, malformed input, deadlines,
overload, and cancellation where relevant. Preserve ownership through streamed responses.
Keep credentials and argument payloads out of logs. Document new trust boundaries and
make plugin ABI or digest changes explicit. See [security](SECURITY.md).
