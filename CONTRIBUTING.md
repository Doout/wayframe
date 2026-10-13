# Contributing

Use Rust with rustup and Python 3.11 or later. The repository pins the tested Rust toolchain.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
bash scripts/verify.sh
```

Format changes before running verification:

```bash
cargo fmt --all
python3 -m ruff format scripts examples/mcp-server
```

Commit intentional dependency updates with the resulting Cargo.lock. CI checks formatting without
rewriting source and runs verification with locked dependency resolution on Linux and macOS.

Keep the core protocol-independent. Put HTTP, MCP, Wasm, and transport implementations in
optional crates, and runnable applications under `examples/`. Do not add mandatory networking,
observability, or plugin dependencies to `wayframe-core`.

New blocks should test allow/deny behavior, malformed inputs, deadlines, overload, and state
ownership where applicable. New capabilities need documented trust boundaries, default-deny
enforcement, and negative tests. Avoid secret or payload logging, unbounded queues/maps,
blocking native hot-path I/O, and shared mutable guest instances.

The workspace forbids unsafe Rust. Plugin ABI changes and digest updates must be intentional.
See [security boundaries](SECURITY.md) and [build status](BUILD_STATUS.md) for release requirements.
