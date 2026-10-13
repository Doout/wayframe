#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p verification/local
python3 -c 'import jsonschema' || { echo 'Install development dependencies: python3 -m pip install -r requirements-dev.txt' >&2; exit 2; }
python3 -m ruff format --check scripts examples/mcp-server
python3 -m ruff check scripts examples/mcp-server
python3 --version | tee verification/local/python-toolchain.txt
python3 -m pip freeze > verification/local/python-dependencies.txt
python3 scripts/check_source.py > verification/local/source-checks.json
python3 -m unittest discover -s examples/mcp-server/tests -v 2>&1 | tee verification/local/python-tests.txt
command -v cargo >/dev/null || { echo 'BLOCKED: Cargo unavailable; Rust checks were NOT run.' >&2; exit 2; }
rustc --version | tee verification/local/toolchain.txt
cargo --version | tee -a verification/local/toolchain.txt
cargo fmt --all -- --check
cargo fmt --manifest-path plugins/path-control/Cargo.toml -- --check
cargo check --locked -p wayframe --no-default-features
cargo test --locked --workspace --all-features --all-targets
cargo test --locked --workspace --all-features --doc
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
cargo build --locked -p wayframe-example-mcp-gateway
python3 scripts/smoke.py | tee verification/local/gateway-smoke.json
cargo build --locked -p wayframe-example-mcp-gateway --features wasm
python3 scripts/smoke.py --wasm | tee verification/local/wasm-smoke.json
cargo tree --locked --workspace > verification/local/dependencies.txt
