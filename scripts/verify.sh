#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

command -v cargo >/dev/null || { echo 'Rust/Cargo required; see README.md' >&2; exit 2; }
python3 -c 'import jsonschema, ruff' || { echo 'Install requirements-dev.txt before running verification.' >&2; exit 2; }

report_dir=target/verification
mkdir -p "$report_dir"
exec > >(tee "$report_dir/verification.log") 2>&1

python3 --version
rustc --version
cargo --version
python3 -m ruff format --check scripts examples/mcp-server
python3 -m ruff check scripts examples/mcp-server
python3 -m unittest discover -s examples/mcp-server/tests -v
cargo fmt --all -- --check
cargo fmt --manifest-path plugins/path-control/Cargo.toml -- --check
cargo check --locked -p wayframe --no-default-features
cargo test --locked --workspace --all-features --all-targets
cargo test --locked --workspace --all-features --doc
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
cargo build --locked -p wayframe-example-mcp-gateway
python3 scripts/smoke.py | tee "$report_dir/gateway-smoke.json"
cargo build --locked -p wayframe-example-mcp-gateway --features wasm
python3 scripts/smoke.py --wasm | tee "$report_dir/wasm-smoke.json"
cargo tree --locked --workspace > "$report_dir/dependencies.txt"
