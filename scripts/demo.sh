#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v cargo >/dev/null || { echo 'Rust/Cargo required; see BUILD_STATUS.md' >&2; exit 2; }
# Compile before starting any listeners. Smoke harness creates ephemeral secrets and cleans up both processes.
if [[ "${1:-}" == "--wasm" ]]; then
  cargo build --locked -p wayframe-example-mcp-gateway --features wasm
  python3 scripts/smoke.py --wasm
elif [[ $# -eq 0 ]]; then
  cargo build --locked -p wayframe-example-mcp-gateway
  python3 scripts/smoke.py
else
  echo 'usage: scripts/demo.sh [--wasm]' >&2; exit 2
fi
