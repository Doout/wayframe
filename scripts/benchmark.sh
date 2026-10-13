#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -c 'import mcp, httpx, httpx2, psutil' || { echo 'Install requirements-dev.txt before benchmarking.' >&2; exit 2; }
cargo build --locked --release -p wayframe-example-mcp-gateway --features wasm
cargo build --locked --release -p wayframe --all-features --example pipeline_cost --example policy_cost --example lb_cost
python3 scripts/benchmark.py "$@"
mkdir -p target/benchmarks
target/release/examples/lb_cost > target/benchmarks/native-load-balancer.json
