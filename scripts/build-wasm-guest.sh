#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v wasm-tools >/dev/null || { echo 'Install wasm-tools 1.261.0: cargo install wasm-tools --version 1.261.0 --locked' >&2; exit 2; }
rustup target add wasm32-unknown-unknown
CARGO_TARGET_DIR="$PWD/target/path-control" cargo build --locked --manifest-path plugins/path-control/Cargo.toml --target wasm32-unknown-unknown --release
wasm-tools component new target/path-control/wasm32-unknown-unknown/release/wayframe_example_path_control.wasm -o target/path-control/path-control.component.wasm
wasm-tools validate target/path-control/path-control.component.wasm
python3 - <<'PY'
import hashlib
from pathlib import Path
root=Path('target/path-control')
component=root/'path-control.component.wasm'
source=Path('plugins/path-control/plugin.toml').read_text().replace('component = "path-control.component.wat"','component = "path-control.component.wasm"')
import re
source=re.sub(r'sha256 = "[a-f0-9]+"',f'sha256 = "{hashlib.sha256(component.read_bytes()).hexdigest()}"',source).replace('\ntimeout_ms = 10\n','\ntimeout_ms = 100\n')
(root/'plugin.toml').write_text(source)
print(root/'plugin.toml')
PY
