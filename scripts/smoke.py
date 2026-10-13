#!/usr/bin/env python3
"""Live Rust gateway + Python MCP fixture tests. Requires an already built binary.
This fails explicitly when the binary is missing; it does not substitute a Python gateway.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("demo_client", ROOT / "examples/mcp-server/client.py")
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_ready(url, process):
    # No user proxy or external requests in the smoke harness.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for _ in range(150):
        if process.poll() is not None:
            raise RuntimeError(f"process exited during startup: {process.returncode}")
        try:
            with opener.open(url, timeout=0.2) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("readiness deadline exceeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=ROOT / "target/debug/wayframe-example-mcp-gateway"
    )
    parser.add_argument("--wasm", action="store_true")
    parser.add_argument(
        "--plugin-manifest", type=Path, default=ROOT / "plugins/path-control/plugin.toml"
    )
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        parser.error(
            f"Rust binary missing: {binary}; run cargo build -p wayframe-example-mcp-gateway first"
        )
    up_port, gw_port = free_port(), free_port()
    while gw_port == up_port:
        gw_port = free_port()
    env = os.environ.copy()
    env.pop("WAYFRAME_PLUGIN_MANIFEST", None)
    for name in ("WAYFRAME_READER_TOKEN", "WAYFRAME_OPERATOR_TOKEN", "WAYFRAME_UPSTREAM_TOKEN"):
        env[name] = secrets.token_urlsafe(48)
    env.update(
        WAYFRAME_LISTEN=f"127.0.0.1:{gw_port}",
        WAYFRAME_UPSTREAM=f"http://127.0.0.1:{up_port}",
        WAYFRAME_ALLOWED_ORIGINS=f"http://127.0.0.1:{gw_port}",
        RUST_LOG="warn",
    )
    if args.wasm:
        env["WAYFRAME_PLUGIN_MANIFEST"] = str(args.plugin_manifest.resolve())
    processes = []
    with tempfile.TemporaryDirectory(prefix="wayframe-smoke-") as tmp:
        logpath = Path(tmp) / "services.log"
        with logpath.open("w+") as log:
            try:
                upstream_env = {
                    k: v
                    for k, v in env.items()
                    if k not in ("WAYFRAME_READER_TOKEN", "WAYFRAME_OPERATOR_TOKEN")
                }
                upstream = subprocess.Popen(
                    [
                        sys.executable,
                        str(ROOT / "examples/mcp-server/server.py"),
                        "--port",
                        str(up_port),
                    ],
                    env=upstream_env,
                    stdout=log,
                    stderr=log,
                )
                processes.append(upstream)
                wait_ready(f"http://127.0.0.1:{up_port}/healthz", upstream)
                gateway = subprocess.Popen([str(binary)], env=env, stdout=log, stderr=log, cwd=ROOT)
                processes.append(gateway)
                wait_ready(f"http://127.0.0.1:{gw_port}/healthz", gateway)
                endpoint = f"http://127.0.0.1:{gw_port}/mcp"
                reports = []
                for legacy in (False, True):
                    reports.append(
                        client.showcase(
                            endpoint,
                            env["WAYFRAME_READER_TOKEN"],
                            env["WAYFRAME_OPERATOR_TOKEN"],
                            legacy=legacy,
                            plugin=args.wasm,
                        )
                    )
                    time.sleep(1.0)  # replenish the demonstration rate bucket between profiles
                outsider = client.Client(endpoint, secrets.token_urlsafe(48))
                if outsider.exchange("ping")[0] != 401:
                    raise RuntimeError("unauthenticated request was accepted")
                # The gateway token must NOT work directly on the upstream server.
                wrong = client.Client(
                    f"http://127.0.0.1:{up_port}/mcp", env["WAYFRAME_READER_TOKEN"]
                )
                if wrong.exchange("ping")[0] != 401:
                    raise RuntimeError("client/upstream trust boundaries were conflated")
                reader = client.Client(endpoint, env["WAYFRAME_READER_TOKEN"])
                statuses = [reader.exchange("ping")[0] for _ in range(60)]
                if 429 not in statuses:
                    raise RuntimeError("request limiter did not reject a burst")
                if any(s not in (200, 429, 503) for s in statuses):
                    raise RuntimeError(f"unexpected burst statuses: {statuses}")
                print(
                    json.dumps(
                        {
                            "rust_gateway_executed": True,
                            "wasm_enabled": args.wasm,
                            "profiles": reports,
                            "unauthorized_status": 401,
                            "separate_upstream_credential": True,
                            "burst_rate_limited": statuses.count(429),
                        },
                        indent=2,
                    )
                )
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                for process in reversed(processes):
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
