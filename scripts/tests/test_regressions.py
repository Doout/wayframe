"""Regression probes for stalled peers, local stream deadlines and failed benchmark reports."""

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts/tests"))
from lb_support import cluster
from mcp_support import Services, modern_call, modern_headers
from test_load_balancing import initialize

BINARY = Path(
    os.environ.get("WAYFRAME_TEST_BINARY", ROOT / "target/debug/wayframe-example-mcp-gateway")
)


async def stalled_reader():
    import httpx

    services = Services(BINARY)
    event = (
        b"data: "
        + json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {"content": [{"type": "text", "text": "x" * 8000}]},
            }
        ).encode()
        + b"\n\n"
    )
    repetitions = (64 * 1024 * 1024) // len(event)
    size = len(event) * repetitions

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            try:
                for _ in range(repetitions):
                    self.wfile.write(event)
            except (OSError, ConnectionError):
                pass

        def log_message(self, *_):
            pass

        def handle(self):
            try:
                super().handle()
            except (OSError, ConnectionError):
                pass

    upstream = ThreadingHTTPServer(("127.0.0.1", services.up_port), Handler)
    upstream.daemon_threads = True
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    services.config.write_text(
        "timeout_seconds = 1\nmax_in_flight = 1\nmax_response_bytes = 67108864\n"
        + services.config.read_text()
    )
    sock = None
    try:
        process = await asyncio.to_thread(
            subprocess.Popen,
            [str(services.binary)],
            env=services.env,
            cwd=ROOT,
            stdout=services.log,
            stderr=services.log,
        )
        services.processes.append(process)
        base = services.gateway.rsplit("/mcp", 1)[0]
        await services.wait_ready(base + "/healthz", process)
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        sock.settimeout(3)
        sock.connect(("127.0.0.1", services.gw_port))
        body = json.dumps(modern_call("get_note", {"key": "probe"})).encode()
        headers = modern_headers(services.tokens["reader"], "get_note")
        headers.update(
            Host=f"127.0.0.1:{services.gw_port}",
            **{"Content-Type": "application/json", "Content-Length": str(len(body))},
        )
        request = (
            "POST /mcp HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items()) + "\r\n"
        )
        sock.sendall(request.encode() + body)
        response = bytearray()
        while b"\r\n\r\n" not in response:
            response.extend(sock.recv(1))
        await asyncio.sleep(3)
        async with httpx.AsyncClient(trust_env=False) as http:
            metrics = (
                await http.get(
                    base + "/metrics",
                    headers={"Authorization": "Bearer " + services.tokens["operator"]},
                )
            ).json()
            async with http.stream(
                "POST",
                services.gateway,
                headers=modern_headers(services.tokens["reader"], "get_note"),
                json=modern_call("get_note", {"key": "probe"}),
            ) as next_response:
                next_status = next_response.status_code
            result = {
                "request_timeout_seconds": 1,
                "waited_seconds": 3,
                "first_status": response.split(b"\r\n")[0].decode(),
                "active_after_deadline": metrics["active"],
                "next_request_status": next_status,
            }
            sock.close()
            sock = None
            for _ in range(50):
                after = (
                    await http.get(
                        base + "/metrics",
                        headers={"Authorization": "Bearer " + services.tokens["operator"]},
                    )
                ).json()["active"]
                if after == 0:
                    break
                await asyncio.sleep(0.02)
            result["active_after_disconnect"] = after
            return result
    finally:
        if sock:
            sock.close()
        services.close()
        upstream.shutdown()
        upstream.server_close()
        thread.join()


async def healthy_stream_timeout():
    import httpx
    from test_load_balancing import metrics

    # Patch only the temporary harness configuration before gateway startup.
    original = Services.start

    async def short_start(self):
        self.config.write_text("timeout_seconds = 1\n" + self.config.read_text())
        return await original(self)

    Services.start = short_start
    try:
        async with (
            cluster(BINARY) as (services, _),
            httpx.AsyncClient(trust_env=False, timeout=5) as http,
        ):
            headers = await initialize(http, services)
            before = await metrics(http, services)
            error = None
            try:
                async with http.stream("GET", services.gateway, headers=headers) as response:
                    async for _ in response.aiter_bytes():
                        pass
            except httpx.HTTPError as e:
                error = type(e).__name__
            after = await metrics(http, services)
            live = await http.get(services.upstream.rsplit("/api/mcp", 1)[0] + "/healthz")
            return {
                "stream_error": error,
                "upstream_liveness_status": live.status_code,
                "before": before,
                "after": after,
            }
    finally:
        Services.start = original


async def benchmark_outage():
    import tempfile

    import benchmark

    original = benchmark.measure

    async def zero(*args):
        return {
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "raw_latency_ms": [],
            "errors": [
                {"type": "ConnectError", "message": "injected outage after successful warmup"}
            ],
            "succeeded": 0,
            "process_usage": {},
            "successful_requests_per_second": 0,
        }

    benchmark.measure = zero
    try:
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                binary=BINARY,
                trace_protocol=None,
                managed=False,
                trace_sample_ratio=1,
                rounds=1,
                requests=100,
                transports="tcp",
                formats="json",
                payloads=[64],
                concurrency=[1],
                native_only=True,
                skip_boundaries=True,
                output=Path(directory),
            )
            error = None
            try:
                await benchmark.run(args)
            except (RuntimeError, ValueError, TypeError) as e:
                error = type(e).__name__ + ": " + str(e)
            return {
                "injected": "all timed requests fail after successful setup",
                "exception": error,
                "saved_report_files": [p.name for p in Path(directory).iterdir()],
            }
    finally:
        benchmark.measure = original


class RuntimeRegressions(unittest.IsolatedAsyncioTestCase):
    async def test_failed_setup_keeps_the_partial_reports(self):
        import tempfile
        from unittest.mock import AsyncMock, patch

        import benchmark

        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                binary=BINARY,
                trace_protocol=None,
                managed=False,
                trace_sample_ratio=1,
                rounds=1,
                requests=100,
                transports="tcp",
                formats="json",
                payloads=[64],
                concurrency=[1],
                native_only=True,
                skip_boundaries=True,
                output=Path(directory),
            )
            with (
                patch.object(
                    benchmark, "scenario", AsyncMock(side_effect=RuntimeError("setup failed"))
                ),
                self.assertRaisesRegex(RuntimeError, "setup failed"),
            ):
                await benchmark.run(args)
            report = json.loads((args.output / "mcp-overhead.json").read_text())
            self.assertEqual(report["scenarios"][0]["status"], "failed")
            self.assertEqual(report["scenarios"][0]["execution_error"]["type"], "RuntimeError")
            self.assertIn("setup failed", (args.output / "mcp-overhead.csv").read_text())

    async def test_stalled_reader_releases_admission_after_deadline(self):
        result = await stalled_reader()
        self.assertEqual(result["active_after_deadline"], 0)
        self.assertEqual(result["next_request_status"], 200)
        self.assertEqual(result["active_after_disconnect"], 0)

    async def test_local_stream_timeout_does_not_eject_healthy_backend(self):
        result = await healthy_stream_timeout()
        self.assertEqual(result["upstream_liveness_status"], 200)
        self.assertEqual(
            result["before"][0]["failures"],
            result["after"][0]["failures"],
        )
        self.assertTrue(result["after"][0]["healthy"])

    async def test_all_failed_benchmarks_keep_json_and_csv(self):
        result = await benchmark_outage()
        self.assertTrue(result["exception"].startswith("RuntimeError:"), result)
        self.assertTrue(any(f.endswith(".json") for f in result["saved_report_files"]))
        self.assertTrue(any(f.endswith(".csv") for f in result["saved_report_files"]))
