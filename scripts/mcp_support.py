"""Shared local MCP process harness. Credentials and databases live in temporary directories."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import socket
import ssl
import subprocess
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@asynccontextmanager
async def sdk_client(url, token, *, legacy=False, uds=None, verify=True):
    transport = httpx2.AsyncHTTPTransport(
        uds=str(uds) if uds else None, verify=verify, trust_env=False
    )
    async with (
        httpx2.AsyncClient(
            transport=transport,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
            trust_env=False,
        ) as http,
        Client(
            streamable_http_client(url, http_client=http),
            mode="legacy" if legacy else "2026-07-28",
            cache=None,
            read_timeout_seconds=10,
        ) as client,
    ):
        yield client


class Services:
    def __init__(
        self, binary, *, wasm=False, json_response=False, tls=False, unix=False, benchmark=False
    ):
        self.binary = Path(binary).resolve()
        if not self.binary.is_file():
            raise ValueError(f"Gateway binary missing: {self.binary}")
        self.temporary = tempfile.TemporaryDirectory(prefix="wayframe-mcp-")
        self.directory = Path(self.temporary.name)
        self.up_port, self.gw_port = free_port(), free_port()
        while self.up_port == self.gw_port:
            self.gw_port = free_port()
        self.database = self.directory / "notes.sqlite3"
        self.socket = self.directory / "gateway.sock" if unix else None
        self.tokens = {
            role: secrets.token_urlsafe(48) for role in ("reader", "operator", "upstream")
        }
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("WAYFRAME_")}
        self.env.update(
            {f"WAYFRAME_{role.upper()}_TOKEN": token for role, token in self.tokens.items()}
        )
        self.env["RUST_LOG"] = "warn"
        scheme = "https" if tls else "http"
        self.upstream = f"{scheme}://127.0.0.1:{self.up_port}/api/mcp"
        self.gateway = (
            f"http://127.0.0.1:{self.gw_port}/mcp" if not unix else "http://localhost/mcp"
        )
        self.config = self.directory / "gateway.toml"
        config = (ROOT / "examples/mcp-gateway/notes.toml").read_text()
        config = config.replace("127.0.0.1:8080", f"127.0.0.1:{self.gw_port}").replace(
            "http://127.0.0.1:9001/api/mcp", self.upstream
        )
        self.certificate = None
        self.key = None
        if tls:
            self.certificate, self.key = self.directory / "ca.pem", self.directory / "key.pem"
            ca_key = self.directory / "ca-key.pem"
            csr = self.directory / "server.csr"
            self.server_certificate = self.directory / "server.pem"
            extensions = self.directory / "extensions.cnf"
            extensions.write_text(
                "subjectAltName=IP:127.0.0.1,DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n"
            )
            commands = [
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-keyout",
                    str(ca_key),
                    "-out",
                    str(self.certificate),
                    "-days",
                    "1",
                    "-subj",
                    "/CN=Wayframe-Test-CA",
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-addext",
                    "keyUsage=critical,keyCertSign,cRLSign",
                ],
                [
                    "openssl",
                    "req",
                    "-new",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-keyout",
                    str(self.key),
                    "-out",
                    str(csr),
                    "-subj",
                    "/CN=localhost",
                ],
                [
                    "openssl",
                    "x509",
                    "-req",
                    "-in",
                    str(csr),
                    "-CA",
                    str(self.certificate),
                    "-CAkey",
                    str(ca_key),
                    "-CAcreateserial",
                    "-out",
                    str(self.server_certificate),
                    "-days",
                    "1",
                    "-extfile",
                    str(extensions),
                ],
            ]
            for command in commands:
                subprocess.run(command, check=True, capture_output=True)
            config = f"upstream_ca = {json.dumps(str(self.certificate))}\n" + config
        if benchmark:
            config = (
                "requests_per_second = 1000000.0\nburst = 1000000\nmax_in_flight = 256\n" + config
            )
        self.config.write_text(config)
        self.env["WAYFRAME_CONFIG"] = str(self.config)
        if unix:
            self.env["WAYFRAME_UNIX_SOCKET"] = str(self.socket)
        if wasm:
            manifest = (ROOT / "plugins/path-control/plugin.toml").read_text()
            manifest = manifest.replace("warm_instances = 8", "warm_instances = 64").replace(
                "\ntimeout_ms = 10\n", "\ntimeout_ms = 100\n"
            )
            (self.directory / "path-control.component.wat").write_bytes(
                (ROOT / "plugins/path-control/path-control.component.wat").read_bytes()
            )
            plugin = self.directory / "plugin.toml"
            plugin.write_text(manifest)
            self.env["WAYFRAME_PLUGIN_MANIFEST"] = str(plugin)
        self.server_args = [
            sys.executable,
            str(ROOT / "examples/notes-server/server.py"),
            "--port",
            str(self.up_port),
            "--database",
            str(self.database),
        ]
        if json_response:
            self.server_args.append("--json")
        if tls:
            self.server_args.extend(
                ["--tls-cert", str(self.server_certificate), "--tls-key", str(self.key)]
            )
        self.processes = []
        self.log = (self.directory / "services.log").open("w+")
        self.upstream_process = None
        self.gateway_process = None

    async def wait_ready(self, url, process, *, uds=None):
        verify = (
            ssl.create_default_context(cafile=str(self.certificate)) if self.certificate else True
        )
        transport = httpx.AsyncHTTPTransport(
            uds=str(uds) if uds else None, verify=verify, trust_env=False
        )
        async with httpx.AsyncClient(transport=transport, timeout=0.5, trust_env=False) as client:
            for _ in range(150):
                if process.poll() is not None:
                    self.log.flush()
                    raise RuntimeError(
                        f"Service exited ({process.returncode}): {(self.directory / 'services.log').read_text()}"
                    )
                try:
                    if (await client.get(url)).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.1)
        raise RuntimeError("Service readiness timeout")

    async def start(self):
        self.upstream_process = await asyncio.to_thread(
            subprocess.Popen,
            self.server_args,
            env={
                "WAYFRAME_UPSTREAM_TOKEN": self.tokens["upstream"],
                **{k: v for k, v in self.env.items() if not k.startswith("WAYFRAME_")},
            },
            stdout=self.log,
            stderr=self.log,
            cwd=ROOT,
        )
        self.processes.append(self.upstream_process)
        await self.wait_ready(
            self.upstream.rsplit("/api/mcp", 1)[0] + "/healthz", self.upstream_process
        )
        self.gateway_process = await asyncio.to_thread(
            subprocess.Popen,
            [str(self.binary)],
            env=self.env,
            stdout=self.log,
            stderr=self.log,
            cwd=ROOT,
        )
        self.processes.append(self.gateway_process)
        await self.wait_ready(
            self.gateway.rsplit("/mcp", 1)[0] + "/healthz", self.gateway_process, uds=self.socket
        )
        return self

    async def restart_upstream(self):
        self.upstream_process.terminate()
        await asyncio.to_thread(self.upstream_process.wait, 5)
        self.upstream_process = await asyncio.to_thread(
            subprocess.Popen,
            self.server_args,
            env=self.env,
            stdout=self.log,
            stderr=self.log,
            cwd=ROOT,
        )
        self.processes.append(self.upstream_process)
        await self.wait_ready(
            self.upstream.rsplit("/api/mcp", 1)[0] + "/healthz", self.upstream_process
        )

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        self.log.close()
        self.temporary.cleanup()


def modern_call(tool, arguments, identifier=1):
    return {
        "jsonrpc": "2.0",
        "id": identifier,
        "method": "tools/call",
        "params": {
            "name": tool,
            "arguments": arguments,
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {},
            },
        },
    }


def modern_headers(token, tool):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": "tools/call",
        "Mcp-Name": tool,
    }
