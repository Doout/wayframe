"""Live TLS/mTLS, credential-provider, route-group and custom TCP checks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import socket
import ssl
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mcp_support import ROOT, Services, free_port, modern_call, modern_headers

BINARY = Path(
    os.environ.get("WAYFRAME_TEST_BINARY", ROOT / "target/debug/wayframe-example-mcp-gateway")
)


class NetworkLifecycle(unittest.IsolatedAsyncioTestCase):
    def prepare(self, *, tls=False):
        services = Services(BINARY, tls=tls, json_response=True, benchmark=True)
        self.addCleanup(services.close)
        return services

    async def openssl(self, *args):
        await asyncio.to_thread(
            subprocess.run, ["openssl", *map(str, args)], check=True, capture_output=True
        )

    async def test_tls_mtls_verification_and_certificate_reload(self):
        services = self.prepare(tls=True)
        client_key = services.directory / "client.key"
        client_csr = services.directory / "client.csr"
        client_cert = services.directory / "client.pem"
        ext = services.directory / "client.ext"
        ext.write_text(
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\n"
        )
        await self.openssl(
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            client_key,
            "-out",
            client_csr,
            "-subj",
            "/CN=wayframe-client",
        )
        await self.openssl(
            "x509",
            "-req",
            "-in",
            client_csr,
            "-CA",
            services.certificate,
            "-CAkey",
            services.directory / "ca-key.pem",
            "-CAcreateserial",
            "-out",
            client_cert,
            "-days",
            "1",
            "-extfile",
            ext,
        )
        context = ssl.create_default_context(cafile=str(services.certificate))
        context.load_cert_chain(client_cert, client_key)
        owner = secrets.token_hex(32)
        services.env.update(
            WAYFRAME_TLS_CERT=str(services.server_certificate),
            WAYFRAME_TLS_KEY=str(services.key),
            WAYFRAME_TLS_CLIENT_CA=str(services.certificate),
            WAYFRAME_STATE_FILE=str(services.directory / "users.json"),
            WAYFRAME_ADMIN_TOKEN=owner,
        )
        services.gateway = services.gateway.replace("http:", "https:")

        async def ready(url, process, **_):
            async with httpx.AsyncClient(verify=context, trust_env=False, timeout=0.5) as http:
                for _ in range(150):
                    if process.poll() is not None:
                        raise RuntimeError("TLS process exited")
                    try:
                        if (await http.get(url)).status_code == 200:
                            return
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                raise RuntimeError("TLS readiness timeout")

        services.wait_ready = ready
        await services.start()
        base = services.gateway.rsplit("/mcp", 1)[0]
        async with httpx.AsyncClient(verify=context, trust_env=False) as http:
            self.assertEqual((await http.get(base + "/healthz")).status_code, 200)
            response = await http.post(
                services.gateway,
                headers=modern_headers(services.tokens["operator"], "admin_preview"),
                json=modern_call("admin_preview", {}),
            )
            self.assertEqual(response.status_code, 200)

            def fingerprint():
                with (
                    socket.create_connection(("127.0.0.1", services.gw_port), timeout=3) as io,
                    context.wrap_socket(io, server_hostname="localhost") as tls,
                ):
                    return hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest()

            old = await asyncio.to_thread(fingerprint)
            rotated_key = services.directory / "rotated.key"
            rotated_csr = services.directory / "rotated.csr"
            rotated_cert = services.directory / "rotated.pem"
            await self.openssl(
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                rotated_key,
                "-out",
                rotated_csr,
                "-subj",
                "/CN=localhost",
            )
            await self.openssl(
                "x509",
                "-req",
                "-in",
                rotated_csr,
                "-CA",
                services.certificate,
                "-CAkey",
                services.directory / "ca-key.pem",
                "-CAcreateserial",
                "-out",
                rotated_cert,
                "-days",
                "1",
                "-extfile",
                services.directory / "extensions.cnf",
            )
            services.server_certificate.write_bytes(rotated_cert.read_bytes())
            services.key.write_bytes(rotated_key.read_bytes())
            response = await http.post(
                base + "/admin/reload",
                headers={"Authorization": f"Bearer {owner}", "If-Match": '"1"'},
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotEqual(old, await asyncio.to_thread(fingerprint))
        async with httpx.AsyncClient(
            verify=ssl.create_default_context(cafile=str(services.certificate)), trust_env=False
        ) as http:
            with self.assertRaises((httpx.HTTPError, ssl.SSLError)):
                await http.get(base + "/healthz")
        async with httpx.AsyncClient(trust_env=False) as http:
            with self.assertRaises((httpx.HTTPError, ssl.SSLError)):
                await http.get(base + "/healthz")

    async def test_upstream_oauth_uses_private_client_credentials_and_reuses_token(self):
        services = self.prepare(tls=True)
        calls = []
        client_secret = secrets.token_hex(32)
        secret_file = services.directory / "oauth-secret"
        secret_file.write_text(client_secret)
        secret_file.chmod(0o600)

        class Issuer(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                from urllib.parse import parse_qs

                form = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
                calls.append(form)
                body = json.dumps(
                    {
                        "access_token": services.tokens["upstream"],
                        "token_type": "Bearer",
                        "expires_in": 60,
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        issuer = ThreadingHTTPServer(("127.0.0.1", free_port()), Issuer)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(services.server_certificate, services.key)
        issuer.socket = context.wrap_socket(issuer.socket, server_side=True)
        threading.Thread(target=issuer.serve_forever, daemon=True).start()
        self.addCleanup(issuer.server_close)
        self.addCleanup(issuer.shutdown)
        services.config.write_text(
            services.config.read_text()
            + f'\n[upstream_oauth]\ntoken_endpoint="https://127.0.0.1:{issuer.server_port}/token"\nclient_id="wayframe-service"\nsecret_file={json.dumps(str(secret_file))}\nresource={json.dumps(services.upstream)}\nscopes=["notes.read"]\nca={json.dumps(str(services.certificate))}\n'
        )
        await services.start()
        async with httpx.AsyncClient(trust_env=False) as http:
            for _ in range(2):
                response = await http.post(
                    services.gateway,
                    headers=modern_headers(services.tokens["operator"], "admin_preview"),
                    json=modern_call("admin_preview", {}),
                )
                self.assertEqual(response.status_code, 200)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["grant_type"], ["client_credentials"])
        self.assertEqual(calls[0]["client_secret"], [client_secret])
        self.assertNotIn(services.tokens["reader"], json.dumps(calls))
        self.assertNotIn(services.tokens["operator"], json.dumps(calls))

    async def test_named_group_route_and_active_probe_fail_closed(self):
        services = self.prepare()
        services.config.write_text(
            services.config.read_text()
            + f'\n[[routes]]\nid="notes"\ngroup="notes"\npath="/mcp"\nmethods=["POST"]\ntenants=["notes"]\n[groups.notes]\nstrategy="round_robin"\nhealth_path="/healthz"\nhealth_interval_ms=100\n[[groups.notes.backends]]\nid="notes-one"\nendpoint={json.dumps(services.upstream)}\nhard_limit=2\n'
        )
        await services.start()
        async with httpx.AsyncClient(trust_env=False) as http:
            response = await http.post(
                services.gateway,
                headers=modern_headers(services.tokens["operator"], "admin_preview"),
                json=modern_call("admin_preview", {}),
            )
            self.assertEqual(response.status_code, 200)
            base = services.gateway.rsplit("/mcp", 1)[0]
            self.assertEqual((await http.get(base + "/readyz")).status_code, 200)
            services.upstream_process.terminate()
            await asyncio.to_thread(services.upstream_process.wait, 5)
            for _ in range(50):
                status = (await http.get(base + "/readyz")).status_code
                if status == 503:
                    break
                await asyncio.sleep(0.05)
            self.assertEqual(status, 503)
            response = await http.post(
                services.gateway,
                headers=modern_headers(services.tokens["operator"], "admin_preview"),
                json=modern_call("admin_preview", {}),
            )
            self.assertEqual(response.status_code, 503)

    async def test_custom_tcp_binding_keeps_authorization_per_message(self):
        services = self.prepare()
        await services.start()
        services.gateway_process.terminate()
        await asyncio.to_thread(services.gateway_process.wait, 5)
        services.env["WAYFRAME_TRANSPORT"] = "tcp"
        process = await asyncio.to_thread(
            subprocess.Popen,
            [str(BINARY)],
            env=services.env,
            cwd=ROOT,
            stdout=services.log,
            stderr=services.log,
        )
        services.processes.append(process)
        for _ in range(100):
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", services.gw_port)
                break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            raise RuntimeError("custom TCP listener did not start")

        async def call(message, token):
            body = json.dumps({"authorization": f"Bearer {token}", "message": message}).encode()
            writer.write(len(body).to_bytes(4, "big") + body)
            await writer.drain()
            size = int.from_bytes(await reader.readexactly(4), "big")
            return json.loads(await reader.readexactly(size))

        try:
            denied = await call(modern_call("admin_preview", {}), services.tokens["reader"])
            self.assertEqual(denied["status"], 403)
            allowed = await call(modern_call("admin_preview", {}), services.tokens["operator"])
            self.assertEqual(allowed["status"], 200)
            self.assertEqual(allowed["messages"][0]["result"]["structuredContent"]["notes"], 0)
        finally:
            writer.close()
            await writer.wait_closed()
