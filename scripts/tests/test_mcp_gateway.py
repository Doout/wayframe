"""Official SDK and adversarial checks against the running Rust gateway."""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest
from contextlib import closing
from pathlib import Path

import httpx
from mcp import Client, StdioServerParameters

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mcp_support import ROOT, Services, modern_call, modern_headers, sdk_client

BINARY = Path(
    os.environ.get("WAYFRAME_TEST_BINARY", ROOT / "target/debug/wayframe-example-mcp-gateway")
)


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.services = Services(BINARY)
        self.addCleanup(self.services.close)
        await self.services.start()

    async def seed(self):
        async with sdk_client(self.services.gateway, self.services.tokens["operator"]) as client:
            result = await client.call_tool(
                "put_note", {"key": "hello", "value": "Persisted through Wayframe"}
            )
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["revision"], 1)

    async def test_sdk_persistence_survives_upstream_restart(self):
        await self.seed()
        await self.services.restart_upstream()
        async with sdk_client(self.services.gateway, self.services.tokens["reader"]) as client:
            tools = await client.list_tools()
            self.assertEqual(
                {t.name for t in tools.tools},
                {"get_note", "put_note", "delete_note", "admin_preview"},
            )
            result = await client.call_tool("get_note", {"key": "hello"})
            self.assertFalse(result.is_error)
            self.assertEqual(
                result.structured_content,
                {"key": "hello", "value": "Persisted through Wayframe", "revision": 1},
            )

    async def test_sdk_streams_progress_and_returns_committed_value(self):
        events = []

        async def progress(value, total, message):
            events.append(value)

        async with sdk_client(self.services.gateway, self.services.tokens["operator"]) as client:
            result = await client.call_tool(
                "put_note", {"key": "progress", "value": "saved"}, progress_callback=progress
            )
            self.assertFalse(result.is_error)
        self.assertEqual(events, [0, 1])
        with closing(sqlite3.connect(self.services.database)) as connection, connection:
            self.assertEqual(
                connection.execute("SELECT value FROM notes WHERE key = 'progress'").fetchone()[0],
                "saved",
            )

    async def test_reader_denial_does_not_write_or_delete(self):
        await self.seed()
        async with httpx.AsyncClient(trust_env=False) as http:
            for tool, args in [
                ("put_note", {"key": "hello", "value": "changed"}),
                ("delete_note", {"key": "hello"}),
            ]:
                response = await http.post(
                    self.services.gateway,
                    headers=modern_headers(self.services.tokens["reader"], tool),
                    json=modern_call(tool, args),
                )
                self.assertEqual(response.status_code, 403)
        with closing(sqlite3.connect(self.services.database)) as connection, connection:
            self.assertEqual(
                connection.execute(
                    "SELECT value, revision FROM notes WHERE key = 'hello'"
                ).fetchone(),
                ("Persisted through Wayframe", 1),
            )

    async def test_authentication_and_origin_boundaries(self):
        body = modern_call("admin_preview", {})
        async with httpx.AsyncClient(trust_env=False) as http:
            for url, headers, expected in [
                (self.services.gateway, modern_headers("x" * 48, "admin_preview"), 401),
                (
                    self.services.upstream,
                    modern_headers(self.services.tokens["reader"], "admin_preview"),
                    401,
                ),
                (
                    self.services.gateway,
                    {
                        **modern_headers(self.services.tokens["operator"], "admin_preview"),
                        "Origin": "https://evil.example",
                    },
                    403,
                ),
                (
                    self.services.gateway,
                    {
                        **modern_headers(self.services.tokens["operator"], "admin_preview"),
                        "Host": "evil.example",
                    },
                    403,
                ),
            ]:
                self.assertEqual(
                    (await http.post(url, headers=headers, json=body)).status_code, expected
                )
            metrics = self.services.gateway.replace("/mcp", "/metrics")
            self.assertEqual(
                (
                    await http.get(
                        metrics,
                        headers={"Authorization": f"Bearer {self.services.tokens['reader']}"},
                    )
                ).status_code,
                403,
            )
            response = await http.get(
                metrics, headers={"Authorization": f"Bearer {self.services.tokens['operator']}"}
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["active"], 0)

    async def test_invalid_envelopes_and_unknown_tools_fail_closed(self):
        async with httpx.AsyncClient(trust_env=False) as http:
            headers = modern_headers(self.services.tokens["operator"], "get_note")
            mismatch = modern_call("delete_note", {"key": "not-written"})
            self.assertEqual(
                (
                    await http.post(self.services.gateway, headers=headers, json=mismatch)
                ).status_code,
                400,
            )
            headers["Content-Type"] = "application/json"
            self.assertEqual(
                (
                    await http.post(
                        self.services.gateway,
                        headers=headers,
                        content=b'{"jsonrpc":"2.0","method":"tools/call","method":"ping"}',
                    )
                ).status_code,
                400,
            )
            self.assertEqual(
                (
                    await http.post(self.services.gateway, headers=headers, content=b"x" * 40000)
                ).status_code,
                413,
            )
            self.assertEqual(
                (
                    await http.post(
                        self.services.gateway,
                        headers=modern_headers(self.services.tokens["operator"], "unknown"),
                        json=modern_call("unknown", {}),
                    )
                ).status_code,
                403,
            )
        with closing(sqlite3.connect(self.services.database)) as connection, connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM notes").fetchone()[0], 0)

    async def test_legacy_sdk_session_calls(self):
        async with sdk_client(
            self.services.gateway, self.services.tokens["operator"], legacy=True
        ) as client:
            result = await client.call_tool("put_note", {"key": "session", "value": "legacy"})
            self.assertFalse(result.is_error)
            self.assertEqual(
                (await client.call_tool("get_note", {"key": "session"})).structured_content[
                    "value"
                ],
                "legacy",
            )

    async def test_session_ids_cannot_be_reused_by_another_identity(self):
        async with httpx.AsyncClient(trust_env=False) as http:
            headers = {
                "Authorization": f"Bearer {self.services.tokens['operator']}",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2025-11-25",
            }
            initialize = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "ownership-test", "version": "1"},
                },
            }
            response = await http.post(self.services.gateway, headers=headers, json=initialize)
            self.assertEqual(response.status_code, 200)
            session_id = response.headers["mcp-session-id"]
            other = {
                **headers,
                "MCP-Session-Id": session_id,
                "Authorization": f"Bearer {self.services.tokens['reader']}",
            }
            for method in ("GET", "DELETE"):
                self.assertEqual(
                    (await http.request(method, self.services.gateway, headers=other)).status_code,
                    403,
                )
            self.assertEqual(
                (
                    await http.post(
                        self.services.gateway,
                        headers=other,
                        json={"jsonrpc": "2.0", "id": 2, "method": "ping"},
                    )
                ).status_code,
                403,
            )
            owned = {**headers, "MCP-Session-Id": session_id}
            self.assertEqual(
                (await http.delete(self.services.gateway, headers=owned)).status_code, 200
            )
            self.assertEqual(
                (
                    await http.post(
                        self.services.gateway,
                        headers=owned,
                        json={"jsonrpc": "2.0", "id": 2, "method": "ping"},
                    )
                ).status_code,
                404,
            )

    async def test_official_stdio_legacy_client(self):
        await self.seed()
        env = {
            **self.services.env,
            "WAYFRAME_TRANSPORT": "stdio",
            "WAYFRAME_LOCAL_TOKEN": self.services.tokens["reader"],
        }
        async with Client(
            StdioServerParameters(command=str(BINARY), env=env, cwd=str(ROOT)),
            mode="legacy",
            cache=None,
            read_timeout_seconds=10,
        ) as client:
            result = await client.call_tool("get_note", {"key": "hello"})
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["value"], "Persisted through Wayframe")

    async def test_official_stdio_client(self):
        await self.seed()
        env = {
            **self.services.env,
            "WAYFRAME_TRANSPORT": "stdio",
            "WAYFRAME_LOCAL_TOKEN": self.services.tokens["reader"],
        }
        async with Client(
            StdioServerParameters(command=str(BINARY), env=env, cwd=str(ROOT)),
            mode="2026-07-28",
            cache=None,
            read_timeout_seconds=10,
        ) as client:
            result = await client.call_tool("get_note", {"key": "hello"})
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["value"], "Persisted through Wayframe")


class AdditionalTransports(unittest.IsolatedAsyncioTestCase):
    async def scenario(self, **options):
        services = Services(BINARY, **options)
        self.addCleanup(services.close)
        await services.start()
        async with sdk_client(
            services.gateway, services.tokens["operator"], uds=services.socket
        ) as client:
            result = await client.call_tool("put_note", {"key": "transport", "value": "works"})
            self.assertFalse(result.is_error)
            self.assertEqual(
                (await client.call_tool("get_note", {"key": "transport"})).structured_content[
                    "value"
                ],
                "works",
            )
        return services

    async def test_verified_https_upstream(self):
        await self.scenario(tls=True)

    async def test_unix_socket_frontend(self):
        services = await self.scenario(unix=True)
        self.assertEqual(services.socket.stat().st_mode & 0o777, 0o600)

    async def test_json_responses(self):
        await self.scenario(json_response=True)

    async def test_wasm_cannot_override_native_permissions(self):
        services = await self.scenario(wasm=True)
        async with httpx.AsyncClient(trust_env=False) as http:
            for role in ("reader", "operator"):
                response = await http.post(
                    services.gateway,
                    headers=modern_headers(services.tokens[role], "admin_preview"),
                    json=modern_call("admin_preview", {}),
                )
                self.assertEqual(response.status_code, 403)

    async def test_untrusted_https_certificate_fails_closed(self):
        services = Services(BINARY, tls=True)
        self.addCleanup(services.close)
        services.config.write_text(
            "\n".join(
                line
                for line in services.config.read_text().splitlines()
                if not line.startswith("upstream_ca")
            )
        )
        await services.start()
        async with httpx.AsyncClient(trust_env=False) as http:
            result = await http.post(
                services.gateway,
                headers=modern_headers(services.tokens["operator"], "admin_preview"),
                json=modern_call("admin_preview", {}),
            )
            self.assertEqual(result.status_code, 502)
