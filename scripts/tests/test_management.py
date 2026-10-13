"""Control-plane persistence, isolation and real OTLP exports from a running gateway."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import subprocess
import sys
import unittest
from pathlib import Path

import httpx
from mcp import Client, StdioServerParameters

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mcp_support import ROOT, Services, free_port, modern_call, modern_headers
from otel_receiver import Receiver

BINARY = Path(
    os.environ.get("WAYFRAME_TEST_BINARY", ROOT / "target/debug/wayframe-example-mcp-gateway")
)


class ManagementTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.services = Services(BINARY, json_response=True)
        self.addCleanup(self.services.close)
        self.admin_token = secrets.token_hex(32)
        self.state_file = self.services.directory / "state.json"
        self.services.env.update(
            WAYFRAME_STATE_FILE=str(self.state_file), WAYFRAME_ADMIN_TOKEN=self.admin_token
        )
        await self.services.start()
        self.base = self.services.gateway.rsplit("/mcp", 1)[0]
        self.http = httpx.AsyncClient(trust_env=False)
        self.addAsyncCleanup(self.http.aclose)

    async def state(self):
        response = await self.http.get(
            self.base + "/admin/state", headers={"Authorization": f"Bearer {self.admin_token}"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["ETag"], f'"{response.json()["revision"]}"')
        return response.json()

    async def change(self, method, path, payload=None, revision=None):
        if revision is None:
            revision = (await self.state())["revision"]
        return await self.http.request(
            method,
            self.base + path,
            headers={"Authorization": f"Bearer {self.admin_token}", "If-Match": f'"{revision}"'},
            json=payload if payload is not None else None,
        )

    async def tool(self, token, tool="admin_preview", arguments=None):
        return await self.http.post(
            self.services.gateway,
            headers=modern_headers(token, tool),
            json=modern_call(tool, arguments or {}),
        )

    async def create(self, identity="alice"):
        profile = {
            "id": identity,
            "subject": identity,
            "tenant": "notes",
            "permissions": ["notes:admin"],
        }
        response = await self.change("POST", "/admin/users", profile)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["issued_token"]

    async def restart(self):
        old = self.services.gateway_process
        old.terminate()
        await asyncio.to_thread(old.wait, 10)
        process = await asyncio.to_thread(
            subprocess.Popen,
            [str(BINARY)],
            env=self.services.env,
            cwd=ROOT,
            stdout=self.services.log,
            stderr=self.services.log,
        )
        self.services.gateway_process = process
        self.services.processes.append(process)
        await self.services.wait_ready(self.base + "/healthz", process)

    async def test_create_rotate_disable_delete_and_restart(self):
        token = await self.create()
        self.assertEqual((await self.tool(token)).status_code, 200)
        state = await self.state()
        self.assertNotIn("token", json.dumps(state))
        self.assertNotIn(token, self.state_file.read_text())
        self.assertEqual(self.state_file.stat().st_mode & 0o777, 0o600)
        response = await self.change("POST", "/admin/users/alice/token")
        self.assertEqual(response.status_code, 200)
        rotated = response.json()["issued_token"]
        self.assertEqual((await self.tool(token)).status_code, 401)
        self.assertEqual((await self.tool(rotated)).status_code, 200)
        profile = next(u for u in (await self.state())["users"] if u["id"] == "alice")
        profile["enabled"] = False
        self.assertEqual((await self.change("PUT", "/admin/users/alice", profile)).status_code, 200)
        for key in ("WAYFRAME_READER_TOKEN", "WAYFRAME_OPERATOR_TOKEN"):
            self.services.env.pop(key)
        await self.restart()
        self.assertEqual((await self.tool(rotated)).status_code, 401)
        profile["enabled"] = True
        self.assertEqual((await self.change("PUT", "/admin/users/alice", profile)).status_code, 200)
        self.assertEqual((await self.tool(rotated)).status_code, 200)
        self.assertEqual((await self.change("DELETE", "/admin/users/alice")).status_code, 200)
        self.assertEqual((await self.tool(rotated)).status_code, 401)

    async def test_live_grants_and_tool_controls(self):
        token = await self.create()
        profile = next(u for u in (await self.state())["users"] if u["id"] == "alice")
        profile["permissions"] = []
        self.assertEqual((await self.change("PUT", "/admin/users/alice", profile)).status_code, 200)
        self.assertEqual((await self.tool(token)).status_code, 403)
        rules = (await self.state())["rules"]
        rules["denied_tools"] = ["admin_preview"]
        self.assertEqual((await self.change("PUT", "/admin/rules", rules)).status_code, 200)
        self.assertEqual((await self.tool(self.services.tokens["operator"])).status_code, 403)
        await self.restart()
        self.assertEqual((await self.tool(self.services.tokens["operator"])).status_code, 403)
        rules["denied_tools"] = []
        self.assertEqual((await self.change("PUT", "/admin/rules", rules)).status_code, 200)
        self.assertEqual((await self.tool(self.services.tokens["operator"])).status_code, 200)

    async def test_scoped_rate_limits_refill_and_no_reset_on_unrelated_change(self):
        alice = await self.create()
        bob = await self.create("bob")
        rules = (await self.state())["rules"]
        rules["rate_limits"].append(
            {
                "id": "alice-preview",
                "user": "alice",
                "tool": "admin_preview",
                "requests_per_second": 2.0,
                "burst": 1,
            }
        )
        self.assertEqual((await self.change("PUT", "/admin/rules", rules)).status_code, 200)
        self.assertEqual((await self.tool(alice)).status_code, 200)
        self.assertEqual((await self.tool(alice)).status_code, 429)
        self.assertEqual((await self.tool(bob)).status_code, 200)
        await self.create("unrelated")
        self.assertEqual((await self.tool(alice)).status_code, 429)
        await asyncio.sleep(0.55)
        self.assertEqual((await self.tool(alice)).status_code, 200)

    async def test_management_auth_origin_stale_revision_and_invalid_changes(self):
        for token, status in [(self.services.tokens["operator"], 401), ("bad", 401)]:
            response = await self.http.get(
                self.base + "/admin/state", headers={"Authorization": f"Bearer {token}"}
            )
            self.assertEqual(response.status_code, status)
        response = await self.http.get(
            self.base + "/admin/state",
            headers={
                "Authorization": f"Bearer {self.admin_token}",
                "Origin": "https://evil.example",
            },
        )
        self.assertEqual(response.status_code, 403)
        response = await self.http.get(
            self.base + "/admin/state",
            headers={"Authorization": f"Bearer {self.admin_token}", "Host": "evil.example"},
        )
        self.assertEqual(response.status_code, 403)
        before = await self.state()
        rules = json.loads(json.dumps(before["rules"]))
        rules["rate_limits"][0]["burst"] = 0
        self.assertEqual((await self.change("PUT", "/admin/rules", rules)).status_code, 400)
        self.assertEqual(await self.state(), before)
        await self.create()
        self.assertEqual(
            (
                await self.change("PUT", "/admin/rules", before["rules"], before["revision"])
            ).status_code,
            412,
        )
        response = await self.http.put(
            self.base + "/admin/rules",
            headers={"Authorization": f"Bearer {self.admin_token}"},
            json=before["rules"],
        )
        self.assertEqual(response.status_code, 428)
        self.assertEqual((await self.tool(self.admin_token)).status_code, 401)

    async def test_cli_and_exclusive_state_owner(self):
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(ROOT / "scripts/admin.py"), "--url", self.base, "show"],
            env=self.services.env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), await self.state())
        for command in [
            ["create-user", str(ROOT / "examples/mcp-gateway/user.json")],
            ["apply-rules", str(ROOT / "examples/mcp-gateway/rules.json")],
            ["rotate-token", "alice"],
            ["delete-user", "alice"],
        ]:
            changed = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, str(ROOT / "scripts/admin.py"), "--url", self.base, *command],
                env=self.services.env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(changed.returncode, 0, changed.stderr)
            if command[0] == "create-user":
                token = json.loads(changed.stdout)["issued_token"]
                written = await self.tool(token, "put_note", {"key": "cli", "value": "saved"})
                self.assertEqual(written.status_code, 200)
                self.assertEqual(written.json()["result"]["structuredContent"]["value"], "saved")
            elif command[0] == "rotate-token":
                self.assertEqual((await self.tool(token)).status_code, 401)
                token = json.loads(changed.stdout)["issued_token"]
        self.assertEqual((await self.tool(token)).status_code, 401)
        second = await asyncio.to_thread(
            subprocess.run,
            [str(BINARY)],
            env=self.services.env,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertNotEqual(second.returncode, 0)
        self.assertIn("another gateway owns", second.stderr)

    async def test_rotation_during_body_collection_revokes_admitted_identity(self):
        token = await self.create()
        first_chunk = asyncio.Event()
        release = asyncio.Event()
        body = json.dumps(modern_call("admin_preview", {})).encode()

        async def upload():
            yield body[:10]
            first_chunk.set()
            await release.wait()
            yield body[10:]

        request = asyncio.create_task(
            self.http.post(
                self.services.gateway,
                headers={
                    **modern_headers(token, "admin_preview"),
                    "Content-Type": "application/json",
                },
                content=upload(),
            )
        )
        await asyncio.wait_for(first_chunk.wait(), 5)
        # The server is waiting for the remaining body, after preflight authentication.
        await asyncio.sleep(0.05)
        response = await self.change("POST", "/admin/users/alice/token")
        self.assertEqual(response.status_code, 200)
        release.set()
        result = await request
        self.assertEqual(result.status_code, 401, result.text)
        self.assertEqual((await self.tool(response.json()["issued_token"])).status_code, 200)

    async def test_failed_persistence_and_concurrent_edits(self):
        state = await self.state()
        rules = json.loads(json.dumps(state["rules"]))
        rules["denied_tools"] = ["admin_preview"]
        backup = self.state_file.with_suffix(".saved")
        self.state_file.rename(backup)
        self.state_file.mkdir()
        try:
            self.assertEqual((await self.change("PUT", "/admin/rules", rules)).status_code, 500)
            self.assertEqual(await self.state(), state)
            self.assertEqual((await self.tool(self.services.tokens["operator"])).status_code, 200)
        finally:
            self.state_file.rmdir()
            backup.rename(self.state_file)
        results = await asyncio.gather(
            self.change("PUT", "/admin/rules", rules, state["revision"]),
            self.change("PUT", "/admin/rules", rules, state["revision"]),
        )
        self.assertEqual(sorted(r.status_code for r in results), [200, 412])
        self.assertEqual((await self.state())["revision"], state["revision"] + 1)
        self.assertEqual(json.loads(self.state_file.read_text())["revision"], state["revision"] + 1)

    async def test_real_otlp_spans_and_propagation(self):
        for protocol in ("http/protobuf", "http/json"):
            with self.subTest(protocol=protocol):
                receiver = Receiver()
                self.addCleanup(receiver.close)
                self.services.env.update(
                    OTEL_EXPORTER_OTLP_ENDPOINT=receiver.endpoint,
                    OTEL_EXPORTER_OTLP_PROTOCOL=protocol,
                    OTEL_BSP_SCHEDULE_DELAY="100",
                )
                await self.restart()
                trace_id = "0123456789abcdef0123456789abcdef"
                headers = modern_headers(self.services.tokens["operator"], "admin_preview")
                headers["traceparent"] = f"00-{trace_id}-0123456789abcdef-01"
                result = await self.http.post(
                    self.services.gateway, headers=headers, json=modern_call("admin_preview", {})
                )
                self.assertEqual(result.status_code, 200)
                self.assertEqual(result.headers["x-wayframe-trace-id"], trace_id)
                await self.create("traced-" + protocol.replace("/", "-"))
                denied = await self.tool(self.services.tokens["reader"])
                self.assertEqual(denied.status_code, 403)
                self.assertEqual((await self.tool("invalid-token" * 4)).status_code, 401)
                self.services.upstream_process.terminate()
                await asyncio.to_thread(self.services.upstream_process.wait, 10)
                self.assertEqual(
                    (await self.tool(self.services.tokens["operator"])).status_code, 502
                )
                await self.services.restart_upstream()
                for _ in range(100):
                    spans = receiver.spans()
                    if len([s for s in spans if s["name"] == "gateway.request"]) >= 4:
                        break
                    await asyncio.sleep(0.05)
                self.assertFalse(receiver.errors)
                self.assertTrue(any(s["name"] == "admin.persist" for s in spans))
                self.assertTrue(
                    any(s["name"] == "gateway.request" and s["status"] == 2 for s in spans)
                )
                trace = [s for s in spans if s["trace_id"] == trace_id]
                root = next(s for s in trace if s["name"] == "gateway.request")
                self.assertEqual(root["parent_id"], "0123456789abcdef")
                self.assertEqual(root["attributes"]["stream.outcome"], "complete")
                self.assertEqual(root["attributes"]["mcp.tool"], "admin_preview")
                self.assertTrue(
                    {
                        "gateway.admission",
                        "gateway.body.inspect",
                        "gateway.upstream",
                        "gateway.response.stream",
                    }.issubset({s["name"] for s in trace})
                )
                steps = [s for s in trace if s["name"] == "policy.step"]
                self.assertEqual(
                    {s["attributes"]["block.id"] for s in steps},
                    {
                        "origin",
                        "auth",
                        "session-owner",
                        "rate",
                        "mcp-envelope",
                        "tool-permissions",
                        "mcp-continuation-owner",
                        "tool-rate",
                        "current-grants",
                        "current-continuation-owner",
                        "upstream-path",
                    },
                )
                for span in trace:
                    self.assertGreaterEqual(span["end_ns"], span["start_ns"])
                    if span is not root:
                        by_id = {s["span_id"]: s for s in trace}
                        parent = span
                        while (
                            parent["parent_id"] in by_id and parent["parent_id"] != root["span_id"]
                        ):
                            parent = by_id[parent["parent_id"]]
                        self.assertEqual(parent["parent_id"], root["span_id"])
                        self.assertGreaterEqual(span["start_ns"], root["start_ns"])
                        self.assertLessEqual(span["end_ns"], root["end_ns"], span["name"])
                self.assertTrue(
                    any(s["attributes"].get("denial.code") == "tool_not_permitted" for s in spans)
                )
                for token in self.services.tokens.values():
                    self.assertNotIn(token, json.dumps(spans))
                self.assertNotIn(self.admin_token, json.dumps(spans))
                # Keep normalized evidence if requested by a local verification run.
                if directory := os.environ.get("WAYFRAME_TRACE_REPORT"):
                    report = Path(directory)
                    report.mkdir(parents=True, exist_ok=True)
                    (report / (protocol.replace("/", "-") + ".json")).write_text(
                        json.dumps(spans, indent=2)
                    )


class ManagementFrontendTests(unittest.IsolatedAsyncioTestCase):
    async def test_unix_admin_api(self):
        services = Services(BINARY, unix=True, json_response=True)
        self.addCleanup(services.close)
        admin = secrets.token_hex(32)
        services.env.update(
            WAYFRAME_STATE_FILE=str(services.directory / "state.json"), WAYFRAME_ADMIN_TOKEN=admin
        )
        await services.start()
        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(services.socket), trust_env=False),
            trust_env=False,
        ) as http:
            response = await http.get(
                "http://localhost/admin/state", headers={"Authorization": f"Bearer {admin}"}
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(response.json()["users"]), 2)

    async def test_stdio_admin_listener_changes_live_permissions(self):
        services = Services(BINARY, json_response=True)
        self.addCleanup(services.close)
        await services.start()
        admin = secrets.token_hex(32)
        port = free_port()
        env = {
            **services.env,
            "WAYFRAME_TRANSPORT": "stdio",
            "WAYFRAME_LOCAL_TOKEN": services.tokens["reader"],
            "WAYFRAME_STATE_FILE": str(services.directory / "stdio-state.json"),
            "WAYFRAME_ADMIN_TOKEN": admin,
            "WAYFRAME_ADMIN_LISTEN": f"127.0.0.1:{port}",
        }
        async with Client(
            StdioServerParameters(command=str(BINARY), cwd=str(ROOT), env=env),
            mode="2026-07-28",
            cache=None,
            read_timeout_seconds=10,
        ) as client:
            await client.list_tools()
            async with httpx.AsyncClient(trust_env=False) as http:
                url = f"http://127.0.0.1:{port}"
                headers = {"Authorization": f"Bearer {admin}"}
                response = await http.get(url + "/admin/state", headers=headers)
                self.assertEqual(response.status_code, 200)
                state = response.json()
                reader = next(u for u in state["users"] if u["subject"] == "reader")
                reader["permissions"] = ["notes:admin"]
                headers["If-Match"] = f'"{state["revision"]}"'
                response = await http.put(
                    url + "/admin/users/" + reader["id"], headers=headers, json=reader
                )
                self.assertEqual(response.status_code, 200)
            result = await client.call_tool("admin_preview", {})
            self.assertFalse(result.is_error)
            self.assertEqual(result.structured_content["notes"], 0)
