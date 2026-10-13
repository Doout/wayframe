"""Live gateway reload, native plugins, scoped administration and concurrent policy execution."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import ssl
import sys
import threading
import unittest
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mcp_support import ROOT, Services, free_port, modern_call, modern_headers
from otel_receiver import Receiver

BINARY = Path(
    os.environ.get("WAYFRAME_TEST_BINARY", ROOT / "target/debug/wayframe-example-mcp-gateway")
)


class Extensions(unittest.IsolatedAsyncioTestCase):
    def prepare(self, *, tls=False, json_response=True):
        services = Services(BINARY, json_response=json_response, tls=tls, benchmark=True)
        self.addCleanup(services.close)
        self.owner = secrets.token_hex(32)
        services.env.update(
            WAYFRAME_STATE_FILE=str(services.directory / "state.json"),
            WAYFRAME_ADMIN_TOKEN=self.owner,
        )
        return services

    async def request(self, services, method, path, *, token=None, revision=None, payload=None):
        headers = {"Authorization": f"Bearer {token or self.owner}"}
        if revision is not None:
            headers["If-Match"] = f'"{revision}"'
        async with httpx.AsyncClient(trust_env=False) as http:
            return await http.request(
                method, services.gateway.rsplit("/mcp", 1)[0] + path, headers=headers, json=payload
            )

    async def call(self, services, tool="get_note", arguments=None, *, role="reader", identifier=1):
        value = modern_call(tool, arguments or {})
        value["id"] = identifier
        async with httpx.AsyncClient(trust_env=False) as http:
            return await http.post(
                services.gateway, headers=modern_headers(services.tokens[role], tool), json=value
            )

    async def test_reload_schema_rollback_and_stale_generation(self):
        services = self.prepare()
        await services.start()
        config = services.config.read_text()
        services.config.write_text(
            config
            + '\n[[plugins]]\nid="put-contract"\nkind="tool-schema"\nversion="0.1"\nsettings={tools={put_note={type="object", required=["key","value"],properties={key={type="string"},value={type="string"}},additionalProperties=false}}}\n'
        )
        response = await self.request(services, "POST", "/admin/reload", revision=1)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["generation"], 2)
        self.assertEqual(
            (await self.request(services, "POST", "/admin/reload", revision=1)).status_code, 412
        )
        bad = await self.call(services, "put_note", {"key": "x", "value": 17}, role="operator")
        self.assertEqual(bad.status_code, 400)
        good = await self.call(services, "put_note", {"key": "x", "value": "ok"}, role="operator")
        self.assertEqual(good.status_code, 200)
        services.config.write_text(
            config + '\n[[plugins]]\nid="unknown"\nkind="unregistered"\nversion="1"\n'
        )
        self.assertEqual(
            (await self.request(services, "POST", "/admin/reload", revision=2)).status_code, 400
        )
        self.assertEqual(
            (await self.call(services, "get_note", {"key": "x"})).headers["x-wayframe-generation"],
            "2",
        )
        self.assertEqual(
            (await self.request(services, "GET", "/admin/deployment")).json()["generation"], 2
        )
        records = [
            json.loads(line)["event"]
            for line in (services.directory / "state.audit.jsonl").read_text().splitlines()
        ]
        self.assertEqual([r["outcome"] for r in records], ["intent", "committed"])

    async def test_admin_roles_cannot_cross_mutation_scopes(self):
        services = self.prepare()
        reader, users = secrets.token_hex(32), secrets.token_hex(32)
        services.env.update(READER_ADMIN=reader, USERS_ADMIN=users)
        services.config.write_text(
            services.config.read_text()
            + '\n[[admins]]\nid="auditor"\ntoken_env="READER_ADMIN"\nrole="reader"\n[[admins]]\nid="users"\ntoken_env="USERS_ADMIN"\nrole="users"\n'
        )
        await services.start()
        state = await self.request(services, "GET", "/admin/state", token=reader)
        self.assertEqual(state.status_code, 200)
        profile = {
            "id": "alice",
            "subject": "alice",
            "tenant": "one",
            "permissions": ["notes:read"],
        }
        denied = await self.request(
            services, "POST", "/admin/users", token=reader, revision=1, payload=profile
        )
        self.assertEqual(denied.status_code, 403)
        created = await self.request(
            services, "POST", "/admin/users", token=users, revision=1, payload=profile
        )
        self.assertEqual(created.status_code, 200)
        denied = await self.request(
            services, "PUT", "/admin/rules", token=users, revision=2, payload=state.json()["rules"]
        )
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(
            (
                await self.request(services, "POST", "/admin/reload", token=users, revision=1)
            ).status_code,
            403,
        )
        audit = (services.directory / "state.audit.jsonl").read_text()
        self.assertNotIn(created.json()["issued_token"], audit)
        self.assertNotIn(users, audit)
        self.assertEqual(
            [json.loads(line)["event"]["actor"] for line in audit.splitlines()], ["users", "users"]
        )

    async def test_cache_rewrites_request_ids_and_runs_current_authorization(self):
        services = self.prepare()
        services.config.write_text(
            services.config.read_text()
            + '\n[[plugins]]\nid="note-cache"\nkind="response-cache"\nversion="0.1"\nsettings={tools=["get_note"],ttl_ms=60000}\n'
        )
        await services.start()
        self.assertEqual(
            (
                await self.call(
                    services, "put_note", {"key": "cached", "value": "before"}, role="operator"
                )
            ).status_code,
            200,
        )
        first = await self.call(services, arguments={"key": "cached"}, identifier=51)
        self.assertEqual(first.status_code, 200)
        hit = await self.call(services, arguments={"key": "cached"}, identifier=72)
        self.assertEqual(hit.headers.get("x-wayframe-cache"), "hit")
        self.assertEqual(hit.json()["id"], 72)
        self.assertNotEqual(
            (await self.call(services, arguments={"key": "cached"}, role="operator")).headers.get(
                "x-wayframe-cache"
            ),
            "hit",
        )
        state = (await self.request(services, "GET", "/admin/state")).json()
        state["rules"]["denied_tools"] = ["get_note"]
        self.assertEqual(
            (
                await self.request(
                    services,
                    "PUT",
                    "/admin/rules",
                    revision=state["revision"],
                    payload=state["rules"],
                )
            ).status_code,
            200,
        )
        self.assertEqual((await self.call(services, arguments={"key": "cached"})).status_code, 403)

    async def test_parallel_policy_spans_overlap_and_denial_wins(self):
        services = self.prepare(tls=True)
        receiver = Receiver()
        self.addCleanup(receiver.close)
        barrier = threading.Barrier(2)
        counts = []
        paused = threading.Event()
        resume = threading.Event()

        class Policy(BaseHTTPRequestHandler):
            deny = False
            hold = False

            def log_message(self, *args):
                pass

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                counts.append(request)
                try:
                    barrier.wait(timeout=2)
                except threading.BrokenBarrierError:
                    self.send_error(500)
                    return
                if self.hold:
                    paused.set()
                    resume.wait(timeout=2)
                response = json.dumps(
                    {
                        "version": 1,
                        "request_id": request["request_id"],
                        "status": 403 if self.deny and self.path == "/b" else 0,
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

        server = ThreadingHTTPServer(("127.0.0.1", free_port()), Policy)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(services.server_certificate, services.key)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        config = services.config.read_text()
        for name in ("a", "b"):
            env = f"POLICY_{name.upper()}"
            services.env[env] = secrets.token_hex(32)
            config += f'\n[[async_policies]]\nid="policy-{name}"\nurl="https://127.0.0.1:{server.server_port}/{name}"\ntoken_env="{env}"\nca={json.dumps(str(services.certificate))}\ntimeout_ms=3000\n'
        services.config.write_text(config)
        services.env.update(
            OTEL_EXPORTER_OTLP_ENDPOINT=receiver.endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf",
            OTEL_TRACES_SAMPLER="always_on",
            OTEL_BSP_SCHEDULE_DELAY="100",
        )
        await services.start()
        result = await self.call(services, "admin_preview", role="operator")
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(len(counts), 2)
        self.assertTrue(
            all("arguments" not in item and "authorization" not in item for item in counts)
        )
        first_trace = result.headers["x-wayframe-trace-id"]
        Policy.deny = True
        result = await self.call(services, "admin_preview", role="operator")
        self.assertEqual(result.status_code, 403)
        await asyncio.sleep(0.5)
        nodes = [s for s in receiver.spans() if s["name"] == "workflow.node"]
        self.assertGreaterEqual(len(nodes), 4)
        by_name = {
            s["attributes"].get("node.id"): s
            for s in nodes
            if s["attributes"].get("node.outcome") == "succeeded" and s["trace_id"] == first_trace
        }
        a, b = by_name["policy-a"], by_name["policy-b"]
        self.assertLess(max(a["start_ns"], b["start_ns"]), min(a["end_ns"], b["end_ns"]))
        self.assertIn("node.execution_ns", a["attributes"])
        Policy.deny = False
        Policy.hold = True
        pending = asyncio.create_task(
            self.call(
                services,
                "put_note",
                {"key": "late-revoked", "value": "must-not-write"},
                role="operator",
            )
        )
        try:
            self.assertTrue(await asyncio.to_thread(paused.wait, 2))
            state = (await self.request(services, "GET", "/admin/state")).json()
            profile = next(p for p in state["users"] if p["subject"] == "operator")
            profile["permissions"].remove("notes:write")
            changed = await self.request(
                services,
                "PUT",
                "/admin/users/" + profile["id"],
                revision=state["revision"],
                payload=profile,
            )
            self.assertEqual(changed.status_code, 200)
        finally:
            Policy.hold = False
            resume.set()
        self.assertEqual((await pending).status_code, 403)
        import sqlite3

        with closing(sqlite3.connect(services.database)) as database:
            self.assertEqual(
                database.execute("SELECT COUNT(*) FROM notes WHERE key='late-revoked'").fetchone()[
                    0
                ],
                0,
            )

    async def test_resources_and_prompts_use_current_rules_with_official_sdk(self):
        from mcp_support import sdk_client

        services = self.prepare()
        await services.start()
        state = (await self.request(services, "GET", "/admin/state")).json()
        state["rules"]["resources"] = {"notes://summary": "notes:admin"}
        state["rules"]["prompts"] = {"summarize_notes": "notes:read"}
        self.assertEqual(
            (
                await self.request(
                    services, "PUT", "/admin/rules", revision=1, payload=state["rules"]
                )
            ).status_code,
            200,
        )
        async with sdk_client(services.gateway, services.tokens["operator"]) as client:
            resources = await client.list_resources()
            self.assertEqual([str(r.uri) for r in resources.resources], ["notes://summary"])
            result = await client.read_resource("notes://summary")
            self.assertEqual(json.loads(result.contents[0].text)["notes"], 0)
            prompts = await client.list_prompts()
            self.assertEqual([p.name for p in prompts.prompts], ["summarize_notes"])
            self.assertTrue((await client.get_prompt("summarize_notes")).messages)
        async with sdk_client(services.gateway, services.tokens["reader"]) as client:
            self.assertTrue((await client.list_prompts()).prompts)
            from mcp.shared.exceptions import MCPError

            with self.assertRaises(MCPError):
                await client.read_resource("notes://summary")

    async def test_supervised_stdio_upstream_with_official_sdk_and_progress(self):
        from mcp_support import sdk_client

        services = self.prepare(json_response=False)
        config = (
            services.config.read_text()
            + f'\n[stdio_upstream]\ncommand={json.dumps(sys.executable)}\nargs=[{json.dumps(str(ROOT / "examples/notes-server/server.py"))},"--stdio","--database",{json.dumps(str(services.directory / "process.sqlite3"))}]\ninherit_env=["WAYFRAME_UPSTREAM_TOKEN"]\nmax_in_flight=8\nmax_message_bytes=1048576\n'
        )
        services.config.write_text(config)
        await services.start()
        async with sdk_client(services.gateway, services.tokens["operator"]) as client:
            progress = []

            async def observe(done, total, message):
                progress.append((done, total, message))

            saved = await client.call_tool(
                "put_note", {"key": "stdio", "value": "persisted"}, progress_callback=observe
            )
            self.assertEqual(len(progress), 2)
            self.assertFalse(saved.is_error)
            read = await client.call_tool("get_note", {"key": "stdio"})
            self.assertEqual(read.structured_content["value"], "persisted")
        self.assertEqual((await self.request(services, "GET", "/readyz")).status_code, 200)

    async def test_custom_websocket_binding_runs_the_same_policy(self):
        from websockets.asyncio.client import connect

        services = self.prepare()
        await services.start()
        url = services.gateway.replace("http:", "ws:").rsplit("/mcp", 1)[0] + "/ws"
        async with connect(
            url, additional_headers={"Authorization": f"Bearer {services.tokens['reader']}"}
        ) as wire:
            await wire.send(json.dumps(modern_call("admin_preview", {})))
            rejected = json.loads(await wire.recv())
            self.assertEqual(rejected["status"], 403)
            discovery = modern_call("get_note", {})
            discovery["method"] = "tools/list"
            discovery["params"].pop("name")
            discovery["params"].pop("arguments")
            await wire.send(json.dumps(discovery))
            response = json.loads(await wire.recv())
            self.assertEqual(response["status"], 200)
            self.assertEqual(
                [t["name"] for t in response["messages"][0]["result"]["tools"]], ["get_note"]
            )

    async def test_tenant_rate_rule_shares_a_budget_across_users(self):
        services = self.prepare()
        await services.start()
        tokens = []
        revision = 1
        for name in ("alice", "bob"):
            created = await self.request(
                services,
                "POST",
                "/admin/users",
                revision=revision,
                payload={
                    "id": name,
                    "subject": name,
                    "tenant": "shared",
                    "permissions": ["notes:admin"],
                },
            )
            self.assertEqual(created.status_code, 200)
            tokens.append(created.json()["issued_token"])
            revision = created.json()["revision"]
        state = (await self.request(services, "GET", "/admin/state")).json()
        state["rules"]["rate_limits"].append(
            {
                "id": "tenant-preview",
                "scope": "tenant",
                "tenant": "shared",
                "user": None,
                "tool": "admin_preview",
                "requests_per_second": 0.01,
                "burst": 1,
            }
        )
        changed = await self.request(
            services, "PUT", "/admin/rules", revision=revision, payload=state["rules"]
        )
        self.assertEqual(changed.status_code, 200)
        async with httpx.AsyncClient(trust_env=False) as http:
            statuses = []
            for token in tokens:
                response = await http.post(
                    services.gateway,
                    headers=modern_headers(token, "admin_preview"),
                    json=modern_call("admin_preview", {}),
                )
                statuses.append(response.status_code)
            self.assertEqual(statuses, [200, 429])

    async def test_redaction_and_cors_preflight_run_on_real_responses(self):
        services = self.prepare()
        origin = f"http://127.0.0.1:{services.gw_port}"
        services.config.write_text(
            services.config.read_text()
            + f'\n[[plugins]]\nid="cors"\nkind="cors"\nversion="0.1"\nsettings={{origin={json.dumps(origin)}}}\n[[plugins]]\nid="redact"\nkind="response-redaction"\nversion="0.1"\nsettings={{pointers=["/result/structuredContent/value"]}}\n'
        )
        await services.start()
        self.assertEqual(
            (
                await self.call(
                    services, "put_note", {"key": "private", "value": "secret"}, role="operator"
                )
            ).status_code,
            200,
        )
        async with httpx.AsyncClient(trust_env=False) as http:
            preflight = await http.options(
                services.gateway,
                headers={
                    "Origin": origin,
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "authorization, content-type",
                },
            )
            self.assertEqual(preflight.status_code, 204)
            self.assertEqual(preflight.headers["access-control-allow-origin"], origin)
            denied = await http.options(
                services.gateway,
                headers={
                    "Origin": "https://untrusted.example",
                    "Access-Control-Request-Method": "POST",
                },
            )
            self.assertEqual(denied.status_code, 403)
            headers = modern_headers(services.tokens["reader"], "get_note")
            headers["Origin"] = origin
            response = await http.post(
                services.gateway, headers=headers, json=modern_call("get_note", {"key": "private"})
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["access-control-allow-origin"], origin)
            self.assertEqual(response.json()["result"]["structuredContent"]["value"], "[redacted]")

    async def test_task_and_opaque_continuation_owner_cannot_be_changed(self):
        import subprocess

        services = self.prepare()
        requests = []

        class Backend(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(message)
                method = message["method"]
                if method == "tools/call" and message["params"]["arguments"].get("mode") == "task":
                    result = {
                        "resultType": "task",
                        "taskId": "job-one",
                        "status": "working",
                        "ttlMs": 300000,
                        "pollIntervalMs": 1,
                    }
                elif (
                    method == "tools/call"
                    and message["params"]["arguments"].get("mode") == "ask"
                    and "requestState" not in message["params"]
                ):
                    result = {"resultType": "input_required", "requestState": "opaque-state-one"}
                else:
                    result = {
                        "resultType": "complete",
                        "content": [{"type": "text", "text": "done"}],
                    }
                body = json.dumps(
                    {"jsonrpc": "2.0", "id": message["id"], "result": result}
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", services.up_port), Backend)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        process = await asyncio.to_thread(
            subprocess.Popen,
            [str(BINARY)],
            env=services.env,
            cwd=ROOT,
            stdout=services.log,
            stderr=services.log,
        )
        services.processes.append(process)
        await services.wait_ready(services.gateway.rsplit("/mcp", 1)[0] + "/healthz", process)
        state = (await self.request(services, "GET", "/admin/state")).json()
        state["rules"]["tasks_permission"] = "notes:read"
        self.assertEqual(
            (
                await self.request(
                    services, "PUT", "/admin/rules", revision=1, payload=state["rules"]
                )
            ).status_code,
            200,
        )
        async with httpx.AsyncClient(trust_env=False) as http:

            async def send(value, role):
                headers = modern_headers(services.tokens[role], "get_note")
                headers["Mcp-Method"] = value["method"]
                if value["method"].startswith("tasks/"):
                    headers.pop("Mcp-Name", None)
                return await http.post(services.gateway, headers=headers, json=value)

            request = modern_call("get_note", {"mode": "ask"})
            reply = await send(request, "reader")
            self.assertEqual(reply.status_code, 200)
            request["params"]["requestState"] = reply.json()["result"]["requestState"]
            self.assertEqual((await send(request, "operator")).status_code, 403)
            self.assertEqual((await send(request, "reader")).status_code, 200)
            request["params"]["arguments"]["changed"] = True
            self.assertEqual((await send(request, "reader")).status_code, 403)
            request = modern_call("get_note", {"mode": "task"})
            request["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = {
                "extensions": {"io.modelcontextprotocol/tasks": {}}
            }
            reply = await send(request, "reader")
            self.assertEqual(reply.status_code, 200)
            poll = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tasks/get",
                "params": {
                    "taskId": reply.json()["result"]["taskId"],
                    "_meta": request["params"]["_meta"],
                },
            }
            self.assertEqual((await send(poll, "operator")).status_code, 403)
            self.assertEqual((await send(poll, "reader")).status_code, 200)
            poll["params"]["taskId"] = "unknown"
            self.assertEqual((await send(poll, "reader")).status_code, 403)
        self.assertEqual(len(requests), 4)

    async def test_official_sdk_subscription_acknowledges_and_releases_its_stream(self):
        from mcp_support import sdk_client

        services = self.prepare(json_response=False)
        await services.start()
        async with (
            sdk_client(services.gateway, services.tokens["reader"]) as client,
            client.listen(tools_list_changed=True) as subscription,
        ):
            self.assertIsNotNone(subscription.honored)
        async with httpx.AsyncClient(trust_env=False) as http:
            base = services.gateway.rsplit("/mcp", 1)[0]
            for _ in range(100):
                response = await http.get(
                    base + "/metrics",
                    headers={"Authorization": f"Bearer {services.tokens['operator']}"},
                )
                active = response.json()["active"]
                if active == 0:
                    break
                await asyncio.sleep(0.02)
            self.assertEqual(active, 0)
