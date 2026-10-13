"""Physical routing, health failover, stream reservations and MCP affinity on a live gateway."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import unittest
from pathlib import Path

import httpx
from mcp import Client, StdioServerParameters

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lb_support import cluster
from mcp_support import ROOT, modern_call, modern_headers, sdk_client
from otel_receiver import Receiver

BINARY = Path(
    os.environ.get("WAYFRAME_TEST_BINARY", ROOT / "target/debug/wayframe-example-mcp-gateway")
)


async def read(http, services, *, headers=None):
    return await http.post(
        services.gateway,
        headers=headers or modern_headers(services.tokens["reader"], "get_note"),
        json=modern_call("get_note", {"key": "replica"}),
    )


async def metrics(http, services):
    response = await http.get(
        services.gateway.rsplit("/mcp", 1)[0] + "/metrics",
        headers={"Authorization": f"Bearer {services.tokens['operator']}"},
    )
    if response.status_code != 200:
        raise AssertionError(response.text)
    return response.json()["load_balancer"]["backends"]


async def wait_active(http, services, expected):
    for _ in range(100):
        current = [s["active"] for s in await metrics(http, services)]
        if current == expected:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"active reservations {current}, expected {expected}")


async def initialize(http, services):
    headers = {
        "Authorization": f"Bearer {services.tokens['reader']}",
        "Accept": "application/json, text/event-stream",
    }
    response = await http.post(
        services.gateway,
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "lb-test", "version": "1"},
            },
        },
    )
    if response.status_code != 200:
        raise AssertionError(response.text)
    headers["MCP-Session-Id"] = response.headers["mcp-session-id"]
    headers["MCP-Protocol-Version"] = "2025-11-25"
    initialized = await http.post(
        services.gateway,
        headers=headers,
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    if initialized.status_code != 202:
        raise AssertionError(initialized.text)
    return headers


class LoadBalancingTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_robin_and_weighted_distribution_hit_real_backends(self):
        for strategy, weights, expected in [
            ("round_robin", (1, 1), {"a": 10, "b": 10}),
            ("weighted_round_robin", (1, 3), {"a": 5, "b": 15}),
        ]:
            with self.subTest(strategy=strategy):
                async with (
                    cluster(BINARY, strategy, weights=weights) as (services, _),
                    httpx.AsyncClient(trust_env=False) as http,
                ):
                    observed = {"a": 0, "b": 0}
                    for _ in range(20):
                        response = await read(http, services)
                        self.assertEqual(response.status_code, 200, response.text)
                        observed[response.json()["result"]["structuredContent"]["value"]] += 1
                    self.assertEqual(observed, expected)
                    stats = await metrics(http, services)
                    self.assertEqual({s["id"]: s["selected"] for s in stats}, expected)
                    await wait_active(http, services, [0, 0])
                    before = [s["selected"] for s in stats]
                    denied = await http.post(
                        services.gateway,
                        headers=modern_headers(services.tokens["reader"], "put_note"),
                        json=modern_call("put_note", {"key": "replica", "value": "changed"}),
                    )
                    self.assertEqual(denied.status_code, 403)
                    self.assertEqual([s["selected"] for s in await metrics(http, services)], before)

    async def test_legacy_session_affinity_survives_other_selections(self):
        async with (
            cluster(BINARY) as (services, _),
            sdk_client(services.gateway, services.tokens["reader"], legacy=True) as client,
            httpx.AsyncClient(trust_env=False) as http,
        ):
            values = []
            for _ in range(5):
                await read(http, services)
                result = await client.call_tool("get_note", {"key": "replica"})
                self.assertFalse(result.is_error)
                values.append(result.structured_content["value"])
            self.assertEqual(len(set(values)), 1)
            self.assertEqual(values[0], "a")

    async def test_priority_overflow_and_least_connections_hold_sse_reservations(self):
        for strategy in ("quota_priority", "least_connections"):
            with self.subTest(strategy=strategy):
                async with (
                    cluster(BINARY, strategy, quotas=(1, 1)) as (services, _),
                    httpx.AsyncClient(trust_env=False, timeout=5) as http,
                ):
                    a_headers = await initialize(http, services)
                    stream_a = await http.send(
                        http.build_request("GET", services.gateway, headers=a_headers), stream=True
                    )
                    self.assertEqual(stream_a.status_code, 200)
                    try:
                        await wait_active(http, services, [1, 0])
                        b_headers = await initialize(http, services)
                        stream_b = await http.send(
                            http.build_request("GET", services.gateway, headers=b_headers),
                            stream=True,
                        )
                        self.assertEqual(stream_b.status_code, 200)
                        try:
                            await wait_active(http, services, [1, 1])
                            result = await read(http, services)
                            self.assertEqual(result.status_code, 200)
                            if strategy == "quota_priority":
                                self.assertEqual(
                                    result.json()["result"]["structuredContent"]["value"], "b"
                                )
                                self.assertGreater(
                                    (await metrics(http, services))[1]["overflow"], 0
                                )
                            await wait_active(http, services, [1, 1])
                        finally:
                            await stream_b.aclose()
                    finally:
                        await stream_a.aclose()
                    await wait_active(http, services, [0, 0])

    async def test_failure_cooldown_failover_and_pinned_sessions_fail_closed(self):
        async with (
            cluster(BINARY, "quota_priority") as (services, _),
            httpx.AsyncClient(trust_env=False) as http,
        ):
            session = await initialize(http, services)
            services.upstream_process.terminate()
            await asyncio.to_thread(services.upstream_process.wait, 5)
            failed = await read(http, services)
            self.assertEqual(failed.status_code, 502)
            await wait_active(http, services, [0, 0])
            result = await read(http, services)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["result"]["structuredContent"]["value"], "b")
            stats = await metrics(http, services)
            self.assertFalse(stats[0]["healthy"])
            self.assertEqual(stats[0]["failures"], 1)
            session["X-Wayframe-Backend"] = "b"
            response = await http.post(
                services.gateway,
                headers=session,
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "get_note", "arguments": {"key": "replica"}},
                },
            )
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["error"], "pinned_backend_unavailable")

    async def test_disabled_backends_fail_closed_and_distinct_tls_credentials_work(self):
        async with (
            cluster(BINARY, enabled=(False, False)) as (services, _),
            httpx.AsyncClient(trust_env=False) as http,
        ):
            self.assertEqual((await read(http, services)).status_code, 503)
            self.assertEqual([s["selected"] for s in await metrics(http, services)], [0, 0])
        async with (
            cluster(BINARY, tls=True) as (services, _),
            httpx.AsyncClient(trust_env=False) as http,
        ):
            values = [
                (await read(http, services)).json()["result"]["structuredContent"]["value"]
                for _ in range(4)
            ]
            self.assertEqual(values, ["a", "b", "a", "b"])

    async def test_unix_frontend_and_stdio_route_to_both_backends(self):
        async with (
            cluster(BINARY, unix=True) as (services, _),
            httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(uds=str(services.socket), trust_env=False),
                trust_env=False,
            ) as http,
        ):
            values = [
                (await read(http, services)).json()["result"]["structuredContent"]["value"]
                for _ in range(4)
            ]
            self.assertEqual(values, ["a", "b", "a", "b"])
        async with cluster(BINARY) as (services, _):
            env = {k: v for k, v in services.env.items() if k != "WAYFRAME_UNIX_SOCKET"}
            env.update(WAYFRAME_TRANSPORT="stdio", WAYFRAME_LOCAL_TOKEN=services.tokens["reader"])
            async with Client(
                StdioServerParameters(command=str(BINARY), cwd=str(ROOT), env=env),
                mode="2026-07-28",
                cache=None,
                read_timeout_seconds=10,
            ) as client:
                values = [
                    (await client.call_tool("get_note", {"key": "replica"})).structured_content[
                        "value"
                    ]
                    for _ in range(4)
                ]
                self.assertEqual(set(values), {"a", "b"})

    async def test_otlp_has_backend_strategy_selection_timing_and_no_credentials(self):
        receiver = Receiver()
        self.addCleanup(receiver.close)
        async with (
            cluster(BINARY, trace_receiver=receiver) as (services, other),
            httpx.AsyncClient(trust_env=False) as http,
        ):
            for _ in range(4):
                self.assertEqual((await read(http, services)).status_code, 200)
            for _ in range(100):
                spans = receiver.spans()
                if len([s for s in spans if s["name"] == "gateway.request"]) == 4:
                    break
                await asyncio.sleep(0.02)
            roots = [s for s in spans if s["name"] == "gateway.request"]
            self.assertEqual(
                [s["attributes"]["lb.backend.id"] for s in roots], ["a", "b", "a", "b"]
            )
            steps = [s for s in spans if s["attributes"].get("block.id") == "load-balancer"]
            self.assertEqual(len(steps), 4)
            for span in steps:
                self.assertEqual(span["attributes"]["lb.strategy"], "round_robin")
                self.assertGreaterEqual(span["end_ns"], span["start_ns"])
            for token in [*services.tokens.values(), *other.tokens.values()]:
                self.assertNotIn(token, json.dumps(spans))
            self.assertFalse(receiver.errors)
            if directory := os.environ.get("WAYFRAME_TRACE_REPORT"):
                report = Path(directory)
                report.mkdir(parents=True, exist_ok=True)
                (report / "load-balancer.json").write_text(json.dumps(spans, indent=2))
