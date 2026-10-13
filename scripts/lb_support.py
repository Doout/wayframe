"""Two real official-SDK MCP servers with distinct credentials and stored replica identities."""

from __future__ import annotations

import asyncio
import json
import ssl
import subprocess
from contextlib import asynccontextmanager

from mcp_support import Services, sdk_client


@asynccontextmanager
async def cluster(
    binary,
    strategy="round_robin",
    *,
    unix=False,
    tls=False,
    weights=(1, 1),
    quotas=(0, 0),
    enabled=(True, True),
    trace_receiver=None,
):
    a = Services(binary, json_response=True, unix=unix)
    b = Services(binary, json_response=True, tls=tls)
    while b.up_port in (a.up_port, a.gw_port):
        b.close()
        b = Services(binary, json_response=True, tls=tls)
    config = (
        a.config.read_text()
        + f'\n[load_balancer]\nstrategy = "{strategy}"\nfailure_cooldown_ms = 60000\n'
    )
    for index, service in enumerate((a, b)):
        name = "a" if index == 0 else "b"
        config += f'\n[[load_balancer.backends]]\nid = "{name}"\nendpoint = {json.dumps(service.upstream)}\nweight = {weights[index]}\nquota = {quotas[index]}\nenabled = {str(enabled[index]).lower()}\n'
        if name == "b":
            config += 'token_env = "WAYFRAME_BACKEND_B_TOKEN"\n'
        if service.certificate:
            config += f"ca = {json.dumps(str(service.certificate))}\n"
    a.config.write_text(config)
    a.env["WAYFRAME_BACKEND_B_TOKEN"] = b.tokens["upstream"]
    if trace_receiver:
        a.env.update(
            OTEL_EXPORTER_OTLP_ENDPOINT=trace_receiver.endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf",
            OTEL_TRACES_SAMPLER="always_on",
            OTEL_BSP_SCHEDULE_DELAY="100",
        )
    try:
        b.upstream_process = await asyncio.to_thread(
            subprocess.Popen,
            b.server_args,
            env=b.env,
            stdout=b.log,
            stderr=b.log,
            cwd=b.config.parent,
        )
        b.processes.append(b.upstream_process)
        await b.wait_ready(b.upstream.rsplit("/api/mcp", 1)[0] + "/healthz", b.upstream_process)
        await a.start()
        for name, service in (("a", a), ("b", b)):
            verify = (
                ssl.create_default_context(cafile=str(service.certificate))
                if service.certificate
                else True
            )
            async with sdk_client(
                service.upstream, service.tokens["upstream"], verify=verify
            ) as client:
                result = await client.call_tool("put_note", {"key": "replica", "value": name})
                if result.is_error or result.structured_content["value"] != name:
                    raise RuntimeError("replica seed failed")
        yield a, b
    finally:
        a.close()
        b.close()
