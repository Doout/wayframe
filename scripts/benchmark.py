#!/usr/bin/env python3
"""Paired direct/upstream versus gateway measurements with correctness checks and raw samples."""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import importlib.metadata
import json
import platform
import secrets
import ssl
import statistics
import subprocess
import time
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psutil
from mcp import Client, StdioServerParameters
from mcp_support import ROOT, Services, modern_call, modern_headers, sdk_client
from otel_receiver import Receiver


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[round((len(ordered) - 1) * fraction)] if ordered else None


def summarize(samples):
    return {
        "p50_ms": percentile(samples, 0.50),
        "p95_ms": percentile(samples, 0.95),
        "p99_ms": percentile(samples, 0.99),
        "mean_ms": statistics.fmean(samples) if samples else None,
    }


def result_data(body, content_type, identifier):
    if content_type.startswith("text/event-stream"):
        messages = []
        for event in body.decode().replace("\r\n", "\n").split("\n\n"):
            data = "\n".join(
                line[5:].lstrip(" ") for line in event.splitlines() if line.startswith("data:")
            )
            if data:
                messages.append(json.loads(data))
        matches = [m for m in messages if m.get("id") == identifier and "result" in m]
        if len(matches) != 1:
            raise ValueError("SSE did not contain exactly one matching result")
        value = matches[0]
    else:
        value = json.loads(body)
    if (
        type(value.get("id")) is not type(identifier)
        or value.get("id") != identifier
        or "error" in value
    ):
        raise ValueError("MCP result correlation/error check failed")
    result = value["result"]
    if result.get("isError"):
        raise ValueError("MCP tool returned an error")
    return result["structuredContent"]


def cpu(process):
    usage = process.cpu_times()
    return usage.user + usage.system


async def measure(services, route, transport_name, size, concurrency, requests):
    processes = {"upstream": psutil.Process(services.upstream_process.pid)}
    if route == "gateway" and transport_name != "stdio":
        processes["gateway"] = psutil.Process(services.gateway_process.pid)
    async with AsyncExitStack() as stack:
        clients = []
        if route == "gateway" and transport_name == "stdio":
            existing = {p.pid for p in psutil.Process().children()}
            env = {
                **services.env,
                "WAYFRAME_TRANSPORT": "stdio",
                "WAYFRAME_LOCAL_TOKEN": services.tokens["reader"],
            }
            if "WAYFRAME_STATE_FILE" in env:
                # Each sequential stdio process owns independent local control-plane state.
                env["WAYFRAME_STATE_FILE"] = str(services.directory / "stdio-state.json")
            for _ in range(concurrency):
                clients.append(
                    await stack.enter_async_context(
                        Client(
                            StdioServerParameters(
                                command=str(services.binary), cwd=str(ROOT), env=env
                            ),
                            mode="2026-07-28",
                            cache=None,
                            read_timeout_seconds=10,
                        )
                    )
                )
            new = [p for p in psutil.Process().children() if p.pid not in existing]
            for index, process in enumerate(new):
                processes[f"gateway_stdio_{index}"] = process
            if len(new) != concurrency:
                raise RuntimeError("could not identify all stdio gateway processes")
        else:
            target = services.upstream if route == "direct" else services.gateway
            uds = services.socket if route == "gateway" else None
            verify = (
                ssl.create_default_context(cafile=str(services.certificate))
                if services.certificate
                else True
            )
            client = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(
                        uds=str(uds) if uds else None, verify=verify, trust_env=False
                    ),
                    limits=httpx.Limits(
                        max_connections=concurrency, max_keepalive_connections=concurrency
                    ),
                    timeout=10,
                    trust_env=False,
                )
            )
            clients = [client] * concurrency

        async def call(worker, identifier):
            if route == "gateway" and transport_name == "stdio":
                result = await clients[worker].call_tool("get_note", {"key": "benchmark"})
                if result.is_error:
                    raise ValueError("stdio MCP tool error")
                data = result.structured_content
                byte_count = len(json.dumps(data).encode())
                first_byte = None
            else:
                token = (
                    services.tokens["upstream"] if route == "direct" else services.tokens["reader"]
                )
                first_byte = None
                started = time.perf_counter_ns()
                body = bytearray()
                async with clients[worker].stream(
                    "POST",
                    target,
                    headers=modern_headers(token, "get_note"),
                    json=modern_call("get_note", {"key": "benchmark"}, identifier),
                ) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        if first_byte is None:
                            first_byte = (time.perf_counter_ns() - started) / 1e6
                        body.extend(chunk)
                        if len(body) > 2 * 1024 * 1024:
                            raise ValueError("benchmark response limit exceeded")
                    data = result_data(
                        bytes(body), response.headers.get("content-type", ""), identifier
                    )
                byte_count = len(body)
            if data != {"key": "benchmark", "value": "x" * size, "revision": 1}:
                raise ValueError("incorrect persisted result")
            return byte_count, first_byte

        await asyncio.gather(*(call(worker, -worker - 1) for worker in range(concurrency)))
        for n in range(50):
            await call(0, -1000 - n)
        before = {name: cpu(process) for name, process in processes.items()}
        rss = {name: process.memory_info().rss for name, process in processes.items()}
        stop = asyncio.Event()

        async def monitor():
            while not stop.is_set():
                for name, process in processes.items():
                    rss[name] = max(rss[name], process.memory_info().rss)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.05)
                except TimeoutError:
                    pass

        monitoring = asyncio.create_task(monitor())
        latencies = []
        errors = []
        transferred = 0
        first_bytes = []
        started = time.perf_counter()

        async def worker(index):
            nonlocal transferred
            for identifier in range(index, requests, concurrency):
                start = time.perf_counter_ns()
                try:
                    byte_count, first_byte = await call(index, identifier + 1)
                    transferred += byte_count
                    if first_byte is not None:
                        first_bytes.append(first_byte)
                    latencies.append((time.perf_counter_ns() - start) / 1e6)
                except (httpx.HTTPError, ValueError, KeyError) as error:
                    errors.append(
                        {
                            "request": identifier + 1,
                            "type": type(error).__name__,
                            "message": str(error),
                        }
                    )

        try:
            await asyncio.gather(*(worker(n) for n in range(concurrency)))
        finally:
            elapsed = time.perf_counter() - started
            stop.set()
            await monitoring
        usage = {
            name: {"cpu_seconds": cpu(process) - before[name], "sampled_peak_rss_bytes": rss[name]}
            for name, process in processes.items()
        }
        return {
            "route": route,
            "attempted": requests,
            "succeeded": len(latencies),
            "errors": errors,
            "elapsed_seconds": elapsed,
            "successful_requests_per_second": len(latencies) / elapsed,
            "response_bytes_per_second": transferred / elapsed,
            "process_usage": usage,
            **summarize(latencies),
            "raw_latency_ms": latencies,
            "first_response_body_byte": summarize(first_bytes),
            "raw_first_response_body_byte_ms": first_bytes,
        }


async def scenario(args, transport_name, response_format, size, concurrency, wasm):
    services = Services(
        args.binary,
        wasm=wasm,
        json_response=response_format == "json",
        tls=transport_name == "https",
        unix=transport_name == "unix",
        benchmark=True,
    )
    receiver = Receiver(retain=0) if args.trace_protocol else None
    case = None
    if args.managed:
        services.env.update(
            WAYFRAME_STATE_FILE=str(services.directory / "state.json"),
            WAYFRAME_ADMIN_TOKEN=secrets.token_hex(32),
        )
    if receiver:
        services.env.update(
            OTEL_EXPORTER_OTLP_ENDPOINT=receiver.endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL=args.trace_protocol,
            OTEL_TRACES_SAMPLER="traceidratio",
            OTEL_TRACES_SAMPLER_ARG=str(args.trace_sample_ratio),
        )
    else:
        # Baselines must not inherit an unrelated exporter from the caller's environment.
        services.env = {k: v for k, v in services.env.items() if not k.startswith("OTEL_")}
    try:
        await services.start()
        async with sdk_client(
            services.gateway, services.tokens["operator"], uds=services.socket
        ) as client:
            result = await client.call_tool("put_note", {"key": "benchmark", "value": "x" * size})
            if result.is_error or result.structured_content["revision"] != 1:
                raise RuntimeError("SDK setup failed to persist benchmark note")
        rounds = []
        for round_number in range(args.rounds):
            order = ("direct", "gateway") if round_number % 2 == 0 else ("gateway", "direct")
            observations = {}
            for route in order:
                observations[route] = await measure(
                    services, route, transport_name, size, concurrency, args.requests
                )
            direct, gateway = observations["direct"], observations["gateway"]
            rounds.append(
                {
                    "round": round_number + 1,
                    **observations,
                    "added_p50_ms": gateway["p50_ms"] - direct["p50_ms"]
                    if gateway["p50_ms"] is not None and direct["p50_ms"] is not None
                    else None,
                }
            )
            print(
                f"{transport_name}/{response_format} size={size} c={concurrency} wasm={wasm} round={round_number + 1}: direct={direct['p50_ms']:.3f}ms gateway={gateway['p50_ms']:.3f}ms errors={len(direct['errors']) + len(gateway['errors'])}",
                flush=True,
            )
        direct_samples = [n for round_ in rounds for n in round_["direct"]["raw_latency_ms"]]
        gateway_samples = [n for round_ in rounds for n in round_["gateway"]["raw_latency_ms"]]
        deltas = [round_["added_p50_ms"] for round_ in rounds if round_["added_p50_ms"] is not None]
        successful = sum(r["gateway"]["succeeded"] for r in rounds)
        gateway_cpu = sum(
            u["cpu_seconds"]
            for r in rounds
            for name, u in r["gateway"]["process_usage"].items()
            if name.startswith("gateway")
        )
        gateway_rss = max(
            sum(
                u["sampled_peak_rss_bytes"]
                for name, u in r["gateway"]["process_usage"].items()
                if name.startswith("gateway")
            )
            for r in rounds
        )
        case = {
            "transport": transport_name,
            "response_format": response_format,
            "value_bytes": size,
            "concurrency": concurrency,
            "policy": "native+wasm" if wasm else "native",
            "gateway_cpu_us_per_success": gateway_cpu * 1e6 / successful if successful else None,
            "gateway_sampled_peak_rss_mib": gateway_rss / (1024 * 1024),
            "gateway_median_requests_per_second": statistics.median(
                r["gateway"]["successful_requests_per_second"] for r in rounds
            ),
            "direct_median_requests_per_second": statistics.median(
                r["direct"]["successful_requests_per_second"] for r in rounds
            ),
            "direct": summarize(direct_samples),
            "gateway": summarize(gateway_samples),
            "median_paired_added_p50_ms": statistics.median(deltas) if deltas else None,
            "min_paired_added_p50_ms": min(deltas) if deltas else None,
            "max_paired_added_p50_ms": max(deltas) if deltas else None,
            "error_count": sum(
                len(r[route]["errors"]) for r in rounds for route in ("direct", "gateway")
            ),
            "rounds": rounds,
        }
        return case
    finally:
        services.close()
        if receiver:
            summary = receiver.summary()
            receiver.close()
            if receiver.errors or (args.trace_sample_ratio == 1 and not summary):
                raise RuntimeError("OTLP benchmark receiver failed or received no spans")
            if case is not None:
                case["trace_step_timings"] = summary


async def run(args):
    report = {
        "kind": "wayframe_overhead",
        "created_at": datetime.now(UTC).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "architecture": platform.machine(),
            "logical_cpus": psutil.cpu_count(),
            "python": platform.python_version(),
            "mcp_sdk": importlib.metadata.version("mcp"),
            "rustc": (
                await asyncio.to_thread(subprocess.check_output, ["rustc", "--version"], text=True)
            ).strip(),
            "commit": (
                await asyncio.to_thread(
                    subprocess.check_output, ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
                )
            ).strip(),
            "working_tree_modified": bool(
                (
                    await asyncio.to_thread(
                        subprocess.check_output,
                        ["git", "status", "--porcelain"],
                        cwd=ROOT,
                        text=True,
                    )
                ).strip()
            ),
            "profile": args.binary.parent.name
            if args.binary.parent.name in ("release", "debug")
            else "unknown",
            "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
            "binary": args.binary.name,
        },
        "method": {
            "management": "persistent" if args.managed else "static bootstrap",
            "trace_protocol": args.trace_protocol,
            "trace_sample_ratio": args.trace_sample_ratio if args.trace_protocol else None,
            "requests_per_round": args.requests,
            "rounds": args.rounds,
            "warmup": "50 sequential requests and one per concurrent worker; not timed",
            "order": "direct/gateway order alternates between rounds",
            "connections": "persistent pooled connections; TLS verified",
            "rate_limit": "enabled at 1,000,000 requests/sec with 1,000,000 burst",
            "wasm_pool": "64 single-use instances, 100ms budget",
            "scope": "closed-loop successful calls including network hops, client serialization/parsing, SDK server, and SQLite read; overhead is a paired path difference, not pure framework CPU cost",
            "stdio": "one sequential gateway process at concurrency 1; baseline uses the same HTTP upstream directly; wire transports differ",
            "rss": "sampled every 50ms; not OS high-water RSS",
        },
        "scenarios": [],
    }
    args.output.mkdir(parents=True, exist_ok=True)
    for transport_name in args.transports.split(","):
        for response_format in args.formats.split(","):
            for size in args.payloads:
                for concurrency in args.concurrency:
                    if transport_name == "stdio" and concurrency != 1:
                        continue
                    for wasm in (False, True) if not args.native_only else (False,):
                        report["scenarios"].append(
                            await scenario(
                                args, transport_name, response_format, size, concurrency, wasm
                            )
                        )
                        (args.output / "mcp-overhead.json").write_text(json.dumps(report, indent=2))
    with (args.output / "mcp-overhead.csv").open("w", newline="") as output:
        fields = [
            "transport",
            "response_format",
            "value_bytes",
            "concurrency",
            "policy",
            "direct_p50_ms",
            "gateway_p50_ms",
            "gateway_p95_ms",
            "gateway_p99_ms",
            "median_paired_added_p50_ms",
            "gateway_median_requests_per_second",
            "gateway_cpu_us_per_success",
            "gateway_sampled_peak_rss_mib",
            "error_count",
        ]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for s in report["scenarios"]:
            writer.writerow(
                {
                    **{k: s[k] for k in fields if k in s},
                    "direct_p50_ms": s["direct"]["p50_ms"],
                    "gateway_p50_ms": s["gateway"]["p50_ms"],
                    "gateway_p95_ms": s["gateway"]["p95_ms"],
                    "gateway_p99_ms": s["gateway"]["p99_ms"],
                }
            )
    if not args.skip_boundaries:
        services = Services(args.binary, tls=True, benchmark=True)
        try:
            await services.start()
            env = {
                **services.env,
                "WAYFRAME_POLICY_HTTPS": services.upstream.replace("/api/mcp", "/policy"),
                "WAYFRAME_POLICY_CA": str(services.certificate),
                "WAYFRAME_POLICY_RUNS": str(args.policy_runs),
            }
            result = await asyncio.to_thread(
                subprocess.run,
                [str(ROOT / "target/release/examples/policy_cost")],
                env=env,
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=True,
            )
            (args.output / "policy-boundaries.json").write_text(result.stdout)
        finally:
            services.close()
        micro = await asyncio.to_thread(
            subprocess.run,
            [str(ROOT / "target/release/examples/pipeline_cost")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        (args.output / "native-pipeline.json").write_text(micro.stdout)
    if any(s["error_count"] for s in report["scenarios"]):
        raise RuntimeError(
            "benchmark recorded failures; inspect error_count/raw errors before interpreting latency"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary", type=Path, default=ROOT / "target/release/wayframe-example-mcp-gateway"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "target/benchmarks")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 16])
    parser.add_argument("--payloads", type=int, nargs="+", default=[64, 4096])
    parser.add_argument("--transports", default="tcp,unix,https,stdio")
    parser.add_argument("--formats", default="json,sse")
    parser.add_argument("--native-only", action="store_true")
    parser.add_argument("--managed", action="store_true", help="enable durable admin/user state")
    parser.add_argument("--trace-protocol", choices=["http/protobuf", "http/json"])
    parser.add_argument("--trace-sample-ratio", type=float, default=1.0)
    parser.add_argument("--skip-boundaries", action="store_true")
    parser.add_argument("--policy-runs", type=int, default=2000)
    args = parser.parse_args()
    if (
        not 0 <= args.trace_sample_ratio <= 1
        or not 100 <= args.requests <= 100000
        or not 1 <= args.rounds <= 20
        or any(c < 1 or c > 128 for c in args.concurrency)
        or any(n < 1 or n > 16384 for n in args.payloads)
    ):
        parser.error("invalid request, round, concurrency, or payload bounds")
    if not set(args.transports.split(",")) <= {"tcp", "unix", "https", "stdio"} or not set(
        args.formats.split(",")
    ) <= {"json", "sse"}:
        parser.error("unknown transport or response format")
    args.binary = args.binary.resolve()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
