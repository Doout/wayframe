#!/usr/bin/env python3
"""Bounded showcase client; no general-purpose MCP SDK or OAuth support."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
from typing import Any
from urllib.parse import urlsplit

MODERN = "2026-07-28"
LEGACY = "2025-11-25"
VERSION = "io.modelcontextprotocol/protocolVersion"
CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
MAX_RESPONSE = 1024 * 1024


class Client:
    def __init__(self, url: str, token: str, *, legacy: bool = False):
        parsed = urlsplit(url)
        if (
            parsed.scheme not in ("http", "https")
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or not parsed.hostname
        ):
            raise ValueError("use a fixed HTTP(S) MCP URL without user info, query, or fragment")
        if not token or any(c.isspace() for c in token):
            raise ValueError("nonempty bearer token required")
        self.url, self.token, self.legacy, self.sequence = parsed, token, legacy, 0

    def exchange(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        notification: bool = False,
        overrides: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], list[dict[str, Any]]]:
        self.sequence += 1
        params = dict(params or {})
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": LEGACY if self.legacy else MODERN,
        }
        if not self.legacy:
            params["_meta"] = {
                **params.get("_meta", {}),
                VERSION: MODERN,
                CAPABILITIES: {},
                "io.modelcontextprotocol/clientInfo": {
                    "name": "wayframe-demo-client",
                    "version": "0.1.0",
                },
            }
            headers["Mcp-Method"] = method
            if method in ("tools/call", "prompts/get", "resources/read"):
                name = params.get("uri" if method == "resources/read" else "name", "")
                if not isinstance(name, str):
                    raise ValueError("target must be a string")
                if (
                    name.strip() != name
                    or not all(32 <= ord(c) <= 126 or c == "\t" for c in name)
                    or (name.startswith("=?base64?") and name.endswith("?="))
                ):
                    import base64

                    name = "=?base64?" + base64.b64encode(name.encode()).decode() + "?="
                headers["Mcp-Name"] = name
        headers.update(overrides or {})
        message = {"jsonrpc": "2.0", "method": method, "params": params}
        if not notification:
            message["id"] = self.sequence
        wire = json.dumps(message, allow_nan=False).encode()
        connection_class = (
            http.client.HTTPSConnection
            if self.url.scheme == "https"
            else http.client.HTTPConnection
        )
        connection = connection_class(self.url.hostname, self.url.port, timeout=20)
        try:
            connection.request("POST", self.url.path or "/mcp", wire, headers)
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE:
                raise ValueError("response exceeds client budget")
            hs = {k.lower(): v for k, v in response.getheaders()}
            if not raw:
                messages = []
            elif "text/event-stream" in hs.get("content-type", ""):
                messages = []
                # The showcase emits one JSON data field per event; join data lines for ordinary SSE clients too.
                for event in raw.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
                    data = "\n".join(
                        line[5:].lstrip(" ")
                        for line in event.splitlines()
                        if line.startswith("data:")
                    )
                    if data:
                        messages.append(json.loads(data))
            else:
                messages = [json.loads(raw)]
            return response.status, hs, messages
        finally:
            connection.close()

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        status, _, messages = self.exchange(method, params)
        if status != 200 or not messages or "result" not in messages[-1]:
            raise RuntimeError(f"{method}: HTTP {status}: {messages}")
        final = messages[-1]
        if final.get("id") != self.sequence:
            raise RuntimeError("response ID does not match request")
        return final["result"]

    def start(self):
        if self.legacy:
            info = self.request(
                "initialize",
                {
                    "protocolVersion": LEGACY,
                    "capabilities": {},
                    "clientInfo": {"name": "wayframe-demo-client", "version": "0.1.0"},
                },
            )
            status, _, messages = self.exchange("notifications/initialized", notification=True)
            if status != 202 or messages:
                raise RuntimeError("initialized notification not accepted")
            return info
        return self.request("server/discover")


def showcase(
    url: str, reader_token: str, operator_token: str | None, *, legacy=False, plugin=False
) -> dict[str, Any]:
    reader = Client(url, reader_token, legacy=legacy)
    discovery = reader.start()
    tools = reader.request("tools/list")["tools"]
    result = reader.request("tools/call", {"name": "add", "arguments": {"a": 20, "b": 22}})
    if result.get("structuredContent") != {"sum": 42}:
        raise RuntimeError("unexpected addition result")
    docs = reader.request(
        "tools/call", {"name": "search_docs", "arguments": {"query": "blocks", "limit": 2}}
    )
    denied = reader.exchange("tools/call", {"name": "admin_preview", "arguments": {}})[0]
    if denied != 403:
        raise RuntimeError(f"reader must not call admin_preview; got {denied}")
    stream_status, _, events = reader.exchange(
        "tools/call",
        {
            "name": "add",
            "arguments": {"a": 1, "b": 2},
            "_meta": {"progressToken": "example-progress"},
        },
    )
    if (
        stream_status != 200
        or len(events) != 2
        or events[-1].get("result", {}).get("structuredContent") != {"sum": 3}
    ):
        raise RuntimeError("SSE progress/final response not preserved")
    report = {
        "protocol": LEGACY if legacy else MODERN,
        "discovery": discovery,
        "tools": [t["name"] for t in tools],
        "addition": result["structuredContent"],
        "documentation_matches": len(docs["structuredContent"]["matches"]),
        "reader_admin_status": denied,
        "sse_events": len(events),
    }
    if not legacy:
        mismatch = reader.exchange(
            "tools/call", {"name": "admin_preview", "arguments": {}}, overrides={"Mcp-Name": "add"}
        )[0]
        if mismatch != 400:
            raise RuntimeError(f"header/body mismatch must be rejected; got {mismatch}")
        report["header_mismatch_status"] = mismatch
    if operator_token:
        operator = Client(url, operator_token, legacy=legacy)
        operator.start()
        status, _, messages = operator.exchange(
            "tools/call", {"name": "admin_preview", "arguments": {}}
        )
        expected = 403 if plugin else 200
        if status != expected:
            raise RuntimeError(
                f"operator admin_preview expected {expected}, got {status}: {messages}"
            )
        report["operator_admin_status"] = status
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080/mcp")
    parser.add_argument("--legacy", action="store_true")
    parser.add_argument("--expect-plugin-denial", action="store_true")
    args = parser.parse_args()
    token = os.environ.get("WAYFRAME_READER_TOKEN")
    if not token:
        parser.error("set WAYFRAME_READER_TOKEN")
    try:
        print(
            json.dumps(
                showcase(
                    args.url,
                    token,
                    os.environ.get("WAYFRAME_OPERATOR_TOKEN"),
                    legacy=args.legacy,
                    plugin=args.expect_plugin_denial,
                ),
                indent=2,
            )
        )
    except (ValueError, RuntimeError, OSError, http.client.HTTPException) as error:
        print(f"Example failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
