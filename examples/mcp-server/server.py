#!/usr/bin/env python3
"""Loopback MCP showcase, not a production SDK or OAuth authorization server.

Implements the small tools-only 2026-07-28 profile and stateless 2025-11-25
compatibility. No shell, filesystem, arbitrary URL, model API, or billable calls.
The separate Rust gateway is the Wayframe application; this is its upstream.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import hmac
import ipaddress
import json
import math
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

MODERN = "2026-07-28"
LEGACY = "2025-11-25"
SUPPORTED = [MODERN, LEGACY]
VERSION = "io.modelcontextprotocol/protocolVersion"
CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
SERVER_INFO = {"name": "wayframe-example-tools", "version": "0.1.0"}
MAX_BODY = 32768
MAX_HEADER_BYTES = 16384

DOCS = (
    {
        "id": "blocks",
        "title": "Composable blocks",
        "text": "Wayframe composes typed native blocks into validated pipelines.",
    },
    {
        "id": "plugins",
        "title": "Sandboxed plugins",
        "text": "Optional Wasm policy plugins run with narrowly granted metadata and clean prewarmed instances.",
    },
    {
        "id": "limits",
        "title": "Traffic controls",
        "text": "Local token buckets limit request rates; stream pacing limits bytes per second.",
    },
    {
        "id": "transport",
        "title": "Explicit connections",
        "text": "Direct calls are the default. Task, Unix socket and HTTPS bridges are explicit boundaries.",
    },
    {
        "id": "framework",
        "title": "Framework, not product",
        "text": "Wayframe is a library-first framework. The MCP gateway is a reference application.",
    },
)
TOOLS = [
    {
        "name": "add",
        "description": "Add two bounded numbers; performs no external action.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "a": {"type": "number", "minimum": -1e12, "maximum": 1e12},
                "b": {"type": "number", "minimum": -1e12, "maximum": 1e12},
            },
            "required": ["a", "b"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
    },
    {
        "name": "search_docs",
        "description": "Search five bundled Wayframe documentation snippets; no filesystem or web access.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "minLength": 1, "maxLength": 100},
                "limit": {"type": "integer", "minimum": 1, "maximum": 5, "default": 3},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
    },
    {
        "name": "admin_preview",
        "description": "Show a harmless simulated maintenance preview. The gateway requires tools:admin; no change is made.",
        "inputSchema": {"type": "object", "additionalProperties": False},
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
    },
]


class ProtocolError(Exception):
    def __init__(self, status: int, code: int, message: str, data: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.data = status, code, message, data


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def parse_json(raw: bytes) -> Any:
    if len(raw) > MAX_BODY:
        raise ProtocolError(413, -32600, "Envelope too large")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite number")),
        )
        pending, count = [(value, 0)], 0
        while pending:
            node, depth = pending.pop()
            count += 1
            if depth > 64 or count > 4096:
                raise ValueError("JSON complexity limit")
            if isinstance(node, str):
                node.encode("utf-8")  # reject escaped lone surrogates too
            elif isinstance(node, dict):
                for key in node:
                    key.encode("utf-8")
                pending.extend((v, depth + 1) for v in node.values())
            elif isinstance(node, list):
                pending.extend((v, depth + 1) for v in node)
            elif isinstance(node, float) and not math.isfinite(node):
                raise ValueError("non-finite number")
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ProtocolError(400, -32700, "Invalid JSON") from exc


def valid_id(value: Any) -> bool:
    return (type(value) is int and -(2**63) <= value <= 2**64 - 1) or (
        isinstance(value, str) and len(value.encode()) <= 256
    )


def decode_name(value: str) -> str:
    if value.startswith("=?base64?") and value.endswith("?="):
        try:
            if len(value) > 4096:
                raise ValueError("encoded name too long")
            return base64.b64decode(value[9:-2], validate=True).decode("utf-8")
        except (ValueError, UnicodeError, binascii.Error) as exc:
            raise ProtocolError(400, -32020, "Header mismatch") from exc
    if value.strip() != value or not all(32 <= ord(c) <= 126 or c == "\t" for c in value):
        raise ProtocolError(400, -32020, "Header mismatch")
    return value


def result_payload(value: dict[str, Any], modern: bool) -> dict[str, Any]:
    result = dict(value)
    if modern:
        result["resultType"] = "complete"
        result["_meta"] = {"io.modelcontextprotocol/serverInfo": SERVER_INFO}
    return result


def tool_result(value: dict[str, Any], modern: bool, *, error: bool = False) -> dict[str, Any]:
    return result_payload(
        {
            "content": [
                {"type": "text", "text": json.dumps(value, ensure_ascii=False, allow_nan=False)}
            ],
            "structuredContent": value,
            "isError": error,
        },
        modern,
    )


def execute_tool(name: str, args: dict[str, Any], modern: bool) -> dict[str, Any]:
    if name == "add":
        if set(args) != {"a", "b"} or any(
            type(args[k]) not in (int, float) or abs(args[k]) > 1e12 or not math.isfinite(args[k])
            for k in ("a", "b")
        ):
            return tool_result(
                {"error": "a and b must be finite numbers in [-1e12, 1e12]"}, modern, error=True
            )
        return tool_result({"sum": args["a"] + args["b"]}, modern)
    if name == "search_docs":
        query, limit = args.get("query"), args.get("limit", 3)
        if (
            set(args) - {"query", "limit"}
            or not isinstance(query, str)
            or not (1 <= len(query) <= 100)
            or type(limit) is not int
            or not 1 <= limit <= 5
        ):
            return tool_result(
                {"error": "query requires 1..100 characters; limit requires an integer 1..5"},
                modern,
                error=True,
            )
        matches = [
            dict(d) for d in DOCS if query.casefold() in (d["title"] + " " + d["text"]).casefold()
        ]
        return tool_result({"matches": matches[:limit]}, modern)
    if name == "admin_preview":
        if args:
            return tool_result({"error": "admin_preview takes no arguments"}, modern, error=True)
        return tool_result(
            {
                "would_change": False,
                "mode": "simulation",
                "message": "No maintenance action was executed.",
            },
            modern,
        )
    raise ProtocolError(400, -32602, "Unknown tool")


class BoundedServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], token: str, *, capacity: int = 32):
        if not ipaddress.ip_address(address[0]).is_loopback:
            raise ValueError("example server binds loopback only")
        if not 32 <= len(token) <= 8192 or any(c.isspace() for c in token):
            raise ValueError("WAYFRAME_UPSTREAM_TOKEN must be a 32+ character secret")
        self.token_digest = hashlib.sha256(token.encode()).digest()
        self.slots = threading.BoundedSemaphore(capacity)
        self.allowed_origins: set[str] = set()
        super().__init__(address, Handler)
        port = self.server_address[1]
        self.allowed_origins.update(
            {
                f"http://localhost:{port}",
                f"http://127.0.0.1:{port}",
                "http://localhost:8080",
                "http://127.0.0.1:8080",
            }
        )

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            try:
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
                )
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "WayframeDemo/0.1"

    def log_message(self, *_):
        # Never log credentials, tool arguments, or complete request URLs.
        pass

    def single(self, name: str) -> str | None:
        values = self.headers.get_all(name, [])
        if len(values) > 1:
            raise ProtocolError(400, -32020, "Duplicate metadata header")
        return values[0] if values else None

    def send_payload(
        self,
        status: int,
        value: Any = None,
        *,
        sse: list[dict[str, Any]] | None = None,
        challenge: bool = False,
        allow: str | None = None,
    ):
        chunks = (
            [
                b"event: message\ndata: "
                + json.dumps(v, ensure_ascii=False, allow_nan=False).encode()
                + b"\n\n"
                for v in sse
            ]
            if sse is not None
            else [
                b""
                if value is None
                else json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
            ]
        )
        self.send_response(status)
        self.send_header(
            "Content-Type", "text/event-stream" if sse is not None else "application/json"
        )
        self.send_header("Content-Length", str(sum(map(len, chunks))))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        if sse is not None:
            self.send_header("X-Accel-Buffering", "no")
        if challenge:
            self.send_header("WWW-Authenticate", 'Bearer realm="wayframe-upstream-demo"')
        if allow:
            self.send_header("Allow", allow)
        self.end_headers()
        self.close_connection = (
            True  # do not leave rejected/unread bodies on a persistent connection
        )
        try:
            for chunk in chunks:
                self.wfile.write(chunk)
                self.wfile.flush()
        except (TimeoutError, BrokenPipeError, ConnectionResetError):
            pass  # request-scoped work is finished; never retry a tool invocation automatically

    def error(self, error: ProtocolError, request_id: Any = None):
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "error": {"code": error.code, "message": error.message},
        }
        if valid_id(request_id):
            payload["id"] = request_id
        if error.data is not None:
            payload["error"]["data"] = error.data
        self.send_payload(error.status, payload)

    def security(self) -> bool:
        if sum(len(k) + len(v) for k, v in self.headers.items()) > MAX_HEADER_BYTES:
            self.send_payload(431, {"error": "header_limit"})
            return False
        port = self.server.server_address[1]
        if self.single("Host") not in (f"127.0.0.1:{port}", f"localhost:{port}"):
            self.send_payload(403, {"error": "host_not_allowed"})
            return False
        origin = self.single("Origin")
        if origin is not None and origin not in self.server.allowed_origins:
            self.send_payload(403, {"error": "origin_not_allowed"})
            return False
        auth = self.single("Authorization") or ""
        fields = auth.split(" ", 1)
        if (
            len(fields) != 2
            or fields[0].lower() != "bearer"
            or len(fields[1]) > 8192
            or not hmac.compare_digest(
                hashlib.sha256(fields[1].encode()).digest(), self.server.token_digest
            )
        ):
            self.send_payload(401, {"error": "unauthorized"}, challenge=True)
            return False
        return True

    def do_GET(self):
        if self.path == "/healthz":
            self.send_payload(200, {"ready": True, "profile": "loopback-development"})
        else:
            self.send_payload(
                405 if self.path == "/mcp" else 404, {"error": "post_required"}, allow="POST"
            )

    def do_DELETE(self):
        self.send_payload(405, {"error": "sessions_not_used"}, allow="POST")

    def do_POST(self):
        request_id = None
        try:
            if self.path != "/mcp":
                self.send_payload(404, {"error": "not_found"})
                return
            if not self.security():
                return
            if self.single("Transfer-Encoding") is not None:
                # The example fixture accepts bounded Content-Length, not arbitrary chunk framing.
                raise ProtocolError(400, -32600, "Content-Length is required by this example")
            media = self.single("Content-Type") or ""
            if (
                media.split(";", 1)[0].strip().lower() != "application/json"
                or self.single("Content-Encoding") is not None
            ):
                raise ProtocolError(415, -32600, "Uncompressed application/json is required")
            accept = self.single("Accept") or ""
            accepted = set()
            for item in accept.split(","):
                pieces = [p.strip() for p in item.split(";")]
                quality = next((p[2:] for p in pieces[1:] if p.startswith("q=")), "1")
                try:
                    if 0 < float(quality) <= 1:
                        accepted.add(pieces[0].lower())
                except ValueError:
                    pass
            if not {"application/json", "text/event-stream"} <= accepted:
                raise ProtocolError(
                    406, -32600, "Accept must include application/json and text/event-stream"
                )
            raw_length = self.single("Content-Length")
            if raw_length is None or not raw_length.isascii() or not raw_length.isdecimal():
                raise ProtocolError(411, -32600, "Valid Content-Length required")
            length = int(raw_length)
            if not 0 < length <= MAX_BODY:
                raise ProtocolError(413, -32600, "Envelope too large or empty")
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ProtocolError(400, -32600, "Incomplete body")
            message = parse_json(raw)
            if (
                not isinstance(message, dict)
                or message.get("jsonrpc") != "2.0"
                or "result" in message
                or "error" in message
            ):
                raise ProtocolError(
                    400,
                    -32600,
                    "One JSON-RPC request is required; batches and client responses are not supported",
                )
            request_id = message.get("id")
            if "id" in message and not valid_id(request_id):
                raise ProtocolError(400, -32600, "Invalid request ID")
            method, params = message.get("method"), message.get("params", {})
            if (
                not isinstance(method, str)
                or not 1 <= len(method.encode()) <= 128
                or not isinstance(params, dict)
            ):
                raise ProtocolError(400, -32600, "Invalid method or params")
            meta = params.get("_meta", {})
            if not isinstance(meta, dict):
                raise ProtocolError(400, -32602, "Invalid metadata")
            version = self.single("MCP-Protocol-Version")
            modern = VERSION in meta or version == MODERN
            if modern:
                if not isinstance(meta.get(VERSION), str) or not isinstance(
                    meta.get(CAPABILITIES), dict
                ):
                    raise ProtocolError(400, -32602, "Required per-request metadata missing")
                body_version = meta[VERSION]
                if body_version != MODERN or version not in (None, MODERN):
                    raise ProtocolError(
                        400,
                        -32022,
                        "Unsupported protocol version",
                        {"supported": SUPPORTED, "requested": version or body_version},
                    )
                if version != body_version or self.single("Mcp-Method") != method:
                    raise ProtocolError(400, -32020, "Header mismatch")
                if "id" not in message:
                    raise ProtocolError(
                        400, -32600, "No client notification extensions are enabled"
                    )
            else:
                if method == "initialize":
                    if (
                        params.get("protocolVersion") != LEGACY
                        or not isinstance(params.get("capabilities"), dict)
                        or not isinstance(params.get("clientInfo"), dict)
                    ):
                        raise ProtocolError(
                            400,
                            -32602,
                            "This compatibility endpoint supports 2025-11-25 initialization",
                        )
                    if version not in (None, LEGACY):
                        raise ProtocolError(
                            400,
                            -32022,
                            "Unsupported protocol version",
                            {"supported": SUPPORTED, "requested": version},
                        )
                elif version != LEGACY:
                    raise ProtocolError(
                        400,
                        -32022,
                        "Unsupported protocol version",
                        {"supported": SUPPORTED, "requested": version},
                    )
                if "id" not in message:
                    if method != "notifications/initialized":
                        raise ProtocolError(
                            400, -32600, "Notification not supported by this example"
                        )
                    self.send_payload(202)
                    return
                if method.startswith("notifications/"):
                    raise ProtocolError(400, -32600, "A notification must not have an ID")
            source = "uri" if method == "resources/read" else "name"
            target = (
                params.get(source)
                if method in ("tools/call", "resources/read", "prompts/get")
                else None
            )
            if method in ("tools/call", "resources/read", "prompts/get") and (
                not isinstance(target, str) or not 1 <= len(target.encode()) <= 1024
            ):
                raise ProtocolError(400, -32602, "Invalid target")
            if modern:
                mirrored = self.single("Mcp-Name")
                if (target is None and mirrored is not None) or (
                    target is not None and (mirrored is None or decode_name(mirrored) != target)
                ):
                    raise ProtocolError(400, -32020, "Header mismatch")
            if method == "server/discover" and modern:
                result = result_payload(
                    {
                        "supportedVersions": SUPPORTED,
                        "capabilities": {"tools": {}},
                        "instructions": "Read-only example tools; the gateway enforces caller permissions.",
                    },
                    True,
                )
            elif method == "initialize" and not modern:
                result = {
                    "protocolVersion": LEGACY,
                    "capabilities": {"tools": {}},
                    "serverInfo": SERVER_INFO,
                }
            elif method == "ping":
                result = result_payload({}, modern)
            elif method == "tools/list":
                if params.get("cursor") is not None:
                    raise ProtocolError(400, -32602, "This finite catalog has no next page")
                result = result_payload({"tools": TOOLS}, modern)
            elif method == "tools/call":
                arguments = params.get("arguments", {})
                if not isinstance(arguments, dict):
                    raise ProtocolError(400, -32602, "Tool arguments must be an object")
                result = execute_tool(target, arguments, modern)
            else:
                raise ProtocolError(404 if modern else 200, -32601, "Method not found")
            response = {"jsonrpc": "2.0", "id": request_id, "result": result}
            progress = meta.get("progressToken")
            if method == "tools/call" and valid_id(progress):
                event = {
                    "jsonrpc": "2.0",
                    "method": "notifications/progress",
                    "params": {
                        "progressToken": progress,
                        "progress": 1,
                        "total": 1,
                        "message": "Read-only operation complete",
                    },
                }
                self.send_payload(200, sse=[event, response])
            else:
                self.send_payload(200, response)
        except ProtocolError as exc:
            self.error(exc, request_id)
        except TimeoutError:
            self.send_payload(408, {"error": "request_timeout"})
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    args = parser.parse_args()
    token = os.environ.get("WAYFRAME_UPSTREAM_TOKEN", "")
    with BoundedServer((args.host, args.port), token) as server:
        server.allowed_origins.update(
            filter(None, os.environ.get("WAYFRAME_ALLOWED_ORIGINS", "").split(","))
        )
        print(
            f"Read-only MCP upstream listening on {args.host}:{server.server_address[1]}",
            flush=True,
        )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
