#!/usr/bin/env python3
"""A persistent notes service built with the official MCP SDK."""

from __future__ import annotations

import argparse
import asyncio
import hmac
import os
import sqlite3
from contextlib import closing
from pathlib import Path

import uvicorn
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from starlette.responses import JSONResponse


def create_server(database: Path) -> MCPServer:
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS notes (key TEXT PRIMARY KEY, value TEXT NOT NULL, revision INTEGER NOT NULL)"
        )
    server = MCPServer("wayframe-notes", version="0.1.0")

    def key_check(key: str) -> None:
        if not 1 <= len(key) <= 128 or any(ord(c) < 32 for c in key):
            raise ValueError("key must contain 1..128 printable characters")

    def read(key: str) -> dict[str, str | int | bool]:
        key_check(key)
        with closing(sqlite3.connect(database, timeout=5)) as connection, connection:
            row = connection.execute(
                "SELECT value, revision FROM notes WHERE key = ?", (key,)
            ).fetchone()
        if row is None:
            raise ValueError("note not found")
        return {"key": key, "value": row[0], "revision": row[1]}

    @server.tool(structured_output=True)
    async def get_note(key: str) -> dict[str, str | int | bool]:
        """Read a stored note and its revision."""
        return await asyncio.to_thread(read, key)

    @server.tool(structured_output=True)
    async def put_note(key: str, value: str, ctx: Context) -> dict[str, str | int | bool]:
        """Create or update a note, returning the persisted revision."""
        key_check(key)
        if len(value.encode()) > 64 * 1024:
            raise ValueError("value exceeds 64 KiB")
        await ctx.report_progress(0, 1, "Saving note")

        def write() -> dict[str, str | int | bool]:
            with closing(sqlite3.connect(database, timeout=5)) as connection, connection:
                connection.execute(
                    "INSERT INTO notes VALUES (?, ?, 1) ON CONFLICT(key) DO UPDATE SET value = excluded.value, revision = notes.revision + 1",
                    (key, value),
                )
                row = connection.execute(
                    "SELECT value, revision FROM notes WHERE key = ?", (key,)
                ).fetchone()
            return {"key": key, "value": row[0], "revision": row[1]}

        result = await asyncio.to_thread(write)
        await ctx.report_progress(1, 1, "Note saved")
        return result

    @server.tool(structured_output=True)
    async def delete_note(key: str) -> dict[str, str | int | bool]:
        """Delete one note. Returns whether a stored record was removed."""
        key_check(key)

        def delete() -> dict[str, str | int | bool]:
            with closing(sqlite3.connect(database, timeout=5)) as connection, connection:
                changed = connection.execute("DELETE FROM notes WHERE key = ?", (key,)).rowcount
            return {"key": key, "deleted": changed == 1}

        return await asyncio.to_thread(delete)

    @server.tool(structured_output=True)
    async def admin_preview() -> dict[str, str | int | bool]:
        """Read the database's note count and stored payload size for maintenance planning."""

        def preview() -> dict[str, str | int | bool]:
            with closing(sqlite3.connect(database)) as connection, connection:
                row = connection.execute(
                    "SELECT COUNT(*), COALESCE(SUM(length(CAST(value AS BLOB))), 0) FROM notes"
                ).fetchone()
            return {"notes": row[0], "stored_bytes": row[1]}

        return await asyncio.to_thread(preview)

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--path", default="/api/mcp")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    args = parser.parse_args()
    token = os.environ["WAYFRAME_UPSTREAM_TOKEN"]
    if len(token) < 32:
        raise ValueError("upstream token must contain at least 32 characters")
    server = create_server(args.database)
    app = server.streamable_http_app(streamable_http_path=args.path, json_response=args.json)

    async def authenticated(scope, receive, send):
        if scope["type"] == "http":
            if scope["path"] == "/healthz":
                await JSONResponse({"alive": True})(scope, receive, send)
                return
            headers = [v for k, v in scope["headers"] if k == b"authorization"]
            if len(headers) != 1 or not hmac.compare_digest(headers[0], f"Bearer {token}".encode()):
                await JSONResponse({"error": "unauthorized"}, status_code=401)(scope, receive, send)
                return
            if scope["path"] == "/policy":
                from starlette.requests import Request

                request = Request(scope, receive)
                body = await request.body()
                if len(body) > 16384:
                    await JSONResponse({"error": "body_limit"}, status_code=413)(
                        scope, receive, send
                    )
                    return
                import json

                value = json.loads(body)
                await JSONResponse({"version": 1, "request_id": value["request_id"], "status": 0})(
                    scope, receive, send
                )
                return
        await app(scope, receive, send)

    uvicorn.run(
        authenticated,
        host="127.0.0.1",
        port=args.port,
        access_log=False,
        log_level="warning",
        ssl_certfile=args.tls_cert,
        ssl_keyfile=args.tls_key,
    )


if __name__ == "__main__":
    main()
