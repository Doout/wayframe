"""Live socket tests of the Python MCP fixture. These do NOT execute Rust."""

from __future__ import annotations

import http.client
import importlib.util
import json
import socket
import sys
import threading
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("wayframe_demo_server", HERE / "server.py")
server = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = server
spec.loader.exec_module(server)
TOKEN = "test-only-upstream-credential-" + "x" * 40


def envelope(method="tools/call", **params):
    params["_meta"] = {server.VERSION: server.MODERN, server.CAPABILITIES: {}}
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}


class LiveMcpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = server.BoundedServer(("127.0.0.1", 0), TOKEN)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=5)

    def call(self, value=None, *, headers=None, raw=None, method="POST", path="/mcp", remove=()):
        value = value if value is not None else envelope("ping")
        body = raw if raw is not None else json.dumps(value).encode()
        hs = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": server.MODERN,
            "Mcp-Method": value.get("method", "ping") if isinstance(value, dict) else "ping",
        }
        if isinstance(value, dict) and value.get("method") == "tools/call":
            hs["Mcp-Name"] = value.get("params", {}).get("name", "")
        hs.update(headers or {})
        for key in remove:
            hs.pop(key, None)
        con = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            con.request(method, path, body=body, headers=hs)
            response = con.getresponse()
            data = response.read()
            content_type = response.getheader("Content-Type", "")
            result = json.loads(data) if data and "application/json" in content_type else data
            return response.status, dict(response.getheaders()), result
        finally:
            con.close()

    def assert_error(self, status, code, **kw):
        actual, _, body = self.call(**kw)
        self.assertEqual(actual, status, body)
        self.assertEqual(body["error"]["code"], code)

    def test_discovery(self):
        status, _, body = self.call(envelope("server/discover"))
        self.assertEqual(status, 200)
        self.assertIn(server.MODERN, body["result"]["supportedVersions"])
        self.assertEqual(body["result"]["resultType"], "complete")

    def test_tools_are_read_only(self):
        status, _, body = self.call(envelope("tools/list"))
        self.assertEqual(status, 200)
        self.assertEqual(
            {t["name"] for t in body["result"]["tools"]}, {"add", "search_docs", "admin_preview"}
        )
        self.assertTrue(all(t["annotations"]["readOnlyHint"] for t in body["result"]["tools"]))

    def test_add(self):
        status, _, body = self.call(envelope(name="add", arguments={"a": 2, "b": 3}))
        self.assertEqual(status, 200)
        self.assertEqual(body["result"]["structuredContent"], {"sum": 5})

    def test_tool_validation_errors_are_results(self):
        for args in [
            {"a": True, "b": 1},
            {"a": 1},
            {"a": 1e13, "b": 0},
            {"a": 1, "b": 2, "extra": 1},
        ]:
            with self.subTest(args=args):
                status, _, body = self.call(envelope(name="add", arguments=args))
                self.assertEqual(status, 200)
                self.assertTrue(body["result"]["isError"])

    def test_search_is_bounded(self):
        status, _, body = self.call(
            envelope(name="search_docs", arguments={"query": "Wayframe", "limit": 2})
        )
        self.assertEqual(status, 200)
        self.assertLessEqual(len(body["result"]["structuredContent"]["matches"]), 2)

    def test_admin_preview_does_not_mutate_anything(self):
        status, _, body = self.call(envelope(name="admin_preview", arguments={}))
        self.assertEqual(status, 200)
        self.assertFalse(body["result"]["structuredContent"]["would_change"])

    def test_sse_progress_and_final_result(self):
        value = envelope(name="add", arguments={"a": 4, "b": 5})
        value["params"]["_meta"]["progressToken"] = "progress-1"
        status, headers, raw = self.call(value)
        self.assertEqual(status, 200)
        self.assertIn("text/event-stream", headers["Content-Type"])
        frames = [
            json.loads(line[6:]) for line in raw.decode().splitlines() if line.startswith("data: ")
        ]
        self.assertEqual(frames[0]["method"], "notifications/progress")
        self.assertEqual(frames[0]["params"]["progressToken"], "progress-1")
        self.assertEqual(frames[-1]["result"]["structuredContent"], {"sum": 9})

    def test_legacy_initialize_and_notification(self):
        init = {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "initialize",
            "params": {
                "protocolVersion": server.LEGACY,
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            },
        }
        status, _, body = self.call(init, remove=("MCP-Protocol-Version", "Mcp-Method"))
        self.assertEqual(status, 200)
        self.assertEqual(body["result"]["protocolVersion"], server.LEGACY)
        note = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        status, _, body = self.call(
            note, headers={"MCP-Protocol-Version": server.LEGACY}, remove=("Mcp-Method",)
        )
        self.assertEqual(status, 202)
        self.assertEqual(body, b"")

    def test_legacy_tools_call(self):
        value = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "add", "arguments": {"a": 1, "b": 1}},
        }
        status, _, body = self.call(
            value,
            headers={"MCP-Protocol-Version": server.LEGACY},
            remove=("Mcp-Method", "Mcp-Name"),
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["result"]["structuredContent"], {"sum": 2})

    def test_missing_credentials(self):
        status, headers, _ = self.call(remove=("Authorization",))
        self.assertEqual(status, 401)
        self.assertIn("WWW-Authenticate", headers)

    def test_wrong_audience_credential(self):
        status, _, _ = self.call(
            headers={"Authorization": "Bearer reader-token-not-upstream-" + "x" * 40}
        )
        self.assertEqual(status, 401)

    def test_invalid_origin(self):
        self.assertEqual(self.call(headers={"Origin": "https://attacker.invalid"})[0], 403)

    def test_invalid_host(self):
        self.assertEqual(self.call(headers={"Host": "attacker.invalid"})[0], 403)

    def test_valid_origin(self):
        self.assertEqual(self.call(headers={"Origin": f"http://127.0.0.1:{self.port}"})[0], 200)

    def test_mirrored_name_mismatch(self):
        self.assert_error(
            400,
            -32020,
            value=envelope(name="admin_preview", arguments={}),
            headers={"Mcp-Name": "add"},
        )

    def test_mirrored_method_mismatch(self):
        self.assert_error(400, -32020, headers={"Mcp-Method": "tools/list"})

    def test_missing_required_header(self):
        self.assert_error(400, -32020, remove=("Mcp-Method",))

    def test_missing_version_header(self):
        self.assert_error(400, -32020, remove=("MCP-Protocol-Version",))

    def test_missing_capabilities(self):
        v = envelope("ping")
        del v["params"]["_meta"][server.CAPABILITIES]
        self.assert_error(400, -32602, value=v)

    def test_unsupported_version(self):
        v = envelope("ping")
        v["params"]["_meta"][server.VERSION] = "2099-01-01"
        self.assert_error(400, -32022, value=v, headers={"MCP-Protocol-Version": "2099-01-01"})

    def test_base64_name(self):
        self.assertEqual(
            self.call(
                envelope(name="add", arguments={"a": 1, "b": 2}),
                headers={"Mcp-Name": "=?base64?YWRk?="},
            )[0],
            200,
        )

    def test_bad_base64_name(self):
        self.assert_error(
            400, -32020, value=envelope(name="add"), headers={"Mcp-Name": "=?base64?!!!!?="}
        )

    def test_duplicate_json_keys(self):
        self.assert_error(
            400, -32700, raw=b'{"jsonrpc":"2.0","id":1,"method":"ping","method":"tools/list"}'
        )

    def test_nested_duplicate_json_keys(self):
        self.assert_error(
            400,
            -32700,
            raw=b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add","name":"admin_preview"}}',
        )

    def test_batch_is_rejected(self):
        self.assert_error(400, -32600, value=[envelope("ping")])

    def test_invalid_ids(self):
        for id_ in [None, True, 1.5, "x" * 257]:
            with self.subTest(id=id_):
                v = envelope("ping")
                v["id"] = id_
                self.assert_error(400, -32600, value=v)

    def test_unknown_method(self):
        self.assert_error(404, -32601, value=envelope("unknown/method"))

    def test_unknown_tool(self):
        self.assert_error(400, -32602, value=envelope(name="not-a-tool", arguments={}))

    def test_accept_is_required(self):
        self.assert_error(406, -32600, headers={"Accept": "application/json"})

    def test_zero_quality_sse_rejected(self):
        self.assert_error(
            406, -32600, headers={"Accept": "application/json, text/event-stream;q=0"}
        )

    def test_compressed_body_rejected(self):
        self.assert_error(415, -32600, headers={"Content-Encoding": "gzip"})

    def test_body_size_limit(self):
        self.assert_error(413, -32600, raw=b"x" * (server.MAX_BODY + 1))

    def test_invalid_utf8(self):
        self.assert_error(400, -32700, raw=b"\xff")

    def test_get_and_delete_not_session_endpoints(self):
        self.assertEqual(self.call(method="GET")[0], 405)
        self.assertEqual(self.call(method="DELETE")[0], 405)

    def test_nonfinite_json(self):
        self.assert_error(400, -32700, raw=b'{"n":NaN}')

    def test_json_complexity_limit(self):
        with self.assertRaises(server.ProtocolError):
            server.parse_json(b"[" * 65 + b"0" + b"]" * 65)

    def test_duplicate_authorization_headers(self):
        body = json.dumps(envelope("ping")).encode()
        request = (
            f"POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\nAuthorization: Bearer {TOKEN}\r\n"
            f"Authorization: Bearer {TOKEN}\r\nContent-Length: {len(body)}\r\n\r\n"
        ).encode() + body
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as sock:
            sock.sendall(request)
            self.assertIn(b" 400 ", sock.recv(1024).split(b"\r\n")[0])

    def test_tool_schemas_are_valid(self):
        try:
            import jsonschema
        except ImportError:
            self.skipTest("optional jsonschema package not installed")
        for tool in server.TOOLS:
            jsonschema.Draft202012Validator.check_schema(tool["inputSchema"])

    def test_escaped_surrogates_rejected(self):
        self.assert_error(400, -32700, raw=b'{"name":"\\ud800"}')

    def test_huge_tool_number_is_validation_error_not_crash(self):
        status, _, body = self.call(envelope(name="add", arguments={"a": 10**400, "b": 1}))
        self.assertEqual(status, 200)
        self.assertTrue(body["result"]["isError"])

    def demo_client(self, legacy=False):
        spec = importlib.util.spec_from_file_location("example_client", HERE / "client.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.Client(f"http://127.0.0.1:{self.port}/mcp", TOKEN, legacy=legacy)

    def test_modern_client_against_live_fixture(self):
        client = self.demo_client()
        self.assertIn(server.MODERN, client.start()["supportedVersions"])
        self.assertEqual(
            client.request("tools/call", {"name": "add", "arguments": {"a": 20, "b": 22}})[
                "structuredContent"
            ],
            {"sum": 42},
        )

    def test_legacy_client_against_live_fixture(self):
        client = self.demo_client(legacy=True)
        self.assertEqual(client.start()["protocolVersion"], server.LEGACY)
        status, _, events = client.exchange(
            "tools/call",
            {"name": "add", "arguments": {"a": 1, "b": 1}, "_meta": {"progressToken": "p"}},
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(events), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
