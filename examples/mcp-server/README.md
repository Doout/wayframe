# Read-only MCP fixture

`server.py` is a Python standard-library implementation of a small tools-only MCP profile.
`client.py` exercises the reference gateway and expects gateway authorization decisions.
Neither file is intended as a general MCP SDK or a production HTTP server.

No LLM, model API key, paid service, local file browsing, arbitrary network fetch or shell execution
is involved. `search_docs` uses five bundled strings. `admin_preview` is intentionally a simulation.

From the workspace root, run fixture tests without Rust:

```bash
python3 -m unittest discover -s examples/mcp-server/tests -v
```

Install `requirements-dev.txt` to include the JSON Schema test; fixture-only runs skip that test
when `jsonschema` is unavailable. The server and client use the Python standard library.
For end-to-end gateway instructions see [the gateway example](../mcp-gateway/README.md).

The fixture supports modern `2026-07-28` per-request metadata/discovery and explicitly selected
`2025-11-25` initialize/notification compatibility. See ../../docs/mcp-profile.md for precise
scope, restrictions and official references. Local fixture tests are not Rust gateway tests.
