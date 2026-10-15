# Read-only MCP fixture

`server.py` implements a small tools-only profile using Python's standard library.
`search_docs` reads five bundled strings; `admin_preview` is simulated. It performs no
model calls, file browsing, network fetches, or shell execution.

From the repository root:

```bash
python3 -m unittest discover -s examples/mcp-server/tests -v
```

Install `requirements-dev.txt` for the JSON Schema check. `bash scripts/demo.sh` exercises
this fixture through the Rust gateway. For real persisted writes, use the
[notes example](../mcp-gateway/README.md).
