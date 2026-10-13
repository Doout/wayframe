# Deployment reloads

Run from a fixed `WAYFRAME_CONFIG` file with persistent administration enabled. The deployment API
reports an ETag and generation. The CLI uses that ETag when requesting a reload:

```bash
python3 scripts/admin.py deployment
python3 scripts/admin.py reload
# Structural checks without opening listeners or changing state:
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml target/debug/wayframe-example-mcp-gateway --validate
WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml target/debug/wayframe-example-mcp-gateway --inspect
```

`POST /admin/reload` requires the deployment or owner role, an empty body, and an `If-Match` generation.
The API reads its original trusted configuration source. Callers cannot supply paths or inline code.
Preparation resolves credentials, builds clients, refreshes JWKS, starts probes/processes, warms
plugins, and compiles policies. Only a complete ready candidate is published. Validation failure,
preparation timeout, or a stale ETag leaves the current generation serving requests.

Responses retain their generation until EOF/drop. Old streams finish with their original pools,
filters and credentials. Legacy session IDs are invalidated on publication; clients reinitialize.
Task and opaque continuation ownership is also generation-scoped, so IDs cannot silently move to
another physical upstream after a configuration change. New TLS handshakes use a prepared replacement
certificate; existing TLS connections retain their negotiated keys.

Restore the previous configuration file and request another reload to roll back. Generation IDs
remain monotonic; there is no reuse of an old ID. Listener addresses, connection/deadline settings,
and bootstrap clients/rules/admin roles require restart. Use the admin API to change persistent
users and rules. Environment values are process inputs; use new configured environment names or
private credential/certificate files for rotation. OAuth secret files are reread during token refresh.

Audit records are fsynced before a mutation attempt and after commitment. A failed commit-record
write can report a failure after the durable state has changed; read the current revision/generation
before retrying. Audit files are bounded and exclusively locked. Archive them during a planned
restart when they approach capacity. The digest chain detects corruption; it is not an externally
anchored, tamper-proof log.
