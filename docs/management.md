# Users and controls

The reference gateway has an optional admin API and Python CLI. Users carry explicit permissions;
rules map tools to required permissions, disable selected tools, and set per-user token buckets.
Changes take effect on subsequent checks without restarting the gateway.

## Enable management

Generate separate development credentials and keep the output private:

```bash
umask 077
python3 scripts/dev_tokens.py --admin > .env
source .env
export WAYFRAME_CONFIG=examples/mcp-gateway/notes.toml
export WAYFRAME_STATE_FILE="$PWD/var/gateway-state.json"
cargo run --locked -p wayframe-example-mcp-gateway
```

Start the notes upstream separately as described in [gateway setup](../examples/mcp-gateway/README.md).
Both `WAYFRAME_STATE_FILE` and `WAYFRAME_ADMIN_TOKEN` are required for management. The admin token
must differ from upstream and client credentials. With neither setting, management is disabled
and the existing TOML credentials and rules remain available.

The first start seeds `bootstrap-1`, `bootstrap-2`, etc. from TOML clients and their token environment
variables. Later starts restore users/rules from the state file; bootstrap client secrets are no
longer required. TOML still configures the upstream, listener, hard request limits, and origins.
The admin and upstream secrets remain environment configuration. Rotate the admin secret by
restarting with a new value.

State holds SHA-256 digests of high-entropy bearer tokens, never their plaintext. Files are mode
0600 on Unix, bounded to 4 MiB, and replaced atomically after validation. One process exclusively
owns each state file. Use an operator-owned directory, preserve backups, and keep it outside
`target/`; Cargo cleanup must not delete users. This is a local control plane, not a distributed
database or fleet-wide quota service.

## CLI

```bash
python3 scripts/admin.py --url http://127.0.0.1:8080 show
python3 scripts/admin.py create-user examples/mcp-gateway/user.json
python3 scripts/admin.py rotate-token alice
python3 scripts/admin.py update-user examples/mcp-gateway/user.json
python3 scripts/admin.py apply-rules examples/mcp-gateway/rules.json
python3 scripts/admin.py delete-user alice
```

`WAYFRAME_ADMIN_TOKEN` is read from the environment. Creation and rotation return `issued_token`
once; protect captured output and supply that token to the user's MCP client. `show` never returns
credentials or digests. Disable a user by setting `enabled` to `false` in their full profile and
running `update-user`; adjust `permissions` in the same way. Identity (`id`, `tenant`, `subject`)
is immutable. Deletion also removes that user's rate rules. Existing admitted work can finish;
revocation does not cancel already running upstream operations or established streams. Credentials
are checked again after body collection, so rotation also rejects an old token whose request was
admitted but has not yet reached dispatch.

The sample rules preserve the four notes tool grants and add a `put_note` budget. Customize the
entire document before applying it. Rules always add mandatory controls; they cannot override a
permission or Wasm denial. Unknown tools and unsupported MCP operations remain denied. Discovery
still returns the upstream catalog.

A rate rule's `user: null` creates an independent bucket for every user. A specific user ID narrows
that rule. `tool: null` applies before body collection, including lifecycle and denied requests;
a tool name applies after inspected tool authorization. All matching rules must have capacity.
Rates must be positive and at most 1,000,000/sec; bursts are 1..1,000,000. Up to 128 rules and 1024
users are supported. Unrelated edits preserve bucket balances; changed budgets cap existing tokens.
Buckets reset on restart, so restarting can grant a fresh burst. Quotas multiply across processes.

TCP and Unix frontends expose the same API. For stdio, optionally bind a separate loopback admin
listener with `WAYFRAME_ADMIN_LISTEN=127.0.0.1:8081`; use that URL in the CLI. An explicit admin
listener accepts its exact bound Host. The CLI supports TCP HTTP(S); Unix operators can use an
HTTP client with Unix socket support. Remote administration requires a TLS ingress.

## API

Every endpoint requires the separate admin bearer token. Browser Origin headers are rejected;
Host must match the configured listener. Responses use `Cache-Control: no-store`.

| Method and path | Body / behavior |
| --- | --- |
| `GET /admin/state` | Redacted users, rules, revision, and an ETag |
| `POST /admin/users` | Full user profile; issue a new random token |
| `PUT /admin/users/{id}` | Full profile; update permissions/enabled |
| `DELETE /admin/users/{id}` | No body; revoke and remove user |
| `POST /admin/users/{id}/token` | No body; rotate credential |
| `PUT /admin/rules` | Complete rules document |

Writes require `If-Match: "<revision>"` from the last read. Missing preconditions return 428;
stale edits return 412. Invalid input returns 400. Auth failure is 401; disallowed origin/host is
403. Bodies are bounded to 64 KiB and duplicate JSON keys are rejected. The CLI reads the revision
before writing and reports conflicts rather than silently overwriting a newer change.

Persistence happens off the request executor before an immutable snapshot is published. A failed
write leaves the active configuration unchanged. Once persistence starts, the transaction finishes
even if the admin client disconnects; re-read state after an uncertain response. Creation is not
idempotent: if its response is lost, inspect the user and rotate their token. File/schema validation
and the process lock fail startup rather than falling back to bootstrap state.

See [tracing](observability.md) for request steps and management persistence spans.
