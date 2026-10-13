# Roadmap

The alpha implements typed linear pipelines, deployment snapshots, native controls, HTTP
forwarding, task/Unix/HTTPS policy bridges, Wasm policies, and a read-only MCP example.

Release qualification still requires minimum Rust version and target support decisions,
Rust guest build validation, and third-party MCP SDK interoperability tests. Packages retain
`publish=false` until package names, ownership, and licensing are reviewed.

Planned work:

- Production ingress and resource controls: TLS, connection admission, readiness based on required
  resources, and fleet-wide quota semantics.
- Reliability and performance: cancellation, overload, reload and worker-failure testing; fuzzing;
  comparable native and Wasm benchmarks across cold, warm, depleted, and refilling states.
- Plugin distribution: package signatures, registries, versioned host capabilities, and isolated
  external-worker supervision.
- Protocol support: explicit stream/frame ownership, codecs, backpressure, and graph branches.
- MCP integration: verified SDK examples, OAuth/OIDC, protected-resource metadata, identity-aware
  tool discovery, and redacted request/response auditing.
- Distributed operations: configuration distribution, health checks, load balancing, circuit
  breakers, and retries with explicit idempotency rules.
