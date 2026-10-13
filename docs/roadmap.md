# Release qualification

The alpha includes linear policies and typed local graphs; plugin factories and lifecycle contracts;
persistent users/rules, scoped administration and auditing; JWKS/OIDC and upstream OAuth; route groups,
health probes and local circuit/admission controls; TLS/mTLS and framed adapters; response transforms,
scoped single-flight caches; generation reloads; and signed Wasm artifact loading.

Validation includes an independently built Rust guest component, an official Python MCP SDK fixture,
real OTLP protobuf/JSON exports, failure/overload regressions, signature/JWT negative tests, graph
cancellation/joins, and repeatable native/network/open-loop benchmarks. See `scripts/verify.sh` and
`scripts/build-wasm-guest.sh` for the executable checks.

Remaining release work concerns deployment qualification and supported expansion points:

- Decide a supported minimum Rust version and target matrix beyond the pinned tested toolchain.
- Qualify sustained workload/capacity, certificate rotation and failure recovery on the intended deployment.
- Add shared storage/admission providers when a fleet requires coordinated quotas or durable workflow jobs.
- Extend conformance coverage for additional MCP extensions and legacy callback bridges as needed.
- Add coverage-guided fuzzing and production corpus retention alongside the bounded mutation harness.
- Review package names, ownership, licensing and publication before enabling crates.io publication.

Local async workflows are not a durable distributed execution service. The core remains independent
of those deployment choices, and packages retain `publish=false` during this qualification work.
