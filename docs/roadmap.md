# Roadmap

## Current alpha

Library-first workspace, typed linear blocks/runtime, deployment snapshot primitive, native controls,
optional HTTP forwarding, explicit task/Unix/HTTPS policy boundaries, narrow prewarmed Wasm policy
adapter, and a modern/legacy read-only MCP showcase. See [build status](../BUILD_STATUS.md) for
compiler and integration validation.

## Gate 1 — release qualification

Run `scripts/verify.sh` on the supported CI targets. Qualify the Rust minimum, additional target
architectures, and optional Rust guest build. Scan the locked dependency graph for advisories.
Keep `publish=false` until package names, ownership, and licensing are reviewed.

## Gate 2 — measured and hardened foundation

Add CPU/connection admission at ingress, TLS, cancellation/fault/fuzz tests, and replay-free protocol
behavior. Benchmark native and Wasm controls under equivalent loads; test cold, warm, depleted and
refilling states. Add operational readiness endpoints based on required resources, deployment
health, guest error rates and replenishment—not merely a bound listener.

## Gate 3 — reusable protocol and plugin ecosystem

Introduce stream/frame/block contracts with explicit ownership, codecs and backpressure; graph
branches and protocol adapters; shared-memory/QUIC transports only with explicit trust and delivery
semantics; plugin package schema/signatures/registry; event and control-plane plugin interfaces;
versioned host capabilities; isolated external worker supervision. Do not pretend these exist as
implemented generic transports in v0.1.

## Gate 4 — production MCP/application examples

Replace or supplement the small Python fixture with a verified SDK integration. Implement the
chosen OAuth/OIDC profile and protected-resource metadata, authenticated principal mapping,
identity-aware tool catalog filtering where required, appropriate upstream audience/scope handling,
request/response audit redaction and full server features only when needed. MCP stays optional.

## Gate 5 — distributed operations

Fleet configuration distribution, exact-versus-approximate quota options, health checking/load
balancing, circuit breakers, explicit safe retries, sandbox placement/recovery, and measured rolling
upgrades. Keep data-plane requests independent of management availability and retain known-good
configurations. All new isolation/capability claims require corresponding executable tests.
