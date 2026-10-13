# Build status

Wayframe is an early alpha. Validation is performed with `bash scripts/verify.sh`; local reports
are written to ignored `verification/local/`. GitHub Actions runs that script on Linux and macOS.

## Local validation — October 13, 2026

Environment: macOS on Apple Silicon, Rust 1.99.0, Python 3.14.7, Wasmtime 49.0.2.

`bash scripts/verify.sh` passed locally with:

- Python/Rust formatting, Python lint, and structural source checks.
- Native facade compilation with no optional features.
- All 40 Rust unit tests across workspace features/targets and all doctest targets.
- All 42 Python fixture tests, including JSON Schema validation; none skipped.
- Clippy with warnings treated as errors.
- Live Rust gateway smoke scenarios with native controls and with the Wasm plugin. Both exercised
  modern/legacy MCP requests, SSE, tool permissions, authentication, separate upstream credentials,
  and burst rate limiting.

A Trivy filesystem scan of the locked workspace dependencies and project sources found no known
vulnerabilities or secrets on October 13, 2026. This is a dated scan result, not a security guarantee.
The local verification log and scan report are retained under `verification/local/`.

The source download contained component fixtures using legacy WAT reference syntax. Both fixtures
now use the current component syntax, and the sample plugin manifest's SHA-256 matches its artifact.
Cargo.lock records the resolved workspace dependencies; verification uses `--locked`.

## Remaining qualification

- Linux GitHub Actions execution and third-party MCP SDK interoperability.
- The optional Rust guest build and conversion to a Wasm component; host tests use the bundled WAT.
- Minimum supported Rust version and targets beyond the local Apple Silicon build.
- Ongoing dependency advisory checks, fuzzing, load/fault testing, and independent security review.
- Native and Wasm performance measurements.

Passing local checks does not establish production readiness. See [security boundaries](SECURITY.md)
and [the roadmap](docs/roadmap.md). Packages retain `publish=false` pending release qualification.

## Reproduce

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
bash scripts/verify.sh
```
