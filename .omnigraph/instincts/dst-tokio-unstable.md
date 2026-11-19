---
id: dst-tokio-unstable
trigger: "when compiling, running, or debugging simulation tests in crates/omnigraph-dst"
confidence: 0.95
domain: testing
source: build-rule
action: "Ensure --cfg tokio_unstable is enabled when running tests in crates/omnigraph-dst"
---

# Deterministic Simulation Testing (DST) tokio_unstable Requirement

## Action
When running or building deterministic simulation tests in `crates/omnigraph-dst`, ensure the `--cfg tokio_unstable` flag is active. Execute tests from inside the repository root or the crate directory using `cargo test -p omnigraph-dst --locked`.

## Explanation & Invariant
`omnigraph-dst` relies on Tokio's deterministic time mocking, paused runtime, and unstable synchronization hooks. These APIs are only available under `--cfg tokio_unstable`.

The workspace `.cargo/config.toml` sets this configuration automatically for builds inside the checkout directory. However, setting `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS` in the environment overrides `.cargo/config.toml` and causes DST compilation to fail. Furthermore, the root workspace test gate explicitly excludes `omnigraph-dst` by name; it must be invoked specifically.

## Command Reference
```bash
# Run simulation tests
cargo test -p omnigraph-dst --locked

# Run specific DST test case
cargo test -p omnigraph-dst --locked --test simulation <case_name>
```
