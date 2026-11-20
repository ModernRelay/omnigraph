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
When running or building deterministic simulation tests in `crates/omnigraph-dst`, ensure the `--cfg tokio_unstable` flag is active and the pool-quiescing environment trio (`RAYON_NUM_THREADS=1`, `LANCE_CPU_THREADS=1`, `LANCE_DETERMINISTIC_BACKOFF=1`) is supplied. The canonical invocation is from the crate directory: `cd crates/omnigraph-dst && cargo test --locked`.

## Explanation & Invariant
`omnigraph-dst` relies on Tokio's deterministic time mocking, paused runtime, and unstable synchronization hooks. These APIs are only available under `--cfg tokio_unstable`.

The workspace `.cargo/config.toml` sets `--cfg tokio_unstable` automatically for builds inside the checkout directory. However, setting `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS` in the environment overrides `.cargo/config.toml` and causes DST compilation to fail.

Furthermore, `crates/omnigraph-dst` asserts at process start that the pool-quiescing trio is active (`require_pool_env`). The crate's local `.cargo/config.toml` supplies these `[env]` variables when cargo is invoked from `crates/omnigraph-dst`. If invoking cargo from the workspace root, export the trio explicitly. The root workspace test gate excludes `omnigraph-dst` by name; it must be invoked specifically.

## Command Reference
```bash
# Run simulation tests from the crate directory (preferred; supplies pool-quiescing [env] trio)
cd crates/omnigraph-dst && cargo test --locked

# Or run from workspace root with explicit pool trio
RAYON_NUM_THREADS=1 LANCE_CPU_THREADS=1 LANCE_DETERMINISTIC_BACKOFF=1 cargo test -p omnigraph-dst --locked

# Run specific DST test case
cd crates/omnigraph-dst && cargo test --locked --test simulation <case_name>
```
