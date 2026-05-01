# Testing

This file is the always-on map of the test surface. **Consult it before every task** so you know what tests already cover the area you're about to change, what helpers to reuse, and where a new test belongs. The architectural invariant *"tests at every boundary, not just end-to-end"* lives in [docs/invariants.md §VII.33](invariants.md).

## Where tests live, per crate

| Crate | Path | Style |
|---|---|---|
| `omnigraph` (engine) | `crates/omnigraph/tests/` | Integration tests (15 files), fixture-driven, share `tests/helpers/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | `cli.rs` (unit-ish), `system_local.rs`, `system_remote.rs`, share `tests/support/mod.rs` |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | `server.rs` (HTTP-level), `openapi.rs` (OpenAPI drift / regeneration) |
| `omnigraph-compiler` | mostly in-source `#[cfg(test)] mod tests` | Parser, type-checker, IR lowering, lint |

The engine's `tests/` is the principal coverage surface; most graph-shaped behavior is exercised there.

## Engine integration tests (`crates/omnigraph/tests/`)

| File | Covers |
|---|---|
| `end_to_end.rs` | Full init → load → query/mutate flow |
| `branching.rs` | Branch create / list / delete, lazy fork |
| `runs.rs` | Direct-publish writes: cancellation, concurrent-writer CAS, multi-statement atomicity, MR-794 staged-write rewire (D₂ rejection, insert+update coalesce, multi-append coalesce, partial-failure recovery, load RI/cardinality recovery) |
| `staged_writes.rs` | TableStore staged-write primitives (`stage_append`, `stage_merge_insert`, `commit_staged`, `scan_with_staged`, `count_rows_with_staged`) — primitive-level only; engine code uses the in-memory `MutationStaging` accumulator instead |
| `lifecycle.rs` | Repo lifecycle, schema state |
| `point_in_time.rs` | Snapshots, time travel (`snapshot_at_version`, `entity_at`) |
| `changes.rs` | `diff_between` / `diff_commits` |
| `consistency.rs` | Cross-table snapshot isolation, atomic publish |
| `schema_apply.rs` | Migration plan + apply, schema-apply lock |
| `search.rs` | FTS / vector / hybrid (`bm25`, `nearest`, `rrf`) |
| `traversal.rs` | `Expand`, variable-length hops, anti-join |
| `aggregation.rs` | `count`, `sum`, `avg`, `min`, `max` |
| `export.rs` | NDJSON streaming export filters |
| `s3_storage.rs` | S3-backed repo (skipped unless `OMNIGRAPH_S3_TEST_BUCKET` is set) |
| `lance_version_columns.rs` | Per-row `_row_last_updated_at_version` behavior |
| `failpoints.rs` | Failure-injection coverage (gated on `failpoints` feature) |

## Fixtures

`crates/omnigraph/tests/fixtures/` holds the canonical schema (`.pg`), seed data (`.jsonl`), and queries (`.gq`) shared across tests. Reuse these before inventing new ones — the helpers harness already knows how to load them.

## Test helpers

- **Engine** — `crates/omnigraph/tests/helpers/mod.rs`: `init_and_load()` (bootstrap a temp repo + load standard fixture), `snapshot_main()`, `snapshot_branch()`, query/mutation runners, row collection and counting. Use these instead of hand-rolling.
- **CLI** — `crates/omnigraph-cli/tests/support/mod.rs`: `Command`-style wrapper for invoking `omnigraph`, server-process spawning, fixture resolution, output assertion helpers.
- **Server** — no shared helpers; server tests call the `Omnigraph` engine API directly and exercise endpoints over the wire.

> Note: there is **no `MemStorage` or in-memory backend** today. Tests use `tempfile::tempdir()` for local FS. If you find yourself needing one for layer isolation, that's an architectural ask — see [docs/invariants.md §VII.34](invariants.md) (reference impl + test impl per trait).

## Failpoints (fault injection)

- Cargo feature: `failpoints = ["dep:fail", "fail/failpoints"]` (in `crates/omnigraph/Cargo.toml`).
- Wrapper: `crates/omnigraph/src/failpoints.rs` exposes `maybe_fail("name")` and `ScopedFailPoint` for tests.
- Call sites are inserted at sensitive transaction boundaries (branch create, graph publish commit, etc.).
- Activated tests: `crates/omnigraph/tests/failpoints.rs`. Run with `cargo test -p omnigraph-engine --features failpoints --test failpoints`.

## RustFS / S3 integration

CI runs three S3-backed tests against a containerized RustFS server (`.github/workflows/ci.yml` → `rustfs_integration` job):

- `cargo test -p omnigraph-engine --test s3_storage`
- `cargo test -p omnigraph-server --test server server_opens_s3_repo_directly_and_serves_snapshot_and_read`
- `cargo test -p omnigraph-cli --test system_local local_cli_s3_end_to_end_init_load_read_flow`

Locally, set `OMNIGRAPH_S3_TEST_BUCKET` (and the usual `AWS_*` vars including `AWS_ENDPOINT_URL_S3` for non-AWS) before running. Without those, S3 tests skip gracefully.

## OpenAPI drift

`crates/omnigraph-server/tests/openapi.rs` regenerates `openapi.json` and diffs against the checked-in copy. CI auto-commits the regeneration on same-repo PRs and otherwise runs in strict-check mode (env: `OMNIGRAPH_UPDATE_OPENAPI`).

## Examples & benches

- `crates/omnigraph/examples/bench_expand.rs` — runnable example (not part of CI).
- No `benches/` directories. The architectural rule [docs/invariants.md §VII.36](invariants.md) requires benchmark motivation before optimization, so add `benches/` per crate when you ship a perf-driven change.

## Coverage tooling — what's missing

There is **no** coverage tooling in the repo today: no `tarpaulin.toml`, no `codecov.yml`, no coverage CI step. If you want to know whether your change is covered, the answer comes from reading and running the relevant integration tests, not from a tool.

If introducing coverage tooling is in scope for your task, the natural first step is `cargo-llvm-cov` wired into a separate CI job, and a per-crate threshold rather than a global one.

## First principle: check what already covers it

**Before writing any new test, check whether an existing test already covers the case.** The cost of duplicating coverage is high: more code to read, more places to keep in sync when behavior changes, and more drift when one copy lags. The cost of *extending* an existing test is usually one extra assertion or one extra fixture row.

How to check:

1. **Map the change to an area** — use the engine integration-test table above (`branching.rs`, `runs.rs`, `search.rs`, etc.). The filename usually names the area.
2. **Open the file and skim every test fn name.** Test fn names are the index — read them all, not just the first few.
3. **Grep for the symbol or path you're changing.** `rg <FunctionName>` or `rg <enum_variant>` across all `tests/` directories surfaces existing coverage you might miss.
4. **Decide one of three outcomes**, in this order of preference:
   - *Existing test already asserts the new behavior* → no new test needed; this PR is a refactor or no-op behaviorally. Confirm by running the existing test against the change.
   - *Existing test covers the area but not your case* → **add an assertion or a fixture row to the existing test**, don't write a new function with `init_and_load()` again.
   - *No existing coverage in any test file* → only then write a new test; put it in the file that owns the area, or open a new file only if the area itself is new.

Three duplicated `init_and_load() → run_query → assert_eq` blocks where one parameterized test would do is the most common form of test rot in this repo. Don't add to it.

## Before-every-task checklist

When you pick up any change, walk through this:

1. **Find existing coverage** (per the principle above). Don't just look at the first test file by name — grep for the symbol you're touching across every crate's `tests/`.
2. **Run those tests locally before editing.** `cargo test --workspace --locked` for the broad pass; `-p <crate> --test <file>` for a focused loop. Confirm a clean baseline.
3. **Decide extend-vs-new** explicitly. If you can extend an existing test (assertion, fixture row, parameterization), do that. Only add a new test fn or new file if no existing one owns the area.
4. **Reuse the helpers.** `init_and_load()`, fixture files, the CLI `support` harness — re-use them. Don't bootstrap a fresh repo by hand if a helper exists.
5. **Mind the boundary.** Per [docs/invariants.md §VII.33](invariants.md), test at the layer the change lives at — planner-level changes deserve planner-level tests, not just end-to-end.
6. **For substrate-touching changes** (Lance behavior), reach for `failpoints` or fixture-driven scenarios, not stubbed-out mocks.
7. **For server / API changes**, confirm the OpenAPI regeneration happens in `openapi.rs` and that the diff lands in `openapi.json`.
8. **Verify your change makes an existing test fail before it makes the new one pass.** If you can break the code without breaking a test, your coverage gap is the problem to fix first.

When in doubt, re-read [docs/invariants.md §VII](invariants.md) — quality gates apply to every change.
