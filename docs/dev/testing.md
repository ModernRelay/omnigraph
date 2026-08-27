# Testing

This is the ownership map for OmniGraph's tests. Read it before changing code: find the existing owner, run it as a clean baseline, and extend it instead of creating a parallel fixture.

## Rules

1. Test at the boundary that owns the promise. Compiler behavior belongs in compiler tests; engine guarantees belong at the public engine API; HTTP and CLI behavior belongs at those transports.
2. Prefer one new assertion, fixture row, or parameter over another `init_and_load` test.
3. Test logical results and durable state. Inspect Lance internals only for a compatibility fence, recovery fault, or physical-cost contract.
4. Every failure path must prove what did *not* move: manifest head, table head, lineage, sidecar, or external I/O as appropriate.
5. Time and RSS measurements are decision instruments, not ordinary correctness gates. Deterministic operation counts may be CI contracts.

The invariants behind these rules are in [invariants.md](invariants.md). Lance-dependent changes also require the upstream review and guards described in [lance.md](lance.md).

## Test layout

| Package | Primary owners | Shared support |
|---|---|---|
| `omnigraph-compiler` | In-source parser, catalog, type-checking, lowering, and lint tests | Module-local fixtures |
| `omnigraph-storage` | In-source control-object storage, CAS, locking, and URI tests | Module-local fixtures |
| `omnigraph-engine` | `crates/omnigraph/tests/` plus focused in-source tests | `tests/helpers/` and `tests/fixtures/` |
| `omnigraph-policy` | In-source Cedar policy parsing and evaluation tests | Module-local fixtures |
| `omnigraph-cluster` | In-source lifecycle tests; `tests/failpoints.rs`; `tests/s3_cluster.rs` | Module-local fixtures |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | `tests/support/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | `tests/support/mod.rs` |
| `omnigraph-dst` | `crates/omnigraph-dst/tests/` (`scenarios.rs`, `lane_b.rs`, `torn_init.rs`) plus in-source proofs | Crate-local fixtures. Deterministic simulation; needs `--cfg tokio_unstable` (the crate-local `.cargo/config.toml` sets it when cargo runs from the crate dir; every test file is `#![cfg(tokio_unstable)]`-gated and the crate compiles empty without it, so the default workspace gate is unaffected). `#[ignore]`d tests are fleet/hunt instruments driven by the DST workflows |
| `omnigraph-bench` | In-source configuration tests and `crates/omnigraph-bench/tests/` | Checked-in cases and suites under `benchmarks/` |

Do not copy server or CLI process setup into a new suite. Their support modules own hermetic configuration, binary startup, temporary roots, and common assertions.

## Engine ownership

The engine integration suite is grouped by behavior, not implementation module:

| Concern | Existing owners |
|---|---|
| Initialization and representative journeys | `lifecycle.rs`, `end_to_end.rs`, `composite_flow.rs`, `consistency.rs` |
| Query results and operators | `aggregation.rs`, `literal_filters.rs`, `ordering.rs`, `traversal.rs`, `traversal_indexed.rs`, `proptest_equivalence.rs` |
| Search and physical indexes | `search.rs`, `scalar_indexes.rs`, `lance_surface_guards.rs` |
| Writes, validation, schema, and policy | `writes.rs`, `validators.rs`, `schema_apply.rs`, `policy_engine_chassis.rs` |
| Branches, snapshots, diffs, and merges | `branching.rs`, `point_in_time.rs`, `changes.rs`, `merge_truth_table.rs`, `merge_fast_forward.rs` |
| Recovery and crash windows | `recovery.rs`, `failpoints.rs`, `failpoint_names_guard.rs`, in-source manifest/recovery tests |
| Maintenance and substrate fences | `maintenance.rs`, `lance_surface_guards.rs`, `lance_version_columns.rs`, `forbidden_apis.rs` |
| Export and lineage | `export.rs`, `lineage_projection.rs` |
| Cost and benchmark contracts | `write_cost.rs`, `write_cost_s3.rs`, `warm_read_cost.rs`, `branch_control_cost.rs`, `merge_cost.rs`, `changes_cost.rs`, the checkpoint/head lookup instruments, and `benchmark_scenario_contract.rs` |

Use `tests/helpers/mod.rs` for the standard graph, snapshots, row reads, Blob selectors, and bounded Blob collection. Recovery helpers belong in `tests/helpers/recovery.rs`; object-store counters belong in `tests/helpers/cost.rs`.

`changes_cost.rs` owns the change-feed cost boundary: transaction-footprint
candidate scans, bounded page work, and caught-up versus backlog polling curves.

### Recovery and failpoints

Recovery tests must cover the protocol layer, the writer, and the user-visible reopening behavior:

- in-source tests own sidecar encoding, validation, classification, and exact publication rules;
- `tests/recovery.rs` owns deterministic completed, partial, ambiguous, and foreign-effect outcomes;
- `tests/failpoints.rs` owns crash windows around durable effects;
- the writer's normal integration owner proves pre-arm failures leave no residue.

When adding a new writer or sidecar field, update all three layers. See [recovery.md](recovery.md).

### Blob behavior

Blob coverage is deliberately split:

- engine `end_to_end.rs`, `branching.rs`, and in-source Blob tests own logical cell selection, snapshots, integrity, ranges, external classification, and write admission;
- cluster tests own persisted external-source policy and serving projections;
- server `data_routes.rs`, `auth_policy.rs`, and `openapi.rs` own GET/HEAD, auth, conditions, ranges, redirects, backpressure, and schema drift;
- CLI `cli_data.rs` owns `blob get/stat`; `parity_matrix.rs` compares embedded and remote results.

Do not exercise a server promise solely through the engine facade. The complete contract is summarized in [blob.md](blob.md).

### Lance compatibility

Run this first for every Lance change:

```bash
cargo test -p omnigraph-engine --test lance_surface_guards
```

The guards pin only substrate behavior OmniGraph actually depends on: version and row columns, transaction witnesses, primary-key conflict filters, branch/ref cleanup, index coverage, stable row IDs, vector ordering fences, and Blob reads through compaction. If an upstream limitation disappears, remove the workaround and its guard together.

## Server and CLI ownership

Server suites are organized by public route: `auth_policy`, `data_routes`, `schema_routes`, `stored_queries`, `multi_graph`, `boot_settings`, object-store coverage in `s3`, and the generated contract in `openapi`.

CLI suites own their named planes: cluster lifecycle, data commands, stored queries, schema/config, cross-version rebuild, embedded/remote parity, and local/remote system journeys. Keep `OMNIGRAPH_HOME` hermetic by using `tests/support::cli()` or `cli_process()`.

The system tests start workspace binaries on ephemeral localhost ports. Set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` only in constrained local sandboxes; CI's configured owners must not skip.

## Commands

Focused iteration:

```bash
cargo test -p omnigraph-engine --test traversal
cargo test -p omnigraph-engine --test writes concurrent
cargo test -p omnigraph-server --test data_routes
cargo test -p omnigraph-cli --test cli_data
cargo test -p omnigraph-cluster --test failpoints --features failpoints
cargo test -p omnigraph-bench --locked
```

Canonical workspace graph:

```bash
cargo test --workspace --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

The feature-superset command is the canonical graph because it compiles the current tree once with failpoint hooks present but inert unless a test enables one. Also run formatting and both Clippy graphs before pushing; [ci.md](ci.md) lists the exact gates.

AWS server support has a separate feature owner:

```bash
cargo test -p omnigraph-server --features aws
```

S3-backed tests skip unless `OMNIGRAPH_S3_TEST_BUCKET` and the corresponding AWS endpoint/credential variables are set. Azure-backed tests skip unless `OMNIGRAPH_AZURE_TEST_CONTAINER` and the documented Azure/Azurite variables are set. A configured CI backend treats a skip as failure.

### OpenAPI

`crates/omnigraph-server/tests/openapi.rs` regenerates the specification in memory and compares it with `openapi.json`. For an intentional API change:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 \
  cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date
```

Commit the generated file with the API change. CI checks drift; it never updates the file.

## Cost tests and benchmarks

Correctness tests may assert deterministic logical or object-store operation counts when the count is part of the design contract. Wall time and peak RSS depend on the host and belong in the `omnigraph-bench` scenario harness; benchmark results are evidence rather than pass/fail assertions. Declarative benchmark cases and suites live under `benchmarks/`; the engine's deterministic benchmark contracts remain in `crates/omnigraph/tests/`.

The current runner executes the narrow, fail-closed local envelope documented
in `crates/omnigraph-bench/README.md`. It requires a release binary, restores
every repetition from a never-opened APFS clonefile template at the fixture's
stable path, contains each measured merge in a fresh SHA-attested,
hard-deadline worker process, verifies exact target/source/main state, and emits
diagnostic output rather than a durable benchmark record:

```bash
cargo run --release --locked -p omnigraph-bench -- \
  suite run benchmarks/suites/local-smoke.suite-v1.yaml
```

Do not archive that diagnostic JSON as telemetry. Immutable records and their
query projection are owned by the next harness slice.

Keep measurement fixtures separate from production schemas and recovery state. A no-go result belongs in the RFC or issue that consumed the experiment, not as a permanent narrative in this map.

## Before every task

1. Read [invariants.md](invariants.md).
2. Use [lance.md](lance.md) to identify and read every relevant full upstream page.
3. Search existing tests by public API, error variant, route, and durable object name.
4. Run the narrowest existing owner as a clean baseline.
5. Extend that owner unless the behavior crosses a genuinely new public boundary.
6. Run the focused owner again, then the canonical workspace graph in proportion to risk.
7. For docs, workflow, or API changes, also run the repository link/pin/OpenAPI checks that own those generated contracts.
