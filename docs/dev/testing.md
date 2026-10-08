# Testing

This is the ownership map for OmniGraph's tests. Read it before changing code: find the existing owner, run it as a clean baseline, and extend it instead of creating a parallel fixture.

## Rules

1. Test at the boundary that owns the promise. Compiler behavior belongs in compiler tests; engine guarantees belong at the public engine API; HTTP and CLI behavior belongs at those transports.
2. Prefer one new assertion, fixture row, or parameter over another `init_and_load` test. When the change fixes an issue, the `Fix Regression Gate` keys on `issue_N` in the test's name or the `.gqt` case's file name: extend an owner test by renaming it to carry `issue_N` in the same change, or add a row to the `issue_N_*.gqt` case (`docs/dev/ci.md`).
3. Test logical results and durable state. Inspect Lance internals only for a compatibility fence, recovery fault, or physical-cost contract.
4. Every failure path must prove what did *not* move: manifest head, table head, lineage, schema staging, or external I/O as appropriate.
5. Time and RSS measurements are decision instruments, not ordinary correctness gates. Deterministic operation counts may be CI contracts.

The invariants behind these rules are in [invariants.md](invariants.md). Lance-dependent changes also require the upstream review and guards described in [lance.md](lance.md).

## Test layout

| Package | Primary owners | Shared support |
|---|---|---|
| `omnigraph-compiler` | In-source parser, catalog, type-checking, lowering, and lint tests | Module-local fixtures |
| `omnigraph-planner` | In-source optimizer/cost tests and `tests/query_plan.rs`, `tests/registry.rs`, `tests/lower_walk.rs`; plan assertions over real snapshots live in GQT | `PlanSource` fixtures for metadata and refusal states |
| `omnigraph-storage` | In-source control-object storage, CAS, locking, and URI tests | Module-local fixtures |
| `omnigraph-seams` | In-source tests of the seam type: slot scopes, the guard, the decision behaviors; `tests/failpoint_names_guard.rs`, the source walk over the engine, core, catalog, cluster and DST crates and the `.gqt` corpus that keeps every seam catalogued, crossed and armed | None |
| `omnigraph-core` | In-file `#[cfg(test)]` tests (62 today) of the error type, branch names, branch control, Lance clone, metadata, full-text compatibility and instrumentation | Module-local fixtures |
| `omnigraph-catalog` | In-source tests (52 today): `crates/omnigraph-catalog/src/tests.rs` for `__manifest` publication, state and lineage, plus in-file tests in `migrations.rs` and `retention.rs` | Module-local fixtures; `omnigraph-core`'s `test-util` helpers |
| `omnigraph-engine` | `crates/omnigraph/tests/` plus focused in-source tests | `tests/helpers/` and `tests/fixtures/` |
| `omnigraph-policy` | In-source Cedar policy parsing and evaluation tests | Module-local fixtures |
| `omnigraph-cluster` | In-source lifecycle, deployment and admission tests; `tests/failpoints.rs`; `tests/identity_recovery.rs`; `tests/s3_cluster.rs` | Module-local fixtures |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | `tests/support/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | `tests/support/mod.rs` |
| `omnigraph-dst` | `crates/omnigraph-dst/tests/` (`scenarios.rs`, `lane_b.rs`, `torn_init.rs`) plus in-source proofs | Crate-local fixtures. Deterministic simulation; needs `--cfg tokio_unstable` (the workspace `.cargo/config.toml` sets it for every build; the default workspace gate excludes the crate by name). Run from `crates/omnigraph-dst`: its `[env]`-only `.cargo/config.toml` supplies the pool trio that `require_pool_env` asserts at process start. `#[ignore]`d tests are fleet/hunt instruments driven by the DST workflows |
| `omnigraph-bench` | In-source configuration tests and `crates/omnigraph-bench/tests/` | Checked-in cases and suites under `benchmarks/` |
| `omnigraph-gqt-core` | In-source format, expectation and ordinary execution tests | Shared parser and executor used by the GQT runner and benchmark harness |
| `omnigraph-gqt` | `tests/gq_logic_tests.rs`, one libtest test per `.gqt` case (`datatest-stable`, `harness = false`), plus runner self-tests and the corpus layout check | The `.gqt` corpus under `crates/omnigraph-gqt/cases/`; author-marked slow cases under `crates/omnigraph-gqt/cases_slow/`, run by the `GQT slow nightly` workflow and never by `cargo test`; format in RFC 0045 |

Do not copy server or CLI process setup into a new suite. Their support modules own hermetic configuration, binary startup, temporary roots, and common assertions.

Test helpers that live in `omnigraph-core` or `omnigraph-catalog` and are reached by another crate's tests are gated `#[cfg(any(test, feature = "test-util"))]`. A plain `#[cfg(test)]` is not enough: `cfg(test)` is set per crate, so a dependent crate's test build compiles the base crate without it and cannot see the helper. The engine enables `test-util` on both crates through its dev-dependencies in `crates/omnigraph/Cargo.toml`, so the helpers exist only in test builds and never in a release artifact. `omnigraph-cluster` follows the same pattern: the server enables its `test-util` feature through its dev-dependencies to read a local cluster's serving snapshot with the storage root spelled as an `s3://` prefix, which is how the server's strict-boot test reaches the external Blob base overlap quarantine without an object store. `forbidden_apis.rs::split_crate_test_util_is_enabled_only_by_dev_dependencies` refuses a production enable of any of the three crates' `test-util` (`SPLIT_CRATE_PACKAGES` plus `TEST_UTIL_SEAM_PACKAGES`).

`tests/forbidden_apis.rs` walks the engine, `omnigraph-core` and `omnigraph-catalog` sources; a line that carries the sentinel comment `// forbidden-api-allow: <reason>`, on the line itself or the line above, is exempt from the lexical deny-list only (the structural graph-write guard still counts it), so every exemption shows up in review.

## Engine ownership

The engine integration suite is grouped by behavior, not implementation module:

| Concern | Existing owners |
|---|---|
| Initialization and representative journeys | `lifecycle.rs`, `end_to_end.rs`, `composite_flow.rs`, `consistency.rs` |
| Query results and operators | `aggregation.rs`, `literal_filters.rs`, `ordering.rs`, `traversal_indexed.rs`, `traversal_adaptive.rs`, `proptest_equivalence.rs`; the `.gqt` corpus lives in `omnigraph-gqt` (`crates/omnigraph-gqt/cases/`) |
| V2 execution and memory | `engine_v2.rs`, `engine_v2_memory.rs`; `repro_issue_703.rs` and `repro_issue_723.rs` own ignored scale symptoms |
| Frozen engine v1, the reference | `crates/omnigraph-reference-engine/tests/frozen.rs` pins every source file's bytes; `forbidden_apis.rs` owns that `omnigraph-gqt` is the crate's only dependent and that its source names neither the engine nor the planner; the crate has no other tests, and a `.gqt` step's `--- expect same as v1` compares v2's rows with its answer |
| V2 plan nodes' operators and the execution report | In-source `engine/report/tests.rs` owns which operator each node builds and what its row says (`ran`, `actual_rows`, `drained`); `crates/omnigraph-planner/tests/lower_walk.rs` owns the walk's order and refusals; the `ran` lines of `--- expect plan` read the report of the case's own run |
| V2 plan sufficiency: the bound plan alone reproduces a run | `crates/omnigraph/tests/engine_v2_plan_replay.rs` (see [Plan replay](#plan-replay)): one query per node kind with a switch or a ladder, replayed twice through `Session::replay_bound_plan` from the serialized `BoundPlan`, equal rows and the same trace required; `crates/omnigraph-planner/tests/bound_plan.rs` owns the serialized form; in-source `engine/search.rs` tests own the declared ladder's stepping; `engine/plan_source.rs` tests own that the assumed memory limit sizes the plan, the lowering and the pool |
| V2 scrubbed replay: no input beside the bound plan and the snapshot reaches execution | `engine_v2_scrubbed_replay.rs` owns the mechanism (the `std::env` grep over `crates/omnigraph/src/engine/`, the replay under an ambient memory limit and an ambient `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` the plan did not capture, `ann_nprobes` as a plan field, the `profile` rows); `engine_v2_plan_replay.rs` owns the snapshot pins (an edge write or an insert after planning refuses the replay); the inventory cases `cases/v2/planner/input_*.gqt` own one input class each (`engine`, `ann_nprobes`, index build state), and the clock class is owned by `engine_v2_plan_replay.rs`; `engine_v2_memory.rs` owns `ran id_lookup` under a refused build |
| V2 declared switches: the report names the side a run took | `cases/v2/planner/switch_ran_on_the_report.gqt` (the `ran` line of `--- expect plan`); in-source `engine/report/tests.rs` owns the switch's row and a scan whose plan declares no fallback |
| Search and physical indexes | `search.rs`, `scalar_indexes.rs`, `lance_surface_guards.rs`, `rrf_prefilter_gate.rs` (the rrf plan gate's differential oracle and fences), `repro_issue_563.rs` (`#[ignore]`d overflow-scale symptom tier) |
| Writes, validation, schema, and policy | `writes.rs`, `validators.rs`, `schema_apply.rs`, `policy_engine_chassis.rs` |
| Branches, snapshots, diffs, and merges | `branching.rs`, `point_in_time.rs`, `changes.rs`, `merge_truth_table.rs`, `merge_fast_forward.rs` |
| Recovery and crash windows | `recovery.rs`, `failpoints.rs` (including the `live_handle_*` liveness owners: a live handle writes again once faults stop, without reopening), `detached_commit_matrix.rs` (the RFC 0067 writer × window × fault × recovery-actor matrix over the insert, multi-table, load, cleanup, ensure-indices, full-text-rebuild, merge, schema-apply, optimize and system-column-upgrade writers; the same-handle liveness actor runs by default and `OMNIGRAPH_MATRIX=full` adds the other-process and cleanup actors), in-source manifest/recovery tests |
| Maintenance and substrate fences | `maintenance.rs`, `lance_surface_guards.rs`, `lance_version_columns.rs`, `forbidden_apis.rs` |
| Export and lineage | `export.rs`, `lineage_projection.rs` |
| Legacy-vintage graphs (`id`/`src`/`dst` spellings, born at the current stamp) | `legacy_columns.rs` — load, query, export round trip, evolution; needs `--features failpoints` |
| System-column upgrade (RFC 0040 step 3: respelling in place on a supported standalone graph; vintage is independent of the storage stamp) | `system_column_upgrade.rs`: check and execute, preflight refusals, every window before the manifest commit leaving no residue, a complete contract and table state after a post-commit failure, same-handle retry, the control-object cost; needs `--features failpoints` |
| Cost and benchmark contracts | `write_cost.rs`, `write_cost_s3.rs`, `warm_read_cost.rs`, `branch_control_cost.rs`, `merge_cost.rs`, `changes_cost.rs`, the checkpoint/head lookup instruments, and the GQT benchmark adapter tests in `omnigraph-bench` |

Use `tests/helpers/mod.rs` for the standard graph, snapshots, row reads, Blob selectors, and bounded Blob collection. Recovery helpers belong in `tests/helpers/recovery.rs`; object-store counters belong in `tests/helpers/cost.rs`; graphs whose rows trip the ordered-scan sorter cap belong in `tests/helpers/wide_rows.rs`.

`changes_cost.rs` owns the change-feed cost boundary: transaction-footprint
candidate scans, bounded page work, and caught-up versus backlog polling curves.

### Recovery and failpoints

Crash tests must cover the writer and the user-visible reopening behavior:

- `tests/failpoints.rs` owns crash windows around durable effects: after a detached effect and before publication, where the graph is unchanged, and after publication, where the pin is complete;
- `tests/detached_commit_matrix.rs` owns the writer × window × fault × recovery-actor matrix under one oracle;
- `tests/recovery.rs` owns manifest-only contract admission, ignored orphan schema artifacts and read-only opens without writes; a sidecar from an older build refuses read-write but not read-only open. Read-write open of a local root retains its temporary create-if-absent capability probe;
- `tests/lance_surface_guards.rs` owns the Lance detached-commit facts the pin and the collector depend on;
- the writer's normal integration owner proves pre-effect failures leave no residue.

To add a seam: declare it beside the site it guards, above the item that
crosses it, with
`decide_seam! { pub static NAME = ("area.place", Op, [Effect, ..]); }`
(imported from `crate::seams`; the compiler records the file and line as
the seam's `site()`, and a hand-written `Seam::decide` is refused); add its
path to the `catalog!` list in `crates/omnigraph/src/seams/catalog.rs`; call
`fail`, `skip` or `contention` (imported from `crate::seams`) with it at a
site between two steps (one declared effect), or `guarded` around the one
operation it wraps (every outcome that operation can have; a further action
there is then a case, not an edit). A private module on the path from the
crate root to the declaring file becomes `pub(crate)` for the re-export.
`crates/omnigraph-seams/tests/failpoint_names_guard.rs` checks the index, that no static is left
in the catalog, under a test module or without `pub`, that a single-effect
helper takes a seam declaring exactly its effect, that production code under
`src/` crosses it, and that test code (an integration target, a `tests.rs` /
`…_tests.rs` file, or any item gated on `cfg(test)`), the DST crate or a
`.gqt` case (the `at` of a `--- seam` body, decoded as the runner decodes
it) arms it; a crossing never counts as arming. `scripts/seam_corpus.py`
lists every seam with where it is declared and which cases cover it.

When adding a new writer, update all of these layers. See [recovery.md](recovery.md).

### Blob behavior

Blob coverage is deliberately split:

- engine `end_to_end.rs`, `branching.rs`, and in-source Blob tests own logical cell selection, snapshots, integrity, ranges, external classification, and write admission;
- engine `maintenance.rs` owns Blob compaction (the batch derived from a row's summed Blob columns, fragments with deleted rows, per-task sizing in `maintenance.rs::optimize_sizes_each_compaction_task_from_its_own_fragments`, external references counting nothing in `maintenance.rs::optimize_does_not_size_a_blob_batch_by_external_references`);
- cluster tests own persisted external-source policy and serving projections;
- server `data_routes.rs`, `auth_policy.rs`, and `openapi.rs` own GET/HEAD, auth, conditions, ranges, redirects, backpressure, and schema drift;
- CLI `cli_data.rs` owns `blob get/stat`; `parity_matrix.rs` compares embedded and remote results.

Do not exercise a server promise solely through the engine facade. The complete contract is summarized in [blob.md](blob.md).

### Lance compatibility

Run this first for every Lance change:

```bash
cargo test -p omnigraph-engine --test lance_surface_guards
```

The guards pin only substrate behavior OmniGraph actually depends on: version and row columns, transaction witnesses, primary-key conflict filters, branch/ref cleanup, index coverage, stable row IDs, vector ordering fences, Blob reads through compaction, and the detached-commit privacy, twin replay and self-conflict rules that RFC 0067 builds on. If an upstream limitation disappears, remove the workaround and its guard together.

## Server and CLI ownership

CLI `system_remote` runs the actual CLI, server and fault proxy in the ordinary
workspace gate; its required-cell checks reject removed or ignored cases. The
lost-delivery matrix covers disconnect, truncated response, proxy 504 and caller
timeout without replaying a committed merge. Its deployment matrix holds a real
admitted request through drain, disconnects or times out the caller before
durable acceptance, and loses replies after acceptance. Exact-ID observation
must finish with one ledger result, one schema publication and the same server
PID; neither apply nor recovery polling may resubmit.

Server suites are organized by public route: `auth_policy`, `data_routes`, `schema_routes`, `stored_queries`, `multi_graph`, `boot_settings`, object-store coverage in `s3`, and the generated contract in `openapi`.

Per-graph serving transitions extend these owners: in-source `registry` tests
own capture/close ordering, drain-only deployment deadlines, affected activation
scope, schema identity and candidate bounds;
`operations`, `ingress` and `mcp` own detached execution and output lifetimes.
`stored_queries` parks a request before engine capture, `data_routes` retains
disconnected writes and stream bytes, and `boot_settings`/`mcp` check authorized
availability. The same owners cover coherent schema/query batch activation; these assertions
are not generic native settlement. Server `boot_settings` exercises authenticated
submission, parked requests, caller disconnect, pre-effect refusal, durable
acceptance before completion, activation-in-progress observation and exact receipt
access after management handoff. It also holds a merge across served schema
planning to prove preview does not wait for the graph gate. In-source
`deployment` tests suspend observer read futures across owner start, finish and complete
turnover, exercising bounded re-observation for aggregate and exact status.
CLI `cli_cluster` owns submit-once polling, transient 429/503 and truncated-body
retries, malformed-receipt refusal, terminal outcomes and caller timeout without
replay. Its managed fixtures cover status/history scope and filters, explicit
`--managed` selection, and wrong-mode refusals before context or external access.
Direct apply must ignore both valid and malformed managed folder context.
CLI `cli_cluster_e2e` proves one PID/listener survives schema/query
replacement, graph addition, policy grant/revocation and management handoff;
the original submitter retains only its exact receipt access after restart.
Extend that same journey for graph deletion, proving target storage/history
removal, peer preservation and unchanged PID/listener, including a served deletion
preview followed by `--no-wait` submission and exact-ID `status --wait`. It checks
the achieved configuration again after restart. The local journey also exercises
counted embedding-provider replacement, external-Blob admission changes, catalog
integrity reporting, schema-drift refusal and missing-root refusal. S3/Azure wrappers
share the transport-independent phases; local success does not qualify their
storage-fault paths. CI requires the local journey to execute successfully.

CLI suites own their named planes: cluster lifecycle, data commands, stored queries, schema/config, cross-version rebuild, embedded/remote parity, and local/remote system journeys. Keep `OMNIGRAPH_HOME` hermetic by using `tests/support::cli()` or `cli_process()`.

Deployment tests extend these owners: cluster `tests.rs` pins no-reset
ledger conversion, captured source bytes, exact-ID lookup, bounded results and
exact applied schema identity after receipt eviction; `admission.rs` pins lifetime
exclusion and exact reconciliation admission. Cluster `tests/failpoints.rs` owns
interruption windows, killed-process recovery and corrective successors, including
deletion before start, during partial removal, after root absence and before
terminal ledger acknowledgement, same-lifetime older manifest survivors,
replacement refusal and corrective creation in empty local settlement residue;
`tests/identity_recovery.rs` owns current-actor authorization and adoption of a
persisted settlement without replacing its author. CLI
`tests/cli_cluster_e2e.rs` owns the root-only deployment round trip, and
`tests/cli_cluster.rs` owns CLI admission. Engine `tests/schema_apply.rs` owns
strict prepared publication and settlement proofs, with actor checks in
`tests/policy_engine_chassis.rs`; catalog tests own numeric CAS. The storage
in-source contract owns bounded same-GET bytes/tokens, and cluster
`tests/s3_cluster.rs` owns the shared S3/Azure backend journey. Passing Azurite is
not qualification of live Azure lease-loss or delayed accepted writes.

The cross-version rebuild owner, `crossversion_upgrade.rs`, skips each predecessor case when its binary is not configured, so a local `cargo test -p omnigraph-cli --test crossversion_upgrade` is green even while CI's `V5 ↔ V10 Format Fence` is red. To run the fence locally, build the predecessor CLI from the commit `ci.yml` pins as `FINAL_INTERNAL_V5_COMMIT` (`git worktree add <dir> <sha>`, then `cargo build --locked -p omnigraph-cli --bin omnigraph` inside it) and run the exact case with that binary:

```bash
OMNIGRAPH_V5_BIN=<dir>/target/debug/omnigraph cargo test --locked -p omnigraph-cli --test crossversion_upgrade current_v10_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v10 -- --exact --nocapture
```

The older seams work the same way with released binaries: `OMNIGRAPH_OLD_BIN` (0.7.2) and `OMNIGRAPH_PREVIOUS_BIN` (0.8.1). `OMNIGRAPH_V6_BIN` (the 0.10.0 release) owns the v6↔v10 fence. RFC 0062 introduced v7's registration clock, RFC 0042's native-ref retirement metadata requires v8, RFC 0040's system columns stamped new graphs v9, and RFC 0067's detached table commits stamp every graph v10. The v0.9 journey is a different case, a fully exercised v6 graph — branches, edges, vectors, full-text and blobs — that the current binary refuses and that is rebuilt from a 0.9 export; `Test Workspace` runs both on every pull request that changes engine input, with the releases it installs.

The separate `Storage Upgrade Compatibility` CI job requires the
`storage_upgrade` cases of `crossversion_upgrade.rs` (the report on a fresh
graph, the cluster-path refusal and the five genuine journeys:
`genuine_v13_storage_upgrade_preserves_history`,
`genuine_v0_11_0_storage_upgrade_preserves_history`,
`genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup`,
`genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history` and
`genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history`), the engine
`db::upgrade::tests`, `lance_version_columns` and `forbidden_apis`. Missing
cases, empty runs and skipped required cases fail the job.

The stamp-13 journey needs the stamp-13 CLI: the job builds it from the commit
`ci.yml` pins as `STAMP_13_SOURCE_COMMIT` and exports `OMNIGRAPH_V13_BIN`. It
proves its predecessor by behaviour (that binary's `snapshot --json` reports
`internal_schema_version` 13 and the current `upgrade --check` observes 13),
builds branches, a merge, a deleted branch, a recreated one and a fork of a
deleted branch with the old binary, upgrades, and compares commit history,
rows, `cleanup` and a backup restore after it.

The 0.11.x journeys need the released CLIs, which the job installs:
`OMNIGRAPH_V011_BIN` (0.11.0, writes stamp 9) and `OMNIGRAPH_V6_BIN` (0.10.0).
`genuine_v0_11_0_storage_upgrade_preserves_history` runs the same script as
the stamp-13 journey plus a schema apply (a new type and a nullable property
on the existing `Doc`, with a commit before and one after it) and an
`optimize` with the old binary, asserts that the three root schema objects
are byte-identical after `completed`, and compares what the two commits
around the apply answer for the added property before and after the upgrade.
`genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup`
runs the old binary's `cleanup --keep 1` before the upgrade. After the
upgrade, a write and a merge, `cleanup --older-than 7d` refuses a table on a
pre-upgrade linear pin. `cleanup --keep 1` then passes and keeps every
`__manifest` version; `--older-than 7d` and `--keep 100` still refuse, and
`--older-than 0s` passes, as the upgrade guide
describes. A commit the old binary refused as reclaimed must be refused after
the upgrade, any other failure of a read fails the journey, and at least one
commit must still be served.
`genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history` builds the
graph with 0.10.0, takes it to stamp 8 with the 0.11.0
`upgrade --to-format 8`, and upgrades from there;
`genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history`
lets the 0.11.0 `upgrade` run to its default target (stamp 9, the three
handlers through the system-column respelling). That route needs a graph
with only main, so 0.10.0 merges and deletes `review` before 0.11.0's default
conversion, and 0.11.0 forks `temp` after it. The journey asserts the reads at
every commit main lists right after the predecessor's conversion, those
written before the respelling among them. Main builds 10 to 13 also
print `0.11.0`, so these journeys prove their source by the stamp
`snapshot --json` reports, not by `--version`.

Each journey resolver reads its variable, else the binary under
`target/storage-upgrade-binaries/` (`stamp-13/`, `v0.11.0/`, `v0.10.0/`).
Without a binary the journey prints a skip line locally and panics when
`OMNIGRAPH_REQUIRE_STORAGE_UPGRADE_TESTS=1`, as CI sets. The v6 format fence
reads `OMNIGRAPH_V6_BIN` alone and skips when it is unset, whatever that
variable says. To run the
crossversion scope locally, build the stamp-13 predecessor as for the v5 fence
above, install the two releases with `scripts/install.sh` (`VERSION=v0.11.0`
and `VERSION=v0.10.0`, each with its `INSTALL_DIR`), place the three binaries
in those directories and run:

```bash
cargo test --workspace --locked --test crossversion_upgrade --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints storage_upgrade -- --test-threads=1
```

`db/upgrade/tests.rs` owns the `upgrade_storage` protocol over
`legacy::write::LegacyHistory`, the test-only writer of
`omnigraph-catalog` that replays scripted publishes: `create` in the stamp-13
overwrite order, `create_stamped` in the flat shape of stamps 8 and 9, for
which the test module also writes the three root schema objects. It covers
the reports (`already_current` with no write on a fresh graph,
`unsupported_source`, `unsupported_target`, the pending-marker reports,
`--check` leaving the store untouched), conversion and equivalence of main,
named refs and fresh forks, the pre-fence refusals and bounds, every seam
interrupted and rerun, and the post-upgrade reads: commit list and change feed
across the upgrade, numeric snapshots below it, merges on a legacy base,
retired refs, leftover merge-input tags and `cleanup`. The census, plan,
locator codecs and legacy read arms are owned by the `legacy_` tests of
`omnigraph-catalog` (`tests.rs`, `history.rs`). A fixture cannot drift from
the predecessor unnoticed only because the genuine journey is required; change
both together. Keep ordinary-open refusal for all pre-v14 stamps.
`schema_apply.rs`, `system_column_upgrade.rs` and historical-read owners cover
atomic contract publication, first-touch retry and current-contract historical
reads. The catalog tests own row uniqueness, projection and validation;
`lance_surface_guards.rs` owns the filtered packed-record scan. See the
[support matrix](versioning.md#storage-upgrade-support-matrix).

The system tests start workspace binaries on ephemeral localhost ports. Set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` only in constrained local sandboxes; CI's configured owners must not skip.

### Manual 0.12 cluster upgrade qualification

`genuine_v0_12_0_cluster_ledger_upgrade_preserves_live_deployment` is an ignored,
Unix-only release qualification test. It creates genuine 0.12 receipts, stops
the old server, converts the ledger, checks data/history/schema identity and
historical reads, then applies live schema and policy changes. Storage stays at
format 14. Ordinary CI keeps current-version live-deployment coverage; it does
not download 0.12 or run this journey.

Run explicitly from the repository root when qualifying that upgrade path,
using a native build with Cargo's default `target/` directory.
Both predecessor variables are required; missing binaries fail. The installer
verifies the official archive checksum. Copy the freshly built candidate server
so another build cannot replace it during qualification:

```bash
set -euo pipefail
qualification_dir=$(mktemp -d)
REPO_SLUG=ModernRelay/omnigraph VERSION=v0.12.0 INSTALL_DIR="$qualification_dir/v012" bash scripts/install.sh
qualification_features=omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo build --locked -p omnigraph-cli -p omnigraph-server -p omnigraph-engine -p omnigraph-cluster --features "$qualification_features"
cp target/debug/omnigraph-server "$qualification_dir/omnigraph-server"
env RUST_MIN_STACK=16777216 \
  OMNIGRAPH_V012_BIN="$qualification_dir/v012/omnigraph" \
  OMNIGRAPH_V012_SERVER_BIN="$qualification_dir/v012/omnigraph-server" \
  "CARGO_BIN_EXE_omnigraph-server=$qualification_dir/omnigraph-server" \
  cargo test --locked -p omnigraph-cli -p omnigraph-server -p omnigraph-engine -p omnigraph-cluster \
    --features "$qualification_features" --test crossversion_upgrade \
    genuine_v0_12_0_cluster_ledger_upgrade_preserves_live_deployment \
    -- --exact --ignored --test-threads=1 --nocapture
```

Qualification requires `1 passed; 0 failed; 0 ignored`; an empty or ignored run
is not evidence. Keep its output with the release qualification record.

## Commands

Focused iteration:

```bash
cargo test -p omnigraph-engine --test traversal_indexed
cargo test -p omnigraph-engine --test writes concurrent
cargo test -p omnigraph-server --test data_routes
cargo test -p omnigraph-cli --test cli_data
cargo test -p omnigraph-cluster --test failpoints --features failpoints
cargo test -p omnigraph-bench --locked
```

GQT commands run from any directory inside the checkout; the workspace Cargo
configuration enables the DST runtime that corpus files request:

```bash
cargo test -p omnigraph-gqt --locked                            # complete corpus and harness tests
cargo test -p omnigraph-gqt --test gq_logic_tests issue_563      # matching case names
cargo test -p omnigraph-gqt --test gq_logic_tests -- --list      # one line per case
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt --measure   # store requests per step under DST
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- cases/concurrent_read_beside_publish.gqt --measure   # a `--- concurrent` block: sessions overlap under an `order:` line, one cost row per session
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt --measure --baseline /tmp/gqt-cost.tsv --write-baseline   # record a cost baseline anywhere on disk; --baseline alone prints the delta
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- --store file:///path/to/graph /path/to/queries.gqt
```

Schema and seed are optional together; a file without them needs `--store`
and belongs outside the automatically discovered corpus. Dataset-only files
with schema and seed may have zero steps. `--store` opens the supplied root,
skips fixture preparation, and runs ordinary steps, including writes and
restart, on that root. Its backend must match the declared direct-engine
environment; DST and server targets are refused. External-store reports
cannot replay because the data is not frozen. `--measure` requires at least
one selected DST environment; direct-only selections fail before execution.

Discovery includes every `.gqt` file below `cases/`, recursively. Shared
cases live at its root; v2-specific cases live in `v2/`, and plan assertions
in `v2/planner/`. Every case runs on engine v2, the one engine; a
`set engine = v2;` line some cases carry changes nothing, and directory
placement selects nothing.

A query step may end with `--- expect same as v1`, directly after its
`--- expect shape` or `--- expect plan`. The runner then runs the step's query
again on a copy of the case session that carries the frozen reference engine
(`omnigraph_reference_engine::ReferenceEngine`, installed through
`Session::with_read_executor` under the engine's `test-util` feature) and
compares its rows with v2's, ordered or unordered as the step's rows expect
says. A v1 error, a v1 gate refusal included, or a row difference fails the
step. The section is refused on a mutate step, an error expect, `show` and
`branch list`, and the DST runner skips it. The reference answers `not { ... }`
blocks only among the correlated blocks, and refuses count predicates and a
string `nearest` argument, so a case using those carries no
`expect same as v1`.

Every case is its own libtest test named `case::<relative/path>.gqt`, registered
at run time (`datatest-stable`), so the ordinary name filter selects cases, a
case-only pull request needs no Rust change, and `cargo-nextest` sees each
case (an IDE's test-results view lists cases from the
libtest-shaped output; no per-case gutter runnable exists, since no source
item does). `--test-threads=<n>` bounds how many cases run
concurrently (default: the machine's available parallelism); each case fails
if it exceeds `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS=<n>` seconds (default 10);
`OMNIGRAPH_GQ_BLESS=1` rewrites the failing step's expect rows, or its shape
lines, in place (local workflow only, never CI). Every rows step carries a `--- expect shape`
section, one `<name>: <type>` line per result column in `.pg` property syntax
(`p.age: I32?`), checked against the executed result before the rows, and the
executed schema is also checked against the compiler's inferred schema, so a
wrongly typed column fails even when every cell is null (RFC 0045
§Comparison semantics). Every `ok`/`FAIL` line carries the case's elapsed
time, and a case over budget leaves the corpus: it moves to
`crates/omnigraph-gqt/cases_slow/`, the nightly slow tier
([GQT README](../../crates/omnigraph-gqt/README.md#slow-cases)), when the
format can express it, and a symptom the format cannot express belongs in a
`heavy-repro:` `#[ignore]`d test under
`crates/omnigraph/tests/repro_issue_*.rs`. A name filter
that matches no case is libtest's ordinary green zero-test run; read the
`filtered out` count.

Canonical workspace graph:

```bash
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-dst --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo test -p omnigraph-gqt --locked --lib --test runner_dispatch
```

The feature-superset command compiles the current tree with failpoint hooks present but inert unless a test enables one; it also runs the seams crate's seam guard, the check a cases-only change can turn red (CI runs it again in `GQT (ordinary)`, where `Test Workspace` is skipped). The separate `GQ Logic Tests` context owns GQT: the `runner_dispatch` command above covers dispatch (CI also runs it with `RUSTFLAGS` cleared to prove unavailable-DST refusal), and the complete corpus command runs both execution targets. Neither command substitutes for the other. Also run formatting and both workspace Clippy graphs plus configured GQT Clippy; [ci.md](ci.md) lists the exact gates.

AWS server support has a separate feature owner:

```bash
cargo test -p omnigraph-server --features aws
```

S3-backed tests skip unless `OMNIGRAPH_S3_TEST_BUCKET` and the corresponding AWS endpoint/credential variables are set. Azure-backed tests skip unless `OMNIGRAPH_AZURE_TEST_CONTAINER` and the documented Azure/Azurite variables are set. A configured CI backend treats a skip as failure.

### Plan replay

Query behavior has two test tiers. A `.gqt` case owns what is visible in rows, counts, result column types, or errors. A Rust test owns what the format cannot express: mechanism, scale, process environment, concurrency. The claim that a v2 run is a function of its bound plan and the snapshot is of the second kind: no case reaches the replay door, and the GQT runner applies no replay check of its own to a step (its own checks are the report-shape parse of the `ran` lines' input, `schema_drift` and `check_pin`). The runner once replayed every v2 step against its report (two per-case replay invariants); that fence went when the plan started carrying everything execution reads, because a per-case replay in the same process found nothing the tests below do not find once, and cost every case a second run. The mapping from plan nodes to operators needs no check here: its rule is in [execution.md](execution.md#v2-plan-lowering-one-node-one-operator).

`crates/omnigraph/tests/engine_v2_plan_replay.rs` owns the replay. Each test gathers one query through `Session::query_inspected`, serializes the `BoundPlan` it emitted through the planner's mirrors, reads it back (it must read back equal), and executes it twice through `Session::replay_bound_plan`, a door that takes the bound plan and the engine context and nothing else about the query. Each replay must return the run's rows and repeat its trace: `id`, `operator` and `status` per row, `rung` and `ran` per attempt, and `actual_rows` wherever both attempts are `drained`. `drained` says the consumer pulled the operator's stream to its end; below an operator that stopped early (a `Limit`, a `Sort` with a fetch), a producer's count is a lower bound that depends on how far ahead its channel ran, and is not compared. One query per node kind that has a switch or a ladder: a `HashJoin` over an `Expand` (both switches on the report), a `nearest` ladder with an edge (two rungs), an `rrf` fusion, a `Limit` of zero (everything below skipped), a bulk `AntiJoin`, an `Aggregate`. Three more tests own the pins: after an edge write the replay of a traversal is refused, after an insert the replay of an unfiltered count is refused, and a write to a table the plan never read leaves the replay accepted, because `engine::plan_pins_snapshot` compares the plan's `Assumptions.datasets` (path, Lance branch and version per table key the planner read, or its absence) against the snapshot.

The scrubbed side is `crates/omnigraph/tests/engine_v2_scrubbed_replay.rs` (process environment, which no case can express): a grep that fails on any `std::env` read under `crates/omnigraph/src/engine/` outside `plan_source.rs`, the one permitted reader of configuration, before planning; two replays that set an ambient value the plan did not capture (the task-local memory limit, `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`) and require the captured one to win; and the replay of a plan gathered under one `ann_nprobes` in a session set to another. `Session::replay_bound_plan` takes no settings argument and sizes the pool from the plan's memory limit. The inventory sweep is one case per input class that a case can set two ways, `cases/v2/planner/input_*.gqt` (`engine`, `ann_nprobes`, index build state), each requiring equal rows or a plan line that differs, and the clock class is one of the replay plans of `engine_v2_plan_replay.rs`; the `ran` line and the `nprobes` claim of `--- expect plan` are what a differing plan line is spelled with.

The `--- expect plan` of an inspected step reads the explain document of that same `Executed`, never a second planning run; its `ran` lines read the report of that run (`crates/omnigraph-gqt-core/src/report.rs` reads the rows). A parameter refusal, a settings error and a query that failed produce no `Executed` and keep their ordinary checks.

### GQT execution through DST

GQT files select their execution target in a `--- runner` YAML section before
the schema. The file owns its storage, seeds and explicit faults;
the runner preserves GQT assertions and uses isolated seeded processes.
The workspace Cargo configuration enables the Tokio runtime DST cases need
from any directory inside the checkout; a build that overrides it (an env
`RUSTFLAGS` without the cfg, as CI's refusal step does) explicitly refuses
DST cases. The [GQT README](../../crates/omnigraph-gqt/README.md)
defines supported targets, hooks, replay observations, and limits. The configured
CI owner enrolls the complete corpus.

### OpenAPI

`crates/omnigraph-server/tests/openapi.rs` regenerates the specification in memory and compares it with `openapi.json`. For an intentional API change:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 \
  cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date
```

Commit the generated file with the API change. CI checks drift; it never updates the file.

## Cost tests and benchmarks

The active performance workload definitions live in GQT under
[`benchmarks/`](../../benchmarks/README.md). Unsupported maintenance, real
concurrency, HTTP and streaming instruments are preserved in
[`benchmarks/deferred/`](../../benchmarks/deferred/README.md), outside Cargo
discovery. Cleanup and optimization belong in GQ before GQT can exercise them.
Historical HTTP records remain readable by `scripts/analyze-http-perf.py` and
`scripts/analyze-http-retention.py`; acquisition is deferred.

Correctness tests may assert deterministic logical or object-store operation counts when the count is part of the design contract. Wall time and peak RSS depend on the host and belong in the `omnigraph-bench` scenario harness; benchmark results are evidence rather than pass/fail assertions. Declarative benchmark cases and suites live under `benchmarks/`; deterministic engine cost contracts remain in `crates/omnigraph/tests/`.

The benchmark runner executes `gqt-v1` dataset/query pairs through the same
production core used by GQT correctness tests. It selects one engine operation
by ordinal and exact header/body echo, then closes its clock and logical
counters before expectations and subsequent explicit verification. Reads,
mutations, branch controls, single-call generated loads, and engine restart
use the same adapter. Bench wall-clock still requires a qualified release
binary; GQT correctness and DST execution do not acquire benchmark timings.

Every repetition uses a fresh SHA-attested process and restores the dataset
at its stable active path from a never-opened APFS clonefile or verified
Linux/XFS copy. The persistent dataset cache holds its lock through worker
verification and containment. Index requirements are unioned across dataset
and queries and applied with the seed before updates/deletes. This ordering
is exercised by the shipped stale-index pairs.

```bash
RUSTFLAGS= cargo run --release --locked -p omnigraph-bench -- \
  run local-fast --dataset-cache /qualified/cache
```

The empty `RUSTFLAGS` clears the workspace development cfg; the existing
release guard refuses encoded flags and unmodeled runtime overrides.
Children clear their environment, pin locale, and use protocol-owned scratch
siblings for `TMPDIR` and merge staging. Containment must be proved before
cleanup; a later assertion or protocol failure cannot turn Settled timing
into a passing sample.

`gqt_tests.rs` owns public pair/directory discovery, selection, engine receipts,
index preparation, and mixed archive/projection coverage. `dataset_identity`,
`dataset_cache`, and `dataset_worker` own logical history, cache integrity,
and contained building; `gqt_runner`, `gqt_supervisor`, and `gqt_protocol` own
operation boundaries and worker admission. `registered_fixture`, `reset`, and
`environment` retain byte-copy, stable-path, and backend qualification checks.
The retired `real_graph_run` and Rust fixture builder remain test oracles,
with no production execution route.

The slow nightly GQT workflow also explicitly selects the 20 ordinary `benchmarks/fixtures` recipes, including the
full 800,000-row D50 dataset with post-build assertions. Reduced legacy parity
cases validate generator equivalence but do not stand in for that scale.
Dataset/query catalog parsing is a normal correctness test; selected-operation
wall-clock and unpinned benchmark counters remain report-only. The new D50
warm prefix is 24 authored aggregate reads, a distinct cache program from
the retired Rust scans.

Registered FinBench uses the ordinary `gqt-v1` suite with a logical reference,
schema-less GQT preparation, and `--fixture ID=BUNDLE`. `fixture run-graph` and
its run YAML are retired. Current registered sources must be main-only,
relocation-self-contained, and have deterministic node keys. Original source
IDs are additionally hashed before preparation; generated transfer IDs use
explicit logical equivalence. Commands and limits are in the
[benchmark catalog](../../benchmarks/README.md#registered-finbench-merge).

Do not archive diagnostic JSON as telemetry. To publish authoritative
`suite run` records, first commit the exact source under test, build the release binary from
that clean tree, and pass `--archive <DIR>`. The commit records source
provenance; the executable digest and normalized build/engine facts bind the
exact SUT bytes. Source revalidation compares raw tracked source bytes without
Git clean filters, disables replacement objects and permissive stat-cache
modes, and refuses hidden index flags or ignored untracked source inputs.
Profile-file LTO/codegen/strip values are declarations, not
effective compiler facts: Cargo does not expose the final target rustc command
to build scripts, so records mark effective codegen options unproved until
controlled infrastructure supplies a digest-bound receipt. Raw timing records remain
useful evidence, but that absence cannot authorize a performance conclusion.
Accordingly, the projection reports `claim_eligible: false` even for complete
local acquisitions until a controlled digest-bound build receipt supplies that
proof. Acquisition status and global claim eligibility are separate facts.
Validate records independently with
`archive verify`; rebuild the disposable OmniGraph read model with `projection
rebuild --archive <DIR> --root <DIR>`. The content-addressed canonical JSON is
authority. Projection generations and `CURRENT` may always be deleted and
rebuilt from it. Archive verification streams a fixed invocation inventory;
archive writers and inventory capture coordinate at the immutable pointer
publication boundary. The current publication guarantee is local Unix
file/directory durability through every descriptor-rooted ancestor back to the
captured archive root. Readers fix the pointer inventory under the publication
lock, then durability-close each yielded record once or fail; a substantive sync failure after pointer visibility is
`possibly_published`, never success. Projection queries and rebuild verification use bounded,
exclusive pages whose continuation cursors are pinned to an immutable
generation, and publication verifies a canonical digest over every projected
field rather than keys alone.

Process-effective machine evidence is captured in each isolated repetition
worker immediately before it declares readiness. All repetitions in a run must
match exactly; the CLI does not reuse a session-start machine snapshot.

Archive-mode suite execution publishes and releases each complete raw run
before starting the next suite entry. Its command result contains a completed
count and immutable receipts instead of duplicating the raw samples already in
the authoritative records. If pointer visibility succeeds but bounded
directory-sync recovery cannot prove durability, the JSON failure includes a
`possibly_published` identity. Pass its invocation and record digest to
`archive reconcile`; only a `durable`, `absent`, or `conflict` result closes the
specific ambiguity. Do that before retrying under a different invocation. If
an acquisition fails after at least one fully verified repetition, the CLI
publishes only that prefix as a `censored`, permanently claim-ineligible record
and still exits nonzero. A rep-zero failure publishes nothing, and a merely
settled repetition never enters durable samples. If
record construction or publication fails before authority exists, the bounded
failure output retains that one complete execution or censored verified prefix
as state-neutral `unpublished_run` evidence. Human mode prints the same complete
JSON envelope.

Keep measurement fixtures separate from production schemas and recovery state. A no-go result belongs in the RFC or issue that consumed the experiment, not as a permanent narrative in this map.

## Before every task

1. Read [invariants.md](invariants.md).
2. Use [lance.md](lance.md) to identify and read every relevant full upstream page.
3. Search existing tests by public API, error variant, route, and durable object name.
4. Run the narrowest existing owner as a clean baseline.
5. Extend that owner unless the behavior crosses a genuinely new public boundary.
6. Run the focused owner again, then the canonical workspace graph in proportion to risk.
7. For docs, workflow, or API changes, also run the repository link/pin/OpenAPI checks that own those generated contracts.
