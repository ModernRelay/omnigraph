# Testing

This file is the always-on map of the test surface. **Consult it before every task** so you know what tests already cover the area you're about to change, what helpers to reuse, and where a new test belongs. The architectural invariant for boundary-matched tests lives in [docs/dev/invariants.md](invariants.md).

## Where tests live, per crate

| Crate | Path | Style |
|---|---|---|
| `omnigraph` (engine) | `crates/omnigraph/tests/` | Integration tests (29 files), fixture-driven, share `tests/helpers/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | Per-area suites (post-modularization): `cli_cluster.rs` (cluster command surface + operator-actor cascade), `cli_cluster_e2e.rs` (spawned-binary lifecycle compositions — lost-state re-import recovery, out-of-band drift, graph-root destruction, multi-graph mixed-disposition convergence), `cli_data.rs` (load/read/change/branch/commit/export/snapshot/policy/embed/maintenance + operator format cascade), `cli_schema_config.rs` (init/config, schema plan/apply), `cli_queries.rs`, `parity_matrix.rs` (RFC-009 Phase 1: the embedded-vs-remote referee — every forked verb run against both arms with matched Cedar policy and the same actor, scrubbed-JSON + exit-code equality; divergences are pinned in its `KNOWN_DIVERGENCES` ledger, never silently repaired), `system_local.rs` (full-cycle cluster lifecycle with a spawned `--cluster` server, applied-policy enforcement over HTTP, keyed-credential auth, operator aliases), `system_remote.rs`; share `tests/support/mod.rs` (hermetic `OMNIGRAPH_HOME` by default) |
| `omnigraph-cluster` | mostly in-source `#[cfg(test)] mod tests`; `tests/failpoints.rs` (feature-gated); `tests/s3_cluster.rs` (bucket-gated full lifecycle on object storage) | Cluster config parser, local JSON state diff, state CAS/lock handling/recovery, read-only validate/plan/status plus explicit refresh/import graph observations, config-only apply (content-addressed payload publish, disposition gating, composite-digest convergence, idempotent re-apply), catalog payload verification (status read-only, refresh drift + self-heal), failpoint crash-mid-apply / CAS-race coverage, Stage 4A graph creation (create executor, recovery sidecars + sweep rows, create crash windows), Stage 4B schema apply (migration previews in plan, schema executor, schema-apply sweep classification, schema crash windows), Stage 4C gated deletes (digest-bound approvals, delete executor + tombstones, delete sweep rows, delete crash windows), and 5A policy binding metadata (applies_to in the applied revision, binding-change diffing + convergence, pre-5A backfill), and the 5B serving-snapshot read API (converged read, refusal rows) |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | Per-area suites (post-modularization): `auth_policy.rs`, `data_routes.rs`, `schema_routes.rs`, `stored_queries.rs`, `multi_graph.rs` (cluster-mode boot — converged serving, policy binding wiring, boot refusals — + the concurrent branch-ops matrix), `boot_settings.rs` (mode inference, PolicySource), `s3.rs` (bucket-gated: single-graph serving + config-free `--cluster s3://` boot), `openapi.rs` (OpenAPI drift / regeneration); share `tests/support/mod.rs` |
| `omnigraph-compiler` | mostly in-source `#[cfg(test)] mod tests` | Parser, type-checker, IR lowering, lint |
| `omnigraph-dst` (DST harness) | `crates/omnigraph-dst/` | Dev-only, non-default workspace member, `publish=false`. The shared test-harness foundation: a `Backend` trait (`Embedded` with white-box `db()` + `Cli` subprocess), the `Model` reference oracle, the `OpKind`/`Rng`/`step` op alphabet, the white-box invariant battery + structured `classify`/`Finding` registry (`run_battery`), `FaultAdapter` (storage-CAS-loss injection), and the multi-writer `Cohort` (N independent coordinators on one store) + `convergence` battery (`run_convergence_battery`: linear-chain / chain-len / all-writers-converge). The stable core of #309; the experimental generative-walk / fuzz / cross-backend / coverage suites keep reviewing on `dst-extract-crate`. |

The engine's `tests/` is the principal coverage surface; most graph-shaped behavior is exercised there.

## Engine integration tests (`crates/omnigraph/tests/`)

| File | Covers |
|---|---|
| `end_to_end.rs` | Full init → load → query/mutate flow |
| `branching.rs` | Branch create / list / delete, lazy fork |
| `merge_truth_table.rs` | Merge-pair truth table (MR-786): all 9×9 `(left_op, right_op)` cells from `{noop, addNode, removeNode, addEdge, removeEdge, setProperty, dropProperty, addLabel, removeLabel}`. Adding a new op to `OpVariant` forces a compile error in `build_case` until the new row + column are dispositioned. 36 executable cells run through real `branch_merge` with a structured oracle (`MergeOutcome` / `MergeConflictKind` + graph-state assert); 45 cells involving `dropProperty`/`addLabel`/`removeLabel` are recorded as `Unsupported` until the mutation grammar grows. |
| `writes.rs` | Direct-publish writes: cancellation, non-strict insert/merge rebase under the per-table queue, strict stale-write conflicts, multi-statement atomicity, MR-794 staged-write rewire (D₂ rejection, insert+update coalesce, multi-append coalesce, partial-failure recovery, load RI/cardinality recovery) |
| `staged_writes.rs` | TableStore staged-write primitives (`stage_append`, `stage_merge_insert`, `commit_staged`, `scan_with_staged`, `count_rows_with_staged`) — primitive-level only; engine code uses the in-memory `MutationStaging` accumulator instead |
| `cross_process_writes.rs` | Cross-coordinator (multi-writer) writes via `omnigraph_dst::Cohort` (N INDEPENDENT `Omnigraph` handles, separate `known_state`): concurrent disjoint-table writes on one branch serialize into a LINEAR lineage chain (the `graph_head:<branch>` §7.1 CAS), no fork / no lost write, both coordinators converge (`run_convergence_battery`). The safety net Phase 3's warm-attempt-0 publish is built against (regime-independent: holds today and after the warm path lands). The deterministic stale-warm CAS-loss and fork-reclaim rendezvous cells live in `failpoints.rs` (process-global `fail` registry) |
| `forbidden_apis.rs` | Defense-in-depth source-walk guard: engine code (`exec/`, `db/omnigraph/`, `loader/`, `changes/`) must not reach around the sealed storage trait to Lance inline-commit APIs, nor open datasets directly (`Dataset::open` / `DatasetBuilder::from_uri`/`from_namespace`) — reads route through `Snapshot::open` and the held-handle cache; `// forbidden-api-allow: <reason>` sentinel exempts reviewed lines |
| `lance_surface_guards.rs` | Pins the Lance API surfaces omnigraph depends on (named runtime + compile-only guards; see [lance.md](lance.md)) — the first smoke check on any Lance version bump; e.g. `compact_files_still_fails_on_blob_columns` turns red when the upstream blob-compaction fix lands |
| `warm_read_cost.rs` | Cost-budget tests for the warm read path (query-latency work), measured at the object-store boundary with Lance `IOTracker` (the LanceDB IO-counted pattern): a warm same-branch read does 0 manifest opens, 0 commit-graph opens, 1 version probe, validates the schema once (Fix 1 / finding A / Fix 2 at commit-history depth); stale same-branch reads perform exactly 2 probes and refresh manifest-only; recreated non-main branches with the same Lance version refresh by incarnation; recreated branch-owned table handles are distinguished by table e_tag or refresh-time cache clearing; recreated traversal topology is protected by synthetic snapshot-id incarnation or refresh-time cache clearing; a warm *repeat* read does 0 table opens via the held-handle cache and a write re-opens only the changed table at its new version/e_tag (Fix 3/6A). See "Cost-budget tests" below |
| `write_cost.rs` | Cost-budget tests for the WRITE path (RFC-013), the latency twin of `warm_read_cost.rs` on the **shared `helpers::cost` harness** (`measure`/`IoCounts`/`assert_flat`/`local_graph`). Runs on **local FS**; gates the **internal-table** term (`__manifest` scans flat in commit-history depth, lineage rows included — `internal_table_scans_are_flat_in_history`, now **green every-PR** since RFC-013 step 2 brought the internal tables into `optimize`; the test compacts at each depth before measuring) plus green every-PR guards (single-insert `data_writes` bounded, a per-write read-op ceiling that fails the moment a round-trip is added, and a `measure_with_staged` fitness assert that a keyed insert routes through `stage_merge_insert` once with no `stage_append`/vector-index build). The **data-table opener** term is S3-only — see `write_cost_s3.rs` and the backend-split note in "Cost-budget tests" below |
| `helpers/cost.rs` | The shared cost-budget harness (not a test): `IoCounts`/`StagedCounts` (counts by table class), `measure`/`measure_with_staged` (the one place the `with_query_io_probes` + `MergeWriteProbes` task-local + `IOTracker` wiring lives; reads per-op deltas via lance's `incremental_stats()`, the upstream per-request idiom from `rust/lance/src/dataset/tests/dataset_io.rs`), `cost_harness`/`GraphIoMeter` (installs ONE `__manifest` `IOTracker` for a whole test body so the graph opens **under** it and `manifest_reads` is **ground truth** — every read regardless of handle age, the warm-coordinator freshness probe included — closing the blind spot where a per-op tracker installed at measure time cannot see a long-lived handle's reads; outside `cost_harness`, `measure` falls back to fresh per-op tracking, so `write_cost_s3.rs` is unaffected), `last_manifest_reads()` (the manifest read log for `assert_io_eq!`-style failure diagnostics), `assert_flat(curve, select, slack, what)`, and store-agnostic `local_graph`/`s3_graph` fixtures. `warm_read_cost.rs`, `write_cost.rs`, and `write_cost_s3.rs` all consume it so a cost test body is written once and reads in one vocabulary |
| `lifecycle.rs` | Graph lifecycle, schema state |
| `point_in_time.rs` | Snapshots, time travel (`snapshot_at_version`, `entity_at`) |
| `changes.rs` | `diff_between` / `diff_commits` |
| `consistency.rs` | Cross-table snapshot isolation, atomic publish |
| `schema_apply.rs` | Migration plan + apply, schema-apply lock; index materialization deferred to the reconciler (iss-848): `apply_schema_defers_vector_index_on_empty_table` (an empty-table Vector `@index` never aborts the apply) and `index_only_constraint_apply_touches_no_table_data` (adding an `@index` is metadata-only — no table-version bump) |
| `search.rs` | FTS / vector / hybrid (`bm25`, `nearest`, `rrf`) |
| `traversal.rs` | `Expand`, variable-length hops, anti-join (CSR path — `OMNIGRAPH_TRAVERSAL_MODE` unset) |
| `traversal_indexed.rs` | BTREE-indexed Expand (`execute_expand_indexed`) forced via `OMNIGRAPH_TRAVERSAL_MODE`, asserted semantically equal to the CSR path; own binary, all `#[serial]` so env writes never race |
| `proptest_equivalence.rs` | Property-based query-correctness invariants over generated graphs (shared key alphabet forces cross-type id collisions, cycles, self-loops) — pins Expand-mode equivalence so a future fork divergence fails loudly instead of silently; `#[serial]` |
| `ordering.rs` | ORDER BY contract: descending, multi-key precedence, deterministic key-column tie-break (total order, so `ORDER … LIMIT` is deterministic), NULL placement (`nulls_first = !descending`) |
| `literal_filters.rs` | Execution goldens for non-string/non-integer scalar literal filters (F64/F32/Bool/Date/DateTime) across both the in-memory comparison arm and the Lance-pushdown arm |
| `aggregation.rs` | `count`, `sum`, `avg`, `min`, `max` |
| `export.rs` | NDJSON streaming export filters |
| `s3_storage.rs` | S3-backed graph (skipped unless `OMNIGRAPH_S3_TEST_BUCKET` is set) |
| `lance_version_columns.rs` | Per-row `_row_last_updated_at_version` behavior |
| `validators.rs` | Schema constraint enforcement (enum, range, unique, cardinality) across JSONL, insert, update paths |
| `policy_engine_chassis.rs` | Engine-layer Cedar enforcement (MR-722): allow + deny through every `_as` writer via the SDK directly — no HTTP — proving embedded and CLI callers hit the same gate as the server, with action × scope shapes matching `authorize_request` |
| `maintenance.rs` | `optimize` (compaction), `repair` (explicit uncovered-drift publish), and `cleanup` (version GC): empty/idempotent/no-op edges, policy validation, head preservation; `optimize` publishes its own compaction (`optimize_publishes_compaction_to_manifest_so_schema_apply_succeeds`), skips pre-existing uncovered drift (`optimize_skips_preexisting_manifest_head_drift`), and refuses to run while a `__recovery` sidecar is pending (`optimize_defers_when_recovery_sidecar_is_pending`); `repair` previews/heals verified maintenance drift, refuses raw semantic drift without `--force`, and forced repair publishes only by explicit operator choice; the index reconciler (iss-848): `index_build_tolerates_null_vector_rows` (an untrainable Vector column defers instead of aborting the build, sibling indexes still build) and `optimize_materializes_index_declared_but_unbuilt` (optimize creates a declared-but-deferred index) |
| `failpoints.rs` | Failure-injection coverage (gated on `failpoints` feature). Includes the five per-writer Phase B → recovery integration tests (`recovery_rolls_forward_after_finalize_publisher_failure`, `schema_apply_phase_b_failure_recovered_on_next_open`, `branch_merge_phase_b_failure_recovered_on_next_open`, `ensure_indices_phase_b_failure_recovered_on_next_open`, `optimize_phase_b_failure_recovered_on_next_open`) and the write-entry in-process heal contract (the four `*_after_finalize_publisher_failure_heals_without_reopen` tests — load, mutation, schema apply, branch merge: a follow-up write on the same handle rolls a sidecar-covered residual forward without reopen/refresh) and the storage-fault matrix for the sidecar lifecycle (`recovery.sidecar_{write,delete,list}` / `recovery.record_audit` failpoints: Phase A put failure aborts with zero drift, Phase D delete failure is swallowed and healed by the next write, list failures are loud at heal and open, audit-append failures are retried to exactly one audit row; plus the bucket-gated `s3_load_recovers_after_publisher_failure_without_reopen`). And the convergence-idempotent roll-forward regression (`open_sweep_roll_forward_converges_when_manifest_advances_concurrently`: two concurrent open-sweeps race one sidecar at the `recovery.before_roll_forward_publish` rendezvous; the CAS loser must converge, not fail the open — iss-schema-apply-reopen-recovery-race). And the cross-coordinator stale-warm publish cell (`stale_warm_publish_precheck_loses_cas_then_converges`): two independent `omnigraph_dst::Cohort` coordinators where writer A parks at `graph_publish.before_commit_append` while writer B advances the manifest + `graph_head:main`, so A's released append loses the CAS and the publisher retries internally (cold reload + re-parent) — the safety net Phase 3's warm-attempt-0 publish is built against. |
| `recovery.rs` | Open-time recovery sweep — sidecar I/O, classifier dispatch (NoMovement / RolledPastExpected / UnexpectedAtP1 / UnexpectedMultistep / InvariantViolation), all-or-nothing decision, roll-forward via `ManifestBatchPublisher::publish`, roll-back via `Dataset::restore`, audit row in `_graph_commit_recoveries.lance`, `OpenMode::ReadOnly` skip path |
| `composite_flow.rs` | Compositional/narrative end-to-end stories — multi-step flows that compose mechanics covered by other test files. Catches integration regressions where individual operations all pass their unit tests but their composition breaks (sequential merges, post-merge main writes, time-travel through merge DAG, reopen consistency over multi-merge histories, post-optimize and post-cleanup strict writes). |

## Fixtures

`crates/omnigraph/tests/fixtures/` holds the canonical schema (`.pg`), seed data (`.jsonl`), and queries (`.gq`) shared across tests. Reuse these before inventing new ones — the helpers harness already knows how to load them.

## Test helpers

- **Engine** — `crates/omnigraph/tests/helpers/mod.rs`: `init_and_load()` (bootstrap a temp graph + load standard fixture), `snapshot_main()`, `snapshot_branch()`, query/mutation runners, row collection and counting. Use these instead of hand-rolling.
- **CLI** — `crates/omnigraph-cli/tests/support/mod.rs`: `Command`-style wrapper for invoking `omnigraph`, server-process spawning, fixture resolution, output assertion helpers.
- **Server** — no shared helpers; server tests call the `Omnigraph` engine API directly and exercise endpoints over the wire.

> Note: the storage adapter has an in-memory backend (`ObjectStorageAdapter::in_memory()`, full contract including true conditional updates) used by the adapter contract tests in `storage.rs`. It covers only the text-object layer (sidecars, schema staging, cluster state) — **Lance datasets bypass the adapter**, so engine integration tests still use `tempfile::tempdir()`. An in-memory Lance substrate remains an architectural ask — keep it explicit in [docs/dev/invariants.md](invariants.md) under known gaps.

## Failpoints (fault injection)

- Cargo feature: `failpoints = ["dep:fail", "fail/failpoints"]` in `crates/omnigraph/Cargo.toml`; the cluster's `failpoints` feature additionally enables `omnigraph/failpoints` (`crates/omnigraph-cluster/Cargo.toml`), so the shared test guard is available to cluster tests.
- Wrappers: `crates/omnigraph/src/failpoints.rs` and `crates/omnigraph-cluster/src/failpoints.rs` each expose `maybe_fail("name")` (per-crate error type). The test-side config guard `ScopedFailPoint` (`new` for action strings, `with_callback` for callbacks; RAII `Drop` removes the point) lives **once** in the engine and is reused by both test binaries.
- **Names are compile-checked.** Every failpoint name is a `pub const` in `omnigraph::failpoints::names` (engine) / `omnigraph_cluster::failpoints::names` (cluster). Call sites and tests reference the constant, never a bare literal — a typo is a compile error, not a silently-never-firing point. Add a new failpoint by adding its const first.
- Call sites are inserted at sensitive transaction boundaries (branch create, graph publish commit, the recovery sweep's classify→roll-forward-publish window, cluster apply's payload→state-write window, etc.).
- **Serialize and rendezvous, never sleep.** The `fail` registry is process-global, so every failpoint test carries `#[serial]` (`serial_test`). For concurrent tests, use `helpers::failpoint::Rendezvous` (`tests/helpers/failpoint.rs`): `park_first(name)` parks the first thread to hit the point until `release()`, and `wait_until_reached().await` blocks on that condition (it doubles as a fired-assertion). Do not coordinate threads with fixed `sleep`s.
- Activated tests: `crates/omnigraph/tests/failpoints.rs` and `crates/omnigraph-cluster/tests/failpoints.rs` (integration binaries, never in-source — the fail registry is process-global). Run with `cargo test -p omnigraph-engine --features failpoints --test failpoints` / `cargo test -p omnigraph-cluster --features failpoints --test failpoints`.

## RustFS / S3 integration

CI runs these S3-backed tests against a containerized RustFS server (`.github/workflows/ci.yml` → `rustfs_integration` job):

- `cargo test -p omnigraph-engine --test s3_storage`
- `cargo test -p omnigraph-engine --test write_cost_s3` (RFC-013 step 3a's data-table opener cost gate — flat across commit depth on S3; the term local FS can't reproduce)
- `cargo test -p omnigraph-server --test s3` (single-graph serving + config-free `--cluster s3://` boot)
- `cargo test -p omnigraph-cluster --test s3_cluster` (full control-plane lifecycle on the bucket)
- `cargo test -p omnigraph-cli --test system_local local_cli_s3_end_to_end_init_load_read_flow`
- `cargo test -p omnigraph-engine --features failpoints --test failpoints s3_` (recovery-sidecar lifecycle on a real bucket)

Locally, set `OMNIGRAPH_S3_TEST_BUCKET` (and the usual `AWS_*` vars including `AWS_ENDPOINT_URL_S3` for non-AWS) before running. Without those, S3 tests skip gracefully.

## System e2e requirements and suppression

The CLI system tests (`system_local.rs`) spawn the workspace-built `omnigraph` and `omnigraph-server` binaries (cargo provides paths via `CARGO_BIN_EXE_*`), bind ephemeral localhost ports, and use local-FS temp dirs — no external services, no env vars required; they run in the default `cargo test --workspace`. The comprehensive cluster lifecycle e2es (multi-server-restart flows) honor an opt-out for constrained sandboxes: set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` to skip them with a logged message (the same graceful-skip pattern as the S3 gate). Cargo-native filtering also works: `cargo test --test system_local -- --skip local_cluster`.

## OpenAPI drift

`crates/omnigraph-server/tests/openapi.rs` regenerates `openapi.json` and diffs against the checked-in copy. CI auto-commits the regeneration on same-repository PRs and otherwise runs in strict-check mode (env: `OMNIGRAPH_UPDATE_OPENAPI`).

## Examples & benches

- `crates/omnigraph/examples/bench_expand.rs` — runnable example (not part of CI).
- No `benches/` directories. Add `benches/` per crate when you ship a perf-driven change, and include the motivating workload with the optimization.

## Coverage tooling — what's missing

There is **no** coverage tooling in the repository today: no `tarpaulin.toml`, no `codecov.yml`, no coverage CI step. If you want to know whether your change is covered, the answer comes from reading and running the relevant integration tests, not from a tool.

If introducing coverage tooling is in scope for your task, the natural first step is `cargo-llvm-cov` wired into a separate CI job, and a per-crate threshold rather than a global one.

## First principle: check what already covers it

**Before writing any new test, check whether an existing test already covers the case.** The cost of duplicating coverage is high: more code to read, more places to keep in sync when behavior changes, and more drift when one copy lags. The cost of *extending* an existing test is usually one extra assertion or one extra fixture row.

How to check:

1. **Map the change to an area** — use the engine integration-test table above (`branching.rs`, `writes.rs`, `search.rs`, etc.). The filename usually names the area.
2. **Open the file and skim every test fn name.** Test fn names are the index — read them all, not just the first few.
3. **Grep for the symbol or path you're changing.** `rg <FunctionName>` or `rg <enum_variant>` across all `tests/` directories surfaces existing coverage you might miss.
4. **Decide one of three outcomes**, in this order of preference:
   - *Existing test already asserts the new behavior* → no new test needed; this PR is a refactor or no-op behaviorally. Confirm by running the existing test against the change.
   - *Existing test covers the area but not your case* → **add an assertion or a fixture row to the existing test**, don't write a new function with `init_and_load()` again.
   - *No existing coverage in any test file* → only then write a new test; put it in the file that owns the area, or open a new file only if the area itself is new.

Three duplicated `init_and_load() → run_query → assert_eq` blocks where one parameterized test would do is the most common form of test rot in this repository. Don't add to it.

## Before-every-task checklist

When you pick up any change, walk through this:

1. **Find existing coverage** (per the principle above). Don't just look at the first test file by name — grep for the symbol you're touching across every crate's `tests/`.
2. **Run those tests locally before editing.** `cargo test --workspace --locked` for the broad pass; `-p <crate> --test <file>` for a focused loop. Confirm a clean baseline.
3. **Decide extend-vs-new** explicitly. If you can extend an existing test (assertion, fixture row, parameterization), do that. Only add a new test fn or new file if no existing one owns the area.
4. **Reuse the helpers.** `init_and_load()`, fixture files, the CLI `support` harness — re-use them. Don't bootstrap a fresh graph by hand if a helper exists.
5. **Mind the boundary.** Per [docs/dev/invariants.md](invariants.md), test at the layer the change lives at — planner-level changes deserve planner-level tests, not just end-to-end.
6. **For substrate-touching changes** (Lance behavior), reach for `failpoints` or fixture-driven scenarios, not stubbed-out mocks.
7. **For server / API changes**, confirm the OpenAPI regeneration happens in `openapi.rs` and that the diff lands in `openapi.json`.
8. **Verify your change makes an existing test fail before it makes the new one pass.** If you can break the code without breaking a test, your coverage gap is the problem to fix first.
9. **Bound hot-path cost at history depth.** If the change touches a read, **write**, or open path, add or extend a test that asserts a *bounded* cost (e.g. a warm same-branch read performs zero `Dataset::open`, or a per-write read-op count flat across commit depth) against a fixture with realistic *commit-history depth*, not just realistic row counts. Reuse the shared `helpers::cost` harness (`measure`/`IoCounts`/`assert_flat`) — don't hand-roll `IOTracker` wiring. Cost that scales with history is invisible on a shallow fixture and only bites in production. See "Cost-budget tests" below.

## Cost-budget tests: bound hot-path cost at history depth

Correctness bugs fail loudly in tests; cost-scaling bugs pass every test and degrade silently in production. The engine read path historically had no cost assertion, and fixtures carry shallow commit history, so an O(commits)-per-query cost stayed green in CI and only surfaced on a long-lived graph (read snapshot resolution re-scanned the internal manifest and commit-graph tables on every query, and those tables were never compacted). Guard against the class:

- **Assert a cost budget, not just a result.** For a read/open path, assert the number of `Dataset::open` calls (or object-store ops) a warm query performs, and that it does not grow with commit count. The reference is LanceDB's IO-counted tests, which assert a cached read costs 0-1 IO and carry a named regression test against "a list call on every subsequent query."
- **Test at history depth.** Build a fixture with many *commits* (not many rows) and assert warm-read cost is flat across depths. A shallow fixture cannot catch an O(commits) cost.
- **Use the shared harness, and gate each term on the backend where it manifests.** `helpers::cost` (`measure`/`IoCounts`/`assert_flat`/`local_graph`/`s3_graph`) is the one place the `IOTracker`/task-local plumbing lives — consume it, don't duplicate it. The write path has *two distinct* depth terms that split cleanly across backends, and conflating them is a real trap (the local data-table read count grows with depth too, but for a different reason — the merge-insert/RI scan reading O(depth) *fragments*, reduced by compaction, not by the opener): (1) the **internal-table** scan term (`__manifest` fragment scans, lineage rows included) reproduces on **any** backend including local FS, so `write_cost.rs` gates it on local every-PR; (2) the **data-table opener** term (latest-version resolution) is a per-object-store-RPC phenomenon — local-FS resolves latest with one cheap `read_dir` regardless of the opener used, so the namespace-vs-direct difference is **invisible on local** and only shows on a real object store (per-version GETs), gated by the bucket-gated `write_cost_s3.rs`. Same harness, different fixture; each term asserted where it actually appears.
- **Count on the handle that does the reads, not just the one a measured op opens.** Lance's IO-counted tests attach the `IOTracker` to the (warm, cached) dataset and read `incremental_stats()` per request — the tracker MUST be on the handle performing the reads, or warm-handle reads escape. A per-op tracker installed at measure time cannot see reads on a long-lived handle opened earlier (the warm coordinator's `__manifest` handle, reused across writes), so such reads were silently undercounted. Wrap a depth-swept body in `cost_harness` so the manifest tracker is installed before the graph opens and `manifest_reads` is **ground truth** (handle-age-irrelevant). The `version_probes` counter is the freshness-probe *call* count; ground truth additionally reveals that a write's probe does ~3 object-store RPCs (a read's probe is a 0-IO cache hit). `manifest_reads_capture_warm_probe` is the guard that this stays true.
- This is the testing companion to invariant 15 in [docs/dev/invariants.md](invariants.md) (hot-path cost is bounded by work, not history).

When in doubt, re-read [docs/dev/invariants.md](invariants.md) — quality gates apply to every change.
