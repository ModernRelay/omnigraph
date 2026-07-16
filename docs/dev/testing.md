# Testing

This file is the always-on map of the test surface. **Consult it before every task** so you know what tests already cover the area you're about to change, what helpers to reuse, and where a new test belongs. The architectural invariant for boundary-matched tests lives in [docs/dev/invariants.md](invariants.md).

## Where tests live, per crate

| Crate | Path | Style |
|---|---|---|
| `omnigraph` (engine) | `crates/omnigraph/tests/` | Integration tests (one file per behavior area — see the table below), fixture-driven, share `tests/helpers/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | Per-area suites (post-modularization): `cli_cluster.rs` (cluster command surface + operator-actor cascade), `cli_cluster_e2e.rs` (spawned-binary lifecycle compositions — lost-state re-import recovery, out-of-band drift, graph-root destruction, multi-graph mixed-disposition convergence), `cli_data.rs` (load/read/change/branch/commit/export/snapshot/policy/embed/maintenance + operator format cascade), `cli_schema_config.rs` (init/config, schema plan/apply), `cli_queries.rs`, `parity_matrix.rs` (RFC-009 Phase 1: the embedded-vs-remote referee — every forked verb run against both arms with matched Cedar policy and the same actor, scrubbed-JSON + exit-code equality; divergences are pinned in its `KNOWN_DIVERGENCES` ledger, never silently repaired), `system_local.rs` (full-cycle cluster lifecycle with a spawned `--cluster` server, applied-policy enforcement over HTTP, keyed-credential auth, operator aliases), `system_remote.rs`, `crossversion_upgrade.rs` (gated genuine v3/v4→v6 and v5↔v6 rebuild/refusal harness — see below); share `tests/support/mod.rs` (hermetic `OMNIGRAPH_HOME` by default) |
| `omnigraph-cluster` | mostly in-source `#[cfg(test)] mod tests`; `tests/failpoints.rs` (feature-gated); `tests/s3_cluster.rs` (bucket-gated full lifecycle on object storage) | Cluster config parser, local JSON state diff, state CAS/lock handling/recovery, read-only validate/plan/status plus explicit refresh/import graph observations, config-only apply (content-addressed payload publish, disposition gating, composite-digest convergence, idempotent re-apply), catalog payload verification (status read-only, refresh drift + self-heal), failpoint crash-mid-apply / CAS-race coverage, Stage 4A graph creation (create executor, recovery sidecars + sweep rows, create crash windows), Stage 4B schema apply (migration previews in plan, schema executor, schema-apply sweep classification, schema crash windows), Stage 4C gated deletes (digest-bound approvals, delete executor + tombstones, delete sweep rows, delete crash windows), and 5A policy binding metadata (applies_to in the applied revision, binding-change diffing + convergence, pre-5A backfill), and the 5B serving-snapshot read API (converged read, refusal rows) |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | Per-area suites (post-modularization): `auth_policy.rs`, `data_routes.rs`, `schema_routes.rs`, `stored_queries.rs`, `multi_graph.rs` (cluster-mode boot — converged serving, policy binding wiring, boot refusals — + the concurrent branch-ops matrix), `boot_settings.rs` (mode inference, PolicySource), `s3.rs` (bucket-gated: single-graph serving + config-free `--cluster s3://` boot), `openapi.rs` (OpenAPI drift / regeneration); share `tests/support/mod.rs` |
| `omnigraph-compiler` | mostly in-source `#[cfg(test)] mod tests` | Parser, type-checker, IR lowering, lint. Schema parser and SchemaIR validation tests both reject the five exact Lance virtual system-column property names while preserving near-miss identifiers |

The engine's `tests/` is the principal coverage surface; most graph-shaped behavior is exercised there.

## Engine integration tests (`crates/omnigraph/tests/`)

| File | Covers |
|---|---|
| `end_to_end.rs` | Full init → load → query/mutate flow; blob coverage includes `blob_read_after_mutation_insert`, where a handle opened before another handle's commit must freshness-probe and read the newly committed blob, plus `blob_load_external_file_uri`, which proves Overwrite retains an external URI reference |
| `branching.rs` | Branch create/list/delete and lazy fork; native-control hardening includes main and named-source clone-only create recovery, invalid-name-before-clone, live path-prefix namespace rejection, legacy prefix-collision leaf-first delete, and delete/recreate first-write safety. The control path captures one operation-local accepted catalog plus fresh manifest/namespace view after the complete gate envelope rather than refreshing the handle-local coordinator around table-gate acquisition; `forbidden_apis.rs::native_branch_controls_use_post_gate_captures_not_handle_refreshes` structurally pins that shape and post-success cache invalidation. RFC-023 pins exact-`id` PK metadata both on an inherited feature snapshot and after the first write materializes its lazy `Person` fork. `branch_merge_with_external_blob_uri_materializes_payload` proves a `LoadMode::Append` strict insert materializes an external URI cell and the later fenced branch merge preserves readable bytes. `branch_merge_rejects_oversized_blob_payloads_pre_effect` proves that one external blob above 32 MiB, or several blob columns whose materialized row total exceeds 32 MiB, returns typed `ResourceLimitExceeded` before raw HEAD, manifest, table pin, row image, lineage, or sidecar movement. The lower classifier truth cells (absent-ref/tree-present delete, same-identifier native refusal, recreated-identifier typed conflict with JSON details) live in `src/branch_control.rs` unit tests |
| `merge_truth_table.rs` | Merge-pair truth table (MR-786): all 9×9 `(left_op, right_op)` cells from `{noop, addNode, removeNode, addEdge, removeEdge, setProperty, dropProperty, addLabel, removeLabel}`. Adding a new op to `OpVariant` forces a compile error in `build_case` until the new row + column are dispositioned. 36 executable cells run through real `branch_merge` with a structured oracle (`MergeOutcome` / `MergeConflictKind` + graph-state assert); 45 cells involving `dropProperty`/`addLabel`/`removeLabel` are recorded as `Unsupported` until the mutation grammar grows. |
| `merge_fast_forward.rs` | Branch-adopt cost + correctness under RFC-023. The one-batch and 8,193-row fixtures prove that a complete v1 insertion-absence history chain publishes one/two bounded exact-`id` filtered `Update` transactions with zero target strict-insert preflights, target MergeInsert joins, committed Appends, ordered-cursor scans, or whole-delta staged combines. `pure_insert_fast_forward_retains_value_constraint_validation` proves the certificate skips only redundant key work, not logical row constraints; its all-new Upsert source is certified from completed effect statistics. `proven_fast_forward_certificate_composes_across_merge_generation` proves the publisher re-mints v1 and a second merge consumes that output as the next proof-chain link. A missing intermediate transaction proves cleaned history is an optimization miss: the merge enters the general ordered diff, preserves exact rows, and leaves no recovery residue. `lazy_target_ref_only_fast_forward_uses_pin_after_main_advances` distinguishes a valid old lazy graph pin from drift when the inherited main ref advances. A nested `main → feature → experiment` cell prevents a deeper valid `BranchIdentifier` from becoming a false read-set conflict. Every general-route base/source/target `OrderedTableCursor` scan applies both Lance `batch_size(8,192)` and `batch_size_bytes(32 MiB)`. Validation streams projected `id`/`src`/`dst`/scalar batches, charges exact Arrow memory before retention, and shares one 32 MiB operation-wide budget across candidate tables; `branch_merge_validation_delta_is_aggregate_bounded_pre_arm` crosses it with two individually valid ~18 MiB deltas while proving zero HEAD/manifest/lineage/sidecar movement. Deletes use exact escaped-filter chunks with the same row/byte and retained-plan bounds. Production-helper unit cells pin chain/delete/recovery limits. The subprocess scenario owns the final production latency/RSS evidence; these integration tests own route semantics, not timings |
| `writes.rs` | Direct-publish writes: cancellation, RFC-022 non-strict full-attempt reprepare from fresh branch authority, strict stale-write conflicts, multi-statement atomicity, MR-794 staged-write rewire (D₂ rejection, insert+update coalesce, multi-append coalesce, partial-failure recovery, load RI/cardinality recovery); RFC-023 pins the inclusive 8,192-row keyed input ceiling, the same exact/+1 boundary on streamed mutation-update matches, no-effect state for both refusals, and oversized stored-Blob rejection before payload read. Crate-internal pending-scan cells pin inclusive/+1 32 MiB accounting plus pending-key shadow-before-charge. The lance#7444 row-id-overlap regression (`filtered_read_after_merge_update_and_delete_keeps_row_ids_consistent` — merge-load → same-key merge-load → delete → keyed point lookup, green only under the vendored lance-table patch — plus its append-only control) |
| `src/table_store/staged_tests.rs` | Crate-internal staged primitives. RFC-023 pins one exact target preflight for general StrictInsert, durable v1 mint/commit/reopen/history persistence, exact-`id` filter emission, typed `KeyConflict`, and missing/wrong PK refusal. `all_new_upsert_certifies_insert_absence_and_persists_it_in_history` proves an all-new completed Upsert receives the optional certificate, a mixed/update Upsert does not, unrelated transaction properties survive, and UUID rebinding does not erase it. Proven-insert cells show the opaque path performs zero strict preflights; stages with `InsertBuilder` but commits the full pure-insert `Update` shape (exact parent and `id` filter, `RewriteRows`, no updates/removals, full nested schema preorder, physical rows); persists/re-admits its own output for proof composition; leaves new fragments outside old index coverage; and fails same-key races loudly in proven/proven and proven/general orders. The in-source `exec/merge.rs` certificate unit table rejects missing/unknown properties, wrong parent/filter/full-preorder/mode/offsets, rewrite/removal shapes, missing `physical_rows`, and Append. Source-interval cells pin exact selection, lazy retained-parent splitting, coalescing, and pinned Lance's approximate raw-emission boundary while every normalized/writer chunk remains hard-capped. Generic `stage_append`/`stage_merge_insert` remain primitive tests only. The file also owns index staging and `commit_staged{,_exact}` |
| `forbidden_apis.rs` | Defense-in-depth syntax-tree/source guard over the whole engine. The primary boundary is Rust visibility: raw storage/coordinator/handle-cache modules are crate-private; public `Snapshot::open` returns `SnapshotTable`; and `SnapshotScanner` executes reads without exposing Lance's raw scanner or physical plan. The guard pins those visibility/return-type boundaries, classifies public async inherent `Omnigraph` methods plus loader conveniences, classifies every crate-visible async method on `GraphCoordinator` / `ManifestCoordinator`, and exact-counts registered method/UFCS durable-call shapes including recovery. RFC-023 rejects production graph call sites of generic `stage_append{,_stream}` and `proven_insert_capability_has_one_production_mint_site` pins `ProvenInsertChunk::from_verified_history` to the complete-history classifier in `exec/merge.rs`, preventing the no-preflight capability from becoming a reusable bypass. It also counts selected raw `SnapshotHandle` / Dataset shapes, rejects renamed-owner/macro/include/path-lookalike forms, skips structurally test-only code, and pins retired escape hatches absent. This is intentionally not a Rust macro-expander or general alias analysis; `// forbidden-api-allow: <reason>` exempts reviewed inline-Lance lines only |
| `lance_surface_guards.rs` | Pins the Lance API surfaces omnigraph depends on (named runtime + compile-only guards; see [lance.md](lance.md)) — the first smoke check on any Lance version bump. `cached_and_zero_cache_sessions_share_store_registry_not_metadata_cache` proves a cached data Session and zero-cache control Session reuse one live `ObjectStoreRegistry` client while their metadata caches remain isolated. `_compile_uncommitted_full_table_vector_index_shape` pins the public `IndexMetadata` shape suitable for `Operation::CreateIndex`; `compact_files_succeeds_on_blob_columns` pins blob-v2 compaction; Guard 9 pins clone-only branch reclaim semantics. RFC-023's `unenforced_pk_filter_shape_is_route_dependent` explicitly forces v2 versus indexed routes and pins the `Some(populated)` / `Some(empty)` / `None` key-filter shapes; `unenforced_pk_conflict_matrix_is_directional` pins the directional filtered/unfiltered and filtered/Append matrix. RFC-024's compile guard pins the public `BranchIdentifier` + current `Transaction.uuid` + `ManifestLocation.e_tag` physical-ref candidate; the local, shared-`Session`, and RustFS tests require unchanged-reopen stability and distinguish main/named-ref delete/recreate ABA at the same numeric version. The RC.1 compiler guard pins the five surveyed public Lance virtual system-column constants to early `.pg` rejection; every Lance bump still audits upstream source for additions. All 22 guards pass on RC.1. These prove substrate tokens, not an accepted heads format or publisher. They remain substrate-upgrade tripwires; v6 production route, recovery, format, and cost evidence live at their own boundaries |
| `durable_head_lookup_cost.rs` | RFC-024 Gate A decision instrument, isolated from the production manifest schema/publisher. At fixed catalog width 10 it runs the full absent/reconciled/one-uncovered/eight-uncovered/reconciled-after-tail matrix over compacted and uncompacted histories, with cold-open and warm-repeat measurements on local FS and bucket-gated S3/RustFS. Default depths are 20/80; the ignored decision-scale cell runs 10/100/1,000. Correct exact heads, flat indexed `rows_scanned`/range work, an index-absent growing negative control, and observable bounded tails all pass; after the eight-fragment tail, `optimize_indices` returns coverage to zero uncovered and representative `rows_scanned`/range work from 27→10 / 17→10. The test deliberately pins the no-go: uncompacted RustFS cold object reads/bytes and compacted byte terms grow, while RC.1 also crosses a bounded one-operation boundary by 1,000 commits, so RFC-024 remains research-blocked. `rows_scanned` is an RC.1 debug proxy, not a universal decoded-row counter. Object-store wrapper bytes and Lance execution-summary bytes are separate fixture-owned metrics and are not additive |
| `warm_read_cost.rs` | Cost-budget tests for the warm read/control path (query-latency work), measured at the object-store boundary with Lance `IOTracker` (the LanceDB IO-counted pattern): a warm same-branch read does 0 manifest opens, 1 version probe, validates the schema once (Fix 1 / finding A / Fix 2 at commit-history depth); a cold other-branch resolution derives snapshot state and lineage from one coherent manifest open/scan; native branch create and create-from each use one post-gate open/scan, while delete uses one target capture plus one native-ref opener and only one row scan; stale same-branch reads perform exactly 2 probes and refresh manifest-only; recreated non-main branches with the same Lance version refresh by incarnation; recreated branch-owned table handles are distinguished by table e_tag or refresh-time cache clearing; recreated traversal topology is protected by per-edge-table e_tag in the graph-index cache key or refresh-time cache clearing; a warm *repeat* read does 0 table opens via the held-handle cache and a write re-opens only the changed table at its new version/e_tag (Fix 3/6A). Also the CSR topology-build cost guards: `fresh_branch_traversal_reuses_main_graph_index` (A1 — a lazy-fork branch reuses main's cached CSR index, 0 rebuilds via `graph_build_count`) and `single_edge_query_builds_only_referenced_edge` (A2 — a one-edge query builds only that edge via `graph_edges_built`); both force CSR via the scoped `with_traversal_mode` seam, so they need no `#[serial]`. See "Cost-budget tests" below. |
| `write_cost.rs` | Cost-budget tests for the WRITE path (RFC-013), the latency twin of `warm_read_cost.rs` on the **shared `helpers::cost` harness** (`measure`/`IoCounts`/`assert_flat`/`local_graph`). Runs on **local FS**; gates the **internal-table** term (`__manifest` scans flat in commit-history depth, lineage rows included — `internal_table_scans_are_flat_in_history`, now **green every-PR** since RFC-013 step 2 brought the internal tables into `optimize`; the test compacts at each depth before measuring), graph-visible maintenance arbitration (`ensure_indices_manifest_reads_are_flat_in_history` and `optimize_manifest_reads_are_flat_in_history`), plus green every-PR guards (single-insert `data_writes` bounded, a per-write read-op ceiling that fails the moment a round-trip is added, and a `measure_with_staged` fitness assert that a keyed insert routes through the exact-`id` fenced adapter once with no bare `stage_append`/vector-index build). Also gates the batched committed `@unique` probe: `unique_probe_io_is_flat_in_delta_rows` sweeps DELTA size (4 vs 64 rows) at fixed shallow history and asserts `data_open_count`/`data_scan_reads` flat — red when the cross-version probe regresses to per-row scans/opens. The **data-table opener** term is S3-only — see `write_cost_s3.rs` and the backend-split note in "Cost-budget tests" below. RFC-023's representative row-count and peak-RSS decision measurements use the scenario harness, not this every-PR I/O budget |
| `write_cost_s3.rs` | Bucket-gated (skips without `OMNIGRAPH_S3_TEST_BUCKET`) twin of `write_cost.rs` on the same `helpers::cost` harness: gates the **data-table opener** term (per-write latest-version resolution flat across commit depth on a real object store — per-version GETs are invisible on local FS). A cost gate, not a correctness test — run on demand, not in the every-merge `rustfs_integration` job (see the backend-split note in "Cost-budget tests" below) |
| `helpers/cost.rs` | The shared cost-budget harness (not a test): `IoCounts`/`StagedCounts` (counts by table class), `measure`/`measure_with_staged` (the one place the `with_query_io_probes` + `MergeWriteProbes` task-local + `IOTracker` wiring lives; reads per-op deltas via lance's `incremental_stats()`, the upstream per-request idiom from `rust/lance/src/dataset/tests/dataset_io.rs`), `cost_harness`/`GraphIoMeter` (installs ONE `__manifest` `IOTracker` for a whole test body so the graph opens **under** it and `manifest_reads` is **ground truth** — every read regardless of handle age, the warm-coordinator freshness probe included — closing the blind spot where a per-op tracker installed at measure time cannot see a long-lived handle's reads; outside `cost_harness`, `measure` falls back to fresh per-op tracking, so `write_cost_s3.rs` is unaffected), `open_tracked_lance_dataset` (attaches a caller-owned `IOTracker` before `DatasetBuilder::load`, so a cold-open fixture includes latest-manifest resolution), `last_manifest_reads()` (the manifest read log for `assert_io_eq!`-style failure diagnostics), `assert_flat(curve, select, slack, what)`, and store-agnostic `local_graph`/`s3_graph` fixtures. The general `IoCounts` vocabulary remains operation counts; RFC-024's decision fixture owns its object/plan byte metrics. `warm_read_cost.rs`, `write_cost.rs`, `write_cost_s3.rs`, and the RFC-024 instrument consume the relevant seams |
| `benchmark_scenario_contract.rs` | Source/protocol contract for the non-CI scenario harness. RFC-023 pins the production route's explicit `strict_insert_preflight_calls == 0` assertion and emitted `probe_strict_insert_preflight_calls` field, alongside route labels, clean-tree/binary identity, child-protocol refusal, and exact-content verification fields. A benchmark record therefore cannot silently claim the proven path after paying a target preflight |
| `lifecycle.rs` | Graph lifecycle and schema state, including the v6 creation invariant that every fresh node/edge table declares exactly physical `id` as its Lance unenforced PK |
| `point_in_time.rs` | Snapshots, time travel (`snapshot_at_version`, `entity_at`) |
| `changes.rs` | `diff_between` / `diff_commits` |
| `consistency.rs` | Cross-table snapshot isolation and atomic publish; RFC-023 cells prove `LoadMode::Append` is strict (existing `id` rejected without update/version movement), pin the inclusive 8,192-row load ceiling with a one-over pre-effect refusal, reject an input above 32 MiB through the shared Mutation/Load staging seam with raw table HEAD/manifest/sidecar unchanged, reject an oversized external blob on a lazy branch from object metadata before payload access/ref creation/sidecar arm, and use a barrier-synchronized stress cell over 16 pre-opened handles to prove one same-key winner, 15 typed `KeyConflict` losers, exactly one stored row carrying the winner's value, and survival of disjoint IDs |
| `lineage_projection.rs` | RFC-013 Phase 7 acceptance gate: graph lineage lives ONLY in `__manifest` — over a realistic history (main commits, a branch, a merge, actors), the production coordinator reconstructs manifest snapshot state and the full DAG projection from one coherent manifest scan (commit set, parents, merge parents + merge actor, per-branch heads, inline actors), and the `_graph_commits.lance` / `_graph_commit_actors.lance` dataset directories are never created at all |
| `schema_apply.rs` | Migration plan + apply, schema-apply lock; schema-contract publication is pinned by `read_only_open_holds_schema_gate_through_catalog_capture` and `refresh_holds_schema_gate_through_catalog_publication` (source, accepted IR/state, and compiled catalog are captured under one root schema gate). `long_lived_handle_uses_the_schema_catalog_bound_to_its_write_token` covers mutation/load plus a post-apply new node type merged through the pre-apply handle; `stale_handle_branch_delete_gates_tables_added_by_schema_apply` parks delete over that new type while a legacy index reconciler waits, proving merge planning and native-control table envelopes use an operation-local accepted catalog rather than stale ArcSwap state. Index materialization is deferred to the reconciler (iss-848): `apply_schema_defers_vector_index_on_empty_table` (an empty-table Vector `@index` never aborts the apply) and `index_only_constraint_apply_touches_no_table_data` (adding an `@index` is metadata-only — no table-version bump); enum widening (iss-enum-widening-migration): `enum_widening_apply_is_metadata_only_and_accepts_new_variant` (no table-version bump; new variant accepted, out-of-set still rejected) + `enum_narrowing_apply_is_refused` (OG-MF-106 with the graph left writable). The planner's widening/narrowing matrix lives in `schema_plan.rs`'s in-source tests. RFC-023 assertions prove exact-`id` PK metadata survives rewrites, applies to added types, remains on retained types across drop/re-add, and is present after reopen |
| `search.rs` | FTS / vector / hybrid (`bm25`, `nearest`, `rrf`) |
| `scalar_indexes.rs` | Per-property index dispatch of `build_indices_on_dataset_for_catalog`: enums + orderable scalars get a BTREE (so `=`/range/IN/IS NULL are index-accelerated), free-text Strings keep FTS — observed through the read-only `SnapshotTable::index_coverage`, backed by the same helper the traversal chooser uses |
| `traversal.rs` | `Expand`, variable-length hops, anti-join, undirected traversal (`$a <edge> $b`, `Direction::Both` — out ∪ in with set-semantics dedup, both-direction anti-join) (CSR path — `OMNIGRAPH_TRAVERSAL_MODE` unset) |
| `traversal_indexed.rs` | BTREE-indexed Expand (`execute_expand_indexed`) forced via the scoped `with_traversal_mode` seam (not the env var), asserted semantically equal to the CSR path. No `#[serial]` needed — the seam is scope-bound and process-safe. (The CSR topology-build cost guards — `fresh_branch_traversal_reuses_main_graph_index` (A1, `graph_build_count`) and `single_edge_query_builds_only_referenced_edge` (A2, `graph_edges_built`) — live in `warm_read_cost.rs`.) |
| `proptest_equivalence.rs` | Property-based query-correctness invariants over generated graphs (shared key alphabet forces cross-type id collisions, cycles, self-loops) — pins Expand-mode equivalence so a future fork divergence fails loudly instead of silently; `#[serial]` |
| `ordering.rs` | ORDER BY contract: descending, multi-key precedence, deterministic key-column tie-break (total order, so `ORDER … LIMIT` is deterministic), NULL placement (`nulls_first = !descending`) |
| `literal_filters.rs` | Execution goldens for non-string/non-integer scalar literal filters (F64/F32/Bool/Date/DateTime) across both the in-memory comparison arm and the Lance-pushdown arm |
| `aggregation.rs` | `count`, `sum`, `avg`, `min`, `max` |
| `export.rs` | NDJSON streaming export filters; RFC-023's blob fixture also performs a later `LoadMode::Append` strict insert into a populated v6 table and verifies both exact blob bytes and exact-`id` PK metadata afterward. `export_jsonl_round_trips_branch_snapshot` separately exports `main` and a named feature branch, rebuilds each into a main-only graph, and proves independent identity domains plus disjoint, self-contained histories |
| `s3_storage.rs` | S3-backed graph (skipped unless `OMNIGRAPH_S3_TEST_BUCKET` is set). Includes `s3_fresh_branch_traversal_reuses_main_graph_index_with_etags` — the CSR topology cache-key test on a **real** per-table e_tag (`None` on local FS, so `warm_read_cost.rs` can't reach this path); forces CSR via the scoped `with_traversal_mode` seam |
| `lance_version_columns.rs` | Per-row `_row_last_updated_at_version` behavior |
| `validators.rs` | Schema constraint enforcement (enum, range, unique, cardinality) across JSONL load, mutation insert/update. ALL THREE write surfaces — mutation, bulk load, AND merge — route through the unified `crate::validate` evaluator (Δ-scoped, index-backed, reusing these leaf checks). Cross-version-uniqueness closure: `cross_version_unique_rejected_on_mutation_insert` + `reinsert_existing_key_is_upsert_not_unique_violation` (mutation path); `cross_version_unique_rejected_on_append_load` + `merge_load_reupsert_existing_key_is_not_unique_violation` (load path). Per-table `Overwrite`: `overwrite_load_validates_ri_against_new_image` (an edges-only overwrite still resolves RI against retained committed nodes) + `append_load_rejects_orphan_edge`. The evaluator's own unit tests live in `src/validate.rs` (`#[cfg(test)]`); its merge-conflict equivalence is pinned by `merge_truth_table.rs` (OrphanEdge) + `branching.rs` (Unique/Cardinality merge tests). Intra-batch duplicate-`@key` rejection on every load mode is pinned by `consistency.rs::loader_rejects_intra_batch_duplicate_keys`; the mutation-coalesce counterpart (insert+update / chained updates of one id are NOT a self-collision) by `writes.rs`. Non-String `@unique` columns probe committed state with a TYPED literal (not a stringified key): `cross_version_unique_rejected_on_date_column` + `noncolliding_write_to_date_unique_column_succeeds` (a `Date @unique` collision is a proper `@unique` violation, and a distinct value does not raise a Date32-vs-Utf8 coercion error). Cardinality is keyed by edge id, last-wins (matching commit's `dedupe_merge_batches_by_id`): `merge_load_edge_src_move_rechecks_vacated_src_cardinality` (a Merge-load moving an edge recounts the vacated src for `@card` min) + `merge_load_duplicate_edge_id_counts_once_per_card` (a dup edge id under two srcs in one batch counts once, no spurious max violation). Direct deletes capture the ids they remove (from the delete op's own scan) into the change-set's `deleted_ids`, so a delete emptying a src is validated: `mutation_delete_edge_below_card_min_rejected` (a `delete Edge` dropping a src below `@card` min is rejected, not silently committed). |
| `merge_cost.rs` | Cost budgets for branch MERGE on the shared `helpers::cost` harness: `merge_validation_is_delta_scoped` keeps validation tied to the delta and caps the common one-row fast-forward route at 3 internal opens / 3 coherent manifest scans. `merge_manifest_cost_grows_with_history` caps the diverged route at 4 opens and 4 scans across the checked depths while preserving the growing object-read tripwire. Retained source/target manifest `Dataset` probe handles and combined manifest+lineage decoding reduce the pre-slice measured depth-5/depth-80 baseline from 59/651 manifest reads to 40/410, but the surviving journal fold and fresh publisher authority scan remain history-sensitive on an uncompacted graph; this is reduced amplification, not a history-flat claim |
| `policy_engine_chassis.rs` | Engine-layer Cedar enforcement (MR-722): allow + deny through every `_as` writer via the SDK directly — no HTTP — proving embedded and CLI callers hit the same gate as the server, with action × scope shapes matching `authorize_request` |
| `maintenance.rs` | `ensure_indices`, `optimize` (compaction), `repair` (explicit uncovered-drift publish), and `cleanup` (version GC): empty/idempotent/no-op edges, policy validation, head preservation. EnsureIndices refuses uncovered drift before arming its identity-bearing v9 envelope and keeps untrainable Vector work pending. Cleanup pins exact keep-count behavior, lazy-branch retention, graph-wide fail-closed ordering, and refusal of uncovered main HEAD drift before GC. Optimize's bounded payload inside the v9 envelope publishes multiple productive data tables through one graph commit, emits no lineage/sidecar at steady state, skips uncovered drift, refuses pending recovery, and compacts blob-v2 tables. Repair previews/heals verified maintenance drift and requires `--force` for semantic drift |
| `failpoints.rs` | Failure-injection coverage (gated on `failpoints` feature). RFC-022 includes deterministic post-stage/pre-effect races for mutation/load uniqueness and strict disjoint-head changes, plus the cross-handle post-effect `RecoveryRequired` → read-write-open rollback cell. Branch merge adds the captured-source advance cell; post-confirm target-winner compensation; mixed physical + pointer-only delta recovery with fixed commit id/actor/parents; both sidecar-before-first-ref and ambiguous-ref-create recovery; and an 8,193-delete between-chunk crash proving an `Armed` exact-transaction prefix is rolled back before the successful retry. Identity-bearing v9 SchemaApply is pinned by `schema_apply_phase_b_failure_recovered_on_next_open` (exact confirmed roll-forward with fixed commit id + initiating actor), `schema_apply_partial_table_effect_rolls_back_exactly` (Armed proper-prefix compensation), `schema_apply_recovery_reclaims_owned_add_type_target_and_retry_succeeds` (strict owned first-touch cleanup), `schema_apply_first_touch_foreign_winner_is_preserved_not_adopted` (foreign unregistered winner preservation), `schema_apply_post_effect_disjoint_winner_is_preserved` (winner-preserving compensation), `schema_apply_post_effect_same_table_winner_fails_closed` (buried-effect refusal), `schema_apply_recovers_partial_schema_promotion_after_commit_crash` (read-only refusal for both valid and corrupt intents in the torn manifest/schema window, followed by fixed-outcome completion of a partial source/IR/state promotion), and `schema_apply_live_query_waits_for_coherent_schema_publication` (same-handle publication wait plus pre-apply-handle query/export/whole-graph-index capture from the operation-local accepted catalog). Metadata-only before/after-staging and rollback-retry cells keep the empty-effect v9 boundary pinned. EnsureIndices v9 recovery retains both boundaries in `recovery_rolls_forward_ensure_indices_on_feature_branch`: the first residual rolls forward on the next read-write open, and a second roll-forward-eligible `EffectsConfirmed` residual under an unchanged captured token is completed by a same-handle retry before new planning. `ensure_indices_complete_armed_effects_roll_back` keeps the authority-clean complete-effect Armed rollback rule isolated, while `ensure_indices_entry_barrier_refuses_partial_armed_before_staging` leaves one of two table effects pending and proves the original `RecoveryRequired` wins before the remaining index can reach the post-stage failpoint. Its remaining cells are `ensure_indices_stage_btree_failure_leaves_existing_tables_writable` (after a clean entry barrier, expensive mixed-index staging remains outside the final authority/gates), `ensure_indices_first_touch_crash_before_ref_recovers_cleanly` (sidecar-before-ref no-effect recovery), `ensure_indices_mixed_first_touch_rollback_does_not_delete_moved_ref` (owned-effect rollback and sibling first-touch cleanup), and the no-work/no-sidecar failpoint cell; the recovery module separately pins existing + first-touch payload round-trip and identity-less-input refusal. Optimize's graph-wide identity-bearing v9 envelope is pinned by `optimize_phase_b_failure_recovered_on_next_open` (two-table roll-forward), `optimize_multi_table_partial_effect_rolls_back_under_one_v2_sidecar` (one shared sidecar, no partial visibility, compensation), `optimize_post_manifest_failure_finalizes_multi_table_v2_sidecar` (lost publish acknowledgement), and `optimize_excludes_pending_only_vector_table_from_v2_sidecar` (pending status cannot poison sibling recovery), plus its late-sidecar/main-gate/retry cells. Native controls are pinned by `native_branch_controls_reclassify_lost_acknowledgements` (matching create and absent-ref delete, with no version/lineage movement); `armed_first_touch_recovery_accepts_missing_target_ref` additionally forges and reclaims the clone-only/no-`BranchContents` table state. Legacy path overlap has both sides pinned: `armed_first_touch_recovery_defers_legacy_path_overlap_until_leaf_delete` permits open only for a proven no-effect intent, while `partial_first_touch_recovery_fails_closed_on_legacy_path_overlap` leaves one exact multi-table effect and verifies open fails closed until offline leaf cleanup lets rollback converge. Other control/recovery race cells include `first_touch_post_create_open_error_keeps_recovery_ownership`, `branch_delete_orphans_sidecar_armed_after_initial_barrier`, `branch_merge_fences_target_delete_recreate_aba`, `branch_merge_fences_concurrent_sync_on_same_handle`, `branch_merge_rejects_fresh_target_manifest_change_before_effects`, `branch_merge_rechecks_late_sidecar_after_table_gates`, `optimize_rechecks_late_schema_apply_sidecar_after_main_gate` (late zero-pin graph-global intent), `optimize_rechecks_late_disjoint_main_sidecar_after_main_gate` (table-disjoint intent sharing `graph_head:main`), `optimize_holds_main_gate_through_disjoint_table_effects` (post-relist branch-gate lifetime), `cleanup_rechecks_sidecars_under_gc_gates`, `full_recovery_rereads_sidecar_body_after_discovery`, `recovery_discovery_skips_sidecar_deleted_after_list` (an unrelated write succeeds after a listed sidecar is published/deleted), and `read_only_recovery_discovery_skips_sidecar_deleted_after_list` (read-only open succeeds against that same concurrent completion). The suite also includes the five per-writer effect → manifest-CAS recovery tests, write-entry in-process heal contract, storage-fault matrix, S3 recovery twin, and convergence-idempotent roll-forward regression. |
| `failpoint_names_guard.rs` | Source-walk guard (same defense-in-depth shape as `forbidden_apis.rs`): every failpoint call site across engine + cluster (`maybe_fail`, `ScopedFailPoint::new`/`with_callback`, `Rendezvous::park_first`) must reference a compile-checked `failpoints::names` const, never a bare string literal — a typo'd literal compiles but silently never fires |
| `recovery.rs` | Open-time recovery sweep — identity-bearing schema-v9 envelopes for Mutation/Load exact transaction identity, BranchMerge exact chains/ref-only effects + complete delta, SchemaApply exact overwrite/create ownership + schema promotion, EnsureIndices exact mixed-index transactions/authority/lineage/delta/first-touch identity, and Optimize's bounded maintenance payload; explicit refusal rather than alias inference for identity-less input; restartable compensation, fixed logical/rollback IDs, branch-token comparison, fresh under-gate reread/reparse, all-or-nothing roll-forward/rollback/refusal, recovery audit, and read-only schema-coherence guard. RFC-023 additionally asserts that restoring a feature-branch sidecar leaves the selected feature ref with exact-`id` PK metadata |
| `composite_flow.rs` | Compositional/narrative end-to-end stories — multi-step flows that compose mechanics covered by other test files. Catches integration regressions where individual operations all pass their unit tests but their composition breaks (sequential merges, post-merge main writes, time-travel through merge DAG, reopen consistency over multi-merge histories, post-optimize and post-cleanup strict writes). |

## Fixtures

`crates/omnigraph/tests/fixtures/` holds the canonical schema (`.pg`), seed data (`.jsonl`), and queries (`.gq`) shared across tests. Reuse these before inventing new ones — the helpers harness already knows how to load them.

## Test helpers

- **Engine** — `crates/omnigraph/tests/helpers/mod.rs`: `init_and_load()` (bootstrap a temp graph + load standard fixture), `snapshot_main()`, `snapshot_branch()`, query/mutation runners, row collection and counting. Use these instead of hand-rolling.
- **CLI** — `crates/omnigraph-cli/tests/support/mod.rs`: `Command`-style wrapper for invoking `omnigraph`, server-process spawning, fixture resolution, output assertion helpers.
- **Server** — no shared helpers; server tests call the `Omnigraph` engine API directly and exercise endpoints over the wire.

> Note: the storage adapter has an in-memory backend (`ObjectStorageAdapter::in_memory()`, full contract including true conditional updates) used by the adapter contract tests in `storage.rs`. Those tests also pin the optional single-GET text-read contract: present objects return `Some`, typed `NotFound` returns `None`, and non-absence failures remain loud. It covers only the text-object layer (sidecars, schema staging, cluster state) — **Lance datasets bypass the adapter**, so engine integration tests still use `tempfile::tempdir()`. An in-memory Lance substrate remains an architectural ask — keep it explicit in [docs/dev/invariants.md](invariants.md) under known gaps.

## Failpoints (fault injection)

RFC-023's Mutation/Load effect classifier is pinned here, not by ordinary unit tests:
`rfc023_effect_free_conflict_is_typed_or_fully_reprepared` proves that a strict
same-key conflict is terminal `KeyConflict` while an upsert stages a fresh,
revalidated attempt; `rfc023_table_n_conflict_after_table_1_keeps_recovery_ownership`
proves that an earlier table effect makes a later conflict
`RecoveryRequired` and retains the exact sidecar.
`rfc023_disjoint_retryable_strict_conflict_reprepares_without_key_conflict`
proves that the broad retryable/no-exact-match branch performs two complete
strict preparations, commits both disjoint rows, and leaves no false
`KeyConflict` or sidecar. BranchMerge's 8,193-row two-chunk recovery is pinned
in both directions: `branch_merge_multichunk_insert_armed_prefix_rolls_back` proves an
`Armed` first-chunk prefix compensates before a successful retry, and
`branch_merge_multichunk_effects_confirmed_rolls_forward` proves two confirmed
but graph-invisible chunks publish the complete fixed outcome on reopen.
`branch_merge_pure_insert_rejects_source_table_ref_aba_before_arm` parks after
proof, replaces the raw source-table ref, and proves the final native-identifier
check returns typed `ReadSetChanged` before target movement or recovery arm.
`branch_merge_pure_insert_rejects_target_table_ref_aba_before_arm` separately
replaces an already-owned target's raw `BranchContents` identifier while
preserving its path, numeric version, and rows; only the final target native-
incarnation check can catch that same-version ABA, and it does so before
sidecar arm or graph movement.

- Cargo feature: `failpoints = ["dep:fail", "fail/failpoints"]` in `crates/omnigraph/Cargo.toml`; the cluster's `failpoints` feature additionally enables `omnigraph/failpoints` (`crates/omnigraph-cluster/Cargo.toml`), so the shared test guard is available to cluster tests.
- Wrappers: `crates/omnigraph/src/failpoints.rs` and `crates/omnigraph-cluster/src/failpoints.rs` each expose `maybe_fail("name")` (per-crate error type). The test-side config guard `ScopedFailPoint` (`new` for action strings, `with_callback` for callbacks; RAII `Drop` removes the point) lives **once** in the engine and is reused by both test binaries.
- **Names are compile-checked.** Every failpoint name is a `pub const` in `omnigraph::failpoints::names` (engine) / `omnigraph_cluster::failpoints::names` (cluster). Call sites and tests reference the constant, never a bare literal — a typo is a compile error, not a silently-never-firing point. Add a new failpoint by adding its const first.
- Call sites are inserted at sensitive transaction boundaries (branch create, graph publish commit, the recovery sweep's classify→roll-forward-publish window, cluster apply's payload→state-write window, etc.).
- **Serialize and rendezvous, never sleep.** The `fail` registry is process-global, so every failpoint test carries `#[serial]` (`serial_test`). For concurrent tests, use `helpers::failpoint::Rendezvous` (`tests/helpers/failpoint.rs`): `park_first(name)` parks the first thread to hit the point until `release()`, and `wait_until_reached().await` blocks on that condition (it doubles as a fired-assertion). Do not coordinate threads with fixed `sleep`s.
- Activated tests: `crates/omnigraph/tests/failpoints.rs` and `crates/omnigraph-cluster/tests/failpoints.rs` (integration binaries, never in-source — the fail registry is process-global). Run with `cargo test -p omnigraph-engine --features failpoints --test failpoints` / `cargo test -p omnigraph-cluster --features failpoints --test failpoints`.

## RustFS / S3 integration

CI runs these S3-backed **correctness** tests against a containerized RustFS server (`.github/workflows/ci.yml` → `rustfs_integration` job, sharded one suite per runner):

- `cargo test -p omnigraph-engine --test s3_storage` (lifecycle/branching + the e_tag-present CSR topology cache-key reuse test — the path local FS can't reach since its e_tag is `None`)
- `cargo test -p omnigraph-engine --test lance_surface_guards public_physical_ref_token_rejects_s3_same_version_aba -- --exact` (RFC-024's public physical-ref token across unchanged reopen plus main/named same-version ABA; the workflow additionally rejects a zero-test/vacuous match)
- `cargo test -p omnigraph-server --test s3` (single-graph serving + config-free `--cluster s3://` boot)
- `cargo test -p omnigraph-cluster --test s3_cluster` (full control-plane lifecycle on the bucket)
- `cargo test -p omnigraph-cli --test system_local local_cli_s3_end_to_end_init_load_read_flow`
- `cargo test -p omnigraph-engine --features failpoints --test failpoints s3_` (recovery-sidecar lifecycle on a real bucket)

Locally, set `OMNIGRAPH_S3_TEST_BUCKET` (and the usual `AWS_*` vars including `AWS_ENDPOINT_URL_S3` for non-AWS) before running. Without those, S3 tests skip gracefully.

RFC-024's S3 **cost** matrix is deliberately not in this correctness job. Run
it on demand with
`OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --test durable_head_lookup_cost s3_durable_head_lookup_matrix_is_correct_and_observable -- --exact --nocapture`.

## Cross-version upgrade (genuine old binaries → v6)

`crates/omnigraph-cli/tests/crossversion_upgrade.rs` contains genuine-binary
coverage—not the stamp-rewind stand-in in
`db/manifest/tests.rs::sub_current_graph_is_refused_then_rebuilt_via_export_import`.
The long-baseline case mints internal schema v3 with OmniGraph 0.7.2; the v4
case uses 0.8.1. Both prove current-v6 refusal, export/init/load into a different
v6 root, row/vector fidelity, and exact-`id` PK metadata on every rebuilt graph
table; the v4 case also pins reverse refusal by the old binary.

RFC-023 adds an immediate-predecessor case gated on `OMNIGRAPH_V5_BIN`, built
from the final internal-v5 commit. It mints a genuine SchemaIR-v2 v5 graph,
proves v6 refuses it with the 0.9.x rebuild guidance, exports with v5, rebuilds
under v6, checks row/vector/blob fidelity, exact blob bytes, and exact-`id` PK
metadata, then proves the v5 binary refuses the v6 root. The same cell injects
a duplicate logical ID into the v5 export: v6 rejects the load atomically,
leaves every initialized target table empty, and a canonical re-export proves
the v5 source unchanged. The initialized empty target remains a valid graph;
the operator must not serve it and should discard it after the failed rebuild.
The genuine run passed on 2026-07-15 with a binary built from the final
internal-v5 commit:

```bash
OMNIGRAPH_V5_BIN=/path/to/final-v5/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_v6_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v6 -- --nocapture
```

Result: 1 passed. The no-environment full file also passed all three explicit
skip paths.

Cross-version suites are deliberately outside default CI because old-source
builds are expensive and this seam changes only at format/release boundaries.
They are gated on absolute old-binary paths and skip gracefully when unset.

## System e2e requirements and suppression

The CLI system tests (`system_local.rs`) spawn the workspace-built `omnigraph` and `omnigraph-server` binaries (cargo provides paths via `CARGO_BIN_EXE_*`), bind ephemeral localhost ports, and use local-FS temp dirs — no external services, no env vars required; they run in the default `cargo test --workspace`. The comprehensive cluster lifecycle e2es (multi-server-restart flows) honor an opt-out for constrained sandboxes: set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` to skip them with a logged message (the same graceful-skip pattern as the S3 gate). Cargo-native filtering also works: `cargo test --test system_local -- --skip local_cluster`.

## OpenAPI drift

`crates/omnigraph-server/tests/openapi.rs` regenerates `openapi.json` and diffs against the checked-in copy. The drift check runs strict on PRs (the auto-commit step lives in the heavy `test` job, which is post-merge-only) — for server/API changes, regenerate locally with `OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi` and commit the result, or the PR's `test_aws_feature` job fails on drift. See [ci.md](ci.md).

## Examples & benches

- `crates/omnigraph/examples/bench_expand.rs` — runnable example (not part of CI).
- `crates/omnigraph/benches/scenarios.rs` — the **scenario benchmark harness**: a
  decision instrument, never a CI gate. Each scenario is ONE cold, stateful
  macro-run (a branch merge, a filtered vector search) executed in a fresh
  subprocess and instrumented for wall-clock + peak RSS (`libc::wait4` /
  `ru_maxrss` — kernel-exact, no sampling) + scenario metrics, emitted as JSON
  lines. Scenario-local structural assertions keep a run on its claimed route;
  timing/RSS thresholds are evaluated from the records, not asserted in the
  executable. It is not part of `cargo test --workspace`. Criterion is
  deliberately not used (statistics over warm in-process iterations is the wrong
  model for multi-second stateful scenarios; no memory measurement; no crash
  isolation — an OOM under `--memory-cap-mb` is a *data point*). Run:
  `cargo bench -p omnigraph-engine --bench scenarios -- --scenario
  merge-all-changed --rows 20000 --dims 256` (also `nearest-prefilter`;
  existing scenarios use `--baseline` to omit or replace the measured op,
  while RFC-023 records the exact comparator boundary in `metrics.routing` and
  `metrics.measurement_boundary`; `--memory-cap-mb` applies and verifies
  `RLIMIT_AS` on Linux. A requested cap on an unsupported platform, or one that
  cannot be verified, is recorded before allocation and the child refuses the
  scenario with exit status 78). Every run appends its record (with `ts` +
  `git_sha`, full `git_tree_sha`, `git_worktree_dirty`, and an exact SHA-256
  digest of the benchmark binary) to a results log — `--out <path>`, else `OMNIGRAPH_BENCH_RESULTS`,
  else `crates/omnigraph/benches/results.jsonl` (gitignored; host-specific) —
  so baselines survive across sessions and substrate bumps. Add new scenarios
  here rather than new bench targets; keep the JSON-lines/no-assertions
  contract.
- `crates/omnigraph/benches/scenarios.rs` with
  `benches/scenarios/rfc023.rs` — RFC-023's decision instrument. It measures a
  fixed 32-row mixed upsert against 10K/100K/1M-row
  indexed targets (forced v2 filter route versus default index-enabled route),
  one exact filtered 8,192-row transaction mirroring the Mutation/Load
  single-transaction ceiling, and an embedding-bearing all-new branch adopt.
  Every adopt trial is explicitly three-phase over one persisted fresh root.
  An uncapped setup child initializes the same real graph, loads main, creates
  the source branch, loads its all-new rows, validates main=N/source=2N, and
  records both observed table versions in its fingerprint. A fresh measured
  child alone receives `--memory-cap-mb`, identically `Omnigraph::open`s the
  root for either arm, records pre-operation HWM, and executes production
  `Omnigraph::branch_merge` or the labeled non-production comparator.
  Production includes the full coordinator lifecycle. For this proven all-new
  fixture that means complete v1 history-chain admission, bounded source-
  interval scans, final source/target native-incarnation checks, sidecar and
  recovery-chain work, table commits, and manifest publication. The admitted
  opaque chunks stage immutable fragments with `InsertBuilder`, replace its
  temporary uncommitted Append operation with the exact-`id` filter-bearing
  `Update`, and re-mint v1. `MergeWriteProbes` assert the observed transaction
  count exactly equals the row/byte plan, all rows were fenced, and target
  MergeInsert calls, strict-insert target preflights, committed/bare Append,
  whole-delta combines, and ordered-cursor scans all stayed at zero. Raw Lance
  interval-emission count/maximum bytes are recorded separately from the hard
  normalized chunk boundaries. The comparator
  streams only `adopt-new-*` rows through `InsertBuilder::execute_stream` in
  Lance Append mode and never collects the whole delta. Because it cannot
  access OmniGraph's private Session, the lower-level comparator opens one raw
  Lance Session and explicitly shares it between physical main/source handles.
  Both arms capture operation wall time and immediate post-operation HWM, then
  perform no final row scan. A third uncapped fresh child uses bounded
  `id`/`slug`/`embedding` projections plus an exact-domain bitset and
  deterministic vector checks to prove physical and graph-visible content, not
  merely row counts.
  The parent exposes setup/controller/operation/verify peaks separately, while
  top-level `peak_rss_bytes` is exactly the measured-operation child's
  whole-process `wait4` peak. Unsupported requested caps still fail closed
  before the operation child opens the fixture. A failed/refused child still
  produces its one aggregate JSON record, and the parent exits nonzero after
  finishing the requested runs; malformed, missing, duplicate, or non-object
  child protocol records are harness failures. Final evidence is exactly five
  matched pairs / ten trials per size over separate fresh roots, with A =
  production, B = comparator, the same seed within a pair, and order AB, BA,
  AB, BA, AB. Every exit and phase must be successful, exact route/content
  checks green, the worktree clean, and `git_tree_sha` plus benchmark-binary
  SHA-256 identical across all ten records. The exact gates are
  `median(A metrics.operation_wall_ms) / median(B metrics.operation_wall_ms) <= 5.0` and
  `max_i(A_i operation-child peak - B_i operation-child peak) <= 67,108,864`
  bytes, using signed pair differences. All raw records/pairs are reported;
  there is no exclusion or replacement. When immediate post-operation HWM is
  not above pre-operation HWM, the recorded increment is transparently
  censored rather than replaced with zero; the RSS gate still uses whole-child
  `wait4` peaks.

  The predeclared replacement series completed on clean Git tree
  `22b31354b237b981683fa1bc5b01275a6c8b8750` with benchmark digest
  `17b4eb12083afd3eb8c26b23ef01dbd90b6ac9b2ab4160352b6617887f403edb`.
  The 10K file
  `/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-no-preflight-acceptance-10k.jsonl`
  used seeds `2404001..2404005`: production operation times
  `[31, 30, 30, 31, 31]` ms versus comparator `[8, 8, 8, 9, 8]` ms give
  medians 31/8 and **3.875×**; maximum signed paired RSS overhead was
  **24,297,472 bytes**. The 100K file
  `/Users/andrew/.local/state/omnigraph/benchmarks/rfc023-no-preflight-acceptance-100k.jsonl`
  used seeds `2414001..2414005`: production
  `[136, 136, 137, 134, 134]` ms versus comparator
  `[40, 36, 34, 35, 35]` ms give medians 136/35 and **~3.886×**; maximum
  signed paired RSS overhead was **32,604,160 bytes**. Both sizes pass both
  fixed gates. All twenty records completed every phase and exact-content
  check; every production record reports zero target strict-insert preflights,
  zero MergeInsert calls, and zero ordered-diff scans.

  Historical direct-substrate bulk rows remain narrower substrate evidence,
  not production acceptance. The earlier full-lifecycle 10K series failed at
  30.0× and 108,625,920 bytes and is preserved; that failure motivated the
  complete-certificate/InsertBuilder path. The historical 1M small-upsert and
  8,192 × 256 one-ceiling substrate cells remain valid for their own gates.
  Those macOS measurements predate fail-closed cap handling: the RFC records
  observed `ru_maxrss` and does not claim the requested 256 MiB cap was
  enforced. The current harness refuses a requested capped scenario on macOS.
- Add `benches/` per crate when you ship a perf-driven change, and include the motivating workload with the optimization.

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
- **Use the shared harness, and gate each term on the backend where it manifests.** `helpers::cost` (`measure`/`IoCounts`/`assert_flat`/`local_graph`/`s3_graph`) is the one place the `IOTracker`/task-local plumbing lives — consume it, don't duplicate it. The write path has *two distinct* depth terms that split cleanly across backends, and conflating them is a real trap (the local data-table *scan* term used to grow with depth for a different reason — the merge-insert/RI scan re-reading O(depth) *fragments* — until the dataset-opener unification attached the shared per-graph `Session` to write-side opens; immutable fragment/manifest metadata now comes from the session cache, and `write_cost.rs::data_table_reads_split_into_flat_opener_and_scan_flat_with_session` pins that flatness — a red there means a write-side open dropped the session): (1) the **internal-table** scan term (`__manifest` fragment scans, lineage rows included) reproduces on **any** backend including local FS, so `write_cost.rs` gates it on local every-PR; (2) the **data-table opener** term (latest-version resolution) is a per-object-store-RPC phenomenon — local-FS resolves latest with one cheap `read_dir` regardless of the opener used, so the namespace-vs-direct difference is **invisible on local** and only shows on a real object store (per-version GETs), gated by the bucket-gated `write_cost_s3.rs`. Same harness, different fixture; each term asserted where it actually appears. **`write_cost_s3` is a cost (IO-count) gate, not a correctness test, so it was pulled out of the every-merge `rustfs_integration` CI job — run it on demand (`OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --test write_cost_s3`) pending a dedicated cost/perf harness. The local `write_cost.rs` opener/scan-split guard still runs every-PR, so the split itself stays covered; only the S3 acceptance of the opener term is off the correctness path.**
- **Separate access-shape wins from history-slope claims.** A shared
  `ObjectStoreRegistry`, a graph-handle-scoped cached data session, a zero-cache
  control session, or one manifest+lineage scan per coordinator open can remove
  duplicate client construction and scans without making the surviving
  append-only journal fold O(1). Merge instrumentation therefore reports both
  open/scan counts and underlying reads; until a checked-in gate passes at
  realistic history depth, describe the result as reduced amplification, not
  history-flat authority lookup.
- **Keep decision instruments honest when the answer is no.** RFC-024's `durable_head_lookup_cost.rs` attaches tracking before the cold dataset load through `open_tracked_lance_dataset`, then reports object-store wrapper I/O separately from Lance execution-summary I/O. Its reconciled BTREE row/range curve is flat, but its required RustFS cold-open and compacted-byte curves grow; those red design facts are asserted as the current result rather than erased because some counters pass. Run the default local 20/80 matrix with `cargo test -p omnigraph-engine --test durable_head_lookup_cost local_durable_head_lookup_matrix_is_correct_and_observable -- --exact --nocapture`; run the ignored 10/100/1,000 local matrix with `cargo test -p omnigraph-engine --test durable_head_lookup_cost local_durable_head_lookup_matrix_at_one_thousand_commits -- --ignored --exact --nocapture`. The bucket-gated S3 command is in the RustFS section above and remains on demand.
- **Count on the handle that does the reads, not just the one a measured op opens.** Lance's IO-counted tests attach the `IOTracker` to the (warm, cached) dataset and read `incremental_stats()` per request — the tracker MUST be on the handle performing the reads, or warm-handle reads escape. A per-op tracker installed at measure time cannot see reads on a long-lived handle opened earlier (the warm coordinator's `__manifest` handle, reused across writes), so such reads were silently undercounted. Wrap a depth-swept body in `cost_harness` so the manifest tracker is installed before the graph opens and `manifest_reads` is **ground truth** (handle-age-irrelevant). The `version_probes` counter is the freshness-probe *call* count; ground truth additionally reveals that a write's probe does ~3 object-store RPCs (a read's probe is a 0-IO cache hit). `manifest_reads_capture_warm_probe` is the guard that this stays true.
- This is the testing companion to invariant 15 in [docs/dev/invariants.md](invariants.md) (hot-path cost is bounded by work, not history).

When in doubt, re-read [docs/dev/invariants.md](invariants.md) — quality gates apply to every change.
