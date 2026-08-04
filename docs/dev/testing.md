# Testing

This file is the always-on map of the test surface. **Consult it before every task** so you know what tests already cover the area you're about to change, what helpers to reuse, and where a new test belongs. The architectural invariant for boundary-matched tests lives in [docs/dev/invariants.md](invariants.md).

## Where tests live, per crate

| Crate | Path | Style |
|---|---|---|
| `omnigraph` (engine) | `crates/omnigraph/tests/` | Integration tests (one file per behavior area — see the table below), fixture-driven, share `tests/helpers/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | Per-area suites (post-modularization): `cli_cluster.rs` (cluster command surface + operator-actor cascade, including strict stream-block/dead-letter grammar, scope, plan parsing, and effect-free offline preflight), `cli_cluster_e2e.rs` (spawned-binary lifecycle compositions — lost-state re-import recovery, out-of-band drift, graph-root destruction, multi-graph mixed-disposition convergence), `cli_data.rs` (load/read/change/branch/commit/export/snapshot/policy/embed/maintenance + operator format cascade), `cli_schema_config.rs` (init/config, schema plan/apply), `cli_queries.rs`, `parity_matrix.rs` (RFC-009 Phase 1: the embedded-vs-remote referee — every forked verb run against both arms with matched Cedar policy and the same actor, scrubbed-JSON + exit-code equality; divergences are pinned in its `KNOWN_DIVERGENCES` ledger, never silently repaired), `system_local.rs` (full-cycle cluster lifecycle with a spawned `--cluster` server, applied-policy enforcement over HTTP, keyed-credential auth, operator aliases, and the real-binary graph-first firehose golden journey), `system_remote.rs`, `crossversion_upgrade.rs` (genuine historical source→CURRENT rebuild/refusal cells plus the required adjacent harness — see below); share `tests/support/mod.rs` (hermetic `OMNIGRAPH_HOME` by default) |
| `omnigraph-control-authority` | in-source `#[cfg(test)] mod tests` | Concrete-storage, lock-derived checked authority: offline confirmation/actor/operation binding, state-CAS and graph/declaration/profile-revision validation, normalized graph-root binding, non-cloneable runtime guards, and one process-local writer registration per cluster graph. F6b1 pins a distinct served-export guard for exact terminal `DISABLED | RETIRED` state and proves it shares that registration without becoming writer authority |
| `omnigraph-cluster` | mostly in-source `#[cfg(test)] mod tests`; `tests/failpoints.rs` (feature-gated); `tests/s3_cluster.rs` (bucket-gated full lifecycle on object storage) | Cluster config parser, local JSON state diff, state CAS/lock handling/recovery, read-only validate/plan/status plus explicit refresh/import graph observations, config-only apply (content-addressed payload publish, disposition gating, composite-digest convergence, idempotent re-apply), catalog payload verification (status read-only, refresh drift + self-heal), failpoint crash-mid-apply / CAS-race coverage, graph create/schema/delete lifecycle, policy binding and serving snapshots, v11 streaming-profile ownership, authority-retirement preflight, and stopped/offline stream-control preflight. The current dead-letter owner pins actor/offline/applied-streaming/declaration/state-lock binding before selected-token list or payload export; it is inspection-only and exposes no served route. F6b1 adds only the exact-terminal served-export binding consumed at boot |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | Per-area suites (post-modularization): `auth_policy.rs`, `data_routes.rs`, `schema_routes.rs`, `stored_queries.rs`, `multi_graph.rs` (cluster-mode boot — converged serving, policy binding wiring, boot refusals — + the concurrent branch-ops matrix), `boot_settings.rs` (mode inference, PolicySource), `s3.rs` (bucket-gated: single-graph serving + config-free `--cluster s3://` boot), `openapi.rs` (OpenAPI drift / regeneration); share `tests/support/mod.rs`. F5a changes no route: `serve` starts checked-runtime resident fold supervisors only after listener bind and joins every selected graph concurrently after Axum graceful shutdown; engine/failpoint owners pin the scheduler behavior. F6b1 boot consumes the terminal served-export guard and installs the hidden engine authority; it adds no handler, route, or OpenAPI surface |
| `omnigraph-compiler` | mostly in-source `#[cfg(test)] mod tests` | Parser, type-checker, IR lowering, lint. Schema parser and SchemaIR validation tests both reject the five exact Lance virtual system-column property names while preserving near-miss identifiers |

**F6b5 amendment:** the F6b1 phrases in the table are historical slice
boundaries. F6b5 later activates the existing served HTTP/remote-CLI/OpenAPI
export route using that exact-terminal binding and doc-hidden move-only engine
cut; it does not activate row ingress or lifecycle/maintenance/status surfaces.
The dedicated F6b5 ownership section below lists the current tests.

**F7a amendment:** graph-native row ingress is now the narrow public exception.
`memwal_stream.rs` owns the checked-runtime bridge, bodyless token challenge,
redacted ordered results, and existing driver composition. Server
`auth_policy.rs`, `data_routes.rs`, and `multi_graph.rs` own pre-body auth/media/
runtime refusal plus the enabled-runtime 428/412/200 route composition;
`openapi.rs` pins the generated contract. CLI in-source `client::tests` owns
preflight-before-file-open, single-use streaming request/response bytes, and
no replay after 412, while `planes::tests` pins served graph addressing and
direct-mode refusal before dispatch. No test exposes a table/lane selector or
adds a lifecycle, status, or maintenance route.

**F7b amendment:** checked operational status is now the second narrow served
stream surface. `memwal_stream.rs` extends the existing F6b6 owner with the
graph-logical projection and no-effect proof; raw table, binding, shard,
generation, token-sample, and recovery identities stay engine-private.
Server route/auth tests own `GET /graphs/{graph_id}/stream/status`, read-policy
authorization, redaction, checked-runtime ownership, and retryable refusal;
`openapi.rs` pins the generated contract. CLI in-source client and plane tests
own selected-graph GET/decode, served-only addressing, and JSON/human output.
No F7b test exposes or mutates lifecycle, maintenance, or rebind authority.

The engine's `tests/` is the principal coverage surface; most graph-shaped behavior is exercised there.

## CI control-plane tests

The workflow has one conservative text-only classifier: only known top-level
documentation files and documentation formats below `docs/` may skip the
post-merge heavy jobs. A Markdown/text fixture under `crates/` is source, not
documentation. There is no dedicated firehose artifact or dependency-key
harness. Code pull requests run a reporting-only
`cargo check --workspace --locked` with default features; the heavy test graphs
remain post-merge, tag-, or dispatch-time owners.

When changing `.github/workflows/ci.yml`, validate its syntax and the remaining
shell owners:

```bash
shellcheck scripts/*.sh
actionlint .github/workflows/ci.yml
```

Also inspect the `Classify Changes` case table whenever a new fixture format or
source subtree is added. Hosted-runner latency is measured workflow evidence;
it is not inferred from a local syntax check. See [ci.md](ci.md).

## Engine integration tests (`crates/omnigraph/tests/`)

| File | Covers |
|---|---|
| `end_to_end.rs` | Full init → load → query/mutate flow; blob coverage includes `blob_read_after_mutation_insert`, where a handle opened before another handle's commit must freshness-probe and read the newly committed blob, plus `blob_load_external_file_uri`, which proves Overwrite retains an external URI reference |
| `branching.rs` | Branch create/list/delete and lazy fork; native-control hardening includes main and named-source clone-only create recovery, invalid-name-before-clone, live path-prefix namespace rejection, legacy prefix-collision leaf-first delete, and delete/recreate first-write safety. The control path captures one operation-local accepted catalog plus fresh manifest/namespace view after the complete gate envelope rather than refreshing the handle-local coordinator around table-gate acquisition; `forbidden_apis.rs::native_branch_controls_use_post_gate_captures_not_handle_refreshes` structurally pins that shape and post-success cache invalidation. RFC-023 pins exact-`id` PK metadata both on an inherited feature snapshot and after the first write materializes its lazy `Person` fork. `branch_merge_with_external_blob_uri_materializes_payload` proves a `LoadMode::Append` strict insert materializes an external URI cell and the later fenced branch merge preserves readable bytes. `branch_merge_rejects_oversized_blob_payloads_pre_effect` proves that one external blob above 32 MiB, or several blob columns whose materialized row total exceeds 32 MiB, returns typed `ResourceLimitExceeded` before raw HEAD, manifest, table pin, row image, lineage, or sidecar movement. The lower classifier truth cells (absent-ref/tree-present delete, same-identifier native refusal, recreated-identifier typed conflict with JSON details) live in `src/branch_control.rs` unit tests |
| `merge_truth_table.rs` | Merge-pair truth table (MR-786): all 9×9 `(left_op, right_op)` cells from `{noop, addNode, removeNode, addEdge, removeEdge, setProperty, dropProperty, addLabel, removeLabel}`. Adding a new op to `OpVariant` forces a compile error in `build_case` until the new row + column are dispositioned. 36 executable cells run through real `branch_merge` with a structured oracle (`MergeOutcome` / `MergeConflictKind` + graph-state assert); 45 cells involving `dropProperty`/`addLabel`/`removeLabel` are recorded as `Unsupported` until the mutation grammar grows. |
| `merge_fast_forward.rs` | Branch-adopt cost + correctness under RFC-023. The one-batch and 8,193-row fixtures prove that a complete v1 insertion-absence history chain publishes one/two bounded exact-`id` filtered `Update` transactions with zero target strict-insert preflights, target MergeInsert joins, committed Appends, ordered-cursor scans, or whole-delta staged combines. `pure_insert_fast_forward_retains_value_constraint_validation` proves the certificate skips only redundant key work, not logical row constraints; its all-new Upsert source is certified from completed effect statistics. `proven_fast_forward_certificate_composes_across_merge_generation` proves the publisher re-mints v1 and a second merge consumes that output as the next proof-chain link. A missing intermediate transaction proves cleaned history is an optimization miss: the merge enters the general ordered diff, preserves exact rows, and leaves no recovery residue. `lazy_target_ref_only_fast_forward_uses_pin_after_main_advances` distinguishes a valid old lazy graph pin from drift when the inherited main ref advances. A nested `main → feature → experiment` cell prevents a deeper valid `BranchIdentifier` from becoming a false read-set conflict. Every general-route base/source/target `OrderedTableCursor` scan applies both Lance `batch_size(8,192)` and `batch_size_bytes(32 MiB)`. Validation streams projected `id`/`src`/`dst`/scalar batches, charges exact Arrow memory before retention, and shares one 32 MiB operation-wide budget across candidate tables; `branch_merge_validation_delta_is_aggregate_bounded_pre_arm` crosses it with two individually valid ~18 MiB deltas while proving zero HEAD/manifest/lineage/sidecar movement. Deletes use exact escaped-filter chunks with the same row/byte and retained-plan bounds. Production-helper unit cells pin chain/delete/recovery limits. The subprocess scenario owns the final production latency/RSS evidence; these integration tests own route semantics, not timings |
| `writes.rs` | Direct-publish writes: cancellation, RFC-022 non-strict full-attempt reprepare from fresh branch authority, strict stale-write conflicts, multi-statement atomicity, MR-794 staged-write rewire (D₂ rejection, insert+update coalesce, multi-append coalesce, partial-failure recovery, load RI/cardinality recovery); RFC-023 pins the inclusive 8,192-row keyed input ceiling, the same exact/+1 boundary on streamed mutation-update matches, no-effect state for both refusals, and oversized stored-Blob rejection before payload read. Crate-internal pending-scan cells pin inclusive/+1 32 MiB accounting plus pending-key shadow-before-charge. The lance#7444 row-id-overlap regression (`filtered_read_after_merge_update_and_delete_keeps_row_ids_consistent` — merge-load → same-key merge-load → delete → keyed point lookup, green only under the vendored lance-table patch — plus its append-only control) |
| `src/table_store/staged_tests.rs` | Crate-internal staged primitives. RFC-023 pins one exact target preflight for general StrictInsert, durable v1 mint/commit/reopen/history persistence, exact-`id` filter emission, typed `KeyConflict`, and missing/wrong PK refusal. `all_new_upsert_certifies_insert_absence_and_persists_it_in_history` proves an all-new completed Upsert receives the optional certificate, a mixed/update Upsert does not, unrelated transaction properties survive, and UUID rebinding does not erase it. Proven-insert cells show the opaque path performs zero strict preflights; stages with `InsertBuilder` but commits the full pure-insert `Update` shape (exact parent and `id` filter, `RewriteRows`, no updates/removals, full nested schema preorder, physical rows); persists/re-admits its own output for proof composition; leaves new fragments outside old index coverage; and fails same-key races loudly in proven/proven and proven/general orders. The in-source `exec/merge.rs` certificate unit table rejects missing/unknown properties, wrong parent/filter/full-preorder/mode/offsets, rewrite/removal shapes, missing `physical_rows`, and Append. Source-interval cells pin exact selection, lazy retained-parent splitting, coalescing, and pinned Lance's approximate raw-emission boundary while every normalized/writer chunk remains hard-capped. Generic `stage_append`/`stage_merge_insert` remain primitive tests only. The file also owns index staging and `commit_staged{,_exact}` |
| `forbidden_apis.rs` | Defense-in-depth syntax-tree/source guard over the whole engine. The primary boundary is Rust visibility: raw storage/coordinator/handle-cache modules are crate-private; public `Snapshot::open` returns `SnapshotTable`; and `SnapshotScanner` executes reads without exposing Lance's raw scanner or physical plan. The guard pins those visibility/return-type boundaries, classifies public async inherent `Omnigraph` methods plus loader conveniences, classifies every crate-visible async method on `GraphCoordinator` / `ManifestCoordinator`, and exact-counts registered method/UFCS durable-call shapes including recovery. RFC-023 rejects production graph call sites of generic `stage_append{,_stream}` and `proven_insert_capability_has_one_production_mint_site` pins `ProvenInsertChunk::from_verified_history` to the complete-history classifier in `exec/merge.rs`, preventing the no-preflight capability from becoming a reusable bypass. At the RFC-026 Phase-A checkpoint the guard registered only the exact v10 enrollment gateway and feature-gated test seam, counted its sidecar/index/shard durability primitives, and kept every row-put/ack/fold surface absent; the Phase-B1 owner below exact-counts only the approved crate-private put/fold durable-call sites. V11 adds only the bounded checked offline profile/runtime authority factories and recovery-v13 profile gateway; the guard must keep ambient constructors and public row, enrollment, drain, claim, lifecycle, SDK, HTTP, CLI, and OpenAPI side doors absent. F5a classifies the doc-hidden start/shutdown bridge as composed recovery-v14 fold orchestration while adding no durable primitive allowance for the supervisor module itself. F6b1 structurally pins the doc-hidden export cut as private-field, move-only, and non-forgeable while adding no public export seam. F7a registers exactly one doc-hidden public graph-ingest bridge as composed recovery-v14/v21 ownership while retaining the ban on raw table/lane writers and public management side doors. The embedded SDK's read-only `stream_status` is classified separately and must remain free of durable calls. B2a inventories the only production `_mem_wal` literal owners, forbids MemWAL reclamation/adoption symbols and destructive primitives in the adapter, keeps generic maintenance unaware of MemWAL, and keeps raw inventory/classifier helpers private. This remains defense in depth rather than macro expansion, alias, or data-flow analysis; the visibility boundary and behavior tests are still primary. It also counts selected raw `SnapshotHandle` / Dataset shapes, rejects renamed-owner/macro/include/path-lookalike forms, skips structurally test-only code, and pins retired escape hatches absent. `// forbidden-api-allow: <reason>` exempts reviewed inline-Lance lines only |
| `lance_surface_guards.rs` | Pins the Lance API surfaces omnigraph depends on (named runtime + compile-only guards; see [lance.md](lance.md)) — the first smoke check on any Lance version bump. `cached_and_zero_cache_sessions_share_store_registry_not_metadata_cache` proves a cached data Session and zero-cache control Session reuse one live `ObjectStoreRegistry` client while their metadata caches remain isolated. `_compile_uncommitted_full_table_vector_index_shape` pins the public `IndexMetadata` shape suitable for `Operation::CreateIndex`; `compact_files_succeeds_on_blob_columns` pins blob-v2 compaction; Guard 9 pins clone-only branch reclaim semantics. RFC-023's `unenforced_pk_filter_shape_is_route_dependent` explicitly forces v2 versus indexed routes and pins the `Some(populated)` / `Some(empty)` / `None` key-filter shapes; `unenforced_pk_conflict_matrix_is_directional` pins the directional filtered/unfiltered and filtered/Append matrix. RFC-024's compile guard pins the public `BranchIdentifier` + current table version + current `Transaction.uuid` + `ManifestLocation.e_tag` current-HEAD witness; the local/shared-`Session` guard proves unchanged-reopen stability, ordinary-commit movement, and same-version ABA, while RustFS covers object-store ABA. RFC-025 adds exact main/named-branch tag-target, sparse cleanup pin/unpin, and branch-tree-deletion guards. RFC-026 pins doc-hidden `has_successor_version`, initializer/readback/shard-writer/durability/fencing, flush/drain, replay watermark, scanner, and merged-generation shapes; runtime Gate E0 classification belongs to `memwal_enrollment_gate.rs`, while v7 Phase-A and v8 B1 publication/recovery belong to the manifest/failpoint suites. B2b adds `cleanup_old_versions_does_not_reclaim_mem_wal_objects` and `mem_wal_deleted_fence_slot_allows_stale_writer_success_on_pinned_lance`: the first proves generic cleanup leaves the present MemWAL fixture unchanged and the second proves deleting the successor's empty fence sentinel is unsafe. The pinned source audit, not those two tests alone, establishes that stock RC.1 exposes no owned MemWAL reclamation API. The RC.1 compiler guard pins the five surveyed public Lance virtual system-column constants to early `.pg` rejection. These guards prove substrate shapes/tokens and negative ownership boundaries; they do not by themselves prove heads/checkpoint activation, the current publisher, or a safe reclamation implementation |
| `memwal_enrollment_gate.rs` | RFC-026's green production-neutral Gate E0 harness, isolated from the production manifest and graph writer. Fourteen substantive local cells plus one explicit unconfigured-S3 skip cover exact no-effect / `N + 1` index / pre-minted empty-shard classification, buried-effect refusal, marker survival, strict inventory/error handling, and the broad fail-closed matrix. The rejected first instrument used `checkout_latest` plus `IOTracker`, which missed local `read_dir`. The accepted exact-version classifier pins doc-hidden `has_successor_version`; its `AttemptTracker` records failed/`NotFound` attempts before forwarding and proves the identical complete six-attempt shape at baseline versions 8/80: four successful manifest HEADs, one `NotFound` manifest HEAD, one successful manifest GET, zero lists. A Unix execute-only `_versions` tripwire proves exact probing works when latest enumeration fails and an unreadable exact HEAD errors. The configured RustFS exact cell passes non-vacuously with the same zero-list shape and owns the positive lost-result/index/empty-shard/reopen sequence plus foreign shard, malformed/loose root, durable WAL, persisted cursor, and corrupt-manifest negatives. S3 ABA remains in `lance_surface_guards.rs`; CI rejects skipped E0/ABA cells. This file never mutates production manifest/schema state or deletes ambiguous artifacts; Phase A consumes its classifier through the private adapter |
| `memwal_stream.rs` | Feature-gated RFC-026 private B1 mechanics, B2 compare-and-chain behavior, B2a provider-failure evidence, and the hidden lifecycle-v3 integration. B1 owns bounded put/ack/replay, authority, cancellation, and manifest-only visibility, including row-local value rejection before any WAL or manifest effect. B2 owns idempotency conflicts, same-generation overlays, stale-authority recapture, durable attribution, and one watcher/fence result over a distinct-key contiguous multi-row physical prefix. The hidden F4 request proof additionally pins graph-scoped `stream_ingest` policy and exact checked-runtime authority before body work; separate root-wide and per-actor transport admission before polling; incremental NDJSON framing across accepted chunks, CRLF, EOF, and over-limit-line boundaries without whole-request retention; strict `$stream` parsing; duplicate, unknown, and reserved-field refusal; explicit canonical IDs; dense schema-ordered node/edge conversion; scalar/list/enum/vector and value-constraint validation before recovery entry; and effect-free Blob-table refusal through a deliberately stale pre-schema-apply handle before either the request or lower B1/B2 seam can invoke MemWAL. Run-splitting cells cover invalid lines, repeated keys, token dispositions, and row/byte ceilings; bounded result/reorder ownership preserves caller order and stop-tail `blocking_ordinal` precedence, while disconnect stops new body polling/admission and transfers the invoked tail to root-owned settlement. The graph-native hidden slice pins strict mixed node/edge rows with no caller-visible table key, one graph-scoped policy decision, catalog-resolved lazy enrollment, move-only bounded normalization, declaration handoff through the existing finite node-before-edge driver round, graph-wide ambiguous-result blocking, and a scalar logical result whitelist with no physical evidence. The bodyless prepare cells pin effect-free witness challenge, checked-runtime/policy ordering, Blob refusal before enrollment, actor-bound durable-receipt replay, and two concurrent request IDs converging on one OPEN lane before ingest/fold composition. F5a extends this same owner with checked-runtime start refusal, coalesced timer/cap folding, trigger-during-fold preservation, cold-reopen discovery without an in-memory pending bit, deterministic finite node-before-edge rounds, root singleflight, retry/backoff visibility, and bounded supervisor shutdown; every effect is still proved by the existing recovery-v14 fold/crash cells rather than a duplicate replay suite. F6a adds one hidden in-process candidate-runtime composition: prepare, ordered NDJSON, an automatic mixed visible/dead-letter fold, stopped/offline selected-token list/export, an ordinary corrected successor, driver restart, clean shutdown ownership, and checked offline disable. F6b1 adds ambient-enrolled pre-byte refusal; managed and unmanaged-terminal checked success; checked `WITHDRAWN | DEAD_LETTERED` pre-byte refusal; one immutable exact-version cut across a later writer; sole nonwaiting root-slot exclusion and release; named-branch delete/recreate exclusion; and preservation of a post-start storage error with slot release. It remains inaccessible to production callers and exposes no SDK/HTTP/CLI/API/OpenAPI ingress, public driver status, or public rebind surface. Lifecycle-v3 owns recovery-covered cold/fold claims, exact full-generation projection after flush/reopen, recovery-v14 ordinary/drain folds, empty and non-empty `OPEN → DRAINING → SEALED`, an empty successor after an ordinary published fold, durable/reopen-stable typed `DataBlock` publication with no base or graph commit (including a fresh-source minimum-cardinality violation whose streamed edge supplies correction identity), idempotent same-request restart, conflicting request/stale-revision refusal, and the claim-before-seal plus seal-before-fold crash boundaries. Recovery-v15 adds receipt-first idempotent `SEALED → OPEN` resume, guarded `DRAINING → OPEN` abort, higher-epoch claim, terminal receipt publication, named-branch refusal, and current-binding-chain ancestry checks. F6b8 adds compile-enforced root-producer ownership transfer through detached resume installation, urgent trigger-before-release, exact empty-owner housekeeping before the unchanged node-before-edge round, prompt retirement, driver-first and resume-first/caller-cancelled races, cross-lane slot reuse, and shutdown waiting for the detached owner. The strict-block path streams `DRAINING` validation directly into the bounded evidence collector; unit owners below pin the detailed cap, empty-evidence refusal, and non-materialized overflow digest. F3d's checked-offline cell releases the served runtime, reaches terminal `DISABLED`, crashes after arming v18 but before a physical effect, and proves retry selects one fresh `SEALED` scope while retaining the old MemWAL inventory; repeating the same occurrence is physically effect-free. B2a injects a recording/failing store at the real Lance table-store boundary and covers post-invocation ambiguity and inert orphan residue. F7a extends this owner with the production checked-runtime graph token bridge, redacted newline result contract, and a non-resetting 50-ms same-declaration coalescing boundary that acknowledges a complete row while its body stream remains pending; server and CLI owners cover transport. No test here implies a supported public lifecycle API. |
| `memwal_stream_cost.rs` | Feature-gated RFC-026 B1, Gate-R0, and B2a decision instrument. It separately measures warm already-claimed durability acknowledgement, cold replay, selected-generation fold scanning, visibility, retained merged metadata, the uncompacted graph-manifest term, legal no-roll estimates, and paired peak RSS. Gate R0 adds a revision-pinned source-audit tripwire, strict current-object classification/reference census, listed path/class/size retain-all comparisons at one/four/eight folds, referenced-cut retry reuse, and deterministic high-entropy near-cap local/configured-RustFS cells. The near-cap cell proves the exact B2-attributed boundary through the real adapter: 3,742 payload bytes per row admits 8,192 rows at 33,550,336 logical bytes, while 3,743 is rejected effect-free at 33,558,528 bytes. The legal generation acknowledges without graph visibility, then folds and publishes exactly once after logical-slice charging plus dense per-scanner-batch take. The reference-environment paired fold peak-RSS lift measured 286,441,472 bytes (about 273 MiB), below a one-sided 384-MiB remeasurement tripwire; common initialization may censor that lifetime high-water lift to zero or a negative value on another runner, and the tripwire is not a runtime allocator limit. B2a adds 1/8/32/128 local and configured-RustFS retained-history sweeps whose terms remain separate: warm ack, cold reopen/replay, fold, visibility, MemWAL/base-table/token-authority/other table-store work, graph-manifest/adapter work, advisory current-object bytes, and whole-process peak RSS. Older retained roots must receive zero reads, writes, or deletes. The only allowed delete shape is Lance's losing manifest-CAS `.binpb.tmp.<uuid>` staging; canonical durable MemWAL delete requests remain zero. LIST totals, wall times, and RSS are advisory—not a quota, SLO, isolated WAL slope, or provider billing. A green test proves private closure/retention behavior; it does not activate a public API. |
| `memwal_stream_cost.rs` (F6b3) | Exact-selected uncovered-tail current-token cost owner. The normal local 1/8-cycle cell and ignored local/configured-RustFS 1/8/32/128 sweeps hold current-token/page cardinality fixed while growing immutable token-ledger receipt history; graph-manifest history also advances during setup. Per sample they report selected version, lookup-index coverage, serialized page bytes, and cumulative advisory whole-process RSS. Fresh-handle hit/miss plus the first terminal page, then warm hit/miss and repeat terminal pages, report token-read counts, total table-store read bytes, manifest reads/bytes, adapter operations, and the applicable per-sample warm/repeat p50 plus max-of-eight. This historical F6b3 fixture did not claim a cold provider cache, receipt-key lookup, or a covered/reconciled curve; F6b7 adds the latter two terms. |
| `memwal_stream_cost.rs` (F6b7) | Paired selected-token lookup-index decision owner. A failpoints-only test writer creates and selects one content-identical fully covered `CreateIndex` cut, then the same fixture measures current-token and profile-receipt hit/miss on both cuts. The 2026-08-03 configured-RustFS 6/20/68/260-fragment uncompacted profile-cycle sweep records a bounded NO-GO for that physical shape: all four recurring terms retain a 3× token-table read-request ratio and the deepest byte term remains 2.084×, but total maintenance request-cost amortization grows to 1,697 calls at 260 fragments, above the predeclared 1,000-call ceiling. No standalone production reconciler is scheduled; remeasure beyond 260 exact uncovered fragments, after a Lance-pin/index-grammar change, or before coupling reconciliation to graph-manifest compaction/checked Optimize. |
| `memwal_stream_cost.rs` (F6b4) | Ignored production-size dead-letter encoder/verifier decision cell plus paired RSS subprocess. It drives 8,192 adversarial candidates through the exact production codec at 64 MiB minus one, exactly 64 MiB, and one byte over; records source, canonical-payload, encoded length/capacity, encode/verify time, and isolated peak RSS; and pins a 192-MiB remeasurement tripwire. Nested legal payloads remain validated raw JSON during verification/export instead of expanding into recursive `serde_json::Value` trees. The 2026-08-02 local macOS reference exact-cap run measured 10,364,432 source-value bytes, 62,301,270 canonical-payload input bytes, 67,108,864 encoded length/capacity, 286,280 µs encode, 2,254,424 µs verify, and a 146,292,736-byte paired peak-RSS lift. These are evidence, not admission, quota, or SLO. |
| `durable_head_lookup_cost.rs` | RFC-024 Gate A decision instrument, isolated from the production manifest schema/publisher. At fixed catalog width 10 it runs the full absent/reconciled/one-uncovered/eight-uncovered/reconciled-after-tail matrix over compacted and uncompacted histories, with cold-open and warm-repeat measurements on local FS and bucket-gated S3/RustFS. Default depths are 20/80; the ignored decision-scale cell runs 10/100/1,000. Correct exact heads, flat indexed `rows_scanned`/range work, an index-absent growing negative control, and observable bounded tails all pass; after the eight-fragment tail, `optimize_indices` returns coverage to zero uncovered and representative `rows_scanned`/range work from 27→10 / 17→10. The test deliberately pins the no-go: uncompacted RustFS cold object reads/bytes and compacted byte terms grow, while RC.1 also crosses a bounded one-operation boundary by 1,000 commits, so RFC-024 remains research-blocked. `rows_scanned` is an RC.1 debug proxy, not a universal decoded-row counter. Object-store wrapper bytes and Lance execution-summary bytes are separate fixture-owned metrics and are not additive |
| `checkpoint_retention_cost.rs` | RFC-025 Gate 0 decision instrument, isolated from the production manifest schema. It models three live checkpoints at catalog width 10 and measures complete list, exact show, and cleanup-root authority reads across absent/reconciled/eight-uncovered index states, compacted/uncompacted layouts, and cold/warm access. It also owns the reference V1 name-normalization matrix. Default local depths 20/80 pass the checked-in **no-go-preservation** assertions; the RC.1 ignored 10/100/1,000 run shows reconciled uncompacted work and the bounded tail flat, but rejects the current format shape after compaction: list/cleanup scan bytes grow 17,012→38,000 cold and 12,336→15,064 warm; show grows 29,348→53,064 and 24,672→30,128; scan operations add one at 1,000. The S3/RustFS cell is bucket-gated and was not run for this decision. The result keeps RFC-025 research-blocked; current v19 adds no checkpoint state |
| `warm_read_cost.rs` | Cost-budget tests for the warm read/control path (query-latency work), measured at the object-store boundary with Lance `IOTracker` (the LanceDB IO-counted pattern): a warm same-branch read does 0 manifest opens, 1 version probe, validates the schema once (Fix 1 / finding A / Fix 2 at commit-history depth); a cold other-branch resolution derives snapshot state and lineage from one coherent manifest open/scan; native branch create and create-from each use one post-gate open/scan, while delete uses one target capture plus one native-ref opener and only one row scan; stale same-branch reads perform exactly 2 probes and refresh manifest-only; recreated non-main branches with the same Lance version refresh by incarnation; recreated branch-owned table handles are distinguished by table e_tag or refresh-time cache clearing; recreated traversal topology is protected by per-edge-table e_tag in the graph-index cache key or refresh-time cache clearing; a warm *repeat* read does 0 table opens via the held-handle cache and a write re-opens only the changed table at its new version/e_tag (Fix 3/6A). Also the CSR topology-build cost guards: `fresh_branch_traversal_reuses_main_graph_index` (A1 — a lazy-fork branch reuses main's cached CSR index, 0 rebuilds via `graph_build_count`) and `single_edge_query_builds_only_referenced_edge` (A2 — a one-edge query builds only that edge via `graph_edges_built`); both force CSR via the scoped `with_traversal_mode` seam, so they need no `#[serial]`. See "Cost-budget tests" below. |
| `write_cost.rs` | Cost-budget tests for the WRITE path (RFC-013), the latency twin of `warm_read_cost.rs` on the **shared `helpers::cost` harness** (`measure`/`IoCounts`/`assert_flat`/`local_graph`). Runs on **local FS**; gates the **internal-table** term (`__manifest` scans flat in commit-history depth, lineage rows included — `internal_table_scans_are_flat_in_history`, now **green every-PR** since RFC-013 step 2 brought the internal tables into `optimize`; the test compacts at each depth before measuring), graph-visible maintenance arbitration (`ensure_indices_manifest_reads_are_flat_in_history` and `optimize_manifest_reads_are_flat_in_history`), plus green every-PR guards (single-insert `data_writes` bounded, a per-write read-op ceiling that fails the moment a round-trip is added, and a `measure_with_staged` fitness assert that a keyed insert routes through the exact-`id` fenced adapter once with no bare `stage_append`/vector-index build). Also gates the batched committed `@unique` probe: `unique_probe_io_is_flat_in_delta_rows` sweeps DELTA size (4 vs 64 rows) at fixed shallow history and asserts `data_open_count`/`data_scan_reads` flat — red when the cross-version probe regresses to per-row scans/opens. The **data-table opener** term is S3-only — see `write_cost_s3.rs` and the backend-split note in "Cost-budget tests" below. RFC-023's representative row-count and peak-RSS decision measurements use the scenario harness, not this every-PR I/O budget |
| `write_cost_s3.rs` | Bucket-gated (skips without `OMNIGRAPH_S3_TEST_BUCKET`) twin of `write_cost.rs` on the same `helpers::cost` harness: gates the **data-table opener** term (per-write latest-version resolution flat across commit depth on a real object store — per-version GETs are invisible on local FS). A cost gate, not a correctness test — run on demand, not in the every-merge `rustfs_integration` job (see the backend-split note in "Cost-budget tests" below) |
| `helpers/cost.rs` | The shared cost-budget harness (not a test): `IoCounts`/`StagedCounts` (counts by table class), `measure`/`measure_with_staged` (the one place the `with_query_io_probes` + `MergeWriteProbes` task-local + `IOTracker` wiring lives; reads per-op deltas via lance's `incremental_stats()`, the upstream per-request idiom from `rust/lance/src/dataset/tests/dataset_io.rs`), `cost_harness`/`GraphIoMeter` (installs ONE `__manifest` `IOTracker` for a whole test body so the graph opens **under** it and `manifest_reads` is **ground truth** — every read regardless of handle age, the warm-coordinator freshness probe included — closing the blind spot where a per-op tracker installed at measure time cannot see a long-lived handle's reads; outside `cost_harness`, `measure` falls back to fresh per-op tracking, so `write_cost_s3.rs` is unaffected), `open_tracked_lance_dataset` (attaches a caller-owned `IOTracker` before `DatasetBuilder::load`, so a cold-open fixture includes latest-manifest resolution), `last_manifest_reads()` (the manifest read log for `assert_io_eq!`-style failure diagnostics), `assert_flat(curve, select, slack, what)`, and store-agnostic `local_graph`/`s3_graph` fixtures. The general `IoCounts` vocabulary remains operation counts; RFC-024's decision fixture owns its object/plan byte metrics. `warm_read_cost.rs`, `write_cost.rs`, `write_cost_s3.rs`, and the RFC-024 instrument consume the relevant seams |
| `benchmark_scenario_contract.rs` | Source/protocol contract for the non-CI scenario harness. RFC-023 pins the production route's explicit `strict_insert_preflight_calls == 0` assertion and emitted `probe_strict_insert_preflight_calls` field, alongside route labels, clean-tree/binary identity, child-protocol refusal, and exact-content verification fields. A benchmark record therefore cannot silently claim the proven path after paying a target preflight |
| `lifecycle.rs` | Graph lifecycle and schema state, including the v6-origin creation invariant—preserved through current v19—that every fresh node/edge table declares exactly physical `id` as Lance's unenforced PK |
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
| `export.rs` | NDJSON streaming export filters; RFC-023's blob fixture also performs a later `LoadMode::Append` strict insert into a populated current-format table (preserving the v6 PK contract) and verifies both exact blob bytes and exact-`id` PK metadata afterward. `export_jsonl_round_trips_branch_snapshot` separately exports `main` and a named feature branch, rebuilds each into a main-only graph, and proves independent identity domains plus disjoint, self-contained histories |
| `s3_storage.rs` | S3-backed graph (skipped unless `OMNIGRAPH_S3_TEST_BUCKET` is set). Includes `s3_fresh_branch_traversal_reuses_main_graph_index_with_etags` — the CSR topology cache-key test on a **real** per-table e_tag (`None` on local FS, so `warm_read_cost.rs` can't reach this path); forces CSR via the scoped `with_traversal_mode` seam |
| `lance_version_columns.rs` | Per-row `_row_last_updated_at_version` behavior |
| `validators.rs` | Schema constraint enforcement (enum, range, unique, cardinality) across JSONL load, mutation insert/update. ALL THREE write surfaces — mutation, bulk load, AND merge — route through the unified `crate::validate` evaluator (Δ-scoped, index-backed, reusing these leaf checks). Cross-version-uniqueness closure: `cross_version_unique_rejected_on_mutation_insert` + `reinsert_existing_key_is_upsert_not_unique_violation` (mutation path); `cross_version_unique_rejected_on_append_load` + `merge_load_reupsert_existing_key_is_not_unique_violation` (load path). Per-table `Overwrite`: `overwrite_load_validates_ri_against_new_image` (an edges-only overwrite still resolves RI against retained committed nodes) + `append_load_rejects_orphan_edge`. The evaluator's own unit tests live in `src/validate.rs` (`#[cfg(test)]`), including the correction identity for a fresh-source minimum-cardinality violation; its merge-conflict equivalence is pinned by `merge_truth_table.rs` (OrphanEdge) + `branching.rs` (Unique/Cardinality merge tests). Intra-batch duplicate-`@key` rejection on every load mode is pinned by `consistency.rs::loader_rejects_intra_batch_duplicate_keys`; the mutation-coalesce counterpart (insert+update / chained updates of one id are NOT a self-collision) by `writes.rs`. Non-String `@unique` columns probe committed state with a TYPED literal (not a stringified key): `cross_version_unique_rejected_on_date_column` + `noncolliding_write_to_date_unique_column_succeeds` (a `Date @unique` collision is a proper `@unique` violation, and a distinct value does not raise a Date32-vs-Utf8 coercion error). Cardinality is keyed by edge id, last-wins (matching commit's `dedupe_merge_batches_by_id`): `merge_load_edge_src_move_rechecks_vacated_src_cardinality` (a Merge-load moving an edge recounts the vacated src for `@card` min) + `merge_load_duplicate_edge_id_counts_once_per_card` (a dup edge id under two srcs in one batch counts once, no spurious max violation). Direct deletes capture the ids they remove (from the delete op's own scan) into the change-set's `deleted_ids`, so a delete emptying a src is validated: `mutation_delete_edge_below_card_min_rejected` (a `delete Edge` dropping a src below `@card` min is rejected, not silently committed). |
| `merge_cost.rs` | Cost budgets for branch MERGE on the shared `helpers::cost` harness: `merge_validation_is_delta_scoped` keeps validation tied to the delta and caps the common one-row fast-forward route at 3 internal opens / 3 coherent manifest scans. `merge_manifest_cost_grows_with_history` caps the diverged route at 4 opens and 4 scans across the checked depths while preserving the growing object-read tripwire. Retained source/target manifest `Dataset` probe handles and combined manifest+lineage decoding reduce the pre-slice measured depth-5/depth-80 baseline from 59/651 manifest reads to 40/410, but the surviving journal fold and fresh publisher authority scan remain history-sensitive on an uncompacted graph; this is reduced amplification, not a history-flat claim |
| `branch_control_cost.rs` | Cost-budget tests for native branch CONTROL ops on the shared `helpers::cost` harness: `branch_delete_manifest_reads_bounded_per_surviving_branch` gates the SLOPE of `branch_delete`'s `__manifest` reads per surviving branch — the delete dependency check reads one manifest-only snapshot per foreign branch, never a full cold resolve (state + lineage scans + schema-contract re-read) per branch |
| `policy_engine_chassis.rs` | Engine-layer Cedar enforcement (MR-722): allow + deny through every `_as` writer via the SDK directly — no HTTP — proving embedded and CLI callers hit the same gate as the server, with action × scope shapes matching `authorize_request` |
| `maintenance.rs` | `ensure_indices`, `optimize` (compaction), `repair` (explicit uncovered-drift publish), and `cleanup` (version GC): empty/idempotent/no-op edges, policy validation, head preservation. EnsureIndices refuses uncovered drift before arming its identity-bearing v9 envelope and keeps untrainable Vector work pending. Cleanup pins exact keep-count behavior, lazy-branch retention, graph-wide fail-closed ordering, and refusal of uncovered main HEAD drift before GC. Optimize's bounded payload inside the v9 envelope publishes multiple productive data tables through one graph commit, emits no lineage/sidecar at steady state, skips uncovered drift, refuses pending recovery, and compacts blob-v2 tables. Repair previews/heals verified maintenance drift and requires `--force` for semantic drift |
| `failpoints.rs` | Failure-injection coverage (gated on `failpoints` feature). RFC-026 Phase A owns exact enrollment no-effect, index-only, and index-plus-empty-shard crash recovery; named-branch enrollment refusal; uncovered-index format refusal; typed maintenance/GC/index-build exclusion on an `OPEN` lifecycle; and disjoint-table maintenance/repair allowance. RFC-022 includes deterministic post-stage/pre-effect races for mutation/load uniqueness and strict disjoint-head changes, plus the cross-handle post-effect `RecoveryRequired` → read-write-open rollback cell. Branch merge adds the captured-source advance cell; post-confirm target-winner compensation; mixed physical + pointer-only delta recovery with fixed commit id/actor/parents; both sidecar-before-first-ref and ambiguous-ref-create recovery; and an 8,193-delete between-chunk crash proving an `Armed` exact-transaction prefix is rolled back before the successful retry. Identity-bearing v9 SchemaApply is pinned by `schema_apply_phase_b_failure_recovered_on_next_open` (exact confirmed roll-forward with fixed commit id + initiating actor), `schema_apply_partial_table_effect_rolls_back_exactly` (Armed proper-prefix compensation), `schema_apply_recovery_reclaims_owned_add_type_target_and_retry_succeeds` (strict owned first-touch cleanup), `schema_apply_first_touch_foreign_winner_is_preserved_not_adopted` (foreign unregistered winner preservation), `schema_apply_post_effect_disjoint_winner_is_preserved` (winner-preserving compensation), `schema_apply_post_effect_same_table_winner_fails_closed` (buried-effect refusal), `schema_apply_recovers_partial_schema_promotion_after_commit_crash` (read-only refusal for both valid and corrupt intents in the torn manifest/schema window, followed by fixed-outcome completion of a partial source/IR/state promotion), and `schema_apply_live_query_waits_for_coherent_schema_publication` (same-handle publication wait plus pre-apply-handle query/export/whole-graph-index capture from the operation-local accepted catalog). Metadata-only before/after-staging and rollback-retry cells keep the empty-effect v9 boundary pinned. EnsureIndices v9 recovery retains both boundaries in `recovery_rolls_forward_ensure_indices_on_feature_branch`: the first residual rolls forward on the next read-write open, and a second roll-forward-eligible `EffectsConfirmed` residual under an unchanged captured token is completed by a same-handle retry before new planning. `ensure_indices_complete_armed_effects_roll_back` keeps the authority-clean complete-effect Armed rollback rule isolated, while `ensure_indices_entry_barrier_refuses_partial_armed_before_staging` leaves one of two table effects pending and proves the original `RecoveryRequired` wins before the remaining index can reach the post-stage failpoint. Its remaining cells are `ensure_indices_stage_btree_failure_leaves_existing_tables_writable` (after a clean entry barrier, expensive mixed-index staging remains outside the final authority/gates), `ensure_indices_first_touch_crash_before_ref_recovers_cleanly` (sidecar-before-ref no-effect recovery), `ensure_indices_mixed_first_touch_rollback_does_not_delete_moved_ref` (owned-effect rollback and sibling first-touch cleanup), and the no-work/no-sidecar failpoint cell; the recovery module separately pins existing + first-touch payload round-trip and identity-less-input refusal. Optimize's graph-wide identity-bearing v9 envelope is pinned by `optimize_phase_b_failure_recovered_on_next_open` (two-table roll-forward), `optimize_multi_table_partial_effect_rolls_back_under_one_v2_sidecar` (one shared sidecar, no partial visibility, compensation), `optimize_post_manifest_failure_finalizes_multi_table_v2_sidecar` (lost publish acknowledgement), and `optimize_excludes_pending_only_vector_table_from_v2_sidecar` (pending status cannot poison sibling recovery), plus its late-sidecar/main-gate/retry cells. Native controls are pinned by `native_branch_controls_reclassify_lost_acknowledgements` (matching create and absent-ref delete, with no version/lineage movement); `armed_first_touch_recovery_accepts_missing_target_ref` additionally forges and reclaims the clone-only/no-`BranchContents` table state. Legacy path overlap has both sides pinned: `armed_first_touch_recovery_defers_legacy_path_overlap_until_leaf_delete` permits open only for a proven no-effect intent, while `partial_first_touch_recovery_fails_closed_on_legacy_path_overlap` leaves one exact multi-table effect and verifies open fails closed until offline leaf cleanup lets rollback converge. Other control/recovery race cells include `first_touch_post_create_open_error_keeps_recovery_ownership`, `branch_delete_orphans_sidecar_armed_after_initial_barrier`, `branch_merge_fences_target_delete_recreate_aba`, `branch_merge_fences_concurrent_sync_on_same_handle`, `branch_merge_rejects_fresh_target_manifest_change_before_effects`, `branch_merge_rechecks_late_sidecar_after_table_gates`, `optimize_rechecks_late_schema_apply_sidecar_after_main_gate` (late zero-pin graph-global intent), `optimize_rechecks_late_disjoint_main_sidecar_after_main_gate` (table-disjoint intent sharing `graph_head:main`), `optimize_holds_main_gate_through_disjoint_table_effects` (post-relist branch-gate lifetime), `cleanup_rechecks_sidecars_under_gc_gates`, `full_recovery_rereads_sidecar_body_after_discovery`, `recovery_discovery_skips_sidecar_deleted_after_list` (an unrelated write succeeds after a listed sidecar is published/deleted), and `read_only_recovery_discovery_skips_sidecar_deleted_after_list` (read-only open succeeds against that same concurrent completion). The suite also includes the established per-writer effect → manifest-CAS recovery tests, write-entry in-process heal contract, storage-fault matrix, S3 recovery twin, and convergence-idempotent roll-forward regression. |
| `failpoint_names_guard.rs` | Source-walk guard (same defense-in-depth shape as `forbidden_apis.rs`): every failpoint call site across engine + cluster (`maybe_fail`, `ScopedFailPoint::new`/`with_callback`, `Rendezvous::park_first`) must reference a compile-checked `failpoints::names` const, never a bare string literal — a typo'd literal compiles but silently never fires |
| `recovery.rs` | Open-time recovery sweep — identity-bearing schema-v9 envelopes for established graph writers; recovery-v13 profile changes; recovery-v14 lifecycle-v3 enrollment/claim/fold/terminal receipts; recovery-v15 resume/abort; recovery-v16/v17 sealed maintenance; recovery-v18 rebind; historical recovery-v19 retirement; recovery-v20 correction; and recovery-v21 `DeadLetterFold` plus `StreamAuthorityRetirementV2`. V14–v20 retain their exact historical grammars. V21 pins the closed object/base/token plan, terminal candidate/evidence binding, versioned attribution, marker-only all-diverted base effect, conditional-object/base/token commit-order classification, sole joint lineage/lifecycle publication, and three-disposition retirement counts/cut. Effect-free v21 intents retire without publication; exact partial effects roll forward, while foreign, buried, token-only, stale-authority, or corrupt outcomes fail closed. Historical recovery-v10 enrollment, recovery-v11 base-only fold, recovery-v12 lifecycle-v2 fold, recovery-v19 two-disposition retirement, recovery-v20 correction, and the frozen v14 scaffolds are never reinterpreted. Explicit refusal, fresh under-gate reread/reparse, fixed lineage, recovery audit, and read-only guards remain pinned. |
| `composite_flow.rs` | Compositional/narrative end-to-end stories — multi-step flows that compose mechanics covered by other test files. Catches integration regressions where individual operations all pass their unit tests but their composition breaks (sequential merges, post-merge main writes, time-travel through merge DAG, reopen consistency over multi-merge histories, post-optimize and post-cleanup strict writes). |

RFC-026 reclamation qualification: the two B2b runtime guards do not prove
that every possible safe RC.1 API is absent. The source audit establishes that
surface fact; the tests prove the narrower generic-cleanup non-ownership and
deleted-successor-sentinel fencing hazard.

### RFC-026 F3b evidence ownership

`memwal_stream.rs` currently proves the narrow recovery-v16 capability at its
implemented boundary: one productive checked-runtime `SEALED` EnsureIndices
transition, ambient-operation refusal, atomic table-pointer/lifecycle-proof
publication, a true no-work retry with no lineage or sidecar, subsequent
resume/ingest/fold composition, and cold roll-forward of one confirmed
sidecar. It also pins complete-effect `Armed` compensation: recovery restores
the table and atomically refreshes the `SEALED` proof to that Restore HEAD.
Recovery-v8's established EnsureIndices owners continue to cover the generic
physical transaction boundaries.

Do not describe that evidence as the complete F3b matrix. Mixed productive and
no-work tables, `OPEN`/`DRAINING`, named-branch, stale-proof, mismatched-runtime,
proper-prefix rollback, and the remaining crash boundaries still need focused
cells before the capability broadens. F3c adds the separate checked `SEALED`
Optimize slice under recovery-v17; F3d adds the separate recovery-v18 private
physical-rebind owner while leaving public/production rebind inactive.

### RFC-026 F3c evidence ownership

`memwal_stream.rs` pins recovery-v17 at its current narrow boundary. One cell
stops after Optimize effects are confirmed but before manifest publication,
then proves cold recovery selects only the recorded achieved HEAD and refreshes
the `SEALED` proof atomically. A second cell creates a proper-prefix `Armed`
effect across two productive tables, proves recovery restores the moved table
with a matching lifecycle proof without publishing either original effect, and
then proves a normal retry remains productive. Broader public maintenance and
transport evidence remain future work.

### RFC-026 F3d evidence ownership

`recovery.rs` owns the narrow v18 format and dispatch boundary.
`stream_rebind_v18_pins_physical_armed_grammar_and_exclusive_strand` round-trips
the exact `PhysicalArmed` JSON and rejects a v17 schema, the wrong writer kind,
and a second v14 payload. `process_sidecar_routes_schema_v18_to_rebind_not_frozen_v14`
passes that shape through the common recovery dispatcher and pins the v18
owner's exact-authority refusal, including that it cannot fall through to the
frozen v14 discriminator. `stream_lifecycle.rs` adds
`rebind_builds_only_the_exact_fresh_scope_sealed_successor` and
`rebind_successor_rejects_unbound_receipt_tail_or_physical_cut` for the terminal
row grammar. `memwal_stream.rs` then owns the first end-to-end adapter cell:
`checked_offline_rebind_retries_effect_free_intent_and_selects_fresh_sealed_binding`
proves the stopped/offline, terminal-`DISABLED` authority boundary, effect-free
recovery of a sidecar-only crash, one fresh `SEALED` binding with the exact
two-version physical transition, retained prior inventory, and idempotent
same-occurrence replay.
`rebind_rolls_forward_confirmed_physical_and_ledger_without_a_third_table_head`
stops the same adapter after the N+2 table and exact token-ledger transaction
are durable but before manifest publication. It pins the old selected table and
lifecycle beside the N+2 raw HEAD, then drops the handle and proves a genuine
cold ReadWrite open publishes the pre-minted ledger/table/lifecycle outcome,
selects the fresh `SEALED` scope, deletes the sidecar, and never manufactures
N+3. After that recovery, the cell creates a named branch and proves same-ID
receipt replay still returns the selected occurrence without a manifest,
table, token, lifecycle, or sidecar effect, while a fresh occurrence refuses
the changed topology with the same effect-free boundary. The inline MemWAL
tests own the earlier physical staircase: index drop, fresh index, shard
provisioning and manifest-only claim convergence, strict staging-shape
classification, and retention of the prior shard prefix. The worker unit
`passive_current_binding_uses_only_fixed_size_selected_commitment` pins the
fixed-size selected-commitment validation rules; forbidden-API guard
`ordinary_stream_authority_capture_never_relists_retained_binding_history`
pins ordinary capture to the current-binding validator and excludes the full
inventory validator. Complete prefix inventory remains a
cold-open/rebind/recovery check.

This is not the complete F3d recovery matrix. Focused cells still need to cover
token-ledger staging/commit ambiguity before confirmation, lost manifest-CAS
acknowledgement, and post-visible cleanup/audit. Public/production rebind and
every transport surface remain inactive.

### RFC-026 F3e evidence ownership

Recovery-v19's in-source suite owns the new format boundary. It pins the
closed, lineage-neutral `StreamAuthorityRetirement` grammar, exclusive profile
gate, exact N+1 receipt transaction, zero-`WITHDRAWN` refusal, receipt/cut
binding, and cold roll-forward into `RETIRED` without moving graph heads,
creating graph lineage, or appending `RecoveryAudit`. The roll-forward cell
also reopens the source, proves receipt-bearing export provenance with a closed
selected-branch-member witness, and proves a normal maintenance writer is
fenced. Stream-profile tests pin the immutable,
actor- and plan-bound receipt chain. Cluster and CLI tests own actor/offline/
declared-graph preflight plus the exact `--graph`/`--config` command scope.
The final-v17 merge recorded the genuine v16↔v17 refusal/rebuild fence. The
current source suite retains v16→CURRENT evidence; the live adjacent gate now
builds immutable final v18 and proves v18↔v19. The v17↔v18 result remains
historical; the checked-in v17 seam now provides source→CURRENT coverage.

The retired-export cells pin `branch_member` as a closed selected-member
witness: canonical branch, exact Lance branch identifier, optional graph head,
manifest version, `table_witness_digest`, and a recomputable
`branch_member_digest`. Main and named-branch exports retain the same root
receipt and exact `ordered_branch_member_digests`, but select their own member
with `selected_member_index`. Load recomputes the selected member digest from
the witness, checks that exact slot, and combines the ordered digests with
`source_schema_ir_hash` to recompute the receipt's `export_cut_digest`. The
source schema hash is proof input for the frozen source cut, not a requirement
that it equal the fresh target graph identity; ordinary loader schema and row
validation enforce target compatibility. The rebuild imports no live stream
authority. Focused tamper cases cover the graph head, manifest version, table
and member digests, selected index, sibling digest, and source schema hash.

This evidence activates only the stopped/offline retirement/export exit. The
later F3f suite separately owns the production `WITHDRAWN` correction path;
`memwal_stream.rs` owns hidden F4/F5a evidence and the engine side of the F7a
graph bridge. Server and CLI suites own the public F7a ingress transport;
general lifecycle transports remain outside these slices.

## Fixtures

`crates/omnigraph/tests/fixtures/` holds the canonical schema (`.pg`), seed data (`.jsonl`), and queries (`.gq`) shared across tests. Reuse these before inventing new ones — the helpers harness already knows how to load them.

## Test helpers

- **Engine** — `crates/omnigraph/tests/helpers/mod.rs`: `init_and_load()` (bootstrap a temp graph + load standard fixture), `snapshot_main()`, `snapshot_branch()`, query/mutation runners, row collection and counting. Use these instead of hand-rolling.
- **CLI** — `crates/omnigraph-cli/tests/support/mod.rs`: `Command`-style wrapper for invoking `omnigraph`, server-process spawning, fixture resolution, output assertion helpers.
- **Server** — no shared helpers; server tests call the `Omnigraph` engine API directly and exercise endpoints over the wire.

> Note: the shared storage adapter has an in-memory backend (`ObjectStorageAdapter::in_memory()`, full contract including true conditional updates) used by the adapter contract tests in `crates/omnigraph-storage/src/lib.rs`. Those tests also pin the optional single-GET text-read contract: present objects return `Some`, typed `NotFound` returns `None`, and non-absence failures remain loud. The engine's `crates/omnigraph/src/storage.rs` is a compatibility facade over that implementation. This covers only the text-object layer (sidecars, schema staging, cluster state) — **Lance datasets bypass the adapter**, so engine integration tests still use `tempfile::tempdir()`. An in-memory Lance substrate remains an architectural ask — keep it explicit in [docs/dev/invariants.md](invariants.md) under known gaps.

## Failpoints (fault injection)

RFC-026 Phase A uses the same suite for its three enrollment crash states:
`stream_enrollment_no_effect_crash_retires_intent_and_can_retry`,
`stream_enrollment_index_only_crash_rolls_forward_and_fences_ordinary_writes`,
and
`stream_enrollment_empty_shard_crash_rolls_forward_without_reclaim_or_reclaiming_epoch`.
The surrounding cells pin post-publish audit-failure recovery and Phase-D
sidecar-delete failure returning visible success before reopen cleanup, main-only
topology, uncovered index and raw-shard residue without lifecycle authority,
live HEAD movement past the durable witness, typed maintenance/index/GC
exclusion for `OPEN`, and allowance of a disjoint-table effect. These historical
cells remain the enrollment crash owner; the private row/fold/quiesce coverage
lives in `memwal_stream.rs` and the v14–v20 in-source suites below. In-source
manifest/engine tests separately pin lifecycle CAS, effect refusal for
`OPEN`/`DRAINING`/`SEALED`, admission ordering, and the narrow native-branch
rule: create/delete refuse active lifecycle state but may proceed at `SEALED`
without moving table HEAD. The existing lazy-child branching cell also creates
and deletes a grandchild whose physical ref remains its ancestor, preventing
Phase-A main admission from regressing inherited named-branch semantics.

### RFC-026 Phase B1 coverage ownership (implemented)

Phase B1 is a new private row-admission/fold area, so it earns one focused
feature-gated `crates/omnigraph/tests/memwal_stream.rs` owner instead of adding
row behavior to the production-neutral `memwal_enrollment_gate.rs`. A single
`#[doc(hidden)]` test seam under `failpoints` reaches the crate-private core;
`forbidden_apis.rs` exact-counts it so production visibility does not widen.
The implemented coverage keeps the boundaries split as follows:

- in-source `table_store::mem_wal` tests own exact persisted writer-config
  v2 encoding, the no-auto-roll profile, the four-case active/flushed reopen
  classifier, root-scoped cross-handle singleflight, generation reservation,
  and registry retirement.
  Two-handle claim/eviction races prove one owner and no eviction past an
  in-flight waiter. Retirement closes the worker to puts before public
  `ShardWriter::abort`, and no test treats `ShardWriter::close` as durability
  evidence — a posture that holds regardless of upstream's contract, which
  tightened in Lance 9.0.0 (#7769) to propagate final flush failures instead of
  returning a false `Ok(())`. One background-owned abort completion is retained in the retired
  entry; a caller deadline never cancels that future, retries abort, or permits
  reopen. A stalled-handler/deadline/second-retirement test pins that exact
  RC.1 `shutdown_all` hazard. A claim-vs-drain
  race holds the shared admission lease from before
  epoch claim through durability or quiesced retirement and proves an exclusive
  drainer cannot capture a stale floor. Stale-capture-vs-fold/drain and
  late-relevant-sidecar races prove fresh under-lease checks run before claim
  and again before put, releasing to exclusive recovery and restarting when
  required;
- in-source `stream_request` unit tests own the transport-neutral request
  algebra: arbitrary-chunk/CRLF/EOF incremental NDJSON framing, lazy
  one-frame-at-a-time consumption, refusal of a transport chunk over 32 MiB
  without advancing framing, over-limit-line discard and resume, separate
  root-wide/per-actor request and byte admission, the complete internal tagged
  status mapping, stop-tail precedence, `blocking_ordinal`, bounded
  caller-order reordering, permit release, buffered-result-before-task-error
  ordering, clean fused EOF, cancellation-safe join ownership, and propagation
  of a root request task panic through both normal terminal receive and the
  cancellation proof seam.
  `memwal_stream.rs` owns the composition of those pieces with the low-level B2
  path: one physical call may carry one non-empty contiguous caller-ordinal
  prefix with distinct keys, while invalid lines, repeated keys, token
  dispositions, or row/byte ceilings split runs without adding a second
  durability implementation. A focused unit cell pins the bounded 256-row
  descending exact token scan where prefix one is too large but prefix two fits
  after replacing a larger current winner; the non-monotonic projection is
  never binary-searched. The feature-gated request wrapper validates
  `$stream` and strictly normalizes each node or edge before B2; it never
  collects the whole NDJSON request. Its bodyless prepare helper echoes an
  opaque eligibility witness through the test seam and reuses the same
  enrollment/recovery implementation; focused cells own challenge inertness,
  receipt replay, and concurrent convergence. The existing authenticated
  one-row cells pin policy/runtime refusal before input work; the in-source
  request-registry cells pin atomic root/per-actor admission and release.
  `memwal_stream.rs` additionally pins ordered per-line outcomes across
  multi-row physical prefixes, temporary B2 preprocessing ownership for
  adapter-local stopped-tail sizing, fresh achieved writer authority on a
  post-cold-claim refusal, and disconnect ownership: no new line is read or
  admitted after output loss, while the bounded invoked tail settles under
  root ownership. The file also owns
  all-or-nothing validation of one contiguous ordinal range, whole-generation
  8,192-row/32-MiB reservation (including duplicate submissions while the same
  live generation remains active), the single charge → shared admission →
  same-key queue → mode lock order and its historical three-party deadlock,
  immediate fold-only replay accounting while already-charged callers drain,
  effect-free
  `FoldRequired`, watcher-backed durability followed by the same writer's
  post-durability `check_fenced()` before clean acknowledgement, post-watcher
  epoch loss as typed `AckUnknown` plus worker retirement, every other
  post-invocation error as typed `AckUnknown`, cardinality-only same-payload
  retry without attempt reconciliation, and the adversarial
  `X(unknown) -> Y(durable) -> retry X`
  stale-overwrite shape that keeps public B2 gated on sequencing/idempotency.
  A lost-ack retry crosses the mandatory fold boundary when durable replay
  residue exists; it is not charged to the retired generation. This file also
  owns strict blocked input, explicit one-generation fold, output expansion refusal,
  and pre-/post-`__manifest` visibility. None of this activates an SDK, HTTP,
  CLI, API DTO, or OpenAPI ingress surface. The v13 F3a cells extend this owner
  with private `SEALED → OPEN` resume, guarded `DRAINING → OPEN` abort, and
  bounded current-binding ancestry validation. V16's F3d in-source recovery and
  lifecycle cells separately own private physical rebind; no public control surface exists;
- existing `lance_surface_guards.rs` extends its compile/runtime guards for the
  exact `put_no_wait` return shape; forced seal/drain, epoch, and generation
  primitives; quiesced `abort`; public `in_memory_memtable_refs`, BatchStore
  iteration, replayed stored batches, and
  `BatchStore::set_max_flushed_batch_position`; public
  `LsmScanner::without_base_table`; caller-supplied exact `ShardSnapshot`;
  optional-store-parameter/required-Session propagation; and
  streaming/generation tags.
  An adversarial rollover cell delays generation `N + 1`'s WAL PUT and pins the
  RC.1 false-ack bug; adapter tests prove B1 retires before that path. Another
  graph-level cell parks immediately after watcher success, claims a successor
  epoch through a test-only foreign writer, and proves the predecessor returns
  `AckUnknown`, retires, and leaves its durable row replayable instead of
  returning a clean acknowledgement. This closes the adapter outcome only; the
  deleted-sentinel negative guard and Lance-owned reclamation gate remain.
  Another guard pins the replay watermark at exact `len - 1` only with zero
  frozen refs and no possible put, and proves reseal writes no extra WAL entry,
  performs no second PK-index insertion, and stamps the exact replay cursor. Repeated
  pre-shard-manifest failures/crashes must not increase replayed batch or row
  count. A fast failed-flush cell starts `wait_for_flush_drain` after the
  handler removed its watcher and proves B1 still refuses without empty frozen
  refs plus the exact authoritative generation/cursor. Channel-loss and
  handler-stall cells prove the background registry task retains the exclusive
  lease and owns seal/drain/abort to completion while caller deadlines return
  typed recovery, keep admission closed, and never arm the fold sidecar. A
  worker-unit cell separately parks the cold fold opener past the original seal
  deadline and proves its full reservation, inflight permit, and exclusive
  authority remain owned until the uncancelled opener proves no claim. The
  guard keeps proving
  that active-MemTable `batch_positions` and WAL statistics are not durable row
  addresses;
- existing `db/manifest/recovery.rs` tests own schema-v11 `StreamFold`
  serialization/version refusal, `Armed`/`EffectsConfirmed` classification,
  exact effect proof, and the rule that `MergedGeneration` is part of the Lance
  transaction rather than a separate manifest participant;
- existing `failpoints.rs` owns post-invocation/lost-watcher ambiguity and
  crash orchestration before sidecar arm (pre-/mid-generation output and
  post-shard-manifest publication) and around every `StreamFold` boundary: arm,
  armed-before-any-effect, table effect,
  achieved-effect confirmation, manifest publication, and sidecar
  finalization. The armed-before-any-effect cell
  (`crash_after_arm_before_any_effect_retires_the_intent_effect_free`) pins the
  `EffectFree` arm: recovery retires the durable intent without advancing the
  base-table pointer or publishing, and the acknowledged generation still folds
  exactly once afterwards. An unreferenced recognized randomized generation subtree,
  complete or partial, remains a retained derived orphan and is never
  adopted/deleted in B1; any other loose state fails closed. Unresolved
  no-effect and effected fold intents both block
  a later put until the recovery barrier resolves them;
- existing `forbidden_apis.rs` keeps B1 crate-private, exact-counts its allowed
  put/fold durable-call sites, and proves no schema, SDK, HTTP, CLI, OpenAPI, or
  generic raw-Lance side door appears;
- a focused feature-gated `memwal_stream_cost.rs` instrument owns warm
  steady-state ack object-store work across graph-history endpoints. It records
  cold claim/reopen/replay separately against retained WAL depth, includes the
  watcher's WAL-plus-in-memory-index completion cost and the post-durability
  epoch probe, reports fold data-scan work versus the one selected generation,
  sweeps accumulated already-merged generation metadata retained in the shard
  manifest, and retains the publisher's known graph-manifest-history term.
  Record local and configured RustFS evidence before making a latency or
  group-commit claim.

Post-containment local evidence recorded on 2026-07-20 by the feature-gated
debug integration binary is deliberately term-separated. A warm
already-claimed clean ack at compacted graph-history depths 8/80 stayed flat at
9 table reads / 219 bytes, 2 table writes / 1,096 bytes, 2 tracked WAL writes,
9 graph-manifest reads, and 21 adapter operations. The 2026-07-19
pre-containment baseline was 6 table reads / 146 bytes, so the explicit epoch
probe adds 3 reads / 73 bytes while remaining flat in graph-history depth.
Nonzero WAL writes and zero generation or graph-manifest writes prove the
detached watcher was measured. The remaining term-separated evidence was
recorded on 2026-07-19: cold claim/replay at retained WAL
depths 1/8/32 used 5/19/67 WAL reads and 3,303/19,218/73,878 aggregate table-read
bytes (4.87/5.01/13.55 ms). Selected folds of 1/4,096 rows read the one fresh
generation and left 601/41,885 bytes of physical generation data; the observed
range-read counters were 4/2 reads and 3,853/2,651 bytes, so those compressed,
cache-sensitive counters are reported rather than misrepresented as decoded-row
work. Retaining 1/4/8 already-merged generations grew the largest retained
shard-manifest payload 52/112/192 bytes and aggregate cold-read work
3,611/4,458/5,770 bytes.
The shared fold authority/publisher term remains intentionally non-flat without
compaction: graph-manifest work at history 8/80 grew from 46 reads / 111,918
bytes to 334 / 1,112,718 bytes (28.3/59.7 ms). The current widest one-batch cell
exercises the real B2 attribution path: 3,742 payload bytes per row charges
33,550,336 logical post-attribution/post-tombstone Arrow bytes, 4,096 below the
32-MiB cap; 3,743 charges 33,558,528, 4,096 above it, and is refused before any
table, manifest, recovery, or MemWAL write. The legal `StoredBatch` estimate is
33,550,376 bytes plus a 32,768-byte Bloom estimate = 33,583,144, below the
1-GiB no-auto-roll threshold. A conservative 8,192-one-row-batch trigger upper
bound is 33,914,880 bytes, with both row and batch counts below 8,193. The
current isolated one-batch RSS pair was 77,725,696 bytes baseline vs
264,683,520 bytes wide (+186,957,824); that whole-process delta includes Arrow,
the mandatory PK index, runtime, and allocator overhead and is neither an Arrow
reservation nor a PK-index-only estimate. B2 therefore admits one resident
writer with a 32-MiB aggregate attributed Arrow reservation. Cheap raw caller
row/byte bounds reject obviously over-cap input before recovery I/O; raw-fit
input then receives exact post-attribution/post-tombstone validation at that
same pre-recovery boundary.
After any recovery/authority prelude, the exact charge is recomputed and
reserved against the same aggregate, then every put follows charge → shared
admission → same-key queue → mode before detached ownership or cold claim and
transfers into the generation without double-counting. Cold replay is the
narrow honest-overlap exception: its exact accounting can temporarily push the
ledger above the nominal cap while previously charged callers drain, but the
fold-only marker refuses all new charges. The oversized-first-batch cell proves
the put invocation is never reached. B2 must measure concurrent residents
before raising either bound.
The configured RustFS figures below are the 2026-07-19 **pre-containment**
baseline because the post-containment run had no configured RustFS environment:
at compacted history 8/80, warm ack stayed at 9 table reads / 146 bytes, 1 WAL
write / 1,096 bytes, 12 graph-manifest reads, and 21 adapter operations
(38.426/49.253 ms). Local uses two tracked writes for its temp-write + atomic
rename while the object-store arm uses one conditional write. The required
post-containment RustFS cell must be rerun before making a current object-store
ack-cost claim. These debug timings still are not a product latency or group-
commit claim, and B2 must re-qualify any higher resident-writer/resource limit
before exposing public admission.

Format tests retain historical source-v7 and source-v8 refusal/rebuild evidence
against CURRENT; the required immediate-predecessor CI cell is v18 ↔ v19. The
recorded v17 ↔ v18 result is historical evidence, while the checked-in v17 seam
is source→CURRENT coverage rather than the current adjacent gate.
The v7 binary exposes no production enrollment route, so its historical-source
cell proves refusal and no in-place adoption but does not claim recovery of
retained physical config-v1 state. The v8-source cell also pins that the new
trusted physical attribution column is absent from logical exports. Focused
behavior covers empty-batch refusal, exact-cap admission,
one-row/one-byte
over-cap refusal before `put_no_wait` with no row/WAL batch, automatic-rollover
refusal, higher-epoch reopen,
restart reconstruction of the exact post-tombstone row/Arrow-byte reservation
(including physical duplicate batches), conservative fold-only routing for
non-empty replay and one flushed-unmerged generation, refusal of active data
beside an unmerged generation, wide/derived embedding expansion
beyond the post-fold byte limit, active-state authoritative cursor validation,
and the deliberate absence of a B1 correction generation.

### RFC-026 Gate R0 coverage ownership (retain-all closure)

Gate R0 extends the existing `memwal_stream_cost.rs` owner rather than creating
a parallel streaming silo. Its revision-pinned source audit records that stock
RC.1 has neither an admission-grade reserve-first complete physical-output
envelope/receipt nor a MemWAL-persisted/enforced durable cross-open cap/receipt
for randomized generation-materialization attempts. Those facts prevent a
finite storage-bound claim, but they are not blockers for the selected
unbounded retain-all profile. A lockfile pin change still fails the tripwire
until the source audit is refreshed.

The strict current-object census recursively lists the graph prefix, classifies
WAL, shard-manifest version/hint, generation data/manifest/transaction/deletion,
PK, Bloom, and maintained-index objects, and compares generation roots with the
latest decoded shard manifest. WAL positions and manifest versions must be
canonical positive 1-based bit-reversed names; generation roots must use
canonical positive decimal generations. Decoded shard identity must match its
directory, generation/path authority must agree and be unique, and every
referenced root must be currently listed. Unknown, malformed, missing, or
duplicate authority fails closed. Except for the mutable best-effort shard
version hint, every earlier path must retain the same class and size. This
proves only current listed objects; incomplete multipart uploads, superseded
provider versions, delete markers, local staging files, and billed storage
remain outside the observation surface.

The success sweep records 1/4/8 referenced roots and approximately 37.4 / 150.6
/ 302.3 thousand local currently listed immutable bytes. The small exact total
varies with generated metadata, so root/reference/path-class-size
retention/monotonicity—not one byte total or content identity—is the assertion.
The referenced-cut retry cell fails after drain but before the fold sidecar,
then proves retry reuses the identical root and publishes the row once. The
legal high-entropy near-cap cell avoids the old compression-friendly repeated
payload and now proves the repaired closure path: acknowledgement retains the
WAL while manifest/table versions remain unchanged; fold charges logical
slices against the 32-MiB cap, takes each scanner emission into dense owned
arrays, materializes one referenced generation, publishes exactly one manifest
and table version, verifies all 8,192 rows plus sampled payloads, and retires the
recovery sidecar. The separate subprocess instrument measured a 286,441,472-byte
(about 273 MiB) paired fold peak-RSS lift under one exclusive fold on the
reference environment. Because `ru_maxrss` is a lifetime high-water mark,
common graph initialization can dominate both subprocesses on another runner,
so the paired lift may be zero or negative. The child still proves its exact
writes, fold, and rows before exiting; the 384-MiB threshold is a one-sided CI
remeasurement tripwire for this implementation shape, not a runtime memory
reservation or allocator cap.

Run the production-neutral Gate-R0 cells with:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost gate_r0_ -- --nocapture
```

The configured-RustFS near-cap twin skips explicitly when
`OMNIGRAPH_S3_TEST_BUCKET` is absent. CI requires its measured evidence line and
positive test polarity, so an explicit skip is a failure there. The fragmented
8,192-single-row shape remains an accounting/RSS fixture rather than a claimed
configured-object-store physical run. A green Gate-R0 run means the private
8,192-row/32-MiB closure and retain-all observations reproduced; it does not
activate a format or public API.

Run the private integration owners with:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost widest_legal_generation_records_no_roll_estimates_and_peak_rss -- --exact --nocapture
```

### RFC-026 Phase B2a coverage ownership (private gate implemented)

B2a reuses the B1 owners instead of creating a second streaming stack. The
shared `tests/helpers/memwal.rs` classifier is the sole strict test authority
for canonical WAL, shard-manifest, generation, PK, Bloom, deletion, and user-
index paths plus decoded reference agreement. `memwal_stream.rs` injects real
provider failures at the Lance table-store boundary and proves complete and
partial orphan output remains retained, non-authoritative, and untouched below
its root through blocked admission, retry, and cold reopen. A parent
`list_with_delimiter` may reveal the orphan common prefix during shard
discovery; no production path may descend into, read, mutate, adopt, or delete
the subtree. The exact configured-RustFS provider cell is non-vacuous in CI.

`memwal_stream_cost.rs` owns the separate retained-history instrument. The
small 1/8 cell runs on every relevant CI change; ignored local and configured-
RustFS cells sweep 1/8/32/128. It records warm acknowledgement, cold replay,
fold, visibility, graph-manifest-store, adapter, advisory object, and
whole-process RSS terms independently, and further partitions table-store
requests into MemWAL, base-table, `_stream_tokens.lance`, and other paths. The
current local 1→8 cell keeps the actual warm-ack MemWAL operation counts flat
at 9 reads and 2 writes while token-authority lookup grows 2→8 reads, so
aggregate tracked reads honestly grow 11→17 and are not claimed history-flat.
It asserts zero IO
against every older retained generation root and zero canonical durable MemWAL
deletes. Lance may remove a losing shard-manifest-CAS `.binpb.tmp.<uuid>`
staging object; the classifier accepts only that exact shape because it never
became authority. The observed LIST bytes, wall time, and RSS are diagnostics,
not quotas, latency SLOs, provider billing, or an isolated MemWAL history slope.

Run the relevant local/CI B2a cells with:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream provider_
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost b2a_
```

Run the decision-scale local sweep explicitly with:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost b2a_retained_history_decision_scale_sweeps_to_128_generations -- --ignored --exact --nocapture
```

### RFC-026 private B2/lifecycle-v3 coverage ownership (implemented)

The private B2/lifecycle-v3 slice extends the existing owners rather than
creating a parallel test stack. `memwal_stream.rs` owns compare-and-chain
retries/conflicts, stale-authority recapture, recovery-covered cold/fold claims,
exact ordinary/drain folds, and empty/non-empty quiesce crash boundaries. Unit
tests in `stream_token.rs`, `token_store.rs`, `stream_lifecycle.rs`, and
`recovery.rs` own canonical digests, ledger-chain integrity, WAL-tail/full-LWW
authentication, claim continuation, direct violation streaming into the
bounded collector, the 8,192-entry/32-MiB detailed canonical-JSON bounds, the
raw-UTF-8 key/token ordering and domain-separated length-framed overflow
digest without an expanded aggregate, and the v14 participant matrices.
Manifest tests own selected token/ledger pointers, strict-block
authenticated-cut relations, and fold attribution.
`forbidden_apis.rs` proves the hidden seam did not add an SDK lifecycle or
CLI/HTTP/OpenAPI ingest side door.

Run the focused private B2-common owners with:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream b2_
cargo test -p omnigraph-engine --features failpoints --test memwal_stream crash_after_
cargo test -p omnigraph-engine stream_token::
cargo test -p omnigraph-engine --features failpoints --lib stream_lifecycle
cargo test -p omnigraph-engine --features failpoints --lib lifecycle_ledger
cargo test -p omnigraph-engine --features failpoints --lib stream_claim_v14
cargo test -p omnigraph-engine --features failpoints --lib stream_fold_v14
cargo test -p omnigraph-engine --test lifecycle public_snapshot_wildcard_omits_protocol_metadata -- --exact
cargo test -p omnigraph-engine --test forbidden_apis
```

RFC-026 §4.7 P1 and 2a own the first public streaming surfaces, so their
evidence extends the existing owners rather than opening a new silo:
`policy_engine_chassis.rs` proves the `stream_manage` gate in both directions
(including that an `admin`-only policy no longer authorizes the enablement
flip after the 2a migration); `omnigraph-policy`'s in-source suite proves both
stream actions are graph-scoped and reject `branch_scope` /
`target_branch_scope`; `omnigraph-cli/src/planes.rs` parses the underscore
policy wire names and their kebab-case compatibility aliases through the real
Clap surface; `lifecycle.rs` owns read-only `stream_status` against a graph
with no lanes; and `memwal_stream.rs` owns it against an enrolled lane,
pinning both the `lifecycle_revision` compare token and the logical
`stream_incarnation_id` that fences every row request from a prior incarnation.
The feature-gated `stream_status` unit test deliberately makes the lifecycle's
diagnostic alias stale and proves status resolves the current registration by
immutable identity. `forbidden_apis.rs` registers `stream_status` as a named
read-only surface and direct-call-count guards constrain its visible durable
calls. That registry is an API-shape guard, not a transitive call-graph purity
proof; the no-manifest/no-sidecar behavior tests supply the composed evidence
that status does not move a lifecycle.

F6b6 adds a second, engine-internal operational-status owner without changing
that manifest-only, nonblocking public projection. Its runtime and checked-
apply failpoint seams are also registered as named read-only surfaces;
`forbidden_apis.rs` exact-counts the one direct Lance `.dataset()` handle read
in `stream_status.rs` and guards the named surfaces' direct durable-call shape.
It does not prove the full composed call graph contains no publication
primitive. The operational-status cells' unchanged manifest version and absent
sidecar/effect assertions own that behavioral claim. The served F7b
route/CLI/OpenAPI projection is covered by the server and CLI owners below; a
direct-SDK checked-runtime transport remains intentionally absent.

The v11 bounded profile-authority slice extends different existing owners.
`omnigraph-control-authority` in-source tests pin lock-derived offline
authority, exact state/declaration/profile binding, and the one live runtime
registration. Stream-profile in-source tests own protocol-v2 strict decoding,
receipt-chain/delegation/continuation/retirement validation, and fail-closed
`DISABLING`/`RETIRED` behavior. Cluster in-source tests own explicit offline confirmation,
state-lock and actor refusal before graph effects, exact profile revision in
the ledger, serving-binding validation, applied-plus-desired graph-policy
authorization, retention of the prior allowing policy when the profile effect
is denied before the state CAS, and quarantine of incomplete enabled
authority. Engine stream-profile tests also pin BranchMerge refusal under
`ENABLED`. F5b0 replaces the old effect-free `OPEN`-lane disable refusal with
a durable `DISABLING` plan: engine and MemWAL cells prove deterministic
node-before-edge serial convergence of `OPEN`, goal-`SEALED`, and adopted
`OPEN_AFTER_FOLD` lanes while preserving the direct-write fence.
The receipt-first replay cell returns the original result to the same actor
after profile movement and rejects a different actor even though actor is
outside the stable operation ID and request digest.
Engine manifest/recovery tests own the v11 stamp and sole
`StreamProfileChange` v13 envelope, including exact token-ledger receipt
classification and atomic terminal selection with the next profile, while
preserving byte-for-byte `protocol_v10` enrollment and `protocol_v12` fold shapes;
`lifecycle.rs` and server route tests prove embedded/direct writers refuse
under an enabled profile while the exact cluster-booted runtime remains the
only ordinary content-write owner. The CLI exercises the offline
`cluster apply --confirm-stream-offline` profile surface and the separate
`cluster stream retire-for-rebuild plan|confirm` terminal-exit handshake and
the stopped/offline `cluster stream block show|correct` DataBlock surface. No
test in the v11 profile-authority slice activated ingress, enrollment, claim,
or ordinary lifecycle mutation. The current v19 hidden lifecycle owners are
`memwal_stream.rs` plus the v14/v15/v16/v17/v18/v19/v20/v21 in-source
recovery/lifecycle suites described above. F7a extends the existing graph-row
owner and adds server/CLI/OpenAPI coverage. F7c adds only graph-wide controls:
`memwal_stream.rs` owns all-OPEN no-op, mixed OPEN/SEALED convergence,
multi-declaration resume, and DRAINING/strict-block preflight-before-effect;
the existing F3b/F3c cells remain the recovery-v16/v17 maintenance authority.
`forbidden_apis.rs` classifies the three doc-hidden graph bridges by their
frozen recovery owners. Server auth/multi-graph/OpenAPI tests own bodyless
routes, graph-scoped `stream_manage`, default deny, aggregate DTOs, and error
redaction. CLI client/plane/output tests own selector-free grammar, exact POST
paths, bearer propagation, aggregate rendering, and status next-action hints.
`system_local.rs::local_cluster_firehose_golden_journey_uses_graph_only_controls`
closes the public composition through the real CLI and server binaries:
cluster-owned enablement, four independently visible mixed node/edge folds,
checked status, stopped/offline disable and re-enable, productive sealed
EnsureIndices and Optimize, convergent graph-wide resume, and a visible
successor ingest. It never names a table, dataset, or lane.

The historical B1/B2a and private B2-common slices added no
parser/server/ingest-CLI tests because they had no public row surface. F7a now
extends the existing server/OpenAPI, CLI, Cedar, shutdown, no-raw-GC, and
provider-failure ownership for graph ingress. F7c activates graph-wide resume
and sealed maintenance without exposing a lane control; per-declaration
enrollment/resume/abort-drain, public rebind, and direct-SDK checked control
remain future work. The selected retain-all profile has no byte/object/file/history
quota; its tests must instead prove that provider exhaustion is loud and cannot
drop an acknowledgement or bypass recovery/manifest visibility. Storage
watermarks and graph-history admission controls belong only to a future B2b
bounded/managed profile. Those tests are a product gate, not incidental B1
scope. The lifecycle
matrix includes `quiesce -> create named branch -> resume`: bounded resume must
recheck branch topology under the closed gates and remain `SEALED`, while a
compatible main-only resume advances the epoch and opens.

F5a extends those owners for orchestration and creates no separate replay
suite. `stream_driver.rs` in-source tests pin immutable identity
classification, node-before-edge cohorts, the carried round-robin cursor, and
preservation of a newer pressure deadline. `memwal_stream.rs` pins exact
checked-runtime start refusal, empty-start effect freedom, timer publication,
cold-reopen discovery without an in-memory pending bit, and cancellation after
detached ownership both before and after physical invocation: the shutdown
profile fence settles the pre-invocation owner, and the post-invocation wake
survives the caller while passive readiness filters no-effect wakes. Existing recovery-v14
cells remain the effect/crash authority. Cluster-server `serve` owns start after
listener bind, prefix cleanup on partial start, and concurrent bounded shutdown
after Axum settles in-flight requests; because this adds no public route it adds
no OpenAPI or transport-parity matrix. F5a adds no format, cross-version,
export, cluster-CLI, or row-transport matrix.

F5b0 extends the same owners without a new format or public surface.
`stream_driver.rs` and `memwal_stream.rs` pin cold resident continuation of an
exact unblocked goal-`SEALED` occurrence, parked `DataBlock` wakeup, checked
runtime shutdown through writer abort plus idle-owner join, and offline
`DISABLING` convergence. The offline cells cover clean `OPEN` folding,
deterministic `OPEN_AFTER_FOLD` adoption, adoption receipt recovery before and
after its token effect, adopted-drain block/correction/retry, row preservation,
and exactly-once terminal selection. Cluster in-source tests pin the exact
observed `DISABLING` ledger revision and ensure a parked or unreadable
continuation blocks schema and dependent query work before schema apply. The
existing v14 lifecycle-receipt validators retain their historical meaning;
`forbidden_apis.rs` classifies the sole `OPEN_AFTER_FOLD` constructor as a
feature-gated test seam rather than a production lifecycle surface.

F5b extends the existing fold/recovery/failpoint/export/cluster/CLI seams with
one bounded deterministic dead-letter object, current `DEAD_LETTERED` authority,
mixed and all-diverted folds, recovery-owned conditional PUT, exact retry while
current, predecessor fencing, ordinary-successor correction, bounded selected-
version list/export, export blocking, and same-format retirement. The landed
integration owner covers marker-only all-diverted advancement, mixed winner
visibility, one object per fold, and list→payload→retire→ordinary-export.
Object-codec unit cells cover one-under/exact/one-over bytes and conditional
create/verify. F6b3 owns the historical exact-selected uncovered-tail current-
token hit/miss and terminal-page measurement; F6b7 now owns the paired test-only
covered/reconciled current-token and profile-receipt measurement and records its
bounded standalone-reconciler NO-GO through 260 exact uncovered fragments for
the uncompacted profile-cycle fixture. F6b4 owns isolated production-size dead-
letter encoding/materialization bytes, time, and peak-RSS evidence. No
standalone production reconciler is scheduled; graph-manifest-compacted or
checked-Optimize-coupled reconciliation and the remaining guardrail matrix stay
open to new evidence. No cell may
assume a chunk chain,
replay mutation, public history pagination, maintained dead-letter inventory,
or ordinary `optimize` coverage of `_stream_tokens`.

F6a extends the existing owners rather than creating a parallel acceptance
stack. `stream_driver.rs` owns the typed failpoints-only process-local snapshot
and proves deterministic pending trigger/backoff ordering plus completion/error
event sequencing. The snapshot is explicitly advisory: pending entries are not a
durable backlog, and stopped run state does not mint or prove checked offline
authority. `memwal_stream.rs` owns that composition in
`candidate_runtime_composes_lazy_prepare_mixed_fold_terminal_correction_and_disable`.
Public durable `StreamStatus` remains manifest-only. F6a itself does not cover
OS-process forced termination, the full node+edge/fairness matrix, cost
evidence, or maintenance/rebind/resume composition. Later F6b2 closes the named
acceptance cells, F6b3 closes the uncovered-tail token-cost harness, and F6b7
closes the paired decision evidence with a bounded standalone-reconciler NO-GO
through 260 exact uncovered fragments for the uncompacted profile-cycle
fixture. At that boundary, public operational-status transport and the
remaining guardrails kept F7 closed; F7b now covers the graph-redacted served
status surface. Graph-manifest-compacted or checked-Optimize-coupled
reconciliation requires fresh evidence.

### RFC-026 F6b1 checked export-cut evidence ownership

F6b1 extends existing owners rather than adding a transport test silo. Control-
authority tests accept only exact terminal `DISABLED | RETIRED` state and pin
the shared process-local registration; cluster/server tests pin binding and boot
installation. `forbidden_apis.rs` keeps `StreamExportCut`
doc-hidden, private-field, move-only, and non-forgeable.

`memwal_stream.rs` proves ambient enrolled ordinary `DISABLED` export refuses
with empty output and the receipt-verified `RETIRED` rebuild bridge remains;
checked ordinary `DISABLED` and `RETIRED` cuts succeed; current `WITHDRAWN` or
`DEAD_LETTERED` authority refuses before output; the sole nonwaiting export cut
owns the root gate exclusively against a second cut, named-branch
delete/recreate, cleanup, and schema apply, then releases on completion/error;
the destructive controls use the shared side and remain mutually concurrent;
managed and unmanaged
`DISABLED | RETIRED` binds are pinned;
and a cut keeps its exact snapshot/catalog/table versions while a writer commits
after capture. The cluster delete owner separately proves the same normalized
root exclusion preserves the graph until release. A later storage error remains
the provider error and releases the slot. F6b1 changes no format or recovery
grammar. F6b1 itself did not cover an HTTP/remote-CLI/OpenAPI handler, bounded
channel, queue reservation, deadline, or stall/disconnect behavior; F6b5 now
owns those cells. F6b3 closes the historical exact-selected uncovered-tail
token instrument; F6b7 closes the paired decision evidence but leaves the
recovery-owned production reconciler and remaining correctness/performance
matrix in F6b/F7.

### RFC-026 implemented F6b5 served-export ownership

F6b5 extends the existing export owners. `export.rs` proves a JSON row wider
than 64 KiB is split into strict independently owned chunks whose concatenation
is byte-identical and valid UTF-8. Blob export keeps its existing value tests;
the implementation explicitly slices Lance descriptor batches to one logical
row before materializing that row's complete Blob-property set.

`export_transport.rs` pins the two-chunk queue, 256-KiB queue-envelope and
2-MiB process budget arithmetic, 250-ms typed saturation/recovery, body-plus-
producer lease ownership, queue backpressure, missing-terminal body error, and
deterministic transfer of the move-only root cut into an unpolled terminal
frame. `data_routes.rs` pins filter refusal before success headers, stalled-body
root-cut exclusion, disconnect release, and direct/served byte parity.
`multi_graph.rs` pins exact checked `DISABLED` served export, refuses an
`ENABLED` writer runtime as export authority, and holds eight graph responses
to prove the ninth receives typed `stream_export_transport_bytes` refusal before
release/retry. `openapi.rs` pins `400 | 401 | 403 | 404 | 409 | 413 | 503`
error schemas and the generated artifact. Existing `memwal_stream.rs` cells
remain the owner for terminal-token refusal, retired provenance, immutable-cut
versioning, and post-start provider errors. The queue reservation covers only
owned transport chunks, not scanner memory, a complete response, or RSS.

### RFC-026 implemented F6b6 operational-status evidence ownership

`memwal_stream.rs::checked_operational_status_reports_one_coherent_read_only_physical_cut`
owns the composed status proof. It admits one row without folding, captures an
`ENABLED` checked-runtime cut, and pins the manifest version, durable lane,
observed/authoritative epoch relationship, exact resident pending rows/bytes/
batches, token counts and bounded sample, honest known-or-explained-unknown
index coverage, non-authoritative driver projection, rebuild blockers, and a
stable repeat observation. It also proves a zero deadline returns typed
`StreamStatusBusy` without publishing a manifest version.
The production path runs full token/base parity and the selected receipt proofs
before the short writer fence; the terminal sample is retained by that same
token scan. Only mutable physical/recovery/manifest witnesses are repeated
under the five-second cut. The immutable preflight has its own 60-second
observation budget, so a slow provider cannot be mislabeled as gate
contention or make status hold ingestion closed for the duration of a graph
scan.
The `omnigraph-storage` in-source cells
`bounded_list_refuses_the_first_excess_matching_entry`,
`bounded_list_counts_direct_and_nested_residue_as_irrelevant`, and
`bounded_list_caps_cumulative_input_anchored_uri_bytes` pin refusal at the
257th matching direct `.json` object, the first excess direct-or-nested
irrelevant object, and the first excess cumulative input-anchored URI byte.
The recovery in-source
`branch_merge_v9_arms_multi_commit_ref_only_and_pointer_slots` cell separately
pins typed per-sidecar-body and cumulative-body enforcement through deliberately
small bounds. Production status combines those owners as a hard envelope of
256 matching direct `.json` sidecars, 256 irrelevant direct-or-nested objects
encountered below the prefix, 4 MiB of cumulative input-anchored URI bytes
across every encountered object, 32 MiB per sidecar body, and 32 MiB of
cumulative bodies. Crossing any one bound must refuse the whole status; the
bounded path never returns a truncated inventory.

`memwal_stream.rs::operational_status_times_out_only_the_blocked_authority_cut_and_cancels_cleanly`
first parks one admitted observer and proves a second observer on the same root
immediately returns `StreamStatusBusy`, then proves completion releases the
slot. It also proves the immutable preflight can succeed while a deterministic
root owner blocks only the short authority cut. The cut returns `StreamStatusBusy` for
`exclusive authority cut` without manifest movement, cancellation retains no
partial gate ownership, and a later status plus ordinary writer both progress.
`memwal_stream.rs::operational_status_terminal_sample_is_the_first_eight_current_keys_and_marks_more`
creates nine current terminal keys and pins the deterministic first eight plus
`terminal_sample_has_more = true` from the fused parity scan.

`memwal_stream.rs::full_operational_status_requires_the_checked_serving_owner`
proves an ambient `ENABLED` handle cannot obtain the checked cut while its
public manifest-only `stream_status` still works. The authority validator also
requires checked served-export ownership for terminal `DISABLED | RETIRED` and
distinct checked cluster-apply status ownership for `DISABLING`.
`disabling_operational_status_uses_the_checked_offline_apply_owner` exercises
that exact owner against a persisted disable plan and pins its flushed
projection as `UnavailableFlushed`.
`operational_status_reports_recovery_before_and_after_its_base_effect` proves
the sidecar is visible and rebuild-blocking both before and after physical
effect; only the latter exact sidecar-owned HEAD movement makes the physical
projection unavailable, and status leaves the sidecar untouched. The existing
`operational_status_refuses_unowned_base_head_movement` pins
`StreamStatusChanged` after exact recovery ownership is removed, while
`operational_status_marks_unopened_durable_wal_as_cold_replay_unavailable`
pins `UnavailableColdReplay` and its rebuild blocker. The retirement fixture
pins both `DISABLED` plus terminal authority as blocked and receipt-verified
`RETIRED` as rebuild-ready; it also proves a real uncovered tail has unavailable
oldest age.
The existing
`flushed_unmerged_generation_resumes_fold_only_and_refuses_a_second_generation`
cell also pins the LWW-projection explanation instead of invented original
row/byte/batch counts. Worker in-source tests prove observation neither creates
a missing/busy writer nor disturbs one,
and prove exact retained accounting for resident admit/fold modes. Token-store
in-source coverage pins exact selected-version fragment coverage. The same
authority-retirement fixture pins the bounded terminal sample emitted by the
fused parity scan.
The existing driver unit suite owns deterministic advisory snapshot ordering;
the snapshot is now production-internal but still not authoritative.

No test may turn missing resident state into zero. Active/replayable cold state
must remain `UnavailableColdReplay`, because exact counting would mutate Lance
cursor state or claim a writer; flushed LWW projection accounting is also
`UnavailableFlushed`. Every sidecar in an accepted bounded inventory must be
reported and block rebuild. A sidecar-explained moved physical HEAD produces explicit physical-unavailable
status, while unexplained movement remains `StreamStatusChanged`. Likewise, a
nonempty uncovered token tail has no exact fragment-creation timestamp and
must report oldest age unavailable.
Recovery in this assertion means an exact canonical-main participant outcome,
not writer kind or table-identity overlap: profile-only recovery, a named-
branch pin, pre-effect/no-effect state, and unrelated later HEAD movement must
all remain `StreamStatusChanged`. Worker-registry tests likewise use the full
identity/enrollment/shard key and cover non-vacant Active, Opening, and Retiring
entries so a stale physical binding cannot disappear behind identity-only
projection.
F7b extends this owner rather than duplicating the physical proof. The engine
test pins the graph-logical projection and no manifest movement. Server and
CLI owners pin the read-authorized HTTP/OpenAPI/remote contract and a forbidden
wire-key allowlist. The ambient SDK still exposes only manifest-only
`stream_status`; the checked method remains a doc-hidden served bridge.

### RFC-026 implemented F6b2 acceptance scope

F6b2 extends the existing server, worker, and `memwal_stream.rs` owners rather
than creating another harness. Green cells cover Unix `SIGTERM` reaching the
same graceful-shutdown path as Ctrl-C; sequential OS-process exit/reopen with
persisted recovery; a finite driver round in which a newly ready node cannot
overtake an edge already captured by the round; and terminal disable →
same-schema physical rebind → re-enable → reopen → explicit resume. The rebind
cell must prove that the fresh binding remains `SEALED` until resume, the old
binding is not re-adopted, and a post-resume ingest/fold publishes exactly
once.

The composed `quiesce → EnsureIndices → Optimize → resume` chain and checked
ordinary `DISABLED` export-cut load into a fresh target are green. The latter
imports logical rows only and proves the target starts `DISABLED` without
lifecycle or token authority. The legacy Mutation/Load/delete, `load_file`, and
corresponding `_as` checked-runtime refusal matrix is green under `ENABLED` and
interrupted `DISABLING`. F6b2 is implemented.

The fairness owner pins bounded preprocessing/inflight → shared root MemWAL
opportunity → shared profile → table admission for resident-producing served
puts. The driver holds the root opportunity exclusively across the frozen
finite round, then takes profile/admission per candidate. Producer and round
permits retain the `MemWalWorkerRegistry` `Arc`, so dropping every graph handle
cannot create a second fence through the weak root map while a permit lives.
Shutdown takes root opportunity exclusive and then profile exclusive, drops
both before joining the driver, and therefore does not deadlock the driver's
final round. F6b8 closes the empty-resume exception: the root producer permit
is a mandatory move-only input to detached install and is embedded in the
exclusive authority retained by failure/retirement. Resume arms an urgent
trigger before transferring it; under the exclusive root fence the finite
round snapshots and retires only exact empty owners under lane-exclusive
authority before its unchanged node-before-edge candidate order. The
integration cells pin driver-first fencing, caller cancellation after
ownership transfer, shutdown waiting, a lower-sorted cold tail publishing in
the same first round without a driver error, and later caller-shaped cold
claim/fold. The in-source split cell pins that an
empty edge owner is removed into housekeeping while the remaining cold nodes
retain ordinary node-first order; productive residents never enter that
prepass. The broader post-claim install/retirement-failure matrix remains later
F6 work.

F6b2 deliberately does **not** accept in-place productive SchemaApply on an
enrolled graph. Schema-change acceptance is a separate checked sealed/retired
export → initialize fresh graph with the desired schema → ordinary load
workflow; physical rebind keeps the accepted schema unchanged. Covered/
reconciled token evidence is owned by F6b7; implemented F7b owns graph-redacted
checked operational-status transport, while direct SDK status and remaining
lifecycle/maintenance parity remain later F6/F7 owners. F6b4 separately closes the
isolated dead-letter envelope evidence.

### RFC-026 implemented F6b3/F6b7 selected-token cost evidence

`memwal_stream_cost.rs` owns both slices without creating another test target
or CI job. F6b3 established the uncovered-tail curve. F6b7 adds one disposable,
failpoints-only exact selected reconciled cut and measures the same fixture on
both sides. Its controlled token-ledger variable is the number of zero-lane
profile cycles before enrollment: each cycle commits an enable receipt and a
terminal-disable receipt but cannot touch MemWAL because no lifecycle exists yet. The
profile transitions also advance graph-manifest history; graph open and offline-
authority setup occur outside the timed first-probe windows. The fixture then
enrolls once and creates one all-diverted current token, so hit/miss result size,
current-token cardinality, and terminal logical ID/page cardinality remain
fixed. Exact page fields and serialized bytes are measured, not asserted equal
across depths.

The normal local regression is:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost f6b7_selected_token_reconciliation_cost_is_measured_at_small_depth -- --exact --nocapture
```

The on-demand 1/8/32/128 local and configured-object-store sweeps are:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost f6b7_selected_token_reconciliation_cost_sweeps_to_128_profile_cycles -- --ignored --exact --nocapture
OMNIGRAPH_F6B7_DECISION_BACKEND=rustfs OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost f6b7_selected_token_reconciliation_cost_sweeps_on_configured_object_store -- --ignored --exact --nocapture
```

Each paired sample reports the exact selected token versions, named-index coverage,
serialized terminal-page bytes, and cumulative advisory whole-process RSS.
Fresh-handle current-token and profile-receipt hit/miss plus the first terminal
page, then warm hit/miss series and repeat terminal pages, report token-read
counts, total table-store read bytes, manifest reads/bytes, adapter operations,
and per-sample warm/repeat elapsed p50 plus
max-of-eight. Graph open
precedes those windows, so “fresh handle” is not a cold-open or cold-provider-
cache claim. Assertions pin one exact hit, one exact miss, one terminal entry,
zero lookup-window writes, zero MemWAL/base reads, zero token-table prefix
listing, and zero payload-object reads for the measured hit/miss/list windows. Coverage is a separate read-only
sample-level probe. The doc-hidden lookup and coverage seams are `failpoints`-
gated and registered in `forbidden_apis.rs` as read-only. The reconciled-cut seam
is separately registered as a test-only writer. It refuses raw-HEAD drift and
stored Lance auto-cleanup, targets only the named token lookup index, requires
one exact `CreateIndex` transaction, proves unchanged fragments/schema/row count,
and selects only that content-identical version. It owns no recovery sidecar and
must never be reused as the production reconciler.

Pre-maintenance authority/content proof, gate and coordinator setup, post-
maintenance fragment/schema/row/coverage proof, and handle refresh are outside
the maintenance window. The measured lower-bound window contains the named
`optimize_indices` effect, exact transaction classification, and graph-manifest
selection. This keeps evidence-only scans from deciding the cost result while
still charging the minimum durable selection work a production owner would
need.

The decision backend is a non-skipped configured-RustFS run. For each recurring
warm current-token or receipt hit/miss, a dimension qualifies only when the
uncovered/reconciled ratio is at least 2× and the measured maintenance cost
amortizes within 1,000 calls. The ratio is specifically token-table read
requests; the break-even numerator conservatively charges all measured token-
table, graph-manifest, and adapter requests in the maintenance window. A GO
requires both token-table read-request and byte terms
to qualify for every operation and remain true at every deeper sample. Latency
is advisory. This test-only maintenance cost omits future
recovery-sidecar/exact-effect overhead, so any crossing is only a candidate
lower bound that authorizes a production-reconciler slice, never its scheduling
threshold. The RustFS-specific disposition is enforced only when
`OMNIGRAPH_F6B7_DECISION_BACKEND=rustfs`; other configured S3-compatible runs
remain semantic and advisory evidence. A bounded no-go is remeasured beyond the
recorded envelope, after a Lance-pin or token-index-grammar change, and before
an implementation couples the refresh to graph-manifest compaction or checked
Optimize.

The non-skipped 2026-08-03 configured-RustFS run produced exact uncovered-
fragment counts `6 / 20 / 68 / 260`. For each of the four warm hit/miss terms,
the token-table read-request ratio was `3.000×`; total maintenance-request
break-even grew
`45 / 136 / 448 / 1,697` calls. The byte ratios were
`19.267× / 10.913× / 4.849× / 2.084×`, with byte break-even
`12 / 20 / 46 / 150` calls. At 260 fragments each eight-call series read
`24` token objects and `742,464` table bytes uncovered versus `8` objects and
`356,200` bytes reconciled. The production-shaped lower-bound maintenance
window itself used `274` token reads, `4` token writes, `2,147,836` table bytes
read, `73,646` table bytes written, `3,111` manifest reads, `4` manifest writes,
`4,856,424` manifest bytes read, and `138,347` manifest bytes written. The byte
dimension still qualifies, but the request dimension does not, so F6b7 records
a bounded **NO-GO through 260 exact uncovered fragments for the uncompacted
profile-cycle fixture**. Profile cycles intentionally grow token and graph-
manifest history together; this is not a universal token-index or history-flat
claim. Remeasure at the first deeper count, whenever the Lance pin or token-
index grammar changes, and before graph-manifest-compacted/checked-Optimize-
coupled reconciliation is proposed.

### RFC-026 implemented F6b4 dead-letter envelope evidence

F6b4 reuses `memwal_stream_cost.rs`, the existing codec unit owner, and the
existing real-fold integration owners; it creates no new test target or CI job.
`bounded_writer_accepts_one_under_and_exact_then_refuses_one_over` remains the
small inclusive-bound regression. The ignored cost cell constructs 8,192
adversarial canonical payloads and runs the exact production encoder and
verifier at 67,108,863, 67,108,864, and 67,108,865 encoded bytes. The one-over
case must be typed `stream_dead_letter_object_encoded_bytes` with actual
`67,108,865` and limit `67,108,864`.

Run the production-size cell explicitly:

```bash
cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost f6b4_dead_letter_object_records_production_envelope_and_peak_rss -- --ignored --exact --nocapture
```

The 2026-08-02 local macOS exact-cap record is: 10,364,432 source-value bytes;
62,301,270 canonical-payload input bytes; 67,108,864 encoded bytes and retained
encoded capacity; 286,280 microseconds encode; and 2,254,424 microseconds
verify. Before the cap-aware reservation fix, the same shape retained an
observed 132,644,864-byte encoded capacity. The paired child recorded
85,557,248 baseline and 231,849,984 exact peak RSS, a 146,292,736-byte lift.
`DEAD_LETTER_RSS_DELTA_REMEASURE_BYTES = 201,326,592` (192 MiB) is a one-sided
remeasurement tripwire, not runtime allocator admission, a quota, or an SLO.
The verifier/exporter retains payloads as raw canonical JSON, and
`verifier_retains_nested_payload_as_raw_canonical_json` pins that a nested list
is not expanded into a `serde_json::Value` tree. The JSON value/schema remains
the same, while the Rust DTO field type and lexical object-member order may
differ from the former `Value` reserialization. The only new test seam is the
doc-hidden, exact-`cfg(feature = "failpoints")`
`failpoint_measure_stream_dead_letter_object_for_test` function and its
`StreamDeadLetterEncodingCostForTest` result; `forbidden_apis.rs` pins both
definitions and both reexport layers.
The ignored `f6b4_dead_letter_object_cost_child` is a subprocess helper and is
not run directly.

The existing real overflow integration owner,
`quiesce_persists_a_stable_data_block_when_dead_letter_object_overflows`, pins
the production classification: a durable operational `DataBlock` lands before
canonical-object creation, base-table movement, or a current-token terminal-
disposition transition. Manifest and token-ledger state may advance to persist
the block; no recovery sidecar or partial fold remains. The existing
`all_diverted_dead_letter_advances_marker_and_accepts_an_ordinary_successor`
owner pins one object on success, marker-only base advancement, and no duplicate
object on retry.

### RFC-026 Phase B2b coverage ownership (specified, inactive)

B2b adds evidence at the boundary where the design depends on Lance, while
leaving all production stream surfaces absent. The two checked-in RC.1 guards
live in `lance_surface_guards.rs` because they characterize substrate behavior:

- `cleanup_old_versions_does_not_reclaim_mem_wal_objects` creates ordinary
  reclaimable table history plus a durable MemWAL entry, runs Lance's generic
  cleanup, and proves ordinary versions are removed while every object name and
  byte in that present `_mem_wal` fixture is unchanged. It does not contain or
  classify an orphan fixture; and
- `mem_wal_deleted_fence_slot_allows_stale_writer_success_on_pinned_lance`
  decodes and deletes the successor's exact empty epoch-2 WAL fence sentinel,
  then proves the stale writer can still report watcher success even though its
  explicit epoch check returns typed `PeerClaimedEpoch`. This is a negative
  regression for the required post-success epoch check, not permission for
  OmniGraph to delete the sentinel.

Run them with:

```bash
cargo test -p omnigraph-engine --test lance_surface_guards cleanup_old_versions_does_not_reclaim_mem_wal_objects -- --exact
cargo test -p omnigraph-engine --test lance_surface_guards mem_wal_deleted_fence_slot_allows_stale_writer_success_on_pinned_lance -- --exact
```

The Lance patch must turn the second guard into a typed fence/unknown outcome
for raw Lance callers and reclamation; B1's wrapper check is not a substitute.
It must also add local plus object-store inspect/plan/execute coverage for stale plans,
whole-cut/cursor pruning, partial deletion, durable attempt/receipt replay,
lost results, authoritative-checkpoint-plus-successor-chain orphan
classification, unknown retention, strong HEAD/GET/LIST-after-PUT/DELETE and
multipart-accounting refusal or Lance-owned complete accounting, and bounded
history checkpointing. Cold-open/quiesce/resume/checkpoint claims also crash at
attempt/sentinel/manifest boundaries: ordinary sentinel-first claims must
preserve the replay cursor and classify the complete tail, while only a proved
whole-cut reclaim may advance it to the new sentinel. Cross-version tests pin
genesis body/pointer/details publication, a new fail-closed MemWAL details kind,
checkpoint-epoch `ReceiptExpired`/`ClaimReceiptExpired`, and no fallback to a
latest hint. OmniGraph's implementation then extends the existing
`memwal_stream.rs`, recovery, failpoint, forbidden-API, cross-version,
server/OpenAPI, CLI-parity, and Cedar owners; it does not create a parallel
streaming test silo. Lance must expose and enforce a source-derived maximum
physical object/byte growth reservation (or equivalent quota), bounded durable
materialization attempts, exact multipart abort/accounting, and reserved
control headroom before the hard admission watermark is real. The future matrix
also pins one reserve-first ledger per physical binding across concurrent
shards, reserve/effect/settlement/reclaim crashes, cold reconstruction, exact
`observed + unmaterialized remainder` arithmetic, bounded terminal/control/body-
orphan history, and emergency reclaim/quiesce/checkpoint progress at the full
row-admission watermark. Versioned, soft-delete, and Object-Lock storage is
refused unless every retained version/delete marker/locked byte is exactly
accounted and eligible versions are permanently removed. A separate
local/RustFS matrix validates that bound across schemas, fragmentation, crashes,
and retries; measurement alone does not establish it. Until all of that is
green, no B2b bounded/managed-reclamation route is active. This future matrix
does not gate the selected unbounded retain-all profile; that profile remains
private because public row admission, lifecycle mutation/correction,
and transport-parity contracts above are still inactive. The checked read-only
operational-status core is implemented internally by F6b6, but its public
transport is not. The Cedar vocabulary, `stream_manage`-gated enablement, and
embedded manifest-only status are already active. A
separate bounded-profile matrix initializes and validates the
manifest-authoritative graph-global `GraphHistoryBudget`, then charges every
manifest-writer class and its pending recovery sidecars through reserve, effect,
lost publication acknowledgement, exact settlement, and effect-free release.
It covers source-bounded physical-growth accounting, cap-too-small/bootstrap and
missing/mismatched-authority refusal, ordinary `GraphRebuildRequired` wire/error
mapping, and two simultaneously blocked streams whose dynamic closure reserves
cannot be spent by ordinary work or by each other. At the aggregate floor, each
stream can still consume its own worst-case block/correction/abort-drain/
requiesce path and reach `SEALED` rebuild. Lifecycle tests also pin monotonic
revision CAS, complete terminal management-receipt replay after later movement,
same-ID/different-digest conflict, stale-revision refusal, and receipt count/byte
closure reserves. Concurrency coverage pins sorted relevant stream admission →
graph history → schema → main → stream token → tables, the history gate held
from sidecar arm through effect/CAS/finalization, a two-publisher fresh-revision
restart, plus release-all/root-restart after late global discovery. The `_mem_wal` watermark is never
asserted to bound whole-root history.

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
- Activated tests: `crates/omnigraph/tests/failpoints.rs`,
  `crates/omnigraph/tests/memwal_stream.rs`,
  `crates/omnigraph/tests/memwal_stream_cost.rs`, and
  `crates/omnigraph-cluster/tests/failpoints.rs` (integration binaries, never
  in-source — the fail registry is process-global). Run the main suites with
  `cargo test -p omnigraph-engine --features failpoints --test failpoints` /
  `cargo test -p omnigraph-cluster --features failpoints --test failpoints`;
  Gate R0's exact command is documented above.

## RustFS / S3 integration

CI runs these S3-backed **correctness** tests against a containerized RustFS
server (`.github/workflows/ci.yml` → `rustfs_integration` job) in two
feature-graph shards. The default shard selects its six test binaries in one
Cargo invocation so dependency features and large test links compile once,
then checks the captured log for every required S3 cell and explicit skip. The
failpoints shard runs its three feature-gated cells together. These remain the
focused local equivalents:

- `cargo test -p omnigraph-engine --test s3_storage` (lifecycle/branching + the e_tag-present CSR topology cache-key reuse test — the path local FS can't reach since its e_tag is `None`)
- `cargo test -p omnigraph-engine --test lance_surface_guards public_physical_ref_token_rejects_s3_same_version_aba -- --exact` (RFC-024's public current-HEAD witness across unchanged reopen plus main/named same-version ABA; the workflow additionally rejects a zero-test/vacuous match)
- `cargo test -p omnigraph-server --test s3` (single-graph serving + config-free `--cluster s3://` boot)
- `cargo test -p omnigraph-cluster --test s3_cluster` (full control-plane lifecycle on the bucket)
- `cargo test -p omnigraph-cli --test system_local local_cli_s3_end_to_end_init_load_read_flow`
- `cargo test -p omnigraph-engine --features failpoints --test failpoints s3_` (recovery-sidecar lifecycle on a real bucket)
- `cargo test -p omnigraph-engine --features failpoints --test memwal_stream s3_provider_shard_manifest_failure_retains_unreferenced_generation -- --exact --nocapture` (RFC-026 B2a provider-boundary failure after complete randomized generation output; CI rejects an explicit skip and proves the root remains non-authoritative and untouched below its root through retry/reopen)
- `cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost gate_r0_widest_generation_closes_and_records_retain_all_growth_on_configured_rustfs -- --exact --nocapture` (RFC-026's configured-object-store 8,192-row/near-32-MiB closure twin; the workflow requires its measured `rustfs` line and positive-test polarity, so an explicit skip is a failure)

Locally, set `OMNIGRAPH_S3_TEST_BUCKET` (and the usual `AWS_*` vars including `AWS_ENDPOINT_URL_S3` for non-AWS) before running. Without those, S3 tests skip gracefully.

RFC-024's S3 **cost** matrix is deliberately not in this correctness job. Run
it on demand with
`OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --test durable_head_lookup_cost s3_durable_head_lookup_matrix_is_correct_and_observable -- --exact --nocapture`.

RFC-025's S3 **cost** matrix is likewise on demand and was not run for the
2026-07-17 local no-go decision:
`OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --test checkpoint_retention_cost s3_checkpoint_retention_matrix_is_exact_and_records_the_current_no_go -- --exact --nocapture`.

RFC-026 B2a's configured-RustFS 1/8/32/128 retained-history **cost** sweep is
also on demand; its LIST totals, wall time, and RSS are advisory and its terms
must remain separate:
`OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost b2a_retained_history_decision_scale_sweeps_to_128_generations_on_configured_rustfs -- --ignored --exact --nocapture`.

RFC-026 Gate E0's configured RustFS cell is both classifier and complete
exact-probe evidence. It owns the positive lost-result/index/empty-shard/reopen
sequence, listing-dependent foreign/malformed/loose/data/cursor/corrupt
negatives, and the six-attempt zero-list shape. The RustFS CI job rejects a
`SKIP` or zero-test match; run it explicitly with
`OMNIGRAPH_S3_TEST_BUCKET=… cargo test -p omnigraph-engine --test memwal_enrollment_gate s3_memwal_enrollment_gate_positive_and_listing_negatives -- --exact --nocapture`.
Outside that configured job the test skips explicitly when the bucket is
absent. CI separately rejects a skipped S3 ABA surface guard.

## Cross-version upgrade (genuine binary format fences)

`crates/omnigraph-cli/tests/crossversion_upgrade.rs` contains genuine-binary
coverage—not the stamp-rewind stand-in in
`db/manifest/tests.rs::sub_current_graph_is_refused_then_rebuilt_via_export_import`.
The long-baseline case mints internal schema v3 with OmniGraph 0.7.2; the v4
case uses 0.8.1. Both are genuine-old-source→CURRENT gates: the archived binary
mints and exports the source, while the binary under test refuses it, rebuilds
a different current-format root, and proves row/vector fidelity plus exact-`id`
PK metadata; the v4 case also pins reverse refusal by the old binary. Historical
cells below have the same shape. They do **not** invoke an archived intermediate
target binary and therefore do not claim an adjacent vN↔vN+1 gate. The current
required adjacent gate is final-v18 → v19; the final-v17 → CURRENT seam below
is historical.

RFC-023 added its then-immediate-predecessor case gated on `OMNIGRAPH_V5_BIN`, built
from the final internal-v5 commit. It mints a genuine SchemaIR-v2 v5 graph,
proves CURRENT refuses it with the `0.9.0-dev` rebuild guidance, exports with v5,
rebuilds under CURRENT, checks row/vector/blob fidelity, exact blob bytes, and
exact-`id` PK metadata, then proves the v5 binary refuses the current root. The
same cell injects a duplicate logical ID into the v5 export: CURRENT rejects the load atomically,
leaves every initialized target table empty, and a canonical re-export proves
the v5 source unchanged. The initialized empty target remains a valid graph;
the operator must not serve it and should discard it after the failed rebuild.

RFC-026 Phase A added the `OMNIGRAPH_V6_BIN` source seam. It mints a genuine
internal-v6 graph, proves CURRENT refuses it before serving, exports with v6,
rebuilds into a different current root, verifies row/vector fidelity and
exact-`id` PK metadata, and proves the v6 binary refuses that current root.
This is old-source format-boundary evidence that a
stamp-rewind test cannot supply.

RFC-026 Phase B1 added the historical `OMNIGRAPH_V7_BIN` source seam. It mints
a genuine internal-v7 graph with no physical enrollment (the v7 binary exposes
no production enrollment route), proves CURRENT refuses it before serving,
exports with v7, rebuilds into a different current-format root, verifies
row/vector fidelity and exact-`id` PK metadata, and proves the v7 binary refuses
the current root. This is old-source format-boundary evidence, not a
retained-enrollment/config-v1 recovery claim. Run it with:

```bash
OMNIGRAPH_V7_BIN=/path/to/final-v7/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v7_and_v7_refuses_current -- --exact --nocapture
```

RFC-026 Phase B2 added the historical `OMNIGRAPH_V8_BIN` source seam. It mints
a genuine internal-v8/config-v2 graph, proves CURRENT refuses it with
`0.9.0-dev` rebuild guidance (no published release ever stamped v5–v8, so the
refusal names the development window rather than a release line), exports with
v8, rebuilds a distinct current-format root, and proves row/vector fidelity
plus exact-`id` PK metadata. The current re-export must
not expose the physical `__omnigraph_stream_v1$` attribution column, and the v8
fixture's ordinary user property `__omnigraph_stream_v1` must retain its value;
the v8 binary must refuse the current root. CI pinned this seam while v9 was
CURRENT; with v10 and later formats it became a historical boundary and joined
the env-gated seams, so it now skips unless `OMNIGRAPH_V8_BIN` points at a build of
the last merged schema-v8 commit
(`725793af83394235bf4b848b6c2c4454ac1f95e1`). Run it locally with:

```bash
OMNIGRAPH_V8_BIN=/path/to/final-v8/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v8_and_v8_refuses_current -- --exact --nocapture
```

RFC-026 §4.7 P1 (the v10 stream-profile format) added the historical
`OMNIGRAPH_V9_BIN` seam. It mints a genuine internal-v9 graph with the pinned
final-v9 binary, proves CURRENT refuses it naming the published `0.9.x` line in
both message slots (`created by omnigraph 0.9.x` and `with an omnigraph 0.9.x
binary` — the exact strings are also pinned in-source by
`migrations.rs::release_names_the_writing_line_for_each_stamp`), exports with
v9, rebuilds a distinct current-format root, proves row/vector fidelity plus
exact-`id` PK metadata, and proves the v9 binary refuses the current root. It remains env-gated
historical evidence. Run it locally with:

```bash
OMNIGRAPH_V9_BIN=/path/to/final-v9/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v9_and_v9_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V10_BIN` seam mints a genuine final-v10 graph with
the matching 0.10.0-dev source build, proves CURRENT refuses it with
source-build/export guidance, exports with v10, rebuilds a distinct
current-format root, and proves row/vector/blob fidelity plus exact-`id` PK
metadata. The old v10 binary must refuse the current root. It remains available
on demand:

```bash
OMNIGRAPH_V10_BIN=/path/to/final-v10/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v10_and_v10_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V11_BIN` seam mints a genuine final-v11 graph from immutable commit
`5589529cd784759c33cb34bdadd10b912955d4bd`, proves CURRENT refuses it with
source-build/export guidance, exports with v11, rebuilds a distinct current-format root,
and proves row/vector/blob fidelity plus exact-`id` PK metadata. The old v11
binary must refuse the current root. This is format evidence only: the fixture is
clean, disabled, and unenrolled, and rebuild deliberately transfers no private
lifecycle, WAL, token, ledger, or receipt authority. Run it locally with:

```bash
OMNIGRAPH_V11_BIN=/path/to/final-v11/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v11_and_v11_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V12_BIN` seam mints a
genuine final-v12 graph from immutable main commit
`f1bdeca60eeb16540de24309eb3483feffbe27c8`, proves CURRENT refuses it with
source-build/export guidance, exports with v12, rebuilds a distinct current-format root,
and proves row/vector/blob fidelity plus exact-`id` PK metadata. The old v12
binary must refuse the current root. The fixture is clean, disabled, and
unenrolled; ordinary export deliberately transfers no private lifecycle, WAL,
token, ledger, or receipt authority. Run it locally with:

```bash
OMNIGRAPH_V12_BIN=/path/to/final-v12/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v12_and_v12_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V13_BIN` seam mints a
genuine final-v13 graph with the matching 0.10.0-dev source build, proves CURRENT
refuses it with source-build/export guidance, exports with v13, rebuilds a
distinct current-format root, and proves row/vector/blob fidelity plus exact-`id` PK
metadata. The old v13 binary must refuse the current root. The fixture is clean,
disabled, and unenrolled; ordinary export deliberately transfers no private
lifecycle, WAL, token, ledger, receipt, or maintenance authority. Run it
locally with:

```bash
OMNIGRAPH_V13_BIN=/path/to/final-v13/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v13_and_v13_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V14_BIN` seam builds the
final v14 binary from immutable merge
`1afc89b8602dba6525a200916fab0fdf3f1eabd6`, mints a genuine v14 graph, proves
CURRENT refuses it with source-build/export guidance, exports with v14, rebuilds
a distinct current-format root, and proves row/vector/blob fidelity plus
exact-`id` PK metadata. The old v14 binary must refuse the current root. The fixture is clean,
disabled, and unenrolled; ordinary export deliberately transfers no private
lifecycle, WAL, token, ledger, receipt, or maintenance authority. Run it
locally with:

```bash
OMNIGRAPH_V14_BIN=/path/to/final-v14/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v14_and_v14_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V15_BIN` seam builds the final v15 binary from immutable merge
`84f3af758947970d16040a987cb1d6ea0f0931e8`, mints a genuine v15 graph, proves
CURRENT refuses it with source-build/export guidance, exports with v15, rebuilds
a distinct current-format root, and proves row/vector/blob fidelity plus
exact-`id` PK metadata. The old v15 binary must refuse the current root. The fixture is clean,
disabled, and unenrolled; ordinary export deliberately transfers no private
lifecycle, WAL, token, ledger, receipt, maintenance, or rebind authority. Run
it locally with:

```bash
OMNIGRAPH_V15_BIN=/path/to/final-v15/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v15_and_v15_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V16_BIN` seam builds the final v16 binary from immutable merge
`ac59c4f6d1d83acc8118c410c39de2bed91f9c15`, mints a genuine v16 graph, proves
CURRENT refuses it with source-build/export guidance, exports with v16, rebuilds
a distinct current-format root, and proves row/vector/blob fidelity plus
exact-`id` PK metadata. The old v16 binary must refuse the current root. The fixture is clean,
disabled, and unenrolled; this strict format fence deliberately transfers no
private lifecycle, WAL, token, ledger, receipt, maintenance, rebind, or
retirement authority. Run it locally with:

```bash
OMNIGRAPH_V16_BIN=/path/to/final-v16/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v16_and_v16_refuses_current -- --exact --nocapture
```

The historical `OMNIGRAPH_V17_BIN` seam builds the final v17 binary from
immutable merge
`41a5990d53238d63d17e139859c66613f9c25867`, mints a genuine v17 graph, proves
CURRENT refuses it with source-build/export guidance, exports with v17, rebuilds
a distinct current-format root, and proves row/vector/blob fidelity plus
exact-`id` PK metadata. The old v17 binary must refuse the current root. The fixture is clean,
disabled, and unenrolled; this strict format fence deliberately transfers no
private lifecycle, WAL, token, ledger, receipt, maintenance, rebind,
retirement, or correction authority. Run it locally with:

```bash
OMNIGRAPH_V17_BIN=/path/to/final-v17/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_refuses_and_rebuilds_genuine_v17_and_v17_refuses_current -- --exact --nocapture
```

The CI-owned adjacent seam uses `OMNIGRAPH_V18_BIN`, built from immutable final-
v18 merge `c7c81b186bed37989fe5ce591baf0965b5102648`. It mints a genuine v18
graph, proves v19 refuses it with source-build/export guidance, rebuilds a
distinct v19 root with row/vector/blob and exact-`id` PK fidelity, and proves
the v18 binary refuses that root. The same exact cell loads
`tests/fixtures/final_v18_retired_main.jsonl`, captured once from that commit's
production retirement exporter after its failpoint-only precondition setup.
It pins `source_internal_schema_version = 18`, one withdrawn token, and the
absence of v19's `dead_lettered_token_count`, then proves v19 imports only the
logical row into a `DISABLED`, unenrolled graph. Freezing the genuine bytes
avoids compiling a second predecessor failpoint graph in every CI run. Run it
locally with:

```bash
OMNIGRAPH_V18_BIN=/path/to/final-v18/omnigraph \
  cargo test -p omnigraph-cli --test crossversion_upgrade --locked \
  current_v19_refuses_and_rebuilds_genuine_v18_and_v18_refuses_v19 -- --exact --nocapture
```

Older cross-version seams remain gated on absolute old-binary paths and skip
gracefully when unset because rebuilding every historical source revision in
default CI would be expensive. A set but invalid path, including
`OMNIGRAPH_V8_BIN`, `OMNIGRAPH_V9_BIN`, `OMNIGRAPH_V10_BIN`,
`OMNIGRAPH_V11_BIN`, `OMNIGRAPH_V12_BIN`, `OMNIGRAPH_V13_BIN`, and
`OMNIGRAPH_V14_BIN`, `OMNIGRAPH_V15_BIN`, `OMNIGRAPH_V16_BIN`, and
`OMNIGRAPH_V17_BIN`, fails loudly rather than making the proof vacuous. The
CI-owned `OMNIGRAPH_V18_BIN` seam has the same set-but-invalid behavior.

## System e2e requirements and suppression

The CLI system tests (`system_local.rs`) spawn the workspace-built `omnigraph` and `omnigraph-server` binaries (cargo provides paths via `CARGO_BIN_EXE_*`), bind ephemeral localhost ports, and use local-FS temp dirs — no external services, no env vars required; they run in the default `cargo test --workspace`. The comprehensive cluster lifecycle e2es (multi-server-restart flows) honor an opt-out for constrained sandboxes: set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` to skip them with a logged message (the same graceful-skip pattern as the S3 gate). Cargo-native filtering also works: `cargo test --test system_local -- --skip local_cluster`.

## OpenAPI drift

`crates/omnigraph-server/tests/openapi.rs` regenerates `openapi.json` and diffs against the checked-in copy. CI always runs the drift check strictly and does not auto-commit generated output. For server/API changes, regenerate locally with `OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi` and commit the result, or the PR's `test_aws_feature` job fails on drift. See [ci.md](ci.md).

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
- `crates/omnigraph/benches/scenarios.rs` with
  `benches/scenarios/rfc023.rs` — the `general-merge-updates` scenario, the
  counterpart to `fenced-adopt-all-new`. That scenario measures the proven
  insert-only shortcut against an untouched target; this one always advances
  `main` on a disjoint key range so the merge is genuinely diverged. The update
  arm (`--source-mode update`) rewrites committed rows, carries no
  insert-absence certificate, and pins the general ordered-diff route. The
  insert arm (`--source-mode insert`) writes all-new rows within the pure-insert
  history-proof limit and records whether the shortcut survives target
  movement. Both arms assert `MergeOutcome::Merged`; the update arm
  additionally asserts ordered cursor scans and zero strict-insert preflights.
  Fresh verification checks the exact
  row count plus deterministic payloads from both the source delta and target
  divergence. `--delta-rows` sets the branch delta independently of `--rows`,
  which is the whole point — the scenario separates delta cost from target
  cost for [#384](https://github.com/ModernRelay/omnigraph/issues/384). Its
  fixture sizes `load()` chunks from the loader's real keyed accounting (a
  JSON array is charged `(dims + 1) * 4` offset bytes **plus** `dims * 4`
  value bytes, roughly `dims * 8`), not from `derive_chunk_plan`'s `dims * 4`
  value-buffer model; the two agree at dims=256 only because the 8,192-row cap
  binds first.
  A decision instrument, not a CI gate: it asserts route where the shape fixes
  it, records the insert-arm discovery, and never asserts a performance
  threshold.
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
- **Apply the same rule to RFC-025.** `checkpoint_retention_cost.rs` keeps live checkpoint count and catalog width fixed while unrelated journal history grows, and counts complete list/show/cleanup-root authority reads. The uncompacted reconciled counters and bounded tail are flat; compacted scan bytes and the 1,000-commit operation boundary are not, so the assertions preserve a no-go. Run the default local matrix with `cargo test -p omnigraph-engine --test checkpoint_retention_cost local_checkpoint_retention_matrix_is_exact_and_records_the_current_no_go -- --exact --nocapture`; run the ignored decision scale with `cargo test -p omnigraph-engine --test checkpoint_retention_cost local_checkpoint_retention_matrix_at_one_thousand_commits -- --ignored --exact --nocapture`. A green test means the known result was reproduced, not that RFC-025 passed Gate 0.
- **Keep RFC-026 Gate E0 reproducible.** The first `checkout_latest`/`IOTracker` instrument was false-green because local `read_dir` escaped tracking; it is not acceptance evidence. The green harness uses the public but guide-hidden `Dataset::has_successor_version` from freshly ABA-verified exact `N`, probes only `N + 1`, then uses exact `N + 1` to reject buried `N + 2`. `AttemptTracker` records before forwarding, including failed/`NotFound` HEADs, and versions 8/80 must retain the identical four-success-HEAD + one-NotFound-HEAD + one-success-GET shape with zero lists. The Unix execute-only `_versions` tripwire must keep exact probing green while latest enumeration fails, and an unreadable exact HEAD must error. Run the 14-substantive-cell local file with `cargo test -p omnigraph-engine --test memwal_enrollment_gate -- --nocapture`; its fifteenth bucket-gated cell logs an explicit skip when unconfigured. Run the exact configured RustFS command above for its positive plus listing-dependent negative matrix. Green E0 authorized only Phase A; Phase A has now activated v7 foundation state, but E0 never authorizes row admission, acknowledgement, or fold.
- **Preserve RFC-026 Gate R0's closure and retain-all evidence.** Its current-object census is useful only inside its stated observation boundary; never label LIST totals as provider-retained or billed bytes, and never turn them into a quota claim. The source audit remains pinned to the exact Lance lockfile revision. The high-entropy cell must continue to reject 3,743 payload bytes per row effect-free at the exact B2-attributed 33,558,528-byte charge, admit 3,742 at 33,550,336 bytes, keep acknowledgement graph-invisible, close through logical-slice charge plus dense per-scanner-batch take, publish exactly once, and preserve every previously listed immutable path/class/size. Use `cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost gate_r0_ -- --nocapture`. Also run `widest_legal_generation_records_no_roll_estimates_and_peak_rss` exactly: its reference-environment paired fold peak-RSS lift was 286,441,472 bytes (about 273 MiB), and 384 MiB is a one-sided remeasurement tripwire for the single-exclusive-fold implementation shape, not a runtime hard allocator limit. A zero or negative paired lift is valid when common initialization dominates the lifetime high-water mark; the child separately asserts that the exact workload completed. A green run proves the private evidence only; it does not activate row admission, lifecycle mutation, full physical status, or transport surfaces.
- **Preserve RFC-026 F6b4's production dead-letter envelope.** Run `cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost f6b4_dead_letter_object_records_production_envelope_and_peak_rss -- --ignored --exact --nocapture`. The fixture must keep 8,192 candidates and exact `67,108,863 / 67,108,864 / 67,108,865` encoded-byte outcomes, retain no more than the 64-MiB encoded cap, verify the exact-cap object, and keep the one-over refusal typed. The accepted 2026-08-02 local macOS reference has a 146,292,736-byte paired peak-RSS lift beneath the 192-MiB one-sided remeasurement tripwire. Keep descriptor verification and payload export on bounded raw canonical JSON so nested lists cannot create a recursive `serde_json::Value` allocation term. Remeasure before changing the object grammar, allocation shape, cap, raw-payload representation, or tripwire; never describe the tripwire as runtime allocator admission, quota, or SLO. Preserve the real overflow integration's operational `DataBlock` publication before canonical-object/base-table/current-token terminal-disposition transition and its no-residual-sidecar/no-partial-fold result.
- **Preserve RFC-026 B2a's no-delete and provider-failure evidence.** Reuse the shared strict MemWAL classifier; do not weaken it with a second path parser. Complete and partial unreferenced generation roots must stay non-authoritative and receive zero subtree reads, writes, deletes, or adoption through retry/reopen. Parent shard discovery may observe their prefix. Canonical durable MemWAL deletion remains zero; only an exact losing shard-manifest-CAS `.binpb.tmp.<uuid>` staging path is allowed. Run `cargo test -p omnigraph-engine --features failpoints --test memwal_stream provider_` and `cargo test -p omnigraph-engine --features failpoints --test memwal_stream_cost b2a_`. Run the ignored 1/8/32/128 local/RustFS sweeps when the retained-history shape or Lance pin changes. Keep every term separate and describe LIST bytes, wall time, and RSS as advisory diagnostics, never as a quota, SLO, provider bill, or isolated WAL slope.
- **Count on the handle that does the reads, not just the one a measured op opens.** Lance's IO-counted tests attach the `IOTracker` to the (warm, cached) dataset and read `incremental_stats()` per request — the tracker MUST be on the handle performing the reads, or warm-handle reads escape. A per-op tracker installed at measure time cannot see reads on a long-lived handle opened earlier (the warm coordinator's `__manifest` handle, reused across writes), so such reads were silently undercounted. Wrap a depth-swept body in `cost_harness` so the manifest tracker is installed before the graph opens and `manifest_reads` is **ground truth** (handle-age-irrelevant). The `version_probes` counter is the freshness-probe *call* count; ground truth additionally reveals that a write's probe does ~3 object-store RPCs (a read's probe is a 0-IO cache hit). `manifest_reads_capture_warm_probe` is the guard that this stays true.
- This is the testing companion to invariant 15 in [docs/dev/invariants.md](invariants.md) (hot-path cost is bounded by work, not history).

When in doubt, re-read [docs/dev/invariants.md](invariants.md) — quality gates apply to every change.
