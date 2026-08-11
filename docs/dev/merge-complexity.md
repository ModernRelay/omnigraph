# Branch-merge algorithmic complexity

**Type:** investigation artifact (complexity + object-store cost)
**Status:** living reference for timeout diagnosis
**Audience:** anyone debugging slow / timed-out `branch_merge`
**Upstream surveyed:** Lance 9.0.0 (`v9.0.0` / `7653c206`)
**Companion docs:** [merge.md](merge.md) (correctness / routes),
[testing.md](testing.md) (`merge_cost.rs`, `merge_fast_forward.rs`),
[lance.md](lance.md)

This document answers: for each function on the merge path, what is its
asymptotic cost in CPU/memory **and** object-store requests? Merges timeout
when the wrong route is selected, when history/fragments are uncompacted, or
when Lance's full-table `merge_insert` join is paid per keyed chunk.

## Notation

| Symbol | Meaning |
|---|---|
| `T` | Catalog / table-key universe for the merge (union of base/source/target) |
| `TΔ` | Tables whose manifest state differs between source and target |
| `N` | Live rows in one table image (base / source / target as labeled) |
| `Δ` | Changed / new / deleted rows for one table |
| `C` | Keyed chunks for one table: `ceil(Δ / 8192)` and also cut by 32 MiB; hard cap **1024** |
| `V` / `K` | Lance versions in a source interval `(base_version, source_version]` |
| `H` | Append-only `__manifest` history depth / fragment count (uncompacted) |
| `F` | Fragments in a data table |
| `I` | Index metadata entries on a data table |
| `B` | Blob payload bytes materialized |
| `D` | Branch ancestry depth (`BranchIdentifier` mapping length) |
| `R` | Commit / publisher retry attempts |

Hard OmniGraph bounds (refuse before sidecar arm when crossed):

- `KEYED_WRITE_MAX_ROWS = 8_192`, `KEYED_WRITE_MAX_BYTES = 32 MiB`
- `MAX_BRANCH_MERGE_DATA_TRANSACTIONS = 1_024` per table
- `PURE_INSERT_HISTORY_MAX_VERSIONS = 1_024`
- Operation-wide retained validation delta: **32 MiB** projected scalars

## Route selection (the dominant complexity switch)

```
branch_merge_as
  └─ branch_merge_impl                    # authority, merge-base, recovery gate
       └─ branch_merge_on_current_target  # classify → validate → arm → publish → CAS
            for each changed table_key (sequential, sorted):
              if source == target: skip
              if source == base:   skip (target already has source)
              if target == base:   classify_adopt  → AdoptPureInserts
                                                   | AdoptWithDelta
                                                   | AdoptSourceState
              else:                stage_streaming_table_merge → RewriteMerged
```

| Route | When | Dominant cost |
|---|---|---|
| **`AdoptPureInserts`** | Target equals merge base **and** complete Lance txn chain proves exact-`id` pure inserts | Scan Δ source rows + `C` join-free commits. **Best path.** |
| **`AdoptWithDelta`** | Target equals base, proof misses, HEAD-advancing | Full sorted scan of **base+source** + per-chunk `MergeInsert` with **`use_index(false)`** (full target join) |
| **`AdoptSourceState`** | Pointer switch / first-touch fork | O(1) pointer or native ref create; optional validation delta still scans base+source |
| **`RewriteMerged`** | Target diverged from base (true three-way) | Full sorted scan of **base+source+target** + eager signatures + same fenced keyed writes |

Untouched tables are skipped after `same_manifest_state` checks. Tables are
processed **sequentially** — no cross-table parallelism.

---

## End-to-end complexity by phase

### A. Outer prepare — `branch_merge_impl`

| Step | Location | Time | Object-store |
|---|---|---|---|
| Policy + schema-apply idle | `merge.rs` `branch_merge_as` | O(1) + Cedar | control reads |
| Recovery sidecar list | `recovery.rs` | O(sidecars) | `LIST` + `GET` per sidecar |
| Source+target authority | `omnigraph.rs` `open_merge_write_txns` | warm: O(1) probe; cold: **O(H + T)** | latest-version probe **or** `__manifest` open+full scan |
| Merge-base over commit vectors | in-memory | O(H) | none |
| Historical base snapshot (diverged) | `ManifestCoordinator::snapshot_at` | **O(H + T log T)** | one historical `__manifest` open + full fold |
| Merge-exclusive mutex | swap active coordinator | serializes concurrent merges | — |

Checked cost ceilings (`merge_cost.rs`): common fast-forward ≤ **3** internal
opens / **3** manifest scans; diverged ≤ **4** / **4**. Scan **count** is
capped; each scan still folds **O(H)** journal rows, so
`manifest_reads` **grows with history** on uncompacted graphs.

### B. Classification — per changed table

#### `try_proven_pure_insert_admit` / `try_proven_pure_insert_adopt`

`merge.rs` ~678. Opens base+source, walks `(base, source]` transaction
metadata, checks pure `Operation::Update` shape + exact-`id` filter + full
nested schema preorder + physical row total.

| | |
|---|---|
| Time | **O(K + fields + plan_scan(Δ))** with `K ≤ 1024` |
| Object-store | 2 dataset opens; **≈ K** version-location probes + manifest/txn GETs (`Dataset::read_transaction_by_version`); then one bounded source-interval scan for chunk planning |
| Miss | Any gap / cleaned txn / unfamiliar op → fall through to `compute_adopt_delta` |

#### `plan_proven_pure_insert_chunks`

Scans `_row_created_at_version ∈ (base, source]` through the bounded
normalizer. **O(Δ + B)**; emits exact chunk row counts for recovery pre-mint.

#### `compute_adopt_delta` (fallback adopt)

`merge.rs` ~1154. Two lazy `OrderedTableCursor`s over base and source.

| | |
|---|---|
| Time | **O(N_base + N_source)** scan + **O(σ_matched)** signatures only for ids on both sides |
| Staging | **O(Δ + B)** into temp Lance datasets (inserts + upserts) + delete-id chunks |
| Memory | Two scanner batches (≤ 8 192 rows / 32 MiB requested) + writer buffer ≤ 32 MiB + delete plan ≤ 32 MiB |

#### `stage_streaming_table_merge` (three-way)

`merge.rs` ~1253. Three **eager**-signature cursors over base, source, target.

| | |
|---|---|
| Time | **O(N_base + N_source + N_target)** + **O(σ_all)** signatures for every row of every side |
| Staging | Only **changed** result rows + deletes (Δ-scoped publish payload) |
| Conflict detection | Signature equality among `{base, source, target}` |

### C. `OrderedTableCursor` and `row_signature`

```397:411:crates/omnigraph/src/exec/merge.rs
            Some(Box::pin(
                crate::table_store::TableStore::scan_stream_bounded(
                    ds,
                    None,
                    None,
                    Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                    true,
                    KEYED_WRITE_MAX_ROWS,
                    KEYED_WRITE_MAX_BYTES,
                )
```

Lance upstream (`scanner.rs`):

- `order_by` docs: *“all data must be read before the first batch can be returned.”*
- Plan inserts DataFusion **`SortExec`** → wall-clock **O(N log N)** sort work
  and high memory/spill risk before any merge progress.
- Unsorted scan would be O(N) I/O; the sort adds the log factor and TTFP delay.

`row_signature` (`merge.rs` ~1484) stringifies **every non-`_row*` column**,
including **Vector embeddings**. Cost per row ≈ O(columns + serialized bytes).
On the three-way path this runs for **every** base/source/target row (eager).
On adopt fallback it runs only for matched ids (lazy).

### D. Validation — `validate_merge_candidates`

Δ-scoped since the post-#5 evaluator. Does **not** open untouched catalog
tables (`merge_cost.rs::merge_validation_is_delta_scoped`).

| Step | Cost |
|---|---|
| Project out Vector/Blob; retain id/src/dst/scalars | O(Δ_projected); **32 MiB** aggregate cap |
| Unique / RI / cardinality probes | Chunked 8 192-key index-backed lookups against target |
| Missing index | Falls back to scans (correctness preserved, cost rises) |

Pure identity-key fast-forward with only `@key` can skip validation entirely
(`proven_fast_forward_needs_no_validation`).

### E. Recovery arm + physical publish

| Step | Cost |
|---|---|
| Pre-mint exact Lance txn identities | O(C) per table; refuse if C > 1024 |
| Sidecar write | 1 object `PUT` |
| Publish loop | **Sequential over tables**, then sequential over chunks |

#### `publish_proven_pure_insert_adopt` (cheap publish)

- Source-interval scan **O(Δ + B)**
- Per chunk: `InsertBuilder` stages fragments (**no target probe / no join**),
  then commit of filtered insertion-only `Update` with v1 certificate
- Object-store ≈ **O(C)** data PUTs + **O(C)** commit CAS pairs
- Explicitly **`strict_insert_preflight_calls == 0`**,
  **`stage_merge_insert_calls == 0`**

#### `publish_adopted_delta` / `publish_rewritten_merge_table` (expensive publish)

For each insert/upsert chunk:

1. `stage_keyed_write` → Lance `MergeInsertBuilder` with **`use_index(false)`**
2. Commit with transparent retries disabled (exact identity)

```1914:1917:crates/omnigraph/src/table_store.rs
        // Beta.21's scalar-index v1 route omits the key filter.  Force v2 and
        // assert the resulting transaction shape below so a future Lance
        // routing change fails closed.
        builder.use_index(false);
```

Lance `create_joined_stream` with `use_index(false)` takes
`create_full_table_joined_stream`: DataFusion **full (or left) hash join of the
entire target table** against the source chunk
(`merge_insert.rs` ~1006–1036). Indexed path is available upstream but OmniGraph
**deliberately disables it** so the transaction carries the exact-`id`
`KeyExistenceFilter` (RFC-023). Consequence:

> **Per keyed chunk cost ≈ O(N_target + chunk)** join/scan work, not O(chunk).**
> With `C = ceil(Δ/8192)` chunks, publish pays roughly **C × O(N_target)**.**

StrictInsert also runs an exact `id IN (...)` preflight per insert chunk
(**O(indexed lookup)** when BTREE covers `id`; else scan).

Deletes: one `DeleteBuilder` per chunk with `id IN (...)`. Best case indexed
lookup O(q + A + Fa); without usable scalar index, **O(N)** filter scan +
O(F) fragment walk for deletion vectors.

`RewriteMerged` additionally builds missing indexes inline after data effects
— can add **O(N log N)** BTree / **O(N·dim)** vector work on that table.

### F. Manifest CAS + cleanup

| Step | Cost |
|---|---|
| Confirm sidecar | Per-effect HEAD/txn probes O(C); rewrite sidecar `PUT` |
| `publish_with_precondition` | Up to **5** retries; each loads full `__manifest` **O(H + T)** |
| `merge_rows` into `__manifest` | One Lance merge-insert (`use_index(false)` again) on pending rows |
| Delete sidecar | 1 `DELETE` |

---

## Lance substrate costs (not in OmniGraph source)

Surveyed from `/tmp/lance-src/lance` at `v9.0.0` and full docs under
`format/table/{transaction,layout,branch_tag,row_id_lineage}` and
`guide/{read_and_write,object_store,performance,observability}`.

| OmniGraph call | Lance API | Complexity | Typical OS pattern |
|---|---|---|---|
| Open pinned table | `DatasetBuilder::with_version` / `checkout_version` | Parse **O(F+I)** | 1 manifest HEAD/GET |
| Open branch HEAD | `with_branch` / `checkout_latest` | + latest resolution | branch JSON GET; latest via hint **O(1–K)** or list **O(V)** |
| Sorted `id` scan | `scan().order_by(id).batch_size(8192).batch_size_bytes(32MiB)` | **O(N log N)** `SortExec`; TTFP after full sort input | Data page GETs for all projected columns |
| Tx history `(base, source]` | `read_transaction_by_version` loop | **O(K)** | ≈ K manifest + ≤ K `_transactions` GETs |
| Stage append | `InsertBuilder::execute_uncommitted` | O(M) rows | Data file PUTs |
| Keyed StrictInsert/Upsert | `MergeInsertBuilder` **`use_index(false)`** | **Full-table join O(N+M)** per execute | Full target scan GETs + write PUTs + deletion PUTs |
| Indexed MergeInsert (unused) | `use_index(true)` when every ON col indexed | O(M + lookups + candidates); but **v1 omits key filter** | Index page GETs |
| Source replay in indexed path | `ReplayExec(Capacity::Unbounded)` | Holds **entire source** in memory | — |
| Delete `id IN (...)` | `DeleteBuilder` | Indexed O(q·lookup+A+Fa); else **O(N+F)** | Scan/index GETs; deletion GET/PUT |
| Commit | `CommitBuilder::execute` | Per attempt **O(K + F + I)**; × retries | `_transactions` PUT + manifest conditional PUT/rename; default timeout **30 min** |
| Branch create | shallow clone + `_refs/branches/*.json` | Metadata O(F+I); **two-phase non-atomic** | Manifest write + branch JSON PUT |
| `find_referenced_version` | `BranchIdentifier` prefix compare | **O(D)** CPU | none |
| Index bitmap update | `fields_for_preserving_frag_bitmap` | Metadata O(I·Fa) | manifest only |
| AIMD object-store throttle | cloud stores | Can stretch wall-clock under S3 503s | Retries look like timeouts |

Docs (`guide/performance`): **manifest-level** ops scale with fragment count;
**fragment-level** ops (update/delete/`merge_insert`) scale with fragment size.
Tens of thousands of fragments are “generally fine”; very high `F` still taxes
every open/commit/plan.

---

## Worked timeout scenarios

### 1. Large diverged three-way on a vector-heavy table

1. `stage_streaming_table_merge` sorts+scans base, source, **and** target
   (`3 × O(N log N)` TTFP + I/O).
2. Eager `row_signature` stringifies embeddings for all three sides.
3. Publish runs `C` full-table `merge_insert` joins against growing target.
4. Optional inline index rebuild.

**Symptom:** merge stuck with high CPU / S3 GET volume before sidecar confirms;
timeouts common when `N` is millions and embeddings are wide.

### 2. Fast-forward that **misses** the pure-insert proof

Missing/cleaned `_transactions`, schema-preorder mismatch, or `K > 1024`
falls to `compute_adopt_delta` + `AdoptWithDelta`. Then each insert chunk still
pays **full-table MergeInsert** (and StrictInsert preflight), even though the
logical change was “all new rows.”

**Symptom:** “simple” branch with only inserts is slow; instrumentation shows
`ordered_cursor_scan_calls > 0`, `stage_merge_insert_calls == C`,
`strict_insert_preflight_calls == C`.

### 3. Uncompacted `__manifest` / deep history

Fixed number of coherent scans (3–4) but each folds **O(H)** fragments. On
RustFS/S3, `manifest_reads` grows with depth (`merge_manifest_cost_grows_with_history`).
Worse when publisher retries after concurrent writers.

**Symptom:** even tiny deltas time out on old graphs; `optimize` on `__manifest`
flattens this (write-path already gates post-compaction flatness).

### 4. Many tables changed, sequential publish

`TΔ` tables × (classify + validate + C commits) with **no parallelism**.
A multi-table review branch pays sum of per-table costs.

### 5. Blob-heavy rows

Declared external sizes are checked pre-arm (aggregate 32 MiB); still **O(B)**
payload reads during staging/publish. One oversized blob fails closed before
effects.

---

## Instrumentation map

`MergeTimingPhase` (`instrumentation.rs`):

`OuterPrepare` → `ProvenInsertHistory` → `ProvenInsertPlanScan` →
`CandidateValidation` → `FinalRevalidation` → `RecoveryArm` →
`PhysicalPublish` (`KeyedStage` / `KeyedCommit`) → `RecoveryConfirm` →
`ManifestPublish` → `RecoveryCleanup` → `OuterRestoreRefresh`

Structural probes to classify the route of a timed-out merge:

| Probe | Proven path | Expensive path |
|---|---|---|
| `ordered_cursor_scan_calls` | 0 | ≥ 2 (adopt) or ≥ 3 (three-way) |
| `stage_fenced_insert_calls` | C | 0 |
| `stage_merge_insert_calls` | 0 | C_insert + C_upsert |
| `strict_insert_preflight_calls` | 0 | C_insert |
| validation projected bytes | often 0 (identity-only FF) | ≤ 32 MiB |

---

## Per-function cheat sheet (OmniGraph)

| Function | File | Time | OS requests (order) |
|---|---|---|---|
| `branch_merge_as` | `exec/merge.rs` | policy + body | — |
| `branch_merge_impl` | `exec/merge.rs` | O(H+T) authority + O(H) base | 3–4 `__manifest` scans typical; LIST sidecars |
| `branch_merge_on_current_target` | `exec/merge.rs` | Σ over TΔ | sequential |
| `classify_adopt` | `exec/merge.rs` | proof or O(N_b+N_s) | opens + scans |
| `try_proven_pure_insert_adopt` | `exec/merge.rs` | O(K+Δ) | O(K) txn reads |
| `compute_adopt_delta` | `exec/merge.rs` | O(N_b+N_s+σ_match) | 2 sorted full scans + temp writes |
| `stage_streaming_table_merge` | `exec/merge.rs` | O(N_b+N_s+N_t+σ_all) | 3 sorted full scans + temp writes |
| `OrderedTableCursor::*` | `exec/merge.rs` | O(N log N) via Lance sort | full projected read |
| `row_signature` | `exec/merge.rs` | O(width) incl. vectors | none |
| `StagedTableWriter::*` | `exec/merge.rs` | O(Δ+B) | temp Lance PUTs; blob GETs |
| `build_merge_changeset` | `exec/merge.rs` | O(Δ_proj) | staged/proven scans |
| `validate_merge_candidates` | `exec/merge.rs` + `validate.rs` | O(Δ + probes) | index-backed filtered scans |
| `plan_merge_transactions` | `exec/merge.rs` | O(C) | none |
| `publish_proven_pure_insert_adopt` | `exec/merge.rs` | O(Δ+C·commit) | C fragment PUTs + C CAS |
| `publish_adopted_delta` | `exec/merge.rs` | **≈ C·O(N_target)** joins | C full-table MergeInsert + deletes |
| `publish_rewritten_merge_table` | `exec/merge.rs` | same + optional index | + index PUTs |
| `commit_keyed_stream_chunks` | `exec/merge.rs` | O(Δ+C·stage/commit) | as above |
| `commit_staged_delete_chunks` | `exec/merge.rs` | O(C_del·delete) | DeleteBuilder each |
| `stage_keyed_write` | `table_store.rs` | **O(N+chunk)** join | full target scan GETs |
| `stage_proven_strict_insert` | `table_store.rs` | O(chunk) | fragment PUTs only |
| `preflight_strict_insert_ids` | `table_store.rs` | O(lookup) | 1 filtered scan |
| `scan_stream_bounded` | `table_store.rs` | scan+sort | page GETs |
| `open_merge_write_txns` | `db/omnigraph.rs` | O(H+T) cold | manifest probes/scans |
| `snapshot_at` / `read_manifest_scan` | `db/manifest*` | O(H+T) | full `__manifest` scan |
| `publish_with_precondition` | `publisher.rs` | O(R·(H+T)) | R scans + 1 merge-insert |
| `write_sidecar` / `confirm_*` / `delete_sidecar` | `recovery.rs` | O(C) confirm | PUT / PUT / DELETE |

---

## What this investigation intentionally deprioritizes

Relative to clarity of the cost model:

- **Efficiency of an alternate join-free general merge** — would need Lance
  API changes or accepting the indexed MergeInsert route without the exact key
  filter (RFC-023 currently forbids that).
- **Cross-table parallel publish** — correctness/recovery envelopes are
  single-sidecar; parallelism is a separate design.

The right near-term levers for timeouts are therefore: keep merges on
**`AdoptPureInserts`**, compact `__manifest`/data tables (`optimize`), avoid
unnecessary three-way divergence, and treat `use_index(false)` × `C` × `N`
as the primary publish tax when the proof path misses.

## Related checked-in instruments

- `crates/omnigraph/tests/merge_cost.rs` — Δ-scoped validation; history-growing manifest reads
- `crates/omnigraph/tests/merge_fast_forward.rs` — proven path probes and bounds
- `crates/omnigraph/benches/scenarios/rfc023.rs` — production vs direct-Lance latency gates
- `MergeTimingPhase` + structural probes in `instrumentation.rs`
