# Plan: Merge latency L1–L3

**Status:** L1 / L2a / L3 implemented; L2b deferred (not an RFC)
**Depends on:** [merge-complexity.md](merge-complexity.md) ranking
**Out of scope here:** L4 cleaned-history admission, L5 Lance indexed key filter,
L6 fragment adopt (RFC-0001)

This plan turns the validated L1–L3 fixes into shippable PRs: what changes,
what must not change, how to test, and which assumptions gate each step.

**Implementation outcome (2026-08-12).** L3 and L1 landed as specified. L2a
uses a closed `KeyedWriteSemantics::KnownPresentUpdate` arm on the existing
staged gateway instead of adding another trait method; this is a narrower
surface than the proposed sealed method. Its Lance indexed-v1 coverage guard
passed locally, so the implementation enables the indexed update-only route
in the same change rather than shipping the planned forced-v2 intermediate.
Missing coverage still falls back to v2, and the shared validator accepts only
v1 `None` / v2 `Some(empty)` filters, `RewriteRows`, exact updated-row stats,
and present affected-row metadata. L2b (splitting true three-way rewrite
output) remains deferred. Detailed “PR-C/C4” steps below are retained as
planning provenance and are superseded by this outcome where they differ.

## Goals

| ID | Goal | Primary tax removed |
|---|---|---|
| **L3** | Three-way merge stops building indexes inline | `O(N log N)` / `O(N·dim)` on `Merged` publish |
| **L1** | StrictInsert never runs a full-table MergeInsert join after absence is proven | `C × O(N_target)` on all StrictInsert surfaces |
| **L2a** | `AdoptWithDelta` changed rows stop using insert-capable full-table Upsert | join tax on known-present updates |
| **L2b** | (follow-up) Split `RewriteMerged` the same way | same for three-way |

## Non-goals

- No public SDK / CLI surface change except docs for index deferral (L3).
- No recovery sidecar schema bump in the first three PRs (keep v9 compatible).
- No `use_index(true)` on insert-bearing keyed writes (L5).
- No fragment adopt (L6), no RFC-027 classifier replacement.
- No cross-table parallel publish.

## Sequencing

```
PR-A  L3  (independent; smallest; docs + failpoint rename)
  │
PR-B  L1  (table_store StrictInsert; Mutation/Load + merge inserts)
  │
PR-C  L2a AdoptWithDelta known-present update adapter
  │
PR-D  L2b RewriteMerged disposition split  (optional follow-up)
```

L3 can land before or parallel with L1. L2a depends on L1 for the new-row
half of adopt; without L1, changed-row work alone leaves insert chunks on the
expensive path.

---

## PR-A — L3: defer three-way inline indexes

### User problem

`MergeOutcome::Merged` still calls `build_indices_on_dataset` inside
`publish_rewritten_merge_table`, so embedding tables pay vector/FTS/BTREE build
on the merge critical path. Adopt/FF already defer (invariant 7). This is the
deny-list inconsistency.

### Change

**Code**

1. `crates/omnigraph/src/exec/merge.rs` — `publish_rewritten_merge_table`
   - Remove Phase 3 `build_indices_on_dataset`.
   - Keep final `table_state` so the published version is the **data** HEAD.
   - Mirror the adopt-path comment: indexes are reconciler-owned.
2. `crates/omnigraph/src/failpoints.rs`
   - Rename `BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_INDEX` →
     `BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM` (or delete and reuse
     `BRANCH_MERGE_POST_EFFECTS_PRE_CONFIRM` if the precise window is redundant).
3. Do **not** shrink `MAX_EFFECT_IDENTITY_SCAN_VERSIONS` (`+2`) yet.
   - Keep accepting one legacy `CreateIndex` tail in
     `prove_branch_merge_multi_commit_effect` for crash residuals from older
     binaries. Comment it as legacy tolerance.

**Docs (same PR)**

- `docs/user/branching/merge.md` — three-way no longer rebuilds inline; run
  `optimize` / `ensure_indices` like FF.
- `docs/dev/merge-complexity.md` — mark L3 done; drop “optional inline index”
  from timeout scenarios.
- `docs/dev/merge.md`, `writes.md`, `execution.md`, `canon.md`,
  `user/reference/constants.md` — clarify CreateIndex headroom is legacy
  recovery tolerance if the constant stays `+2`.
- `docs/dev/invariants.md` truth matrix — BranchMerge joins mutation/load/schema
  apply as no-inline-index writers.

### Tests

| Owner | Change |
|---|---|
| `merge_fast_forward.rs` (or merge + index cell) | New: force `Merged` on a `Vector @index` table; assert `stage_vector_index_calls == 0`, rows visible, coverage pending until `ensure_indices`/`optimize`. Prefer extending near `fast_forward_merge_defers_vector_index_to_reconciler`. |
| `failpoints.rs` | Update rewrite-after-delete scenario for renamed failpoint; convert `branch_merge_armed_index_tail_*` to legacy/synthetic recovery coverage; replace foreign-append-before-index-tail with a no-index-tail confirmation fail-closed cell. |
| `composite_flow.rs` | Rewrite comments that treat post-merge lookup as proof of Phase 3 rebuild — now prove correctness under deferred coverage. |
| Recovery unit tests | Keep “one derived index tail” as **legacy** acceptance. |

### Assumptions

1. Reads remain correct under partial index coverage (invariant 7 — already true).
2. Operators run maintenance after large merges (already documented for FF).
3. Old Armed sidecars with a CreateIndex tail must still recover.

### Success criteria

- `Merged` path: `stage_vector_index_calls == 0` (and no BTREE/FTS stage) under probes.
- Failpoint suite green for rewrite partials without an index phase.
- User doc states FF and Merged both defer indexes.

### Invasiveness

Small: primarily `exec/merge.rs` + failpoints + docs + a few tests. No adapter
or recovery schema change.

---

## PR-B — L1: join-free StrictInsert after preflight

### User problem

`stage_keyed_write(StrictInsert)` already runs `preflight_strict_insert_ids`,
then still pays Lance `MergeInsertBuilder` with `use_index(false)` — a full
target join per chunk. RFC-023 §5.2 already calls that join redundant once
absence is proven. The proven merge path (`stage_proven_strict_insert`) shows
the correct physical shape.

### Design

Keep **two absence authorities**, share **one physical stager**:

| Authority | Who | Evidence |
|---|---|---|
| Runtime preflight | Mutation/Load + general merge StrictInsert | `preflight_strict_insert_ids` at pinned parent |
| History certificate | `AdoptPureInserts` only | `ProvenInsertChunk` / v1 chain |

```
stage_keyed_write(StrictInsert)
  → preflight_strict_insert_ids
  → stage_absence_proven_strict_insert(...)   // NEW private helper

stage_proven_strict_insert(ProvenInsertChunk)
  → validate chunk / target version / stable row ids
  → stage_absence_proven_strict_insert(...)   // SAME helper

stage_keyed_write(Upsert)
  → unchanged MergeInsert use_index(false)
```

Helper responsibilities (lifted from `stage_proven_strict_insert`):

1. Validate batch bounds (8 192 / 32 MiB).
2. Materialize keyed blobs.
3. `InsertBuilder::execute_uncommitted` → fragments.
4. Rewrite operation to insertion-only `Update` with exact-`id`
   `KeyExistenceFilter` and **full nested schema preorder**
   `fields_for_preserving_frag_bitmap`.
5. Preserve `strict_source_ids` on `StagedWrite`.
6. Record `stage_fenced_insert` probe — **not** `stage_merge_insert`.

Do **not** mint `ProvenInsertChunk` from Mutation/Load (forbidden_apis /
capability narrowing).

**Correction (2026-08-12 review):** general StrictInsert **already** mints v1
mandatorily — `stage_keyed_write` calls `certify_insert_absence` on every
StrictInsert (`table_store.rs` ~1543), certifying the forced-v2 MergeInsert
transaction. L1 must **preserve** that: the shared helper builds the same
filter-bearing insertion-only `Update` and passes the same validator, so
certification comes for free. Mutation/Load-written histories therefore
already compose into the merge proof chain today; L4's gap is only histories
whose `_transactions` were cleaned, not histories that never carried v1.

### Call-site impact

- Mutation finalize / Load Append: automatic via `stage_keyed_write`.
- `publish_adopted_delta` insert chunks: automatic via
  `KeyedWriteSemantics::StrictInsert`.
- `publish_proven_pure_insert_adopt`: stays on `ProvenStrictInsert` enum arm;
  shares helper only.

### Recovery

No sidecar format change. Staged transaction must remain:

- insertion-only `Operation::Update`
- exact-`id` filter
- bindable `StagedTransactionIdentity`
- `conflict_retries(0)` at commit

### Tests (test-first)

1. **Red first** in `src/table_store/staged_tests.rs`:
   - After `stage_keyed_write(..., StrictInsert)`, assert
     `strict_insert_preflight_calls == 1`,
     `stage_fenced_insert_calls == 1`,
     `stage_merge_insert_calls == 0`.
   - Confirm fails on current code (merge-insert still counted).
2. Extend `write_cost.rs` / `measure_with_staged` fitness asserts for
   single-insert: fenced insert once, zero bare merge-insert.
3. `consistency.rs` Append key-conflict: still typed `KeyConflict`, no publish;
   probes show preflight conflict without fenced stage.
4. `merge_fast_forward.rs`: proven path expectations unchanged; if an
   `AdoptWithDelta` inserts-only cell exists, assert fenced inserts and zero
   merge-insert.
5. `forbidden_apis.rs` / `lance_surface_guards.rs`: run unchanged; must not
   broaden `ProvenInsertChunk` mint sites.
6. Failpoints that arm StrictInsert chunks: still roll forward/back correctly.

### Instrumentation contract

| Path | preflight | fenced insert | merge insert |
|---|---|---|---|
| StrictInsert success | ≥1 | = chunks | **0** |
| StrictInsert KeyConflict | ≥1 | 0 | 0 |
| Upsert | 0 | 0 | = chunks |
| Proven pure-insert merge | 0 | = chunks | 0 |

### Assumptions

1. Preflight parent == staged `read_version`.
2. Manually minted `KeyExistenceFilter` OCC ≡ forced-v2 MergeInsert filter class
   (already proven by `stage_proven_strict_insert` + surface guards).
3. Full nested preorder bitmap required (silent index over-claim otherwise).
4. Upsert untouched.

### Success criteria

- Every StrictInsert probe path shows `stage_merge_insert_calls == 0`.
- KeyConflict / recovery / blob / bound tests remain green.
- No new public trait methods unless unavoidable.

### Invasiveness

Medium-small: centered on `table_store.rs` + probe assertion updates. One PR
covers Mutation/Load and merge inserts by construction.

---

## PR-C — L2a: disposition-correct publish for `AdoptWithDelta`

### User problem

`compute_adopt_delta` already partitions new / changed / deleted, but
`publish_adopted_delta` sends changed rows through Upsert MergeInsert
(`use_index(false)`), re-joining the entire target to rediscover presence the
classifier already proved.

### Substrate blocker (validated)

Lance `UpdateBuilder` exposes only **`execute()`** (inline commit + retries).
There is **no** `execute_uncommitted` sibling (unlike `DeleteBuilder` /
`InsertBuilder` / `MergeInsertBuilder`). BranchMerge requires staged exact
identities under recovery-v9, so raw `UpdateBuilder::execute` is **rejected**.

### Chosen design (correctness-preserving without waiting on Lance)

Add a sealed adapter:

```text
stage_known_present_exact_id_update(ds, table_key, batch) -> StagedWrite
```

Physical plan:

1. Require exact non-null `id` PK metadata (same fence as keyed writes).
2. Bound batch (8 192 / 32 MiB); materialize blobs like keyed writes.
3. Stage via `MergeInsertBuilder` with:
   - `when_matched(UpdateAll)`
   - `when_not_matched(DoNothing)`  // insert-capable path closed
   - **`use_index(false)` (forced v2) in the first PR** — the indexed v1 route
     is a separate, evidence-gated follow-up (frag-bitmap hazard below)
   - `conflict_retries(0)`
4. Fail closed unless stats and shape prove **exact** update of every source id:
   - `num_updated_rows == batch.num_rows()`
   - `num_inserted_rows == 0`, `num_deleted_rows == 0`,
     `num_skipped_duplicates == 0` (verified: `MergeStats` has no
     "not-matched skipped" counter, so an absent id is caught by
     `num_updated_rows` falling short — fail-closed holds)
   - `update_mode == RewriteRows` and `affected_rows.is_some()`
5. Filter expectation (verified against Lance 9.0.0 + surface guards): the
   forced-v2 matched-only route emits **`Some(empty KeyExistenceFilter)`**;
   the v1 route emits `None`. Accept `Some(empty)` or `None`; reject any
   **non-empty** filter — this txn must insert nothing. Presence fencing is
   the pre-arm read-set + stats check plus `affected_rows` row-level OCC, not
   a Bloom of inserts.
6. Register in `forbidden_apis.rs`.

**Why not Delete+Insert for changed rows?** Would mint new
`_row_created_at_version` and break merge’s intentional preservation of
creation lineage (explicitly why three-way/adopt use Upsert today).

**Why an update-only MergeInsert does not reopen RFC-023's conflict class:**
insert-bearing filtering is unnecessary when `WhenNotMatched::DoNothing` and
stats forbid inserts; row-level OCC is carried by `affected_rows`.

### Frag-bitmap hazard — why the indexed v1 route is gated (review finding)

The v1 full-schema update arm (Lance `merge_insert.rs` ~2185) sets
`fields_for_preserving_frag_bitmap` to **top-level field ids only**. Lance's
manifest builder (`register_pure_rewrite_rows_update_frags_in_indices`,
`transaction.rs` ~2666) **extends** an index's `fragment_bitmap` over the
rewritten fragments whenever the index's field ids are *not* in that list. An
index keyed on a **nested** field id (e.g. a vector column's FixedSizeList
child) would then falsely claim coverage of fragments whose values were just
rewritten — the "silent missing/stale query rows" hazard OmniGraph's own
insert-path comment warns about (`table_store.rs` ~1666), and the reason
`certify_insert_absence` demands the full nested preorder.

Consequences:

- **First PR ships forced-v2 update-only.** Its win over today's Upsert is
  closing the insert-capable path + fail-closed stats — a correctness
  narrowing, **not yet** a join-cost win (v2 still full-joins).
- **The indexed v1 latency win is evidence-gated**: a new
  `lance_surface_guards` cell must prove that OmniGraph-built index metadata
  references only top-level field ids and that a v1 UpdateAll claims no new
  fragments. **2026-08-12 empirical result (see Review ledger): this holds on
  Lance 9.0.0** — BTREE/FTS/vector metadata are all top-level (an FSL child
  has no field id), and the direct v1 UpdateAll test showed zero
  new-fragment claims. The gate is now only "land the checked-in guard
  cell", not "wait for upstream".
- Original ranking impact is correspondingly softened: the indexed follow-up
  is expected to pass its gate; L4 overtakes it only if the guard cell
  surfaces a schema shape (e.g. an index on a `List` child) we do not build
  today.

Also verified: Lance auto-falls back from the indexed route when coverage is
missing, so once gated-on, the adapter must accept both v1 and v2 shapes. The
v1 **partial-schema** arm (`RewriteColumns`) returns `affected_rows: None` —
structurally unreachable for OmniGraph's full-row staged images and rejected
by check 4.

### `publish_adopted_delta` after L1+L2a

```text
inserts  → StrictInsert (L1 join-free)
upserts  → stage_known_present_exact_id_update  // NEW
deletes  → stage_delete (unchanged)
```

### Read-set before arm

Already largely present; make the L2 contract explicit in comments/tests:

- target HEAD/incarnation equals classification baseline;
- changed ids still present; new ids still absent;
- failure before arm → clean abort; after arm → `RecoveryRequired`.

### Tests

| Owner | Focus |
|---|---|
| `staged_tests.rs` | Transaction shape: zero inserts in stats, filter `Some(empty)`/`None` (never non-empty), `RewriteRows` + affected rows present, `conflict_retries(0)`, bounds. |
| `merge_fast_forward.rs` / adopt cells | Mixed new+changed+deleted: probe counts for fenced insert / known-present update / delete; merge-insert upsert calls for adopt changed rows → 0 (or only degraded no-index path still counted under a distinct probe if we instrument separately). |
| `merge_truth_table.rs` | Semantic oracle unchanged for adopt-capable cells. |
| `failpoints.rs` | Crash between insert/update/delete phases; recovery roll-back/forward. |

Prefer a dedicated probe (`stage_known_present_update_calls`) so cost tests can
distinguish “bad Upsert” from “good update-only MergeInsert.”

### Assumptions

1. L1 already landed for insert chunks.
2. Classification partitions are disjoint (true today).
3. First PR is forced-v2: still a full join, but **cannot insert**; stats fail
   closed if any expected id is missing. The join-cost win arrives only with
   the gated indexed follow-up.
4. Full-row images in the staged upsert table (already true) — this is also
   what keeps the v1 `RewriteColumns` partial-schema arm unreachable.
5. Update-vs-update / delete-vs-update OCC is carried by `affected_rows`
   overlap in Lance's rebase (verified: both v1 full-schema and v2 arms return
   `Some(affected_rows)`).

### Success criteria

- Adopt changed-row path never uses `WhenNotMatched::InsertAll`.
- Probe: zero general Upsert merge-insert on adopt; known-present update count
  matches chunk plan.
- Truth table + failpoints green.

### Invasiveness

Medium: new sealed adapter + merge publish wiring + probes + failpoints.
Still adopt-only. Note the revised value proposition: the first PR is a
correctness narrowing; the latency win depends on the evidence-gated indexed
follow-up.

### Exit ramp if Lance adds `UpdateBuilder::execute_uncommitted`

Replace the MergeInsert body of the sealed adapter with staged Update while
keeping the same `StagedWrite` / stats / recovery contract. Call sites unchanged.

---

## PR-D — L2b: split `RewriteMerged` (follow-up)

### Change

In `stage_streaming_table_merge`, maintain separate insert vs update writers
(mirror `compute_adopt_delta`) instead of one Upsert delta:

- selection present, target absent → insert writer
- selection present, target present, signatures differ → update writer
- selection absent, target present → delete chunks (already)

`publish_rewritten_merge_table` then calls L1 + L2a adapters + deletes.
Keep index deferral from L3.

### Extra risk

Three-way conflict continuum and recovery chain planning must count both
insert and update chunk vectors. Extend `plan_merge_transactions` accordingly.

Ship only after L2a is proven on adopt.

---

## Cross-cutting checklist (every PR)

1. Update [merge-complexity.md](merge-complexity.md) status for the landed letter.
2. Run focused suites before claiming done:
   - L3: `merge_fast_forward`, `failpoints` (`branch_merge_`), `composite_flow`, `maintenance`
   - L1: `staged_tests`, `write_cost`, `consistency`, `writes`, `merge_fast_forward`, `forbidden_apis`
   - L2a: above + `merge_truth_table`, adopt failpoints
3. `cargo fmt --all --check` and clippy workspace (CI gates).
4. Small commits: test assertion red → implementation green where applicable
   (L1 especially).

## Deferred / rejected in this plan

| Item | Why |
|---|---|
| Shrinking recovery scan ceiling to `+1` | Needs schema/generation discriminator; do after fleet drains old sidecars |
| Minting v1 from general StrictInsert | Useful (feeds L4) but expands Mutation/Load history contract; optional after L1 |
| Cheaper `row_signature` only | Symptomatic for publish cost |
| `use_index(true)` on StrictInsert/Upsert insert path | Still `inserted_rows_filter: None` on Lance 9.0.0 / main |
| Fragment adopt | RFC-0001; separate track |

## Suggested first slice

Start with **PR-A (L3)**: independent, user-doc clear, removes the worst
embedding-table cliff on true three-way merges, and unblocks cleaner reasoning
about L1/L2 publish costs without an index-tail confounder in failpoints.

---

## Implementation spec (commit-level)

Repo rules applied: each commit compiles and passes tests (rule 11); docs land
in the same PR (rule 1); `forbidden_apis.rs` occurrence counts are updated in
the same commit as the code they pin. Run per PR before pushing:
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked --
-D warnings -W clippy::dbg_macro`, and the canonical
`cargo test --workspace --locked --features
omnigraph-engine/failpoints,omnigraph-cluster/failpoints` (or at minimum the
focused suites listed per PR).

### PR-A — L3 commits

**A1 — mechanical failpoint rename (pure refactor).**
`failpoints.rs:86` `BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_INDEX` →
`BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM`; update the two consumers
(`exec/merge.rs:2474`, `tests/failpoints.rs:8956`). No behavior change;
`failpoint_names_guard` keeps the compile-checked const wiring honest.

**A2 — defer the index phase + test updates (one commit; old assertions would
fail if split).**

- `exec/merge.rs` `publish_rewritten_merge_table` (~2467–2493): delete the
  `row_count` probe + `build_indices_on_dataset` call; replace the Phase 3
  comment with the adopt-path posture (indexes reconciler-owned, invariant 7).
  Keep the final `table_state` read.
- New regression test (prefer `merge_fast_forward.rs`, near
  `fast_forward_merge_defers_vector_index_to_reconciler`):
  `merged_outcome_defers_index_build_to_reconciler` — diverge both branches on
  one table to force `MergeOutcome::Merged`; assert via
  `SnapshotDataset::index_coverage` (the deterministic signal — BTREE/FTS
  coverage absent immediately post-merge, present after `ensure_indices`), and
  `with_merge_write_probes` `stage_vector_index_calls() == 0`. Do **not** rely
  on the vector probe alone: small fixtures leave Vector untrainable, so the
  inline build may already skip it.
- `tests/failpoints.rs`: `branch_merge_armed_index_tail_rolls_back_after_
  exact_transaction_prefix` (~8801) becomes a legacy-sidecar recovery cell
  (synthetic tail, comment says legacy tolerance);
  `branch_merge_confirmation_rejects_foreign_append_before_index_tail`
  (~8904) becomes a no-index-tail foreign-append fail-closed cell;
  `MergeScenario::Rewrite` comments drop the index phase.
- `composite_flow.rs` (~761–790): rewrite comments — post-merge lookups prove
  correctness under deferred coverage, not an inline rebuild.
- Recovery: no constant change. `MAX_EFFECT_IDENTITY_SCAN_VERSIONS` stays
  `+2`; comment `prove_branch_merge_multi_commit_effect`'s one-CreateIndex-tail
  acceptance as legacy tolerance
  (`recovery.rs` test `branch_merge_v4_accepts_only_one_derived_index_tail`
  keeps guarding it).

**A3 — docs.** `docs/user/branching/merge.md` (~52–69: three-way now defers
like FF; also state plainly: FTS **absence** errors until first
`ensure_indices`/`optimize`, vector brute-forces, FTS **coverage gaps**
flat-search — the verified matrix), `docs/dev/merge.md` (1,026-version
explanation → legacy tail), `docs/dev/merge-complexity.md` (L3 done; drop
"optional inline index" row/scenario), `writes.md` / `execution.md` /
`canon.md` / `constants.md` headroom wording, `invariants.md` truth matrix
(BranchMerge joins the no-inline-index writers).

Focused suites: `merge_fast_forward`, `failpoints branch_merge_`,
`composite_flow`, `maintenance`, `recovery` in-source cells.

### PR-B — L1 commits

**B1 — extract the shared stager (pure refactor, no route change).**
In `table_store.rs`, move the physical body of `stage_proven_strict_insert`
(bounds → blob materialization → `InsertBuilder::execute_uncommitted` →
fragment-id/row-id assignment → Append→filtered-`Update` conversion with
`KeyExistenceFilterBuilder` over exact source ids + full `schema_preorder_
field_ids` → `record_stage_fenced_insert`) into a private helper:

```rust
async fn stage_absence_proven_strict_insert(
    &self,
    ds: Dataset,                        // pinned parent; read_version source
    table_key: &str,
    batch: RecordBatch,                 // bounds-checked, blob-prepared
    source_ids: Vec<String>,
    id_field_id: i32,
    expected_schema_preorder_ids: &[u32],
    context: &'static str,              // error/em text ("stage_keyed_write" | "stage_proven_strict_insert chunk N")
) -> Result<StagedWrite>
```

`stage_proven_strict_insert` keeps its `ProvenInsertChunk` validation
(version pin, stable-row-ids, chunk_index) and delegates. Move the
stable-row-ids requirement **into the helper** (fail loudly; every v6 table
has them). No new `InsertBuilder::new(` occurrence — the literal moves with
the code, so the `forbidden_apis` count `("table_store.rs",
"InsertBuilder::new(", 3, ..)` is unchanged. `ProvenInsertChunk` minting stays
merge-only. Green on the existing suite proves the refactor.

**B2 — route general StrictInsert through the helper (+ probe assertions, one
commit).**

- `stage_keyed_write` (~1517): after `preflight_strict_insert_ids`, the
  StrictInsert arm calls the helper instead of
  `stage_keyed_write_from_stream`; then the existing mandatory
  `certify_insert_absence` runs against the helper's transaction (same
  validator, same v1 property) and `set_strict_source_ids` is kept. The
  Upsert arm is untouched (still forced-v2 MergeInsert +
  `validate_exact_id_filter` + optional all-new certification).
- Delete `validate_strict_insert_merge_stats`'s StrictInsert call site (no
  MergeInsert stats exist on this path; `certify_insert_absence` is the
  shape/filter proof). Keep the fn if the test-only entry points still use it;
  otherwise remove it in the same commit.
- Probe deltas in the same commit:
  - `staged_tests.rs::keyed_strict_insert_preflights_typed_conflict_without_
    changing_mode` + a new assertion block: successful
    `stage_keyed_write(StrictInsert)` shows `strict_insert_preflight == 1`,
    `stage_fenced_insert == 1`, `stage_merge_insert == 0`.
  - `write_cost.rs::keyed_insert_routes_through_fenced_adapter_only` (~361):
    `staged.stage_merge_insert` `1 → 0`, add
    `staged.stage_fenced_insert == 1`; keep `stage_append == 0` and the
    vector-build guard.
  - `consistency.rs` Append strict-insert cells: unchanged outcomes (typed
    `KeyConflict`, ceilings, no partial publish) — extend one cell with the
    probe triple to pin the route.
  - `merge_fast_forward.rs`: existing proven-route assertions unchanged;
    `AdoptWithDelta` insert cells now expect fenced inserts, zero
    merge-insert.
- `forbidden_apis.rs`: `.execute_uncommitted(` count in `table_store.rs` may
  shift if the StrictInsert MergeInsert call disappears from the general arm
  (`stage_keyed_write_from_stream` remains for Upsert — expect count
  unchanged; adjust if the refactor consolidates).

**B3 — docs.** `docs/dev/merge.md` strategy note (general StrictInsert no
longer pays a target merge join), `docs/dev/merge-complexity.md` (L1 done;
update the per-function table rows for `stage_keyed_write`), RFC-023 is
historical — no edit; `AGENTS.md` capability-matrix keyed-writes cell gets the
one-line update.

Focused suites: `staged_tests` (in-source), `writes`, `consistency`,
`write_cost`, `merge_fast_forward`, `forbidden_apis`, `failpoints` mutation +
adopt cells, `benchmark_scenario_contract`.

### PR-C — L2a commits

**C0 — substrate guard cell (can land independently, unblocks the indexed
follow-up).** New `lance_surface_guards.rs` cell
`v1_update_claims_no_new_fragments_for_omnigraph_index_shapes`: fixture with
`id Utf8 PK`, free-text `String`, orderable scalar, **one list-typed
property**, and an FSL vector; build BTREE + inverted + vector; run
`UpdateAll`+`DoNothing` with default `use_index` (v1); assert
`inserted_rows_filter.is_none()`, `affected_rows.is_some()`,
`update_mode == RewriteRows`, and **no index fragment_bitmap contains any new
fragment id**; assert every index's `fields` are top-level ids. This checks in
the 2026-08-12 empirical result (review ledger).

**C1 — sealed adapter + unit cells (one commit).**
`table_store.rs`:

```rust
pub(crate) async fn stage_known_present_exact_id_update(
    &self,
    ds: Dataset,
    table_key: &str,
    batch: RecordBatch,   // full-row images, ≤ 8_192 rows / 32 MiB
) -> Result<StagedWrite>
```

Body: bounds + `exact_id_primary_key_field_id` + `validate_keyed_write_batch_
ids` + `prepare_keyed_write_batch`; `MergeInsertBuilder` with `UpdateAll` /
`DoNothing` / `use_index(false)` / `conflict_retries(0)` /
`SourceDedupeBehavior::FirstSeen`; fail closed per the revised checks
(stats exact-update, `RewriteRows`, `affected_rows.is_some()`, filter
`Some(empty)`-or-`None`, never non-empty). New probe
`record_stage_known_present_update` in `instrumentation.rs` (+
`MergeWriteProbes` counter + `StagedCounts` field in `helpers/cost.rs`).
Registry updates in the same commit: `forbidden_apis.rs`
`("table_store.rs", "MergeInsertBuilder::try_new(", 1 → 2, ..)` and
`.execute_uncommitted(` count +1; add the method name to both sealed-adapter
allowlists (~530, ~566). Unit cells in `staged_tests.rs` mirror
`keyed_upsert_forces_filter_route_and_preserves_conflict_metadata` plus an
absent-id fail-closed cell.

**C2 — wire the adopt publish (one commit).**
`exec/merge.rs`: add `KeyedChunkStage::KnownPresentUpdate`;
`publish_adopted_delta` upsert phase switches
`KeyedWriteSemantics::Upsert` → the new stage kind (the
`commit_keyed_stream_chunks` match arm calls the new adapter).
`plan_merge_transactions` counts are unchanged (same chunk vectors).
Read-set comments state the L2 contract explicitly (changed ids
known-present at the classification baseline; post-arm divergence is
`RecoveryRequired`). Test deltas: `merge_fast_forward` adopt cells assert
`stage_known_present_update == C_upsert`, general `stage_merge_insert == 0`
on adopt; `merge_truth_table` unchanged semantics; `failpoints` adopt
crash-window cells re-run (identities/chain unchanged, so expect green
without edits — verify).

**C3 — docs.** `merge.md` + `merge-complexity.md` (L2a status; forced-v2
first, indexed follow-up gated on C0's cell), `AGENTS.md` matrix line if the
capability wording changes.

**C4 (separate follow-up PR, after C0 is green in CI):** flip the adapter to
default `use_index` (v1 when covered), extend C0's cell to pin the routing,
and re-measure the adopt merge in the RFC-023 scenario harness. Only this
commit claims the join-cost win.

Focused suites: `staged_tests`, `merge_fast_forward`, `merge_truth_table`,
`failpoints branch_merge_adopt`, `forbidden_apis`, `lance_surface_guards`.

### Dependency graph

```
A1 → A2 → A3            (PR-A, independent)
B1 → B2 → B3            (PR-B, independent of PR-A; textual merge only)
C0 ─┐
B2 ─┴→ C1 → C2 → C3     (PR-C after PR-B; C0 any time)
C0 + C2 → C4            (indexed follow-up, separate PR)
```

## Review ledger (2026-08-12)

Assumption-by-assumption verification of this plan against OmniGraph main and
Lance 9.0.0 source (`v9.0.0` + `origin/main`).

**Verified sound:**

- L1: preflight and staging share one pinned `Dataset`, so the absence proof
  and `transaction.read_version` name the same parent (`stage_keyed_write`).
- L1: the concurrent-writer race window is unchanged by dropping the join —
  the current MergeInsert join runs against the same pinned handle, and the
  post-L1 conflict story (filtered-Update overlap at commit, effect-free
  finalize, exact re-probe → typed `KeyConflict`) is exactly RFC-023 §6.
- L1: every served graph is storage v6 → stable row IDs and exact-`id` PK on
  all node/edge tables; the helper keeps both checks fail-closed.
- L2a: both the v1 full-schema arm and the v2 arm return
  `affected_rows: Some(..)`, so row-level update OCC holds on either route.
- L2a: `MergeStats` fail-closed check works — there is no "not-matched
  skipped" counter to miss; an absent id shows up as `num_updated_rows` short.
- L3: recovery keeps `+2` headroom as legacy CreateIndex-tail tolerance; no
  sidecar schema bump needed.
- L3 is independent of L1/L2 (different functions; only textual merge risk).

**Corrected in this revision:**

- L1 plan text wrongly offered v1 minting as optional follow-up work. General
  StrictInsert **already certifies v1 mandatorily** (`certify_insert_absence`
  is unconditional for StrictInsert in `stage_keyed_write`); the helper must
  preserve that, and gets it structurally. L4's remaining gap is only
  **cleaned** histories, not uncertified ones.
- L2a filter expectation was wrong: forced-v2 matched-only updates emit
  `Some(empty)` (pinned by `lance_surface_guards`), not `None`. `None` is the
  v1 shape. The adapter accepts either and rejects non-empty.
- L2a's "prefer `use_index(true)`" was **unsafe as written**: the v1
  full-schema update arm supplies top-level-only
  `fields_for_preserving_frag_bitmap`, and Lance's manifest builder extends
  index fragment bitmaps over rewritten fragments for indexes whose field ids
  are absent from that list — an index on a nested field id (vector FSL child)
  could silently claim rewritten fragments. First PR is forced-v2; the indexed
  route is gated on a surface-guard proof or an upstream fix.

**Ranking impact:** L1 and L3 stand as planned. L2a's near-term payoff drops
from "join removal for changed rows" to "insert-capability closure +
fail-closed stats"; its join win is evidence-gated. If the guard cell fails,
prioritize **L4 ahead of the L2a indexed follow-up**.

**Open items — RESOLVED empirically (2026-08-12, Lance `=9.0.0` scratch
harness with stable row IDs, schema `id: Utf8 PK / name: Utf8 / vec:
FixedSizeList<Float32, 8>`, 256 rows):**

1. **Index metadata field ids → top-level; the L2a indexed gate can pass.**
   - `create_index` metadata: BTREE(`id`) → `fields=[0]`, inverted(`name`) →
     `fields=[1]`, vector(`vec`) → `fields=[2]` — the **FSL parent**, not a
     child. Lance's schema preorder for this table is exactly `[0, 1, 2]`:
     a FixedSizeList child has **no separate field id** in Lance's model, so
     the vector column contributes no nested id at all.
   - Direct hazard test: v1-route `UpdateAll` + `DoNothing` (BTREE on `id`,
     default `use_index`) emitted `inserted_rows_filter: None` (v1 route
     confirmed), `fields_for_preserving_frag_bitmap = [0, 1, 2]`,
     `affected_rows` present, stats `updated=2 / inserted=0`. After commit,
     **no index claimed the new fragment** (`claims_new_frags=[]` for all
     three), and FTS found the rewritten rows via flat search of the
     uncovered fragment. The hazard does not materialize for
     OmniGraph-shaped schemas because every OmniGraph-built index (BTREE /
     FTS / vector) references a top-level field id present in the v1 list.
   - Residual caveat to pin in the guard cell: OmniGraph **does** support
     list-typed properties (`types.rs::to_arrow` emits `DataType::List`),
     and a Lance `List` child *does* carry a nested field id — but no
     OmniGraph index targets a list column or child
     (BTREE = enum/orderable scalar, FTS = free-text String, vector = FSL).
     The `lance_surface_guards` cell that unlocks `use_index(true)` for the
     update adapter should therefore include one list-typed property in its
     fixture and assert zero new-fragment claims after a v1 UpdateAll.
   - Status: indexed follow-up is **viable**; it stays gated only on landing
     that checked-in guard cell (invariants: performance/behavior claims
     require a checked-in instrument), not on an upstream fix.

2. **FTS after a `Merged` outcome — behavior verified, docs need one
   clarification, no new class from L3.**
   - Never-indexed table: `scanner.full_text_search` returns a hard Lance
     error (*"Cannot perform full text search unless an INVERTED index has
     been created on at least one column"*). OmniGraph's executor passes the
     scanner error straight through as `OmniError::Lance`
     (`exec/query.rs` ~2491) — no pre-check, no scan fallback.
   - Never-indexed `nearest`: returns Ok via flat KNN (brute force) — the
     documented vector fallback is real.
   - Existing index with stale coverage: FTS **finds** rows in uncovered
     fragments (flat-searched), so post-L3 three-way merges into
     already-indexed tables stay fully searchable before maintenance.
   - L3 therefore adds **no new failure class**: schema apply, load, and
     mutate already defer FTS builds, so "bm25 errors until the first
     `ensure_indices`/`optimize`" is reachable today on any fresh table; L3
     only removes the last eager builder. The L3 PR should still align
     wording: [docs/user/search/index.md](../user/search/index.md) ("needs
     the backing index") is correct for FTS **absence**;
     [docs/user/search/indexes.md](../user/search/indexes.md) ("falls back
     to scans") is correct for vector and for FTS **coverage gaps**, and
     must not be read as an FTS-absence fallback.
