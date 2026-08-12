# Plan: Merge latency L1–L3

**Status:** implementation plan (not authorized as an RFC)
**Depends on:** [merge-complexity.md](merge-complexity.md) ranking
**Out of scope here:** L4 cleaned-history admission, L5 Lance indexed key filter,
L6 fragment adopt (RFC-0001)

This plan turns the validated L1–L3 fixes into shippable PRs: what changes,
what must not change, how to test, and which assumptions gate each step.

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
  `lance_surface_guards` cell must prove either (a) OmniGraph's vector/FTS
  index metadata references only top-level field ids for our schemas, or
  (b) an upstream fix emits the full preorder on the v1 arm. Until one holds,
  do not enable `use_index(true)` for the update adapter.
- This lowers L2a's expected leverage vs the original ranking; if the guard
  cell fails, the indexed win waits on upstream and **L4 overtakes L2a** in
  practical value.

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

**New open verification items:**

1. Surface-guard cell: which field ids do OmniGraph's vector/FTS index
   metadata reference (top-level vs nested child)? Decides the L2a indexed
   gate.
2. L3 user-visible check: behavior of `search`/`bm25` immediately after a
   `Merged` outcome on a table whose inverted index has **never** been built
   (fresh table). FF/adopt already defer, so this is not a new class, but
   [docs/user/search/index.md](../user/search/index.md) says search functions
   need the backing index — verify fallback vs error and align wording in the
   L3 PR.
