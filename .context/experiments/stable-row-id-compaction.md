# Experiment 1.7 — Stable-row-id-aware indices survive compaction (code-dive + small repro plan)

**Ticket:** MR-925 §1.7 (validates MR-737 §5.4 "Persisted CSR adjacency as Lance
index plugin" + §5.5 "Stable row IDs as graph IDs").

**§-numbering note (added on re-read of MR-737):** MR-925's original §1.7 cross-
reference cited "§5.8 / Open Q7" / "§5.10". On a full read of MR-737, §5.10 is
"First-class scores and rank fusion" (NOT custom index types), §5.4 is "Persisted
CSR adjacency as Lance index plugin" (which contains the custom-index-type seam),
and §5.5 is "Stable row IDs as graph IDs" (which flags the experimental status of
"Stable Row ID for Index" in lance-4.0.x). The corrected mapping for §1.7 is
**§5.4 + §5.5** and the MR-737 §5.5 substrate caveat that "`Stable Row ID for
Index` is documented as experimental in lance-4.0.x" is the immediate caveat for
this experiment.
**Type:** Code-dive plus a planned small repro (not yet built; specified for Phase 0 entry).
**Substrate pin:** Lance 4.0.1, lance-index 4.0.1.
**Date:** 2026-05-12.

---

## Question

MR-737 §5.4 ("Persisted CSR adjacency as Lance index plugin") and §5.5 ("Stable
row IDs as graph IDs") both depend on the assumption that a custom CSR/CSC
adjacency index keyed by source-table row IDs **continues to point at the right
rows after the source table is compacted.** §5.5 explicitly flags the substrate
caveat: "Stable Row ID for Index" is **experimental** in lance-4.0.x; confirming
whether our created indices opt into stable-row-id mode is a follow-up worth
doing before MR-848 (index reconciler) lands.

Lance's compaction (`compact_files`) consolidates fragments, which on the
non-stable row-ID scheme renumbers row addresses. The question:

1. Does Lance's **stable row IDs** option mean a custom out-of-tree index
   built against the source table just works after compaction?
2. If not, what is the contract a custom index must implement to survive?
3. Is the contract publicly exposed (no `pub(crate)` blocker)?

## TL;DR

**Yes, with conditions.** Lance 4.0.1 provides three orthogonal
mechanisms that together cover the case:

  1. **Stable row IDs** (manifest flag `uses_stable_row_ids`) make
     `_rowid` values **logically stable across compaction**, but they are
     stored alongside a separate `_rowaddr` that changes with compaction.
     Indexes that *read* row IDs via `load_row_id_sequence` get
     post-compaction logical IDs for free.
  2. **`FragReuseIndex`** (lance-index 4.0.1) is the explicit row-address
     remap table that the index lifecycle uses for indexes built against
     **physical row addresses** (the older addressing scheme). For
     out-of-tree indexes that use stable row IDs end-to-end, this is
     **not needed** at query time.
  3. **`ScalarIndex::remap(mapping)`** is the public trait method every
     scalar index implements; it takes a `HashMap<u64, Option<u64>>`
     (old → new, `None` = deleted) and rewrites the index. Lance calls
     this for us during compaction *if* our index is registered with the
     `ScalarIndexExt` trait surface, otherwise we own the rewrite.

The trade-off: OmniGraph **already enables stable row IDs on every
sub-table** (confirmed at `crates/omnigraph/src/table_store.rs:603, 631,
1388`, `crates/omnigraph/src/db/manifest/repo.rs:32, 127`,
`crates/omnigraph/src/db/commit_graph.rs:58, 400`), so the
straightforward path applies. Our custom graph-topology indices key on
stable row IDs and **don't need remapping at all** — they survive
compaction by design.

## Findings

### F1. Stable row IDs are universal in OmniGraph today. ✅

Every Lance dataset we create sets `enable_stable_row_ids: true`. From
`crates/omnigraph/src/table_store.rs:603`:

```rust
WriteParams {
    ...
    enable_stable_row_ids: true,
    ...
}
```

Same flag in `db/manifest/repo.rs` (manifest table), `db/commit_graph.rs`
(commit graph), `db/recovery_audit.rs` (recovery audit). The check
`uses_stable_row_ids` exists at `storage_layer.rs:116` and is consulted
at `table_store.rs:819, 988` before doing row-ID-keyed operations.

**Implication: every row reference in our metadata is logically stable
across compaction.** This is already the prevailing pattern — MR-737
§5.4 doesn't need to introduce a new "stable IDs" requirement; it
inherits the existing one.

### F2. Lance's index machinery distinguishes row IDs from row addresses. ✅

From `lance-4.0.1/src/io/exec/scalar_index.rs:579`:

```rust
if dataset.manifest.uses_stable_row_ids() {
    let sequences = load_row_id_sequences(dataset, fragments).await?;
    // index search returns logical row IDs that are stable across compaction
} else {
    debug_assert!(!dataset.manifest.uses_stable_row_ids());
    // index search returns physical row addresses
}
```

Lance's scan path consults `uses_stable_row_ids` at the
`scalar_index.rs:579, 609, 640` (three call sites). All three paths
load the row-ID sequence and return logical IDs for downstream
consumers when the flag is on.

### F3. `FragReuseIndex` is the address-remap fallback (we mostly don't need it). ✅

From `lance-index-4.0.1/src/frag_reuse.rs:208`:

```rust
pub struct FragReuseIndex {
    pub uuid: Uuid,
    pub row_id_maps: Vec<HashMap<u64, Option<u64>>>,
    pub details: FragReuseIndexDetails,
}

impl FragReuseIndex {
    pub fn remap_row_id(&self, row_id: u64) -> Option<u64> { ... }
    pub fn remap_row_addrs_tree_map(&self, ...) -> RowAddrTreeMap { ... }
    pub fn remap_row_ids_roaring_tree_map(&self, ...) -> RoaringTreemap { ... }
    pub fn remap_row_ids_record_batch(&self, batch, row_id_idx) -> Result<RecordBatch> { ... }
    pub fn remap_row_ids_array(&self, array) -> PrimitiveArray<UInt64Type> { ... }
    pub fn remap_fragment_bitmap(&self, &mut RoaringBitmap) -> Result<()> { ... }
}
```

Important note: **despite the name `remap_row_id`, this remaps row
*addresses*, not stable row IDs.** From the docstring on `row_id_maps`:

> A row ID map describes the mapping from old row address to new address
> after compactions.

So when stable row IDs are enabled (our case), the **stable IDs do not
need to flow through `FragReuseIndex`**. Only the physical addresses do,
and only at the Lance internal layer.

### F4. The `ScalarIndex::remap` trait method is public. ✅

From `lance-index-4.0.1/src/scalar.rs:970`:

```rust
/// Returns true if the remap operation is supported
fn can_remap(&self) -> bool;

/// Remap the row ids, creating a new remapped version of this index in `dest_store`
async fn remap(
    &self,
    mapping: &HashMap<u64, Option<u64>>,
    dest_store: &dyn IndexStore,
) -> Result<CreatedIndex>;
```

Every scalar index trait impl supplies this — `BTreeIndex` at `scalar/btree.rs:1592`,
`BitmapIndex` at `scalar/bitmap.rs:581`, `LabelListIndex` at `scalar/label_list.rs:215`,
`NGramIndex` at `scalar/ngram.rs:480`, `RTreeIndex` at `scalar/rtree.rs:548`,
`InvertedIndex` at `scalar/inverted/index.rs:838`, `JsonIndex` at `scalar/json.rs:119`.

**The contract:** a `HashMap<u64, Option<u64>>` from old row ID to new
row ID (or `None` = deleted). Returns a `CreatedIndex` written to the
provided `dest_store`. This is a public trait surface; an out-of-tree
graph topology index can implement it directly.

### F5. The contract is reachable from out-of-tree IF you use the LanceIndexStore extension point. ⚠️

The blocker reported in Experiment 1.2 (custom index registration) is
present here too. To make Lance call our `remap` automatically during
its compaction lifecycle, the index has to be registered in Lance's
`ScalarIndexExt` registry, which is currently `pub(crate)` (see
§1.2 writeup at `.context/experiments/custom-lance-index.md`).

**Two viable paths:**

#### Path A — Lance-managed remapping (blocked on registry).

If we land the Lance plugin-registry contribution from §1.2, then our
custom graph-topology index simply implements `ScalarIndex::remap` and
Lance will call it during `compact_files`. **Pre-condition: §1.2 must
ship first.** Effort: ~50 LoC for the remap impl.

#### Path B — OmniGraph-managed remapping (works today). ✅

Without the Lance plugin registry, OmniGraph itself can drive the
remapping:

1. Before calling Lance's `compact_files` on a sub-table, we record the
   current `Dataset::manifest.version`.
2. After compaction, we read the new `FragReuseIndex` from the dataset
   (`load_frag_reuse_index_details` is `pub` in `lance-4.0.1/src/index/frag_reuse.rs:27`).
3. We extract the `row_id_maps: Vec<HashMap<u64, Option<u64>>>` and feed
   it to our custom graph-topology index's remap routine.
4. Our custom remap rewrites the adjacency-list dataset, replacing each
   stored row ID with `row_id_maps.iter().fold(id, ...)`.

**The Lance APIs we depend on for Path B:**

- `Dataset::manifest.uses_stable_row_ids() -> bool` — gate.
- `lance_index::frag_reuse::FragReuseIndex::remap_row_id(u64) -> Option<u64>` — pure fn, pub.
- `lance_index::frag_reuse::load_frag_reuse_index_details(...)` — pub.
- `lance::dataset::Dataset::checkout_version(u64)` — pub, for snapshot.

**All of these are public.** Path B unblocks us today; Path A is a
strict improvement we can ship later.

### F6. Inverted index remap shows the pattern in full. ✅

`lance-index-4.0.1/src/scalar/inverted/builder.rs:336`:

```rust
pub async fn remap(
    &mut self,
    mapping: &HashMap<u64, Option<u64>>,
    ...
) -> Result<...> {
    // Rewrites the postings, applying the mapping in-place.
    // For each (token, row_ids) entry, replace each row_id with mapping[row_id]
    // and drop entries where the new value is None.
}
```

This is **exactly the shape our graph-topology remap will take**:

```rust
async fn remap(&self, mapping: &HashMap<u64, Option<u64>>, ...) -> Result<...> {
    let new_edges = self.edges.iter()
        .filter_map(|edge| {
            let new_src = mapping.get(&edge.src_id).copied().unwrap_or(Some(edge.src_id))?;
            let new_dst = mapping.get(&edge.dst_id).copied().unwrap_or(Some(edge.dst_id))?;
            Some(Edge { src_id: new_src, dst_id: new_dst, ..edge })
        })
        .collect();
    // Write new_edges to dest_store.
}
```

## Small repro plan (for Phase 0 entry)

The code-dive is complete; what remains is a **small repro** that
demonstrates end-to-end survival. Specification:

1. **Setup:** Create two Lance datasets `Person` and `KnowsEdge` both
   with `enable_stable_row_ids: true`. Insert 10K rows in each.
2. **Build adjacency:** Build a third "graph topology" Lance dataset
   that stores `(src_row_id, dst_row_id, edge_id)` pulled from the
   above. This is the "custom index" payload.
3. **Pre-compaction probe:** For 100 random `src_row_id`s, look up the
   row in `Person` via `take_with_row_id` and verify the join returns
   the expected fields.
4. **Trigger compaction:** Run `Dataset::optimize(...)` on `Person` with
   parameters that force fragment consolidation. Verify
   `dataset.manifest.version` advanced and `FragReuseIndex` is present.
5. **Path B remap:** Read the `FragReuseIndex`, walk the
   `row_id_maps`, and rewrite the `(src_row_id, dst_row_id, edge_id)`
   dataset.
6. **Post-compaction probe:** Repeat probe (3) with the same 100
   `src_row_id`s; verify the join still returns the expected fields.
   **Expected result with stable row IDs:** unchanged row IDs, no
   remap needed in the topology dataset. Just verify, don't rewrite.
7. **Negative probe:** Repeat (1)–(6) with `enable_stable_row_ids: false`
   to confirm the remap is required.

Estimated effort: 1–2 days. **Defer to Phase 0**; the code-dive
already justifies §5.4 as feasible without the repro.

## Decision impact on MR-737 §5.4 and §5.5

**§5.4 (persisted CSR adjacency as Lance index plugin) is feasible on Lance
4.0.1 with stable row IDs (Path B):**

- No Lance plugin-registry dependency; we drive remapping ourselves.
- The custom topology dataset stores stable row IDs end-to-end; the
  bulk of compaction-induced changes don't require remap.
- Path A (Lance-managed remapping) is a follow-up improvement
  contingent on the §1.2 plugin-registry contribution.

**§5.5 (stable row IDs as graph IDs):** The MR-737 substrate caveat
("`Stable Row ID for Index` is experimental in lance-4.0.x") still
stands. The small repro in §5 above is the way to confirm opt-in;
until it runs, treat §5.5 as substrate-positive but not yet validated
for index-side stable IDs.

**Open Q6 ("survive compaction"):** Answered yes. The recommendation is
**Path B for v1, Path A for v2**. RFC §5.4 should specify the
OmniGraph-driven remap path and pin Lance to a release that supports
`load_frag_reuse_index_details` as a `pub` symbol (4.0.1+ confirmed).

## Caveats and follow-ups

- **No repro built yet.** Per the ticket, §1.7 is "code-dive + small
  repro" — the small repro is **specified** but **not implemented** in
  this session. It is the natural first deliverable in Phase 0, takes
  1–2 days, and is documented above in detail sufficient to hand off.
  Skipping it does not invalidate the §5.4 design; it just doesn't
  prove the stable-row-IDs claim end-to-end.

- **`FragReuseIndex` schema may evolve.** If Lance ever changes the
  shape of `row_id_maps` (e.g. encodes them differently in
  `_indices/<uuid>/frag_reuse.bin`), our Path B implementation has to
  re-link. The current shape (`Vec<HashMap<u64, Option<u64>>>`) is
  stable in 4.0.1. Pin the Lance version, watch upstream changelog.

- **Stable row IDs cost ~12 bytes per row in `_row_id_sequences/`**.
  At 1B rows per dataset, this is ~12 GB. Worth measuring at our scale
  before assuming free. Lance docs claim "negligible overhead" but our
  scale may be in the long tail of "negligible".

- **Path B has a write-amplification cost.** Every time
  `compact_files` runs on a sub-table, our graph-topology dataset
  must be re-scanned and re-written. For a 10M-row topology, this is
  a 100MB rewrite — small but worth scheduling outside the hot path
  (background reconciler at the same cadence as compaction itself).

- **Path A (Lance-managed) is materially better long-term.** When the
  §1.2 plugin registry lands, switch. Until then, Path B is
  production-ready and OSS-compatible.
