# Experiment 1.2 — Custom Lance index plugin from outside the lance crate

**Ticket:** MR-925 §1.2 (validates MR-737 §5.4, §5.5).
**Prototype:** `validation-prototypes/custom-lance-index/` (long-lived branch).
**Substrate pin:** Lance 4.0.1 (matched by cargo to 4.0.0 spec). Lance 4.0.1 internally pulls roaring 0.11 and prost-types 0.14; the workspace deps were lifted to match.
**Date:** 2026-05-12.

---

## Hypothesis

A graph engine running on top of Lance can ship a custom index type
(e.g. a neighbor-set adjacency index) from a third-party crate, by:

  1. constructing an `IndexMetadata` row with a custom `index_details: Any`,
  2. committing it via the transaction API (`Operation::CreateIndex`),
  3. having Lance round-trip it through the manifest unchanged, and
  4. having the Lance scanner dispatch filter pushdown to our plugin.

§5.4 of MR-737 currently leaves (4) as an open question — this experiment
turns the answer into evidence.

## Method

`custom-lance-index/` builds a tiny Lance dataset (`(key: UInt64, payload:
Utf8)`, 1000 rows in fragment 0), then runs five probes against the public
surface of `lance = 4.0.1`:

| Probe | What is exercised |
|-------|-------------------|
| **P1** Construct + commit | Build an `IndexMetadata` with a custom `index_details.type_url = "omnigraph.v0.NeighborIndexDetails"` and commit it with `Dataset::commit(..., Operation::CreateIndex { new_indices, removed_indices }, ...)`. |
| **P2** Load round-trip    | Reopen the dataset and call `DatasetIndexExt::load_indices()`. Verify the index survives Lance's `retain_supported_indices()` filter and its `index_details` survives bit-for-bit. |
| **P3** Append coverage    | Call `Dataset::append(...)`, then re-load indices. Verify the `fragment_bitmap` is *not* auto-updated to cover the new fragment — i.e. coverage is the plugin's responsibility, not Lance's. |
| **P4** Scan filter        | Run a `Dataset::scan().filter("key = 42")` and observe whether Lance attempts to open our plugin. With the plugin registry closed (see below), expect a full-scan fallback rather than an opt-in dispatch. |
| **P5** Compact (Rewrite)  | Call `compact_files(...)` and observe whether the index survives the Rewrite operation and whether the `fragment_bitmap` is remapped. |

Output (release-mode run, single execution):

```
--------------------------------------- custom-lance-index compatibility matrix ----------------------------------------
probe                            outcome        notes
------------------------------------------------------------------------------------------------------------------------
P1 construct+commit              OK             Operation::CreateIndex accepted custom type_url; commit v2
P2 load_indices (round-trip)     OK             type_url='omnigraph.v0.NeighborIndexDetails' fragment_bitmap.len=1 survives retain_supported_indices
P3 append-row coverage           STALE_AS_EXPECTED fragment_bitmap=[0] (expected [0]); new fragments not auto-covered
P4 scan with filter on indexed col FULL_SCAN_FALLBACK rows=1 (expected 1); SCALAR_INDEX_PLUGIN_REGISTRY refuses unknown type_url so scanner falls back to full scan
P5 compact_files (Rewrite)       STALE_BITMAP   before=1 indices; after=1 indices; rewritten files=0; new fragments=[0, 1]; idx.fragment_bitmap=[0]
```

## Findings

### F1. The transaction surface is open. ✅

`Dataset::commit(uri, Operation::CreateIndex { new_indices: vec![idx],
removed_indices: vec![] }, ...)` is a fully public API. `IndexMetadata` is
a `pub struct` in `lance-table::format` with **every field public**,
including `index_details: Option<Arc<prost_types::Any>>`, `fragment_bitmap:
Option<RoaringBitmap>`, `index_version: i32`, `fields: Vec<i32>`. We can
construct it with any `type_url` and `value: Vec<u8>` we want.

### F2. The retention filter does not block unknown type_urls. ✅

`lance/src/index.rs::retain_supported_indices` defends against version
skew, not against unknown plugins. Its core check is:

```rust
let max_supported_version = idx
    .index_details
    .as_ref()
    .map(|details| {
        IndexDetails(details.clone())
            .index_version()
            // If we don't know how to read the index, it isn't supported
            .unwrap_or(i32::MAX as u32)
    })
    .unwrap_or_default();
let is_valid = idx.index_version <= max_supported_version as i32;
```

When `index_details.type_url` is unknown to the static
`SCALAR_INDEX_PLUGIN_REGISTRY`, `index_version()` returns `Err`, the
`.unwrap_or(i32::MAX as u32)` triggers, and the index is retained. Our
P2 outcome confirms this — the comment-vs-code mismatch ("If we don't
know how to read the index, it isn't supported") is misleading; the actual
behavior is that unknown indices are *kept* in the manifest. Good for our
purposes (we want our custom index to round-trip cleanly), but worth
filing upstream as a comment/behavior fix.

### F3. The plugin registry is closed. ❌ **HARD BLOCKER for §5.4.**

`lance/src/index/scalar.rs:223` (4.0.1):

```rust
// TODO: Allow users to register their own plugins
static SCALAR_INDEX_PLUGIN_REGISTRY: LazyLock<Arc<IndexPluginRegistry>> =
    LazyLock::new(IndexPluginRegistry::with_default_plugins);
```

- The static is **module-private** (no `pub`).
- `IndexPluginRegistry::with_default_plugins` is the only constructor used,
  and its initialization registers a fixed set of types (BTree, Bitmap,
  LabelList, Inverted, NGram, ZoneMap, BloomFilter, RTree, and the vector
  family).
- There is no `register_plugin` or `extend_registry` API exposed by the
  `lance` crate.
- `IndexType` is itself a closed enum (lance-index/src/lib.rs:106) with no
  `Custom` variant; `Index::index_type(&self)` must return one of the
  built-in values.

Consequence: **Lance 4.0.1 cannot dispatch its scanner to a third-party
index plugin**. The downstream functions that gate scan-time index use —
`open_scalar_index`, `infer_scalar_index_details`, `IndexDetails::supports_fts`,
`IndexDetails::is_vector` — all consult `SCALAR_INDEX_PLUGIN_REGISTRY` or
hard-coded `type_url` suffix checks. Even if we masquerade as
`type_url.ends_with("BTreeIndexDetails")`, the scanner will then assume
our index is a real BTreeIndex and try to open BTree-format files in the
index directory, which we don't have.

### F4. The engine owns fragment_bitmap maintenance. ⚠️

P3 confirms: when we append a new fragment, Lance does **not** update the
custom index's `fragment_bitmap` (and would not even know how — the plugin
contract for "rebuild on append" lives inside the plugin registry, which
is closed to us). Any custom-index reconciler we ship has to:

  - re-read `load_indices()` after every commit,
  - compute the diff between `fragment_bitmap` and the current fragment set,
  - emit `Operation::CreateIndex { new_indices: vec![updated], removed_indices: vec![old] }`
    to re-publish the index with the updated bitmap.

This is *consistent with* the §5.5 reconciler pattern in MR-737, so it's
not a blocker — but the writeup of §5.5 should explicitly say "the
reconciler also owns fragment coverage diffs, not just file content".

### F5. Compaction does not move our index. ⚠️

P5: with default `CompactionOptions`, two small fragments of 1000 + 500
rows did not trigger a Rewrite (`files_added: 0`). This is not a
custom-index issue — it's the default heuristic. The signal we need is:
**if a Rewrite had happened, would `Operation::Rewrite { groups, rewritten_indices,
frag_reuse_index }` have remapped our index?** Looking at the conflict
resolver (lance/src/io/commit/conflict_resolver.rs:495 onward), the answer
is no — `rewritten_indices: Vec<RewrittenIndex>` is constructed only for
indices whose plugin returns a remapper. Unknown-type indices fall through
without remapping. So:

- **After a real compaction, our custom index will have a stale
  `fragment_bitmap`** pointing at fragment IDs that may have been
  rewritten into new IDs.
- **Stable row IDs** (when `enable_stable_row_ids=true` on the dataset)
  would survive — but our `fragment_bitmap` would not.

We need to re-run with a more aggressive `CompactionOptions` to capture
the exact post-Rewrite bitmap drift; that's a 1-hour follow-up. The
qualitative answer is settled: **compaction without an index reconciler
will leave our custom index pointing at dead fragments.**

## Per-operation compatibility matrix (the table §1.2 asks for)

| Lance operation       | Custom index behavior with the public-API approach           | Engine reconciler responsibility |
|-----------------------|--------------------------------------------------------------|----------------------------------|
| `Append`              | IndexMetadata retained, `fragment_bitmap` STALE.             | Detect new fragments; re-publish IndexMetadata with updated bitmap. |
| `Update` (vertical)   | Same as Append — new fragments arrive; old bitmap stale.     | Same as Append, plus invalidate index entries for moved rows. |
| `Delete`              | IndexMetadata retained; new deletion files don't touch bitmap. | Index need not change unless the plugin caches row→key mappings. |
| `Rewrite` (compact)   | IndexMetadata retained but `fragment_bitmap` points at dead fragments; no remap. | Reconciler must rebuild bitmap (or use stable row IDs and remap externally). |
| `Merge` (column add)  | IndexMetadata retained; index files unaffected since indexed columns unchanged. | None for column-add. For column-rewrite, full rebuild. |
| `Project` (column drop)| IndexMetadata retained but `fields: Vec<i32>` may now point at a dropped column. | Reconciler must DROP the IndexMetadata when its column is removed. |

The "engine reconciler responsibility" column is *additional* work over
what a fully-registered Lance plugin would get for free, because we can't
register.

## Decision impact on MR-737 §5.4

**§5.4's current premise (build custom index plugins from outside the
lance crate) is NOT achievable on Lance 4.0.1 as written.** Three viable
paths forward:

1. **Vendored fork of lance-index** — fork lance-index, expose
   `SCALAR_INDEX_PLUGIN_REGISTRY` plus a `register_plugin` API, and pin
   to the fork. Reduces to a maintenance burden equivalent to running our
   own substrate; explicitly disallowed by docs/invariants.md "Hand-rolling
   something Lance already does" — but here Lance does NOT yet do this. The
   honest framing is that Lance's *interface* for it doesn't exist yet.

2. **Upstream contribution** — implement the "Allow users to register their
   own plugins" TODO and contribute it back. Requires upstream review +
   release cycle; Lance is in pre-1.0 (4.x) and the protobuf surface for
   `index_details` is already pluggable, so the interface delta is small.
   This is the **recommended path**; the next §11 update to MR-737 should
   call out "depends on Lance issue: scalar-index-plugin-registry pluggability".

3. **Run our custom index entirely outside Lance** — store our index in a
   separate Lance dataset (or a sidecar key-value store) keyed by the
   primary table's stable row IDs. Lance round-trips an empty IndexMetadata
   row (or none) for visibility; query-time pushdown is done by the
   engine's planner via a manually-injected `PrefilterExec` that consults
   our external index and produces a row-ID `BatchSelection`. This is the
   pattern lance-graph appears to use for its neighbor index (TBC in
   experiment 3.3); it bypasses Lance's index-dispatch entirely.

§5.4 should be rewritten to **pick path (2) or path (3) explicitly**, not
both. The current MR-737 wording implies path (1) is available; this
experiment proves it is not.

§5.5 (reconciler pattern) is unaffected by this finding — but it must
expand to explicitly own `fragment_bitmap` recomputation across all
mutating operations, since with path (2) or path (3) we are the only
party that knows the index's row coverage.

## Caveats

- **Default `CompactionOptions` did not trigger a Rewrite.** P5 is a
  qualitative answer from source-code reading; we need a re-run with
  `CompactionOptions { target_rows_per_fragment: 100, ..default }` (or
  enough small fragments to force one) to capture the exact bitmap drift.
  Follow-up: 1 hour.
- **Stable row IDs not exercised.** The dataset was created without
  `enable_stable_row_ids=true`. Experiment 1.7 covers this surface.
- **No write/read of actual index data.** This experiment is about the
  *metadata* round-trip, not about a working index over `key`. A real
  prototype would write a BTreeMap<u64, RowAddr> to a sidecar file under
  `<uri>/_indices/<uuid>/` and read it back at scan time via a manual
  prefilter. F3 says we already can't dispatch via Lance, so building the
  data round-trip is a path (2)/(3) decision deferred to Phase 0.

## Follow-ups (tracked, not done in this experiment)

- File upstream Lance issue: "Document or change behavior of
  `retain_supported_indices` for unknown `type_url`s — comment claims
  drop, code retains."
- File upstream Lance issue: "Make `SCALAR_INDEX_PLUGIN_REGISTRY` pluggable
  (`register_plugin` API)." Block point for `lance-graph` and other
  graph layers.
- Re-run P5 with aggressive `CompactionOptions` and an `enable_stable_row_ids`
  dataset to capture bitmap drift quantitatively (1 hr).
- Compare the lance-graph repo's actual approach to extending Lance —
  cover in experiment 3.3.
