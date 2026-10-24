---
rfc: "2026-10-25-incremental-index-reconciliation"
title: "Incremental index reconciliation"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-25
updated: 2026-10-25
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Incremental index reconciliation

> A term set in ***bold italics*** is being defined at that exact spot;
> it is used in plain text everywhere after.

## Summary

Index maintenance today rebuilds every lagging index from the whole table, so
one appended row costs a pass over every row, and full-text coverage is never
refreshed by maintenance at all. This RFC makes the work proportional to what
changed. An index gains a segment over just the fragments that hold only rows
no segment of that index has indexed; segments are merged under a bounded
policy; rows an update or compaction moved are folded in by a filtered merge
(BTREE) or a whole rebuild (full text, vector).
One rule keeps every answer equal to a full build's, and full text is
maintained like the other kinds.

The observable contract does not change: `omnigraph index build` and
`optimize` (PR #1150) still leave every buildable declared index present and
covering every current fragment, publish once per run, and refuse uncertified
full-text indexes. Only their cost, and the `action` they report per index,
change. The storage format does not change; multi-segment indexes are native
Lance.

## Motivation

The index reconciler (`maintain_indices_for_branch`, and optimize's index
links after compaction) has three outcomes per index: build it if it is
missing, rebuild it whole if its segments leave a current fragment uncovered
(`TableStore::stage_index_fold`), or leave it alone. A table that receives a
few rows between two runs therefore has each of its BTREE and vector indexes
rebuilt from every row, every run. The cost of routine maintenance grows with
table size, not with the rows written since the last run.

Full text is worse. `TableStore::can_fold_index` excludes it from the
coverage refresh, so a full-text index's uncovered tail grows until an
operator runs `omnigraph index rebuild --kind full-text`, which rebuilds every
full-text index of the branch. The tail is still answered correctly (Lance
scans it with the index's analyzer), but a bm25 scan over a partly covered
index applies its filter after scoring (`Eligibility::AfterScoring`) and every
query pays the tail scan.

Whole rebuilds were chosen for two reasons, both still true and both
avoidable:

- **Lance serves stale entries from delta segments after an update.** On a
  stable-row-ID dataset an update keeps the row ID and moves the row to a new
  fragment. Lance's shared prefilter admits a row ID when its current live
  copy sits in a fragment some segment covers, so once a newer segment covers
  the moved row's fragment, an older segment's entry for the old value passes
  too: full-text search returns the row for a term it no longer holds, and a
  BTREE equality returns it for its old value (lance#9817, pinned by
  `lance_surface_guards.rs::index_delta_segment_serves_stale_entries_of_an_updated_row`).
  Vector search filters each segment by its own coverage; scalar and
  full-text search do not.
- **Lance's own incremental fold commits linearly.** `optimize_indices`
  appends and merges segments inside its own commit, which a detached-only
  table (RFC 0067) cannot use, and it offers no point before publication to
  write the full-text analyzer certificate (RFC 0043).

Neither forces a whole rebuild. The hazard needs a segment over a fragment
that holds the current copy of a row another segment indexed from elsewhere,
which a reconciler can refuse to build. And Lance exposes the pieces of its
fold publicly: fragment-subset builds and segment merges that return
uncommitted metadata, which OmniGraph stages in its own detached `CreateIndex`
as it already does for every index build.

## User and operational behavior

No command, flag, route or error changes. `omnigraph index build` and
`optimize` keep the contract PR #1150 states.

The `action` an index build reports per index gains two values: `extended`
(a segment was added over new rows) and `merged` (segments were combined).
`built` and `refreshed` (rebuilt whole) remain. The field is additive in the
JSON output and in `BuiltIndex`.

Full-text coverage stops being reported as pending: a certified full-text
index is extended, merged or rebuilt like any other kind. A full-text index
without a certificate is still refused by maintenance and still needs
`omnigraph index rebuild --kind full-text`, which remains the analyzer
migration and the way to replace an externally created tokenizer. A vector
property with no non-null vectors stays pending.

Reads see more segments per index between merges. Each segment is searched
on its own: a BTREE probe per segment, a full-text posting lookup per segment
scored by one corpus-wide scorer, a nearest search per segment with its own
probe budget (the per-delta `maximum_nprobes` the engine already sets). The
merge policy bounds the number of segments.

## Design

### Terms

An index is a name with one or more ***segments***, each covering a set of
fragments recorded in its `fragment_bitmap`. An index's ***covered
fragments*** are the current fragments some segment of it covers. A current
fragment is ***fresh*** for an index when none of its row IDs appears in a
fragment that index covers; a fragment that is not fresh is ***moved***.

### The rule

> An index gains a segment only over fragments that are fresh for it.

Why it keeps answers equal to a full build. On a stable-row-ID dataset
Lance's prefilter is an allow-list of the row IDs whose current live copy
lies in the covered fragments: the row-ID sequence of each covered fragment
minus its deletion vector
(`lance-13.0.0/src/index/prefilter.rs:176`, `do_create_deletion_mask_row_id`,
built from the union of segment bitmaps at `prefilter.rs:77-97`). A segment's
entry for row ID `r` can be wrong only if `r` is admitted and the entry was
built from a copy of `r` other than its current one. Under the rule, a
segment never covers a fragment holding a copy of a row ID that an existing
segment already indexed, so the covered current copy of every admitted row
is the copy its entry was built from. An insert gets a new row ID from the
manifest's monotonic `next_row_id`, so a fragment written by inserts alone is
fresh. An update keeps the row ID and writes the new copy to a new fragment,
which is moved for every index that covered the old copy (also when the same
write inserted other rows into it); that fragment stays uncovered, so the
stale entry is masked and the row is answered by the scan of uncovered
fragments, as today. Compaction moves copies without changing values, so a
compacted fragment is moved too. Deletions are masked by deletion vectors.

The rule holds for any number of segments and is preserved by any merge of
segments that each satisfied it: a merged segment covers the union of its
sources' fragments and holds the union of their entries, and the argument
above does not depend on how entries are grouped into segments.

The rule is the same for BTREE, full-text and vector indexes. Vector search
additionally restricts each segment to its own fragments
(`lance-13.0.0/src/io/exec/knn.rs:2276-2289`, `:2390-2407`), which would allow
vector segments over moved fragments, but one rule for every kind is simpler
to keep and to review.

### The fresh test

The test reads only metadata. For each index it takes the row-ID sequences of
the covered fragments and of each uncovered fragment through Lance's public
`lance::dataset::rowids::load_row_id_sequence(s)`
(`lance-13.0.0/src/dataset/rowids.rs:37`, `:194`), which reads them from the
manifest and caches them in the session's metadata cache; graph tables keep
their sequences inline (spilled row lineage is not a graph capability,
[lance.md](../dev/lance.md#current-compatibility-fences)). An uncovered
fragment is fresh when its smallest row ID exceeds the largest row ID of every
covered fragment.

The test is conservative. Inserts allocate row IDs above every existing one,
so a fragment of new rows always passes; a fragment holding an updated or
compacted row always fails. When a segment names a fragment the current
manifest no longer holds, or a sequence cannot be read, every uncovered
fragment counts as moved. A wrong "moved" costs the moved-fragment action (a
merge for BTREE, a rebuild otherwise); a wrong "fresh" is impossible from the
sequences alone. An index with a segment whose `fragment_bitmap` is absent
(coverage unknown) is left to explicit handling, as today: Lance cannot merge
it, and unknown coverage is not evidence of work.

### The decision per index

For each declared index of a table, from one pinned version, the reconciler
takes one of these actions, and stages every table's actions as one detached
`CreateIndex`:

| State | Action | Work |
|---|---|---|
| no segment with rows (missing, or full text with only the untrained declaration) | build | every row, once |
| uncovered fragments, all fresh | extend: one segment over them | the new rows |
| uncovered moved fragments, BTREE | build a segment over them, then merge it with every segment in one filtered merge; publish only the merged segment | the index's entries; no table scan |
| uncovered moved fragments, full text or vector | rebuild whole | every row |
| more than the policy's segment count | merge (BTREE, full text) or rebuild (vector) | the merged segments' entries |
| full text without a certificate | refuse, as today | none |
| vector property with no non-null vector | pending, as today | none |

The BTREE moved-row action is safe although its intermediate segment breaks
the rule, because that segment is never published: Lance's BTREE merge
filters each source segment's entries to the live row IDs of that segment's
own fragments (`lance-13.0.0/src/index/scalar/btree.rs:122`, through
`index/append.rs:201` `build_per_segment_filters` and `:103` `live_row_ids`),
which drops the old segment's entry for every moved row. The merged segment
therefore satisfies the rule's conclusion directly. Lance's full-text merge
applies no such filter (`index/scalar/inverted.rs:1002`, `None` as the old-data
filter), and its vector merge needs shared model artifacts, so those kinds
rebuild.

Segments come from the public builder over a fragment subset,
`create_index_builder(..).fragments(ids).replace(true).execute_uncommitted()`
(`lance-13.0.0/src/index/create.rs:127`; `replace(true)` passes the existing-name
check at `:376`; the subset becomes the segment's `fragment_bitmap` at
`:665-668`), and merges from `merge_existing_index_segments`
(`lance-13.0.0/src/index.rs:2114`), both returning uncommitted
`IndexMetadata`, the shape Lance documents for distributed index builds
([distributed indexing](https://lance.org/guide/distributed_indexing/)).
OmniGraph builds the `CreateIndex` itself: an extension adds the new segment
and removes nothing; a merge or rebuild removes its source segments by UUID.
Lance's `CreateIndex` apply checks neither names nor coverage
(`lance-table-13.0.0/src/transaction/manifest_build.rs:1157-1178`); its
disjointness check runs only on its own commit path
(`lance-13.0.0/src/index.rs:116-145`). The reconciler asserts disjoint
coverage for every index before staging.

### Merge policy

The policy replaces today's fold rule that collapses every vector index with
more than one segment (`TableStore::foldable_index_lag`). Segments are merged
when an index holds more than `K` of them, smallest first, so that each row
is rewritten a bounded number of times as the index grows (a tiered policy: a
row moves into a larger segment only when the segments of its size outnumber
the bound). `K` bounds the per-query fan-out.
Both are constants of the reconciler, named in its report, not hidden
statistics. The constants are an unresolved question.

### Full text

`TableStore::can_fold_index` stops excluding full text for certified
indexes. Two certificate rules change in RFC 0043:

- A segment built by the engine's builder from the rows of the fragments it
  covers is certified, as a whole-table build is today (`fts_compat.rs`
  currently names "all of them, or none").
- A merge whose source segments all carry a valid certificate of the current
  analyzer generation is certified. The merge combines posting lists without
  tokenizing again, and Lance refuses sources whose parameters differ
  (`validate_segment_params_compatible`, called from `index.rs:2114`). The
  certificate is written before publication, as for any build, so the
  recovery obligation RFC 0043 rejected for certifying incremental outputs
  after publication does not arise.

BM25 over several segments uses one scorer built from every segment's token,
document and term-frequency counts
(`lance-index-13.0.0/src/scalar/inverted.rs:418`, `build_global_bm25_scorer`).
Scores therefore equal a full build's over the same rows, except that rows
deleted after a segment was built still count in its statistics until the
next merge or rebuild, as they do in a single segment today.

### One decision function

Optimize applies the decision after its compaction link; `index build`
applies it on any branch; read-only `index status`
([RFC 0046](0046-index-status.md), draft) reports it without executing, so
status and maintenance cannot disagree about what an index needs.

## Invariants

- **Graph publication and snapshots (2, 3, 4).** Unchanged: one pinned
  snapshot plans every table, artifacts are built before the writer gates,
  authority is revalidated under them, and every table publishes in one
  manifest commit.
- **Crash convergence (5).** New segments are staged index directories that
  no manifest names until publication; the collector reclaims them as it
  reclaims any staged index today.
- **Physical acceleration is derived state (7).** The rule is what makes a
  partly maintained index answer as a full build does. Coverage chooses
  maintenance work, never answers. Index work stays in explicit
  reconciliation, never on the content-write path.
- **Integrity failures are loud (8).** An unknown fresh test takes the
  moved-fragment action, never a guess; a coverage overlap fails the run
  before staging.
- **Bounded work (11).** Routine maintenance scales with rows written since
  the last run, and the merge policy bounds both rewrites per row and query
  fan-out.
- **One source of truth (12).** Freshness is derived from the pinned
  manifest's fragments and sequences; nothing new is persisted.

Deny-list: no synchronous index work on a content write; no logical
precondition based on index coverage (coverage selects maintenance, not
results); no cost-blind choice hidden from the report.

## Compatibility and reversibility

No format change. Multi-segment indexes are native Lance and are already read
by every supported OmniGraph release: index facts take the union of a name's
segments, nearest search sets a probe budget per delta, and full-text search
verifies each segment's certificate. An older binary reads such an index as
it reads one today; its optimize leaves a fully covered BTREE or full-text
index alone and collapses a multi-segment vector index into one. Reverting
this RFC restores whole rebuilds; existing multi-segment indexes stay valid
until the next rebuild folds them.

The [Lance fence](../dev/lance.md#current-compatibility-fences) "Index
segments" changes from "OmniGraph never adds a delta segment" to "OmniGraph
adds a segment only over fragments fresh for its index". The existing guard
stays: it pins the hazard the rule avoids, and when it goes red (Lance applies
segment ownership to scalar and full-text search) the rule can relax to admit
moved fragments.

## Alternatives

- **Keep whole rebuilds.** Correct and simple; cost grows with table size on
  every run, and full-text coverage stays manual.
- **Wait for lance#9817.** Segment ownership in scalar and full-text search
  would make segments over moved fragments safe. No fix is scheduled, and the
  rule here needs nothing from upstream; it relaxes when the fix lands.
- **Use Lance's `optimize_indices`.** It commits linearly and has no
  certificate hook; incompatible with detached-only tables.
- **Defer rebuilds behind a threshold.** Leave a moved tail scanned until it
  exceeds a fraction of the table. It bounds amortized cost but lets a
  selective query scan a large tail, and it does not help appends, which are
  the common case. It may complement this design for full text and vector
  (unresolved).
- **Persist a row-ID watermark per segment** (a file in the segment's
  inventory, as the full-text certificate is). It would keep the fresh test
  exact after a compaction removes covered fragments, at the cost of a new
  artifact to verify and collect. The sequences suffice; compaction already
  rewrites the table.
- **Allow vector segments over moved fragments**, relying on vector search's
  per-segment filter. It saves vector rebuilds after updates but splits the
  rule by kind; not worth it until updates of vector rows are measured to
  matter.

## Evidence and tests

Lance surfaces reviewed at 13.0.0: the index, BTREE, full-text, vector,
fragment-reuse and distributed-indexing pages; the row-ID lineage and
transaction specifications; and the source cited above. The row-ID lineage
page says secondary indexes reference row addresses and that stable row IDs
in indexes are in progress; the 13.0.0 source paths cited here
(`do_create_deletion_mask_row_id`, `build_stable_row_id_filter`) work in
stable row IDs, so the guards below pin the code, not the page.

New `lance_surface_guards.rs` guards, each red when the fact changes:

- a subset segment over fresh fragments answers BTREE equality and range,
  full-text search and BM25 scores exactly as a full build over the same rows;
- two segments of one name with disjoint coverage, staged through OmniGraph's
  own `CreateIndex`, open and answer as one index;
- the shared prefilter admits a row ID only through its current live copy
  (an updated row's old entry is masked while its new fragment is uncovered);
- the BTREE merge drops a moved row's old entry; the full-text merge keeps it
  (a negative guard, red when the filter lands);
- a certificate written on a subset build and on a certified merge verifies.

Engine owners (`integration/maintenance.rs`, `integration/branching.rs`):
appends extend each index without a rebuild; an update merges the BTREE
(equality on the old value returns nothing) and rebuilds full text and vector;
a compaction followed by a build takes the moved-fragment action; an
unreadable sequence does too; a segment without a coverage record is left
alone and reported; the segment count stays within `K`. A
differential test in `integration/proptest_equivalence.rs` runs random
inserts, updates, deletes, compactions and index builds and compares
equality, range, full-text and exact nearest answers with an index-free scan.

Cost: the [DST storage-cost golden](../dev/testing.md#dst-storage-cost-golden)
gains "append then index build" at two table sizes with equal rows, the
golden's own form of a flat-in-table-size claim, and a `write_cost.rs` curve
asserts the same through `IoCounts`. The DST index-data consistency audit
extends to the new actions.

## Rollout

1. Guards, the fresh test and extension for BTREE indexes. Full text and
   vector keep whole rebuilds; nothing else changes.
2. The merge policy and the BTREE moved-row merge.
3. Full text: lift the exclusion for certified indexes, the certificate rules
   (with a disposition note on RFC 0043), extension and merge.
4. Vector extension, with rebuild as its merge, replacing the fold rule that
   collapses every multi-segment vector index.
5. `index status` through RFC 0046.

Each phase keeps the rule and falls back to a whole rebuild for anything it
does not handle, so each ships alone. `implementation` advances per phase.

## Unresolved questions

- The merge policy's constants: `K`, and the size ratio that defines a tier.
- Whether optimize should defer full-text and vector rebuilds for a small
  moved tail, and how its report states that, given that `index build`
  promises full coverage.

## Decision log

- 2026-10-25: drafted as the second step of #840. PR #1150 (open) puts the
  `index build` contract on today's whole rebuilds; this RFC changes only
  their cost.
