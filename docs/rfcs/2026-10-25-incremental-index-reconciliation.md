---
rfc: "2026-10-25-incremental-index-reconciliation"
title: "Incremental index reconciliation"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-10-25
updated: 2026-11-08
discussion: https://github.com/ModernRelay/omnigraph/pull/1156
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
refreshed by maintenance at all. This RFC makes the work for appended rows
proportional to those rows. An index gains a segment over just the fragments
that hold only rows no segment of that index has indexed; segments are merged
under a bounded policy; rows an update moved are folded in by a filtered
merge (BTREE) or a whole rebuild (full text, vector). One rule keeps
every answer equal to a full build's.

For BTREE and vector indexes the contract of `omnigraph index build` (#1150,
shipped in v0.15.0) and `optimize` does not change: they leave every
buildable declared index present and covering every current fragment, and
publish once per run. For full text it does: both leave full-text coverage
pending, to be refreshed by `omnigraph index rebuild --kind full-text`, and
this RFC has both commands maintain a certified full-text index like the
other kinds. A full-text index without a certificate stays pending, as today.
The storage format does not change; multi-segment indexes are native Lance.

The moved-row half of the design is a fence for Lance 13.0.0. Lance's fixes
for stale entries from segmented scalar and full-text indexes (lance#9785,
lance#9872) merged upstream on 2026-10-23 and are in no stable release; the
bump that contains them can index moved rows the way it indexes appended ones
and delete the fence. An earlier bump has its own obligation: Lance 14.0.0
writes a new BTREE segment in the row-address domain, which today's
compaction refuses on a graph table. That bump must remap such segments in the
compaction's own `Rewrite` (see [The Lance fences](#the-lance-fences)).

## Motivation

Optimize's index links after compaction, and the `index build` command
(#1150, through `TableStore::stage_index_reconciliation`), have three
outcomes per index: build it if it is missing, rebuild it whole if its
segments leave a current fragment uncovered (`TableStore::stage_index_fold`),
or leave it alone. This includes the identity and endpoint BTREEs every node
and edge table carries, not only schema `@index` declarations. A table that
receives a few rows between two runs therefore has each of its BTREE and
vector indexes rebuilt from every row, every run. The cost of routine
maintenance grows with table size, not with the rows written since the last
run.

Full text is worse. `TableStore::can_fold_index` excludes it from the
coverage refresh, so a full-text index's uncovered tail grows until an
operator runs `omnigraph index rebuild --kind full-text` (`omnigraph
rebuild-full-text-indexes` before v0.15.0, a deprecated spelling there),
which rebuilds every full-text index of the branch. The tail is still answered
correctly (Lance scans it with the index's analyzer), but a bm25 scan over a
partly covered index applies its filter after scoring
(`Eligibility::AfterScoring`) and every query pays the tail scan.

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
  full-text search do not in 13.0.0. Upstream fixed both on 2026-10-23
  (lance#9785 scalar, lance#9872 full text and the full-text merge), after
  13.0.0 and outside v14.0.0; Lance's v15.0.0 betas contain both.
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
`optimize` keep the contract PR #1150 states, except that a certified
full-text index is maintained instead of reported pending.

The `action` an index build reports per index gains two values: `extended`
(a segment was added over new rows) and `merged` (segments were combined).
`built` and `refreshed` (rebuilt whole) remain. The field is additive in the
JSON output and in `BuiltIndex`.

Full-text coverage stops being reported as pending: a certified full-text
index is extended, merged or rebuilt like any other kind. A full-text index
without a certificate is left as it is and reported pending, as today, while
search refuses it (`FullTextIndexRebuildRequired`); `omnigraph index rebuild
--kind full-text` remains the analyzer migration and the way to replace an
externally created tokenizer. A vector property with no non-null vectors
stays pending.

Reads see more segments per index between merges. Each segment is searched
on its own: a BTREE probe per segment, a full-text posting lookup per segment
scored by one corpus-wide scorer, a nearest search per segment with its own
probe budget (the per-delta `maximum_nprobes` the engine already sets). The
merge policy bounds the number of segments. A vector segment built over a
fragment subset trains its own centroids, so approximate nearest results can
differ from those of one index trained on the whole table; exact answers do
not.

## Design

### Terms

An index is a name with one or more ***segments***, each covering a set of
fragments recorded in its `fragment_bitmap`. The indexes are the declared ones
and the identity and endpoint BTREEs every table carries. An index's ***covered
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
fragments, as today. On 13.0.0 compaction keeps coverage: on a stable-row-ID
table Lance's `Rewrite` apply moves each row-ID index's bitmap from the
replaced fragments to their rewritten ones
(`lance-table-13.0.0/src/transaction/manifest_build.rs:1071-1094`; BTREE,
full-text and vector indexes are row-ID indexes, `format/index.rs:142-153`),
and it refuses a rewrite group that mixes covered and uncovered fragments
(`transaction/index_maintenance.rs:657-689`). A compacted fragment is
therefore covered exactly when its sources were, and a fragment compacted from
fresh ones stays fresh. A BTREE segment the Lance 14 builder writes stores row
addresses and does not follow its rows this way
([The Lance fences](#the-lance-fences)). Deletions are masked by deletion
vectors.

The argument assumes that coverage leaves a fragment only when the fragment
leaves the manifest, so that the covered fragments hold every live copy the
index has indexed. Lance 13.0.0 also withdraws coverage from a fragment that
stays: on an in-place column rewrite (an update with `fields_modified`, a
`Merge` that swaps a column's data file, a `DataReplacement`;
`withdraw_in_place_rewrites`, `transaction/index_maintenance.rs:155`), and
when a compaction bakes in data overlays newer than the index
(`prune_overlay_stale_fields_from_indices`, `index_maintenance.rs:474`). The
fresh test below would call such a fragment fresh while an older segment
still holds entries for its rows. Graph writers do neither: keyed writes send
whole rows, so Lance's default merge mode rewrites rows
(`lance-13.0.0/src/dataset/write/merge_insert.rs:414-426`), and a
known-present update must stage `RewriteRows`
(`validate_known_present_update`); schema evolution's `Merge` adds fields
without swapping a data file; and no graph writer produces data overlays. A
writer that withdraws coverage in place must first extend the test, for
example with the persisted watermark under Alternatives.

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
so a fragment of new rows always passes; a fragment holding an updated row
always fails. When a segment names a fragment the current manifest no longer
holds, or a sequence cannot be read, every uncovered fragment counts as moved.
Graph compaction does not reach that arm: it commits its `Rewrite` with no
rewritten indexes (`TableStore::stage_compaction`), and the apply above moves
every row-ID bitmap to the rewritten fragments. Index work after a compaction
is therefore only the fragments those bitmaps still omit, read from the bitmap
Lance wrote like any other coverage. A fully covered index stays covered and
needs no coverage work: `foldable_index_lag` leaves alone a fully covered
scalar index and a fully covered vector index with one segment. It rebuilds a
vector index with more than one segment even when coverage is complete; under
this RFC the merge policy decides that case instead (past `K`, a rebuild). A
wrong "moved" costs the moved-fragment action (a merge for BTREE, a rebuild
otherwise); under the assumption above, a wrong "fresh" is impossible from
the sequences alone. An
index with a segment whose `fragment_bitmap` is absent (coverage unknown) is
left to explicit handling, as today: Lance cannot merge it, and unknown
coverage is not evidence of work.

### The decision per index

For each declared index of a table, from one pinned version, the reconciler
takes one of these actions, and stages every table's actions as one detached
`CreateIndex`:

| State | Action | Work |
|---|---|---|
| no segment with rows (missing, or full text with only the untrained declaration) | build | every row, once |
| uncovered fragments, all fresh | extend: one segment over them | the new rows |
| any uncovered fragment moved, BTREE | build one segment over every uncovered fragment, fresh and moved, then merge it with every segment in one filtered merge; publish only the merged segment | the uncovered rows, then every entry of the index; no scan of covered fragments |
| any uncovered fragment moved, full text or vector | rebuild whole | every row |
| more than the policy's segment count | merge (BTREE, full text) or rebuild (vector) | the merged segments' entries |
| full text without a certificate | pending, as today | none |
| vector property with no non-null vector | pending, as today | none |

The BTREE moved-row action covers every uncovered fragment in one input,
because a fresh fragment left out would stay uncovered and a second published
segment over the moved fragments is exactly the stale-entry case. It is safe
although its intermediate segment breaks the rule, because that segment is
never published: Lance's BTREE merge filters each source segment's entries to
the live row IDs of that segment's own fragments
(`lance-13.0.0/src/index/scalar/btree.rs:122`, through `index/append.rs:201`
`build_per_segment_filters` and `:103` `live_row_ids`), which drops the old
segment's entry for every moved row. The merged segment therefore satisfies
the rule's conclusion directly. Lance's 13.0.0 full-text merge applies no such
filter (`index/scalar/inverted.rs:1004`, `None` as the old-data filter), so full
text rebuilds. Its vector merge does filter each source
(`index/vector/ivf.rs:2473`), but it never joins segments trained with
different centroids (`select_steady_state_rebalance`, `ivf.rs:407`), and
subset builds train their own, so vector rebuilds too.

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
statistics. The constants are an unresolved question. The planner's
`IndexFact` records a name's union coverage but not its segment count, and
`K` segments cost `K` probes; the implementation adds the count to that fact,
so plan cost (request-aware plan cost, PR #996) prices the fan-out from the
same index inventory.

This RFC owns the segment policy of index maintenance. The served-maintenance
draft (#1168, closed on 2026-10-27) proposed one segment per catch-up; served
index work runs this decision function rather than a policy of its own.

### Full text

`TableStore::can_fold_index` stops excluding full text for certified
indexes. The reconciler verifies each segment's certificate before it extends
or merges the index, with the same immutable, session-cached proof search
uses; an index with any uncertified segment is left as it is and reported
pending. Two certificate rules change in RFC 0043:

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

Optimize applies the decision after its compaction link, as a second detached
table commit under the same graph publication; `index build` applies it on any
branch; read-only `index status` ([RFC 0046](0046-index-status.md), draft)
reports it without executing, so status and maintenance cannot disagree about
what an index needs. The publication stays index-only: it moves no row, so a
schema apply that counts row changes on a branch is unaffected.

### The Lance fences

Two Lance changes bear on this design, and Lance shipped them apart. Lance
14.0.0 (2026-10-26) contains lance#9481 and lance#10041 and neither
stale-entry fix. The stale-entry fixes, lance#9785 and lance#9872, are in
Lance's v15.0.0 betas and in no stable release. Each bump carries its own
obligation.

#### Lance 14.0.0: address-domain BTREE compaction

lance#9481 stores a BTREE segment's `ids` as row addresses from index format
version 1; an older segment stores row IDs
([BTREE format](https://lance.org/format/index/scalar/btree/)). Lance 13.0.0
writes version 0 (`lance-index-13.0.0/src/scalar/btree.rs:98`). The 14.0.0
builder writes version 1 on a stable-row-ID table (`BTREE_INDEX_VERSION =
BTREE_ROW_ADDR_DOMAIN_VERSION`, `lance-index/src/scalar/btree.rs:119-122` at
tag `v14.0.0`). lance#10041 remaps such a segment eagerly on compaction:
`IndexMetadata::supports_eager_remap` is true only for a BTREE with
`index_version >= 1` (`lance-table/src/format/index.rs:170-190`), and when a
table holds one, `rewrite_files` fills `RewriteResult.row_addrs` on a
stable-row-ID table too (`lance/src/dataset/optimize.rs:2656-2665`,
`:2939-2940`).

Current code refuses that result. `TableStore::stage_compaction` returns an
error when any task result carries `row_addrs` ("produced an address-style
rewrite; graph tables use stable row ids") and builds no `Rewrite`. The
refusal is not limited to this RFC's extension. The builder that writes
version 1 is the one today's `stage_index_fold` and
`stage_index_reconciliation` call, so on 14.0.0 the first build or whole
rebuild of any BTREE on a table, its identity BTREE included, makes every later
compaction of that table fail.

Removing the check alone does not keep coverage. With an empty
`rewritten_indices`, the 14.0.0 `Rewrite` apply drops the rewritten fragments
from a version-1 segment's bitmap (`drop_rewritten_fragments`,
`lance-table/src/transaction/manifest_build.rs:1090-1137` at `v14.0.0`;
OmniGraph commits no fragment-reuse index, so the deferred-remap arm does not
apply). A version-0 segment keeps the 13.0.0 bitmap move
(`recalculate_fragment_bitmap`), and so do full-text and vector segments, which
are not address-domain on that tag.

The bump to 14.0.0 therefore remaps. It runs Lance's public remapper
(`lance::dataset::index::DatasetIndexRemapperOptions`, an `IndexRemapper` of
`optimize::remapping`) over the tasks' row addresses, which writes each
affected version-1 segment under a new UUID without committing, and passes the
results as the `Rewrite`'s `rewritten_indices`. The apply installs them and
moves every other bitmap as on 13.0.0, so index work after a compaction stays
the fragments the bitmaps omit. The other two choices break this design's own
rules. Keeping the refusal stops compaction of every table once its first
BTREE is built on 14.0.0. Dropping coverage is correct but moves the work to
the next build, which indexes the compacted rows again (by the BTREE moved-row
merge over every entry whenever the fresh test calls those fragments moved),
while queries scan them in between. That is the per-compaction index work the
compaction paragraph above rules out. The remap reads and rewrites each
affected segment, as Lance's own `commit_compaction` does.

Two more obligations fall on that bump. Lance 14's BTREE merge refuses
segments that disagree on the domain (`BTreeIndex::merge_segments`), so an
index whose version-0 segments would be merged with a version-1 segment is
rebuilt whole instead, which leaves one version-1 segment. And the rule's
argument cites the 13.0.0 row-ID prefilter, so the bump reruns the rule's
guards against version-1 segments. The remap is needed whether or not any
phase of this RFC has shipped, because today's whole rebuild already writes
the new segment.

#### The moved-row fence: Lance 13.0.0 until lance#9785 and lance#9872

The fresh test, the BTREE moved-row merge and the full-text whole rebuild for
moved rows exist because 13.0.0's scalar and full-text search, and its
full-text merge, do not restrict a segment to its own fragments. The bump that
contains lance#9785 and lance#9872 removes them: segments over moved
fragments become safe for every kind, so the reconciler extends over every
uncovered fragment and merges under the same policy. That bump is the one
that turns `index_delta_segment_serves_stale_entries_of_an_updated_row` red,
and it is not 14.0.0. Deleting the moved-row path there is safe because
compaction keeps coverage: row-ID segments move their bitmaps, and version-1
BTREE segments are remapped from the 14.0.0 bump on. Extension over appended
rows, the merge policy and full-text certification are not part of either
fence.

## Invariants

- **Graph publication and snapshots (2, 3, 4).** Unchanged: one pinned
  snapshot plans every table, artifacts are built before the writer gates,
  authority is revalidated under them, and every table publishes in one
  manifest commit.
- **Crash convergence (5).** New segments, the intermediate BTREE segment
  of a moved-row merge, and from the 14.0.0 bump the remapped BTREE segments
  of a compaction, are index directories that no manifest names until
  publication (the intermediate one never); the collector's orphan sweep
  reclaims an unnamed `_indices/<uuid>` after its seven-day age, as it does
  for any staged index today.
- **Physical acceleration is derived state (7).** The rule is what makes a
  partly maintained index answer as a full build does. Coverage chooses
  maintenance work, never answers. Index work stays in explicit
  reconciliation, never on the content-write path.
- **Integrity failures are loud (8).** An unknown fresh test takes the
  moved-fragment action, never a guess; a coverage overlap fails the run
  before staging.
- **Bounded work (11).** Maintenance after appends scales with the rows
  appended, and the merge policy bounds both rewrites per row and query
  fan-out. Moved rows cost more until the fence is removed: a BTREE merge
  reads every entry of the index, and full text and vector rebuild from every
  row, which is what every lagging index costs today.
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
- **Wait for the Lance bump that contains lance#9785 and lance#9872.** Segment
  ownership in scalar and full-text search makes segments over moved
  fragments safe. The fixes merged on 2026-10-23 but are in no release
  OmniGraph pins. Extension over appended rows is the durable part and is
  useful after the bump too; only the moved-row fence waits for it.
- **Use Lance's `optimize_indices`.** It commits linearly and has no
  certificate hook; incompatible with detached-only tables.
- **Defer rebuilds behind a threshold.** Leave a moved tail scanned until it
  exceeds a fraction of the table. It bounds amortized cost but lets a
  selective query scan a large tail, and it does not help appends, which are
  the common case. It may complement this design for full text and vector
  (unresolved).
- **Persist a row-ID watermark per segment** (a file in the segment's
  inventory, as the full-text certificate is). It would keep the fresh test
  exact however coverage was withdrawn, including in-place column rewrites
  and overlay pruning, at the cost of a new artifact to verify and collect.
  The sequences suffice while coverage leaves a fragment only with the
  fragment (see The rule), as compaction does for row-ID indexes.
- **Allow vector segments over moved fragments**, relying on vector search's
  per-segment filter. It saves vector rebuilds after updates but splits the
  rule by kind; not worth it until updates of vector rows are measured to
  matter.

## Evidence and tests

Lance surfaces reviewed at 13.0.0: the index, BTREE, full-text, vector,
fragment-reuse and distributed-indexing pages; the row-ID lineage and
transaction specifications; and the source cited above. At tag `v14.0.0`: the
BTREE format version, `rewrite_files`, `commit_compaction` and its remapper,
and the `Rewrite` apply. The published BTREE page already describes the
version-1 row-address `ids`. The row-ID lineage page says secondary indexes
reference row addresses and that stable row IDs in indexes are in progress;
the 13.0.0 source paths cited here (`do_create_deletion_mask_row_id`,
`build_stable_row_id_filter`) work in stable row IDs, so the guards below pin
the code, not the page.

New `lance_surface_guards.rs` guards, each red when the fact changes:

- a subset segment over fresh fragments answers BTREE equality and range,
  full-text search and BM25 scores exactly as a full build over the same rows;
- two segments of one name with disjoint coverage, staged through OmniGraph's
  own `CreateIndex`, open and answer as one index;
- the shared prefilter admits a row ID only through its current live copy
  (an updated row's old entry is masked while its new fragment is uncovered);
- the BTREE merge drops a moved row's old entry; the full-text merge keeps it
  (a negative guard, red at the bump that contains lance#9872);
- a certificate written on a subset build and on a certified merge verifies;
- a compaction `Rewrite` with no rewritten indexes keeps full-text and vector
  indexes covering the rewritten fragments (extending the scalar assertion of
  `compacting_a_chosen_fragment_drops_the_bytes_of_dropped_columns`); green
  across 14.0.0;
- the BTREE the pinned builder writes is version 0, `rewrite_files` returns no
  `row_addrs` for its table, and the same `Rewrite` keeps it covering the
  rewritten fragments. A separate assertion, because it goes red at 14.0.0
  for a reason that does not apply to full text or vector. The 14.0.0 bump
  replaces it: a newly built BTREE is version 1 and loses that coverage under
  an empty `rewritten_indices`, a version-0 segment keeps it, and a segment
  remapped through `rewritten_indices` keeps it.

Engine owners (`integration/maintenance.rs`, `integration/branching.rs`):
appends extend each index, including the identity and endpoint BTREEs,
without a rebuild; an append and an update in one run produce one merged
BTREE segment covering both fragments (equality on the old value returns
nothing) and rebuild full text and vector;
after a compaction of covered fragments a build does no index work, and after
a compaction of fresh fragments it extends over them; a segment that names a
fragment the manifest no longer holds, and an unreadable sequence, take the
moved-fragment action; a segment without a coverage record is left alone and
reported; the segment count stays within `K`. A
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
5. `index status` through RFC 0046, amending its rows that name optimize as
   the only reconciler and send full-text coverage to the rebuild command.

Each phase keeps the rule and falls back to a whole rebuild for anything it
does not handle, so each ships alone. `implementation` advances per phase.
The 14.0.0 compaction remap is not a phase: it belongs to the pull request
that bumps Lance to 14.0.0, whichever phases have shipped by then.

## Unresolved questions

- The merge policy's constants: `K`, and the size ratio that defines a tier.
- Whether optimize should defer full-text and vector rebuilds for a small
  moved tail, and how its report states that, given that `index build`
  promises full coverage.

## Decision log

- 2026-10-25: drafted as the second step of #840. PR #1150 (open) puts the
  `index build` contract on today's whole rebuilds; this RFC changes their
  cost, and for certified full text their outcome.
- 2026-10-26: review. The summary no longer claims an unchanged contract for
  full text; an index without a certificate is reported pending, not refused.
  The decision table gains the mixed case: when any uncovered fragment is
  moved, one BTREE merge input covers every uncovered fragment. The identity
  and endpoint BTREEs are named. The upstream fixes (lance#9785, lance#9872)
  are recorded, and the moved-row half is marked as the 13.0.0 fence.
  Invariant 11 states the moved-row costs. #1150 is in `blocked_on`; the
  segment count joins `IndexFact`; the vector rebuild reason is the separate
  IVF model, not a missing filter.
- 2026-10-27: second review. Compaction keeps row-ID index coverage, because
  Lance's `Rewrite` apply moves the bitmap, so index work after a compaction
  is only the fragments the bitmaps omit; the earlier text had every
  compaction take the moved-row action. The rule's assumption, that coverage
  leaves a fragment only with the fragment, is stated with the two Lance
  applies that break it and why graph writers do neither. The fence names
  lance#9481 and lance#10041, which change BTREE compaction coverage at the
  same bump. This RFC owns the segment policy.
- 2026-11-08: third review. Lance 14.0.0 contains lance#9481 and lance#10041
  but not lance#9785 or lance#9872, so the fence splits in two. On 14.0.0 a
  newly built BTREE segment is address-domain (version 1), `rewrite_files`
  returns `row_addrs` for its table, and `TableStore::stage_compaction`
  refuses that result before it builds the `Rewrite`; today's
  `stage_index_fold` uses the same builder, so the refusal does not wait for
  extension. The 14.0.0 bump remaps such segments through `rewritten_indices`
  rather than keeping the refusal (no compaction) or dropping coverage (a
  BTREE merge after every compaction). A version-0 segment, full text and
  vector keep the 13.0.0 bitmap move; the compaction guard splits so the
  BTREE assertion alone goes red at 14.0.0. The moved-row fence stays for the
  later bump. The fully covered case names the scalar index and the
  one-segment vector index. #1150 merged in v0.15.0, so `blocked_on` is empty.
