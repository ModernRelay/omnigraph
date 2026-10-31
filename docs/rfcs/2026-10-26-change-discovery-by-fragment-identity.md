---
rfc: "2026-10-26-change-discovery-by-fragment-identity"
title: "Change discovery by fragment and row identity"
track: maintainer
status: draft
implementation: in-progress
authors:
  - Ragnor Comerford
created: 2026-10-26
updated: 2026-10-29
discussion: https://github.com/ModernRelay/omnigraph/pull/1171
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Change discovery by fragment and row identity

> A term set in ***bold italics*** is being defined at that exact spot; it is
> plain text everywhere after.

## Summary

Commit changes, the change feed and the graph diff compare a changed type's two
pinned table versions. [RFC 0030](0030-cdc-time-travel.md) prunes that
comparison only when the two pins are one proving transaction apart; every
wider window took the ***exact ordered merge***, the walk that sorts every
live id of both versions, hydrates every row and compares them, so a diff of
ten changed rows read both whole tables.

This RFC adds one discovery rule in front of that merge, for a window with no
adjacency proof. Pair the two manifests' fragments by fragment id and skip the
pairs whose data files, overlay files and deletion file are identical. For the
rest, decode each fragment's inline ***row identity***, the stable `_rowid`
and the `_row_last_updated_at_version` stamp Lance keeps per physical row, with
the deletion vector applied, and pair the live rows of the two versions by
`_rowid`. A row id live on both sides with equal stamps is one unchanged row and
is never read. Every other row id is a candidate, and each side then runs the
exact ordered merge over its touched fragments narrowed to its candidate ids.
Results, order, page tokens, feed cursors, errors and authorization do not
change; nothing is persisted; the exact merge stays as the fallback and as the
oracle.

One Lance 13 fact bounds the rule: the stamp a commit writes is its read
manifest's version plus one, not the detached version the commit receives, so
two sibling first commits after a fork stamp a row they both update alike. Stamp
equality therefore proves two copies to be one unchanged row only where the
`from` side lies on the `to` side's first-parent chain, which the diff decides
from lineage the `to`
branch's coordinator already holds. A diff of two diverged heads takes the exact
merge.

The `change_lineage` session setting (`off`, `on`, `verify`; `on` by default,
`verify` in a debug build) selects the rule for all four readers. Measured on
the issue's fixture, ten updates on a table of 4,000, 20,000 and 100,000 rows:
both wider diffs read 14 table-data requests and 27,152 bytes at every extent,
down from 277 requests and 6.9 MB at 100,000 rows, and flat across a compaction.

## Motivation

[RFC 0030](0030-cdc-time-travel.md) §8.2 and §14 let a changed type prune its
comparison only when the `to` pin is one proving transaction past the `from`
pin. Every other changed type takes the exact ordered merge. The user guide
stated the consequence: cost grows with the size of each changed type, and
apart from a single commit every changed type is compared in full.

Under [detached-only tables](2026-09-21-detached-only-tables.md), each write is
its own detached commit, so a pin is one transaction past its parent only for
a single write. The shapes people read are wider:

- a branch that wrote a type in more than one commit;
- a three-way merge, which publishes a chain of insert, update and delete
  chunks, each a detached commit reading the one before
  (`crates/omnigraph/src/exec/merge.rs`, the three-way route);
- a two-point diff of two diverged heads.

Measured on 2026-10-25 against a hosted server running `0.14.1-edge+7711e4a`
on S3-compatible storage, with client wall time
([#969](https://github.com/ModernRelay/omnigraph/issues/969#issuecomment-6396724962),
[#1157](https://github.com/ModernRelay/omnigraph/issues/1157)):

| Read | Changes | Time |
|---|---:|---:|
| `GET /diff`, merge base to a branch with 9 commits | 350 | 37.4–62.1 s |
| The same diff with `type=Artifact` (100,058 rows, written by 3 of the commits) | 5 | 47.6 s |
| The same diff with any other single type | 11–28 | 7.9–9.6 s |
| `GET /commits/{id}/changes` on a merge commit, 3 changes on `Artifact` | 6 | 17.8 s |
| One change-feed page covering 4 commits | 706 | 51.7 s |

OmniGraph Studio reads a branch review through the change feed with a 60 s
request timeout, so these reviews fail or succeed depending on cache state.

Reproduced deterministically on
[#969](https://github.com/ModernRelay/omnigraph/issues/969)'s fixture with the
GQT runner's `--measure` (a `Person` type with a key, an integer and a 256-byte
string, seeded with 4,000, 20,000 and 100,000 rows, ten identical updates on a
branch): the two-commit diff and the branch review read 17, 57 and 277
table-data requests and 287,492, 1,405,044 and 6,922,180 bytes, growing with
the table, while the adjacent diff stayed at 12 requests and 83,106 bytes.

Merge had the same problem and fixed it without new storage.
[#540](https://github.com/ModernRelay/omnigraph/pull/540) replaced the merge's
full scans with candidate discovery from fragment and deletion metadata. The
first draft of this RFC proposed that rule alone for change discovery. It
leaves one window table-sized that the issue's shapes meet: a window across an
`optimize`, where compaction rewrites every fragment's data files, so every
live row of every rewritten fragment is a candidate on both sides. Row identity
closes it, because compaction carries each row's `_rowid` and stamp into the
rewritten fragment unchanged.

## User and operational behavior

Results do not change. Every surface returns the same entity changes in the
same RFC 0030 §4.4 order, with the same page token and cursor contents, the
same errors and the same authorization.

Cost changes. For a window with no adjacency proof, a changed type costs:

- one pass over the fragment lists of its two manifests, which opening both
  versions already loads, and the decode of the inline row-id and stamp
  sequences of the fragments whose files differ;
- one deletion-vector read per such fragment that has a deletion file;
- the exact ordered merge over each side's touched fragments, narrowed to the
  rows whose stamp differs or that are live on that side alone, so the rows
  read are the changed rows plus the id column of the fragments that hold
  them.

Unchanged fragments are never read. A window across a compaction stays flat:
the rewritten fragments differ by files, their rows pair by `_rowid` with equal
stamps, and none is read. Two shapes stay larger than the number of changes,
and the user guide states both:

- a window across an overwrite costs the whole type, since an overwrite
  allocates fresh row ids;
- a two-point diff of two diverged heads takes the exact merge (Design, The
  ancestry restriction).

The cost paragraph in `docs/user/branching/changes.md` says this.

The `change_lineage` setting has `request` scope and the process default
`OMNIGRAPH_CHANGE_LINEAGE`:

- `on` pairs rows by identity where the rule applies and takes the exact merge
  elsewhere. It is the definition's default.
- `verify` runs both and refuses the page on the first change where they
  disagree (Design, Verify). It is the default of a debug build, as
  `merge_lineage` is.
- `off` takes today's path for every window, the ancestry answer included: the
  adjacency proofs, then the exact merge, with no lineage read for the rule it
  does not run. It stays a performance fallback, since the adjacency path is
  kept.

A value outside the three spellings is refused by the settings registry, as
for `merge_lineage`; the kill switch cannot misfire on. A GQ session carries
the setting through `set change_lineage = …`, which covers the CLI, embedded
use, the query route's `settings` field (`SettingsRequest.change_lineage`,
applied before the source's own `set` lines) and the `branch diff` statement
that route serves. The REST routes `/diff`, `/branches/diff`, `/changes` and
`/commits/{id}/changes` run under the process default; their `set=` parameter
stays validated and selects nothing, as before this RFC.

## Design

### Inherited from Lance

OmniGraph relies on these Lance 13.0.0 behaviors, pinned by
`lance_surface_guards::row_identity_discovery_stamps_row_ids_and_rewrites_lance_13`
through the engine's own writers and recorded in the "Row version stamps" row
of `docs/dev/lance.md`.

- A data file is never rewritten behind its name. Data files have UUID names,
  so the same resolved path in two manifests means the same bytes.
- Every graph-table write keeps a fragment's `_rowid` and
  `_row_last_updated_at_version` sequences inline in the manifest
  (`RowIdMeta::Inline`, `RowDatasetVersionMeta::Inline`), one entry per
  physical row, the deletion vector masking the deleted ones. Lance writes them
  to a file only under its spill configuration, which no graph write sets.
- A `merge_insert` update keeps the row's `_rowid` and stamps it with the read
  manifest's version plus one (`src/dataset/write/merge_insert.rs` passes
  `manifest.version + 1` to the fragment update; an append is built with
  `current_manifest.version + 1` in `src/dataset/transaction.rs`). The stamp is
  assigned before the detached commit receives its random version, so two
  sibling first commits over one parent stamp a row they both update with the
  same value.
- A deletion-only commit restamps nothing.
- Compaction (`Operation::Rewrite`) carries every live row's id and stamp into
  the rewritten fragments.
- An overwrite allocates fresh row ids.

### Where the rule sits

`changes::candidate_scan::interval_candidate_plan` keeps RFC 0030's ladder and
adds one rung:

1. Both pins stable-row-id tables of one identity on one native branch, else
   the exact merge.
2. An adjacency proof (the Stamp or Commit proof of RFC 0030 §14): the one
   transaction is read and the existing `Upserts` or `Deletes` plan is built,
   as today.
3. No adjacency proof, `change_lineage` not `off`, and `from` on `to`'s
   first-parent chain: the row-identity plan below.
4. Otherwise the exact ordered merge.

### The discovery rule

`changes::candidate_scan::row_identity_plan`, for one changed type lifetime
with pinned datasets `from` and `to`:

1. Pair fragments by fragment id across the two manifests.
2. Same resolved data and overlay files, same deletion file: skip. Nothing is
   read. (`fragment_data_identical` and `deletion_file_identical`, the merge
   classifier's tests since
   [#540](https://github.com/ModernRelay/omnigraph/pull/540).)
3. For every other fragment, on either side: decode its inline `_rowid` and
   `_row_last_updated_at_version` sequences, require both lengths to equal the
   fragment's physical row count, read its deletion vector when it has a
   deletion file and mask both sequences with it. The result is the fragment's
   live `(_rowid, stamp)` pairs. The fragment is a ***touched fragment*** of
   its side.
4. Pair the live rows of the two sides by `_rowid`. A row id live on both
   sides with equal stamps is one unchanged row: skipped. A row id live on both
   sides with different stamps is a candidate on both. A row id live on one
   side only is a candidate on that side.
5. Each side runs the exact ordered merge of RFC 0030 §4.2 over its touched
   fragments, with the scan narrowed by a structured `_rowid IN (...)`
   expression over its candidate ids (DataFusion's `in_list`; no string is
   built) unless the candidates are every live row of that side. Order,
   images, equality, grouping by `id` and paging are the merge's own.

Any miss sends the type to the exact ordered merge: a sequence absent, spilled
to a file, undecodable or shorter than the fragment; more than 8,388,608 live
rows whose stamps the interval would hold; more than 65,536 candidate ids on
one side.

### Why the candidates are complete

Call a live row ***shared*** when its `_rowid` is live on both sides with equal
stamps. A commit that changes a row's bytes stamps the row with its own
version, a deletion restamps nothing, and a rewrite carries both id and stamp,
so equal stamps on one `_rowid` mean the same commit last wrote both copies.
Where the `from` side's lineage is a prefix of the `to` side's, one commit has
one place in that lineage, so the two copies are the same bytes. A shared row
is therefore unchanged and may be skipped; every other live row is a candidate
on its side, and the exact merge over the candidates decides insert, delete,
update or nothing by the `id` it reads, exactly as the unnarrowed merge does.

Pairing is by `_rowid`, whose uniqueness per version is Lance's own row-id
protocol, not by the unenforced primary key. The `id` is read for order and
output as today, so a duplicate `id` degrades the output, never the pruning.

### The ancestry restriction

The completeness argument needs one lineage. Across a fork it fails: two
sibling branches that each update one row in their first commit after the fork
both stamp it with the fork's version plus one, so the pair looks shared and
the update is lost (`changes::cross_branch_diff_detects_same_length_blob_update_between_sibling_branches`
found this on the first implementation). The rule therefore applies only when
`from` lies on `to`'s first-parent chain:

- the branch form when its merge base is on the source head's first-parent
  chain. A source that merged its target holds the base as the merge commit's
  second parent, off that chain; two sibling first commits after the fork that
  each inserted a row gave both rows one `_rowid` and one stamp, so the review
  takes the exact merge there
  (`issue_969_branch_review_after_a_merge_from_target.gqt`);
- a two-point diff whose `to` names a branch whose chain holds `from`;
- a commit page and a feed commit always, since their window is parent to
  child.

Both diff forms decide it from lineage already held, with no commit listing.
The branch form walks the source's captured commit graph, which computing the
merge base loaded. The two-point form (`Omnigraph::diff_from_is_ancestor`)
takes `to` resolved to `from` itself as an ancestor; otherwise the `to`
branch's `GraphCoordinator` answers `first_parent_chain_holds`, from its head
and buffered commits with no read, then a first-parent page walk down to
`from`'s generation. The coordinator is the handle's own when `to` is its
branch and the merge-authority cache's otherwise. A `to` spelled as a commit
id, a `from` of unknown generation, or a commit of the chain below the
buffered commits that `__history` no longer holds answers `false` and takes
the exact merge, which needs no lineage.

Drop the restriction at the Lance bump whose stamp is the commit's own version;
diverged heads then pair directly.

### Verify

Under `verify`, an interval the row-identity plan serves also runs the exact
ordered merge of the same interval, and the two emitters are compared change
by change (`EmitSource::Verified`, `verify_emit_agreement`): the same end, the
same operation, the same `id`, and equal images through the enumerator's own
`rows_equal`. The first disagreement refuses the page with an internal
manifest error that names the type and both changes and says how to run the
exact merge alone (`set change_lineage = off`); the HTTP routes return it as
their `internal` error. Nothing is advanced: no page is returned, so no token
or cursor moves.

### Observability

Two counters in `omnigraph-core`'s request instrumentation name the route an
interval took, `change_intervals_row_identity` and
`change_intervals_exact_merge`, beside the existing candidate counters. The
cost tests assert them, so a window that silently fell back fails the gate
that expected the flat line.

### Out of scope

- The change feed's work ceiling and the commit-ceiling request parameter
  ([#1157](https://github.com/ModernRelay/omnigraph/issues/1157)). Discovery
  knows each interval's candidate count before any row is read, which a
  ceiling could consume; that is a separate change to the feed's page
  contract.
- Commit-id lookup ([#972](https://github.com/ModernRelay/omnigraph/issues/972)).
- Change sets recorded on the commit
  ([RFC 0068](0068-graph-commit-record.md)). Nothing measured here needs them.
- Removing the adjacency candidate path. It stays the first rung and `off`'s
  behavior.
- Cross-request caches of discovery results or images.

## Invariants

- **1, respect the substrate.** Discovery reads Lance manifest and fragment
  metadata, the inline row-id and stamp sequences, deletion vectors and
  fragment-scoped scans, through the surfaces the merge classifier and the
  exact merge already use. It adds no parallel storage machinery.
- **3, one coherent view.** Discovery and images read only the two pinned
  versions the enumerator already opens; the ancestry answer reads lineage the
  `to` branch's coordinator holds.
- **6, stable identity.** Types pair by stable graph identity, as today.
  `_rowid` locates a physical row and decides what to read; entities are keyed
  by the `id` read at the pinned version, never by a row id or a position.
- **7, physical acceleration is derived.** Correctness does not depend on an
  index, on fragment count or on layout. No `__id` index is required.
- **8, integrity failures are loud.** `verify` refuses the page on any
  difference from the exact merge, with the type and both changes named.
- **9, typed semantics.** The candidate narrowing is a structured expression,
  not a built string.
- **11, bounded and observable.** The candidate cap, the held-stamps cap and
  the existing page ceilings bound each request; the route counters make the
  path observable. Hot-path work scales with the changed rows plus the
  fragment lists and sequences of the changed types, not with table rows.
- **12, one source of truth.** Nothing is persisted. Every fact comes from the
  two pinned manifests, their files and the held lineage.
- **13, evidence.** The Lance facts are guards; the cost claims are tests with
  route assertions; `verify` makes the existing suites the oracle.

Deny-list: no logical precondition depends on index coverage, fragment count
or layout; no `IN (...)` string is generated; no parallel truth is maintained.

RFC 0030 §4.3 forbids four delete shortcuts. This design uses none of them. A
deletion vector only masks the stamp sequence; which rows are deletes is
decided by the exact merge over the candidates, which reads the `id` at the
`from` pin as today. No key is inferred from a fragment id or row address, no
transaction predicate is parsed, no tombstone is persisted.

RFC 0030's decision log carries the amendment of 2026-10-28: §4.2's "an
interval wider than one version falls back before any transaction read" still
holds for the transaction read, and the row-identity rule runs after it; the
§9 cost evidence row "every changed type is compared in full" for a
multi-commit window no longer holds.

## Compatibility and reversibility

There is no storage, wire, token or result change. A page token minted by one
build continues on another, since contents and order are unchanged. The new
setting, its environment variable and the `SettingsRequest.change_lineage`
field are additive. `off` restores today's path exactly, adjacency proofs
included. Reverting the code leaves nothing behind.

## Alternatives

1. **Fragment identity alone**, this RFC's first draft: skip identical
   fragments, take the deletion-vector difference of fragments whose deletion
   file changed, take every live row of a fragment whose files differ. It is
   the minus-one-mechanism design, without the stamp sequences, and it is
   change-sized for a `merge_insert` update, whose new fragment holds only
   changed rows. It fails the compaction window: `optimize` rewrites every
   fragment's files, so every live row of every rewritten fragment is read on
   both sides. The cost gate
   `graph_diff_across_a_compaction_reads_data_flat_in_table_extent` is that
   scenario; under this rule its data reads are flat (3 at 4,096 rows). Rules
   1 and 2 of that draft survive as the first skip.
2. **Chain proof**, the recommendation on
   [#969](https://github.com/ModernRelay/omnigraph/issues/969): walk
   `read_version` links from `to` back to `from`, proving each link and taking
   the union of touched fragments. It reads one transaction file per link, up
   to 1,024 per merge, and breaks across compaction, overwrite, any unproven
   link, and once cleanup reclaims an intermediate merge chunk no graph commit
   pins.
3. **Change sets recorded at commit**
   ([RFC 0068](0068-graph-commit-record.md), and a candidate in the
   detached-only RFC). A second record beside the rows that invariant 12 has to
   police, plus a format change, for a cost this rule reaches from existing
   metadata.
4. **Writer-set row-lineage versions**, an upstream Lance option that stamps
   rows with the published version. It waits on a Lance release and a pin
   bump. It is also the bump at which this RFC's ancestry restriction is
   dropped.
5. **Ordered stamp tests against the fork version** (`stamp > fork`, `created
   <= fork`): unsound on graph tables, since the stamp is the read version
   plus one and both lineages after a fork count from the same base.
6. **A live `_rowid` set difference alone**: unsound, since an update keeps
   its `_rowid`, so an updated row is live on both sides.
7. **Larger pages, longer client timeouts, a wall-clock page budget, a plan
   cache across pages.** Each hides or defers the cost; the first page still
   compares whole tables, and a wall-clock budget costs replay its
   determinism.
8. **Do nothing.** Branch review, merge-commit inspection and multi-commit
   diffs stay table-sized, and clients time out.

## Evidence and tests

- **Lance guard.**
  `lance_surface_guards::row_identity_discovery_stamps_row_ids_and_rewrites_lance_13`
  pins, through the engine's writers on Lance 13.0.0: inline sequences on
  every fragment of every graph write; an update's stamp equal to the read
  version plus one and unequal to the detached version; the sibling-commit
  collision; a deletion-only commit restamping nothing; compaction carrying
  every live row's id and stamp. A bump that moves one of these is a design
  review of the rule, not a test to weaken.
- **Cost owner**, `crates/omnigraph/tests/integration/changes_cost.rs`, each
  asserting the route counters:
  - `graph_diff_over_several_commits_reads_data_flat_in_table_extent`: a
    two-point diff and a branch review over several commits, at 64 and 1,024
    rows per load, data reads flat, route row identity;
  - `graph_diff_across_a_compaction_reads_data_flat_in_table_extent`: four
    loads, `optimize` (every fragment rewritten, asserted), ten updates, a
    two-point diff across the window at 256 and 4,096 rows, the exact ten
    updates on one page, data reads flat;
  - `graph_diff_of_diverged_heads_takes_the_exact_merge`: diverged heads take
    the exact merge; the same two-commit window takes the exact merge under
    `off` and row identity under `on` and `verify`, with the same rows, and
    `off` opens no coordinator for the ancestry answer.
- **Missing history.**
  `integration/changes.rs::graph_diff_takes_the_exact_merge_below_a_missing_history_block`:
  a `__history` block removed below the buffered commits; the two-point diff
  across it answers `false` to the ancestry question and takes the exact merge.
- **Served routes.**
  `data_routes::changes_routes_run_under_the_process_change_lineage`:
  `OMNIGRAPH_CHANGE_LINEAGE=off` takes the exact merge on `GET /diff`,
  `GET /branches/diff` and the `branch diff` statement, and the query route's
  `settings` field selects `on` over it, read through the route counters.
- **Logic.** `crates/omnigraph-gqt/cases/issue_969_graph_diff_wider_windows.gqt`,
  on the engine, DST seeds 0 and 42 and the served runner: a window that
  folds two updates of one row, a delete, a delete and reinsert, an insert and
  a keyed edge insert; `off` answering the same rows; the diff of two sibling
  heads that updated one row alike; a window from a commit before the fork
  into a sibling. `issue_969_branch_review_after_a_merge_from_target.gqt`, the
  same targets: main and feature each insert one row in their first commit
  after the fork, feature merges main, and the review of feature into main
  reports feature's insert alone, as the two-point diff from main's head does.
- **Oracle.** `change_lineage = verify` is the debug default, so every change
  read in the engine's `changes` suites, the GQT corpus and the DST suite runs
  both emitters and fails on any difference.
- **Storage-cost golden**, `crates/omnigraph-dst/cost_table.txt`, the
  `DiffPage*` and `BranchDiffPage*` rows (a two-commit window on a reopened
  handle, under `verify`): the branch form's first page and continuation each
  gain two reads, the row-identity emitter's candidate reads beside the exact
  merge's; the two-point form's first page gains 14 reads and 3 listings, one
  coordinator open of the `to` branch for the ancestry answer, and its
  continuation loses 6 reads and 3 listings, since that coordinator then
  serves the side resolution from the cache.
- **Measured.** The issue's fixture under `on`, GQT `--measure` on the
  in-memory DST store: both wider diffs read 14 table-data requests and 27,152
  bytes at 4,000, 20,000 and 100,000 rows, from 17/57/277 requests and
  287,492/1,405,044/6,922,180 bytes; the adjacent diff unchanged at 12 and
  83,106. Total requests of the branch form 53/55/59 (from 56/98/322); of the
  two-point form 67/70/74 (from 54/96/320), the difference to the branch form
  being the coordinator open above.
- **Hosted re-measure.** The five reads in Motivation after deployment.

Lance reviewed: 13.0.0, the workspace pin. Surfaces: `Fragment` (`files`,
`overlays`, `deletion_file`, `physical_rows`, `row_id_meta`,
`last_updated_at_version_meta`), fragment deletion-vector reads, and
fragment-scoped scans with a `_rowid` filter.

## Rollout

The rule, the setting, the guard, the cost gates, the logic case, the user
docs and the RFC 0030 amendment land in one change beside this RFC; the
`implementation` field moves to `complete` when it merges. Release builds
default to `on`, debug builds to `verify`. Before that change, release builds
behave as today.

Follow-ups, each its own change:

1. Fold the two-point form's two opens of the `to` branch into one, so its
   first page costs what the branch form's costs (Unresolved question 1).
2. The feed's work ceiling and commit-ceiling parameter
   ([#1157](https://github.com/ModernRelay/omnigraph/issues/1157)).
3. Drop the ancestry restriction at the Lance bump named in Design, The
   ancestry restriction.

## Unresolved questions

1. **The two-point form's second coordinator open.** On a fresh handle the
   side classification resolves the `to` branch cold and the ancestry answer
   opens it again into the merge-authority cache (14 reads and 3 listings in
   the golden). Classify a non-current `to` through that cache, or accept the
   cost. Decided by the implementer before the change merges.
2. **`set=` on the REST change routes.** The routes validate `set=` and
   select nothing, so `change_lineage` reaches them only as the process
   default. Decided at the Session settings RFC's step that makes the routes
   read their `set=` values.
3. **The feed bounds of #1157.** Whether they take a separate RFC or an
   amendment here. Decided by the author once this RFC is accepted.

## Decision log

- 2026-10-26: drafted from the hosted measurements on
  [#969](https://github.com/ModernRelay/omnigraph/issues/969),
  [#972](https://github.com/ModernRelay/omnigraph/issues/972) and
  [#1157](https://github.com/ModernRelay/omnigraph/issues/1157), and the merge
  classifier of [#540](https://github.com/ModernRelay/omnigraph/pull/540).
- 2026-10-28: rewritten from the implementation. Fragment identity stays as
  the first skip; discovery pairs live rows by `_rowid` and skips equal stamps,
  which keeps a window across a compaction flat; the exact ordered merge over
  the candidates replaces the first draft's positional image reads and
  deletion-vector difference, so RFC 0030 §4.3 is untouched; the rule is
  restricted to a `from` on `to`'s first-parent chain after the sibling-stamp
  collision surfaced in the existing cross-branch update test; the feed's work
  ceiling and commit ceiling moved out of scope; the adjacency path stays, so
  `off` is today's path.
- 2026-10-29: the branch form checks its merge base against the source head's
  first-parent chain instead of assuming it, since a source that has merged
  its target holds the base as a second parent and its sibling inserts would
  pair; the REST change routes and
  the `branch diff` statement read `change_lineage` from the process default
  and the request session; a chain commit `__history` no longer holds answers
  `false`; `off` skips the ancestry answer.
