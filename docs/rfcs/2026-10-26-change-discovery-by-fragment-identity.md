---
rfc: "2026-10-26-change-discovery-by-fragment-identity"
title: "Change discovery by fragment identity"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford
created: 2026-10-26
updated: 2026-10-26
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: Change discovery by fragment identity

## Summary

Commit changes, the change feed and the graph diff find a changed type's
candidate rows by comparing the fragment metadata of its two pinned table
versions, the way branch merge has done since
[#540](https://github.com/ModernRelay/omnigraph/pull/540). A fragment whose
data files and deletion file are the same in both versions holds the same live
rows and is never read. A fragment present in one version only, or backed by
different data files, contributes its live rows. A fragment whose only
difference is its deletion file contributes the rows in the deletion-vector
difference. The candidates then go through the typed equality the enumerator
already uses. The exact ordered comparison stays the fallback and becomes the
test oracle.

Discovery stops requiring the two pins to be one transaction apart. A diff
across several commits, a merge commit whose target moved, and a two-point
diff of diverged heads then cost what changed between their pins, plus a pass
over both manifests' fragment lists. Nothing new is persisted. Results, order,
page tokens, feed cursors, errors and authorization do not change. A
request-scoped setting, `change_lineage`, selects `on`, `off` or `verify`, as
`merge_lineage` does for merge.

The change feed also gains a deterministic work ceiling per page, and its
commit ceiling becomes a request parameter.

## Motivation

[RFC 0030](0030-cdc-time-travel.md) §8.2 and §14 let a changed type prune its
comparison only when the `to` pin is one proving transaction past the `from`
pin. Every other changed type takes the exact ordered comparison: it sorts
every id of both versions, hydrates every row in full and compares them. The
user guide states the consequence: cost grows with the size of each changed
type, and apart from a single commit every changed type is compared in full.

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

Merge had the same problem and fixed it without new storage.
[#540](https://github.com/ModernRelay/omnigraph/pull/540) replaced the merge's
full scans with candidate discovery from fragment and deletion metadata, and
measured 9.5x to 22.4x on a 100,000-row table, with the whole merge suite run
as a scan-against-lineage oracle in debug builds. The detached-only RFC's
change-discovery comparison did not list this design among its candidates.

An issue and a local change are not enough. The adjacency rule and the §4.3
delete rules are accepted RFC text, and this design replaces them.

## User and operational behavior

Results do not change. Every surface returns the same entity changes in the
same RFC 0030 §4.4 order, with the same page token and cursor contents, the
same errors and the same authorization.

Cost changes. A changed type costs:

- one CPU pass over the fragment lists of its two manifests, which opening
  both versions already loads;
- one deletion-vector read per fragment whose deletion file differs;
- one narrow `id` read per candidate row;
- full images only for the rows a page emits.

Unchanged fragments are never read. Two shapes stay larger than the number of
changes, and the user guide states both:

- a window that crosses a compaction costs the live rows of the fragments the
  compaction rewrote, on both sides;
- a window that crosses an overwrite costs the whole type, as today.

The cost sentence in `docs/user/branching/changes.md` changes to say this.

The `change_lineage` session setting has `request` scope and the process
default `OMNIGRAPH_CHANGE_LINEAGE`:

- `on` uses fragment-identity discovery and falls back to the exact comparison
  when the gate refuses. It is the release default once rollout step 5 lands.
- `verify` runs both and fails the request loudly on any difference. It is the
  debug-build default.
- `off` uses today's path. A mistyped value warns and behaves as `off`, so the
  kill switch cannot misfire on, as with `merge_lineage`.

A feed request can pass the setting with `set=change_lineage=off`.

Change-feed pages gain two bounds (Design, Change feed pages). Callers see
smaller pages when a commit is expensive. Cursors keep their meaning.

## Design

### Inherited from Lance

OmniGraph relies on these Lance 13.0.0 behaviors. The merge classifier relies
on all of them today.

- A data file is never rewritten behind its name. Data files have UUID names,
  so the same resolved path in two manifests means the same bytes.
- A fragment's live rows change only through a new deletion file, new data or
  overlay files, or a new fragment. Fragment metadata (`files`, `overlays`,
  `deletion_file`, `physical_rows`) is part of the manifest that opening a
  version loads.
- A deletion vector is read per fragment. A row is read by its position in a
  fragment at a pinned version.

### The discovery rule

For one changed type lifetime with pins `from` and `to` that pass the gate:

1. Pair fragments by fragment id across the two manifests.
2. Same resolved data and overlay files, same deletion file: skip. Nothing is
   read.
3. Same resolved data and overlay files, different deletion file: read both
   deletion vectors. Offsets deleted in `to` and live in `from` are candidates
   on the `from` side. Offsets deleted in `from` and live in `to` are
   candidates on the `to` side.
4. Different data or overlay files, or a fragment present on one side only:
   every live row of that fragment is a candidate on its side.
5. Read each candidate's `id` at the version where it is live. Retain the keys
   under a fixed budget, `LINEAGE_CANDIDATE_MAX_BYTES` (32 MiB) as the merge
   does. A budget miss sends that type to the exact comparison.

Resolved file identity is the owning root plus the relative path, with
`base_id` resolved through each manifest's base paths. An unresolvable
reference makes its fragment fully changed. This is `fragment_data_identical`
and `lineage_side_candidates` in `crates/omnigraph/src/exec/merge.rs`. This RFC
moves that code into one module that merge and the change enumerator share.
Merge pairs a base with each side; the enumerator pairs `from` with `to`.

### Why the candidates are complete

Call a live row *shared* when it sits in a rule-2 fragment, or in a rule-3
fragment at an offset outside the deletion-vector difference. A shared row is
the same physical row, with the same bytes, at the same position in both
versions, because neither its data files nor its deletion state changed.
Every live row that is not shared is a candidate on its side.

Take a logical id `L` whose state differs between the pins.

- If `L` is live on one side only, its row there is not shared, since a shared
  row is live on both sides. So it is a candidate.
- If `L` is live on both sides and both rows were shared, they would be the
  same row with the same image. Ids are unique within one version, so that
  row is `L`'s only row on each side, and `L` would be unchanged. So at least
  one of `L`'s rows is a candidate.

Every changed id is therefore found. A candidate that did not change, such as
a row a compaction moved, is dropped by equality.

### Images and pairing

Each candidate is read in full at the position where it is live, from that
side's pinned dataset, in fragment-scoped batches (the shape
`HydrateExec::fragment_rows` already uses). Candidates are grouped by logical
id. An id found only on the `from` side is a delete, only on the `to` side an
insert, and on both sides an update or nothing, by the existing
`rows_equal_across_vintages` and `emitted_image`.

An id found on one side only is absent from the other side. Were `L` live
there, its row would be a candidate, which contradicts the premise, or shared.
A shared row is also live on the first side, at its shared position, so the
first side would hold two live rows for `L`. The gate requires the exact-id
primary-key contract on both pins, as the merge gate does, so that cannot
happen. No `id IN` probe of the other side is needed, and no `__id` index is
needed.

### Order and paging

Candidate ids are kept sorted per type. A page walks types in the frozen
`(kind, type id, id, operation rank)` order, takes the ids after the resume
key, and hydrates only the rows it emits. Page tokens keep their v1 contents.

A continuation runs discovery again for the resume type and the types after
it. That costs metadata, changed deletion vectors and the candidates' `id`
reads, with no sort of the type and no full hydration. The restart cost of
[#970](https://github.com/ModernRelay/omnigraph/issues/970) shrinks to the
size of the change. A cache of discovery results keyed by pin would be allowed
by invariant 12; this RFC does not propose one.

### The gate

Any miss sends the type to the exact ordered comparison:

- the same table identity and dataset path at both pins;
- both datasets opened at their recorded pins;
- stable row ids on both pins;
- the exact-id primary-key contract on both pins;
- the same user schema at both pins, which the schema-boundary refusal
  already requires;
- the candidate key budget.

The merge gate also refuses Blob schemas. Discovery reads only `id`, and
equality already compares Blob descriptors with an exact payload tie-break, so
this RFC does not carry that condition over. Unresolved question 1 asks
reviewers to confirm.

### What it replaces

The adjacency admission (the Commit and Stamp proofs), the
`_row_last_updated_at_version` window, the transaction-footprint plans and the
`CandidateUpserts` and `CandidateDeletes` emitters go once rollout reaches step
6. Discovery reads no transaction file and depends on no provenance marker. A
by-source delete inside an unmarked `Update` appears as a deletion-vector
difference and is found, so `no_by_source_delete` and `insert_absence` stop
being needed for change discovery. A delete's ids are read at the `from` pin,
so `omnigraph.deleted_ids` is not needed either, and deletes over the 16 MiB
record limit no longer fall back. Writers keep stamping all three; merge still
uses `insert_absence`. Removing them is out of scope.

For a single-transaction interval, the new rule does O(F) work over the two
fragment lists instead of O(log F). It reads fewer rows: the shipped path's
data work covers the new fragments plus the rows of every touched parent
fragment (RFC 0030 §14), while rule 3 reads only the deletion-vector
difference.

### Change feed pages

A feed page stops before a commit only on its commit ceiling (128, at most
512) or an exhausted change or byte budget. HTTP passes no commit ceiling,
and nothing bounds a page's work
([#1157](https://github.com/ModernRelay/omnigraph/issues/1157)).

- **Work ceiling.** Discovery knows each commit's candidate count before any
  row is read: the live rows of rule-4 fragments plus the sizes of rule-3
  differences. A fourth page ceiling, candidate rows examined, stops a page at
  a block boundary before a commit that would exceed it. The first commit of a
  page is always admitted. The same cut always splits the same way, so RFC
  0030 §4.4 replay holds.
- **Commit ceiling.** HTTP, the MCP `changes_poll` tool and the CLI pass a
  caller's commit ceiling, 1 to 512, through to `max_commits`.
- **Handle reuse.** Commit `k`'s child pins are commit `k + 1`'s parent pins.
  A poll opens each pinned dataset once and reuses it across commits.
  Immutable, version-pinned state may be cached (invariant 12).

### Out of scope

- Commit-id lookup ([#972](https://github.com/ModernRelay/omnigraph/issues/972)).
  A side spelled as a branch-held commit id still scans live branches. Its fix,
  resolving each side once and locating a block's branch, is separate.
- Change sets recorded on the commit
  ([RFC 0068](0068-graph-commit-record.md)). Nothing measured here needs them.
- Skipping the fragments a compaction rewrote. A `Rewrite` transaction maps
  old fragments to new ones with row ids and content preserved, so a later
  change could skip those pairs while the transaction is retained.
- Cross-request caches of discovery results or images.

## Invariants

- **1, respect the substrate.** Discovery uses Lance manifest and fragment
  metadata, deletion vectors and positional reads, through the same surfaces
  the merge classifier uses. It adds no parallel storage machinery.
- **3, one coherent view.** Discovery and images read only the two pinned
  versions the enumerator already opens.
- **6, stable identity.** Types pair by stable graph identity, as today.
  Fragment ids and positions locate data; they are never treated as entity
  identity. Entities are keyed by the `id` read at the pinned version.
- **7, physical acceleration is derived.** Correctness does not depend on an
  index, on fragment count or on layout. Layout changes cost only, which is
  why compaction is a documented cost term. No `__id` index is required.
- **8, integrity failures are loud.** `verify` fails the request on any
  difference from the exact comparison.
- **11, bounded and observable.** The key budget, the existing page ceilings
  and the new work ceiling bound each request. Hot-path work scales with the
  change plus the fragment-list length of the changed types, not with history
  or table rows.
- **12, one source of truth.** Nothing is persisted. Every fact comes from the
  two pinned manifests and their files.
- **13, evidence.** Cost claims go into `changes_cost.rs`, and `verify` makes
  the existing suites the oracle.

Deny-list: no logical precondition depends on index coverage, fragment count
or layout; there is no ad-hoc `IN (...)` string, since images are positional
reads; and no parallel truth is maintained.

RFC 0030 §4.3 forbids four delete shortcuts. This design uses none of them:

- It does not treat a deletion-vector scan as a complete delete feed.
  Completeness comes from comparing both pinned manifests, as argued above.
- It does not infer keys from fragment ids or row addresses. It reads `id` at
  the pinned version, and positions only locate the row.
- It does not parse a transaction predicate. It reads no transaction.
- It persists no tombstone.

On acceptance, RFC 0030 gets a disposition note replacing the §4.2
row-version classification, the §4.3 delete rules and the §8.2 and §14
adjacency cost rule. The detached-only RFC gets one for its change-discovery
decision. The feed contract, the exact comparison as authority, cursors and
retention gaps stay.

## Compatibility and reversibility

There is no storage, wire, token or result change. A page token minted by one
build continues on another, since contents and order are unchanged. The new
setting and environment variable are additive. `off` restores today's path
exactly. Reverting the code leaves nothing behind.

## Alternatives

1. **Chain proof**, the recommendation on
   [#969](https://github.com/ModernRelay/omnigraph/issues/969): walk
   `read_version` links from `to` back to `from`, proving each link and taking
   the union of touched fragments. It reads one transaction file per link, up
   to 1,024 per merge. It breaks across compaction, overwrite and any unproven
   link, and once cleanup reclaims an intermediate merge chunk that no graph
   commit pins. It keeps the marker machinery. Fragment identity needs only
   the two end manifests, so none of these limits apply.
2. **Change sets recorded at commit**
   ([RFC 0068](0068-graph-commit-record.md), and a candidate in the
   detached-only RFC). This adds a second record beside the rows that
   invariant 12 has to police, plus a format change. Its main advantage over
   this design is a compaction window, which is not one of the measured
   failures.
3. **Writer-set row-lineage versions**, an upstream Lance option that stamps
   rows with the published version. It waits on a Lance release and a pin
   bump, and stamps order one lineage only, so a two-point diff of diverged
   heads would still need another rule.
4. **Larger pages and longer client timeouts.** These hide the cost. The first
   page still compares whole tables.
5. **A plan cache across pages**
   ([#970](https://github.com/ModernRelay/omnigraph/issues/970)). It helps
   continuations only. The first page stays table-sized.
6. **A wall-clock page budget.** Where a page ends would depend on timing, so
   replay and DST lose determinism. The work ceiling bounds the same thing
   deterministically.
7. **Do nothing.** Branch review, merge-commit inspection and multi-commit
   diffs stay table-sized, and clients time out.

## Evidence and tests

- **Oracle.** `change_lineage = verify` is the debug-build default. Every
  commit-changes, feed and diff enumeration in the engine, server, GQT and DST
  suites runs both classifiers and fails on any difference, as #540 did for 72
  merge tests.
- **Cost owner.** `crates/omnigraph/tests/integration/changes_cost.rs`:
  - a branch diff over `k` commits on a type of `N` rows reads data that is
    flat in `N` and grows with the changes;
  - a three-way merge commit's own changes are flat in `N`;
  - a two-point diff of diverged heads is flat in `N`;
  - a window across optimize grows with the rewritten rows, the documented
    term;
  - a window across an overwrite grows with `N`, as today.
- **Merge.** `crates/omnigraph/tests/integration/merge_cost.rs` stays flat
  after the module move.
- **Lance guards.** `crates/omnigraph/tests/lance_surface_guards.rs` gains
  guards that a data file path never names different bytes in two versions of
  a detached chain, and that a deletion change produces a new deletion-file
  identity. Merge already relies on both.
- **Witness seams.** `CHANGE_FEED_PRE_TABLE_OPEN` and
  `CHANGE_FEED_POST_HEAD_WITNESS` keep refusing a branch delete and recreate
  under `on` and `verify`.
- **Hosted re-measure.** The five reads in Motivation after deployment.

Lance reviewed: 13.0.0, the workspace pin. Surfaces: `Fragment` (`files`,
`overlays`, `deletion_file`, `physical_rows`, `row_id_meta`), fragment
deletion-vector reads, and fragment-scoped positional reads.

## Rollout

1. Move discovery into a shared module. Merge uses it unchanged, and
   `merge_cost.rs` stays flat.
2. Commit changes and each commit of a feed poll use it behind
   `change_lineage`: `verify` in debug builds, `off` in release builds.
   `implementation` becomes `in-progress`.
3. The graph diff, two-point and branch form, uses it.
4. Feed bounds: the work ceiling, the commit-ceiling parameter and handle
   reuse.
5. Release default `on` after one edge cycle with `verify` clean in CI and DST.
   The user guide and the RFC 0030 and detached-only dispositions land here.
6. Remove the adjacency candidate path once a stable release has shipped `on`.
   `implementation` becomes `complete`.

Each step can ship alone. Before step 5, release builds behave as today.

## Unresolved questions

1. **Blob tables.** Is there a reason beyond merge staging for the merge
   gate's Blob refusal that change discovery must inherit?
2. **Positions or `id IN`.** Images are read at candidate positions, which
   relies on per-version id uniqueness under the exact-id primary-key
   contract. Does that contract hold for every edge table? If not, read by
   chunked exact `id IN` at both pins, as merge does, at the cost of an index
   or a narrow scan.
3. **Budget miss granularity.** Does a type over the key budget fall back to
   the exact comparison, or does discovery page by fragment range?
4. **Witness frequency.** Can the branch-head re-prove run once per feed poll,
   after the last table open, instead of once per commit?
5. **Defaults and names.** The work ceiling's default, and the request
   parameter names under the [HTTP API
   conventions](2026-10-19-http-api-conventions.md).

## Decision log

- 2026-10-26: drafted from the hosted measurements on
  [#969](https://github.com/ModernRelay/omnigraph/issues/969),
  [#972](https://github.com/ModernRelay/omnigraph/issues/972) and
  [#1157](https://github.com/ModernRelay/omnigraph/issues/1157), and the merge
  classifier of [#540](https://github.com/ModernRelay/omnigraph/pull/540).
