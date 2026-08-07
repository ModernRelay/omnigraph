---
type: spec
title: "RFC-030 — Graph change feed and retained-history contract"
description: Defines a graph-commit change feed, caller-owned cursors, and honest retention failures by composing OmniGraph lineage with Lance's native row-version tracking; it does not create a second WAL or expose physical datasets.
status: draft
tags: [eng, rfc, cdc, change-feed, time-travel, provenance, lineage, audit, omnigraph]
timestamp: 2026-08-05
owner: OmniGraph maintainers
---

# RFC-030: Graph change feed and retained-history contract

**Status:** Draft

**Date:** 2026-08-05

**Author track:** Maintainer design series

**Depends on:** RFC-013 Phase 7 graph lineage, RFC-022 snapshot capture and
publication, RFC-023 exact-`id` table fencing, and RFC-028 immutable table
identity.

**Surveyed:** OmniGraph `main` at commit
`ad3da4170ee90cdd065256f61ebcb6634f35104b` (internal schema v6) and Lance
9.0.0.

**Audience:** engine, server, CLI, and documentation maintainers.

---

## 0. Decision

OmniGraph will expose changes as an ordered sequence of **graph commit
blocks**. A block is the logical difference between one graph commit and its
first parent, plus the cause already recorded on that commit. The user never
sees or resumes a feed for an individual Lance dataset.

The implementation reuses the coordinator we already have:

1. `__manifest` graph lineage selects the branch path and supplies commit,
   parent, merge-parent, actor, and graph-snapshot authority.
2. The two exact manifest snapshots select each table lifetime's exact Lance
   begin/end versions.
3. Lance's native row-version tracking supplies inserts and updates from the
   dataset checked out at the **exact end version**.
4. Deletes come from an exact, bounded comparison of live logical IDs at the
   begin and end snapshots. Lance 9 has no complete deleted-row feed.
5. One opaque caller-owned cursor resumes the graph feed. OmniGraph stores no
   per-consumer state.

This is a derived read model. It adds no WAL, transaction manager, change-log
table, server-side cursor registry, delete tombstone, or second coordinator.
It does not expose Lance paths, branches, fragment IDs, row addresses, or
per-table versions as public CDC concepts.

The first contract is a **cause-carrying entity change feed**. Inserts and
updates carry the exact logical after-image from the child snapshot; deletes
carry the exact logical before-image from the parent snapshot. This is enough
to apply retained entity changes downstream and to derive the existing `diff`
result.

It is not a complete empty-store graph replay log: graph-schema evolution is
separated in §10 because the current lineage does not preserve an accepted
SchemaIR for every historical graph commit. The limitation is schema replay,
not row-image availability.

## 1. What Lance gives us—and what it does not

The design is based on the pinned Lance 9.0.0 implementation as well as the
published format documentation.

| Lance surface | Safe use in this RFC | Boundary |
|---|---|---|
| Immutable dataset versions and exact checkout | Read the exact table state selected by each graph manifest snapshot | Cleanup may remove an old version permanently |
| Stable row IDs and `_row_created_at_version` / `_row_last_updated_at_version` | Classify live rows inserted or updated in a table-version interval | Available only when stable row IDs were enabled at dataset creation |
| Public `Dataset::delta()` with streaming inserted/updated/upserted rows | Preferred insert/update substrate after the bounded-batch guard in §9 | It scans the `Dataset` handle's snapshot; asking a current-HEAD handle about an old interval can omit a row changed again later |
| Transaction files | Optional, fail-closed proof that an interval cannot contain logical deletes | They describe physical operations, are reclaimable, and do not persist deleted logical keys |
| `include_deleted_rows()` | Debugging and physical alignment only | It omits compacted tombstones and whole-fragment deletes and returns null `_rowid` for deleted rows |
| Manifest version timestamp | Optional derived publication-time evidence | Writer-clock based, not guaranteed monotonic; enumerating all timestamps reads all retained manifests |
| Tags, branches, and cleanup | Retain exact versions and reclaim old history | Retention can contain holes; it is not one scalar floor shared by all graph tables |

Primary references:

- [Lance row ID and lineage specification](https://lance.org/format/table/row_id_lineage/)
- [Lance transaction specification](https://lance.org/format/table/transaction/)
- [Lance versioning guide](https://lance.org/quickstart/versioning/)
- [Lance read/write and cleanup guide](https://lance.org/guide/read_and_write/)
- [Lance 9.0.0 `DatasetDelta` source](https://github.com/lance-format/lance/blob/v9.0.0/rust/lance/src/dataset/delta.rs)

The `DatasetDelta` source on current Lance `main` is still byte-identical to
the v9.0.0 file surveyed here. There is no merged deleted-row API. Draft
[Lance PR #5002](https://github.com/lance-format/lance/pull/5002) explores
`_row_deleted_at_version`; its continuation
[PR #6671](https://github.com/lance-format/lance/pull/6671) closed without
merging. RFC-030 keeps an adoption seam for a future complete upstream delete
surface but does not depend on either proposal.

Two details are load-bearing:

- A delta range is exact only when its base `Dataset` is checked out at the
  requested end version. The numeric end version is a row predicate, not a
  snapshot pin. A later update or delete on the handle can otherwise change or
  remove the result for an older interval.
- There is no native complete delete stream. Persisted `Delete` transactions
  carry updated fragments, deleted fragment IDs, and a predicate—not the
  logical keys. Merge-delete is represented as `Update` and likewise does not
  retain those keys.

These facts rule out both a custom tombstone interpretation and the claim that
Lance makes the entire graph feed free. Lance owns the table history; OmniGraph
still owns the small amount of graph-level coordination needed to compose it.

## 2. Existing truth

The required durable authority already exists:

- Each successful graph publish atomically writes its `graph_commit` and
  `graph_head` rows with the table-version changes they describe.
- A `GraphCommit` records `graph_commit_id`, first parent, optional merge
  parent, actor, branch, manifest version, and `created_at`.
- A historical manifest snapshot maps immutable table identity
  `(stable_table_id, table_incarnation_id)` to the exact table branch and Lance
  version visible at that graph commit.
- Every current v6 graph table is created with stable row IDs and exact
  non-null logical `id` as its unenforced primary key.
- `diff_between` and `diff_commits` already know how to compare two graph
  snapshots, including table creation, removal, rename, and cross-lineage ID
  comparison.

Two current names must not be allowed to overstate their meaning:

- `GraphCommit.created_at` is minted before table effects and remains fixed
  across retries and recovery. Public CDC calls it **`authored_at`**, not
  `committed_at` or `published_at`. Renaming the persisted column is unnecessary
  and would create a format change.
- `EntityChange.manifest_version` currently mixes table-local row-version
  stamps with a graph-manifest fallback. A graph feed does not carry this field
  forward. Commit cause belongs on the block; physical table versions remain
  implementation details.

## 3. Public semantic model

### 3.1 One logical block per graph commit

Conceptually, the engine returns:

```text
GraphChangeBlock {
  cause: {
    graph_commit_id,
    parent_commit_id,
    merged_parent_commit_id?,
    authored_branch,
    graph_snapshot_version,
    actor_id?,
    authored_at
  },
  part,
  commit_complete,
  changes: [
    { kind, type_name, id, op, endpoints?, before?, after? }
  ]
}
```

The exact Rust and wire DTOs land with the implementation. The semantics above
are fixed:

- `kind` is node or edge.
- `type_name` is a graph-schema name, never a dataset name or path.
- `id` is the graph's exact logical `id`.
- `op` is `INSERT`, `UPDATE`, or `DELETE`.
- `endpoints` is present for edge changes, including deletes, and is read from
  the appropriate exact snapshot.
- `after` contains the complete user-visible logical row for inserts and
  updates and is absent for deletes.
- `before` contains the complete user-visible logical row for deletes and is
  absent for inserts and updates. Update before-images are not required to
  apply the feed and are deferred.
- Images use the same canonical logical value conversion as graph export.
  Lance `_row*` and other storage-only columns are never exposed.
- Cause is stated once on the block, not copied onto every entity.
- `authored_branch` is the branch on which the commit originally landed. The
  selected feed branch is page/request context; inherited commits on a named
  branch do not have their cause rewritten.
- `part` is zero-based and `commit_complete` is true only on the final part of
  a commit split across pages. No later commit appears before that terminal
  part. Each transmitted part repeats the same cause; parts never mix commits.

A physical-only graph commit such as compaction produces an empty block. The
cursor still advances over it. A pure type rename also produces no row changes;
future changes use the destination name because immutable identity, not alias,
pairs the table lifetime.

### 3.2 Branch and merge order

The feed order is the **first-parent chain of one captured graph branch
incarnation**. It is not a global sort of every commit in the DAG.

For a merge commit, changes are computed against the first parent: this is the
state transition observed by a consumer tailing the target branch. The merged
parent ID remains on the cause so a DAG-aware caller can inspect it separately.
No `both sides` mode is part of v1.

The existing `GraphCommit::lineage_key()` remains useful for deterministic
listing and head selection inside one lineage projection. It is not encoded as
the CDC order and is not a cross-branch cursor.

### 3.3 Graph-level filtering

Filters may select graph concepts—node/edge kind, graph type name, and
operation. They may not select a Lance dataset, table path, native branch,
fragment, or table version.

The cursor binds the canonical filter and image contract. Reusing it with a
different filter or image contract fails as `CursorScopeMismatch`; it never
silently skips data.

## 4. Exact per-commit derivation

For each first-parent edge `P -> C`:

1. Load the exact graph snapshots named by `P` and `C`.
2. Pair their table entries by immutable table identity, not alias.
3. Skip identical table branch/version pairs.
4. Derive changes for each remaining table lifetime.

Logical operation is defined only by the two graph-visible states:

- absent in `P`, present in `C` → insert;
- present in both with different canonical logical images → update;
- present in `P`, absent in `C` → delete;
- present in both with equal images, or absent in both → no entity change.

Lance row-version columns are candidate pruning. They do not override this
definition. In particular, overwrite or restore can make a row look physically
new while reusing a logical graph `id` that was already present.

### 4.1 Table addition and removal

- A lifetime present only in `C` emits all live end rows as inserts with
  after-images.
- A lifetime present only in `P` emits all live begin rows as deletes with
  before-images.
- Drop/re-add is two lifetimes even when the public alias and logical IDs are
  reused. The internal continuation key includes immutable lifetime identity so
  pagination cannot conflate them.

### 4.2 Inserts and updates on one lifetime

Open the table at the exact end version selected by `C`. For the table-version
interval `(begin, end]`, stream rows matching Lance's documented row-version
predicates and partition the physical candidates as:

- insert when `_row_created_at_version > begin`;
- update otherwise when `_row_last_updated_at_version > begin`.

The adapter treats these rows as candidates. For **every** candidate it performs
a bounded parent membership/image probe: parent absence means insert; parent
presence plus a different logical image means update; equal user-visible images
mean no logical change. This suppresses physical no-ops and
storage-metadata-only movement as well as closing overwrite and delete/reinsert
cases without turning physical row lineage into graph identity.

Membership/image checks are coalesced into bounded structured exact-ID batches;
the design does not authorize one object-store round trip per candidate or an
ad-hoc string `IN (...)` filter.

The adapter prefers `DatasetDelta::get_upserted_rows()` if its runtime surface
passes the projection, blob, and batch-memory gates in §9. Lance 9's convenience
builder hardcodes wildcard projection and does not expose row/byte batch limits.
If that cannot satisfy OmniGraph's bounds, the adapter uses one thin
`DatasetScanner` over the same public version columns and predicates, with the
required projection and batch ceilings. It must not create a second lineage
algorithm.

Every invocation asserts:

- the handle is pinned to `end`, not current HEAD;
- stable row IDs and both version columns are genuinely active;
- every storage-only/system column is excluded from the public projection;
- every emitted insert/update image is taken from this exact end handle.

If table branch lineage changes, the end version does not advance from the
begin version, or the exact transaction interval contains an operation whose
row-version behavior is not proven, the optimization is unavailable. The
correct fallback is a bounded ordered comparison of complete logical rows at
the exact parent and child snapshots. `Restore`, unknown operations, and a lazy
branch fork therefore cannot disappear from CDC merely because their row
version stamps predate the graph commit.

### 4.3 Deletes

Correctness is an ordered, bounded merge of live logical IDs from the exact
begin and end snapshots. IDs present only at the begin snapshot are deletes;
their edge endpoints and logical before-images are read from that same begin
snapshot.

An optimization may skip this comparison only after inspecting every exact
table transaction in the interval and proving that every operation is a mature,
row-set-preserving shape used by OmniGraph. Missing transaction files, cleaned
version holes, `Overwrite`, `Restore`, delete-capable `Update`, unknown/new
operation variants, and experimental Lance operations all mean **unknown** and
fall back to the exact ID comparison. The optimization is never authority.

Forbidden delete shortcuts:

- treating Lance's deletion-vector scan as complete;
- parsing a transaction predicate to rediscover keys;
- inferring keys from fragment IDs or row addresses;
- persisting OmniGraph tombstones only to make this reader cheaper.

### 4.4 Bounds and deterministic continuation

The engine implementation is streaming. It does not build a delta-wide
`Vec<EntityChange>` or a delta-wide set of row images before applying page
limits.

Each request has three independent ceilings:

- graph commits examined;
- entity changes returned;
- retained/serialized bytes.

This prevents a sparse filter from scanning unbounded history merely to fill a
row limit. A commit block is kept whole when it fits. A larger commit is split
at a deterministic internal key composed from immutable table lifetime,
logical ID, and operation rank; the key remains opaque on the wire. Replaying
the same page input against the same retained cut returns the same ordered
events.

The total event order inside a block is
`(table_key, stable_table_id, table_incarnation_id, id, operation_rank)`.
Stable/incarnation IDs are hidden tie-breakers in the opaque cursor, not public
dataset handles. Operation rank is frozen as `INSERT = 0`, `UPDATE = 1`,
`DELETE = 2` for cursor v1.

A consumer that needs graph-commit atomicity durably buffers non-terminal
parts and commits that buffer together with the cursor from the
`commit_complete = true` part. It does not apply a partial commit. Retrying from
the cursor before a part replays that part exactly; advancing a durable cursor
without durably retaining the corresponding part is caller data loss, just as
with any caller-owned offset.

The byte ceiling is chosen at least as large as OmniGraph's maximum legal
logical row image. If historical or blob-backed data still produces one image
larger than that ceiling, the request fails with a typed resource-limit error;
it does not truncate the image or silently switch to keys-only output.

The implementation must prove the ordering path is bounded. It may use Lance's
ordered scan or a bounded merge, but it may not sort an unbounded graph commit
in memory or depend on unspecified concurrent scan order.

## 5. Cursor contract

The wire cursor is opaque and versioned. Its encoding is deliberately not
documented as colon-separated fields. Semantically it binds:

- graph identity (derived from the persisted schema identity domain);
- lineage/storage-strand incarnation (the first-parent root/genesis commit);
- cursor purpose and traversal direction (`changes/forward` for cursor v1);
- normalized graph branch name;
- Lance native branch identifier, closing delete/recreate ABA;
- canonical filter and logical-image contract digest;
- last completed graph commit;
- a captured upper-cut commit for an in-progress page sequence;
- within-block continuation when the current commit is split;
- cursor format version and corruption checksum.

The cursor is not an authorization token. Every request is authorized normally,
then the cursor scope is validated. Cursors from different graphs, branch
incarnations, filters, or cursor versions fail loudly and are not comparable.

On the first poll, the engine captures the branch head as the upper cut. Page
continuations keep that cut even if new commits arrive. Once the cut is reached,
the returned cursor is caught up; the next poll captures a new head and begins
after the last completed commit. This gives one coherent finite replay window
without hiding later work.

The server persists no cursor or consumer offset. Durability belongs to the
caller. A cursor is not a retention lease: cleanup may reclaim versions after
the cursor is issued.

### 5.1 Starting a feed

The first request chooses one explicit start mode:

- `Now` (default): capture the current head and return a cursor positioned
  after it; no accidental replay of the graph's entire history.
- `AfterCommit(id)`: begin after an exact commit that must lie on the captured
  branch incarnation's first-parent chain.
- `Beginning`: begin before that chain's root, including inherited history for
  a named branch, and fail with a typed gap if the required data is no longer
  retained.

After validating the start, the same coherent branch snapshot supplies the
upper cut. A missing cursor is not an ambiguous alias for `Beginning`.

### 5.2 Exact bootstrap and reset

C2 ships one baseline handshake with the public feed:

```text
capture_change_baseline(branch, feed_scope) -> {
  snapshot_commit_id,
  exact_graph_snapshot,
  resume_cursor
}
```

The coordinator validates the filter/image scope, captures one branch
incarnation and head `H`, exports the graph snapshot pinned to `H`, and creates
a cursor equivalent to `AfterCommit(H)` in that same feed scope. Concurrent
commits after `H` are picked up on the next poll. The caller durably installs
the snapshot before it durably installs the resume cursor. If exact snapshot
construction fails or cleanup removes a participant, the handshake returns no
usable cursor.

This is not today's current-HEAD-only export with a commit ID added afterward;
the export itself is opened at the captured graph commit. The same primitive is
the only supported reset after a retention gap, closing the head-capture/export
race.

## 6. Retention and failure semantics

There is no scalar `oldest_readable_manifest_version()` in this RFC.

Lance cleanup acts independently on physical datasets, may retain tagged or
branched versions, may leave version holes, and OmniGraph cleanup records
per-table failures while allowing other tables to converge. A graph commit is
readable only when every exact table endpoint needed for its transition can be
opened. That is a property of a concrete first-parent edge, not a comparison
against one numeric floor.

Before deriving an edge, the feed verifies the exact required begin/end table
versions. Failures are translated into graph-level typed outcomes:

- `HistoricalDataReclaimed { graph_commit_id, type_name }` for direct time
  travel to a graph snapshot whose participant is gone;
- `ChangeFeedGap { cursor, first_unreadable_commit_id }` when
  a feed cannot continue contiguously.

The public error names graph concepts. Exact physical paths and table versions
may appear in operator diagnostics/logs, not in the public CDC contract.

The recovery action is §5.2's exact baseline handshake, not a suggested commit
ID that can race before export. Computing the oldest contiguous resumable suffix
requires walking and validating real participant pins; if a later
implementation exposes that answer, it must cache and cost-test the derivation
rather than guessing from HEAD arithmetic or table minima.

A page is atomic. If an endpoint becomes unreadable while constructing it, the
request returns the typed gap and no cursor advancement; the caller retries
from its previously durable cursor or deliberately resets from a snapshot. The
engine streams scans into one bounded page buffer, but a transport does not
publish that page or its cursor until construction succeeds.

`commit list` continues to list durable lineage even when old table data is no
longer readable. This RFC does not add `--mark-readable`: determining that for
every historical commit is a history walk, not a cheap annotation.

## 7. Time semantics

Exact time travel by graph commit ID or graph snapshot version remains the
authoritative contract.

This RFC does **not** add `--at-time` in its core phases. The former proposal
used `GraphCommit.created_at`, called the result committed time, and promised a
binary search. All three parts were wrong:

- `created_at` is intent/authorship time, minted before effects and recovery;
- Lance's `__manifest` version timestamp is a better publication-time witness
  and can be read with `read_version_transaction(version)`;
- neither timestamp is guaranteed monotonic, and `Dataset::versions()` reads
  every retained manifest. Lance's own date-range delta resolver performs a
  full scan.

A later publication-time slice may expose `published_at` as derived metadata
and define a deterministic wall-clock selection rule. It must first measure the
cold history cost and state explicitly that writer-clock timestamps are not a
linearizable real-time oracle. It may not introduce a binary search without a
proven monotonic index.

## 8. Relationship to existing diff

`diff_between` remains the direct net-current comparison API while the feed is
built. It does not receive optional cause fields: a range collapsed across
multiple commits has no single honest actor or commit.

C0 makes its changed-table traversal deterministic by destination/source
`table_key` and then immutable identity. It does not turn the existing net diff
into the feed's per-entity cursor order; entity order within one table remains
outside that API's contract.

Once the graph feed is proven, its exact adjacent enumerator supplies most of
the same machinery. That does **not** make arbitrary-range net diff a free
operation algebra. Update-then-revert must disappear; delete-then-reinsert may
be update relative to the range baseline; table lifetimes can change; and
intermediate history may be reclaimed while both endpoint snapshots remain
readable.

Any future feed-backed net diff must therefore reduce against baseline
membership/images and final membership/images, not merely fold operation
labels. The direct endpoint snapshot algorithm remains a valid and often
lower-cost reconciliation path. Both paths share table-identity interval
construction, canonical image comparison, and a conformance suite; they do not
grow separate definitions of insert, update, or delete.

## 9. Evidence gates

Implementation cannot move a phase to accepted without the matching evidence.
Extend existing owners before adding a new test silo: `changes.rs`,
`point_in_time.rs`, `lineage_projection.rs`, `maintenance.rs`, and
`lance_surface_guards.rs`.

### L0 — pinned Lance surface

- Exact-end regression: a row updated in v2 and again in v3 is present with its
  v2 value when the delta is run on a v2 handle, and is not trusted when run on
  a v3 handle for `(v1, v2]`.
- Stable-row-ID/version-column guard on every OmniGraph-created table.
- `DatasetDelta` batch-row, batch-byte, blob, and system/storage-only-column observations.
  If bounds cannot be enforced, select the bounded scanner adapter before C1.
- Exact insert/update after-images and delete before-images, including nested
  values, blobs, edge endpoints, and exclusion of every reserved system field.
- Overwrite with an existing logical `id`, delete/reinsert, restore, and a
  later update prove graph membership/image comparison—not physical
  `_row_created_at_version` alone—selects the logical operation.
- Delete guards covering deletion vectors, whole-fragment delete, update,
  merge-delete, compaction, and missing transaction files.
- A source-walk or exhaustive match makes new Lance `Operation` variants fall
  back to exact ID comparison until reviewed.

### G0 — graph semantics

- Interleaved main/named-branch history follows one branch incarnation's
  first-parent chain.
- Merge output is exactly first-parent-relative and carries the merged parent.
- Table rename is row-neutral; drop/re-add with the same alias and IDs emits
  distinct deletes/inserts without cursor collision.
- Physical-only commits emit empty blocks and still advance the cursor.
- Bounded graph NDJSON, ordinary mutation/load, schema table add/drop, branch
  merge, maintenance, and recovery-completed publication share the same block
  model.

### P0 — cursor and pagination

- Cursor graph/branch-incarnation/filter/version mismatches are typed refusals.
- Cursor lineage/genesis mismatch is refused even when a rebuilt graph reuses a
  schema identity domain or main branch name.
- `Now`, `AfterCommit`, and `Beginning` have exact named-branch inheritance and
  reclaimed-history behavior.
- Baseline capture concurrent with a new commit exports exactly captured `H`
  and the returned cursor later yields `H + 1`; a failed export yields no usable
  cursor.
- New commits arriving between pages do not enter the captured cut.
- Oversized single commits split and replay exactly under row, byte, and
  commits-scanned ceilings.
- Sparse filters stop at the commits-scanned bound.
- Reopen and another process can resume from the caller's cursor with no server
  state.

### R0 — retention

- Cleanup one participant past a required version while retaining others:
  exact `ChangeFeedGap`, no partial page, and successful reset only through the
  exact snapshot+cursor baseline handshake.
- Tagged/branched holes and per-table cleanup failure do not produce a false
  global watermark.
- Direct snapshot access maps reclaimed participant versions to
  `HistoricalDataReclaimed` rather than leaking a raw Lance error.

### Cost evidence

C0 pins adjacent first-parent classification as a pure comparison against the
already-loaded child's persisted parent pointer. Its direct/reversed/arbitrary/
merge matrix is the structural proof: the classifier performs no I/O, history
walk, or allocation proportional to lineage depth, so the storage I/O harness
has no meaningful curve to record for it.

Before C2 exposes a public feed, use `helpers::cost` and realistic history depth
to record separate curves for:

- an unchanged warm caught-up poll through the existing freshness probe;
- refresh after one new commit, including the known current
  `__manifest` full-fold/history term rather than mislabeling it flat;
- page navigation at increasing backlog depth;
- exact-end insert/update enumeration against increasing table size and
  changed-row count;
- parent membership/image probes;
- the no-delete proof for explicitly allowed complete transaction intervals;
- the delete/full-row fallback against increasing table size.

These are measurements, not pre-declared flatness results. Any flat claim lands
only for the dimensions the instrument proves. The acceptance constraint is
that this RFC adds no O(history log history) binary-lifting state to normal
open, refresh, or existing adjacent `diff_commits`, and that every non-flat
term is documented before its public surface ships.

## 10. Explicit exclusions and future work

### 10.1 Graph-schema replay

The entity feed carries canonical logical row images, but it still cannot
recreate a graph from an empty store by itself. The graph lineage does not
retain the accepted SchemaIR for every commit. Property additions, removals,
renames, constraints, and annotations therefore cannot be reconstructed as an
exact historical schema stream with today's authority.

A schema-feed extension must decide:

- historical SchemaIR identity and schema-change event encoding;
- bootstrap semantics for a consumer starting from an empty store;
- whether a full schema snapshot rides every change or only schema commits;
- retention and rebuild behavior for schema history;
- whether the required authority earns an internal-format strand.

It must not infer graph property identity from Lance field IDs or physical
column names. Until that extension lands, the entity feed is replayable only
against a compatible graph schema established out of band.

### 10.2 Other exclusions

- **Delete tombstones:** not needed for retained-history entity CDC; adding
  them changes every write path and the storage format.
- **Push delivery:** poll + cursor is the contract. SSE or another transport
  may wrap it later without changing semantics.
- **Branch lifecycle events:** branch create/delete are control-plane events,
  not graph content commits.
- **Retention pins/checkpoints:** RFC-025's domain.
- **Cross-rebuild history:** export/import rebuild preserves logical graph data,
  not the old storage strand's commit feed.
- **History-flat arbitrary catch-up:** requires measured substrate support; no
  speculative index or shadow log is authorized here.

## 11. Format and compatibility audit

The C0–C3 core below persists nothing and therefore requires no internal-schema
or recovery-schema bump:

- lineage and table pins already exist;
- runtime path/cursor indexes are derived and rebuildable;
- cursors are caller-owned wire values;
- typed errors and new read APIs are additive.

The opaque cursor has its own wire version. An unsupported cursor version is a
typed error, not best-effort decoding.

Any implementation that proposes a stored watermark, feed offset, operation
summary, delete tombstone, or historical SchemaIR changes this conclusion and
must return to this RFC's format audit before landing.

## 12. Phasing

| Phase | Ships | Safe stop |
|---|---|---|
| C0 — foundation correction | Identity-keyed table intervals with deterministic graph-visible table ordering; O(1) adjacent first-parent validation; Lance surface guards; remove speculative binary lifting | No new public API or persisted state; existing diff table traversal becomes deterministic |
| C1 — engine feed | Internal graph commit blocks with exact logical images, exact-end insert/update adapter, complete delete fallback, bounded page/cursor engine, typed gaps | Engine-only contract can be exercised before wire commitment |
| C2 — graph surfaces | SDK, exact snapshot+cursor baseline, `omnigraph changes`, HTTP/OpenAPI, docs, authorization and parity tests | Useful caller-owned entity feed; compatible schema is established out of band |
| C3 — entity history | Newest-first history derived from the same per-commit enumerator, with a separately versioned `history/backward` cursor | Investigation surface; no new storage authority |
| C4 — publication time, optional | Derived `published_at` and possibly a measured as-of-time selector | Lands only after its semantics and cold-history cost pass §7 |
| C5 — schema replay, separate decision | Historical SchemaIR authority and schema-change events | Requires its own format conclusion before implementation |

C0 deliberately does **not** add a second coordinator or an O(history log
history) ancestry index. `CommitGraph` already holds the warm lineage
projection; the feed may add at most the minimal first-parent navigation view
whose cost is justified by C1.

## 13. Resolved decisions

1. Public unit: graph commit block, not table/dataset delta.
2. Merge default: first parent only.
3. Cause placement: once per block.
4. Commit time field in v1: `authored_at`; no false `committed_at` label.
5. Cursor: opaque, caller-owned, graph/branch/filter/purpose/direction bound,
   fixed-cut paging.
6. Delete authority: exact begin/end logical-ID comparison; transaction history
   may only prove that comparison unnecessary.
7. Retention: validate concrete participant versions; no scalar watermark.
8. Existing `diff`: distinct net-current API with shared primitives, no
   optional multi-commit attribution.
9. Row images: exact after-image for insert/update and exact before-image for
   delete; graph-schema replay remains separate.
10. Reset: one exact graph snapshot and its `AfterCommit` cursor are captured
    together; a bare head ID is not a safe bootstrap.
11. Format: no bump for the entity feed; revisit before persisting any new
    history authority.
