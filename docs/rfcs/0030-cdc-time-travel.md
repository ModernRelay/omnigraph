---
type: spec
title: "RFC-030 — Graph change feed and retained-history contract"
description: Defines graph-vocabulary commit diffs and a graph change feed with bounded page tokens, durable cursors, and no public storage-table machinery.
status: draft
tags: [eng, rfc, cdc, change-feed, time-travel, provenance, lineage, audit, omnigraph]
timestamp: 2026-08-05
owner: OmniGraph maintainers
---

# RFC-030: Graph change feed and retained-history contract

**Status:** Accepted for C0–C3 (shipped); C4+ remain design-stage

**Date:** 2026-08-05

**Author track:** Maintainer design series

**Depends on:** RFC-013 Phase 7 graph lineage, RFC-022 snapshot capture and
publication, RFC-023 exact-`id` table fencing, and RFC-028 immutable table
identity.

**Surveyed:** OmniGraph `main` at commit
`d0b4502dfe463138524ed9b53cfa5c0ab83a5deb` (internal schema v6); the Lance
surface was revalidated against the pinned 10.0.0 release.

**Audience:** engine, server, CLI, and documentation maintainers.

---

## 0. Decision

OmniGraph will expose entity-data changes in graph vocabulary: node or edge,
graph type identity and name, logical entity ID, edge endpoints, operation, and
exact logical before/after values. Public results do not expose tables,
datasets, fragments, physical versions, row addresses, or storage ordering
keys.

Two public surfaces share one private adjacent-commit enumerator:

1. A **finite commit entity diff** compares one exact graph commit with its
   first parent. It is an inspection result. A `next_page_token` may continue a
   bounded response, but there is no durable feed cursor.
2. A **durable entity change feed** returns those graph commit blocks in
   first-parent order. Its cursor means the caller has consumed every complete
   commit through one position; it advances only after the final page of that
   commit.

A page token and feed cursor are deliberately separate. The token means
"continue this response." The cursor means "resume the durable feed after this
complete commit." Normal SDK/CLI usage consumes page tokens automatically.

The implementation reuses the coordinator we already have:

1. `__manifest` graph lineage selects the branch path and supplies commit,
   parent, merge-parent, actor, and graph-snapshot authority.
2. The two exact manifest snapshots select each table lifetime's exact Lance
   begin/end versions.
3. Lance's native row-version tracking supplies inserts and updates from the
   dataset checked out at the **exact end version**.
4. Deletes come from an exact, bounded comparison of live logical IDs at the
   begin and end snapshots. Lance 10 has no complete deleted-row feed.
5. Opaque page tokens bound one response; a separate caller-owned cursor
   resumes the graph feed. OmniGraph stores no per-consumer state.

This is a derived read model. It adds no WAL, transaction manager, change-log
table, server-side cursor registry, delete tombstone, or second coordinator.
It does not expose Lance paths, branches, fragment IDs, row addresses, or
per-table versions as public CDC concepts.

The first contract is a **cause-carrying entity-data diff**. Inserts carry the
exact child after-image, updates carry exact parent before- and child
after-images, and deletes carry the exact parent before-image. Edge images
carry their endpoints as graph references.

Entity-data diff and graph-schema diff are separate contracts. A schema change
is not translated into synthetic row inserts, updates, or deletes. Historical
schema selection used to decode a retained row is private correctness
machinery, not a user-facing schema-history feature. Because current lineage
does not retain accepted SchemaIR for every historical commit, the initial
entity surfaces fail with a typed error at an unproven schema boundary. A
separate schema contract is deferred to §10.

## 1. What Lance gives us—and what it does not

The design is based on the pinned Lance 10.0.0 implementation as well as the
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
- [Lance 10.0.0 `DatasetDelta` source](https://github.com/lance-format/lance/blob/v10.0.0/rust/lance/src/dataset/delta.rs)

The v10.0.0 `DatasetDelta` source is byte-identical to the v9.0.0 file
originally surveyed here. There is no merged deleted-row API. Draft
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
- Persisted accepted SchemaIR v2 owns graph-scoped type/property identity.
  Supported renames preserve it; drop/re-add creates a new declaration
  identity. Public `type.id` is an opaque projection of that graph authority,
  not a second registry or a table identifier.
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

### 3.1 Graph-vocabulary entity changes

Conceptually, the engine returns:

```text
GraphChangeBlock {
  cause: {
    graph_commit_id,
    parent_commit_id?,
    merged_parent_commit_id?,
    authored_branch,
    actor_id?,
    authored_at
  },
  changes: [
    {
      kind,
      type: { id, name },
      id,
      op,
      before?: { properties, endpoints?: { from: entity_id, to: entity_id } },
      after?:  { properties, endpoints?: { from: entity_id, to: entity_id } }
    }
  ]
}
```

The exact Rust and wire DTOs land with the implementation. The semantics above
are fixed:

- `kind` is node or edge.
- `type.id` is an opaque graph-scoped type identity. It survives a supported
  rename and changes after drop/re-add. It is not a serialized table,
  incarnation, Lance field, dataset, or path identifier.
- `type.name` is the graph-schema name useful to humans.
- `id` is the graph's exact logical `id`.
- `op` is `INSERT`, `UPDATE`, or `DELETE`.
- Edge endpoints use the same public `from` / `to` graph-reference vocabulary
  as mutation and load. They belong to each image, so an endpoint-moving update
  has distinct before and after endpoints.
- An insert has only the exact child `after` image. An update has the exact
  parent `before` and child `after` images. A delete has only the exact parent
  `before` image.
- Images use the same canonical logical value conversion as graph export.
  Exact reserved Lance virtual columns and other storage-only columns are never
  exposed; a legal user property is not hidden merely because its name starts
  with `_row`.
- Cause is stated once on the block, not copied onto every entity.
- `authored_branch` is the branch on which the commit originally landed. The
  selected feed branch is page/request context; inherited commits on a named
  branch do not have their cause rewritten.

`GraphChangeBlock` is the logical product. Bounded HTTP/SDK transport may split
it into pages that repeat the cause and carry `next_page_token`; `part`,
`commit_complete`, internal change indexes, and byte limits are not entity
fields. A feed returns its durable cursor only on the terminal page.

A physical-only graph commit such as compaction produces an empty block. The
feed cursor still advances over it. Schema operations produce no synthetic
entity changes; crossing an unprovable schema boundary fails as described in
§10 rather than guessing from physical tables.

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

Filters may select graph concepts—node/edge kind, opaque graph type identity or
name, and operation. They may not select a Lance dataset, table alias/path,
native branch, fragment, or physical version.

Page tokens and feed cursors bind the canonical filter and image contract.
Reusing either with a different scope fails with its corresponding typed scope
error; it never silently skips data.

### 3.4 Finite commit diff versus feed

The finite commit diff asks which entity-data changes one exact commit made
relative to its first parent. It returns one logical block, may paginate with a
`next_page_token`, and never returns or accepts a feed cursor.
The parentless root has no entity diff; callers bootstrap from the exact
baseline instead of receiving invented inserts.

The feed asks for those blocks in first-parent order. It returns a durable
cursor only after a complete block. This keeps bounded transport mechanics out
of the user-facing entity model and avoids freezing the later feed protocol in
an inspection command.

## 4. Exact per-commit derivation

For each first-parent edge `P -> C`:

1. Load the exact graph snapshots named by `P` and `C`.
2. Prove that their logical graph schemas are compatible for entity-data diff.
3. Pair graph type lifetimes by stable graph identity, resolving physical table
   entries privately rather than exposing them.
4. Skip identical physical branch/version pairs internally.
5. Derive changes for each remaining type lifetime.

Logical operation is defined only by the two graph-visible states:

- absent in `P`, present in `C` → insert;
- present in both with different canonical logical images → update;
- present in `P`, absent in `C` → delete;
- present in both with equal images, or absent in both → no entity change.

Lance row-version columns are candidate pruning. They do not override this
definition. In particular, overwrite or restore can make a row look physically
new while reusing a logical graph `id` that was already present.

Equality is typed and structural. It preserves null versus valid empty values,
does not join display strings with a delimiter, and includes the complete
logical Blob reference `(uri, offset, length)` where applicable.

### 4.1 Schema and type-lifetime boundaries

Adding, dropping, renaming, or recreating a type is schema evolution, not proof
that every affected row was inserted or deleted as entity data. The entity
diff therefore does not synthesize row events from table presence alone.

The initial surface accepts only a pair whose compatible logical schema can be
proven. A schema boundary returns a typed refusal and directs the caller to the
separate schema/baseline contract. Drop/re-add remains two graph type lifetimes
even when the name is reused; no page token or cursor may conflate their opaque
graph identities.

### 4.2 Inserts and updates on one lifetime

Open the table at the exact end version selected by `C`. For the table-version
interval `(begin, end]`, stream rows matching Lance's documented row-version
predicates and partition the physical candidates as:

- insert when `_row_created_at_version > begin`;
- update otherwise when `_row_last_updated_at_version > begin`.

The adapter treats these rows as candidates. For **every** candidate it performs
a bounded parent membership/image probe: parent absence means insert; parent
presence plus a different logical image means update and retains both exact
images; equal user-visible images mean no logical change. This suppresses
physical no-ops and storage-metadata-only movement as well as closing overwrite
and delete/reinsert cases without turning physical row lineage into graph
identity.

Membership/image checks are coalesced into bounded structured exact-ID batches;
the design does not authorize one object-store round trip per candidate or an
ad-hoc string `IN (...)` filter.

The adapter prefers `DatasetDelta::get_upserted_rows()` if its runtime surface
passes the projection, blob, and batch-memory gates in §9. Lance 10's convenience
builder hardcodes wildcard projection and does not expose row/byte batch limits.
If that cannot satisfy OmniGraph's bounds, the adapter uses one thin
`DatasetScanner` over the same public version columns and predicates, with the
required projection and batch ceilings. It must not create a second lineage
algorithm.

Every invocation asserts:

- the handle is pinned to `end`, not current HEAD;
- stable row IDs and both version columns are genuinely active;
- every storage-only/system column is excluded from the public projection;
- every emitted insert/update after-image is taken from this exact end handle;
- every emitted update before-image is taken from the exact parent handle.

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
at a deterministic graph-semantic key composed from entity kind, opaque graph
type identity, logical ID, and operation rank. The continuation is carried only
inside an opaque page token; it is not an entity field. Replaying the same page
token against the same retained cut returns the same ordered events.

The total event order inside a block is
`(entity_kind, graph_type_id, id, operation_rank)`. Operation rank is frozen as
`INSERT = 0`, `UPDATE = 1`, `DELETE = 2` for page-token v1. Physical table or
incarnation identity may locate data internally but is not part of the public
ordering contract or continuation payload.

Raw HTTP may return `next_page_token`; SDK and CLI helpers consume it
automatically and stream or spool the block within explicit bounds. In a feed,
intermediate pages carry no advanced durable cursor. The terminal page returns
the cursor after that complete commit, so a caller never checkpoints a partial
block by mistake.

The per-page byte ceiling is a PACKING target — how many small changes share a
page — never a wall a single legal change must fit under. An `Update` serializes
two row images and managed Blobs inline as base64, so one legal committed change
can exceed the ceiling. Such a change is delivered on its own page (forward
progress: it is emitted solo even when it exceeds the remaining budget, and the
cursor advances past it), so no legal committed change is ever un-crossable. The
enumerator never truncates an image or switches to keys-only output. *(Amended
from the original "fails with a typed resource-limit error": that made a legal
managed-Blob update a permanent poison commit no page token or feed cursor could
cross. The resource-limit error is retained only for a zero-capacity request,
which validation already rejects.)*

The implementation must prove the ordering path is bounded. It may use Lance's
ordered scan or a bounded merge, but it may not sort an unbounded graph commit
in memory or depend on unspecified concurrent scan order.

## 5. Continuation contracts

### 5.1 Page token

A page token is opaque, versioned transport state for one bounded result. It
binds the exact commit/block, captured cut, graph/branch incarnation, canonical
filter and image contract, ordering version, enforced bounds, and logical
continuation key. It contains no durable consumer position and is not accepted
as a feed cursor.

The token does not expose table aliases/IDs, dataset paths, Lance versions, or
row addresses. Private placement is re-derived after validation. A page token
is useful to raw HTTP clients that must resume interrupted bounded work; normal
CLI and SDK calls auto-fetch it.

### 5.2 Durable feed cursor

The wire cursor is opaque and versioned. Its encoding is deliberately not
documented as colon-separated fields. Semantically it binds:

- graph identity (derived from the persisted schema identity domain);
- graph-history incarnation (the first-parent root/genesis commit);
- cursor purpose and traversal direction (`changes/forward` for cursor v1);
- normalized graph branch name and graph branch incarnation;
- canonical filter and logical-image contract digest;
- last completed graph commit;
- cursor format version and corruption checksum.

The cursor is not an authorization token. Every request is authorized normally,
then the cursor scope is validated. Cursors from different graphs, branch
incarnations, filters, or cursor versions fail loudly and are not comparable.

On each poll, the engine captures the branch head as the upper cut. Page tokens
keep that cut even if new commits arrive. Once the cut is reached, the returned
cursor is caught up; the next poll captures a new head and begins after the
last completed commit. A durable cursor never points inside a commit.

The server persists no cursor or consumer offset. Durability belongs to the
caller. Delivery is at least once: retrying the prior cursor may replay the
complete next commit, so consumers apply by `graph_commit_id` idempotently and
persist the terminal cursor with the block. A cursor is not a retention lease:
cleanup may reclaim versions after it is issued.

### 5.3 Starting a feed

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

### 5.4 Exact bootstrap and reset

C3 ships one baseline handshake with the public feed:

```text
capture_change_baseline(branch, feed_scope) -> {
  snapshot_commit_id,
  exact_entity_snapshot,
  resume_cursor
}
```

The coordinator validates the filter/image scope, captures one branch
incarnation and head `H`, exports the data-only entity snapshot pinned to `H`,
and creates a cursor equivalent to `AfterCommit(H)` in that same feed scope.
The payload carries no schema authority; the compatible graph schema is
established separately as required by §10. Concurrent commits after `H` are
picked up on the next poll. The caller durably installs the entity snapshot
before it durably installs the resume cursor. If exact snapshot construction
fails or cleanup removes a participant, the handshake returns no usable
cursor.

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

- `HistoricalDataReclaimed { graph_commit_id, type: { id, name } }` for direct
  time travel to a graph snapshot whose required type lifetime is gone;
- `ChangeFeedGap { cursor, first_unreadable_commit_id }` when
  a feed cannot continue contiguously.

The public error names graph concepts. Exact physical paths and table versions
may appear in operator diagnostics/logs, not in the public CDC contract.

The recovery action is §5.4's exact baseline handshake, not a suggested commit
ID that can race before export. Computing the oldest contiguous resumable suffix
requires walking and validating real participant pins; if a later
implementation exposes that answer, it must cache and cost-test the derivation
rather than guessing from HEAD arithmetic or table minima.

A page is atomic. If an endpoint becomes unreadable while constructing it, the
request returns the typed gap and no page token or cursor advancement; the
caller retries from its previously durable cursor or deliberately resets from
a snapshot. The engine streams scans into one bounded page buffer, but a
transport does not publish that page or either continuation until construction
succeeds.

`commit list` continues to list durable lineage even when old table data is no
longer readable. This RFC does not add `--mark-readable`: determining that for
every historical commit is a history walk, not a cheap annotation.

## 7. Time semantics

Exact time travel by graph commit ID or graph snapshot target remains the
authoritative contract. Physical manifest/table versions stay private.

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

Its current table-shaped `ChangeSet` / `EntityChange` fields are transitional.
The canonical public entity shape in §3 does not carry `table_key`, table or
incarnation IDs, manifest/table versions, or physical ordering fields. C2
introduces the finite adjacent-commit entity diff with that graph shape rather
than expanding the table-shaped result into HTTP/OpenAPI/SDK contracts.

C0 may still use immutable table identity to locate data internally. Public
ordering and output are by graph type identity and logical entity ID.

Once the graph feed is proven, its exact adjacent enumerator supplies most of
the same machinery. That does **not** make arbitrary-range net diff a free
operation algebra. Update-then-revert must disappear; delete-then-reinsert may
be update relative to the range baseline; table lifetimes can change; and
intermediate history may be reclaimed while both endpoint snapshots remain
readable.

Any future feed-backed net diff must therefore reduce against baseline
membership/images and final membership/images, not merely fold operation
labels. The direct endpoint snapshot algorithm remains a valid and often
lower-cost reconciliation path. Both paths share graph type-lifetime interval
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
- `DatasetDelta` batch-row, batch-byte, blob, and system/storage-only-column
  observations. If bounds cannot be enforced, select the bounded scanner
  adapter before C1.
- Exact insert after-images, update before/after images, and delete
  before-images, including nested values, blobs, edge endpoints, null versus
  valid empty, and exclusion of exact reserved system fields without hiding a
  legal `_row*` user property.
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
- Type/property rename and drop/re-add are schema operations, not synthetic
  entity updates/deletes/inserts. An unprovable schema boundary is a typed
  refusal and cannot collide graph type identities.
- Physical-only commits emit empty blocks and still advance the cursor.
- Bounded graph NDJSON, ordinary mutation/load, branch merge, physical
  maintenance, and recovery-completed publication share the block model within
  a proven compatible schema.

### P0 — cursor and pagination

- Page-token/cursor cross-use and scope mismatches are typed refusals. A finite
  commit diff never returns or accepts a feed cursor.
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
- SDK/CLI helpers consume page tokens automatically; the feed exposes no
  advanced durable cursor before the terminal page.
- Sparse filters stop at the commits-scanned bound.
- Reopen and another process can resume from the caller's cursor with no server
  state.
- DTO/OpenAPI tests reject `table_key`, table/incarnation IDs, physical
  versions, row addresses, `part`, `commit_complete`, and caller byte limits in
  the graph entity contract.

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

Before C3 exposes a public feed, use `helpers::cost` and realistic history depth
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

Entity-data diff and graph-schema diff are separate public contracts. The
entity surface emits no type/property/constraint/annotation operations and does
not turn table add/drop/rewrite into synthetic entity changes. It therefore
cannot recreate an empty graph by itself.

Historical decoding is a different concern. To return an old entity image
honestly, the engine must interpret that snapshot with its matching logical and
physical schema rather than today's catalog. That is internal correctness
machinery: the result contains the graph entity image, not a schema object,
schema version, table field ID, or user-selectable decoder mode.

Today's lineage does not retain accepted SchemaIR for every commit. Therefore
the entity diff/feed phases cross only snapshot pairs whose compatible logical
schema can be proven; an unprovable schema boundary returns a typed refusal. It
is not guessed from table layout and does not silently fall back to the current
schema.

A schema-feed extension must decide:

- historical SchemaIR identity and schema-change event encoding;
- bootstrap semantics for a consumer starting from an empty store;
- whether a full schema snapshot rides every change or only schema commits;
- retention and rebuild behavior for schema history;
- whether the required authority earns an internal-format strand.

It must not infer graph identity from Lance field IDs or physical column names.
Until that extension lands, the entity feed is replayable only within a proven
compatible graph schema established out of band.

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

The C0–C4 core below persists nothing and therefore requires no internal-schema
or recovery-schema bump:

- lineage and table pins already exist;
- graph type identity already exists in accepted SchemaIR and is projected
  opaquely rather than persisted again;
- runtime path/cursor indexes are derived and rebuildable;
- page tokens and cursors are caller-owned wire values;
- typed errors and new read APIs are additive.

Opaque page tokens and cursors have separate wire versions and decoders. An
unsupported version or cross-use is a typed error, not best-effort decoding.

Any implementation that proposes a stored watermark, feed offset, operation
summary, delete tombstone, or historical SchemaIR changes this conclusion and
must return to this RFC's format audit before landing.

## 12. Phasing

| Phase | Ships | Safe stop |
|---|---|---|
| C0 — foundation correction | Graph type-lifetime pairing with deterministic graph-semantic ordering; O(1) adjacent first-parent validation; Lance surface guards; remove speculative binary lifting | No new public API or persisted state; physical placement remains private |
| C1 — private enumerator | Internal graph commit blocks with exact logical images, exact-end insert/update adapter, complete delete fallback, bounded page positions, typed gaps, and schema-compatibility refusal | Engine-only correctness before wire commitment |
| C2 — finite commit entity diff | Graph-vocabulary DTO, exact commit-vs-first-parent diff, next-page token, bounded SDK/CLI auto-pagination, HTTP/OpenAPI, authorization, docs, and parity tests | Useful audit/inspection surface without a durable feed protocol |
| C3 — public entity feed | First-parent feed cursor plus exact snapshot/cursor baseline and at-least-once consumer contract | Useful caller-owned feed within a proven compatible schema |
| C4 — entity history | Newest-first history derived from the same per-commit enumerator, with separate backward continuation domains | Investigation surface; no new storage authority |
| C5 — publication time, optional | Derived `published_at` and possibly a measured as-of-time selector | Lands only after its semantics and cold-history cost pass §7 |
| C6 — schema replay, separate decision | Historical SchemaIR authority and schema-change events | Requires its own format conclusion before implementation |

C0 deliberately does **not** add a second coordinator or an O(history log
history) ancestry index. `CommitGraph` already holds the warm lineage
projection; the feed may add at most the minimal first-parent navigation view
whose cost is justified by C1.

## 13. Resolved decisions

1. Public vocabulary: graph commit, node/edge type, opaque graph type identity,
   logical entity ID, endpoints, and logical values—not table/dataset machinery.
2. Merge default: first parent only.
3. Cause placement: once per block.
4. Commit time field in v1: `authored_at`; no false `committed_at` label.
5. Continuation: a page token continues one bounded result; a separate opaque
   caller-owned cursor resumes only the durable feed after a complete commit.
6. Delete authority: exact begin/end logical-ID comparison; transaction history
   may only prove that comparison unnecessary.
7. Retention: validate concrete participant versions; no scalar watermark.
8. Existing `diff`: distinct net-current API with shared primitives, no
   optional multi-commit attribution.
9. Row images: insert after, update before and after, delete before; edge
   endpoints follow each image.
10. Entity-data and graph-schema diffs are separate. Historical schema decoding
    is private correctness machinery, not a public schema-history mode.
11. Reset: one exact entity-data snapshot and its `AfterCommit` cursor are
    captured together; a bare head ID is not a safe bootstrap.
12. Format: no bump for the entity feed; revisit before persisting any new
    history authority.

---

## 14. Implementation amendment (2026-08-15)

C0 through C3 shipped on the surveyed contract. Details frozen by the
implementation, recorded here so later phases inherit them:

- **v1 derivation is the exact ordered-merge authority path only.** No
  row-version candidate pruning and no transaction-interval no-delete proof
  shipped; both remain the sanctioned optimizations of §4.2/§4.3. The cost
  instrument (`changes_cost.rs`) pins the O(table-extent) scan term as a
  growing tripwire that the pruning slice must flip to a flat assertion, and
  pins bounded per-page opens, Blob-lazy payload work, data-flat caught-up
  polls, and the one-manifest-snapshot-per-commit backlog term.
- **Typed structural equality** uses Arrow logical equality on one-row
  slices for non-Blob user columns and physical descriptor identity with an
  exact payload tie-break for Blob columns. Float comparison is bitwise.
  Managed Blob descriptor fields (`position`/`size`/`blob_id`) are resolved by
  Lance RELATIVE to the owning data file (via the row's fragment), so the
  identity is qualified with the owning fragment id (high 32 bits of
  `_rowaddr`, projected via `with_row_address`). Without that, a same-length
  Blob-only update that moves the row to a new fragment with colliding local
  descriptor coordinates would be misread as unchanged and silently dropped
  from the diff and feed. External references resolve by URI independently of
  placement, so their identity stays source-independent.
- **Strict schema gate:** paired lifetimes must share one user-schema
  fingerprint (name-keyed Arrow type + nullability + stable property marker +
  Blob marker; the five reserved Lance virtual columns excluded); a non-empty
  added or removed lifetime refuses the commit; empty ones emit nothing. The
  gate ignores the request filter — a boundary is a property of the commit
  pair. The gate runs over ALL changed intervals before any emission, on
  every page.
- **Continuations:** three payload kinds (commit page token, feed cursor,
  feed page token) share one checksummed opaque envelope with kind and
  version tags; every cross-use, corruption, scope, witness, or digest
  mismatch is one typed rejection surfaced as a stable-prefix 400. The commit
  page token also binds the filter digest. The feed cursor binds the hashed
  graph identity domain, the first-parent genesis, `changes/forward`, the
  branch name plus its Lance-native incarnation witness (main uses a fixed
  witness), the filter digest, and the last complete commit. In-commit
  continuation positions are keyed by the PUBLISHED opaque type identity —
  the same key that orders emission — so a token's decodable payload carries
  no numeric table or incarnation component (the appended SHA-256 is
  integrity, not encryption), honoring §4.4's payload exclusion literally.
- **Continuations bind position, not page bounds.** §5.1 lists "enforced
  bounds" among a page token's bindings; v1 deliberately deviates: tokens
  bind identity, scope, and position, while row limits stay per-request
  (server-clamped) and byte ceilings stay server-owned. Replaying one token
  is therefore position-stable — the continuation resumes at exactly the same
  event — but not page-size-stable. Binding bounds would reject legitimate
  client reconfiguration (a smaller limit on retry after a timeout, an SDK
  default change) without any correctness gain, since resume position is
  limit-independent.
- **Feed stop rules:** a mid-block page carries only a page token and its
  block rides partially with its cause; a boundary page carries the cursor
  plus a `caught_up` flag; an unreadable or boundary-refused commit surfaces
  its typed error only as the FIRST commit of a poll, otherwise the page ends
  atomically at the previous boundary and the next poll surfaces it. Commits
  examined per poll are bounded (128 default / 512 ceiling, server-owned).
- **Baseline framing:** the served handshake streams over the bounded export
  transport and appends exactly one terminal `{"baseline": …}` NDJSON record,
  sent only after every snapshot record succeeded. The snapshot honors kind
  and type-name scope; `op` binds only the cursor. Baselines require the
  `export` policy action; the diff and feed require `read`, with the commit
  diff authorizing against the commit's authored branch because it returns
  row images.
- **Strict wire hygiene:** the change routes reject unknown query parameters
  outright, and two vocabulary gates (response-walk and OpenAPI-walk) keep
  physical storage vocabulary structurally absent from the contract.
- **Known dependency for retention semantics:** historical snapshots resolve
  by checking out `__manifest` at the commit's manifest version, so retained
  manifest version history is a readability participant alongside table
  versions. Bringing internal tables into `cleanup` must account for this
  before reclaiming manifest versions.
- **OPEN — §4.4 boundedness obligation is not discharged.** §4.4 requires the
  ordering path to be provably bounded and to "not sort an unbounded graph
  commit in memory." The shipped enumerator streams the *merge* and applies
  page limits before building any delta-wide `Vec`, but it obtains its two
  ordered inputs from Lance's `order_by` scan — and that scan, on pinned Lance
  10.0.0, materializes the whole projected table in one single-partition
  `SortExec` backed by an `UnboundedMemoryPool` with spill structurally
  disabled (no public `Scanner` knob, no env override). So resident memory is
  O(table projected width), embeddings included, not O(page). This is
  pre-existing shared debt: branch merge (`OrderedTableCursor`), the legacy
  commit diff, and export all use the identical scan shape, and it is the
  mechanism behind the recorded `branch_merge` embedding OOM. Closing it needs
  one of: an upstream ask to expose scanner spilling (the `FairSpillPool` +
  `DiskManager` machinery already exists — Lance uses it for `merge_insert`);
  an upstream index-ordered scan that elides the sort; or an OmniGraph-side
  cursor-chunked ordered read (`id > after AND id <= bound` + `limit(k)`, which
  DataFusion folds into a bounded TopK) that fits the feed's existing `after_id`
  continuation but would need `changes_cost.rs`/`merge_cost.rs` re-measurement.
  See [merge-complexity.md](../dev/merge-complexity.md) for the full source
  citations. Until then, the practical bound is the 8,192-row / 32 MiB Mutation
  ceiling on the *write* side; large historical tables can exceed the read-side
  memory envelope.
