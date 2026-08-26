---
rfc: "0042"
title: "Read-only index status"
track: maintainer
status: draft
implementation: not-started
authors:
  - Azim Afroozeh
created: 2026-08-25
updated: 2026-08-26
discussion: "https://github.com/ModernRelay/omnigraph/issues/550"
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0042: Read-only index status

> A term set in ***bold italics*** is being defined at that exact spot;
> it is used in plain text everywhere after.

## Summary

OmniGraph gains a read-only index-status surface: `omnigraph index
status` and a served `GET` route report, per index (declared or
not), a ***state*** (`ready`, `unbuildable`, `missing`, `degraded`), a
machine-readable reason, and coverage counts, derived entirely from
published state. `omnigraph index await` blocks until every declared
index leaves the gating states (`missing`, `degraded`), or exits
nonzero on timeout.

The unchanged boundary: this surface observes, never builds.
`optimize` remains the only index ***reconciler***, the component
that compares index intent against the indexes actually
present and builds or maintains what closes the gap; no background
scheduler, no new authority, no stored work list. One owned
exception rides along: `optimize` gains a repair for indexes
carrying no coverage record (Compatibility owns it).

## Motivation

The only structured index readout today is `pending_indexes` in
`optimize --json`, so observing index state requires a command that
also commits: `optimize` compacts fragments, builds indexes, and
publishes the result through `__manifest`. A health signal with write
side effects cannot be polled freely, cannot run under a read-only
credential, and violates command-query separation (the rule that a
query returns a result without mutating state, while a command
mutates): the readout can build the very index it was asked about,
so its answer describes a state the question just changed. Issue #550
asks for a read-only alternative: a query
warning reports a condition for one query, not index readiness
across the graph, and automation has no read-only way to see
graph-wide readiness. #486 is the monitoring twin: a
degenerate mono-partition vector index is invisible to operators.
#533 is adjacent, not overlapping: planner behavior when indexes
exist, versus this RFC's "do they exist and what do they cover".

New CLI and HTTP surface meets the RFC bar in [README.md](README.md);
the lasting liability is the JSON contract automation will script
against.

## User and operational behavior

### CLI

```
omnigraph index status [<uri>] [--branch <b> | --snapshot <s>] [--json]
```

The command runs in capability class "any": embedded against the
positional URI, or served via the global `--server`/`--profile`
flags, addressed per [RFC 0011](0011-cli-addressing-and-config.md). `--branch` defaults to the graph's
default branch, as the `snapshot` command does. Human output: one
row per index (identity per rule 10). `--json` emits one envelope object: the
snapshot identity (`graph_branch`, `graph_manifest_version`) plus an
`indexes` array of per-index rows; embedded and served output share
this envelope. Under snapshot addressing, `graph_branch` is null
when the snapshot resolves to no branch. A `degraded` row:

```json
{
  "table_key": "edge:knows",
  "name": "src_idx",
  "kind": "btree",
  "fields": ["src"],
  "declared": true,
  "state": "degraded",
  "reason": "uncovered_fragments",
  "message": "3 fragment(s) not covered by the index on 'src'",
  "indexed_rows": 4000000,
  "unindexed_rows": 120000,
  "indexed_fragments": 40,
  "unindexed_fragments": 3,
  "updated_at": "2026-08-25T09:00:00Z"
}
```

An `unbuildable` row (no physical index exists yet):

```json
{
  "table_key": "node:Person",
  "name": null,
  "kind": "vector",
  "fields": ["embedding"],
  "declared": true,
  "state": "unbuildable",
  "reason": "no_trainable_vectors",
  "message": "property has no non-null vectors to train on yet",
  "indexed_rows": 0,
  "unindexed_rows": 4000000,
  "indexed_fragments": 0,
  "unindexed_fragments": 40,
  "updated_at": null
}
```

Field notes, identity:

- `table_key`: `node:<type>` or `edge:<type>`.
- `kind`: `btree` | `fts` | `vector`; `other` for a
  present-but-undeclared index whose Lance type is outside this
  set, or whose `index_details` is absent (legacy metadata: an
  unknowable kind reads as outside the set, default deny).
- `fields`: the indexed column or columns, in index order; a
  composite `@index(a, b)` lists both (composites classify
  `unbuildable`, per the Design cascade). Sourced from the declared
  intent when no physical index exists, from the Lance index
  metadata otherwise.
- (`table_key`, `kind`, `fields`) identifies an index across
  reports and states (rule 10); `name` never identifies, null
  until the first build.

Field notes, contract fields:

- `reason` is a token, `message` its human reading. OmniGraph mints
  the token vocabulary here (the reasons table below); today's
  free-string reconciler reasons
  (`PendingIndex.reason`) map to tokens, string preserved in
  `message`: the widened per-index rows carry a typed reason minted
  at the classification site, and the legacy string is never
  parsed. `message` is free text for humans, never contract
  (rule 9). Neither field is ever omitted.
- `updated_at`: RFC 3339 UTC, converted by OmniGraph from the
  millisecond timestamp on the index's latest ***segment*** (one
  immutable build unit; an incrementally reindexed index is several
  segments under one name); null when the index was never built or
  its segments predate Lance's `created_at` field.
- `declared: false`: present in the dataset, not declared (its
  `@index`/`@key` intent was dropped by a schema change, or it was
  created outside omnigraph).

The states:

| state | meaning | operator response |
|---|---|---|
| `ready` | built, covers every current fragment | none |
| `unbuildable` | declared, cannot be built or rebuilt as things stand; the reason says why | per reason (below) |
| `missing` | declared (schema `@index`/`@key`, or a fixed node/edge BTREE), never built | `optimize` |
| `degraded` | built, below full quality; the reason says why | `optimize` |

The reasons (the owning list of today's token vocabulary; open set
per rule 4):

| reason | state | meaning | operator response |
|---|---|---|---|
| `uncovered_fragments` | `degraded` | rows appended or rewritten after the build, scanned until reindex | `optimize` |
| `coverage_unknown` | `degraded` | no coverage record (`fragment_bitmap: None`); see Semantics | `optimize` (the Repair procedure) |
| `no_trainable_vectors` | `unbuildable` | the property has no non-null vectors to train on yet | load data, then `optimize` |
| `composite_unsupported` | `unbuildable` | multi-column `@index`; the reconciler builds single-column indexes only | amend the schema |
| `edge_index_unsupported` | `unbuildable` | property `@index` on an edge type; edge datasets receive only the fixed `id`/`src`/`dst` BTREEs | amend the schema |
| `type_unindexable` | `unbuildable` | property type with no index kind (a list, or `Blob`) | amend the schema |

Per-row shape, the value rules for every case (counts are the four
`indexed_`/`unindexed_` fields; "per `created_at`" means set when
Lance recorded a timestamp, null for older metadata):

| row case | `name` | `reason`/`message` | counts | `updated_at` |
|---|---|---|---|---|
| `ready` | set | null | set | per `created_at` |
| `missing` | null | null | indexed 0; unindexed = the dataset's logical totals (the rows that full-scan today) | null |
| never-built `unbuildable` | null | set | indexed 0; unindexed = the dataset's logical totals | null |
| built `unbuildable` (an untrainable `None`-bitmap vector index) | set | set | all null (the `None`-bitmap rule) | per `created_at` |
| `degraded` / `coverage_unknown` | set | set | all null | per `created_at` |
| `degraded` / `uncovered_fragments` | set | set | set | per `created_at` |

The `no_trainable_vectors` reason corresponds to `optimize
--json`'s `pending_indexes` field; the other `unbuildable` reasons
have no reconciler counterpart, which is part of why this surface
exists. The word `pending` is deliberately unused
on this surface, reserved for a true in-progress meaning should a
background builder ever exist. When #486 lands its detection, a
degenerate vector index reports `degraded` / `mono_partition` with
no schema change.

`status` exits 0 whenever a report is produced; states are data, not
errors. Producing no report is a typed error that exits nonzero, in
the same failure family as `snapshot` (exit codes owned by the CLI
reference, `docs/user/cli/reference.md`, which gains its exit-code
section in phase 1): unresolvable URI, unknown
branch or snapshot, empty root, a root holding only orphan
artifacts (files present, no `__manifest` Create committed), or a
snapshot whose tables cannot bind to today's schema.

```
omnigraph index await [<uri>] [--branch <b>] [--timeout <dur>] [--json]
```

Blocks until no declared index is in a gating state, then exits 0.
`missing` and `degraded` gate (`optimize` can clear them);
`unbuildable` never gates (waiting cannot build it, only more data
or a schema change can, and blocking on it would hang the pipeline
this command serves), and a `declared: false` row never blocks
(drift is reported, not gated). Exit 0 therefore guarantees that
every declared index is `ready` or `unbuildable`, never that all
are `ready` (rule 6); a pipeline that cannot accept an unbuildable
index fails on the final report, not the exit code. On exit 0 that
report prints to stdout, human rows or the `status` envelope under
`--json`. Zero declared indexes satisfies `await` immediately. `await` rejects `--snapshot`: physical state under a
frozen snapshot can never change, so a gating state can never
clear and waiting on one is meaningless. Without
`--timeout` it waits indefinitely (CI should always pass one).
Timeout exits with its own code, distinct from the `snapshot`
failure family and owned by the same CLI-reference exit-code
section, printing the last report to stderr, in the `status`
envelope when `--json` is set and human rows otherwise.

`await` complements `optimize`, never replaces it (the observe-only
boundary above): the pipeline is load, `optimize`, `await`. With
nothing building, `await` runs out its timeout and fails.

### Server

`GET /graphs/{graph_id}/index-status` returns the same
envelope as the CLI (identity fields in the `SnapshotOutput`
vocabulary); the server is cluster-only, so the route nests under
`/graphs/{graph_id}` like every per-graph read, with no flat
variant. It accepts `branch`
and `snapshot` query parameters with the CLI's defaulting. Gated
like every read route: bearer auth resolves the actor at the
boundary, then the server's Cedar read policy action applies. Served
`await` runs its poll loop in the CLI over this route.

### Semantics

The report reads ***committed state only***: facts a `__manifest`
publication or Lance commit has made visible. A mid-run `optimize` is
invisible; until its publish lands, its output is orphan artifacts in
the bucket. Consequences:

- The report may say `missing` or `degraded` seconds before a build
  commits. Polling (or `await`) is the contract; no in-progress state
  exists.
- "Never attempted" and "crashed before publishing" are
  indistinguishable, the correct default-deny reading.

Each report runs under snapshot isolation: it derives from one pinned
snapshot for its whole lifetime, a consistent cut across all
datasets, never torn by a concurrent publish. The declared set
always comes from the currently accepted schema; snapshot
addressing pins physical state only (matching `capture_read_view`,
which binds the current catalog to a historical snapshot). A
snapshot whose tables cannot bind to today's schema joins the
no-report error family.

Counts are logical rows (physical rows minus deletions, from
fragment metadata); a deleted row still in an index is not coverage.
Computing a logical count may read small deletion sidecars, never
column data.

A zero-row dataset reports its buildable declared indexes `ready`
with zero counts, vacuously covered: no rows exist to cover, and
the reconciler skips empty tables, so a gating classification
would hang `await`. Declarations the reconciler does not build
classify `unbuildable` even at zero rows: that verdict depends
only on the schema, and it should surface before data arrives.
First rows flip the state to `missing`, or straight to
`unbuildable` for a vector index whose rows still hold no
non-null vectors.

An index segment can carry no coverage record at all
(`fragment_bitmap: None`, metadata written before Lance tracked
coverage): whether it covers the current fragments is unknowable
from metadata. Such an index classifies `degraded` /
`coverage_unknown` (or `unbuildable` / `no_trainable_vectors` for
an untrainable vector column, per the Design cascade), the default-deny
reading again: absent evidence never reads as coverage, and all
four count fields are null (partial per-segment bitmaps would
claim precision the metadata lacks). `optimize` repairs it
(Design, the Repair procedure).

`ready` asserts coverage, not planner use: Lance disables scalar
indexes for an entire scan when any fragment lacks
`physical_rows` metadata, so a `ready` index can still be bypassed
per query. That condition is planner-side observability, #533's
territory, not a coverage state.

### Contract rules

1. The report derives from committed state only; no in-progress state
   exists on this surface.
2. Every report is assembled from one pinned snapshot.
3. The state set is exactly `ready`, `unbuildable`, `missing`,
   `degraded`; it changes only by amendment to this RFC.
4. `reason` tokens are an open set; consumers must tolerate unknown
   tokens.
5. Response fields may be added; existing fields are never removed or
   retyped without deprecation.
6. `await` gates on declared indexes in `missing` or `degraded`
   only; `unbuildable` and `declared: false` rows never gate: on
   exit 0, every declared index is `ready` or `unbuildable`.
7. Neither `status` nor `await` ever triggers reconciliation.
8. System indexes (Lance-internal bookkeeping indexes, the
   `is_system_index` set) are omitted; a present-but-undeclared
   index is reported with `declared: false`.
9. `message` is human-readable free text, never contract; consumers
   must not parse it.
10. A row is identified by (`table_key`, `kind`, `fields`) across
    reports and states; `name` never identifies. The key is unique
    for omnigraph-managed indexes; a manifest holding duplicates
    (same kind and column under distinct names, which Lance
    permits) renders one row per physical index, keys colliding
    as-is.

## Design

A graph is many Lance datasets (one per node and edge type)
coordinated by `__manifest`. Per report:

1. Resolve one snapshot from `__manifest`: published `table_version`
   per `(table_key, table_branch)` plus the accepted schema. The
   declared set = the catalog's index-intent list (a property
   reaches it through `@index` or `@key`) + the fixed BTREEs
   (`id` on node datasets; `id`/`src`/`dst` on edge datasets).
2. Per dataset, read the Lance manifest at the published version. Its
   index section holds one `IndexMetadata` per index segment: name,
   typed `index_details` (kind), and `fragment_bitmap`, the Roaring
   bitmap of covered fragment ids.
3. Classify each declared index through the following cascade,
   first match wins (declared intent matches a physical index by
   column):

   a. A declaration the reconciler does not build, regardless of
      row count: `unbuildable`, reason `composite_unsupported`,
      `edge_index_unsupported`, or `type_unindexable` (the reasons
      table).
   b. A column-matched physical index whose `index_details` is
      absent: `degraded` / `coverage_unknown` (present, kind and
      coverage unknowable), never `missing`, so no duplicate is
      built beside it; the Repair procedure (below) skips it.
   c. A zero-row dataset short-circuits: its remaining declared
      indexes report `ready` with zero counts (per Semantics).
   d. Listed in the reconciler's `PendingIndex` set per
      `index_work_status_on_dataset_for_catalog`
      (`PendingIndex { type_key, property, reason }`):
      `unbuildable`.
   e. Absent from the manifest and buildable: `missing`.
   f. Any segment without a coverage record
      (`fragment_bitmap: None`): `degraded` / `coverage_unknown`;
      for a vector index whose column is currently untrainable,
      `unbuildable` / `no_trainable_vectors` instead (the repair
      cannot run until data arrives).
   g. Every current fragment id in the union of its segments'
      bitmaps: `ready`.
   h. Uncovered fragments (`TableStore::has_unindexed_fragments` /
      `IndexCoverage::Degraded`): `degraded` /
      `uncovered_fragments`.

   Outside the declared set: system indexes are omitted (rule 8); a
   present-but-undeclared index reports `declared: false`, state
   from coverage alone. The reconciler's top-up pass still maintains
   undeclared indexes, because its trigger and Lance's
   `optimize_indices` filter only system indexes, never declaration;
   creation, deliberate retrain, and the `None`-bitmap repair are
   declared-only (the Repair procedure below). Reporting undeclared
   indexes keeps that drift visible.
4. Counts: fragments from bitmap containment; rows by summing
   logical fragment row counts, the `index_statistics` arithmetic
   (null for `None`-bitmap rows, per Semantics). No column
   data, one bounded exception: `unbuildable` classification reuses the
   reconciler's trainability probe, a filtered null count over the
   vector column. The per-segment plugin statistics path (which can
   open index files) is not used.

Lance owns the index metadata, bitmaps, and statistics; OmniGraph
adds the catalog-intent diff, state classification, and graph-level
aggregation. In code the reconciler is the shared planner/builder pair,
`plan_index_work_node` / `plan_index_work_edge_on_dataset` feeding
`build_indices_on_dataset_for_catalog`, reached through `optimize`
and the `ensure_indices` entry point; the planners compute the
same declared-versus-present diff this surface reports. The inputs are
the checks `optimize` already runs,
exposed through a read-only engine entry point that widens their
return shapes: per-index rows where `IndexWorkStatus` exposes only
`needs_commit` plus the `pending` list, per-index coverage where
`has_unindexed_fragments` is a dataset-level boolean. Shared checks
keep endpoint and reconciler aligned; the widened shapes are new
code, covered by the truth-table tests.

One reconciler change rides along: the ***Repair procedure***, the
path that heals a `None` bitmap. Today
`has_unindexed_fragments` skips a `None` `fragment_bitmap`, so
a coverage-unknown index neither reports as work nor gets
repaired. This RFC specifies:

1. Trigger: `has_unindexed_fragments` widens so a `None` bitmap
   counts as uncovered, for kinds omnigraph builds (`btree`,
   `fts`, `vector`) only, decidable from `index_details` alone.
   The pre-existing uncovered-fragments arm stays kind- and
   declaration-blind, so top-up maintenance of `other`-kind and
   undeclared indexes continues.
2. Scope gates, applied at `optimize`'s call sites (catalog,
   storage handle, and snapshot in hand; a call-site skip treats
   the index as quiesced): declared indexes only (omnigraph does
   not rewrite indexes it does not own, which a rebuild would
   silently re-parameterize), and, for vector indexes, a
   trainable column (creation's probe; an untrainable column
   skips without error, the row classifying `unbuildable` /
   `no_trainable_vectors` until data arrives).
3. Mechanism: a full per-index rebuild, not the incremental
   top-up (Lance's `optimize_indices` path errors on a `None`
   bitmap rather than folding it in). The index is rebuilt from
   its own Lance metadata, same kind, same column, same name,
   with builder parameters from the declaration (the declaration
   is the authority for a declared index's shape). Lance replaces
   an index by name in one commit, so the broken segments are
   swapped out atomically and the new segments carry real
   bitmaps.
4. Exclusion: the top-up runs with an explicit
   `OptimizeOptions::index_names` include-list naming every index
   except the `None`-bitmap ones. The excluded rest (undeclared,
   `other` or unknown kind, or untrainable) is deliberately left,
   keeping `optimize` green, their rows persisting
   `coverage_unknown` (or `unbuildable`), never gating.
5. Ordering: the rebuild commits after compaction and the top-up,
   so its bitmap covers the final fragment set.
6. Outcome: for declared, buildable, trainable indexes,
   `coverage_unknown` is self-clearing: report, `optimize`, next
   report `ready` with proof.

The two sides must move together:
reporting `degraded` against today's reconciler would gate `await`
on a state `optimize` can never clear, and keeping the covered
reading would have the report claim proof the metadata cannot
give. A third reader of the bitmap, `key_column_index_coverage`
(scan pricing and full-scan warnings), keeps its covered reading:
its consumer is per-query cost estimation, the mispricing is
transient, and the first `optimize` heals the state for all three
readers. The rebuild cost is one-time, paid only by indexes whose
metadata predates coverage tracking (imported or legacy datasets;
every index built at the pinned Lance version writes its bitmap).

Caching: physical coverage is cacheable per `(table_key,
table_branch, table_version)`, because index builds become visible
through the same single atomic `__manifest` publication (the
publication door) as data, so coverage cannot change without the
version moving. Declared intent is not covered by that key, because
an index-only schema change (an `@index` addition applies as pure
metadata, touching no table data) bumps no table version. Full key: `(table_key, table_branch, table_version,
accepted-schema fingerprint)`, where the fingerprint is a content
hash of the accepted SchemaIR (no monotonic schema version exists
to cite). Warm poll: one `__manifest` read plus
cache hits. The cache is a hint keyed by immutable versions, never
authority, and unphased: phase 1 may ship derivation-only, with
the warm-poll cost holding once the cache lands.

`await` is a poll loop with backoff over the same derivation, in the
CLI in both modes; it is stateless: no lock, no server-side session,
no persisted cursor.

## Invariants

Checked against the
[architectural invariants](../dev/invariants.md):

- **Invariant 7** (physical acceleration is derived state) is the
  foundation, unchanged: missing coverage changes cost, not
  correctness; index work still happens only through explicit
  reconciliation. This surface makes that derived state observable.
- **Invariant 12** (one source of truth, cheaply derived): derived
  on demand from Lance and `__manifest`; the cache is
  immutable-pinned, a hint, never commit authority; no parallel
  copy that can drift.
- **Invariant 10** (trust is established at the boundary and
  enforced at the engine): the served route resolves the actor at
  the HTTP boundary and applies the server's Cedar read policy,
  like every read route.
- **Deny-list, "a job queue for state derivable from accepted
  manifest state, where an idempotent reconciler suffices"**: this
  RFC is the compliant shape; the work list is derived every time
  (stored alternatives rejected below).
- **Deny-list, "a logical precondition based on physical index
  coverage, fragment count, a cache entry, or staged layout"**:
  `await` is an operator pipeline gate outside the engine; this RFC
  adds no such precondition. No engine operation, planner choice,
  or mutation precondition consumes the status.

No invariant is weakened. The support boundary "Physical index
reconciliation is explicit; there is no background scheduler whose
queue is a second authority" is unchanged.

## Compatibility and reversibility

Additive but for one owned exception: one CLI verb pair, one GET
route, new `omnigraph-api-types` response types; no storage or
wire format change. The exception: `optimize`
gains the `None`-bitmap repair, so its first run over an imported
legacy dataset rebuilds indexes it previously skipped (one-time,
priced in Design). The JSON field set and state vocabulary are the compatibility
surface, governed by rules 3 to 5. Reverting is technically cheap but
breaks gating scripts, so removal means deprecation, not deletion:
the reversible end of the evidence-demand scale (evidence
proportional to reversibility, invariant 13).

## Alternatives

- **Do nothing**: polling `optimize --json` mutates on every check,
  conflating observing with reconciling.
- **Extend `snapshot`**: `SnapshotOutput` is version topology;
  per-index derived-state health has a different row shape and
  cadence.
- **Expose Lance `index_statistics` raw**: misses the catalog side;
  `missing` and `unbuildable` live in schema intent, invisible to
  Lance.
- **Stored work list / progress sidecar**: the maintained parallel
  truth the deny-list rejects: it can lie after a crash, needs its
  own concurrency control, and is deny-listed; derivation is too
  cheap to be worth caching durably.

Out of scope, compatible later (deliberately not `blocked_on`):

- **In-flight progress** (heartbeat object for a running
  `optimize`): real machinery, own lease-staleness questions.
- **Index events on the change feed**
  ([RFC 0030](0030-cdc-time-travel.md)): builds already
  commit through the publication door the feed observes.
- **Server-side long-poll for `await`**: a transport swap under an
  unchanged contract.
- **Fleet-level aggregation**: per-graph reports compose; an
  aggregate route can follow operational demand.

## Evidence and tests

- Unit: state-classification truth table over kind (`Btree`, `Fts`,
  `Vector`) times presence times coverage times buildability (a
  trainable versus untrainable vector property, so `unbuildable`
  rows are reachable in the table), including the fixed node
  `id` and edge `id`/`src`/`dst` BTREEs, zero-row datasets (`ready`,
  zero counts), `declared: false`, `None` `fragment_bitmap`
  (`degraded` / `coverage_unknown`, null counts), an `other`-kind
  `None`-bitmap index (`coverage_unknown` persists, excluded from
  the repair, never gates), a multi-column declaration, an
  edge-property `@index`, and a list/`Blob` `@index` (`unbuildable`
  with their reason tokens, at zero rows too), and two declared FTS indexes on different
  properties of one type (rows distinguished by `fields`).
- Integration, extending the existing CLI test owner: fresh load
  reports `missing`; post-`optimize`, `ready`; post-append,
  `degraded`/`uncovered_fragments`; a legacy `None`-bitmap index
  reports `degraded`/`coverage_unknown`, then `ready` with counts
  after `optimize` repairs it; `await` returns after the
  publish, ignores an `unbuildable` vector index, times out nonzero when
  nothing builds.
- Legacy `None`-bitmap fixtures cannot come from the production
  writer (it always writes bitmaps): tests construct them with a
  test-only doctored `CreateIndex` commit (Lance's transaction
  apply copies `new_indices` unvalidated) or a checked-in legacy
  fixture dataset.
- Consistency: a concurrent publish never tears a report (one pinned
  snapshot).
- Cost: "metadata and deletion sidecars only, no column data except
  the trainability probe" is backed by a checked-in IO-probe
  instrument, per invariant 13.
- Lance survey, per the Lance reading protocol, at the pinned Lance
  10.0.0 (workspace `Cargo.toml`), read at lance `d644e7a6`
  (2026-08-03): `IndexMetadata` (`fragment_bitmap`, typed
  `index_details`, delta segments, millisecond `created_at`),
  `index_statistics` arithmetic, logical-row fragment counts.

## Rollout

1. Engine: one read-only entry point over the derivation (today
   internal to the engine crate) plus the Repair procedure in
   `optimize` (Design; its widened trigger, scope gates, rebuild,
   which widens `IndexBuildSpec::FullText`/`Vector` with
   `name: Option<String>` so the repair replaces under the legacy
   name, and the `OptimizeOptions::index_names` include-list omitting
   every `None`-bitmap index); the new `omnigraph-api-types`
   response types (embedded and served share the envelope by
   construction); embedded CLI `index status`, whose served path
   rejects with a typed not-yet-served error in the `status`
   failure family until phase 2; the CLI reference's exit-code
   section, covering today's codes plus a reserved distinct
   timeout code wired in phase 3.
2. Server route; CLI `--server` path.
3. CLI `index await`.

Each phase ships alone; `implementation` advances as each lands.

## Unresolved questions

None.

## Decision log

- 2026-08-25: drafted from the #550 request (read-only index readiness
  status plus a blocking wait command) and the #486 monitoring gap.
- 2026-08-25: PR review: clarified undeclared-index maintenance (the
  top-up pass is declaration-blind, filtering only system indexes;
  creation and retrain are declared-only).
- 2026-08-26: PR review: rows gained `fields` and the (`table_key`,
  `kind`, `fields`) identity (unbuilt rows on different properties
  were indistinguishable); the `None` `fragment_bitmap` reading
  reclassified from covered to `degraded` / `coverage_unknown`
  (the covered reading claimed proof the metadata cannot give);
  the repair specified: a full per-index rebuild in `optimize`,
  committing after compaction under the legacy name
  (`IndexBuildSpec` name widening), scoped to declared indexes of
  buildable kinds with trainable columns (omnigraph does not
  rewrite indexes it does not own; the declaration is the
  parameter authority), every `None`-bitmap index excluded from
  the top-up include-list (Lance's top-up errors on a `None`
  bitmap); declarations the reconciler does not build classify
  `unbuildable` (`composite_unsupported`,
  `edge_index_unsupported`, `type_unindexable`), decided at zero
  rows (structural verdicts precede the empty-table
  short-circuit); kind-unknown metadata reads as `other`, and
  column-matched intent with absent `index_details` as
  `coverage_unknown`, never `missing` (no duplicate build);
  gating restricted to declared indexes, with `await`'s exit-0
  guarantee stated explicitly (every declared index `ready` or
  `unbuildable`, never all `ready`); the served route nests
  cluster-only under `/graphs/{graph_id}`; the declared set is the
  catalog's index-intent list (`@index` or `@key`) from the
  currently accepted schema (snapshot addressing pins physical
  state only); envelope types and the exit-code section assigned
  to phase 1.
