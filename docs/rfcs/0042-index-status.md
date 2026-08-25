---
rfc: "0042"
title: "Read-only index status"
track: maintainer
status: draft
implementation: not-started
authors:
  - Azim Afroozeh
created: 2026-08-25
updated: 2026-08-25
discussion: "https://github.com/ModernRelay/omnigraph/issues/550"
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0042: Read-only index status

> Number provisional: 0042 is the next available per this README at
> base `bb0e3dc8` (0040 is reserved by PR #546); re-verify at PR-open
> time and renumber if raced. Remove this note before merge.

> A term set in ***bold italics*** is being defined at that exact spot;
> it is used in plain text everywhere after.

## Summary

OmniGraph gains a read-only index-status surface: `omnigraph index
status` and a served `GET` route report, per index (declared or
not), a ***state*** (`ready`, `unbuildable`, `missing`, `degraded`), a
machine-readable reason, and coverage counts, derived entirely from
published state. `omnigraph index await` blocks until the gating
states (`missing`, `degraded`) clear, or exits nonzero on timeout.

The unchanged boundary: this surface observes, never builds.
`optimize` remains the only index reconciler; no background scheduler,
no new authority, no stored work list.

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
asks for a read-only alternative: its
control-plane POC loads a graph, traversals warn about full scans
(the edge `id`/`src`/`dst` BTREEs are unbuilt), and automation has no
way to see that without mutating. #486 is the monitoring twin: a
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
flags, addressed per RFC 0011. `--branch` defaults to the graph's
default branch, as the `snapshot` command does. Human output: one
row per (table_key, index). `--json` emits one envelope object: the
snapshot identity (`graph_branch`, `graph_manifest_version`) plus an
`indexes` array of per-index rows; embedded and served output share
this envelope. Under snapshot addressing, `graph_branch` is null
when the snapshot resolves to no branch. A `degraded` row:

```json
{
  "table_key": "edge:knows",
  "name": "src_idx",
  "kind": "btree",
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

Field notes:

- `table_key`: `node:<type>` or `edge:<type>`.
- `kind`: `btree` | `fts` | `vector`; `other` for a
  present-but-undeclared index whose Lance type is outside this set.
- `reason` is a token, `message` its human reading. OmniGraph mints
  the token vocabulary here; today's free-string reconciler reasons
  (`PendingIndex.reason`) map to tokens, string preserved in
  `message`. `message` is free text for humans, never contract
  (rule 9).
- `updated_at`: RFC 3339 UTC, converted by OmniGraph from the
  millisecond timestamp on the index's latest ***segment*** (one
  immutable build unit; an incrementally reindexed index is several
  segments under one name); null when the index was never built or
  its segments predate Lance's `created_at` field.
- For `missing` and `unbuildable` rows no physical index exists: `name`
  and `updated_at` are null, the indexed counts are 0, and the
  unindexed counts are the dataset's logical totals (the rows that
  full-scan today).
- `declared: false`: present in the dataset, no longer declared (its
  `@index` intent was dropped by a schema change).

The states:

| state | meaning | operator response |
|---|---|---|
| `ready` | built, covers every current fragment | none |
| `unbuildable` | declared, cannot be built as things stand (reason says why, e.g. a vector index whose property has no non-null vectors to train on) | load data, then `optimize` |
| `missing` | declared (schema `@index`, or a fixed node/edge BTREE), never built | `optimize` |
| `degraded` | built, below full quality; today's one reason is `uncovered_fragments` (rows appended or rewritten after the build, scanned until reindex) | `optimize` |

The `unbuildable` state corresponds to `optimize --json`'s
`pending_indexes` field; the word `pending` is deliberately unused
on this surface, reserved for a true in-progress meaning should a
background builder ever exist. When #486 lands its detection, a
degenerate vector index reports `degraded` / `mono_partition` with
no schema change.

`status` exits 0 whenever a report is produced; states are data, not
errors. Producing no report is a typed error that exits nonzero, in
the same failure family as `snapshot` (exit codes owned by the CLI
reference, `docs/user/cli/reference.md`): unresolvable URI, unknown
branch or snapshot, empty root, or a root holding only orphan
artifacts (files present, no `__manifest` Create committed).

```
omnigraph index await [<uri>] [--branch <b>] [--timeout <dur>] [--json]
```

Blocks until every gating index is `ready`, then exits 0. `missing`
and `degraded` gate (`optimize` can clear them); `unbuildable` never
gates: waiting cannot build it, only more data can, and blocking on it
would hang the pipeline this command serves. `unbuildable` indexes
appear in the final report. Zero declared indexes satisfies `await`
immediately. `await` rejects `--snapshot`: a frozen snapshot's
states can never change, so waiting on one is meaningless. Without
`--timeout` it waits indefinitely (CI should always pass one).
Timeout exits with its own code, distinct from the `snapshot`
failure family and owned by the same CLI-reference exit-code
section, printing the last report to stderr, in the `status`
envelope when `--json` is set and human rows otherwise.

`await` complements `optimize`, never replaces it (the observe-only
boundary above): the pipeline is load, `optimize`, `await`. With
nothing building, `await` runs out its timeout and fails.

### Server

`GET /index-status` (single-graph) and
`GET /graphs/{graph_id}/index-status` (multi-graph) return the same
envelope as the CLI (identity fields in the `SnapshotOutput`
vocabulary). Both routes accept `branch`
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
  indistinguishable, the correct fail-closed reading.

Each report runs under snapshot isolation: it derives from one pinned
snapshot for its whole lifetime, a consistent cut across all
datasets, never torn by a concurrent publish.

A zero-row dataset reports its declared indexes `ready` with zero
counts, vacuously covered: no rows exist to cover, and the
reconciler skips empty tables, so any other classification would
hang `await`. First rows flip the state to `missing`, or straight
to `unbuildable` for a vector index whose rows still hold no
non-null vectors.

Counts are logical rows (physical rows minus deletions, from
fragment metadata); a deleted row still in an index is not coverage.
Computing a logical count may read small deletion sidecars, never
column data.

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
6. `await` gates on `missing` and `degraded` only; `unbuildable`
   never gates.
7. Neither `status` nor `await` ever triggers reconciliation.
8. System indexes are omitted; a present-but-undeclared index is
   reported with `declared: false`.
9. `message` is human-readable free text, never contract; consumers
   must not parse it.

## Design

A graph is many Lance datasets (one per node and edge type)
coordinated by `__manifest`. Per report:

1. Resolve one snapshot from `__manifest`: published `table_version`
   per `(table_key, table_branch)` plus the accepted schema. The
   declared set = the schema's `@index` intent + the fixed BTREEs
   (`id` on node datasets; `id`/`src`/`dst` on edge datasets).
2. Per dataset, read the Lance manifest at the published version. Its
   index section holds one `IndexMetadata` per index segment: name,
   typed `index_details` (kind), and `fragment_bitmap`, the Roaring
   bitmap of covered fragment ids.
3. Classify each declared index, first match wins. A zero-row
   dataset short-circuits: its declared indexes report `ready` with
   zero counts (per Semantics). Otherwise: listed in the
   reconciler's `PendingIndex` set per
   `index_work_status_on_dataset_for_catalog`
   (`PendingIndex { type_key, property, reason }`), `unbuildable`;
   absent from the manifest and buildable, `missing`; every current
   fragment id in the union of its segments' bitmaps, `ready`;
   uncovered fragments (`TableStore::has_unindexed_fragments` /
   `IndexCoverage::Degraded`), `degraded`. Outside the declared set:
   system indexes (Lance-internal bookkeeping indexes, the
   `is_system_index` set) are omitted (rule 8); a
   present-but-undeclared index reports `declared: false`, state
   from coverage alone. The reconciler still maintains undeclared
   indexes; reporting them keeps that drift visible.
4. Counts: fragments from bitmap containment; rows by summing
   logical fragment row counts, the `index_statistics` arithmetic. No column
   data, one bounded exception: `unbuildable` classification reuses the
   reconciler's trainability probe, a filtered null count over the
   vector column. The per-segment plugin statistics path (which can
   open index files) is not used.

Lance owns the index metadata, bitmaps, and statistics; OmniGraph
adds the catalog-intent diff, state classification, and graph-level
aggregation. The inputs are the checks `optimize` already runs,
exposed through a read-only engine entry point that widens their
return shapes: per-index rows where `IndexWorkStatus` exposes only
`needs_commit` plus the `pending` list, per-index coverage where
`has_unindexed_fragments` is a dataset-level boolean. Shared checks
keep endpoint and reconciler aligned; the widened shapes are new
code, covered by the truth-table tests.

Caching: physical coverage is cacheable per `(table_key,
table_branch, table_version)`, because index builds become visible
through the same single atomic `__manifest` publication (the
publication door) as data, so coverage cannot change without the
version moving. Declared intent is not covered by that key, because
an index-only schema change (an `@index` addition applies as pure
metadata, touching no table data) bumps no table version. Full key: `(table_key, table_branch, table_version,
accepted-schema version)`. Warm poll: one `__manifest` read plus
cache hits. The cache is a hint keyed by immutable versions, never
authority.

`await` is a poll loop with backoff over the same derivation, in the
CLI in both modes; it is stateless: no lock, no server-side session,
no persisted cursor.

## Invariants

- **Invariant 7** (physical acceleration is derived state) is the
  foundation, unchanged: missing coverage changes cost, not
  correctness; index work still happens only through explicit
  reconciliation. This surface makes that derived state observable.
- **Invariant 12**: derived on demand from Lance and `__manifest`;
  the cache is immutable-pinned, a hint, never commit authority; no
  shadow copy.
- **Invariant 10**: the served route resolves the actor at the HTTP
  boundary and applies the server's Cedar read policy, like every
  read route.
- **Deny-list, "a job queue for state derivable from accepted
  manifest state"**: this RFC is the compliant shape; the work list
  is derived every time (stored alternatives rejected below).
- **Deny-list, "a logical precondition based on physical index
  coverage"**: `await` is an operator pipeline gate outside the
  engine. The item's other members (fragment count, cache entry,
  staged layout) are engine-internal preconditions; this RFC adds
  none. No engine operation, planner choice, or mutation precondition
  consumes the status.

No invariant is weakened. The support boundary "physical index
reconciliation is explicit; there is no background scheduler whose
queue is a second authority" is unchanged.

## Compatibility and reversibility

Additive only: one CLI verb pair, one GET route, new
`omnigraph-api-types` response types; no storage or wire format
change. The JSON field set and state vocabulary are the compatibility
surface, governed by rules 3 to 5. Reverting is technically cheap but
breaks gating scripts, so removal means deprecation, not deletion:
the reversible end of the evidence-demand scale.

## Alternatives

- **Do nothing**: polling `optimize --json` mutates on every check,
  conflating observing with reconciling.
- **Extend `snapshot`**: `SnapshotOutput` is version topology;
  per-index derived-state health has a different row shape and
  cadence.
- **Expose Lance `index_statistics` raw**: misses the catalog side;
  `missing` and `unbuildable` live in schema intent, invisible to
  Lance.
- **Stored work list / progress sidecar**: a shadow copy (invariant
  12's term) of derivable state that can lie after a crash, needs its
  own concurrency control, and is deny-listed; derivation is too
  cheap to be worth caching durably.

Out of scope, compatible later (deliberately not `blocked_on`):

- **In-flight progress** (heartbeat object for a running
  `optimize`): real machinery, own lease-staleness questions.
- **Index events on the change feed** (RFC 0030): builds already
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
  zero counts), `declared: false`, and `None` `fragment_bitmap`
  (coverage unreportable, treated as covered, mirroring
  `has_unindexed_fragments`).
- Integration, extending the existing CLI test owner: fresh load
  reports `missing`; post-`optimize`, `ready`; post-append,
  `degraded`/`uncovered_fragments`; `await` returns after the
  publish, ignores an `unbuildable` vector index, times out nonzero when
  nothing builds.
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
   internal to the engine crate); embedded CLI `index status`.
2. Server route + `omnigraph-api-types`; CLI `--server` path.
3. CLI `index await`.

Each phase ships alone; `implementation` advances as each lands.

## Unresolved questions

None.

## Decision log

- 2026-08-25: drafted from the #550 request (read-only index readiness
  status plus a blocking wait command) and the #486 monitoring gap.
