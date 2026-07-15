---
type: spec
title: "RFC-024 — Durable table heads"
description: Materialize one live-or-tombstoned current-state row per stable table identity inside each manifest branch and prove bounded physical lookup without weakening derived-index correctness.
status: draft
tags: [eng, rfc, manifest, write-path, versioning, migration, lance]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-024: Durable table heads

**Status:** Draft / for team review
**Date:** 2026-07-10
**Author track:** Maintainer design series
**Surveyed:** omnigraph 0.8.1 (`main`); Lance 9.0.0-beta.21 at git rev
`1aec14652dcbace23ac277fa8ced35000bea0c40`; full Lance table layout,
transaction, branching, indexing, compaction, cleanup, and object-store
specifications
**Relationship to RFC-022:** this RFC is the durable-heads decision split from
the earlier monolithic RFC-022 draft. [RFC-022](0022-unified-write-path.md)
defines the shared publisher/recovery protocol; this RFC owns the heads-format
rows, lookup, and publication boundary. Stable table identity and incarnation come from
[RFC-028](0028-stable-schema-identity.md), which is a prerequisite; this RFC
owns only head storage, lookup, and publication. It deliberately excludes
checkpoint retention, which
[RFC-025](0025-checkpoint-retention.md) reviews separately. Key fencing in
[RFC-023](0023-key-conflict-fencing.md) is also independently reviewable;
the two may share a release but do not block one another's evidence gates.
**Audience:** engine, manifest, migration, branch, and release maintainers
**Open architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

Throughout this draft, **heads format** means the first internal schema that
contains the table-head contract. RFC-028 now occupies internal schema v5, so
this RFC deliberately does not reserve a numeral. The exact later number is
assigned when the independently accepted capabilities for a release are known.

## 0. Decision summary

The current manifest is both an immutable graph journal and the place writers
ask "what is current?" Current-state resolution folds history, so its physical
cost grows with commit count even though the answer contains only one value per
table.

The heads format adds one mutable, durable `table_head` row per stable table identity inside
each native `__manifest` branch. The publisher updates the head in the same
Lance merge-insert transaction as immutable table-version rows, tombstone
events, graph lineage, and `graph_head`. The journal remains the history source;
heads become the current-state source.

The format does **not** ship merely because the logical result has O(tables)
rows. A filtered scan over a history-sized Lance table is still O(history)
physical work. The heads format is gated on a Lance-native indexed lookup whose measured I/O
is flat at history depth and whose uncovered tail is bounded and observable.

Normative decisions:

1. Heads live in `__manifest`; there is no second heads dataset and no warm
   mutable-tip authority.
2. Head state is explicitly `live` or `tombstoned` and carries an incarnation.
3. Every publish and recovery outcome updates journal and head atomically.
4. Current-state reads use heads; history reads use the journal.
5. Missing or duplicate heads in the heads format are corruption, not a reason to silently
   return to the history fold.
6. A new graph's first valid state contains complete identity-bearing journal
   rows and heads; an existing graph reaches the format only by export/init/load.
7. If RFC-023 is co-released, target initialization also verifies PK fencing;
   durable heads do not depend on that decision.
8. Checkpoint/retention markers are deferred to a separate RFC.

## 1. Problem

`__manifest` currently stores immutable table-version and tombstone rows. To
resolve a branch tip, readers scan those rows, select the greatest version per
table, and apply tombstones. This makes a write's coordination cost depend on
the total graph history. Compaction reduces fragment count but cannot remove
the semantic journal rows, so even a compacted manifest retains a row-volume
slope.

Caching that fold as mutable in-process state is the wrong authority shape for
writes: invalidation becomes a correctness condition across processes and
branch incarnations. Storing the folded answer durably in the same transaction
as the journal removes both liabilities:

- no parallel authority can drift; and
- current-state work is proportional to catalog width, provided physical
  lookup is also bounded.

## 2. Scope and non-goals

In scope:

- heads-format table-head schema and state transitions;
- atomic publisher, recovery, and current-read semantics;
- bounded physical access proof;
- strict-format refusal and export/init/load rebuild activation;
- optional co-release integration with RFC-023's new-graph PK activation.

Out of scope:

- a `GraphState` singleton or warm publish input;
- a separate `__heads` Lance dataset;
- deleting immutable journal rows;
- commit-graph ancestry acceleration;
- checkpoint retention and arbitrary-version GC;
- using a physical index as a correctness precondition.

Checkpoint retention is excluded because a row in `__manifest` does not by
itself pin a version in another Lance dataset. Lance cleanup recognizes its own
versions, tags, and branch references, not foreign references. A later RFC must
define substrate-native per-table pins and their crash-safe lifecycle.

## 3. Table-head schema

Each native manifest branch contains exactly one current head row for each
stable table identity known to that branch.

### 3.1 Identity and object key

```
object_id   = "table_head:<stable_table_id>"
object_type = "table_head"
```

`stable_table_id` and `incarnation_id` are the graph-scoped nonzero `u64`
identities defined and minted by
[RFC-028](0028-stable-schema-identity.md). `stable_table_id` is the node or edge
type ID; it survives a supported rename and is never derived from the display
name, `kind:name`, path, or Lance field ID. This RFC consumes those values and
does not define a competing allocator or identity registry.

The head row is mutable under `WhenMatched::UpdateAll`, just like
`graph_head:<branch>`. There is one object ID per stable identity. Under
RFC-028's current drop/re-add rule, the old identity remains tombstoned and the
new declaration receives a distinct head object; there is still never more than
one candidate head for one stable identity.

### 3.2 Payload

The heads format reuses the manifest's typed columns where they already fit and stores the
remaining versioned payload in a typed JSON structure:

```text
TableHeadMetadata {
    state: "live" | "tombstoned",
    stable_table_id: u64,
    incarnation_id: u64,
    physical_ref_incarnation: String,
    schema_ir_hash: String,
    head_graph_commit_id: Option<ULID>,
}
```

`physical_ref_incarnation` is an opaque, backend-derived token that changes
when the Lance dataset or native ref at the same path/branch/version is deleted
and recreated. Use the manifest/ref e_tag when the backend provides one;
otherwise the implementation must prove an equivalent Lance-native token. A
backend with no proven token cannot activate the heads format. Logical
`incarnation_id` cannot substitute for this field because a physical owner or
ref may be replaced while logical table identity is preserved.

The row columns have these meanings:

| Column | `live` | `tombstoned` |
|---|---|---|
| `table_key` | current public table key/name | last public key/name |
| `location` | physical table location | last physical location, diagnostic only |
| `table_version` | current visible Lance version | last live Lance version |
| `table_branch` | physical owner branch, nullable for main | last owner branch |
| `row_count` | current row count | null |
| `metadata` | `TableHeadMetadata` | `TableHeadMetadata`; `head_graph_commit_id` names the tombstoning graph commit |

The `state` field is authoritative. A tombstoned row MUST NOT become live merely
because an older journal version has a greater data-table version than some
other row.

### 3.3 Incarnations

`incarnation_id` distinguishes table-lifetime ABA:

- rename preserves stable ID and incarnation;
- ordinary writes preserve both;
- physical owner handoff preserves both;
- dropping the type transitions its head to `tombstoned`;
- an ordinary later same-name add mints a new stable ID and incarnation, creates
  a new live head, and leaves the old head tombstoned;
- no name comparison revives an old head.

Any future explicit rematerialization that preserves stable type identity but
changes incarnation must first be specified by RFC-028 or a successor. The head
transition then follows that accepted schema outcome; RFC-024 never invents it.

### 3.4 Journal identity

The mutable head is not the only place that records identity. Every heads-format
table-version, registration, rename, and tombstone journal event carries
`stable_table_id`, `incarnation_id`, and the exact physical location/ref token
for its table version; otherwise drop/recreate followed by a new physical
dataset whose Lance versions restart cannot be replayed unambiguously or repair
the full head token.

A fresh graph's first valid manifest writes identity-bearing registration events
and matching heads for its initial tables. Later registrations write immutable
identity-bearing events in the same publish as their new heads. Offline head
repair replays only these identity-capable events from graph initialization; it
does not infer identity from mutable names, compare unrelated Lance version
numbers, or translate predecessor history.

## 4. State-transition rules

| Event | Required head transition |
|---|---|
| Register table | absent → live, new incarnation |
| Data write / optimize / index publish | live version N → live version M |
| Owner-branch handoff | live owner A → live owner B, even if version is equal |
| Rename | live key/name A → live key/name B; identity/incarnation unchanged |
| Drop table | live → tombstoned in the same graph publish as the tombstone journal row |
| Same-name add after drop | old identity stays tombstoned; new identity → live |
| Recovery roll-forward | apply the failed writer's intended live/tombstone transition |
| Recovery rollback | publish a head matching the restored physical version and logical pre-write state |

No path may append a table-version or tombstone journal row without including
its corresponding head mutation in the same publisher source batch.

## 5. Atomic publisher contract

The heads-format publisher constructs one merge-insert source containing:

- immutable, identity-bearing table-version rows;
- immutable, identity-bearing table-tombstone/transition rows;
- mutable table-head rows;
- when the RFC-022 plan carries `LineageIntent`, the immutable `graph_commit`
  and mutable `graph_head:<branch>` rows;
- for metadata-only plans, their specific CAS authority/operation rows without
  manufacturing graph lineage.

One Lance manifest commit makes the entire set visible. The publisher still
resolves the graph parent and re-reads commit authority inside every CAS retry.
Expected table versions are compared against table heads, not reconstructed by
folding the journal.

The comparison is not version-only. Every writer captures and revalidates the
complete expected token:

```text
(state, stable_table_id, incarnation_id, location, table_branch,
 physical_ref_incarnation, table_version, schema_ir_hash)
```

Any difference returns control to full RFC-022 revalidation before effects; a
publisher retry may not reparent a prepared table effect across the mismatch.
The desired head token is derived from the exact achieved dataset/ref after the
effect, never copied forward from a stale expected head.

Two graph-content writers touching disjoint data tables still contend on
`graph_head:<branch>`, form one linear graph history, and re-parent on retry.
Writers touching the same table also contend on its one `table_head` object.
Metadata-only CASes contend on the stable authority rows named by their complete
`ReadSet`; they do not update `graph_head` merely to create contention.

The immutable journal remains necessary for snapshots, diffs, audit, rebuild
verification, and head repair. Head rows do not replace or truncate it.

## 6. Read contract

### 6.1 Current state

A heads-format current-state read:

1. derives the expected live stable table IDs from the pinned catalog and fixed
   system-table registry;
2. issues a structured lookup for the exact head object IDs;
3. requires exactly one valid row per expected identity;
4. includes only rows whose authoritative state is `live`;
5. validates schema identity from the head payload;
6. opens the exact pinned physical table/ref and validates
   `physical_ref_incarnation` before exposing it;
7. returns one immutable `Snapshot` used for the operation's lifetime.

Missing, duplicate, unknown-state, or schema-mismatched **live** heads fail
loudly. The hot path does not enumerate every identity ever dropped merely to
prove all tombstone heads exist; explicit `heads verify`/repair owns tombstone
validation by replaying identity-bearing history offline. A missing or
duplicate tombstone is still corruption, but normal reads do not regain an
O(history) scan to discover it.

### 6.2 History

`snapshot_at_version`, commit resolution, change feeds, and audit continue to
read immutable journal/lineage state at the requested manifest version. A heads-format
manifest version contains the heads as they stood at that version, but the
journal remains the normative explanation of how the state arose.

### 6.3 Diagnostic repair

An explicit offline repair replays identity-bearing journal transitions from
the graph's first heads-format manifest version, compares the result to current
heads, and publishes corrected heads with an audited system actor. Repair is not
part of the read hot path and never silently runs from a query.

## 7. Bounded physical lookup is a ship gate

Logical O(tables) output does not prove physical O(tables) work. Without an
index, `object_id IN (...)` still scans the journal-bearing manifest fragments;
compaction reduces files but not semantic row count.

### 7.1 Required property

At fixed catalog width, a reconciled heads-format current-state lookup MUST have zero
positive slope in:

- manifest object-store reads;
- bytes read;
- fragments/pages scanned; and
- rows decoded

as commit history grows.

The bound must hold on a real object store as well as local FS and must be shown
for compacted and uncompacted histories. The test uses the shared IO-tracking
harness and installs the tracker before the manifest handle opens.

### 7.2 Candidate access shape

The primary candidate is a structured exact lookup on `object_id` backed by a
Lance scalar index. It is acceptable only if measurement proves:

- indexed head lookup avoids journal-fragment scans;
- newly committed head rows leave at most a bounded uncovered tail;
- reconciliation restores coverage without synchronous expensive work in the
  logical write path;
- index absence or partial coverage remains logically correct and is surfaced
  as an observable degraded-cost mode.

The index is derived state. Queries MUST remain correct if it is missing, and a
missing index cannot block a logical write. The performance promise applies to
the reconciled serving state and includes an explicit bound on uncovered work;
it is not inferred merely from the existence of an index declaration.

### 7.3 Rejected access shape

A separate heads dataset is rejected. Lance commits are per dataset, so it
would reintroduce a journal→heads crash gap and require another sidecar protocol
for the very pointer whose purpose is to remove drift.

If no in-manifest Lance-native access shape passes the gate, the heads format
does not ship. The fallback is to retain the then-current identity-capable
format plus the local session/view-passing improvements, not to waive the cost
claim.

## 8. Recovery protocol

Data-table writers still use the existing four phases:

1. write sidecar before a Lance HEAD advance;
2. commit staged/inline table work;
3. publish `__manifest`;
4. delete sidecar.

The sidecar's logical intent in the heads format includes the expected and desired table-head
payload. Recovery behavior is therefore complete:

- roll-forward publishes the data version, journal row, table head, lineage,
  and graph head together;
- rollback restores the physical version, then publishes journal/audit state
  and a table head matching the restored logical state;
- a stale sidecar whose goal is already represented by the complete exact head
  token converges idempotently;
- a partially matching head is not treated as success.

Recovery remains a synchronous barrier before any later writer advances a
touched table. Index reconciliation may be asynchronous; unresolved commit
protocol state may not.

## 9. Heads-format boundary and compatibility

The heads capability comprises:

1. table-head rows and their publish/read semantics;
2. identity-bearing table journal events from the graph's first valid state;
3. publisher and recovery rules that update those rows atomically; and
4. the graph-level internal-schema stamp that declares the capability.

It does **not** comprise identity minting (RFC-028), PK fencing (RFC-023), or
checkpoint/retention markers (RFC-025).

Serving remains strict-single-version:

- a heads-capable binary refuses an older graph with the documented
  export/import rebuild instructions;
- an older binary refuses the heads-format stamp;
- every open reads the graph-wide main stamp before selecting a named branch;
- missing or partial head state under a heads-format stamp is corruption;
- there is no mixed-format serving period or compatibility reader.

If RFC-023 or another capability is independently accepted for the same
release, one new format may initialize all of them after the combined
initialization/recovery matrix passes. If capabilities release separately,
each format bump requires its own rebuild under the strand policy. Co-release
is operator-cost coordination, not a dependency between heads and fencing.

## 10. Fresh graph initialization and rebuild cutover

### 10.1 First valid target state

Initialization constructs the identity-capable schema and complete heads before
the graph becomes writable:

1. RFC-028 mints the target graph identity, stable IDs, and incarnations;
2. every initial node/edge table is created with its accepted logical identity
   and exact physical schema;
3. the first valid manifest publish writes one identity-bearing registration
   event and matching live head per table, plus ordinary graph authority;
4. initialization verifies exact head cardinality, payload/schema identity, and
   any independently accepted co-released capability;
5. only a complete target carrying the new internal-schema stamp may open for
   normal writes.

There is no branch-local completion marker or predecessor-derived incarnation
baseline. A new branch forks a source manifest that already contains complete
heads and identity-bearing history; its native ref incarnation is validated
separately as physical authority.

### 10.2 Existing graph rebuild

An existing graph reaches the heads format only through the standard strand:

1. quiesce and export the selected source branch with a binary that reads the
   old format;
2. initialize a different graph root with the heads-capable binary;
3. import through ordinary RFC-022 `load` staging/recovery;
4. verify head/current-state equivalence to the imported rows and any
   co-released capability, then cut clients over.

The source graph is never annotated or stamped by the new binary. The target
starts a new graph identity and contains no predecessor journal to fold. It
preserves current rows, vectors, blobs, and schema shape, but not branches,
commit DAG, snapshots, tombstones, checkpoints, or time-travel history. A source
branch that matters is exported into a separate target graph today.

## 11. Rebuild failure and ordinary recovery

A failed target initialization or import does not mutate the source. Before the
target receives a complete format stamp it is not a serveable graph; discard or
repair it and retry. After initialization, import failures use the ordinary
RFC-022 sidecar protocol:

- unresolved table effects block later writes and recover all-or-nothing;
- head, journal, graph lineage, and graph head publish together;
- a stale sidecar converges only when the exact logical identity/incarnation
  and physical outcome match;
- co-released PK metadata or other immutable capability state is never cleared
  to simulate rollback to an older format.

No migration claimant, per-branch conversion ledger, old-format writer mode, or
`omnigraph:migration/*` actor is introduced.

## 12. Tests and acceptance gates

### 12.1 Head semantics

- current state from heads is byte-equivalent to the existing journal fold across realistic
  histories;
- live→tombstoned never resurrects an older live version;
- drop/recreate distinguishes incarnations;
- identity-bearing journal replay remains unambiguous when a recreated physical
  dataset restarts Lance version numbering;
- rename preserves identity/incarnation and changes the public key only;
- owner-branch handoff at an equal table version updates the head;
- delete/recreate of a dataset or native ref at the same path, branch, and
  numeric version changes `physical_ref_incarnation` and rejects a stale
  writer, on local FS and S3/RustFS;
- a current read refuses that same replacement until an authoritative publish
  selects its exact token; it never opens replacement data under the old head;
- missing, duplicate, malformed, and schema-mismatched heads fail loudly.
- `heads verify` detects a missing/duplicate tombstone by replaying the
  identity-bearing journal without adding that enumeration to every current
  read.

### 12.2 Publisher and recovery

- concurrent disjoint writers produce one linear graph chain and correct heads;
- same-table writers contend on one head row;
- failpoints after every table commit but before manifest publish recover to
  matching physical version, journal, table head, and graph head;
- rollback and roll-forward assertions include head payloads, not only table
  versions;
- publisher retry compares the complete expected token and never reparents a
  prepared effect across a physical-ref incarnation change;
- a stale sidecar converges exactly once with one audit record.

### 12.3 Format and rebuild

- a genuine old-format graph is refused before head/schema/recovery parsing by
  the new binary, and the old binary refuses the heads format;
- old-binary export followed by new-binary init/load produces current state
  byte-equivalent to the selected source branch;
- the target's first valid manifest contains exactly one live head and one
  identity-bearing registration event per initial table;
- crashes during target init never make a partial target serveable, and import
  crashes converge through ordinary RFC-022 recovery;
- a separately rebuilt branch has the documented independent graph identity
  and no inherited commit history;
- a post-activation branch inherits complete heads while native ref-incarnation
  validation remains separate;
- co-release tests prove every accepted capability exists in the first valid
  target state.

### 12.4 Cost gates

At fixed table count and increasing commit depth, assert flat curves for
manifest reads, bytes, fragments/pages, and decoded rows:

- local FS, compacted and uncompacted;
- S3/RustFS with real e_tags;
- warm repeated read;
- cold operation-local open with shared Session;
- one uncovered-head update before reconciliation;
- reconciled steady state.

The test must fail if the lookup silently falls back to scanning journal
history in the claimed steady state.

The decode term is part of the gate: parsing head rows — including the typed
JSON `TableHeadMetadata` payload — must be bounded by catalog width. A
per-read parse cost that grows with anything other than table count fails the
gate even when I/O is flat.

### 12.5 Format guards

- exact heads-format metadata schema and object IDs;
- one head row per stable identity;
- RFC-028 stable-ID/incarnation types plus `physical_ref_incarnation` in head
  and identity-bearing journal event schemas;
- RFC-023 PK metadata on node and edge tables when the release combines them;
- heads-format publisher source always pairs a journal/tombstone event with a head row.

## 13. Decisions and open gates

### Decided

- Heads and journal share one `__manifest` transaction.
- Current reads use heads; historical reads keep the journal.
- Heads represent `live | tombstoned` plus incarnation explicitly.
- A separate heads dataset and a mutable in-process tip authority are rejected.
- Existing graphs use strict-strand export/init/load; there is no in-place
  all-branch heads migration.
- RFC-023 PK activation is verified at target initialization only when
  deliberately co-released.
- Checkpoint retention is deferred.

### Open ship gates

1. Accepted and implemented
   [RFC-028](0028-stable-schema-identity.md) identity/incarnation contract.
2. The in-manifest indexed lookup implementation and bounded uncovered-tail
   proof.
3. Passing local and object-store depth-slope cost gates.
4. Final heads metadata JSON/typed-column compatibility review.
5. Genuine old/new binary refusal plus export/init/load rebuild evidence.
6. Combined initialization/recovery matrix for any co-released capability.
7. A proven `physical_ref_incarnation` source on every supported backend; no
   timestamp/version-only fallback is accepted without an ABA test.
