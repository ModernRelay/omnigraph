---
type: spec
title: "RFC-024 — Durable table heads and the v5 manifest"
description: Materialize one live-or-tombstoned current-state row per table inside each manifest branch, prove bounded physical lookup, and migrate every branch atomically before stamping v5.
status: draft
tags: [eng, rfc, manifest, write-path, versioning, migration, lance]
timestamp: 2026-07-10
owner:
---

# RFC-024: Durable table heads and the v5 manifest

**Status:** Draft / for team review
**Date:** 2026-07-10
**Surveyed:** omnigraph 0.8.1 (`main`); Lance 9.0.0-beta.15 at git rev
`f24e42c1`; full Lance table layout, transaction, branching, indexing,
compaction, cleanup, and object-store specifications
**Relationship to RFC-022:** this RFC is the durable-heads decision split from
the earlier monolithic RFC-022 draft. [RFC-022](rfc-022-unified-write-path.md)
defines the shared publisher/recovery protocol; this RFC owns the v5 format and
migration boundary. It deliberately excludes checkpoint retention, which
[RFC-025](rfc-025-checkpoint-retention.md) reviews separately. Key fencing in
[RFC-023](rfc-023-key-conflict-fencing.md) is also independently reviewable;
the two may share a release but do not block one another's evidence gates.
**Audience:** engine, manifest, migration, branch, and release maintainers
**Open architecture review:** [RFC-022–027 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

Throughout this draft, **v5** means the next internal schema after today's v4.
If another independently accepted format change lands first, the release number
is reassigned; none of the head-row semantics depend on the numeral.

## 0. Decision summary

The current manifest is both an immutable graph journal and the place writers
ask "what is current?" Current-state resolution folds history, so its physical
cost grows with commit count even though the answer contains only one value per
table.

v5 adds one mutable, durable `table_head` row per stable table identity inside
each native `__manifest` branch. The publisher updates the head in the same
Lance merge-insert transaction as immutable table-version rows, tombstone
events, graph lineage, and `graph_head`. The journal remains the history source;
heads become the current-state source.

The format does **not** ship merely because the logical result has O(tables)
rows. A filtered scan over a history-sized Lance table is still O(history)
physical work. v5 is gated on a Lance-native indexed lookup whose measured I/O
is flat at history depth and whose uncovered tail is bounded and observable.

Normative decisions:

1. Heads live in `__manifest`; there is no second heads dataset and no warm
   mutable-tip authority.
2. Head state is explicitly `live` or `tombstoned` and carries an incarnation.
3. Every publish and recovery outcome updates journal and head atomically.
4. Current-state reads use heads; history reads use the journal.
5. Missing or duplicate heads in v5 are corruption, not a reason to silently
   return to the history fold.
6. Migration covers every live manifest branch, then stamps main v5 last.
7. If RFC-023 is co-released, the combined migration also verifies PK fencing;
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

- v5 table-head schema and state transitions;
- atomic publisher, recovery, and current-read semantics;
- bounded physical access proof;
- all-branch predecessor→heads migration and compatibility refusal;
- optional co-release integration with RFC-023's all-branch PK activation.

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

## 3. v5 table-head schema

Each native manifest branch contains exactly one current head row for each
stable table identity known to that branch.

### 3.1 Identity and object key

```
object_id   = "table_head:<stable_table_id>"
object_type = "table_head"
```

`stable_table_id` survives a type rename. It is not the display name and is not
derived anew from `kind:name`. Closing the current rename-stable identity gap is
a v5 ship gate, and **this RFC owns it**: the stable-ID encoding, the compiler
contract that mints and preserves it, and the incarnation baseline are defined
here. RFC-023 §4.1 (rename-safe PK claims) and RFC-025 §2.1 (checkpoint table
rows) consume this identity and must not ship identity-dependent claims before
it lands. No other RFC in the family defines a competing identity scheme.

> 💬 **Superseded by review (BLOCKER-07 in the
> [review ledger](../dev/rfc-022-027-architecture-review.md)):** ownership
> *inside* this RFC contradicts siblings that consume the identity while
> calling this RFC optional (RFC-025) or without declaring the dependency
> (RFC-026). Disposition: extract identity/incarnation into its own
> prerequisite RFC, or accept this RFC in full before the identity-dependent
> siblings. Replace this ownership note accordingly.

The head row is mutable under `WhenMatched::UpdateAll`, just like
`graph_head:<branch>`. There is one object ID per stable identity; recreation
does not leave multiple candidate heads.

### 3.2 Payload

v5 reuses the manifest's typed columns where they already fit and stores the
remaining versioned payload in a typed JSON structure:

```text
TableHeadMetadata {
    state: "live" | "tombstoned",
    stable_table_id: String,
    incarnation_id: ULID,
    schema_hash: String,
    head_graph_commit_id: Option<ULID>,
}
```

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

`incarnation_id` distinguishes drop/recreate ABA:

- rename preserves stable ID and incarnation;
- ordinary writes preserve both;
- physical owner handoff preserves both;
- dropping the type transitions the one head to `tombstoned`;
- recreating a logically new table mints a new incarnation and updates the same
  stable-identity head to `live` only through an explicit schema operation.

If recreation is assigned a new stable table identity by schema semantics, it
gets a new head object and the old identity remains tombstoned. The schema
planner, not a name comparison, chooses this outcome.

### 3.4 Journal identity

The mutable head is not the only place that records identity. Every v5
table-version, registration, rename, and tombstone journal event carries
`stable_table_id` and `incarnation_id`; otherwise drop/recreate followed by a
new physical dataset whose Lance versions restart cannot be replayed
unambiguously.

Migration writes one immutable v5 incarnation-baseline row per known identity,
bound to the source-tip digest. New registrations/recreations write an immutable
incarnation transition. Head repair starts from that baseline and replays only
identity-bearing v5 events. It does not infer identity from mutable names or
compare unrelated Lance version numbers. If pre-v5 history cannot be mapped to
one baseline identity deterministically, migration refuses before the stamp.

## 4. State-transition rules

| Event | Required head transition |
|---|---|
| Register table | absent → live, new incarnation |
| Data write / optimize / index publish | live version N → live version M |
| Owner-branch handoff | live owner A → live owner B, even if version is equal |
| Rename | live key/name A → live key/name B; identity/incarnation unchanged |
| Drop table | live → tombstoned in the same graph publish as the tombstone journal row |
| Recreate | tombstoned → live with the schema-planned identity/incarnation outcome |
| Recovery roll-forward | apply the failed writer's intended live/tombstone transition |
| Recovery rollback | publish a head matching the restored physical version and logical pre-write state |

No path may append a table-version or tombstone journal row without including
its corresponding head mutation in the same publisher source batch.

## 5. Atomic publisher contract

The v5 publisher constructs one merge-insert source containing:

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

Two graph-content writers touching disjoint data tables still contend on
`graph_head:<branch>`, form one linear graph history, and re-parent on retry.
Writers touching the same table also contend on its one `table_head` object.
Metadata-only CASes contend on the stable authority rows named by their complete
`ReadSet`; they do not update `graph_head` merely to create contention.

The immutable journal remains necessary for snapshots, diffs, audit, migration
verification, and head repair. Head rows do not replace or truncate it.

## 6. Read contract

### 6.1 Current state

A v5 current-state read:

1. derives the expected live stable table IDs from the pinned catalog and fixed
   system-table registry;
2. issues a structured lookup for the exact head object IDs;
3. requires exactly one valid row per expected identity;
4. includes only rows whose authoritative state is `live`;
5. validates schema identity from the head payload;
6. returns one immutable `Snapshot` used for the operation's lifetime.

Missing, duplicate, unknown-state, or schema-mismatched **live** heads fail
loudly. The hot path does not enumerate every identity ever dropped merely to
prove all tombstone heads exist; the branch completion digest plus explicit
`heads verify`/repair owns bounded tombstone-set validation. A missing or
duplicate tombstone is still corruption, but normal reads do not regain an
O(history) scan to discover it.

### 6.2 History

`snapshot_at_version`, commit resolution, change feeds, and audit continue to
read immutable journal/lineage state at the requested manifest version. A v5
manifest version contains the heads as they stood at that version, but the
journal remains the normative explanation of how the state arose.

### 6.3 Diagnostic repair

An explicit offline repair loads the branch's v5 incarnation baseline, replays
identity-bearing journal transitions from that baseline, compares the result to
its heads/marker digest, and publishes corrected heads with an audited system
actor. Repair is not part of the read hot path and never silently runs from a
query.

## 7. Bounded physical lookup is a ship gate

Logical O(tables) output does not prove physical O(tables) work. Without an
index, `object_id IN (...)` still scans the journal-bearing manifest fragments;
compaction reduces files but not semantic row count.

### 7.1 Required property

At fixed catalog width, a reconciled v5 current-state lookup MUST have zero
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

If no in-manifest Lance-native access shape passes the gate, v5 does not ship
with head rows. The fallback is to retain v4 plus the local session/view-passing
improvements, not to waive the cost claim.

## 8. Recovery protocol

Data-table writers still use the existing four phases:

1. write sidecar before a Lance HEAD advance;
2. commit staged/inline table work;
3. publish `__manifest`;
4. delete sidecar.

The sidecar's logical intent in v5 includes the expected and desired table-head
payload. Recovery behavior is therefore complete:

- roll-forward publishes the data version, journal row, table head, lineage,
  and graph head together;
- rollback restores the physical version, then publishes journal/audit state
  and a table head matching the restored logical state;
- a stale sidecar whose goal is already represented by the exact head
  incarnation/version converges idempotently;
- a partially matching head is not treated as success.

Recovery remains a synchronous barrier before any later writer advances a
touched table. Index reconciliation may be asynchronous; unresolved commit
protocol state may not.

## 9. v5 boundary and compatibility

v5 comprises:

1. table-head rows and their publish/read semantics;
2. branch-local v5 completion markers; and
3. the graph-level internal-schema stamp written after verification.

It does **not** comprise checkpoint/retention markers.

If RFC-023 is independently accepted and ready for the same release, v5 may
also carry its PK annotation and fencing-compatible marker after a combined
migration review. That is release coordination, not a prerequisite of heads.

Format sequencing is explicit:

| Order | Result |
|---|---|
| Heads first | proposed v5 contains heads; later fencing migration preserves and atomically updates heads for every PK-version repoint |
| Fencing first | fencing takes the next format number; this heads migration takes the following number, accepts that exact predecessor, and preserves its PK/stamp invariant |
| Co-release | one format maps to both independently accepted capabilities and runs the combined failure matrix |

Migration code dispatches from an exact supported predecessor feature set; it
never assumes that “pre-heads” means pristine v4 or drops a capability it does
not own.

After upgrade, serving is strict-single-version:

- v5 binaries refuse unstamped or partially migrated graphs in normal mode;
- older binaries refuse v5 with the existing upgrade message;
- every open reads the graph-wide main stamp before selecting a named branch;
- only the dedicated offline migration command may open the exact supported
  predecessor for conversion;
- there is no mixed v4/v5 serving period.

## 10. All-branch predecessor→heads migration

### 10.1 Preconditions

The operator stops every graph writer and acquires an atomic
create-if-absent migration claim (with exact owner/fencing token; not a Lance
branch sentinel). The barrier covers server, CLI, embedded, maintenance,
branch, and schema writes. Because predecessor binaries do not understand the
new in-graph migration marker, the offline fleet barrier remains mandatory
until the final stamp.

The claim uses `PutMode::Create`, records the migration operation and owner
token, and permits no time-only takeover. Recovery classifies the durable
ledger/sidecars under the fleet outage before replacing a stale token.

After acquiring the claim, migration completes RFC-022's recovery barrier
before pinning any branch source tip.

If fencing is co-released, the migration first executes RFC-023's all-branch
table-PK plan. Otherwise head migration neither adds nor changes PK metadata.

### 10.2 Per-branch conversion

For each live native `__manifest` branch:

1. capture branch name, ref incarnation, manifest version, and e_tag/timestamp;
2. run the predecessor journal+tombstone fold once at that pinned tip;
3. construct exactly one live or tombstoned head plus one immutable incarnation
   baseline per stable table identity;
4. validate table schema hashes and, only in a combined release, RFC-023 PK state;
5. publish all heads/baselines, an audited migration record
   (`actor = omnigraph:migration/v5`), and a branch-local completion marker in
   one manifest CAS that revalidates the captured ref incarnation/source tip;
   this physical representation migration does not create a graph-content
   commit or move `graph_head`;
6. store in the marker the source tip, head count, identity-baseline digest,
   head-set digest, and heads-format version; the marker is content-scoped and
   deliberately does not embed the source branch incarnation; source tip is
   provenance, while digest/format determine inherited-marker validity;
7. verify by reopening the produced branch version through the v5 head reader.

The completion marker has a deterministic object ID within each manifest
branch. A retry that finds a matching source/digest is complete; a mismatching
marker is a loud migration conflict.

### 10.3 Finalization

After all branches report completion, migration re-enumerates native manifest
refs and verifies the same incarnation set. Branch create/delete is blocked, so
any difference indicates out-of-band modification and aborts finalization.

Only then does it stamp main as internal schema v5. The stamp is the fleet
commit point: before it, no serving process may start; after it, only v5 code may
open the graph.

New branches created after v5 fork a source manifest that already contains
complete heads and the content-scoped v5 marker. The inherited marker remains
valid for the identical snapshot; branch open separately binds that content to
the new native ref incarnation and validates the graph-wide stamp. No
post-create marker rewrite is required.

## 11. Migration recovery

The migration keeps a durable ledger outside the ordinary read path with one
record per branch and, in a combined release, per RFC-023 table conversion. It
records expected source incarnations, achieved physical versions, produced
manifest versions, and digests.

Recovery is idempotent and roll-forward-only:

- completed, digest-matching branch conversions are skipped;
- a table HEAD advance not yet represented in its graph branch is recovered by
  the normal sidecar and then branch conversion resumes;
- an uncommitted head-row source leaves no visible state and is rebuilt;
- a committed branch marker is authoritative for that branch conversion;
- the main v5 stamp is never written while any ledger unit is incomplete;
- co-released PK metadata is never cleared to simulate rollback.

If migration crashes, the graph remains offline. Restarting an old serving
binary is not a recovery procedure; the operator resumes the v5 migration.

## 12. Tests and acceptance gates

### 12.1 Head semantics

- current state from heads is byte-equivalent to the predecessor fold across realistic
  histories;
- live→tombstoned never resurrects an older live version;
- drop/recreate distinguishes incarnations;
- identity-bearing journal replay remains unambiguous when a recreated physical
  dataset restarts Lance version numbering;
- rename preserves identity/incarnation and changes the public key only;
- owner-branch handoff at an equal table version updates the head;
- missing, duplicate, malformed, and schema-mismatched heads fail loudly.
- `heads verify` detects a missing/duplicate tombstone against the baseline and
  completion digest without adding that enumeration to every current read.

### 12.2 Publisher and recovery

- concurrent disjoint writers produce one linear graph chain and correct heads;
- same-table writers contend on one head row;
- failpoints after every table commit but before manifest publish recover to
  matching physical version, journal, table head, and graph head;
- rollback and roll-forward assertions include head payloads, not only table
  versions;
- a stale sidecar converges exactly once with one audit record.

### 12.3 All-branch migration

- main, owned named branches, and lazy-inherited branches all convert;
- tombstoned types are represented on every relevant branch;
- crashes before/after every per-branch CAS resume from the ledger;
- branch ref deletion/recreation is caught by incarnation verification;
- final stamp is impossible while any table or branch marker is absent;
- each branch's graph head and content lineage are unchanged by head migration;
- old/new binary refusal matrix covers every supported predecessor capability
  set, partial heads migration, and complete heads format;
- a post-v5 branch inherits and validates complete heads.
- the inherited completion marker is content-scoped while ref-incarnation
  validation is performed separately.

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

- exact v5 head metadata schema and object IDs;
- one head row per stable identity;
- incarnation-baseline and identity-bearing journal event schemas;
- content-scoped branch completion marker schema/digest;
- RFC-023 PK metadata on node and edge tables when the release combines them;
- v5 publisher source always pairs a journal/tombstone event with a head row.

## 13. Decisions and open gates

### Decided

- Heads and journal share one `__manifest` transaction.
- Current reads use heads; historical reads keep the journal.
- Heads represent `live | tombstoned` plus incarnation explicitly.
- A separate heads dataset and a mutable in-process tip authority are rejected.
- Migration is offline, all-branch, resumable, and stamps v5 last.
- RFC-023 PK activation is verified by v5 only when deliberately co-released.
- Checkpoint retention is deferred.

### Open ship gates

1. Rename-stable table/type identity and final stable-ID encoding — owned by
   this RFC; consumed by RFC-023 and RFC-025 (§3.1).
2. The in-manifest indexed lookup implementation and bounded uncovered-tail
   proof.
3. Passing local and object-store depth-slope cost gates.
4. Atomic cross-process migration-claim implementation and operator runbook.
5. Final v5 metadata JSON/typed-column compatibility review.
6. Full all-branch migration/failpoint matrix.
