---
type: spec
title: "RFC-023 — Substrate-native key-conflict fencing"
description: Make concurrent keyed writes fail or retry through Lance's transaction conflict filters, forbid keyed Append, and activate the contract only in a fencing-compatible internal format reached through rebuild cutover.
status: draft
tags: [eng, rfc, write-path, concurrency, lance, primary-key]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-023: Substrate-native key-conflict fencing

**Status:** Draft / for team review
**Date:** 2026-07-10
**Author track:** Maintainer design series
**Surveyed:** omnigraph 0.8.1 (`main`); Lance 9.0.0-beta.21 at git rev
`1aec14652dcbace23ac277fa8ced35000bea0c40`; full Lance transaction,
table-schema, read/write, branching, and MemWAL specifications; pinned Rust
conflict-resolver and merge-insert sources plus runtime surface probes
**Evidence status:** the beta.21 filter shape and directional conflict matrix
are pinned substrate facts. OmniGraph has not activated keyed fencing, changed
production routing, or crossed a fencing-compatible format barrier.
**Relationship to RFC-022:** this RFC is the fencing decision split from the
earlier monolithic RFC-022 draft. [RFC-022](0022-unified-write-path.md)
defines the shared write/recovery protocol; this RFC owns the substrate,
compatibility stamp, and rollout requirements for key conflicts. Stable table
identity and incarnation come from
[RFC-028](0028-stable-schema-identity.md), which is a prerequisite. This RFC
may share a release with [RFC-024](0024-durable-table-heads.md), but neither
RFC depends on the other's storage decision.
**Audience:** engine, storage, migration, and release maintainers
**Open architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

## 0. Decision summary

OmniGraph will use Lance's unenforced-primary-key merge-insert filter as the
key-conflict primitive. It will not emulate the filter in the engine and will
not add a lock table or custom transaction manager.

The guarantee is deliberately narrow and strong:

> For a keyed table, two concurrent operations that may insert the same `id`
> MUST NOT both commit silently. They either serialize, retry from a new graph
> base, or fail loudly.

That guarantee cannot be rolled out by annotating a few new tables in an
otherwise v4 graph. Lance's current mixed filtered/unfiltered conflict handling
is directional, and a bare `Append` can commit after a filtered update. The
contract therefore exists only in a fencing-compatible internal format whose
graphs are created with the PK metadata and fenced routing before the first row
is accepted. Existing graphs reach it through the project's strict
export/init/load rebuild cutover, not an in-place all-branch conversion.

Normative decisions:

1. Node and edge table PK = `id`.
2. Bare Lance `Append` is forbidden for keyed tables.
3. Every keyed insert/upsert path produces the same Lance key filter.
4. Mixed-version serving is forbidden; old and new binaries mutually refuse
   incompatible graph formats.
5. PK metadata is permanent and preserved by every later schema/data rewrite.
6. Existing graphs are exported with the old binary and rebuilt at a new graph
   root whose initial schema is already fencing-compatible.
7. Fencing does not replace read-set OCC for `@unique`, RI, or cardinality.

## 1. Problem

OmniGraph validates duplicate `@key` values inside one delta, but today two
processes can both read a base where `id = K` is absent, stage disjoint Lance
fragments containing `K`, and let Lance rebase both commits. The graph manifest
CAS orders graph publication; it does not tell Lance that the two data-table
transactions inserted the same logical key.

The result is worse than an ordinary write conflict: both callers can receive
success while a keyed table contains duplicate IDs. Every subsequent upsert,
edge lookup, uniqueness check, and merge then operates on a broken identity
relation.

The desired primitive already exists in Lance. The missing work is to use it on
every keyed path and to define a rollout that never admits an unfenced writer.

## 2. Substrate facts and the directional asymmetry

This section is load-bearing. Tests pin every statement before implementation.

### 2.1 Filter attachment

On the pinned Lance revision, the explicitly selected v2 merge plan
(`use_index(false)`) compares the ordered ON field IDs with the schema's
non-empty unenforced-PK field IDs. For the insert-capable and matched-only
update shapes this RFC consumes, an exact match attaches
`Some(KeyExistenceFilter)` to the resulting `Operation::Update`:

- a fresh insert produces a populated Bloom filter;
- `WhenMatched::Fail` on a fresh key preserves that populated filter;
- a matched-only partial-schema `UpdateAll` with
  `WhenNotMatched::DoNothing` produces `Some` with a semantically empty Bloom
  filter, not `None`, because no row took the Insert action; and
- a mismatched ON field set produces `None`.

The filter contains keys only for rows classified as inserts. Updates of
existing rows continue to use Lance's affected-row / fragment conflict
machinery, and delete-only operations do not need an insertion filter.

When every ON column has a scalar index and `use_index` remains enabled, Lance
selects the legacy v1/indexed merge path, which hardcodes the filter to `None`.
Therefore a BTREE on `id` can route an otherwise correct merge onto an unfenced
path unless the caller disables that path or Lance wires the filter into it.

### 2.2 Conflict compatibility is directional

Lance transaction compatibility is evaluated from the transaction currently
attempting to commit against transactions that committed after its read
version. It is not implicitly symmetric.

At `1aec1465`, with each probe arranged so no independent fragment conflict
decides the result, `check_update_txn` has this matrix. “Current” means the
second transaction attempting to commit; “committed” means the first
transaction already visible after the current transaction's read version.

| Current / second transaction | Committed / first transaction | beta.21 result |
|---|---|---|
| filtered Update | filtered Update, overlapping fresh key | retryable conflict |
| filtered Update | filtered Update, disjoint fresh key | succeeds |
| filtered Update | unfiltered Update | retryable conflict |
| unfiltered Update | filtered Update | succeeds |
| filtered Update | bare Append | retryable conflict |
| bare Append | filtered Update | succeeds |

The matched-only v2 result above resolves the narrow empty-filter question: it
stays in the filtered class even though it inserted no row. It does not make
beta.21 symmetric. The indexed v1 route remains unfiltered, and a bare keyed
Append can still land second.

Consequently, "filtered vs unfiltered conflicts" is not a sufficient rollout
argument. Commit order matters. A filtered writer can win first and a stale
unfiltered writer or bare keyed append can still land second.

### 2.3 What a PK annotation does not do

The key is explicitly *unenforced*. Merely setting the metadata:

- does not validate historical uniqueness;
- does not make bare appends unique;
- does not protect a merge whose ON set differs from the PK;
- does not repair an existing duplicate;
- does not replace OmniGraph's semantic validators.

## 3. Scope and non-goals

In scope:

- node and edge data tables;
- mutation, load, branch merge, recovery replay, and WAL fold paths;
- new-format table creation and export/import cutover activation;
- schema/overwrite preservation of PK metadata;
- typed retry behavior and coverage gates.

Out of scope:

- using a composite edge PK (`src`, `dst`);
- enforcing arbitrary `@unique` groups through the Lance PK;
- keyless streaming-table deduplication;
- a custom OmniGraph WAL, lock table, or transaction manager;
- declaring general multi-process writes supported before foreign-process
  recovery-sidecar ownership is solved.

## 4. Table classes and PK contract

### 4.1 Keyed graph tables

Every normal node and edge table has a non-null `id` field. Its Lance schema
MUST mark exactly that field as the unenforced PK. For edges, `src` and `dst`
remain ordinary fields governed by referential-integrity and cardinality
validation. An edge endpoint move is an update of the row identified by `id`.

The PK field is addressed by the pinned Lance schema's stable field ID, not
column position or mutable name. The surrounding logical table contract is
bound to [RFC-028](0028-stable-schema-identity.md)'s stable table ID and
incarnation. Fencing cannot activate before that capability lands.

### 4.2 Keyless append-only tables

A table explicitly declared append-only may omit a PK. Such a table may use
Lance `Append`, including MemWAL append-only operation. It receives no
same-logical-key guarantee because it has no logical key.

Current node and edge types are not in this class: both have graph identity on
`id`. The class is reserved for an explicitly designed internal or future
non-graph table surface.

The distinction is catalog-derived and first-class. Callers do not choose it
with an ad-hoc flag.

## 5. Normative write routing

| Logical operation | Keyed table | Keyless append-only table |
|---|---|---|
| Strict insert / load append | merge-insert ON exactly `id`, `WhenMatched::Fail`, filtered path | `Append` allowed |
| Upsert / load merge | merge-insert ON exactly `id`, `WhenMatched::UpdateAll`, filtered path | workload-specific |
| Fast-forward branch merge of new rows | filtered merge-insert with `WhenMatched::Fail`, even when every row was classified new | `Append` allowed |
| WAL upsert fold | filtered merge-insert with `merged_generations` | append transaction allowed |
| Update existing row | merge-insert/update with affected-row conflict metadata | workload-specific |
| Delete | staged Lance delete; PK filter is not the delete-conflict primitive | staged Lance delete |
| Overwrite | staged overwrite whose output schema preserves the exact PK | staged overwrite |

### 5.1 No keyed `Append`

The prohibition includes internal optimization paths. A caller may not infer
"all rows are new" and switch a keyed table to `stage_append`: that inference
was made against a snapshot and is exactly what a concurrent same-key writer can
invalidate.

Routing through merge-insert does not collapse strict and upsert semantics.
Strict `load --mode append` and other insert-only surfaces use
`WhenMatched::Fail`; a row already present at the pinned base or discovered at
execution remains an error. Only declared upsert surfaces use `UpdateAll`.

The storage trait and `forbidden_apis` guard MUST make a keyed append difficult
to express accidentally. The fast-forward merge optimization is retained only
for keyless append-only tables unless Lance ships a key-filtered append
transaction.

This prohibition knowingly retires a measured fix. The fast-forward append
path exists because a whole-delta merge-insert join exhausted the query memory
pool on embedding-bearing tables (#277); routing adopted rows back through a
filtered merge-insert re-exposes that workload to join memory behavior. The
regression class is therefore a named ship gate: the fenced bulk adopt-merge
must pass the §11.4 memory/cost gate on embedding-bearing tables — via bounded
batched fenced merges inside one staged transaction, a pool-bounded execution
mode, or an upstream key-filtered append — before the keyed fast-forward path
is removed. Correctness wins the ordering, but the memory bound is not
optional.

### 5.2 Routing choice

There are two acceptable implementations:

1. use the v2 merge path (`use_index(false)`) and pass its scale gate; or
2. consume a pinned Lance revision whose indexed path emits the identical
   filter and passes the same surface guards.

If the v2 hash-join cost scales unacceptably at the Phase-B workload, fencing
waits for option 2. Correctness is not traded for the old indexed-path speed.

### 5.3 Symmetric mixed-transaction behavior

Beta.21 does not conservatively reject both orders of filtered Update vs
unfiltered Update or filtered Update vs bare Append. Under today's production
routing that directional behavior remains an activation blocker.

Activation first requires an engine-boundary proof that every
insertion-bearing keyed path uses the exact-PK filtered primitive and keyed
`Append` is structurally unreachable. For any unfiltered Update that remains,
the RFC must then either consume an upstream Lance revision with symmetric
conflict-resolver behavior, or prove that the operation can neither insert a
row nor alter `id`. Affected-row conflict metadata must continue to cover two
updates to the same existing row.

The beta.21 matched-only `Some(empty)` result makes the latter proof possible
for that one route; it does not complete the engine-wide proof. A
workspace-only Lance fork is not an accepted permanent design. The fleet
barrier remains necessary under either disposition because two old, unfiltered
writers still have no filters to compare.

## 6. Retry and validation semantics

A Lance retryable key conflict does **not** by itself authorize a restart. The
writer first classifies the current RFC-022 attempt:

1. A conflict detected before the recovery sidecar is armed has no physical
   effect. The writer discards the staged attempt and may perform a bounded
   semantic retry from fresh authority.
2. If the sidecar is armed but exact classification proves that no participant
   advanced, the writer finalizes that empty intent before retrying.
3. If any participant advanced, or emptiness cannot be proved, the writer keeps
   the sidecar and returns `RecoveryRequired`. The synchronous recovery barrier
   must resolve that exact attempt before any later semantic attempt is
   prepared; the writer never replans around partial physical state.

An eligible semantic retry always restarts the entire logical attempt:

1. gather a new graph snapshot and schema identity;
2. rerun all delta and committed-state validation;
3. restage from that base;
4. commit and publish through the normal recovery-covered pipeline.

It is incorrect to retry only `commit_staged`: an insert may have become an
update, defaults or checks may now differ, and cross-table validation may have
changed.

Upsert surfaces may perform that bounded whole-operation retry only after the
prior attempt is proven effect-free and finalized. Strict insert surfaces,
including `load --mode append`, never retry and never change meaning under
contention. An already-present match from `WhenMatched::Fail`, or a concurrent
same-key conflict from an effect-free finalized attempt, normalizes to typed
`KeyConflict` / HTTP 409 for the whole strict operation. A concurrent conflict
after another participant advanced instead returns `RecoveryRequired` and
retains the sidecar; recovery takes precedence over reporting a terminal key
result. Strict callers decide whether to resubmit after that barrier. Strict
inserts never switch to `UpdateAll`; other strict read-modify-write surfaces
retain their typed write conflict. Retry exhaustion on a non-strict upsert
remains a retryable 409.

Fencing covers the PK insertion race only. `@unique` values on different IDs,
edge RI, and cardinality depend on a read set. Their correctness requires the
read-set-in-CAS design or equivalent revalidation before HEAD movement; this RFC
does not claim that fences close those races.

## 7. Format boundary and rebuild cutover

### 7.1 No partial activation in the old format

OmniGraph MUST NOT annotate a new data table and advertise fencing while the
graph remains generally writable by older binaries. An older process can select
the legacy merge path or keyed append and bypass the guarantee.

The graph-wide internal-schema stamp is read from the reserved main manifest
before any named-branch open; selecting a named branch cannot bypass the
compatibility check. A fencing-capable binary keeps
`MIN_SUPPORTED == CURRENT`, refuses an older graph with the documented rebuild
instructions, and refuses a newer graph with “upgrade omnigraph.” An older
binary refuses the fencing-capable stamp.

The exact format number is assigned when this RFC is accepted. If RFC-028,
RFC-024, or another independently accepted capability is ready for the same
release, they may share one new-graph format after a combined initialization and
recovery review. If they release separately, each internal-format change
requires its own rebuild under the strand policy; no draft capability is
silently smuggled into another RFC's stamp.

Quiescence is required only to obtain a consistent export and perform operator
cutover. The new binary never opens the old graph in a special write mode,
claims an old-format migration, annotates a branch in place, or writes a
completion stamp back to the source.

### 7.2 New graphs

A graph created directly at the fencing-compatible format creates every keyed
table with the PK metadata already present and enables only the write routing
in §5. There is no post-create annotation window.

## 8. Strict-strand activation

Existing graphs activate fencing only through the documented rebuild path:

1. quiesce the selected old graph branch and export it with a binary that reads
   the old format;
2. show the accepted schema with that old binary;
3. initialize a different graph root with the fencing-capable binary; every
   keyed table is created with the exact PK metadata before any data load;
4. import through ordinary `load`, using RFC-022 staging/recovery and the
   production fenced route;
5. fail before serving if the export contains duplicate `id` values or the
   physical PK contract differs from the accepted RFC-028 identity/catalog;
6. verify the target, then cut clients over.

The source is never mutated, so a failed target init or import is discarded or
repaired and retried. There is no lazy-branch annotation problem because no old
physical version is adopted into the new root.

Rebuild preserves the selected branch's current rows, vectors, blobs, and
schema shape. It does not preserve the old graph's identities, branches, commit
DAG, snapshots, tombstones, or time-travel history. A branch whose state must
survive is exported and rebuilt separately today, producing a separate graph
root. This loss is shown in plan/runbook output rather than hidden behind the
word “migration.”

## 9. Preservation after activation

Once set, the following are storage invariants:

- a table overwrite within one physical dataset carries the same PK field ID
  and position;
- schema apply cannot remove, replace, reorder semantically, or make nullable a
  PK field;
- a property rename within one dataset preserves the PK field ID and metadata;
- a supported type rename may rematerialize a new dataset with different Lance
  field IDs, but the target is created with `id` as its exact PK before data is
  accepted and retains RFC-028's logical table ID/incarnation;
- branch fork/clone preserves it;
- import/rebuild creates it before accepting data;
- recovery restore may select an older data image only if that image is already
  fencing/PK-compatible;
- a table recreation uses RFC-028's new stable table ID and incarnation and
  installs the catalog-derived PK contract at creation;
- `__manifest`'s existing legacy PK key form is preserved exactly as stored;
  fresh initialization writes that selected form directly rather than
  “normalizing” an existing table. Lance forbids changing a set PK, and the
  native-namespace decoupling documented in the Lance alignment audit depends
  on that legacy form remaining in place.

Every open-for-write path verifies the physical schema matches the catalog PK
contract. The check is against the pinned physical schema and is not a
maintained parallel registry.

## 10. Recovery and multi-process scope

All data writes retain the existing Phase A-D sidecar protocol. The key filter
does not close the table-HEAD-before-graph-manifest window.

The fenced data-table transaction is cross-process safe in its failure-free
commit path. OmniGraph's current recovery sweep, however, serializes with live
writers only in-process; a foreign recovery process can still inspect a live
sidecar, and destructive `Restore` cannot be made convergence-idempotent.

Therefore this RFC MUST use one of two honest dispositions:

1. retain the documented single-writer-process support boundary and describe
   fences as closing the silent key-race primitive only; or
2. land a cross-process sidecar claim/lease before advertising general
   multi-process writes.

Fences alone are not evidence for disposition 2.

## 11. Tests and acceptance gates

### 11.1 Lance surface guards

The beta.21 guards pin current substrate truth, not activation readiness:

- exact PK ON + v2 produces a populated filter for a fresh insert and a
  fresh-key `WhenMatched::Fail`;
- matched-only partial-schema UpdateAll + DoNothing produces
  `Some(empty)`, while a mismatched ON set produces `None`;
- a scalar-indexed/default-v1 route produces `None` until Lance changes that
  path;
- filtered/filtered overlapping keys retry and disjoint keys may rebase;
- filtered current vs unfiltered committed retries, while the reverse order
  succeeds;
- filtered current vs committed Append retries, while current Append vs a
  committed filtered Update succeeds; and
- PK metadata cannot be changed or removed once installed.

The directional guards deliberately turn red if a future Lance revision gains
symmetry, forcing a new alignment audit and RFC disposition. They do not prove
that OmniGraph production routing has eliminated unfiltered insertion-bearing
operations, installed the PK on every reachable table image, or crossed the
fleet/format barrier.

### 11.2 Engine concurrency tests

- the same-key DST cell becomes a hard assertion with N concurrent writers;
- different keys remain concurrently writable;
- every keyed load/mutation/merge/fold path is observed to use the filtered
  primitive;
- strict append of an existing `id` still fails and never updates the row;
- strict pre-existing-match and concurrent-insert cases normalize to the same
  external `KeyConflict` when the attempt is effect-free, while preserving
  `WhenMatched::Fail`;
- a conflict on table N after table 1 committed retains the exact sidecar,
  returns `RecoveryRequired`, and starts no semantic retry until the recovery
  barrier resolves that attempt;
- a source-walk guard rejects keyed `stage_append`, including the former
  fast-forward path;
- an effect-free retry finalizes its prior intent and reruns validation rather
  than committing the stale staged batch.

### 11.3 Format and rebuild tests

- a genuine old-format graph is refused before schema/recovery work by the new
  binary, and the old binary refuses a fencing-format graph;
- old-binary export followed by new-binary init/load round-trips rows, vectors,
  and blobs into a graph whose keyed tables were PK-annotated from creation;
- duplicate IDs abort target import before serving and leave the source intact;
- a separately rebuilt branch has the documented independent-root identity and
  no inherited commit history;
- a co-release, when selected, creates every accepted capability in the first
  valid target state and never exposes a partial-format serving window;
- overwrite, schema apply, branch fork, restore, and later imports preserve PK
  metadata inside an already compatible graph.

### 11.4 Cost gate

Measure a small upsert into 10K, 100K, and 1M-row indexed tables using the
shared cost harness. If `use_index(false)` makes work scale with table size
beyond the accepted budget, the indexed-path upstream work is a ship blocker.

Additionally measure the bulk adopt-merge shape that motivated the keyed
fast-forward path (#277): a many-row, all-new-rows fenced merge into an
embedding-bearing table, asserting peak memory bounded by batch size rather
than table or delta width. If the fenced path cannot meet that bound, the
keyed fast-forward removal waits for the mitigation named in §5.1.

> 💬 **Instrument required (tightening 5 in the
> [review ledger](../dev/rfc-022-027-architecture-review.md)):**
> `helpers::cost` measures I/O, not peak RSS, so this memory bound is
> unenforceable as written. Use the subprocess `scenarios.rs` harness or an
> equivalent `wait4`/`ru_maxrss` instrument, and name dataset sizes, baseline,
> cap, and pass threshold.

## 12. Decisions and open gates

### Decided

- `id`, not `src`+`dst`, is the edge PK.
- No keyed append, including optimization-only append.
- No mixed-fleet or new-table-only v4 rollout.
- Existing graphs activate fencing only through strict-strand export/init/load
  into a new graph root; there is no in-place PK migration.
- A retryable upsert conflict retries the whole logical operation only after
  the prior attempt is proven effect-free and finalized. Strict insert maps
  effect-free existing and concurrent matches to `KeyConflict` without
  changing mode; a partial or unclassifiable attempt returns
  `RecoveryRequired` first.
- Read-set validation remains a separate required concurrency design.

### Open ship gates

1. Close post-activation routing under beta.21's directional resolver: every
   insertion-bearing keyed path carries the exact-PK filter, keyed `Append` is
   structurally unreachable, and each remaining unfiltered Update either is
   proven unable to insert or alter `id` with adequate affected-row conflict
   coverage, or is protected by a symmetric resolver on the pinned Lance
   revision.
2. v2-path scale result versus indexed-path filter availability.
3. Operator repair procedure for pre-existing duplicate IDs.
4. Accepted and implemented [RFC-028](0028-stable-schema-identity.md) identity.
5. Cross-process recovery ownership before any broadened topology claim.
6. Final format-number/co-release sequencing and genuine cross-version rebuild
   evidence.
7. The fenced bulk adopt-merge memory/cost gate on embedding-bearing tables
   (the #277 regression class) — see §5.1 and §11.4.
