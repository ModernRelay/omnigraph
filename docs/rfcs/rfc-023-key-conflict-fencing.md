---
type: spec
title: "RFC-023 — Substrate-native key-conflict fencing"
description: Make concurrent keyed writes fail or retry through Lance's transaction conflict filters, forbid keyed Append, and activate the contract only behind an all-branch fleet/format barrier.
status: draft
tags: [eng, rfc, write-path, concurrency, lance, primary-key]
timestamp: 2026-07-10
owner:
---

# RFC-023: Substrate-native key-conflict fencing

**Status:** Draft / for team review
**Date:** 2026-07-10
**Surveyed:** omnigraph 0.8.1 (`main`); Lance 9.0.0-beta.15 at git rev
`f24e42c1`; full Lance transaction, table-schema, read/write, branching, and
MemWAL specifications; pinned Rust conflict-resolver and merge-insert sources
**Relationship to RFC-022:** this RFC is the fencing decision split from the
earlier monolithic RFC-022 draft. [RFC-022](rfc-022-unified-write-path.md)
defines the shared write/recovery protocol; this RFC owns the substrate,
compatibility stamp, and rollout requirements for key conflicts. It may share a
release with [RFC-024](rfc-024-durable-table-heads.md), but neither RFC depends
on the other's storage decision.
**Audience:** engine, storage, migration, and release maintainers
**Open architecture review:** [RFC-022–027 review ledger](../dev/rfc-022-027-architecture-review.md).
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
contract therefore activates only after all supported writers and every table
state reachable from every graph branch have crossed a fencing-compatible
format barrier.

Normative decisions:

1. Node and edge table PK = `id`.
2. Bare Lance `Append` is forbidden for keyed tables.
3. Every keyed insert/upsert path produces the same Lance key filter.
4. Mixed-version serving is forbidden during activation; the fencing-compatible
   format stamp is written last and older binaries refuse it.
5. PK metadata is permanent and preserved by every later schema/data rewrite.
6. Existing-table migration covers every graph branch, including lazy-inherited
   table states, and is recoverable by rolling forward.
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

On the pinned Lance revision, a v2 merge-insert whose ON field IDs exactly equal
the schema's unenforced PK field IDs attaches an `inserted_rows_filter` to its
`Operation::Update`. The filter contains keys for rows classified as inserts;
updates of existing rows continue to use Lance's affected-row / fragment
conflict machinery.

The legacy indexed merge path does not attach this filter. Therefore a BTREE on
`id` can route an otherwise correct merge onto an unfenced path unless the
caller disables that path or Lance wires the filter into it.

### 2.2 Conflict compatibility is directional

Lance transaction compatibility is evaluated from the transaction currently
attempting to commit against transactions that committed after its read
version. It is not implicitly symmetric.

At `f24e42c1`, `check_update_txn` behaves as follows:

- current `Some(filter)` vs committed `Some(filter)` — compare field IDs and
  filter intersection; overlap or incompatible filter configuration retries;
- current `Some(filter)` vs committed `None` — retry conservatively;
- current `None` vs committed `Some(filter)` — no corresponding conservative
  arm; it may rebase;
- current bare `Append` vs committed filtered `Update` — `Append` treats the
  `Update` as compatible.

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
- new-table creation and all-branch existing-table activation;
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

The PK field is addressed by stable field ID, not column position or mutable
name. Until rename-stable OmniGraph type/property identity is closed, the
fencing migration cannot claim rename safety.

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

Before activation, the pinned Lance revision MUST conservatively reject both
orders of:

- filtered Update vs unfiltered Update;
- filtered Update vs bare Append.

The preferred fix is upstream conflict-resolver symmetry. A workspace-only
fork is not an accepted permanent design. The fleet barrier remains necessary
even after that fix because two old, unfiltered writers still have no filters
to compare.

## 6. Retry and validation semantics

A Lance retryable key conflict restarts the entire logical attempt:

1. gather a new graph snapshot and schema identity;
2. rerun all delta and committed-state validation;
3. restage from that base;
4. commit and publish through the normal recovery-covered pipeline.

It is incorrect to retry only `commit_staged`: an insert may have become an
update, defaults or checks may now differ, and cross-table validation may have
changed.

Upsert surfaces may perform the bounded semantic retry. Strict insert surfaces,
including `load --mode append`, do not change meaning under contention: both an
already-present match from `WhenMatched::Fail` and a concurrent same-key commit
normalize to typed `KeyConflict` / HTTP 409 for the whole strict operation. They
do not switch to `UpdateAll`; callers decide whether to resubmit. Other strict
read-modify-write surfaces retain their typed write conflict. Retry exhaustion
on a non-strict upsert remains a retryable 409.

Fencing covers the PK insertion race only. `@unique` values on different IDs,
edge RI, and cardinality depend on a read set. Their correctness requires the
read-set-in-CAS design or equivalent revalidation before HEAD movement; this RFC
does not claim that fences close those races.

## 7. Version and fleet barrier

### 7.1 No partial activation in the old format

OmniGraph MUST NOT annotate a new data table and advertise fencing while the
graph remains generally writable by older binaries. An older process can select
the legacy merge path or keyed append and bypass the guarantee.

This RFC owns its activation boundary:

- operators quiesce every server, CLI writer, and embedded writer for the
  graph;
- one migration claimant holds an atomic create-if-absent claim with a random
  owner/fencing token; a native Lance branch sentinel is not accepted as CAS;
- only the dedicated migration binary may open the old graph for writes;
- the fencing-compatible stamp is written after every branch/table verification;
- normal serving begins only after the stamp; older binaries then refuse.

The migration claim uses the storage adapter's `PutMode::Create` contract,
records operation/owner token, and has no time-only takeover. Recovery under the
fleet outage must classify the migration ledger/sidecars before replacing a
stale token.

The stamp is graph-wide and read from the reserved main manifest before any
named-branch open; selecting a named branch cannot bypass the compatibility
check.

An in-process mutex is not a fleet barrier. A marker unknown to v4 binaries is
also not a fleet barrier. The operator procedure and cluster control plane must
keep old writers stopped until finalization.

The exact format number is assigned when this RFC is accepted. If RFC-024 is
also accepted and ready, the two migrations may deliberately share its v5
release after a combined failure-matrix review. If durable heads fail their
cost gate, fencing still proceeds with its own next compatible stamp; if
fencing is blocked upstream, durable heads need not wait.

If durable heads are already active when fencing migrates, every PK metadata
version repoint also emits the identity-bearing journal event and matching
`table_head` transition in the same manifest CAS. If fencing lands first, its
format/stamp becomes an explicit predecessor that a later heads migration must
preserve. Acceptance covers heads-first, fencing-first, and co-release orders.

### 7.2 New graphs

A graph created directly at the fencing-compatible format creates every keyed
table with the PK metadata already present and enables only the write routing
in §5. There is no post-create annotation window.

## 8. All-branch PK migration

Migration operates on graph states, not merely table roots. The unit is every
reachable tuple:

```
(graph_branch, table_key, table_path, pinned_table_branch, pinned_table_version)
```

This matters for lazy branches: a graph branch may still point at an old main
table version whose schema predates the PK, even after main HEAD is annotated.

Under the fleet barrier, the migration:

1. enumerates and incarnation-pins every live graph branch;
2. folds each branch manifest and enumerates its live keyed tables;
3. validates that every pinned table image has non-null, unique `id` values;
4. acquires branch/table gates in RFC-022 order and freshly revalidates the
   pinned tuple, schema identity, and migration claim;
5. writes a per-unit RFC-022 sidecar declaring expected branch/table state, the
   optional native fork effect, PK metadata effect, and intended manifest delta
   before either effect can persist;
6. for an owned table branch, commits a set-if-absent PK metadata update;
7. for a lazy-inherited state, forks an owned table branch from the *exact
   pinned version*, applies the PK metadata there, and leaves row contents
   unchanged;
8. records exact achieved fork identity and table version in the sidecar;
9. publishes that graph branch's manifest to the annotated physical version
   with an audited migration marker but no graph-content commit or graph-head
   movement, including a table-head transition when heads are active;
10. records a branch/table completion digest;
11. re-enumerates branches and verifies every live branch before writing the
   fencing-compatible stamp.

PK installation advances a Lance table version before the graph manifest can
publish it, and a lazy fork creates native ref state first. The sidecar covers
both gaps and lets recovery reclaim or adopt the exact fork rather than infer
from a branch name. Because Lance forbids clearing/changing a set PK,
migration is roll-forward-only:

- an already-correct PK is an idempotent success and is not rewritten;
- an absent PK resumes installation;
- a different PK is a loud, operator-visible refusal;
- recovery never attempts to "undo" PK metadata.

Branch create/delete, schema apply, and normal data writes remain blocked for
the whole enumeration/install/verify interval. The migration ledger makes a
crash resumable without treating partial annotation as a served graph.

## 9. Preservation after activation

Once set, the following are storage invariants:

- a table overwrite carries the same PK field IDs and positions;
- schema apply cannot remove, replace, reorder semantically, or make nullable a
  PK field;
- rename preserves the PK field ID and metadata;
- branch fork/clone preserves it;
- import/rebuild creates it before accepting data;
- recovery restore may select an older data image only if that image is already
  fencing/PK-compatible;
- a table recreation uses a new table incarnation but installs the same
  catalog-derived PK contract at creation;
- `__manifest`'s existing legacy PK key form is preserved exactly as stored;
  the migration never rewrites or "normalizes" it. Lance forbids changing a
  set PK, and the native-namespace decoupling documented in the Lance
  alignment audit depends on that legacy form remaining in place.

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

- exact PK ON set + v2 path produces a non-empty filter for inserts;
- `WhenMatched::Fail` preserves that filter and reports an existing match
  without writing;
- mismatched ON set produces no filter;
- legacy/indexed path behavior is pinned until replaced;
- filtered/filtered overlapping keys retry;
- filtered/filtered disjoint keys may rebase;
- filtered/unfiltered retries in **both** commit orders;
- filtered Update/bare Append retries in **both** commit orders;
- PK metadata cannot be changed or removed once installed.

### 11.2 Engine concurrency tests

- the same-key DST cell becomes a hard assertion with N concurrent writers;
- different keys remain concurrently writable;
- every keyed load/mutation/merge/fold path is observed to use the filtered
  primitive;
- strict append of an existing `id` still fails and never updates the row;
- strict pre-existing-match and concurrent-insert cases normalize to the same
  external `KeyConflict` while preserving `WhenMatched::Fail`;
- a source-walk guard rejects keyed `stage_append`, including the former
  fast-forward path;
- a retry reruns validation rather than committing the stale staged batch.

### 11.3 Migration and recovery tests

- main plus owned and lazy-inherited graph branches all emerge PK-annotated;
- duplicate historical IDs abort before the fencing-compatible stamp;
- crash after each table annotation and before each manifest repoint resumes
  without data change;
- crash before/after lazy fork and PK metadata commit recovers the exact
  sidecar-recorded ref/version;
- branch enumeration is incarnation-safe;
- old binary/new graph and new binary/partially migrated graph refuse loudly;
- heads-first, fencing-first, and co-release upgrades preserve every active
  format capability and produce identical logical rows;
- overwrite, schema apply, branch fork, restore, and import preserve PK metadata.

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
- PK migration is all-branch, offline, idempotent, and roll-forward-only.
- A retryable upsert conflict retries the logical operation; strict insert maps
  both existing and concurrent matches to `KeyConflict` without changing mode.
- Read-set validation remains a separate required concurrency design.

### Open ship gates

1. Upstream symmetric filtered/unfiltered and filtered/Append conflict behavior.
2. v2-path scale result versus indexed-path filter availability.
3. Operator repair procedure for pre-existing duplicate IDs.
4. Rename-stable field/type identity.
5. Cross-process recovery ownership before any broadened topology claim.
6. Final format-number/release sequencing and the all-branch fleet stamp.
7. The fenced bulk adopt-merge memory/cost gate on embedding-bearing tables
   (the #277 regression class) — see §5.1 and §11.4.
