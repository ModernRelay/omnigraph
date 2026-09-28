---
rfc: "2026-09-28-group-commit"
title: "Group commit"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-09-28
updated: 2026-09-28
discussion: "https://github.com/ModernRelay/omnigraph/pull/785"
supersedes: []
superseded_by: []
blocked_on:
  - "Dependency: the shared schema gate (PR #783) merged; the publisher takes its shared side once per batch."
  - "Surface guard: the fragments of several strict inserts, each staged against one pin, commit as one detached transaction from a later pin of the same table: the rows are the union, row ids come fresh from the later pin's counter, Lance derives the row-version metadata at commit, and the change feed's one-transaction proof and merge's insert-absence proof accept the result."
  - "Correctness: the admission matrix under Evidence and tests, failpoints around the composed detached commits and the compare-and-swap including the in-doubt read-back, and the DST concurrent universe with the publisher on (every acknowledged write visible exactly once, one actor per commit)."
  - "Instrument: `concurrent-writes` with eight writers on one branch, local and on RustFS at +30 ms per round trip, above one writer's rate on the same build (31.7 and 0.87 commits/s on `b14c22c5` with PR #783), with one writer within noise of today and per-writer commit counts within a factor of two of each other (#784)."
---

# RFC: Group commit

**Depends on:** [RFC 0067](0067-detached-table-commits.md), whose
Throughput section sketches this as step 3 of its path;
[RFC: Detached-only tables](2026-09-21-detached-only-tables.md), which leaves
no work after a publication and defines the staging witness the collector
judges; [RFC: Shared schema gate](2026-09-18-shared-schema-gate.md), whose
decision log holds the measurement that motivates this.
**Surveyed:** OmniGraph `main` at `b14c22c5` and PR #783 at `8863d607`: the
publication path in `omnigraph-catalog` (`publisher.rs`, `commit.rs`,
`state.rs`, `commit_graph.rs`, `retention.rs`), the engine's write path
(`exec/staging.rs`, `exec/mutation.rs`, `validate.rs`, `loader/mod.rs`,
`table_store.rs`, `db/omnigraph.rs`, `db/write_queue.rs`), the change feed
(`changes/`), merge discovery (`exec/merge.rs`) and the collector
(`db/omnigraph/collector.rs`); the complete Lance
[distributed write](https://lance.org/guide/distributed_write/) guide and
[transaction specification](https://lance.org/format/table/transaction/),
read 2026-09-28.

## Summary

Several writes on one branch publish with one `__manifest` compare-and-swap
as one graph commit. A publisher per branch in each process takes over the
part of a write that runs under the branch gate today. Writers prepare as
they do now and submit what they staged. While one publication is in flight,
new submissions queue. When it returns, the publisher admits the queued
writes that cannot invalidate each other or anything published since they
were captured, commits each table's admitted effects as one detached commit,
and publishes them through the unchanged single-commit publisher. Every
admitted write is acknowledged with that commit.

Two decisions keep this small:

- **A batch is one graph commit.** Its effect is the union of its writes.
  The writes were in flight together, so no reader could observe an order
  between them, and no intermediate state was ever a branch head. Every
  surface that maps a commit to its `__manifest` version keeps working: time
  travel, diffs, the change feed, merge bases, retention roots and the
  lost-acknowledgement read-back. RFC 0067's sketch, N commit rows in one
  publication, breaks every one of them (Alternatives).
- **Same-table inserts compose by key.** Fragments that independent writers
  wrote against one table commit as one transaction, the shape Lance
  documents for distributed writes. This is the case the measurement
  needs: every writer in the `concurrent-writes` benchmark inserts rows into
  one table, so admission by disjoint tables alone would batch nothing.

A batch holds the writes of one actor, because a commit row names one actor.
A write that names an expected graph head always publishes alone. No storage
format, wire shape or API field changes.

## Motivation

### The same-branch ceiling

The measurement is recorded in the shared schema gate RFC's decision log
(entries dated 2026-09-28, added by PR #783). The instrument was `concurrent-writes`, closed-loop, with
insert-only mutations and disjoint keys, on the local filesystem and on
RustFS behind toxiproxy adding about 30 ms per round trip.

| | Local | +30 ms |
|---|---|---|
| One writer on one branch | 31.7 commits/s | 0.87 commits/s |
| Eight writers on one branch, PR #783 | 25.5 | 0.40 |
| Branch-gate hold for one commit | 7.3 ms | 670 ms |
| of which revalidation / detached commit / publication | 0.6 / 1.1 / 5.6 ms | 298 / 85 / 284 ms |
| Failed revalidations per publication, eight writers | 6.6 | 2.9 |

Revalidation fails whenever the branch head has moved, so every publication
invalidates every other prepared write on the branch. Eight writers on one
branch therefore cannot exceed one writer. At +30 ms each failed attempt
holds the gate for about 300 ms before giving up, about 19 s of a 30 s
window, which is more than the successful writes used. Failing faster from
the handle's in-memory view was prototyped and measured. It raised the
+30 ms rate from 0.40 to 0.53 commits/s, lowered the local rate, and left
the ceiling where it was. Issue #784 is the same mechanism seen from the
writers' side: at +30 ms one writer makes all twelve commits of each run.

### What a batch buys

The work under the branch gate is fixed per publication:

- revalidation's round trips;
- one detached commit per touched table;
- the catalog's copy-on-write rewrite at the next `__manifest` version, with
  its compare-and-swap (`commit::overwrite` since #781).

With B writes per publication that cost is paid once, and the detached
commits of a batch's tables run in parallel. No prepared write is
invalidated by a write it does not conflict with.

The model: at +30 ms a batch costs about the 670 ms one write costs today,
so eight writes per batch would publish several times the current rate.
This is a model, not a claim. The instrument in `blocked_on` decides, and
RFC 0039's rule that claims need open-loop driving applies to anything
stated later.

The history-dependent costs RFC 0067 and RFC 0068 measure grow with
publications, not with writes. A publication adds one registration row per
touched table and two lineage rows. Under load, B same-table inserts
produce one registration row instead of B, so `__manifest` grows B times
more slowly.

### Why disjoint tables are not enough

RFC 0067 admits an entry when the tables it read and wrote are disjoint from
the tables other entries wrote. All eight benchmark writers insert `Chunk`
rows, so that rule admits one entry per batch. Insert-heavy ingestion into
a few types is the common production shape too.

Lance itself tracks conflicts between concurrent inserts at key grain: a
merge-insert `Update` carries `inserted_rows`, a key-existence filter "used
for conflict detection" (transaction specification, Update). The engine
already carries the exact ids of a production strict insert on its
`StagedWrite`.

## User and operational behavior

- **Commit grain.** Concurrent writes by one actor on one branch may publish
  as one graph commit.
  - Each write's receipt names that commit.
  - `commit list` shows one commit for all of them.
  - That commit's change-feed block, diff and time-travel snapshot contain
    every one of its writes.
  - Without contention every write is its own commit, as today.
  - `graph_manifest_version` in `CommitOutput` stays the commit's own
    version.
- **Conditional writes.** A write that names an expected graph head
  (`Omnigraph-If-Graph-Commit`, or `expected_head` in the SDK) is always its
  own commit, and that commit's parent is the named head.
- **Errors.** A write the publisher refuses at admission gets the typed
  read-set conflict (`ReadSetChanged`). The existing bounded re-prepare
  loop already handles it; for an insert-only mutation that loop retries up
  to 32 times. A publication whose outcome is in doubt gives every write in
  the batch the in-doubt error, naming the batch's commit id.
- **Fairness.** Entries are admitted in arrival order. A refused entry
  re-prepares and keeps its original place, so a writer that just published
  cannot overtake the writers it made stale (#784).
- **Cancellation.**
  - A caller that goes away before its entry is admitted is dropped from the
    queue.
  - Once admitted, the publication runs to its outcome on the publisher's
    task, whatever happens to any one caller.
  - A write whose caller disconnected after admission may land. That caller
    is in the same position as a lost acknowledgement today.
- **Unchanged.**
  - Merge, schema apply, index builds, Optimize, cleanup and branch controls
    publish as today, and the branch gate serializes them with batches.
  - Reads.
  - Writers in different processes still race only at the compare-and-swap.
- **Documentation.** `docs/dev/writes.md` gets the write path. The user
  guide's commit pages say that a commit may carry several writes. The
  release notes record the commit-grain change.

## Design

### Entries

A mutation or load prepares exactly as today, and nothing is committed
before submission:

1. capture the authority;
2. execute;
3. validate at the pinned base;
4. stage: fragments are written and each table's Lance transaction is built.

Instead of taking the gates in `commit_all`, it submits an **entry** and
waits for its outcome. An entry holds:

- the captured authority: branch incarnation, graph head H0, schema
  identity, and the caller's expected head if any;
- for each written table, the staged write, the pin it was staged from, and
  its class (below);
- the **read footprint**: every table the write's validation read that it
  does not write. Edge-endpoint existence reads the two endpoint node
  tables. A node delete's cascade and referential checks read every edge
  table incident to its type. An overwrite load's referential checks read
  edge tables outside the load.
- the actor and an arrival ticket.

Today these reads are derivable from the changeset and the catalog but
recorded nowhere. `MutationStaging.expected_versions` covers only the tables
opened through `ensure_path` (`exec/staging.rs`, `validate.rs`). Recording
them is the first rollout step.

Each written table is in one of two classes:

- **Append-only**, when all of these hold:
  - the write is a strict insert;
  - its staged transaction adds fragments and removes or updates none;
  - its exact inserted ids fit the per-entry cap;
  - its table is a node table with no non-key `@unique` group, or an edge
    table with no bounded `@card`.

  Validation of such a write reads only the key-absence probe on its own
  table and, for an edge, the existence of its endpoints.
- **Exclusive** covers everything else: update, delete, upsert, overwrite,
  cascades, non-key uniqueness, bounded cardinality (whose check reads a
  fresh live version, `validate.rs`), and oversized id sets.

### The publisher

Each process runs one publisher per graph root and branch. It lives in the
process-global write-queue manager that already owns the gates
(`db/write_queue.rs`, keyed by root). It starts at the first entry, ends
when idle, and owns a coordinator for its branch.

It runs one batch at a time:

1. **Gates.** Take the schema gate's shared side and the branch gate, which
   is what `commit_all` takes today. The per-table gates it also takes add
   no exclusion inside the branch gate (the shared gate's decision log), so
   the publisher does not take them.
2. **Revalidate once** with the checks `revalidate_write_txn` runs per
   write today: the schema-apply sentinel, the branch authority probe and
   the schema contract. This yields the current head Hc and its pins.
3. **Log check.** If Hc is not the head the publisher's last batch
   produced, another publication landed; clear the effect log.
4. **Admit** queued entries in ticket order, up to caps on entries and on
   composed fragments. The fragment cap keeps a composed transaction inline
   in its manifest.
   - An entry of another actor stays queued and leads the next batch.
   - An entry with an expected head is admitted only as the batch's first
     entry and only when Hc equals that head, and it closes the batch.
5. **Commit** each table's effect detached from the table's pin at Hc,
   stamped with the staging witness (branch incarnation, Hc). Tables commit
   in parallel.
6. **Publish** with `publish_with_precondition`, passing:
   - one `DatasetUpdate` per table;
   - the expected versions at Hc for every table in the batch's footprints;
   - one `LineageIntent` carrying the batch's actor;
   - `ExactGraphHead(Hc)`.

   The publisher API is unchanged.
7. **Complete** every entry with the outcome. A submitting handle applies
   the published snapshot to its own coordinator before its receipt
   returns, so a read on that handle sees its write. No handle's
   coordinator is held across the compare-and-swap. That is expected to
   remove the same-handle read wait the shared gate RFC lists as
   unresolved; the implementation has to show it.

Two failure outcomes:

- **Confirmed refusal** (`ReadSetChanged`): delete the batch's detached
  manifests, which is the existing rule for a refused publication, refresh,
  and judge the entries again from step 3. Entries captured before the
  foreign commit are refused.
- **In doubt:** every entry gets the in-doubt outcome. The read-back is
  unchanged, because it looks for exactly one commit.

### Admission

Take an entry k captured at H0 and a batch forming at Hc. Let *writers*
be every commit in (H0, Hc] together with every entry admitted earlier in
this batch. k is admitted when all three hold:

1. **Authority.** Its branch incarnation and schema identity equal the
   batch's.
2. **Horizon.** Every commit in (H0, Hc] is in the effect log, so this
   publisher published it, and there are at most K of them.
3. **Conflicts.** k's footprint is its written tables plus its read
   footprint.
   - If k has any exclusive table, no writer wrote any table in k's
     footprint.
   - If k is append-only:
     - no writer wrote any table in k's footprint exclusively;
     - for every table that k and a writer both appended to, their id sets
       are disjoint.

The second case follows from what validation read. An append-only entry
read the absence of its own ids and the existence of its endpoints. Appends
by other writers with other ids preserve both; an exclusive write to any
footprint table may not. An exclusive entry read rows or absences of
arbitrary shape, such as a predicate scan, a cascade, or referential
emptiness, so any write to its footprint invalidates it.

The batch is conflict-serializable in admission order:

- an entry's reads are unaffected by every earlier writer;
- a later entry's writes come after its reads in that order;
- a write that lands after a read is not a conflict in that order.

Constraints validated at an entry's base therefore hold at Hc. A constraint
the rule cannot carry forward makes the write exclusive, and then its whole
footprint must be unchanged.

### Composition

Each table in a batch has either one exclusive entry or any number of
append-only entries; the admission rule excludes a mix.

- **Exclusive:** the entry's transaction commits as staged. Its base is
  still the table's pin, because nothing wrote the table since H0.
- **Append-only:** one transaction whose new fragments are the union of the
  entries' fragments, read from the table's pin at Hc. That pin may be later
  than an entry's base, but only appends with disjoint ids came in between.
  Lance assigns what the union needs at commit, and never earlier:
  - fragment ids: "not assigned at transaction creation time; they are
    assigned during manifest construction" (Append);
  - stable row ids: "only the commit knows which values are free"
    (distributed write);
  - row-version metadata: "leave them as `None`. Lance derives both while
    building the manifest" (distributed write).

  The composed transaction carries the markers its components carry.
  `omnigraph.no_by_source_delete` is copied. `omnigraph.insert_absence` is
  minted again for the composed base: the admission proof establishes
  absence at Hc with no further read, and a guard pins that the proof and
  the merge chain's reading of it agree.

The commit goes through the sealed table adapter, as a new entry the
durable-call guard registers. The change feed's fast path still applies. It
requires the pinned transaction's read version to be the previous pin
(`changes/candidate_scan.rs`), and a composed commit's read version is the
table's pin at Hc.

### The effect log

The effect log is in memory, per publisher. For each of the publisher's
last K batches it records:

- the commit id;
- the head that batch produced;
- for each table the batch wrote, either "exclusive" or the ids it appended.

An append whose ids exceed the cap is logged as exclusive. The log is
bounded by K times the cap.

It caches facts about immutable, published commits, which invariant 12
permits. It is never commit authority; `ExactGraphHead(Hc)` is. If the
process restarts, the log is lost, and entries captured before the restart
fail the horizon rule and re-prepare.

### Why the collector's staging rule does not change

The collector judges an unpublished staging dead once the branch head has
moved past the head it recorded (`judge_staging` in the collector). Under
this RFC, every detached commit is written by the publisher after
admission and stamped with Hc. A batch that loses the compare-and-swap
leaves stagings the existing rule proves dead once the head moves past Hc.

RFC 0067's sketch had writers commit detached effects before submitting.
Those effects would carry the entry's own H0, and an admitted entry whose
H0 is older than Hc would be judged dead while it was about to publish.
Committing after admission keeps the rule. It also keeps the shared gate's
finding that detached commits made ahead of admission are mostly garbage
under contention.

An entry's fragments are unreferenced from preparation until its batch's
detached commit. The collector deletes a file that no manifest references
only when the file is older than `UNVERIFIED_THRESHOLD_DAYS`, which is seven
days. Every write already relies on that window between preparation and
its detached commit. A bounded queue and the horizon keep an entry's wait
many orders of magnitude inside it. A refused entry's fragments are the
orphans every failed revalidation leaves today, and the same age rule
reclaims them.

## Invariants

- **2 (one publication door).** Unchanged: one publication per batch.
- **3 (one coherent view).**
  - Each entry holds one captured view, and the batch revalidates the
    complete authority once before any of its effects.
  - An entry admitted at Hc later than its H0 publishes against state that
    differs from its capture only by commits the admission rule proves did
    not touch its footprint. That is backward validation, the
    optimistic-concurrency shape databases use.
  - The invariant's wording, "revalidates that complete authority before
    effects" and "never combines fresh and stale facts", needs a precise
    amendment in the implementation change: an entry's footprint is proven
    unchanged, not its whole view.
- **4 (publish once).** Every write publishes once, in exactly one commit.
- **5 (recovery).** Unchanged. Detached effects are committed only after
  admission, and are garbage under the unchanged rule otherwise.
- **7 (derived acceleration).** A composed append leaves an unindexed tail
  like any append.
- **8 (loud integrity).** Every constraint an entry validated holds at
  publication by the admission rule. Those it cannot carry forward make the
  write exclusive.
- **10 (policy).** Enforced per write at its `_as` entry point before
  submission. Unchanged.
- **11 (bounded).**
  - The queue is bounded by entries and bytes, and the effect log by K and
    the cap.
  - A refusal goes through the existing bounded re-prepare loop and is
    counted as a re-prepare.
- **12 (one source of truth).** The effect log caches immutable facts. The
  queue holds requests, not state derivable from the manifest.
- **Deny-list.**
  - The publisher is process-local and is not presented as fencing; the
    compare-and-swap remains the fence between processes.
  - It adds no job queue for manifest-derived work.

## Compatibility and reversibility

- **Storage:** none. Commit, head and registration rows are unchanged, and
  an older binary reads a batch commit as an ordinary commit.
- **Wire:** none.
- **Behavior:** commit grain under concurrent writes by one actor (User and
  operational behavior).
- **Reversal:** a batch cap of one entry reproduces today's behavior and is
  the rollback.

## Alternatives

- **N commit rows in one publication**, as RFC 0067 sketched. Both code
  surveys for this draft found it breaks every mapping from a commit to its
  `__manifest` version:
  - Snapshot reads and diffs of a non-last member fail their head check
    (`graph_coordinator.rs`, `feed.rs`), and so does the change feed.
  - The feed's one-transaction proof needs one detached commit per interval.
  - Merge bases and the collector's merge-base roots go through
    `pinned_graph_commit` (`retention.rs`), which aborts the whole cleanup
    plan on a non-head member.
  - Parent resolution orders by `(graph_manifest_version, created_at, id)`,
    so it can silently fork lineage.
  - `commit::overwrite` keeps duplicate head rows within one publication.
  - The lost-acknowledgement read-back recognizes only the head commit.
  - `ExactGraphHead` holds one expectation.
  - Schema apply records its parent before publication.

  Per-commit pins would need registration rows keyed by commit, a format
  change. Its interior commits would name states no reader ever observed,
  and with same-table composition they would have no table versions at all.
- **Admission by disjoint tables only.** No gain on the measured workload.
- **Chaining same-table writes**, staging each on the previous write's
  detached version the way merge chunks chain. This serializes preparation
  per table, and #784 puts one preparation at about 2.4 s at +30 ms under
  contention. Refusing one link refuses its successors. Composition needs no
  re-preparation.
- **A timed batching window.** Adds latency at low load. Queuing while a
  publication is in flight batches without a timer.
- **A leader-follower write group without a task**, the RocksDB write-thread
  shape. The leader's future would own everyone's publication, so cancelling
  one caller would abandon the others'.
- **Failing fast before revalidation.** Measured, and bounded by the
  single-writer ceiling (shared gate decision log).
- **Batches across actors.** Needs per-write attribution in the commit row, a
  format change. Its natural home is beside #513's per-write idempotency key.
- **RFC 0068's commit record.** Lowers the per-publication cost. It is
  orthogonal and composes with this: one record per batch.

## Evidence and tests

- **`lance_surface_guards.rs`: the composition guard.**
  - Setup: strict inserts staged against pin P0, with a further append
    landing in between, committed as one detached transaction from the later
    pin.
  - Pinned: rows, row ids, fragment ids, row-version metadata, and both
    markers.
  - `changes.rs` and the merge owners assert that the feed's fast path and
    the merge chain's insert-absence proof accept the composed commit.
- **`writes.rs`: the admission matrix.** Each cell asserts the commit count,
  the rows, and which entry re-prepared.
  - two appends with disjoint ids make one commit;
  - the same id makes one commit, and the other entry re-prepares into its
    key conflict;
  - an append against an exclusive write on the same table;
  - an edge insert against a delete on its endpoint type;
  - bounded `@card` is exclusive;
  - an expected head publishes alone;
  - two actors make two commits;
  - a publication by merge or by another handle between capture and batch.
- **`failpoints.rs`.** For each window, assert what did not move:
  - after the composed detached commits and before publication;
  - a refused compare-and-swap, whose detached manifests are deleted;
  - in doubt, where every entry is in doubt and the read-back resolves the
    batch;
  - a waiter cancelled before and after admission.
- **DST: the concurrent universe with the publisher on.**
  - The writers already share the process-global manager, so batching
    happens.
  - The seam scheduler needs the publisher task as an actor.
  - Oracle: every acknowledged write is visible exactly once, and each
    commit has one actor.
  - The collector oracle runs with batches in flight.
- **`write_cost.rs`.** A batch of one costs what a write costs today.
- **`concurrent-writes`.** One and eight writers on one branch, local and at
  +30 ms, with the per-writer commit distribution.

## Rollout

Each step leaves `main` shippable.

1. Record the read footprint and each staged table's class on the
   mutation and load staging. No behavior change.
2. Add the publisher with batches of one. It replaces the gate section of
   `commit_all`, behavior is identical, and the cost owner and DST stay
   green.
3. Batch exclusive entries by footprint.
4. Compose append-only entries, behind the surface guard.
5. Run the instrument; update the documentation and the release note.

## Unresolved questions

- **K and the caps.** Chosen from the instrument by the implementer.
- **Loads in batches.** A small load composes under the same rules, and a
  large one exceeds the id cap and is exclusive. Recommended: admit loads.
  Decided in step 3.
- **Merge, index builds and Optimize as exclusive entries**, instead of
  taking the branch gate between batches. Later, once batches are measured.
- **Admitting across a foreign commit** by reading that commit's per-table
  change sets, instead of refusing every entry captured before it. The
  change sets exist (`omnigraph.deleted_ids` and the fragments of each
  detached transaction). Later.
- **Where per-write attribution belongs.** It is needed for batches across
  actors and for #513's per-write idempotency key: in this format or in RFC
  0068's record. Decided when either is proposed.
- **The next lever after this one.** Once publication is amortized, a
  writer's cycle at +30 ms is dominated by preparation's own round trips:
  the sentinel, the schema contract and the branch probe. A capture served
  from the publisher's known head is the next step. Not part of this RFC.

## Decision log

- 2026-09-28 — Drafted from the shared schema gate's step-2 measurement and
  three code surveys of `main` plus PR #783 (lineage and time travel, the
  change feed and per-commit changes, write footprints and the publisher).
  - Chose one graph commit per batch over RFC 0067's N commit rows, because
    the surveys found every commit-to-version mapping assumes it.
  - Chose key-grain composition of same-table inserts, because the measured
    workload writes one table.
  - Chose detached commits after admission, because the collector's staging
    rule would otherwise sweep admitted entries.
