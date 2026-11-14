---
rfc: "2026-11-14-group-commit-with-known-key-admission"
title: "Group commit with known-key admission"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-11-14
updated: 2026-11-14
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Engine half of the composition guard: a composed commit keeps only the markers every component carries, and the change feed's one-transaction fast path and the branch merge's pure-insert proof accept it; `lance_surface_guards.rs` pins a Blob v2 column through composition before tables with a Blob column are eligible. The Lance half is `lance_surface_guards.rs::group_commit_composes_inserts_staged_against_older_pins_onto_a_later_pin`, carried over from PR #785 and passing on Lance 13.0.0."
  - "Admission and failure evidence: the admission matrix (Evidence and tests), `failpoints.rs` and `detached_commit_matrix.rs` windows for the batch path, and the DST concurrent universe with batching on: every acknowledged write visible exactly once, one actor per commit, the collector oracle with batches in flight."
  - "Batching forced on across the existing gates: the GQT corpus, the engine `integration`, `writes` and `failpoints` binaries, and the configured RustFS and Azurite suites."
  - "Soak: the throughput instrument at 64 writers on one branch, RustFS behind toxiproxy at +30 ms, for 30 minutes with `optimize` every 5 minutes and `index build` every minute. Every acknowledged write verified, every maintenance run published, writes/s over the last 5 minutes at least 80% of the first 5, peak RSS at 30 minutes within 1.5x of the 10-minute mark."
  - "Instrument gate on the production build, same backend: at 16 and 64 writers on one branch at least 10x the baseline's writes/s with p99 under 1 s, one writer within 10% of the ordinary path, per-writer counts within 2x of each other (#784)."
---

# RFC: Group commit with known-key admission

> A term set in ***bold italics*** is being defined at that exact spot.

**Depends on:** [RFC 0067](0067-detached-table-commits.md), whose
throughput path names group commit as its third step;
[Detached-only tables](2026-09-21-detached-only-tables.md), which leaves no
work after a publication and defines the staging witness the collector
judges; [Writes carried over row-preserving publications](2026-11-10-writes-carried-over-row-preserving-publications.md),
whose carry proof and gated retry this RFC reuses;
[Writer epoch fence on the branch manifest](2026-11-09-writer-epoch-fence.md).
**Surveyed:** Omnigraph `main` at `2c0941e0`; PR #785 at `0069e925`; the
research drafts #1399 (`e628c35d`), #1400 (`f22f0718`) and #1401
(`ad39204a`), with their reports, logs, raw results and prototypes
(`db/group_commit.rs`, `table_store::known_keys`); Lance 13.0.0's
transaction, versioning, branch/tag, distributed-write, row-ID-lineage,
object-store and Blob pages, read 2026-11-14, and its `io/commit.rs`,
`lance-table` `transaction/manifest_build.rs` and `transaction/row_version.rs`.

This RFC supersedes the group-commit draft of PR #785
(`2026-09-28-group-commit.md`, never merged, so no metadata names it) and
consolidates the three research drafts. Their prototypes are evidence, not
code to land.

## Summary

Concurrent insert-only writes by one actor on one branch publish in one
`__manifest` compare-and-swap, as one graph commit. Writers capture,
execute, validate and stage as today, then queue their staged effects in the
branch's ***write group***, one per branch and graph root in the process.
The writer that finds no batch in flight leads. Under the branch gates it
captures the current authority once and admits each queued entry whose exact
inserted keys are disjoint from everything published since its capture. It
then composes each table's admitted inserts into one Lance transaction on
the table's current pin, commits it detached and publishes once. Every
admitted write is acknowledged with that commit, after the publication is
durable and visible. A refused write prepares again under the writer gates,
so it still finishes within two attempts in its process.

Admission proves that an entry's ***footprint***, the facts its preparation
read, is unchanged, instead of revalidating its whole view; invariant 3 gets
a precise rewording. A process-wide map of the exact keys this process
wrote into each immutable data file, the ***known keys***, lets a
key-absence probe read the indexed fragments and memory instead of every
unindexed fragment. That probe is what limits writers once publication is
amortized.

No storage format, wire shape or API field changes. Writes that update or
delete, name an expected head, or are validated against committed rows an
insert can change keep today's path: non-key `@unique` and bounded `@card`
read such rows.

## Motivation

### One branch serves one write per publication

A write on RustFS at +30 ms per request takes about 10.5 serial round trips.
Six of them are under the branch gate: the table's transaction file and
detached manifest, the publisher's `_versions` listing, then the
`__manifest` data file, its transaction file and the version put (#1401
§3, traced). Revalidation refuses whenever the branch head moved. So every
publication sends every other prepared write on the branch back. Since
#1349 the second attempt prepares under the writer gates. Writers are now
treated fairly, but the branch still serves one write per publication:
2.2 writes/s at 4, 16 and 64 writers on RustFS, about 22 locally, with p99
growing with the writer count (22.7 s at 64 writers; #1401 §4 and §5.9).
Issue #784 is the same mechanism before #1349: one writer made every commit.

The floor is one conditional put per publication, serial per branch. With
Lance 13 a publication is five serial puts: the table's transaction file,
its manifest, then the `__manifest` data file, transaction file and
version. Lance writes the transaction file before the manifest
(`do_commit_detached_transaction`; the switch that skips it,
`ManifestWriteConfig::with_transaction_file_disabled`, is `#[cfg(test)]`).
That is about 165 ms at 32 ms per request. More than about six writes per
second per branch therefore needs several writes per publication
(#1401 §3).

### What a batch buys

The gated work is fixed per publication. With B writes in one publication it
is paid once, and `__manifest` grows B times more slowly: one registration
row per touched table per batch. Admission by disjoint tables batches
nothing on the measured workload, because every writer inserts into one
table. Insert-heavy ingestion into a few types is the common production
shape too. An `insert` into a `@key` type stages as an upsert
(`PendingMode::Upsert`, `exec/mutation.rs`). When none of its keys exists
and the id index leaves a small enough tail for the exact-id probe, the
upsert stages through the absence-proven insert writer, as a strict insert
does (`TableStore::stage_keyed_write`). Its read is then key absence, so
composition works at key grain.

### After batching, the writers' own probe is the limit

An insert into a keyed type first proves its keys absent
(`first_existing_id`, `existing_id_count`). This is a Lance scan with
`id IN (…)`: the id BTREE answers indexed fragments, and the id column of
every fragment the index does not cover is read. Every write adds a
fragment. Only explicit maintenance rebuilds the index, since invariant 7
forbids inline index work. So the probe's cost grows with every write since
the last index build. With batching on, #1401 measured every variant topping
out near 70 writes/s locally with two writes per publication: preparation
was the limit, not publication. Known keys removed the limit (Evidence).
#1399 met the same wall on a table without an id index: 6.3 cores busy at
62 writes/s, and CPU per write rising from 31 to 110 ms within one run as
fragments grew.

## User and operational behavior

### What a writer observes

- **Acknowledgement.** A write is acknowledged only after the `__manifest`
  publication holding its effects is durable and visible on its branch. A
  lost acknowledgement counts only when the read-back of the attempted
  version finds the batch's commit. Nothing is acknowledged from the queue,
  a detached commit or a log.
- **Commit grain.** Concurrent writes by one actor on one branch may share
  one graph commit. Each write's receipt names that commit: its
  `CommitOutput` (`graph_commit_id`, `graph_manifest_version`,
  `parent_commit_id`, `actor_id`) is the batch's. `commit list`, the
  commit's change-feed block, its diff and its time-travel snapshot hold
  every one of its writes. A write's own affected counts stay its own.
  Without contention every write is its own commit, as today, and
  consecutive writes from one client never share one: the second starts
  after the first's acknowledgement.
- **Lineage.** One commit per batch, parented on the head the batch built
  on. The batch's intermediate states were never a branch head and cannot be
  addressed. Its writes were in flight together, so no reader could observe
  an order between them, and admission makes every order give the same
  state.
- **Actor.** A batch holds the writes of one actor, because a lineage row,
  and an RFC 0068 commit record, names one actor. Policy is checked per
  write at its `_as` entry point before it queues.
- **Commit record.** Under [RFC 0068](0068-graph-commit-record.md) a batch
  writes one record. Nothing here needs a record per write.
- **Conditional writes.** A write that names an expected head
  (`--if-commit`, `Omnigraph-If-Graph-Commit`, `expected_head`) never joins
  a batch. It publishes alone on today's path, and its commit's parent is
  the named head. A precondition scoped to what a write touches (#1397)
  would let such writes batch; that is future work.

### Outcomes per member

| Batch outcome | Each member |
|---|---|
| Refused at admission | Sent back: it prepares again under the writer gates and reruns its request (#1349). `ReadSetChanged` reaches the caller only when the retry bound is exhausted |
| Fails before the compare-and-swap with definite evidence (a detached commit, the lineage intent, a publisher read) | The batch's typed error with `CompletionEvidence::Unpublished`; the staging stays unreachable |
| Loses the compare-and-swap | Sent back, as a single write that loses it is today |
| `WriterFenced` | `WriterFenced`, not retried; the process's holder for the branch is cleared |
| Publication in doubt | In doubt (`manifest_publish_in_doubt`), naming the batch's attempted commit id, with uncertain evidence; a server closes admission once, as for one in-doubt write |
| The leader's future is dropped (an embedded caller) | Before the batch's first effect: sent back. After it: uncertain evidence. The next waiter leads either way |

An HTTP write is an owned operation, so a disconnect never drops its future
([Owned server operations](2026-09-30-owned-server-operations.md)). An
embedded caller that drops its future before admission leaves the queue.

### Fairness, settings and what does not change

- **Fairness.** Entries are admitted in arrival order. A member sent back
  takes the branch gate for its second attempt in its turn, so a writer that
  just published cannot overtake the writers it made stale (#784).
- **Settings.** One process-scope session setting, `group_commit` (`off`,
  `on`), default `off` until the gates in `blocked_on` pass, then `on`.
  `off` is today's path exactly. Known keys have no setting: they change
  cost, never a result.
- **Unchanged.** Reads; branch merge, schema apply, `optimize`, index
  builds, `cleanup` and branch controls, which serialize with batches at the
  branch gate; writers in other processes, which race only at the
  compare-and-swap; the writer epoch.
- **Documentation**, with the rollout step that enables batching:
  `docs/user/mutations/index.md` (Atomicity says separate commands are
  separate commits), `docs/user/branching/changes.md`, `docs/dev/writes.md`,
  `skills/omnigraph`, and a `changed` release note.

## Design

### Eligibility

An entry joins a batch only when all of these hold. Everything else takes
today's path, serialized with batches by the branch gate.

1. It is a mutation, and every operation it executed on every table it
   opened is an insert. The recorded `MutationOpKind` is `Insert` or
   `Merge`, that is, `!strict_pre_stage_version_check()`. `ensure_path`
   records the kind before any row is read (`exec/staging.rs`). So an update
   or delete that matched nothing still makes the entry ineligible, which is
   the counterexample #785's review found (Admission). Today
   `stage_all_with_concurrency` drops `op_kinds`; the entry carries it.
2. Every staged effect is an insert-only keyed `Update` from the
   absence-proven insert writer, carrying its exact inserted ids. Such an
   effect has new fragments only, `RewriteRows` mode and the inserted-key
   filter. An upsert that matched a key is not insert-only.
3. No written table declares a non-key `@unique` group, node or edge, and
   no written edge table declares a bounded `@card`. Both checks read
   committed rows another insert can change (`validate.rs`; cardinality
   opens a fresh live version).
4. No written table has a Blob column, until the composition guard covers
   one (Rollout).
5. There is no caller head precondition, no unattached base, and no
   `as commit` or `as unattached branch`.
6. It is a first attempt. A second attempt publishes alone (§Progress).

Edge inserts are eligible when rule 3 allows them. Their endpoint node
tables join the footprint as tables read and not written.

### The write group

Each process keeps one write group per graph root and branch in the
process-wide `WriteQueueManager` that owns the gates (`db/write_queue.rs`,
`for_root`). Every handle of the root therefore shares it, and two handles
of one process batch together. #1401's prototype keyed the queue per
handle; #1399's, per root.

The writers form the group themselves; the engine spawns no task. A writer
that submits while no batch is publishing leads. It publishes the batch
holding its own entry on its own task, then hands the lead to the next
waiting entry, or clears the flag when none waits. No writer publishes
others' writes at the expense of its own next one, and every publication
runs inside the owned operation of the writer leading it (#785, decision log
2026-10-03). A batch takes, in arrival order, the queued entries of its
first entry's actor. Entries of other actors keep their places, and the
oldest of them leads next. Batching is natural: a batch is whatever queued
while the previous one published. There is no timer.

A batch runs these steps.

1. **Gates.** The shared schema permit and the branch gate, which
   `commit_all` takes today.
2. **Capture once.** The current authority, as revalidation reads it. On a
   held bound branch this reads nothing (#1350). On an unheld branch the
   leader may skip the probe when its handle's installed view is the head
   the effect log last recorded and the log explains every entry's capture.
   A stale capture loses the compare-and-swap and sends the members back; it
   cannot change a member's meaning, because each member was prepared at its
   own capture.
3. **Admit** (§Admission), up to the caps (§Bounds).
4. **Compose and commit.** Per table, compose the admitted entries
   (§Composition) and commit the result detached through
   `TableStore::commit_staged_detached`, stamped with the batch's staging
   witness: the branch incarnation and the captured head. Tables commit in
   parallel.
5. **Publish** through the unchanged publisher: one dataset update per
   table, the expected versions at the captured head for every table in the
   admitted footprints, one lineage intent with the batch's actor, and
   `ExactGraphHead` on the captured head.
6. **Warm** each new table version's index metadata into the shared Lance
   session while the publication is in flight. Lance 13's detached commit
   does not populate the session caches (`do_commit_detached_transaction`
   skips `record_successful_commit`), so the next writer's validation would
   otherwise read it, one serial request per write (#1401 §5.3). This is
   cache population only.
7. **Complete.** Append the batch's effect to the log. Each submitting
   handle installs the published snapshot before its receipt returns, so a
   read on that handle sees its write. Answer every member and hand the
   lead.

### Admission

Let an entry e have captured head `He` and the batch capture head `Hc`.
Both are materialized heads; a named branch before its first publication has
none. The leader admits e, in arrival order, when all hold.

1. **Same authority.** e's branch incarnation and accepted schema identity
   are the batch's. Every table e read or wrote has the same table identity,
   dataset location and native ref at `Hc`.
2. **Explained interval.** Walking back from `Hc` to `He`, within K
   publications, every publication is explained. It is either in the
   branch's effect log (§The effect log), or each table pin it moved reaches
   the previous pin through `Rewrite` and `CreateIndex` commits alone, with
   the same row count and registrations. That second test is the durable
   proof `revalidate_write_txn_carrying` runs (#1349).
3. **Untouched reads.** For each table e inserts into, every effect in the
   interval preserved rows or inserted ids disjoint from e's ids there. For
   each table e read without writing, every effect preserved rows or only
   inserted.
4. **Disjoint batch.** No entry admitted earlier in this batch inserts any
   of e's ids into the same table.

A refused entry has made no effect of the batch.

**Why this is enough.** An eligible entry's outcome depends on four facts
and nothing else:

- the accepted schema and the registrations of the tables it touches;
- the absence of its own ids from the tables it inserts into;
- for an edge, the existence of its endpoint rows;
- checks over its own rows: types, required values, value constraints and
  computed embeddings.

Eligibility removes every other read. An executed update or delete reads
through an arbitrary predicate, even one that matches nothing. A non-key
`@unique` group reads other rows' committed values. A bounded `@card`
counts a source's committed edges. An overwrite reads the whole table.

Rule 1 keeps the first fact. Rules 2 and 3 keep the next two. Since `He`,
the tables e inserts into changed only by inserts of other ids and by
commits that changed no row, so e's ids are still absent at `Hc`. Inserts
and row-preserving commits remove no endpoint. The last fact depends on
nothing outside e. So e validates at `Hc` exactly as it did at `He`. Its
staged effect, a pure insertion of its rows, is the effect a fresh
preparation at `Hc` would stage.

Rule 4 makes the members of a batch pairwise disjoint per table, so they
commute. Every serial order of them on `Hc` yields the one state the
composed commit publishes. A batch is therefore equivalent to executing its
members serially, in admission order, on `Hc`. Every member was in flight
when the batch published, so the publication point lies inside every
member's invocation. The history of these writes on a branch is strictly
serializable.

What admission must still catch:

| Case | Caught by |
|---|---|
| Two entries insert the same new key | Rule 4 in one batch, rule 3 across batches. The later entry is sent back and its rerun sees the key: a strict insert gets `KeyConflict`, a keyed insert becomes an update on today's path |
| Another process wrote, merged or deleted since the capture | Rule 2: unexplained |
| An edge's endpoint was deleted since the capture | Rule 3 (logged as `changed`) or rule 2 |
| A schema apply, or the branch deleted and recreated | Rule 1 |
| Write skew through a predicate scan of a table another entry writes | Eligibility: an executed update or delete is ineligible |
| Insert `Left` and update `Right` against insert `Right` and update `Left`, from one base where neither exists (#785 review) | Eligibility, from `op_kinds` carried through staging |
| Two edges with one non-key `@unique` value, or past a bounded `@card` | Eligibility |
| `optimize` or an index build since the capture | Explained as row-preserving; composition reads the current pin |
| A stale batch capture | `ExactGraphHead` refuses the batch; the members are sent back |

### The effect log

Each write group keeps an in-memory log. Every publication this process
makes on the branch, through any path, appends a record while it still
holds the branch gate. A record holds the head the publication built on, the
head it produced, and one class per touched table:

- `inserted(ids)`, for an insert-only effect carrying its exact ids;
- `row_preserving`, for a `Rewrite` or `CreateIndex` chain;
- `changed`, for anything else.

Logging ordinary-path publications too is what stops a sent-back member's
publication from refusing every entry queued behind it. #1401's first
prototype hit that cascade after a Bloom false positive.

The log is cleared by a publication that changes the schema contract or the
table set, by a branch delete or recreate, by an outcome the read-back
leaves in doubt, and by a process restart. Entries captured before then fail
rule 2 and are sent back.

The log is bounded by K records and a byte budget for ids. An effect with
more ids than one entry may carry is logged as `changed`. It caches facts
about immutable, published commits, as invariant 12 permits, and is never
commit authority: `ExactGraphHead` is.

Exact id sets decide admission. #1401's first version compared Lance's key
filters. These are Bloom filters, so a false positive refused an entry. That
entry's re-preparation published outside the log, and every older entry was
refused in turn; at 16 writers the batching collapsed.

### Composition

Each table in a batch receives only insert-only effects with pairwise
disjoint ids. The leader builds one `Update`, read on the table's pin at
`Hc`:

- `new_fragments` is the concatenation of the members' raw staged
  fragments. They come from each staged write's transaction, not from
  `StagedWrite::new_fragments()`: that is the read-your-writes copy, whose
  ids are provisionally assigned from the member's own base.
- `fields_for_preserving_frag_bitmap` is the members' common list. It is
  equal under one schema; a difference refuses composition.
- The inserted-key filter is the bitwise OR of the members' Bloom filters,
  which share one key column and one configuration.
- `omnigraph.insert_absence` is certified again for the composed base only
  when every component carries it: admission establishes absence at `Hc`
  with no further read. `omnigraph.no_by_source_delete` is kept only when
  every component carries it.

The composed commit goes through a sealed `table_store` adapter that
`forbidden_apis.rs` registers.

**What Lance 13 guarantees.** The guard this PR carries over from #785
pins the first three.

- Fragment ids are assigned while the manifest is built: "Fragment IDs are
  not assigned at transaction creation time" (transaction specification,
  Append). `Transaction::fragments_with_ids` fills only a zero id.
- Stable row ids come from the manifest's counter at commit: "Only the
  commit knows which values are free" (distributed write guide).
  `assign_row_ids` fills a missing or partial sequence, and a raw insert
  fragment carries an empty one.
- Row version metadata is derived while the manifest is built, and the
  composed rows share one stamp.
- The manifest is validated before it is written: fragment ids unique,
  required fields present, field-id allocation (transaction specification,
  Validation).
- An insert-only `Update` is compatible with `Rewrite` and `CreateIndex`:
  it overlaps no fragment, and graph tables use stable row ids. #1349's
  `TransactionRebase` guards pin this.
- Data files and their Blob v2 sidecars keep their names through
  composition. Lance names a data file at random
  (`generate_random_filename`) and writes its sidecars to
  `data/{data_file_key}/{blob_id}.blob` (`dataset/write.rs`), so nothing
  depends on the fragment id. The guard still has to pin a Blob column
  before Blob tables join.

**What Lance 13 does not guarantee.**

- **Key uniqueness.** The primary key is unenforced. A detached commit runs
  no conflict pass, and only `TransactionRebase` consults the inserted-key
  filter. Disjointness is admission's job.
- **Schema agreement** between components. The engine compares the schema
  identity and the field list.
- **Session caches** after a detached commit (step 6 above).
- **A bound on manifest growth.** Every member adds one fragment and the
  manifest lists them all. `FLAG_FRAGMENT_TREE`, a later format feature,
  takes fragments out of the manifest.

### Known keys

The known keys are a process-wide map from a data file's resolved object
location (dataset root, base and path) to the exact set of key values this
process wrote into it.

**Filling.** The absence-proven insert writer
(`stage_absence_proven_strict_insert`) fills it when its staged write
produced exactly one data file, the usual shape of a keyed batch, which
holds at most 8,192 rows. It records the ids from the key column it wrote,
after the file is written and before any manifest can name it. No other
writer records.

**Reading.** The key-absence probes, `first_existing_id` and
`existing_id_count`, use it when every fragment the id index leaves
uncovered at the pinned version meets three conditions: it has exactly one
data file, the map knows that file, and the fragment has no deletion file
and no overlay. The probe then scans only the indexed fragments (Lance
`fast_search`) and answers the uncovered ones from memory. Otherwise it
scans as today.

**Why the answer is exact.** Lance names every data file it writes from a
fresh random UUID (`generate_random_filename`) and never rewrites a file.
So the ids in a file are a fact about the object, true in every version,
branch and handle that lists it. A fragment with one data file, no deletion
file and no overlay holds exactly that file's rows. For such a fragment, an
id is present exactly when the map's set contains it, and the probe answers
what the scan would.

Schema evolution keeps the file's ids: a `Merge` adding nullable columns
writes no data file, and a `Project` keeps the key column's file. The
system-column upgrade renames the column without rewriting the file.

**When the map is bypassed.** The probe falls back to the scan for:

- a fragment with a deletion file, because deleted rows would make the map
  report presence that is gone: a false `KeyConflict`, or an upsert taken
  for an update;
- a second data file (a column patch) or an overlay;
- a file the map does not know: another process wrote it, compaction, a
  load or a merge rewrote it, an entry was evicted, or the process
  restarted;
- a table without an id index.

Another handle of the same process is not a bypass, since the map is
process-wide. A recreated branch or a restored backup cannot make an entry
wrong, because an entry describes an immutable object, not a branch. At
most it makes an entry irrelevant. Keying by resolved location keeps equal
names in different roots apart.

**Bound and eviction.** A process-wide byte budget, counting key bytes and
per-entry overhead, with the oldest entries evicted first. An evicted entry
costs only the scan it saves. The prototype bounded by id count (4 M); the
production bound is in bytes. Probes answered from the map, and misses, are
counted in `QueryIoProbes`.

**Derived state.** The map is a cache of immutable facts keyed by the
object they describe (invariant 12). A miss or a bypass reads the file. The
map changes cost, never a result, so it is physical acceleration under
invariant 7. A wrong entry would need the recording writer to record values
it did not write into that file. The owner test compares the map's answer
with the scan's on every fragment shape above.

### Bounds and backpressure

- **Queue.** Bounded per group by entries and by staged bytes. A submission
  beyond the bound waits for room, which slows the writer instead of
  dropping work. Server admission additionally bounds HTTP writes.
- **Batch.** At most B entries, and at most F composed fragments per table,
  so a composed transaction stays inline in its manifest. The prototype
  capped batches at 512 entries; its largest, about 120 writes at 256
  writers, stayed below that (#1401 §5.10).
- **Horizon and log.** K records and a byte budget. A capture older than
  the oldest record is sent back.
- **Known keys.** The byte budget above.
- **Retries.** Unchanged: two attempts within the process (§Progress) and
  `MAX_PRE_EFFECT_REPREPARES` (32) across processes.

B, F, K and the budgets are constants chosen from the instrument by the
implementer. The batcher's own memory is bounded by these caps. The #785
spike's RSS growth with commit rate came from the table-handle cache (#859)
and the history rewrite (#860, fixed), not from batching.

### Failure checkpoints and recovery

There is no new durable state and nothing to recover. Before the
compare-and-swap, a batch's detached commits are unreachable and stamped
with `Hc`, and each member's data files are unreferenced. After it, every
member's effect is complete. An ambiguous outcome is settled by the
publisher's existing read-back, which looks for exactly one commit.

**The collector's rule does not change.** Detached commits are made after
admission and stamped with the head the compare-and-swap names. A batch that
loses therefore leaves staging that `judge_staging` proves dead once the
head moves past `Hc`. #785 chose commits after admission for this reason.
Pipelining breaks it (Alternatives).

A member's data files are unreferenced between staging and its batch's
commit, as every write's are today. The collector deletes an unreferenced
file only after `UNVERIFIED_THRESHOLD_DAYS` (7 days). The queue bounds a
wait in publications, not wall-clock time; this RFC inherits that
assumption.

**Checkpoints the batch path must keep.**

- Each entry crosses `mutation.post_stage_pre_effect_gate` before it
  queues.
- The publisher's `publish.post_merge_pre_ack` and `publish.read_back` are
  crossed unchanged.
- New seams (`decide_seam!`, catalogued): after queueing, after admission,
  after the composed detached commits and before the compare-and-swap, and
  at the lead handoff.
- `detached_commit_matrix.rs` gains a grouped-insert writer through every
  window, fault and recovery actor.

### Progress: the #1331 bound

Since #1349 a write sent back before its effects prepares again under the
branch's writer gates, the shared schema permit and the branch gate, from
capture through publication (`PreEffectReprepares`,
`db::write_queue::preparing`). A member refused at admission, or whose batch
lost the compare-and-swap, keeps that rule. Its second attempt runs on
today's gated path and publishes alone, as a batch of one, under the gates
it holds. Its publication is logged, so entries queued behind it are judged
against its effect instead of refused wholesale.

Every publisher on the branch in the process takes the branch gate, a
batch's leader included. So nothing in the process moves the branch under
the second attempt, and a write still finishes within two attempts in its
process. Another process stays outside the bound: the compare-and-swap and
the 32-attempt limit cover it.

#1401's prototype re-prepared refused entries outside the gates and gave up
this bound. Its two `issue_1331` failpoint cases failed for that reason.

### Interactions

- **Writer epoch.** A batch's publication asserts the process's holder like
  any publication, and `WriterFenced` reaches every member. On a held bound
  branch the leader's capture reads nothing. The probe-free capture for
  writers stays the epoch's holder skip (#1350); this RFC adds none.
- **`optimize` and index builds: batches over maintenance.** A maintenance
  publication is row-preserving, so rule 2 explains it whether this process
  or another made it. Composition reads the compacted pin, and Lance assigns
  fresh ids above its high-water marks.
- **`optimize` and index builds: maintenance under batches.** An index
  build captures before staging and is refused when the head moved before
  its gates (`maintain_indices_for_branch`), and so is `optimize`'s direct
  path (`AuthorityCapture::BeforeGates`). #1399 measured 1 of 13 index
  builds landing at about 240 writes/s. Whatever raises the publication rate
  starves maintenance, and the unindexed tail then grows. This RFC carries
  both over insert-only and row-preserving publications, mirroring #1349:
  - under the gates, prove that each moved pin reaches the captured one
    through insert-only `Update` links and `Rewrite` or `CreateIndex`
    links, with registrations unchanged and no row count going down;
  - then re-anchor the staged `CreateIndex` or `Rewrite` with Lance's
    `TransactionRebase` before its first detached commit.

  Lance makes `CreateIndex` compatible with appended and updated fragments,
  which stay unindexed, and makes `Rewrite` conflict with an `Update` only
  on overlapping fragments. Served `optimize` keeps capturing under the
  gates (`AuthorityCapture::UnderGates`), so batches wait within the bound
  it already promises writers (`max_gate_hold`, `docs/dev/writes.md`).
- **Schema apply.** It takes the schema gate exclusively and waits for
  in-flight batches, which hold the shared permit. Its publication clears
  the log. A queued entry captured before it is sent back and reruns
  against the new schema; an insert crosses a schema change (#1349).
- **Branch merge, delete and recreate.** A merge into the branch takes the
  branch gate and is logged `changed` for the tables it moved. A delete or
  recreate fails rule 1.
- **`cleanup`.** Unchanged: a batch is an ordinary publication and its
  unpublished staging is judged by the existing rule.
- **The per-branch writer-state cache.** It is landing separately as a bug
  fix: one authority-coordinator slot per branch instead of the single
  shared `merge_authority_cache`. Batches on named branches use it, and
  this RFC does not redesign it. #1399 (§6.4), #1400 (E) and #1401 (§5.6,
  §5.8) measured it.

## Invariants

- **2, one publication door.** Unchanged: one compare-and-swap per batch,
  through the unchanged publisher.
- **3, one coherent accepted view.** Each entry holds one captured view, and
  the batch captures one authority before any of its effects. An entry
  admitted at a later head publishes against state that differs from its
  capture only by commits proven not to touch its footprint. This is
  backward validation, as FoundationDB's resolvers do it (#1401
  `prior-art.md`). #1349's carry is the row-preserving case of the same
  argument. The invariant's text changes when step 6 of the rollout lands.

  Today: "A writer captures schema, catalog, branch, graph head, native ref
  identity, and table baselines from one attempt, then revalidates that
  complete authority before effects. A retry starts a new attempt; it never
  combines fresh and stale facts."

  Proposed: "A writer captures schema, catalog, branch, graph head, native
  ref identity, and table baselines from one attempt. Before effects it
  either revalidates that complete authority, or proves that every
  publication since its capture left unchanged everything the write read.
  That is the accepted schema, the branch incarnation, the identity of
  every table it touched, and the rows it read; only inserts of other keys
  and commits that changed no row may have followed. A write so proven
  publishes on the current authority, which its publication's
  compare-and-swap names. A retry starts a new attempt; it never combines
  fresh and stale facts beyond what that proof covers."

  `AGENTS.md`'s working-memory line 2 changes to match: "A query or write
  attempt uses one coherent accepted snapshot; a write may publish on a
  later one only by proving everything it read unchanged. A retry starts
  fresh rather than mixing old and new authority."
- **4, a mutation publishes once.** Each write is in exactly one
  publication, and its statements stay together. A publication may hold
  several writes of one actor. The text needs no change.
- **5, crash convergence.** Unchanged: detached commits follow admission,
  carry the batch's witness and stay unreachable until the publication.
- **7, physical acceleration is derived state.** A composed append leaves
  an unindexed tail like any append. Known keys and the index warm-up are
  caches.
- **8, integrity failures are loud.** Every constraint an entry validated
  holds at publication, by admission. A constraint admission cannot carry
  forward makes the write ineligible.
- **10, policy.** Enforced per write at its `_as` entry point, before it
  queues.
- **11, bounded and observable.** Queue, batch, log, horizon and known keys
  are bounded (§Bounds). Refusals go through the existing bounded loop and
  are counted as re-preparations; batch size, known-key probes and misses
  are counted.
- **12, one source of truth.** The effect log and the known keys cache
  immutable facts. The queue holds requests, not state derivable from the
  manifest.
- **Deny-list.** The write group is process-local and is not presented as
  fencing; the compare-and-swap and the writer epoch remain the
  cross-process fences. There is no job queue for manifest-derived work, no
  silent retry (a send-back is counted and bounded and reruns the request)
  and no acknowledgement before durable visibility.

## Compatibility and reversibility

- **Storage:** none. Commit, head and registration rows are unchanged, and
  an older binary reads a batch commit as an ordinary commit.
- **Wire and API:** none.
- **Behavior:** the commit grain under concurrent writes by one actor, a
  user-visible change documented with the rollout step that enables it.
- **Reversal:** `group_commit = off` restores today's path. The known keys
  leave no durable trace. Reverting either change leaves no residue.

## Alternatives

| Alternative | Why not |
|---|---|
| Do nothing, or only shave round trips | The ordinary path with known keys and a listing-free publisher stays at one write per publication: 2.45 and 2.65 writes/s at 16 and 64 writers on RustFS (#1401 §5.9). #1400 removed round trips, carried inserts over inserts and pipelined publication without batching, and reached 9.6 writes/s at 16 writers: two serial chains bound it |
| N commit rows in one publication (RFC 0067's sketch) | Breaks every mapping from a commit to its `__manifest` version: snapshot reads, diffs and the change feed's head checks, merge bases and collector roots through `pinned_graph_commit`, parent resolution, `commit::overwrite`'s head rows, the lost-acknowledgement read-back and `ExactGraphHead` (#785's survey) |
| Admission by disjoint tables | Batches nothing on the measured workload |
| Rebase each write over the intervening inserts instead of re-preparing (#1401 §5.1; #1400 A) | Walks one link per intervening commit under the branch gate and still pays a gated commit per write. On RustFS, 16 writers fell from 2.15 to 0.90 writes/s and 64 from 2.35 to 0.85. #1400's in-process link log reached 3.0 |
| Chain same-table writes on each other's detached versions | Serializes preparation per table, and refusing one link refuses its successors |
| A leader linger or timed window (#1401 §5.4) | 39.75 to 36.0 writes/s at 10 and 25 ms. Writers settle into two cohorts that alternate, because a cohort prepares in about 67 ms while the other's batch takes about 200 ms. A timer adds latency at low load |
| Pipelined batches (#1401 §5.5; #1399; #1400 C) | +70% publications/s at 16 writers on RustFS (4.85 to 8.25) with writes/s unchanged: closed-loop throughput follows per-write latency, and a batch still takes about 230 ms. Stage A's detached commits carry the predecessor's parent head, which the collector would judge dead, and #1400 needs invariant 3 reworded for predicted state. Future work |
| Probe-free capture for every writer (#1401 §5.7; #1400 B) | Safe for the compare-and-swap but not for a write's meaning. A query compiled against a catalog another handle replaced failed terminally; a recreated branch's first attempt bound the dead incarnation; a vanished `__manifest` lost its before-effect evidence. Withdrawn; the supported form is #1350's holder skip |
| Bloom filters as the admission test | False positives refused entries and cascaded (§The effect log) |
| The leader stages each table's write for the batch (#1399) | One probe and one fragment per table per batch. At 64 writers on a table without an id index it lifted 58.3 to 147.2 writes/s, but it moves the probe and the data put into the serialized path: 4 writers fell from 12.0 to 9.2 and 16 from 46.4 to 36.8. A strict insert's `KeyConflict` would need attributing to one member. Known keys remove the per-write probe without that; leader staging stays a candidate for fragment growth past about 250 writes/s (#1401 §5.10) |
| A dedicated publisher task | A child producer no owned operation owns, invisible to `--measure` and `.gqt` `order:` (#785) |
| A record log in front of `__manifest` (#1399) | One conditional put per publication and +32% at 64 writers in its setting, but it rewords invariants 2, 5 and 12 and overlaps RFC 0068. Future work, in its own RFC |
| RFC 0068's commit record | Lowers the per-publication cost and composes with this RFC: one record per batch |
| Batches across actors | Needs per-write attribution in the commit, a format change that belongs with per-write idempotency (#513) |

## Evidence and tests

### Measured

The three research drafts used different instruments and backends, so each
is reported on its own. All are closed-loop, so they are decision evidence,
not claims (RFC 0039, Rule 1). Every cell verified exact row counts on a
fresh handle afterwards.

**#1401: RustFS and local disk.**

- Backend: RustFS 1.0.0-beta.12 behind toxiproxy, 15 ms each way (about
  32 ms per request), and a local tempdir.
- Build: every crate at `opt-level=2`.
- Load: N writers over one `Arc<Omnigraph>`, one row per write into
  `Item { slug: String @key, n: I64 }`, 64 seed rows with indexes built.
- Timing: 3 s warm-up and a 20 s window, medians of 3 interleaved runs.
- Columns: the baseline is `ed09ee63`. The ordinary path is the research
  binary with batching off; it runs known keys and the publisher's
  listing-free first attempt (#837). Group commit adds batching, the
  leader's capture from its own publication, the index warm-up and the
  named-branch warm state (`results/final-best-matrix.md`).

| One branch | Baseline writes/s (p99) | Ordinary path | Group commit (graph commits/s) |
|---|---:|---:|---:|
| RustFS, 16 writers | 2.20 (7,519 ms) | 2.45 (6,692 ms) | **44.85** (367 ms; 5.65) |
| RustFS, 64 writers | 2.35 (22,663 ms) | 2.65 (22,873 ms) | **153.54** (504 ms; 4.75) |
| local, 16 writers | 21.10 (1,061 ms) | 47.80 (536 ms) | **203.19** (142 ms; 25.8) |
| local, 64 writers | 22.30 (3,878 ms) | 49.25 (1,799 ms) | **247.49** (534 ms; 8.0) |

On RustFS, requests per write fell from 24.2 to 3.8 at 16 writers and from
22.2 to 3.2 at 64; re-preparations fell from about 1.0 per write to 0.
Starvation is gone: the longest service time including the drain fell from
7,944 to 554 ms at 16 writers and from 32,665 to 601 ms at 64.

A batch of one costs no more than today. One writer on RustFS ran 4.15
writes/s with batching on, 3.70 on the ordinary path and 2.90 on the
baseline binary (single runs).

Past 64 writers, RustFS reached 208.05 writes/s at 128 and 244.53 at 256,
flattening near 250 as a batch's own work grows with its size: one
fragment per write.

Separate branches gained 2.2x on RustFS at 64 writers (34.20 to 73.90).
With one writer per branch every batch holds one write, so that gain came
from warm named-branch writer state (§5.6), which the per-branch
writer-state cache covers, not from batching.

**#1401: known keys off and on, one binary, batching on.**
`results/known-keys-ab.md`. Local off-side runs are medians of 2; the rest
are single runs.

| One branch | writes/s | p99 | requests/write | writes/publication |
|---|---|---|---|---|
| local, 16 writers | 69.95 → 191.99 | 489 → 159 ms | 4.2 → 2.3 | 1.9 → 7.9 |
| local, 64 writers | 65.45 → 249.32 | 2,688 → 510 ms | 4.1 → 2.1 | 2.1 → 30.0 |
| RustFS, 16 writers | 38.95 → 44.85 | 578 → 371 ms | 11.0 → 3.8 | 7.1 → 7.9 |
| RustFS, 64 writers | 68.60 → 152.74 | 2,012 → 485 ms | 7.7 → 3.2 | 16.0 → 32.2 |

**#1399: an in-memory store with injected delay.**

- Backend: Lance's `shared-memory` object store behind the engine's
  object-store seam, with 20 ms injected per request and the S3
  conditional-put commit handler.
- Build: workspace crates at `opt-level=2`.
- Load: one `Session`, the writer epoch claimed, and a table with no id
  index.
- Timing: 2 s warm-up and a 10 s window.

| One branch | Baseline writes/s (p99) | Group commit, writers' staged files composed | Leader stages the batch |
|---|---:|---:|---:|
| 16 writers | 1.7 (5,214 ms) | 46.4 (512 ms) | 36.8 (422 ms) |
| 64 writers | none in 10 s | 58.3 (1,799 ms) | 147.2 (422 ms) |

At 64 writers the composed form was CPU-bound in the writers' probes, the
limit known keys address on indexed tables. Over longer windows, the
report's full configuration fell from 351.5 writes/s at 10 s to 234.0 at
60 s as unindexed fragments accumulated. With an index build every 5 s,
only 1 of 13 builds landed (§Interactions). Its fault test (2% and 10% of
conditional creates failing) and its mixed upsert and update test kept
every acknowledged write exactly once.

**#1400 and the #785 spike.**

- #1400 used RustFS at +30 ms with its own instrument: 15 s window and 32
  history commits. Without batching, carry-over, a trusted in-process tip,
  the warm-up and pipelined publication took one branch from 2.13 to 9.60
  writes/s at 16 writers. Its report names batching as the way past that.
- #785's spike used the deferred `concurrent-writes` bench at `97f293e8` on
  RustFS at +30 ms. It took 8 writers from 0.87 to 9.23 writes/s, with
  per-writer max/min from 26 to 1.0.

**What did not work:** rebasing each write, a linger, pipelining,
probe-free capture and Bloom-filter admission; each is under Alternatives
with its numbers.

### The carried-over guard

`lance_surface_guards.rs::group_commit_composes_inserts_staged_against_older_pins_onto_a_later_pin`
is the Lance half of the composition guard. It passes on Lance 13.0.0 at
`main` `2c0941e0`, as does the whole binary. The test:

- stages two keyed upserts of new keys against P0 and one against P1, where
  P1 is a third insert committed detached from P0;
- commits the three as one detached transaction from P1;
- pins that the raw fragments carry id 0 and an empty row-id sequence;
- pins that the key filters OR exactly;
- pins that the result is the union of rows with P1's rows unchanged, and
  that fragment and row ids are fresh and unique;
- pins that the composed rows share one stamp, and that the version reopens
  by id.

### Owners and new evidence

- **Known keys:** in-source `table_store` tests compare the map's answer
  with the scan's over deletion files, a column patch, compaction, an
  unknown file and eviction. `integration/write_cost.rs` asserts that a
  keyed insert's requests stay flat as the unindexed tail grows. The DST
  storage-cost golden records that insert at two tail lengths under two
  labels.
- **Composition, engine half:** `integration/changes.rs` (the feed's fast
  path over a composed commit) and the merge owners (the pure-insert proof).
  `lance_surface_guards.rs` adds the Blob v2 case and, for the maintenance
  carry, `TransactionRebase` of `CreateIndex` and `Rewrite` over an
  insert-only `Update`.
- **The admission matrix.** Outcomes go in `.gqt` cases with a
  `--- concurrent` block, the default for behavior visible in outcomes.
  Commit counts and which entry was sent back go in `writes.rs`, because
  `.gqt` cannot express them. The cells:
  - disjoint keys make one commit, for strict inserts and keyed inserts;
  - the same key: the later entry is sent back, then gets `KeyConflict` or
    becomes an update;
  - an insert beside an update on the same table;
  - an edge insert beside a delete of its endpoint;
  - non-key `@unique` and bounded `@card` stay on today's path;
  - write skew through a predicate scan;
  - the `Left`/`Right` counterexample: one publishes, the other is sent
    back and reruns, and the rows are (1, 0) or (0, 1), never (0, 0);
  - an expected head publishes alone, including the first write on a fresh
    named branch;
  - two actors make two commits;
  - a publication by `optimize`, a merge or another handle between capture
    and batch;
  - `group_commit = off` matches today.

  Which request a `.gqt` session can park at after preparation is still
  open (#785): a fragment write that runs on Lance's pool cannot be named.
- **Failure windows:** `failpoints.rs` covers the new seams, a refused
  compare-and-swap, the in-doubt read-back, a dropped leader before and
  after the first effect, and both `issue_1331` gated-retry cases with
  batching on. `detached_commit_matrix.rs` gains the grouped-insert writer
  and a carried index build.
- **DST:** the concurrent universe (`crates/omnigraph-dst/src/concurrent.rs`;
  `dst_concurrent_*`) with batching on and the collector oracle; the oracle
  checks every acknowledged write visible once and one actor per commit.
  The storage-cost golden gains a batch row.
- **Cost:** `write_cost.rs` asserts that a batch of one issues no more
  requests than today's write.
- **Instrument:** #1401's `concurrent_write_throughput.rs` lands at step 5
  as an `instrument:` ignored test. It owns the soak and the instrument gate
  until #895's executor can measure overlap open-loop.

## Rollout

Each step leaves `main` shippable and lands with its owner tests. The
per-branch writer-state cache lands separately, as a bug fix, before
batches on named branches are measured.

1. **Known keys**, with no behavior change. Owners: `table_store` in-source
   tests, `write_cost.rs`, the DST cost golden, and `forbidden_apis.rs`
   (only the absence-proven writer records).
2. **The footprint on the staged mutation**, with no behavior change:
   - carry `op_kinds` through staging, and rewrite its doc comment, which
     names a drift check `commit_all` no longer runs, to name the
     executed-operation class, with `strict_pre_stage_version_check` as its
     one owner (#785 review);
   - record validation reads;
   - give `StagedWrite` the exact inserted ids its dangling doc comment
     describes;
   - make eligibility one function.

   Owners: in-source staging tests and `writes.rs`.
3. **Composition:** the sealed adapter, the engine half of the guard, and
   the Blob case. Owners: `lance_surface_guards.rs`, `changes.rs`, the
   merge owners and `forbidden_apis.rs`.
4. **The maintenance carry** for index builds and `optimize`'s direct
   path. It is useful without batching. Owners: `lance_surface_guards.rs`,
   `failpoints.rs`, `detached_commit_matrix.rs` and the DST cost golden.
5. **The write group, with batches of one** and behavior unchanged: the
   queue, the lead handoff, the log of every publication, the gated second
   attempt, the seams, cancellation, per-member evidence, the
   `group_commit` setting (default `off`) and the instrument. Owners:
   `write_cost.rs`, `failpoints.rs`, `detached_commit_matrix.rs`, DST, and
   the GQT corpus with the setting on.
6. **Batching:** admission and composition in the batch, with the
   setting still off by default. The invariant 3 text, `AGENTS.md` and
   `docs/dev/writes.md` change here. Owners: the admission matrix,
   `failpoints.rs`, DST, and the RustFS and Azurite suites.
7. **Default on,** once `blocked_on` is clear: the soak and the instrument
   gate, the user docs, the skill and the release note.

`implementation` moves to `in-progress` at step 1 and to `complete` at
step 7.

## Out of scope and future work

- **A record log in front of `__manifest`, and pipelined publication**
  (#1399, #1400). They need their own RFC, reconciled with RFC 0068. They
  change the wording of invariant 2 (the record or the version is the
  door), invariant 3 (staging against predicted state), invariant 12 (a
  bounded tail where versions live) and invariant 5 (a fold after the
  publication). The collector witness must also name the head a pipelined
  batch publishes on.
- **Lance upstream asks.**
  - A public switch to record a transaction inline only.
    `FLAG_DISABLE_TRANSACTION_FILE` is in the format, but Lance 13 has no
    public way to set it. This would cut a publication from five serial
    puts to three. The engine would then match a pin by the inline
    transaction's uuid instead of the `.txn` name.
  - Cache population after a detached commit, as `record_successful_commit`
    does for a linear one.
  - Lance 14's `CompositeOperation`, whose actions include
    `AssertUniqueKeys`. It could carry several writers' inserts with the
    uniqueness check in the substrate. Readers before 14 fail on it, so it
    waits for that bump and a format review.
- **Broader batching:** updates and deletes by recorded footprint, Append
  and Merge loads with exact ids, admission across foreign commits by
  reading their change sets, per-write attribution for cross-actor batches
  (#513), and leader staging against fragment growth.

## Unresolved questions

- **Loads.** Whether Append and Merge loads staged by the absence-proven
  writer join at step 6 or later. Recommended: later, after the instrument
  has measured mutations.
- **The default for embedded and CLI handles.** Whether `group_commit`
  defaults to `on` everywhere at step 7 or only for the server.
  Recommended: everywhere, since a lone writer forms batches of one, which
  cost no more.
- **The soak thresholds** in `blocked_on`, which are proposals, are
  confirmed or changed by the maintainers before acceptance.

## Decision log

- 2026-11-14: Drafted from PR #785 and the research drafts #1399, #1400 and
  #1401, superseding #785's draft.
  - **Kept from #785:** one graph commit per batch; key-grain composition of
    same-table inserts from raw staged fragments; detached commits after
    admission; a write group per root and branch, not a spawned task, and
    not per handle as #1401's prototype had it; one actor per batch;
    conditional writes alone; classification by executed operations.
  - **The #785 reviews.** The 2026-10-05 counterexample stays an
    eligibility rule and a matrix cell. The 2026-11-08 optional finding
    (rewrite the `op_kinds` doc comment) is rollout step 2. The 2026-11-08
    correction (an update sent back returns `ReadSetChanged`) matched the
    contract of its day. Since #1349 a sent-back update reruns, so #785's
    original expected rows, (1, 0) or (0, 1), hold again.
  - **Changed from #785:**
    - step one batches insert-only entries only; batching updates and
      deletes by table footprint is future work, since nothing measured
      needs it and its read bookkeeping carries the most risk;
    - exact id sets decide admission (#1401's Bloom cascade);
    - the effect log records every publication of the process on the
      branch;
    - refused entries keep #1349's gated second attempt, which #1401's
      prototype gave up;
    - row-preserving publications are explained by #1349's proof;
    - the gate's instrument is an `instrument:` test, because
      `concurrent-writes` is deferred (#895).
  - **Added:** known keys (#1401 §5.8) and the maintenance carry (#1399
    §5.3).
  - **Out of scope:** the record log, pipelining and the upstream asks,
    recorded as future work.
  - **Carried over:** #785's Lance composition guard, unchanged except for
    the RFC path in its comment.
