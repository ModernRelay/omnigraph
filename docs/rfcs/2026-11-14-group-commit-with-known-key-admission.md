---
rfc: "2026-11-14-group-commit-with-known-key-admission"
title: "Group commit with known-key admission"
track: maintainer
status: draft
implementation: not-started
authors:
  - ragnorc
created: 2026-11-14
updated: 2026-11-15
discussion: "https://github.com/ModernRelay/omnigraph/pull/1405"
supersedes: []
superseded_by: []
blocked_on:
  - "Composition guard, engine half: a composed commit keeps only the markers every component carries, the change feed's one-transaction fast path and the merge's pure-insert proof accept it, and `lance_surface_guards.rs` pins a Blob v2 column through composition. The Lance half passes on Lance 13.0.0 (Part 3, The carried-over guard)."
  - "Batch-path seams exist before step 6, so that `failpoints.rs`, `detached_commit_matrix.rs` and DST can reach the batch path."
  - "Admission and failure evidence: the admission matrix, including its `@unique` and `@card` cells, the batch-path windows in `failpoints.rs` and `detached_commit_matrix.rs`, and the DST concurrent universe with batching on (every acknowledged write visible exactly once, one actor per commit, the collector oracle with batches in flight)."
  - "Batching forced on across the existing gates: the GQT corpus, the engine `integration`, `writes` and `failpoints` binaries, DST, and the RustFS and Azurite suites."
  - "Server maintenance admission: a served `optimize` is admitted while 64 or more writes are in flight. Today `DEFAULT_WRITE_OBSERVERS` (64, no override) refuses it with 429. This is a separate server fix that lands before the soak."
  - "Soak: 64 writers on one branch, RustFS behind toxiproxy at +30 ms, 60 minutes, a served `optimize` every minute and an embedded `index build` from a second process every minute. Every acknowledged write verified, every maintenance run published, writes/s in the last 5 minutes at least 80% of the first 5, peak RSS at 60 minutes at most 1.25x the 30-minute mark, and the known keys and the effect log within their byte budgets."
  - "Instrument gate on the production build, same backend. Pure inserts: at 16 and 64 writers, at least 10x the baseline writes/s with p99 under 1 s. Mixed (70% inserts, 15% key-addressed updates and deletes on the same table): at least 2.5x the ordinary path at 16 and 64 writers. One writer within 10% of the ordinary path. Per-writer counts within 2x of each other (#784). With no maintenance, 64 writers for 5 minutes on local disk never fall to 0 writes/s over a 10 s window."
---

# RFC: Group commit with known-key admission

This RFC has three parts.

- Part 1 is for reviewers. It explains the problem, the change, why the
  change is safe, what it costs, and the decisions that reviewers must make.
  It needs no earlier context.
- Part 2 is for the engineer or agent who implements the change. It gives the
  exact rules, the code they touch, the defects the prototype had, and the
  tests and done criteria for each step.
- Part 3 holds the measurements, the alternatives, future work and the
  history.

Part 1 states each rule in plain terms. Part 2 is the authority when the two
differ in detail.

Depends on: [RFC 0067](0067-detached-table-commits.md),
[Detached-only tables](2026-09-21-detached-only-tables.md),
[Writes carried over row-preserving publications](2026-11-10-writes-carried-over-row-preserving-publications.md),
[Writer epoch fence on the branch manifest](2026-11-09-writer-epoch-fence.md).
Supersedes: the group-commit draft of PR #785
(`2026-09-28-group-commit.md`, never merged, so no metadata names it), and
the research drafts #1399, #1400 and #1401.

## Part 1: For reviewers

### The problem

One branch accepts about two writes per second on object storage, whatever
the number of clients that write to it.

A write becomes visible when Omnigraph publishes a new version of the branch.
A publication is a compare-and-swap on the branch's `__manifest` (an atomic
"replace version N with version N+1, but only if N is still the newest").
Only one publication on a branch can succeed at a time. Each publication
holds the branch's lock (the branch gate) for six serial storage requests.
Every other writer that prepared against the old version must then prepare
again.

The result, measured on RustFS with about 32 ms per request:

| Writers on one branch | Writes/s | Slowest 1% wait (p99) |
|---|---:|---:|
| 16 | 2.2 | 7.5 s |
| 64 | 2.4 | 22.7 s |

On local disk the same limit is about 22 writes/s. More writers do not help,
and they make each write wait longer. Workloads that write many small changes
to one branch at the same time hit this limit: ingestion, agents that write
memory, and sync jobs.

### The change

Many writes share one publication. This is group commit (one durable commit
for many writes), which PostgreSQL, MySQL and RocksDB also use.

1. Writers work in parallel as today. Each one reads, checks its rows, and
   writes its data files.
2. Each writer then joins a queue for its branch, the write group.
3. If no publication is in progress, that writer becomes the leader.
4. The leader takes the branch gate and checks each queued write (admission,
   explained below).
5. The leader combines the data of the admitted writes into one commit per
   table, and publishes once.
6. Every admitted write gets that one commit as its receipt.

Writes that arrive during a publication form the next batch. There is no
timer, so a writer that is alone is never delayed.

Batching moves the limit to a second place: the check that a new key does not
exist yet. That check reads every data file the id index does not cover yet,
and each write adds one such file. So the check gets slower with each write.
The second part of this RFC is the known keys. This is an in-memory record of
the keys that this process wrote into each data file. A data file never
changes after it is written, so the record stays correct for that file. The
check then reads memory instead of storage for those files.

### Which writes join a batch

In the first version, only inserts of new rows join a batch. These writes
publish alone, as today:

- Updates and deletes.
- Writes that name an expected head (`--if-commit`).
- Writes to a table with a Blob column, until a test covers it.
- A second attempt of a write that was sent back.

Constraints do not exclude a write. Every check that the validator runs reads
a known set of keys, and admission compares those keys (see below). For
example, an insert into a type with `@unique(email)` reads the email values
it adds. Two inserts with the same email cannot share a batch, and two with
different emails can.

A later step (step 7) can batch updates and deletes that name their rows by
key. It lands only if its own measurement passes. Updates and deletes that
select rows by another condition stay on today's path.

### Why the change is safe

For each queued write, the leader must answer one question. Does the write
give the same result on the newest version as on the version it read? The
leader compares two sets to answer it.

- The footprint of the write is the set of keys it read and wrote. A key is
  a value that one check compares:
  - a row's id, which every insert reads (it proves the id absent);
  - the values of a `@unique` column group, which the `@unique` check reads;
  - an edge's endpoint node ids, which the endpoint check reads;
  - an edge's source, which a `@card` check reads, because it counts the
    edges of each source.
- The effect log is an in-memory list of what each recent publication on the
  branch changed, by exact key.

The leader admits a write only if no publication since the write's read
touched a key in its footprint. The leader also refuses a write if an earlier
write in the same batch touches one of its keys. So the writes in a batch do
not touch each other, and every order of them gives the same final state. A
batch is therefore the same as running its writes one after another on the
newest version. This is the standard proof for optimistic validation (check
at commit time what a transaction read). FoundationDB uses the same proof.

If the leader cannot prove the write safe, it refuses it. A refused write
prepares again under the branch gate and publishes alone, as today. It still
finishes within two attempts.

Four structural rules make the proof hold by construction, not by the care
of each caller:

1. Only the engine's one publication path writes the effect log. A
   publication cannot forget to record itself.
2. Each record's content comes from the staged data, not from what a caller
   claims. If the exact keys are not known, the record says "changed". That
   refuses more writes than needed, but it never admits a wrong one.
3. The log does not reason across a gap. If a record does not continue from
   the record before it, the log starts again. Admission then never trusts a
   publication it did not see, for example one from another process.
4. The footprint comes from the validator's own list of checks. Each kind of
   check states the keys it reads, in one place that the compiler checks for
   completeness. A new kind of check cannot join a batch until someone states
   its keys.

The prototype broke each of these rules under stress, and the result was
duplicate keys and edges to deleted nodes. Part 2 lists every defect it had.

Nothing is acknowledged before its publication is durable and visible. If a
publication fails, every write in it gets the same typed error. The change
adds no durable state, so there is nothing new to recover. The garbage
collector does not change.

### What users see

- Acknowledgement does not change. A write returns only after its data is
  durable and visible on its branch.
- Concurrent writes by one actor on one branch can share one graph commit.
  Each receipt names that commit. `commit list`, the change feed and the diff
  show one commit that holds all of the writes. Each write still reports its
  own affected counts.
- Two writes that one client sends one after the other never share a commit,
  because the second starts after the first returns.
- A commit holds the writes of one actor only.
- A batch makes one commit, whose parent is the head the batch built on. The
  states between its writes were never a branch head, so no one can read
  them.
- A new session setting, `group_commit`, is `off` or `on`. It stays `off`
  until the gates in `blocked_on` pass.
- No storage format, wire format or API changes. An older binary reads a
  batch commit as an ordinary commit.

### What it does not fix

- The floor stays. Each publication still needs one compare-and-swap, one
  after another on a branch. A record log (a write-ahead log in front of
  `__manifest`) removes that floor. It changes more invariants, so it needs
  its own RFC.
- Small files. Each write still adds one fragment (a small set of rows in its
  own data file). Batching raises the write rate, so fragments grow faster.
  Without maintenance, 48 writers on local disk fell from 649 to 24 writes/s
  in 40 s. So batching requires regular `optimize`. Two fixes must land
  first:
  - Maintenance must be able to publish while batches publish. Today 0 of 16
    direct `optimize` and index builds landed under batching.
  - The server must accept an `optimize` while 64 writes are in flight.
    Today it refuses with 429.
- Mixed workloads gain less. With 15% updates and deletes on the same table,
  the rate went from 3.0 to 9.0 writes/s, not to 100.
- Edges under a bounded `@card` from one busy node serialize. Each such edge
  reads the count of its source, so two of them from the same source never
  share a batch. Edges from different sources still batch.
- The gain is in one process. Another process's writes are not in the log,
  so the leader refuses the writes they can affect. One writer process per
  cluster is the supported topology today.

### A change to invariant 3

Invariant 3 says that a writer revalidates its complete captured authority
before it has effects. An admitted write instead proves that nothing it read
changed. So the text must change when step 6 lands.

Today: "A writer captures schema, catalog, branch, graph head, native ref
identity, and table baselines from one attempt, then revalidates that
complete authority before effects. A retry starts a new attempt; it never
combines fresh and stale facts."

Proposed: "A writer captures schema, catalog, branch, graph head, native ref
identity, and table baselines from one attempt. Before effects it either
revalidates that complete authority, or proves that every publication since
its capture left unchanged everything the write read. That is the accepted
schema, the branch incarnation, the identity of every table it touched, and
the rows it read; only writes of other keys and commits that changed no row
may have followed. A write so proven publishes on the current authority,
which its publication's compare-and-swap names. A retry starts a new attempt;
it never combines fresh and stale facts beyond what that proof covers."

### Results so far

These come from prototypes, so they support the decision but do not prove
the final code. All numbers are for one branch.

| Setup | Today | Group commit |
|---|---:|---:|
| RustFS, 16 writers | 2.2 writes/s, p99 7.5 s | 44.9 writes/s, p99 0.37 s |
| RustFS, 64 writers | 2.4 writes/s, p99 22.7 s | 153.5 writes/s, p99 0.50 s |
| Local disk, 64 writers | 22.3 writes/s | 247.5 writes/s |
| RustFS, 1 writer | 2.9 writes/s | 4.2 writes/s |
| RustFS, 48 writers, mixed workload | 3.0 writes/s | 9.0 writes/s |
| RustFS, 56 writers for 1 hour, `optimize` every minute | | About 100 writes/s, all 362,706 writes verified |

A stress study of the prototype on release builds found eleven defects. Five
of them can lose data or stop progress under load. Each one became a rule
in this revision. After the fixes, no acknowledged write was lost or
duplicated under crashes, network faults, cancellation, several actors and
processes, and maintenance.

### Rollout

Each step leaves `main` shippable and adds its tests.

1. Known keys, with no behavior change.
2. The footprint recorded on each staged write, with no behavior change.
3. Composition of several inserts into one table commit, with its Lance
   guard.
4. Maintenance that can land while writes publish.
5. The write group with batches of one, with behavior unchanged.
6. Batching of inserts, behind the setting.
7. Batching of key-addressed updates and deletes, only if its measurement
   passes.
8. Default on, after the soak and the gates.

### Decisions for reviewers

- Do you accept the reworded invariant 3?
- Do you accept the commit grain change: concurrent writes of one actor can
  share a commit?
- Do you accept that batching requires a maintenance cadence, written in the
  user docs?
- Do you accept the footprint taken from the validator's checks, so that
  `@unique` and `@card` types batch? This is not measured yet. The stress
  study kept its oracle for `@unique` but did not batch those types.
- At step 8, does `group_commit` default to `on` everywhere, or only on the
  server? The recommendation is everywhere, because a writer that is alone
  forms a batch of one, which costs no more.
- Do loads join batches at step 6 or later? The recommendation is later.
- Is the leader to write one data file per table per batch, to slow fragment
  growth? The recommendation is to decide from the step 6 measurement.
- Are the soak and instrument thresholds in `blocked_on` correct?

### Alternatives in brief

Part 3 gives the numbers for each.

- Fewer round trips without batching: still one write per publication.
- Several commit rows in one publication: breaks every mapping from a commit
  to a `__manifest` version.
- Admission by table, not by key: batches nothing, because writers usually
  insert into the same table.
- Exclude `@unique` and `@card` types by name: a hand-kept list lets a new
  kind of check into batches unproven.
- A timer that waits for more writers: slower, and adds latency.
- Pipelined batches: more publications, but not more writes per second.
- Bloom filters to compare keys: false matches refuse writes, and the
  refusals cascade.
- A record log: the next step, in its own RFC.

## Part 2: For implementers

### Before you start

Read these first:

- `AGENTS.md`, [invariants](../dev/invariants.md) (2, 3, 4, 5, 7, 8, 11 and
  12 apply here), [writes](../dev/writes.md) (the writer epoch and the
  carry), [recovery](../dev/recovery.md) and [testing](../dev/testing.md).
- The full Lance 13 pages for transactions, table versioning, branches and
  tags, distributed write, row-ID lineage, object stores and Blob v2
  ([lance.md](../dev/lance.md)). Also the Lance source for
  `io/commit.rs` (`do_commit_detached_transaction`), `lance-table`
  `transaction/manifest_build.rs` and `transaction/row_version.rs`.
- The four RFCs under Depends on.
- The stress report, `research/group-commit-stress/REPORT.md` on fork branch
  `research/group-commit-stress` at `155911f0`, and the defect table below.

The prototypes are evidence, not code to port. They are the #1401
`db/group_commit.rs` and `table_store::known_keys`, the #1399 and #1400
prototypes, and the stress branch. Each defect below came from that code.

Three fixes are outside this RFC and must land on their own:

- The per-branch writer-state cache (#1407): one authority-coordinator slot
  per branch instead of the shared `merge_authority_cache`. It lands before
  batches on named branches are measured.
- Server maintenance admission (O1): `DEFAULT_WRITE_OBSERVERS`
  (`crates/omnigraph-server/src/operations.rs`) counts every recorded
  operation, so a served `optimize` gets 429 with 64 writes in flight.
- If the publisher's listing-free first attempt (#837) lands, its epoch
  assertion on an image it did not verify must compare without side effects
  and send a mismatch to a fresh load (H12). In the prototype, it cleared the
  live holder, and later writes got `WriterFenced` with batching off too.

Code locations are on `main` at `4f7c2076`. Search by name if a line moved.

### Terms

| Term | Meaning | Code |
|---|---|---|
| Publication | One compare-and-swap of a branch's `__manifest`, which makes one graph commit visible | `omnigraph-catalog` publisher |
| Publication door | The engine's one path to the publisher. Every mutation, load, merge, `optimize`, index build, schema apply, collector run and system-column upgrade goes through it | `GraphCoordinator` commit methods, `crates/omnigraph/src/db/graph_coordinator.rs` |
| Gates | The shared schema permit and the branch gate, which every publisher in the process takes | `WriteQueueManager`, `crates/omnigraph/src/db/write_queue.rs` |
| Capture | The authority a write reads at the start of its attempt: schema, catalog, branch, graph head, native refs, table baselines | |
| `He` | The graph head an entry captured | |
| `Hc` | The graph head the batch captures under the gates | |
| Entry | One prepared write in the queue: its staged effects, `op_kinds`, footprint, actor and capture | |
| Write group | The queue and leader flag for one graph root and branch, shared by every handle of the root in the process | |
| Batch | The entries one leader admits and publishes in one publication | |
| Sent back | Refused at admission or by a lost compare-and-swap. The write prepares again under the gates | `PreEffectReprepares`, `crates/omnigraph/src/db/reprepare.rs` |
| Key space | The values that one check compares in one table: the table's ids, the tuples of one `@unique` column group, or an edge table's `src` or `dst` values | |
| Footprint | Per key space, the keys an entry read and the keys it added or removed | Derived from `ValidateCheck` (`omnigraph-compiler` `src/ir/load.rs`) |
| Effect log | Per write group, the in-memory records of recent publications on the branch | |
| Record | One publication in the log: parent head, produced head, and one class per touched table, with keys per key space | |
| Class | `inserted(ids)`, `touched(written ids)`, `row_preserving` or `changed` | `PublicationEffect` (new) |
| Known keys | Process-wide map from a data file's resolved location to the exact keys this process wrote into it | |
| Uncovered tail | The fragments the id index does not cover at a pinned version | |

### Rules that must hold

Each rule prevents a defect the stress study found (the H numbers are the
report's), or a hazard an earlier review found. Do not weaken a rule to make
a test pass.

1. Decide admission by exact id sets. Never compare Bloom filters. A false
   match refuses an entry, its re-preparation publishes, and every older
   entry is then refused in turn.
2. Only the publication door appends to the effect log. Callers do not
   append. `forbidden_apis.rs` pins this. (H13: `optimize` and index builds
   published unlogged, so every queued entry was refused after each run.)
3. Derive each record's class from the staged effect. Each class has one
   constructor that reads the staged effect. A constructor that cannot
   produce the complete id set, or exceeds the byte budget, returns
   `changed`.
4. An append whose parent is not the log's head resets the log to that one
   record. The admission walk follows links only, and then falls back to the
   durable proof. (H1: a walk across a gap hid a foreign insert of the same
   key. The result was duplicate keys, edges to deleted nodes, and false
   `omnigraph.insert_absence` certificates.)
5. The leader reads the effect log after it takes the gates, never before. A
   batch that admits nothing leaves the log unchanged. (H14: a copy taken
   before the gates refused everything and then cleared the live log. On the
   S3 soak the rate fell from 122 to 2.35 writes/s and did not recover.)
6. Keep one write group per graph root and branch, in `WriteQueueManager`,
   shared by every handle. Do not key it by actor or by handle. A batch holds
   one actor. (H2: groups keyed by actor made one actor's batches foreign to
   another's.)
7. The footprint of an edge entry holds presence reads of the endpoint ids
   it checked, in the endpoint node tables' id spaces. (H10: an edge was
   admitted across a delete of its endpoint.)
8. A sent-back entry prepares again under the gates and publishes alone, as a
   batch of one, and its publication is logged. (H5: ungated re-preparation
   lost the two-attempt bound, and 5 to 7% of writes returned
   `ReadSetChanged` (HTTP 409) with two or more actors or handles.)
9. The group, not the leader's task, owns one typed outcome per batch. It
   delivers that outcome unchanged to every member, the leader included, with
   its kind, details, completion evidence and HTTP status. (H7: followers of
   an in-doubt batch got an internal error. H11: every error became
   `manifest_internal`, so `WriterFenced` and conflicts were lost and 409
   became 500.)
10. A drop guard on the lead answers every drained member and hands the lead
    on, if the leader's future is dropped at any point. A drop guard on each
    waiter passes on a lead it was sent but never took. (H3: a dropped leader
    hung its group forever.)
11. Remove entries whose callers are gone when the leader drains the queue,
    and again after it takes the gates. (H4: a cancelled write was
    published.)
12. Make detached commits only after admission, stamped with the batch's
    staging witness (the branch incarnation and `Hc`). The collector's
    `judge_staging` then proves a lost batch's staging dead.
13. Compose from the raw fragments in each staged write's transaction. Do not
    use `StagedWrite::new_fragments()`: that is the read-your-writes copy,
    whose ids come from the member's own base.
14. Use no timer and spawn no task. The leader publishes on its own task,
    inside its owned operation.
15. Acknowledge nothing before the publication is durable and visible. A lost
    acknowledgement counts only when the publisher's read-back finds the
    batch's commit.
16. Record known keys only from the absence-proven insert writer, and only
    when its staged write produced exactly one data file. Answer from the map
    only for a fragment with one data file, no deletion file and no overlay.
17. Derive the footprint from the checks the judge runs, never from a list of
    constraint kinds. Map each `ValidateCheck` to its key space through one
    `match` with no wildcard arm. A check with no key form makes the entry
    ineligible. `ValidateCheck` is not `#[non_exhaustive]`. Keep it so, and
    pin the missing wildcard arm in `forbidden_apis.rs`. (Hazard: the earlier
    revision excluded `@unique` and `@card` types by name, so a new kind of
    cross-row check would have batched unproven.)

### Eligibility

An entry joins a batch only when all of these are true. Every other write
takes today's path, which the branch gate serializes with batches.

1. It is a mutation, and every operation it executed on every table it opened
   is an insert. The recorded `MutationOpKind` (`crates/omnigraph/src/db/mod.rs`)
   is `Insert` or `Merge`, so `!strict_pre_stage_version_check()`.
   `ensure_path` (`crates/omnigraph/src/exec/staging.rs`) records the kind
   before it reads a row. So an update or delete that matched nothing still
   makes the entry ineligible. Today `stage_all_with_concurrency` drops
   `op_kinds`. The entry must carry them.
2. Every staged effect is an insert-only keyed `Update` from the
   absence-proven insert writer (`stage_absence_proven_strict_insert`,
   `crates/omnigraph/src/table_store.rs`), with its exact inserted ids. Such
   an effect has new fragments only, `RewriteRows` mode and the inserted-key
   filter. An `insert` into a `@key` type stages as an upsert
   (`PendingMode::Upsert`, `exec/staging.rs`). It takes this writer when none
   of its keys exists and the tail is small enough for the exact-id probe
   (`stage_keyed_write`, `crates/omnigraph/src/storage_layer.rs`). An upsert
   that matched a key is not insert-only.
3. Every check the judge runs on each table it writes has a key form
   (Footprint). Today only `RetainedEdges` has none, and only an overwrite
   runs it, which rule 1 already excludes.
4. No written table has a Blob column, until the composition guard covers one
   (step 3).
5. It has no caller head precondition, no unattached base, and no `as commit`
   or `as unattached branch`. A precondition scoped to what a write touches
   (#1397) can let such writes batch later.
6. It is a first attempt. A second attempt publishes alone (Progress).

### Footprint

The judge already plans each write as a typed list of checks per table:
`WriteTableIR.checks` from `lower_write_table`
(`omnigraph-compiler` `src/ir/write.rs`), planned by `plan_write`
(`crates/omnigraph/src/validate/plan.rs`). Each `ValidateCheck` names the
committed table it consults. The footprint is derived from that same list, so
the checks that decide an entry and the keys that admission compares have one
source.

A key space is the set of values one check compares in one table. A read is
one of two kinds:

- an absence read, which only an addition of that key can break;
- a presence or count read, which an addition or a removal of that key can
  break.

| `ValidateCheck` | Key space | What the entry reads | What the entry adds |
|---|---|---|---|
| `Value` | None. It reads only the entry's own rows | Nothing | Nothing |
| `Unique { role: Key }` | The table's ids. A `@key` group is id-backed, so the check reads only the delta (`unique_checks`, `ir/load.rs`). The absence read comes from the absence-proven writer (Eligibility rule 2), and every insert's footprint holds it | Absence of each id it inserts, from the writer | Those ids |
| `Unique { role: Unique, columns }` | The tuples of `columns` in the table. A tuple that holds a null is exempt from the check, so it is no key (`match_unique_holders`, `validate/kernels.rs`) | Absence of each tuple it inserts | Those tuples |
| `EdgeRi { endpoint, node_type }` | The ids of `node_type` | Presence of each endpoint id | Nothing |
| `Cardinality { endpoint, .. }` | The edge table's values of `endpoint`. The compiler emits it only for a non-default `@card`, and always for `src` today | The count of each endpoint value its edges carry | Those endpoint values |
| `SourceCardinality { edge_table, .. }` | `edge_table`'s `src` values | The count of each node id it inserts | Nothing |
| `RetainedEdges { .. }` | No key form | The entry is ineligible | |

An insert also adds its keys to every key space of its table, whether or not
it runs a check there: its ids, its tuples for each `@unique` group, and, for
an edge, its `src` and `dst` values. Another entry can read them.

The claim the table must keep true: the result of each check depends only on
the committed state of the keys it reads. A uniqueness check on tuple v
depends only on whether a committed row holds v. A cardinality check on
source s depends only on the number of committed edges whose source is s,
which changes only by an addition or a removal of s in that space. A new
`ValidateCheck` variant joins this table only with that argument written
down.

Known keys cover the id space only. A `@unique` check on other columns still
reads committed rows as it does today, so its cost does not change.

### The batch

A writer that submits while no batch is publishing leads. It publishes the
batch that holds its own entry, on its own task. Then it hands the lead to the
next waiting entry, or clears the flag when none waits. A batch takes, in
arrival order, the queued entries of its first entry's actor. Entries of
other actors keep their places, and the oldest of them leads next. A member
sent back takes the branch gate for its second attempt in its turn, so a
writer that just published cannot overtake the writers it made stale (#784).

The `group_commit` session setting is process-scoped, `off` or `on`. `off`
is today's path exactly. Known keys have no setting, because they change
cost and never a result.

The leader runs these steps.

1. Gates. Take the shared schema permit and the branch gate, as `commit_all`
   (`exec/staging.rs`) does today. Drop entries whose callers are gone. Read
   the effect log as it is now (rule 5).
2. Capture once. Read the current authority as revalidation reads it. On a
   held bound branch this reads nothing (#1350). On an unheld branch the
   leader can skip the probe when its handle's installed view is the head the
   log last recorded and the log explains every entry's capture. A stale
   capture loses the compare-and-swap and sends the members back. It cannot
   change a member's meaning, because each member was prepared at its own
   capture.
3. Admit (Admission), up to the caps (Bounds).
4. Compose and commit. For each table, compose the admitted entries
   (Composition). Commit the result detached through
   `commit_staged_detached`, stamped with the staging witness. Tables commit
   in parallel.
5. Publish through the unchanged publisher: one dataset update per table, the
   expected versions at `Hc` for every table in the admitted footprints, one
   lineage intent with the batch's actor, and `ExactGraphHead` on `Hc`.
6. Warm each new table version's index metadata into the shared Lance session
   while the publication is in flight. Lance 13's detached commit skips
   `record_successful_commit`. Without the warm-up, the next writer's
   validation reads it, one serial request per write. This is cache population
   only.
7. Complete. The door appended the batch's record. Each submitting handle
   installs the published snapshot before its receipt returns, so a read on
   that handle sees its write. Answer every member and hand the lead on.

Outcomes, one per batch, delivered to every member:

| Batch outcome | Each member |
|---|---|
| Refused at admission | Sent back. `ReadSetChanged` reaches the caller only when the retry bound is exhausted |
| Fails before the compare-and-swap with definite evidence (a detached commit, the lineage intent, a publisher read) | The batch's typed error with `CompletionEvidence::Unpublished`. The staging stays unreachable |
| Loses the compare-and-swap | Sent back |
| `WriterFenced` | `WriterFenced`, not retried. The process's holder for the branch is cleared |
| Publication in doubt | `manifest_publish_in_doubt`, naming the batch's attempted commit id, with uncertain evidence. A server closes admission once, as for one in-doubt write |
| The leader's future is dropped (an embedded caller) | Before the batch's first detached commit: sent back. After it: uncertain evidence. The next waiter leads |
| A follower is dropped while the lead is handed to it | The lead passes to the next waiter |
| A member's future is dropped before admission | It leaves the queue. Nothing of it is published |

An HTTP write is an owned operation, so a disconnect never drops its future
([Owned server operations](2026-09-30-owned-server-operations.md)). Only
embedded callers can drop.

### Admission

The leader admits entry e, in arrival order, when all of these hold. `He` and
`Hc` are materialized heads. A named branch before its first publication has
none.

1. Same authority. e's branch incarnation and accepted schema identity are
   the batch's. Every table e read or wrote has the same table identity,
   dataset location and native ref at `Hc`.
2. Explained interval. Walk back from `Hc` to `He`, within K publications.
   First follow the log's links from its head. Each record's parent is the
   head of the record before it. Never step to a record that does not link.
   Where the log ends before `He`, each remaining publication must have moved
   every table pin only through `Rewrite` and `CreateIndex` commits, with the
   same row count and registrations. That is the durable proof of
   `revalidate_write_txn_carrying` (`crates/omnigraph/src/db/omnigraph.rs`),
   and it is how another process's maintenance is explained. Anything else is
   unexplained, and e is sent back.
3. Untouched footprint. For every key e reads, no effect in the interval
   added it, and, if e reads it for presence or a count, no effect removed
   it. A row-preserving effect passes. A `changed` effect on a table that
   holds a key space e reads fails the rule.
4. Disjoint batch. No entry admitted earlier in this batch adds or removes a
   key that e reads, and e adds or removes no key that an earlier entry
   reads, in the same key space.

A refused entry has made no effect of the batch.

Why this is enough. An eligible entry's outcome depends on three facts only:

- the accepted schema and the registrations of the tables it touches;
- for each check it runs, the committed state of the keys that check reads:
  the absence of its ids and tuples, the presence of its endpoints, and the
  edge counts of its endpoint values;
- checks over its own rows: types, required values, value constraints and
  computed embeddings.

Eligibility removes every other read. An executed update or delete reads
through a predicate, even one that matches nothing. An overwrite reads the
whole table.

Rule 1 keeps the first fact. Rules 2 and 3 keep the second: since `He`, no
effect added a key e reads, and no effect removed a key e reads for presence
or a count. The last fact depends on nothing outside e. So e validates at `Hc` as it did at `He`, and
its staged effect is the one a fresh preparation at `Hc` would stage. Rule 4
makes the members pairwise disjoint per table, so they commute, and every
serial order on `Hc` yields the state the composed commit publishes. Every
member was in flight when the batch published, so the publication point lies
inside every member's invocation. The history of these writes on a branch is
strictly serializable.

| Case | Caught by |
|---|---|
| Two entries insert the same new key | Rule 4 in one batch, rule 3 across batches. The later entry is sent back. Its rerun sees the key: a strict insert gets `KeyConflict`, a keyed insert becomes an update on today's path |
| An update, upsert or delete of the same key in this process since the capture | Rule 3: the record's written keys hold e's key |
| An update or delete of other keys in the same table since the capture | Explained by rule 3. e is admitted |
| Another process wrote, merged or deleted since the capture | Rule 2: unexplained |
| A foreign insert of e's key behind a stale log record (H1) | Rule 2: the walk follows links only, and the log resets on a record that does not link |
| An edge's endpoint deleted since the capture | Rule 3: the delete's record removes the id in the node table's id space, or the table is `changed`. Otherwise rule 2 |
| A publication on a path that does not append to the log (H13) | Cannot happen: the door appends |
| A schema apply, or the branch deleted and recreated | Rule 1 |
| Write skew through a predicate scan of a table another entry writes | Eligibility: an executed update or delete is ineligible |
| Insert `Left` and update `Right` against insert `Right` and update `Left`, from a base where neither exists (#785 review) | Eligibility, from `op_kinds` carried through staging |
| Two entries insert one `@unique` tuple | Rule 4 in one batch, rule 3 across batches: both read its absence. The later rerun gets the uniqueness refusal |
| An update moves a row's `@unique` tuple to v, beside an insert of v | Rule 3: the update's record adds v |
| Two edges from one source under a bounded `@card` | Rule 4 in one batch, rule 3 across batches: both read the source's count. The later one is sent back and recounts |
| A delete of an edge from s, beside an edge insert from s under a `@card` minimum | Rule 3: the delete's record removes s, which the insert reads as a count |
| A node overwrite with retained edges | Eligibility: `RetainedEdges` has no key form, and an overwrite is not an insert |
| `optimize` or an index build since the capture | Explained as row-preserving. Composition reads the current pin |
| A stale batch capture | `ExactGraphHead` refuses the batch. The members are sent back |

### The effect log

Each write group keeps an in-memory log. A record holds the head the
publication built on (its parent), the head it produced, and one class per
touched table. An exact class lists, for every key space of the table, the
keys the publication added and the keys it removed. Every key space means
the ids, each `@unique` group, and, for an edge table, `src` and `dst`, so
that any entry's read can be judged against it.

The door writes it. Every publication passes through a `GraphCoordinator`
commit method (`commit_updates_with_actor`,
`commit_updates_with_actor_with_expected`,
`commit_changes_with_intent_and_expected`). Give each one a required
`PublicationEffect` per table, with no default. Append the record after a
successful compare-and-swap, before the caller releases the branch gate.
Make sure that no publishing path reaches the publisher another way, and pin
both facts in `forbidden_apis.rs`.

The constructors, one per class:

- `inserted`: only from an insert-only effect of the absence-proven insert
  writer. It adds the written rows' keys in every key space, read from the
  staged rows. It removes nothing.
- `touched`: only from a mutation's staging. It adds the keys of every
  inserted, upserted or updated row image, and removes the keys of every
  updated row's old image and every deleted row, cascaded edges included.
  The delete path records deleted ids today
  (`MutationStaging::record_deleted_ids`, `exec/staging.rs`). It must also
  record the removed rows' `@unique` tuples and edge endpoints, which it can
  project from the rows it matches. If it lacks them for a key space, the
  class is `changed` for that table.
- `row_preserving`: only from a staged chain whose Lance operations are all
  `Rewrite` or `CreateIndex`.
- `changed`: takes nothing. Loads, merges, overwrites, schema evolution of a
  table, and everything else.

The classes fail closed. An incomplete footprint admits a write it
must refuse. A `changed` record only refuses more than it needs to.

The log type enforces its links (rule 4). Logging every publication of the
process, not only batches, stops a sent-back member's publication from
refusing every entry queued behind it. Logging updates and deletes by their
exact ids, not as `changed`, keeps batching alive under a mixed workload:
3.3 to 9.0 writes/s on RustFS in the stress study.

The log is cleared by a publication that changes the schema contract or the
table set, by a branch delete or recreate, by an outcome the read-back leaves
in doubt, and by a process restart. Entries captured before then fail rule 2
and are sent back.

The log is bounded by K records and a byte budget for keys. A `@unique` tuple
of long strings counts its bytes, and a record over the budget is `changed`. It caches facts
about immutable, published commits, which invariant 12 permits. It is never
commit authority: `ExactGraphHead` is.

### Composition

Each table in a batch receives only insert-only effects with pairwise
disjoint ids. The leader builds one `Update`, read on the table's pin at
`Hc`:

- `new_fragments`: the concatenation of the members' raw staged fragments
  (rule 13);
- `fields_for_preserving_frag_bitmap`: the members' common list, equal under
  one schema. A difference refuses composition;
- the inserted-key filter: the bitwise OR of the members' Bloom filters,
  which share one key column and one configuration;
- `omnigraph.insert_absence`: certified for the composed base only when every
  component carries it. `omnigraph.no_by_source_delete`: kept only when every
  component carries it.

The composed commit goes through a sealed `table_store` adapter that
`forbidden_apis.rs` registers.

What Lance 13 guarantees. The carried-over guard pins the first three.

- Fragment ids are assigned while the manifest is built.
  `Transaction::fragments_with_ids` fills only a zero id.
- Stable row ids come from the manifest's counter at commit. `assign_row_ids`
  fills a missing or partial sequence, and a raw insert fragment carries an
  empty one.
- Row version metadata is derived while the manifest is built, and the
  composed rows share one stamp.
- The manifest is validated before it is written: unique fragment ids,
  required fields, field-id allocation.
- An insert-only `Update` is compatible with `Rewrite` and `CreateIndex`,
  because it overlaps no fragment and graph tables use stable row ids.
  #1349's `TransactionRebase` guards pin this.
- Data files and their Blob v2 sidecars keep their names. Lance names a data
  file at random (`generate_random_filename`) and writes sidecars to
  `data/{data_file_key}/{blob_id}.blob`, so nothing depends on the fragment
  id. The guard must still pin a Blob column before Blob tables join.

What Lance 13 does not guarantee, so the engine must:

- Key uniqueness. The primary key is unenforced, and a detached commit runs
  no conflict pass. Disjointness is admission's job.
- Schema agreement between components. The engine compares the schema
  identity and the field list.
- Session caches after a detached commit (batch step 6).
- A bound on manifest growth. Every member adds one fragment, and the
  manifest lists them all.

### Known keys

A process-wide map from a data file's resolved object location (dataset
root, base and path) to the exact set of key values this process wrote into
it.

Filling. `stage_absence_proven_strict_insert` fills it when its staged write
produced exactly one data file, the usual shape of a keyed batch (at most
8,192 rows). It records the ids from the key column it wrote, after the file
is written and before any manifest can name it. No other writer records.

Reading. `first_existing_id` (`storage_layer.rs`) and `existing_id_count`
(`table_store.rs`) use it when every fragment of the uncovered tail at the
pinned version has exactly one data file that the map knows, and no deletion
file and no overlay. The probe then scans only the indexed fragments (Lance
`fast_search`) and answers the tail from memory. Otherwise it scans as today.

Two rules keep the read cheap as the tail grows:

- Lift the tail cap when the map answers the whole tail. `stage_keyed_write`
  proves absence only while uncovered fragments times ids stay within
  `KEYED_WRITE_MAX_ROWS` (8,192, `storage_layer.rs`). Past that, every keyed
  insert becomes a full-table merge join: 0 writes/s (C1, on `main` today).
  The cap bounds the `IN` lists a scan of the tail plans, and a tail answered
  from memory plans none. In the stress study the lift took 27–36 to 126–136
  writes/s on release binaries past the cliff.
- Compute the tail analysis once per table version. Which fragments are
  uncovered, and whether the map knows all of them, is a fact about one
  pinned version. Share it across probes instead of computing it three times
  per write under a process-wide lock. Pure inserts doubled in the dev build
  (112 to 225 writes/s).

Why the answer is exact. Lance names every data file from a fresh random UUID
and never rewrites a file. So the ids in a file are a fact about the object,
true in every version, branch and handle that lists it. A fragment with one
data file, no deletion file and no overlay holds exactly that file's rows.
Schema evolution keeps the file's ids: a `Merge` adding nullable columns
writes no data file, and a `Project` keeps the key column's file. The
system-column upgrade renames the column without rewriting the file.

The probe falls back to the scan for:

- a fragment with a deletion file (the map reports deleted rows as
  present: a false `KeyConflict`, or an upsert taken for an update);
- a second data file (a column patch) or an overlay;
- a file the map does not know (another process wrote it, compaction, a load
  or a merge rewrote it, the entry was evicted, or the process restarted);
- a table without an id index.

Another handle of the same process is not a bypass, because the map is
process-wide. A recreated branch or a restored backup cannot make an entry
wrong, only irrelevant. Keying by resolved location keeps equal names in
different roots apart. Lance 14 moves BTREE to the row-address domain
(lance#9481), so a compacted fragment can be uncovered. Its file is unknown
and scanned: re-measure the cost at that bump.

Bound and eviction. A process-wide byte budget that counts key bytes and
per-entry overhead, oldest evicted first. When this process publishes a
`Rewrite`, drop the entries for the files it removed. An evicted entry costs
only the scan it saves. The prototype bounded by id count and never evicted:
in the one-hour soak it grew to 362,650 files and about 100 MB. Count probes
answered from the map, and misses, in `QueryIoProbes`
(`crates/omnigraph-core/src/instrumentation.rs`), and report the map's bytes.

The map is a cache of immutable facts keyed by the object they describe
(invariant 12). It changes cost, never a result, so it is physical
acceleration (invariant 7). A wrong entry would need the recording writer to
record values it did not write into that file.

### Bounds and backpressure

| Bound | Rule |
|---|---|
| Queue | Per group, by entries and by staged bytes. A submission beyond it waits for room. Server admission also bounds HTTP writes |
| Batch | At most B entries, and at most F composed fragments per table, so a composed transaction stays inline in its manifest. The prototype's largest batch was about 120 writes at 256 writers |
| Horizon and log | K records and a byte budget. A capture older than the oldest record is sent back |
| Known keys | The byte budget above |
| Retries | Unchanged: two attempts in the process (Progress), and `MAX_PRE_EFFECT_REPREPARES` (32, `exec/mutation.rs`) across processes |

Choose B, F, K and the budgets from the instrument. In the stress soak, RSS
rose from 2.6 GB at 10 minutes to 3.5 GB at 30 and 5.2 GB at 60. That fits
Lance's session caches filling toward their defaults (6 GiB index, 1 GiB
metadata, `lance_access.rs`), plus the unbounded known keys. So the soak gate
compares 60 minutes with 30, and checks the known keys and the log against
their budgets.

### Fragments and maintenance

These passes are linear in the fragment count: the uncovered-set computation,
the known-key lookups per uncovered fragment, the tail's data-file sizes, and
each batch's table manifest build and write. Batching raises the rate at
which fragments accumulate, so the rate decays without maintenance.

So batching is specified with a maintenance cadence:

- The maintenance carry (Interactions) lands at step 4, before step 6.
- The server admits a recorded `optimize` while writes are in flight (O1).
- The user docs that enable batching state the cadence. In the soak, a served
  `optimize` every minute at about 100 writes/s took a flat 6.6 s and held the
  rate within 15% for an hour.
- The instrument reports the decay with no maintenance and requires that it
  never reach 0 writes/s.

One data file per table per batch is an open question. The leader would
write each table's admitted rows into one data file instead of one fragment
per member. The #1399 leader-staging variant did this on a table without an
id index: 58.3 to 147.2 writes/s at 64 writers, but 12.0 to 9.2 at 4 writers
and 46.4 to 36.8 at 16, because the probe and the data put moved into the
serialized path. With known keys the probe stays with the writers, and only
the put moves. Measure it at step 6 at 4, 16, 64 and 256 writers, with and
without maintenance. Adopt it only where it does not lower small-batch
throughput, possibly only above a batch size. Lance's `FLAG_FRAGMENT_TREE`
(marked unsupported in 14.0.2) is the substrate's answer to manifests that
list every fragment.

### Failure, recovery and progress

There is no new durable state and nothing to recover. Before the
compare-and-swap, a batch's detached commits are unreachable and stamped with
`Hc`, and each member's data files are unreferenced. After it, every member's
effect is complete. The publisher's read-back settles an ambiguous outcome.

The collector's rule does not change. A batch that loses leaves staging that
`judge_staging` (`db/omnigraph/collector.rs`) proves dead once the head moves
past `Hc`. Pipelining breaks this (Part 3, Alternatives). A member's data
files stay unreferenced between staging and the commit, as every write's do
today. The collector deletes an unreferenced file only after
`UNVERIFIED_THRESHOLD_DAYS` (7 days). The queue bounds a wait in
publications, not in time. This RFC inherits that assumption.

Progress. Since #1349, a write sent back before its effects prepares again
under the gates, from capture through publication (`PreEffectReprepares`,
`db::write_queue::preparing`). A member refused at admission, or whose batch
lost the compare-and-swap, keeps that rule: its second attempt runs on
today's gated path and publishes alone. Every publisher on the branch in the
process takes the branch gate, a leader included. So nothing in the process
moves the branch under the second attempt, and a write finishes within two
attempts in its process. Another process stays outside the bound: the
compare-and-swap and the 32-attempt limit cover it.

Checkpoints:

- Each entry crosses `mutation.post_stage_pre_effect_gate` before it queues.
- The publisher's `publish.post_merge_pre_ack` and `publish.read_back` are
  crossed unchanged.
- New seams (`decide_seam!`, catalogued in `crates/omnigraph/src/seams/catalog.rs`):
  after queueing, after admission, after the composed detached commits and
  before the compare-and-swap, and at the lead handoff. They land at step 5.
  Without them the failure suites cannot reach the batch path. In the stress
  study, the `failpoints`, `detached_commit_matrix` and DST failures that
  remained with batching forced on all armed an ordinary-path seam
  (`mutation.post_finalize_pre_publisher`, `mutation.post_table_commit`) that
  the batch path never crosses.

### Interactions

- Writer epoch. A batch's publication asserts the process's holder like any
  publication, and `WriterFenced` reaches every member. On a held bound
  branch, the leader's capture reads nothing. This RFC adds no probe-free
  capture beyond the holder skip (#1350). See H12 under Before you start.
- `optimize` and index builds under batches. An index build captures before
  staging and is refused when the head moved before its gates
  (`maintain_indices_for_branch`, `db/omnigraph/table_ops.rs`). So is
  `optimize`'s direct path (`AuthorityCapture::BeforeGates`,
  `db/omnigraph/optimize.rs`). Under batching, 0 of 16 landed. Step 4 carries
  both over insert-only and row-preserving publications, as #1349 does for
  writes:
  - under the gates, prove that each moved pin reaches the captured one
    through insert-only `Update` links and `Rewrite` or `CreateIndex` links,
    with registrations unchanged and no row count going down;
  - then re-anchor the staged `CreateIndex` or `Rewrite` with Lance's
    `TransactionRebase` before its first detached commit.

  Lance makes `CreateIndex` compatible with appended and updated fragments,
  which stay unindexed, and makes `Rewrite` conflict with an `Update` only on
  overlapping fragments. Served `optimize` keeps capturing under the gates
  (`AuthorityCapture::UnderGates`), so batches wait within its `max_gate_hold`
  ([writes](../dev/writes.md)). The served path has no index-build route. Its
  `optimize` reconciles indexes.
- Batches over maintenance. A maintenance publication is row-preserving, so
  rule 2 explains it: this process's through its `row_preserving` record,
  another process's through the durable proof. Composition reads the
  compacted pin, and Lance assigns fresh ids above its high-water marks.
- Schema apply. It takes the schema gate exclusively and waits for batches,
  which hold the shared permit. Its publication clears the log. A queued
  entry captured before it is sent back and reruns against the new schema.
- Branch merge, delete and recreate. A merge into the branch takes the branch
  gate and is logged `changed` for the tables it moved. A delete or recreate
  fails rule 1.
- `cleanup`. Unchanged. A batch is an ordinary publication.

### Step 7: key-addressed updates and deletes

This step extends eligibility to updates and deletes that name their rows by
key. It is designed now so that the footprint, the log and admission need no
second redesign. It lands only if its measurement passes.

Eligibility. An update or delete is key-addressed when its predicate has a
conjunct that is an equality or an `IN` list on the target's key property,
against parameters or literals. Read the typed IR predicate
(`MutationOpIR::Update { target, assignments, predicate }` and
`MutationOpIR::Delete { target, predicate }`, `omnigraph-compiler`
`src/ir/mod.rs`), never its text (invariant 9). Other conjuncts are then
evaluated only on the rows of those keys. The write must also:

- assign no key, `src` or `dst` property;
- meet rules 3 to 6 of Eligibility, so every check it runs has a key form;
- execute only key-addressed updates, deletes and inserts, recorded per
  operation as `op_kinds` are.

An upsert that matched a present key counts as a key-addressed update of
that key.

The footprint. The write reads the presence of its narrowed keys in the id
space, whether or not a row matched, because a row's absence is read too. Its
checks add their reads from the Footprint table: an update that sets a
`@unique` column reads the absence of the new tuple. It removes the keys of
the old row images in every key space and adds those of the new images. A
node delete whose type has incident edge types also reads each incident edge
table by endpoint, to cascade. That is a presence read of the deleted node
ids in the edge table's `src` and `dst` spaces, and the cascade removes the
edges it found in every key space. An edge insert to node `k` adds `k` in the
edge table's endpoint space, so admission rules 3 and 4 keep it and a delete
of `k` apart.

Why this is enough. A key-addressed write's outcome depends on the accepted
schema, the rows at its keys, and, for a cascading delete, the edges incident
to its keys. Admission proves that nothing since `He`, and no earlier member,
wrote any of them. So the write computes at `Hc` what it computed at `He`:
the same matched rows, row images, cascade and affected counts. The
serializability argument carries over unchanged.

Composition. An update's or delete's staged effect cannot be composed like
an insert's. It removes old rows through a deletion file read at `He`, and
since then another write can have added a deletion file to the same fragment,
or compaction can have rewritten it. So the leader stages these again at
`Hc`. Per table it writes one keyed write of the admitted updates' row
images, which preparation computed and admission keeps valid, and one delete
of the admitted deleted keys. Both are detached commits chained on the
table's composed insert commit, and one pin publishes the chain. That puts an
id-index read and at least one detached commit per table into the serialized
path. Do not rely on Lance 13's rebase of one `Update` over another on the
same fragment. A guard would have to pin its deletion-file merge first.

Evidence before it lands. The mixed-workload instrument at 16, 64 and 1,024
writers with the step off and on, and matrix cells for: two updates of one
key, two updates that move different rows to one `@unique` tuple, an update and an insert of one key, a delete of node `k` beside an edge
insert to `k` in one batch and across batches, a delete that matched nothing
followed by an insert of that key, and an update whose other conjunct matches
no row. It lands only if mixed-workload writes/s rise over step 6 and one
writer stays within 10% of the ordinary path.

### Invariants

| Invariant | Effect |
|---|---|
| 2, one publication door | Unchanged: one compare-and-swap per batch, through the unchanged publisher |
| 3, one coherent accepted view | Reworded at step 6 (Part 1). `AGENTS.md` working-memory line 2 becomes: "A query or write attempt uses one coherent accepted snapshot; a write may publish on a later one only by proving everything it read unchanged. A retry starts fresh rather than mixing old and new authority." |
| 4, a mutation publishes once | Each write is in exactly one publication, and its statements stay together. A publication can hold several writes of one actor. No text change |
| 5, crash convergence | Unchanged: detached commits follow admission, carry the batch's witness and stay unreachable until the publication |
| 7, physical acceleration | A composed append leaves an unindexed tail like any append. Known keys and the index warm-up are caches |
| 8, integrity failures are loud | Every constraint an entry validated holds at publication, by admission. A check with no key form makes the write ineligible |
| 10, policy | Enforced per write at its `_as` entry point, before it queues |
| 11, bounded and observable | Queue, batch, log, horizon and known keys are bounded. Send-backs are counted as re-preparations. Batch size, known-key probes and misses are counted |
| 12, one source of truth | The log and the known keys cache immutable facts. The log has one writer, the door, and its classes are derived and fail closed. The queue holds requests, not manifest-derived state |
| Deny-list | The write group is process-local and is not fencing: the compare-and-swap and the writer epoch remain the cross-process fences. No job queue, no silent retry, no acknowledgement before durable visibility |

### Rollout and done criteria

The per-branch writer-state cache (#1407) lands before batches on named
branches are measured.

| Step | Scope | Done when |
|---|---|---|
| 1. Known keys | The map, the byte bound with eviction, dropping files a `Rewrite` removed, the tail analysis once per version, and the cap lift | In-source `table_store` tests compare map and scan on every fragment shape (deletion file, column patch, compaction, unknown file, eviction). A tail past `KEYED_WRITE_MAX_ROWS` that the map answers stays on the absence-proven writer. `write_cost.rs` shows a keyed insert's requests flat as the tail grows. The DST cost golden records it at two tail lengths. `forbidden_apis.rs` pins that only the absence-proven writer records. No behavior change |
| 2. Footprint | Carry `op_kinds` through staging and rewrite its doc comment to name the executed-operation class, with `strict_pre_stage_version_check` its one owner. Record validation reads, with endpoint ids. Give `StagedWrite` the exact inserted ids its doc comment describes, and carry a mutation's written ids to its publication. Derive the footprint from `ValidateCheck` through one wildcard-free `match`, and record removed rows' tuples and endpoints on the delete path. Define `PublicationEffect` and its four constructors, with keys per key space. Make eligibility one function | In-source staging tests, one per `ValidateCheck` arm, and `writes.rs`. `forbidden_apis.rs` pins the wildcard-free match. No behavior change |
| 3. Composition | The sealed adapter, the engine half of the guard, the Blob case | `lance_surface_guards.rs`, `integration/changes.rs` (the feed's fast path), the merge owners (the pure-insert proof), `forbidden_apis.rs` |
| 4. Maintenance carry | Index builds and `optimize`'s direct path carried over insert-only and row-preserving publications | `lance_surface_guards.rs` (`TransactionRebase` of `CreateIndex` and `Rewrite` over an insert-only `Update`), `failpoints.rs`, `detached_commit_matrix.rs` (a carried index build), the DST cost golden |
| 5. Write group, batches of one | Queue per root and branch, lead handoff with drop guards, group-owned typed outcome, the log written by the door with its link rule, the gated second attempt, batch-path seams, cancellation, per-member evidence, the `group_commit` setting (default `off`), the instrument | `write_cost.rs` (a batch of one costs no more than today), `failpoints.rs` batch-outcome cells, `detached_commit_matrix.rs`, DST, `forbidden_apis.rs`, the GQT corpus with the setting on. Behavior unchanged |
| 6. Batching of inserts | Admission and composition in the batch, exact footprints for publications outside it, setting still off by default. Invariant 3, `AGENTS.md` and `docs/dev/writes.md` change. Measure one data file per batch | The admission matrix, `failpoints.rs`, DST, RustFS and Azurite suites, all with batching forced on |
| 7. Key-addressed updates and deletes | Step 7 above | Its matrix cells and measurement. If it fails, record it as declined, and step 8 follows step 6 |
| 8. Default on | After `blocked_on` is clear: O1, the soak, the instrument gate, the user docs with the maintenance cadence, the skill and a `changed` release note | `docs/user/mutations/index.md` (Atomicity), `docs/user/branching/changes.md`, `docs/dev/writes.md`, `skills/omnigraph` updated |

`implementation` moves to `in-progress` at step 1 and to `complete` at
step 8.

### Tests

The admission matrix. Outcomes go in `.gqt` cases with a `--- concurrent`
block. Commit counts, and which entry was sent back, go in `writes.rs`,
because `.gqt` cannot express them. The cells:

- disjoint keys make one commit, for strict inserts and keyed inserts;
- the same key: the later entry is sent back, then gets `KeyConflict` or
  becomes an update;
- an insert beside an update of another key on the same table is admitted,
  and beside an update of its own key is sent back;
- an edge insert beside a delete of its endpoint;
- an entry parked across a foreign insert of its key behind a stale log
  record, and across another actor's batch (H1);
- two handles of one process batch together;
- two inserts of one `@unique` tuple, in one batch and across batches: one
  publishes, the other is refused for uniqueness on its rerun;
- inserts with different `@unique` tuples share one commit, and a tuple that
  holds a null is no key, so two such inserts batch;
- an insert of tuple v beside an update that moves another row to v;
- two edges from one source under a bounded `@card` maximum, one batch and
  across batches: the final count is within the bound;
- an edge insert from s under a `@card` minimum, beside a delete of another
  edge from s;
- a node insert under `SourceCardinality` with its edges in the same write;
- a unit test for each `ValidateCheck` arm, comparing the footprint with the
  rows the judge read;
- write skew through a predicate scan;
- the `Left`/`Right` counterexample: one publishes, the other is sent back
  and reruns, and the rows are (1, 0) or (0, 1), never (0, 0);
- an expected head publishes alone, including the first write on a fresh
  named branch;
- two actors make two commits;
- a publication by `optimize`, a merge or another handle between capture and
  batch;
- `group_commit = off` matches today.

Which request a `.gqt` session can park at after preparation is still open
(#785): a fragment write that runs on Lance's pool cannot be named.

Other owners:

- The effect log: in-source tests that a non-linking append resets the log,
  that a walk never crosses a reset, and that each constructor returns
  `changed` when its ids are incomplete or over budget. `writes.rs` asserts
  that every publishing path (mutation, load, merge, `optimize`, index build,
  schema apply, collector) leaves a record of the expected class.
- Batch outcomes: `failpoints.rs` cells for a leader dropped before and after
  its first detached commit, a follower dropped while handed the lead, an
  entry dropped before admission, and every member receiving the batch's
  typed error (`WriterFenced`, a conflict with details and HTTP status,
  `manifest_publish_in_doubt` naming the batch's commit).
- Failure windows: `failpoints.rs` covers the new seams, a refused
  compare-and-swap, the in-doubt read-back, and both `issue_1331` gated-retry
  cases with batching on. `detached_commit_matrix.rs` gains a grouped-insert
  writer through every window, fault and recovery actor.
- DST: the concurrent universe (`crates/omnigraph-dst/src/concurrent.rs`,
  `dst_concurrent_*`) with batching on and the collector oracle, which checks
  every acknowledged write visible once and one actor per commit. The
  storage-cost golden gains a batch row.
- Instrument: #1401's `concurrent_write_throughput.rs` lands at step 5 as an
  `instrument:` ignored test. It owns the soak and the instrument gate until
  #895's executor can measure overlap open-loop.

The stress study's oracles are the model for the soak: every acknowledged
receipt names a commit that holds the write with its actor, one lineage
chain, no duplicate key, no dangling edge, `@unique` held, and a replay of
the acknowledged writes in commit order equal to the final state and to every
reader's pinned snapshot. A write in doubt (cancelled, disconnected, crashed)
appears at most once.

### Defects in the prototype

Each defect has a rule above and a test that must fail without the rule.

| Defect | Severity | What broke | Rule | Test |
|---|---|---|---|---|
| H1 | Critical | Admission across a gap in the log: duplicate keys, edges to deleted nodes, false `omnigraph.insert_absence` | 4 | Matrix: parked entry across a foreign insert behind a stale record, and across another actor's batch |
| H3 | High | A dropped leader hung its group forever | 10 | `failpoints.rs` dropped-leader cells |
| H13 | High | Maintenance publications unlogged: every queued entry refused after each run | 2 | `writes.rs` record-per-path assertion |
| H14 | High | Log copied before the gates: a refusal storm that never recovered | 5 | Effect-log in-source tests, the soak |
| H5 | High | Ungated re-preparation: no progress bound, 5 to 7% of writes returned 409 | 8 | Both `issue_1331` cases with batching on |
| H12 | Medium–high | Listing-free publisher cleared the epoch holder | Before you start | DST writer-claims universe |
| H11 | Medium | Every batch error re-typed as internal | 9 | Batch-outcome cells |
| H10 | Medium | Edge footprint omitted endpoint tables | 7 | Matrix: edge insert beside a delete of its endpoint |
| H2 | Medium | Groups keyed by actor: one actor's batches foreign to another's | 6 | Matrix: two actors, two handles |
| H4 | Low–medium | A write cancelled before admission was published | 11 | Batch-outcome cells |
| H7 | Low | In-doubt followers got an internal error | 9 | Batch-outcome cells |

Hypotheses that produced no defect: Blob tables in batches (H6), an
unbounded queue (H8), and known keys keyed by relative path (H9).

## Part 3: Evidence and history

### Sources

Surveyed: Omnigraph `main` at `2c0941e0` for the design and `4f7c2076` for
code locations; PR #785 at `0069e925`; the research drafts #1399
(`e628c35d`), #1400 (`f22f0718`) and #1401 (`ad39204a`), with their reports,
logs, raw results and prototypes; Lance 13.0.0's transaction, versioning,
branch/tag, distributed-write, row-ID-lineage, object-store and Blob pages,
read 2026-11-14. Stress-tested: the #1401 prototype merged with #1407 and
`main`, on release binaries, fork branch `research/group-commit-stress` at
`155911f0` (2026-11-15).

### Why one branch serves one write per publication

A write on RustFS at +30 ms per request takes about 10.5 serial round trips.
Six are under the branch gate: the table's transaction file and detached
manifest, the publisher's `_versions` listing, then the `__manifest` data
file, its transaction file and the version put (#1401 §3, traced).
Revalidation refuses whenever the branch head moved, so every publication
sends every other prepared write back. Since #1349 the second attempt
prepares under the gates. Writers are now treated fairly, but the branch
still serves one write per publication. Issue #784 is the same mechanism
before #1349.

The floor is one conditional put per publication, serial per branch. With
Lance 13 a publication is five serial puts. Lance writes the transaction
file before the manifest (`do_commit_detached_transaction`). The switch that
skips it, `ManifestWriteConfig::with_transaction_file_disabled`, is
`#[cfg(test)]` in Lance 13 and `pub(crate)` in 14.0.2. That is about 165 ms
at 32 ms per request, so more than about six writes per second per branch
needs several writes per publication.

An insert into a keyed type first proves its keys absent. This is a Lance
scan with `id IN (…)`: the id BTREE answers indexed fragments, and the id
column of every uncovered fragment is read. Every write adds a fragment, and
only explicit maintenance rebuilds the index (invariant 7). With batching
on, #1401 measured every variant topping out near 70 writes/s locally with
two writes per publication: preparation was the limit. #1399 met the same
wall on a table without an id index: 6.3 cores busy at 62 writes/s, and CPU
per write rising from 31 to 110 ms within one run.

### Measured

All instruments are closed-loop, so they are decision evidence, not claims
(RFC 0039, Rule 1). Every cell verified exact row counts on a fresh handle
afterwards.

#1401 on RustFS and local disk. RustFS 1.0.0-beta.12 behind toxiproxy, 15 ms
each way (about 32 ms per request), and a local tempdir. Every crate at
`opt-level=2`. N writers over one `Arc<Omnigraph>`, one row per write into
`Item { slug: String @key, n: I64 }`, 64 seed rows with indexes built. 3 s
warm-up, a 20 s window, medians of 3 interleaved runs. The baseline is
`ed09ee63`. The ordinary path is the research binary with batching off, with
known keys and the publisher's listing-free first attempt (#837). Group
commit adds batching, the leader's capture from its own publication, the
index warm-up and the named-branch warm state
(`results/final-best-matrix.md`).

| One branch | Baseline writes/s (p99) | Ordinary path | Group commit (graph commits/s) |
|---|---:|---:|---:|
| RustFS, 16 writers | 2.20 (7,519 ms) | 2.45 (6,692 ms) | 44.85 (367 ms; 5.65) |
| RustFS, 64 writers | 2.35 (22,663 ms) | 2.65 (22,873 ms) | 153.54 (504 ms; 4.75) |
| local, 16 writers | 21.10 (1,061 ms) | 47.80 (536 ms) | 203.19 (142 ms; 25.8) |
| local, 64 writers | 22.30 (3,878 ms) | 49.25 (1,799 ms) | 247.49 (534 ms; 8.0) |

- On RustFS, requests per write fell from 24.2 to 3.8 at 16 writers and from
  22.2 to 3.2 at 64. Re-preparations fell from about 1.0 per write to 0.
- The longest service time including the drain fell from 7,944 to 554 ms at
  16 writers and from 32,665 to 601 ms at 64.
- One writer on RustFS ran 4.15 writes/s with batching on, 3.70 on the
  ordinary path and 2.90 on the baseline (single runs).
- RustFS reached 208.05 writes/s at 128 writers and 244.53 at 256, flattening
  near 250 because a batch's work grows with its size.
- Separate branches gained 2.2x at 64 writers (34.20 to 73.90). With one
  writer per branch every batch holds one write, so the gain came from warm
  named-branch writer state, which #1407 covers.

#1401 known keys off and on, one binary, batching on
(`results/known-keys-ab.md`). Local off-side runs are medians of 2. The rest
are single runs.

| One branch | writes/s | p99 | requests/write | writes/publication |
|---|---|---|---|---|
| local, 16 writers | 69.95 → 191.99 | 489 → 159 ms | 4.2 → 2.3 | 1.9 → 7.9 |
| local, 64 writers | 65.45 → 249.32 | 2,688 → 510 ms | 4.1 → 2.1 | 2.1 → 30.0 |
| RustFS, 16 writers | 38.95 → 44.85 | 578 → 371 ms | 11.0 → 3.8 | 7.1 → 7.9 |
| RustFS, 64 writers | 68.60 → 152.74 | 2,012 → 485 ms | 7.7 → 3.2 | 16.0 → 32.2 |

#1399 on an in-memory store. Lance's `shared-memory` object store behind the
engine's object-store seam, 20 ms injected per request, the S3
conditional-put commit handler. Workspace crates at `opt-level=2`. One
`Session`, the writer epoch claimed, a table with no id index. 2 s warm-up
and a 10 s window.

| One branch | Baseline writes/s (p99) | Writers' staged files composed | Leader stages the batch |
|---|---:|---:|---:|
| 16 writers | 1.7 (5,214 ms) | 46.4 (512 ms) | 36.8 (422 ms) |
| 64 writers | none in 10 s | 58.3 (1,799 ms) | 147.2 (422 ms) |

At 64 writers the composed form was CPU-bound in the writers' probes, the
limit known keys address on indexed tables. Over longer windows, the full
configuration fell from 351.5 writes/s at 10 s to 234.0 at 60 s as unindexed
fragments accumulated. With an index build every 5 s, only 1 of 13 builds
landed. Its fault test (2% and 10% of conditional creates failing) and its
mixed upsert and update test kept every acknowledged write exactly once.

#1400 and the #785 spike. #1400 used RustFS at +30 ms with a 15 s window and
32 history commits. Without batching, carry-over, a trusted in-process tip,
the warm-up and pipelined publication took one branch from 2.13 to 9.60
writes/s at 16 writers. #785's spike used the deferred `concurrent-writes`
bench at `97f293e8` on RustFS at +30 ms. It took 8 writers from 0.87 to 9.23
writes/s, with per-writer max/min from 26 to 1.0.

The stress study (2026-11-15). Release builds of `omnigraph` and
`omnigraph-server`, on local disk and on RustFS behind toxiproxy at +15 ms
each way, with batching forced on.

- Correctness. Eleven defects, five of them able to lose data or liveness
  under load (H1, H3, H5, H13, H14), each reproduced, fixed and re-verified.
  After the fixes, no acknowledged write was lost or duplicated under crash
  points between the detached commits and the compare-and-swap, `kill -9` of
  the server, toxiproxy resets, truncated acknowledgements and stalls,
  cancellation of up to half the writes, several actors, handles and
  processes, a writer-epoch claim, Blob values and maintenance alongside.
  After eight aborts before the compare-and-swap, `cleanup` reclaimed exactly
  eight unpublished manifests and changed no row.
- Suites with batching forced on: `writes` 67 of 67, engine `integration`
  704 of 704, GQT 477 of 477. The remaining `failpoints` (13),
  `detached_commit_matrix` (7) and DST (4) failures were seams the batch path
  never crosses, tests asserting the #1331 carry where admission now explains
  the `optimize`, and the cost golden.
- Pure inserts. 56 writers on RustFS for an hour with a served `optimize`
  every minute: about 100 writes/s, 28 writes per commit, p50 about 500 ms,
  p99 0.6–1.3 s, 362,706 writes all verified, 54 of 54 `optimize` runs
  published.
- Fragments. 48 writers on local disk with no maintenance fell from 649 to 24
  writes/s in 40 s. Past `KEYED_WRITE_MAX_ROWS` uncovered fragments times
  ids, every keyed insert became a full-table merge join: 0 writes/s.
- Maintenance. 0 of 16 direct-path `optimize` and index builds landed under
  batching. The served `optimize` kept up, 54 of 54 in an hour, but the
  server refuses it once 64 writes are in flight.

| Mixed workload, RustFS, 48 writers, 4 actors | writes/s | writes/commit | p50 / p99 |
|---|---:|---:|---|
| Batching off | 3.0 | 1.00 | 23.0 / 26.3 s |
| First-draft rules (`changed` per table) | 3.2 | 1.06 | 19.6 / 32.3 s |
| Exact footprints | 9.0 | 4.07 | 5.8 / 11.9 s |

The mix was 70% fresh inserts, about 15% same-table updates, deletes and
upserts of present keys, and persons and edges, with `optimize` every 30 s.
At 1,024 embedded writers with 10% same-table updates and deletes, the
first-draft rules made 1.0 writes per commit and exact footprints 15.6.

### The carried-over guard

`lance_surface_guards.rs::group_commit_composes_inserts_staged_against_older_pins_onto_a_later_pin`
is the Lance half of the composition guard, carried over from PR #785. It
passes on Lance 13.0.0 at `main` `2c0941e0`, as does the whole binary. The
test:

- stages two keyed upserts of new keys against P0 and one against P1, where
  P1 is a third insert committed detached from P0;
- commits the three as one detached transaction from P1;
- pins that the raw fragments carry id 0 and an empty row-id sequence;
- pins that the key filters OR exactly;
- pins that the result is the union of rows with P1's rows unchanged, and
  that fragment and row ids are fresh and unique;
- pins that the composed rows share one stamp, and that the version reopens
  by id.

### Alternatives

| Alternative | Why not |
|---|---|
| Do nothing, or only shave round trips | The ordinary path with known keys and a listing-free publisher stays at one write per publication: 2.45 and 2.65 writes/s at 16 and 64 writers on RustFS (#1401 §5.9). #1400 removed round trips, carried inserts over inserts and pipelined publication without batching, and reached 9.6 writes/s at 16 writers |
| N commit rows in one publication (RFC 0067's sketch) | Breaks every mapping from a commit to its `__manifest` version: snapshot reads, diffs and the change feed's head checks, merge bases and collector roots through `pinned_graph_commit`, parent resolution, `commit::overwrite`'s head rows, the lost-acknowledgement read-back and `ExactGraphHead` (#785's survey) |
| Admission by disjoint tables | Batches nothing on the measured workload |
| Rebase each write over the intervening inserts (#1401 §5.1, #1400 A) | Walks one link per intervening commit under the branch gate and still pays a gated commit per write. On RustFS, 16 writers fell from 2.15 to 0.90 writes/s and 64 from 2.35 to 0.85. #1400's in-process link log reached 3.0 |
| Chain same-table writes on each other's detached versions | Serializes preparation per table, and refusing one link refuses its successors |
| A leader linger or timed window (#1401 §5.4) | 39.75 to 36.0 writes/s at 10 and 25 ms. Writers settle into two alternating cohorts. A timer adds latency at low load |
| Pipelined batches (#1401 §5.5, #1399, #1400 C) | +70% publications/s at 16 writers on RustFS (4.85 to 8.25) with writes/s unchanged, because closed-loop throughput follows per-write latency. Stage A's detached commits carry the predecessor's parent head, which the collector would judge dead. Future work |
| Probe-free capture for every writer (#1401 §5.7, #1400 B) | Safe for the compare-and-swap but not for a write's meaning: a query compiled against a replaced catalog failed terminally, a recreated branch's first attempt bound the dead incarnation, and a vanished `__manifest` lost its before-effect evidence. The supported form is #1350's holder skip |
| Bloom filters as the admission test | False positives refused entries and cascaded |
| Exclude `@unique` and bounded `@card` types from batches by name (the 2026-11-15 revision) | A hand-kept list of constraint kinds fails open: a new kind of cross-row check batches unproven until someone adds it. Deriving the footprint from `ValidateCheck` fails closed and lets these types batch |
| Log writes outside the group as `changed` per table (the first draft) | Under a mixed workload every update or delete refuses the queue: 3.2 writes/s against 3.0 with batching off, and one write per batch at 1,024 writers. Exact footprints reach 9.0 |
| Retry a refused entry ungated before the gated attempt | 32 ungated retries reached 8.0 writes/s on the mixed run, below exact footprints' 9.0, with no progress bound. One retry changed nothing |
| Each caller appends its own publication to the log | One forgotten path makes every queued entry unexplained after each of its publications (H13) |
| The leader stages each table's write for the batch (#1399) | Moves the probe and the data put into the serialized path: 4 writers fell from 12.0 to 9.2 and 16 from 46.4 to 36.8. A strict insert's `KeyConflict` would need attributing to one member. One data file per batch, with the probe left to the writers, stays open |
| A dedicated publisher task | A child producer no owned operation owns, invisible to `--measure` and `.gqt` `order:` (#785) |
| A record log in front of `__manifest` (#1399) | One conditional put per publication and +32% at 64 writers in its setting, but it rewords invariants 2, 5 and 12 and overlaps RFC 0068. Future work, in its own RFC |
| RFC 0068's commit record | Lowers the per-publication cost and composes with this RFC: one record per batch |
| Batches across actors | Needs per-write attribution in the commit, a format change that belongs with per-write idempotency (#513) |

### Compatibility and reversibility

- Storage: none. Commit, head and registration rows are unchanged, and an
  older binary reads a batch commit as an ordinary commit.
- Wire and API: none.
- Behavior: the commit grain under concurrent writes by one actor, documented
  with the rollout step that enables it.
- Reversal: `group_commit = off` restores today's path. The known keys leave
  no durable trace.

### Out of scope and future work

- A record log in front of `__manifest`, and pipelined publication (#1399,
  #1400). They need their own RFC, reconciled with RFC 0068. They change
  invariant 2 (the record or the version is the door), invariant 3 (staging
  against predicted state), invariant 12 (a bounded tail where versions live)
  and invariant 5 (a fold after the publication). The collector witness must
  also name the head a pipelined batch publishes on.
- Lance upstream asks:
  - A public switch to record a transaction inline only.
    `FLAG_DISABLE_TRANSACTION_FILE` is in the format, but no public API sets
    it, and `with_transaction_file_disabled` is `pub(crate)` in 14.0.2. It
    would cut a publication from five serial puts to three. The engine would
    then match a pin by the inline transaction's uuid instead of the `.txn`
    name.
  - Concurrent transaction-file and manifest puts for a detached commit,
    since nothing names the manifest until the caller publishes it.
  - Let the caller choose the detached version, so the `__manifest` rows can
    be built while the table commit is in flight.
  - Cache population after a detached commit, as `record_successful_commit`
    does for a linear one.
  - Build the Blob v2 projection schema once per scan, not once per fragment
    read (`io/exec/filtered_read.rs`,
    `public_blob_v2_binary_projection_schema`).
  - Do not unwrap a cancelled fragment-read task (`filtered_read.rs`), which
    panics a worker at runtime shutdown.
  - `FLAG_FRAGMENT_TREE`, marked unsupported in 14.0.2.
  - An operation that carries several writers' inserts with a key uniqueness
    assertion. Lance 14.0.2 has none: its `Transaction.operation` ends at
    `DataOverlay`.
- Broader batching: updates and deletes by predicate, Append and Merge loads
  with exact ids, admission across foreign commits by reading their change
  sets, and per-write attribution for cross-actor batches (#513).

### Decision log

- 2026-11-14: Drafted from PR #785 and the research drafts #1399, #1400 and
  #1401, superseding #785's draft.
  - Kept from #785: one graph commit per batch, key-grain composition of
    same-table inserts from raw staged fragments, detached commits after
    admission, a write group per root and branch (not a spawned task, and not
    per handle as #1401's prototype had it), one actor per batch, conditional
    writes alone, and classification by executed operations.
  - The #785 reviews. The 2026-10-05 counterexample stays an eligibility
    rule and a matrix cell. The 2026-11-08 optional finding (rewrite the
    `op_kinds` doc comment) is rollout step 2. The 2026-11-08 correction (an
    update sent back returns `ReadSetChanged`) matched the contract of its
    day. Since #1349 a sent-back update reruns, so #785's original expected
    rows, (1, 0) or (0, 1), hold again.
  - Changed from #785: step one batches insert-only entries only, exact id
    sets decide admission, the log records every publication of the process
    on the branch, refused entries keep #1349's gated second attempt,
    row-preserving publications are explained by #1349's proof, and the
    gate's instrument is an `instrument:` test because `concurrent-writes`
    is deferred (#895).
  - Added: known keys (#1401 §5.8) and the maintenance carry (#1399 §5.3).
  - Out of scope: the record log, pipelining and the upstream asks.
  - Carried over: #785's Lance composition guard, unchanged except for the
    RFC path in its comment.
- 2026-11-15: Revised after the stress study of the prototype on release
  binaries (`research/group-commit-stress`, `155911f0`).
  - Correctness by construction: the door appends to the log (H13), classes
    are derived and fail closed, the log resets on an append that does not
    link and the walk follows links only (H1), the leader reads the log under
    the gates and a batch that admits nothing leaves it unchanged (H14), the
    group owns one typed outcome with drop guards (H3, H4, H7, H11), and the
    edge footprint names endpoint ids (H10).
  - Exact footprints for publications outside the group: 1.06x of the
    ordinary path became 3x on a mixed workload.
  - Key-addressed updates and deletes become step 7, landing only on
    measurement.
  - Known keys: the cap lift (C1), the tail analysis once per version, and a
    byte bound with eviction.
  - Fragments and maintenance: batching requires a maintenance cadence, the
    carry is a prerequisite, and the server's admission of `optimize` under
    write load is a `blocked_on` item (O1).
  - Gates: batch-path seams before step 6, a mixed-workload gate, a
    no-maintenance decay check, a 60-minute soak compared with its 30-minute
    mark.
  - Corrections: Lance 14.0.2 has no composite operation, and
    `with_transaction_file_disabled` is `pub(crate)` there.
  - Outside this RFC: O1 and H12 are fixes of their own, and the cap's cliff
    without known keys is on `main`.
- 2026-11-15: Footprints derived from the validator's checks. The
  eligibility rule that excluded non-key `@unique` and bounded `@card` types
  is replaced by a key space per `ValidateCheck` variant, mapped by one match
  with no wildcard arm. Reads are absence reads or presence and count reads.
  Records list added and removed keys per key space. These types now batch.
  Rule 17, the Footprint section and matrix cells added. Batching them is
  not measured yet.
- 2026-11-15: Restructured for two audiences. Part 1 explains the design to
  reviewers without context. Part 2 gives implementers the rules, each tied
  to the defect it prevents, code locations at `4f7c2076`, and done criteria
  per step. Part 3 holds the evidence. No design change.
