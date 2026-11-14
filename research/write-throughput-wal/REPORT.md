# A write-ahead log for concurrent writes: research report

**Branch:** `research/write-throughput-wal` (fork `ragnorc/omnigraph`), based on
`origin/main` `2c0941e0` (Omnigraph 0.15.x, Lance 13.0.0).
**Date:** 2026-11-14. **Status:** research prototype, not for merge.
**Log of every step, command and raw number:** [LOG.md](LOG.md); raw results:
[results/](results/); notes from the source reading: [notes/](notes/).

## Summary

<!-- SUMMARY -->

## 1. The question and the angle

Many writers commit at once, on one branch and across branches. Every graph commit today
passes through one conditional put per branch: the next `__manifest` Lance version,
`_versions/{V}.manifest`, created only if absent. The angle studied here is a write-ahead
log in front of that publication: writers append small durable records with conditional
puts and a publisher folds them into `__manifest` later. The questions: what a commit
costs then and how many land per second; whether an acknowledged write can be visible the
moment its record is durable; what that does to snapshots, branches, merges, change feeds
and recovery; how ordering, conflicts, fencing, compaction, recovery and bounds work; and
why Omnigraph removed its earlier MemWAL experiments and whether Lance 13's MemWAL fits.

## 2. Where a commit's time goes today

### 2.1 The protocol, from the code

`Session::mutate` → `mutate_one_attempt` (`exec/mutation.rs`):

1. **Capture** (`open_write_txn`): accepted schema, branch identity, graph head, table pins.
   On the bound branch a freshness probe of `__manifest` (HEAD `v` and `v+1`) unless the
   process holds the branch's writer epoch (PR #1350).
2. **Execute and validate** against the pins; an insert into a keyed type is an upsert
   whose staging probes the table for the ids (`TableStore::stage_keyed_write`: an
   exact-id probe or, with no id BTREE, Lance's merge join over every uncovered fragment).
3. **Stage** outside the gates: data files written, one Lance transaction per table, no
   HEAD movement.
4. **Gates:** shared schema permit → branch gate → sorted table gates (`commit_all`).
5. **Revalidate** the complete authority (a probe, unless held).
6. **Detached commit** per table (`commit_staged_detached`): Lance writes the transaction
   file, then the manifest `_versions/d{random}.manifest` put-if-absent.
7. **Publish** (`GraphNamespacePublisher::publish_attempts` → `commit::overwrite`): build
   the next complete row set (copy-on-write of every live row), write the hot-row data file,
   the transaction file, then `_versions/{V}.manifest` put-if-absent: the commit point.

Lance 13 always writes a separate transaction file before the manifest
(`ManifestWriteConfig::disable_transaction_file` is crate-private), so any Lance commit is
at least two dependent round trips, and the `__manifest` publish with its data file is
three.

### 2.2 Measured: one commit, request by request

The instrument (section 3) traces one commit on `main` with 20 ms injected per request
(LOG.md, "One commit, traced"): **12 requests, 232 ms, about 10 sequential round trips**:

```
   0- 22  HEAD __manifest v, v+1          capture probe
  22- 45  GET  table manifest (ranged)     open the pin (index metadata)
  49- 71  HEAD __manifest v, v+1 | GET table data      probe beside staging | key probe
  74- 95  PUT  table data file             staging
  96-118  PUT  table txn                   detached commit   ┐
 119-141  PUT-IF-ABSENT table manifest                       │ under the
 141-165  (CPU)                                              │ branch gate
 165-187  PUT  __manifest data file        publish           │ (~135 ms)
 187-209  PUT  __manifest txn                                │
 210-232  PUT-IF-ABSENT __manifest V       commit point      ┘
```

### 2.3 Why many writers on one branch collapse

The branch gate is held for about 6 round trips per commit, so one branch publishes at most
about 7 commits/s at 20 ms per request, one commit per CAS. It is worse than that: every
publication moves the head, which sends every other prepared write on the branch back
(`ReadSetChanged`), and the second attempt prepares again *under* the writer gates
(`PreEffectReprepares`), holding the branch for preparation plus commit. At 64 writers no
commit landed in a 10 s window (PRs #783/#784 measured the same shape on RustFS +30 ms).

### 2.4 The physical floor

A publication on one branch needs one conditional put that must follow the previous one.
With L the round trip of that put, one commit per put gives at most 1/L per branch (50/s
at 20 ms, 10–20/s at the 50–100 ms S3 Standard writes SlateDB and Delta report, 100–200/s
on S3 Express). Two things raise it: putting B commits in each publication (B/L), and
making the serialized section as short as one put. Closed-loop throughput is N / latency,
so the per-commit latency under load, not the capacity, is what a fixed set of writers
sees.

## 3. The instrument

`crates/omnigraph/tests/write_throughput.rs`, an ignored `instrument:` test in its own
binary (`--features dst`). N closed-loop writers over clones of one `Session` (8 Tokio
worker threads), each inserting one `Item` node with a unique key per operation; `same`
mode puts every writer on `main`, `branches` gives each writer its own branch. Backends:
`file` (a local directory, no injected latency; Lance's direct local I/O bypasses the seam,
so request counts there are partial) and `mem` (Lance's `shared-memory` provider behind the
engine's Lance-realm object-store seam with a fixed sleep per request; every request
counted; its commit handler is the `ConditionalPutCommitHandler` S3 uses). After every
cell the instrument folds every log, drops the in-process tails, reopens a fresh handle and
checks that every acknowledged insert is present exactly once per branch (`verified`).
Reported: commits/s, p50/p99/max service time, requests per commit by class and
operation, engine re-prepares, client conflicts, and per-batch phase times. Runner:
`research/write-throughput-wal/scripts/sweep.sh`; every number below has a JSONL record.
Caveats: closed loop (coordinated omission), injected constant latency (no tail, no
throttling), one process, and a table with no id index and no maintenance (section 6.3).

## 4. Baseline

`origin/main` `2c0941e0` plus the instrument (`results/baseline.jsonl`), and the same with
the writer epoch claimed (`results/baseline-claim.jsonl`, the best existing path).
Commits/s (p50 ms, p99 ms):

**One branch, mem 20 ms per request**

| N | baseline | baseline, epoch claimed |
|---|---|---|
| 1 | 4.3 (230, 233) | 4.7 (208, 212) |
| 4 | 2.4 (1391, 1609) | 2.9 (1183, 1356) |
| 16 | 1.3 (5621, 5912) | 1.7 (4858, 5214) |
| 64 | 0 commits in 10 s | 0 commits in 10 s |

**One branch per writer, mem 20 ms per request**

| N | baseline | baseline, epoch claimed |
|---|---|---|
| 1 | 2.3 (423, 426) | 2.3 (424, 427) |
| 4 | 5.6 (725, 813) | 5.5 (725, 919) |
| 16 | 16.5 (922, 1010) | 16.6 (915, 972) |
| 64 | 60.0 (1024, 1102) | 59.8 (1022, 1094) |

**Local files (no injected latency)**

| N | same branch | same, claimed | branches | branches, claimed |
|---|---|---|---|---|
| 1 | 30.8 (33, 50) | 30.7 (33, 47) | 29.7 (34, 57) | 28.7 (36, 50) |
| 4 | 24.5 (155, 220) | 24.7 (152, 281) | 78.6 (52, 84) | 75.7 (52, 84) |
| 16 | 24.5 (638, 826) | 24.6 (613, 836) | 169.2 (91, 190) | 154.2 (101, 175) |
| 64 | 21.4 (2487, 2808) | 19.9 (2494, 3168) | 175.7 (353, 502) | 170.7 (372, 435) |

Findings: one branch collapses as writers are added (section 2.3); separate branches scale
but each named-branch write costs 21.6 requests at N=1 (main: 9.2 claimed) rising to 50 at
N=64, from the single-entry non-bound coordinator cache (section 6.4); holding the epoch
helps only the bound branch.

## 5. Ideas tried

Every idea was measured with the same instrument (mem at 20 ms per request unless stated,
writer epoch claimed, 2 s warm-up, 10 s window, every cell verified). Raw files are named
in each row.

| # | Idea | Result (commits/s, p50) | Verdict |
|---|---|---|---|
| 0 | Hold the writer epoch (PR #1350, existing) | same branch N=1: 4.3 → 4.7, 230 → 208 ms; requests 13.2 → 9.2; still 0 commits at N=64 (`baseline-claim`) | Keep as the floor; not enough |
| 1 | Group commit, writers compose their own staged files (PR #785's shape) | N=16: 1.7 → 46.4 (330 ms); N=64: 0 → 58.3 (823 ms) (`gc`) | Keep the batching; composition caps at ~60/s (below) |
| 2 | + record log in front of `__manifest` | N=1: 4.7 → 7.0 (208 → 140 ms); N=4: 12.0 → 17.9; N=16: 46.4 → 56.6; N=64: ~60 (`gc-wal`) | Keep: the publish step drops from 91 to 23 ms |
| 3 | The 64-writer plateau of 1–2 | 6.3 cores busy at 62 commits/s; CPU per commit grows with fragment count (31 → 110 ms in one run) | Diagnosed: per-write id probe + one fragment per write |
| 4 | Leader stages the batch (one write per table per batch) | without log N=64: 147 (416 ms); with log N=64: **217 (281 ms)**, 0.3 cores (`gc-leader`, `gc-wal-leader`) | Keep: fixes the CPU plateau; the log adds +48% here |
| 5 | Per-branch authority cache + held-branch probe skip (non-WAL) | branches N=64: 60 → 148 (400 ms) (`branchcache`) | Keep: independent cross-branch win |
| 6 | + log and leader staging on named branches | branches N=1: 2.3 → 7.0 (423 → 140 ms); N=16: 16.5 → 111; N=64: 60 → **207** (`gc-wal-leader`) | Keep |
| 7 | Pipeline the batch: stage / commit-on-tip / record | same branch N=64: 217 → **286–292 (205 ms, p99 366)**; N=16: 56 → 66 (LOG.md) | Keep |
| 8 | Pipeline with ticket order and a depth semaphore | depth 2: 246, 3: 253, 4: 192 at N=64 | Discard: smaller batches; hand-over-hand batch formation is better |
| 9 | Crash recovery and cold reads from the log tail | test passes: 12 of 12 acknowledged rows visible to a fresh reader with 7 unfolded; the next owner folds them | Keep (correctness evidence) |

<!-- FINAL_TABLES -->

### 5.1 Ideas considered and not built, with the reason

- **A logical WAL with an in-memory row overlay (SlateDB/turbopuffer memtable).** It would
  make the acknowledgement one put with no table work, but every read path (scans, the
  topology index, vector and full-text search, hydration) would have to merge unfolded rows,
  the "fresh tier" RFC 0018/0026 already found to be the expensive part. The record design
  keeps table data in real Lance detached versions before the record, so reads need only
  snapshot resolution. Not built.
- **Acknowledge before validation, or before ordering** (per-writer unique record names
  ordered later, WarpStream lightning topics, LazyLog). Fast, but a fold could then reject
  an acknowledged write (uniqueness, cardinality, endpoints) and external consistency is
  lost: RFC 0026's dead-letter path. Rejected.
- **Per-table Lance MemWAL.** Rebuilds the per-dataset second coordinator (section 8).
  Rejected.
- **Pipelining record puts (several records in flight).** The record is one round trip and
  not the bottleneck once stage 2 is serialized; out-of-order completion needs a gap rule
  and fence records over several slots (SlateDB `max_parallel_writes`). Not worth it yet.
- **Committing each writer's detached table version before the gate.** Measured in #783:
  under same-branch contention each losing attempt writes a dead commit, 3.9–7.6× the table
  writes, to save ≤ 15% of the hold. Not repeated.
- **A smaller `__manifest` publication** (delta fragments instead of the copy-on-write row
  set, skipping the transaction file). Still ≥ 2 dependent Lance puts (the transaction
  file is mandatory through the public API) versus one record; this is the catalog-structure
  angle the other researcher owns. The record design subsumes it on the critical path.

## 6. The winning design: a record log in front of `__manifest`

The design that measured best is three mechanisms together. Each is useful alone; the log
is what makes the serialized section one round trip.

### 6.1 Group commit at the branch, staged by the leader

A write group per (graph root, branch) in the process (`db/group_commit.rs`, held by the
root's `WriteQueueManager`, so every handle on the root shares it; no spawned task).
Writers capture, execute and validate as today, then submit their validated pending rows.
Whoever holds the branch's leader lock takes everything queued as one batch (zero-delay
batching: the batch is whatever arrived during the previous one, PostgreSQL/RocksDB/
turbopuffer style). The leader:

- captures the current authority once (with the writer epoch held: no request);
- admits entries in arrival order: same branch incarnation and schema identity as the
  batch; every commit between the entry's captured head and the current head was
  published by this group (an in-memory effect log of the last 256 batches: produced head,
  parent, ids per table); ids disjoint from those commits, from batches in flight and from
  the batch so far. A refused entry gets `ReadSetChanged` and prepares again through the
  mutation's own bounded loop;
- stages the whole batch as one write per table pinned at the current head
  (`MutationStaging::absorb_pending` then the ordinary `stage_all`), so the id probe runs
  once per batch and a batch adds one fragment per table;
- commits each table detached and publishes once: **one graph commit per batch** (PR
  #785's decision, because many surfaces map one commit to one `__manifest` version); every
  admitted write is acknowledged with that commit.

Only insertion-only mutations with exact ids join (keyed inserts and all-new upserts, no
update, delete, overwrite, column patch, caller `--if-commit` head, unattached base, or
re-preparation under gates); everything else takes the ordinary path.

### 6.2 The record log (`crates/omnigraph-catalog/src/wal.rs`)

On a branch whose writer epoch this process holds, a head-advancing publication writes one
self-contained object instead of a `__manifest` Lance commit:

```text
{graph root}/__wal/{native ref of the branch life, or "main"}/{V:020}.rec
```

created with one conditional put (`put_if_absent`). The record holds the complete row set
`__manifest` version `V` will hold (the stored, packed `record` form, as Arrow IPC), plus
the cold registry rows the publication pushes, if any. Properties:

- **Same version numbers, same contention.** Record `V` *is* `__manifest` version `V`
  before it is folded. Two writers that captured the same head contend on the same object
  name, exactly as they contend on `_versions/V.manifest` today; a lost acknowledgement is
  resolved by reading the record back and comparing its bytes.
- **One request.** The commit point drops from three dependent puts to one, and the
  publication no longer reads `__manifest` (the process builds `V+1` from its in-memory
  tail).
- **Visible at once in the process.** The coordinator installs version `V` when the record
  lands (`ManifestCoordinator::version()` is the installed state's version; opens and
  refreshes overlay the in-process tail).
- **Visible to other readers through the tail.** A reader with no in-process log probes the
  record names above the latest `__manifest` version (one GET when caught up, `k+1` when `k`
  records are unfolded) and opens at the newest record (`read_storage_tail`, used by opens,
  refreshes and freshness probes).
- **Folded in the background, in order.** A fold task per branch writes `__manifest` version
  `V` from record `V` with the ordinary `commit::overwrite` (the same rows, so every history
  surface later sees exactly the versions it would have seen). A fold whose answer was lost
  reads the version back and compares heads.
- **Bounded.** An append waits while 64 records are unfolded
  (`OMNIGRAPH_WAL_MAX_UNFOLDED`), bounding the tail's length, the memory its rows hold and a
  cold reader's tail reads (Unity Catalog bounds unpublished commits at 50, SlateDB by
  bytes).
- **Recovered by the next owner.** A writer-epoch claim first folds every record storage
  holds above `__manifest`, then claims (the test `wal_recovery_and_cold_reads`: 7
  acknowledged, unfolded writes survive a crash, a fresh reader sees all 12 rows, the next
  owner folds them and continues).
- **Everything else folds first.** A publication that is not head-advancing (schema apply
  with its exact-version precondition, proposals, unattached snapshots, claims) first waits
  for the process's log to fold (`quiesce`), then publishes the ordinary way.

### 6.3 Pipelined batches

With the log the batch splits into three stages, each run in batch (ticket) order:
(1) stage the batch's rows at the published head (probe + data put); (2) recompose each
table's staged insert onto the previous batch's detached tip, held in memory, and commit it
detached; (3) append the record. Batch j+1 stages while batch j commits and logs; the next
batch forms when the current one enters stage 2. A failure in stage 2 or 3 starts a new
chain generation and refuses every batch built on the old one (their writers prepare
again). The path takes the shared schema permit through stage 3 and no branch gate: the
record's conditional create orders it with every other publication on the branch.

### 6.4 Cross-branch fixes the log relies on

Writes on a branch that is not the handle's bound branch went through one cache entry
behind one mutex held across a storage probe, so 64 branches serialized on it and evicted
each other's coordinator (a reopen, ~20–40 requests). The prototype keeps one slot per
branch (`AuthorityCache`), and treats a branch this process holds, at the version of its
own last publication, as current with no probe (PR #1350's rule, extended to non-bound
branches). These are independent of the log and help without it.

### 6.5 What the record costs and how many land per second

Per batch on one branch at 20 ms per request (measured): stage 1 ≈ 2 round trips (the id
probe's read of the newest fragment, the data put); stage 2 ≈ 3 (Lance reads the tip's
index metadata, then the transaction file and the manifest); stage 3 = 1 (the record). The
serialized part per batch is stage 2 + stage 3 without pipelining, and stage 2 with it.
Lance caches a committed version's index metadata only when the list is non-empty
(`io/commit.rs` ~line 1556), so on a table with any index stage 2 is 2 round trips; the
benchmark table has none. The `__manifest` fold costs what a publication cost before
(three puts, ~70–90 ms) but off the acknowledgement path, one per record.

### 6.6 Snapshots, branches, merges, change feeds, recovery

- **Snapshots** are versions, as today; a version in the tail is read from its record
  (complete rows) instead of `__manifest`. Commit ids and lineage are unchanged: every
  record is a graph commit with its normal `hb1.<block>.<slot>.<nonce>` id.
- **Branch create, delete and merge, schema apply, cleanup and history reads** (commit
  list, `from_commit`, merge-base search, change feed, collector roots) read `__manifest`
  versions. The prototype's rule is fold-to-quiescence first (RFC 0018's rule for branch
  operations); a production version makes the readers that cannot wait log-aware (most only
  need the tip, which the record holds).
- **Change feeds** order commits by version; a tail commit is a version like any other.
  The feed's per-commit diff compares consecutive versions' pins, which a record carries.
- **Recovery**: records are the durable acknowledgement; the next owner folds them before
  its claim; a record never names an effect that is not already durable (table effects are
  detached commits written before the record, as today).
- **The collector** must treat records as retention roots (their pins) and must not reclaim
  a record until its fold landed and no retained version needs it; the prototype keeps
  records forever.

## 7. Invariants

Group commit, leader staging, pipelining and the per-branch cache keep every hard
invariant in [invariants.md](../../docs/dev/invariants.md) as written (one publication per
batch; one captured authority per batch, with admission proving each entry's footprint
unchanged; detached effects unreachable until the publication; policy enforced at each
writer's `_as` door before it submits). Two refinements need saying plainly:

- **Invariant 3 (one coherent view)**: an admitted entry was validated against its own
  captured view, not the batch's; the admission rule proves nothing but insertions with
  disjoint ids landed in between, so the entry's validation still holds at the batch's head
  (PR #785 states the same amendment: "an entry's footprint is proven unchanged, not its
  whole view"). Constraints the rule cannot prove (non-key `@unique`, bounded `@card`,
  updates, deletes) keep the entry on the ordinary path.
- **Invariant 4 (a mutation publishes once)**: holds; several mutations share one
  publication and one graph commit. A receipt's commit then names the batch.

The record log is the part that changes an invariant's *text*, not its contract. As an
RFC-level proposal:

**Invariant 2 (one publication door).** Today: "A graph change becomes authoritative
through one `__manifest` publication containing every visible table pointer, accepted
schema contract and graph-lineage update." Proposed: "...through one publication of
version V of the branch: the conditional create of record V of the branch's log or, for a
publication that does not use the log, of `__manifest` version V. Either object alone holds
every visible table pointer, the accepted schema contract and the lineage update. The fold
copies record V into `__manifest` version V unchanged." It is still one door, one object,
all or nothing, and the version numbers and their CAS semantics are unchanged; what moves
is the object's representation (as RFC 0068 also proposes, there for every publication).

**Invariant 5 (crash convergence).** Pre-publication effects stay unreachable (detached
table commits, as today). The fold *follows* a publication, which writes.md currently
forbids ("nothing follows the CAS"). The argument: the fold produces derived state (a
checkpoint of a version that already exists in the record) under invariant 7, never
publication authority; a crash between a record and its fold leaves nothing to classify
(the record is complete and is read as the version), and the next owner finishes the fold
before its first other publication. RFC 0068 treats its checkpoints the same way.

**Invariant 12 (one source of truth).** Today `__manifest` is the only lineage. With the
log, authority for versions above the latest folded one is the branch's record tail; the two
never disagree because the fold copies the record, a reader prefers `__manifest` where both
exist only because they are equal, and the tail is bounded (64 records). This is a bounded
derived lag, not a shadow copy that can drift.

**Invariant 10 and the fencing support boundary.** The log relies on the writer epoch: only
a process holding the branch's epoch appends records, and every head-advancing publication
on a held branch goes through its log. The prototype's claim folds the previous owner's
records and then claims, but does not yet write a fencing record: a previous owner that is
still alive could append record V+1 after the new owner folded V and claimed at V+1 in
`__manifest`. SlateDB and Lance MemWAL close exactly this gap by having the new writer
occupy the next log slot with a fence before replay (`ShardWriter::open` writes a sentinel;
SlateDB puts a zero-byte object at the next WAL id), so the old writer's next append
collides and it learns it is fenced. A production version must do the same (claim → fence
record at the next free version → fold up to the fence → publish), and readers must ignore
records whose epoch is below the newest claim.

**Deny-list: "a custom WAL ... already owned by Lance".** Lance owns per-dataset
transactions; the graph's multi-dataset commit journal is Omnigraph's (today the
`__manifest` dataset). The record changes its representation rather than adding a second
log, the argument RFC 0068 makes. Lance 13 also ships the primitive a production version
should use: `WalAppender` / `WalTailer` / `ShardManifestStore` are dataset-free and give an
epoch-fenced, put-if-absent, gap-free Arrow-IPC log at any prefix (notes/memwal.md).

**Acknowledgement after durable graph visibility.** Holds for every reader that is
log-aware: the process's own reads (installed view), and other processes' opens, refreshes
and freshness probes (they read the tail). It does not hold for a reader that opens
`__manifest` versions directly without the tail (history surfaces, older binaries); the
prototype folds before those operations, and a storage stamp would refuse older binaries.

## 8. Lance MemWAL

Full notes with file:line citations: [notes/memwal.md](notes/memwal.md).

**Why Omnigraph removed it.** RFC 0018 (Lance 8) and RFC 0026 put a per-table MemWAL
(shard writer, memtable, flushed generations) in front of ingestion and acknowledged on WAL
durability; PR #449 removed it (2026-08-06, −152k lines, 13 unreleased manifest formats).
The stated reasons: per-dataset durable acceptance did not remove the graph commit, it added
a second coordinator whose fold still paid it (graph-level validation, a token ledger,
replay, fold, correction, lifecycle), with one resident writer and one exclusive fold for
the whole root; and acknowledging *before* graph validation forced dead letters, strict
blocks, terminal tokens and correction machinery. The end-to-end benchmark behind "orders of
magnitude slower than direct load" was never checked in; measured pieces include a warm ack
of 38–49 ms on RustFS and fold manifest work growing from 46 reads at depth 8 to 334 at
depth 80.

**Lessons this design takes from it.** Validate before acknowledging (every entry here is
validated, admitted and published before its writer hears back; nothing is rejected after
an acknowledgement); keep one graph commit per publication rather than per-dataset
acceptance; do not serialize the whole root (one write group per branch); measure end to
end first (the instrument here).

**What Lance 13 offers.** One `put_if_absent` per WAL entry; `ShardWriter` group-commits on
a 100 ms tick or 10 MB (new since 9.0-rc.1); epoch fencing by a shard-manifest CAS plus a
fence sentinel at the WAL tip; `compacted_sstables` can ride an uncommitted merge-insert and
a detached commit; `LsmScanner` reads a base version plus SSTables plus same-process
memtables. All of it is per dataset and per branch: no multi-dataset atomicity and no
cross-dataset snapshot. The live spec is ahead of 13.0.0 and now marked experimental.

**Fit.** Per-table MemWAL would rebuild RFC 0026's second coordinator: a graph commit spans
several datasets and their markers would still need the catalog CAS to tie them together.
The reusable part is the log: `WalAppender`, `WalTailer` and `ShardManifestStore` work at
any prefix with no dataset, and give exactly the record log of section 6.2 (Arrow IPC
entries, put-if-absent slots, writer epoch in each entry, positional fencing). Two gaps to
cover: no fence check after a *successful* append (the consumer must filter entries by the
epoch `WalTailer::read_entry` returns), and the fence sentinel is written only by
`ShardWriter::open` (crate-private for raw appenders). A production record log should sit
on `WalAppender` with one shard per branch life, positions offset from the log's base
version.

## 9. What a production version needs

In order of dependency:

1. **Group commit with leader staging** (no format change; independent of the log): the
   batch shape, admission rules and composition guard of PR #785, plus leader staging
   (one staged write per table per batch). Needs the engine half of #785's composition
   guard, the admission matrix (strict-insert `KeyConflict` attribution to the one entry
   that collided instead of bouncing the batch; mixed batches; multi-table entries with
   edge endpoint validation), failpoints around the composed commits and the publication,
   and DST concurrent universes with the group on (every acknowledged write visible exactly
   once). The per-writer receipt names the batch's commit; the server's
   `Omnigraph-If-Graph-Commit` writes publish alone.
2. **The per-branch authority cache and the held-branch probe skip** (no format change):
   bound the cache (LRU by bytes: each coordinator holds lineage), and decide whether
   PR #1350's rule is safe for non-bound branches across handles in one process (the holder
   registry's landed version is per process, which is what the skip compares).
3. **The record log**, behind a storage stamp so older binaries refuse a graph with records:
   - record format on Lance's `WalAppender` (one shard per branch life; epoch per entry);
     deltas rather than complete rows (the prototype stores complete rows: simplest, and a
     reader needs only the newest record, but bytes grow with the catalog);
   - claim → fence record → fold to the fence → publish; readers ignore records below the
     newest claim's epoch;
   - every reader that resolves a branch head made log-aware (opens, refreshes, probes:
     done in the prototype) and every history surface either log-aware or folding first;
   - the collector: records as retention roots, record deletion only after the fold landed
     and nothing retained needs it, `cleanup` reporting a stuck fold;
   - backpressure surfaced (unfolded-record gauges, a typed refusal when the fold is
     stuck), fold failures observable, and a fold that resumes after a transient error;
   - the invariants text (section 7) and writes.md/recovery.md updated in the same change,
     as an RFC (it overlaps RFC 0068; the two should be reconciled into one: records as the
     door with `__manifest` kept as the checkpoint is a migration path to RFC 0068's end
     state that keeps every history surface working on folded state).
4. **Pipelining**, after the above: the staging witness of a batch built on a predecessor's
   tip should name that predecessor's head (today it names the published head; a collector
   running mid-pipeline could judge the staging dead), and a stage-2 failure path proven by
   failpoints.
5. **Live maintenance** (RFC 0067 step 1): the id probe still scans every uncovered fragment
   once per batch; an index build or compaction that keeps up bounds it. These publications
   are row-preserving, so batches can be carried over them (the carry rule of
   [RFC: writes carried over row-preserving publications](../../docs/rfcs/2026-11-10-writes-carried-over-row-preserving-publications.md))
   instead of being refused by the effect-log horizon as the prototype does.

## Appendix: code map and commands

Prototype switches (environment, read once per process; all default off):
`OMNIGRAPH_GROUP_COMMIT=1` (group commit), `OMNIGRAPH_WAL=1` (record log; implies group
commit), `OMNIGRAPH_GROUP_STAGE=leader` (leader staging), `OMNIGRAPH_GROUP_PIPELINE=1`
(pipeline; needs the two before), `OMNIGRAPH_GROUP_DEPTH`, `OMNIGRAPH_WAL_MAX_UNFOLDED`.
The per-branch authority cache and the held-branch probe skip are always on in this branch.

| Piece | Where |
|---|---|
| Instrument and recovery test | `crates/omnigraph/tests/write_throughput.rs` |
| Group commit, leader staging, pipeline | `crates/omnigraph/src/db/group_commit.rs` |
| Batch entry from a mutation | `crates/omnigraph/src/exec/mutation.rs` (`mutate_one_attempt`) |
| Pending-row absorption, eligibility, ids | `crates/omnigraph/src/exec/staging.rs` |
| Insert composition on a later pin | `crates/omnigraph/src/table_store.rs` (`compose_pure_inserts`, `StagedWrite::pure_insert_ids`) |
| Record log, tail, fold, recovery, storage tail | `crates/omnigraph-catalog/src/wal.rs` |
| Publication through the log | `crates/omnigraph-catalog/src/publisher.rs` (`wal_tail`, `publish_attempts`, `prepare_claim`) |
| Version of an unfolded view, open/refresh/probe overlays | `crates/omnigraph-catalog/src/lib.rs` |
| Per-branch authority cache, held-branch skip | `crates/omnigraph/src/db/omnigraph.rs` (`AuthorityCache`, `revalidate_write_txn_inner`) |

Build and run (from the repository root):

```bash
CARGO_PROFILE_DEV_OPT_LEVEL=2 cargo test -p omnigraph-engine --features dst \
  --test write_throughput --no-run
OG_WT_CLAIM=1 OMNIGRAPH_WAL=1 OMNIGRAPH_GROUP_STAGE=leader OMNIGRAPH_GROUP_PIPELINE=1 \
  research/write-throughput-wal/scripts/sweep.sh final-wal "mem file" "same,branches" "1,4,16,64"
OMNIGRAPH_WAL=1 target/debug/deps/write_throughput-<hash> --ignored --nocapture \
  --exact wal_recovery_and_cold_reads
python3 research/write-throughput-wal/scripts/table.py mem:20:same baseline final-wal
```

`sweep.sh` runs only `concurrent_write_throughput` (`--exact`): the recovery test must not
run in the same process, because it pauses folds and drops every in-process log.
