# A write-ahead log for concurrent writes: research report

**Branch:** `research/write-throughput-wal` (fork `ragnorc/omnigraph`), based on
`origin/main` `2c0941e0` (Omnigraph 0.15.x, Lance 13.0.0).
**Date:** 2026-11-14. **Status:** research prototype, not for merge.
**Log of every step, command and raw number:** [LOG.md](LOG.md); raw results:
[results/](results/); notes from the source reading: [notes/](notes/).

## Summary

**Headline.** On one branch at 20 ms per object-store request, the best existing path
(writer epoch held) lands 1.7 commits/s with 16 writers (p99 5.2 s) and none at all with 64.
The prototype lands **81.5 commits/s at 16 writers (p99 337 ms) and 351.5/s at 64 (p99
331 ms)**. With one branch per writer at 20 ms, 16.6 and 59.8 commits/s (p99 0.97 s and
1.09 s) become **128.2 and 187.2** (p99 140 and 571 ms). On local files, one branch goes
from 24.6 and 19.9 commits/s (p99 0.84 s and 3.17 s) to **296 and 1159** (p99 100 and 89 ms)
with the full design, or 361 and 1362 with group commit and leader staging alone, which is
the faster choice on a local disk. At 5 ms per request one branch reaches 189 and 751
commits/s; at 50 ms, where the existing path lands nothing, 29 and 126. Every cell is
verified: after the run a fresh handle finds every acknowledged insert exactly once.

**Cost model.** A commit today is about 10 sequential object-store round trips, 6 of them
under the branch gate; its commit point is three dependent puts (the `__manifest` hot-row
data file, the transaction file Lance always writes first, then `_versions/V.manifest`). On
one branch every publication sends the other prepared writes back, and the second attempt
prepares under the gate, so throughput falls as writers are added (section 2).

**What won, in two parts.** (1) *Group commit with the leader staging the batch*: a write
group per branch takes every queued insertion-only mutation, stages one write per table for
the whole batch (one id probe, one fragment per batch instead of per write) and publishes
once; this alone takes 64 writers from 0 to 147 commits/s and fixes a CPU plateau the
writer-staged form hits. (2) *A record log in front of `__manifest`*: the publication of
version V becomes one conditional put of a record holding V's complete row set, read by
every log-aware reader at once and folded into `__manifest` V in the background; with
pipelined batches it adds +32% commits/s and −33% median latency at 64 writers (211 → 280
commits/s) and cuts a single writer from 208 to 140 ms. Prefetching each new table tip's
index metadata once per batch, instead of every writer reading it in validation, takes
the pipeline from 280 to 352 commits/s at 64 writers and a single writer to 119 ms. A per-branch authority cache plus
PR #1350's probe skip extended to held named branches is what makes separate branches scale.

**Where the log does not help.** On a local disk the system is CPU-bound and I/O is
sub-millisecond: group commit with leader staging alone is fastest there (1362 commits/s at
64 writers); the log and the pipeline add CPU (each record is also folded) and land 1318 and
1182. The log pays off in proportion to the object-store round-trip time.

**Visibility and invariants.** An acknowledged write is visible the moment its record is
durable: in the process through the installed view, and to other readers because opens,
refreshes and freshness probes read the records above `__manifest` (one GET when caught up).
A crash test shows 7 acknowledged, unfolded writes visible to a fresh reader and folded by
the next owner. The design keeps every hard invariant's contract but changes the text of
three (the publication door is "version V's record or `__manifest` V", the fold is derived
state that follows the publication, and the bounded tail is a second place versions live),
and fences an old owner SlateDB-style by making the epoch claim itself the next record
(implemented and tested); argued RFC-style in section 7. It overlaps RFC 0068 (records as the door) and is a migration path to it that
keeps `__manifest` as the checkpoint every history surface already reads.

**MemWAL.** Omnigraph removed RFC 0018/0026 because per-dataset MemWAL added a second
coordinator in front of the graph commit and acknowledged writes before graph validation.
This design validates before acknowledging and keeps one graph commit per batch. Lance 13's
per-table MemWAL still does not fit a multi-dataset commit, but its dataset-free
`WalAppender` is the right primitive for the record log (section 8).

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
throttling), one process, and a table with no id index and no maintenance unless stated
(the CPU plateau in section 5 and the indexed variant in 5.1).

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
| 7 | Pipeline the batch: stage / commit-on-tip / record | same branch N=64: 217 → **280–292 (205–212 ms, p99 366)**; N=16: 56 → 64–66; without the log the best pipeline is 211 at N=64 (`final-wal`, `pipeline-*`) | Keep |
| 8 | Pipeline with ticket order and a depth semaphore | depth 2: 246, 3: 253, 4: 192 at N=64 | Discard: smaller batches; hand-over-hand batch formation is better |
| 9 | Crash recovery and cold reads from the log tail | test passes: 12 of 12 acknowledged rows visible to a fresh reader with 7 unfolded; the next owner folds them | Keep (correctness evidence) |
| 10 | Fencing by making the claim the next record | the old owner's next write is refused `WriterFenced`; throughput unchanged (285 at N=64) | Keep |
| 11 | Cold reads: newest record only, exponential + binary probe | 63 unfolded records: 2.98 s / 138 requests → 0.77 s / 36; caught up: as today | Keep |
| 12 | Prefetch each new tip's index metadata once per batch | N=1: 140 → 119 ms; N=16: 66 → 82; N=64: 279 → **352 (p99 331)** (`final-wal-pf`) | Keep |
| 13 | Adaptive staging: a writer stages itself while its group is idle | threshold 2: N=4 16.1 → 14.7, N=64 350 → 266; threshold 4: N=64 → 63 | Discard: brings the per-write probe CPU back |
| 14 | Multi-table writes (node + edge per mutation) | N=1/16/64: 4.6 / 1.6 / 0 → 5.9 / 50.0 / **215.9** (`edge-*`) | Works; batches compose per table |
| 15 | Faults on records and folds (2% and 10% of conditional creates fail, half after writing) | 961 and 293 acknowledged, all present once; 4 and 23 failed, none landed; no phantom keys | Keep (correctness evidence) |

### 5.1 Final measurements

`final` = group commit + leader staging + record log + pipeline + the tip index prefetch
(`OMNIGRAPH_GROUP_PREFETCH=1`), with the per-branch cache (always on in this branch);
`final-wal` is the same without the prefetch. Commits/s (p50 ms, p99 ms); every cell
verified.

**One branch, mem 20 ms**

| N | baseline | baseline, epoch claimed | final-wal | final |
|---|---|---|---|---|
| 1 | 4.3 (230, 233) | 4.7 (208, 212) | 6.9 (141, 145) | 8.3 (119, 123) |
| 4 | 2.4 (1391, 1609) | 2.9 (1183, 1356) | 13.9 (276, 302) | 16.2 (264, 283) |
| 16 | 1.3 (5621, 5912) | 1.7 (4858, 5214) | 64.1 (220, 379) | **81.5 (196, 337)** |
| 64 | 0 in 10 s | 0 in 10 s | 279.6 (212, 366) | **351.5 (193, 331)** |

**One branch per writer, mem 20 ms**

| N | baseline | baseline, epoch claimed | final-wal | final |
|---|---|---|---|---|
| 1 | 2.3 (423, 426) | 2.3 (423, 427) | 7.0 (141, 145) | 8.3 (120, 125) |
| 4 | 5.6 (725, 813) | 5.5 (724, 919) | 26.6 (143, 186) | 32.8 (120, 141) |
| 16 | 16.5 (922, 1010) | 16.6 (915, 972) | 110.5 (142, 155) | **128.2 (122, 140)** |
| 64 | 60.0 (1023, 1102) | 59.8 (1022, 1094) | 197.3 (308, 527) | **187.2 (306, 571)** |

**One branch, local files**

| N | baseline | baseline, epoch claimed | final | best local: group commit + leader staging, no log |
|---|---|---|---|---|
| 1 | 30.8 (33, 50) | 30.7 (33, 47) | 37.2 (26, 39) | 33.8 (29, 39) |
| 4 | 24.5 (155, 220) | 24.7 (152, 281) | 63.3 (59, 113) | 84.3 (40, 97) |
| 16 | 24.5 (638, 825) | 24.6 (613, 836) | **295.6 (52, 100)** | 361.2 (38, 80) |
| 64 | 21.4 (2487, 2808) | 19.9 (2494, 3167) | **1158.9 (55, 89)** | 1361.7 (41, 86) |

**One branch per writer, local files**

| N | baseline | baseline, epoch claimed | final |
|---|---|---|---|
| 1 | 29.7 (34, 57) | 28.7 (36, 50) | 36.0 (28, 39) |
| 4 | 78.6 (52, 84) | 75.7 (52, 84) | 100.8 (37, 81) |
| 16 | 169.2 (91, 190) | 154.2 (101, 175) | 205.1 (75, 123) |
| 64 | 175.7 (353, 502) | 170.7 (372, 435) | 252.7 (237, 415) |

**Latency sensitivity** (`final-wal` rows have no prefetch; the `final` cells were run at
16 and 64 writers)

| N | 5 ms: epoch claimed | 5 ms: final-wal | 5 ms: final | 50 ms: epoch claimed | 50 ms: final | 50 ms branches: final |
|---|---|---|---|---|---|---|
| 1 | 13.5 (73, 78) | 19.2 (51, 58) | - | 2.0 (477, 479) | - | - |
| 4 | 8.6 (449, 465) | 38.1 (103, 135) | - | 1.0 (2736, 2738) | - | - |
| 16 | 7.4 (1796, 1935) | 183.1 (84, 137) | 188.7 (84, 154) | 0 in 10 s | 29.0 (468, 784) | 57.6 (268, 285) |
| 64 | 2.7 (7123, 7159) | 699.7 (92, 139) | 751.2 (85, 136) | 0 in 10 s | 126.3 (469, 783) | 208.6 (277, 585) |

**Ablation: what each piece contributes (one branch, mem 20 ms)**

| N | group commit (writers stage) | + log | leader staging | + log | pipeline, no log (depth 2) | pipeline + log |
|---|---|---|---|---|---|---|
| 1 | 4.7 (208, 215) | 7.0 (140, 144) | 4.7 (206, 209) | 7.0 (141, 144) | - | 6.9 (141, 145) |
| 4 | 12.0 (323, 329) | 17.9 (189, 288) | 9.2 (416, 426) | 13.4 (280, 326) | - | 13.9 (276, 302) |
| 16 | 46.4 (330, 512) | 56.6 (286, 306) | 36.8 (417, 422) | 52.8 (282, 326) | 53.5 (303, 468) | 64.1 (220, 379) |
| 64 | 58.3 (823, 1799) | 60.4 (925, 1593) | 147.2 (416, 422) | 217.6 (283, 294) | 211.2 (310, 468) | 279.6 (212, 366) |

The log's own share: −33% latency for a lone writer; +22% (writers stage), +48% (leader
staging) and +32% (pipelined) commits/s at the concurrency where each configuration peaks.
Leader staging trades latency at low concurrency (N=4: 17.9 → 13.9 with the log) for the
CPU headroom that makes N=64 possible; an adaptive choice (writers stage when the group is
idle) is future work.

**Indexed table** (`OG_WT_INDEXED=1`: one seed row and the id BTREE `ensure_indices` builds,
the production shape): one branch is unchanged (final-wal 7.2 / 14.1 / 63.3 / 279.6
commits/s at N = 1/4/16/64); one branch per writer improves to 115.6 at 16 and **297.2 at
64** (p99 376 ms): the BTREE probe is cheaper than the merge join each named-branch write
pays alone.

**Correctness instruments (all pass):** per-cell verification; `wal_faults_keep_every_acknowledged_write`
(2% and 10% of record and `__manifest` conditional creates fail, half after writing: every
acknowledged key present exactly once, failed ones never landed, no phantom keys); `wal_recovery_and_cold_reads`
(crash between records and fold); `wal_claim_record_fences_the_previous_owner` (a takeover
fences the old owner's next write); `wal_cold_read_cost` (a reader in another process sees
every acknowledged write over an unfolded tail of 0–63 records); `group_commit_mixed_workload_is_consistent` (20 writers
upserting 120 colliding keys beside 4 updaters, 5 ms latency, 5 of 5 runs: every
acknowledged key present exactly once, no errors beyond bounded conflicts). The mixed test
found one real bug, now fixed: after a failed publish attempt the writer-epoch holder
withdraws its recorded version, and the prototype then sent the next publication around
the log, which a later record collided with in the fold. Once a branch has a log in the
process, every head-advancing publication now goes through it.

### 5.2 Findings outside the log

- Each writer's validation planning (`validate_staged_mutation` → `plan_write` →
  `load_indices`) reads the pinned table version's index metadata, one ranged GET per write,
  because Lance has not cached it for a freshly committed detached version. Loading it once
  per batch in the background right after the commit stage (the prefetch) removes it from
  every writer's path: 279 → 352 commits/s at 64 writers, 140 → 119 ms for one writer.
- Lance caches a committed version's index metadata only when the list is non-empty
  (`io/commit.rs` ~1556); an index-less table pays one GET per commit for its successor.
- Under load, the pipeline's commit stage measures ~66 ms against 45 ms at one writer: the
  ceiling at ~15 batches/s is CPU in composing and serializing the table manifest, plus
  that GET on index-less tables.
- The non-bound branch cache serializing all branches behind one mutex held across a probe
  (section 6.4) is a cross-branch bottleneck worth fixing regardless of the log.

### 5.3 Long runs and live maintenance

Nothing in the benchmark compacts or indexes the table while it runs, and a batch adds one
fragment per table. Over longer windows at 64 writers (final, mem 20 ms) throughput falls
from 351.5 commits/s (10 s) to 291.8 (30 s) and 234.0 (60 s, p99 494 ms) while requests per
commit rise from 3.1 to 8.0 and the process stays under one core: the id probe reads ever
more uncovered fragments. That is RFC 0067's first throughput step, live maintenance. Run
alongside (an index build every 5 s on an indexed table, `OG_WT_MAINTAIN_SECS=5`), only 1 of
13 index builds landed: the others failed with "write authority 'graph_head:main' changed
during preparation", because the head moves several times a second and an index build
neither re-prepares nor is carried over. At this write rate maintenance must be carried over
insertion-only commits (a `CreateIndex` rebases over appends in Lance, the index simply
covering fewer fragments, which the coverage rules already allow), or be admitted by the
group as an exclusive entry that rebases at the head of the queue. Without it the design's
steady state is bounded by the growth of uncovered fragments.

### 5.4 Ideas considered and not built, with the reason

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
- **Visible to other readers through the tail.** A reader with no in-process log finds the
  newest record above the latest `__manifest` version (one HEAD when caught up; otherwise an
  exponential probe then a binary search, as SlateDB's reader does, since records are
  contiguous) and reads only that record, which holds the complete rows
  (`read_latest_record`, used by opens, refreshes and freshness probes). Measured cold open
  plus one row count at 20 ms (`wal_cold_read_cost`): caught up, 8 requests and 178 ms
  (today: 9 and 178); 1 unfolded record, 14 and 306 ms; 8, 26 and 558 ms; 32 or 63, 36 and
  ~770 ms (reading every record in turn, the first version, cost 138 requests and 2.98 s at
  63). Under load the fold keeps up: 1.0–1.6 unfolded records on average, at most 2, at 16
  and 64 writers (`foldlag.jsonl`), so a reader normally pays one or two HEADs and one GET.
- **Folded in the background, in order.** A fold task per branch writes `__manifest` version
  `V` from record `V` with the ordinary `commit::overwrite` (the same rows, so every history
  surface later sees exactly the versions it would have seen). A fold whose answer was lost
  reads the version back and compares heads.
- **Bounded.** An append waits while 64 records are unfolded
  (`OMNIGRAPH_WAL_MAX_UNFOLDED`), bounding the tail's length, the memory its rows hold and a
  cold reader's tail reads (Unity Catalog bounds unpublished commits at 50, SlateDB by
  bytes).
- **Fenced by a claim record, recovered by the next owner.** With the log on, a writer-epoch
  claim is itself the record at the next version, built on a tail that first adopts the
  records storage holds above `__manifest` (a previous owner's acknowledged, unfolded
  writes). The record CAS orders the claim with every append, so a previous owner that is
  still alive meets the claim at its next append, reads the higher writer epoch and is
  refused `WriterFenced` (test `wal_claim_record_fences_the_previous_owner`, all three
  staging modes); a previous owner's record that lands before the claim is adopted (it was
  acknowledged first) and the claim retries one version up. This is SlateDB's and Lance
  MemWAL's fence (occupy the next log slot), with the claim as the fence. Test
  `wal_recovery_and_cold_reads`: 7 acknowledged, unfolded writes survive a crash, a fresh
  reader sees all 12 rows, and the next owner's claim adopts and folds them.
- **One path per branch.** Once the process holds a log of a branch, every head-advancing
  publication on it goes through the log, the group's batches and the ordinary path alike:
  the record's conditional create is what orders them (the mixed-workload test found the
  bug of letting one through around the log).
- **Everything else folds first.** A publication that is not head-advancing (schema apply
  with its exact-version precondition, proposals, unattached snapshots, claims) first waits
  for the process's log to fold (`quiesce`), drops it, then publishes the ordinary way; the
  next head-advancing publication opens a new log over that version.

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
probe's read of the newest fragment, the data put); stage 2 = 2 round trips at one writer
(the transaction file, then the manifest: 45 ms in the trace) and ~66 ms under load; stage 3
= 1 (the record, 23 ms). The serialized part per batch is stage 2 + stage 3 without
pipelining (~90 ms) and stage 2 with it, so one branch takes ~15 batches/s; throughput then
scales with the batch size, which grows with the writers (21 entries per batch at 64
writers). Lance caches a committed version's index metadata only when the list is non-empty
(`io/commit.rs` ~line 1556), so an index-less table also pays one ranged GET per commit for
its successor; the indexed variant removes it at one writer but not the ~66 ms under load,
which is CPU. The `__manifest` fold costs what a publication cost before (three puts, ~70–90
ms) but off the acknowledgement path, one per record; per acknowledged write the requests
fall from 9–13 (baseline) to 3.5 at 64 writers (`final-wal`, mem, same branch).

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
on a held branch goes through its log. A claim is the record at the next version, so the
record CAS fences an old owner exactly as SlateDB's fence object and MemWAL's sentinel do: its
next append collides with the claim, it decodes the record and is refused with the typed
`WriterFenced` (implemented and tested). An old owner's record that landed before the claim
was acknowledged before the takeover, and the claim adopts it. Two production gaps remain:
a fenced process should clear its holder entry and drop its log (the prototype refuses every
later write instead), and readers should refuse a record whose epoch is lower than an earlier
record's (it cannot happen through the protocol, but a reader should not trust it).

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
   - fencing as prototyped (the claim is the next record; adoption of an earlier owner's
     records; `WriterFenced` on a higher epoch), plus clearing the holder and dropping the
     log when fenced, and readers refusing an epoch that goes backwards;
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
   once per batch; an index build or compaction that keeps up bounds it. Measured (5.3): at
   this write rate an index build almost never lands, because it is refused whenever the head
   moved during its preparation; it must be carried over insertion-only commits. These publications
   are row-preserving, so batches can be carried over them (the carry rule of
   [RFC: writes carried over row-preserving publications](../../docs/rfcs/2026-11-10-writes-carried-over-row-preserving-publications.md))
   instead of being refused by the effect-log horizon as the prototype does.

## Appendix: code map and commands

Prototype switches (environment, read once per process; all default off):
`OMNIGRAPH_GROUP_COMMIT=1` (group commit), `OMNIGRAPH_WAL=1` (record log; implies group
commit), `OMNIGRAPH_GROUP_STAGE=leader` (leader staging), `OMNIGRAPH_GROUP_PIPELINE=1`
(pipeline; needs the two before), `OMNIGRAPH_GROUP_PREFETCH=1` (tip index prefetch),
`OMNIGRAPH_GROUP_DEPTH`, `OMNIGRAPH_WAL_MAX_UNFOLDED`.
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
  OMNIGRAPH_GROUP_PREFETCH=1 \
  research/write-throughput-wal/scripts/sweep.sh final-wal-pf "mem file" "same,branches" "1,4,16,64"
OMNIGRAPH_WAL=1 target/debug/deps/write_throughput-<hash> --ignored --nocapture \
  --exact wal_recovery_and_cold_reads
python3 research/write-throughput-wal/scripts/table.py mem:20:same baseline final-wal
```

`sweep.sh` runs only `concurrent_write_throughput` (`--exact`): the recovery test must not
run in the same process, because it pauses folds and drops every in-process log.
