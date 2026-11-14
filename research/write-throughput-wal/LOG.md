# WAL research log: many concurrent writes, far more commits per second

Branch: `research/write-throughput-wal` (fork `ragnorc/omnigraph`).
Started 2026-11-14 on a Modal dev box (2 vCPU reported by nproc, 64 GiB RAM, gVisor kernel).

Angle: a write-ahead log in front of `__manifest` publication.

## Timeline

- 2026-11-14: branch created from origin/main 2c0941e0.

## Instrument (fixed from here on)

`crates/omnigraph/tests/write_throughput.rs` (own test binary, `--features dst`, ignored
`instrument:` test). N closed-loop writers over clones of one `Session` (8 tokio worker
threads), each inserting one `Item` node with a unique key per op. Modes: `same` (all on
`main`) and `branches` (writer i on its own branch `b{i}`, created before the window).
Backends: `file` (tempdir, no injected latency; Lance's direct local I/O bypasses the
seam so counts there cover manifest/listing only) and `mem` (Lance `shared-memory`
provider behind the engine's Lance-realm object-store seam with a fixed sleep per
request; every Lance request counted; commit handler = `ConditionalPutCommitHandler`,
the S3 one). Build: `CARGO_PROFILE_DEV_OPT_LEVEL=2 cargo test -p omnigraph-engine
--features dst --test write_throughput --no-run` (deps are opt-level 2 already; this
makes the workspace crates opt-level 2 too). Runner: `scripts/sweep.sh <label>`.
Window: 2 s warm-up, 10 s measured. Closed loop: commits/s = N / latency.

## Baseline (origin/main 2c0941e0 + instrument), 2026-11-14

Raw: `results/baseline.jsonl`.

| backend | mode | N | commits/s | p50 ms | p99 ms | req/commit | reprepares/commit |
|---|---|---|---|---|---|---|---|
| mem 20 ms | same | 1 | 4.3 | 230 | 233 | 13.2 | 0 |
| mem 20 ms | same | 4 | 2.4 | 1391 | 1609 | 33.3 | 1.17 |
| mem 20 ms | same | 16 | 1.3 | 5621 | 5912 | 61.6 | 2.23 |
| mem 20 ms | same | 64 | 0.0 | - | - | - | (no commit in 10 s) |
| mem 20 ms | branches | 1 | 2.3 | 423 | 426 | 21.6 | 0 |
| mem 20 ms | branches | 4 | 5.6 | 725 | 813 | 35.8 | 0 |
| mem 20 ms | branches | 16 | 16.5 | 922 | 1010 | 47.2 | 0 |
| mem 20 ms | branches | 64 | 60.0 | 1024 | 1102 | 50.4 | 0 |
| file | same | 1 | 30.8 | 33 | 50 | 7.0* | 0 |
| file | same | 4 | 24.5 | 155 | 220 | 15.6* | 1.02 |
| file | same | 16 | 24.5 | 638 | 826 | 16.5* | 1.07 |
| file | same | 64 | 21.4 | 2487 | 2808 | 20.5* | 1.30 |
| file | branches | 1 | 29.7 | 34 | 57 | 14.0* | 0 |
| file | branches | 4 | 78.6 | 52 | 84 | 19.3* | 0 |
| file | branches | 16 | 169.2 | 91 | 190 | 21.1* | 0 |
| file | branches | 64 | 175.7 | 353 | 502 | 23.7* | 0 |

(*) file counts see only the hooked manifest/listing traffic.

### One commit, traced (mem, 20 ms/request, 1 writer, main)

12 requests, 232 ms, about 10 sequential round trips:

```
   0- 22  HEAD __manifest v, v+1          capture freshness probe (open_write_txn)
  22- 45  GET  table detached manifest     open the table pin
  49- 71  HEAD __manifest v, v+1  ||  GET table data file    beside-staging probe || strict-insert key probe
  74- 95  PUT  table data file             staging (outside gates)
  96-118  PUT  table _transactions/*.txn   detached commit (under branch gate)
 119-141  PUT-IF-ABSENT table _versions/d*.manifest
 141-165  (no request: CPU / control plane)
 165-187  PUT  __manifest data file (hot rows)       publish
 187-209  PUT  __manifest _transactions/*.txn
 210-232  PUT-IF-ABSENT __manifest _versions/V.manifest   <- the commit point (version CAS)
```

Under the branch gate: ~6 round trips + ~24 ms (≈135 ms) -> the serial ceiling for one
branch is ~7/s with one commit per CAS, before any preparation is put under the gates.

### Findings

- Same branch collapses with writers: the head moves on every publish, so an
  optimistically prepared write is sent back and re-prepared under the writer gates
  (`PreEffectReprepares`), holding the branch for preparation + commit (~11 RTT). At
  64 writers no commit lands in 10 s at 20 ms (#783/#784 measured the same shape).
- Cross-branch scales but named-branch writes cost more requests than main (21.6 vs
  13.2 at N=1) and grow with the branch count (50 at N=64): the one-entry non-bound
  coordinator cache (`merge_authority_cache`) thrashes across branches, so most
  writes reopen their branch's coordinator from storage. A per-branch cache is a
  cheap, non-WAL fix (noted, not pursued here).
- Lance 13 always writes a separate `_transactions/*.txn` before the manifest
  (`ManifestWriteConfig::disable_transaction_file` is private), so every Lance commit
  is >= 2 dependent round trips and the `__manifest` publish is 3 (hot-row data
  file, txn, manifest). Only a self-contained one-object record makes the commit point
  one round trip.

## Baseline with the writer epoch claimed (`OG_WT_CLAIM=1`), 2026-11-14

Raw: `results/baseline-claim.jsonl`. Holding the epoch (PR #1350) skips the bound
branch's head probes: same-branch requests per commit 13.2 -> 9.2, latency 230 -> 208 ms
at N=1 (the probes overlapped other requests). Named branches are not the handle's bound
branch, so nothing changes there. Same branch still collapses: 4.7 / 2.9 / 1.7 / 0.0
commits/s at N = 1/4/16/64 (mem 20 ms). This is the bar for every prototype below (all
prototypes run claimed).

## Prototype 1: group commit (`OMNIGRAPH_GROUP_COMMIT=1`), 2026-11-14

`crates/omnigraph/src/db/group_commit.rs` + `table_store::compose_pure_inserts`. A write
group per (root, branch) in the root's `WriteQueueManager` (no spawned task): writers
prepare and stage as today, submit an entry, and whoever holds the branch's leader lock
publishes everything queued as one batch (PR #785's shape): gates once, capture once,
admit by identity + effect-log horizon + disjoint ids, compose each table's raw staged
fragments into one insertion-only `Update` on the current pin (re-certified insert
absence), one detached commit per table, one publication, one graph commit per batch.
Only insertion-only effects with exact ids join; anything else takes the ordinary path.

## Prototype 2: group commit + record log in front of `__manifest` (`OMNIGRAPH_WAL=1`)

`crates/omnigraph-catalog/src/wal.rs` + hooks in `publisher.rs` and the coordinator.
On a branch whose writer epoch the process holds, a head-advancing publication writes
`{root}/__wal/{native ref|main}/{V:020}.rec` with one conditional create: the complete
row set of `__manifest` version V (Arrow IPC). Version numbers are unchanged (record V
*is* manifest version V, unfolded), so contention semantics are today's. The process
keeps the tail (tip version, tip rows, folded dataset, unfolded records) in memory; the
publisher builds V+1 from it with no `__manifest` read; the coordinator installs V at
once (`ManifestCoordinator::version()` = max(known_state.version, dataset version); opens
overlay the in-process tip). A background fold writes `__manifest` V from record V with
the ordinary `commit::overwrite`, in order; appends wait when 64 records are unfolded.
A new tail adopts records storage holds above `__manifest` (crash recovery). Any other
kind of publication on the branch first folds the log (`quiesce`).

Verification after every cell (all `true`): fold everything, forget the tails, reopen a
fresh handle, and check every acknowledged insert is present exactly once per branch.

### Results (raw: `results/gc.jsonl`, `results/gc-wal.jsonl`)

Same branch, commits/s (p50 ms):

| backend | N | baseline-claim | group commit | group commit + log |
|---|---|---|---|---|
| mem 20 ms | 1 | 4.7 (208) | 4.7 (208) | **7.0 (140)** |
| mem 20 ms | 4 | 2.9 (1183) | 12.0 (323) | **17.9 (189)** |
| mem 20 ms | 16 | 1.7 (4858) | 46.4 (330) | **56.6 (286)** |
| mem 20 ms | 64 | 0.0 | 58.3 (823) | 60.4 (925) |
| file | 1 | 30.7 (33) | 29.3 (34) | **40.4 (25)** |
| file | 4 | 24.7 (152) | 57.3 (66) | 54.6 (68) |
| file | 16 | 24.6 (613) | 77.1 (198) | 75.9 (197) |
| file | 64 | 19.9 (2494) | 55.6 (944) | 55.2 (1039) |

Separate branches: no change from either (one writer per branch -> batches of one); the
log is slightly worse there (non-bound branch captures still probe `__manifest`, which
now lags the tail).

Per-batch phases (mem 20 ms): the table detached commits take 45 ms at batch size 1
(2 RTT: txn + manifest) and grow with batch size; the publication takes 91 ms (group
commit, 3 dependent puts + CPU) vs 23 ms (one record put). Single-commit trace with the
log: 6 sequential RTT (table manifest GET, key-probe GET, data PUT | table txn PUT,
table manifest PUT-IF-ABSENT, record PUT-IF-ABSENT), 140 ms, vs 10 RTT / 232 ms baseline;
the previous record's fold overlaps the next write.

### The 64-writer plateau is CPU in preparation, not the commit point

At N=64 both prototypes sit at ~60 commits/s with p50 ~0.9 s; batches are only 11-23
entries, so most writers are still preparing. The process is CPU-bound: 6.3 cores busy
at 62 commits/s (~100 ms CPU per one-row insert). CPU per commit grows with the table's
fragment count within a run: 16 writers, 1.7 cores at ~3 s (31 ms/commit) vs 5.8 cores at
~18 s (110 ms/commit). Every insert adds a fragment and every insert's id probe
(`first_existing_id` / the upsert merge join) scans every fragment no BTREE covers, and
every Lance commit serializes a manifest listing every fragment. The benchmark table has
no id index and no live maintenance, so this grows without bound; it penalizes the faster
designs (they create more fragments per second). Next: stage one merged fragment per
batch in the leader.

## Prototype 3: the leader stages the batch (`OMNIGRAPH_GROUP_STAGE=leader`), 2026-11-14

Writers prepare and validate (capture, execute, `validate_staged_mutation`) but do not
stage; they submit their pending rows. The leader absorbs every entry's rows into one
`MutationStaging` pinned at the batch's head (`MutationStaging::absorb_pending`) and runs
the ordinary `stage_all` + `commit_all` under the gates it holds
(`WriteAttempt::with_admitted_gates`), then one publication. Effect: the id probe (merge
join over uncovered fragments) runs once per batch instead of once per write, and a batch
adds one fragment per table instead of one per write.

Raw: `results/gc-wal-leader.jsonl` (cells run by hand, see commands in this log),
`results/gc-leader.jsonl`. Same branch, mem 20 ms, claimed:

| N | gc (compose) | gc + log (compose) | gc + leader staging | **gc + log + leader staging** |
|---|---|---|---|---|
| 1 | 4.7 (208) | 7.0 (140) | 4.7 (208) | 7.0 (140) |
| 4 | 12.0 (323) | 17.9 (189) | 9.2 (417) | 13.9 (281) |
| 16 | 46.4 (330) | 56.6 (286) | 36.8 (413) | 55.9 (280) |
| 64 | 58.3 (823) | 60.4 (925) | 147.2 (416) | **217.4 (281), p99 332** |

CPU at N=64 fell from 6.3 cores (compose) to 0.3 cores (leader staging). Per batch:
tables ~115 ms (probe GET of the previous batch's fragment, data PUT, Lance txn PUT,
Lance manifest PUT) + publication 23 ms (record) vs 91 ms (`__manifest` commit). The
record's contribution in this regime: +48% commits/s, -32% p50 at N=64. In a closed
loop a writer waits ~2 batch cycles (queue ~120 ms + its own batch ~140 ms), so
commits/s ~= N / 280 ms: latency, not capacity, is now the limit. Single-batch trace: 5
sequential RTT in the batch + 1 RTT preparation (table manifest GET).

## Cross-branch: per-branch authority cache and the held-branch probe skip, 2026-11-14

The non-bound write path (`validated_cached_coordinator`) used ONE cache entry behind ONE
tokio mutex held across a storage probe: with 64 branches every capture serialized on it
and evicted the previous branch's coordinator (a reopen: ~20-40 requests). Prototype
changes: `AuthorityCache` with one slot per branch (`omnigraph.rs`); a non-bound branch
this process holds, at the version of its own last publication, is current with no probe
(PR #1350's rule extended to non-bound branches, both in the cached capture and in write
revalidation); with the log on, the publisher does not reread the branch ref on every
publication of a branch whose log it holds.

Raw: `results/branchcache.jsonl` (cache + probe skip only), `results/gc-wal-leader.jsonl`
(branches rows). Separate branches, mem 20 ms, claimed, commits/s (p50 ms):

| N | baseline | per-branch cache + held skip | + group commit + log + leader staging |
|---|---|---|---|
| 1 | 2.3 (423) | 2.9 (335) | **7.0 (140)** |
| 4 | 5.6 (725) | 11.6 (337) | **28.0 (140)** |
| 16 | 16.5 (922) | 46.4 (333) | **111.0 (141)** |
| 64 | 60.0 (1024) | 148.0 (400) | **206.9 (298)** |

A named-branch write with the log costs 9.2 requests (vs 21.6 baseline), the same as main.
Note: a shell pitfall cost one run: zsh does not word-split `$env` in `env $env cmd`; the
sweep script (bash) is the reliable runner.

## Crash recovery and cold reads (test `wal_recovery_and_cold_reads`), 2026-11-14

Prototype additions: a claim first folds every record storage holds above `__manifest`
(`wal::fold_storage_tail`, called from `prepare_claim`); a coordinator open, refresh and
freshness probe with no in-process log read the records storage holds above the latest
folded version (`read_storage_tail`: one GET per record plus one for the first absent;
`latest_version_with_log`). Test (mem, `OMNIGRAPH_WAL=1`): 5 writes folded, then 7 writes
with the fold paused (records only), then a "crash" (`crash_forget_all`: tails dropped
unfolded). A fresh handle sees 12 rows (5 folded + 7 from the log tail); the next owner's
claim folds the 7 records, then one more write gives 13. Passes, also with the pipeline.

## Prototype 4: pipelined batches (`OMNIGRAPH_GROUP_PIPELINE=1`), 2026-11-14

Three stages per batch, each in batch (ticket) order: (1) admission + one staged write per
table at the published head (probe + data PUT); (2) each table's staged insert recomposed
onto the previous batch's detached tip (`compose_pure_inserts` on the chain tip, held in
memory) and committed detached; (3) the record. Batch j+1 stages while batch j commits and
logs. No branch gate on this path (the schema permit is shared, held through the log
stage); the record's conditional create orders everything else. A failure in stage 2 or 3
starts a new chain generation and refuses every batch built on the old one. Admission
also checks ids of batches in flight.

Mem 20 ms, same branch, claimed: first version (hand-over-hand locks) 279/s p50 208 at
N=64; ticket order with a depth semaphore was worse (d2 246, d3 253, d4 192); ticket order
plus hand-over-hand batch formation (next batch forms when this one enters stage 2):
**N=64 291.6 and 285.6 commits/s (p50 205, p99 366), N=16 66.0 and 66.2 (p50 213)**.
Stage 2 measures ~66 ms = 3 RTT, not 2: Lance's commit loads the base version's index
metadata (`load_all_indices`, a ranged GET of the tip manifest) and a fresh detached tip is
not in the session cache. No public Lance API avoids it.

## Final sweep and ablations, 2026-11-14

A first attempt of the final sweep was contaminated: `sweep.sh` ran the binary without a
test filter, so the recovery test (added just before) ran in the same process, installed
the object-store decorator with zero latency and dropped every in-process log mid-run. Its
output was deleted; `sweep.sh` now runs `--exact concurrent_write_throughput`. Earlier
sweeps predate the recovery test and are unaffected.

Raw: `results/final-wal.jsonl`, `gc-wal-leader-final.jsonl`, `gc-leader-final.jsonl`,
`baseline-claim-lat{5,50}.jsonl`, `final-wal-lat{5,50}.jsonl`, `final-wal-recheck.jsonl`,
`pipeline-nowal*.jsonl`, `pipeline-wal-d{2,3}.jsonl`, `baseline-claim-indexed.jsonl`,
`final-wal-indexed.jsonl`; printed lines in `results/final-sweep.log`. Tables: REPORT.md
section 5.1. Highlights (commits/s, p50/p99 ms):

- mem 20 ms, one branch, final-wal: N=16 64.1 (220/379), N=64 279.6 (212/366); baseline
  claimed 1.7 (4858/5214) and 0.
- mem 20 ms, branches, final-wal: N=16 110.5 (142/155), N=64 197.3 (308/527); baseline 16.5
  and 60.0. Indexed table: 115.6 and 297.2.
- file, one branch, final-wal: N=16 315.5 (49/98), N=64 1181.9 (51/93); baseline 24.5 and
  21.4 (p99 826 / 2808). Locally the fastest is group commit + leader staging without the
  log (1361.7 at N=64): CPU-bound, the fold repeats each publication's work.
- 5 ms: final-wal 183.1 and 699.7 at N=16/64 (p99 137/139) vs 7.4 and 2.7 claimed.
- 50 ms: final-wal 27.8 and 129.7 (p99 791) vs 0 and 0 claimed.
- Pipeline ablation (mem 20 ms, one branch): without the log the hand-over-hand formation
  rule is pathological (stage 3, a ~104 ms `__manifest` publish, becomes the bottleneck and
  batches of 1.1 pile up in front of it: 9.8/s at N=16); with depth 2 it reaches 53.5 and
  211.2 at N=16/64 (p50 310 at 64). With the log, depth 3: 64.9 and 279.4 (p50 208). The
  log's share in the pipelined configuration: +32% commits/s, -33% p50 at N=64.

## Mixed workload test and a routing bug, 2026-11-14

`group_commit_mixed_workload_is_consistent`: 20 writers upserting 120 colliding keys
beside 4 updaters on one branch (5 ms). First run with the final configuration failed in
the final fold: after any failed publish attempt the writer-epoch holder withdraws its
recorded version, and `wal_tail` then routed the next head-advancing publication around
the log straight to `__manifest`, while the tail kept its stale tip; the next record
collided with that version in the fold (and its rows missed the direct publication's
changes). Fix: once the process holds a log of a branch, every head-advancing publication
goes through it; any other publication folds the log and drops it. After the fix: 5 of 5
runs pass (every acknowledged key exactly once, no errors beyond bounded conflicts). The
benchmark cells never hit a failed attempt; the final numbers were re-measured with the
fixed binary (`final-wal-recheck`: 63.1 and 279.0 at N=16/64).

Also fixed: a pipelined batch whose merged write is not insertion-only (an upsert met an
existing key) is refused before any chain tip moves, instead of failing in composition.

## Cold reads over the tail, fold lag, fencing by a claim record, 2026-11-14

- `wal_cold_read_cost` (20 ms, a reader with no in-process log: open + one count): first
  version read every record in turn, twice per open: k=63 unfolded cost 138 requests,
  2.98 s. A parallel window of HEADs bounded latency (350 ms) at ~140 requests. Final: one
  HEAD when caught up, else exponential probe + binary search, then GET only the newest
  record (complete rows): k=0 8 req 178 ms (no log: 9 req 178 ms); k=1 14 req 306 ms; k=8
  26 req 558 ms; k=32/63 36 req ~770 ms.
- Fold lag (`results/foldlag.jsonl`, final config, mem 20 ms): mean unfolded records at an
  append 1.0 (N=1), 1.6 (N=16, 64 one branch), 1.0 (branches); max 2.
- Fencing: a claim with the log on is the record at the next version (`prepare_claim` builds
  it on the tail, `put_claim_record` appends it). `Tail::append` on a held name: equal bytes =
  our lost ack; a record with a higher writer epoch = `WriterFenced`; lower/equal epoch = a
  previous owner's acknowledged record, adopted (tip advanced, folded) and the attempt is
  retried as `RowLevelCasContention`. Test `wal_claim_record_fences_the_previous_owner`
  (forges another process's claim record at tip+1): the old owner's next write is refused
  `WriterFenced`, the graph keeps 3 rows; passes with writer staging, leader staging and the
  pipeline. Group commit now fans a fenced batch out as typed `WriterFenced`. Recheck with
  claims as records (`final-wal-fenced`): one branch 7.1 / 63.4 / 285.0 at N=1/16/64,
  branches 7.0 / 110.0 / 195.5; mixed test 2 of 2.
