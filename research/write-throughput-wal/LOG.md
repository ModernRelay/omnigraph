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
