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
