# Catalog structure and publication for concurrent writes

Research report, branch `research/write-throughput-structure` (fork `ragnorc/omnigraph`), based
on `main` at `2c0941e0` (2026-11-14). Angle: data structures, publication protocol and Lance
primitives. Group commit and a write-ahead log are covered by other work and are not prototyped
here. Raw numbers: `results/*.jsonl`. Running log with every intermediate measurement: `LOG.md`.

<!-- SUMMARY -->

## 1. Instrument

`crates/omnigraph/tests/write_throughput.rs`, an `#[ignore]`d instrument (`instrument:`), run by
`scripts/sweep.sh <binary> <label> <file|s3>`. It was written first and kept fixed; later
revisions only added output fields (phase timers, event counters, PUT bytes, a two-handle mode).

- N closed-loop writers over one `Omnigraph` handle (the server shape), each inserting one row
  per mutation with disjoint keys into node type `C{i % WT_TYPES}` (default one table).
- Same branch (`main`, the handle's bound branch) or one branch per writer.
- Sweep: writers 1, 4, 16, 64; 3 s warm-up, 15 s window, 32 commits of history before the run.
- Reported per cell: commits/s in the window, p50/p90/p99 service time of operations acknowledged
  in the window, the longest service time of any operation including the drain (starvation shows
  there), per-writer counts, engine re-prepares and carries, driver resubmits, and object-store
  requests per commit by plane (`__manifest`, `__history`, tables) and method.
- Requests are counted by a `WrappingObjectStore` installed through `QueryIoProbes` on every
  writer task. On S3 every request reaches it; on `file://` Lance's direct local reader/writer
  bypass it, so local counts are a lower bound.
- After the drain a fresh handle checks the exact row count of every (branch, table). Every cell
  reported here verified.
- Backends: `file://` on the box's disk, and S3 = RustFS 1.0.0-beta.12 (the CI image's version)
  behind toxiproxy 2.13.0 adding 15 ms each way (+30 ms per request), the setup of #784 and #785.
- Build: the repository's `cargo dev-test` profile (dependencies, including Lance, DataFusion and
  Arrow, at opt-level 2; workspace crates at opt-level 0) unless a table says otherwise. The box
  has 32 CPUs.
- The driver is closed-loop, so results are diagnostic, not claim-grade (RFC 0039 Rule 1).

## 2. Cost model

### Where a commit's time goes (baseline, one writer, S3 +30 ms, 372 ms per commit)

Phase timers on the write path (`omnigraph_core::instrumentation::phase_timers`):

| Phase | ms | Requests |
|---|---:|---|
| capture (`open_write_txn`) | 33 | HEAD `_versions/v`, HEAD `v+1` (one round trip) |
| execute | 0.1 | |
| stage (`stage_all`) | 72 | key-check GET, data file PUT |
| *gate wait* | 0 | |
| revalidate | 0.3 | the beside-staging probe served it (2 HEADs, overlapped with staging) |
| detached table commit | 68 | table `.txn` PUT, detached manifest PUT-if-absent |
| publisher load | 54 | LIST `__manifest/_versions` (freshness) |
| publisher overwrite | 106 | hot data file PUT, `.txn` PUT, `_versions/{v+1}.manifest` PUT-if-absent (the CAS) |
| **gate hold** | **232** | about 7 serial round trips |

12.9 requests per commit. The branch, table and schema gates are held from revalidation to the
end of the CAS.

### Why same-branch writers cannot beat one writer

Revalidation compares the complete captured head. Any publication on the branch therefore
invalidates every other prepared write, even a disjoint one. The write is sent back, and the
re-prepare holds the branch's writer gates from capture through publication. Under contention
each commit costs one failed attempt and one whole attempt under the gate, about 450-500 ms on
S3. So 4 to 64 writers give a flat ~2.07 commits/s on S3 and ~14.5 locally.

### Why separate branches scale sublinearly

- Every non-bound branch shares one coordinator cache slot. Branches evict each other, so each
  write reopens its branch coordinator at capture and again at publication.
- The slot's mutex is held across the freshness probe and the refresh.
- A named-branch publish always reopens `__manifest` from the root (215 ms), because the held
  acknowledged dataset is retained only for main.

Requests per commit grow from 20.5 (one branch) to 35 (64 branches).

### Physical floors

- One branch publishes through a put-if-absent on `_versions/{v+1}.manifest`. Without batching,
  the floor is one CAS round trip per commit per branch: ~33 commits/s at +30 ms.
- With Lance 13's commit path, a `__manifest` commit is two dependent PUTs: the `.txn` file, then
  the manifest. That chain is ~70 ms, so ~14 commits/s per branch.
- Writers on one table also chain detached table commits: `.txn` PUT plus manifest PUT, ~68 ms,
  so ~15 commits/s per table.
- Everything else (capture, staging, rebuilding the row set, the hot data file) can leave the
  serialized path.

<!-- RESULTS -->
