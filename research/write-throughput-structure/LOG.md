# Research log: catalog structure and publication for concurrent writes

Branch `research/write-throughput-structure` (fork `ragnorc/omnigraph`). Base: `origin/main` at
`2c0941e0` (2026-11-14). Box: 2 vCPU (nproc), 64 GiB RAM, gVisor Linux.

Angle: data structures, protocols and Lance primitives (two other agents cover group commit /
broad, and a WAL).

## 2026-11-14 — prior work read

Prior measurements (not mine; from docs/PRs, older bases):

- Shared-schema-gate RFC decision log (2026-09-28): one writer, one branch holds the gates
  7.3 ms locally (revalidation 0.6, detached commit 1.1, publication 5.6) and 670 ms at
  +30 ms/RTT (298, 85, 284 ms). 8 writers/1 branch cannot beat 1 writer: 31.7 commits/s
  local, 0.87 at +30 ms. Stale-attempt early-fail prototype: 0.40 -> 0.53 at +30 ms.
  8 branches: 154 commits/s local, 2.13 at +30 ms.
- PR #785 group-commit spike (base 97f293e8, RustFS + toxiproxy 15 ms each way):
  1 writer 1.83/s; 8 writers 0.87 -> 9.23; 16 writers 0.50 -> 13.63. Local: 1 writer 31.8,
  8 writers 21.5 -> 65.0. Batch of one = today's cost (3.05 `__manifest` writes/commit).
- PR #781 COW catalog publication: 1w/1b 21.8 -> 60.9 commits/s local; 4w/4b 22.2 -> 113.4.
- Issue #837 trace (warm single-row insert, DST unit model, main 0a62bb7b): 14 requests,
  makespan 12; requests 7-14 gated: HEAD v / HEAD v+1 (revalidation), PUT txn + PUT detached
  table manifest, LIST `__manifest/_versions` (publisher freshness), PUT `__manifest` data,
  PUT txn, PUT `_versions/{v+1}.manifest` (the CAS). Ungated: HEAD v/v+1 capture, HEAD+GET
  table manifest, GET key check, PUT table data file.
- Writer epoch (#1350): a holder skips the capture/revalidation probes on its bound branch.
- `concurrent-writes` instrument is deferred (benchmarks/deferred, does not build) — #895.

Initial hypotheses (to test):

- H1 the per-branch ceiling is the critical-section length (~8 serial object-store requests),
  not the single CAS; the physical floor is 1 CAS round trip per publication (per batch).
- H2 every publication invalidates every other prepared write on the branch (revalidation
  fails on any head move); precise footprint conflict detection + re-anchoring/rebase would
  let commuting writes pass without re-preparing.
- H3 pipelining: the next publication's payload (catalog data file, txn) and table detached
  commits can be prepared while the previous CAS is in flight; only the CAS needs the gate.

## 2026-11-14 — environment and instrument

- Latency backend: RustFS 1.0.0-beta.12 (musl, the CI image's version) on 127.0.0.1:9000 plus
  toxiproxy 2.13.0 adding 15 ms each way on 127.0.0.1:19000 (`scripts/start-services.sh`,
  `scripts/s3env.sh`). Local backend: `file://` on the box's scratch disk.
- Instrument: `crates/omnigraph/tests/write_throughput.rs` (`#[ignore]`, `instrument:`), run by
  `scripts/sweep.sh <binary> <label> <file|s3>`; fixed cell: writers 1,4,16,64 x same/separate
  branch, 3 s warm-up, 15 s window, 32 history commits, one table (`WT_TYPES=1`), closed loop.
  Requests counted by plane/method through a counting `WrappingObjectStore` on the
  manifest/history/table probes (complete on S3, a lower bound on file://). Rows verified exactly
  per (branch, table) on a fresh handle after the drain. Build: `cargo dev-test` profile (deps at
  opt-level 2, workspace crates at opt-level 0). Binaries are copied to /opt/bench/bins/<label>.
- Phase timers (process-wide atomics, `omnigraph_core::instrumentation::phase_timers`) added after
  the baseline run; the baseline binary does not have them.

## Baseline (main 2c0941e0 + instrument), results/baseline-{file,s3}.jsonl

| backend | mode | w | commits/s | p50 ms | p99 ms | max incl drain | req/commit | reprepares/commit |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| file | same | 1 | 14.80 | 68 | 91 | 95 | 7.0 | 0 |
| file | same | 4 | 13.93 | 298 | 410 | 415 | 15.5 | 0.99 |
| file | same | 16 | 14.53 | 1122 | 1442 | 1449 | 15.6 | 1.03 |
| file | same | 64 | 14.33 | 4025 | 5552 | 6259 | 16.3 | 0.88 |
| file | separate | 1 | 14.40 | 71 | 99 | 103 | 14.0 | 0 |
| file | separate | 4 | 43.46 | 93 | 137 | 173 | 19.1 | 0 |
| file | separate | 16 | 85.26 | 183 | 298 | 361 | 21.1 | 0 |
| file | separate | 64 | 97.85 | 644 | 836 | 1002 | 23.1 | 0 |
| s3 | same | 1 | 2.73 | 369 | 381 | 508 | 12.9 | 0 |
| s3 | same | 4 | 2.07 | 1936 | 2093 | 2093 | 24.0 | 1.0 |
| s3 | same | 16 | 2.07 | 7665 | 7899 | 8461 | 23.8 | 0.97 |
| s3 | same | 64 | 2.20 | 10374 | 17813 | 34633 | 21.9 | 0.03* |
| s3 | separate | 1 | 1.53 | 666 | 685 | 1038 | 20.5 | 0 |
| s3 | separate | 4 | 4.27 | 1008 | 1068 | 1506 | 28.7 | 0 |
| s3 | separate | 16 | 12.66 | 1185 | 1671 | 2304 | 33.8 | 0 |
| s3 | separate | 64 | 26.67 | 2504 | 5007 | 5012 | 34.8 | 0 |

(*) at 64 writers each op lasts longer than the window, so its re-prepare falls before the window.

Reading:
- Same branch is fully serialized: flat ~14.5/s local, ~2.1/s at +30 ms from 4 to 64 writers.
  Every commit costs a re-prepare of another writer, and since the re-prepare holds the branch's
  writer gates from capture through publication, the gated section per commit is a whole attempt
  (~11 serial requests on S3; 1 writer = 12.9 requests, 369 ms).
- One writer on S3: requests = manifest HEAD x4 (capture + revalidation probes), LIST x1 (publisher
  freshness), PUT x3 (data, txn, CAS); table GET x2, PUT x2, PUT-create x1 (detached commit).
- Separate branches: a named-branch write costs 20.5 requests on S3 (vs 12.9 on main) and grows to
  ~35 with 16-64 branches. Cause found in code: every non-bound branch shares ONE coordinator cache
  slot (`merge_authority_cache: Mutex<Option<(String, GraphCoordinator)>>`) and the mutex is held
  across the freshness probe and refresh; branches evict each other, so each write reopens its
  branch coordinator at capture and again at publication.
