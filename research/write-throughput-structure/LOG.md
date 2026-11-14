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

## Phase breakdown of the baseline (wt-timers = baseline + phase timers), S3 +30 ms

Same branch, 1 writer, 372 ms/commit: capture 33 (HEAD v / v+1), stage 72 (key-check GET + data
PUT), gate wait 0, revalidate 0.3 (the beside-staging probe served), detached commit 68 (txn PUT +
manifest PUT), publish 163 (publisher load 54 = `_versions` LIST; overwrite 106 = data PUT, txn
PUT, CAS PUT). Gate hold 232 ms ~ 7 round trips.
Same branch, 4/16 writers: 2.0 attempts per commit; the re-prepared attempt holds the writer gates
through capture, stage, detached, publish, plus the failed attempt's under-gate revalidation:
~450-500 ms gated per commit, hence ~2/s.
Separate branches: named-branch publisher load 215 ms (the held-dataset fast path is main-only:
`current_control_dataset` refuses a branch manifest, so each publish reopens main + branch
`__manifest`), under-gate revalidation 65 ms (not served by the early probe off the bound branch),
and with 4+ branches a 317 ms coordinator open on 0.75-1 of the commits (the shared slot).

## E: per-branch authority cache (results/E-s3.jsonl)

`merge_authority_cache` becomes a bounded map (64) of per-branch slots, each its own async mutex;
publication clones the slot's coordinator instead of taking it. S3 separate: 4w 4.27 -> 5.87,
16w 12.66 -> 22.47, 64w 26.67 -> 46.8 commits/s; requests/commit flat at ~21 (was 28-35). Same
branch unchanged. Verdict: keep (pure data-structure fix; memory bound by entries).
At 64 branches the publisher load still grows 220 -> 846 ms (each named publish reopens
`__manifest` from the root); addressed later by extending the held dataset to named branches.

## A: commutative carry-over (results/EA-s3.jsonl, EA2-s3.jsonl)

Insert-only mutation, written tables = node types w/o non-key @unique -> revalidation carries the
attempt over publications that touched other tables or only inserted into its tables
(`table_store::commuting_chain`), and `rebase_staged_write` (Lance `TransactionRebase`, key filter
intersection) moves the staged insert onto the current pin.
- EA (chain walk from storage): re-prepares 1.0 -> 0, but revalidation walks k links (HEAD+GET
  each) under the gate: 234 ms at 4w, 806 ms at 16w -> 16w throughput 2.0 -> 1.0. Discard as is.
- EA2 (+ in-process log of detached commits this process made, `table_store::detached_links`,
  immutable facts): 4w 2.0 -> 3.0, 16w 2.0 -> 3.0 (p99 8.0 s -> 5.5 s). 64w 1.67: lag > 32 links
  overflowed the storage-walk cap; raised the in-memory walk to the log size (1024).
- Local URI key bug: `file://` vs bare path made every local walk miss; normalized (EAB3).

## B: trusted in-process tip (results/EAB-s3.jsonl, EAB2-*.jsonl)

Bound branch: capture and revalidation use the installed coordinator (no probe; the early probe
removed), publisher first attempt starts from its own acknowledged dataset (no `_versions` LIST);
the CAS stays the authority, a stale tip costs one failed CAS and a retry with a fresh load.
- EAB S3 same: 1w 2.73 -> 3.47 (p50 369 -> 290 ms, 12.9 -> 8.0 requests), 4/16/64w -> 4.0.
- EAB2 (+ open the carried table through the read-handle cache, `open_pinned_for_write`):
  same 4/16w -> 4.6 commits/s, 7.9 requests/commit (the minimum: 3 manifest PUTs, 2 table PUTs,
  1 table PUT-create, 2 table GETs), gate hold 217 ms = detached 103 + publish 114.
- EAB2 local: same 4w 13.9 -> 25.1, 16w 14.5 -> 25.6; separate 16w 85 -> 118, 64w 98 -> 155.
  Same 64w regressed to 12.2 (0.39 re-prepares) = the URI bug; EAB3 64w -> 24.5.
- Local gate hold is now ~38 ms of CPU (O0 engine code): publish 33 (load 7.5 decode, overwrite
  16, ~9 building rows), detached 3.

Open question: detached commit under the gate went 68 -> 103 ms when rebased (no extra request
counted). Possibly store contention from concurrent ungated staging; not yet explained.
