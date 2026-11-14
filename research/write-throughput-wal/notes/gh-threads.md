# GitHub threads on concurrent write throughput: reading notes

Read on 2026-11-14 with `gh` against `ModernRelay/omnigraph`. For each item I read the
body, the full diff (PRs), every conversation comment, every review, every inline
review comment, and every issue comment. Repository state when read: `main` at
`2c0941e0`; this branch (`research/write-throughput-wal`) at `6165f7fa`.

Conventions used below:

- Numbers are copied as the threads report them, with their conditions. When two
  threads give different numbers for what looks like the same cell, both are kept with
  their base commit; they come from different sessions and builds.
- Unless stated otherwise, throughput comes from the closed-loop `concurrent-writes`
  diagnostic (#746). It labels itself `driver: "closed-loop"`, `claim_grade: false`
  (RFC 0039 Rule 1: closed-loop drivers suffer coordinated omission). "Service time" is
  the time to serve one acknowledged write, not arrival latency.
- "+30 ms" means RustFS (S3-compatible) behind toxiproxy adding 15 ms each way, about
  30 ms per round trip.
- Several reviews below were posted from the `ragnorc` account (the account of the
  author of most of these PRs). They read as independent reviews, and they are
  attributed here to that account.

## Index

| Item | Kind | Title | State on 2026-11-14 | Author | Dates |
|---|---|---|---|---|---|
| #746 | PR | bench: concurrent-writes throughput diagnostic | merged (`70abf65e`) | ragnorc | 2026-09-18 → 09-20 |
| #754 | PR | RFC: shared schema gate and the write critical section (#643) | merged, docs only | ragnorc | 09-18 → 09-25 |
| #781 | PR | perf: use copy-on-write for catalog publication | merged | azimafroozeh | 09-27 → 09-27 |
| #783 | PR | engine: shared schema gate and the write critical section (RFC 2026-09-18) | merged (`8bc9aa5f`) | ragnorc | 09-28 → 09-28 |
| #784 | issue | performance: same-branch writers starve under object-store latency | open | ragnorc | 09-28 |
| #785 | PR | docs(rfc): group commit, with the Lance composition guard | open; RFC front matter `status: draft`, `implementation: not-started`; GitHub `mergeable: CONFLICTING`; `PR Title` check failing | ragnorc | 09-28, last update 11-08 |
| #837 | issue | performance: capture a write from the process's own last publication instead of re-reading __manifest | open; labels `accepted`, `feature`, `P-high`, `performance` | ragnorc | 10-01 |
| #895 | issue | bench: a GQT benchmark executor that measures concurrent overlap, so concurrent-writes and concurrent-merges can return | open; `accepted`, `feature`, `P-medium` | ragnorc | 10-08 |
| #1018 | issue | rfc: make schema apply, branch create and delete, and cleanup safe across writer processes | open; `needs-triage` | ragnorc | 10-16 |
| #1350 | PR | feat(engine): add writer epoch fencing and the owner's probe skip | merged (`757ab0e9`) | azimafroozeh | 11-11 → 11-11 |

Order of events: #746 builds the instrument; #754 proposes the shared schema gate;
#781 replaces catalog publication with copy-on-write; #783 implements the shared gate and
measures the same-branch ceiling; #784 reports starvation; #785 proposes group commit and
reports a spike; #837 proposes the single-writer capture lever; #894 (not in this list)
moves the instrument to `benchmarks/deferred/`; #895 asks for a replacement executor;
#1018 documents the multi-process gaps; #1350 adds the writer epoch and the probe skip.

---

## #746 — `concurrent-writes` throughput diagnostic (merged 2026-09-20)

### What it adds

A `concurrent-writes` scenario in the engine's old diagnostic benchmark harness
(`cargo bench -p omnigraph-engine --bench scenarios`). It measures sustained commits per
second per branch under N concurrent writers, per-op service-time percentiles, and
logical object-store call counts split into planes (Lance manifest plane, Lance table
plane, engine control plane). It is the instrument RFC 0067's "What to measure before
promising numbers" section asked for (compact vs fragmented manifest, local FS vs an
S3-compatible store). It landed on `main` so the same code could measure the sidecar-era
engine and the detached-commit branch (#744).

No engine behavior change. It adds instrumentation only:

- `crates/omnigraph/src/exec/mutation.rs`: one call to
  `crate::instrumentation::record_mutation_reprepare()` inside `Omnigraph::mutate`'s
  pre-effect reprepare loop (the loop that replays a `ReadSetChanged`).
- `crates/omnigraph/src/instrumentation.rs`: `ProbedStores` (the distinct Lance
  `ObjectStore`s a probe plane opened datasets on), new `QueryIoProbes` fields
  `mutation_reprepares`, `manifest_stores`, `table_stores`, `record_probed_store`,
  `record_mutation_reprepare`, and `open_dataset` registering the store behind each probed
  open. Totals are read from each store's own `io_tracker`, because on `file://` Lance's
  direct local reader and writer never reach a `WrappingObjectStore`.

Files: `crates/omnigraph/benches/scenarios/concurrent_writes.rs` (new, 889 lines),
`crates/omnigraph/benches/scenarios.rs` (flags, usage, dispatch, `--tokio-workers`
runtime pinning), `crates/omnigraph/benches/scenarios/rfc023.rs` (age and layout controls
admit the scenario), `crates/omnigraph/tests/benchmark_scenario_contract.rs` (pins the
self-labels, probe coverage on every spawned worker, the measured-window boundary,
fresh-handle verification and refusal exit codes), `benchmarks/README.md`
("Concurrent-writes throughput diagnostics").

### Command line (as merged)

```bash
RUSTFLAGS= cargo bench --locked -p omnigraph-engine --bench scenarios -- \
  --scenario concurrent-writes --writers 8 --duration-secs 30 \
  --rows 256 --dims 8 --history-commits 64 --manifest-layout uncompacted \
  --runs 3 --out /tmp/concurrent-writes.jsonl
```

`RUSTFLAGS=` (empty) is part of the command: the workspace `.cargo/config.toml` otherwise
injects `--cfg tokio_unstable`. The record's `attestation` block records the cfg, enabled
engine features, `LANCE_MEM_POOL_SIZE`, `RUSTFLAGS` and the Tokio worker count.

Scenario flags (defaults in parentheses): `--writers N` (4), `--duration-secs W` (30),
`--warmup-secs W` (5), `--write-branches B` (1: all writers on `main`; B > 1: writers
round-robin over forks `cw-0..cw-{B-1}`; the first write on each fork pays the deferred
first-touch fork of `node:Chunk`, absorbed by warm-up unless `--warmup-secs 0`),
`--target-uri s3://…` (unset: local tempdir), `--keep-fixture`, `--no-probes` (A/B of
counting overhead), `--tokio-workers N`. Shared flags it uses: `--rows`, `--dims`,
`--seed`, `--runs`, `--history-commits N` (even, 0..256), `--retired-branches N` (0..32),
`--manifest-layout uncompacted|compacted`, `--memory-cap-mb`, `--out`. It refuses
`--cache-state warm`, a baseline arm, `--phase`/`--fixture-root`, and a non-`s3://`
target. With `--target-uri`, the child appends a unique `cw-{nanos}` segment, probes the
store with a write and delete first, exits 78 if the store is unusable, and deletes the
prefix afterwards unless `--keep-fixture`. Credentials come from the standard `AWS_*`
variables; the CI RustFS recipe works as is.

The +30 ms recipe, from #784's reproduction section:

```bash
docker run -d --name rustfs -p 9000:9000 -e RUSTFS_ACCESS_KEY=omnigraphci -e RUSTFS_SECRET_KEY=omnigraphci-secret rustfs/rustfs:1.0.0-beta.12 /data
docker run -d --name toxiproxy -p 8474:8474 -p 19000:19000 ghcr.io/shopify/toxiproxy:2.9.0
curl -s -X POST localhost:8474/proxies -d '{"name":"rustfs","listen":"0.0.0.0:19000","upstream":"host.docker.internal:9000"}'
for s in upstream downstream; do curl -s -X POST localhost:8474/proxies/rustfs/toxics -d "{\"type\":\"latency\",\"stream\":\"$s\",\"attributes\":{\"latency\":15}}"; done
aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket omnigraph-bench

AWS_ACCESS_KEY_ID=omnigraphci AWS_SECRET_ACCESS_KEY=omnigraphci-secret AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://127.0.0.1:19000 AWS_ENDPOINT_URL_S3=http://127.0.0.1:19000 \
AWS_ALLOW_HTTP=true AWS_S3_FORCE_PATH_STYLE=true \
RUSTFLAGS= cargo bench --locked -p omnigraph-engine --bench scenarios -- \
  --scenario concurrent-writes --writers 8 --write-branches 1 --duration-secs 30 \
  --rows 64 --dims 8 --history-commits 64 --manifest-layout uncompacted --runs 3 \
  --target-uri s3://omnigraph-bench/starvation --out /tmp/starvation.jsonl
```

### What one repetition does and measures

- One repetition is one measured child process. It builds a fresh `Chunk` fixture
  (`--rows`, `--dims`, age and layout controls), then runs `--writers` closed-loop tokio
  tasks over clones of ONE `Session` (one `Arc<Omnigraph>`, the server shape) for
  `--warmup-secs` plus `--duration-secs`.
- Workload: insert-only mutations with disjoint keys per worker, slug
  `cw-w{worker:02}-{seq:010}`, query
  `query put($slug: String, $v: Vector({dims})) { insert Chunk { slug: $slug, embedding: $v } }`.
  `Chunk.slug` is `@key`, so the engine stages this insert as an upsert (#785 points this
  out; it matters for batching designs).
- Retries: `Omnigraph::mutate` replays a `ReadSetChanged` itself, up to 32 times for an
  insert-only mutation (`MAX_PRE_EFFECT_REPREPARES`); counted as `reprepares`. A conflict
  that exhausts that loop reaches the driver, which retries it like a client and counts
  `authority_conflicts`. Both sit inside the op's service time. Any other error, any
  `KeyConflict`, or a verification mismatch fails the run.
- Setup, warm-up, verification and teardown are outside the window. After all clocks and
  counters are read, a fresh uncounted handle checks exact per-branch row counts (seeded
  plus acknowledged) and reads back sampled acknowledged keys.
- A watchdog kills the child at `warmup + duration + 120 s`.

Record fields (JSON line per run): `commits_per_sec` (= `ops_committed / window_us`),
`commits_by_second` (acknowledgements per second of the window, where plateaus and
stalls show), `window_us`, `drain_us` (time ops in flight at the stop took to finish),
`completed_in_drain`, `warmup_ops`, `service_time_us {min, mean, p50, p95, p99, max,
samples}` (nearest-rank, same formula as `omnigraph-bench`), `per_branch`, `per_worker
[{worker, branch, acked_ops, measured_ops, authority_conflicts}]`, `reprepares`,
`authority_conflicts`, `key_conflicts`, `errors`, `io {manifest {read_iops, read_bytes,
write_iops, write_bytes}, table {…}, probes {data_open_count, internal_open_count,
manifest_scan_count, …}, control_plane {read_text, exists, list_dir, write_text, delete,
…}, per_op {manifest_reads_per_commit, manifest_writes_per_commit, table_reads_per_commit,
table_writes_per_commit, reprepares_per_commit}}`, `attestation`, peak RSS before and
after, `verification`. `--no-probes` nulls `io` and `reprepares`.

Counter semantics stated by the PR: "Counters are logical calls, not physical requests";
the unit is "one range get or one put at the seam", so they are not the round trips
RFC 0067 counts and "must not be divided by them". The PR names `io.per_op` as "the axis
where a future group-commit change must show `manifest_writes_per_commit` drop below one".

Known blind spot (from #784): service-time samples include only ops whose start fell
inside the window. Starved writers whose ops finish in the drain do not appear in the
percentiles; per-writer counts (`per_worker[].measured_ops`) show them.

### Numbers reported in #746

None beyond "live smoke runs (`--writers 1` and `--writers 8`, local-fs) emit records
with exact verification passing; refusal paths exit 2/78".

### Reviews

None (no comments, no reviews, no inline comments).

### Status for reuse (checked 2026-11-14)

- Not runnable on `main`. #894 (`a951b0ea`, 2026-10-07) moved the harness to
  `benchmarks/deferred/crates/omnigraph/benches/scenarios/concurrent_writes.rs.disabled`
  (with `scenarios.rs.disabled`, `fixture_controls.rs.disabled`, `rfc023*.rs.disabled`,
  `child_protocol.rs.disabled`, and `tests/benchmark_scenario_contract.rs.disabled`).
  `omnigraph-engine` has no `[[bench]]` now; the original command exits 101 with "no
  bench target named `scenarios`" (verified in #895's comment).
- The preserved copy is byte-identical to the last runnable version: sha256
  `c1287fc4…` both in `benchmarks/deferred/sources.json` and for
  `git show b54f4709:crates/omnigraph/benches/scenarios/concurrent_writes.rs`.
  `b54f4709` is the parent of #894 and still has the `[[bench]] name = "scenarios"`
  entry. Running it there measures that old engine, not current `main`.
- The instrumentation the harness imports still exists and is public:
  `omnigraph::instrumentation::{QueryIoProbes, ProbedStores, CountingStorageAdapter,
  StorageReadCounts, with_query_io_probes, enabled_engine_cargo_features}` (now defined
  in `crates/omnigraph-core/src/instrumentation.rs`, re-exported by
  `crates/omnigraph/src/instrumentation.rs`). The reprepare probe call now lives in
  `crates/omnigraph/src/db/reprepare.rs`. `crates/omnigraph/tests/helpers/cost.rs::drain_probed_io`
  uses the same accounting as the harness. I did not attempt a port to `main`.
- `benchmarks/deferred/README.md` gives the condition for its return: "Concurrent writes
  require a benchmark executor that measures actual overlap" (tracked as #895).

---

## #754 — RFC: shared schema gate and the write critical section (merged 2026-09-25, docs only)

### What it proposes

`docs/rfcs/2026-09-18-shared-schema-gate.md`, resolving RFC 0067's open question #643,
the second step of RFC 0067's throughput path:

- The process-local schema gate (write-queue key `("__schema_apply__", None)`), which
  every writer, maintenance pass, branch control and read-view capture took exclusively,
  becomes a shared/exclusive lock (`tokio::sync::RwLock<()>` slot in `WriteQueueManager`,
  beside `export_gate`). Rule: "a pass that only reads the accepted contract/catalog view
  shares; a pass that can change which view is accepted excludes."
- The branch gate stays: same-branch writers still serialize from revalidation through
  publication. Per-table gates stay.
- Promotion moves after guard release (separately revertible). Later retired: #776 /
  detached-only tables removed promotion altogether (see #783).
- Nothing cross-process changes: durable sentinel, mono-branch refusal and manifest CAS
  remain the authority; the gate is an in-process contention structure (deny-list:
  "process-local locks presented as distributed writer fencing").
- Why the old rationale no longer binds: the exclusive gate guarded a mutation advancing
  a Lance HEAD before discovering an in-flight schema lock; under RFC 0067's detached
  staging no pre-publication effect moves a HEAD, so a misclassified site can at worst
  attempt a stale publish, which the CAS refuses.
- It is stated as the enabler for group commit: "under the current gates, writers never
  arrive at the publisher concurrently, so every batch would have size one".

### Numbers (RFC Motivation table)

Sequential runs, both engines, closed-loop, all writers on one branch
(`--write-branches 1`):

| regime | writers | commits/s (median) | service p50 | p95 |
|---|---|---|---|---|
| local FS, detached engine | 1 | 18.9 | 52 ms | 76 ms |
| local FS, detached engine | 8 | 17.2 (−9%) | 62 ms | 2.6 s |
| RustFS + 30 ms injected RTT, detached | 1 | 0.64 | ~1.5 s | 1.9 s |
| RustFS + 30 ms injected RTT, detached | 8 | 0.21 (−68%) | ~2.6 s | 4.5 s |
| RustFS + 30 ms injected RTT, sidecar-era main | 8 | 0.15 (−76%) | ~3.4 s | 3.9 s |

- Manifest reads per commit "double from ~95 to ~178 between w1 and w8 at 30 ms".
- A read could wait behind a writer's whole publish hold, "about 1.5 s per commit at
  30 ms RTT".
- #783 later added to this section: "The table's base commit and run date are not
  recorded"; the 2026-09-28 entries ran on `b14c22c5`, "whose one-writer rates differ
  (31.7 and 0.87 commits/s there against 18.9 and 0.64 here), so the two sets do not
  compare cell by cell".

### Evidence plan it set (acceptance bar for the implementation)

`concurrent-writes` at `--writers 8 --write-branches {1,8}`, local and RustFS +30 ms.
Success: w8×B8 scales toward B× the single-writer rate while w8×B1 stays branch-gate
bound; `authority_conflicts` falls at B > 1; the critical section shrinks to
revalidate-and-publish. `write_cost.rs` per-op counts byte-identical. A new DST arm racing
writers against a schema apply.

### Reviews

One: azimafroozeh approved on 2026-09-25 with an empty body. No other comments.

### Files

`docs/rfcs/2026-09-18-shared-schema-gate.md` (new), `docs/rfcs/0067-detached-table-commits.md`
(pointer), `docs/rfcs/README.md`.

---

## #781 — copy-on-write catalog publication (merged 2026-09-27)

### What it changes

Catalog (`__manifest`) publication accumulated fragments and deletion files under
merge-insert, so small writes read more storage as history grew. The PR replaces the
merge-insert with a copy-on-write rewrite of the publishing branch's live row set,
committed against the exact version read:

- New `crates/omnigraph-catalog/src/commit.rs::overwrite(dataset, pending, live_rows)`:
  rewrites `live_rows` minus the keys `pending` replaces, plus `pending`, as new files
  (`InsertBuilder` with `WriteMode::Overwrite`, `execute_uncommitted`), then
  `CommitBuilder::with_max_retries(0).with_skip_auto_cleanup(true)`. Lance then writes
  `_versions/{read_version + 1}.manifest` only if absent and never rebases, "so the
  version number is the put-if-absent CAS". Earlier versions stay readable (snapshots and
  lost-acknowledgement read-back unchanged).
- `crates/omnigraph-catalog/src/publisher.rs`: `merge_rows` calls `commit::overwrite`;
  `LoadedPublishState` carries `live_rows` from the publish scan (`state.rs`,
  `ManifestScan.live_rows`); `map_lance_publish_error` maps `CommitConflict`,
  `RetryableCommitConflict` and `TooMuchWriteContention` to the typed
  `RowLevelCasContention`; `PUBLISHER_RETRY_BUDGET = 5` retries around the version CAS,
  each attempt re-running `load_publish_state` and the expected-version pre-check.
- `docs/dev/lance.md` gains the "Catalog publication" fence (strict overwrite under zero
  retries is the version CAS; a Lance release that rebased overwrites under zero retries
  would break it), owned by `omnigraph-catalog`
  `stale_overwrite_loses_the_version_cas_without_replacing_the_winner` and
  `publisher_retry_vocabulary_is_the_version_cas`.
- `crates/omnigraph/tests/write_cost.rs`: the served-regime tripwire
  `internal_table_scans_grow_without_compaction` (`assert_grows`) becomes
  `internal_table_scans_are_flat_without_compaction` (`assert_flat`, slack 4): per-write
  `__manifest` reads stay flat without `optimize` because every publish leaves one live
  fragment.
- New ignored instrument `crates/omnigraph/tests/manifest_history_curve.rs`: cost of one
  fixed-live-row update as `__manifest` history grows (requests and bytes per stage, and
  retained `__manifest` bytes on disk).

### Numbers

Local `concurrent-writes`, release builds at the same base commit, `file://`, 64 prior
commits, 2-second warm-up, 10-second measurement, medians of three alternating
repetitions; all runs passed readback verification; "same-branch p99 remains in seconds
under contention":

| Workload | Before, commits/s | After, commits/s | After / before |
|---|---:|---:|---:|
| 1 writer, 1 branch | 21.8 | 60.9 | 2.79× |
| 4 writers, 1 branch | 21.3 | 41.9 | 1.97× |
| 4 writers, 4 branches | 22.2 | 113.4 | 5.11× |

- "The local history curve measured 40–41 Lance object-store calls per main-branch write
  through 1,024 prior updates; a shallow named-branch write increased from 48 to 103
  calls. These are logical calls, not measured network round trips."
- DST storage-cost golden (`crates/omnigraph-dst/cost_table.txt`, memory store), before →
  after, selected rows: `InsertV` `l.get` 53 → 22, `l.list` 10 → 9, `l.put` 7 → 6
  (`a.exists` 4, `a.get` 7 unchanged); `AddFriend` `l.get` 345 → 237, `l.list` 49 → 45,
  `l.put` 27 → 24; `InsertLegacy` `l.get` 195 → 133; `RemoveFriendshipsFrom` `l.get`
  110 → 61; `BranchDelete` `l.get` 66 → 41; `Cleanup` `l.get` 596 → 288; `Optimize`
  `l.get` 301 → 98, `l.put` 62 → 50; `_history` `l.get` 1146 → 600; `_verify` `l.get`
  3814 → 2494.

### Costs it states (design conclusions)

- The live set is normally one fragment (Lance splits a write at 1,048,576 rows per
  file), so a scan no longer pays per-fragment and per-deletion-file requests; pages are
  still read separately, so constant request count is "a measurement, not a guarantee".
- The live set still holds every history row (`graph_commit` and every `table_version`
  registration): bytes written and decoded per publish, and publish memory (the publish
  scan keeps every batch for the rewrite), grow with history.
- "Each version keeps its own full copy and nothing prunes `__manifest` versions today,
  so retained bytes grow with the square of the publication count until version
  retention exists."
- Unverified by these runs: live S3/Azure behavior, mixed-version writers, the
  million-row file-split boundary.

### Reviews

None on the PR (it was merged 21 minutes after opening). Two later threads touch it: on
#783 a reviewer asked whether the benchmark mixed copy-on-write with the gate change (it
did not; both sides included #781). On #785 the memory follow-up filed #860, "each
catalog publish rewrites every history row".

---

## #783 — shared schema gate implementation, and the same-branch measurement (merged 2026-09-28)

### What it changes

Implements #754:

- `crates/omnigraph/src/db/write_queue.rs`: `SchemaGateSlot` (tokio `RwLock` plus a
  release epoch for DST determinism), `SchemaSharedPermit`, `SchemaExclusivePermit`,
  `acquire_schema_shared`, `try_acquire_schema_shared`, `HeldWriteGates { _schema,
  _branch, _tables }`; `schema_apply_serial_queue_key()` deleted.
- Shared side: `StagedMutation::commit_all` (`exec/staging.rs`, which now returns
  `CommittedMutation.gates: HeldWriteGates`), the no-op conditional-mutation CAS in
  `mutate_one_attempt` (`exec/mutation.rs`), `branch_merge_impl` (`exec/merge.rs`),
  `ensure_indices` and the full-text rebuild (`table_ops.rs`), optimize, cleanup, repair,
  `branch_delete_as`, `ensure_no_pending_recovery`, the read-view captures, the
  system-column upgrade under `--check`. Exclusive: schema apply, the system-column
  upgrade, open, `sync_branch`, `refresh`, `settle_pending_schema_install`,
  `reload_schema_if_source_changed`, and branch create / create-from (their ref inventory
  has no CAS). The RFC lists 24 permit sites.
- `open_write_txn` (`db/omnigraph.rs`): when the schema-apply sentinel stands and this
  process's gate is busy, the capture parks on the shared side and recaptures; with a
  free gate it refuses after a second listing. The common path takes no permit.
- Reprepare no longer takes the exclusive side: `refresh_for_reprepare` runs
  `refresh_coordinator_only` plus one `inspect_staged_contract` probe (called from the
  reprepare loops in `exec/mutation.rs` and `loader/mod.rs`). Before, every same-branch
  reprepare called `refresh`, which, under the write-preferring lock, parked every new
  shared acquisition in the process.
- Branch gate unchanged for same-branch writers; per-table gates unchanged (found to add
  no exclusion: every production path takes them inside the branch gate, and their key
  includes the branch).
- Tests: `crates/omnigraph/tests/failpoints.rs`
  (`cross_branch_writers_overlap_inside_schema_gate`, `sibling_branch_creates_exclude_at_the_schema_gate`,
  `read_capture_proceeds_while_writer_parked`), `crates/omnigraph/tests/schema_apply.rs`
  (`parked_writer_blocks_schema_apply`), write-queue unit test
  `queued_schema_exclusive_blocks_later_shared`,
  `crates/omnigraph-gqt/cases/concurrent_reprepare_refresh_blocks_read.gqt`, DST
  `dst_schema_apply_racing_writers_first_contact` and the ignored
  `dst_schema_apply_racing_writers_hunt` (`crates/omnigraph-dst/tests/scenarios.rs`,
  `src/concurrent.rs`). Docs: `docs/dev/writes.md` gate order, `docs/dev/architecture.md`
  read-path sentence, release note.

### Numbers

Attribution, one effect at a time. Builds: `main` = `b14c22c5` (the merge of #781, so
both sides include copy-on-write); a never-merged diagnostic build = this PR plus one
process-wide lock around every mutation and load publication (captures overlap,
publications still serialize); this PR. Run back to back per cell, 3 runs per cell,
medians; `--writers 8`; 30 s windows; local FS with `--rows 256`; RustFS + toxiproxy
+30 ms with `--rows 64`:

| Cell, commits/s | `main` | Diagnostic | This PR |
|---|---|---|---|
| local, 8 branches | 69.4 (p95 160 ms) | 68.6 (p95 161 ms) | 154.0 (p95 70 ms) |
| +30 ms, 8 branches | 0.50 (p50 15.4 s) | 0.43 (p50 17.4 s) | 2.13 (p50 3.6 s) |
| local, 1 branch, round 1 | 24.4 | 21.7 | 21.7 |
| local, 1 branch, round 2 | 20.3 | 22.2 | 22.8 |
| +30 ms, 1 branch | 0.40 | 0.40 | 0.40 (the diagnostic from this session, the other two from an earlier session; "this cell reads 0.40 for every build in every session") |

- PR body: this PR's 8-branch rate is "about 7× (local) and 5.3× (+30 ms) its own
  single-branch rate"; 2.2× `main` locally and 4.3× at +30 ms. `authority_conflicts` is 0
  in every 8-branch cell (the merged RFC text says "`authority_conflicts` is not
  reported").
- Merged RFC text: w8×B8 "reached 154.0 commits/s locally and 2.13 at +30 ms, 4.9× and
  2.4× the one-writer rate (31.7 and 0.87, 2026-09-28 entry) against B = 8".
- Session drift: "The local one-branch cell drifts by about 20% between sessions,
  because re-prepares dominate it: every build exhausts 116–178 re-prepare budgets per
  cell." This PR's local 8-branch cell read 177 in an earlier session and 154 here. The
  earlier separate-session comparison (2.5× local, 5.9× at +30 ms for 8 branches; +36%
  local one-branch, 18.8 → 25.5) is retracted for the one-branch part.
- Single writer on one branch (`b14c22c5` with #783): 31.7 commits/s locally, 0.87 at
  +30 ms. Eight writers on one branch reach "about 65–80% and 46% of that".

Where one commit's time goes (timing probes in the write path, single writer, one
branch, 2026-09-28 decision log):

| | Local | +30 ms |
|---|---|---|
| Gate hold for one commit | 7.3 ms | 670 ms |
| of which revalidation | 0.6 ms | 298 ms |
| of which detached commit | 1.1 ms | 85 ms |
| of which publication | 5.6 ms | 284 ms |
| Failed revalidations per publication, 8 writers | 6.6 | 2.9 |

- "At +30 ms, one commit is about 38 serial round trips"; "about 670 ms of that is under
  the branch gate"; "the remaining ~480 ms is preparation". A single writer has 0 failed
  revalidations. "Revalidation alone is about 45% of the gate hold at +30 ms."
- The detached commit is 13–15% of the gate hold. At +30 ms each failed revalidation
  holds the gate "for about 300 ms before giving up (about 19 s of a 30 s window, more
  than the successful writes used)".
- Committing detached effects before the gates (the other half of RFC 0067 step 2)
  would make each loser write a dead commit: "7.6 times the table writes locally and 3.9
  times at +30 ms (one publication plus 6.6 and 2.9 failed revalidations)", to save at
  most 15% of the hold. Measured and not adopted.
- Fail-fast prototype (stale attempt failed from the handle's in-memory view before
  revalidation's round trips; it caught every failed revalidation): with 8 writers on
  one branch, +30 ms went from 0.40 to 0.53 commits/s and manifest reads per commit from
  60 to 43; local 23.2 commits/s, inside the 20–25 band; local p95 widened from 232 ms to
  1.9 s; #784 starvation unchanged. Not adopted.

### Design conclusions recorded

- The whole 8-branch gain comes from publications on different branches overlapping;
  overlapping read captures adds nothing measurable (it stays a mechanism pinned by
  `read_capture_proceeds_while_writer_parked`).
- One branch is unchanged: same-branch publications still serialize on the branch gate.
- "Because any publication invalidates every other attempt on the branch, eight writers
  on one branch cannot exceed one writer alone." Raising it "requires same-branch writes
  that do not invalidate each other, which is the footprint admission of RFC 0067's group
  commit (step 3), not further work on the gate."
- Two levers named in the PR comment: one writer: fewer serial round trips (revalidation
  ~45% of the gate hold; the manifest fold is RFC 0068's subject); many writers on one
  branch: writes that do not invalidate each other (#785).
- The shared gate does not yet let a same-branch batch form: "same-branch writers still
  serialize at the branch gate before the publisher (`commit_all`), so releasing the
  branch gate ahead of the publisher is step-3 work."
- Unresolved then: a read on the writer's own handle waited for that handle's
  coordinator during a publish (`commit_updates_on_branch_with_expected` held
  `coordinator.write()` across the CAS). The `docs/dev/architecture.md` context visible
  in #1350's diff says this wait was later removed (`Omnigraph::install_coordinator`):
  "A read and a write sharing one handle and branch ... no longer wait for each other."
- `#643` (two merges overlapping) is not evidenced by this change and stays open.

### Reviews and objections

- azimafroozeh's review came in chat; ragnorc's PR comment relays and answers it:
  1. "The benchmark mixes copy-on-write and the schema gate." Answered: both sides
     include #781. The underlying point (two bundled effects) was accepted; the
     diagnostic build split them, and the +36% one-branch figure was retracted
     (`35926d3c`). Resolved.
  2. "With one writer we're bottlenecked by the writer: rejections, more IO, and IO
     latency dominates." Answered: IO latency dominates, but one writer never rejects
     (0 failed revalidations); breakdown above. Resolved by explanation.
  azimafroozeh then approved ("LGTM, Thanks!", 2026-09-28).
- An independent review (Codex, `gpt-6-astra`, per the PR body) found three gaps, all
  fixed: a vacuous interleaving check in the DST arm, reader actors outside the seam
  scheduler's turns in the hunt, and an overstated read-path claim.
- The DST arm itself found two engine issues before going green: the write capture sat
  outside the gate (writers on another handle were refused and retried hot, 256
  immediate retries on the first op), and promotion after guard release made two
  promoters serialize on Lance's commit path (reverted; later moot).

---

## #784 — same-branch writers starve under object-store latency (open)

### Report

`concurrent-writes` on `main` at `b14c22c5`: 8 closed-loop writers on one branch, RustFS
behind toxiproxy (~30 ms per round trip), 30 s window after a 5 s warm-up, 3 runs:

| Run | Commits in the window | Commits per writer |
|---|---|---|
| 0 | 12 | `[0, 0, 0, 0, 0, 0, 0, 12]` |
| 1 | 12 | `[0, 0, 0, 0, 12, 0, 0, 0]` |
| 2 | 12 | `[0, 12, 0, 0, 0, 0, 0, 0]` |

- The seven losers' operations finish only in the roughly 21 s drain after the window.
  The reported service time (p50 2.46 s, p95 2.8 s) describes the winner alone.
- Local disk: within one run, per-writer commit counts range from 13 to 219.
- 8 writers on 8 branches: every writer makes the same number of commits (±2) in every
  run, locally and at +30 ms.
- "At +30 ms a preparation takes about 2.4 s." Each run records 56 re-prepares and no
  exhausted re-prepare budgets (`authority_conflicts` 0).

Likely mechanism (stated as not confirmed in code): a writer prepares outside the gates,
then takes the branch gate and revalidates. After one publication, each queued writer
finds the head moved and re-prepares after paying the failed revalidation's round trips.
The writer that just published starts its next preparation first, reaches the gate first
and wins again.

Scope: #783 spreads the same 12 commits over two or three writers (reads no longer wait
behind a publish) but does not make admission fair. Possible directions: fair admission
(re-prepare under the held branch gate, or grant the gate by first-attempt order), or
group commit. Acceptance: every writer commits within the window with per-writer counts
within a stated bound; the benchmark reports each writer's longest wait, including ops
finishing in the drain.

### Comment (ragnorc, 2026-09-30)

- #783 does not fix it; the +30 ms one-branch cell reads 0.40 commits/s before and after.
- Fail-fast from memory: +30 ms 0.40 → 0.53 commits/s; local unchanged; starvation stayed.
- "The ceiling is structural": eight writers on one branch cannot beat one writer
  (31.7 commits/s locally and 0.87 at +30 ms).
- Proposed fix: #785 (arrival-order queue; a refused write that re-prepares keeps its
  place).

No other comments. Status: open; the fix it names (#785) is unimplemented.

---

## #785 — RFC: group commit, with the Lance composition guard (open)

### Status

- GitHub PR open (not marked draft on GitHub). The RFC file
  `docs/rfcs/2026-09-28-group-commit.md` is not on `main`; at the PR head (`0069e925`)
  its front matter says `status: draft`, `implementation: not-started`, `updated:
  2026-11-08`.
- `mergeable: CONFLICTING` on 2026-11-14. The `PR Title` check fails
  (`[rfc-type] docs(rfc) is not the RFC type`); the other checks on the head passed or
  were skipped (the rollup also lists cancelled runs from earlier pushes). The 2026-11-08
  review reports 27 passing checks at `0069e925`, including `Test Workspace`, which runs
  the new guard on Lance 13.0.0.
- `blocked_on` (front matter):
  1. Engine half of the composition guard: a composed commit carries its components'
     markers, and the change feed's one-transaction proof and merge's insert-absence
     certificate accept it.
  2. Correctness: the admission matrix, failpoints around the composed detached commits
     and the CAS (including the in-doubt read-back), and the DST concurrent universe with
     the publisher on (every acknowledged write visible exactly once, one actor per
     commit).
  3. Instrument: `concurrent-writes` with eight writers on one branch, local and RustFS
     +30 ms, above one writer's rate on the same build (31.7 and 0.87 commits/s on
     `b14c22c5` with #783), one writer within noise of today, per-writer counts within a
     factor of two (#784). Per #895, this gate cannot be evaluated on `main` today.

### Diff

1. `docs/rfcs/2026-09-28-group-commit.md` (new, 882 lines).
2. `crates/omnigraph/tests/lance_surface_guards.rs`: new test
   `group_commit_composes_inserts_staged_against_older_pins_onto_a_later_pin` with helpers
   `pk_rows`, `stage_keyed_upsert` (calls `stage_pk_merge` with `WhenMatched::UpdateAll`,
   `WhenNotMatched::InsertAll`), `commit_detached_from` (`CommitBuilder::with_detached(true)
   .with_skip_auto_cleanup(true)`), `keyed_row_identities`.
3. `docs/rfcs/0067-detached-table-commits.md`: a pointer in Unresolved questions.
4. `docs/rfcs/README.md`: registry row.

### The proposed design

Summary: several writes on one branch publish with one `__manifest` compare-and-swap as
one graph commit. A per-branch publisher in each process takes over the part of a write
that runs under the branch gate today. Writers prepare as now and submit what they
staged; while one publication is in flight, new submissions queue; when it returns, the
publisher admits queued writes that cannot invalidate each other or anything published
since their capture, commits each table's admitted effects as one detached commit, and
publishes through the unchanged single-commit publisher. Every admitted write is
acknowledged with that commit. No storage format, wire or API change.

Two load-bearing decisions:

- **A batch is one graph commit.** Its effect is the union of its writes. Every surface
  that maps a commit to its `__manifest` version keeps working. RFC 0067's sketch (N
  commit rows per publication) breaks each of these (code survey): snapshot reads and
  diffs of a non-last member fail their head check (`graph_coordinator.rs`, `feed.rs`);
  the feed's one-transaction proof needs one detached commit per interval; merge bases
  and collector merge-base roots go through `pinned_graph_commit` (`retention.rs`), which
  aborts the whole cleanup plan on a non-head member; parent resolution orders by
  `(graph_manifest_version, created_at, id)` and could silently fork lineage;
  `commit::overwrite` keeps duplicate head rows within one publication; the lost-ack
  read-back recognizes only the head commit; `ExactGraphHead` holds one expectation;
  schema apply records its parent before publication. Per-commit pins would need
  registration rows keyed by commit (a format change).
- **Same-table inserts compose by key.** All benchmark writers insert into one `@key`
  table (`Chunk`), and `insert` into a keyed type is staged as an upsert
  (`PendingMode::Upsert`, `exec/mutation.rs`), so disjoint-table admission would batch
  nothing. Composition covers strict inserts and upserts whose staged transaction only
  appends (no key matched), using Lance's documented distributed-write shape: raw
  unassigned fragments committed in one transaction.

A batch holds the writes of one actor (a commit row names one actor). A write naming an
expected graph head (`Omnigraph-If-Graph-Commit`, SDK `expected_head`) always publishes
alone.

Entries. A mutation or load prepares as today (capture authority, execute, validate at
the pinned base, stage fragments and build each table's Lance transaction), then submits
an entry instead of taking the gates in `commit_all`. An entry holds: the captured
authority (branch incarnation, schema identity, caller expected head, the materialized
head H0 = the branch's `graph_head` row that `ExactGraphHead` compares, and the effective
head a fresh named branch inherits from its fork point); per written table the staged
write, its pin and its class; per opened table the strictest `MutationOpKind` that
`ensure_path` records in `MutationStaging.op_kinds` (today dropped by
`stage_all_with_concurrency`); the footprint = written tables ∪ execution reads (every
table execution opened through `ensure_path`, including a predicate scan that matched
nothing; = `MutationStaging.expected_versions`) ∪ validation reads (edge-endpoint
existence, cascade/referential reads of incident edge tables, overwrite-load referential
reads; recorded nowhere today); the actor; an arrival ticket.

Classes per written table:

- Append-only, when every executed operation on it was a strict insert or upsert (no
  update, delete or schema rewrite recorded), its staged transaction adds fragments and
  removes or updates none, it carries the exact inserted ids within the per-entry cap, the
  table has no non-key `@unique` group (node or edge), and an edge table has no bounded
  `@card`.
- Exclusive otherwise: every executed update or delete (even zero-match, even one that
  only touched the entry's own pending inserts), upserts that matched a key, overwrites,
  cascades, non-key uniqueness, bounded cardinality, oversized id sets. Any exclusive
  table, or any executed update/delete anywhere in the footprint, makes the entry
  exclusive.

The publisher: one per graph root and branch per process; a queue and a leader flag in
the process-global write-queue manager (`db/write_queue.rs`). No spawned task: a write
group. An entry arriving while no batch is publishing leads, publishes the batch holding
its own entry on its own task, then promotes the next waiting entry or clears the flag. A
batch runs:

1. Gates: the schema gate's shared side and the branch gate (not the per-table gates,
   which add no exclusion inside the branch gate).
2. Revalidate once (branch authority and schema contract, a `__manifest` row since
   #834; may reuse its own last published authority the way #813 reuses a captured
   authority for non-bound branches). Yields current materialized head Hc, current
   effective head, current pins.
3. Log check: if Hc is not the head the publisher's last batch produced, clear the
   effect log.
4. Admit queued entries in ticket order up to caps on entries and on composed fragments
   (the fragment cap keeps a composed transaction inline in its manifest). Another actor's
   entry stays queued and leads the next batch. An expected-head entry is admitted only
   as the batch's first entry, with effective head = expected head and Hc = its captured
   materialized head, and then closes the batch.
5. Commit each table's effect detached from its pin at Hc, stamped with the staging
   witness (branch incarnation, Hc); tables in parallel.
6. Publish with `publish_with_precondition`: one `DatasetUpdate` per table, expected
   versions at Hc for every table in the batch's footprints, one `LineageIntent` with the
   batch's actor, `ExactGraphHead(Hc)`. Publisher API unchanged.
7. Complete every entry; a submitting handle applies the published snapshot to its own
   coordinator before its receipt returns. No coordinator held across the CAS.

Failure outcomes: confirmed refusal (`ReadSetChanged`): delete the batch's detached
manifests, refresh, re-judge from step 3 (entries captured before the foreign commit are
refused). In doubt: every entry gets the in-doubt outcome; the read-back is unchanged
because it looks for exactly one commit.

Admission. Entry k captured at H0, batch forming at Hc; *writers* = every commit in
(H0, Hc] plus entries admitted earlier in this batch. Admit when: (1) authority: branch
incarnation and schema identity equal the batch's; (2) horizon: every commit in (H0, Hc]
is in the effect log (this publisher published it) and there are at most K of them;
(3) conflicts: an exclusive k requires that no writer wrote any table in its footprint;
an append-only k requires that no writer wrote a footprint table exclusively and that id
sets are disjoint on every table both appended to. Claimed result: the batch is
conflict-serializable in admission order (backward validation). Worked counterexamples in
the text: write skew through zero-match scans (why execution reads are in the footprint),
and the `Left`/`Right` mixed insert plus zero-match update (why classes come from executed
operations, not staged transactions).

Composition. Per table: one exclusive entry (commits as staged; its base is still the
table's pin) or any number of append-only entries composed into one merge-insert `Update`
(`RewriteRows` mode, new fragments only), read from the table's pin at Hc. Lance assigns
fragment ids, stable row ids and row-version metadata at commit. The union must come from
each staged write's raw transaction (fragment id 0, empty row-id sequence for an insert;
`assign_row_ids` fills an incomplete sequence), not `StagedWrite::new_fragments()` (the
read-your-writes copy with provisionally assigned ids, which Lance would keep). The
key-existence filter (`inserted_rows_filter`) is the bitwise OR of the components' Bloom
filters (one configuration, one key column). A marker survives only when every component
carries it (`omnigraph.no_by_source_delete`, `omnigraph.insert_absence`; the latter
re-certified for the composed base by the admission proof). The commit goes through the
sealed table adapter as a new entry the durable-call guard registers. The change feed's
fast path still applies (`changes/candidate_scan.rs`: the read version is the table's pin
at Hc).

The effect log: in memory, per publisher, last K batches: commit id, head produced, per
written table "exclusive" or the appended ids (over-cap appends logged as exclusive);
bounded by K × cap; a cache of immutable published facts (invariant 12), never commit
authority (`ExactGraphHead(Hc)` is). Lost on restart: entries captured before a restart
fail the horizon rule and re-prepare.

Collector. Detached commits are written after admission, stamped with Hc, so
`judge_staging`'s rule (an unpublished staging is dead once the head moves past its
recorded head) is unchanged. RFC 0067's sketch (commit before submitting, stamped with
the entry's own H0) would let the collector sweep an admitted entry. An entry's fragments
are unreferenced from preparation until its batch's detached commit; the collector
deletes unreferenced files only after `UNVERIFIED_THRESHOLD_DAYS` (seven days); the RFC
inherits that wall-clock assumption.

User-visible behavior: concurrent writes by one actor on one branch may publish as one
commit (each receipt names it; `commit list` shows one; its change-feed block, diff and
snapshot contain all of them; `graph_manifest_version` stays the commit's own); errors
unchanged (`ReadSetChanged`; insert-only mutations re-prepare up to 32 times, Append or
Merge loads re-prepare, updates, deletes and Overwrite loads return the conflict); fewer
conflicts (refused only when a publication since capture touched the footprint);
fairness by arrival order (a refused entry that re-prepares keeps its place, for kinds
that re-prepare automatically); cancellation rules tied to owned server operations;
merge, schema apply, index builds, optimize, cleanup and branch controls unchanged and
serialized with batches by the branch gate; cross-process writers still race only at the
CAS.

Invariants: 2 unchanged (one publication per batch); 3 needs a precise amendment ("an
entry's footprint is proven unchanged, not its whole view"); 4 every write publishes once;
5 unchanged; 7 composed append leaves an unindexed tail; 8 constraints carried by the
admission rule or the write is exclusive; 10 policy per write at `_as` before submission;
11 queue bounded by entries and bytes, effect log by K and cap; 12 effect log caches
immutable facts; deny-list: the publisher is process-local, not fencing.

Compatibility: no storage or wire change; an older binary reads a batch commit as an
ordinary commit. Rollback = batch cap of one AND horizon zero (a cap of one alone would
still admit entries captured before an unrelated publication).

Alternatives rejected: N commit rows per publication (above); disjoint-table admission
only (no gain on the measured workload); chaining same-table writes on the previous
write's detached version (serializes preparation per table; "#784 puts one preparation at
about 2.4 s at +30 ms under contention"; refusing one link refuses its successors); a
timed batching window (adds latency at low load); a dedicated publisher task (a child
producer no owned operation owns; outside `--measure` rows and `.gqt` `order:`); failing
fast before revalidation (bounded by the single-writer ceiling); batches across actors
(needs per-write attribution, a format change; natural home beside #513's idempotency
key); RFC 0068's commit record (orthogonal, composes: one record per batch).

Rollout: (1) record footprint, executed op per opened table (carry `op_kinds` through
`stage_all_with_concurrency`), class, exact inserted ids on the staged write, and remove
`StagedWrite`'s dangling doc comment; (2) publisher with batches of one replacing the gate
section of `commit_all`; (3) batch exclusive entries by footprint; (4) compose append-only
entries behind the surface guard; (5) run the instrument, docs, release note.

Unresolved: K and the caps; loads in batches (recommended: admit); merge, index builds and
optimize as exclusive entries; admitting across a foreign commit by reading its per-table
change sets (`omnigraph.deleted_ids`, detached-transaction fragments); where per-write
attribution belongs; the next lever after this one: "Since #834 one insert makes 13
requests: 6 writes, the key check, the table open, and 4 `__manifest` reads", and a capture
served from the publisher's known head is #837.

Model claim, labeled "a model, not a claim": at +30 ms a batch costs about the 670 ms one
write costs today, so eight writes per batch would publish several times the current rate.
`__manifest` growth: B same-table inserts produce one registration row instead of B.

### The Lance composition guard

The Lance half is checked in (the engine half is still `blocked_on`).
`group_commit_composes_inserts_staged_against_older_pins_onto_a_later_pin`:

- Setup: a primary-keyed dataset at P0. Entries A (`b1`, `b2`) and B (`c1`) stage keyed
  upserts of new keys against P0. A third insert (`d1`) lands as a detached commit from P0,
  giving P1. Entry C (`e1`) stages against P1. A's operation is cloned, its
  `new_fragments` replaced with the union of the three raw staged fragment lists and its
  `inserted_rows_filter` replaced with the OR of the three Bloom filters, and the result is
  committed as one detached transaction from P1 (`Transaction::new(p1.version, …)`).
- Pinned: each staged transaction is `Operation::Update` with empty `removed_fragment_ids`
  and `updated_fragments`; every raw staged fragment has id 0 and an empty row-id sequence
  (`RowIdMeta::Inline` decoding to length 0); the filters share field ids and one Bloom
  configuration (`num_bits`, `number_of_items`, `probability`), and their OR intersects
  each; the composed version is detached; rows are exactly the union (`alice`, `b1`, `b2`,
  `bob`, `c1`, `d1`, `e1`) and P1's rows keep their row ids and stamps; new stable row ids
  are unique and above P1's maximum; new fragment ids are unique and above P1's; the
  composed rows share one version stamp distinct from `d1`'s; the composed version reopens
  by id.
- Passed on Lance 11.0.0 (2026-09-30) and Lance 13.0.0 (after merging `main`, 2026-11-08;
  whole `lance_surface_guards` binary 73 passed, 2 ignored). The guard's comment says a
  Lance release that assigned ids at staging would be "a design review for that RFC, not a
  test to weaken".
- Its first run corrected a review claim: staged fragments do carry a row-id sequence,
  empty for an insert, filled by `assign_row_ids` at commit.

### Measurements posted on the PR

Spike (comment 2026-10-01, ragnorc). A local, never-to-be-proposed branch: an eligible
mutation (one table, a keyed write that only appends, no caller head precondition, one
actor) joins its branch's batch; the leader revalidates under the gates (reusing the first
entry's capture when still current); admission by "captured at the current head, or at an
earlier head reached only through this batcher's own publications, with exact id sets
checked"; raw fragments composed into one detached commit; write-group handoff (a writer
leads only the batch holding its own entry). Not covered: leader cancellation, multi-table
writes, updates and deletes, the footprint rules, the engine half of the guard.

Method: `concurrent-writes`, one branch, `--rows 64 --dims 8`, 5 s warm-up and a 30 s
window, release builds with `RUSTFLAGS=` empty; baseline `main` `97f293e8` vs the spike on
the same base; alternating binaries, 3 repetitions each; every run verified exact row
counts and readback; RustFS behind toxiproxy with 15 ms each way. A flag-off control of
the spike binary at 1 and 8 writers matched baseline. Medians of 3, "the repetitions agree
within a few percent".

| Backend | Writers | Baseline commits/s | Spike commits/s | Gain | Per-writer max/min | p99 service | `__manifest` writes per commit |
|---|---:|---:|---:|---:|---|---|---|
| RustFS +30 ms | 1 | 1.83 | 1.83 | 1.0× | 1.0 → 1.0 | 579 → 577 ms | 3.05 → 3.05 |
| | 2 | 1.87 | 2.30 | 1.2× | 56 → 1.0 | 574 → 1,008 ms | 3.05 → 3.09 |
| | 4 | 1.47 | 4.63 | 3.1× | 44 → 1.0 | 738 → 958 ms | 3.27 → 1.53 |
| | 8 | 0.87 | 9.23 | 10.6× | 26 → 1.0 | 1,185 → 984 ms | 3.23 → 0.77 |
| | 16 | 0.50 | 13.63 | 27× | 15 → 1.3 | 2,068 → 1,788 ms | 6.00 → 0.52 |
| local | 1 | 31.8 | 32.6 | 1.0× | 1.0 → 1.0 | 47 → 45 ms | 4.00 → 4.00 |
| | 2 | 29.1 | 43.2 | 1.5× | 1.8 → 1.0 | 1,006 → 67 ms | 4.01 → 4.01 |
| | 8 | 21.5 | 65.0 | 3.0× | 231 → 1.0 | 6,947 → 180 ms | 4.05 → 2.42 |

Stated against the gate: 8 writers beat 1 (9.23 vs 1.83 on RustFS; 65.0 vs 32.6 locally);
one writer within noise and "a batch of one makes the same requests per write"; per-writer
counts within 1.3× (today up to 231×); retries after a moved head fall "from hundreds or
thousands per window to zero". At 2 writers the gain is small and p99 rises: the two
writers mostly alternate batches of one. Earlier spike versions (lone-writer recapture; a
leader that kept leading) cost the 2-writer cell and fairness, and one run starved a
writer.

Note on the baselines: the spike's one-writer +30 ms baseline (1.83 on `97f293e8`) is not
the 0.87 one-writer figure from `b14c22c5`; the spike's 8-writer baseline is 0.87. Between
the two bases, #834 cut an insert from 20 to 13 requests (12 to 8 gated), per #785 and #837.
The threads do not attribute the difference explicitly.

Memory (comment 2026-10-03). First comment: peak RSS 86 → 233 MB at 16 writers on RustFS;
313 MB → 1.4 GB at 8 writers locally (where the spike made 3× the commits). Follow-up:
peak RSS follows commit count with and without the batcher (local, end of window):

| arm | commits | peak RSS |
|---|---|---|
| baseline, 1 writer | 1,625 / 2,316 / 3,380 | 215 / 386 / 918 MB |
| spike, 1 writer | 1,967 | 427 MB |
| spike, 8 writers | 2,201 / 3,449 / 4,648 | 782 / 1,235 / 2,018 MB |

Lance session caches stay small (21 MB metadata at 4,648 commits). Two engine terms filed:
#859 (the table-handle cache holds 64 superseded manifests of the hot table; a cap of 4
cut the live heap from 197 to 40 MB at the same throughput) and #860 (each catalog publish
rewrites every history row). Conclusion stated: the batcher adds no memory term of its
own.

### Reviews and objections

1. Codex (`gpt-6-astra`) review of the first draft, recorded in the decision log (not a
   GitHub review): two unsound admission cases and five gaps, all fixed in `970f3f68`:
   edge non-key `@unique` admitted as append-only; footprint counted only validation reads
   (write skew via zero-match scans); materialized vs effective head conflated; a cap of one
   did not restore today's behavior (rollback became cap one + horizon zero); fairness
   assumed every refused entry re-prepares; keyed inserts are staged as upserts; exact
   inserted ids are not carried on `StagedWrite`. The same review confirmed one commit per
   batch keeps commit-to-version consumers intact, Lance 11 can compose raw fragments at a
   later pin, and the collector rule holds with the materialized head as witness.
2. GitHub review 2026-10-05 on `f2757800` (from the `ragnorc` account), recommendation
   "request changes to the RFC's append-only admission rule before implementation". Inline
   [P2] "Preserve predicate reads when classifying append-only entries": the `Left`/`Right`
   mixed insert plus zero-match update stages only appends with disjoint ids, yet no serial
   order gives `(0, 0)`; reproduced on existing request semantics; cause:
   `execute_update` returns before staging when nothing matches and
   `stage_all_with_concurrency` discards `op_kinds`. Resolved in `0069e925` (class from
   executed operations). The review body also lists tradeoffs: low contention offers less
   benefit (single-writer path needs the cost gate); shared commit id loses intermediate
   history states; one-actor restriction limits batching; batch shares publication failure
   and in-doubt handling, and a leader's cancellation affects others; exact id sets, queued
   payloads and the effect log consume memory, caps need workload evidence; table-level
   exclusion causes avoidable conflicts. Liability: "the design reuses one graph
   publication door, existing receipts, collector records, and Lance transactions. It
   avoids a second durable commit protocol. That removes potential format and recovery
   liability. It also adds a real concurrency validator, queue ownership, fairness rules,
   and bounded historical state." It checked the detached commit path in pinned Lance
   (`rust/lance/src/io/commit.rs`): it "builds from the supplied dataset. It does not supply
   the graph's predicate-read proof."
3. GitHub review 2026-11-08 on `0069e925` (from the `ragnorc` account), recommendation
   "ready after one small edit to the test matrix, plus the title fix that CI requires":
   - Should fix (inline, line 687): the new matrix case expects `(1, 0)` or `(0, 1)`, which
     conflicts with the Errors section (an update returns `ReadSetChanged` and does not
     re-prepare); correct expectation: exactly one publishes, the other is refused with no
     effect, never both at age 0; "a correct batcher fails this test". **Unresolved at the
     PR head** (line 687 still reads "`(1, 0)` or `(0, 1)`, never `(0, 0)`").
   - Optional (inline, line 253): `op_kinds` has no reader today and its doc comment in
     `exec/staging.rs` wrongly says it drives a drift check in `commit_all`; rollout step 1
     should rewrite it so a cleanup cannot delete it as dead state; the append-only test
     equals `!strict_pre_stage_version_check()` on the recorded kind. **Unresolved.**
   - Required by CI: rename to the `rfc:` type. **Unresolved** (title unchanged).
   - Checks it made: `ensure_path` records the kind before any row is read; `MutationOpIR`
     has `Insert`, `Update`, `Delete`; keyed insert records `Merge`, unkeyed `Insert`;
     Overwrite load records `SchemaRewrite`; the upgrade rule in `ensure_path` is a
     two-level order. Context: #1291 moves the OCC probe of `revalidate_write_txn` out of
     the gates (`revalidate_write_txn_with`).
4. The PR also shows a passing `Devin Review` check with no comments.

---

## #837 — capture a write from the process's own last publication (open)

### Proposal

Under the v0.12 deployment model (one mutation-capable serving process per cluster root),
most of a write's object-store requests re-read state the writing process itself produced.
Direction (not a design): capture a write from the state this process last published
(schema, catalog, branch head, table pins) as an immutable snapshot; keep the publication
CAS (`_versions/{v+1}.manifest` written only if absent) as the cross-process fence; a
foreign publication then fails the CAS and the write re-prepares. Constraints listed:
invariant 3 (one immutable snapshot; retry starts fresh), invariant 12 (the held state is a
hint), branch identity (a version-file CAS does not show the branch is the same lifetime;
#822's recreation test is `ReadSetChanged` on `branch_identifier:t`; one identity check
before acknowledgement probably stays), foreign schema apply (fails the CAS since #834),
embedded and CLI writers (correct, may pay a CAS loss). Areas: `open_write_txn`,
`revalidate_write_txn`, `commit_all`, the publisher's state load. "Should be measured on its
own, separately from group commit, before any RFC is written."

### Request counts

Issue body: one single-row insert on `main` `97f293e8` (after #834), `omnigraph-gqt
--measure` (DST, in-memory store, unit cost model), 13 serial requests:

| # | Requests | What |
|---:|---|---|
| 6 | puts | table data file, transaction and detached manifest; `__manifest` data file, transaction and the version (the publish CAS) |
| 1 | get, table data | the key-existence check |
| 2 | head + get, table manifest | the table open (#818 serves it from the handle on a warm write) |
| 3 | lists of `__manifest/_versions` | latest-version probes: at capture, in revalidation, at publisher load |
| 1 | get, `__manifest` manifest | the publisher reloading the catalog state it then overwrites |

Requests 6–13 run after `mutation.post_stage_pre_effect_gate`, under the branch gate: "8
serial requests bound one branch's commit rate however many writers it has". Before #834:
20 requests, 12 gated. Expected effect of the proposal: about 9 serial requests, 7 with
#818; gated section from 8 to about 5.

Comment 2026-10-18 (ragnorc), reproduction on `main` `0a62bb7b`, `omnigraph-gqt
insert_one_row.gqt --measure --model unit`, DST target `omnigraph-engine-dst`,
`in-memory-object-store`, seed 0, replay matched; a `Person` node keyed by `name`, empty
seed, three inserts on one handle. Second insert: 14 requests, 1 repeat, makespan 12:

| # | Verb | Object | Role |
|---:|---|---|---|
| 1–2 | head, head (absent) | `__manifest/_versions` for `v` and `v+1` | capture probe, in parallel (1 tick) |
| 3–4 | head + get | detached table manifest | table open |
| 5 | get | table data file | key-existence check |
| 6 | put | table data file | staged data |
| 7–8 | head, head (absent) | `__manifest` `v` and `v+1` | revalidation, after `mutation.post_stage_pre_effect_gate` |
| 9–10 | put, put | table transaction, detached table manifest | detached commit |
| 11 | list | `__manifest/_versions` | publisher freshness check, `publish.load_state` |
| 12–14 | put, put, put | `__manifest` data, transaction, `_versions/{v+1}.manifest` | publish, last put is the CAS |

- Requests 7–14 are gated: 8 gated requests.
- Third insert on the same handle: 16 requests, makespan 14, 2 repeats; the detached
  commit heads and gets the same table manifest the stage just opened (a 4096-byte range;
  the manifest put by the second insert is 33921 bytes), inside the gate: 10 gated.
- First insert: 27 requests, makespan 19 (writes the full-text declaration
  `omnigraph_fts_compat.json` and a second detached manifest, #909; cold-loads catalog
  fragments in `publish.load_state`: 11 requests, one list, one manifest get, nine
  data-range gets).
- Since `97f293e8`: #962 replaced the capture and revalidation listings with two HEAD
  pairs (`latest_attached_version`, `crates/omnigraph-catalog/src/lib.rs`), each pair one
  makespan tick; the warm publisher no longer gets the `__manifest` manifest body
  (`GraphNamespacePublisher::dataset` returns the dataset from `published_rows` when
  `current_control_dataset` reports it latest; `load_publish_state` uses `cached_rows`);
  the remaining catalog read is the listing inside Lance `resolve_latest_location` →
  `current_manifest_path`. The strict overwrite (`max_retries` 0) does not list again.
- The DST golden row for `InsertV` is 6 puts, 1 list, 3 gets; it does not record the probe
  HEADs (current `cost_table.txt`: `InsertV` `l.get` 3, `l.list` 1, `l.put` 6).
- Where each `__manifest` read comes from: capture (`open_write_txn` →
  `write_authority_for_known_branch(..., fresh: true)` → `probe_latest_incarnation` →
  `latest_attached_version`); revalidation (`commit_all` → `revalidate_write_txn` → the
  same two HEADs under the gate); publisher load (`publish_with_precondition` →
  `load_publish_state` → `current_control_dataset` → Lance list). Mutation and load share
  all three.
- Recommended fix: keep the snapshot this process last published (catalog version, branch
  identifier, schema identity, graph head, published row batch); skip the capture HEAD
  pair, the revalidation HEAD pair and the publisher `_versions` listing while the
  process-local record still names that publication; keep the create-if-absent put as the
  only commit authority; drop the snapshot on CAS conflict, `ReadSetChanged`, a branch
  change, process start; keep the named-branch identifier comparison before any detached
  table commit. Effect on the second-insert shape: 9 requests, makespan about 9, 5 gated;
  about 7 with #818.
- Alternatives: leave probes ("a single-writer server keeps paying 5 catalog requests per
  insert, 3 of them under the branch gate"); group commit only (one writer still pays the
  full prepare); trust Lance's version hint (lance#6752, in 11.0.0; `uses_version_hint` is
  false when the store's listing is lexically ordered, so S3 Standard, GCS, Azure and the
  in-memory store do not use it; the hint does not know this process wrote `v`).
- Lance notes: 11.0.0 has the version hint and `CommitHandler::version_exists`;
  `current_manifest_path` still lists on lexically ordered stores; strict overwrite skips
  the conflict listing. Lance 12.0.0 has `put_if_predecessor` (lance#8800) for an external
  manifest store; no built-in store implements it. Lance 13 has lance#8729 (resolution
  stops after the first manifest; the list request remains). No open upstream PR lets a
  writer skip latest-version resolution when it holds the version it will commit with
  `max_retries` 0.
- Liability stated: one process-local snapshot plus the invalidation rule; because the CAS
  stays, a missed clear becomes one failed attempt and unreachable detached files.
- Limitations stated: `97f293e8` not rerun; `concurrent-writes` and `concurrent-merges`
  not run; DST golden not regenerated.

Comment 2026-11-13 (posted by an audit workflow under the `ragnorc` account, finding
`L-publisher-load-probe`, at `9fb5b48e`): the listing left in the publisher comes from
`GraphNamespacePublisher::dataset` (`publisher.rs:604-626`) → `current_control_dataset`
(`control_dataset.rs:71-104`) → Lance `resolve_latest_location`; on S3, GCS and Azure that is
a LIST page of `_versions/` on every publish by every process. The by-name probe
(`latest_attached_version`, HEAD `v` and `v + 1`) answers the same question and its HEAD of
`v` returns the ETag and size `same_location` compares. Keep the listing as fallback
(handlers with an identity, a gap of more than one version, any error). Expected: publishing
rows such as `InsertV`, `KeyedInsert*`, `OverwriteWriter*` go from 1 `l.list` to 0 at the
price of two HEADs. "Code inspection only."

No reviewer objections in the thread. Status: open, not started. #1350 later removed the
capture and staging probes for a process that holds the bound branch's writer epoch (below);
the publisher's load listing remains on every path.

---

## #895 — a GQT benchmark executor for concurrent overlap (open)

### Problem and ask

Since #894, every benchmark is a GQT fixture plus a workload selecting one measured
operation. The adapter accepts direct-engine, local-filesystem environments only and
refuses concurrent actor blocks, DST seams and cloud stores (`crates/omnigraph-bench/README.md`,
"Local support envelope"). `concurrent-writes` moved to `benchmarks/deferred/`;
`concurrent-merges` (#835) was closed unmerged and was never on `main`. GQT's `---
concurrent` block "runs two to four statements once each, in a scripted interleaving, under
the DST runner's paused clock", an ordering proof, not a throughput measurement.

Blocked: #785's gate, #784's fix, #643's overlap regression watch (#825 recorded 2.89× →
1.27× for eight merges with `concurrent-merges`), RFC 0067's "What to measure" section.

Executor requirements: (1) N sessions over one `Omnigraph`, released together, clock
started before release; (2) per-actor operation streams with disjoint keys, by duration or
count; (3) closed-loop (diagnostic, `claim_grade: false`) and open-loop with scheduled
arrivals and an on-time validity rule (RFC 0039 Rule 1) for any claim; (4) warm-up, window
and drain separated; commits per second per branch and within the window; service-time
percentiles; per-actor acknowledged counts; engine re-prepares and driver-visible
conflicts; batch completion over a single-operation control; (5) verification outside the
timer (row counts, sampled readback, merge receipts against the target head, #823); (6) an
S3-compatible store with injected latency, or a declared latency-injection treatment.
Acceptance: both scenarios return as catalog scenarios with new experiment identities, not
appended to the deferred series; #785's gate evaluable or restated; deferred sources kept
until covered.

### Comment (ragnorc, 2026-10-19): reproduction and design

- Verdict "partially reproduced": only `concurrent-writes` is in `benchmarks/deferred/`.
- Catalog (`benchmarks/benchmarks.yaml`, `main` `9e2425d2`): 93 scenarios, 16 groups, none
  containing `concurrent`; `omnigraph-bench run concurrent-writes` → `unknown_selection`.
  Wall-clock runs require a release build and (on Linux) XFS on a directly mounted NVMe
  namespace (`environment.rs`, `verify_linux`).
- Adapter refusals: `--- concurrent` and `--- seam` workloads → `unsupported_environment:
  this executor cannot run seams or concurrent blocks`; S3 runner or backend → refused
  (`gqt-v1 supports local-filesystem only`).
- `cases/concurrent_read_beside_publish.gqt` under `--measure --model unit`: each seed runs
  the block once plus a replay; block `wall_ms` 12 or 13; writer row virtual time 67 ms
  (seed 0) and 120 ms (seed 42); whole case 1.57 s wall. "An ordering proof plus a virtual
  cost model of one scripted pass." GQT `--measure`'s `s3-like` model sleeps a request
  cost on the paused clock; it does not inject latency into a live store.
- Root cause: `Case::needs_dst` → `PlainHost::admit_case` refuses (`host.rs`);
  `validate_definition` rejects non-`local-fs` backends (`gqt_case.rs`); the DST runtime is
  current-thread with `start_paused(true)`; `run_concurrent_step` starts each session once.
- Recommended design: one new catalog execution kind for concurrent actors (do not move the
  `.disabled` harness back into Cargo; leave `--- concurrent` as the ordering proof); one
  `Omnigraph`, N session clones, barrier release; closed-loop diagnostic and open-loop
  claim as separate identities; backend identity includes store and delay injection; "An
  in-process sleep is a different backend"; "A DST seam is the wrong injection: it pauses
  the clock and changes the question"; Azure out of this slice; HTTP concurrency stays the
  separate deferred `bench_concurrent_http` source.
- Alternatives: restore the deferred Rust files as Cargo benches (keeps the old identity,
  closed-loop only, still no `concurrent-merges`, skips RFC 0039's catalog protocol);
  stretch `--- concurrent`/`--measure`; drive the HTTP server.
- Lance notes: `commit_transaction` loops up to `CommitConfig.num_retries` (default 20) and
  rebases when the resolver allows; a detached commit retries a random-version collision,
  not a logical conflict; merge-insert retry default 10 retries and a 30 s budget;
  "Same-branch starvation is that graph retry (`ReadSetChanged` and re-prepare), which
  Lance's dataset retry does not remove and does not measure." lance#8952 (open):
  concurrent merge-insert can miss an inserted-key filter when join columns are not the
  dataset primary key ("Re-check keyed upserts if it merges"). #1072 switched the
  merge-insert writer to strict `SourceDedupeBehavior::Fail`, so a write scenario needs a
  new baseline. lance#8914 (concurrent range requests for large metadata) and lance#9350
  (dataset label on store metrics) can change request counts.
- Limitations: catalog not executed; RustFS/toxiproxy not run.

No objections recorded. Status: open; no PR implements the executor.

---

## #1018 — schema apply, branch create/delete and cleanup across writer processes (open)

### Problem and options

These operations are supported from one writer process only (`docs/dev/invariants.md`,
"Current support boundaries"); write queues and the schema gate are process-local
(`WriteQueueManager::for_root`). Nothing stops or warns two processes. Options: (a) engine:
condition each operation's effects on the manifest version or native-ref CAS and revalidate
under publication; (b) a cooperative machine-local writer lock. Ask: an engine RFC
preferring (a).

### Comment (ragnorc, 2026-10-20): reproduction on `main` `b8e82abf`

Method: two OS processes (the child re-executes the test binary), failpoint seams holding
each side inside the window; one deterministic trial per race; local disk; Lance 11.0.0;
temporary test `issue_1018_repro` (removed).

- Same process, two handles creating `race`: one success, one `branch 'race' already
  exists`.
- Two processes creating `race`: both succeed; two native refs
  (`race.01M5BPR55ERWY77FHD6H9XB5W5.json`, `race.01M5BPR55EVTGRQ51EY0BNFWVY.json`);
  `branch_list` then fails ("more than one live native incarnation; run cleanup").
- Branch delete beside a publish on `feat` (writer held at `publish.pre_merge`, after the
  precondition check): both succeed; the commit later returns `NotFound` with
  `publication_in_doubt: false`.
- Schema apply beside branch create (apply held at `schema_apply.post_lock_pre_effect`):
  both succeed; `side` lacks the new column; merging `side` into `main` returns
  `ReadSetChanged` on `schema_ir_hash`.
- Cleanup beside a live insert (cleanup held at `cleanup.table_gc`, keep-one): no damage.
- Two processes updating one row, both held after detached commits
  (`mutation.post_finalize_pre_publisher`): one publishes (graph version 2), the other gets
  `ReadSetChanged` for `graph_head:main` (expected 1, current 2); the Person table pin was
  version 3 with 4 detached manifests left (the loser's is unreachable).

Root causes: branch create checks names by listing refs and creates `{logical}.{ULID}`
(Lance `Branches::create` is put-if-absent per path, so different ULIDs do not conflict);
branch delete uses `replace_metadata`, which in Lance 11 (and at v13.0.0) is an exists check
plus an unconditional put; schema apply checks "only `main`" while planning and does not
recheck at publication, and a create does not move `main`'s version; row writes are already
decided by the CAS.

Classes: already decided by the manifest CAS (row mutate and load, merge, schema apply
against a writer that moves `main`'s version, system-column upgrade against that writer,
`ensure_indices`, optimize; #297 is this class) vs not decided by it (branch create and
create-from, branch delete, cleanup against a tree Lance wrote but did not yet name, proposal
drop vs publish, contract install/discard/reload).

CAS primitive facts: Lance's put-if-absent of the next numbered `__manifest` version with
zero retries (`lance-io-11.0.0` `object_store.rs`); local disk stages and uses
`rename_if_not_exists`; S3 and Azure send `PutMode::Create` (`If-None-Match: *`); local
`PutMode::Update` is `NotImplemented`; there is no conditional delete.

Recommendation: option (a), as specified in draft PR #1017 ("Engine: control operations on
the version CAS"): retirement as a head-preserving publication on the branch chain; the
logical branch name registered on `main` after the native ref exists (two creates race on
`main`'s version); schema apply and system-column upgrade then lose to a later create;
cleanup deletes an unregistered tree only after the seven-day orphan age and never calls
Lance's forced branch delete; drop becomes a publication before any tag release. Cost: one
publication on `main` for create; one on the branch plus one on `main` for delete; one on
the target branch for drop; no new put for schema apply, row writes, merges, optimize, index
publication or cleanup. Liability: new `__manifest` rows under the next storage stamp
(irreversible; needs ordering with #963). Option (b) protects only cooperating processes of
one account on one machine; presenting it as fencing would match the deny-list.

Limitations: one trial per race; no S3/Azure run; cleanup against Lance's two-phase create
not reached (no seam between tree write and ref write); proposal drop, system-column upgrade
vs create, contract install/discard/reload not raced.

No other comments. Status: open; #1017 (draft) implements nothing.

---

## #1350 — writer epoch fencing and the owner's probe skip (merged 2026-11-11)

First rollout step of the writer epoch fence RFC (`docs/rfcs/2026-11-09-writer-epoch-fence.md`,
#1328), plus the probe skip from its second step.

### How the writer epoch works

- Row: each branch's own `__manifest` carries a `writer_epoch` control-fact row (storage
  format v17; `crates/omnigraph-catalog/src/state.rs` `WriterEpochRow`). A branch without
  the row, or with a row its fork source wrote, is at epoch 0.
- Holder registry: `crates/omnigraph-catalog/src/writer_epoch.rs`, `WriterEpochHolders`, a
  process-wide `Mutex<HashMap<HolderKey, HeldEpoch>>` (`of_process()`), keyed by
  `HolderKey { root: write_queue_root_identity(root_uri), branch }`. `HeldEpoch { epoch,
  native (the native ref of the claim), landed (the `__manifest` version of this process's
  last publication on the branch, or None once withdrawn) }`. A process holds an epoch only
  after its own claim landed; no reload raises it; a refusal clears it (`clear`); its own
  delete of the branch drops it (`release`); otherwise it ends with the process. An entry
  whose native ref is not the loaded chain's is dropped (`held`).
- Claim: `Omnigraph::claim_branch_writer` / `claim_branch_writer_as(branch, actor)` under
  policy action `branch_claim` (target-branch scoped), the shared schema permit and the
  branch gate, through a coordinator opened under those gates
  (`GraphCoordinator::claim_writer_epoch`). The catalog's `claim_writer_epoch` writes the
  loaded rows with the row at `E + 1` and a fresh attempt id as the next `__manifest`
  version under `PublishPrecondition::ExactBranchIdentity` (branch name plus Lance-native
  identifier, no head compare). The version records no graph commit and moves no head. A
  holder of the current epoch gets it back and writes nothing (`EpochVerdict::AlreadyHeld`).
  A claim that loses its version reloads and asserts the epoch its first load read, so of
  two racing claims one holds `E + 1` and the other is refused with `read: E`. A put with
  no answer is settled from the attempted version (`settle_unanswered_claim`: the row with
  this attempt id → landed; another publication before any acknowledgement → lost; after an
  acknowledgement → in doubt, holds nothing, the next claim settles it). Internals:
  `ClaimIntent`, `PreparedClaim`, `put_claim`, `read_back_claim`, `claim_attempts`.
- Check: every head-moving publication asserts the epoch in `publish_attempts` (the OCC
  retry loop), on each attempt, over the rows that attempt loaded, before
  `check_publish_precondition` and before the put-if-absent of the branch's next
  `__manifest` version (`assert_writer_epoch`, `check_writer_epoch`). A process holding
  nothing proceeds only on rows at epoch 0; a process holding `E` only on rows at `E`;
  otherwise `WriterFenced { held, read, current }`, which `is_retryable_publish_conflict`
  does not match (never retried), clears the holder, and leaves the publish's detached
  staging for the collector. A publish that lost the put to a claim reloads and is refused.
  "The check issues no request" (it compares loaded rows with the registry). `HeldVersion`
  records each landed publication (`record_publication`, `max(landed, version)`) and its
  `Drop` withdraws the version (`withdraw_version`) on any error or dropped future, because
  such an attempt may have landed unrecorded.
- Not fenced: Lance ref metadata and ref deletes, the collector's deletes, `optimize`'s
  configuration commit, the stamp-only upgrade route; detached candidate versions carry the
  row and assert nothing. A mutation that stages nothing never reaches the publisher.
- Error surface: new API code `writer_fenced` (HTTP 409, disposition `refresh`) in
  `openapi.json`; `ErrorCode` is a closed enum, so clients built before cannot decode it.
- Format: new graphs are born at v17 (older binaries refuse); `omnigraph upgrade` stamps
  v14–v16 branches to v17; cluster graphs cannot reach v17 yet (`upgrade` refuses cluster
  roots).
- No command or route claims yet. Once an embedding program claims a branch,
  `omnigraph-server` and the CLI are refused there with `writer_fenced`.

### What the owner's probe skip removes from the write path

Before: "a warm read of a handle's bound branch pays one freshness probe and a write two"
(PR body). The RFC names the two write probes: at `open_write_txn` and beside staging in
`early_revalidation` (whose answer serves `revalidate_write_txn_with` under the gates). By
#837's description after #962, a probe is a by-name HEAD of `__manifest` `_versions` `v` and
`v + 1`.

After, while the process holds its handle's bound branch with its last publication on
record, no head probe runs at: a read targeting the branch (`ReadTarget::Branch`),
`resolve_snapshot` / `GraphCoordinator::resolve_snapshot_id`, a write's open and its
revalidation (both ask `GraphCoordinator::head_is_current`, which returns
`manifest.version() >= landed` with no request), and the probe beside staging
(`early_revalidation` returns `None` when `held_head()` is set). A handle below the recorded
version (one that did not run that publication) reloads first, also with no probe.

Still probing or reading on that branch: the publisher's own load in `publish_attempts`
(which, per #837, includes a `_versions` LIST on S3/GCS/Azure), a merge's authority capture
and revalidation, a branch create's capture of its source, the change-feed poll, every held
branch that is not the handle's bound branch, every branch the process holds nothing on, and
every handle after a failed or cancelled publish until the next publication lands. A write
that stages nothing, opened on the record, probes once before answering
(`unpublished_view_is_current`).

Freshness cost stated: after another process claims the branch or deletes it, the owner
serves reads from its installed snapshot until a publish of its own loads the branch (stale
reads until then).

### Measured effects

Request counts only; no commits/s or latency was reported.

- `warm_read_cost.rs::held_bound_branch_read_issues_no_probe`: warm read of `main`,
  `version_probes` 1 before the claim, 0 after, `manifest_reads` 0; a held branch that is not
  the bound one is read like an unclaimed one.
- `held_bound_branch_second_handle_reaches_the_owners_publish_without_a_probe`: a second
  handle reaches the owner's publish with `version_probes` 0, then `manifest_reads` 0.
- `held_bound_branch_reads_and_writes_across_an_optimize_configuration_commit`: the second
  optimize moves the `__manifest` version by one without publishing; the other handle reads
  with 0 probes and 0 manifest reads and its write lands.
- `write_cost.rs::branch_claim_adds_no_request_to_a_write` (helper
  `tests/helpers/cost.rs::assert_branch_claim_adds_no_request_to_a_write`): an insert on a
  held bound branch issues exactly the unclaimed insert's requests (`data_reads`,
  `data_writes`, `data_open_count`, `manifest_reads`, `manifest_writes`,
  `manifest_scan_count`, `internal_open_count`), with `version_probes` > 0 unclaimed and 0
  held. The S3 twin `write_cost_s3.rs::branch_claim_adds_no_request_to_a_write_on_s3` "has run
  nowhere" (skips without a bucket; no CI job runs it).
- `branch_control_cost.rs::branch_claim_is_one_publish_and_the_owners_repeat_writes_nothing`:
  a claim's `manifest_writes` equal one publish's; 0 data reads, writes, opens; the owner's
  repeat writes nothing.
- DST storage-cost golden: `WriterClaim` `l.get` 7, `l.list` 2 (its open and its load of
  main's `__manifest`), `l.put` 3 (its one version).
- Fence behavior: `failpoints.rs::branch_claim_fences_a_publish_parked_before_its_put` (two
  processes; the owner's publish parked before its put; a second process claims; the resumed
  publish is refused `WriterFenced`, no row written) and 17 more `branch_claim_*` two-process
  tests; four GQT cases on a held branch through a new `# writer: held` header
  (`held_branch_owner_reads_its_own_writes_across_restart.gqt`,
  `held_branch_owner_reads_a_write_whose_publish_stays_in_doubt.gqt`,
  `held_branch_contended_writes_land_and_are_read.gqt`,
  `held_branch_write_that_fails_before_its_publish_changes_nothing.gqt`); DST
  `WorldOp::WriterClaim` claims in one process with no fault injection.

### Author's own caveats (PR body)

Taking storage format 17 ahead of #963 (wrong if an existing cluster graph must be
claimable before a cluster upgrade route exists); three departures from the RFC (a process
holding nothing is refused `WriterFenced` ahead of a schema apply's `ReadSetChanged`; a
failed or cancelled publish withdraws the recorded version; `branch_claim` lands with the
engine API); ABA on `main` (no native ref, so a stale holder entry passes when a recreated or
restored root's epoch equals its own); a ref delete asserts no epoch; a write that stages
nothing is not refused; the S3 cost twin and the closed `ErrorCode` enum.

### Reviews and objections

- `chatgpt-codex-connector[bot]`, two inline P1 findings on `f3704ba0`:
  1. `publisher.rs` line 2174, "Keep claim-only versions out of graph-commit retention": the
     collector's `retained_by_position` counts native `__manifest` versions, so with
     `keep_versions: Some(2)` a claim-only version can push out commit A's pins. The
     `ragnorc`-account review confirmed it and noted `optimize`'s configuration commit
     already has the same effect; suggested counting distinct head commits, possibly in its
     own change. Not fixed in this PR.
  2. `writer_epoch.rs` line 35, "Include the captured storage destination in holder
     identities": `write_queue_root_identity` is URI-only, so two captured S3 endpoints for
     one `s3://bucket/graph` collide on `main`. Same root as S4 below. Not fixed in this PR.
- Review of `1d409999` (from the `ragnorc` account, 2026-11-11 10:50):
  - B1 (blocking): record the format decision and the four RFC departures in the RFC before
    an irreversible stamp lands (the RFC was unchanged: `status: draft`, `implementation:
    not-started`, `blocked_on` still listing the stamp order with #963). **Unresolved at
    merge; the RFC on `main` is still `draft` / `not-started`.**
  - D1 (decision): born at 17 costs downgrade compatibility now for a capability that ships
    later; options: keep new graphs at 16 until the claim command ships, or let the claim
    raise its own branch to 17 in its own CAS (reviewer's preference). **Not adopted; shipped
    born at 17.**
  - S1: the claim is a one-off publication path (its own retry loop and read-back); consider
    a generic control-fact publication. Not addressed.
  - S2: `ErrorCode` closed enum; add a `#[serde(other)] Unknown` fallback in its own change.
    Not addressed here.
  - S3: "A process-wide mutex on every warm read and publish": `head_is_current` →
    `held_writer_version` → `WriterEpochHolders::holds_nothing` locks one global
    `Mutex<HashMap>` on each warm read of the bound branch, and `assert_writer_epoch` on each
    publish attempt; "one serialization point for all reads in a server process"; not
    measured; suggested an `AtomicUsize` fast path. **Unresolved** (`holds_nothing` still
    takes the mutex on `main`).
  - S4: `main` ABA; `record_publication` keeps `max(landed, version)`, so after a restore
    under a live process every read does a full reopen until a publish passes the old
    version; suggested keying `main` by its chain's lifetime. Deferred.
  - S5: merge order with #1349 (carrying revalidation): decide whether a held owner carries
    over its own process's `optimize` publication from another handle. (#1349 has since
    merged as `b2dbaff7`.)
  - S6: the fence is as strong as the store's put-if-absent; Lance's guide says safe
    concurrent writes on `s3://` need a store that enforces `If-None-Match: *`, and some
    S3-compatible stores do not; suggested one sentence in troubleshooting.
  - "Five more changes like this": the epoch rides the object every publish already reads
    and swaps (no second source of truth, no extra request); what would compound is S1, S2
    and stamp ownership.
  - Validation listed: catalog `v17_` 35 passed; whole catalog 250; `branch_claim_` 18;
    `forbidden_apis` 42; `policy_engine_chassis` 26; `warm_read_cost` / `write_cost` /
    `branch_control_cost` / `lifecycle` 31 / 20 / 5 / 26; `write_cost_s3` "3 passed in 0.00
    s" (skipped, not S3 evidence); `upgrade::tests` 103 passed, 1 failed (root-uid
    environment artifact); GQT `held_branch` 4; `writer_header` 2. Found the new
    `overwrite_probed` predicate also fixes a latent v15 cold-registry-fragment loss.
- Review of `32214cf9` (2026-11-11 11:22): the only new commit bumps `rustix` 1.1.4 → 1.1.6
  for RUSTSEC-2026-0486; no blocking issue; B1 still open, D1 still a decision; S7 suggested
  landing the bump on `main` separately.
- Merged by azimafroozeh at 11:56, after the second review, with no further commits.

---

## Referenced in these threads but not read in full

#643 (independent merges serialize), #744 (detached-commit branch), #776 (promotion
removed), #797 and #794 (GQT `--- concurrent` block and `--measure`), #813 (reuse captured
authority at revalidation for non-bound branches), #818 (hold the table version a write just
committed; skips the table-manifest head and get on a warm open), #822, #823, #825 (merge
overlap ratio), #834 (schema contract into `__manifest`; insert 20 → 13 requests), #835
(closed `concurrent-merges`), #859 and #860 (memory), #894 (benchmark migration that deferred
the harness), #962 (HEAD-pair freshness probe), #963 (schema apply with live branches; also
wants a stamp), #1017 (draft RFC for option (a)), #1072 (Lance 13 upgrade), #1291 (OCC probe
out of the gates), #1328 (writer epoch RFC; only its front matter and probe sections were
read), #1349 (carry writes over row-preserving publications), RFC 0067 (throughput path),
RFC 0068 (graph commit record), RFC 0039 (measurement rules).

---

## Implications

Facts from these threads that constrain or inform a design for higher concurrent commit
throughput, stated without preference for a design:

1. **Same-branch ceiling.** Revalidation fails whenever the branch head moves, so every
   publication invalidates every other prepared write on the branch; N writers on one branch
   cannot exceed one writer (31.7 commits/s locally, 0.87 at +30 ms on `b14c22c5`; eight
   writers reach about 65–80% and 46% of that). Gate-level fixes measured so far stay under
   that ceiling: the shared schema gate changed nothing on one branch (0.40 at +30 ms in
   every build); fail-fast raised +30 ms from 0.40 to 0.53 and widened local p95 from 232 ms
   to 1.9 s.
2. **Where one commit's time goes.** At +30 ms: about 38 serial round trips; about 670 ms
   under the branch gate (revalidation 298 ms, detached commit 85 ms, publication 284 ms) and
   about 480 ms of preparation; revalidation is about 45% of the gate hold. Locally: a 7.3 ms
   gate hold (0.6 / 1.1 / 5.6 ms), dominated by publication. Under contention at +30 ms, a
   preparation takes about 2.4 s (#784), and failed revalidations held the gate about 19 s of
   a 30 s window.
3. **Per-commit object-store requests (single-row insert, DST in-memory store, unit model).**
   13 serial on `97f293e8` (6 puts, 1 key-check get, 2 table-open requests, 4 `__manifest`
   reads), 8 of them under the branch gate; 20 (12 gated) before #834; 14 requests with
   makespan 12 on `0a62bb7b` (8 gated); a third insert on the same handle 16 (10 gated); a
   first insert 27. The 6 puts are table data file, table transaction, detached table
   manifest, `__manifest` data file, `__manifest` transaction, and `_versions/{v+1}.manifest`
   (the CAS). The DST golden `InsertV` row is 6 puts, 1 list, 3 gets (probe HEADs not
   counted). #781's local curve: 40–41 Lance logical calls per main-branch write through
   1,024 prior updates; 48 → 103 for a shallow named-branch write. A writer-epoch claim costs
   7 gets, 2 lists, 3 puts.
4. **Proposed request reductions not yet built.** #837: skip the capture HEAD pair, the
   revalidation HEAD pair and the publisher LIST while the process's last publication is on
   record → 9 requests, about 5 gated (about 7 with #818). #837 (2026-11-13): replace the
   publisher's `_versions` LIST with a HEAD pair. #1350 already removes the two write probes
   and the read probe, but only for a process holding the bound branch's epoch, and nothing
   claims in production yet.
5. **Measured effect of batching (spike, not merged).** With one CAS per batch, `__manifest`
   writes per commit fell below one (0.77 at 8 writers, 0.52 at 16, RustFS +30 ms), commits/s
   rose from 0.87 to 9.23 (8 writers) and 0.50 to 13.63 (16) at +30 ms and from 21.5 to 65.0
   (8 writers) locally; one writer unchanged; per-writer max/min ≤ 1.3. p99 did not fall at
   two writers (574 → 1,008 ms at +30 ms). The correctness gates of #785 remain open, and the
   spike covered only single-table, append-only, single-actor writes.
6. **Commit grain and attribution.** Every mapping from a commit to its `__manifest` version
   assumes one commit per version (snapshot reads, diffs, change feed, merge bases, retention
   roots, parent resolution, lost-ack read-back, `ExactGraphHead`). N commit rows per
   publication breaks them; per-commit pins would be a format change. A commit row names one
   actor, so cross-actor batching needs per-write attribution (a format change).
7. **Admission correctness.** Classifying writes by staged transactions alone admits
   non-serializable batches (zero-match predicate reads; the `Left`/`Right` case). The #785
   rule needs executed-operation kinds (`op_kinds`, dropped today by
   `stage_all_with_concurrency`), validation reads (recorded nowhere today) and exact inserted
   ids (not carried on `StagedWrite` today). Same-table inserts in the benchmark are keyed
   upserts, so disjoint-table admission batches nothing on that workload.
8. **Lance facts relied on.** Lance assigns fragment ids, stable row ids and row-version
   metadata at commit, so raw staged insert fragments from older pins compose onto a later
   pin in one detached commit (guard passes on Lance 11 and 13). Merge-insert key filters are
   Bloom filters that OR under one configuration. Detached commits need no conflict pass. The
   `__manifest` publish is an `Overwrite` with `with_max_retries(0)`: a strict put-if-absent
   of `_versions/{read_version + 1}.manifest`, never rebased. Lance's dataset retry (default
   20) does not address graph-level `ReadSetChanged` retries. lance#8952 (open) concerns
   concurrent merge-insert key filters.
9. **The CAS primitive and its limits.** Local disk: stage and `rename_if_not_exists`; S3 and
   Azure: `PutMode::Create` (`If-None-Match: *`). No conditional delete; Lance
   `replace_metadata` is not conditional. The fence is only as strong as the store's
   put-if-absent; some S3-compatible stores do not enforce `If-None-Match: *`.
10. **Process topology.** One mutation-capable writer process per cluster is the supported
    topology; process-local gates and publishers must not be presented as distributed
    fencing (deny-list). Row writes across processes are already decided by the version CAS
    (one winner, the loser gets `ReadSetChanged` after detached effects, which stay
    unreachable). Branch create/delete, schema apply against a create, and cleanup are not on
    the CAS today (#1018). The writer epoch fences head-moving publications through the same
    CAS at no extra request.
11. **Collector and recovery.** Pre-publication effects must stay unreachable; the collector
    judges an unpublished staging dead once the head moves past the head it recorded, so
    detached commits made before admission (stamped with an older head) could be swept while
    admitted; unreferenced fragment files are reclaimed only after seven days. Committing
    detached effects before the gates was measured as producing 7.6× (local) and 3.9×
    (+30 ms) table writes under contention.
12. **History-dependent costs.** Copy-on-write publication rewrites the whole live row set
    (all history rows) on every publish: bytes and memory per publish grow with history, and
    retained `__manifest` bytes grow with the square of the publication count until version
    retention exists (#781, #860). Peak RSS tracked commit count with and without batching
    (#859: 64 cached superseded manifests). Fewer publications per write would slow
    `__manifest` growth proportionally (#785).
13. **Acknowledgement rule.** A write is acknowledged only after durable graph visibility
    (the CAS returned); an in-doubt publication makes every write that shares it in doubt;
    lost-acknowledgement read-back looks for exactly one commit. The #785 review notes that
    reusing the one publication door "avoids a second durable commit protocol", which it
    counts as removing format and recovery liability.
14. **Cross-branch work already scales.** Since #783, publications on different branches
    overlap: 8 writers on 8 branches gave 154.0 commits/s locally and 2.13 at +30 ms (vs 69.4
    and 0.50 before), with equal per-writer counts (±2) and `authority_conflicts` 0. Each
    branch publishes to its own `__manifest` chain.
15. **Instrument availability and noise.** The `concurrent-writes` harness does not run on
    `main` (preserved byte-identical in `benchmarks/deferred/`, last runnable at `b54f4709`);
    the benchmark catalog refuses concurrent blocks, seams and S3 backends; GQT `---
    concurrent` is an ordering proof on a paused clock; #895 asks for a closed-loop
    diagnostic plus an open-loop claim executor with an S3 latency backend. Closed-loop
    percentiles hid the starved writers in #784. The local one-branch cell drifts about 20%
    between sessions (116–178 exhausted re-prepare budgets per cell), so only same-session
    comparisons count; the +30 ms one-branch cell read 0.40 in every build and session on
    the `b14c22c5` line. The counters are logical calls at the store seam, not round trips.
