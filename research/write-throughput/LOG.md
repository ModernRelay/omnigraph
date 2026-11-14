# Write-throughput research log

Running log: hypotheses, commands, raw numbers, verdicts. Newest entries at
the bottom. Raw instrument output lives in `results/`.

## 2026-11-14 — setup

- Branch `research/write-throughput` from `origin/main` at `ed09ee63`.
- Box: 2 vCPU (gVisor), 64 GB RAM, Rust dev profile with `opt-level = 2` for
  every dependency (Lance, DataFusion, Arrow, object_store) and `opt-level = 0`
  for workspace crates (`Cargo.toml` `[profile.dev.package."*"]`). Numbers are
  from that profile unless stated otherwise.
- Object store under latency: RustFS `1.0.0-beta.12` (the CI-pinned version,
  `rustfs-linux-x86_64-gnu-v1.0.0-beta.12.zip`, SHA256SUMS verified) behind
  toxiproxy `v2.13.0` (`checksums.txt` verified) adding 15 ms each way
  (about 30 ms per round trip; `curl` round trip to the proxy measured
  32 ms), the setup PRs #783/#785 and issue #784 measured with.
  `scripts/s3-up.sh` brings both up; bucket `omnigraph-bench`.

## Reading done

- AGENTS.md, invariants.md, lance.md (index), testing.md, systems.md,
  writes.md, recovery.md, RFC 0067 (incl. "Throughput after this RFC"),
  RFC 0068 (graph commit record).
- PR #785 (group commit RFC, spike numbers, reviews), #746, #754, #783, #781,
  #1350; issues #784, #837, #895, #1018.

Key prior numbers (closed-loop `concurrent-writes`, insert-only, disjoint keys):

| setup | 1 writer | 8 writers 1 branch | source |
|---|---:|---:|---|
| local, main b14c22c5 | 31.7 c/s | 20–25 c/s | RFC group commit |
| +30 ms RustFS | 0.87 c/s | 0.40 c/s | same |
| +30 ms, group-commit spike, 16 writers | — | 13.6 c/s (vs 0.50) | PR #785 comment |
| local, group-commit spike, 8 writers | 32.6 | 65.0 (vs 21.5) | same |

Same-branch ceiling mechanism (from #783/#784/#785): revalidation fails when
the branch head moves, so each publication invalidates every other prepared
write on the branch; N writers on one branch cannot beat one writer.

## 2026-11-14 — the instrument

`crates/omnigraph/tests/integration/concurrent_write_throughput.rs`, one
ignored test (`instrument:`), configured by `OG_CW_*` env vars (module docs).
N closed-loop writers over one `Arc<Omnigraph>`; each cell is a fresh graph
(`node Item { slug: String @key, n: I64 }`, 64 seed rows, `ensure_indices`),
3 s warm-up, 20 s window, verification of exact row counts per branch on a
fresh handle. Request counts come from a counting `WrappingObjectStore`
installed on the manifest, history and table planes through `QueryIoProbes`
(complete on S3; on `file://` Lance's direct local reader/writer bypass the
wrapper, so local request counts are a lower bound). Tokio runtime with 8
worker threads (the box exposes 32 CPUs; `nproc` reports 2). Run with
`scripts/sweep.sh <label> <local|s3> [writers] [modes] [runs] [secs]`; the
baseline binary is kept at `/srv/bins/baseline` for same-session reruns.

### Single-write request trace, S3 +30 ms (OG_CW_TRACE=1, 1 writer)

One insert = 352 ms ≈ 10.5 round trips, all serial except the two HEAD pairs:

| t (ms) | request | phase |
|---:|---|---|
| 0 | HEAD `__manifest/_versions/{v}` ∥ HEAD `{v+1}` | capture probe |
| 33 | GET table detached manifest (the pin) | table open (handle-cache miss) |
| 68 | GET table data file ∥ HEAD v ∥ HEAD v+1 | key check ∥ early revalidation |
| 102 | PUT table data file | staging |
| 136 | PUT table `_transactions/*.txn` | detached commit (gated) |
| 170 | PUT-if-absent table `_versions/d*.manifest` | detached commit (gated) |
| 204 | LIST `__manifest/_versions` (~45 ms) | publisher load (gated) |
| 249 | PUT `__manifest/data/*.lance` | publish (gated) |
| 284 | PUT `__manifest/_transactions/*.txn` | publish (gated) |
| 318 | PUT-if-absent `__manifest/_versions/{v+1}.manifest` | the CAS (gated) |

Gated section ≈ 216 ms (6 serial requests + the LIST); preparation ≈ 136 ms.
Lance 13 has `disable_transaction_file`, but only `#[cfg(test)]`
(`ManifestWriteConfig::with_transaction_file_disabled`); `CommitBuilder`
exposes no switch, so the two `.txn` PUTs cannot be removed without changing
Lance. Ruled out as a lever.

### Baseline, local (file://), label `baseline`, binary at ed09ee63

| mode | writers | writes/s | p50 ms | p99 ms | req/write | reprepares/write |
|---|---:|---:|---:|---:|---:|---:|
| same | 1 | 17.65 | 53 | 86 | 7.0 | 0 |
| same | 4 | 15.20 | 265 | 368 | 15.5 | 1.01 |
| same | 16 | 15.35 | 1,015 | 1,561 | 15.4 | 0.98 |
| same | 64 | 15.35 | 3,952 | 5,784 | 15.6 | 1.03 |
| separate | 1 | 15.10 | 62 | 112 | 14.0 | 0 |
| separate | 4 | 46.15 | 86 | 145 | 18.8 | 0 |
| separate | 16 | 106.28 | 146 | 258 | 20.9 | 0 |
| separate | 64 | 145.78 | 438 | 635 | 22.9 | 0 |

Same branch: flat from 4 to 64 writers, fair (per-writer counts equal: the
gated re-prepare of `PreEffectReprepares` makes the branch gate FIFO-like),
every write prepared twice. Separate branches scale; request count per write
grows with the branch count (one-entry `merge_authority_cache` thrash, to be
checked).

### Baseline, S3 +30 ms (RustFS + toxiproxy), label `baseline`

| mode | writers | writes/s | p50 ms | p99 ms | req/write | reprepares/write |
|---|---:|---:|---:|---:|---:|---:|
| same | 1 | 2.75 | 362 | 375 | 13.1 | 0 |
| same | 4 | 2.15 | 1,864 | 1,975 | 24.0 | 1.05 |
| same | 16 | 2.15 | 7,319 | 7,612 | 24.3 | 1.02 |
| same | 64 | 2.35 | 12,930 | 22,886 | 22.2 | (0.02, see note) |
| separate | 1 | 1.55 | 645 | 674 | 20.9 | 0 |
| separate | 4 | 4.40 | 987 | 1,255 | 28.4 | 0 |
| separate | 16 | 13.80 | 1,041 | 1,387 | 34.0 | 0 |
| separate | 64 | 22.25 | 2,825 | 5,320 | 35.3 | 0 |

Note: at 64 same-branch writers each write waits ~13 s, longer than the 3 s
warm-up, so the reprepare counter (recorded when the first attempt fails,
long before the write completes) undercounts in the window; throughput is
unaffected because the branch gate is saturated from the start.

A single writer on a *named* branch is much slower than on `main`
(1.55 vs 2.75 writes/s, 21 vs 13 requests): a cross-branch lever to look at.

## Idea 1 — carry insert-only writes over insert-only commits (rebase, not re-prepare)

Hypothesis: a same-branch write is invalidated only because the head moved;
when every commit since its capture only inserted keys into tables, Lance's
own resolver (`TransactionRebase`, which already checks merge-insert key
filters) can carry the staged write onto the current pin, as the engine
already does for row-preserving publications. Removes the gated re-prepare.

Prototype: `carry_chain(.., inserts=true)` accepts insert-only `Update` links;
`row_preserving_tables` accepts a growing row count; `commit_all` carries an
insert-only write when no written table has non-key `@unique` or bounded
`@card`. Binary `/srv/bins/carry`.

| target | writers | baseline w/s | carry w/s | carry req/write |
|---|---:|---:|---:|---:|
| local | 4 | 15.20 | 20.75 | 9.0 |
| local | 16 | 15.35 | 14.85 | 10.5 |
| local | 64 | 15.35 | 11.15 | 12.8 (0.56 reprepares: chain > 32 links) |
| s3 | 4 | 2.15 | 2.20 | 21.1 |
| s3 | 16 | 2.15 | 0.90 | 41.2 |
| s3 | 64 | 2.35 | 0.85 | 43.2 |

**Verdict: discard as a standalone lever.** Carrying a write over N-1
intervening commits walks N-1 links (a transaction read and a manifest
checkout per link) under the branch gate; on S3 that is O(N) serial GETs per
write. Even with the walk free, each write still pays the full gated commit
(~6.5 round trips), a ceiling of ~4.6 writes/s at +30 ms. Kept in the branch
because group commit uses the same insert-only classification.

## Idea 2 — group commit (leader/follower write group, compose same-table inserts)

Prototype (`crates/omnigraph/src/db/group_commit.rs`, switch
`OMNIGRAPH_GROUP_COMMIT=1`): an insert-only mutation prepares and stages as
before, then submits its staged effects to a per-(handle, branch, actor)
queue. The writer that finds no batch in flight leads: takes the branch
gates, captures the current authority once, admits queued entries (same
incarnation/schema; captured at the current head, or at an older head reached
only through this group's own batches whose inserted ids are disjoint; no
duplicate id within the batch), composes each table's admitted inserts into
one Lance `Update` read on the table's current pin (raw fragments, union of
key filters, Lance assigns fragment and row ids at commit), commits detached,
publishes once. Leader leads only the batch holding its own entry, then hands
the lead to the next waiting entry: natural batching, no timer.

First version collapsed at 16 writers: a Bloom-filter overlap (a false
positive: keys are disjoint) refused one entry; it re-prepared under the
gates on the ordinary path, its commit was not in the group's log, so the log
cleared and every older entry was refused too (cascade). Fixed by (a) exact
inserted-id sets on the staged write (`StagedWrite::inserted_ids`, Bloom only
as fallback), (b) refused group entries re-prepare *outside* the gates and
rejoin the queue.

| target | writers | baseline w/s | group w/s | group commits/s | writes/batch | p99 ms (base → group) |
|---|---:|---:|---:|---:|---:|---|
| local | 1 | 17.65 | 18.10 | 18.10 | 1.0 | 86 → 84 |
| local | 4 | 15.20 | 34.90 | 24.05 | 1.5 | 368 → 204 |
| local | 16 | 15.35 | 68.50 | 16.30 | 4.2 | 1,561 → 435 |
| local | 64 | 15.35 | 74.55 | 15.65 | 4.8 | 5,784 → 2,502 |
| s3 | 1 | 2.75 | 2.60 | 2.60 | 1.0 | 375 → 402 |
| s3 | 4 | 2.15 | 6.80 | 3.40 | 2.0 | 1,975 → 616 |
| s3 | 16 | 2.15 | 26.45 | 3.35 | 7.9 | 7,612 → 636 |
| s3 | 64 | 2.35 | 69.45 | 2.65 | 26.2 | 22,886 → 1,344 |

Zero re-prepares, per-writer counts within a few percent (S3 64 writers:
20–24 each). **Verdict: keep — 12× at 16 and 30× at 64 writers on S3.**
Publications stay at ~3/s on S3 (≈300 ms per batch): next lever is the
batch's critical path.

## Idea 3 — trim round trips from the batch and the writer (group4)

Phase timing of a batch at 16 writers, S3 (`OMNIGRAPH_GROUP_COMMIT_TIMING=1`),
before: capture probe 33 ms, LIST of `__manifest/_versions` ~45 ms, detached
commits ~107 ms, publish ~115 ms. Three trims:

1. **Publisher trusts its own acknowledged version on the first attempt**
   (`GraphNamespacePublisher::dataset(trust_acknowledged)`,
   `instrumentation::rebind_control_dataset`). The publish is a put-if-absent
   of `read_version + 1` and versions have no holes, so a stale held view loses
   the CAS; the retry (attempt > 0) qualifies the view with the listing as
   before. Removes the LIST from every publish (the audit's
   L-publisher-load-probe, #837). Switch `OMNIGRAPH_PUBLISH_TRUST_ACKNOWLEDGED`.
2. **Capture from the handle's installed view without a probe** when that view
   is this group's own last publication (leader), and for every publishing
   writer under group commit (`open_write_txn_probed(.., fresh=false)`): the
   group's admission refuses a capture another writer made stale, the
   ordinary path's revalidation re-probes under the gates, and the CAS refuses
   it in every case. Removes 1 RTT from the writer and 1 from the batch.
3. **Warm the next version's index metadata during the publication.** Every
   write's validation (`plan_write → load_index_facts → load_indices`) read
   the index section of the table version the previous batch had just
   committed (backtrace via `OG_CW_BT_PATTERN`): one GET per write. The
   leader now loads it in parallel with its publish, into the shared session.

Single writer, S3: 352 ms → 248 ms per write (10 → 8 requests), trace:
key-check GET → data PUT → table txn PUT → table manifest PUT → manifest data
PUT (∥ index warm GET) → manifest txn PUT → CAS.

| target | writers | baseline | group | group4 | group4 commits/s |
|---|---:|---:|---:|---:|---:|
| s3 | 1 | 2.75 | 2.60 | 4.05 | 4.05 |
| s3 | 4 | 2.15 | 6.80 | 10.80 | 5.40 |
| s3 | 16 | 2.15 | 26.45 | 38.95 | 4.85 |
| s3 | 64 | 2.35 | 69.45 | 58.50 (repeat 81–88) | 2.0–2.8 |
| local | 1 | 17.65 | 18.10 | 14.35 (repeat 19.55) | |
| local | 16 | 15.35 | 68.50 | 66.45 | |
| local | 64 | 15.35 | 74.55 | 66.70 | |

Local single-writer cells are noisy run to run on this box (14.35 vs 19.55
for the same binary and cell); local comparisons need repetitions. Removing
the LIST matters locally too: `_versions/` listing grows with history, which
is why local throughput decays over a 20 s window.

At 64 writers on S3 the benchmark process is CPU-bound: 6.8 of 8 runtime
threads busy, RustFS 0.4 cores, toxiproxy 0.1 (workspace crates are
`opt-level = 0` in the dev profile). Optimized builds started for the final
numbers (`scripts/build-opt.sh`, separate target dir).

## Idea 4 — leader linger for the returning cohort

Hypothesis: batches alternate 1 / 15 entries because the first writer back
leads alone. A bounded linger (wait ≤ L ms while queued < last batch / 2).

| linger | w/s (S3, 16 writers) | commits/s |
|---:|---:|---:|
| 0 | 39.75 | 5.00 |
| 10 ms | 36.00 | 4.50 |
| 25 ms | 36.00 | 4.50 |

**Verdict: discard.** The writers settle into two cohorts that alternate (a
cohort prepares, ~67 ms, faster than the other cohort's batch, ~200 ms); the
linger rule never fires usefully and merging the cohorts would idle the
publisher.

## Idea 5 — pipelined batches (stage A overlaps the predecessor's publication)

Stage A (no gate): capture, admit against the effect log plus the in-flight
batch's pending effect, compose and commit detached on the pins the
in-flight batch will publish. Stage B (branch gate): wait for the
predecessor, check it published exactly the pins stage A assumed, publish.
The composing lead passes on as soon as a batch enters stage B. A failed
predecessor refuses the whole successor before its publication.

S3, 16 writers: commits/s 4.85 → 8.25 (publication capacity ×1.7), writes/s
37.25 (unchanged), p50 449 ms. Batch latency is unchanged (~230 ms: stage A
~107 ms + stage B ~115 ms) and closed-loop throughput follows per-write
latency (X = N / R), so at 16 writers batches just got smaller. To be
re-measured at 64 writers on the optimized build, where the publisher
saturates.

Invariant cost of pipelining: stage A's detached commits record the staging
witness of the head they were composed at (the predecessor's parent). Between
the predecessor's publication and this batch's, the collector's staging rule
("dead once the branch head moved past the recorded head") would judge them
dead; a concurrent `cleanup` could reclaim them before they publish. A
production version needs the witness to name the head they will publish on
(e.g. the predecessor's pre-minted nonce, which its commit id carries) and the
collector rule extended accordingly — RFC-level.

## Idea 6 — warm writer state for named branches (cross-branch lever)

Trace of one named-branch writer (baseline, S3): 637 ms, ~19 round trips. The
capture and revalidation each read the branch ref beside the HEAD pair, and
the publish reopened the branch's coordinator from durable state (LIST + GET
of main's `__manifest`, LIST of `_refs/branches`, ref GETs, LIST + GET of the
branch's `__manifest`) because the one-entry `merge_authority_cache` missed.

Changes: a group keeps the coordinator it last published through for a branch
other than the bound one, captures from it (no probe) and publishes through
it (`commit_updates_with_coordinator`); the publisher retains its acknowledged
image for named branches too (`retain_control_dataset_of(.., branches=true)`:
a branch's `__manifest` image has `base_paths`, which the retention excluded).
Delete-and-recreate stays refused: the publish precondition reads the exact
native ref the image names (`get_live_manifest_branch_contents`); a retired
ref is `BranchNotFound` → `ReadSetChanged`.

One named-branch writer, S3: 637 → 282 ms per write; 1.55 → 3.50 w/s;
21.8 → 9.1 requests/write.

## Idea 7 — probe-free capture for every writer: withdrawn

Measured as part of group4 (−1 RTT per write). With group commit forced on,
the engine integration suite found 4 contract breaks from it:
`composite_flow::composite_flow_schema_apply_then_branch_ops_no_deadlock_in_refresh`
(a query compiled against the catalog another handle's schema apply replaced
fails terminally), `warm_read_cost::recreated_branch_misses_cached_topology`
(the first attempt's identity came from a recreated branch's old incarnation),
`lifecycle::write_preparation_manifest_read_failures_carry_before_effect_evidence`
(a vanished `__manifest` lost `BeforeEffect` evidence),
`write_cost::branch_claim_adds_no_request_to_a_write` (probe count). A stale
capture is safe for the CAS, not for the write's meaning. Writers probe again
(switch `OMNIGRAPH_GROUP_COMMIT_WRITER_CAPTURE_SKIP=1` keeps the experiment);
the supported probe-free capture is the writer epoch claim (#1350), measured
as the `claim` variants. The leader still captures from its own last
publication, but only when the group's log explains every entry's capture.

## Correctness evidence (dev build, group commit forced on)

- `writes.rs`: 67/67 on and off. It caught a regression of the always-on
  publisher trust: a conditional write whose head check failed on the stale
  trusted image was refused terminally; now a check that fails on the trusted
  image is retried on a fresh load.
- Engine `integration`: 701/712 on (2 request-count deltas,
  `warm_read_cost::branch_delete_reuses_only_a_current_write_capture`,
  `warm_read_cost::named_read_borrows_write_capture_and_rejects_recreated_branch`:
  the group's own coordinator is not the handle's one-entry cache); 712/712 off.
- `failpoints.rs` (stopped after 25 tests): 20 pass, 5 fail or hang — seams on
  the ordinary path the group path bypasses (two-process `branch_claim_*`
  children never reach their seam) and the two #1331 cases (a write sent back
  by another handle's optimize is expected to be carried over / hold off the
  next run; group entries re-prepare outside the gates instead).
- Followers now keep the batch's completion evidence (`Unpublished` stays
  definite), found by `writes.rs::publisher_failure_before_the_manifest_put_is_definite_issue_1067`.

## Final matrix (optimized builds)

`scripts/build-opt.sh` builds every crate at opt-level 2 into
`/srv/target-opt`; binaries `baseline-opt` (ed09ee63 + instrument) and
`group10-opt` (e6c809c5). `scripts/matrix.sh` interleaves variants per
repetition: `opt-baseline`, `opt-group`, and for same-branch cells
`opt-baseline-claim`, `opt-group-claim` (writer epoch claimed on `main`),
`opt-group-pipe`. 3 repetitions at 16 and 64 writers, 1 at 1 and 4.
`scripts/summarize.py` prints medians.
