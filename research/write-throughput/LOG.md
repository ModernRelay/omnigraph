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
