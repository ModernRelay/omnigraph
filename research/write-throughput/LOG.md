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
