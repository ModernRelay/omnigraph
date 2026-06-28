# Write-latency roadmap (validated cost model + layered fix)

**Type:** planning roadmap (forward-looking; not yet implemented)
**Companion to:** [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) (design) and [handoff-rfc-013-write-path.md](handoff-rfc-013-write-path.md) (current state + next actions)
**Status:** validated against Lance 7.0.0 source + a measured S3/RustFS trace of the `a7d4cba5` (#307) binary; **re-validated unchanged against HEAD `0dcdcf5a` (#308)** — see "Currency" below

This doc records the **validated** root-cause analysis of single-row write latency and the layered, correct-by-design fix, so the analysis is not lost between sessions. It also records how the separate commit-graph-table **retirement** work (see the strand-and-rebuild plan) feeds into it.

## Currency (re-validated at HEAD `0dcdcf5a`, #308)

#308 ("Stage the delete path; retire the inline-delete residual", MR-A) is a **pure delete-path refactor**: it moved `delete_where` off the inline `Dataset::delete` HEAD-advance onto Lance 7.0's two-phase `stage_delete` (`DeleteBuilder::execute_uncommitted`) + `commit_staged`. The **insert path is untouched** — every step of the cost model below re-traced identically (the `stage_merge_insert` + `commit_staged` open/commit count is unchanged; WriteTxn "collapse #1/#4", the probe-gated `occ_snapshot_for_branch`, and the publisher chain all survive). Two consequences worth recording: (1) deletes are now staged like inserts, so this cost model — and the layered fix — **now apply uniformly to deletes too** (before #308 a delete was an inline-commit residual with a different shape); (2) the sole remaining inline-commit residual is `create_vector_index`, and D2 (constructive XOR destructive per query) is now a **permanent by-design boundary, not a gap**. Lance stays pinned at 7.0.0, so Root-cause-A's `commit.rs` / `cleanup.rs` references are unchanged.

## Problem

A single keyed-node insert on `main` against the production graph (Cloudflare Container + R2, 343 MB, never cleaned) measured **~7.2s median**. The cost is not the writes — it is read/list amplification, dominated by repeated "resolve latest version" listings of `_versions/` prefixes.

## Validated cost model of one warm write

A warm server insert of one keyed node on `main` performs **6 `_versions/` LISTs + 1 full `__manifest` scan + 1 recovery LIST + ~5 writes**, every operation reconciled to code and matching the measured served trace exactly (3 `__manifest/_versions/` lists, 3 `node:SyncState/_versions/` lists, ~132 `__manifest` reads, 1 recovery list, 8 PUT + 1 POST):

| # | Operation | Cost | Source |
|---|---|---|---|
| 1 | recovery sidecar list | 1 `__recovery/` LIST | `recovery.rs` heal |
| 2 | schema-contract validate | 3 text GETs (not manifest) | `schema_state.rs` |
| 3 | branch resolve (warm) | 0 I/O (in-memory coord) | `omnigraph.rs` `resolved_branch_target` |
| 4 | per-table mutation open | **0** — WriteTxn returns `None` (RFC-013 collapse #1) | `table_ops.rs` |
| 5 | staging open (`reopen_for_mutation`) | **1 `node:SyncState/_versions/` LIST** (opens at *latest*) | `table_store.rs` `open_dataset_head_for_write` |
| 6 | OCC re-capture probe | **1 `__manifest/_versions/` LIST** (`latest_version_id`) | `manifest.rs` `probe_latest_version` |
| 7 | data-table drift probe | **1 `node:SyncState/_versions/` LIST** | `staging.rs` |
| 8 | sidecar PUT | 1 PUT | `staging.rs` `write_sidecar` |
| 9 | `commit_staged` (data commit) | **1 `node:SyncState/_versions/` LIST** (Lance commit) + PUTs | `table_store.rs` |
| 10 | index-build reopen | **0** — reuses handle (RFC-013 collapse #4) | `table_ops.rs` |
| 11 | `load_publish_state` | **1 `__manifest/_versions/` LIST + 1 full scan (~132 reads)** | `publisher.rs` |
| 12 | publish merge-insert commit | **1 `__manifest/_versions/` LIST** (Lance commit) + 1 POST | `publisher.rs` `merge_rows` |
| 13 | sidecar DELETE | 1 DELETE | `mutation.rs` |

The WriteTxn (RFC-013 step 3b) and #307 already eliminated several opens (steps 4, 10) and the redundant publish scans. What remains is structural.

## Two independent root causes

**Root cause A — per-LIST cost grows with history (un-GC'd `_versions/`).** On S3/R2, Lance resolves "latest version" via `resolve_version_from_listing` (`lance-table-7.0.0/src/io/commit.rs:552`). Even though V2 reverse-sorted naming puts the latest first, it reads **up to 1000 entries as a lexical-order sanity check** (`commit.rs:584`, `valid_manifests.take(999)`). So each LIST pulls a full ~1000-key page (~205 KB, ~500 ms on R2). `cleanup` covers only catalog data tables — **the internal tables are never version-GC'd** (`optimize.rs` `all_table_keys(&db.catalog())`), so `__manifest/_versions/` grows one entry per commit forever. The version hint that would skip the list is **unavailable on S3/R2** — Lance gates it to non-lexical stores (`commit.rs:327`, `uses_version_hint = enabled && !list_is_lexically_ordered`); `LANCE_USE_VERSION_HINT` can only *disable* it. NOTE: `optimize` already *compacts* the internal tables (RFC-013 step 2), which bounds the `__manifest` data **scan** (step 11), but compaction adds a version — only `cleanup` shrinks the `_versions/` **list**.

**Root cause B — the write re-resolves "latest" 6× by listing instead of reusing a known version.** Each boundary (table open, OCC probe, table commit, manifest open, manifest commit) independently asks "what's latest?" via a list. Opening at a *known* version is list-free: `DatasetBuilder::from_uri(loc).with_version(N)` reconstructs the exact V2 filename and does one GET — the read path already uses this (the held-handle cache). The write path deliberately opens HEAD and never consults the pinned pointer, even though the `__manifest` `table_version` row already stores `manifest_path` + `version` + `e_tag` (`TableVersionMetadata`).

## The layered fix (each layer correct on its own)

Target end state: **a warm single-writer insert costs O(keep-N + writes), independent of total history, with the publish CAS as the sole correctness authority.** Ordered by impact-per-risk.

### Layer 1 — version-GC the internal table(s) (attacks Root cause A; biggest win)
Bring `__manifest` into `cleanup` with a small keep-window (e.g. keep 20–50), shrinking every LIST from ~1000 keys (~205 KB, ~500 ms) to ~20–50 keys (~5 KB, ~40 ms) — a ~10× cut to the dominant term, reusing the `cleanup_all_tables` loop. Lance's `cleanup_old_versions` supports this directly (`CleanupPolicy { before_version, before_timestamp }`, always preserves latest, honors tags — `lance-7.0.0/src/dataset/cleanup.rs`).
- **Correctness:** a *manual, quiesced, sidecar-refusing* cleanup is correct **without** the Q8 watermark (the resurrection race only exists under concurrent live writers). Time-travel below the window is lost — the same tradeoff data-table cleanup already makes.
- **Immediate operational fix:** a one-shot quiesced `__manifest` cleanup on the production graph takes ~7s → ~1s today, zero code risk.
- **Connection to the retirement work:** after the commit-graph-table retirement, `_graph_commits`/`_graph_commit_actors` no longer exist, so Layer 1's GC scope is **just `__manifest`** (originally three internal tables). Retiring those tables also removes their 2 cold-open LISTs entirely — an orthogonal cold-start win.

### Layer 2 — Q8 durable monotonic watermark (makes Layer 1 safe under live writers)
Lance creates versions with a bare `PutMode::Create` and no monotonic guard, so a stalled writer can resurrect a GC'd version = a silent lost write on R2/S3. To run GC automatically while serving writes, add a durable boundary watermark (a Lance boundary **tag**, which `cleanup_old_versions` protects): GC advances the floor before deleting; every writer/open rejects `version ≤ boundary`. Invariant-level, touches the open path; correctly deferred in RFC-013 (§ step 2b / Q8). See [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) and [handoff-rfc-013-write-path.md](handoff-rfc-013-write-path.md) for the full design.

### Layer 3 — open data tables at the pinned manifest version (attacks Root cause B)
On the **non-strict** insert/merge path, route `reopen_for_mutation` through the version-pinned opener (`open_table_dataset(location, pinned_version, session)`) instead of `Dataset::open` at HEAD. The `__manifest` row already pins the exact version + e_tag, and the non-strict path already skips the strict version check — so opening at the pinned version is information-equivalent and **list-free** (removes step 5, folds the drift probe). The publisher CAS + `latest_version_id` drift guard remain the concurrency fences.

### Layer 4 — optimistic warm publish (attacks Root cause B)
The publisher loop runs cold `load_publish_state` (open + full scan) on every attempt, then arbitrates via the merge-insert row-level CAS — and the CAS is the stated authority. Thread the warm coordinator's already-open `__manifest` handle + in-memory `known_state` (folded after every commit, RFC-013 PR2 #1b) into **attempt 0**; fall through to today's cold `load_publish_state` on attempts 1..N **only when the CAS actually conflicts**. A current warm view (single-writer server, the common case) commits with zero manifest open/scan. Scope to non-strict ops; strict ops (update/delete/schema) keep the cold fresh-read drift check.

## Why this is the optimal shape (not a symptomatic patch)

- It is the natural extension of the WriteTxn "capture-once" architecture (RFC-013 step 3b) that already eliminated the per-table mutation opens — Layers 3–4 finish the job for the publish + commit opens.
- It separates **correctness (the CAS) from optimization (every read/probe)**, matching invariant 15: the optimistic path is probe-free; the cold path runs only under genuine contention. Write cost becomes O(1) with no contention, O(contention) only under real concurrency.
- It introduces **no parallel copy of version state** (deny-listed) — Lance + the manifest stay the single source of truth; we stop *re-deriving* "latest" from a cold list on the hot path (the "cold re-derivation" anti-pattern, invariant 15).
- Explicitly **not** pursued: the version hint (unavailable on S3) or an external manifest store (a drift-prone parallel pointer). GC + capture-once reuse is the substrate-respecting path.

## Sequencing

0. **Prereq (separate plan): retire `_graph_commits.lance` + `_graph_commit_actors.lance`. — DONE.** Phase B of the strand-and-retire storage-versioning work landed this: the tables are gone, Layer 1's GC scope is `__manifest`-only, and the 2 cold-open LISTs are removed.
1. **Operational now:** one-shot quiesced `__manifest` cleanup on the production graph (~7s → ~1s, no code).
2. **Layer 1:** wire `__manifest` into the `cleanup` command (manual/quiesced + sidecar-refuse) — the 80/20.
3. **Layers 3 + 4:** open-at-pinned-version + optimistic warm publish — remove the redundant round-trips; bring a warm write to ~2 cheap Lance-commit lists + writes.
4. **Layer 2:** Q8 watermark — only when GC must run continuously under live writers; heaviest, correctly deferred.

Layers 1+3+4 are independently shippable, each correctness-preserving, composing to the scalable end state. Each lands with `write_cost_s3.rs` extensions asserting the per-write LIST count drops and stays flat across commit-history depth.
