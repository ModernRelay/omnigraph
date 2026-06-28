# Latency: write path and branch ops — validated state, production evidence, and roadmap

**Type:** synthesis / current-truth map (read this first for any latency work)
**Audience:** engine / storage maintainers
**Companions:** [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) (full design), [write-latency-roadmap.md](write-latency-roadmap.md) (the layered plan), [handoff-rfc-013-write-path.md](handoff-rfc-013-write-path.md) (handoff), [invariants.md](invariants.md), [lance.md](lance.md).

This doc consolidates: the two user-reported production latency problems, the cost model **validated against Lance 7.0.0 source and live production storage**, what has shipped, what is left, and the key architectural fork between *bounded* and *unlimited* history. Where a claim is grounded in code or measurement it says so; where it is a design not yet built it says so.

---

## 1. The two reported problems

Both were measured on the production `personal` graph / a cluster graph, on Cloudflare R2, server same-region (~17 ms warm RTT).

**A. Single-row write latency.** A single keyed-node insert on `main` measured **~7.2 s median**; delete ~7.6 s; insert+delete ~14.9 s. The cost is **not the writes** — it is read/list amplification, dominated by repeated "resolve latest version" listings of `_versions/` prefixes plus the `__manifest` scan. Actual PUT/POST was ~11–33 ms of the total.

**B. Branch-op latency.** `fork` (POST /branches) ~5.5 s / 448 S3 requests; `delete` (DELETE /branches/{b}) ~13.6 s / 1281 requests on a 112-table near-empty graph (production R2 saw branch create 6.25 s, delete 28.1 s). ~94 % of wall is I/O wait. The same ops against `file://` ran in 45 ms / 55 ms, and against in-cluster MinIO (sub-ms RTT) in 0.25 s / 0.46 s. So wall ≈ round-trip-count × per-request-latency, and the round-trip count is the amplifier.

The reporter's stop-gap (move storage from R2 to co-located MinIO) cut fork 4.7 s→0.14 s and delete 10.3 s→0.26 s by attacking per-request latency, not the round-trip count — so it regrows with history and forces storage co-location. The real fix is to cut the round-trip count.

---

## 2. Production evidence (validated against live storage, this investigation)

Two distinct graphs exist, which is why the report is hard to reproduce today:

| | Old graph (the report's subject) | Current graph |
|---|---|---|
| Storage | Cloudflare R2, bucket `ragnor-omnigraph-personal`, prefix `cluster/graphs/personal.omni/` | Railway bucket `omnigraph-os2`, `clusters/personal/graphs/personal.omni/` |
| Manifest naming | **V2** (20-digit zero-padded, reverse-sorted) | **V2** |
| Internal schema | **v3** (pre-strand, pre-Phase-7) | **v3** (rebuilt under v0.7.2) |
| `__manifest` versions | **689** | 6 |
| `_graph_commits.lance` versions | **723** (most-versioned dataset) | 6 |
| `_graph_commit_actors.lance` versions | **681** | 6 |
| Deepest node table | 280 | 11 |
| Datasets total | 221 | ~120 |

Findings that change the analysis:

1. **Both graphs are V2.** The V1 worst case (where `resolve_version_from_listing` iterates *every* manifest, O(N) across pages) never applied. No `migrate_scheme_to_v2` is needed.
2. **The 205 KB / ~500 ms `__manifest/_versions/` LIST the report measured is a 689-key V2 page.** `resolve_version_from_listing` reads the latest entry then `take(999)` as a one-page sanity check (`lance-table-7.0.0/src/io/commit.rs:572-603`); at 689 versions that is all 689 keys in one `ListObjectsV2` page. Root Cause A confirmed against the real object count.
3. **The retired commit-graph tables were the *worst* amplifiers.** `_graph_commits.lance` (723) and `_graph_commit_actors.lance` (681) out-versioned `__manifest` (689) — each per-op latest-resolve on them was a ~700-key page, mapping exactly onto the report's `_graph_commits.lance` list (429 ms / 212 KB) and `_graph_commit_actors.lance` list (451 ms / 206 KB). So **Phase B retirement removes the single largest chunk of the per-op list amplification**, not just "2 cold opens."
4. **The current graph is a v0.7.2 rebuild** — single-digit version chains, so the amplification is temporarily gone. The rebuild did Layer 1's job by hand once; depth regrows toward ~1000 as commits accumulate.

> Inspection was read-only `ListObjectsV2` plus one manifest `GET`, via the Railway/R2 S3 credentials. Reproducible method in the appendix.

---

## 3. Lance substrate facts (validated against `lance-table-7.0.0` source)

These pin what the substrate does, so we optimize *with* it (invariant 1), not around it.

- **Latest-version resolution is a one-page list on lexical stores.** `resolve_version_from_listing` (`commit.rs:552`): on a V2, lexically-ordered store it takes the first (latest) entry then `take(999)` to sanity-check ordering — one `ListObjectsV2` page, bounded at ~1000 keys regardless of total N. So per-list cost grows with depth up to ~1000 then **plateaus** at one page (~205 KB / ~500 ms on R2). It is bounded, not unbounded.
- **V2 naming is deterministic and reverse-sorted.** `manifest_path(base, version) = _versions/{u64::MAX - version:020}.manifest` (`commit.rs:103-108`); "Zero-padded and reversed for O(1) lookup of latest" (line 88). Both schemes share the `_versions/` directory (the enum doc saying `_manifests/` is stale). Lance 7.0.0 **defaults to V2** (`commit.rs:425, 743, 849`); omnigraph does not override it.
- **Opening a *known* version is list-free.** `with_version(N)` / `checkout_version(N)` builds `manifest_path(N)` directly and does one GET. `resolve_version_from_listing` runs only when resolving *latest*. Validated by omnigraph's `warm_read_cost.rs` (0 opens on warm repeat).
- **Lance has a latest pointer and a forward-probe — both gated off for lexical stores.** `latest_version_hint.json` + `write_version_hint` (best-effort, correctness-independent) and `read_version_hint_and_probe` / `probe_versions_upward` (HEAD version V+1, V+2 … until 404). `uses_version_hint = version_hint_globally_enabled() && !list_is_lexically_ordered` (`commit.rs:327-328`) — disabled on S3/R2; the `LANCE_USE_VERSION_HINT` env can only *disable*, never force-enable. So the substrate already implements the O(1) latest-resolution we want; it just declines it on lexical stores because it considers the one-page list "roughly one request."
- **Compaction packs fragments; it does not delete rows.** `compact_files` rewrites fragments; it adds a version. Only `cleanup_old_versions` removes versions.

---

## 4. The validated cost model

### 4.1 Single-row warm write (RFC-013 cost model, reconciled to the served trace)

A warm server insert of one keyed node on `main` performs **6 `_versions/` LISTs + 1 full `__manifest` scan + 1 recovery LIST + ~5 writes**. The six latest-resolves are: staging open, OCC re-capture probe, data-table drift probe, `commit_staged`, `load_publish_state` open, publish merge-insert commit (plus the `__manifest` scan inside `load_publish_state`). Each LIST is one page whose cost scales with that dataset's version depth (Root Cause A); the *count* of six is Root Cause B.

**Two root causes:**
- **A — per-LIST cost grows with history (un-GC'd `_versions/`).** Bounded one-page, but ~205 KB / ~500 ms at depth ≥ ~700. `cleanup` covers only catalog data tables, so the internal tables grow forever (`optimize.rs` `all_table_keys`). NOTE: `optimize` already *compacts* `__manifest` (RFC-013 step 2), bounding the **scan** I/O; only `cleanup` shrinks the **list**.
- **B — the write re-resolves "latest" 6× by listing.** Each boundary independently asks "what's latest?" via a list, instead of opening at the version the `__manifest` row already pins.

**A third cost the production data forced into view — the manifest row-accumulation wall.** `table_version` and `graph_commit` rows are **immutable, one inserted per commit** (`version_object_id = "{table}${version}"` is per-version; `manifest.rs:431` "one immutable table_version row per updated table"; `assemble_manifest_state` reduces them to latest-per-table). So the current `__manifest` carries **O(commits) rows**. Compaction bounds the scan's *I/O* (fragments) but not its *decode* (rows). GC bounds the *list*, not the rows. At unlimited history this scan is the asymptotic wall, and Phase 7 made it heavier by folding lineage into the manifest as accumulating `graph_commit` rows.

### 4.2 Branch ops

- **Branch create is NOT O(num-tables).** `branch_create_from_impl` forks only `__manifest` (`omnigraph.rs`); tables fork lazily on first write (`fork_branch_from_state`). The report's "4 req/table" is a normalization, not measured per-table scaling. Create's cost is the same `__manifest` (+ on v3, commit-graph) list amplifier as the write path, plus "opens the commit graph twice" and re-validates the schema.
- **Branch delete IS O(num-forked-tables) and serial.** `delete_branch_storage_only` computes the branch's forked tables, flips manifest authority (O(1)), then `cleanup_deleted_branch_tables` runs a **plain serial `for` loop**, each iteration a `force_delete_branch` round-trip (`omnigraph.rs`). No `buffer_unordered`/`join_all`. So delete scales with the number of tables written on the branch, with zero concurrency.
- **Manifest/branch opens pay a cold connection.** `open_table_dataset` (read path) attaches the shared per-graph `Session`; `open_dataset_tracked` (manifest/branch opens) does not (`instrumentation.rs:244-249` vs `216`). So every manifest/branch open pays a fresh TLS handshake — matching the 115/288 fresh TCP connections the report captured.

---

## 5. What we have done

| Work | Effect | Status |
|---|---|---|
| #307 — halve per-write `__manifest` scans | `load_publish_state` does ONE `read_publish_scan` for table state + tombstones + lineage (was multiple) | shipped (production edge) |
| #308 — stage the delete path | deletes go through `stage_delete` + `commit_staged`; the cost model now applies to deletes too | shipped |
| WriteTxn / capture-once (RFC-013 step 3b) | collapses the per-table mutation open and the index-build reopen (cost-model steps 4, 10 → 0) | shipped |
| Compaction of internal tables (step 2) | `optimize` compacts `__manifest`, bounding the publish **scan** I/O on a periodically-optimized graph | shipped |
| Read-path warm-up (PR #268) | warm same-branch read: 0 manifest opens, 1 probe, version-pinned held-handle cache — the read twin of the write fix | shipped |
| **Phase B — retire `_graph_commits.lance` / `_graph_commit_actors.lance`** (this branch) | removes the **two biggest per-op list amplifiers** (723/681 versions on the old graph) and folds lineage into `__manifest` | **shipped on this branch** |
| Strand storage versioning + version surfacing (this branch) | one internal-schema version, export/import upgrade; `version`/`snapshot`/`healthz` expose it | **shipped on this branch** |

---

## 6. What is left — the layered fix

Ordered roughly by impact-per-risk. Layers 1/3/4 are independently shippable and correctness-preserving; full design in [write-latency-roadmap.md](write-latency-roadmap.md) and [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md).

- **Layer 1 — version-GC `__manifest` in `cleanup`** (attacks Root cause A; bounded-history). Keep-window (e.g. 20–50), shrinking each LIST from ~1000 keys to ~20–50. A *manual, quiesced, sidecar-refusing* cleanup is correct without the watermark; a one-shot quiesced `__manifest` cleanup is the zero-code operational lever. **Deletes history below the window** — see §7.
- **Layer 2 — Q8 durable monotonic watermark** (makes Layer 1 safe under live writers). A Lance boundary tag that `cleanup_old_versions` protects; writers/opens reject `version ≤ boundary`. Heaviest, invariant-touching; correctly deferred.
- **Layer 3 — open data tables at the pinned manifest version** (attacks Root cause B; unlimited-history-compatible). Route the non-strict `reopen_for_mutation` through `open_table_dataset(location, pinned_version, session)` instead of `Dataset::open` at HEAD. List-free; the publish CAS + drift guard remain the fences.
- **Layer 4 — optimistic warm publish** (attacks Root cause B; unlimited-history-compatible). Thread the warm coordinator's open `__manifest` handle + in-memory `known_state` into publish attempt 0; fall back to cold `load_publish_state` only on real CAS contention. A warm single-writer commits with zero manifest open/scan.
- **Branch-op workstream (the reporter's "step 6", not yet in the roadmap):**
  - Parallelize the per-table fork reclaim on delete (`buffer_unordered` over `owned_tables`).
  - Open those tables version-pinned, not at HEAD.
  - Share the `Session`/pooled client on the manifest/branch path (close the `open_dataset_tracked` asymmetry) so branch and write ops stop paying a cold TLS handshake per open.
- **Head-pointer rows (the unlimited-history cold-open enabler; new, from this investigation).** Add superseded `table_head:<table>` rows (mirroring `graph_head:<branch>`) so the publish/open reads O(tables+branches) head rows instead of O(commits) accumulated rows; keep the immutable per-commit rows for time-travel only. This is what bounds cold-open and absorbs the lineage rows Phase 7 moved into the manifest.
- **Forward-probe freshness (unlimited-history latest-resolve).** Replace the residual `__manifest` `latest_version_id` LIST with a forward HEAD-probe of `manifest_path(V+1)` from the warm coordinator's known version — O(versions-since-last-check), independent of total depth. Feasible because V2 naming is deterministic and Lance already implements the pattern for non-lexical stores.

---

## 7. The architectural fork: bounded vs unlimited history

This is the decision that determines which of the above you build. **They are different strategies, and Layer 1 conflicts with the unlimited-history goal.**

| | Bounded history | Unlimited history (constant low warm latency) |
|---|---|---|
| Latest-resolve | keep listing, make the list short via **GC (Layer 1 + 2)** | **stop listing**: pinned opens (Layer 3) + forward-probe head (Layer 4 + probe) |
| Manifest scan | compaction bounds I/O; rows still O(commits) but window is small | warm publish reuses in-memory `known_state` (Layer 4) → no warm scan; **head-pointer rows** bound cold-open |
| Time-travel | lost below the keep-window | full, any version, O(1) per version (pinned opens — already true) |
| History retention | bounded by keep-N | unbounded |
| Storage space | bounded | **unbounded (inherent — the cost of keeping all history)** |
| GC | required | **not used** |

Key facts behind the unlimited column, all validated this session:
- **Reads/time-travel are already O(1) per version** (`with_version(N)` → one GET; `snapshot_at` → `checkout_version`). Unlimited history does not slow reads.
- **Per-list cost already plateaus on V2** (one page), so unlimited history is a bad *constant*, not a runaway; Layers 3/4 turn the bad constant into a low one without deleting anything.
- **Warm writes become O(1)** with pinned opens + warm in-memory state + forward-probe — no lists, no scan, no GC.
- **The residual is cold-open** (O(commits) scan to rebuild `known_state`), amortized over server lifetime and bounded by the head-pointer change.

Recommendation: pick the profile explicitly per deployment. The personal graph wants **unlimited history** (it is a memory graph), so the target is Layers 3/4 + head-pointers + forward-probe + the branch-op workstream, and **not** Layer 1. A graph that does not need deep time-travel can take the cheap Layer-1 path.

---

## 8. Correctness constraints (do not regress these while optimizing)

Per [invariants.md](invariants.md). Several reads are explicitly correctness-motivated; do not remove without preserving:
- the publish CAS as the **sole** concurrency authority (row-level merge-insert on `object_id`);
- snapshot isolation (a query holds one captured snapshot; no mid-query head re-read);
- expected-table-version checks on strict ops;
- recovery from "table commit succeeded / manifest publish failed" (the sidecar sweep);
- multi-writer safety in cluster mode.

Layers 3/4 are correct because they separate **correctness (the CAS) from optimization (every read/probe)**: the optimistic path is probe-free and the cold path runs only under genuine contention. No parallel copy of version state is introduced (deny-listed); Lance + the manifest stay the single source of truth (invariant 15).

---

## 9. Verification

- `crates/omnigraph/tests/write_cost.rs` — local internal-table scan term flat across commit depth (`internal_table_scans_are_flat_in_history`), per-write read-op ceiling, staged-write fitness.
- `crates/omnigraph/tests/write_cost_s3.rs` — bucket-gated data-table opener term, flat across depth on S3.
- `crates/omnigraph/tests/warm_read_cost.rs` — the read twin (0 opens warm).
- Each new layer lands with a `write_cost_s3.rs` extension asserting the per-op LIST count drops and stays flat across commit-history depth. The shared harness is `tests/helpers/cost.rs` (`measure`/`IoCounts`/`assert_flat`).

---

## 10. Appendix: reproducible production inspection

To re-measure a production graph's naming scheme, version depth, and stamp (read-only):

1. Credentials: Railway via `railway run --service omnigraph-server -- <cmd>` (injects `AWS_*` + `OMNIGRAPH_CLUSTER`); R2 via `op read 'op://Centaur/R2_ACCESS_KEY_ID/credential'` etc. and endpoint `https://<account>.r2.cloudflarestorage.com`.
2. `ListObjectsV2` on `<graph>/__manifest/_versions/`: a 20-digit zero-padded filename is V2, a plain integer is V1; count `.manifest` objects for depth.
3. Per-dataset depth: list `_versions/` under each `nodes/*`, `edges/*`, and the internal tables to find the amplifiers.
4. Stamp: `GET` the latest manifest file and search the bytes for `omnigraph:internal_schema_version` (the value follows the key in the schema-metadata map).

This is how the §2 numbers were obtained.
