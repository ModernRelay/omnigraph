# Unlimited-history latency — the end-to-end plan

**Type:** forward execution plan (the live plan for this phase)
**Audience:** engine / storage maintainers
**Companions:** [latency.md](latency.md) (current-state synthesis — read for the validated cost model and production evidence), [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) (design archive), [write-latency-roadmap.md](write-latency-roadmap.md) (the bounded-history layered fix), [lance.md](lance.md), [versioning.md](versioning.md).

This is the end-to-end plan to make the engine serve **unlimited history at constant warm latency**. It states the goal, what is already done, the bounded-vs-unlimited fork, the LanceDB/Lance prior art that shapes the design, the phased plan (gate first), how it maps onto RFC-013's remaining steps, and the naming we adopt from the substrate.

---

## 1. Goal and non-goals

**Goal:** a warm single-row write is **O(1) in commit-history depth**, time-travel reads any version in **O(1)**, and **no history is ever deleted** (no GC). This is what the production `personal` graph needs — it is a memory graph where deep time-travel is a feature, not a luxury.

**Non-goal:** bounded history via GC. Garbage-collecting `__manifest` versions (RFC-013 Layer 1) makes the lists cheap by *deleting* history. That is a legitimate *separate* profile for deployments that do not need deep time-travel, and it is **mutually exclusive** with this goal. We do not pursue it here.

**Release constraint that drives sequencing:** 0.8.0 already forces a hard `v3 → v4` rebuild (strand + commit-graph retirement). **0.8.0 should be the last release that ever requires a rebuild.** So the one on-disk format change this work needs (head-pointer rows, `v4 → v5`) is folded into 0.8.0, so operators rebuild **once**. Deferring it would force a *second* rebuild later. This overrides the otherwise-correct instinct to defer a format change until a metric demands it: here the migration economics dominate.

---

## 2. Where we are (validated this cycle)

The cost model and production evidence are in [latency.md](latency.md); the short version:

- A warm single-row write does **~6 `_versions/` LISTs + 1 full `__manifest` scan + ~5 writes**. On R2 each LIST is a one-page read (production-measured: **205 KB at 689 versions**). Wall ≈ round-trip-count × RTT; ~94 % of wall is I/O wait.
- **Production ground truth** (read-only inspection of both graphs): both are **V2 naming** (so the bounded one-page model holds, no V1 worst case), both **internal-schema v3**. On the old R2 graph the *retired* `_graph_commits.lance` (723 versions) and `_graph_commit_actors.lance` (681) out-versioned `__manifest` (689) — they were the **two biggest per-op list amplifiers**, so Phase B retirement is a top-tier win, not a tidy-up.
- **Already shipped:** Phase 7 (lineage in `__manifest`), Phase B (retire the two tables), `__manifest` compaction (RFC-013 step 2), WriteTxn / opener bypass (step 3a — the *mutation* open is already pinned), #307 (one publish scan), the read-path warm-up. Confirmed by source: the warm `known_state` is **already folded incrementally** O(touched-tables) per commit, so the in-memory state the warm-publish step needs already exists.

---

## 3. The bounded-vs-unlimited fork

| | Bounded history (NOT this plan) | Unlimited history (this plan) |
|---|---|---|
| Latest-resolve | keep listing, make the list short via **GC (Layer 1+2)** | **stop listing**: pinned opens + forward-probe |
| Manifest scan | compaction bounds I/O; small keep-window | warm publish reuses folded `known_state`; **head-pointer rows** bound cold-open |
| Time-travel | lost below the keep-window | full, any version, O(1) (already true) |
| History | bounded | unbounded |
| Storage space | bounded | **unbounded (inherent — the price of keeping all history)** |
| GC | required | **never** |

The only thing unlimited history cannot escape is **storage space** (unread objects cost storage, not latency). Everything else is made O(1) by capturing once and reusing, not by deleting.

---

## 4. Prior art: LanceDB / Lance (researched against `lancedb/lancedb` + `lance-format/lance`)

LanceDB sits on the same `lance` substrate and hits the same S3 list-amplification, so its choices are direct evidence.

- **`read_consistency_interval` / `ConsistencyMode { Lazy, Strong, Eventual }`** (`lancedb/rust/lancedb/src/table/dataset.rs`). The configurable freshness budget for a warm handle. **Default Lazy = 0 IO between explicit reloads.** Strong checks every read; Eventual uses a TTL with background refresh inside a pre-TTL window (`BackgroundCache` + `refresh_window`) so a read never blocks unless fully expired. Freshness is checked via Lance's own `checkout_latest()`, not a hand-rolled list. This is a cleaner, named version of our implicit per-read probe, and a user-facing knob.
- **Lance deliberately rejected the fully-optimistic write.** `lance/rust/lance/src/dataset/write/commit.rs::test_commit_iops` comment: a fully-optimistic 0-read commit "would mean wasted write requests (txn + manifest) if there was a conflict. We choose to be pessimistic for more consistent performance." Their warm commit is **1 read (latest-version check) + 2 writes, flat in history** (`test_reuse_session`), because the shared `Session` metadata cache serves everything except the one latest-version check. Conflict retries reuse cached manifests, not re-reads.
- **`IoStats` exposes `num_stages`** (disjoint in-flight periods = round-trip/hop count) **and `requests`** (the full RPC log, dumped into `assert_io_eq!/lt!/gt!` panics) — `lance/rust/lance-io/src/utils/tracking_store.rs`. We already wrap this `IOTracker`; we just don't surface those two fields.
- **`test_reload_resets_consistency_timer`** is LanceDB's named regression for "a list call on every query after the interval expires" — the exact class our gate guards.
- **Version hint** (`latest_version_hint.json` + `probe_versions_upward`) is gated OFF for lexically-ordered stores. Our R2 is lexical V2 (production-confirmed), so the hint is unavailable, but a pinned-version open saves the **entire** list RPC anyway — which is the same outcome the hint would give.

**What LanceDB does that we should adopt:** the `read_consistency_interval` abstraction, the `num_stages` hop metric, the request-log-on-failure diagnostics, and the discipline of keeping one cheap pre-commit check rather than going fully optimistic. **Targets for our gate** (their measured warm commit): `read_iops ≈ 1`, `num_stages ≈ 3`, flat across depth.

---

## 5. The plan: U0 → U1 → U2 (all targeting 0.8.0)

- **U0 (now): the depth-swept S3 cost gate.** The acceptance test for everything below. §6.
- **U1 (no on-disk format change; freely reversible):**
  - **Layer 4 — warm publish.** Thread the already-folded `known_state` into publish attempt 0; fall back to cold `load_publish_state` only on real CAS contention. Keep ONE cheap pre-commit freshness check (the forward-probe), per Lance's lesson; do not go fully blind. (`db/manifest/publisher.rs`, `db/manifest.rs`.)
  - **Layer 3 — pinned-version opens** on the remaining write opens (staging open, data-table commit) via `open_table_dataset(location, pinned_version, session)`. List-free; the CAS stays the fence. (`db/omnigraph/table_ops.rs`, `exec/staging.rs`.)
  - **Forward-probe freshness + `ConsistencyMode`.** Replace the residual `latest_version_id` LIST with a forward HEAD-probe of `manifest_path(V+1)` from the coordinator's known version, behind a named `ConsistencyMode`/`read_consistency_interval` (default Strong-ish; Lazy/Eventual as a staleness budget for read-heavy graphs). (`db/manifest.rs`, `db/graph_coordinator.rs`.)
  - **Shared `Session`** on `open_dataset_tracked` / `open_manifest_dataset`, closing the cold-TLS-per-open asymmetry with `open_table_dataset`. (`src/instrumentation.rs`, `db/manifest/layout.rs`.)
  - **Branch-delete parallelize** — `buffer_unordered` over `owned_tables` in `cleanup_deleted_branch_tables`, pinned opens for those tables. (`db/omnigraph.rs`.)
- **U2 (the one format change, `v4 → v5`, in 0.8.0):** head-pointer `table_head:<table>` rows. §9.

---

## 6. U0 — the depth-swept S3 cost gate (the immediate deliverable)

An S3-backed test that builds commit-history depth, **compacts at each depth (allowed) but never GCs**, and asserts a warm single-row write's cost is **flat across depth** and **under a ceiling that ratchets down as each U1 layer lands**. It fails the moment the write path re-introduces an O(history) round-trip.

**Metrics** (most-faithful-to-the-reports first):
1. **`num_stages`** (round-trip/hops) — the reports' "wall ≈ round-trip-count × RTT" maps directly to this.
2. **`read_iops`** (total per-write read requests).
3. **LIST count** (the `_versions/` resolves) — ratchets 6 → ~1 as Layers 3/4 land.

Target after U1 (LanceDB-anchored): `read_iops ≈ 1`, `num_stages ≈ 3`, LIST ≈ 1 (or 0 with forward-probe), flat across depth.

**Harness additions** (`crates/omnigraph/tests/helpers/cost.rs`): surface `num_stages` and a `list_requests` count into `IoCounts` from the `IOTracker`'s `incremental_stats()` (`IoStats { num_stages, requests, .. }` is already available), counting LISTs by method on BOTH the `__manifest` ground-truth tracker and the data-table wrapper; adopt Lance's `assert_io_eq!/lt!/gt!` shape (dump the request log into the panic, reusing `last_manifest_reads()`).

**The test** (extend `crates/omnigraph/tests/write_cost_s3.rs`; bucket-gated; CI `rustfs_integration`): reuse `s3_graph`, `commit_many`/`commit_many_as`, `measure_insert`, `assert_flat`, the anchor-depth sweep `[10, 50, 100]`. **Run inside `cost_harness`** (the current `write_cost_s3.rs` test does not — required so the warm-coordinator probe's reads are ground truth). Compact at each depth (no `cleanup`). Assert `num_stages`/`read_iops`/`list_requests` flat across depth AND under a shallow-depth ceiling set to today's value (green now, ratchets per layer). Keep an `assert_grows`-without-compaction companion so the gate has teeth.

**Reuse map:** `helpers/cost.rs` (`IoCounts`, `assert_flat`, `measure_insert`, `cost_harness`, `last_manifest_reads`, `s3_graph`, `PrefixCounter`); `helpers/mod.rs` (`commit_many`, `commit_many_as`); `write_cost.rs` (`internal_table_scans_are_flat_in_history`, `internal_table_scans_grow_without_compaction`, `write_op_count_ceiling_at_shallow_depth` — the patterns to mirror on S3). Gotchas: `#![recursion_limit = "512"]`, `cost_harness` mandatory for ground-truth counting.

**LANDED (U0).** `warm_write_cost_flat_and_bounded_in_history_on_s3` (`write_cost_s3.rs`), measured on RustFS. **Baseline a warm single-row write must beat:** `manifest_list=6`, `num_stages=13`, `manifest_reads=9`, `data_opener=4`, `total=14` — **identical at depth 10 and 50** (perfectly flat with compaction, no GC). Depth capped at 50 (depth 100 trips an unrelated Lance FTS-index-builder panic during `optimize_indices` on S3; tracked separately). Each U1 layer ratchets the ceiling down toward `manifest_list ≈ 1`, `num_stages ≈ 3`.

---

## 6b. U1 execution plan (assumptions validated against source)

Two Explore passes validated the U1 surface (file:line in `crates/omnigraph/src/`). Findings and the resulting plan:

- **Layer 4 — warm publish.** `GraphNamespacePublisher::publish` runs a cold `load_publish_state` (open + `read_publish_scan`) on EVERY attempt (`db/manifest/publisher.rs:693`). The warm `known_state` exists in `ManifestCoordinator` (`db/manifest.rs:298`) and is folded O(touched-tables) after each commit, but the publisher is trait-boxed (`Arc<dyn ManifestBatchPublisher>`) with no access. **Design choice:** thread `known_state_hint: Option<&ManifestState>` into the `ManifestBatchPublisher::publish` signature (attempt 0 uses it, attempts 1+ reload after a CAS conflict; cold path unchanged when `None`) — vs guarding the attempt-0 load inside `ManifestCoordinator::commit`. **Recommend the `Option` hint param** (~50 lines, the `None` default keeps the contract). **Non-strict ops only**; keep one cheap pre-commit freshness check (the forward-probe), per Lance's "don't go fully optimistic" lesson.
- **Layer 3 — pinned opens.** Non-strict (Insert/Merge) opens at HEAD via `open_dataset_head_for_write` (`db/omnigraph/table_ops.rs:917`); the pinned version IS captured (`MutationStaging::expected_versions` / the `table_version` row) but not threaded. Route non-strict opens through the existing `open_table_dataset(location, pinned_version, session)` (`src/instrumentation.rs:241`). Strict ops (Update/Delete/SchemaRewrite — `MutationOpKind::strict_pre_stage_version_check`, `db/mod.rs:49`) keep the cold drift check.
- **Forward-probe.** The OCC path (`occ_snapshot_for_branch`, `exec/staging.rs:684`) already reuses the warm coordinator on probe-match, but the probe itself is a `latest_version_id()` LIST (`db/manifest.rs:536`). Add a `manifest_path_for_version(version)` constructor (V2 naming) and HEAD `V+1` instead of LISTing.
- **Shared `Session` (A).** Lives in `ReadCaches { session }` owned by `Omnigraph` (`src/runtime_cache.rs:275`, created `db/omnigraph.rs:369/473`); data reads use it via `TableHandleCache`, but manifest opens (`open_manifest_dataset` → `open_dataset_tracked`) don't. Thread `session` through `open_manifest_dataset` → `open_manifest_graph` → `ManifestCoordinator::open`/`open_at_branch`, passing `self.read_caches.session`. `DatasetBuilder::with_session` is already used by the data path. NOTE: this cuts cold-TLS wall latency but is **invisible to the request-count gate** (it removes handshakes, not requests) — verify via the connection/`num_stages` angle, not `read_iops`.
- **Branch parallelize (B).** `cleanup_deleted_branch_tables` (`db/omnigraph.rs:1482`) is a serial loop of independent `force_delete_branch` calls on the `Send + Sync` `TableStorage`. Swap to `buffer_unordered(8)`; adapt the `BRANCH_DELETE_BEFORE_TABLE_CLEANUP` failpoint test (drop the deterministic log-order assertion). Branch CREATE forks only `__manifest` (confirmed) — only delete is the serial-O(tables) cost.

**Internal sequencing** (impact-per-risk; each lands with a gate ratchet):
1. **Shared Session (A) + Branch parallelize (B)** — cheap, orthogonal, low-risk; land first. (A helps wall latency, B helps branch delete; neither moves the write request-count gate.)
2. **Layer 3** (pinned non-strict opens) — mechanical; gate: `data_opener` → ~0.
3. **Layer 4** (warm publish, `known_state_hint`) — biggest gate win; `manifest_reads` + `manifest_list` drop on the no-contention path.
4. **Forward-probe** — last (only matters after 3+4 remove the other LISTs); `manifest_list` → ~1.

**Invariant:** Layers 3/4 apply to non-strict (Insert/Merge) only; strict ops keep the cold drift check, so the publish CAS stays the sole concurrency authority.

---

## 7. RFC-013 mapping — interleave or not

- **Interleave (they ARE this phase):** Layers 3/4. The full `GraphPublishAuthority` / `PublishPlan` unification (RFC-013 step 5) is the clean home for Layer 4, but is a large refactor — do Layer 4 in the current publisher first; earn the refactor later.
- **Do NOT interleave (different profile, conflicts):** Layer 1 (GC `__manifest`) + Layer 2 (Q8 watermark). They delete history. Keep as a separate optional bounded-history track.
- **Pull forward (orthogonal, cheap, help now):** shared `Session`, branch-delete parallelize.
- **Already done (re-tense the RFC accordingly):** WriteTxn/opener bypass, internal-table compaction, Phase 7, Phase B, the cost-gate harness.

---

## 8. Naming / abstraction alignment with LanceDB

Principle: adopt the substrate's vocabulary where the concept is identical (we are an OSS library on Lance), keep our names where the concept is genuinely ours (multi-table-per-graph coordination, which LanceDB lacks).

- **Adopt:** `ConsistencyMode { Lazy, Strong, Eventual }` + `read_consistency_interval` (the freshness abstraction); `num_stages` + `requests`-log + `assert_io_eq!/lt!/gt!` (cost harness); a forward-probe helper named after `probe_versions_upward` / `checkout_latest`; `BackgroundCache` + `refresh_window` only if/when Eventual is built (don't add the abstraction before the feature).
- **Keep ours:** `ManifestCoordinator` / `GraphCoordinator`, `known_state`, `assemble_manifest_state` / `fold_inputs`, the `GraphNamespacePublisher` / `ManifestBatchPublisher` / `PublishPlan` family, and **"incarnation"** (branch-recreate identity Lance has no concept of). We already use Lance's `Session` / `ObjectStoreRegistry` / `IOTracker` directly.

---

## 9. U2 — the head-pointer format change (`v4 → v5`, lands in 0.8.0)

**What:** add a mutable `table_head:<table>` row kind to `__manifest` — the exact analog of the existing `graph_head:<branch>` lineage pointer — pointing at each table's current version. `assemble_manifest_state` and the publish/cold-open read **O(tables)** head rows instead of reducing **O(commits)** accumulated immutable `table_version` rows. The immutable `table_version` rows stay for time-travel (read from the historical Lance manifest version, not the hot path).

**Why it's the asymptotic wall:** `table_version` AND `graph_commit` rows accumulate one per commit (validated: `version_object_id` is per-version, rows are immutable, `assemble_manifest_state` reduces them). Compaction bounds the scan's I/O but not its row decode; GC bounds the list but not the rows; warm publish (Layer 4) avoids the scan on the warm path but **cold-open** and the **contention re-scan** still pay O(commits). Head-pointers bound those to O(tables). Phase 7 made this heavier by folding lineage into the manifest.

**Why now:** it bumps `INTERNAL_MANIFEST_SCHEMA_VERSION` to 5, and under the strand model a v5 binary refuses a v4 graph (export/import). Folding it into 0.8.0's single rebuild means operators rebuild once.

**Touch points:** `db/manifest/state.rs` (the `object_type` set + assembly), `db/manifest/publisher.rs` (write/update the head row in the publish CAS), `db/manifest.rs`, recovery (`db/manifest/recovery.rs`), and time-travel (`snapshot_at`). Needs Phase-7-depth recovery + snapshot + concurrency tests, plus the v4→v5 refusal test.

---

## 10. Acceptance criteria and verification

1. The U0 gate is GREEN at today's ceiling, and the `assert_grows`-without-compaction companion proves the term genuinely grows uncompacted (the gate has teeth).
2. As each U1 layer lands, ratchet the ceiling down toward `read_iops ≈ 1`, `num_stages ≈ 3`, LIST ≈ 1, staying flat across depth.
3. Run locally against RustFS/MinIO: `OMNIGRAPH_S3_TEST_BUCKET` + `AWS_*` (incl. `AWS_ENDPOINT_URL_S3`); `cargo test -p omnigraph-engine --test write_cost_s3`. Skips gracefully without the env; CI runs it in `rustfs_integration`.
4. For U2: recovery + time-travel coverage across the format change, and the v4→v5 internal-schema refusal test.

---

## 11. Risks and open questions

- **0.8.0 scope.** U2 is the only piece that bumps the on-disk format and MUST land in 0.8.0 or it forces a second rebuild. If 0.8.0 timing cannot absorb U2's risk, the fallback is to ship U1 in 0.8.0 and accept that U2 later forces another rebuild — decide explicitly at release-cut time.
- **Do not conflate compaction (allowed; bounds scan I/O) with GC/cleanup (forbidden for unlimited history).** The gate compacts, never cleans.
- **Layer 4 design.** Heed Lance's evidence: reuse warm state but keep one cheap pre-commit check; a fully-optimistic 0-read commit wastes write RPCs on conflict.
- **`ConsistencyMode` default** (Strong vs Lazy) is an open product question — Strong is safest (read-your-writes); Lazy/Eventual is a per-graph opt-in for read-heavy memory graphs.
- The local twin of the gate stays `write_cost.rs` (the list/round-trip cost is a per-RPC phenomenon invisible on local FS).
