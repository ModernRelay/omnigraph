//! S3 (object-store) cost-budget gate for the WRITE path — the bucket-gated twin of
//! `write_cost.rs` that proves RFC-013 **step 3a's data-table opener win**. On the
//! shared `helpers::cost` harness (`measure`/`IoCounts`/`assert_flat`/`s3_graph`).
//!
//! The opener term is an **object-store-RPC phenomenon**: latest-version resolution
//! costs per-version GETs/HEADs on S3 (O(depth) before step 3a, when writes routed
//! through the lance-namespace builder), which local FS cannot reproduce (one cheap
//! `read_dir` regardless). After step 3a (direct-by-URI opens), the per-write
//! **data-table read count is FLAT across commit-history depth** — the measured 70%
//! win. This file is the red→green acceptance for that term (it would be RED on the
//! pre-3a `from_namespace` opener); `write_cost.rs` gates the internal-table term on
//! local every-PR.
//!
//! **Isolating the opener (important):** total `data_reads` is not opener-only — the
//! same wrapped `Dataset` backs the merge-insert/RI **scan**, which reads
//! O(fragment-count) and grows with history for a *different* reason (compaction's
//! domain, not the opener; this is the term that made the *local* data-table count
//! grow). The shared harness's `PrefixCounter` attributes each read by object-key
//! prefix, so this gate asserts `data_opener_reads` (reads of `_versions/`/`.manifest`)
//! **directly** — no compaction or fixture massaging needed. After step 3a the opener
//! is O(1) regardless of version-history depth; before it grew ~+12/depth (RFC §2.4
//! [M]). (See `write_cost.rs` for the local test that proves the split itself —
//! opener flat, scan growing.)
//!
//! Skips gracefully without `OMNIGRAPH_S3_TEST_BUCKET` (the `tests/s3_storage.rs`
//! pattern); runs for real in the rustfs CI job (`.github/workflows/ci.yml`).
#![recursion_limit = "512"]

mod helpers;

use helpers::cost::{IoCounts, assert_flat, cost_harness, measure_insert, s3_graph};
use helpers::commit_many;

/// After step 3a the data-table opener term is flat across depth on a real object
/// store (the measured win). RED on the pre-3a namespace-builder opener (O(depth)
/// per-version resolution).
#[tokio::test]
async fn data_table_opener_is_flat_in_history_on_s3() {
    let Some(mut db) = s3_graph("write-cost-opener").await else {
        eprintln!(
            "SKIP data_table_opener_is_flat_in_history_on_s3: OMNIGRAPH_S3_TEST_BUCKET \
             unset (or store unreachable) — the S3 opener gate needs an object store"
        );
        return;
    };

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 50] {
        if d > current {
            commit_many(&mut db, (d - current) as usize).await;
            current = d;
        }
        let io = measure_insert(&mut db, &format!("s3_{d}")).await;
        current += 1;
        eprintln!(
            "depth~{d}: opener={} scan={} data_total={} __manifest={}",
            io.data_opener_reads,
            io.data_scan_reads,
            io.data_reads,
            io.manifest_reads
        );
        curve.push((d, io));
    }

    // The opener (latest-version resolution) is O(1) after step 3a (direct-by-URI),
    // isolated from the scan by the PrefixCounter. Slack absorbs object-store variance;
    // the pre-3a builder grew this ~+12/depth (RFC §2.4 [M]).
    assert_flat(&curve, |c| c.data_opener_reads, 8, "S3 data-table opener");
}

/// **The unlimited-history warm-write gate** (`docs/dev/unlimited-history-latency-plan.md`
/// §6 — U0). A warm single-row write's `__manifest` LIST count, round-trip count
/// (`num_stages`), reads, and data-table opener stay **FLAT across commit-history
/// depth with compaction but NO GC** (the unlimited-history-compatible state), and
/// under a shallow-depth ceiling that **ratchets down as Layers 3/4 land** (target,
/// per LanceDB's measured warm commit: `read_iops ≈ 1`, `num_stages ≈ 3`).
///
/// Runs inside `cost_harness` so the warm-coordinator freshness probe's reads land in
/// the ground-truth `__manifest` tracker (a per-op tracker installed at measure time
/// would miss reads on the long-lived coordinator handle — see `helpers::cost`).
///
/// NOTE on the numbers: the `assert_flat` slacks and the ceiling are seeded
/// conservatively; tighten them to the first real RustFS/CI run's printed values, and
/// ratchet the ceiling down as each U1 layer removes a LIST/round-trip.
#[tokio::test]
async fn warm_write_cost_flat_and_bounded_in_history_on_s3() {
    let Some(mut db) = s3_graph("write-cost-warm").await else {
        eprintln!(
            "SKIP warm_write_cost_flat_and_bounded_in_history_on_s3: \
             OMNIGRAPH_S3_TEST_BUCKET unset (or store unreachable)"
        );
        return;
    };

    cost_harness(async move {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        let mut current = 0u64;
        // Depth capped at 50 (matches the opener test). Going to 100 trips a Lance
        // FTS-index-builder panic during `optimize`'s `optimize_indices` merge on S3
        // (`lance-index/src/scalar/inverted/builder.rs`: `token_id_map[token_id] ==
        // u32::MAX`) over the fixture's `@index String` field — unrelated to this gate
        // and tracked separately. The 10→50 slope already catches O(history) growth.
        for d in [10u64, 50] {
            if d > current {
                commit_many(&mut db, (d - current) as usize).await;
                current = d;
            }
            // Compaction IS allowed under unlimited history (it packs fragments, never
            // deletes versions). GC/`cleanup` is NOT — never call it in this gate.
            db.optimize().await.unwrap();
            let io = measure_insert(&mut db, &format!("warm_{d}")).await;
            current += 1; // the measured write advanced depth by one
            eprintln!(
                "depth~{d}: manifest_list={} manifest_stages={} manifest_reads={} \
                 data_opener={} total_reads={}",
                io.manifest_list_requests,
                io.manifest_num_stages,
                io.manifest_reads,
                io.data_opener_reads,
                io.total_reads(),
            );
            curve.push((d, io));
        }

        // Core regression guard: every per-write term is FLAT across depth — no
        // O(history) round-trip survives. The LIST count is the headline (drives
        // toward ~1 as pinned opens + warm publish land); `num_stages` is the
        // round-trip proxy closest to the reports' "wall ≈ round-trips × RTT".
        // Measured baseline on RustFS (identical at depth 10 and 50, so perfectly
        // flat with compaction): list=6, num_stages=13, manifest_reads=9, opener=4,
        // total=14. Slacks are tight (the term is flat; the slack only absorbs minor
        // object-store variance). A real O(history) term would add several per 40
        // commits and trip these.
        assert_flat(&curve, |c| c.manifest_list_requests, 1, "S3 warm-write __manifest LIST count");
        assert_flat(&curve, |c| c.manifest_num_stages, 2, "S3 warm-write __manifest round-trips");
        assert_flat(&curve, |c| c.manifest_reads, 2, "S3 warm-write __manifest reads");
        assert_flat(&curve, |c| c.data_opener_reads, 1, "S3 warm-write data-table opener");
        assert_flat(&curve, |c| c.total_reads(), 2, "S3 warm-write total reads");

        // Absolute ceiling at shallow depth — the layer-progress tracker, seeded at
        // the measured baseline + 1. **Ratchet DOWN as each U1 layer removes a LIST**
        // (target: list ~1, num_stages ~3 — LanceDB's measured warm commit).
        let shallow = &curve[0].1;
        assert!(
            shallow.manifest_list_requests <= 7,
            "warm-write __manifest LIST ceiling exceeded: {} (baseline 6; a layer regressed?)",
            shallow.manifest_list_requests,
        );
        assert!(
            shallow.manifest_num_stages <= 14,
            "warm-write __manifest round-trip ceiling exceeded: {} (baseline 13)",
            shallow.manifest_num_stages,
        );
    })
    .await;
}

/// The S3 served-regime gate: the SAME warm-write depth sweep as
/// `warm_write_cost_flat_and_bounded_in_history_on_s3` but with NO `optimize()`
/// between depths. Pre-warm this was an `assert_grows` negative control (the cold
/// O(fragments) `read_manifest_scan` grew with history); Layer 4 (warm publish)
/// drove that scan to 0, so it is now `assert_flat` — the warm write's `__manifest`
/// scan stays flat across depth EVEN WITHOUT compaction, the load-bearing
/// unlimited-history property on the served path. The S3 mirror of the local twin
/// `write_cost.rs::served_regime_manifest_scan_is_flat_with_warm_publish`. Run under
/// RustFS in CI's `rustfs_integration` job.
#[tokio::test]
async fn served_regime_manifest_scan_is_flat_with_warm_publish_on_s3() {
    let Some(mut db) = s3_graph("write-cost-grows").await else {
        eprintln!(
            "SKIP served_regime_manifest_scan_is_flat_with_warm_publish_on_s3: \
             OMNIGRAPH_S3_TEST_BUCKET unset (or store unreachable)"
        );
        return;
    };

    cost_harness(async move {
        let mut curve: Vec<(u64, IoCounts)> = Vec::new();
        let mut current = 0u64;
        // Same 10→50 sweep as the flat gate (depth 100 trips the unrelated Lance
        // FTS-builder panic). The omission of `db.optimize()` is the whole point.
        for d in [10u64, 50] {
            if d > current {
                commit_many(&mut db, (d - current) as usize).await;
                current = d;
            }
            // NO `db.optimize()` here — so the `__manifest` fragments/rows
            // accumulate O(commits) and the publish-path scan reads grow with depth.
            let io = measure_insert(&mut db, &format!("grows_{d}")).await;
            current += 1; // the measured write advanced depth by one
            eprintln!(
                "depth~{d} (uncompacted): manifest_reads={} manifest_list={} total={}",
                io.manifest_reads,
                io.manifest_list_requests,
                io.total_reads(),
            );
            curve.push((d, io));
        }

        // INVERTED to the permanent served-regime gate now that Layer 4 (warm
        // publish) is active — exactly as the local twin
        // (`write_cost.rs::served_regime_manifest_scan_is_flat_with_warm_publish`).
        // The warm attempt-0 publish reuses the folded `known_state` and does ZERO
        // `__manifest` scan, so the per-write scan is flat across the 10→50 sweep
        // even WITHOUT compaction — the load-bearing unlimited-history property on
        // the served path (the freshness probe is a `_versions/` LIST, counted in
        // `manifest_list_requests`, not this scan term). If warm regresses to the
        // cold O(fragments) `read_manifest_scan`, this grows again and trips the
        // small slack. CI's rustfs_integration job is the running measurement.
        assert_flat(
            &curve,
            |c| c.manifest_reads,
            2,
            "S3 warm-write __manifest scan (served, warm-published)",
        );
    })
    .await;
}
