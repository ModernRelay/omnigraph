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
        for d in [10u64, 50, 100] {
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
        assert_flat(&curve, |c| c.manifest_list_requests, 1, "S3 warm-write __manifest LIST count");
        assert_flat(&curve, |c| c.manifest_num_stages, 2, "S3 warm-write __manifest round-trips");
        assert_flat(&curve, |c| c.manifest_reads, 6, "S3 warm-write __manifest reads");
        assert_flat(&curve, |c| c.data_opener_reads, 4, "S3 warm-write data-table opener");
        assert_flat(&curve, |c| c.total_reads(), 8, "S3 warm-write total reads");

        // Absolute ceiling at shallow depth — the layer-progress tracker. Ratchet
        // DOWN as U1 lands (target list ~1, num_stages ~3). Seeded generously.
        let shallow = &curve[0].1;
        assert!(
            shallow.manifest_list_requests <= 8,
            "warm-write __manifest LIST ceiling exceeded: {} (tighten/ratchet per the plan)",
            shallow.manifest_list_requests,
        );
    })
    .await;
}
