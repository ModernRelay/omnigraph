//! Cost-budget tests for the WRITE path (RFC-013 step 1) — the safety/latency
//! twin of `warm_read_cost.rs`, on the shared `helpers::cost` harness. A
//! committing write's per-table opens and internal-table scans must be bounded
//! and **flat across commit-history depth**, measured at the object-store
//! boundary. Guards invariant 15 (cost bounded by work, not history) on writes.
//!
//! **Backend split (see docs/dev/testing.md / RFC-013).** This file runs on
//! **local FS** and gates the **internal-table** term (`__manifest`/`_graph_commits`
//! fragment scans, ~+18/depth — O(fragments) on any backend, step 2's target).
//!
//! The **data-table opener** term (step 3a's win) is a per-object-store-RPC
//! phenomenon and is NOT gated here: local-FS latest-resolution is cheap whether
//! the open goes through the namespace builder or direct-by-URI, so the
//! namespace→direct switch is invisible on local. Measured: the local data-table
//! read count grows with depth too (~+0.9/depth), but that is a *different* term —
//! the merge-insert/RI scan reading O(depth) **fragments**, unchanged by the
//! opener switch (depth-100 = 92 ops both before and after step 3a, same slope)
//! and reduced only by compaction. The opener term shows up only on a real object
//! store (per-version GETs, ~+12/depth → flat after step 3a), so it is gated in
//! `write_cost_s3.rs` (bucket-gated). Same `measure`/`IoCounts` harness, different
//! backend; each term gated where it actually manifests.
#![recursion_limit = "512"]

mod helpers;

use helpers::cost::{
    IoCounts, assert_flat, assert_grows, local_graph, measure_insert, measure_with_staged,
};
use helpers::{MUTATION_QUERIES, commit_many, mixed_params};

// ── (A) The internal-table LOCK — RED today, the acceptance test for step 2 ──
//
// `__manifest` / `_graph_commits` scans must be O(1) in commit-history depth.
// RED today (O(fragments), uncompacted). Un-ignore when step 2 (internal-table
// compaction) lands — it must go green flat. (The data-table term is the S3
// gate's, `write_cost_s3.rs`; local-FS hides it.)
#[tokio::test]
#[ignore = "RED until step 2 (internal-table compaction): __manifest/_graph_commits scans are O(fragments) today — RFC-013 §0/§2.2. Un-ignore there as the red→green acceptance test."]
async fn internal_table_scans_are_flat_in_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 100] {
        if d > current {
            commit_many(&mut db, (d - current) as usize).await;
            current = d;
        }
        let io = measure_insert(&mut db, &format!("lock_{d}")).await;
        current += 1; // the measured write advanced depth by one
        eprintln!(
            "depth~{d}: data={} __manifest={} _graph_commits={}",
            io.data_reads, io.manifest_reads, io.commit_graph_reads
        );
        curve.push((d, io));
    }

    assert_flat(&curve, |c| c.manifest_reads, 4, "__manifest scan");
    assert_flat(&curve, |c| c.commit_graph_reads, 4, "_graph_commits scan");
}

// The data-table OPENER history-gate (opener flat across depth) lives in
// `write_cost_s3.rs` — its history-dependence is an S3-only phenomenon. But the
// *probe that isolates* the opener (the `PrefixCounter` split) is validated here,
// every-PR, on local FS:

/// Proves the `PrefixCounter` opener/scan split: a committing write's data-table
/// reads divide into a **flat opener** term and a **growing scan** term. This pins
/// (a) the classifier actually attributes reads to the opener bucket (non-zero, so a
/// flat assertion isn't vacuously flat-at-zero), and (b) the local data-table growth
/// is the merge-insert/RI fragment scan, not the opener — which is *why* the S3
/// gate asserts `data_opener_reads`, not total `data_reads`. (On local FS the opener
/// is O(1) regardless of step 3a; the opener's history-dependence is gated on S3.)
#[tokio::test]
async fn data_table_reads_split_into_flat_opener_and_growing_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 100] {
        if d > current {
            commit_many(&mut db, (d - current) as usize).await;
            current = d;
        }
        let io = measure_insert(&mut db, &format!("split_{d}")).await;
        current += 1;
        eprintln!(
            "depth~{d}: opener={} scan={} data_total={}",
            io.data_opener_reads, io.data_scan_reads, io.data_reads
        );
        curve.push((d, io));
    }

    assert!(
        curve[0].1.data_opener_reads > 0,
        "opener reads must be > 0 — the classifier missed version-resolution reads, \
         so a flat opener assertion would be vacuous"
    );
    assert_flat(&curve, |c| c.data_opener_reads, 4, "local data-table opener");
    assert_grows(&curve, |c| c.data_scan_reads, 20, "local data-table scan");
}

// ── (B) Green-today regression guards — run on every PR ──

/// A single insert's *data-table* write cost is O(1): the table commit is a small
/// constant number of writes, independent of history.
#[tokio::test]
async fn single_insert_data_write_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    commit_many(&mut db, 5).await;
    let io = measure_insert(&mut db, "w").await;
    eprintln!("single insert: data_writes={}", io.data_writes);
    assert!(io.data_writes <= 4, "data-table write_iops should be a small constant, got {}", io.data_writes);
}

/// At a fixed shallow depth, the per-write object-store read count is below a
/// documented ceiling. Fails the moment a change *adds* a round-trip on the write
/// path — the "no new round-trip" guard (calibrated: ~50 at depth ~5).
#[tokio::test]
async fn write_op_count_ceiling_at_shallow_depth() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    commit_many(&mut db, 5).await;
    let io = measure_insert(&mut db, "ceil").await;
    eprintln!(
        "depth~5: data={} __manifest={} _graph_commits={} total_reads={}",
        io.data_reads, io.manifest_reads, io.commit_graph_reads, io.total_reads()
    );
    const CEILING: u64 = 80;
    assert!(
        io.total_reads() <= CEILING,
        "per-write read ops {} exceeded ceiling {CEILING} — a new round-trip was added",
        io.total_reads()
    );
}

// ── (C) Fitness assert via the staged-write probes ──

/// A keyed `Person` insert routes through `stage_merge_insert` exactly once, does
/// no `stage_append`, and no inline vector-index build. Pins the structural shape.
#[tokio::test]
async fn keyed_insert_routes_through_merge_insert_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    let (res, _io, staged) = measure_with_staged(db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "fit")], &[("$age", 30)]),
    ))
    .await;
    res.unwrap();
    assert_eq!(staged.stage_merge_insert, 1, "keyed Person insert stages one merge-insert");
    assert_eq!(staged.stage_append, 0, "keyed insert must not stage_append");
    assert_eq!(staged.create_vector_index, 0, "no inline vector-index build on a plain insert");
}
