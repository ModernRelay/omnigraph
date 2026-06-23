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
    IoCounts, assert_flat, assert_grows, local_graph, measure, measure_insert, measure_insert_as,
    measure_with_staged,
};
use helpers::{MUTATION_QUERIES, commit_many, commit_many_as, init_and_load, mixed_params};

// ── (A) The internal-table LOCK — the acceptance test for step 2 (compaction) ──
//
// `__manifest` / `_graph_commits` / `_graph_commit_actors` scans on a write must be
// O(1) in commit-history depth **on a compacted graph**. Without internal-table
// compaction these scans are O(fragments) and grow forever; step 2 brings all three
// internal tables into `db.optimize()`, so after compaction the per-write scan is
// flat. The test runs the **authenticated (actorful) write path** — every commit
// carries an actor, so it grows `_graph_commit_actors.lance` too (the production
// server/CLI path); the commit-graph IO wrapper covers both that and `_graph_commits`,
// so `commit_graph_reads` includes the actor-table scan. It compacts at each depth
// checkpoint before measuring — pinning the production invariant "a periodically-
// compacted graph's write cost does not grow with version history."
#[tokio::test]
async fn internal_table_scans_are_flat_in_history() {
    const ACTOR: &str = "act-cost-gate";
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;

    let mut curve: Vec<(u64, IoCounts)> = Vec::new();
    let mut current = 0u64;
    for d in [10u64, 100] {
        if d > current {
            commit_many_as(&mut db, (d - current) as usize, ACTOR).await;
            current = d;
        }
        // Step 2: compaction folds all three internal tables' O(depth) fragments back
        // to a small constant, so the following write's scan of them is flat.
        db.optimize().await.unwrap();
        let io = measure_insert_as(&mut db, &format!("lock_{d}"), ACTOR).await;
        current += 1; // the measured write advanced depth by one
        eprintln!(
            "depth~{d}: data={} __manifest={} _graph_commits+actors={}",
            io.data_reads, io.manifest_reads, io.commit_graph_reads
        );
        curve.push((d, io));
    }

    assert_flat(&curve, |c| c.manifest_reads, 4, "__manifest scan");
    // commit_graph_reads covers BOTH _graph_commits and _graph_commit_actors (shared
    // wrapper), so this also gates the actor table on the authenticated path.
    assert_flat(&curve, |c| c.commit_graph_reads, 4, "_graph_commits + _graph_commit_actors scan");
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

// ── (D) Step-3b capture-once fitness asserts (RED today → GREEN after WriteTxn) ──

/// A write must validate the schema contract EXACTLY ONCE (3 `read_text` + 2 `exists`).
/// Today the write path re-validates at every resolve point (entry, per-table
/// `resolved_branch_target`, commit-time `fresh_snapshot_for_branch`), so the delta is
/// a multiple of that. Step 3b's `WriteTxn` validates once and threads it. The shape is
/// the write twin of `warm_read_cost.rs::warm_query_validates_schema_contract_once`,
/// built with ZERO production change via the counting storage adapter.
#[tokio::test]
async fn write_validates_schema_contract_once() {
    use omnigraph::instrumentation::CountingStorageAdapter;
    use omnigraph::storage::storage_for_uri;

    let dir = tempfile::tempdir().unwrap();
    let _ = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let (adapter, counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
    let db = omnigraph::db::Omnigraph::open_with_storage(uri, adapter)
        .await
        .unwrap();

    let before_read_text = counts.read_text();
    let before_exists = counts.exists();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "schema_once")], &[("$age", 30)]),
    )
    .await
    .unwrap();

    let read_text_delta = counts.read_text() - before_read_text;
    let exists_delta = counts.exists() - before_exists;
    eprintln!("schema-contract reads on one write: read_text={read_text_delta} exists={exists_delta}");
    assert_eq!(
        read_text_delta, 3,
        "a write must validate the schema contract once (3 reads), not N times",
    );
    assert_eq!(
        exists_delta, 2,
        "a write must probe contract-file existence once (2 probes), not N times",
    );
}

/// A keyed single-table write must open its DATA table AT MOST ONCE. Today it opens
/// ~4× (accumulation, staging, commit drift-guard, publish-prepare/index-build), each
/// a fresh cold `Dataset::open`. Step 3b opens the base once (a *session-aware* base
/// open is deferred to step 5), threads the commit-return handle, and replaces the
/// drift-guard open with a cheap `latest_version_id` probe — collapsing to 1 open.
/// Counted by `data_open_count`, the
/// table-class-scoped chokepoint probe: the internal-table opens (publisher CAS +
/// commit-graph append) are EXCLUDED, since they are unrelated to data-table reuse and
/// would otherwise keep this count >1 regardless of threading. (`forbidden_apis` keeps
/// engine code outside the storage layer from opening datasets except through the
/// instrumented chokepoints — `table_store.rs`'s own direct opens are branch-management
/// ops, not this keyed-write path.)
#[tokio::test]
async fn keyed_insert_opens_table_at_most_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = local_graph(&dir).await;
    let io = {
        let (res, io) = measure(db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "opens")], &[("$age", 30)]),
        ))
        .await;
        res.unwrap();
        io
    };
    eprintln!(
        "data_open_count={} internal_open_count={} for a single-table keyed insert",
        io.data_open_count, io.internal_open_count
    );
    assert!(
        io.data_open_count <= 1,
        "a keyed single-table write must open its data table at most once, got {}",
        io.data_open_count,
    );
}
