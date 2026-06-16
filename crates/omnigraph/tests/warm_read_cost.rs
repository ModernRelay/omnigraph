//! Cost-budget tests for the warm read path (Fix 1): a warm same-branch read
//! must perform no manifest or commit-graph opens, measured with Lance's
//! `IOTracker` at the object-store boundary (the LanceDB IO-counted-test
//! pattern; see docs/dev/testing.md). Guards invariant 15 (read cost bounded by
//! work, not history) for snapshot resolution, and invariant 6 (a warm reader
//! still observes external commits).

mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lance::io::WrappingObjectStore;
use lance_io::utils::tracking_store::IOTracker;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};

use helpers::{
    MUTATION_QUERIES, TEST_QUERIES, commit_many, count_rows, init_and_load, mixed_params,
    mutate_main, params,
};

/// IO probes plus the tracker handles to read `read_iops` after the query.
/// Returns `(probes, manifest, commit_graph, table, probe_count)` — `table`
/// counts per-table data opens (the cache-miss path), so a cost test can assert
/// N opens on a cold read and 0 on a warm repeat (Fix 3).
fn probes() -> (QueryIoProbes, IOTracker, IOTracker, IOTracker, Arc<AtomicU64>) {
    let manifest = IOTracker::default();
    let commit_graph = IOTracker::default();
    let table = IOTracker::default();
    let probe_count = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(manifest.clone()) as Arc<dyn WrappingObjectStore>),
        commit_graph_wrapper: Some(Arc::new(commit_graph.clone()) as Arc<dyn WrappingObjectStore>),
        table_wrapper: Some(Arc::new(table.clone()) as Arc<dyn WrappingObjectStore>),
        probe_count: Arc::clone(&probe_count),
    };
    (probes, manifest, commit_graph, table, probe_count)
}

/// A warm same-branch read must not re-open or scan `__manifest`, and must not
/// open the commit graph, even at commit-history depth. The only manifest IO is
/// the version probe (counted by invocation). Fails before Fix 1, where the read
/// path re-opens a fresh coordinator and scans both internal tables.
#[tokio::test]
async fn warm_same_branch_read_does_no_resolution_opens() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Deep history: warm-read resolution cost must be flat in commit count.
    commit_many(&mut db, 20).await;

    let (probes_in, manifest, commit_graph, _table, probe_count) = probes();
    with_query_io_probes(
        probes_in,
        db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ),
    )
    .await
    .unwrap();

    // A warm same-branch read opens nothing from the internal tables, even at
    // commit-history depth. Fix 1 reuses the coordinator (no re-open: 0
    // commit-graph opens, exactly 1 cheap version probe). Fix 2 opens the touched
    // data table by location+version instead of via the namespace, so the
    // per-table __manifest scan is gone too. Pre-fix, each of these is a deep scan
    // of an internal table that grows with commit count.
    assert_eq!(
        manifest.stats().read_iops,
        0,
        "warm same-branch read must not scan __manifest (resolution or per-table)"
    );
    assert_eq!(
        commit_graph.stats().read_iops,
        0,
        "warm same-branch read must not open the commit graph (no coordinator re-open)"
    );
    assert_eq!(
        probe_count.load(Ordering::Relaxed),
        1,
        "warm same-branch read performs exactly one version probe"
    );
}

/// A multi-table query (a traversal touching Person, WorksAt, and Company) scans
/// `__manifest` zero times. Fix 2 opens every touched table by location+version,
/// so manifest IO no longer scales with the number of tables — pre-Fix-2 each
/// table cost two full `__manifest` scans (`describe_table` +
/// `describe_table_version`), which is the "2 tables = 2×" multi-table tax.
#[tokio::test]
async fn multi_table_query_does_no_manifest_scans() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let (probes_in, manifest, _commit_graph, _table, _probe) = probes();
    with_query_io_probes(
        probes_in,
        db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "age_stats",
            &params(&[]),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        manifest.stats().read_iops,
        0,
        "a multi-table read must not scan __manifest once per touched table"
    );
}

/// A warm reader must observe a commit made through another handle (invariant 6,
/// strong consistency): the version probe detects the advance and refreshes.
/// Passes before and after Fix 1 (today's cold re-read is always fresh); a
/// regression guard so the warm-reuse fast path never serves a stale read.
#[tokio::test]
async fn external_commit_observed_by_warm_reader() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();

    let before = count_rows(&reader, "node:Person").await;

    // External commit through a separate handle.
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "ext_new_person")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    let after = count_rows(&reader, "node:Person").await;
    assert_eq!(
        after,
        before + 1,
        "warm reader must observe an external commit"
    );
}

// ── Finding A: drop the redundant per-query schema validation ─────────────────
//
// Every query runs `ensure_schema_state_valid`. It ran TWICE per query (once in
// query()/run_query_at, once again in resolved_target/snapshot_at_version), each
// reading 3 contract files + 2 existence probes (~10 storage ops). Finding A
// removes the redundant caller, so validation runs once. (A cheaper source-only
// probe was rejected: the codebase requires per-call detection of IR/state drift
// on long-lived handles -- lifecycle::long_lived_handle_rejects_schema_ir_drift
// -- which a source-only compare would miss.) Measured at the StorageAdapter
// boundary with the counting decorator.

/// A warm query validates the schema contract exactly once (3 reads + 2 exists),
/// not twice. Fails before finding A, where query() and resolved_target each
/// validate (6 read_text + 4 exists).
#[tokio::test]
async fn warm_query_validates_schema_contract_once() {
    use omnigraph::instrumentation::CountingStorageAdapter;
    use omnigraph::storage::storage_for_uri;

    let dir = tempfile::tempdir().unwrap();
    // Init through the standard path, then re-open behind a counting adapter to
    // measure the per-query schema-contract storage reads (delta around the
    // query excludes open-time reads).
    let _ = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let (adapter, counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
    let db = Omnigraph::open_with_storage(uri, adapter).await.unwrap();

    let before_read_text = counts.read_text();
    let before_exists = counts.exists();
    db.query(
        ReadTarget::branch("main"),
        TEST_QUERIES,
        "total_people",
        &params(&[]),
    )
    .await
    .unwrap();

    assert_eq!(
        counts.read_text() - before_read_text,
        3,
        "warm query should validate the schema contract once (3 reads), not twice"
    );
    assert_eq!(
        counts.exists() - before_exists,
        2,
        "warm query should probe contract-file existence once (2 probes), not twice"
    );
}

/// The cheap source-compare must still detect that the on-disk schema source has
/// drifted from the validated contract and fail the read, rather than serving the
/// stale-but-cached schema. Passes before and after finding A (regression guard
/// for the documented weaker per-query guard).
#[tokio::test]
async fn schema_source_drift_is_caught_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let _writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();

    // Drift the on-disk schema source behind the reader's back.
    std::fs::write(dir.path().join("_schema.pg"), "this is not a valid schema {{{").unwrap();

    let result = reader
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        )
        .await;
    assert!(
        result.is_err(),
        "a query must fail when the on-disk schema source has drifted from the validated contract"
    );
}

// ── Morphological-matrix coverage: branch-warm + stale-refresh cells ──────────

/// A WARM read on a non-main branch (handle synced to that branch) also scans
/// `__manifest` zero times. Exercises Fix 2's branch-owned-table open
/// (`{table_path}/tree/{branch}` + with_version) on Fix 1's warm path — the cell
/// that regressed when the open used `with_branch` against the base.
#[tokio::test]
async fn warm_branch_read_does_no_manifest_scans() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    // Write to the branch so its tables are branch-owned (under tree/feature).
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    // Bind the handle's coordinator to the branch so reads of it take the warm path.
    db.sync_branch("feature").await.unwrap();

    let (probes_in, manifest, commit_graph, _table, probe_count) = probes();
    with_query_io_probes(
        probes_in,
        db.query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        manifest.stats().read_iops,
        0,
        "warm branch read must not scan __manifest (branch-owned table opened by location)"
    );
    assert_eq!(
        commit_graph.stats().read_iops,
        0,
        "warm branch read must not open the commit graph"
    );
    assert_eq!(
        probe_count.load(Ordering::Relaxed),
        1,
        "warm branch read performs exactly one version probe"
    );
}

/// When an external writer advances the manifest, the reader's next query takes
/// the STALE path: it re-reads the manifest (read_iops > 0) but never scans the
/// commit graph (`refresh_manifest_only`), unlike a full coordinator refresh.
/// Pins Fix 1's manifest-only refresh.
#[tokio::test]
async fn stale_read_refreshes_manifest_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = init_and_load(&dir).await;
    let uri = dir.path().to_str().unwrap();
    let reader = Omnigraph::open(uri).await.unwrap();
    // Establish the reader's warm coordinator.
    reader
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        )
        .await
        .unwrap();

    // External commit advances the on-disk manifest behind the reader.
    mutate_main(
        &mut writer,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .unwrap();

    let (probes_in, manifest, commit_graph, _table, _probe) = probes();
    with_query_io_probes(
        probes_in,
        reader.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ),
    )
    .await
    .unwrap();

    assert!(
        manifest.stats().read_iops > 0,
        "stale read must re-read the manifest"
    );
    assert_eq!(
        commit_graph.stats().read_iops,
        0,
        "stale refresh must be manifest-only (no commit-graph scan)"
    );
}

// ── Fix 3: held-handle cache — warm repeat reads stop re-opening tables ────────
//
// After Fix 1+2 a warm same-branch read still re-opened every touched table per
// query (the "never warms up" residual). Fix 3 holds the open `Dataset` per
// `(table, branch, version)` (the version-keyed analogue of LanceDB's
// `DatasetConsistencyWrapper`) and shares one `Session` per graph, so a second
// identical warm read reuses the handle with zero table opens.

/// Headline: a second identical warm same-branch read does ZERO table opens
/// (the cold first read opens; the warm repeat serves from the held-handle
/// cache). Fails before Fix 3, where every read re-opens the table.
#[tokio::test]
async fn repeat_warm_read_reuses_table_handles() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    // Deep history: the win must hold regardless of commit count.
    commit_many(&mut db, 10).await;

    // Cold first read: opens the touched table.
    let (p1, _m1, _c1, table1, _pr1) = probes();
    with_query_io_probes(
        p1,
        db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ),
    )
    .await
    .unwrap();
    assert!(
        table1.stats().read_iops > 0,
        "the cold first read must open the table"
    );

    // Warm repeat: the held handle is reused, so no open happens through this
    // query's table wrapper.
    let (p2, manifest2, commit_graph2, table2, probe2) = probes();
    with_query_io_probes(
        p2,
        db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        table2.stats().read_iops,
        0,
        "a warm repeat read must reuse the held handle (0 table opens)"
    );
    assert_eq!(
        manifest2.stats().read_iops,
        0,
        "warm repeat read: 0 manifest opens"
    );
    assert_eq!(
        commit_graph2.stats().read_iops,
        0,
        "warm repeat read: 0 commit-graph opens"
    );
    assert_eq!(
        probe2.load(Ordering::Relaxed),
        1,
        "warm repeat read: exactly one version probe"
    );
}

/// A write advances the table's version, so the next read misses the
/// version-keyed cache and re-opens — never serving a stale handle (invariant 6
/// for the cached path). Passes with or without the cache; a correctness guard
/// that the cache cannot serve pre-write data.
#[tokio::test]
async fn write_invalidates_table_cache_for_changed_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let before = count_rows(&db, "node:Person").await;

    // Warm the cache for Person.
    db.query(
        ReadTarget::branch("main"),
        TEST_QUERIES,
        "total_people",
        &params(&[]),
    )
    .await
    .unwrap();

    // Write Person: its version advances, so the cached (table, branch, version)
    // key is now superseded.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "cache_miss_one")], &[("$age", 50)]),
    )
    .await
    .unwrap();

    // The next read re-opens Person at the new version (cache miss).
    let (p, _m, _c, table, _pr) = probes();
    with_query_io_probes(
        p,
        db.query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "total_people",
            &params(&[]),
        ),
    )
    .await
    .unwrap();
    assert!(
        table.stats().read_iops > 0,
        "a read after a write to the table must re-open it (version-keyed miss)"
    );

    let after = count_rows(&db, "node:Person").await;
    assert_eq!(
        after,
        before + 1,
        "the post-write read observes the new row (no stale handle served)"
    );
}
