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
fn probes() -> (QueryIoProbes, IOTracker, IOTracker, Arc<AtomicU64>) {
    let manifest = IOTracker::default();
    let commit_graph = IOTracker::default();
    let probe_count = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        manifest_wrapper: Some(Arc::new(manifest.clone()) as Arc<dyn WrappingObjectStore>),
        commit_graph_wrapper: Some(Arc::new(commit_graph.clone()) as Arc<dyn WrappingObjectStore>),
        probe_count: Arc::clone(&probe_count),
    };
    (probes, manifest, commit_graph, probe_count)
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

    let (probes_in, _manifest, commit_graph, probe_count) = probes();
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

    // Fix 1's guarantee: the warm read reuses the coordinator instead of
    // re-opening it. A coordinator re-open opens BOTH the commit graph and
    // __manifest, so commit_graph == 0 proves neither resolution scan happened,
    // even at commit-history depth (pre-Fix-1 this is a deep commit-graph scan).
    // Exactly one cheap version probe replaces the re-open.
    //
    // The query's residual `_manifest` read_iops are the per-table namespace
    // describe_table scans (Fix 2 removes those); they are not from resolution.
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
