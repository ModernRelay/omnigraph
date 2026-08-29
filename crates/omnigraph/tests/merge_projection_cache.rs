//! Structural gates for the incremental merge-authority projection cache: a
//! repeated merge must serve its branch authority through the INCREMENTAL
//! projection fold (reading only appended catalog fragments), and a
//! delete/recreate of a cached branch must be fenced to a full re-read, never
//! a stale reuse. Fold correctness itself is oracled everywhere: debug builds
//! verify every incremental fold against a full scan of the same version and
//! fail loudly on divergence, so the whole merge suite exercises it.

#![recursion_limit = "512"]

mod helpers;

use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};

use helpers::*;

/// One handle, two branches — the pattern `merge_truth_table.rs` documents
/// (two handles for one store invite cache-coherency surprises that are out
/// of scope here).
async fn diverge(db: &mut Omnigraph, round: i64) {
    mutate_branch(
        db,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31 + round)]),
    )
    .await
    .unwrap();
    mutate_main(
        db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26 + round)]),
    )
    .await
    .unwrap();
}

/// The headline pin: after a first merge has warmed the authority cache, a
/// later merge (with real intervening publishes on both branches) refreshes
/// the cached branch's projection incrementally — no full O(history) re-read.
#[tokio::test]
async fn repeated_merge_refreshes_projection_incrementally() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    diverge(&mut db, 0).await;
    let outcome = db.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    // Real history advances between the merges, so the cached authority is
    // provably stale and must REFRESH (not merely probe-hit).
    diverge(&mut db, 1).await;

    let probes = QueryIoProbes::default();
    let outcome = with_query_io_probes(probes.clone(), db.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    let incremental = probes
        .projection_incremental_refreshes
        .load(std::sync::atomic::Ordering::Relaxed);
    let full = probes
        .projection_full_refreshes
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        incremental >= 1,
        "the repeated merge must refresh at least one cached branch authority \
         through the incremental projection fold (incremental {incremental}, full {full})"
    );
    // The bound coordinator itself still pays ONE full re-read per merge:
    // its accumulators are cleared by its own publishes (the staleness
    // fence) and the post-merge restore-refresh rebuilds them full. Folding
    // the publish delta into the accumulators instead of clearing is the
    // named follow-up that takes this bound to zero — tightening this
    // assertion is that change's red test.
    assert!(
        full <= 1,
        "the repeated merge paid {full} full projection re-read(s) \
         (incremental {incremental}); only the publishing coordinator's \
         post-publish rebuild may pay one"
    );
}

/// The ABA fence: deleting and recreating a cached branch must not reuse the
/// old lifetime's projection. The incarnation probe carries the
/// BranchIdentifier, so the recreated branch takes a full re-open/refresh and
/// the merge sees the NEW branch's (empty) divergence — asserted through the
/// merge outcome, which would be wrong under stale reuse.
#[tokio::test]
async fn branch_recreate_is_fenced_from_the_cached_projection() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    diverge(&mut db, 0).await;
    assert_eq!(
        db.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::Merged
    );

    db.branch_delete("feature").await.unwrap();
    db.branch_create("feature").await.unwrap();

    // The recreated branch has no divergence from main: the correct outcome
    // is AlreadyUpToDate. A stale cached projection (the old lifetime's
    // commits) would instead present divergence.
    let outcome = db.branch_merge("feature", "main").await.unwrap();
    assert_eq!(
        outcome,
        MergeOutcome::AlreadyUpToDate,
        "a recreated branch must be re-read from its new lifetime, never \
         served from the deleted lifetime's cached projection"
    );
}
