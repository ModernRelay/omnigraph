//! Structural gates for the incremental merge-authority projection cache: a
//! repeated merge must serve its branch authority through the INCREMENTAL
//! projection fold (reading only appended catalog fragments), retains at most
//! one non-bound branch's complete authority, and a
//! delete/recreate of a cached branch must be fenced to a full re-read, never
//! a stale reuse. The explicit fold-vs-full correctness oracle lives with the
//! manifest unit tests; it is not hidden in the measured production path.

#![recursion_limit = "512"]

mod helpers;

use std::future::Future;

use omnigraph::db::{MergeOutcome, Omnigraph};

use helpers::cost::{cost_harness, measure};
use helpers::*;

fn on_big_stack<F>(body: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(body());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Defense-in-depth for the physical cost boundary: selecting a fragment is
/// not selecting rows. Keep the deletion-vector classifier on Lance's public
/// physical-address take primitive, and reject the whole-fragment scanner
/// shape that originally made a post-compaction refresh O(history).
#[test]
fn deleted_head_classifier_uses_physical_addresses_not_fragment_scan() {
    let source = include_str!("../src/db/manifest/state.rs");
    let helper = source
        .split("pub(super) async fn read_object_identities_at_offsets")
        .nth(1)
        .and_then(|tail| tail.split("/// Reduce raw manifest rows").next())
        .expect("read_object_identities_at_offsets source body");
    assert!(
        helper.contains("TakeBuilder::try_new_from_addresses"),
        "deleted manifest heads must be hydrated with Lance's physical-address take"
    );
    for forbidden in [".scan()", "with_fragments", "try_into_stream"] {
        assert!(
            !helper.contains(forbidden),
            "deleted-head classifier regressed to whole-fragment scan primitive {forbidden}"
        );
    }
}

/// Cache invalidation and merge insertion share the branch gate. Purging
/// before that gate lets an already-running merge insert the deleted
/// incarnation after the purge and retain it indefinitely.
#[test]
fn branch_delete_purges_merge_authority_after_acquiring_branch_gate() {
    let source = include_str!("../src/db/omnigraph.rs");
    let body = source
        .split("pub async fn branch_delete_as")
        .nth(1)
        .and_then(|tail| tail.split("pub async fn get_commit").next())
        .expect("branch_delete source body");
    let gate = body
        .find(".acquire_branch")
        .expect("branch_delete must acquire its branch gate");
    let purge = body
        .find("merge_authority_cache.lock()")
        .expect("branch_delete must purge cached merge authority");
    assert!(
        gate < purge,
        "branch deletion must acquire the incarnation gate before purging cached authority"
    );
}

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
#[test]
fn repeated_merge_refreshes_projection_incrementally() {
    on_big_stack(|| async {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            let mut db = init_and_load(&dir).await;
            db.branch_create("feature").await.unwrap();

            diverge(&mut db, 0).await;
            let outcome = db.branch_merge("feature", "main").await.unwrap();
            assert_eq!(outcome, MergeOutcome::Merged);

            // Real history advances between the merges, so the cached authority is
            // provably stale and must REFRESH (not merely probe-hit).
            diverge(&mut db, 1).await;

            let (outcome, io) = measure(db.branch_merge("feature", "main")).await;
            assert_eq!(outcome.unwrap(), MergeOutcome::Merged);
            assert!(
                io.projection_incremental_refreshes >= 1,
                "the repeated merge must refresh at least one cached branch authority \
                 through the incremental projection fold (incremental {}, full {})",
                io.projection_incremental_refreshes,
                io.projection_full_refreshes,
            );
            assert!(
                io.projection_full_refreshes <= 1,
                "the cached non-bound authority must stay incremental; only the publishing \
                 bound coordinator may rebuild once (observed {})",
                io.projection_full_refreshes,
            );
            assert_eq!(
                io.projection_identity_rows, 1,
                "one replaced feature head must hydrate one row by physical address, \
                 never the containing history-sized fragment"
            );
            assert!(
                io.manifest_reads > 0,
                "the ground-truth object-store tracker must observe the measured refresh"
            );
            assert!(
                io.manifest_read_bytes > 0,
                "the ground-truth object-store tracker must measure returned bytes"
            );
            eprintln!(
                "incremental repeated merge: manifest_reads={} manifest_read_bytes={}",
                io.manifest_reads, io.manifest_read_bytes,
            );
            // Keep a fixed ground-truth request ceiling alongside the
            // structural physical-take guard above. The ceiling leaves room
            // for Lance metadata layout changes without allowing a hidden
            // full coordinator reopen to multiply the measured reads.
            assert!(
                io.manifest_reads <= 32,
                "incremental repeated merge used {} manifest object reads; hidden full scans must not ride the measured path",
                io.manifest_reads,
            );
        })
        .await;
    });
}

/// The persistent cache has capacity one. Alternating to another non-bound
/// branch must evict the first, so returning to it performs a real coordinator
/// open rather than retaining O(branches * history) lineage.
#[tokio::test]
async fn merge_authority_cache_retains_only_one_non_bound_branch() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature-a").await.unwrap();
    db.branch_create("feature-b").await.unwrap();

    assert_eq!(
        db.branch_merge("feature-a", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate
    );
    assert_eq!(
        db.branch_merge("feature-b", "main").await.unwrap(),
        MergeOutcome::AlreadyUpToDate
    );

    let (outcome, io) = measure(db.branch_merge("feature-a", "main")).await;
    assert_eq!(outcome.unwrap(), MergeOutcome::AlreadyUpToDate);
    assert!(
        io.internal_open_count >= 1,
        "feature-a must have been evicted when feature-b became the one hot authority"
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
