//! Structural gates for the per-branch authority cache: a repeated merge
//! reuses acknowledged local publication views and refreshes foreign changes
//! from `__manifest` without reading `__history`, every non-bound branch keeps
//! its own authority, and a delete/recreate of a cached branch must be fenced
//! to a full re-read, never a stale reuse. The refresh-vs-reopen correctness
//! oracle lives with the manifest unit tests; it is not hidden in the measured
//! production path. The bound and its eviction order are owned by the cache's
//! in-source tests.

use crate::helpers;

use std::future::Future;

use omnigraph::Session;
use omnigraph::db::{MergeOutcome, Omnigraph};

use crate::helpers::cost::{cost_harness, measure};
use crate::helpers::*;

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

/// Cache invalidation and merge insertion share the branch gate. Purging
/// before that gate lets an already-running merge insert the deleted
/// incarnation after the purge and retain it until its next probe.
#[test]
fn branch_delete_purges_branch_authority_after_acquiring_branch_gate() {
    let source = include_str!("../../src/db/omnigraph.rs");
    let body = source
        .split("pub async fn branch_delete_as")
        .nth(1)
        .and_then(|tail| tail.split("pub async fn get_commit").next())
        .expect("branch_delete source body");
    let gate = body
        .find(".acquire_branch")
        .expect("branch_delete must acquire its branch gate");
    let purge = body
        .find("branch_authorities.take(")
        .expect("branch_delete must purge cached branch authority");
    assert!(
        gate < purge,
        "branch deletion must acquire the incarnation gate before purging cached authority"
    );
}

/// Update both branches through one handle, allowing publication to retain
/// the acknowledged source and target projections.
async fn diverge(db: &Session, round: i64) {
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

/// Acknowledged local publishes retain exact projections; an external publish
/// still requires a refresh of the replaced manifest fragment. The local path
/// performs no projection rebuild; both paths must preserve the merged payload.
#[test]
fn repeated_merge_reuses_local_projection_and_refreshes_foreign() {
    on_big_stack(|| async {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            let db = init_and_load(&dir).await;
            db.branch_create("feature").await.unwrap();

            diverge(&db, 0).await;
            let outcome = db.branch_merge("feature", "main").await.unwrap();
            assert_eq!(outcome.outcome, MergeOutcome::Merged);

            // Both writes use this handle. Publication returns the acknowledged
            // exact projections, so no deleted head row needs reconstructing.
            diverge(&db, 1).await;

            let (outcome, io) = measure(db.branch_merge("feature", "main")).await;
            assert_eq!(outcome.unwrap().outcome, MergeOutcome::Merged);
            assert_eq!(
                io.projection_incremental_refreshes, 0,
                "acknowledged local publication needs only an incarnation probe, not a projection fold",
            );
            eprintln!("local publication reuse: {io:?}");
            assert_eq!(
                io.projection_full_refreshes, 0,
                "acknowledged local publication must retain both exact projections",
            );
            assert_eq!(
                io.projection_identity_rows, 0,
                "acknowledged local heads must not be hydrated again"
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
                "local projection reuse: manifest_reads={} manifest_read_bytes={}",
                io.manifest_reads, io.manifest_read_bytes,
            );
            // Keep a fixed ground-truth request ceiling alongside the
            // structural physical-take guard above. The ceiling leaves room
            // for Lance metadata layout changes without allowing a hidden
            // full coordinator reopen to multiply the measured reads.
            assert!(
                io.manifest_reads <= 32,
                "local projection reuse used {} manifest object reads; hidden full scans must not ride the measured path",
                io.manifest_reads,
            );

            let foreign = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
            foreign.mutate(
                "feature", MUTATION_QUERIES, "set_age",
                &mixed_params(&[("$name", "Alice")], &[("$age", 33)]),
            ).await.unwrap();
            db.mutate(
                "main", MUTATION_QUERIES, "set_age",
                &mixed_params(&[("$name", "Bob")], &[("$age", 28)]),
            ).await.unwrap();
            let (outcome, foreign_io) = measure(db.branch_merge("feature", "main")).await;
            assert_eq!(outcome.unwrap().outcome, MergeOutcome::Merged);
            eprintln!("foreign publication refresh: {foreign_io:?}");
            assert_eq!(
                (foreign_io.projection_full_refreshes, foreign_io.projection_identity_rows),
                (0, 0),
                "a foreign publish one commit ahead is refreshed from `__manifest`: the head it \
                 replaced is the parent of the new head, so `__history` is not read",
            );
            assert!(foreign_io.manifest_reads > 0 && foreign_io.manifest_read_bytes > 0);
            assert!(
                foreign_io.manifest_reads <= 40,
                "foreign-source merge input protection and projection refresh used {} manifest reads",
                foreign_io.manifest_reads,
            );

            // Check every Person payload after the measured refresh, on both
            // source and target, so reduced I/O cannot hide a stale merge.
            for (branch, bob_age) in [("main", 28), ("feature", 25)] {
                assert_eq!(count_rows_branch(&db, branch, "node:Person").await, 4);
                for (name, age) in [("Alice", 33), ("Bob", bob_age), ("Charlie", 35), ("Diana", 28)] {
                    let result = db.query(
                        omnigraph::db::ReadTarget::branch(branch), TEST_QUERIES, "get_person",
                        &params(&[("$name", name)]),
                    ).await.unwrap();
                    assert_eq!(result.num_rows(), 1);
                    let batch = result.concat_batches().unwrap();
                    let actual_age = batch.column(1).as_any()
                        .downcast_ref::<arrow_array::Int32Array>().unwrap().value(0);
                    assert_eq!(actual_age, age, "{branch}/{name}");
                }
            }
        })
        .await;
    });
}

/// A cold merge uses held tails for recent divergence and addressed history
/// blocks for an older base, without enumerating all settled ancestry. Under a
/// 2048-byte `history_release_bytes` main's buffer releases every fourth publish.
#[test]
fn cold_merge_uses_held_tail_before_settled_history() {
    on_big_stack(|| async {
        cost_harness(async {
            for divergence in [1, 6] {
                let dir = tempfile::tempdir().unwrap();
                let db = with_setting(&init_and_load(&dir).await, "history_release_bytes", "2048");
                for age in 40..47 {
                    mutate_main(
                        &db,
                        MUTATION_QUERIES,
                        "set_age",
                        &mixed_params(&[("$name", "Alice")], &[("$age", age)]),
                    )
                    .await
                    .unwrap();
                }
                db.branch_create("feature").await.unwrap();
                for round in 0..divergence {
                    diverge(&db, 100 + round).await;
                }
                drop(db);

                let db =
                    helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
                let (outcome, io) = measure(db.branch_merge("feature", "main")).await;
                assert_eq!(outcome.unwrap().outcome, MergeOutcome::Merged);
                assert_eq!(
                    io.projection_full_refreshes, 0,
                    "divergence {divergence}: cold merge must use held tails or addressed blocks \
                     (the fork is one publish after a release, so one round keeps the base in \
                     the held tail and six rounds release it from both heads at round 2)"
                );
                for (name, age) in [("Alice", 130 + divergence), ("Bob", 125 + divergence)] {
                    let result = db
                        .query(
                            omnigraph::db::ReadTarget::branch("main"),
                            TEST_QUERIES,
                            "get_person",
                            &params(&[("$name", name)]),
                        )
                        .await
                        .unwrap();
                    assert_eq!(result.num_rows(), 1);
                    let batch = result.concat_batches().unwrap();
                    assert_eq!(
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .unwrap()
                            .value(0),
                        i32::try_from(age).unwrap(),
                        "the selected base must preserve both sides' changes for {name}"
                    );
                }
            }
        })
        .await;
    });
}

/// Forking again from an acknowledged merge leaves its merged parent outside
/// the new branch's first-parent tail. That older frontier must not force a
/// history scan when both new heads prove the more recent shared base.
#[test]
fn consecutive_recent_fork_merges_avoid_settled_history() {
    on_big_stack(|| async {
        cost_harness(async {
            let dir = tempfile::tempdir().unwrap();
            let db = init_and_load(&dir).await;
            for age in 40..88 {
                mutate_main(
                    &db,
                    MUTATION_QUERIES,
                    "set_age",
                    &mixed_params(&[("$name", "Alice")], &[("$age", age)]),
                )
                .await
                .unwrap();
            }
            drop(db);
            let db = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());

            for (branch, alice_age, bob_age) in
                [("feature-first", 131, 126), ("feature-second", 132, 127)]
            {
                db.branch_create(branch).await.unwrap();
                mutate_branch(
                    &db,
                    branch,
                    MUTATION_QUERIES,
                    "set_age",
                    &mixed_params(&[("$name", "Alice")], &[("$age", alice_age)]),
                )
                .await
                .unwrap();
                mutate_main(
                    &db,
                    MUTATION_QUERIES,
                    "set_age",
                    &mixed_params(&[("$name", "Bob")], &[("$age", bob_age)]),
                )
                .await
                .unwrap();

                let (outcome, io) = measure(db.branch_merge(branch, "main")).await;
                assert_eq!(outcome.unwrap().outcome, MergeOutcome::Merged);
                assert_eq!(
                    io.projection_full_refreshes, 0,
                    "{branch}: both captured heads share the recent fork base; an older \
                     merged-parent frontier must not cause a settled-lineage scan"
                );
                for (name, age) in [("Alice", alice_age), ("Bob", bob_age)] {
                    let result = db
                        .query(
                            omnigraph::db::ReadTarget::branch("main"),
                            TEST_QUERIES,
                            "get_person",
                            &params(&[("$name", name)]),
                        )
                        .await
                        .unwrap();
                    assert_eq!(result.num_rows(), 1);
                    let batch = result.concat_batches().unwrap();
                    assert_eq!(
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .unwrap()
                            .value(0),
                        i32::try_from(age).unwrap(),
                        "{branch}: merge must preserve both sides' changes for {name}"
                    );
                }
            }
        })
        .await;
    });
}

/// Every non-bound branch keeps its own authority: returning to a branch
/// after work on another one reuses its coordinator behind the incarnation
/// probe instead of opening it again from `__manifest`.
#[tokio::test]
async fn branch_authority_cache_keeps_each_non_bound_branch() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature-a").await.unwrap();
    db.branch_create("feature-b").await.unwrap();

    assert_eq!(
        db.branch_merge("feature-a", "main").await.unwrap().outcome,
        MergeOutcome::AlreadyUpToDate
    );
    assert_eq!(
        db.branch_merge("feature-b", "main").await.unwrap().outcome,
        MergeOutcome::AlreadyUpToDate
    );

    let (outcome, io) = measure(db.branch_merge("feature-a", "main")).await;
    assert_eq!(outcome.unwrap().outcome, MergeOutcome::AlreadyUpToDate);
    assert_eq!(
        (io.internal_open_count, io.manifest_scan_count),
        (0, 0),
        "feature-a must keep its authority while feature-b holds its own: {io:?}"
    );
}

/// The ABA fence: deleting and recreating a cached branch must not reuse the
/// old lifetime's projection. The incarnation probe carries the
/// BranchIdentifier, so the recreated branch takes a full re-open and the
/// merge sees the NEW branch's (empty) divergence — asserted through the
/// merge outcome, which would be wrong under stale reuse. A recreation by
/// another handle leaves this handle's entry in place until its next use,
/// whose probe finds the new lifetime: the write lands there, and nothing the
/// old lifetime held shows through.
#[tokio::test]
async fn branch_recreate_is_fenced_from_the_cached_projection() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    diverge(&db, 0).await;
    assert_eq!(
        db.branch_merge("feature", "main").await.unwrap().outcome,
        MergeOutcome::Merged
    );

    db.branch_delete("feature").await.unwrap();
    db.branch_create("feature").await.unwrap();

    // The recreated branch has no divergence from main: the correct outcome
    // is AlreadyUpToDate. A stale cached projection (the old lifetime's
    // commits) would instead present divergence.
    let outcome = db.branch_merge("feature", "main").await.unwrap();
    assert_eq!(
        outcome.outcome,
        MergeOutcome::AlreadyUpToDate,
        "a recreated branch must be re-read from its new lifetime, never \
         served from the deleted lifetime's cached projection"
    );

    mutate_branch(
        &db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "OldLifetime")], &[("$age", 50)]),
    )
    .await
    .unwrap();
    let other = helpers::session(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());
    other.branch_delete("feature").await.unwrap();
    other.branch_create("feature").await.unwrap();
    mutate_branch(
        &db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "NewLifetime")], &[("$age", 51)]),
    )
    .await
    .unwrap();
    for reader in [&db, &other] {
        for (name, rows) in [("OldLifetime", 0), ("NewLifetime", 1)] {
            let result = reader
                .query(
                    omnigraph::db::ReadTarget::branch("feature"),
                    TEST_QUERIES,
                    "get_person",
                    &params(&[("$name", name)]),
                )
                .await
                .unwrap();
            assert_eq!(
                result.num_rows(),
                rows,
                "{name}: a write after another handle recreated the branch lands on \
                 its new lifetime"
            );
        }
    }
}
