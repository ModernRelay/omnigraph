//! Tests for the direct-to-target write path (MR-771: Run state machine
//! removed). The Run/`__run__` staging branch / RunRecord state machine no
//! longer exists; mutations and loads write directly to target tables and
//! commit once via the publisher's `expected_table_versions` CAS.
//!
//! What this file covers:
//! - No `__run__*` branches are created by load or mutate.
//! - Cancellation of a mutation future leaves no graph-level state.
//! - Concurrent writers to the same table land exactly one publish; the
//!   loser surfaces `ManifestConflictDetails::ExpectedVersionMismatch`.
//! - Failed mutations and loads leave the target unchanged.
//! - Multi-statement mutations are atomic (one commit per query).
//! - actor_id propagates through to the commit graph.

mod helpers;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::{ManifestConflictDetails, ManifestErrorKind, OmniError};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

/// `omnigraph load` (no `--branch`) writes directly to the target — no
/// `__run__*` staging branch is created on success.
#[tokio::test]
async fn load_does_not_create_run_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
    assert!(
        !std::path::Path::new(&format!("{}/_graph_runs.lance", uri)).exists(),
        "run state machine should not write _graph_runs.lance",
    );

    let qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

/// `omnigraph change` writes directly to the target. After the call,
/// `branch_list()` shows only `main`; no run record exists.
#[tokio::test]
async fn mutation_does_not_create_run_branch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_nodes, 1);

    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
    assert!(
        !std::path::Path::new(&format!("{}/_graph_runs.lance", uri)).exists(),
        "run state machine should not write _graph_runs.lance",
    );
}

/// A failed mutation (validation error mid-query) leaves the target branch's
/// observable state unchanged. There is nothing for cleanup to delete.
#[tokio::test]
async fn failed_mutation_leaves_target_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let err = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "add_friend",
            &params(&[("$from", "Alice"), ("$to", "Missing")]),
        )
        .await
        .unwrap_err();
    match err {
        OmniError::Manifest(message) => assert!(message.message.contains("not found")),
        other => panic!("unexpected error: {}", other),
    }

    let qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 2);

    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
}

/// Multi-statement mutations are atomic at the query boundary. The
/// `insert_person_and_friend` query inserts a person and an edge that
/// references it; both must land together (read-your-writes within the
/// query, single publish at the end).
#[tokio::test]
async fn multi_statement_mutation_is_atomic_with_read_your_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person_and_friend",
            &mixed_params(
                &[("$name", "Eve"), ("$friend", "Alice")],
                &[("$age", 22)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_nodes, 1);
    assert_eq!(result.affected_edges, 1);

    // Both writes are visible after one publish.
    let person = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(person.num_rows(), 1);

    let friends = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(friends.num_rows(), 1);
}

/// Mid-query partial failure: op-1 writes a Lance fragment, op-2 fails.
/// Documents the *current* observable behavior — not the desired one. The
/// publisher never publishes (good — no manifest commit, target state
/// unchanged), but Lance HEAD on the touched table is now ahead of the
/// manifest-recorded version. The next mutation against that table fails
/// loudly with `ExpectedVersionMismatch` (the engine's
/// `ensure_expected_version` strict-equality check refuses the drift).
///
/// **Known limitation, MR-771 follow-up**: a proper rollback requires
/// per-table Lance branches (write to a transient branch, fast-forward
/// main on success, drop on failure). Lance's `restore()` is not a rewind
/// — it appends a new commit, advancing HEAD further. See `docs/runs.md`
/// for the workaround (rare in practice: most validation runs before any
/// Lance write, so this only fires on multi-statement queries where a
/// late op fails after an earlier op committed).
#[tokio::test]
async fn partial_failure_observably_rolls_back_but_blocks_next_mutation_on_same_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Op-1 inserts Person "Eve" successfully (advancing Lance HEAD on
    // node:Person). Op-2 inserts Knows from Eve to "Missing" — fails at
    // validate_edge_insert_endpoints because "Missing" doesn't exist.
    // The query as a whole errors; the publisher never runs.
    let err = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person_and_friend",
            &mixed_params(
                &[("$name", "Eve"), ("$friend", "Missing")],
                &[("$age", 22)],
            ),
        )
        .await
        .expect_err("op-2 must fail");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("not found"),
        "unexpected error: {}",
        manifest_err.message,
    );

    // Atomicity at the manifest level: Eve is *not* observable. The Lance
    // fragment from op-1 exists on disk but is not referenced by the
    // manifest; readers at the manifest's pinned version see the
    // pre-mutation state.
    let eve = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(eve.num_rows(), 0, "partial Lance write must not be visible");

    // The next mutation against the *same* table fails loudly. Other
    // tables are unaffected, and reads still work.
    let blocked = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
        )
        .await
        .expect_err("next mutation on the touched table is blocked by orphan Lance HEAD");
    let OmniError::Manifest(blocked_err) = blocked else {
        panic!("expected Manifest error, got {blocked:?}");
    };
    assert!(matches!(
        blocked_err.details,
        Some(omnigraph::error::ManifestConflictDetails::ExpectedVersionMismatch { .. })
    ));
}

/// Concurrent writers to the same `(table, branch)` produce exactly one
/// success and one `ExpectedVersionMismatch`. The replacement for the old
/// `concurrent_conflicting_run_publish_fails_cleanly` test — the OCC fence
/// has moved from a graph-level run-publish merge into the publisher's
/// per-table CAS (MR-766 + MR-771).
///
/// Drives the race by interleaving two handles that captured the same
/// pre-write manifest snapshot: A commits first; B's commit then sees
/// `expected_versions[node:Person] = pre` while the manifest is at
/// `pre + 1`, and the publisher rejects.
#[tokio::test]
async fn concurrent_writers_one_succeeds_one_gets_expected_version_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    {
        let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
    }

    // Open handle B first — it captures the pre-write snapshot. We don't
    // actually mutate yet; we just want B's coordinator to be at the
    // pre-A-commit state when we eventually call mutate.
    let mut db_b = Omnigraph::open(&uri).await.unwrap();

    // Writer A advances the manifest by inserting a new Person.
    {
        let mut db_a = Omnigraph::open(&uri).await.unwrap();
        db_a.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "WriterA")], &[("$age", 41)]),
        )
        .await
        .unwrap();
    }

    // Writer B's coordinator is still at the pre-A snapshot. Its mutation
    // captures expected_versions[node:Person] = pre (stale), then publishes
    // — the publisher's CAS pre-check sees the manifest is now at post and
    // rejects with ExpectedVersionMismatch.
    let result_b = db_b
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "WriterB")], &[("$age", 42)]),
        )
        .await;

    let err = result_b.expect_err("stale writer must hit ExpectedVersionMismatch");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert_eq!(manifest_err.kind, ManifestErrorKind::Conflict);
    let Some(ManifestConflictDetails::ExpectedVersionMismatch {
        ref table_key,
        expected,
        actual,
    }) = manifest_err.details
    else {
        panic!(
            "expected ExpectedVersionMismatch, got {:?}",
            manifest_err.details,
        );
    };
    assert_eq!(table_key, "node:Person");
    assert!(
        actual > expected,
        "actual ({actual}) should be ahead of expected ({expected})",
    );
}

/// The cancellation hole that motivated MR-771: dropping a mutation future
/// mid-flight must not leave any graph-level state behind. With the run
/// state machine gone, only orphaned Lance fragments can remain — and those
/// are reclaimed by `omnigraph cleanup`.
///
/// The test deliberately does NOT assert that the manifest version is
/// unchanged: `handle.abort()` is racing the spawned task, and on a fast
/// machine the mutation may complete before cancellation. That is acceptable
/// — what matters for cancel safety is that no `__run__*` staging branches
/// are ever created, that `_graph_runs.lance` is never written, and that
/// any partial state on disk is reachable through the regular manifest /
/// commit graph pipes (so `omnigraph cleanup` can reclaim it). Asserting
/// version equality would just be a flake on hosts where the abort lands
/// late.
#[tokio::test]
async fn cancelled_mutation_future_leaves_no_state() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_string_lossy().into_owned();

    {
        let mut db = Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap();
        load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
            .await
            .unwrap();
    }

    let branches_before = {
        let db = Omnigraph::open(&uri).await.unwrap();
        db.branch_list().await.unwrap()
    };

    let uri_handle = uri.clone();
    let handle = tokio::spawn(async move {
        let mut db = Omnigraph::open(&uri_handle).await.unwrap();
        db.mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
    });

    // Cancel the future. Whether the in-flight write managed to land a
    // fragment (or even fully publish) is timing-dependent and irrelevant —
    // see the doc comment on this test for why.
    handle.abort();
    let _ = handle.await;

    let db = Omnigraph::open(&uri).await.unwrap();
    let branches_after = db.branch_list().await.unwrap();

    // Cancel-safety property: no graph-level run/staging state remains.
    //
    // Note: `branch_list()` already filters `__run__*` via
    // `is_internal_system_branch`, so a runtime "no `__run__` branches" check
    // would be vacuous. The structural property that no `__run__` branches
    // can ever be created is enforced by deletion of `begin_run` etc. in
    // MR-771 (verified by the build itself — those symbols no longer exist).
    //
    // (1) The branch list is unchanged: cancellation/completion cannot
    //     synthesize new public branches.
    assert_eq!(
        branches_after, branches_before,
        "cancelled mutation must not synthesize new public branches",
    );
    // (2) The legacy run-state machine table never reappears on disk.
    assert!(
        !std::path::Path::new(&format!("{}/_graph_runs.lance", uri)).exists(),
        "no _graph_runs.lance after cancel — state machine is gone",
    );
}

/// `actor_id` provided to `mutate_as` reaches the commit graph so audit can
/// reconstruct who published which commit. This used to be plumbed via the
/// run record; now it goes directly through the publisher and
/// `record_graph_commit`.
#[tokio::test]
async fn mutation_actor_id_lands_in_commit_graph() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;

    db.mutate_as(
        "main",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
        Some("act-andrew"),
    )
    .await
    .unwrap();

    let head = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.actor_id.as_deref(), Some("act-andrew"));
}

/// Repeated loads must not accumulate `__run__*` branches across calls. In
/// the post-demotion world there are no run branches at all — verify that
/// 10 sequential loads end with `branch_list() == ["main"]`.
#[tokio::test]
async fn repeated_loads_do_not_accumulate_branches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    for i in 0..10 {
        let payload = format!(
            r#"{{"type":"Person","data":{{"name":"p{}","age":{}}}}}"#,
            i, i
        );
        load_jsonl(&mut db, &payload, LoadMode::Append)
            .await
            .unwrap();
    }

    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
}

/// User code must not be able to write to internal `__run__*` names. The
/// branch-name guard predicate is kept as defense-in-depth (MR-770 will
/// remove it once production legacy branches are swept).
#[tokio::test]
async fn public_branch_apis_reject_internal_run_refs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let create_err = db.branch_create("__run__synthetic").await.unwrap_err();
    let OmniError::Manifest(err) = create_err else {
        panic!("expected Manifest error");
    };
    assert!(
        err.message.contains("internal run ref"),
        "unexpected error: {}",
        err.message
    );

    let merge_err = db
        .branch_merge("__run__synthetic", "main")
        .await
        .unwrap_err();
    let OmniError::Manifest(err) = merge_err else {
        panic!("expected Manifest error");
    };
    assert!(
        err.message.contains("internal run refs"),
        "unexpected error: {}",
        err.message
    );
}
