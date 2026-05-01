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

use arrow_array::Array;
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

/// Mid-query partial failure: op-1 stages a Person insert, op-2 fails on
/// referential integrity (validate_edge_insert_endpoints). Under the
/// MR-794 staged-write rewire, op-1's batch lives in the in-memory
/// accumulator and never reaches Lance — Lance HEAD on `node:Person`
/// stays at the pre-mutation version. The publisher never publishes,
/// the manifest never advances, and the next mutation against the same
/// table proceeds normally (no `ExpectedVersionMismatch`).
///
/// This test pins the post-MR-794 contract:
/// - Failed multi-statement mutation surfaces a clear error, no manifest
///   commit, no observable state change.
/// - The touched tables stay queryable and writable from the next
///   query — Lance HEAD has not drifted.
#[tokio::test]
async fn partial_failure_leaves_target_queryable_and_unblocks_next_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Op-1 stages a Person 'Eve' insert. Op-2 attempts an edge to
    // 'Missing' — fails at validate_edge_insert_endpoints because
    // 'Missing' doesn't exist (and isn't pending).
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

    // Atomicity at the manifest level: Eve is *not* observable. The
    // staged batch never reached Lance, so neither the Lance HEAD nor
    // the manifest moved.
    let eve = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(eve.num_rows(), 0, "partial mutation must not be visible");

    // The next mutation against the same table SUCCEEDS — staged writes
    // never advance Lance HEAD on a failed query, so there is no drift
    // to trip the publisher's CAS.
    let result = db
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
        )
        .await
        .expect("next mutation on the touched table must succeed under MR-794");
    assert_eq!(
        result.affected_nodes, 1,
        "follow-up insert should report 1 affected node"
    );

    // And Frank is observable.
    let frank = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Frank")]),
        )
        .await
        .unwrap();
    assert_eq!(frank.num_rows(), 1, "Frank must be visible after publish");
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

// ─── MR-794: staged-write rewire — additional contract tests ────────────────

/// Mutation queries used only by the MR-794 tests below. Kept in the test
/// file (not in helpers' shared `MUTATION_QUERIES`) to keep their scope
/// local to the staged-write coverage.
const STAGED_QUERIES: &str = r#"
query insert_two_persons($a_name: String, $a_age: I32, $b_name: String, $b_age: I32) {
    insert Person { name: $a_name, age: $a_age }
    insert Person { name: $b_name, age: $b_age }
}

query insert_then_update_same_person(
    $name: String, $insert_age: I32, $update_age: I32
) {
    insert Person { name: $name, age: $insert_age }
    update Person set { age: $update_age } where name = $name
}

query insert_two_friends($from: String, $a: String, $b: String) {
    insert Knows { from: $from, to: $a }
    insert Knows { from: $from, to: $b }
}

query mixed_insert_and_delete($name: String, $age: I32, $victim: String) {
    insert Person { name: $name, age: $age }
    delete Person where name = $victim
}
"#;

/// D₂: a query mixing inserts/updates with deletes is rejected at parse
/// time, BEFORE any I/O. The error shape directs the user to split the
/// query into two mutations.
#[tokio::test]
async fn mutation_rejects_mixed_insert_and_delete_at_parse_time() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Capture pre-mutation state on touched tables to confirm no I/O.
    let persons_before = count_rows(&db, "node:Person").await;

    let err = db
        .mutate(
            "main",
            STAGED_QUERIES,
            "mixed_insert_and_delete",
            &mixed_params(
                &[("$name", "Eve"), ("$victim", "Alice")],
                &[("$age", 22)],
            ),
        )
        .await
        .expect_err("D₂ must reject mixed insert+delete");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("inserts/updates and deletes"),
        "unexpected error message: {}",
        manifest_err.message,
    );
    assert!(
        manifest_err.message.contains("split into separate mutations"),
        "error message should direct user to split: {}",
        manifest_err.message,
    );

    // No I/O — counts unchanged, branches unchanged.
    let persons_after = count_rows(&db, "node:Person").await;
    assert_eq!(
        persons_before, persons_after,
        "D₂ rejection must fire before any write",
    );
    assert_eq!(db.branch_list().await.unwrap(), vec!["main".to_string()]);
}

/// `insert Person 'X'; update Person where name='X' set age=...` — both
/// ops produce content on `node:Person` and coalesce into one
/// `stage_merge_insert` at end-of-query. The accumulator's last-write-wins
/// dedupe (in `MutationStaging::finalize`) ensures the update's value
/// wins. Single Lance commit per table per query.
#[tokio::test]
async fn mixed_insert_and_update_on_same_person_coalesces_to_one_merge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let pre_version = version_main(&db).await.unwrap();

    let result = db
        .mutate(
            "main",
            STAGED_QUERIES,
            "insert_then_update_same_person",
            &mixed_params(
                &[("$name", "Yves")],
                &[("$insert_age", 10), ("$update_age", 99)],
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_nodes, 2, "1 insert + 1 update reported");

    // The end-state row carries the update value (last-write-wins via
    // dedupe in finalize), proving the staged merge_insert ran with the
    // correct source dedupe. Read the underlying Person table directly
    // and assert age=99 for the row we just inserted+updated.
    let batches = read_table(&db, "node:Person").await;
    let mut found_age: Option<i32> = None;
    for batch in &batches {
        let names = batch
            .column_by_name("name")
            .expect("Person table missing 'name' column")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("'name' should be Utf8");
        let ages = batch
            .column_by_name("age")
            .expect("Person table missing 'age' column")
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .expect("'age' should be I32");
        for i in 0..batch.num_rows() {
            if names.is_valid(i) && names.value(i) == "Yves" {
                if ages.is_valid(i) {
                    found_age = Some(ages.value(i));
                }
            }
        }
    }
    assert_eq!(
        found_age,
        Some(99),
        "dedupe must keep the update's age value, not the insert's",
    );

    // One-publish guarantee: manifest version advanced by exactly 1.
    let post_version = version_main(&db).await.unwrap();
    assert_eq!(
        post_version,
        pre_version + 1,
        "insert+update query must publish exactly once",
    );
}

/// `insert Knows from='Alice' to='Bob'; insert Knows from='Alice' to='Eve'`
/// — both append to `edge:Knows`. The accumulator coalesces them into one
/// `stage_append` at end-of-query. Edge IDs are ULID-generated so no
/// dedupe is needed (Append mode).
#[tokio::test]
async fn multiple_appends_to_same_edge_coalesce_to_one_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Add Eve so the second edge has a valid endpoint.
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let edges_before = count_rows(&db, "edge:Knows").await;
    let pre_version = version_main(&db).await.unwrap();

    let result = db
        .mutate(
            "main",
            STAGED_QUERIES,
            "insert_two_friends",
            &params(&[
                ("$from", "Alice"),
                ("$a", "Bob"),
                ("$b", "Eve"),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_edges, 2);

    // Both edges visible.
    let edges_after = count_rows(&db, "edge:Knows").await;
    assert_eq!(edges_after, edges_before + 2);

    // One manifest version bump for the two-edge query (atomic publish).
    let post_version = version_main(&db).await.unwrap();
    assert_eq!(
        post_version,
        pre_version + 1,
        "two-statement edge insert must publish exactly once",
    );
}

/// A multi-statement insert query touching two Person rows produces a
/// single `stage_*` + `commit_staged` per table — verified by checking
/// that the manifest version advances exactly once across the query.
#[tokio::test]
async fn multi_statement_inserts_publish_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let pre_version = version_main(&db).await.unwrap();

    db.mutate(
        "main",
        STAGED_QUERIES,
        "insert_two_persons",
        &mixed_params(
            &[("$a_name", "Owen"), ("$b_name", "Pat")],
            &[("$a_age", 50), ("$b_age", 51)],
        ),
    )
    .await
    .unwrap();

    let post_version = version_main(&db).await.unwrap();
    assert_eq!(
        post_version,
        pre_version + 1,
        "two-statement insert query must publish exactly once",
    );

    // Both rows visible.
    let owen = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Owen")]),
        )
        .await
        .unwrap();
    assert_eq!(owen.num_rows(), 1);
    let pat = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Pat")]),
        )
        .await
        .unwrap();
    assert_eq!(pat.num_rows(), 1);
}

/// A load with a mid-input edge RI violation must leave Lance HEAD on
/// the touched node tables untouched (staged loader never commits any
/// fragment when the load fails). The next load on the same tables
/// succeeds — no `ExpectedVersionMismatch` from drift.
#[tokio::test]
async fn load_with_bad_edge_reference_unblocks_next_load() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    // Seed with the standard fixture so we're working from a non-empty
    // baseline.
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let pre_persons = count_rows(&db, "node:Person").await;
    let pre_edges = count_rows(&db, "edge:Knows").await;

    // First load: append a Person + an edge whose `to` points to a
    // non-existent Person. RI fails AFTER the staged Person is in the
    // accumulator but BEFORE the publish.
    let bad = r#"{"type": "Person", "data": {"name": "Mallory", "age": 5}}
{"edge": "Knows", "from": "Mallory", "to": "Ghost"}
"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Append)
        .await
        .expect_err("RI violation must fail the load");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("not found"),
        "unexpected error: {}",
        manifest_err.message,
    );

    // No write made it to disk: counts unchanged.
    let mid_persons = count_rows(&db, "node:Person").await;
    let mid_edges = count_rows(&db, "edge:Knows").await;
    assert_eq!(mid_persons, pre_persons, "failed load must not advance Person count");
    assert_eq!(mid_edges, pre_edges, "failed load must not advance Knows count");

    // Second load against the same tables — succeeds (no HEAD drift).
    let good = r#"{"type": "Person", "data": {"name": "Pat", "age": 55}}"#;
    load_jsonl(&mut db, good, LoadMode::Append).await.unwrap();
    assert_eq!(
        count_rows(&db, "node:Person").await,
        pre_persons + 1,
        "follow-up load must succeed (no drift)",
    );
}

/// Same shape as the RI test above, but driven by a cardinality
/// violation (`@card(0..1)` on `WorksAt`). The staged loader's pending
/// edge accumulator drives the cardinality scan; a violation aborts
/// the load before publish; the next load on the same tables succeeds.
#[tokio::test]
async fn load_with_cardinality_violation_unblocks_next_load() {
    // Use a custom schema where WorksAt has a strict 0..1 cardinality
    // bound — the default test schema leaves WorksAt unbounded. Seed
    // Alice + two companies, then attempt two WorksAt edges from Alice,
    // which violates the bound.
    const CARD_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge WorksAt: Person -> Company @card(0..1)
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CARD_SCHEMA).await.unwrap();

    let seed = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Company", "data": {"name": "Acme"}}
{"type": "Company", "data": {"name": "Bigco"}}
"#;
    load_jsonl(&mut db, seed, LoadMode::Overwrite).await.unwrap();

    let pre_works = count_rows(&db, "edge:WorksAt").await;

    // Two WorksAt edges from Alice — exceeds @card(0..1).
    let bad = r#"{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
{"edge": "WorksAt", "from": "Alice", "to": "Bigco"}
"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Append)
        .await
        .expect_err("cardinality violation must fail the load");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("@card violation"),
        "unexpected error: {}",
        manifest_err.message,
    );

    // No edges added; next load on the same edge table succeeds.
    let mid_works = count_rows(&db, "edge:WorksAt").await;
    assert_eq!(mid_works, pre_works);

    let good = r#"{"edge": "WorksAt", "from": "Alice", "to": "Acme"}"#;
    load_jsonl(&mut db, good, LoadMode::Append).await.unwrap();
    assert_eq!(
        count_rows(&db, "edge:WorksAt").await,
        pre_works + 1,
        "follow-up load must succeed (no drift on edge table)",
    );
}
