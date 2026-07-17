//! Tests for the direct-publish write path. Mutations and loads capture one
//! branch-wide authority token, prepare exact per-table transactions, then
//! acquire the root-shared schema → branch → sorted-table gates, revalidate,
//! arm identity-bearing recovery-v9, commit the table effects, and publish once
//! under the same exact-head/table precondition. (History: this replaced the
//! removed Run state machine / `__run__` staging branches / RunRecord — MR-771.)
//!
//! What this file covers:
//! - No `__run__*` branches are created by load or mutate.
//! - Cancellation of a mutation future leaves no graph-level state.
//! - A pre-effect branch-authority change makes an upsert mutation/load discard
//!   and fully reprepare with a bounded retry; strict insert conflicts return
//!   `KeyConflict`, while Update/Delete/Overwrite authority movement surfaces
//!   `ReadSetChanged`. Post-effect failures require recovery.
//! - Failed mutations and loads leave the target unchanged.
//! - Multi-statement mutations are atomic (one commit per query).
//! - actor_id propagates through to the commit graph.

mod helpers;

use arrow_array::Array;
use lance::Dataset;
use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
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
            &mixed_params(&[("$name", "Eve"), ("$friend", "Alice")], &[("$age", 22)]),
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

/// Mid-query partial failure: op-1 stages a Person insert, op-2 fails
/// on referential integrity (validate_edge_insert_endpoints). Under
/// the staged-write writer, op-1's batch lives in the in-memory
/// accumulator and never reaches Lance — Lance HEAD on `node:Person`
/// stays at the pre-mutation version. The publisher never publishes,
/// the manifest never advances, and the next mutation against the same
/// table proceeds normally (no `ExpectedVersionMismatch`).
///
/// Pins the staged-write contract:
/// - Failed multi-statement mutation surfaces a clear error, no
///   manifest commit, no observable state change.
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
            &mixed_params(&[("$name", "Eve"), ("$friend", "Missing")], &[("$age", 22)]),
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
        .expect("next mutation on the touched table must succeed under the staged-write writer");
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

/// Stale non-strict writers discard and reprepare their whole logical attempt
/// from the live branch authority instead of rebasing an already-validated
/// staged transaction or returning a false 409. Strict update/delete semantics
/// are covered by the consistency/server tests.
#[tokio::test]
async fn stale_non_strict_insert_reprepares_from_live_branch_state() {
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

    // Writer B's coordinator is still at the pre-A snapshot, but Insert is
    // retryable: the RFC-022 adapter notices the authority change under the
    // branch gate, discards B's prepared/staged attempt, and reruns the full
    // operation from A's committed state. Lance never rebases a plan whose
    // validation inputs are stale.
    db_b.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "WriterB")], &[("$age", 42)]),
    )
    .await
    .unwrap();

    for name in ["WriterA", "WriterB"] {
        let person = query_main(
            &mut db_b,
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", name)]),
        )
        .await
        .unwrap();
        assert_eq!(person.num_rows(), 1, "{name} should be visible");
    }
}

/// The cancellation hole that motivated removing the Run state machine: dropping a mutation future
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
    // No `__run__` branches can ever be created: the Run state machine
    // (`begin_run` etc.) was deleted in MR-771 — verified by the build itself,
    // those symbols no longer exist. Any legacy `__run__*` branch on an
    // upgraded graph is swept by the v2→v3 manifest migration.
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

/// After MR-770, `__run__*` is an ordinary branch name — the Run state machine
/// and its `is_internal_run_branch` guard are gone. The surviving internal-ref
/// guard still rejects the active `__schema_apply_lock__` branch on the public
/// create/merge APIs.
#[tokio::test]
async fn public_branch_apis_reject_internal_system_refs() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // `__run__*` is no longer reserved — creating it now succeeds.
    db.branch_create("__run__formerly_reserved")
        .await
        .expect("__run__ prefix is a normal branch name post-MR-770");

    // The schema-apply lock branch is still rejected on public branch APIs.
    let create_err = db.branch_create("__schema_apply_lock__").await.unwrap_err();
    let OmniError::Manifest(err) = create_err else {
        panic!("expected Manifest error");
    };
    assert!(
        err.message.contains("internal system ref"),
        "unexpected error: {}",
        err.message
    );

    let merge_err = db
        .branch_merge("__schema_apply_lock__", "main")
        .await
        .unwrap_err();
    let OmniError::Manifest(err) = merge_err else {
        panic!("expected Manifest error");
    };
    assert!(
        err.message.contains("internal system refs"),
        "unexpected error: {}",
        err.message
    );
}

// ─── Staged-write rewire — additional contract tests ───────────────────────

/// Mutation queries used only by the staged-write tests below. Kept in
/// the test file (not in helpers' shared `MUTATION_QUERIES`) to keep
/// their scope local to the staged-write coverage.
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

query update_then_filter_by_old_value(
    $first_name: String, $first_new_age: I32,
    $second_threshold: I32, $second_new_age: I32
) {
    update Person set { age: $first_new_age } where name = $first_name
    update Person set { age: $second_new_age } where age > $second_threshold
}

query delete_two_persons($first: String, $second: String) {
    delete Person where name = $first
    delete Person where name = $second
}

query delete_overlapping_persons($name: String, $threshold: I32) {
    delete Person where name = $name
    delete Person where age > $threshold
}

query update_age_by_name($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
"#;

fn bulk_insert_mutation(rows: usize) -> String {
    let mut query = String::with_capacity(rows * 48);
    query.push_str("query bulk_insert() {\n");
    for row in 0..rows {
        query.push_str(&format!("    insert Thing {{ key: \"mutation-{row}\" }}\n"));
    }
    query.push_str("}\n");
    query
}

fn bulk_update_fixture(rows: usize) -> String {
    let mut data = String::with_capacity(rows * 64);
    for row in 0..rows {
        data.push_str(&format!(
            "{{\"type\":\"Thing\",\"data\":{{\"key\":\"update-{row}\"}}}}\n"
        ));
    }
    data
}

/// Mutation staging shares the same inclusive 8,192-row keyed-write cap as
/// bulk load. The rejected one-over query must remain entirely pre-effect: no
/// recovery sidecar, table version, manifest version, or row may move.
#[tokio::test]
async fn mutation_keyed_write_row_cap_accepts_limit_and_rejects_one_over_pre_effect() {
    const LIMIT: usize = 8192;
    const SCHEMA: &str = "node Thing { key: String @key }\n";

    let exact_dir = tempfile::tempdir().unwrap();
    let exact = Omnigraph::init(exact_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let exact_result = exact
        .mutate(
            "main",
            &bulk_insert_mutation(LIMIT),
            "bulk_insert",
            &params(&[]),
        )
        .await
        .expect("the exact mutation keyed-row limit is inclusive");
    assert_eq!(exact_result.affected_nodes, LIMIT);
    assert_eq!(count_rows(&exact, "node:Thing").await, LIMIT);

    let over_dir = tempfile::tempdir().unwrap();
    let over = Omnigraph::init(over_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let before = snapshot_main(&over).await.unwrap();
    let before_manifest = before.version();
    let entry = before.entry("node:Thing").unwrap();
    let before_table = entry.table_version;
    let table_uri = format!(
        "{}/{}",
        over.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;
    let error = over
        .mutate(
            "main",
            &bulk_insert_mutation(LIMIT + 1),
            "bulk_insert",
            &params(&[]),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 8192,
                actual: 8193,
            } if resource == "keyed rows for node:Thing"
        ),
        "one-over mutation must return the typed keyed-row limit, got {error:?}"
    );
    let after = snapshot_main(&over).await.unwrap();
    assert_eq!(after.version(), before_manifest);
    assert_eq!(
        after.entry("node:Thing").unwrap().table_version,
        before_table
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head,
        "one-over mutation must fail before a Lance table effect"
    );
    assert_eq!(count_rows(&over, "node:Thing").await, 0);
    let recovery_dir = over_dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "one-over mutation must fail before writing a recovery sidecar"
    );
}

/// Update predicate matching is itself a bounded allocation. The committed
/// side must stream and charge rows before it is retained/concatenated, rather
/// than first collecting an arbitrarily wide match set and relying on the
/// later staging guard.
#[tokio::test]
async fn mutation_update_row_cap_accepts_limit_and_rejects_one_over_pre_effect() {
    const LIMIT: usize = 8192;
    const SCHEMA: &str = "node Thing { key: String @key marked: Bool? }\n";
    const UPDATE_ALL: &str = r#"
query update_all() {
    update Thing set { marked: true } where key != ""
}
"#;

    let exact_dir = tempfile::tempdir().unwrap();
    let mut exact = Omnigraph::init(exact_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    load_jsonl(&mut exact, &bulk_update_fixture(LIMIT), LoadMode::Overwrite)
        .await
        .unwrap();
    let exact_result = exact
        .mutate("main", UPDATE_ALL, "update_all", &params(&[]))
        .await
        .expect("the exact mutation update-row limit is inclusive");
    assert_eq!(exact_result.affected_nodes, LIMIT);
    assert_eq!(count_rows(&exact, "node:Thing").await, LIMIT);

    let over_dir = tempfile::tempdir().unwrap();
    let mut over = Omnigraph::init(over_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    load_jsonl(
        &mut over,
        &bulk_update_fixture(LIMIT + 1),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let before = snapshot_main(&over).await.unwrap();
    let before_manifest = before.version();
    let entry = before.entry("node:Thing").unwrap();
    let before_table = entry.table_version;
    let table_uri = format!(
        "{}/{}",
        over.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;

    let error = over
        .mutate("main", UPDATE_ALL, "update_all", &params(&[]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 8192,
                actual: 8193,
            } if resource == "keyed rows for node:Thing"
        ),
        "one-over update must return the typed keyed-row limit, got {error:?}"
    );
    let after = snapshot_main(&over).await.unwrap();
    assert_eq!(after.version(), before_manifest);
    assert_eq!(
        after.entry("node:Thing").unwrap().table_version,
        before_table
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head,
        "one-over update must fail before a Lance table effect"
    );
    assert_eq!(count_rows(&over, "node:Thing").await, LIMIT + 1);
    let recovery_dir = over_dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "one-over update must fail before writing a recovery sidecar"
    );
}

/// Blob descriptors do not represent their payload allocation. An update must
/// consult `BlobFile::size()` against its remaining byte budget before calling
/// `BlobFile::read`; the scoped payload-read probe makes this a structural
/// assertion rather than inferring it from the eventual error.
#[tokio::test]
async fn mutation_update_rejects_oversized_blob_before_payload_read_pre_effect() {
    const LIMIT: u64 = 32 * 1024 * 1024;
    const SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
    note: String?
}
"#;
    const UPDATE: &str = r#"
query update_note($note: String) {
    update Document set { note: $note } where title = "wide"
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let external_path = dir.path().join("wide.blob");
    let file = std::fs::File::create(&external_path).unwrap();
    file.set_len(LIMIT + 1).unwrap();
    drop(file);
    let external_uri = format!("file://{}", external_path.display());

    let graph_path = dir.path().join("graph");
    let mut db = Omnigraph::init(graph_path.to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let row = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "wide",
            "content": external_uri,
        }
    })
    .to_string();
    load_jsonl(&mut db, &row, LoadMode::Overwrite)
        .await
        .unwrap();
    let before = snapshot_main(&db).await.unwrap();
    let before_manifest = before.version();
    let entry = before.entry("node:Document").unwrap();
    let before_table = entry.table_version;
    let table_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;

    let probes = omnigraph::instrumentation::MergeWriteProbes::default();
    let error = omnigraph::instrumentation::with_merge_write_probes(
        probes.clone(),
        db.mutate(
            "main",
            UPDATE,
            "update_note",
            &params(&[("$note", "bounded")]),
        ),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: LIMIT,
                actual,
            } if resource == "keyed bytes for node:Document" && actual > LIMIT
        ),
        "oversized update blob must be rejected before payload read, got {error:?}"
    );
    assert_eq!(
        probes.blob_payload_read_calls(),
        0,
        "BlobFile::size must reject the update before BlobFile::read"
    );
    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(after.version(), before_manifest);
    assert_eq!(
        after.entry("node:Document").unwrap().table_version,
        before_table
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head,
        "oversized update must fail before a Lance table effect"
    );
    let recovery_dir = graph_path.join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "oversized update must fail before writing a recovery sidecar"
    );
}

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
            &mixed_params(&[("$name", "Eve"), ("$victim", "Alice")], &[("$age", 22)]),
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
        manifest_err
            .message
            .contains("split into separate mutations"),
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

/// Overlapping delete predicates within one query must NOT double-count
/// `affected_*`. Deletes stage (they no longer inline-commit), so both
/// statements scan the same unchanged committed snapshot; counting each
/// predicate independently over-reports when they overlap. The contract —
/// matching the old inline path, where each delete committed before the next
/// ran — is the DISTINCT count of rows removed (= what the combined
/// `(p1) OR (p2)` staged delete actually removes).
///
/// Fixture: Alice(30), Bob(25), Charlie(35), Diana(28); Knows Alice→Bob,
/// Alice→Charlie, Bob→Diana; WorksAt Alice→Acme, Bob→Globex. `name = "Alice"`
/// ∪ `age > 29` = {Alice, Charlie} (2 distinct nodes); the combined cascade
/// removes {Alice→Bob, Alice→Charlie, Alice→Acme} (3 distinct edges — Charlie
/// adds none new). Buggy per-statement counting reports 3 nodes / 6 edges.
#[tokio::test]
async fn overlapping_delete_predicates_do_not_double_count_affected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let r = db
        .mutate(
            "main",
            STAGED_QUERIES,
            "delete_overlapping_persons",
            &mixed_params(&[("$name", "Alice")], &[("$threshold", 29)]),
        )
        .await
        .expect("delete-only mutation must succeed");

    assert_eq!(
        r.affected_nodes, 2,
        "distinct nodes removed are {{Alice, Charlie}}; overlapping predicates must not double-count",
    );
    assert_eq!(
        r.affected_edges, 3,
        "distinct edges removed are {{Alice→Bob, Alice→Charlie, Alice→Acme}}; cascade must not double-count",
    );

    // The data is correct regardless of the count: Bob + Diana remain.
    assert_eq!(
        count_rows(&db, "node:Person").await,
        2,
        "Bob and Diana remain"
    );
    assert_eq!(
        count_rows(&db, "edge:Knows").await,
        1,
        "only Bob→Diana remains"
    );
    assert_eq!(
        count_rows(&db, "edge:WorksAt").await,
        1,
        "only Bob→Globex remains",
    );
}

/// The overlap-exclusion filter must use SQL `IS NOT TRUE`, not `NOT`: a prior
/// delete predicate referencing a NULLable column must NOT drop a later
/// statement's matching row just because that column is NULL (SQL UNKNOWN).
/// With `NOT (age > 30)`, a row with NULL `age` makes the clause UNKNOWN and the
/// row is filtered out of `deleted_ids` — skipping its cascade (orphaned edges),
/// or, if it is the only match, leaving the node undeleted. This is a data bug,
/// not just a miscount.
///
/// Data: Charlie (age 35), Zoe (age NULL); Knows Zoe→Charlie. The query deletes
/// `age > 30` (Charlie) then `name = "Zoe"`. Zoe must still be deleted and her
/// edge cascaded despite the prior `age > 30` evaluating to UNKNOWN for her.
#[tokio::test]
async fn delete_dedup_filter_does_not_drop_null_column_rows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Person {
    name: String @key
    age: I32?
}
edge Knows: Person -> Person
"#;
    let data = r#"{"type":"Person","data":{"name":"Charlie","age":35}}
{"type":"Person","data":{"name":"Zoe"}}
{"edge":"Knows","from":"Zoe","to":"Charlie"}"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();

    let q = r#"
query del_age_then_name($threshold: I32, $name: String) {
    delete Person where age > $threshold
    delete Person where name = $name
}
"#;
    let r = db
        .mutate(
            "main",
            q,
            "del_age_then_name",
            &mixed_params(&[("$name", "Zoe")], &[("$threshold", 30)]),
        )
        .await
        .expect("delete-only mutation must succeed");

    assert_eq!(
        count_rows(&db, "node:Person").await,
        0,
        "both Charlie (age>30) and Zoe (name=Zoe, NULL age) must be deleted",
    );
    assert_eq!(
        count_rows(&db, "edge:Knows").await,
        0,
        "Zoe→Charlie must cascade — Zoe's NULL age must not skip her cascade",
    );
    assert_eq!(r.affected_nodes, 2, "Charlie + Zoe");
    assert_eq!(r.affected_edges, 1, "Zoe→Charlie, counted once");
}

/// `insert Person 'X'; update Person where name='X' set age=...` — both
/// ops produce content on `node:Person` and coalesce into one
/// `stage_merge_insert` at end-of-query. The accumulator's last-write-wins
/// dedupe (during `MutationStaging::stage_all`) ensures the update's value
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
    // end-of-query dedupe), proving the staged merge_insert ran with the
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

    // One-publish guarantee: manifest version advanced by exactly 1. The graph
    // commit (`graph_commit` + `graph_head` rows) rides the SAME publish CAS as
    // the table-version rows (RFC-013 Phase 7), so one graph commit is exactly
    // one manifest version bump.
    let post_version = version_main(&db).await.unwrap();
    assert_eq!(
        post_version,
        pre_version + 1,
        "insert+update query must publish exactly once",
    );
}

/// `insert Knows from='Alice' to='Bob'; insert Knows from='Alice' to='Eve'`
/// — both add rows to `edge:Knows`. The accumulator coalesces them into one
/// strict exact-id fenced write at end-of-query. Edge IDs are ULID-generated,
/// but the write still carries the same conflict filter as every graph table.
#[tokio::test]
async fn multiple_edge_inserts_coalesce_to_one_fenced_write() {
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
            &params(&[("$from", "Alice"), ("$a", "Bob"), ("$b", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_edges, 2);

    // Both edges visible.
    let edges_after = count_rows(&db, "edge:Knows").await;
    assert_eq!(edges_after, edges_before + 2);

    // One manifest version bump for the two-edge query (atomic publish): the
    // graph commit rides the same publish CAS as the table-version rows
    // (RFC-013 Phase 7).
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

    // One manifest version bump: the graph commit rides the same publish CAS
    // as the table-version rows (RFC-013 Phase 7).
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
    assert_eq!(
        mid_persons, pre_persons,
        "failed load must not advance Person count"
    );
    assert_eq!(
        mid_edges, pre_edges,
        "failed load must not advance Knows count"
    );

    // Second load against the same tables — succeeds (no HEAD drift).
    let good = r#"{"type": "Person", "data": {"name": "Pat", "age": 55}}"#;
    load_jsonl(&mut db, good, LoadMode::Append).await.unwrap();
    assert_eq!(
        count_rows(&db, "node:Person").await,
        pre_persons + 1,
        "follow-up load must succeed (no drift)",
    );
}

#[tokio::test]
async fn load_overwrite_with_bad_edge_reference_unblocks_next_load() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let pre_persons = count_rows(&db, "node:Person").await;
    let pre_edges = count_rows(&db, "edge:Knows").await;

    let bad = r#"{"type": "Person", "data": {"name": "Mallory", "age": 5}}
{"edge": "Knows", "from": "Mallory", "to": "Ghost"}
"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Overwrite)
        .await
        .expect_err("RI violation must fail overwrite before commit_staged");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("not found"),
        "unexpected error: {}",
        manifest_err.message,
    );

    assert_eq!(count_rows(&db, "node:Person").await, pre_persons);
    assert_eq!(count_rows(&db, "edge:Knows").await, pre_edges);

    // The good overwrite must be self-consistent: it replaces Person, so it also
    // replaces every edge table that referenced the old Persons. WorksAt is in the
    // batch (pointing the surviving Company at a new Person) so the retained
    // WorksAt rows that named Alice/Bob don't strand against the new node image.
    let good = r#"{"type": "Person", "data": {"name": "Pat", "age": 55}}
{"type": "Person", "data": {"name": "Quinn", "age": 56}}
{"edge": "Knows", "from": "Pat", "to": "Quinn"}
{"edge": "WorksAt", "from": "Pat", "to": "Acme"}
"#;
    load_jsonl(&mut db, good, LoadMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, 2);
    assert_eq!(count_rows(&db, "edge:Knows").await, 1);
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
    load_jsonl(&mut db, seed, LoadMode::Overwrite)
        .await
        .unwrap();

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

#[tokio::test]
async fn load_overwrite_with_cardinality_violation_unblocks_next_load() {
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
    load_jsonl(&mut db, seed, LoadMode::Overwrite)
        .await
        .unwrap();

    let pre_works = count_rows(&db, "edge:WorksAt").await;

    let bad = r#"{"edge": "WorksAt", "from": "Alice", "to": "Acme"}
{"edge": "WorksAt", "from": "Alice", "to": "Bigco"}
"#;
    let err = load_jsonl(&mut db, bad, LoadMode::Overwrite)
        .await
        .expect_err("cardinality violation must fail overwrite before commit_staged");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("@card violation"),
        "unexpected error: {}",
        manifest_err.message,
    );
    assert_eq!(count_rows(&db, "edge:WorksAt").await, pre_works);

    let good = r#"{"edge": "WorksAt", "from": "Alice", "to": "Acme"}"#;
    load_jsonl(&mut db, good, LoadMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(count_rows(&db, "edge:WorksAt").await, 1);
}

// ─── Chained-mutation correctness — pinned coverage ─────────────────────────

/// Chained `update` ops in one query must respect each previous op's
/// view of the rows. Without merge-shadow semantics on
/// `scan_with_pending`, the second update sees the stale committed value
/// (the first update's row still appears in the Lance scan because the
/// pending side hasn't committed), the predicate matches it, and the
/// end-of-query dedupe-last-wins step ends up applying the second update
/// to a row whose pending value should have shielded it.
///
/// Concretely: Alice starts at age=30 in TEST_DATA. Op-1 sets Alice to
/// age=99. Op-2 updates anyone with age > 50 to age=10. After op-1,
/// Alice's logical value is age=99 — within op-2's predicate. So op-2
/// SHOULD update Alice to age=10. The interesting case is: op-2 must
/// see Alice at age=99 (op-1's pending value), not age=30 (committed).
/// If the helper unioned without shadowing, op-2 would also match the
/// stale committed Alice (age=30 doesn't trigger the predicate, but the
/// row would appear twice and dedupe could pick either). The test
/// asserts both ends: Alice ends at age=10, the publisher publishes
/// once.
#[tokio::test]
async fn chained_updates_with_overlapping_predicate_respects_intermediate_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let pre_version = version_main(&db).await.unwrap();

    db.mutate(
        "main",
        STAGED_QUERIES,
        "update_then_filter_by_old_value",
        &mixed_params(
            &[("$first_name", "Alice")],
            &[
                ("$first_new_age", 99),
                ("$second_threshold", 50),
                ("$second_new_age", 10),
            ],
        ),
    )
    .await
    .unwrap();

    // After op-1: Alice = 99. After op-2 (where age > 50): Alice
    // matches (99 > 50) → set to 10. End state: Alice = 10.
    let batches = read_table(&db, "node:Person").await;
    let mut alice_age: Option<i32> = None;
    for batch in &batches {
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let ages = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if names.is_valid(i) && names.value(i) == "Alice" && ages.is_valid(i) {
                alice_age = Some(ages.value(i));
            }
        }
    }
    assert_eq!(
        alice_age,
        Some(10),
        "chained-update final value must reflect the second update applied to op-1's pending value"
    );

    // One manifest version bump: the graph commit rides the same publish CAS
    // as the table-version rows (RFC-013 Phase 7).
    let post_version = version_main(&db).await.unwrap();
    assert_eq!(
        post_version,
        pre_version + 1,
        "chained update must publish exactly once",
    );
}

/// Two `delete` ops on the same node table in one query. Pre-fix,
/// op-2's `open_table_for_mutation` went through
/// `open_for_mutation_on_branch` which trips `ensure_expected_version`
/// (Lance HEAD has advanced past the manifest's pinned version after
/// op-1's inline-commit, but the manifest hasn't moved). Post-fix,
/// `open_table_for_mutation` reopens via `inline_committed[table_key]`
/// at the post-delete Lance version. Test asserts both deletes succeed
/// in one query, both rows are gone, manifest version advances by 1.
#[tokio::test]
async fn multi_statement_delete_on_same_node_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let pre_persons = count_rows(&db, "node:Person").await;
    let pre_version = version_main(&db).await.unwrap();

    db.mutate(
        "main",
        STAGED_QUERIES,
        "delete_two_persons",
        &params(&[("$first", "Alice"), ("$second", "Bob")]),
    )
    .await
    .expect("multi-delete on same table must succeed");

    assert_eq!(
        count_rows(&db, "node:Person").await,
        pre_persons - 2,
        "both deletes must land",
    );
    // One manifest version bump: the graph commit (delete-only queries record
    // one too) rides the same publish CAS as the table-version rows
    // (RFC-013 Phase 7).
    let post_version = version_main(&db).await.unwrap();
    assert_eq!(
        post_version,
        pre_version + 1,
        "multi-delete query publishes exactly once at end",
    );

    // Both rows actually gone:
    for name in ["Alice", "Bob"] {
        let qr = db
            .query(
                ReadTarget::branch("main"),
                TEST_QUERIES,
                "get_person",
                &params(&[("$name", name)]),
            )
            .await
            .unwrap();
        assert_eq!(qr.num_rows(), 0, "{name} should be deleted");
    }
}

/// Cascade-then-explicit variant: deleting a node cascades to its
/// edges, advancing Lance HEAD on the edge table. A subsequent
/// `delete <Edge>` op in the same query must reopen at the
/// post-cascade-commit version of the edge table — not trip
/// `ensure_expected_version` against the manifest's pinned version.
#[tokio::test]
async fn cascade_delete_node_then_explicit_delete_edge_on_same_table() {
    const QUERY: &str = r#"
query cascade_then_explicit($name: String, $other: String) {
    delete Person where name = $name
    delete Knows where from = $other
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // TEST_DATA seeds three Knows edges:
    //   Alice → Bob, Alice → Charlie (cascade target — should be deleted by op-1)
    //   Bob → Diana                  (explicit target — should be deleted by op-2)
    // After both ops, all three edges must be gone. A weaker assertion
    // (just "count decreased") would pass even if op-2 silently no-op'd
    // — Bob→Diana would survive. The exact-count check makes both ops
    // independently observable.
    let pre_knows = count_rows(&db, "edge:Knows").await;
    assert_eq!(
        pre_knows, 3,
        "fixture invariant: TEST_DATA seeds 3 Knows edges"
    );

    db.mutate(
        "main",
        QUERY,
        "cascade_then_explicit",
        &params(&[("$name", "Alice"), ("$other", "Bob")]),
    )
    .await
    .expect("cascade-then-explicit-delete on same edge table must succeed");

    // Both ops landed: cascade removed Alice→Bob and Alice→Charlie;
    // explicit removed Bob→Diana. Anything > 0 means one op silently
    // did nothing (the bug we're guarding against).
    let post_knows = count_rows(&db, "edge:Knows").await;
    assert_eq!(
        post_knows, 0,
        "both cascade + explicit delete must complete (Bob→Diana would survive if op-2 no-op'd)",
    );
}

/// The engine cardinality path must enforce `min` bounds. Pre-fix the
/// engine path silently dropped the min check (a `let _ = card.min;`
/// line). The loader path always enforced both. Post-fix, both paths
/// route through `enforce_cardinality_bounds` which checks both bounds.
///
/// Build a custom schema with `Knows: Person -> Person @card(2..*)`.
/// Inserting a single Knows edge violates min=2. The mutation path must
/// reject.
#[tokio::test]
async fn mutation_insert_edge_enforces_min_cardinality() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    const MIN_CARD_SCHEMA: &str = r#"
node Person {
    name: String @key
}
edge Knows: Person -> Person @card(2..)
"#;
    const MIN_CARD_QUERY: &str = r#"
query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, MIN_CARD_SCHEMA).await.unwrap();

    let seed = r#"{"type": "Person", "data": {"name": "Alice"}}
{"type": "Person", "data": {"name": "Bob"}}
"#;
    load_jsonl(&mut db, seed, LoadMode::Overwrite)
        .await
        .unwrap();

    // Single insert: count=1 < min=2 → reject with clear message.
    let err = db
        .mutate(
            "main",
            MIN_CARD_QUERY,
            "add_friend",
            &params(&[("$from", "Alice"), ("$to", "Bob")]),
        )
        .await
        .expect_err("min cardinality must reject the engine path");
    let OmniError::Manifest(manifest_err) = err else {
        panic!("expected Manifest error, got {err:?}");
    };
    assert!(
        manifest_err.message.contains("@card violation") && manifest_err.message.contains("min 2"),
        "unexpected error: {}",
        manifest_err.message,
    );
}

/// `LoadMode::Merge` on edges must NOT double-count the committed
/// edge AND its updated pending replacement. Build a custom
/// schema where WorksAt has @card(0..1). Seed Alice with one WorksAt to
/// Acme. Then Merge-load the SAME edge id (so it's an update, not an
/// insert) pointing Alice's WorksAt at Bigco. Cardinality must count
/// Alice's edges as 1 (the post-merge count), not 2 (committed + pending).
#[tokio::test]
async fn load_merge_mode_dedupes_edge_for_cardinality_count() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    const CARD_SCHEMA: &str = r#"
node Person {
    name: String @key
}
node Company {
    name: String @key
}
edge WorksAt: Person -> Company @card(0..1)
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CARD_SCHEMA).await.unwrap();

    // Seed: Alice + Acme + Bigco + WorksAt(id=w1, Alice→Acme). Note the
    // loader reads edge ids from the `data.id` field (not top-level), so
    // we place the id inside `data` for both the seed and the update.
    let seed = r#"{"type": "Person", "data": {"name": "Alice"}}
{"type": "Company", "data": {"name": "Acme"}}
{"type": "Company", "data": {"name": "Bigco"}}
{"edge": "WorksAt", "from": "Alice", "to": "Acme", "data": {"id": "w1"}}
"#;
    load_jsonl(&mut db, seed, LoadMode::Overwrite)
        .await
        .unwrap();

    // Merge-update the same edge id w1 to point at Bigco. Counted naively
    // as union, Alice has 2 WorksAt (committed Acme + pending Bigco) which
    // would trip @card(0..1). With merge dedupe, Alice has 1 WorksAt.
    let merge_data = r#"{"edge": "WorksAt", "from": "Alice", "to": "Bigco", "data": {"id": "w1"}}
"#;
    load_jsonl(&mut db, merge_data, LoadMode::Merge)
        .await
        .expect("Merge update must dedupe the committed edge by id");

    // Confirm there's exactly 1 WorksAt edge after merge.
    assert_eq!(count_rows(&db, "edge:WorksAt").await, 1);
}

/// A Merge load whose input has TWO rows with the same edge id must be
/// deduped at cardinality-count time, not just during end-of-query staging. Without
/// dedup, two pending rows count twice → spurious `@card` violation.
/// With dedup (last-occurrence-wins, mirroring
/// `dedupe_merge_batches_by_id`), the pending side counts once.
///
/// This is a separate path from `load_merge_mode_dedupes_edge_for_cardinality_count`
/// (which dedupes committed-vs-pending). Here we verify pending-vs-pending
/// dedup.
#[tokio::test]
async fn load_merge_mode_dedupes_within_pending_for_cardinality_count() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    const CARD_SCHEMA: &str = r#"
node Person {
    name: String @key
}
node Company {
    name: String @key
}
edge WorksAt: Person -> Company @card(0..1)
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CARD_SCHEMA).await.unwrap();

    let seed = r#"{"type": "Person", "data": {"name": "Alice"}}
{"type": "Company", "data": {"name": "Acme"}}
{"type": "Company", "data": {"name": "Bigco"}}
"#;
    load_jsonl(&mut db, seed, LoadMode::Overwrite)
        .await
        .unwrap();

    // Merge load with the SAME edge id twice — the second row supersedes
    // the first in the end-of-query dedupe. If pending-counting doesn't
    // dedupe, Alice has 2 pending edges → @card(0..1) trips → load
    // fails. With dedupe, Alice has 1 → load succeeds.
    let dup_data = r#"{"edge": "WorksAt", "from": "Alice", "to": "Acme", "data": {"id": "w1"}}
{"edge": "WorksAt", "from": "Alice", "to": "Bigco", "data": {"id": "w1"}}
"#;
    load_jsonl(&mut db, dup_data, LoadMode::Merge)
        .await
        .expect("Merge load with within-input dup ids must dedupe pending count");

    // Exactly one WorksAt edge after the dedup; the second row wins
    // (last-occurrence) so dst should be Bigco.
    assert_eq!(count_rows(&db, "edge:WorksAt").await, 1);
}

/// A blob-table insert followed by a non-blob update must remain one
/// full-schema merge stream. The update reads the just-inserted pending row,
/// copies its logical blob column through, and replaces the pending row under
/// the existing last-write-wins shadow semantics. This used to produce a
/// partial update batch and fail schema validation at the second op.
#[tokio::test]
async fn blob_table_insert_then_non_blob_update_preserves_full_schema() {
    use omnigraph::loader::{LoadMode, load_jsonl};

    const BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
    note: String?
}
"#;
    const BLOB_QUERIES: &str = r#"
query insert_then_update_note(
    $title: String, $blob: String, $note: String
) {
    insert Document { title: $title, content: $blob }
    update Document set { note: $note } where title = $title
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();

    // Keep one committed blob row as well as the just-inserted pending row so
    // the table has the real blob-v2 physical representation while this query
    // exercises the pending union.
    load_jsonl(
        &mut db,
        r#"{"type":"Document","data":{"title":"seed","content":"base64:AQID"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let result = db
        .mutate(
            "main",
            BLOB_QUERIES,
            "insert_then_update_note",
            &params(&[
                ("$title", "letter"),
                ("$blob", "base64:BAUG"),
                ("$note", "draft 1"),
            ]),
        )
        .await
        .expect("blob-table insert + non-blob update must share a full-schema merge batch");
    assert_eq!(
        result.affected_nodes, 2,
        "one insert plus one pending-row update"
    );

    let bytes = read_blob_bytes(&db, "Document", "letter", "content").await;
    assert_eq!(&bytes[..], &[4, 5, 6]);

    let qr = db
        .query(
            ReadTarget::branch("main"),
            r#"query get_doc($title: String) {
                match { $d: Document { title: $title } }
                return { $d.title, $d.note }
            }"#,
            "get_doc",
            &params(&[("$title", "letter")]),
        )
        .await
        .unwrap();
    assert_eq!(qr.num_rows(), 1);
    let json = qr.to_sdk_json();
    let row = json.as_array().unwrap().first().unwrap();
    assert_eq!(row["d.title"], "letter");
    assert_eq!(row["d.note"], "draft 1");
}

/// MR-920 regression: two sequential `update T set {f:v} where x=y`
/// invocations against the same row must both succeed. Pre-fix, the
/// second one failed with `Ambiguous merge inserts are prohibited:
/// multiple source rows match the same target row on (id = "Alice")`
/// even though the scan returned exactly one row.
///
/// Root cause hypothesis (per MR-920): Lance's
/// `processed_row_ids: Mutex<HashSet<u64>>`
/// (`src/dataset/write/merge_insert.rs:2099`) double-processes the
/// same target row_id against datasets previously rewritten by
/// merge_insert. `SourceDedupeBehavior::FirstSeen` makes Lance skip
/// rather than error.
///
/// Companion to `consistency.rs::load_merge_repeated_against_overlapping_keys_succeeds`
/// (PR #98 / Window 1 of the bug class via the load surface).
#[tokio::test]
async fn second_sequential_update_on_same_row_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.mutate(
        "main",
        STAGED_QUERIES,
        "update_age_by_name",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .expect("first sequential update on Alice must succeed");

    let batches = read_table(&db, "node:Person").await;
    let alice_count: usize = batches
        .iter()
        .map(|b| {
            let names = b
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..b.num_rows())
                .filter(|i| names.is_valid(*i) && names.value(*i) == "Alice")
                .count()
        })
        .sum();
    assert_eq!(
        alice_count, 1,
        "after first update, exactly one Alice row should be visible"
    );

    db.mutate(
        "main",
        STAGED_QUERIES,
        "update_age_by_name",
        &mixed_params(&[("$name", "Alice")], &[("$age", 42)]),
    )
    .await
    .expect("second sequential update on Alice must succeed");

    let batches = read_table(&db, "node:Person").await;
    let mut alice_age: Option<i32> = None;
    for batch in &batches {
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let ages = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if names.is_valid(i) && names.value(i) == "Alice" && ages.is_valid(i) {
                alice_age = Some(ages.value(i));
            }
        }
    }
    assert_eq!(
        alice_age,
        Some(42),
        "Alice's age must reflect the second update"
    );
}

// An interrupted first-write fork (create_branch succeeded, the manifest
// publish did not) leaves a fully-formed Lance branch ref on the table that
// the manifest never references — a "manifest-unreferenced fork". The branch
// itself stays a valid manifest branch, so `cleanup`'s reconciler (keyed on
// the manifest branch list) never reclaims it. Today the next write to that
// table on that branch re-enters the fork path, `create_branch` collides, and
// the engine wedges with "incomplete prior delete; run `omnigraph cleanup`".
//
// We forge that exact residue (a live `feature` branch + a directly-created
// `feature` ref on the Person table the manifest doesn't reference) and assert
// the next write — via both `load` and `mutate` — self-heals by reclaiming the
// orphan fork and re-forking, rather than wedging. No process death / timing
// needed: the forge is the post-crash state.
#[tokio::test]
async fn first_write_self_heals_manifest_unreferenced_fork_on_live_branch() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    // Forge the manifest-unreferenced fork directly at the Lance layer.
    let main = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let person_path = &main.entry("node:Person").unwrap().table_path;
    let person_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        person_path.trim_start_matches('/')
    );
    {
        let mut ds = lance::Dataset::open(&person_uri).await.unwrap();
        let base = ds.version().version;
        ds.create_branch("feature", base, None).await.unwrap();
        assert!(
            ds.list_branches().await.unwrap().contains_key("feature"),
            "precondition: forged orphan fork present on Person"
        );
    }

    // load → must self-heal, not wedge with "incomplete prior delete".
    let row = r#"{"type":"Person","data":{"name":"Zoe","age":30}}"#;
    db.load_as("feature", None, row, LoadMode::Merge, None)
        .await
        .expect("load onto a manifest-unreferenced fork must self-heal, not wedge");

    // mutate → same path, must also self-heal.
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Yan")], &[("$age", 41)]),
    )
    .await
    .expect("mutate onto a manifest-unreferenced fork must self-heal");

    // The healed branch holds the new rows; main is untouched (still no Zoe/Yan).
    let feature_people = count_rows_branch(&db, "feature", "node:Person").await;
    let main_people = count_rows(&db, "node:Person").await;
    assert!(
        feature_people >= main_people + 2,
        "feature must contain the two new rows on top of the inherited set \
         (feature={feature_people}, main={main_people})"
    );
}

// A node delete cascades to every edge table touching that node, forking those
// edge tables during execution. The up-front fork-queue acquisition must cover
// those cascade-forked edges, not just the node table named in the IR — else
// commit_all's held-guard coverage check fails the write (and, before the
// coverage check was promoted out of debug-only, edge commits would slip
// through unserialized). This drives the new code via a DELETE (the only
// cascading op), on a branch, as the FIRST write (so it actually forks).
#[tokio::test]
async fn branch_cascade_delete_forks_node_and_edges_under_held_queues() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    // Baseline inherited from main (Alice has 2 Knows + 1 WorksAt edge).
    let main_people = count_rows(&db, "node:Person").await;
    let main_knows = count_rows(&db, "edge:Knows").await;

    // First write to `feature` is `delete Person Alice`, whose cascade forks
    // node:Person AND edge:Knows + edge:WorksAt. Pre-fix the up-front set held
    // only node:Person, so commit_all's coverage check rejected the write.
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "Alice")], &[]),
    )
    .await
    .expect("branch cascade-delete must hold queues for cascade-forked edge tables");

    // Alice and her edges are gone on feature; main is untouched.
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Person").await,
        main_people - 1,
        "feature should have Alice removed from the inherited set"
    );
    assert!(
        count_rows_branch(&db, "feature", "edge:Knows").await < main_knows,
        "feature should have Alice's cascade-deleted Knows edges removed"
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        main_people,
        "main must be untouched by the branch delete"
    );
}

// #283: a mutation predicate (`where camelField = ...`) on a camelCase column
// must execute, not fail at the Lance scan with "No field named ...". Covers
// both `update` (committed scan via scan_with_pending) and `delete`
// (stage_delete), which share the same emitted SQL filter string.
const CC_SCHEMA: &str = r#"
node Doc {
    slug: String @key
    repoName: String @index
    status: String?
}
"#;
const CC_DATA: &str = r#"{"type":"Doc","data":{"slug":"d1","repoName":"acme","status":"open"}}
{"type":"Doc","data":{"slug":"d2","repoName":"globex","status":"open"}}"#;

#[tokio::test]
async fn camelcase_mutation_predicate_updates_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CC_SCHEMA).await.unwrap();
    load_jsonl(&mut db, CC_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    let m = r#"
query set_status($repo: String, $st: String) { update Doc set { status: $st } where repoName = $repo }
query del($repo: String) { delete Doc where repoName = $repo }
"#;

    let upd = db
        .mutate(
            "main",
            m,
            "set_status",
            &params(&[("$repo", "acme"), ("$st", "closed")]),
        )
        .await
        .expect("update with a camelCase predicate must execute");
    assert_eq!(upd.affected_nodes, 1, "exactly the acme Doc should update");

    let del = db
        .mutate("main", m, "del", &params(&[("$repo", "globex")]))
        .await
        .expect("delete with a camelCase predicate must execute");
    assert_eq!(
        del.affected_nodes, 1,
        "exactly the globex Doc should delete"
    );

    assert_eq!(
        count_rows(&db, "node:Doc").await,
        1,
        "one Doc (acme) should remain"
    );
}

// #283 (pending side): a chained mutation whose 2nd op filters a camelCase
// column must read op-1's staged rows through the pending DataFusion `MemTable`
// (`SELECT … WHERE {filter}` via ctx.sql), which lowercases unquoted idents.
// This is the path the single update/delete above does NOT exercise.
#[tokio::test]
async fn camelcase_chained_mutation_reads_pending_by_camelcase() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, CC_SCHEMA).await.unwrap();
    load_jsonl(&mut db, CC_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // op-1 stages a status change to the acme Doc; op-2 re-filters the same
    // camelCase column, so it must match op-1's pending row.
    let m = r#"
query chain($repo: String) {
    update Doc set { status: "stage1" } where repoName = $repo
    update Doc set { status: "stage2" } where repoName = $repo
}
"#;
    let r = db
        .mutate("main", m, "chain", &params(&[("$repo", "acme")]))
        .await
        .expect(
            "chained camelCase mutation must read the pending row, not fail at the MemTable SELECT",
        );
    assert_eq!(
        r.affected_nodes, 2,
        "both ops should touch the acme Doc (read-your-writes)"
    );
}

/// A zero-row cascade delete must not advance an edge table's Lance HEAD past
/// its manifest version. A `delete <Node>` cascades a delete into every incident
/// edge type (`exec/mutation.rs`). The original bug this guards against: the old
/// inline `delete_where` (`Dataset::delete`) advanced Lance HEAD **even when zero
/// edges matched**, while the cascade recorded the new version in the manifest
/// only `if deleted_rows > 0`. So deleting a node with no incident edges advanced
/// `edge:Knows` Lance HEAD while the manifest stayed behind — a `HEAD > manifest`
/// drift that then tripped the next strict write's `ExpectedVersionMismatch`, and
/// `repair` refused (delete-class drift), wedging the graph.
///
/// This pins the invariant directly: after any node delete, every edge table's
/// manifest version must equal its on-disk Lance HEAD — no write may advance HEAD
/// past the manifest (invariant 2 / the deny-list). Now GREEN: `delete` is staged
/// (MR-A / iss-950, via Lance 7.0's `DeleteBuilder::execute_uncommitted`), so a
/// 0-row delete commits no Lance version at all — correct by construction.
#[tokio::test]
async fn node_delete_with_no_incident_edges_leaves_no_edge_table_drift() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let root = dir.path().to_str().unwrap().to_string();

    // A person with NO Knows edges. Deleting it cascades a 0-row delete
    // into `edge:Knows` (the cascade runs for every incident edge type).
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Loner")], &[("$age", 30)]),
    )
    .await
    .unwrap();
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Loner")]),
    )
    .await
    .expect("the first delete itself succeeds — it leaves the drift for the NEXT write");

    // The invariant: edge:Knows manifest version == its on-disk Lance HEAD.
    let snap = snapshot_main(&db).await.unwrap();
    let entry = snap
        .entry("edge:Knows")
        .expect("edge:Knows must be in the manifest");
    let full = format!("{}/{}", root.trim_end_matches('/'), entry.table_path);
    let head = Dataset::open(&full).await.unwrap().version().version;
    assert_eq!(
        entry.table_version, head,
        "a node delete matching no edges advanced edge:Knows Lance HEAD to v{head} but the \
         manifest still records v{} — HEAD>manifest drift from a 0-row cascade delete. A staged \
         0-row delete must commit no Lance version at all (MR-A); this drift means that \
         regressed.",
        entry.table_version,
    );
}

/// RFC-013 PR2 #1b: the publisher folds the new `known_state` in-memory after a
/// publish instead of re-scanning `__manifest`. That fold MUST be byte-identical
/// to a fresh re-scan, or the warm coordinator silently desyncs. After a sequence
/// of writes (insert, a second insert to the same table, then a delete that
/// advances the table version), the in-memory coordinator holds the folded state;
/// a freshly reopened graph rebuilds it via a real `read_manifest_state` scan.
/// Counting `node:Person` off each resolves the table at the version each side
/// recorded — a fold that set the wrong version (or path → open failure) makes the
/// in-memory count diverge from the reopened one. Reopen is the scan side; the live
/// `db` is the fold side.
#[tokio::test]
async fn post_publish_fold_matches_fresh_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;

    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "fold_a")], &[("$age", 30)]),
    )
    .await
    .unwrap();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "fold_b")], &[("$age", 31)]),
    )
    .await
    .unwrap();
    db.mutate(
        "main",
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "fold_a")], &[]),
    )
    .await
    .unwrap();

    // Fold side: count resolves the snapshot from the in-memory folded known_state.
    let folded = count_rows(&db, "node:Person").await;

    // Scan side: a fresh open rebuilds known_state via `read_manifest_state`.
    let reopened = Omnigraph::open(uri).await.unwrap();
    let scanned = count_rows(&reopened, "node:Person").await;

    assert_eq!(
        folded, scanned,
        "post-publish fold diverged from a fresh re-scan (folded {folded} vs scanned {scanned})"
    );
}

const FIND_PERSON_QUERY: &str = r#"
query find_person($name: String) {
    match { $p: Person { name: $name } }
    return { $p.name }
}
"#;

/// Regression: iss-merge-rowid-overlap-corrupts-filtered-reads / lance#7444.
///
/// An update-style merge (same-key merge load) reuses the updated rows'
/// stable row ids in the rewritten fragments while the superseded fragment
/// keeps its full row-id sequence plus a deletion vector — overlapping
/// cross-fragment id ranges, legal per the Lance row-id-lineage spec. A
/// later delete punches a hole in that overlapping range; on unpatched
/// Lance 7.0.0 `RowIdIndex::new` then fails any filtered scan that needs
/// the id→address map ("all columns in a record batch must have the same
/// length" in release, a "Wrong range" debug assert). Fixed upstream by
/// lance#7480; consumed here via the vendored `lance-table` patch.
#[tokio::test]
async fn filtered_read_after_merge_update_and_delete_keeps_row_ids_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let seed: String = (1..=40)
        .map(|i| format!("{{\"type\":\"Person\",\"data\":{{\"name\":\"p{i}\",\"age\":{i}}}}}\n"))
        .collect();
    load_jsonl(&mut db, &seed, LoadMode::Merge).await.unwrap();

    // Same-key updates: Lance Operation::Update rewrites these 15 rows into
    // new fragments that keep their original stable row ids (the overlap).
    let updates: String = (1..=15)
        .map(|i| {
            format!(
                "{{\"type\":\"Person\",\"data\":{{\"name\":\"p{i}\",\"age\":{}}}}}\n",
                100 + i
            )
        })
        .collect();
    load_jsonl(&mut db, &updates, LoadMode::Merge)
        .await
        .unwrap();

    // The delete adds a deletion vector, so the overlapping region no longer
    // densely tiles its id range — the shape lance#7444 choked on.
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "p20")], &[]),
    )
    .await
    .unwrap();

    // Filtered point lookups must still resolve: an updated row, an
    // untouched row, and the deleted row (absent), each via the key filter.
    for (name, expected) in [("p3", vec!["p3"]), ("p30", vec!["p30"]), ("p20", vec![])] {
        let result = query_main(
            &mut db,
            FIND_PERSON_QUERY,
            "find_person",
            &mixed_params(&[("$name", name)], &[]),
        )
        .await
        .unwrap_or_else(|e| panic!("filtered read for {name} failed: {e}"));
        let got = first_column_sorted(&result);
        assert_eq!(got, expected, "filtered read for {name}");
    }
}

/// Isolation control for the regression above: the same load/delete/filtered
/// read walk WITHOUT same-key updates (append-only merges, disjoint keys)
/// never produces overlapping row-id ranges and passes on unpatched Lance.
/// If this one fails alongside the merge-update case, the defect is not the
/// lance#7444 overlap shape.
#[tokio::test]
async fn filtered_read_after_append_and_delete_is_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    let seed: String = (1..=40)
        .map(|i| format!("{{\"type\":\"Person\",\"data\":{{\"name\":\"p{i}\",\"age\":{i}}}}}\n"))
        .collect();
    load_jsonl(&mut db, &seed, LoadMode::Merge).await.unwrap();

    // Disjoint keys: plain inserts, no fragment rewrite, no id reuse.
    let more: String = (41..=55)
        .map(|i| format!("{{\"type\":\"Person\",\"data\":{{\"name\":\"p{i}\",\"age\":{i}}}}}\n"))
        .collect();
    load_jsonl(&mut db, &more, LoadMode::Merge).await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &mixed_params(&[("$name", "p20")], &[]),
    )
    .await
    .unwrap();

    for (name, expected) in [("p3", vec!["p3"]), ("p50", vec!["p50"]), ("p20", vec![])] {
        let result = query_main(
            &mut db,
            FIND_PERSON_QUERY,
            "find_person",
            &mixed_params(&[("$name", name)], &[]),
        )
        .await
        .unwrap_or_else(|e| panic!("filtered read for {name} failed: {e}"));
        let got = first_column_sorted(&result);
        assert_eq!(got, expected, "filtered read for {name}");
    }
}
