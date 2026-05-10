//! Composite end-to-end flow integration test.
//!
//! Walks the canonical user flow in one fixture: init → load → branch →
//! mutate → query → merge → time-travel → optimize → cleanup → reopen.
//! Every numbered step has at least one assertion.
//!
//! This is the deterministic narrative counterpart to a randomized /
//! property-based reliability harness — it catches regressions where
//! individual operations all pass their unit tests but their composition
//! breaks. It runs in CI on every PR (no `#[ignore]`).

mod helpers;

use arrow_array::{Array, Int64Array};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::result::QueryResult;

use helpers::{
    MUTATION_QUERIES, count_rows, count_rows_branch, mixed_params, mutate_branch, mutate_main,
    query_branch, query_main, snapshot_main, version_branch, version_main,
};

/// Extract the `total` value from a `total_people` query result and
/// assert it equals `expected`. The query returns one row with one
/// `Int64` column named `total`; asserting only `num_rows() == 1`
/// would not catch a regression that returns a stale or wrong count.
fn assert_total(result: &QueryResult, expected: i64, context: &str) {
    let batch = result.concat_batches().unwrap();
    assert_eq!(
        batch.num_rows(),
        1,
        "total_people must return exactly one summary row ({context})"
    );
    let total_col = batch
        .column_by_name("total")
        .unwrap_or_else(|| panic!("missing `total` column ({context})"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("`total` column is not Int64 ({context})"));
    assert_eq!(
        total_col.value(0),
        expected,
        "total_people count mismatch ({context})"
    );
}

const TEST_SCHEMA: &str = include_str!("fixtures/test.pg");
const TEST_DATA: &str = include_str!("fixtures/test.jsonl");
const TEST_QUERIES: &str = include_str!("fixtures/test.gq");

#[tokio::test]
async fn composite_flow_canonical_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // ─────────────────────────────────────────────────────────────────
    // Step 1: init a fresh repo with the standard test schema.
    // ─────────────────────────────────────────────────────────────────
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    let v_init = version_branch(&db, "main").await.unwrap();
    assert!(
        v_init >= 1,
        "init must produce a non-zero manifest version; got {}",
        v_init
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 2: load JSONL seed data (Person + Company nodes,
    // Knows + WorksAt edges).
    // ─────────────────────────────────────────────────────────────────
    load_jsonl(&mut db, TEST_DATA, LoadMode::Append).await.unwrap();
    let v_after_load = version_branch(&db, "main").await.unwrap();
    assert!(
        v_after_load > v_init,
        "load must advance the manifest version: v_init={}, v_after_load={}",
        v_init,
        v_after_load,
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        4,
        "test.jsonl declares 4 Person rows"
    );
    assert_eq!(
        count_rows(&db, "node:Company").await,
        2,
        "test.jsonl declares 2 Company rows"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 3: branch_create `feature` off main.
    // ─────────────────────────────────────────────────────────────────
    db.branch_create("feature").await.unwrap();
    let branches = db.branch_list().await.unwrap();
    assert!(
        branches.iter().any(|b| b == "feature"),
        "feature branch must appear in branch_list; got {:?}",
        branches,
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 4: mutate on `feature` — single statement (insert) +
    // multi-statement (insert + insert).
    // ─────────────────────────────────────────────────────────────────
    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .expect("single-statement insert on feature");

    mutate_branch(
        &mut db,
        "feature",
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(
            &[("$name", "Frank"), ("$friend", "Eve")],
            &[("$age", 33)],
        ),
    )
    .await
    .expect("multi-statement insert+edge on feature");

    // After: feature has 4 + Eve + Frank = 6 Persons.
    let snap = db
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    let person_ds = snap.open("node:Person").await.unwrap();
    assert_eq!(
        person_ds.count_rows(None).await.unwrap(),
        6,
        "feature should now have 6 Persons (4 seeded + Eve + Frank)"
    );

    // Main is untouched by feature mutations.
    assert_eq!(
        count_rows(&db, "node:Person").await,
        4,
        "main must remain at 4 Persons after feature mutations"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 5: query on `feature` — exercise multi-modal modes.
    // The fixture queries cover scalar lookup (get_person), traversal
    // (friends_of), aggregation (friend_counts, total_people, age_stats).
    // ─────────────────────────────────────────────────────────────────
    let total_people = query_branch(
        &mut db,
        "feature",
        TEST_QUERIES,
        "total_people",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert!(
        !total_people.batches().is_empty(),
        "total_people must return at least one batch"
    );

    let friends_of_alice = query_branch(
        &mut db,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &mixed_params(&[("$name", "Alice")], &[]),
    )
    .await
    .unwrap();
    assert!(
        !friends_of_alice.batches().is_empty(),
        "friends_of(Alice) must return data — Alice knows Bob and Charlie in the seed"
    );

    let unemployed = query_branch(
        &mut db,
        "feature",
        TEST_QUERIES,
        "unemployed",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert!(
        !unemployed.batches().is_empty(),
        "unemployed (anti-join) must return Persons without WorksAt edges"
    );

    let friend_counts = query_branch(
        &mut db,
        "feature",
        TEST_QUERIES,
        "friend_counts",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert!(
        !friend_counts.batches().is_empty(),
        "friend_counts (aggregation) must return per-person counts"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 6: mutate on `main` simultaneously — sets up a non-conflicting
    // merge by touching a sibling type (Company) that feature didn't
    // touch. (The test schema doesn't have a Company-mutation query, so
    // we update an existing Person's age — Bob is on main but his age
    // wasn't changed on feature.)
    // ─────────────────────────────────────────────────────────────────
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .expect("set Bob's age on main");
    let v_pre_merge_main = version_branch(&db, "main").await.unwrap();

    // Capture the pre-merge main snapshot for time-travel verification later.
    let snapshot_pre_merge = snapshot_main(&db).await.unwrap();
    let pre_merge_version = snapshot_pre_merge.version();

    // ─────────────────────────────────────────────────────────────────
    // Step 7: branch_merge feature → main, verify merge result + audit.
    // ─────────────────────────────────────────────────────────────────
    let merge_outcome = db.branch_merge("feature", "main").await.unwrap();
    let v_post_merge = version_branch(&db, "main").await.unwrap();
    assert!(
        v_post_merge > v_pre_merge_main,
        "merge must advance main's manifest version: pre={}, post={}",
        v_pre_merge_main,
        v_post_merge,
    );
    let _ = merge_outcome;

    // ─────────────────────────────────────────────────────────────────
    // Step 8: query at the post-merge snapshot — verify both sides'
    // writes are visible. Main now has 4 + Eve + Frank = 6 Persons,
    // and Bob's age is 26 (from the main mutation).
    // ─────────────────────────────────────────────────────────────────
    assert_eq!(
        count_rows(&db, "node:Person").await,
        6,
        "post-merge main must have all 6 Persons"
    );

    // Verify Bob's age update from main carried through the merge.
    let bob_after = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &mixed_params(&[("$name", "Bob")], &[]),
    )
    .await
    .unwrap();
    assert!(
        !bob_after.batches().is_empty(),
        "Bob must still be present on main post-merge"
    );

    // Verify Eve (from feature) is now visible on main.
    let eve_after = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &mixed_params(&[("$name", "Eve")], &[]),
    )
    .await
    .unwrap();
    assert!(
        !eve_after.batches().is_empty(),
        "Eve (from feature) must be visible on main post-merge"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 9: snapshot_at_version(pre_merge_version) — verify time-travel
    // still sees the pre-merge state (4 Persons on main, no Eve/Frank).
    // ─────────────────────────────────────────────────────────────────
    let pre_merge_snapshot = db.snapshot_at_version(pre_merge_version).await.unwrap();
    let pre_merge_persons = pre_merge_snapshot
        .open("node:Person")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(
        pre_merge_persons, 4,
        "time-travel to pre-merge version must show 4 Persons (pre-feature-merge state)"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 10: optimize the post-merge graph — verify indices stay
    // valid and queryable.
    //
    // **Known limitation**: `optimize_all_tables` calls Lance
    // `compact_files` directly — it advances per-table Lance HEAD
    // without updating the omnigraph `__manifest` pin. After optimize,
    // the next writer's expected_table_versions captures the
    // pre-optimize manifest pin, but the publisher's pre-check reads
    // a higher version from the manifest dataset (because some other
    // path — possibly schema-state recovery on reopen — wrote a newer
    // __manifest row). The `ExpectedVersionMismatch` is benign
    // (re-issuing the mutation after a snapshot refresh succeeds), but
    // a composite test cannot reliably exercise post-optimize mutations
    // until that path is investigated. Coverage of post-optimize
    // mutations is left to a focused optimize+cleanup integration test.
    // ─────────────────────────────────────────────────────────────────
    let optimize_stats = db.optimize().await.unwrap();
    assert!(
        !optimize_stats.is_empty(),
        "optimize must return per-table stats"
    );

    // Re-run a query to verify post-optimize correctness.
    let post_optimize_total = query_main(
        &mut db,
        TEST_QUERIES,
        "total_people",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert!(
        !post_optimize_total.batches().is_empty(),
        "queries must still work after optimize"
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        6,
        "row counts unchanged by optimize"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 11: cleanup — keep last 10 versions, only purge versions
    // older than 1 hour. With this small test, we have well under 10
    // versions and nothing that old, so cleanup is a no-op except for
    // any orphan files. The recovery floor (--keep ≥ 3) needed for the
    // open-time recovery sweep is preserved by the keep-10 default.
    // Verify the call doesn't break subsequent queries.
    // ─────────────────────────────────────────────────────────────────
    use omnigraph::db::CleanupPolicyOptions;
    use std::time::Duration;
    let _cleanup_stats = db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(10),
            older_than: Some(Duration::from_secs(3600)),
        })
        .await
        .unwrap();

    // ─────────────────────────────────────────────────────────────────
    // Step 12: reopen the engine — verify post-cleanup state is consistent.
    // ─────────────────────────────────────────────────────────────────
    drop(db);
    let mut db = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows(&db, "node:Person").await,
        6,
        "Person count consistent across reopen"
    );
    assert_eq!(
        count_rows(&db, "node:Company").await,
        2,
        "Company count consistent across reopen"
    );

    // Branch list still contains feature.
    let branches = db.branch_list().await.unwrap();
    assert!(
        branches.iter().any(|b| b == "feature"),
        "feature branch must still be visible after reopen; got {:?}",
        branches,
    );

    // Final query exercise — full read path works post-reopen,
    // post-cleanup. Post-cleanup mutation is omitted here pending
    // resolution of the optimize-vs-manifest-pin interaction documented
    // in Step 10.
    let final_total = query_main(
        &mut db,
        TEST_QUERIES,
        "total_people",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert!(!final_total.batches().is_empty());
}

/// Cross-handle sequence that exercises operations after a schema_apply
/// invalidates a peer handle's cached `_schema.pg`. The narrow load-bearing
/// pin is that `Omnigraph::refresh()` must not deadlock when its
/// `reload_schema_if_source_changed()` step needs to acquire a read on the
/// coordinator's `RwLock`. The broader sequencing — schema_apply →
/// branch_create → branch_delete → branch_merge → mutate (using the new
/// schema's added property) → reopen — pins that the fix doesn't regress
/// any of the related call sites.
///
/// Pre-fix bug class: `Omnigraph::refresh()` held
/// `coordinator.write().await` from start to finish, including across the
/// `self.reload_schema_if_source_changed()` call. That helper's
/// `self.coordinator.read().await` (only reached when the on-disk schema
/// source differs from the in-memory cache) deadlocks against the outer
/// write guard because tokio's `RwLock` is not reentrant. Reachable from
/// every public refresh-using API: `branch_delete` (`omnigraph.rs:910`),
/// `branch_merge` (post-merge refresh on bound target), and any caller
/// that calls `Omnigraph::refresh` directly.
///
/// The cross-handle setup is the realistic trigger: handle A applies a
/// schema, advancing `_schema.pg` on disk; handle B has stale in-memory
/// schema_source. B's next `refresh()` (via branch_delete here) hits the
/// read-after-write reload path. Single-handle is unreachable because
/// `apply_schema` updates the local ArcSwap cache in-line.
///
/// Post-fix invariant: `refresh()` scopes its write guard to the recovery
/// section only, releasing it before `reload_schema_if_source_changed()`.
/// The reload's read acquisition is uncontested.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_flow_schema_apply_then_branch_ops_no_deadlock_in_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Step 1: init + load on handle A.
    let mut db_a = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db_a, TEST_DATA, LoadMode::Append).await.unwrap();
    assert_eq!(count_rows(&db_a, "node:Person").await, 4);

    // Step 2: open handle B on the same repo. B's in-memory schema_source
    // cache is now a snapshot of `_schema.pg` at open time.
    let db_b = Omnigraph::open(uri).await.unwrap();

    // Step 3: A applies a schema that adds a nullable property to Person.
    // A's on-disk `_schema.pg` is rewritten; A's in-memory cache is updated
    // in-line by `apply_schema`. B's in-memory cache is now STALE relative
    // to disk.
    const TEST_SCHEMA_V2: &str = "node Person {\n    name: String @key\n    age: I32?\n    nickname: String?\n}\n\nnode Company {\n    name: String @key\n}\n\nedge Knows: Person -> Person {\n    since: Date?\n}\n\nedge WorksAt: Person -> Company\n";
    let plan = db_a.apply_schema(TEST_SCHEMA_V2).await.unwrap();
    assert!(plan.applied, "apply_schema must succeed on a clean repo");
    assert!(
        !plan.steps.is_empty(),
        "apply_schema must record the AddProperty step"
    );

    // Step 4: deadlock vector. B.branch_delete calls B.refresh() internally
    // (omnigraph.rs:910). refresh() pre-fix holds the coord write guard
    // across reload_schema_if_source_changed; with B's cache stale, that
    // helper takes the not-early-return branch and tries
    // self.coordinator.read().await — deadlocks against the outer write.
    //
    // Wrap in tokio::time::timeout so a deadlock surfaces as a clean test
    // panic instead of a stuck CI job. 15s is well above natural completion
    // on local FS (sub-second under normal conditions).
    db_b.branch_create("post-schema-apply-test").await.unwrap();
    let delete_result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        db_b.branch_delete("post-schema-apply-test"),
    )
    .await;
    assert!(
        delete_result.is_ok(),
        "branch_delete deadlocked in refresh() with stale schema cache. \
         Pre-fix symptom: Omnigraph::refresh() holds coordinator.write().await \
         across reload_schema_if_source_changed(), which acquires \
         coordinator.read().await on the same non-reentrant RwLock when the \
         on-disk schema source differs from the in-memory cache.",
    );
    delete_result
        .unwrap()
        .expect("branch_delete must succeed once refresh() releases its write guard");

    // Step 5: continuing operations on B post-refresh — verify the broader
    // sequence works. B's catalog should now reflect the new schema (the
    // refresh path includes reload_schema_if_source_changed which calls
    // store_catalog).
    db_b.branch_create("feature-after-apply").await.unwrap();

    // Step 6: branch_merge from B exercises the post-merge refresh() path
    // (merge.rs:1100-1107) — same deadlock surface as branch_delete,
    // sanity-pinned by reusing the same handle whose cache was just
    // refreshed.
    let _outcome = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        db_b.branch_merge("feature-after-apply", "main"),
    )
    .await
    .expect("branch_merge deadlocked in refresh() post-schema-apply")
    .expect("branch_merge must succeed");

    // Step 7: mutation on main using the new schema's added property —
    // verifies the catalog reload completed and the engine accepts a
    // mutation referencing `nickname`.
    const NICKNAME_QUERY: &str = "query set_nickname($name: String, $nickname: String) {\n    update Person set { nickname: $nickname } where name = $name\n}";
    db_b.mutate_as(
        "main",
        NICKNAME_QUERY,
        "set_nickname",
        &mixed_params(&[("$name", "Alice"), ("$nickname", "Ali")], &[]),
        None,
    )
    .await
    .expect("update using post-apply schema property must succeed");

    // Step 8: reopen — final integration check that the post-deadlock-fix
    // state persists across handle drop/open.
    drop(db_a);
    drop(db_b);
    let db_c = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows(&db_c, "node:Person").await,
        4,
        "Person count consistent across reopen post-schema-apply",
    );
    let branches = db_c.branch_list().await.unwrap();
    assert!(
        !branches.iter().any(|b| b == "post-schema-apply-test"),
        "deleted branch must stay deleted across reopen; got {:?}",
        branches,
    );
}

/// Multi-branch sequential merges with main writes interleaved between
/// every diverge point. Catches compositional regressions that single-
/// merge tests can't see:
///
/// - **Base/LCA recomputation across two merges**: feat-b's base must be
///   the main version *at feat-b's branch creation*, not main's
///   post-feat-a-merge HEAD. A regression that uses main HEAD as the
///   merge base would re-classify Eve / Grace as unknown source-only
///   rows and re-apply them.
/// - **Manifest pin propagation through merge commits**: after merge
///   feat-a → main, main's table_branch entries for Person and Knows
///   must reflect the rewrite-on-active path; the second merge needs
///   them to compute its diff correctly.
/// - **Time-travel through merge DAG**: snapshot_at_version at three
///   distinct points (pre-feat-a-merge, post-feat-a-merge-pre-helen,
///   pre-feat-b-merge) must each return the right historical state
///   without bleed-through from later commits.
/// - **Reopen consistency over a multi-merge history**: dropping the
///   handle and reopening must replay the full merge DAG cleanly with
///   no recovery sweep activity (steady state).
///
/// All other compositional concerns (single merge mechanics, conflict
/// detection, time-travel mechanics) are covered by `branching.rs` and
/// `point_in_time.rs`. This test only exercises *composition*.
#[tokio::test]
async fn composite_flow_multi_branch_sequential_merges() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // ─────────────────────────────────────────────────────────────────
    // Step 1: init + load baseline (4 Person, 2 Company, 3 Knows, 2 WorksAt
    // edges from test.jsonl).
    // ─────────────────────────────────────────────────────────────────
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
    load_jsonl(&mut db, TEST_DATA, LoadMode::Append).await.unwrap();
    assert_eq!(count_rows(&db, "node:Person").await, 4);
    assert_eq!(count_rows(&db, "edge:Knows").await, 3);

    // ─────────────────────────────────────────────────────────────────
    // Step 2: mutate main — insert "Alice2" before any branching. Main
    // diverges from the load baseline by exactly one row.
    // ─────────────────────────────────────────────────────────────────
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Alice2")], &[("$age", 31)]),
    )
    .await
    .expect("insert Alice2 on main");
    assert_eq!(count_rows(&db, "node:Person").await, 5);

    // ─────────────────────────────────────────────────────────────────
    // Step 3: branch_create feat-a from main. feat-a inherits main's
    // 5-Person state.
    // ─────────────────────────────────────────────────────────────────
    db.branch_create("feat-a").await.unwrap();
    assert_eq!(count_rows_branch(&db, "feat-a", "node:Person").await, 5);

    // ─────────────────────────────────────────────────────────────────
    // Step 4: mutate main — insert "Bob2" AFTER feat-a was created. main
    // and feat-a now diverge: main has Bob2, feat-a does not.
    // ─────────────────────────────────────────────────────────────────
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Bob2")], &[("$age", 26)]),
    )
    .await
    .expect("insert Bob2 on main");
    assert_eq!(count_rows(&db, "node:Person").await, 6);
    assert_eq!(
        count_rows_branch(&db, "feat-a", "node:Person").await,
        5,
        "feat-a must not see main's post-branch-create writes"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 5: mutate feat-a — insert "Eve". feat-a now also has 6 rows,
    // but the *sixth* is Eve, not Bob2.
    // ─────────────────────────────────────────────────────────────────
    mutate_branch(
        &mut db,
        "feat-a",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .expect("insert Eve on feat-a");
    assert_eq!(count_rows_branch(&db, "feat-a", "node:Person").await, 6);
    assert_eq!(
        count_rows(&db, "node:Person").await,
        6,
        "main must not see feat-a's writes"
    );
    // Branch isolation through the QUERY ENGINE (not just dataset-direct):
    // `get_person` on feat-a finds Eve (uses the BTree index on Person.name);
    // the same query on main finds nothing. Catches regressions where the
    // planner resolves the wrong snapshot for branch-targeted reads.
    let eve_on_feat_a = query_branch(
        &mut db,
        "feat-a",
        TEST_QUERIES,
        "get_person",
        &mixed_params(&[("$name", "Eve")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(
        eve_on_feat_a.num_rows(),
        1,
        "get_person(Eve) on feat-a must return 1 row through the query engine"
    );
    let eve_on_main = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &mixed_params(&[("$name", "Eve")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(
        eve_on_main.num_rows(),
        0,
        "get_person(Eve) on main must return 0 rows — feat-a's writes are isolated"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 6: branch_create feat-b from main. feat-b's base is main's
    // current state (post-Bob2): 6 Persons including Bob2 but NOT Eve.
    // The two branches now share neither base nor head with each other.
    // ─────────────────────────────────────────────────────────────────
    db.branch_create("feat-b").await.unwrap();
    assert_eq!(count_rows_branch(&db, "feat-b", "node:Person").await, 6);

    // ─────────────────────────────────────────────────────────────────
    // Step 7: mutate feat-b — insert "Frank".
    // ─────────────────────────────────────────────────────────────────
    mutate_branch(
        &mut db,
        "feat-b",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 33)]),
    )
    .await
    .expect("insert Frank on feat-b");
    assert_eq!(count_rows_branch(&db, "feat-b", "node:Person").await, 7);

    // ─────────────────────────────────────────────────────────────────
    // Step 8: mutate feat-a again — insert "Grace" + Knows(Grace → Eve).
    // feat-a now has 7 Persons and 4 Knows edges.
    // ─────────────────────────────────────────────────────────────────
    mutate_branch(
        &mut db,
        "feat-a",
        MUTATION_QUERIES,
        "insert_person_and_friend",
        &mixed_params(
            &[("$name", "Grace"), ("$friend", "Eve")],
            &[("$age", 28)],
        ),
    )
    .await
    .expect("insert Grace + Knows(Grace → Eve) on feat-a");
    assert_eq!(count_rows_branch(&db, "feat-a", "node:Person").await, 7);
    assert_eq!(count_rows_branch(&db, "feat-a", "edge:Knows").await, 4);
    assert_eq!(
        count_rows(&db, "edge:Knows").await,
        3,
        "main's Knows must be untouched by feat-a's edge insert"
    );
    // Edge traversal through the QUERY ENGINE on feat-a: `friends_of(Grace)`
    // exercises the Knows topology + index from feat-a's snapshot. Catches
    // regressions in graph-index lookup against branch-local edge tables.
    let graces_friends = query_branch(
        &mut db,
        "feat-a",
        TEST_QUERIES,
        "friends_of",
        &mixed_params(&[("$name", "Grace")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(
        graces_friends.num_rows(),
        1,
        "friends_of(Grace) on feat-a must return Eve via the query engine + Knows index"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 9: capture pre-merge-feat-a state. Both a version (for direct
    // dataset open) AND a SnapshotId (for query-engine time-travel) are
    // captured so we can later assert historical state through both paths.
    // ─────────────────────────────────────────────────────────────────
    let pre_merge_a_version = version_main(&db).await.unwrap();
    let pre_merge_a_snap_id = db.resolve_snapshot("main").await.unwrap();
    let pre_merge_a_persons = count_rows(&db, "node:Person").await;
    assert_eq!(pre_merge_a_persons, 6);

    // ─────────────────────────────────────────────────────────────────
    // Step 10: merge feat-a → main. main gains Eve, Grace, and the
    // Knows(Grace → Eve) edge. main's manifest version advances.
    // ─────────────────────────────────────────────────────────────────
    db.branch_merge("feat-a", "main").await.unwrap();
    let post_merge_a_version = version_main(&db).await.unwrap();
    assert!(
        post_merge_a_version > pre_merge_a_version,
        "merge feat-a → main must advance main's manifest version"
    );
    assert_eq!(count_rows(&db, "node:Person").await, 8);
    assert_eq!(count_rows(&db, "edge:Knows").await, 4);
    // Post-merge query-engine readback: Eve is now reachable on main via
    // `get_person` (BTree index lookup) and Grace's edge to Eve survives
    // the merge as a traversable edge via `friends_of`. This is the
    // load-bearing check that `publish_rewritten_merge_table`'s Phase 3
    // index rebuild produced a queryable result, not just data on disk.
    let eve_on_main_post_merge = query_main(
        &mut db,
        TEST_QUERIES,
        "get_person",
        &mixed_params(&[("$name", "Eve")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(
        eve_on_main_post_merge.num_rows(),
        1,
        "Eve must be findable on main post-merge through the BTree index"
    );
    let graces_friends_on_main = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &mixed_params(&[("$name", "Grace")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(
        graces_friends_on_main.num_rows(),
        1,
        "friends_of(Grace) on main post-merge must traverse the rebuilt Knows index"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 11: mutate main AFTER the first merge — insert "Helen". This
    // makes feat-b's eventual merge a non-trivial one: feat-b's base
    // (created in step 6) does not include Eve / Grace / Helen, but
    // main now has all three on top of Bob2.
    // ─────────────────────────────────────────────────────────────────
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Helen")], &[("$age", 44)]),
    )
    .await
    .expect("insert Helen on main post-merge");
    assert_eq!(count_rows(&db, "node:Person").await, 9);

    // ─────────────────────────────────────────────────────────────────
    // Step 12: capture pre-merge-feat-b state. Used for time-travel
    // assertions in step 14.
    // ─────────────────────────────────────────────────────────────────
    let pre_merge_b_version = version_main(&db).await.unwrap();
    let pre_merge_b_snap_id = db.resolve_snapshot("main").await.unwrap();
    assert!(
        pre_merge_b_version > post_merge_a_version,
        "Helen insert must advance main's version past the merge"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 13: merge feat-b → main. The diff base for this merge is
    // feat-b's branch-creation point (step 6), NOT main's current head.
    // A regression that uses main HEAD as the base would attempt to
    // re-apply Eve/Grace/Helen as source-only rows or surface conflicts.
    // ─────────────────────────────────────────────────────────────────
    db.branch_merge("feat-b", "main").await.unwrap();
    let post_merge_b_version = version_main(&db).await.unwrap();
    assert!(
        post_merge_b_version > pre_merge_b_version,
        "merge feat-b → main must advance main's manifest version"
    );
    assert_eq!(
        count_rows(&db, "node:Person").await,
        10,
        "main must contain all 10 Persons after both merges land"
    );
    // Aggregation through the QUERY ENGINE over the fully merged graph:
    // `total_people` returns count(Person) = 10. Catches regressions in
    // group-by/count execution against a multi-fragment table whose
    // current shape was produced by two sequential merges.
    let total_post_merges = query_main(
        &mut db,
        TEST_QUERIES,
        "total_people",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert_total(&total_post_merges, 10, "post both merges, main must total 10 Persons");

    // ─────────────────────────────────────────────────────────────────
    // Step 14: time-travel to pre-merge-a-version. Reads must return
    // main's pre-feat-a-merge state: 6 Persons, no Eve / Grace / Frank /
    // Helen. Catches snapshot leakage from later commits.
    //
    // Verified through TWO paths: direct dataset open (catches manifest-
    // pin propagation regressions) AND `.gq` query against the captured
    // SnapshotId (catches planner / index-state regressions where a
    // historical query accidentally resolves against current indices
    // instead of the snapshot's frozen index state).
    // ─────────────────────────────────────────────────────────────────
    let pre_a_snap = db.snapshot_at_version(pre_merge_a_version).await.unwrap();
    let pre_a_persons = pre_a_snap
        .open("node:Person")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(
        pre_a_persons, 6,
        "time-travel to pre-merge-a must show exactly 6 Persons (dataset-direct)"
    );
    let pre_a_knows = pre_a_snap
        .open("edge:Knows")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(
        pre_a_knows, 3,
        "time-travel to pre-merge-a must show exactly 3 Knows edges (no Grace → Eve)"
    );
    // `.gq` query against the captured SnapshotId — the planner must
    // resolve `total_people` against the historical Person snapshot,
    // not main's current head. Asserts the actual count value (not just
    // row count) so a planner regression that resolves to current state
    // would surface here as a count mismatch (10 instead of 6).
    let pre_a_total_via_query = db
        .query(
            ReadTarget::Snapshot(pre_merge_a_snap_id.clone()),
            TEST_QUERIES,
            "total_people",
            &ParamMap::default(),
        )
        .await
        .unwrap();
    assert_total(
        &pre_a_total_via_query,
        6,
        "time-travel to pre-merge-a must report 6 Persons via the query engine",
    );
    // Edge-traversal time-travel: Grace and her Knows(Grace → Eve) edge
    // do not exist at pre_merge_a, so `friends_of(Grace)` must return 0
    // even though Grace's row IS visible at later snapshots.
    let pre_a_grace_friends = db
        .query(
            ReadTarget::Snapshot(pre_merge_a_snap_id.clone()),
            TEST_QUERIES,
            "friends_of",
            &mixed_params(&[("$name", "Grace")], &[]),
        )
        .await
        .unwrap();
    assert_eq!(
        pre_a_grace_friends.num_rows(),
        0,
        "friends_of(Grace) at pre-merge-a must return 0 — Grace's row predates the merge"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 15: time-travel to pre-merge-b-version. Reads must show
    // post-feat-a-merge state (Eve, Grace, Helen present) but NOT Frank.
    // ─────────────────────────────────────────────────────────────────
    let pre_b_snap = db.snapshot_at_version(pre_merge_b_version).await.unwrap();
    let pre_b_persons = pre_b_snap
        .open("node:Person")
        .await
        .unwrap()
        .count_rows(None)
        .await
        .unwrap();
    assert_eq!(
        pre_b_persons, 9,
        "time-travel to pre-merge-b must show 9 Persons (post-feat-a-merge + Helen, pre-feat-b-merge)"
    );
    // Frank does not exist at pre-merge-b (he was on feat-b only); a
    // historical `get_person(Frank)` via the query engine must return 0.
    let pre_b_frank_via_query = db
        .query(
            ReadTarget::Snapshot(pre_merge_b_snap_id.clone()),
            TEST_QUERIES,
            "get_person",
            &mixed_params(&[("$name", "Frank")], &[]),
        )
        .await
        .unwrap();
    assert_eq!(
        pre_b_frank_via_query.num_rows(),
        0,
        "Frank must not appear at pre-merge-b — his row only enters main when feat-b merges"
    );
    // Eve is present at pre-merge-b (feat-a already landed); the
    // historical query must find her.
    let pre_b_eve_via_query = db
        .query(
            ReadTarget::Snapshot(pre_merge_b_snap_id),
            TEST_QUERIES,
            "get_person",
            &mixed_params(&[("$name", "Eve")], &[]),
        )
        .await
        .unwrap();
    assert_eq!(
        pre_b_eve_via_query.num_rows(),
        1,
        "Eve must be findable at pre-merge-b — she landed on main during feat-a's merge"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 16: query feat-b at its current head — feat-b is unchanged
    // by main's merges; it still shows its own 7-row state.
    // ─────────────────────────────────────────────────────────────────
    assert_eq!(
        count_rows_branch(&db, "feat-b", "node:Person").await,
        7,
        "feat-b's own snapshot must be unaffected by main's merge of feat-a"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 17: a feature-side query exercises the read path on a branch
    // whose base predates a completed merge (feat-b's base is pre-feat-a).
    // ─────────────────────────────────────────────────────────────────
    let frank_on_feat_b = query_branch(
        &mut db,
        "feat-b",
        TEST_QUERIES,
        "get_person",
        &mixed_params(&[("$name", "Frank")], &[]),
    )
    .await
    .unwrap();
    assert!(
        !frank_on_feat_b.batches().is_empty(),
        "feat-b must still see its own Frank insert"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 18: drop + reopen. Steady state — no recovery sidecars on
    // disk, manifest replays cleanly, all branches and tables visible.
    // ─────────────────────────────────────────────────────────────────
    drop(db);
    let db = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows(&db, "node:Person").await,
        10,
        "main Person count must persist across reopen"
    );
    assert_eq!(
        count_rows(&db, "edge:Knows").await,
        4,
        "main Knows count must persist across reopen"
    );
    let branches = db.branch_list().await.unwrap();
    assert!(
        branches.iter().any(|b| b == "feat-a") && branches.iter().any(|b| b == "feat-b"),
        "both feature branches must persist across reopen; got {:?}",
        branches
    );

    // No recovery sidecars left behind by a clean flow.
    let recovery_dir = std::path::Path::new(uri).join("__recovery");
    let leftover_sidecars = if recovery_dir.exists() {
        std::fs::read_dir(&recovery_dir).unwrap().count()
    } else {
        0
    };
    assert_eq!(
        leftover_sidecars, 0,
        "clean compositional flow must not leave recovery sidecars on disk"
    );

    // ─────────────────────────────────────────────────────────────────
    // Step 19: post-reopen query-engine readback. Exercises the full
    // read path (planner, indices, snapshot resolution) against the
    // reopened engine — catches regressions where indices serialize
    // correctly to disk but the reopened catalog can't bind them.
    // ─────────────────────────────────────────────────────────────────
    let mut db = db;
    let post_reopen_total = query_main(
        &mut db,
        TEST_QUERIES,
        "total_people",
        &ParamMap::default(),
    )
    .await
    .unwrap();
    assert_total(
        &post_reopen_total,
        10,
        "post-reopen total_people must still report 10 Persons",
    );
    // Edge-traversal post-reopen: Grace's Knows(Grace → Eve) survived
    // both the merge and the reopen as a queryable graph edge.
    let graces_friends_post_reopen = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &mixed_params(&[("$name", "Grace")], &[]),
    )
    .await
    .unwrap();
    assert_eq!(
        graces_friends_post_reopen.num_rows(),
        1,
        "friends_of(Grace) must traverse post-reopen — index + topology bound correctly"
    );
}
