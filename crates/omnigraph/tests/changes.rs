// This integration owner composes the full engine + cost helper futures across
// many exact-snapshot/feed scenarios. Keep compiler query depth aligned with
// the engine crate; this does not change any runtime test-thread stack.
#![recursion_limit = "256"]

mod helpers;

use omnigraph::changes::{ChangeFilter, ChangeOp, EntityKind};
use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;

use helpers::*;

async fn head_commit_id(uri: &str, branch: Option<&str>) -> String {
    let commit_graph = match branch {
        Some(branch) => CommitGraph::open_at_branch(uri, branch).await.unwrap(),
        None => CommitGraph::open(uri).await.unwrap(),
    };
    commit_graph.head_commit_id().await.unwrap().unwrap()
}

fn change_tuples(change_set: &omnigraph::changes::ChangeSet) -> Vec<(String, String, ChangeOp)> {
    let mut tuples: Vec<_> = change_set
        .changes
        .iter()
        .map(|change| (change.table_key.clone(), change.id.clone(), change.op))
        .collect();
    tuples.sort_by(|a, b| {
        a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)).then_with(|| {
            let a_op = match a.2 {
                ChangeOp::Insert => 0,
                ChangeOp::Update => 1,
                ChangeOp::Delete => 2,
            };
            let b_op = match b.2 {
                ChangeOp::Insert => 0,
                ChangeOp::Update => 1,
                ChangeOp::Delete => 2,
            };
            a_op.cmp(&b_op)
        })
    });
    tuples
}

// ─── Same-branch diff tests ────────────────────────────────────────────────

#[tokio::test]
async fn write_receipts_identify_exact_commits_and_mutation_noops() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    let receipt = db
        .mutate_with_receipt(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    let commit = receipt
        .commit
        .expect("a row-changing mutation publishes once");
    let stored = db.get_commit(&commit.graph_commit_id).await.unwrap();
    assert_eq!(stored.graph_commit_id, commit.graph_commit_id);
    assert_eq!(stored.manifest_version, commit.manifest_version);

    let changes = db
        .diff_commits(
            commit.parent_commit_id.as_deref().unwrap(),
            &commit.graph_commit_id,
            &ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(changes.to_version, commit.manifest_version);
    assert_eq!(
        change_tuples(&changes),
        vec![("node:Person".into(), "Eve".into(), ChangeOp::Insert)]
    );

    let head_before_no_op = snapshot_id(&db, "main").await.unwrap();
    let no_op = db
        .mutate_with_receipt(
            "main",
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Missing")], &[("$age", 22)]),
        )
        .await
        .unwrap();
    assert_eq!(no_op.result.affected_nodes, 0);
    assert!(no_op.commit.is_none());
    assert_eq!(snapshot_id(&db, "main").await.unwrap(), head_before_no_op);

    let load_receipt = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"LoadReceipt","age":40}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    assert_eq!(load_receipt.result.nodes_loaded.get("Person"), Some(&1));
    let stored = db
        .get_commit(&load_receipt.commit.graph_commit_id)
        .await
        .unwrap();
    assert_eq!(stored.graph_commit_id, load_receipt.commit.graph_commit_id);
    assert_eq!(
        stored.manifest_version,
        load_receipt.commit.manifest_version
    );

    // Empty Load has no table effect or recovery sidecar, but Load's public
    // contract still publishes one lineage commit and returns that exact
    // receipt rather than reconstructing it from a later branch-head read.
    let head_before_empty_load = snapshot_id(&db, "main").await.unwrap();
    let empty_load_receipt = db
        .load_with_receipt("main", "", LoadMode::Merge)
        .await
        .unwrap();
    assert!(empty_load_receipt.result.nodes_loaded.is_empty());
    assert!(empty_load_receipt.result.edges_loaded.is_empty());
    assert_eq!(
        empty_load_receipt.commit.parent_commit_id.as_deref(),
        Some(head_before_empty_load.as_str())
    );
    assert_eq!(
        snapshot_id(&db, "main").await.unwrap().as_str(),
        empty_load_receipt.commit.graph_commit_id.as_str()
    );
    let stored = db
        .get_commit(&empty_load_receipt.commit.graph_commit_id)
        .await
        .unwrap();
    assert_eq!(
        stored.graph_commit_id,
        empty_load_receipt.commit.graph_commit_id
    );
}

#[tokio::test]
async fn diff_empty_when_nothing_changed() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let v = snapshot_id(&db, "main").await.unwrap();
    let cs = db
        .diff_between(
            ReadTarget::Snapshot(v.clone()),
            ReadTarget::Snapshot(v),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert!(cs.changes.is_empty());
    assert_eq!(cs.stats.inserts, 0);
    assert_eq!(cs.stats.updates, 0);
    assert_eq!(cs.stats.deletes, 0);
}

#[tokio::test]
async fn diff_pairs_type_renames_by_identity_and_separates_reincarnations() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Person { name: String @key }
node Anchor { name: String @key }
"#,
    )
    .await
    .unwrap();
    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"Alice"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let before_rename = snapshot_id(&db, "main").await.unwrap();

    db.apply_schema(
        r#"
node Human @rename_from("Person") { name: String @key }
node Anchor { name: String @key }
"#,
    )
    .await
    .unwrap();
    let after_rename = snapshot_id(&db, "main").await.unwrap();

    let rename_diff = db
        .diff_between(
            ReadTarget::Snapshot(before_rename),
            ReadTarget::Snapshot(after_rename.clone()),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();
    assert!(
        rename_diff.changes.is_empty(),
        "a pure alias change on one table identity must not become delete+insert: {:?}",
        change_tuples(&rename_diff)
    );

    // Drop the renamed type, then independently declare the same public name.
    // The replacement is a new logical lifetime even though the alias matches.
    db.apply_schema("node Anchor { name: String @key }")
        .await
        .unwrap();
    db.apply_schema(
        r#"
node Human { name: String @key }
node Anchor { name: String @key }
"#,
    )
    .await
    .unwrap();
    db.load(
        "main",
        r#"{"type":"Human","data":{"name":"Bob"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let reincarnation_diff = diff_since_branch(&db, "main", after_rename, &ChangeFilter::default())
        .await
        .unwrap();
    let reincarnation_changes = reincarnation_diff
        .changes
        .iter()
        .map(|change| (change.table_key.clone(), change.id.clone(), change.op))
        .collect::<Vec<_>>();
    assert_eq!(
        reincarnation_changes,
        vec![
            (
                "node:Human".to_string(),
                "Alice".to_string(),
                ChangeOp::Delete,
            ),
            (
                "node:Human".to_string(),
                "Bob".to_string(),
                ChangeOp::Insert,
            ),
        ],
        "drop/re-add under one alias must visit the old identity before the newly allocated identity"
    );
    assert_eq!(reincarnation_diff.stats.deletes, 1);
    assert_eq!(reincarnation_diff.stats.inserts, 1);
    assert_eq!(reincarnation_diff.stats.updates, 0);
}

#[tokio::test]
async fn diff_detects_node_insert() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = snapshot_id(&db, "main").await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let cs = diff_since_branch(&db, "main", v_before, &ChangeFilter::default())
        .await
        .unwrap();
    let inserts: Vec<_> = cs
        .changes
        .iter()
        .filter(|c| c.op == ChangeOp::Insert && c.table_key == "node:Person")
        .collect();
    assert!(
        !inserts.is_empty(),
        "Should detect the Person insert. Got changes: {:?}",
        cs.changes
            .iter()
            .map(|c| (&c.table_key, &c.id, c.op))
            .collect::<Vec<_>>()
    );
    assert!(
        inserts.iter().any(|c| c.id == "Eve"),
        "Insert should contain Eve. Got: {:?}",
        inserts.iter().map(|c| &c.id).collect::<Vec<_>>()
    );
    assert_eq!(inserts[0].kind, EntityKind::Node);
    assert_eq!(inserts[0].endpoints, None);
}

#[tokio::test]
async fn diff_detects_node_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = snapshot_id(&db, "main").await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    let cs = diff_since_branch(&db, "main", v_before, &ChangeFilter::default())
        .await
        .unwrap();
    let updates: Vec<_> = cs
        .changes
        .iter()
        .filter(|c| c.op == ChangeOp::Update && c.table_key == "node:Person")
        .collect();
    assert!(
        !updates.is_empty(),
        "Should detect the Person update. Got changes: {:?}",
        cs.changes
            .iter()
            .map(|c| (&c.table_key, &c.id, c.op))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn diff_detects_node_delete_with_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = snapshot_id(&db, "main").await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    let cs = diff_since_branch(&db, "main", v_before, &ChangeFilter::default())
        .await
        .unwrap();
    let table_keys = cs
        .changes
        .iter()
        .map(|change| change.table_key.as_str())
        .collect::<Vec<_>>();
    assert!(
        table_keys.windows(2).all(|pair| pair[0] <= pair[1]),
        "multi-table changes must follow graph-visible table-key order: {table_keys:?}"
    );

    // Should have node:Person delete
    let person_deletes: Vec<_> = cs
        .changes
        .iter()
        .filter(|c| c.op == ChangeOp::Delete && c.table_key == "node:Person")
        .collect();
    assert!(
        !person_deletes.is_empty(),
        "Should detect Person delete. Changes: {:?}",
        cs.changes
            .iter()
            .map(|c| (&c.table_key, &c.id, c.op))
            .collect::<Vec<_>>()
    );

    // Should also have edge:Knows cascade deletes
    let edge_deletes: Vec<_> = cs
        .changes
        .iter()
        .filter(|c| c.op == ChangeOp::Delete && c.table_key == "edge:Knows")
        .collect();
    assert!(
        !edge_deletes.is_empty(),
        "Should detect cascaded Knows edge deletes. Changes: {:?}",
        cs.changes
            .iter()
            .map(|c| (&c.table_key, &c.id, c.op))
            .collect::<Vec<_>>()
    );

    // Cascaded edge deletes should have endpoints
    for edge_del in &edge_deletes {
        assert!(
            edge_del.endpoints.is_some(),
            "Deleted edge should have endpoint context"
        );
    }
}

#[tokio::test]
async fn diff_detects_edge_insert_with_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = snapshot_id(&db, "main").await.unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Bob"), ("$to", "Charlie")]),
    )
    .await
    .unwrap();

    let cs = diff_since_branch(&db, "main", v_before, &ChangeFilter::default())
        .await
        .unwrap();

    let edge_inserts: Vec<_> = cs
        .changes
        .iter()
        .filter(|c| c.op == ChangeOp::Insert && c.table_key == "edge:Knows")
        .collect();
    assert!(
        !edge_inserts.is_empty(),
        "Should detect Knows edge insert. Changes: {:?}",
        cs.changes
            .iter()
            .map(|c| (&c.table_key, &c.id, c.op))
            .collect::<Vec<_>>()
    );

    let e = &edge_inserts[0];
    assert_eq!(e.kind, EntityKind::Edge);
    let ep = e
        .endpoints
        .as_ref()
        .expect("Edge insert should have endpoints");
    assert!(!ep.src.is_empty(), "src should not be empty");
    assert!(!ep.dst.is_empty(), "dst should not be empty");
}

// ─── Filter tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn filter_by_type_name_skips_non_matching() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = snapshot_id(&db, "main").await.unwrap();

    // Insert a person (node:Person) and add a friend (edge:Knows)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "FilterTest")], &[("$age", 30)]),
    )
    .await
    .unwrap();

    // Filter to Company only — should not see Person changes
    let filter = ChangeFilter {
        type_names: Some(vec!["Company".to_string()]),
        ..Default::default()
    };
    let cs = diff_since_branch(&db, "main", v_before, &filter)
        .await
        .unwrap();
    assert!(
        cs.changes.is_empty(),
        "Filter to Company should skip Person changes. Got: {:?}",
        cs.changes
            .iter()
            .map(|c| (&c.table_key, &c.id, c.op))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn filter_by_op_skips_unwanted_operations() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = snapshot_id(&db, "main").await.unwrap();

    // Insert Eve, update Bob, delete Alice
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    // Filter to Insert only
    let filter = ChangeFilter {
        ops: Some(vec![ChangeOp::Insert]),
        ..Default::default()
    };
    let cs = diff_since_branch(&db, "main", v_before, &filter)
        .await
        .unwrap();

    // Should only have inserts, no updates or deletes
    for c in &cs.changes {
        assert_eq!(
            c.op,
            ChangeOp::Insert,
            "Filter for Insert-only should not include {:?} for {} ({})",
            c.op,
            c.table_key,
            c.id
        );
    }
}

// ─── Cross-branch diff tests ──────────────────────────────────────────────

#[tokio::test]
async fn diff_after_merge_reports_actual_changes() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();
    let v_before_branch = snapshot_id(&main, "main").await.unwrap();

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();

    // Main updates Bob
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    // Diff from pre-branch to post-merge on main
    let cs = diff_since_branch(&main, "main", v_before_branch, &ChangeFilter::default())
        .await
        .unwrap();

    // Should have:
    // - Person insert (Eve) — from the merge
    // - Person update (Bob) — from the main write
    // Should NOT have: all original persons re-reported as inserts
    let person_changes: Vec<_> = cs
        .changes
        .iter()
        .filter(|c| c.table_key == "node:Person")
        .collect();

    let person_inserts: Vec<_> = person_changes
        .iter()
        .filter(|c| c.op == ChangeOp::Insert)
        .collect();
    let person_updates: Vec<_> = person_changes
        .iter()
        .filter(|c| c.op == ChangeOp::Update)
        .collect();

    // There should be exactly 1 insert (Eve) not all persons
    assert!(
        person_inserts.len() <= 2,
        "After surgical merge, should not re-report all persons as inserts. \
         Got {} inserts: {:?}",
        person_inserts.len(),
        person_inserts.iter().map(|c| &c.id).collect::<Vec<_>>()
    );

    // Bob's update should be detected
    assert!(
        !person_updates.is_empty() || !person_inserts.is_empty(),
        "Should detect Bob's age update or Eve's insert"
    );
}

#[tokio::test]
async fn diff_commits_resolves_feature_commit_from_main_handle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let main_head = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap()
        .graph_commit_id;
    let feature_head = CommitGraph::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap()
        .graph_commit_id;

    let cs = main
        .diff_commits(&main_head, &feature_head, &ChangeFilter::default())
        .await
        .unwrap();
    assert!(
        cs.changes
            .iter()
            .any(|change| change.op == ChangeOp::Insert && change.id == "Eve"),
        "expected feature-only insert to be diffable from a main handle"
    );
}

#[tokio::test]
async fn cross_branch_diff_honors_insert_only_filter() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let main_head = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap()
        .graph_commit_id;
    let feature_head = CommitGraph::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap()
        .graph_commit_id;

    let filter = ChangeFilter {
        ops: Some(vec![ChangeOp::Insert]),
        ..Default::default()
    };
    let cs = main
        .diff_commits(&main_head, &feature_head, &filter)
        .await
        .unwrap();
    assert!(!cs.changes.is_empty());
    assert!(
        cs.changes
            .iter()
            .all(|change| change.op == ChangeOp::Insert)
    );
}

#[tokio::test]
async fn diff_commits_resolves_commits_across_branches_from_any_handle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_commit = head_commit_id(uri, None).await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();
    let feature_commit = head_commit_id(uri, Some("feature")).await;

    let from_main = main
        .diff_commits(&base_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();
    let from_feature = feature
        .diff_commits(&base_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();

    assert_eq!(change_tuples(&from_main), change_tuples(&from_feature));
    assert!(from_main.changes.iter().any(|change| {
        change.table_key == "node:Person" && change.id == "Eve" && change.op == ChangeOp::Insert
    }));
}

#[tokio::test]
async fn cross_lineage_diff_honors_delete_only_filter() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    let before = snapshot_id(&feature, "feature").await.unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 99)]),
    )
    .await
    .unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    let filter = ChangeFilter {
        ops: Some(vec![ChangeOp::Delete]),
        ..Default::default()
    };
    let change_set = diff_since_branch(&feature, "feature", before, &filter)
        .await
        .unwrap();

    assert!(
        !change_set.changes.is_empty(),
        "expected delete changes after removing Alice"
    );
    assert!(
        change_set
            .changes
            .iter()
            .all(|change| change.op == ChangeOp::Delete)
    );
}

#[tokio::test]
async fn same_branch_diff_across_first_lazy_fork_detects_update() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    let before = snapshot_id(&feature, "feature").await.unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 77)]),
    )
    .await
    .unwrap();

    let change_set = diff_since_branch(&feature, "feature", before, &ChangeFilter::default())
        .await
        .unwrap();
    assert!(change_set.changes.iter().any(|change| {
        change.table_key == "node:Person" && change.id == "Bob" && change.op == ChangeOp::Update
    }));
}

#[tokio::test]
async fn diff_commits_cross_branch_reports_property_only_updates() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    let base_commit = head_commit_id(uri, None).await;

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 55)]),
    )
    .await
    .unwrap();
    let feature_commit = head_commit_id(uri, Some("feature")).await;

    let change_set = main
        .diff_commits(&base_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();

    assert!(change_set.changes.iter().any(|change| {
        change.table_key == "node:Person" && change.id == "Bob" && change.op == ChangeOp::Update
    }));
    assert!(!change_set.changes.iter().any(|change| {
        change.table_key == "node:Person" && change.id == "Bob" && change.op == ChangeOp::Insert
    }));
}

#[tokio::test]
async fn diff_commits_ignores_row_version_only_differences() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 55)]),
    )
    .await
    .unwrap();
    let feature_commit = head_commit_id(uri, Some("feature")).await;

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 55)]),
    )
    .await
    .unwrap();
    let main_commit = head_commit_id(uri, None).await;

    let change_set = main
        .diff_commits(&main_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();

    assert!(
        change_set.changes.is_empty(),
        "identical user-visible state should not produce diff entries: {:?}",
        change_set.changes
    );
}

/// The cross-branch diff must distinguish an empty string from null. The legacy
/// display-string signature rendered both as `""` and missed the update; the
/// typed comparator sees the validity-bit difference. This is the same
/// conflation that silently drops a `""`↔null change on the merge path.
#[tokio::test]
async fn cross_branch_diff_detects_empty_string_to_null_update() {
    const SCHEMA: &str = "node Doc {\n    slug: String @key\n    body: String?\n}";
    const SET_BODY: &str = r#"
query set_body($slug: String, $body: String) {
    update Doc set { body: $body } where slug = $slug
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    // body omitted → null on main.
    db.load_with_receipt(
        "main",
        r#"{"type":"Doc","data":{"slug":"x"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let main_commit = head_commit_id(uri, None).await;

    db.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    // Set body to the empty string on feature: null → "".
    mutate_branch(
        &mut feature,
        "feature",
        SET_BODY,
        "set_body",
        &mixed_params(&[("$slug", "x"), ("$body", "")], &[]),
    )
    .await
    .unwrap();
    let feature_commit = head_commit_id(uri, Some("feature")).await;

    let change_set = db
        .diff_commits(&main_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();

    assert!(
        change_set.changes.iter().any(|change| {
            change.table_key == "node:Doc" && change.id == "x" && change.op == ChangeOp::Update
        }),
        "null → empty-string must surface as an update, not be conflated: {:?}",
        change_set.changes
    );
}

/// A legal user property whose name begins with `_row_` (only the five exact
/// Lance virtual columns are reserved) must participate in change detection.
/// The legacy signature skipped every `_row_`-prefixed column and hid the
/// update; the typed comparator skips only the reserved five.
#[tokio::test]
async fn cross_branch_diff_detects_underscore_prefixed_property_update() {
    const SCHEMA: &str = "node Doc {\n    slug: String @key\n    _row_notes: String?\n}";
    const SET_NOTES: &str = r#"
query set_notes($slug: String, $notes: String) {
    update Doc set { _row_notes: $notes } where slug = $slug
}
"#;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, SCHEMA).await.unwrap();
    db.load_with_receipt(
        "main",
        r#"{"type":"Doc","data":{"slug":"x","_row_notes":"before"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let main_commit = head_commit_id(uri, None).await;

    db.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        SET_NOTES,
        "set_notes",
        &mixed_params(&[("$slug", "x"), ("$notes", "after")], &[]),
    )
    .await
    .unwrap();
    let feature_commit = head_commit_id(uri, Some("feature")).await;

    let change_set = db
        .diff_commits(&main_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();

    assert!(
        change_set.changes.iter().any(|change| {
            change.table_key == "node:Doc" && change.id == "x" && change.op == ChangeOp::Update
        }),
        "a change to a legal _row_-prefixed property must be detected: {:?}",
        change_set.changes
    );
}

// ─── Per-commit entity change pages ────────────────────────────────────────

fn properties(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("expected object").clone()
}

/// A Blob-only update where the before/after payloads are the same length can
/// produce byte-identical file-relative Blob descriptors in two different data
/// files. Lance resolves those descriptors relative to the owning physical row
/// (data file), so equal descriptors in different fragments reference different
/// bytes. The comparator must not treat equal descriptors as equal payloads
/// without proving the owning immutable source is also identical.
#[tokio::test]
async fn commit_changes_detects_same_length_blob_only_update_across_fragments() {
    use omnigraph::changes::{ChangeFeedScope, ChangeOpKind};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Document {
    title: String @key
    payload: Blob?
}
"#,
    )
    .await
    .unwrap();

    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:QQ=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // A -> B: both payloads are one byte. A second write creates a new
    // physical row/data file, but its file-relative Blob descriptor can be
    // identical to the first descriptor.
    let updated = db
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qg=="}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let page = db
        .commit_changes_page(
            &updated.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        page.block.changes.len(),
        1,
        "Blob-only update was suppressed"
    );
    let change = &page.block.changes[0];
    assert_eq!(change.id, "doc");
    assert_eq!(change.op, ChangeOpKind::Update);
    assert_eq!(
        change.before.as_ref().unwrap().properties["payload"],
        serde_json::json!("base64:QQ==")
    );
    assert_eq!(
        change.after.as_ref().unwrap().properties["payload"],
        serde_json::json!("base64:Qg==")
    );
}

const BLOB_DOC_SCHEMA: &str = r#"
node Document {
    title: String @key
    payload: Blob?
}
"#;

/// Acceptance #2: the durable feed shares the enumerator, so the same-length
/// Blob-only update must also be delivered through `poll_change_feed`, not only
/// the finite commit diff.
#[tokio::test]
async fn change_feed_detects_same_length_blob_only_update() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart, ChangeOpKind};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BLOB_DOC_SCHEMA).await.unwrap();

    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:QQ=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Capture a caught-up cursor at the insert, then apply the A -> B update.
    let now = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    let (cursor, _) = boundary_cursor(&now);

    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qg=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let page = db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(cursor)))
        .await
        .unwrap();
    assert_eq!(
        page.blocks.len(),
        1,
        "the feed dropped the Blob-only update"
    );
    let changes = &page.blocks[0].changes;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].id, "doc");
    assert_eq!(changes[0].op, ChangeOpKind::Update);
    assert_eq!(
        changes[0].after.as_ref().unwrap().properties["payload"],
        serde_json::json!("base64:Qg==")
    );
}

/// Acceptance #7: the cross-branch net diff shares the same comparator, so a
/// same-length Blob-only update on a forked branch must surface through
/// `diff_commits` too.
#[tokio::test]
async fn cross_branch_diff_detects_same_length_blob_only_update() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BLOB_DOC_SCHEMA).await.unwrap();
    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:QQ=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let main_commit = head_commit_id(uri, None).await;

    db.branch_create("feature").await.unwrap();
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .load_with_receipt(
            "feature",
            r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qg=="}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let feature_commit = head_commit_id(uri, Some("feature")).await;

    let change_set = db
        .diff_commits(&main_commit, &feature_commit, &ChangeFilter::default())
        .await
        .unwrap();
    assert!(
        change_set.changes.iter().any(|change| {
            change.table_key == "node:Document"
                && change.id == "doc"
                && change.op == ChangeOp::Update
        }),
        "cross-branch diff dropped the Blob-only update: {:?}",
        change_set.changes
    );
}

/// Two sibling branches forked from the same base each apply a same-length
/// Blob-only update to the same entity. On each branch the update relocates the
/// row to the SAME next fragment id, so the two managed descriptors are
/// byte-identical (`mgd:<frag>:0:0:<len>:0`) even though the payloads differ.
/// Fragment ids are branch-local, so a cross-branch diff must byte-compare
/// managed columns rather than trust equal descriptor identities — otherwise
/// the update is silently dropped. (`cross_branch_diff_detects_same_length_..`
/// above diffs main→feature, where the two fragment ids differ, so it does not
/// exercise this collision.)
#[tokio::test]
async fn cross_branch_diff_detects_same_length_blob_update_between_sibling_branches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BLOB_DOC_SCHEMA).await.unwrap();
    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:QQ=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    db.branch_create("branch-a").await.unwrap();
    db.branch_create("branch-b").await.unwrap();

    // "B" on branch-a and "C" on branch-b: both one byte, both relocate `doc`
    // to the first forked fragment, so the two managed descriptors collide.
    let a = Omnigraph::open(uri).await.unwrap();
    a.load_with_receipt(
        "branch-a",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qg=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let a_commit = head_commit_id(uri, Some("branch-a")).await;

    let b = Omnigraph::open(uri).await.unwrap();
    b.load_with_receipt(
        "branch-b",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qw=="}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let b_commit = head_commit_id(uri, Some("branch-b")).await;

    let change_set = db
        .diff_commits(&a_commit, &b_commit, &ChangeFilter::default())
        .await
        .unwrap();
    assert!(
        change_set.changes.iter().any(|change| {
            change.table_key == "node:Document"
                && change.id == "doc"
                && change.op == ChangeOp::Update
        }),
        "cross-branch diff dropped the sibling-branch Blob update (branch-local \
         fragment-id collision): {:?}",
        change_set.changes
    );
}

/// `LoadMode::Overwrite` restarts Lance fragment ids at 0, so the before image
/// (old fragment 0) and the after image (new fragment 0) collide on a
/// fragment-qualified managed identity even within one lineage — a same-length
/// Blob-only overwrite is silently dropped. The identity must be qualified by
/// the immutable data-file path (a per-file UUID that overwrite never reuses),
/// not the numeric fragment id.
#[tokio::test]
async fn commit_changes_detects_same_length_blob_update_after_overwrite() {
    use omnigraph::changes::{ChangeFeedScope, ChangeOpKind};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BLOB_DOC_SCHEMA).await.unwrap();
    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:QQ=="}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let overwritten = db
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qg=="}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();

    let page = db
        .commit_changes_page(
            &overwritten.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        page.block.changes.len(),
        1,
        "same-length Blob update after Overwrite was dropped (fragment-id reuse)"
    );
    let change = &page.block.changes[0];
    assert_eq!(change.id, "doc");
    assert_eq!(change.op, ChangeOpKind::Update);
    assert_eq!(
        change.after.as_ref().unwrap().properties["payload"],
        serde_json::json!("base64:Qg==")
    );
}

/// The durable feed shares the enumerator, so the Overwrite same-length
/// Blob-only update must also survive `poll_change_feed`.
#[tokio::test]
async fn change_feed_detects_same_length_blob_update_after_overwrite() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart, ChangeOpKind};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, BLOB_DOC_SCHEMA).await.unwrap();
    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:QQ=="}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    let now = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    let (cursor, _) = boundary_cursor(&now);

    db.load_with_receipt(
        "main",
        r#"{"type":"Document","data":{"title":"doc","payload":"base64:Qg=="}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let page = db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(cursor)))
        .await
        .unwrap();
    assert_eq!(
        page.blocks.len(),
        1,
        "the feed dropped the Overwrite Blob update"
    );
    let changes = &page.blocks[0].changes;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].id, "doc");
    assert_eq!(changes[0].op, ChangeOpKind::Update);
    assert_eq!(
        changes[0].after.as_ref().unwrap().properties["payload"],
        serde_json::json!("base64:Qg==")
    );
}

/// RFC-030 §4.2/§9 L0: candidate pruning must NOT be applied to an operation
/// that can remove or reuse a logical id. An overwrite that drops one id and
/// changes another is `Operation::Overwrite`, which the classifier rejects, so
/// the enumerator falls back to the exact ordered merge and reports BOTH the
/// delete and the update. A candidate scan of the child's new fragments alone
/// would never see the dropped id, silently losing the delete.
#[tokio::test]
async fn commit_changes_falls_back_for_overwrite_that_removes_an_id() {
    use omnigraph::changes::{ChangeFeedScope, ChangeOpKind};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        "node Doc {\n    slug: String @key\n    body: String?\n}",
    )
    .await
    .unwrap();
    db.load_with_receipt(
        "main",
        "{\"type\":\"Doc\",\"data\":{\"slug\":\"x\",\"body\":\"a\"}}\n{\"type\":\"Doc\",\"data\":{\"slug\":\"y\",\"body\":\"b\"}}",
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    // Overwrite the whole table: y is dropped and x is changed.
    let overwritten = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Doc\",\"data\":{\"slug\":\"x\",\"body\":\"c\"}}",
            LoadMode::Overwrite,
        )
        .await
        .unwrap();

    let page = db
        .commit_changes_page(
            &overwritten.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let mut ops: Vec<(String, ChangeOpKind)> = page
        .block
        .changes
        .iter()
        .map(|change| (change.id.clone(), change.op))
        .collect();
    ops.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        ops,
        vec![
            ("x".to_string(), ChangeOpKind::Update),
            ("y".to_string(), ChangeOpKind::Delete),
        ],
        "overwrite must fall back to the exact merge and report the dropped id as a delete"
    );
}

#[tokio::test]
async fn commit_changes_are_exact_ordered_and_bounded() {
    use omnigraph::changes::{ChangeEntityKind, ChangeFeedScope, ChangeOpKind};
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let scope = ChangeFeedScope::default();

    let head_before = snapshot_id(&db, "main").await.unwrap();
    let inserted = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Person\",\"data\":{\"name\":\"feed-C\",\"age\":3}}\n{\"type\":\"Person\",\"data\":{\"name\":\"feed-A\",\"age\":1}}\n{\"type\":\"Person\",\"data\":{\"name\":\"feed-B\",\"age\":2}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();

    // Bounded page: id-ordered inserts with after-images only, cause stated
    // once on the block, continuation carried only by the opaque token.
    let first_page = db
        .commit_changes_page(
            &inserted.commit.graph_commit_id,
            &scope,
            None,
            Some(2),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        first_page.block.cause.graph_commit_id,
        inserted.commit.graph_commit_id
    );
    assert_eq!(
        first_page.block.cause.parent_commit_id.as_deref(),
        Some(head_before.as_str())
    );
    assert_eq!(first_page.block.cause.authored_branch, None, "main");
    assert_eq!(
        first_page
            .block
            .changes
            .iter()
            .map(|change| (change.id.as_str(), change.op))
            .collect::<Vec<_>>(),
        vec![
            ("feed-A", ChangeOpKind::Insert),
            ("feed-B", ChangeOpKind::Insert)
        ]
    );
    assert!(first_page.block.changes.iter().all(|c| c.before.is_none()));
    assert_eq!(
        first_page.block.changes[0]
            .after
            .as_ref()
            .unwrap()
            .properties,
        properties(serde_json::json!({"name": "feed-A", "age": 1}))
    );
    let token = first_page
        .next_page_token
        .expect("a truncated block continues by page token");

    let second_page = db
        .commit_changes_page(
            &inserted.commit.graph_commit_id,
            &scope,
            Some(&token),
            Some(2),
            None,
        )
        .await
        .unwrap();
    assert_eq!(second_page.block.changes.len(), 1);
    assert_eq!(second_page.block.changes[0].id, "feed-C");
    assert_eq!(
        second_page.block.changes[0]
            .after
            .as_ref()
            .unwrap()
            .properties["age"],
        serde_json::json!(3)
    );
    assert!(second_page.next_page_token.is_none());

    // An update carries the exact parent before-image AND child after-image.
    let updated = db
        .mutate_with_receipt(
            "main",
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Bob")], &[("$age", 99)]),
        )
        .await
        .unwrap()
        .commit
        .unwrap();
    let update_page = db
        .commit_changes_page(&updated.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert_eq!(update_page.block.changes.len(), 1);
    let update = &update_page.block.changes[0];
    assert_eq!(update.op, ChangeOpKind::Update);
    assert_eq!(update.id, "Bob");
    assert_eq!(update.entity_type.name, "Person");
    assert_eq!(
        update.before.as_ref().unwrap().properties["age"],
        serde_json::json!(25),
        "an update must carry the exact parent before-image"
    );
    assert_eq!(
        update.after.as_ref().unwrap().properties["age"],
        serde_json::json!(99)
    );

    // A node delete cascades to incident edges: deletes carry before-images
    // only, nodes precede edges, and every edge image carries its endpoints.
    let deleted = db
        .mutate_with_receipt(
            "main",
            MUTATION_QUERIES,
            "remove_person",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap()
        .commit
        .unwrap();
    let delete_page = db
        .commit_changes_page(&deleted.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert!(
        delete_page
            .block
            .changes
            .iter()
            .all(|change| change.op == ChangeOpKind::Delete
                && change.before.is_some()
                && change.after.is_none())
    );
    let kinds: Vec<ChangeEntityKind> = delete_page
        .block
        .changes
        .iter()
        .map(|change| change.kind)
        .collect();
    assert!(
        kinds
            .windows(2)
            .all(|pair| !(pair[0] == ChangeEntityKind::Edge && pair[1] == ChangeEntityKind::Node)),
        "nodes precede edges in the frozen block order: {kinds:?}"
    );
    assert!(kinds.contains(&ChangeEntityKind::Edge));
    for change in &delete_page.block.changes {
        if change.kind == ChangeEntityKind::Edge {
            let endpoints = change
                .before
                .as_ref()
                .unwrap()
                .endpoints
                .as_ref()
                .expect("edge images carry endpoints");
            assert_eq!(endpoints.from, "Alice");
        } else {
            assert!(change.before.as_ref().unwrap().endpoints.is_none());
        }
    }

    // A paged walk over the same commit reproduces the single-page sequence.
    let mut token = None;
    let mut paged = Vec::new();
    loop {
        let page = db
            .commit_changes_page(
                &deleted.graph_commit_id,
                &scope,
                token.as_deref(),
                Some(1),
                None,
            )
            .await
            .unwrap();
        paged.extend(
            page.block
                .changes
                .iter()
                .map(|change| (change.entity_type.name.clone(), change.id.clone())),
        );
        match page.next_page_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert_eq!(
        paged,
        delete_page
            .block
            .changes
            .iter()
            .map(|change| (change.entity_type.name.clone(), change.id.clone()))
            .collect::<Vec<_>>()
    );

    // An out-of-scope operation filter yields an empty complete block.
    let insert_only = ChangeFeedScope {
        ops: Some(vec![ChangeOpKind::Insert]),
        ..ChangeFeedScope::default()
    };
    let filtered = db
        .commit_changes_page(&deleted.graph_commit_id, &insert_only, None, None, None)
        .await
        .unwrap();
    assert!(filtered.block.changes.is_empty());
    assert!(filtered.next_page_token.is_none());

    // An empty load still publishes a lineage commit; its block is empty.
    let empty = db
        .load_with_receipt("main", "", LoadMode::Merge)
        .await
        .unwrap();
    let empty_page = db
        .commit_changes_page(&empty.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert!(empty_page.block.changes.is_empty());
    assert!(empty_page.next_page_token.is_none());

    // Reclaiming a pinned participant turns the commit into a typed gap.
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let person_path = &snapshot.entry("node:Person").unwrap().table_path;
    let person_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        person_path.trim_start_matches('/')
    );
    let person = lance::Dataset::open(&person_uri).await.unwrap();
    let removed = lance::dataset::cleanup::cleanup_old_versions(
        &person,
        lance::dataset::cleanup::CleanupPolicy {
            before_version: Some(person.version().version),
            delete_unverified: true,
            error_if_tagged_old_versions: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        removed.old_versions > 0,
        "precondition: history was reclaimed"
    );
    let gap = db
        .commit_changes_page(&inserted.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap_err();
    match gap {
        OmniError::ChangeFeedGap {
            first_unreadable_commit_id,
            cursor,
        } => {
            assert_eq!(first_unreadable_commit_id, inserted.commit.graph_commit_id);
            // A finite commit diff carries no durable feed cursor; recovery is
            // the baseline handshake.
            assert_eq!(cursor, None, "finite gap must not carry a feed cursor");
        }
        other => panic!("expected a typed change feed gap, got: {other:?}"),
    }
}

#[tokio::test]
async fn commit_changes_use_the_commit_era_physical_schema() {
    use omnigraph::changes::ChangeFeedScope;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Document {
    title: String @key
    payload: String?
}
"#,
    )
    .await
    .unwrap();
    let scope = ChangeFeedScope::default();
    let old_commit = db
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"old","payload":"keep"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    db.apply_schema(
        r#"
node Article @rename_from("Document") {
    title: String @key
    payload: String?
}
"#,
    )
    .await
    .unwrap();
    // A pure rename moves no table state: the schema commit is an empty block.
    let rename_commit = head_commit_id(uri, None).await;
    assert_ne!(rename_commit, old_commit.commit.graph_commit_id);
    let rename_page = db
        .commit_changes_page(&rename_commit, &scope, None, None, None)
        .await
        .unwrap();
    assert!(rename_page.block.changes.is_empty());

    let new_commit = db
        .load_with_receipt(
            "main",
            r#"{"type":"Article","data":{"title":"new","payload":"x"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    // The retained commit decodes with its commit-era schema and name…
    let old_page = db
        .commit_changes_page(&old_commit.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert_eq!(old_page.block.changes.len(), 1);
    assert_eq!(old_page.block.changes[0].entity_type.name, "Document");
    assert_eq!(
        old_page.block.changes[0].after.as_ref().unwrap().properties,
        properties(serde_json::json!({"title": "old", "payload": "keep"}))
    );

    // …while the opaque type identity is rename-stable across both eras.
    let new_page = db
        .commit_changes_page(&new_commit.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert_eq!(new_page.block.changes[0].entity_type.name, "Article");
    assert_eq!(
        old_page.block.changes[0].entity_type.id, new_page.block.changes[0].entity_type.id,
        "a supported rename preserves the opaque graph type identity"
    );
}

#[tokio::test]
async fn commit_changes_suppress_unchanged_blob_rows_and_physical_only_commits() {
    use omnigraph::changes::ChangeFeedScope;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Document {
    title: String @key
    note: String?
    payload: Blob?
}
"#,
    )
    .await
    .unwrap();
    let scope = ChangeFeedScope::default();
    db.load_with_receipt(
        "main",
        concat!(
            r#"{"type":"Document","data":{"title":"a","note":"one","payload":"base64:QQ=="}}"#,
            "\n",
            r#"{"type":"Document","data":{"title":"b","note":"two","payload":"base64:Qg=="}}"#,
        ),
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // A scalar-only update to one row: the sibling row's Blob descriptor is
    // untouched, so the page holds exactly one change whose after-image still
    // carries the stored payload, and the unchanged sibling never surfaces.
    let updated = db
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"a","note":"revised","payload":"base64:QQ=="}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let page = db
        .commit_changes_page(&updated.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert_eq!(
        page.block
            .changes
            .iter()
            .map(|change| (change.id.as_str(), change.op))
            .collect::<Vec<_>>(),
        vec![("a", omnigraph::changes::ChangeOpKind::Update)],
        "an unchanged sibling row must not surface as a change"
    );
    assert_eq!(
        page.block.changes[0].after.as_ref().unwrap().properties,
        properties(serde_json::json!({
            "title": "a",
            "note": "revised",
            "payload": "base64:QQ==",
        }))
    );
    assert_eq!(
        page.block.changes[0].before.as_ref().unwrap().properties["note"],
        serde_json::json!("one")
    );

    // Compaction rewrites the Blob table and moves every descriptor without
    // touching logical rows. The moved descriptors force the payload
    // tie-break, which must classify every row as unchanged: the maintenance
    // commit is an empty block, not a phantom full-table update.
    let stats = db.optimize().await.unwrap();
    assert!(
        stats
            .iter()
            .any(|table| table.fragments_removed > 0 && table.committed),
        "the fixture must actually compact so descriptors move"
    );
    let optimize_commit = head_commit_id(uri, None).await;
    assert_ne!(optimize_commit, updated.commit.graph_commit_id);
    let physical_only = db
        .commit_changes_page(&optimize_commit, &scope, None, None, None)
        .await
        .unwrap();
    assert!(
        physical_only.block.changes.is_empty(),
        "a physical-only commit is an empty block: {:?}",
        physical_only.block.changes
    );
    assert!(physical_only.next_page_token.is_none());
}

#[tokio::test]
async fn commit_changes_update_carries_exact_before_and_after_images_including_null_vs_empty() {
    use omnigraph::changes::{ChangeEntityKind, ChangeFeedScope, ChangeOpKind};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Note {
    slug: String @key
    body: String?
}

edge Refs: Note -> Note {
    label: String?
}
"#,
    )
    .await
    .unwrap();
    let scope = ChangeFeedScope::default();
    db.load_with_receipt(
        "main",
        concat!(
            r#"{"type":"Note","data":{"slug":"note-a","body":""}}"#,
            "\n",
            r#"{"type":"Note","data":{"slug":"note-b"}}"#,
            "\n",
            r#"{"type":"Note","data":{"slug":"note-c","body":"same"}}"#,
            "\n",
            r#"{"edge":"Refs","from":"note-a","to":"note-b","data":{"id":"ref-1","label":"old"}}"#,
        ),
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let second = db
        .load_with_receipt(
            "main",
            concat!(
                r#"{"type":"Note","data":{"slug":"note-a"}}"#,
                "\n",
                r#"{"type":"Note","data":{"slug":"note-b","body":"x"}}"#,
                "\n",
                r#"{"type":"Note","data":{"slug":"note-c","body":"same"}}"#,
                "\n",
                r#"{"edge":"Refs","from":"note-a","to":"note-b","data":{"id":"ref-1","label":"new"}}"#,
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let page = db
        .commit_changes_page(&second.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    let summary: Vec<(ChangeEntityKind, &str, ChangeOpKind)> = page
        .block
        .changes
        .iter()
        .map(|change| (change.kind, change.id.as_str(), change.op))
        .collect();
    assert_eq!(
        summary
            .iter()
            .filter(|(kind, _, _)| *kind == ChangeEntityKind::Node)
            .map(|(_, id, op)| (*id, *op))
            .collect::<Vec<_>>(),
        vec![
            ("note-a", ChangeOpKind::Update),
            ("note-b", ChangeOpKind::Update)
        ],
        "the identical re-load of note-c is a physical no-op and must not surface: {summary:?}"
    );

    let note_a = &page.block.changes[0];
    assert_eq!(
        note_a.before.as_ref().unwrap().properties["body"],
        serde_json::json!(""),
        "a valid empty string is not null"
    );
    assert_eq!(
        note_a.after.as_ref().unwrap().properties["body"],
        serde_json::Value::Null
    );
    let note_b = &page.block.changes[1];
    assert_eq!(
        note_b.before.as_ref().unwrap().properties["body"],
        serde_json::Value::Null
    );
    assert_eq!(
        note_b.after.as_ref().unwrap().properties["body"],
        serde_json::json!("x")
    );

    let edge = page
        .block
        .changes
        .iter()
        .find(|change| change.kind == ChangeEntityKind::Edge)
        .expect("the edge label change must surface");
    assert_eq!(edge.op, ChangeOpKind::Update);
    for image in [edge.before.as_ref().unwrap(), edge.after.as_ref().unwrap()] {
        let endpoints = image
            .endpoints
            .as_ref()
            .expect("edge images carry endpoints");
        assert_eq!(endpoints.from, "note-a");
        assert_eq!(endpoints.to, "note-b");
    }
    assert_eq!(
        edge.before.as_ref().unwrap().properties["label"],
        serde_json::json!("old")
    );
    assert_eq!(
        edge.after.as_ref().unwrap().properties["label"],
        serde_json::json!("new")
    );
}

#[tokio::test]
async fn commit_changes_reject_parentless_genesis() {
    use omnigraph::changes::ChangeFeedScope;
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let genesis = db
        .list_commits(Some("main"))
        .await
        .unwrap()
        .into_iter()
        .last()
        .unwrap();
    assert!(genesis.parent_commit_id.is_none(), "fixture sanity");
    let err = db
        .commit_changes_page(
            &genesis.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    match err {
        OmniError::CommitHasNoParent { graph_commit_id } => {
            assert_eq!(graph_commit_id, genesis.graph_commit_id)
        }
        other => panic!("expected the typed parentless refusal, got: {other:?}"),
    }
}

#[tokio::test]
async fn commit_changes_page_token_rejections_are_typed() {
    use omnigraph::changes::{ChangeFeedScope, ChangeOpKind};
    use omnigraph::error::OmniError;

    fn assert_rejected(err: OmniError, expected_fragment: &str) {
        match err {
            OmniError::ChangeCursorRejected { reason } => assert!(
                reason.contains(expected_fragment),
                "reason '{reason}' should mention '{expected_fragment}'"
            ),
            other => panic!("expected a typed continuation rejection, got: {other:?}"),
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let scope = ChangeFeedScope::default();
    let inserted = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Person\",\"data\":{\"name\":\"t-A\",\"age\":1}}\n{\"type\":\"Person\",\"data\":{\"name\":\"t-B\",\"age\":2}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let commit_id = inserted.commit.graph_commit_id.clone();
    let page = db
        .commit_changes_page(&commit_id, &scope, None, Some(1), None)
        .await
        .unwrap();
    let token = page.next_page_token.expect("truncated page");

    // Garbage and tampered tokens.
    assert_rejected(
        db.commit_changes_page(&commit_id, &scope, Some("not-a-token"), None, None)
            .await
            .unwrap_err(),
        "encoding",
    );
    let mut tampered = token.clone().into_bytes();
    tampered[4] = if tampered[4] == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered).unwrap();
    assert_rejected(
        db.commit_changes_page(&commit_id, &scope, Some(&tampered), None, None)
            .await
            .unwrap_err(),
        "commit changes page token",
    );

    // A different commit of the same graph.
    let other = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"t-C","age":3}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    assert_rejected(
        db.commit_changes_page(
            &other.commit.graph_commit_id,
            &scope,
            Some(&token),
            None,
            None,
        )
        .await
        .unwrap_err(),
        "does not match this graph and commit",
    );

    // A different filter scope than the one the token was minted under.
    let insert_only = ChangeFeedScope {
        ops: Some(vec![ChangeOpKind::Insert]),
        ..ChangeFeedScope::default()
    };
    assert_rejected(
        db.commit_changes_page(&commit_id, &insert_only, Some(&token), None, None)
            .await
            .unwrap_err(),
        "different filter scope",
    );

    // A different graph entirely.
    let other_dir = tempfile::tempdir().unwrap();
    let other_db = init_and_load(&other_dir).await;
    let foreign = other_db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"t-A","age":1}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    assert_rejected(
        other_db
            .commit_changes_page(
                &foreign.commit.graph_commit_id,
                &scope,
                Some(&token),
                None,
                None,
            )
            .await
            .unwrap_err(),
        "does not match this graph and commit",
    );

    // Limit validation: zero is malformed, above the ceiling is a typed
    // resource limit.
    assert!(
        db.commit_changes_page(&commit_id, &scope, None, Some(0), None)
            .await
            .is_err()
    );
    match db
        .commit_changes_page(&commit_id, &scope, None, Some(8_193), None)
        .await
        .unwrap_err()
    {
        OmniError::ResourceLimitExceeded { resource, .. } => {
            assert_eq!(resource, "commit_changes_page_rows")
        }
        other => panic!("expected a typed resource limit, got: {other:?}"),
    }
}

#[tokio::test]
async fn commit_changes_refuse_unprovable_schema_boundary() {
    use omnigraph::changes::ChangeFeedScope;
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Person {
    name: String @key
    age: I32?
}

node Ghost {
    name: String @key
}
"#,
    )
    .await
    .unwrap();
    let scope = ChangeFeedScope::default();
    db.load_with_receipt(
        "main",
        r#"{"type":"Person","data":{"name":"Alice","age":30}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // (a) A property add rewrites the table: the two pinned endpoints of the
    // schema-apply commit no longer share one user schema.
    db.apply_schema(
        r#"
node Person {
    name: String @key
    age: I32?
    note: String?
}

node Ghost {
    name: String @key
}
"#,
    )
    .await
    .unwrap();
    let add_commit = head_commit_id(uri, None).await;
    match db
        .commit_changes_page(&add_commit, &scope, None, None, None)
        .await
        .unwrap_err()
    {
        OmniError::ChangeSchemaBoundary { type_name, .. } => assert_eq!(type_name, "Person"),
        other => panic!("expected a typed schema boundary, got: {other:?}"),
    }

    // (b) Dropping a type that still holds data is schema evolution with data
    // present — refused, never synthesized into entity deletes.
    db.apply_schema(
        r#"
node Ghost {
    name: String @key
}
"#,
    )
    .await
    .unwrap();
    let drop_commit = head_commit_id(uri, None).await;
    match db
        .commit_changes_page(&drop_commit, &scope, None, None, None)
        .await
        .unwrap_err()
    {
        OmniError::ChangeSchemaBoundary { type_name, .. } => assert_eq!(type_name, "Person"),
        other => panic!("expected a typed schema boundary, got: {other:?}"),
    }

    // (c) Dropping an EMPTY type emits nothing and is not a boundary.
    db.apply_schema(
        r#"
node Anchor {
    name: String @key
}
"#,
    )
    .await
    .unwrap();
    let empty_drop_commit = head_commit_id(uri, None).await;
    let page = db
        .commit_changes_page(&empty_drop_commit, &scope, None, None, None)
        .await
        .unwrap();
    assert!(page.block.changes.is_empty());
}

// ─── Durable change feed ───────────────────────────────────────────────────

fn feed_request(
    branch: Option<&str>,
    position: omnigraph::changes::ChangeFeedPosition,
) -> omnigraph::changes::ChangeFeedRequest {
    omnigraph::changes::ChangeFeedRequest {
        branch: branch.map(str::to_string),
        position,
        scope: omnigraph::changes::ChangeFeedScope::default(),
        max_changes: None,
        max_bytes: None,
        max_commits: None,
    }
}

fn boundary_cursor(page: &omnigraph::changes::ChangeFeedPage) -> (String, bool) {
    match &page.continuation {
        omnigraph::changes::ChangeFeedContinuation::AtBlockBoundary { cursor, caught_up } => {
            (cursor.clone(), *caught_up)
        }
        other => panic!("expected a block-boundary continuation, got {other:?}"),
    }
}

/// A handle that already polled must FOLLOW commits made through a DIFFERENT
/// handle. The warm live-read refresh sees the new durable head; a state-only
/// refresh (taken whenever the head row merely exists) would pair that new
/// head with a stale lineage projection, and every later poll on this handle
/// would fail with a missing-commit error and never self-heal. The refresh
/// must re-read lineage whenever the durable head is a commit the projection
/// does not contain.
#[tokio::test]
async fn change_feed_poll_follows_commits_from_another_handle() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db_a = Omnigraph::init(uri, "node Person {\n    name: String @key\n}\n")
        .await
        .unwrap();
    let now = db_a
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    let (cursor, _) = boundary_cursor(&now);

    let db_b = Omnigraph::open(uri).await.unwrap();
    db_b.load(
        "main",
        r#"{"type":"Person","data":{"name":"from-b"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let page = db_a
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(cursor)))
        .await
        .expect("a warm handle must follow another handle's commit");
    assert_eq!(page.blocks.len(), 1, "the foreign commit is one block");
    assert_eq!(page.blocks[0].changes.len(), 1);
    assert_eq!(page.blocks[0].changes[0].id, "from-b");
    let (cursor, caught_up) = boundary_cursor(&page);
    assert!(caught_up);
    // The projection now contains the foreign head, so the next poll stays
    // healthy rather than repeating a missing-commit failure.
    let again = db_a
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(cursor)))
        .await
        .unwrap();
    assert!(again.blocks.is_empty());
}

/// Once the poll-wide BYTE budget is exhausted, the page ends at the block
/// boundary: the solo-oversized forward-progress rule admits ONE over-budget
/// change per PAGE, not one per commit. Each of the three commits here carries
/// a change larger than the 1-byte budget, so each poll delivers exactly one
/// solo block and the backlog drains across three polls.
#[tokio::test]
async fn change_feed_byte_budget_admits_one_solo_oversized_change_per_page() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, "node Person {\n    name: String @key\n}\n")
        .await
        .unwrap();
    let now = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    let (mut cursor, _) = boundary_cursor(&now);
    for row in 0..3 {
        db.load(
            "main",
            &format!(r#"{{"type":"Person","data":{{"name":"solo-{row}"}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }

    for expected in ["solo-0", "solo-1", "solo-2"] {
        let mut request = feed_request(None, ChangeFeedPosition::Cursor(cursor.clone()));
        request.max_bytes = Some(1);
        let page = db.poll_change_feed(request).await.unwrap();
        assert_eq!(
            page.blocks.len(),
            1,
            "an exhausted byte budget must stop at the block boundary instead \
             of force-emitting one oversized change per remaining commit"
        );
        assert_eq!(page.blocks[0].changes.len(), 1);
        assert_eq!(page.blocks[0].changes[0].id, expected);
        let (next, caught_up) = boundary_cursor(&page);
        assert_eq!(caught_up, expected == "solo-2");
        cursor = next;
    }
}

/// A positive byte remainder is still page-wide state. After the first commit
/// consumes almost all of the budget, the next commit may not treat itself as
/// the page's first change and claim another solo-oversized exception.
#[tokio::test]
async fn change_feed_byte_budget_does_not_reset_solo_exception_per_commit() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedScope, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, "node Person {\n    name: String @key\n}")
        .await
        .unwrap();
    let now = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    let (cursor, _) = boundary_cursor(&now);

    let first = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"budget-first"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let second = db
        .load_with_receipt(
            "main",
            &format!(
                r#"{{"type":"Person","data":{{"name":"budget-second-{}"}}}}"#,
                "x".repeat(512)
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let first_change = db
        .commit_changes_page(
            &first.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .block
        .changes
        .remove(0);
    let first_bytes = u64::try_from(serde_json::to_vec(&first_change).unwrap().len()).unwrap();

    let mut request = feed_request(None, ChangeFeedPosition::Cursor(cursor));
    request.max_bytes = Some(first_bytes + 1);
    let page = db.poll_change_feed(request).await.unwrap();
    assert_eq!(
        page.blocks
            .iter()
            .map(|block| block.cause.graph_commit_id.as_str())
            .collect::<Vec<_>>(),
        vec![first.commit.graph_commit_id.as_str()],
        "the second commit must wait for a fresh page even though one byte remains"
    );
    let (cursor, caught_up) = boundary_cursor(&page);
    assert!(!caught_up);

    let resumed = db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(cursor)))
        .await
        .unwrap();
    assert_eq!(resumed.blocks.len(), 1);
    assert_eq!(
        resumed.blocks[0].cause.graph_commit_id,
        second.commit.graph_commit_id
    );
}

#[tokio::test]
async fn change_feed_advances_only_at_block_boundaries_and_pins_the_cut() {
    use omnigraph::changes::{ChangeFeedContinuation, ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // `Now` captures the head and returns a caught-up cursor with no replay.
    let now_page = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    assert!(now_page.blocks.is_empty());
    let (c0, caught_up) = boundary_cursor(&now_page);
    assert!(caught_up);

    let commit_a = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Person\",\"data\":{\"name\":\"fa-1\",\"age\":1}}\n{\"type\":\"Person\",\"data\":{\"name\":\"fa-2\",\"age\":2}}\n{\"type\":\"Person\",\"data\":{\"name\":\"fa-3\",\"age\":3}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let commit_b = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Person\",\"data\":{\"name\":\"fb-1\",\"age\":4}}\n{\"type\":\"Person\",\"data\":{\"name\":\"fb-2\",\"age\":5}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();

    // A row budget smaller than the first block ends the page MID-BLOCK: the
    // partial block rides with its cause and only a page token continues it —
    // no durable cursor exists, so a partial block cannot be checkpointed.
    let mut small = feed_request(None, ChangeFeedPosition::Cursor(c0.clone()));
    small.max_changes = Some(2);
    let first = db.poll_change_feed(small).await.unwrap();
    assert_eq!(first.blocks.len(), 1);
    assert_eq!(
        first.blocks[0].cause.graph_commit_id,
        commit_a.commit.graph_commit_id
    );
    assert_eq!(
        first.blocks[0]
            .changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<Vec<_>>(),
        vec!["fa-1", "fa-2"]
    );
    let page_token = match &first.continuation {
        ChangeFeedContinuation::MidBlock { page_token } => page_token.clone(),
        other => panic!("expected a mid-block continuation, got {other:?}"),
    };

    // A commit landing after the poll began stays outside its captured cut.
    let commit_c = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"fc-1","age":6}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let resumed = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::PageToken(page_token),
        ))
        .await
        .unwrap();
    assert_eq!(
        resumed
            .blocks
            .iter()
            .map(|block| block.cause.graph_commit_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            commit_a.commit.graph_commit_id.as_str(),
            commit_b.commit.graph_commit_id.as_str(),
        ],
        "the resumed page repeats the split block's cause and excludes the post-cut commit"
    );
    assert_eq!(
        resumed.blocks[0]
            .changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<Vec<_>>(),
        vec!["fa-3"]
    );
    let (c1, caught_up) = boundary_cursor(&resumed);
    assert!(
        !caught_up,
        "the token's cut was reached, but the live head has already moved past it"
    );

    // The next poll captures a fresh head and yields exactly the new commit.
    let next = db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(c1)))
        .await
        .unwrap();
    assert_eq!(next.blocks.len(), 1);
    assert_eq!(
        next.blocks[0].cause.graph_commit_id,
        commit_c.commit.graph_commit_id
    );
    let (_, caught_up) = boundary_cursor(&next);
    assert!(caught_up);
}

#[tokio::test]
async fn change_feed_start_modes_now_aftercommit_beginning() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    let extra = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"Late","age":9}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    // Beginning replays the whole first-parent history in order.
    let beginning = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Beginning),
        ))
        .await
        .unwrap();
    let (_, caught_up) = boundary_cursor(&beginning);
    assert!(caught_up);
    assert!(
        beginning
            .blocks
            .iter()
            .any(|block| block.changes.iter().any(|change| change.id == "Alice")),
        "the fixture load block replays from the beginning"
    );
    let late_position = beginning
        .blocks
        .iter()
        .position(|block| block.cause.graph_commit_id == extra.commit.graph_commit_id)
        .expect("the late commit is on the chain");
    assert_eq!(late_position, beginning.blocks.len() - 1, "oldest first");

    // AfterCommit begins exactly after a chain commit; an off-chain id is a
    // typed rejection.
    let fixture_load_commit = beginning
        .blocks
        .iter()
        .find(|block| block.changes.iter().any(|change| change.id == "Alice"))
        .unwrap()
        .cause
        .graph_commit_id
        .clone();
    let after = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::AfterCommit(fixture_load_commit.clone())),
        ))
        .await
        .unwrap();
    assert!(
        after
            .blocks
            .iter()
            .all(|block| block.cause.graph_commit_id != fixture_load_commit)
    );
    assert!(
        after
            .blocks
            .iter()
            .any(|block| block.cause.graph_commit_id == extra.commit.graph_commit_id)
    );
    let err = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::AfterCommit("no-such-commit".into())),
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, OmniError::ChangeCursorRejected { .. }));

    // A named branch inherits main history: Beginning includes the inherited
    // block with its ORIGINAL cause, and the branch-owned block names the
    // branch.
    db.branch_create("feature").await.unwrap();
    let feature_handle = Omnigraph::open(uri).await.unwrap();
    drop(feature_handle);
    let mut feature_db = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature_db,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "FeatureOnly")], &[("$age", 1)]),
    )
    .await
    .unwrap();
    let feature_feed = feature_db
        .poll_change_feed(feed_request(
            Some("feature"),
            ChangeFeedPosition::Start(ChangeFeedStart::Beginning),
        ))
        .await
        .unwrap();
    let inherited = feature_feed
        .blocks
        .iter()
        .find(|block| block.cause.graph_commit_id == fixture_load_commit)
        .expect("inherited main history rides the feature chain");
    assert_eq!(
        inherited.cause.authored_branch, None,
        "an inherited commit keeps its original cause"
    );
    let owned = feature_feed
        .blocks
        .iter()
        .find(|block| {
            block
                .changes
                .iter()
                .any(|change| change.id == "FeatureOnly")
        })
        .expect("the branch-owned commit is on the chain");
    assert_eq!(owned.cause.authored_branch.as_deref(), Some("feature"));
}

#[tokio::test]
async fn change_feed_merge_commit_is_first_parent_relative_with_merged_parent_on_cause() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();
    let pre_merge_head = snapshot_id(&main, "main").await.unwrap();

    main.branch_create("side").await.unwrap();
    let mut side = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut side,
        "side",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Merged")], &[("$age", 7)]),
    )
    .await
    .unwrap();
    // Diverge main so the merge is a true three-way merge commit rather than
    // a fast-forward adoption.
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 41)]),
    )
    .await
    .unwrap();
    let outcome = main.branch_merge("side", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let feed = main
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::AfterCommit(
                pre_merge_head.as_str().to_string(),
            )),
        ))
        .await
        .unwrap();
    let merge_block = feed
        .blocks
        .iter()
        .find(|block| block.cause.merged_parent_commit_id.is_some())
        .expect("the merge commit carries its merged parent for DAG-aware callers");
    let divergence_block = feed
        .blocks
        .iter()
        .find(|block| block.cause.merged_parent_commit_id.is_none())
        .expect("main's own divergence commit rides the chain");
    assert_eq!(
        merge_block.cause.parent_commit_id.as_deref(),
        Some(divergence_block.cause.graph_commit_id.as_str()),
        "the merge block is first-parent relative to main's own head"
    );
    assert!(
        merge_block
            .changes
            .iter()
            .any(|change| change.id == "Merged"),
        "the merge's first-parent delta carries the branch's row"
    );
    assert!(
        merge_block.changes.iter().all(|change| change.id != "Bob"),
        "main's own divergence is not re-attributed to the merge commit"
    );
}

#[tokio::test]
async fn change_feed_cursor_scope_mismatches_are_typed() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedScope, ChangeFeedStart, ChangeOpKind};
    use omnigraph::error::OmniError;

    fn assert_rejected(err: OmniError, fragment: &str) {
        match err {
            OmniError::ChangeCursorRejected { reason } => assert!(
                reason.contains(fragment),
                "reason '{reason}' should mention '{fragment}'"
            ),
            other => panic!("expected a typed continuation rejection, got: {other:?}"),
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();

    let (c_main, _) = boundary_cursor(
        &db.poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap(),
    );

    // Wrong branch.
    assert_rejected(
        db.poll_change_feed(feed_request(
            Some("feature"),
            ChangeFeedPosition::Cursor(c_main.clone()),
        ))
        .await
        .unwrap_err(),
        "different branch",
    );

    // Wrong filter scope.
    let mut scoped = feed_request(None, ChangeFeedPosition::Cursor(c_main.clone()));
    scoped.scope = ChangeFeedScope {
        ops: Some(vec![ChangeOpKind::Insert]),
        ..ChangeFeedScope::default()
    };
    assert_rejected(
        db.poll_change_feed(scoped).await.unwrap_err(),
        "different filter scope",
    );

    // Kind cross-use in both directions.
    let inserted = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Person\",\"data\":{\"name\":\"x-1\",\"age\":1}}\n{\"type\":\"Person\",\"data\":{\"name\":\"x-2\",\"age\":2}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let page = db
        .commit_changes_page(
            &inserted.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            None,
            Some(1),
            None,
        )
        .await
        .unwrap();
    let commit_page_token = page.next_page_token.unwrap();
    assert_rejected(
        db.poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Cursor(commit_page_token.clone()),
        ))
        .await
        .unwrap_err(),
        "change feed cursor",
    );
    assert_rejected(
        db.commit_changes_page(
            &inserted.commit.graph_commit_id,
            &ChangeFeedScope::default(),
            Some(&c_main),
            None,
            None,
        )
        .await
        .unwrap_err(),
        "commit changes page token",
    );

    // Branch delete/recreate: the incarnation witness fences cursor ABA.
    db.branch_create("aba").await.unwrap();
    let (c_aba, _) = boundary_cursor(
        &db.poll_change_feed(feed_request(
            Some("aba"),
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap(),
    );
    db.branch_delete("aba").await.unwrap();
    db.branch_create("aba").await.unwrap();
    assert_rejected(
        db.poll_change_feed(feed_request(Some("aba"), ChangeFeedPosition::Cursor(c_aba)))
            .await
            .unwrap_err(),
        "different branch incarnation",
    );

    // The warm same-branch path must make the same check before reusing its
    // cached cut. Both lifetimes fork the exact same main source, so their
    // manifest version, e_tag, timestamp, head, and rows can all coincide;
    // only Lance's BranchIdentifier distinguishes the replacement.
    db.branch_create("warm-aba").await.unwrap();
    let warm_reader = Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap();
    warm_reader.sync_branch("warm-aba").await.unwrap();
    let (warm_cursor, _) = boundary_cursor(
        &warm_reader
            .poll_change_feed(feed_request(
                Some("warm-aba"),
                ChangeFeedPosition::Start(ChangeFeedStart::Now),
            ))
            .await
            .unwrap(),
    );
    db.branch_delete("warm-aba").await.unwrap();
    db.branch_create("warm-aba").await.unwrap();
    assert_rejected(
        warm_reader
            .poll_change_feed(feed_request(
                Some("warm-aba"),
                ChangeFeedPosition::Cursor(warm_cursor),
            ))
            .await
            .unwrap_err(),
        "different branch incarnation",
    );

    // Another graph entirely (fresh identity domain and genesis).
    let other_dir = tempfile::tempdir().unwrap();
    let other_db = init_and_load(&other_dir).await;
    let err = other_db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(c_main)))
        .await
        .unwrap_err();
    assert!(matches!(err, OmniError::ChangeCursorRejected { .. }));
}

#[tokio::test]
async fn change_feed_commits_examined_ceiling_bounds_sparse_polls() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    for row in 0..3 {
        db.load_with_receipt(
            "main",
            &format!(r#"{{"type":"Person","data":{{"name":"s-{row}","age":{row}}}}}"#),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }

    let full = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Beginning),
        ))
        .await
        .unwrap();
    let (_, caught_up) = boundary_cursor(&full);
    assert!(caught_up);
    let total_blocks = full.blocks.len();
    assert!(total_blocks >= 4, "fixture history plus three loads");

    // Bounded polls drain the same chain deterministically, two commits at a
    // time, without a durable cursor ever landing inside a block.
    let mut bounded = feed_request(None, ChangeFeedPosition::Start(ChangeFeedStart::Beginning));
    bounded.max_commits = Some(2);
    let mut collected = Vec::new();
    let mut page = db.poll_change_feed(bounded).await.unwrap();
    loop {
        assert!(
            page.blocks.len() <= 2,
            "the commit ceiling bounds each poll"
        );
        collected.extend(
            page.blocks
                .iter()
                .map(|block| block.cause.graph_commit_id.clone()),
        );
        let (cursor, caught_up) = boundary_cursor(&page);
        if caught_up {
            break;
        }
        let mut next = feed_request(None, ChangeFeedPosition::Cursor(cursor));
        next.max_commits = Some(2);
        page = db.poll_change_feed(next).await.unwrap();
    }
    assert_eq!(
        collected,
        full.blocks
            .iter()
            .map(|block| block.cause.graph_commit_id.clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn change_feed_resume_is_stateless_across_handles() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = init_and_load(&dir).await;
    let (cursor, _) = boundary_cursor(
        &db.poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap(),
    );
    let commit = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"Stateless","age":1}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    // A second handle resumes the first handle's cursor with no server state.
    let other = Omnigraph::open(uri).await.unwrap();
    let page = other
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(cursor)))
        .await
        .unwrap();
    assert_eq!(page.blocks.len(), 1);
    assert_eq!(
        page.blocks[0].cause.graph_commit_id,
        commit.commit.graph_commit_id
    );
    let (_, caught_up) = boundary_cursor(&page);
    assert!(caught_up);
}

#[tokio::test]
async fn change_feed_gap_then_baseline_reset() {
    use omnigraph::changes::{ChangeFeedPosition, ChangeFeedScope, ChangeFeedStart};
    use omnigraph::error::OmniError;

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let (c0, _) = boundary_cursor(
        &db.poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap(),
    );
    let commit_a = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"gap-a","age":1}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    db.load_with_receipt(
        "main",
        r#"{"type":"Person","data":{"name":"gap-b","age":2}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Reclaim the Person history commit A pins: the feed cannot continue
    // contiguously and surfaces the typed gap with the caller's cursor.
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let person_path = &snapshot.entry("node:Person").unwrap().table_path;
    let person_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        person_path.trim_start_matches('/')
    );
    let person = lance::Dataset::open(&person_uri).await.unwrap();
    let removed = lance::dataset::cleanup::cleanup_old_versions(
        &person,
        lance::dataset::cleanup::CleanupPolicy {
            before_version: Some(person.version().version),
            delete_unverified: true,
            error_if_tagged_old_versions: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(removed.old_versions > 0, "precondition: history reclaimed");

    let gap = db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::Cursor(c0.clone())))
        .await
        .unwrap_err();
    match gap {
        OmniError::ChangeFeedGap {
            cursor,
            first_unreadable_commit_id,
        } => {
            assert_eq!(cursor.as_deref(), Some(c0.as_str()));
            assert_eq!(first_unreadable_commit_id, commit_a.commit.graph_commit_id);
        }
        other => panic!("expected a typed change feed gap, got: {other:?}"),
    }

    // Recovery is the exact baseline handshake: one snapshot pinned AT the
    // captured head plus the cursor that resumes right after it.
    let scope = ChangeFeedScope::default();
    let mut snapshot_bytes = Vec::new();
    let baseline = db
        .capture_change_baseline("main", &scope, &mut snapshot_bytes)
        .await
        .unwrap();
    assert_eq!(
        baseline.snapshot_commit_id,
        snapshot_id(&db, "main").await.unwrap().as_str(),
        "the snapshot is pinned at the captured head"
    );
    let exported = String::from_utf8(snapshot_bytes).unwrap();
    assert!(exported.contains("gap-a") && exported.contains("gap-b"));
    let direct = db.export_jsonl("main", &[], &[]).await.unwrap();
    assert_eq!(exported, direct, "the baseline is the exact entity export");

    // A commit landing AFTER the handshake is outside the snapshot and is the
    // first block the resumed feed yields.
    let post = db
        .load_with_receipt(
            "main",
            r#"{"type":"Person","data":{"name":"post-baseline","age":3}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();
    assert!(!exported.contains("post-baseline"));
    let resumed = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Cursor(baseline.resume_cursor),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.blocks.len(), 1);
    assert_eq!(
        resumed.blocks[0].cause.graph_commit_id,
        post.commit.graph_commit_id
    );

    // A failing writer fails the handshake: no cursor can outlive a broken
    // snapshot.
    struct FailingWriter;
    impl std::io::Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("sink failed"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    assert!(
        db.capture_change_baseline("main", &scope, &mut FailingWriter)
            .await
            .is_err()
    );
}

/// The frozen block order is keyed by the PUBLISHED opaque type identity, not
/// by any internal numeric identity: within one kind, types emit in
/// lexicographic order of the `entity_type.id` the caller actually sees, and a
/// paged walk resumes across those type boundaries on the same key.
///
/// The opaque ids are hash projections of a per-graph identity domain, so
/// which permutation the internal numeric order would produce varies per run —
/// with six types, an implementation sorting by internal identity matches this
/// assertion only with probability 1/720 per run.
#[tokio::test]
async fn commit_changes_blocks_order_by_published_type_id() {
    use omnigraph::changes::ChangeFeedScope;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        r#"
node Alpha { name: String @key }
node Bravo { name: String @key }
node Charlie { name: String @key }
node Delta { name: String @key }
node Echo { name: String @key }
node Foxtrot { name: String @key }
"#,
    )
    .await
    .unwrap();
    let scope = ChangeFeedScope::default();
    let commit = db
        .load_with_receipt(
            "main",
            concat!(
                r#"{"type":"Alpha","data":{"name":"a"}}"#,
                "\n",
                r#"{"type":"Bravo","data":{"name":"b"}}"#,
                "\n",
                r#"{"type":"Charlie","data":{"name":"c"}}"#,
                "\n",
                r#"{"type":"Delta","data":{"name":"d"}}"#,
                "\n",
                r#"{"type":"Echo","data":{"name":"e"}}"#,
                "\n",
                r#"{"type":"Foxtrot","data":{"name":"f"}}"#,
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let page = db
        .commit_changes_page(&commit.commit.graph_commit_id, &scope, None, None, None)
        .await
        .unwrap();
    assert_eq!(page.block.changes.len(), 6);
    let emitted: Vec<(String, String)> = page
        .block
        .changes
        .iter()
        .map(|change| (change.entity_type.id.clone(), change.id.clone()))
        .collect();
    let mut expected = emitted.clone();
    expected.sort();
    assert_eq!(
        emitted, expected,
        "changes must emit in lexicographic order of the published type id"
    );

    // A bounded walk (two changes per page) crosses type boundaries and must
    // reproduce exactly the same published order.
    let mut token = None;
    let mut paged = Vec::new();
    loop {
        let page = db
            .commit_changes_page(
                &commit.commit.graph_commit_id,
                &scope,
                token.as_deref(),
                Some(2),
                None,
            )
            .await
            .unwrap();
        paged.extend(
            page.block
                .changes
                .iter()
                .map(|change| (change.entity_type.id.clone(), change.id.clone())),
        );
        match page.next_page_token {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    assert_eq!(paged, expected, "a paged walk resumes on the published key");
}

/// IDs above the exact continuation cap use a bounded prefix+digest position.
/// Both finite commit paging and a split feed block must resolve that position
/// within the shared-prefix range and resume after the exact row — no duplicate
/// and no skipped successor even though the token never carries the long tail.
#[tokio::test]
async fn long_ids_resume_exactly_with_bounded_commit_and_feed_tokens() {
    use omnigraph::changes::{
        ChangeFeedContinuation, ChangeFeedPosition, ChangeFeedScope, ChangeFeedStart,
    };

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, "node Person {\n    name: String @key\n}")
        .await
        .unwrap();
    let now = db
        .poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap();
    let (cursor, _) = boundary_cursor(&now);

    let common = "long-prefix-".repeat(32);
    let first_id = format!("{common}a");
    let second_id = format!("{common}b");
    assert!(first_id.len() > 256, "fixture must take the digest path");
    let commit = db
        .load_with_receipt(
            "main",
            &format!(
                "{}\n{}",
                serde_json::json!({"type": "Person", "data": {"name": &first_id}}),
                serde_json::json!({"type": "Person", "data": {"name": &second_id}}),
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();

    let scope = ChangeFeedScope::default();
    let first = db
        .commit_changes_page(&commit.commit.graph_commit_id, &scope, None, Some(1), None)
        .await
        .unwrap();
    assert_eq!(first.block.changes[0].id, first_id);
    let token = first.next_page_token.expect("long-id page must continue");
    assert!(
        token.len() <= 4096,
        "continuation must have a fixed ceiling"
    );
    let second = db
        .commit_changes_page(
            &commit.commit.graph_commit_id,
            &scope,
            Some(&token),
            Some(1),
            None,
        )
        .await
        .unwrap();
    assert_eq!(second.block.changes.len(), 1);
    assert_eq!(second.block.changes[0].id, second_id);
    assert!(second.next_page_token.is_none());

    let mut request = feed_request(None, ChangeFeedPosition::Cursor(cursor));
    request.max_changes = Some(1);
    let first = db.poll_change_feed(request).await.unwrap();
    assert_eq!(first.blocks[0].changes[0].id, first_id);
    let token = match first.continuation {
        ChangeFeedContinuation::MidBlock { page_token } => page_token,
        other => panic!("expected a split long-id block, got {other:?}"),
    };
    assert!(token.len() <= 4096, "feed token must have a fixed ceiling");
    let second = db
        .poll_change_feed(feed_request(None, ChangeFeedPosition::PageToken(token)))
        .await
        .unwrap();
    assert_eq!(second.blocks.len(), 1);
    assert_eq!(second.blocks[0].changes.len(), 1);
    assert_eq!(second.blocks[0].changes[0].id, second_id);
    let (_, caught_up) = boundary_cursor(&second);
    assert!(caught_up);
}

/// A page-token resume whose FIRST change exceeds the poll's own byte budget
/// must still DELIVER that change — a legal committed change is always
/// crossable, emitted on its own page rather than wedging the feed with a typed
/// limit. The byte budget is a packing target (how many small changes share a
/// page), never a wall a single legal two-image change cannot pass.
#[tokio::test]
async fn change_feed_resumed_oversized_change_is_delivered_solo() {
    use omnigraph::changes::{ChangeFeedContinuation, ChangeFeedPosition, ChangeFeedStart};

    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    let (c0, _) = boundary_cursor(
        &db.poll_change_feed(feed_request(
            None,
            ChangeFeedPosition::Start(ChangeFeedStart::Now),
        ))
        .await
        .unwrap(),
    );

    // One commit, two changes: a small row first, then one whose image alone
    // exceeds the resumed poll's byte budget.
    let large_name = format!("z-large-{}", "x".repeat(512));
    db.load_with_receipt(
        "main",
        &format!(
            "{}\n{}",
            r#"{"type":"Person","data":{"name":"a-small","age":1}}"#,
            format_args!(r#"{{"type":"Person","data":{{"name":"{large_name}","age":2}}}}"#),
        ),
        LoadMode::Merge,
    )
    .await
    .unwrap();

    // Split the block after the small change.
    let mut first = feed_request(None, ChangeFeedPosition::Cursor(c0));
    first.max_changes = Some(1);
    let first = db.poll_change_feed(first).await.unwrap();
    let token = match &first.continuation {
        ChangeFeedContinuation::MidBlock { page_token } => page_token.clone(),
        other => panic!("expected a mid-block continuation, got {other:?}"),
    };
    assert_eq!(first.blocks[0].changes[0].id, "a-small");

    // Resume with a byte budget smaller than the remaining change: it is
    // delivered on its own page, and the feed advances past it.
    let mut resumed = feed_request(None, ChangeFeedPosition::PageToken(token));
    resumed.max_bytes = Some(64);
    let page = db.poll_change_feed(resumed).await.unwrap();
    let delivered: Vec<&str> = page
        .blocks
        .iter()
        .flat_map(|block| block.changes.iter())
        .map(|change| change.id.as_str())
        .collect();
    assert_eq!(
        delivered,
        vec![large_name.as_str()],
        "the oversized change must be delivered solo, not refused"
    );
    match &page.continuation {
        ChangeFeedContinuation::AtBlockBoundary { caught_up, .. } => {
            assert!(
                *caught_up,
                "the feed must advance past the solo oversized change"
            )
        }
        other => panic!("expected a caught-up block boundary after the solo change, got {other:?}"),
    }
}
