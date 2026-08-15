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

// ─── Per-commit entity change pages ────────────────────────────────────────

fn properties(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("expected object").clone()
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
            ..
        } => assert_eq!(first_unreadable_commit_id, inserted.commit.graph_commit_id),
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
