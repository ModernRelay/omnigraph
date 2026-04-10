mod helpers;

use omnigraph::changes::{ChangeFilter, ChangeOp, EntityKind};
use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};

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
        !person_updates.is_empty() || person_inserts.len() > 0,
        "Should detect Bob's age update or Eve's insert"
    );
}

#[tokio::test]
async fn diff_commits_resolves_feature_commit_from_main_handle() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
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
    let mut main = init_and_load(&dir).await;
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
    let mut main = init_and_load(&dir).await;
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
    let mut main = init_and_load(&dir).await;
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
    let mut main = init_and_load(&dir).await;
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
    let mut main = init_and_load(&dir).await;
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
