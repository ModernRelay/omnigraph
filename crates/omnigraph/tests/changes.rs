mod helpers;

use omnigraph::changes::{ChangeFilter, ChangeOp, EntityKind};
use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{CleanupPolicyOptions, MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
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
async fn commit_changes_are_exact_ordered_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let first = db
        .list_commits(Some("main"))
        .await
        .unwrap()
        .into_iter()
        .last()
        .unwrap();
    let first_page = db
        .commit_changes_page(
            &first.graph_commit_id,
            None,
            100,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert!(first_page.commit_complete);
    assert!(first_page.changes.is_empty());

    let inserted = db
        .load_with_receipt(
            "main",
            "{\"type\":\"Person\",\"data\":{\"name\":\"feed-C\",\"age\":3}}\n{\"type\":\"Person\",\"data\":{\"name\":\"feed-A\",\"age\":1}}\n{\"type\":\"Person\",\"data\":{\"name\":\"feed-B\",\"age\":2}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();
    let first_page = db
        .commit_changes_page(
            &inserted.commit.graph_commit_id,
            None,
            2,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(
        first_page
            .changes
            .iter()
            .map(|change| (change.change_index, change.id.as_str(), change.op))
            .collect::<Vec<_>>(),
        vec![
            (0, "feed-A", ChangeOp::Insert),
            (1, "feed-B", ChangeOp::Insert)
        ]
    );
    assert!(!first_page.commit_complete);
    let second_page = db
        .commit_changes_page(
            &inserted.commit.graph_commit_id,
            first_page.next_cursor.as_deref(),
            2,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(second_page.changes[0].change_index, 2);
    assert_eq!(second_page.changes[0].id, "feed-C");
    assert_eq!(second_page.changes[0].before, None);
    assert_eq!(second_page.changes[0].after.as_ref().unwrap()["age"], 3);
    assert!(second_page.commit_complete);
    assert!(second_page.next_cursor.is_none());

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
        .commit_changes_page(
            &updated.graph_commit_id,
            None,
            10,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(update_page.changes.len(), 1);
    assert_eq!(update_page.changes[0].op, ChangeOp::Update);
    assert!(update_page.changes[0].before.is_none());
    assert_eq!(update_page.changes[0].after.as_ref().unwrap()["age"], 99);

    let deleted = db
        .mutate_with_receipt(
            "main",
            MUTATION_QUERIES,
            "remove_person",
            &params(&[("", "Alice")]),
        )
        .await
        .unwrap()
        .commit
        .unwrap();
    let delete_page = db
        .commit_changes_page(
            &deleted.graph_commit_id,
            None,
            10,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert!(
        delete_page
            .changes
            .iter()
            .all(|change| change.op == ChangeOp::Delete)
    );
    assert!(
        delete_page
            .changes
            .iter()
            .all(|change| change.before.is_some())
    );
    assert!(
        delete_page
            .changes
            .iter()
            .all(|change| change.after.is_none())
    );
    assert!(
        delete_page
            .changes
            .iter()
            .any(|change| change.kind == EntityKind::Edge)
    );
    assert_eq!(
        delete_page
            .changes
            .iter()
            .map(|change| change.change_index)
            .collect::<Vec<_>>(),
        (0..delete_page.changes.len()).collect::<Vec<_>>()
    );

    let mut cursor = None;
    let mut paged = Vec::new();
    loop {
        let page = db
            .commit_changes_page(
                &deleted.graph_commit_id,
                cursor.as_deref(),
                1,
                omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
            )
            .await
            .unwrap();
        paged.extend(
            page.changes
                .iter()
                .map(|change| (change.table_key.clone(), change.id.clone())),
        );
        if page.commit_complete {
            break;
        }
        cursor = page.next_cursor;
    }
    assert_eq!(
        paged,
        delete_page
            .changes
            .iter()
            .map(|change| (change.table_key.clone(), change.id.clone()))
            .collect::<Vec<_>>()
    );

    let empty = db
        .load_with_receipt("main", "", LoadMode::Merge)
        .await
        .unwrap();
    let empty_page = db
        .commit_changes_page(
            &empty.commit.graph_commit_id,
            None,
            10,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert!(empty_page.commit_complete);
    assert!(empty_page.changes.is_empty());

    db.cleanup(CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    assert!(matches!(
        db.commit_changes_page(
            &inserted.commit.graph_commit_id,
            None,
            10,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await,
        Err(OmniError::ChangeFeedGap {
            first_unreadable_commit_id,
            ..
        }) if first_unreadable_commit_id == inserted.commit.graph_commit_id
    ));
}

#[tokio::test]
async fn commit_changes_use_the_commit_era_physical_schema() {
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
    let inserted = db
        .load_with_receipt(
            "main",
            r#"{"type":"Document","data":{"title":"old","payload":"base64:T2xk"}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    db.apply_schema(
        r#"
node Article @rename_from("Document") {
    title: String @key
    summary: String?
}
"#,
    )
    .await
    .unwrap();

    let page = db
        .commit_changes_page(
            &inserted.commit.graph_commit_id,
            None,
            10,
            omnigraph::changes::COMMIT_CHANGES_DEFAULT_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(page.changes.len(), 1);
    assert_eq!(page.changes[0].table_key, "node:Document");
    assert_eq!(
        page.changes[0].after,
        Some(serde_json::json!({
            "id": "old",
            "title": "old",
            "payload": "base64:T2xk",
        }))
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
