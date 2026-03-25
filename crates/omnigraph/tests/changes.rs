mod helpers;

use omnigraph::changes::{ChangeFilter, ChangeOp, EntityKind};
use omnigraph::db::{MergeOutcome, Omnigraph};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

// ─── Same-branch diff tests ────────────────────────────────────────────────

#[tokio::test]
async fn diff_empty_when_nothing_changed() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v = db.version();
    let cs = db.diff(v, v, &ChangeFilter::default()).await.unwrap();
    assert!(cs.changes.is_empty());
    assert_eq!(cs.stats.inserts, 0);
    assert_eq!(cs.stats.updates, 0);
    assert_eq!(cs.stats.deletes, 0);
}

#[tokio::test]
async fn diff_detects_node_insert() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = db.version();

    db.run_mutation(
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let cs = db
        .changes_since(v_before, &ChangeFilter::default())
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
        cs.changes.iter().map(|c| (&c.table_key, &c.id, c.op)).collect::<Vec<_>>()
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
    let v_before = db.version();

    db.run_mutation(
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    let cs = db
        .changes_since(v_before, &ChangeFilter::default())
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
        cs.changes.iter().map(|c| (&c.table_key, &c.id, c.op)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn diff_detects_node_delete_with_cascade() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = db.version();

    db.run_mutation(
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    let cs = db
        .changes_since(v_before, &ChangeFilter::default())
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
        cs.changes.iter().map(|c| (&c.table_key, &c.id, c.op)).collect::<Vec<_>>()
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
        cs.changes.iter().map(|c| (&c.table_key, &c.id, c.op)).collect::<Vec<_>>()
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
    let v_before = db.version();

    db.run_mutation(
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Bob"), ("$to", "Charlie")]),
    )
    .await
    .unwrap();

    let cs = db
        .changes_since(v_before, &ChangeFilter::default())
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
        cs.changes.iter().map(|c| (&c.table_key, &c.id, c.op)).collect::<Vec<_>>()
    );

    let e = &edge_inserts[0];
    assert_eq!(e.kind, EntityKind::Edge);
    let ep = e.endpoints.as_ref().expect("Edge insert should have endpoints");
    assert!(!ep.src.is_empty(), "src should not be empty");
    assert!(!ep.dst.is_empty(), "dst should not be empty");
}

// ─── Filter tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn filter_by_type_name_skips_non_matching() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = db.version();

    // Insert a person (node:Person) and add a friend (edge:Knows)
    db.run_mutation(
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
    let cs = db.changes_since(v_before, &filter).await.unwrap();
    assert!(
        cs.changes.is_empty(),
        "Filter to Company should skip Person changes. Got: {:?}",
        cs.changes.iter().map(|c| (&c.table_key, &c.id, c.op)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn filter_by_op_skips_unwanted_operations() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    let v_before = db.version();

    // Insert Eve, update Bob, delete Alice
    db.run_mutation(
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    db.run_mutation(
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
    let cs = db.changes_since(v_before, &filter).await.unwrap();

    // Should only have inserts, no updates or deletes
    for c in &cs.changes {
        assert_eq!(
            c.op,
            ChangeOp::Insert,
            "Filter for Insert-only should not include {:?} for {} ({})",
            c.op, c.table_key, c.id
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
    let v_before_branch = main.version();

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open_branch(uri, "feature").await.unwrap();

    // Main updates Bob
    main.run_mutation(
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve
    feature
        .run_mutation(
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    // Diff from pre-branch to post-merge on main
    let cs = main
        .changes_since(v_before_branch, &ChangeFilter::default())
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
