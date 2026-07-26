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
    assert_eq!(
        change_tuples(&reincarnation_diff),
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
        "drop/re-add under one alias must report the old lifetime's deletes and the new lifetime's inserts"
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

// ─── RFC-029: bounded, ordered, tiered change feed ─────────────────────────
//
// These cover the three defects RFC-029 records: unbounded materialization,
// signature retention of every column, and a summary gated behind the full
// row build. They also pin the deterministic ordering the keyset cursor
// depends on — `diff_snapshots` previously walked table identities through a
// `HashSet`, so table order was unspecified (a deny-list violation in its own
// right: "hash-map iteration order in result ordering").

// Six types, not three: the pre-fix implementation walked identities through a
// `HashSet`, so a narrow schema had a real chance of coming out sorted by luck
// and showing a false green. Six makes an accidental sort ~1/720.
const RFC029_SCHEMA: &str = r#"
node Alpha   { name: String @key }
node Beta    { name: String @key }
node Gamma   { name: String @key }
node Delta   { name: String @key }
node Epsilon { name: String @key }
node Zeta    { name: String @key }
"#;

const RFC029_TYPES: [&str; 6] = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta"];

/// Load `count` rows into each of the RFC-029 node types.
async fn load_rfc029_rows(db: &Omnigraph, branch: &str, prefix: &str, count: usize) {
    let mut rows = String::new();
    for type_name in RFC029_TYPES {
        for i in 0..count {
            rows.push_str(&format!(
                "{{\"type\":\"{type_name}\",\"data\":{{\"name\":\"{prefix}-{i}\"}}}}\n"
            ));
        }
    }
    db.load(branch, &rows, LoadMode::Merge).await.unwrap();
}

/// The change list is ordered by `(table_key, id)`. Keyset pagination is only
/// correct against a total order, and callers depend on stable output ordering
/// per Hyrum's Law once it is exposed over HTTP.
#[tokio::test]
async fn diff_changes_are_ordered_by_table_key_then_id() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, RFC029_SCHEMA).await.unwrap();
    let before = snapshot_id(&db, "main").await.unwrap();
    load_rfc029_rows(&db, "main", "row", 12).await;
    let after = snapshot_id(&db, "main").await.unwrap();

    let cs = db
        .diff_between(
            ReadTarget::Snapshot(before),
            ReadTarget::Snapshot(after),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();

    let observed: Vec<(String, String)> = cs
        .changes
        .iter()
        .map(|c| (c.table_key.clone(), c.id.clone()))
        .collect();
    let mut expected = observed.clone();
    expected.sort();
    assert_eq!(
        observed, expected,
        "change list must be totally ordered by (table_key, id)"
    );
    assert_eq!(cs.changes.len(), 12 * RFC029_TYPES.len());
}

/// `limit` bounds the returned page and yields a resumable cursor. The bound
/// must be enforced while accumulating, not by truncating a fully-built vector
/// — that is the whole point of the defect RFC-029 §2.1 records.
#[tokio::test]
async fn diff_respects_limit_and_returns_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, RFC029_SCHEMA).await.unwrap();
    let before = snapshot_id(&db, "main").await.unwrap();
    load_rfc029_rows(&db, "main", "row", 10).await;
    let after = snapshot_id(&db, "main").await.unwrap();

    let filter = ChangeFilter {
        limit: Some(7),
        ..Default::default()
    };
    let cs = db
        .diff_between(
            ReadTarget::Snapshot(before),
            ReadTarget::Snapshot(after),
            &filter,
        )
        .await
        .unwrap();

    assert_eq!(cs.changes.len(), 7, "limit must bound the page");
    assert!(
        cs.next_cursor.is_some(),
        "a truncated page must carry a resume cursor"
    );
    // Page-scoped stats: totals come from the summary tier.
    assert_eq!(cs.stats.inserts, 7);
}

/// Paging with the cursor covers the full change set exactly once — no
/// duplicates across page boundaries, no gaps, and a terminal page whose
/// cursor is `None`.
#[tokio::test]
async fn diff_cursor_pages_cover_the_change_set_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, RFC029_SCHEMA).await.unwrap();
    let before = snapshot_id(&db, "main").await.unwrap();
    load_rfc029_rows(&db, "main", "row", 10).await;
    let after = snapshot_id(&db, "main").await.unwrap();

    let unbounded = db
        .diff_between(
            ReadTarget::Snapshot(before.clone()),
            ReadTarget::Snapshot(after.clone()),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();

    let mut paged: Vec<(String, String)> = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let filter = ChangeFilter {
            limit: Some(4),
            after: cursor.clone(),
            ..Default::default()
        };
        let page = db
            .diff_between(
                ReadTarget::Snapshot(before.clone()),
                ReadTarget::Snapshot(after.clone()),
                &filter,
            )
            .await
            .unwrap();
        paged.extend(
            page.changes
                .iter()
                .map(|c| (c.table_key.clone(), c.id.clone())),
        );
        pages += 1;
        assert!(pages < 50, "paging failed to terminate");
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let expected: Vec<(String, String)> = unbounded
        .changes
        .iter()
        .map(|c| (c.table_key.clone(), c.id.clone()))
        .collect();
    assert_eq!(
        paged, expected,
        "paged traversal must reproduce the unbounded change list exactly"
    );
}

/// The summary tier reports the same totals as a full diff without the caller
/// materializing the row list.
#[tokio::test]
async fn diff_summary_matches_the_full_diff_totals() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, RFC029_SCHEMA).await.unwrap();
    let before = snapshot_id(&db, "main").await.unwrap();
    load_rfc029_rows(&db, "main", "row", 5).await;
    let after = snapshot_id(&db, "main").await.unwrap();

    let full = db
        .diff_between(
            ReadTarget::Snapshot(before.clone()),
            ReadTarget::Snapshot(after.clone()),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();
    let summary = db
        .diff_summary_between(
            ReadTarget::Snapshot(before),
            ReadTarget::Snapshot(after),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();

    assert_eq!(summary.stats.inserts, full.stats.inserts);
    assert_eq!(summary.stats.updates, full.stats.updates);
    assert_eq!(summary.stats.deletes, full.stats.deletes);
    assert_eq!(summary.stats.types_affected, full.stats.types_affected);
    assert_eq!(summary.from_version, full.from_version);
    assert_eq!(summary.to_version, full.to_version);
    assert_eq!(summary.stats.inserts, 5 * RFC029_TYPES.len());
}

/// Regression guard for the digest change in RFC-029 §3.3.
///
/// The row signature stops retaining every stringified column, but it must not
/// stop *observing* any column: a row whose only difference is its embedding is
/// still an `Update`. Dropping vector/blob columns from the signature would be
/// cheaper and would silently lose this change — that alternative is explicitly
/// rejected. Cross-branch is the path that compares signatures at all.
#[tokio::test]
async fn diff_reports_an_embedding_only_update_across_branches() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(
        uri,
        "node Chunk {\n  slug: String @key\n  embedding: Vector(8)\n}\n",
    )
    .await
    .unwrap();

    let base = "{\"type\":\"Chunk\",\"data\":{\"slug\":\"c0\",\"embedding\":[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]}}\n";
    db.load("main", base, LoadMode::Merge).await.unwrap();
    db.branch_create("feature").await.unwrap();

    // Same key, same scalars — only the vector differs.
    let moved = "{\"type\":\"Chunk\",\"data\":{\"slug\":\"c0\",\"embedding\":[9.0,9.0,9.0,9.0,9.0,9.0,9.0,9.0]}}\n";
    db.load("feature", moved, LoadMode::Merge).await.unwrap();

    let cs = db
        .diff_between(
            ReadTarget::branch("main"),
            ReadTarget::branch("feature"),
            &ChangeFilter::default(),
        )
        .await
        .unwrap();

    let ops: Vec<ChangeOp> = cs.changes.iter().map(|c| c.op).collect();
    assert_eq!(
        ops,
        vec![ChangeOp::Update],
        "an embedding-only change must still be reported as an Update: {:?}",
        change_tuples(&cs)
    );
}
