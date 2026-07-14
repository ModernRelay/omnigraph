mod helpers;

use std::fs;

use arrow_array::{Array, Int32Array, UInt64Array};
use futures::TryStreamExt;
use lance_index::is_system_index;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::error::{MergeConflictKind, OmniError};
use omnigraph::loader::{LoadMode, load_jsonl};

use helpers::*;

const SEARCH_SCHEMA: &str = include_str!("fixtures/search.pg");
const SEARCH_DATA: &str = include_str!("fixtures/search.jsonl");
const SEARCH_QUERIES: &str = include_str!("fixtures/search.gq");
const SEARCH_MUTATIONS: &str = r#"
query set_doc_title($slug: String, $title: String) {
    update Doc set { title: $title } where slug = $slug
}
"#;

const UNIQUE_SCHEMA: &str = r#"
node User {
    name: String @key
    email: String?
    @unique(email)
}
"#;

const UNIQUE_DATA: &str = r#"{"type":"User","data":{"name":"Alice","email":"alice@example.com"}}"#;

const UNIQUE_MUTATIONS: &str = r#"
query insert_user($name: String, $email: String) {
    insert User { name: $name, email: $email }
}
"#;

const EDGE_UNIQUE_SCHEMA: &str = r#"
node Person {
    name: String @key
}

edge Knows: Person -> Person {
    @unique(src, dst)
}
"#;

const EDGE_UNIQUE_DATA: &str = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}
{"type":"Person","data":{"name":"Carol"}}"#;

const EDGE_UNIQUE_MUTATIONS: &str = r#"
query add_knows($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
"#;

const CARDINALITY_SCHEMA: &str = r#"
node Person {
    name: String @key
}

node Company {
    name: String @key
}

edge WorksAt: Person -> Company @card(0..1)
"#;

const CARDINALITY_DATA: &str = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"type":"Company","data":{"name":"Beta"}}"#;

const CARDINALITY_MUTATIONS: &str = r#"
query add_employment($person: String, $company: String) {
    insert WorksAt { from: $person, to: $company }
}
"#;

const BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
    note: String?
}
"#;

const BLOB_MUTATIONS: &str = r#"
query insert_doc($title: String, $content: Blob, $note: String) {
    insert Document { title: $title, content: $content, note: $note }
}

query update_doc_note($title: String, $note: String) {
    update Document set { note: $note } where title = $title
}
"#;

async fn init_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&mut db, SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

async fn init_db_from_schema_and_data(
    dir: &tempfile::TempDir,
    schema: &str,
    data: &str,
) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&mut db, data, LoadMode::Overwrite)
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn branch_create_open_list_and_lazy_branching_work() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    let main_person = snapshot_main(&main)
        .await
        .unwrap()
        .entry("node:Person")
        .unwrap()
        .clone();

    main.branch_create("feature").await.unwrap();
    // Reproduce Lance's phase-1-only crash state: keep the shallow-cloned
    // `tree/feature` dataset but remove BranchContents, its sole logical
    // authority. A same-name graph create must reclaim the zombie and retry,
    // rather than surfacing DatasetAlreadyExists forever.
    std::fs::remove_file(
        dir.path()
            .join("__manifest")
            .join("_refs")
            .join("branches")
            .join("feature.json"),
    )
    .unwrap();
    main.branch_create("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main", "feature"]);

    let mut feature = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        4
    );
    let initial_feature_snap = snapshot_branch(&feature, "feature").await.unwrap();
    let inherited_person = initial_feature_snap.entry("node:Person").unwrap();
    assert_eq!(
        inherited_person.table_path, main_person.table_path,
        "branch creation must inherit the source table identity/path"
    );
    assert_eq!(inherited_person.table_version, main_person.table_version);
    assert_eq!(inherited_person.table_branch.as_deref(), None);

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        snap.entry("node:Person").unwrap().table_path,
        main_person.table_path,
        "the first lazy fork must preserve the logical table identity/path"
    );
    assert_eq!(
        snap.entry("node:Person").unwrap().table_branch.as_deref(),
        Some("feature")
    );
    assert_eq!(
        snap.entry("edge:Knows").unwrap().table_branch.as_deref(),
        None
    );

    let main = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows(&main, "node:Person").await, 4);
}

#[tokio::test]
async fn explicit_target_query_reads_multiple_branches_from_one_handle() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    db.mutate(
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let main_qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 0);

    let feature_qr = db
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(feature_qr.num_rows(), 1);
}

#[tokio::test]
async fn resolved_snapshot_stays_pinned_after_branch_advances() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let snapshot_id = db.resolve_snapshot("main").await.unwrap();
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let pinned = db
        .query(
            ReadTarget::Snapshot(snapshot_id.clone()),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(pinned.num_rows(), 0);

    let head = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(head.num_rows(), 1);
}

#[tokio::test]
async fn explicit_target_load_writes_to_named_branch() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    db.load(
        "feature",
        r#"{"type":"Person","data":{"name":"Eve","age":22}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    let main_qr = db
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(main_qr.num_rows(), 0);

    let feature_qr = db
        .query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "get_person",
            &params(&[("$name", "Eve")]),
        )
        .await
        .unwrap();
    assert_eq!(feature_qr.num_rows(), 1);
}

#[tokio::test]
async fn branch_merge_updates_main_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let feature_qr = query_branch(
        &mut feature,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let main_before = query_main(
        &mut main,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(main_before.num_rows(), 2);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let merged = query_main(
        &mut main,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(merged.num_rows(), 3);
}

#[tokio::test]
async fn branch_merge_with_blob_columns_preserves_blob_data() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    load_jsonl(
        &mut main,
        concat!(
            "{\"type\":\"Document\",\"data\":{\"title\":\"seed\",\"content\":\"base64:U2VlZA==\",\"note\":\"original\"}}\n",
            "{\"type\":\"Document\",\"data\":{\"title\":\"main-doc\",\"content\":\"base64:TWFpbg==\",\"note\":\"main\"}}",
        ),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    // This regression must not rely on an incidental physical index selecting
    // Lance's legacy partial-column merge plan. The materialized-blob update
    // path is correct even when the table has no user index at all.
    let ds = snapshot_main(&main)
        .await
        .unwrap()
        .open("node:Document")
        .await
        .unwrap();
    let indices = ds.load_indices().await.unwrap();
    assert!(
        indices.iter().all(is_system_index),
        "blob correctness regression requires an index-absent table"
    );

    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_main(
        &mut main,
        BLOB_MUTATIONS,
        "update_doc_note",
        &params(&[("$title", "main-doc"), ("$note", "updated on main")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[
            ("$title", "readme"),
            ("$content", "base64:SGVsbG8="),
            ("$note", "branch insert"),
        ]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        BLOB_MUTATIONS,
        "update_doc_note",
        &params(&[("$title", "seed"), ("$note", "updated on branch")]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let readme = main
        .read_blob("Document", "readme", "content")
        .await
        .unwrap();
    let readme_bytes = readme.read().await.unwrap();
    assert_eq!(&readme_bytes[..], b"Hello");

    let seed = main.read_blob("Document", "seed", "content").await.unwrap();
    let seed_bytes = seed.read().await.unwrap();
    assert_eq!(&seed_bytes[..], b"Seed");

    let main_doc = main
        .read_blob("Document", "main-doc", "content")
        .await
        .unwrap();
    let main_doc_bytes = main_doc.read().await.unwrap();
    assert_eq!(&main_doc_bytes[..], b"Main");
}

#[tokio::test]
async fn branch_merge_with_external_blob_uri_materializes_payload() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let external_path = external_dir.path().join("external.txt");
    fs::write(&external_path, b"External").unwrap();
    let external_uri = format!("file://{}", external_path.display());

    let mut main = Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap();
    load_jsonl(&mut main, "", LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    load_jsonl(
        &mut main,
        "{\"type\":\"Document\",\"data\":{\"title\":\"main-doc\",\"content\":\"base64:TWFpbg==\",\"note\":\"main\"}}",
        LoadMode::Append,
    )
    .await
    .unwrap();

    let external_data = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "external",
            "content": external_uri,
            "note": "branch insert",
        }
    })
    .to_string();
    feature
        .load("feature", &external_data, LoadMode::Append)
        .await
        .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let external = main
        .read_blob("Document", "external", "content")
        .await
        .unwrap();
    let external_bytes = external.read().await.unwrap();
    assert_eq!(&external_bytes[..], b"External");
}

#[tokio::test]
async fn branch_merge_applies_node_insert_to_main() {
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

    let outcome = feature.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut reopened = Omnigraph::open(uri).await.unwrap();
    let qr = query_main(
        &mut reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

#[tokio::test]
async fn branch_merge_records_single_latest_commit_with_two_parents() {
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

    let source_head_before = CommitGraph::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    let target_head_before = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let commit_graph = CommitGraph::open(uri).await.unwrap();
    let head = commit_graph.head_commit().await.unwrap().unwrap();
    let commits = commit_graph.load_commits().await.unwrap();
    let latest_manifest_version = commits.iter().map(|c| c.manifest_version).max().unwrap();
    let latest_commits: Vec<_> = commits
        .iter()
        .filter(|commit| commit.manifest_version == latest_manifest_version)
        .collect();

    assert_eq!(latest_commits.len(), 1);
    assert_eq!(head.manifest_version, latest_manifest_version);
    assert_eq!(
        head.parent_commit_id.as_deref(),
        Some(target_head_before.graph_commit_id.as_str())
    );
    assert_eq!(
        head.merged_parent_commit_id.as_deref(),
        Some(source_head_before.graph_commit_id.as_str())
    );
}

// ── P1: commit-DAG coherence on same-branch writes after an external commit ──
//
// `append_commit` takes a new commit's parent from the coordinator's in-memory
// head (commit_graph head_commit, zero storage read), but `commit_all` rebases
// the MANIFEST from a fresh coordinator. So after an external writer advances
// the branch, a same-branch write on a non-refreshed handle commits a fresh
// manifest version yet appends off the stale head — forking the commit DAG (the
// new commit and the external commit share a parent). Data is unaffected (the
// manifest is the visibility authority); only commit history is malformed.
// P1 refreshes the commit-graph head before the append, so the parent is the
// true current head. These two tests are RED before that fix, GREEN after.

/// Non-strict insert: the fork is pre-existing (commit_all rebases the manifest
/// regardless of the stale head), independent of Fix 1.
#[tokio::test]
async fn same_branch_insert_after_external_commit_is_linear() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // Handle A: a long-lived writer whose coordinator head stays pinned at the
    // load commit (C0) — it never refreshes before its own write below.
    let mut a = init_and_load(&dir).await;
    let c0 = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();

    // External writer B advances main: commit C1, parent C0.
    let mut b = Omnigraph::open(uri).await.unwrap();
    mutate_main(
        &mut b,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "ext_b")], &[("$age", 30)]),
    )
    .await
    .unwrap();
    let c1 = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        c1.parent_commit_id.as_deref(),
        Some(c0.graph_commit_id.as_str()),
        "sanity: B's commit C1 should descend from C0"
    );

    // A writes to main WITHOUT refreshing. A's coordinator still thinks the head
    // is C0, so a pre-fix append parents the new commit on C0 instead of C1.
    mutate_main(
        &mut a,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "local_a")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    let commits = CommitGraph::open(uri)
        .await
        .unwrap()
        .load_commits()
        .await
        .unwrap();
    let latest = commits.iter().max_by_key(|c| c.manifest_version).unwrap();
    assert_eq!(
        latest.parent_commit_id.as_deref(),
        Some(c1.graph_commit_id.as_str()),
        "A's same-branch write after an external commit must append off the true \
         head C1, not the stale head C0 (commit-DAG fork)"
    );
    let c0_children = commits
        .iter()
        .filter(|c| c.parent_commit_id.as_deref() == Some(c0.graph_commit_id.as_str()))
        .count();
    assert_eq!(
        c0_children, 1,
        "C0 must have exactly one child; two is the fork"
    );
}

/// Strict update after a read: Fix 1's `refresh_manifest_only` makes the read
/// freshen the read-time pin, defeating the strict 409 that used to force a
/// coherent refresh — so the same stale-head append forks strict ops too.
#[tokio::test]
async fn same_branch_update_after_external_commit_and_read_is_linear() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    // A inserts the row it will later update; this is A's own commit (Ca), so
    // A's coordinator head is Ca.
    let mut a = init_and_load(&dir).await;
    mutate_main(
        &mut a,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "target")], &[("$age", 40)]),
    )
    .await
    .unwrap();
    let ca = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();

    // External writer B advances main: commit Cb, parent Ca.
    let mut b = Omnigraph::open(uri).await.unwrap();
    mutate_main(
        &mut b,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "ext_b")], &[("$age", 30)]),
    )
    .await
    .unwrap();
    let cb = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cb.parent_commit_id.as_deref(),
        Some(ca.graph_commit_id.as_str())
    );

    // A reads main: the stale-probe path refreshes A's MANIFEST (via
    // refresh_manifest_only) but not its commit-graph head, freshening the
    // read-time pin so the strict update below skips its 409.
    query_main(&mut a, TEST_QUERIES, "total_people", &params(&[]))
        .await
        .unwrap();

    // Strict update, no explicit refresh: pre-fix it appends off the stale head
    // Ca instead of Cb.
    mutate_main(
        &mut a,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "target")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    let commits = CommitGraph::open(uri)
        .await
        .unwrap()
        .load_commits()
        .await
        .unwrap();
    let latest = commits.iter().max_by_key(|c| c.manifest_version).unwrap();
    assert_eq!(
        latest.parent_commit_id.as_deref(),
        Some(cb.graph_commit_id.as_str()),
        "a strict update after an external commit and a local read must append \
         off the true head Cb, not the stale head Ca"
    );
    let ca_children = commits
        .iter()
        .filter(|c| c.parent_commit_id.as_deref() == Some(ca.graph_commit_id.as_str()))
        .count();
    assert_eq!(
        ca_children, 1,
        "Ca must have exactly one child; two is the fork"
    );
}

#[tokio::test]
async fn branch_merge_records_actor_on_latest_commit() {
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

    let outcome = main
        .branch_merge_as("feature", "main", Some("act-ragnor"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let head = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.actor_id.as_deref(), Some("act-ragnor"));
}

#[tokio::test]
async fn already_up_to_date_branch_merge_returns_without_new_commit() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let source_head_before = CommitGraph::open_at_branch(uri, "feature")
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    let target_head_before = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        source_head_before.manifest_version,
        target_head_before.manifest_version
    );

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::AlreadyUpToDate);

    let commit_graph = CommitGraph::open(uri).await.unwrap();
    let head = commit_graph.head_commit().await.unwrap().unwrap();

    assert_eq!(head.manifest_version, target_head_before.manifest_version);
    assert_eq!(head.graph_commit_id, target_head_before.graph_commit_id);
    assert_eq!(head.graph_commit_id, source_head_before.graph_commit_id);
}

#[tokio::test]
async fn branch_merge_returns_merged_for_non_fast_forward_auto_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

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

    let bob = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap()
    .concat_batches()
    .unwrap();
    let bob_ages = bob.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(bob_ages.value(0), 26);

    let eve = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1);
}

#[tokio::test]
async fn branch_merge_allows_identical_updates_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let alice = query_main(
        &mut main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .concat_batches()
    .unwrap();
    let ages = alice
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ages.value(0), 31);
}

#[tokio::test]
async fn merged_rewritten_indexed_table_is_searchable_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_search_db(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        SEARCH_MUTATIONS,
        "set_doc_title",
        &params(&[("$slug", "ml-intro"), ("$title", "Orion ML Intro")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        SEARCH_MUTATIONS,
        "set_doc_title",
        &params(&[("$slug", "dl-basics"), ("$title", "Orion DL Basics")]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let result = query_main(
        &mut main,
        SEARCH_QUERIES,
        "text_search",
        &params(&[("$q", "Orion")]),
    )
    .await
    .unwrap();
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let values: Vec<&str> = (0..slugs.len()).map(|idx| slugs.value(idx)).collect();
    assert!(values.contains(&"ml-intro"));
    assert!(values.contains(&"dl-basics"));

    let ds = snapshot_main(&main)
        .await
        .unwrap()
        .open("node:Doc")
        .await
        .unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();
    assert_eq!(
        user_indices.len(),
        4,
        "expected rebuilt id BTree plus key-property and title/body indices after rewritten merge"
    );
}

#[tokio::test]
async fn explicit_target_reads_see_branch_local_writes_without_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut writer = Omnigraph::open(uri).await.unwrap();
    let mut reader = Omnigraph::open(uri).await.unwrap();
    let mut main_reader = Omnigraph::open(uri).await.unwrap();

    mutate_branch(
        &mut writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let visible = query_branch(
        &mut reader,
        "feature",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(visible.num_rows(), 1);

    let main_result = query_main(
        &mut main_reader,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(main_result.num_rows(), 0);
}

#[tokio::test]
async fn branch_created_from_non_main_inherits_branch_state() {
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
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();
    std::fs::remove_file(
        dir.path()
            .join("__manifest")
            .join("_refs")
            .join("branches")
            .join("experiment.json"),
    )
    .unwrap();
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .expect("non-main create must also reclaim a clone-only target");

    assert_eq!(
        feature.branch_list().await.unwrap(),
        vec!["main", "experiment", "feature"]
    );

    let mut experiment = Omnigraph::open(uri).await.unwrap();
    let qr = query_branch(
        &mut experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_qr = query_main(
        &mut reopened_main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(main_qr.num_rows(), 0);
}

#[tokio::test]
async fn ensure_indices_on_child_branch_keeps_inherited_table_when_no_work_is_needed() {
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
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    let mut experiment = Omnigraph::open(uri).await.unwrap();
    let experiment_inherited = snapshot_branch(&experiment, "experiment").await.unwrap();
    assert_eq!(
        experiment_inherited
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );

    experiment.ensure_indices_on("experiment").await.unwrap();

    let experiment_snap = snapshot_branch(&experiment, "experiment").await.unwrap();
    assert_eq!(
        experiment_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature"),
        "index reconciliation must not manufacture a ref-only first-touch effect"
    );
    assert_eq!(
        experiment_snap
            .entry("edge:Knows")
            .unwrap()
            .table_branch
            .as_deref(),
        None
    );

    let feature_snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        feature_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        5
    );
    assert_eq!(
        count_rows_branch(&experiment, "experiment", "node:Person").await,
        5
    );
}

#[tokio::test]
async fn branch_edge_only_write_only_branches_edge_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();
    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        snap.entry("node:Person").unwrap().table_branch.as_deref(),
        None
    );
    assert_eq!(
        snap.entry("edge:Knows").unwrap().table_branch.as_deref(),
        Some("feature")
    );
    assert_eq!(
        snap.entry("edge:WorksAt").unwrap().table_branch.as_deref(),
        None
    );

    let feature_qr = query_branch(
        &mut feature,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_qr = query_main(
        &mut reopened_main,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(main_qr.num_rows(), 2);
}

#[tokio::test]
async fn branch_merge_into_non_main_target_works() {
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
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "experiment").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let mut experiment = Omnigraph::open(uri).await.unwrap();
    let bob = query_branch(
        &mut experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();
    let bob_batch = bob.concat_batches().unwrap();
    let bob_ages = bob_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(bob_ages.value(0), 26);

    let eve = query_branch(
        &mut experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1);
    let experiment_snap = snapshot_branch(&experiment, "experiment").await.unwrap();
    assert_eq!(
        experiment_snap
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("experiment")
    );

    let mut reopened_main = Omnigraph::open(uri).await.unwrap();
    let main_bob = query_main(
        &mut reopened_main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();
    let main_batch = main_bob.concat_batches().unwrap();
    let main_ages = main_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(main_ages.value(0), 25);
}

#[tokio::test]
async fn branch_merge_reports_unique_violation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, UNIQUE_SCHEMA, UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Bob"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Carol"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap();

    let err = main.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "node:User"
                    && conflict.kind == MergeConflictKind::UniqueViolation
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

/// Regression for the MR-983 follow-up: the branch-merge path must enforce an
/// edge composite `@unique(src, dst)` as a true composite key, consistent with
/// the intake path. Two branches inserting the *same* (src, dst) pair must
/// conflict on merge.
#[tokio::test]
async fn branch_merge_reports_composite_unique_violation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, EDGE_UNIQUE_SCHEMA, EDGE_UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        EDGE_UNIQUE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        EDGE_UNIQUE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    let err = main.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "edge:Knows"
                    && conflict.kind == MergeConflictKind::UniqueViolation
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

/// Sibling to the above: pairs sharing `src` but differing on `dst` are unique
/// on the (src, dst) tuple and must merge cleanly. Guards against the composite
/// degrading back into a single-field `@unique(src)` on the merge path.
#[tokio::test]
async fn branch_merge_allows_distinct_composite_unique_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, EDGE_UNIQUE_SCHEMA, EDGE_UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        EDGE_UNIQUE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        EDGE_UNIQUE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Carol")]),
    )
    .await
    .unwrap();

    main.branch_merge("feature", "main")
        .await
        .expect("distinct (src, dst) pairs are unique on the composite and must merge cleanly");
    assert_eq!(count_rows(&main, "edge:Knows").await, 2);
}

#[tokio::test]
async fn branch_merge_reports_cardinality_violation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, CARDINALITY_SCHEMA, CARDINALITY_DATA).await;
    main.branch_create("feature").await.unwrap();

    let mut feature = Omnigraph::open(uri).await.unwrap();

    mutate_main(
        &mut main,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Acme")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &mut feature,
        "feature",
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Beta")]),
    )
    .await
    .unwrap();

    let err = main.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(conflicts.iter().any(|conflict| {
                conflict.table_key == "edge:WorksAt"
                    && conflict.kind == MergeConflictKind::CardinalityViolation
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

/// Fix C regression: a table adopted by pointer switch (`AdoptSourceState`)
/// must still be validated. Merging `main` -> `feature` where `feature` deleted
/// a node and `main` added an edge referencing it classifies the edge table as
/// `AdoptSourceState` (source on main, target on a branch). The unified
/// evaluator must see the adopted edge and reject the orphan; before the fix it
/// skipped the table entirely and silently published the dangling edge.
#[tokio::test]
async fn merge_main_into_branch_validates_adopted_edge_against_branch_node_delete() {
    const MUTATIONS: &str = r#"
query add_knows($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}

query delete_person($name: String) {
    delete Person where name = $name
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_db_from_schema_and_data(&dir, EDGE_UNIQUE_SCHEMA, EDGE_UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();

    // main (merge source): add an edge referencing Bob.
    mutate_main(
        &mut main,
        MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    // feature (merge target): delete Bob.
    mutate_branch(
        &mut feature,
        "feature",
        MUTATIONS,
        "delete_person",
        &params(&[("$name", "Bob")]),
    )
    .await
    .unwrap();

    // Merge main -> feature: edge:Knows is adopted by pointer switch
    // (AdoptSourceState). The adopted edge Alice->Bob references Bob, which the
    // target branch deleted, so the merge must reject with OrphanEdge.
    let err = feature
        .branch_merge("main", "feature")
        .await
        .expect_err("adopting main's edge into a branch that deleted its endpoint must conflict");
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(
                conflicts
                    .iter()
                    .any(|c| c.table_key == "edge:Knows"
                        && c.kind == MergeConflictKind::OrphanEdge),
                "expected OrphanEdge on edge:Knows, got {conflicts:?}"
            );
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

#[tokio::test]
async fn branch_api_rejects_reserved_main_and_same_source_target_merge() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let err = db.branch_create("main").await.unwrap_err();
    assert!(err.to_string().contains("cannot create branch 'main'"));

    let err = db.branch_delete("main").await.unwrap_err();
    assert!(err.to_string().contains("cannot delete branch 'main'"));

    let err = db.branch_create("bad branch").await.unwrap_err();
    assert!(err.to_string().contains("invalid") || err.to_string().contains("allowed"));
    assert!(
        !dir.path()
            .join("__manifest")
            .join("tree")
            .join("bad branch")
            .exists(),
        "branch names must be validated before Lance's shallow-clone phase"
    );

    let err = db.branch_merge("main", "main").await.unwrap_err();
    assert!(err.to_string().contains("distinct source and target"));

    db.branch_create("feature").await.unwrap();
    db.sync_branch("feature").await.unwrap();
    let err = db.branch_delete("feature").await.unwrap_err();
    assert!(err.to_string().contains("currently active branch"));
}

#[tokio::test]
async fn branch_delete_removes_owned_table_branches_and_allows_recreate() {
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

    main.branch_delete("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main"]);

    main.branch_create("feature").await.unwrap();
    mutate_branch(
        &mut main,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();

    assert_eq!(count_rows_branch(&main, "feature", "node:Person").await, 5);
}

#[tokio::test]
async fn branch_namespace_rejects_live_physical_path_prefix_collisions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    let err = db.branch_create("feature/child").await.unwrap_err();
    assert!(
        err.to_string().contains("physical Lance path")
            && err.to_string().contains("ancestors or descendants"),
        "prefix collision must be actionable; got: {err}"
    );
    assert!(
        !dir.path()
            .join("__manifest")
            .join("tree")
            .join("feature")
            .join("child")
            .exists(),
        "prefix admission must reject before Lance creates the target clone"
    );

    db.branch_delete("feature").await.unwrap();
    db.branch_create("feature/child").await.unwrap();
    let err = db.branch_create("feature").await.unwrap_err();
    assert!(
        err.to_string().contains("physical Lance path")
            && err.to_string().contains("feature/child"),
        "ancestor creation must reject the inverse prefix collision; got: {err}"
    );
    assert!(
        !lance::Dataset::open(&format!("{}/__manifest", dir.path().display()))
            .await
            .unwrap()
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "inverse admission refusal must not create an ancestor ref"
    );
}

#[tokio::test]
async fn branch_delete_refuses_legacy_physical_path_children() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = init_and_load(&dir).await;
    db.branch_create("feature/child").await.unwrap();

    // Forge a graph written before the prefix-disjoint namespace invariant.
    // Lance permits both refs but intentionally cannot reclaim the ancestor's
    // dataset directory while the child path is live.
    let mut manifest = lance::Dataset::open(&format!("{uri}/__manifest"))
        .await
        .unwrap();
    let version = manifest.version().version;
    manifest
        .create_branch("feature", version, None)
        .await
        .unwrap();

    let err = db.branch_delete("feature").await.unwrap_err();
    assert!(
        err.to_string().contains("feature/child")
            && err.to_string().contains("delete the child branch first"),
        "legacy prefix collisions must be deleted leaf-first; got: {err}"
    );
    assert!(
        manifest
            .list_branches()
            .await
            .unwrap()
            .contains_key("feature"),
        "refusal must not remove ancestor authority"
    );

    db.branch_delete("feature/child").await.unwrap();
    db.branch_delete("feature").await.unwrap();
    assert_eq!(
        db.branch_list().await.unwrap(),
        vec!["main"],
        "legacy overlap must converge when deleted leaf-first"
    );
}

#[tokio::test]
async fn branch_delete_rejects_branches_still_referenced_by_descendants() {
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
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    let err = main.branch_delete("feature").await.unwrap_err();
    assert!(err.to_string().contains("still depends on it"));
}

// ─── Step 9b: Surgical merge publish tests ──────────────────────────────────

#[tokio::test]
async fn merged_table_preserves_row_version_for_unchanged_rows() {
    // After a non-FF merge, unchanged rows retain their original _row_created_at_version.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();

    // Main updates Bob's age → changes one row
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve → adds one row
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

    // After merge: scan node:Person with _row_created_at_version
    let snap = snapshot_main(&main).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    let mut scanner = ds.scan();
    scanner.project(&["id", "_row_created_at_version"]).unwrap();
    let batches: Vec<_> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    // Collect _row_created_at_version for each person
    let mut version_by_id: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let versions = batch
            .column_by_name("_row_created_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..ids.len() {
            version_by_id.insert(ids.value(i).to_string(), versions.value(i));
        }
    }

    // The key assertion: NOT all rows have the same _row_created_at_version.
    // With truncate+append, all rows would be re-stamped to the merge version.
    // With surgical merge_insert, unchanged rows keep their original version.
    let unique_versions: std::collections::HashSet<u64> = version_by_id.values().copied().collect();
    assert!(
        unique_versions.len() > 1,
        "After surgical merge, rows should have different _row_created_at_version values \
         (original rows keep old version, merged-in rows get new version). \
         Got only {:?} for ids {:?}",
        unique_versions,
        version_by_id
    );
}

#[tokio::test]
async fn edge_tables_have_id_btree_after_ensure_indices() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;
    db.ensure_indices().await.unwrap();

    let snap = snapshot_main(&db).await.unwrap();
    let ds = snap.open("edge:Knows").await.unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();

    // Should have BTree on id, src, dst = 3 indices
    let index_names: Vec<_> = user_indices.iter().map(|idx| idx.fields.clone()).collect();
    assert!(
        user_indices.len() >= 3,
        "Edge table should have at least 3 indices (id, src, dst), got {:?}",
        index_names
    );
}

#[tokio::test]
async fn merge_delta_only_bumps_changed_rows() {
    // After a non-FF merge, unchanged rows should NOT have _row_last_updated_at_version
    // bumped. Only rows that were actually modified should get new version stamps.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();

    main.branch_create("feature").await.unwrap();
    let mut feature = Omnigraph::open(uri).await.unwrap();

    // Main updates Bob's age → changes one Person row
    mutate_main(
        &mut main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve → adds one Person row (makes it non-FF)
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

    // Scan all persons with _row_last_updated_at_version
    let snap = snapshot_main(&main).await.unwrap();
    let ds = snap.open("node:Person").await.unwrap();
    let mut scanner = ds.scan();
    scanner
        .project(&["id", "_row_last_updated_at_version"])
        .unwrap();
    let batches: Vec<_> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    // Collect all _row_last_updated_at_version values
    let mut versions: Vec<u64> = Vec::new();
    for batch in &batches {
        let v = batch
            .column_by_name("_row_last_updated_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..v.len() {
            versions.push(v.value(i));
        }
    }

    // Not all rows should have the same version — unchanged rows keep old version
    let unique_versions: std::collections::HashSet<u64> = versions.iter().copied().collect();
    assert!(
        unique_versions.len() > 1,
        "After surgical merge, rows should have different _row_last_updated_at_version values. \
         Unchanged rows should keep old version, changed rows get new version. \
         Got only {:?}",
        unique_versions
    );
}
