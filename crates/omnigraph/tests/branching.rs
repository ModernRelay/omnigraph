mod helpers;

use std::fmt::Write as _;
use std::fs;
use std::io::Write;

use arrow_array::{Array, Int32Array, StringArray, StructArray, UInt64Array};
use futures::TryStreamExt;
use lance::Dataset;
use lance_index::is_system_index;

use omnigraph::db::commit_graph::CommitGraph;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::error::{MergeConflictKind, OmniError};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};

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

const MULTI_TABLE_EXTERNAL_BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
    note: String?
}

node Asset {
    name: String @key
    payload: Blob?
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

const WIDE_BLOB_SCHEMA: &str = r#"
node Document {
    title: String @key
    first: Blob?
    second: Blob?
}

node Asset {
    name: String @key
    payload: Blob?
}
"#;

fn write_sized_external_blob(path: &std::path::Path, bytes: u64) {
    const BLOCK_BYTES: usize = 1024 * 1024;

    let mut file = fs::File::create(path).unwrap();
    let block = vec![0x5a_u8; BLOCK_BYTES];
    let mut remaining = bytes;
    while remaining > 0 {
        let write = remaining.min(BLOCK_BYTES as u64) as usize;
        file.write_all(&block[..write]).unwrap();
        remaining -= write as u64;
    }
    file.flush().unwrap();
}

async fn init_search_db(dir: &tempfile::TempDir) -> Omnigraph {
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap();
    load_jsonl(&db, SEARCH_DATA, LoadMode::Overwrite)
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
    let db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&db, data, LoadMode::Overwrite).await.unwrap();
    db
}

async fn assert_exact_id_primary_key_on_branch(db: &Omnigraph, branch: &str, table_key: &str) {
    let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await.unwrap();
    let dataset = snapshot.open(table_key).await.unwrap();
    let primary_key = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        primary_key,
        ["id"],
        "branch {branch} table {table_key} must preserve exactly `id` as its Lance unenforced primary key"
    );
}

#[tokio::test]
async fn branch_create_open_list_and_lazy_branching_work() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
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
    assert_exact_id_primary_key_on_branch(&feature, "feature", "node:Person").await;

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
    assert_exact_id_primary_key_on_branch(&feature, "feature", "node:Person").await;

    let main = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows(&main, "node:Person").await, 4);
}

#[tokio::test]
async fn explicit_target_query_reads_multiple_branches_from_one_handle() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

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
    let db = init_and_load(&dir).await;

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
        &main,
        concat!(
            "{\"type\":\"Document\",\"data\":{\"title\":\"seed\",\"content\":\"base64:\",\"note\":\"original\"}}\n",
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
    assert!(
        seed_bytes.is_empty(),
        "a valid empty Blob must survive branch rewrite and merge"
    );

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
    let external_path = external_dir.path().join("external~source.txt");
    fs::write(&external_path, b"External").unwrap();
    let external_uri = url::Url::from_file_path(&external_path)
        .expect("external blob path is absolute")
        .to_string();
    let base_uri = url::Url::from_directory_path(external_dir.path())
        .expect("external blob base is absolute")
        .to_string();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(base_uri, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();
    let encoded_alias = external_uri.replace("~source", "%7Esource");
    assert_ne!(encoded_alias, external_uri);

    let setup = Omnigraph::init(uri, MULTI_TABLE_EXTERNAL_BLOB_SCHEMA)
        .await
        .unwrap()
        .with_external_blob_policy(policy.clone())
        .unwrap();
    let converged_base = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "converged",
            "content": external_uri.clone(),
            "note": "base",
        }
    })
    .to_string();
    setup
        .load("main", &converged_base, LoadMode::Overwrite)
        .await
        .unwrap();
    setup.branch_create("feature").await.unwrap();

    let feature = Omnigraph::open(uri)
        .await
        .unwrap()
        .with_external_blob_policy(policy.clone())
        .unwrap();
    let configured_main = Omnigraph::open(uri)
        .await
        .unwrap()
        .with_external_blob_policy(policy.clone())
        .unwrap();
    let target_data = format!(
        "{}\n{}",
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "main-doc",
                "content": "base64:TWFpbg==",
                "note": "main",
            }
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "converged",
                "content": external_uri.clone(),
                "note": "same on both",
            }
        })
    );
    configured_main
        .load("main", &target_data, LoadMode::Overwrite)
        .await
        .unwrap();
    let main = Omnigraph::open(uri).await.unwrap();

    let external_data = format!(
        "{}\n{}\n{}\n{}",
        serde_json::json!({
            "type": "Asset",
            "data": {
                "name": "external-asset",
                "payload": external_uri.clone(),
            }
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "external",
                "content": encoded_alias.clone(),
                "note": "branch insert",
            }
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "external-two",
                "content": external_uri.clone(),
                "note": "same-table normalized alias",
            }
        }),
        serde_json::json!({
            "type": "Document",
            "data": {
                "title": "converged",
                "content": external_uri.clone(),
                "note": "same on both",
            }
        })
    );
    feature
        .load("feature", &external_data, LoadMode::Overwrite)
        .await
        .unwrap();

    let source_snapshot = snapshot_branch(&feature, "feature").await.unwrap();
    let source_dataset = source_snapshot.open("node:Document").await.unwrap();
    let mut source_scan = source_dataset.scan();
    source_scan.blob_handling(lance::datatypes::BlobHandling::BlobsDescriptions);
    let source_batches: Vec<arrow_array::RecordBatch> = source_scan
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let source_descriptors = source_batches[0]
        .column_by_name("content")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let source_uris = source_descriptors
        .column_by_name("blob_uri")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        source_uris.value(0),
        encoded_alias,
        "precondition: source branch must retain the external descriptor"
    );

    let before = snapshot_main(&main).await.unwrap();
    let before_manifest = before.version();
    let entry = before.entry("node:Document").unwrap();
    let before_table = entry.table_version;
    let table_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;
    let before_commits = main.list_commits(Some("main")).await.unwrap().len();
    let before_source_table = snapshot_branch(&main, "feature")
        .await
        .unwrap()
        .entry("node:Document")
        .unwrap()
        .table_version;

    let error = main.branch_merge("feature", "main").await.unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ExternalBlobPolicy { ref uri, .. } if uri == &external_uri
        ),
        "default-deny merge must return typed ExternalBlobPolicy, got {error:?}"
    );
    let after = snapshot_main(&main).await.unwrap();
    assert_eq!(
        after.version(),
        before_manifest,
        "denied merge moved manifest"
    );
    assert_eq!(
        after.entry("node:Document").unwrap().table_version,
        before_table,
        "denied merge moved the target table pointer"
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head,
        "denied merge moved target Lance HEAD"
    );
    assert_eq!(
        main.list_commits(Some("main")).await.unwrap().len(),
        before_commits,
        "denied merge moved target lineage"
    );
    assert_eq!(
        snapshot_branch(&main, "feature")
            .await
            .unwrap()
            .entry("node:Document")
            .unwrap()
            .table_version,
        before_source_table,
        "denied merge moved the source table pointer"
    );
    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "denied merge must fail before recovery is armed"
    );

    let main = main.with_external_blob_policy(policy.clone()).unwrap();
    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        probes.external_blob_probe_inputs(),
        3,
        "every selected URI cell across both tables must join the merge-wide external preflight"
    );
    assert_eq!(
        probes.external_blob_probe_calls(),
        1,
        "normalized-equivalent URIs across tables must cause one HEAD"
    );
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        2,
        "equivalent aliases must share one payload GET in the Document chunk; the separate Asset chunk owns one bounded GET"
    );

    let external = main
        .read_blob("Document", "external", "content")
        .await
        .unwrap();
    assert_eq!(
        external.uri(),
        None,
        "branch merge must materialize an authorized source descriptor"
    );
    let external_bytes = external.read().await.unwrap();
    assert_eq!(&external_bytes[..], b"External");
    let asset = main
        .read_blob("Asset", "external-asset", "payload")
        .await
        .unwrap();
    assert_eq!(
        asset.uri(),
        None,
        "every row-writing table must materialize its authorized descriptor"
    );
    assert_eq!(&asset.read().await.unwrap()[..], b"External");
    let external_two = main
        .read_blob("Document", "external-two", "content")
        .await
        .unwrap();
    assert_eq!(external_two.uri(), None);
    assert_eq!(&external_two.read().await.unwrap()[..], b"External");
    let converged = main
        .read_blob("Document", "converged", "content")
        .await
        .unwrap();
    assert_eq!(
        converged.uri(),
        Some(external_uri.as_str()),
        "the descriptor-only decision walk and row-staging walk must both keep the identical target row out of the delta"
    );

    // A pointer-only main -> named-branch adoption is not new ingress and does
    // not write a row. It must preserve the already-stored descriptor without
    // policy approval or source I/O, even when the caller-owned target has
    // disappeared.
    main.branch_create("pointer-target").await.unwrap();
    let pointer_path = external_dir.path().join("pointer-only.txt");
    fs::write(&pointer_path, b"Pointer only").unwrap();
    let pointer_uri = url::Url::from_file_path(&pointer_path)
        .expect("pointer-only external blob path is absolute")
        .to_string();
    let pointer_data = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "pointer-only",
            "content": pointer_uri.clone(),
            "note": "main source",
        }
    })
    .to_string();
    main.load("main", &pointer_data, LoadMode::Overwrite)
        .await
        .unwrap();
    let source_entry = snapshot_main(&main)
        .await
        .unwrap()
        .entry("node:Document")
        .unwrap()
        .clone();
    assert_eq!(
        main.read_blob("Document", "pointer-only", "content")
            .await
            .unwrap()
            .uri(),
        Some(pointer_uri.as_str())
    );
    fs::remove_file(&pointer_path).unwrap();

    let deny = Omnigraph::open(uri).await.unwrap();
    let outcome = deny.branch_merge("main", "pointer-target").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    let target_entry = snapshot_branch(&deny, "pointer-target")
        .await
        .unwrap()
        .entry("node:Document")
        .unwrap()
        .clone();
    // `table_path` is identity-derived, so an exact path/version/branch match
    // proves this was pointer adoption rather than a rewritten table effect.
    assert_eq!(target_entry.table_key, source_entry.table_key);
    assert_eq!(target_entry.table_path, source_entry.table_path);
    assert_eq!(target_entry.table_version, source_entry.table_version);
    assert_eq!(target_entry.table_branch, source_entry.table_branch);
}

/// Blob descriptors are tiny even when their referenced payload is not. The
/// merge planner must account from `BlobFile::size()` before reading bytes,
/// both for one over-limit cell and for several individually-valid cells whose
/// row total exceeds 32 MiB. Both failures are entirely pre-effect.
#[tokio::test]
async fn branch_merge_rejects_oversized_blob_payloads_pre_effect() {
    const LIMIT: u64 = 32 * 1024 * 1024;

    for (case, first_bytes, second_bytes, split_across_tables, expected_actual) in [
        ("single", LIMIT + 1, None, false, LIMIT + 1),
        (
            "cumulative",
            LIMIT / 2 + 1,
            Some(LIMIT / 2 + 1),
            false,
            LIMIT + 2,
        ),
        (
            "cross-table",
            LIMIT / 2 + 1,
            Some(LIMIT / 2 + 1),
            true,
            LIMIT + 2,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let graph_path = dir.path().join("graph");
        let graph_uri = graph_path.to_str().unwrap();
        let first_path = dir.path().join("first.blob");
        write_sized_external_blob(&first_path, first_bytes);
        let first_uri = format!("file://{}", first_path.display());

        let mut wide_data = serde_json::Map::new();
        wide_data.insert(
            "title".to_string(),
            serde_json::Value::String(format!("wide-{case}")),
        );
        wide_data.insert("first".to_string(), serde_json::Value::String(first_uri));
        let second_uri = if let Some(second_bytes) = second_bytes {
            let second_path = dir.path().join("second.blob");
            write_sized_external_blob(&second_path, second_bytes);
            let uri = format!("file://{}", second_path.display());
            if !split_across_tables {
                wide_data.insert("second".to_string(), serde_json::Value::String(uri.clone()));
            }
            Some(uri)
        } else {
            None
        };
        let document_row = serde_json::json!({
            "type": "Document",
            "data": serde_json::Value::Object(wide_data),
        })
        .to_string();
        let selected_rows = if split_across_tables {
            format!(
                "{document_row}\n{}",
                serde_json::json!({
                    "type": "Asset",
                    "data": {
                        "name": "wide-asset",
                        "payload": second_uri.expect("cross-table case has second payload"),
                    }
                })
            )
        } else {
            document_row
        };

        let base_uri = url::Url::from_directory_path(dir.path())
            .expect("external blob base is absolute")
            .to_string();
        let policy = ExternalBlobPolicy::allow(vec![
            ExternalBlobBase::new(base_uri, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
        ])
        .unwrap();
        let db = Omnigraph::init(graph_uri, WIDE_BLOB_SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap();
        let base = r#"{"type":"Document","data":{"title":"base"}}"#;
        load_jsonl(&db, base, LoadMode::Overwrite).await.unwrap();
        db.branch_create("feature").await.unwrap();
        db.load(
            "feature",
            &format!("{base}\n{selected_rows}"),
            LoadMode::Overwrite,
        )
        .await
        .unwrap();

        let before = snapshot_main(&db).await.unwrap();
        let before_manifest = before.version();
        let mut before_tables = Vec::new();
        for table_key in ["node:Document", "node:Asset"] {
            let entry = before.entry(table_key).unwrap();
            let table_uri = format!(
                "{}/{}",
                db.uri().trim_end_matches('/'),
                entry.table_path.trim_start_matches('/')
            );
            before_tables.push((
                table_key,
                entry.table_version,
                Dataset::open(&table_uri).await.unwrap().version().version,
                table_uri,
            ));
        }
        let before_commits = db.list_commits(Some("main")).await.unwrap().len();

        let probes = MergeWriteProbes::default();
        let error = with_merge_write_probes(probes.clone(), db.branch_merge("feature", "main"))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                OmniError::ResourceLimitExceeded {
                    ref resource,
                    limit: LIMIT,
                    actual,
                } if resource == "materialized blob payload bytes"
                    && actual == expected_actual
            ),
            "{case}: oversized blob merge must return the typed pre-read limit, got {error:?}"
        );
        assert_eq!(
            probes.external_blob_payload_read_calls(),
            0,
            "{case}: external overflow must be rejected before payload GET"
        );
        assert_eq!(
            probes.blob_payload_read_calls(),
            0,
            "{case}: overflow must be rejected before any Blob payload read"
        );
        let after = snapshot_main(&db).await.unwrap();
        assert_eq!(after.version(), before_manifest, "{case}: manifest moved");
        for (table_key, before_table, before_head, table_uri) in before_tables {
            assert_eq!(
                after.entry(table_key).unwrap().table_version,
                before_table,
                "{case}: {table_key} main table pointer moved"
            );
            assert_eq!(
                Dataset::open(&table_uri).await.unwrap().version().version,
                before_head,
                "{case}: {table_key} main Lance HEAD moved"
            );
        }
        assert_eq!(count_rows(&db, "node:Document").await, 1);
        assert_eq!(count_rows(&db, "node:Asset").await, 0);
        assert_eq!(
            db.list_commits(Some("main")).await.unwrap().len(),
            before_commits,
            "{case}: main lineage moved"
        );
        let recovery_dir = graph_path.join("__recovery");
        assert!(
            !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
            "{case}: pre-effect rejection must not leave a recovery sidecar"
        );
    }

    // Managed descriptors need no external HEAD, but their carried payload is
    // still one operation-wide merge budget. Load each row in its own valid
    // source operation so only the merge's aggregate can exceed the ceiling.
    let dir = tempfile::tempdir().unwrap();
    let graph_path = dir.path().join("managed-aggregate-graph");
    let graph_uri = graph_path.to_str().unwrap();
    let db = Omnigraph::init(graph_uri, WIDE_BLOB_SCHEMA).await.unwrap();
    let base = r#"{"type":"Document","data":{"title":"base"}}"#;
    load_jsonl(&db, base, LoadMode::Overwrite).await.unwrap();
    db.branch_create("feature").await.unwrap();
    let payload_bytes = LIMIT / 2 + 1;
    for index in 0..2 {
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            vec![0x5a_u8; payload_bytes as usize],
        );
        let row = serde_json::json!({
            "type": "Document",
            "data": {
                "title": format!("managed-{index}"),
                "first": format!("base64:{encoded}"),
            }
        })
        .to_string();
        db.load("feature", &row, LoadMode::Append).await.unwrap();
    }

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
    let before_commits = db.list_commits(Some("main")).await.unwrap().len();
    let probes = MergeWriteProbes::default();
    let error = with_merge_write_probes(probes.clone(), db.branch_merge("feature", "main"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: LIMIT,
                actual,
            } if resource == "materialized blob payload bytes" && actual == LIMIT + 2
        ),
        "managed aggregate must return the typed pre-read operation limit, got {error:?}"
    );
    assert_eq!(probes.external_blob_probe_calls(), 0);
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        0,
        "managed aggregate refusal must not perform an external payload GET"
    );
    assert_eq!(
        probes.blob_payload_read_calls(),
        0,
        "managed aggregate refusal must happen from descriptor lengths before payload reads"
    );
    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(after.version(), before_manifest);
    assert_eq!(
        after.entry("node:Document").unwrap().table_version,
        before_table
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head
    );
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        before_commits
    );
    let recovery_dir = graph_path.join("__recovery");
    assert!(!recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none());

    // The descriptor pass owns one operation-wide cell bound. Assemble a
    // source image with two separate Overwrites that are each legal on their
    // own, then prove the merge refuses their 8,193 selected external cells
    // before the first external HEAD, payload GET, recovery arm, or target
    // effect.
    const REFERENCE_LIMIT: usize = 8192;
    let dir = tempfile::tempdir().unwrap();
    let graph_path = dir.path().join("external-cell-aggregate-graph");
    let external_path = dir.path().join("shared-external.blob");
    fs::write(&external_path, b"x").unwrap();
    let external_uri = url::Url::from_file_path(&external_path)
        .expect("external source path is absolute")
        .to_string();
    let base_uri = url::Url::from_directory_path(dir.path())
        .expect("external source base is absolute")
        .to_string();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(base_uri, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();
    let db = Omnigraph::init(graph_path.to_str().unwrap(), WIDE_BLOB_SCHEMA)
        .await
        .unwrap()
        .with_external_blob_policy(policy)
        .unwrap();
    db.branch_create("feature").await.unwrap();

    let external_rows = |table: &str, key: &str, blob: &str, rows: usize, prefix: &str| {
        let mut data = String::with_capacity(rows * (external_uri.len() + 96));
        for row in 0..rows {
            let mut values = serde_json::Map::new();
            values.insert(
                key.to_string(),
                serde_json::Value::String(format!("{prefix}-{row}")),
            );
            values.insert(
                blob.to_string(),
                serde_json::Value::String(external_uri.clone()),
            );
            writeln!(
                data,
                "{}",
                serde_json::json!({"type": table, "data": values})
            )
            .unwrap();
        }
        data
    };
    for (table, key, blob, rows, prefix) in [
        (
            "Document",
            "title",
            "first",
            REFERENCE_LIMIT / 2,
            "document",
        ),
        ("Asset", "name", "payload", REFERENCE_LIMIT / 2 + 1, "asset"),
    ] {
        let input = external_rows(table, key, blob, rows, prefix);
        let source_probes = MergeWriteProbes::default();
        with_merge_write_probes(
            source_probes.clone(),
            db.load("feature", &input, LoadMode::Overwrite),
        )
        .await
        .unwrap_or_else(|error| panic!("{table} source Overwrite must be legal: {error:?}"));
        assert_eq!(source_probes.external_blob_probe_inputs(), rows as u64);
        assert_eq!(source_probes.external_blob_probe_calls(), 1);
        assert_eq!(source_probes.external_blob_payload_read_calls(), 0);
    }
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Document").await,
        REFERENCE_LIMIT / 2
    );
    assert_eq!(
        count_rows_branch(&db, "feature", "node:Asset").await,
        REFERENCE_LIMIT / 2 + 1
    );

    let before = snapshot_main(&db).await.unwrap();
    let before_manifest = before.version();
    let source_before = snapshot_branch(&db, "feature").await.unwrap();
    let mut before_tables = Vec::new();
    for table_key in ["node:Document", "node:Asset"] {
        let entry = before.entry(table_key).unwrap();
        let table_uri = format!(
            "{}/{}",
            db.uri().trim_end_matches('/'),
            entry.table_path.trim_start_matches('/')
        );
        before_tables.push((
            table_key,
            entry.table_version,
            Dataset::open(&table_uri).await.unwrap().version().version,
            source_before.entry(table_key).unwrap().table_version,
            table_uri,
        ));
    }
    let before_commits = db.list_commits(Some("main")).await.unwrap().len();
    let merge_probes = MergeWriteProbes::default();
    let error = with_merge_write_probes(merge_probes.clone(), db.branch_merge("feature", "main"))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit,
            actual,
        } if resource == "external Blob reference cells"
            && limit == REFERENCE_LIMIT as u64
            && actual == REFERENCE_LIMIT as u64 + 1
    ));
    assert_eq!(merge_probes.external_blob_probe_inputs(), 0);
    assert_eq!(merge_probes.external_blob_probe_calls(), 0);
    assert_eq!(merge_probes.external_blob_payload_read_calls(), 0);
    assert_eq!(merge_probes.blob_payload_read_calls(), 0);

    let after = snapshot_main(&db).await.unwrap();
    let source_after = snapshot_branch(&db, "feature").await.unwrap();
    assert_eq!(after.version(), before_manifest);
    for (table_key, before_table, before_head, before_source, table_uri) in before_tables {
        assert_eq!(after.entry(table_key).unwrap().table_version, before_table);
        assert_eq!(
            Dataset::open(&table_uri).await.unwrap().version().version,
            before_head
        );
        assert_eq!(
            source_after.entry(table_key).unwrap().table_version,
            before_source,
            "refused merge must not move the source table pointer"
        );
    }
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        before_commits
    );
    let recovery_dir = graph_path.join("__recovery");
    assert!(!recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none());
}

#[tokio::test]
async fn branch_merge_applies_node_insert_to_main() {
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

/// Strict update after a read: the stale read refreshes the manifest but leaves
/// the derived lineage cache warm. The following write must prefer the exact
/// head from that refreshed manifest, or it can parent its commit on the old
/// cached head even though it planned from fresh rows.
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

    // A reads main: the stale-probe path refreshes A's exact manifest head and
    // table pins while deliberately leaving the derived lineage cache warm.
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
    let main = init_and_load(&dir).await;
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
    let main = init_and_load(&dir).await;
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
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();

    let experiment = Omnigraph::open(uri).await.unwrap();
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

    // A lazy descendant retains its ancestor's physical Lance ref. Native
    // branch controls must fence main-only Phase-A enrollment without assuming
    // the logical source name is also the resolved physical ref.
    experiment
        .branch_create_from(ReadTarget::branch("experiment"), "grandchild")
        .await
        .unwrap();
    let grandchild = snapshot_branch(&experiment, "grandchild").await.unwrap();
    assert_eq!(
        grandchild
            .entry("node:Person")
            .unwrap()
            .table_branch
            .as_deref(),
        Some("feature")
    );
    experiment.branch_delete("grandchild").await.unwrap();
    experiment.branch_delete("experiment").await.unwrap();
}

#[tokio::test]
async fn branch_edge_only_write_only_branches_edge_table() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
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
    let db = init_and_load(&dir).await;

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
    let db = init_and_load(&dir).await;

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
    let db = init_and_load(&dir).await;
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
    let db = init_and_load(&dir).await;
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
