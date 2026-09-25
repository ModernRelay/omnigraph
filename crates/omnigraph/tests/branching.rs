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
use omnigraph::error::{ManifestErrorKind, MergeConflictKind, OmniError};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use omnigraph::{
    BLOB_READ_RANGE_MAX_BYTES, BlobContent, ExternalBlobBase, ExternalBlobExecutionScope,
    ExternalBlobPolicy, Session,
};

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
    @unique(@src, @dst)
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

query delete_doc($title: String) {
    delete Document where title = $title
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

async fn init_search_db(dir: &tempfile::TempDir) -> Session {
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, SEARCH_SCHEMA).await.unwrap());
    db.load_jsonl(SEARCH_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    db.ensure_indices().await.unwrap();
    db
}

async fn init_db_from_schema_and_data(
    dir: &tempfile::TempDir,
    schema: &str,
    data: &str,
) -> Session {
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, schema).await.unwrap());
    db.load_jsonl(data, LoadMode::Overwrite).await.unwrap();
    db
}

async fn assert_exact_id_primary_key_on_branch(db: &Omnigraph, branch: &str, table_key: &str) {
    let snapshot = db.snapshot_of(ReadTarget::branch(branch)).await.unwrap();
    let dataset = snapshot.open_dataset(table_key).await.unwrap();
    let primary_key = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        primary_key,
        ["__id"],
        "branch {branch} table {table_key} must preserve exactly `__id` as its Lance unenforced primary key"
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
        .dataset("node:Person")
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
            .join(format!(
                "{}.json",
                graph_native_ref(dir.path().to_str().unwrap(), "feature").await
            )),
    )
    .unwrap();
    main.branch_create("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main", "feature"]);

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        4
    );
    let initial_feature_snap = snapshot_branch(&feature, "feature").await.unwrap();
    let inherited_person = initial_feature_snap.dataset("node:Person").unwrap();
    assert_eq!(
        inherited_person.dataset_path, main_person.dataset_path,
        "branch creation must inherit the source table identity/path"
    );
    assert_eq!(
        inherited_person.published_dataset_version,
        main_person.published_dataset_version
    );
    assert_eq!(inherited_person.native_dataset_branch.as_deref(), None);
    assert_exact_id_primary_key_on_branch(&feature, "feature", "node:Person").await;

    mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        snap.dataset("node:Person").unwrap().dataset_path,
        main_person.dataset_path,
        "the first branch write must preserve the logical table identity/path"
    );
    assert_eq!(
        snap.dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        None,
        "a branch write stages on the inherited dataset without forking it"
    );
    let feature_pin = pinned_version(&feature, "feature", "node:Person").await;
    assert!(is_detached_version(feature_pin), "{feature_pin}");
    assert_ne!(
        feature_pin,
        pinned_version(&feature, "main", "node:Person").await,
        "the branch write stages a new detached pin"
    );
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        5
    );
    assert_eq!(
        snap.dataset("edge:Knows")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        None
    );
    assert_eq!(
        pinned_version(&feature, "feature", "edge:Knows").await,
        pinned_version(&feature, "main", "edge:Knows").await,
        "an unwritten table keeps the inherited pin"
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
    let db = init_and_load(&dir).await;

    let snapshot_id = db.resolve_snapshot("main").await.unwrap();
    mutate_main(
        &db,
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
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let feature_qr = query_branch(
        &feature,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let main_before = query_main(
        &main,
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
        &main,
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
    let main = helpers::session(Omnigraph::init(uri, BLOB_SCHEMA).await.unwrap());
    main.load_jsonl(
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
        .open_dataset("node:Document")
        .await
        .unwrap();
    let indices = ds.load_indices().await.unwrap();
    assert!(
        indices.iter().all(is_system_index),
        "blob correctness regression requires an index-absent table"
    );

    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_main(
        &main,
        BLOB_MUTATIONS,
        "update_doc_note",
        &params(&[("$title", "main-doc"), ("$note", "updated on main")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
        &feature,
        "feature",
        BLOB_MUTATIONS,
        "update_doc_note",
        &params(&[("$title", "seed"), ("$note", "updated on branch")]),
    )
    .await
    .unwrap();

    // Keep one out-of-line payload lazy until after the source branch tree is
    // reclaimed. A returned reader is pinned and can never retarget, but it is
    // not a durable lease over destructive branch deletion.
    let deletion_payload = vec![0x5a_u8; BLOB_READ_RANGE_MAX_BYTES as usize + 1];
    let deletion_encoded = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &deletion_payload,
    );
    let deletion_value = format!("base64:{deletion_encoded}");
    mutate_branch(
        &feature,
        "feature",
        BLOB_MUTATIONS,
        "insert_doc",
        &params(&[
            ("$title", "delete-boundary"),
            ("$content", deletion_value.as_str()),
            ("$note", "unread before delete"),
        ]),
    )
    .await
    .unwrap();
    let deletion_read = feature
        .read_blob_at(
            ReadTarget::branch("feature"),
            node_blob_cell("Document", "delete-boundary", "content"),
        )
        .await
        .unwrap();
    let BlobContent::Managed {
        reader: deletion_reader,
        ..
    } = deletion_read.content
    else {
        panic!("expected managed branch-deletion fixture")
    };

    let readme_cell = node_blob_cell("Document", "readme", "content");
    let feature_read = feature
        .read_blob_at(ReadTarget::branch("feature"), readme_cell.clone())
        .await
        .unwrap();
    let BlobContent::Managed {
        reader: feature_reader,
        ..
    } = feature_read.content
    else {
        panic!("expected managed feature-branch Blob")
    };
    assert_eq!(
        &feature_reader.read_range(0..5).await.unwrap()[..],
        b"Hello"
    );

    // A fresh child has no materialized graph-head row and inherits this table
    // from the named parent. Blob admission must accept its effective inherited
    // head, including through a handle whose warm coordinator is bound to the
    // child (and whose public snapshot id is therefore synthetic).
    feature
        .branch_create_from(ReadTarget::branch("feature"), "experiment")
        .await
        .unwrap();
    feature.sync_branch("experiment").await.unwrap();
    let inherited_child = feature
        .read_blob_at(ReadTarget::branch("experiment"), readme_cell.clone())
        .await
        .unwrap();
    let BlobContent::Managed {
        reader: child_reader,
        ..
    } = inherited_child.content
    else {
        panic!("expected managed Blob inherited from the named parent")
    };
    assert_eq!(&child_reader.read_range(0..5).await.unwrap()[..], b"Hello");
    drop(child_reader);
    main.branch_delete("experiment").await.unwrap();

    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        probes.table_walk_interval_count(),
        1,
        "one diverged Blob table must emit one post-preflight staging walk"
    );

    let merged_snapshot = main.resolve_snapshot("main").await.unwrap();
    main.branch_delete("feature").await.unwrap();
    match deletion_reader
        .read_range(BLOB_READ_RANGE_MAX_BYTES..BLOB_READ_RANGE_MAX_BYTES + 1)
        .await
    {
        Ok(bytes) => assert_eq!(&bytes[..], &[0x5a]),
        Err(OmniError::Storage(_)) => {}
        Err(other) => panic!(
            "destructive branch reclamation may return old bytes or fail loudly, never retarget; got {other:?}"
        ),
    }
    let readme = main
        .read_blob_at(ReadTarget::branch("main"), readme_cell.clone())
        .await
        .unwrap();
    assert_eq!(readme.resolved_target.requested, ReadTarget::branch("main"));
    let (merged_etag, pinned_reader) = match readme.content {
        BlobContent::Managed {
            length,
            etag,
            reader,
        } => {
            assert_eq!(length, 5);
            (etag.to_string(), reader)
        }
        BlobContent::External(external) => {
            panic!("expected managed branch Blob, got {external:?}")
        }
    };
    assert_eq!(&pinned_reader.read_range(0..5).await.unwrap()[..], b"Hello");

    let seed_bytes = read_managed_blob_bytes(
        &main,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "seed", "content"),
    )
    .await;
    assert!(
        seed_bytes.is_empty(),
        "a valid empty Blob must survive branch rewrite and merge"
    );

    let main_doc_bytes = read_managed_blob_bytes(
        &main,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "main-doc", "content"),
    )
    .await;
    assert_eq!(&main_doc_bytes[..], b"Main");

    // The returned reader owns the exact resolved table version. Advancing
    // branch HEAD must not retarget it, and the same cell remains addressable
    // through the explicit historical snapshot with the identical strong tag.
    main.load(
        "main",
        r#"{"type":"Document","data":{"title":"readme","content":"base64:V29ybGQ=","note":"head advance"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    assert_eq!(&pinned_reader.read_range(0..5).await.unwrap()[..], b"Hello");

    let historical = main
        .read_blob_at(
            ReadTarget::snapshot(merged_snapshot.clone()),
            readme_cell.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        historical.resolved_target.requested,
        ReadTarget::snapshot(merged_snapshot)
    );
    let BlobContent::Managed {
        etag,
        reader: historical_reader,
        ..
    } = historical.content
    else {
        panic!("historical managed Blob changed classification")
    };
    assert_eq!(etag.to_string(), merged_etag);
    assert_eq!(
        &historical_reader.read_range(0..5).await.unwrap()[..],
        b"Hello"
    );

    let current = main
        .read_blob_at(ReadTarget::branch("main"), readme_cell.clone())
        .await
        .unwrap();
    let BlobContent::Managed {
        etag,
        reader: current_reader,
        ..
    } = current.content
    else {
        panic!("current managed Blob changed classification")
    };
    assert_ne!(etag.to_string(), merged_etag);
    assert_eq!(
        &current_reader.read_range(0..5).await.unwrap()[..],
        b"World"
    );

    mutate_main(
        &main,
        BLOB_MUTATIONS,
        "delete_doc",
        &params(&[("$title", "readme")]),
    )
    .await
    .unwrap();
    assert_eq!(
        &current_reader.read_range(0..5).await.unwrap()[..],
        b"World",
        "a reader captured before row deletion must remain usable"
    );
    let deleted = main
        .read_blob_at(ReadTarget::branch("main"), readme_cell)
        .await
        .unwrap_err();
    assert!(
        matches!(
            deleted,
            OmniError::Manifest(ref error) if error.kind == ManifestErrorKind::NotFound
        ),
        "the advanced branch must observe the deletion, got {deleted:?}"
    );
}

#[tokio::test]
async fn blob_named_branch_delete_recreate_never_retargets_cached_or_snapshot_reads() {
    // Lance branch versions live in independent namespaces and a deleted
    // branch can be recreated at the same name and numeric table version.
    // The UUID-bearing transaction-file identity in each immutable manifest
    // prevents the two live values from reusing an ETag. An old graph snapshot
    // is refused because v6 did not persist the native branch incarnation that
    // would be needed to prove its path/version still denotes the old tree.
    let aba_dir = tempfile::tempdir().unwrap();
    let aba_uri = aba_dir.path().to_str().unwrap();
    let aba = helpers::session(Omnigraph::init(aba_uri, BLOB_SCHEMA).await.unwrap());
    aba.load_jsonl(
        r#"{"type":"Document","data":{"title":"aba","content":"base64:QmFzZQ==","note":"base"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();
    aba.branch_create("feature").await.unwrap();
    aba.load(
        "feature",
        r#"{"type":"Document","data":{"title":"aba","content":"base64:T2xk","note":"old"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let old_entry = aba
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();
    let old_pin = pinned_version(&aba, "feature", "node:Document").await;
    let live_aba_reader = Omnigraph::open(aba_uri).await.unwrap();
    let stale_aba = Omnigraph::open(aba_uri).await.unwrap();
    stale_aba.sync_branch("feature").await.unwrap();
    let old_snapshot_id = stale_aba.resolve_snapshot("feature").await.unwrap();
    let old_read = live_aba_reader
        .read_blob_at(
            ReadTarget::branch("feature"),
            node_blob_cell("Document", "aba", "content"),
        )
        .await
        .unwrap();
    let BlobContent::Managed { etag: old_etag, .. } = old_read.content else {
        panic!("old ABA value must be managed")
    };
    let exact_snapshot_read = stale_aba
        .read_blob_at(
            ReadTarget::snapshot(old_snapshot_id.clone()),
            node_blob_cell("Document", "aba", "content"),
        )
        .await
        .expect("the exact current named-branch snapshot has a live incarnation proof");
    let BlobContent::Managed {
        etag: exact_snapshot_etag,
        reader: exact_snapshot_reader,
        ..
    } = exact_snapshot_read.content
    else {
        panic!("the exact current named-branch snapshot must remain managed")
    };
    assert_eq!(exact_snapshot_etag, old_etag);
    assert_eq!(
        exact_snapshot_reader
            .read_range(0..exact_snapshot_reader.len())
            .await
            .unwrap(),
        b"Old"[..]
    );

    aba.branch_delete("feature").await.unwrap();
    aba.branch_create("feature").await.unwrap();
    aba.load(
        "feature",
        r#"{"type":"Document","data":{"title":"aba","content":"base64:TmV3","note":"new"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let new_entry = aba
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();
    assert_eq!(new_entry.dataset_path, old_entry.dataset_path);
    assert_eq!(new_entry.native_dataset_branch, None);
    assert_eq!(old_entry.native_dataset_branch, None);
    let new_pin = pinned_version(&aba, "feature", "node:Document").await;
    assert!(is_detached_version(old_pin) && is_detached_version(new_pin));
    assert_ne!(
        new_pin, old_pin,
        "each incarnation's write stages its own detached pin"
    );
    assert_eq!(
        new_entry.published_dataset_version, old_entry.published_dataset_version,
        "ABA fixture must recreate the same named ref at the same numeric table version"
    );
    // Reuse the same main-bound handle that cached the old feature table. On a
    // local filesystem the cache key has no manifest e-tag, so the Blob facade
    // must bypass that handle for named native tables rather than return Old at
    // the replacement branch's reused path/version.
    let new_read = live_aba_reader
        .read_blob_at(
            ReadTarget::branch("feature"),
            node_blob_cell("Document", "aba", "content"),
        )
        .await
        .unwrap();
    let BlobContent::Managed {
        etag: new_etag,
        reader: new_reader,
        ..
    } = new_read.content
    else {
        panic!("new ABA value must be managed")
    };
    assert_eq!(new_reader.read_range(0..3).await.unwrap(), b"New"[..]);
    assert_ne!(
        new_etag, old_etag,
        "same-name/same-version branch recreation with different bytes must mint a different strong ETag"
    );
    let stale_snapshot_error = stale_aba
        .read_blob_at(
            ReadTarget::snapshot(old_snapshot_id),
            node_blob_cell("Document", "aba", "content"),
        )
        .await
        .expect_err("an old snapshot must never reopen the recreated branch's bytes");
    assert!(
        matches!(
            stale_snapshot_error,
            OmniError::Manifest(ref error)
                if error.kind == ManifestErrorKind::BadRequest
                    // The commit-level snapshot resolution now performs the
                    // structural head re-prove first, so this scenario is
                    // refused there; the Blob-level witness remains for the
                    // windows commit resolution cannot see (post-capture
                    // table-open ABA, pinned by the failpoint cells). Either
                    // refusal carries the shared incarnation-witness phrase.
                    && error
                        .message
                        .contains("has no persisted native-branch incarnation witness")
        ),
        "named-branch ABA must fail loudly instead of retargeting; got {stale_snapshot_error:?}"
    );
}

#[tokio::test]
async fn blob_snapshot_inherited_from_main_refuses_named_branch_recreation_aba() {
    const SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob?
}

node Marker {
    name: String @key
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = helpers::session(Omnigraph::init(uri, SCHEMA).await.unwrap());
    db.load_jsonl(
        concat!(
            "{\"type\":\"Document\",\"data\":{\"title\":\"doc\",\"content\":\"base64:T2xk\"}}\n",
            "{\"type\":\"Marker\",\"data\":{\"name\":\"base\"}}",
        ),
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    db.branch_create("feature").await.unwrap();
    db.load(
        "feature",
        r#"{"type":"Marker","data":{"name":"feature-commit"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    let old_feature_version = db
        .graph_manifest_version_of(ReadTarget::branch("feature"))
        .await
        .unwrap();
    let old_entry = db
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();
    assert_eq!(
        old_entry.native_dataset_branch, None,
        "the old named-branch snapshot must inherit its Blob table from main"
    );

    let stale = Omnigraph::open(uri).await.unwrap();
    stale.sync_branch("feature").await.unwrap();
    let old_snapshot_id = stale.resolve_snapshot("feature").await.unwrap();
    assert_eq!(
        read_managed_blob_bytes(
            &stale,
            ReadTarget::snapshot(old_snapshot_id.clone()),
            node_blob_cell("Document", "doc", "content"),
        )
        .await,
        b"Old"
    );

    db.load(
        "feature",
        r#"{"type":"Marker","data":{"name":"ordinary-advance"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    assert_eq!(
        read_managed_blob_bytes(
            &stale,
            ReadTarget::snapshot(old_snapshot_id.clone()),
            node_blob_cell("Document", "doc", "content"),
        )
        .await,
        b"Old",
        "ordinary branch advance must preserve safe inherited-main history"
    );

    db.branch_delete("feature").await.unwrap();
    db.load(
        "main",
        r#"{"type":"Document","data":{"title":"doc","content":"base64:TmV3"}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();
    db.branch_create("feature").await.unwrap();

    assert_eq!(
        db.graph_manifest_version_of(ReadTarget::branch("feature"))
            .await
            .unwrap(),
        old_feature_version,
        "the replacement ref must reuse the old manifest version for this ABA regression"
    );
    let replacement_entry = db
        .snapshot_of(ReadTarget::branch("feature"))
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();
    assert_eq!(replacement_entry.native_dataset_branch, None);
    assert_eq!(
        read_managed_blob_bytes(
            &db,
            ReadTarget::branch("feature"),
            node_blob_cell("Document", "doc", "content"),
        )
        .await,
        b"New"
    );

    let stale_snapshot_error = stale
        .read_blob_at(
            ReadTarget::snapshot(old_snapshot_id),
            node_blob_cell("Document", "doc", "content"),
        )
        .await
        .expect_err("an inherited-main table must not bypass named graph-ref ABA fencing");
    assert!(
        matches!(
            stale_snapshot_error,
            OmniError::Manifest(ref error)
                if error.kind == ManifestErrorKind::BadRequest
                    // The commit-level snapshot resolution now performs the
                    // structural head re-prove first, so this scenario is
                    // refused there; the Blob-level witness remains for the
                    // windows commit resolution cannot see (post-capture
                    // table-open ABA, pinned by the failpoint cells). Either
                    // refusal carries the shared incarnation-witness phrase.
                    && error
                        .message
                        .contains("has no persisted native-branch incarnation witness")
        ),
        "named graph-ref ABA must fail loudly even when the Blob table is inherited from main; got {stale_snapshot_error:?}"
    );
}

#[tokio::test]
async fn branch_merge_with_external_blob_uri_materializes_payload() {
    Box::pin(branch_merge_with_external_blob_uri_materializes_payload_body()).await;
}

async fn branch_merge_with_external_blob_uri_materializes_payload_body() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let external_path = external_dir.path().join("external~source.txt");
    fs::write(&external_path, b"External").unwrap();
    let external_uri = url::Url::from_file_path(&external_path)
        .expect("external blob path is absolute")
        .to_string();
    let canonical_external_uri =
        url::Url::from_file_path(fs::canonicalize(&external_path).unwrap())
            .expect("canonical external blob path is absolute")
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

    let setup = helpers::session(
        Omnigraph::init(uri, MULTI_TABLE_EXTERNAL_BLOB_SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy.clone())
            .unwrap(),
    );
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

    let feature = helpers::session(
        Omnigraph::open(uri)
            .await
            .unwrap()
            .with_external_blob_policy(policy.clone())
            .unwrap(),
    );
    let configured_main = helpers::session(
        Omnigraph::open(uri)
            .await
            .unwrap()
            .with_external_blob_policy(policy.clone())
            .unwrap(),
    );
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
    let main = helpers::session(Omnigraph::open(uri).await.unwrap());

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
    let source_dataset = source_snapshot.open_dataset("node:Document").await.unwrap();
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
        canonical_external_uri,
        "precondition: source branch must retain the normalized external descriptor"
    );

    let before = snapshot_main(&main).await.unwrap();
    let before_manifest = before.graph_manifest_version();
    let entry = before.dataset("node:Document").unwrap();
    let before_table = entry.published_dataset_version;
    let table_uri = format!(
        "{}/{}",
        main.uri().trim_end_matches('/'),
        entry.dataset_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;
    let before_commits = main.list_commits(Some("main")).await.unwrap().len();
    let before_source_table = snapshot_branch(&main, "feature")
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .published_dataset_version;

    let error = main.branch_merge("feature", "main").await.unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ExternalBlobPolicy { ref uri, .. } if uri == &canonical_external_uri
        ),
        "default-deny merge must return typed ExternalBlobPolicy, got {error:?}"
    );
    let after = snapshot_main(&main).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before_manifest,
        "denied merge moved manifest"
    );
    assert_eq!(
        after
            .dataset("node:Document")
            .unwrap()
            .published_dataset_version,
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
            .dataset("node:Document")
            .unwrap()
            .published_dataset_version,
        before_source_table,
        "denied merge moved the source table pointer"
    );
    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "denied merge must fail before recovery is armed"
    );

    drop(main);
    let main = helpers::session(
        Omnigraph::open(uri)
            .await
            .unwrap()
            .with_external_blob_policy(policy.clone())
            .unwrap(),
    );
    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), main.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);
    assert_eq!(
        probes.external_blob_probe_inputs(),
        2,
        "the three-way Document table's two selected URI cells join the external preflight; the unadvanced Asset table is a pointer switch"
    );
    assert_eq!(
        probes.external_blob_probe_calls(),
        1,
        "normalized-equivalent URIs must cause one HEAD"
    );
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        1,
        "equivalent aliases must share one payload GET in the Document chunk; the switched Asset pin reads none"
    );

    let external_bytes = read_managed_blob_bytes(
        &main,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "external", "content"),
    )
    .await;
    assert_eq!(&external_bytes[..], b"External");
    let asset = main
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Asset", "external-asset", "payload"),
        )
        .await
        .unwrap();
    let BlobContent::External(asset) = asset.content else {
        panic!("a pointer switch keeps the Asset descriptor external on main")
    };
    assert_eq!(asset.uri, canonical_external_uri);
    assert_eq!(asset.offset, 0);
    assert_eq!(asset.length, None);
    let external_two_bytes = read_managed_blob_bytes(
        &main,
        ReadTarget::branch("main"),
        node_blob_cell("Document", "external-two", "content"),
    )
    .await;
    assert_eq!(&external_two_bytes[..], b"External");
    let converged = main
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Document", "converged", "content"),
        )
        .await
        .unwrap();
    let BlobContent::External(converged) = converged.content else {
        panic!(
            "the descriptor-only decision walk and row-staging walk must keep the identical target row out of the delta"
        )
    };
    assert_eq!(converged.uri, canonical_external_uri);
    assert_eq!(converged.offset, 0);
    assert_eq!(converged.length, None);
}

/// A pointer-only main -> named-branch adoption is not new ingress and does
/// not write a row. It must preserve the already-stored descriptor without
/// policy approval or source I/O, even when the caller-owned target has
/// disappeared.
#[tokio::test]
async fn branch_merge_pointer_only_external_blob_needs_no_source_io() {
    Box::pin(branch_merge_pointer_only_external_blob_needs_no_source_io_body()).await;
}

async fn branch_merge_pointer_only_external_blob_needs_no_source_io_body() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let pointer_path = external_dir.path().join("pointer-only.txt");
    fs::write(&pointer_path, b"Pointer only").unwrap();
    let pointer_uri = url::Url::from_file_path(&pointer_path)
        .expect("pointer-only external blob path is absolute")
        .to_string();
    let canonical_pointer_uri = url::Url::from_file_path(fs::canonicalize(&pointer_path).unwrap())
        .expect("canonical pointer-only external blob path is absolute")
        .to_string();
    let base_uri = url::Url::from_directory_path(external_dir.path())
        .expect("external blob base is absolute")
        .to_string();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(base_uri, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();
    let main = helpers::session(
        Omnigraph::init(uri, MULTI_TABLE_EXTERNAL_BLOB_SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap(),
    );
    main.branch_create("pointer-target").await.unwrap();
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
        .dataset("node:Document")
        .unwrap()
        .clone();
    let pointer = main
        .read_blob_at(
            ReadTarget::branch("main"),
            node_blob_cell("Document", "pointer-only", "content"),
        )
        .await
        .unwrap();
    let BlobContent::External(pointer) = pointer.content else {
        panic!("overwrite must preserve the pointer-only descriptor")
    };
    assert_eq!(pointer.uri, canonical_pointer_uri);
    assert_eq!(pointer.offset, 0);
    assert_eq!(pointer.length, None);
    fs::remove_file(&pointer_path).unwrap();

    let deny = helpers::session(Omnigraph::open(uri).await.unwrap());
    let outcome = deny.branch_merge("main", "pointer-target").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    let target_entry = snapshot_branch(&deny, "pointer-target")
        .await
        .unwrap()
        .dataset("node:Document")
        .unwrap()
        .clone();
    // `table_path` is identity-derived, so an exact path/version/branch match
    // proves this was pointer adoption rather than a rewritten table effect.
    assert_eq!(target_entry.type_key, source_entry.type_key);
    assert_eq!(target_entry.dataset_path, source_entry.dataset_path);
    assert_eq!(
        target_entry.published_dataset_version,
        source_entry.published_dataset_version
    );
    assert_eq!(
        target_entry.native_dataset_branch,
        source_entry.native_dataset_branch
    );

    let read_probes = MergeWriteProbes::default();
    let pointer = with_merge_write_probes(
        read_probes.clone(),
        deny.read_blob_at(
            ReadTarget::branch("pointer-target"),
            node_blob_cell("Document", "pointer-only", "content"),
        ),
    )
    .await
    .unwrap();
    let BlobContent::External(pointer) = pointer.content else {
        panic!("pointer adoption must preserve the external descriptor")
    };
    assert_eq!(pointer.uri, canonical_pointer_uri);
    assert_eq!(pointer.offset, 0);
    assert_eq!(pointer.length, None);
    assert_eq!(read_probes.external_blob_probe_calls(), 0);
    assert_eq!(read_probes.external_blob_payload_read_calls(), 0);
}

/// External payloads past the 32 MiB materialization ceiling, in one cell,
/// one row, or two tables, merge onto main by pointer switch: no payload is
/// read and the descriptors stay external.
#[tokio::test]
async fn branch_merge_onto_main_switches_oversized_external_blob_pointers() {
    Box::pin(branch_merge_onto_main_switches_oversized_external_blob_pointers_body()).await;
}

async fn branch_merge_onto_main_switches_oversized_external_blob_pointers_body() {
    const LIMIT: u64 = 32 * 1024 * 1024;

    for (case, first_bytes, second_bytes, split_across_tables) in [
        ("single", LIMIT + 1, None, false),
        ("cumulative", LIMIT / 2 + 1, Some(LIMIT / 2 + 1), false),
        ("cross-table", LIMIT / 2 + 1, Some(LIMIT / 2 + 1), true),
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
        let db = helpers::session(
            Omnigraph::init(graph_uri, WIDE_BLOB_SCHEMA)
                .await
                .unwrap()
                .with_external_blob_policy(policy)
                .unwrap(),
        );
        let base = r#"{"type":"Document","data":{"title":"base"}}"#;
        db.load_jsonl(base, LoadMode::Overwrite).await.unwrap();
        db.branch_create("feature").await.unwrap();
        db.load(
            "feature",
            &format!("{base}\n{selected_rows}"),
            LoadMode::Overwrite,
        )
        .await
        .unwrap();

        let before_tables = pointer_switch_tables(&db).await;
        let probes = MergeWriteProbes::default();
        let outcome = with_merge_write_probes(probes.clone(), db.branch_merge("feature", "main"))
            .await
            .unwrap();
        assert_eq!(outcome, MergeOutcome::FastForward, "{case}");
        assert_pointer_switch_onto_main(&db, &probes, before_tables, case).await;
        assert_eq!(count_rows(&db, "node:Document").await, 2);
        assert_eq!(
            count_rows(&db, "node:Asset").await,
            usize::from(split_across_tables)
        );
        let first = db
            .read_blob_at(
                ReadTarget::branch("main"),
                node_blob_cell("Document", format!("wide-{case}"), "first"),
            )
            .await
            .unwrap();
        let BlobContent::External(first) = first.content else {
            panic!("{case}: a pointer switch keeps the external descriptor as written")
        };
        assert_eq!(
            first.uri,
            url::Url::from_file_path(fs::canonicalize(&first_path).unwrap())
                .unwrap()
                .to_string()
        );
        assert_eq!(first.offset, 0);
        let recovery_dir = graph_path.join("__recovery");
        assert!(
            !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
            "{case}: a pointer switch leaves no recovery sidecar"
        );
    }
}

/// Main's `node:Document` and `node:Asset` before a merge from `feature`:
/// key, `dataset_path`, feature's pin, linear HEAD, table uri.
async fn pointer_switch_tables(db: &Omnigraph) -> Vec<(&'static str, String, u64, u64, String)> {
    let before = snapshot_main(db).await.unwrap();
    let mut tables = Vec::new();
    for table_key in ["node:Document", "node:Asset"] {
        let entry = before.dataset(table_key).unwrap();
        let table_uri = format!(
            "{}/{}",
            db.uri().trim_end_matches('/'),
            entry.dataset_path.trim_start_matches('/')
        );
        tables.push((
            table_key,
            entry.dataset_path.clone(),
            pinned_version(db, "feature", table_key).await,
            Dataset::open(&table_uri).await.unwrap().version().version,
            table_uri,
        ));
    }
    tables
}

/// A merge onto an unadvanced main is not Blob ingress: no policy check, no
/// payload read, nothing staged; main takes each source pin in place.
async fn assert_pointer_switch_onto_main(
    db: &Omnigraph,
    probes: &MergeWriteProbes,
    tables: Vec<(&'static str, String, u64, u64, String)>,
    case: &str,
) {
    assert_eq!(probes.external_blob_probe_inputs(), 0, "{case}");
    assert_eq!(probes.external_blob_probe_calls(), 0, "{case}");
    assert_eq!(probes.external_blob_payload_read_calls(), 0, "{case}");
    assert_eq!(probes.blob_payload_read_calls(), 0, "{case}");
    assert_eq!(probes.stage_append_calls(), 0, "{case}");
    assert_eq!(probes.stage_merge_insert_calls(), 0, "{case}");
    assert_eq!(probes.stage_fenced_insert_calls(), 0, "{case}");
    assert_eq!(probes.stage_known_present_update_calls(), 0, "{case}");
    let after = snapshot_main(db).await.unwrap();
    for (table_key, dataset_path, source_pin, linear_head, table_uri) in tables {
        assert_eq!(
            after.dataset(table_key).unwrap().dataset_path,
            dataset_path,
            "{case}: {table_key}"
        );
        assert_eq!(
            pinned_version(db, "main", table_key).await,
            source_pin,
            "{case}: {table_key} main takes the source's pin"
        );
        assert_eq!(
            Dataset::open(&table_uri).await.unwrap().version().version,
            linear_head,
            "{case}: {table_key} linear HEAD moved"
        );
    }
}

/// Two managed payloads whose sum passes the 32 MiB materialization ceiling
/// merge onto main by pointer switch: no payload is read or copied, and both
/// managed descriptors read back through main.
#[tokio::test]
async fn branch_merge_onto_main_switches_managed_blob_pointers() {
    Box::pin(branch_merge_onto_main_switches_managed_blob_pointers_body()).await;
}

async fn branch_merge_onto_main_switches_managed_blob_pointers_body() {
    const LIMIT: u64 = 32 * 1024 * 1024;

    let dir = tempfile::tempdir().unwrap();
    let graph_path = dir.path().join("managed-aggregate-graph");
    let graph_uri = graph_path.to_str().unwrap();
    let db = helpers::session(Omnigraph::init(graph_uri, WIDE_BLOB_SCHEMA).await.unwrap());
    let base = r#"{"type":"Document","data":{"title":"base"}}"#;
    db.load_jsonl(base, LoadMode::Overwrite).await.unwrap();
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

    let before_tables = pointer_switch_tables(&db).await;
    let probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(probes.clone(), db.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_pointer_switch_onto_main(&db, &probes, before_tables, "managed").await;
    assert_eq!(count_rows(&db, "node:Document").await, 3);
    for index in 0..2 {
        let read = db
            .read_blob_at(
                ReadTarget::branch("main"),
                node_blob_cell("Document", format!("managed-{index}"), "first"),
            )
            .await
            .unwrap();
        let BlobContent::Managed { reader, .. } = read.content else {
            panic!("managed-{index}: the switched pin keeps the managed descriptor")
        };
        assert_eq!(reader.len(), payload_bytes);
    }
    let recovery_dir = graph_path.join("__recovery");
    assert!(!recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none());
}

/// Two source Overwrites, each legal alone, hold 8,193 external cells, one
/// past the ingress cell bound; the merge onto main switches their pointers
/// with no external HEAD or payload GET, and every descriptor stays external.
#[tokio::test]
async fn branch_merge_onto_main_switches_external_blob_reference_cells() {
    Box::pin(branch_merge_onto_main_switches_external_blob_reference_cells_body()).await;
}

async fn branch_merge_onto_main_switches_external_blob_reference_cells_body() {
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
    let db = helpers::session(
        Omnigraph::init(graph_path.to_str().unwrap(), WIDE_BLOB_SCHEMA)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap(),
    );
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

    let source_before = snapshot_branch(&db, "feature").await.unwrap();
    let before_tables = pointer_switch_tables(&db).await;
    let merge_probes = MergeWriteProbes::default();
    let outcome = with_merge_write_probes(merge_probes.clone(), db.branch_merge("feature", "main"))
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_pointer_switch_onto_main(&db, &merge_probes, before_tables, "reference cells").await;
    assert_eq!(count_rows(&db, "node:Document").await, REFERENCE_LIMIT / 2);
    assert_eq!(count_rows(&db, "node:Asset").await, REFERENCE_LIMIT / 2 + 1);
    let source_after = snapshot_branch(&db, "feature").await.unwrap();
    for table_key in ["node:Document", "node:Asset"] {
        assert!(
            source_after
                .dataset(table_key)
                .unwrap()
                .same_registration(source_before.dataset(table_key).unwrap()),
            "the merge must not move the source table pointer"
        );
    }
    let canonical_external_uri =
        url::Url::from_file_path(fs::canonicalize(&external_path).unwrap())
            .unwrap()
            .to_string();
    for (type_name, id, property) in [
        ("Document", "document-0", "first"),
        ("Asset", "asset-0", "payload"),
    ] {
        let read = db
            .read_blob_at(
                ReadTarget::branch("main"),
                node_blob_cell(type_name, id, property),
            )
            .await
            .unwrap();
        let BlobContent::External(reference) = read.content else {
            panic!("{type_name}: a pointer switch keeps the external descriptor as written")
        };
        assert_eq!(reference.uri, canonical_external_uri);
    }
    let recovery_dir = graph_path.join("__recovery");
    assert!(!recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none());
}

#[tokio::test]
async fn branch_merge_applies_node_insert_to_main() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let outcome = feature.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);

    let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
    let qr = query_main(
        &reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);
}

/// Rust because the pins are detached table versions, the linear HEAD, and the
/// entry retained on an empty delta; the row-visible half is
/// `merge_adopt_*.gqt`. Both named targets adopt the exact source registration.
#[tokio::test]
async fn branch_merge_preserves_state_when_pins_differ() {
    for lazy_target in [false, true] {
        assert_native_version_case(8, lazy_target).await;
    }
}

// Construct the composed case in its own stack frame, then poll its heap-held
// future after that frame is gone. Keep ordinary Tokio stacks for every call.
fn assert_native_version_case(
    branch_updates: i64,
    lazy_target: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
    Box::pin(async move {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let main = init_and_load(&dir).await;
        main.branch_create("feature").await.unwrap();
        let history_branch = if lazy_target { "main" } else { "feature" };
        let history_updates = branch_updates + i64::from(lazy_target);
        for age in 40..40 + history_updates {
            main.mutate(
                history_branch,
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "Alice")], &[("$age", age)]),
            )
            .await
            .unwrap();
        }
        if !lazy_target {
            main.branch_create_from(ReadTarget::branch("feature"), "borrower")
                .await
                .unwrap();
        }
        let (source, target) = if lazy_target {
            // Keep the changes disjoint: a long main history and one Bob
            // edit on feature merge into one additional feature commit.
            main.mutate(
                "feature",
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
            )
            .await
            .unwrap();
            assert_eq!(
                main.branch_merge("main", "feature").await.unwrap(),
                MergeOutcome::Merged
            );
            main.mutate(
                "feature",
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "Alice")], &[("$age", 50)]),
            )
            .await
            .unwrap();
            main.branch_create_from(ReadTarget::branch("main"), "child")
                .await
                .unwrap();
            ("feature", "child")
        } else {
            assert_eq!(
                main.branch_merge("feature", "main").await.unwrap(),
                MergeOutcome::FastForward
            );
            main.mutate(
                "main",
                MUTATION_QUERIES,
                "set_age",
                &mixed_params(&[("$name", "Alice")], &[("$age", 50)]),
            )
            .await
            .unwrap();
            ("main", "feature")
        };
        let source_entry = snapshot_branch(&main, source)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .clone();
        let target_entry = snapshot_branch(&main, target)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .clone();
        let source_pin = pinned_version(&main, source, "node:Person").await;
        let target_pin = pinned_version(&main, target, "node:Person").await;
        assert!(is_detached_version(source_pin) && is_detached_version(target_pin));
        assert_ne!(
            source_pin, target_pin,
            "fixture must exercise different pins on the two sides"
        );
        assert_eq!(source_entry.native_dataset_branch, None);
        assert_eq!(target_entry.native_dataset_branch, None);
        assert_eq!(
            main.branch_merge(source, target).await.unwrap(),
            MergeOutcome::FastForward
        );
        assert_eq!(
            pinned_version(&main, target, "node:Person").await,
            source_pin,
            "{target}: adoption takes the source's pin"
        );
        let merged_entry = snapshot_branch(&main, target)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .clone();
        assert_eq!(
            (
                merged_entry.published_dataset_version,
                merged_entry.native_dataset_branch.as_deref()
            ),
            (
                source_entry.published_dataset_version,
                source_entry.native_dataset_branch.as_deref()
            ),
            "{target}, {branch_updates} updates: adoption preserves the exact source ref and version"
        );
        let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
        for handle in [&main, &reopened] {
            let result = handle
                .query(
                    ReadTarget::branch(target),
                    TEST_QUERIES,
                    "get_person",
                    &params(&[("$name", "Alice")]),
                )
                .await
                .unwrap();
            let batch = result.concat_batches().unwrap();
            assert_eq!(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                50,
                "{target}, {branch_updates} updates: source value must survive adoption"
            );
        }
        if lazy_target {
            return;
        }
        main.mutate(
            target,
            MUTATION_QUERIES,
            "add_friend",
            &params(&[("$from", "Alice"), ("$to", "Diana")]),
        )
        .await
        .unwrap();
        assert_eq!(
            main.branch_merge(target, "main").await.unwrap(),
            MergeOutcome::FastForward
        );
        let result = main
            .query(
                ReadTarget::branch("main"),
                TEST_QUERIES,
                "get_person",
                &params(&[("$name", "Alice")]),
            )
            .await
            .unwrap();
        let batch = result.concat_batches().unwrap();
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            50,
            "{target}, {branch_updates} updates: an unrelated edit must not roll back main"
        );

        // Main and the target now share Person's registration. Bringing main
        // back is an empty adopt: retain the target's complete public
        // registration and do not create a physical table commit.
        let before_empty = snapshot_branch(&main, target)
            .await
            .unwrap()
            .dataset("node:Person")
            .unwrap()
            .clone();
        let table_uri = format!("{uri}/{}", before_empty.dataset_path);
        let head_before =
            open_dataset_head_exact(&table_uri, before_empty.native_dataset_branch.as_deref())
                .await
                .version()
                .version;
        assert_eq!(
            main.branch_merge("main", target).await.unwrap(),
            MergeOutcome::FastForward
        );
        let reopened = Omnigraph::open(uri).await.unwrap();
        for handle in [&main, &reopened] {
            let after_empty = snapshot_branch(handle, target)
                .await
                .unwrap()
                .dataset("node:Person")
                .unwrap()
                .clone();
            assert_eq!(after_empty.type_key, before_empty.type_key);
            assert_eq!(after_empty.dataset_path, before_empty.dataset_path);
            assert_eq!(
                after_empty.native_dataset_branch,
                before_empty.native_dataset_branch
            );
            assert_eq!(
                after_empty.published_dataset_version,
                before_empty.published_dataset_version
            );
            assert_eq!(after_empty.entity_count, before_empty.entity_count);
            assert!(
                after_empty.same_registration(&before_empty),
                "empty adoption must retain the target's Lance manifest metadata"
            );
        }
        assert_eq!(
            open_dataset_head_exact(&table_uri, before_empty.native_dataset_branch.as_deref())
                .await
                .version()
                .version,
            head_before,
            "empty adoption must not advance the physical target HEAD"
        );
        let maintenance = Omnigraph::open(uri).await.unwrap();
        maintenance
            .cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(100),
                older_than: None,
            })
            .await
            .unwrap();
        main.mutate(
            target,
            MUTATION_QUERIES,
            "set_age",
            &mixed_params(&[("$name", "Alice")], &[("$age", 51)]),
        )
        .await
        .unwrap();
        let written = snapshot_branch(&main, target).await.unwrap();
        let written = written.dataset("node:Person").unwrap();
        assert_eq!(written.native_dataset_branch, None);
        let written_pin = pinned_version(&main, target, "node:Person").await;
        assert!(is_detached_version(written_pin), "{written_pin}");
        assert_ne!(
            written_pin,
            pinned_version(&main, "main", "node:Person").await
        );
        assert_ne!(written_pin, target_pin);
        let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
        for handle in [&main, &reopened] {
            for (branch, age) in [
                ("borrower", 39 + branch_updates as i32),
                (target, 51),
                ("main", 50),
            ] {
                let result = handle
                    .query(
                        ReadTarget::branch(branch),
                        TEST_QUERIES,
                        "get_person",
                        &params(&[("$name", "Alice")]),
                    )
                    .await
                    .unwrap();
                let batch = result.concat_batches().unwrap();
                assert_eq!(
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0),
                    age,
                    "{branch}: cleanup and a fresh target pin preserve independent branch values"
                );
            }
        }
    })
}

#[tokio::test]
async fn branch_write_after_adoption_keeps_borrowers_and_stages_fresh_pins() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;
    db.branch_create("feature").await.unwrap();
    db.load_as(
        "feature",
        None,
        r#"{"type":"Company","data":{"name":"BorrowedCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    db.branch_create_from(ReadTarget::branch("feature"), "child")
        .await
        .unwrap();
    let borrowed = snapshot_branch(&db, "child")
        .await
        .unwrap()
        .dataset("node:Company")
        .unwrap()
        .clone();
    assert_eq!(borrowed.native_dataset_branch, None);
    let borrowed_pin = pinned_version(&db, "child", "node:Company").await;
    assert!(is_detached_version(borrowed_pin), "{borrowed_pin}");
    assert_eq!(
        db.branch_merge("feature", "main").await.unwrap(),
        MergeOutcome::FastForward
    );
    db.load_as(
        "main",
        None,
        r#"{"type":"Company","data":{"name":"MainCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        db.branch_merge("main", "feature").await.unwrap(),
        MergeOutcome::FastForward
    );
    let owner_before = snapshot_branch(&db, "feature").await.unwrap();
    let owner_pin_before = pinned_version(&db, "feature", "node:Company").await;
    let table_uri = format!("{}/{}", db.uri(), borrowed.dataset_path);
    let head_before = open_dataset_head_exact(&table_uri, None)
        .await
        .version()
        .version;

    db.load_as(
        "feature",
        None,
        r#"{"type":"Company","data":{"name":"NewCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    let owner_after = snapshot_branch(&db, "feature").await.unwrap();
    let written = owner_after.dataset("node:Company").unwrap();
    assert_eq!(written.dataset_path, borrowed.dataset_path);
    assert_eq!(written.native_dataset_branch, None);
    let written_pin = pinned_version(&db, "feature", "node:Company").await;
    assert!(is_detached_version(written_pin), "{written_pin}");
    assert_ne!(written_pin, borrowed_pin);
    assert_ne!(written_pin, owner_pin_before);
    assert_eq!(
        open_dataset_head_exact(&table_uri, None)
            .await
            .version()
            .version,
        head_before,
        "a branch write never moves the linear HEAD"
    );
    assert!(
        helpers::collector::detached_versions(&table_uri)
            .await
            .contains(&borrowed_pin),
        "a fresh write must leave the borrowed pin in place"
    );
    db.branch_create_from(ReadTarget::branch("feature"), "replacement")
        .await
        .unwrap();
    db.load_as(
        "replacement",
        None,
        r#"{"type":"Company","data":{"name":"ReplacementCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    let replacement = snapshot_branch(&db, "replacement").await.unwrap();
    let replacement_entry = replacement.dataset("node:Company").unwrap();
    let replacement_pin = pinned_version(&db, "replacement", "node:Company").await;
    assert_eq!(
        db.branch_merge("replacement", "feature").await.unwrap(),
        MergeOutcome::FastForward
    );
    assert_eq!(
        pinned_version(&db, "feature", "node:Company").await,
        replacement_pin,
        "adoption takes the replacement's pin"
    );
    let adopted = snapshot_branch(&db, "feature").await.unwrap();
    let adopted_entry = adopted.dataset("node:Company").unwrap();
    assert_eq!(
        (
            &adopted_entry.type_key,
            &adopted_entry.dataset_path,
            adopted_entry.published_dataset_version,
            &adopted_entry.native_dataset_branch,
            adopted_entry.entity_count,
        ),
        (
            &replacement_entry.type_key,
            &replacement_entry.dataset_path,
            replacement_entry.published_dataset_version,
            &replacement_entry.native_dataset_branch,
            replacement_entry.entity_count,
        ),
        "adoption must preserve the exact replacement table pointer"
    );
    db.load_as(
        "feature",
        None,
        r#"{"type":"Company","data":{"name":"AfterAdoptCo"}}"#,
        LoadMode::Merge,
        None,
    )
    .await
    .unwrap();
    let after_adopt_write = snapshot_branch(&db, "feature").await.unwrap();
    let after_adopt_entry = after_adopt_write.dataset("node:Company").unwrap();
    assert_eq!(after_adopt_entry.native_dataset_branch, None);
    let after_adopt_pin = pinned_version(&db, "feature", "node:Company").await;
    assert!(is_detached_version(after_adopt_pin), "{after_adopt_pin}");
    assert_ne!(after_adopt_pin, replacement_pin);
    assert_ne!(after_adopt_pin, written_pin);
    let replacement_after = snapshot_branch(&db, "replacement").await.unwrap();
    assert!(
        replacement_after
            .dataset("node:Company")
            .unwrap()
            .same_registration(replacement_entry)
    );
    let reopened = Omnigraph::open(db.uri()).await.unwrap();
    for handle in [&db, &reopened] {
        for branch in ["child", "main", "replacement", "feature"] {
            let names = collect_column_strings(
                &read_table_branch(handle, branch, "node:Company").await,
                "name",
            );
            assert_eq!(
                names.iter().any(|name| name == "AfterAdoptCo"),
                branch == "feature"
            );
            assert_eq!(
                names.iter().any(|name| name == "ReplacementCo"),
                matches!(branch, "replacement" | "feature")
            );
            assert_eq!(
                names.iter().any(|name| name == "NewCo"),
                matches!(branch, "replacement" | "feature")
            );
            assert_eq!(names.iter().any(|name| name == "MainCo"), branch != "child");
            assert!(names.iter().any(|name| name == "BorrowedCo"));
        }
    }
    let child = snapshot_branch(&db, "child").await.unwrap();
    let child = child.dataset("node:Company").unwrap();
    assert_eq!(child.native_dataset_branch, borrowed.native_dataset_branch);
    assert_eq!(
        child.published_dataset_version,
        borrowed.published_dataset_version
    );
    assert_eq!(
        pinned_version(&db, "child", "node:Company").await,
        borrowed_pin,
        "the borrower keeps its pin across every later write"
    );
    assert_ne!(
        snapshot_branch(&db, "feature")
            .await
            .unwrap()
            .graph_manifest_version(),
        owner_before.graph_manifest_version()
    );
}

#[tokio::test]
async fn branch_merge_records_single_latest_commit_with_two_parents() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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
    let latest_manifest_version = commits
        .iter()
        .map(|c| c.graph_manifest_version)
        .max()
        .unwrap();
    let latest_commits: Vec<_> = commits
        .iter()
        .filter(|commit| commit.graph_manifest_version == latest_manifest_version)
        .collect();

    assert_eq!(latest_commits.len(), 1);
    assert_eq!(head.graph_manifest_version, latest_manifest_version);
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
    let a = init_and_load(&dir).await;
    let c0 = CommitGraph::open(uri)
        .await
        .unwrap()
        .head_commit()
        .await
        .unwrap()
        .unwrap();

    // External writer B advances main: commit C1, parent C0.
    let b = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_main(
        &b,
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
        &a,
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
    let latest = commits
        .iter()
        .max_by_key(|c| c.graph_manifest_version)
        .unwrap();
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
    let a = init_and_load(&dir).await;
    mutate_main(
        &a,
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
    let b = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_main(
        &b,
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
    query_main(&a, TEST_QUERIES, "total_people", &params(&[]))
        .await
        .unwrap();

    // Strict update, no explicit refresh: pre-fix it appends off the stale head
    // Ca instead of Cb.
    mutate_main(
        &a,
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
    let latest = commits
        .iter()
        .max_by_key(|c| c.graph_manifest_version)
        .unwrap();
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

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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
        source_head_before.graph_manifest_version,
        target_head_before.graph_manifest_version
    );

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::AlreadyUpToDate);

    let commit_graph = CommitGraph::open(uri).await.unwrap();
    let head = commit_graph.head_commit().await.unwrap().unwrap();

    assert_eq!(
        head.graph_manifest_version,
        target_head_before.graph_manifest_version
    );
    assert_eq!(head.graph_commit_id, target_head_before.graph_commit_id);
    assert_eq!(head.graph_commit_id, source_head_before.graph_commit_id);
}

#[tokio::test]
async fn branch_merge_returns_merged_for_non_fast_forward_auto_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
        &main,
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
        &main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1);
}

/// The three-way merge classifier must compare row values TYPED, not by Arrow's
/// display string, which is not injective for nested values: `["a, b"]` (one
/// element with a comma) and `["a","b"]` (two elements) render identically as
/// `[a, b]`. A feature-branch change between two such values would be classified
/// as a no-op and silently dropped.
#[tokio::test]
async fn branch_merge_detects_nested_list_value_change() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = "node Doc {\n    slug: String @key\n    tags: [String]\n}";
    let main = helpers::session(Omnigraph::init(uri, schema).await.unwrap());
    // Base: one element containing a comma.
    main.load_with_receipt(
        "main",
        r#"{"type":"Doc","data":{"slug":"x","tags":["a, b"]}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    main.branch_create("feature").await.unwrap();
    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    // Feature changes it to two elements — displays identically to the base.
    feature
        .load_with_receipt(
            "feature",
            r#"{"type":"Doc","data":{"slug":"x","tags":["a","b"]}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap();

    // Diverge main with an unrelated row so the merge is a real three-way, not
    // a fast-forward (the display-string classifier only runs on the three-way path).
    main.load_with_receipt(
        "main",
        r#"{"type":"Doc","data":{"slug":"y","tags":["z"]}}"#,
        LoadMode::Merge,
    )
    .await
    .unwrap();

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    // `["a","b"]` contains the element "b"; `["a, b"]` does not. So `x` is
    // returned only if feature's change survived the merge.
    let queries = r#"
query docs_with_tag($tag: String) {
    match { $d: Doc  $d.tags contains $tag }
    return { $d.slug }
}
"#;
    let result = query_main(&main, queries, "docs_with_tag", &params(&[("$tag", "b")]))
        .await
        .unwrap();
    let batch = result.concat_batches().unwrap();
    let slugs = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let got: Vec<&str> = (0..slugs.len()).map(|i| slugs.value(i)).collect();
    assert!(
        got.contains(&"x"),
        "merge dropped the nested-list value change (display-string collision): {got:?}"
    );
}

#[tokio::test]
async fn branch_merge_allows_identical_updates_on_both_sides() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 31)]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
        &main,
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
    let main = init_search_db(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        SEARCH_MUTATIONS,
        "set_doc_title",
        &params(&[("$slug", "ml-intro"), ("$title", "Orion ML Intro")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
        &main,
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
        .open_dataset("node:Doc")
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

    let writer = helpers::session(Omnigraph::open(uri).await.unwrap());
    let reader = helpers::session(Omnigraph::open(uri).await.unwrap());
    let main_reader = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_branch(
        &writer,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let visible = query_branch(
        &reader,
        "feature",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(visible.num_rows(), 1);

    let main_result = query_main(
        &main_reader,
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

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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
            .join(format!(
                "{}.json",
                graph_native_ref(dir.path().to_str().unwrap(), "experiment").await
            )),
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

    let experiment = helpers::session(Omnigraph::open(uri).await.unwrap());
    let qr = query_branch(
        &experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1);

    let reopened_main = helpers::session(Omnigraph::open(uri).await.unwrap());
    let main_qr = query_main(
        &reopened_main,
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

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        None
    );
    let feature_pin = pinned_version(&experiment, "feature", "node:Person").await;
    assert!(is_detached_version(feature_pin), "{feature_pin}");
    assert_eq!(
        pinned_version(&experiment, "experiment", "node:Person").await,
        feature_pin,
        "the child branch inherits feature's pin"
    );

    experiment.ensure_indices_on("experiment").await.unwrap();

    let experiment_snap = snapshot_branch(&experiment, "experiment").await.unwrap();
    assert_eq!(
        experiment_snap
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch,
        experiment_inherited
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch,
        "index reconciliation must preserve the exact inherited table ref when no work is needed"
    );
    assert_eq!(
        pinned_version(&experiment, "experiment", "node:Person").await,
        feature_pin,
        "index reconciliation must keep the inherited pin when no work is needed"
    );
    assert_eq!(
        experiment_snap
            .dataset("edge:Knows")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        None
    );

    let feature_snap = snapshot_branch(&feature, "feature").await.unwrap();
    assert_eq!(
        feature_snap
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        None
    );
    assert_ne!(
        feature_pin,
        pinned_version(&feature, "main", "node:Person").await,
        "feature's write staged its own pin"
    );
    assert_eq!(
        count_rows_branch(&feature, "feature", "node:Person").await,
        5
    );
    assert_eq!(
        count_rows_branch(&experiment, "experiment", "node:Person").await,
        5
    );

    experiment
        .branch_create_from(ReadTarget::branch("experiment"), "grandchild")
        .await
        .unwrap();
    let grandchild = snapshot_branch(&experiment, "grandchild").await.unwrap();
    assert_eq!(
        grandchild
            .dataset("node:Person")
            .unwrap()
            .native_dataset_branch
            .as_deref(),
        None
    );
    assert_eq!(
        pinned_version(&experiment, "grandchild", "node:Person").await,
        feature_pin,
        "a lazy descendant keeps its ancestor's pin"
    );
    experiment.branch_delete("grandchild").await.unwrap();
    experiment.branch_delete("experiment").await.unwrap();
}

#[tokio::test]
async fn branch_edge_only_write_only_moves_edge_table_pin() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let snap = snapshot_branch(&feature, "feature").await.unwrap();
    for table_key in ["node:Person", "edge:Knows", "edge:WorksAt"] {
        assert_eq!(
            snap.dataset(table_key)
                .unwrap()
                .native_dataset_branch
                .as_deref(),
            None,
            "{table_key}: a branch write never forks a table"
        );
        let branch_pin = pinned_version(&feature, "feature", table_key).await;
        let main_pin = pinned_version(&feature, "main", table_key).await;
        assert_eq!(
            branch_pin != main_pin,
            table_key == "edge:Knows",
            "{table_key}: only the written edge table takes a new pin"
        );
    }
    assert!(is_detached_version(
        pinned_version(&feature, "feature", "edge:Knows").await
    ));

    let feature_qr = query_branch(
        &feature,
        "feature",
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(feature_qr.num_rows(), 3);

    let reopened_main = helpers::session(Omnigraph::open(uri).await.unwrap());
    let main_qr = query_main(
        &reopened_main,
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

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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
        &feature,
        "feature",
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    let main_commit_ids = main
        .list_commits(None)
        .await
        .unwrap()
        .into_iter()
        .map(|commit| commit.graph_commit_id)
        .collect::<Vec<_>>();
    let source_before = snapshot_branch(&feature, "feature").await.unwrap();
    let source_entry = source_before.dataset("node:Person").unwrap();
    let outcome = main.branch_merge("feature", "experiment").await.unwrap();
    assert_eq!(outcome, MergeOutcome::FastForward);
    assert_eq!(
        main.list_commits(None)
            .await
            .unwrap()
            .into_iter()
            .map(|commit| commit.graph_commit_id)
            .collect::<Vec<_>>(),
        main_commit_ids,
        "merging into another branch must preserve main's graph lineage"
    );

    let experiment = helpers::session(Omnigraph::open(uri).await.unwrap());
    let bob = query_branch(
        &experiment,
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
        &experiment,
        "experiment",
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1);
    let experiment_snap = snapshot_branch(&experiment, "experiment").await.unwrap();
    let adopted_entry = experiment_snap.dataset("node:Person").unwrap();
    assert_eq!(
        (
            &adopted_entry.type_key,
            &adopted_entry.dataset_path,
            adopted_entry.published_dataset_version,
            &adopted_entry.native_dataset_branch,
            adopted_entry.entity_count,
        ),
        (
            &source_entry.type_key,
            &source_entry.dataset_path,
            source_entry.published_dataset_version,
            &source_entry.native_dataset_branch,
            source_entry.entity_count,
        ),
        "the named target must adopt the exact source table pointer"
    );

    let reopened_main = helpers::session(Omnigraph::open(uri).await.unwrap());
    let main_bob = query_main(
        &reopened_main,
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
    let main = init_db_from_schema_and_data(&dir, UNIQUE_SCHEMA, UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        UNIQUE_MUTATIONS,
        "insert_user",
        &params(&[("$name", "Bob"), ("$email", "dup@example.com")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
                conflict.type_key == "node:User"
                    && conflict.kind == MergeConflictKind::UniqueViolation
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

/// Regression for the MR-983 follow-up: branch merge must enforce an edge
/// composite `@unique(@src, @dst)` as a true composite key, like intake, so two
/// branches inserting the same `(__src, __dst)` pair conflict on merge.
#[tokio::test]
async fn branch_merge_reports_composite_unique_violation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_db_from_schema_and_data(&dir, EDGE_UNIQUE_SCHEMA, EDGE_UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        EDGE_UNIQUE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
                conflict.type_key == "edge:Knows"
                    && conflict.kind == MergeConflictKind::UniqueViolation
            }));
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}

/// Sibling to the above: pairs sharing `__src` but differing on `__dst` are unique
/// on the (__src, __dst) tuple and must merge cleanly. Guards against the composite
/// degrading back into a single-field `@unique(@src)` on the merge path.
#[tokio::test]
async fn branch_merge_allows_distinct_composite_unique_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_db_from_schema_and_data(&dir, EDGE_UNIQUE_SCHEMA, EDGE_UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        EDGE_UNIQUE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
    let main = init_db_from_schema_and_data(&dir, CARDINALITY_SCHEMA, CARDINALITY_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        CARDINALITY_MUTATIONS,
        "add_employment",
        &params(&[("$person", "Alice"), ("$company", "Acme")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
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
                conflict.type_key == "edge:WorksAt"
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
    let main = init_db_from_schema_and_data(&dir, EDGE_UNIQUE_SCHEMA, EDGE_UNIQUE_DATA).await;
    main.branch_create("feature").await.unwrap();
    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    // main (merge source): add an edge referencing Bob.
    mutate_main(
        &main,
        MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    // feature (merge target): delete Bob.
    mutate_branch(
        &feature,
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
                    .any(|c| c.type_key == "edge:Knows" && c.kind == MergeConflictKind::OrphanEdge),
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
async fn branch_delete_defers_pin_collection_and_allows_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    let first_entry = snapshot_branch(&main, "feature")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(first_entry.native_dataset_branch, None);
    let first_pin = pinned_version(&main, "feature", "node:Person").await;
    assert!(is_detached_version(first_pin), "{first_pin}");
    let person_uri = format!("{uri}/{}", first_entry.dataset_path);
    main.branch_delete("feature").await.unwrap();
    assert_eq!(main.branch_list().await.unwrap(), vec!["main"]);
    assert!(
        helpers::collector::detached_versions(&person_uri)
            .await
            .contains(&first_pin),
        "delete defers collection of the deleted branch's pin"
    );
    main.branch_create("feature").await.unwrap();
    mutate_branch(
        &main,
        "feature",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();
    let second_entry = snapshot_branch(&main, "feature")
        .await
        .unwrap()
        .dataset("node:Person")
        .unwrap()
        .clone();
    assert_eq!(second_entry.dataset_path, first_entry.dataset_path);
    assert_eq!(second_entry.native_dataset_branch, None);
    let second_pin = pinned_version(&main, "feature", "node:Person").await;
    assert!(is_detached_version(second_pin), "{second_pin}");
    assert_ne!(first_pin, second_pin);
    assert_eq!(count_rows_branch(&main, "feature", "node:Person").await, 5);
    let detached = helpers::collector::detached_versions(&person_uri).await;
    assert!(
        detached.contains(&first_pin),
        "recreated writes leave the former pin intact"
    );
    assert!(detached.contains(&second_pin));
    main.cleanup(omnigraph::db::CleanupPolicyOptions {
        keep_versions: Some(1),
        older_than: None,
    })
    .await
    .unwrap();
    let detached = helpers::collector::detached_versions(&person_uri).await;
    assert!(
        !detached.contains(&first_pin),
        "cleanup reclaims the deleted incarnation's pin"
    );
    assert!(
        detached.contains(&second_pin),
        "cleanup preserves the recreated branch's pin"
    );
    drop(feature);
    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(
        count_rows_branch(&reopened, "feature", "node:Person").await,
        5
    );
    assert_eq!(main.branch_list().await.unwrap(), vec!["main", "feature"]);
}

#[tokio::test]
async fn branch_names_reject_incarnation_shaped_suffixes() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    // A native ref is `{logical}.{ULID}`; a logical name that already looks
    // like one could not be split back, and an inner segment shaped that way
    // would nest a child tree under a native ref's physical path.
    for name in [
        "feature.01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "feature.01ARZ3NDEKTSV4RRFFQ69G5FAV/child",
    ] {
        let err = db.branch_create(name).await.unwrap_err();
        assert!(
            err.to_string().contains("incarnation-shaped suffix"),
            "create must refuse '{name}' by its suffix rule; got: {err}"
        );
        let err = db.snapshot_of(ReadTarget::branch(name)).await.unwrap_err();
        assert!(
            err.to_string().contains("incarnation-shaped suffix"),
            "native refs are not addressable on reads either; got: {err}"
        );
    }
    // Ordinary dotted names stay legal.
    db.branch_create("release.1.2").await.unwrap();
    assert_eq!(db.branch_list().await.unwrap(), vec!["main", "release.1.2"]);
}

#[tokio::test]
async fn branch_namespace_rejects_live_physical_path_prefix_collisions() {
    let dir = tempfile::tempdir().unwrap();
    let db = init_and_load(&dir).await;

    db.branch_create("feature").await.unwrap();
    for result in [
        db.branch_create("feature").await,
        db.branch_create_from("main", "feature").await,
    ] {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("already exists"),
            "both creation routes must reject a duplicate: {error}"
        );
    }
    for result in [
        db.branch_create("feature/child").await,
        db.branch_create_from("main", "feature/child").await,
    ] {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("physical Lance path")
                && error.to_string().contains("ancestors or descendants"),
            "prefix collision must be actionable: {error}"
        );
    }
    assert!(
        !dir.path()
            .join("__manifest")
            .join("tree")
            .join("feature")
            .exists(),
        "prefix admission must reject before Lance creates the target clone"
    );

    let manifest = lance::Dataset::open(&format!("{}/__manifest", dir.path().display()))
        .await
        .unwrap();
    let ancestor_native = helpers::native_ref_for(&manifest, "feature").await.unwrap();
    let ancestor = manifest.branches().get(&ancestor_native).await.unwrap();
    db.branch_delete("feature").await.unwrap();
    db.branch_create("feature/child").await.unwrap();
    let refs_before_refusal = manifest.list_branches().await.unwrap();
    assert!(!refs_before_refusal.contains_key(&ancestor_native));
    let ancestor_archive = dir
        .path()
        .join("__manifest/tree")
        .join(&ancestor_native)
        .join("_omnigraph_retired_branch.json");
    let archived: lance::dataset::refs::BranchContents =
        serde_json::from_slice(&std::fs::read(&ancestor_archive).unwrap()).unwrap();
    assert_eq!(archived.identifier, ancestor.identifier);
    let retirement: serde_json::Value =
        serde_json::from_str(&archived.metadata["omnigraph.retired_manifest_branch"]).unwrap();
    assert_eq!(
        retirement,
        serde_json::json!({
            "version": 1,
            "native_branch": ancestor_native,
            "identifier": ancestor.identifier,
        })
    );
    for result in [
        db.branch_create("feature").await,
        db.branch_create_from("main", "feature").await,
    ] {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("physical Lance path")
                && error.to_string().contains("feature/child"),
            "ancestor creation must reject the inverse prefix collision: {error}"
        );
    }
    assert_eq!(
        serde_json::to_value(manifest.list_branches().await.unwrap()).unwrap(),
        serde_json::to_value(refs_before_refusal).unwrap(),
        "inverse admission refusal must preserve the retired ancestor and create no ref"
    );
    assert!(
        helpers::native_ref_for(&manifest, "feature")
            .await
            .is_none()
    );
    assert_eq!(
        db.branch_list().await.unwrap(),
        vec!["main", "feature/child"]
    );
    db.branch_delete("feature/child").await.unwrap();
    db.branch_create_from("main", "feature").await.unwrap();
    let recreated_native = helpers::native_ref_for(&manifest, "feature").await.unwrap();
    assert_ne!(recreated_native, ancestor_native);
    assert!(manifest.branches().get(&ancestor_native).await.is_err());
    assert!(ancestor_archive.exists());
    db.branch_create("feature-2").await.unwrap();
    assert_eq!(
        db.branch_list().await.unwrap(),
        vec!["main", "feature", "feature-2"],
        "an unrelated string prefix is not a hierarchy collision"
    );
    assert_eq!(count_rows_branch(&db, "feature", "node:Person").await, 4);
}

#[tokio::test]
async fn branch_delete_retires_legacy_physical_path_parents() {
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

    db.branch_delete("feature").await.unwrap();
    assert!(
        helpers::native_ref_for(&manifest, "feature")
            .await
            .is_none()
    );
    assert!(manifest.branches().get("feature").await.is_err());
    let archive = dir
        .path()
        .join("__manifest/tree/feature/_omnigraph_retired_branch.json");
    assert!(archive.exists());
    assert_eq!(
        count_rows_branch(&db, "feature/child", "node:Person").await,
        4
    );
    db.branch_delete("feature/child").await.unwrap();
    assert_eq!(db.branch_list().await.unwrap(), vec!["main"]);
    let refused = db.branch_create("feature/new-child").await.unwrap_err();
    assert!(
        refused.to_string().contains("retired branch 'feature'"),
        "{refused}"
    );
    let rows = db
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
    assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
    assert!(!archive.exists());
    db.branch_create("feature/new-child").await.unwrap();
    assert_eq!(
        count_rows_branch(&db, "feature/new-child", "node:Person").await,
        4
    );
}

#[tokio::test]
async fn branch_delete_retires_native_parent_and_cleanup_preserves_live_child() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;

    main.branch_create("feature").await.unwrap();
    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());
    mutate_branch(
        &feature,
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

    let manifest = lance::Dataset::open(&format!("{uri}/__manifest"))
        .await
        .unwrap();
    let live = manifest.list_branches().await.unwrap();
    let native = live
        .keys()
        .find(|name| helpers::is_incarnation_of(name, "feature"))
        .unwrap()
        .clone();
    let identifier = live[&native].identifier.clone();
    let retained = manifest.checkout_branch(&native).await.unwrap();

    let changed_metadata = [("external-owner".to_string(), "preserved".to_string())]
        .into_iter()
        .collect();
    manifest
        .branches()
        .replace_metadata(&native, changed_metadata)
        .await
        .unwrap();
    assert_eq!(
        manifest.list_branches().await.unwrap()[&native].metadata["external-owner"],
        "preserved"
    );
    assert!(
        Omnigraph::open(uri)
            .await
            .unwrap()
            .branch_list()
            .await
            .unwrap()
            .contains(&"feature".to_string())
    );
    let tagged_snapshot = snapshot_branch(&main, "feature").await.unwrap();
    let tagged_entry = tagged_snapshot.dataset("node:Person").unwrap();
    manifest
        .tags()
        .create(
            "retirement-pin",
            (native.as_str(), retained.version().version),
        )
        .await
        .unwrap();
    let refused = main.branch_delete("feature").await.expect_err(
        "native graph tags retain the deletion fence until graph-wide tagged table pins exist",
    );
    assert!(
        refused.to_string().contains("native manifest tag"),
        "{refused}"
    );
    assert_eq!(
        manifest.branches().get(&native).await.unwrap().identifier,
        identifier
    );
    let after_refusal = snapshot_branch(&main, "feature").await.unwrap();
    let current_entry = after_refusal.dataset("node:Person").unwrap();
    assert_eq!(
        current_entry.native_dataset_branch,
        tagged_entry.native_dataset_branch
    );
    assert_eq!(
        current_entry.published_dataset_version,
        tagged_entry.published_dataset_version
    );
    assert_eq!(count_rows_branch(&main, "feature", "node:Person").await, 5);
    assert_eq!(
        count_rows_branch(&main, "experiment", "node:Person").await,
        5
    );
    manifest.tags().delete("retirement-pin").await.unwrap();
    main.branch_delete("feature").await.unwrap();
    assert!(
        helpers::native_ref_for(&manifest, "feature")
            .await
            .is_none()
    );
    assert!(
        !main
            .branch_list()
            .await
            .unwrap()
            .contains(&"feature".to_string())
    );
    assert!(manifest.branches().get(&native).await.is_err());
    assert!(retained.branch_identifier().await.is_err());
    let ref_path = dir
        .path()
        .join("__manifest/_refs/branches")
        .join(format!("{native}.json"));
    let archive_path = dir
        .path()
        .join("__manifest/tree")
        .join(&native)
        .join("_omnigraph_retired_branch.json");
    assert!(!ref_path.exists());
    let retired_bytes = std::fs::read(&archive_path).unwrap();
    let archived: lance::dataset::refs::BranchContents =
        serde_json::from_slice(&retired_bytes).unwrap();
    assert_eq!(archived.identifier, identifier);
    assert_eq!(archived.metadata["external-owner"], "preserved");
    let retired_contents: serde_json::Value = serde_json::from_slice(&retired_bytes).unwrap();
    let marker = &retired_contents["metadata"]["omnigraph.retired_manifest_branch"];
    let retirement: serde_json::Value = serde_json::from_str(marker.as_str().unwrap()).unwrap();
    assert_eq!(retirement["version"], serde_json::json!(1));
    assert_eq!(retirement["native_branch"], serde_json::json!(native));
    assert_eq!(
        retirement["identifier"],
        serde_json::to_value(&identifier).unwrap()
    );
    for invalid in [
        "missing_identifier",
        "identifier",
        "version",
        "native_branch",
        "unknown_field",
    ] {
        let mut broken_marker = retirement.clone();
        match invalid {
            "missing_identifier" => {
                broken_marker.as_object_mut().unwrap().remove("identifier");
            }
            "version" => broken_marker["version"] = serde_json::json!(99),
            "identifier" => {
                let foreign = live
                    .values()
                    .find(|contents| contents.identifier != identifier)
                    .unwrap();
                broken_marker["identifier"] = serde_json::to_value(&foreign.identifier).unwrap();
            }
            "native_branch" => broken_marker["native_branch"] = serde_json::json!("foreign"),
            "unknown_field" => broken_marker["unknown"] = serde_json::json!(true),
            _ => unreachable!(),
        }
        let mut broken = retired_contents.clone();
        broken["metadata"]["omnigraph.retired_manifest_branch"] =
            serde_json::json!(serde_json::to_string(&broken_marker).unwrap());
        std::fs::write(&ref_path, serde_json::to_vec(&broken).unwrap()).unwrap();
        assert!(manifest.branches().get(&native).await.is_ok());
        assert!(main.branch_list().await.is_err(), "{invalid}");
        assert!(
            snapshot_branch(&main, "feature").await.is_err(),
            "{invalid}"
        );
        assert!(
            main.cleanup(omnigraph::db::CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: None,
            })
            .await
            .is_err(),
            "{invalid}"
        );
        assert!(
            ref_path.exists(),
            "invalid retirement must preserve physical authority"
        );
        assert!(dir.path().join("__manifest/tree").join(&native).exists());
    }
    std::fs::write(&ref_path, retired_bytes).unwrap();
    assert!(snapshot_branch(&feature, "feature").await.is_err());
    assert_eq!(
        count_rows_branch(&feature, "experiment", "node:Person").await,
        5
    );

    let reopened = helpers::session(Omnigraph::open(uri).await.unwrap());
    reopened.branch_create("feature").await.unwrap();
    assert_eq!(
        count_rows_branch(&reopened, "feature", "node:Person").await,
        4
    );
    mutate_branch(
        &reopened,
        "experiment",
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 41)]),
    )
    .await
    .unwrap();
    reopened
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
    assert!(manifest.branches().get(&native).await.is_err());
    assert!(
        archive_path.exists(),
        "a live child retains its archived native ancestor"
    );
    assert_eq!(
        count_rows_branch(&reopened, "experiment", "node:Person").await,
        6
    );
    reopened.branch_delete("experiment").await.unwrap();
    reopened
        .cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .unwrap();
    assert!(manifest.branches().get(&native).await.is_err());
    assert!(!ref_path.exists());
    assert!(
        !archive_path.exists(),
        "cleanup removes unreferenced retirement history with its tree"
    );
    assert!(!dir.path().join("__manifest/tree").join(&native).exists());
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        vec!["main".to_string(), "feature".to_string()],
        "retained retirement evidence must not expose deleted branches"
    );
    assert!(snapshot_branch(&reopened, "experiment").await.is_err());
    let fresh = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows_branch(&fresh, "main", "node:Person").await, 4);
    assert_eq!(count_rows_branch(&fresh, "feature", "node:Person").await, 4);
}

// ─── Step 9b: Surgical merge publish tests ──────────────────────────────────

#[tokio::test]
async fn merged_table_preserves_row_version_for_unchanged_rows() {
    // After a non-FF merge, unchanged rows retain their original _row_created_at_version.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();

    main.branch_create("feature").await.unwrap();
    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    // Main updates Bob's age → changes one row
    mutate_main(
        &main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve → adds one row
    mutate_branch(
        &feature,
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
    let ds = snap.open_dataset("node:Person").await.unwrap();
    let mut scanner = ds.scan();
    scanner
        .project(&["__id", "_row_created_at_version"])
        .unwrap();
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
            .column_by_name("__id")
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
    let ds = snap.open_dataset("edge:Knows").await.unwrap();
    let indices = ds.load_indices().await.unwrap();
    let user_indices: Vec<_> = indices.iter().filter(|idx| !is_system_index(idx)).collect();

    let index_names: Vec<_> = user_indices.iter().map(|idx| idx.fields.clone()).collect();
    assert!(
        user_indices.len() >= 3,
        "Edge table should have at least 3 indices (__id, __src, __dst), got {:?}",
        index_names
    );
}

#[tokio::test]
async fn merge_delta_only_bumps_changed_rows() {
    // After a non-FF merge, unchanged rows should NOT have _row_last_updated_at_version
    // bumped. Only rows that were actually modified should get new version stamps.
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_and_load(&dir).await;
    main.ensure_indices().await.unwrap();

    main.branch_create("feature").await.unwrap();
    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    // Main updates Bob's age → changes one Person row
    mutate_main(
        &main,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Bob")], &[("$age", 26)]),
    )
    .await
    .unwrap();

    // Feature inserts Eve → adds one Person row (makes it non-FF)
    mutate_branch(
        &feature,
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
    let ds = snap.open_dataset("node:Person").await.unwrap();
    let mut scanner = ds.scan();
    scanner
        .project(&["__id", "_row_last_updated_at_version"])
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

// ─── Edge @key: born-on-both convergence and divergence (issue #583) ─────────

const EDGE_KEY_MERGE_SCHEMA: &str = r#"
node Person {
    name: String @key
}

edge Knows: Person -> Person {
    since: String?
    @key(@src, @dst)
}
"#;

// The unkeyed control: identical fixture without the key declaration.
const EDGE_UNKEYED_MERGE_SCHEMA: &str = r#"
node Person {
    name: String @key
}

edge Knows: Person -> Person {
    since: String?
}
"#;

const EDGE_KEY_MERGE_DATA: &str = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}
{"type":"Person","data":{"name":"Carol"}}
{"type":"Person","data":{"name":"Dave"}}
{"type":"Person","data":{"name":"Eve"}}
{"edge":"Knows","from":"Alice","to":"Carol"}
{"edge":"Knows","from":"Alice","to":"Dave"}
{"edge":"Knows","from":"Alice","to":"Eve"}"#;

const EDGE_KEY_MERGE_MUTATIONS: &str = r#"
query add_knows($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}

query add_knows_since($from: String, $to: String, $since: String) {
    insert Knows { from: $from, to: $to, since: $since }
}
"#;

const EDGE_KEY_MERGE_QUERIES: &str = r#"
query friends() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
    }
    return { $f.name }
}

query friend_edges() {
    match {
        $p: Person { name: "Alice" }
        $p $w:knows $f
    }
    return { $f.name }
}
"#;

/// Issue #583's repro with `@key(@src, @dst)` declared: the same keyed edge on
/// both sides of a fork derives the same id, so the merge converges with no
/// conflict and the plain and bound-edge traversals both return 4 rows.
#[tokio::test]
async fn branch_merge_converges_born_on_both_keyed_edge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_db_from_schema_and_data(&dir, EDGE_KEY_MERGE_SCHEMA, EDGE_KEY_MERGE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
        "feature",
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    main.branch_merge("feature", "main")
        .await
        .expect("identical born-on-both keyed edges converge without conflict");

    assert_eq!(count_rows(&main, "edge:Knows").await, 4);
    let plain = query_main(&main, EDGE_KEY_MERGE_QUERIES, "friends", &params(&[]))
        .await
        .unwrap();
    assert_eq!(first_column_sorted(&plain).len(), 4);
    let bound = query_main(&main, EDGE_KEY_MERGE_QUERIES, "friend_edges", &params(&[]))
        .await
        .unwrap();
    assert_eq!(first_column_sorted(&bound).len(), 4);
}

/// The unkeyed control pins the documented multiset outcome the RFC's
/// acceptance threshold names: the merge keeps both rows (5 edges), the
/// plain traversal's visited gate suppresses the duplicate (4), and the
/// bound-edge traversal reports every row (5).
#[tokio::test]
async fn branch_merge_keeps_both_born_on_both_unkeyed_edges() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main =
        init_db_from_schema_and_data(&dir, EDGE_UNKEYED_MERGE_SCHEMA, EDGE_KEY_MERGE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
        "feature",
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    main.branch_merge("feature", "main")
        .await
        .expect("unkeyed born-on-both edges keep the documented multiset outcome");

    assert_eq!(count_rows(&main, "edge:Knows").await, 5);
    let plain = query_main(&main, EDGE_KEY_MERGE_QUERIES, "friends", &params(&[]))
        .await
        .unwrap();
    assert_eq!(first_column_sorted(&plain).len(), 4);
    let bound = query_main(&main, EDGE_KEY_MERGE_QUERIES, "friend_edges", &params(&[]))
        .await
        .unwrap();
    assert_eq!(first_column_sorted(&bound).len(), 5);
}

/// Distinct keyed pairs inserted one per side derive distinct ids and merge
/// cleanly: no conflict, both rows land.
#[tokio::test]
async fn branch_merge_keeps_distinct_keyed_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_db_from_schema_and_data(&dir, EDGE_KEY_MERGE_SCHEMA, EDGE_KEY_MERGE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Alice"), ("$to", "Bob")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
        "feature",
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows",
        &params(&[("$from", "Bob"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    main.branch_merge("feature", "main")
        .await
        .expect("distinct keyed pairs must merge cleanly");
    // Three fixture edges plus the two distinct new pairs.
    assert_eq!(count_rows(&main, "edge:Knows").await, 5);
}

/// Two branches inserting the same key with DIFFERENT non-key properties
/// surface `DivergentInsert`, with the derived id as the conflict entity id.
#[tokio::test]
async fn branch_merge_reports_divergent_insert_for_keyed_edge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let main = init_db_from_schema_and_data(&dir, EDGE_KEY_MERGE_SCHEMA, EDGE_KEY_MERGE_DATA).await;
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(uri).await.unwrap());

    mutate_main(
        &main,
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows_since",
        &params(&[("$from", "Alice"), ("$to", "Bob"), ("$since", "2020")]),
    )
    .await
    .unwrap();

    mutate_branch(
        &feature,
        "feature",
        EDGE_KEY_MERGE_MUTATIONS,
        "add_knows_since",
        &params(&[("$from", "Alice"), ("$to", "Bob"), ("$since", "2021")]),
    )
    .await
    .unwrap();

    let err = main.branch_merge("feature", "main").await.unwrap_err();
    match err {
        OmniError::MergeConflicts(conflicts) => {
            assert!(
                conflicts.iter().any(|conflict| {
                    conflict.type_key == "edge:Knows"
                        && conflict.kind == MergeConflictKind::DivergentInsert
                        && conflict.entity_id.as_deref() == Some(r#"["Alice","Bob"]"#)
                }),
                "expected DivergentInsert on the derived edge id, got {conflicts:?}"
            );
        }
        other => panic!("expected merge conflicts, got {other:?}"),
    }
}
