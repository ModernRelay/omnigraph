use crate::helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use omnigraph::db::MergeOutcome;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::instrumentation::{
    MergeWriteProbes, QueryIoProbes, with_merge_write_probes, with_query_io_probes,
};
use omnigraph::loader::LoadMode;
use omnigraph::storage::{storage_for_uri, storage_read_through};
use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};
use omnigraph_storage::read_through_contract::{self, Fixture, s3_prefix_store};

use crate::helpers::*;

#[tokio::test(flavor = "multi_thread")]
async fn s3_compatible_graph_lifecycle_works() {
    let Some(uri) = s3_test_graph_uri("omnigraph-runtime") else {
        eprintln!("skipping s3 runtime test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let db = helpers::session(Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap());
    db.load_jsonl(TEST_DATA, LoadMode::Overwrite).await.unwrap();

    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let snapshot = reopened.snapshot_of("main").await.unwrap();
    assert!(snapshot.dataset("node:Person").is_some());
    assert!(snapshot.dataset("edge:Knows").is_some());

    let alice = query_main(
        &reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(alice[0]["p.name"], "Alice");

    reopened
        .mutate(
            "main",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "RustFS-Eve")], &[("$age", 29)]),
        )
        .await
        .unwrap();

    // Direct-to-target load: no run lifecycle, single publisher
    // commit lands the row.
    reopened
        .load(
            "main",
            r#"{"type":"Person","data":{"name":"RunOnly","age":31}}"#,
            LoadMode::Append,
        )
        .await
        .unwrap();

    let reopened_again = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let eve = query_main(
        &reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RustFS-Eve")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(eve[0]["p.name"], "RustFS-Eve");

    let run_only = query_main(
        &reopened_again,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "RunOnly")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(run_only[0]["p.name"], "RunOnly");
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_branch_change_merge_flow_works() {
    let Some(uri) = s3_test_graph_uri("omnigraph-branching") else {
        eprintln!("skipping s3 branch test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let main = helpers::session(Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap());
    main.load_jsonl(TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();
    main.branch_create("feature").await.unwrap();

    let feature = helpers::session(Omnigraph::open(&uri).await.unwrap());
    feature
        .mutate(
            "feature",
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", "Feature-Eve")], &[("$age", 22)]),
        )
        .await
        .unwrap();

    let before_merge = query_main(
        &main,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Feature-Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(before_merge.num_rows(), 0);

    let outcome = main.branch_merge("feature", "main").await.unwrap();
    assert_eq!(outcome.outcome, MergeOutcome::FastForward);

    let reopened = helpers::session(Omnigraph::open(&uri).await.unwrap());
    let after_merge = query_main(
        &reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Feature-Eve")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(after_merge[0]["p.name"], "Feature-Eve");
    assert_eq!(
        reopened.branch_list().await.unwrap(),
        vec!["main".to_string(), "feature".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_public_load_uses_hidden_run_and_publishes() {
    let Some(uri) = s3_test_graph_uri("omnigraph-public-load") else {
        eprintln!("skipping s3 public load test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let schema =
        format!("{TEST_SCHEMA}\nnode Document {{\n    title: String @key\n    content: Blob\n}}\n");
    let graph_uri = format!("{uri}/graph");
    let db = helpers::session(Omnigraph::init(&graph_uri, &schema).await.unwrap());
    db.load_jsonl(TEST_DATA, LoadMode::Overwrite).await.unwrap();

    db.load(
        "main",
        r#"{"type":"Person","data":{"name":"Loaded-Over-S3","age":34}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap();

    // Direct-to-target writes: no run state machine, just the
    // published commit lands the row. Verify by reopening and reading.
    let reopened = helpers::session(Omnigraph::open(&graph_uri).await.unwrap());
    let loaded = query_main(
        &reopened,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Loaded-Over-S3")]),
    )
    .await
    .unwrap()
    .to_rust_json()
    .unwrap();
    assert_eq!(loaded[0]["p.name"], "Loaded-Over-S3");

    // RFC-033's only server-safe source scheme is S3. Exercise it against the
    // configured backend rather than inferring remote behavior from file://:
    // two URI spellings for one object share one metadata probe and one payload
    // read, and keyed load owns the resulting bytes after the source disappears.
    let external_base = format!("{uri}/external/");
    let external_uri = format!("{external_base}shared~source.bin");
    let adapter = omnigraph::storage::storage_for_uri(&uri).unwrap();
    adapter
        .write_text(&external_uri, "RustFS external Blob")
        .await
        .unwrap();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(&external_base, ExternalBlobExecutionScope::ServerSafe).unwrap(),
    ])
    .unwrap();
    drop(db);
    let db = helpers::session(
        Omnigraph::open(&graph_uri)
            .await
            .unwrap()
            .with_external_blob_policy(policy)
            .unwrap(),
    );
    let encoded_alias = external_uri.replace("~source", "%7Esource");
    assert_ne!(encoded_alias, external_uri);
    let rows = format!(
        "{}\n{}",
        serde_json::json!({
            "type": "Document",
            "data": {"title": "remote-a", "content": external_uri},
        }),
        serde_json::json!({
            "type": "Document",
            "data": {"title": "remote-b", "content": encoded_alias},
        })
    );
    let probes = MergeWriteProbes::default();
    with_merge_write_probes(probes.clone(), db.load("main", &rows, LoadMode::Append))
        .await
        .unwrap();
    assert_eq!(probes.external_blob_probe_inputs(), 2);
    assert_eq!(
        probes.external_blob_probe_calls(),
        1,
        "normalized-equivalent S3 sources must issue one metadata probe"
    );
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        1,
        "one prepared keyed batch must read a normalized S3 source once"
    );

    adapter.delete(&external_uri).await.unwrap();
    let reopened = Omnigraph::open(&graph_uri).await.unwrap();
    for title in ["remote-a", "remote-b"] {
        let blob = read_managed_blob_bytes(
            &reopened,
            ReadTarget::branch("main"),
            node_blob_cell("Document", title, "content"),
        )
        .await;
        assert_eq!(&blob[..], b"RustFS external Blob");
    }
}

/// The conditional-write contract the cluster ledger depends on (RFC-006):
/// versioned read -> If-Match replace -> stale token refused. Pins the
/// S3-compatible backend's behavior (RustFS in CI) — turns red if a backend
/// bump regresses conditional puts.
#[tokio::test(flavor = "multi_thread")]
async fn s3_adapter_conditional_writes_contract() {
    let Some(uri) = s3_test_graph_uri("adapter-cas") else {
        eprintln!("skipping s3 adapter cas test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let adapter = storage_for_uri(&uri).unwrap();
    let object = format!("{uri}/cas-probe.json");

    assert!(adapter.write_text_if_absent(&object, "v1").await.unwrap());
    assert!(!adapter.write_text_if_absent(&object, "v1b").await.unwrap());
    assert_eq!(
        adapter
            .read_text_if_exists(&object)
            .await
            .unwrap()
            .as_deref(),
        Some("v1")
    );
    assert_eq!(
        adapter
            .read_text_if_exists(&format!("{uri}/missing-probe.json"))
            .await
            .unwrap(),
        None
    );

    let (text, version) = adapter.read_text_versioned(&object).await.unwrap();
    assert_eq!(text, "v1");
    let next = adapter
        .write_text_if_match(&object, "v2", &version)
        .await
        .unwrap()
        .expect("fresh etag must win");
    assert!(
        adapter
            .write_text_if_match(&object, "v3", &version)
            .await
            .unwrap()
            .is_none(),
        "stale etag must be refused"
    );
    let again = adapter
        .write_text_if_match(&object, "v3", &next)
        .await
        .unwrap();
    assert!(again.is_some());

    // Prefix delete: recursive + idempotent.
    adapter
        .write_text(&format!("{uri}/tree/a.json"), "a")
        .await
        .unwrap();
    adapter
        .write_text(&format!("{uri}/tree/sub/b.json"), "b")
        .await
        .unwrap();
    adapter.delete_prefix(&format!("{uri}/tree")).await.unwrap();
    assert!(!adapter.exists(&format!("{uri}/tree/a.json")).await.unwrap());
    adapter.delete_prefix(&format!("{uri}/tree")).await.unwrap();
    adapter.delete(&object).await.unwrap();
    assert_eq!(adapter.read_text_if_exists(&object).await.unwrap(), None);
}

/// Schema apply against an S3 graph — the cluster's schema executor will
/// lean on this; previously untested upstream on object storage.
#[tokio::test(flavor = "multi_thread")]
async fn s3_schema_apply_migrates_live_graph() {
    let Some(uri) = s3_test_graph_uri("schema-apply") else {
        eprintln!("skipping s3 schema apply test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let db = helpers::session(Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap());
    db.load_jsonl(TEST_DATA, LoadMode::Overwrite).await.unwrap();

    let desired = format!("{TEST_SCHEMA}\nnode Note {{\n    title: String @key\n}}\n");
    let result = db.apply_schema(&desired).await.unwrap();
    assert!(result.applied, "{result:?}");

    let reopened = Omnigraph::open(&uri).await.unwrap();
    assert!(
        reopened.schema_source().contains("Note"),
        "live S3 schema must carry the migration"
    );
}

/// Graph-index cross-branch reuse on a real object store, where the cache key's
/// per-table `e_tag` is a genuine token (the local twin in `warm_read_cost.rs`
/// keys on `None`): a fresh lazy-fork branch reuses main's cached index.
#[tokio::test(flavor = "multi_thread")]
async fn s3_fresh_branch_traversal_reuses_main_graph_index_with_etags() {
    let Some(uri) = s3_test_graph_uri("graph-index-etag") else {
        eprintln!("skipping s3 graph-index test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };

    let writer = helpers::session(Omnigraph::init(&uri, TEST_SCHEMA).await.unwrap());
    // TEST_DATA seeds Alice->Bob and Alice->Charlie Knows edges.
    writer
        .load_jsonl(TEST_DATA, LoadMode::Overwrite)
        .await
        .unwrap();

    // Separate reader: it never creates the branch, so branch_create below does
    // not invalidate the reader's warm cache.
    let reader = helpers::session(Omnigraph::open(&uri).await.unwrap());

    // Warm main on the CSR path: builds + caches the topology index keyed by the
    // edge table's physical identity incl. its real e_tag.
    let csr = helpers::with_traversal(&reader, Traversal::Csr);
    let warm = csr
        .query(
            ReadTarget::branch("main"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Alice")]),
        )
        .await
        .unwrap();
    assert_eq!(
        first_column_sorted(&warm),
        vec!["Bob", "Charlie"],
        "test setup: Alice knows Bob and Charlie"
    );

    // Lazy fork: feature's edge tables are physically main's (same version +
    // e_tag, table_branch = None).
    writer.branch_create("feature").await.unwrap();

    let graph_build = Arc::new(AtomicU64::new(0));
    let probes = QueryIoProbes {
        graph_build_count: Arc::clone(&graph_build),
        ..Default::default()
    };
    let on_branch = with_query_io_probes(
        probes,
        csr.query(
            ReadTarget::branch("feature"),
            TEST_QUERIES,
            "friends_of",
            &params(&[("$name", "Alice")]),
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        first_column_sorted(&on_branch),
        vec!["Bob", "Charlie"],
        "fresh branch sees main's edges (lazy fork) and the reused index is correct"
    );
    assert_eq!(
        graph_build.load(Ordering::Relaxed),
        0,
        "with real e_tags, a fresh lazy-fork branch must reuse main's cached CSR index, not rebuild"
    );
}

/// Every object under an `s3://` prefix with its size and ETag, sorted by
/// key: the base's bytes as the listing reports them.
async fn listing(uri: &str) -> Vec<(String, u64, Option<String>)> {
    let mut rows: Vec<_> = s3_prefix_store(uri)
        .unwrap()
        .list(None)
        .map_ok(|meta| (meta.location.to_string(), meta.size, meta.e_tag))
        .try_collect()
        .await
        .unwrap();
    rows.sort();
    rows
}

/// The read-through storage contract's stores on prefixes of the test
/// bucket: a base prefix holding the named objects and a fresh prefix per
/// clone root, all under one test root.
struct S3Fixture {
    root: String,
    next: AtomicU64,
}

impl S3Fixture {
    fn store(&self, kind: &str) -> Arc<dyn ObjectStore> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        s3_prefix_store(&format!("{}/{kind}-{n}", self.root)).unwrap()
    }
}

#[async_trait]
impl Fixture for S3Fixture {
    async fn base(&self, files: &[(&str, &str)]) -> Arc<dyn ObjectStore> {
        let store = self.store("base");
        for (key, text) in files {
            store
                .put(
                    &ObjectPath::from(*key),
                    PutPayload::from(text.as_bytes().to_vec()),
                )
                .await
                .unwrap();
        }
        store
    }

    async fn clone_root(&self) -> Arc<dyn ObjectStore> {
        self.store("clone")
    }
}

/// The durable-form rows of the storage contract (the graph shallow clone
/// RFC, The storage contract), run over a bucket prefix as base and another
/// as clone root; the bodies are the storage crate's own.
macro_rules! s3_contract_rows {
    ($($name:ident),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                let Some(root) = s3_test_graph_uri("read-through") else {
                    eprintln!("skipping s3 read-through contract test: OMNIGRAPH_S3_TEST_BUCKET is not set");
                    return;
                };
                let fixture = S3Fixture { root: root.clone(), next: AtomicU64::new(0) };
                read_through_contract::$name(&fixture).await;
                storage_for_uri(&root).unwrap().delete_prefix(&root).await.unwrap();
            }
        )*
    };
}

s3_contract_rows!(
    durable_deletes_survive_a_reopen_and_stay_in_their_own_clone_root,
    a_durable_store_keeps_its_whiteouts_out_of_sight,
    durable_refuses_a_base_that_is_itself_a_clone_root,
    durable_refuses_a_base_that_holds_an_object_at_the_reserved_name,
    a_clone_root_carries_the_reserved_directory_from_its_first_build,
    a_stop_between_the_whiteout_and_the_delete_leaves_the_key_deleted_or_untouched,
    an_update_and_a_delete_of_one_base_object_take_turns,
    a_create_and_a_delete_of_one_base_object_take_turns,
    a_rename_of_a_base_object_is_refused_and_one_of_a_clone_object_runs,
    create_after_a_durable_delete_holds_across_reopens,
    racing_updates_of_a_base_object_over_a_clone_root_admit_one,
);

/// The read-through adapter over two prefixes: reads fall through, writes and
/// prefix deletes stay in the clone root, and the base prefix's listing and
/// ETags are as they were after the calls and after the clone root is deleted.
#[tokio::test(flavor = "multi_thread")]
async fn s3_read_through_adapter_masks_the_base_and_leaves_its_listing_as_it_was() {
    let Some(root) = s3_test_graph_uri("read-through-adapter") else {
        eprintln!("skipping s3 read-through adapter test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let base_uri = format!("{root}/base");
    let clone_uri = format!("{root}/clone");
    let base = storage_for_uri(&base_uri).unwrap();
    for (name, text) in [
        ("a.json", "a"),
        ("tree/x.json", "x"),
        ("tree/sub/y.json", "y"),
    ] {
        base.write_text(&format!("{base_uri}/{name}"), text)
            .await
            .unwrap();
    }
    let before = listing(&base_uri).await;

    let clone = storage_read_through(&base_uri, &clone_uri).await.unwrap();
    assert_eq!(
        clone
            .read_text_if_exists(&format!("{clone_uri}/a.json"))
            .await
            .unwrap()
            .as_deref(),
        Some("a")
    );
    clone
        .write_text(&format!("{clone_uri}/b.json"), "b")
        .await
        .unwrap();
    assert!(!base.exists(&format!("{base_uri}/b.json")).await.unwrap());
    clone
        .delete_prefix(&format!("{clone_uri}/tree"))
        .await
        .unwrap();
    assert!(
        !clone
            .exists(&format!("{clone_uri}/tree/x.json"))
            .await
            .unwrap()
    );
    assert!(
        base.exists(&format!("{base_uri}/tree/x.json"))
            .await
            .unwrap()
    );
    assert!(
        clone
            .delete_prefix(&format!("{clone_uri}/__clone"))
            .await
            .is_err(),
        "a prefix delete of the reserved directory is refused"
    );
    clone.delete(&format!("{clone_uri}/a.json")).await.unwrap();

    let reopened = storage_read_through(&base_uri, &clone_uri).await.unwrap();
    assert_eq!(
        reopened
            .read_text_if_exists(&format!("{clone_uri}/a.json"))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        reopened
            .read_text_if_exists(&format!("{clone_uri}/b.json"))
            .await
            .unwrap()
            .as_deref(),
        Some("b")
    );
    assert_eq!(
        listing(&base_uri).await,
        before,
        "the base prefix's listing and ETags are as they were"
    );

    storage_for_uri(&clone_uri)
        .unwrap()
        .delete_prefix(&clone_uri)
        .await
        .unwrap();
    assert_eq!(listing(&clone_uri).await, Vec::new());
    assert_eq!(
        listing(&base_uri).await,
        before,
        "deleting the clone root's prefix leaves the base's listing as it was"
    );
    storage_for_uri(&root)
        .unwrap()
        .delete_prefix(&root)
        .await
        .unwrap();
}

/// `Omnigraph::open_clone` over two prefixes: the clone answers as its base,
/// a write lands only under the clone prefix, the base's listing and ETags
/// are unchanged, and a second open over the same clone root finds the write.
#[tokio::test(flavor = "multi_thread")]
async fn s3_open_clone_reads_the_base_and_writes_only_its_own_prefix() {
    let Some(root) = s3_test_graph_uri("shallow-clone") else {
        eprintln!("skipping s3 shallow clone test: OMNIGRAPH_S3_TEST_BUCKET is not set");
        return;
    };
    let base_uri = format!("{root}/base");
    let clone_uri = format!("{root}/clone");
    let persons = {
        let base = init_and_load_at(&base_uri, TEST_SCHEMA).await;
        count_rows(&base, "node:Person").await
    };
    let before = listing(&base_uri).await;

    let clone = session(Omnigraph::open_clone(&base_uri, &clone_uri).await.unwrap());
    assert_eq!(count_rows(&clone, "node:Person").await, persons);
    mutate_main(
        &clone,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Cloned")], &[("$age", 1)]),
    )
    .await
    .unwrap();
    assert_eq!(count_rows(&clone, "node:Person").await, persons + 1);

    assert_eq!(
        listing(&base_uri).await,
        before,
        "the base prefix's listing and ETags are unchanged"
    );
    assert!(
        storage_for_uri(&clone_uri)
            .unwrap()
            .exists(&format!("{clone_uri}/__clone/created"))
            .await
            .unwrap()
    );
    let base = session(Omnigraph::open(&base_uri).await.unwrap());
    assert_eq!(count_rows(&base, "node:Person").await, persons);

    let reopened = session(Omnigraph::open_clone(&base_uri, &clone_uri).await.unwrap());
    assert_eq!(count_rows(&reopened, "node:Person").await, persons + 1);
    storage_for_uri(&root)
        .unwrap()
        .delete_prefix(&root)
        .await
        .unwrap();
}
