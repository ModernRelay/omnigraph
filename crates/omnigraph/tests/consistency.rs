mod helpers;

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::{Array, Date32Array, Int32Array, StringArray};
use futures::{TryStreamExt, future::join_all};
use lance::Dataset;
use tokio::sync::Barrier;

use omnigraph::db::Omnigraph;
use omnigraph::error::OmniError;
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::{ExternalBlobBase, ExternalBlobExecutionScope, ExternalBlobPolicy};
use omnigraph_compiler::ir::ParamMap;
use omnigraph_compiler::query::ast::Literal;

use helpers::*;

// ─── Snapshot data-level isolation ──────────────────────────────────────────

#[tokio::test]
async fn snapshot_returns_stale_data_after_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Snapshot BEFORE mutation
    let snap_before = snapshot_main(&db).await.unwrap();

    // Insert a new person
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Snapshot AFTER mutation
    let snap_after = snapshot_main(&db).await.unwrap();

    // Old snapshot should still see 4 persons
    let ds_before = snap_before.open_dataset("node:Person").await.unwrap();
    assert_eq!(ds_before.count_rows(None).await.unwrap(), 4);

    // New snapshot should see 5 persons
    let ds_after = snap_after.open_dataset("node:Person").await.unwrap();
    assert_eq!(ds_after.count_rows(None).await.unwrap(), 5);

    // Verify Eve is NOT in old snapshot's data
    let batches_before: Vec<arrow_array::RecordBatch> = ds_before
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids_before = collect_column_strings(&batches_before, "id");
    assert!(!ids_before.contains(&"Eve".to_string()));

    // Verify Eve IS in new snapshot's data
    let batches_after: Vec<arrow_array::RecordBatch> = ds_after
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids_after = collect_column_strings(&batches_after, "id");
    assert!(ids_after.contains(&"Eve".to_string()));
}

// ─── LoadMode::Merge ────────────────────────────────────────────────────────

/// Append is strict insert, not an unchecked physical append. Reusing an
/// existing logical id must return the typed RFC-023 conflict and leave both
/// graph visibility and the underlying table pointer unchanged.
#[tokio::test]
async fn load_append_rejects_existing_id_without_update() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    load_jsonl(
        &db,
        r#"{"type":"Person","data":{"name":"Alice","age":30}}"#,
        LoadMode::Overwrite,
    )
    .await
    .unwrap();

    let before = snapshot_main(&db).await.unwrap();
    let before_manifest = before.graph_manifest_version();
    let before_table = before
        .dataset("node:Person")
        .expect("Person entry before strict conflict")
        .published_dataset_version;

    let err = load_jsonl(
        &db,
        r#"{"type":"Person","data":{"name":"Alice","age":99}}"#,
        LoadMode::Append,
    )
    .await
    .unwrap_err();
    match err {
        OmniError::KeyConflict {
            type_key,
            entity_id,
        } => {
            assert_eq!(type_key, "node:Person");
            assert_eq!(entity_id.as_deref(), Some("Alice"));
        }
        other => panic!("strict append must return typed KeyConflict, got {other:?}"),
    }

    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before_manifest,
        "rejected strict insert must not publish a graph version"
    );
    assert_eq!(
        after
            .dataset("node:Person")
            .expect("Person entry after strict conflict")
            .published_dataset_version,
        before_table,
        "rejected strict insert must not advance the Person table"
    );

    let rows = read_table(&db, "node:Person").await;
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    let batch = &rows[0];
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let alice = (0..batch.num_rows())
        .find(|&row| ids.value(row) == "Alice")
        .expect("Alice remains visible");
    assert_eq!(ages.value(alice), 30, "strict conflict must not upsert");
}

/// RFC-023's in-memory keyed adapter has a hard 8,192-row ceiling per
/// Append/Merge attempt. The boundary is inclusive; one row over must fail
/// before recovery is armed or either graph/table visibility moves. Overwrite
/// uses Lance's replacement transaction rather than the keyed adapter, so the
/// canonical strict graph-batch boundary must not impose that row ceiling.
#[tokio::test]
async fn load_keyed_write_row_cap_excludes_strict_overwrite() {
    const LIMIT: usize = 8192;
    const SCHEMA: &str = "node Thing { key: String @key }\n";

    let jsonl = |rows: usize| {
        let mut data = String::with_capacity(rows * 55);
        for row in 0..rows {
            data.push_str(&format!(
                "{{\"type\":\"Thing\",\"data\":{{\"key\":\"load-{row}\"}}}}\n"
            ));
        }
        data
    };

    let exact_dir = tempfile::tempdir().unwrap();
    let exact = Omnigraph::init(exact_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    load_jsonl(&exact, &jsonl(LIMIT), LoadMode::Append)
        .await
        .expect("the exact keyed row limit is inclusive");
    assert_eq!(count_rows(&exact, "node:Thing").await, LIMIT);

    let over_dir = tempfile::tempdir().unwrap();
    let over = Omnigraph::init(over_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let before = snapshot_main(&over).await.unwrap();
    let before_manifest = before.graph_manifest_version();
    let entry = before.dataset("node:Thing").unwrap();
    let before_table = entry.published_dataset_version;
    let table_uri = format!(
        "{}/{}",
        over.uri().trim_end_matches('/'),
        entry.dataset_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;
    let error = load_jsonl(&over, &jsonl(LIMIT + 1), LoadMode::Append)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 8192,
                actual: 8193,
            } if resource == "keyed entities for node:Thing"
        ),
        "one-over load must return the typed keyed-row limit, got {error:?}"
    );
    let after = snapshot_main(&over).await.unwrap();
    assert_eq!(after.graph_manifest_version(), before_manifest);
    assert_eq!(
        after
            .dataset("node:Thing")
            .unwrap()
            .published_dataset_version,
        before_table
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head,
        "one-over load must fail before a Lance table effect"
    );
    assert_eq!(count_rows(&over, "node:Thing").await, 0);
    let recovery_dir = over_dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "one-over load must fail before writing a recovery sidecar"
    );

    over.load_graph_batch("main", &jsonl(LIMIT + 1), LoadMode::Overwrite)
        .await
        .expect("a keyed-limit refusal must not poison a following strict Overwrite");
    assert_eq!(count_rows(&over, "node:Thing").await, LIMIT + 1);
}

/// The sibling 32 MiB cap is measured from the staged Arrow batch, not JSON
/// syntax. A single wide keyed row must be rejected with a typed limit before
/// either Lance HEAD or graph visibility can move.
#[tokio::test]
async fn load_keyed_write_byte_cap_rejects_wide_row_pre_effect() {
    const LIMIT: u64 = 32 * 1024 * 1024;
    const SCHEMA: &str = r#"
node Thing {
    key: String @key
    payload: String
}
"#;

    let dir = tempfile::tempdir().unwrap();
    let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    let before = snapshot_main(&db).await.unwrap();
    let before_manifest = before.graph_manifest_version();
    let entry = before.dataset("node:Thing").unwrap();
    let before_table = entry.published_dataset_version;
    let table_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        entry.dataset_path.trim_start_matches('/')
    );
    let before_head = Dataset::open(&table_uri).await.unwrap().version().version;

    let wide = "x".repeat(LIMIT as usize + 1024);
    let input =
        format!("{{\"type\":\"Thing\",\"data\":{{\"key\":\"wide\",\"payload\":\"{wide}\"}}}}");
    let error = load_jsonl(&db, &input, LoadMode::Append).await.unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: LIMIT,
                actual,
            } if (resource == "keyed entity bytes for node:Thing"
                || resource == "keyed parsed entity bytes for node:Thing")
                && actual > LIMIT
        ),
        "wide load must return the typed keyed-byte limit, got {error:?}"
    );

    let after = snapshot_main(&db).await.unwrap();
    assert_eq!(after.graph_manifest_version(), before_manifest);
    assert_eq!(
        after
            .dataset("node:Thing")
            .unwrap()
            .published_dataset_version,
        before_table
    );
    assert_eq!(
        Dataset::open(&table_uri).await.unwrap().version().version,
        before_head,
        "wide input must fail before a Lance table effect"
    );
    assert_eq!(count_rows(&db, "node:Thing").await, 0);
    let recovery_dir = dir.path().join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "wide input must fail before writing a recovery sidecar"
    );
}

async fn assert_lazy_external_blob_rejection_is_effect_free(
    db: &Omnigraph,
    graph_dir: &std::path::Path,
    table_key: &str,
    table_uri: &str,
    before_manifest: u64,
    before_table: u64,
    before_head: u64,
    case: &str,
) {
    let after = snapshot_branch(db, "feature").await.unwrap();
    assert_eq!(
        after.graph_manifest_version(),
        before_manifest,
        "{case}: manifest moved"
    );
    assert_eq!(
        after.dataset(table_key).unwrap().published_dataset_version,
        before_table,
        "{case}: feature table pointer moved"
    );
    assert_eq!(
        after.dataset(table_key).unwrap().native_dataset_branch,
        None,
        "{case}: rejection must not publish a deferred table fork"
    );
    let after_dataset = Dataset::open(table_uri).await.unwrap();
    assert_eq!(
        after_dataset.version().version,
        before_head,
        "{case}: rejection must precede a Lance table effect"
    );
    assert!(
        !after_dataset
            .list_branches()
            .await
            .unwrap()
            .keys()
            .any(|name| helpers::is_incarnation_of(name, "feature")),
        "{case}: rejected first touch created the native feature ref"
    );
    assert_eq!(count_rows_branch(db, "feature", table_key).await, 0);
    let recovery_dir = graph_dir.join("__recovery");
    assert!(
        !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
        "{case}: rejection must precede the recovery sidecar"
    );
}

/// External Blob admission is an operation-wide pre-effect gate: default deny
/// is a typed policy failure, an allowed but missing source is a typed source
/// failure, and an oversized source is rejected from metadata before its bytes
/// are read. None may create a lazy branch ref, move Lance HEAD or graph
/// visibility, or arm recovery. Linux CI also watches the sparse source for
/// `IN_ACCESS`, proving the oversized payload was never read. The same owner
/// pins the generic external-URI cell boundary independently of keyed entities:
/// exact-limit Overwrite succeeds and a cross-table one-over Overwrite is
/// typed and effect-free before HEAD.
#[tokio::test]
async fn external_blob_ingress_caps_are_operation_wide_and_pre_effect() {
    const LIMIT: u64 = 32 * 1024 * 1024;
    const SCHEMA: &str = r#"
node Document {
    title: String @key
    content: Blob
}

node Attachment {
    name: String @key
    payload: Blob
}
"#;

    let graph_dir = tempfile::tempdir().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let external_path = external_dir.path().join("oversized.blob");
    let external = std::fs::File::create(&external_path).unwrap();
    external.set_len(LIMIT + 1).unwrap();
    drop(external);

    #[cfg(target_os = "linux")]
    let access_watch = {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
        use std::os::unix::ffi::OsStrExt as _;

        let raw_fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
        assert!(
            raw_fd >= 0,
            "create inotify payload-read probe: {}",
            std::io::Error::last_os_error()
        );
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        let path = CString::new(external_path.as_os_str().as_bytes()).unwrap();
        let watch =
            unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), libc::IN_ACCESS) };
        assert!(
            watch >= 0,
            "watch external blob access: {}",
            std::io::Error::last_os_error()
        );
        (fd, watch)
    };

    let db = Omnigraph::init(graph_dir.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap();
    db.branch_create("feature").await.unwrap();
    let before = snapshot_branch(&db, "feature").await.unwrap();
    let before_manifest = before.graph_manifest_version();
    let entry = before.dataset("node:Document").unwrap();
    let before_table = entry.published_dataset_version;
    assert_eq!(
        entry.native_dataset_branch, None,
        "precondition: feature still inherits main's table version lazily"
    );
    let table_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        entry.dataset_path.trim_start_matches('/')
    );
    let before_dataset = Dataset::open(&table_uri).await.unwrap();
    let before_head = before_dataset.version().version;
    assert!(
        !before_dataset
            .list_branches()
            .await
            .unwrap()
            .keys()
            .any(|name| helpers::is_incarnation_of(name, "feature")),
        "precondition: feature has no native table ref before first touch"
    );
    let external_uri = url::Url::from_file_path(&external_path)
        .expect("external blob path is absolute")
        .to_string();
    let input = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "oversized",
            "content": external_uri.clone(),
        }
    })
    .to_string();

    let error = db
        .load("feature", &input, LoadMode::Append)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ExternalBlobPolicy { ref uri, .. } if uri == &external_uri
        ),
        "default deny must return typed ExternalBlobPolicy, got {error:?}"
    );
    assert_lazy_external_blob_rejection_is_effect_free(
        &db,
        graph_dir.path(),
        "node:Document",
        &table_uri,
        before_manifest,
        before_table,
        before_head,
        "default-deny external source",
    )
    .await;

    let base_uri = url::Url::from_directory_path(external_dir.path())
        .expect("external blob base is absolute")
        .to_string();
    let policy = ExternalBlobPolicy::allow(vec![
        ExternalBlobBase::new(base_uri, ExternalBlobExecutionScope::EmbeddedOnly).unwrap(),
    ])
    .unwrap();
    let db = db.with_external_blob_policy(policy.clone()).unwrap();

    let missing_uri = url::Url::from_file_path(external_dir.path().join("missing.blob"))
        .expect("missing external blob path is absolute")
        .to_string();
    let missing_input = serde_json::json!({
        "type": "Document",
        "data": {
            "title": "missing",
            "content": missing_uri.clone(),
        }
    })
    .to_string();
    let error = db
        .load("feature", &missing_input, LoadMode::Append)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ExternalBlobSource { ref uri, .. } if uri == &missing_uri
        ),
        "an allowed missing object must return typed ExternalBlobSource, got {error:?}"
    );
    assert_lazy_external_blob_rejection_is_effect_free(
        &db,
        graph_dir.path(),
        "node:Document",
        &table_uri,
        before_manifest,
        before_table,
        before_head,
        "missing allowed external source",
    )
    .await;

    let oversized_probes = omnigraph::instrumentation::MergeWriteProbes::default();
    let result = omnigraph::instrumentation::with_merge_write_probes(
        oversized_probes.clone(),
        db.load("feature", &input, LoadMode::Append),
    )
    .await;
    let error = result.unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: LIMIT,
                actual,
            } if resource == "materialized external blob payload bytes" && actual == LIMIT + 1
        ),
        "oversized external blob must be rejected from metadata before payload read, got {error:?}"
    );
    assert_eq!(
        oversized_probes.external_blob_payload_read_calls(),
        0,
        "oversized external payload must not be read"
    );

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd as _;

        let (fd, _watch) = access_watch;
        let mut events = [0_u8; 256];
        let read = unsafe {
            libc::read(
                fd.as_raw_fd(),
                events.as_mut_ptr().cast::<libc::c_void>(),
                events.len(),
            )
        };
        assert_eq!(read, -1, "oversized external payload was read");
        assert_eq!(
            std::io::Error::last_os_error().kind(),
            std::io::ErrorKind::WouldBlock,
            "payload-read probe failed unexpectedly"
        );
    }

    assert_lazy_external_blob_rejection_is_effect_free(
        &db,
        graph_dir.path(),
        "node:Document",
        &table_uri,
        before_manifest,
        before_table,
        before_head,
        "oversized allowed external source",
    )
    .await;

    // The copy ceiling belongs to the graph operation, not to each table.
    // Two individually valid sources selected for different tables must be
    // rejected from metadata before either payload is read or either lazy
    // table ref is created.
    let document_path = external_dir.path().join("document-half.blob");
    let attachment_path = external_dir.path().join("attachment-half.blob");
    for path in [&document_path, &attachment_path] {
        let file = std::fs::File::create(path).unwrap();
        file.set_len(LIMIT / 2 + 1).unwrap();
    }
    let document_uri = url::Url::from_file_path(&document_path)
        .expect("external document path is absolute")
        .to_string();
    let attachment_uri = url::Url::from_file_path(&attachment_path)
        .expect("external attachment path is absolute")
        .to_string();
    let cumulative_input = format!(
        "{}\n{}",
        serde_json::json!({
            "type": "Document",
            "data": {"title": "half", "content": document_uri},
        }),
        serde_json::json!({
            "type": "Attachment",
            "data": {"name": "half", "payload": attachment_uri},
        })
    );
    let cumulative_before = snapshot_branch(&db, "feature").await.unwrap();
    let attachment_entry = cumulative_before.dataset("node:Attachment").unwrap();
    let attachment_table = attachment_entry.published_dataset_version;
    let attachment_table_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        attachment_entry.dataset_path.trim_start_matches('/')
    );
    let attachment_head = Dataset::open(&attachment_table_uri)
        .await
        .unwrap()
        .version()
        .version;
    let probes = omnigraph::instrumentation::MergeWriteProbes::default();
    let error = omnigraph::instrumentation::with_merge_write_probes(
        probes.clone(),
        db.load("feature", &cumulative_input, LoadMode::Append),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: LIMIT,
                actual,
            } if resource == "materialized external blob payload bytes" && actual == LIMIT + 2
        ),
        "cross-table external payloads must share one operation budget, got {error:?}"
    );
    assert_eq!(
        probes.blob_payload_read_calls(),
        0,
        "aggregate admission must finish before either payload read"
    );
    assert_eq!(
        probes.external_blob_payload_read_calls(),
        0,
        "aggregate admission must finish before either external payload read"
    );
    assert_lazy_external_blob_rejection_is_effect_free(
        &db,
        graph_dir.path(),
        "node:Document",
        &table_uri,
        before_manifest,
        before_table,
        before_head,
        "cross-table aggregate external source",
    )
    .await;
    assert_lazy_external_blob_rejection_is_effect_free(
        &db,
        graph_dir.path(),
        "node:Attachment",
        &attachment_table_uri,
        before_manifest,
        attachment_table,
        attachment_head,
        "cross-table aggregate external source",
    )
    .await;

    // The external-reference cell ceiling is an operation-wide source-ingress
    // bound, not the keyed-row ceiling: it applies to Overwrite too. The exact
    // 8,192-cell boundary remains legal even though Overwrite is not otherwise
    // row-capped, while one extra cell split across two individually legal
    // tables fails before the first HEAD or graph effect.
    const REFERENCE_LIMIT: usize = 8192;
    let external_overwrite_input = |document_rows: usize,
                                    attachment_rows: usize,
                                    key_prefix: &str| {
        let mut data =
            String::with_capacity((document_rows + attachment_rows) * (external_uri.len() + 96));
        for row in 0..document_rows {
            writeln!(
                data,
                "{}",
                serde_json::json!({
                    "type": "Document",
                    "data": {
                        "title": format!("{key_prefix}-document-{row}"),
                        "content": external_uri,
                    }
                })
            )
            .unwrap();
        }
        for row in 0..attachment_rows {
            writeln!(
                data,
                "{}",
                serde_json::json!({
                    "type": "Attachment",
                    "data": {
                        "name": format!("{key_prefix}-attachment-{row}"),
                        "payload": external_uri,
                    }
                })
            )
            .unwrap();
        }
        data
    };

    let exact_graph = tempfile::tempdir().unwrap();
    let exact = Omnigraph::init(exact_graph.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap()
        .with_external_blob_policy(policy.clone())
        .unwrap();
    let exact_input = external_overwrite_input(REFERENCE_LIMIT, 0, "exact");
    let exact_probes = omnigraph::instrumentation::MergeWriteProbes::default();
    omnigraph::instrumentation::with_merge_write_probes(
        exact_probes.clone(),
        exact.load("main", &exact_input, LoadMode::Overwrite),
    )
    .await
    .expect("Overwrite must admit exactly 8,192 external URI cells");
    assert_eq!(
        exact_probes.external_blob_probe_inputs(),
        REFERENCE_LIMIT as u64
    );
    assert_eq!(
        exact_probes.external_blob_probe_calls(),
        1,
        "normalized-equivalent exact-limit cells must share one HEAD"
    );
    assert_eq!(
        exact_probes.external_blob_payload_read_calls(),
        0,
        "Overwrite retains admitted descriptors and must not copy payloads"
    );
    assert_eq!(count_rows(&exact, "node:Document").await, REFERENCE_LIMIT);

    let overflow_graph = tempfile::tempdir().unwrap();
    let overflow = Omnigraph::init(overflow_graph.path().to_str().unwrap(), SCHEMA)
        .await
        .unwrap()
        .with_external_blob_policy(policy)
        .unwrap();
    overflow.branch_create("feature").await.unwrap();
    let overflow_before = snapshot_branch(&overflow, "feature").await.unwrap();
    let overflow_manifest = overflow_before.graph_manifest_version();
    let mut overflow_tables = Vec::new();
    for table_key in ["node:Document", "node:Attachment"] {
        let entry = overflow_before.dataset(table_key).unwrap();
        let table_uri = format!(
            "{}/{}",
            overflow.uri().trim_end_matches('/'),
            entry.dataset_path.trim_start_matches('/')
        );
        overflow_tables.push((
            table_key,
            table_uri.clone(),
            entry.published_dataset_version,
            Dataset::open(&table_uri).await.unwrap().version().version,
        ));
    }
    let overflow_input =
        external_overwrite_input(REFERENCE_LIMIT / 2, REFERENCE_LIMIT / 2 + 1, "overflow");
    let overflow_probes = omnigraph::instrumentation::MergeWriteProbes::default();
    let error = omnigraph::instrumentation::with_merge_write_probes(
        overflow_probes.clone(),
        overflow.load("feature", &overflow_input, LoadMode::Overwrite),
    )
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
    assert_eq!(
        overflow_probes.external_blob_probe_inputs(),
        0,
        "cell-count refusal must precede preflight admission"
    );
    assert_eq!(overflow_probes.external_blob_probe_calls(), 0);
    assert_eq!(overflow_probes.external_blob_payload_read_calls(), 0);
    assert_eq!(overflow_probes.blob_payload_read_calls(), 0);
    for (table_key, table_uri, before_table, before_head) in overflow_tables {
        assert_lazy_external_blob_rejection_is_effect_free(
            &overflow,
            overflow_graph.path(),
            table_key,
            &table_uri,
            overflow_manifest,
            before_table,
            before_head,
            "cross-table Overwrite external-cell overflow",
        )
        .await;
    }
}

/// N handles inserting the same id have exactly one winner and every loser is
/// the typed RFC-023 conflict; disjoint ids still both survive. This is the
/// graph-level stress counterpart to Lance's lower conflict-matrix guard and
/// remains valid whether the process-local gates serialize preparation or
/// Lance resolves stale transactions.
#[tokio::test]
async fn n_concurrent_strict_loads_have_one_same_id_winner_and_keep_disjoint_ids() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    value: String
}
"#;
    let _db = Omnigraph::init(uri, schema).await.unwrap();

    const SAME_KEY_WRITERS: usize = 16;
    // Open every handle at the same empty graph image, then release every load
    // together. A Barrier gives this a real N-way contention window without a
    // timing-dependent sleep.
    let writers = join_all((0..SAME_KEY_WRITERS).map(|_| Omnigraph::open(uri)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let start = Arc::new(Barrier::new(SAME_KEY_WRITERS));
    let same_results = join_all(writers.into_iter().enumerate().map(|(writer, db)| {
        let start = Arc::clone(&start);
        async move {
            start.wait().await;
            let row =
                format!(r#"{{"type":"Thing","data":{{"key":"SAME","value":"writer-{writer}"}}}}"#);
            (writer, load_jsonl(&db, &row, LoadMode::Append).await)
        }
    }))
    .await;
    assert_eq!(
        same_results
            .iter()
            .filter(|(_, result)| result.is_ok())
            .count(),
        1,
        "same-id strict inserts must have exactly one winner: {same_results:?}"
    );
    let winner = same_results
        .iter()
        .find_map(|(writer, result)| result.is_ok().then_some(*writer))
        .expect("one same-id writer must win");
    for (writer, result) in &same_results {
        match result {
            Ok(_) => assert_eq!(*writer, winner),
            Err(OmniError::KeyConflict {
                type_key,
                entity_id,
            }) => {
                assert_eq!(type_key, "node:Thing");
                assert!(
                    entity_id
                        .as_deref()
                        .is_none_or(|entity_id| entity_id == "SAME"),
                    "writer {writer} reported the wrong conflicting entity id: {entity_id:?}"
                );
            }
            Err(other) => {
                panic!("same-id loser {writer} must return typed KeyConflict, got {other:?}")
            }
        }
    }

    let observer = Omnigraph::open(uri).await.unwrap();
    let same_rows = read_table(&observer, "node:Thing").await;
    let same_ids = collect_column_strings(&same_rows, "id");
    let same_values = collect_column_strings(&same_rows, "value");
    assert_eq!(same_ids, ["SAME"], "the N-way race must publish one row");
    assert_eq!(
        same_values,
        [format!("writer-{winner}")],
        "the persisted row must belong to the sole successful writer"
    );

    let left = Omnigraph::open(uri).await.unwrap();
    let right = Omnigraph::open(uri).await.unwrap();
    let (left, right) = tokio::join!(
        load_jsonl(
            &left,
            r#"{"type":"Thing","data":{"key":"LEFT","value":"l"}}"#,
            LoadMode::Append,
        ),
        load_jsonl(
            &right,
            r#"{"type":"Thing","data":{"key":"RIGHT","value":"r"}}"#,
            LoadMode::Append,
        ),
    );
    left.expect("disjoint strict insert LEFT must succeed");
    right.expect("disjoint strict insert RIGHT must succeed");

    let observer = Omnigraph::open(uri).await.unwrap();
    let rows = read_table(&observer, "node:Thing").await;
    let ids = collect_column_strings(&rows, "id");
    assert_eq!(ids.iter().filter(|id| id.as_str() == "SAME").count(), 1);
    assert!(ids.iter().any(|id| id == "LEFT"), "LEFT missing: {ids:?}");
    assert!(ids.iter().any(|id| id == "RIGHT"), "RIGHT missing: {ids:?}");
    assert_eq!(ids.len(), 3, "unexpected strict-load rows: {ids:?}");
}

#[tokio::test]
async fn load_merge_upserts_existing_and_inserts_new() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load Alice(30) and Bob(25) via Overwrite
    let initial = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob", "age": 25}}"#;
    load_jsonl(&db, initial, LoadMode::Overwrite).await.unwrap();

    assert_eq!(count_rows(&db, "node:Person").await, 2);

    // Merge: Alice updated to age=31, Charlie is new
    let merge_data = r#"{"type": "Person", "data": {"name": "Alice", "age": 31}}
{"type": "Person", "data": {"name": "Charlie", "age": 35}}"#;
    load_jsonl(&db, merge_data, LoadMode::Merge).await.unwrap();

    // Should have 3 persons total (not 4)
    assert_eq!(count_rows(&db, "node:Person").await, 3);

    // Verify individual values
    let batches = read_table(&db, "node:Person").await;
    let batch = &batches[0];
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    for i in 0..batch.num_rows() {
        match ids.value(i) {
            "Alice" => assert_eq!(ages.value(i), 31, "Alice should be updated to 31"),
            "Bob" => assert_eq!(ages.value(i), 25, "Bob should be unchanged"),
            "Charlie" => assert_eq!(ages.value(i), 35, "Charlie should be inserted"),
            other => panic!("unexpected person: {}", other),
        }
    }
}

/// Regression: two sequential `LoadMode::Merge` invocations against the
/// same set of keys must both succeed. Pre-fix, the second one failed
/// with `Ambiguous merge inserts are prohibited: multiple source rows
/// match the same target row on (id = "TEST-1")` even though every
/// source batch had one row per key.
///
/// Triggered by Lance's `processed_row_ids: Mutex<HashSet<u64>>`
/// (lance-6.0.1 `src/dataset/write/merge_insert.rs:2099`) double-
/// processing the same source/target match against datasets previously
/// rewritten by merge_insert. Worked around by opting
/// `MergeInsertBuilder` into `SourceDedupeBehavior::FirstSeen` in
/// `crates/omnigraph/src/table_store.rs` — see that file for the full
/// rationale and the safety pin (`loader_rejects_intra_batch_duplicate_keys`).
/// Tracked at MR-957; upstream: lance-format/lance#6877.
#[tokio::test]
async fn load_merge_repeated_against_overlapping_keys_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    required_val: String
    optional_val: String?
}
"#;
    let db = Omnigraph::init(uri, schema).await.unwrap();

    // Seed with 50 fully-populated rows (id + required + optional).
    let mut seed = String::new();
    for i in 1..=50 {
        seed.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i}","optional_val":"optional {i}"}}}}
"#,
        ));
    }
    load_jsonl(&db, &seed, LoadMode::Overwrite).await.unwrap();

    // Partial-schema delta — mirrors the bug report exactly: omits
    // `optional_val`. 25 existing keys + 5 new keys, one row per key.
    let mut delta = String::new();
    for i in (1..=25).chain(51..=55) {
        delta.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i} UPDATED"}}}}
"#,
        ));
    }

    load_jsonl(&db, &delta, LoadMode::Merge)
        .await
        .expect("first merge must succeed");
    assert_eq!(count_rows(&db, "node:Thing").await, 55);

    load_jsonl(&db, &delta, LoadMode::Merge)
        .await
        .expect("second merge against same keys must succeed");
    assert_eq!(count_rows(&db, "node:Thing").await, 55);
}

/// Safety pin for the `SourceDedupeBehavior::FirstSeen` workaround in
/// `crates/omnigraph/src/table_store.rs`. FirstSeen tells Lance to
/// silently skip a duplicate source row instead of erroring. Our use of
/// it depends on user-provided duplicates being rejected *before* the
/// batch reaches Lance — otherwise FirstSeen could silently drop user
/// data.
///
/// Defense in depth:
/// 1. The loader's `enforce_unique_constraints_intra_batch`
///    (`loader/mod.rs:1442`), invoked unconditionally on any node type
///    with a `@key`, errors on intra-batch duplicate `@key` values at
///    intake — pinned by this test across every `LoadMode`.
/// 2. The `check_batch_unique_by_keys` precondition at the top of
///    `merge_insert_batch` and `stage_merge_insert` is the final
///    fail-fast guard: even if a future caller bypasses the loader path
///    (e.g. branch-merge's `publish_rewritten_merge_table` builds its
///    own source batch directly), a real duplicate id reaches Lance
///    only after surfacing as an `OmniError::Manifest`, never silently
///    via FirstSeen. Pinned by the unit tests in `table_store::tests`.
#[tokio::test]
async fn loader_rejects_intra_batch_duplicate_keys() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    value: String
}
"#;
    let db = Omnigraph::init(uri, schema).await.unwrap();

    let dupes = r#"{"type":"Thing","data":{"key":"DUP","value":"first"}}
{"type":"Thing","data":{"key":"DUP","value":"second"}}
"#;

    for mode in [LoadMode::Overwrite, LoadMode::Append, LoadMode::Merge] {
        let err = load_jsonl(&db, dupes, mode).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("@unique violation") && msg.contains("DUP"),
            "load mode {mode:?} must reject intra-batch duplicate @key (got: {msg})"
        );
        assert_eq!(
            count_rows(&db, "node:Thing").await,
            0,
            "load mode {mode:?} must not persist any rows when the batch is rejected"
        );
    }
}

/// Internal schema v6 keys every physical graph table by `id`, even when the
/// public schema has no `@key`. The common pre-arm preparation must therefore
/// reject duplicate explicit ids for both strict insert and Overwrite before a
/// lazy branch's first-touch ref or recovery intent exists. In particular,
/// Overwrite cannot rely on Lance's unenforced-PK metadata to deduplicate rows.
#[tokio::test]
async fn lazy_load_rejects_duplicate_physical_ids_before_arm_for_append_and_overwrite() {
    const SCHEMA: &str = r#"
node Thing {
    value: String
}
"#;
    const DUPLICATES: &str = r#"{"type":"Thing","data":{"id":"DUP","value":"first"}}
{"type":"Thing","data":{"id":"DUP","value":"second"}}
"#;

    for mode in [LoadMode::Append, LoadMode::Overwrite] {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        db.branch_create("feature").await.unwrap();

        let before = snapshot_branch(&db, "feature").await.unwrap();
        let before_manifest = before.graph_manifest_version();
        let entry = before.dataset("node:Thing").unwrap();
        let before_table = entry.published_dataset_version;
        assert_eq!(
            entry.native_dataset_branch, None,
            "precondition: feature inherits the empty main table lazily"
        );
        let table_uri = format!(
            "{}/{}",
            db.uri().trim_end_matches('/'),
            entry.dataset_path.trim_start_matches('/')
        );
        let before_dataset = Dataset::open(&table_uri).await.unwrap();
        let before_head = before_dataset.version().version;
        assert!(
            !before_dataset
                .list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "feature")),
            "precondition: feature has no native table ref before first touch"
        );

        let error = db
            .load("feature", DUPLICATES, mode)
            .await
            .expect_err("duplicate physical ids must fail before first touch");
        assert!(
            matches!(
                error,
                OmniError::KeyConflict {
                    ref type_key,
                    ref entity_id
                }
                    if type_key == "node:Thing" && entity_id.as_deref() == Some("DUP")
            ),
            "load mode {mode:?} must return typed physical-id conflict, got {error:?}"
        );

        let after = snapshot_branch(&db, "feature").await.unwrap();
        assert_eq!(
            after.graph_manifest_version(),
            before_manifest,
            "load mode {mode:?} must not publish the feature manifest"
        );
        let after_entry = after.dataset("node:Thing").unwrap();
        assert_eq!(after_entry.published_dataset_version, before_table);
        assert_eq!(
            after_entry.native_dataset_branch, None,
            "load mode {mode:?} must not publish a deferred table fork"
        );
        let after_dataset = Dataset::open(&table_uri).await.unwrap();
        assert_eq!(
            after_dataset.version().version,
            before_head,
            "load mode {mode:?} must not advance raw table HEAD"
        );
        assert!(
            !after_dataset
                .list_branches()
                .await
                .unwrap()
                .keys()
                .any(|name| helpers::is_incarnation_of(name, "feature")),
            "load mode {mode:?} must fail before creating the native feature ref"
        );
        assert_eq!(count_rows_branch(&db, "feature", "node:Thing").await, 0);
        let recovery_dir = dir.path().join("__recovery");
        assert!(
            !recovery_dir.exists() || std::fs::read_dir(recovery_dir).unwrap().next().is_none(),
            "load mode {mode:?} must fail before writing a recovery sidecar"
        );
    }
}

/// Regression for MR-983: a node-level composite `@unique(a, b)` must be
/// enforced as a true composite key, not degraded into independent
/// single-field checks. Pre-fix, `unique_property_names_for_node` flattened
/// every constraint group into one property list, so `@unique(source,
/// external_id)` was enforced as `@unique(source)` *and* `@unique(external_id)`
/// — rejecting rows that were unique on the composite key and naming only the
/// first field in the error.
#[tokio::test]
async fn loader_enforces_composite_unique_as_composite_key() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node ExternalID {
    slug: String @key
    source: String @index
    external_id: String @index
    @unique(source, external_id)
}
"#;
    let db = Omnigraph::init(uri, schema).await.unwrap();

    // Same `source`, different `external_id` → unique on the composite key.
    // This is the exact repro from MR-983 and must be accepted.
    let composite_ok = r#"{"type":"ExternalID","data":{"slug":"a","source":"whatsapp","external_id":"+E.164"}}
{"type":"ExternalID","data":{"slug":"b","source":"whatsapp","external_id":"pn:12345"}}
"#;
    load_jsonl(&db, composite_ok, LoadMode::Overwrite)
        .await
        .expect("rows unique on the composite (source, external_id) must be accepted");
    assert_eq!(count_rows(&db, "node:ExternalID").await, 2);

    // Both composite columns equal → genuine violation. The error must name
    // the whole composite, not just the first field.
    let composite_dupe = r#"{"type":"ExternalID","data":{"slug":"c","source":"whatsapp","external_id":"dup"}}
{"type":"ExternalID","data":{"slug":"d","source":"whatsapp","external_id":"dup"}}
"#;
    let err = load_jsonl(&db, composite_dupe, LoadMode::Overwrite)
        .await
        .unwrap_err();
    let msg = err.to_string();
    // Columns are canonicalized to sorted order in the catalog, so the
    // message reads `(external_id, source)`; assert order-agnostically that
    // both composite columns are named (not just the first, as pre-fix).
    assert!(
        msg.contains("@unique violation") && msg.contains("source") && msg.contains("external_id"),
        "composite violation must name both columns (got: {msg})"
    );
}

/// Guard: the intake path (load/insert/update) and the branch-merge path must
/// derive the same composite `@unique(a, b)` key, so a pair of rows unique on
/// the tuple is accepted by BOTH. Both paths now key on the tuple itself (no
/// separator), so a value containing any byte — including the `|` that an
/// earlier merge-path join used as its separator — can't forge a collision.
/// `("x|y", "z")` and `("x", "y|z")` are distinct tuples and must survive a
/// load-on-branch then merge without a phantom `UniqueViolation`. This pins the
/// cross-path consistency against any future drift in the shared keying.
#[tokio::test]
async fn composite_unique_key_is_consistent_across_intake_and_merge() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Item {
    slug: String @key
    a: String @index
    b: String @index
    @unique(a, b)
}
"#;
    let insert_item = r#"
query insert_item($slug: String, $a: String, $b: String) {
    insert Item { slug: $slug, a: $a, b: $b }
}
"#;
    let main = Omnigraph::init(uri, schema).await.unwrap();
    main.branch_create("feature").await.unwrap();

    // Two rows unique on the composite (a, b), where `a`/`b` carry a literal
    // `|`. Distinct under a tuple key; identical (`x|y|z`) under a `|`-join.
    let feature = Omnigraph::open(uri).await.unwrap();
    feature
        .mutate(
            "feature",
            insert_item,
            "insert_item",
            &params(&[("$slug", "r1"), ("$a", "x|y"), ("$b", "z")]),
        )
        .await
        .expect("intake must accept the first composite-unique row");
    feature
        .mutate(
            "feature",
            insert_item,
            "insert_item",
            &params(&[("$slug", "r2"), ("$a", "x"), ("$b", "y|z")]),
        )
        .await
        .expect("intake must accept the second composite-unique row (distinct on the tuple)");

    // The merge re-validates uniqueness over the adopted source rows. Both
    // rows are unique on (a, b), so this must merge cleanly with no phantom
    // conflict — intake and merge must key the tuple identically.
    let merge_result = feature.branch_merge("feature", "main").await;
    assert!(
        merge_result.is_ok(),
        "rows unique on the composite (a, b) must merge cleanly; \
         intake and merge must key the tuple the same way (got: {:?})",
        merge_result.err()
    );

    let reopened = Omnigraph::open(uri).await.unwrap();
    assert_eq!(count_rows(&reopened, "node:Item").await, 2);
}

/// Canary for the upstream Lance gap that the `FirstSeen` workaround
/// in `table_store.rs` masks. The bug class is "Window 2": load →
/// indices built explicitly → merge → merge. Even with the engine
/// fully aligned to the "indexes are derived state" invariant
/// (MR-848), as long as an `id` index has been built between the
/// first and second merge_insert, the Lance internal that triggers
/// the bug remains reachable.
///
/// This test runs the Window-2 sequence under the FirstSeen workaround.
/// It is expected to pass today. If a future Lance upgrade or local
/// change makes it START failing, the workaround has lost effectiveness
/// (upstream Lance changed something, or the FirstSeen setter was
/// dropped from `table_store.rs`). If a future Lance upgrade fixes the
/// bug class, this test continues to pass and the FirstSeen setter can
/// be retired.
///
/// Tracked at MR-957; upstream: lance-format/lance#6877.
#[tokio::test]
async fn load_merge_window_2_documents_upstream_lance_gap() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Thing {
    key: String @key
    required_val: String
    optional_val: String?
}
"#;
    let db = Omnigraph::init(uri, schema).await.unwrap();

    let mut seed = String::new();
    for i in 1..=50 {
        seed.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i}","optional_val":"optional {i}"}}}}
"#,
        ));
    }
    load_jsonl(&db, &seed, LoadMode::Overwrite).await.unwrap();

    // Explicit ensure_indices between seed and the merges — the Window
    // 2 trigger. The eager-build behavior (MR-583) means the BTREE on
    // `id` is already present here, but calling explicitly pins the
    // invariant for the post-MR-848 future where the eager build is
    // gone.
    db.ensure_indices().await.unwrap();

    let mut delta = String::new();
    for i in (1..=25).chain(51..=55) {
        delta.push_str(&format!(
            r#"{{"type":"Thing","data":{{"key":"TEST-{i}","required_val":"required {i} UPDATED"}}}}
"#,
        ));
    }

    // Both merges must succeed under the FirstSeen workaround.
    // `processed_row_ids` re-processes the same target row_id under
    // the default `SourceDedupeBehavior::Fail`; FirstSeen tolerates it.
    load_jsonl(&db, &delta, LoadMode::Merge)
        .await
        .expect("first merge after ensure_indices must succeed");
    db.ensure_indices().await.unwrap();
    load_jsonl(&db, &delta, LoadMode::Merge).await.expect(
        "second merge after ensure_indices must succeed \
             (Window 2 canary: drop the FirstSeen setter in table_store.rs \
             only when this stays green WITHOUT it)",
    );
    assert_eq!(count_rows(&db, "node:Thing").await, 55);
}

#[tokio::test]
async fn cross_type_traversal_deduplicates_duplicate_edges() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = r#"
node Person { name: String @key }
node Company { name: String @key }
edge WorksAt: Person -> Company
"#;
    let data = r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Company","data":{"name":"Acme"}}
{"edge":"WorksAt","from":"Alice","to":"Acme"}
{"edge":"WorksAt","from":"Alice","to":"Acme"}"#;
    let query = r#"
query company($name: String) {
    match {
        $p: Person { name: $name }
        $p worksAt $c
    }
    return { $c.name }
}
"#;

    let mut db = Omnigraph::init(uri, schema).await.unwrap();
    load_jsonl(&db, data, LoadMode::Overwrite).await.unwrap();

    let result = query_main(&mut db, query, "company", &params(&[("$name", "Alice")]))
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

// ─── Multi-writer refresh ───────────────────────────────────────────────────

#[tokio::test]
async fn explicit_target_query_sees_other_writer_commits_without_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();

    // Two independent handles to the same graph
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    // Writer 1 inserts Eve
    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Explicit-target reads resolve the latest branch head and should see Eve
    let qr = query_main(
        &mut db2,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(qr.num_rows(), 1, "explicit target reads should see Eve");
}

#[tokio::test]
async fn explicit_target_query_rebuilds_graph_index_after_external_edge_write() {
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    let warm = query_main(
        &mut db2,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(warm.num_rows(), 2);

    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Alice"), ("$to", "Diana")]),
    )
    .await
    .unwrap();

    let refreshed = query_main(
        &mut db2,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(
        refreshed.num_rows(),
        3,
        "explicit target reads should rebuild topology after edge change"
    );

    let batch = refreshed.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
    assert!(values.contains(&"Bob"));
    assert!(values.contains(&"Diana"));
}

// ─── Null handling ──────────────────────────────────────────────────────────

#[tokio::test]
async fn null_values_in_filter_and_projection() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

    // Load data: Alice has age, Bob has null age, Charlie has age
    let data = r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}
{"type": "Person", "data": {"name": "Bob"}}
{"type": "Person", "data": {"name": "Charlie", "age": 35}}"#;
    load_jsonl(&db, data, LoadMode::Overwrite).await.unwrap();

    // Filter: age > 30 should exclude Bob (null) and Alice (30), keep Charlie (35)
    let queries = r#"
query older_than_30() {
    match {
        $p: Person
        $p.age > 30
    }
    return { $p.name, $p.age }
    order { $p.age desc }
}

query all_persons() {
    match { $p: Person }
    return { $p.name, $p.age }
    order { $p.age desc }
}
"#;

    let result = query_main(&mut db, queries, "older_than_30", &ParamMap::new())
        .await
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let batch = &result.batches()[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Charlie");

    // Projection: Bob's age should be null
    let all = query_main(&mut db, queries, "all_persons", &ParamMap::new())
        .await
        .unwrap();
    let batch = &all.batches()[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    for i in 0..batch.num_rows() {
        if ids.value(i) == "Bob" {
            assert!(ages.is_null(i), "Bob's age should be null");
        }
    }
}

// ─── Graph index after node+edge insert ─────────────────────────────────────

#[tokio::test]
async fn traversal_works_after_node_then_edge_insert() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Warm up the graph index cache by running a traversal
    let _ = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();

    // Insert a new node (does NOT invalidate graph index)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Frank")], &[("$age", 40)]),
    )
    .await
    .unwrap();

    // Insert an edge from Frank → Alice (DOES invalidate graph index)
    mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "add_friend",
        &params(&[("$from", "Frank"), ("$to", "Alice")]),
    )
    .await
    .unwrap();

    // Traversal should work: Frank → Alice
    let result = query_main(
        &mut db,
        TEST_QUERIES,
        "friends_of",
        &params(&[("$name", "Frank")]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    let batch = result.concat_batches().unwrap();
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
}

// ─── Edge property insert ───────────────────────────────────────────────────

#[tokio::test]
async fn insert_edge_with_property() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    // Knows has `since: Date?` property
    let queries = r#"
query add_friend_since($from: String, $to: String, $since: Date) {
    insert Knows { from: $from, to: $to, since: $since }
}
"#;
    let mut p = params(&[("$from", "Diana"), ("$to", "Bob")]);
    p.insert("since".to_string(), Literal::Date("2024-06-15".to_string()));

    let result = mutate_main(&mut db, queries, "add_friend_since", &p)
        .await
        .unwrap();
    assert_eq!(result.affected_edges, 1);

    // Verify the edge property was stored
    let batches = read_table(&db, "edge:Knows").await;
    let mut found = false;
    for batch in &batches {
        let srcs = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dsts = batch
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let since = batch
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if srcs.value(i) == "Diana" && dsts.value(i) == "Bob" {
                assert!(!since.is_null(i), "since should not be null");
                found = true;
            }
        }
    }
    assert!(found, "should find Diana→Bob edge");
}

// ─── Update / delete no-match ───────────────────────────────────────────────

#[tokio::test]
async fn update_nonexistent_returns_zero_affected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Nobody")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 0);
}

#[tokio::test]
async fn delete_nonexistent_returns_zero_affected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = init_and_load(&dir).await;

    let result = mutate_main(
        &mut db,
        MUTATION_QUERIES,
        "remove_person",
        &params(&[("$name", "Nobody")]),
    )
    .await
    .unwrap();

    assert_eq!(result.affected_nodes, 0);
    assert_eq!(result.affected_edges, 0);

    // All 4 persons still intact
    assert_eq!(count_rows(&db, "node:Person").await, 4);
}

// ─── Large batch load ───────────────────────────────────────────────────────

#[tokio::test]
async fn large_batch_load_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let schema = r#"
node Item {
    name: String @key
    value: I32
}
"#;
    let mut db = Omnigraph::init(uri, schema).await.unwrap();

    // Generate 500 items
    let mut lines = Vec::with_capacity(500);
    for i in 0..500 {
        lines.push(format!(
            r#"{{"type": "Item", "data": {{"name": "item_{:04}", "value": {}}}}}"#,
            i, i
        ));
    }
    let data = lines.join("\n");
    load_jsonl(&db, &data, LoadMode::Overwrite).await.unwrap();

    assert_eq!(count_rows(&db, "node:Item").await, 500);

    // Query with filter — value > 490
    let queries = r#"
query high_value() {
    match {
        $i: Item
        $i.value > 490
    }
    return { $i.name, $i.value }
    order { $i.value asc }
}
"#;
    let result = query_main(&mut db, queries, "high_value", &ParamMap::new())
        .await
        .unwrap();

    // Items 491..499 = 9 items
    assert_eq!(result.num_rows(), 9);
    let batch = &result.batches()[0];
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.value(0), 491);
    assert_eq!(values.value(8), 499);
}

// ─── Long-lived strict handle refreshes before preparation ───────────────

#[tokio::test]
async fn long_lived_handle_prepares_strict_mutation_from_current_head() {
    // Merely opening a handle before another completed commit does not make the
    // next attempt stale: open_write_txn probes the manifest incarnation and
    // captures the current branch authority before it plans the strict update.
    // ReadSetChanged is reserved for movement during an already-prepared
    // attempt (covered deterministically in the failpoint suite).
    let dir = tempfile::tempdir().unwrap();
    let _db = init_and_load(&dir).await;
    drop(_db);

    let uri = dir.path().to_str().unwrap();
    let mut db1 = Omnigraph::open(uri).await.unwrap();
    let mut db2 = Omnigraph::open(uri).await.unwrap();

    // Writer 1 inserts Eve — advances the Person sub-table.
    mutate_main(
        &mut db1,
        MUTATION_QUERIES,
        "insert_person",
        &mixed_params(&[("$name", "Eve")], &[("$age", 22)]),
    )
    .await
    .unwrap();

    // Writer 2's handle predates Eve, but its strict attempt prepares from the
    // current head and succeeds in one call.
    mutate_main(
        &mut db2,
        MUTATION_QUERIES,
        "set_age",
        &mixed_params(&[("$name", "Alice")], &[("$age", 99)]),
    )
    .await
    .unwrap();

    // Both Writer 1's insert and Writer 2's update are visible.
    let result = query_main(
        &mut db2,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Alice")]),
    )
    .await
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.to_rust_json()[0]["p.age"], serde_json::json!(99));

    let eve = query_main(
        &mut db2,
        TEST_QUERIES,
        "get_person",
        &params(&[("$name", "Eve")]),
    )
    .await
    .unwrap();
    assert_eq!(eve.num_rows(), 1, "concurrent insert should be preserved");
}
