//! Lance API surface guards.
//!
//! Each guard pins a Lance API surface that OmniGraph relies on. If a future
//! Lance bump silently renames a variant, restructures a public struct, or
//! flips a method to async, the corresponding guard either fails to compile
//! (compile-time guards) or fails at runtime (runtime guards). The purpose
//! is to turn silent-break risks into red CI bars on the *next* Lance bump,
//! rather than into wrong-state recovery in production.
//!
//! Pair this file with `docs/dev/lance.md`'s alignment audit stanza: any
//! Lance bump runs `cargo test -p omnigraph-engine --test lance_surface_guards`
//! first as the smoke check.
//!
//! ## Compile-only guards
//!
//! Functions prefixed with `_compile_` are gated with a broad `#[allow(...)]`
//! and never called. They exist to make `cargo build -p omnigraph-engine --tests`
//! enforce the API shape. Using `unimplemented!()` as a placeholder lets type
//! inference proceed without running anything.
//!
//! ## Runtime guards
//!
//! Functions decorated `#[tokio::test]` actually run; they construct real
//! values and assert field shapes / types.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::refs::BranchIdentifier;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::write::delete::DeleteResult;
use lance::dataset::write::merge_insert::UncommittedMergeInsert;
use lance::dataset::write::merge_insert::inserted_rows::{FilterType, KeyExistenceFilter};
use lance::dataset::{
    CommitBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode,
    WriteParams,
};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_core::{
    ROW_ADDR, ROW_CREATED_AT_VERSION, ROW_ID, ROW_LAST_UPDATED_AT_VERSION, ROW_OFFSET,
    is_system_column,
};
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_io::object_store::ObjectStoreRegistry;
use lance_namespace::LanceNamespace;
use lance_table::io::commit::{ManifestLocation, ManifestNamingScheme};
use omnigraph_compiler::schema::parser::parse_schema;

#[test]
fn compiler_rejects_five_surveyed_lance_virtual_system_columns() {
    let names = [
        ROW_ID,
        ROW_ADDR,
        ROW_OFFSET,
        ROW_CREATED_AT_VERSION,
        ROW_LAST_UPDATED_AT_VERSION,
    ];
    for name in names {
        assert!(is_system_column(name), "Lance no longer reserves {name}");
        let error = parse_schema(&format!("node N {{ {name}: String }}"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("reserved"), "unexpected error: {error}");
        assert!(error.contains(name), "unexpected error: {error}");
    }
}

/// Helper: build a small fresh dataset in a tempdir. Pinned at V2_2 to match
/// production write paths (blob v2 requires V2_2; see `docs/dev/lance.md`).
async fn fresh_dataset(uri: &str) -> Dataset {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap()
}

/// RFC-024 Gate A candidate built exclusively from public Lance surfaces.
///
/// `BranchIdentifier` distinguishes named-ref lifetimes, the current
/// transaction UUID distinguishes main-dataset replacement where main's
/// identifier is necessarily empty, and the manifest e_tag gives object stores
/// an independent physical-object witness. The e_tag remains optional at the
/// type level for backends that omit it; the pinned Lance local filesystem backend
/// resolves a metadata-derived e_tag, and the local guards below require it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicPhysicalRefIncarnation {
    branch_identifier: BranchIdentifier,
    transaction_uuid: String,
    manifest_e_tag: Option<String>,
}

async fn public_physical_ref_incarnation(dataset: &Dataset) -> PublicPhysicalRefIncarnation {
    let branch_identifier = dataset
        .branch_identifier()
        .await
        .expect("the current dataset/ref must expose its public BranchIdentifier");
    let transaction = dataset
        .read_transaction()
        .await
        .expect("the current manifest transaction must be readable")
        .expect("a heads-format candidate cannot admit a manifest without a transaction UUID");
    assert!(
        !transaction.uuid.is_empty(),
        "a heads-format candidate cannot admit an empty transaction UUID"
    );
    let manifest_e_tag = dataset.manifest_location().e_tag.clone();
    let revalidated_branch_identifier = dataset
        .branch_identifier()
        .await
        .expect("the current dataset/ref must re-expose its public BranchIdentifier");
    assert_eq!(
        branch_identifier, revalidated_branch_identifier,
        "physical-ref capture must reject branch movement while reading manifest authority"
    );
    PublicPhysicalRefIncarnation {
        branch_identifier,
        transaction_uuid: transaction.uuid,
        manifest_e_tag,
    }
}

async fn recreate_named_branch_and_assert_token_changes(main: &mut Dataset, require_etag: bool) {
    let base_version = main.version().version;
    let first = main
        .create_branch("rfc024-physical-token", base_version, None)
        .await
        .expect("first named-ref incarnation must be created");
    let first_version = first.version().version;
    let first_token = public_physical_ref_incarnation(&first).await;

    main.force_delete_branch("rfc024-physical-token")
        .await
        .expect("first named-ref incarnation must be deleted completely");
    let second = main
        .create_branch("rfc024-physical-token", base_version, None)
        .await
        .expect("same-name named ref must be recreatable");
    let second_version = second.version().version;
    let second_token = public_physical_ref_incarnation(&second).await;

    assert_eq!(
        first_version, second_version,
        "the ABA fixture must recreate the named ref at the same numeric version"
    );
    assert_ne!(
        first_token.branch_identifier, second_token.branch_identifier,
        "a same-name/same-version named-ref recreation must mint a new BranchIdentifier"
    );
    assert_ne!(
        first_token.transaction_uuid, second_token.transaction_uuid,
        "the recreated named ref's current manifest must carry a new transaction UUID"
    );
    assert_ne!(
        first_token, second_token,
        "the complete public physical-ref token must reject named-ref ABA"
    );
    if require_etag {
        let first_etag = first_token
            .manifest_e_tag
            .as_deref()
            .expect("the selected backend must expose the first named-ref manifest e_tag");
        let second_etag = second_token
            .manifest_e_tag
            .as_deref()
            .expect("the selected backend must expose the recreated named-ref manifest e_tag");
        assert_ne!(
            first_etag, second_etag,
            "the selected backend must distinguish the recreated named-ref manifest object by e_tag"
        );
    }
}

/// RFC-023 substrate fixture: a V2_2 table whose internal `id` is Lance's
/// unenforced primary key. `note` is nullable so the matched-only partial-schema
/// guard can omit it without testing an unrelated nullability rejection.
async fn fresh_pk_dataset(uri: &str) -> Dataset {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
        Field::new("value", DataType::Int32, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap()
}

fn pk_full_row(dataset: &Dataset, id: &str, value: i32) -> RecordBatch {
    let schema = Arc::new(Schema::from(dataset.schema()));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id])),
            Arc::new(Int32Array::from(vec![value])),
            Arc::new(StringArray::from(vec![Some("guard")])),
        ],
    )
    .unwrap()
}

async fn stage_pk_merge(
    dataset: Arc<Dataset>,
    batch: RecordBatch,
    on: &str,
    when_matched: WhenMatched,
    when_not_matched: WhenNotMatched,
    use_index: Option<bool>,
) -> UncommittedMergeInsert {
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let mut builder = MergeInsertBuilder::try_new(dataset, vec![on.to_string()]).unwrap();
    builder
        .when_matched(when_matched)
        .when_not_matched(when_not_matched)
        .conflict_retries(0);
    if let Some(use_index) = use_index {
        builder.use_index(use_index);
    }
    builder
        .try_build()
        .unwrap()
        .execute_uncommitted(reader)
        .await
        .unwrap()
}

fn transaction_inserted_rows_filter(transaction: &Transaction) -> Option<&KeyExistenceFilter> {
    match &transaction.operation {
        Operation::Update {
            inserted_rows_filter,
            ..
        } => inserted_rows_filter.as_ref(),
        other => panic!("expected merge_insert to stage Operation::Update, got {other:?}"),
    }
}

fn staged_inserted_rows_filter(staged: &UncommittedMergeInsert) -> Option<&KeyExistenceFilter> {
    let transaction_filter = transaction_inserted_rows_filter(&staged.transaction);
    assert_eq!(
        staged.inserted_rows_filter.as_ref(),
        transaction_filter,
        "the public uncommitted result and its transaction must expose the same key filter"
    );
    transaction_filter
}

fn assert_bloom_empty(filter: &KeyExistenceFilter, expected_empty: bool, case: &str) {
    let FilterType::Bloom { bitmap, .. } = &filter.filter else {
        panic!("{case}: pinned Lance should emit a Bloom key filter")
    };
    assert_eq!(
        bitmap.iter().all(|byte| *byte == 0),
        expected_empty,
        "{case}: unexpected Bloom-filter population state"
    );
}

#[derive(Debug, Clone, Copy)]
enum ConflictMatrixTxn {
    Filtered { id: &'static str, value: i32 },
    UnfilteredUpdate { id: &'static str, value: i32 },
    Append { id: &'static str, value: i32 },
}

async fn stage_conflict_matrix_txn(dataset: Arc<Dataset>, kind: ConflictMatrixTxn) -> Transaction {
    let (id, value) = match kind {
        ConflictMatrixTxn::Filtered { id, value }
        | ConflictMatrixTxn::UnfilteredUpdate { id, value }
        | ConflictMatrixTxn::Append { id, value } => (id, value),
    };
    let batch = pk_full_row(dataset.as_ref(), id, value);

    let transaction = match kind {
        ConflictMatrixTxn::Filtered { .. } => {
            stage_pk_merge(
                dataset,
                batch,
                "id",
                WhenMatched::UpdateAll,
                WhenNotMatched::InsertAll,
                Some(false),
            )
            .await
            .transaction
        }
        ConflictMatrixTxn::UnfilteredUpdate { .. } => {
            stage_pk_merge(
                dataset,
                batch,
                "value",
                WhenMatched::UpdateAll,
                WhenNotMatched::InsertAll,
                Some(false),
            )
            .await
            .transaction
        }
        ConflictMatrixTxn::Append { .. } => InsertBuilder::new(dataset)
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute_uncommitted(vec![batch])
            .await
            .unwrap(),
    };

    match kind {
        ConflictMatrixTxn::Filtered { .. } => assert!(
            transaction_inserted_rows_filter(&transaction).is_some(),
            "filtered matrix fixture must carry the PK filter"
        ),
        ConflictMatrixTxn::UnfilteredUpdate { .. } => assert!(
            transaction_inserted_rows_filter(&transaction).is_none(),
            "non-PK merge matrix fixture must be an unfiltered Update"
        ),
        ConflictMatrixTxn::Append { .. } => assert!(
            matches!(&transaction.operation, Operation::Append { .. }),
            "append matrix fixture must stage Operation::Append"
        ),
    }
    transaction
}

// --- Guard 1: LanceError::TooMuchWriteContention variant exists ------------
//
// `db/manifest/publisher.rs::map_lance_publish_error` pattern-matches on this
// variant to surface typed `OmniError::ManifestRowLevelCasContention`. If
// Lance renames the variant or removes the builder, this guard fails.

#[tokio::test]
async fn lance_error_too_much_write_contention_variant_exists() {
    let err = lance::Error::too_much_write_contention("guard");
    assert!(
        matches!(err, lance::Error::TooMuchWriteContention { .. }),
        "Lance::Error::TooMuchWriteContention variant missing or renamed; \
         update db/manifest/publisher.rs::map_lance_publish_error and \
         this guard, then re-pin docs/dev/lance.md."
    );
}

// --- Guard 1c: LanceError::DatasetAlreadyExists variant exists --------------
//
// `db/commit_graph.rs` and `db/recovery_audit.rs` create internal Lance tables
// with a create-or-open idempotency fallback: a concurrent/prior create races,
// and the `DatasetAlreadyExists` arm falls back to `Dataset::open`. They match
// the typed variant, NOT the display string ("Dataset already exists: ..."),
// which is not a Lance API contract. If Lance renames the variant the match
// silently stops catching the race and a re-create errors instead of opening —
// this guard turns red to force an update.

#[tokio::test]
async fn lance_error_dataset_already_exists_variant_exists() {
    let err = lance::Error::dataset_already_exists("guard");
    assert!(
        matches!(err, lance::Error::DatasetAlreadyExists { .. }),
        "Lance::Error::DatasetAlreadyExists variant missing or renamed; update the \
         db/commit_graph.rs + db/recovery_audit.rs create-or-open fallbacks and \
         this guard, then re-pin docs/dev/lance.md."
    );
}

// --- Guard 2: ManifestLocation field shape ---------------------------------
//
// `db/manifest/metadata.rs:84-88` reads `.path`, `.size`, `.e_tag`,
// `.naming_scheme` off `dataset.manifest_location()`. If any field renames
// or changes type, this guard fails to compile.

#[tokio::test]
async fn manifest_location_field_shape() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard.lance");
    let ds = fresh_dataset(uri.to_str().unwrap()).await;

    let loc = ds.manifest_location();
    // Explicit type bindings — these are the load-bearing assertions. If a
    // type drifts (e.g. .size: Option<u64> → .size: u64), this fails to
    // compile.
    let _path: &object_store::path::Path = &loc.path;
    let _size: Option<u64> = loc.size;
    let _e_tag: Option<String> = loc.e_tag.clone();
    let _scheme: ManifestNamingScheme = loc.naming_scheme;
    // Runtime sanity — naming_scheme should produce a Debug string we use
    // verbatim in `TableVersionMetadata::naming_scheme`.
    assert!(!format!("{:?}", loc.naming_scheme).is_empty());
}

// --- Guard 2a: shared client pool with cache-isolated Sessions ---------------
//
// OmniGraph uses a cached data Session and a zero-cache control Session. They
// must reuse the same object-store client pool without turning mutable-tip
// control metadata into a second shared cache. This exercises the public Lance
// construction with real Dataset opens: the second Session hits the first
// Session's live object store, while its own zero-sized metadata cache remains
// empty.

#[tokio::test]
async fn cached_and_zero_cache_sessions_share_store_registry_not_metadata_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared-registry-isolated-caches.lance");
    let uri = path.to_str().unwrap();
    drop(fresh_dataset(uri).await);

    let registry = Arc::new(ObjectStoreRegistry::default());
    let cached_session = Arc::new(Session::new(
        16 * 1024 * 1024,
        16 * 1024 * 1024,
        Arc::clone(&registry),
    ));
    let control_session = Arc::new(Session::new(0, 0, Arc::clone(&registry)));

    let before = registry.stats();
    let cached = DatasetBuilder::from_uri(uri)
        .with_session(Arc::clone(&cached_session))
        .load()
        .await
        .expect("the cached data Session must open the dataset");
    let after_cached = registry.stats();
    assert!(
        after_cached.misses > before.misses,
        "the first Session open must create an object-store client"
    );
    let cached_metadata_items = cached_session
        .metadata_cache_keys()
        .await
        .expect("the default Lance cache backend must expose key inventory")
        .count();
    assert!(
        cached_metadata_items > 0,
        "a real data-Session open must populate its metadata cache"
    );
    assert_eq!(
        control_session
            .metadata_cache_keys()
            .await
            .expect("the zero-cache Session backend must expose key inventory")
            .count(),
        0,
        "the control Session must not observe the data Session's metadata entries"
    );

    // Keep `cached` alive so the registry's weak entry still has a live store
    // for the control Session to reuse.
    let control = DatasetBuilder::from_uri(uri)
        .with_session(Arc::clone(&control_session))
        .load()
        .await
        .expect("the zero-cache control Session must open the dataset");
    let after_control = registry.stats();
    assert!(
        after_control.hits > after_cached.hits,
        "the control Session must reuse the data Session's live object-store client"
    );
    assert_eq!(
        after_control.misses, after_cached.misses,
        "the second Session must not build a duplicate client for identical store parameters"
    );
    assert_eq!(
        control_session
            .metadata_cache_keys()
            .await
            .expect("the zero-cache Session backend must expose key inventory")
            .count(),
        0,
        "a control-plane open must leave the zero-sized metadata cache empty"
    );
    assert_eq!(cached.version().version, control.version().version);
}

// --- Guard 2b: RFC-024 public physical-ref incarnation surfaces -----------
//
// RFC-024 may activate durable table heads only if a public, backend-portable
// token rejects delete/recreate ABA at an unchanged URI/ref name and numeric
// version. Pin all three candidate components at compile time, then exercise
// them against real local and object-store datasets below.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_public_physical_ref_incarnation_surfaces() -> lance::Result<()> {
    let ds: &Dataset = unimplemented!();
    let _branch_identifier: BranchIdentifier = ds.branch_identifier().await?;
    let transaction: Option<Transaction> = ds.read_transaction().await?;
    if let Some(transaction) = transaction {
        let _transaction_uuid: String = transaction.uuid;
    }
    let location: &ManifestLocation = ds.manifest_location();
    let _manifest_e_tag: Option<String> = location.e_tag.clone();
    Ok(())
}

#[tokio::test]
async fn public_physical_ref_token_rejects_local_same_version_aba() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rfc024-physical-token.lance");
    let uri = path.to_str().unwrap();

    let first = fresh_dataset(uri).await;
    let first_version = first.version().version;
    let first_token = public_physical_ref_incarnation(&first).await;
    drop(first);

    std::fs::remove_dir_all(&path).expect("the first local dataset must be deleted completely");
    let mut second = fresh_dataset(uri).await;
    let second_version = second.version().version;
    let second_token = public_physical_ref_incarnation(&second).await;

    assert_eq!(
        first_version, second_version,
        "the ABA fixture must recreate main at the same numeric version"
    );
    assert_eq!(
        first_token.branch_identifier, second_token.branch_identifier,
        "main's BranchIdentifier is intentionally stable/empty and cannot detect replacement"
    );
    assert_eq!(
        first_token.branch_identifier,
        BranchIdentifier::main(),
        "main must expose Lance's canonical empty BranchIdentifier"
    );
    assert_ne!(
        first_token.transaction_uuid, second_token.transaction_uuid,
        "the public current-transaction UUID must distinguish local main replacement"
    );
    let first_etag = first_token
        .manifest_e_tag
        .as_deref()
        .expect("pinned Lance must expose the first local main manifest's metadata-derived e_tag");
    let second_etag = second_token.manifest_e_tag.as_deref().expect(
        "pinned Lance must expose the recreated local main manifest's metadata-derived e_tag",
    );
    assert_ne!(
        first_etag, second_etag,
        "pinned Lance's local e_tag must distinguish the recreated main manifest object"
    );
    assert_ne!(
        first_token, second_token,
        "the complete public physical-ref token must reject local main ABA"
    );

    recreate_named_branch_and_assert_token_changes(&mut second, true).await;
}

/// Exercise the production-shaped shared-Session case. Once the
/// canonical first incarnation is cached, deleting and recreating main at the
/// same URI/version must still resolve the second incarnation rather than reuse
/// the cached transaction UUID. A fresh public DatasetBuilder open is the
/// cache-bypass fallback and must agree with the shared-Session result.
#[tokio::test]
async fn local_physical_ref_token_is_stable_and_survives_shared_session_aba() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rfc024-shared-session-token.lance");
    let uri = path.to_str().unwrap();

    let first_committed = fresh_dataset(uri).await;
    let first_version = first_committed.version().version;
    let first_committed_token = public_physical_ref_incarnation(&first_committed).await;
    assert!(
        first_committed_token.manifest_e_tag.is_some(),
        "pinned Lance's public Dataset result must expose the local manifest's metadata-derived e_tag"
    );
    drop(first_committed);

    let shared_session = Arc::new(Session::default());
    let first = DatasetBuilder::from_uri(uri)
        .with_session(shared_session.clone())
        .load()
        .await
        .expect("the first local incarnation must reopen through the shared Session");
    let first_token = public_physical_ref_incarnation(&first).await;
    assert_eq!(
        first_committed_token, first_token,
        "the public token must be stable from the commit result through a shared-Session reopen"
    );

    let first_again = DatasetBuilder::from_uri(uri)
        .with_session(shared_session.clone())
        .load()
        .await
        .expect("the unchanged first incarnation must reopen from the shared Session");
    assert_eq!(
        first_token,
        public_physical_ref_incarnation(&first_again).await,
        "a canonical token must be stable across unchanged shared-Session reopens"
    );
    drop(first_again);
    drop(first);

    std::fs::remove_dir_all(&path).expect("the first local dataset must be deleted completely");
    let second_committed = fresh_dataset(uri).await;
    let second_version = second_committed.version().version;
    assert_eq!(
        first_version, second_version,
        "the shared-Session ABA fixture must recreate main at the same numeric version"
    );
    drop(second_committed);

    let second = DatasetBuilder::from_uri(uri)
        .with_session(shared_session)
        .load()
        .await
        .expect("the recreated local incarnation must reopen through the original Session");
    let second_token = public_physical_ref_incarnation(&second).await;
    assert_ne!(
        first_token.transaction_uuid, second_token.transaction_uuid,
        "the shared Session must not return the deleted incarnation's cached transaction"
    );
    let first_etag = first_token
        .manifest_e_tag
        .as_deref()
        .expect("pinned Lance must retain the first local manifest's metadata-derived e_tag");
    let second_etag = second_token
        .manifest_e_tag
        .as_deref()
        .expect("pinned Lance must expose the recreated local manifest's metadata-derived e_tag");
    assert_ne!(
        first_etag, second_etag,
        "the shared Session must resolve the recreated local manifest's distinct e_tag"
    );
    assert_ne!(
        first_token, second_token,
        "the canonical public token must distinguish shared-Session local main ABA"
    );

    let fresh = DatasetBuilder::from_uri(uri)
        .load()
        .await
        .expect("a fresh public Session must reopen the recreated incarnation");
    assert_eq!(
        second_token,
        public_physical_ref_incarnation(&fresh).await,
        "fresh-session cache bypass must agree with the canonical shared-Session token"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn public_physical_ref_token_rejects_s3_same_version_aba() {
    let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
        eprintln!(
            "skipping RFC-024 S3 physical-ref token guard: \
             OMNIGRAPH_S3_TEST_BUCKET is not set"
        );
        return;
    };
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let uri = format!(
        "s3://{bucket}/{prefix}/rfc024-physical-token/{}-{unique}.lance",
        std::process::id()
    );

    let first = fresh_dataset(&uri).await;
    let first_version = first.version().version;
    let first_token = public_physical_ref_incarnation(&first).await;
    let first_again = DatasetBuilder::from_uri(&uri)
        .load()
        .await
        .expect("the unchanged first S3/RustFS incarnation must reopen");
    assert_eq!(
        first_token,
        public_physical_ref_incarnation(&first_again).await,
        "the S3/RustFS token must be stable across an unchanged reopen"
    );
    drop(first_again);
    let (store, path) = lance_io::object_store::ObjectStore::from_uri(&uri)
        .await
        .expect("configured S3/RustFS dataset URI must resolve");
    drop(first);

    store
        .remove_dir_all(path.clone())
        .await
        .expect("the first S3/RustFS dataset must be deleted completely");
    let mut second = fresh_dataset(&uri).await;
    let second_version = second.version().version;
    let second_token = public_physical_ref_incarnation(&second).await;

    assert_eq!(
        first_version, second_version,
        "the ABA fixture must recreate S3/RustFS main at the same numeric version"
    );
    assert_eq!(
        first_token.branch_identifier, second_token.branch_identifier,
        "main's BranchIdentifier is intentionally stable/empty and cannot detect replacement"
    );
    assert_ne!(
        first_token.transaction_uuid, second_token.transaction_uuid,
        "the public current-transaction UUID must distinguish S3/RustFS main replacement"
    );
    let first_etag = first_token
        .manifest_e_tag
        .as_deref()
        .expect("S3/RustFS must expose the first main manifest e_tag");
    let second_etag = second_token
        .manifest_e_tag
        .as_deref()
        .expect("S3/RustFS must expose the recreated main manifest e_tag");
    assert_ne!(
        first_etag, second_etag,
        "S3/RustFS must distinguish the recreated main manifest object by e_tag"
    );
    assert_ne!(
        first_token, second_token,
        "the complete public physical-ref token must reject S3/RustFS main ABA"
    );

    recreate_named_branch_and_assert_token_changes(&mut second, true).await;

    drop(second);
    store
        .remove_dir_all(path)
        .await
        .expect("configured S3/RustFS test prefix cleanup must succeed");
}

// --- Guard 3: checkout_version + restore async chain -----------------------
//
// `db/manifest/recovery.rs:505-522` chains `Dataset::open(...).await?
// .checkout_version(N).await?.restore().await?` as the recovery rollback
// hammer. Compile-only — never runs.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_checkout_version_then_restore_signature() -> lance::Result<()> {
    let ds: Dataset = unimplemented!();
    let mut ds: Dataset = ds.checkout_version(1u64).await?;
    // `restore()` takes `&mut self` and returns `Result<()>`; the dataset
    // mutates in place. If Lance flips this to return a fresh `Dataset`
    // (consuming `self`), this guard fails to compile.
    let _: () = ds.restore().await?;
    Ok(())
}

// --- Guard 4: DatasetBuilder::from_namespace fluent chain ------------------
//
// `db/manifest/namespace.rs:162-174` chains
// `DatasetBuilder::from_namespace(ns, vec![id]).await?.with_branch(...).with_version(...).load().await?`.
// Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_dataset_builder_from_namespace_signature(
    ns: Arc<dyn LanceNamespace>,
) -> lance::Result<()> {
    let builder: DatasetBuilder =
        DatasetBuilder::from_namespace(ns, vec!["table".to_string()]).await?;
    let builder: DatasetBuilder = builder.with_branch("b", None);
    let builder: DatasetBuilder = builder.with_version(1u64);
    let _ds: Dataset = builder.load().await?;
    Ok(())
}

// --- Guard 5: MergeInsertBuilder fluent chain ------------------------------
//
// `db/manifest/publisher.rs:370-391` is the manifest CAS. If any method on
// the builder renames or changes signature, the publisher silently breaks.
// Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_merge_insert_builder_method_chain() -> lance::Result<()> {
    use lance::dataset::MergeStats;

    let ds: Arc<Dataset> = unimplemented!();
    let job = MergeInsertBuilder::try_new(ds, vec!["object_id".to_string()])?
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0)
        .use_index(false)
        .try_build()?;

    // execute_reader takes `impl StreamingWriteSource` (lance trait), which
    // RecordBatchIterator implements. Pin the return shape
    // `(Arc<Dataset>, MergeStats)` — the publisher's CAS loop depends on
    // both: the new Dataset to advance HEAD, the stats for the audit row.
    let source: RecordBatchIterator<Vec<Result<RecordBatch, arrow_schema::ArrowError>>> =
        unimplemented!();
    let result: (Arc<Dataset>, MergeStats) = job.execute_reader(source).await?;
    let _ds: Arc<Dataset> = result.0;
    let _stats: MergeStats = result.1;
    Ok(())
}

// --- Guard 6: WriteParams::default() leaves data_storage_version = None ----
//
// Our V2_2 pin is load-bearing for blob v2 (verified earlier this session
// when V2_1 produced "Blob v2 requires file version >= 2.2" on 13 blob
// tests). If Lance changes the default to pin some version itself, audit
// every `data_storage_version: Some(LanceFileVersion::V2_2)` site.

#[test]
fn write_params_default_does_not_set_storage_version() {
    let params = WriteParams::default();
    assert_eq!(
        params.data_storage_version, None,
        "WriteParams::default().data_storage_version is no longer None; \
         audit every explicit V2_2 pin (see rg 'LanceFileVersion::V2_2')."
    );
}

// --- Guard 7: compact_files signature --------------------------------------
//
// `db/omnigraph/optimize.rs:107` calls `compact_files(&mut ds, options, None)`.
// Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_compact_files_signature() -> lance::Result<()> {
    let mut ds: Dataset = unimplemented!();
    let options: CompactionOptions = CompactionOptions::default();
    let _metrics = compact_files(&mut ds, options, None).await?;
    Ok(())
}

// --- Guard 7b: transaction history exposes repair's classification surface -
//
// `db/omnigraph/repair.rs` reads Lance transactions between manifest and HEAD
// and treats only `ReserveFragments` + `Rewrite` as safe maintenance drift.
// Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_transaction_history_for_repair_signature() -> lance::Result<()> {
    let ds: Dataset = unimplemented!();
    let tx = ds.read_transaction_by_version(1u64).await?;
    if let Some(tx) = tx {
        let operation = tx.operation;
        let _name: &str = operation.name();
        match operation {
            Operation::Rewrite { .. } | Operation::ReserveFragments { .. } => {}
            _ => {}
        }
    }
    Ok(())
}

// --- Guard 8: DeleteBuilder::execute_uncommitted returns
//     UncommittedDelete { transaction, affected_rows, num_deleted_rows } ---
//
// `table_store.rs::stage_delete` uses the two-phase delete (lance#6658, Lance
// 7.0): it reads `num_deleted_rows` (0 ⇒ no-op `None`) and stages `transaction`
// WITHOUT committing, instead of the inline `Dataset::delete`. It must also
// preserve `affected_rows` and pass it to `CommitBuilder::with_affected_rows`
// through `StagedWrite`; dropping it disables Lance's row-level rebase metadata.
// Compile-only.
#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_uncommitted_delete_field_shape() -> lance::Result<()> {
    use lance::dataset::DeleteBuilder;
    use lance_select::mask::RowAddrTreeMap;
    let ds: Arc<Dataset> = unimplemented!();
    let staged = DeleteBuilder::new(ds, "x = 1")
        .execute_uncommitted()
        .await?;
    let _txn: lance::dataset::transaction::Transaction = staged.transaction;
    let _num_deleted: u64 = staged.num_deleted_rows;
    let _affected: Option<RowAddrTreeMap> = staged.affected_rows;
    Ok(())
}

// --- Guard 8a: full-table vector indexing exposes uncommitted metadata -----
//
// EnsureIndices batches BTREE, FTS, and the current one-segment full-table
// vector shape into one exact `Operation::CreateIndex`. This requires the
// pinned Lance builder to return complete public `IndexMetadata` without committing
// HEAD. Compile-only: a Lance bump that removes or narrows the surface must
// turn the compatibility smoke test red.
#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_uncommitted_full_table_vector_index_shape() -> lance::Result<()> {
    use lance::index::vector::VectorIndexParams;
    use lance_linalg::distance::MetricType;
    use lance_table::format::IndexMetadata;

    let mut ds: Dataset = unimplemented!();
    let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
    let metadata: IndexMetadata = ds
        .create_index_builder(&["embedding"], IndexType::Vector, &params)
        .replace(true)
        .execute_uncommitted()
        .await?;
    let _transaction_shape = Operation::CreateIndex {
        new_indices: vec![metadata],
        removed_indices: Vec::new(),
    };
    Ok(())
}

// --- Guard 8b: MergeInsertJob::execute_uncommitted returns
//     UncommittedMergeInsert { transaction, affected_rows, stats, inserted_rows_filter } ---
//
// `TableStore::stage_merge_insert` has the same staged commit contract as
// delete: the Lance transaction and `affected_rows` metadata must travel
// together into `commit_staged`.
#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_uncommitted_merge_insert_field_shape() -> lance::Result<()> {
    use lance_select::mask::RowAddrTreeMap;
    let ds: Arc<Dataset> = unimplemented!();
    let source: Box<dyn arrow_array::RecordBatchReader + Send> = unimplemented!();
    let job = MergeInsertBuilder::try_new(ds, vec!["x".to_string()])?.try_build()?;
    let staged = job.execute_uncommitted(source).await?;
    let _txn: lance::dataset::transaction::Transaction = staged.transaction;
    let _affected: Option<RowAddrTreeMap> = staged.affected_rows;
    let _stats = staged.stats;
    let _inserted_rows_filter = staged.inserted_rows_filter;
    Ok(())
}

// --- Guard 9: force_delete_branch semantics --------------------------------
//
// The branch-delete reconciler (`db/omnigraph/optimize.rs::reconcile_orphaned_branches`)
// and the eager best-effort reclaim in `cleanup_deleted_branch_tables` call
// `force_delete_branch` to drop orphaned branch refs. The single-authority
// design relies on five facts pinned here:
//   1. plain `delete_branch` errors on a missing ref (so the design uses the
//      force variant instead);
//   2. `force_delete_branch` removes an existing (forked) branch — the orphan
//      case, where a `tree/{branch}/` exists;
//   3. `force_delete_branch` on a *fully-absent* branch (no tree dir) is
//      idempotent. Beta.18 maps object-store absence to Lance `NotFound`, and
//      branch cleanup now treats that as success. Pin the positive contract;
//   4. a clone-only zombie (branch dataset present, BranchContents absent)
//      blocks raw create and is reclaimed by `force_delete_branch`. Lance's
//      create is explicitly two-phase, so this is the crash state OmniGraph's
//      native branch-control wrapper must heal before retrying;
//   5. a live slash-name path-child makes force delete remove an ancestor's
//      BranchContents but intentionally retain its dataset files. OmniGraph's
//      prefix-disjoint live-name invariant prevents this false-success shape.

#[tokio::test]
async fn force_delete_branch_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard9.lance");
    let uri = uri.to_str().unwrap();
    let mut ds = fresh_dataset(uri).await;

    // (1) Plain delete of a never-created branch errors (RefNotFound).
    assert!(
        ds.delete_branch("nope").await.is_err(),
        "Dataset::delete_branch on a missing ref should error; if this is now \
         Ok, the reconciler could drop the force variant."
    );

    // (2) force_delete_branch removes an existing (forked) branch.
    let base = ds.version().version;
    ds.create_branch("feature", base, None).await.unwrap();
    ds.force_delete_branch("feature").await.unwrap();
    assert!(
        !ds.list_branches().await.unwrap().contains_key("feature"),
        "force_delete_branch should remove an existing branch ref"
    );

    // (3) Force delete is idempotent even when both the ref and tree are absent.
    ds.force_delete_branch("never").await.unwrap();

    // (4) Exact phase-1-only create state: create the shallow-cloned branch
    // dataset, then remove only its authoritative BranchContents ref. This is
    // the same fixture Lance's own dataset-versioning test uses for a zombie.
    ds.create_branch("zombie", base, None).await.unwrap();
    std::fs::remove_file(
        std::path::Path::new(uri)
            .join("_refs")
            .join("branches")
            .join("zombie.json"),
    )
    .unwrap();
    assert!(
        !ds.list_branches().await.unwrap().contains_key("zombie"),
        "BranchContents is the authority; the clone-only tree must not list as a branch"
    );
    assert!(
        ds.create_branch("zombie", base, None).await.is_err(),
        "the clone-only tree should block an unclassified raw create"
    );
    ds.force_delete_branch("zombie").await.unwrap();
    assert!(
        !std::path::Path::new(uri)
            .join("tree")
            .join("zombie")
            .exists(),
        "force_delete_branch must reclaim the clone-only tree"
    );

    // (5) Slash-separated names overlap physically. A path-child created from
    // main is not a lineage descendant of its lexical ancestor, so raw force
    // delete removes the ancestor ref but deliberately leaves its dataset
    // files to avoid recursively deleting the child.
    ds.create_branch("ancestor/child", base, None)
        .await
        .unwrap();
    ds.create_branch("ancestor", base, None).await.unwrap();
    ds.force_delete_branch("ancestor").await.unwrap();
    assert!(
        !ds.list_branches().await.unwrap().contains_key("ancestor"),
        "raw force delete still removes authoritative ancestor metadata"
    );
    assert!(
        std::path::Path::new(uri)
            .join("tree")
            .join("ancestor")
            .join("_versions")
            .exists(),
        "Lance must retain ancestor dataset files while a physical path-child is live"
    );
}

// --- Guard 10: blob-column compaction works in this Lance ------------------
//
// Historical: through Lance 7.0.0, `compact_files` forced
// `BlobHandling::AllBinary` and the blob-v2 struct decoder mis-counted columns,
// failing even a pristine uniform-V2_2 multi-fragment blob table; `optimize`
// skipped blob-bearing tables behind `LANCE_SUPPORTS_BLOB_COMPACTION = false`.
// Lance 8.0.0 shipped full blob-v2 compaction (upstream PR #7017; hardened by
// #7618 in 9.0.0-beta.15 after a beta.13 regression), so the gate, the skip
// branch, and the `BlobColumnsUnsupportedByLance` skip reason were removed at
// the 9.0.0-beta.15 bump. This guard pins the POSITIVE behavior `optimize` now
// relies on: a multi-fragment blob table compacts, preserving every row. If it
// turns red on a future bump, blob compaction regressed — restore the skip
// machinery from git history.

#[tokio::test]
async fn compact_files_succeeds_on_blob_columns() {
    use arrow_array::{LargeBinaryArray, StructArray};

    fn blob_batch(start: i32, n: i32) -> RecordBatch {
        let ids: Vec<String> = (start..start + n).map(|i| format!("n{i}")).collect();
        let data =
            LargeBinaryArray::from_iter_values((start..start + n).map(|i| format!("blob{i}")));
        let blob_uri = StringArray::from(vec![None::<&str>; n as usize]);
        let DataType::Struct(fields) = lance::blob::blob_field("content", true).data_type().clone()
        else {
            unreachable!("blob_field is always a Struct");
        };
        let content = StructArray::new(
            fields,
            vec![Arc::new(data) as _, Arc::new(blob_uri) as _],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            lance::blob::blob_field("content", true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(content) as _,
            ],
        )
        .unwrap()
    }

    async fn write(uri: &str, batch: RecordBatch, mode: WriteMode) {
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        // Blob v2 requires file version >= 2.2; without the pin the *write*
        // would fail with a different error, masking the guard's intent.
        let params = WriteParams {
            mode,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        Dataset::write(reader, uri, Some(params)).await.unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard10-blob.lance");
    let uri = uri.to_str().unwrap();

    // Uniform V2_2, two fragments → forces compaction to actually rewrite.
    write(uri, blob_batch(0, 2), WriteMode::Create).await;
    write(uri, blob_batch(100, 2), WriteMode::Append).await;

    let mut ds = Dataset::open(uri).await.unwrap();
    assert!(
        ds.get_fragments().len() >= 2,
        "guard needs a multi-fragment table to trigger a real compaction rewrite"
    );

    let rows_before = ds.count_rows(None).await.unwrap();
    let metrics = compact_files(&mut ds, CompactionOptions::default(), None)
        .await
        .expect(
            "compact_files FAILED on a blob table — the Lance blob-v2 compaction \
             fix (present since 8.0.0, hardened by lance#7618) regressed. If this \
             is a Lance downgrade, restore the pre-9 blob-skip branch in \
             db/omnigraph/optimize.rs (see git history + docs/dev/lance.md).",
        );
    assert!(
        metrics.fragments_removed >= 2 && metrics.fragments_added >= 1,
        "expected a real rewrite of the multi-fragment blob table, got {metrics:?}"
    );
    let ds = Dataset::open(uri).await.unwrap();
    assert_eq!(
        ds.count_rows(None).await.unwrap(),
        rows_before,
        "compaction must preserve every blob row"
    );
}

// --- Guard 11: scalar-index coverage surface (physical_rows + index details) ---
//
// `table_store.rs::key_column_index_coverage` mirrors Lance's `create_filter_plan`
// C6 fallback: it reads `fragment.physical_rows` (the field whose absence on ANY
// fragment disables the scalar index for the whole scan) and sniffs the BTREE via
// `load_indices()` → `index.fields` / `index.index_details.type_url`. This is the
// one real Lance-internal coupling on the indexed-traversal read path. If any of
// these surfaces renames or changes type, the coverage check (and the cost-based
// traversal chooser that consumes it) silently misclassifies. Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_scalar_index_coverage_surface() -> lance::Result<()> {
    let ds: Dataset = unimplemented!();
    // The create_filter_plan coupling: a fragment lacking `physical_rows`
    // disables the scalar index for the entire scan.
    for frag in ds.fragments().iter() {
        let _physical_rows: Option<usize> = frag.physical_rows;
        // `key_column_index_coverage` checks each current fragment id against the
        // index `fragment_bitmap`.
        let _id: u64 = frag.id;
    }
    // The index sniff: BTREE presence is detected by single-field index whose
    // details type_url ends with "BTreeIndexDetails". The fragment coverage check
    // reads `fragment_bitmap` (Option<RoaringBitmap>) and calls `.contains(u32)`.
    let indices = ds.load_indices().await?;
    for index in indices.iter() {
        let _fields: &Vec<i32> = &index.fields;
        if let Some(details) = index.index_details.as_ref() {
            let _type_url: &str = details.type_url.as_str();
        }
        let _covered: Option<bool> = index.fragment_bitmap.as_ref().map(|b| b.contains(0u32));
    }
    Ok(())
}

// --- Guard 12: can a scalar BTREE be built on a system version column? --------
//
// The deferred persisted-adjacency artifact plan assumed a cheap delta read of
// `_row_last_updated_at_version > V` could be a BTREE range lookup. Lance resolves
// index columns from the dataset schema, and the version columns are system
// metadata — so this probe documents whether the assumption holds. The outcome is
// the load-bearing fact, not a pass/fail of intent: if this starts SUCCEEDING when
// it currently errors (or vice versa), the artifact's delta-cost story changes.

#[tokio::test]
async fn scalar_index_on_system_version_column_probe() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard12.lance");
    let mut ds = fresh_dataset(uri.to_str().unwrap()).await;

    // Sanity: the system version column is present (stable row ids + V2_2).
    assert!(
        ds.schema().field("_row_last_updated_at_version").is_none(),
        "PROBE NOTE: `_row_last_updated_at_version` is NOT in the user schema \
         (it is system metadata); indexing it resolves through a different path."
    );

    let result = ds
        .create_index_builder(
            &["_row_last_updated_at_version"],
            IndexType::BTree,
            &ScalarIndexParams::default(),
        )
        .replace(true)
        .await;

    // Pin the observed behavior: a scalar index on the system version column is
    // NOT buildable via the normal create-index path in this Lance. If this turns
    // green (Ok), the artifact delta CAN use a version-column BTREE — revisit the
    // deferred plan's Phase-2 delta-cost note in docs/dev/traversal handoff.
    assert!(
        result.is_err(),
        "create_index on `_row_last_updated_at_version` unexpectedly SUCCEEDED — \
         a system-column scalar index is now buildable; the persisted-artifact \
         delta read could use it. Update the deferred-design notes."
    );
}

// --- Guard 13: per-fragment deletion metadata is exposed without a scan -------
//
// The deferred artifact's delete-correctness coverage model needs to detect,
// cheaply (O(fragments), no row scan), that a covered fragment acquired new
// deletions. That hinges on Lance tracking deletions at fragment-metadata level.
// This pins that a delete populates `fragment.deletion_file`, and probes whether
// the deleted-row COUNT is available as metadata (`num_deleted_rows`) — the
// difference between an O(fragments) coverage check and an O(|E|) scan.

#[tokio::test]
async fn fragment_deletion_metadata_is_available() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard13.lance");
    let ds = fresh_dataset(uri.to_str().unwrap()).await; // 2 rows: alice, bob

    let deleted: DeleteResult = {
        let mut ds = ds;
        ds.delete("id = 'alice'").await.unwrap()
    };
    assert_eq!(deleted.num_deleted_rows, 1, "one row deleted");
    let ds = deleted.new_dataset;

    // A delete must be tracked at fragment-metadata level (not only in data).
    let with_deletion = ds
        .fragments()
        .iter()
        .find(|f| f.deletion_file.is_some())
        .expect(
            "after a delete, some fragment must carry a deletion_file — if not, \
             Lance changed deletion tracking; the artifact coverage model's \
             cheap delete-detection assumption is invalid.",
        );

    // Probe: is the deleted-row count available as metadata (cheap), or must the
    // deletion vector be read? Pin whichever holds so the artifact plan knows.
    let count: Option<usize> = with_deletion
        .deletion_file
        .as_ref()
        .and_then(|df| df.num_deleted_rows);
    assert_eq!(
        count,
        Some(1),
        "PROBE: deletion_file.num_deleted_rows is not a populated metadata count \
         (got {count:?}); the artifact coverage model cannot cheaply detect \
         per-fragment deletions and would need to read the deletion vector.",
    );
}

// --- Guard 14: Dataset::optimize_indices signature ----------------------------
//
// `db/omnigraph/optimize.rs::optimize_one_table` calls
// `ds.optimize_indices(&OptimizeOptions::default())` (via `DatasetIndexExt`) to
// fold appended/compacted fragments back into existing indexes. If Lance
// changes the receiver, the options type, or the return shape, this fails to
// compile. Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_optimize_indices_signature() -> lance::Result<()> {
    let mut ds: Dataset = unimplemented!();
    let options = OptimizeOptions::default();
    // `&mut self`, `&OptimizeOptions`, returns `Result<()>` (mutates in place
    // and commits — there is no uncommitted variant in this Lance, which is why
    // optimize treats it as an inline-commit residual under a recovery sidecar).
    let _: () = ds.optimize_indices(&options).await?;
    Ok(())
}

// --- Guard 15: optimize_indices extends fragment coverage ----------------------
//
// PR3's reindex assumes `optimize_indices` folds fragments appended AFTER an
// index was built into that index (incremental merge, not retrain). This pins
// that Lance behavior at the surface layer so a regression turns red here, the
// first smoke check on a Lance bump, before the slower engine suite.

#[tokio::test]
async fn optimize_indices_extends_fragment_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard_optimize_indices.lance");
    let uri = uri.to_str().unwrap();

    // Fragment 0: alice, bob. Build a BTREE over `value` covering only it.
    let mut ds = fresh_dataset(uri).await;
    ds.create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
        .replace(true)
        .await
        .unwrap();

    // Append a second fragment the existing index does not cover.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["carol"])),
            Arc::new(Int32Array::from(vec![3])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Append,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap();

    let mut ds = Dataset::open(uri).await.unwrap();
    assert!(
        value_index_uncovered_count(&ds).await > 0,
        "appended fragment should be uncovered by the BTREE before optimize_indices"
    );

    ds.optimize_indices(&OptimizeOptions::default())
        .await
        .unwrap();

    assert_eq!(
        value_index_uncovered_count(&ds).await,
        0,
        "optimize_indices must fold the appended fragment into the existing index \
         (incremental coverage); if this regresses, PR3's reindex no longer keeps \
         coverage current — revisit db/omnigraph/optimize.rs and docs/dev/lance.md."
    );
}

/// Count current fragments not covered by the single-column `value` BTREE —
/// mirrors `TableStore::has_unindexed_fragments` (load_indices +
/// `fragment_bitmap.contains`), pinned by Guard 11.
async fn value_index_uncovered_count(ds: &Dataset) -> usize {
    let indices = ds.load_indices().await.unwrap();
    let frag_ids: Vec<u32> = ds.fragments().iter().map(|f| f.id as u32).collect();
    let value_fid = ds.schema().field("value").unwrap().id;
    for index in indices.iter() {
        if index.fields.len() == 1 && index.fields[0] == value_fid {
            if let Some(bitmap) = index.fragment_bitmap.as_ref() {
                return frag_ids.iter().filter(|id| !bitmap.contains(**id)).count();
            }
        }
    }
    // No `value` index found — treat as fully uncovered so a missing index
    // is never mistaken for full coverage.
    frag_ids.len()
}

// --- Guard 16: scalar index use requires a literal matching the column type ---
//
// Pins the substrate behavior the pushdown literal-coercion fix relies on
// (`query.rs::literal_to_typed_expr`): Lance uses the BTREE only when the filter
// is `column OP literal` with a matching type. A width-mismatched literal makes
// DataFusion widen and cast the COLUMN (`CAST(n32 AS Int64)`), which drops the
// scalar index and full-scans. Temporal columns are immune (DataFusion casts the
// Utf8 LITERAL to the date type, not the column). If a Lance/DataFusion bump
// changes either coercion direction, this turns red — re-validate the fix.
#[tokio::test]
async fn scalar_index_use_requires_matched_literal_type() {
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{col, lit};
    use datafusion::scalar::ScalarValue;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("probe_literal_type.lance");
    let uri = uri.to_str().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("n32", DataType::Int32, false),
        Field::new("d32", DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 5, 9, 13])),
            Arc::new(arrow_array::Date32Array::from(vec![
                19000, 19723, 20000, 20500,
            ])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let mut ds = Dataset::write(reader, uri, Some(params)).await.unwrap();
    for c in ["n32", "d32"] {
        ds.create_index_builder(&[c], IndexType::BTree, &ScalarIndexParams::default())
            .replace(true)
            .await
            .unwrap();
    }

    async fn plan_str(ds: &Dataset, filter: datafusion::prelude::Expr) -> String {
        let mut scanner = ds.scan();
        scanner.filter_expr(filter);
        let plan = scanner.create_plan().await.unwrap();
        format!("{}", displayable(plan.as_ref()).indent(true))
    }

    // (label, filter, expect_index_used)
    let cases = [
        ("n32 = 5i32 (matched Int32)", col("n32").eq(lit(5i32)), true),
        (
            "n32 = 5i64 (widened Int64)",
            col("n32").eq(lit(5i64)),
            // 7.0.0: a width-mismatched literal blocked index pushdown (this
            // pinned `false`). The v8 coercion fixes (lance#6935 et al.) now
            // coerce Int64(5) -> Int32(5) BEFORE pushdown, so the BTREE is
            // used. query.rs::literal_to_typed_expr remains load-bearing for
            // producing exactly-typed literals; Lance now also rescues the
            // widened case.
            true,
        ),
        (
            "d32 = Date32 (matched)",
            col("d32").eq(lit(ScalarValue::Date32(Some(19723)))),
            true,
        ),
        (
            "d32 = '2024-01-01' (Utf8 vs Date32)",
            col("d32").eq(lit("2024-01-01")),
            true,
        ),
    ];

    for (label, filter, expect_index) in cases {
        let s = plan_str(&ds, filter).await;
        let uses_index = s.contains("ScalarIndexQuery");
        assert_eq!(
            uses_index, expect_index,
            "[{label}] expected scalar-index use = {expect_index}, got {uses_index}.\n\
             A change here means Lance/DataFusion shifted its coercion or index \
             pushdown; re-validate query.rs::literal_to_typed_expr.\nplan:\n{s}"
        );
    }

    // 7.0.0 planned the widened case as an index-defeating column-side CAST
    // (`CAST(n32 AS Int64) = 5`); since the v8 coercion fixes the literal is
    // coerced to the column type instead — pin the new mechanism: no column
    // cast, literal narrowed to Int32.
    let widened = plan_str(&ds, col("n32").eq(lit(5i64))).await;
    assert!(
        !widened.contains("CAST(n32") && widened.contains("Int32(5)"),
        "expected a literal coerced to Int32 with no column-side cast, got:\n{widened}"
    );
}

// --- Guard 17: BTREE scalar-index range-boundary correctness (lance#6796) -----
//
// lance#6796 (issue #6792) fixed a BTREE range-query bound-inclusiveness bug:
// `price <= 10 AND price > 5` returned the wrong boundary row (5.0 instead of
// 10.0). OmniGraph today builds BTREE only on string `@key` columns and queries
// them by equality/IN, not range, so its current patterns do not hit this — the
// guard protects any future BTREE-range path. It reproduces the exact #6792 shape
// (5 rows + an explicit BTREE drives the index path even on tiny data, per the
// upstream repro) and pins the corrected inclusive-`<=` / exclusive-`>` semantics.
#[tokio::test]
async fn btree_range_query_boundary_is_correct() {
    use arrow_array::Float64Array;
    use futures::TryStreamExt;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard17.lance");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(Float64Array::from(vec![1.0, 5.0, 10.0, 15.0, 20.0])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let mut ds = Dataset::write(reader, uri.to_str().unwrap(), Some(params))
        .await
        .unwrap();

    // Build the BTREE on the numeric column so the range filter resolves through
    // the scalar index (the path lance#6796 fixed).
    ds.create_index_builder(&["price"], IndexType::BTree, &ScalarIndexParams::default())
        .replace(true)
        .await
        .unwrap();

    let mut scanner = ds.scan();
    scanner.filter("price <= 10.0 AND price > 5.0").unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut got: Vec<f64> = Vec::new();
    for b in &batches {
        let col = b
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..col.len() {
            got.push(col.value(i));
        }
    }
    got.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        got,
        vec![10.0],
        "BTREE range `price <= 10 AND price > 5` must return exactly [10.0] \
         (lance#6796 / issue #6792 boundary fix); got {got:?}. If this regressed, \
         Lance reintroduced the range-bound inclusiveness bug.",
    );
}

// --- Guard 18: skip_auto_cleanup suppresses version GC (lance#6755 / PR #229) --
//
// After the v7 bump, OmniGraph relies on `CommitBuilder::with_skip_auto_cleanup`
// (`commit_staged`) and `MergeInsertBuilder::skip_auto_cleanup` (the `__manifest`
// publisher) to stop Lance's per-commit auto-cleanup hook from GC'ing versions
// the `__manifest` pins for snapshots/time-travel. This is load-bearing for
// graphs created BEFORE the bump: 6.0.1 defaulted `WriteParams::auto_cleanup` ON,
// so those datasets carry `lance.auto_cleanup.*` config that `auto_cleanup = None`
// on new writes cannot retroactively clear — only the per-commit skip stops it.
//
// Pins both halves: WITHOUT the skip the aggressive config GCs v1; WITH the skip
// (the exact call `commit_staged` makes) v1 survives.
#[tokio::test]
async fn skip_auto_cleanup_suppresses_version_gc() {
    use std::collections::HashMap;

    // The cleanup config 6.0.1 stored by default, made aggressive: fire on every
    // commit, delete anything older than now.
    async fn set_legacy_cleanup(ds: &mut Dataset) {
        let mut cfg = HashMap::new();
        cfg.insert("lance.auto_cleanup.interval".to_string(), "1".to_string());
        cfg.insert(
            "lance.auto_cleanup.older_than".to_string(),
            "0ms".to_string(),
        );
        ds.update_config(cfg).await.unwrap();
    }
    fn row(i: i32) -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![format!("k{i}")])),
                Arc::new(Int32Array::from(vec![i])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    // Negative control: WITHOUT skip, the legacy config GCs the pinned v1.
    let ctrl = tempfile::tempdir().unwrap();
    let curi = ctrl.path().join("g18_ctrl.lance");
    let curi = curi.to_str().unwrap();
    let mut ds = fresh_dataset(curi).await;
    let v1 = ds.version().version;
    set_legacy_cleanup(&mut ds).await;
    for i in 0..5 {
        let (schema, batch) = row(i);
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        ds.append(
            reader,
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    }
    assert!(
        ds.checkout_version(v1).await.is_err(),
        "negative control: without skip_auto_cleanup, the legacy auto_cleanup \
         config should have GC'd pinned v{v1}; if this fails the config is not \
         firing and the positive assertion below proves nothing."
    );

    // The guarantee: WITH the per-commit skip, v1 survives. Mirrors
    // `TableStore::commit_staged` (InsertBuilder::execute_uncommitted +
    // CommitBuilder::with_skip_auto_cleanup(true)).
    let keep = tempfile::tempdir().unwrap();
    let kuri = keep.path().join("g18.lance");
    let kuri = kuri.to_str().unwrap();
    let mut ds = fresh_dataset(kuri).await;
    let v1 = ds.version().version;
    set_legacy_cleanup(&mut ds).await;
    for i in 0..5 {
        let (_schema, batch) = row(i);
        let tx = InsertBuilder::new(Arc::new(ds.clone()))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute_uncommitted(vec![batch])
            .await
            .unwrap();
        ds = CommitBuilder::new(Arc::new(ds.clone()))
            .with_skip_auto_cleanup(true)
            .execute(tx)
            .await
            .unwrap();
    }
    assert!(
        ds.checkout_version(v1).await.is_ok(),
        "v{v1} was GC'd despite CommitBuilder::with_skip_auto_cleanup(true) — the \
         commit_staged / publisher skip is the only thing protecting \
         __manifest-pinned versions on upgraded (pre-bump) graphs."
    );
}

// --- Guard 19: unenforced primary key is immutable once set (lance v7) ------
//
// Lance 7 (`lance::dataset::transaction`) makes the unenforced PK reserved:
// once `lance-schema:unenforced-primary-key` is set on a field, any later write
// that touches that reserved key — even re-applying the SAME value — errors
// "the unenforced primary key is a reserved key and cannot be changed once set".
//
// This is the upstream behavior that broke
// `db/manifest/migrations.rs::migrate_v1_to_v2`'s crash-idempotency: a
// pre-v0.4.0 graph that crashed after the field-set but before the stamp bump
// re-enters the migration with the PK already present, and on Lance 6 the
// re-apply was a no-op. The migration now guards the set on the manifest's
// unenforced-PK field (`["object_id"]` → no-op, `[]` → set, anything else →
// loud refusal). If Lance ever relaxes immutability (a re-set becomes a no-op
// again), this guard goes red — revisit whether that field-guard is still
// needed, and re-pin docs/dev/lance.md.
#[tokio::test]
async fn unenforced_primary_key_is_immutable_once_set() {
    use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("g19.lance");
    let mut ds = fresh_dataset(uri.to_str().unwrap()).await;

    // Precondition: no unenforced PK yet (mirrors a genuine pre-v0.4.0 manifest).
    assert!(
        ds.schema().unenforced_primary_key().is_empty(),
        "fresh dataset should carry no unenforced primary key"
    );

    // First set succeeds — the genuine pre-v0.4.0 migration path. (Discard the
    // returned &Schema so the &mut borrow ends before the next call.)
    ds.update_field_metadata()
        .update(
            "id",
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())],
        )
        .unwrap()
        .await
        .unwrap();
    let pk: Vec<String> = ds
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    assert_eq!(
        pk,
        ["id"],
        "first set should install `id` as the unenforced PK"
    );

    // Re-applying the SAME reserved key must still error. Normalize the sync
    // validation stage (`.update()`) and the async commit stage (`.await`) into
    // one Result so the actionable diagnostic below fires whichever stage Lance
    // enforces immutability at — and even if a future Lance relaxes it to `Ok`.
    // Bare `.unwrap()` / `.unwrap_err()` would instead panic with a generic
    // message in those cases, defeating the guard's purpose.
    let outcome: lance::Result<()> = match ds.update_field_metadata().update(
        "id",
        [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())],
    ) {
        Ok(builder) => builder.await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(
        matches!(&outcome, Err(e) if e.to_string().contains("cannot be changed once set")),
        "Lance no longer rejects re-setting the unenforced PK as immutable \
         (got: {outcome:?}); immutability relaxed or moved off the commit path \
         — revisit migrate_v1_to_v2's field-guard and re-pin docs/dev/lance.md."
    );
}

// --- Guard 19b: pinned Lance merge_insert PK-filter shape ------------------
//
// RFC-023 can rely on Lance's key-conflict fencing only when every keyed
// insert produces an `Operation::Update.inserted_rows_filter`. On the pinned
// Lance revision that is a route-dependent contract: the v2 plan emits a filter when the ordered
// ON field ids exactly match the unenforced PK, while the scalar-index v1 path
// and non-PK ON shapes emit `None`. A matched-only v2 update still emits
// `Some(empty Bloom)`, which is important because `Some` selects the strict
// conflict-resolver branch even though this attempt inserted no key.
#[tokio::test]
async fn unenforced_pk_filter_shape_is_route_dependent() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("rfc023_filter_v2.lance");
    let dataset = Arc::new(fresh_pk_dataset(uri.to_str().unwrap()).await);
    let id_field_id = dataset.schema().field("id").unwrap().id;

    // Exact PK ON + forced non-index plan: a real insert produces a populated
    // Bloom filter over exactly the PK field id.
    let upsert = stage_pk_merge(
        dataset.clone(),
        pk_full_row(dataset.as_ref(), "fresh-upsert", 10),
        "id",
        WhenMatched::UpdateAll,
        WhenNotMatched::InsertAll,
        Some(false),
    )
    .await;
    assert_eq!(upsert.stats.num_inserted_rows, 1);
    assert_eq!(upsert.stats.num_updated_rows, 0);
    let filter = staged_inserted_rows_filter(&upsert)
        .expect("exact PK v2 upsert must carry a key-existence filter");
    assert_eq!(filter.field_ids, vec![id_field_id]);
    assert_bloom_empty(filter, false, "exact-PK upsert");

    // Strict insert uses the same filter-bearing v2 route. The source
    // key must be fresh: `WhenMatched::Fail` correctly errors during staging
    // if the key already exists.
    let strict_create = stage_pk_merge(
        dataset.clone(),
        pk_full_row(dataset.as_ref(), "fresh-strict", 11),
        "id",
        WhenMatched::Fail,
        WhenNotMatched::InsertAll,
        Some(false),
    )
    .await;
    let filter = staged_inserted_rows_filter(&strict_create)
        .expect("strict create on the PK must retain the key-existence filter");
    assert_eq!(filter.field_ids, vec![id_field_id]);
    assert_bloom_empty(filter, false, "strict PK create");

    // A valid but non-PK ON column still uses v2 when indices are disabled,
    // yet it must not claim PK conflict coverage.
    let mismatched_on = stage_pk_merge(
        dataset.clone(),
        pk_full_row(dataset.as_ref(), "fresh-non-pk", 12),
        "value",
        WhenMatched::UpdateAll,
        WhenNotMatched::InsertAll,
        Some(false),
    )
    .await;
    assert!(
        staged_inserted_rows_filter(&mismatched_on).is_none(),
        "v2 merge on a non-PK field must not emit a PK filter"
    );

    // Matched-only partial-schema v2 update: no Insert action touches the
    // filter builder, but pinned Lance deliberately retains `Some(empty Bloom)`.
    let partial_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let partial_batch = RecordBatch::try_new(
        partial_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(Int32Array::from(vec![42])),
        ],
    )
    .unwrap();
    let matched_only = stage_pk_merge(
        dataset,
        partial_batch,
        "id",
        WhenMatched::UpdateAll,
        WhenNotMatched::DoNothing,
        Some(false),
    )
    .await;
    assert_eq!(matched_only.stats.num_updated_rows, 1);
    assert_eq!(matched_only.stats.num_inserted_rows, 0);
    let filter = staged_inserted_rows_filter(&matched_only)
        .expect("matched-only PK v2 update must emit Some(empty filter)");
    assert_eq!(filter.field_ids, vec![id_field_id]);
    assert_bloom_empty(filter, true, "matched-only partial-schema PK update");

    // With a scalar index on every ON column and the default `use_index=true`,
    // pinned Lance selects v1. That route hardcodes `inserted_rows_filter=None`.
    let indexed_uri = dir.path().join("rfc023_filter_indexed_v1.lance");
    let mut indexed = fresh_pk_dataset(indexed_uri.to_str().unwrap()).await;
    indexed
        .create_index_builder(&["id"], IndexType::BTree, &ScalarIndexParams::default())
        .replace(true)
        .await
        .unwrap();
    let indexed = Arc::new(indexed);
    let indexed_route = stage_pk_merge(
        indexed.clone(),
        pk_full_row(indexed.as_ref(), "fresh-indexed", 13),
        "id",
        WhenMatched::UpdateAll,
        WhenNotMatched::InsertAll,
        None, // preserve MergeInsertBuilder's default use_index=true
    )
    .await;
    assert!(
        staged_inserted_rows_filter(&indexed_route).is_none(),
        "the pinned Lance all-keys-indexed v1 route must remain visibly unfenced"
    );
}

// --- Guard 19c: pinned Lance key-filter conflicts are directional ----------
//
// Lance evaluates compatibility from the transaction currently being rebased
// against the transaction already committed. Pinned Lance is deliberately strict
// for `filtered current / unfiltered committed`, but the reverse order is
// accepted. RFC-023 must not assume a symmetric conflict matrix until the
// upstream resolver actually supplies one.
#[tokio::test]
async fn unenforced_pk_conflict_matrix_is_directional() {
    struct Case {
        name: &'static str,
        committed: ConflictMatrixTxn,
        current: ConflictMatrixTxn,
        current_succeeds: bool,
        assert_disjoint_filters: bool,
    }

    let cases = [
        Case {
            name: "same-key filtered current / filtered committed",
            committed: ConflictMatrixTxn::Filtered {
                id: "same-fresh-key",
                value: 10,
            },
            current: ConflictMatrixTxn::Filtered {
                id: "same-fresh-key",
                value: 11,
            },
            current_succeeds: false,
            assert_disjoint_filters: false,
        },
        Case {
            name: "disjoint filtered current / filtered committed",
            committed: ConflictMatrixTxn::Filtered {
                id: "left-fresh-key",
                value: 10,
            },
            current: ConflictMatrixTxn::Filtered {
                id: "right-fresh-key",
                value: 11,
            },
            current_succeeds: true,
            assert_disjoint_filters: true,
        },
        Case {
            name: "filtered current / unfiltered Update committed",
            committed: ConflictMatrixTxn::UnfilteredUpdate {
                id: "same-mixed-update-key",
                value: 10,
            },
            current: ConflictMatrixTxn::Filtered {
                id: "same-mixed-update-key",
                value: 11,
            },
            current_succeeds: false,
            assert_disjoint_filters: false,
        },
        Case {
            name: "unfiltered Update current / filtered committed",
            committed: ConflictMatrixTxn::Filtered {
                id: "same-mixed-update-key",
                value: 10,
            },
            current: ConflictMatrixTxn::UnfilteredUpdate {
                id: "same-mixed-update-key",
                value: 11,
            },
            current_succeeds: true,
            assert_disjoint_filters: false,
        },
        Case {
            name: "filtered current / Append committed",
            committed: ConflictMatrixTxn::Append {
                id: "same-mixed-append-key",
                value: 10,
            },
            current: ConflictMatrixTxn::Filtered {
                id: "same-mixed-append-key",
                value: 11,
            },
            current_succeeds: false,
            assert_disjoint_filters: false,
        },
        Case {
            name: "Append current / filtered Update committed",
            committed: ConflictMatrixTxn::Filtered {
                id: "same-mixed-append-key",
                value: 10,
            },
            current: ConflictMatrixTxn::Append {
                id: "same-mixed-append-key",
                value: 11,
            },
            current_succeeds: true,
            assert_disjoint_filters: false,
        },
    ];

    let dir = tempfile::tempdir().unwrap();
    for (case_index, case) in cases.into_iter().enumerate() {
        let uri = dir
            .path()
            .join(format!("rfc023_conflict_{case_index}.lance"));
        let base = Arc::new(fresh_pk_dataset(uri.to_str().unwrap()).await);

        // Stage both operations from the exact same stale base. Staging the
        // second after the first commit would avoid the rebase this guard owns.
        let committed_tx = stage_conflict_matrix_txn(base.clone(), case.committed).await;
        let current_tx = stage_conflict_matrix_txn(base.clone(), case.current).await;

        if case.assert_disjoint_filters {
            let committed_filter = transaction_inserted_rows_filter(&committed_tx).unwrap();
            let current_filter = transaction_inserted_rows_filter(&current_tx).unwrap();
            assert_eq!(
                committed_filter.intersects(current_filter).unwrap(),
                (false, false),
                "{}: chosen Bloom-filter fixtures must be definitively disjoint",
                case.name
            );
        }

        CommitBuilder::new(base.clone())
            .with_max_retries(0)
            .execute(committed_tx)
            .await
            .unwrap_or_else(|error| panic!("{}: first commit failed: {error}", case.name));

        // This is the raw commit/rebase result. `MergeInsertJob::execute`
        // wraps an exhausted semantic retry as TooMuchWriteContention; the
        // substrate contract exposed here is RetryableCommitConflict.
        let current_result = CommitBuilder::new(base)
            .with_max_retries(0)
            .execute(current_tx)
            .await;
        if case.current_succeeds {
            assert!(
                current_result.is_ok(),
                "{}: pinned Lance should accept this direction, got {current_result:?}",
                case.name
            );
        } else {
            assert!(
                matches!(
                    &current_result,
                    Err(lance::Error::RetryableCommitConflict { .. })
                ),
                "{}: expected pinned Lance RetryableCommitConflict, got {current_result:?}",
                case.name
            );
        }
    }
}

// --- Guard 20: camelCase @index equality routes to the scalar index (#283) ----
//
// The #283 read-pushdown fix builds the filter column with datafusion `ident()`
// (case-preserving) instead of `col()` (SQL identifier normalization, which
// lowercases an unquoted name). The correctness tests in literal_filters.rs /
// writes.rs prove the right rows come back, but a result-only assertion also
// passes on a full-scan fallback — exactly the gap testing.md warns about. This
// guard pins the *plan*: an equality on a camelCase BTREE column must compile to
// a `ScalarIndexQuery` under the fix's expr shape, and must NOT under the old
// `col()` shape (which lowercases `repoName` → a nonexistent `reponame`). A
// regression that breaks camelCase index routing — or a revert to `col()` —
// turns this red instead of silently degrading to a full scan.
#[tokio::test]
async fn camelcase_index_equality_routes_to_scalar_index() {
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{col, ident, lit};

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("camelcase_index.lance");
    let uri = uri.to_str().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("repoName", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(StringArray::from(vec![
                "acme", "globex", "initech", "umbrella",
            ])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let mut ds = Dataset::write(reader, uri, Some(params)).await.unwrap();
    ds.create_index_builder(
        &["repoName"],
        IndexType::BTree,
        &ScalarIndexParams::default(),
    )
    .replace(true)
    .await
    .unwrap();

    async fn plan_str(ds: &Dataset, filter: datafusion::prelude::Expr) -> lance::Result<String> {
        let mut scanner = ds.scan();
        scanner.filter_expr(filter);
        let plan = scanner.create_plan().await?;
        Ok(format!("{}", displayable(plan.as_ref()).indent(true)))
    }

    // The fix's shape: ident() preserves case → resolves `repoName` → index.
    let used = plan_str(&ds, ident("repoName").eq(lit("acme")))
        .await
        .expect("ident(\"repoName\") must plan against the case-preserved schema");
    assert!(
        used.contains("ScalarIndexQuery"),
        "camelCase @index equality must route to the scalar index (not full scan), got:\n{used}"
    );

    // The pre-fix shape: col() normalizes `repoName` → `reponame`, which does not
    // exist in the case-sensitive schema, so planning fails. This is precisely
    // why `col()` could never reach the index and surfaced the #283 runtime error
    // — it could not silently full-scan past the index either.
    let err = plan_str(&ds, col("repoName").eq(lit("acme"))).await;
    assert!(
        err.is_err(),
        "col() lowercases repoName→reponame against a case-sensitive schema; \
         planning must fail rather than resolve, confirming ident() is required \
         for camelCase index routing. got plan:\n{err:?}"
    );
}

// --- Guard: filtered scans tolerate merge_insert's overlapping row-id ranges
//     (lance#7444, fixed by lance#7480; consumed via the vendored lance-table
//     patch) -------------------------------------------------------------
//
// An update-style merge_insert over a fragment that was itself merge-written
// reuses the updated rows' stable row ids in its rewritten fragments (row-id
// lineage spec: updates preserve `_rowid`) while the superseded fragment keeps
// its full id sequence plus a deletion vector — legal, overlapping
// cross-fragment id ranges. A later delete leaves the overlap sparsely tiled;
// unpatched lance-table 7.0.0's `RowIdIndex::new` asserted dense tiling and
// failed any filtered read that builds the id→address map: "Wrong range"
// debug assert, "all columns in a record batch must have the same length" (or
// a silently-wrong batch) in release. Faithful transcription of lance#7444's
// minimal repro: merge-seed → merge-update → delete → filter + with_row_id.
// This guard turns red if a Lance bump regresses the fix, or if the vendored
// patch is dropped before the pinned lance-table ships lance#7480.
#[tokio::test]
async fn filtered_scan_tolerates_merge_update_row_id_overlap() {
    use futures::TryStreamExt;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("slug", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
    ]));
    let mk_batch = |slugs: Vec<String>, titles: Vec<String>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(slugs)) as arrow_array::ArrayRef,
                Arc::new(StringArray::from(titles)) as arrow_array::ArrayRef,
            ],
        )
        .unwrap()
    };

    // Empty dataset WITH stable row ids; both data writes are merge_inserts
    // (merge-on-merge is lance#7444's trigger qualifier — a plain
    // Dataset::write seed does not reproduce).
    let empty = mk_batch(Vec::new(), Vec::new());
    let reader = RecordBatchIterator::new(vec![Ok(empty)], schema.clone());
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let ds = Dataset::write(reader, uri, Some(params)).await.unwrap();

    let merge = |ds: Dataset, batch: RecordBatch, schema: Arc<Schema>| async move {
        let job = MergeInsertBuilder::try_new(Arc::new(ds), vec!["slug".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let source = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let (ds, _stats) = job.execute_reader(source).await.unwrap();
        (*ds).clone()
    };

    // Merge #1 seeds 40 rows; merge #2 rewrites 15 of them (keeping their
    // stable ids — the overlap with the merge-written seed fragment).
    let seed = mk_batch(
        (1..=40).map(|i| format!("t{i}")).collect(),
        (1..=40).map(|i| format!("r{i}")).collect(),
    );
    let ds = merge(ds, seed, schema.clone()).await;
    let updates = mk_batch(
        (1..=15).map(|i| format!("t{i}")).collect(),
        (1..=15).map(|i| format!("e{i}")).collect(),
    );
    let ds = Arc::new(merge(ds, updates, schema.clone()).await);

    // The delete's deletion vector makes the overlapping id region sparse.
    let staged = lance::dataset::DeleteBuilder::new(ds.clone(), "slug = 't20'")
        .execute_uncommitted()
        .await
        .unwrap();
    assert_eq!(staged.num_deleted_rows, 1, "expected exactly t20 deleted");
    let ds = CommitBuilder::new(ds)
        .execute(staged.transaction)
        .await
        .unwrap();

    // filter + with_row_id forces the RowIdIndex build (a full scan does
    // not). On the broken index this errors/panics; on the fixed one every
    // live id resolves.
    for (slug, expected) in [("t3", 1usize), ("t20", 0usize)] {
        let mut scan = ds.scan();
        scan.with_row_id();
        scan.filter(&format!("slug = '{slug}'")).unwrap();
        let batches: Vec<RecordBatch> = scan
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, expected, "filtered read for {slug}");
    }
}
