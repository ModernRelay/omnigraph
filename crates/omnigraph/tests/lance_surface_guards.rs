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
use lance::dataset::transaction::Operation;
use lance::dataset::write::delete::DeleteResult;
use lance::dataset::{
    CommitBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode,
    WriteParams,
};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_namespace::LanceNamespace;
use lance_table::io::commit::ManifestNamingScheme;

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
    use lance_core::utils::mask::RowAddrTreeMap;
    let ds: Arc<Dataset> = unimplemented!();
    let staged = DeleteBuilder::new(ds, "x = 1")
        .execute_uncommitted()
        .await?;
    let _txn: lance::dataset::transaction::Transaction = staged.transaction;
    let _num_deleted: u64 = staged.num_deleted_rows;
    let _affected: Option<RowAddrTreeMap> = staged.affected_rows;
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
    use lance_core::utils::mask::RowAddrTreeMap;
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
// design relies on three facts pinned here:
//   1. plain `delete_branch` errors on a missing ref (so the design uses the
//      force variant instead);
//   2. `force_delete_branch` removes an existing (forked) branch — the orphan
//      case, where a `tree/{branch}/` exists;
//   3. `force_delete_branch` on a *fully-absent* branch (no tree dir) still
//      errors on the local store, because `remove_dir_all`'s NotFound is not
//      caught for Lance's native error variant. `TableStore::force_delete_branch`
//      wraps this to be fully idempotent. Pin the raw quirk so a future Lance
//      fix (which would let us simplify the wrapper) is noticed.

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

    // (3) Quirk: force_delete on a fully-absent branch errors on the local
    // store (worked around by TableStore::force_delete_branch).
    assert!(
        ds.force_delete_branch("never").await.is_err(),
        "force_delete_branch on a fully-absent branch no longer errors — \
         TableStore::force_delete_branch's NotFound tolerance can be simplified."
    );
}

// --- Guard 10: blob-column compaction is still broken in this Lance --------
//
// `db/omnigraph/optimize.rs` skips tables with blob columns while
// `LANCE_SUPPORTS_BLOB_COMPACTION = false`: Lance `compact_files` forces
// `BlobHandling::AllBinary`, and the blob-v2 struct decoder mis-counts columns
// ("more fields in the schema than provided column indices"), failing even a
// pristine uniform-V2_2 multi-fragment blob table. Reads are unaffected (they
// use descriptor handling).
//
// WHEN THIS TEST TURNS RED (compact_files no longer errors), the Lance bug is
// fixed: flip `LANCE_SUPPORTS_BLOB_COMPACTION` to true in optimize.rs, drop the
// blob-skip branch + the `optimize_skips_blob_table_and_reports_skip`
// skip assertions in maintenance.rs, and re-pin docs/dev/lance.md.

#[tokio::test]
async fn compact_files_still_fails_on_blob_columns() {
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

    let result = compact_files(&mut ds, CompactionOptions::default(), None).await;
    let err = result.expect_err(
        "compact_files unexpectedly SUCCEEDED on a blob table — the Lance blob-v2 \
         compaction bug is fixed. Flip LANCE_SUPPORTS_BLOB_COMPACTION to true in \
         db/omnigraph/optimize.rs, remove the blob-skip branch, and re-pin docs/dev/lance.md.",
    );
    assert!(
        err.to_string()
            .contains("more fields in the schema than provided column indices"),
        "blob compaction failed with an unexpected error (Lance internals may have \
         shifted): {err}"
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
            false,
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

    // The widened case must show the index-defeating column CAST (the precise
    // shape the fix avoids by coercing the literal to the column type).
    let widened = plan_str(&ds, col("n32").eq(lit(5i64))).await;
    assert!(
        widened.contains("CAST(n32 AS Int64)"),
        "expected a column-side cast in the widened plan, got:\n{widened}"
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

// --- Guard: row-lineage stamping survives an in-attempt rebase ---------------
//
// `db/manifest/state.rs::decode_graph_commit_row` DERIVES each graph commit's
// manifest version from `_row_created_at_version` instead of a payload literal.
// That is correct only because Lance assigns the lineage stamp inside
// `Transaction::build_manifest`, per commit attempt, from the just-reloaded
// manifest (`lance/src/io/commit.rs` target_version; `transaction.rs` Update
// arm) — so a commit that internally REBASES past a concurrent disjoint commit
// stamps the FINAL landed version, never the version implied by the base the
// transaction was built against. Nothing upstream pins lineage-under-rebase;
// if a future Lance moves the stamping to write time, this goes red before the
// derivation silently breaks.
#[tokio::test]
async fn merge_insert_rebase_stamps_row_lineage_at_landed_version() {
    use futures::TryStreamExt;
    use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
    use arrow_array::UInt64Array;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("g_lineage_rebase.lance");
    let uri = uri.to_str().unwrap();
    let mut ds = fresh_dataset(uri).await; // stable row IDs on (fresh_dataset)

    // Mirror `__manifest`: `id` is the unenforced PK, so concurrent disjoint
    // merge-inserts carry non-intersecting inserted-key blooms and REBASE
    // rather than conflict (the exact publisher shape).
    ds.update_field_metadata()
        .update(
            "id",
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())],
        )
        .unwrap()
        .await
        .unwrap();
    let base_version = ds.version().version;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let merge_insert_batch = |id: &str, value: i32| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![id.to_string()])),
                Arc::new(Int32Array::from(vec![value])),
            ],
        )
        .unwrap()
    };
    let uncommitted_insert = |ds: Arc<Dataset>, batch: RecordBatch| async move {
        let mut builder = MergeInsertBuilder::try_new(ds, vec!["id".to_string()]).unwrap();
        builder.when_matched(WhenMatched::UpdateAll);
        builder.when_not_matched(WhenNotMatched::InsertAll);
        builder.use_index(false);
        builder.skip_auto_cleanup(true);
        let job = builder.try_build().unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema_of_batch());
        let stream = lance_datafusion::utils::reader_to_stream(Box::new(reader));
        job.execute_uncommitted(stream).await.unwrap()
    };
    fn schema_of_batch() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    // Two-phase merge-insert built against `base_version`, inserting `carol`.
    let pending = uncommitted_insert(Arc::new(ds.clone()), merge_insert_batch("carol", 3)).await;

    // Interleave: a DISJOINT commit (`dave`) lands before `carol`'s commit.
    let ds_fresh = Dataset::open(uri).await.unwrap();
    let interleaved =
        uncommitted_insert(Arc::new(ds_fresh.clone()), merge_insert_batch("dave", 4)).await;
    let interleaved_committed = CommitBuilder::new(Arc::new(ds_fresh))
        .with_skip_auto_cleanup(true)
        .execute(interleaved.transaction)
        .await
        .unwrap();
    let interleaved_version = interleaved_committed.version().version;

    // Commit `carol` against the STALE base handle: Lance reconciles against
    // live storage and rebases past `dave`'s commit.
    let committed = CommitBuilder::new(Arc::new(ds))
        .with_skip_auto_cleanup(true)
        .execute(pending.transaction)
        .await
        .unwrap();
    let landed_version = committed.version().version;
    // Non-vacuity: the rebase must actually have happened, else this guard
    // pins nothing.
    assert!(
        landed_version >= base_version + 2 && landed_version > interleaved_version,
        "expected a rebased commit (base {base_version}, interleaved \
         {interleaved_version}, landed {landed_version})"
    );

    // The load-bearing pin: the rebased commit's inserted row is stamped at the
    // LANDED version, not base+1.
    let mut scanner = committed.scan();
    scanner
        .project(&["id", "_row_created_at_version"])
        .unwrap();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut carol = None;
    let mut dave = None;
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let created = batch
            .column_by_name("_row_created_at_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            match ids.value(row) {
                "carol" => carol = Some(created.value(row)),
                "dave" => dave = Some(created.value(row)),
                _ => {}
            }
        }
    }
    assert_eq!(
        carol,
        Some(landed_version),
        "the REBASED commit's inserted row must be stamped at the landed \
         version {landed_version} — a write-time stamp would break \
         decode_graph_commit_row's derivation"
    );
    assert_eq!(
        dave,
        Some(interleaved_version),
        "the interleaved commit's row keeps its own landed version"
    );
}
