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
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
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

// --- Guard 8: Dataset::delete returns DeleteResult { new_dataset, num_deleted_rows } ---
//
// `table_store.rs::delete_where` consumes both fields. When MR-A migrates
// `delete_where` to two-phase via `DeleteBuilder::execute_uncommitted`, this
// guard updates to pin the staged path. Compile-only.

#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    unused_mut,
    clippy::diverging_sub_expression
)]
async fn _compile_delete_result_field_shape() -> lance::Result<()> {
    let mut ds: Dataset = unimplemented!();
    let result: DeleteResult = ds.delete("x = 1").await?;
    let _new_dataset: Arc<Dataset> = result.new_dataset;
    let _num_deleted: u64 = result.num_deleted_rows;
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

// --- Guard 14: BTREE scalar-index range-boundary correctness (lance#6796) ----
//
// lance#6796 (issue #6792) fixed a BTREE range-query bound-inclusiveness bug:
// `price <= 10 AND price > 5` returned the wrong boundary row (5.0 instead of
// 10.0). OmniGraph builds BTREE scalar indexes (`ensure_indices`) and pushes
// range filters, so it was exposed on 6.0.1. This reproduces the exact #6792
// shape (5 rows + an explicit BTREE drives the index path even on tiny data,
// per the upstream repro) and pins the corrected inclusive-`<=` / exclusive-`>`
// semantics. It turns red if a future Lance regression reintroduces the bug.
#[tokio::test]
async fn btree_range_query_boundary_is_correct() {
    use arrow_array::Float64Array;
    use futures::TryStreamExt;

    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("guard14.lance");
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
