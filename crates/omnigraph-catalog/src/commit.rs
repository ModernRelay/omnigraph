//! The copy-on-write publish of `__manifest`.
//!
//! A publish rewrites the branch's live row set into new files and commits it
//! as a Lance overwrite with zero retries. Lance then writes
//! `_versions/{read_version + 1}.manifest` only if it is absent and never
//! rebases over a concurrent commit, so the version number is the put-if-absent
//! CAS. Earlier versions stay readable, which keeps point-in-time reads and
//! lost-acknowledgement read-back unchanged.
//!
//! The stored row shape lives in `record`.
//!
//! Costs. The live set is normally one fragment (Lance splits a write at
//! 1,048,576 rows per file). The live set still holds every history row
//! (`graph_commit` and every `table_version` registration): the bytes written
//! and decoded per publish, and the publish's memory (the scan keeps every
//! batch for the rewrite), grow with history. Each version keeps its own full
//! copy and nothing prunes `__manifest` versions today, so retained bytes grow
//! with the square of the publication count until version retention exists.
//!
//! Lance's auto-cleanup hook is skipped: `__manifest` versions are the snapshot
//! and time-travel authority, and a `__manifest` created before the v7 bump
//! still carries the stored auto-cleanup config.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::Schema;
use datafusion::arrow::compute::filter_record_batch;
use lance::Dataset;
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

use crate::error::{OmniError, Result};
use crate::migrations::{INTERNAL_MANIFEST_SCHEMA_VERSION, read_stamp, stamp_entry};
use crate::publisher::map_lance_publish_error;
use crate::record::{
    StoredShape, compact_to_storage, flat_manifest_schema, flat_to_storage,
    manifest_storage_schema, written_shape,
};
use crate::state::string_column;

/// Rewrite the live row set (`live_rows` minus the keys `pending` replaces,
/// plus `pending`) as new files and commit it at `dataset`'s version + 1. Both
/// inputs carry the logical `manifest_schema` columns; the stored shape follows
/// `written_shape`, and a packed write stamps [`INTERNAL_MANIFEST_SCHEMA_VERSION`]
/// in the same commit.
pub(crate) async fn overwrite(
    dataset: Dataset,
    pending: RecordBatch,
    live_rows: Vec<RecordBatch>,
) -> Result<Dataset> {
    let replaced: HashSet<&str> = string_column(&pending, "object_id")?
        .iter()
        .flatten()
        .collect();
    let mut metadata = dataset.schema().metadata.clone();
    let stamp = read_stamp(&dataset);
    if let Some(above) = stamp.filter(|stamp| *stamp > INTERNAL_MANIFEST_SCHEMA_VERSION) {
        return Err(OmniError::manifest_internal(format!(
            "__manifest is stamped at internal schema v{above}, above the v{INTERNAL_MANIFEST_SCHEMA_VERSION} this binary writes; refusing to publish over it"
        )));
    }
    let shape = written_shape(stamp);
    let schema = match shape {
        StoredShape::Packed => {
            let (key, value) = stamp_entry(INTERNAL_MANIFEST_SCHEMA_VERSION);
            metadata.insert(key, value);
            manifest_storage_schema(metadata)?
        }
        StoredShape::Flat => Arc::new(Schema::new_with_metadata(
            flat_manifest_schema().fields().clone(),
            metadata,
        )),
    };
    let to_storage = |batch: &RecordBatch| match shape {
        StoredShape::Packed => compact_to_storage(batch, &schema),
        StoredShape::Flat => flat_to_storage(batch, &schema),
    };
    let mut batches = Vec::with_capacity(live_rows.len() + 1);
    for batch in &live_rows {
        let keep = BooleanArray::from_iter(
            string_column(batch, "object_id")?
                .iter()
                .map(|id| Some(!id.is_some_and(|id| replaced.contains(id)))),
        );
        let kept = filter_record_batch(batch, &keep).map_err(OmniError::arrow_internal)?;
        if kept.num_rows() > 0 {
            batches.push(to_storage(&kept)?);
        }
    }
    batches.push(to_storage(&pending)?);
    commit_overwrite(dataset, batches).await
}

/// Commit `batches` as the whole stored row set at `dataset`'s version + 1 (the module doc: zero
/// retries, the version number is the CAS).
pub(crate) async fn commit_overwrite(
    dataset: Dataset,
    batches: Vec<RecordBatch>,
) -> Result<Dataset> {
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        auto_cleanup: None,
        skip_auto_cleanup: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let dataset = Arc::new(dataset);
    let transaction = InsertBuilder::new(dataset.clone())
        .with_params(&params)
        .execute_uncommitted(batches)
        .await
        .map_err(map_lance_publish_error)?;
    CommitBuilder::new(dataset)
        .with_max_retries(0)
        .with_skip_auto_cleanup(true)
        .execute(transaction)
        .await
        .map_err(map_lance_publish_error)
}
