//! The copy-on-write publish of `__manifest`.
//!
//! A publish rewrites the branch's live row set into new files and commits it
//! as a Lance overwrite with zero retries. Lance then writes
//! `_versions/{read_version + 1}.manifest` only if it is absent and never
//! rebases over a concurrent commit, so the version number is the put-if-absent
//! CAS. Earlier versions stay readable, which keeps point-in-time reads and
//! lost-acknowledgement read-back unchanged.
//!
//! Costs. The live set is normally one fragment (Lance splits a write at
//! 1,048,576 rows per file), so a scan no longer pays the merge writer's
//! per-fragment and per-deletion-file requests. A file's pages are still read
//! separately, so this is not a guarantee of a constant request count; the
//! local history curve measured 40-41 requests per write from 1 to 1,024 prior
//! publications. The live set still holds every history row (`graph_commit`
//! and every `table_version` registration): the bytes written and decoded per
//! publish, and the publish's memory (the scan keeps every batch for the
//! rewrite), grow with history. Each version keeps its own full copy and
//! nothing prunes `__manifest` versions today, so retained bytes grow with the
//! square of the publication count until version retention exists.
//!
//! Lance's auto-cleanup hook is skipped: `__manifest` versions are the snapshot
//! and time-travel authority, and a `__manifest` created before the v7 bump
//! still carries the stored auto-cleanup config.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use datafusion::arrow::compute::filter_record_batch;
use lance::Dataset;
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};

use crate::error::{OmniError, Result};
use crate::publisher::map_lance_publish_error;
use crate::state::{manifest_schema, string_column};

/// Rewrite the live row set (`live_rows` minus the keys `pending` replaces,
/// plus `pending`) as new files and commit it at `dataset`'s version + 1,
/// keeping the dataset's schema metadata, where the internal-schema stamp lives.
pub(crate) async fn overwrite(
    dataset: Dataset,
    pending: RecordBatch,
    live_rows: Vec<RecordBatch>,
) -> Result<Dataset> {
    let replaced: HashSet<&str> = string_column(&pending, "object_id")?
        .iter()
        .flatten()
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        manifest_schema().fields().clone(),
        dataset.schema().metadata.clone(),
    ));
    let mut batches = Vec::with_capacity(live_rows.len() + 1);
    for batch in &live_rows {
        let keep = BooleanArray::from_iter(
            string_column(batch, "object_id")?
                .iter()
                .map(|id| Some(!id.is_some_and(|id| replaced.contains(id)))),
        );
        let kept = filter_record_batch(batch, &keep).map_err(OmniError::arrow_internal)?;
        if kept.num_rows() > 0 {
            batches.push(with_schema(&kept, &schema)?);
        }
    }
    batches.push(with_schema(&pending, &schema)?);
    let params = WriteParams {
        mode: WriteMode::Overwrite,
        auto_cleanup: None,
        skip_auto_cleanup: true,
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

fn with_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "manifest batch is missing column {}",
                    field.name()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(OmniError::arrow_internal)
}
