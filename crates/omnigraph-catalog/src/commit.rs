//! The copy-on-write publish of `__manifest`.
//!
//! A publish writes the branch's whole row set into new files and commits it
//! as a Lance overwrite with zero retries. Lance then writes
//! `_versions/{read_version + 1}.manifest` only if it is absent and never
//! rebases over a concurrent commit, so the version number is the put-if-absent
//! CAS. Earlier versions stay readable, which keeps point-in-time reads and
//! lost-acknowledgement read-back unchanged.
//!
//! The stored row shape lives in `record`.
//!
//! Costs. The row set is one row per table identity, the head commit's
//! record and the buffer, which `TAIL_MAX_COMMITS` bounds, in one fragment:
//! the bytes written and decoded per publish do not depend on the count of
//! commits behind the head. Each version keeps its own
//! copy and nothing prunes `__manifest` versions today, so retained bytes grow
//! with the publication count until version retention exists.
//!
//! Lance's auto-cleanup hook is skipped: `__manifest` versions are the snapshot
//! and time-travel authority.

use std::sync::Arc;

use arrow_array::RecordBatch;
use lance::Dataset;
use lance::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

use crate::error::Result;
use crate::migrations::guard_row_layout;
use crate::publisher::map_lance_publish_error;
use crate::record::{compact_to_storage, manifest_storage_schema};
use crate::state::ManifestRows;

/// Write `rows` as the whole row set of `dataset`'s version + 1. The schema
/// metadata, the stamp with it, carries over from `dataset`, whose stamp must be
/// the one this binary writes.
pub(crate) async fn overwrite(
    dataset: Dataset,
    rows: &ManifestRows,
) -> Result<(Dataset, RecordBatch)> {
    guard_row_layout(&dataset)?;
    let schema = manifest_storage_schema(dataset.schema().metadata.clone());
    let batch = compact_to_storage(&rows.to_batch()?, &schema)?;
    let committed = commit_overwrite(dataset, vec![batch.clone()]).await?;
    Ok((committed, batch))
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
