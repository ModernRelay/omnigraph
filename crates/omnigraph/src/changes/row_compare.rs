//! Typed logical row equality shared by every change surface.
//!
//! Both the per-commit enumerator ([`super::enumerate`]) and the net-diff
//! ([`super::diff_snapshots`]) answer the same question — *did this logical row
//! change?* — and must answer it identically, so the definition lives here once
//! rather than in two comparators that can drift. Non-Blob user columns compare
//! by Arrow logical equality on one-row slices (typed, offset-aware,
//! validity-aware, so null, `""`, and `[]` stay distinct and no display-string
//! join can conflate values); Blob columns compare payload-free by physical
//! descriptor identity, with an exact payload byte-compare only on a descriptor
//! tie so compaction cannot surface phantom updates. Only the five reserved
//! Lance virtual columns are skipped — a legal `_row_`-prefixed user property
//! participates in change detection.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;

use arrow_array::{Array, RecordBatch, StringArray, StructArray, UInt64Array};
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use lance_core::datatypes::BlobHandling;

use super::model::{COMMIT_CHANGES_MAX_BYTES, is_reserved_storage_system_column};
use crate::blob::BlobDescriptorDecoder;
use crate::db::export_blob_values;
use crate::error::{OmniError, Result};
use crate::table_store::TableStore;

/// Ordered-scan batch shape, matching the other production ordered-by-id
/// scans (branch merge, export): row estimate plus byte target, never strict.
/// Lance treats both as targets, so page bounds are enforced by charging the
/// serialized size of each emitted change, never by trusting the scanner.
const CHANGE_SCAN_TARGET_ROWS: usize = 8_192;

/// One scanned row prepared for comparison. Holds a zero-copy one-row slice
/// (retaining `_rowid` and descriptor columns for lazy image and payload
/// access) plus per-Blob-column physical descriptor identities. No JSON image
/// and no Blob payload I/O happen here.
#[derive(Debug, Clone)]
pub(crate) struct RawRow {
    pub(crate) id: String,
    pub(crate) slice: RecordBatch,
    /// `(column, source-qualified physical identity)` per Blob column, in
    /// schema order. Managed identities carry the owning fragment (Blob
    /// descriptor fields are file-relative), so equal identities on one table
    /// lifetime imply equal payloads and inequality forces a payload compare.
    blob_signatures: Vec<(String, String)>,
}

/// An `id`-ordered stream of one table snapshot's rows, filled lazily one Lance
/// batch at a time. Both endpoints of a diff are walked in lockstep so the
/// merge stays streaming and never buffers a delta-wide row set.
pub(crate) struct OrderedRows {
    dataset: Dataset,
    stream: Option<Pin<Box<DatasetRecordBatchStream>>>,
    pending: VecDeque<RawRow>,
}

impl OrderedRows {
    pub(crate) async fn open(dataset: Dataset, after_id: Option<&str>) -> Result<Self> {
        let after_id = after_id.map(str::to_string);
        let stream = Box::pin(
            TableStore::scan_stream_with(
                &dataset,
                None,
                None,
                Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                true,
                move |scanner| {
                    if let Some(after_id) = after_id {
                        scanner.filter_expr(col("id").gt(lit(after_id)));
                    }
                    // Descriptor-sized batches bounded by rows AND bytes, the
                    // same shape as every other production ordered-by-id scan.
                    // strict_batch_size is deliberately absent: Lance's strict
                    // stream coalesces to a row count the environment can
                    // override and ignores the byte target while accumulating.
                    scanner.batch_size(CHANGE_SCAN_TARGET_ROWS);
                    scanner.batch_size_bytes(COMMIT_CHANGES_MAX_BYTES);
                    scanner.blob_handling(BlobHandling::BlobsDescriptions);
                    // Managed Blob descriptors are file-relative, so the owning
                    // fragment (high 32 bits of `_rowaddr`) is required to tell
                    // an in-place unchanged row from a same-length Blob-only
                    // update that moved to a new fragment with colliding local
                    // descriptor coordinates. `with_row_id` above stays on for
                    // the payload tie-break's stable-id `take_blobs`.
                    scanner.with_row_address();
                    Ok(())
                },
            )
            .await?,
        );
        Ok(Self {
            dataset,
            stream: Some(stream),
            pending: VecDeque::new(),
        })
    }

    pub(crate) async fn peek(&mut self) -> Result<Option<&RawRow>> {
        self.fill().await?;
        Ok(self.pending.front())
    }

    pub(crate) async fn pop(&mut self) -> Result<Option<RawRow>> {
        self.fill().await?;
        Ok(self.pending.pop_front())
    }

    async fn fill(&mut self) -> Result<()> {
        while self.pending.is_empty() {
            let Some(stream) = self.stream.as_mut() else {
                return Ok(());
            };
            match stream.try_next().await {
                Ok(Some(batch)) => self.pending = prepare_batch(&batch)?,
                Ok(None) => {
                    self.stream = None;
                    return Ok(());
                }
                Err(error) => return Err(OmniError::Lance(error.to_string())),
            }
        }
        Ok(())
    }

    pub(crate) fn dataset(&self) -> &Dataset {
        &self.dataset
    }
}

/// Turn one scanned batch into comparison-ready rows: one-row slices plus
/// physical descriptor identities for Blob columns. Pure in-memory work —
/// Blob payloads are never touched here.
fn prepare_batch(batch: &RecordBatch) -> Result<VecDeque<RawRow>> {
    let ids = batch
        .column_by_name("id")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| OmniError::Lance("change row is missing string id".to_string()))?;

    let mut blob_columns = Vec::new();
    for (field, column) in batch.schema_ref().fields().iter().zip(batch.columns()) {
        if is_reserved_storage_system_column(field.name()) {
            continue;
        }
        let lance_field = lance::datatypes::Field::try_from(field.as_ref())
            .map_err(|error| OmniError::Lance(error.to_string()))?;
        if lance_field.is_blob() {
            let descriptions = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    OmniError::Lance(format!(
                        "expected blob descriptions for change column '{}'",
                        field.name()
                    ))
                })?;
            blob_columns.push((
                field.name().as_str(),
                BlobDescriptorDecoder::try_new(descriptions)?,
            ));
        }
    }
    // Name-order the signatures so `rows_equal`'s positional zip aligns by
    // column NAME whenever the two sides' column sets match — the same
    // order-insensitivity the schema gate's name-keyed fingerprint provides.
    // No supported writer reorders columns within one table lifetime, so this
    // is defense in depth against a physical reorder surfacing as phantom
    // whole-table updates.
    blob_columns.sort_by(|left, right| left.0.cmp(right.0));

    // Managed Blob descriptors are resolved relative to the owning fragment, so
    // qualify each managed identity with the row's fragment id (high 32 bits of
    // `_rowaddr`). Only needed when the table has a Blob column.
    let row_addresses = if blob_columns.is_empty() {
        None
    } else {
        Some(
            batch
                .column_by_name("_rowaddr")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::Lance(
                        "change scan is missing _rowaddr; managed Blob comparison needs the owning fragment"
                            .to_string(),
                    )
                })?,
        )
    };

    let mut rows = VecDeque::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut blob_signatures = Vec::with_capacity(blob_columns.len());
        if let Some(row_addresses) = row_addresses {
            let fragment_id = (row_addresses.value(row) >> 32) as u32;
            for (name, decoder) in &blob_columns {
                blob_signatures.push((
                    name.to_string(),
                    decoder.physical_identity(row, fragment_id)?,
                ));
            }
        }
        rows.push_back(RawRow {
            id: ids.value(row).to_string(),
            slice: batch.slice(row, 1),
            blob_signatures,
        });
    }
    Ok(rows)
}

/// Typed structural row equality with an exact Blob payload tie-break.
///
/// Non-Blob user columns compare by Arrow logical equality on the two one-row
/// slices — typed, offset-aware, validity-aware, so null, `""`, and `[]` stay
/// distinct and no display-string join can conflate values. Blob columns
/// compare by physical descriptor identity first; only a descriptor tie —
/// equal scalars, relocated descriptor — pays payload I/O, because compaction
/// relocates identical bytes and must not surface as a phantom update.
///
/// The two slices are required to share one user schema (both diff surfaces
/// gate on a schema fingerprint before comparison), so a missing column is an
/// internal invariant break, not a logical difference.
pub(crate) async fn rows_equal(
    from_dataset: &Dataset,
    left: &RawRow,
    to_dataset: &Dataset,
    right: &RawRow,
) -> Result<bool> {
    let blob_columns: HashSet<&str> = left
        .blob_signatures
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    for (field, column) in left
        .slice
        .schema_ref()
        .fields()
        .iter()
        .zip(left.slice.columns())
    {
        let name = field.name();
        if is_reserved_storage_system_column(name) || blob_columns.contains(name.as_str()) {
            continue;
        }
        let other = right.slice.column_by_name(name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "schema-gated change row is missing column '{name}'"
            ))
        })?;
        if column.to_data() != other.to_data() {
            return Ok(false);
        }
    }

    if left.blob_signatures == right.blob_signatures {
        return Ok(true);
    }
    let column_sets_match = left.blob_signatures.len() == right.blob_signatures.len()
        && left
            .blob_signatures
            .iter()
            .zip(&right.blob_signatures)
            .all(|(l, r)| l.0 == r.0);
    if !column_sets_match {
        // The two pinned versions disagree on the Blob column set, so the
        // logical image necessarily changed shape.
        return Ok(false);
    }
    let differing: HashSet<String> = left
        .blob_signatures
        .iter()
        .zip(&right.blob_signatures)
        .filter(|(l, r)| l.1 != r.1)
        .map(|(l, _)| l.0.clone())
        .collect();
    let left_values = blob_values_for(from_dataset, &left.slice, &differing).await?;
    let right_values = blob_values_for(to_dataset, &right.slice, &differing).await?;
    Ok(left_values == right_values)
}

/// Logical Blob values (managed bytes, external URI, or null) for one row's
/// selected columns, read through the same helper export uses.
async fn blob_values_for(
    dataset: &Dataset,
    slice: &RecordBatch,
    columns: &HashSet<String>,
) -> Result<HashMap<String, Vec<Option<String>>>> {
    let row_id = slice
        .column_by_name("_rowid")
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| OmniError::Lance("change row is missing _rowid".to_string()))?
        .value(0);
    export_blob_values(dataset, slice, &[row_id], columns).await
}
