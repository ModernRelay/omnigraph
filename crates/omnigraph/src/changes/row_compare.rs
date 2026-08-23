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

use std::collections::{HashMap, HashSet};
use std::pin::Pin;

use arrow_array::{Array, RecordBatch, StringArray, StructArray, UInt64Array};
use datafusion::prelude::{Expr, col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use lance_core::datatypes::BlobHandling;
use lance_table::format::Fragment;

use super::model::{COMMIT_CHANGES_MAX_BYTES, is_reserved_storage_system_column};
use crate::blob::{BlobDescriptor, BlobDescriptorDecoder};
use crate::db::{STABLE_PROPERTY_ID_METADATA_KEY, export_blob_values};
use crate::error::{OmniError, Result};
use crate::table_store::TableStore;

/// Fingerprint of one table's user-visible schema for the schema-compatibility
/// proof both change surfaces rely on: per field, its Arrow type, nullability,
/// stable property identity marker, and whether it is a Blob. Name-keyed map
/// comparison is order-insensitive, so a physical column reorder is not a false
/// boundary. Shared by the per-commit enumerator (parent→child gate) and the
/// cross-branch net diff so the two cannot drift.
pub(crate) fn user_schema_fingerprint(
    dataset: &Dataset,
) -> HashMap<String, (String, bool, Option<String>, bool)> {
    dataset
        .schema()
        .fields
        .iter()
        .filter(|field| !is_reserved_storage_system_column(&field.name))
        .map(|field| {
            (
                field.name.clone(),
                (
                    format!("{:?}", field.data_type()),
                    field.nullable,
                    field.metadata.get(STABLE_PROPERTY_ID_METADATA_KEY).cloned(),
                    field.is_blob(),
                ),
            )
        })
        .collect()
}

/// One Blob column's comparison signature: its data-file-qualified physical
/// identity plus whether it is a managed (inline/packed/dedicated) descriptor.
/// External and null identities are source-independent and fully encode the
/// logical value, so a difference is authoritative; a managed identity is
/// qualified by the owning data file's immutable UUID path, so an equal managed
/// identity implies equal bytes across overwrites and branches (see
/// [`BlobDescriptorDecoder::physical_identity`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlobColumnSig {
    name: String,
    identity: String,
    managed: bool,
}

/// Ordered-scan batch shape, matching the other production ordered-by-id
/// scans (branch merge, export): row estimate plus byte target, never strict.
/// Lance treats both as targets, so page bounds are enforced by charging the
/// serialized size of each emitted change, never by trusting the scanner.
const CHANGE_SCAN_TARGET_ROWS: usize = 8_192;

/// Approximate scanner batch targets. Candidate scans derive these from the
/// caller's remaining page budget (plus one continuation sentinel), so a
/// one-change page never asks Lance to prepare an 8,192-row candidate batch.
/// Lance treats both values as targets rather than hard limits; the hard page
/// bound remains the serialized-change accounting in `enumerate`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanTargets {
    rows: usize,
    bytes: u64,
}

impl ScanTargets {
    pub(crate) fn for_page(remaining_rows: usize, remaining_bytes: u64) -> Self {
        Self {
            rows: remaining_rows
                .saturating_add(1)
                .clamp(1, CHANGE_SCAN_TARGET_ROWS),
            bytes: remaining_bytes.clamp(1, COMMIT_CHANGES_MAX_BYTES),
        }
    }

    pub(crate) fn rows(self) -> usize {
        self.rows
    }

    pub(crate) fn bytes(self) -> u64 {
        self.bytes
    }
}

impl Default for ScanTargets {
    fn default() -> Self {
        Self {
            rows: CHANGE_SCAN_TARGET_ROWS,
            bytes: COMMIT_CHANGES_MAX_BYTES,
        }
    }
}

/// One scanned row prepared for comparison. Holds a zero-copy one-row slice
/// (retaining `_rowid` and descriptor columns for lazy image and payload
/// access) plus per-Blob-column physical descriptor identities. No JSON image
/// and no Blob payload I/O happen here.
#[derive(Debug, Clone)]
pub(crate) struct RawRow {
    pub(crate) id: String,
    pub(crate) slice: RecordBatch,
    /// One [`BlobColumnSig`] per Blob column, in schema (name) order. Managed
    /// identities carry the owning data file's immutable UUID path (Blob
    /// descriptor fields are file-relative); the kind flag lets [`rows_equal`]
    /// decide when an equal identity is authoritative and when a payload
    /// byte-compare is required.
    blob_signatures: Vec<BlobColumnSig>,
}

impl RawRow {
    /// Build the typed comparison unit for one row of a scanned batch. The
    /// batch must carry `id` (and, for a Blob dataset, `_rowaddr`); Blob
    /// identities resolve from the in-memory dataset manifest, with no
    /// object-store I/O. Used by the branch-merge cursor so merge and the
    /// change surfaces classify rows through one comparator.
    pub(crate) fn single(
        dataset: &Dataset,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<RawRow> {
        let mut cursor = BatchCursor::try_new(dataset, batch.clone())?;
        cursor.next_row = row_index;
        cursor.next(dataset)?.ok_or_else(|| {
            OmniError::manifest_internal("single-row batch produced no comparison row")
        })
    }
}

#[derive(Debug)]
struct BlobBatchColumn {
    name: String,
    column_index: usize,
    field_id: i32,
}

/// One scanner batch plus the small amount of schema metadata needed to
/// prepare rows lazily. The batch itself is retained once; only the current
/// row is sliced and decorated. This avoids the former `VecDeque<RawRow>` of
/// up to 8,192 slices, ids, and Blob-signature vectors.
#[derive(Debug)]
struct BatchCursor {
    batch: RecordBatch,
    next_row: usize,
    id_index: usize,
    row_address_index: Option<usize>,
    blob_columns: Vec<BlobBatchColumn>,
}

impl BatchCursor {
    fn try_new(dataset: &Dataset, batch: RecordBatch) -> Result<Self> {
        let id_index = batch
            .schema_ref()
            .index_of("id")
            .map_err(|_| OmniError::manifest_internal("change row is missing string id"))?;
        batch
            .column(id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OmniError::manifest_internal("change row is missing string id"))?;

        let mut blob_columns = Vec::new();
        for (column_index, (field, column)) in batch
            .schema_ref()
            .fields()
            .iter()
            .zip(batch.columns())
            .enumerate()
        {
            if is_reserved_storage_system_column(field.name()) {
                continue;
            }
            let lance_field = lance::datatypes::Field::try_from(field.as_ref())
                .map_err(OmniError::lance_internal)?;
            if lance_field.is_blob() {
                let descriptions =
                    column
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            OmniError::blob_integrity(format!(
                                "expected blob descriptions for change column '{}'",
                                field.name()
                            ))
                        })?;
                // Validate the descriptor shape once when the batch arrives.
                BlobDescriptorDecoder::try_new(descriptions)?;
                let field_id = dataset.schema().field_id(field.name()).map_err(|error| {
                    OmniError::blob_integrity(format!(
                        "blob column '{}' has no field id: {error}",
                        field.name()
                    ))
                })?;
                blob_columns.push(BlobBatchColumn {
                    name: field.name().to_string(),
                    column_index,
                    field_id,
                });
            }
        }
        blob_columns.sort_by(|left, right| left.name.cmp(&right.name));

        let row_address_index = if blob_columns.is_empty() {
            None
        } else {
            let index = batch.schema_ref().index_of("_rowaddr").map_err(|_| {
                OmniError::manifest_internal(
                    "change scan is missing _rowaddr; managed Blob comparison needs the owning data file",
                )
            })?;
            batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "change scan is missing _rowaddr; managed Blob comparison needs the owning data file",
                    )
                })?;
            Some(index)
        };

        Ok(Self {
            batch,
            next_row: 0,
            id_index,
            row_address_index,
            blob_columns,
        })
    }

    fn next(&mut self, dataset: &Dataset) -> Result<Option<RawRow>> {
        if self.next_row >= self.batch.num_rows() {
            return Ok(None);
        }
        let row = self.next_row;
        let ids = self
            .batch
            .column(self.id_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id shape was validated when the batch arrived");
        let row_addresses = self.row_address_index.map(|index| {
            self.batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("row-address shape was validated when the batch arrived")
        });

        let mut blob_signatures = Vec::with_capacity(self.blob_columns.len());
        for blob_column in &self.blob_columns {
            let descriptions = self
                .batch
                .column(blob_column.column_index)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Blob descriptor shape was validated when the batch arrived");
            let decoder = BlobDescriptorDecoder::try_new(descriptions)?;
            let fragment_id = row_addresses
                .expect("Blob batches carry row addresses")
                .value(row)
                >> 32;
            let data_file_path =
                data_file_path_for_field(dataset.fragments(), fragment_id, blob_column.field_id)?;
            blob_signatures.push(BlobColumnSig {
                name: blob_column.name.clone(),
                identity: decoder.physical_identity(row, data_file_path)?,
                managed: matches!(decoder.classify(row)?, BlobDescriptor::Managed { .. }),
            });
        }

        self.next_row += 1;
        Ok(Some(RawRow {
            id: ids.value(row).to_string(),
            slice: self.batch.slice(row, 1),
            blob_signatures,
        }))
    }
}

/// An `id`-ordered stream of one dataset snapshot's rows, filled lazily one Lance
/// batch at a time. Both endpoints of a diff are walked in lockstep so the
/// merge stays streaming and never buffers a delta-wide row set.
pub(crate) struct OrderedRows {
    dataset: Dataset,
    stream: Option<Pin<Box<DatasetRecordBatchStream>>>,
    batch: Option<BatchCursor>,
    pending: Option<RawRow>,
}

impl OrderedRows {
    pub(crate) async fn open(dataset: Dataset, after_id: Option<&str>) -> Result<Self> {
        Self::open_scan(dataset, after_id, None, None, ScanTargets::default()).await
    }

    /// The full scan surface. `fragments` scopes the scan to exactly those
    /// physical fragments — the candidate path passes only the transaction's
    /// new or touched fragments, while the version-window `extra_filter` drops
    /// carried-over rows a fragment rewrite pulled along.
    pub(crate) async fn open_scan(
        dataset: Dataset,
        after_id: Option<&str>,
        extra_filter: Option<Expr>,
        fragments: Option<Vec<Fragment>>,
        targets: ScanTargets,
    ) -> Result<Self> {
        if fragments.as_ref().is_some_and(Vec::is_empty) {
            return Ok(Self {
                dataset,
                stream: None,
                batch: None,
                pending: None,
            });
        }
        let after_id = after_id.map(str::to_string);
        let stream = Box::pin(
            TableStore::scan_stream_with(
                &dataset,
                None,
                None,
                Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                true,
                move |scanner| {
                    if let Some(fragments) = fragments {
                        scanner.with_fragments(fragments);
                    }
                    let resume = after_id.map(|after_id| col("id").gt(lit(after_id)));
                    if let Some(filter) = match (resume, extra_filter) {
                        (Some(resume), Some(extra)) => Some(resume.and(extra)),
                        (Some(resume), None) => Some(resume),
                        (None, Some(extra)) => Some(extra),
                        (None, None) => None,
                    } {
                        scanner.filter_expr(filter);
                    }
                    // Descriptor-sized batches bounded by rows AND bytes, the
                    // same shape as every other production ordered-by-id scan.
                    // strict_batch_size is deliberately absent: Lance's strict
                    // stream coalesces to a row count the environment can
                    // override and ignores the byte target while accumulating.
                    scanner.batch_size(targets.rows);
                    scanner.batch_size_bytes(targets.bytes);
                    scanner.blob_handling(BlobHandling::BlobsDescriptions);
                    // Managed Blob descriptors are file-relative, so `BatchCursor`
                    // maps the row's fragment (high 32 bits of `_rowaddr`) to the
                    // owning data file's immutable UUID path — the qualifier that
                    // tells an unchanged row from a same-length Blob-only update
                    // (which lands in a new data file) across relocation,
                    // Overwrite, and branches. `with_row_id` above stays on for
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
            batch: None,
            pending: None,
        })
    }

    pub(crate) async fn peek(&mut self) -> Result<Option<&RawRow>> {
        self.fill().await?;
        Ok(self.pending.as_ref())
    }

    pub(crate) async fn pop(&mut self) -> Result<Option<RawRow>> {
        self.fill().await?;
        Ok(self.pending.take())
    }

    async fn fill(&mut self) -> Result<()> {
        while self.pending.is_none() {
            if let Some(batch) = self.batch.as_mut() {
                if let Some(row) = batch.next(&self.dataset)? {
                    self.pending = Some(row);
                    return Ok(());
                }
                self.batch = None;
            }
            let Some(stream) = self.stream.as_mut() else {
                return Ok(());
            };
            match stream.try_next().await {
                Ok(Some(batch)) => self.batch = Some(BatchCursor::try_new(&self.dataset, batch)?),
                Ok(None) => {
                    self.stream = None;
                    return Ok(());
                }
                Err(error) => return Err(TableStore::ordered_scan_error(error)),
            }
        }
        Ok(())
    }

    pub(crate) fn dataset(&self) -> &Dataset {
        &self.dataset
    }
}

/// Resolve the immutable data-file path that holds `field_id` in the fragment
/// owning a row — the stable UUID qualifier for a managed-Blob identity. Reads
/// only the in-memory manifest.
fn data_file_path_for_field(
    fragments: &[lance_table::format::Fragment],
    fragment_id: u64,
    field_id: i32,
) -> Result<&str> {
    let fragment = fragments
        .binary_search_by_key(&fragment_id, |fragment| fragment.id)
        .ok()
        .map(|index| &fragments[index])
        .ok_or_else(|| {
            OmniError::blob_integrity(format!(
                "change scan referenced fragment {fragment_id} absent from the manifest"
            ))
        })?;
    fragment
        .files
        .iter()
        .find(|file| file.fields.contains(&field_id))
        .map(|file| file.path.as_str())
        .ok_or_else(|| {
            OmniError::blob_integrity(format!(
                "fragment {fragment_id} has no data file for blob field {field_id}"
            ))
        })
}

/// Typed structural row equality with an exact managed-Blob payload tie-break.
///
/// Non-Blob user columns compare by Arrow logical equality on the two one-row
/// slices — typed, offset-aware, validity-aware, so null, `""`, and `[]` stay
/// distinct and no display-string join can conflate values.
///
/// Blob columns compare per descriptor kind:
/// - **External / null**: the physical identity is source-independent and fully
///   encodes the logical value (`uri`/`offset`/`length`, or null), so a
///   difference is authoritative — no payload I/O, and a same-URI range change
///   is never conflated by a bare-URI value compare. *(Ranged externals are not
///   yet writable through a supported path, so this branch is defense in depth.)*
/// - **Managed**: the identity is qualified by the owning data file's immutable
///   UUID path, which is globally unique (it does not restart on `Overwrite` and
///   is not branch-local). So an equal managed identity implies equal bytes even
///   across overwrites and branches — payload I/O is paid only on a descriptor
///   difference (which proves nothing on its own, since compaction relocates
///   identical bytes to a new file), where the byte-compare tie-break decides.
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
        .map(|sig| sig.name.as_str())
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

    // The two sides must agree on the Blob column set (name order); a shape
    // mismatch means the logical image necessarily changed shape.
    if left.blob_signatures.len() != right.blob_signatures.len()
        || left
            .blob_signatures
            .iter()
            .zip(&right.blob_signatures)
            .any(|(l, r)| l.name != r.name)
    {
        return Ok(false);
    }

    // Classify each Blob column. A managed column whose data-file-qualified
    // identity differs reaches the payload tie-break (the difference could be a
    // compaction relocation of identical bytes); an equal identity proves equal
    // bytes and is skipped. External/null identities are authoritative.
    let mut managed_to_bytecheck: HashSet<String> = HashSet::new();
    for (l, r) in left.blob_signatures.iter().zip(&right.blob_signatures) {
        if l.managed && r.managed {
            if l.identity != r.identity {
                managed_to_bytecheck.insert(l.name.clone());
            }
        } else if l.identity != r.identity {
            // At least one side is external or null: identity is authoritative
            // and source-independent, so a difference is a real change.
            return Ok(false);
        }
    }
    if managed_to_bytecheck.is_empty() {
        return Ok(true);
    }
    let left_values = blob_values_for(from_dataset, &left.slice, &managed_to_bytecheck).await?;
    let right_values = blob_values_for(to_dataset, &right.slice, &managed_to_bytecheck).await?;
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
        .ok_or_else(|| OmniError::manifest_internal("change row is missing _rowid"))?
        .value(0);
    export_blob_values(dataset, slice, &[row_id], columns).await
}
