//! Id-ordered walks over one pinned dataset that never sort payload columns.
//!
//! Every id-ordered walk over complete rows (branch merge, the change feed,
//! export) runs in two phases so no payload column reaches a SortExec input:
//!
//! 1. a narrow bounded ordered scan sorts only `id` + `_rowid` + `_rowaddr`
//!    (a few dozen bytes per row, far below the ordered-scan single-batch
//!    hard cap), then
//! 2. sorted keys are hydrated back into complete rows in bounded chunks via
//!    an unordered, fragment-scoped scan filtered to the chunk's exact
//!    `_rowaddr` set against the same pinned dataset. The scan's decode is
//!    byte-governed and streamed, every retained batch is compacted and
//!    hard-charged, and an over-budget chunk aborts and retries with half the
//!    rows, so the per-chunk memory bound holds for any row-width shape.
//!
//! Sorting complete rows instead trips the hard cap in two ways: a row wider
//! than the cap, and an ordinary row arriving as a one-row slice of a larger
//! decoded batch. Lance's byte-targeted scan slices without copying and sizes
//! each remaining slice by its whole parent buffer, so it keeps re-slicing
//! the tail down to single rows that the cap then measures at the parent's
//! size.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use datafusion::prelude::{Expr, col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use lance_table::format::Fragment;

use crate::error::{OmniError, Result};
use crate::storage_layer::{KEYED_WRITE_MAX_BYTES, KEYED_WRITE_MAX_ROWS};
use crate::table_store::TableStore;

/// Per-chunk decoded-byte planning target when hydrating sorted keys back
/// into complete logical rows. Planning uses the widest per-row average this
/// cursor has measured (decayed geometrically so one wide region does not
/// force single-row chunks over a later narrow tail) — but planning is an
/// efficiency knob only. The memory bound does not depend on it: chunk
/// hydration streams through a byte-governed scan and hard-charges every
/// retained batch, so an over-budget chunk is dropped mid-stream and retried
/// with half the rows regardless of how it was planned.
pub(crate) const HYDRATION_CHUNK_TARGET_BYTES: u64 = KEYED_WRITE_MAX_BYTES;
/// Hard retained-byte ceiling for one hydration chunk. Crossing it aborts
/// the chunk's scan and halves the row count (down to one row, which is
/// always accepted — a single indivisible row must hydrate whatever its
/// width; the staging writer's per-row envelope remains the authority for
/// what a merge may actually write). Peak resident hydration is therefore
/// bounded by this ceiling plus one in-flight scanner batch for every data
/// shape, including widths no sampling could have predicted.
pub(crate) const HYDRATION_CHUNK_HARD_BYTES: u64 = 2 * KEYED_WRITE_MAX_BYTES;
/// First-chunk row count before any width measurement exists.
pub(crate) const HYDRATION_CHUNK_SEED_ROWS: usize = 4;
/// Row and decoded-byte targets for the hydration scan's emitted batches.
/// Small batches make the hard charge granular: the accumulation check runs
/// per batch, so the one uncharged in-flight batch stays near this byte
/// target (an indivisible row still arrives as its own batch).
const HYDRATION_SCAN_BATCH_ROWS: usize = 1024;
pub(crate) const HYDRATION_SCAN_BATCH_BYTES: u64 = 8 * 1024 * 1024;

pub(crate) fn row_id_at(batch: &RecordBatch, row: usize, id_col: &str) -> Result<String> {
    let ids = batch
        .column_by_name(id_col)
        .ok_or_else(|| OmniError::manifest(format!("batch missing '{id_col}' column")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{id_col}' column is not Utf8")))?;
    Ok(ids.value(row).to_string())
}

/// The key filter of an ordered walk: a SQL predicate or a structured one.
pub(crate) enum KeyFilter<'a> {
    Sql(&'a str),
    Expr(Expr),
}

/// What an ordered walk reads and how coarsely it plans.
pub(crate) struct KeyOrder<'a> {
    /// Restrict the walk to exactly these physical fragments.
    pub(crate) fragments: Option<Vec<Fragment>>,
    pub(crate) filter: Option<KeyFilter<'a>>,
    /// Scanner targets for the narrow key scan.
    pub(crate) key_batch_rows: usize,
    pub(crate) key_batch_bytes: u64,
    /// Planning ceilings for one hydration chunk. The hard retained-byte
    /// ceiling applies regardless.
    pub(crate) chunk_rows: usize,
    pub(crate) chunk_bytes: u64,
}

impl KeyOrder<'_> {
    /// A full walk at the keyed-write envelope.
    pub(crate) fn full() -> Self {
        Self {
            fragments: None,
            filter: None,
            key_batch_rows: KEYED_WRITE_MAX_ROWS,
            key_batch_bytes: KEYED_WRITE_MAX_BYTES,
            chunk_rows: KEYED_WRITE_MAX_ROWS,
            chunk_bytes: HYDRATION_CHUNK_TARGET_BYTES,
        }
    }
}

/// Who walks, for error context: the operation, the table, the snapshot role.
#[derive(Debug, Clone)]
pub(crate) struct WalkSubject {
    pub(crate) operation: &'static str,
    pub(crate) table: String,
    pub(crate) role: &'static str,
}

/// One complete row in key order: a position in a hydrated batch. `chunk`
/// increments with every hydrated chunk, so `(chunk, batch_index)` names one
/// batch for a consumer that caches per-batch preparation.
#[derive(Debug, Clone)]
pub(crate) struct HydratedRow {
    pub(crate) id: String,
    pub(crate) batch: RecordBatch,
    pub(crate) row_index: usize,
    pub(crate) chunk: u64,
    pub(crate) batch_index: usize,
}

/// One hydrated chunk: the scanned batches plus the (batch, row) position of
/// every sorted key, so rows are emitted in key order without interleaving
/// or copying batch data.
struct HydratedChunk {
    batches: Vec<RecordBatch>,
    order: Vec<(usize, usize)>,
}

/// Outcome of one bounded chunk-scan attempt.
enum ChunkScan {
    Complete {
        chunk: HydratedChunk,
        bytes: u64,
    },
    OverBudget {
        retained_bytes: u64,
        retained_rows: usize,
    },
}

/// An id-ordered stream of one pinned dataset's complete rows; see the
/// module documentation. Blob columns arrive as descriptors and `_rowid` /
/// `_rowaddr` ride along on every hydrated row.
pub(crate) struct OrderedRowCursor {
    key_stream: Option<Pin<Box<DatasetRecordBatchStream>>>,
    dataset: Option<Dataset>,
    subject: WalkSubject,
    id_col: &'static str,
    chunk_rows: usize,
    chunk_bytes: u64,
    key_batch: Option<RecordBatch>,
    key_row: usize,
    hydrated: Option<HydratedChunk>,
    hydrated_pos: usize,
    chunks: u64,
    /// Widest measured per-row average, decayed by half at each observation
    /// so planning recovers geometrically after a wide region. Zero until the
    /// first measurement; the seed row count governs until then. Planning
    /// only — the retained-byte ceiling is enforced independently.
    max_row_bytes: u64,
}

impl OrderedRowCursor {
    /// Open a walk over `dataset` (none: an empty walk).
    pub(crate) async fn open(
        dataset: Option<Dataset>,
        order: KeyOrder<'_>,
        subject: WalkSubject,
        id_col: &'static str,
    ) -> Result<Self> {
        let KeyOrder {
            fragments,
            filter,
            key_batch_rows,
            key_batch_bytes,
            chunk_rows,
            chunk_bytes,
        } = order;
        let empty_scope = fragments.as_ref().is_some_and(Vec::is_empty);
        let key_stream = match &dataset {
            Some(ds) if !empty_scope => {
                // A filtered or fragment-scoped scan is not a full-table
                // scan; record no cursor-scan probe, so probe-count
                // assertions keep counting full walks only.
                if filter.is_none() && fragments.is_none() {
                    crate::instrumentation::record_ordered_cursor_scan(
                        key_batch_rows,
                        key_batch_bytes,
                    );
                }
                let (sql, expr) = match filter {
                    Some(KeyFilter::Sql(sql)) => (Some(sql), None),
                    Some(KeyFilter::Expr(expr)) => (None, Some(expr)),
                    None => (None, None),
                };
                Some(Box::pin(
                    TableStore::scan_stream_with(
                        ds,
                        Some(&[id_col]),
                        sql,
                        Some(vec![ColumnOrdering::asc_nulls_last(id_col.to_string())]),
                        true,
                        move |scanner| {
                            if let Some(fragments) = fragments {
                                scanner.with_fragments(fragments);
                            }
                            if let Some(expr) = expr {
                                scanner.filter_expr(expr);
                            }
                            scanner.batch_size(key_batch_rows);
                            scanner.batch_size_bytes(key_batch_bytes);
                            // `_rowaddr` addresses the hydration scan against
                            // the same pinned version; payload columns
                            // (including Blob descriptors) arrive only there.
                            scanner.with_row_address();
                            Ok(())
                        },
                    )
                    .await?,
                ))
            }
            _ => None,
        };

        Ok(Self {
            key_stream,
            dataset,
            subject,
            id_col,
            chunk_rows: chunk_rows.clamp(1, KEYED_WRITE_MAX_ROWS),
            chunk_bytes: chunk_bytes.clamp(1, HYDRATION_CHUNK_TARGET_BYTES),
            key_batch: None,
            key_row: 0,
            hydrated: None,
            hydrated_pos: 0,
            chunks: 0,
            max_row_bytes: 0,
        })
    }

    /// Attach the table and snapshot role this walk serves to a typed
    /// resource failure. The generic bounded ordered-scan executor cannot know
    /// which snapshot it was reading; this layer is the one that does.
    fn with_scan_context(&self, error: OmniError) -> OmniError {
        let WalkSubject { table, role, .. } = &self.subject;
        match error {
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } => OmniError::ResourceLimitExceeded {
                resource: format!("{resource} for {table} ({role} snapshot)"),
                limit,
                actual,
            },
            other => other,
        }
    }

    /// Like [`Self::with_scan_context`], additionally naming the sorted-key id
    /// range of the failing hydration. Storage and manifest failures — the
    /// shapes a hydration actually produces — keep their classification and
    /// gain the context in-message via `with_context`.
    fn with_hydration_context(&self, error: OmniError, first_id: &str, last_id: &str) -> OmniError {
        let WalkSubject {
            operation,
            table,
            role,
        } = &self.subject;
        match error {
            OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            } => OmniError::ResourceLimitExceeded {
                resource: format!(
                    "{resource} for {table} ({role} snapshot, rows '{first_id}'..='{last_id}')"
                ),
                limit,
                actual,
            },
            other => other.with_context(format!(
                "{operation} hydration for {table} ({role} snapshot, rows '{first_id}'..='{last_id}')"
            )),
        }
    }

    fn hydration_internal(&self, detail: impl std::fmt::Display) -> OmniError {
        OmniError::manifest_internal(format!(
            "ordered cursor hydration for {} ({} snapshot) {detail}",
            self.subject.table, self.subject.role
        ))
    }

    /// The next complete row in key order.
    pub(crate) async fn next(&mut self) -> Result<Option<HydratedRow>> {
        if !self.fill().await? {
            return Ok(None);
        }
        let chunk = self
            .hydrated
            .as_ref()
            .expect("fill leaves a chunk with rows remaining");
        let (batch_index, row_index) = chunk.order[self.hydrated_pos];
        self.hydrated_pos += 1;
        let batch = chunk.batches[batch_index].clone();
        Ok(Some(HydratedRow {
            id: row_id_at(&batch, row_index, self.id_col)?,
            batch,
            row_index,
            chunk: self.chunks,
            batch_index,
        }))
    }

    /// The rest of the current hydrated chunk, or the next one, as a single
    /// batch in key order. For a caller that consumes rows in bulk; it copies
    /// at most one chunk (the hard retained ceiling).
    pub(crate) async fn next_ordered_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.fill().await? {
            return Ok(None);
        }
        let chunk = self
            .hydrated
            .take()
            .expect("fill leaves a chunk with rows remaining");
        let order = &chunk.order[self.hydrated_pos..];
        self.hydrated_pos = 0;
        let mut offsets = Vec::with_capacity(chunk.batches.len());
        let mut total = 0u64;
        for batch in &chunk.batches {
            offsets.push(total);
            total += batch.num_rows() as u64;
        }
        let indices = UInt64Array::from_iter_values(
            order
                .iter()
                .map(|(batch, row)| offsets[*batch] + *row as u64),
        );
        let schema = chunk.batches[0].schema();
        let all = arrow_select::concat::concat_batches(&schema, &chunk.batches)
            .map_err(OmniError::arrow_internal)?;
        let ordered = arrow_select::take::take_record_batch(&all, &indices)
            .map_err(OmniError::arrow_internal)?;
        Ok(Some(ordered))
    }

    /// Make a hydrated chunk with rows remaining current; false at the end.
    async fn fill(&mut self) -> Result<bool> {
        loop {
            if let Some(chunk) = &self.hydrated {
                if self.hydrated_pos < chunk.order.len() {
                    return Ok(true);
                }
                self.hydrated = None;
                self.hydrated_pos = 0;
            }

            if let Some(keys) = &self.key_batch {
                if self.key_row < keys.num_rows() {
                    self.hydrate_next_chunk().await?;
                    continue;
                }
                self.key_batch = None;
                self.key_row = 0;
            }

            let Some(stream) = self.key_stream.as_mut() else {
                return Ok(false);
            };
            match stream.try_next().await {
                Ok(Some(batch)) => {
                    self.key_batch = Some(batch);
                    self.key_row = 0;
                }
                Ok(None) => {
                    self.key_stream = None;
                    return Ok(false);
                }
                Err(err) => {
                    return Err(self.with_scan_context(TableStore::ordered_scan_error(err)));
                }
            }
        }
    }

    /// Rows the next chunk may plan, from the pessimistic width estimate.
    /// Planning only: the retained-byte ceiling bounds memory regardless of
    /// this value; a good plan merely avoids abort-and-retry work.
    fn planned_chunk_rows(&self) -> usize {
        let planned = match self.chunk_bytes.checked_div(self.max_row_bytes) {
            // No width measured yet: the seed governs.
            None => HYDRATION_CHUNK_SEED_ROWS,
            Some(rows) => usize::try_from(rows)
                .unwrap_or(KEYED_WRITE_MAX_ROWS)
                .clamp(1, KEYED_WRITE_MAX_ROWS),
        };
        planned.min(self.chunk_rows)
    }

    /// Hydrate the next bounded chunk of sorted keys into complete rows.
    ///
    /// The chunk is read through an unordered, fragment-scoped scan filtered
    /// to exactly the chunk's `_rowaddr` set, so the decode is byte-governed
    /// and streamed. Every retained batch is hard-charged; crossing the
    /// retained ceiling drops the stream and retries with half the rows, so
    /// peak memory is the ceiling plus one in-flight batch for any row-width
    /// shape — no sample or estimate is load-bearing for the bound.
    async fn hydrate_next_chunk(&mut self) -> Result<()> {
        let dataset = self
            .dataset
            .clone()
            .ok_or_else(|| OmniError::manifest("cursor keys missing source dataset".to_string()))?;
        let keys = self
            .key_batch
            .clone()
            .ok_or_else(|| OmniError::manifest_internal("hydration without a key batch"))?;
        let start = self.key_row;
        let available = keys.num_rows().saturating_sub(start).max(1);
        let mut len = self.planned_chunk_rows().min(available);
        loop {
            match self.scan_chunk(&dataset, &keys, start, len).await? {
                ChunkScan::Complete { chunk, bytes } => {
                    crate::instrumentation::record_ordered_cursor_hydration(len, bytes);
                    // Fold the measured chunk into the planning estimate with
                    // decay: a wide region shrinks later plans immediately,
                    // while a narrow tail recovers geometrically instead of
                    // staying at single-row chunks forever.
                    let average = (bytes / len as u64).max(1);
                    self.max_row_bytes = average.max(self.max_row_bytes / 2);
                    self.key_row = start + len;
                    self.hydrated = Some(chunk);
                    self.hydrated_pos = 0;
                    self.chunks += 1;
                    return Ok(());
                }
                ChunkScan::OverBudget {
                    retained_bytes,
                    retained_rows,
                } => {
                    // Fold what was measured before the abort so both this
                    // retry and future planning shrink; nothing oversized was
                    // retained.
                    let average = (retained_bytes / retained_rows.max(1) as u64).max(1);
                    self.max_row_bytes = self.max_row_bytes.max(average);
                    len = (len / 2).max(1);
                }
            }
        }
    }

    /// One bounded chunk-scan attempt over `len` sorted keys from `start`.
    /// Blob columns come back as descriptors (the typed comparator's identity
    /// unit); `_rowid` and `_rowaddr` ride along for the comparator's Blob
    /// tie-break and fragment-identity mapping. Rows are emitted in scan
    /// order and mapped back to key order positionally — never copied.
    async fn scan_chunk(
        &mut self,
        dataset: &Dataset,
        keys: &RecordBatch,
        start: usize,
        len: usize,
    ) -> Result<ChunkScan> {
        let addresses_column = keys
            .column_by_name(lance_core::ROW_ADDR)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                OmniError::manifest_internal("ordered cursor key batch is missing row addresses")
            })?;
        let addresses: Vec<u64> = (start..start + len)
            .map(|row| addresses_column.value(row))
            .collect();
        // The chunk's sorted key range is the only stable row identity safely
        // available at this layer; carry it so an operator can find the
        // offending rows without replaying the walk.
        let first_id = row_id_at(keys, start, self.id_col)?;
        let last_id = row_id_at(keys, start + len - 1, self.id_col)?;

        let fragment_ids: HashSet<u64> = addresses.iter().map(|address| address >> 32).collect();
        let fragments: Vec<Fragment> = dataset
            .get_fragments()
            .into_iter()
            .filter(|fragment| fragment_ids.contains(&fragment.metadata().id))
            .map(|fragment| fragment.metadata().clone())
            .collect();
        if fragments.len() != fragment_ids.len() {
            return Err(self.hydration_internal("could not resolve every chunk fragment"));
        }
        let address_filter = col(lance_core::ROW_ADDR).in_list(
            addresses.iter().map(|address| lit(*address)).collect(),
            false,
        );

        let mut stream = TableStore::scan_stream_with(dataset, None, None, None, true, |scanner| {
            scanner.with_fragments(fragments);
            scanner.filter_expr(address_filter);
            scanner.batch_size(HYDRATION_SCAN_BATCH_ROWS);
            scanner.batch_size_bytes(HYDRATION_SCAN_BATCH_BYTES);
            // Blob columns must yield DESCRIPTORS (not payloads) for the
            // shared typed comparator's data-file identity.
            scanner.blob_handling(lance_core::datatypes::BlobHandling::BlobsDescriptions);
            scanner.with_row_address();
            Ok(())
        })
        .await
        .map_err(|error| self.with_hydration_context(error, &first_id, &last_id))?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut retained_bytes = 0u64;
        let mut retained_rows = 0usize;
        loop {
            match stream.try_next().await {
                Ok(Some(batch)) => {
                    // Compact before charging and retaining: scanned arrays
                    // can be slices of larger decode buffers, so an
                    // uncompacted batch both overcounts (shared parents) and
                    // pins those parent allocations for as long as the chunk
                    // is retained. The copy makes the retained-byte charge
                    // measure exactly the rows the chunk owns.
                    let indices = UInt64Array::from_iter_values(0..batch.num_rows() as u64);
                    let batch = arrow_select::take::take_record_batch(&batch, &indices)
                        .map_err(OmniError::arrow_internal)?;
                    retained_bytes = retained_bytes.saturating_add(
                        u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
                    );
                    retained_rows += batch.num_rows();
                    batches.push(batch);
                    // A single key must hydrate whatever its width — that one
                    // indivisible row is the only allowance above the ceiling.
                    if len > 1 && retained_bytes > HYDRATION_CHUNK_HARD_BYTES {
                        return Ok(ChunkScan::OverBudget {
                            retained_bytes,
                            retained_rows,
                        });
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    return Err(self.with_hydration_context(
                        TableStore::ordered_scan_error(error),
                        &first_id,
                        &last_id,
                    ));
                }
            }
        }

        // Map every sorted key to its scanned row; any mismatch means this
        // hydration cannot be trusted to feed the caller.
        let mut by_address: HashMap<u64, (usize, usize)> = HashMap::with_capacity(len);
        for (batch_index, batch) in batches.iter().enumerate() {
            let scanned = batch
                .column_by_name(lance_core::ROW_ADDR)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    OmniError::manifest_internal(
                        "ordered cursor hydration batch is missing row addresses",
                    )
                })?;
            for row in 0..batch.num_rows() {
                if by_address
                    .insert(scanned.value(row), (batch_index, row))
                    .is_some()
                {
                    return Err(self.hydration_internal("returned a duplicate row"));
                }
            }
        }
        if retained_rows != len {
            return Err(
                self.hydration_internal(format!("returned {retained_rows} rows for {len} keys"))
            );
        }
        let mut order = Vec::with_capacity(len);
        for (offset, address) in addresses.iter().enumerate() {
            let &(batch_index, row) = by_address
                .get(address)
                .ok_or_else(|| self.hydration_internal("is missing a requested row"))?;
            if row_id_at(&batches[batch_index], row, self.id_col)?
                != row_id_at(keys, start + offset, self.id_col)?
            {
                return Err(self.hydration_internal("returned a row out of key order"));
            }
            order.push((batch_index, row));
        }

        Ok(ChunkScan::Complete {
            chunk: HydratedChunk { batches, order },
            bytes: retained_bytes,
        })
    }
}
