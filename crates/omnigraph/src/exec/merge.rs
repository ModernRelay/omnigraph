use super::*;
use crate::changes::row_compare::{RawRow, rows_equal};
use crate::storage_layer::{
    KEYED_WRITE_MAX_BYTES, KEYED_WRITE_MAX_ROWS, KeyedWriteSemantics, ProvenInsertChunk,
};
use crate::table_store::certified_insert_absence_rows;

const MERGE_STAGE_DIR_ENV: &str = "OMNIGRAPH_MERGE_STAGING_DIR";
const DELETE_FILTER_PREFIX: &str = "id IN (";
const DELETE_FILTER_SEPARATOR: &str = ", ";
const DELETE_FILTER_SUFFIX: &str = ")";
/// The unified validator currently consumes one cross-table `ChangeSet`, so
/// projected scalar deltas must remain resident until evaluation completes.
/// Keep that operation-wide retained input bounded independently of the
/// per-transaction keyed-write chunks.
const MERGE_VALIDATION_MAX_BYTES: u64 = KEYED_WRITE_MAX_BYTES;
const MERGE_VALIDATION_RESOURCE: &str = "branch-merge retained validation delta bytes";
/// Bound transaction-history metadata work independently of recovery's effect
/// scan ceiling. This is an optimization budget, never a correctness limit.
const PURE_INSERT_HISTORY_MAX_VERSIONS: u64 = 1_024;

#[derive(Debug)]
enum CandidateTableState {
    /// Adopt the source's table state via a pointer switch or a branch fork —
    /// no data HEAD advance, so nothing to pin for recovery. `validation_delta`
    /// carries the source-vs-target row delta (added/changed/deleted) for the
    /// evaluator ONLY — the publish is still a pointer/fork — so a pointer-adopt
    /// whose source diverged is still validated (RI/uniqueness/cardinality)
    /// against the merged state instead of being silently published. `None` when
    /// the source matched the target (nothing to validate). Decoupling the
    /// validation delta from the publish mechanism keeps the publish O(1) while
    /// closing the unvalidated-adopt gap.
    AdoptSourceState {
        validation_delta: Option<AdoptDelta>,
    },
    /// The target table still equals the merge base and Lance's complete source
    /// transaction interval proves that every logical change was an exact-id
    /// fenced insert. The pinned source interval is scanned directly into the
    /// established row/byte-bounded exact-transaction chain; no base scan, row
    /// signatures, or temporary delta dataset is needed.
    AdoptPureInserts(ProvenPureInsertAdopt),
    /// Adopt the source's state by applying a non-empty delta onto the target's
    /// lineage (strict-insert new + upsert changed + delete removed). The delta is
    /// pre-computed at classification so this candidate can be recovery-pinned:
    /// its publish advances Lance HEAD before the manifest commit.
    AdoptWithDelta(AdoptDelta),
    RewriteMerged(StagedMergeResult),
}

/// An existing target ref opened and verified against the target manifest pin
/// under branch merge's final schema -> branch -> table gate envelope.
///
/// The handle is carried through Phase A and consumed by the physical effect so
/// the post-arm path cannot reopen a different HEAD. First-touch refs never
/// construct this value: their target ref is created only after the v9 recovery
/// envelope (whose retained merge payload field is `protocol_v4`)
/// sidecar is durable.
#[derive(Debug)]
struct PreparedExistingMergeTarget {
    current: SnapshotHandle,
    full_path: String,
    table_branch: Option<String>,
}

impl PreparedExistingMergeTarget {
    fn into_parts(self) -> (SnapshotHandle, String, Option<String>) {
        (self.current, self.full_path, self.table_branch)
    }
}

#[derive(Debug)]
struct StagedTable {
    _dir: TempDir,
    dataset: Dataset,
    row_count: u64,
    /// Exact row boundaries written by `StagedTableWriter`. Recovery planning
    /// consumes these, rather than assuming Lance's scanner will emit a
    /// particular physical batch shape.
    chunk_rows: Vec<usize>,
}

#[derive(Debug)]
struct StagedMergeResult {
    delta_staged: Option<StagedTable>,
    deleted_ids: DeleteIdChunks,
}

/// Exact delete-filter chunks retained from the ordered merge walk.
///
/// Each chunk is independently bounded by both the keyed-write row ceiling and
/// the exact UTF-8 byte length of the escaped `id IN (...)` filter handed to
/// Lance. The chunk boundary is therefore also the recovery boundary: publish
/// pre-mints and consumes one exact Lance transaction per chunk.
#[derive(Debug, Default)]
struct DeleteIdChunks {
    chunks: Vec<DeleteIdChunk>,
    /// Conservative retained-heap estimate for every owned id plus chunk/Vec
    /// bookkeeping. Per-chunk bounds alone would still permit a 1,024-chunk
    /// plan to retain tens of GiB before recovery is armed.
    retained_bytes: u64,
}

#[derive(Debug)]
struct DeleteIdChunk {
    ids: Vec<String>,
    filter_bytes: u64,
}

impl DeleteIdChunks {
    fn push(&mut self, id: String) -> Result<()> {
        self.push_bounded(id, KEYED_WRITE_MAX_ROWS, KEYED_WRITE_MAX_BYTES)
    }

    fn push_bounded(&mut self, id: String, max_rows: usize, max_bytes: u64) -> Result<()> {
        self.push_with_bounds(id, max_rows, max_bytes, KEYED_WRITE_MAX_BYTES)
    }

    fn push_with_bounds(
        &mut self,
        id: String,
        max_rows: usize,
        max_filter_bytes: u64,
        max_retained_bytes: u64,
    ) -> Result<()> {
        if max_rows == 0 {
            return Err(OmniError::manifest_internal(
                "branch merge delete chunk row bound is zero",
            ));
        }
        let literal_bytes = escaped_delete_literal_bytes(&id)?;
        let empty_filter_bytes = delete_filter_fixed_bytes()?;
        let first_filter_bytes = empty_filter_bytes
            .checked_add(literal_bytes)
            .ok_or_else(|| OmniError::manifest_internal("branch merge delete filter overflow"))?;
        if first_filter_bytes > max_filter_bytes {
            return Err(OmniError::resource_limit(
                "branch-merge delete filter bytes",
                max_filter_bytes,
                first_filter_bytes,
            ));
        }

        let separator_bytes = u64::try_from(DELETE_FILTER_SEPARATOR.len()).map_err(|_| {
            OmniError::manifest_internal("branch merge delete separator bytes exceed u64")
        })?;
        let starts_new_chunk = self.chunks.last().is_none_or(|chunk| {
            chunk.ids.len() >= max_rows
                || chunk
                    .filter_bytes
                    .checked_add(separator_bytes)
                    .and_then(|bytes| bytes.checked_add(literal_bytes))
                    .is_none_or(|bytes| bytes > max_filter_bytes)
        });
        let string_bookkeeping = u64::try_from(std::mem::size_of::<String>())
            .map_err(|_| OmniError::manifest_internal("String size exceeds u64"))?
            .checked_mul(2)
            .ok_or_else(|| {
                OmniError::manifest_internal("branch merge delete bookkeeping overflow")
            })?;
        let id_storage = u64::try_from(id.len())
            .map_err(|_| OmniError::manifest_internal("branch merge delete id exceeds u64"))?
            .checked_add(string_bookkeeping)
            .ok_or_else(|| {
                OmniError::manifest_internal("branch merge delete retained bytes overflow")
            })?;
        let chunk_storage = if starts_new_chunk {
            u64::try_from(std::mem::size_of::<DeleteIdChunk>())
                .map_err(|_| OmniError::manifest_internal("delete chunk size exceeds u64"))?
                .checked_mul(2)
                .ok_or_else(|| {
                    OmniError::manifest_internal("branch merge chunk bookkeeping overflow")
                })?
        } else {
            0
        };
        let retained_bytes = self
            .retained_bytes
            .checked_add(id_storage)
            .and_then(|bytes| bytes.checked_add(chunk_storage))
            .ok_or_else(|| {
                OmniError::manifest_internal("branch merge delete retained bytes overflow")
            })?;
        if retained_bytes > max_retained_bytes {
            return Err(OmniError::resource_limit(
                "branch-merge retained delete plan bytes",
                max_retained_bytes,
                retained_bytes,
            ));
        }
        if starts_new_chunk {
            self.chunks.push(DeleteIdChunk {
                ids: Vec::new(),
                filter_bytes: empty_filter_bytes,
            });
        }

        let chunk = self.chunks.last_mut().ok_or_else(|| {
            OmniError::manifest_internal("branch merge delete chunk was not created")
        })?;
        if !chunk.ids.is_empty() {
            chunk.filter_bytes =
                chunk
                    .filter_bytes
                    .checked_add(separator_bytes)
                    .ok_or_else(|| {
                        OmniError::manifest_internal("branch merge delete filter overflow")
                    })?;
        }
        chunk.filter_bytes = chunk
            .filter_bytes
            .checked_add(literal_bytes)
            .ok_or_else(|| OmniError::manifest_internal("branch merge delete filter overflow"))?;
        chunk.ids.push(id);
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    fn iter(&self) -> impl Iterator<Item = &String> {
        self.chunks.iter().flat_map(|chunk| chunk.ids.iter())
    }
}

impl DeleteIdChunk {
    fn filter(&self) -> Result<String> {
        if self.ids.is_empty() || self.ids.len() > KEYED_WRITE_MAX_ROWS {
            return Err(OmniError::manifest_internal(format!(
                "branch merge delete chunk contains {} ids",
                self.ids.len()
            )));
        }
        if self.filter_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::manifest_internal(format!(
                "branch merge delete chunk exceeds its armed byte bound: {} bytes",
                self.filter_bytes
            )));
        }
        let capacity = usize::try_from(self.filter_bytes).map_err(|_| {
            OmniError::manifest_internal("branch merge delete filter capacity exceeds usize")
        })?;
        let mut filter = String::with_capacity(capacity);
        filter.push_str(DELETE_FILTER_PREFIX);
        for (index, id) in self.ids.iter().enumerate() {
            if index > 0 {
                filter.push_str(DELETE_FILTER_SEPARATOR);
            }
            filter.push('\'');
            for character in id.chars() {
                if character == '\'' {
                    filter.push('\'');
                }
                filter.push(character);
            }
            filter.push('\'');
        }
        filter.push_str(DELETE_FILTER_SUFFIX);
        let actual_bytes = u64::try_from(filter.len()).map_err(|_| {
            OmniError::manifest_internal("branch merge delete filter bytes exceed u64")
        })?;
        if actual_bytes != self.filter_bytes {
            return Err(OmniError::manifest_internal(format!(
                "branch merge delete filter built {actual_bytes} bytes against a {}-byte plan",
                self.filter_bytes
            )));
        }
        Ok(filter)
    }
}

fn delete_filter_fixed_bytes() -> Result<u64> {
    u64::try_from(DELETE_FILTER_PREFIX.len() + DELETE_FILTER_SUFFIX.len())
        .map_err(|_| OmniError::manifest_internal("branch merge delete filter framing exceeds u64"))
}

fn escaped_delete_literal_bytes(id: &str) -> Result<u64> {
    let id_bytes = u64::try_from(id.len())
        .map_err(|_| OmniError::manifest_internal("branch merge delete id bytes exceed u64"))?;
    let escaped_quotes = u64::try_from(id.bytes().filter(|byte| *byte == b'\'').count())
        .map_err(|_| OmniError::manifest_internal("branch merge delete quote count exceeds u64"))?;
    id_bytes
        .checked_add(escaped_quotes)
        .and_then(|bytes| bytes.checked_add(2))
        .ok_or_else(|| OmniError::manifest_internal("branch merge delete literal overflow"))
}

/// Delta for an adopted-source merge (the fast-forward / target-owns path):
/// the new + changed rows to apply onto the target's base lineage, plus the ids
/// removed on source. Distinct from [`StagedMergeResult`] (the three-way path),
/// which also carries a `full_staged` table for validation — the adopt path
/// validates against the source snapshot directly (`candidate_dataset`), so it
/// needs no `full_staged` and never builds it.
///
/// TRANSITIONAL — fragment-adopt excision point. This whole row-level adopt
/// (`AdoptDelta`, [`compute_adopt_delta`], [`publish_adopted_delta`], and the
/// streaming append it drives) re-derives the source branch row-by-row because
/// today's Lance offers no fragment-level branch merge. When Lance ships
/// branch-merge/rebase ([#7263]) + UUID branch paths ([#7185]), a fast-forward
/// merge becomes a *fragment graft* — adopt the source table version's
/// fragments (and their already-built indexes) by reference, no rows scanned,
/// re-appended, upserted, or deleted. At that point this struct and its two
/// functions are removed wholesale; the merge collapses to ~one ref/metadata
/// op per table. Keep them self-contained so that excision stays a clean delete.
///
/// [#7263]: https://github.com/lance-format/lance/issues/7263
/// [#7185]: https://github.com/lance-format/lance/issues/7185
#[derive(Debug)]
struct AdoptDelta {
    /// New-on-source rows → a streaming strict exact-`id` fenced merge. The
    /// source stays batch-bounded while retaining `WhenMatched::Fail` semantics.
    inserts: Option<StagedTable>,
    /// Changed-on-source rows → a fenced upsert bounded to the
    /// genuinely-changed set, not the whole delta).
    upserts: Option<StagedTable>,
    deleted_ids: DeleteIdChunks,
}

/// A source interval whose complete Lance transaction history proves that its
/// only logical row changes are exact-id fenced inserts.
///
/// This is deliberately a proof object, not a heuristic.  Construction checks
/// the full `(base_version, source_version]` transaction chain, exact PK filter,
/// insertion-only `Operation::Update` shape, and physical row total.  If any
/// part is unavailable or ambiguous the caller keeps the existing row-diff
/// path.
#[derive(Debug)]
struct ProvenPureInsertAdopt {
    source: Dataset,
    source_identity: crate::db::manifest::TableIdentity,
    source_table_path: String,
    source_table_branch: Option<String>,
    source_branch_identifier: lance::dataset::refs::BranchIdentifier,
    /// Native incarnation of the merge-base/target ref on which the source
    /// interval's absence proof is founded. Existing targets recheck this
    /// under the final table gate before recovery is armed.
    target_base_branch_identifier: lance::dataset::refs::BranchIdentifier,
    base_version: u64,
    source_version: u64,
    inserted_rows: u64,
    /// Exact pre-arm row boundaries observed from the bounded source-interval
    /// stream. Each boundary becomes one pre-minted filtered transaction.
    chunk_rows: Vec<usize>,
}

#[derive(Debug, Clone)]
struct CursorRow {
    id: String,
    /// The typed comparison unit, precomputed when the cursor is eager (`None`
    /// on the lazy adopt path, built on demand). Compared through the shared
    /// `rows_equal` so merge and the change surfaces classify rows identically:
    /// Arrow-typed for scalars/nested (injective, unlike a display string) and
    /// data-file-qualified for Blob columns.
    typed: Option<RawRow>,
    dataset: Dataset,
    batch: RecordBatch,
    row_index: usize,
}

impl CursorRow {
    /// The typed comparison unit — cloned when eager, built on demand when lazy.
    fn typed(&self) -> Result<RawRow> {
        match &self.typed {
            Some(typed) => Ok(typed.clone()),
            None => RawRow::single(&self.dataset, &self.batch, self.row_index),
        }
    }

    /// Whether this row's logical value differs from `other`, via the shared
    /// typed comparator (injective for nested values; Blob columns by immutable
    /// data-file identity with a payload byte-compare only on a descriptor tie).
    async fn equals(&self, other: &CursorRow) -> Result<bool> {
        let this = self.typed()?;
        let that = other.typed()?;
        rows_equal(&self.dataset, &this, &other.dataset, &that).await
    }

    /// Classify this exact persisted row without opening an external target.
    /// Branch-merge planning uses it to build one operation-wide preflight
    /// before the bounded spill writer reads any selected payload.
    fn include_blob_selection(
        &self,
        selection: &mut crate::table_store::PersistedBlobSelection,
    ) -> Result<()> {
        selection.include_batch(&self.batch.slice(self.row_index, 1))
    }
}

/// Equality of two optional cursor rows: two absent rows are equal; present vs
/// absent differ; two present rows compare via the shared typed comparator.
async fn cursor_rows_equal(a: Option<&CursorRow>, b: Option<&CursorRow>) -> Result<bool> {
    match (a, b) {
        (None, None) => Ok(true),
        (Some(a), Some(b)) => a.equals(b).await,
        _ => Ok(false),
    }
}

struct OrderedTableCursor {
    stream: Option<std::pin::Pin<Box<DatasetRecordBatchStream>>>,
    dataset: Option<Dataset>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    peeked: Option<CursorRow>,
    /// When false, the adopt path builds the typed comparison unit only for
    /// common rows. New/deleted rows therefore avoid comparison work, while
    /// general three-way cursors retain their eager typed rows.
    eager_signatures: bool,
}

impl OrderedTableCursor {
    async fn from_snapshot(snapshot: &Snapshot, table_key: &str) -> Result<Self> {
        Self::open(snapshot, table_key, true).await
    }

    /// Like `from_snapshot` but builds typed rows lazily for adopt-only
    /// equality. See `eager_signatures`.
    async fn from_snapshot_lazy(snapshot: &Snapshot, table_key: &str) -> Result<Self> {
        Self::open(snapshot, table_key, false).await
    }

    async fn open(snapshot: &Snapshot, table_key: &str, eager_signatures: bool) -> Result<Self> {
        let dataset = match snapshot.dataset(table_key) {
            Some(_) => Some(snapshot.open_lance_dataset(table_key).await?),
            None => None,
        };
        Self::from_dataset(dataset, eager_signatures).await
    }

    async fn from_dataset(dataset: Option<Dataset>, eager_signatures: bool) -> Result<Self> {
        let stream = if let Some(ds) = &dataset {
            crate::instrumentation::record_ordered_cursor_scan(
                KEYED_WRITE_MAX_ROWS,
                KEYED_WRITE_MAX_BYTES,
            );
            Some(Box::pin(
                crate::table_store::TableStore::scan_stream_with(
                    ds,
                    None,
                    None,
                    Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                    true,
                    |scanner| {
                        scanner.batch_size(KEYED_WRITE_MAX_ROWS);
                        scanner.batch_size_bytes(KEYED_WRITE_MAX_BYTES);
                        // Blob columns must yield DESCRIPTORS (not payloads) for
                        // the shared typed comparator's data-file identity, and
                        // `_rowaddr` gives the owning fragment.
                        scanner
                            .blob_handling(lance_core::datatypes::BlobHandling::BlobsDescriptions);
                        scanner.with_row_address();
                        Ok(())
                    },
                )
                .await?,
            ))
        } else {
            None
        };

        Ok(Self {
            stream,
            dataset,
            current_batch: None,
            current_row: 0,
            peeked: None,
            eager_signatures,
        })
    }

    async fn peek_cloned(&mut self) -> Result<Option<CursorRow>> {
        if self.peeked.is_none() {
            self.peeked = self.next_row().await?;
        }
        Ok(self.peeked.clone())
    }

    async fn pop(&mut self) -> Result<Option<CursorRow>> {
        if self.peeked.is_some() {
            return Ok(self.peeked.take());
        }
        self.next_row().await
    }

    async fn next_row(&mut self) -> Result<Option<CursorRow>> {
        loop {
            if let Some(batch) = &self.current_batch {
                if self.current_row < batch.num_rows() {
                    let row_index = self.current_row;
                    self.current_row += 1;
                    let dataset = self.dataset.clone().ok_or_else(|| {
                        OmniError::manifest("cursor row missing source dataset".to_string())
                    })?;
                    let typed = if self.eager_signatures {
                        Some(RawRow::single(&dataset, batch, row_index)?)
                    } else {
                        None
                    };
                    return Ok(Some(CursorRow {
                        id: row_id_at(batch, row_index)?,
                        typed,
                        dataset,
                        batch: batch.clone(),
                        row_index,
                    }));
                }
            }

            let Some(stream) = self.stream.as_mut() else {
                return Ok(None);
            };
            match stream.try_next().await {
                Ok(Some(batch)) => {
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                Ok(None) => {
                    self.stream = None;
                    self.current_batch = None;
                    return Ok(None);
                }
                Err(err) => {
                    return Err(crate::table_store::TableStore::ordered_scan_error(err));
                }
            }
        }
    }
}

struct StagedTableWriter {
    schema: SchemaRef,
    materialize_blobs: bool,
    dataset_uri: String,
    dir: TempDir,
    dataset: Option<Dataset>,
    buffered_rows: usize,
    buffered_bytes: u64,
    row_count: u64,
    chunk_rows: Vec<usize>,
    batches: Vec<RecordBatch>,
    external_payloads: crate::table_store::ExternalBlobPayloadCache,
}

impl StagedTableWriter {
    fn new(table_key: &str, schema: SchemaRef) -> Result<Self> {
        let dir = merge_stage_tempdir(table_key)?;
        let dataset_uri = dir.path().join("table.lance").to_string_lossy().to_string();
        let materialize_blobs = schema_has_blob(&schema)?;
        Ok(Self {
            schema,
            materialize_blobs,
            dataset_uri,
            dir,
            dataset: None,
            buffered_rows: 0,
            buffered_bytes: 0,
            row_count: 0,
            chunk_rows: Vec::new(),
            batches: Vec::new(),
            external_payloads: crate::table_store::ExternalBlobPayloadCache::new(),
        })
    }

    async fn push_row(
        &mut self,
        row: &CursorRow,
        materializer: &crate::table_store::TableStore,
        external_preflight: &crate::table_store::ExternalBlobPreflight,
    ) -> Result<()> {
        // `RecordBatch::slice` would retain the complete scanner buffers.
        // Copy exactly one row before sizing or buffering it.
        let indices = UInt64Array::from(vec![row.row_index as u64]);
        let input = arrow_select::take::take_record_batch(&row.batch, &indices)
            .map_err(OmniError::arrow_internal)?;
        let predicted_row_bytes = if self.materialize_blobs {
            materializer.predicted_materialized_blob_batch_bytes(
                &row.dataset,
                &input,
                external_preflight,
                KEYED_WRITE_MAX_BYTES,
            )?
        } else {
            u64::try_from(input.get_array_memory_size()).map_err(|_| {
                OmniError::manifest_internal("branch merge row memory size exceeds u64")
            })?
        };
        if predicted_row_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                "branch-merge fenced entity bytes",
                KEYED_WRITE_MAX_BYTES,
                predicted_row_bytes,
            ));
        }
        let predicted_overflow = self
            .buffered_bytes
            .checked_add(predicted_row_bytes)
            .is_none_or(|bytes| bytes > KEYED_WRITE_MAX_BYTES);
        if self.buffered_rows > 0
            && (self.buffered_rows >= KEYED_WRITE_MAX_ROWS || predicted_overflow)
        {
            // Flush before payload I/O, not after. Otherwise two individually
            // valid rows can transiently retain more than the chunk ceiling.
            self.flush().await?;
        }
        let batch = self
            .row_batch(row, input, materializer, external_preflight)
            .await?;
        let row_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("branch merge row memory size exceeds u64")
        })?;
        if row_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                "branch-merge fenced entity bytes",
                KEYED_WRITE_MAX_BYTES,
                row_bytes,
            ));
        }
        let would_exceed_bytes = self
            .buffered_bytes
            .checked_add(row_bytes)
            .is_none_or(|bytes| bytes > KEYED_WRITE_MAX_BYTES);
        if self.buffered_rows > 0
            && (self.buffered_rows >= KEYED_WRITE_MAX_ROWS || would_exceed_bytes)
        {
            self.flush().await?;
        }
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| OmniError::manifest_internal("branch merge row count overflow"))?;
        self.buffered_rows += 1;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| OmniError::manifest_internal("branch merge byte count overflow"))?;
        self.batches.push(batch);
        if self.buffered_rows >= KEYED_WRITE_MAX_ROWS
            || self.buffered_bytes >= KEYED_WRITE_MAX_BYTES
        {
            self.flush().await?;
        }
        Ok(())
    }

    async fn row_batch(
        &mut self,
        row: &CursorRow,
        batch: RecordBatch,
        materializer: &crate::table_store::TableStore,
        external_preflight: &crate::table_store::ExternalBlobPreflight,
    ) -> Result<RecordBatch> {
        if self.materialize_blobs {
            return materializer
                .materialize_blob_batch_bounded_with_preflight_cache(
                    &row.dataset,
                    batch,
                    KEYED_WRITE_MAX_BYTES,
                    external_preflight,
                    &mut self.external_payloads,
                )
                .await;
        }
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                    OmniError::manifest_internal(format!("batch missing column '{}'", field.name()))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(OmniError::arrow_internal)
    }

    async fn finish(mut self) -> Result<StagedTable> {
        self.flush().await?;
        if self.dataset.is_none() {
            self.dataset = Some(
                crate::table_store::TableStore::create_empty_dataset(
                    &self.dataset_uri,
                    &self.schema,
                )
                .await?,
            );
        }
        Ok(StagedTable {
            _dir: self.dir,
            dataset: self.dataset.unwrap(),
            row_count: self.row_count,
            chunk_rows: self.chunk_rows,
        })
    }

    async fn flush(&mut self) -> Result<()> {
        if self.batches.is_empty() {
            return Ok(());
        }

        let batch = if self.batches.len() == 1 {
            self.batches.pop().unwrap()
        } else {
            let batches = std::mem::take(&mut self.batches);
            arrow_select::concat::concat_batches(&self.schema, &batches)
                .map_err(OmniError::arrow_internal)?
        };
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.chunk_rows.push(batch.num_rows());
        let chunk_count = u64::try_from(self.chunk_rows.len())
            .map_err(|_| OmniError::manifest_internal("branch merge chunk count exceeds u64"))?;
        if chunk_count > crate::db::manifest::MAX_BRANCH_MERGE_DATA_TRANSACTIONS {
            return Err(OmniError::resource_limit(
                "branch-merge recovery transaction chain",
                crate::db::manifest::MAX_BRANCH_MERGE_DATA_TRANSACTIONS,
                chunk_count,
            ));
        }

        let ds = crate::table_store::TableStore::append_or_create_batch(
            &self.dataset_uri,
            self.dataset.take(),
            batch,
        )
        .await?;
        self.dataset = Some(ds);
        self.external_payloads.clear();
        Ok(())
    }
}

fn merge_stage_tempdir(table_key: &str) -> Result<TempDir> {
    if let Ok(root) = env::var(MERGE_STAGE_DIR_ENV) {
        return TempDirBuilder::new()
            .prefix(&format!(
                "omnigraph-merge-{}-",
                sanitize_table_key(table_key)
            ))
            .tempdir_in(PathBuf::from(root))
            .map_err(OmniError::from);
    }
    TempDirBuilder::new()
        .prefix(&format!(
            "omnigraph-merge-{}-",
            sanitize_table_key(table_key)
        ))
        .tempdir()
        .map_err(OmniError::from)
}

fn sanitize_table_key(table_key: &str) -> String {
    table_key
        .chars()
        .map(|ch| match ch {
            ':' | '/' | '\\' => '-',
            other => other,
        })
        .collect()
}

/// Try to prove that `(base, source]` contains only exact-id fenced inserts.
///
/// Missing/cleaned transaction files and every unfamiliar operation are a
/// normal miss: the caller falls back to the general ordered row diff.  Once a
/// proof is returned, however, later scan/count mismatches fail closed rather
/// than silently changing routes after recovery planning. Chunk planning is a
/// separate pass: Blob tables must first add only this proven interval's
/// descriptors to the operation-wide admission plan, then materialize rows
/// with that shared preflight rather than opening external sources here.
async fn try_proven_pure_insert_history(
    table_key: &str,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
) -> Result<Option<ProvenPureInsertAdopt>> {
    let Some(base_entry) = base_snapshot.dataset(table_key) else {
        return Ok(None);
    };
    let Some(source_entry) = source_snapshot.dataset(table_key) else {
        return Ok(None);
    };
    if source_entry.identity != base_entry.identity
        || source_entry.dataset_path != base_entry.dataset_path
    {
        return Ok(None);
    }
    let Some(version_count) = source_entry
        .published_dataset_version
        .checked_sub(base_entry.published_dataset_version)
        .filter(|count| *count > 0)
    else {
        return Ok(None);
    };
    let Some(inserted_rows) = source_entry
        .entity_count
        .checked_sub(base_entry.entity_count)
        .filter(|rows| *rows > 0)
    else {
        return Ok(None);
    };
    if version_count > PURE_INSERT_HISTORY_MAX_VERSIONS {
        return Ok(None);
    }

    let history_start = std::time::Instant::now();
    let base = base_snapshot.open_lance_dataset(table_key).await?;
    let source = source_snapshot.open_lance_dataset(table_key).await?;
    if source.version().version != source_entry.published_dataset_version
        || base.version().version != base_entry.published_dataset_version
        || !source.manifest.uses_stable_row_ids()
        || !base.manifest.uses_stable_row_ids()
    {
        return Ok(None);
    }
    let base_identifier = base.branch_identifier().await.map_err(OmniError::storage)?;
    let source_identifier = source
        .branch_identifier()
        .await
        .map_err(OmniError::storage)?;
    if base_entry.native_dataset_branch == source_entry.native_dataset_branch
        && source_identifier != base_identifier
    {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_merge_source_dataset_incarnation:{table_key}"),
            Some(format!("{base_identifier:?}")),
            Some(format!("{source_identifier:?}")),
        ));
    }
    if base_entry.native_dataset_branch != source_entry.native_dataset_branch
        && source_identifier.find_referenced_version(&base_identifier)
            != Some(base_entry.published_dataset_version)
    {
        tracing::debug!(
            table_key,
            ?base_identifier,
            ?source_identifier,
            "source table is not a provable descendant at the merge base; using general branch-merge diff"
        );
        return Ok(None);
    }
    let primary_key = source.schema().unenforced_primary_key();
    if primary_key.len() != 1
        || primary_key[0].name != "id"
        || primary_key[0].nullable
        || primary_key[0].data_type() != arrow_schema::DataType::Utf8
    {
        return Ok(None);
    }
    let id_field_id = primary_key[0].id;
    let Some(expected_schema_preorder_ids) = source
        .schema()
        .fields_pre_order()
        .map(|field| u32::try_from(field.id).ok())
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let delta = match source
        .delta()
        .with_begin_version(base_entry.published_dataset_version)
        .with_end_version(source_entry.published_dataset_version)
        .build()
    {
        Ok(delta) => delta,
        Err(error) => {
            tracing::debug!(
                table_key,
                error = %error,
                "pure-insert provenance unavailable; using general branch-merge diff"
            );
            return Ok(None);
        }
    };
    let transactions = match delta.list_transactions().await {
        Ok(transactions) => transactions,
        Err(error) => {
            // Cleanup may legitimately have removed an old transaction or
            // manifest in this interval.  The full row diff remains correct.
            tracing::debug!(
                table_key,
                error = %error,
                "pure-insert transaction history unavailable; using general branch-merge diff"
            );
            return Ok(None);
        }
    };
    if u64::try_from(transactions.len()).ok() != Some(version_count) {
        return Ok(None);
    }

    let mut proven_inserted_rows = 0_u64;
    for (offset, transaction) in transactions.iter().enumerate() {
        let expected_read_version = base_entry
            .published_dataset_version
            .checked_add(u64::try_from(offset).map_err(|_| {
                OmniError::manifest_internal(
                    "branch merge pure-insert transaction offset exceeds u64",
                )
            })?)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "branch merge pure-insert transaction version overflow",
                )
            })?;
        let Some(transaction_rows) = certified_insert_absence_rows(
            transaction,
            expected_read_version,
            id_field_id,
            &expected_schema_preorder_ids,
        ) else {
            return Ok(None);
        };
        proven_inserted_rows = proven_inserted_rows
            .checked_add(transaction_rows)
            .ok_or_else(|| {
                OmniError::manifest_internal("branch merge pure-insert row count overflow")
            })?;
    }
    if proven_inserted_rows != inserted_rows {
        return Ok(None);
    }
    crate::instrumentation::record_merge_timing(
        crate::instrumentation::MergeTimingPhase::ProvenInsertHistory,
        history_start.elapsed(),
    );

    Ok(Some(ProvenPureInsertAdopt {
        source,
        source_identity: source_entry.identity,
        source_table_path: source_entry.dataset_path.clone(),
        source_table_branch: source_entry.native_dataset_branch.clone(),
        source_branch_identifier: source_identifier,
        target_base_branch_identifier: base_identifier,
        base_version: base_entry.published_dataset_version,
        source_version: source_entry.published_dataset_version,
        inserted_rows,
        chunk_rows: Vec::new(),
    }))
}

async fn finalize_proven_pure_insert_adopt(
    db: &Omnigraph,
    table_key: &str,
    mut proven: ProvenPureInsertAdopt,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
) -> Result<Option<ProvenPureInsertAdopt>> {
    let Some(chunk_rows) = plan_proven_pure_insert_chunks(
        db,
        table_key,
        &proven.source,
        proven.base_version,
        proven.source_version,
        proven.inserted_rows,
        external_preflight,
    )
    .await?
    else {
        return Ok(None);
    };
    proven.chunk_rows = chunk_rows;
    Ok(Some(proven))
}

async fn try_proven_pure_insert_adopt(
    db: &Omnigraph,
    table_key: &str,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
) -> Result<Option<ProvenPureInsertAdopt>> {
    let Some(proven) =
        try_proven_pure_insert_history(table_key, base_snapshot, source_snapshot).await?
    else {
        return Ok(None);
    };
    let empty_external_preflight = crate::table_store::ExternalBlobPreflight::default();
    finalize_proven_pure_insert_adopt(db, table_key, proven, &empty_external_preflight).await
}

#[cfg(test)]
mod pure_insert_certificate_tests {
    use crate::table_store::{
        INSERT_ABSENCE_PROPERTY, INSERT_ABSENCE_V1, certified_insert_absence_rows,
    };
    use lance::dataset::transaction::{Operation, Transaction, UpdateMode};
    use lance::dataset::write::merge_insert::inserted_rows::{KeyExistenceFilterBuilder, KeyValue};
    use lance_table::format::Fragment;
    use std::collections::HashMap;
    use std::sync::Arc;

    const READ_VERSION: u64 = 7;
    const ID_FIELD: i32 = 3;
    const PREORDER_FIELDS: &[u32] = &[3, 4, 5];

    fn certified_transaction() -> Transaction {
        let mut fragment = Fragment::new(0);
        fragment.physical_rows = Some(1);
        let mut filter = KeyExistenceFilterBuilder::new(vec![ID_FIELD]);
        filter
            .insert(KeyValue::String("row-1".to_string()))
            .unwrap();
        Transaction {
            read_version: READ_VERSION,
            uuid: "certified-transaction".to_string(),
            operation: Operation::Update {
                removed_fragment_ids: Vec::new(),
                updated_fragments: Vec::new(),
                new_fragments: vec![fragment],
                fields_modified: Vec::new(),
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: PREORDER_FIELDS.to_vec(),
                update_mode: Some(UpdateMode::RewriteRows),
                inserted_rows_filter: Some(filter.build()),
                updated_fragment_offsets: None,
            },
            tag: None,
            transaction_properties: Some(Arc::new(HashMap::from([(
                INSERT_ABSENCE_PROPERTY.to_string(),
                INSERT_ABSENCE_V1.to_string(),
            )]))),
        }
    }

    fn accepted(transaction: &Transaction) -> Option<u64> {
        certified_insert_absence_rows(transaction, READ_VERSION, ID_FIELD, PREORDER_FIELDS)
    }

    #[test]
    fn certificate_requires_exact_versioned_property_and_complete_insert_shape() {
        let valid = certified_transaction();
        assert_eq!(accepted(&valid), Some(1));

        let mut missing = valid.clone();
        missing.transaction_properties = None;
        assert_eq!(accepted(&missing), None);

        let mut unknown = valid.clone();
        Arc::make_mut(unknown.transaction_properties.as_mut().unwrap())
            .insert(INSERT_ABSENCE_PROPERTY.to_string(), "v2".to_string());
        assert_eq!(accepted(&unknown), None);

        let mut wrong_parent = valid.clone();
        wrong_parent.read_version += 1;
        assert_eq!(accepted(&wrong_parent), None);

        let mut wrong_fields = valid.clone();
        if let Operation::Update {
            fields_for_preserving_frag_bitmap,
            ..
        } = &mut wrong_fields.operation
        {
            fields_for_preserving_frag_bitmap.pop();
        }
        assert_eq!(accepted(&wrong_fields), None);

        let mut wrong_filter = valid.clone();
        if let Operation::Update {
            inserted_rows_filter: Some(filter),
            ..
        } = &mut wrong_filter.operation
        {
            filter.field_ids = vec![ID_FIELD + 1];
        }
        assert_eq!(accepted(&wrong_filter), None);

        let mut missing_filter = valid.clone();
        if let Operation::Update {
            inserted_rows_filter,
            ..
        } = &mut missing_filter.operation
        {
            *inserted_rows_filter = None;
        }
        assert_eq!(accepted(&missing_filter), None);

        let mut wrong_mode = valid.clone();
        if let Operation::Update { update_mode, .. } = &mut wrong_mode.operation {
            *update_mode = Some(UpdateMode::RewriteColumns);
        }
        assert_eq!(accepted(&wrong_mode), None);

        let mut offsets = valid.clone();
        if let Operation::Update {
            updated_fragment_offsets,
            ..
        } = &mut offsets.operation
        {
            *updated_fragment_offsets = Some(Default::default());
        }
        assert_eq!(accepted(&offsets), None);

        let mut rewrite = valid.clone();
        if let Operation::Update {
            removed_fragment_ids,
            ..
        } = &mut rewrite.operation
        {
            removed_fragment_ids.push(0);
        }
        assert_eq!(accepted(&rewrite), None);

        let mut missing_rows = valid.clone();
        if let Operation::Update { new_fragments, .. } = &mut missing_rows.operation {
            new_fragments[0].physical_rows = None;
        }
        assert_eq!(accepted(&missing_rows), None);

        let mut append = valid;
        append.operation = Operation::Append {
            fragments: Vec::new(),
        };
        assert_eq!(accepted(&append), None);
    }
}

/// Observe the exact row boundaries emitted by the bounded source-interval
/// scanner before recovery is armed. The later physical pass reconstructs
/// these same writer-defined chunks even if Lance chooses different physical
/// batch boundaries, so recovery identities never depend on scanner layout.
/// If the optimization would exceed the existing recovery-chain ceiling, it
/// is a normal miss and the caller retains the general ordered-diff route.
async fn plan_proven_pure_insert_chunks(
    db: &Omnigraph,
    table_key: &str,
    source: &Dataset,
    begin_version: u64,
    end_version: u64,
    expected_rows: u64,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
) -> Result<Option<Vec<usize>>> {
    let scan_start = std::time::Instant::now();
    let source = SnapshotHandle::new(source.clone());
    let mut stream = db
        .storage()
        .scan_proven_insert_delta_bounded(
            &source,
            table_key,
            begin_version,
            end_version,
            external_preflight,
        )
        .await?;
    let mut chunk_rows = Vec::new();
    let mut observed_rows = 0_u64;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::datafusion)? {
        if batch.num_rows() == 0 {
            continue;
        }
        observed_rows = observed_rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| {
                OmniError::manifest_internal("branch merge pure-insert plan row count overflow")
            })?;
        chunk_rows.push(batch.num_rows());
        if chunk_rows.len() > crate::db::manifest::MAX_BRANCH_MERGE_DATA_TRANSACTIONS as usize {
            return Ok(None);
        }
    }
    crate::instrumentation::record_merge_timing(
        crate::instrumentation::MergeTimingPhase::ProvenInsertPlanScan,
        scan_start.elapsed(),
    );
    if observed_rows != expected_rows || chunk_rows.is_empty() {
        return Err(OmniError::manifest_internal(format!(
            "branch merge pure-insert plan for '{table_key}' observed {observed_rows} rows in {} chunks against a provenance proof for {expected_rows}",
            chunk_rows.len()
        )));
    }
    Ok(Some(chunk_rows))
}

/// Recheck the exact native source-table ref under branch merge's final table
/// gates.  The graph ref may have advanced after capture (the pinned source
/// version remains a valid immutable input), but delete/recreate ABA or an
/// unmanifested physical HEAD must not be accepted under the same branch name.
async fn revalidate_proven_pure_insert_source(
    db: &Omnigraph,
    table_key: &str,
    proven: &ProvenPureInsertAdopt,
    live_source: &Snapshot,
) -> Result<()> {
    let Some(live_entry) = live_source.dataset(table_key) else {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_merge_source_dataset:{table_key}"),
            Some(format!(
                "{}@{}",
                proven.source_table_path, proven.source_version
            )),
            None,
        ));
    };
    if live_entry.identity != proven.source_identity
        || live_entry.dataset_path != proven.source_table_path
        || live_entry.native_dataset_branch != proven.source_table_branch
        || live_entry.published_dataset_version < proven.source_version
    {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_merge_source_dataset:{table_key}"),
            Some(format!(
                "{:?}:{}:{:?}@{}",
                proven.source_identity,
                proven.source_table_path,
                proven.source_table_branch,
                proven.source_version
            )),
            Some(format!(
                "{:?}:{}:{:?}@{}",
                live_entry.identity,
                live_entry.dataset_path,
                live_entry.native_dataset_branch,
                live_entry.published_dataset_version
            )),
        ));
    }

    let full_path = db.storage().dataset_uri(&live_entry.dataset_path);
    let head = db
        .storage()
        .open_dataset_head(&full_path, live_entry.native_dataset_branch.as_deref())
        .await?;
    let live_identifier = db.storage().branch_identifier(&head).await?;
    if live_identifier != proven.source_branch_identifier
        || head.version() != live_entry.published_dataset_version
    {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_merge_source_published_dataset_version:{table_key}"),
            Some(format!(
                "{:?}@{}",
                proven.source_branch_identifier, live_entry.published_dataset_version
            )),
            Some(format!("{live_identifier:?}@{}", head.version())),
        ));
    }
    Ok(())
}

/// Bind the no-target-probe proof to the exact native target incarnation.
///
/// Numeric Lance versions can repeat after a branch ref is deleted and
/// recreated. The source history proves absence relative to one native base
/// ref, so accepting a different ref with the same path/version would make the
/// induction unsound. The prepared handle was freshly opened under the final
/// schema -> branch -> table gate envelope; the ordinary baseline check below
/// separately proves that its live HEAD still equals the manifest pin.
async fn revalidate_proven_pure_insert_target_incarnation(
    db: &Omnigraph,
    table_key: &str,
    proven: &ProvenPureInsertAdopt,
    current: &SnapshotHandle,
) -> Result<()> {
    let live_identifier = db.storage().branch_identifier(current).await?;
    if live_identifier != proven.target_base_branch_identifier {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_merge_target_dataset_incarnation:{table_key}"),
            Some(format!("{:?}", proven.target_base_branch_identifier)),
            Some(format!("{live_identifier:?}")),
        ));
    }
    Ok(())
}

/// Computes the delta between base and source for an adopted-source merge.
/// Returns the new + changed rows and the ids deleted on source.
///
/// Unchanged rows are dropped: the adopt path validates against the source
/// snapshot directly (`candidate_dataset`), so no `full_staged` table is built
/// — saving the O(rows) temp write that `compute_source_delta` used to produce
/// and then discard.
///
/// TRANSITIONAL — removed by the fragment-adopt work (see [`AdoptDelta`]): a
/// fragment graft adopts the source's fragments by reference, so there is no
/// row-level delta to compute.
async fn compute_adopt_delta(
    target_db: &Omnigraph,
    table_key: &str,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    materialize_blobs: bool,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
) -> Result<Option<AdoptDelta>> {
    let full_schema = schema_for_table_key(catalog, table_key)?;
    let schema = if materialize_blobs {
        full_schema
    } else {
        validation_schema(catalog, table_key, &full_schema)?
    };
    let materializer = target_db.blob_materializer();
    let mut append_writer =
        StagedTableWriter::new(&format!("{}_adopt_append", table_key), schema.clone())?;
    let mut upsert_writer = StagedTableWriter::new(&format!("{}_adopt_upsert", table_key), schema)?;
    let mut deleted_ids = DeleteIdChunks::default();
    let mut base = OrderedTableCursor::from_snapshot_lazy(base_snapshot, table_key).await?;
    let mut source = OrderedTableCursor::from_snapshot_lazy(source_snapshot, table_key).await?;

    let mut needs_update = false;

    loop {
        let base_row = base.peek_cloned().await?;
        let source_row = source.peek_cloned().await?;

        let next_id = [base_row.as_ref(), source_row.as_ref()]
            .into_iter()
            .flatten()
            .map(|row| row.id.clone())
            .min();
        let Some(next_id) = next_id else { break };

        let base_row = if base_row.as_ref().map(|r| r.id.as_str()) == Some(next_id.as_str()) {
            base.pop().await?
        } else {
            None
        };
        let source_row = if source_row.as_ref().map(|r| r.id.as_str()) == Some(next_id.as_str()) {
            source.pop().await?
        } else {
            None
        };

        match (&base_row, &source_row) {
            (Some(_), None) => {
                // Deleted on source
                deleted_ids.push(next_id)?;
                needs_update = true;
            }
            (None, Some(src)) => {
                // New on source → strict fenced insert. No comparison unit is
                // needed — a new id is absent from base by construction.
                append_writer
                    .push_row(src, &materializer, external_preflight)
                    .await?;
                needs_update = true;
            }
            (Some(base), Some(src)) => {
                // Present on both — compare lazily (the only case that needs it)
                // to tell a changed row from an unchanged one. New/deleted rows
                // above skip building the typed unit entirely.
                if !src.equals(base).await? {
                    // Changed on source → upsert.
                    upsert_writer
                        .push_row(src, &materializer, external_preflight)
                        .await?;
                    needs_update = true;
                }
                // else unchanged — already on the target's base lineage; drop.
            }
            (None, None) => unreachable!(),
        }
    }

    if !needs_update {
        return Ok(None);
    }

    let inserts = if append_writer.row_count > 0 {
        Some(append_writer.finish().await?)
    } else {
        None
    };
    let upserts = if upsert_writer.row_count > 0 {
        Some(upsert_writer.finish().await?)
    } else {
        None
    };

    Ok(Some(AdoptDelta {
        inserts,
        upserts,
        deleted_ids,
    }))
}

/// First pass for a Blob-bearing adopt. It selects exactly the rows the
/// bounded delta writer will copy, collecting only their external descriptors.
/// The second pass can then share one normalized HEAD proof across every
/// spill chunk without retaining an unbounded set of source rows in memory.
async fn collect_adopt_blob_selection(
    table_key: &str,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    blob_selection: &mut crate::table_store::PersistedBlobSelection,
) -> Result<()> {
    let mut base = OrderedTableCursor::from_snapshot_lazy(base_snapshot, table_key).await?;
    let mut source = OrderedTableCursor::from_snapshot_lazy(source_snapshot, table_key).await?;

    loop {
        let base_row = base.peek_cloned().await?;
        let source_row = source.peek_cloned().await?;
        let next_id = [base_row.as_ref(), source_row.as_ref()]
            .into_iter()
            .flatten()
            .map(|row| row.id.clone())
            .min();
        let Some(next_id) = next_id else { break };

        let base_row = if base_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str()) {
            base.pop().await?
        } else {
            None
        };
        let source_row = if source_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str())
        {
            source.pop().await?
        } else {
            None
        };

        match (&base_row, &source_row) {
            (Some(_), None) => {}
            (None, Some(source)) => {
                source.include_blob_selection(blob_selection)?;
            }
            (Some(base), Some(source)) => {
                if !source.equals(base).await? {
                    source.include_blob_selection(blob_selection)?;
                }
            }
            (None, None) => unreachable!(),
        }
    }
    Ok(())
}

fn min_cursor_id(
    base_row: &Option<CursorRow>,
    source_row: &Option<CursorRow>,
    target_row: &Option<CursorRow>,
) -> Option<String> {
    [base_row.as_ref(), source_row.as_ref(), target_row.as_ref()]
        .into_iter()
        .flatten()
        .map(|row| row.id.clone())
        .min()
}

async fn stage_streaming_table_merge(
    target_db: &Omnigraph,
    table_key: &str,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    conflicts: &mut Vec<MergeConflict>,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
) -> Result<Option<StagedMergeResult>> {
    let schema = schema_for_table_key(catalog, table_key)?;
    let prior_conflict_count = conflicts.len();
    let materializer = target_db.blob_materializer();
    let mut delta_writer = StagedTableWriter::new(&format!("{}_delta", table_key), schema)?;
    let mut deleted_ids = DeleteIdChunks::default();
    let mut base = OrderedTableCursor::from_snapshot(base_snapshot, table_key).await?;
    let mut source = OrderedTableCursor::from_snapshot(source_snapshot, table_key).await?;
    let mut target = OrderedTableCursor::from_snapshot(target_snapshot, table_key).await?;

    let mut needs_update = false;

    loop {
        let base_row = base.peek_cloned().await?;
        let source_row = source.peek_cloned().await?;
        let target_row = target.peek_cloned().await?;
        let Some(next_id) = min_cursor_id(&base_row, &source_row, &target_row) else {
            break;
        };

        let base_row = if base_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str()) {
            base.pop().await?
        } else {
            None
        };
        let source_row = if source_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str())
        {
            source.pop().await?
        } else {
            None
        };
        let target_row = if target_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str())
        {
            target.pop().await?
        } else {
            None
        };

        let source_changed = !cursor_rows_equal(source_row.as_ref(), base_row.as_ref()).await?;
        let target_changed = !cursor_rows_equal(target_row.as_ref(), base_row.as_ref()).await?;

        let selection = if !source_changed {
            target_row.as_ref()
        } else if !target_changed {
            source_row.as_ref()
        } else if cursor_rows_equal(source_row.as_ref(), target_row.as_ref()).await? {
            // Both sides changed but agree — no conflict; target already has it.
            target_row.as_ref()
        } else {
            conflicts.push(classify_merge_conflict(
                table_key,
                &next_id,
                base_row.is_some(),
                source_row.is_some(),
                target_row.is_some(),
            ));
            None
        };

        if conflicts.len() > prior_conflict_count {
            continue;
        }

        // Row existed in target but not in merge result → delete
        if selection.is_none() && target_row.is_some() {
            deleted_ids.push(next_id.clone())?;
            needs_update = true;
            continue;
        }

        if let Some(selection) = selection {
            // Only changed rows go to the delta (for publish). The full merged
            // table is no longer staged — validation works off this delta plus
            // the committed target via index lookups, not a full re-scan.
            if !cursor_rows_equal(Some(selection), target_row.as_ref()).await? {
                delta_writer
                    .push_row(selection, &materializer, external_preflight)
                    .await?;
                needs_update = true;
            }
        }
    }

    if conflicts.len() > prior_conflict_count {
        return Ok(None);
    }
    if !needs_update {
        return Ok(None);
    }

    let delta_staged = if delta_writer.row_count > 0 {
        Some(delta_writer.finish().await?)
    } else {
        None
    };

    Ok(Some(StagedMergeResult {
        delta_staged,
        deleted_ids,
    }))
}

/// First pass for a Blob-bearing three-way merge. It repeats the immutable
/// row decision without spilling data, collecting descriptors only for rows
/// that differ from the target and will be copied into the merge delta.
async fn collect_three_way_blob_selection(
    table_key: &str,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    conflicts: &mut Vec<MergeConflict>,
    blob_selection: &mut crate::table_store::PersistedBlobSelection,
) -> Result<()> {
    let prior_conflict_count = conflicts.len();
    let mut base = OrderedTableCursor::from_snapshot(base_snapshot, table_key).await?;
    let mut source = OrderedTableCursor::from_snapshot(source_snapshot, table_key).await?;
    let mut target = OrderedTableCursor::from_snapshot(target_snapshot, table_key).await?;

    loop {
        let base_row = base.peek_cloned().await?;
        let source_row = source.peek_cloned().await?;
        let target_row = target.peek_cloned().await?;
        let Some(next_id) = min_cursor_id(&base_row, &source_row, &target_row) else {
            break;
        };
        let base_row = if base_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str()) {
            base.pop().await?
        } else {
            None
        };
        let source_row = if source_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str())
        {
            source.pop().await?
        } else {
            None
        };
        let target_row = if target_row.as_ref().map(|row| row.id.as_str()) == Some(next_id.as_str())
        {
            target.pop().await?
        } else {
            None
        };

        let source_changed = !cursor_rows_equal(source_row.as_ref(), base_row.as_ref()).await?;
        let target_changed = !cursor_rows_equal(target_row.as_ref(), base_row.as_ref()).await?;
        let selection = if !source_changed {
            target_row.as_ref()
        } else if !target_changed {
            source_row.as_ref()
        } else if cursor_rows_equal(source_row.as_ref(), target_row.as_ref()).await? {
            target_row.as_ref()
        } else {
            conflicts.push(classify_merge_conflict(
                table_key,
                &next_id,
                base_row.is_some(),
                source_row.is_some(),
                target_row.is_some(),
            ));
            None
        };

        if conflicts.len() > prior_conflict_count {
            continue;
        }
        if let Some(selection) = selection {
            if !cursor_rows_equal(Some(selection), target_row.as_ref()).await? {
                selection.include_blob_selection(blob_selection)?;
            }
        }
    }

    Ok(())
}

fn schema_for_table_key(catalog: &Catalog, table_key: &str) -> Result<SchemaRef> {
    if let Some(name) = table_key.strip_prefix("node:") {
        return catalog
            .node_types
            .get(name)
            .map(|t| t.arrow_schema.clone())
            .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", name)));
    }
    if let Some(name) = table_key.strip_prefix("edge:") {
        return catalog
            .edge_types
            .get(name)
            .map(|t| t.arrow_schema.clone())
            .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", name)));
    }
    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

fn schema_has_blob(schema: &SchemaRef) -> Result<bool> {
    for field in schema.fields() {
        if matches!(field.data_type(), arrow_schema::DataType::LargeBinary) {
            // The compiler deliberately uses LargeBinary as its dependency-free
            // logical Blob placeholder.
            return Ok(true);
        }
        let lance_field =
            lance::datatypes::Field::try_from(field.as_ref()).map_err(OmniError::storage)?;
        if lance_field.is_blob() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Pointer-only adoption needs scalar rows solely for graph constraint
/// validation. It must not dereference or rewrite heavy values because the
/// publication preserves the source table pointer exactly.
fn validation_schema(
    catalog: &Catalog,
    table_key: &str,
    full_schema: &SchemaRef,
) -> Result<SchemaRef> {
    let fields = validation_projection(catalog, table_key)
        .into_iter()
        .map(|name| {
            full_schema.field_with_name(&name).cloned().map_err(|error| {
                OmniError::manifest_internal(format!(
                    "branch merge validation schema for '{table_key}' is missing '{name}': {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(arrow_schema::Schema::new(fields)))
}

fn same_manifest_state(
    left: Option<&crate::db::DatasetEntry>,
    right: Option<&crate::db::DatasetEntry>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            left.identity == right.identity
                && left.dataset_path == right.dataset_path
                && left.published_dataset_version == right.published_dataset_version
                && left.native_dataset_branch == right.native_dataset_branch
                && left.entity_count == right.entity_count
                && left.version_metadata == right.version_metadata
        }
        (None, None) => true,
        _ => false,
    }
}

fn ensure_merge_identity_compatible(
    table_key: &str,
    base: Option<crate::db::manifest::TableIdentity>,
    source: Option<crate::db::manifest::TableIdentity>,
    target: Option<crate::db::manifest::TableIdentity>,
) -> Result<()> {
    let mut observed = None;
    for candidate_identity in [base, source, target].into_iter().flatten() {
        if let Some(observed_identity) = observed {
            if candidate_identity != observed_identity {
                return Err(OmniError::manifest_read_set_changed(
                    format!("branch_merge_dataset_identity:{table_key}"),
                    Some(observed_identity.to_string()),
                    Some(candidate_identity.to_string()),
                ));
            }
        } else {
            observed = Some(candidate_identity);
        }
    }
    Ok(())
}

#[cfg(test)]
mod table_identity_tests {
    use super::ensure_merge_identity_compatible;
    use crate::db::manifest::TableIdentity;

    #[test]
    fn merge_identity_guard_includes_base_and_allows_absence() {
        let old = TableIdentity::new(1, 1).unwrap();
        let replacement = TableIdentity::new(2, 2).unwrap();

        let error = ensure_merge_identity_compatible(
            "node:Person",
            Some(old),
            Some(replacement),
            Some(replacement),
        )
        .unwrap_err();
        assert!(error.is_read_set_changed());

        assert!(
            ensure_merge_identity_compatible("node:Person", Some(old), None, Some(old)).is_ok(),
            "absence is a row-lifecycle change, not a different table lifetime"
        );
        assert!(
            ensure_merge_identity_compatible(
                "node:Person",
                None,
                Some(replacement),
                Some(replacement),
            )
            .is_ok()
        );
    }
}

fn classify_merge_conflict(
    table_key: &str,
    row_id: &str,
    base_present: bool,
    source_present: bool,
    target_present: bool,
) -> MergeConflict {
    let (kind, message) = match (base_present, source_present, target_present) {
        (false, true, true) => (
            MergeConflictKind::DivergentInsert,
            format!("divergent insert for id '{}'", row_id),
        ),
        (true, false, true) | (true, true, false) => (
            MergeConflictKind::DeleteVsUpdate,
            format!("delete/update conflict for id '{}'", row_id),
        ),
        _ => (
            MergeConflictKind::DivergentUpdate,
            format!("divergent update for id '{}'", row_id),
        ),
    };
    MergeConflict {
        type_key: table_key.to_string(),
        entity_id: Some(row_id.to_string()),
        kind,
        message,
    }
}

/// Operation-wide budget for the scalar delta retained by merge validation.
///
/// Physical merge staging is chunked, but the unified validator evaluates one
/// cross-table `ChangeSet`. Without a second aggregate ceiling, many valid
/// chunks (or many candidate tables) could be collected back into an unbounded
/// resident delta before recovery was armed.
#[derive(Debug)]
struct MergeValidationBudget {
    retained_bytes: u64,
    limit: u64,
}

impl Default for MergeValidationBudget {
    fn default() -> Self {
        Self {
            retained_bytes: 0,
            limit: MERGE_VALIDATION_MAX_BYTES,
        }
    }
}

impl MergeValidationBudget {
    fn reserve(&mut self, bytes: u64) -> Result<()> {
        let actual = self.retained_bytes.checked_add(bytes).ok_or_else(|| {
            OmniError::manifest_internal("branch merge validation byte count overflow")
        })?;
        if actual > self.limit {
            return Err(OmniError::resource_limit(
                MERGE_VALIDATION_RESOURCE,
                self.limit,
                actual,
            ));
        }
        self.retained_bytes = actual;
        Ok(())
    }

    /// Charge the exact projected Arrow allocation before the batch is pushed
    /// into the retained `ChangeSet`.
    fn reserve_projected_batch(&mut self, batch: &RecordBatch) -> Result<u64> {
        let bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("branch merge validation batch bytes exceed u64")
        })?;
        self.reserve(bytes)?;
        Ok(bytes)
    }

    /// Deleted IDs are cloned from the already-bounded physical delete plan
    /// into the validator's logical delta. Charge their owned UTF-8 payload and
    /// `String` slot so delete-only candidates cannot bypass the aggregate cap.
    fn reserve_deleted_id(&mut self, id: &str) -> Result<()> {
        let payload = u64::try_from(id.len()).map_err(|_| {
            OmniError::manifest_internal("branch merge validation id bytes exceed u64")
        })?;
        let slot = u64::try_from(std::mem::size_of::<String>()).map_err(|_| {
            OmniError::manifest_internal("branch merge validation String size exceeds u64")
        })?;
        self.reserve(payload.checked_add(slot).ok_or_else(|| {
            OmniError::manifest_internal("branch merge validation id byte count overflow")
        })?)
    }
}

fn retain_deleted_ids_for_validation(
    source: &DeleteIdChunks,
    budget: &mut MergeValidationBudget,
    output: &mut Vec<String>,
) -> Result<()> {
    for id in source.iter() {
        budget.reserve_deleted_id(id)?;
        output.push(id.clone());
    }
    Ok(())
}

/// Build the per-table [`ChangeSet`](crate::validate::ChangeSet) for a merge from
/// the classified candidates — the new/changed rows (from the staged deltas) and
/// removed ids the validator evaluates, instead of re-scanning whole tables.
/// `AdoptSourceState` is published as a pointer/fork but still carries a
/// `validation_delta` (the source-vs-target rows) when its source diverged, so
/// it is validated like `AdoptWithDelta`; only an empty-delta adopt is skipped.
/// `AdoptPureInserts` projects the proven source interval directly, avoiding a
/// temporary delta table while retaining the same constraint evaluation.
async fn build_merge_changeset(
    db: &Omnigraph,
    catalog: &Catalog,
    candidates: &HashMap<String, CandidateTableState>,
) -> Result<crate::validate::ChangeSet> {
    let mut changeset = crate::validate::ChangeSet::new();
    let mut validation_budget = MergeValidationBudget::default();
    // The shared-budget failure and the validator's observable traversal must
    // not depend on HashMap iteration order.
    let mut ordered_table_keys = candidates.keys().collect::<Vec<_>>();
    ordered_table_keys.sort();
    for table_key in ordered_table_keys {
        let candidate = candidates.get(table_key).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "branch merge validation candidate '{table_key}' disappeared"
            ))
        })?;
        // Validation reads only id/src/dst + scalar constraint columns; project
        // out Vector/Blob so the change-set never holds embeddings (holding the
        // delta with embeddings would re-introduce the memory pressure the
        // streaming append exists to avoid).
        let projection = validation_projection(catalog, table_key);
        let projection: Vec<&str> = projection.iter().map(String::as_str).collect();
        let mut change = crate::validate::TableChange::default();
        match candidate {
            // Pointer/fork adopt whose source matched the target: nothing to
            // validate. A pointer/fork adopt whose source diverged carries a
            // `validation_delta` and is validated exactly like `AdoptWithDelta`
            // (only the publish differs — pointer vs HEAD-advancing).
            CandidateTableState::AdoptSourceState {
                validation_delta: None,
            } => continue,
            CandidateTableState::AdoptSourceState {
                validation_delta: Some(delta),
            }
            | CandidateTableState::AdoptWithDelta(delta) => {
                if let Some(table) = &delta.inserts {
                    scan_staged_for_validation(
                        db,
                        table,
                        &projection,
                        &mut validation_budget,
                        &mut change.added,
                    )
                    .await?;
                }
                if let Some(table) = &delta.upserts {
                    scan_staged_for_validation(
                        db,
                        table,
                        &projection,
                        &mut validation_budget,
                        &mut change.changed,
                    )
                    .await?;
                }
                retain_deleted_ids_for_validation(
                    &delta.deleted_ids,
                    &mut validation_budget,
                    &mut change.deleted_ids,
                )?;
            }
            CandidateTableState::AdoptPureInserts(proven) => {
                scan_proven_pure_inserts_for_validation(
                    db,
                    table_key,
                    proven,
                    &projection,
                    &mut validation_budget,
                    &mut change.added,
                )
                .await?;
            }
            CandidateTableState::RewriteMerged(staged) => {
                if let Some(table) = &staged.delta_staged {
                    scan_staged_for_validation(
                        db,
                        table,
                        &projection,
                        &mut validation_budget,
                        &mut change.changed,
                    )
                    .await?;
                }
                retain_deleted_ids_for_validation(
                    &staged.deleted_ids,
                    &mut validation_budget,
                    &mut change.deleted_ids,
                )?;
            }
        }
        changeset.insert(table_key.as_str().to_string(), change);
    }
    Ok(changeset)
}

/// Columns validation needs from a staged delta: `id` (+ `src`/`dst` for edges)
/// plus scalar/enum property columns. Vector and Blob columns are excluded — no
/// constraint reads them, and keeping them out of the change-set keeps validation
/// memory bounded regardless of embedding width.
fn validation_projection(catalog: &Catalog, table_key: &str) -> Vec<String> {
    use omnigraph_compiler::types::{PropType, ScalarType};
    let is_heavy = |ty: &PropType| matches!(ty.scalar, ScalarType::Vector(_) | ScalarType::Blob);
    let mut cols = vec!["id".to_string()];
    if let Some(name) = table_key.strip_prefix("node:") {
        if let Some(node_type) = catalog.node_types.get(name) {
            for (prop, ty) in &node_type.properties {
                if !is_heavy(ty) {
                    cols.push(prop.clone());
                }
            }
        }
    } else if let Some(name) = table_key.strip_prefix("edge:") {
        cols.push("src".to_string());
        cols.push("dst".to_string());
        if let Some(edge_type) = catalog.edge_types.get(name) {
            for (prop, ty) in &edge_type.properties {
                if !is_heavy(ty) {
                    cols.push(prop.clone());
                }
            }
        }
    }
    cols
}

/// Scan a staged delta table for validation, projected to the constraint columns
/// (no embeddings) and kept batch-shaped — never concatenated into one batch, so
/// it does not reintroduce the whole-delta materialization the streaming append
/// avoids. Empty batches are dropped.
async fn scan_staged_for_validation(
    db: &Omnigraph,
    table: &StagedTable,
    projection: &[&str],
    budget: &mut MergeValidationBudget,
    output: &mut Vec<RecordBatch>,
) -> Result<()> {
    let snapshot = SnapshotHandle::new(table.dataset.clone());
    let mut stream = db
        .storage()
        .scan_stream_bounded(
            &snapshot,
            Some(projection),
            None,
            None,
            false,
            KEYED_WRITE_MAX_ROWS,
            KEYED_WRITE_MAX_BYTES,
        )
        .await?;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        if batch.num_rows() == 0 {
            continue;
        }
        let projected_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("branch merge validation batch bytes exceed u64")
        })?;
        crate::instrumentation::record_merge_validation_batch(projected_bytes);
        // Charge before `output.push`: rejection never retains the batch that
        // crosses the shared operation-wide ceiling.
        budget.reserve_projected_batch(&batch)?;
        output.push(batch);
    }
    Ok(())
}

/// Project the source rows proven to have been created in `(base, source]`
/// directly into the merge validator.  This is a narrow scalar scan (vectors
/// and blobs are projected out), while the later physical publish performs one
/// independent wide stream into Lance's native filtered merge transaction.
async fn scan_proven_pure_inserts_for_validation(
    db: &Omnigraph,
    table_key: &str,
    proven: &ProvenPureInsertAdopt,
    projection: &[&str],
    budget: &mut MergeValidationBudget,
    output: &mut Vec<RecordBatch>,
) -> Result<()> {
    let filter = format!(
        "_row_created_at_version > {} AND _row_created_at_version <= {}",
        proven.base_version, proven.source_version
    );
    let source = SnapshotHandle::new(proven.source.clone());
    let mut stream = db
        .storage()
        .scan_stream_bounded(
            &source,
            Some(projection),
            Some(&filter),
            None,
            false,
            KEYED_WRITE_MAX_ROWS,
            KEYED_WRITE_MAX_BYTES,
        )
        .await?;
    let mut observed_rows = 0_u64;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        if batch.num_rows() == 0 {
            continue;
        }
        let batch_bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            OmniError::manifest_internal("branch merge validation batch bytes exceed u64")
        })?;
        if batch.num_rows() > KEYED_WRITE_MAX_ROWS {
            return Err(OmniError::resource_limit(
                format!("branch-merge pure-insert validation batch entities for {table_key}"),
                KEYED_WRITE_MAX_ROWS as u64,
                batch.num_rows() as u64,
            ));
        }
        if batch_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                format!("branch-merge pure-insert validation batch bytes for {table_key}"),
                KEYED_WRITE_MAX_BYTES,
                batch_bytes,
            ));
        }
        observed_rows = observed_rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| {
                OmniError::manifest_internal(
                    "branch merge pure-insert validation row count overflow",
                )
            })?;
        crate::instrumentation::record_merge_validation_batch(batch_bytes);
        budget.reserve_projected_batch(&batch)?;
        output.push(batch);
    }
    if observed_rows != proven.inserted_rows {
        return Err(OmniError::manifest_internal(format!(
            "branch merge pure-insert validation for '{table_key}' observed {observed_rows} rows against a provenance proof for {}",
            proven.inserted_rows
        )));
    }
    Ok(())
}

async fn validate_merge_candidates(
    catalog: &Catalog,
    target_snapshot: &Snapshot,
    changeset: &crate::validate::ChangeSet,
) -> Result<()> {
    // Δ-scoped, index-backed validation: the declared constraints are evaluated
    // over the merge delta against the committed target (queried through its
    // BTREE indexes), not by re-scanning every catalog table. Value/enum,
    // uniqueness, edge-RI, and cardinality all route through one evaluator shared
    // with (eventually) the write path — closing the merge-vs-write drift.
    let committed = crate::validate::CommittedState::merge(target_snapshot);
    let constraints = crate::validate::constraints_for(catalog);
    let violations =
        crate::validate::evaluate(&constraints, changeset, &committed, catalog).await?;
    if violations.is_empty() {
        Ok(())
    } else {
        Err(OmniError::MergeConflicts(
            violations
                .into_iter()
                .map(crate::validate::Violation::into_merge_conflict)
                .collect(),
        ))
    }
}

/// Whether exact pure-insert provenance already discharges every logical check
/// that would otherwise require materializing a fast-forward `ChangeSet`.
///
/// This deliberately recognizes only node tables with identity-backed `@key`
/// semantics and no additional value, enum, or non-key uniqueness constraint.
/// Edge candidates retain validation because RI/cardinality are cross-table;
/// every unfamiliar candidate shape also retains the general evaluator. The
/// source rows were accepted under the same schema identity, and strict exact-
/// `id` publication rechecks their only remaining target interaction.
fn proven_fast_forward_needs_no_validation(
    catalog: &Catalog,
    candidates: &HashMap<String, CandidateTableState>,
) -> bool {
    !candidates.is_empty()
        && candidates.iter().all(|(table_key, candidate)| {
            if !matches!(candidate, CandidateTableState::AdoptPureInserts(_)) {
                return false;
            }
            let Some(type_name) = table_key.strip_prefix("node:") else {
                return false;
            };
            let Some(node_type) = catalog.node_types.get(type_name) else {
                return false;
            };
            node_type.range_constraints.is_empty()
                && node_type.check_constraints.is_empty()
                && node_type.unique_constraints.is_empty()
                && node_type
                    .properties
                    .values()
                    .all(|property| property.enum_values.is_none())
        })
}

fn row_id_at(batch: &RecordBatch, row: usize) -> Result<String> {
    let ids = batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("batch missing id column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("id column is not Utf8".to_string()))?;
    Ok(ids.value(row).to_string())
}

fn adopt_advances_head(
    target_active: Option<&str>,
    source_entry: &crate::db::DatasetEntry,
    target_entry: Option<&crate::db::DatasetEntry>,
) -> bool {
    match (target_active, source_entry.native_dataset_branch.as_deref()) {
        // Source on a branch, target on main — delta applied onto main's lineage.
        (None, Some(_)) => true,
        // Both on branches, target owns this table — delta applied onto it.
        (Some(target_branch), Some(_)) => {
            target_entry.and_then(|entry| entry.native_dataset_branch.as_deref())
                == Some(target_branch)
        }
        // Source on main (pointer switch) or target doesn't own (fork): no advance.
        _ => false,
    }
}

/// Classify a table whose target state equals base (the adopt / fast-forward
/// case). A proven insertion-only descendant becomes
/// [`CandidateTableState::AdoptPureInserts`]; every other non-empty delta that
/// advances target HEAD becomes [`CandidateTableState::AdoptWithDelta`] with
/// its write payload pre-computed for recovery planning. Pointer switches and
/// forks become [`CandidateTableState::AdoptSourceState`] and do not advance
/// data HEAD.
///
/// The HEAD-advancing subcases mirror [`publish_adopted_source_state`]: source
/// on a branch with the target either on main or owning the table. Computing the
/// delta here (rather than inside the publish) is what closes the recovery gap —
/// the classifier knows whether the publish will move Lance HEAD.
async fn classify_adopt(
    target_db: &Omnigraph,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    table_key: &str,
    target_active: Option<&str>,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
) -> Result<Option<CandidateTableState>> {
    let Some(source_entry) = source_snapshot.dataset(table_key) else {
        // Source has no such table — nothing to adopt or validate.
        return Ok(Some(CandidateTableState::AdoptSourceState {
            validation_delta: None,
        }));
    };
    let target_entry = target_snapshot.dataset(table_key);
    ensure_merge_identity_compatible(
        table_key,
        base_snapshot.dataset(table_key).map(|entry| entry.identity),
        Some(source_entry.identity),
        target_entry.map(|entry| entry.identity),
    )?;
    let advances_head = adopt_advances_head(target_active, source_entry, target_entry);
    // A complete Lance transaction interval can prove the common all-new-row
    // case without re-scanning and sorting base + source.  The proof is accepted
    // only for a HEAD-advancing publish; pointer/fork adoption still uses the
    // general delta as its validation input.
    if advances_head
        && let Some(proven) =
            try_proven_pure_insert_adopt(target_db, table_key, base_snapshot, source_snapshot)
                .await?
    {
        return Ok(Some(CandidateTableState::AdoptPureInserts(proven)));
    }

    let candidate = classify_general_adopt(
        target_db,
        catalog,
        base_snapshot,
        source_snapshot,
        table_key,
        advances_head,
        external_preflight,
    )
    .await?;

    Ok(keep_publishing_candidate(
        candidate,
        target_active,
        source_entry,
        target_entry,
        table_key,
    ))
}

/// Classify the general adopt route after the pure-insert proof was either
/// unavailable or exceeded its recovery-plan ceiling. The caller has already
/// established table identity and whether publication advances the target
/// data HEAD.
async fn classify_general_adopt(
    target_db: &Omnigraph,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    table_key: &str,
    advances_head: bool,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
) -> Result<CandidateTableState> {
    // Compute the source-vs-target delta for the general route — it is the validation
    // input the evaluator needs, independent of how the table is published.
    // (`classify_adopt` is only reached when base == target, so the
    // base-vs-source delta equals the target-vs-source delta.) A HEAD-advancing
    // publish consumes it as the write payload (`AdoptWithDelta`); a pointer/fork
    // publish ignores it and only validates it (`AdoptSourceState`), so a
    // pointer-adopt whose source diverged is still checked for
    // RI/uniqueness/cardinality against the merged state.
    let validation_delta = compute_adopt_delta(
        target_db,
        table_key,
        catalog,
        base_snapshot,
        source_snapshot,
        advances_head,
        external_preflight,
    )
    .await?;
    match (advances_head, validation_delta) {
        (true, Some(delta)) => Ok(CandidateTableState::AdoptWithDelta(delta)),
        (_, validation_delta) => Ok(CandidateTableState::AdoptSourceState { validation_delta }),
    }
}

/// What publishing a table's adopted source state does to `__manifest`.
///
/// An empty delta does not imply an empty publish: source and target can hold
/// the same content at different Lance versions. Planning purely lets
/// classification drop a table whose registration is already stored, which the
/// registry guard would otherwise reject (#473).
#[must_use = "the adopt plan decides whether this table is a merge candidate"]
enum AdoptPublish {
    /// The planned registration is field-for-field the stored entry.
    Nothing,
    /// A pointer switch onto a lineage the target can already read.
    Pointer(crate::db::DatasetUpdate),
    /// The target holds no ref for this table yet; publication forks one. The
    /// only arm that touches storage.
    Fork {
        source_branch: String,
        target_branch: String,
    },
}

/// Plan what adopting the source's table state publishes, without an effect.
///
/// Reaching a branch-bearing arm means the delta was empty: the HEAD-advancing
/// case is classified [`CandidateTableState::AdoptWithDelta`] and published by
/// [`publish_adopted_delta`].
fn plan_adopted_source_state(
    target_active: Option<&str>,
    source_entry: &crate::db::DatasetEntry,
    target_entry: Option<&crate::db::DatasetEntry>,
    table_key: &str,
) -> AdoptPublish {
    let identity = source_entry.identity;
    let planned = match (target_active, source_entry.native_dataset_branch.as_deref()) {
        // Source on main — pointer switch to its version. The target reads the
        // same lineage whether it sits on main or on a branch.
        (None, None) | (Some(_), None) => crate::db::DatasetUpdate {
            identity,
            type_key: table_key.to_string(),
            published_dataset_version: source_entry.published_dataset_version,
            native_dataset_branch: None,
            entity_count: source_entry.entity_count,
            version_metadata: source_entry.version_metadata.clone(),
        },
        // Source on a branch, target on main — the target keeps its own
        // version and metadata; only the row count comes from the source.
        (None, Some(_)) => crate::db::DatasetUpdate {
            identity,
            type_key: table_key.to_string(),
            published_dataset_version: target_entry
                .map(|entry| entry.published_dataset_version)
                .unwrap_or(source_entry.published_dataset_version),
            native_dataset_branch: None,
            entity_count: source_entry.entity_count,
            version_metadata: target_entry
                .map(|entry| entry.version_metadata.clone())
                .unwrap_or_else(|| source_entry.version_metadata.clone()),
        },
        (Some(target_branch), Some(source_branch)) => {
            let Some(owned) = target_entry
                .filter(|entry| entry.native_dataset_branch.as_deref() == Some(target_branch))
            else {
                // A fork registers a ref the target lacks, so it is never a
                // no-op.
                return AdoptPublish::Fork {
                    source_branch: source_branch.to_string(),
                    target_branch: target_branch.to_string(),
                };
            };
            crate::db::DatasetUpdate {
                identity,
                type_key: table_key.to_string(),
                published_dataset_version: owned.published_dataset_version,
                native_dataset_branch: Some(target_branch.to_string()),
                entity_count: source_entry.entity_count,
                version_metadata: owned.version_metadata.clone(),
            }
        }
    };

    if target_entry.is_some_and(|current| reregisters_current_entry(&planned, current)) {
        return AdoptPublish::Nothing;
    }
    AdoptPublish::Pointer(planned)
}

/// Whether a planned registration is field-for-field the stored entry.
///
/// `table_path` is absent from [`crate::db::DatasetUpdate`] by design: a
/// pointer switch never moves a table, so the path cannot differ.
fn reregisters_current_entry(
    planned: &crate::db::DatasetUpdate,
    current: &crate::db::DatasetEntry,
) -> bool {
    planned.identity == current.identity
        && planned.published_dataset_version == current.published_dataset_version
        && planned.native_dataset_branch == current.native_dataset_branch
        && planned.entity_count == current.entity_count
        && planned.version_metadata == current.version_metadata
}

/// Keep a classified candidate only if publishing it changes `__manifest`.
///
/// Dropping it here gives the same disposition the manifest-equal tables get
/// before classification. Only an empty validation delta qualifies: a
/// non-empty one is the evaluator's RI / uniqueness / cardinality input and is
/// still checked even when the publish is a pointer switch.
fn keep_publishing_candidate(
    candidate: CandidateTableState,
    target_active: Option<&str>,
    source_entry: &crate::db::DatasetEntry,
    target_entry: Option<&crate::db::DatasetEntry>,
    table_key: &str,
) -> Option<CandidateTableState> {
    let publishes_nothing = matches!(
        candidate,
        CandidateTableState::AdoptSourceState {
            validation_delta: None
        }
    ) && matches!(
        plan_adopted_source_state(target_active, source_entry, target_entry, table_key),
        AdoptPublish::Nothing
    );
    if publishes_nothing {
        return None;
    }
    Some(candidate)
}

#[cfg(test)]
mod adopt_plan_tests {
    use super::*;

    /// Build a `TableVersionMetadata` without touching storage. The type is
    /// crate-private with private fields, so the derived `Deserialize` is the
    /// only in-crate constructor available to a unit test.
    fn metadata(manifest_path: &str) -> crate::db::manifest::TableVersionMetadata {
        serde_json::from_value(serde_json::json!({
            "manifest_path": manifest_path,
            "manifest_size": null,
            "e_tag": null,
            "naming_scheme": null,
        }))
        .expect("construct table version metadata")
    }

    fn entry(
        version: u64,
        branch: Option<&str>,
        row_count: u64,
        manifest_path: &str,
    ) -> crate::db::DatasetEntry {
        crate::db::DatasetEntry {
            identity: crate::db::manifest::TableIdentity::new(3, 12).unwrap(),
            type_key: "edge:Knows".to_string(),
            dataset_path: "edges/0000000000000003-000000000000000c".to_string(),
            published_dataset_version: version,
            native_dataset_branch: branch.map(ToOwned::to_owned),
            entity_count: row_count,
            version_metadata: metadata(manifest_path),
        }
    }

    /// The #473 shape: the source advanced two Lance versions on a branch and
    /// came back to the target's content. The plan must be `Nothing`.
    ///
    /// This pins the classification layer specifically. The publisher's
    /// widened registry guard also lets the redundant registration through, so
    /// no integration test can tell the two layers apart and a revert of this
    /// one would otherwise go unnoticed.
    #[test]
    fn net_zero_source_on_a_branch_plans_nothing() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(4, Some("b0"), 3, "manifest-v4");
        assert!(matches!(
            plan_adopted_source_state(None, &source, Some(&target), "edge:Knows"),
            AdoptPublish::Nothing
        ));
    }

    #[test]
    fn identical_state_on_main_plans_nothing() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(2, None, 3, "manifest-v2");
        assert!(matches!(
            plan_adopted_source_state(None, &source, Some(&target), "edge:Knows"),
            AdoptPublish::Nothing
        ));
    }

    #[test]
    fn target_owned_branch_table_with_equal_rows_plans_nothing() {
        let target = entry(5, Some("target"), 3, "manifest-v5");
        let source = entry(7, Some("source"), 3, "manifest-v7");
        assert!(matches!(
            plan_adopted_source_state(Some("target"), &source, Some(&target), "edge:Knows"),
            AdoptPublish::Nothing
        ));
    }

    /// Source on main while the target sits on a branch whose table still
    /// points at main. The planned pointer equals what is stored, so there is
    /// nothing to publish.
    #[test]
    fn source_on_main_matching_a_branch_target_plans_nothing() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(2, None, 3, "manifest-v2");
        assert!(matches!(
            plan_adopted_source_state(Some("target"), &source, Some(&target), "edge:Knows"),
            AdoptPublish::Nothing
        ));
    }

    #[test]
    fn source_on_main_ahead_of_a_branch_target_plans_a_pointer() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(5, None, 6, "manifest-v5");
        assert!(matches!(
            plan_adopted_source_state(Some("target"), &source, Some(&target), "edge:Knows"),
            AdoptPublish::Pointer(_)
        ));
    }

    /// Row counts differing means content differs, so the table must still be
    /// published even though the version and branch would match.
    #[test]
    fn differing_row_count_plans_a_pointer() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(4, Some("b0"), 4, "manifest-v4");
        assert!(matches!(
            plan_adopted_source_state(None, &source, Some(&target), "edge:Knows"),
            AdoptPublish::Pointer(_)
        ));
    }

    #[test]
    fn advanced_source_on_main_plans_a_pointer() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(3, None, 4, "manifest-v3");
        assert!(matches!(
            plan_adopted_source_state(None, &source, Some(&target), "edge:Knows"),
            AdoptPublish::Pointer(_)
        ));
    }

    #[test]
    fn absent_target_entry_plans_a_pointer() {
        let source = entry(4, Some("b0"), 3, "manifest-v4");
        assert!(matches!(
            plan_adopted_source_state(None, &source, None, "edge:Knows"),
            AdoptPublish::Pointer(_)
        ));
    }

    /// A target branch that does not own the table has no ref to compare, so
    /// the publish forks one. This arm can never be a no-op.
    #[test]
    fn unowned_target_branch_plans_a_fork() {
        let target = entry(2, None, 3, "manifest-v2");
        let source = entry(4, Some("source"), 3, "manifest-v4");
        assert!(matches!(
            plan_adopted_source_state(Some("target"), &source, Some(&target), "edge:Knows"),
            AdoptPublish::Fork { .. }
        ));
    }
}

/// Adopt the source's table state without applying a row delta: a pointer
/// switch (source/target share lineage) or a branch fork, as planned by
/// [`plan_adopted_source_state`].
///
/// `target_active` is threaded in rather than re-read. Re-reading agrees today
/// only because the merge's coordinator swap and its restore bracket both
/// calls; taking it as an argument makes agreement a property of the signature.
async fn publish_adopted_source_state(
    target_db: &Omnigraph,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    table_key: &str,
    target_active: Option<&str>,
) -> Result<crate::db::DatasetUpdate> {
    let source_entry = source_snapshot
        .dataset(table_key)
        .ok_or_else(|| OmniError::manifest(format!("missing source entry for {}", table_key)))?;
    let target_entry = target_snapshot.dataset(table_key);
    ensure_merge_identity_compatible(
        table_key,
        None,
        Some(source_entry.identity),
        target_entry.map(|entry| entry.identity),
    )?;

    match plan_adopted_source_state(target_active, source_entry, target_entry, table_key) {
        AdoptPublish::Pointer(update) => Ok(update),
        AdoptPublish::Fork {
            source_branch,
            target_branch,
        } => {
            let full_path = format!("{}/{}", target_db.uri(), source_entry.dataset_path);
            let ds = target_db
                .fork_dataset_from_entry_state(
                    table_key,
                    source_entry.identity,
                    &full_path,
                    Some(&source_branch),
                    source_entry.published_dataset_version,
                    &target_branch,
                )
                .await?;
            let state = target_db.storage().table_state(&full_path, &ds).await?;
            Ok(crate::db::DatasetUpdate {
                identity: source_entry.identity,
                type_key: table_key.to_string(),
                published_dataset_version: state.version,
                native_dataset_branch: Some(target_branch),
                entity_count: state.row_count,
                version_metadata: state.version_metadata,
            })
        }
        // Classification drops a table whose adopt publishes nothing, so this
        // arm means the candidate set and the plan disagree — an engine bug,
        // not a caller error.
        AdoptPublish::Nothing => Err(OmniError::manifest_internal(format!(
            "branch merge table '{table_key}' publishes nothing and must not be a merge candidate"
        ))),
    }
}

fn pre_minted_merge_transaction(
    read_version: u64,
) -> crate::table_store::StagedTransactionIdentity {
    crate::table_store::StagedTransactionIdentity {
        read_version,
        uuid: format!("omnigraph-merge-{}", ulid::Ulid::new()),
    }
}

fn staged_keyed_chunk_count(table_key: &str, table: &StagedTable) -> Result<usize> {
    if table.row_count == 0 || table.chunk_rows.is_empty() {
        return Err(OmniError::manifest_internal(format!(
            "branch merge strict-insert table '{table_key}' is empty"
        )));
    }
    let planned_rows = table.chunk_rows.iter().try_fold(0_u64, |total, rows| {
        total
            .checked_add(*rows as u64)
            .ok_or_else(|| OmniError::manifest_internal("branch merge planned row overflow"))
    })?;
    if planned_rows != table.row_count {
        return Err(OmniError::manifest_internal(format!(
            "branch merge strict-insert chunks for '{table_key}' contain {planned_rows} rows, expected {}",
            table.row_count
        )));
    }
    Ok(table.chunk_rows.len())
}

fn proven_insert_chunk_count(table_key: &str, proven: &ProvenPureInsertAdopt) -> Result<usize> {
    if proven.inserted_rows == 0 || proven.chunk_rows.is_empty() {
        return Err(OmniError::manifest_internal(format!(
            "branch merge proven pure-insert table '{table_key}' is empty"
        )));
    }
    let planned_rows = proven.chunk_rows.iter().try_fold(0_u64, |total, rows| {
        total.checked_add(*rows as u64).ok_or_else(|| {
            OmniError::manifest_internal("branch merge pure-insert planned row overflow")
        })
    })?;
    if planned_rows != proven.inserted_rows {
        return Err(OmniError::manifest_internal(format!(
            "branch merge pure-insert chunks for '{table_key}' contain {planned_rows} rows, expected {}",
            proven.inserted_rows
        )));
    }
    Ok(proven.chunk_rows.len())
}

fn enforce_merge_transaction_ceiling(table_key: &str, transaction_count: usize) -> Result<u64> {
    let transaction_count = u64::try_from(transaction_count).map_err(|_| {
        OmniError::manifest_internal(format!(
            "branch merge transaction count for '{table_key}' exceeds u64"
        ))
    })?;
    if transaction_count > crate::db::manifest::MAX_BRANCH_MERGE_DATA_TRANSACTIONS {
        return Err(OmniError::resource_limit(
            format!("branch-merge recovery transactions for {table_key}"),
            crate::db::manifest::MAX_BRANCH_MERGE_DATA_TRANSACTIONS,
            transaction_count,
        ));
    }
    Ok(transaction_count)
}

fn plan_merge_transactions(
    table_key: &str,
    candidate: &CandidateTableState,
    first_read_version: u64,
) -> Result<Vec<crate::table_store::StagedTransactionIdentity>> {
    let transaction_count = match candidate {
        CandidateTableState::RewriteMerged(staged) => {
            staged
                .delta_staged
                .as_ref()
                .map(|table| staged_keyed_chunk_count(table_key, table))
                .transpose()?
                .unwrap_or(0)
                + staged.deleted_ids.chunk_count()
        }
        CandidateTableState::AdoptWithDelta(delta) => {
            delta
                .inserts
                .as_ref()
                .map(|table| staged_keyed_chunk_count(table_key, table))
                .transpose()?
                .unwrap_or(0)
                + delta
                    .upserts
                    .as_ref()
                    .map(|table| staged_keyed_chunk_count(table_key, table))
                    .transpose()?
                    .unwrap_or(0)
                + delta.deleted_ids.chunk_count()
        }
        CandidateTableState::AdoptPureInserts(proven) => {
            proven_insert_chunk_count(table_key, proven)?
        }
        CandidateTableState::AdoptSourceState { .. } => 0,
    };
    if transaction_count == 0 {
        return Err(OmniError::manifest_internal(format!(
            "HEAD-advancing branch merge candidate '{table_key}' has no logical data transaction"
        )));
    }
    enforce_merge_transaction_ceiling(table_key, transaction_count)?;

    (0..transaction_count)
        .map(|offset| {
            let offset = u64::try_from(offset).map_err(|_| {
                OmniError::manifest_internal(format!(
                    "branch merge transaction count for '{table_key}' exceeds u64"
                ))
            })?;
            let read_version = first_read_version.checked_add(offset).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch merge transaction version overflow for '{table_key}'"
                ))
            })?;
            Ok(pre_minted_merge_transaction(read_version))
        })
        .collect()
}

#[cfg(test)]
mod recovery_chain_limit_tests {
    use super::*;

    fn delete_only_candidate(chunk_count: usize) -> CandidateTableState {
        let filter_bytes =
            delete_filter_fixed_bytes().unwrap() + escaped_delete_literal_bytes("id").unwrap();
        CandidateTableState::RewriteMerged(StagedMergeResult {
            delta_staged: None,
            deleted_ids: DeleteIdChunks {
                chunks: (0..chunk_count)
                    .map(|_| DeleteIdChunk {
                        ids: vec!["id".to_string()],
                        filter_bytes,
                    })
                    .collect(),
                retained_bytes: 0,
            },
        })
    }

    #[test]
    fn branch_merge_delete_ids_split_on_row_and_escaped_byte_bounds() {
        let mut row_bounded = DeleteIdChunks::default();
        for row in 0..=KEYED_WRITE_MAX_ROWS {
            row_bounded.push(format!("id-{row}")).unwrap();
        }
        assert_eq!(row_bounded.chunk_count(), 2);
        assert_eq!(row_bounded.chunks[0].ids.len(), KEYED_WRITE_MAX_ROWS);
        assert_eq!(row_bounded.chunks[1].ids.len(), 1);
        for chunk in &row_bounded.chunks {
            let filter = chunk.filter().unwrap();
            assert_eq!(filter.len() as u64, chunk.filter_bytes);
            assert!(filter.len() as u64 <= KEYED_WRITE_MAX_BYTES);
        }

        // `a'b` is six bytes as an escaped SQL literal (`'a''b'`) and the
        // `id IN (` / `)` framing is another eight. The exact 14-byte filter
        // fits; adding a second id starts a new chunk rather than exceeding it.
        let mut byte_bounded = DeleteIdChunks::default();
        byte_bounded
            .push_bounded("a'b".to_string(), KEYED_WRITE_MAX_ROWS, 14)
            .unwrap();
        byte_bounded
            .push_bounded("x".to_string(), KEYED_WRITE_MAX_ROWS, 14)
            .unwrap();
        assert_eq!(byte_bounded.chunk_count(), 2);
        assert_eq!(byte_bounded.chunks[0].filter().unwrap(), "id IN ('a''b')");
        assert_eq!(byte_bounded.chunks[1].filter().unwrap(), "id IN ('x')");

        let error = DeleteIdChunks::default()
            .push_bounded("a''b".to_string(), KEYED_WRITE_MAX_ROWS, 14)
            .unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 14,
                actual: 16,
            } if resource == "branch-merge delete filter bytes"
        ));

        let mut retained_bounded = DeleteIdChunks::default();
        retained_bounded
            .push_with_bounds("a".to_string(), KEYED_WRITE_MAX_ROWS, 1024, 256)
            .unwrap();
        let retained_error = loop {
            match retained_bounded.push_with_bounds(
                "b".to_string(),
                KEYED_WRITE_MAX_ROWS,
                1024,
                256,
            ) {
                Ok(()) => continue,
                Err(error) => break error,
            }
        };
        assert!(matches!(
            retained_error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 256,
                actual,
            } if resource == "branch-merge retained delete plan bytes" && actual > 256
        ));
    }

    #[test]
    fn branch_merge_delete_chunks_pre_mint_one_exact_identity_each() {
        let mut deleted_ids = DeleteIdChunks::default();
        for row in 0..=KEYED_WRITE_MAX_ROWS {
            deleted_ids.push(format!("id-{row}")).unwrap();
        }
        let candidate = CandidateTableState::RewriteMerged(StagedMergeResult {
            delta_staged: None,
            deleted_ids,
        });
        let planned = plan_merge_transactions("node:Person", &candidate, 41).unwrap();
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].read_version, 41);
        assert_eq!(planned[1].read_version, 42);
        assert_ne!(planned[0].uuid, planned[1].uuid);
    }

    #[test]
    fn branch_merge_recovery_chain_ceiling_is_inclusive() {
        let limit = crate::db::manifest::MAX_BRANCH_MERGE_DATA_TRANSACTIONS as usize;
        assert_eq!(
            crate::db::manifest::MAX_EFFECT_IDENTITY_SCAN_VERSIONS,
            limit as u64 + 2,
            "recovery must reserve one derived-index tail and one Restore"
        );
        assert_eq!(
            enforce_merge_transaction_ceiling("node:Person", limit).unwrap(),
            limit as u64
        );
        let planned =
            plan_merge_transactions("node:Person", &delete_only_candidate(limit), 1).unwrap();
        assert_eq!(planned.len(), limit);
        assert_eq!(planned.last().unwrap().read_version, limit as u64);

        let error = plan_merge_transactions("node:Person", &delete_only_candidate(limit + 1), 1)
            .unwrap_err();
        assert!(matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: actual_limit,
                actual,
            } if resource == "branch-merge recovery transactions for node:Person"
                && actual_limit == limit as u64
                && actual == limit as u64 + 1
        ));
    }
}

async fn commit_exact_merge_stage(
    target_db: &Omnigraph,
    current: SnapshotHandle,
    mut staged: crate::storage_layer::StagedHandle,
    planned: &crate::table_store::StagedTransactionIdentity,
) -> Result<SnapshotHandle> {
    staged.bind_transaction_identity(planned)?;
    let outcome = target_db
        .storage()
        .commit_staged_exact(current, staged)
        .await?;
    if !outcome.is_exact() {
        return Err(OmniError::manifest_read_set_changed(
            "branch_merge_lance_transaction",
            Some(format!("{:?}", outcome.planned_transaction())),
            Some(format!("{:?}", outcome.committed_transaction())),
        ));
    }
    Ok(outcome.into_snapshot())
}

async fn commit_staged_delete_chunks(
    target_db: &Omnigraph,
    table_key: &str,
    deleted_ids: &DeleteIdChunks,
    mut current: SnapshotHandle,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
    planned_index: &mut usize,
) -> Result<SnapshotHandle> {
    for (chunk_index, chunk) in deleted_ids.chunks.iter().enumerate() {
        let filter = chunk.filter()?;
        let staged_delete = target_db
            .storage()
            .stage_delete(&current, &filter)
            .await?
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch merge delete chunk {} for table '{table_key}' matched no rows",
                    chunk_index + 1
                ))
            })?;
        let planned = planned_transactions.get(*planned_index).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "branch merge table '{table_key}' has no transaction planned for delete chunk {}",
                chunk_index + 1
            ))
        })?;
        *planned_index += 1;
        current = commit_exact_merge_stage(target_db, current, staged_delete, planned).await?;
        if chunk_index + 1 < deleted_ids.chunks.len() {
            crate::failpoints::maybe_fail(
                crate::failpoints::names::BRANCH_MERGE_BETWEEN_DELETE_CHUNKS,
            )?;
        }
    }
    Ok(current)
}

async fn publish_rewritten_merge_table(
    target_db: &Omnigraph,
    table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    staged: &StagedMergeResult,
    prepared_target: Option<PreparedExistingMergeTarget>,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
) -> Result<crate::db::DatasetUpdate> {
    // Branch merge's source-rewrite path is Merge-shaped (upsert from
    // source onto target). The staged delete later in this function
    // (`stage_delete` + an exact commit) operates on rows the rewrite chose
    // to remove, not user-facing predicates, so Merge is the correct policy
    // here.
    // Existing refs consume the same handle verified before Phase A. Only a
    // first-touch target reaches `open_for_mutation` here, after recovery owns
    // the ref creation; its no-txn path always returns a handle.
    let (mut current_ds, full_path, table_branch) = match prepared_target {
        Some(prepared) => prepared.into_parts(),
        None => target_db
            .open_for_mutation(table_key, crate::db::MutationOpKind::Merge)
            .await?
            .require_handle("branch merge first touch"),
    };

    // Phase 1: merge_insert changed/new rows (preserves _row_created_at_version for
    // existing rows, bumps _row_last_updated_at_version only for actually-changed rows).
    //
    // Routed through the staged primitive with the transaction identity armed
    // in the v4 sidecar and transparent Lance conflict retries disabled. A
    // failure between writing fragments and committing leaves no Lance-HEAD
    // drift; a failure after the exact commit remains recovery-owned.
    let mut planned_index = 0_usize;
    if let Some(delta) = &staged.delta_staged {
        current_ds = commit_staged_keyed_chunks(
            target_db,
            table_key,
            delta,
            KeyedWriteSemantics::Upsert,
            current_ds,
            planned_transactions,
            &mut planned_index,
            None,
        )
        .await?;
    }

    // Failpoint: crash after the Phase 1 merge_insert commit, before the delete.
    // Models a partial Phase B on the three-way path — the merged constructive
    // rows are on Lance HEAD but the delete has not committed and the
    // achieved-version intent has not been recorded, so recovery must roll BACK.
    // See tests/failpoints.rs::branch_merge_rewrite_partial_after_merge_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE,
    )?;

    // Phase 2: delete removed rows via deletion vectors. Each row/byte-bounded
    // filter chunk is staged through `stage_delete` and consumes one exact
    // pre-minted transaction (MR-A — Lance 7.0's
    // `DeleteBuilder::execute_uncommitted`, #6658, made delete a two-phase
    // staged write, so this no longer inline-commits).
    current_ds = commit_staged_delete_chunks(
        target_db,
        table_key,
        &staged.deleted_ids,
        current_ds,
        planned_transactions,
        &mut planned_index,
    )
    .await?;

    if let Some(unused) = planned_transactions.get(planned_index) {
        return Err(OmniError::manifest_internal(format!(
            "branch merge table '{table_key}' did not apply planned transaction {unused:?}"
        )));
    }

    // Failpoint: crash after the Phase 2 delete commit, before confirmation.
    // Models a partial Phase B on the three-way path — constructive rows +
    // deletes are on Lance HEAD but the achieved-version intent has not been
    // recorded, so recovery must roll BACK. See
    // tests/failpoints.rs::branch_merge_rewrite_partial_after_delete_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM,
    )?;

    // Index coverage is reconciler-owned derived state. As on the adopt path,
    // publish the logical merge without waiting for BTREE / FTS / vector work;
    // `ensure_indices` / `optimize` converges coverage later while reads remain
    // correct over uncovered fragments.
    let final_state = target_db
        .storage()
        .table_state(&full_path, &current_ds)
        .await?;

    Ok(crate::db::DatasetUpdate {
        identity,
        type_key: table_key.to_string(),
        published_dataset_version: final_state.version,
        native_dataset_branch: table_branch,
        entity_count: final_state.row_count,
        version_metadata: final_state.version_metadata,
    })
}

/// Reassemble one exact writer-defined chunk from a scanner whose batches are
/// only upper-bounded. Lance may legally emit shorter batches at fragment/page
/// boundaries; recovery transaction planning must not depend on that physical
/// choice. At most one bounded scanner batch plus one bounded output chunk is
/// retained at a time.
async fn next_exact_staged_chunk(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
    carry: &mut Option<RecordBatch>,
    schema: &SchemaRef,
    expected_rows: usize,
) -> Result<RecordBatch> {
    if expected_rows == 0 {
        return Err(OmniError::manifest_internal(
            "branch merge planned an empty keyed chunk",
        ));
    }
    let mut remaining = expected_rows;
    let mut slices = Vec::new();
    while remaining > 0 {
        let batch = match carry.take() {
            Some(batch) => batch,
            None => loop {
                match stream.try_next().await.map_err(OmniError::datafusion)? {
                    Some(batch) if batch.num_rows() > 0 => break batch,
                    Some(_) => continue,
                    None => {
                        return Err(OmniError::manifest_internal(format!(
                            "branch merge keyed stream ended with {remaining} rows missing from a planned {expected_rows}-row chunk"
                        )));
                    }
                }
            },
        };
        let take = remaining.min(batch.num_rows());
        slices.push(batch.slice(0, take));
        remaining -= take;
        if take < batch.num_rows() {
            *carry = Some(batch.slice(take, batch.num_rows() - take));
        }
    }
    let chunk = if slices.len() == 1 {
        slices.pop().expect("one slice")
    } else {
        arrow_select::concat::concat_batches(schema, &slices).map_err(OmniError::arrow_internal)?
    };
    let chunk_bytes = u64::try_from(chunk.get_array_memory_size())
        .map_err(|_| OmniError::manifest_internal("branch merge chunk bytes exceed u64"))?;
    if chunk.num_rows() > KEYED_WRITE_MAX_ROWS || chunk_bytes > KEYED_WRITE_MAX_BYTES {
        return Err(OmniError::manifest_internal(format!(
            "branch merge reconstructed a keyed chunk outside its armed bound: {} rows / {chunk_bytes} bytes",
            chunk.num_rows()
        )));
    }
    Ok(chunk)
}

async fn commit_staged_keyed_chunks(
    target_db: &Omnigraph,
    table_key: &str,
    table: &StagedTable,
    semantics: KeyedWriteSemantics,
    current: SnapshotHandle,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
    planned_index: &mut usize,
    between_chunk_failpoint: Option<&str>,
) -> Result<SnapshotHandle> {
    let source = SnapshotHandle::new(table.dataset.clone());
    let stream = target_db
        .storage()
        .scan_stream_for_rewrite_bounded(&source, KEYED_WRITE_MAX_ROWS, KEYED_WRITE_MAX_BYTES)
        .await?;
    let schema: SchemaRef = Arc::new(table.dataset.schema().into());
    commit_keyed_stream_chunks(
        target_db,
        table_key,
        stream,
        &schema,
        &table.chunk_rows,
        table.row_count,
        KeyedChunkStage::General(semantics),
        current,
        planned_transactions,
        planned_index,
        between_chunk_failpoint,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum KeyedChunkStage {
    General(KeyedWriteSemantics),
    // Keep this admission variant exclusive to
    // `publish_proven_pure_insert_adopt`; the structural protocol test pins
    // both production uses so another caller cannot silently mint the opaque
    // storage capability through this shared loop.
    ProvenStrictInsert,
}

async fn commit_keyed_stream_chunks(
    target_db: &Omnigraph,
    table_key: &str,
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    schema: &SchemaRef,
    chunk_rows: &[usize],
    expected_row_count: u64,
    stage_kind: KeyedChunkStage,
    mut current: SnapshotHandle,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
    planned_index: &mut usize,
    between_chunk_failpoint: Option<&str>,
) -> Result<SnapshotHandle> {
    let mut carry = None;
    let mut observed_rows = 0_u64;
    for (chunk_index, expected_rows) in chunk_rows.iter().copied().enumerate() {
        let batch = next_exact_staged_chunk(&mut stream, &mut carry, schema, expected_rows).await?;
        observed_rows = observed_rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| OmniError::manifest_internal("branch merge row count overflow"))?;
        let stage_start = std::time::Instant::now();
        let staged = match stage_kind {
            KeyedChunkStage::General(semantics) => {
                target_db
                    .storage()
                    .stage_keyed_write(current.clone(), table_key, batch, semantics)
                    .await?
            }
            KeyedChunkStage::ProvenStrictInsert => {
                let chunk = ProvenInsertChunk::from_verified_history(
                    &current,
                    table_key,
                    batch,
                    chunk_index,
                )?;
                target_db
                    .storage()
                    .stage_proven_strict_insert(current.clone(), chunk)
                    .await?
            }
        };
        crate::instrumentation::record_merge_timing(
            crate::instrumentation::MergeTimingPhase::KeyedStage,
            stage_start.elapsed(),
        );
        let planned = planned_transactions.get(*planned_index).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "branch merge table '{table_key}' has no transaction planned for keyed chunk {}",
                chunk_index + 1
            ))
        })?;
        *planned_index += 1;
        let commit_start = std::time::Instant::now();
        current = commit_exact_merge_stage(target_db, current, staged, planned).await?;
        crate::instrumentation::record_merge_timing(
            crate::instrumentation::MergeTimingPhase::KeyedCommit,
            commit_start.elapsed(),
        );
        if chunk_index + 1 < chunk_rows.len()
            && let Some(failpoint) = between_chunk_failpoint
        {
            crate::failpoints::maybe_fail(failpoint)?;
        }
    }

    let mut has_extra_rows = carry.as_ref().is_some_and(|batch| batch.num_rows() > 0);
    while !has_extra_rows {
        match stream.try_next().await.map_err(OmniError::datafusion)? {
            Some(batch) => has_extra_rows = batch.num_rows() > 0,
            None => break,
        }
    }
    if has_extra_rows || observed_rows != expected_row_count {
        return Err(OmniError::manifest_internal(format!(
            "branch merge table '{table_key}' streamed {observed_rows} keyed entities against an armed plan for {}",
            expected_row_count
        )));
    }
    Ok(current)
}

/// Publish a provenance-proven all-insert source interval as the established
/// row/byte-bounded chain of native Lance filter-bearing Update transactions.
/// The proof plus the final target-incarnation and physical-baseline gates make
/// both an exact target-membership preflight and a target merge join redundant;
/// Lance still writes target-owned fragments, resolves concurrent transaction
/// filters, assigns stable row ids and row-version metadata, and builds each
/// manifest. OmniGraph avoids both the general base/source ordered diff and the
/// redundant lookups without weakening the memory bound or exposing a
/// graph-visible Append operation.
async fn publish_proven_pure_insert_adopt(
    target_db: &Omnigraph,
    table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    proven: &ProvenPureInsertAdopt,
    external_preflight: &crate::table_store::ExternalBlobPreflight,
    prepared_target: PreparedExistingMergeTarget,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
) -> Result<crate::db::DatasetUpdate> {
    let publish_start = std::time::Instant::now();
    let (current, full_path, table_branch) = prepared_target.into_parts();
    let source = SnapshotHandle::new(proven.source.clone());
    let stream = target_db
        .storage()
        .scan_proven_insert_delta_bounded(
            &source,
            table_key,
            proven.base_version,
            proven.source_version,
            external_preflight,
        )
        .await?;
    let schema: SchemaRef = Arc::new(proven.source.schema().into());
    let mut planned_index = 0_usize;
    let committed = commit_keyed_stream_chunks(
        target_db,
        table_key,
        stream,
        &schema,
        &proven.chunk_rows,
        proven.inserted_rows,
        KeyedChunkStage::ProvenStrictInsert,
        current,
        planned_transactions,
        &mut planned_index,
        Some(crate::failpoints::names::BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS),
    )
    .await?;
    if let Some(unused) = planned_transactions.get(planned_index) {
        return Err(OmniError::manifest_internal(format!(
            "branch merge pure-insert table '{table_key}' did not apply planned transaction {unused:?}"
        )));
    }
    let final_state = target_db
        .storage()
        .table_state(&full_path, &committed)
        .await?;
    crate::instrumentation::record_merge_timing(
        crate::instrumentation::MergeTimingPhase::PhysicalPublish,
        publish_start.elapsed(),
    );

    Ok(crate::db::DatasetUpdate {
        identity,
        type_key: table_key.to_string(),
        published_dataset_version: final_state.version,
        native_dataset_branch: table_branch,
        entity_count: final_state.row_count,
        version_metadata: final_state.version_metadata,
    })
}

/// Apply an [`AdoptDelta`] onto the target's base lineage (the fast-forward /
/// target-owns path). Kept separate from `publish_rewritten_merge_table` (the
/// three-way path) because the two paths diverge: commit 3 splits this Phase 1
/// into append (new) + merge_insert (changed), and commit 6 makes its index
/// coverage incremental — neither of which the three-way path takes.
///
/// `open_for_mutation(Merge)` opens the target's own table lineage (active
/// branch is the merge target after the caller's swap), so every write lands on
/// the target and survives source-branch deletion — GC-safe.
///
/// TRANSITIONAL — removed by the fragment-adopt work (see [`AdoptDelta`]): the
/// multi-commit strict-insert → upsert → delete publish here (the source of the
/// partial-Phase-B recovery window the sidecar confirmation guards) collapses to
/// a single fragment-graft commit per table, so this whole function goes away.
async fn publish_adopted_delta(
    target_db: &Omnigraph,
    table_key: &str,
    identity: crate::db::manifest::TableIdentity,
    delta: &AdoptDelta,
    prepared_target: Option<PreparedExistingMergeTarget>,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
) -> Result<crate::db::DatasetUpdate> {
    // Existing refs consume the same handle verified before Phase A. Only a
    // first-touch target reaches `open_for_mutation` here, after recovery owns
    // the ref creation; its no-txn path always returns a handle.
    let (mut current_ds, full_path, table_branch) = match prepared_target {
        Some(prepared) => prepared.into_parts(),
        None => target_db
            .open_for_mutation(table_key, crate::db::MutationOpKind::Merge)
            .await?
            .require_handle("branch merge first touch"),
    };

    // Phase 1a: strict-insert the NEW rows through RFC-023's streaming exact-id
    // adapter. The ordered walk classified them as absent at the pinned base,
    // but `WhenMatched::Fail` is still required: a concurrent same-key writer
    // must conflict rather than letting this optimization bypass the fence.
    // The adapter keeps wide vector/blob rows streaming and batch-bounded.
    let mut planned_index = 0_usize;
    if let Some(insert_table) = &delta.inserts {
        current_ds = commit_staged_keyed_chunks(
            target_db,
            table_key,
            insert_table,
            KeyedWriteSemantics::StrictInsert,
            current_ds,
            planned_transactions,
            &mut planned_index,
            Some(crate::failpoints::names::BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS),
        )
        .await?;
    }

    // Failpoint: crash after the Phase 1a strict-insert commit, before the upsert.
    // Models a partial Phase B — inserts are on Lance HEAD but the upserts/deletes
    // have not committed and the achieved-version intent has not been recorded, so
    // recovery must roll BACK (not publish the inserts-only state). See
    // tests/failpoints.rs::branch_merge_adopt_partial_after_append_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT,
    )?;

    // Phase 1b: update the CHANGED rows. Classification proved these ids present
    // in the target-equals-base image; the sealed update-only adapter forbids
    // insertion and fails closed if final staging cannot update every id. The
    // changed ids are disjoint from the inserted ids (each id is classified into
    // exactly one of new / changed / deleted / unchanged in the single ordered
    // walk). Every logical data step uses the next identity in the exact
    // transaction chain armed before Phase B.
    if let Some(upsert_table) = &delta.upserts {
        current_ds = commit_staged_keyed_chunks(
            target_db,
            table_key,
            upsert_table,
            KeyedWriteSemantics::KnownPresentUpdate,
            current_ds,
            planned_transactions,
            &mut planned_index,
            None,
        )
        .await?;
    }

    // Failpoint: crash after the Phase 1b upsert commit, before the delete.
    // Models a partial Phase B — inserts + upserts are on Lance HEAD but the delete
    // has not committed and the achieved-version intent has not been recorded, so
    // recovery must roll BACK. See
    // tests/failpoints.rs::branch_merge_adopt_partial_after_upsert_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE,
    )?;

    // Phase 2: delete removed rows via row/byte-bounded deletion-vector
    // transactions (same exact-chain helper as the three-way path; MR-A).
    current_ds = commit_staged_delete_chunks(
        target_db,
        table_key,
        &delta.deleted_ids,
        current_ds,
        planned_transactions,
        &mut planned_index,
    )
    .await?;

    if let Some(unused) = planned_transactions.get(planned_index) {
        return Err(OmniError::manifest_internal(format!(
            "branch merge table '{table_key}' did not apply planned transaction {unused:?}"
        )));
    }

    // Phase 4: index coverage is reconciler-owned on the adopt path. Unlike the
    // three-way `RewriteMerged` path, this does NOT build indices inline: the
    // appended/upserted rows are left uncovered (reads stay correct via
    // brute-force — indexes are derived state, invariant 7) and
    // `optimize` / `ensure_indices` folds them in. This keeps even the first
    // merge into a freshly schema-applied (unindexed) table fast — no inline IVF
    // retrain on the publish path — and is the row-level approximation of Layer
    // 2's fragment-adopt, where the source branch's already-built indices carry
    // over by reference. See docs/user/branching/merge.md.
    let final_state = target_db
        .storage()
        .table_state(&full_path, &current_ds)
        .await?;

    Ok(crate::db::DatasetUpdate {
        identity,
        type_key: table_key.to_string(),
        published_dataset_version: final_state.version,
        native_dataset_branch: table_branch,
        entity_count: final_state.row_count,
        version_metadata: final_state.version_metadata,
    })
}

fn ensure_merge_target_authority_unchanged(
    captured: &WriteTxn,
    live: &crate::db::WriteAuthorityToken,
) -> Result<()> {
    let branch = captured.branch.as_deref().unwrap_or("main");
    if live.branch_identifier != captured.authority.branch_identifier {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_identifier:{branch}"),
            Some(
                serde_json::to_string(&captured.authority.branch_identifier).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "serialize captured branch identifier: {error}"
                    ))
                })?,
            ),
            Some(
                serde_json::to_string(&live.branch_identifier).map_err(|error| {
                    OmniError::manifest_internal(format!(
                        "serialize live branch identifier: {error}"
                    ))
                })?,
            ),
        ));
    }
    if live.graph_head != captured.authority.graph_head {
        return Err(OmniError::manifest_read_set_changed(
            format!("graph_head:{branch}"),
            captured.authority.graph_head.clone(),
            live.graph_head.clone(),
        ));
    }
    for (scope, expected, observed) in [
        (
            "schema_ir_hash",
            captured.authority.schema_ir_hash.as_str(),
            live.schema_ir_hash.as_str(),
        ),
        (
            "schema_identity_domain",
            captured.authority.schema_identity_domain.as_str(),
            live.schema_identity_domain.as_str(),
        ),
    ] {
        if expected != observed {
            return Err(OmniError::manifest_read_set_changed(
                scope.to_string(),
                Some(expected.to_string()),
                Some(observed.to_string()),
            ));
        }
    }
    if live.schema_identity_version != captured.authority.schema_identity_version {
        return Err(OmniError::manifest_read_set_changed(
            "schema_identity_version".to_string(),
            Some(captured.authority.schema_identity_version.to_string()),
            Some(live.schema_identity_version.to_string()),
        ));
    }
    Ok(())
}

impl Omnigraph {
    pub async fn branch_merge(&self, source: &str, target: &str) -> Result<MergeOutcome> {
        self.branch_merge_as(source, target, None).await
    }

    pub async fn branch_merge_as(
        &self,
        source: &str,
        target: &str,
        actor_id: Option<&str>,
    ) -> Result<MergeOutcome> {
        // Engine-layer policy gate (MR-722 fan-out / PR #3). Scope is
        // `BranchTransition { source, target }` — matches the HTTP-layer
        // convention at `server_branch_merge` (branch=Some(source),
        // target_branch=Some(target)). Cedar rules using
        // `target_branch_scope: protected` therefore correctly gate
        // merges INTO protected branches without forbidding the
        // (symmetric) source-side reference.
        self.enforce(
            omnigraph_policy::PolicyAction::BranchMerge,
            &omnigraph_policy::ResourceScope::BranchTransition {
                source: source.to_string(),
                target: target.to_string(),
            },
            actor_id,
        )?;
        self.ensure_schema_apply_idle("branch_merge").await?;
        self.branch_merge_impl(source, target, actor_id).await
    }

    async fn branch_merge_impl(
        &self,
        source: &str,
        target: &str,
        actor_id: Option<&str>,
    ) -> Result<MergeOutcome> {
        let outer_prepare_start = std::time::Instant::now();
        if is_internal_system_branch(source) || is_internal_system_branch(target) {
            return Err(OmniError::manifest(format!(
                "branch_merge does not allow internal system refs ('{}' -> '{}')",
                source, target
            )));
        }
        let source_branch = Omnigraph::normalize_branch_name(source)?;
        let target_branch = Omnigraph::normalize_branch_name(target)?;
        if source_branch == target_branch {
            return Err(OmniError::manifest(
                "branch_merge requires distinct source and target branches".to_string(),
            ));
        }

        let relevant_branches = [source_branch.as_deref(), target_branch.as_deref()];
        // Branch merge is still a legacy per-table publisher, but its graph-ref
        // authority must be stable for the complete prepare -> publish window.
        // First converge or reject relevant recovery intent, then join the same
        // root-shared schema -> branch order used by native branch controls.
        // Holding both branch gates through publication prevents a target
        // delete/recreate from reusing the branch name underneath a plan (ABA).
        self.heal_pending_recovery_sidecars_for_write(&relevant_branches)
            .await?;
        let _schema_guard = self
            .write_queue()
            .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
            .await;
        let _branch_guards = self
            .write_queue()
            .acquire_branches(&[source_branch.clone(), target_branch.clone()])
            .await;
        self.ensure_no_pending_recovery_sidecars_under_gates(&relevant_branches, "branch_merge")
            .await?;
        self.ensure_schema_apply_not_locked("branch_merge").await?;
        // Capture each branch as one coherent RFC-022 authority token plus
        // immutable snapshot. The target token is the coarse publish read set;
        // the source token pins the exact merge input without requiring the
        // source head to remain latest until the target CAS.
        let (source_txn, target_txn, source_commits, target_commits) = self
            .open_merge_write_txns(source_branch.as_deref(), target_branch.as_deref())
            .await?;
        let source_head_commit_id = source_txn
            .effective_graph_head
            .clone()
            .ok_or_else(|| OmniError::manifest("source branch has no head commit".to_string()))?;
        let target_head_commit_id = target_txn
            .effective_graph_head
            .clone()
            .ok_or_else(|| OmniError::manifest("target branch has no head commit".to_string()))?;
        let base_commit = CommitGraph::merge_base_from_commits(
            source_commits,
            target_commits,
            &source_head_commit_id,
            &target_head_commit_id,
        )
        .ok_or_else(|| {
            OmniError::manifest(
                "captured branch commits are unavailable or have no common ancestor".to_string(),
            )
        })?;

        if source_head_commit_id == target_head_commit_id
            || base_commit.graph_commit_id == source_head_commit_id
        {
            return Ok(MergeOutcome::AlreadyUpToDate);
        }
        let is_fast_forward = base_commit.graph_commit_id == target_head_commit_id;

        let base_snapshot = if is_fast_forward {
            // The captured target is the logical merge base. Reopening the
            // historical manifest can only recover the same reduced table
            // state (possibly at an older physical-only manifest version), so
            // the already validated authority snapshot is the stronger input.
            target_txn.base.clone()
        } else {
            ManifestCoordinator::snapshot_at(
                self.uri(),
                base_commit.graph_branch.as_deref(),
                base_commit.graph_manifest_version,
            )
            .await?
        };
        crate::failpoints::maybe_fail(
            crate::failpoints::names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE,
        )?;
        // Hold the merge-exclusive mutex across the full optional
        // swap → operate → restore window. Two concurrent branch_merge calls
        // would otherwise interleave their coordinator acquisitions, leaving
        // each merge's body running against the other's target. Pinned by
        // `concurrent_branch_merges_distinct_targets_do_not_swap_into_each_other`
        // in `crates/omnigraph-server/tests/server.rs`.
        let merge_exclusive = self.merge_exclusive();
        let _merge_guard = merge_exclusive.lock().await;

        let previous_branch = self.active_branch().await;
        let target_was_active = previous_branch == target_branch;
        let reuse_active_target =
            target_was_active && self.active_coordinator_matches(&target_txn).await?;
        let previous = if reuse_active_target {
            None
        } else {
            let previous = self
                .swap_coordinator_for_branch(target_branch.as_deref())
                .await?;
            // If the target name was already active but its warm coordinator
            // was stale, keep the fresh replacement instead of restoring the
            // stale instance after the merge.
            (!target_was_active).then_some(previous)
        };
        crate::instrumentation::record_merge_timing(
            crate::instrumentation::MergeTimingPhase::OuterPrepare,
            outer_prepare_start.elapsed(),
        );
        // Keep the deliberately large merge planner/publisher state out of the
        // operation shell's generated future. The inner operation composes
        // ordered typed comparison, Blob materialization, validation, recovery,
        // and publication; carrying that state inline makes debug builds retain
        // large construction/projection temporaries across the shell's guards
        // and restore path. Boxing at this natural ownership boundary changes
        // no scheduling or lifetime semantics; this child slot is one pointer
        // while the inner operation runs.
        let merge_result = Box::pin(self.branch_merge_on_current_target(
            &base_snapshot,
            &source_txn,
            &target_txn,
            source_branch.as_deref(),
            target_branch.as_deref(),
            &target_head_commit_id,
            &source_head_commit_id,
            is_fast_forward,
            actor_id,
        ))
        .await;
        let outer_restore_start = std::time::Instant::now();
        if let Some(previous) = previous {
            self.restore_coordinator(previous).await;
        }

        // When the target was already active, the successful manifest publisher
        // folded its new snapshot and lineage into this same coordinator; an
        // open/swap/restore/refresh cycle would only repeat durable reads. On an
        // error, refresh best-effort in case the durable publish landed but its
        // acknowledgement was lost. Use the coordinator-only refresh so an
        // in-flight recovery sidecar is never consumed by its own writer.
        if target_was_active && merge_result.is_err() {
            if let Err(refresh_err) = self.refresh_coordinator_only().await {
                tracing::warn!(
                    error = %refresh_err,
                    "post-merge coordinator refresh failed on the error path; \
                     the next op or open will re-sync"
                );
            }
        }

        crate::instrumentation::record_merge_timing(
            crate::instrumentation::MergeTimingPhase::OuterRestoreRefresh,
            outer_restore_start.elapsed(),
        );

        merge_result
    }

    async fn branch_merge_on_current_target(
        &self,
        base_snapshot: &Snapshot,
        source_txn: &WriteTxn,
        target_txn: &WriteTxn,
        source_branch: Option<&str>,
        target_branch: Option<&str>,
        target_head_commit_id: &str,
        source_head_commit_id: &str,
        is_fast_forward: bool,
        actor_id: Option<&str>,
    ) -> Result<MergeOutcome> {
        let source_snapshot = &source_txn.base;
        let target_snapshot = &target_txn.base;
        let catalog = target_txn.catalog.as_ref();
        let mut table_keys = HashSet::new();
        for entry in base_snapshot.datasets() {
            table_keys.insert(entry.type_key.clone());
        }
        for entry in source_snapshot.datasets() {
            table_keys.insert(entry.type_key.clone());
        }
        for entry in target_snapshot.datasets() {
            table_keys.insert(entry.type_key.clone());
        }

        let mut ordered_table_keys: Vec<String> = table_keys.into_iter().collect();
        ordered_table_keys.sort();

        let mut conflicts = Vec::new();
        let target_active = self.active_branch().await;
        let mut candidates: HashMap<String, CandidateTableState> = HashMap::new();
        let empty_external_preflight = crate::table_store::ExternalBlobPreflight::default();
        let mut blob_table_keys = HashSet::new();
        let mut blob_selection = crate::table_store::PersistedBlobSelection::default();
        let mut blob_adopt_proof_attempted = HashSet::new();
        let mut blob_pure_insert_histories: HashMap<String, ProvenPureInsertAdopt> = HashMap::new();
        let materializer = self.blob_materializer();

        // Classify scalar tables once before any external source I/O. Blob
        // tables get a descriptor-only first pass so every row-writing managed
        // value and exact external range shares one operation budget. Pointer
        // and fork adoption write no row, so their descriptors require neither
        // policy approval nor source I/O.
        for table_key in &ordered_table_keys {
            let base_entry = base_snapshot.dataset(table_key);
            let source_entry = source_snapshot.dataset(table_key);
            let target_entry = target_snapshot.dataset(table_key);
            if same_manifest_state(source_entry, target_entry)
                || same_manifest_state(base_entry, source_entry)
            {
                continue;
            }
            ensure_merge_identity_compatible(
                table_key,
                base_entry.map(|entry| entry.identity),
                source_entry.map(|entry| entry.identity),
                target_entry.map(|entry| entry.identity),
            )?;
            let has_blob = schema_has_blob(&schema_for_table_key(catalog, table_key)?)?;
            if !has_blob {
                if same_manifest_state(base_entry, target_entry) {
                    if let Some(candidate) = classify_adopt(
                        self,
                        catalog,
                        base_snapshot,
                        source_snapshot,
                        target_snapshot,
                        table_key,
                        target_active.as_deref(),
                        &empty_external_preflight,
                    )
                    .await?
                    {
                        candidates.insert(table_key.clone(), candidate);
                    }
                } else if let Some(staged) = stage_streaming_table_merge(
                    self,
                    table_key,
                    catalog,
                    base_snapshot,
                    source_snapshot,
                    target_snapshot,
                    &mut conflicts,
                    &empty_external_preflight,
                )
                .await?
                {
                    candidates.insert(
                        table_key.clone(),
                        CandidateTableState::RewriteMerged(staged),
                    );
                }
                continue;
            }
            blob_table_keys.insert(table_key.clone());
            if same_manifest_state(base_entry, target_entry) {
                let Some(source_entry) = source_entry else {
                    continue;
                };
                if !adopt_advances_head(target_active.as_deref(), source_entry, target_entry) {
                    continue;
                }
                blob_adopt_proof_attempted.insert(table_key.clone());
                if let Some(proven) =
                    try_proven_pure_insert_history(table_key, base_snapshot, source_snapshot)
                        .await?
                {
                    let external_cells_before = blob_selection.external_cell_count();
                    materializer
                        .include_proven_insert_blob_selection(
                            &proven.source,
                            table_key,
                            proven.base_version,
                            proven.source_version,
                            proven.inserted_rows,
                            &mut blob_selection,
                        )
                        .await?;
                    if blob_selection.external_cell_count() == external_cells_before {
                        blob_pure_insert_histories.insert(table_key.clone(), proven);
                    } else {
                        // Supported insertion-capable writers materialize URI
                        // inputs, so a retained external descriptor here is a
                        // forged/legacy interval. Keep it on the general adopt
                        // writer: its bounded chunk cache reads each normalized
                        // range once. The certified planner intentionally scans
                        // its source twice (pre-arm chunk sizing + post-arm
                        // publish), which would duplicate external object GETs.
                        tracing::debug!(
                            table_key,
                            "pure-insert history retained external Blob descriptors; using the general cached adopt path"
                        );
                    }
                } else {
                    collect_adopt_blob_selection(
                        table_key,
                        base_snapshot,
                        source_snapshot,
                        &mut blob_selection,
                    )
                    .await?;
                }
            } else {
                collect_three_way_blob_selection(
                    table_key,
                    base_snapshot,
                    source_snapshot,
                    target_snapshot,
                    &mut conflicts,
                    &mut blob_selection,
                )
                .await?;
            }
        }
        if !conflicts.is_empty() {
            return Err(OmniError::MergeConflicts(conflicts));
        }

        let external_preflight = materializer
            .preflight_persisted_blob_selection(&blob_selection)
            .await?;
        let carried_blob_bytes = blob_selection.materialized_payload_bytes(&external_preflight)?;
        if carried_blob_bytes > KEYED_WRITE_MAX_BYTES {
            return Err(OmniError::resource_limit(
                "materialized blob payload bytes",
                KEYED_WRITE_MAX_BYTES,
                carried_blob_bytes,
            ));
        }

        for table_key in &ordered_table_keys {
            if !blob_table_keys.contains(table_key) {
                continue;
            }
            let base_entry = base_snapshot.dataset(table_key);
            let source_entry = source_snapshot.dataset(table_key);
            let target_entry = target_snapshot.dataset(table_key);
            if same_manifest_state(source_entry, target_entry) {
                continue;
            }
            if same_manifest_state(base_entry, source_entry) {
                continue;
            }
            // A branch may still point at a dropped incarnation while another
            // branch has the replacement under the same public alias. Row
            // merging across those physical lifetimes would silently adopt
            // stale data, so only the unchanged-source fast path above may
            // ignore the mismatch.
            ensure_merge_identity_compatible(
                table_key,
                base_entry.map(|entry| entry.identity),
                source_entry.map(|entry| entry.identity),
                target_entry.map(|entry| entry.identity),
            )?;
            if same_manifest_state(base_entry, target_entry) {
                let candidate = if blob_adopt_proof_attempted.contains(table_key) {
                    let advances_head = source_entry.is_some_and(|source_entry| {
                        adopt_advances_head(target_active.as_deref(), source_entry, target_entry)
                    });
                    if !advances_head {
                        return Err(OmniError::manifest_internal(format!(
                            "branch merge Blob proof for '{table_key}' lost its HEAD-advancing classification"
                        )));
                    }
                    let classified = match blob_pure_insert_histories.remove(table_key) {
                        Some(proven) => match finalize_proven_pure_insert_adopt(
                            self,
                            table_key,
                            proven,
                            &external_preflight,
                        )
                        .await?
                        {
                            Some(proven) => CandidateTableState::AdoptPureInserts(proven),
                            None => {
                                classify_general_adopt(
                                    self,
                                    catalog,
                                    base_snapshot,
                                    source_snapshot,
                                    table_key,
                                    true,
                                    &external_preflight,
                                )
                                .await?
                            }
                        },
                        None => {
                            classify_general_adopt(
                                self,
                                catalog,
                                base_snapshot,
                                source_snapshot,
                                table_key,
                                true,
                                &external_preflight,
                            )
                            .await?
                        }
                    };
                    // Blob-bearing tables reach the same net-zero shape, so
                    // they share the one candidate rule.
                    match source_entry {
                        Some(source_entry) => keep_publishing_candidate(
                            classified,
                            target_active.as_deref(),
                            source_entry,
                            target_entry,
                            table_key,
                        ),
                        // A blob adopt proof is only attempted for a table the
                        // source carries, so this arm is unreachable; keep the
                        // classified candidate rather than inventing a refusal.
                        None => Some(classified),
                    }
                } else {
                    classify_adopt(
                        self,
                        catalog,
                        base_snapshot,
                        source_snapshot,
                        target_snapshot,
                        table_key,
                        target_active.as_deref(),
                        &external_preflight,
                    )
                    .await?
                };
                if let Some(candidate) = candidate {
                    candidates.insert(table_key.clone(), candidate);
                }
                continue;
            }

            if let Some(staged) = stage_streaming_table_merge(
                self,
                table_key,
                catalog,
                base_snapshot,
                source_snapshot,
                target_snapshot,
                &mut conflicts,
                &external_preflight,
            )
            .await?
            {
                candidates.insert(
                    table_key.clone(),
                    CandidateTableState::RewriteMerged(staged),
                );
            }
        }

        if !conflicts.is_empty() {
            return Err(OmniError::MergeConflicts(conflicts));
        }

        // A narrow pure-insert fast-forward can avoid reconstructing the same
        // accepted source rows solely to recheck identity-backed @key. Any
        // value/enum/non-key-unique constraint, edge candidate, general adopt,
        // or real three-way merge retains the complete evaluator.
        let validation_is_redundant =
            is_fast_forward && proven_fast_forward_needs_no_validation(catalog, &candidates);
        if !validation_is_redundant {
            let validation_start = std::time::Instant::now();
            let changeset = build_merge_changeset(self, catalog, &candidates).await?;
            validate_merge_candidates(catalog, target_snapshot, &changeset).await?;
            crate::instrumentation::record_merge_timing(
                crate::instrumentation::MergeTimingPhase::CandidateValidation,
                validation_start.elapsed(),
            );
        }
        crate::failpoints::maybe_fail(
            crate::failpoints::names::BRANCH_MERGE_POST_CANDIDATE_VALIDATION,
        )?;

        // Recovery sidecar: protect the complete physical effect set. Every
        // `RewriteMerged` / `AdoptWithDelta` logical data step receives a
        // pre-minted, sequential Lance transaction identity and commits with
        // transparent retries disabled. Ref-only `AdoptSourceState` candidates
        // have no data transaction, but a first-touch native target ref is still
        // an independently durable v4 effect. Pointer-only manifest updates need
        // no physical pin; they remain in the complete intended delta so a
        // sibling effect's recovery publishes the merge atomically.
        //
        // This bridge still coexists with legacy maintenance writers that take
        // only `(table, branch)` queues. Acquire the conservative all-catalog
        // envelope for BOTH source and target, in the global sorted order, then
        // re-run the sidecar barrier and re-read both manifest branches before
        // Phase A. Planning remains outside table queues, but no plan derived
        // from a stale source/target snapshot can cross into physical effects.
        let final_revalidation_start = std::time::Instant::now();
        let active_branch_for_keys = target_branch.map(str::to_string);
        let merge_branches = [
            source_branch.map(str::to_string),
            active_branch_for_keys.clone(),
        ];
        let merge_queue_keys = self.table_queue_keys_for_branches(&merge_branches, catalog);
        let _merge_queue_guards = self.write_queue().acquire_many(&merge_queue_keys).await;

        self.ensure_no_pending_recovery_sidecars_under_gates(
            &[source_branch, target_branch],
            "branch_merge after acquiring source/target table gates",
        )
        .await?;
        // Re-read both inputs against one accepted schema contract. Target
        // authority remains exact; source head movement is harmless, but its
        // native ref incarnation and schema identity must remain stable.
        let (
            live_source_authority,
            live_source_snapshot,
            live_target_authority,
            fresh_target_snapshot,
        ) = self.revalidate_merge_inputs(source_txn, target_txn).await?;
        ensure_merge_target_authority_unchanged(target_txn, &live_target_authority)?;
        if target_snapshot.graph_manifest_version()
            != fresh_target_snapshot.graph_manifest_version()
        {
            return Err(OmniError::manifest_read_set_changed(
                format!("branch_merge_target:{}", target_branch.unwrap_or("main")),
                Some(target_snapshot.graph_manifest_version().to_string()),
                Some(fresh_target_snapshot.graph_manifest_version().to_string()),
            ));
        }
        // The comparison above checked the complete target authority
        // (branch incarnation, exact graph head, and schema identity), not only
        // this numeric manifest version. This is the same coarse token
        // mutation/load publish under.

        // Source is an immutable input, not part of the target CAS. A later
        // source-head advance is therefore harmless, but delete/recreate ABA is
        // not: the captured snapshot must still belong to the same native ref
        // incarnation and accepted schema contract.
        if live_source_authority.branch_identifier != source_txn.authority.branch_identifier {
            return Err(OmniError::manifest_read_set_changed(
                format!(
                    "branch_merge_source_incarnation:{}",
                    source_branch.unwrap_or("main")
                ),
                Some(
                    serde_json::to_string(&source_txn.authority.branch_identifier).map_err(
                        |error| {
                            OmniError::manifest_internal(format!(
                                "serialize captured source branch identifier: {error}"
                            ))
                        },
                    )?,
                ),
                Some(
                    serde_json::to_string(&live_source_authority.branch_identifier).map_err(
                        |error| {
                            OmniError::manifest_internal(format!(
                                "serialize live source branch identifier: {error}"
                            ))
                        },
                    )?,
                ),
            ));
        }
        if live_source_authority.schema_identity_domain
            != source_txn.authority.schema_identity_domain
            || live_source_authority.schema_ir_hash != source_txn.authority.schema_ir_hash
            || live_source_authority.schema_identity_version
                != source_txn.authority.schema_identity_version
        {
            return Err(OmniError::manifest_read_set_changed(
                "branch_merge_source_schema".to_string(),
                Some(format!(
                    "{}:{}:{}",
                    source_txn.authority.schema_identity_domain,
                    source_txn.authority.schema_identity_version,
                    source_txn.authority.schema_ir_hash
                )),
                Some(format!(
                    "{}:{}:{}",
                    live_source_authority.schema_identity_domain,
                    live_source_authority.schema_identity_version,
                    live_source_authority.schema_ir_hash
                )),
            ));
        }
        for (table_key, candidate) in &candidates {
            if let CandidateTableState::AdoptPureInserts(proven) = candidate {
                revalidate_proven_pure_insert_source(
                    self,
                    table_key,
                    proven,
                    &live_source_snapshot,
                )
                .await?;
            }
        }

        let expected_versions = candidates
            .keys()
            .filter_map(|table_key| {
                let identity = target_snapshot
                    .dataset(table_key)
                    .or_else(|| source_snapshot.dataset(table_key))
                    .or_else(|| base_snapshot.dataset(table_key))?
                    .identity;
                Some((
                    identity,
                    crate::db::manifest::TableVersionExpectation {
                        table_key: table_key.clone(),
                        table_version: target_snapshot
                            .dataset(table_key)
                            .map(|entry| entry.published_dataset_version)
                            .unwrap_or(0),
                    },
                ))
            })
            .collect::<crate::db::manifest::ExpectedTableVersions>();
        let mut merge_lineage = self
            .new_lineage_intent_for_branch(target_branch, actor_id)
            .await?;
        merge_lineage.merged_parent_commit_id = Some(source_head_commit_id.to_string());

        // Build the v9 recovery envelope (`protocol_v4` payload). Physical pins are a subset of the
        // complete intended manifest delta: pointer-only candidates have no
        // pre-authority effect, but must still be replayed if a sibling table's
        // durable effect forces recovery to finish the merge atomically.
        let mut recovery_pins = Vec::new();
        let mut recovery_effects = Vec::new();
        let mut delta_slots = Vec::new();
        let mut first_touch_effects = HashSet::new();
        let mut planned_transactions_by_table = HashMap::new();
        let mut prepared_existing_targets = HashMap::new();
        for table_key in &ordered_table_keys {
            let Some(candidate) = candidates.get(table_key) else {
                continue;
            };
            let source_entry = source_snapshot.dataset(table_key).ok_or_else(|| {
                OmniError::manifest(format!(
                    "branch merge candidate '{}' has no captured source entry",
                    table_key
                ))
            })?;
            let target_entry = target_snapshot.dataset(table_key);
            ensure_merge_identity_compatible(
                table_key,
                base_snapshot.dataset(table_key).map(|entry| entry.identity),
                Some(source_entry.identity),
                target_entry.map(|entry| entry.identity),
            )?;
            let identity = source_entry.identity;
            let expected_version = target_entry
                .map(|entry| entry.published_dataset_version)
                .unwrap_or(0);
            let planned_output_branch = match candidate {
                CandidateTableState::RewriteMerged(_)
                | CandidateTableState::AdoptWithDelta(_)
                | CandidateTableState::AdoptPureInserts(_) => active_branch_for_keys.clone(),
                CandidateTableState::AdoptSourceState { .. } => {
                    match (target_branch, source_entry.native_dataset_branch.as_deref()) {
                        (Some(target), Some(_)) => Some(target.to_string()),
                        _ => None,
                    }
                }
            };
            delta_slots.push(crate::db::manifest::RecoveryTableUpdateSlot {
                identity,
                table_key: table_key.clone(),
                expected_version,
                table_branch: planned_output_branch,
                confirmed: None,
            });

            match candidate {
                CandidateTableState::RewriteMerged(_)
                | CandidateTableState::AdoptWithDelta(_)
                | CandidateTableState::AdoptPureInserts(_) => {
                    let entry = target_entry.ok_or_else(|| {
                        OmniError::manifest(format!(
                            "HEAD-advancing branch merge candidate '{}' has no target entry",
                            table_key
                        ))
                    })?;
                    let source_fork_version = target_branch
                        .filter(|target| entry.native_dataset_branch.as_deref() != Some(*target))
                        .map(|_| entry.published_dataset_version);
                    if source_fork_version.is_some()
                        && matches!(candidate, CandidateTableState::AdoptPureInserts(_))
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "branch merge proven pure-insert candidate '{table_key}' cannot first-touch a lazy target"
                        )));
                    }
                    if source_fork_version.is_some() {
                        first_touch_effects.insert(table_key.clone());
                    } else {
                        // Existing-ref effects must prove that the physical
                        // baseline still equals the captured manifest pin
                        // before this merge writes a sidecar that claims the
                        // next versions. Keep the opened handle for Phase B so
                        // the post-arm path cannot reopen a different HEAD.
                        let (current, full_path, table_branch) = self
                            .open_for_mutation(table_key, crate::db::MutationOpKind::Merge)
                            .await?
                            .require_handle("branch merge existing-effect preflight");
                        prepared_existing_targets.insert(
                            table_key.clone(),
                            PreparedExistingMergeTarget {
                                current,
                                full_path,
                                table_branch,
                            },
                        );
                    }
                    let planned_transactions =
                        plan_merge_transactions(table_key, candidate, expected_version)?;
                    planned_transactions_by_table
                        .insert(table_key.clone(), planned_transactions.clone());
                    recovery_pins.push(crate::db::manifest::SidecarTablePin {
                        identity,
                        table_key: table_key.clone(),
                        table_path: self.storage().dataset_uri(&entry.dataset_path),
                        expected_version,
                        post_commit_pin: expected_version + 1,
                        confirmed_version: None,
                        table_branch: active_branch_for_keys.clone(),
                    });
                    recovery_effects.push(crate::db::manifest::RecoveryBranchMergeEffect {
                        identity,
                        table_key: table_key.clone(),
                        kind: crate::db::manifest::RecoveryBranchMergeEffectKind::MultiCommitHead {
                            source_fork_version,
                            planned_transactions,
                            confirmed_version: None,
                            confirmed_branch_identifier: None,
                        },
                    });
                }
                CandidateTableState::AdoptSourceState { .. } => {
                    let Some(target) = target_branch else {
                        continue;
                    };
                    let creates_target_ref = source_entry.native_dataset_branch.is_some()
                        && target_entry.and_then(|entry| entry.native_dataset_branch.as_deref())
                            != Some(target);
                    if !creates_target_ref {
                        continue;
                    }
                    first_touch_effects.insert(table_key.clone());
                    recovery_pins.push(crate::db::manifest::SidecarTablePin {
                        identity,
                        table_key: table_key.clone(),
                        table_path: self.storage().dataset_uri(&source_entry.dataset_path),
                        expected_version,
                        post_commit_pin: source_entry.published_dataset_version,
                        confirmed_version: None,
                        table_branch: Some(target.to_string()),
                    });
                    recovery_effects.push(crate::db::manifest::RecoveryBranchMergeEffect {
                        identity,
                        table_key: table_key.clone(),
                        kind: crate::db::manifest::RecoveryBranchMergeEffectKind::RefOnlyFork {
                            source_version: source_entry.published_dataset_version,
                            confirmed_branch_identifier: None,
                        },
                    });
                }
            }
        }

        // Probe after every existing target handle has been collected, at the
        // last pre-arm boundary under the complete gate envelope. First-touch
        // targets are intentionally absent: their native ref does not exist
        // until the sidecar below is durable.
        for table_key in &ordered_table_keys {
            let candidate = candidates.get(table_key);
            if let Some(prepared) = prepared_existing_targets.get(table_key) {
                if let Some(CandidateTableState::AdoptPureInserts(proven)) = candidate {
                    revalidate_proven_pure_insert_target_incarnation(
                        self,
                        table_key,
                        proven,
                        &prepared.current,
                    )
                    .await?;
                }
                let expected_version = target_snapshot
                    .dataset(table_key)
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "prepared branch merge target '{table_key}' has no manifest entry"
                        ))
                    })?
                    .published_dataset_version;
                self.ensure_existing_effect_baseline(
                    table_key,
                    prepared.table_branch.as_deref(),
                    expected_version,
                    &prepared.current,
                )
                .await?;
                continue;
            }
        }
        crate::instrumentation::record_merge_timing(
            crate::instrumentation::MergeTimingPhase::FinalRevalidation,
            final_revalidation_start.elapsed(),
        );

        // Keep the sidecar alongside its handle: after the whole physical
        // effect set completes, confirmation binds every output slot and every
        // first-touch ref identity before the one manifest CAS.
        let mut recovery: Option<(
            crate::db::manifest::RecoverySidecar,
            crate::db::manifest::RecoverySidecarHandle,
        )> = if recovery_pins.is_empty() {
            None
        } else {
            let authority = crate::db::manifest::RecoveryAuthorityToken {
                branch_identifier: target_txn.authority.branch_identifier.clone(),
                graph_head: target_txn.authority.graph_head.clone(),
                schema_identity_domain: target_txn.authority.schema_identity_domain.clone(),
                schema_ir_hash: target_txn.authority.schema_ir_hash.clone(),
                schema_identity_version: target_txn.authority.schema_identity_version,
            };
            let recovery_lineage = crate::db::manifest::RecoveryLineageIntent {
                graph_commit_id: merge_lineage.graph_commit_id.clone(),
                branch: merge_lineage.branch.clone(),
                actor_id: merge_lineage.actor_id.clone(),
                merged_parent_commit_id: merge_lineage.merged_parent_commit_id.clone(),
                created_at: merge_lineage.created_at,
            };
            let sidecar = crate::db::manifest::new_branch_merge_sidecar_v9(
                active_branch_for_keys.clone(),
                actor_id.map(str::to_string),
                recovery_pins,
                authority,
                recovery_lineage,
                recovery_effects,
                crate::db::manifest::RecoveryManifestDelta {
                    table_updates: delta_slots,
                    registrations: Vec::new(),
                    renames: Vec::new(),
                    tombstones: Vec::new(),
                },
            )?;
            let recovery_arm_start = std::time::Instant::now();
            let handle = crate::db::manifest::write_sidecar(
                self.root_uri(),
                self.storage_adapter(),
                &sidecar,
            )
            .await?;
            crate::instrumentation::record_merge_timing(
                crate::instrumentation::MergeTimingPhase::RecoveryArm,
                recovery_arm_start.elapsed(),
            );
            Some((sidecar, handle))
        };

        let recovery_operation_id = recovery
            .as_ref()
            .map(|(_, handle)| handle.operation_id.clone());
        // The armed publisher is a natural state-machine boundary: it owns the
        // physical effects, confirmation, and the one manifest publication.
        // Keep that large future out of the already broad planning/revalidation
        // future so debug builds do not stack both generated state machines'
        // transition storage while polling the substrate publisher.
        let post_arm_result = Box::pin(async {
            if recovery.is_some() && !first_touch_effects.is_empty() {
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::BRANCH_MERGE_POST_SIDECAR_PRE_FORK,
                )?;
            }

            let mut updates = Vec::new();
            let mut changed_edge_tables = false;
            let mut confirmed_ref_identifiers = HashMap::new();
            for table_key in &ordered_table_keys {
                let Some(candidate_state) = candidates.get(table_key) else {
                    continue;
                };
                let identity = source_snapshot
                    .dataset(table_key)
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "branch merge candidate '{table_key}' lost its source identity"
                        ))
                    })?
                    .identity;
                let prepared_target = match candidate_state {
                    CandidateTableState::RewriteMerged(_)
                    | CandidateTableState::AdoptWithDelta(_)
                    | CandidateTableState::AdoptPureInserts(_) => {
                        if first_touch_effects.contains(table_key) {
                            None
                        } else {
                            Some(prepared_existing_targets.remove(table_key).ok_or_else(|| {
                                OmniError::manifest_internal(format!(
                                    "branch merge table '{table_key}' lacks its verified existing target handle"
                                ))
                            })?)
                        }
                    }
                    CandidateTableState::AdoptSourceState { .. } => None,
                };
                let update = match candidate_state {
                    CandidateTableState::AdoptSourceState { .. } => {
                        publish_adopted_source_state(
                            self,
                            source_snapshot,
                            target_snapshot,
                            table_key,
                            target_active.as_deref(),
                        )
                        .await?
                    }
                    CandidateTableState::AdoptWithDelta(delta) => {
                        let planned = planned_transactions_by_table.get(table_key).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "branch merge table '{table_key}' lacks its armed transaction chain"
                            ))
                        })?;
                        publish_adopted_delta(
                            self,
                            table_key,
                            identity,
                            delta,
                            prepared_target,
                            planned,
                        )
                        .await?
                    }
                    CandidateTableState::AdoptPureInserts(proven) => {
                        let planned = planned_transactions_by_table.get(table_key).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "branch merge table '{table_key}' lacks its armed pure-insert transaction"
                            ))
                        })?;
                        let prepared_target = prepared_target.ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "branch merge pure-insert table '{table_key}' lacks its verified existing target handle"
                            ))
                        })?;
                        publish_proven_pure_insert_adopt(
                            self,
                            table_key,
                            identity,
                            proven,
                            &external_preflight,
                            prepared_target,
                            planned,
                        )
                        .await?
                    }
                    CandidateTableState::RewriteMerged(staged) => {
                        let planned = planned_transactions_by_table.get(table_key).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "branch merge table '{table_key}' lacks its armed transaction chain"
                            ))
                        })?;
                        publish_rewritten_merge_table(
                            self,
                            table_key,
                            identity,
                            staged,
                            prepared_target,
                            planned,
                        )
                        .await?
                    }
                };
                if first_touch_effects.contains(table_key) {
                    let target = target_branch.ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "first-touch merge effect '{}' has no named target branch",
                            table_key
                        ))
                    })?;
                    let entry = target_snapshot
                        .dataset(table_key)
                        .or_else(|| source_snapshot.dataset(table_key))
                        .ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "first-touch merge effect '{}' has no table path",
                                table_key
                            ))
                        })?;
                    let full_path = self.storage().dataset_uri(&entry.dataset_path);
                    let target_head = self
                        .storage()
                        .open_dataset_head(&full_path, Some(target))
                        .await?;
                    confirmed_ref_identifiers.insert(
                        update.identity,
                        self.storage().branch_identifier(&target_head).await?,
                    );
                }
                if table_key.starts_with("edge:") {
                    changed_edge_tables = true;
                }
                updates.push(update);
            }

            // The Armed body remains rollback-only until every physical effect,
            // every first-touch ref identity, and every logical output slot are
            // durably confirmed together.
            crate::failpoints::maybe_fail(
                crate::failpoints::names::BRANCH_MERGE_POST_EFFECTS_PRE_CONFIRM,
            )?;
            if let Some((sidecar, _)) = recovery.as_mut() {
                let recovery_confirm_start = std::time::Instant::now();
                crate::db::manifest::confirm_branch_merge_sidecar_v9(
                    self.root_uri(),
                    self.storage_adapter(),
                    sidecar,
                    &updates,
                    &confirmed_ref_identifiers,
                )
                .await?;
                crate::instrumentation::record_merge_timing(
                    crate::instrumentation::MergeTimingPhase::RecoveryConfirm,
                    recovery_confirm_start.elapsed(),
                );
            }

            crate::failpoints::maybe_fail(
                crate::failpoints::names::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            )?;

            // Publish the complete merge delta and its pre-minted lineage under
            // the exact target authority captured before classification.
            debug_assert_eq!(
                target_txn.effective_graph_head.as_deref(),
                Some(target_head_commit_id)
            );
            let manifest_publish_start = std::time::Instant::now();
            // The manifest publisher reaches Lance MergeInsert and DataFusion's
            // physical planner. Erase that substrate-heavy future at the graph
            // publication boundary instead of making this recovery envelope's
            // generated state carry it inline.
            Box::pin(self.commit_updates_on_branch_with_expected(
                target_branch,
                &updates,
                &expected_versions,
                actor_id,
                target_txn,
                merge_lineage,
            ))
            .await?;
            crate::instrumentation::record_merge_timing(
                crate::instrumentation::MergeTimingPhase::ManifestPublish,
                manifest_publish_start.elapsed(),
            );

            Ok::<_, OmniError>((updates, changed_edge_tables))
        })
        .await;
        let (_updates, changed_edge_tables) = match post_arm_result {
            Ok(result) => result,
            Err(error) => {
                return match recovery_operation_id {
                    Some(operation_id) => Err(OmniError::recovery_required(
                        operation_id,
                        error.to_string(),
                    )),
                    None => Err(error),
                };
            }
        };

        // Recovery sidecar lifecycle: delete after the manifest publish (Phase C).
        // Best-effort cleanup; the merge already landed durably so failing the
        // user here is undesirable.
        if let Some((_, handle)) = recovery {
            let recovery_cleanup_start = std::time::Instant::now();
            if let Err(err) =
                crate::db::manifest::delete_sidecar(&handle, self.storage_adapter()).await
            {
                tracing::warn!(
                    error = %err,
                    operation_id = handle.operation_id.as_str(),
                    "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
                );
            }
            crate::instrumentation::record_merge_timing(
                crate::instrumentation::MergeTimingPhase::RecoveryCleanup,
                recovery_cleanup_start.elapsed(),
            );
        }

        if changed_edge_tables {
            self.invalidate_graph_index().await;
        }

        Ok(if is_fast_forward {
            MergeOutcome::FastForward
        } else {
            MergeOutcome::Merged
        })
    }
}
