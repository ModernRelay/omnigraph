use super::*;

const MERGE_STAGE_BATCH_ROWS: usize = 8192;
const MERGE_STAGE_DIR_ENV: &str = "OMNIGRAPH_MERGE_STAGING_DIR";

#[derive(Debug)]
enum CandidateTableState {
    /// Adopt the source's table state via a pointer switch or a branch fork —
    /// no data HEAD advance, so nothing to pin for recovery.
    AdoptSourceState,
    /// Adopt the source's state by applying a non-empty delta onto the target's
    /// lineage (append new + upsert changed + delete removed). The delta is
    /// pre-computed at classification so this candidate can be recovery-pinned:
    /// its publish advances Lance HEAD before the manifest commit.
    AdoptWithDelta(AdoptDelta),
    RewriteMerged(StagedMergeResult),
}

#[derive(Debug)]
struct StagedTable {
    _dir: TempDir,
    dataset: Dataset,
}

#[derive(Debug)]
struct StagedMergeResult {
    full_staged: StagedTable,
    delta_staged: Option<StagedTable>,
    deleted_ids: Vec<String>,
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
    /// New-on-source rows → `stage_append` (a streaming `Operation::Append`, no
    /// hash join). The connector's dominant case and the OOM fix: appending new
    /// rows never buffers the whole delta in a full-outer hash join.
    appends: Option<StagedTable>,
    /// Changed-on-source rows → `stage_merge_insert` (a hash join bounded to the
    /// genuinely-changed set, not the whole delta).
    upserts: Option<StagedTable>,
    deleted_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct CursorRow {
    id: String,
    signature: String,
    dataset: Dataset,
    batch: RecordBatch,
    row_index: usize,
}

impl CursorRow {
    /// Compute this row's signature on demand. Used by the lazy adopt cursor,
    /// where `signature` is left empty; the value is identical to the eager
    /// `signature` field the three-way cursor populates.
    fn compute_signature(&self) -> Result<String> {
        row_signature(&self.batch, self.row_index)
    }
}

struct OrderedTableCursor {
    stream: Option<std::pin::Pin<Box<DatasetRecordBatchStream>>>,
    dataset: Option<Dataset>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    peeked: Option<CursorRow>,
    /// When false, `next_row` leaves `CursorRow::signature` empty and callers
    /// compute it on demand via `CursorRow::compute_signature`. The adopt path
    /// uses this: new/deleted rows never need a signature comparison and would
    /// otherwise eagerly stringify their embedding for nothing.
    eager_signatures: bool,
}

impl OrderedTableCursor {
    async fn from_snapshot(snapshot: &Snapshot, table_key: &str) -> Result<Self> {
        Self::open(snapshot, table_key, true).await
    }

    /// Like `from_snapshot` but leaves row signatures uncomputed (callers use
    /// `CursorRow::compute_signature` on demand). See `eager_signatures`.
    async fn from_snapshot_lazy(snapshot: &Snapshot, table_key: &str) -> Result<Self> {
        Self::open(snapshot, table_key, false).await
    }

    async fn open(snapshot: &Snapshot, table_key: &str, eager_signatures: bool) -> Result<Self> {
        let dataset = match snapshot.entry(table_key) {
            Some(_) => Some(snapshot.open(table_key).await?),
            None => None,
        };
        Self::from_dataset(dataset, eager_signatures).await
    }

    async fn from_dataset(dataset: Option<Dataset>, eager_signatures: bool) -> Result<Self> {
        let stream = if let Some(ds) = &dataset {
            Some(Box::pin(
                crate::table_store::TableStore::scan_stream_with(
                    ds,
                    None,
                    None,
                    Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                    true,
                    |_| Ok(()),
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
                    let signature = if self.eager_signatures {
                        row_signature(batch, row_index)?
                    } else {
                        String::new()
                    };
                    return Ok(Some(CursorRow {
                        id: row_id_at(batch, row_index)?,
                        signature,
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
                Err(err) => return Err(OmniError::Lance(err.to_string())),
            }
        }
    }
}

struct StagedTableWriter {
    schema: SchemaRef,
    dataset_uri: String,
    dir: TempDir,
    dataset: Option<Dataset>,
    buffered_rows: usize,
    row_count: u64,
    batches: Vec<RecordBatch>,
}

impl StagedTableWriter {
    fn new(table_key: &str, schema: SchemaRef) -> Result<Self> {
        let dir = merge_stage_tempdir(table_key)?;
        let dataset_uri = dir.path().join("table.lance").to_string_lossy().to_string();
        Ok(Self {
            schema,
            dataset_uri,
            dir,
            dataset: None,
            buffered_rows: 0,
            row_count: 0,
            batches: Vec::new(),
        })
    }

    async fn push_row(&mut self, row: &CursorRow) -> Result<()> {
        self.row_count += 1;
        self.buffered_rows += 1;
        self.batches.push(self.row_batch(row).await?);
        if self.buffered_rows >= MERGE_STAGE_BATCH_ROWS {
            self.flush().await?;
        }
        Ok(())
    }

    async fn row_batch(&self, row: &CursorRow) -> Result<RecordBatch> {
        let batch = row.batch.slice(row.row_index, 1);
        let has_blob_columns = row
            .dataset
            .schema()
            .fields_pre_order()
            .any(|field| field.is_blob());
        if has_blob_columns {
            return crate::table_store::TableStore::materialize_blob_batch(&row.dataset, batch)
                .await;
        }
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                    OmniError::Lance(format!("batch missing column '{}'", field.name()))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| OmniError::Lance(e.to_string()))
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
                .map_err(|e| OmniError::Lance(e.to_string()))?
        };
        self.buffered_rows = 0;

        let ds = crate::table_store::TableStore::append_or_create_batch(
            &self.dataset_uri,
            self.dataset.take(),
            batch,
        )
        .await?;
        self.dataset = Some(ds);
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
    table_key: &str,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
) -> Result<Option<AdoptDelta>> {
    let schema = schema_for_table_key(catalog, table_key)?;
    let mut append_writer =
        StagedTableWriter::new(&format!("{}_adopt_append", table_key), schema.clone())?;
    let mut upsert_writer =
        StagedTableWriter::new(&format!("{}_adopt_upsert", table_key), schema)?;
    let mut deleted_ids: Vec<String> = Vec::new();
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
                deleted_ids.push(next_id);
                needs_update = true;
            }
            (None, Some(src)) => {
                // New on source → append (streaming, no hash join). No signature
                // needed — a new id is absent from base by construction.
                append_writer.push_row(src).await?;
                needs_update = true;
            }
            (Some(base), Some(src)) => {
                // Present on both — compute signatures lazily (the only case
                // that needs them) to tell a changed row from an unchanged one.
                // New/deleted rows above skip the embedding stringify entirely.
                if src.compute_signature()? != base.compute_signature()? {
                    // Changed on source → upsert.
                    upsert_writer.push_row(src).await?;
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

    let appends = if append_writer.row_count > 0 {
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
        appends,
        upserts,
        deleted_ids,
    }))
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
    table_key: &str,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    conflicts: &mut Vec<MergeConflict>,
) -> Result<Option<StagedMergeResult>> {
    let schema = schema_for_table_key(catalog, table_key)?;
    let mut full_writer = StagedTableWriter::new(&format!("{}_full", table_key), schema.clone())?;
    let mut delta_writer = StagedTableWriter::new(&format!("{}_delta", table_key), schema)?;
    let mut deleted_ids: Vec<String> = Vec::new();
    let mut base = OrderedTableCursor::from_snapshot(base_snapshot, table_key).await?;
    let mut source = OrderedTableCursor::from_snapshot(source_snapshot, table_key).await?;
    let mut target = OrderedTableCursor::from_snapshot(target_snapshot, table_key).await?;

    let prior_conflict_count = conflicts.len();
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

        let base_sig = base_row.as_ref().map(|row| row.signature.as_str());
        let source_sig = source_row.as_ref().map(|row| row.signature.as_str());
        let target_sig = target_row.as_ref().map(|row| row.signature.as_str());

        let source_changed = source_sig != base_sig;
        let target_changed = target_sig != base_sig;

        let selection = if !source_changed {
            target_row.as_ref()
        } else if !target_changed {
            source_row.as_ref()
        } else if source_sig == target_sig {
            target_row.as_ref()
        } else {
            conflicts.push(classify_merge_conflict(
                table_key, &next_id, base_sig, source_sig, target_sig,
            ));
            None
        };

        if conflicts.len() > prior_conflict_count {
            continue;
        }

        // Row existed in target but not in merge result → delete
        if selection.is_none() && target_row.is_some() {
            deleted_ids.push(next_id.clone());
            needs_update = true;
            continue;
        }

        if let Some(selection) = selection {
            // Always write to full (for validation)
            full_writer.push_row(selection).await?;
            // Only write changed rows to delta (for publish)
            if selection.signature.as_str() != target_sig.unwrap_or("") {
                delta_writer.push_row(selection).await?;
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
        full_staged: full_writer.finish().await?,
        delta_staged,
        deleted_ids,
    }))
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

fn same_manifest_state(
    left: Option<&crate::db::SubTableEntry>,
    right: Option<&crate::db::SubTableEntry>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            left.table_version == right.table_version && left.table_branch == right.table_branch
        }
        (None, None) => true,
        _ => false,
    }
}

fn classify_merge_conflict(
    table_key: &str,
    row_id: &str,
    base_sig: Option<&str>,
    source_sig: Option<&str>,
    target_sig: Option<&str>,
) -> MergeConflict {
    let (kind, message) = match (base_sig, source_sig, target_sig) {
        (None, Some(_), Some(_)) => (
            MergeConflictKind::DivergentInsert,
            format!("divergent insert for id '{}'", row_id),
        ),
        (Some(_), None, Some(_)) | (Some(_), Some(_), None) => (
            MergeConflictKind::DeleteVsUpdate,
            format!("delete/update conflict for id '{}'", row_id),
        ),
        _ => (
            MergeConflictKind::DivergentUpdate,
            format!("divergent update for id '{}'", row_id),
        ),
    };
    MergeConflict {
        table_key: table_key.to_string(),
        row_id: Some(row_id.to_string()),
        kind,
        message,
    }
}

fn row_signature(batch: &RecordBatch, row: usize) -> Result<String> {
    let mut values = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if field.name().starts_with("_row") {
            continue;
        }
        values.push(
            array_value_to_string(column.as_ref(), row)
                .map_err(|e| OmniError::Lance(e.to_string()))?,
        );
    }
    Ok(values.join("\u{1f}"))
}

async fn scan_validation_stream(ds: &Dataset) -> Result<DatasetRecordBatchStream> {
    crate::table_store::TableStore::scan_stream_with(ds, None, None, None, false, |_| Ok(())).await
}

async fn validate_merge_candidates(
    db: &Omnigraph,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    candidates: &HashMap<String, CandidateTableState>,
) -> Result<()> {
    let mut conflicts = Vec::new();
    let mut node_ids: HashMap<String, HashSet<String>> = HashMap::new();

    for (type_name, node_type) in &db.catalog().node_types {
        let table_key = format!("node:{}", type_name);
        let mut values = HashSet::new();
        let mut unique_seen = vec![HashMap::new(); node_type.unique_constraints.len()];

        if let Some(ds) =
            candidate_dataset(source_snapshot, target_snapshot, candidates, &table_key).await?
        {
            let mut stream = scan_validation_stream(&ds).await?;
            while let Some(batch) = stream
                .try_next()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
            {
                if let Err(err) = crate::loader::validate_value_constraints(&batch, node_type) {
                    conflicts.push(MergeConflict {
                        table_key: table_key.clone(),
                        row_id: None,
                        kind: MergeConflictKind::ValueConstraintViolation,
                        message: err.to_string(),
                    });
                }
                update_unique_constraints(
                    &table_key,
                    &batch,
                    &node_type.unique_constraints,
                    &mut unique_seen,
                    &mut conflicts,
                )?;
                let ids = batch
                    .column_by_name("id")
                    .ok_or_else(|| {
                        OmniError::manifest(format!("table {} missing id column", table_key))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        OmniError::manifest(format!("table {} id column is not Utf8", table_key))
                    })?;
                for row in 0..ids.len() {
                    values.insert(ids.value(row).to_string());
                }
            }
        }
        node_ids.insert(type_name.clone(), values);
    }

    for (edge_name, edge_type) in &db.catalog().edge_types {
        let table_key = format!("edge:{}", edge_name);
        let mut unique_seen = vec![HashMap::new(); edge_type.unique_constraints.len()];
        let mut src_counts = HashMap::new();

        if let Some(ds) =
            candidate_dataset(source_snapshot, target_snapshot, candidates, &table_key).await?
        {
            let mut stream = scan_validation_stream(&ds).await?;
            while let Some(batch) = stream
                .try_next()
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
            {
                update_unique_constraints(
                    &table_key,
                    &batch,
                    &edge_type.unique_constraints,
                    &mut unique_seen,
                    &mut conflicts,
                )?;
                accumulate_edge_cardinality(&batch, &mut src_counts, &table_key)?;
                conflicts.extend(validate_orphan_edges_batch(
                    &table_key, edge_type, &batch, &node_ids,
                )?);
            }
        }

        conflicts.extend(finalize_edge_cardinality_conflicts(
            &table_key,
            edge_name,
            edge_type.cardinality.min,
            edge_type.cardinality.max,
            src_counts,
        ));
    }

    if conflicts.is_empty() {
        Ok(())
    } else {
        Err(OmniError::MergeConflicts(conflicts))
    }
}

async fn candidate_dataset(
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    candidates: &HashMap<String, CandidateTableState>,
    table_key: &str,
) -> Result<Option<Dataset>> {
    if let Some(candidate) = candidates.get(table_key) {
        return match candidate {
            CandidateTableState::AdoptSourceState | CandidateTableState::AdoptWithDelta(_) => {
                match source_snapshot.entry(table_key) {
                    Some(_) => Ok(Some(source_snapshot.open(table_key).await?)),
                    None => Ok(None),
                }
            }
            CandidateTableState::RewriteMerged(staged) => {
                Ok(Some(staged.full_staged.dataset.clone()))
            }
        };
    }
    match target_snapshot.entry(table_key) {
        Some(_) => Ok(Some(target_snapshot.open(table_key).await?)),
        None => Ok(None),
    }
}

fn update_unique_constraints(
    table_key: &str,
    batch: &RecordBatch,
    constraints: &[Vec<String>],
    seen: &mut [HashMap<Vec<String>, String>],
    conflicts: &mut Vec<MergeConflict>,
) -> Result<()> {
    for (constraint_idx, columns) in constraints.iter().enumerate() {
        let seen = &mut seen[constraint_idx];
        // Resolve the group's columns once. The candidate dataset always
        // carries the full table schema, so a missing column is an internal
        // error rather than a skip.
        let group_columns = columns
            .iter()
            .map(|column_name| {
                batch.column_by_name(column_name).cloned().ok_or_else(|| {
                    OmniError::manifest(format!(
                        "table {} missing unique column '{}'",
                        table_key, column_name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            // Same tuple key as the intake path — one shared derivation in
            // `crate::loader::composite_unique_key`, so the two cannot drift on
            // separator or scalar conversion. Null rows are exempt.
            let Some(key) = crate::loader::composite_unique_key(&group_columns, row)? else {
                continue;
            };
            let row_id = row_id_at(batch, row)?;
            if let Some(first_row_id) = seen.insert(key, row_id.clone()) {
                conflicts.push(MergeConflict {
                    table_key: table_key.to_string(),
                    row_id: Some(row_id.clone()),
                    kind: MergeConflictKind::UniqueViolation,
                    message: format!(
                        "unique constraint {:?} violated by '{}' and '{}'",
                        columns, first_row_id, row_id
                    ),
                });
            }
        }
    }
    Ok(())
}

fn accumulate_edge_cardinality(
    batch: &RecordBatch,
    counts: &mut HashMap<String, u32>,
    table_key: &str,
) -> Result<()> {
    let srcs = batch
        .column_by_name("src")
        .ok_or_else(|| OmniError::manifest(format!("table {} missing src column", table_key)))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest(format!("table {} src column is not Utf8", table_key))
        })?;
    for row in 0..srcs.len() {
        *counts.entry(srcs.value(row).to_string()).or_insert(0_u32) += 1;
    }
    Ok(())
}

fn finalize_edge_cardinality_conflicts(
    table_key: &str,
    edge_name: &str,
    min: u32,
    max: Option<u32>,
    counts: HashMap<String, u32>,
) -> Vec<MergeConflict> {
    let mut conflicts = Vec::new();
    for (src, count) in counts {
        if let Some(max) = max {
            if count > max {
                conflicts.push(MergeConflict {
                    table_key: table_key.to_string(),
                    row_id: None,
                    kind: MergeConflictKind::CardinalityViolation,
                    message: format!(
                        "@card violation on edge {}: source '{}' has {} edges (max {})",
                        edge_name, src, count, max
                    ),
                });
            }
        }
        if count < min {
            conflicts.push(MergeConflict {
                table_key: table_key.to_string(),
                row_id: None,
                kind: MergeConflictKind::CardinalityViolation,
                message: format!(
                    "@card violation on edge {}: source '{}' has {} edges (min {})",
                    edge_name, src, count, min
                ),
            });
        }
    }
    conflicts
}

fn validate_orphan_edges_batch(
    table_key: &str,
    edge_type: &omnigraph_compiler::catalog::EdgeType,
    batch: &RecordBatch,
    node_ids: &HashMap<String, HashSet<String>>,
) -> Result<Vec<MergeConflict>> {
    let srcs = batch
        .column_by_name("src")
        .ok_or_else(|| OmniError::manifest(format!("table {} missing src column", table_key)))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest(format!("table {} src column is not Utf8", table_key))
        })?;
    let dsts = batch
        .column_by_name("dst")
        .ok_or_else(|| OmniError::manifest(format!("table {} missing dst column", table_key)))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest(format!("table {} dst column is not Utf8", table_key))
        })?;

    let from_ids = node_ids.get(&edge_type.from_type).ok_or_else(|| {
        OmniError::manifest(format!(
            "missing candidate node ids for {}",
            edge_type.from_type
        ))
    })?;
    let to_ids = node_ids.get(&edge_type.to_type).ok_or_else(|| {
        OmniError::manifest(format!(
            "missing candidate node ids for {}",
            edge_type.to_type
        ))
    })?;

    let mut conflicts = Vec::new();
    for row in 0..batch.num_rows() {
        let row_id = row_id_at(batch, row)?;
        let src = srcs.value(row);
        let dst = dsts.value(row);
        if !from_ids.contains(src) {
            conflicts.push(MergeConflict {
                table_key: table_key.to_string(),
                row_id: Some(row_id.clone()),
                kind: MergeConflictKind::OrphanEdge,
                message: format!("src '{}' not found in {}", src, edge_type.from_type),
            });
        }
        if !to_ids.contains(dst) {
            conflicts.push(MergeConflict {
                table_key: table_key.to_string(),
                row_id: Some(row_id),
                kind: MergeConflictKind::OrphanEdge,
                message: format!("dst '{}' not found in {}", dst, edge_type.to_type),
            });
        }
    }
    Ok(conflicts)
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

/// Classify a table whose target state equals base (the adopt / fast-forward
/// case). Returns [`CandidateTableState::AdoptWithDelta`] — with the delta
/// pre-computed so it can be recovery-pinned — when the adopt applies a
/// non-empty delta onto the target's lineage (a HEAD-advancing publish via
/// [`publish_adopted_delta`]); otherwise [`CandidateTableState::AdoptSourceState`]
/// (a pointer switch or fork, which does not advance the data HEAD).
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
) -> Result<CandidateTableState> {
    let Some(source_entry) = source_snapshot.entry(table_key) else {
        return Ok(CandidateTableState::AdoptSourceState);
    };
    let target_entry = target_snapshot.entry(table_key);
    let target_active = target_db.active_branch().await;
    let advances_head = match (
        target_active.as_deref(),
        source_entry.table_branch.as_deref(),
    ) {
        // Source on a branch, target on main — delta applied onto main's lineage.
        (None, Some(_)) => true,
        // Both on branches, target owns this table — delta applied onto it.
        (Some(target_branch), Some(_)) => {
            target_entry.and_then(|e| e.table_branch.as_deref()) == Some(target_branch)
        }
        // Source on main (pointer switch) or target doesn't own (fork): no advance.
        _ => false,
    };
    if !advances_head {
        return Ok(CandidateTableState::AdoptSourceState);
    }
    match compute_adopt_delta(table_key, catalog, base_snapshot, source_snapshot).await? {
        Some(delta) => Ok(CandidateTableState::AdoptWithDelta(delta)),
        None => Ok(CandidateTableState::AdoptSourceState),
    }
}

/// Adopt the source's table state without applying a row delta: a pointer
/// switch (source/target share lineage) or a branch fork. The HEAD-advancing
/// delta case is classified [`CandidateTableState::AdoptWithDelta`] and
/// published by [`publish_adopted_delta`], so reaching the branch-bearing arms
/// here means the delta was empty.
async fn publish_adopted_source_state(
    target_db: &Omnigraph,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    table_key: &str,
) -> Result<crate::db::SubTableUpdate> {
    let source_entry = source_snapshot
        .entry(table_key)
        .ok_or_else(|| OmniError::manifest(format!("missing source entry for {}", table_key)))?;
    let target_entry = target_snapshot.entry(table_key);

    let target_active = target_db.active_branch().await;
    match (
        target_active.as_deref(),
        source_entry.table_branch.as_deref(),
    ) {
        // Both on main — pointer switch is safe (same lineage, version columns valid)
        (None, None) => Ok(crate::db::SubTableUpdate {
            table_key: table_key.to_string(),
            table_version: source_entry.table_version,
            table_branch: None,
            row_count: source_entry.row_count,
            version_metadata: source_entry.version_metadata.clone(),
        }),
        // Source on main, target on branch — pointer switch to main version
        // (target reads from main, same lineage)
        (Some(_target_branch), None) => Ok(crate::db::SubTableUpdate {
            table_key: table_key.to_string(),
            table_version: source_entry.table_version,
            table_branch: None,
            row_count: source_entry.row_count,
            version_metadata: source_entry.version_metadata.clone(),
        }),
        // Source on branch, target on main, empty delta — adopt source's
        // version by a pointer switch (the non-empty case is `AdoptWithDelta`).
        (None, Some(_source_branch)) => Ok(crate::db::SubTableUpdate {
            table_key: table_key.to_string(),
            table_version: target_entry
                .map(|e| e.table_version)
                .unwrap_or(source_entry.table_version),
            table_branch: None,
            row_count: source_entry.row_count,
            version_metadata: target_entry
                .map(|entry| entry.version_metadata.clone())
                .unwrap_or_else(|| source_entry.version_metadata.clone()),
        }),
        // Both on branches
        (Some(target_branch), Some(source_branch)) => {
            if target_entry.and_then(|entry| entry.table_branch.as_deref()) == Some(target_branch) {
                // Target already owns this table, empty delta — pointer switch
                // onto its own lineage (the non-empty case is `AdoptWithDelta`).
                Ok(crate::db::SubTableUpdate {
                    table_key: table_key.to_string(),
                    table_version: target_entry.unwrap().table_version,
                    table_branch: Some(target_branch.to_string()),
                    row_count: source_entry.row_count,
                    version_metadata: target_entry.unwrap().version_metadata.clone(),
                })
            } else {
                // Target doesn't own this table yet — fork from source state.
                // This creates the target branch on the sub-table dataset.
                let full_path = format!("{}/{}", target_db.uri(), source_entry.table_path);
                let ds = target_db
                    .fork_dataset_from_entry_state(
                        table_key,
                        &full_path,
                        Some(source_branch),
                        source_entry.table_version,
                        target_branch,
                    )
                    .await?;
                let state = target_db.storage().table_state(&full_path, &ds).await?;
                Ok(crate::db::SubTableUpdate {
                    table_key: table_key.to_string(),
                    table_version: state.version,
                    table_branch: Some(target_branch.to_string()),
                    row_count: state.row_count,
                    version_metadata: state.version_metadata,
                })
            }
        }
    }
}

async fn publish_rewritten_merge_table(
    target_db: &Omnigraph,
    table_key: &str,
    staged: &StagedMergeResult,
) -> Result<crate::db::SubTableUpdate> {
    // Branch merge's source-rewrite path is Merge-shaped (upsert from
    // source onto target). The inline `delete_where` later in this
    // function operates on rows the rewrite chose to remove, not
    // user-facing predicates, so Merge is the correct policy here.
    // `open_for_mutation` is the no-txn entry, so collapse #1's non-strict
    // open-skip (gated on `txn.is_some()`) never fires here — the handle is
    // always `Some`.
    let (ds, _expected_version, full_path, table_branch) = target_db
        .open_for_mutation(table_key, crate::db::MutationOpKind::Merge)
        .await?;
    let mut current_ds = ds.expect("no-txn open_for_mutation always opens its dataset");

    // Phase 1: merge_insert changed/new rows (preserves _row_created_at_version for
    // existing rows, bumps _row_last_updated_at_version only for actually-changed rows).
    //
    // Routed through the staged primitive so a failure between writing
    // fragments and committing leaves no Lance-HEAD drift. The
    // commit_staged here is per-table per-call (Lance has no
    // multi-dataset atomic commit); the residual sits at this single
    // commit point, narrowed from the previous "merge_insert + delete +
    // index" multi-step inline-commit chain.
    if let Some(delta) = &staged.delta_staged {
        // The staged delta dataset is a temp-dir Lance dataset used only
        // to collect the rewrite batches; wrap it in a `SnapshotHandle`
        // so we can route through the trait's `scan_batches_for_rewrite`.
        let delta_snapshot = SnapshotHandle::new(delta.dataset.clone());
        let batches: Vec<RecordBatch> = target_db
            .storage()
            .scan_batches_for_rewrite(&delta_snapshot)
            .await?
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        if !batches.is_empty() {
            // Concat into one batch — stage_merge_insert takes a single batch.
            let combined = if batches.len() == 1 {
                batches.into_iter().next().unwrap()
            } else {
                let schema = batches[0].schema();
                arrow_select::concat::concat_batches(&schema, &batches)
                    .map_err(|e| OmniError::Lance(e.to_string()))?
            };
            let staged_merge = target_db
                .storage()
                .stage_merge_insert(
                    current_ds.clone(),
                    combined,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::InsertAll,
                )
                .await?;
            current_ds = target_db
                .storage()
                .commit_staged(current_ds, staged_merge)
                .await?;
        }
    }

    // Failpoint: crash after the Phase 1 merge_insert commit, before the delete.
    // Models a partial Phase B on the three-way path — the merged constructive
    // rows are on Lance HEAD but the delete has not committed and the
    // achieved-version intent has not been recorded, so recovery must roll BACK.
    // See tests/failpoints.rs::branch_merge_rewrite_partial_after_merge_rolls_back.
    crate::failpoints::maybe_fail("branch_merge.rewrite_after_merge_pre_delete")?;

    // Phase 2: delete removed rows via deletion vectors.
    //
    // INLINE-COMMIT RESIDUAL: lance-6.0.1 does not expose a public
    // two-phase delete API (DeleteJob is `pub(crate)` —
    // lance-format/lance#6658 is open with no PRs). We deliberately do
    // NOT introduce a `stage_delete` wrapper that would secretly
    // inline-commit (it would create a side-channel between the staged
    // and inline write paths). When the upstream API ships, swap this
    // `delete_where` call for `stage_delete` + `commit_staged`.
    if !staged.deleted_ids.is_empty() {
        let escaped: Vec<String> = staged
            .deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let filter = format!("id IN ({})", escaped.join(", "));
        let (new_ds, _) = target_db
            .storage_inline_residual()
            .delete_where(&full_path, current_ds, &filter)
            .await?;
        current_ds = new_ds;
    }

    // Failpoint: crash after the Phase 2 delete commit, before the index build.
    // Models a partial Phase B on the three-way path — constructive rows +
    // deletes are on Lance HEAD but the achieved-version intent has not been
    // recorded, so recovery must roll BACK (the index is reconciler-owned derived
    // state, but the merge itself never reached its commit boundary). See
    // tests/failpoints.rs::branch_merge_rewrite_partial_after_delete_rolls_back.
    crate::failpoints::maybe_fail("branch_merge.rewrite_after_delete_pre_index")?;

    // Phase 3: rebuild indices.
    //
    // `build_indices_on_dataset` uses `stage_create_btree_index` /
    // `stage_create_inverted_index` + `commit_staged` for scalar
    // indices. Vector indices remain inline-commit
    // (`build_index_metadata_from_segments` is `pub(crate)` in lance-
    // 6.0.1 — companion ticket to lance-format/lance#6666).
    let row_count = target_db
        .storage()
        .table_state(&full_path, &current_ds)
        .await?
        .row_count;
    if row_count > 0 {
        target_db
            .build_indices_on_dataset(table_key, &mut current_ds)
            .await?;
    }
    let final_state = target_db
        .storage()
        .table_state(&full_path, &current_ds)
        .await?;

    Ok(crate::db::SubTableUpdate {
        table_key: table_key.to_string(),
        table_version: final_state.version,
        table_branch,
        row_count: final_state.row_count,
        version_metadata: final_state.version_metadata,
    })
}

/// Scan a staged temp table and concat its non-empty batches into the single
/// batch that `stage_append` / `stage_merge_insert` consume. Returns `None` when
/// the table has no rows (both staged primitives reject an empty batch).
async fn scan_staged_combined(
    target_db: &Omnigraph,
    table: &StagedTable,
) -> Result<Option<RecordBatch>> {
    crate::instrumentation::record_scan_staged_combined();
    let snapshot = SnapshotHandle::new(table.dataset.clone());
    let batches: Vec<RecordBatch> = target_db
        .storage()
        .scan_batches_for_rewrite(&snapshot)
        .await?
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect();
    if batches.is_empty() {
        return Ok(None);
    }
    let combined = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| OmniError::Lance(e.to_string()))?
    };
    Ok(Some(combined))
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
/// multi-commit append → upsert → delete publish here (the source of the
/// partial-Phase-B recovery window the sidecar confirmation guards) collapses to
/// a single fragment-graft commit per table, so this whole function goes away.
async fn publish_adopted_delta(
    target_db: &Omnigraph,
    table_key: &str,
    delta: &AdoptDelta,
) -> Result<crate::db::SubTableUpdate> {
    // `open_for_mutation` is the no-txn entry, so collapse #1's non-strict
    // open-skip (gated on `txn.is_some()`) never fires here — the handle is
    // always `Some`.
    let (ds, _expected_version, full_path, table_branch) = target_db
        .open_for_mutation(table_key, crate::db::MutationOpKind::Merge)
        .await?;
    let mut current_ds = ds.expect("no-txn open_for_mutation always opens its dataset");

    // Phase 1a: append the NEW rows. `stage_append_stream` is a streaming
    // `Operation::Append` — no hash join — so it never buffers the delta and
    // cannot exhaust the DataFusion memory pool (the OOM fix). It streams the
    // staged rows straight into the target (Lance rolls fragments at
    // `max_rows_per_file`), so memory is bounded regardless of how many rows the
    // connector appended — never the whole set in one batch. New ids are absent
    // from base by construction (the ordered walk only classifies a row
    // `(None, Some)` when base lacks it), so they never collide on `id`. Routed
    // through the staged primitive so a failure between writing fragments and
    // committing leaves no Lance-HEAD drift. `appends` is `Some` only when the
    // staged table is non-empty (`compute_adopt_delta`).
    if let Some(append_table) = &delta.appends {
        let source = SnapshotHandle::new(append_table.dataset.clone());
        let staged = target_db
            .storage()
            .stage_append_stream(&current_ds, &source, &[])
            .await?;
        current_ds = target_db
            .storage()
            .commit_staged(current_ds, staged)
            .await?;
    }

    // Failpoint: crash after the Phase 1a append commit, before the upsert.
    // Models a partial Phase B — appends are on Lance HEAD but the upserts/deletes
    // have not committed and the achieved-version intent has not been recorded, so
    // recovery must roll BACK (not publish the appends-only state). See
    // tests/failpoints.rs::branch_merge_adopt_partial_after_append_rolls_back.
    crate::failpoints::maybe_fail("branch_merge.adopt_after_append_pre_upsert")?;

    // Phase 1b: upsert the CHANGED rows. The merge_insert hash join is now
    // bounded to the genuinely-changed set, not the whole delta. It runs against
    // the committed view that already includes the appends; the changed ids are
    // disjoint from the appended ids (each id is classified into exactly one of
    // new / changed / deleted / unchanged in the single ordered walk), so the
    // join never collides with an appended row.
    if let Some(upsert_table) = &delta.upserts {
        if let Some(combined) = scan_staged_combined(target_db, upsert_table).await? {
            let staged_merge = target_db
                .storage()
                .stage_merge_insert(
                    current_ds.clone(),
                    combined,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::InsertAll,
                )
                .await?;
            current_ds = target_db
                .storage()
                .commit_staged(current_ds, staged_merge)
                .await?;
        }
    }

    // Failpoint: crash after the Phase 1b upsert commit, before the delete.
    // Models a partial Phase B — appends + upserts on Lance HEAD but the delete
    // has not committed and the achieved-version intent has not been recorded, so
    // recovery must roll BACK. See
    // tests/failpoints.rs::branch_merge_adopt_partial_after_upsert_rolls_back.
    crate::failpoints::maybe_fail("branch_merge.adopt_after_upsert_pre_delete")?;

    // Phase 2: delete removed rows via deletion vectors (inline-commit residual,
    // same as the three-way path until Lance ships a public two-phase delete).
    if !delta.deleted_ids.is_empty() {
        let escaped: Vec<String> = delta
            .deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let filter = format!("id IN ({})", escaped.join(", "));
        let (new_ds, _) = target_db
            .storage_inline_residual()
            .delete_where(&full_path, current_ds, &filter)
            .await?;
        current_ds = new_ds;
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

    Ok(crate::db::SubTableUpdate {
        table_key: table_key.to_string(),
        table_version: final_state.version,
        table_branch,
        row_count: final_state.row_count,
        version_metadata: final_state.version_metadata,
    })
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
        // Converge any pending recovery sidecar before the merge
        // captures its target snapshot: the merge's publish would
        // otherwise make the drifted Phase-B commit visible as an
        // unattributed side effect (manifest catches up to HEAD with no
        // recovery audit row) and leave the stale sidecar behind. Runs
        // before the merge's own sidecar exists.
        self.heal_pending_recovery_sidecars().await?;
        self.branch_merge_impl(source, target, actor_id).await
    }

    async fn branch_merge_impl(
        &self,
        source: &str,
        target: &str,
        actor_id: Option<&str>,
    ) -> Result<MergeOutcome> {
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

        let source_head_commit_id = self
            .head_commit_id_for_branch(source_branch.as_deref())
            .await?
            .ok_or_else(|| OmniError::manifest("source branch has no head commit".to_string()))?;
        let target_head_commit_id = self
            .head_commit_id_for_branch(target_branch.as_deref())
            .await?
            .ok_or_else(|| OmniError::manifest("target branch has no head commit".to_string()))?;
        let base_commit = CommitGraph::merge_base(
            self.uri(),
            source_branch.as_deref(),
            target_branch.as_deref(),
        )
        .await?
        .ok_or_else(|| OmniError::manifest("branches have no common ancestor".to_string()))?;

        if source_head_commit_id == target_head_commit_id
            || base_commit.graph_commit_id == source_head_commit_id
        {
            return Ok(MergeOutcome::AlreadyUpToDate);
        }
        let is_fast_forward = base_commit.graph_commit_id == target_head_commit_id;

        let base_snapshot = ManifestCoordinator::snapshot_at(
            self.uri(),
            base_commit.manifest_branch.as_deref(),
            base_commit.manifest_version,
        )
        .await?;
        let source_snapshot = self
            .resolved_target(ReadTarget::Branch(
                source_branch.clone().unwrap_or_else(|| "main".to_string()),
            ))
            .await?
            .snapshot;
        // Hold the merge-exclusive mutex across the full swap → operate
        // → restore window. Two concurrent branch_merge calls would
        // otherwise interleave their three separate `coordinator.write()`
        // acquisitions, leaving each merge's body running against the
        // other's swapped coord. Pinned by
        // `concurrent_branch_merges_distinct_targets_do_not_swap_into_each_other`
        // in `crates/omnigraph-server/tests/server.rs`.
        let merge_exclusive = self.merge_exclusive();
        let _merge_guard = merge_exclusive.lock().await;

        let previous_branch = self.active_branch().await;
        let previous = self
            .swap_coordinator_for_branch(target_branch.as_deref())
            .await?;
        let merge_result = self
            .branch_merge_on_current_target(
                &base_snapshot,
                &source_snapshot,
                &target_head_commit_id,
                &source_head_commit_id,
                is_fast_forward,
                actor_id,
            )
            .await;
        self.restore_coordinator(previous).await;

        // Sync the restored coordinator's cached manifest snapshot with
        // disk on both Ok and Err paths. During the swap window above,
        // `self.coordinator` was a freshly opened coord for the merge
        // target; any concurrent writer on that target (e.g. a `/change`
        // on `main` racing a `merge into=main`) publishes against the
        // swapped coord and never touches the original. Without this
        // sync, the restored coord's cached manifest snapshot would
        // diverge from disk and seed a stale `expected_versions` into
        // the next op's publisher CAS fence — a non-retryable
        // `ExpectedVersionMismatch` for a user with no concurrent
        // writer of their own. Pinned by the
        // `[d:merge×change:into-target]` cell of
        // `concurrent_branch_ops_morphological_matrix` in
        // `crates/omnigraph-server/tests/server.rs`, which flakes
        // pre-fix and stabilises post-fix.
        //
        // Use `refresh_coordinator_only` rather than `refresh` so the
        // recovery sweep doesn't race the merge's own in-flight
        // sidecar: when the merge body returns Err between Phase B
        // (per-table `commit_staged` + sidecar write) and Phase C
        // (manifest publish + sidecar delete), the sidecar is still on
        // disk. `refresh`'s `RollForwardOnly` sweep would observe it
        // and close it here — masking the failure from the next
        // `Omnigraph::open` sweep and from the audit row that the open
        // sweep emits. Pinned by
        // `branch_merge_phase_b_failure_recovered_on_next_open` in
        // `crates/omnigraph/tests/failpoints.rs`.
        //
        // Err-path refresh is best-effort: the merge body's error
        // (typically the structured `manifest_conflict` from the
        // post_queue_snapshot drift check) is the value the caller
        // needs to see. A refresh-time storage error would replace
        // that with a less informative error; the next op or the next
        // `Omnigraph::open` will re-sync the coord anyway.
        if previous_branch == target_branch {
            if let Err(refresh_err) = self.refresh_coordinator_only().await {
                if merge_result.is_ok() {
                    return Err(refresh_err);
                }
                tracing::warn!(
                    error = %refresh_err,
                    "post-merge coordinator refresh failed on the error path; \
                     the next op or open will re-sync"
                );
            }
        }

        merge_result
    }

    async fn branch_merge_on_current_target(
        &self,
        base_snapshot: &Snapshot,
        source_snapshot: &Snapshot,
        target_head_commit_id: &str,
        source_head_commit_id: &str,
        is_fast_forward: bool,
        actor_id: Option<&str>,
    ) -> Result<MergeOutcome> {
        self.ensure_commit_graph_initialized().await?;
        let target_snapshot = self.snapshot().await;

        let mut table_keys = HashSet::new();
        for entry in base_snapshot.entries() {
            table_keys.insert(entry.table_key.clone());
        }
        for entry in source_snapshot.entries() {
            table_keys.insert(entry.table_key.clone());
        }
        for entry in target_snapshot.entries() {
            table_keys.insert(entry.table_key.clone());
        }

        let mut ordered_table_keys: Vec<String> = table_keys.into_iter().collect();
        ordered_table_keys.sort();

        let mut conflicts = Vec::new();
        let mut candidates: HashMap<String, CandidateTableState> = HashMap::new();

        for table_key in &ordered_table_keys {
            let base_entry = base_snapshot.entry(table_key);
            let source_entry = source_snapshot.entry(table_key);
            let target_entry = target_snapshot.entry(table_key);
            if same_manifest_state(source_entry, target_entry) {
                continue;
            }
            if same_manifest_state(base_entry, source_entry) {
                continue;
            }
            if same_manifest_state(base_entry, target_entry) {
                let candidate = classify_adopt(
                    self,
                    &self.catalog(),
                    base_snapshot,
                    source_snapshot,
                    &target_snapshot,
                    table_key,
                )
                .await?;
                candidates.insert(table_key.clone(), candidate);
                continue;
            }

            if let Some(staged) = stage_streaming_table_merge(
                table_key,
                &self.catalog(),
                base_snapshot,
                source_snapshot,
                &target_snapshot,
                &mut conflicts,
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

        validate_merge_candidates(self, source_snapshot, &target_snapshot, &candidates).await?;

        // Recovery sidecar: protect the per-table commit_staged loop.
        // Pin `RewriteMerged` and `AdoptWithDelta` candidates — both advance
        // Lance HEAD before the manifest publish (RewriteMerged via
        // publish_rewritten_merge_table; AdoptWithDelta via publish_adopted_delta:
        // stage_append + stage_merge_insert + delete_where + index — multiple
        // commit_staged calls per table, which the loose classification handles
        // as multi-step drift).
        //
        // `AdoptSourceState` candidates are NOT pinned: their publish
        // (`publish_adopted_source_state`) is a pure pointer switch or a fork
        // (`fork_dataset_from_entry_state` only adds a Lance branch ref), neither
        // of which advances the data HEAD. Pinning them would classify as
        // NoMovement and force an all-or-nothing rollback that destroys sibling
        // tables' committed work.
        //
        // The former gap — adopt subcases that applied a non-empty delta advanced
        // HEAD unpinned — is closed: `classify_adopt` pre-computes the delta, so a
        // HEAD-advancing adopt is `AdoptWithDelta` (pinned here) and an empty-delta
        // adopt stays `AdoptSourceState`.
        // Acquire per-(table_key, target_branch) queues for every table
        // touched by the merge plan. Sorted-order acquisition prevents
        // lock-order inversion against concurrent multi-table writers.
        // The active branch (set by the caller's `swap_coordinator_for_branch`)
        // is the merge target; queue keys are scoped to it because a
        // branch_merge writes only to the target branch.
        //
        // Held across the per-table publish loop and the manifest
        // commit + record_merge_commit calls below, so no concurrent
        // writer to a touched (table, target_branch) can interleave
        // between our commit_staged and our publish.
        let active_branch_for_keys = self.active_branch().await;
        let merge_queue_keys: Vec<(String, Option<String>)> = ordered_table_keys
            .iter()
            .filter(|table_key| {
                matches!(
                    candidates.get(*table_key),
                    Some(CandidateTableState::RewriteMerged(_))
                        | Some(CandidateTableState::AdoptSourceState)
                        | Some(CandidateTableState::AdoptWithDelta(_))
                )
            })
            .map(|table_key| (table_key.clone(), active_branch_for_keys.clone()))
            .collect();
        let _merge_queue_guards = self.write_queue().acquire_many(&merge_queue_keys).await;

        let post_queue_snapshot = self.snapshot().await;
        for table_key in &ordered_table_keys {
            let Some(candidate) = candidates.get(table_key) else {
                continue;
            };
            if !matches!(
                candidate,
                CandidateTableState::RewriteMerged(_)
                    | CandidateTableState::AdoptSourceState
                    | CandidateTableState::AdoptWithDelta(_)
            ) {
                continue;
            }
            let expected = target_snapshot.entry(table_key).map(|e| e.table_version);
            let current = post_queue_snapshot
                .entry(table_key)
                .map(|e| e.table_version);
            if expected != current {
                return Err(OmniError::manifest_expected_version_mismatch(
                    table_key.clone(),
                    expected.unwrap_or(0),
                    current.unwrap_or(0),
                ));
            }
        }

        let recovery_pins: Vec<crate::db::manifest::SidecarTablePin> = ordered_table_keys
            .iter()
            .filter_map(|table_key| {
                let candidate = candidates.get(table_key)?;
                if !matches!(
                    candidate,
                    CandidateTableState::RewriteMerged(_) | CandidateTableState::AdoptWithDelta(_)
                ) {
                    return None;
                }
                let entry = target_snapshot.entry(table_key)?;
                Some(crate::db::manifest::SidecarTablePin {
                    table_key: table_key.clone(),
                    table_path: self.storage().dataset_uri(&entry.table_path),
                    expected_version: entry.table_version,
                    post_commit_pin: entry.table_version + 1,
                    // Stamped after the whole per-table publish completes
                    // (Phase-B confirmation, just before the manifest publish).
                    // Until then `None` marks an unfinished publish that
                    // recovery must roll back, not roll forward.
                    confirmed_version: None,
                    // Use the merge target branch (where commits actually
                    // land), NOT entry.table_branch (where the table
                    // currently lives). publish_rewritten_merge_table calls
                    // open_for_mutation, which forks an inherited-from-main
                    // table to active_branch on first write — the resulting
                    // Lance commit lands on active_branch. Recovery's
                    // open_lance_head must check the same branch, otherwise
                    // an inherited-table feature-to-feature merge classifies
                    // as NoMovement and the all-or-nothing rollback skips
                    // the orphaned post-Phase-B HEAD on the target ref.
                    // Same rationale as table_ops.rs:115-120 in
                    // ensure_indices_for_branch.
                    table_branch: active_branch_for_keys.clone(),
                })
            })
            .collect();
        // Keep the sidecar alongside its handle: after the per-table publish
        // loop completes (Phase B), we re-write it with each table's confirmed
        // version before the manifest publish, so recovery can tell a finished
        // publish (roll forward) from a partial one (roll back).
        let mut recovery: Option<(
            crate::db::manifest::RecoverySidecar,
            crate::db::manifest::RecoverySidecarHandle,
        )> = if recovery_pins.is_empty() {
            None
        } else {
            // Use the merge target branch directly, NOT a heuristic
            // derived from `ordered_table_keys.first()`. The first
            // sorted table key may not be in the target snapshot at all
            // (its `entry()` returns None → branch becomes None == main),
            // and the SubTableEntry's `table_branch` field isn't
            // necessarily the merge target branch. The caller
            // `branch_merge` calls `swap_coordinator_for_branch(target_branch)`
            // before invoking this function, so `self.active_branch()`
            // is the target.
            let target_branch = active_branch_for_keys.clone();
            let mut sidecar = crate::db::manifest::new_sidecar(
                crate::db::manifest::SidecarKind::BranchMerge,
                target_branch,
                actor_id.map(str::to_string),
                recovery_pins,
            );
            // Carry the source branch's HEAD commit id so the recovery
            // sweep's audit step can record this as a MERGE commit
            // (linked to the source) instead of a plain commit. Without
            // this, future merges between the same pair lose
            // already-up-to-date detection and merge-base correctness.
            sidecar.merge_source_commit_id = Some(source_head_commit_id.to_string());
            let handle = crate::db::manifest::write_sidecar(
                self.root_uri(),
                self.storage_adapter(),
                &sidecar,
            )
            .await?;
            Some((sidecar, handle))
        };

        let mut updates = Vec::new();
        let mut changed_edge_tables = false;
        for table_key in &ordered_table_keys {
            let Some(candidate_state) = candidates.get(table_key) else {
                continue;
            };
            let update = match candidate_state {
                CandidateTableState::AdoptSourceState => {
                    publish_adopted_source_state(self, source_snapshot, &target_snapshot, table_key)
                        .await?
                }
                CandidateTableState::AdoptWithDelta(delta) => {
                    publish_adopted_delta(self, table_key, delta).await?
                }
                CandidateTableState::RewriteMerged(staged) => {
                    publish_rewritten_merge_table(self, table_key, staged).await?
                }
            };
            if table_key.starts_with("edge:") {
                changed_edge_tables = true;
            }
            updates.push(update);
        }

        // Phase-B confirmation: every table's publish finished, so stamp the
        // sidecar with each table's exact achieved version before the manifest
        // publish. This is the commit point of the recovery WAL: a crash from
        // here on rolls FORWARD to these versions, while a crash anywhere in the
        // publish loop above left the sidecar unconfirmed and rolls BACK. The
        // `updates` carry the real per-table final versions (multiple
        // commit_staged calls per table, so not derivable from `post_commit_pin`
        // alone). A failure here leaves the unconfirmed sidecar → roll back.
        if let Some((sidecar, _)) = recovery.as_mut() {
            let confirmed_versions: std::collections::HashMap<String, u64> = updates
                .iter()
                .map(|u| (u.table_key.clone(), u.table_version))
                .collect();
            crate::db::manifest::confirm_sidecar_phase_b(
                self.root_uri(),
                self.storage_adapter(),
                sidecar,
                &confirmed_versions,
            )
            .await?;
        }

        // Failpoint: pin the per-writer Phase B → Phase C residual for
        // branch_merge. Lance HEAD has advanced on every touched table
        // (publish_*) AND the sidecar is confirmed, but the manifest publish
        // below hasn't run — so recovery rolls FORWARD. Used by
        // `tests/failpoints.rs::branch_merge_phase_b_failure_recovered_on_next_open`.
        crate::failpoints::maybe_fail("branch_merge.post_phase_b_pre_manifest_commit")?;

        let manifest_version = if updates.is_empty() {
            self.version().await
        } else {
            self.commit_manifest_updates(&updates).await?
        };

        // Recovery sidecar lifecycle: delete after manifest publish.
        // Best-effort cleanup; the merge already landed durably so
        // failing the user here is undesirable.
        if let Some((_, handle)) = recovery {
            if let Err(err) =
                crate::db::manifest::delete_sidecar(&handle, self.storage_adapter()).await
            {
                tracing::warn!(
                    error = %err,
                    operation_id = handle.operation_id.as_str(),
                    "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
                );
            }
        }
        self.record_merge_commit(
            manifest_version,
            target_head_commit_id,
            source_head_commit_id,
            actor_id,
        )
        .await?;

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
