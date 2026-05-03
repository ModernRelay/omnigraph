use super::*;

const MERGE_STAGE_BATCH_ROWS: usize = 8192;
const MERGE_STAGE_DIR_ENV: &str = "OMNIGRAPH_MERGE_STAGING_DIR";

#[derive(Debug)]
enum CandidateTableState {
    AdoptSourceState,
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

#[derive(Debug, Clone)]
struct CursorRow {
    id: String,
    signature: String,
    batch: RecordBatch,
    row_index: usize,
}

struct OrderedTableCursor {
    stream: Option<std::pin::Pin<Box<DatasetRecordBatchStream>>>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    peeked: Option<CursorRow>,
}

impl OrderedTableCursor {
    async fn from_snapshot(snapshot: &Snapshot, table_key: &str) -> Result<Self> {
        let dataset = match snapshot.entry(table_key) {
            Some(_) => Some(snapshot.open(table_key).await?),
            None => None,
        };
        Self::from_dataset(dataset).await
    }

    async fn from_dataset(dataset: Option<Dataset>) -> Result<Self> {
        let stream = if let Some(ds) = dataset {
            Some(Box::pin(
                crate::table_store::TableStore::scan_stream(
                    &ds,
                    None,
                    None,
                    Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                    false,
                )
                .await?,
            ))
        } else {
            None
        };

        Ok(Self {
            stream,
            current_batch: None,
            current_row: 0,
            peeked: None,
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
                    return Ok(Some(CursorRow {
                        id: row_id_at(batch, row_index)?,
                        signature: row_signature(batch, row_index)?,
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
        self.batches.push(row.batch.slice(row.row_index, 1));
        if self.buffered_rows >= MERGE_STAGE_BATCH_ROWS {
            self.flush().await?;
        }
        Ok(())
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
/// Returns the changed/new rows (for merge_insert) and deleted IDs (for delete).
async fn compute_source_delta(
    table_key: &str,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
) -> Result<Option<StagedMergeResult>> {
    let schema = schema_for_table_key(catalog, table_key)?;
    let mut full_writer =
        StagedTableWriter::new(&format!("{}_adopt_full", table_key), schema.clone())?;
    let mut delta_writer = StagedTableWriter::new(&format!("{}_adopt_delta", table_key), schema)?;
    let mut deleted_ids: Vec<String> = Vec::new();
    let mut base = OrderedTableCursor::from_snapshot(base_snapshot, table_key).await?;
    let mut source = OrderedTableCursor::from_snapshot(source_snapshot, table_key).await?;

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

        let base_sig = base_row.as_ref().map(|r| r.signature.as_str());
        let source_sig = source_row.as_ref().map(|r| r.signature.as_str());

        match (&base_row, &source_row) {
            (Some(_), None) => {
                // Deleted on source
                deleted_ids.push(next_id);
                needs_update = true;
            }
            (None, Some(src)) => {
                // New on source
                full_writer.push_row(src).await?;
                delta_writer.push_row(src).await?;
                needs_update = true;
            }
            (Some(_), Some(src)) if source_sig != base_sig => {
                // Changed on source
                full_writer.push_row(src).await?;
                delta_writer.push_row(src).await?;
                needs_update = true;
            }
            (Some(base), Some(_)) => {
                // Unchanged — write to full (for validation), skip delta
                full_writer.push_row(base).await?;
            }
            (None, None) => unreachable!(),
        }
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
    for column in batch.columns() {
        values.push(
            array_value_to_string(column.as_ref(), row)
                .map_err(|e| OmniError::Lance(e.to_string()))?,
        );
    }
    Ok(values.join("\u{1f}"))
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
            let mut stream =
                crate::table_store::TableStore::scan_stream(&ds, None, None, None, false).await?;
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
            let mut stream =
                crate::table_store::TableStore::scan_stream(&ds, None, None, None, false).await?;
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
            CandidateTableState::AdoptSourceState => match source_snapshot.entry(table_key) {
                Some(_) => Ok(Some(source_snapshot.open(table_key).await?)),
                None => Ok(None),
            },
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
    seen: &mut [HashMap<String, String>],
    conflicts: &mut Vec<MergeConflict>,
) -> Result<()> {
    for (constraint_idx, columns) in constraints.iter().enumerate() {
        let seen = &mut seen[constraint_idx];
        for row in 0..batch.num_rows() {
            let mut parts = Vec::with_capacity(columns.len());
            let mut any_null = false;
            for column_name in columns {
                let column = batch.column_by_name(column_name).ok_or_else(|| {
                    OmniError::manifest(format!(
                        "table {} missing unique column '{}'",
                        table_key, column_name
                    ))
                })?;
                if column.is_null(row) {
                    any_null = true;
                    break;
                }
                parts.push(
                    array_value_to_string(column.as_ref(), row)
                        .map_err(|e| OmniError::Lance(e.to_string()))?,
                );
            }
            if any_null {
                continue;
            }
            let value = parts.join("|");
            let row_id = row_id_at(batch, row)?;
            if let Some(first_row_id) = seen.insert(value.clone(), row_id.clone()) {
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

async fn publish_adopted_source_state(
    target_db: &Omnigraph,
    catalog: &Catalog,
    base_snapshot: &Snapshot,
    source_snapshot: &Snapshot,
    target_snapshot: &Snapshot,
    table_key: &str,
) -> Result<crate::db::SubTableUpdate> {
    let source_entry = source_snapshot
        .entry(table_key)
        .ok_or_else(|| OmniError::manifest(format!("missing source entry for {}", table_key)))?;
    let target_entry = target_snapshot.entry(table_key);

    match (
        target_db.active_branch(),
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
        // Source on branch, target on main — apply delta to preserve version metadata
        (None, Some(_source_branch)) => {
            let delta =
                compute_source_delta(table_key, catalog, base_snapshot, source_snapshot).await?;
            match delta {
                Some(staged) => publish_rewritten_merge_table(target_db, table_key, &staged).await,
                None => Ok(crate::db::SubTableUpdate {
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
            }
        }
        // Both on branches
        (Some(target_branch), Some(source_branch)) => {
            if target_entry.and_then(|entry| entry.table_branch.as_deref()) == Some(target_branch) {
                // Target already owns this table — apply delta onto its lineage
                let delta =
                    compute_source_delta(table_key, catalog, base_snapshot, source_snapshot)
                        .await?;
                match delta {
                    Some(staged) => {
                        publish_rewritten_merge_table(target_db, table_key, &staged).await
                    }
                    None => Ok(crate::db::SubTableUpdate {
                        table_key: table_key.to_string(),
                        table_version: target_entry.unwrap().table_version,
                        table_branch: Some(target_branch.to_string()),
                        row_count: source_entry.row_count,
                        version_metadata: target_entry.unwrap().version_metadata.clone(),
                    }),
                }
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
                let state = target_db.table_store().table_state(&full_path, &ds).await?;
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
    let (ds, full_path, table_branch) = target_db.open_for_mutation(table_key).await?;
    let mut current_ds = ds;

    // Phase 1: merge_insert changed/new rows (preserves _row_created_at_version for
    // existing rows, bumps _row_last_updated_at_version only for actually-changed rows).
    //
    // MR-793 Phase 5: routed through the staged primitive so a failure
    // between writing fragments and committing leaves no Lance-HEAD
    // drift. The commit_staged here is per-table per-call (Lance has no
    // multi-dataset atomic commit); the residual sits at this single
    // commit point, narrowed from the previous "merge_insert + delete +
    // index" multi-step inline-commit chain.
    if let Some(delta) = &staged.delta_staged {
        let batches: Vec<RecordBatch> = target_db
            .table_store()
            .scan_batches(&delta.dataset)
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
                .table_store()
                .stage_merge_insert(
                    current_ds.clone(),
                    combined,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::InsertAll,
                )
                .await?;
            current_ds = target_db
                .table_store()
                .commit_staged(Arc::new(current_ds), staged_merge.transaction)
                .await?;
        }
    }

    // Phase 2: delete removed rows via deletion vectors.
    //
    // INLINE-COMMIT RESIDUAL: lance-4.0.0 does not expose a public
    // two-phase delete API (DeleteJob is `pub(crate)` —
    // lance-format/lance#6658 is open with no PRs). MR-793 deliberately
    // does NOT introduce a `stage_delete` wrapper that would secretly
    // inline-commit (a side-channel — see design doc §3.2). When the
    // upstream API ships, swap this `delete_where` call for
    // `stage_delete` + `commit_staged`.
    if !staged.deleted_ids.is_empty() {
        let escaped: Vec<String> = staged
            .deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let filter = format!("id IN ({})", escaped.join(", "));
        target_db
            .table_store()
            .delete_where(&full_path, &mut current_ds, &filter)
            .await?;
    }

    // Phase 3: rebuild indices.
    //
    // `build_indices_on_dataset` was migrated in MR-793 Phase 4 to use
    // `stage_create_btree_index` / `stage_create_inverted_index` +
    // `commit_staged` for scalar indices. Vector indices remain inline
    // (residual — `build_index_metadata_from_segments` is `pub(crate)`
    // in lance-4.0.0; companion ticket to lance-format/lance#6658).
    let row_count = target_db
        .table_store()
        .table_state(&full_path, &current_ds)
        .await?
        .row_count;
    if row_count > 0 {
        target_db
            .build_indices_on_dataset(table_key, &mut current_ds)
            .await?;
    }
    let final_state = target_db
        .table_store()
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
    pub async fn branch_merge(&mut self, source: &str, target: &str) -> Result<MergeOutcome> {
        self.branch_merge_as(source, target, None).await
    }

    pub async fn branch_merge_as(
        &mut self,
        source: &str,
        target: &str,
        actor_id: Option<&str>,
    ) -> Result<MergeOutcome> {
        self.ensure_schema_apply_idle("branch_merge").await?;
        let previous_actor = self.audit_actor_id.clone();
        self.audit_actor_id = actor_id.map(str::to_string);
        let result = self.branch_merge_impl(source, target).await;
        self.audit_actor_id = previous_actor;
        result
    }

    async fn branch_merge_impl(
        &mut self,
        source: &str,
        target: &str,
    ) -> Result<MergeOutcome> {
        if is_internal_run_branch(source) || is_internal_run_branch(target) {
            return Err(OmniError::manifest(format!(
                "branch_merge does not allow internal run refs ('{}' -> '{}')",
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
        let previous_branch = self.active_branch().map(str::to_string);
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
            )
            .await;
        self.restore_coordinator(previous);

        if merge_result.is_ok() && previous_branch == target_branch {
            self.refresh().await?;
        }

        merge_result
    }

    async fn branch_merge_on_current_target(
        &mut self,
        base_snapshot: &Snapshot,
        source_snapshot: &Snapshot,
        target_head_commit_id: &str,
        source_head_commit_id: &str,
        is_fast_forward: bool,
    ) -> Result<MergeOutcome> {
        self.ensure_commit_graph_initialized().await?;
        let target_snapshot = self.snapshot();

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
                candidates.insert(table_key.clone(), CandidateTableState::AdoptSourceState);
                continue;
            }

            if let Some(staged) = stage_streaming_table_merge(
                table_key,
                self.catalog(),
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

        // MR-847 sidecar: protect the per-table commit_staged loop. Pins
        // every table that will be touched by `publish_adopted_source_state`
        // or `publish_rewritten_merge_table`. BranchMerge uses loose
        // classification — the publish path may run multiple commit_staged
        // calls per table (publish_rewritten_merge_table does
        // stage_merge_insert + delete_where + index rebuilds per the
        // existing branch-merge code path).
        let recovery_pins: Vec<crate::db::manifest::SidecarTablePin> = ordered_table_keys
            .iter()
            .filter(|tk| candidates.contains_key(*tk))
            .filter_map(|table_key| {
                let entry = target_snapshot.entry(table_key)?;
                Some(crate::db::manifest::SidecarTablePin {
                    table_key: table_key.clone(),
                    table_path: self.table_store().dataset_uri(&entry.table_path),
                    expected_version: entry.table_version,
                    post_commit_pin: entry.table_version + 1,
                })
            })
            .collect();
        let recovery_handle = if recovery_pins.is_empty() {
            None
        } else {
            // PR #72 review (chatgpt-codex + cubic): use the merge target
            // branch directly, NOT a heuristic derived from
            // `ordered_table_keys.first()`. The first sorted table key may
            // not be in the target snapshot at all (its `entry()` returns
            // None → branch becomes None == main), and the SubTableEntry's
            // `table_branch` field isn't necessarily the merge target
            // branch. The caller `branch_merge` calls
            // `swap_coordinator_for_branch(target_branch)` before invoking
            // this function, so `self.active_branch()` is the target.
            let target_branch = self.active_branch().map(str::to_string);
            let sidecar = crate::db::manifest::new_sidecar(
                crate::db::manifest::SidecarKind::BranchMerge,
                target_branch,
                self.audit_actor_id.clone(),
                recovery_pins,
            );
            Some(
                crate::db::manifest::write_sidecar(
                    self.root_uri(),
                    self.storage_adapter(),
                    &sidecar,
                )
                .await?,
            )
        };

        let mut updates = Vec::new();
        let mut changed_edge_tables = false;
        for table_key in &ordered_table_keys {
            let Some(candidate_state) = candidates.get(table_key) else {
                continue;
            };
            let update = match candidate_state {
                CandidateTableState::AdoptSourceState => {
                    publish_adopted_source_state(
                        self,
                        self.catalog(),
                        base_snapshot,
                        source_snapshot,
                        &target_snapshot,
                        table_key,
                    )
                    .await?
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

        // MR-847 failpoint: pin the per-writer Phase B → Phase C residual
        // for branch_merge. Lance HEAD has advanced on every touched
        // table (publish_*) but the manifest publish below hasn't run.
        // Used by `tests/failpoints.rs::branch_merge_phase_b_failure_recovered_on_next_open`.
        crate::failpoints::maybe_fail("branch_merge.post_phase_b_pre_manifest_commit")?;

        let manifest_version = if updates.is_empty() {
            self.version()
        } else {
            self.commit_manifest_updates(&updates).await?
        };

        // MR-847 sidecar lifecycle: delete after manifest publish.
        // Best-effort cleanup (PR #72 review).
        if let Some(handle) = recovery_handle {
            if let Err(err) =
                crate::db::manifest::delete_sidecar(&handle, self.storage_adapter()).await
            {
                tracing::warn!(
                    error = %err,
                    operation_id = handle.operation_id.as_str(),
                    "MR-847 sidecar cleanup failed; the next open's recovery sweep will resolve it"
                );
            }
        }
        self.record_merge_commit(
            manifest_version,
            target_head_commit_id,
            source_head_commit_id,
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
