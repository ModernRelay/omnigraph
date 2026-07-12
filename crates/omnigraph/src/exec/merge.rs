use super::*;

const MERGE_STAGE_BATCH_ROWS: usize = 8192;
const MERGE_STAGE_DIR_ENV: &str = "OMNIGRAPH_MERGE_STAGING_DIR";

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
    /// Adopt the source's state by applying a non-empty delta onto the target's
    /// lineage (append new + upsert changed + delete removed). The delta is
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
/// construct this value: their target ref is created only after the v4 recovery
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
}

#[derive(Debug)]
struct StagedMergeResult {
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
    let mut upsert_writer = StagedTableWriter::new(&format!("{}_adopt_upsert", table_key), schema)?;
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
            // Only changed rows go to the delta (for publish). The full merged
            // table is no longer staged — validation works off this delta plus
            // the committed target via index lookups, not a full re-scan.
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

/// Build the per-table [`ChangeSet`](crate::validate::ChangeSet) for a merge from
/// the classified candidates — the new/changed rows (from the staged deltas) and
/// removed ids the validator evaluates, instead of re-scanning whole tables.
/// `AdoptSourceState` is published as a pointer/fork but still carries a
/// `validation_delta` (the source-vs-target rows) when its source diverged, so
/// it is validated like `AdoptWithDelta`; only an empty-delta adopt is skipped.
async fn build_merge_changeset(
    db: &Omnigraph,
    catalog: &Catalog,
    candidates: &HashMap<String, CandidateTableState>,
) -> Result<crate::validate::ChangeSet> {
    let mut changeset = crate::validate::ChangeSet::new();
    for (table_key, candidate) in candidates {
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
                if let Some(table) = &delta.appends {
                    change
                        .added
                        .extend(scan_staged_for_validation(db, table, &projection).await?);
                }
                if let Some(table) = &delta.upserts {
                    change
                        .changed
                        .extend(scan_staged_for_validation(db, table, &projection).await?);
                }
                change.deleted_ids = delta.deleted_ids.clone();
            }
            CandidateTableState::RewriteMerged(staged) => {
                if let Some(table) = &staged.delta_staged {
                    change
                        .changed
                        .extend(scan_staged_for_validation(db, table, &projection).await?);
                }
                change.deleted_ids = staged.deleted_ids.clone();
            }
        }
        changeset.insert(table_key.clone(), change);
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
) -> Result<Vec<RecordBatch>> {
    let snapshot = SnapshotHandle::new(table.dataset.clone());
    let batches = db
        .storage()
        .scan(&snapshot, Some(projection), None, None)
        .await?;
    Ok(batches
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect())
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
        // Source has no such table — nothing to adopt or validate.
        return Ok(CandidateTableState::AdoptSourceState {
            validation_delta: None,
        });
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
    // Compute the source-vs-target delta UNCONDITIONALLY — it is the validation
    // input the evaluator needs, independent of how the table is published.
    // (`classify_adopt` is only reached when base == target, so the
    // base-vs-source delta equals the target-vs-source delta.) A HEAD-advancing
    // publish consumes it as the write payload (`AdoptWithDelta`); a pointer/fork
    // publish ignores it and only validates it (`AdoptSourceState`), so a
    // pointer-adopt whose source diverged is still checked for
    // RI/uniqueness/cardinality against the merged state.
    let validation_delta =
        compute_adopt_delta(table_key, catalog, base_snapshot, source_snapshot).await?;
    match (advances_head, validation_delta) {
        (true, Some(delta)) => Ok(CandidateTableState::AdoptWithDelta(delta)),
        (_, validation_delta) => Ok(CandidateTableState::AdoptSourceState { validation_delta }),
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

fn pre_minted_merge_transaction(
    read_version: u64,
) -> crate::table_store::StagedTransactionIdentity {
    crate::table_store::StagedTransactionIdentity {
        read_version,
        uuid: format!("omnigraph-merge-{}", ulid::Ulid::new()),
    }
}

fn plan_merge_transactions(
    table_key: &str,
    candidate: &CandidateTableState,
    first_read_version: u64,
) -> Result<Vec<crate::table_store::StagedTransactionIdentity>> {
    let transaction_count = match candidate {
        CandidateTableState::RewriteMerged(staged) => {
            usize::from(staged.delta_staged.is_some()) + usize::from(!staged.deleted_ids.is_empty())
        }
        CandidateTableState::AdoptWithDelta(delta) => {
            usize::from(delta.appends.is_some())
                + usize::from(delta.upserts.is_some())
                + usize::from(!delta.deleted_ids.is_empty())
        }
        CandidateTableState::AdoptSourceState { .. } => 0,
    };
    if transaction_count == 0 {
        return Err(OmniError::manifest_internal(format!(
            "HEAD-advancing branch merge candidate '{table_key}' has no logical data transaction"
        )));
    }

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

async fn publish_rewritten_merge_table(
    target_db: &Omnigraph,
    table_key: &str,
    staged: &StagedMergeResult,
    prepared_target: Option<PreparedExistingMergeTarget>,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
) -> Result<crate::db::SubTableUpdate> {
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
    let mut planned_transactions = planned_transactions.iter();
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
            let planned = planned_transactions.next().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch merge table '{table_key}' has no planned merge transaction"
                ))
            })?;
            current_ds =
                commit_exact_merge_stage(target_db, current_ds, staged_merge, planned).await?;
        }
    }

    // Failpoint: crash after the Phase 1 merge_insert commit, before the delete.
    // Models a partial Phase B on the three-way path — the merged constructive
    // rows are on Lance HEAD but the delete has not committed and the
    // achieved-version intent has not been recorded, so recovery must roll BACK.
    // See tests/failpoints.rs::branch_merge_rewrite_partial_after_merge_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE,
    )?;

    // Phase 2: delete removed rows via deletion vectors, staged through
    // `stage_delete` + an exact pre-minted commit (MR-A — Lance 7.0's
    // `DeleteBuilder::execute_uncommitted`, #6658, made delete a two-phase
    // staged write, so this no longer inline-commits).
    if !staged.deleted_ids.is_empty() {
        let escaped: Vec<String> = staged
            .deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let filter = format!("id IN ({})", escaped.join(", "));
        if let Some(staged_delete) = target_db
            .storage()
            .stage_delete(&current_ds, &filter)
            .await?
        {
            let planned = planned_transactions.next().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch merge table '{table_key}' has no planned delete transaction"
                ))
            })?;
            current_ds =
                commit_exact_merge_stage(target_db, current_ds, staged_delete, planned).await?;
        }
    }

    if let Some(unused) = planned_transactions.next() {
        return Err(OmniError::manifest_internal(format!(
            "branch merge table '{table_key}' did not apply planned transaction {unused:?}"
        )));
    }

    // Failpoint: crash after the Phase 2 delete commit, before the index build.
    // Models a partial Phase B on the three-way path — constructive rows +
    // deletes are on Lance HEAD but the achieved-version intent has not been
    // recorded, so recovery must roll BACK (the index is reconciler-owned derived
    // state, but the merge itself never reached its commit boundary). See
    // tests/failpoints.rs::branch_merge_rewrite_partial_after_delete_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_INDEX,
    )?;

    // Phase 3: rebuild indices.
    //
    // `build_indices_on_dataset` uses `stage_create_btree_index` /
    // `stage_create_inverted_index` + `commit_staged` for scalar indices.
    // These are rebuildable `CreateIndex` tail transactions, not part of the
    // merge's logical pre-minted data chain; Armed v4 recovery accepts them
    // only after that complete exact chain and discards them with a rollback.
    // Vector indices remain inline-commit until the exact EnsureIndices adapter
    // migrates beta.21's usable full-table `execute_uncommitted` shape.
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
    prepared_target: Option<PreparedExistingMergeTarget>,
    planned_transactions: &[crate::table_store::StagedTransactionIdentity],
) -> Result<crate::db::SubTableUpdate> {
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
    let mut planned_transactions = planned_transactions.iter();
    if let Some(append_table) = &delta.appends {
        let source = SnapshotHandle::new(append_table.dataset.clone());
        let staged = target_db
            .storage()
            .stage_append_stream(&current_ds, &source, &[])
            .await?;
        let planned = planned_transactions.next().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "branch merge table '{table_key}' has no planned append transaction"
            ))
        })?;
        current_ds = commit_exact_merge_stage(target_db, current_ds, staged, planned).await?;
    }

    // Failpoint: crash after the Phase 1a append commit, before the upsert.
    // Models a partial Phase B — appends are on Lance HEAD but the upserts/deletes
    // have not committed and the achieved-version intent has not been recorded, so
    // recovery must roll BACK (not publish the appends-only state). See
    // tests/failpoints.rs::branch_merge_adopt_partial_after_append_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT,
    )?;

    // Phase 1b: upsert the CHANGED rows. The merge_insert hash join is now
    // bounded to the genuinely-changed set, not the whole delta. It runs against
    // the committed view that already includes the appends; the changed ids are
    // disjoint from the appended ids (each id is classified into exactly one of
    // new / changed / deleted / unchanged in the single ordered walk), so the
    // join never collides with an appended row. Every logical data step uses
    // the next identity in the exact transaction chain armed before Phase B.
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
            let planned = planned_transactions.next().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch merge table '{table_key}' has no planned upsert transaction"
                ))
            })?;
            current_ds =
                commit_exact_merge_stage(target_db, current_ds, staged_merge, planned).await?;
        }
    }

    // Failpoint: crash after the Phase 1b upsert commit, before the delete.
    // Models a partial Phase B — appends + upserts on Lance HEAD but the delete
    // has not committed and the achieved-version intent has not been recorded, so
    // recovery must roll BACK. See
    // tests/failpoints.rs::branch_merge_adopt_partial_after_upsert_rolls_back.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE,
    )?;

    // Phase 2: delete removed rows via deletion vectors, staged through
    // `stage_delete` + an exact pre-minted commit (same as the three-way path;
    // MR-A).
    if !delta.deleted_ids.is_empty() {
        let escaped: Vec<String> = delta
            .deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let filter = format!("id IN ({})", escaped.join(", "));
        if let Some(staged_delete) = target_db
            .storage()
            .stage_delete(&current_ds, &filter)
            .await?
        {
            let planned = planned_transactions.next().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch merge table '{table_key}' has no planned delete transaction"
                ))
            })?;
            current_ds =
                commit_exact_merge_stage(target_db, current_ds, staged_delete, planned).await?;
        }
    }

    if let Some(unused) = planned_transactions.next() {
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
        self.refresh_coordinator_only().await?;
        self.ensure_schema_apply_not_locked("branch_merge").await?;
        // Capture each branch as one coherent RFC-022 authority token plus
        // immutable snapshot. The target token is the coarse publish read set;
        // the source token pins the exact merge input without requiring the
        // source head to remain latest until the target CAS.
        let source_txn = self.open_write_txn(source_branch.as_deref()).await?;
        let target_txn = self.open_write_txn(target_branch.as_deref()).await?;
        let source_head_commit_id = source_txn
            .effective_graph_head
            .clone()
            .ok_or_else(|| OmniError::manifest("source branch has no head commit".to_string()))?;
        let target_head_commit_id = target_txn
            .effective_graph_head
            .clone()
            .ok_or_else(|| OmniError::manifest("target branch has no head commit".to_string()))?;
        let base_commit = CommitGraph::merge_base_between(
            self.uri(),
            source_branch.as_deref(),
            target_branch.as_deref(),
            &source_head_commit_id,
            &target_head_commit_id,
        )
        .await?
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

        let base_snapshot = ManifestCoordinator::snapshot_at(
            self.uri(),
            base_commit.manifest_branch.as_deref(),
            base_commit.manifest_version,
        )
        .await?;
        crate::failpoints::maybe_fail(
            crate::failpoints::names::BRANCH_MERGE_POST_AUTHORITY_CAPTURE,
        )?;
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
                &source_txn,
                &target_txn,
                source_branch.as_deref(),
                target_branch.as_deref(),
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
        // (per-table exact commits + sidecar confirmation) and Phase C
        // (manifest publish + sidecar delete), the sidecar is still on
        // disk. `refresh`'s `RollForwardOnly` sweep would observe it
        // and close it here — masking the failure from the next
        // `Omnigraph::open` sweep and from the audit row that the open
        // sweep emits. Pinned by
        // `branch_merge_phase_b_failure_recovered_on_next_open` in
        // `crates/omnigraph/tests/failpoints.rs`.
        //
        // Err-path refresh is best-effort: the merge body's error
        // (typically the structured read-set conflict from the fresh
        // post-table-gate manifest check) is the value the caller
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
                    catalog,
                    base_snapshot,
                    source_snapshot,
                    target_snapshot,
                    table_key,
                )
                .await?;
                candidates.insert(table_key.clone(), candidate);
                continue;
            }

            if let Some(staged) = stage_streaming_table_merge(
                table_key,
                catalog,
                base_snapshot,
                source_snapshot,
                target_snapshot,
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

        let changeset = build_merge_changeset(self, catalog, &candidates).await?;
        validate_merge_candidates(catalog, target_snapshot, &changeset).await?;

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
        let fresh_target_snapshot = self
            .fresh_snapshot_for_branch_unchecked(target_branch)
            .await?;
        if target_snapshot.version() != fresh_target_snapshot.version() {
            return Err(OmniError::manifest_read_set_changed(
                format!("branch_merge_target:{}", target_branch.unwrap_or("main")),
                Some(target_snapshot.version().to_string()),
                Some(fresh_target_snapshot.version().to_string()),
            ));
        }
        // Revalidate the complete target authority (branch incarnation, exact
        // graph head, and schema identity), not only its numeric manifest
        // version. This is the same coarse token mutation/load publish under.
        self.revalidate_write_txn(target_txn).await?;

        // Source is an immutable input, not part of the target CAS. A later
        // source-head advance is therefore harmless, but delete/recreate ABA is
        // not: the captured snapshot must still belong to the same native ref
        // incarnation and accepted schema contract.
        let live_source = self.open_write_txn(source_branch).await?;
        if live_source.authority.branch_identifier != source_txn.authority.branch_identifier {
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
                    serde_json::to_string(&live_source.authority.branch_identifier).map_err(
                        |error| {
                            OmniError::manifest_internal(format!(
                                "serialize live source branch identifier: {error}"
                            ))
                        },
                    )?,
                ),
            ));
        }
        if live_source.authority.schema_ir_hash != source_txn.authority.schema_ir_hash
            || live_source.authority.schema_identity_version
                != source_txn.authority.schema_identity_version
        {
            return Err(OmniError::manifest_read_set_changed(
                "branch_merge_source_schema".to_string(),
                Some(format!(
                    "{}:{}",
                    source_txn.authority.schema_identity_version,
                    source_txn.authority.schema_ir_hash
                )),
                Some(format!(
                    "{}:{}",
                    live_source.authority.schema_identity_version,
                    live_source.authority.schema_ir_hash
                )),
            ));
        }

        let expected_versions = ordered_table_keys
            .iter()
            .map(|table_key| {
                (
                    table_key.clone(),
                    target_snapshot
                        .entry(table_key)
                        .map(|entry| entry.table_version)
                        .unwrap_or(0),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut merge_lineage = self
            .new_lineage_intent_for_branch(target_branch, actor_id)
            .await?;
        merge_lineage.merged_parent_commit_id = Some(source_head_commit_id.to_string());

        // Build the v4 recovery envelope. Physical pins are a subset of the
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
            let source_entry = source_snapshot.entry(table_key).ok_or_else(|| {
                OmniError::manifest(format!(
                    "branch merge candidate '{}' has no captured source entry",
                    table_key
                ))
            })?;
            let target_entry = target_snapshot.entry(table_key);
            let expected_version = target_entry.map(|entry| entry.table_version).unwrap_or(0);
            let planned_output_branch = match candidate {
                CandidateTableState::RewriteMerged(_) | CandidateTableState::AdoptWithDelta(_) => {
                    active_branch_for_keys.clone()
                }
                CandidateTableState::AdoptSourceState { .. } => {
                    match (target_branch, source_entry.table_branch.as_deref()) {
                        (Some(target), Some(_)) => Some(target.to_string()),
                        _ => None,
                    }
                }
            };
            delta_slots.push(crate::db::manifest::RecoveryTableUpdateSlot {
                table_key: table_key.clone(),
                expected_version,
                table_branch: planned_output_branch,
                confirmed: None,
            });

            match candidate {
                CandidateTableState::RewriteMerged(_) | CandidateTableState::AdoptWithDelta(_) => {
                    let entry = target_entry.ok_or_else(|| {
                        OmniError::manifest(format!(
                            "HEAD-advancing branch merge candidate '{}' has no target entry",
                            table_key
                        ))
                    })?;
                    let source_fork_version = target_branch
                        .filter(|target| entry.table_branch.as_deref() != Some(*target))
                        .map(|_| entry.table_version);
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
                        table_key: table_key.clone(),
                        table_path: self.storage().dataset_uri(&entry.table_path),
                        expected_version,
                        post_commit_pin: expected_version + 1,
                        confirmed_version: None,
                        table_branch: active_branch_for_keys.clone(),
                    });
                    recovery_effects.push(crate::db::manifest::RecoveryBranchMergeEffect {
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
                    let creates_target_ref = source_entry.table_branch.is_some()
                        && target_entry.and_then(|entry| entry.table_branch.as_deref())
                            != Some(target);
                    if !creates_target_ref {
                        continue;
                    }
                    first_touch_effects.insert(table_key.clone());
                    recovery_pins.push(crate::db::manifest::SidecarTablePin {
                        table_key: table_key.clone(),
                        table_path: self.storage().dataset_uri(&source_entry.table_path),
                        expected_version,
                        post_commit_pin: source_entry.table_version,
                        confirmed_version: None,
                        table_branch: Some(target.to_string()),
                    });
                    recovery_effects.push(crate::db::manifest::RecoveryBranchMergeEffect {
                        table_key: table_key.clone(),
                        kind: crate::db::manifest::RecoveryBranchMergeEffectKind::RefOnlyFork {
                            source_version: source_entry.table_version,
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
            let Some(prepared) = prepared_existing_targets.get(table_key) else {
                continue;
            };
            let expected_version = target_snapshot
                .entry(table_key)
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "prepared branch merge target '{table_key}' has no manifest entry"
                    ))
                })?
                .table_version;
            self.ensure_existing_effect_baseline(
                table_key,
                prepared.table_branch.as_deref(),
                expected_version,
                &prepared.current,
            )
            .await?;
        }

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
            let sidecar = crate::db::manifest::new_branch_merge_sidecar(
                active_branch_for_keys.clone(),
                actor_id.map(str::to_string),
                recovery_pins,
                authority,
                recovery_lineage,
                recovery_effects,
                crate::db::manifest::RecoveryManifestDelta {
                    table_updates: delta_slots,
                    registrations: Vec::new(),
                    tombstones: Vec::new(),
                },
            )?;
            let handle = crate::db::manifest::write_sidecar(
                self.root_uri(),
                self.storage_adapter(),
                &sidecar,
            )
            .await?;
            Some((sidecar, handle))
        };

        let recovery_operation_id = recovery
            .as_ref()
            .map(|(_, handle)| handle.operation_id.clone());
        let post_arm_result = async {
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
                let prepared_target = match candidate_state {
                    CandidateTableState::RewriteMerged(_)
                    | CandidateTableState::AdoptWithDelta(_) => {
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
                        )
                        .await?
                    }
                    CandidateTableState::AdoptWithDelta(delta) => {
                        let planned = planned_transactions_by_table.get(table_key).ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "branch merge table '{table_key}' lacks its armed transaction chain"
                            ))
                        })?;
                        publish_adopted_delta(self, table_key, delta, prepared_target, planned)
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
                        .entry(table_key)
                        .or_else(|| source_snapshot.entry(table_key))
                        .ok_or_else(|| {
                            OmniError::manifest_internal(format!(
                                "first-touch merge effect '{}' has no table path",
                                table_key
                            ))
                        })?;
                    let full_path = self.storage().dataset_uri(&entry.table_path);
                    let target_head = self
                        .storage()
                        .open_dataset_head(&full_path, Some(target))
                        .await?;
                    confirmed_ref_identifiers.insert(
                        table_key.clone(),
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
                crate::db::manifest::confirm_branch_merge_sidecar_phase_b(
                    self.root_uri(),
                    self.storage_adapter(),
                    sidecar,
                    &updates,
                    &confirmed_ref_identifiers,
                )
                .await?;
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
            self.commit_updates_on_branch_with_expected(
                target_branch,
                &updates,
                &expected_versions,
                actor_id,
                target_txn,
                merge_lineage,
            )
            .await?;

            Ok::<_, OmniError>((updates, changed_edge_tables))
        }
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
