use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array, UInt64Array,
    builder::{
        BooleanBuilder, Date32Builder, Date64Builder, FixedSizeListBuilder, Float32Builder,
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, UInt32Builder,
        UInt64Builder,
    },
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::BlobArrayBuilder;
use lance::dataset::scanner::{ColumnOrdering, DatasetRecordBatchStream};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{
    IRAssignment, IRExpr, IRFilter, IRMutationPredicate, IROp, IROrdering, IRProjection,
    MutationOpIR, ParamMap, QueryIR,
};
use omnigraph_compiler::lower_mutation_query;
use omnigraph_compiler::lower_query;
use omnigraph_compiler::query::ast::{CompOp, Literal, NOW_PARAM_NAME};
use omnigraph_compiler::query::typecheck::{CheckedQuery, typecheck_query, typecheck_query_decl};
use omnigraph_compiler::result::{MutationResult, QueryResult};
use omnigraph_compiler::types::ScalarType;
use omnigraph_compiler::types::Direction;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::embedding::EmbeddingClient;
use crate::db::commit_graph::CommitGraph;
use crate::db::manifest::ManifestCoordinator;
use crate::db::{MergeOutcome, Omnigraph, is_internal_run_branch};
use crate::db::{ReadTarget, Snapshot};
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::graph_index::GraphIndex;
use tempfile::{Builder as TempDirBuilder, TempDir};

impl Omnigraph {
    /// Run a named query against an explicit branch or snapshot target.
    pub async fn query(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<QueryResult> {
        let resolved = self.resolved_target(target).await?;

        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        let type_ctx = typecheck_query(self.catalog(), &query_decl)?;
        let ir = lower_query(self.catalog(), &query_decl, &type_ctx)?;

        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        let graph_index = if needs_graph {
            Some(self.graph_index_for_resolved(&resolved).await?)
        } else {
            None
        };

        execute_query(
            &ir,
            params,
            &resolved.snapshot,
            graph_index.as_deref(),
            self.catalog(),
        )
        .await
    }

    /// Run a named query against the graph as it existed at a prior manifest version.
    ///
    /// Compiles the query normally, builds a temporary (non-cached) graph index
    /// if traversal is needed, and executes against the historical snapshot.
    pub async fn run_query_at(
        &self,
        version: u64,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<QueryResult> {
        let snapshot = self.snapshot_at_version(version).await?;

        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        let type_ctx = typecheck_query(self.catalog(), &query_decl)?;
        let ir = lower_query(self.catalog(), &query_decl, &type_ctx)?;

        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        let graph_index = if needs_graph {
            let edge_types = self
                .catalog()
                .edge_types
                .iter()
                .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
                .collect();
            Some(Arc::new(
                GraphIndex::build(&snapshot, &edge_types).await?,
            ))
        } else {
            None
        };

        execute_query(
            &ir,
            params,
            &snapshot,
            graph_index.as_deref(),
            self.catalog(),
        )
        .await
    }
}

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
        }),
        // Source on main, target on branch — pointer switch to main version
        // (target reads from main, same lineage)
        (Some(_target_branch), None) => Ok(crate::db::SubTableUpdate {
            table_key: table_key.to_string(),
            table_version: source_entry.table_version,
            table_branch: None,
            row_count: source_entry.row_count,
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
                Ok(crate::db::SubTableUpdate {
                    table_key: table_key.to_string(),
                    table_version: ds.version().version,
                    table_branch: Some(target_branch.to_string()),
                    row_count: source_entry.row_count,
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
    // existing rows, bumps _row_last_updated_at_version only for actually-changed rows)
    if let Some(delta) = &staged.delta_staged {
        let batches: Vec<RecordBatch> = target_db
            .table_store()
            .scan_batches(&delta.dataset)
            .await?
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        if !batches.is_empty() {
            let state = target_db
                .table_store()
                .merge_insert_batches(
                    current_ds,
                    batches,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::InsertAll,
                )
                .await?;
            current_ds = target_db
                .reopen_for_mutation(
                    table_key,
                    &full_path,
                    table_branch.as_deref(),
                    state.version,
                )
                .await?;
        }
    }

    // Phase 2: delete removed rows via deletion vectors
    if !staged.deleted_ids.is_empty() {
        let escaped: Vec<String> = staged
            .deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let filter = format!("id IN ({})", escaped.join(", "));
        target_db
            .table_store()
            .delete_where(&mut current_ds, &filter)
            .await?;
    }

    // Phase 3: rebuild indices
    let row_count = target_db
        .table_store()
        .table_state(&current_ds)
        .await?
        .row_count;
    if row_count > 0 {
        target_db
            .build_indices_on_dataset(table_key, &mut current_ds)
            .await?;
    }

    Ok(crate::db::SubTableUpdate {
        table_key: table_key.to_string(),
        table_version: current_ds.version().version,
        table_branch,
        row_count,
    })
}

// ─── Search mode ─────────────────────────────────────────────────────────────

/// Describes how the query's ordering changes the scan mode.
#[derive(Debug, Default)]
struct SearchMode {
    /// Vector ANN search: (variable, property, query_vector, k).
    nearest: Option<(String, String, Vec<f32>, usize)>,
    /// BM25 full-text search: (variable, property, query_text).
    bm25: Option<(String, String, String)>,
    /// RRF fusion: (primary, secondary, k_constant, limit).
    rrf: Option<RrfMode>,
}

#[derive(Debug)]
struct RrfMode {
    primary: Box<SearchMode>,
    secondary: Box<SearchMode>,
    k: u32,
    limit: usize,
}

/// Extract search ordering mode from the IR.
async fn extract_search_mode(
    ir: &QueryIR,
    params: &ParamMap,
    catalog: &Catalog,
) -> Result<SearchMode> {
    if ir.order_by.is_empty() {
        return Ok(SearchMode::default());
    }
    let ordering = &ir.order_by[0];
    match &ordering.expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let vec =
                resolve_nearest_query_vec(ir, catalog, variable, property, query, params).await?;
            let k = ir.limit.ok_or_else(|| {
                OmniError::manifest("nearest() ordering requires a limit clause".to_string())
            })? as usize;
            Ok(SearchMode {
                nearest: Some((variable.clone(), property.clone(), vec, k)),
                ..Default::default()
            })
        }
        IRExpr::Bm25 { field, query } => {
            let var = match field.as_ref() {
                IRExpr::PropAccess { variable, .. } => variable.clone(),
                _ => {
                    return Err(OmniError::manifest(
                        "bm25 field must be a property access".to_string(),
                    ));
                }
            };
            let prop = extract_property(field).ok_or_else(|| {
                OmniError::manifest("bm25 field must be a property access".to_string())
            })?;
            let text = resolve_to_string(query, params).ok_or_else(|| {
                OmniError::manifest("bm25 query must resolve to a string".to_string())
            })?;
            Ok(SearchMode {
                bm25: Some((var, prop, text)),
                ..Default::default()
            })
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            let limit = ir.limit.ok_or_else(|| {
                OmniError::manifest("rrf() ordering requires a limit clause".to_string())
            })? as usize;
            let k_val = k
                .as_ref()
                .and_then(|e| resolve_to_int(e, params))
                .unwrap_or(60) as u32;

            let primary_mode =
                extract_sub_search_mode(ir, primary, params, catalog, ir.limit).await?;
            let secondary_mode =
                extract_sub_search_mode(ir, secondary, params, catalog, ir.limit).await?;

            Ok(SearchMode {
                rrf: Some(RrfMode {
                    primary: Box::new(primary_mode),
                    secondary: Box::new(secondary_mode),
                    k: k_val,
                    limit,
                }),
                ..Default::default()
            })
        }
        _ => Ok(SearchMode::default()),
    }
}

/// Extract a sub-search mode from a nested RRF expression (nearest or bm25).
async fn extract_sub_search_mode(
    ir: &QueryIR,
    expr: &IRExpr,
    params: &ParamMap,
    catalog: &Catalog,
    limit: Option<u64>,
) -> Result<SearchMode> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let vec =
                resolve_nearest_query_vec(ir, catalog, variable, property, query, params).await?;
            let k = limit.unwrap_or(100) as usize;
            Ok(SearchMode {
                nearest: Some((variable.clone(), property.clone(), vec, k)),
                ..Default::default()
            })
        }
        IRExpr::Bm25 { field, query } => {
            let var = match field.as_ref() {
                IRExpr::PropAccess { variable, .. } => variable.clone(),
                _ => {
                    return Err(OmniError::manifest(
                        "bm25 field must be a property access".to_string(),
                    ));
                }
            };
            let prop = extract_property(field).ok_or_else(|| {
                OmniError::manifest("bm25 field must be a property access".to_string())
            })?;
            let text = resolve_to_string(query, params).ok_or_else(|| {
                OmniError::manifest("bm25 query must resolve to a string".to_string())
            })?;
            Ok(SearchMode {
                bm25: Some((var, prop, text)),
                ..Default::default()
            })
        }
        _ => Ok(SearchMode::default()),
    }
}

/// Resolve an expression to a nearest() query vector.
async fn resolve_nearest_query_vec(
    ir: &QueryIR,
    catalog: &Catalog,
    variable: &str,
    property: &str,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<Vec<f32>> {
    let lit = resolve_literal_or_param(expr, params)?;
    match lit {
        Literal::List(_) => literal_to_f32_vec(&lit),
        Literal::String(text) => {
            let expected_dim =
                nearest_property_dimension(ir, catalog, variable, property)?;
            EmbeddingClient::from_env()?
                .embed_query_text(&text, expected_dim)
                .await
        }
        _ => Err(OmniError::manifest(
            "nearest query must be a string or list of floats".to_string(),
        )),
    }
}

fn resolve_literal_or_param(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    Ok(match expr {
        IRExpr::Literal(lit) => lit.clone(),
        IRExpr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?,
        _ => {
            return Err(OmniError::manifest(
                "nearest query must be a literal or parameter".to_string(),
            ));
        }
    })
}

/// Resolve a literal vector expression to a Vec<f32>.
fn literal_to_f32_vec(lit: &Literal) -> Result<Vec<f32>> {
    match lit {
        Literal::List(items) => items
            .iter()
            .map(|item| match item {
                Literal::Float(f) => Ok(*f as f32),
                Literal::Integer(n) => Ok(*n as f32),
                _ => Err(OmniError::manifest(
                    "vector elements must be numeric".to_string(),
                )),
            })
            .collect(),
        _ => Err(OmniError::manifest(
            "nearest query must be a list of floats".to_string(),
        )),
    }
}

fn nearest_property_dimension(
    ir: &QueryIR,
    catalog: &Catalog,
    variable: &str,
    property: &str,
) -> Result<usize> {
    let type_name = resolve_binding_type_name(&ir.pipeline, variable).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() variable '${}' is not bound to a node type in the lowered pipeline",
            variable
        ))
    })?;
    let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() binding '${}' resolved unknown node type '{}'",
            variable, type_name
        ))
    })?;
    let prop = node_type.properties.get(property).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "nearest() property '{}.{}' is missing from the catalog",
            type_name, property
        ))
    })?;
    match prop.scalar {
        ScalarType::Vector(dim) if !prop.list => Ok(dim as usize),
        _ => Err(OmniError::manifest_internal(format!(
            "nearest() property '{}.{}' is not a scalar vector",
            type_name, property
        ))),
    }
}

fn resolve_binding_type_name<'a>(pipeline: &'a [IROp], variable: &str) -> Option<&'a str> {
    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable: bound_var,
                type_name,
                ..
            } if bound_var == variable => return Some(type_name.as_str()),
            IROp::Expand {
                dst_var, dst_type, ..
            } if dst_var == variable => return Some(dst_type.as_str()),
            IROp::AntiJoin { inner, .. } => {
                if let Some(type_name) = resolve_binding_type_name(inner, variable) {
                    return Some(type_name);
                }
            }
            _ => {}
        }
    }
    None
}

/// Execute a lowered QueryIR. Pure function — no state, no caches.
pub async fn execute_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let search_mode = extract_search_mode(ir, params, catalog).await?;

    // RRF requires forked execution
    if let Some(ref rrf) = search_mode.rrf {
        return execute_rrf_query(ir, params, snapshot, graph_index, catalog, rrf).await;
    }

    let mut bindings: HashMap<String, RecordBatch> = HashMap::new();

    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut bindings,
        &search_mode,
    )
    .await?;

    // Project return expressions
    let mut result_batch = project_return(&bindings, &ir.return_exprs, params)?;

    // Apply ordering (skip if search mode already ordered the results)
    if !ir.order_by.is_empty() && !is_search_ordered(&search_mode) {
        result_batch = apply_ordering(result_batch, &ir.order_by, &bindings, params)?;
    }

    // Apply limit
    if let Some(limit) = ir.limit {
        let len = result_batch.num_rows().min(limit as usize);
        result_batch = result_batch.slice(0, len);
    }

    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}

/// Check if the search mode already returns results in the correct order.
fn is_search_ordered(search_mode: &SearchMode) -> bool {
    search_mode.nearest.is_some() || search_mode.bm25.is_some()
}

/// Execute a query with RRF (Reciprocal Rank Fusion) ordering.
async fn execute_rrf_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    rrf: &RrfMode,
) -> Result<QueryResult> {
    // Execute primary search
    let mut primary_bindings: HashMap<String, RecordBatch> = HashMap::new();
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut primary_bindings,
        &rrf.primary,
    )
    .await?;

    // Execute secondary search
    let mut secondary_bindings: HashMap<String, RecordBatch> = HashMap::new();
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut secondary_bindings,
        &rrf.secondary,
    )
    .await?;

    // For RRF, we need to find the main binding variable
    // (the one that both searches operate on)
    let primary_var = rrf
        .primary
        .nearest
        .as_ref()
        .map(|(v, ..)| v.as_str())
        .or_else(|| rrf.primary.bm25.as_ref().map(|(v, ..)| v.as_str()))
        .ok_or_else(|| OmniError::manifest("rrf primary must be nearest or bm25".to_string()))?;

    let primary_batch = primary_bindings.get(primary_var).ok_or_else(|| {
        OmniError::manifest(format!(
            "rrf primary variable '{}' not in bindings",
            primary_var
        ))
    })?;
    let secondary_batch = secondary_bindings.get(primary_var).ok_or_else(|| {
        OmniError::manifest(format!(
            "rrf secondary variable '{}' not in bindings",
            primary_var
        ))
    })?;

    // Build ID → rank maps
    let primary_ids = extract_id_column(primary_batch)?;
    let secondary_ids = extract_id_column(secondary_batch)?;

    let mut primary_rank: HashMap<String, usize> = HashMap::new();
    for (i, id) in primary_ids.iter().enumerate() {
        primary_rank.entry(id.clone()).or_insert(i);
    }
    let mut secondary_rank: HashMap<String, usize> = HashMap::new();
    for (i, id) in secondary_ids.iter().enumerate() {
        secondary_rank.entry(id.clone()).or_insert(i);
    }

    // Collect all unique IDs
    let mut all_ids: Vec<String> = primary_ids.clone();
    for id in &secondary_ids {
        if !primary_rank.contains_key(id) {
            all_ids.push(id.clone());
        }
    }

    // Compute RRF scores
    let k = rrf.k as f64;
    let mut scored: Vec<(String, f64)> = all_ids
        .iter()
        .map(|id| {
            let p = primary_rank
                .get(id)
                .map(|&r| 1.0 / (k + r as f64 + 1.0))
                .unwrap_or(0.0);
            let s = secondary_rank
                .get(id)
                .map(|&r| 1.0 / (k + r as f64 + 1.0))
                .unwrap_or(0.0);
            (id.clone(), p + s)
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(rrf.limit);

    // Collect winning IDs in order — look up rows from primary or secondary batch
    let winning_ids: Vec<String> = scored.iter().map(|(id, _)| id.clone()).collect();

    // Build a combined row source: merge primary and secondary by id
    let mut id_to_batch_row: HashMap<String, (&RecordBatch, usize)> = HashMap::new();
    for (i, id) in primary_ids.iter().enumerate() {
        id_to_batch_row
            .entry(id.clone())
            .or_insert((primary_batch, i));
    }
    for (i, id) in secondary_ids.iter().enumerate() {
        id_to_batch_row
            .entry(id.clone())
            .or_insert((secondary_batch, i));
    }

    // Reconstruct a combined batch for the binding in winning order
    let fused_batch = build_fused_batch(&winning_ids, &id_to_batch_row, primary_batch.schema())?;

    // Replace the binding and project
    let mut fused_bindings = primary_bindings;
    fused_bindings.insert(primary_var.to_string(), fused_batch);

    let result_batch = project_return(&fused_bindings, &ir.return_exprs, params)?;

    // Already ordered by RRF score + already limited
    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}

fn extract_id_column(batch: &RecordBatch) -> Result<Vec<String>> {
    let col = batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("batch missing 'id' column for RRF".to_string()))?;
    let ids = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("'id' column is not Utf8".to_string()))?;
    Ok((0..ids.len()).map(|i| ids.value(i).to_string()).collect())
}

fn build_fused_batch(
    ordered_ids: &[String],
    id_to_batch_row: &HashMap<String, (&RecordBatch, usize)>,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    if ordered_ids.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Gather indices from source batches, collecting rows in the right order
    let mut row_slices: Vec<RecordBatch> = Vec::with_capacity(ordered_ids.len());
    for id in ordered_ids {
        if let Some(&(batch, row_idx)) = id_to_batch_row.get(id) {
            row_slices.push(batch.slice(row_idx, 1));
        }
    }

    if row_slices.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let schema = row_slices[0].schema();
    arrow_select::concat::concat_batches(&schema, &row_slices)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Check if a filter is a text search filter that needs Lance SQL pushdown.
fn is_search_filter(filter: &IRFilter) -> bool {
    matches!(
        &filter.left,
        IRExpr::Search { .. } | IRExpr::Fuzzy { .. } | IRExpr::MatchText { .. }
    )
}

/// Extract the variable name from a search filter's field expression.
fn search_filter_variable(filter: &IRFilter) -> Option<&str> {
    let field = match &filter.left {
        IRExpr::Search { field, .. } => field,
        IRExpr::Fuzzy { field, .. } => field,
        IRExpr::MatchText { field, .. } => field,
        _ => return None,
    };
    match field.as_ref() {
        IRExpr::PropAccess { variable, .. } => Some(variable.as_str()),
        _ => None,
    }
}

fn execute_pipeline<'a>(
    pipeline: &'a [IROp],
    params: &'a ParamMap,
    snapshot: &'a Snapshot,
    graph_index: Option<&'a GraphIndex>,
    catalog: &'a Catalog,
    bindings: &'a mut HashMap<String, RecordBatch>,
    search_mode: &'a SearchMode,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        // Pre-pass: collect search filters that need to be hoisted to NodeScan
        let mut hoisted_search_filters: HashMap<String, Vec<IRFilter>> = HashMap::new();
        let mut hoisted_indices: HashSet<usize> = HashSet::new();
        for (i, op) in pipeline.iter().enumerate() {
            if let IROp::Filter(filter) = op {
                if is_search_filter(filter) {
                    if let Some(var) = search_filter_variable(filter) {
                        hoisted_search_filters
                            .entry(var.to_string())
                            .or_default()
                            .push(filter.clone());
                        hoisted_indices.insert(i);
                    }
                }
            }
        }

        for (i, op) in pipeline.iter().enumerate() {
            // Skip hoisted search filters
            if hoisted_indices.contains(&i) {
                continue;
            }
            match op {
                IROp::NodeScan {
                    variable,
                    type_name,
                    filters,
                } => {
                    // Merge inline filters with hoisted search filters
                    let mut all_filters: Vec<IRFilter> = filters.clone();
                    if let Some(extra) = hoisted_search_filters.get(variable) {
                        all_filters.extend(extra.iter().cloned());
                    }
                    let batch = execute_node_scan(
                        type_name,
                        variable,
                        &all_filters,
                        params,
                        snapshot,
                        catalog,
                        search_mode,
                    )
                    .await?;
                    bindings.insert(variable.clone(), batch);
                }
                IROp::Filter(filter) => {
                    apply_filter(bindings, filter, params)?;
                }
                IROp::Expand {
                    src_var,
                    dst_var,
                    edge_type,
                    direction,
                    dst_type,
                    min_hops,
                    max_hops,
                } => {
                    let gi = graph_index.ok_or_else(|| {
                        OmniError::manifest("graph index required for traversal".to_string())
                    })?;
                    let batch = execute_expand(
                        bindings, gi, snapshot, catalog, src_var, dst_var, edge_type, *direction,
                        dst_type, *min_hops, *max_hops,
                    )
                    .await?;
                    bindings.insert(dst_var.clone(), batch);
                }
                IROp::AntiJoin { outer_var, inner } => {
                    let gi = graph_index;
                    execute_anti_join(bindings, inner, params, snapshot, gi, catalog, outer_var)
                        .await?;
                }
            }
        }
        Ok(())
    })
}

/// Execute a graph traversal (Expand).
async fn execute_expand(
    bindings: &HashMap<String, RecordBatch>,
    graph_index: &GraphIndex,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    _dst_var: &str,
    edge_type: &str,
    direction: Direction,
    dst_type: &str,
    min_hops: u32,
    max_hops: Option<u32>,
) -> Result<RecordBatch> {
    let src_batch = bindings.get(src_var).ok_or_else(|| {
        OmniError::manifest(format!("expand references unbound variable '{}'", src_var))
    })?;

    let src_ids = src_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("source batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("source 'id' column is not Utf8".to_string()))?;

    // Determine which type index to use for source and destination
    let edge_def = catalog
        .edge_types
        .get(edge_type)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_type)))?;

    let (src_type_name, dst_type_name) = match direction {
        Direction::Out => (&edge_def.from_type, &edge_def.to_type),
        Direction::In => (&edge_def.to_type, &edge_def.from_type),
    };

    let src_type_idx = graph_index
        .type_index(src_type_name)
        .ok_or_else(|| OmniError::manifest(format!("no type index for '{}'", src_type_name)))?;
    let dst_type_idx = graph_index
        .type_index(dst_type_name)
        .ok_or_else(|| OmniError::manifest(format!("no type index for '{}'", dst_type_name)))?;

    let adj = match direction {
        Direction::Out => graph_index.csr(edge_type),
        Direction::In => graph_index.csc(edge_type),
    }
    .ok_or_else(|| OmniError::manifest(format!("no adjacency index for edge '{}'", edge_type)))?;

    let max = max_hops.unwrap_or(min_hops.max(1));

    let same_type = src_type_name == dst_type_name;

    // BFS to collect reachable destination dense IDs
    let mut result_dst_ids: Vec<String> = Vec::new();
    for i in 0..src_ids.len() {
        let src_id = src_ids.value(i);
        let Some(src_dense) = src_type_idx.to_dense(src_id) else {
            continue;
        };

        // BFS with hop tracking
        let mut frontier: Vec<u32> = vec![src_dense];
        let mut visited: HashSet<u32> = HashSet::new();
        let mut seen_dst_ids: HashSet<String> = HashSet::new();
        // Only track visited in the destination namespace for same-type edges
        // (to avoid revisiting the source). For cross-type edges, dense indices
        // are in different namespaces so collision is impossible.
        if same_type {
            visited.insert(src_dense);
        }

        for hop in 1..=max {
            let mut next_frontier = Vec::new();
            for &node in &frontier {
                for &neighbor in adj.neighbors(node) {
                    if !same_type || visited.insert(neighbor) {
                        next_frontier.push(neighbor);
                        if hop >= min_hops {
                            if let Some(dst_id) = dst_type_idx.to_id(neighbor) {
                                let dst_id = dst_id.to_string();
                                if seen_dst_ids.insert(dst_id.clone()) {
                                    result_dst_ids.push(dst_id);
                                }
                            }
                        }
                    }
                }
            }
            frontier = next_frontier;
            if frontier.is_empty() {
                break;
            }
        }
    }

    // Hydrate destination nodes from the snapshot
    hydrate_nodes(snapshot, catalog, dst_type, &result_dst_ids).await
}

/// Load full node rows for a set of IDs from a snapshot.
async fn hydrate_nodes(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    ids: &[String],
) -> Result<RecordBatch> {
    let node_type = catalog
        .node_types
        .get(type_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;

    if ids.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    }

    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open(&table_key).await?;

    // Build filter: id IN ('a', 'b', 'c')
    let escaped: Vec<String> = ids
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "''")))
        .collect();
    let filter_sql = format!("id IN ({})", escaped.join(", "));
    let has_blobs = !node_type.blob_properties.is_empty();
    let non_blob_cols: Vec<&str> = node_type
        .arrow_schema
        .fields()
        .iter()
        .filter(|f| !node_type.blob_properties.contains(f.name()))
        .map(|f| f.name().as_str())
        .collect();
    let projection = has_blobs.then_some(non_blob_cols.as_slice());
    let batches = crate::table_store::TableStore::scan_stream(
        &ds,
        projection,
        Some(&filter_sql),
        None,
        false,
    )
    .await?
    .try_collect::<Vec<RecordBatch>>()
    .await
    .map_err(|e| OmniError::Lance(e.to_string()))?;

    let scan_result = if batches.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| OmniError::Lance(e.to_string()))?
    };

    if has_blobs {
        return add_null_blob_columns(&scan_result, node_type);
    }
    Ok(scan_result)
}

/// Try bulk anti-join via CSR existence check. Returns Some if the inner
/// pipeline is a single Expand from outer_var (the common negation pattern).
fn try_bulk_anti_join(
    outer_batch: &RecordBatch,
    inner_pipeline: &[IROp],
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    outer_var: &str,
) -> Option<Result<RecordBatch>> {
    if inner_pipeline.len() != 1 {
        return None;
    }
    let IROp::Expand {
        src_var,
        edge_type,
        direction,
        ..
    } = &inner_pipeline[0]
    else {
        return None;
    };
    if src_var != outer_var {
        return None;
    }
    let gi = graph_index?;
    let edge_def = catalog.edge_types.get(edge_type.as_str())?;

    let src_type_name = match direction {
        Direction::Out => &edge_def.from_type,
        Direction::In => &edge_def.to_type,
    };
    let adj = match direction {
        Direction::Out => gi.csr(edge_type),
        Direction::In => gi.csc(edge_type),
    }?;
    let type_idx = gi.type_index(src_type_name)?;

    let outer_ids = outer_batch
        .column_by_name("id")?
        .as_any()
        .downcast_ref::<StringArray>()?;

    let keep_mask: Vec<bool> = (0..outer_ids.len())
        .map(|i| {
            let id = outer_ids.value(i);
            match type_idx.to_dense(id) {
                Some(dense) => !adj.has_neighbors(dense),
                None => true, // not in graph index = no edges = keep
            }
        })
        .collect();

    let mask = BooleanArray::from(keep_mask);
    Some(
        arrow_select::filter::filter_record_batch(outer_batch, &mask)
            .map_err(|e| OmniError::Lance(e.to_string())),
    )
}

/// Execute an AntiJoin: remove rows from outer_var where the inner pipeline finds matches.
async fn execute_anti_join(
    bindings: &mut HashMap<String, RecordBatch>,
    inner_pipeline: &[IROp],
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    outer_var: &str,
) -> Result<()> {
    let outer_batch = bindings.get(outer_var).ok_or_else(|| {
        OmniError::manifest(format!(
            "anti-join references unbound variable '{}'",
            outer_var
        ))
    })?;

    // Fast path: bulk CSR existence check (O(N), zero Lance I/O)
    if let Some(result) =
        try_bulk_anti_join(outer_batch, inner_pipeline, graph_index, catalog, outer_var)
    {
        bindings.insert(outer_var.to_string(), result?);
        return Ok(());
    }

    // Slow path: per-row inner pipeline execution
    let outer_ids = outer_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("outer batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("outer 'id' column is not Utf8".to_string()))?;

    let mut keep_mask = vec![true; outer_batch.num_rows()];

    for i in 0..outer_ids.len() {
        let single_row = outer_batch.slice(i, 1);
        let mut inner_bindings: HashMap<String, RecordBatch> = HashMap::new();
        inner_bindings.insert(outer_var.to_string(), single_row);

        let no_search = SearchMode::default();
        execute_pipeline(
            inner_pipeline,
            params,
            snapshot,
            graph_index,
            catalog,
            &mut inner_bindings,
            &no_search,
        )
        .await?;

        let has_match = inner_bindings
            .iter()
            .filter(|(k, _)| *k != outer_var)
            .any(|(_, batch)| batch.num_rows() > 0);

        if has_match {
            keep_mask[i] = false;
        }
    }

    let mask = BooleanArray::from(keep_mask);
    let filtered = arrow_select::filter::filter_record_batch(outer_batch, &mask)
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    bindings.insert(outer_var.to_string(), filtered);
    Ok(())
}

/// Scan a node type's Lance dataset with optional filter pushdown and search modes.
async fn execute_node_scan(
    type_name: &str,
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
    search_mode: &SearchMode,
) -> Result<RecordBatch> {
    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open(&table_key).await?;

    // Build Lance SQL filter string from non-search IR filters
    let filter_sql = build_lance_filter(filters, params);

    // Blob columns must be excluded from scan when a filter is present
    // (Lance bug: BlobsDescriptions + filter triggers a projection assertion).
    // We exclude blob columns and add metadata post-scan via take_blobs_by_indices.
    let node_type = &catalog.node_types[type_name];
    let has_blobs = !node_type.blob_properties.is_empty();
    let non_blob_cols: Vec<&str> = node_type
        .arrow_schema
        .fields()
        .iter()
        .filter(|f| !node_type.blob_properties.contains(f.name()))
        .map(|f| f.name().as_str())
        .collect();
    let projection = has_blobs.then_some(non_blob_cols.as_slice());
    let batches = crate::table_store::TableStore::scan_stream_with(
        &ds,
        projection,
        filter_sql.as_deref(),
        None,
        false,
        |scanner| {
            // Apply FTS queries from hoisted search filters (search/fuzzy/match_text in match clause)
            for filter in filters {
                if is_search_filter(filter) {
                    if let Some(fts_query) = build_fts_query(&filter.left, params) {
                        scanner.full_text_search(fts_query).map_err(|e| {
                            OmniError::Lance(format!("full_text_search filter: {}", e))
                        })?;
                    }
                }
            }

            // Apply nearest vector search if this variable is the target
            if let Some((ref var, ref prop, ref vec, k)) = search_mode.nearest {
                if var == variable {
                    let query_arr = Float32Array::from(vec.clone());
                    scanner
                        .nearest(prop, &query_arr, k)
                        .map_err(|e| OmniError::Lance(format!("nearest: {}", e)))?;
                }
            }

            // Apply BM25 full-text search if this variable is the target
            if let Some((ref var, ref prop, ref text)) = search_mode.bm25 {
                if var == variable {
                    let fts_query = lance_index::scalar::FullTextSearchQuery::new(text.clone())
                        .with_column(prop.clone())
                        .map_err(|e| OmniError::Lance(format!("fts with_column: {}", e)))?;
                    scanner
                        .full_text_search(fts_query)
                        .map_err(|e| OmniError::Lance(format!("full_text_search: {}", e)))?;
                }
            }
            Ok(())
        },
    )
    .await?
    .try_collect::<Vec<RecordBatch>>()
    .await
    .map_err(|e| OmniError::Lance(e.to_string()))?;

    let scan_result = if batches.is_empty() {
        RecordBatch::new_empty(batches.first().map(|b| b.schema()).unwrap_or_else(|| {
            // Build a non-blob schema for empty result
            let fields: Vec<_> = node_type
                .arrow_schema
                .fields()
                .iter()
                .filter(|f| !node_type.blob_properties.contains(f.name()))
                .map(|f| f.as_ref().clone())
                .collect();
            Arc::new(Schema::new(fields))
        }))
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| OmniError::Lance(e.to_string()))?
    };

    // Add null placeholder columns for excluded blob properties
    if has_blobs {
        return add_null_blob_columns(&scan_result, node_type);
    }
    Ok(scan_result)
}

/// Add null Utf8 columns for blob properties excluded from a scan.
/// Uses column_by_name (not positional) so it's order-independent.
fn add_null_blob_columns(
    batch: &RecordBatch,
    node_type: &omnigraph_compiler::catalog::NodeType,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut fields = Vec::with_capacity(node_type.arrow_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(node_type.arrow_schema.fields().len());

    for field in node_type.arrow_schema.fields() {
        if node_type.blob_properties.contains(field.name()) {
            fields.push(Field::new(field.name(), DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(vec![None::<&str>; num_rows])));
        } else if let Some(col) = batch.column_by_name(field.name()) {
            let batch_schema = batch.schema();
            let batch_field = batch_schema
                .field_with_name(field.name())
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            fields.push(batch_field.clone());
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Convert IR filters to a Lance SQL filter string.
fn build_lance_filter(filters: &[IRFilter], params: &ParamMap) -> Option<String> {
    if filters.is_empty() {
        return None;
    }

    let parts: Vec<String> = filters
        .iter()
        .filter_map(|f| ir_filter_to_sql(f, params))
        .collect();

    if parts.is_empty() {
        return None;
    }

    Some(parts.join(" AND "))
}

fn ir_filter_to_sql(filter: &IRFilter, params: &ParamMap) -> Option<String> {
    // Search predicates (search/fuzzy/match_text = true) are NOT converted to SQL.
    // They are handled via scanner.full_text_search() in execute_node_scan.
    if is_search_filter(filter) {
        return None;
    }

    let left = ir_expr_to_sql(&filter.left, params)?;
    let right = ir_expr_to_sql(&filter.right, params)?;
    let op = match filter.op {
        CompOp::Eq => "=",
        CompOp::Ne => "!=",
        CompOp::Gt => ">",
        CompOp::Lt => "<",
        CompOp::Ge => ">=",
        CompOp::Le => "<=",
        CompOp::Contains => return None, // Can't pushdown list contains
    };
    Some(format!("{} {} {}", left, op, right))
}

/// Build a FullTextSearchQuery from a search IR expression.
fn build_fts_query(
    expr: &IRExpr,
    params: &ParamMap,
) -> Option<lance_index::scalar::FullTextSearchQuery> {
    match expr {
        IRExpr::Search { field, query } => {
            let prop = extract_property(field)?;
            let q = resolve_to_string(query, params)?;
            lance_index::scalar::FullTextSearchQuery::new(q)
                .with_column(prop)
                .ok()
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            let prop = extract_property(field)?;
            let q = resolve_to_string(query, params)?;
            let edits = max_edits
                .as_ref()
                .and_then(|e| resolve_to_int(e, params))
                .unwrap_or(2) as u32;
            lance_index::scalar::FullTextSearchQuery::new_fuzzy(q, Some(edits))
                .with_column(prop)
                .ok()
        }
        IRExpr::MatchText { field, query } => {
            // Use regular text search (phrase search not available in Lance 3.0 Rust API)
            let prop = extract_property(field)?;
            let q = resolve_to_string(query, params)?;
            lance_index::scalar::FullTextSearchQuery::new(q)
                .with_column(prop)
                .ok()
        }
        _ => None,
    }
}

/// Extract the property name from a PropAccess expression.
fn extract_property(expr: &IRExpr) -> Option<String> {
    match expr {
        IRExpr::PropAccess { property, .. } => Some(property.clone()),
        _ => None,
    }
}

/// Resolve an expression to a string value (literal or param).
fn resolve_to_string(expr: &IRExpr, params: &ParamMap) -> Option<String> {
    match expr {
        IRExpr::Literal(Literal::String(s)) => Some(s.clone()),
        IRExpr::Param(name) => match params.get(name)? {
            Literal::String(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Resolve an expression to an integer value (literal or param).
fn resolve_to_int(expr: &IRExpr, params: &ParamMap) -> Option<i64> {
    match expr {
        IRExpr::Literal(Literal::Integer(n)) => Some(*n),
        IRExpr::Param(name) => match params.get(name)? {
            Literal::Integer(n) => Some(*n),
            _ => None,
        },
        _ => None,
    }
}

fn ir_expr_to_sql(expr: &IRExpr, params: &ParamMap) -> Option<String> {
    match expr {
        IRExpr::PropAccess { property, .. } => Some(property.clone()),
        IRExpr::Literal(lit) => Some(literal_to_sql(lit)),
        IRExpr::Param(name) => params.get(name).map(literal_to_sql),
        _ => None,
    }
}

fn literal_to_sql(lit: &Literal) -> String {
    match lit {
        Literal::String(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::Integer(n) => n.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Date(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::DateTime(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::List(_) => "NULL".to_string(), // Not supported in SQL pushdown
    }
}

/// Apply an IR filter to the bindings (post-scan filtering).
fn apply_filter(
    bindings: &mut HashMap<String, RecordBatch>,
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<()> {
    // Find which binding this filter applies to
    let var_name = match &filter.left {
        IRExpr::PropAccess { variable, .. } => variable.clone(),
        _ => return Ok(()), // Can't determine variable
    };

    let batch = bindings.get(&var_name).ok_or_else(|| {
        OmniError::manifest(format!("filter references unbound variable '{}'", var_name))
    })?;

    let mask = evaluate_filter(batch, filter, params)?;
    let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    bindings.insert(var_name, filtered);
    Ok(())
}

/// Evaluate a filter predicate against a batch, producing a boolean mask.
fn evaluate_filter(
    batch: &RecordBatch,
    filter: &IRFilter,
    params: &ParamMap,
) -> Result<BooleanArray> {
    let left = evaluate_expr(batch, &filter.left, params)?;
    let right = evaluate_expr(batch, &filter.right, params)?;

    if filter.op == CompOp::Contains {
        return evaluate_contains_filter(&left, &right);
    }

    // Cast right to match left's type if needed (e.g. Int64 literal vs Int32 column)
    let right = if left.data_type() != right.data_type() {
        arrow_cast::cast::cast(&right, left.data_type())
            .map_err(|e| OmniError::Lance(e.to_string()))?
    } else {
        right
    };

    use arrow_ord::cmp;
    let result = match filter.op {
        CompOp::Eq => cmp::eq(&left, &right),
        CompOp::Ne => cmp::neq(&left, &right),
        CompOp::Gt => cmp::gt(&left, &right),
        CompOp::Lt => cmp::lt(&left, &right),
        CompOp::Ge => cmp::gt_eq(&left, &right),
        CompOp::Le => cmp::lt_eq(&left, &right),
        CompOp::Contains => unreachable!("handled above"),
    }
    .map_err(|e| OmniError::Lance(e.to_string()))?;

    Ok(result)
}

/// Evaluate an IR expression against a batch, producing an array.
fn evaluate_expr(batch: &RecordBatch, expr: &IRExpr, params: &ParamMap) -> Result<ArrayRef> {
    match expr {
        IRExpr::PropAccess { property, .. } => {
            batch.column_by_name(property).cloned().ok_or_else(|| {
                OmniError::manifest(format!("column '{}' not found in batch", property))
            })
        }
        IRExpr::Literal(lit) => literal_to_array(lit, batch.num_rows()),
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?;
            literal_to_array(lit, batch.num_rows())
        }
        _ => Err(OmniError::manifest(format!(
            "unsupported expression in filter: {:?}",
            expr
        ))),
    }
}

/// Create a constant array from a literal value.
fn literal_to_array(lit: &Literal, num_rows: usize) -> Result<ArrayRef> {
    Ok(match lit {
        Literal::String(s) => Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef,
        Literal::Integer(n) => {
            // Try to match the most common integer types
            Arc::new(Int64Array::from(vec![*n; num_rows])) as ArrayRef
        }
        Literal::Float(f) => Arc::new(Float64Array::from(vec![*f; num_rows])) as ArrayRef,
        Literal::Bool(b) => Arc::new(BooleanArray::from(vec![*b; num_rows])) as ArrayRef,
        Literal::Date(s) => {
            let days = crate::loader::parse_date32_literal(s)?;
            Arc::new(Date32Array::from(vec![days; num_rows])) as ArrayRef
        }
        Literal::DateTime(s) => {
            let ms = crate::loader::parse_date64_literal(s)?;
            Arc::new(Date64Array::from(vec![ms; num_rows])) as ArrayRef
        }
        Literal::List(items) => literal_list_to_array(items, num_rows)?,
    })
}

fn evaluate_contains_filter(left: &ArrayRef, right: &ArrayRef) -> Result<BooleanArray> {
    let DataType::List(field) = left.data_type() else {
        return Err(OmniError::manifest(
            "contains requires a list property on the left".to_string(),
        ));
    };
    let right = if right.data_type() != field.data_type() {
        arrow_cast::cast::cast(right, field.data_type())
            .map_err(|e| OmniError::Lance(e.to_string()))?
    } else {
        Arc::clone(right)
    };
    let list = left
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| OmniError::manifest("contains requires an Arrow ListArray"))?;

    let mut values = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) || right.is_null(row) {
            values.push(Some(false));
            continue;
        }
        let items = list.value(row);
        let mut found = false;
        for idx in 0..items.len() {
            if array_value_eq(items.as_ref(), idx, right.as_ref(), row)? {
                found = true;
                break;
            }
        }
        values.push(Some(found));
    }
    Ok(BooleanArray::from(values))
}

fn array_value_eq(
    left: &dyn Array,
    left_index: usize,
    right: &dyn Array,
    right_index: usize,
) -> Result<bool> {
    if left.is_null(left_index) || right.is_null(right_index) {
        return Ok(false);
    }
    let left_value =
        array_value_to_string(left, left_index).map_err(|e| OmniError::Lance(e.to_string()))?;
    let right_value =
        array_value_to_string(right, right_index).map_err(|e| OmniError::Lance(e.to_string()))?;
    Ok(left_value == right_value)
}

fn literal_list_to_array(items: &[Literal], num_rows: usize) -> Result<ArrayRef> {
    if items.is_empty() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for _ in 0..num_rows {
            builder.append(true);
        }
        return Ok(Arc::new(builder.finish()));
    }

    let scalar_type = list_scalar_type(items)?;
    match scalar_type {
        ScalarType::String => {
            let mut builder =
                ListBuilder::with_capacity(StringBuilder::new(), num_rows).with_field(Arc::new(
                    Field::new("item", DataType::Utf8, true),
                ));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::String(value) => builder.values().append_value(value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::Bool => {
            let mut builder = ListBuilder::with_capacity(BooleanBuilder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Boolean, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Bool(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::I32 => {
            let mut builder = ListBuilder::with_capacity(Int32Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Int32, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as i32),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::I64 | ScalarType::U32 | ScalarType::U64 => {
            let mut builder = ListBuilder::with_capacity(Int64Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Int64, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::F32 | ScalarType::F64 => {
            let mut builder = ListBuilder::with_capacity(Float64Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Float64, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f64),
                        Literal::Float(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::Date => {
            let mut builder = ListBuilder::with_capacity(Date32Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Date32, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Date(value) => {
                            builder
                                .values()
                                .append_value(crate::loader::parse_date32_literal(value)?)
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::DateTime => {
            let mut builder = ListBuilder::with_capacity(Date64Builder::new(), num_rows)
                .with_field(Arc::new(Field::new("item", DataType::Date64, true)));
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::DateTime(value) => {
                            builder
                                .values()
                                .append_value(crate::loader::parse_date64_literal(value)?)
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        ScalarType::Vector(_) | ScalarType::Blob => Err(OmniError::manifest(
            "unsupported list literal element type".to_string(),
        )),
    }
}

fn list_scalar_type(items: &[Literal]) -> Result<ScalarType> {
    let first = items.first().ok_or_else(|| OmniError::manifest("empty list literal"))?;
    let expected = literal_scalar_type(first)?;
    for item in items.iter().skip(1) {
        let item_type = literal_scalar_type(item)?;
        if item_type != expected {
            return Err(OmniError::manifest(
                "list literal elements must share a compatible scalar type".to_string(),
            ));
        }
    }
    Ok(expected)
}

fn literal_scalar_type(lit: &Literal) -> Result<ScalarType> {
    match lit {
        Literal::String(_) => Ok(ScalarType::String),
        Literal::Integer(_) => Ok(ScalarType::I64),
        Literal::Float(_) => Ok(ScalarType::F64),
        Literal::Bool(_) => Ok(ScalarType::Bool),
        Literal::Date(_) => Ok(ScalarType::Date),
        Literal::DateTime(_) => Ok(ScalarType::DateTime),
        Literal::List(_) => Err(OmniError::manifest(
            "nested list literals are not supported".to_string(),
        )),
    }
}

/// Project return expressions into a result batch.
fn project_return(
    bindings: &HashMap<String, RecordBatch>,
    projections: &[IRProjection],
    params: &ParamMap,
) -> Result<RecordBatch> {
    if projections.is_empty() {
        return Err(OmniError::manifest(
            "query has no return projections".to_string(),
        ));
    }

    let mut fields = Vec::with_capacity(projections.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projections.len());

    for proj in projections {
        let (name, col) = evaluate_projection(bindings, &proj.expr, params)?;
        let field_name = proj.alias.as_deref().unwrap_or(&name);
        fields.push(Field::new(
            field_name,
            col.data_type().clone(),
            col.null_count() > 0,
        ));
        columns.push(col);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
}

/// Evaluate a single projection expression.
fn evaluate_projection(
    bindings: &HashMap<String, RecordBatch>,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<(String, ArrayRef)> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let batch = bindings.get(variable).ok_or_else(|| {
                OmniError::manifest(format!(
                    "projection references unbound variable '{}'",
                    variable
                ))
            })?;
            let col = batch.column_by_name(property).ok_or_else(|| {
                OmniError::manifest(format!(
                    "column '{}' not found in binding '{}'",
                    property, variable
                ))
            })?;
            Ok((format!("{}.{}", variable, property), col.clone()))
        }
        IRExpr::Literal(lit) => {
            // Get row count from first binding
            let num_rows = bindings.values().next().map(|b| b.num_rows()).unwrap_or(0);
            let arr = literal_to_array(lit, num_rows)?;
            Ok(("literal".to_string(), arr))
        }
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name)))?;
            let num_rows = bindings.values().next().map(|b| b.num_rows()).unwrap_or(0);
            let arr = literal_to_array(lit, num_rows)?;
            Ok((name.clone(), arr))
        }
        _ => Err(OmniError::manifest(format!(
            "unsupported projection expression: {:?}",
            expr
        ))),
    }
}

/// Apply ordering to a batch.
fn apply_ordering(
    batch: RecordBatch,
    orderings: &[IROrdering],
    bindings: &HashMap<String, RecordBatch>,
    _params: &ParamMap,
) -> Result<RecordBatch> {
    use arrow_ord::sort::{SortColumn, lexsort_to_indices};

    let mut sort_columns = Vec::with_capacity(orderings.len());

    for ordering in orderings {
        let col = match &ordering.expr {
            IRExpr::PropAccess { variable, property } => {
                let binding = bindings.get(variable).ok_or_else(|| {
                    OmniError::manifest(format!(
                        "ordering references unbound variable '{}'",
                        variable
                    ))
                })?;
                binding
                    .column_by_name(property)
                    .ok_or_else(|| {
                        OmniError::manifest(format!("column '{}' not found for ordering", property))
                    })?
                    .clone()
            }
            IRExpr::AliasRef(alias) => {
                // Look up in the projected batch by column name
                batch
                    .column_by_name(alias)
                    .ok_or_else(|| {
                        OmniError::manifest(format!("alias '{}' not found for ordering", alias))
                    })?
                    .clone()
            }
            _ => {
                return Err(OmniError::manifest(
                    "unsupported ordering expression".to_string(),
                ));
            }
        };

        sort_columns.push(SortColumn {
            values: col,
            options: Some(arrow_schema::SortOptions {
                descending: ordering.descending,
                nulls_first: !ordering.descending,
            }),
        });
    }

    let indices =
        lexsort_to_indices(&sort_columns, None).map_err(|e| OmniError::Lance(e.to_string()))?;

    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_select::take::take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    RecordBatch::try_new(batch.schema(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}

// ─── Mutation helpers ────────────────────────────────────────────────────────

/// Resolve an IRExpr to a concrete Literal value at runtime.
fn resolve_expr_value(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    match expr {
        IRExpr::Literal(lit) => Ok(lit.clone()),
        IRExpr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name))),
        other => Err(OmniError::manifest(format!(
            "unsupported expression in mutation: {:?}",
            other
        ))),
    }
}

/// Create a single-element or N-element array from a Literal, matching the target DataType.
fn literal_to_typed_array(
    lit: &Literal,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    Ok(match (lit, data_type) {
        (Literal::String(s), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef
        }
        (Literal::Integer(n), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*n as i32; num_rows]))
        }
        (Literal::Integer(n), DataType::Int64) => Arc::new(Int64Array::from(vec![*n; num_rows])),
        (Literal::Integer(n), DataType::UInt32) => {
            Arc::new(UInt32Array::from(vec![*n as u32; num_rows]))
        }
        (Literal::Integer(n), DataType::UInt64) => {
            Arc::new(UInt64Array::from(vec![*n as u64; num_rows]))
        }
        (Literal::Float(f), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![*f as f32; num_rows]))
        }
        (Literal::Float(f), DataType::Float64) => Arc::new(Float64Array::from(vec![*f; num_rows])),
        (Literal::Bool(b), DataType::Boolean) => Arc::new(BooleanArray::from(vec![*b; num_rows])),
        (Literal::Date(s), DataType::Date32) => {
            let days = crate::loader::parse_date32_literal(s)?;
            Arc::new(Date32Array::from(vec![days; num_rows]))
        }
        (Literal::DateTime(s), DataType::Date64) => Arc::new(Date64Array::from(vec![
            crate::loader::parse_date64_literal(s)?;
            num_rows
        ])),
        (Literal::List(items), DataType::List(field)) => {
            typed_list_literal_to_array(items, field.data_type(), num_rows)?
        }
        (Literal::List(items), DataType::FixedSizeList(field, dim))
            if field.data_type() == &DataType::Float32 =>
        {
            if items.len() != *dim as usize {
                return Err(OmniError::manifest(format!(
                    "vector property expects {} dimensions, got {}",
                    dim,
                    items.len()
                )));
            }
            let mut builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(num_rows * (*dim as usize)),
                *dim,
                num_rows,
            )
            .with_field(field.clone());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f32),
                        Literal::Float(value) => builder.values().append_value(*value as f32),
                        _ => {
                            return Err(OmniError::manifest(
                                "vector elements must be numeric".to_string(),
                            ));
                        }
                    }
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        _ => {
            return Err(OmniError::manifest(format!(
                "cannot convert {:?} to {:?}",
                lit, data_type
            )));
        }
    })
}

fn typed_list_literal_to_array(
    items: &[Literal],
    item_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match item_type {
        DataType::Utf8 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::String(value) => builder.values().append_value(value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Bool(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = ListBuilder::new(Int32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => {
                            let value = i32::try_from(*value).map_err(|_| {
                                OmniError::manifest(format!("list value {} exceeds Int32 range", value))
                            })?;
                            builder.values().append_value(value);
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt32 => {
            let mut builder = ListBuilder::new(UInt32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => {
                            let value = u32::try_from(*value).map_err(|_| {
                                OmniError::manifest(format!("list value {} exceeds UInt32 range", value))
                            })?;
                            builder.values().append_value(value);
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let mut builder = ListBuilder::new(UInt64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => {
                            let value = u64::try_from(*value).map_err(|_| {
                                OmniError::manifest(format!("list value {} exceeds UInt64 range", value))
                            })?;
                            builder.values().append_value(value);
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = ListBuilder::new(Float32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f32),
                        Literal::Float(value) => builder.values().append_value(*value as f32),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f64),
                        Literal::Float(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = ListBuilder::new(Date32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Date(value) => {
                            builder
                                .values()
                                .append_value(crate::loader::parse_date32_literal(value)?)
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date64 => {
            let mut builder = ListBuilder::new(Date64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::DateTime(value) => {
                            builder
                                .values()
                                .append_value(crate::loader::parse_date64_literal(value)?)
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(OmniError::manifest(format!(
            "cannot convert list literal to {:?}",
            other
        ))),
    }
}

/// Build a single-element blob array from a URI or base64 value string.
fn build_blob_array_from_value(value: &str) -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(1);
    crate::loader::append_blob_value(&mut builder, value)?;
    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Build a null blob array with one element.
fn build_null_blob_array() -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(1);
    builder
        .push_null()
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Build a single-row RecordBatch from resolved assignments.
fn build_insert_batch(
    schema: &SchemaRef,
    id: &str,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        if field.name() == "id" {
            columns.push(Arc::new(StringArray::from(vec![id])));
        } else if blob_properties.contains(field.name()) {
            if let Some(Literal::String(uri)) = assignments.get(field.name()) {
                columns.push(build_blob_array_from_value(uri)?);
            } else if field.is_nullable() {
                columns.push(build_null_blob_array()?);
            } else {
                return Err(OmniError::manifest(format!(
                    "missing required blob property '{}'",
                    field.name()
                )));
            }
        } else if field.name() == "src" {
            let lit = assignments.get("from").ok_or_else(|| {
                OmniError::manifest("missing required edge endpoint 'from'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.name() == "dst" {
            let lit = assignments.get("to").ok_or_else(|| {
                OmniError::manifest("missing required edge endpoint 'to'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.is_nullable() {
            columns.push(arrow_array::new_null_array(field.data_type(), 1));
        } else {
            return Err(OmniError::manifest(format!(
                "missing required property '{}'",
                field.name()
            )));
        }
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}

async fn validate_edge_insert_endpoints(
    db: &Omnigraph,
    edge_name: &str,
    assignments: &HashMap<String, Literal>,
) -> Result<()> {
    let edge_type = db
        .catalog()
        .edge_types
        .get(edge_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_name)))?;
    let from = match assignments.get("from") {
        Some(Literal::String(value)) => value.as_str(),
        Some(other) => {
            return Err(OmniError::manifest(format!(
                "edge {} from endpoint must be a string id, got {}",
                edge_name,
                literal_to_sql(other)
            )));
        }
        None => {
            return Err(OmniError::manifest(format!(
                "edge {} missing 'from' endpoint",
                edge_name
            )));
        }
    };
    let to = match assignments.get("to") {
        Some(Literal::String(value)) => value.as_str(),
        Some(other) => {
            return Err(OmniError::manifest(format!(
                "edge {} to endpoint must be a string id, got {}",
                edge_name,
                literal_to_sql(other)
            )));
        }
        None => {
            return Err(OmniError::manifest(format!(
                "edge {} missing 'to' endpoint",
                edge_name
            )));
        }
    };

    ensure_node_id_exists(db, &edge_type.from_type, from, "src").await?;
    ensure_node_id_exists(db, &edge_type.to_type, to, "dst").await?;
    Ok(())
}

async fn ensure_node_id_exists(
    db: &Omnigraph,
    node_type: &str,
    id: &str,
    label: &str,
) -> Result<()> {
    let snapshot = db.snapshot();
    let table_key = format!("node:{}", node_type);
    let ds = snapshot.open(&table_key).await?;
    let filter = format!("id = '{}'", id.replace('\'', "''"));
    let exists = ds
        .count_rows(Some(filter))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        > 0;
    if exists {
        Ok(())
    } else {
        Err(OmniError::manifest(format!(
            "{} '{}' not found in {}",
            label, id, node_type
        )))
    }
}

/// Convert an IRMutationPredicate to a Lance SQL filter string.
fn predicate_to_sql(
    predicate: &IRMutationPredicate,
    params: &ParamMap,
    is_edge: bool,
) -> Result<String> {
    let column = if is_edge {
        match predicate.property.as_str() {
            "from" => "src".to_string(),
            "to" => "dst".to_string(),
            other => other.to_string(),
        }
    } else {
        predicate.property.clone()
    };

    let value = resolve_expr_value(&predicate.value, params)?;
    let value_sql = literal_to_sql(&value);

    let op = match predicate.op {
        CompOp::Eq => "=",
        CompOp::Ne => "!=",
        CompOp::Gt => ">",
        CompOp::Lt => "<",
        CompOp::Ge => ">=",
        CompOp::Le => "<=",
        CompOp::Contains => {
            return Err(OmniError::manifest(
                "contains predicate not supported in mutations".to_string(),
            ));
        }
    };

    Ok(format!("{} {} {}", column, op, value_sql))
}

/// Replace specific columns in a RecordBatch with new literal values.
/// Apply scalar assignments to a batch. Blob columns are excluded from the
/// scan result and handled separately via a second merge_insert in execute_update.
fn apply_assignments(
    batch: &RecordBatch,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields().iter() {
        if blob_properties.contains(field.name()) {
            // Blob columns aren't in the scan result — skip
            continue;
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(
                lit,
                field.data_type(),
                batch.num_rows(),
            )?);
        } else {
            let col = batch.column_by_name(field.name()).ok_or_else(|| {
                OmniError::Lance(format!(
                    "column '{}' not found in scan result",
                    field.name()
                ))
            })?;
            columns.push(col.clone());
        }
    }

    // Build schema without blob columns
    let non_blob_fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| !blob_properties.contains(f.name()))
        .map(|f| f.as_ref().clone())
        .collect();

    RecordBatch::try_new(Arc::new(Schema::new(non_blob_fields)), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

// ─── Mutation execution ──────────────────────────────────────────────────────

impl Omnigraph {
    pub async fn mutate(
        &mut self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let requested = Self::normalize_branch_name(branch)?;
        let resolved_params = enrich_mutation_params(params)?;
        let operation = format!(
            "mutation:{}:branch={}",
            query_name,
            requested.as_deref().unwrap_or("main")
        );

        if requested.as_deref().is_some_and(is_internal_run_branch) {
            return self
                .execute_named_mutation_on_branch(
                    requested.as_deref(),
                    query_source,
                    query_name,
                    &resolved_params,
                )
                .await;
        }

        let target_branch = requested.clone().unwrap_or_else(|| "main".to_string());
        let target_head_before = self.latest_branch_snapshot_id(&target_branch).await?;
        let run = self
            .begin_run(&target_branch, Some(operation.as_str()))
            .await?;

        let staged_result = match self
            .execute_named_mutation_on_branch(
                Some(run.run_branch.as_str()),
                query_source,
                query_name,
                &resolved_params,
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                let _ = self.fail_run(&run.run_id).await;
                return Err(err);
            }
        };

        let target_head_now = self.latest_branch_snapshot_id(&target_branch).await?;
        if target_head_now.as_str() != target_head_before.as_str() {
            let _ = self.fail_run(&run.run_id).await;
            return Err(OmniError::manifest_conflict(format!(
                "target branch '{}' advanced during transactional mutation; retry",
                target_branch
            )));
        }

        if let Err(err) = self.publish_run(&run.run_id).await {
            let _ = self.fail_run(&run.run_id).await;
            return Err(err);
        }

        Ok(staged_result)
    }

    async fn execute_named_mutation_on_branch(
        &mut self,
        branch: Option<&str>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let requested = match branch {
            Some(branch) => Self::normalize_branch_name(branch)?,
            None => None,
        };
        let current = self.active_branch().map(str::to_string);
        if requested == current {
            return self
                .execute_named_mutation(query_source, query_name, params)
                .await;
        }

        let previous = self
            .swap_coordinator_for_branch(requested.as_deref())
            .await?;
        let result = self
            .execute_named_mutation(query_source, query_name, params)
            .await;
        self.restore_coordinator(previous);
        result
    }

    async fn execute_named_mutation(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;

        let checked = typecheck_query_decl(self.catalog(), &query_decl)?;
        match checked {
            CheckedQuery::Mutation(_) => {}
            CheckedQuery::Read(_) => {
                return Err(OmniError::manifest(
                    "mutation execution called on a read query; use query instead".to_string(),
                ));
            }
        }

        let ir = lower_mutation_query(&query_decl)?;

        match &ir.op {
            MutationOpIR::Insert {
                type_name,
                assignments,
            } => self.execute_insert(type_name, assignments, params).await,
            MutationOpIR::Update {
                type_name,
                assignments,
                predicate,
            } => {
                self.execute_update(type_name, assignments, predicate, params)
                    .await
            }
            MutationOpIR::Delete {
                type_name,
                predicate,
            } => self.execute_delete(type_name, predicate, params).await,
        }
    }

    pub async fn branch_merge(&mut self, source: &str, target: &str) -> Result<MergeOutcome> {
        self.branch_merge_impl(source, target, false).await
    }

    pub(crate) async fn branch_merge_internal(
        &mut self,
        source: &str,
        target: &str,
    ) -> Result<MergeOutcome> {
        self.branch_merge_impl(source, target, true).await
    }

    async fn branch_merge_impl(
        &mut self,
        source: &str,
        target: &str,
        allow_internal_refs: bool,
    ) -> Result<MergeOutcome> {
        if !allow_internal_refs {
            if is_internal_run_branch(source) || is_internal_run_branch(target) {
                return Err(OmniError::manifest(format!(
                    "branch_merge does not allow internal run refs ('{}' -> '{}')",
                    source, target
                )));
            }
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

        let manifest_version = if updates.is_empty() {
            self.version()
        } else {
            self.commit_manifest_updates(&updates).await?
        };
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

    async fn execute_insert(
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }

        let is_node = self.catalog().node_types.contains_key(type_name);
        let is_edge = self.catalog().edge_types.contains_key(type_name);

        if is_node {
            let node_type = &self.catalog().node_types[type_name];
            let schema = node_type.arrow_schema.clone();
            let blob_props = node_type.blob_properties.clone();
            let id = if let Some(key_prop) = node_type.key_property() {
                match resolved.get(key_prop) {
                    Some(Literal::String(s)) => s.clone(),
                    Some(other) => literal_to_sql(other).trim_matches('\'').to_string(),
                    None => {
                        return Err(OmniError::manifest(format!(
                            "insert missing @key property '{}'",
                            key_prop
                        )));
                    }
                }
            } else {
                ulid::Ulid::new().to_string()
            };

            let batch = build_insert_batch(&schema, &id, &resolved, &blob_props)?;
            crate::loader::validate_value_constraints(&batch, node_type)?;
            let has_key = node_type.key_property().is_some();
            let (new_version, row_count, table_branch) = if has_key {
                self.upsert_batch(type_name, true, schema, batch).await?
            } else {
                self.append_batch(type_name, true, schema, batch).await?
            };

            let table_key = format!("node:{}", type_name);
            self.commit_updates(&[crate::db::SubTableUpdate {
                table_key,
                table_version: new_version,
                table_branch,
                row_count,
            }])
            .await?;

            Ok(MutationResult {
                affected_nodes: 1,
                affected_edges: 0,
            })
        } else if is_edge {
            let edge_type = &self.catalog().edge_types[type_name];
            let schema = edge_type.arrow_schema.clone();
            let blob_props = edge_type.blob_properties.clone();
            let id = ulid::Ulid::new().to_string();

            let batch = build_insert_batch(&schema, &id, &resolved, &blob_props)?;
            validate_edge_insert_endpoints(self, type_name, &resolved).await?;
            let (new_version, row_count, table_branch) =
                self.append_batch(type_name, false, schema, batch).await?;

            let table_key = format!("edge:{}", type_name);
            self.commit_updates(&[crate::db::SubTableUpdate {
                table_key,
                table_version: new_version,
                table_branch,
                row_count,
            }])
            .await?;

            self.invalidate_graph_index().await;

            Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 1,
            })
        } else {
            Err(OmniError::manifest(format!("unknown type '{}'", type_name)))
        }
    }

    /// Append a batch to a sub-table, returning (new_version, row_count).
    async fn append_batch(
        &self,
        type_name: &str,
        is_node: bool,
        _schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(u64, u64, Option<String>)> {
        let table_key = if is_node {
            format!("node:{}", type_name)
        } else {
            format!("edge:{}", type_name)
        };
        let (mut ds, _full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let state = self.table_store().append_batch(&mut ds, batch).await?;
        Ok((state.version, state.row_count, table_branch))
    }

    /// Upsert a batch into a sub-table using merge_insert keyed by "id".
    /// Used for @key node types to enforce uniqueness.
    async fn upsert_batch(
        &self,
        type_name: &str,
        is_node: bool,
        _schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(u64, u64, Option<String>)> {
        let table_key = if is_node {
            format!("node:{}", type_name)
        } else {
            format!("edge:{}", type_name)
        };
        let (ds, _full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let state = self
            .table_store()
            .merge_insert_batch(
                ds,
                batch,
                vec!["id".to_string()],
                lance::dataset::WhenMatched::UpdateAll,
                lance::dataset::WhenNotMatched::InsertAll,
            )
            .await?;
        Ok((state.version, state.row_count, table_branch))
    }

    async fn execute_update(
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        predicate: &IRMutationPredicate,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        // Defense in depth: ensure this is a node type
        if !self.catalog().node_types.contains_key(type_name) {
            return Err(OmniError::manifest(format!(
                "update is only supported for node types, not '{}'",
                type_name
            )));
        }

        // Reject updates to @key properties — identity is immutable
        if let Some(key_prop) = self.catalog().node_types[type_name].key_property() {
            if assignments.iter().any(|a| a.property == key_prop) {
                return Err(OmniError::manifest(format!(
                    "cannot update @key property '{}' — delete and re-insert instead",
                    key_prop
                )));
            }
        }

        let pred_sql = predicate_to_sql(predicate, params, false)?;
        let schema = self.catalog().node_types[type_name].arrow_schema.clone();
        let blob_props = self.catalog().node_types[type_name].blob_properties.clone();

        let table_key = format!("node:{}", type_name);
        let (ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let initial_version = ds.version().version;

        let non_blob_cols: Vec<&str> = schema
            .fields()
            .iter()
            .filter(|f| !blob_props.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        let batches = self
            .table_store()
            .scan(
                &ds,
                (!blob_props.is_empty()).then_some(non_blob_cols.as_slice()),
                Some(&pred_sql),
                None,
            )
            .await?;

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        let matched = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            let s = batches[0].schema();
            arrow_select::concat::concat_batches(&s, &batches)
                .map_err(|e| OmniError::Lance(e.to_string()))?
        };

        let affected_count = matched.num_rows();

        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }
        let updated = apply_assignments(&matched, &resolved, &blob_props)?;
        crate::loader::validate_value_constraints(&updated, &self.catalog().node_types[type_name])?;

        // Re-open for merge_insert (scan consumed the dataset;
        // version guard was already applied by open_for_mutation above)
        let ds = self
            .reopen_for_mutation(
                &table_key,
                &full_path,
                table_branch.as_deref(),
                initial_version,
            )
            .await?;
        let update_state = self
            .table_store()
            .merge_insert_batch(
                ds,
                updated,
                vec!["id".to_string()],
                lance::dataset::WhenMatched::UpdateAll,
                lance::dataset::WhenNotMatched::DoNothing,
            )
            .await?;

        let mut final_version = update_state.version;
        let mut final_row_count = update_state.row_count;

        // Phase 2: If there are blob assignments, apply them separately
        let blob_assignments: HashMap<&str, &Literal> = resolved
            .iter()
            .filter(|(k, _)| blob_props.contains(k.as_str()))
            .map(|(k, v)| (k.as_str(), v))
            .collect();

        if !blob_assignments.is_empty() {
            // Extract matched IDs from the scan result
            let id_col = matched.column_by_name("id").ok_or_else(|| {
                OmniError::manifest("matched batch missing 'id' column".to_string())
            })?;
            let ids = id_col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::manifest("id column is not Utf8".to_string()))?;

            // Build batch: id + blob columns
            let mut blob_fields = vec![Field::new("id", DataType::Utf8, false)];
            let mut blob_columns: Vec<ArrayRef> = vec![Arc::new(ids.clone())];

            for blob_prop in &blob_props {
                if let Some(Literal::String(uri)) = blob_assignments.get(blob_prop.as_str()) {
                    let mut builder = BlobArrayBuilder::new(ids.len());
                    for _ in 0..ids.len() {
                        crate::loader::append_blob_value(&mut builder, uri)?;
                    }
                    let blob_field = lance::blob::blob_field(blob_prop, true);
                    blob_fields.push(blob_field);
                    blob_columns.push(
                        builder
                            .finish()
                            .map_err(|e| OmniError::Lance(e.to_string()))?,
                    );
                }
            }

            let blob_schema = Arc::new(Schema::new(blob_fields));
            let blob_batch = RecordBatch::try_new(blob_schema.clone(), blob_columns)
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            // Re-open for blob merge_insert (version guard from open_for_mutation)
            let ds = self
                .reopen_for_mutation(
                    &table_key,
                    &full_path,
                    table_branch.as_deref(),
                    final_version,
                )
                .await?;
            let blob_state = self
                .table_store()
                .merge_insert_batch(
                    ds,
                    blob_batch,
                    vec!["id".to_string()],
                    lance::dataset::WhenMatched::UpdateAll,
                    lance::dataset::WhenNotMatched::DoNothing,
                )
                .await?;

            final_version = blob_state.version;
            final_row_count = blob_state.row_count;
        }

        self.commit_updates(&[crate::db::SubTableUpdate {
            table_key,
            table_version: final_version,
            table_branch,
            row_count: final_row_count,
        }])
        .await?;

        Ok(MutationResult {
            affected_nodes: affected_count,
            affected_edges: 0,
        })
    }

    async fn execute_delete(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let is_node = self.catalog().node_types.contains_key(type_name);
        if is_node {
            self.execute_delete_node(type_name, predicate, params).await
        } else {
            self.execute_delete_edge(type_name, predicate, params).await
        }
    }

    async fn execute_delete_node(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, false)?;

        let table_key = format!("node:{}", type_name);
        let (ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let initial_version = ds.version().version;

        // Scan matching IDs for cascade
        let batches = self
            .table_store()
            .scan(&ds, Some(&["id"]), Some(&pred_sql), None)
            .await?;

        let deleted_ids: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..ids.len())
                    .map(|i| ids.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();

        if deleted_ids.is_empty() {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        let affected_nodes = deleted_ids.len();

        // Delete nodes (re-open needed because the scan consumed the dataset;
        // version guard was already applied by open_for_mutation above)
        let mut ds = self
            .reopen_for_mutation(
                &table_key,
                &full_path,
                table_branch.as_deref(),
                initial_version,
            )
            .await?;
        let delete_state = self.table_store().delete_where(&mut ds, &pred_sql).await?;

        let mut updates = vec![crate::db::SubTableUpdate {
            table_key,
            table_version: delete_state.version,
            table_branch: table_branch.clone(),
            row_count: delete_state.row_count,
        }];

        let mut affected_edges = 0usize;
        let escaped: Vec<String> = deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_list = escaped.join(", ");

        let edge_info: Vec<(String, String, String)> = self
            .catalog()
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), et.from_type.clone(), et.to_type.clone()))
            .collect();

        for (edge_name, from_type, to_type) in &edge_info {
            let mut cascade_filters = Vec::new();
            if from_type == type_name {
                cascade_filters.push(format!("src IN ({})", id_list));
            }
            if to_type == type_name {
                cascade_filters.push(format!("dst IN ({})", id_list));
            }
            if cascade_filters.is_empty() {
                continue;
            }

            let edge_table_key = format!("edge:{}", edge_name);
            let cascade_filter = cascade_filters.join(" OR ");
            let (mut edge_ds, _edge_full_path, edge_table_branch) =
                self.open_for_mutation(&edge_table_key).await?;

            let edge_delete = self
                .table_store()
                .delete_where(&mut edge_ds, &cascade_filter)
                .await?;

            affected_edges += edge_delete.deleted_rows;

            if edge_delete.deleted_rows > 0 {
                updates.push(crate::db::SubTableUpdate {
                    table_key: edge_table_key,
                    table_version: edge_delete.version,
                    table_branch: edge_table_branch,
                    row_count: edge_delete.row_count,
                });
            }
        }

        self.commit_updates(&updates).await?;

        if affected_edges > 0 {
            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes,
            affected_edges,
        })
    }

    async fn execute_delete_edge(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, true)?;

        let table_key = format!("edge:{}", type_name);
        let (mut ds, _full_path, table_branch) = self.open_for_mutation(&table_key).await?;

        let delete_state = self.table_store().delete_where(&mut ds, &pred_sql).await?;
        let affected = delete_state.deleted_rows;

        if affected > 0 {
            self.commit_updates(&[crate::db::SubTableUpdate {
                table_key,
                table_version: delete_state.version,
                table_branch,
                row_count: delete_state.row_count,
            }])
            .await?;

            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes: 0,
            affected_edges: affected,
        })
    }
}

fn enrich_mutation_params(params: &ParamMap) -> Result<ParamMap> {
    let mut resolved = params.clone();
    if !resolved.contains_key(NOW_PARAM_NAME) {
        let now = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|e| OmniError::manifest(format!("failed to format now(): {}", e)))?;
        resolved.insert(NOW_PARAM_NAME.to_string(), Literal::DateTime(now));
    }
    Ok(resolved)
}
