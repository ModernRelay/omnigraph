use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, RecordBatchIterator, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::BlobArrayBuilder;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{
    IRExpr, IRFilter, IROrdering, IRProjection, IROp, IRAssignment, IRMutationPredicate,
    MutationOpIR, ParamMap, QueryIR,
};
use omnigraph_compiler::lower_query;
use omnigraph_compiler::lower_mutation_query;
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_compiler::query::typecheck::{typecheck_query, typecheck_query_decl, CheckedQuery};
use omnigraph_compiler::result::{MutationResult, QueryResult};
use omnigraph_compiler::types::Direction;

use crate::db::Omnigraph;
use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::graph_index::GraphIndex;

impl Omnigraph {
    /// Run a named query from a .gq query source string.
    pub async fn run_query(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<QueryResult> {
        let snapshot = self.snapshot();

        // Parse → typecheck → lower
        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::Manifest(e.to_string()))?;
        let type_ctx = typecheck_query(self.catalog(), &query_decl)?;
        let ir = lower_query(self.catalog(), &query_decl, &type_ctx)?;

        // Build graph index if the query needs traversal
        let needs_graph = ir.pipeline.iter().any(|op| {
            matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. })
        });
        let graph_index = if needs_graph {
            Some(self.graph_index().await?)
        } else {
            None
        };

        execute_query(&ir, params, &snapshot, graph_index.as_deref(), self.catalog()).await
    }
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
fn extract_search_mode(ir: &QueryIR, params: &ParamMap) -> Result<SearchMode> {
    if ir.order_by.is_empty() {
        return Ok(SearchMode::default());
    }
    let ordering = &ir.order_by[0];
    match &ordering.expr {
        IRExpr::Nearest { variable, property, query } => {
            let vec = resolve_to_f32_vec(query, params)?;
            let k = ir.limit.ok_or_else(|| {
                OmniError::Manifest("nearest() ordering requires a limit clause".to_string())
            })? as usize;
            Ok(SearchMode {
                nearest: Some((variable.clone(), property.clone(), vec, k)),
                ..Default::default()
            })
        }
        IRExpr::Bm25 { field, query } => {
            let var = match field.as_ref() {
                IRExpr::PropAccess { variable, .. } => variable.clone(),
                _ => return Err(OmniError::Manifest("bm25 field must be a property access".to_string())),
            };
            let prop = extract_property(field).ok_or_else(|| {
                OmniError::Manifest("bm25 field must be a property access".to_string())
            })?;
            let text = resolve_to_string(query, params).ok_or_else(|| {
                OmniError::Manifest("bm25 query must resolve to a string".to_string())
            })?;
            Ok(SearchMode {
                bm25: Some((var, prop, text)),
                ..Default::default()
            })
        }
        IRExpr::Rrf { primary, secondary, k } => {
            let limit = ir.limit.ok_or_else(|| {
                OmniError::Manifest("rrf() ordering requires a limit clause".to_string())
            })? as usize;
            let k_val = k.as_ref()
                .and_then(|e| resolve_to_int(e, params))
                .unwrap_or(60) as u32;

            let primary_mode = extract_sub_search_mode(primary, params, ir.limit)?;
            let secondary_mode = extract_sub_search_mode(secondary, params, ir.limit)?;

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
fn extract_sub_search_mode(expr: &IRExpr, params: &ParamMap, limit: Option<u64>) -> Result<SearchMode> {
    match expr {
        IRExpr::Nearest { variable, property, query } => {
            let vec = resolve_to_f32_vec(query, params)?;
            let k = limit.unwrap_or(100) as usize;
            Ok(SearchMode {
                nearest: Some((variable.clone(), property.clone(), vec, k)),
                ..Default::default()
            })
        }
        IRExpr::Bm25 { field, query } => {
            let var = match field.as_ref() {
                IRExpr::PropAccess { variable, .. } => variable.clone(),
                _ => return Err(OmniError::Manifest("bm25 field must be a property access".to_string())),
            };
            let prop = extract_property(field).ok_or_else(|| {
                OmniError::Manifest("bm25 field must be a property access".to_string())
            })?;
            let text = resolve_to_string(query, params).ok_or_else(|| {
                OmniError::Manifest("bm25 query must resolve to a string".to_string())
            })?;
            Ok(SearchMode {
                bm25: Some((var, prop, text)),
                ..Default::default()
            })
        }
        _ => Ok(SearchMode::default()),
    }
}

/// Resolve an expression to a Vec<f32> (for vector search).
fn resolve_to_f32_vec(expr: &IRExpr, params: &ParamMap) -> Result<Vec<f32>> {
    let lit = match expr {
        IRExpr::Literal(lit) => lit.clone(),
        IRExpr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            OmniError::Manifest(format!("parameter '{}' not provided", name))
        })?,
        _ => return Err(OmniError::Manifest("nearest query must be a literal or parameter".to_string())),
    };
    match lit {
        Literal::List(items) => items
            .iter()
            .map(|item| match item {
                Literal::Float(f) => Ok(*f as f32),
                Literal::Integer(n) => Ok(*n as f32),
                _ => Err(OmniError::Manifest("vector elements must be numeric".to_string())),
            })
            .collect(),
        _ => Err(OmniError::Manifest("nearest query must be a list of floats".to_string())),
    }
}

/// Execute a lowered QueryIR. Pure function — no state, no caches.
pub async fn execute_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let search_mode = extract_search_mode(ir, params)?;

    // RRF requires forked execution
    if let Some(ref rrf) = search_mode.rrf {
        return execute_rrf_query(ir, params, snapshot, graph_index, catalog, rrf).await;
    }

    let mut bindings: HashMap<String, RecordBatch> = HashMap::new();

    execute_pipeline(&ir.pipeline, params, snapshot, graph_index, catalog, &mut bindings, &search_mode).await?;

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
    execute_pipeline(&ir.pipeline, params, snapshot, graph_index, catalog, &mut primary_bindings, &rrf.primary).await?;

    // Execute secondary search
    let mut secondary_bindings: HashMap<String, RecordBatch> = HashMap::new();
    execute_pipeline(&ir.pipeline, params, snapshot, graph_index, catalog, &mut secondary_bindings, &rrf.secondary).await?;

    // For RRF, we need to find the main binding variable
    // (the one that both searches operate on)
    let primary_var = rrf.primary.nearest.as_ref().map(|(v, ..)| v.as_str())
        .or_else(|| rrf.primary.bm25.as_ref().map(|(v, ..)| v.as_str()))
        .ok_or_else(|| OmniError::Manifest("rrf primary must be nearest or bm25".to_string()))?;

    let primary_batch = primary_bindings.get(primary_var).ok_or_else(|| {
        OmniError::Manifest(format!("rrf primary variable '{}' not in bindings", primary_var))
    })?;
    let secondary_batch = secondary_bindings.get(primary_var).ok_or_else(|| {
        OmniError::Manifest(format!("rrf secondary variable '{}' not in bindings", primary_var))
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
            let p = primary_rank.get(id).map(|&r| 1.0 / (k + r as f64 + 1.0)).unwrap_or(0.0);
            let s = secondary_rank.get(id).map(|&r| 1.0 / (k + r as f64 + 1.0)).unwrap_or(0.0);
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
        id_to_batch_row.entry(id.clone()).or_insert((primary_batch, i));
    }
    for (i, id) in secondary_ids.iter().enumerate() {
        id_to_batch_row.entry(id.clone()).or_insert((secondary_batch, i));
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
    let col = batch.column_by_name("id").ok_or_else(|| {
        OmniError::Manifest("batch missing 'id' column for RRF".to_string())
    })?;
    let ids = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        OmniError::Manifest("'id' column is not Utf8".to_string())
    })?;
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

/// Execute a pipeline of IR operations, populating variable bindings.
fn execute_pipeline<'a>(
    pipeline: &'a [IROp],
    params: &'a ParamMap,
    snapshot: &'a Snapshot,
    graph_index: Option<&'a GraphIndex>,
    catalog: &'a Catalog,
    bindings: &'a mut HashMap<String, RecordBatch>,
    search_mode: &'a SearchMode,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>> {
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
                let batch =
                    execute_node_scan(type_name, variable, &all_filters, params, snapshot, catalog, search_mode).await?;
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
                    OmniError::Manifest("graph index required for traversal".to_string())
                })?;
                let batch = execute_expand(
                    bindings, gi, snapshot, catalog, src_var, dst_var, edge_type,
                    *direction, dst_type, *min_hops, *max_hops,
                )
                .await?;
                bindings.insert(dst_var.clone(), batch);
            }
            IROp::AntiJoin { outer_var, inner } => {
                let gi = graph_index;
                execute_anti_join(
                    bindings, inner, params, snapshot, gi, catalog, outer_var,
                )
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
        OmniError::Manifest(format!("expand references unbound variable '{}'", src_var))
    })?;

    let src_ids = src_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::Manifest("source batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::Manifest("source 'id' column is not Utf8".to_string()))?;

    // Determine which type index to use for source and destination
    let edge_def = catalog.edge_types.get(edge_type).ok_or_else(|| {
        OmniError::Manifest(format!("unknown edge type '{}'", edge_type))
    })?;

    let (src_type_name, dst_type_name) = match direction {
        Direction::Out => (&edge_def.from_type, &edge_def.to_type),
        Direction::In => (&edge_def.to_type, &edge_def.from_type),
    };

    let src_type_idx = graph_index.type_index(src_type_name).ok_or_else(|| {
        OmniError::Manifest(format!("no type index for '{}'", src_type_name))
    })?;
    let dst_type_idx = graph_index.type_index(dst_type_name).ok_or_else(|| {
        OmniError::Manifest(format!("no type index for '{}'", dst_type_name))
    })?;

    let adj = match direction {
        Direction::Out => graph_index.csr(edge_type),
        Direction::In => graph_index.csc(edge_type),
    }
    .ok_or_else(|| {
        OmniError::Manifest(format!("no adjacency index for edge '{}'", edge_type))
    })?;

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
                                result_dst_ids.push(dst_id.to_string());
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
    let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
        OmniError::Manifest(format!("unknown node type '{}'", type_name))
    })?;

    if ids.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    }

    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open(&table_key).await?;

    // Build filter: id IN ('a', 'b', 'c')
    let escaped: Vec<String> = ids.iter().map(|id| format!("'{}'", id.replace('\'', "''"))).collect();
    let filter_sql = format!("id IN ({})", escaped.join(", "));

    let mut scanner = ds.scan();
    let has_blobs = !node_type.blob_properties.is_empty();

    if has_blobs {
        let non_blob_cols: Vec<&str> = node_type
            .arrow_schema
            .fields()
            .iter()
            .filter(|f| !node_type.blob_properties.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        scanner
            .project(&non_blob_cols)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
    }

    scanner
        .filter(&filter_sql)
        .map_err(|e| OmniError::Lance(format!("hydrate filter: {}", e)))?;

    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
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
        OmniError::Manifest(format!("anti-join references unbound variable '{}'", outer_var))
    })?;

    // Fast path: bulk CSR existence check (O(N), zero Lance I/O)
    if let Some(result) = try_bulk_anti_join(outer_batch, inner_pipeline, graph_index, catalog, outer_var) {
        bindings.insert(outer_var.to_string(), result?);
        return Ok(());
    }

    // Slow path: per-row inner pipeline execution
    let outer_ids = outer_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::Manifest("outer batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::Manifest("outer 'id' column is not Utf8".to_string()))?;

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

    let mut scanner = ds.scan();

    // Blob columns must be excluded from scan when a filter is present
    // (Lance bug: BlobsDescriptions + filter triggers a projection assertion).
    // We exclude blob columns and add metadata post-scan via take_blobs_by_indices.
    let node_type = &catalog.node_types[type_name];
    let has_blobs = !node_type.blob_properties.is_empty();
    if has_blobs {
        let non_blob_cols: Vec<&str> = node_type
            .arrow_schema
            .fields()
            .iter()
            .filter(|f| !node_type.blob_properties.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        scanner
            .project(&non_blob_cols)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
    }

    if let Some(sql) = &filter_sql {
        scanner
            .filter(sql.as_str())
            .map_err(|e| OmniError::Lance(format!("filter '{}': {}", sql, e)))?;
    }

    // Apply FTS queries from hoisted search filters (search/fuzzy/match_text in match clause)
    for filter in filters {
        if is_search_filter(filter) {
            if let Some(fts_query) = build_fts_query(&filter.left, params) {
                scanner
                    .full_text_search(fts_query)
                    .map_err(|e| OmniError::Lance(format!("full_text_search filter: {}", e)))?;
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

    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let scan_result = if batches.is_empty() {
        RecordBatch::new_empty(batches.first().map(|b| b.schema()).unwrap_or_else(|| {
            // Build a non-blob schema for empty result
            let fields: Vec<_> = node_type.arrow_schema.fields().iter()
                .filter(|f| !node_type.blob_properties.contains(f.name()))
                .map(|f| f.as_ref().clone()).collect();
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
            let batch_field = batch_schema.field_with_name(field.name())
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
        IRExpr::Fuzzy { field, query, max_edits } => {
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
        Literal::Date(s) => format!("'{}'", s),
        Literal::DateTime(s) => format!("'{}'", s),
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
        OmniError::Manifest(format!("filter references unbound variable '{}'", var_name))
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
        CompOp::Contains => {
            return Err(OmniError::Manifest(
                "list contains not yet implemented".to_string(),
            ));
        }
    }
    .map_err(|e| OmniError::Lance(e.to_string()))?;

    Ok(result)
}

/// Evaluate an IR expression against a batch, producing an array.
fn evaluate_expr(
    batch: &RecordBatch,
    expr: &IRExpr,
    params: &ParamMap,
) -> Result<ArrayRef> {
    match expr {
        IRExpr::PropAccess { property, .. } => {
            batch
                .column_by_name(property)
                .cloned()
                .ok_or_else(|| {
                    OmniError::Manifest(format!("column '{}' not found in batch", property))
                })
        }
        IRExpr::Literal(lit) => literal_to_array(lit, batch.num_rows()),
        IRExpr::Param(name) => {
            let lit = params.get(name).ok_or_else(|| {
                OmniError::Manifest(format!("parameter '{}' not provided", name))
            })?;
            literal_to_array(lit, batch.num_rows())
        }
        _ => Err(OmniError::Manifest(format!(
            "unsupported expression in filter: {:?}",
            expr
        ))),
    }
}

/// Create a constant array from a literal value.
fn literal_to_array(lit: &Literal, num_rows: usize) -> Result<ArrayRef> {
    Ok(match lit {
        Literal::String(s) => {
            Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef
        }
        Literal::Integer(n) => {
            // Try to match the most common integer types
            Arc::new(Int64Array::from(vec![*n; num_rows])) as ArrayRef
        }
        Literal::Float(f) => {
            Arc::new(Float64Array::from(vec![*f; num_rows])) as ArrayRef
        }
        Literal::Bool(b) => {
            Arc::new(BooleanArray::from(vec![*b; num_rows])) as ArrayRef
        }
        _ => {
            return Err(OmniError::Manifest(format!(
                "unsupported literal type: {:?}",
                lit
            )));
        }
    })
}

/// Project return expressions into a result batch.
fn project_return(
    bindings: &HashMap<String, RecordBatch>,
    projections: &[IRProjection],
    params: &ParamMap,
) -> Result<RecordBatch> {
    if projections.is_empty() {
        return Err(OmniError::Manifest(
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
                OmniError::Manifest(format!("projection references unbound variable '{}'", variable))
            })?;
            let col = batch.column_by_name(property).ok_or_else(|| {
                OmniError::Manifest(format!(
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
            let lit = params.get(name).ok_or_else(|| {
                OmniError::Manifest(format!("parameter '{}' not provided", name))
            })?;
            let num_rows = bindings.values().next().map(|b| b.num_rows()).unwrap_or(0);
            let arr = literal_to_array(lit, num_rows)?;
            Ok((name.clone(), arr))
        }
        _ => Err(OmniError::Manifest(format!(
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
    params: &ParamMap,
) -> Result<RecordBatch> {
    use arrow_ord::sort::{SortColumn, lexsort_to_indices};

    let mut sort_columns = Vec::with_capacity(orderings.len());

    for ordering in orderings {
        let col = match &ordering.expr {
            IRExpr::PropAccess { variable, property } => {
                let binding = bindings.get(variable).ok_or_else(|| {
                    OmniError::Manifest(format!("ordering references unbound variable '{}'", variable))
                })?;
                binding.column_by_name(property).ok_or_else(|| {
                    OmniError::Manifest(format!("column '{}' not found for ordering", property))
                })?
                .clone()
            }
            IRExpr::AliasRef(alias) => {
                // Look up in the projected batch by column name
                batch.column_by_name(alias).ok_or_else(|| {
                    OmniError::Manifest(format!("alias '{}' not found for ordering", alias))
                })?
                .clone()
            }
            _ => {
                return Err(OmniError::Manifest(
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

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| OmniError::Lance(e.to_string()))?;

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
        IRExpr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            OmniError::Manifest(format!("parameter '{}' not provided", name))
        }),
        other => Err(OmniError::Manifest(format!(
            "unsupported expression in mutation: {:?}",
            other
        ))),
    }
}

/// Create a single-element or N-element array from a Literal, matching the target DataType.
fn literal_to_typed_array(lit: &Literal, data_type: &DataType, num_rows: usize) -> Result<ArrayRef> {
    Ok(match (lit, data_type) {
        (Literal::String(s), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef
        }
        (Literal::Integer(n), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*n as i32; num_rows]))
        }
        (Literal::Integer(n), DataType::Int64) => {
            Arc::new(Int64Array::from(vec![*n; num_rows]))
        }
        (Literal::Integer(n), DataType::UInt32) => {
            Arc::new(UInt32Array::from(vec![*n as u32; num_rows]))
        }
        (Literal::Integer(n), DataType::UInt64) => {
            Arc::new(UInt64Array::from(vec![*n as u64; num_rows]))
        }
        (Literal::Float(f), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![*f as f32; num_rows]))
        }
        (Literal::Float(f), DataType::Float64) => {
            Arc::new(Float64Array::from(vec![*f; num_rows]))
        }
        (Literal::Bool(b), DataType::Boolean) => {
            Arc::new(BooleanArray::from(vec![*b; num_rows]))
        }
        (Literal::Date(s), DataType::Date32) => {
            let days = parse_date32(s).ok_or_else(|| {
                OmniError::Manifest(format!("invalid date: {}", s))
            })?;
            Arc::new(Date32Array::from(vec![days; num_rows]))
        }
        _ => {
            return Err(OmniError::Manifest(format!(
                "cannot convert {:?} to {:?}",
                lit, data_type
            )));
        }
    })
}

/// Parse "YYYY-MM-DD" to days since epoch (same algorithm as loader/mod.rs).
fn parse_date32(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = (y2 - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era * 146097 + doe as i32 - 719468) as i32)
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
                return Err(OmniError::Manifest(format!(
                    "missing required blob property '{}'",
                    field.name()
                )));
            }
        } else if field.name() == "src" {
            let lit = assignments.get("from").ok_or_else(|| {
                OmniError::Manifest("missing required edge endpoint 'from'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.name() == "dst" {
            let lit = assignments.get("to").ok_or_else(|| {
                OmniError::Manifest("missing required edge endpoint 'to'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.is_nullable() {
            columns.push(arrow_array::new_null_array(field.data_type(), 1));
        } else {
            return Err(OmniError::Manifest(format!(
                "missing required property '{}'",
                field.name()
            )));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))
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
            return Err(OmniError::Manifest(
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

    for (idx, field) in schema.fields().iter().enumerate() {
        if blob_properties.contains(field.name()) {
            // Blob columns aren't in the scan result — skip
            continue;
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(lit, field.data_type(), batch.num_rows())?);
        } else {
            columns.push(batch.column(idx).clone());
        }
    }

    // Build schema without blob columns
    let non_blob_fields: Vec<_> = schema.fields().iter()
        .filter(|f| !blob_properties.contains(f.name()))
        .map(|f| f.as_ref().clone())
        .collect();

    RecordBatch::try_new(Arc::new(Schema::new(non_blob_fields)), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

// ─── Mutation execution ──────────────────────────────────────────────────────

impl Omnigraph {
    /// Run a named mutation from a .gq query source string.
    pub async fn run_mutation(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::Manifest(e.to_string()))?;

        let checked = typecheck_query_decl(self.catalog(), &query_decl)?;
        match checked {
            CheckedQuery::Mutation(_) => {}
            CheckedQuery::Read(_) => {
                return Err(OmniError::Manifest(
                    "run_mutation called on a read query; use run_query instead".to_string(),
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
                        return Err(OmniError::Manifest(format!(
                            "insert missing @key property '{}'",
                            key_prop
                        )));
                    }
                }
            } else {
                ulid::Ulid::new().to_string()
            };

            let batch = build_insert_batch(&schema, &id, &resolved, &blob_props)?;
            let has_key = node_type.key_property().is_some();
            let (new_version, row_count) = if has_key {
                self.upsert_batch(type_name, true, schema, batch).await?
            } else {
                self.append_batch(type_name, true, schema, batch).await?
            };

            let table_key = format!("node:{}", type_name);
            self.manifest_mut()
                .commit(&[crate::db::SubTableUpdate {
                    table_key,
                    table_version: new_version,
                    table_branch: None,
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
            let (new_version, row_count) =
                self.append_batch(type_name, false, schema, batch).await?;

            let table_key = format!("edge:{}", type_name);
            self.manifest_mut()
                .commit(&[crate::db::SubTableUpdate {
                    table_key,
                    table_version: new_version,
                    table_branch: None,
                    row_count,
                }])
                .await?;

            self.invalidate_graph_index();

            Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 1,
            })
        } else {
            Err(OmniError::Manifest(format!(
                "unknown type '{}'",
                type_name
            )))
        }
    }

    /// Append a batch to a sub-table, returning (new_version, row_count).
    async fn append_batch(
        &self,
        type_name: &str,
        is_node: bool,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(u64, u64)> {
        let table_key = if is_node {
            format!("node:{}", type_name)
        } else {
            format!("edge:{}", type_name)
        };
        let snapshot = self.snapshot();
        let entry = snapshot.entry(&table_key).ok_or_else(|| {
            OmniError::Manifest(format!("no manifest entry for {}", table_key))
        })?;
        let full_path = format!("{}/{}", self.uri(), entry.table_path);

        let mut ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        ds.append(reader, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let new_version = ds.version().version;
        let row_count = ds
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

        Ok((new_version, row_count))
    }

    /// Upsert a batch into a sub-table using merge_insert keyed by "id".
    /// Used for @key node types to enforce uniqueness.
    async fn upsert_batch(
        &self,
        type_name: &str,
        is_node: bool,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(u64, u64)> {
        let table_key = if is_node {
            format!("node:{}", type_name)
        } else {
            format!("edge:{}", type_name)
        };
        let snapshot = self.snapshot();
        let entry = snapshot.entry(&table_key).ok_or_else(|| {
            OmniError::Manifest(format!("no manifest entry for {}", table_key))
        })?;
        let full_path = format!("{}/{}", self.uri(), entry.table_path);

        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let ds = Arc::new(ds);

        let job = lance::dataset::MergeInsertBuilder::try_new(
            ds,
            vec!["id".to_string()],
        )
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .when_matched(lance::dataset::WhenMatched::UpdateAll)
        .when_not_matched(lance::dataset::WhenNotMatched::InsertAll)
        .try_build()
        .map_err(|e| OmniError::Lance(e.to_string()))?;

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let (new_ds, _stats) = job
            .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let new_version = new_ds.version().version;
        let row_count = new_ds
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

        Ok((new_version, row_count))
    }

    async fn execute_update(
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        predicate: &IRMutationPredicate,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        // Reject updates to @key properties — identity is immutable
        if let Some(key_prop) = self.catalog().node_types[type_name].key_property() {
            if assignments.iter().any(|a| a.property == key_prop) {
                return Err(OmniError::Manifest(format!(
                    "cannot update @key property '{}' — delete and re-insert instead",
                    key_prop
                )));
            }
        }

        let pred_sql = predicate_to_sql(predicate, params, false)?;
        let schema = self.catalog().node_types[type_name].arrow_schema.clone();
        let blob_props = self.catalog().node_types[type_name].blob_properties.clone();

        let snapshot = self.snapshot();
        let table_key = format!("node:{}", type_name);
        let entry = snapshot.entry(&table_key).ok_or_else(|| {
            OmniError::Manifest(format!("no manifest entry for {}", table_key))
        })?;
        let full_path = format!("{}/{}", self.uri(), entry.table_path);

        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let mut scanner = ds.scan();
        if !blob_props.is_empty() {
            let non_blob_cols: Vec<&str> = schema
                .fields()
                .iter()
                .filter(|f| !blob_props.contains(f.name()))
                .map(|f| f.name().as_str())
                .collect();
            scanner
                .project(&non_blob_cols)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
        }
        scanner
            .filter(&pred_sql)
            .map_err(|e| OmniError::Lance(format!("update filter: {}", e)))?;

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

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

        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let ds = Arc::new(ds);

        let job = lance::dataset::MergeInsertBuilder::try_new(
            ds,
            vec!["id".to_string()],
        )
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .when_matched(lance::dataset::WhenMatched::UpdateAll)
        .when_not_matched(lance::dataset::WhenNotMatched::DoNothing)
        .try_build()
        .map_err(|e| OmniError::Lance(e.to_string()))?;

        let update_schema = updated.schema();
        let reader = RecordBatchIterator::new(vec![Ok(updated)], update_schema);
        let (new_ds, _stats) = job
            .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let mut final_version = new_ds.version().version;
        let mut final_row_count = new_ds
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

        // Phase 2: If there are blob assignments, apply them separately
        let blob_assignments: HashMap<&str, &Literal> = resolved
            .iter()
            .filter(|(k, _)| blob_props.contains(k.as_str()))
            .map(|(k, v)| (k.as_str(), v))
            .collect();

        if !blob_assignments.is_empty() {
            // Extract matched IDs from the scan result
            let id_col = matched.column_by_name("id").ok_or_else(|| {
                OmniError::Manifest("matched batch missing 'id' column".to_string())
            })?;
            let ids = id_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                OmniError::Manifest("id column is not Utf8".to_string())
            })?;

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
                        builder.finish().map_err(|e| OmniError::Lance(e.to_string()))?,
                    );
                }
            }

            let blob_schema = Arc::new(Schema::new(blob_fields));
            let blob_batch = RecordBatch::try_new(blob_schema.clone(), blob_columns)
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            let ds = Dataset::open(&full_path)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            let ds = Arc::new(ds);

            let job = lance::dataset::MergeInsertBuilder::try_new(
                ds,
                vec!["id".to_string()],
            )
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .when_matched(lance::dataset::WhenMatched::UpdateAll)
            .when_not_matched(lance::dataset::WhenNotMatched::DoNothing)
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?;

            let reader = RecordBatchIterator::new(vec![Ok(blob_batch)], blob_schema);
            let (new_ds, _stats) = job
                .execute(lance_datafusion::utils::reader_to_stream(Box::new(reader)))
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            final_version = new_ds.version().version;
            final_row_count = new_ds
                .count_rows(None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))? as u64;
        }

        self.manifest_mut()
            .commit(&[crate::db::SubTableUpdate {
                table_key,
                table_version: final_version,
                table_branch: None,
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

        let snapshot = self.snapshot();
        let table_key = format!("node:{}", type_name);
        let entry = snapshot.entry(&table_key).ok_or_else(|| {
            OmniError::Manifest(format!("no manifest entry for {}", table_key))
        })?;
        let full_path = format!("{}/{}", self.uri(), entry.table_path);

        // Scan matching IDs for cascade
        let ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let mut scanner = ds.scan();
        scanner
            .project(&["id"])
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        scanner
            .filter(&pred_sql)
            .map_err(|e| OmniError::Lance(format!("delete filter: {}", e)))?;

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

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

        // Delete nodes
        let mut ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let delete_result = ds
            .delete(&pred_sql)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let node_version = delete_result.new_dataset.version().version;
        let node_row_count = delete_result
            .new_dataset
            .count_rows(None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

        let mut updates = vec![crate::db::SubTableUpdate {
            table_key,
            table_version: node_version,
            table_branch: None,
            row_count: node_row_count,
        }];

        // Edge cascade
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
            let Some(edge_entry) = snapshot.entry(&edge_table_key) else {
                continue;
            };
            let edge_full_path = format!("{}/{}", self.uri(), edge_entry.table_path);

            let cascade_filter = cascade_filters.join(" OR ");
            let mut edge_ds = Dataset::open(&edge_full_path)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            let edge_delete = edge_ds
                .delete(&cascade_filter)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;

            affected_edges += edge_delete.num_deleted_rows as usize;

            if edge_delete.num_deleted_rows > 0 {
                let edge_version = edge_delete.new_dataset.version().version;
                let edge_row_count = edge_delete
                    .new_dataset
                    .count_rows(None)
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

                updates.push(crate::db::SubTableUpdate {
                    table_key: edge_table_key,
                    table_version: edge_version,
                    table_branch: None,
                    row_count: edge_row_count,
                });
            }
        }

        self.manifest_mut().commit(&updates).await?;

        if affected_edges > 0 {
            self.invalidate_graph_index();
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

        let snapshot = self.snapshot();
        let table_key = format!("edge:{}", type_name);
        let entry = snapshot.entry(&table_key).ok_or_else(|| {
            OmniError::Manifest(format!("no manifest entry for {}", table_key))
        })?;
        let full_path = format!("{}/{}", self.uri(), entry.table_path);

        let mut ds = Dataset::open(&full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let delete_result = ds
            .delete(&pred_sql)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        let affected = delete_result.num_deleted_rows as usize;

        if affected > 0 {
            let new_version = delete_result.new_dataset.version().version;
            let row_count = delete_result
                .new_dataset
                .count_rows(None)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))? as u64;

            self.manifest_mut()
                .commit(&[crate::db::SubTableUpdate {
                    table_key,
                    table_version: new_version,
                    table_branch: None,
                    row_count,
                }])
                .await?;

            self.invalidate_graph_index();
        }

        Ok(MutationResult {
            affected_nodes: 0,
            affected_edges: affected,
        })
    }
}
