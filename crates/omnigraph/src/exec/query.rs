use super::*;

use super::projection::{apply_filter, apply_ordering, project_return};

impl Omnigraph {
    /// Run a named query against an explicit branch or snapshot target.
    pub async fn query(
        &self,
        target: impl Into<ReadTarget>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<QueryResult> {
        self.ensure_schema_state_valid().await?;
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
        self.ensure_schema_state_valid().await?;
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
            Some(Arc::new(GraphIndex::build(&snapshot, &edge_types).await?))
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
            let expected_dim = nearest_property_dimension(ir, catalog, variable, property)?;
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

    let mut wide: Option<RecordBatch> = None;
    execute_pipeline(&ir.pipeline, params, snapshot, graph_index, catalog, &mut wide, &search_mode).await?;
    let wide_batch = wide.unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::empty())));

    // Project return expressions
    let has_aggregates = ir.return_exprs.iter().any(|p| matches!(&p.expr, IRExpr::Aggregate { .. }));
    let mut result_batch = project_return(&wide_batch, &ir.return_exprs, params)?;

    // Apply ordering (skip if search mode already ordered the results)
    if !ir.order_by.is_empty() && !is_search_ordered(&search_mode) {
        result_batch = if has_aggregates {
            apply_ordering(result_batch.clone(), &ir.order_by, &result_batch, params)?
        } else {
            apply_ordering(result_batch, &ir.order_by, &wide_batch, params)?
        };
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
    let mut primary_wide: Option<RecordBatch> = None;
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut primary_wide,
        &rrf.primary,
    )
    .await?;

    // Execute secondary search
    let mut secondary_wide: Option<RecordBatch> = None;
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut secondary_wide,
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

    let primary_batch = primary_wide.as_ref().ok_or_else(|| {
        OmniError::manifest(format!(
            "rrf primary variable '{}' not in bindings",
            primary_var
        ))
    })?;
    let secondary_batch = secondary_wide.as_ref().ok_or_else(|| {
        OmniError::manifest(format!(
            "rrf secondary variable '{}' not in bindings",
            primary_var
        ))
    })?;

    // Build ID → rank maps
    let id_col_name = format!("{}.id", primary_var);
    let primary_ids = extract_id_column_by_name(primary_batch, &id_col_name)?;
    let secondary_ids = extract_id_column_by_name(secondary_batch, &id_col_name)?;

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

    // Project directly from fused batch
    let result_batch = project_return(&fused_batch, &ir.return_exprs, params)?;

    // Already ordered by RRF score + already limited
    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}

fn extract_id_column_by_name(batch: &RecordBatch, col_name: &str) -> Result<Vec<String>> {
    let col = batch
        .column_by_name(col_name)
        .ok_or_else(|| OmniError::manifest(format!("batch missing '{}' column for RRF", col_name)))?;
    let ids = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", col_name)))?;
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
    wide: &'a mut Option<RecordBatch>,
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
                    let prefixed = prefix_batch(&batch, variable)?;
                    *wide = Some(match wide.take() {
                        None => prefixed,
                        Some(existing) => cross_join_batches(&existing, &prefixed)?,
                    });
                }
                IROp::Filter(filter) => {
                    if let Some(batch) = wide.as_mut() {
                        apply_filter(batch, filter, params)?;
                    }
                }
                IROp::Expand {
                    src_var,
                    dst_var,
                    edge_type,
                    direction,
                    dst_type,
                    min_hops,
                    max_hops,
                    dst_filters,
                } => {
                    let gi = graph_index.ok_or_else(|| {
                        OmniError::manifest("graph index required for traversal".to_string())
                    })?;
                    if let Some(batch) = wide.as_mut() {
                        execute_expand(
                            batch, gi, snapshot, catalog, src_var, dst_var, edge_type, *direction,
                            dst_type, *min_hops, *max_hops, dst_filters, params,
                        )
                        .await?;
                    }
                }
                IROp::AntiJoin { outer_var, inner } => {
                    let gi = graph_index;
                    if let Some(batch) = wide.as_mut() {
                        execute_anti_join(batch, inner, params, snapshot, gi, catalog, outer_var)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    })
}

/// Execute a graph traversal (Expand).
async fn execute_expand(
    wide: &mut RecordBatch,
    graph_index: &GraphIndex,
    snapshot: &Snapshot,
    catalog: &Catalog,
    src_var: &str,
    dst_var: &str,
    edge_type: &str,
    direction: Direction,
    dst_type: &str,
    min_hops: u32,
    max_hops: Option<u32>,
    dst_filters: &[IRFilter],
    params: &ParamMap,
) -> Result<()> {
    let src_id_col_name = format!("{}.id", src_var);
    let src_ids = wide
        .column_by_name(&src_id_col_name)
        .ok_or_else(|| OmniError::manifest(format!("wide batch missing '{}' column", src_id_col_name)))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", src_id_col_name)))?
        .clone();

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

    // BFS to collect (src_row_idx, dst_id) pairs with per-source dedup
    let mut src_indices: Vec<u32> = Vec::new();
    let mut dst_id_list: Vec<String> = Vec::new();
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
                                    src_indices.push(i as u32);
                                    dst_id_list.push(dst_id);
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

    // Split dst_filters: SQL-pushable go to Lance, the rest applied post-hconcat
    let pushdown_sql = build_lance_filter(dst_filters, params);
    let non_pushable: Vec<&IRFilter> = dst_filters
        .iter()
        .filter(|f| ir_filter_to_sql(f, params).is_none())
        .collect();

    // Hydrate destination nodes from the snapshot (with pushed-down filters)
    let dst_batch = hydrate_nodes(snapshot, catalog, dst_type, &dst_id_list, pushdown_sql.as_deref()).await?;

    // Build a mapping from dst_id to row index in dst_batch
    let dst_batch_id_col = dst_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("hydrated batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("hydrated 'id' column is not Utf8".to_string()))?;
    let dst_id_to_row: HashMap<&str, usize> = (0..dst_batch_id_col.len())
        .map(|i| (dst_batch_id_col.value(i), i))
        .collect();

    // Build aligned src/dst index arrays (only for IDs that exist in hydrated batch)
    let mut final_src_indices: Vec<u32> = Vec::new();
    let mut dst_indices: Vec<u32> = Vec::new();
    for (src_idx, dst_id) in src_indices.iter().zip(dst_id_list.iter()) {
        if let Some(&dst_row) = dst_id_to_row.get(dst_id.as_str()) {
            final_src_indices.push(*src_idx);
            dst_indices.push(dst_row as u32);
        }
    }

    let src_take = UInt32Array::from(final_src_indices);
    let dst_take = UInt32Array::from(dst_indices);
    let expanded_wide = take_batch(wide, &src_take)?;
    let dst_prefixed = prefix_batch(&dst_batch, dst_var)?;
    let aligned_dst = take_batch(&dst_prefixed, &dst_take)?;
    *wide = hconcat_batches(&expanded_wide, &aligned_dst)?;

    // Apply any non-pushable destination filters (e.g. list-contains) in memory
    for f in &non_pushable {
        apply_filter(wide, f, params)?;
    }

    Ok(())
}

/// Load full node rows for a set of IDs from a snapshot.
///
/// When `extra_filter_sql` is provided (from deferred destination-binding
/// filters), it is ANDed with the `id IN (...)` clause so that Lance can
/// skip non-matching rows at the storage level.
async fn hydrate_nodes(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    ids: &[String],
    extra_filter_sql: Option<&str>,
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
    let mut filter_sql = format!("id IN ({})", escaped.join(", "));
    if let Some(extra) = extra_filter_sql {
        filter_sql = format!("({}) AND ({})", filter_sql, extra);
    }
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

/// Try bulk anti-join via CSR existence check. Returns Some(mask) if the inner
/// pipeline is a single Expand from outer_var (the common negation pattern).
fn try_bulk_anti_join_mask(
    wide: &RecordBatch,
    inner_pipeline: &[IROp],
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    outer_var: &str,
) -> Option<BooleanArray> {
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

    let id_col_name = format!("{}.id", outer_var);
    let outer_ids = wide
        .column_by_name(&id_col_name)?
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

    Some(BooleanArray::from(keep_mask))
}

/// Execute an AntiJoin: remove rows from wide batch where the inner pipeline finds matches.
async fn execute_anti_join(
    wide: &mut RecordBatch,
    inner_pipeline: &[IROp],
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
    outer_var: &str,
) -> Result<()> {
    // Fast path: bulk CSR existence check (O(N), zero Lance I/O)
    if let Some(mask) =
        try_bulk_anti_join_mask(wide, inner_pipeline, graph_index, catalog, outer_var)
    {
        *wide = arrow_select::filter::filter_record_batch(wide, &mask)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        return Ok(());
    }

    // Slow path: per-row inner pipeline execution
    let num_rows = wide.num_rows();
    let mut keep_mask = vec![true; num_rows];

    for i in 0..num_rows {
        let single_row = wide.slice(i, 1);
        let mut inner_wide: Option<RecordBatch> = Some(single_row);

        let no_search = SearchMode::default();
        execute_pipeline(
            inner_pipeline,
            params,
            snapshot,
            graph_index,
            catalog,
            &mut inner_wide,
            &no_search,
        )
        .await?;

        let has_match = inner_wide
            .as_ref()
            .map(|batch| batch.num_rows() > 0)
            .unwrap_or(false);

        if has_match {
            keep_mask[i] = false;
        }
    }

    let mask = BooleanArray::from(keep_mask);
    *wide = arrow_select::filter::filter_record_batch(wide, &mask)
        .map_err(|e| OmniError::Lance(e.to_string()))?;
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

pub(super) fn literal_to_sql(lit: &Literal) -> String {
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

fn prefix_batch(batch: &RecordBatch, variable: &str) -> Result<RecordBatch> {
    let fields: Vec<Field> = batch.schema().fields().iter().map(|f| {
        Field::new(format!("{}.{}", variable, f.name()), f.data_type().clone(), f.is_nullable())
    }).collect();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|e| OmniError::Lance(e.to_string()))
}

fn cross_join_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let n = left.num_rows();
    let m = right.num_rows();
    if n == 0 || m == 0 {
        let mut fields: Vec<Field> = left.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
        return Ok(RecordBatch::new_empty(Arc::new(Schema::new(fields))));
    }
    let left_indices: Vec<u32> = (0..n as u32).flat_map(|i| std::iter::repeat(i).take(m)).collect();
    let right_indices: Vec<u32> = (0..n).flat_map(|_| 0..m as u32).collect();
    let left_expanded = take_batch(left, &UInt32Array::from(left_indices))?;
    let right_expanded = take_batch(right, &UInt32Array::from(right_indices))?;
    hconcat_batches(&left_expanded, &right_expanded)
}

fn hconcat_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = left.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    if cfg!(debug_assertions) {
        let left_schema = left.schema();
        let left_names: HashSet<&str> = left_schema.fields().iter().map(|f| f.name().as_str()).collect();
        let right_schema = right.schema();
        for f in right_schema.fields() {
            debug_assert!(!left_names.contains(f.name().as_str()), "hconcat_batches: duplicate column '{}'", f.name());
        }
    }
    fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
    let mut columns: Vec<ArrayRef> = left.columns().to_vec();
    columns.extend(right.columns().to_vec());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| OmniError::Lance(e.to_string()))
}

fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch.columns().iter()
        .map(|col| arrow_select::take::take(col.as_ref(), indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}
