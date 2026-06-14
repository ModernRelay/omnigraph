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
        let catalog = self.catalog();

        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        let type_ctx = typecheck_query(&catalog, &query_decl)?;
        let ir = lower_query(&catalog, &query_decl, &type_ctx)?;

        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        // Lazy: an index-served query with no AntiJoin never builds the CSR.
        let graph_index = if needs_graph {
            GraphIndexHandle::cached(self, &resolved)
        } else {
            GraphIndexHandle::none()
        };

        execute_query(&ir, params, &resolved.snapshot, &graph_index, &catalog).await
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
        let catalog = self.catalog();

        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;
        let type_ctx = typecheck_query(&catalog, &query_decl)?;
        let ir = lower_query(&catalog, &query_decl, &type_ctx)?;

        let needs_graph = ir
            .pipeline
            .iter()
            .any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        // Lazy build against this historical snapshot (not the RuntimeCache,
        // which is keyed to live branch targets); only a CSR-path Expand or an
        // AntiJoin triggers it.
        let graph_index = if needs_graph {
            let edge_types = catalog
                .edge_types
                .iter()
                .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
                .collect();
            GraphIndexHandle::direct(&snapshot, edge_types)
        } else {
            GraphIndexHandle::none()
        };

        execute_query(&ir, params, &snapshot, &graph_index, &catalog).await
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
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let search_mode = extract_search_mode(ir, params, catalog).await?;

    // RRF requires forked execution
    if let Some(ref rrf) = search_mode.rrf {
        return execute_rrf_query(ir, params, snapshot, graph_index, catalog, rrf).await;
    }

    let mut wide: Option<RecordBatch> = None;
    execute_pipeline(
        &ir.pipeline,
        params,
        snapshot,
        graph_index,
        catalog,
        &mut wide,
        &search_mode,
    )
    .await?;
    let wide_batch = wide.unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::empty())));

    // Project return expressions
    let has_aggregates = ir
        .return_exprs
        .iter()
        .any(|p| matches!(&p.expr, IRExpr::Aggregate { .. }));
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
    graph_index: &GraphIndexHandle<'_>,
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
    let col = batch.column_by_name(col_name).ok_or_else(|| {
        OmniError::manifest(format!("batch missing '{}' column for RRF", col_name))
    })?;
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
    graph_index: &'a GraphIndexHandle<'a>,
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
                    if let Some(batch) = wide.as_mut() {
                        execute_expand(
                            batch,
                            graph_index,
                            snapshot,
                            catalog,
                            src_var,
                            dst_var,
                            edge_type,
                            *direction,
                            dst_type,
                            *min_hops,
                            *max_hops,
                            dst_filters,
                            params,
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

/// Lazily provides the in-memory CSR graph index, building it on first use and
/// memoizing for the rest of the query. Indexed-mode Expand never asks for it,
/// so a query that is entirely index-served and has no AntiJoin never pays the
/// O(|E|) CSR build (the whole point of the indexed path). The `Cached` builder
/// also reuses the cross-query `RuntimeCache` entry; `Direct` builds against an
/// arbitrary snapshot (time-travel reads); `None` is for queries with no
/// traversal at all.
pub struct GraphIndexHandle<'a> {
    cell: tokio::sync::OnceCell<Option<Arc<GraphIndex>>>,
    builder: GraphIndexBuilder<'a>,
}

enum GraphIndexBuilder<'a> {
    None,
    Cached(&'a Omnigraph, &'a crate::db::ResolvedTarget),
    Direct(&'a Snapshot, HashMap<String, (String, String)>),
}

impl<'a> GraphIndexHandle<'a> {
    fn none() -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::None,
        }
    }

    fn cached(db: &'a Omnigraph, resolved: &'a crate::db::ResolvedTarget) -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::Cached(db, resolved),
        }
    }

    fn direct(snapshot: &'a Snapshot, edge_types: HashMap<String, (String, String)>) -> Self {
        Self {
            cell: tokio::sync::OnceCell::new(),
            builder: GraphIndexBuilder::Direct(snapshot, edge_types),
        }
    }

    /// The CSR index, built on first call. `None` only when the query needs no
    /// traversal (the `None` builder).
    async fn get(&self) -> Result<Option<&GraphIndex>> {
        let built = self
            .cell
            .get_or_try_init(|| async {
                match &self.builder {
                    GraphIndexBuilder::None => Ok::<Option<Arc<GraphIndex>>, OmniError>(None),
                    GraphIndexBuilder::Cached(db, resolved) => {
                        Ok(Some(db.graph_index_for_resolved(resolved).await?))
                    }
                    GraphIndexBuilder::Direct(snapshot, edge_types) => {
                        Ok(Some(Arc::new(GraphIndex::build(snapshot, edge_types).await?)))
                    }
                }
            })
            .await?;
        Ok(built.as_deref())
    }

    /// Whether the in-memory CSR is already materialized for this query (a prior
    /// Expand or bulk AntiJoin realized it), so reusing it is ~free. Lets the
    /// cost chooser prefer the warm CSR over per-hop indexed scans.
    fn is_built(&self) -> bool {
        matches!(self.cell.get(), Some(Some(_)))
    }
}

/// Explicit traversal-mode override. `OMNIGRAPH_TRAVERSAL_MODE=indexed|csr`
/// forces the path (ops escape hatch + test hook). Both modes are semantically
/// identical, so the override only changes which path runs, never the result.
fn traversal_indexed_override() -> Option<bool> {
    match std::env::var("OMNIGRAPH_TRAVERSAL_MODE").ok().as_deref() {
        Some("indexed") => Some(true),
        Some("csr") => Some(false),
        _ => None,
    }
}

/// Max source-row frontier for which Expand uses the BTREE-indexed path.
/// Larger frontiers fall back to the in-memory CSR (dense / whole-graph). See
/// `docs/user/reference/constants.md`.
const DEFAULT_EXPAND_INDEXED_MAX_FRONTIER: usize = 1024;
/// Max hop count for the indexed path (each hop is one indexed scan; very deep
/// traversals fan out toward whole-graph and are better served by CSR).
const DEFAULT_EXPAND_INDEXED_MAX_HOPS: u32 = 6;

fn expand_indexed_max_frontier() -> usize {
    std::env::var("OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_FRONTIER)
}

fn expand_indexed_max_hops() -> u32 {
    std::env::var("OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_EXPAND_INDEXED_MAX_HOPS)
}

/// The two Expand execution paths the chooser dispatches between. Extensible:
/// a future persisted-adjacency artifact would become a third variant here, and
/// `choose_expand_mode` would learn to prefer it when covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpandMode {
    /// Per-hop neighbor lookup via the persisted src/dst BTREE. Work scales
    /// with the frontier, not |E| — best for selective traversals.
    IndexedScan,
    /// Whole-graph in-memory CSR (built once, reused). Best for dense / deep /
    /// large-frontier traversals, or when the index is degraded and a full
    /// scan would be paid per hop anyway.
    Csr,
}

/// Building the in-memory CSR costs more than a bare edge scan: it scans every
/// edge AND allocates + groups the adjacency. This factor expresses that
/// overhead so a one-off degraded single-hop scan can still edge out a full CSR
/// build. The crossover is insensitive to its exact value.
const CSR_BUILD_FACTOR: f64 = 1.5;

/// Cardinality inputs for the (pure, IO-free) traversal-mode cost model. Every
/// field is a cheap manifest-resident count or an already-in-hand value — the
/// chooser performs no scans.
#[derive(Debug, Clone)]
struct ExpandCostInputs {
    /// Current frontier size (`wide.num_rows()`).
    frontier_rows: usize,
    /// |E| for the edge type (manifest `row_count`).
    edge_count: u64,
    /// |V_src| — node count of the keyed endpoint type (manifest `row_count`).
    src_node_count: u64,
    /// Effective max hop count for this Expand.
    effective_max_hops: u32,
    /// Hard ceiling above which the indexed path is never used (resolved
    /// `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS`).
    max_hops_cap: u32,
    /// Hard ceiling above which the indexed path is never used (resolved
    /// `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`).
    max_frontier_cap: usize,
    /// Whether `scan_edges_by_endpoint`'s `key_col IN (...)` is served by the
    /// BTREE (`Indexed`) or silently falls back to a full scan (`Degraded`).
    coverage: crate::table_store::IndexCoverage,
    /// Whether the cross-query CSR for this snapshot+edge-version is already
    /// built (making the CSR path ≈ free). Conservatively `false` until the
    /// cache-peek is wired (the plan's optional refinement).
    csr_cached: bool,
}

/// Pure cost-based traversal-mode chooser. Compares an estimate of the indexed
/// path's frontier-relative work against the cost of building (or reusing) the
/// whole-graph CSR, and picks the cheaper. Deterministic and IO-free so it is
/// unit-tested at the crossover; the caller supplies the manifest counts and the
/// (optionally degraded) index coverage.
///
/// Under `Indexed` coverage and a cold CSR the decision reduces to a clean
/// selectivity ratio — indexed wins when `hops * frontier < BUILD_FACTOR *
/// |V_src|`, i.e. when the frontier is a small fraction of the source vertex
/// set — which is independent of |E| (the flat-in-|E| property PR #149 shipped).
fn choose_expand_mode(i: &ExpandCostInputs) -> ExpandMode {
    // Hard ceilings: very deep or very large frontiers fan out toward
    // whole-graph and are always better served by CSR, regardless of the cost
    // estimate. These preserve the documented semantics of the two cap flags.
    if i.effective_max_hops > i.max_hops_cap || i.frontier_rows > i.max_frontier_cap {
        return ExpandMode::Csr;
    }

    let hops = i.effective_max_hops.max(1) as f64;
    let frontier = i.frontier_rows as f64;
    let edges = i.edge_count as f64;
    let src = i.src_node_count.max(1) as f64;
    let fanout = edges / src;

    // Indexed work scales with the frontier when the BTREE serves the IN-list;
    // a degraded scan is a full edge scan per hop instead (the C6 perf cliff).
    let indexed_cost = match i.coverage {
        crate::table_store::IndexCoverage::Indexed => hops * frontier * fanout,
        crate::table_store::IndexCoverage::Degraded { .. } => hops * edges,
    };
    // A warm CSR is ~free to reuse; a cold one costs a build over all edges.
    let csr_cost = if i.csr_cached {
        0.0
    } else {
        CSR_BUILD_FACTOR * edges
    };

    if indexed_cost < csr_cost {
        ExpandMode::IndexedScan
    } else {
        ExpandMode::Csr
    }
}

/// Hops the indexed path will actually run, for cost-model purposes. A cross-type
/// edge cannot chain, so `execute_expand_indexed` caps it at one hop regardless of
/// the requested range; the cost model must use that, or it over-estimates the
/// indexed cost of a cross-type variable-length expand and skews toward CSR.
fn cost_effective_hops(requested_max_hops: u32, same_type: bool) -> u32 {
    if same_type {
        requested_max_hops
    } else {
        requested_max_hops.min(1)
    }
}

/// Gather the cost-model inputs from cheap manifest counts. `None` when the
/// edge type, its source node type, or their manifest entries are absent (e.g.
/// a not-yet-materialized table) — the caller then falls back to the legacy
/// frontier/hop ceiling so the decision is always defined.
fn gather_cost_inputs(
    snapshot: &Snapshot,
    catalog: &Catalog,
    edge_type: &str,
    direction: Direction,
    frontier_rows: usize,
    effective_max_hops: u32,
    coverage: crate::table_store::IndexCoverage,
    csr_cached: bool,
) -> Option<ExpandCostInputs> {
    let edge_entry = snapshot.entry(&format!("edge:{}", edge_type))?;
    let edge_def = catalog.edge_types.get(edge_type)?;
    // Match the indexed path's cross-type one-hop cap so the cost estimate
    // reflects what actually runs (see `cost_effective_hops`).
    let effective_max_hops =
        cost_effective_hops(effective_max_hops, edge_def.from_type == edge_def.to_type);
    // The frontier source vertices are the keyed endpoint's type: `from` for an
    // Out traversal (keyed on `src`), `to` for In (keyed on `dst`).
    let src_type = match direction {
        Direction::Out => &edge_def.from_type,
        Direction::In => &edge_def.to_type,
    };
    let src_entry = snapshot.entry(&format!("node:{}", src_type))?;
    Some(ExpandCostInputs {
        frontier_rows,
        edge_count: edge_entry.row_count,
        src_node_count: src_entry.row_count,
        effective_max_hops,
        max_hops_cap: expand_indexed_max_hops(),
        max_frontier_cap: expand_indexed_max_frontier(),
        coverage,
        csr_cached,
    })
}

/// Coverage value to feed the cost decision. A failed coverage probe is treated
/// as `Degraded` (conservative: don't over-favor the indexed path when we can't
/// confirm the BTREE will serve the scan).
fn coverage_for_decision(
    coverage: &Result<crate::table_store::IndexCoverage>,
) -> crate::table_store::IndexCoverage {
    match coverage {
        Ok(c) => c.clone(),
        Err(_) => crate::table_store::IndexCoverage::Degraded {
            reason: "coverage check failed".to_string(),
        },
    }
}

/// Surface the C6 silent scalar-index fallback (commit `5a7ab6d`): warn when the
/// per-hop `key_col IN (...)` won't route through the BTREE. Detection-only;
/// never fails the query. Behavior-identical to the inline check it replaced.
fn warn_on_degraded_coverage(
    coverage: &Result<crate::table_store::IndexCoverage>,
    key_col: &str,
    edge_type: &str,
) {
    match coverage {
        Ok(crate::table_store::IndexCoverage::Degraded { reason }) => tracing::warn!(
            target: "omnigraph::traverse",
            edge = %edge_type,
            key_col = key_col,
            reason = %reason,
            "indexed traversal falls back to a full edge scan (results correct, perf degraded)"
        ),
        Ok(crate::table_store::IndexCoverage::Indexed) => {}
        Err(e) => tracing::debug!(
            target: "omnigraph::traverse",
            error = %e,
            "index-coverage check failed; proceeding with traversal"
        ),
    }
}

/// The (key, opposite) endpoint columns for a traversal direction. Out follows
/// src -> dst (key on src); In follows the reverse. The persisted BTREE exists
/// on both columns.
fn endpoint_columns(direction: Direction) -> (&'static str, &'static str) {
    match direction {
        Direction::Out => ("src", "dst"),
        Direction::In => ("dst", "src"),
    }
}

/// Execute a graph traversal (Expand). Dispatches to the BTREE-indexed path
/// (selective traversals — neighbor lookups via the persisted src/dst index) or
/// the in-memory CSR path (dense / whole-graph traversals). The CSR index is
/// built lazily and only the CSR path requests it.
async fn execute_expand(
    wide: &mut RecordBatch,
    graph_index: &GraphIndexHandle<'_>,
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
    let frontier_rows = wide.num_rows();
    let effective_max_hops = max_hops.unwrap_or(min_hops.max(1));
    let (key_col, _) = endpoint_columns(direction);
    let edge_table_key = format!("edge:{}", edge_type);

    // Cardinality-first preliminary decision (no IO). The override wins; else the
    // cost model decides under *optimistic* coverage. Optimistic is what lets us
    // skip the dataset open on a clearly-CSR traversal: real coverage can only
    // make the indexed path costlier, so if even a perfectly-indexed scan loses
    // to CSR here, it loses for real.
    let forced = traversal_indexed_override();
    let lean_indexed = match forced {
        Some(v) => v,
        None => match gather_cost_inputs(
            snapshot,
            catalog,
            edge_type,
            direction,
            frontier_rows,
            effective_max_hops,
            crate::table_store::IndexCoverage::Indexed,
            graph_index.is_built(),
        ) {
            Some(inputs) => choose_expand_mode(&inputs) == ExpandMode::IndexedScan,
            // Manifest counts absent (e.g. not-yet-materialized table): fall back
            // to the legacy frontier/hop ceiling so the decision is defined.
            None => {
                frontier_rows <= expand_indexed_max_frontier()
                    && effective_max_hops <= expand_indexed_max_hops()
            }
        },
    };

    if !lean_indexed {
        tracing::debug!(
            target: "omnigraph::traverse",
            edge = %edge_type,
            frontier = frontier_rows,
            hops = effective_max_hops,
            mode = "csr",
            "expand mode chosen",
        );
        let gi = graph_index.get().await?.ok_or_else(|| {
            OmniError::manifest("graph index required for CSR traversal".to_string())
        })?;
        return execute_expand_csr(
            wide, gi, snapshot, catalog, src_var, dst_var, edge_type, direction, dst_type,
            min_hops, max_hops, dst_filters, params,
        )
        .await;
    }

    // Leaning indexed: open the edge dataset once, confirm real coverage, and
    // (unless forced) re-decide with it. The opened dataset is threaded into the
    // indexed path so it is never opened twice.
    let edge_ds = snapshot.open(&edge_table_key).await?;
    let coverage =
        crate::table_store::TableStore::key_column_index_coverage(&edge_ds, key_col).await;

    if forced.is_none() {
        if let Some(inputs) = gather_cost_inputs(
            snapshot,
            catalog,
            edge_type,
            direction,
            frontier_rows,
            effective_max_hops,
            coverage_for_decision(&coverage),
            graph_index.is_built(),
        ) {
            if choose_expand_mode(&inputs) == ExpandMode::Csr {
                tracing::debug!(
                    target: "omnigraph::traverse",
                    edge = %edge_type,
                    frontier = frontier_rows,
                    hops = effective_max_hops,
                    mode = "csr",
                    reason = "index coverage degraded",
                    "expand mode chosen",
                );
                let gi = graph_index.get().await?.ok_or_else(|| {
                    OmniError::manifest("graph index required for CSR traversal".to_string())
                })?;
                return execute_expand_csr(
                    wide, gi, snapshot, catalog, src_var, dst_var, edge_type, direction, dst_type,
                    min_hops, max_hops, dst_filters, params,
                )
                .await;
            }
        }
    }

    tracing::debug!(
        target: "omnigraph::traverse",
        edge = %edge_type,
        frontier = frontier_rows,
        hops = effective_max_hops,
        mode = "indexed",
        "expand mode chosen",
    );
    // Surface the C6 silent scalar-index fallback once, now that coverage is known.
    warn_on_degraded_coverage(&coverage, key_col, edge_type);
    execute_expand_indexed(
        wide, snapshot, catalog, src_var, dst_var, edge_type, direction, dst_type, min_hops,
        max_hops, dst_filters, params, edge_ds,
    )
    .await
}

/// BTREE-indexed graph traversal: per hop, batch the current frontier into one
/// `scan_edges_by_endpoint` call against the persisted src/dst index, then fan
/// out per source row. Cost scales with the frontier, not |E|. Produces the
/// same `(src_row, dst_id)` pairs as the CSR path and shares its hydrate+align
/// tail. Multi-hop only advances for same-type edges; cross-type frontiers go
/// empty after one hop (no edges key off the destination type), matching CSR.
async fn execute_expand_indexed(
    wide: &mut RecordBatch,
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
    edge_ds: Dataset,
) -> Result<()> {
    let src_id_col_name = format!("{}.id", src_var);
    let src_ids = wide
        .column_by_name(&src_id_col_name)
        .ok_or_else(|| {
            OmniError::manifest(format!("wide batch missing '{}' column", src_id_col_name))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("'{}' column is not Utf8", src_id_col_name)))?
        .clone();

    let edge_def = catalog
        .edge_types
        .get(edge_type)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_type)))?;
    let same_type = edge_def.from_type == edge_def.to_type;
    // The keyed/opposite endpoint columns for this direction. The edge dataset
    // and the C6 coverage warn are owned by the caller (`execute_expand`), which
    // opens the dataset once and threads it in.
    let (key_col, opp_col) = endpoint_columns(direction);

    let max = max_hops.unwrap_or(min_hops.max(1));
    // Cross-type edges cannot chain (a Company is not a `WorksAt` source), so a
    // variable-length traversal over one is structurally single-hop. Enforce it
    // here instead of relying on the hop-2 scan returning empty: this BFS interns
    // every endpoint string into ONE dense id space, so a cross-type id-string
    // collision (a Person and a Company sharing an id) would otherwise let hop 2
    // de-intern a destination id back to the colliding source-type id and match
    // its edges, emitting rows the CSR path never produces.
    let max = if same_type { max } else { max.min(1) };

    // Per-source BFS state in DENSE id space: intern node ids to u32 once via a
    // per-traversal interner so visited/seen/frontier/neighbor-map avoid string
    // hashing + cloning in the hot loop (mirrors the CSR path's TypeIndex). The
    // GraphIndex/CSR is NOT built — only a local id↔u32 dictionary. Strings
    // survive at the substrate edges only: the per-hop IN-list to Lance, and the
    // emitted dst ids handed to the string-keyed hydrate+align tail.
    let mut interner = crate::graph_index::TypeIndex::new();
    let n = src_ids.len();
    let mut frontiers: Vec<Vec<u32>> = Vec::with_capacity(n);
    let mut visited: Vec<HashSet<u32>> = Vec::with_capacity(n);
    let mut seen_dst: Vec<HashSet<u32>> = Vec::with_capacity(n);
    for i in 0..n {
        let sid = interner.get_or_insert(src_ids.value(i));
        let mut v = HashSet::new();
        if same_type {
            v.insert(sid);
        }
        frontiers.push(vec![sid]);
        visited.push(v);
        seen_dst.push(HashSet::new());
    }

    let mut src_indices: Vec<u32> = Vec::new();
    let mut dst_dense: Vec<u32> = Vec::new();

    for hop in 1..=max {
        // Union of all live frontiers (dense), de-interned once for the IN-list.
        let mut union_dense: Vec<u32> = Vec::new();
        {
            let mut seen: HashSet<u32> = HashSet::new();
            for f in &frontiers {
                for &node in f {
                    if seen.insert(node) {
                        union_dense.push(node);
                    }
                }
            }
        }
        if union_dense.is_empty() {
            break;
        }
        let union_keys: Vec<String> = union_dense
            .iter()
            .map(|&u| {
                interner
                    .to_id(u)
                    .expect("interned frontier id must resolve")
                    .to_string()
            })
            .collect();

        let batches = crate::table_store::TableStore::scan_edges_by_endpoint(
            &edge_ds, key_col, opp_col, &union_keys,
        )
        .await?;

        // dense key -> dense neighbors (scan order; duplicates preserved, like CSR multi-edges).
        let mut neighbor_map: HashMap<u32, Vec<u32>> = HashMap::new();
        for batch in &batches {
            let keys = batch
                .column_by_name(key_col)
                .ok_or_else(|| OmniError::manifest(format!("edge batch missing '{}'", key_col)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::manifest(format!("edge '{}' is not Utf8", key_col)))?;
            let opps = batch
                .column_by_name(opp_col)
                .ok_or_else(|| OmniError::manifest(format!("edge batch missing '{}'", opp_col)))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::manifest(format!("edge '{}' is not Utf8", opp_col)))?;
            for r in 0..batch.num_rows() {
                let k = interner.get_or_insert(keys.value(r));
                let o = interner.get_or_insert(opps.value(r));
                neighbor_map.entry(k).or_default().push(o);
            }
        }

        // Advance each source row's frontier independently (dense ids).
        for i in 0..n {
            let cur = std::mem::take(&mut frontiers[i]);
            let mut next: Vec<u32> = Vec::new();
            for &node in &cur {
                let Some(neighbors) = neighbor_map.get(&node) else {
                    continue;
                };
                for &neighbor in neighbors {
                    if !same_type || visited[i].insert(neighbor) {
                        next.push(neighbor);
                        if hop >= min_hops && seen_dst[i].insert(neighbor) {
                            src_indices.push(i as u32);
                            dst_dense.push(neighbor);
                        }
                    }
                }
            }
            frontiers[i] = next;
        }
    }

    // De-intern emitted destination ids (parallel to src_indices) for the
    // string-keyed hydrate+align tail, exactly as the CSR path does.
    let dst_ids: Vec<String> = dst_dense
        .iter()
        .map(|&d| {
            interner
                .to_id(d)
                .expect("interned dst id must resolve")
                .to_string()
        })
        .collect();

    expand_hydrate_and_align(
        wide, src_indices, dst_ids, snapshot, catalog, dst_type, dst_var, dst_filters, params,
    )
    .await
}

/// Shared tail for both Expand modes: hydrate the unique destination ids, align
/// the `(src_row, dst_id)` pairs back onto `wide`, hconcat, and apply
/// non-pushable destination filters in memory.
async fn expand_hydrate_and_align(
    wide: &mut RecordBatch,
    src_indices: Vec<u32>,
    dst_ids: Vec<String>,
    snapshot: &Snapshot,
    catalog: &Catalog,
    dst_type: &str,
    dst_var: &str,
    dst_filters: &[IRFilter],
    params: &ParamMap,
) -> Result<()> {
    // Pushable destination filters are applied by `hydrate_nodes`; the rest
    // (`ir_filter_to_expr` → None) are applied in memory after hconcat. The
    // schema arg only affects a pushable literal's TYPE, never Some-vs-None, so
    // `None` here yields the same pushable/non-pushable split as `hydrate_nodes`.
    let non_pushable: Vec<&IRFilter> = dst_filters
        .iter()
        .filter(|f| ir_filter_to_expr(f, params, None).is_none())
        .collect();

    // Unique destination ids (first-seen order) for one batched hydration.
    let mut unique_dst_list: Vec<String> = Vec::new();
    {
        let mut seen: HashSet<&str> = HashSet::with_capacity(dst_ids.len());
        for id in &dst_ids {
            if seen.insert(id.as_str()) {
                unique_dst_list.push(id.clone());
            }
        }
    }
    let dst_batch =
        hydrate_nodes(snapshot, catalog, dst_type, &unique_dst_list, dst_filters, params).await?;

    // id -> row index in the hydrated batch.
    let dst_batch_id_col = dst_batch
        .column_by_name("id")
        .ok_or_else(|| OmniError::manifest("hydrated batch missing 'id' column".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest("hydrated 'id' column is not Utf8".to_string()))?;
    let mut id_to_row: HashMap<&str, u32> = HashMap::with_capacity(dst_batch_id_col.len());
    for row in 0..dst_batch_id_col.len() {
        id_to_row.insert(dst_batch_id_col.value(row), row as u32);
    }

    // Align pairs to (src_row, hydrated_dst_row), dropping ids hydration filtered out.
    let mut final_src_indices: Vec<u32> = Vec::with_capacity(src_indices.len());
    let mut dst_indices: Vec<u32> = Vec::with_capacity(src_indices.len());
    for (&src_idx, dst_id) in src_indices.iter().zip(dst_ids.iter()) {
        if let Some(&dst_row) = id_to_row.get(dst_id.as_str()) {
            final_src_indices.push(src_idx);
            dst_indices.push(dst_row);
        }
    }

    let src_take = UInt32Array::from(final_src_indices);
    let dst_take = UInt32Array::from(dst_indices);
    let expanded_wide = take_batch(wide, &src_take)?;
    let dst_prefixed = prefix_batch(&dst_batch, dst_var)?;
    let aligned_dst = take_batch(&dst_prefixed, &dst_take)?;
    *wide = hconcat_batches(&expanded_wide, &aligned_dst)?;

    for f in &non_pushable {
        apply_filter(wide, f, params)?;
    }
    Ok(())
}

/// CSR-backed graph traversal: BFS over the in-memory adjacency index. Used for
/// dense / whole-graph traversals; selective traversals use
/// `execute_expand_indexed`. Both share `expand_hydrate_and_align`.
async fn execute_expand_csr(
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
        .ok_or_else(|| {
            OmniError::manifest(format!("wide batch missing '{}' column", src_id_col_name))
        })?
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
    // Cross-type edges cannot chain; a variable-length traversal over one is
    // structurally single-hop (mirrors the indexed path's guarantee).
    let max = if same_type { max } else { max.min(1) };

    // BFS to collect (src_row_idx, dst_dense) pairs with per-source dedup.
    // Dense u32 ids stay in hand through BFS, dedup, and align — we only
    // stringify the unique set for Lance's SQL IN-list.
    let mut src_indices: Vec<u32> = Vec::new();
    let mut dst_dense_list: Vec<u32> = Vec::new();
    for i in 0..src_ids.len() {
        let src_id = src_ids.value(i);
        let Some(src_dense) = src_type_idx.to_dense(src_id) else {
            continue;
        };

        // BFS with hop tracking
        let mut frontier: Vec<u32> = vec![src_dense];
        let mut visited: HashSet<u32> = HashSet::new();
        let mut seen_dst_dense: HashSet<u32> = HashSet::new();
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
                        if hop >= min_hops && seen_dst_dense.insert(neighbor) {
                            src_indices.push(i as u32);
                            dst_dense_list.push(neighbor);
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

    // Map BFS-produced dense destination ids to string ids for the shared
    // hydrate+align tail. Dense ids always resolve (they came from the index);
    // drop any that don't, keeping the (src, dst) arrays parallel.
    let mut tail_src_indices: Vec<u32> = Vec::with_capacity(src_indices.len());
    let mut dst_ids: Vec<String> = Vec::with_capacity(dst_dense_list.len());
    for (&s, &d) in src_indices.iter().zip(dst_dense_list.iter()) {
        if let Some(id) = dst_type_idx.to_id(d) {
            tail_src_indices.push(s);
            dst_ids.push(id.to_string());
        }
    }

    expand_hydrate_and_align(
        wide,
        tail_src_indices,
        dst_ids,
        snapshot,
        catalog,
        dst_type,
        dst_var,
        dst_filters,
        params,
    )
    .await
}

/// Load full node rows for a set of IDs from a snapshot.
///
/// The `id IN (...)` predicate is built as a structured DataFusion `Expr` and
/// AND'd with any pushable `dst_filters` (destination-binding filters), then
/// applied via `Scanner::filter_expr`. The structured form routes the id
/// IN-list through the `id` BTREE scalar index (index-search → take) rather
/// than evaluating a string filter via DataFusion `InListEval`, which is
/// O(N×M) and was measured at 72× the indexed cost on a 100k-node hop
/// (MR-376). Non-pushable `dst_filters` (`ir_filter_to_expr` → None) are
/// applied in memory by the caller after hydration.
async fn hydrate_nodes(
    snapshot: &Snapshot,
    catalog: &Catalog,
    type_name: &str,
    ids: &[String],
    dst_filters: &[IRFilter],
    params: &ParamMap,
) -> Result<RecordBatch> {
    use datafusion::prelude::{col, lit};

    let node_type = catalog
        .node_types
        .get(type_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;

    if ids.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    }

    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open(&table_key).await?;

    // `id IN (ids)` AND any pushable destination filters, as a structured Expr.
    let id_list: Vec<datafusion::prelude::Expr> = ids.iter().map(|id| lit(id.clone())).collect();
    let mut filter_expr = col("id").in_list(id_list, false);
    if let Some(dst_expr) = build_lance_filter_expr(dst_filters, params, Some(&node_type.arrow_schema))
    {
        filter_expr = filter_expr.and(dst_expr);
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
    let batches = crate::table_store::TableStore::scan_stream_with(
        &ds,
        projection,
        None,
        None,
        false,
        |scanner| {
            scanner.filter_expr(filter_expr);
            Ok(())
        },
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

/// Whether the inner pipeline is the bulk-anti-join shape: a single Expand from
/// the outer var with no destination filters (the only shape the CSR
/// `has_neighbors` fast path can serve). Pure — it does not touch the CSR — so
/// the caller can decide whether to realize the O(|E|) graph index at all.
fn bulk_anti_join_applies(inner_pipeline: &[IROp], outer_var: &str) -> bool {
    matches!(
        inner_pipeline,
        [IROp::Expand { src_var, dst_filters, min_hops, max_hops, .. }]
            if src_var == outer_var
                && dst_filters.is_empty()
                // `has_neighbors` is a ONE-hop existence test, so the fast path
                // is valid only for a single-hop expand. Multi-hop negations
                // (e.g. `not { $p knows{2,2} $x }`) fall to the slow path, whose
                // inner Expand runs the real bounded traversal.
                && *min_hops == 1
                && (*max_hops).unwrap_or(1) == 1
    )
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
    if !bulk_anti_join_applies(inner_pipeline, outer_var) {
        return None;
    }
    let IROp::Expand {
        edge_type,
        direction,
        ..
    } = &inner_pipeline[0]
    else {
        return None;
    };
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
    graph_index: &GraphIndexHandle<'_>,
    catalog: &Catalog,
    outer_var: &str,
) -> Result<()> {
    // Only the bulk fast path consumes the CSR; the slow path's inner Expand
    // chooses its own access path. Realize the O(|E|) graph index ONLY when the
    // inner-pipeline shape qualifies for the bulk check — a filtered/nested
    // anti-join over a large graph must not pay a whole-graph build it won't use.
    let gi = if bulk_anti_join_applies(inner_pipeline, outer_var) {
        graph_index.get().await?
    } else {
        None
    };
    // Fast path: bulk CSR existence check (O(N), zero Lance I/O)
    if let Some(mask) = try_bulk_anti_join_mask(wide, inner_pipeline, gi, catalog, outer_var) {
        *wide = arrow_select::filter::filter_record_batch(wide, &mask)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        return Ok(());
    }

    // Slow path (filtered / non-bulk inner): run the inner pipeline ONCE over the
    // whole frontier — a set-oriented anti-semi-join — instead of row-by-row.
    // Each outer row is tagged with a synthetic index; an outer row matches iff
    // it produced at least one surviving inner row. No per-row dispatch, so the
    // inner Expand runs as a single set-at-a-time traversal over the full
    // frontier (its own chooser picks indexed vs CSR) rather than one Lance scan
    // per outer row.
    let num_rows = wide.num_rows();
    if num_rows == 0 {
        return Ok(());
    }

    // The tag rides through the inner pipeline: Expand's hconcat preserves
    // existing columns and Filter only drops rows, so each surviving row carries
    // its originating outer-row index. Correlating on the row index (not
    // `outer_var.id`) stays correct even if a dst-filter references other outer
    // bindings. Nested anti-joins reuse this slow path and an enclosing tag rides
    // through too; Arrow allows duplicate field names and `column_by_name`
    // returns the FIRST match, so choose a tag name not already present (each
    // nesting level then reads its own) instead of a fixed one.
    let tag_col: String = {
        let mut n = 0usize;
        loop {
            let candidate = format!("__antijoin_outer_row_{n}");
            if wide.schema().column_with_name(&candidate).is_none() {
                break candidate;
            }
            n += 1;
        }
    };
    let mut fields: Vec<Field> = wide
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(tag_col.as_str(), DataType::UInt32, false));
    let mut columns: Vec<ArrayRef> = wide.columns().to_vec();
    columns.push(Arc::new(UInt32Array::from_iter_values(0..num_rows as u32)));
    let tagged = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut inner_wide: Option<RecordBatch> = Some(tagged);
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

    // Outer rows whose tag survived have >= 1 match. A produced-but-untagged
    // batch means the inner pipeline dropped the correlation column — fail loudly
    // rather than silently keeping every row (which would corrupt the anti-join).
    let mut matched: HashSet<u32> = HashSet::new();
    if let Some(batch) = inner_wide {
        if batch.num_rows() > 0 {
            let tags = batch
                .column_by_name(tag_col.as_str())
                .ok_or_else(|| {
                    OmniError::manifest(
                        "anti-join inner pipeline dropped the correlation column".to_string(),
                    )
                })?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    OmniError::manifest(format!("'{}' column is not UInt32", tag_col))
                })?;
            for i in 0..tags.len() {
                matched.insert(tags.value(i));
            }
        }
    }

    let keep_mask: Vec<bool> = (0..num_rows as u32).map(|i| !matched.contains(&i)).collect();
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

    let node_type = &catalog.node_types[type_name];

    // Lower the IR filters to a DataFusion `Expr` and apply via
    // `Scanner::filter_expr` inside the configure closure. The string
    // pushdown path (`build_lance_filter` → `scanner.filter(&str)`) is
    // gone for node scans — structured Expr unlocks `CompOp::Contains`
    // pushdown (via `array_has`) and lets DF 53's optimizer rules
    // (vectorized IN-list, PhysicalExprSimplifier, CASE-NULL shortcut)
    // reach our predicates. Passing the node's `arrow_schema` lets the lowering
    // coerce literals to each column's exact type so narrow-numeric BTREEs are
    // used. Other call sites that still take string SQL (count_rows, the
    // mutation delete path) migrate in follow-up MRs.
    let filter_expr = build_lance_filter_expr(filters, params, Some(&node_type.arrow_schema));

    // Blob columns must be excluded from scan when a filter is present
    // (Lance bug: BlobsDescriptions + filter triggers a projection assertion).
    // We exclude blob columns and add metadata post-scan via take_blobs_by_indices.
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
        None,
        None,
        false,
        |scanner| {
            // Apply the structured IR filter via Lance's Expr pushdown.
            if let Some(ref expr) = filter_expr {
                scanner.filter_expr(expr.clone());
            }

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

pub(super) fn literal_to_sql(lit: &Literal) -> String {
    match lit {
        Literal::Null => "NULL".to_string(),
        Literal::String(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::Integer(n) => n.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Bool(b) => b.to_string(),
        Literal::Date(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::DateTime(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::List(_) => "NULL".to_string(), // Not supported in SQL pushdown
    }
}

// ---------------------------------------------------------------------------
// Structured DataFusion-Expr pushdown
//
// Parallel to the `ir_*_to_sql` family above, these helpers lower the same
// IR filter shapes to `datafusion::prelude::Expr` so we can call
// `Scanner::filter_expr(Expr)` instead of `Scanner::filter(&str)`. The
// structured form unlocks two things the string path could not express:
//
//   1. `CompOp::Contains` against list-typed columns (lowered to
//      `array_has(col, value)` — requires the `nested_expressions`
//      feature on the `datafusion` crate, enabled in the workspace).
//   2. Optimizer rules in DataFusion 53 that act on `Expr` shapes
//      (vectorized `IN`-list eq kernel, `PhysicalExprSimplifier`, the
//      `CASE WHEN x THEN y ELSE NULL` shortcut, etc.).
//
// Search predicates (`is_search_filter`) are still handled separately via
// `scanner.full_text_search(...)`, not via filter_expr — they stay None
// here (search predicates are never lowered to a scalar filter). The
// `literal_to_sql` path remains because the mutation/update layer
// (`exec/mutation.rs`) still produces SQL strings for `Dataset::delete(&str)`;
// that migration is MR-A's territory (Lance #6658 + delete two-phase).

/// Convert IR filters to a single DataFusion `Expr` (AND-joined), or
/// `None` if no filter is pushable.
pub(super) fn build_lance_filter_expr(
    filters: &[IRFilter],
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::logical_expr::Operator;
    use datafusion::prelude::Expr;

    let mut acc: Option<Expr> = None;
    for f in filters {
        let Some(e) = ir_filter_to_expr(f, params, schema) else {
            continue;
        };
        acc = Some(match acc {
            None => e,
            Some(prev) => Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                Box::new(prev),
                Operator::And,
                Box::new(e),
            )),
        });
    }
    acc
}

/// Convert a single IR filter to a DataFusion `Expr`. Returns `None` for
/// search-mode filters (handled via `scanner.full_text_search`) or any
/// expression shape we can't pushdown.
pub(super) fn ir_filter_to_expr(
    filter: &IRFilter,
    params: &ParamMap,
    schema: Option<&Schema>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::functions_nested::expr_fn::array_has;

    if is_search_filter(filter) {
        return None;
    }

    // List-contains: `prop CONTAINS value` lowers to `array_has(prop, value)`.
    // This is the case the old SQL-string pushdown had to return None for
    // ("Can't pushdown list contains"); with structured Expr it pushes down fine.
    // (Element-type coercion for the contained value is deferred — list columns
    // are not scalar-indexed, so the index-eligibility concern below does not apply.)
    if matches!(filter.op, CompOp::Contains) {
        let left = ir_expr_to_expr(&filter.left, params, None)?;
        let right = ir_expr_to_expr(&filter.right, params, None)?;
        return Some(array_has(left, right));
    }

    // A literal/param operand is coerced to the OTHER operand's column type so
    // the predicate stays a direct `col OP literal` and the scalar index is used.
    // Without this, DataFusion widens a narrow column (`CAST(col AS Int64)`),
    // which defeats the BTREE (validated by `probe_scalar_index_use_under_literal_type`).
    let left_col_type = prop_data_type(&filter.left, schema);
    let right_col_type = prop_data_type(&filter.right, schema);
    let left = ir_expr_to_expr(&filter.left, params, right_col_type.as_ref())?;
    let right = ir_expr_to_expr(&filter.right, params, left_col_type.as_ref())?;
    Some(match filter.op {
        CompOp::Eq => left.eq(right),
        CompOp::Ne => left.not_eq(right),
        CompOp::Gt => left.gt(right),
        CompOp::Lt => left.lt(right),
        CompOp::Ge => left.gt_eq(right),
        CompOp::Le => left.lt_eq(right),
        CompOp::Contains => unreachable!("handled above"),
    })
}

/// Convert an IR expression to a DataFusion `Expr`. Returns `None` for
/// shapes we don't support in pushdown (search funcs, RRF, aggregates,
/// variable refs that aren't a property access).
pub(super) fn ir_expr_to_expr(
    expr: &IRExpr,
    params: &ParamMap,
    target: Option<&arrow_schema::DataType>,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::col;
    match expr {
        IRExpr::PropAccess { property, .. } => Some(col(property)),
        IRExpr::Literal(l) => literal_to_expr_coerced(l, target),
        IRExpr::Param(name) => params
            .get(name)
            .and_then(|l| literal_to_expr_coerced(l, target)),
        _ => None,
    }
}

/// The Arrow type of a `PropAccess` operand, looked up in the scan's schema, or
/// `None` if the expr is not a column or the schema/field is unavailable.
fn prop_data_type(expr: &IRExpr, schema: Option<&Schema>) -> Option<arrow_schema::DataType> {
    match expr {
        IRExpr::PropAccess { property, .. } => schema?
            .field_with_name(property)
            .ok()
            .map(|f| f.data_type().clone()),
        _ => None,
    }
}

/// Lower a literal for pushdown, coercing it to `target` (the comparison
/// column's Arrow type) when known. Falls back to the natural-type
/// `literal_to_expr` on a missing target or any coercion failure, so a filter is
/// never demoted to `None` by coercion (a node scan has no in-memory fallback for
/// inline filters — see `execute_node_scan`).
fn literal_to_expr_coerced(
    lit: &Literal,
    target: Option<&arrow_schema::DataType>,
) -> Option<datafusion::prelude::Expr> {
    if let Some(target) = target {
        if let Some(e) = literal_to_typed_expr(lit, target) {
            return Some(e);
        }
    }
    literal_to_expr(lit)
}

/// Build a literal as a typed Arrow scalar matching `target`, reusing the same
/// `literal_to_array` + `arrow_cast` path as the in-memory arm
/// (`projection.rs::evaluate_filter`) so the two arms agree. Returns `None` on
/// any failure (unbuildable literal, incompatible cast) — the caller then falls
/// back to the natural-type literal.
///
/// Lossless-only for integer targets: typecheck permits numeric cross-type
/// comparisons (`types_compatible`), so a fractional float or out-of-range
/// integer can reach here. Casting those to a narrower integer would truncate
/// (`2.7 -> 2`) or overflow to null, silently changing which rows match. We
/// round-trip the cast and, on mismatch, return `None` so the caller keeps the
/// natural literal — correct via DataFusion coercion, the index just goes unused
/// for that out-of-domain predicate. Float targets are exempt: narrowing
/// `F64 -> F32` is the column's own precision domain, not a value error.
fn literal_to_typed_expr(
    lit: &Literal,
    target: &arrow_schema::DataType,
) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::lit as df_lit;
    use datafusion::scalar::ScalarValue;

    let arr = super::projection::literal_to_array(lit, 1).ok()?;
    if arr.data_type() == target {
        return Some(df_lit(ScalarValue::try_from_array(&arr, 0).ok()?));
    }
    let casted = arrow_cast::cast::cast(&arr, target).ok()?;
    if target.is_integer() {
        let back = arrow_cast::cast::cast(&casted, arr.data_type()).ok()?;
        let original = ScalarValue::try_from_array(&arr, 0).ok()?;
        let round_tripped = ScalarValue::try_from_array(&back, 0).ok()?;
        if original != round_tripped {
            return None;
        }
    }
    Some(df_lit(ScalarValue::try_from_array(&casted, 0).ok()?))
}

/// Convert a Literal to a DataFusion `Expr` in its NATURAL Arrow type. This is
/// the fallback used when the comparison column's type is unknown (no schema) or
/// when coercion to it fails; the typed, column-matched coercion that keeps
/// scalar indexes usable lives in `literal_to_typed_expr`. Returns `None` for
/// List (the SQL path also could not pushdown it — falls through to post-scan
/// in-memory application).
fn literal_to_expr(lit: &Literal) -> Option<datafusion::prelude::Expr> {
    use datafusion::prelude::lit as df_lit;
    Some(match lit {
        Literal::Null => df_lit(datafusion::scalar::ScalarValue::Null),
        Literal::String(s) => df_lit(s.clone()),
        Literal::Integer(n) => df_lit(*n),
        Literal::Float(f) => df_lit(*f),
        Literal::Bool(b) => df_lit(*b),
        // Date/DateTime pass through as strings here. Against a typed Date
        // column DataFusion casts the LITERAL (`CAST(Utf8 AS Date32)`), which is
        // index-safe (proven by `scalar_index_use_requires_matched_literal_type`).
        // At real pushdown sites the schema is known, so `literal_to_typed_expr`
        // produces a typed Date32/Date64 anyway; this branch is only the
        // no-schema fallback.
        Literal::Date(s) => df_lit(s.clone()),
        Literal::DateTime(s) => df_lit(s.clone()),
        Literal::List(_) => return None,
    })
}

fn prefix_batch(batch: &RecordBatch, variable: &str) -> Result<RecordBatch> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            Field::new(
                format!("{}.{}", variable, f.name()),
                f.data_type().clone(),
                f.is_nullable(),
            )
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|e| OmniError::Lance(e.to_string()))
}

fn cross_join_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let n = left.num_rows();
    let m = right.num_rows();
    if n == 0 || m == 0 {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
        return Ok(RecordBatch::new_empty(Arc::new(Schema::new(fields))));
    }
    let left_indices: Vec<u32> = (0..n as u32)
        .flat_map(|i| std::iter::repeat(i).take(m))
        .collect();
    let right_indices: Vec<u32> = (0..n).flat_map(|_| 0..m as u32).collect();
    let left_expanded = take_batch(left, &UInt32Array::from(left_indices))?;
    let right_expanded = take_batch(right, &UInt32Array::from(right_indices))?;
    hconcat_batches(&left_expanded, &right_expanded)
}

fn hconcat_batches(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    if cfg!(debug_assertions) {
        let left_schema = left.schema();
        let left_names: HashSet<&str> = left_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let right_schema = right.schema();
        for f in right_schema.fields() {
            debug_assert!(
                !left_names.contains(f.name().as_str()),
                "hconcat_batches: duplicate column '{}'",
                f.name()
            );
        }
    }
    fields.extend(right.schema().fields().iter().map(|f| f.as_ref().clone()));
    let mut columns: Vec<ArrayRef> = left.columns().to_vec();
    columns.extend(right.columns().to_vec());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow_select::take::take(col.as_ref(), indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}

#[cfg(test)]
mod expand_chooser_tests {
    use super::*;
    use crate::table_store::IndexCoverage;

    /// Build cost inputs with generous hard caps, so the cost comparison (not a
    /// ceiling) is what the assertions exercise unless a test sets one on purpose.
    fn inputs(
        frontier_rows: usize,
        edge_count: u64,
        src_node_count: u64,
        effective_max_hops: u32,
        coverage: IndexCoverage,
    ) -> ExpandCostInputs {
        ExpandCostInputs {
            frontier_rows,
            edge_count,
            src_node_count,
            effective_max_hops,
            max_hops_cap: 6,
            max_frontier_cap: 1024,
            coverage,
            csr_cached: false,
        }
    }

    #[test]
    fn selective_frontier_on_large_graph_picks_indexed() {
        // 50 source rows against 1M source vertices, one hop: tiny selectivity —
        // the PR #149 win the chooser must preserve.
        let m = choose_expand_mode(&inputs(50, 10_000_000, 1_000_000, 1, IndexCoverage::Indexed));
        assert_eq!(m, ExpandMode::IndexedScan);
    }

    #[test]
    fn flat_in_edge_count_same_selectivity_same_choice() {
        // Same selectivity (frontier/|V_src|), 1000× difference in |E|. Indexed
        // cost is independent of |E|, so the choice must not flip.
        let small = choose_expand_mode(&inputs(50, 100_000, 1_000_000, 1, IndexCoverage::Indexed));
        let huge =
            choose_expand_mode(&inputs(50, 100_000_000, 1_000_000, 1, IndexCoverage::Indexed));
        assert_eq!(small, ExpandMode::IndexedScan);
        assert_eq!(huge, ExpandMode::IndexedScan);
    }

    #[test]
    fn frontier_large_fraction_of_source_picks_csr() {
        // hops*frontier (200) exceeds BUILD_FACTOR*|V_src| (1.5*100=150) → CSR,
        // and 200 is below the frontier cap, so it is the cost model deciding.
        let m = choose_expand_mode(&inputs(200, 1_000, 100, 1, IndexCoverage::Indexed));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn frontier_over_hard_cap_picks_csr() {
        // 2000 > 1024 ceiling, even though the selectivity is tiny.
        let m = choose_expand_mode(&inputs(2000, 10_000_000, 1_000_000, 1, IndexCoverage::Indexed));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn hops_over_hard_cap_picks_csr() {
        let m = choose_expand_mode(&inputs(10, 10_000_000, 1_000_000, 8, IndexCoverage::Indexed));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn degraded_single_hop_tiny_frontier_stays_indexed() {
        // One full degraded scan (1*|E|) still edges out a full CSR build
        // (1.5*|E|) for a one-off single hop.
        let m = choose_expand_mode(&inputs(
            5,
            10_000,
            10_000,
            1,
            IndexCoverage::Degraded {
                reason: "no btree".into(),
            },
        ));
        assert_eq!(m, ExpandMode::IndexedScan);
    }

    #[test]
    fn degraded_multi_hop_picks_csr() {
        // Two degraded scans (2*|E|) lose to one CSR build (1.5*|E|).
        let m = choose_expand_mode(&inputs(
            5,
            10_000,
            10_000,
            2,
            IndexCoverage::Degraded {
                reason: "no btree".into(),
            },
        ));
        assert_eq!(m, ExpandMode::Csr);
    }

    #[test]
    fn warm_csr_is_always_reused() {
        // A maximally selective traversal still prefers an already-built CSR
        // (cost ~0) over re-scanning per hop.
        let mut i = inputs(1, 10_000_000, 1_000_000, 1, IndexCoverage::Indexed);
        i.csr_cached = true;
        assert_eq!(choose_expand_mode(&i), ExpandMode::Csr);
    }

    #[test]
    fn cost_model_caps_cross_type_hops() {
        // Same-type passes the requested range through; cross-type caps at 1,
        // matching execute_expand_indexed.
        assert_eq!(cost_effective_hops(5, true), 5);
        assert_eq!(cost_effective_hops(5, false), 1);
        assert_eq!(cost_effective_hops(1, false), 1);

        // Consequence: a selective frontier where the requested 5 hops would
        // (wrongly) flip cross-type to CSR, but the capped 1 hop — what actually
        // runs — keeps it indexed.
        let mut i = inputs(50, 10_000, 100, cost_effective_hops(5, false), IndexCoverage::Indexed);
        assert_eq!(choose_expand_mode(&i), ExpandMode::IndexedScan);
        i.effective_max_hops = 5; // as if the cross-type cap were not applied
        assert_eq!(choose_expand_mode(&i), ExpandMode::Csr);
    }
}

#[cfg(test)]
mod literal_lowering_tests {
    use super::*;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;

    // With the column type known, the generic coercion types a date literal to
    // the column's Date32/Date64 (the live pushdown path). Without a target it
    // is the natural Utf8 fallback, which is still index-safe for dates because
    // DataFusion casts the LITERAL, not the column (proven by
    // `lance_surface_guards::scalar_index_use_requires_matched_literal_type`).
    #[test]
    fn date_literals_coerce_to_typed_arrow_scalars() {
        use arrow_schema::DataType;
        let dt = literal_to_expr_coerced(
            &Literal::DateTime("2024-06-01T12:00:00Z".into()),
            Some(&DataType::Date64),
        )
        .unwrap();
        assert!(
            matches!(dt, Expr::Literal(ScalarValue::Date64(Some(_)), ..)),
            "DateTime vs Date64 column must coerce to a typed Date64, got {dt:?}"
        );
        let d = literal_to_expr_coerced(&Literal::Date("2024-06-01".into()), Some(&DataType::Date32))
            .unwrap();
        assert!(
            matches!(d, Expr::Literal(ScalarValue::Date32(Some(_)), ..)),
            "Date vs Date32 column must coerce to a typed Date32, got {d:?}"
        );
        let nat = literal_to_expr_coerced(&Literal::Date("2024-06-01".into()), None).unwrap();
        assert!(
            matches!(nat, Expr::Literal(ScalarValue::Utf8(Some(_)), ..)),
            "no target should keep the natural Utf8 date literal, got {nat:?}"
        );
    }

    // A malformed date string makes coercion fail, so it falls back to the
    // natural Utf8 literal rather than dropping the predicate to None.
    #[test]
    fn malformed_date_literal_falls_back_to_string() {
        use arrow_schema::DataType;
        let bad = literal_to_expr_coerced(
            &Literal::DateTime("not-a-date".into()),
            Some(&DataType::Date64),
        )
        .unwrap();
        assert!(
            matches!(bad, Expr::Literal(ScalarValue::Utf8(Some(_)), ..)),
            "malformed DateTime literal should fall back to a Utf8 literal, got {bad:?}"
        );
    }

    // With a column target, a literal lowers to the column's EXACT Arrow type
    // (not its natural width), so DataFusion does not widen and cast the column
    // — keeping the scalar BTREE usable. See
    // `lance_surface_guards::scalar_index_use_requires_matched_literal_type`.
    #[test]
    fn integer_literal_coerces_to_narrow_column_type() {
        use arrow_schema::DataType;
        let i32_lit = literal_to_expr_coerced(&Literal::Integer(5), Some(&DataType::Int32)).unwrap();
        assert!(
            matches!(i32_lit, Expr::Literal(ScalarValue::Int32(Some(5)), ..)),
            "integer literal vs Int32 column must lower to Int32, got {i32_lit:?}"
        );
        let u32_lit = literal_to_expr_coerced(&Literal::Integer(7), Some(&DataType::UInt32)).unwrap();
        assert!(
            matches!(u32_lit, Expr::Literal(ScalarValue::UInt32(Some(7)), ..)),
            "integer literal vs UInt32 column must lower to UInt32, got {u32_lit:?}"
        );
    }

    #[test]
    fn float_literal_coerces_to_f32_column_type() {
        use arrow_schema::DataType;
        let f32_lit =
            literal_to_expr_coerced(&Literal::Float(1.5), Some(&DataType::Float32)).unwrap();
        assert!(
            matches!(f32_lit, Expr::Literal(ScalarValue::Float32(Some(_)), ..)),
            "float literal vs Float32 column must lower to Float32, got {f32_lit:?}"
        );
    }

    // Lossless guard: a fractional float against an integer column must NOT
    // truncate (2.7 -> 2). Fall back to the natural Float64 so the comparison
    // stays exact (no integer equals 2.7).
    #[test]
    fn fractional_float_vs_int_column_falls_back_not_truncate() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Float(2.7), Some(&DataType::Int32)).unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Float64(Some(_)), ..)),
            "fractional float vs Int32 must fall back to natural Float64, got {e:?}"
        );
    }

    // A whole-number float IS lossless against an integer column, so it coerces.
    #[test]
    fn whole_float_vs_int_column_coerces() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Float(2.0), Some(&DataType::Int32)).unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Int32(Some(2)), ..)),
            "whole-number float vs Int32 is lossless and must coerce to Int32(2), got {e:?}"
        );
    }

    // Lossless guard: an integer literal outside the column's range must NOT
    // overflow to null; fall back to the natural Int64 (correct via DataFusion).
    #[test]
    fn out_of_range_int_vs_narrow_column_falls_back() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Integer(3_000_000_000), Some(&DataType::Int32))
            .unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Int64(Some(3_000_000_000)), ..)),
            "out-of-range integer vs Int32 must fall back to natural Int64, got {e:?}"
        );
    }

    // Float targets are exempt from the lossless guard: narrowing to the column's
    // own precision is the correct comparison domain, even when the value is not
    // exactly representable in F32 (0.1).
    #[test]
    fn float_vs_f32_column_coerces_even_when_not_exactly_representable() {
        use arrow_schema::DataType;
        let e = literal_to_expr_coerced(&Literal::Float(0.1), Some(&DataType::Float32)).unwrap();
        assert!(
            matches!(e, Expr::Literal(ScalarValue::Float32(Some(_)), ..)),
            "float target must coerce 0.1 to Float32 (exempt from lossless guard), got {e:?}"
        );
    }

    // No target (caller without a schema) keeps the natural width — the existing
    // fallback, so behavior never regresses where the column type is unknown.
    #[test]
    fn literal_without_target_keeps_natural_width() {
        let nat = literal_to_expr_coerced(&Literal::Integer(5), None).unwrap();
        assert!(
            matches!(nat, Expr::Literal(ScalarValue::Int64(Some(5)), ..)),
            "no target should keep the natural Int64 width, got {nat:?}"
        );
    }

    // True if either operand of a binary comparison is an Int32 literal.
    fn binary_has_int32_literal(e: &Expr) -> bool {
        if let Expr::BinaryExpr(b) = e {
            [b.left.as_ref(), b.right.as_ref()]
                .iter()
                .any(|side| matches!(side, Expr::Literal(ScalarValue::Int32(Some(_)), ..)))
        } else {
            false
        }
    }

    fn int32_schema() -> arrow_schema::Schema {
        use arrow_schema::{DataType, Field};
        arrow_schema::Schema::new(vec![Field::new("count", DataType::Int32, true)])
    }

    fn count_prop() -> IRExpr {
        IRExpr::PropAccess {
            variable: "m".into(),
            property: "count".into(),
        }
    }

    // Coercion is operator-independent: a range comparison's literal coerces to
    // the column type just like equality does, so range filters on a narrow
    // numeric column keep the BTREE.
    #[test]
    fn ir_filter_coerces_literal_for_range_op() {
        let schema = int32_schema();
        let filter = IRFilter {
            left: count_prop(),
            op: CompOp::Ge,
            right: IRExpr::Literal(Literal::Integer(2)),
        };
        let expr = ir_filter_to_expr(&filter, &ParamMap::new(), Some(&schema)).unwrap();
        assert!(
            binary_has_int32_literal(&expr),
            "range-op literal must coerce to the Int32 column type, got {expr:?}"
        );
    }

    // The column may be on either side; the literal coerces to the opposite
    // operand's column type regardless of order (`5 < count`).
    #[test]
    fn ir_filter_coerces_literal_when_column_is_on_the_right() {
        let schema = int32_schema();
        let filter = IRFilter {
            left: IRExpr::Literal(Literal::Integer(2)),
            op: CompOp::Lt,
            right: count_prop(),
        };
        let expr = ir_filter_to_expr(&filter, &ParamMap::new(), Some(&schema)).unwrap();
        assert!(
            binary_has_int32_literal(&expr),
            "reversed-operand literal must coerce to the Int32 column type, got {expr:?}"
        );
    }
}
