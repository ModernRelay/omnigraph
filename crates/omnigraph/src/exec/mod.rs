use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, IRFilter, IROrdering, IRProjection, IROp, ParamMap, QueryIR};
use omnigraph_compiler::lower_query;
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_compiler::query::typecheck::typecheck_query;
use omnigraph_compiler::result::QueryResult;
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

/// Execute a lowered QueryIR. Pure function — no state, no caches.
pub async fn execute_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: Option<&GraphIndex>,
    catalog: &Catalog,
) -> Result<QueryResult> {
    let mut bindings: HashMap<String, RecordBatch> = HashMap::new();

    execute_pipeline(&ir.pipeline, params, snapshot, graph_index, catalog, &mut bindings).await?;

    // Project return expressions
    let mut result_batch = project_return(&bindings, &ir.return_exprs, params)?;

    // Apply ordering
    if !ir.order_by.is_empty() {
        result_batch = apply_ordering(result_batch, &ir.order_by, &bindings, params)?;
    }

    // Apply limit
    if let Some(limit) = ir.limit {
        let len = result_batch.num_rows().min(limit as usize);
        result_batch = result_batch.slice(0, len);
    }

    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}

/// Execute a pipeline of IR operations, populating variable bindings.
fn execute_pipeline<'a>(
    pipeline: &'a [IROp],
    params: &'a ParamMap,
    snapshot: &'a Snapshot,
    graph_index: Option<&'a GraphIndex>,
    catalog: &'a Catalog,
    bindings: &'a mut HashMap<String, RecordBatch>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>> {
    Box::pin(async move {
    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                let batch =
                    execute_node_scan(type_name, filters, params, snapshot, catalog).await?;
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

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    let schema = batches[0].schema();
    arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|e| OmniError::Lance(e.to_string()))
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

        execute_pipeline(
            inner_pipeline,
            params,
            snapshot,
            graph_index,
            catalog,
            &mut inner_bindings,
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

/// Scan a node type's Lance dataset with optional filter pushdown.
async fn execute_node_scan(
    type_name: &str,
    filters: &[IRFilter],
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
) -> Result<RecordBatch> {
    let table_key = format!("node:{}", type_name);
    let ds = snapshot.open(&table_key).await?;

    // Build Lance SQL filter string from IR filters
    let filter_sql = build_lance_filter(filters, params);

    let mut scanner = ds.scan();
    if let Some(sql) = &filter_sql {
        scanner
            .filter(sql.as_str())
            .map_err(|e| OmniError::Lance(format!("filter '{}': {}", sql, e)))?;
    }

    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    if batches.is_empty() {
        let node_type = &catalog.node_types[type_name];
        return Ok(RecordBatch::new_empty(node_type.arrow_schema.clone()));
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    let schema = batches[0].schema();
    arrow_select::concat::concat_batches(&schema, &batches)
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
