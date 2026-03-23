use std::collections::HashMap;
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

use crate::db::Omnigraph;
use crate::db::Snapshot;
use crate::error::{OmniError, Result};

impl Omnigraph {
    /// Run a named query from a .gq query source string.
    pub async fn run_query(
        &self,
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

        // Execute as pure function
        execute_query(&ir, params, &snapshot, self.catalog()).await
    }
}

/// Execute a lowered QueryIR. Pure function — no state, no caches.
pub async fn execute_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    catalog: &Catalog,
) -> Result<QueryResult> {
    // Walk the pipeline, building up variable bindings
    let mut bindings: HashMap<String, RecordBatch> = HashMap::new();

    for op in &ir.pipeline {
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
                apply_filter(&mut bindings, filter, params)?;
            }
            IROp::Expand { .. } => {
                return Err(OmniError::Manifest(
                    "graph traversal not yet implemented (Step 6)".to_string(),
                ));
            }
            IROp::AntiJoin { .. } => {
                return Err(OmniError::Manifest(
                    "anti-join not yet implemented (Step 6)".to_string(),
                ));
            }
        }
    }

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
