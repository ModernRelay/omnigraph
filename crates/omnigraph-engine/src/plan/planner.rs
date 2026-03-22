use std::collections::HashSet;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_functions_aggregate::{
    average::avg_udaf,
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::{Literal as PhysicalLiteral, binary, col, try_cast};
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    filter::FilterExec,
    limit::GlobalLimitExec,
    projection::{ProjectionExec, ProjectionExpr},
    sorts::sort::SortExec,
};
use lance::Dataset;
use lance_index::DatasetIndexExt;

use crate::embedding::EmbeddingClient;
use crate::error::{NanoError, Result};
use crate::ir::*;
use crate::plan::bindings::{
    binding_data_type, binding_property_array, binding_struct_array, flat_binding_col,
};
use crate::query::ast::{AggFunc, CompOp, Literal, NOW_PARAM_NAME};
use crate::store::lance_io::logical_node_field_to_lance;
use crate::store::loader::{parse_date32_literal, parse_date64_literal};
use crate::store::runtime::DatabaseRuntime;
use crate::types::ScalarType;
use tracing::{debug, info, instrument};

use super::literal_utils;
use super::node_scan::{NodeScanExec, NodeScanPredicate};
use super::physical::{ExpandExec, ExpandSpec};

type ScanProjectionMap = AHashMap<String, Option<AHashSet<String>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailStrategy {
    Legacy,
    DataFusionNonAggregate {
        alias_ordering: bool,
        final_variable_projection: bool,
    },
    DataFusionAggregate,
}

struct TailPlan {
    plan: Arc<dyn ExecutionPlan>,
    final_manual_projection: bool,
}

fn analyze_tail_strategy(ir: &QueryIR) -> TailStrategy {
    let has_aggregation = ir
        .return_exprs
        .iter()
        .any(|projection| matches!(projection.expr, IRExpr::Aggregate { .. }));
    let has_alias_ordering = ir
        .order_by
        .iter()
        .any(|ordering| matches!(ordering.expr, IRExpr::AliasRef(_)));
    let has_variable_projection = ir
        .return_exprs
        .iter()
        .any(|projection| matches!(projection.expr, IRExpr::Variable(_)));

    if pipeline_contains_unsupported_tail_filters(&ir.pipeline) {
        return TailStrategy::Legacy;
    }

    if !remaining_runtime_filters(&ir.pipeline, &ParamMap::new())
        .iter()
        .all(is_supported_tail_filter)
    {
        return TailStrategy::Legacy;
    }

    if has_aggregation {
        if !ir
            .return_exprs
            .iter()
            .all(is_supported_aggregate_projection)
        {
            return TailStrategy::Legacy;
        }
        if !ir
            .order_by
            .iter()
            .all(|ordering| matches!(ordering.expr, IRExpr::AliasRef(_)))
        {
            return TailStrategy::Legacy;
        }
        return TailStrategy::DataFusionAggregate;
    }

    if !ir
        .return_exprs
        .iter()
        .all(is_supported_non_aggregate_projection)
    {
        return TailStrategy::Legacy;
    }

    if has_alias_ordering && has_variable_projection {
        return TailStrategy::Legacy;
    }

    if has_alias_ordering
        && !ir
            .order_by
            .iter()
            .all(|ordering| matches!(ordering.expr, IRExpr::AliasRef(_)))
    {
        return TailStrategy::Legacy;
    }

    if !ir
        .order_by
        .iter()
        .all(|ordering| is_supported_non_aggregate_tail_expr(&ordering.expr))
    {
        return TailStrategy::Legacy;
    }

    TailStrategy::DataFusionNonAggregate {
        alias_ordering: has_alias_ordering,
        final_variable_projection: has_variable_projection,
    }
}

fn pipeline_contains_unsupported_tail_filters(pipeline: &[IROp]) -> bool {
    pipeline.iter().any(|op| match op {
        IROp::NodeScan { filters, .. } => {
            filters.iter().any(|filter| filter.op == CompOp::Contains)
        }
        IROp::Filter(filter) => filter.op == CompOp::Contains,
        IROp::AntiJoin { inner, .. } => pipeline_contains_unsupported_tail_filters(inner),
        IROp::Expand { .. } => false,
    })
}

fn is_supported_tail_filter(filter: &IRFilter) -> bool {
    filter.op != CompOp::Contains
        && is_supported_non_aggregate_tail_expr(&filter.left)
        && is_supported_non_aggregate_tail_expr(&filter.right)
}

fn is_supported_non_aggregate_projection(projection: &IRProjection) -> bool {
    matches!(projection.expr, IRExpr::Variable(_))
        || is_supported_non_aggregate_tail_expr(&projection.expr)
}

fn is_supported_aggregate_projection(projection: &IRProjection) -> bool {
    match &projection.expr {
        IRExpr::Aggregate { func, arg } => {
            matches!(
                func,
                AggFunc::Count | AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max
            ) && is_supported_aggregate_arg(arg)
        }
        expr => is_supported_group_expr(expr),
    }
}

fn is_supported_group_expr(expr: &IRExpr) -> bool {
    matches!(
        expr,
        IRExpr::PropAccess { .. } | IRExpr::Literal(_) | IRExpr::Param(_)
    )
}

fn is_supported_aggregate_arg(expr: &IRExpr) -> bool {
    matches!(
        expr,
        IRExpr::PropAccess { .. } | IRExpr::Literal(_) | IRExpr::Param(_) | IRExpr::Variable(_)
    )
}

fn is_supported_non_aggregate_tail_expr(expr: &IRExpr) -> bool {
    matches!(
        expr,
        IRExpr::PropAccess { .. } | IRExpr::Literal(_) | IRExpr::Param(_) | IRExpr::AliasRef(_)
    )
}

fn projection_output_name(projection: &IRProjection) -> String {
    projection
        .alias
        .clone()
        .unwrap_or_else(|| default_expr_name(&projection.expr))
}

fn default_expr_name(expr: &IRExpr) -> String {
    match expr {
        IRExpr::PropAccess { property, .. } => property.clone(),
        IRExpr::Variable(name) => name.clone(),
        IRExpr::Param(name) => {
            if name == NOW_PARAM_NAME {
                "now".to_string()
            } else {
                name.clone()
            }
        }
        IRExpr::Literal(_) => "literal".to_string(),
        IRExpr::AliasRef(name) => name.clone(),
        IRExpr::Aggregate { func, .. } => func.to_string(),
        IRExpr::Nearest { .. } => "nearest".to_string(),
        IRExpr::Search { .. } => "search".to_string(),
        IRExpr::Fuzzy { .. } => "fuzzy".to_string(),
        IRExpr::MatchText { .. } => "match_text".to_string(),
        IRExpr::Bm25 { .. } => "bm25".to_string(),
        IRExpr::Rrf { .. } => "rrf".to_string(),
    }
}

fn remaining_runtime_filters(pipeline: &[IROp], params: &ParamMap) -> Vec<IRFilter> {
    let scan_variables = pipeline_scan_variables(pipeline);
    let mut filters = Vec::new();

    for op in pipeline {
        match op {
            IROp::Filter(filter) => {
                if !is_explicit_filter_pushed_down(&scan_variables, filter, params) {
                    filters.push(filter.clone());
                }
            }
            IROp::NodeScan {
                variable,
                filters: scan_filters,
                ..
            } => {
                for filter in scan_filters {
                    if pushdown_scan_filter(variable, filter, params).is_none() {
                        filters.push(filter.clone());
                    }
                }
            }
            _ => {}
        }
    }

    filters
}

fn literal_to_scalar_value(
    literal: &Literal,
    target_type: Option<&DataType>,
) -> Result<ScalarValue> {
    let scalar = match (literal, target_type) {
        (Literal::String(value), _) => ScalarValue::Utf8(Some(value.clone())),
        (Literal::Integer(value), Some(DataType::Int32)) => ScalarValue::Int32(Some(*value as i32)),
        (Literal::Integer(value), Some(DataType::Int64)) => ScalarValue::Int64(Some(*value)),
        (Literal::Integer(value), Some(DataType::UInt32)) => {
            ScalarValue::UInt32(Some(*value as u32))
        }
        (Literal::Integer(value), Some(DataType::UInt64)) => {
            ScalarValue::UInt64(Some(*value as u64))
        }
        (Literal::Integer(value), Some(DataType::Float32)) => {
            ScalarValue::Float32(Some(*value as f32))
        }
        (Literal::Integer(value), Some(DataType::Float64)) => {
            ScalarValue::Float64(Some(*value as f64))
        }
        (Literal::Integer(value), _) => ScalarValue::Int64(Some(*value)),
        (Literal::Float(value), Some(DataType::Float32)) => {
            ScalarValue::Float32(Some(*value as f32))
        }
        (Literal::Float(value), Some(DataType::Float64)) => ScalarValue::Float64(Some(*value)),
        (Literal::Float(value), Some(DataType::Int32)) => ScalarValue::Int32(Some(*value as i32)),
        (Literal::Float(value), Some(DataType::Int64)) => ScalarValue::Int64(Some(*value as i64)),
        (Literal::Float(value), _) => ScalarValue::Float64(Some(*value)),
        (Literal::Bool(value), _) => ScalarValue::Boolean(Some(*value)),
        (Literal::Date(value), _) => ScalarValue::Date32(Some(parse_date32_literal(value)?)),
        (Literal::DateTime(value), _) => ScalarValue::Date64(Some(parse_date64_literal(value)?)),
        (Literal::List(_), _) => {
            return Err(NanoError::Execution(
                "list literals are not supported in the DataFusion tail".to_string(),
            ));
        }
    };
    Ok(scalar)
}

fn lower_tail_value_expr(
    expr: &IRExpr,
    input_schema: &SchemaRef,
    params: &ParamMap,
) -> Result<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let name = flat_binding_col(variable, property);
            col(&name, input_schema.as_ref()).map_err(|e| NanoError::Execution(e.to_string()))
        }
        IRExpr::Literal(literal) => Ok(Arc::new(PhysicalLiteral::new(literal_to_scalar_value(
            literal, None,
        )?))),
        IRExpr::Param(name) => {
            let literal = params
                .get(name)
                .ok_or_else(|| NanoError::Execution(format!("parameter ${} not provided", name)))?;
            Ok(Arc::new(PhysicalLiteral::new(literal_to_scalar_value(
                literal, None,
            )?)))
        }
        IRExpr::AliasRef(name) => {
            col(name, input_schema.as_ref()).map_err(|e| NanoError::Execution(e.to_string()))
        }
        _ => Err(NanoError::Execution(
            "expression is not supported in the DataFusion tail".to_string(),
        )),
    }
}

fn lower_tail_filter_expr(
    filter: &IRFilter,
    input_schema: &SchemaRef,
    params: &ParamMap,
) -> Result<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
    let left = lower_tail_value_expr(&filter.left, input_schema, params)?;
    let mut right = lower_tail_value_expr(&filter.right, input_schema, params)?;
    if filter.op != CompOp::Contains {
        let left_type = left
            .data_type(input_schema.as_ref())
            .map_err(|e| NanoError::Execution(e.to_string()))?;
        let right_type = right
            .data_type(input_schema.as_ref())
            .map_err(|e| NanoError::Execution(e.to_string()))?;
        if left_type != right_type {
            right = try_cast(right, input_schema.as_ref(), left_type)
                .map_err(|e| NanoError::Execution(e.to_string()))?;
        }
    }

    let op = match filter.op {
        CompOp::Eq => Operator::Eq,
        CompOp::Ne => Operator::NotEq,
        CompOp::Gt => Operator::Gt,
        CompOp::Lt => Operator::Lt,
        CompOp::Ge => Operator::GtEq,
        CompOp::Le => Operator::LtEq,
        CompOp::Contains => {
            return Err(NanoError::Execution(
                "contains is not supported in the DataFusion tail".to_string(),
            ));
        }
    };
    binary(left, op, right, input_schema.as_ref()).map_err(|e| NanoError::Execution(e.to_string()))
}

fn lower_projection_expr(
    projection: &IRProjection,
    input_schema: &SchemaRef,
    params: &ParamMap,
) -> Result<ProjectionExpr> {
    Ok(ProjectionExpr {
        expr: lower_tail_value_expr(&projection.expr, input_schema, params)?,
        alias: projection_output_name(projection),
    })
}

fn lower_sort_exprs(
    order_by: &[IROrdering],
    input_schema: &SchemaRef,
    params: &ParamMap,
) -> Result<LexOrdering> {
    let sort_exprs = order_by
        .iter()
        .map(|ordering| {
            let expr = lower_tail_value_expr(&ordering.expr, input_schema, params)?;
            Ok(PhysicalSortExpr::new(
                expr,
                arrow_ord::sort::SortOptions {
                    descending: ordering.descending,
                    nulls_first: false,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    LexOrdering::new(sort_exprs).ok_or_else(|| {
        NanoError::Execution("expected at least one ordering expression".to_string())
    })
}

fn lower_aggregate_arg_expr(
    expr: &IRExpr,
    input_schema: &SchemaRef,
    params: &ParamMap,
    func: &AggFunc,
) -> Result<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
    match expr {
        IRExpr::Variable(variable) if matches!(func, AggFunc::Count) => {
            col(&flat_binding_col(variable, "id"), input_schema.as_ref())
                .map_err(|e| NanoError::Execution(e.to_string()))
        }
        _ => lower_tail_value_expr(expr, input_schema, params),
    }
}

fn build_datafusion_tail_plan(
    ir: &QueryIR,
    input_plan: Arc<dyn ExecutionPlan>,
    params: &ParamMap,
) -> Result<Option<TailPlan>> {
    match analyze_tail_strategy(ir) {
        TailStrategy::Legacy => Ok(None),
        TailStrategy::DataFusionNonAggregate {
            alias_ordering,
            final_variable_projection,
        } => {
            let mut plan = input_plan;
            for filter in remaining_runtime_filters(&ir.pipeline, params) {
                let predicate = lower_tail_filter_expr(&filter, &plan.schema(), params)?;
                plan = Arc::new(
                    FilterExec::try_new(predicate, plan)
                        .map_err(|e| NanoError::Execution(e.to_string()))?,
                );
            }

            if alias_ordering {
                let projection_exprs = ir
                    .return_exprs
                    .iter()
                    .map(|projection| lower_projection_expr(projection, &plan.schema(), params))
                    .collect::<Result<Vec<_>>>()?;
                plan = Arc::new(
                    ProjectionExec::try_new(projection_exprs, plan)
                        .map_err(|e| NanoError::Execution(e.to_string()))?,
                );

                if !ir.order_by.is_empty() {
                    let sort_exprs = lower_sort_exprs(&ir.order_by, &plan.schema(), params)?;
                    let sort = SortExec::new(sort_exprs, plan)
                        .with_fetch(ir.limit.map(|limit| limit as usize));
                    plan = Arc::new(sort);
                }
                if let Some(limit) = ir.limit {
                    plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit as usize)));
                }
            } else {
                if !ir.order_by.is_empty() {
                    let sort_exprs = lower_sort_exprs(&ir.order_by, &plan.schema(), params)?;
                    let sort = SortExec::new(sort_exprs, plan)
                        .with_fetch(ir.limit.map(|limit| limit as usize));
                    plan = Arc::new(sort);
                }
                if let Some(limit) = ir.limit {
                    plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit as usize)));
                }

                if !final_variable_projection {
                    let projection_exprs = ir
                        .return_exprs
                        .iter()
                        .map(|projection| lower_projection_expr(projection, &plan.schema(), params))
                        .collect::<Result<Vec<_>>>()?;
                    plan = Arc::new(
                        ProjectionExec::try_new(projection_exprs, plan)
                            .map_err(|e| NanoError::Execution(e.to_string()))?,
                    );
                }
            }

            Ok(Some(TailPlan {
                plan,
                final_manual_projection: final_variable_projection,
            }))
        }
        TailStrategy::DataFusionAggregate => {
            let mut plan = input_plan;
            for filter in remaining_runtime_filters(&ir.pipeline, params) {
                let predicate = lower_tail_filter_expr(&filter, &plan.schema(), params)?;
                plan = Arc::new(
                    FilterExec::try_new(predicate, plan)
                        .map_err(|e| NanoError::Execution(e.to_string()))?,
                );
            }

            let input_schema = plan.schema();
            let mut group_exprs = Vec::new();
            let mut aggr_exprs = Vec::new();

            for projection in &ir.return_exprs {
                match &projection.expr {
                    IRExpr::Aggregate { func, arg } => {
                        let arg_expr = lower_aggregate_arg_expr(arg, &input_schema, params, func)?;
                        let udaf = match func {
                            AggFunc::Count => count_udaf(),
                            AggFunc::Sum => sum_udaf(),
                            AggFunc::Avg => avg_udaf(),
                            AggFunc::Min => min_udaf(),
                            AggFunc::Max => max_udaf(),
                        };
                        let aggregate_expr = AggregateExprBuilder::new(udaf, vec![arg_expr])
                            .alias(projection_output_name(projection))
                            .schema(input_schema.clone())
                            .build()
                            .map_err(|e| NanoError::Execution(e.to_string()))?;
                        aggr_exprs.push(Arc::new(aggregate_expr));
                    }
                    expr => {
                        group_exprs.push((
                            lower_tail_value_expr(expr, &input_schema, params)?,
                            projection_output_name(projection),
                        ));
                    }
                }
            }

            let group_by = if group_exprs.is_empty() {
                PhysicalGroupBy::new(vec![], vec![], vec![], false)
            } else {
                PhysicalGroupBy::new_single(group_exprs)
            };
            let filter_expr = vec![None; aggr_exprs.len()];
            plan = Arc::new(
                AggregateExec::try_new(
                    AggregateMode::Single,
                    group_by,
                    aggr_exprs,
                    filter_expr,
                    plan,
                    input_schema,
                )
                .map_err(|e| NanoError::Execution(e.to_string()))?,
            );

            if !ir.order_by.is_empty() {
                let sort_exprs = lower_sort_exprs(&ir.order_by, &plan.schema(), params)?;
                let sort = SortExec::new(sort_exprs, plan)
                    .with_fetch(ir.limit.map(|limit| limit as usize));
                plan = Arc::new(sort);
            }
            if let Some(limit) = ir.limit {
                plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit as usize)));
            }

            Ok(Some(TailPlan {
                plan,
                final_manual_projection: false,
            }))
        }
    }
}

async fn collect_plan_batches(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = plan
        .execute(0, task_ctx)
        .map_err(|e| NanoError::Execution(e.to_string()))?;
    let mut batches = Vec::new();
    use futures::StreamExt;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| NanoError::Execution(e.to_string()))?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

#[instrument(skip(ir, runtime, params), fields(query = %ir.name, pipeline_len = ir.pipeline.len()))]
pub(crate) async fn execute_query_with_runtime(
    ir: &QueryIR,
    runtime: Arc<DatabaseRuntime>,
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    let resolved_ir = resolve_nearest_query_embeddings(ir, runtime.as_ref(), params).await?;
    info!("executing query");
    let has_aggregation = resolved_ir
        .return_exprs
        .iter()
        .any(|p| matches!(&p.expr, IRExpr::Aggregate { .. }));

    if !has_aggregation
        && let Some(candidate) = analyze_single_node_nearest_ann_candidate(&resolved_ir, params)
        && let Some(result) = try_execute_single_node_nearest_ann_fast_path(
            &resolved_ir,
            runtime.clone(),
            params,
            &candidate,
        )
        .await?
    {
        info!(
            result_batches = result.len(),
            node_type = %candidate.type_name,
            property = %candidate.property,
            "query execution complete (nearest ANN fast path)"
        );
        return Ok(result);
    }

    let single_scan_pushdown = analyze_single_scan_pushdown(&resolved_ir.pipeline, params);
    let scan_limit_pushdown = if !has_aggregation && resolved_ir.order_by.is_empty() {
        resolved_ir.limit.and_then(|v| {
            single_scan_pushdown
                .as_ref()
                .filter(|info| info.all_filters_pushdown)
                .map(|_| v as usize)
        })
    } else {
        None
    };
    let scan_projections = analyze_scan_projection_requirements(&resolved_ir);
    // Build the physical plan from IR
    let plan = build_physical_plan(
        &resolved_ir.pipeline,
        runtime.clone(),
        params,
        scan_limit_pushdown,
        &scan_projections,
    )?;

    if let Some(tail_plan) = build_datafusion_tail_plan(&resolved_ir, plan.clone(), params)? {
        let mut batches = collect_plan_batches(tail_plan.plan).await?;
        if tail_plan.final_manual_projection {
            batches = apply_projection(&resolved_ir.return_exprs, &batches, params)?;
        }
        info!(result_batches = batches.len(), "query execution complete");
        return Ok(batches);
    }

    // Execute the plan to get intermediate results
    let task_ctx = Arc::new(TaskContext::default());
    let stream = plan
        .execute(0, task_ctx)
        .map_err(|e| NanoError::Execution(e.to_string()))?;

    let can_stream_batchwise = !has_aggregation && ir.order_by.is_empty() && ir.limit.is_none();
    debug!(
        has_aggregation,
        has_order = !ir.order_by.is_empty(),
        has_limit = ir.limit.is_some(),
        fast_path = can_stream_batchwise,
        "query execution mode selected"
    );

    if can_stream_batchwise {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| NanoError::Execution(e.to_string()))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let filtered = apply_ir_filters(&resolved_ir.pipeline, &[batch], params)?;
            if filtered.is_empty() {
                continue;
            }
            let projected = apply_projection(&resolved_ir.return_exprs, &filtered, params)?;
            out.extend(projected.into_iter().filter(|b| b.num_rows() > 0));
        }
        info!(result_batches = out.len(), "query execution complete");
        return Ok(out);
    }

    let batches: Vec<RecordBatch> = {
        use futures::StreamExt;
        let mut batches = Vec::new();
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let b = batch.map_err(|e| NanoError::Execution(e.to_string()))?;
            batches.push(b);
        }
        batches
    };

    if batches.is_empty() || (batches.len() == 1 && batches[0].num_rows() == 0) {
        return Ok(vec![]);
    }

    // Apply filters from the pipeline that reference struct fields
    let filtered = apply_ir_filters(&resolved_ir.pipeline, &batches, params)?;

    // Apply return projections
    if has_aggregation {
        let result = apply_aggregation(&resolved_ir.return_exprs, &filtered, params)?;
        let result =
            apply_order_and_limit(&result, &resolved_ir.order_by, resolved_ir.limit, params)?;
        info!(result_batches = result.len(), "query execution complete");
        Ok(result)
    } else {
        let has_nearest = resolved_ir
            .order_by
            .iter()
            .any(|o| matches!(o.expr, IRExpr::Nearest { .. }));
        let has_alias_order = resolved_ir
            .order_by
            .iter()
            .any(|o| matches!(o.expr, IRExpr::AliasRef(_)));

        let result = if has_alias_order && !has_nearest {
            // Alias-based ordering needs globally comparable alias values. For rank-producing
            // aliases (bm25/rrf), projecting per input batch would compute scores on different
            // local corpora/rankings and produce unstable global ordering.
            let projected =
                apply_projection_for_alias_ordering(&resolved_ir.return_exprs, &filtered, params)?;
            apply_order_and_limit(&projected, &resolved_ir.order_by, resolved_ir.limit, params)?
        } else {
            let ordered =
                apply_order_and_limit(&filtered, &resolved_ir.order_by, resolved_ir.limit, params)?;
            apply_projection(&resolved_ir.return_exprs, &ordered, params)?
        };
        info!(result_batches = result.len(), "query execution complete");
        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct SingleNodeNearestAnnCandidate {
    variable: String,
    type_name: String,
    property: String,
    query: IRExpr,
    filters: Vec<IRFilter>,
}

fn analyze_single_node_nearest_ann_candidate(
    ir: &QueryIR,
    params: &ParamMap,
) -> Option<SingleNodeNearestAnnCandidate> {
    if ir.limit.is_none() || ir.order_by.len() != 1 {
        return None;
    }

    let nearest = &ir.order_by[0];
    let (nearest_var, nearest_prop, nearest_query) = match &nearest.expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } if !nearest.descending => (variable, property, query.as_ref().clone()),
        _ => return None,
    };

    let mut scan: Option<(String, String, Vec<IRFilter>)> = None;
    let mut explicit_filters = Vec::new();

    for op in &ir.pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                if scan.is_some() {
                    return None;
                }
                scan = Some((variable.clone(), type_name.clone(), filters.clone()));
            }
            IROp::Filter(filter) => explicit_filters.push(filter.clone()),
            _ => return None,
        }
    }

    let (scan_var, scan_type, mut filters) = scan?;
    if nearest_var != &scan_var {
        return None;
    }
    filters.extend(explicit_filters);

    if filters
        .iter()
        .any(|f| pushdown_scan_filter(&scan_var, f, params).is_none())
    {
        return None;
    }

    Some(SingleNodeNearestAnnCandidate {
        variable: scan_var,
        type_name: scan_type,
        property: nearest_prop.clone(),
        query: nearest_query,
        filters,
    })
}

async fn resolve_nearest_query_embeddings(
    ir: &QueryIR,
    runtime: &DatabaseRuntime,
    params: &ParamMap,
) -> Result<QueryIR> {
    let mut variable_types = AHashMap::<String, String>::new();
    collect_variable_types_from_ops(&ir.pipeline, &mut variable_types);

    let mut requests = AHashSet::<(String, usize)>::new();
    collect_nearest_embedding_requests(ir, runtime, &variable_types, params, &mut requests)?;
    if requests.is_empty() {
        return Ok(ir.clone());
    }

    let client = EmbeddingClient::from_env()?;
    let mut resolved_vectors = AHashMap::<(String, usize), Vec<f32>>::new();
    for (text, dim) in requests {
        let vector = client.embed_text(&text, dim).await?;
        resolved_vectors.insert((text, dim), vector);
    }

    let mut resolved = ir.clone();
    apply_resolved_nearest_embeddings(
        &mut resolved,
        runtime,
        &variable_types,
        params,
        &resolved_vectors,
    )?;
    Ok(resolved)
}

fn collect_variable_types_from_ops(ops: &[IROp], variable_types: &mut AHashMap<String, String>) {
    for op in ops {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                ..
            } => {
                variable_types.insert(variable.clone(), type_name.clone());
            }
            IROp::Expand {
                dst_var, dst_type, ..
            } => {
                variable_types.insert(dst_var.clone(), dst_type.clone());
            }
            IROp::Filter(_) => {}
            IROp::AntiJoin { inner, .. } => {
                collect_variable_types_from_ops(inner, variable_types);
            }
        }
    }
}

fn collect_nearest_embedding_requests(
    ir: &QueryIR,
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    params: &ParamMap,
    out: &mut AHashSet<(String, usize)>,
) -> Result<()> {
    for op in &ir.pipeline {
        collect_nearest_embedding_requests_from_op(op, runtime, variable_types, params, out)?;
    }
    for projection in &ir.return_exprs {
        collect_nearest_embedding_requests_from_expr(
            &projection.expr,
            runtime,
            variable_types,
            params,
            out,
        )?;
    }
    for ordering in &ir.order_by {
        collect_nearest_embedding_requests_from_expr(
            &ordering.expr,
            runtime,
            variable_types,
            params,
            out,
        )?;
    }
    Ok(())
}

fn collect_nearest_embedding_requests_from_op(
    op: &IROp,
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    params: &ParamMap,
    out: &mut AHashSet<(String, usize)>,
) -> Result<()> {
    match op {
        IROp::NodeScan { filters, .. } => {
            for filter in filters {
                collect_nearest_embedding_requests_from_expr(
                    &filter.left,
                    runtime,
                    variable_types,
                    params,
                    out,
                )?;
                collect_nearest_embedding_requests_from_expr(
                    &filter.right,
                    runtime,
                    variable_types,
                    params,
                    out,
                )?;
            }
        }
        IROp::Expand { .. } => {}
        IROp::Filter(filter) => {
            collect_nearest_embedding_requests_from_expr(
                &filter.left,
                runtime,
                variable_types,
                params,
                out,
            )?;
            collect_nearest_embedding_requests_from_expr(
                &filter.right,
                runtime,
                variable_types,
                params,
                out,
            )?;
        }
        IROp::AntiJoin { inner, .. } => {
            for inner_op in inner {
                collect_nearest_embedding_requests_from_op(
                    inner_op,
                    runtime,
                    variable_types,
                    params,
                    out,
                )?;
            }
        }
    }
    Ok(())
}

fn collect_nearest_embedding_requests_from_expr(
    expr: &IRExpr,
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    params: &ParamMap,
    out: &mut AHashSet<(String, usize)>,
) -> Result<()> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let dim = lookup_nearest_vector_dim(runtime, variable_types, variable, property)?;
            let query_literal = nearest_query_literal(query.as_ref(), params)?;
            if let Literal::String(text) = query_literal {
                out.insert((text.clone(), dim));
            }
        }
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => {
            collect_nearest_embedding_requests_from_expr(
                field.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
            collect_nearest_embedding_requests_from_expr(
                query.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            collect_nearest_embedding_requests_from_expr(
                field.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
            collect_nearest_embedding_requests_from_expr(
                query.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
            if let Some(max_edits) = max_edits {
                collect_nearest_embedding_requests_from_expr(
                    max_edits.as_ref(),
                    runtime,
                    variable_types,
                    params,
                    out,
                )?;
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            collect_nearest_embedding_requests_from_expr(
                primary.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
            collect_nearest_embedding_requests_from_expr(
                secondary.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
            if let Some(k) = k {
                collect_nearest_embedding_requests_from_expr(
                    k.as_ref(),
                    runtime,
                    variable_types,
                    params,
                    out,
                )?;
            }
        }
        IRExpr::Aggregate { arg, .. } => {
            collect_nearest_embedding_requests_from_expr(
                arg.as_ref(),
                runtime,
                variable_types,
                params,
                out,
            )?;
        }
        IRExpr::PropAccess { .. }
        | IRExpr::Variable(_)
        | IRExpr::Param(_)
        | IRExpr::Literal(_)
        | IRExpr::AliasRef(_) => {}
    }
    Ok(())
}

fn apply_resolved_nearest_embeddings(
    ir: &mut QueryIR,
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    params: &ParamMap,
    vectors: &AHashMap<(String, usize), Vec<f32>>,
) -> Result<()> {
    for op in &mut ir.pipeline {
        apply_resolved_nearest_embeddings_to_op(op, runtime, variable_types, params, vectors)?;
    }
    for projection in &mut ir.return_exprs {
        apply_resolved_nearest_embeddings_to_expr(
            &mut projection.expr,
            runtime,
            variable_types,
            params,
            vectors,
        )?;
    }
    for ordering in &mut ir.order_by {
        apply_resolved_nearest_embeddings_to_expr(
            &mut ordering.expr,
            runtime,
            variable_types,
            params,
            vectors,
        )?;
    }
    Ok(())
}

fn apply_resolved_nearest_embeddings_to_op(
    op: &mut IROp,
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    params: &ParamMap,
    vectors: &AHashMap<(String, usize), Vec<f32>>,
) -> Result<()> {
    match op {
        IROp::NodeScan { filters, .. } => {
            for filter in filters {
                apply_resolved_nearest_embeddings_to_expr(
                    &mut filter.left,
                    runtime,
                    variable_types,
                    params,
                    vectors,
                )?;
                apply_resolved_nearest_embeddings_to_expr(
                    &mut filter.right,
                    runtime,
                    variable_types,
                    params,
                    vectors,
                )?;
            }
        }
        IROp::Expand { .. } => {}
        IROp::Filter(filter) => {
            apply_resolved_nearest_embeddings_to_expr(
                &mut filter.left,
                runtime,
                variable_types,
                params,
                vectors,
            )?;
            apply_resolved_nearest_embeddings_to_expr(
                &mut filter.right,
                runtime,
                variable_types,
                params,
                vectors,
            )?;
        }
        IROp::AntiJoin { inner, .. } => {
            for inner_op in inner {
                apply_resolved_nearest_embeddings_to_op(
                    inner_op,
                    runtime,
                    variable_types,
                    params,
                    vectors,
                )?;
            }
        }
    }
    Ok(())
}

fn apply_resolved_nearest_embeddings_to_expr(
    expr: &mut IRExpr,
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    params: &ParamMap,
    vectors: &AHashMap<(String, usize), Vec<f32>>,
) -> Result<()> {
    match expr {
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            let dim = lookup_nearest_vector_dim(runtime, variable_types, variable, property)?;
            let query_literal = nearest_query_literal(query.as_ref(), params)?;
            if let Literal::String(text) = query_literal {
                let key = (text.clone(), dim);
                let vector = vectors.get(&key).ok_or_else(|| {
                    NanoError::Execution(format!(
                        "missing resolved embedding vector for nearest() query (dim={})",
                        dim
                    ))
                })?;
                **query = IRExpr::Literal(vector_to_literal(vector));
            }
        }
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => {
            apply_resolved_nearest_embeddings_to_expr(
                field.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
            apply_resolved_nearest_embeddings_to_expr(
                query.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            apply_resolved_nearest_embeddings_to_expr(
                field.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
            apply_resolved_nearest_embeddings_to_expr(
                query.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
            if let Some(max_edits) = max_edits {
                apply_resolved_nearest_embeddings_to_expr(
                    max_edits.as_mut(),
                    runtime,
                    variable_types,
                    params,
                    vectors,
                )?;
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            apply_resolved_nearest_embeddings_to_expr(
                primary.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
            apply_resolved_nearest_embeddings_to_expr(
                secondary.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
            if let Some(k) = k {
                apply_resolved_nearest_embeddings_to_expr(
                    k.as_mut(),
                    runtime,
                    variable_types,
                    params,
                    vectors,
                )?;
            }
        }
        IRExpr::Aggregate { arg, .. } => {
            apply_resolved_nearest_embeddings_to_expr(
                arg.as_mut(),
                runtime,
                variable_types,
                params,
                vectors,
            )?;
        }
        IRExpr::PropAccess { .. }
        | IRExpr::Variable(_)
        | IRExpr::Param(_)
        | IRExpr::Literal(_)
        | IRExpr::AliasRef(_) => {}
    }
    Ok(())
}

fn nearest_query_literal<'a>(query: &'a IRExpr, params: &'a ParamMap) -> Result<&'a Literal> {
    match query {
        IRExpr::Literal(lit) => Ok(lit),
        IRExpr::Param(name) => params
            .get(name)
            .ok_or_else(|| NanoError::Execution(format!("parameter `${}` not provided", name))),
        _ => Err(NanoError::Execution(
            "nearest() query must be a Vector or String literal/parameter".to_string(),
        )),
    }
}

fn lookup_nearest_vector_dim(
    runtime: &DatabaseRuntime,
    variable_types: &AHashMap<String, String>,
    variable: &str,
    property: &str,
) -> Result<usize> {
    let type_name = variable_types.get(variable).ok_or_else(|| {
        NanoError::Execution(format!(
            "nearest() variable `{}` is not bound to a node type",
            variable
        ))
    })?;
    let node_type = runtime.catalog().node_types.get(type_name).ok_or_else(|| {
        NanoError::Execution(format!(
            "nearest() node type `{}` not found in catalog",
            type_name
        ))
    })?;
    let prop = node_type.properties.get(property).ok_or_else(|| {
        NanoError::Execution(format!(
            "nearest() property `{}` not found on node type `{}`",
            property, type_name
        ))
    })?;
    if prop.list {
        return Err(NanoError::Execution(format!(
            "nearest() property `{}` on `{}` must be Vector(dim)",
            property, type_name
        )));
    }
    match prop.scalar {
        ScalarType::Vector(dim) if dim > 0 => Ok(dim as usize),
        _ => Err(NanoError::Execution(format!(
            "nearest() property `{}` on `{}` must be Vector(dim)",
            property, type_name
        ))),
    }
}

fn vector_to_literal(vector: &[f32]) -> Literal {
    Literal::List(
        vector
            .iter()
            .map(|v| Literal::Float(*v as f64))
            .collect::<Vec<_>>(),
    )
}

async fn try_execute_single_node_nearest_ann_fast_path(
    ir: &QueryIR,
    runtime: Arc<DatabaseRuntime>,
    params: &ParamMap,
    candidate: &SingleNodeNearestAnnCandidate,
) -> Result<Option<Vec<RecordBatch>>> {
    let limit = ir
        .limit
        .ok_or_else(|| NanoError::Execution("nearest ANN fast path requires limit".to_string()))?;
    let k = usize::try_from(limit)
        .map_err(|_| NanoError::Execution(format!("limit {} exceeds platform usize", limit)))?;
    let lim_i64 = i64::try_from(limit)
        .map_err(|_| NanoError::Execution(format!("limit {} exceeds i64", limit)))?;

    let node_type = match runtime.catalog().node_types.get(&candidate.type_name) {
        Some(node_type) => node_type,
        None => return Ok(None),
    };
    let vector_dim = match node_type.properties.get(&candidate.property) {
        Some(prop) if !prop.list => match prop.scalar {
            ScalarType::Vector(dim) if dim > 0 => dim as usize,
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let dataset_path = match runtime
        .node_dataset_path(&candidate.type_name)
        .filter(|p| p.exists())
    {
        Some(path) => path.to_path_buf(),
        None => return Ok(None),
    };
    let dataset_version = runtime.node_dataset_version(&candidate.type_name);

    let query_vec = resolve_nearest_query_vector(&candidate.query, params, vector_dim)?;
    let query_array = Float32Array::from(query_vec);

    let uri = dataset_path.to_string_lossy().to_string();
    let dataset = match Dataset::open(&uri).await {
        Ok(dataset) => dataset,
        Err(e) => {
            debug!(
                node_type = %candidate.type_name,
                error = %e,
                "nearest ANN fast path unavailable, falling back to exact path"
            );
            return Ok(None);
        }
    };
    let dataset = match dataset_version {
        Some(version) => match dataset.checkout_version(version).await {
            Ok(dataset) => dataset,
            Err(e) => {
                debug!(
                    node_type = %candidate.type_name,
                    dataset_version = version,
                    error = %e,
                    "nearest ANN fast path unavailable, falling back to exact path"
                );
                return Ok(None);
            }
        },
        None => dataset,
    };

    let field_idx = node_type
        .arrow_schema
        .index_of(&candidate.property)
        .ok()
        .map(|i| i as i32);
    let has_index = match field_idx {
        Some(field_idx) => match dataset.load_indices().await {
            Ok(indices) => indices.iter().any(|idx| idx.fields.contains(&field_idx)),
            Err(_) => false,
        },
        None => false,
    };
    if !has_index {
        return Ok(None);
    }

    let scan_projections = analyze_scan_projection_requirements(ir);
    let struct_fields = select_scan_struct_fields(
        &candidate.variable,
        &node_type.arrow_schema,
        &scan_projections,
    );
    let projected_columns: Vec<String> = struct_fields
        .iter()
        .map(|field| logical_node_field_to_lance(field.name()).to_string())
        .collect();

    let filter_sql = match build_lance_sql_filter_for_pushdown_preds(
        &candidate.variable,
        &candidate.filters,
        params,
    ) {
        Ok(sql) => sql,
        Err(e) => {
            debug!(
                node_type = %candidate.type_name,
                error = %e,
                "nearest ANN fast path unavailable, falling back to exact path"
            );
            return Ok(None);
        }
    };

    let mut scanner = dataset.scan();
    if let Err(e) = scanner.project(&projected_columns) {
        debug!(
            node_type = %candidate.type_name,
            error = %e,
            "nearest ANN fast path unavailable, falling back to exact path"
        );
        return Ok(None);
    }
    if let Some(expr) = filter_sql.as_deref() {
        if let Err(e) = scanner.filter(expr) {
            debug!(
                node_type = %candidate.type_name,
                error = %e,
                "nearest ANN fast path unavailable, falling back to exact path"
            );
            return Ok(None);
        }
        scanner.prefilter(true);
    }
    if let Err(e) = scanner.nearest(&candidate.property, &query_array, k) {
        debug!(
            node_type = %candidate.type_name,
            property = %candidate.property,
            error = %e,
            "nearest ANN fast path unavailable, falling back to exact path"
        );
        return Ok(None);
    }
    scanner.use_index(true);
    if let Err(e) = scanner.limit(Some(lim_i64), None) {
        debug!(
            node_type = %candidate.type_name,
            error = %e,
            "nearest ANN fast path unavailable, falling back to exact path"
        );
        return Ok(None);
    }

    let mut stream = match scanner.try_into_stream().await {
        Ok(stream) => stream,
        Err(e) => {
            debug!(
                node_type = %candidate.type_name,
                error = %e,
                "nearest ANN fast path unavailable, falling back to exact path"
            );
            return Ok(None);
        }
    };

    use futures::StreamExt;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => batches.push(batch),
            Err(e) => {
                debug!(
                    node_type = %candidate.type_name,
                    error = %e,
                    "nearest ANN fast path unavailable, falling back to exact path"
                );
                return Ok(None);
            }
        }
    }

    if batches.is_empty() {
        return Ok(Some(vec![]));
    }

    let merged = if batches.len() == 1 {
        batches.remove(0)
    } else {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| NanoError::Execution(e.to_string()))?
    };

    if merged.num_rows() == 0 {
        return Ok(Some(vec![]));
    }

    let wrapped = wrap_projected_node_batch(&candidate.variable, &struct_fields, &merged)?;
    let filtered = apply_ir_filters(&ir.pipeline, &[wrapped], params)?;
    let ordered = apply_order_and_limit(&filtered, &ir.order_by, ir.limit, params)?;
    let projected = apply_projection(&ir.return_exprs, &ordered, params)?;
    Ok(Some(projected))
}

fn build_lance_sql_filter_for_pushdown_preds(
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
) -> Result<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut clauses = Vec::with_capacity(filters.len());
    for filter in filters {
        let predicate = pushdown_scan_filter(variable, filter, params).ok_or_else(|| {
            NanoError::Execution("non-pushdown filter in nearest ANN path".to_string())
        })?;
        let lit = literal_to_lance_sql(&predicate.literal).ok_or_else(|| {
            NanoError::Execution(format!(
                "unsupported literal in Lance filter pushdown for property {}",
                predicate.property
            ))
        })?;
        let op = match predicate.op {
            CompOp::Eq => "=",
            CompOp::Ne => "!=",
            CompOp::Gt => ">",
            CompOp::Lt => "<",
            CompOp::Ge => ">=",
            CompOp::Le => "<=",
            CompOp::Contains => {
                return Err(NanoError::Execution(
                    "membership predicate is not supported in Lance filter pushdown".to_string(),
                ));
            }
        };
        clauses.push(format!(
            "{} {} {}",
            logical_node_field_to_lance(&predicate.property),
            op,
            lit
        ));
    }

    Ok(Some(clauses.join(" AND ")))
}

fn literal_to_lance_sql(literal: &Literal) -> Option<String> {
    literal_utils::literal_to_lance_sql(literal)
}

fn wrap_projected_node_batch(
    variable: &str,
    struct_fields: &[Field],
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let mut runtime_fields = Vec::with_capacity(struct_fields.len());
    let mut output_columns = Vec::with_capacity(struct_fields.len());
    for expected in struct_fields {
        let col = batch
            .column_by_name(expected.name())
            .or_else(|| batch.column_by_name(logical_node_field_to_lance(expected.name())))
            .cloned()
            .ok_or_else(|| {
                NanoError::Execution(format!(
                    "column {} missing from Lance nearest result",
                    expected.name()
                ))
            })?;
        let field = Field::new(
            expected.name(),
            col.data_type().clone(),
            expected.is_nullable() || col.null_count() > 0,
        );
        runtime_fields.push(Field::new(
            &flat_binding_col(variable, expected.name()),
            field.data_type().clone(),
            field.is_nullable(),
        ));
        output_columns.push(col);
    }

    let output_schema = Arc::new(Schema::new(runtime_fields));

    RecordBatch::try_new(output_schema, output_columns)
        .map_err(|e| NanoError::Execution(e.to_string()))
}

fn build_physical_plan(
    pipeline: &[IROp],
    runtime: Arc<DatabaseRuntime>,
    params: &ParamMap,
    scan_limit_pushdown: Option<usize>,
    scan_projections: &ScanProjectionMap,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut current_plan: Option<Arc<dyn ExecutionPlan>> = None;

    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                let node_schema =
                    runtime.catalog().node_types.get(type_name).ok_or_else(|| {
                        NanoError::Plan(format!("unknown node type: {}", type_name))
                    })?;

                let selected_fields = select_scan_struct_fields(
                    variable,
                    &node_schema.arrow_schema,
                    scan_projections,
                );
                let scan_fields = selected_fields
                    .into_iter()
                    .map(|field| {
                        Field::new(
                            &flat_binding_col(variable, field.name()),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    })
                    .collect::<Vec<_>>();

                let output_schema = if let Some(ref plan) = current_plan {
                    // Append to existing schema
                    let mut fields: Vec<Field> = plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect();
                    fields.extend(scan_fields.clone());
                    Arc::new(Schema::new(fields))
                } else {
                    Arc::new(Schema::new(scan_fields.clone()))
                };

                let indexed_props = &node_schema.indexed_properties;
                let mut pushdown_filters =
                    build_scan_pushdown_filters(variable, filters, params, Some(indexed_props));
                pushdown_filters.extend(build_explicit_pushdown_filters(
                    variable,
                    pipeline,
                    params,
                    Some(indexed_props),
                ));

                let scan = NodeScanExec::new(
                    type_name.clone(),
                    variable.clone(),
                    Arc::new(Schema::new(scan_fields)),
                    pushdown_filters,
                    if current_plan.is_none() {
                        scan_limit_pushdown
                    } else {
                        None
                    },
                    runtime.clone(),
                );

                if let Some(prev) = current_plan {
                    // Cross join with previous plan (for multi-binding patterns)
                    current_plan = Some(Arc::new(CrossJoinExec::new(
                        prev,
                        Arc::new(scan),
                        output_schema,
                    )));
                } else {
                    current_plan = Some(Arc::new(scan));
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
            } => {
                let input = current_plan
                    .ok_or_else(|| NanoError::Plan("Expand without input".to_string()))?;
                let expand = ExpandExec::new(
                    input,
                    ExpandSpec {
                        src_var: src_var.clone(),
                        dst_var: dst_var.clone(),
                        edge_type: edge_type.clone(),
                        direction: *direction,
                        dst_type: dst_type.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                    },
                    runtime.clone(),
                );
                current_plan = Some(Arc::new(expand));
            }
            IROp::Filter(_) => {
                // Filters are applied post-execution for simplicity in v0
            }
            IROp::AntiJoin { outer_var, inner } => {
                let outer = current_plan
                    .ok_or_else(|| NanoError::Plan("AntiJoin without outer input".to_string()))?;

                // Build the inner plan, seeding it with the outer plan as input
                // so Expand ops have the source rows to work with
                let inner_plan = build_physical_plan_with_input(
                    inner,
                    runtime.clone(),
                    outer.clone(),
                    params,
                    scan_projections,
                )?;

                current_plan = Some(Arc::new(AntiJoinExec::new(
                    outer,
                    inner_plan,
                    outer_var.clone(),
                    inner.clone(),
                    params.clone(),
                )));
            }
        }
    }

    current_plan.ok_or_else(|| NanoError::Plan("empty pipeline".to_string()))
}

/// Like build_physical_plan but seeds with an initial input plan.
/// Used for AntiJoin inner pipelines that start with Expand (needing source rows).
fn build_physical_plan_with_input(
    pipeline: &[IROp],
    runtime: Arc<DatabaseRuntime>,
    input: Arc<dyn ExecutionPlan>,
    params: &ParamMap,
    scan_projections: &ScanProjectionMap,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut current_plan: Option<Arc<dyn ExecutionPlan>> = Some(input);

    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                let node_schema =
                    runtime.catalog().node_types.get(type_name).ok_or_else(|| {
                        NanoError::Plan(format!("unknown node type: {}", type_name))
                    })?;

                let selected_fields = select_scan_struct_fields(
                    variable,
                    &node_schema.arrow_schema,
                    scan_projections,
                );
                let scan_fields = selected_fields
                    .into_iter()
                    .map(|field| {
                        Field::new(
                            &flat_binding_col(variable, field.name()),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                    })
                    .collect::<Vec<_>>();

                let output_schema = if let Some(ref plan) = current_plan {
                    let mut fields: Vec<Field> = plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect();
                    fields.extend(scan_fields.clone());
                    Arc::new(Schema::new(fields))
                } else {
                    Arc::new(Schema::new(scan_fields.clone()))
                };

                let indexed_props = &node_schema.indexed_properties;
                let mut pushdown_filters =
                    build_scan_pushdown_filters(variable, filters, params, Some(indexed_props));
                pushdown_filters.extend(build_explicit_pushdown_filters(
                    variable,
                    pipeline,
                    params,
                    Some(indexed_props),
                ));

                let scan = NodeScanExec::new(
                    type_name.clone(),
                    variable.clone(),
                    Arc::new(Schema::new(scan_fields)),
                    pushdown_filters,
                    None,
                    runtime.clone(),
                );

                if let Some(prev) = current_plan {
                    current_plan = Some(Arc::new(CrossJoinExec::new(
                        prev,
                        Arc::new(scan),
                        output_schema,
                    )));
                } else {
                    current_plan = Some(Arc::new(scan));
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
            } => {
                let input = current_plan
                    .ok_or_else(|| NanoError::Plan("Expand without input".to_string()))?;
                let expand = ExpandExec::new(
                    input,
                    ExpandSpec {
                        src_var: src_var.clone(),
                        dst_var: dst_var.clone(),
                        edge_type: edge_type.clone(),
                        direction: *direction,
                        dst_type: dst_type.clone(),
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                    },
                    runtime.clone(),
                );
                current_plan = Some(Arc::new(expand));
            }
            IROp::Filter(_) => {
                // Filters handled post-execution
            }
            IROp::AntiJoin { outer_var, inner } => {
                let outer = current_plan
                    .ok_or_else(|| NanoError::Plan("AntiJoin without outer input".to_string()))?;
                let inner_plan = build_physical_plan_with_input(
                    inner,
                    runtime.clone(),
                    outer.clone(),
                    params,
                    scan_projections,
                )?;
                current_plan = Some(Arc::new(AntiJoinExec::new(
                    outer,
                    inner_plan,
                    outer_var.clone(),
                    inner.clone(),
                    params.clone(),
                )));
            }
        }
    }

    current_plan.ok_or_else(|| NanoError::Plan("empty inner pipeline".to_string()))
}

fn build_scan_pushdown_filters(
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
    indexed_props: Option<&HashSet<String>>,
) -> Vec<NodeScanPredicate> {
    filters
        .iter()
        .filter_map(|filter| {
            pushdown_scan_filter(variable, filter, params)
                .map(|pred| with_index_eligibility(pred, indexed_props))
        })
        .collect()
}

fn build_explicit_pushdown_filters(
    variable: &str,
    pipeline: &[IROp],
    params: &ParamMap,
    indexed_props: Option<&HashSet<String>>,
) -> Vec<NodeScanPredicate> {
    pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::Filter(filter) => pushdown_scan_filter(variable, filter, params)
                .map(|pred| with_index_eligibility(pred, indexed_props)),
            _ => None,
        })
        .collect()
}

fn with_index_eligibility(
    mut predicate: NodeScanPredicate,
    indexed_props: Option<&HashSet<String>>,
) -> NodeScanPredicate {
    predicate.index_eligible = is_index_eligible(indexed_props, &predicate.property, predicate.op);
    predicate
}

fn is_index_eligible(indexed_props: Option<&HashSet<String>>, property: &str, op: CompOp) -> bool {
    matches!(
        op,
        CompOp::Eq | CompOp::Gt | CompOp::Lt | CompOp::Ge | CompOp::Le
    ) && indexed_props
        .map(|props| props.contains(property))
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct SingleScanPushdownInfo {
    all_filters_pushdown: bool,
}

fn analyze_single_scan_pushdown(
    pipeline: &[IROp],
    params: &ParamMap,
) -> Option<SingleScanPushdownInfo> {
    let mut scan_var: Option<&str> = None;
    let mut scan_filters: &[IRFilter] = &[];
    let mut explicit_filters: Vec<&IRFilter> = Vec::new();

    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable, filters, ..
            } => {
                if scan_var.is_some() {
                    return None;
                }
                scan_var = Some(variable);
                scan_filters = filters;
            }
            IROp::Filter(filter) => explicit_filters.push(filter),
            _ => return None,
        }
    }

    let variable = scan_var?;
    let mut all_filters_pushdown = true;
    for filter in scan_filters {
        if pushdown_scan_filter(variable, filter, params).is_none() {
            all_filters_pushdown = false;
        }
    }

    for filter in explicit_filters {
        if pushdown_scan_filter(variable, filter, params).is_none() {
            all_filters_pushdown = false;
        }
    }

    Some(SingleScanPushdownInfo {
        all_filters_pushdown,
    })
}

fn analyze_scan_projection_requirements(ir: &QueryIR) -> ScanProjectionMap {
    let mut scan_variables = AHashSet::new();
    collect_scan_variables(&ir.pipeline, &mut scan_variables);

    let mut requirements: ScanProjectionMap = scan_variables
        .iter()
        .map(|var| (var.clone(), Some(AHashSet::new())))
        .collect();

    collect_pipeline_projection_requirements(&ir.pipeline, &scan_variables, &mut requirements);
    for proj in &ir.return_exprs {
        collect_expr_projection_requirements(&proj.expr, &scan_variables, &mut requirements);
    }
    for ordering in &ir.order_by {
        collect_expr_projection_requirements(&ordering.expr, &scan_variables, &mut requirements);
    }

    // Keep id available for join/expand semantics and stable downstream behavior.
    for props in requirements.values_mut().flatten() {
        props.insert("id".to_string());
    }

    requirements
}

fn collect_scan_variables(pipeline: &[IROp], out: &mut AHashSet<String>) {
    for op in pipeline {
        match op {
            IROp::NodeScan { variable, .. } => {
                out.insert(variable.clone());
            }
            IROp::AntiJoin { inner, .. } => collect_scan_variables(inner, out),
            _ => {}
        }
    }
}

fn collect_pipeline_projection_requirements(
    pipeline: &[IROp],
    scan_variables: &AHashSet<String>,
    requirements: &mut ScanProjectionMap,
) {
    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable, filters, ..
            } => {
                for filter in filters {
                    collect_expr_projection_requirements(
                        &filter.left,
                        scan_variables,
                        requirements,
                    );
                    collect_expr_projection_requirements(
                        &filter.right,
                        scan_variables,
                        requirements,
                    );
                }

                // Always keep id available for the bound variable.
                mark_scan_property(requirements, variable, "id");
            }
            IROp::Expand { src_var, .. } => {
                mark_scan_property(requirements, src_var, "id");
            }
            IROp::Filter(filter) => {
                collect_expr_projection_requirements(&filter.left, scan_variables, requirements);
                collect_expr_projection_requirements(&filter.right, scan_variables, requirements);
            }
            IROp::AntiJoin { outer_var, inner } => {
                mark_scan_property(requirements, outer_var, "id");
                collect_pipeline_projection_requirements(inner, scan_variables, requirements);
            }
        }
    }
}

fn collect_expr_projection_requirements(
    expr: &IRExpr,
    scan_variables: &AHashSet<String>,
    requirements: &mut ScanProjectionMap,
) {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            if scan_variables.contains(variable) {
                mark_scan_property(requirements, variable, property);
            }
        }
        IRExpr::Variable(variable) => {
            if scan_variables.contains(variable) {
                mark_scan_full(requirements, variable);
            }
        }
        IRExpr::Aggregate { arg, .. } => {
            collect_expr_projection_requirements(arg, scan_variables, requirements);
        }
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => {
            if scan_variables.contains(variable) {
                mark_scan_property(requirements, variable, property);
                mark_scan_property(requirements, variable, "id");
            }
            collect_expr_projection_requirements(query, scan_variables, requirements);
        }
        IRExpr::Search { field, query } => {
            collect_expr_projection_requirements(field, scan_variables, requirements);
            collect_expr_projection_requirements(query, scan_variables, requirements);
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            collect_expr_projection_requirements(field, scan_variables, requirements);
            collect_expr_projection_requirements(query, scan_variables, requirements);
            if let Some(expr) = max_edits.as_deref() {
                collect_expr_projection_requirements(expr, scan_variables, requirements);
            }
        }
        IRExpr::MatchText { field, query } => {
            collect_expr_projection_requirements(field, scan_variables, requirements);
            collect_expr_projection_requirements(query, scan_variables, requirements);
        }
        IRExpr::Bm25 { field, query } => {
            collect_expr_projection_requirements(field, scan_variables, requirements);
            collect_expr_projection_requirements(query, scan_variables, requirements);
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            collect_expr_projection_requirements(primary, scan_variables, requirements);
            collect_expr_projection_requirements(secondary, scan_variables, requirements);
            if let Some(expr) = k.as_deref() {
                collect_expr_projection_requirements(expr, scan_variables, requirements);
            }
        }
        _ => {}
    }
}

fn mark_scan_property(requirements: &mut ScanProjectionMap, variable: &str, property: &str) {
    if let Some(required) = requirements.get_mut(variable)
        && let Some(props) = required
    {
        props.insert(property.to_string());
    }
}

fn mark_scan_full(requirements: &mut ScanProjectionMap, variable: &str) {
    if let Some(required) = requirements.get_mut(variable) {
        *required = None;
    }
}

fn select_scan_struct_fields(
    variable: &str,
    node_schema: &SchemaRef,
    scan_projections: &ScanProjectionMap,
) -> Vec<Field> {
    match scan_projections.get(variable) {
        Some(Some(required_props)) => {
            let selected: Vec<Field> = node_schema
                .fields()
                .iter()
                .filter(|field| required_props.contains(field.name()))
                .map(|f| f.as_ref().clone())
                .collect();
            if selected.is_empty() {
                node_schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect()
            } else {
                selected
            }
        }
        _ => node_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect(),
    }
}

fn pushdown_scan_filter(
    variable: &str,
    filter: &IRFilter,
    params: &ParamMap,
) -> Option<NodeScanPredicate> {
    match (&filter.left, &filter.right) {
        (
            IRExpr::PropAccess {
                variable: v,
                property,
            },
            rhs,
        ) if v == variable => pushdown_literal(rhs, params).map(|literal| NodeScanPredicate {
            property: property.clone(),
            op: filter.op,
            literal,
            index_eligible: false,
        }),
        (
            lhs,
            IRExpr::PropAccess {
                variable: v,
                property,
            },
        ) if v == variable => pushdown_literal(lhs, params).map(|literal| NodeScanPredicate {
            property: property.clone(),
            op: flip_comp_op(filter.op),
            literal,
            index_eligible: false,
        }),
        _ => None,
    }
}

fn pushdown_literal(expr: &IRExpr, params: &ParamMap) -> Option<Literal> {
    match expr {
        IRExpr::Literal(lit) => Some(lit.clone()),
        IRExpr::Param(name) => params.get(name).cloned(),
        _ => None,
    }
}

fn flip_comp_op(op: CompOp) -> CompOp {
    match op {
        CompOp::Eq => CompOp::Eq,
        CompOp::Ne => CompOp::Ne,
        CompOp::Gt => CompOp::Lt,
        CompOp::Lt => CompOp::Gt,
        CompOp::Ge => CompOp::Le,
        CompOp::Le => CompOp::Ge,
        CompOp::Contains => CompOp::Contains,
    }
}

fn apply_ir_filters(
    pipeline: &[IROp],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    let scan_variables = pipeline_scan_variables(pipeline);
    let mut filters: Vec<&IRFilter> = Vec::new();

    for op in pipeline {
        match op {
            IROp::Filter(f) => {
                if is_explicit_filter_pushed_down(&scan_variables, f, params) {
                    continue;
                }
                filters.push(f);
            }
            IROp::NodeScan {
                variable,
                filters: scan_filters,
                ..
            } => {
                for f in scan_filters {
                    if pushdown_scan_filter(variable, f, params).is_none() {
                        filters.push(f);
                    }
                }
            }
            _ => {}
        }
    }

    if filters.is_empty() {
        return Ok(batches.to_vec());
    }

    let mut result = batches.to_vec();
    for filter in filters {
        result = apply_single_filter(filter, &result, params)?;
    }
    Ok(result)
}

fn pipeline_scan_variables(pipeline: &[IROp]) -> AHashSet<String> {
    pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::NodeScan { variable, .. } => Some(variable.clone()),
            _ => None,
        })
        .collect()
}

fn is_explicit_filter_pushed_down(
    scan_variables: &AHashSet<String>,
    filter: &IRFilter,
    params: &ParamMap,
) -> bool {
    scan_variables
        .iter()
        .any(|variable| pushdown_scan_filter(variable, filter, params).is_some())
}

fn apply_single_filter(
    filter: &IRFilter,
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    let mut result = Vec::new();
    for batch in batches {
        let left = eval_ir_expr(&filter.left, batch, params)?;
        let right = eval_ir_expr(&filter.right, batch, params)?;

        // Cast right to match left's data type if they differ
        let right = if filter.op != CompOp::Contains && left.data_type() != right.data_type() {
            arrow_cast::cast(&right, left.data_type())
                .map_err(|e| NanoError::Execution(format!("cast error: {}", e)))?
        } else {
            right
        };

        let mask = compare_arrays(&left, &right, filter.op)?;

        let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
            .map_err(|e| NanoError::Execution(e.to_string()))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

fn eval_ir_expr(
    expr: &IRExpr,
    batch: &RecordBatch,
    params: &ParamMap,
) -> Result<arrow_array::ArrayRef> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            binding_property_array(batch, variable, property)
        }
        IRExpr::Literal(lit) => {
            let num_rows = batch.num_rows();
            literal_to_array(lit, num_rows)
        }
        IRExpr::Variable(name) => {
            if let Ok(col_idx) = batch.schema().index_of(name) {
                return Ok(batch.column(col_idx).clone());
            }
            binding_struct_array(batch, name)
        }
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| NanoError::Execution(format!("parameter ${} not provided", name)))?;
            literal_to_array(lit, batch.num_rows())
        }
        IRExpr::Nearest {
            variable,
            property,
            query,
        } => compute_nearest_distance_column(batch, variable, property, query, params),
        IRExpr::Search { field, query } => {
            let field_arr = eval_ir_expr(field, batch, params)?;
            let query_arr = eval_ir_expr(query, batch, params)?;
            evaluate_search_boolean_array(&field_arr, &query_arr)
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            let field_arr = eval_ir_expr(field, batch, params)?;
            let query_arr = eval_ir_expr(query, batch, params)?;
            let max_edits_arr = max_edits
                .as_ref()
                .map(|expr| eval_ir_expr(expr, batch, params))
                .transpose()?;
            evaluate_fuzzy_boolean_array(&field_arr, &query_arr, max_edits_arr.as_ref())
        }
        IRExpr::MatchText { field, query } => {
            let field_arr = eval_ir_expr(field, batch, params)?;
            let query_arr = eval_ir_expr(query, batch, params)?;
            evaluate_match_text_boolean_array(&field_arr, &query_arr)
        }
        IRExpr::Bm25 { field, query } => {
            let field_arr = eval_ir_expr(field, batch, params)?;
            let query_arr = eval_ir_expr(query, batch, params)?;
            evaluate_bm25_score_array(&field_arr, &query_arr)
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => evaluate_rrf_score_array(primary, secondary, k.as_deref(), batch, params),
        _ => Err(NanoError::Execution(
            "unsupported expr in filter".to_string(),
        )),
    }
}

fn evaluate_search_boolean_array(
    field_arr: &arrow_array::ArrayRef,
    query_arr: &arrow_array::ArrayRef,
) -> Result<arrow_array::ArrayRef> {
    if field_arr.len() != query_arr.len() {
        return Err(NanoError::Execution(format!(
            "search() argument length mismatch: field has {}, query has {}",
            field_arr.len(),
            query_arr.len()
        )));
    }

    let field_utf8 = field_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution("search() field expression must evaluate to String".to_string())
        })?;
    let query_utf8 = query_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution("search() query expression must evaluate to String".to_string())
        })?;

    let mut out = Vec::with_capacity(field_arr.len());
    for row in 0..field_arr.len() {
        if field_utf8.is_null(row) || query_utf8.is_null(row) {
            out.push(false);
            continue;
        }
        out.push(matches_keyword_search(
            field_utf8.value(row),
            query_utf8.value(row),
        ));
    }
    Ok(Arc::new(BooleanArray::from(out)))
}

fn evaluate_fuzzy_boolean_array(
    field_arr: &arrow_array::ArrayRef,
    query_arr: &arrow_array::ArrayRef,
    max_edits_arr: Option<&arrow_array::ArrayRef>,
) -> Result<arrow_array::ArrayRef> {
    if field_arr.len() != query_arr.len() {
        return Err(NanoError::Execution(format!(
            "fuzzy() argument length mismatch: field has {}, query has {}",
            field_arr.len(),
            query_arr.len()
        )));
    }
    if let Some(max_edits) = max_edits_arr
        && max_edits.len() != field_arr.len()
    {
        return Err(NanoError::Execution(format!(
            "fuzzy() max_edits length mismatch: expected {}, got {}",
            field_arr.len(),
            max_edits.len()
        )));
    }

    let field_utf8 = field_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution("fuzzy() field expression must evaluate to String".to_string())
        })?;
    let query_utf8 = query_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution("fuzzy() query expression must evaluate to String".to_string())
        })?;

    let mut out = Vec::with_capacity(field_arr.len());
    for row in 0..field_arr.len() {
        if field_utf8.is_null(row) || query_utf8.is_null(row) {
            out.push(false);
            continue;
        }
        let override_edits = max_edits_arr
            .map(|arr| array_value_to_optional_non_negative_usize(arr, row))
            .transpose()?
            .flatten();
        out.push(matches_fuzzy_search(
            field_utf8.value(row),
            query_utf8.value(row),
            override_edits,
        ));
    }
    Ok(Arc::new(BooleanArray::from(out)))
}

fn evaluate_match_text_boolean_array(
    field_arr: &arrow_array::ArrayRef,
    query_arr: &arrow_array::ArrayRef,
) -> Result<arrow_array::ArrayRef> {
    if field_arr.len() != query_arr.len() {
        return Err(NanoError::Execution(format!(
            "match_text() argument length mismatch: field has {}, query has {}",
            field_arr.len(),
            query_arr.len()
        )));
    }

    let field_utf8 = field_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution(
                "match_text() field expression must evaluate to String".to_string(),
            )
        })?;
    let query_utf8 = query_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution(
                "match_text() query expression must evaluate to String".to_string(),
            )
        })?;

    let mut out = Vec::with_capacity(field_arr.len());
    for row in 0..field_arr.len() {
        if field_utf8.is_null(row) || query_utf8.is_null(row) {
            out.push(false);
            continue;
        }
        out.push(matches_match_text(
            field_utf8.value(row),
            query_utf8.value(row),
        ));
    }
    Ok(Arc::new(BooleanArray::from(out)))
}

fn evaluate_bm25_score_array(
    field_arr: &arrow_array::ArrayRef,
    query_arr: &arrow_array::ArrayRef,
) -> Result<arrow_array::ArrayRef> {
    const BM25_K1: f64 = 1.2;
    const BM25_B: f64 = 0.75;

    if field_arr.len() != query_arr.len() {
        return Err(NanoError::Execution(format!(
            "bm25() argument length mismatch: field has {}, query has {}",
            field_arr.len(),
            query_arr.len()
        )));
    }

    let field_utf8 = field_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution("bm25() field expression must evaluate to String".to_string())
        })?;
    let query_utf8 = query_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            NanoError::Execution("bm25() query expression must evaluate to String".to_string())
        })?;

    let mut doc_term_freqs: Vec<Option<AHashMap<String, usize>>> =
        Vec::with_capacity(field_arr.len());
    let mut doc_lengths: Vec<f64> = Vec::with_capacity(field_arr.len());
    let mut doc_freq: AHashMap<String, usize> = AHashMap::new();
    let mut doc_count = 0usize;
    let mut total_doc_len = 0.0f64;

    for row in 0..field_arr.len() {
        if field_utf8.is_null(row) {
            doc_term_freqs.push(None);
            doc_lengths.push(0.0);
            continue;
        }
        let tokens = tokenize_search_terms(field_utf8.value(row));
        let dl = tokens.len() as f64;
        total_doc_len += dl;
        doc_count += 1;

        let mut tf: AHashMap<String, usize> = AHashMap::new();
        let mut seen_in_doc: AHashSet<String> = AHashSet::new();
        for token in tokens {
            *tf.entry(token.clone()).or_insert(0) += 1;
            if seen_in_doc.insert(token.clone()) {
                *doc_freq.entry(token).or_insert(0) += 1;
            }
        }
        doc_term_freqs.push(Some(tf));
        doc_lengths.push(dl);
    }

    let avg_doc_len = if doc_count > 0 {
        total_doc_len / (doc_count as f64)
    } else {
        0.0
    };

    // Optimize broadcast query expressions (e.g. bm25($doc, $q) with scalar $q):
    // tokenize and count query terms once instead of per row.
    let mut broadcast_query_tf: Option<AHashMap<String, usize>> = None;
    if query_utf8.len() > 0 && !query_utf8.is_null(0) {
        let first_query = query_utf8.value(0);
        let is_broadcast = (1..query_utf8.len())
            .all(|row| !query_utf8.is_null(row) && query_utf8.value(row) == first_query);
        if is_broadcast {
            let mut tf = AHashMap::new();
            for token in tokenize_search_terms(first_query) {
                *tf.entry(token).or_insert(0) += 1;
            }
            if !tf.is_empty() {
                broadcast_query_tf = Some(tf);
            }
        }
    }

    let mut scores = Vec::with_capacity(field_arr.len());
    for (row, dl) in doc_lengths
        .iter()
        .copied()
        .enumerate()
        .take(field_arr.len())
    {
        if query_utf8.is_null(row) {
            scores.push(0.0);
            continue;
        }
        let Some(tf) = doc_term_freqs.get(row).and_then(|m| m.as_ref()) else {
            scores.push(0.0);
            continue;
        };

        let norm = if avg_doc_len > 0.0 {
            BM25_K1 * (1.0 - BM25_B + BM25_B * (dl / avg_doc_len))
        } else {
            BM25_K1
        };

        if let Some(query_tf) = broadcast_query_tf.as_ref() {
            scores.push(score_bm25_terms(
                tf, query_tf, &doc_freq, doc_count, norm, BM25_K1,
            ));
            continue;
        }

        let query_tokens = tokenize_search_terms(query_utf8.value(row));
        if query_tokens.is_empty() {
            scores.push(0.0);
            continue;
        }

        let mut query_tf: AHashMap<String, usize> = AHashMap::new();
        for token in query_tokens {
            *query_tf.entry(token).or_insert(0) += 1;
        }
        scores.push(score_bm25_terms(
            tf, &query_tf, &doc_freq, doc_count, norm, BM25_K1,
        ));
    }

    Ok(Arc::new(Float64Array::from(scores)))
}

fn score_bm25_terms(
    doc_tf: &AHashMap<String, usize>,
    query_tf: &AHashMap<String, usize>,
    doc_freq: &AHashMap<String, usize>,
    doc_count: usize,
    norm: f64,
    k1: f64,
) -> f64 {
    let mut score = 0.0f64;
    for (term, qf) in query_tf {
        let tf_term = *doc_tf.get(term).unwrap_or(&0) as f64;
        if tf_term <= 0.0 {
            continue;
        }
        let df = *doc_freq.get(term).unwrap_or(&0) as f64;
        let n = doc_count as f64;
        let idf = (1.0 + ((n - df + 0.5) / (df + 0.5))).ln();
        let numerator = tf_term * (k1 + 1.0);
        let denom = tf_term + norm;
        if denom > 0.0 {
            score += idf * (numerator / denom) * (*qf as f64);
        }
    }
    score
}

fn evaluate_rrf_score_array(
    primary_expr: &IRExpr,
    secondary_expr: &IRExpr,
    k_expr: Option<&IRExpr>,
    batch: &RecordBatch,
    params: &ParamMap,
) -> Result<arrow_array::ArrayRef> {
    const DEFAULT_RRF_K: usize = 60;

    let primary_scores = eval_ir_expr(primary_expr, batch, params)?;
    let secondary_scores = eval_ir_expr(secondary_expr, batch, params)?;

    if primary_scores.len() != secondary_scores.len() {
        return Err(NanoError::Execution(format!(
            "rrf() rank input length mismatch: primary has {}, secondary has {}",
            primary_scores.len(),
            secondary_scores.len()
        )));
    }

    let primary_values = numeric_array_to_optional_f64(&primary_scores)?;
    let secondary_values = numeric_array_to_optional_f64(&secondary_scores)?;

    let primary_ranks = rank_positions_for_rrf(
        &primary_values,
        matches!(primary_expr, IRExpr::Bm25 { .. } | IRExpr::Rrf { .. }),
    );
    let secondary_ranks = rank_positions_for_rrf(
        &secondary_values,
        matches!(secondary_expr, IRExpr::Bm25 { .. } | IRExpr::Rrf { .. }),
    );

    let k_values = if let Some(expr) = k_expr {
        let k_arr = eval_ir_expr(expr, batch, params)?;
        let mut per_row = Vec::with_capacity(k_arr.len());
        for row in 0..k_arr.len() {
            let k = array_value_to_optional_positive_usize(&k_arr, row)?.unwrap_or(DEFAULT_RRF_K);
            per_row.push(k as f64);
        }
        per_row
    } else {
        vec![DEFAULT_RRF_K as f64; primary_scores.len()]
    };

    let mut out = Vec::with_capacity(primary_scores.len());
    for row in 0..primary_scores.len() {
        let k = k_values[row];
        let mut score = 0.0f64;
        if let Some(rank) = primary_ranks[row] {
            score += 1.0 / (k + rank as f64);
        }
        if let Some(rank) = secondary_ranks[row] {
            score += 1.0 / (k + rank as f64);
        }
        out.push(score);
    }

    Ok(Arc::new(Float64Array::from(out)))
}

fn numeric_array_to_optional_f64(arr: &arrow_array::ArrayRef) -> Result<Vec<Option<f64>>> {
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        values.push(array_value_to_f64(arr, row));
    }
    if values.iter().any(|v| v.is_none()) {
        return Err(NanoError::Execution(
            "rrf() rank expressions must evaluate to numeric scores".to_string(),
        ));
    }
    Ok(values)
}

fn rank_positions_for_rrf(values: &[Option<f64>], descending: bool) -> Vec<Option<usize>> {
    let mut idx_values: Vec<(usize, f64)> = values
        .iter()
        .enumerate()
        .filter_map(|(idx, value)| value.map(|v| (idx, v)))
        .collect();

    idx_values.sort_by(|(idx_a, val_a), (idx_b, val_b)| {
        let ord = val_a
            .partial_cmp(val_b)
            .unwrap_or(std::cmp::Ordering::Equal);
        let ord = if descending { ord.reverse() } else { ord };
        ord.then_with(|| idx_a.cmp(idx_b))
    });

    let mut out = vec![None; values.len()];
    for (rank_idx, (row_idx, _)) in idx_values.into_iter().enumerate() {
        out[row_idx] = Some(rank_idx + 1);
    }
    out
}

fn array_value_to_optional_non_negative_usize(
    arr: &arrow_array::ArrayRef,
    row: usize,
) -> Result<Option<usize>> {
    if arr.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = arr.as_any().downcast_ref::<arrow_array::Int32Array>() {
        let value = values.value(row);
        if value < 0 {
            return Err(NanoError::Execution(format!(
                "fuzzy() max_edits must be >= 0, got {}",
                value
            )));
        }
        return Ok(Some(value as usize));
    }
    if let Some(values) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        let value = values.value(row);
        if value < 0 {
            return Err(NanoError::Execution(format!(
                "fuzzy() max_edits must be >= 0, got {}",
                value
            )));
        }
        return Ok(Some(value as usize));
    }
    if let Some(values) = arr.as_any().downcast_ref::<arrow_array::UInt32Array>() {
        return Ok(Some(values.value(row) as usize));
    }
    if let Some(values) = arr.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return Ok(Some(values.value(row) as usize));
    }
    Err(NanoError::Execution(
        "fuzzy() max_edits must evaluate to an integer".to_string(),
    ))
}

fn array_value_to_optional_positive_usize(
    arr: &arrow_array::ArrayRef,
    row: usize,
) -> Result<Option<usize>> {
    let value = array_value_to_optional_non_negative_usize(arr, row)?;
    if let Some(v) = value
        && v == 0
    {
        return Err(NanoError::Execution(
            "rrf() k must be greater than 0".to_string(),
        ));
    }
    Ok(value)
}

fn matches_keyword_search(field: &str, query: &str) -> bool {
    let field_tokens: AHashSet<String> = tokenize_search_terms(field).into_iter().collect();
    if field_tokens.is_empty() {
        return false;
    }
    let query_tokens = tokenize_search_terms(query);
    if query_tokens.is_empty() {
        return false;
    }
    query_tokens
        .iter()
        .all(|term| field_tokens.contains(term.as_str()))
}

fn matches_match_text(field: &str, query: &str) -> bool {
    let field_tokens = tokenize_search_terms(field);
    if field_tokens.is_empty() {
        return false;
    }
    let query_tokens = tokenize_search_terms(query);
    if query_tokens.is_empty() {
        return false;
    }
    if query_tokens.len() > field_tokens.len() {
        return false;
    }

    field_tokens
        .windows(query_tokens.len())
        .any(|window| window == query_tokens.as_slice())
}

fn matches_fuzzy_search(field: &str, query: &str, max_edits_override: Option<usize>) -> bool {
    let field_tokens = tokenize_search_terms(field);
    if field_tokens.is_empty() {
        return false;
    }
    let query_tokens = tokenize_search_terms(query);
    if query_tokens.is_empty() {
        return false;
    }

    query_tokens.iter().all(|needle| {
        let max_edits = max_edits_override.unwrap_or_else(|| default_fuzzy_max_edits(needle));
        field_tokens
            .iter()
            .any(|token| levenshtein_within(token, needle, max_edits))
    })
}

fn tokenize_search_terms(input: &str) -> Vec<String> {
    input
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
        .collect()
}

fn default_fuzzy_max_edits(term: &str) -> usize {
    let len = term.chars().count();
    if len <= 2 {
        0
    } else if len <= 5 {
        1
    } else {
        2
    }
}

fn levenshtein_within(a: &str, b: &str, max_edits: usize) -> bool {
    if a == b {
        return true;
    }
    let a_len = a.chars().count();
    let b_len = b.chars().count();
    if a_len.abs_diff(b_len) > max_edits {
        return false;
    }
    if max_edits == 0 {
        return false;
    }

    let b_chars: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b_chars.len()).collect();
    let mut curr = vec![0usize; b_chars.len() + 1];

    for (i, a_char) in a.chars().enumerate() {
        curr[0] = i + 1;
        let mut row_min = curr[0];
        for (j, b_char) in b_chars.iter().enumerate() {
            let cost = usize::from(a_char != *b_char);
            let insert = curr[j] + 1;
            let delete = prev[j + 1] + 1;
            let replace = prev[j] + cost;
            let value = insert.min(delete).min(replace);
            curr[j + 1] = value;
            row_min = row_min.min(value);
        }
        if row_min > max_edits {
            return false;
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b_chars.len()] <= max_edits
}

fn literal_to_array(lit: &Literal, num_rows: usize) -> Result<arrow_array::ArrayRef> {
    literal_utils::literal_to_array(lit, num_rows).map_err(NanoError::Execution)
}

fn compare_arrays(
    left: &arrow_array::ArrayRef,
    right: &arrow_array::ArrayRef,
    op: CompOp,
) -> Result<arrow_array::BooleanArray> {
    use arrow_ord::cmp;
    if op == CompOp::Contains {
        return literal_utils::compare_list_membership(left, right).map_err(NanoError::Execution);
    }
    let result = match op {
        CompOp::Eq => cmp::eq(left, right),
        CompOp::Ne => cmp::neq(left, right),
        CompOp::Gt => cmp::gt(left, right),
        CompOp::Lt => cmp::lt(left, right),
        CompOp::Ge => cmp::gt_eq(left, right),
        CompOp::Le => cmp::lt_eq(left, right),
        CompOp::Contains => unreachable!("handled above"),
    }
    .map_err(|e| NanoError::Execution(format!("comparison error: {}", e)))?;
    Ok(result)
}

fn apply_projection(
    projections: &[IRProjection],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Build output schema from projections
    let sample = &batches[0];
    let mut out_fields = Vec::new();
    for proj in projections {
        let (name, dt, nullable) =
            infer_projection_field(&proj.expr, proj.alias.as_deref(), sample)?;
        out_fields.push(Field::new(name, dt, nullable));
    }
    let out_schema = Arc::new(Schema::new(out_fields));

    let mut result = Vec::new();
    for batch in batches {
        let mut columns = Vec::new();
        for proj in projections {
            let col = eval_ir_expr(&proj.expr, batch, params)?;
            columns.push(col);
        }
        let out_batch = RecordBatch::try_new(out_schema.clone(), columns)
            .map_err(|e| NanoError::Execution(format!("projection error: {}", e)))?;
        result.push(out_batch);
    }
    Ok(result)
}

fn apply_projection_for_alias_ordering(
    projections: &[IRProjection],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    if batches.len() == 1 {
        return apply_projection(projections, batches, params);
    }

    let schema = batches[0].schema();
    let combined = arrow_select::concat::concat_batches(&schema, batches)
        .map_err(|e| NanoError::Execution(e.to_string()))?;
    apply_projection(projections, &[combined], params)
}

fn infer_projection_field(
    expr: &IRExpr,
    alias: Option<&str>,
    batch: &RecordBatch,
) -> Result<(String, DataType, bool)> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let field_name = flat_binding_col(variable, property);
            let batch_schema = batch.schema();
            let field = batch_schema
                .field_with_name(&field_name)
                .or_else(|_| batch_schema.field_with_name(property))
                .map_err(|e| NanoError::Execution(e.to_string()))?;
            let name = alias.unwrap_or(property).to_string();
            Ok((name, field.data_type().clone(), field.is_nullable()))
        }
        IRExpr::Literal(lit) => {
            let name = alias.unwrap_or("literal").to_string();
            let dt = match lit {
                Literal::String(_) => DataType::Utf8,
                Literal::Integer(_) => DataType::Int64,
                Literal::Float(_) => DataType::Float64,
                Literal::Bool(_) => DataType::Boolean,
                Literal::Date(_) => DataType::Date32,
                Literal::DateTime(_) => DataType::Date64,
                Literal::List(_) => DataType::Utf8,
            };
            Ok((name, dt, false))
        }
        IRExpr::Variable(v) => {
            let name = alias.unwrap_or(v).to_string();
            let dt = binding_data_type(batch, v)?;
            let nullable = match &dt {
                DataType::Struct(fields) => fields.iter().any(|field| field.is_nullable()),
                _ => true,
            };
            Ok((name, dt, nullable))
        }
        IRExpr::Param(p) => {
            let (name, data_type, nullable) = if p == NOW_PARAM_NAME {
                (alias.unwrap_or("now").to_string(), DataType::Date64, false)
            } else {
                (alias.unwrap_or(p).to_string(), DataType::Utf8, true)
            };
            Ok((name, data_type, nullable))
        }
        IRExpr::AliasRef(a) => {
            let name = alias.unwrap_or(a).to_string();
            Ok((name, DataType::Int64, true))
        }
        IRExpr::Nearest { .. } => {
            let name = alias.unwrap_or("nearest").to_string();
            Ok((name, DataType::Float64, true))
        }
        IRExpr::Search { .. } => {
            let name = alias.unwrap_or("search").to_string();
            Ok((name, DataType::Boolean, false))
        }
        IRExpr::Fuzzy { .. } => {
            let name = alias.unwrap_or("fuzzy").to_string();
            Ok((name, DataType::Boolean, false))
        }
        IRExpr::MatchText { .. } => {
            let name = alias.unwrap_or("match_text").to_string();
            Ok((name, DataType::Boolean, false))
        }
        IRExpr::Bm25 { .. } => {
            let name = alias.unwrap_or("bm25").to_string();
            Ok((name, DataType::Float64, false))
        }
        IRExpr::Rrf { .. } => {
            let name = alias.unwrap_or("rrf").to_string();
            Ok((name, DataType::Float64, false))
        }
        IRExpr::Aggregate { func, arg } => {
            let name = alias.unwrap_or(&func.to_string()).to_string();
            let dt = match func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Avg => DataType::Float64,
                _ => {
                    // Try to infer from arg
                    let (_, dt, _) = infer_projection_field(arg, None, batch)?;
                    dt
                }
            };
            Ok((name, dt, true))
        }
    }
}

fn apply_aggregation(
    projections: &[IRProjection],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Concatenate all batches
    let schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches[0].clone()
    } else {
        arrow_select::concat::concat_batches(&schema, batches)
            .map_err(|e| NanoError::Execution(e.to_string()))?
    };

    // Identify group-by and aggregate expressions
    let mut group_exprs: Vec<(usize, &IRProjection)> = Vec::new();
    let mut agg_exprs: Vec<(usize, &IRProjection)> = Vec::new();

    for (i, proj) in projections.iter().enumerate() {
        match &proj.expr {
            IRExpr::Aggregate { .. } => agg_exprs.push((i, proj)),
            _ => group_exprs.push((i, proj)),
        }
    }

    // Evaluate group keys
    let mut group_columns: Vec<arrow_array::ArrayRef> = Vec::new();
    for (_, proj) in &group_exprs {
        let col = eval_ir_expr(&proj.expr, &combined, params)?;
        group_columns.push(col);
    }

    // Simple grouping: build a hashmap of group key -> row indices
    let num_rows = combined.num_rows();
    let mut groups: AHashMap<Vec<String>, Vec<usize>> = AHashMap::new();

    for row in 0..num_rows {
        let mut key = Vec::new();
        for gc in &group_columns {
            key.push(array_value_to_string(gc, row));
        }
        groups.entry(key).or_default().push(row);
    }

    // Build output
    let mut output_fields = Vec::new();
    let mut output_columns: Vec<Vec<String>> = Vec::new();
    let mut output_agg_columns: Vec<Vec<f64>> = Vec::new();

    // Initialize output column storage
    for (_, proj) in &group_exprs {
        let (name, dt, nullable) =
            infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        output_fields.push(Field::new(name, dt, nullable));
        output_columns.push(Vec::new());
    }
    for (_, proj) in &agg_exprs {
        let (name, dt, nullable) =
            infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        output_fields.push(Field::new(name, dt, nullable));
        output_agg_columns.push(Vec::new());
    }

    // Compute groups
    let mut group_keys_ordered: Vec<Vec<String>> = groups.keys().cloned().collect();
    group_keys_ordered.sort();

    for key in &group_keys_ordered {
        let rows = &groups[key];

        // Group columns
        for (col_idx, _) in group_exprs.iter().enumerate() {
            output_columns[col_idx].push(key[col_idx].clone());
        }

        // Aggregate columns
        for (agg_idx, (_, proj)) in agg_exprs.iter().enumerate() {
            if let IRExpr::Aggregate { func, arg } = &proj.expr {
                let arg_col = eval_ir_expr(arg, &combined, params)?;
                let value = compute_aggregate(func, &arg_col, rows)?;
                output_agg_columns[agg_idx].push(value);
            }
        }
    }

    // Convert to RecordBatch
    let out_schema = Arc::new(Schema::new(output_fields.clone()));
    let mut arrays: Vec<arrow_array::ArrayRef> = Vec::new();

    for (col_idx, (_, proj)) in group_exprs.iter().enumerate() {
        let (_, dt, _) = infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        let arr = strings_to_array(&output_columns[col_idx], &dt);
        arrays.push(arr);
    }

    for (agg_idx, (_, proj)) in agg_exprs.iter().enumerate() {
        let (_, dt, _) = infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        let arr = match dt {
            DataType::Int64 => Arc::new(arrow_array::Int64Array::from(
                output_agg_columns[agg_idx]
                    .iter()
                    .map(|v| *v as i64)
                    .collect::<Vec<_>>(),
            )) as arrow_array::ArrayRef,
            DataType::Float64 => Arc::new(arrow_array::Float64Array::from(
                output_agg_columns[agg_idx].clone(),
            )) as arrow_array::ArrayRef,
            _ => Arc::new(arrow_array::Int64Array::from(
                output_agg_columns[agg_idx]
                    .iter()
                    .map(|v| *v as i64)
                    .collect::<Vec<_>>(),
            )) as arrow_array::ArrayRef,
        };
        arrays.push(arr);
    }

    let out_batch = RecordBatch::try_new(out_schema, arrays)
        .map_err(|e| NanoError::Execution(e.to_string()))?;

    Ok(vec![out_batch])
}

fn compute_aggregate(func: &AggFunc, col: &arrow_array::ArrayRef, rows: &[usize]) -> Result<f64> {
    match func {
        AggFunc::Count => Ok(rows.len() as f64),
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
            let mut values = Vec::new();
            for &row in rows {
                if let Some(v) = array_value_to_f64(col, row) {
                    values.push(v);
                }
            }
            if values.is_empty() {
                return match func {
                    AggFunc::Sum => Ok(0.0),
                    _ => Ok(f64::NAN), // min/max/avg of nothing = NaN
                };
            }
            match func {
                AggFunc::Sum => Ok(values.iter().sum()),
                AggFunc::Avg => Ok(values.iter().sum::<f64>() / values.len() as f64),
                AggFunc::Min => Ok(values.iter().cloned().fold(f64::INFINITY, f64::min)),
                AggFunc::Max => Ok(values.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
                _ => unreachable!(),
            }
        }
    }
}

fn array_value_to_string(arr: &arrow_array::ArrayRef, row: usize) -> String {
    use arrow_array::*;
    if arr.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return a.value(row).to_string();
    }
    "?".to_string()
}

fn array_value_to_f64(arr: &arrow_array::ArrayRef, row: usize) -> Option<f64> {
    use arrow_array::*;
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Some(a.value(row) as f64);
    }
    None
}

fn strings_to_array(values: &[String], dt: &DataType) -> arrow_array::ArrayRef {
    use arrow_array::{Int32Array, Int64Array, StringArray, UInt64Array};
    match dt {
        DataType::Utf8 => Arc::new(StringArray::from(
            values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values
                .iter()
                .map(|s| s.parse::<i32>().unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|s| s.parse::<i64>().unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            values
                .iter()
                .map(|s| s.parse::<u64>().unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
        _ => Arc::new(StringArray::from(
            values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
    }
}

fn apply_order_and_limit(
    batches: &[RecordBatch],
    order_by: &[IROrdering],
    limit: Option<u64>,
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Concat all batches
    let schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches[0].clone()
    } else {
        arrow_select::concat::concat_batches(&schema, batches)
            .map_err(|e| NanoError::Execution(e.to_string()))?
    };

    if combined.num_rows() == 0 {
        return Ok(vec![combined]);
    }

    let mut result = combined;

    // Apply ordering
    if !order_by.is_empty() {
        // Build sort columns
        let mut sort_columns = Vec::new();
        let mut nearest_tie_break_var: Option<String> = None;
        for ord in order_by {
            let col = match &ord.expr {
                IRExpr::PropAccess { variable, property } => {
                    sort_column_from_prop_or_flat(&result, variable, property)
                }
                IRExpr::AliasRef(name) => result
                    .schema()
                    .index_of(name)
                    .ok()
                    .map(|i| result.column(i).clone()),
                IRExpr::Nearest {
                    variable,
                    property,
                    query,
                } => {
                    nearest_tie_break_var.get_or_insert_with(|| variable.clone());
                    Some(compute_nearest_distance_column(
                        &result, variable, property, query, params,
                    )?)
                }
                _ => eval_ir_expr(&ord.expr, &result, params).ok(),
            };

            if let Some(c) = col {
                sort_columns.push(arrow_ord::sort::SortColumn {
                    values: c,
                    options: Some(arrow_ord::sort::SortOptions {
                        descending: ord.descending,
                        nulls_first: false,
                    }),
                });
            }
        }

        // Deterministic tie-break for nearest: id ascending.
        if let Some(var) = nearest_tie_break_var
            && let Some(id_col) = sort_column_from_prop_or_flat(&result, &var, "id")
        {
            sort_columns.push(arrow_ord::sort::SortColumn {
                values: id_col,
                options: Some(arrow_ord::sort::SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            });
        }

        if !sort_columns.is_empty() {
            let indices = arrow_ord::sort::lexsort_to_indices(&sort_columns, None)
                .map_err(|e| NanoError::Execution(e.to_string()))?;

            let mut new_columns = Vec::new();
            for col in result.columns() {
                let taken = arrow_select::take::take(col.as_ref(), &indices, None)
                    .map_err(|e| NanoError::Execution(e.to_string()))?;
                new_columns.push(taken);
            }
            result = RecordBatch::try_new(result.schema(), new_columns)
                .map_err(|e| NanoError::Execution(e.to_string()))?;
        }
    }

    // Apply limit
    if let Some(limit) = limit {
        let limit = limit as usize;
        if result.num_rows() > limit {
            result = result.slice(0, limit);
        }
    }

    Ok(vec![result])
}

fn sort_column_from_prop_or_flat(
    batch: &RecordBatch,
    variable: &str,
    property: &str,
) -> Option<arrow_array::ArrayRef> {
    if let Ok(col) = binding_property_array(batch, variable, property) {
        return Some(col);
    }
    batch
        .schema()
        .index_of(property)
        .ok()
        .map(|i| batch.column(i).clone())
}

fn compute_nearest_distance_column(
    batch: &RecordBatch,
    variable: &str,
    property: &str,
    query_expr: &IRExpr,
    params: &ParamMap,
) -> Result<arrow_array::ArrayRef> {
    let vector_col = binding_property_array(batch, variable, property)?;
    let vector_arr = vector_col
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            NanoError::Execution(format!(
                "nearest() property `{}` on `{}` must be Vector(dim)",
                property, variable
            ))
        })?;

    let dim = vector_arr.value_length() as usize;
    let query = resolve_nearest_query_vector(query_expr, params, dim)?;
    let query_norm = query
        .iter()
        .map(|v| (*v as f64) * (*v as f64))
        .sum::<f64>()
        .sqrt();

    let mut distances = Vec::with_capacity(vector_arr.len());
    for row in 0..vector_arr.len() {
        if vector_arr.is_null(row) {
            distances.push(None);
            continue;
        }
        let values = vector_arr.value(row);
        let vec = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                NanoError::Execution(format!(
                    "nearest() vector field `{}` is not Float32",
                    property
                ))
            })?;
        if vec.len() != dim {
            return Err(NanoError::Execution(format!(
                "nearest() vector field `{}` dimension mismatch at row {}: expected {}, got {}",
                property,
                row,
                dim,
                vec.len()
            )));
        }

        let mut dot = 0.0f64;
        let mut vec_norm_sq = 0.0f64;
        for (i, a) in query.iter().copied().enumerate().take(dim) {
            let a = a as f64;
            let b = vec.value(i) as f64;
            dot += a * b;
            vec_norm_sq += b * b;
        }
        // Float32 vector inputs can produce small unstable norms above machine epsilon.
        let denom = query_norm * vec_norm_sq.sqrt();
        let dist = if denom > 1e-10 {
            1.0 - (dot / denom)
        } else {
            f64::INFINITY
        };
        distances.push(Some(dist));
    }

    Ok(Arc::new(Float64Array::from(distances)))
}

fn resolve_nearest_query_vector(
    expr: &IRExpr,
    params: &ParamMap,
    expected_dim: usize,
) -> Result<Vec<f32>> {
    let lit = match expr {
        IRExpr::Literal(lit) => lit,
        IRExpr::Param(name) => params
            .get(name)
            .ok_or_else(|| NanoError::Execution(format!("parameter `${}` not provided", name)))?,
        _ => {
            return Err(NanoError::Execution(
                "nearest() query must be a Vector or String literal/parameter".to_string(),
            ));
        }
    };
    literal_to_query_vector(lit, expected_dim)
}

fn literal_to_query_vector(lit: &Literal, expected_dim: usize) -> Result<Vec<f32>> {
    let vec = match lit {
        Literal::List(items) => literal_list_to_f32_vector(items)?,
        Literal::String(_) => {
            return Err(NanoError::Execution(
                "nearest() string query was not pre-resolved to an embedding vector".to_string(),
            ));
        }
        _ => {
            return Err(NanoError::Execution(
                "nearest() query must be provided as a numeric vector or string".to_string(),
            ));
        }
    };
    if vec.len() != expected_dim {
        return Err(NanoError::Execution(format!(
            "nearest() query dimension mismatch: expected {}, got {}",
            expected_dim,
            vec.len()
        )));
    }
    Ok(vec)
}

fn literal_list_to_f32_vector(items: &[Literal]) -> Result<Vec<f32>> {
    if items.is_empty() {
        return Err(NanoError::Execution(
            "nearest() vector cannot be empty".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Literal::Integer(v) => out.push(*v as f32),
            Literal::Float(v) => out.push(*v as f32),
            _ => {
                return Err(NanoError::Execution(
                    "nearest() vector elements must be numeric".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

/// A simple cross-join exec for combining multiple NodeScans
#[derive(Debug)]
struct CrossJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl CrossJoinExec {
    fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion_physical_plan::execution_plan::EmissionType::Incremental,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            left,
            right,
            output_schema,
            properties,
        }
    }
}

impl datafusion_physical_plan::DisplayAs for CrossJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CrossJoinExec")
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn name(&self) -> &str {
        "CrossJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CrossJoinExec::new(
            children[0].clone(),
            children[1].clone(),
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        enum CrossJoinState {
            Init {
                left_stream: SendableRecordBatchStream,
                right_stream: SendableRecordBatchStream,
                schema: SchemaRef,
            },
            Running {
                left_stream: SendableRecordBatchStream,
                right_batches: Vec<RecordBatch>,
                current_left: Option<RecordBatch>,
                right_idx: usize,
                schema: SchemaRef,
            },
        }

        let left_stream = self.left.execute(partition, context.clone())?;
        let right_stream = self.right.execute(partition, context)?;

        let init = CrossJoinState::Init {
            left_stream,
            right_stream,
            schema: self.output_schema.clone(),
        };

        let stream = futures::stream::try_unfold(init, |state| async move {
            use futures::StreamExt;

            let mut state = state;
            loop {
                match state {
                    CrossJoinState::Init {
                        left_stream,
                        mut right_stream,
                        schema,
                    } => {
                        let mut right_batches = Vec::new();
                        while let Some(batch) = right_stream.next().await {
                            right_batches.push(batch?);
                        }

                        if right_batches.is_empty() {
                            return Ok(None);
                        }

                        state = CrossJoinState::Running {
                            left_stream,
                            right_batches,
                            current_left: None,
                            right_idx: 0,
                            schema,
                        };
                    }
                    CrossJoinState::Running {
                        mut left_stream,
                        right_batches,
                        mut current_left,
                        mut right_idx,
                        schema,
                    } => {
                        if current_left.is_none() {
                            let next_left = match left_stream.next().await {
                                Some(batch) => batch?,
                                None => return Ok(None),
                            };
                            current_left = Some(next_left);
                            right_idx = 0;
                        }

                        let left_batch = current_left.as_ref().expect("left batch set");

                        while right_idx < right_batches.len() {
                            let right_batch = &right_batches[right_idx];
                            right_idx += 1;
                            let cross = cross_join_batches(left_batch, right_batch, &schema)?;
                            if cross.num_rows() > 0 {
                                let next_state = CrossJoinState::Running {
                                    left_stream,
                                    right_batches,
                                    current_left,
                                    right_idx,
                                    schema,
                                };
                                return Ok(Some((cross, next_state)));
                            }
                        }

                        state = CrossJoinState::Running {
                            left_stream,
                            right_batches,
                            current_left: None,
                            right_idx: 0,
                            schema,
                        };
                    }
                }
            }
        });

        Ok(Box::pin(
            datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                stream,
            ),
        ))
    }
}

fn cross_join_batches(
    left: &RecordBatch,
    right: &RecordBatch,
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let left_rows = left.num_rows();
    let right_rows = right.num_rows();
    let total = left_rows * right_rows;

    if total == 0 {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<arrow_array::ArrayRef> = Vec::new();

    // Replicate left columns
    for col in left.columns() {
        let mut indices = Vec::with_capacity(total);
        for i in 0..left_rows {
            for _ in 0..right_rows {
                indices.push(i as u64);
            }
        }
        let idx = arrow_array::UInt64Array::from(indices);
        let taken = arrow_select::take::take(col.as_ref(), &idx, None)?;
        columns.push(taken);
    }

    // Replicate right columns
    for col in right.columns() {
        let mut indices = Vec::with_capacity(total);
        for _ in 0..left_rows {
            for j in 0..right_rows {
                indices.push(j as u64);
            }
        }
        let idx = arrow_array::UInt64Array::from(indices);
        let taken = arrow_select::take::take(col.as_ref(), &idx, None)?;
        columns.push(taken);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))
}

/// Anti-join exec: returns rows from left where no matching rows exist in right
#[derive(Debug)]
struct AntiJoinExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    join_var: String,
    inner_pipeline: Vec<IROp>,
    params: ParamMap,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl AntiJoinExec {
    fn new(
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
        join_var: String,
        inner_pipeline: Vec<IROp>,
        params: ParamMap,
    ) -> Self {
        let output_schema = outer.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion_physical_plan::execution_plan::EmissionType::Incremental,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            outer,
            inner,
            join_var,
            inner_pipeline,
            params,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for AntiJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AntiJoinExec on ${}", self.join_var)
    }
}

impl ExecutionPlan for AntiJoinExec {
    fn name(&self) -> &str {
        "AntiJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.outer, &self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AntiJoinExec::new(
            children[0].clone(),
            children[1].clone(),
            self.join_var.clone(),
            self.inner_pipeline.clone(),
            self.params.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        enum AntiJoinState {
            Init {
                outer_stream: SendableRecordBatchStream,
                inner_stream: SendableRecordBatchStream,
                join_var: String,
                inner_pipeline: Vec<IROp>,
                params: ParamMap,
            },
            Running {
                outer_stream: SendableRecordBatchStream,
                inner_ids: AHashSet<u64>,
                join_var: String,
            },
        }

        let outer_stream = self.outer.execute(partition, context.clone())?;
        let inner_stream = self.inner.execute(partition, context)?;

        let init = AntiJoinState::Init {
            outer_stream,
            inner_stream,
            join_var: self.join_var.clone(),
            inner_pipeline: self.inner_pipeline.clone(),
            params: self.params.clone(),
        };

        let stream = futures::stream::try_unfold(init, |state| async move {
            use futures::StreamExt;

            let mut state = state;
            loop {
                match state {
                    AntiJoinState::Init {
                        outer_stream,
                        mut inner_stream,
                        join_var,
                        inner_pipeline,
                        params,
                    } => {
                        let mut inner_ids: AHashSet<u64> = AHashSet::new();

                        while let Some(batch) = inner_stream.next().await {
                            let batch = batch?;
                            let filtered_batches =
                                apply_ir_filters(&inner_pipeline, &[batch], &params).map_err(
                                    |e| {
                                        datafusion_common::DataFusionError::Execution(e.to_string())
                                    },
                                )?;

                            for filtered in filtered_batches {
                                if let Ok(id_col) =
                                    binding_property_array(&filtered, &join_var, "id")
                                    && let Some(id_arr) =
                                        id_col.as_any().downcast_ref::<arrow_array::UInt64Array>()
                                {
                                    for i in 0..id_arr.len() {
                                        inner_ids.insert(id_arr.value(i));
                                    }
                                }
                            }
                        }

                        state = AntiJoinState::Running {
                            outer_stream,
                            inner_ids,
                            join_var,
                        };
                    }
                    AntiJoinState::Running {
                        mut outer_stream,
                        inner_ids,
                        join_var,
                    } => {
                        while let Some(batch) = outer_stream.next().await {
                            let batch = batch?;
                            let id_col =
                                binding_property_array(&batch, &join_var, "id").map_err(|e| {
                                    datafusion_common::DataFusionError::Execution(e.to_string())
                                })?;
                            let id_arr = id_col
                                .as_any()
                                .downcast_ref::<arrow_array::UInt64Array>()
                                .ok_or_else(|| {
                                    datafusion_common::DataFusionError::Execution(format!(
                                        "{} id field is not UInt64",
                                        join_var
                                    ))
                                })?;

                            let mask = arrow_array::BooleanArray::from(
                                (0..id_arr.len())
                                    .map(|i| !inner_ids.contains(&id_arr.value(i)))
                                    .collect::<Vec<_>>(),
                            );
                            let filtered =
                                arrow_select::filter::filter_record_batch(&batch, &mask)?;
                            if filtered.num_rows() > 0 {
                                let next_state = AntiJoinState::Running {
                                    outer_stream,
                                    inner_ids,
                                    join_var,
                                };
                                return Ok(Some((filtered, next_state)));
                            }
                        }

                        return Ok(None);
                    }
                }
            }
        });

        Ok(Box::pin(
            datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                stream,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::Literal;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::{
        ArrayRef, Date32Array, Date64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn pushdown_marks_index_eligible_for_indexed_property() {
        let filter = IRFilter {
            left: IRExpr::PropAccess {
                variable: "p".to_string(),
                property: "name".to_string(),
            },
            op: CompOp::Eq,
            right: IRExpr::Literal(Literal::String("Alice".to_string())),
        };
        let mut indexed_props = HashSet::new();
        indexed_props.insert("name".to_string());

        let predicates =
            build_scan_pushdown_filters("p", &[filter], &ParamMap::new(), Some(&indexed_props));
        assert_eq!(predicates.len(), 1);
        assert!(predicates[0].index_eligible);
    }

    #[test]
    fn pushdown_marks_non_indexed_property_as_not_index_eligible() {
        let filter = IRFilter {
            left: IRExpr::PropAccess {
                variable: "p".to_string(),
                property: "age".to_string(),
            },
            op: CompOp::Eq,
            right: IRExpr::Literal(Literal::Integer(30)),
        };
        let mut indexed_props = HashSet::new();
        indexed_props.insert("name".to_string());

        let predicates =
            build_scan_pushdown_filters("p", &[filter], &ParamMap::new(), Some(&indexed_props));
        assert_eq!(predicates.len(), 1);
        assert!(!predicates[0].index_eligible);
    }

    #[test]
    fn pushdown_marks_not_equal_as_not_index_eligible() {
        let filter = IRFilter {
            left: IRExpr::PropAccess {
                variable: "p".to_string(),
                property: "name".to_string(),
            },
            op: CompOp::Ne,
            right: IRExpr::Literal(Literal::String("Alice".to_string())),
        };
        let mut indexed_props = HashSet::new();
        indexed_props.insert("name".to_string());

        let predicates =
            build_scan_pushdown_filters("p", &[filter], &ParamMap::new(), Some(&indexed_props));
        assert_eq!(predicates.len(), 1);
        assert!(!predicates[0].index_eligible);
    }

    #[test]
    fn literal_to_array_builds_native_temporal_arrays() {
        let date_arr = literal_to_array(&Literal::Date("2026-02-14".to_string()), 2).unwrap();
        assert_eq!(date_arr.data_type(), &DataType::Date32);
        let date_arr = date_arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(date_arr.len(), 2);
        assert_eq!(date_arr.value(0), date_arr.value(1));

        let dt_arr =
            literal_to_array(&Literal::DateTime("2026-02-14T10:00:00Z".to_string()), 3).unwrap();
        assert_eq!(dt_arr.data_type(), &DataType::Date64);
        let dt_arr = dt_arr.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(dt_arr.len(), 3);
        assert_eq!(dt_arr.value(0), dt_arr.value(2));
    }

    #[test]
    fn literal_to_array_rejects_invalid_temporal_literals() {
        let date_err = literal_to_array(&Literal::Date("not-a-date".to_string()), 1).unwrap_err();
        assert!(date_err.to_string().contains("invalid Date literal"));

        let dt_err =
            literal_to_array(&Literal::DateTime("not-a-datetime".to_string()), 1).unwrap_err();
        assert!(dt_err.to_string().contains("invalid DateTime literal"));
    }

    #[test]
    fn infer_projection_field_uses_temporal_types_for_temporal_literals() {
        let batch =
            RecordBatch::new_empty(Arc::new(Schema::new(Vec::<arrow_schema::Field>::new())));

        let (_, date_ty, _) = infer_projection_field(
            &IRExpr::Literal(Literal::Date("2026-02-14".to_string())),
            Some("d"),
            &batch,
        )
        .unwrap();
        assert_eq!(date_ty, DataType::Date32);

        let (_, dt_ty, _) = infer_projection_field(
            &IRExpr::Literal(Literal::DateTime("2026-02-14T10:00:00Z".to_string())),
            Some("ts"),
            &batch,
        )
        .unwrap();
        assert_eq!(dt_ty, DataType::Date64);
    }

    #[test]
    fn evaluate_search_boolean_array_matches_all_query_tokens() {
        let field: ArrayRef = Arc::new(StringArray::from(vec![
            "billing reconciliation delay due to missing invoice data",
            "warm referral for analytics migration project",
        ]));
        let query: ArrayRef = Arc::new(StringArray::from(vec![
            "billing missing",
            "analytics migration",
        ]));

        let out = evaluate_search_boolean_array(&field, &query).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(out.value(1));
    }

    #[test]
    fn evaluate_match_text_boolean_array_matches_contiguous_token_phrase() {
        let field: ArrayRef = Arc::new(StringArray::from(vec![
            "enterprise procurement questionnaire backlog",
            "warm referral for analytics migration project",
        ]));
        let query: ArrayRef = Arc::new(StringArray::from(vec![
            "procurement backlog",
            "analytics migration",
        ]));

        let out = evaluate_match_text_boolean_array(&field, &query).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!out.value(0));
        assert!(out.value(1));
    }

    #[test]
    fn evaluate_match_text_and_search_have_distinct_semantics() {
        let field: ArrayRef = Arc::new(StringArray::from(vec![
            "enterprise procurement questionnaire backlog",
        ]));
        let query: ArrayRef = Arc::new(StringArray::from(vec!["procurement backlog"]));

        let search = evaluate_search_boolean_array(&field, &query).unwrap();
        let search = search.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(search.value(0));

        let match_text = evaluate_match_text_boolean_array(&field, &query).unwrap();
        let match_text = match_text.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!match_text.value(0));
    }

    #[test]
    fn evaluate_bm25_score_array_ranks_relevant_document_higher() {
        let field: ArrayRef = Arc::new(StringArray::from(vec![
            "billing reconciliation delay due to missing invoice data",
            "warm referral for analytics migration project",
            "vendor procurement backlog and mitigation tracking",
        ]));
        let query: ArrayRef = Arc::new(StringArray::from(vec![
            "billing missing invoice",
            "billing missing invoice",
            "billing missing invoice",
        ]));

        let out = evaluate_bm25_score_array(&field, &query).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(out.value(0) > out.value(1));
        assert!(out.value(0) > out.value(2));
    }

    #[test]
    fn alias_order_projection_combines_batches_for_global_scores() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "billing invoice reconciliation delay",
                "warm referral for analytics migration project",
            ])) as ArrayRef],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "billing invoice dispute with procurement owner",
                "customer onboarding checklist",
            ])) as ArrayRef],
        )
        .unwrap();

        let projections = vec![IRProjection {
            expr: IRExpr::Bm25 {
                field: Box::new(IRExpr::Variable("text".to_string())),
                query: Box::new(IRExpr::Literal(Literal::String(
                    "billing invoice".to_string(),
                ))),
            },
            alias: Some("score".to_string()),
        }];

        let split_projected = apply_projection(
            &projections,
            &[batch1.clone(), batch2.clone()],
            &ParamMap::new(),
        )
        .unwrap();
        assert_eq!(split_projected.len(), 2);

        let global_projected =
            apply_projection_for_alias_ordering(&projections, &[batch1, batch2], &ParamMap::new())
                .unwrap();
        assert_eq!(global_projected.len(), 1);
        assert_eq!(global_projected[0].num_rows(), 4);
        assert_eq!(global_projected[0].schema().field(0).name(), "score");
    }

    #[test]
    fn evaluate_rrf_score_array_fuses_two_rankings() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let text: ArrayRef = Arc::new(StringArray::from(vec![
            "billing invoice reconciliation delay",
            "billing invoice for customer account",
            "warm referral for analytics migration",
        ]));
        let batch = RecordBatch::try_new(schema, vec![text]).unwrap();

        let primary = IRExpr::Bm25 {
            field: Box::new(IRExpr::Variable("text".to_string())),
            query: Box::new(IRExpr::Literal(Literal::String(
                "billing invoice".to_string(),
            ))),
        };
        let secondary = IRExpr::Bm25 {
            field: Box::new(IRExpr::Variable("text".to_string())),
            query: Box::new(IRExpr::Literal(Literal::String(
                "reconciliation delay".to_string(),
            ))),
        };

        let out =
            evaluate_rrf_score_array(&primary, &secondary, None, &batch, &ParamMap::new()).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(out.value(0) > out.value(1));
        assert!(out.value(0) > out.value(2));
    }

    #[test]
    fn evaluate_rrf_rejects_zero_k() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let text: ArrayRef = Arc::new(StringArray::from(vec!["billing invoice"]));
        let batch = RecordBatch::try_new(schema, vec![text]).unwrap();

        let primary = IRExpr::Bm25 {
            field: Box::new(IRExpr::Variable("text".to_string())),
            query: Box::new(IRExpr::Literal(Literal::String(
                "billing invoice".to_string(),
            ))),
        };
        let secondary = IRExpr::Bm25 {
            field: Box::new(IRExpr::Variable("text".to_string())),
            query: Box::new(IRExpr::Literal(Literal::String(
                "billing invoice".to_string(),
            ))),
        };
        let k = IRExpr::Literal(Literal::Integer(0));

        let err =
            evaluate_rrf_score_array(&primary, &secondary, Some(&k), &batch, &ParamMap::new())
                .unwrap_err();
        assert!(err.to_string().contains("rrf() k must be greater than 0"));
    }

    #[test]
    fn evaluate_fuzzy_boolean_array_handles_typo_and_max_edits() {
        let field: ArrayRef = Arc::new(StringArray::from(vec![
            "enterprise procurement questionnaire backlog",
            "billing reconciliation delay",
        ]));
        let query: ArrayRef = Arc::new(StringArray::from(vec![
            "procuremnt backlog",
            "reconciliaton",
        ]));

        let out = evaluate_fuzzy_boolean_array(&field, &query, None).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(out.value(1));

        let strict_edits: ArrayRef = Arc::new(Int64Array::from(vec![0_i64, 0_i64]));
        let strict = evaluate_fuzzy_boolean_array(&field, &query, Some(&strict_edits)).unwrap();
        let strict = strict.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!strict.value(0));
        assert!(!strict.value(1));
    }

    #[test]
    fn evaluate_fuzzy_boolean_array_rejects_negative_max_edits() {
        let field: ArrayRef = Arc::new(StringArray::from(vec!["alpha"]));
        let query: ArrayRef = Arc::new(StringArray::from(vec!["alpah"]));
        let max_edits: ArrayRef = Arc::new(Int64Array::from(vec![-1_i64]));

        let err = evaluate_fuzzy_boolean_array(&field, &query, Some(&max_edits)).unwrap_err();
        assert!(err.to_string().contains("max_edits must be >= 0"));
    }

    #[test]
    fn resolve_nearest_query_vector_rejects_unresolved_string_query() {
        let mut params = ParamMap::new();
        params.insert("q".to_string(), Literal::String("alpha".to_string()));
        let query = IRExpr::Param("q".to_string());

        let err = resolve_nearest_query_vector(&query, &params, 5).unwrap_err();
        assert!(
            err.to_string()
                .contains("nearest() string query was not pre-resolved")
        );
    }

    #[test]
    fn apply_order_and_limit_nearest_tie_breaks_by_id() {
        let ids: ArrayRef = Arc::new(UInt64Array::from(vec![20_u64, 10_u64, 30_u64]));
        let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        // id=20 -> cosine distance 1.0 from query [1, 0]
        emb_builder.values().append_value(0.0);
        emb_builder.values().append_value(1.0);
        emb_builder.append(true);
        // id=10 -> cosine distance 1.0 from query [1, 0]
        emb_builder.values().append_value(0.0);
        emb_builder.values().append_value(-1.0);
        emb_builder.append(true);
        // id=30 -> cosine distance 0.0 from query [1, 0]
        emb_builder.values().append_value(1.0);
        emb_builder.values().append_value(0.0);
        emb_builder.append(true);
        let embeddings: ArrayRef = Arc::new(emb_builder.finish());

        let node_fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            )),
        ]);
        let node_struct = Arc::new(arrow_array::StructArray::new(
            node_fields,
            vec![ids, embeddings],
            None,
        )) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "n",
            node_struct.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![node_struct]).unwrap();

        let ordering = vec![IROrdering {
            expr: IRExpr::Nearest {
                variable: "n".to_string(),
                property: "embedding".to_string(),
                query: Box::new(IRExpr::Literal(Literal::List(vec![
                    Literal::Float(1.0),
                    Literal::Float(0.0),
                ]))),
            },
            descending: false,
        }];

        let out = apply_order_and_limit(&[batch], &ordering, Some(3), &ParamMap::new()).unwrap();
        let struct_arr = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        let out_ids = struct_arr
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        // nearest distance asc with deterministic tie-break by id asc for equal distances.
        assert_eq!(
            vec![out_ids.value(0), out_ids.value(1), out_ids.value(2)],
            vec![30, 10, 20]
        );
    }

    #[test]
    fn compute_nearest_distance_column_preserves_null_vectors() {
        let ids: ArrayRef = Arc::new(UInt64Array::from(vec![1_u64, 2_u64]));
        let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        emb_builder.values().append_null();
        emb_builder.values().append_null();
        emb_builder.append(false);
        emb_builder.values().append_value(1.0);
        emb_builder.values().append_value(0.0);
        emb_builder.append(true);
        let embeddings: ArrayRef = Arc::new(emb_builder.finish());

        let node_fields = Fields::from(vec![
            Arc::new(Field::new("id", DataType::UInt64, false)),
            Arc::new(Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            )),
        ]);
        let node_struct = Arc::new(arrow_array::StructArray::new(
            node_fields,
            vec![ids, embeddings],
            None,
        )) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "n",
            node_struct.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![node_struct]).unwrap();

        let distances = compute_nearest_distance_column(
            &batch,
            "n",
            "embedding",
            &IRExpr::Literal(Literal::List(vec![
                Literal::Float(1.0),
                Literal::Float(0.0),
            ])),
            &ParamMap::new(),
        )
        .unwrap();

        let distances = distances.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(distances.is_null(0));
        assert!((distances.value(1) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn analyze_tail_strategy_supports_simple_relational_tail() {
        let ir = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "p".to_string(),
                type_name: "Person".to_string(),
                filters: vec![IRFilter {
                    left: IRExpr::PropAccess {
                        variable: "p".to_string(),
                        property: "age".to_string(),
                    },
                    op: CompOp::Gt,
                    right: IRExpr::Literal(Literal::Integer(30)),
                }],
            }],
            return_exprs: vec![IRProjection {
                expr: IRExpr::PropAccess {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                alias: None,
            }],
            order_by: vec![IROrdering {
                expr: IRExpr::PropAccess {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                descending: false,
            }],
            limit: Some(10),
        };

        assert_eq!(
            analyze_tail_strategy(&ir),
            TailStrategy::DataFusionNonAggregate {
                alias_ordering: false,
                final_variable_projection: false,
            }
        );
    }

    #[test]
    fn analyze_tail_strategy_supports_variable_projection_with_final_materialization() {
        let ir = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "p".to_string(),
                type_name: "Person".to_string(),
                filters: vec![],
            }],
            return_exprs: vec![IRProjection {
                expr: IRExpr::Variable("p".to_string()),
                alias: None,
            }],
            order_by: vec![IROrdering {
                expr: IRExpr::PropAccess {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                descending: false,
            }],
            limit: Some(10),
        };

        assert_eq!(
            analyze_tail_strategy(&ir),
            TailStrategy::DataFusionNonAggregate {
                alias_ordering: false,
                final_variable_projection: true,
            }
        );
    }

    #[test]
    fn analyze_tail_strategy_supports_aggregate_alias_ordering() {
        let ir = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "p".to_string(),
                type_name: "Person".to_string(),
                filters: vec![],
            }],
            return_exprs: vec![
                IRProjection {
                    expr: IRExpr::PropAccess {
                        variable: "p".to_string(),
                        property: "name".to_string(),
                    },
                    alias: None,
                },
                IRProjection {
                    expr: IRExpr::Aggregate {
                        func: AggFunc::Count,
                        arg: Box::new(IRExpr::Variable("p".to_string())),
                    },
                    alias: Some("friends".to_string()),
                },
            ],
            order_by: vec![IROrdering {
                expr: IRExpr::AliasRef("friends".to_string()),
                descending: true,
            }],
            limit: Some(5),
        };

        assert_eq!(
            analyze_tail_strategy(&ir),
            TailStrategy::DataFusionAggregate
        );
    }

    #[test]
    fn analyze_tail_strategy_falls_back_for_nearest() {
        let ir = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "p".to_string(),
                type_name: "Person".to_string(),
                filters: vec![],
            }],
            return_exprs: vec![IRProjection {
                expr: IRExpr::PropAccess {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                alias: None,
            }],
            order_by: vec![IROrdering {
                expr: IRExpr::Nearest {
                    variable: "p".to_string(),
                    property: "embedding".to_string(),
                    query: Box::new(IRExpr::Literal(Literal::List(vec![
                        Literal::Float(1.0),
                        Literal::Float(0.0),
                    ]))),
                },
                descending: false,
            }],
            limit: Some(5),
        };

        assert_eq!(analyze_tail_strategy(&ir), TailStrategy::Legacy);
    }

    #[test]
    fn analyze_tail_strategy_falls_back_for_contains() {
        let ir = QueryIR {
            name: "q".to_string(),
            params: vec![],
            pipeline: vec![
                IROp::NodeScan {
                    variable: "p".to_string(),
                    type_name: "Person".to_string(),
                    filters: vec![],
                },
                IROp::Filter(IRFilter {
                    left: IRExpr::PropAccess {
                        variable: "p".to_string(),
                        property: "tags".to_string(),
                    },
                    op: CompOp::Contains,
                    right: IRExpr::Literal(Literal::String("graph".to_string())),
                }),
            ],
            return_exprs: vec![IRProjection {
                expr: IRExpr::PropAccess {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };

        assert_eq!(analyze_tail_strategy(&ir), TailStrategy::Legacy);
    }
}
