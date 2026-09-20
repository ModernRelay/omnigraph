//! The read engine selected by the session setting `engine = v2`. The
//! planner's physical plan lowers to a DataFusion plan executed under the
//! query's context and memory pool. Relational nodes use DataFusion
//! operators; scans and graph nodes use this module's operators.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
    builder::{
        BooleanBuilder, Date32Builder, Date64Builder, Float64Builder, Int32Builder, Int64Builder,
        ListBuilder, StringBuilder,
    },
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, IRFilter, IROp, IROrdering, IRProjection, ParamMap, QueryIR};
use omnigraph_compiler::query::ast::{AggFunc, CompOp, Literal};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::settings::{RrfPlan, SessionSettings, Traversal};
use omnigraph_compiler::types::Direction;
use omnigraph_compiler::types::ScalarType;

use crate::db::{Omnigraph, Snapshot};
use crate::embedding::EmbeddingClient;
use crate::error::{OmniError, Result};
use crate::graph_index::GraphIndex;
use crate::instrumentation::{
    RrfGateFallback, RrfGatePlan, RrfGateVerdict, record_ann_prefilter_verdict,
    record_rrf_gate_verdict,
};

mod adapters;
mod context;
mod explain;
mod expr;
mod graph;
mod lower;
mod operators;
mod plan_source;
/// The push-based pipeline for change-feed and merge plans; the planner's
/// registry routes neither operation to it.
#[expect(
    dead_code,
    reason = "change-feed and merge plans have no engine callers; reads run through engine/run.rs"
)]
pub(crate) mod push;
mod run;
mod scan;
mod search;

use expr::*;
use scan::*;
use search::*;

use context::QueryContext;
pub(crate) use explain::{explain_document, explain_rows};
pub(crate) use graph::{EmbeddingResolver, GraphIndexHandle};
use lower::{Lowering, RunMode};
pub(crate) use plan_source::plan_query;
use run::run_plan;
pub(crate) use search::referenced_edge_types;

/// One pass of the tree under `mode`: lower it and run it under `ctx`.
/// `execute_query` reruns it under a widened mode when a capped search
/// under-fills.
async fn run_once(
    lowering: &Lowering<'_>,
    ctx: &QueryContext,
    mode: &SearchMode,
) -> Result<(RecordBatch, ScanReport)> {
    let lowered = lowering.lower_query(&RunMode::Single(mode))?;
    lowered.record_in_memory_filters();
    let batch = run_plan(&lowered.root, ctx).await?;
    let report = *lowered
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok((batch, report))
}

/// A query's parameters as `resolve_params` binds them; the planner and the
/// lowering take no other form.
pub(crate) struct ResolvedParams(Arc<ParamMap>);

impl ResolvedParams {
    pub(crate) fn shared(&self) -> &Arc<ParamMap> {
        &self.0
    }
}

/// The query's parameters with every omitted nullable one bound to null; an
/// omitted required one is the error the query answers.
fn resolve_params(ir: &QueryIR, params: &ParamMap) -> Result<ResolvedParams> {
    check_param_date_literals(params, &ir.params)?;
    let mut resolved_params = None;
    for param in &ir.params {
        if !params.contains_key(&param.name) {
            if param.nullable {
                resolved_params
                    .get_or_insert_with(|| params.clone())
                    .insert(param.name.clone(), Literal::Null);
            } else {
                return Err(OmniError::manifest(format!(
                    "parameter '{}' not provided",
                    param.name
                )));
            }
        }
    }
    let mut resolved = resolved_params.unwrap_or_else(|| params.clone());
    let now_name = omnigraph_compiler::query::ast::NOW_PARAM_NAME;
    if !resolved.contains_key(now_name) {
        let now = time::OffsetDateTime::from(crate::dst_clock::system_time_now())
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|error| OmniError::manifest(format!("failed to format now(): {error}")))?;
        resolved.insert(now_name.to_string(), Literal::DateTime(now));
    }
    Ok(ResolvedParams(Arc::new(resolved)))
}

/// Plan and execute a read under its own DataFusion context and session settings.
/// Widen nearest candidates when traversal or filtering leaves a full scan's
/// answer short of the limit.
pub(crate) async fn execute_query(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: GraphIndexHandle,
    catalog: &Arc<Catalog>,
    embedding: &EmbeddingResolver<'_>,
    settings: &SessionSettings,
) -> Result<QueryResult> {
    let params = resolve_params(ir, params)?;

    let physical = plan_query(ir, &params, catalog, snapshot, settings.traversal()).await?;
    let graph_index = Arc::new(graph_index);
    let ctx = QueryContext::new()?;
    let lowering = Lowering {
        plan: &physical,
        ir,
        params: &params,
        snapshot,
        graph_index: &graph_index,
        catalog,
        settings,
        memory_limit: ctx.memory_limit(),
    };
    let search_mode =
        extract_search_mode(ir, params.shared(), catalog, embedding, settings).await?;
    if let Some(ref rrf) = search_mode.rrf {
        let eligible = Box::pin(rrf_prefilter_gate(
            ir,
            snapshot,
            &graph_index,
            catalog,
            rrf,
            settings.rrf_plan(),
        ))
        .await;
        let (primary, secondary) = match eligible.as_ref() {
            Some(ids) => (
                arm_with_bm25_prefilter(&rrf.primary, ids),
                arm_with_bm25_prefilter(&rrf.secondary, ids),
            ),
            None => ((*rrf.primary).clone(), (*rrf.secondary).clone()),
        };
        let lowered = lowering.lower_query(&RunMode::Fused {
            rrf,
            primary: &primary,
            secondary: &secondary,
        })?;
        lowered.record_in_memory_filters();
        let fused = Box::pin(run_plan(&lowered.root, &ctx)).await?;
        return Ok(QueryResult::new(fused.schema(), vec![fused]));
    }

    let gate_applies = search_mode
        .nearest
        .as_ref()
        .is_some_and(|(ranked_var, ..)| pipeline_expands_from(&ir.pipeline, ranked_var));
    let search_mode = if gate_applies {
        match Box::pin(nearest_prefilter_gate(
            ir,
            snapshot,
            &graph_index,
            catalog,
            &search_mode,
            settings.rrf_plan(),
        ))
        .await
        {
            NearestGatePlan::Prefilter(ids) => SearchMode {
                eligible_ids: Some(ids),
                ..search_mode
            },
            NearestGatePlan::Postfilter => search_mode,
            NearestGatePlan::ProvenEmpty => SearchMode {
                answer_proven_empty: true,
                ..search_mode
            },
        }
    } else {
        search_mode
    };

    let (result_batch, report) = Box::pin(run_once(&lowering, &ctx, &search_mode)).await?;
    let mut result_batch = result_batch;
    let mut report = report;
    if let (Some((_, _, _, k0)), Some(limit)) = (search_mode.nearest.as_ref(), ir.limit) {
        if !projections_have_aggregates(&ir.return_exprs) && !search_mode.answer_proven_empty {
            let short = |batch: &RecordBatch| (batch.num_rows() as u64) < limit;
            let full_scan = |report: &ScanReport| {
                report
                    .nearest_scan
                    .inspect(|scan| {
                        crate::instrumentation::record_query_ladder_report(
                            crate::instrumentation::QueryLadderReport {
                                rows: scan.rows,
                                k: scan.k,
                                maximum_nprobes: scan.maximum_nprobes,
                                exhausted: scan.exhausted,
                                dataset_rows: scan.dataset_rows,
                            },
                        );
                    })
                    .filter(|scan| scan.rows >= scan.k && !scan.exhausted)
            };
            let mut factor = 1usize;
            while short(&result_batch) {
                let Some(scan) = full_scan(&report) else {
                    break;
                };
                let Some(rung) = next_overfetch_rung(factor, *k0, scan) else {
                    break;
                };
                crate::instrumentation::record_ann_overfetch();
                let rows_before = result_batch.num_rows();
                let wider = match rung {
                    OverfetchRung::Wider {
                        factor: next,
                        k,
                        maximum,
                    } => {
                        factor = next;
                        tracing::debug!(
                            limit,
                            rows = rows_before,
                            k,
                            maximum_nprobes = ?maximum,
                            "nearest answer short after a full scan; rerunning with more candidates"
                        );
                        search_mode.with_nearest_k(k, maximum)
                    }
                    OverfetchRung::Exact { k } => {
                        tracing::debug!(
                            limit,
                            rows = rows_before,
                            rows_hydrated = k,
                            "nearest answer still short at the overfetch ceiling; rerunning exact over the whole type"
                        );
                        crate::instrumentation::record_ann_exact_pass();
                        search_mode.with_exact_nearest(k)
                    }
                };
                let (retried, retried_report) = Box::pin(run_once(&lowering, &ctx, &wider)).await?;
                result_batch = retried;
                report = retried_report;
                if let OverfetchRung::Exact { k } = rung {
                    if result_batch.num_rows() > rows_before {
                        tracing::warn!(
                            limit,
                            rows_before,
                            rows = result_batch.num_rows(),
                            rows_hydrated = k,
                            "the overfetch ceiling hid survivors; the exact pass over the whole type found them"
                        );
                    } else {
                        tracing::debug!(
                            limit,
                            rows = result_batch.num_rows(),
                            rows_hydrated = k,
                            "the exact pass added no row: fewer survivors than limit exist"
                        );
                    }
                }
            }
        }
    }

    Ok(QueryResult::new(result_batch.schema(), vec![result_batch]))
}
