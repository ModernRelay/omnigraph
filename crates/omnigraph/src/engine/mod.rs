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
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, ParamMap, QueryIR};
use omnigraph_compiler::query::ast::{AggFunc, BinaryOp, CompOp, Literal};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_compiler::types::Direction;
use omnigraph_compiler::types::ScalarType;
use omnigraph_planner::{
    BoundPlan, DatasetPin, NodeId, OverfetchRung, PhysicalNode, PhysicalPlan, Prefilter, RankKind,
    RankScope,
};

use crate::db::{DatasetEntry, Omnigraph, Snapshot};
use crate::embedding::EmbeddingClient;
use crate::error::{OmniError, Result};
use crate::graph_index::GraphIndex;
use crate::instrumentation::{
    RrfGateFallback, RrfGatePlan, RrfGateVerdict, record_ann_prefilter_verdict,
    record_rrf_gate_verdict,
};

mod adapters;
mod bind;
mod constant;
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
mod report;
mod run;
mod scan;
mod search;

use expr::*;
use scan::*;
use search::*;

use bind::bind;
pub(crate) use constant::evaluate_constant;
use context::QueryContext;
pub(crate) use explain::{explain_document, explain_rows};
pub(crate) use graph::{EmbeddingResolver, GraphIndexHandle};
use lower::Lowering;
pub(crate) use operators::{SubqueryAggregate, absorb_inner_batches};
use plan_source::{ExplainedQuery, QuerySource, explain_query, plan_query};
pub(crate) use report::{Executed, PlanRun};
use report::{ExecutionReport, ReportRow};
use run::{pass_rows, run_plan};
pub(crate) use scan::{id_in_list_expr, ir_expr_to_df_expr};
pub(crate) use search::referenced_edge_types;

/// What `execute` takes beside the bound plan, each member data and not a
/// decision: the read-consistency unit, the type metadata the lowered
/// operators read back, and the built index state. No session setting: every
/// setting the run needs is a field of the plan.
pub(crate) struct EngineContext<'a> {
    pub(crate) snapshot: &'a Snapshot,
    pub(crate) catalog: &'a Arc<Catalog>,
    pub(crate) graph_index: Arc<GraphIndexHandle>,
}

/// The identity of a snapshot's dataset entry as a plan pins it.
pub(crate) fn dataset_pin(entry: &DatasetEntry) -> DatasetPin {
    DatasetPin {
        dataset_path: entry.dataset_path.clone(),
        native_branch: entry.native_dataset_branch.clone(),
        version: entry
            .version_metadata
            .staged_version()
            .unwrap_or(entry.published_dataset_version),
    }
}

/// Refuse a snapshot that is not the one `plan` was built on: the planner
/// recorded every dataset it read in the plan's `Assumptions`, by path,
/// branch and version, and a replay reads exactly those or nothing.
pub(crate) fn plan_pins_snapshot(plan: &PhysicalPlan, snapshot: &Snapshot) -> Result<()> {
    for (table, planned) in &plan.assumptions().datasets {
        let pinned = snapshot.dataset(table).map(dataset_pin);
        if pinned != *planned {
            let spell = |pin: &Option<DatasetPin>| match pin {
                Some(pin) => format!(
                    "version {} of `{}`{}",
                    pin.version,
                    pin.dataset_path,
                    pin.native_branch
                        .as_deref()
                        .map(|branch| format!(" on branch `{branch}`"))
                        .unwrap_or_default()
                ),
                None => "no such table".to_string(),
            };
            return Err(OmniError::manifest_internal(format!(
                "`{table}` was planned at dataset {}; the snapshot holds {}",
                spell(planned),
                spell(&pinned)
            )));
        }
    }
    Ok(())
}

/// Whether `plan` traverses edges, so its run builds a graph index: an
/// `Expand` anywhere, an `AntiJoin`'s bulk path included, since that path
/// reads the CSR of the inner tree's `Expand`.
pub(crate) fn plan_traverses(plan: &PhysicalPlan) -> bool {
    plan.live()
        .any(|(_, node)| matches!(node, PhysicalNode::Expand { .. }))
}

/// The table key of an edge type's dataset.
pub(crate) fn edge_table_key(edge_type: &str) -> String {
    format!("edge:{edge_type}")
}

/// The edge types `plan` traverses, `AntiJoin` inner trees included, mapped
/// to their endpoint types: the scope of the graph-index build.
pub(crate) fn plan_edge_types(
    plan: &PhysicalPlan,
    catalog: &Catalog,
) -> HashMap<String, (String, String)> {
    plan.live()
        .filter_map(|(_, node)| match node {
            PhysicalNode::Expand { edge_type, .. } => Some(edge_type),
            _ => None,
        })
        .filter_map(|name| {
            catalog
                .edge_types
                .get(name)
                .map(|et| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
        })
        .collect()
}

/// One pass of the tree under `pass`: lower it, run it under `ctx`, and read
/// the pass's report rows back as rung `rung`. `execute` reruns it under a
/// widened pass when a capped search under-fills.
async fn run_once(
    lowering: &Lowering<'_>,
    ctx: &QueryContext,
    pass: &Pass,
    rung: usize,
) -> Result<(RecordBatch, ScanReport, Vec<ReportRow>)> {
    let lowered = lowering.lower_query(pass)?;
    lowered.record_in_memory_filters();
    let batch = run_plan(&lowered, lowering.plan, ctx).await?;
    let report = *lowered
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rows = pass_rows(&lowered, lowering.plan, rung)?;
    Ok((batch, report, rows))
}

/// The standalone `nearest` scan of a search order: what the plan declares
/// for its pre-pass and its overfetch ladder.
struct NearestScan<'p> {
    id: NodeId,
    prefilter: Option<&'p Prefilter>,
    overfetch: &'p [OverfetchRung],
}

/// The two arms of an `rrf()` in arm order, and the pre-pass their
/// `RankFuse` declares.
struct Fusion<'p> {
    arms: [ArmTarget<'p>; 2],
    prefilter: &'p Prefilter,
}

/// The ranked scans of `plan` the run's gates and ladder act on.
struct RankedScans<'p> {
    nearest: Option<NearestScan<'p>>,
    fusion: Option<Fusion<'p>>,
}

fn ranked_scans(plan: &PhysicalPlan) -> RankedScans<'_> {
    let mut nearest = None;
    let mut arms: [Option<ArmTarget<'_>>; 2] = [None, None];
    let mut fuse = None;
    for (id, node) in plan.live() {
        if let PhysicalNode::RankFuse { prefilter, .. } = node {
            fuse = Some(prefilter);
            continue;
        }
        let PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        } = node
        else {
            continue;
        };
        let target = ArmTarget {
            kind: ranked.kind,
            property: &ranked.property,
        };
        match ranked.scope {
            RankScope::Order => {
                if ranked.kind == RankKind::Nearest && ranked.fetch.is_some() {
                    nearest = Some(NearestScan {
                        id,
                        prefilter: ranked.prefilter.as_ref(),
                        overfetch: &ranked.overfetch,
                    });
                }
            }
            RankScope::Primary => arms[0] = Some(target),
            RankScope::Secondary => arms[1] = Some(target),
        }
    }
    RankedScans {
        nearest,
        fusion: match (arms, fuse) {
            ([Some(primary), Some(secondary)], Some(prefilter)) => Some(Fusion {
                arms: [primary, secondary],
                prefilter,
            }),
            _ => None,
        },
    }
}

/// The rows the query's `limit` clause admits: the `Limit` at the plan's
/// root.
fn declared_limit(plan: &PhysicalPlan) -> Option<usize> {
    match plan.node(plan.root()) {
        Some(PhysicalNode::Limit { rows, .. }) => Some(*rows),
        _ => None,
    }
}

/// A query's parameters as `QuerySource::gather` binds them; the planner and
/// the lowering take no other form.
pub(crate) struct ResolvedParams(Arc<ParamMap>);

impl ResolvedParams {
    pub(crate) fn shared(&self) -> &Arc<ParamMap> {
        &self.0
    }
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
    let source = QuerySource::gather(ir, catalog, snapshot, params, settings).await?;
    let physical = plan_query(&source)?;
    let bound = bind(physical, &source, embedding).await?;
    let context = EngineContext {
        snapshot,
        catalog,
        graph_index: Arc::new(graph_index),
    };
    let run = Box::pin(execute(bound, &context)).await?;
    #[cfg(test)]
    report::tests::capture(&run.plan.plan, &run.report);
    Ok(run.result)
}

/// [`execute_query`] as an [`Executed`]: the gate plans once, and that one
/// plan is both the plan the run executes and the plan its explain renders.
pub(crate) async fn execute_query_inspected(
    ir: &QueryIR,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: GraphIndexHandle,
    catalog: &Arc<Catalog>,
    embedding: &EmbeddingResolver<'_>,
    settings: &SessionSettings,
) -> Result<Executed> {
    let source = QuerySource::gather(ir, catalog, snapshot, params, settings).await?;
    let ExplainedQuery { explain, physical } = explain_query(&source)?;
    let bound = bind(physical, &source, embedding).await?;
    let context = EngineContext {
        snapshot,
        catalog,
        graph_index: Arc::new(graph_index),
    };
    let PlanRun {
        result,
        plan,
        report,
    } = Box::pin(execute(bound, &context)).await?;
    Ok(Executed {
        result,
        plan,
        explain,
        report,
    })
}

/// Run `bound` under `context` and report what each of its nodes did, every
/// pass of the overfetch ladder folded into one report.
pub(crate) async fn execute(bound: BoundPlan, context: &EngineContext<'_>) -> Result<PlanRun> {
    let mut executed = ExecutionReport::default();
    let ctx = QueryContext::new(bound.plan.assumptions().memory_limit)?;
    let policy = bound.plan.assumptions().gate_policy;
    let lowering = Lowering::new(&bound, context);
    let RankedScans { nearest, fusion } = ranked_scans(&bound.plan);
    if let Some(Fusion { arms, prefilter }) = fusion {
        let eligible = Box::pin(rrf_prefilter_gate(context, arms, prefilter, policy)).await;
        let mut pass = Pass::default();
        if let Some(ids) = eligible {
            for id in &prefilter.feeds {
                pass = pass.prefiltered(*id, ids.clone());
            }
        }
        let lowered = lowering.lower_query(&pass)?;
        lowered.record_in_memory_filters();
        let fused = Box::pin(run_plan(&lowered, &bound.plan, &ctx)).await?;
        executed.record(pass_rows(&lowered, &bound.plan, 0)?);
        return Ok(PlanRun {
            result: QueryResult::new(fused.schema(), vec![fused]),
            plan: bound,
            report: executed,
        });
    }

    let mut pass = Pass::default();
    if let Some(NearestScan {
        id,
        prefilter: Some(prefilter),
        ..
    }) = &nearest
    {
        pass = match Box::pin(nearest_prefilter_gate(context, prefilter, policy)).await {
            NearestGatePlan::Prefilter(ids) => pass.prefiltered(*id, ids),
            NearestGatePlan::Postfilter => pass,
            NearestGatePlan::ProvenEmpty => pass.proven_empty(*id),
        };
    }

    let (result_batch, report, rows) = Box::pin(run_once(&lowering, &ctx, &pass, 0)).await?;
    executed.record(rows);
    let mut result_batch = result_batch;
    let mut report = report;
    let aggregate = bound
        .plan
        .live()
        .any(|(_, node)| matches!(node, PhysicalNode::Aggregate { .. }));
    if let (Some(NearestScan { id, overfetch, .. }), Some(limit)) =
        (&nearest, declared_limit(&bound.plan))
    {
        let id = *id;
        if !aggregate && !pass.answer_proven_empty() {
            let short = |batch: &RecordBatch| batch.num_rows() < limit;
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
            let mut taken = 0usize;
            while short(&result_batch) {
                let Some(scan) = full_scan(&report) else {
                    break;
                };
                let Some(NextPass { rung, step }) = next_overfetch_rung(overfetch, taken, scan)
                else {
                    break;
                };
                taken = rung;
                crate::instrumentation::record_ann_overfetch();
                let rows_before = result_batch.num_rows();
                let wider = match step {
                    PassStep::Wider { k, maximum } => {
                        tracing::debug!(
                            limit,
                            rows = rows_before,
                            k,
                            maximum_nprobes = ?maximum,
                            "nearest answer short after a full scan; rerunning with more candidates"
                        );
                        pass.with_nearest_k(id, k, maximum)
                    }
                    PassStep::Exact { k } => {
                        tracing::debug!(
                            limit,
                            rows = rows_before,
                            rows_hydrated = k,
                            "nearest answer still short at the overfetch ceiling; rerunning exact over the whole type"
                        );
                        crate::instrumentation::record_ann_exact_pass();
                        pass.with_exact_nearest(id, k)
                    }
                };
                let (retried, retried_report, rows) =
                    Box::pin(run_once(&lowering, &ctx, &wider, rung)).await?;
                executed.record(rows);
                result_batch = retried;
                report = retried_report;
                if let PassStep::Exact { k } = step {
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

    Ok(PlanRun {
        result: QueryResult::new(result_batch.schema(), vec![result_batch]),
        plan: bound,
        report: executed,
    })
}
