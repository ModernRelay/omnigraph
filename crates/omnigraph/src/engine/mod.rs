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
use omnigraph_compiler::ir::{IRExpr, IROp, IROrdering, IRProjection, ParamMap};
use omnigraph_compiler::query::ast::{AggFunc, BinaryOp, CompOp, Literal};
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_compiler::types::Direction;
use omnigraph_compiler::types::ScalarType;
use omnigraph_planner::{
    AcceptedBoundPlan, DatasetPin, ExpandMode, ExpandPolicy, NodeId, OverfetchRung, PhysicalNode,
    PhysicalPlan, Prefilter, RankKind, RankScope,
};

use crate::db::{DatasetEntry, Omnigraph, Snapshot};
use crate::embedding::EmbeddingClient;
use crate::error::{OmniError, Result};
use crate::graph_index::GraphIndex;
use crate::instrumentation::{
    RrfGateFallback, RrfGatePlan, RrfGateVerdict, record_ann_prefilter_verdict,
    record_rrf_gate_verdict,
};
use crate::runtime_cache::CompiledQuery;

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
use plan_source::{ExplainedQuery, QuerySource, accept_query, explain_query};
pub(crate) use plan_source::{accept_replay, replay_refused};
pub(crate) use report::{Executed, PlanRun};
use report::{ExecutionReport, ReportRow};
use run::{pass_rows, run_plan};
pub(crate) use scan::{id_in_list_expr, ir_expr_to_df_expr};
pub(crate) use search::{check_param_date_literals, referenced_edge_types};

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
/// branch and version, and a replay reads exactly those or nothing. A
/// changed prerequisite is a conflict: replan against the current view.
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
            return Err(OmniError::manifest_conflict(format!(
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
        .flat_map(|(_, node)| match node {
            PhysicalNode::Expand { edges, .. } => edges.members(),
            _ => &[],
        })
        .map(|member| &member.edge_type)
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
    let report = lowered
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
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

/// An `rrf()` with both its arms: the `RankFuse` node and the pre-pass it
/// declares.
struct Fusion<'p> {
    id: NodeId,
    prefilter: &'p Prefilter,
}

/// The ranked scans of `plan` the run's gates and ladder act on.
struct RankedScans<'p> {
    nearest: Option<NearestScan<'p>>,
    fusion: Option<Fusion<'p>>,
}

fn ranked_scans(plan: &PhysicalPlan) -> RankedScans<'_> {
    let mut nearest = None;
    let mut arms = [false, false];
    let mut fuse = None;
    for (id, node) in plan.live() {
        if let PhysicalNode::RankFuse { prefilter, .. } = node {
            fuse = Some((id, prefilter));
            continue;
        }
        let PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        } = node
        else {
            continue;
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
            RankScope::Primary => arms[0] = true,
            RankScope::Secondary => arms[1] = true,
        }
    }
    RankedScans {
        nearest,
        fusion: match (arms, fuse) {
            ([true, true], Some((id, prefilter))) => Some(Fusion { id, prefilter }),
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
    query: &CompiledQuery,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: GraphIndexHandle,
    catalog: &Arc<Catalog>,
    embedding: &EmbeddingResolver<'_>,
    settings: &SessionSettings,
) -> Result<QueryResult> {
    let source = QuerySource::gather(query, catalog, snapshot, params, settings).await?;
    let accepted = accept_query(&source)?;
    let bound = bind(accepted, &source, embedding).await?;
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
    query: &CompiledQuery,
    params: &ParamMap,
    snapshot: &Snapshot,
    graph_index: GraphIndexHandle,
    catalog: &Arc<Catalog>,
    embedding: &EmbeddingResolver<'_>,
    settings: &SessionSettings,
) -> Result<Executed> {
    let source = QuerySource::gather(query, catalog, snapshot, params, settings).await?;
    let ExplainedQuery { explain, accepted } = explain_query(&source)?;
    let bound = bind(accepted, &source, embedding).await?;
    let context = EngineContext {
        snapshot,
        catalog,
        graph_index: Arc::new(graph_index),
    };
    let PlanRun {
        result,
        plan,
        evidence,
        report,
    } = Box::pin(execute(bound, &context)).await?;
    Ok(Executed {
        result,
        plan,
        evidence,
        catalog: omnigraph_planner::catalog_digest(catalog),
        explain,
        report,
    })
}

/// Replayed plans must keep the statement's admission contract intact.
/// Refuse inconsistent policy before any shortcut can materialize a CSR.
fn validate_traversal_admission(plan: &PhysicalPlan) -> Result<Option<std::num::NonZeroU64>> {
    let cap = plan
        .assumptions()
        .validated_traversal_work_limit()
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    for (_, node) in plan.live() {
        match node {
            PhysicalNode::Expand {
                edges,
                policy,
                mode,
                versions,
                src_type,
                dst_type,
                min_hops,
                max_hops,
                edge_binding,
                ..
            } => {
                let budgeted = matches!(policy, ExpandPolicy::Budgeted);
                if (edges.named().is_none() || budgeted) && cap.is_none()
                    || cap.is_some() && (!budgeted || *mode != ExpandMode::IndexedScan)
                {
                    return Err(OmniError::manifest_internal(
                        "inconsistent traversal budget, selection and execution policy",
                    ));
                }
                operators::validate_expand_structure(
                    edges.members(),
                    matches!(
                        edges,
                        omnigraph_compiler::traversal::EdgeSelection::Alternation(_)
                    ),
                    edges.named().is_none() && src_type != dst_type,
                    *min_hops,
                    *max_hops,
                    edge_binding.is_some(),
                )?;
                let members = edges.members();
                if members.len() != versions.len()
                    || members.iter().any(|member| {
                        let name = member.edge_type.as_str();
                        let version = versions.get(name).copied();
                        version.is_none()
                            || plan
                                .assumptions()
                                .datasets
                                .get(&edge_table_key(name))
                                .map(|pin| pin.as_ref().map(|pin| pin.version))
                                != version
                    })
                {
                    return Err(OmniError::manifest_internal(
                        "traversal members do not match captured dataset versions",
                    ));
                }
            }
            PhysicalNode::RankFuse { prefilter, .. } if cap.is_some() && prefilter.admits() => {
                return Err(OmniError::manifest_internal(
                    "budgeted traversal cannot use the CSR prefilter",
                ));
            }
            PhysicalNode::Scan {
                ranked: Some(ranked),
                ..
            } if cap.is_some() && ranked.prefilter.as_ref().is_some_and(Prefilter::admits) => {
                return Err(OmniError::manifest_internal(
                    "budgeted traversal cannot use the CSR prefilter",
                ));
            }
            _ => {}
        }
    }
    Ok(cap)
}

/// Run `bound` under `context` and report what each of its nodes did, every
/// pass of the overfetch ladder folded into one report.
pub(crate) async fn execute(
    accepted: AcceptedBoundPlan,
    context: &EngineContext<'_>,
) -> Result<PlanRun> {
    let (bound, evidence) = accepted.into_parts();
    let traversal_limit = validate_traversal_admission(&bound.plan)?;
    omnigraph_planner::optimizer::validate_rank_fuse_row_tiebreaks(&bound.plan)
        .map_err(|error| OmniError::manifest_internal(error.to_string()))?;
    let mut executed = ExecutionReport::default();
    let ctx =
        QueryContext::with_traversal_limit(bound.plan.assumptions().memory_limit, traversal_limit)?;
    let policy = bound.plan.assumptions().gate_policy;
    let lowering = Lowering::new(&bound, context);
    let RankedScans { nearest, fusion } = ranked_scans(&bound.plan);
    if let Some(Fusion { id, prefilter }) = fusion {
        let (eligible, verdict) = Box::pin(rrf_prefilter_gate(context, prefilter, policy)).await;
        let plan = if eligible.is_some() {
            "prefilter"
        } else {
            "postfilter"
        };
        executed.decide(id, 0, report::Taken::gate(plan, &verdict));
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
            evidence,
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
        let (plan, verdict) = Box::pin(nearest_prefilter_gate(context, prefilter, policy)).await;
        let (taken, next) = match plan {
            NearestGatePlan::Prefilter(ids) => ("prefilter", pass.prefiltered(*id, ids)),
            NearestGatePlan::Postfilter => ("postfilter", pass),
            NearestGatePlan::ProvenEmpty => ("proven_empty", pass.proven_empty(*id)),
        };
        executed.decide(*id, 0, report::Taken::gate(taken, &verdict));
        pass = next;
    }

    let (result_batch, report, rows) = Box::pin(run_once(&lowering, &ctx, &pass, 0)).await?;
    executed.record(rows);
    if let Some(NearestScan { id, .. }) = &nearest
        && !report.probes.is_empty()
    {
        executed.decide(
            *id,
            0,
            report::Taken::Probes {
                attempts: report.probes.clone(),
            },
        );
    }
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
                if !retried_report.probes.is_empty() {
                    executed.decide(
                        id,
                        rung,
                        report::Taken::Probes {
                            attempts: retried_report.probes.clone(),
                        },
                    );
                }
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
        evidence,
        report: executed,
    })
}

#[cfg(test)]
mod traversal_admission_tests {
    use super::*;
    use omnigraph_compiler::traversal::{EdgeMember, EdgeSelection};
    use omnigraph_planner::{ExpandMode, ExpandPolicy};

    fn selected_plan() -> PhysicalPlan {
        let mut plan = PhysicalPlan::new();
        let input = plan.add(PhysicalNode::OuterReference {
            outer_var: "p".into(),
        });
        let root = plan.add(PhysicalNode::Expand {
            input,
            src: "p".into(),
            dst: "q".into(),
            edges: EdgeSelection::Alternation(vec![EdgeMember {
                edge_type: "Knows".into(),
                direction: Direction::Out,
            }]),
            src_type: "Person".into(),
            dst_type: "Person".into(),
            min_hops: 1,
            max_hops: Some(1),
            edge_binding: None,
            mode: ExpandMode::IndexedScan,
            frontier_estimate: None,
            policy: ExpandPolicy::Budgeted,
            versions: [("Knows".into(), None)].into(),
        });
        plan.set_root(root);
        let mut assumptions = plan.assumptions().clone();
        assumptions.traversal_work_limit = Some(100);
        assumptions.datasets.insert("edge:Knows".into(), None);
        plan.set_assumptions(assumptions);
        plan
    }

    #[test]
    fn replay_refuses_missing_or_zero_selector_budget_issue_659() {
        let mut plan = selected_plan();
        validate_traversal_admission(&plan).unwrap();
        for cap in [None, Some(0), Some(i64::MAX as u64 + 1)] {
            let mut assumptions = plan.assumptions().clone();
            assumptions.traversal_work_limit = cap;
            plan.set_assumptions(assumptions);
            assert!(validate_traversal_admission(&plan).is_err());
        }
    }

    #[test]
    fn replay_refuses_duplicate_cap_authorities_and_noncanonical_members_issue_659() {
        let mut plan = selected_plan();
        let mut assumptions = plan.assumptions().clone();
        assumptions
            .settings
            .insert("traversal_work_limit".into(), "100".into());
        plan.set_assumptions(assumptions);
        assert!(validate_traversal_admission(&plan).is_err());
        let member = |name: &str| EdgeMember {
            edge_type: name.into(),
            direction: Direction::Out,
        };
        for members in [
            vec![],
            vec![member("Knows"), member("Knows")],
            vec![member("Likes"), member("Knows")],
        ] {
            let mut plan = selected_plan();
            let root = plan.root();
            let Some(PhysicalNode::Expand {
                edges, versions, ..
            }) = plan.node_mut(root)
            else {
                unreachable!()
            };
            *versions = members
                .iter()
                .map(|member| (member.edge_type.clone(), None))
                .collect();
            *edges = EdgeSelection::Alternation(members);
            let mut assumptions = plan.assumptions().clone();
            assumptions.datasets.insert("edge:Likes".into(), None);
            plan.set_assumptions(assumptions);
            assert!(validate_traversal_admission(&plan).is_err());
        }
    }

    #[test]
    fn replay_refuses_csr_mode_and_missing_member_pin_issue_659() {
        let mut plan = selected_plan();
        let root = plan.root();
        let Some(PhysicalNode::Expand { mode, .. }) = plan.node_mut(root) else {
            unreachable!()
        };
        *mode = ExpandMode::Csr;
        assert!(validate_traversal_admission(&plan).is_err());
        let mut plan = selected_plan();
        let mut assumptions = plan.assumptions().clone();
        assumptions.datasets.clear();
        plan.set_assumptions(assumptions);
        assert!(validate_traversal_admission(&plan).is_err());
    }

    #[test]
    fn replay_refuses_capped_pinned_policy_and_mismatched_version_issue_659() {
        for named in [false, true] {
            let mut plan = selected_plan();
            validate_traversal_admission(&plan).unwrap();
            let root = plan.root();
            let Some(PhysicalNode::Expand { edges, policy, .. }) = plan.node_mut(root) else {
                unreachable!()
            };
            if named {
                *edges = EdgeSelection::Named(edges.members()[0].clone());
            }
            *policy = ExpandPolicy::Pinned;
            let error = validate_traversal_admission(&plan).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("inconsistent traversal budget, selection and execution policy"),
                "{error}"
            );
        }

        let mut plan = selected_plan();
        let root = plan.root();
        let Some(PhysicalNode::Expand { versions, .. }) = plan.node_mut(root) else {
            unreachable!()
        };
        versions.insert("Knows".into(), Some(7));
        let error = validate_traversal_admission(&plan).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("members do not match captured dataset versions"),
            "{error}"
        );
    }

    #[test]
    fn replay_refuses_ranked_topology_prefilter_bypasses_issue_659() {
        use omnigraph_planner::{
            ColumnRef, Hop, RankArm, RankedAccess, ScanInput, ScanSpec, SideId, TableRef,
        };
        let prefilter = |feeds| Prefilter {
            ranked_type: "Person".into(),
            hops: vec![Hop {
                edge_type: "Knows".into(),
                direction: Direction::Out,
            }],
            feeds,
            on_empty: omnigraph_planner::EmptyEligible::ProvenEmpty,
            coverage_admits: true,
        };
        let mut plan = selected_plan();
        let input = 0;
        *plan.node_mut(input).unwrap() = PhysicalNode::Scan {
            source: ScanInput::Table,
            spec: Box::new(ScanSpec {
                side: SideId::Base,
                table: TableRef {
                    type_key: "node:Person".into(),
                    dataset_path: "node_Person".into(),
                    native_branch: None,
                },
                version: None,
                columns: SystemColumns {
                    id: "__id",
                    src: "__src",
                    dst: "__dst",
                },
                fragments: None,
                projection: None,
                filter: None,
                binding: Some("p".into()),
                runtime_filter: None,
            }),
            ordered: false,
            keys_only: false,
            ranked: Some(RankedAccess {
                kind: RankKind::Nearest,
                property: "embedding".into(),
                query: IRExpr::Literal(Literal::String("query".into())),
                fetch: Some(1),
                nprobes: None,
                scope: RankScope::Order,
                overfetch: vec![],
                prefilter: None,
                eligibility: omnigraph_planner::Eligibility::BeforeScoring,
                policy: Some(omnigraph_planner::NearestPolicy::DEFAULT),
            }),
        };
        validate_traversal_admission(&plan).unwrap();
        let Some(PhysicalNode::Scan {
            ranked: Some(ranked),
            ..
        }) = plan.node_mut(input)
        else {
            unreachable!()
        };
        ranked.prefilter = Some(prefilter(vec![input]));
        let error = validate_traversal_admission(&plan).unwrap_err();
        assert!(
            error.to_string().contains("cannot use the CSR prefilter"),
            "{error}"
        );

        let mut plan = selected_plan();
        let input = plan.root();
        let arm = |kind| RankArm {
            input,
            binding: "p".into(),
            kind,
        };
        let root = plan.add(PhysicalNode::RankFuse {
            arms: [arm(RankKind::Nearest), arm(RankKind::Bm25)],
            k: None,
            limit: Some(1),
            prefilter: prefilter(vec![]),
            row_tiebreak: vec![ColumnRef::property("q", "@id")],
        });
        plan.set_root(root);
        validate_traversal_admission(&plan).unwrap();
        let Some(PhysicalNode::RankFuse { prefilter, .. }) = plan.node_mut(root) else {
            unreachable!()
        };
        prefilter.feeds.push(0);
        let error = validate_traversal_admission(&plan).unwrap_err();
        assert!(
            error.to_string().contains("cannot use the CSR prefilter"),
            "{error}"
        );
    }

    #[test]
    fn replay_refuses_unbounded_and_multi_hop_edge_bindings_issue_659() {
        let mut plan = selected_plan();
        let root = plan.root();
        let Some(PhysicalNode::Expand { max_hops, .. }) = plan.node_mut(root) else {
            unreachable!()
        };
        *max_hops = None;
        assert!(validate_traversal_admission(&plan).is_err());
        let Some(PhysicalNode::Expand {
            max_hops,
            edge_binding,
            ..
        }) = plan.node_mut(root)
        else {
            unreachable!()
        };
        *max_hops = Some(2);
        *edge_binding = Some("e".into());
        assert!(validate_traversal_admission(&plan).is_err());
    }
}
