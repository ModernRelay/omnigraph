//! Executable read planning and diagnostic routing. Read planning returns a
//! physical plan or a typed failure; routing includes explain diagnostics and
//! sends unsupported operations to the executor with a typed reason.

use std::cell::RefCell;
use std::collections::BTreeSet;

use arrow_schema::SchemaRef;
use omnigraph_compiler::ir::{IRExpr, QueryIR};
use omnigraph_compiler::settings::{SettingId, Traversal};
use omnigraph_compiler::types::Direction;
use serde_json::{Value, json};

use crate::error::PlanError;
use crate::explain::{EntrySummary, Explain, OperationSummary};
use crate::logical::{Census, LogicalPlan};
use crate::operation::Operation;
use crate::optimizer::{Bounds, Optimized, physical_plan, resolve, rewrite};
use crate::physical::{Assumptions, DatasetPin, GatePolicy, NodeId, PhysicalNode, PhysicalPlan};
use crate::registry::{Coverage, Entry, Route, coverage, lookup};
use crate::route::RouteOverride;
use crate::source::{
    AdjacencyProof, EXPAND_INDEXED_MAX_FRONTIER_ENV, EXPAND_INDEXED_MAX_HOPS_ENV, ExpandStatistics,
    FragmentStat, NodeTypeSpec, PlanSource, SideId,
};

/// A `PlanSource` that records what the planner read through it: the
/// parameter names of every filter it asked about and every setting, so the
/// plan can carry them as its `Assumptions`.
struct Recorded<'s> {
    source: &'s dyn PlanSource,
    read: RefCell<Assumptions>,
}

impl<'s> Recorded<'s> {
    fn new(source: &'s dyn PlanSource) -> Self {
        Self {
            source,
            read: RefCell::new(Assumptions::default()),
        }
    }

    /// Everything read so far, with the policy and the limit the plan runs
    /// under.
    fn assumptions(&self, bounds: &Bounds) -> Assumptions {
        let mut assumptions = self.read.borrow().clone();
        assumptions.gate_policy = self.source.gate_policy();
        assumptions.memory_limit = bounds.query_memory_pool_bytes;
        assumptions
    }
}

impl PlanSource for Recorded<'_> {
    fn is_unique_property(&self, type_key: &str, property: &str) -> bool {
        self.source.is_unique_property(type_key, property)
    }

    fn table_data_bytes(&self, type_key: &str) -> Option<u64> {
        self.source.table_data_bytes(type_key)
    }

    fn column_data_bytes(&self, type_key: &str, column: &str) -> Option<u64> {
        self.source.column_data_bytes(type_key, column)
    }

    fn query_memory_pool_bytes(&self) -> u64 {
        self.source.query_memory_pool_bytes()
    }

    fn schema(&self, side: SideId) -> Result<SchemaRef, PlanError> {
        self.source.schema(side)
    }

    fn fragments(&self, side: SideId) -> Vec<FragmentStat> {
        self.source.fragments(side)
    }

    fn adjacency_proof(&self) -> Option<&AdjacencyProof> {
        self.source.adjacency_proof()
    }

    fn node_type(&self, type_name: &str) -> Result<NodeTypeSpec, PlanError> {
        let spec = self.source.node_type(type_name)?;
        self.read.borrow_mut().datasets.insert(
            spec.table.type_key.clone(),
            spec.version.map(|version| DatasetPin {
                dataset_path: spec.table.dataset_path.clone(),
                native_branch: spec.table.native_branch.clone(),
                version,
            }),
        );
        Ok(spec)
    }

    fn filter_pushable(&self, filter: &IRExpr) -> bool {
        let mut read = self.read.borrow_mut();
        params_of_expr(filter, &mut read.params);
        drop(read);
        self.source.filter_pushable(filter)
    }

    fn expand_statistics(&self, edge_type: &str, direction: Direction) -> Option<ExpandStatistics> {
        let statistics = self.source.expand_statistics(edge_type, direction)?;
        let mut read = self.read.borrow_mut();
        read.env.insert(
            EXPAND_INDEXED_MAX_FRONTIER_ENV.to_string(),
            statistics.max_frontier_cap.to_string(),
        );
        read.env.insert(
            EXPAND_INDEXED_MAX_HOPS_ENV.to_string(),
            statistics.max_hops_cap.to_string(),
        );
        Some(statistics)
    }

    fn edge_dataset(&self, edge_type: &str) -> Option<DatasetPin> {
        let pin = self.source.edge_dataset(edge_type);
        self.read
            .borrow_mut()
            .datasets
            .insert(format!("edge:{edge_type}"), pin.clone());
        pin
    }

    fn traversal(&self) -> Traversal {
        let traversal = self.source.traversal();
        self.read
            .borrow_mut()
            .settings
            .insert("traversal".to_string(), traversal.as_str().to_string());
        traversal
    }

    /// Recorded as the setting spells it: `0` is no cap.
    fn ann_nprobes(&self) -> Option<usize> {
        let nprobes = self.source.ann_nprobes();
        self.read.borrow_mut().settings.insert(
            SettingId::AnnNprobes.name().to_string(),
            nprobes.unwrap_or_default().to_string(),
        );
        nprobes
    }

    fn gate_policy(&self) -> GatePolicy {
        self.source.gate_policy()
    }
}

/// Every parameter name `expr` names.
fn params_of_expr(expr: &IRExpr, out: &mut BTreeSet<String>) {
    match expr {
        IRExpr::Param(name) => {
            out.insert(name.clone());
        }
        IRExpr::Nearest { query, .. } => params_of_expr(query, out),
        IRExpr::Search { field, query }
        | IRExpr::MatchText { field, query }
        | IRExpr::Bm25 { field, query } => {
            params_of_expr(field, out);
            params_of_expr(query, out);
        }
        IRExpr::Fuzzy {
            field,
            query,
            max_edits,
        } => {
            params_of_expr(field, out);
            params_of_expr(query, out);
            if let Some(max_edits) = max_edits {
                params_of_expr(max_edits, out);
            }
        }
        IRExpr::Rrf {
            primary,
            secondary,
            k,
        } => {
            params_of_expr(primary, out);
            params_of_expr(secondary, out);
            if let Some(k) = k {
                params_of_expr(k, out);
            }
        }
        IRExpr::Aggregate { arg, .. } => params_of_expr(arg, out),
        IRExpr::Binary { left, right, .. } => {
            params_of_expr(left, out);
            params_of_expr(right, out);
        }
        IRExpr::Not(inner) | IRExpr::IsNull { expr: inner, .. } => params_of_expr(inner, out),
        IRExpr::PropAccess { .. }
        | IRExpr::Variable(_)
        | IRExpr::Literal(_)
        | IRExpr::AliasRef(_) => {}
    }
}

/// Why the gate built no routed plan. A change-feed or merge operation then
/// runs on the executor; a read query has no executor behind it, so
/// [`plan_query`]'s caller returns the reason as a failed query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unrouted {
    /// The plan holds a node kind the coverage list refuses by name.
    UnregisteredNode { kind: String },
    /// Every kind is routable, the combination is not registered.
    NoMatchingEntry { census: Census },
    /// The matched entry routes to the executor, or sits behind the flag
    /// under the default override.
    RegistryRouteExecutor { entry: &'static str },
    /// The built plan declares more retained memory than its bound allows.
    DeclaredBytesOverBound { node: NodeId },
    /// The operator forced the executor.
    Override,
    /// Resolution or a pass failed: an unknown name in the plan source, or a
    /// planner defect. A read query fails with this message.
    PlannerError { message: String },
    /// A well-formed query shape the planner refuses by design
    /// (`PlanError::Unsupported`); the caller's error, not a planner defect.
    UnsupportedQuery { message: String },
}

impl Unrouted {
    /// The route of a planning failure: an unsupported shape keeps its class,
    /// everything else is a planner error.
    fn of(error: PlanError) -> Self {
        match error {
            PlanError::Unsupported { detail } => Self::UnsupportedQuery { message: detail },
            other => Self::PlannerError {
                message: other.to_string(),
            },
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnregisteredNode { .. } => "unregistered_node",
            Self::NoMatchingEntry { .. } => "no_matching_entry",
            Self::RegistryRouteExecutor { .. } => "registry_route_executor",
            Self::DeclaredBytesOverBound { .. } => "declared_bytes_over_bound",
            Self::Override => "override",
            Self::PlannerError { .. } => "planner_error",
            Self::UnsupportedQuery { .. } => "unsupported_query",
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Self::UnregisteredNode { kind } => json!({ "kind": self.kind(), "node": kind }),
            Self::NoMatchingEntry { census } => {
                json!({ "kind": self.kind(), "census": census.to_string() })
            }
            Self::RegistryRouteExecutor { entry } => json!({ "kind": self.kind(), "entry": entry }),
            Self::DeclaredBytesOverBound { node } => json!({ "kind": self.kind(), "node": node }),
            Self::Override => json!({ "kind": self.kind() }),
            Self::PlannerError { message } | Self::UnsupportedQuery { message } => {
                json!({ "kind": self.kind(), "message": message })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Decision {
    /// A registered change-feed or merge shape: the engine's push operators
    /// (`engine/push/`) run `plan`.
    Routed {
        plan: PhysicalPlan,
        entry: &'static Entry,
        explain: Explain,
    },
    /// A GQ query: the engine's `engine/lower.rs` executes `plan`, lowered
    /// one-to-one from `logical`. Never consults the registry.
    Engine {
        plan: PhysicalPlan,
        logical: LogicalPlan,
        explain: Explain,
    },
    Executor {
        reason: Unrouted,
        explain: Explain,
        /// The rewritten logical plan, when one was built before the miss;
        /// empty when the operation was refused before a plan existed.
        logical: LogicalPlan,
    },
}

impl Decision {
    pub fn explain(&self) -> &Explain {
        match self {
            Self::Routed { explain, .. }
            | Self::Engine { explain, .. }
            | Self::Executor { explain, .. } => explain,
        }
    }
}

/// Build an executable read plan without rendering explain diagnostics.
pub fn plan_query(
    query: &QueryIR,
    source: &dyn PlanSource,
    bounds: &Bounds,
) -> Result<PhysicalPlan, Unrouted> {
    let operation = Operation::Query(Box::new(query.clone()));
    let recorded = Recorded::new(source);
    let mut logical = resolve(&operation, &recorded).map_err(Unrouted::of)?;
    crate::optimizer::optimize(&mut logical, &recorded, bounds)
        .map(|mut optimized| {
            optimized
                .physical
                .set_assumptions(recorded.assumptions(bounds));
            optimized.physical
        })
        .map_err(Unrouted::of)
}

/// Decide the route of one operation. The census is computed from the
/// operation and the plan source only; the override is applied after the
/// registry lookup. The logical plan is built for every operation and the
/// optimizer runs whenever the registry names the shape, whatever its route,
/// so explain shows the candidate plan even for an executor run.
pub fn route(
    op: &Operation,
    source: &dyn PlanSource,
    override_: RouteOverride,
    bounds: &Bounds,
) -> Decision {
    let operation = OperationSummary::of(op);
    let recorded = Recorded::new(source);
    let source = &recorded;
    let mut plan = match resolve(op, source) {
        Ok(plan) => plan,
        Err(error) => {
            return executor(
                Unrouted::of(error),
                Explain::without_plan(operation, override_),
                LogicalPlan::new(),
            );
        }
    };
    let census = plan.census();
    let fired = match rewrite(&mut plan, source) {
        Ok(fired) => fired,
        Err(error) => {
            let explain = LogicalView::of(&plan).explain(operation, override_, None, None, &[]);
            return executor(Unrouted::of(error), explain, plan);
        }
    };
    if matches!(op, Operation::Query(_)) {
        let lowered = physical_plan(&mut plan, source, bounds, fired.clone());
        let logical = LogicalView::of(&plan);
        return match lowered {
            Ok(mut optimized) => {
                optimized
                    .physical
                    .set_assumptions(recorded.assumptions(bounds));
                let mut explain = logical
                    .explain(operation, override_, None, Some(&optimized), &fired)
                    .engine();
                explain.pipelines = None;
                Decision::Engine {
                    plan: optimized.physical,
                    logical: plan,
                    explain,
                }
            }
            Err(error) => executor(
                Unrouted::of(error),
                logical.explain(operation, override_, None, None, &fired),
                plan,
            ),
        };
    }
    let refused = plan
        .live()
        .map(|(_, node)| node.kind())
        .find(|kind| coverage(*kind) == Coverage::RefusedByName);
    if let Some(kind) = refused {
        return executor(
            Unrouted::UnregisteredNode {
                kind: kind.name().to_string(),
            },
            LogicalView::of(&plan).explain(operation, override_, None, None, &fired),
            plan,
        );
    }
    let Some(entry) = lookup(&census) else {
        return executor(
            Unrouted::NoMatchingEntry { census },
            LogicalView::of(&plan).explain(operation, override_, None, None, &fired),
            plan,
        );
    };
    let lowered = physical_plan(&mut plan, source, bounds, fired.clone());
    let logical = LogicalView::of(&plan);
    let optimized = match lowered {
        Ok(optimized) => optimized,
        Err(error) => {
            return executor(
                Unrouted::of(error),
                logical.explain(operation, override_, Some(entry), None, &fired),
                plan,
            );
        }
    };
    if let Some(node) = over_bound(&optimized.physical, bounds) {
        return executor(
            Unrouted::DeclaredBytesOverBound { node },
            logical.explain(operation, override_, Some(entry), Some(&optimized), &fired),
            plan,
        );
    }
    let explain = logical.explain(operation, override_, Some(entry), Some(&optimized), &fired);
    match (entry.route, override_) {
        (Route::Executor, _) => executor(
            Unrouted::RegistryRouteExecutor { entry: entry.name },
            explain,
            plan,
        ),
        (Route::PlannerBehindFlag, RouteOverride::Registry) => executor(
            Unrouted::RegistryRouteExecutor { entry: entry.name },
            explain,
            plan,
        ),
        (Route::Planner | Route::PlannerBehindFlag, RouteOverride::ForceExecutor) => {
            executor(Unrouted::Override, explain, plan)
        }
        (Route::Planner, RouteOverride::Registry)
        | (Route::Planner | Route::PlannerBehindFlag, RouteOverride::ForcePlanner) => {
            Decision::Routed {
                plan: optimized.physical,
                entry,
                explain: explain.routed(),
            }
        }
    }
}

fn executor(reason: Unrouted, explain: Explain, logical: LogicalPlan) -> Decision {
    let explain = explain.with_reason(&reason);
    Decision::Executor {
        reason,
        explain,
        logical,
    }
}

/// The first node whose declared retained-memory limit exceeds the bound its
/// operator enforces: a build side wider than the executor's key cap (the
/// limit comes from manifest rows), or a hydration declared wider than its
/// chunk ceiling. A node kind with no operator bound is never compared.
pub fn over_bound(plan: &PhysicalPlan, bounds: &Bounds) -> Option<NodeId> {
    plan.live().map(|(id, _)| id).find(|id| {
        let bound = match plan.node(*id) {
            Some(PhysicalNode::SortMergeJoin { build: true, .. }) => bounds
                .key_width_bytes
                .saturating_mul(bounds.build_key_cap_rows),
            Some(PhysicalNode::HydrateByAddress { .. }) => bounds.hydration_chunk_hard_bytes,
            _ => return false,
        };
        plan.properties(*id)
            .and_then(|properties| properties.retained_limit)
            .is_some_and(|limit| limit > bound)
    })
}

struct LogicalView {
    json: Value,
    hash: u64,
}

impl LogicalView {
    fn of(plan: &LogicalPlan) -> Self {
        #[cfg(test)]
        EXPLAIN_RENDERS.with(|count| count.set(count.get() + 1));
        Self {
            json: plan.to_json(),
            hash: plan.structural_hash(),
        }
    }

    fn explain(
        &self,
        operation: OperationSummary,
        override_: RouteOverride,
        entry: Option<&'static Entry>,
        optimized: Option<&Optimized>,
        rewritten: &[&'static str],
    ) -> Explain {
        Explain {
            route: "executor",
            override_,
            reason: None,
            entry: entry.map(EntrySummary::of),
            operation,
            logical_plan: Some(self.json.clone()),
            logical_hash: Some(format!("{:016x}", self.hash)),
            physical_plan: optimized.map(|optimized| optimized.physical.to_json()),
            pipelines: optimized.map(|optimized| optimized.physical.pipelines_json()),
            statistics: optimized.map(|optimized| optimized.statistics.clone()),
            passes: optimized
                .map(|optimized| optimized.fired.clone())
                .unwrap_or_else(|| rewritten.to_vec()),
            ..Explain::without_plan(OperationSummary::empty(), override_)
        }
    }
}

#[cfg(test)]
thread_local! {
    static EXPLAIN_RENDERS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemorySource, NodeTypeSpec, TableRef};
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::SYSTEM_COLUMNS_V3;
    use omnigraph_compiler::ir::{IRExpr, IROp, IRProjection};
    use std::sync::Arc;

    use crate::fixture_bounds::BOUNDS;

    /// GQT can inspect explain output but cannot count diagnostic rendering.
    #[test]
    fn execution_planning_does_not_render_explain() {
        let source = MemorySource::default().with_node_type(
            "Doc",
            NodeTypeSpec {
                table: TableRef {
                    type_key: "node:Doc".into(),
                    dataset_path: "node/Doc".into(),
                    native_branch: None,
                },
                version: Some(1),
                columns: SYSTEM_COLUMNS_V3,
                schema: Arc::new(Schema::new(vec![Field::new("__id", DataType::Utf8, false)])),
                key: vec![],
                object_columns: vec!["__id".into()],
                row_count: None,
            },
        );
        let query = QueryIR {
            name: "documents".into(),
            params: vec![],
            pipeline: vec![IROp::NodeScan {
                variable: "d".into(),
                type_name: "Doc".into(),
                filters: vec![],
            }],
            return_exprs: vec![IRProjection {
                expr: IRExpr::Variable("d".into()),
                alias: None,
            }],
            order_by: vec![],
            limit: None,
        };
        EXPLAIN_RENDERS.with(|count| count.set(0));
        let physical = plan_query(&query, &source, &BOUNDS).expect("read plan");
        assert_eq!(
            EXPLAIN_RENDERS.with(|count| count.get()),
            0,
            "ordinary query planning rendered explain"
        );
        let decision = route(
            &Operation::Query(Box::new(query.clone())),
            &source,
            RouteOverride::Registry,
            &BOUNDS,
        );
        assert_eq!(
            EXPLAIN_RENDERS.with(|count| count.get()),
            1,
            "explicit explain must render diagnostics"
        );
        assert_eq!(
            decision.explain().physical_plan.as_ref(),
            Some(&physical.to_json())
        );
        let missing = MemorySource::default();
        let expected = route(
            &Operation::Query(Box::new(query.clone())),
            &missing,
            RouteOverride::Registry,
            &BOUNDS,
        );
        let Decision::Executor { reason, .. } = expected else {
            panic!("missing type must fail planning")
        };
        assert_eq!(plan_query(&query, &missing, &BOUNDS).unwrap_err(), reason);
    }
}
