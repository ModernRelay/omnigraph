//! Executable read planning and diagnostic routing. Read planning returns a
//! physical plan or a typed failure; routing includes explain diagnostics and
//! sends unsupported operations to the executor with a typed reason.

use omnigraph_compiler::ir::QueryIR;
use serde_json::{Value, json};

use crate::explain::{EntrySummary, Explain, OperationSummary};
use crate::logical::{Census, LogicalPlan};
use crate::operation::Operation;
use crate::optimizer::{Bounds, Optimized, physical_plan, resolve, rewrite};
use crate::physical::{NodeId, PhysicalNode, PhysicalPlan};
use crate::registry::{Coverage, Entry, Route, coverage, lookup};
use crate::route::RouteOverride;
use crate::source::PlanSource;

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
}

impl Unrouted {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UnregisteredNode { .. } => "unregistered_node",
            Self::NoMatchingEntry { .. } => "no_matching_entry",
            Self::RegistryRouteExecutor { .. } => "registry_route_executor",
            Self::DeclaredBytesOverBound { .. } => "declared_bytes_over_bound",
            Self::Override => "override",
            Self::PlannerError { .. } => "planner_error",
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
            Self::PlannerError { message } => json!({ "kind": self.kind(), "message": message }),
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
    let mut logical = resolve(&operation, source).map_err(|error| Unrouted::PlannerError {
        message: error.to_string(),
    })?;
    crate::optimizer::optimize(&mut logical, source, bounds)
        .map(|optimized| optimized.physical)
        .map_err(|error| Unrouted::PlannerError {
            message: error.to_string(),
        })
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
    let mut plan = match resolve(op, source) {
        Ok(plan) => plan,
        Err(error) => {
            return executor(
                Unrouted::PlannerError {
                    message: error.to_string(),
                },
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
            return executor(
                Unrouted::PlannerError {
                    message: error.to_string(),
                },
                explain,
                plan,
            );
        }
    };
    if matches!(op, Operation::Query(_)) {
        let lowered = physical_plan(&mut plan, source, bounds, fired.clone());
        let logical = LogicalView::of(&plan);
        return match lowered {
            Ok(optimized) => {
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
                Unrouted::PlannerError {
                    message: error.to_string(),
                },
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
                Unrouted::PlannerError {
                    message: error.to_string(),
                },
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
