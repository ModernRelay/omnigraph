//! Planner and optimizer for Omnigraph operations (RFC 0068).
//!
//! The crate owns four things: the logical plan an operation resolves to, the
//! physical plan the optimizer selects, the fixed pass sequence between them,
//! and the routing registry for change-feed and merge shapes. Read queries
//! always produce an engine plan; other operations retain their executor
//! until a plan route has integration and differential evidence. The planner
//! reads no environment and opens no data: every statistic a pass consults comes
//! through [`PlanSource`], which the engine implements over its pinned
//! snapshot, and every physical node is executed by an engine operator.
//!
//! [`plan_query`] builds executable read plans. [`route`] returns routing
//! decisions with explain diagnostics for an [`Operation`]. Traversals
//! resolve to topology-only `Expand` followed by a `Scan` restricted to input
//! identities. The scan owns the pinned destination read, storage predicate and
//! projection, preserving
//! traversal multiplicity and order; ranking remains a separate contract.
//! Query traversal schemas in this crate remain conservative input schemas;
//! the engine derives their complete runtime output schemas from the catalog.

pub mod cost;
pub mod error;
pub mod explain;
pub mod gate;
pub mod logical;
pub mod operation;
pub mod optimizer;
pub mod physical;
pub mod registry;
pub mod route;
pub mod source;

pub use cost::{
    AccessPath, CSR_BUILD_FACTOR, ExpandCostInputs, ExpandMode, HASH_JOIN_POOL_DIVISOR,
    HASH_JOIN_RATIO, IndexCoverage, choose_access_path, choose_expand_mode, cost_effective_hops,
    direction_probe_factor, estimate_rows, executed_hops, scan_row_estimate, should_switch_to_csr,
};
pub use error::PlanError;
pub use explain::Explain;
pub use gate::{Decision, Unrouted, plan_query, route};
pub use logical::{
    Census, ColumnRef, JoinKind, KeyJoinKind, LogicalId, LogicalKind, LogicalNode, LogicalPlan,
    Predicate, ScanSpec,
};
pub use operation::{Operation, PageBudgetSpec, ScopeSpec, Side, TableRef};
pub use optimizer::{Bounds, physical_plan, rewrite};
pub use physical::{
    Estimate, NodeId, PhysicalNode, PhysicalPlan, Properties, ScanInput, StatisticSource,
};
pub use registry::{Coverage, Entry, Route, Shape};
pub use route::RouteOverride;
pub use source::{
    AdjacencyProof, ExpandStatistics, FragmentStat, MemorySource, NodeTypeSpec, PlanSource, SideId,
};

#[cfg(test)]
#[path = "../tests/support/bounds.rs"]
mod fixture_bounds;
