//! Planner and optimizer for Omnigraph operations (RFC 0068).
//!
//! The crate owns four things: the logical plan an operation resolves to, the
//! physical plan the optimizer selects, the fixed pass sequence between them,
//! and the routing registry for change-feed and merge shapes. Read queries
//! always produce an engine plan; other operations retain their executor
//! until a plan route has integration and differential evidence. The planner
//! reads no environment and opens no data: every statistic a pass consults comes
//! through [`PlanSource`], which the engine implements over its pinned
//! snapshot, and every physical node is executed by an engine operator: the
//! [`Lower`] trait has one method per [`PhysicalNode`] variant,
//! [`PhysicalPlan::lower`] walks the plan itself, and the engine's report
//! names an operator of every node (`docs/dev/execution.md`, "V2 plan
//! lowering and operator ownership").
//!
//! [`plan_query`] builds executable read plans. [`route`] returns routing
//! decisions with explain diagnostics for an [`Operation`]. Traversals
//! resolve to topology-only `Expand` followed by a `Scan` restricted to input
//! identities. The scan owns the pinned destination read, storage predicate,
//! projection and, for a ranked binding, the [`RankedAccess`] the index
//! computes while scanning, preserving traversal multiplicity and order.
//! Query traversal schemas in this crate remain conservative input schemas;
//! the engine derives their complete runtime output schemas from the catalog.

pub mod bound;
pub mod cost;
pub mod error;
pub mod explain;
pub mod gate;
pub mod logical;
pub mod lower;
pub mod mirror;
pub mod operation;
pub mod optimizer;
pub mod physical;
pub mod registry;
pub mod route;
pub mod source;

pub use bound::{BoundPlan, ValueTable};
pub use cost::{
    AccessPath, CSR_BUILD_FACTOR, ExpandCostInputs, ExpandMode, ExpandPolicy,
    HASH_JOIN_POOL_DIVISOR, HASH_JOIN_RATIO, IndexCoverage, choose_access_path, choose_expand_mode,
    cost_effective_hops, direction_probe_factor, estimate_rows, executed_hops, scan_row_estimate,
    should_switch_to_csr,
};
pub use error::PlanError;
pub use explain::Explain;
pub use gate::{Decision, Unrouted, plan_query, route};
pub use logical::{
    Census, ColumnRef, JoinKind, KeyJoinKind, LogicalId, LogicalKind, LogicalNode, LogicalPlan,
    Predicate, ScanSpec, SearchArm,
};
pub use lower::{ExpandFields, HashJoinFields, Lower, SortMergeJoinFields};
pub use operation::{Operation, PageBudgetSpec, ScopeSpec, Side, TableRef};
pub use optimizer::{Bounds, physical_plan, rewrite};
pub use physical::{
    Assumptions, DatasetPin, Estimate, GatePolicy, Hop, NodeId, OverfetchRung, PhysicalNode,
    PhysicalPlan, Prefilter, PrefilterMode, Properties, RankArm, RankKind, RankScope, RankedAccess,
    ScanInput, StatisticSource,
};
pub use registry::{Coverage, Entry, Route, Shape};
pub use route::RouteOverride;
pub use source::{
    AdjacencyProof, EXPAND_INDEXED_MAX_FRONTIER_ENV, EXPAND_INDEXED_MAX_HOPS_ENV, ExpandStatistics,
    FragmentStat, MemorySource, NodeTypeSpec, PlanSource, SideId,
};

#[cfg(test)]
#[path = "../tests/support/bounds.rs"]
mod fixture_bounds;
