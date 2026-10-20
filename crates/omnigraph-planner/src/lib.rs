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
//! decisions for an [`Operation`]. A query decision stays pending until
//! [`Decision::finalize`] awaits scan access and publishes its explain. Traversals
//! resolve to topology-only `Expand` followed by a `Scan` restricted to input
//! identities. The scan owns the pinned destination read, storage predicate,
//! projection and, for a ranked binding, the [`RankedAccess`] the index
//! computes while scanning, preserving traversal multiplicity and order.
//! Query traversal schemas in this crate remain conservative input schemas;
//! the engine derives their complete runtime output schemas from the catalog.

pub mod aggregate;
pub mod bound;
pub mod cost;
pub mod error;
pub mod explain;
pub mod gate;
mod key_set;
pub mod logical;
pub mod lower;
pub mod mirror;
pub mod operation;
pub mod optimizer;
pub mod output;
pub mod passes;
pub mod physical;
pub mod registry;
pub mod route;
pub mod scan_access;
pub mod source;
mod typed;
pub mod validate;

pub use aggregate::{
    Accumulator, AggregateSpec, Overflow, plan_aggregate, plan_block_aggregate,
    validate_aggregate_specs,
};
pub use bound::{BOUND_PLAN_VERSION, BoundPlan, ValueTable};
pub use cost::{
    AccessPath, AntiJoinStrategy, CSR_BUILD_FACTOR, CoverageProvenance, ExpandCostInputs,
    ExpandMode, ExpandPolicy, HASH_JOIN_POOL_DIVISOR, HASH_JOIN_RATIO, HYDRATE_ROW_RATIO,
    IndexCoverage, PROBE_CHUNK_KEYS, SOURCE_PARTITION_BYTES, ValidateAccess, ValidateCostInputs,
    ValidateIndexCoverage, chain_rows, choose_access_path, choose_anti_join_strategy,
    choose_expand_mode, choose_validate_access, direction_probe_factor, estimate_rows,
    executed_hops, hydrate_chunk_bytes, scan_row_estimate, should_switch_to_csr,
};
pub use error::PlanError;
pub use explain::Explain;
pub use gate::{Decision, Unrouted, accept_query, accept_query_explained, plan_query, route};
pub use logical::{
    Census, ColumnRef, ConsultedImage, CsvSourceSpec, FragmentWriteSpec, IndexQuery, JoinKind,
    KeyJoinKind, LogicalId, LogicalKind, LogicalNode, LogicalPlan, Predicate, RuntimeFilterKind,
    RuntimeFilterSpec, RuntimeInput, ScanAccess, ScanSpec, SearchArm, ValidateSpec,
};
pub use lower::{
    AggregateFields, ContainsJoinFields, CsvSourceFields, ExpandFields, FragmentWriteFields,
    HashJoinFields, Lower, RankFuseFields, SortMergeJoinFields, ValidateFields,
};
pub use operation::{Operation, PageBudgetSpec, ScopeSpec, Side, TableRef, TableSource};
pub use optimizer::{Bounds, physical_plan, rewrite};
pub use output::{NodeObjectType, validate_output_schemas};
pub use passes::{PassId, PassRow, PassStage, pass_rows};
pub use physical::{
    Assumptions, DatasetPin, Eligibility, EmptyEligible, Estimate, GatePolicy, Hop,
    HydratedBinding, HydratedColumn, NearestPolicy, NodeId, OperatorRow, OrderKey, OverfetchRung,
    PhysicalNode, PhysicalNodeKind, PhysicalPlan, PipelineSink, PipelineSource, PipelineSpec,
    Prefilter, PrefilterMode, Properties, ROW_ADDRESS_PREFIX, RankArm, RankKind, RankScope,
    RankedAccess, ScanInput, SideChain, StatisticSource, operator_rows,
};
pub use registry::{Coverage, Entry, Route, Shape};
pub use route::RouteOverride;
pub use source::{
    AdjacencyProof, EXPAND_INDEXED_MAX_FRONTIER_ENV, EXPAND_INDEXED_MAX_HOPS_ENV, ExpandStatistics,
    FragmentCoverage, FragmentStat, FullTextCoverage, IndexFact, IndexKind, IndexSplitFuture,
    MemorySource, NodeTypeSpec, PlanSource, SideId,
};
pub use validate::{
    AcceptInput, AcceptedBoundPlan, AcceptedPlan, Evidence, KeyIds, ReplayEnvelope, ReplayQuery,
    ReplayRefusal, ValidationError, ValidationLimits, ValidationScope, ValidationSummary,
    accept_replay, catalog_digest, decode_replay,
};

#[cfg(test)]
#[path = "../tests/support/bounds.rs"]
mod fixture_bounds;
