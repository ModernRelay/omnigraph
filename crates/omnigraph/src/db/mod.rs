pub mod commit_graph;
pub mod graph_coordinator;
pub mod manifest;
mod omnigraph;
mod run_registry;
mod schema_state;

pub use commit_graph::GraphCommit;
pub use graph_coordinator::{GraphCoordinator, ReadTarget, ResolvedTarget, SnapshotId};
pub use manifest::{Snapshot, SubTableEntry, SubTableUpdate};
pub(crate) use omnigraph::ensure_public_branch_ref;
pub use omnigraph::{
    CleanupPolicyOptions, MergeOutcome, Omnigraph, SchemaApplyResult, TableCleanupStats,
    TableOptimizeStats,
};
pub(crate) use run_registry::is_internal_run_branch;

pub(crate) const SCHEMA_APPLY_LOCK_BRANCH: &str = "__schema_apply_lock__";

pub(crate) fn is_schema_apply_lock_branch(name: &str) -> bool {
    name.trim_start_matches('/') == SCHEMA_APPLY_LOCK_BRANCH
}

pub(crate) fn is_internal_system_branch(name: &str) -> bool {
    is_internal_run_branch(name) || is_schema_apply_lock_branch(name)
}
