pub mod commit_graph;
pub mod graph_coordinator;
pub mod manifest;
mod omnigraph;
mod recovery_audit;
mod run_registry;
mod schema_state;
pub(crate) mod write_queue;

pub use commit_graph::GraphCommit;
pub use graph_coordinator::{GraphCoordinator, ReadTarget, ResolvedTarget, SnapshotId};
pub use manifest::{Snapshot, SubTableEntry, SubTableUpdate};
pub use omnigraph::{
    CleanupPolicyOptions, MergeOutcome, Omnigraph, OpenMode, SchemaApplyOptions,
    SchemaApplyResult, TableCleanupStats, TableOptimizeStats,
};
pub(crate) use omnigraph::ensure_public_branch_ref;
pub(crate) use run_registry::is_internal_run_branch;

pub(crate) const SCHEMA_APPLY_LOCK_BRANCH: &str = "__schema_apply_lock__";

/// Mutation kind, threaded through the version-check call sites so the
/// engine can apply an op-kind-aware policy:
///
/// - `Insert` / `Merge`: skip the strict pre-stage `ensure_expected_version`
///   check. Lance's `MergeInsertBuilder` rebases concurrent appends; the
///   per-(table, branch) writer queue serializes `commit_staged`; the
///   publisher's CAS (refreshed under the queue via
///   `MutationStaging::commit_all`'s `snapshot_for_branch` call) catches
///   genuine cross-process drift as `ManifestConflictDetails::ExpectedVersionMismatch`.
///   The pre-stage strict check would over-reject in-process concurrent
///   inserts, which is exactly the case PR 2 / MR-686 designed the
///   per-table queue to allow.
///
/// - `Update` / `Delete`: keep the strict check. These have read-modify-write
///   semantics; Lance moving between the read at stage time and the write
///   at commit time means the staged batch is computed against stale state.
///   The strict check guards the per-query SI invariant. SERIALIZABLE
///   opt-in (§VI.36 future seam) is the long-term answer for tighter
///   semantics; today, in-process update-update races on the same key
///   stay rejected as 409 — acceptable.
///
/// - `SchemaRewrite`: keep the strict check. Schema apply runs under the
///   graph-wide `__schema_apply_lock__` AND per-table queues; the strict
///   check is uncontested at that point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationOpKind {
    Insert,
    Merge,
    Update,
    Delete,
    SchemaRewrite,
}

impl MutationOpKind {
    /// Whether the strict pre-stage `ensure_expected_version` check should
    /// fire for this op kind. See [`MutationOpKind`] for the rationale per
    /// kind.
    pub(crate) fn strict_pre_stage_version_check(self) -> bool {
        match self {
            MutationOpKind::Insert | MutationOpKind::Merge => false,
            MutationOpKind::Update
            | MutationOpKind::Delete
            | MutationOpKind::SchemaRewrite => true,
        }
    }
}

pub(crate) fn is_schema_apply_lock_branch(name: &str) -> bool {
    name.trim_start_matches('/') == SCHEMA_APPLY_LOCK_BRANCH
}

pub(crate) fn is_internal_system_branch(name: &str) -> bool {
    is_internal_run_branch(name) || is_schema_apply_lock_branch(name)
}
