pub mod commit_graph;
pub mod graph_coordinator;
pub mod manifest;
mod omnigraph;
mod recovery_audit;
mod schema_state;
pub(crate) mod write_queue;

pub use commit_graph::GraphCommit;
pub use graph_coordinator::{GraphCoordinator, ReadTarget, ResolvedTarget, SnapshotId};
pub use manifest::{Snapshot, SubTableEntry, SubTableUpdate};
pub(crate) use omnigraph::ensure_public_branch_ref;
pub use omnigraph::{
    CleanupPolicyOptions, InitOptions, MergeOutcome, Omnigraph, OpenMode, SchemaApplyOptions,
    SchemaApplyResult, SkipReason, TableCleanupStats, TableOptimizeStats,
};

pub(crate) const SCHEMA_APPLY_LOCK_BRANCH: &str = "__schema_apply_lock__";

/// Mutation kind, threaded through the version-check call sites so the
/// engine can apply an op-kind-aware policy:
///
/// - `Insert` / `Merge`: skip the strict pre-stage write precondition
///   (`Omnigraph::ensure_writable_or_defer`). Lance's `MergeInsertBuilder`
///   rebases concurrent appends; the per-(table, branch) writer queue
///   serializes `commit_staged`; the publisher's CAS (refreshed under the
///   queue via `MutationStaging::commit_all`'s `snapshot_for_branch` call)
///   catches genuine cross-process drift as
///   `ManifestConflictDetails::ExpectedVersionMismatch`. The pre-stage check
///   would over-reject in-process concurrent inserts, which is exactly the
///   case PR 2 / MR-686 designed the per-table queue to allow.
///
/// - `Update` / `Delete`: keep the strict precondition. These have
///   read-modify-write semantics, so the staged batch must be computed against
///   current committed state. The OCC fence is the *fresh* manifest pin: a
///   stale handle (the caller's view is behind the live manifest) is rejected
///   as 409 before any staged commit, while benign content-preserving drift
///   (Lance HEAD ahead of the manifest with no recovery sidecar) is tolerated.
///   SERIALIZABLE opt-in (§VI.36 future seam) is the long-term answer for
///   tighter semantics; today, in-process update-update races on the same key
///   stay rejected as 409 — acceptable.
///
/// - `SchemaRewrite`: keep the strict precondition. Schema apply runs under the
///   graph-wide `__schema_apply_lock__` AND per-table queues; the check is
///   uncontested at that point (it tolerates benign drift and defers only on a
///   foreign recovery sidecar).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationOpKind {
    Insert,
    Merge,
    Update,
    Delete,
    SchemaRewrite,
}

impl MutationOpKind {
    /// Whether the strict pre-stage write precondition
    /// (`Omnigraph::ensure_writable_or_defer`) should fire for this op kind.
    /// See [`MutationOpKind`] for the rationale per kind.
    pub(crate) fn strict_pre_stage_version_check(self) -> bool {
        match self {
            MutationOpKind::Insert | MutationOpKind::Merge => false,
            MutationOpKind::Update | MutationOpKind::Delete | MutationOpKind::SchemaRewrite => true,
        }
    }
}

pub(crate) fn is_schema_apply_lock_branch(name: &str) -> bool {
    name.trim_start_matches('/') == SCHEMA_APPLY_LOCK_BRANCH
}

pub(crate) fn is_internal_system_branch(name: &str) -> bool {
    // Legacy `__run__*` staging branches (Run state machine, removed MR-771)
    // are swept off `__manifest` by the v2→v3 internal-schema migration, so the
    // only internal branch the engine still creates is the schema-apply lock.
    is_schema_apply_lock_branch(name)
}
