pub mod commit_graph;
mod graph_coordinator;
pub mod manifest;
mod omnigraph;
mod recovery_audit;
mod schema_state;
pub(crate) mod write_queue;

pub use commit_graph::GraphCommit;
pub use graph_coordinator::{ReadTarget, ResolvedTarget, SnapshotId};
pub use manifest::{Snapshot, SnapshotScanner, SnapshotTable, SubTableEntry, SubTableUpdate};
pub(crate) use omnigraph::ensure_public_branch_ref;
pub(crate) use omnigraph::{DeferredTableFork, WriteAuthorityToken, WriteTxn};
pub use omnigraph::{
    BlobContent, BlobRead, BlobReader, BlobVersionTag, CleanupPolicyOptions, InitOptions,
    MergeOutcome, Omnigraph, OpenMode, PendingIndex, RepairAction, RepairClassification,
    RepairOptions, RepairStats, SchemaApplyOptions, SchemaApplyResult, SkipReason,
    TableCleanupStats, TableOptimizeStats, TableRepairStats,
};

use crate::error::{OmniError, Result};

pub(crate) const SCHEMA_APPLY_LOCK_BRANCH: &str = "__schema_apply_lock__";

/// Mutation kind, threaded through the early table-version checks so the
/// engine can apply an op-kind-aware staging policy. This check is not the
/// RFC-022 publish authority: enrolled mutation/load attempts additionally
/// capture an exact branch-wide `WriteTxn`, then revalidate it while holding
/// the root-shared schema → branch → sorted-table gates.
///
/// - `Insert` / `Merge`: skip the strict pre-stage `ensure_expected_version`
///   check because their staged files are reclaimable and the complete prepared
///   attempt is checked later against the exact branch authority. On a
///   pre-effect `ReadSetChanged`, mutation Insert and load Append/Merge discard
///   the whole attempt and reprepare with a bounded retry; they never patch
///   table pins beneath an already-validated plan.
///
/// - `Update` / `Delete`: keep the strict early check because these are
///   read-modify-write effects computed from a pinned image. Enrolled attempts
///   still perform the later branch-wide revalidation; a mismatch is strict
///   `ReadSetChanged` (HTTP 409), not a transparent replay.
///
/// - `SchemaRewrite`: keep the strict early check for overwrite/rewrite effects.
///   An enrolled load Overwrite also uses the exact branch-wide gate and
///   surfaces `ReadSetChanged`; schema apply has its own schema/table protocol.
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

/// Microseconds since the UNIX epoch — the `created_at` stamp threaded through
/// every graph-lineage / recovery-audit / commit-graph row. One canonical
/// helper so the clock-error mapping (variant + message) cannot drift across
/// the call sites that record those timestamps.
pub(crate) fn now_micros() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| OmniError::manifest(format!("system clock before UNIX_EPOCH: {e}")))?;
    Ok(duration.as_micros() as i64)
}
