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
pub use omnigraph::{
    CleanupPolicyOptions, EXPORT_CHUNK_MAX_BYTES, ExportCut, InitOptions, MergeOutcome, Omnigraph,
    OpenMode, PendingIndex, RepairAction, RepairClassification, RepairOptions, RepairStats,
    SchemaApplyOptions, SchemaApplyResult, SkipReason, TableCleanupStats, TableOptimizeStats,
    TableRepairStats,
};
pub(crate) use omnigraph::{DeferredTableFork, WriteAuthorityToken, WriteTxn};
pub(crate) use omnigraph::{export_blob_values, logical_row_image};

use crate::error::{OmniError, Result};

/// Process-local exclusion shared by immutable export cuts and cooperative
/// whole-root destructive control. It grants no storage or graph authority.
#[doc(hidden)]
#[must_use = "dropping the guard releases destructive root control"]
pub struct ExportRootExclusion {
    _permit: write_queue::ExportDestructivePermit,
}

/// Nonwaitingly reserve one graph root against a live immutable export cut.
#[doc(hidden)]
pub fn reserve_export_root_exclusion(graph_uri: &str) -> Result<ExportRootExclusion> {
    let normalized = crate::storage::normalize_root_uri(graph_uri)?;
    let identity = crate::storage::write_queue_root_identity(&normalized)?;
    let manager = write_queue::WriteQueueManager::for_root(&identity);
    let permit = manager.try_acquire_export_destructive().ok_or_else(|| {
        OmniError::ResourceLimitExceeded {
            resource: "stream_export_slots".to_string(),
            limit: 1,
            actual: 2,
        }
    })?;
    Ok(ExportRootExclusion { _permit: permit })
}

pub(crate) const SCHEMA_APPLY_LOCK_BRANCH: &str = "__schema_apply_lock__";
/// Persisted graph-level property identity carried by each user property's
/// physical Lance field.  Historical consumers compare this authority rather
/// than inferring property lifetime from a Lance field id or field position.
pub(crate) const STABLE_PROPERTY_ID_METADATA_KEY: &str = "omnigraph.stable_property_id";

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
