use crate::error::Result;

pub(crate) fn maybe_fail(_name: &str) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        let name = _name;
        fail::fail_point!(name, |_| {
            return Err(crate::error::OmniError::manifest(format!(
                "injected failpoint triggered: {}",
                name
            )));
        });
    }
    Ok(())
}

/// Failpoint that injects a *retryable* `RowLevelCasContention` `OmniError` — the
/// typed conflict the manifest publisher's outer retry treats as retryable
/// (`is_retryable_publish_conflict`). Used to drive the publisher's
/// retry-on-`load_publish_state`-error path deterministically, a path otherwise
/// reachable only under sustained multi-writer contention.
/// A no-op without the `failpoints` feature.
#[allow(unused_variables)]
pub(crate) fn maybe_fail_retryable_contention(name: &str) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        fail::fail_point!(name, |_| {
            return Err(crate::error::OmniError::manifest_row_level_cas_contention(
                format!("injected retryable contention failpoint: {name}"),
            ));
        });
    }
    Ok(())
}

/// Compile-checked catalog of every failpoint name in this crate. Call sites
/// (`maybe_fail`) and tests (`ScopedFailPoint` / the test rendezvous helper)
/// reference these constants instead of bare string literals, so a typo is a
/// compile error rather than a silently-never-firing failpoint.
pub mod names {
    pub const BRANCH_CREATE_AFTER_MANIFEST_BRANCH_CREATE: &str = "branch_create.after_manifest_branch_create";
    pub const BRANCH_DELETE_BEFORE_COMMIT_GRAPH_RECLAIM: &str = "branch_delete.before_commit_graph_reclaim";
    pub const BRANCH_DELETE_BEFORE_TABLE_CLEANUP: &str = "branch_delete.before_table_cleanup";
    pub const BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT: &str = "branch_merge.adopt_after_append_pre_upsert";
    pub const BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE: &str = "branch_merge.adopt_after_upsert_pre_delete";
    pub const BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT: &str = "branch_merge.post_phase_b_pre_manifest_commit";
    pub const BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_INDEX: &str = "branch_merge.rewrite_after_delete_pre_index";
    pub const BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE: &str = "branch_merge.rewrite_after_merge_pre_delete";
    pub const CLASSIFY_FRESH_READ: &str = "classify.fresh_read";
    pub const CLEANUP_RECONCILE_FORK: &str = "cleanup.reconcile_fork";
    pub const CLEANUP_RESOLVE_BRANCH_SNAPSHOT: &str = "cleanup.resolve_branch_snapshot";
    pub const CLEANUP_TABLE_GC: &str = "cleanup.table_gc";
    pub const ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT: &str = "ensure_indices.post_phase_b_pre_manifest_commit";
    pub const ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE: &str = "ensure_indices.post_stage_pre_commit_btree";
    pub const FORK_BEFORE_CLASSIFY: &str = "fork.before_classify";
    pub const FORK_BEFORE_RECLAIM: &str = "fork.before_reclaim";
    pub const GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT: &str = "graph_publish.after_manifest_commit";
    pub const GRAPH_PUBLISH_BEFORE_COMMIT_APPEND: &str = "graph_publish.before_commit_append";
    pub const INIT_AFTER_COORDINATOR_INIT: &str = "init.after_coordinator_init";
    pub const INIT_AFTER_SCHEMA_CONTRACT_WRITTEN: &str = "init.after_schema_contract_written";
    pub const INIT_AFTER_SCHEMA_PG_WRITTEN: &str = "init.after_schema_pg_written";
    pub const MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE: &str = "mutation.delete_node_pre_primary_delete";
    pub const MUTATION_POST_FINALIZE_PRE_PUBLISHER: &str = "mutation.post_finalize_pre_publisher";
    pub const OPTIMIZE_BEFORE_COMPACT: &str = "optimize.before_compact";
    pub const OPTIMIZE_INJECT_REINDEX_CONFLICT: &str = "optimize.inject_reindex_conflict";
    pub const OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT: &str = "optimize.post_phase_b_pre_manifest_commit";
    pub const RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH: &str = "recovery.before_roll_forward_publish";
    pub const RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND: &str = "recovery.orphan_discard_audit_append";
    pub const RECOVERY_RECORD_AUDIT: &str = "recovery.record_audit";
    pub const RECOVERY_SIDECAR_CONFIRM: &str = "recovery.sidecar_confirm";
    pub const RECOVERY_SIDECAR_DELETE: &str = "recovery.sidecar_delete";
    pub const RECOVERY_SIDECAR_LIST: &str = "recovery.sidecar_list";
    pub const RECOVERY_SIDECAR_WRITE: &str = "recovery.sidecar_write";
    pub const SCHEMA_APPLY_AFTER_MANIFEST_COMMIT: &str = "schema_apply.after_manifest_commit";
    pub const SCHEMA_APPLY_AFTER_STAGING_WRITE: &str = "schema_apply.after_staging_write";
    pub const SCHEMA_APPLY_BEFORE_STAGING_WRITE: &str = "schema_apply.before_staging_write";
    /// Injects a retryable `RowLevelCasContention` from `load_publish_state` so a
    /// test can prove the publisher's outer retry re-runs the load.
    pub const PUBLISH_LOAD_STATE_RETRYABLE_CONTENTION: &str =
        "publish.load_state_retryable_contention";
}

#[cfg(feature = "failpoints")]
pub struct ScopedFailPoint {
    name: String,
}

#[cfg(feature = "failpoints")]
impl ScopedFailPoint {
    pub fn new(name: &str, action: &str) -> Self {
        fail::cfg(name, action).expect("configure failpoint");
        Self {
            name: name.to_string(),
        }
    }

    /// Register a callback failpoint with the same Drop-based cleanup as
    /// `new`. Without the guard, a panic while the point is active would
    /// leak the callback into the process-global registry and fire it under
    /// later tests in the same binary.
    pub fn with_callback<F>(name: &str, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        fail::cfg_callback(name, callback).expect("configure callback failpoint");
        Self {
            name: name.to_string(),
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for ScopedFailPoint {
    fn drop(&mut self) {
        fail::remove(&self.name);
    }
}
