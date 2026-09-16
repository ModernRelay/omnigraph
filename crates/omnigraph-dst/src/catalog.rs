//! The full crash-window catalog for the hunt
//! (`dst_hunt_crash_window_sweep`): 71 of the engine's decision seams
//! (`omnigraph::seams::catalog`) at the pinned engine version. A seam added
//! to the engine enters here as never-reached until its workload exists.
//!
//! Kept honest by `catalog_names_are_engine_seams` below: every entry must
//! be a name the engine catalog declares, so a typo'd or renamed-away window
//! fails the suite instead of compiling and silently never firing.

pub const CRASH_WINDOWS: [&str; 71] = [
    "blob_read.post_capture",
    "branch_control.post_recovery_barrier",
    "branch_create.post_native",
    "branch_delete.post_native",
    "branch_delete.post_table_gates",
    "branch_merge.adopt_after_append_pre_upsert",
    "branch_merge.adopt_after_upsert_pre_delete",
    "branch_merge.adopt_between_insert_chunks",
    "branch_merge.between_delete_chunks",
    "branch_merge.post_authority_capture",
    "branch_merge.post_candidate_validation",
    "branch_merge.post_table_effect",
    "branch_merge.post_phase_b_pre_manifest_commit",
    "branch_merge.post_fork_pre_commit",
    "branch_merge.rewrite_after_delete_pre_confirm",
    "branch_merge.rewrite_after_merge_pre_delete",
    "classify.fresh_read",
    "cleanup.post_recovery_check_pre_gates",
    "cleanup.reconcile_fork",
    "cleanup.resolve_branch_snapshot",
    "cleanup.table_gc",
    "ensure_indices.post_publish_pre_promotion",
    "ensure_indices.post_phase_b_pre_manifest_commit",
    "ensure_indices.post_fork_pre_commit",
    "ensure_indices.post_stage_pre_commit_btree",
    "ensure_indices.post_table_effect",
    "fork.before_classify",
    "fork.post_create_pre_open",
    "graph_publish.after_manifest_commit",
    "graph_publish.before_commit_append",
    "init.after_coordinator_init",
    "init.after_schema_contract_written",
    "init.after_schema_pg_written",
    "init.post_manifest_create",
    "load.between_table_stages",
    "load.post_branch_create_pre_stage",
    "mutation.delete_node_pre_primary_delete",
    "mutation.post_finalize_pre_publisher",
    "mutation.post_fork_pre_commit",
    "mutation.post_no_effect_pre_gate",
    "mutation.post_publish_pre_promotion",
    "mutation.post_stage_pre_effect_gate",
    "mutation.post_table_commit",
    "open.before_schema_contract_read",
    "optimize.before_compact",
    "optimize.post_compact_pre_reindex",
    "optimize.post_authority_capture_pre_gates",
    "optimize.post_phase_b_pre_manifest_commit",
    "optimize.post_recovery_check_pre_main_gate",
    "publish.load_state",
    "read.refresh_post_state_pre_lineage",
    "recovery.before_roll_forward_publish",
    "recovery.orphan_discard_audit_append",
    "recovery.post_list_pre_gates",
    "recovery.post_rollback_publish_pre_audit",
    "recovery.post_sidecar_list_pre_read",
    "recovery.post_table_restore_pre_publish",
    "recovery.record_audit",
    "recovery.sidecar_confirm",
    "recovery.sidecar_delete",
    "recovery.sidecar_list",
    "recovery.sidecar_write",
    "schema_apply.after_manifest_commit",
    "schema_apply.after_staging_write",
    "schema_apply.before_staging_write",
    "schema_apply.post_sidecar_pre_effect",
    "schema_apply.post_table_commit",
    "schema_reload.before_contract_read",
    "storage.local_create_if_absent_probe",
    // Append new windows so index-derived census seeds for existing windows
    // remain stable.
    "branch_merge.rewrite_after_insert_pre_update",
    "branch_merge.post_publish_pre_promotion",
];

#[cfg(test)]
mod tests {
    use super::CRASH_WINDOWS;

    /// The catalog's names-guard (module doc): every entry must be a seam
    /// the engine catalog declares.
    #[test]
    fn catalog_names_are_engine_seams() {
        let engine_names: std::collections::BTreeSet<&str> = omnigraph::seams::catalog::ALL
            .iter()
            .map(|seam| seam.name())
            .collect();
        let missing: Vec<&&str> = CRASH_WINDOWS
            .iter()
            .filter(|w| !engine_names.contains(**w))
            .collect();
        assert!(
            missing.is_empty(),
            "catalog windows the engine does not define (typo or renamed \
             away — such a window silently never fires): {missing:?}"
        );
    }
}
