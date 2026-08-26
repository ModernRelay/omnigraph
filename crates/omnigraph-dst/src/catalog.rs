//! The full crash-window catalog for the hunt
//! (`dst_hunt_crash_window_sweep`): the engine's `src/failpoints.rs`
//! name set, 71 windows at the pinned engine version. A window added to
//! the engine enters here as never-reached until its workload exists.
//!
//! Kept honest by `catalog_names_are_engine_failpoints` below: every
//! entry must be a name the engine's `names` module defines, so a
//! typo'd or renamed-away window fails the suite instead of compiling
//! and silently never firing. (Swapping the literals for the
//! `names::*` consts directly would be stronger still; the guard covers
//! the failure mode until then.)

pub const CRASH_WINDOWS: [&str; 71] = [
    "blob_read.post_capture",
    "branch_control.post_recovery_barrier",
    "branch_create.post_native",
    "branch_delete.before_table_cleanup",
    "branch_delete.post_native",
    "branch_delete.post_table_gates",
    "branch_merge.adopt_after_append_pre_upsert",
    "branch_merge.adopt_after_upsert_pre_delete",
    "branch_merge.adopt_between_insert_chunks",
    "branch_merge.between_delete_chunks",
    "branch_merge.post_authority_capture",
    "branch_merge.post_candidate_validation",
    "branch_merge.post_effects_pre_confirm",
    "branch_merge.post_phase_b_pre_manifest_commit",
    "branch_merge.post_sidecar_pre_fork",
    "branch_merge.rewrite_after_delete_pre_confirm",
    "branch_merge.rewrite_after_merge_pre_delete",
    "classify.fresh_read",
    "cleanup.post_recovery_check_pre_gates",
    "cleanup.reconcile_fork",
    "cleanup.resolve_branch_snapshot",
    "cleanup.table_gc",
    "ensure_indices.post_effects_pre_confirm",
    "ensure_indices.post_phase_b_pre_manifest_commit",
    "ensure_indices.post_sidecar_pre_fork",
    "ensure_indices.post_stage_pre_commit_btree",
    "ensure_indices.post_table_effect",
    "fork.before_classify",
    "fork.before_reclaim",
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
    "mutation.post_sidecar_pre_fork",
    "mutation.post_stage_pre_effect_gate",
    "mutation.post_table_commit",
    "open.before_schema_contract_read",
    "optimize.before_compact",
    "optimize.inject_reindex_conflict",
    "optimize.post_authority_capture_pre_gates",
    "optimize.post_phase_b_pre_manifest_commit",
    "optimize.post_recovery_check_pre_main_gate",
    "publish.load_state_retryable_contention",
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
];

#[cfg(test)]
mod tests {
    use super::CRASH_WINDOWS;

    /// The catalog's names-guard (module doc): every entry must be a
    /// string the engine's `names` module defines. Textual, like the
    /// engine's own `failpoint_names_guard.rs` — the engine exposes no
    /// iterable of its failpoint names, so the source is the authority.
    #[test]
    fn catalog_names_are_engine_failpoints() {
        let engine_src =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../omnigraph/src/failpoints.rs");
        let text = std::fs::read_to_string(&engine_src)
            .expect("read the engine's failpoints.rs beside this crate");
        let mut engine_names = std::collections::BTreeSet::new();
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            // `pub const NAME: &str = "the.window.name";` — rustfmt may
            // wrap the string literal onto the following line.
            if !line.contains(": &str =") {
                continue;
            }
            let value_src = if line.contains('"') {
                *line
            } else {
                lines.get(i + 1).copied().unwrap_or("")
            };
            if let Some(start) = value_src.find('"')
                && let Some(rest) = value_src.get(start + 1..)
                && let Some(end) = rest.find('"')
            {
                engine_names.insert(rest[..end].to_string());
            }
        }
        assert!(
            engine_names.len() >= CRASH_WINDOWS.len(),
            "parsed only {} engine failpoint names — the source scan is broken",
            engine_names.len()
        );
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
