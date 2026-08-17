//! The full crash-window catalog for the hunt
//! (`dst_hunt_crash_window_sweep`), generated from the engine's
//! `src/failpoints.rs` name set.
//!
//! Regenerated 2026-08-14 on the rebase past the merge-rework/#487 base:
//! 66 -> 71 windows. New to the catalog: `blob_read.post_capture`,
//! `storage.local_create_if_absent_probe`, `mutation.post_no_effect_pre_gate`,
//! `read.refresh_post_state_pre_lineage`, and `init.post_manifest_create`
//! (the #487 replacement of `post_manifest_create_pre_stamp`, which the old
//! catalog never carried); rename `rewrite_after_delete_pre_index` ->
//! `_pre_confirm`. The new windows enter as never-reached until their
//! workloads exist.
//!
//! TODO: swap these string literals for `omnigraph::failpoints::names::*`
//! consts and extend `failpoint_names_guard.rs`'s scan to this crate — the
//! catalog is exempt from the guard today, so a typo'd window compiles and
//! silently never fires; only the suite's coverage accounting would notice.

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
